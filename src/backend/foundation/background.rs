//! Background downloads on Apple platforms: one background NSURLSession per
//! identifier, owned by `nsurlsessiond`, which keeps transferring while the
//! app is suspended or gone and hands the results to whichever launch of
//! the app next creates a session with the same identifier.
//!
//! Each task carries its job in `taskDescription`, which survives relaunch.
//! Resume data from a failed task is kept on disk per job and used when the
//! job is enqueued again.

use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use block2::{DynBlock, RcBlock};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use objc2::rc::{Retained, autoreleasepool};
use objc2::runtime::ProtocolObject;
use objc2::{AllocAnyThread, DefinedClass, define_class, msg_send};
use objc2_foundation::{
    NSArray, NSData, NSError, NSHTTPURLResponse, NSMutableURLRequest, NSObject, NSObjectProtocol,
    NSString, NSURL, NSURLResponse, NSURLSession, NSURLSessionConfiguration, NSURLSessionDelegate,
    NSURLSessionDownloadDelegate, NSURLSessionDownloadTask, NSURLSessionDownloadTaskResumeData,
    NSURLSessionTask, NSURLSessionTaskDelegate, NSURLSessionTaskState,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use url::Url;

use crate::background::{BackgroundHandler, BackgroundJob, FinishedDownload, JobState, JobStatus};
use crate::client::download::DownloadResponse;
use crate::{Error, Result};

/// Session identifier for `Client::download_background` without one.
const DEFAULT_IDENTIFIER: &str = "frakt.background";

/// Job id prefix for `Client::download_background`, whose results only the
/// awaiting future can use.
const CLIENT_JOB_PREFIX: &str = "frakt-client-";

/// What `taskDescription` holds for a task.
#[derive(Serialize, Deserialize)]
struct Descriptor {
    job: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    destination: Option<PathBuf>,
}

fn descriptor(task: &NSURLSessionTask) -> Option<Descriptor> {
    let description = task.taskDescription()?;
    serde_json::from_str(&description.to_string()).ok()
}

type ProgressCallback = Box<dyn Fn(u64, Option<u64>) + Send + Sync>;

/// A `Client::download_background` call waiting for its job.
struct Waiter {
    destination: PathBuf,
    progress: Option<ProgressCallback>,
    error_for_status: bool,
    outcome: Option<Result<DownloadResponse>>,
    done: Option<oneshot::Sender<Result<DownloadResponse>>>,
}

/// State shared by a session's delegate and its owners.
struct SessionState {
    handler: Mutex<Option<Arc<dyn BackgroundHandler>>>,
    waiters: Mutex<HashMap<String, Waiter>>,
    /// Pending tasks by job id, found at startup or enqueued since.
    tasks: Mutex<HashMap<String, Retained<NSURLSessionTask>>>,
    /// Jobs cancelled through `cancel`, whose completion is not reported.
    cancelled: Mutex<HashSet<String>>,
    /// Error statuses seen in didFinishDownloadingToURL, by task identifier,
    /// reported when the task completes.
    error_statuses: Mutex<HashMap<usize, StatusCode>>,
    resume_dir: PathBuf,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SessionState {
    fn handler(&self) -> Option<Arc<dyn BackgroundHandler>> {
        lock(&self.handler).clone()
    }

    fn resume_path(&self, job: &str) -> PathBuf {
        let name: String = job.bytes().map(|b| format!("{:02x}", b)).collect();
        self.resume_dir.join(format!("{}.resume", name))
    }

    fn save_resume_data(&self, job: &str, data: &[u8]) -> bool {
        std::fs::create_dir_all(&self.resume_dir).is_ok()
            && std::fs::write(self.resume_path(job), data).is_ok()
    }

    fn take_resume_data(&self, job: &str) -> Option<Vec<u8>> {
        let path = self.resume_path(job);
        let data = std::fs::read(&path).ok()?;
        let _ = std::fs::remove_file(path);
        Some(data)
    }

    fn drop_resume_data(&self, job: &str) {
        let _ = std::fs::remove_file(self.resume_path(job));
    }
}

/// A completion handler from the app delegate.
struct CompletionHandler(RcBlock<dyn Fn()>);

// SAFETY: the block is called once, on the main queue, as UIKit requires;
// moving it to another thread before that only moves the reference.
unsafe impl Send for CompletionHandler {}

/// Completion handlers handed over before their session finished its
/// events, and sessions that finished before their handler arrived.
struct Completions {
    handlers: HashMap<String, CompletionHandler>,
    finished_early: HashSet<String>,
}

static COMPLETIONS: LazyLock<Mutex<Completions>> = LazyLock::new(|| {
    Mutex::new(Completions {
        handlers: HashMap::new(),
        finished_early: HashSet::new(),
    })
});

unsafe extern "C" {
    static _dispatch_main_q: c_void;
    fn dispatch_async_f(
        queue: *const c_void,
        context: *mut c_void,
        work: extern "C" fn(*mut c_void),
    );
}

extern "C" fn call_completion_handler(context: *mut c_void) {
    let handler = unsafe { Box::from_raw(context as *mut CompletionHandler) };
    handler.0.call(());
}

fn call_on_main_queue(handler: CompletionHandler) {
    let context = Box::into_raw(Box::new(handler)) as *mut c_void;
    unsafe {
        dispatch_async_f(
            &raw const _dispatch_main_q,
            context,
            call_completion_handler,
        )
    };
}

pub(crate) fn handle_events_for_background_session(
    identifier: &str,
    completion_handler: &DynBlock<dyn Fn()>,
) {
    let handler = CompletionHandler(completion_handler.copy());
    let mut completions = lock(&COMPLETIONS);
    if completions.finished_early.remove(identifier) {
        drop(completions);
        call_on_main_queue(handler);
    } else {
        completions.handlers.insert(identifier.to_string(), handler);
    }
}

fn session_finished_events(identifier: &str) {
    let mut completions = lock(&COMPLETIONS);
    match completions.handlers.remove(identifier) {
        Some(handler) => {
            drop(completions);
            call_on_main_queue(handler);
        }
        None => {
            completions.finished_early.insert(identifier.to_string());
        }
    }
}

/// Instance variables for the BackgroundDelegate
pub struct BackgroundDelegateIvars {
    identifier: String,
    state: Arc<SessionState>,
}

define_class!(
    /// The delegate of one background session.
    #[unsafe(super = NSObject)]
    #[ivars = BackgroundDelegateIvars]
    pub struct BackgroundDelegate;

    unsafe impl NSObjectProtocol for BackgroundDelegate {}

    unsafe impl NSURLSessionDelegate for BackgroundDelegate {
        #[unsafe(method(URLSessionDidFinishEventsForBackgroundURLSession:))]
        fn URLSessionDidFinishEventsForBackgroundURLSession(&self, _session: &NSURLSession) {
            session_finished_events(&self.ivars().identifier);
        }
    }

    unsafe impl NSURLSessionTaskDelegate for BackgroundDelegate {
        #[unsafe(method(URLSession:task:didCompleteWithError:))]
        fn URLSession_task_didCompleteWithError(
            &self,
            _session: &NSURLSession,
            task: &NSURLSessionTask,
            error: Option<&NSError>,
        ) {
            if let Some(descriptor) = descriptor(task) {
                self.completed(task, &descriptor.job, error);
            }
        }
    }

    unsafe impl NSURLSessionDownloadDelegate for BackgroundDelegate {
        #[unsafe(method(URLSession:downloadTask:didFinishDownloadingToURL:))]
        fn URLSession_downloadTask_didFinishDownloadingToURL(
            &self,
            _session: &NSURLSession,
            download_task: &NSURLSessionDownloadTask,
            location: &NSURL,
        ) {
            let (Some(descriptor), Some(path)) = (descriptor(download_task), location.path())
            else {
                return;
            };
            self.downloaded(download_task, &descriptor, Path::new(&path.to_string()));
        }

        #[unsafe(method(URLSession:downloadTask:didWriteData:totalBytesWritten:totalBytesExpectedToWrite:))]
        fn URLSession_downloadTask_didWriteData_totalBytesWritten_totalBytesExpectedToWrite(
            &self,
            _session: &NSURLSession,
            download_task: &NSURLSessionDownloadTask,
            _bytes_written: i64,
            total_bytes_written: i64,
            total_bytes_expected_to_write: i64,
        ) {
            let Some(descriptor) = descriptor(download_task) else {
                return;
            };
            let written = total_bytes_written.max(0) as u64;
            let total = u64::try_from(total_bytes_expected_to_write)
                .ok()
                .filter(|&total| total > 0);
            let state = &self.ivars().state;
            if let Some(waiter) = lock(&state.waiters).get(&descriptor.job) {
                if let Some(progress) = &waiter.progress {
                    progress(written, total);
                }
                return;
            }
            if let Some(handler) = state.handler() {
                handler.progress(&descriptor.job, written, total);
            }
        }
    }
);

// SAFETY: the ivars are either behind a Mutex or never change after
// construction, and Objective-C reference counting is atomic.
unsafe impl Send for BackgroundDelegate {}
unsafe impl Sync for BackgroundDelegate {}

impl BackgroundDelegate {
    fn new(identifier: &str, state: Arc<SessionState>) -> Retained<Self> {
        let delegate = Self::alloc().set_ivars(BackgroundDelegateIvars {
            identifier: identifier.to_string(),
            state,
        });
        unsafe { msg_send![super(delegate), init] }
    }

    /// The system deletes `file` when the delegate method returns, so
    /// whoever wants it takes it here.
    fn downloaded(&self, task: &NSURLSessionDownloadTask, descriptor: &Descriptor, file: &Path) {
        let state = &self.ivars().state;
        let (status, headers) = response_head(task.response().as_deref());

        if let Some(waiter) = lock(&state.waiters).get_mut(&descriptor.job) {
            waiter.outcome = Some(if status.is_success() || !waiter.error_for_status {
                take_file(file, &waiter.destination).map(|bytes_downloaded| DownloadResponse {
                    file_path: waiter.destination.clone(),
                    bytes_downloaded,
                    status,
                    headers,
                })
            } else {
                Err(Error::HttpStatus(status))
            });
            return;
        }

        if descriptor.job.starts_with(CLIENT_JOB_PREFIX) {
            return;
        }
        if !status.is_success() {
            lock(&state.error_statuses).insert(task.taskIdentifier(), status);
            return;
        }
        if let Some(handler) = state.handler() {
            handler.finished(FinishedDownload {
                job: &descriptor.job,
                file,
                destination: descriptor.destination.as_deref(),
                status,
                headers: &headers,
            });
        }
    }

    fn completed(&self, task: &NSURLSessionTask, job: &str, error: Option<&NSError>) {
        let state = &self.ivars().state;
        lock(&state.tasks).remove(job);
        let error_status = lock(&state.error_statuses).remove(&task.taskIdentifier());

        if lock(&state.cancelled).remove(job) {
            if let Some(mut waiter) = lock(&state.waiters).remove(job) {
                if let Some(done) = waiter.done.take() {
                    let _ = done.send(Err(Error::Cancelled));
                }
            }
            return;
        }

        let resumable = match error.and_then(resume_data) {
            Some(data) => state.save_resume_data(job, &data),
            None => false,
        };

        if let Some(mut waiter) = lock(&state.waiters).remove(job) {
            let result = match (error, waiter.outcome.take()) {
                (Some(error), _) => Err(Error::from_ns_error(error)),
                (None, Some(outcome)) => outcome,
                (None, None) => Err(Error::Internal(
                    "Background download finished without a file".to_string(),
                )),
            };
            if let Some(done) = waiter.done.take() {
                let _ = done.send(result);
            }
            return;
        }
        if job.starts_with(CLIENT_JOB_PREFIX) {
            state.drop_resume_data(job);
            return;
        }

        let Some(handler) = state.handler() else {
            return;
        };
        match (error, error_status) {
            (Some(error), _) => handler.failed(job, Error::from_ns_error(error), resumable),
            (None, Some(status)) => handler.failed(job, Error::HttpStatus(status), false),
            (None, None) => state.drop_resume_data(job),
        }
    }
}

fn resume_data(error: &NSError) -> Option<Vec<u8>> {
    let info = error.userInfo();
    let value = info.objectForKey(unsafe { NSURLSessionDownloadTaskResumeData })?;
    let data = value.downcast_ref::<NSData>()?;
    Some(data.to_vec())
}

/// Move the downloaded file to `destination`, returning its size.
fn take_file(file: &Path, destination: &Path) -> Result<u64> {
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if std::fs::rename(file, destination).is_err() {
        std::fs::copy(file, destination)?;
    }
    Ok(std::fs::metadata(destination)?.len())
}

/// Status and headers of a task's response. CFNetwork decodes header bytes
/// as Latin-1, so each character maps back to the byte it came from.
fn response_head(response: Option<&NSURLResponse>) -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    let Some(http) = response.and_then(|r| r.downcast_ref::<NSHTTPURLResponse>()) else {
        return (StatusCode::OK, headers);
    };
    let status = u16::try_from(http.statusCode())
        .ok()
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let fields = http.allHeaderFields();
    let keys = fields.allKeys();
    for index in 0..keys.count() {
        let key = keys.objectAtIndex(index);
        let (Some(name), Some(value)) = (key.downcast_ref::<NSString>(), fields.objectForKey(&key))
        else {
            continue;
        };
        let Some(value) = value.downcast_ref::<NSString>() else {
            continue;
        };
        let mut bytes = Vec::new();
        for c in value.to_string().chars() {
            match u8::try_from(u32::from(c)) {
                Ok(byte) => bytes.push(byte),
                Err(_) => bytes.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes()),
            }
        }
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.to_string().as_bytes()),
            HeaderValue::from_bytes(&bytes),
        ) {
            headers.append(name, value);
        }
    }
    (status, headers)
}

/// A background session, kept for the life of the process: a process must
/// have only one session per identifier.
struct Session {
    session: Retained<NSURLSession>,
    _delegate: Retained<BackgroundDelegate>,
    state: Arc<SessionState>,
}

static SESSIONS: LazyLock<Mutex<HashMap<String, Arc<Session>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn resume_dir(identifier: &str) -> PathBuf {
    // The app's container on iOS; the user's home on macOS.
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    let name: String = identifier
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    home.join("Library/Application Support/frakt/background")
        .join(name)
}

/// The session for `identifier`, created on first use. A `handler` given
/// here replaces the session's; on creation it is in place before the
/// session exists, so events queued from an earlier launch reach it.
fn session_for(
    identifier: &str,
    handler: Option<Arc<dyn BackgroundHandler>>,
) -> Result<Arc<Session>> {
    let mut sessions = lock(&SESSIONS);
    if let Some(session) = sessions.get(identifier) {
        if let Some(handler) = handler {
            *lock(&session.state.handler) = Some(handler);
        }
        return Ok(session.clone());
    }

    let state = Arc::new(SessionState {
        handler: Mutex::new(handler),
        waiters: Mutex::new(HashMap::new()),
        tasks: Mutex::new(HashMap::new()),
        cancelled: Mutex::new(HashSet::new()),
        error_statuses: Mutex::new(HashMap::new()),
        resume_dir: resume_dir(identifier),
    });
    let session = autoreleasepool(|_| {
        let delegate = BackgroundDelegate::new(identifier, state.clone());
        let config = NSURLSessionConfiguration::backgroundSessionConfigurationWithIdentifier(
            &NSString::from_str(identifier),
        );
        config.setSessionSendsLaunchEvents(true);
        let session = unsafe {
            NSURLSession::sessionWithConfiguration_delegate_delegateQueue(
                &config,
                Some(ProtocolObject::from_ref(&*delegate)),
                None,
            )
        };
        Session {
            session,
            _delegate: delegate,
            state,
        }
    });
    let session = Arc::new(session);
    session.adopt_existing_tasks();
    sessions.insert(identifier.to_string(), session.clone());
    Ok(session)
}

/// Run `f` on the session's tasks, as `getAllTasksWithCompletionHandler`
/// reports them on the delegate queue.
fn with_all_tasks(
    session: &NSURLSession,
    f: impl FnOnce(&NSArray<NSURLSessionTask>) + Send + 'static,
) {
    let f = Mutex::new(Some(f));
    let block = RcBlock::new(move |tasks: NonNull<NSArray<NSURLSessionTask>>| {
        if let Some(f) = lock(&f).take() {
            f(unsafe { tasks.as_ref() });
        }
    });
    autoreleasepool(|_| unsafe { session.getAllTasksWithCompletionHandler(&block) });
}

fn is_pending(task: &NSURLSessionTask) -> bool {
    let state = task.state();
    state == NSURLSessionTaskState::Running || state == NSURLSessionTaskState::Suspended
}

impl Session {
    /// Record tasks left from an earlier launch, so enqueue and cancel know
    /// them. Waits briefly for the delegate queue to answer; called on that
    /// queue (from a handler) it would wait out the timeout instead.
    fn adopt_existing_tasks(&self) {
        let (tx, rx) = std::sync::mpsc::channel();
        let state = self.state.clone();
        with_all_tasks(&self.session, move |tasks| {
            let mut known = lock(&state.tasks);
            for index in 0..tasks.count() {
                let task = tasks.objectAtIndex(index);
                if let (Some(descriptor), true) = (descriptor(&task), is_pending(&task)) {
                    known.insert(descriptor.job, task);
                }
            }
            let _ = tx.send(());
        });
        let _ = rx.recv_timeout(Duration::from_secs(2));
    }

    fn enqueue(&self, job: BackgroundJob) -> Result<()> {
        if lock(&self.state.tasks).contains_key(&job.id) {
            return Err(Error::Internal(format!(
                "Background job {} is already pending",
                job.id
            )));
        }
        let description = serde_json::to_string(&Descriptor {
            job: job.id.clone(),
            destination: job.destination.clone(),
        })
        .map_err(|e| Error::Internal(e.to_string()))?;

        autoreleasepool(|_| {
            // Resume data carries the original request, so a resumed job
            // keeps the URL and headers it was first enqueued with.
            let task = match self.state.take_resume_data(&job.id) {
                Some(data) => self
                    .session
                    .downloadTaskWithResumeData(&NSData::from_vec(data)),
                None => {
                    let url = NSURL::URLWithString(&NSString::from_str(job.url.as_str()))
                        .ok_or(Error::InvalidUrl)?;
                    let request = NSMutableURLRequest::requestWithURL(&url);
                    for (name, value) in &job.headers {
                        let value: String =
                            value.as_bytes().iter().map(|&b| char::from(b)).collect();
                        request.addValue_forHTTPHeaderField(
                            &NSString::from_str(&value),
                            &NSString::from_str(name.as_str()),
                        );
                    }
                    self.session.downloadTaskWithRequest(&request)
                }
            };
            task.setTaskDescription(Some(&NSString::from_str(&description)));
            lock(&self.state.cancelled).remove(&job.id);
            lock(&self.state.tasks).insert(job.id.clone(), Retained::into_super(task.clone()));
            task.resume();
            Ok(())
        })
    }

    fn cancel(&self, job: &str) {
        let task = lock(&self.state.tasks).remove(job);
        if let Some(task) = task {
            lock(&self.state.cancelled).insert(job.to_string());
            autoreleasepool(|_| task.cancel());
        }
        self.state.drop_resume_data(job);
    }

    async fn jobs(&self) -> Result<Vec<JobStatus>> {
        let (tx, rx) = oneshot::channel();
        with_all_tasks(&self.session, move |tasks| {
            let mut jobs = Vec::new();
            for index in 0..tasks.count() {
                let task = tasks.objectAtIndex(index);
                let Some(descriptor) = descriptor(&task) else {
                    continue;
                };
                if !is_pending(&task) || descriptor.job.starts_with(CLIENT_JOB_PREFIX) {
                    continue;
                }
                jobs.push(JobStatus {
                    id: descriptor.job,
                    state: if task.state() == NSURLSessionTaskState::Running {
                        JobState::Running
                    } else {
                        JobState::Waiting
                    },
                    downloaded: task.countOfBytesReceived().max(0) as u64,
                    total: u64::try_from(task.countOfBytesExpectedToReceive())
                        .ok()
                        .filter(|&total| total > 0),
                });
            }
            let _ = tx.send(jobs);
        });
        rx.await
            .map_err(|_| Error::Internal("Background session did not list its tasks".to_string()))
    }
}

pub(crate) struct Downloads {
    session: Arc<Session>,
    handler: Arc<dyn BackgroundHandler>,
}

impl Downloads {
    pub(crate) fn new(identifier: &str, handler: Arc<dyn BackgroundHandler>) -> Result<Self> {
        let session = session_for(identifier, Some(handler.clone()))?;
        Ok(Self { session, handler })
    }

    pub(crate) fn enqueue(&self, job: BackgroundJob) -> Result<()> {
        if job.id.starts_with(CLIENT_JOB_PREFIX) {
            return Err(Error::Internal(format!(
                "Job ids starting with {} are reserved",
                CLIENT_JOB_PREFIX
            )));
        }
        self.session.enqueue(job)
    }

    pub(crate) fn cancel(&self, job: &str) -> Result<()> {
        self.session.cancel(job);
        Ok(())
    }

    pub(crate) async fn jobs(&self) -> Result<Vec<JobStatus>> {
        self.session.jobs().await
    }
}

impl Drop for Downloads {
    fn drop(&mut self) {
        let mut handler = lock(&self.session.state.handler);
        if handler
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &self.handler))
        {
            *handler = None;
        }
    }
}

static NEXT_CLIENT_JOB: AtomicU64 = AtomicU64::new(1);

/// `Client::download_background` on Apple platforms: a job on the shared
/// session for `session_identifier`, awaited by this call.
pub(crate) async fn execute_background_download(
    url: Url,
    file_path: PathBuf,
    session_identifier: Option<String>,
    headers: HeaderMap,
    progress_callback: Option<ProgressCallback>,
    error_for_status: bool,
) -> Result<DownloadResponse> {
    let identifier = session_identifier.unwrap_or_else(|| DEFAULT_IDENTIFIER.to_string());
    let session = session_for(&identifier, None)?;
    let job = format!(
        "{}{}-{}",
        CLIENT_JOB_PREFIX,
        std::process::id(),
        NEXT_CLIENT_JOB.fetch_add(1, Ordering::Relaxed)
    );

    let (tx, rx) = oneshot::channel();
    lock(&session.state.waiters).insert(
        job.clone(),
        Waiter {
            destination: file_path,
            progress: progress_callback,
            error_for_status,
            outcome: None,
            done: Some(tx),
        },
    );
    let mut guard = WaiterGuard {
        session: session.clone(),
        job: job.clone(),
        armed: true,
    };
    session.enqueue(BackgroundJob {
        id: job,
        url,
        headers,
        destination: None,
    })?;

    let result = rx.await;
    guard.armed = false;
    result.map_err(|_| Error::Internal("Background download was abandoned".to_string()))?
}

/// Forgets the job and cancels its task if the awaiting future is dropped
/// first.
struct WaiterGuard {
    session: Arc<Session>,
    job: String,
    armed: bool,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            lock(&self.session.state.waiters).remove(&self.job);
            self.session.cancel(&self.job);
        }
    }
}
