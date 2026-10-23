//! Data task delegate and the per-task state it feeds.
//!
//! NSURLSession calls the delegate on its own serial operation queue, never
//! on a thread the caller owns, so the delegate only records what happened in
//! a `DataTaskState` and wakes whoever is waiting on it.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use block2::DynBlock;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use objc2::rc::Retained;
use objc2::{AllocAnyThread, ClassType, DefinedClass, define_class, msg_send};
use objc2_foundation::{
    NSData, NSError, NSHTTPURLResponse, NSObject, NSObjectProtocol, NSString,
    NSURLAuthenticationChallenge, NSURLAuthenticationMethodServerTrust, NSURLCredential,
    NSURLCredentialPersistence, NSURLResponse, NSURLSession, NSURLSessionAuthChallengeDisposition,
    NSURLSessionDataDelegate, NSURLSessionDataTask, NSURLSessionDelegate,
    NSURLSessionResponseDisposition, NSURLSessionTask, NSURLSessionTaskDelegate,
};
use objc2_security::SecTrust;
use url::Url;

use crate::backend::types::ProgressCallback;
use crate::{Error, Result};

/// Unread body above which the task is suspended. NSURLSession may still
/// deliver data it had already read when `suspend` is called, so this is not
/// a hard cap, but a suspended task stops reading the socket.
const HIGH_WATER: usize = 4 << 20;

/// Unread body below which a suspended task is resumed.
const LOW_WATER: usize = 1 << 20;

/// Status line and headers of a response, converted on the delegate queue.
pub(crate) struct ResponseHead {
    pub status: StatusCode,
    pub headers: HeaderMap,
    /// The response's URL, which is the final one after redirects
    pub url: Option<Url>,
}

struct Inner {
    head: Option<ResponseHead>,
    chunks: VecDeque<Bytes>,
    buffered: usize,
    suspended: bool,
    outcome: Option<Result<()>>,
    waker: Option<Waker>,
    /// Last time the network made progress: data sent or received, or the
    /// task resumed.
    last_activity: Instant,
}

impl Inner {
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// What one data task has produced so far, shared between the delegate and
/// the task's owner.
pub(crate) struct DataTaskState {
    inner: Mutex<Inner>,
    progress: Option<ProgressCallback>,
}

impl DataTaskState {
    pub(crate) fn new(progress: Option<ProgressCallback>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                head: None,
                chunks: VecDeque::new(),
                buffered: 0,
                suspended: false,
                outcome: None,
                waker: None,
                last_activity: Instant::now(),
            }),
            progress,
        }
    }

    pub(crate) fn last_activity(&self) -> Instant {
        self.lock().last_activity
    }

    fn on_send_progress(&self) {
        self.lock().last_activity = Instant::now();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn on_response(&self, head: ResponseHead) {
        let mut inner = self.lock();
        inner.last_activity = Instant::now();
        if inner.head.is_none() && inner.outcome.is_none() {
            inner.head = Some(head);
        }
        inner.wake();
    }

    fn on_data(&self, task: &NSURLSessionDataTask, chunk: Bytes) {
        let mut inner = self.lock();
        inner.last_activity = Instant::now();
        inner.buffered += chunk.len();
        inner.chunks.push_back(chunk);
        // Suspend under the lock so that it cannot be reordered with the
        // consumer's resume in poll_chunk.
        if inner.buffered >= HIGH_WATER && !inner.suspended {
            inner.suspended = true;
            task.suspend();
        }
        inner.wake();
    }

    fn on_complete(&self, outcome: Result<()>) {
        let mut inner = self.lock();
        inner.outcome = Some(outcome);
        inner.wake();
    }

    /// Wait for the response head, or the error that ended the task first.
    pub(crate) fn poll_head(&self, cx: &mut Context<'_>) -> Poll<Result<ResponseHead>> {
        let mut inner = self.lock();
        if let Some(head) = inner.head.take() {
            return Poll::Ready(Ok(head));
        }
        match inner.outcome.take() {
            Some(Err(error)) => Poll::Ready(Err(error)),
            Some(Ok(())) => Poll::Ready(Err(Error::Internal(
                "Task finished without a response".to_string(),
            ))),
            None => {
                inner.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Next body chunk; `None` once the task finished cleanly, `Err` if it
    /// failed, after every chunk received before the failure.
    pub(crate) fn poll_chunk(
        &self,
        task: &NSURLSessionDataTask,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes>>> {
        let mut inner = self.lock();
        if let Some(chunk) = inner.chunks.pop_front() {
            inner.buffered -= chunk.len();
            if inner.suspended && inner.buffered <= LOW_WATER {
                inner.suspended = false;
                inner.last_activity = Instant::now();
                task.resume();
            }
            return Poll::Ready(Some(Ok(chunk)));
        }
        match inner.outcome.take() {
            Some(Ok(())) => Poll::Ready(None),
            Some(Err(error)) => Poll::Ready(Some(Err(error))),
            None => {
                inner.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Drop buffered body data once the owner has cancelled the task.
    pub(crate) fn discard(&self) {
        let mut inner = self.lock();
        inner.chunks.clear();
        inner.buffered = 0;
        inner.waker = None;
    }
}

/// Instance variables for the DataTaskDelegate
pub struct DataTaskDelegateIvars {
    tasks: Mutex<HashMap<usize, Arc<DataTaskState>>>,
    ignore_certificate_errors: bool,
    proxy_credentials: Option<(String, String)>,
}

define_class!(
    /// NSURLSessionDataDelegate that feeds each task's `DataTaskState`.
    #[unsafe(super = NSObject)]
    #[ivars = DataTaskDelegateIvars]
    pub struct DataTaskDelegate;

    unsafe impl NSObjectProtocol for DataTaskDelegate {}

    unsafe impl NSURLSessionDelegate for DataTaskDelegate {}

    unsafe impl NSURLSessionTaskDelegate for DataTaskDelegate {
        #[unsafe(method(URLSession:task:didCompleteWithError:))]
        fn URLSession_task_didCompleteWithError(
            &self,
            _session: &NSURLSession,
            task: &NSURLSessionTask,
            error: Option<&NSError>,
        ) {
            if let Some(state) = self.take(task.taskIdentifier()) {
                state.on_complete(match error {
                    Some(error) => Err(Error::from_ns_error(error)),
                    None => Ok(()),
                });
            }
        }

        #[unsafe(method(URLSession:task:didReceiveChallenge:completionHandler:))]
        fn URLSession_task_didReceiveChallenge_completionHandler(
            &self,
            _session: &NSURLSession,
            _task: &NSURLSessionTask,
            challenge: &NSURLAuthenticationChallenge,
            completion_handler: &DynBlock<
                dyn Fn(NSURLSessionAuthChallengeDisposition, *mut NSURLCredential),
            >,
        ) {
            match self.credential_for(challenge) {
                Some(credential) => completion_handler.call((
                    NSURLSessionAuthChallengeDisposition::UseCredential,
                    Retained::as_ptr(&credential) as *mut NSURLCredential,
                )),
                None => completion_handler.call((
                    NSURLSessionAuthChallengeDisposition::PerformDefaultHandling,
                    std::ptr::null_mut(),
                )),
            }
        }

        #[unsafe(method(URLSession:task:didSendBodyData:totalBytesSent:totalBytesExpectedToSend:))]
        fn URLSession_task_didSendBodyData_totalBytesSent_totalBytesExpectedToSend(
            &self,
            _session: &NSURLSession,
            task: &NSURLSessionTask,
            _bytes_sent: i64,
            total_bytes_sent: i64,
            total_bytes_expected_to_send: i64,
        ) {
            if let Some(state) = self.get(task.taskIdentifier()) {
                state.on_send_progress();
                if let Some(progress) = &state.progress {
                    let total = u64::try_from(total_bytes_expected_to_send)
                        .ok()
                        .filter(|&total| total > 0);
                    progress(total_bytes_sent.max(0) as u64, total);
                }
            }
        }
    }

    unsafe impl NSURLSessionDataDelegate for DataTaskDelegate {
        #[unsafe(method(URLSession:dataTask:didReceiveResponse:completionHandler:))]
        fn URLSession_dataTask_didReceiveResponse_completionHandler(
            &self,
            _session: &NSURLSession,
            data_task: &NSURLSessionDataTask,
            response: &NSURLResponse,
            completion_handler: &DynBlock<dyn Fn(NSURLSessionResponseDisposition)>,
        ) {
            if let Some(state) = self.get(data_task.taskIdentifier()) {
                state.on_response(response_head(response));
            }
            completion_handler.call((NSURLSessionResponseDisposition::Allow,));
        }

        #[unsafe(method(URLSession:dataTask:didReceiveData:))]
        fn URLSession_dataTask_didReceiveData(
            &self,
            _session: &NSURLSession,
            data_task: &NSURLSessionDataTask,
            data: &NSData,
        ) {
            if let Some(state) = self.get(data_task.taskIdentifier()) {
                state.on_data(data_task, Bytes::from(data.to_vec()));
            }
        }
    }
);

// SAFETY: the ivars are either behind a Mutex or never change after
// construction, and Objective-C reference counting is atomic.
unsafe impl Send for DataTaskDelegate {}
unsafe impl Sync for DataTaskDelegate {}

impl DataTaskDelegate {
    /// Create a delegate for one session.
    pub fn new(
        ignore_certificate_errors: bool,
        proxy_credentials: Option<(String, String)>,
    ) -> Retained<Self> {
        let delegate = Self::alloc().set_ivars(DataTaskDelegateIvars {
            tasks: Mutex::new(HashMap::new()),
            ignore_certificate_errors,
            proxy_credentials,
        });
        unsafe { msg_send![super(delegate), init] }
    }

    fn tasks(&self) -> std::sync::MutexGuard<'_, HashMap<usize, Arc<DataTaskState>>> {
        self.ivars()
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Route a task's callbacks to `state`. Must happen before the task is
    /// resumed.
    pub(crate) fn register(&self, task_id: usize, state: Arc<DataTaskState>) {
        self.tasks().insert(task_id, state);
    }

    fn get(&self, task_id: usize) -> Option<Arc<DataTaskState>> {
        self.tasks().get(&task_id).cloned()
    }

    fn take(&self, task_id: usize) -> Option<Arc<DataTaskState>> {
        self.tasks().remove(&task_id)
    }

    fn credential_for(
        &self,
        challenge: &NSURLAuthenticationChallenge,
    ) -> Option<Retained<NSURLCredential>> {
        let ivars = self.ivars();
        let space = challenge.protectionSpace();
        let method = space.authenticationMethod();

        if unsafe { method.isEqualToString(NSURLAuthenticationMethodServerTrust) } {
            if !ivars.ignore_certificate_errors {
                return None;
            }
            let trust: *mut SecTrust = unsafe { msg_send![&*space, serverTrust] };
            if trust.is_null() {
                return None;
            }
            let credential: Retained<NSURLCredential> =
                unsafe { msg_send![NSURLCredential::class(), credentialForTrust: trust] };
            return Some(credential);
        }

        if space.isProxy() && challenge.previousFailureCount() == 0 {
            let (user, password) = ivars.proxy_credentials.as_ref()?;
            return Some(NSURLCredential::credentialWithUser_password_persistence(
                &NSString::from_str(user),
                &NSString::from_str(password),
                NSURLCredentialPersistence::ForSession,
            ));
        }

        None
    }
}

fn response_head(response: &NSURLResponse) -> ResponseHead {
    let url = response
        .URL()
        .and_then(|url| url.absoluteString())
        .and_then(|url| Url::parse(&url.to_string()).ok());

    let Some(http) = response.downcast_ref::<NSHTTPURLResponse>() else {
        return ResponseHead {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            url,
        };
    };

    let status = u16::try_from(http.statusCode())
        .ok()
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::BAD_GATEWAY);

    let mut headers = HeaderMap::new();
    let fields = http.allHeaderFields();
    let keys = fields.allKeys();
    for index in 0..keys.count() {
        let key = keys.objectAtIndex(index);
        let Some(name) = key.downcast_ref::<NSString>() else {
            continue;
        };
        let Some(value) = fields.objectForKey(&key) else {
            continue;
        };
        let Some(value) = value.downcast_ref::<NSString>() else {
            continue;
        };
        // A value http::HeaderValue rejects (control characters) is dropped
        // rather than failing the response.
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.to_string().as_bytes()),
            HeaderValue::from_bytes(&header_value_bytes(&value.to_string())),
        ) {
            headers.append(name, value);
        }
    }

    ResponseHead {
        status,
        headers,
        url,
    }
}

/// A header value as the NSString NSURLRequest wants. CFNetwork writes
/// header strings as Latin-1, so each byte becomes the character with that
/// code point and goes on the wire unchanged.
pub(crate) fn header_value_string(value: &HeaderValue) -> Retained<NSString> {
    let latin1: String = value.as_bytes().iter().map(|&b| char::from(b)).collect();
    NSString::from_str(&latin1)
}

/// The bytes CFNetwork received for a header, which it decoded as Latin-1.
fn header_value_bytes(value: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(value.len());
    for c in value.chars() {
        match u8::try_from(u32::from(c)) {
            Ok(byte) => bytes.push(byte),
            Err(_) => bytes.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes()),
        }
    }
    bytes
}
