//! BITS (Background Intelligent Transfer Service) downloads for Windows.
//!
//! BITS transfers files in a system service, as jobs in a queue that belongs
//! to the user and not to the process that filled it.
//!
//! Ownership and threading:
//!
//! - Every COM interface (`IBackgroundCopyManager`, `IBackgroundCopyJob`,
//!   `IBackgroundCopyError`, ...) is created, used and released on one worker
//!   thread per [`BitsDownloadManager`]. The thread joins the multithreaded
//!   apartment before it creates the first and leaves it after it has
//!   released the last. No interface pointer leaves the thread in any form.
//! - Other threads send the worker [`Command`]s made of plain data, each with
//!   a channel for the reply, and name a job by its `GUID`.
//! - A `windows::core::Error` can own an `IErrorInfo`, so the worker turns
//!   each into a [`crate::Error`] before it replies.
//! - A job nobody waits for any more is cancelled; see
//!   [`BitsDownloadManager::start_background_download`].
//! - No job is created for a user BITS will not act for, because that user
//!   could not remove it again; see [`logon_can_use_bits`].

use crate::client::download::DownloadResponse;
use crate::{Error, Result};
use http::{HeaderMap, StatusCode};
use std::collections::HashMap;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::os::windows::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;
use url::Url;
use windows::Win32::Foundation::{ERROR_NOT_LOGGED_ON, HANDLE};
use windows::Win32::Globalization::GetThreadLocale;
use windows::Win32::Networking::BackgroundIntelligentTransferService::{
    BG_E_INVALID_STATE, BG_ERROR_CONTEXT_NONE, BG_HTTP_REDIRECT_POLICY_DISALLOW,
    BG_JOB_PRIORITY_NORMAL, BG_JOB_PROGRESS, BG_JOB_STATE, BG_JOB_STATE_ACKNOWLEDGED,
    BG_JOB_STATE_CANCELLED, BG_JOB_STATE_ERROR, BG_JOB_STATE_TRANSFERRED,
    BG_JOB_STATE_TRANSIENT_ERROR, BG_JOB_TYPE_DOWNLOAD, BackgroundCopyManager,
    IBackgroundCopyError, IBackgroundCopyJob, IBackgroundCopyJobHttpOptions,
    IBackgroundCopyJobHttpOptions3, IBackgroundCopyManager,
};
use windows::Win32::Security::{
    GetTokenInformation, IsWellKnownSid, LookupAccountSidW, SECURITY_MAX_SID_SIZE, SID_NAME_USE,
    TOKEN_USER, TokenUser, WinLocalServiceSid, WinLocalSystemSid, WinNetworkServiceSid,
};
use windows::Win32::System::Com::{
    CLSCTX_LOCAL_SERVER, COINIT_MULTITHREADED, CoCreateInstance, CoInitializeEx, CoTaskMemFree,
    CoUninitialize,
};
use windows::Win32::System::RemoteDesktop::{
    WTS_CURRENT_SESSION, WTS_INFO_CLASS, WTSDomainName, WTSFreeMemory, WTSQuerySessionInformationW,
    WTSUserName,
};
use windows::core::{GUID, HRESULT, HSTRING, Interface, PCWSTR, PWSTR};

use super::error::map_windows_error_with_context;

/// How often a job's state and progress are read.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// BITS's limit for a job's display name, in UTF-16 units.
const MAX_DISPLAY_NAME: usize = 256;

/// BITS's limit for a job's custom headers, in UTF-16 units and counting the
/// terminator.
const MAX_CUSTOM_HEADERS: usize = 16_384;

/// `BG_SIZE_UNKNOWN`: BITS does not know the size yet.
const SIZE_UNKNOWN: u64 = u64::MAX;

/// `MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US)`.
const LANG_EN_US: u32 = 0x0409;

/// BITS does not regard the caller's user as logged on.
const E_NOT_LOGGED_ON: HRESULT = HRESULT::from_win32(ERROR_NOT_LOGGED_ON.0);

/// `GetCurrentProcessToken()`, an inline function in the Windows SDK: a
/// pseudo-handle that queries of this process's token accept.
const CURRENT_PROCESS_TOKEN: HANDLE = HANDLE(-4_isize as *mut c_void);

/// Set once BITS has refused a call with [`E_NOT_LOGGED_ON`]. Whatever
/// [`logon_can_use_bits`] made of this process, BITS will not act for its
/// user, and no further job is created.
static BITS_SAYS_NOT_LOGGED_ON: AtomicBool = AtomicBool::new(false);

/// Called with the bytes transferred so far and, once BITS knows it, the
/// total.
pub type ProgressCallback = dyn Fn(u64, Option<u64>) + Send + Sync + 'static;

type Reply<T> = oneshot::Sender<Result<T>>;

/// Downloads files as BITS jobs.
///
/// Owns the worker thread that holds the BITS COM interfaces; see the module
/// documentation. The manager itself is `Send + Sync`. Dropping it waits for
/// the worker to finish what it was sent and to exit.
pub struct BitsDownloadManager {
    // Declared before `_worker`, so dropped before it: the worker only exits,
    // and the join in `WorkerThread`'s drop only returns, once this is gone.
    commands: mpsc::Sender<Command>,
    _worker: WorkerThread,
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BitsDownloadManager>();
};

impl BitsDownloadManager {
    /// Start the worker thread and connect it to BITS.
    ///
    /// Fails when BITS is unavailable, and when it would not act for this
    /// process's user: see [`logon_can_use_bits`].
    pub async fn new() -> Result<Self> {
        logon_can_use_bits()?;
        let (commands, inbox) = mpsc::channel();
        let (started, startup) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("frakt-bits".to_string())
            .spawn(move || run_worker(inbox, started))
            .map_err(|e| {
                Error::Internal(format!("Failed to start the BITS worker thread: {}", e))
            })?;

        // Connecting can start the BITS service, which takes a while. If this
        // future is dropped meanwhile, the thread is not joined: it finds
        // nobody waiting once it has connected, and exits on its own.
        let startup = startup.await;
        let manager = Self {
            commands,
            _worker: WorkerThread(Some(thread)),
        };
        match startup {
            Ok(Ok(())) => Ok(manager),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(worker_gone()),
        }
    }

    /// Download `url` to `file_path` as a BITS job, and wait for the job.
    ///
    /// The job gets `session_identifier` as its display name and `headers`
    /// as its request headers. It is polled until BITS has transferred the
    /// file, with `progress_callback` told of each change, and then
    /// completed, which is when BITS renames its temporary file to
    /// `file_path`. A file already there is replaced by that and untouched
    /// until then. A relative `file_path` is resolved against the current
    /// directory when the job is created.
    ///
    /// The job does not outlive this call. BITS would keep transferring for
    /// a job nobody waits for, but the file only appears when the job is
    /// completed, and nothing here can find a job again to complete it. So
    /// the job is cancelled, and its temporary file deleted, before this
    /// returns an error, and when the returned future is dropped before it
    /// resolves. A job is only left behind when that cannot be done: the
    /// process ends without dropping the future, or BITS cannot be reached.
    /// BITS cancels such a job itself after its inactivity timeout, 90 days
    /// by default.
    ///
    /// What BITS cannot do, and what happens instead:
    ///
    /// - It reports neither the status nor the headers of the responses it
    ///   got. A job only finishes when the server answered every request
    ///   with success, so the response says 200 OK and has no headers.
    /// - It stores nothing of a response with an error status. With
    ///   `error_for_status` such a status is `Error::HttpStatus`; without, it
    ///   is some other error, as is everything else BITS cannot do (a server
    ///   without range requests, a `Range` header, a `\\?\` path), so that
    ///   the caller can fetch the URL another way.
    /// - It sends the request headers with every request of the job, so a
    ///   job with headers does not follow redirects: they could carry
    ///   credentials to another origin. The redirect is an error.
    /// - It retries what it calls transient errors by itself, for up to two
    ///   weeks. That is left to it while the network or the server is
    ///   unreachable, or policy holds the transfer back: the future keeps
    ///   waiting. An HTTP status or a TLS failure is not waited on, because
    ///   waiting does not change it.
    pub async fn start_background_download(
        &self,
        url: Url,
        file_path: PathBuf,
        session_identifier: Option<String>,
        headers: &HeaderMap,
        progress_callback: Option<&ProgressCallback>,
        error_for_status: bool,
    ) -> Result<DownloadResponse> {
        let spec = JobSpec::new(&url, &file_path, session_identifier, headers)?;
        let id = self.create(spec).await?;
        let _cancel = JobGuard {
            commands: &self.commands,
            id,
        };

        match self.finish(id, progress_callback, error_for_status).await {
            Ok(bytes_downloaded) => Ok(DownloadResponse {
                file_path,
                bytes_downloaded,
                status: StatusCode::OK,
                headers: HeaderMap::new(),
            }),
            Err(error) => {
                // The guard cancels too, but does not wait: this way the
                // job and its temporary file are gone when the caller hears
                // of the failure, and can use the destination.
                let cancelled = |reply| Command::Cancel {
                    id,
                    reply: Some(reply),
                };
                let _ = self.call(cancelled).await;
                Err(error)
            }
        }
    }

    /// Poll a job until its file is transferred, and complete it. The bytes
    /// transferred are returned.
    async fn finish(
        &self,
        id: GUID,
        progress_callback: Option<&ProgressCallback>,
        error_for_status: bool,
    ) -> Result<u64> {
        let mut reported = (0, None);
        loop {
            let status = self.call(|reply| Command::Poll { id, reply }).await?;
            let progress = status.progress();
            if progress != reported {
                reported = progress;
                if let Some(callback) = progress_callback {
                    callback(progress.0, progress.1);
                }
            }
            match status.next_step(error_for_status) {
                Step::Wait => tokio::time::sleep(POLL_INTERVAL).await,
                Step::Complete => {
                    self.call(|reply| Command::Complete { id, reply }).await?;
                    return Ok(status.bytes_transferred);
                }
                Step::Done => return Ok(status.bytes_transferred),
                Step::Fail(error) => return Err(error),
            }
        }
    }

    /// Create and start a job. Until its id is handed over, for a
    /// [`JobGuard`] to hold, dropping this future is what cancels the job.
    async fn create(&self, spec: JobSpec) -> Result<GUID> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Create { spec, reply })
            .map_err(|_| worker_gone())?;
        let mut pending = PendingJob {
            commands: &self.commands,
            response,
        };
        (&mut pending.response).await.map_err(|_| worker_gone())?
    }

    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .map_err(|_| worker_gone())?;
        response.await.map_err(|_| worker_gone())?
    }
}

fn worker_gone() -> Error {
    Error::Internal("The BITS worker thread has stopped".to_string())
}

/// Whether BITS will act for the user this process runs as.
///
/// BITS lets a user it does not regard as logged on create a job, and then
/// refuses, with `ERROR_NOT_LOGGED_ON`, every call that needs the user's
/// token: `AddFile`, `SetCustomHeaders`, `Complete`, and `Cancel` as well.
/// The job stays in the user's queue, which is limited, until an
/// administrator or BITS's inactivity timeout removes it. So this is asked
/// before any job is created.
///
/// Microsoft's rule ("Users and Network Connections" in the BITS
/// documentation): LocalSystem, LocalService and NetworkService are always
/// logged on. Any other user has to have logged on interactively: at the
/// console, through Remote Desktop, or by fast user switching. Running a
/// program as another user is expressly not that, although the token it
/// gets is an interactive one, and neither is what SSH, WinRM, a service, a
/// CI runner or a task that runs "whether the user is logged on or not"
/// gets.
///
/// The token therefore does not tell. What an interactive logon leaves and
/// none of the others does is a session that belongs to the user, so that
/// is the test: the process is one of the three service accounts, or it
/// runs in a session whose user it is.
///
/// This is stricter than BITS for a process outside the session its user is
/// logged on in, and for PowerShell Remoting, which BITS has accepted since
/// Windows 10 1607. Being stricter only costs the use of BITS.
fn logon_can_use_bits() -> Result<()> {
    if BITS_SAYS_NOT_LOGGED_ON.load(Ordering::Relaxed) {
        return Err(Error::Internal(
            "BITS has refused this process's user as not logged on".to_string(),
        ));
    }
    let user = match process_user()? {
        ProcessUser::ServiceAccount => return Ok(()),
        ProcessUser::Account(user) => user,
    };
    match session_user() {
        Some(session) if session.is(&user) => Ok(()),
        Some(session) => Err(Error::Internal(format!(
            "BITS does not act for {}, who is not logged on: this session is {}'s",
            user, session
        ))),
        None => Err(Error::Internal(format!(
            "BITS does not act for {}, who is not logged on: this session has no user",
            user
        ))),
    }
}

/// A user account by its names, which Windows compares without case.
#[derive(Debug)]
struct Account {
    domain: String,
    name: String,
}

impl Account {
    fn is(&self, other: &Account) -> bool {
        !self.name.is_empty()
            && self.name.to_lowercase() == other.name.to_lowercase()
            && self.domain.to_lowercase() == other.domain.to_lowercase()
    }
}

impl std::fmt::Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\\{}", self.domain, self.name)
    }
}

enum ProcessUser {
    /// LocalSystem, LocalService or NetworkService.
    ServiceAccount,
    Account(Account),
}

/// The user of the process token, which is who BITS sees: the worker thread
/// impersonates nobody.
fn process_user() -> Result<ProcessUser> {
    /// What `GetTokenInformation` writes for `TokenUser`: the structure,
    /// and the SID it points to behind it.
    #[repr(C)]
    struct Buffer {
        user: TOKEN_USER,
        sid: [u8; SECURITY_MAX_SID_SIZE as usize],
    }

    let mut buffer = Buffer {
        user: TOKEN_USER::default(),
        sid: [0; SECURITY_MAX_SID_SIZE as usize],
    };
    let mut written = 0;
    // SAFETY: the pseudo-handle is valid for querying this process's token.
    // `buffer` is valid for writes of the size passed, which is its own, and
    // aligned for the `TOKEN_USER` that is written at its start.
    unsafe {
        GetTokenInformation(
            CURRENT_PROCESS_TOKEN,
            TokenUser,
            Some((&raw mut buffer).cast()),
            size_of::<Buffer>() as u32,
            &mut written,
        )
    }
    .map_err(|e| map_windows_error_with_context(e, "Failed to read the process token's user"))?;
    // Points into `buffer`, which stays where it is until this function
    // returns.
    let sid = buffer.user.User.Sid;

    let service_account = [WinLocalSystemSid, WinLocalServiceSid, WinNetworkServiceSid]
        .into_iter()
        // SAFETY: `sid` is the SID GetTokenInformation wrote into `buffer`,
        // which is alive.
        .any(|kind| unsafe { IsWellKnownSid(sid, kind) }.as_bool());
    if service_account {
        return Ok(ProcessUser::ServiceAccount);
    }

    let mut name = [0_u16; 512];
    let mut domain = [0_u16; 512];
    let mut name_length = name.len() as u32;
    let mut domain_length = domain.len() as u32;
    let mut kind = SID_NAME_USE::default();
    // SAFETY: `sid` as above. The two buffers are valid for writes of the
    // lengths passed with them, and the other out-pointers for theirs.
    unsafe {
        LookupAccountSidW(
            PCWSTR::null(),
            sid,
            Some(PWSTR(name.as_mut_ptr())),
            &mut name_length,
            Some(PWSTR(domain.as_mut_ptr())),
            &mut domain_length,
            &mut kind,
        )
    }
    .map_err(|e| map_windows_error_with_context(e, "Failed to name the process's user"))?;
    // On success the lengths are those of the names, without terminators.
    let text = |buffer: &[u16], length: u32| {
        String::from_utf16_lossy(buffer.get(..length as usize).unwrap_or_default())
    };
    Ok(ProcessUser::Account(Account {
        domain: text(&domain, domain_length),
        name: text(&name, name_length),
    }))
}

/// The user whose session this process runs in. Session 0, where services
/// and all they start run, has none.
fn session_user() -> Option<Account> {
    let name = session_string(WTSUserName)?;
    let domain = session_string(WTSDomainName)?;
    (!name.is_empty()).then_some(Account { domain, name })
}

/// One of the strings Remote Desktop Services keeps about this process's
/// session.
fn session_string(class: WTS_INFO_CLASS) -> Option<String> {
    let mut text = PWSTR::null();
    let mut bytes = 0;
    // SAFETY: both out-pointers are valid for writes, and no server handle
    // means this computer.
    unsafe { WTSQuerySessionInformationW(None, WTS_CURRENT_SESSION, class, &mut text, &mut bytes) }
        .ok()?;
    if text.is_null() {
        return None;
    }
    // SAFETY: on success, and for the classes this is called with, the
    // buffer is a NUL-terminated string, which lives until it is freed.
    let value = String::from_utf16_lossy(unsafe { text.as_wide() });
    // SAFETY: the buffer came from WTSQuerySessionInformationW and is freed
    // exactly once, here.
    unsafe { WTSFreeMemory(text.as_ptr().cast()) };
    Some(value)
}

/// Every failed BITS call passes through here, so that [`E_NOT_LOGGED_ON`]
/// is remembered: see [`BITS_SAYS_NOT_LOGGED_ON`].
fn note_refusal(error: &windows::core::Error) {
    if error.code() == E_NOT_LOGGED_ON {
        BITS_SAYS_NOT_LOGGED_ON.store(true, Ordering::Relaxed);
    }
}

fn bits_error(error: windows::core::Error, context: &str) -> Error {
    note_refusal(&error);
    map_windows_error_with_context(error, context)
}

/// The wait for a new job's id. Dropped before the id was taken, it cancels
/// the job the worker may have created meanwhile.
struct PendingJob<'a> {
    commands: &'a mpsc::Sender<Command>,
    response: oneshot::Receiver<Result<GUID>>,
}

impl Drop for PendingJob<'_> {
    fn drop(&mut self) {
        // Once closed, the worker's reply either fails, and the worker
        // cancels the job itself, or was sent before and is still here.
        self.response.close();
        if let Ok(Ok(id)) = self.response.try_recv() {
            let _ = self.commands.send(Command::Cancel { id, reply: None });
        }
    }
}

/// Joins the worker thread when dropped.
///
/// The join is what makes a cancellation sent from a dropped future certain
/// to have reached BITS by the time the manager is gone, even if the process
/// exits right after.
struct WorkerThread(Option<JoinHandle<()>>);

impl Drop for WorkerThread {
    fn drop(&mut self) {
        if let Some(thread) = self.0.take() {
            let _ = thread.join();
        }
    }
}

/// Tells the worker to cancel a job when dropped.
///
/// The worker forgets a job once it is completed, so after that this does
/// nothing; in every other case the job is one nobody waits for any more.
struct JobGuard<'a> {
    commands: &'a mpsc::Sender<Command>,
    id: GUID,
}

impl Drop for JobGuard<'_> {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Cancel {
            id: self.id,
            reply: None,
        });
    }
}

/// A request to the worker thread.
enum Command {
    /// Create a job and start it.
    Create { spec: JobSpec, reply: Reply<GUID> },
    /// Read a job's state and progress.
    Poll { id: GUID, reply: Reply<JobStatus> },
    /// Put a transferred job's file in place and end the job.
    Complete { id: GUID, reply: Reply<()> },
    /// Forget a job, cancelling it if BITS still has it. Nobody can be told
    /// when this is sent from `Drop`.
    Cancel { id: GUID, reply: Option<Reply<()>> },
}

/// What the worker needs to create a job.
struct JobSpec {
    display_name: String,
    url: String,
    /// Absolute, as BITS requires.
    local_path: PathBuf,
    /// The request headers as CRLF-terminated lines, if there are any.
    headers: Option<String>,
}

impl JobSpec {
    /// Checks everything BITS is known to refuse, so that no job is created
    /// only to be cancelled.
    fn new(
        url: &Url,
        file_path: &Path,
        session_identifier: Option<String>,
        headers: &HeaderMap,
    ) -> Result<Self> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::InvalidUrl);
        }
        Ok(Self {
            display_name: display_name(session_identifier),
            url: url.to_string(),
            local_path: local_path(file_path)?,
            headers: header_block(headers)?,
        })
    }
}

/// A job's display name: the session identifier, cut to what BITS accepts.
fn display_name(session_identifier: Option<String>) -> String {
    let name = session_identifier.unwrap_or_else(|| {
        format!(
            "frakt-download-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        )
    });
    let mut units = 0;
    name.chars()
        .take_while(|character| {
            units += character.len_utf16();
            units <= MAX_DISPLAY_NAME
        })
        .collect()
}

/// The destination as BITS wants it: absolute, with backslashes, and without
/// the `\\?\` prefix it does not accept.
fn local_path(file_path: &Path) -> Result<PathBuf> {
    let path = std::path::absolute(file_path).map_err(|e| {
        Error::Internal(format!(
            "Cannot make {} absolute for BITS: {}",
            file_path.display(),
            e
        ))
    })?;
    let verbatim = matches!(
        path.components().next(),
        Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim()
    );
    if verbatim {
        return Err(Error::Internal(format!(
            "BITS does not accept the verbatim path {}",
            path.display()
        )));
    }
    // BITS reads the path up to the first NUL, which would name another file.
    if path.as_os_str().encode_wide().any(|unit| unit == 0) {
        return Err(Error::Internal(
            "The download path contains a NUL".to_string(),
        ));
    }
    Ok(path)
}

/// The request headers as BITS takes them: `name: value` lines, each ended
/// by CRLF. `None` when there are no headers.
fn header_block(headers: &HeaderMap) -> Result<Option<String>> {
    if headers.is_empty() {
        return Ok(None);
    }
    let mut block = String::new();
    for (name, value) in headers {
        // BITS fetches the file with range requests of its own.
        if name == http::header::RANGE || name == http::header::IF_RANGE {
            return Err(Error::Internal(format!(
                "BITS cannot send a {} header",
                name
            )));
        }
        // Other bytes would need a code page to become UTF-16, and the
        // server might not get back the bytes the caller set.
        let value = value.to_str().map_err(|_| {
            Error::Internal(format!("BITS cannot send the non-ASCII {} header", name))
        })?;
        block.push_str(name.as_str());
        block.push_str(": ");
        block.push_str(value);
        block.push_str("\r\n");
    }
    // All ASCII, so bytes are UTF-16 units.
    if block.len() >= MAX_CUSTOM_HEADERS {
        return Err(Error::Internal(
            "The request headers are too long for BITS".to_string(),
        ));
    }
    Ok(Some(block))
}

/// A job as one poll found it.
#[derive(Debug)]
struct JobStatus {
    state: BG_JOB_STATE,
    bytes_transferred: u64,
    /// `None` until BITS knows the size.
    bytes_total: Option<u64>,
    /// Why the job is in an error state, if it is in one and BITS still
    /// says.
    failure: Option<JobFailure>,
}

/// What the caller waiting for a job does after a poll.
#[derive(Debug)]
enum Step {
    /// The job is still BITS's to work on.
    Wait,
    /// The file is transferred, and the job has to be completed.
    Complete,
    /// The job has been completed already, by someone else.
    Done,
    Fail(Error),
}

impl JobStatus {
    /// The bytes transferred and their total. Once the file is transferred
    /// the total is known even where BITS never reported one, as Wine's
    /// does not.
    fn progress(&self) -> (u64, Option<u64>) {
        let finished = matches!(
            self.state,
            BG_JOB_STATE_TRANSFERRED | BG_JOB_STATE_ACKNOWLEDGED
        );
        let total = self
            .bytes_total
            .or(finished.then_some(self.bytes_transferred));
        (self.bytes_transferred, total)
    }

    fn next_step(&self, error_for_status: bool) -> Step {
        match self.state {
            BG_JOB_STATE_TRANSFERRED => Step::Complete,
            BG_JOB_STATE_ACKNOWLEDGED => Step::Done,
            BG_JOB_STATE_CANCELLED => Step::Fail(Error::Cancelled),
            BG_JOB_STATE_ERROR => Step::Fail(match &self.failure {
                Some(failure) => failure.to_error(error_for_status),
                None => Error::Internal("BITS job failed".to_string()),
            }),
            // BITS retries by itself after a transient error, for days if it
            // has to. That suits a network that is down, and not an answer
            // that will be the same next time.
            BG_JOB_STATE_TRANSIENT_ERROR => match &self.failure {
                Some(failure) if failure.is_final() => {
                    Step::Fail(failure.to_error(error_for_status))
                }
                _ => Step::Wait,
            },
            // Queued, connecting, transferring or suspended.
            _ => Step::Wait,
        }
    }
}

/// The error BITS recorded for a job.
#[derive(Debug)]
struct JobFailure {
    code: HRESULT,
    /// BITS's own text for the error, if it has one in a language asked for.
    description: Option<String>,
}

impl JobFailure {
    /// The status of the response BITS gave up on: `BG_E_HTTP_ERROR_*` is
    /// 0x8019_0000 with the status in the low word.
    fn http_status(&self) -> Option<StatusCode> {
        let code = self.code.0 as u32;
        if code & 0xFFFF_0000 != 0x8019_0000 {
            return None;
        }
        StatusCode::from_u16((code & 0xFFFF) as u16).ok()
    }

    /// Whether the code is one of Schannel's or of certificate validation
    /// (`FACILITY_SSPI`, `FACILITY_CERT`), which BITS passes on as they are.
    fn is_tls(&self) -> bool {
        matches!(self.code.0 as u32 >> 16, 0x8009 | 0x800B)
    }

    /// Whether the failure would be the same on the next attempt.
    fn is_final(&self) -> bool {
        self.http_status().is_some()
            || matches!(self.to_error(true), Error::Tls { .. } | Error::InvalidUrl)
    }

    fn to_error(&self, error_for_status: bool) -> Error {
        if let Some(status) = self.http_status() {
            let is_error = status.is_client_error() || status.is_server_error();
            // The last two are not `HttpStatus`: the caller wants what a
            // redirect leads to, or this response, and has to fetch it
            // another way.
            return if error_for_status && is_error {
                Error::HttpStatus(status)
            } else if status.is_redirection() {
                Error::Internal(format!("BITS did not follow an HTTP {} redirect", status))
            } else {
                Error::Internal(format!(
                    "BITS does not store the body of an HTTP {} response",
                    status
                ))
            };
        }

        let context = match &self.description {
            Some(description) => format!("BITS job failed: {}", description),
            None => "BITS job failed".to_string(),
        };
        if self.is_tls() {
            return Error::Tls {
                message: format!("{} (0x{:08X})", context, self.code.0 as u32),
            };
        }
        map_windows_error_with_context(windows::core::Error::from_hresult(self.code), &context)
    }
}

/// The thread's membership of the multithreaded apartment, which ends when
/// this is dropped.
struct Apartment {
    // Not `Send`: the apartment has to be left on the thread that joined it.
    _thread: PhantomData<*const ()>,
}

impl Apartment {
    fn enter() -> Result<Self> {
        // SAFETY: CoInitializeEx may be called on any thread, and the
        // reserved pointer is null as it has to be.
        let entered = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
        // S_OK and S_FALSE both have to be undone by `drop`. A failure, such
        // as RPC_E_CHANGED_MODE on a thread that is already in a
        // single-threaded apartment, must not be, and makes no `Apartment`.
        if entered.is_err() {
            return Err(map_windows_error_with_context(
                windows::core::Error::from_hresult(entered),
                "CoInitializeEx",
            ));
        }
        Ok(Self {
            _thread: PhantomData,
        })
    }
}

impl Drop for Apartment {
    fn drop(&mut self) {
        // SAFETY: balances the successful CoInitializeEx in `enter`, on the
        // same thread, since `Apartment` is not `Send`. `run_worker` holds
        // the `Apartment` for longer than any COM interface.
        unsafe { CoUninitialize() };
    }
}

fn run_worker(commands: mpsc::Receiver<Command>, started: Reply<()>) {
    let _apartment = match Apartment::enter() {
        Ok(apartment) => apartment,
        Err(error) => {
            let _ = started.send(Err(error));
            return;
        }
    };
    // Every COM interface lives and dies inside `serve`, so all of them are
    // released before `_apartment` is dropped, also when `serve` panics.
    serve(commands, started);
}

fn serve(commands: mpsc::Receiver<Command>, started: Reply<()>) {
    // SAFETY: the thread is in an apartment (`run_worker`), the class id
    // outlives the call, and no aggregation is asked for. CoCreateInstance
    // hands its caller one reference to the object, which `manager` now owns
    // and releases when dropped.
    let manager: windows::core::Result<IBackgroundCopyManager> =
        unsafe { CoCreateInstance(&BackgroundCopyManager, None, CLSCTX_LOCAL_SERVER) };
    let manager = match manager {
        Ok(manager) => manager,
        Err(error) => {
            let _ = started.send(Err(map_windows_error_with_context(
                error,
                "Failed to connect to BITS",
            )));
            return;
        }
    };
    if started.send(Ok(())).is_err() {
        return;
    }

    let mut worker = Worker {
        manager,
        jobs: HashMap::new(),
    };
    while let Ok(command) = commands.recv() {
        worker.handle(command);
    }
    // Every sender is gone. A job still here was waited for by a future that
    // was leaked instead of dropped, and can no longer be completed.
    for (id, job) in worker.jobs {
        cancel_job(id, &job);
    }
}

/// The COM interfaces a [`BitsDownloadManager`] owns.
///
/// Invariant, on which the `SAFETY` comments below rely: a `Worker` exists
/// only inside [`serve`], so its interfaces are only ever used on the worker
/// thread, while that thread is in the apartment it obtained them in.
struct Worker {
    manager: IBackgroundCopyManager,
    jobs: HashMap<GUID, IBackgroundCopyJob>,
}

impl Worker {
    fn handle(&mut self, command: Command) {
        match command {
            Command::Create { spec, reply } => {
                // An id that comes back unsent belongs to a caller that
                // stopped waiting before it knew it, so nothing else will
                // ever cancel that job.
                if let Err(Ok(id)) = reply.send(self.create(spec)) {
                    self.cancel(id);
                }
            }
            Command::Poll { id, reply } => {
                let _ = reply.send(self.poll(id));
            }
            Command::Complete { id, reply } => {
                let _ = reply.send(self.complete(id));
            }
            Command::Cancel { id, reply } => {
                self.cancel(id);
                if let Some(reply) = reply {
                    let _ = reply.send(Ok(()));
                }
            }
        }
    }

    fn job(&self, id: GUID) -> Result<&IBackgroundCopyJob> {
        self.jobs
            .get(&id)
            .ok_or_else(|| Error::Internal("The BITS job is no longer tracked".to_string()))
    }

    fn create(&mut self, spec: JobSpec) -> Result<GUID> {
        let mut id = GUID::zeroed();
        let mut created: Option<IBackgroundCopyJob> = None;
        // SAFETY: `self.manager` is live (see `Worker`). The name is a
        // NUL-terminated string that lives until the call returns, and `id`
        // and `created` are valid for the writes. BITS writes one reference
        // to the new job into `created`, which has the layout of a nullable
        // interface pointer and releases the reference when dropped.
        unsafe {
            self.manager.CreateJob(
                &HSTRING::from(spec.display_name.as_str()),
                BG_JOB_TYPE_DOWNLOAD,
                &mut id,
                &mut created,
            )
        }
        .map_err(|e| bits_error(e, "Failed to create BITS job"))?;
        let handle = created.ok_or_else(|| {
            Error::Internal("BITS created a job without an interface".to_string())
        })?;

        if let Err(error) = start_job(&handle, &spec) {
            cancel_job(id, &handle);
            return Err(error);
        }
        self.jobs.insert(id, handle);
        Ok(id)
    }

    fn poll(&self, id: GUID) -> Result<JobStatus> {
        let job = self.job(id)?;
        // SAFETY: `job` is live (see `Worker`); the call takes nothing else.
        let state =
            unsafe { job.GetState() }.map_err(|e| bits_error(e, "Failed to get BITS job state"))?;
        let mut progress = BG_JOB_PROGRESS::default();
        // SAFETY: `job` is live (see `Worker`), and `progress` is valid for
        // the write.
        unsafe { job.GetProgress(&mut progress) }
            .map_err(|e| bits_error(e, "Failed to get BITS job progress"))?;
        let failure = match state {
            BG_JOB_STATE_ERROR | BG_JOB_STATE_TRANSIENT_ERROR => read_failure(job),
            _ => None,
        };
        Ok(JobStatus {
            state,
            bytes_transferred: progress.BytesTransferred,
            bytes_total: (progress.BytesTotal != SIZE_UNKNOWN).then_some(progress.BytesTotal),
            failure,
        })
    }

    fn complete(&mut self, id: GUID) -> Result<()> {
        let job = self.job(id)?;
        // Completing a job that is not transferred ends it without its
        // file, and reports success.
        // SAFETY: `job` is live (see `Worker`); the call takes nothing else.
        let state =
            unsafe { job.GetState() }.map_err(|e| bits_error(e, "Failed to get BITS job state"))?;
        if state != BG_JOB_STATE_TRANSFERRED {
            return Err(Error::Internal(format!(
                "BITS job is not transferred (state {})",
                state.0
            )));
        }

        // This renames BITS's temporary file to the destination, over a
        // file that is already there.
        // SAFETY: `job` is live (see `Worker`); the call takes nothing else.
        unsafe { job.Complete() }.map_err(|e| bits_error(e, "Failed to complete BITS job"))?;
        // Complete also has success codes for a file that was not renamed,
        // which the binding does not tell from S_OK. The state does: the job
        // is only acknowledged once every file is in place.
        // SAFETY: as for the first GetState.
        let state =
            unsafe { job.GetState() }.map_err(|e| bits_error(e, "Failed to get BITS job state"))?;
        if state != BG_JOB_STATE_ACKNOWLEDGED {
            return Err(Error::Internal(
                "BITS did not move the download into place".to_string(),
            ));
        }
        self.jobs.remove(&id);
        Ok(())
    }

    fn cancel(&mut self, id: GUID) {
        if let Some(job) = self.jobs.remove(&id) {
            cancel_job(id, &job);
        }
    }
}

/// Give a new job its headers and its file, and start it.
///
/// `job` has to be live and on its own thread, as a [`Worker`]'s jobs are.
fn start_job(job: &IBackgroundCopyJob, spec: &JobSpec) -> Result<()> {
    if let Some(headers) = &spec.headers {
        // A job that cannot take the headers must not run without them.
        let options: IBackgroundCopyJobHttpOptions = job
            .cast()
            .map_err(|e| bits_error(e, "This version of BITS cannot send request headers"))?;
        // SAFETY: `options` is live, queried from `job` just above on this
        // thread. The headers are a NUL-terminated string that lives until
        // the call returns; BITS copies them.
        unsafe { options.SetCustomHeaders(&HSTRING::from(headers.as_str())) }
            .map_err(|e| bits_error(e, "Failed to set BITS request headers"))?;
        // BITS repeats the headers on every request of the job, a redirected
        // one included, wherever it leads.
        // SAFETY: `options` is live, as above; the call takes only flags.
        unsafe { options.SetSecurityFlags(BG_HTTP_REDIRECT_POLICY_DISALLOW) }
            .map_err(|e| bits_error(e, "Failed to set BITS redirect policy"))?;
        // The headers can be credentials, and any program of this user can
        // read a job's headers back. Since Windows 10 1809 BITS can refuse
        // that; before, there is no such interface and nothing to do.
        if let Ok(options) = job.cast::<IBackgroundCopyJobHttpOptions3>() {
            // SAFETY: `options` is live, queried from `job` just above on
            // this thread; the call takes nothing else.
            let _ = unsafe { options.MakeCustomHeadersWriteOnly() };
        }
    }

    // SAFETY: `job` is live (see above). Both names are NUL-terminated
    // strings that live until the call returns; BITS copies them.
    unsafe {
        job.AddFile(
            &HSTRING::from(spec.url.as_str()),
            &HSTRING::from(spec.local_path.as_path()),
        )
    }
    .map_err(|e| bits_error(e, "Failed to add file to BITS job"))?;
    // A background priority: BITS gives way to the user's other traffic.
    // SAFETY: `job` is live (see above); the call takes only the priority.
    unsafe { job.SetPriority(BG_JOB_PRIORITY_NORMAL) }
        .map_err(|e| bits_error(e, "Failed to set BITS job priority"))?;
    // A job is created suspended.
    // SAFETY: `job` is live (see above); the call takes nothing else.
    unsafe { job.Resume() }.map_err(|e| bits_error(e, "Failed to start BITS job"))?;
    Ok(())
}

/// Cancel a job, which removes it from the queue and deletes its temporary
/// file.
///
/// `job` has to be live and on its own thread, as a [`Worker`]'s jobs are.
fn cancel_job(id: GUID, job: &IBackgroundCopyJob) {
    // SAFETY: `job` is live (see above); the call takes nothing else.
    if let Err(error) = unsafe { job.Cancel() } {
        // A job that is already acknowledged or cancelled has nothing left
        // to cancel.
        if error.code() == HRESULT(BG_E_INVALID_STATE) {
            return;
        }
        note_refusal(&error);
        // Nothing more can be done from here, so name the job for whoever
        // can remove it with `bitsadmin /cancel`.
        tracing::warn!("BITS job {:?} is left in the queue: {}", id, error);
    }
}

/// The error BITS holds for a job in an error state. `None` once BITS has
/// let go of it, which it does when it starts transferring again.
///
/// `job` has to be live and on its own thread, as a [`Worker`]'s jobs are.
fn read_failure(job: &IBackgroundCopyJob) -> Option<JobFailure> {
    // SAFETY: `job` is live (see above). GetError hands its caller one
    // reference to the error object, which `error` owns and releases when
    // dropped, here on this thread.
    let error = unsafe { job.GetError() }.ok()?;
    let mut context = BG_ERROR_CONTEXT_NONE;
    let mut code = HRESULT(0);
    // SAFETY: `error` is live, obtained just above on this thread, and
    // `context` and `code` are valid for the writes.
    unsafe { error.GetError(&mut context, &mut code) }.ok()?;
    Some(JobFailure {
        code,
        description: describe(&error),
    })
}

/// BITS's text for an error, in the thread's language or else in English.
///
/// `error` has to be live and on its own thread.
fn describe(error: &IBackgroundCopyError) -> Option<String> {
    // SAFETY: GetThreadLocale has no preconditions.
    let thread_language = unsafe { GetThreadLocale() } & 0xFFFF;
    [thread_language, LANG_EN_US]
        .into_iter()
        .find_map(|language| {
            // SAFETY: `error` is live (see above). On success BITS returns a
            // string from the COM task allocator that the caller has to
            // free, which `TaskString` does.
            let text = TaskString(unsafe { error.GetErrorDescription(language) }.ok()?);
            text.trimmed()
        })
}

/// A string a COM method allocated for its caller, freed when dropped.
struct TaskString(PWSTR);

impl TaskString {
    /// The text without the line break BITS ends it with, if any is left.
    fn trimmed(&self) -> Option<String> {
        if self.0.is_null() {
            return None;
        }
        // SAFETY: not null, and NUL-terminated by the method that returned
        // it; the allocation lives until `self` is dropped.
        let text = String::from_utf16_lossy(unsafe { self.0.as_wide() });
        let text = text.trim();
        (!text.is_empty()).then(|| text.to_string())
    }
}

impl Drop for TaskString {
    fn drop(&mut self) {
        // SAFETY: the pointer came from the COM task allocator and is freed
        // only here; CoTaskMemFree accepts null.
        unsafe { CoTaskMemFree(Some(self.0.as_ptr() as *const c_void)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use windows::Win32::Networking::BackgroundIntelligentTransferService::{
        BG_E_HTTP_ERROR_304, BG_E_HTTP_ERROR_404, BG_E_HTTP_ERROR_503,
        BG_E_INSUFFICIENT_RANGE_SUPPORT, BG_JOB_STATE_CONNECTING, BG_JOB_STATE_QUEUED,
        BG_JOB_STATE_SUSPENDED, BG_JOB_STATE_TRANSFERRING,
    };

    /// `HRESULT_FROM_WIN32(ERROR_WINHTTP_NAME_NOT_RESOLVED)`.
    const NAME_NOT_RESOLVED: i32 = 0x8007_2EE7_u32 as i32;
    /// `HRESULT_FROM_WIN32(ERROR_WINHTTP_SECURE_FAILURE)`.
    const SECURE_FAILURE: i32 = 0x8007_2F8F_u32 as i32;
    /// `CERT_E_UNTRUSTEDROOT`.
    const UNTRUSTED_ROOT: i32 = 0x800B_0109_u32 as i32;

    fn failure(code: i32) -> JobFailure {
        JobFailure {
            code: HRESULT(code),
            description: None,
        }
    }

    fn status(state: BG_JOB_STATE, code: Option<i32>) -> JobStatus {
        JobStatus {
            state,
            bytes_transferred: 0,
            bytes_total: None,
            failure: code.map(failure),
        }
    }

    fn cancelled(inbox: &mpsc::Receiver<Command>) -> Option<GUID> {
        match inbox.try_recv() {
            Ok(Command::Cancel { id, .. }) => Some(id),
            _ => None,
        }
    }

    #[test]
    fn accounts_are_the_same_whatever_the_case_of_their_names() {
        let account = |domain: &str, name: &str| Account {
            domain: domain.to_string(),
            name: name.to_string(),
        };
        assert!(account("DESKTOP", "admin").is(&account("desktop", "Admin")));
        assert!(!account("DESKTOP", "admin").is(&account("DESKTOP", "other")));
        assert!(!account("DESKTOP", "admin").is(&account("DOMAIN", "admin")));
        // Session 0 has no user, which is nobody and not a nameless somebody.
        assert!(!account("", "").is(&account("", "")));
    }

    #[test]
    fn job_guard_cancels_its_job_when_dropped() {
        let id = GUID::from_u128(1);
        let (commands, inbox) = mpsc::channel();
        drop(JobGuard {
            commands: &commands,
            id,
        });
        assert_eq!(cancelled(&inbox), Some(id));
    }

    #[test]
    fn abandoned_wait_for_a_job_id_cancels_the_job() {
        let id = GUID::from_u128(2);
        let (commands, inbox) = mpsc::channel();

        // The worker replied, and nobody took the reply.
        let (reply, response) = oneshot::channel();
        assert!(reply.send(Ok(id)).is_ok());
        drop(PendingJob {
            commands: &commands,
            response,
        });
        assert_eq!(cancelled(&inbox), Some(id));

        // The worker had not replied yet: its reply fails, which is what
        // makes it cancel the job itself.
        let (reply, response) = oneshot::channel();
        drop(PendingJob {
            commands: &commands,
            response,
        });
        assert!(reply.send(Ok(id)).is_err());
        assert_eq!(cancelled(&inbox), None);
    }

    #[test]
    fn wait_for_a_job_id_that_was_taken_cancels_nothing() {
        let id = GUID::from_u128(3);
        let (commands, inbox) = mpsc::channel();
        let (reply, response) = oneshot::channel();
        let mut pending = PendingJob {
            commands: &commands,
            response,
        };
        assert!(reply.send(Ok(id)).is_ok());
        assert!(matches!(pending.response.try_recv(), Ok(Ok(taken)) if taken == id));
        drop(pending);
        assert_eq!(cancelled(&inbox), None);
    }

    #[test]
    fn http_status_is_read_from_bits_http_errors_only() {
        assert_eq!(
            failure(BG_E_HTTP_ERROR_404).http_status(),
            Some(StatusCode::NOT_FOUND)
        );
        assert_eq!(
            failure(BG_E_HTTP_ERROR_503).http_status(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(failure(BG_E_INSUFFICIENT_RANGE_SUPPORT).http_status(), None);
        assert_eq!(failure(NAME_NOT_RESOLVED).http_status(), None);
        // Not a status the `http` crate accepts.
        assert_eq!(failure(0x8019_0000_u32 as i32).http_status(), None);
    }

    #[test]
    fn error_status_is_http_status_only_when_asked_for() {
        assert!(matches!(
            failure(BG_E_HTTP_ERROR_404).to_error(true),
            Error::HttpStatus(StatusCode::NOT_FOUND)
        ));
        assert!(matches!(
            failure(BG_E_HTTP_ERROR_404).to_error(false),
            Error::Internal(_)
        ));
        assert!(matches!(
            failure(BG_E_HTTP_ERROR_304).to_error(true),
            Error::Internal(_)
        ));
    }

    #[test]
    fn transport_failures_keep_their_kind() {
        assert!(matches!(
            failure(NAME_NOT_RESOLVED).to_error(true),
            Error::Network { code: 12007, .. }
        ));
        assert!(matches!(
            failure(SECURE_FAILURE).to_error(true),
            Error::Tls { .. }
        ));
        assert!(matches!(
            failure(UNTRUSTED_ROOT).to_error(true),
            Error::Tls { .. }
        ));
        assert!(matches!(
            failure(BG_E_INSUFFICIENT_RANGE_SUPPORT).to_error(true),
            Error::Internal(_)
        ));
    }

    #[test]
    fn description_is_part_of_the_message() {
        let failure = JobFailure {
            code: HRESULT(BG_E_INSUFFICIENT_RANGE_SUPPORT),
            description: Some("The server does not support ranges.".to_string()),
        };
        let Error::Internal(message) = failure.to_error(true) else {
            panic!("expected an internal error");
        };
        assert!(message.starts_with("BITS job failed: The server does not support ranges."));
    }

    #[test]
    fn progress_has_a_total_once_bits_knows_it_or_the_file_is_transferred() {
        let mut job = status(BG_JOB_STATE_TRANSFERRING, None);
        job.bytes_transferred = 10;
        assert_eq!(job.progress(), (10, None));
        job.bytes_total = Some(40);
        assert_eq!(job.progress(), (10, Some(40)));

        let mut job = status(BG_JOB_STATE_TRANSFERRED, None);
        job.bytes_transferred = 40;
        assert_eq!(job.progress(), (40, Some(40)));
    }

    #[test]
    fn states_in_progress_wait() {
        for state in [
            BG_JOB_STATE_QUEUED,
            BG_JOB_STATE_CONNECTING,
            BG_JOB_STATE_TRANSFERRING,
            BG_JOB_STATE_SUSPENDED,
        ] {
            assert!(matches!(status(state, None).next_step(true), Step::Wait));
        }
    }

    #[test]
    fn finished_states_end_the_wait() {
        assert!(matches!(
            status(BG_JOB_STATE_TRANSFERRED, None).next_step(true),
            Step::Complete
        ));
        assert!(matches!(
            status(BG_JOB_STATE_ACKNOWLEDGED, None).next_step(true),
            Step::Done
        ));
        assert!(matches!(
            status(BG_JOB_STATE_CANCELLED, None).next_step(true),
            Step::Fail(Error::Cancelled)
        ));
        assert!(matches!(
            status(BG_JOB_STATE_ERROR, Some(BG_E_HTTP_ERROR_404)).next_step(true),
            Step::Fail(Error::HttpStatus(StatusCode::NOT_FOUND))
        ));
        assert!(matches!(
            status(BG_JOB_STATE_ERROR, None).next_step(true),
            Step::Fail(Error::Internal(_))
        ));
    }

    #[test]
    fn transient_errors_wait_unless_waiting_cannot_help() {
        assert!(matches!(
            status(BG_JOB_STATE_TRANSIENT_ERROR, None).next_step(true),
            Step::Wait
        ));
        assert!(matches!(
            status(BG_JOB_STATE_TRANSIENT_ERROR, Some(NAME_NOT_RESOLVED)).next_step(true),
            Step::Wait
        ));
        assert!(matches!(
            status(BG_JOB_STATE_TRANSIENT_ERROR, Some(BG_E_HTTP_ERROR_503)).next_step(true),
            Step::Fail(Error::HttpStatus(StatusCode::SERVICE_UNAVAILABLE))
        ));
        assert!(matches!(
            status(BG_JOB_STATE_TRANSIENT_ERROR, Some(BG_E_HTTP_ERROR_503)).next_step(false),
            Step::Fail(Error::Internal(_))
        ));
        assert!(matches!(
            status(BG_JOB_STATE_TRANSIENT_ERROR, Some(SECURE_FAILURE)).next_step(true),
            Step::Fail(Error::Tls { .. })
        ));
    }

    #[test]
    fn header_block_is_crlf_terminated_lines() {
        assert_eq!(header_block(&HeaderMap::new()).unwrap(), None);

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer token"),
        );
        headers.append("x-tag", HeaderValue::from_static("one"));
        headers.append("x-tag", HeaderValue::from_static("two"));
        let block = header_block(&headers).unwrap().unwrap();
        assert!(block.ends_with("\r\n"));
        // `HeaderMap` does not promise an order between names.
        let mut lines: Vec<&str> = block.split_terminator("\r\n").collect();
        lines.sort_unstable();
        assert_eq!(
            lines,
            ["authorization: Bearer token", "x-tag: one", "x-tag: two"]
        );
    }

    #[test]
    fn header_block_refuses_what_bits_cannot_send() {
        let mut ranged = HeaderMap::new();
        ranged.insert(http::header::RANGE, HeaderValue::from_static("bytes=0-9"));
        assert!(header_block(&ranged).is_err());

        let mut opaque = HeaderMap::new();
        opaque.insert("x-name", HeaderValue::from_bytes(b"caf\xE9").unwrap());
        assert!(header_block(&opaque).is_err());

        // "x-long: " and the CRLF are 10 units, and the terminator one more.
        let mut headers = HeaderMap::new();
        let fits = "a".repeat(MAX_CUSTOM_HEADERS - 11);
        headers.insert("x-long", HeaderValue::from_str(&fits).unwrap());
        assert!(header_block(&headers).is_ok());
        let too_long = "a".repeat(MAX_CUSTOM_HEADERS - 10);
        headers.insert("x-long", HeaderValue::from_str(&too_long).unwrap());
        assert!(header_block(&headers).is_err());
    }

    #[test]
    fn display_name_is_cut_to_the_limit() {
        assert_eq!(display_name(Some("updates".to_string())), "updates");
        assert!(display_name(None).starts_with("frakt-download-"));

        let long = "a".repeat(MAX_DISPLAY_NAME + 10);
        assert_eq!(display_name(Some(long)).len(), MAX_DISPLAY_NAME);
        // Two UTF-16 units each, and never half of one.
        let wide = "\u{1F600}".repeat(MAX_DISPLAY_NAME);
        let name = display_name(Some(wide));
        assert_eq!(name.encode_utf16().count(), MAX_DISPLAY_NAME);
    }

    #[test]
    fn local_path_is_absolute_and_plain() {
        let relative = local_path(Path::new("downloads/file.bin")).unwrap();
        assert!(relative.is_absolute());
        assert!(relative.ends_with(r"downloads\file.bin"));

        let forward = local_path(Path::new("C:/downloads/file.bin")).unwrap();
        assert_eq!(forward.as_os_str(), r"C:\downloads\file.bin");

        assert!(local_path(Path::new(r"\\?\C:\downloads\file.bin")).is_err());
    }

    #[test]
    fn job_spec_refuses_other_schemes() {
        let url = Url::parse("ftp://example.com/file.bin").unwrap();
        let spec = JobSpec::new(&url, Path::new(r"C:\file.bin"), None, &HeaderMap::new());
        assert!(matches!(spec, Err(Error::InvalidUrl)));

        let url = Url::parse("https://example.com/file.bin").unwrap();
        let spec = JobSpec::new(&url, Path::new(r"C:\file.bin"), None, &HeaderMap::new()).unwrap();
        assert_eq!(spec.url, "https://example.com/file.bin");
        assert_eq!(spec.headers, None);
    }

    /// Tests that put real jobs in the user's BITS queue, so they only run
    /// when asked for: `cargo test bits::tests::live -- --ignored`.
    mod live {
        use super::*;
        use std::io::{BufRead, BufReader, Write};
        use std::net::{SocketAddr, TcpListener, TcpStream};
        use std::sync::{Arc, Mutex};

        /// BITS starts a background job when it sees fit.
        const LIMIT: Duration = Duration::from_secs(120);

        /// A file server of the kind BITS asks for: HEAD and byte ranges.
        /// `/missing` is not found, `/redirect` redirects to the file, and
        /// `/stall` never finishes its body.
        struct Server {
            address: SocketAddr,
            /// The head of every request, in lower case.
            requests: Arc<Mutex<Vec<String>>>,
        }

        impl Server {
            fn start(body: Vec<u8>) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let requests = Arc::new(Mutex::new(Vec::new()));
                let (body, seen) = (Arc::new(body), requests.clone());
                std::thread::spawn(move || {
                    for stream in listener.incoming().flatten() {
                        let (body, seen) = (body.clone(), seen.clone());
                        std::thread::spawn(move || answer(stream, &body, &seen));
                    }
                });
                Self { address, requests }
            }

            fn url(&self, path: &str) -> Url {
                Url::parse(&format!("http://{}{}", self.address, path)).unwrap()
            }
        }

        fn answer(
            mut stream: TcpStream,
            body: &[u8],
            seen: &Mutex<Vec<String>>,
        ) -> std::io::Result<()> {
            let mut reader = BufReader::new(stream.try_clone()?);
            let mut head = String::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line)? == 0 || line == "\r\n" {
                    break;
                }
                head.push_str(&line.to_ascii_lowercase());
            }
            seen.lock().unwrap().push(head.clone());

            let mut words = head.split_whitespace();
            let (method, path) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
            if path == "/missing" {
                return stream.write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
            if path == "/redirect" {
                return stream.write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /file.bin\r\nContent-Length: 0\r\n\
                      Connection: close\r\n\r\n",
                );
            }

            let end = body.len() - 1;
            let range = head
                .lines()
                .find_map(|line| line.strip_prefix("range: bytes="))
                .and_then(|range| {
                    let (first, last) = range.trim().split_once('-')?;
                    let last = last.parse().unwrap_or(end).min(end);
                    Some((first.parse::<usize>().ok()?.min(last), last))
                });
            let (first, last) = range.unwrap_or((0, end));
            let mut response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\
                 Last-Modified: Thu, 01 Jan 2026 00:00:00 GMT\r\nConnection: close\r\n",
                if range.is_some() {
                    "206 Partial Content"
                } else {
                    "200 OK"
                },
                last - first + 1,
            );
            if range.is_some() {
                response.push_str(&format!(
                    "Content-Range: bytes {}-{}/{}\r\n",
                    first,
                    last,
                    body.len()
                ));
            }
            response.push_str("\r\n");
            stream.write_all(response.as_bytes())?;
            if method == "head" {
                return Ok(());
            }
            if path == "/stall" {
                stream.write_all(&body[first..first + 1])?;
                std::thread::sleep(LIMIT);
                return Ok(());
            }
            stream.write_all(&body[first..=last])
        }

        fn body() -> Vec<u8> {
            (0..300_000_u32).map(|index| (index % 251) as u8).collect()
        }

        /// A display name no other job has.
        fn job_name(test: &str) -> String {
            format!("frakt live test {} {}", std::process::id(), test)
        }

        /// The manager, or `None` where BITS does not act for whoever runs
        /// the tests, as over SSH: all there is to test then is that no job
        /// gets made.
        async fn manager() -> Option<BitsDownloadManager> {
            match BitsDownloadManager::new().await {
                Ok(manager) => Some(manager),
                Err(error) => {
                    assert!(logon_can_use_bits().is_err(), "{error}");
                    eprintln!("BITS is not tested: {error}");
                    None
                }
            }
        }

        /// How many jobs with the display name this user has in the queue.
        /// BITS answers that for a user who is not logged on, too. It does
        /// not list a job that was completed or cancelled; Wine's does, so
        /// those are not counted.
        fn jobs_named(name: &str) -> usize {
            let name = name.to_string();
            let count = move || {
                let _apartment = Apartment::enter().unwrap();
                // SAFETY: as in `serve`.
                let manager: IBackgroundCopyManager =
                    unsafe { CoCreateInstance(&BackgroundCopyManager, None, CLSCTX_LOCAL_SERVER) }
                        .unwrap();
                // SAFETY: `manager` is live on this thread; the enumerator
                // it returns is owned by `jobs`.
                let jobs = unsafe { manager.EnumJobs(0) }.unwrap();
                let mut count = 0;
                loop {
                    let mut job = [None];
                    let mut fetched = 0;
                    // SAFETY: `jobs` is live on this thread, and the slice
                    // and `fetched` are valid for the writes. A job written
                    // into the slice is owned by it.
                    unsafe { jobs.Next(&mut job, &mut fetched) }.unwrap();
                    let [Some(job)] = job else { break };
                    // SAFETY: `job` is live on this thread; the call takes
                    // nothing else.
                    let state = unsafe { job.GetState() }.unwrap();
                    if state == BG_JOB_STATE_ACKNOWLEDGED || state == BG_JOB_STATE_CANCELLED {
                        continue;
                    }
                    // SAFETY: `job` is live on this thread; the string it
                    // returns is the caller's to free, as `TaskString` does.
                    let display = TaskString(unsafe { job.GetDisplayName() }.unwrap());
                    if display.trimmed().as_deref() == Some(name.as_str()) {
                        count += 1;
                    }
                }
                count
            };
            std::thread::spawn(count).join().unwrap()
        }

        /// What is in the directory: the download, and BITS's temporary file
        /// for as long as there is a job.
        fn files_in(directory: &Path) -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(directory)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names
        }

        #[tokio::test]
        #[ignore = "puts jobs in the BITS queue"]
        async fn download_completes_and_replaces_the_destination() {
            let Some(manager) = manager().await else {
                return;
            };
            let server = Server::start(body());
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("file.bin");
            std::fs::write(&destination, b"stale").unwrap();
            let name = job_name("complete");
            let mut headers = HeaderMap::new();
            headers.insert("x-frakt-test", HeaderValue::from_static("live"));
            let reports = Arc::new(Mutex::new(Vec::new()));
            let progress = {
                let reports = reports.clone();
                move |done, total| reports.lock().unwrap().push((done, total))
            };

            let download = manager.start_background_download(
                server.url("/file.bin"),
                destination.clone(),
                Some(name.clone()),
                &headers,
                Some(&progress),
                true,
            );
            let response = tokio::time::timeout(LIMIT, download)
                .await
                .expect("BITS did not finish in time")
                .unwrap();
            drop(manager);

            let length = body().len() as u64;
            assert_eq!(response.bytes_downloaded, length);
            assert_eq!(std::fs::read(&destination).unwrap(), body());
            assert_eq!(
                reports.lock().unwrap().last(),
                Some(&(length, Some(length)))
            );
            let requests = server.requests.lock().unwrap();
            assert!(
                requests
                    .iter()
                    .any(|head| head.contains("x-frakt-test: live"))
            );
            assert_eq!(files_in(directory.path()), ["file.bin"]);
            assert_eq!(jobs_named(&name), 0);
        }

        #[tokio::test]
        #[ignore = "puts jobs in the BITS queue"]
        async fn error_status_fails_the_download() {
            let Some(manager) = manager().await else {
                return;
            };
            let server = Server::start(body());
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("file.bin");
            let name = job_name("status");
            let headers = HeaderMap::new();

            for error_for_status in [true, false] {
                let download = manager.start_background_download(
                    server.url("/missing"),
                    destination.clone(),
                    Some(name.clone()),
                    &headers,
                    None,
                    error_for_status,
                );
                let error = tokio::time::timeout(LIMIT, download)
                    .await
                    .expect("BITS did not finish in time")
                    .unwrap_err();
                assert_eq!(
                    matches!(error, Error::HttpStatus(StatusCode::NOT_FOUND)),
                    error_for_status,
                    "{error:?}"
                );
            }
            drop(manager);
            assert!(files_in(directory.path()).is_empty());
            assert_eq!(jobs_named(&name), 0);
        }

        #[tokio::test]
        #[ignore = "puts jobs in the BITS queue"]
        async fn redirect_is_followed_only_without_headers() {
            let Some(manager) = manager().await else {
                return;
            };
            let server = Server::start(body());
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("file.bin");
            let name = job_name("redirect");
            let mut headers = HeaderMap::new();
            headers.insert("x-frakt-test", HeaderValue::from_static("live"));

            let download = manager.start_background_download(
                server.url("/redirect"),
                destination.clone(),
                Some(name.clone()),
                &headers,
                None,
                true,
            );
            let error = tokio::time::timeout(LIMIT, download)
                .await
                .expect("BITS did not finish in time")
                .unwrap_err();
            assert!(!matches!(error, Error::HttpStatus(_)), "{error:?}");
            assert!(files_in(directory.path()).is_empty());

            headers.clear();
            let download = manager.start_background_download(
                server.url("/redirect"),
                destination.clone(),
                Some(name.clone()),
                &headers,
                None,
                true,
            );
            tokio::time::timeout(LIMIT, download)
                .await
                .expect("BITS did not finish in time")
                .unwrap();
            drop(manager);
            assert_eq!(std::fs::read(&destination).unwrap(), body());
            assert_eq!(jobs_named(&name), 0);
        }

        #[tokio::test]
        #[ignore = "puts jobs in the BITS queue"]
        async fn dropped_download_is_cancelled() {
            let Some(manager) = manager().await else {
                return;
            };
            let server = Server::start(body());
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("file.bin");
            let name = job_name("drop");
            let headers = HeaderMap::new();

            let download = manager.start_background_download(
                server.url("/stall"),
                destination,
                Some(name.clone()),
                &headers,
                None,
                true,
            );
            let waited = tokio::time::timeout(Duration::from_secs(5), download).await;
            assert!(waited.is_err(), "{waited:?}");
            // Joins the worker, so the cancellation has reached BITS.
            drop(manager);

            // Neither the file nor the temporary file BITS keeps beside it.
            assert!(files_in(directory.path()).is_empty());
            assert_eq!(jobs_named(&name), 0);
        }

        /// The whole way from the client, which has to deliver the file
        /// whether BITS acts for this user or not, and leave no job behind
        /// either way.
        #[tokio::test]
        #[ignore = "puts jobs in the BITS queue"]
        async fn client_downloads_with_or_without_bits() {
            let server = Server::start(body());
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("file.bin");
            let name = job_name("client");

            let client = crate::Client::builder()
                .backend(crate::client::BackendType::Windows)
                .build()
                .unwrap();
            let download = client
                .download_background(server.url("/file.bin"), &destination)
                .unwrap()
                .session_identifier(name.clone())
                .send();
            let response = tokio::time::timeout(LIMIT, download)
                .await
                .expect("the download did not finish in time")
                .unwrap();

            assert_eq!(response.bytes_downloaded, body().len() as u64);
            assert_eq!(std::fs::read(&destination).unwrap(), body());
            assert_eq!(files_in(directory.path()), ["file.bin"]);
            assert_eq!(jobs_named(&name), 0);
            let by_bits = server
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|head| head.contains("user-agent: microsoft bits"));
            assert_eq!(by_bits, logon_can_use_bits().is_ok());
        }
    }
}
