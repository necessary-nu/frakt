//! Background downloads in a process that outlives the host: the host's own
//! executable, started again as a helper that runs one download.
//!
//! The library has no executable of its own, so the host opts in by calling
//! [`enable`] first thing in `main`. In a process started as the helper that
//! call never returns: it reads one job from stdin, downloads, reports on
//! stdout and exits. In any other process it records that the executable can
//! be started that way.
//!
//! The helper is a new program image, not a fork of a multi-threaded
//! process, and it is handed plain data only: it builds its own runtime and
//! client. The job holds the request's headers, credentials included, so it
//! travels over the pipe and nowhere else.

use super::background::{self, Download, Event, Finished};
use super::{ClientSettings, build_client};
use crate::{Error, Result};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::pipe;

/// Set to `1` in the environment of a process started as the helper.
const HELPER_ENV: &str = "FRAKT_BACKGROUND_HELPER";

/// With this set too, the helper first reports which descriptors it has
/// open, so the tests can see that it kept none of the host's.
const DESCRIPTORS_ENV: &str = "FRAKT_BACKGROUND_HELPER_DESCRIPTORS";

/// How long a started process has to say that it is a helper.
const READY_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the helper waits for its last message to be read, and how long
/// the parent waits for one from a helper that has already exited.
const LAST_WORD_TIMEOUT: Duration = Duration::from_secs(5);

/// The helper reports progress no more often than this.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

const JOB_LIMIT: u64 = 16 << 20;
const MESSAGE_LIMIT: u64 = 1 << 20;

/// Whether this process called [`enable`], so its executable can be started
/// as the helper.
static AVAILABLE: AtomicBool = AtomicBool::new(false);

type Headers = Vec<(String, Vec<u8>)>;

/// What the helper is told to do, as one line of JSON on its stdin.
#[derive(Serialize, Deserialize)]
struct Job {
    url: String,
    headers: Headers,
    destination: Vec<u8>,
    error_for_status: bool,
    client: ClientSettings,
}

/// What the helper says, one JSON object per line on its stdout.
#[derive(Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Message {
    /// Sent first, and only if [`DESCRIPTORS_ENV`] asks for it.
    Descriptors {
        open: Vec<i32>,
    },
    /// The job was understood and the download is the helper's from here.
    Ready,
    Started {
        status: u16,
        headers: Headers,
        total: Option<u64>,
    },
    Progress {
        downloaded: u64,
        total: Option<u64>,
    },
    Completed {
        status: u16,
        headers: Headers,
        bytes: u64,
    },
    Failed {
        error: Failure,
    },
}

/// The errors a download ends with, in a form that crosses the pipe.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Failure {
    HttpStatus { status: u16 },
    Timeout,
    Network { code: i64, message: String },
    Tls { message: String },
    Io { message: String },
    Internal { message: String },
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            Error::HttpStatus(status) => Failure::HttpStatus {
                status: status.as_u16(),
            },
            Error::Timeout => Failure::Timeout,
            Error::Network { code, message } => Failure::Network { code, message },
            Error::Tls { message } => Failure::Tls { message },
            Error::Io(message) => Failure::Io { message },
            Error::Internal(message) => Failure::Internal { message },
            other => Failure::Internal {
                message: other.to_string(),
            },
        }
    }
}

impl From<Failure> for Error {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::HttpStatus { status } => match StatusCode::from_u16(status) {
                Ok(status) => Error::HttpStatus(status),
                Err(_) => Error::Internal(format!("HTTP error: {}", status)),
            },
            Failure::Timeout => Error::Timeout,
            Failure::Network { code, message } => Error::Network { code, message },
            Failure::Tls { message } => Error::Tls { message },
            Failure::Io { message } => Error::Io(message),
            Failure::Internal { message } => Error::Internal(message),
        }
    }
}

fn to_wire(headers: &HeaderMap) -> Headers {
    headers
        .iter()
        .map(|(name, value)| (name.as_str().to_string(), value.as_bytes().to_vec()))
        .collect()
}

fn from_wire(headers: &[(String, Vec<u8>)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(value),
        ) {
            map.append(name, value);
        }
    }
    map
}

/// What `frakt::enable_background_helper` does on this backend.
pub(super) fn enable() {
    // A set-uid or set-gid program must not let its environment choose what
    // it does, and starting it again would not give a helper either.
    let privileged =
        unsafe { libc::getuid() != libc::geteuid() || libc::getgid() != libc::getegid() };
    if privileged {
        return;
    }
    if std::env::var_os(HELPER_ENV).is_some_and(|value| value == "1") {
        // On a thread of its own, as the host may have called from inside a
        // runtime (the body of a `#[tokio::main]` function, say), where a
        // second one cannot be started.
        let served = std::thread::spawn(serve).join();
        std::process::exit(served.unwrap_or(2));
    }
    AVAILABLE.store(true, Ordering::Relaxed);
}

/// Run `download` in a helper process, passing its progress on. `None` if
/// no helper could be had, in which case none is running either.
pub(super) async fn download(
    settings: &ClientSettings,
    download: &Download,
    progress: Option<&(dyn Fn(u64, Option<u64>) + Send + Sync)>,
) -> Option<Result<Finished>> {
    if !AVAILABLE.load(Ordering::Relaxed) {
        return None;
    }
    let mut job = serde_json::to_vec(&Job {
        url: download.url.as_str().to_string(),
        headers: to_wire(&download.headers),
        destination: download.destination.as_os_str().as_bytes().to_vec(),
        error_for_status: download.error_for_status,
        client: settings.clone(),
    })
    .ok()?;
    job.push(b'\n');

    let mut helper = Helper::start().ok()?;
    match tokio::time::timeout(READY_TIMEOUT, helper.handshake(&job)).await {
        Ok(Ok(())) => Some(helper.follow(progress).await),
        _ => {
            helper.kill().await;
            None
        }
    }
}

/// The parent's end of a helper process.
struct Helper {
    /// Where the job is written, and the reading end of the same pipe. While
    /// that is open here, writing the job cannot fail for want of a reader,
    /// which would be a SIGPIPE to a host that has not chosen to ignore it.
    stdin: Option<(pipe::Sender, std::io::PipeReader)>,
    stdout: BufReader<pipe::Receiver>,
    /// As much of the next message as has arrived.
    line: Vec<u8>,
    /// Tells the thread that reaps the helper to kill it first. Dropping
    /// this leaves the helper running.
    kill: Option<std::sync::mpsc::Sender<()>>,
    /// Resolves once the helper has been reaped.
    reaped: tokio::sync::oneshot::Receiver<()>,
}

impl Helper {
    fn start() -> std::io::Result<Self> {
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;

        let (job_reader, job_writer) = std::io::pipe()?;
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command
            .args(std::env::args_os().skip(1))
            .env(HELPER_ENV, "1")
            .stdin(job_reader.try_clone()?)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        // A session of its own keeps the helper clear of whatever ends the
        // host's: its terminal closing, or a signal to its process group.
        // Only async-signal-safe calls may be made between fork and exec in
        // a multi-threaded process, and setsid is one.
        unsafe {
            command.pre_exec(|| match libc::setsid() {
                -1 => Err(std::io::Error::last_os_error()),
                _ => Ok(()),
            });
        }
        let mut child = command.spawn()?;
        let stdout = child.stdout.take();

        // The helper is not ours to wait for: it may run long after the
        // caller has lost interest. A thread reaps it whenever it exits.
        let (kill, killed) = std::sync::mpsc::channel::<()>();
        let (gone, reaped) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("frakt-background-helper".to_string())
            .spawn(move || {
                if killed.recv().is_ok() {
                    let _ = child.kill();
                }
                let _ = child.wait();
                let _ = gone.send(());
            })?;

        let stdout = stdout.ok_or_else(|| std::io::Error::other("no pipe"))?;
        Ok(Self {
            stdin: Some((
                pipe::Sender::from_owned_fd(OwnedFd::from(job_writer))?,
                job_reader,
            )),
            stdout: BufReader::new(pipe::Receiver::from_owned_fd(OwnedFd::from(stdout))?),
            line: Vec::new(),
            kill: Some(kill),
            reaped,
        })
    }

    /// Hand over the job and wait to hear that it was taken.
    async fn handshake(&mut self, job: &[u8]) -> std::io::Result<()> {
        if let Some((mut stdin, _reader)) = self.stdin.take() {
            stdin.write_all(job).await?;
        }
        loop {
            match Self::next(&mut self.stdout, &mut self.line).await? {
                Some(Message::Ready) => return Ok(()),
                Some(Message::Descriptors { .. }) => {}
                _ => return Err(std::io::Error::other("not a helper")),
            }
        }
    }

    /// The helper's next message, or `None` once its end of the pipe is
    /// closed. What has arrived of a message is kept in `line`, so the
    /// future can be dropped and the call made again.
    async fn next(
        stdout: &mut BufReader<pipe::Receiver>,
        line: &mut Vec<u8>,
    ) -> std::io::Result<Option<Message>> {
        let room = MESSAGE_LIMIT.saturating_sub(line.len() as u64);
        stdout.take(room).read_until(b'\n', line).await?;
        let line = std::mem::take(line);
        match line.last() {
            None => Ok(None),
            Some(b'\n') => Ok(Some(serde_json::from_slice(&line)?)),
            Some(_) => Err(std::io::ErrorKind::InvalidData.into()),
        }
    }

    /// Wait for the helper's result. Dropping this future stops the
    /// waiting, not the helper.
    async fn follow(
        mut self,
        progress: Option<&(dyn Fn(u64, Option<u64>) + Send + Sync)>,
    ) -> Result<Finished> {
        self.kill = None;
        let mut exited = false;
        let mut reported = None;
        loop {
            let next = Self::next(&mut self.stdout, &mut self.line);
            let message = if exited {
                // What it wrote before it went is still to be read, but the
                // end of it may never show: the writing end of the pipe can
                // be open in a process that the host started just as the
                // pipe was made.
                tokio::time::timeout(LAST_WORD_TIMEOUT, next)
                    .await
                    .unwrap_or(Ok(None))
            } else {
                tokio::select! {
                    biased;
                    message = next => message,
                    _ = &mut self.reaped => {
                        exited = true;
                        continue;
                    }
                }
            };
            let message = message.map_err(|e| {
                Error::Internal(format!(
                    "The background download helper could not be understood: {}",
                    e
                ))
            })?;
            match message {
                Some(Message::Progress { downloaded, total }) => {
                    if let Some(progress) = progress {
                        progress(downloaded, total);
                    }
                    reported = Some((downloaded, total));
                }
                Some(Message::Completed {
                    status,
                    headers,
                    bytes,
                }) => {
                    // The helper drops progress reports rather than wait for
                    // them to be read, and the last one may be among them.
                    if let Some(progress) = progress
                        && reported.is_some_and(|last| last != (bytes, Some(bytes)))
                    {
                        progress(bytes, Some(bytes));
                    }
                    return Ok(Finished {
                        status: StatusCode::from_u16(status).map_err(|_| {
                            Error::Internal(format!("Invalid HTTP status {}", status))
                        })?,
                        headers: from_wire(&headers),
                        bytes,
                    });
                }
                Some(Message::Failed { error }) => return Err(error.into()),
                Some(_) => {}
                None => {
                    return Err(Error::Internal(
                        "The background download helper exited without a result".to_string(),
                    ));
                }
            }
        }
    }

    /// Kill the helper and wait until it is gone, so that it cannot be
    /// writing to the destination's partial file after this returns.
    async fn kill(mut self) {
        if let Some(kill) = self.kill.take() {
            let _ = kill.send(());
        }
        let _ = self.reaped.await;
    }
}

/// The helper process: run the job on stdin and return the exit code.
fn serve() -> i32 {
    // A parent that has gone away must not end the download with a signal
    // on the next report to it. Rust's own start-up sees to that only when
    // `main` is Rust's.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
    close_inherited_descriptors();

    let mut stdout = std::io::stdout();
    if std::env::var_os(DESCRIPTORS_ENV).is_some() {
        let report = Message::Descriptors {
            open: open_descriptors(),
        };
        let _ = serde_json::to_writer(&mut stdout, &report);
        let _ = stdout.write_all(b"\n").and_then(|()| stdout.flush());
    }

    // Up to the end of the line, not of the input: that would not come if
    // the writing end of the pipe were also open in another process, one
    // the host started just as the pipe was made.
    let mut input = Vec::new();
    let job = std::io::stdin()
        .lock()
        .take(JOB_LIMIT)
        .read_until(b'\n', &mut input)
        .ok()
        .and_then(|_| serde_json::from_slice::<Job>(&input).ok());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    match (job, runtime) {
        (Some(job), Ok(runtime)) => runtime.block_on(run(job)).unwrap_or(2),
        _ => 2,
    }
}

/// Run a job, reporting to stdout. `None` if it could not be started, which
/// leaves the download to the parent.
async fn run(job: Job) -> Option<i32> {
    let stdout = std::io::stdout().as_fd().try_clone_to_owned().ok()?;
    let stdout = pipe::Sender::from_owned_fd(stdout).ok()?;
    let client = build_client(&job.client, None).ok()?;
    let download = Download {
        url: url::Url::parse(&job.url).ok()?,
        headers: from_wire(&job.headers),
        destination: std::ffi::OsString::from_vec(job.destination).into(),
        error_for_status: job.error_for_status,
        read_timeout: job.client.read_timeout,
    };

    // Reports are queued for a task that writes them, and dropped when the
    // queue is full: the download never waits for a parent to read them.
    let (reports, queue) = tokio::sync::mpsc::channel(64);
    let writer = tokio::spawn(report(stdout, queue));
    let _ = reports.try_send(Message::Ready);
    // The writer gets to send that before the destination is opened, so
    // the parent's wait for it does not depend on how slow a filesystem is.
    tokio::task::yield_now().await;

    let mut last_progress = None::<Instant>;
    let outcome = background::run(&client, &download, |event| {
        let message = match event {
            Event::Started {
                status,
                headers,
                total,
            } => Message::Started {
                status: status.as_u16(),
                headers: to_wire(headers),
                total,
            },
            Event::Progress { downloaded, total } => {
                let now = Instant::now();
                let recent = |last| now.duration_since(last) < PROGRESS_INTERVAL;
                if total != Some(downloaded) && last_progress.is_some_and(recent) {
                    return;
                }
                last_progress = Some(now);
                Message::Progress { downloaded, total }
            }
        };
        let _ = reports.try_send(message);
    })
    .await;

    let (code, last_word) = match outcome {
        Ok(finished) => (
            0,
            Message::Completed {
                status: finished.status.as_u16(),
                headers: to_wire(&finished.headers),
                bytes: finished.bytes,
            },
        ),
        Err(error) => (
            1,
            Message::Failed {
                error: error.into(),
            },
        ),
    };
    let _ = tokio::time::timeout(LAST_WORD_TIMEOUT, async {
        let _ = reports.send(last_word).await;
        drop(reports);
        let _ = writer.await;
    })
    .await;
    Some(code)
}

async fn report(mut stdout: pipe::Sender, mut queue: tokio::sync::mpsc::Receiver<Message>) {
    while let Some(message) = queue.recv().await {
        let Ok(mut line) = serde_json::to_vec(&message) else {
            continue;
        };
        line.push(b'\n');
        // Nobody is listening any more; the download carries on regardless.
        if stdout.write_all(&line).await.is_err() {
            break;
        }
    }
}

/// Close what the host passed on other than the standard three: it is of no
/// use here and must not be kept open, perhaps for hours, by a process the
/// host knows nothing of.
///
/// That is every descriptor without close-on-exec. One with it cannot have
/// come through exec, so it is this process's own, such as those of a
/// runtime the host started before it called us.
fn close_inherited_descriptors() {
    for descriptor in open_descriptors() {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if descriptor > 2 && flags != -1 && flags & libc::FD_CLOEXEC == 0 {
            unsafe { libc::close(descriptor) };
        }
    }
}

/// The descriptors this process has open.
fn open_descriptors() -> Vec<libc::c_int> {
    let is_open =
        |descriptor: &libc::c_int| unsafe { libc::fcntl(*descriptor, libc::F_GETFD) != -1 };

    // Where the kernel lists them, the list has them all, however high.
    // Elsewhere /dev/fd may exist and show only the standard three.
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        let listing = ["/proc/self/fd", "/dev/fd"]
            .iter()
            .find_map(|path| std::fs::read_dir(path).ok());
        if let Some(listing) = listing {
            let listed: Vec<libc::c_int> = listing
                .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
                .collect();
            // The listing was read through a descriptor of its own, which
            // is closed by now.
            return listed.into_iter().filter(is_open).collect();
        }
    }

    let limit = match unsafe { libc::sysconf(libc::_SC_OPEN_MAX) } {
        limit if limit > 0 => limit.min(1 << 20) as libc::c_int,
        _ => 1 << 16,
    };
    (0..limit).filter(is_open).collect()
}
