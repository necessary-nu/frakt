//! Background downloads on Android, run by WorkManager.
//!
//! The worker is `se.brendan.frakt.background.FraktDownloadWorker`, in the
//! `android/frakt-background` library module, which the host app includes
//! so the worker is in the app's own dex: WorkManager recreates it by class
//! name, including in a process the system starts only to run it. The
//! worker downloads with `HttpURLConnection` (the system's proxy settings
//! and trust store) and needs no Rust code.
//!
//! Each job's state lives in files in the app's storage, which the worker
//! writes and this module reads: a JSON status, the partial download and
//! its If-Range validator, and the finished file. Events are delivered by
//! reading them, at [`Downloads::new`] for jobs that ended while the
//! process was not running, and every half second while it lives.
//!
//! # Host integration
//!
//! - Include `android/frakt-background` as a Gradle module
//!   (`include(":frakt-background")` with its `projectDir` pointing there,
//!   then `implementation(project(":frakt-background"))`), or the AAR it
//!   builds plus `androidx.work:work-runtime`.
//! - Its manifest adds the INTERNET, ACCESS_NETWORK_STATE,
//!   FOREGROUND_SERVICE and FOREGROUND_SERVICE_DATA_SYNC permissions and
//!   declares WorkManager's foreground service with the `dataSync` type.
//!   On Android 13 and later the app must also hold POST_NOTIFICATIONS for
//!   the progress notification to be shown; the download runs without it.
//! - WorkManager's default initialization (its androidx.startup provider)
//!   must not be removed, or the app must initialize WorkManager itself in
//!   `Application.onCreate`, so a process started for the worker can run
//!   it.
//! - Call `frakt::backend::android::init` before creating
//!   [`BackgroundDownloads`](crate::background::BackgroundDownloads).
//!
//! `am force-stop` cancels the app's WorkManager jobs; WorkManager
//! reschedules them the next time the app starts, and they resume from
//! their partial files.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JValue};
use serde::Deserialize;
use tokio::sync::oneshot;
use url::Url;

use super::callback::{java_error, java_string};
use crate::background::{BackgroundHandler, BackgroundJob, FinishedDownload, JobState, JobStatus};
use crate::{Error, Result};

const HELPER: &str = "se.brendan.frakt.background.FraktBackground";
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// A job's status file, as `JobFiles` in frakt-background writes it.
#[derive(Debug, Deserialize)]
struct Status {
    id: String,
    state: String,
    #[serde(default)]
    downloaded: u64,
    #[serde(default)]
    total: i64,
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    destination: Option<String>,
    #[serde(default)]
    status: Option<u16>,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default, rename = "httpStatus")]
    http_status: u16,
    #[serde(default)]
    resumable: bool,
}

impl Status {
    fn total(&self) -> Option<u64> {
        u64::try_from(self.total).ok()
    }

    fn pending(&self) -> bool {
        matches!(self.state.as_str(), "waiting" | "running")
    }
}

/// The file name key `JobFiles` derives from a job id.
fn file_key(id: &str) -> String {
    id.bytes().map(|b| format!("{:02x}", b)).collect()
}

struct Shared {
    directory: PathBuf,
    handler: Arc<dyn BackgroundHandler>,
    /// Progress last reported per job, so unchanged progress is not repeated.
    reported: Mutex<HashMap<String, u64>>,
    /// Held while reading and consuming status files, so an event is never
    /// delivered twice by concurrent scans.
    scanning: Mutex<()>,
    stop: AtomicBool,
}

impl Shared {
    fn status_path(&self, id: &str) -> PathBuf {
        self.directory.join(format!("{}.json", file_key(id)))
    }

    fn read_status(&self, path: &Path) -> Option<Status> {
        serde_json::from_slice(&std::fs::read(path).ok()?).ok()
    }

    fn statuses(&self) -> Vec<(PathBuf, Status)> {
        let Ok(entries) = std::fs::read_dir(&self.directory) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .filter_map(|path| Some((path.clone(), self.read_status(&path)?)))
            .collect()
    }

    /// Deliver what the status files say has happened since the last scan.
    fn scan(&self) {
        let _scanning = self.scanning.lock().unwrap_or_else(|e| e.into_inner());
        for (path, status) in self.statuses() {
            match status.state.as_str() {
                "waiting" | "running" => {
                    let mut reported = self.reported.lock().unwrap_or_else(|e| e.into_inner());
                    if reported.get(&status.id) != Some(&status.downloaded) {
                        reported.insert(status.id.clone(), status.downloaded);
                        drop(reported);
                        self.deliver(|handler| {
                            handler.progress(&status.id, status.downloaded, status.total())
                        });
                    }
                }
                "succeeded" => {
                    self.finished(&status);
                    self.forget(&path, &status.id);
                }
                "failed" => {
                    let id = status.id.clone();
                    let resumable = status.resumable;
                    self.failed(status);
                    if resumable {
                        let _ = std::fs::remove_file(&path);
                        self.reported
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&id);
                    } else {
                        self.forget(&path, &id);
                    }
                }
                _ => {}
            }
        }
    }

    fn finished(&self, status: &Status) {
        let Some(file) = status.file.as_deref().map(PathBuf::from) else {
            return;
        };
        let mut headers = HeaderMap::new();
        for (name, value) in &status.headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                headers.append(name, value);
            }
        }
        let destination = status.destination.as_deref().map(Path::new);
        let code = status
            .status
            .and_then(|code| StatusCode::from_u16(code).ok())
            .unwrap_or(StatusCode::OK);
        self.deliver(|handler| {
            handler.finished(FinishedDownload {
                job: &status.id,
                file: &file,
                destination,
                status: code,
                headers: &headers,
            })
        });
    }

    fn failed(&self, status: Status) {
        let error = match StatusCode::from_u16(status.http_status) {
            Ok(code) if code.as_u16() >= 400 => Error::HttpStatus(code),
            _ => Error::Network {
                code: -1,
                message: status
                    .error
                    .clone()
                    .unwrap_or_else(|| "background download failed".to_string()),
            },
        };
        self.deliver(|handler| handler.failed(&status.id, error, status.resumable));
    }

    /// Remove a consumed job's files, including the finished file if the
    /// handler left it in place.
    fn forget(&self, status_path: &Path, id: &str) {
        let key = file_key(id);
        for suffix in ["part", "resume", "download"] {
            let _ = std::fs::remove_file(self.directory.join(format!("{}.{}", key, suffix)));
        }
        let _ = std::fs::remove_file(status_path);
        self.reported
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
    }

    /// Call the handler, keeping the poller alive if it panics.
    fn deliver(&self, f: impl FnOnce(&dyn BackgroundHandler)) {
        let handler = self.handler.as_ref();
        if std::panic::catch_unwind(AssertUnwindSafe(|| f(handler))).is_err() {
            tracing::error!("background download handler panicked");
        }
    }
}

pub(crate) struct Downloads {
    identifier: String,
    shared: Arc<Shared>,
    poller: Option<JoinHandle<()>>,
}

impl Downloads {
    pub(crate) fn new(identifier: &str, handler: Arc<dyn BackgroundHandler>) -> Result<Self> {
        let directory = with_helper(|env, context, helper| {
            let identifier = new_string(env, Some(identifier))?;
            let directory = env
                .call_static_method(
                    helper,
                    "directory",
                    "(Landroid/content/Context;Ljava/lang/String;)Ljava/lang/String;",
                    &[JValue::Object(context), JValue::Object(&identifier)],
                )
                .and_then(|v| v.l())
                .map_err(|e| java_error(env, "FraktBackground.directory", e))?;
            java_string(env, &directory)
                .ok_or_else(|| Error::Internal("No background download directory".to_string()))
        })?;

        let shared = Arc::new(Shared {
            directory: PathBuf::from(directory),
            handler,
            reported: Mutex::new(HashMap::new()),
            scanning: Mutex::new(()),
            stop: AtomicBool::new(false),
        });
        shared.scan();

        let poller = {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("frakt-background".to_string())
                .spawn(move || {
                    while !shared.stop.load(Ordering::Acquire) {
                        std::thread::park_timeout(POLL_INTERVAL);
                        if !shared.stop.load(Ordering::Acquire) {
                            shared.scan();
                        }
                    }
                })
                .map_err(|e| Error::Internal(format!("Failed to start the poller: {}", e)))?
        };

        Ok(Self {
            identifier: identifier.to_string(),
            shared,
            poller: Some(poller),
        })
    }

    pub(crate) fn enqueue(&self, job: BackgroundJob) -> Result<()> {
        if self
            .shared
            .read_status(&self.shared.status_path(&job.id))
            .is_some_and(|status| status.pending())
        {
            return Err(Error::Internal(format!(
                "Job {} is already pending",
                job.id
            )));
        }

        let headers: Vec<(String, String)> = job
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        let headers = serde_json::to_string(&headers).map_err(|e| Error::Json(e.to_string()))?;
        let destination = job
            .destination
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned());

        let failure = with_helper(|env, context, helper| {
            let identifier = new_string(env, Some(&self.identifier))?;
            let id = new_string(env, Some(&job.id))?;
            let url = new_string(env, Some(job.url.as_str()))?;
            let headers = new_string(env, Some(&headers))?;
            let destination = new_string(env, destination.as_deref())?;
            let failure = env
                .call_static_method(
                    helper,
                    "enqueue",
                    "(Landroid/content/Context;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;",
                    &[
                        JValue::Object(context),
                        JValue::Object(&identifier),
                        JValue::Object(&id),
                        JValue::Object(&url),
                        JValue::Object(&headers),
                        JValue::Object(&destination),
                    ],
                )
                .and_then(|v| v.l())
                .map_err(|e| java_error(env, "FraktBackground.enqueue", e))?;
            Ok(java_string(env, &failure))
        })?;
        match failure {
            None => Ok(()),
            Some(failure) => Err(Error::Internal(format!(
                "Could not enqueue {}: {}",
                job.id, failure
            ))),
        }
    }

    pub(crate) fn cancel(&self, job: &str) -> Result<()> {
        let failure = with_helper(|env, context, helper| {
            let identifier = new_string(env, Some(&self.identifier))?;
            let id = new_string(env, Some(job))?;
            let failure = env
                .call_static_method(
                    helper,
                    "cancel",
                    "(Landroid/content/Context;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;",
                    &[
                        JValue::Object(context),
                        JValue::Object(&identifier),
                        JValue::Object(&id),
                    ],
                )
                .and_then(|v| v.l())
                .map_err(|e| java_error(env, "FraktBackground.cancel", e))?;
            Ok(java_string(env, &failure))
        })?;
        self.shared
            .reported
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(job);
        match failure {
            None => Ok(()),
            Some(failure) => Err(Error::Internal(format!(
                "Could not cancel {}: {}",
                job, failure
            ))),
        }
    }

    pub(crate) async fn jobs(&self) -> Result<Vec<JobStatus>> {
        Ok(self
            .shared
            .statuses()
            .into_iter()
            .filter(|(_, status)| status.pending())
            .map(|(_, status)| JobStatus {
                state: if status.state == "running" {
                    JobState::Running
                } else {
                    JobState::Waiting
                },
                downloaded: status.downloaded,
                total: status.total(),
                id: status.id,
            })
            .collect())
    }
}

impl Drop for Downloads {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(poller) = self.poller.take() {
            poller.thread().unpark();
            let _ = poller.join();
        }
    }
}

fn new_string<'a>(env: &mut JNIEnv<'a>, value: Option<&str>) -> Result<JObject<'a>> {
    match value {
        Some(value) => env
            .new_string(value)
            .map(JObject::from)
            .map_err(|e| java_error(env, "NewString", e)),
        None => Ok(JObject::null()),
    }
}

/// Run `f` with the application context and the `FraktBackground` class.
fn with_helper<T>(f: impl FnOnce(&mut JNIEnv, &JObject, &JClass) -> Result<T>) -> Result<T> {
    let jvm = super::get_global_vm()?;
    let context = super::application_context()?;
    let mut env = jvm
        .attach_current_thread_as_daemon()
        .map_err(|e| Error::Internal(format!("Failed to attach to the JVM: {}", e)))?;
    super::local_frame(&mut env, 16, |env| {
        let helper = super::load_app_class(env, HELPER).map_err(|error| {
            Error::Internal(format!(
                "{} is not in the app ({}); include the android/frakt-background module",
                HELPER, error
            ))
        })?;
        f(env, context.as_obj(), &helper)
    })
}

/// Routes events to `Client::download_background` calls waiting in this
/// process. A job that ends after its caller's process died has nobody to
/// hand the file to, so it is discarded.
struct Waiters {
    pending: Mutex<HashMap<String, Waiter>>,
}

type ProgressFn = Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>;

struct Waiter {
    file_path: PathBuf,
    progress: Option<ProgressFn>,
    error_for_status: bool,
    done: oneshot::Sender<Result<crate::client::download::DownloadResponse>>,
}

impl BackgroundHandler for Waiters {
    fn progress(&self, job: &str, downloaded: u64, total: Option<u64>) {
        if let Some(waiter) = self.pending.lock().unwrap().get(job) {
            if let Some(progress) = &waiter.progress {
                progress(downloaded, total);
            }
        }
    }

    fn finished(&self, download: FinishedDownload<'_>) {
        let Some(waiter) = self.pending.lock().unwrap().remove(download.job) else {
            return;
        };
        let result = move_file(download.file, &waiter.file_path).map(|bytes_downloaded| {
            crate::client::download::DownloadResponse {
                file_path: waiter.file_path.clone(),
                bytes_downloaded,
                status: download.status,
                headers: download.headers.clone(),
            }
        });
        let _ = waiter.done.send(result);
    }

    fn failed(&self, job: &str, error: Error, _resumable: bool) {
        let Some(waiter) = self.pending.lock().unwrap().remove(job) else {
            return;
        };
        let result = match error {
            Error::HttpStatus(status) if !waiter.error_for_status => {
                Ok(crate::client::download::DownloadResponse {
                    file_path: waiter.file_path.clone(),
                    bytes_downloaded: 0,
                    status,
                    headers: HeaderMap::new(),
                })
            }
            error => Err(error),
        };
        let _ = waiter.done.send(result);
    }
}

fn move_file(from: &Path, to: &Path) -> Result<u64> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if std::fs::rename(from, to).is_err() {
        std::fs::copy(from, to)?;
    }
    Ok(std::fs::metadata(to)?.len())
}

type Router = (Arc<Waiters>, Arc<Downloads>);

static ROUTERS: LazyLock<Mutex<HashMap<String, Router>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Download `url` to `file_path` through WorkManager and wait for it.
pub(crate) async fn download_to_file(
    session_identifier: Option<String>,
    url: Url,
    file_path: PathBuf,
    headers: HeaderMap,
    progress: Option<ProgressFn>,
    error_for_status: bool,
) -> Result<crate::client::download::DownloadResponse> {
    let identifier = session_identifier.unwrap_or_else(|| "frakt-default".to_string());
    let (waiters, downloads) = {
        let mut routers = ROUTERS.lock().unwrap();
        match routers.get(&identifier) {
            Some(router) => router.clone(),
            None => {
                let waiters = Arc::new(Waiters {
                    pending: Mutex::new(HashMap::new()),
                });
                let downloads = Arc::new(Downloads::new(&identifier, waiters.clone())?);
                routers.insert(identifier.clone(), (waiters.clone(), downloads.clone()));
                (waiters, downloads)
            }
        }
    };

    static NEXT_JOB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let id = format!(
        "frakt-{}-{}",
        std::process::id(),
        NEXT_JOB.fetch_add(1, Ordering::Relaxed)
    );
    let (done, finished) = oneshot::channel();
    waiters.pending.lock().unwrap().insert(
        id.clone(),
        Waiter {
            file_path,
            progress,
            error_for_status,
            done,
        },
    );
    let mut job = BackgroundJob::new(id.clone(), url);
    job.headers = headers;
    if let Err(error) = downloads.enqueue(job) {
        waiters.pending.lock().unwrap().remove(&id);
        return Err(error);
    }
    finished
        .await
        .map_err(|_| Error::Internal("Background download was abandoned".to_string()))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_keys_match_job_files() {
        assert_eq!(file_key("ab"), "6162");
        assert_eq!(file_key("é"), "c3a9");
    }

    #[test]
    fn statuses_parse_as_the_worker_writes_them() {
        let status: Status = serde_json::from_str(
            r#"{"id":"j","url":"http://x/","state":"succeeded","downloaded":5,"total":5,
                "file":"/f","status":206,"headers":[["ETag","\"a\""]]}"#,
        )
        .unwrap();
        assert_eq!(status.total(), Some(5));
        assert_eq!(status.headers[0].0, "ETag");
        let waiting: Status =
            serde_json::from_str(r#"{"id":"j","state":"waiting","total":-1}"#).unwrap();
        assert!(waiting.pending());
        assert_eq!(waiting.total(), None);
    }
}
