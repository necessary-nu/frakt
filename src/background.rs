//! Downloads the platform carries out on the app's behalf, which survive
//! the app being suspended or killed: an NSURLSession background session on
//! Apple platforms, WorkManager on Android.
//!
//! The platform, not the app, owns the transfer, so results arrive through
//! a [`BackgroundHandler`] rather than a future: possibly in a later launch
//! of the app, after [`BackgroundDownloads::new`] re-attaches to the same
//! identifier.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use frakt::background::*;
//! struct Handler;
//!
//! impl BackgroundHandler for Handler {
//!     fn finished(&self, download: FinishedDownload<'_>) {
//!         // Verify and move the file now: it is removed when this returns.
//!         std::fs::rename(download.file, "/path/to/keep").unwrap();
//!     }
//!
//!     fn failed(&self, job: &str, error: frakt::Error, resumable: bool) {
//!         eprintln!("{job} failed: {error} (resumable: {resumable})");
//!     }
//! }
//!
//! # fn main() -> frakt::Result<()> {
//! // At every launch, before anything else uses the identifier:
//! let downloads = BackgroundDownloads::new("com.example.app.downloads", Arc::new(Handler))?;
//! downloads.enqueue(BackgroundJob::new(
//!     "package-1.2.3",
//!     "https://example.com/package.zip".parse().unwrap(),
//! ))?;
//! # Ok(())
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use http::{HeaderMap, StatusCode};
use url::Url;

use crate::{Error, Result};

#[cfg(all(feature = "backend-android", target_os = "android"))]
use crate::backend::android::background as platform;
#[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
use crate::backend::foundation::background as platform;

/// A download to hand to the platform.
#[derive(Clone, Debug)]
pub struct BackgroundJob {
    /// The caller's name for the job, unique among pending jobs. Events,
    /// [`BackgroundDownloads::cancel`] and resumption use it, across
    /// launches.
    pub id: String,
    /// What to download.
    pub url: Url,
    /// Headers to send with the request.
    pub headers: HeaderMap,
    /// Where the caller means to put the file, handed back in
    /// [`FinishedDownload::destination`]. frakt does not move the file
    /// there itself.
    pub destination: Option<PathBuf>,
}

impl BackgroundJob {
    /// A job with no extra headers or destination.
    pub fn new(id: impl Into<String>, url: Url) -> Self {
        Self {
            id: id.into(),
            url,
            headers: HeaderMap::new(),
            destination: None,
        }
    }
}

/// A finished download, valid for the duration of
/// [`BackgroundHandler::finished`].
#[derive(Debug)]
pub struct FinishedDownload<'a> {
    /// The job's id.
    pub job: &'a str,
    /// The downloaded file. It is removed after the handler returns, so
    /// move or copy it to keep it.
    pub file: &'a Path,
    /// The job's destination hint.
    pub destination: Option<&'a Path>,
    /// The final response's status (2xx; other statuses are failures).
    pub status: StatusCode,
    /// The final response's headers.
    pub headers: &'a HeaderMap,
}

/// Receives the results of background downloads. Methods may be called on
/// any thread, including during [`BackgroundDownloads::new`] for jobs that
/// ended while the app was not running.
pub trait BackgroundHandler: Send + Sync + 'static {
    /// Progress of a running job; `total` is the expected size if known.
    fn progress(&self, job: &str, downloaded: u64, total: Option<u64>) {
        let _ = (job, downloaded, total);
    }

    /// A job finished. Runs synchronously while the file still exists: on
    /// Apple platforms inside the session's
    /// `URLSession:downloadTask:didFinishDownloadingToURL:`, after which
    /// the system deletes the file.
    fn finished(&self, download: FinishedDownload<'_>);

    /// A job failed. If `resumable`, enqueueing a job with the same id
    /// continues from where this one stopped.
    fn failed(&self, job: &str, error: Error, resumable: bool);
}

/// Where a pending job is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobState {
    /// Waiting to run, for example for connectivity or a retry.
    Waiting,
    /// Transferring.
    Running,
}

/// A pending job, as [`BackgroundDownloads::jobs`] reports it.
#[derive(Clone, Debug)]
pub struct JobStatus {
    /// The job's id.
    pub id: String,
    /// Where it is.
    pub state: JobState,
    /// Bytes received so far.
    pub downloaded: u64,
    /// The expected size, if known.
    pub total: Option<u64>,
}

/// Background downloads under one identifier.
///
/// Create it once per launch, early (before handling the platform's
/// background events), with the same identifier every time; that is how
/// jobs started in an earlier launch are found again. Dropping it stops
/// event delivery but not the platform's transfers.
pub struct BackgroundDownloads {
    inner: platform::Downloads,
}

impl BackgroundDownloads {
    /// Attach to the background downloads named `identifier`, delivering
    /// their events to `handler`.
    pub fn new(identifier: &str, handler: Arc<dyn BackgroundHandler>) -> Result<Self> {
        Ok(Self {
            inner: platform::Downloads::new(identifier, handler)?,
        })
    }

    /// Start a job. If a job with this id failed resumably, the transfer
    /// continues from where it stopped; if one is pending, this fails.
    pub fn enqueue(&self, job: BackgroundJob) -> Result<()> {
        self.inner.enqueue(job)
    }

    /// Cancel a pending job and forget any resume state for it. No event is
    /// delivered for it afterwards.
    pub fn cancel(&self, job: &str) -> Result<()> {
        self.inner.cancel(job)
    }

    /// The pending jobs.
    pub async fn jobs(&self) -> Result<Vec<JobStatus>> {
        self.inner.jobs().await
    }
}

/// Hand frakt the completion handler from the app delegate's
/// `application(_:handleEventsForBackgroundURLSession:completionHandler:)`.
///
/// frakt calls it on the main queue once the session named `identifier`
/// has delivered every pending event (`urlSessionDidFinishEvents`), as
/// UIKit requires. Create the [`BackgroundDownloads`] for `identifier`
/// before or right after calling this.
#[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
pub fn handle_events_for_background_session(
    identifier: &str,
    completion_handler: &block2::DynBlock<dyn Fn()>,
) {
    platform::handle_events_for_background_session(identifier, completion_handler)
}
