//! Shared types between backend implementations

use crate::body::Body;
use bytes::Bytes;
use futures_util::Stream;
use http::{HeaderMap, Method, StatusCode};
use std::{
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::mpsc;
use url::Url;

/// Platform-agnostic HTTP request
pub struct BackendRequest {
    /// HTTP method for the request
    pub method: Method,
    /// URL for the request
    pub url: Url,
    /// Headers for the request
    pub headers: HeaderMap,
    /// Optional body content
    pub body: Option<Body>,
    /// Optional progress callback for uploads
    pub progress_callback: Option<ProgressCallback>,
    /// Per-request override of the backend's read (idle) timeout: the longest
    /// the transfer may wait for the network while frakt wants data.
    pub read_timeout: Option<Duration>,
}

/// A response body as a stream of chunks.
///
/// The stream ends with `None` only after the whole body arrived; a transfer
/// that fails part-way yields `Some(Err(_))`. Dropping the stream before the
/// end must cancel the transfer and release everything the backend holds for
/// it.
pub type BodyStream = Pin<Box<dyn Stream<Item = crate::Result<Bytes>> + Send + 'static>>;

/// Platform-agnostic HTTP response
pub struct BackendResponse {
    /// HTTP status code
    pub status: StatusCode,
    /// Response headers
    pub headers: HeaderMap,
    /// The final URL, after any redirects
    pub url: Url,
    /// The response body
    pub body: BodyStream,
    /// Headers from redirect responses (for cookie processing)
    pub redirect_headers: Vec<HeaderMap>,
}

/// Wrap the receiving end of a body channel as a [`BodyStream`].
///
/// Dropping the stream drops the receiver, which the sending side observes as
/// a failed send.
pub fn channel_body(receiver: mpsc::Receiver<crate::Result<Bytes>>) -> BodyStream {
    struct ChannelBody(mpsc::Receiver<crate::Result<Bytes>>);

    impl Stream for ChannelBody {
        type Item = crate::Result<Bytes>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }

    Box::pin(ChannelBody(receiver))
}

/// Platform-agnostic download handle
pub struct BackendDownloadHandle {
    /// Unique identifier for the download
    pub id: String,
    /// Platform-specific handle data
    pub platform_handle: PlatformHandle,
}

/// Platform-specific download handle data
pub enum PlatformHandle {
    /// Foundation (NSURLSession) download task on Apple platforms
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    Foundation {
        /// NSURLSession download task
        task: objc2::rc::Retained<objc2_foundation::NSURLSessionDownloadTask>,
    },

    /// Reqwest-based download task for cross-platform support
    Reqwest {
        /// Tokio task handle for the download
        task_handle: tokio::task::JoinHandle<Result<PathBuf, crate::Error>>,
    },
}

/// Progress information for downloads/uploads
#[derive(Debug, Clone)]
pub struct ProgressInfo {
    /// Number of bytes transferred so far
    pub bytes_transferred: u64,
    /// Total bytes to transfer (if known)
    pub total_bytes: Option<u64>,
}

/// Callback type for progress reporting
pub type ProgressCallback = Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>;
