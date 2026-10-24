//! Shared types between backend implementations

use crate::body::Body;
use bytes::Bytes;
use futures_util::Stream;
use http::{HeaderMap, Method, StatusCode};
use std::{
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// How the body relates to the bytes on the wire
    pub accounting: Arc<BodyAccounting>,
    /// Headers from redirect responses (for cookie processing)
    pub redirect_headers: Vec<HeaderMap>,
}

/// What a backend knows about a body's encoding and size.
///
/// The backend creates it with the response head and, if its stack reports
/// how many body bytes arrived over the network, updates the wire count as
/// the body is read; the count must be final before the body stream ends.
/// "Wire bytes" are the body as sent, after any transfer coding (chunked
/// framing) is removed and before any content coding (gzip, br, ...) is
/// decoded: the bytes Content-Length counts.
#[derive(Debug)]
pub struct BodyAccounting {
    decoded: bool,
    expected_wire_length: Option<u64>,
    wire_bytes: AtomicU64,
    wire_counted: AtomicBool,
    integrity: std::sync::OnceLock<BodyIntegrity>,
}

/// Whether a body cut off by a closed connection is reported as an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyIntegrity {
    /// The body's end is checked: against Content-Length, the chunked
    /// terminator or the stream's end marker. A body cut short is an error.
    Checked,
    /// Nothing distinguishes the whole body from one cut short by the
    /// connection closing cleanly: a body delimited only by the connection
    /// closing, or one the backend's stack does not check (on Foundation,
    /// any body without Content-Length).
    Unverifiable,
}

impl BodyAccounting {
    /// `decoded`: the stack decoded a content coding, so the delivered body
    /// differs from the wire bytes. `expected_wire_length`: the
    /// Content-Length the server sent, which some stacks strip when they
    /// decode.
    pub fn new(decoded: bool, expected_wire_length: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            decoded,
            expected_wire_length,
            wire_bytes: AtomicU64::new(0),
            wire_counted: AtomicBool::new(false),
            integrity: std::sync::OnceLock::new(),
        })
    }

    /// Say whether the stack checks this body's end. Without it, a body is
    /// taken as checked if it has a Content-Length or chunked framing,
    /// which stacks other than Foundation check.
    pub fn set_integrity(&self, integrity: BodyIntegrity) {
        let _ = self.integrity.set(integrity);
    }

    /// The integrity the backend set, if it set one.
    pub fn integrity(&self) -> Option<BodyIntegrity> {
        self.integrity.get().copied()
    }

    /// For a body delivered exactly as sent: its Content-Length, if any, is
    /// the expected length.
    pub fn undecoded(headers: &HeaderMap) -> Arc<Self> {
        Self::new(false, content_length(headers))
    }

    /// Record that `n` more body bytes arrived over the network.
    pub fn add_wire_bytes(&self, n: u64) {
        self.wire_bytes.fetch_add(n, Ordering::Relaxed);
        self.wire_counted.store(true, Ordering::Release);
    }

    /// Record the total body bytes that arrived over the network so far.
    pub fn set_wire_bytes(&self, total: u64) {
        self.wire_bytes.store(total, Ordering::Relaxed);
        self.wire_counted.store(true, Ordering::Release);
    }

    /// Whether the stack decoded a content coding.
    pub fn decoded(&self) -> bool {
        self.decoded
    }

    /// The body length the server announced, when it did.
    pub fn expected_wire_length(&self) -> Option<u64> {
        self.expected_wire_length
    }

    /// Body bytes received over the network, if the stack reports them.
    pub fn wire_bytes(&self) -> Option<u64> {
        self.wire_counted
            .load(Ordering::Acquire)
            .then(|| self.wire_bytes.load(Ordering::Relaxed))
    }
}

/// The Content-Length header's value, if it has a valid one.
pub fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Whether the headers say the body was sent with chunked framing.
pub fn is_chunked(headers: &HeaderMap) -> bool {
    headers
        .get_all(http::header::TRANSFER_ENCODING)
        .iter()
        .any(|value| {
            value.to_str().is_ok_and(|v| {
                v.split(',')
                    .any(|c| c.trim().eq_ignore_ascii_case("chunked"))
            })
        })
}

/// Whether the headers name a content coding other than identity.
pub fn has_content_coding(headers: &HeaderMap) -> bool {
    headers
        .get_all(http::header::CONTENT_ENCODING)
        .iter()
        .any(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
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
