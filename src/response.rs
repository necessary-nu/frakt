//! Response types with backend abstraction

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::backend::types::{BackendResponse, BodyStream};
use crate::{Error, Result};
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use tokio::time::{Instant, Sleep};
use url::Url;

/// HTTP response from any backend
pub struct Response {
    status: StatusCode,
    headers: HeaderMap,
    url: Url,
    body: ResponseBody,
}

impl Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("url", &self.url)
            .field("headers", &self.headers)
            .finish()
    }
}

impl Response {
    /// Create a Response from a BackendResponse.
    ///
    /// `deadline` is the request's total deadline, if it has one; reading the
    /// body past it fails with [`Error::Timeout`] and cancels the transfer.
    pub(crate) fn from_backend(
        backend_response: BackendResponse,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            status: backend_response.status,
            headers: backend_response.headers,
            url: backend_response.url,
            body: ResponseBody::new(backend_response.body, deadline),
        }
    }

    /// Get the status code
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Get the final URL of the response, after any redirects were followed
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Get the headers
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Get a specific header value
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// Check if the response status indicates success (2xx)
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Check if the response status indicates a client error (4xx)
    pub fn is_client_error(&self) -> bool {
        self.status.is_client_error()
    }

    /// Check if the response status indicates a server error (5xx)
    pub fn is_server_error(&self) -> bool {
        self.status.is_server_error()
    }

    /// Consume the response and return the body as bytes
    pub async fn bytes(mut self) -> Result<Bytes> {
        let mut body = Vec::new();

        while let Some(chunk) = self.body.next().await {
            body.extend_from_slice(&chunk?);
        }

        Ok(Bytes::from(body))
    }

    /// Consume the response and return the body as text
    pub async fn text(self) -> Result<String> {
        let bytes = self.bytes().await?;
        String::from_utf8(bytes.to_vec()).map_err(Error::Utf8)
    }

    /// Consume the response and deserialize the body as JSON
    pub async fn json<T: serde::de::DeserializeOwned>(self) -> Result<T> {
        let bytes = self.bytes().await?;
        serde_json::from_slice(&bytes).map_err(|x| Error::Json(x.to_string()))
    }

    /// Get the response body as a stream of bytes.
    ///
    /// Dropping the stream before it ends cancels the transfer.
    pub fn stream(self) -> ResponseStream {
        ResponseStream {
            body: self.body,
            current_chunk: None,
            chunk_offset: 0,
        }
    }
}

/// A backend body plus the request's total deadline.
struct ResponseBody {
    stream: Option<BodyStream>,
    deadline: Option<Pin<Box<Sleep>>>,
}

// SAFETY: every method takes `&mut self`, so a shared reference gives no
// access to the inner stream, which need not be `Sync`. `Response` has to be
// `Sync` because `Error::HttpError` holds one.
unsafe impl Sync for ResponseBody {}

impl ResponseBody {
    fn new(stream: BodyStream, deadline: Option<Instant>) -> Self {
        Self {
            stream: Some(stream),
            deadline: deadline.map(|deadline| Box::pin(tokio::time::sleep_until(deadline))),
        }
    }

    async fn next(&mut self) -> Option<Result<Bytes>> {
        std::future::poll_fn(|cx| self.poll_chunk(cx)).await
    }

    fn poll_chunk(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };

        if let Some(deadline) = self.deadline.as_mut() {
            if deadline.as_mut().poll(cx).is_ready() {
                // Dropping the backend stream cancels the transfer.
                self.stream = None;
                return Poll::Ready(Some(Err(Error::Timeout)));
            }
        }

        match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Some(Ok(chunk))),
            Poll::Ready(Some(Err(error))) => {
                self.stream = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.stream = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stream of response body bytes.
///
/// Yields `Some(Err(_))` if the transfer fails part-way; it only ends with
/// `None` once the whole body has arrived. Dropping it cancels the transfer.
pub struct ResponseStream {
    body: ResponseBody,
    current_chunk: Option<Bytes>,
    chunk_offset: usize,
}

impl ResponseStream {
    /// Get the next chunk of bytes
    pub async fn next(&mut self) -> Option<Result<Bytes>> {
        if let Some(rest) = self.take_partial_chunk() {
            return Some(Ok(rest));
        }
        self.body.next().await
    }

    /// The unread part of a chunk that `AsyncRead` only partly consumed.
    fn take_partial_chunk(&mut self) -> Option<Bytes> {
        let chunk = self.current_chunk.take()?;
        let rest = chunk.slice(self.chunk_offset..);
        self.chunk_offset = 0;
        (!rest.is_empty()).then_some(rest)
    }
}

impl futures_util::Stream for ResponseStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(rest) = self.take_partial_chunk() {
            return Poll::Ready(Some(Ok(rest)));
        }
        self.body.poll_chunk(cx)
    }
}

impl tokio::io::AsyncRead for ResponseStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if let Some(ref chunk) = self.current_chunk {
                let remaining = chunk.len() - self.chunk_offset;
                if remaining > 0 {
                    let to_copy = std::cmp::min(remaining, buf.remaining());
                    let start = self.chunk_offset;
                    buf.put_slice(&chunk[start..start + to_copy]);
                    self.chunk_offset += to_copy;
                    return Poll::Ready(Ok(()));
                }
                self.current_chunk = None;
                self.chunk_offset = 0;
            }

            match self.body.poll_chunk(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    self.current_chunk = Some(chunk);
                    self.chunk_offset = 0;
                }
                Poll::Ready(Some(Err(error))) => {
                    let kind = match error {
                        Error::Timeout => std::io::ErrorKind::TimedOut,
                        _ => std::io::ErrorKind::Other,
                    };
                    return Poll::Ready(Err(std::io::Error::new(kind, error)));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
