//! Reqwest backend for cross-platform HTTP support

mod background;

pub mod websocket;
pub use websocket::{ReqwestWebSocket, ReqwestWebSocketBuilder};

use crate::backend::types::{BackendRequest, BackendResponse, ProgressCallback};
use crate::{Error, Result};
use bytes::Bytes;
use futures_util::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;
use url::Url;

/// A stream that tracks upload progress as data is consumed
struct ProgressTrackingStream {
    data: Bytes,
    position: usize,
    total: u64,
    uploaded: Arc<AtomicU64>,
    callback: Arc<ProgressCallback>,
    chunk_size: usize,
}

impl ProgressTrackingStream {
    fn new(data: Bytes, callback: ProgressCallback, chunk_size: usize) -> Self {
        let total = data.len() as u64;
        let uploaded = Arc::new(AtomicU64::new(0));

        // Call progress callback at start
        callback(0, Some(total));

        Self {
            data,
            position: 0,
            total,
            uploaded,
            callback: Arc::new(callback),
            chunk_size,
        }
    }
}

impl Stream for ProgressTrackingStream {
    type Item = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let remaining = self.data.len() - self.position;
        if remaining == 0 {
            return Poll::Ready(None);
        }

        let chunk_size = std::cmp::min(self.chunk_size, remaining);
        let chunk = self.data.slice(self.position..self.position + chunk_size);
        self.position += chunk_size;

        // Update progress
        let uploaded = self
            .uploaded
            .fetch_add(chunk_size as u64, Ordering::Relaxed)
            + chunk_size as u64;
        (self.callback)(uploaded, Some(self.total));

        Poll::Ready(Some(Ok(chunk)))
    }
}

/// A reqwest body polled directly by the consumer, so hyper reads from the
/// socket only as fast as the consumer reads and dropping it closes the
/// connection.
struct ReqwestBody {
    inner: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    read_timeout: Option<Duration>,
    /// Armed only while the consumer is waiting on the network.
    idle: Option<Pin<Box<Sleep>>>,
    done: bool,
}

impl Stream for ReqwestBody {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.idle = None;
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.done = true;
                Poll::Ready(Some(Err(map_reqwest_error(error))))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                let Some(timeout) = self.read_timeout else {
                    return Poll::Pending;
                };
                let idle = self
                    .idle
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
                if idle.as_mut().poll(cx).is_ready() {
                    self.done = true;
                    return Poll::Ready(Some(Err(Error::Timeout)));
                }
                Poll::Pending
            }
        }
    }
}

fn map_reqwest_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Timeout
    } else {
        Error::Network {
            code: -1,
            message: format!("Request failed: {}", error),
        }
    }
}

/// Reqwest backend for cross-platform HTTP
#[derive(Clone)]
pub struct ReqwestBackend {
    client: reqwest::Client,
    cookie_jar: Option<crate::CookieJar>,
    read_timeout: Option<Duration>,
    total_timeout: Option<Duration>,
}

impl ReqwestBackend {
    /// Create a new Reqwest backend
    pub fn new() -> Result<Self> {
        Self::with_config(crate::backend::BackendConfig::default())
    }

    /// Create a new Reqwest backend with configuration
    pub fn with_config(config: crate::backend::BackendConfig) -> Result<Self> {
        let mut builder = reqwest::Client::builder();

        // reqwest's own `timeout` is a total deadline and its `read_timeout`
        // cannot be overridden per request, so both are enforced here instead.
        if let Some(timeout) = config.connect_timeout {
            builder = builder.connect_timeout(timeout);
        }

        // Apply user agent configuration
        if let Some(user_agent) = config.user_agent {
            builder = builder.user_agent(user_agent);
        }

        // Apply certificate validation settings
        if let Some(ignore_certs) = config.ignore_certificate_errors {
            if ignore_certs {
                builder = builder.danger_accept_invalid_certs(true);
            }
        }

        // Apply default headers
        if let Some(default_headers) = config.default_headers {
            // Convert http::HeaderMap to reqwest::header::HeaderMap
            let mut reqwest_headers = reqwest::header::HeaderMap::new();
            for (name, value) in default_headers {
                if let Some(name) = name {
                    if let Ok(reqwest_name) =
                        reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes())
                    {
                        if let Ok(reqwest_value) =
                            reqwest::header::HeaderValue::from_bytes(value.as_bytes())
                        {
                            reqwest_headers.insert(reqwest_name, reqwest_value);
                        }
                    }
                }
            }
            builder = builder.default_headers(reqwest_headers);
        }

        // Apply cookie configuration
        if let Some(use_cookies) = config.use_cookies {
            if use_cookies {
                builder = builder.cookie_store(true);
            }
        }

        // Apply proxy configurations
        if let Some(http_proxy) = config.http_proxy {
            let proxy_url = format!("http://{}:{}", http_proxy.host, http_proxy.port);
            let mut proxy = reqwest::Proxy::http(&proxy_url)
                .map_err(|e| Error::Internal(format!("Invalid HTTP proxy: {}", e)))?;

            if let (Some(username), Some(password)) = (&http_proxy.username, &http_proxy.password) {
                proxy = proxy.basic_auth(username, password);
            }
            builder = builder.proxy(proxy);
        }

        if let Some(https_proxy) = config.https_proxy {
            let proxy_url = format!("https://{}:{}", https_proxy.host, https_proxy.port);
            let mut proxy = reqwest::Proxy::https(&proxy_url)
                .map_err(|e| Error::Internal(format!("Invalid HTTPS proxy: {}", e)))?;

            if let (Some(username), Some(password)) = (&https_proxy.username, &https_proxy.password)
            {
                proxy = proxy.basic_auth(username, password);
            }
            builder = builder.proxy(proxy);
        }

        if let Some(socks_proxy) = config.socks_proxy {
            let proxy_url = format!("socks5://{}:{}", socks_proxy.host, socks_proxy.port);
            let mut proxy = reqwest::Proxy::all(&proxy_url)
                .map_err(|e| Error::Internal(format!("Invalid SOCKS proxy: {}", e)))?;

            if let (Some(username), Some(password)) = (&socks_proxy.username, &socks_proxy.password)
            {
                proxy = proxy.basic_auth(username, password);
            }
            builder = builder.proxy(proxy);
        }

        let client = builder
            .build()
            .map_err(|e| Error::Internal(format!("Failed to create reqwest client: {}", e)))?;

        Ok(Self {
            client,
            cookie_jar: config.cookie_jar,
            read_timeout: config.read_timeout,
            total_timeout: config.timeout,
        })
    }

    pub(crate) fn total_timeout(&self) -> Option<Duration> {
        self.total_timeout
    }

    /// Execute an HTTP request using reqwest
    pub async fn execute(&self, request: BackendRequest) -> Result<BackendResponse> {
        // Validate URL scheme
        match request.url.scheme() {
            "http" | "https" => {}
            _ => {
                return Err(Error::InvalidUrl);
            }
        }

        // Build request
        let read_timeout = request.read_timeout.or(self.read_timeout);
        let mut req_builder = self.client.request(request.method, request.url);

        // Add headers
        for (name, value) in &request.headers {
            req_builder = req_builder.header(name, value);
        }

        // Add body
        if let Some(body) = request.body {
            match &body {
                crate::body::Body::Multipart { parts } => {
                    let mut form = reqwest::multipart::Form::new();
                    for part in parts {
                        let mut part_builder =
                            reqwest::multipart::Part::bytes(part.content.to_vec());

                        if let Some(filename) = &part.filename {
                            part_builder = part_builder.file_name(filename.clone());
                        }

                        if let Some(content_type) = &part.content_type {
                            part_builder = part_builder.mime_str(content_type).map_err(|e| {
                                Error::Internal(format!("Invalid content type: {}", e))
                            })?;
                        }

                        form = form.part(part.name.clone(), part_builder);
                    }
                    req_builder = req_builder.multipart(form);
                }
                crate::body::Body::Form { .. } => {
                    if let Some(callback) = request.progress_callback.as_ref() {
                        // For form data with progress tracking, we need to convert to bytes first
                        let bytes = self.body_to_bytes(&body)?;
                        let stream = ProgressTrackingStream::new(bytes, callback.clone(), 8192);
                        req_builder = req_builder
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .body(reqwest::Body::wrap_stream(stream));
                    } else {
                        req_builder = req_builder
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .body(self.convert_body(body)?);
                    }
                }
                _ => {
                    if let Some(callback) = &request.progress_callback {
                        // Use progress tracking for upload
                        let bytes = self.body_to_bytes(&body)?;
                        let stream = ProgressTrackingStream::new(bytes, callback.clone(), 8192);
                        req_builder = req_builder.body(reqwest::Body::wrap_stream(stream));
                    } else {
                        req_builder = req_builder.body(self.convert_body(body)?);
                    }
                }
            }
        }

        let send = req_builder.send();
        let response = match read_timeout {
            Some(timeout) => tokio::time::timeout(timeout, send)
                .await
                .map_err(|_| Error::Timeout)?,
            None => send.await,
        }
        .map_err(map_reqwest_error)?;

        Ok(BackendResponse {
            status: response.status(),
            headers: response.headers().clone(),
            url: response.url().clone(),
            body: Box::pin(ReqwestBody {
                inner: Box::pin(response.bytes_stream()),
                read_timeout,
                idle: None,
                done: false,
            }),
            redirect_headers: vec![],
        })
    }

    fn convert_body(&self, body: crate::body::Body) -> Result<reqwest::Body> {
        match body {
            crate::body::Body::Empty => Ok(reqwest::Body::from("")),
            crate::body::Body::Bytes { content, .. } => Ok(reqwest::Body::from(content)),
            crate::body::Body::Form { fields } => {
                let form_data = fields
                    .iter()
                    .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                    .collect::<Vec<_>>()
                    .join("&");
                Ok(reqwest::Body::from(form_data))
            }
            crate::body::Body::Json { value } => {
                let json_bytes =
                    serde_json::to_vec(&value).map_err(|e| Error::Json(e.to_string()))?;
                Ok(reqwest::Body::from(json_bytes))
            }
            crate::body::Body::Multipart { .. } => {
                // Multipart is handled separately in the execute function
                Ok(reqwest::Body::from(""))
            }
        }
    }

    fn body_to_bytes(&self, body: &crate::body::Body) -> Result<Bytes> {
        match body {
            crate::body::Body::Empty => Ok(Bytes::from("")),
            crate::body::Body::Bytes { content, .. } => Ok(Bytes::from(content.clone())),
            crate::body::Body::Form { fields } => {
                let form_data = fields
                    .iter()
                    .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                    .collect::<Vec<_>>()
                    .join("&");
                Ok(Bytes::from(form_data))
            }
            crate::body::Body::Json { value } => {
                let json_bytes =
                    serde_json::to_vec(&value).map_err(|e| Error::Json(e.to_string()))?;
                Ok(Bytes::from(json_bytes))
            }
            crate::body::Body::Multipart { .. } => {
                // Multipart is handled separately in the execute function
                Ok(Bytes::from(""))
            }
        }
    }

    /// Get the underlying reqwest client
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Execute a background download
    pub async fn execute_background_download(
        &self,
        url: Url,
        file_path: std::path::PathBuf,
        session_identifier: Option<String>,
        headers: http::HeaderMap,
        progress_callback: Option<Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
        error_for_status: bool,
    ) -> Result<crate::client::download::DownloadResponse> {
        #[cfg(unix)]
        {
            background::execute_unix_background_download(
                &self.client,
                url,
                file_path,
                session_identifier,
                headers,
                progress_callback,
                error_for_status,
            )
            .await
        }

        #[cfg(not(unix))]
        {
            background::execute_resumable_background_download(
                &self.client,
                url,
                file_path,
                session_identifier,
                headers,
                progress_callback,
            )
            .await
        }
    }

    /// Get the cookie jar if configured
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        self.cookie_jar.as_ref()
    }
}
