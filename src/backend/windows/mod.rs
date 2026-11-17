//! Windows backend using WinRT HTTP and BITS

pub mod bits;
pub mod error;
pub mod http_client;
pub mod websocket;

pub use bits::BitsDownloadManager;
pub use error::*;
pub use websocket::{WindowsWebSocket, WindowsWebSocketBuilder};

use crate::backend::types::{BackendRequest, BackendResponse};
use crate::{Error, Result};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Windows backend using WinHTTP client
#[derive(Clone)]
pub struct WindowsBackend {
    /// The WinHTTP session every request of this backend uses
    session: Arc<http_client::Session>,
    /// The jar requests send cookies from and store them into, which is the
    /// one `cookie_jar` hands out; `None` when cookies are off
    cookie_jar: Option<crate::CookieJar>,
    /// Default headers to add to all requests
    default_headers: Option<http::HeaderMap>,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    total_timeout: Option<Duration>,
    ignore_certificate_errors: bool,
}

impl WindowsBackend {
    /// Create a new Windows backend with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(crate::backend::BackendConfig::default())
    }

    /// Create a new Windows backend with configuration
    pub fn with_config(config: crate::backend::BackendConfig) -> Result<Self> {
        let cookie_jar = config.cookie_store_jar()?;
        let user_agent = config.user_agent.as_deref().unwrap_or("frakt/1.0");
        let proxy = [("http", &config.http_proxy), ("https", &config.https_proxy)]
            .into_iter()
            .filter_map(|(scheme, proxy)| {
                proxy
                    .as_ref()
                    .map(|proxy| format!("{}={}:{}", scheme, proxy.host, proxy.port))
            })
            .collect::<Vec<_>>()
            .join(" ");
        let session =
            http_client::Session::open(user_agent, (!proxy.is_empty()).then_some(proxy.as_str()))?;

        Ok(Self {
            session: Arc::new(session),
            cookie_jar,
            default_headers: config.default_headers,
            connect_timeout: config.connect_timeout,
            read_timeout: config.read_timeout,
            total_timeout: config.timeout,
            ignore_certificate_errors: config.ignore_certificate_errors.unwrap_or(false),
        })
    }

    /// Execute an HTTP request using WinHTTP
    pub async fn execute(&self, request: BackendRequest) -> Result<BackendResponse> {
        http_client::execute(
            http_client::RequestConfig {
                session: &self.session,
                default_headers: self.default_headers.as_ref(),
                cookie_jar: self.cookie_jar.as_ref().and_then(|jar| jar.cookie_store()),
                connect_timeout: self.connect_timeout,
                read_timeout: self.read_timeout,
                ignore_certificate_errors: self.ignore_certificate_errors,
            },
            request,
        )
        .await
    }

    /// Download `url` to `file_path`, through BITS when it can do the job
    /// and through WinHTTP otherwise.
    pub async fn execute_background_download(
        &self,
        url: Url,
        file_path: std::path::PathBuf,
        session_identifier: Option<String>,
        headers: http::HeaderMap,
        progress_callback: Option<crate::backend::types::BoxedProgressCallback>,
        error_for_status: bool,
    ) -> Result<crate::client::download::DownloadResponse> {
        // The BITS manager is created here rather than kept in the backend:
        // it is a worker thread holding the BITS COM interfaces, which a
        // backend that never downloads in the background should not start,
        // and BITS being unavailable for one download says nothing about
        // the next.
        let bits_manager = BitsDownloadManager::new()
            .await
            .inspect_err(|error| tracing::debug!("Not using BITS: {}", error));
        if let Ok(bits_manager) = bits_manager {
            // BITS sends only what it is given, so it is given what a WinHTTP
            // request would carry: the default headers, the request's own in
            // place of them, and the jar's cookies. It reports no response
            // headers, so cookies the server sets are not stored.
            let mut bits_headers = self.default_headers.clone().unwrap_or_default();
            for name in headers.keys() {
                bits_headers.remove(name);
            }
            for (name, value) in &headers {
                bits_headers.append(name, value.clone());
            }
            if let Some(jar) = self.cookie_jar.as_ref().and_then(|jar| jar.cookie_store()) {
                jar.add_cookie_header(&url, &mut bits_headers);
            }

            match bits_manager
                .start_background_download(
                    url.clone(),
                    file_path.clone(),
                    session_identifier,
                    &bits_headers,
                    progress_callback.as_deref(),
                    error_for_status,
                )
                .await
            {
                Ok(response) => return Ok(response),
                // The server's answer, which WinHTTP would only be given
                // again.
                Err(error @ Error::HttpStatus(_)) => return Err(error),
                Err(e) => {
                    tracing::warn!("BITS download failed, falling back to WinHTTP: {}", e);
                }
            }
        }

        // An ordinary download: the destination is replaced only once the
        // whole body is on disk.
        crate::client::DownloadBuilder::new(
            crate::backend::Backend::Windows(self.clone()),
            url,
            file_path,
        )
        .headers(headers)
        .progress_callback(progress_callback)
        .error_for_status(error_for_status)
        .send()
        .await
        .map_err(|error| match error {
            // A background download has no response to hand over.
            Error::HttpError(response) => Error::HttpStatus(response.status()),
            error => error,
        })
    }

    pub(crate) fn default_headers(&self) -> Option<&http::HeaderMap> {
        self.default_headers.as_ref()
    }

    pub(crate) fn total_timeout(&self) -> Option<Duration> {
        self.total_timeout
    }

    /// Get the cookie jar if configured
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        self.cookie_jar.as_ref()
    }
}
