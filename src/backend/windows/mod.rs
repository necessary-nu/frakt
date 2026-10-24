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
    /// Optional cookie jar for this backend
    cookie_jar: Option<crate::CookieJar>,
    /// Cookie storage using cookie_store
    cookie_storage: Option<Arc<crate::backend::CookieJar>>,
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

        // Create cookie storage if cookies are enabled
        let cookie_storage = if config.use_cookies.unwrap_or(false) {
            Some(Arc::new(crate::backend::CookieJar::new()))
        } else {
            None
        };

        Ok(Self {
            session: Arc::new(session),
            cookie_jar: config.cookie_jar,
            cookie_storage,
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
                cookie_storage: self.cookie_storage.as_ref(),
                connect_timeout: self.connect_timeout,
                read_timeout: self.read_timeout,
                ignore_certificate_errors: self.ignore_certificate_errors,
            },
            request,
        )
        .await
    }

    /// Execute a background download
    pub async fn execute_background_download(
        &self,
        url: Url,
        file_path: std::path::PathBuf,
        session_identifier: Option<String>,
        headers: http::HeaderMap,
        progress_callback: Option<Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
        _error_for_status: bool,
    ) -> Result<crate::client::download::DownloadResponse> {
        // Try BITS first if available, fall back to regular download.
        // The BITS manager is created here rather than kept in the backend:
        // its COM interface is neither Send nor Sync, which made every
        // client and request future !Send, and creating it initialises COM
        // on the calling thread.
        if let Ok(bits_manager) = BitsDownloadManager::new() {
            // For now, disable progress callbacks with BITS to avoid lifetime issues
            // TODO: Fix progress callback lifetime management
            // TODO: Pass headers to BITS manager when it supports them
            match bits_manager
                .start_background_download(url.clone(), file_path.clone(), session_identifier, None)
                .await
            {
                Ok(response) => return Ok(response),
                Err(e) => {
                    tracing::warn!("BITS download failed, falling back to WinHTTP: {}", e);
                }
            }
        }
        let request = BackendRequest {
            method: http::Method::GET,
            url,
            headers, // Use the provided headers
            body: None,
            progress_callback: progress_callback
                .map(|cb| Arc::new(cb) as Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>),
            read_timeout: None,
        };

        let response = self.execute(request).await?;

        let status = response.status;
        let headers = response.headers;

        // Stream response to file
        let mut file = tokio::fs::File::create(&file_path)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create file: {}", e)))?;

        let mut body = response.body;
        let mut bytes_downloaded = 0u64;

        while let Some(chunk_result) = futures_util::StreamExt::next(&mut body).await {
            let chunk = chunk_result?;
            bytes_downloaded += chunk.len() as u64;

            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
                .await
                .map_err(|e| Error::Internal(format!("Failed to write to file: {}", e)))?;
        }

        Ok(crate::client::download::DownloadResponse {
            file_path,
            bytes_downloaded,
            status,
            headers,
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
