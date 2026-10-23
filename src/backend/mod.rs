//! Backend abstraction for HTTP client implementations

pub mod cookie_store;
pub mod types;

#[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
pub mod foundation;

#[cfg(all(feature = "backend-winhttp", windows))]
pub mod windows;

#[cfg(all(feature = "backend-android", target_os = "android"))]
pub mod android;

#[cfg(all(feature = "backend-android", target_os = "android"))]
pub use android::{check_permission, list_permissions, start_netlog, stop_netlog, test_dns};

#[cfg(feature = "backend-reqwest")]
pub mod reqwest;

pub use cookie_store::CookieJar;

use crate::{
    Error, Result,
    cookies::{Cookie, CookieAcceptPolicy},
};
use std::time::Duration;
use types::{BackendRequest, BackendResponse};
use url::Url;

fn no_backend_error() -> Error {
    Error::Internal("No HTTP backend is enabled for this target".to_string())
}

/// Configuration for backend creation
#[derive(Clone, Debug, Default)]
pub struct BackendConfig {
    /// Request timeout
    pub timeout: Option<Duration>,
    /// User agent string
    pub user_agent: Option<String>,
    /// Whether to ignore certificate errors
    pub ignore_certificate_errors: Option<bool>,
    /// Default headers to add to all requests
    pub default_headers: Option<http::HeaderMap>,
    /// Enable or disable cookies
    pub use_cookies: Option<bool>,
    /// Custom cookie jar
    pub cookie_jar: Option<crate::CookieJar>,
    /// HTTP proxy configuration
    pub http_proxy: Option<crate::client::ProxyConfig>,
    /// HTTPS proxy configuration
    pub https_proxy: Option<crate::client::ProxyConfig>,
    /// SOCKS proxy configuration
    pub socks_proxy: Option<crate::client::ProxyConfig>,
}

/// HTTP client backend implementations
#[derive(Clone)]
#[non_exhaustive]
pub enum Backend {
    /// Native Apple implementation using NSURLSession
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    Foundation(foundation::FoundationBackend),

    /// Native Windows implementation using WinRT HTTP
    #[cfg(all(feature = "backend-winhttp", windows))]
    Windows(windows::WindowsBackend),

    /// Native Android implementation using Cronet
    #[cfg(all(feature = "backend-android", target_os = "android"))]
    Android(android::AndroidBackend),

    /// Cross-platform implementation using reqwest
    #[cfg(feature = "backend-reqwest")]
    Reqwest(reqwest::ReqwestBackend),
}

impl Backend {
    /// Auto-select the best backend compiled in for this target.
    ///
    /// The native stack is preferred (Foundation on Apple targets, WinHTTP on
    /// Windows, Cronet on Android) when its feature is enabled; otherwise
    /// reqwest is used if `backend-reqwest` is enabled.
    pub fn default_for_platform() -> Result<Self> {
        Self::with_config(
            crate::client::BackendType::fallback().ok_or_else(no_backend_error)?,
            BackendConfig::default(),
        )
    }

    /// Create the given backend type with configuration.
    pub fn with_config(
        backend_type: crate::client::BackendType,
        config: BackendConfig,
    ) -> Result<Self> {
        use crate::client::BackendType;
        match backend_type {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            BackendType::Foundation => Self::foundation_with_config(config),
            #[cfg(all(feature = "backend-winhttp", windows))]
            BackendType::Windows => Self::windows_with_config(config),
            #[cfg(all(feature = "backend-android", target_os = "android"))]
            BackendType::Android => Self::android_with_config(config),
            #[cfg(feature = "backend-reqwest")]
            BackendType::Reqwest => Self::reqwest_with_config(config),
            #[allow(unreachable_patterns)]
            _ => Err(no_backend_error()),
        }
    }

    /// Explicitly use reqwest backend (works on all platforms)
    #[cfg(feature = "backend-reqwest")]
    pub fn reqwest() -> Result<Self> {
        Ok(Backend::Reqwest(reqwest::ReqwestBackend::new()?))
    }

    /// Use Foundation backend (Apple only)
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    pub fn foundation() -> Result<Self> {
        Ok(Backend::Foundation(foundation::FoundationBackend::new()?))
    }

    /// Use Foundation backend with configuration (Apple only)
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    pub fn foundation_with_config(config: BackendConfig) -> Result<Self> {
        Ok(Backend::Foundation(
            foundation::FoundationBackend::with_config(config)?,
        ))
    }

    /// Use Windows backend (Windows only)
    #[cfg(all(feature = "backend-winhttp", windows))]
    pub fn windows() -> Result<Self> {
        Ok(Backend::Windows(windows::WindowsBackend::new()?))
    }

    /// Use Windows backend with configuration (Windows only)
    #[cfg(all(feature = "backend-winhttp", windows))]
    pub fn windows_with_config(config: BackendConfig) -> Result<Self> {
        Ok(Backend::Windows(windows::WindowsBackend::with_config(
            config,
        )?))
    }

    /// Use Android backend (Android only)
    #[cfg(all(feature = "backend-android", target_os = "android"))]
    pub fn android() -> Result<Self> {
        Ok(Backend::Android(android::AndroidBackend::new()?))
    }

    /// Use Android backend with configuration (Android only)
    #[cfg(all(feature = "backend-android", target_os = "android"))]
    pub fn android_with_config(config: BackendConfig) -> Result<Self> {
        Ok(Backend::Android(android::AndroidBackend::with_config(
            config,
        )?))
    }

    /// Use reqwest backend with configuration
    #[cfg(feature = "backend-reqwest")]
    pub fn reqwest_with_config(config: BackendConfig) -> Result<Self> {
        Ok(Backend::Reqwest(reqwest::ReqwestBackend::with_config(
            config,
        )?))
    }

    /// Execute an HTTP request
    pub async fn execute(&self, request: BackendRequest) -> Result<BackendResponse> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            Backend::Foundation(f) => f.execute(request).await,

            #[cfg(all(feature = "backend-winhttp", windows))]
            Backend::Windows(w) => w.execute(request).await,

            #[cfg(all(feature = "backend-android", target_os = "android"))]
            Backend::Android(a) => a.execute(request).await,

            #[cfg(feature = "backend-reqwest")]
            Backend::Reqwest(r) => r.execute(request).await,
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Execute a background download that survives app termination
    pub async fn execute_background_download(
        &self,
        url: Url,
        file_path: std::path::PathBuf,
        session_identifier: Option<String>,
        headers: http::HeaderMap,
        progress_callback: Option<Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
        error_for_status: bool,
    ) -> Result<crate::client::download::DownloadResponse> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            Backend::Foundation(f) => {
                f.execute_background_download(
                    url,
                    file_path,
                    session_identifier,
                    headers,
                    progress_callback,
                    error_for_status,
                )
                .await
            }

            #[cfg(all(feature = "backend-winhttp", windows))]
            Backend::Windows(w) => {
                w.execute_background_download(
                    url,
                    file_path,
                    session_identifier,
                    headers,
                    progress_callback,
                    error_for_status,
                )
                .await
            }

            #[cfg(all(feature = "backend-android", target_os = "android"))]
            Backend::Android(a) => {
                a.execute_background_download(
                    url,
                    file_path,
                    session_identifier,
                    headers,
                    progress_callback,
                    error_for_status,
                )
                .await
            }

            #[cfg(feature = "backend-reqwest")]
            Backend::Reqwest(r) => {
                r.execute_background_download(
                    url,
                    file_path,
                    session_identifier,
                    headers,
                    progress_callback,
                    error_for_status,
                )
                .await
            }
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Get the cookie jar if configured
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            Backend::Foundation(f) => f.cookie_jar(),

            #[cfg(all(feature = "backend-winhttp", windows))]
            Backend::Windows(w) => w.cookie_jar(),

            #[cfg(all(feature = "backend-android", target_os = "android"))]
            Backend::Android(a) => a.cookie_jar(),

            #[cfg(feature = "backend-reqwest")]
            Backend::Reqwest(r) => r.cookie_jar(),
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }
}

/// Cookie storage backend implementations
#[derive(Clone, Debug)]
pub enum CookieStorage {
    /// Native Apple implementation using NSHTTPCookieStorage
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    Foundation(foundation::FoundationCookieStorage),

    /// RFC 6265 compliant implementation using cookie_store
    /// Used by Windows, Android, and Reqwest backends
    CookieStore(CookieJar),
}

impl CookieStorage {
    /// Create a new cookie storage with default configuration
    pub fn new(backend: Backend) -> Self {
        match backend {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            Backend::Foundation(_) => {
                CookieStorage::Foundation(foundation::FoundationCookieStorage::new())
            }
            // All non-Apple backends use the unified CookieStore implementation
            #[cfg(all(feature = "backend-winhttp", windows))]
            Backend::Windows(_) => CookieStorage::CookieStore(CookieJar::new()),

            #[cfg(all(feature = "backend-android", target_os = "android"))]
            Backend::Android(_) => CookieStorage::CookieStore(CookieJar::new()),

            #[cfg(feature = "backend-reqwest")]
            Backend::Reqwest(_) => CookieStorage::CookieStore(CookieJar::new()),
        }
    }

    /// Create a new cookie storage for a group container (Apple only)
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    pub fn for_group_container(identifier: &str) -> Result<Self> {
        Ok(CookieStorage::Foundation(
            foundation::FoundationCookieStorage::for_group_container(identifier),
        ))
    }

    /// Get all cookies
    pub fn all_cookies(&self) -> Vec<Cookie> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            CookieStorage::Foundation(storage) => storage.all_cookies(),

            CookieStorage::CookieStore(storage) => storage.all_cookies(),
        }
    }

    /// Get cookies for a specific URL
    pub fn cookies_for_url(&self, url: &str) -> Result<Vec<Cookie>> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            CookieStorage::Foundation(storage) => storage.cookies_for_url(url),

            CookieStorage::CookieStore(storage) => storage.cookies_for_url(url),
        }
    }

    /// Add a cookie
    pub fn add_cookie(&self, cookie: Cookie) -> Result<()> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            CookieStorage::Foundation(storage) => storage.add_cookie(cookie),

            CookieStorage::CookieStore(storage) => storage.add_cookie(cookie),
        }
    }

    /// Remove a cookie
    pub fn remove_cookie(&self, cookie: Cookie) -> Result<()> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            CookieStorage::Foundation(storage) => storage.remove_cookie(cookie),

            CookieStorage::CookieStore(storage) => storage.remove_cookie(cookie),
        }
    }

    /// Clear all cookies
    pub fn clear(&self) {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            CookieStorage::Foundation(storage) => storage.clear(),

            CookieStorage::CookieStore(storage) => storage.clear(),
        }
    }

    /// Set cookie acceptance policy
    pub fn set_cookie_accept_policy(&self, policy: CookieAcceptPolicy) {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            CookieStorage::Foundation(storage) => storage.set_cookie_accept_policy(policy),

            CookieStorage::CookieStore(storage) => storage.set_cookie_accept_policy(policy),
        }
    }
}
