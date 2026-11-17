//! Backend abstraction for HTTP client implementations

pub mod cookie_store;
// Foundation follows redirects itself and uses only the origin comparison.
#[cfg_attr(
    not(any(
        feature = "backend-reqwest",
        all(feature = "backend-winhttp", windows),
        all(feature = "backend-android", target_os = "android")
    )),
    allow(dead_code)
)]
pub(crate) mod redirect;
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

/// Default for [`BackendConfig::connect_timeout`].
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default for [`BackendConfig::read_timeout`].
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Configuration for backend creation
///
/// Timeouts mean the same thing on every backend:
///
/// - `connect_timeout` bounds name resolution plus connection setup where the
///   platform stack exposes that phase (reqwest, WinHTTP). Foundation and
///   Cronet do not, so there `read_timeout` bounds the wait for the response
///   to start instead.
/// - `read_timeout` bounds how long a transfer may wait on the network while
///   frakt wants data: for the response to start, and then for each next
///   chunk of the body. Time spent waiting for the consumer to read does not
///   count, so a slow consumer never trips it.
/// - `timeout` is a total deadline from sending the request to reading the
///   last byte of the body. It is off by default because it would cut off
///   large downloads.
///
/// Every backend asks for compressed bodies and decodes them, as its
/// platform stack does by default; [`Response::is_decoded`] says when it
/// did and [`Response::body_stats`] how many bytes crossed the wire. A
/// request with a `Range` header is sent with `Accept-Encoding: identity`
/// unless the caller (on the request or as a client default header) set
/// `Accept-Encoding` itself, because a byte range of a decoded body means
/// nothing. An explicit `Accept-Encoding: identity` is honoured everywhere.
/// With any other explicit `Accept-Encoding`, Foundation and Cronet still
/// decode, reqwest decodes gzip, deflate and br, and WinHTTP delivers the
/// body as the server encoded it (it would otherwise rewrite the header);
/// `is_decoded()` tells which happened.
///
/// Every backend follows redirects, up to ten on the backends that count
/// them (Foundation follows CFNetwork's own limit). A redirect to another
/// origin (scheme, host or port) is followed without the request's
/// credentials: `Authorization`, `Cookie`, `Proxy-Authorization` and any
/// other header whose name marks it as one, such as an API key or token.
/// Cookies come from the jar afresh for each hop, and a cookie a hop sets is
/// stored for that hop's origin. A redirect within the origin keeps every
/// header. 303, and 301 or 302 in answer to a POST, are followed with a GET
/// and no body; 307 and 308 repeat the request, body included.
///
/// [`Response::is_decoded`]: crate::Response::is_decoded
/// [`Response::body_stats`]: crate::Response::body_stats
#[derive(Clone)]
pub struct BackendConfig {
    /// Total deadline per request; `None` (the default) for no deadline
    pub timeout: Option<Duration>,
    /// Connect timeout; see the type documentation
    pub connect_timeout: Option<Duration>,
    /// Read (idle) timeout; see the type documentation
    pub read_timeout: Option<Duration>,
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

impl std::fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConfig")
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("read_timeout", &self.read_timeout)
            .field("user_agent", &self.user_agent)
            .field("ignore_certificate_errors", &self.ignore_certificate_errors)
            .field(
                "default_headers",
                &self
                    .default_headers
                    .as_ref()
                    .map(crate::redact::RedactedHeaders),
            )
            .field("use_cookies", &self.use_cookies)
            .field("cookie_jar", &self.cookie_jar)
            .field("http_proxy", &self.http_proxy)
            .field("https_proxy", &self.https_proxy)
            .field("socks_proxy", &self.socks_proxy)
            .finish()
    }
}

impl BackendConfig {
    /// Whether the client keeps cookies: `use_cookies` if set, otherwise
    /// whether a jar was supplied.
    pub(crate) fn cookies_enabled(&self) -> bool {
        self.use_cookies.unwrap_or(self.cookie_jar.is_some())
    }

    /// The jar a backend that keeps its cookies in a `cookie_store` (every
    /// backend but Foundation) sends from and stores into, which is also
    /// the one `Client::cookie_jar` hands out: the caller's jar, or a new
    /// one. `None` when cookies are off. A jar backed by another kind of
    /// storage cannot be honoured and is an error rather than being
    /// silently replaced.
    #[cfg_attr(
        not(any(
            feature = "backend-reqwest",
            all(feature = "backend-winhttp", windows),
            all(feature = "backend-android", target_os = "android")
        )),
        allow(dead_code)
    )]
    pub(crate) fn cookie_store_jar(&self) -> Result<Option<crate::CookieJar>> {
        if !self.cookies_enabled() {
            return Ok(None);
        }
        match &self.cookie_jar {
            Some(jar) if jar.cookie_store().is_some() => Ok(Some(jar.clone())),
            Some(_) => Err(Error::Internal(
                "The cookie jar is backed by NSHTTPCookieStorage, which only the Foundation \
                 backend can use"
                    .to_string(),
            )),
            None => Ok(Some(crate::CookieJar::with_cookie_store())),
        }
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            timeout: None,
            connect_timeout: Some(DEFAULT_CONNECT_TIMEOUT),
            read_timeout: Some(DEFAULT_READ_TIMEOUT),
            user_agent: None,
            ignore_certificate_errors: None,
            default_headers: None,
            use_cookies: None,
            cookie_jar: None,
            http_proxy: None,
            https_proxy: None,
            socks_proxy: None,
        }
    }
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
        progress_callback: Option<crate::backend::types::BoxedProgressCallback>,
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

    /// Whether the client's default headers include `name`.
    pub(crate) fn has_default_header(&self, name: &http::HeaderName) -> bool {
        let defaults = match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            Backend::Foundation(f) => f.default_headers(),

            #[cfg(all(feature = "backend-winhttp", windows))]
            Backend::Windows(w) => w.default_headers(),

            #[cfg(all(feature = "backend-android", target_os = "android"))]
            Backend::Android(a) => a.default_headers(),

            #[cfg(feature = "backend-reqwest")]
            Backend::Reqwest(r) => r.default_headers(),
            #[allow(unreachable_patterns)]
            _ => None,
        };
        defaults.is_some_and(|headers| headers.contains_key(name))
    }

    /// The client-wide total deadline per request, if configured
    pub(crate) fn total_timeout(&self) -> Option<Duration> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            Backend::Foundation(f) => f.total_timeout(),

            #[cfg(all(feature = "backend-winhttp", windows))]
            Backend::Windows(w) => w.total_timeout(),

            #[cfg(all(feature = "backend-android", target_os = "android"))]
            Backend::Android(a) => a.total_timeout(),

            #[cfg(feature = "backend-reqwest")]
            Backend::Reqwest(r) => r.total_timeout(),
            #[allow(unreachable_patterns)]
            _ => None,
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
