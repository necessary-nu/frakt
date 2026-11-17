//! Cookie management using backend abstraction

use crate::{
    Result,
    backend::{Backend, CookieStorage},
};

/// Policy for cookie acceptance.
///
/// This enum defines which cookies a jar stores from the responses of the
/// clients that use it. It never affects cookies added with
/// [`CookieJar::add_cookie`]. On the Foundation backend it is
/// NSHTTPCookieAcceptPolicy.
///
/// # Examples
///
/// ```rust
/// use frakt::{Client, CookieAcceptPolicy};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::builder()
///     .use_cookies(true)
///     .build()?;
///
/// if let Some(jar) = client.cookie_jar() {
///     jar.set_cookie_accept_policy(CookieAcceptPolicy::Always);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookieAcceptPolicy {
    /// Accept all cookies.
    ///
    /// Cookies will be accepted from all domains and stored in the cookie jar.
    Always,
    /// Never accept cookies.
    ///
    /// All cookies will be rejected and not stored.
    Never,
    /// Accept cookies only from the main document domain.
    ///
    /// frakt has no documents, so the main document is the URL the request
    /// was made to. A response's cookies are stored only if the response
    /// came from that URL's host, or from a subdomain or parent domain of
    /// it (`example.com` and `www.example.com` are one domain;
    /// `a.example.com` and `b.example.com` are not, and neither are a
    /// single-label host and its subdomains). In practice: cookies set by
    /// another host that a redirect led to are dropped.
    ///
    /// On the Foundation backend the comparison is CFNetwork's own, against
    /// the same main document.
    OnlyFromMainDocumentDomain,
}

/// A cookie jar that manages HTTP cookies using backend abstraction.
///
/// `CookieJar` provides a high-level interface for managing HTTP cookies. It uses
/// the appropriate backend for the platform and provides methods for adding, removing, and querying cookies.
/// A client that keeps cookies sends them from its jar and stores into it;
/// clones of a jar share one store. Each cookie is stored for the origin
/// whose response set it, redirects included, and sent only where it
/// applies.
///
/// Its `Debug` output and [`Cookie`]'s leave cookie values out.
///
/// # Examples
///
/// ```rust
/// use frakt::{Client, CookieJar, Cookie};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a client with cookies enabled
/// let client = Client::builder()
///     .use_cookies(true)
///     .build()?;
///
/// // Access the cookie jar
/// if let Some(jar) = client.cookie_jar() {
///     // Add a custom cookie
///     let cookie = Cookie::new("session", "abc123")
///         .domain("example.com")
///         .path("/")
///         .secure(true);
///     jar.add_cookie(cookie)?;
///
///     // Get all cookies
///     let cookies = jar.all_cookies();
///     println!("Found {} cookies", cookies.len());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CookieJar {
    storage: CookieStorage,
}

impl CookieJar {
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    pub(crate) fn foundation_storage(
        &self,
    ) -> Option<&crate::backend::foundation::FoundationCookieStorage> {
        match &self.storage {
            CookieStorage::Foundation(storage) => Some(storage),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// The `cookie_store` behind this jar, for the backends that send and
    /// store cookies themselves.
    #[cfg_attr(
        not(any(
            feature = "backend-reqwest",
            all(feature = "backend-winhttp", windows),
            all(feature = "backend-android", target_os = "android")
        )),
        allow(dead_code)
    )]
    pub(crate) fn cookie_store(&self) -> Option<&crate::backend::CookieJar> {
        match &self.storage {
            CookieStorage::CookieStore(store) => Some(store),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    #[cfg_attr(
        not(any(
            feature = "backend-reqwest",
            all(feature = "backend-winhttp", windows),
            all(feature = "backend-android", target_os = "android")
        )),
        allow(dead_code)
    )]
    pub(crate) fn with_cookie_store() -> Self {
        Self {
            storage: CookieStorage::CookieStore(crate::backend::CookieJar::new()),
        }
    }

    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    pub(crate) fn with_foundation_storage(
        storage: crate::backend::foundation::FoundationCookieStorage,
    ) -> Self {
        Self {
            storage: CookieStorage::Foundation(storage),
        }
    }

    /// Create a new cookie jar with default storage.
    ///
    /// The jar's storage matches `backend`: NSHTTPCookieStorage (the shared
    /// storage) for the Foundation backend, an in-memory RFC 6265 store for
    /// the others. Pass it to [`ClientBuilder::cookie_jar`](crate::ClientBuilder::cookie_jar)
    /// of a client using the same kind of backend; building a client with a
    /// jar its backend cannot use fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::{CookieJar, backend::Backend};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let backend = Backend::reqwest()?;
    /// let jar = CookieJar::new(backend);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(backend: Backend) -> Self {
        Self {
            storage: CookieStorage::new(backend),
        }
    }

    /// Create a new cookie jar with storage for a specific group container.
    ///
    /// This creates a cookie jar that uses a separate cookie storage for the specified
    /// group container identifier. This is useful for app extensions or when you need
    /// isolated cookie storage. Only available on Apple platforms.
    ///
    /// # Arguments
    ///
    /// * `identifier` - The group container identifier
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::CookieJar;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    /// let jar = CookieJar::for_group_container("group.com.example.app")?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    pub fn for_group_container(identifier: &str) -> Result<Self> {
        Ok(Self {
            storage: CookieStorage::for_group_container(identifier)?,
        })
    }

    /// Get all cookies stored in this jar.
    ///
    /// Returns a vector containing all cookies currently stored in the cookie jar.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::{Client, Cookie};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder().use_cookies(true).build()?;
    ///
    /// if let Some(jar) = client.cookie_jar() {
    ///     let cookies = jar.all_cookies();
    ///     for cookie in cookies {
    ///         println!("Cookie: {}={}", cookie.name, cookie.value);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn all_cookies(&self) -> Vec<Cookie> {
        self.storage.all_cookies()
    }

    /// Get cookies for a specific URL.
    ///
    /// Returns only the cookies that would be sent with a request to the given URL,
    /// taking into account domain, path, and security restrictions.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to get cookies for
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::{Client, Cookie};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder().use_cookies(true).build()?;
    ///
    /// if let Some(jar) = client.cookie_jar() {
    ///     let cookies = jar.cookies_for_url("https://example.com/api")?;
    ///     println!("Found {} cookies for example.com", cookies.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn cookies_for_url(&self, url: &str) -> Result<Vec<Cookie>> {
        self.storage.cookies_for_url(url)
    }

    /// Add a cookie to the jar.
    ///
    /// The cookie will be stored and automatically sent with future requests
    /// that match the cookie's domain, path, and security requirements.
    ///
    /// # Arguments
    ///
    /// * `cookie` - The cookie to add
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::{Client, Cookie};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder().use_cookies(true).build()?;
    ///
    /// if let Some(jar) = client.cookie_jar() {
    ///     let cookie = Cookie::new("session_id", "abc123")
    ///         .domain("example.com")
    ///         .path("/")
    ///         .secure(true);
    ///     jar.add_cookie(cookie)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_cookie(&self, cookie: Cookie) -> Result<()> {
        self.storage.add_cookie(cookie)
    }

    /// Remove a cookie from the jar.
    ///
    /// The cookie will no longer be stored or sent with requests. It is
    /// identified by its name, domain and path.
    ///
    /// # Arguments
    ///
    /// * `cookie` - The cookie to remove
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::{Client, Cookie};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder().use_cookies(true).build()?;
    ///
    /// if let Some(jar) = client.cookie_jar() {
    ///     let cookie = Cookie::new("session_id", "abc123")
    ///         .domain("example.com");
    ///     jar.remove_cookie(cookie)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove_cookie(&self, cookie: Cookie) -> Result<()> {
        self.storage.remove_cookie(cookie)
    }

    /// Remove all cookies from the jar.
    ///
    /// This clears all stored cookies. Use with caution as this will affect
    /// all HTTP clients using the same cookie storage.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::Client;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder().use_cookies(true).build()?;
    ///
    /// if let Some(jar) = client.cookie_jar() {
    ///     jar.clear();
    ///     println!("All cookies cleared");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn clear(&self) {
        self.storage.clear()
    }

    /// Set the cookie acceptance policy.
    ///
    /// This controls which cookies received from servers are stored; see
    /// [`CookieAcceptPolicy`]. The policy affects all HTTP clients using
    /// the same cookie storage, from their next response on.
    ///
    /// # Arguments
    ///
    /// * `policy` - The cookie acceptance policy to use
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::{Client, CookieAcceptPolicy};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder().use_cookies(true).build()?;
    ///
    /// if let Some(jar) = client.cookie_jar() {
    ///     // Only accept cookies from the main document domain
    ///     jar.set_cookie_accept_policy(CookieAcceptPolicy::OnlyFromMainDocumentDomain);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_cookie_accept_policy(&self, policy: CookieAcceptPolicy) {
        self.storage.set_cookie_accept_policy(policy)
    }
}

/// An HTTP cookie.
///
/// This struct represents an HTTP cookie with all its attributes including name, value,
/// domain, path, security flags, and expiration. Cookies can be created manually or
/// extracted from HTTP responses.
///
/// # Examples
///
/// ```rust
/// use frakt::Cookie;
///
/// // Create a simple session cookie
/// let session_cookie = Cookie::new("session_id", "abc123")
///     .domain("example.com")
///     .path("/")
///     .secure(true);
///
/// // Create a cookie with expiration
/// let persistent_cookie = Cookie::new("user_pref", "dark_mode")
///     .domain("myapp.com")
///     .path("/settings")
///     .expires("Wed, 09 Jun 2024 10:18:14 GMT");
/// ```
#[derive(Clone)]
pub struct Cookie {
    /// Cookie name
    pub name: String,
    /// Cookie value
    pub value: String,
    /// Domain
    pub domain: String,
    /// Path
    pub path: String,
    /// Whether the cookie is secure (HTTPS only)
    pub secure: bool,
    /// Whether the cookie is HTTP only
    pub http_only: bool,
    /// Expiration date as string (None for session cookies)
    pub expires: Option<String>,
}

/// A cookie's value is usually a session credential, so it is not shown.
impl std::fmt::Debug for Cookie {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cookie")
            .field("name", &self.name)
            .field("value", &crate::redact::Redacted)
            .field("domain", &self.domain)
            .field("path", &self.path)
            .field("secure", &self.secure)
            .field("http_only", &self.http_only)
            .field("expires", &self.expires)
            .finish()
    }
}

impl Cookie {
    /// Create a new cookie with the given name and value.
    ///
    /// The cookie is created with default values: domain empty, path "/",
    /// not secure, not HTTP-only, and no expiration (session cookie).
    ///
    /// # Arguments
    ///
    /// * `name` - The cookie name
    /// * `value` - The cookie value
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::Cookie;
    ///
    /// let cookie = Cookie::new("session_id", "abc123");
    /// assert_eq!(cookie.name, "session_id");
    /// assert_eq!(cookie.value, "abc123");
    /// assert_eq!(cookie.path, "/");
    /// ```
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            domain: String::new(),
            path: "/".to_string(),
            secure: false,
            http_only: false,
            expires: None,
        }
    }

    /// Set the domain for this cookie.
    ///
    /// The domain determines which hosts will receive this cookie.
    /// If not set, the cookie will only be sent to the exact host that set it.
    ///
    /// # Arguments
    ///
    /// * `domain` - The domain (e.g., "example.com" or ".example.com")
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::Cookie;
    ///
    /// let cookie = Cookie::new("session", "abc123")
    ///     .domain("example.com");
    /// ```
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = domain.into();
        self
    }

    /// Set the path for this cookie.
    ///
    /// The path determines which URLs will receive this cookie.
    /// Only URLs that start with this path will get the cookie.
    ///
    /// # Arguments
    ///
    /// * `path` - The path (e.g., "/" or "/api")
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::Cookie;
    ///
    /// let cookie = Cookie::new("api_token", "token123")
    ///     .path("/api");
    /// ```
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Set whether the cookie should only be sent over HTTPS.
    ///
    /// Secure cookies are only sent over encrypted connections,
    /// helping to prevent interception by attackers.
    ///
    /// # Arguments
    ///
    /// * `secure` - Whether the cookie should be secure
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::Cookie;
    ///
    /// let cookie = Cookie::new("auth", "sensitive_token")
    ///     .secure(true);
    /// ```
    pub fn secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    /// Set whether the cookie should be HTTP-only.
    ///
    /// HTTP-only cookies cannot be accessed via JavaScript,
    /// helping to prevent XSS attacks.
    ///
    /// # Arguments
    ///
    /// * `http_only` - Whether the cookie should be HTTP-only
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::Cookie;
    ///
    /// let cookie = Cookie::new("session", "abc123")
    ///     .http_only(true);
    /// ```
    pub fn http_only(mut self, http_only: bool) -> Self {
        self.http_only = http_only;
        self
    }

    /// Set the expiration date for this cookie.
    ///
    /// The expiration date should be in RFC 1123 format. If not set,
    /// the cookie will be a session cookie that expires when the browser closes.
    ///
    /// # Arguments
    ///
    /// * `expires` - The expiration date string
    ///
    /// # Examples
    ///
    /// ```rust
    /// use frakt::Cookie;
    ///
    /// let cookie = Cookie::new("remember_me", "true")
    ///     .expires("Wed, 09 Jun 2024 10:18:14 GMT");
    /// ```
    pub fn expires(mut self, expires: impl Into<String>) -> Self {
        self.expires = Some(expires.into());
        self
    }
}
