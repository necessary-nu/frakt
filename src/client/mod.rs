//! Client implementation using backend abstraction

pub mod background;
pub mod download;
pub mod upload;

use crate::backend::Backend;
use http::{HeaderMap, HeaderName, HeaderValue};

pub use background::BackgroundDownloadBuilder;
pub use download::{DownloadBuilder, DownloadResponse};
pub use upload::UploadBuilder;
use url::Url;

/// Proxy configuration for requests
#[derive(Clone)]
pub struct ProxyConfig {
    /// Proxy host
    pub host: String,
    /// Proxy port
    pub port: u16,
    /// Optional username for authentication
    pub username: Option<String>,
    /// Optional password for authentication
    pub password: Option<String>,
}

/// The credentials are not shown, only whether they are set.
impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::redact::redacted_option;
        f.debug_struct("ProxyConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &redacted_option(&self.username))
            .field("password", &redacted_option(&self.password))
            .finish()
    }
}

/// HTTP client for making requests
///
/// The client provides a high-level interface for making HTTP requests,
/// downloading files, uploading data, and establishing WebSocket connections.
/// It uses pluggable backends (Foundation on Apple platforms, reqwest on others)
/// to provide optimal performance and native integration.
pub struct Client {
    backend: Backend,
}

impl Client {
    /// Create a new HTTP client with default configuration.
    ///
    /// The client will automatically select the best backend for the current platform:
    /// - Apple platforms: Uses NSURLSession for native integration
    /// - Other platforms: Uses reqwest for cross-platform compatibility
    ///
    /// # Returns
    ///
    /// Returns a new `Client` instance ready to make HTTP requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP backend cannot be initialized.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client.get("https://httpbin.org/get")?.send().await?;
    /// println!("Status: {}", response.status());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new() -> crate::Result<Self> {
        Ok(Self {
            backend: Backend::default_for_platform()?,
        })
    }

    /// Create a client builder for advanced configuration.
    ///
    /// The builder provides a fluent interface for configuring client settings
    /// such as timeouts, headers, authentication, proxy settings, and backend selection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder()
    ///     .user_agent("MyApp/1.0")
    ///     .timeout(Duration::from_secs(30))
    ///     .header("X-API-Key", "secret")?
    ///     .use_cookies(true)
    ///     .build()?;
    ///
    /// let response = client.get("https://api.example.com/data")?.send().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Create a GET request to the specified URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to send the GET request to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .get("https://httpbin.org/get")?
    ///     .header("Accept", "application/json")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Response: {}", response.text().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&self, url: impl TryInto<Url>) -> crate::Result<crate::RequestBuilder> {
        let url = url.try_into().map_err(|_| crate::Error::InvalidUrl)?;
        Ok(crate::RequestBuilder::new(
            http::Method::GET,
            url,
            self.backend.clone(),
        ))
    }

    /// Create a POST request to the specified URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to send the POST request to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let json_data = r#"{"name": "John", "age": 30}"#;
    /// let response = client
    ///     .post("https://httpbin.org/post")?
    ///     .header("Content-Type", "application/json")?
    ///     .body(json_data)
    ///     .send()
    ///     .await?;
    ///
    /// println!("Status: {}", response.status());
    /// # Ok(())
    /// # }
    /// ```
    pub fn post(&self, url: impl TryInto<Url>) -> crate::Result<crate::RequestBuilder> {
        let url = url.try_into().map_err(|_| crate::Error::InvalidUrl)?;
        Ok(crate::RequestBuilder::new(
            http::Method::POST,
            url,
            self.backend.clone(),
        ))
    }

    /// Create a PUT request to the specified URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to send the PUT request to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let json_data = r#"{"name": "Jane", "age": 25}"#;
    /// let response = client
    ///     .put("https://httpbin.org/put")?
    ///     .header("Content-Type", "application/json")?
    ///     .body(json_data)
    ///     .send()
    ///     .await?;
    ///
    /// println!("Status: {}", response.status());
    /// # Ok(())
    /// # }
    /// ```
    pub fn put(&self, url: impl TryInto<Url>) -> crate::Result<crate::RequestBuilder> {
        let url = url.try_into().map_err(|_| crate::Error::InvalidUrl)?;
        Ok(crate::RequestBuilder::new(
            http::Method::PUT,
            url,
            self.backend.clone(),
        ))
    }

    /// Create a DELETE request to the specified URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to send the DELETE request to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .delete("https://httpbin.org/delete")?
    ///     .header("Authorization", "Bearer token123")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Deleted successfully: {}", response.status().is_success());
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete(&self, url: impl TryInto<Url>) -> crate::Result<crate::RequestBuilder> {
        let url = url.try_into().map_err(|_| crate::Error::InvalidUrl)?;
        Ok(crate::RequestBuilder::new(
            http::Method::DELETE,
            url,
            self.backend.clone(),
        ))
    }

    /// Create a HEAD request to the specified URL.
    ///
    /// HEAD requests are like GET requests but only return headers without the response body.
    /// They are useful for checking if a resource exists or getting metadata.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to send the HEAD request to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .head("https://httpbin.org/get")?
    ///     .send()
    ///     .await?;
    ///
    /// // Check if resource exists and get content length
    /// if response.status().is_success() {
    ///     if let Some(content_length) = response.headers().get("content-length") {
    ///         println!("Content length: {:?}", content_length);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn head(&self, url: impl TryInto<Url>) -> crate::Result<crate::RequestBuilder> {
        let url = url.try_into().map_err(|_| crate::Error::InvalidUrl)?;
        Ok(crate::RequestBuilder::new(
            http::Method::HEAD,
            url,
            self.backend.clone(),
        ))
    }

    /// Create a PATCH request to the specified URL.
    ///
    /// PATCH requests are used to make partial updates to a resource.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to send the PATCH request to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let patch_data = r#"{"age": 31}"#;
    /// let response = client
    ///     .patch("https://httpbin.org/patch")?
    ///     .header("Content-Type", "application/json")?
    ///     .body(patch_data)
    ///     .send()
    ///     .await?;
    ///
    /// println!("Patch successful: {}", response.status().is_success());
    /// # Ok(())
    /// # }
    /// ```
    pub fn patch(&self, url: impl TryInto<Url>) -> crate::Result<crate::RequestBuilder> {
        let url = url.try_into().map_err(|_| crate::Error::InvalidUrl)?;
        Ok(crate::RequestBuilder::new(
            http::Method::PATCH,
            url,
            self.backend.clone(),
        ))
    }

    /// Create a download builder for streaming downloads to disk.
    ///
    /// The download builder provides a fluent interface for configuring file downloads,
    /// including progress monitoring. Downloads are streamed directly to disk to handle
    /// large files efficiently.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to download from
    /// * `path` - The local file path where the downloaded content should be saved
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .download("https://httpbin.org/bytes/1048576", "large_file.bin")? // 1MB file
    ///     .progress(|downloaded, total| {
    ///         if let Some(total) = total {
    ///             println!("Progress: {}%", (downloaded * 100) / total);
    ///         }
    ///     })
    ///     .send()
    ///     .await?;
    ///
    /// println!("Downloaded to: {:?}", response.file_path);
    /// # Ok(())
    /// # }
    /// ```
    pub fn download<P: AsRef<std::path::Path>>(
        &self,
        url: impl TryInto<Url>,
        path: P,
    ) -> crate::Result<DownloadBuilder> {
        Ok(DownloadBuilder::new(
            self.backend.clone(),
            url.try_into().map_err(|_| crate::Error::InvalidUrl)?,
            path.as_ref().to_path_buf(),
        ))
    }

    /// Create a background download builder for downloads that survive app termination.
    ///
    /// How far a download survives depends on the backend:
    /// - **Apple platforms** (Foundation backend): Uses NSURLSession background downloads
    /// - **reqwest backend**: Uses a resumable download. The body is kept in a partial
    ///   file next to `path`, and the file at `path` is replaced in one step once the
    ///   download is complete; a later call with the same URL and path carries on where
    ///   an interrupted one stopped. The download runs in the task that awaits it,
    ///   unless the program called
    ///   [`enable_background_helper`](crate::enable_background_helper) at the top of
    ///   `main`: on Unix it then runs in a detached helper process that outlives the
    ///   app.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to download from
    /// * `path` - The local file path where the downloaded content should be saved
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidUrl`](crate::Error::InvalidUrl) if `url` is not a
    /// valid URL.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .download_background("https://httpbin.org/bytes/1073741824", "updates/app-v2.0.dmg")? // 1GB file
    ///     .session_identifier("app-update-v2.0")
    ///     .progress(|downloaded, total| {
    ///         if let Some(total) = total {
    ///             println!("Background download: {}%", (downloaded * 100) / total);
    ///         }
    ///     })
    ///     .send()
    ///     .await?;
    ///
    /// println!("Background download finished: {:?}", response.file_path);
    /// # Ok(())
    /// # }
    /// ```
    pub fn download_background<P: AsRef<std::path::Path>>(
        &self,
        url: impl TryInto<Url>,
        path: P,
    ) -> crate::Result<BackgroundDownloadBuilder> {
        Ok(BackgroundDownloadBuilder::new(
            self.backend.clone(),
            url.try_into().map_err(|_| crate::Error::InvalidUrl)?,
            path.as_ref().to_path_buf(),
        ))
    }

    /// Create an upload builder for uploading files or data.
    ///
    /// The upload builder provides a fluent interface for configuring uploads,
    /// including file uploads, raw data uploads, and progress monitoring.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to upload to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .upload("https://httpbin.org/post")?
    ///     .from_file("document.pdf")
    ///     .progress(|uploaded, total| {
    ///         if let Some(total) = total {
    ///             println!("Upload progress: {}%", (uploaded * 100) / total);
    ///         }
    ///     })
    ///     .send()
    ///     .await?;
    ///
    /// println!("Upload completed: {}", response.status());
    /// # Ok(())
    /// # }
    /// ```
    pub fn upload(&self, url: impl TryInto<Url>) -> crate::Result<UploadBuilder> {
        Ok(UploadBuilder::new(
            self.backend.clone(),
            url.try_into().map_err(|_| crate::Error::InvalidUrl)?,
        ))
    }

    /// Create a WebSocket connection builder.
    ///
    /// WebSockets provide full-duplex communication over a single TCP connection.
    /// The implementation varies by platform:
    /// - **Apple platforms**: Uses NSURLSession WebSocket support
    /// - **Other platforms**: Uses tokio-tungstenite for WebSocket connections
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::{Client, Message};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client
    ///     .websocket()
    ///     .connect("wss://echo.websocket.org")
    ///     .await?;
    ///
    /// // Send a message
    /// websocket.send(Message::text("Hello WebSocket!")).await?;
    ///
    /// // Receive a message
    /// let message = websocket.receive().await?;
    /// match message {
    ///     Message::Text(text) => println!("Received: {}", text),
    ///     Message::Binary(data) => println!("Received {} bytes", data.len()),
    /// }
    ///
    /// websocket.close(frakt::CloseCode::Normal, None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn websocket(&self) -> crate::websocket::WebSocketBuilder {
        match &self.backend {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            Backend::Foundation(foundation_backend) => {
                // Use Foundation backend for WebSocket
                crate::websocket::WebSocketBuilder::Foundation(
                    crate::backend::foundation::FoundationWebSocketBuilder::for_backend(
                        foundation_backend,
                    ),
                )
            }
            #[cfg(all(feature = "backend-winhttp", windows))]
            Backend::Windows(_) => crate::websocket::WebSocketBuilder::Windows(
                crate::backend::windows::WindowsWebSocketBuilder::new(),
            ),
            // Cronet has no WebSocket support; tokio-tungstenite is used when
            // the reqwest backend is compiled in.
            #[cfg(all(
                feature = "backend-android",
                target_os = "android",
                feature = "backend-reqwest"
            ))]
            Backend::Android(_) => crate::websocket::WebSocketBuilder::Reqwest(
                crate::backend::reqwest::ReqwestWebSocketBuilder::new(),
            ),
            #[cfg(all(
                feature = "backend-android",
                target_os = "android",
                not(feature = "backend-reqwest")
            ))]
            Backend::Android(_) => crate::websocket::WebSocketBuilder::Unsupported,
            #[cfg(feature = "backend-reqwest")]
            Backend::Reqwest(_) => {
                // Use Reqwest backend for WebSocket with tokio-tungstenite
                crate::websocket::WebSocketBuilder::Reqwest(
                    crate::backend::reqwest::ReqwestWebSocketBuilder::new(),
                )
            }
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Get the cookie jar this client sends cookies from and stores them
    /// into, on every backend.
    ///
    /// This is the jar given to [`ClientBuilder::cookie_jar`], or the
    /// client's own if cookies were turned on with
    /// [`ClientBuilder::use_cookies`] alone. Cookies added to it are sent
    /// with later requests, and cookies servers set show up in it. Returns
    /// `None` if the client does not keep cookies.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::{Client, Cookie};
    /// # #[tokio::main]
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::builder()
    ///     .use_cookies(true)
    ///     .build()?;
    ///
    /// if let Some(jar) = client.cookie_jar() {
    ///     // Add a custom cookie
    ///     let cookie = Cookie::new("session", "abc123")
    ///         .domain("example.com");
    ///     jar.add_cookie(cookie)?;
    ///
    ///     // Get all cookies
    ///     let cookies = jar.all_cookies();
    ///     println!("Found {} cookies", cookies.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        self.backend.cookie_jar()
    }
}

/// Builder for configuring HTTP clients
///
/// Provides a fluent interface for configuring client settings like timeouts,
/// headers, authentication, and backend selection.
pub struct ClientBuilder {
    config: crate::backend::BackendConfig,
    backend_type: Option<BackendType>,
}

/// Backend type
///
/// - `Foundation`: Use Foundation backend (Apple platforms only)
/// - `Reqwest`: Use Reqwest backend (all platforms)
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum BackendType {
    /// Foundation backend (Apple only)
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    Foundation,
    /// Reqwest backend (all platforms)
    #[cfg(feature = "backend-reqwest")]
    Reqwest,
    /// Windows backend (Windows only)
    #[cfg(all(feature = "backend-winhttp", windows))]
    Windows,
    /// Android backend (Android only)
    #[cfg(all(feature = "backend-android", target_os = "android"))]
    Android,
}

impl BackendType {
    /// The default backend type for the current target, or `None` if no
    /// backend usable on this target is enabled.
    ///
    /// The native stack is preferred when its feature is enabled; reqwest is
    /// the fallback.
    #[allow(unreachable_code)]
    pub fn fallback() -> Option<Self> {
        #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
        return Some(BackendType::Foundation);

        #[cfg(all(feature = "backend-winhttp", windows))]
        return Some(BackendType::Windows);

        #[cfg(all(feature = "backend-android", target_os = "android"))]
        return Some(BackendType::Android);

        #[cfg(feature = "backend-reqwest")]
        return Some(BackendType::Reqwest);

        None
    }
}

impl ClientBuilder {
    /// Create a new client builder
    pub fn new() -> Self {
        Self {
            config: crate::backend::BackendConfig::default(),
            backend_type: None,
        }
    }

    /// Set a total deadline for every request, from sending it to reading the
    /// last byte of the body. Off by default; it would cut off large
    /// downloads. See [`BackendConfig`](crate::backend::BackendConfig) for how
    /// the timeouts relate.
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config.timeout = Some(timeout);
        self
    }

    /// Set the connect timeout (default 30 seconds). See
    /// [`BackendConfig`](crate::backend::BackendConfig) for which backends can
    /// enforce it separately.
    pub fn connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config.connect_timeout = Some(timeout);
        self
    }

    /// Set the read (idle) timeout (default 60 seconds): how long a transfer
    /// may wait on the network for the response to start or for the next
    /// chunk of the body. Time the consumer spends not reading does not count.
    pub fn read_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config.read_timeout = Some(timeout);
        self
    }

    /// Set user agent string
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.config.user_agent = Some(user_agent.into());
        self
    }

    /// Set whether to ignore certificate errors (for testing only)
    pub fn ignore_certificate_errors(mut self, ignore: bool) -> Self {
        self.config.ignore_certificate_errors = Some(ignore);
        self
    }

    /// Add a default header to all requests
    pub fn header(
        mut self,
        name: impl TryInto<HeaderName>,
        value: impl Into<String>,
    ) -> crate::Result<Self> {
        let header_name = name.try_into().map_err(|_| crate::Error::InvalidHeader)?;
        let header_value =
            HeaderValue::from_str(&value.into()).map_err(|_| crate::Error::InvalidHeader)?;

        if self.config.default_headers.is_none() {
            self.config.default_headers = Some(HeaderMap::new());
        }
        self.config
            .default_headers
            .as_mut()
            .unwrap()
            .insert(header_name, header_value);
        Ok(self)
    }

    /// Enable or disable cookie handling. Cookies are off unless this is
    /// set or a jar is given with [`cookie_jar`](Self::cookie_jar);
    /// `use_cookies(false)` turns them off even when a jar was given.
    pub fn use_cookies(mut self, use_cookies: bool) -> Self {
        self.config.use_cookies = Some(use_cookies);
        self
    }

    /// Keep this client's cookies in `jar`, which turns cookies on.
    ///
    /// The client sends from and stores into this jar, and
    /// [`Client::cookie_jar`] returns it, so several clients can share one
    /// and the caller can preload, inspect and remove cookies. A jar is
    /// tied to a kind of storage (see [`CookieJar::new`](crate::CookieJar::new));
    /// [`build`](Self::build) fails if the client's backend cannot use the
    /// jar's kind.
    pub fn cookie_jar(mut self, jar: crate::CookieJar) -> Self {
        self.config.cookie_jar = Some(jar);
        self
    }

    /// Set HTTP proxy
    pub fn http_proxy(mut self, host: impl Into<String>, port: u16) -> Self {
        self.config.http_proxy = Some(ProxyConfig {
            host: host.into(),
            port,
            username: None,
            password: None,
        });
        self
    }

    /// Set proxy authentication for the most recently configured proxy
    pub fn proxy_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        let username = Some(username.into());
        let password = Some(password.into());

        // Apply to the most recently set proxy
        if let Some(ref mut proxy) = self.config.socks_proxy {
            proxy.username = username.clone();
            proxy.password = password.clone();
        } else if let Some(ref mut proxy) = self.config.https_proxy {
            proxy.username = username.clone();
            proxy.password = password.clone();
        } else if let Some(ref mut proxy) = self.config.http_proxy {
            proxy.username = username;
            proxy.password = password;
        }
        self
    }

    /// Set HTTPS proxy
    pub fn https_proxy(mut self, host: impl Into<String>, port: u16) -> Self {
        self.config.https_proxy = Some(ProxyConfig {
            host: host.into(),
            port,
            username: None,
            password: None,
        });
        self
    }

    /// Set SOCKS proxy
    pub fn socks_proxy(mut self, host: impl Into<String>, port: u16) -> Self {
        self.config.socks_proxy = Some(ProxyConfig {
            host: host.into(),
            port,
            username: None,
            password: None,
        });
        self
    }

    /// Force use of reqwest backend (available on all platforms)
    pub fn backend(mut self, backend_type: BackendType) -> Self {
        self.backend_type = Some(backend_type);
        self
    }

    /// Build the client with the configured settings
    pub fn build(self) -> crate::Result<Client> {
        let backend_type = match self.backend_type {
            Some(backend_type) => backend_type,
            None => BackendType::fallback().ok_or_else(|| {
                crate::Error::Internal("No HTTP backend is enabled for this target".to_string())
            })?,
        };
        Ok(Client {
            backend: Backend::with_config(backend_type, self.config)?,
        })
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
