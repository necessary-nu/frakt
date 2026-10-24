//! Background download builder for downloads that survive app termination

use http::HeaderMap;
use url::Url;

use crate::backend::Backend;
use crate::client::download::DownloadResponse;

/// Background download builder for downloads that survive app termination
///
/// Platform-specific behavior:
/// - **Apple platforms**: Uses NSURLSession background downloads
/// - **Unix platforms**: Uses double-fork daemon process with reqwest
/// - **Other platforms**: Uses resumable downloads with retry logic
///
/// All platforms support:
/// - Progress callbacks
/// - Automatic resume on failure
/// - Session identifiers for tracking
pub struct BackgroundDownloadBuilder {
    backend: Backend,
    url: Url,
    session_identifier: Option<String>,
    file_path: Option<std::path::PathBuf>,
    headers: HeaderMap,
    progress_callback: Option<Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
    error_for_status: bool,
}

impl BackgroundDownloadBuilder {
    /// Create a new background download builder (internal use)
    pub(crate) fn new(backend: Backend, url: Url, file_path: std::path::PathBuf) -> Self {
        Self {
            backend,
            url,
            session_identifier: None,
            file_path: Some(file_path),
            headers: HeaderMap::new(),
            progress_callback: None,
            error_for_status: true,
        }
    }

    /// Set session identifier for background downloads.
    ///
    /// The session identifier is used to track and manage background downloads
    /// across app restarts. On Apple platforms it names the background
    /// NSURLSession, which is shared by every download with the same
    /// identifier; without one, `frakt.background` is used. For results that
    /// must survive the app being terminated, use
    /// `frakt::background::BackgroundDownloads`, which reports them to a
    /// handler in a later launch; this builder's future only completes in
    /// the process that started the download.
    ///
    /// # Arguments
    ///
    /// * `identifier` - A unique string to identify this download session
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .download_background("https://httpbin.org/bytes/10485760", "updates/app-v1.2.3.zip") // 10MB
    ///     .session_identifier("my-app-update-v1.2.3")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn session_identifier(mut self, identifier: impl Into<String>) -> Self {
        self.session_identifier = Some(identifier.into());
        self
    }

    /// Set a progress callback to monitor background download progress.
    ///
    /// The callback will be called periodically during the download with the number
    /// of bytes downloaded so far and the total number of bytes to download (if known).
    /// The progress callback works across app restarts on supported platforms.
    ///
    /// # Arguments
    ///
    /// * `callback` - A function that receives `(bytes_downloaded, total_bytes)`
    ///   where `total_bytes` may be `None` if the server doesn't provide Content-Length
    ///
    /// # Platform Behavior
    ///
    /// - **Apple platforms**: Progress is maintained by NSURLSession across app restarts
    /// - **Unix platforms**: Progress is tracked by the daemon process
    /// - **Other platforms**: Progress is only available while the app is running
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .download_background("https://httpbin.org/bytes/1073741824", "huge_download.bin") // 1GB
    ///     .progress(|bytes_downloaded, total_bytes| {
    ///         match total_bytes {
    ///             Some(total) => {
    ///                 let percent = (bytes_downloaded as f64 / total as f64) * 100.0;
    ///                 println!("Background download: {:.1}% ({} MB / {} MB)",
    ///                     percent,
    ///                     bytes_downloaded / 1_048_576,
    ///                     total / 1_048_576);
    ///             }
    ///             None => {
    ///                 println!("Background downloaded: {} MB", bytes_downloaded / 1_048_576);
    ///             }
    ///         }
    ///     })
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn progress<F>(mut self, callback: F) -> Self
    where
        F: Fn(u64, Option<u64>) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Box::new(callback));
        self
    }

    /// Add a header to the background download request.
    ///
    /// Headers are added to the HTTP request that will be sent to the server.
    /// Multiple headers with the same name will overwrite previous values.
    ///
    /// # Arguments
    ///
    /// * `name` - The header name (can be a string or `HeaderName`)
    /// * `value` - The header value
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` on success, allowing method chaining.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The header name is invalid (contains invalid characters)
    /// - The header value is invalid (contains newlines or other invalid characters)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .download_background("https://api.example.com/protected-large-file.zip", "protected_large_file.zip")
    ///     .session_identifier("protected-download")
    ///     .header("Authorization", "Bearer token123")?
    ///     .header("User-Agent", "MyApp/1.0")?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn header(
        mut self,
        name: impl TryInto<http::HeaderName>,
        value: impl Into<String>,
    ) -> crate::Result<Self> {
        let header_name = name.try_into().map_err(|_| crate::Error::InvalidHeader)?;
        let header_value =
            http::HeaderValue::from_str(&value.into()).map_err(|_| crate::Error::InvalidHeader)?;
        self.headers.insert(header_name, header_value);
        Ok(self)
    }

    /// Set authentication for the background download request.
    ///
    /// Adds an `Authorization` header to the request using the provided authentication
    /// method. Supports Basic, Bearer, and custom authentication schemes.
    ///
    /// # Arguments
    ///
    /// * `auth` - The authentication method to use
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` on success, allowing method chaining.
    ///
    /// # Errors
    ///
    /// Returns an error if the authentication header value is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::{Client, Auth};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    ///
    /// // Bearer token for authenticated background download
    /// let response = client
    ///     .download_background("https://api.example.com/premium-content.zip", "premium_content.zip")
    ///     .session_identifier("premium-content-download")
    ///     .auth(Auth::bearer("your-api-token"))?
    ///     .send()
    ///     .await?;
    ///
    /// // Basic authentication
    /// let response = client
    ///     .download_background("https://secure.example.com/data.zip", "secure_data.zip")
    ///     .session_identifier("secure-data-download")
    ///     .auth(Auth::basic("username", "password"))?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auth(mut self, auth: crate::Auth) -> crate::Result<Self> {
        let header_value = http::HeaderValue::from_str(&auth.to_header_value())
            .map_err(|_| crate::Error::InvalidHeader)?;
        self.headers
            .insert(http::header::AUTHORIZATION, header_value);
        Ok(self)
    }

    /// Configure whether to return an error for HTTP error status codes (>= 400).
    ///
    /// When enabled (the default), responses with status codes >= 400 will return
    /// an `HttpError` containing the full response without downloading the file.
    /// When disabled, all status codes are treated as success and the file will be
    /// downloaded regardless of the status code.
    ///
    /// # Arguments
    ///
    /// * `enabled` - If `true`, error on status >= 400; if `false`, download regardless
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    ///
    /// // Download a 404 error page in background
    /// let response = client
    ///     .download_background("https://httpbin.org/status/404", "error.html")
    ///     .session_identifier("test-404")
    ///     .error_for_status(false)
    ///     .send()
    ///     .await?;
    ///
    /// assert_eq!(response.status, 404);
    /// # Ok(())
    /// # }
    /// ```
    pub fn error_for_status(mut self, enabled: bool) -> Self {
        self.error_for_status = enabled;
        self
    }

    /// Convenience method to allow error status codes (>= 400) to be downloaded.
    ///
    /// This is equivalent to calling `.error_for_status(false)`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    ///
    /// // Download a 404 error page in background
    /// let response = client
    ///     .download_background("https://httpbin.org/status/404", "error.html")
    ///     .session_identifier("test-404")
    ///     .allow_error_status()
    ///     .send()
    ///     .await?;
    ///
    /// assert_eq!(response.status, 404);
    /// # Ok(())
    /// # }
    /// ```
    pub fn allow_error_status(self) -> Self {
        self.error_for_status(false)
    }

    /// Start the background download and return immediately.
    ///
    /// This method initiates a background download that will continue even if the
    /// application is terminated. The download behavior varies by platform:
    ///
    /// - **Apple platforms**: Uses NSURLSession background downloads
    /// - **Unix platforms**: Spawns a daemon process using double-fork
    /// - **Other platforms**: Uses resumable downloads with retry logic
    ///
    /// # Returns
    ///
    /// Returns a `DownloadResponse` immediately with initial download information.
    /// The actual download continues in the background.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No destination file path was specified using `to_file()`
    /// - The parent directory cannot be created
    /// - The background download session cannot be started
    /// - Platform-specific background services are unavailable
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    ///
    /// // Start a background download that survives app termination
    /// let response = client
    ///     .download_background("https://httpbin.org/bytes/1073741824", "updates/app-v2.0.dmg") // 1GB
    ///     .session_identifier("app-update-v2.0")
    ///     .progress(|downloaded, total| {
    ///         // This will be called even after app restart on supported platforms
    ///         if let Some(total) = total {
    ///             println!("Download progress: {}%", (downloaded * 100) / total);
    ///         }
    ///     })
    ///     .send()
    ///     .await?;
    ///
    /// println!("Background download started for: {:?}", response.file_path);
    /// // App can now terminate - download continues in background
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(self) -> crate::Result<DownloadResponse> {
        let file_path = self.file_path.clone().ok_or_else(|| {
            crate::Error::Internal("Background download file path not specified".to_string())
        })?;

        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                crate::Error::Internal(format!("Failed to create parent directory: {}", e))
            })?;
        }

        // Delegate to backend
        self.backend
            .execute_background_download(
                self.url,
                file_path,
                self.session_identifier,
                self.headers,
                self.progress_callback,
                self.error_for_status,
            )
            .await
    }
}
