//! Download builder for downloading files to disk

use http::{HeaderMap, StatusCode};
use url::Url;

use crate::backend::Backend;

/// Response from a completed download operation.
///
/// Contains information about the completed download, including where the file
/// was saved, how many bytes were downloaded, the HTTP status code, and headers.
///
/// # Examples
///
/// ```no_run
/// # use frakt::Client;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let response = client
///     .download("https://httpbin.org/base64/SGVsbG8gV29ybGQ=", "hello.txt")?
///     .send()
///     .await?;
///
/// println!("Downloaded to: {:?}", response.file_path);
/// println!("Downloaded {} bytes", response.bytes_downloaded);
/// println!("Status: {}", response.status);
/// if let Some(content_type) = response.headers.get("content-type") {
///     println!("Content-Type: {:?}", content_type);
/// }
/// # Ok(())
/// # }
/// ```
pub struct DownloadResponse {
    /// Path where the file was saved
    pub file_path: std::path::PathBuf,
    /// Total bytes downloaded
    pub bytes_downloaded: u64,
    /// HTTP status code from the response
    pub status: StatusCode,
    /// HTTP headers from the response
    pub headers: HeaderMap,
}

/// `Set-Cookie` and other credential-bearing header values are not shown.
impl std::fmt::Debug for DownloadResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadResponse")
            .field("file_path", &self.file_path)
            .field("bytes_downloaded", &self.bytes_downloaded)
            .field("status", &self.status)
            .field("headers", &crate::redact::RedactedHeaders(&self.headers))
            .finish()
    }
}

/// A download in progress: a uniquely named file beside the destination,
/// renamed over it only once the whole body has been written. If it is
/// dropped before that, because the transfer failed or the download's future
/// was cancelled, the file is removed and the destination is as it was.
struct PendingFile {
    file: tokio::fs::File,
    path: tempfile::TempPath,
}

impl PendingFile {
    fn create(destination: &std::path::Path) -> std::io::Result<Self> {
        let directory = match destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => std::path::Path::new("."),
        };
        let name = destination.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{} does not name a file", destination.display()),
            )
        })?;
        let mut prefix = std::ffi::OsString::from(".");
        prefix.push(name);
        prefix.push(".");

        let mut builder = tempfile::Builder::new();
        builder.prefix(&prefix).suffix(".frakt-download");
        // What `File::create` would give the destination: the umask applies.
        #[cfg(unix)]
        builder.permissions(std::os::unix::fs::PermissionsExt::from_mode(0o666));
        let pending = builder.tempfile_in(directory)?;

        // Replacing a file keeps its permissions, as truncating it did.
        if let Ok(existing) = std::fs::metadata(destination)
            && existing.is_file()
        {
            pending.as_file().set_permissions(existing.permissions())?;
        }

        let (file, path) = pending.into_parts();
        Ok(Self {
            file: tokio::fs::File::from_std(file),
            path,
        })
    }

    async fn write(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(&mut self.file, chunk).await
    }

    /// Put the finished file at `destination`, replacing what is there. A
    /// symlink at the destination is replaced, not followed.
    async fn publish(mut self, destination: &std::path::Path) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::flush(&mut self.file).await?;
        // Closed before the rename: Windows does not rename an open file.
        drop(self.file.into_std().await);

        let path = self.path;
        let destination = destination.to_path_buf();
        tokio::task::spawn_blocking(move || path.persist(destination))
            .await
            .map_err(std::io::Error::other)?
            .map_err(|error| error.error)
    }
}

/// Builder for downloading files from URLs to local disk.
///
/// The `DownloadBuilder` provides a fluent interface for configuring downloads,
/// including the destination path, headers, authentication, progress monitoring,
/// and other options. Downloads are streamed directly to disk to handle large
/// files efficiently without loading them entirely into memory.
///
/// # Examples
///
/// ## Basic download
///
/// ```no_run
/// # use frakt::Client;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let response = client
///     .download("https://httpbin.org/base64/SGVsbG8gV29ybGQ=", "downloaded_file.txt")?
///     .send()
///     .await?;
///
/// println!("Downloaded {} bytes to {:?}",
///     response.bytes_downloaded,
///     response.file_path);
/// # Ok(())
/// # }
/// ```
///
/// ## Download with progress monitoring
///
/// ```no_run
/// # use frakt::Client;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let response = client
///     .download("https://httpbin.org/bytes/1048576", "large_file.bin")? // 1MB
///     .progress(|downloaded, total| {
///         if let Some(total) = total {
///             let percent = (downloaded as f64 / total as f64) * 100.0;
///             println!("Download progress: {:.1}%", percent);
///         } else {
///             println!("Downloaded: {} bytes", downloaded);
///         }
///     })
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Authenticated download
///
/// ```no_run
/// # use frakt::{Client, Auth};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let response = client
///     .download("https://api.example.com/protected/document.pdf", "protected_document.pdf")?
///     .auth(Auth::bearer("your-api-token"))?
///     .header("User-Agent", "MyApp/1.0")?
///     .progress(|downloaded, total| {
///         if let Some(total) = total {
///             let percent = (downloaded as f64 / total as f64) * 100.0;
///             println!("Download progress: {:.1}%", percent);
///         }
///     })
///     .send()
///     .await?;
///
/// println!("Downloaded protected file: {} bytes", response.bytes_downloaded);
/// # Ok(())
/// # }
/// ```
pub struct DownloadBuilder {
    backend: Backend,
    url: Url,
    file_path: Option<std::path::PathBuf>,
    headers: HeaderMap,
    progress_callback: Option<crate::backend::types::BoxedProgressCallback>,
    error_for_status: bool,
    require_checked_body: bool,
}

impl DownloadBuilder {
    /// Create a new download builder (internal use)
    pub(crate) fn new(backend: Backend, url: Url, file_path: std::path::PathBuf) -> Self {
        Self {
            backend,
            url,
            file_path: Some(file_path),
            headers: HeaderMap::new(),
            progress_callback: None,
            error_for_status: true,
            require_checked_body: false,
        }
    }

    /// Replace the request's headers.
    #[cfg_attr(not(all(feature = "backend-winhttp", windows)), allow(dead_code))]
    pub(crate) fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// [`progress`](Self::progress) for a callback that is already boxed.
    #[cfg_attr(not(all(feature = "backend-winhttp", windows)), allow(dead_code))]
    pub(crate) fn progress_callback(
        mut self,
        callback: Option<crate::backend::types::BoxedProgressCallback>,
    ) -> Self {
        self.progress_callback = callback;
        self
    }

    /// Refuse a response whose body could be cut short without an error;
    /// see [`RequestBuilder::require_checked_body`](crate::RequestBuilder::require_checked_body).
    pub fn require_checked_body(mut self) -> Self {
        self.require_checked_body = true;
        self
    }

    /// Set a progress callback to monitor download progress.
    ///
    /// The callback will be called periodically during the download with the number
    /// of bytes downloaded so far and the total number of bytes to download (if known).
    /// The total may be `None` if the server doesn't provide a `Content-Length` header.
    ///
    /// # Arguments
    ///
    /// * `callback` - A function that receives `(bytes_downloaded, total_bytes)`
    ///   where `total_bytes` may be `None` if the total size is unknown
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .download("https://httpbin.org/bytes/1048576", "large_download.bin")? // 1MB file
    ///     .progress(|bytes_downloaded, total_bytes| {
    ///         match total_bytes {
    ///             Some(total) => {
    ///                 let percent = (bytes_downloaded as f64 / total as f64) * 100.0;
    ///                 println!("Download: {:.1}% ({}/{})", percent, bytes_downloaded, total);
    ///             }
    ///             None => {
    ///                 println!("Downloaded: {} bytes (total unknown)", bytes_downloaded);
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
    /// // Download a 404 error page
    /// let response = client
    ///     .download("https://httpbin.org/status/404", "error.html")?
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
    /// // Download a 404 error page
    /// let response = client
    ///     .download("https://httpbin.org/status/404", "error.html")?
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

    /// Add a header to the download request.
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
    ///     .download("https://api.example.com/protected-file.pdf", "downloaded.pdf")?
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

    /// Set authentication for the download request.
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
    /// // Bearer token
    /// let response = client
    ///     .download("https://api.example.com/protected-file.zip", "protected.zip")?
    ///     .auth(Auth::bearer("your-api-token"))?
    ///     .send()
    ///     .await?;
    ///
    /// // Basic authentication
    /// let response = client
    ///     .download("https://secure.example.com/file.pdf", "secure.pdf")?
    ///     .auth(Auth::basic("username", "password"))?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auth(mut self, auth: crate::Auth) -> crate::Result<Self> {
        self.headers
            .insert(http::header::AUTHORIZATION, auth.header_value()?);
        Ok(self)
    }

    /// Execute the download and save the file to disk.
    ///
    /// This method performs the actual download, streaming the content to
    /// disk rather than loading it into memory. The body is written to a
    /// temporary file in the destination's directory, which replaces the
    /// destination in one rename once the whole body has arrived. Until
    /// then, and if the download fails, times out or its future is dropped,
    /// a file already at the destination is left exactly as it was and no
    /// partial file appears under the destination's name. A symlink at the
    /// destination is replaced rather than followed. The rename is atomic;
    /// the data is not forced to stable storage first, so call
    /// `File::sync_all` on the result if it must survive a power failure.
    ///
    /// # Returns
    ///
    /// Returns a `DownloadResponse` containing information about the completed download,
    /// including the file path and total bytes downloaded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No destination file path was specified using `to_file()`
    /// - The parent directory cannot be created
    /// - The file cannot be created or written to
    /// - The network request fails
    /// - The server returns an error response
    /// - File I/O operations fail during the download
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .download("https://httpbin.org/json", "response.json")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Download completed!");
    /// println!("File saved to: {:?}", response.file_path);
    /// println!("Downloaded {} bytes", response.bytes_downloaded);
    ///
    /// // File is now available on disk
    /// let content = std::fs::read_to_string(&response.file_path)?;
    /// println!("File content: {}", content);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(self) -> crate::Result<DownloadResponse> {
        let file_path = self.file_path.ok_or_else(|| {
            crate::Error::Internal("Download file path not specified".to_string())
        })?;

        let error_for_status = self.error_for_status;

        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                crate::Error::Internal(format!("Failed to create parent directory: {}", e))
            })?;
        }

        // Status errors are checked here, where the builder's setting is.
        let mut request_builder =
            crate::RequestBuilder::new(http::Method::GET, self.url, self.backend)
                .allow_error_status()
                .headers(self.headers);
        if self.require_checked_body {
            request_builder = request_builder.require_checked_body();
        }

        let response = request_builder.send().await?;

        let status = response.status();
        let headers = response.headers().clone();

        if error_for_status && status.as_u16() >= 400 {
            return Err(crate::Error::HttpError(Box::new(response)));
        }

        // The announced length counts encoded bytes, so it is only the
        // total when the body is delivered as it was sent.
        let total_bytes = response
            .expected_wire_length()
            .filter(|_| !response.is_decoded());

        let mut pending = PendingFile::create(&file_path)?;
        let mut stream = response.stream();
        let mut bytes_downloaded = 0u64;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            bytes_downloaded += chunk.len() as u64;
            pending.write(&chunk).await?;

            if let Some(ref callback) = self.progress_callback {
                callback(bytes_downloaded, total_bytes);
            }
        }

        pending.publish(&file_path).await?;

        Ok(DownloadResponse {
            file_path,
            bytes_downloaded,
            status,
            headers,
        })
    }
}
