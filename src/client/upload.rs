//! Upload builder for uploading files or data

use crate::backend::Backend;
use http::{HeaderMap, HeaderValue};
use url::Url;

/// Builder for uploading files or data to a server.
///
/// The `UploadBuilder` provides a fluent interface for configuring uploads,
/// including the data source (file or bytes), headers, authentication, and
/// progress monitoring. Uploads are performed asynchronously and can handle
/// both small data and large files efficiently.
///
/// # Examples
///
/// ## Upload a file
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
///             println!("Progress: {}%", (uploaded * 100) / total);
///         }
///     })
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Upload data with authentication
///
/// ```no_run
/// # use frakt::{Client, Auth};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let data = serde_json::to_vec(&serde_json::json!({
///     "message": "Hello, API!"
/// }))?;
///
/// let response = client
///     .upload("https://api.example.com/messages")?
///     .from_data(data)
///     .header("content-type", "application/json")?
///     .auth(Auth::bearer("your_token_here"))?
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct UploadBuilder {
    backend: Backend,
    url: Url,
    file_path: Option<(std::path::PathBuf, String)>, // (path, content_type)
    data: Option<Vec<u8>>,
    headers: HeaderMap,
    progress_callback: Option<crate::backend::types::BoxedProgressCallback>,
    error_for_status: bool,
}

impl UploadBuilder {
    /// Create a new upload builder (internal use)
    pub(crate) fn new(backend: Backend, url: Url) -> Self {
        Self {
            backend,
            url,
            file_path: None,
            data: None,
            headers: HeaderMap::new(),
            progress_callback: None,
            error_for_status: true,
        }
    }

    /// Upload a file from the local filesystem.
    ///
    /// The file is opened when the upload is sent and read from disk as it
    /// goes out, so memory use does not grow with its size; see
    /// [`FileBody`](crate::FileBody) for what happens if it changes
    /// meanwhile. The content type is guessed from the file extension, or
    /// defaults to `application/octet-stream` if the extension is not
    /// recognized.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to upload
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
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_file<P: AsRef<std::path::Path>>(mut self, path: P) -> Self {
        let path = path.as_ref().to_path_buf();

        // Guess content type from file extension
        let content_type = match path.extension().and_then(|ext| ext.to_str()) {
            Some("txt") => "text/plain",
            Some("html") => "text/html",
            Some("css") => "text/css",
            Some("js") => "application/javascript",
            Some("pdf") => "application/pdf",
            Some("png") => "image/png",
            Some("jpg") | Some("jpeg") => "image/jpeg",
            Some("gif") => "image/gif",
            Some("zip") => "application/zip",
            _ => "application/octet-stream",
        }
        .to_string();

        // Store the file path for later async reading in send()
        self.file_path = Some((path, content_type));
        self
    }

    /// Upload data from a byte vector.
    ///
    /// The data will be uploaded with the content type `application/octet-stream`
    /// unless a different content type is explicitly set using the `header` method.
    ///
    /// # Arguments
    ///
    /// * `data` - The bytes to upload
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let data = b"Hello, world!".to_vec();
    /// let response = client
    ///     .upload("https://httpbin.org/post")?
    ///     .from_data(data)
    ///     .header("content-type", "text/plain")?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_data(mut self, data: Vec<u8>) -> Self {
        self.data = Some(data);
        self
    }

    /// Add a header to the upload request.
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
    ///     .upload("https://httpbin.org/post")?
    ///     .from_data(b"data".to_vec())
    ///     .header("content-type", "application/json")?
    ///     .header("x-api-key", "secret")?
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

    /// Set a progress callback to monitor upload progress.
    ///
    /// The callback will be called periodically during the upload with the number
    /// of bytes uploaded so far and the total number of bytes to upload (if known).
    ///
    /// # Arguments
    ///
    /// * `callback` - A function that receives `(bytes_uploaded, total_bytes)`
    ///   where `total_bytes` may be `None` if the total size is unknown
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .upload("https://httpbin.org/post")?
    ///     .from_file("large_file.zip")
    ///     .progress(|bytes_uploaded, total_bytes| {
    ///         if let Some(total) = total_bytes {
    ///             let percent = (bytes_uploaded as f64 / total as f64) * 100.0;
    ///             println!("Upload progress: {:.1}%", percent);
    ///         } else {
    ///             println!("Uploaded: {} bytes", bytes_uploaded);
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

    /// Set authentication for the upload request.
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
    /// // Basic authentication
    /// let response = client
    ///     .upload("https://httpbin.org/post")?
    ///     .from_data(b"data".to_vec())
    ///     .auth(Auth::basic("username", "password"))?
    ///     .send()
    ///     .await?;
    ///
    /// // Bearer token
    /// let response = client
    ///     .upload("https://api.example.com/upload")?
    ///     .from_file("document.pdf")
    ///     .auth(Auth::bearer("jwt_token_here"))?
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

    /// Configure whether to return an error for HTTP error status codes (>= 400).
    ///
    /// When enabled (the default), responses with status codes >= 400 will return
    /// an `HttpError` containing the full response. When disabled, all status codes
    /// are treated as success and you must check the status manually.
    ///
    /// # Arguments
    ///
    /// * `enabled` - If `true`, error on status >= 400; if `false`, accept all status codes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    ///
    /// // Upload to an endpoint that might return 404, don't error
    /// let response = client
    ///     .upload("https://httpbin.org/status/404")?
    ///     .from_data(b"test".to_vec())
    ///     .error_for_status(false)
    ///     .send()
    ///     .await?;
    ///
    /// assert_eq!(response.status(), 404);
    /// # Ok(())
    /// # }
    /// ```
    pub fn error_for_status(mut self, enabled: bool) -> Self {
        self.error_for_status = enabled;
        self
    }

    /// Convenience method to allow error status codes (>= 400) to be treated as success.
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
    /// // Don't error on 404 response
    /// let response = client
    ///     .upload("https://httpbin.org/status/404")?
    ///     .from_data(b"test".to_vec())
    ///     .allow_error_status()
    ///     .send()
    ///     .await?;
    ///
    /// assert_eq!(response.status(), 404);
    /// # Ok(())
    /// # }
    /// ```
    pub fn allow_error_status(self) -> Self {
        self.error_for_status(false)
    }

    /// Execute the upload and return the response.
    ///
    /// This method performs the actual upload using the configured data or file.
    /// A file is streamed from disk; data given with `from_data` is sent
    /// from memory.
    ///
    /// # Returns
    ///
    /// Returns a `Response` containing the server's response to the upload.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No data or file was specified for upload
    /// - The file cannot be read (for file uploads)
    /// - The network request fails
    /// - The server returns an error response
    /// - Header values are invalid
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let response = client
    ///     .upload("https://httpbin.org/post")?
    ///     .from_data(b"Hello, server!".to_vec())
    ///     .header("content-type", "text/plain")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Response status: {}", response.status());
    /// let body = response.text().await?;
    /// println!("Response body: {}", body);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(mut self) -> crate::Result<crate::Response> {
        // Determine the body
        let body = if let Some(data) = self.data {
            crate::Body::bytes(data, "application/octet-stream")
        } else if let Some((path, content_type)) = self.file_path {
            // Set content type header if not already set
            if !self.headers.contains_key("content-type") {
                self.headers.insert(
                    "content-type",
                    HeaderValue::from_str(&content_type)
                        .map_err(|_| crate::Error::InvalidHeader)?,
                );
            }
            crate::Body::from_file(path, Some(content_type)).await?
        } else {
            return Err(crate::Error::Internal(
                "No file or data specified for upload".to_string(),
            ));
        };

        let mut request_builder =
            crate::RequestBuilder::new(http::Method::POST, self.url, self.backend)
                .error_for_status(self.error_for_status)
                .headers(self.headers);

        // Add progress callback if set
        if let Some(callback) = self.progress_callback {
            request_builder = request_builder.progress(callback);
        }

        // Set body and send
        request_builder.body(body).send().await
    }
}
