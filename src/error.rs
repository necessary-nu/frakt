//! Error types and NSError mapping

use thiserror::Error;

/// Result type for this crate.
///
/// This is a convenience type alias for `Result<T, Error>` to reduce boilerplate.
/// All functions in this crate that can fail return this Result type.
///
/// # Examples
///
/// ```rust
/// use frakt::{Client, Result};
///
/// async fn make_request() -> Result<String> {
///     let client = Client::new()?;
///     let response = client.get("https://httpbin.org/get")?.send().await?;
///     response.text().await
/// }
/// ```
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for NSURLSession operations.
///
/// This enum represents all possible errors that can occur when using the frakt library.
/// Errors are mapped from NSURLSession's NSError types to provide idiomatic Rust error handling.
///
/// # Examples
///
/// ```rust
/// use frakt::{Client, Error};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new().unwrap();
///
/// match client.get("https://invalid-url")?.send().await {
///     Ok(response) => println!("Success: {}", response.status()),
///     Err(Error::InvalidUrl) => println!("Invalid URL provided"),
///     Err(Error::Network { code, message }) => {
///         println!("Network error {}: {}", code, message)
///     }
///     Err(Error::Timeout) => println!("Request timed out"),
///     Err(e) => println!("Other error: {}", e),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid URL was provided.
    ///
    /// This error occurs when a URL string cannot be parsed into a valid URL
    /// or when the URL format is not supported by NSURLSession.
    #[error("Invalid URL")]
    InvalidUrl,

    /// Invalid HTTP header name or value.
    ///
    /// This error occurs when a header name or value contains invalid characters
    /// or when the header format is not valid according to HTTP specifications.
    #[error("Invalid header")]
    InvalidHeader,

    /// Network error from NSURLSession.
    ///
    /// This represents various network-level errors including DNS resolution failures,
    /// connection refused, host unreachable, and other networking issues. The error
    /// includes the original NSURLError code and a descriptive message.
    #[error("Network error: {message} (code: {code})")]
    Network {
        /// Error code from NSURLError
        code: i64,
        /// Error message
        message: String,
    },

    /// TLS/Certificate error.
    ///
    /// This error occurs when there are issues with SSL/TLS connections,
    /// including certificate validation failures, protocol errors, or
    /// other security-related issues.
    #[error("TLS error: {message}")]
    Tls {
        /// Error message
        message: String,
    },

    /// Request timed out.
    ///
    /// This error occurs when a request takes longer than the configured timeout
    /// duration to complete. The timeout can be set using `Client::builder().timeout()`.
    #[error("Request timed out")]
    Timeout,

    /// Request was cancelled.
    ///
    /// This error occurs when a request is cancelled before completion,
    /// either explicitly by the user or due to the task being dropped.
    #[error("Request was cancelled")]
    Cancelled,

    /// HTTP error response (4xx or 5xx status code).
    ///
    /// This error occurs when the server returns an error status code (>= 400)
    /// and error checking is enabled. The full Response is included, allowing
    /// access to the status code, headers, and error response body.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use frakt::{Client, Error};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// match client.get("https://httpbin.org/status/404")?.send().await {
    ///     Err(Error::HttpError(response)) => {
    ///         println!("HTTP error: {}", response.status());
    ///         let body = response.text().await?;
    ///         println!("Error body: {}", body);
    ///     }
    ///     Ok(response) => println!("Success: {}", response.status()),
    ///     Err(e) => return Err(e.into()),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[error("HTTP error: {}", .0.status())]
    HttpError(Box<crate::Response>),

    /// WebSocket connection was closed.
    ///
    /// This error occurs when attempting to use a WebSocket connection
    /// that has been closed, either normally or due to an error.
    #[error("WebSocket connection was closed")]
    WebSocketClosed,

    /// UTF-8 conversion error.
    ///
    /// This error occurs when trying to convert response bytes to a UTF-8 string
    /// using methods like `Response::text()`, but the response body contains
    /// invalid UTF-8 sequences.
    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    /// JSON serialization/deserialization error.
    ///
    /// This error occurs when JSON operations fail, such as when using
    /// `RequestBuilder::json()` or `Response::json()`. This variant is only
    /// available when the "json" feature is enabled.
    #[error("JSON error: {0}")]
    Json(String),

    /// I/O error.
    ///
    /// This error occurs for file system operations, such as when reading
    /// files for request bodies or writing downloads to disk.
    #[error("I/O error: {0}")]
    Io(String),

    /// Response body exceeds maximum size.
    ///
    /// This error occurs when a response body is larger than the configured
    /// maximum size limit, helping to prevent memory exhaustion attacks.
    #[error("Response body exceeds maximum size")]
    ResponseTooLarge,

    /// Internal library error.
    ///
    /// This error represents internal inconsistencies or unexpected conditions
    /// within the library. If you encounter this error, it may indicate a bug
    /// in the library.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err.to_string())
    }
}
