//! Cross-platform HTTP client with native backend support
//!
//! This crate provides a unified async HTTP client that automatically selects
//! the best backend for your platform. On Apple platforms, it uses NSURLSession
//! for native performance and iOS background downloads. On other platforms,
//! it uses reqwest, with resumable background downloads that a helper process
//! can carry on with after the program exits on Unix systems.
//!
//! # Features
//!
//! - **Async-only design**: Built from the ground up for async/await with tokio
//! - **HTTP client**: Full-featured HTTP client with support for all standard methods
//! - **File downloads**: Efficient streaming downloads directly to disk with progress tracking
//! - **File uploads**: Support for uploading files or data with progress tracking
//! - **Background downloads**: Platform-specific background downloads (NSURLSession on Apple, resumable downloads with reqwest)
//! - **WebSocket support**: Native WebSocket connections (NSURLSessionWebSocketTask on Apple, tokio-tungstenite elsewhere)
//! - **Cookie management**: Automatic cookie handling with custom cookie jar support
//! - **Authentication**: Built-in support for Bearer, Basic, and custom authentication
//! - **Proxy support**: HTTP, HTTPS, and SOCKS proxy configuration
//! - **TLS configuration**: Certificate validation control and custom TLS settings
//! - **Request/Response body streaming**: Memory-efficient handling of large payloads
//! - **Multipart uploads**: Support for multipart/form-data uploads (with `multipart` feature)
//!
//! # Quick Start
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! frakt = "0.1"
//! tokio = { version = "1.0", features = ["full"] }
//! ```
//!
//! ## Basic HTTP Request
//!
//! ```rust,no_run
//! use frakt::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::new()?;
//!
//!     let response = client
//!         .get("https://httpbin.org/json")?
//!         .header(http::header::ACCEPT, "application/json")?
//!         .send()
//!         .await?;
//!
//!     println!("Status: {}", response.status());
//!     let body = response.text().await?;
//!     println!("Response: {}", body);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## File Download with Progress
//!
//! ```rust,no_run
//! use frakt::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::new()?;
//!
//!     let response = client
//!         .download("https://example.com/large-file.zip", "./downloads/file.zip")?
//!         .progress(|downloaded, total| {
//!             if let Some(total) = total {
//!                 let percent = (downloaded as f64 / total as f64) * 100.0;
//!                 println!("Downloaded: {:.1}%", percent);
//!             }
//!         })
//!         .send()
//!         .await?;
//!
//!     println!("Downloaded {} bytes to {}",
//!              response.bytes_downloaded,
//!              response.file_path.display());
//!
//!     Ok(())
//! }
//! ```
//!
//! ## File Upload
//!
//! ```rust,no_run
//! use frakt::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::new()?;
//!
//!     let response = client
//!         .upload("https://httpbin.org/post")?
//!         .from_file("./upload.txt")
//!         .header(http::header::CONTENT_TYPE, "text/plain")?
//!         .progress(|uploaded, total| {
//!             if let Some(total) = total {
//!                 let percent = (uploaded as f64 / total as f64) * 100.0;
//!                 println!("Uploaded: {:.1}%", percent);
//!             }
//!         })
//!         .send()
//!         .await?;
//!
//!     println!("Upload completed with status: {}", response.status());
//!
//!     Ok(())
//! }
//! ```
//!
//! ## WebSocket Connection
//!
//! ```rust,no_run
//! use frakt::{Client, Message, CloseCode};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::new()?;
//!
//!     let mut websocket = client
//!         .websocket()
//!         .connect("wss://echo.websocket.org")
//!         .await?;
//!
//!     // Send a message
//!     websocket.send(Message::text("Hello, WebSocket!")).await?;
//!
//!     // Receive a message
//!     let message = websocket.receive().await?;
//!     println!("Received: {:?}", message);
//!
//!     // Close the connection
//!     websocket.close(CloseCode::Normal, Some("Goodbye"));
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Background Downloads
//!
//! Background downloads are made to outlast the code that started them. How
//! far they do depends on the backend:
//! - **Apple platforms**: Uses NSURLSession background downloads, which carry
//!   on while the app is suspended or terminated
//! - **reqwest**: Uses resumable downloads. The body is written to a partial
//!   file next to the destination (`<name>.frakt-partial`, described by
//!   `<name>.frakt-partial.meta`), and the destination is replaced in one
//!   step once the download is complete. An interrupted download is carried
//!   on by the next one for the same URL and destination, if the server still
//!   has the same content.
//!
//! With reqwest the download runs in the task that awaits it. On Unix, a
//! program that calls [`enable_background_helper`] at the top of `main` gets
//! a detached helper process for each download instead, which keeps going
//! after the program exits.
//!
//! ```rust,no_run
//! use frakt::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     frakt::enable_background_helper();
//!
//!     let client = Client::new()?;
//!
//!     let response = client
//!         .download_background("https://example.com/large-video.mp4", "./downloads/video.mp4")?
//!         .session_identifier("com.myapp.downloads")
//!         .progress(|downloaded, total| {
//!             if let Some(total) = total {
//!                 let percent = (downloaded as f64 / total as f64) * 100.0;
//!                 println!("Background download: {:.1}%", percent);
//!             }
//!         })
//!         .send()
//!         .await?;
//!
//!     println!("Background download completed: {}", response.file_path.display());
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Client Configuration
//!
//! ```rust,no_run
//! use frakt::Client;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::builder()
//!         .user_agent("MyApp/1.0")
//!         .timeout(Duration::from_secs(30))
//!         .use_cookies(true)
//!         .header("X-API-Version", "v1")?
//!         .build()?;
//!
//!     let response = client
//!         .get("https://api.example.com/data")?
//!         .send()
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Platform Support
//!
//! This crate supports multiple platforms through a backend abstraction:
//!
//! - **Apple platforms** (macOS, iOS, tvOS, watchOS): Uses NSURLSession for native performance and iOS background downloads
//! - **Other platforms**: Uses reqwest with platform-specific enhancements:
//!   - **All platforms**: Resumable background downloads with retry logic
//!   - **Unix systems**: A detached helper process for background downloads,
//!     once [`enable_background_helper`] has been called
//!
//! # Performance
//!
//! **Apple platforms** benefit from NSURLSession's optimized networking stack:
//! - HTTP/2 and HTTP/3 support
//! - Connection pooling and reuse
//! - Network quality-of-service (QoS) handling
//! - Cellular and Wi-Fi network management
//! - True background transfer capabilities
//!
//! **Other platforms** use reqwest with additional features:
//! - HTTP/2 support via reqwest
//! - Connection pooling and keep-alive
//! - Resumable background downloads, in a helper process on Unix if enabled
//!
//! Every backend asks for compressed bodies and decodes them, except for
//! `Range` requests, which are sent with `Accept-Encoding: identity` unless
//! the caller chose an encoding. [`Response::is_decoded`] and
//! [`Response::body_stats`] say what happened to the body; see
//! [`BackendConfig`](crate::backend::BackendConfig) for this and for what
//! the timeouts mean.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

// Multi-platform support via backend abstraction

pub use auth::Auth;
pub use backend::types::BodyIntegrity;
pub use client::{
    BackendType, BackgroundDownloadBuilder, Client, ClientBuilder, DownloadBuilder,
    DownloadResponse, ProxyConfig, UploadBuilder,
};
pub use error::{Error, Result};
pub use request::{Request, RequestBuilder};
pub use response::{BodyStats, Response, ResponseStream};

// Re-export body types
pub use body::Body;
pub use body::FileBody;
pub use body::MultipartPart;
pub use cookies::{Cookie, CookieAcceptPolicy, CookieJar};
pub use websocket::{
    CloseCode, DEFAULT_MAXIMUM_MESSAGE_SIZE, Message, WebSocket, WebSocketBuilder,
};

// Re-export http types for convenience
pub use http;

mod auth;
pub mod backend;
#[cfg(any(
    all(feature = "backend-foundation", target_vendor = "apple"),
    all(feature = "backend-android", target_os = "android")
))]
pub mod background;
mod body;
mod client;
mod cookies;
mod error;
mod redact;
mod request;
mod response;
mod task;
mod websocket;

/// Let background downloads on the reqwest backend outlive this process.
///
/// Call it first thing in `main`:
///
/// ```no_run
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     frakt::enable_background_helper();
///
///     let client = frakt::Client::new()?;
///     // ... the rest of the program
///     Ok(())
/// }
/// ```
///
/// On Unix targets other than iOS, tvOS, watchOS, visionOS and Android,
/// [`Client::download_background`] on the reqwest backend then runs each
/// download in a helper process: this program's own executable, started
/// again with the same arguments, with `FRAKT_BACKGROUND_HELPER=1` in its
/// environment and the download described to it over a pipe. In a process
/// started that way this function never returns. It closes the descriptors
/// it inherited, runs the one download and exits, which is why the call
/// belongs at the top of `main`: whatever came before it would be done again
/// in every helper. The helper is in a session of its own, so it carries on
/// when this process exits or its terminal closes.
///
/// Without this call, or where no helper can be started, the download runs
/// in the task that awaits it and stops when that is dropped; the next
/// download to the same destination resumes it.
///
/// On every other target and backend this does nothing, so it can be called
/// unconditionally. It also does nothing in a set-uid or set-gid program,
/// which must not take instructions from its environment.
pub fn enable_background_helper() {
    #[cfg(feature = "backend-reqwest")]
    backend::reqwest::enable_background_helper();
}

#[cfg(all(target_os = "android", vampire))]
pub use vampire::JNI_OnLoad;

#[cfg(all(target_os = "android", vampire))]
#[path = "../tests"]
mod android_tests {
    #[path = "integration_tests.rs"]
    mod integration_tests;

    #[path = "transfers.rs"]
    mod transfers;
}
