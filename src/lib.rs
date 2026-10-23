//! Cross-platform HTTP client with native backend support
//!
//! This crate provides a unified async HTTP client that automatically selects
//! the best backend for your platform. On Apple platforms, it uses NSURLSession
//! for native performance and iOS background downloads. On other platforms,
//! it uses reqwest with additional features like daemon processes for
//! background downloads on Unix systems.
//!
//! # Features
//!
//! - **Async-only design**: Built from the ground up for async/await with tokio
//! - **HTTP client**: Full-featured HTTP client with support for all standard methods
//! - **File downloads**: Efficient streaming downloads directly to disk with progress tracking
//! - **File uploads**: Support for uploading files or data with progress tracking
//! - **Background downloads**: Platform-specific background downloads (NSURLSession on Apple, daemon processes on Unix)
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
//! Background downloads continue even when your app is suspended or terminated.
//! The implementation varies by platform:
//! - **Apple platforms**: Uses NSURLSession background downloads
//! - **Unix systems**: Uses daemon processes for true background operation
//! - **Other platforms**: Uses resumable downloads with retry logic
//!
//! ```rust,no_run
//! use frakt::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::new()?;
//!
//!     let response = client
//!         .download_background("https://example.com/large-video.mp4", "./downloads/video.mp4")
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
//!   - **Unix systems**: Daemon processes for true background downloads
//!   - **All platforms**: Resumable downloads with retry logic
//!
//! # Performance
//!
//! **Apple platforms** benefit from NSURLSession's optimized networking stack:
//! - HTTP/2 and HTTP/3 support
//! - Connection pooling and reuse
//! - Automatic compression (gzip, deflate)
//! - Network quality-of-service (QoS) handling
//! - Cellular and Wi-Fi network management
//! - True background transfer capabilities
//!
//! **Other platforms** use reqwest with additional features:
//! - HTTP/2 support via reqwest
//! - Connection pooling and keep-alive
//! - Automatic decompression
//! - Background downloads via daemon processes (Unix) or resumable downloads

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

// Multi-platform support via backend abstraction

pub use auth::Auth;
pub use client::{
    BackendType, BackgroundDownloadBuilder, Client, ClientBuilder, DownloadBuilder,
    DownloadResponse, UploadBuilder,
};
pub use error::{Error, Result};
pub use request::{Request, RequestBuilder};
pub use response::{Response, ResponseStream};

// Re-export body types
pub use body::Body;
pub use body::MultipartPart;
pub use cookies::{Cookie, CookieAcceptPolicy, CookieJar};
pub use websocket::{CloseCode, Message, WebSocket, WebSocketBuilder};

// Re-export http types for convenience
pub use http;

mod auth;
pub mod backend;
mod body;
mod client;
mod cookies;
mod error;
mod request;
mod response;
mod task;
mod websocket;

#[cfg(all(target_os = "android", vampire))]
pub use vampire::JNI_OnLoad;

#[cfg(all(target_os = "android", vampire))]
#[path = "../tests"]
mod android_tests {
    #[path = "integration_tests.rs"]
    mod integration_tests;
}
