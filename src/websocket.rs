//! WebSocket support with backend abstraction

// With no WebSocket backend compiled in (Cronet without reqwest), WebSocket
// has no variants and these methods never use their arguments.
#![cfg_attr(
    not(any(
        feature = "backend-reqwest",
        all(feature = "backend-foundation", target_vendor = "apple"),
        all(feature = "backend-winhttp", windows)
    )),
    allow(unused_variables)
)]

use crate::{Error, Result};

// Backend-specific implementations are in their respective modules

/// [`WebSocket`] message types
#[derive(Debug, Clone)]
pub enum Message {
    /// Text message
    Text(String),
    /// Binary data message
    Binary(Vec<u8>),
}

impl From<Vec<u8>> for Message {
    fn from(data: Vec<u8>) -> Self {
        Message::Binary(data)
    }
}

impl From<String> for Message {
    fn from(text: String) -> Self {
        Message::Text(text)
    }
}

impl Message {
    /// Create a text message
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    /// Create a binary message
    pub fn binary(data: impl Into<Vec<u8>>) -> Self {
        Self::Binary(data.into())
    }
}

/// [`WebSocket`] close codes.
///
/// These codes indicate the reason why a WebSocket connection was closed.
/// They correspond to the standard WebSocket close codes defined in RFC 6455.
///
/// # Examples
///
/// ```rust,no_run
/// use frakt::{Client, CloseCode};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let mut websocket = client
///     .websocket()
///     .connect("wss://echo.websocket.org")
///     .await?;
///
/// // Close the connection normally
/// websocket.close(CloseCode::Normal, Some("Goodbye"));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum CloseCode {
    /// Normal closure (1000).
    ///
    /// The connection was closed normally and the purpose for which it was opened has been fulfilled.
    Normal = 1000,
    /// Going away (1001).
    ///
    /// The endpoint is going away, such as a server going down or a browser navigating away.
    GoingAway = 1001,
    /// Protocol error (1002).
    ///
    /// The connection was terminated due to a protocol error.
    ProtocolError = 1002,
    /// Unsupported data (1003).
    ///
    /// The connection was terminated because the endpoint received data of a type it cannot accept.
    UnsupportedData = 1003,
    /// No status received (1005).
    ///
    /// No status code was actually present. This is a reserved value and MUST NOT be set as a status code.
    NoStatusReceived = 1005,
    /// Abnormal closure (1006).
    ///
    /// The connection was closed abnormally without a close frame being sent.
    AbnormalClosure = 1006,
    /// Invalid frame payload data (1007).
    ///
    /// The connection was terminated because the endpoint received data inconsistent with the type of the message.
    InvalidFramePayloadData = 1007,
    /// Policy violation (1008).
    ///
    /// The connection was terminated because the endpoint received a message that violates its policy.
    PolicyViolation = 1008,
    /// Message too big (1009).
    ///
    /// The connection was terminated because the endpoint received a message that is too big for it to process.
    MessageTooBig = 1009,
    /// Mandatory extension (1010).
    ///
    /// The connection was terminated because the client expected the server to negotiate one or more extensions.
    MandatoryExtension = 1010,
    /// Internal server error (1011).
    ///
    /// The connection was terminated because the server encountered an unexpected condition.
    InternalServerError = 1011,
    /// TLS handshake (1015).
    ///
    /// The connection was closed due to a failure to perform a TLS handshake.
    TlsHandshake = 1015,
}

/// A [`WebSocket`] connection using NSURLSessionWebSocketTask.
///
/// This struct represents an active [`WebSocket`] connection that can send and receive messages.
/// It uses NSURLSession's [`WebSocket`] implementation for optimal performance and integration
/// with the Apple ecosystem.
///
/// # Examples
///
/// ```rust,no_run
/// use frakt::{Client, Message, CloseCode};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let mut websocket = client
///     .websocket()
///     .maximum_message_size(1024 * 1024)
///     .connect("wss://echo.websocket.org")
///     .await?;
///
/// // Send a text message
/// websocket.send(Message::text("Hello, WebSocket!")).await?;
///
/// // Receive a message
/// let message = websocket.receive().await?;
/// match message {
///     Message::Text(text) => println!("Received text: {}", text),
///     Message::Binary(data) => println!("Received {} bytes", data.len()),
/// }
///
/// // Close the connection
/// websocket.close(CloseCode::Normal, Some("Goodbye"));
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
pub enum WebSocket {
    /// Foundation backend using NSURLSessionWebSocketTask
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    Foundation(crate::backend::foundation::FoundationWebSocket),
    /// Windows backend using MessageWebSocket
    #[cfg(all(feature = "backend-winhttp", windows))]
    Windows(crate::backend::windows::WindowsWebSocket),
    /// Reqwest backend using tokio-tungstenite
    #[cfg(feature = "backend-reqwest")]
    Reqwest(crate::backend::reqwest::ReqwestWebSocket),
}

impl WebSocket {
    /// Send a message over the [`WebSocket`] connection.
    ///
    /// This method sends a message to the [`WebSocket`] server. The message can be either
    /// text or binary data, and will be automatically converted from types that implement
    /// `Into<Message>`.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to send (`String`, `&str`, `Vec<u8>`, or [`Message`])
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, Message};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client.websocket().connect("wss://echo.websocket.org").await?;
    ///
    /// // Send text directly
    /// websocket.send(Message::text("Hello, World!")).await?;
    ///
    /// // Send binary data
    /// websocket.send(vec![1, 2, 3, 4]).await?;
    ///
    /// // Send a Message explicitly
    /// websocket.send(Message::text("Explicit message")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&mut self, message: impl Into<Message>) -> Result<()> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocket::Foundation(ws) => ws.send(message.into()).await,
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocket::Windows(ws) => ws.send(message.into()).await,
            #[cfg(feature = "backend-reqwest")]
            WebSocket::Reqwest(ws) => ws.send(message.into()).await,
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Receive a message from the [`WebSocket`] connection.
    ///
    /// This method waits for and returns the next message from the [`WebSocket`] server.
    /// The method will block until a message is received or an error occurs.
    ///
    /// # Returns
    ///
    /// Returns a [`Message`] which can be either text or binary data.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, Message};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client.websocket().connect("wss://echo.websocket.org").await?;
    ///
    /// // Send a message first
    /// websocket.send(Message::text("Hello")).await?;
    ///
    /// // Receive the echo
    /// let message = websocket.receive().await?;
    /// match message {
    ///     Message::Text(text) => println!("Received text: {}", text),
    ///     Message::Binary(data) => println!("Received {} bytes of binary data", data.len()),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn receive(&mut self) -> Result<Message> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocket::Foundation(ws) => ws.receive().await,
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocket::Windows(ws) => ws.receive().await,
            #[cfg(feature = "backend-reqwest")]
            WebSocket::Reqwest(ws) => ws.receive().await,
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Close the [`WebSocket`] connection.
    ///
    /// This method closes the [`WebSocket`] connection with the specified close code and reason.
    /// The connection will be terminated and no further messages can be sent or received.
    ///
    /// # Arguments
    ///
    /// * `code` - The close code indicating the reason for closure
    /// * `reason` - Optional human-readable reason for closure
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, CloseCode};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client.websocket().connect("wss://echo.websocket.org").await?;
    ///
    /// // Close normally
    /// websocket.close(CloseCode::Normal, Some("Session ended"));
    ///
    /// // Close due to policy violation
    /// websocket.close(CloseCode::PolicyViolation, None);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&mut self, code: CloseCode, reason: Option<&str>) -> Result<()> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocket::Foundation(ws) => ws.close(code, reason).await,
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocket::Windows(ws) => ws.close(code, reason).await,
            #[cfg(feature = "backend-reqwest")]
            WebSocket::Reqwest(ws) => ws.close(code, reason).await,
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Get the current close code if the connection has been closed.
    ///
    /// Returns the close code that was used when the connection was closed,
    /// or `None` if the connection is still open.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, CloseCode};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client.websocket().connect("wss://echo.websocket.org").await?;
    ///
    /// // Connection is open
    /// assert_eq!(websocket.close_code(), None);
    ///
    /// websocket.close(CloseCode::Normal, None);
    /// // After closing, code should be available
    /// // (Note: this might not be immediately available)
    /// # Ok(())
    /// # }
    /// ```
    pub fn close_code(&self) -> Option<isize> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocket::Foundation(ws) => ws.close_code(),
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocket::Windows(ws) => ws.close_code(),
            #[cfg(feature = "backend-reqwest")]
            WebSocket::Reqwest(ws) => ws.close_code(),
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Get the close reason if the connection has been closed.
    ///
    /// Returns the human-readable reason that was provided when the connection
    /// was closed, or `None` if no reason was provided or the connection is still open.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, CloseCode};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client.websocket().connect("wss://echo.websocket.org").await?;
    ///
    /// websocket.close(CloseCode::Normal, Some("User requested"));
    /// // After closing, reason should be available
    /// // (Note: this might not be immediately available)
    /// # Ok(())
    /// # }
    /// ```
    pub fn close_reason(&self) -> Option<String> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocket::Foundation(ws) => ws.close_reason(),
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocket::Windows(ws) => ws.close_reason(),
            #[cfg(feature = "backend-reqwest")]
            WebSocket::Reqwest(ws) => ws.close_reason(),
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Set the maximum message size for this [`WebSocket`].
    ///
    /// This controls the maximum size of messages that can be sent or received.
    /// Messages larger than this size will be rejected.
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum message size in bytes
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, Message};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client.websocket().connect("wss://echo.websocket.org").await?;
    ///
    /// // Set maximum message size to 1MB
    /// websocket.set_maximum_message_size(1024 * 1024);
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_maximum_message_size(&mut self, size: isize) {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocket::Foundation(ws) => ws.set_maximum_message_size(size),
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocket::Windows(ws) => ws.set_maximum_message_size(size),
            #[cfg(feature = "backend-reqwest")]
            WebSocket::Reqwest(ws) => ws.set_maximum_message_size(size),
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }

    /// Get the current maximum message size.
    ///
    /// Returns the maximum size of messages that can be sent or received
    /// through this [`WebSocket`] connection.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, Message};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client.websocket().connect("wss://echo.websocket.org").await?;
    ///
    /// let max_size = websocket.maximum_message_size();
    /// println!("Maximum message size: {} bytes", max_size);
    /// # Ok(())
    /// # }
    /// ```
    pub fn maximum_message_size(&self) -> isize {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocket::Foundation(ws) => ws.maximum_message_size(),
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocket::Windows(ws) => ws.maximum_message_size(),
            #[cfg(feature = "backend-reqwest")]
            WebSocket::Reqwest(ws) => ws.maximum_message_size(),
            #[allow(unreachable_patterns)]
            _ => unreachable!("No backend available"),
        }
    }
}

/// Builder for [`WebSocket`] connections.
///
/// This builder allows you to configure [`WebSocket`] connection parameters before
/// establishing the connection. It provides a fluent interface for setting options
/// like maximum message size.
///
/// # Examples
///
/// ```rust,no_run
/// use frakt::{Client, Message};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
/// let mut websocket = client
///     .websocket()
///     .maximum_message_size(1024 * 1024)  // 1MB max messages
///     .connect("wss://echo.websocket.org")
///     .await?;
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
pub enum WebSocketBuilder {
    /// Foundation backend using NSURLSession
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    Foundation(crate::backend::foundation::FoundationWebSocketBuilder),
    /// Windows backend using MessageWebSocket
    #[cfg(all(feature = "backend-winhttp", windows))]
    Windows(crate::backend::windows::WindowsWebSocketBuilder),
    /// Reqwest backend using tokio-tungstenite
    #[cfg(feature = "backend-reqwest")]
    Reqwest(crate::backend::reqwest::ReqwestWebSocketBuilder),
    /// The client's backend has no WebSocket implementation in this build;
    /// connecting fails with an error.
    Unsupported,
}

impl WebSocketBuilder {
    /// Set the maximum message size for the [`WebSocket`] connection.
    ///
    /// This sets the maximum size of messages that can be sent or received
    /// through the [`WebSocket`] connection. Messages larger than this size will be rejected.
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum message size in bytes
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, Message};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client
    ///     .websocket()
    ///     .maximum_message_size(1024 * 1024)  // 1MB
    ///     .connect("wss://echo.websocket.org")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn maximum_message_size(self, size: isize) -> Self {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocketBuilder::Foundation(builder) => {
                WebSocketBuilder::Foundation(builder.maximum_message_size(size))
            }
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocketBuilder::Windows(builder) => {
                WebSocketBuilder::Windows(builder.maximum_message_size(size))
            }
            #[cfg(feature = "backend-reqwest")]
            WebSocketBuilder::Reqwest(builder) => {
                WebSocketBuilder::Reqwest(builder.maximum_message_size(size))
            }
            WebSocketBuilder::Unsupported => WebSocketBuilder::Unsupported,
        }
    }

    /// Connect to the [`WebSocket`] server at the specified URL.
    ///
    /// This method establishes the [`WebSocket`] connection and returns a [`WebSocket`]
    /// instance that can be used to send and receive messages.
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket URL (must use ws:// or wss:// scheme)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::{Client, Message};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    /// let mut websocket = client
    ///     .websocket()
    ///     .connect("wss://echo.websocket.org")
    ///     .await?;
    ///
    /// // WebSocket is now ready for use
    /// websocket.send(Message::text("Hello, WebSocket!")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(self, url: &str) -> Result<WebSocket> {
        match self {
            #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
            WebSocketBuilder::Foundation(builder) => {
                let ws = builder.connect(url).await?;
                Ok(WebSocket::Foundation(ws))
            }
            #[cfg(all(feature = "backend-winhttp", windows))]
            WebSocketBuilder::Windows(builder) => {
                let ws = builder.connect(url).await?;
                Ok(WebSocket::Windows(ws))
            }
            #[cfg(feature = "backend-reqwest")]
            WebSocketBuilder::Reqwest(builder) => {
                let ws = builder.connect(url).await?;
                Ok(WebSocket::Reqwest(ws))
            }
            WebSocketBuilder::Unsupported => {
                let _ = url;
                Err(Error::Internal(
                    "WebSockets are not supported by this backend".to_string(),
                ))
            }
        }
    }
}
