//! Reqwest WebSocket implementation using tokio-tungstenite

use crate::{
    Error, Result,
    websocket::{CloseCode, Message},
};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, protocol::CloseFrame},
};

/// Reqwest WebSocket implementation
pub struct ReqwestWebSocket {
    /// The underlying WebSocket stream
    pub stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    /// Flag to track if connection is closed
    pub closed: bool,
    max_message_size: usize,
}

impl ReqwestWebSocket {
    /// Create a new Reqwest WebSocket connection with the default maximum
    /// message size
    pub async fn new(url: &str) -> Result<Self> {
        Self::new_with_config(url, None).await
    }

    /// Create a new Reqwest WebSocket connection with configuration
    pub async fn new_with_config(url: &str, max_message_size: Option<usize>) -> Result<Self> {
        use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

        let max_message_size = crate::websocket::checked_maximum_message_size(max_message_size)?;
        // A frame is part of a message, so the frame limit can be no larger;
        // left at tungstenite's 16 MiB it would let one frame of a too-large
        // message be buffered before the message limit is applied.
        let config = WebSocketConfig::default()
            .max_message_size(Some(max_message_size))
            .max_frame_size(Some(max_message_size));

        let (stream, _) = tokio_tungstenite::connect_async_with_config(url, Some(config), false)
            .await
            .map_err(|e| Error::Network {
                code: -1,
                message: format!("WebSocket connection failed: {}", e),
            })?;

        Ok(Self {
            stream,
            closed: false,
            max_message_size,
        })
    }

    /// Send a message
    pub async fn send(&mut self, message: Message) -> Result<()> {
        if self.closed {
            return Err(Error::WebSocketClosed);
        }

        crate::websocket::check_message_size(message.len(), self.max_message_size)?;

        let tungstenite_message = match message {
            Message::Text(text) => tungstenite::Message::Text(text.into()),
            Message::Binary(data) => tungstenite::Message::Binary(data.into()),
        };

        self.stream
            .send(tungstenite_message)
            .await
            .map_err(|e| Error::Network {
                code: -1,
                message: format!("Failed to send WebSocket message: {}", e),
            })
    }

    /// Receive a message
    pub async fn receive(&mut self) -> Result<Message> {
        if self.closed {
            return Err(Error::WebSocketClosed);
        }

        loop {
            match self.stream.next().await {
                Some(Ok(tungstenite::Message::Text(text))) => {
                    return Ok(Message::Text(text.to_string()));
                }
                Some(Ok(tungstenite::Message::Binary(data))) => {
                    return Ok(Message::Binary(data.to_vec()));
                }
                Some(Ok(tungstenite::Message::Close(_))) => {
                    self.closed = true;
                    return Err(Error::WebSocketClosed);
                }
                Some(Ok(tungstenite::Message::Ping(_))) => {
                    // Pings are automatically handled by tungstenite
                    continue;
                }
                Some(Ok(tungstenite::Message::Pong(_))) => {
                    // Pongs are automatically handled by tungstenite
                    continue;
                }
                Some(Ok(tungstenite::Message::Frame(_))) => {
                    // Raw frames are not exposed at this level
                    continue;
                }
                Some(Err(tungstenite::Error::Capacity(
                    tungstenite::error::CapacityError::MessageTooLong { .. },
                ))) => {
                    self.closed = true;
                    return Err(Error::WebSocketMessageTooLarge {
                        limit: self.max_message_size,
                    });
                }
                Some(Err(e)) => {
                    self.closed = true;
                    return Err(Error::Network {
                        code: -1,
                        message: format!("WebSocket error: {}", e),
                    });
                }
                None => {
                    self.closed = true;
                    return Err(Error::WebSocketClosed);
                }
            }
        }
    }

    /// Close the WebSocket connection
    pub async fn close(&mut self, code: CloseCode, reason: Option<&str>) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let close_frame = reason.map(|r| CloseFrame {
            code: tungstenite::protocol::frame::coding::CloseCode::from(code as u16),
            reason: r.to_string().into(),
        });

        let close_message = tungstenite::Message::Close(close_frame);

        let result = self.stream.send(close_message).await;
        self.closed = true;

        result.map_err(|e| Error::Network {
            code: -1,
            message: format!("Failed to close WebSocket: {}", e),
        })
    }

    /// Get the current close code if the connection has been closed
    pub fn close_code(&self) -> Option<isize> {
        if self.closed {
            Some(CloseCode::Normal as isize)
        } else {
            None
        }
    }

    /// Get the close reason if the connection has been closed
    pub fn close_reason(&self) -> Option<String> {
        if self.closed {
            Some("Connection closed".to_string())
        } else {
            None
        }
    }

    /// Get the maximum message size, fixed when the connection was made
    pub fn maximum_message_size(&self) -> usize {
        self.max_message_size
    }
}

/// Reqwest WebSocket builder
pub struct ReqwestWebSocketBuilder {
    /// Maximum message size for the WebSocket connection
    pub max_message_size: Option<usize>,
}

impl Default for ReqwestWebSocketBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestWebSocketBuilder {
    /// Create a new Reqwest WebSocket builder
    pub fn new() -> Self {
        Self {
            max_message_size: None,
        }
    }

    /// Set the maximum message size for the WebSocket connection
    pub fn maximum_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Connect to a WebSocket URL
    pub async fn connect(self, url: &str) -> Result<ReqwestWebSocket> {
        ReqwestWebSocket::new_with_config(url, self.max_message_size).await
    }
}
