//! Foundation WebSocket implementation using NSURLSessionWebSocketTask

use crate::{
    Error, Result,
    websocket::{CloseCode, Message},
};
use objc2::{AnyThread, rc::Retained};
use objc2_foundation::{
    NSData, NSError, NSString, NSURL, NSURLSession, NSURLSessionWebSocketCloseCode,
    NSURLSessionWebSocketMessage, NSURLSessionWebSocketMessageType, NSURLSessionWebSocketTask,
};
use tokio::sync::oneshot;

impl From<CloseCode> for NSURLSessionWebSocketCloseCode {
    fn from(code: CloseCode) -> Self {
        NSURLSessionWebSocketCloseCode(code as isize)
    }
}

/// Foundation WebSocket implementation
pub struct FoundationWebSocket {
    /// The underlying NSURLSessionWebSocketTask
    pub task: Retained<NSURLSessionWebSocketTask>,
    /// The delegate - must be kept alive for the WebSocket lifetime
    pub delegate: Retained<crate::backend::foundation::delegate::websocket::WebSocketDelegate>,
    /// Flag to track if connection is closed
    pub closed: bool,
}

impl FoundationWebSocket {
    /// Create a new Foundation WebSocket connection
    pub async fn new(session: Retained<NSURLSession>, url: &str) -> Result<Self> {
        let nsurl =
            NSURL::URLWithString(&NSString::from_str(url)).ok_or_else(|| Error::InvalidUrl)?;

        let (delegate, connection_receiver) =
            crate::backend::foundation::delegate::websocket::WebSocketDelegate::new_with_channel();

        let task = session.webSocketTaskWithURL(&nsurl);

        task.setDelegate(Some(objc2::runtime::ProtocolObject::from_ref(&*delegate)));

        task.resume();

        // Wait for connection to be established with timeout
        let connection_result =
            tokio::time::timeout(std::time::Duration::from_secs(30), connection_receiver).await;

        match connection_result {
            Ok(Ok(_)) => {
                tracing::debug!("WebSocket::new_foundation - Connection established successfully");
                Ok(Self {
                    task,
                    delegate,
                    closed: false,
                })
            }
            Ok(Err(e)) => {
                tracing::debug!("WebSocket::new_foundation - Connection failed: {:?}", e);
                Err(Error::Internal(e.to_string()))
            }
            Err(_) => {
                tracing::debug!("WebSocket::new_foundation - Connection timed out");

                task.cancel();

                Err(Error::Timeout)
            }
        }
    }

    /// Send a message
    pub async fn send(&mut self, message: Message) -> Result<()> {
        if self.closed {
            return Err(Error::Internal(
                "WebSocket connection is closed".to_string(),
            ));
        }

        let ns_message = match message {
            Message::Text(text) => {
                let ns_string = NSString::from_str(&text);

                NSURLSessionWebSocketMessage::initWithString(
                    NSURLSessionWebSocketMessage::alloc(),
                    &ns_string,
                )
            }
            Message::Binary(data) => {
                let ns_data = NSData::from_vec(data);

                NSURLSessionWebSocketMessage::initWithData(
                    NSURLSessionWebSocketMessage::alloc(),
                    &ns_data,
                )
            }
        };

        let (tx, rx) = oneshot::channel();
        let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
        let completion_block = block2::RcBlock::new(move |error: *mut NSError| {
            let result = if error.is_null() {
                Ok(())
            } else {
                unsafe { Err(Error::from_ns_error(&*error)) }
            };
            if let Ok(mut tx_guard) = tx.lock() {
                if let Some(tx) = tx_guard.take() {
                    let _ = tx.send(result);
                }
            }
        });

        unsafe {
            self.task
                .sendMessage_completionHandler(&ns_message, &completion_block);
        }

        rx.await
            .map_err(|_| Error::Internal("Send operation was cancelled".to_string()))?
    }

    /// Receive a message
    pub async fn receive(&mut self) -> Result<Message> {
        if self.closed {
            return Err(Error::Internal(
                "WebSocket connection is closed".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
        let completion_block = block2::RcBlock::new(
            move |message: *mut NSURLSessionWebSocketMessage, error: *mut NSError| {
                let result = if !error.is_null() {
                    unsafe { Err(Error::from_ns_error(&*error)) }
                } else if !message.is_null() {
                    unsafe {
                        let msg = &*message;
                        let message_type = msg.r#type();
                        match message_type {
                            NSURLSessionWebSocketMessageType::Data => {
                                let data = msg.data();
                                if let Some(data) = data {
                                    Ok(Message::Binary(data.to_vec()))
                                } else {
                                    Err(Error::Internal(
                                        "Failed to get binary data from message".to_string(),
                                    ))
                                }
                            }
                            NSURLSessionWebSocketMessageType::String => {
                                let string = msg.string();
                                if let Some(string) = string {
                                    Ok(Message::Text(string.to_string()))
                                } else {
                                    Err(Error::Internal(
                                        "Failed to get string from message".to_string(),
                                    ))
                                }
                            }
                            _ => Err(Error::Internal("Unknown message type".to_string())),
                        }
                    }
                } else {
                    Err(Error::Internal("No message or error received".to_string()))
                };
                if let Ok(mut tx_guard) = tx.lock() {
                    if let Some(tx) = tx_guard.take() {
                        let _ = tx.send(result);
                    }
                }
            },
        );

        unsafe {
            self.task
                .receiveMessageWithCompletionHandler(&completion_block);
        }

        rx.await
            .map_err(|_| Error::Internal("Receive operation was cancelled".to_string()))?
    }

    /// Close the WebSocket connection
    pub async fn close(&mut self, code: CloseCode, reason: Option<&str>) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let ns_code = NSURLSessionWebSocketCloseCode(code as isize);
        let ns_reason = reason.map(|r| NSData::from_vec(r.as_bytes().to_vec()));

        if let Some(reason_data) = ns_reason {
            self.task
                .cancelWithCloseCode_reason(ns_code, Some(&reason_data));
        } else {
            self.task.cancelWithCloseCode_reason(ns_code, None);
        }

        self.closed = true;
        Ok(())
    }

    /// Get the current close code if the connection has been closed
    pub fn close_code(&self) -> Option<isize> {
        let code = self.task.closeCode();
        if code.0 == 0 { None } else { Some(code.0) }
    }

    /// Get the close reason if the connection has been closed
    pub fn close_reason(&self) -> Option<String> {
        self.task
            .closeReason()
            .map(|data| String::from_utf8_lossy(&data.to_vec()).to_string())
    }

    /// Set the maximum message size for this WebSocket
    pub fn set_maximum_message_size(&self, size: isize) {
        self.task.setMaximumMessageSize(size);
    }

    /// Get the current maximum message size
    pub fn maximum_message_size(&self) -> isize {
        self.task.maximumMessageSize()
    }
}

/// Foundation WebSocket builder
pub struct FoundationWebSocketBuilder {
    /// The NSURLSession instance to use for the WebSocket connection
    pub session: Retained<NSURLSession>,
    /// Maximum message size for the WebSocket connection
    pub max_message_size: Option<isize>,
    /// Keeps a backend's session from being invalidated before the task is
    /// created on it.
    owner: Option<std::sync::Arc<super::Session>>,
}

impl FoundationWebSocketBuilder {
    /// Create a new Foundation WebSocket builder
    pub fn new(session: Retained<NSURLSession>) -> Self {
        Self {
            session,
            max_message_size: None,
            owner: None,
        }
    }

    pub(crate) fn for_backend(backend: &super::FoundationBackend) -> Self {
        Self {
            session: backend.session().clone(),
            max_message_size: None,
            owner: Some(backend.session.clone()),
        }
    }

    /// Set the maximum message size for the WebSocket connection
    pub fn maximum_message_size(mut self, size: isize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Connect to a WebSocket URL
    pub async fn connect(self, url: &str) -> Result<FoundationWebSocket> {
        let websocket = FoundationWebSocket::new(self.session, url).await?;
        drop(self.owner);

        // Set maximum message size if specified
        if let Some(size) = self.max_message_size {
            websocket.set_maximum_message_size(size);
        }

        Ok(websocket)
    }
}
