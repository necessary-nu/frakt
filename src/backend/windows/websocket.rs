//! Windows WebSocket implementation using WinHTTP WebSocket APIs

use crate::{
    Error, Result,
    websocket::{CloseCode, Message},
};
use base64::Engine;
use rand::Rng;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicI32, Ordering},
};
use windows::{
    Win32::Foundation::GetLastError,
    Win32::Networking::WinHttp::{
        WINHTTP_ACCESS_TYPE_DEFAULT_PROXY, WINHTTP_FLAG_SECURE, WINHTTP_OPEN_REQUEST_FLAGS,
        WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET, WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE,
        WINHTTP_WEB_SOCKET_BUFFER_TYPE, WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE,
        WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE, WinHttpCloseHandle, WinHttpConnect,
        WinHttpOpen, WinHttpOpenRequest, WinHttpReceiveResponse, WinHttpSendRequest,
        WinHttpSetOption, WinHttpWebSocketClose, WinHttpWebSocketCompleteUpgrade,
        WinHttpWebSocketReceive, WinHttpWebSocketSend,
    },
    core::{HSTRING, PCWSTR},
};

/// Thread-safe wrapper for WinHTTP handles
struct WebSocketHandles {
    websocket_handle: *mut std::ffi::c_void,
    session_handle: *mut std::ffi::c_void,
    connection_handle: *mut std::ffi::c_void,
}

unsafe impl Send for WebSocketHandles {}
unsafe impl Sync for WebSocketHandles {}

impl Drop for WebSocketHandles {
    fn drop(&mut self) {
        unsafe {
            if !self.websocket_handle.is_null() {
                let _ = WinHttpCloseHandle(self.websocket_handle);
            }
            if !self.connection_handle.is_null() {
                let _ = WinHttpCloseHandle(self.connection_handle);
            }
            if !self.session_handle.is_null() {
                let _ = WinHttpCloseHandle(self.session_handle);
            }
        }
    }
}

/// Convert library Message to WinHTTP WebSocket buffer type
fn convert_message_for_sending(message: &Message) -> (WINHTTP_WEB_SOCKET_BUFFER_TYPE, Vec<u8>) {
    match message {
        Message::Text(text) => (
            WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE,
            text.as_bytes().to_vec(),
        ),
        Message::Binary(data) => (WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE, data.clone()),
    }
}

/// Windows WebSocket implementation using WinHTTP WebSocket APIs
pub struct WindowsWebSocket {
    /// Thread-safe wrapper for WinHTTP handles
    handles: Arc<WebSocketHandles>,
    /// Close code
    close_code: Arc<AtomicI32>,
    /// Close reason
    close_reason: Arc<RwLock<Option<String>>>,
    /// Flag to track if connection is closed
    closed: Arc<std::sync::atomic::AtomicBool>,
    /// Maximum message size for this WebSocket
    max_message_size: Arc<std::sync::atomic::AtomicUsize>,
}

impl WindowsWebSocket {
    /// Create a new Windows WebSocket connection
    pub async fn new(url: &str) -> Result<Self> {
        // Parse URL components
        let parsed_url = url::Url::parse(url).map_err(|_| Error::InvalidUrl)?;
        let scheme = parsed_url.scheme();
        let host = parsed_url.host_str().ok_or(Error::InvalidUrl)?;
        let port = parsed_url
            .port()
            .unwrap_or(if scheme == "wss" { 443 } else { 80 });
        let path = parsed_url.path();
        let query = parsed_url
            .query()
            .map(|q| format!("?{}", q))
            .unwrap_or_default();
        let full_path = format!("{}{}", path, query);

        let close_code = Arc::new(AtomicI32::new(0));
        let close_reason = Arc::new(RwLock::new(None));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let max_message_size = Arc::new(std::sync::atomic::AtomicUsize::new(1024 * 1024)); // 1MB default

        // Create WebSocket connection synchronously to avoid thread safety issues with raw pointers
        let (websocket_handle, session_handle, connection_handle) = {
            unsafe {
                // Open WinHTTP session
                let user_agent_wide = HSTRING::from("frakt-websocket/1.0");
                let session = WinHttpOpen(
                    &user_agent_wide,
                    WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
                    PCWSTR::null(),
                    PCWSTR::null(),
                    0,
                );

                if session.is_null() {
                    return Err(Error::Internal(format!(
                        "Failed to open WinHTTP session: {}",
                        GetLastError().0
                    )));
                }

                // Connect to server
                let host_wide = HSTRING::from(host);
                let connection = WinHttpConnect(session, &host_wide, port, 0);

                if connection.is_null() {
                    let _ = WinHttpCloseHandle(session);
                    return Err(Error::Internal(format!(
                        "Failed to connect to server: {}",
                        GetLastError().0
                    )));
                }

                // Open WebSocket request
                let method_wide = HSTRING::from("GET");
                let path_wide = HSTRING::from(&full_path);
                let mut flags = WINHTTP_OPEN_REQUEST_FLAGS(0);
                if scheme == "wss" {
                    flags = WINHTTP_FLAG_SECURE;
                }

                let request_handle = WinHttpOpenRequest(
                    connection,
                    &method_wide,
                    &path_wide,
                    PCWSTR::null(),   // HTTP version (default)
                    PCWSTR::null(),   // Referrer
                    std::ptr::null(), // Accept types
                    flags,
                );

                if request_handle.is_null() {
                    let _ = WinHttpCloseHandle(connection);
                    let _ = WinHttpCloseHandle(session);
                    return Err(Error::Internal(format!(
                        "Failed to open request: {}",
                        GetLastError().0
                    )));
                }

                // Set the request to be a WebSocket upgrade request
                WinHttpSetOption(
                    Some(request_handle),
                    WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET,
                    None, // No data needed for this option
                )
                .map_err(|e| {
                    let _ = WinHttpCloseHandle(request_handle);
                    let _ = WinHttpCloseHandle(connection);
                    let _ = WinHttpCloseHandle(session);
                    Error::Internal(format!("Failed to set WebSocket upgrade option: {}", e))
                })?;

                // Add WebSocket headers for proper handshake
                // Generate a cryptographically random 16-byte key as per RFC 6455
                let mut key_bytes = [0u8; 16];
                rand::rng().fill_bytes(&mut key_bytes);
                let websocket_key = base64::engine::general_purpose::STANDARD.encode(key_bytes);
                let headers = format!(
                    "Connection: Upgrade\r\n\
                     Upgrade: websocket\r\n\
                     Sec-WebSocket-Version: 13\r\n\
                     Sec-WebSocket-Key: {}\r\n",
                    websocket_key
                );
                let headers_wide: Vec<u16> =
                    headers.encode_utf16().chain(std::iter::once(0)).collect();

                // Send WebSocket upgrade request with proper headers
                WinHttpSendRequest(
                    request_handle,
                    Some(&headers_wide[..headers_wide.len() - 1]), // Exclude null terminator
                    None,
                    0,
                    0,
                    0,
                )
                .map_err(|e| {
                    let _ = WinHttpCloseHandle(request_handle);
                    let _ = WinHttpCloseHandle(connection);
                    let _ = WinHttpCloseHandle(session);
                    Error::Internal(format!("Failed to send WebSocket upgrade request: {}", e))
                })?;

                // Receive response
                WinHttpReceiveResponse(request_handle, std::ptr::null_mut()).map_err(|e| {
                    let _ = WinHttpCloseHandle(request_handle);
                    let _ = WinHttpCloseHandle(connection);
                    let _ = WinHttpCloseHandle(session);
                    Error::Internal(format!(
                        "Failed to receive WebSocket upgrade response: {}",
                        e
                    ))
                })?;

                // Complete WebSocket upgrade
                let websocket_handle = WinHttpWebSocketCompleteUpgrade(request_handle, Some(0));
                if websocket_handle.is_null() {
                    let _ = WinHttpCloseHandle(request_handle);
                    let _ = WinHttpCloseHandle(connection);
                    let _ = WinHttpCloseHandle(session);
                    return Err(Error::Internal(format!(
                        "Failed to complete WebSocket upgrade: {}",
                        GetLastError().0
                    )));
                }

                // Close the original request handle as it's no longer needed
                let _ = WinHttpCloseHandle(request_handle);

                Ok::<(_, _, _), Error>((websocket_handle, session, connection))
            }
        }?;

        Ok(Self {
            handles: Arc::new(WebSocketHandles {
                websocket_handle,
                session_handle,
                connection_handle,
            }),
            close_code,
            close_reason,
            closed,
            max_message_size,
        })
    }

    /// Send a message
    pub async fn send(&mut self, message: Message) -> Result<()> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Internal(
                "WebSocket connection is closed".to_string(),
            ));
        }

        let (message_type, data) = convert_message_for_sending(&message);
        let handles = self.handles.clone();

        tokio::task::spawn_blocking(move || unsafe {
            let result = WinHttpWebSocketSend(handles.websocket_handle, message_type, Some(&data));

            if result != 0 {
                Err(Error::Internal(format!(
                    "WinHttpWebSocketSend failed with error: {}",
                    result
                )))
            } else {
                Ok(())
            }
        })
        .await
        .map_err(|e| Error::Internal(format!("WebSocket send task failed: {}", e)))?
    }

    /// Receive a message
    pub async fn receive(&mut self) -> Result<Message> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Internal(
                "WebSocket connection is closed".to_string(),
            ));
        }

        let handles = self.handles.clone();

        tokio::task::spawn_blocking(move || unsafe {
            let mut buffer_type = WINHTTP_WEB_SOCKET_BUFFER_TYPE::default();
            let mut buffer = vec![0u8; 8192];
            let mut bytes_read = 0u32;

            let result = WinHttpWebSocketReceive(
                handles.websocket_handle,
                buffer.as_mut_ptr() as *mut _,
                buffer.len() as u32,
                &mut bytes_read,
                &mut buffer_type,
            );

            if result != 0 {
                return Err(Error::Internal(format!(
                    "WinHttpWebSocketReceive failed with error: {}",
                    result
                )));
            }

            buffer.truncate(bytes_read as usize);

            match buffer_type {
                WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE => {
                    let text = String::from_utf8(buffer).map_err(|e| {
                        Error::Internal(format!("Invalid UTF-8 in WebSocket message: {}", e))
                    })?;
                    Ok(Message::Text(text))
                }
                WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE => Ok(Message::Binary(buffer)),
                WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE => Err(Error::Internal(
                    "WebSocket connection closed by peer".to_string(),
                )),
                _ => Err(Error::Internal(
                    "Unknown WebSocket message type".to_string(),
                )),
            }
        })
        .await
        .map_err(|e| Error::Internal(format!("WebSocket receive task failed: {}", e)))?
    }

    /// Close the WebSocket connection
    pub async fn close(&mut self, code: CloseCode, reason: Option<&str>) -> Result<()> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        // Store close information
        self.close_code.store(code as i32, Ordering::Relaxed);
        if let Some(reason_str) = reason {
            *self.close_reason.write().unwrap() = Some(reason_str.to_string());
        }

        let handles = self.handles.clone();
        let close_reason = reason.map(|s| s.to_string());

        tokio::task::spawn_blocking(move || unsafe {
            let reason_bytes = close_reason.map(|r| r.into_bytes()).unwrap_or_default();

            let result = WinHttpWebSocketClose(
                handles.websocket_handle,
                code as u16,
                if reason_bytes.is_empty() {
                    None
                } else {
                    Some(reason_bytes.as_ptr() as *const _)
                },
                reason_bytes.len() as u32,
            );

            if result != 0 {
                Err(Error::Internal(format!(
                    "WinHttpWebSocketClose failed with error: {}",
                    result
                )))
            } else {
                Ok(())
            }
        })
        .await
        .map_err(|e| Error::Internal(format!("WebSocket close task failed: {}", e)))??;

        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Get the current close code if the connection has been closed
    pub fn close_code(&self) -> Option<isize> {
        let code = self.close_code.load(Ordering::Relaxed);
        if code == 0 { None } else { Some(code as isize) }
    }

    /// Get the close reason if the connection has been closed
    pub fn close_reason(&self) -> Option<String> {
        self.close_reason.read().unwrap().clone()
    }

    /// Set the maximum message size for this WebSocket
    pub fn set_maximum_message_size(&self, size: isize) {
        self.max_message_size
            .store(size as usize, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get the current maximum message size
    pub fn maximum_message_size(&self) -> isize {
        self.max_message_size
            .load(std::sync::atomic::Ordering::Relaxed) as isize
    }
}

/// Windows WebSocket builder
pub struct WindowsWebSocketBuilder {
    /// Maximum message size for the WebSocket connection
    max_message_size: Option<isize>,
}

impl WindowsWebSocketBuilder {
    /// Create a new Windows WebSocket builder
    pub fn new() -> Self {
        Self {
            max_message_size: None,
        }
    }

    /// Set the maximum message size for the WebSocket connection
    pub fn maximum_message_size(mut self, size: isize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Connect to a WebSocket URL
    pub async fn connect(self, url: &str) -> Result<WindowsWebSocket> {
        let websocket = WindowsWebSocket::new(url).await?;

        // Set maximum message size if specified
        if let Some(size) = self.max_message_size {
            websocket.set_maximum_message_size(size);
        }

        Ok(websocket)
    }
}
