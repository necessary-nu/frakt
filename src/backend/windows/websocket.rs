//! Windows WebSocket implementation using WinHTTP WebSocket APIs
//!
//! The WinHTTP session is synchronous, so each call that waits on the
//! network runs on tokio's blocking pool, where dropping the future that
//! awaits it does not stop it. Everything such a call uses is therefore
//! owned by its task: the handles through an `Arc`, the buffers outright.

use crate::{
    Error, Result,
    websocket::{CloseCode, Message, check_message_size, checked_maximum_message_size},
};
use base64::Engine;
use rand::Rng;
use std::{
    ffi::c_void,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};
use tokio::task::JoinHandle;
use windows::{
    Win32::Foundation::{ERROR_INVALID_PARAMETER, GetLastError},
    Win32::Networking::WinHttp::{
        ERROR_WINHTTP_OPERATION_CANCELLED, WINHTTP_ACCESS_TYPE_DEFAULT_PROXY, WINHTTP_FLAG_SECURE,
        WINHTTP_OPEN_REQUEST_FLAGS, WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET,
        WINHTTP_WEB_SOCKET_BINARY_FRAGMENT_BUFFER_TYPE,
        WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE, WINHTTP_WEB_SOCKET_BUFFER_TYPE,
        WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE, WINHTTP_WEB_SOCKET_MAX_CLOSE_REASON_LENGTH,
        WINHTTP_WEB_SOCKET_UTF8_FRAGMENT_BUFFER_TYPE, WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE,
        WinHttpCloseHandle, WinHttpConnect, WinHttpOpen, WinHttpOpenRequest,
        WinHttpReceiveResponse, WinHttpSendRequest, WinHttpSetOption, WinHttpWebSocketClose,
        WinHttpWebSocketCompleteUpgrade, WinHttpWebSocketQueryCloseStatus, WinHttpWebSocketReceive,
        WinHttpWebSocketSend, WinHttpWebSocketShutdown,
    },
    core::{HSTRING, PCWSTR},
};

/// The size of the buffer `WinHttpWebSocketReceive` fills on each call.
const READ_BUFFER_SIZE: u32 = 16 * 1024;

/// A WinHTTP handle, closed when dropped.
struct Handle(*mut c_void);

impl Handle {
    /// Takes ownership of the handle a WinHTTP function returned. `action`
    /// says what the function was for, in the error for a null handle.
    fn new(handle: *mut c_void, action: &str) -> Result<Self> {
        if handle.is_null() {
            // SAFETY: GetLastError only reads the calling thread's last
            // error code.
            let code = unsafe { GetLastError() }.0;
            return Err(Error::Internal(format!("Failed to {action}: {code}")));
        }
        Ok(Self(handle))
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: the handle is open, since only `new` makes a `Handle` and
        // nothing else closes one, and no call is in progress on it: every
        // call borrows the `Handle` it uses.
        unsafe {
            let _ = WinHttpCloseHandle(self.0);
        }
    }
}

// SAFETY: a WinHTTP handle is not tied to the thread that opened it, and
// WinHTTP arbitrates WebSocket calls made on one from several threads: a
// call that conflicts with one in progress fails with
// ERROR_INVALID_OPERATION.
unsafe impl Send for Handle {}
// SAFETY: as for `Send`.
unsafe impl Sync for Handle {}

struct WebSocketHandles {
    // Fields drop in declaration order. Closing a parent first would
    // invalidate the WebSocket handle while it is still open.
    websocket: Handle,
    _connection: Handle,
    _session: Handle,
}

impl WebSocketHandles {
    /// Opens a session and performs the WebSocket handshake with the server
    /// at `url`. Blocks until the handshake is over.
    fn open(url: &url::Url) -> Result<Self> {
        let scheme = url.scheme();
        let host = url.host_str().ok_or(Error::InvalidUrl)?;
        let port = url.port().unwrap_or(if scheme == "wss" { 443 } else { 80 });
        let path = url.path();
        let query = url.query().map(|q| format!("?{q}")).unwrap_or_default();
        let full_path = format!("{path}{query}");

        // SAFETY: each handle passed to WinHTTP below was returned non-null
        // by it and is owned by a `Handle` still in scope, and the strings
        // and the header buffer are locals that outlive the calls they are
        // passed to.
        unsafe {
            let user_agent_wide = HSTRING::from("frakt-websocket/1.0");
            let session = Handle::new(
                WinHttpOpen(
                    &user_agent_wide,
                    WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
                    PCWSTR::null(),
                    PCWSTR::null(),
                    0,
                ),
                "open WinHTTP session",
            )?;

            let host_wide = HSTRING::from(host);
            let connection = Handle::new(
                WinHttpConnect(session.0, &host_wide, port, 0),
                "connect to server",
            )?;

            let method_wide = HSTRING::from("GET");
            let path_wide = HSTRING::from(&full_path);
            let flags = if scheme == "wss" {
                WINHTTP_FLAG_SECURE
            } else {
                WINHTTP_OPEN_REQUEST_FLAGS(0)
            };
            let request = Handle::new(
                WinHttpOpenRequest(
                    connection.0,
                    &method_wide,
                    &path_wide,
                    PCWSTR::null(),   // HTTP version (default)
                    PCWSTR::null(),   // Referrer
                    std::ptr::null(), // Accept types
                    flags,
                ),
                "open request",
            )?;

            WinHttpSetOption(Some(request.0), WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET, None).map_err(
                |e| Error::Internal(format!("Failed to set WebSocket upgrade option: {e}")),
            )?;

            // RFC 6455: the key is 16 random bytes, base64-encoded.
            let mut key_bytes = [0u8; 16];
            rand::rng().fill_bytes(&mut key_bytes);
            let websocket_key = base64::engine::general_purpose::STANDARD.encode(key_bytes);
            let headers = format!(
                "Connection: Upgrade\r\n\
                 Upgrade: websocket\r\n\
                 Sec-WebSocket-Version: 13\r\n\
                 Sec-WebSocket-Key: {websocket_key}\r\n"
            );
            let headers_wide: Vec<u16> = headers.encode_utf16().chain(std::iter::once(0)).collect();

            WinHttpSendRequest(
                request.0,
                Some(&headers_wide[..headers_wide.len() - 1]), // Exclude null terminator
                None,
                0,
                0,
                0,
            )
            .map_err(|e| {
                Error::Internal(format!("Failed to send WebSocket upgrade request: {e}"))
            })?;

            WinHttpReceiveResponse(request.0, std::ptr::null_mut()).map_err(|e| {
                Error::Internal(format!("Failed to receive WebSocket upgrade response: {e}"))
            })?;

            // The WebSocket handle does not need the request handle it came
            // from, which closes at the end of this block.
            let websocket = Handle::new(
                WinHttpWebSocketCompleteUpgrade(request.0, Some(0)),
                "complete WebSocket upgrade",
            )?;

            Ok(Self {
                websocket,
                _connection: connection,
                _session: session,
            })
        }
    }
}

/// The status code and reason a connection closed with.
struct CloseStatus {
    code: u16,
    reason: Option<String>,
}

/// What a socket shares with its blocking tasks.
struct Connection {
    handles: WebSocketHandles,
    /// Held for every send, shutdown and close. WinHTTP refuses one of
    /// those while another is pending, and they can meet: a `send` or
    /// `close` future dropped part way leaves its call running, and the
    /// receiving task closes the connection on its own account.
    send_channel: Mutex<()>,
    /// How the connection closed, once it has. Written with `send_channel`
    /// held, but never itself held across a WinHTTP call, so that reading it
    /// does not wait on the network.
    close_status: Mutex<Option<CloseStatus>>,
}

impl Connection {
    fn websocket(&self) -> *mut c_void {
        self.handles.websocket.0
    }

    fn is_closed(&self) -> bool {
        lock(&self.close_status).is_some()
    }

    /// Sends one whole message. Blocks.
    fn send(&self, buffer_type: WINHTTP_WEB_SOCKET_BUFFER_TYPE, data: &[u8]) -> Result<()> {
        let _send_channel = lock(&self.send_channel);
        if self.is_closed() {
            return Err(Error::WebSocketClosed);
        }
        // WinHTTP refuses an empty buffer unless its pointer is null
        // (ERROR_INVALID_PARAMETER), which an empty slice's is not.
        let data = (!data.is_empty()).then_some(data);
        // SAFETY: the handle stays open for as long as `self` is borrowed,
        // and the call is synchronous, so WinHTTP has finished with `data`
        // when it returns.
        let status = unsafe { WinHttpWebSocketSend(self.websocket(), buffer_type, data) };
        if status != 0 {
            return Err(winhttp_error("WinHttpWebSocketSend", status));
        }
        Ok(())
    }

    /// Receives one whole message, however many buffers WinHTTP delivers it
    /// in, of at most `limit` bytes. Blocks.
    fn receive(&self, limit: usize) -> Result<Message> {
        let mut assembler = MessageAssembler::new(limit);
        let mut buffer = vec![0u8; READ_BUFFER_SIZE as usize];
        loop {
            let mut read = 0u32;
            let mut buffer_type = WINHTTP_WEB_SOCKET_BUFFER_TYPE::default();
            // SAFETY: the handle stays open for as long as `self` is
            // borrowed. `buffer` is READ_BUFFER_SIZE bytes long and, like
            // the two out-parameters, belongs to this frame; the call is
            // synchronous, so WinHTTP has finished with all three when it
            // returns.
            let status = unsafe {
                WinHttpWebSocketReceive(
                    self.websocket(),
                    buffer.as_mut_ptr().cast(),
                    READ_BUFFER_SIZE,
                    &mut read,
                    &mut buffer_type,
                )
            };
            match status {
                0 => {}
                // WinHTTP's status for a receive that WinHttpWebSocketClose
                // cut short.
                ERROR_WINHTTP_OPERATION_CANCELLED => return Err(Error::WebSocketClosed),
                status => return Err(winhttp_error("WinHttpWebSocketReceive", status)),
            }
            if buffer_type == WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE {
                self.answer_close();
                return Err(Error::WebSocketClosed);
            }
            let data = buffer.get(..read as usize).ok_or_else(|| {
                Error::Internal(format!(
                    "WinHttpWebSocketReceive reported {read} bytes in a buffer of {READ_BUFFER_SIZE}"
                ))
            })?;
            match assembler.push(buffer_type, data) {
                Ok(Some(message)) => return Ok(message),
                Ok(None) => {}
                Err(error) => {
                    if matches!(error, Error::WebSocketMessageTooLarge { .. }) {
                        self.refuse_message();
                    }
                    return Err(error);
                }
            }
        }
    }

    /// Records the status of the close frame the peer sent and answers it,
    /// which WinHTTP leaves to the application.
    fn answer_close(&self) {
        let peer = self.peer_close_status();
        let _send_channel = lock(&self.send_channel);
        if self.is_closed() {
            return;
        }
        // SAFETY: the handle stays open for as long as `self` is borrowed,
        // and no reason buffer is passed.
        let _ =
            unsafe { WinHttpWebSocketClose(self.websocket(), CloseCode::Normal as u16, None, 0) };
        *lock(&self.close_status) = Some(peer);
    }

    fn peer_close_status(&self) -> CloseStatus {
        let mut code = 0u16;
        let mut reason = [0u8; WINHTTP_WEB_SOCKET_MAX_CLOSE_REASON_LENGTH as usize];
        let mut reason_length = 0u32;
        // SAFETY: the handle stays open for as long as `self` is borrowed,
        // and the out-parameters are locals, `reason` being as long as the
        // length passed with it.
        let status = unsafe {
            WinHttpWebSocketQueryCloseStatus(
                self.websocket(),
                &mut code,
                Some(reason.as_mut_ptr().cast()),
                WINHTTP_WEB_SOCKET_MAX_CLOSE_REASON_LENGTH,
                &mut reason_length,
            )
        };
        // 0 is not a status a close frame can carry: Wine's WinHTTP reports
        // it for a frame without one, where Windows documents 1005.
        if status != 0 || code == 0 {
            return CloseStatus {
                code: CloseCode::NoStatusReceived as u16,
                reason: None,
            };
        }
        let reason = reason
            .get(..reason_length as usize)
            .filter(|reason| !reason.is_empty())
            .map(|reason| String::from_utf8_lossy(reason).into_owned());
        CloseStatus { code, reason }
    }

    /// Closes the connection because the peer's message outgrew the limit.
    fn refuse_message(&self) {
        let _send_channel = lock(&self.send_channel);
        if self.is_closed() {
            return;
        }
        // Shutdown, not Close: Close also waits, up to its timeout, for the
        // peer's close frame, and the peer is still sending the message.
        //
        // SAFETY: the handle stays open for as long as `self` is borrowed,
        // and no reason buffer is passed.
        let _ = unsafe {
            WinHttpWebSocketShutdown(self.websocket(), CloseCode::MessageTooBig as u16, None, 0)
        };
        *lock(&self.close_status) = Some(CloseStatus {
            code: CloseCode::MessageTooBig as u16,
            reason: None,
        });
    }

    /// Performs the close handshake, which cuts short a receive in
    /// progress. Blocks until the peer answers or WinHTTP's close timeout
    /// runs out.
    fn close(&self, code: u16, reason: Option<String>) -> Result<()> {
        let reason = reason.filter(|reason| !reason.is_empty());
        let reason_length = match &reason {
            Some(reason) => u32::try_from(reason.len())
                .ok()
                .filter(|length| *length <= WINHTTP_WEB_SOCKET_MAX_CLOSE_REASON_LENGTH)
                .ok_or_else(|| {
                    Error::Internal(format!(
                        "WebSocket close reason is {} bytes long, and the most a close frame carries is {WINHTTP_WEB_SOCKET_MAX_CLOSE_REASON_LENGTH}",
                        reason.len()
                    ))
                })?,
            None => 0,
        };

        let _send_channel = lock(&self.send_channel);
        if self.is_closed() {
            return Ok(());
        }
        // SAFETY: the handle stays open for as long as `self` is borrowed,
        // and `reason`, of which WinHTTP reads `reason_length` bytes,
        // outlives the synchronous call.
        let status = unsafe {
            WinHttpWebSocketClose(
                self.websocket(),
                code,
                reason.as_ref().map(|reason| reason.as_ptr().cast()),
                reason_length,
            )
        };
        // WinHTTP did nothing if it refused the arguments. After any other
        // failure the close frame is out or the connection is gone.
        if status != ERROR_INVALID_PARAMETER.0 {
            *lock(&self.close_status) = Some(CloseStatus { code, reason });
        }
        if status != 0 {
            return Err(winhttp_error("WinHttpWebSocketClose", status));
        }
        Ok(())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn winhttp_error(function: &str, status: u32) -> Error {
    Error::Internal(format!("{function} failed with error: {status}"))
}

/// One message being put back together from the buffers
/// `WinHttpWebSocketReceive` delivers it in, and refused once it outgrows
/// the limit.
struct MessageAssembler {
    limit: usize,
    /// Whether the buffers so far held text; `None` before the first.
    text: Option<bool>,
    payload: Vec<u8>,
}

impl MessageAssembler {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            text: None,
            payload: Vec::new(),
        }
    }

    /// Adds the next buffer of the message, and returns the message if that
    /// was its last. A buffer that takes the message past the limit is not
    /// added.
    fn push(
        &mut self,
        buffer_type: WINHTTP_WEB_SOCKET_BUFFER_TYPE,
        data: &[u8],
    ) -> Result<Option<Message>> {
        let (text, last) = match buffer_type {
            WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE => (false, true),
            WINHTTP_WEB_SOCKET_BINARY_FRAGMENT_BUFFER_TYPE => (false, false),
            WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE => (true, true),
            WINHTTP_WEB_SOCKET_UTF8_FRAGMENT_BUFFER_TYPE => (true, false),
            other => {
                return Err(Error::Internal(format!(
                    "Unknown WebSocket buffer type: {}",
                    other.0
                )));
            }
        };
        if *self.text.get_or_insert(text) != text {
            return Err(Error::Internal(
                "WinHTTP changed the type of a WebSocket message part way through it".to_string(),
            ));
        }

        let size = self.payload.len().saturating_add(data.len());
        check_message_size(size, self.limit)?;
        if size > self.payload.capacity() {
            // Left to itself, Vec doubles its capacity, which could take it
            // to nearly twice the limit.
            let capacity = self
                .payload
                .capacity()
                .saturating_mul(2)
                .clamp(size, self.limit);
            self.payload.reserve_exact(capacity - self.payload.len());
        }
        self.payload.extend_from_slice(data);
        if !last {
            return Ok(None);
        }

        let payload = std::mem::take(&mut self.payload);
        if !text {
            return Ok(Some(Message::Binary(payload)));
        }
        // Only now: a fragment may end part way through a character.
        String::from_utf8(payload)
            .map(|text| Some(Message::Text(text)))
            .map_err(|e| Error::Internal(format!("Invalid UTF-8 in WebSocket message: {e}")))
    }
}

/// Windows WebSocket implementation using WinHTTP WebSocket APIs
pub struct WindowsWebSocket {
    connection: Arc<Connection>,
    /// The receive in progress. WinHTTP allows one at a time and a blocking
    /// call outlives the future awaiting it, so the task is kept here for
    /// the next `receive` to take up, rather than lost with the future.
    receiving: Option<JoinHandle<Result<Message>>>,
    max_message_size: usize,
}

impl WindowsWebSocket {
    /// Create a new Windows WebSocket connection with the default maximum
    /// message size
    pub async fn new(url: &str) -> Result<Self> {
        WindowsWebSocketBuilder::new().connect(url).await
    }

    /// Send a message
    pub async fn send(&mut self, message: Message) -> Result<()> {
        if self.connection.is_closed() {
            return Err(Error::WebSocketClosed);
        }
        check_message_size(message.len(), self.max_message_size)?;

        let (buffer_type, data) = match message {
            Message::Text(text) => (
                WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE,
                text.into_bytes(),
            ),
            Message::Binary(data) => (WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE, data),
        };
        // The binding takes the length from the slice and panics if it does
        // not fit the u32 WinHTTP is passed.
        if u32::try_from(data.len()).is_err() {
            return Err(Error::Internal(format!(
                "WinHTTP cannot send a WebSocket message of {} bytes in one call",
                data.len()
            )));
        }

        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || connection.send(buffer_type, &data))
            .await
            .map_err(|e| Error::Internal(format!("WebSocket send task failed: {e}")))?
    }

    /// Receive a message
    ///
    /// Dropping the returned future loses nothing: the message it was
    /// waiting for goes to the next call.
    pub async fn receive(&mut self) -> Result<Message> {
        let receiving = match &mut self.receiving {
            Some(receiving) => receiving,
            None => {
                if self.connection.is_closed() {
                    return Err(Error::WebSocketClosed);
                }
                let connection = self.connection.clone();
                let limit = self.max_message_size;
                self.receiving.insert(tokio::task::spawn_blocking(move || {
                    connection.receive(limit)
                }))
            }
        };
        let outcome = receiving.await;
        self.receiving = None;
        outcome.map_err(|e| Error::Internal(format!("WebSocket receive task failed: {e}")))?
    }

    /// Close the WebSocket connection
    pub async fn close(&mut self, code: CloseCode, reason: Option<&str>) -> Result<()> {
        if self.connection.is_closed() {
            return Ok(());
        }

        let connection = self.connection.clone();
        let reason = reason.map(str::to_owned);
        let result = tokio::task::spawn_blocking(move || connection.close(code as u16, reason))
            .await
            .map_err(|e| Error::Internal(format!("WebSocket close task failed: {e}")))?;
        if self.connection.is_closed() {
            self.receiving = None;
        }
        result
    }

    /// Get the close code if the connection has been closed: the peer's if
    /// the peer closed it, otherwise the one it was closed with from here
    pub fn close_code(&self) -> Option<isize> {
        lock(&self.connection.close_status)
            .as_ref()
            .map(|status| status.code as isize)
    }

    /// Get the close reason if the connection has been closed with one
    pub fn close_reason(&self) -> Option<String> {
        lock(&self.connection.close_status)
            .as_ref()
            .and_then(|status| status.reason.clone())
    }

    /// Get the maximum message size, fixed when the connection was made
    pub fn maximum_message_size(&self) -> usize {
        self.max_message_size
    }
}

impl Drop for WindowsWebSocket {
    fn drop(&mut self) {
        let Some(receiving) = self.receiving.take() else {
            return;
        };
        if receiving.is_finished() {
            return;
        }
        // A thread is blocked in WinHttpWebSocketReceive, holding the
        // handles open, for as long as the peer stays quiet. Closing the
        // connection is how WinHTTP lets another thread cut that short;
        // closing the handle under the call would be a race. It takes a
        // thread because the close handshake blocks too.
        let connection = self.connection.clone();
        let _ = std::thread::Builder::new()
            .name("frakt-websocket-close".to_string())
            .spawn(move || {
                let _ = connection.close(CloseCode::GoingAway as u16, None);
            });
    }
}

/// Windows WebSocket builder
#[derive(Default)]
pub struct WindowsWebSocketBuilder {
    max_message_size: Option<usize>,
}

impl WindowsWebSocketBuilder {
    /// Create a new Windows WebSocket builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum message size for the WebSocket connection
    pub fn maximum_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Connect to a WebSocket URL
    pub async fn connect(self, url: &str) -> Result<WindowsWebSocket> {
        let max_message_size = checked_maximum_message_size(self.max_message_size)?;
        let url = url::Url::parse(url).map_err(|_| Error::InvalidUrl)?;

        let handles = tokio::task::spawn_blocking(move || WebSocketHandles::open(&url))
            .await
            .map_err(|e| Error::Internal(format!("WebSocket connect task failed: {e}")))??;

        Ok(WindowsWebSocket {
            connection: Arc::new(Connection {
                handles,
                send_channel: Mutex::new(()),
                close_status: Mutex::new(None),
            }),
            receiving: None,
            max_message_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BINARY: WINHTTP_WEB_SOCKET_BUFFER_TYPE = WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE;
    const BINARY_FRAGMENT: WINHTTP_WEB_SOCKET_BUFFER_TYPE =
        WINHTTP_WEB_SOCKET_BINARY_FRAGMENT_BUFFER_TYPE;
    const TEXT: WINHTTP_WEB_SOCKET_BUFFER_TYPE = WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE;
    const TEXT_FRAGMENT: WINHTTP_WEB_SOCKET_BUFFER_TYPE =
        WINHTTP_WEB_SOCKET_UTF8_FRAGMENT_BUFFER_TYPE;

    type Buffer<'a> = (WINHTTP_WEB_SOCKET_BUFFER_TYPE, &'a [u8]);

    /// Feeds `buffers` to an assembler the way the receive loop does: up to
    /// the first that ends the message or is refused.
    fn assemble(limit: usize, buffers: &[Buffer]) -> Result<Message> {
        let mut assembler = MessageAssembler::new(limit);
        for (index, (buffer_type, data)) in buffers.iter().enumerate() {
            if let Some(message) = assembler.push(*buffer_type, data)? {
                assert_eq!(index + 1, buffers.len(), "the message ended early");
                return Ok(message);
            }
        }
        panic!("the message did not end");
    }

    fn binary(result: Result<Message>) -> Vec<u8> {
        match result {
            Ok(Message::Binary(data)) => data,
            other => panic!("expected a binary message, got {other:?}"),
        }
    }

    fn text(result: Result<Message>) -> String {
        match result {
            Ok(Message::Text(text)) => text,
            other => panic!("expected a text message, got {other:?}"),
        }
    }

    /// The limit the message was refused for passing.
    fn too_large<T: std::fmt::Debug>(result: Result<T>) -> usize {
        match result {
            Err(Error::WebSocketMessageTooLarge { limit }) => limit,
            other => panic!("expected the message to be refused, got {other:?}"),
        }
    }

    fn internal_error(result: Result<Message>) -> String {
        match result {
            Err(Error::Internal(message)) => message,
            other => panic!("expected an internal error, got {other:?}"),
        }
    }

    #[test]
    fn a_whole_message_below_or_at_the_limit_passes() {
        assert_eq!(binary(assemble(4, &[(BINARY, b"abc")])), b"abc");
        assert_eq!(binary(assemble(4, &[(BINARY, b"abcd")])), b"abcd");
        assert_eq!(text(assemble(4, &[(TEXT, b"abc")])), "abc");
        assert_eq!(text(assemble(4, &[(TEXT, b"abcd")])), "abcd");
    }

    #[test]
    fn a_whole_message_above_the_limit_is_refused() {
        assert_eq!(too_large(assemble(4, &[(BINARY, b"abcde")])), 4);
        assert_eq!(too_large(assemble(4, &[(TEXT, b"abcde")])), 4);
    }

    #[test]
    fn fragments_are_joined_in_order() {
        let binary_buffers: &[Buffer] = &[
            (BINARY_FRAGMENT, b"ab"),
            (BINARY_FRAGMENT, b"cde"),
            (BINARY, b"f"),
        ];
        assert_eq!(binary(assemble(1024, binary_buffers)), b"abcdef");

        let text_buffers: &[Buffer] = &[
            (TEXT_FRAGMENT, b"ab"),
            (TEXT_FRAGMENT, b"cde"),
            (TEXT, b"f"),
        ];
        assert_eq!(text(assemble(1024, text_buffers)), "abcdef");
    }

    #[test]
    fn fragments_adding_up_to_the_limit_pass() {
        let binary_buffers: &[Buffer] = &[
            (BINARY_FRAGMENT, b"ab"),
            (BINARY_FRAGMENT, b"cd"),
            (BINARY, b"ef"),
        ];
        assert_eq!(binary(assemble(7, binary_buffers)), b"abcdef");
        assert_eq!(binary(assemble(6, binary_buffers)), b"abcdef");

        let text_buffers: &[Buffer] = &[
            (TEXT_FRAGMENT, b"ab"),
            (TEXT_FRAGMENT, b"cd"),
            (TEXT, b"ef"),
        ];
        assert_eq!(text(assemble(7, text_buffers)), "abcdef");
        assert_eq!(text(assemble(6, text_buffers)), "abcdef");
    }

    #[test]
    fn the_buffer_that_crosses_the_limit_is_refused_and_not_kept() {
        let mut assembler = MessageAssembler::new(5);
        assert!(matches!(assembler.push(BINARY_FRAGMENT, b"ab"), Ok(None)));
        assert!(matches!(assembler.push(BINARY_FRAGMENT, b"cd"), Ok(None)));
        assert_eq!(too_large(assembler.push(BINARY_FRAGMENT, b"ef")), 5);
        assert_eq!(assembler.payload, b"abcd");

        let mut assembler = MessageAssembler::new(5);
        assert!(matches!(assembler.push(TEXT_FRAGMENT, b"abc"), Ok(None)));
        assert_eq!(too_large(assembler.push(TEXT, b"def")), 5);
        assert_eq!(assembler.payload, b"abc");
    }

    #[test]
    fn the_payload_buffer_never_outgrows_the_limit() {
        let limit = 1000;
        let mut assembler = MessageAssembler::new(limit);
        for _ in 0..142 {
            assert!(matches!(assembler.push(BINARY_FRAGMENT, &[0; 7]), Ok(None)));
            assert!(assembler.payload.capacity() <= limit);
        }
        let data = binary(assembler.push(BINARY, &[0; 6]).transpose().unwrap());
        assert_eq!(data.len(), limit);
        assert!(data.capacity() <= limit);
    }

    #[test]
    fn a_character_split_between_fragments_survives() {
        let whole = "na\u{ef}ve \u{20ac}5 \u{1f980}";
        let bytes = whole.as_bytes();
        for split in 0..=bytes.len() {
            let (head, tail) = bytes.split_at(split);
            let buffers: &[Buffer] = &[(TEXT_FRAGMENT, head), (TEXT, tail)];
            assert_eq!(
                text(assemble(bytes.len(), buffers)),
                whole,
                "split at {split}"
            );
        }

        let one_byte_each: Vec<Buffer> = bytes
            .iter()
            .enumerate()
            .map(|(index, byte)| {
                let buffer_type = if index + 1 == bytes.len() {
                    TEXT
                } else {
                    TEXT_FRAGMENT
                };
                (buffer_type, std::slice::from_ref(byte))
            })
            .collect();
        assert_eq!(text(assemble(bytes.len(), &one_byte_each)), whole);
    }

    #[test]
    fn text_that_is_not_utf8_once_whole_is_an_error() {
        let cut_short: &[Buffer] = &[(TEXT_FRAGMENT, b"ok \xe2\x82"), (TEXT, b"")];
        assert!(internal_error(assemble(1024, cut_short)).contains("UTF-8"));
        assert!(internal_error(assemble(1024, &[(TEXT, b"\xff")])).contains("UTF-8"));

        assert_eq!(binary(assemble(1024, &[(BINARY, b"\xff")])), b"\xff");
    }

    #[test]
    fn a_message_that_changes_type_is_an_error() {
        let cases: [&[Buffer]; 4] = [
            &[(TEXT_FRAGMENT, b"ab"), (BINARY, b"cd")],
            &[(BINARY_FRAGMENT, b"ab"), (TEXT, b"cd")],
            &[(TEXT_FRAGMENT, b"ab"), (BINARY_FRAGMENT, b"cd")],
            &[(BINARY_FRAGMENT, b"ab"), (TEXT_FRAGMENT, b"cd")],
        ];
        for buffers in cases {
            assert!(internal_error(assemble(1024, buffers)).contains("type"));
        }
    }

    #[test]
    fn empty_messages_and_fragments_pass() {
        assert_eq!(text(assemble(1, &[(TEXT, b"")])), "");
        assert_eq!(binary(assemble(1, &[(BINARY, b"")])), b"");

        let buffers: &[Buffer] = &[
            (BINARY_FRAGMENT, b""),
            (BINARY_FRAGMENT, b"ab"),
            (BINARY_FRAGMENT, b""),
            (BINARY, b""),
        ];
        assert_eq!(binary(assemble(2, buffers)), b"ab");
    }

    #[test]
    fn a_buffer_that_is_not_message_data_is_an_error() {
        for buffer_type in [
            WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE,
            WINHTTP_WEB_SOCKET_BUFFER_TYPE(99),
        ] {
            assert!(internal_error(assemble(1024, &[(buffer_type, b"")])).contains("buffer type"));
        }
    }
}
