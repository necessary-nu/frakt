//! WinHTTP request execution for the Windows backend.
//!
//! Lifetime hazards, and how they are handled:
//!
//! - WinHTTP can call back, or complete a read into our buffer, until it
//!   reports `WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING` for the request handle.
//!   The callback context is therefore an `Arc<RequestContext>` of which
//!   WinHTTP owns one reference, released only in that notification. The read
//!   buffer and the upload body live in the context.
//! - Every path that abandons a request (an error, the `execute` future being
//!   dropped at an await, the body stream being dropped) closes the request
//!   handle through [`RequestHandle`], which makes WinHTTP cancel the transfer.
//! - The connection handle belongs to the context, so it is closed only after
//!   the request handle has finished closing.

use crate::backend::types::{BackendRequest, BackendResponse, channel_body};
use crate::{Error, Result};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use std::ffi::c_void;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use windows::Win32::Foundation::GetLastError;
use windows::Win32::Networking::WinHttp::*;
use windows::core::{HSTRING, PCWSTR};

use super::error::{map_windows_error, map_winhttp_error};

const READ_BUFFER_SIZE: usize = 64 * 1024;

/// Chunks the body channel holds before no further read is issued, so at most
/// about 1 MiB of a body is buffered in frakt.
const BODY_CHANNEL_CHUNKS: usize = 16;

/// WinHTTP's sentinel for a failed `WinHttpSetStatusCallback`.
const INVALID_STATUS_CALLBACK: usize = usize::MAX;

/// A WinHTTP handle, closed when dropped.
struct Handle(*mut c_void);

// SAFETY: WinHTTP handles may be used and closed from any thread.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                let _ = WinHttpCloseHandle(self.0);
            }
        }
    }
}

/// The asynchronous WinHTTP session shared by every request of one backend.
pub(crate) struct Session {
    handle: Handle,
}

impl Session {
    /// Open a session that uses the system proxy configuration, including
    /// WPAD and PAC scripts, or the given `proxy` list (WinHTTP syntax, for
    /// example `http=host:3128 https=host:3128`).
    pub(crate) fn open(user_agent: &str, proxy: Option<&str>) -> Result<Self> {
        let agent = HSTRING::from(user_agent);
        let handle = unsafe {
            match proxy {
                Some(proxy) => WinHttpOpen(
                    &agent,
                    WINHTTP_ACCESS_TYPE_NAMED_PROXY,
                    &HSTRING::from(proxy),
                    PCWSTR::null(),
                    WINHTTP_FLAG_ASYNC,
                ),
                None => {
                    let automatic = WinHttpOpen(
                        &agent,
                        WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,
                        PCWSTR::null(),
                        PCWSTR::null(),
                        WINHTTP_FLAG_ASYNC,
                    );
                    if automatic.is_null() {
                        // AUTOMATIC_PROXY needs Windows 8.1; DEFAULT_PROXY only
                        // reads the static `netsh winhttp` configuration.
                        WinHttpOpen(
                            &agent,
                            WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
                            PCWSTR::null(),
                            PCWSTR::null(),
                            WINHTTP_FLAG_ASYNC,
                        )
                    } else {
                        automatic
                    }
                }
            }
        };
        if handle.is_null() {
            return Err(last_error("WinHttpOpen"));
        }
        Ok(Session {
            handle: Handle(handle),
        })
    }
}

/// Backend settings a request needs.
pub(crate) struct RequestConfig<'a> {
    pub session: &'a Arc<Session>,
    pub default_headers: Option<&'a HeaderMap>,
    pub cookie_storage: Option<&'a Arc<crate::backend::CookieJar>>,
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub ignore_certificate_errors: bool,
}

enum Event {
    SendComplete,
    HeadersAvailable,
    Read(Bytes),
    Failed(u32),
}

struct RequestContext {
    events: mpsc::UnboundedSender<Event>,
    /// `READ_BUFFER_SIZE` bytes that WinHTTP writes into asynchronously.
    read_buffer: *mut u8,
    /// Passed to `WinHttpSendRequest`, which reads it until the response
    /// arrives.
    upload: Vec<u8>,
    /// Held so the connection handle outlives the request handle.
    _connect: Handle,
    _session: Arc<Session>,
}

// SAFETY: the read buffer is only written by WinHTTP while a read is pending
// and only read by the callback that completes it; one read is outstanding
// at a time.
unsafe impl Send for RequestContext {}
unsafe impl Sync for RequestContext {}

impl Drop for RequestContext {
    fn drop(&mut self) {
        let buffer = std::ptr::slice_from_raw_parts_mut(self.read_buffer, READ_BUFFER_SIZE);
        drop(unsafe { Box::from_raw(buffer) });
    }
}

/// The request handle. Dropping it closes the handle, cancelling whatever
/// WinHTTP is doing for the request.
struct RequestHandle(Handle);

impl RequestHandle {
    fn raw(&self) -> *mut c_void {
        self.0.0
    }
}

unsafe extern "system" fn status_callback(
    _handle: *mut c_void,
    context: usize,
    status: u32,
    info: *mut c_void,
    info_len: u32,
) {
    if context == 0 {
        return;
    }
    let context = context as *const RequestContext;

    if status == WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING {
        // The last notification for the handle: release WinHTTP's reference.
        drop(unsafe { Arc::from_raw(context) });
        return;
    }

    let context = unsafe { &*context };
    let event = match status {
        WINHTTP_CALLBACK_STATUS_SENDREQUEST_COMPLETE => Event::SendComplete,
        WINHTTP_CALLBACK_STATUS_HEADERS_AVAILABLE => Event::HeadersAvailable,
        WINHTTP_CALLBACK_STATUS_READ_COMPLETE => {
            let len = (info_len as usize).min(READ_BUFFER_SIZE);
            if len == 0 || info.is_null() {
                Event::Read(Bytes::new())
            } else {
                let data = unsafe { std::slice::from_raw_parts(info as *const u8, len) };
                Event::Read(Bytes::copy_from_slice(data))
            }
        }
        WINHTTP_CALLBACK_STATUS_REQUEST_ERROR => {
            let code = if info.is_null() {
                ERROR_WINHTTP_INTERNAL_ERROR
            } else {
                unsafe { (*(info as *const WINHTTP_ASYNC_RESULT)).dwError }
            };
            Event::Failed(code)
        }
        _ => return,
    };
    let _ = context.events.send(event);
}

/// Execute an HTTP request with WinHTTP, returning once the response headers
/// have arrived. The body is read as the returned stream is consumed.
pub(crate) async fn execute(
    config: RequestConfig<'_>,
    request: BackendRequest,
) -> Result<BackendResponse> {
    let url = request.url;
    let https = match url.scheme() {
        "https" => true,
        "http" => false,
        _ => return Err(Error::InvalidUrl),
    };
    let host = match url.host() {
        Some(url::Host::Ipv6(address)) => address.to_string(),
        Some(host) => host.to_string(),
        None => return Err(Error::InvalidUrl),
    };
    let port = url.port_or_known_default().ok_or(Error::InvalidUrl)?;
    let mut object = url.path().to_string();
    if let Some(query) = url.query() {
        object.push('?');
        object.push_str(query);
    }

    let (upload, content_type) = encode_body(request.body)?;
    let cookies = config
        .cookie_storage
        .and_then(|storage| storage.get_cookies_for_url(&url).ok());
    let headers = request_headers(
        config.default_headers,
        &request.headers,
        cookies,
        content_type,
    );
    let read_timeout = request.read_timeout.or(config.read_timeout);
    let progress = request.progress_callback;
    let head_request = request.method == http::Method::HEAD;

    let connect =
        Handle(unsafe { WinHttpConnect(config.session.handle.0, &HSTRING::from(host), port, 0) });
    if connect.0.is_null() {
        return Err(last_error("WinHttpConnect"));
    }

    let flags = if https {
        WINHTTP_FLAG_SECURE
    } else {
        WINHTTP_OPEN_REQUEST_FLAGS(0)
    };
    let raw = unsafe {
        WinHttpOpenRequest(
            connect.0,
            &HSTRING::from(request.method.as_str()),
            &HSTRING::from(object),
            PCWSTR::null(),
            PCWSTR::null(),
            std::ptr::null(),
            flags,
        )
    };
    if raw.is_null() {
        return Err(last_error("WinHttpOpenRequest"));
    }

    let (events_tx, mut events) = mpsc::unbounded_channel();
    let read_buffer = Box::into_raw(vec![0u8; READ_BUFFER_SIZE].into_boxed_slice()) as *mut u8;
    let context = Arc::new(RequestContext {
        events: events_tx,
        read_buffer,
        upload,
        _connect: connect,
        _session: config.session.clone(),
    });
    // Declared after `context` so it is dropped, and the handle closed, first.
    let handle = RequestHandle(Handle(raw));

    let notifications = WINHTTP_CALLBACK_FLAG_SENDREQUEST_COMPLETE
        | WINHTTP_CALLBACK_FLAG_HEADERS_AVAILABLE
        | WINHTTP_CALLBACK_FLAG_READ_COMPLETE
        | WINHTTP_CALLBACK_FLAG_REQUEST_ERROR
        | WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING;
    let previous =
        unsafe { WinHttpSetStatusCallback(handle.raw(), Some(status_callback), notifications, 0) };
    if previous.map(|callback| callback as usize) == Some(INVALID_STATUS_CALLBACK) {
        return Err(last_error("WinHttpSetStatusCallback"));
    }

    let context_value = Arc::into_raw(context.clone()) as usize;
    if let Err(error) = unsafe {
        WinHttpSetOption(
            Some(handle.raw()),
            WINHTTP_OPTION_CONTEXT_VALUE,
            Some(&context_value.to_ne_bytes()),
        )
    } {
        // WinHTTP did not take the reference, so HANDLE_CLOSING won't free it.
        drop(unsafe { Arc::from_raw(context_value as *const RequestContext) });
        return Err(map_windows_error(error));
    }

    // Cookies are handled by frakt's own jar.
    set_option_u32(
        handle.raw(),
        WINHTTP_OPTION_DISABLE_FEATURE,
        WINHTTP_DISABLE_COOKIES,
    )?;
    if config.ignore_certificate_errors {
        set_option_u32(
            handle.raw(),
            WINHTTP_OPTION_SECURITY_FLAGS,
            SECURITY_FLAG_IGNORE_UNKNOWN_CA
                | SECURITY_FLAG_IGNORE_CERT_DATE_INVALID
                | SECURITY_FLAG_IGNORE_CERT_CN_INVALID
                | SECURITY_FLAG_IGNORE_CERT_WRONG_USAGE,
        )?;
    }

    // The receive timeouts bound each wait for the network (the response
    // headers, then every WinHttpReadData), which is the read timeout's
    // meaning. Reads are only issued when the consumer has room, so a slow
    // consumer does not run the clock.
    let connect_ms = timeout_ms(config.connect_timeout);
    let read_ms = timeout_ms(read_timeout);
    unsafe { WinHttpSetTimeouts(handle.raw(), connect_ms, connect_ms, read_ms, read_ms) }
        .map_err(map_windows_error)?;
    set_option_u32(
        handle.raw(),
        WINHTTP_OPTION_RECEIVE_RESPONSE_TIMEOUT,
        read_ms as u32,
    )?;

    let upload_len = context.upload.len() as u64;
    if upload_len > 0 {
        if let Some(progress) = &progress {
            progress(0, Some(upload_len));
        }
    }
    let upload = (!context.upload.is_empty()).then(|| context.upload.as_ptr() as *const c_void);
    unsafe {
        WinHttpSendRequest(
            handle.raw(),
            (!headers.is_empty()).then_some(headers.as_slice()),
            upload,
            upload_len as u32,
            upload_len as u32,
            context_value,
        )
    }
    .map_err(map_windows_error)?;

    expect_event(&mut events, |event| matches!(event, Event::SendComplete)).await?;
    if upload_len > 0 {
        if let Some(progress) = &progress {
            progress(upload_len, Some(upload_len));
        }
    }

    unsafe { WinHttpReceiveResponse(handle.raw(), std::ptr::null_mut()) }
        .map_err(map_windows_error)?;
    expect_event(&mut events, |event| {
        matches!(event, Event::HeadersAvailable)
    })
    .await?;

    let status = query_status(handle.raw())?;
    let response_headers = query_response_headers(handle.raw())?;
    let final_url = query_url(handle.raw()).unwrap_or(url);

    if let Some(storage) = config.cookie_storage {
        let _ = storage.process_response_headers(&final_url, &response_headers);
    }

    let expected_len = if head_request
        || status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED
        || response_headers.contains_key(http::header::CONTENT_ENCODING)
    {
        None
    } else {
        response_headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
    };

    let (tx, rx) = mpsc::channel(BODY_CHANNEL_CHUNKS);
    tokio::spawn(read_body(handle, context, events, tx, expected_len));

    Ok(BackendResponse {
        status,
        headers: response_headers,
        url: final_url,
        body: channel_body(rx),
        redirect_headers: vec![],
    })
}

/// Read the body into `tx`, one `WinHttpReadData` per reserved channel slot.
async fn read_body(
    handle: RequestHandle,
    context: Arc<RequestContext>,
    mut events: mpsc::UnboundedReceiver<Event>,
    tx: mpsc::Sender<Result<Bytes>>,
    expected_len: Option<u64>,
) {
    let mut received = 0u64;
    loop {
        let Ok(permit) = tx.reserve().await else {
            return;
        };

        if let Err(error) = unsafe {
            WinHttpReadData(
                handle.raw(),
                context.read_buffer as *mut c_void,
                READ_BUFFER_SIZE as u32,
                std::ptr::null_mut(),
            )
        } {
            permit.send(Err(map_windows_error(error)));
            return;
        }

        let event = tokio::select! {
            event = events.recv() => event,
            // The consumer went away mid-read; returning closes the handle,
            // which cancels the read.
            _ = tx.closed() => return,
        };

        match event {
            Some(Event::Read(data)) if data.is_empty() => {
                if let Some(expected) = expected_len {
                    if received < expected {
                        permit.send(Err(Error::Network {
                            code: i64::from(ERROR_WINHTTP_CONNECTION_ERROR),
                            message: format!(
                                "connection closed after {} of {} body bytes",
                                received, expected
                            ),
                        }));
                    }
                }
                return;
            }
            Some(Event::Read(data)) => {
                received += data.len() as u64;
                permit.send(Ok(data));
            }
            Some(Event::Failed(code)) => {
                permit.send(Err(map_winhttp_error(code)));
                return;
            }
            Some(_) => {
                permit.send(Err(Error::Internal(
                    "Unexpected WinHTTP notification while reading".to_string(),
                )));
                return;
            }
            None => return,
        }
    }
}

async fn expect_event(
    events: &mut mpsc::UnboundedReceiver<Event>,
    wanted: impl Fn(&Event) -> bool,
) -> Result<()> {
    match events.recv().await {
        Some(Event::Failed(code)) => Err(map_winhttp_error(code)),
        Some(event) if wanted(&event) => Ok(()),
        Some(_) => Err(Error::Internal(
            "Unexpected WinHTTP notification".to_string(),
        )),
        None => Err(Error::Internal("WinHTTP notifications stopped".to_string())),
    }
}

fn timeout_ms(timeout: Option<Duration>) -> i32 {
    match timeout {
        // WinHTTP treats -1 as "wait indefinitely".
        None => -1,
        Some(timeout) => timeout.as_millis().clamp(1, i32::MAX as u128) as i32,
    }
}

fn set_option_u32(handle: *mut c_void, option: u32, value: u32) -> Result<()> {
    unsafe { WinHttpSetOption(Some(handle), option, Some(&value.to_ne_bytes())) }
        .map_err(map_windows_error)
}

fn last_error(operation: &str) -> Error {
    let code = unsafe { GetLastError() }.0;
    if (12000..13000).contains(&code) {
        map_winhttp_error(code)
    } else {
        Error::Internal(format!("{} failed with Win32 error {}", operation, code))
    }
}

fn encode_body(body: Option<crate::body::Body>) -> Result<(Vec<u8>, Option<String>)> {
    Ok(match body {
        None | Some(crate::body::Body::Empty) => (Vec::new(), None),
        Some(crate::body::Body::Bytes {
            content,
            content_type,
        }) => (content.to_vec(), Some(content_type)),
        Some(crate::body::Body::Form { fields }) => {
            let encoded = fields
                .iter()
                .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            (
                encoded.into_bytes(),
                Some("application/x-www-form-urlencoded".to_string()),
            )
        }
        Some(crate::body::Body::Json { value }) => (
            serde_json::to_vec(&value).map_err(|e| Error::Json(e.to_string()))?,
            Some("application/json".to_string()),
        ),
        Some(crate::body::Body::Multipart { .. }) => {
            return Err(Error::Internal(
                "Multipart not yet supported with WinHTTP".to_string(),
            ));
        }
    })
}

/// The request headers as the CRLF-separated UTF-16 block WinHTTP takes.
fn request_headers(
    default_headers: Option<&HeaderMap>,
    headers: &HeaderMap,
    cookies: Option<HeaderMap>,
    content_type: Option<String>,
) -> Vec<u16> {
    let mut merged = default_headers.cloned().unwrap_or_default();
    for name in headers.keys() {
        merged.remove(name);
    }
    for (name, value) in headers {
        merged.append(name, value.clone());
    }
    if let Some(content_type) = content_type {
        if !merged.contains_key(http::header::CONTENT_TYPE) {
            if let Ok(value) = HeaderValue::from_str(&content_type) {
                merged.insert(http::header::CONTENT_TYPE, value);
            }
        }
    }
    if let Some(cookies) = cookies {
        if !merged.contains_key(http::header::COOKIE) {
            for (name, value) in &cookies {
                merged.append(name, value.clone());
            }
        }
    }

    let mut block = Vec::new();
    for (name, value) in &merged {
        block.extend(name.as_str().encode_utf16());
        block.extend(": ".encode_utf16());
        // WinHTTP narrows header text to one byte per code unit, so widening
        // each byte to the code unit of the same value sends non-ASCII values
        // (UTF-8 or otherwise) unchanged.
        block.extend(value.as_bytes().iter().map(|&byte| u16::from(byte)));
        block.extend("\r\n".encode_utf16());
    }
    block
}

fn query_status(handle: *mut c_void) -> Result<StatusCode> {
    let mut status: u32 = 0;
    let mut size = std::mem::size_of::<u32>() as u32;
    unsafe {
        WinHttpQueryHeaders(
            handle,
            WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
            PCWSTR::null(),
            Some(&mut status as *mut u32 as *mut c_void),
            &mut size,
            std::ptr::null_mut(),
        )
    }
    .map_err(map_windows_error)?;
    u16::try_from(status)
        .ok()
        .and_then(|status| StatusCode::from_u16(status).ok())
        .ok_or_else(|| Error::Internal(format!("Invalid HTTP status {}", status)))
}

fn query_response_headers(handle: *mut c_void) -> Result<HeaderMap> {
    let mut size: u32 = 0;
    // Fails with ERROR_INSUFFICIENT_BUFFER and reports the size in bytes.
    let _ = unsafe {
        WinHttpQueryHeaders(
            handle,
            WINHTTP_QUERY_RAW_HEADERS_CRLF,
            PCWSTR::null(),
            None,
            &mut size,
            std::ptr::null_mut(),
        )
    };
    let mut buffer = vec![0u16; (size as usize).div_ceil(2)];
    unsafe {
        WinHttpQueryHeaders(
            handle,
            WINHTTP_QUERY_RAW_HEADERS_CRLF,
            PCWSTR::null(),
            Some(buffer.as_mut_ptr() as *mut c_void),
            &mut size,
            std::ptr::null_mut(),
        )
    }
    .map_err(map_windows_error)?;
    buffer.truncate(size as usize / 2);

    let mut headers = HeaderMap::new();
    // The first line is the status line.
    for line in buffer.split(|&unit| unit == u16::from(b'\n')).skip(1) {
        let line = line.strip_suffix(&[u16::from(b'\r')]).unwrap_or(line);
        let Some(colon) = line.iter().position(|&unit| unit == u16::from(b':')) else {
            continue;
        };
        let name = String::from_utf16_lossy(&line[..colon]);
        let value = header_bytes(&line[colon + 1..]);
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(name.trim().as_bytes()),
            HeaderValue::from_bytes(value.trim_ascii()),
        ) {
            headers.append(name, value);
        }
    }
    Ok(headers)
}

/// Undo WinHTTP's widening of response header bytes: code units up to 0xFF
/// are the original bytes; anything wider is kept as UTF-8.
fn header_bytes(units: &[u16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(units.len());
    for c in char::decode_utf16(units.iter().copied()) {
        let c = c.unwrap_or(char::REPLACEMENT_CHARACTER);
        if let Ok(byte) = u8::try_from(u32::from(c)) {
            bytes.push(byte);
        } else {
            let mut utf8 = [0u8; 4];
            bytes.extend_from_slice(c.encode_utf8(&mut utf8).as_bytes());
        }
    }
    bytes
}

/// The URL the response came from, after redirects.
fn query_url(handle: *mut c_void) -> Option<url::Url> {
    let mut size: u32 = 0;
    let _ = unsafe { WinHttpQueryOption(handle, WINHTTP_OPTION_URL, None, &mut size) };
    if size == 0 {
        return None;
    }
    let mut buffer = vec![0u16; (size as usize).div_ceil(2)];
    unsafe {
        WinHttpQueryOption(
            handle,
            WINHTTP_OPTION_URL,
            Some(buffer.as_mut_ptr() as *mut c_void),
            &mut size,
        )
    }
    .ok()?;
    let len = buffer
        .iter()
        .position(|&unit| unit == 0)
        .unwrap_or(buffer.len());
    url::Url::parse(&String::from_utf16_lossy(&buffer[..len])).ok()
}
