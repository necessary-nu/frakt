//! WinHTTP request execution for the Windows backend.
//!
//! Lifetime hazards, and how they are handled:
//!
//! - WinHTTP can call back, or complete a read into our buffer, until it
//!   reports `WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING` for the request handle.
//!   The callback context is therefore an `Arc<RequestContext>` of which
//!   WinHTTP owns one reference, released only in that notification. The read
//!   buffer and the bytes being uploaded live in the context.
//! - Every path that abandons a request (an error, the `execute` future being
//!   dropped at an await, the body stream being dropped) closes the request
//!   handle through [`RequestHandle`], which makes WinHTTP cancel the transfer.
//! - The connection handle is shared by the requests of one redirect chain
//!   that go to the same origin. Each of their contexts holds it, so it is
//!   closed only after the last of their request handles has finished
//!   closing.
//!
//! WinHTTP's own redirect following is off: it re-sends every header the
//! caller supplied, credentials included, to whatever origin a redirect
//! names. [`execute`] follows redirects itself, one request handle per hop,
//! sending what [`redirect::next_hop`] and the cookie jar allow for that
//! hop's URL.

use crate::backend::redirect;
use crate::backend::types::{
    BackendRequest, BackendResponse, BodyAccounting, BodyIntegrity, ProgressCallback, channel_body,
};
use crate::body::{Body, FileBody};
use crate::{Error, Result};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use url::Url;
use windows::Win32::Foundation::GetLastError;
use windows::Win32::Globalization::{
    CP_ACP, MULTI_BYTE_TO_WIDE_CHAR_FLAGS, MultiByteToWideChar, WideCharToMultiByte,
};
use windows::Win32::Networking::WinHttp::*;
use windows::core::{HSTRING, PCSTR, PCWSTR};

use super::error::{map_windows_error, map_winhttp_error};

const READ_BUFFER_SIZE: usize = 64 * 1024;

/// How much of a body one `WinHttpWriteData` sends, which is also how much of
/// a file body is in memory at a time.
const UPLOAD_CHUNK_SIZE: usize = 64 * 1024;

/// How often one request is sent again on WinHTTP's demand
/// (`ERROR_WINHTTP_RESEND_REQUEST`): two more legs for a proxy's NTLM or
/// Negotiate handshake and two for the server's.
const MAX_RESENDS: usize = 4;

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
    /// Where cookies are sent from and stored; `None` when cookies are off.
    pub cookie_jar: Option<&'a crate::backend::CookieJar>,
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub ignore_certificate_errors: bool,
}

enum Event {
    SendComplete,
    /// A `WinHttpWriteData` completed, having sent this many bytes.
    Written(u32),
    HeadersAvailable,
    Read(Bytes),
    Failed(u32),
}

struct RequestContext {
    events: mpsc::UnboundedSender<Event>,
    /// `READ_BUFFER_SIZE` bytes that WinHTTP writes into asynchronously.
    read_buffer: *mut u8,
    /// What WinHTTP was last given to send: the whole body passed to
    /// `WinHttpSendRequest`, which reads it until the response arrives, or
    /// the chunk passed to `WinHttpWriteData`. It lives here rather than in
    /// the future that made the call because that future can be dropped
    /// while WinHTTP is still reading.
    sending: Mutex<Bytes>,
    /// Held so the connection handle outlives the request handle.
    _connect: Arc<Handle>,
    _session: Arc<Session>,
}

impl RequestContext {
    /// Keep `data` readable until the request handle has closed and return
    /// its address. It replaces what was held before, so WinHTTP must have
    /// finished with that.
    fn hold(&self, data: Bytes) -> *const c_void {
        let address = data.as_ptr() as *const c_void;
        *self
            .sending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = data;
        address
    }
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
        // SAFETY: the context value is the pointer `Exchange::open` took from
        // `Arc::into_raw` for WinHTTP, and this notification comes once.
        drop(unsafe { Arc::from_raw(context) });
        return;
    }

    // SAFETY: WinHTTP's reference keeps the context alive until
    // HANDLE_CLOSING, which has not been delivered yet.
    let context = unsafe { &*context };
    let event = match status {
        WINHTTP_CALLBACK_STATUS_SENDREQUEST_COMPLETE => Event::SendComplete,
        WINHTTP_CALLBACK_STATUS_WRITE_COMPLETE => {
            if info.is_null() {
                Event::Failed(ERROR_WINHTTP_INTERNAL_ERROR)
            } else {
                // SAFETY: for this notification `info` points to a DWORD,
                // the number of bytes written.
                Event::Written(unsafe { (info as *const u32).read_unaligned() })
            }
        }
        WINHTTP_CALLBACK_STATUS_HEADERS_AVAILABLE => Event::HeadersAvailable,
        WINHTTP_CALLBACK_STATUS_READ_COMPLETE => {
            let len = (info_len as usize).min(READ_BUFFER_SIZE);
            if len == 0 || info.is_null() {
                Event::Read(Bytes::new())
            } else {
                // SAFETY: for this notification `info` is the buffer given
                // to `WinHttpReadData`, the context's `READ_BUFFER_SIZE`
                // bytes, of which WinHTTP filled `info_len`.
                let data = unsafe { std::slice::from_raw_parts(info as *const u8, len) };
                Event::Read(Bytes::copy_from_slice(data))
            }
        }
        WINHTTP_CALLBACK_STATUS_REQUEST_ERROR => {
            let code = if info.is_null() {
                ERROR_WINHTTP_INTERNAL_ERROR
            } else {
                // SAFETY: for this notification `info` points to a
                // WINHTTP_ASYNC_RESULT.
                unsafe { (*(info as *const WINHTTP_ASYNC_RESULT)).dwError }
            };
            Event::Failed(code)
        }
        _ => return,
    };
    let _ = context.events.send(event);
}

/// A request body. Either kind can be sent again from the start, which a
/// redirect that keeps the body and WinHTTP's demand to resend both need.
enum Upload {
    None,
    Memory(Bytes),
    File(FileBody),
}

impl Upload {
    fn len(&self) -> u64 {
        match self {
            Upload::None => 0,
            Upload::Memory(body) => body.len() as u64,
            Upload::File(file) => file.len(),
        }
    }

    /// Up to `UPLOAD_CHUNK_SIZE` bytes of the body starting at `offset`. A
    /// file is read on the blocking pool, so neither a runtime worker nor a
    /// WinHTTP callback thread waits for the disk.
    async fn chunk(&self, offset: u64) -> Result<Bytes> {
        match self {
            Upload::None => Ok(Bytes::new()),
            Upload::Memory(body) => {
                let start =
                    usize::try_from(offset).map_or(body.len(), |start| start.min(body.len()));
                let end = body.len().min(start.saturating_add(UPLOAD_CHUNK_SIZE));
                Ok(body.slice(start..end))
            }
            Upload::File(file) => {
                let file = file.clone();
                let chunk =
                    tokio::task::spawn_blocking(move || file.read_chunk(offset, UPLOAD_CHUNK_SIZE))
                        .await
                        .map_err(|error| {
                            Error::Internal(format!("Reading the file to upload failed: {}", error))
                        })??;
                Ok(chunk)
            }
        }
    }
}

/// One request on the wire. Dropping it closes the request handle, which
/// cancels whatever WinHTTP is doing for the request.
struct Exchange {
    // Declared first so it is dropped, and the handle closed, before this
    // reference to the context, which holds the connection handle.
    handle: RequestHandle,
    context: Arc<RequestContext>,
    events: mpsc::UnboundedReceiver<Event>,
}

impl Exchange {
    /// Open a request for `method url` on `connect` with its callback and
    /// options set. Nothing is sent yet.
    fn open(
        config: &RequestConfig<'_>,
        connect: &Arc<Handle>,
        method: &Method,
        url: &Url,
        decompress: bool,
        read_timeout: Option<Duration>,
    ) -> Result<Self> {
        let mut object = url.path().to_string();
        if let Some(query) = url.query() {
            object.push('?');
            object.push_str(query);
        }
        let flags = if url.scheme() == "https" {
            WINHTTP_FLAG_SECURE
        } else {
            WINHTTP_OPEN_REQUEST_FLAGS(0)
        };

        let (events_tx, events) = mpsc::unbounded_channel();
        let read_buffer = Box::into_raw(vec![0u8; READ_BUFFER_SIZE].into_boxed_slice()) as *mut u8;
        let context = Arc::new(RequestContext {
            events: events_tx,
            read_buffer,
            sending: Mutex::new(Bytes::new()),
            _connect: connect.clone(),
            _session: config.session.clone(),
        });

        // SAFETY: `connect` is an open connection handle, the strings are
        // NUL-terminated and outlive the call, and WinHTTP allows the
        // version, referrer and accept types to be null.
        let raw = unsafe {
            WinHttpOpenRequest(
                connect.0,
                &HSTRING::from(method.as_str()),
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
        // Declared after `context` so it is dropped, and the handle closed, first.
        let handle = RequestHandle(Handle(raw));

        let notifications = WINHTTP_CALLBACK_FLAG_SENDREQUEST_COMPLETE
            | WINHTTP_CALLBACK_FLAG_WRITE_COMPLETE
            | WINHTTP_CALLBACK_FLAG_HEADERS_AVAILABLE
            | WINHTTP_CALLBACK_FLAG_READ_COMPLETE
            | WINHTTP_CALLBACK_FLAG_REQUEST_ERROR
            | WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING;
        // SAFETY: the handle is open, and `status_callback` has the
        // signature WinHTTP calls and stays valid for the process's life.
        let previous = unsafe {
            WinHttpSetStatusCallback(handle.raw(), Some(status_callback), notifications, 0)
        };
        if previous.map(|callback| callback as usize) == Some(INVALID_STATUS_CALLBACK) {
            return Err(last_error("WinHttpSetStatusCallback"));
        }

        let context_value = Arc::into_raw(context.clone()) as usize;
        // SAFETY: the handle is open, and WinHTTP copies the pointer-sized
        // value out of the slice during the call.
        if let Err(error) = unsafe {
            WinHttpSetOption(
                Some(handle.raw()),
                WINHTTP_OPTION_CONTEXT_VALUE,
                Some(&context_value.to_ne_bytes()),
            )
        } {
            // WinHTTP did not take the reference, so HANDLE_CLOSING won't free it.
            // SAFETY: the pointer came from `Arc::into_raw` just above, and
            // nothing else took the reference it stands for.
            drop(unsafe { Arc::from_raw(context_value as *const RequestContext) });
            return Err(map_windows_error(error));
        }

        // Cookies come from frakt's own jar, and `execute` follows
        // redirects. Both flags go in one call: whether a second one would
        // add to the first or replace it is not documented.
        set_option_u32(
            handle.raw(),
            WINHTTP_OPTION_DISABLE_FEATURE,
            WINHTTP_DISABLE_COOKIES | WINHTTP_DISABLE_REDIRECTS,
        )?;
        if decompress {
            // Fails before Windows 8.1, which then gets bodies as sent.
            let _ = set_option_u32(
                handle.raw(),
                WINHTTP_OPTION_DECOMPRESSION,
                WINHTTP_DECOMPRESSION_FLAG_GZIP | WINHTTP_DECOMPRESSION_FLAG_DEFLATE,
            );
        }
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
        // meaning. Reads are only issued when the consumer has room, so a
        // slow consumer does not run the clock. The send timeout is the
        // read timeout too: a server that stops taking the request body
        // stalls the transfer just as one that stops sending does.
        let connect_ms = timeout_ms(config.connect_timeout);
        let read_ms = timeout_ms(read_timeout);
        // SAFETY: the handle is open.
        unsafe { WinHttpSetTimeouts(handle.raw(), connect_ms, connect_ms, read_ms, read_ms) }
            .map_err(map_windows_error)?;
        set_option_u32(
            handle.raw(),
            WINHTTP_OPTION_RECEIVE_RESPONSE_TIMEOUT,
            read_ms as u32,
        )?;

        Ok(Exchange {
            handle,
            context,
            events,
        })
    }

    /// Send the request and its body, and wait for the response head.
    async fn send(
        &mut self,
        headers: &[u16],
        upload: &Upload,
        total_len: u32,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        let mut resends = 0;
        loop {
            match self.send_once(headers, upload, total_len, progress).await {
                // WinHTTP answers an NTLM or Negotiate challenge itself but
                // cannot repeat a body it was given in chunks, so it asks
                // for the request again, on the same handle.
                Err(error) if must_resend(&error) && resends < MAX_RESENDS => resends += 1,
                result => return result,
            }
        }
    }

    async fn send_once(
        &mut self,
        headers: &[u16],
        upload: &Upload,
        total_len: u32,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        let len = upload.len();
        report(progress, 0, len);

        // A body in memory goes to WinHttpSendRequest whole when its length
        // fits the 32 bits that takes. Any other is written in chunks once
        // the request head has gone out.
        let whole = match upload {
            Upload::Memory(body) if !body.is_empty() => u32::try_from(body.len())
                .ok()
                .map(|len| (body.clone(), len)),
            _ => None,
        };
        let in_chunks = whole.is_none() && len > 0;
        self.send_request(headers, whole, total_len)?;
        match self.event().await? {
            Event::SendComplete => {}
            _ => return Err(unexpected("sending the request")),
        }
        if in_chunks {
            self.write_body(upload, len, progress).await?;
        } else {
            report(progress, len, len);
        }

        // SAFETY: the handle is open, and the reserved parameter is null as
        // WinHTTP requires.
        unsafe { WinHttpReceiveResponse(self.handle.raw(), std::ptr::null_mut()) }
            .map_err(map_windows_error)?;
        match self.event().await? {
            Event::HeadersAvailable => Ok(()),
            _ => Err(unexpected("waiting for the response")),
        }
    }

    /// Start sending the request head, and with it `whole`, a body and its
    /// length, if the body is not written in chunks.
    fn send_request(
        &self,
        headers: &[u16],
        whole: Option<(Bytes, u32)>,
        total_len: u32,
    ) -> Result<()> {
        let (optional, optional_len) = match whole {
            Some((body, len)) => (Some(self.context.hold(body)), len),
            None => (None, 0),
        };
        // SAFETY: the handle is open. `headers` outlives the call. `optional`
        // points to `optional_len` bytes that the context holds until the
        // handle has closed, and WinHTTP has finished with what the context
        // held before: no send or write is in progress. The context value
        // is the pointer WinHTTP already holds its reference through.
        unsafe {
            WinHttpSendRequest(
                self.handle.raw(),
                (!headers.is_empty()).then_some(headers),
                optional,
                optional_len,
                total_len,
                Arc::as_ptr(&self.context) as usize,
            )
        }
        .map_err(map_windows_error)
    }

    /// Write the body after the request head, one `WinHttpWriteData` at a
    /// time. A file's next chunk is only read once the one before it has
    /// been written, so one chunk is in memory.
    async fn write_body(
        &mut self,
        upload: &Upload,
        len: u64,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        let mut sent = 0;
        while sent < len {
            let mut chunk = upload.chunk(sent).await?;
            if chunk.is_empty() {
                return Err(Error::Internal(
                    "The request body ended before its length".to_string(),
                ));
            }
            while !chunk.is_empty() {
                self.write(chunk.clone())?;
                let written = match self.event().await? {
                    Event::Written(written) => (written as usize).min(chunk.len()),
                    _ => return Err(unexpected("writing the request body")),
                };
                if written == 0 {
                    return Err(Error::Internal(
                        "WinHTTP wrote none of the request body".to_string(),
                    ));
                }
                chunk = chunk.slice(written..);
                sent += written as u64;
                report(progress, sent, len);
            }
        }
        Ok(())
    }

    /// Start one `WinHttpWriteData` of `chunk`.
    fn write(&self, chunk: Bytes) -> Result<()> {
        let len = u32::try_from(chunk.len())
            .map_err(|_| Error::Internal("Request body chunk too large for WinHTTP".to_string()))?;
        let data = self.context.hold(chunk);
        // SAFETY: the handle is open. `data` points to `len` bytes that the
        // context holds until the handle has closed, and WinHTTP has
        // finished with what the context held before: the write before this
        // one has completed. The count is reported to the callback, so the
        // out pointer is null, as asynchronous use requires.
        unsafe { WinHttpWriteData(self.handle.raw(), Some(data), len, std::ptr::null_mut()) }
            .map_err(map_windows_error)
    }

    /// Issue one `WinHttpReadData` and wait for it; an empty chunk is the end.
    async fn read_chunk(&mut self) -> Result<Bytes> {
        // SAFETY: the handle is open. The buffer is `READ_BUFFER_SIZE` bytes
        // that the context keeps until the handle has closed, and no other
        // read is outstanding: the one before this was waited for. The
        // count is reported to the callback, so the out pointer is null, as
        // asynchronous use requires.
        unsafe {
            WinHttpReadData(
                self.handle.raw(),
                self.context.read_buffer as *mut c_void,
                READ_BUFFER_SIZE as u32,
                std::ptr::null_mut(),
            )
        }
        .map_err(map_windows_error)?;
        match self.event().await? {
            Event::Read(data) => Ok(data),
            _ => Err(unexpected("reading the response body")),
        }
    }

    /// The next notification, or the error of the operation that failed.
    async fn event(&mut self) -> Result<Event> {
        match self.events.recv().await {
            Some(Event::Failed(code)) => Err(map_winhttp_error(code)),
            Some(event) => Ok(event),
            None => Err(Error::Internal("WinHTTP notifications stopped".to_string())),
        }
    }
}

fn unexpected(doing: &str) -> Error {
    Error::Internal(format!("Unexpected WinHTTP notification while {}", doing))
}

fn must_resend(error: &Error) -> bool {
    matches!(
        error,
        Error::Network { code, .. } if *code == i64::from(ERROR_WINHTTP_RESEND_REQUEST)
    )
}

/// Tell the upload progress callback, if there is one and a body to report on.
fn report(progress: Option<&ProgressCallback>, sent: u64, len: u64) {
    if len > 0
        && let Some(progress) = progress
    {
        progress(sent, Some(len));
    }
}

/// The length to give `WinHttpSendRequest` for a body of `len` bytes. It
/// takes 32 bits; for a longer body WinHTTP reads the length from a
/// Content-Length header instead, which is returned to be sent.
fn declared_length(len: u64) -> (u32, Option<HeaderValue>) {
    match u32::try_from(len) {
        Ok(total) => (total, None),
        Err(_) => (
            WINHTTP_IGNORE_REQUEST_TOTAL_LENGTH,
            Some(HeaderValue::from(len)),
        ),
    }
}

/// A connection handle for `url`'s host and port. WinHTTP connects when a
/// request is sent on it, not here.
fn open_connection(session: &Session, url: &Url) -> Result<Arc<Handle>> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Error::InvalidUrl);
    }
    let host = match url.host() {
        Some(url::Host::Ipv6(address)) => address.to_string(),
        Some(host) => host.to_string(),
        None => return Err(Error::InvalidUrl),
    };
    let port = url.port_or_known_default().ok_or(Error::InvalidUrl)?;
    // SAFETY: the session handle is open for as long as `session` lives, and
    // the host name is NUL-terminated and outlives the call.
    let handle = Handle(unsafe { WinHttpConnect(session.handle.0, &HSTRING::from(host), port, 0) });
    if handle.0.is_null() {
        return Err(last_error("WinHttpConnect"));
    }
    Ok(Arc::new(handle))
}

/// Execute an HTTP request with WinHTTP, following redirects, and return once
/// the headers of the response to deliver have arrived. The body is read as
/// the returned stream is consumed.
pub(crate) async fn execute(
    config: RequestConfig<'_>,
    request: BackendRequest,
) -> Result<BackendResponse> {
    let (mut upload, content_type) = encode_body(request.body)?;
    // Merged once: what a redirect to another origin strips, defaults
    // included, must not come back on a later hop.
    let mut headers = merge_headers(config.default_headers, &request.headers, content_type);
    let read_timeout = request.read_timeout.or(config.read_timeout);
    let progress = request.progress_callback;
    let original_url = request.url.clone();
    let mut method = request.method;
    let mut url = request.url;
    let mut connect = open_connection(config.session, &url)?;
    let mut redirects = 0;

    let (mut exchange, status, response_headers, decompress) = loop {
        // WinHTTP adds its own codings to any Accept-Encoding the caller
        // sent when it decompresses (an explicit identity went out as
        // "identity, gzip, deflate"), so it only decompresses requests that
        // left the choice to frakt.
        let decompress = !headers.contains_key(http::header::ACCEPT_ENCODING);

        // The jar's cookies go on a copy: through `headers` they would
        // reach the next hop whatever its URL.
        let mut hop_headers = headers.clone();
        if let Some(jar) = config.cookie_jar {
            jar.add_cookie_header(&url, &mut hop_headers);
        }
        let (total_len, content_length) = declared_length(upload.len());
        if let Some(content_length) = content_length {
            hop_headers.insert(http::header::CONTENT_LENGTH, content_length);
        }
        let block = header_block(&hop_headers);

        let mut exchange =
            Exchange::open(&config, &connect, &method, &url, decompress, read_timeout)?;
        exchange
            .send(&block, &upload, total_len, progress.as_ref())
            .await?;
        let status = query_status(exchange.handle.raw())?;
        let response_headers = query_response_headers(exchange.handle.raw())?;

        // Stored before the next hop's cookies are computed, so a cookie
        // this response set is sent with the next request it applies to.
        if let Some(jar) = config.cookie_jar {
            jar.store_response_cookies(&original_url, &url, &response_headers);
        }

        let Some(hop) = redirect::next_hop(&method, &url, &headers, status, &response_headers)
        else {
            break (exchange, status, response_headers, decompress);
        };
        if redirects == redirect::MAX_REDIRECTS {
            return Err(redirect::too_many_redirects());
        }
        redirects += 1;

        // Closes the request handle, and with it whatever body the redirect
        // came with, before the next request is opened.
        drop(exchange);
        if !redirect::same_origin(&url, &hop.url) {
            connect = open_connection(config.session, &hop.url)?;
        }
        if !hop.keep_body {
            upload = Upload::None;
        }
        method = hop.method;
        url = hop.url;
        headers = hop.headers;
    };

    let has_body = !(method == Method::HEAD
        || status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED);
    let content_length = crate::backend::types::content_length(&response_headers);

    // WinHTTP removes Content-Encoding and Content-Length from a body it
    // decompresses, so only its counters tell: WinHttpResponseBodyCompressedSize
    // grows once it has decoded compressed bytes. A body that kept its
    // Content-Length was not decoded; for any other, read the first chunk to
    // find out.
    let mut first = None;
    let decoded = if decompress && has_body && content_length.is_none() {
        let chunk = exchange.read_chunk().await;
        let decoded =
            chunk.is_ok() && body_stats(exchange.handle.raw()).is_some_and(|stats| stats.wire > 0);
        first = Some(chunk);
        decoded
    } else {
        false
    };

    let accounting = BodyAccounting::new(decoded, content_length);
    if decoded {
        // The stripped headers hide the framing, but WinHTTP checks it and
        // fails a cut compressed body (12152) with either framing.
        accounting.set_integrity(BodyIntegrity::Checked);
    }
    let (tx, rx) = mpsc::channel(BODY_CHANNEL_CHUNKS);
    tokio::spawn(read_body(
        BodyReader {
            exchange,
            accounting: accounting.clone(),
            decoded,
            expected_len: content_length.filter(|_| has_body),
            received: 0,
        },
        first,
        tx,
    ));

    Ok(BackendResponse {
        status,
        accounting,
        headers: response_headers,
        url,
        body: channel_body(rx),
    })
}

/// Body bytes so far, from WINHTTP_OPTION_REQUEST_STATS (Windows 10 1903
/// and later).
struct BodyCounts {
    /// Bytes received over the network; 0 unless WinHTTP decompressed.
    wire: u64,
}

fn body_stats(handle: *mut c_void) -> Option<BodyCounts> {
    let mut stats = WINHTTP_REQUEST_STATS {
        // An input: how many entries to fill.
        cStats: WinHttpRequestStatLast.0 as u32,
        ..Default::default()
    };
    let mut size = std::mem::size_of::<WINHTTP_REQUEST_STATS>() as u32;
    unsafe {
        WinHttpQueryOption(
            handle,
            WINHTTP_OPTION_REQUEST_STATS,
            Some(&mut stats as *mut WINHTTP_REQUEST_STATS as *mut c_void),
            &mut size,
        )
    }
    .ok()?;
    Some(BodyCounts {
        wire: stats.rgullStats[WinHttpResponseBodyCompressedSize.0 as usize],
    })
}

/// What reading a body needs; dropping it closes the request handle.
struct BodyReader {
    exchange: Exchange,
    accounting: Arc<BodyAccounting>,
    decoded: bool,
    /// The Content-Length of a body delivered as sent.
    expected_len: Option<u64>,
    received: u64,
}

impl BodyReader {
    /// Account for a chunk, or for the end of the body if `chunk` is empty;
    /// the error for a body that ended short.
    fn record(&mut self, chunk: &Bytes) -> Option<Error> {
        self.received += chunk.len() as u64;
        if self.decoded {
            if let Some(stats) = body_stats(self.exchange.handle.raw()) {
                self.accounting.set_wire_bytes(stats.wire);
            }
        } else {
            self.accounting.set_wire_bytes(self.received);
        }
        if !chunk.is_empty() {
            return None;
        }
        let expected = self.expected_len?;
        (self.received < expected).then(|| Error::Network {
            code: i64::from(ERROR_WINHTTP_CONNECTION_ERROR),
            message: format!(
                "connection closed after {} of {} body bytes",
                self.received, expected
            ),
        })
    }
}

/// Read the body into `tx`, one `WinHttpReadData` per reserved channel slot,
/// starting with `first` if the first chunk was already read.
async fn read_body(
    mut reader: BodyReader,
    mut first: Option<Result<Bytes>>,
    tx: mpsc::Sender<Result<Bytes>>,
) {
    loop {
        let Ok(permit) = tx.reserve().await else {
            return;
        };

        let chunk = match first.take() {
            Some(chunk) => chunk,
            None => tokio::select! {
                chunk = reader.exchange.read_chunk() => chunk,
                // The consumer went away mid-read; returning closes the
                // handle, which cancels the read.
                _ = tx.closed() => return,
            },
        };

        match chunk {
            Ok(data) => {
                let end = data.is_empty();
                if let Some(error) = reader.record(&data) {
                    permit.send(Err(error));
                    return;
                }
                if end {
                    return;
                }
                permit.send(Ok(data));
            }
            Err(error) => {
                permit.send(Err(error));
                return;
            }
        }
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

/// The body to send and the content type it implies. A body already in
/// memory is shared, not copied.
fn encode_body(body: Option<Body>) -> Result<(Upload, Option<String>)> {
    Ok(match body {
        None | Some(Body::Empty) => (Upload::None, None),
        Some(Body::Bytes {
            content,
            content_type,
        }) => (Upload::Memory(content), Some(content_type)),
        Some(Body::Form { fields }) => {
            let encoded = fields
                .iter()
                .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            (
                Upload::Memory(encoded.into()),
                Some("application/x-www-form-urlencoded".to_string()),
            )
        }
        Some(Body::Json { value }) => (
            Upload::Memory(
                serde_json::to_vec(&value)
                    .map_err(|e| Error::Json(e.to_string()))?
                    .into(),
            ),
            Some("application/json".to_string()),
        ),
        Some(Body::File { file, content_type }) => (Upload::File(file), Some(content_type)),
        Some(Body::Multipart { .. }) => {
            return Err(Error::Internal(
                "Multipart not yet supported with WinHTTP".to_string(),
            ));
        }
    })
}

/// What the caller asked to send: the request's headers over the client's
/// defaults, with the body's content type unless a header already names one.
fn merge_headers(
    default_headers: Option<&HeaderMap>,
    headers: &HeaderMap,
    content_type: Option<String>,
) -> HeaderMap {
    let mut merged = default_headers.cloned().unwrap_or_default();
    for name in headers.keys() {
        merged.remove(name);
    }
    for (name, value) in headers {
        merged.append(name, value.clone());
    }
    if let Some(content_type) = content_type
        && !merged.contains_key(http::header::CONTENT_TYPE)
        && let Ok(value) = HeaderValue::from_str(&content_type)
    {
        merged.insert(http::header::CONTENT_TYPE, value);
    }
    merged
}

/// Request headers as the CRLF-separated UTF-16 block WinHTTP takes.
fn header_block(headers: &HeaderMap) -> Vec<u16> {
    let mut block = Vec::new();
    for (name, value) in headers {
        block.extend(name.as_str().encode_utf16());
        block.extend(": ".encode_utf16());
        block.extend(widen_header_text(value.as_bytes()));
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
        let value = narrow_header_text(&line[colon + 1..]);
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(name.trim().as_bytes()),
            HeaderValue::from_bytes(value.trim_ascii()),
        ) {
            headers.append(name, value);
        }
    }
    Ok(headers)
}

// WinHTTP converts header text between bytes and UTF-16 with the ANSI code
// page. Converting with the same code page in the other direction keeps the
// bytes the caller set or the server sent; bytes the code page cannot
// represent cannot survive WinHTTP either way.

fn widen_header_text(bytes: &[u8]) -> Vec<u16> {
    if bytes.is_ascii() {
        return bytes.iter().map(|&byte| u16::from(byte)).collect();
    }
    let flags = MULTI_BYTE_TO_WIDE_CHAR_FLAGS(0);
    let len = unsafe { MultiByteToWideChar(CP_ACP, flags, bytes, None) };
    if len > 0 {
        let mut wide = vec![0u16; len as usize];
        let written = unsafe { MultiByteToWideChar(CP_ACP, flags, bytes, Some(&mut wide)) };
        if written > 0 {
            wide.truncate(written as usize);
            return wide;
        }
    }
    bytes.iter().map(|&byte| u16::from(byte)).collect()
}

fn narrow_header_text(units: &[u16]) -> Vec<u8> {
    if units.iter().all(|&unit| unit < 0x80) {
        return units.iter().map(|&unit| unit as u8).collect();
    }
    let len = unsafe { WideCharToMultiByte(CP_ACP, 0, units, None, PCSTR::null(), None) };
    if len > 0 {
        let mut bytes = vec![0u8; len as usize];
        let written =
            unsafe { WideCharToMultiByte(CP_ACP, 0, units, Some(&mut bytes), PCSTR::null(), None) };
        if written > 0 {
            bytes.truncate(written as usize);
            return bytes;
        }
    }
    String::from_utf16_lossy(units).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_length_past_32_bits_goes_in_a_header() {
        assert_eq!(declared_length(0), (0, None));
        assert_eq!(declared_length(u64::from(u32::MAX)), (u32::MAX, None));

        let (total, header) = declared_length(u64::from(u32::MAX) + 1);
        assert_eq!(total, WINHTTP_IGNORE_REQUEST_TOTAL_LENGTH);
        assert_eq!(header.unwrap(), "4294967296");
    }

    #[test]
    fn request_headers_replace_defaults_of_the_same_name() {
        let mut defaults = HeaderMap::new();
        defaults.insert("x-api-key", HeaderValue::from_static("default"));
        defaults.insert("accept", HeaderValue::from_static("*/*"));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("request"));

        let merged = merge_headers(Some(&defaults), &headers, Some("text/plain".to_string()));
        let keys: Vec<_> = merged.get_all("x-api-key").iter().collect();
        assert_eq!(keys, ["request"]);
        assert_eq!(merged["accept"], "*/*");
        assert_eq!(merged["content-type"], "text/plain");

        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-own"),
        );
        let merged = merge_headers(None, &headers, Some("text/plain".to_string()));
        assert_eq!(merged["content-type"], "application/x-own");
    }

    #[test]
    fn header_block_is_crlf_terminated_lines() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("*/*"));
        headers.insert("x-n", HeaderValue::from_static("1"));
        assert_eq!(
            String::from_utf16(&header_block(&headers)).unwrap(),
            "accept: */*\r\nx-n: 1\r\n"
        );
        assert!(header_block(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn a_body_in_memory_is_not_copied() {
        let content = Bytes::from(vec![1u8; 32]);
        let (upload, content_type) =
            encode_body(Some(Body::bytes(content.clone(), "application/x-test"))).unwrap();
        assert_eq!(content_type.as_deref(), Some("application/x-test"));
        let Upload::Memory(body) = upload else {
            panic!("not a body in memory");
        };
        assert_eq!(body.as_ptr(), content.as_ptr());
    }

    #[tokio::test]
    async fn memory_chunks_cover_the_body_without_copying() {
        let body = Bytes::from(vec![7u8; UPLOAD_CHUNK_SIZE + 10]);
        let upload = Upload::Memory(body.clone());
        assert_eq!(upload.len(), body.len() as u64);

        let first = upload.chunk(0).await.unwrap();
        assert_eq!(first.len(), UPLOAD_CHUNK_SIZE);
        assert_eq!(first.as_ptr(), body.as_ptr());
        let rest = upload.chunk(first.len() as u64).await.unwrap();
        assert_eq!(rest.len(), 10);
        assert!(upload.chunk(body.len() as u64).await.unwrap().is_empty());
        assert!(upload.chunk(u64::MAX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn file_chunks_stop_at_the_length_and_fail_if_the_file_shrank() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload.bin");
        std::fs::write(&path, vec![9u8; UPLOAD_CHUNK_SIZE + 10]).unwrap();
        let upload = Upload::File(FileBody::open(&path).unwrap());
        assert_eq!(upload.len(), (UPLOAD_CHUNK_SIZE + 10) as u64);

        assert_eq!(upload.chunk(0).await.unwrap().len(), UPLOAD_CHUNK_SIZE);
        let rest = upload.chunk(UPLOAD_CHUNK_SIZE as u64).await.unwrap();
        assert_eq!(rest.as_ref(), [9u8; 10]);

        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(5)
            .unwrap();
        assert!(matches!(upload.chunk(0).await, Err(Error::Io(_))));
    }

    /// `execute` re-sends on this error by recognising how `error.rs` maps it.
    #[test]
    fn only_the_resend_error_is_resent() {
        assert!(must_resend(&map_winhttp_error(
            ERROR_WINHTTP_RESEND_REQUEST
        )));
        assert!(!must_resend(&map_winhttp_error(
            ERROR_WINHTTP_CONNECTION_ERROR
        )));
        assert!(!must_resend(&map_winhttp_error(ERROR_WINHTTP_TIMEOUT)));
    }

    /// Requests are spawned onto multi-threaded runtimes, so nothing the
    /// future holds across an await may be `!Send`: no raw handle or buffer
    /// pointer.
    #[test]
    fn the_request_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}

        let session = Arc::new(Session::open("frakt-test", None).unwrap());
        let request = BackendRequest {
            method: Method::GET,
            url: Url::parse("http://localhost/").unwrap(),
            headers: HeaderMap::new(),
            body: None,
            progress_callback: None,
            read_timeout: None,
        };
        let config = RequestConfig {
            session: &session,
            default_headers: None,
            cookie_jar: None,
            connect_timeout: None,
            read_timeout: None,
            ignore_certificate_errors: false,
        };
        assert_send(&execute(config, request));
    }
}
