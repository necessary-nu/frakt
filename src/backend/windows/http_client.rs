//! WinHTTP client utilities for Windows backend

use crate::backend::types::BackendResponse;
use crate::{Error, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use windows::{
    Win32::Foundation::{GetLastError, VARIANT_TRUE},
    Win32::Networking::WinHttp::{
        WINHTTP_ACCESS_TYPE_DEFAULT_PROXY, WINHTTP_ASYNC_RESULT,
        WINHTTP_CALLBACK_FLAG_ALL_NOTIFICATIONS, WINHTTP_CALLBACK_STATUS_CLOSING_CONNECTION,
        WINHTTP_CALLBACK_STATUS_CONNECTED_TO_SERVER, WINHTTP_CALLBACK_STATUS_CONNECTING_TO_SERVER,
        WINHTTP_CALLBACK_STATUS_CONNECTION_CLOSED, WINHTTP_CALLBACK_STATUS_DATA_AVAILABLE,
        WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING, WINHTTP_CALLBACK_STATUS_HANDLE_CREATED,
        WINHTTP_CALLBACK_STATUS_HEADERS_AVAILABLE, WINHTTP_CALLBACK_STATUS_INTERMEDIATE_RESPONSE,
        WINHTTP_CALLBACK_STATUS_NAME_RESOLVED, WINHTTP_CALLBACK_STATUS_READ_COMPLETE,
        WINHTTP_CALLBACK_STATUS_RECEIVING_RESPONSE, WINHTTP_CALLBACK_STATUS_REDIRECT,
        WINHTTP_CALLBACK_STATUS_REQUEST_ERROR, WINHTTP_CALLBACK_STATUS_REQUEST_SENT,
        WINHTTP_CALLBACK_STATUS_RESOLVING_NAME, WINHTTP_CALLBACK_STATUS_RESPONSE_RECEIVED,
        WINHTTP_CALLBACK_STATUS_SENDING_REQUEST, WINHTTP_CALLBACK_STATUS_SENDREQUEST_COMPLETE,
        WINHTTP_CALLBACK_STATUS_WRITE_COMPLETE, WINHTTP_DISABLE_COOKIES, WINHTTP_DISABLE_REDIRECTS,
        WINHTTP_FLAG_ASYNC, WINHTTP_FLAG_SECURE, WINHTTP_OPEN_REQUEST_FLAGS,
        WINHTTP_OPTION_CONNECT_TIMEOUT, WINHTTP_OPTION_CONTEXT_VALUE,
        WINHTTP_OPTION_DISABLE_FEATURE, WINHTTP_OPTION_RESOLVE_TIMEOUT,
        WINHTTP_OPTION_SEND_TIMEOUT, WINHTTP_QUERY_FLAG_NUMBER, WINHTTP_QUERY_RAW_HEADERS_CRLF,
        WINHTTP_QUERY_SET_COOKIE, WINHTTP_QUERY_STATUS_CODE, WinHttpCloseHandle, WinHttpConnect,
        WinHttpOpen, WinHttpOpenRequest, WinHttpQueryDataAvailable, WinHttpQueryHeaders,
        WinHttpReadData, WinHttpReceiveResponse, WinHttpSendRequest, WinHttpSetOption,
        WinHttpSetStatusCallback, WinHttpSetTimeouts,
    },
    core::{HSTRING, PCWSTR},
};

/// Request state for async WinHTTP operations
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum RequestState {
    Connecting = 0,
    Sending = 1,
    HeadersReceived = 2,
    ReadingData = 3,
    Complete = 4,
    Error = 5,
}

impl RequestState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Connecting,
            1 => Self::Sending,
            2 => Self::HeadersReceived,
            3 => Self::ReadingData,
            4 => Self::Complete,
            _ => Self::Error,
        }
    }
}

impl From<RequestState> for u8 {
    fn from(state: RequestState) -> u8 {
        state as u8
    }
}

/// Async completion result for WinHTTP operations
type AsyncResult = Result<()>;

/// Context data passed to WinHTTP callback for cookie handling, progress tracking, and async signaling
#[repr(C)]
struct CallbackContext {
    cookie_storage: Option<std::sync::Arc<crate::backend::CookieJar>>,
    request_url: url::Url,
    progress_callback: Option<Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
    total_body_size: u64,
    bytes_sent: Arc<std::sync::atomic::AtomicU64>,

    // Async state management
    state: Arc<std::sync::atomic::AtomicU8>, // RequestState as u8
    completion_sender: Arc<std::sync::Mutex<Option<oneshot::Sender<AsyncResult>>>>,
    error_storage: Arc<std::sync::Mutex<Option<Error>>>,

    // Data reading management
    response_data: Arc<std::sync::Mutex<Vec<u8>>>,
    reading_data: Arc<std::sync::atomic::AtomicBool>,
    request_handle: Arc<std::sync::Mutex<Option<*mut std::ffi::c_void>>>,
    read_buffer: Arc<std::sync::Mutex<Vec<u8>>>, // Buffer for current read operation
    current_read_size: Arc<std::sync::atomic::AtomicU32>, // Size of current read operation
}

/// WinHTTP async callback function to handle state transitions and progress tracking
unsafe extern "system" fn winhttp_callback(
    h_internet: *mut std::ffi::c_void,
    dw_context: usize,
    dw_internet_status: u32,
    lpv_status_information: *mut std::ffi::c_void,
    dw_status_information_length: u32,
) {
    if dw_context == 0 {
        return;
    }

    // Reconstruct Arc from raw pointer, but don't drop it
    let context = unsafe {
        let arc_ptr = dw_context as *const CallbackContext;
        let context_arc = Arc::from_raw(arc_ptr);
        let context_ref = Arc::clone(&context_arc);
        std::mem::forget(context_arc); // Don't drop the original Arc
        context_ref
    };
    let current_state =
        RequestState::from_u8(context.state.load(std::sync::atomic::Ordering::Acquire));

    // Handle async completion states
    match dw_internet_status {
        // Informational status updates (don't trigger completion)
        WINHTTP_CALLBACK_STATUS_RESOLVING_NAME => {}
        WINHTTP_CALLBACK_STATUS_NAME_RESOLVED => {}
        WINHTTP_CALLBACK_STATUS_CONNECTING_TO_SERVER => {}
        WINHTTP_CALLBACK_STATUS_CONNECTED_TO_SERVER => {}
        WINHTTP_CALLBACK_STATUS_SENDING_REQUEST => {
            // Trigger progress callback for start of upload
            if let Some(ref progress_callback) = context.progress_callback {
                progress_callback(0, Some(context.total_body_size));
            }
        }
        WINHTTP_CALLBACK_STATUS_REQUEST_SENT => {}
        WINHTTP_CALLBACK_STATUS_RECEIVING_RESPONSE => {}
        WINHTTP_CALLBACK_STATUS_RESPONSE_RECEIVED => {
            // Final progress update for upload completion
            if let Some(ref progress_callback) = context.progress_callback {
                progress_callback(context.total_body_size, Some(context.total_body_size));
            }
        }

        // Completion status updates (trigger completion)
        WINHTTP_CALLBACK_STATUS_SENDREQUEST_COMPLETE => {
            context.state.store(
                RequestState::Sending.into(),
                std::sync::atomic::Ordering::Release,
            );

            // Signal completion of send phase
            if let Ok(mut sender_lock) = context.completion_sender.lock() {
                if let Some(sender) = sender_lock.take() {
                    let _ = sender.send(Ok(()));
                }
            }
        }

        WINHTTP_CALLBACK_STATUS_HEADERS_AVAILABLE => {
            context.state.store(
                RequestState::HeadersReceived.into(),
                std::sync::atomic::Ordering::Release,
            );

            // Process cookies if available
            if let Some(ref cookie_storage) = context.cookie_storage {
                // Query headers for Set-Cookie
                let mut header_buffer_size: u32 = 0;
                let _ = unsafe {
                    WinHttpQueryHeaders(
                        h_internet,
                        WINHTTP_QUERY_RAW_HEADERS_CRLF,
                        PCWSTR::null(),
                        None,
                        &mut header_buffer_size,
                        std::ptr::null_mut(),
                    )
                };

                if header_buffer_size > 0 {
                    let mut header_buffer = vec![0u16; (header_buffer_size / 2) as usize];
                    let result = unsafe {
                        WinHttpQueryHeaders(
                            h_internet,
                            WINHTTP_QUERY_RAW_HEADERS_CRLF,
                            PCWSTR::null(),
                            Some(header_buffer.as_mut_ptr() as *mut _),
                            &mut header_buffer_size,
                            std::ptr::null_mut(),
                        )
                    };

                    if result.is_ok() {
                        let header_string = String::from_utf16_lossy(&header_buffer);
                        store_cookies_from_headers(
                            &header_string,
                            &context.request_url,
                            cookie_storage,
                        );
                    }
                }
            }

            // Signal headers received
            if let Ok(mut sender_lock) = context.completion_sender.lock() {
                if let Some(sender) = sender_lock.take() {
                    let _ = sender.send(Ok(()));
                }
            }
        }

        WINHTTP_CALLBACK_STATUS_DATA_AVAILABLE => {
            context.state.store(
                RequestState::ReadingData.into(),
                std::sync::atomic::Ordering::Release,
            );

            // If we have data available, trigger a read
            if dw_status_information_length > 0 {
                // Copy request handle and release lock before calling WinHTTP API
                let request_handle = {
                    if let Ok(handle_lock) = context.request_handle.lock() {
                        *handle_lock
                    } else {
                        None
                    }
                };

                if let Some(request_handle) = request_handle {
                    context
                        .reading_data
                        .store(true, std::sync::atomic::Ordering::Release);

                    // Use a fixed 8KB buffer size to match WinHTTP internal buffer
                    let buffer_size = 8192;

                    // Always try to read with full buffer size for efficiency
                    // WinHTTP will read as much as actually available up to this size
                    let read_size = buffer_size as u32;

                    // Store the expected read size for the callback
                    context
                        .current_read_size
                        .store(read_size, std::sync::atomic::Ordering::Release);

                    // Allocate and store buffer in context, then get a pointer to it
                    let buffer_ptr = {
                        if let Ok(mut context_buffer) = context.read_buffer.lock() {
                            *context_buffer = vec![0u8; read_size as usize];
                            context_buffer.as_mut_ptr() as *mut _
                        } else {
                            std::ptr::null_mut()
                        }
                    };

                    // Call WinHTTP API with the persistent buffer pointer
                    let result = unsafe {
                        windows::Win32::Networking::WinHttp::WinHttpReadData(
                            request_handle,
                            buffer_ptr,
                            read_size,
                            std::ptr::null_mut(), // NULL for async mode - get result in callback
                        )
                    };

                    if result.is_err() {
                        context
                            .reading_data
                            .store(false, std::sync::atomic::Ordering::Release);
                        // Signal error
                        if let Ok(mut sender_lock) = context.completion_sender.lock() {
                            if let Some(sender) = sender_lock.take() {
                                let _ = sender
                                    .send(Err(Error::Internal("Failed to read data".to_string())));
                            }
                        }
                    }
                }
            } else {
                // No more data available - signal completion
                if let Ok(mut sender_lock) = context.completion_sender.lock() {
                    if let Some(sender) = sender_lock.take() {
                        let _ = sender.send(Ok(()));
                    }
                }
            }
        }

        WINHTTP_CALLBACK_STATUS_READ_COMPLETE => {
            context
                .reading_data
                .store(false, std::sync::atomic::Ordering::Release);

            // Access the buffer we provided to WinHttpReadData and copy the read data
            if dw_status_information_length > 0 {
                if let Ok(context_buffer) = context.read_buffer.lock() {
                    if context_buffer.len() >= dw_status_information_length as usize {
                        let data_slice = &context_buffer[..dw_status_information_length as usize];
                        if let Ok(mut response_data) = context.response_data.lock() {
                            response_data.extend_from_slice(data_slice);
                        }
                    }
                }
            } else {
                // When we read 0 bytes, it means no data is available, signal completion
                if let Ok(mut sender_lock) = context.completion_sender.lock() {
                    if let Some(sender) = sender_lock.take() {
                        let _ = sender.send(Ok(()));
                    }
                }
                return; // Exit early, don't query for more data
            }

            // Always query for more data after a read, regardless of bytes read
            // WinHTTP will tell us via DATA_AVAILABLE if there's more data or not

            // Copy request handle and release lock before calling WinHTTP API
            let request_handle = {
                if let Ok(handle_lock) = context.request_handle.lock() {
                    *handle_lock
                } else {
                    None
                }
            };

            if let Some(request_handle) = request_handle {
                // Call WinHTTP API outside of any locks to prevent deadlock
                let result = unsafe {
                    windows::Win32::Networking::WinHttp::WinHttpQueryDataAvailable(
                        request_handle,
                        std::ptr::null_mut(), // NULL for async mode - get result in callback
                    )
                };

                match result {
                    Ok(_) => {} // Success - callback will handle DATA_AVAILABLE
                    Err(e) => {
                        // Signal completion on query error
                        if let Ok(mut sender_lock) = context.completion_sender.lock() {
                            if let Some(sender) = sender_lock.take() {
                                let _ = sender.send(Ok(()));
                            }
                        }
                    }
                }
            } else {
                // No handle available - signal completion
                if let Ok(mut sender_lock) = context.completion_sender.lock() {
                    if let Some(sender) = sender_lock.take() {
                        let _ = sender.send(Ok(()));
                    }
                }
            }
        }

        WINHTTP_CALLBACK_STATUS_WRITE_COMPLETE => {
            // Track upload progress
            let bytes_sent = context.bytes_sent.fetch_add(
                dw_status_information_length as u64,
                std::sync::atomic::Ordering::Relaxed,
            ) + dw_status_information_length as u64;

            if let Some(ref progress_callback) = context.progress_callback {
                progress_callback(bytes_sent, Some(context.total_body_size));
            }
        }

        WINHTTP_CALLBACK_STATUS_REQUEST_ERROR => {
            context.state.store(
                RequestState::Error.into(),
                std::sync::atomic::Ordering::Release,
            );

            // Extract error information and map to appropriate error type
            let error = if !lpv_status_information.is_null()
                && dw_status_information_length
                    >= std::mem::size_of::<WINHTTP_ASYNC_RESULT>() as u32
            {
                let async_result =
                    unsafe { &*(lpv_status_information as *const WINHTTP_ASYNC_RESULT) };

                // Map specific error codes to appropriate error types
                match async_result.dwError {
                    12002 => Error::Timeout, // ERROR_WINHTTP_TIMEOUT
                    _ => Error::Internal(format!(
                        "WinHTTP async error: dwResult={}, dwError={}",
                        async_result.dwResult, async_result.dwError
                    )),
                }
            } else {
                Error::Internal("Unknown WinHTTP async error".to_string())
            };

            // Store error for inspection
            if let Ok(mut error_lock) = context.error_storage.lock() {
                *error_lock = Some(match &error {
                    Error::Timeout => Error::Timeout,
                    Error::Internal(msg) => Error::Internal(msg.clone()),
                    _ => Error::Internal("Request failed".to_string()),
                });
            }

            // Signal error completion with the specific error type
            if let Ok(mut sender_lock) = context.completion_sender.lock() {
                if let Some(sender) = sender_lock.take() {
                    let _ = sender.send(Err(error));
                }
            }
        }

        // Connection status updates - no action needed
        WINHTTP_CALLBACK_STATUS_CLOSING_CONNECTION
        | WINHTTP_CALLBACK_STATUS_CONNECTION_CLOSED
        | WINHTTP_CALLBACK_STATUS_HANDLE_CREATED
        | WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING
        | WINHTTP_CALLBACK_STATUS_REDIRECT
        | WINHTTP_CALLBACK_STATUS_INTERMEDIATE_RESPONSE
        | _ => {}
    }
}

/// Execute an HTTP request using WinHTTP in async mode
pub async fn execute_winhttp_request(
    request: crate::backend::types::BackendRequest,
    user_agent: &str,
    default_headers: &Option<http::HeaderMap>,
    timeout: &Option<Duration>,
    cookie_storage: &Option<std::sync::Arc<crate::backend::CookieJar>>,
) -> Result<BackendResponse> {
    let url = request.url;
    let method = request.method;
    let headers = request.headers;
    let body = request.body;
    let progress_callback = request.progress_callback;

    // Use request timeout if specified, otherwise fall back to backend timeout
    let effective_timeout = request.timeout.as_ref().or(timeout.as_ref());

    // Parse URL components
    let scheme = url.scheme();
    let host = url.host_str().ok_or(Error::InvalidUrl)?;
    let port = url
        .port()
        .unwrap_or(if scheme == "https" { 443 } else { 80 });
    let path = url.path();
    let query = url.query().map(|q| format!("?{}", q)).unwrap_or_default();
    let full_path = format!("{}{}", path, query);

    // Prepare body data and determine content type
    let (body_bytes, body_content_type) = if let Some(body_data) = body {
        match body_data {
            crate::body::Body::Empty => (Vec::new(), None),
            crate::body::Body::Bytes { content, .. } => (content.to_vec(), None),
            crate::body::Body::Form { fields } => {
                let encoded = fields
                    .iter()
                    .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                    .collect::<Vec<_>>()
                    .join("&");
                (
                    encoded.into_bytes(),
                    Some("application/x-www-form-urlencoded"),
                )
            }
            crate::body::Body::Json { value } => {
                let json_bytes = serde_json::to_string(&value)
                    .map_err(|e| Error::Internal(format!("Failed to serialize JSON: {}", e)))?
                    .into_bytes();
                (json_bytes, Some("application/json"))
            }
            crate::body::Body::Multipart { .. } => {
                return Err(Error::Internal(
                    "Multipart not yet supported with WinHTTP".to_string(),
                ));
            }
        }
    } else {
        (Vec::new(), None)
    };

    // Execute the async WinHTTP request
    execute_async_winhttp(
        method,
        &url,
        scheme,
        host,
        port,
        &full_path,
        user_agent,
        &headers,
        default_headers,
        &body_bytes,
        body_content_type,
        effective_timeout,
        cookie_storage,
        &progress_callback,
    )
    .await
}

/// Execute async WinHTTP request with proper state machine
async fn execute_async_winhttp(
    method: http::Method,
    url: &url::Url,
    scheme: &str,
    host: &str,
    port: u16,
    full_path: &str,
    user_agent: &str,
    headers: &http::HeaderMap,
    default_headers: &Option<http::HeaderMap>,
    body_bytes: &[u8],
    body_content_type: Option<&str>,
    timeout: Option<&Duration>,
    cookie_storage: &Option<std::sync::Arc<crate::backend::CookieJar>>,
    progress_callback: &Option<Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
) -> Result<BackendResponse> {
    // Build headers string
    let mut header_string = String::new();

    // Add default headers
    if let Some(default_headers) = default_headers {
        for (name, value) in default_headers {
            header_string.push_str(&format!(
                "{}: {}\r\n",
                name.as_str(),
                value.to_str().unwrap_or("")
            ));
        }
    }

    // Add request headers
    for (name, value) in headers {
        header_string.push_str(&format!(
            "{}: {}\r\n",
            name.as_str(),
            value.to_str().unwrap_or("")
        ));
    }

    // Add Content-Type for body if provided by body type
    if let Some(content_type) = body_content_type {
        if !header_string.contains("Content-Type:") {
            header_string.push_str(&format!("Content-Type: {}\r\n", content_type));
        }
    }

    // Add cookies if available
    if let Some(cookie_storage) = cookie_storage {
        if let Ok(cookie_headers) = cookie_storage.get_cookies_for_url(url) {
            for (name, value) in &cookie_headers {
                header_string.push_str(&format!(
                    "{}: {}\r\n",
                    name.as_str(),
                    value.to_str().unwrap_or("")
                ));
            }
        }
    }

    // Execute the async WinHTTP request
    unsafe {
        // Open WinHTTP session with async flag
        let user_agent_wide = HSTRING::from(user_agent);
        let session = WinHttpOpen(
            &user_agent_wide,
            WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
            PCWSTR::null(),
            PCWSTR::null(),
            WINHTTP_FLAG_ASYNC, // Enable async mode
        );

        if session.is_null() {
            return Err(Error::Internal(format!(
                "Failed to open WinHTTP session: {}",
                GetLastError().0
            )));
        }

        // Set timeout if configured
        if let Some(timeout_duration) = timeout {
            let timeout_ms = timeout_duration.as_millis() as i32;
            WinHttpSetTimeouts(
                session, timeout_ms, // Resolve timeout
                timeout_ms, // Connect timeout
                timeout_ms, // Send timeout
                timeout_ms, // Receive timeout
            )
            .map_err(|e| {
                let _ = WinHttpCloseHandle(session);
                Error::Internal(format!("Failed to set timeouts: {}", e))
            })?;
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

        // Open request
        let method_wide = HSTRING::from(method.as_str());
        let path_wide = HSTRING::from(full_path);
        let mut flags = WINHTTP_OPEN_REQUEST_FLAGS(0);
        if scheme == "https" {
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

        // Set up async callback context
        let (send_completion_tx, send_completion_rx) = oneshot::channel();
        let (headers_completion_tx, headers_completion_rx) = oneshot::channel();
        let (data_completion_tx, data_completion_rx) = oneshot::channel();

        let callback_context = Arc::new(CallbackContext {
            cookie_storage: cookie_storage.clone(),
            request_url: url.clone(),
            progress_callback: progress_callback.clone(),
            total_body_size: body_bytes.len() as u64,
            bytes_sent: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            state: Arc::new(std::sync::atomic::AtomicU8::new(
                RequestState::Connecting.into(),
            )),
            completion_sender: Arc::new(std::sync::Mutex::new(Some(send_completion_tx))),
            error_storage: Arc::new(std::sync::Mutex::new(None)),
            response_data: Arc::new(std::sync::Mutex::new(Vec::new())),
            reading_data: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            request_handle: Arc::new(std::sync::Mutex::new(Some(request_handle))),
            read_buffer: Arc::new(std::sync::Mutex::new(Vec::new())),
            current_read_size: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        });
        let callback_ctx_ptr = Arc::into_raw(callback_context.clone());

        // Set callback
        let callback_flags = WINHTTP_CALLBACK_FLAG_ALL_NOTIFICATIONS;
        let _ = WinHttpSetStatusCallback(request_handle, Some(winhttp_callback), callback_flags, 0);

        // Set callback context
        WinHttpSetOption(
            Some(request_handle),
            WINHTTP_OPTION_CONTEXT_VALUE,
            Some(&(callback_ctx_ptr as usize).to_ne_bytes()),
        )
        .map_err(|e| {
            let _ = WinHttpCloseHandle(request_handle);
            let _ = WinHttpCloseHandle(connection);
            let _ = WinHttpCloseHandle(session);
            drop(Arc::from_raw(callback_ctx_ptr));
            Error::Internal(format!("Failed to set callback context: {}", e))
        })?;

        // Set other options
        WinHttpSetOption(
            Some(request_handle),
            WINHTTP_OPTION_DISABLE_FEATURE,
            Some(&(WINHTTP_DISABLE_COOKIES | WINHTTP_DISABLE_REDIRECTS).to_ne_bytes()),
        )
        .map_err(|e| {
            let _ = WinHttpCloseHandle(request_handle);
            let _ = WinHttpCloseHandle(connection);
            let _ = WinHttpCloseHandle(session);
            drop(Arc::from_raw(callback_ctx_ptr));
            Error::Internal(format!("Failed to disable cookies: {}", e))
        })?;

        // Prepare headers for WinHTTP
        let headers_wide = if !header_string.is_empty() {
            Some(
                header_string
                    .encode_utf16()
                    .chain(std::iter::once(0))
                    .collect::<Vec<u16>>(),
            )
        } else {
            None
        };

        let (headers_ptr, body_ptr, body_len, total_len) = if let Some(ref headers) = headers_wide {
            let headers_slice = &headers[..headers.len() - 1]; // Exclude null terminator

            if !body_bytes.is_empty() {
                (
                    Some(headers_slice),
                    Some(body_bytes.as_ptr() as *const _),
                    body_bytes.len() as u32,
                    body_bytes.len() as u32,
                )
            } else {
                (Some(headers_slice), None, 0, 0)
            }
        } else {
            if !body_bytes.is_empty() {
                (
                    None,
                    Some(body_bytes.as_ptr() as *const _),
                    body_bytes.len() as u32,
                    body_bytes.len() as u32,
                )
            } else {
                (None, None, 0, 0)
            }
        };

        // Start async send request
        WinHttpSendRequest(
            request_handle,
            headers_ptr,
            body_ptr,
            body_len,
            total_len,
            0,
        )
        .map_err(|e| {
            let _ = WinHttpCloseHandle(request_handle);
            let _ = WinHttpCloseHandle(connection);
            let _ = WinHttpCloseHandle(session);
            drop(Arc::from_raw(callback_ctx_ptr));
            Error::Internal(format!("Failed to start send request: {}", e))
        })?;

        // Wait for send completion
        match send_completion_rx.await {
            Ok(Ok(())) => {} // Success
            Ok(Err(e)) => {
                let _ = WinHttpCloseHandle(request_handle);
                let _ = WinHttpCloseHandle(connection);
                let _ = WinHttpCloseHandle(session);
                drop(Arc::from_raw(callback_ctx_ptr));
                return Err(e);
            }
            Err(_) => {
                let _ = WinHttpCloseHandle(request_handle);
                let _ = WinHttpCloseHandle(connection);
                let _ = WinHttpCloseHandle(session);
                drop(Arc::from_raw(callback_ctx_ptr));
                return Err(Error::Internal(
                    "Send completion channel closed".to_string(),
                ));
            }
        }

        // Reset completion sender for headers phase
        if let Ok(mut sender_lock) = callback_context.completion_sender.lock() {
            *sender_lock = Some(headers_completion_tx);
        }

        // Start receiving response
        WinHttpReceiveResponse(request_handle, std::ptr::null_mut()).map_err(|e| {
            let _ = WinHttpCloseHandle(request_handle);
            let _ = WinHttpCloseHandle(connection);
            let _ = WinHttpCloseHandle(session);
            drop(Arc::from_raw(callback_ctx_ptr));
            Error::Internal(format!("Failed to start receive response: {}", e))
        })?;

        // Wait for headers completion
        match headers_completion_rx.await {
            Ok(Ok(())) => {} // Success
            Ok(Err(e)) => {
                let _ = WinHttpCloseHandle(request_handle);
                let _ = WinHttpCloseHandle(connection);
                let _ = WinHttpCloseHandle(session);
                drop(Arc::from_raw(callback_ctx_ptr));
                return Err(e);
            }
            Err(_) => {
                let _ = WinHttpCloseHandle(request_handle);
                let _ = WinHttpCloseHandle(connection);
                let _ = WinHttpCloseHandle(session);
                drop(Arc::from_raw(callback_ctx_ptr));
                return Err(Error::Internal(
                    "Headers completion channel closed".to_string(),
                ));
            }
        }

        // Get status code and headers synchronously (they're available now)
        let mut status_code: u32 = 0;
        let mut status_code_size = std::mem::size_of::<u32>() as u32;
        WinHttpQueryHeaders(
            request_handle,
            WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
            PCWSTR::null(),
            Some(&mut status_code as *mut _ as *mut _),
            &mut status_code_size,
            std::ptr::null_mut(),
        )
        .unwrap_or(());

        let status = http::StatusCode::from_u16(status_code as u16)
            .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);

        // Get headers
        let mut response_headers = http::HeaderMap::new();
        let mut header_buffer_size: u32 = 0;

        // First call to get buffer size
        let _ = WinHttpQueryHeaders(
            request_handle,
            WINHTTP_QUERY_RAW_HEADERS_CRLF,
            PCWSTR::null(),
            None,
            &mut header_buffer_size,
            std::ptr::null_mut(),
        );

        if header_buffer_size > 0 {
            let mut header_buffer = vec![0u16; (header_buffer_size / 2) as usize];
            let result = WinHttpQueryHeaders(
                request_handle,
                WINHTTP_QUERY_RAW_HEADERS_CRLF,
                PCWSTR::null(),
                Some(header_buffer.as_mut_ptr() as *mut _),
                &mut header_buffer_size,
                std::ptr::null_mut(),
            );

            if result.is_ok() {
                let header_string = String::from_utf16_lossy(&header_buffer);
                parse_headers_into_map(&header_string, &mut response_headers);

                // Store cookies if we have cookie storage
                if let Some(cookie_storage) = cookie_storage {
                    store_cookies_from_headers(&header_string, url, cookie_storage);
                }
            }
        }

        // Reset completion sender for data reading phase
        if let Ok(mut sender_lock) = callback_context.completion_sender.lock() {
            *sender_lock = Some(data_completion_tx);
        }

        // Start async data reading by querying data availability
        WinHttpQueryDataAvailable(request_handle, std::ptr::null_mut()) // NULL for async mode
            .map_err(|e| {
                let _ = WinHttpCloseHandle(request_handle);
                let _ = WinHttpCloseHandle(connection);
                let _ = WinHttpCloseHandle(session);
                drop(Arc::from_raw(callback_ctx_ptr));
                Error::Internal(format!("Failed to start data query: {}", e))
            })?;

        // Wait for data reading completion
        match data_completion_rx.await {
            Ok(Ok(())) => {} // Success
            Ok(Err(e)) => {
                let _ = WinHttpCloseHandle(request_handle);
                let _ = WinHttpCloseHandle(connection);
                let _ = WinHttpCloseHandle(session);
                drop(Arc::from_raw(callback_ctx_ptr));
                return Err(e);
            }
            Err(_) => {
                let _ = WinHttpCloseHandle(request_handle);
                let _ = WinHttpCloseHandle(connection);
                let _ = WinHttpCloseHandle(session);
                drop(Arc::from_raw(callback_ctx_ptr));
                return Err(Error::Internal(
                    "Data reading completion channel closed".to_string(),
                ));
            }
        }

        // Get the accumulated response data
        let response_data = if let Ok(data_lock) = callback_context.response_data.lock() {
            let data = data_lock.clone();
            data
        } else {
            Vec::new()
        };

        // Clean up
        drop(Arc::from_raw(callback_ctx_ptr));
        let _ = WinHttpCloseHandle(request_handle);
        let _ = WinHttpCloseHandle(connection);
        let _ = WinHttpCloseHandle(session);

        // Create channel and send response data
        let (tx, rx) = mpsc::channel(1);
        if !response_data.is_empty() {
            let _ = tx.try_send(Ok(bytes::Bytes::from(response_data)));
        }

        Ok(BackendResponse {
            status,
            headers: response_headers,
            url: url.clone(),
            body_receiver: rx,
            redirect_headers: vec![],
        })
    }
}

/// Parse raw HTTP headers into HeaderMap
fn parse_headers_into_map(header_string: &str, headers: &mut http::HeaderMap) {
    for line in header_string.lines() {
        if let Some(colon_pos) = line.find(':') {
            let name = line[..colon_pos].trim();
            let value = line[colon_pos + 1..].trim();

            if let (Ok(header_name), Ok(header_value)) = (
                http::HeaderName::from_bytes(name.as_bytes()),
                http::HeaderValue::from_str(value),
            ) {
                headers.insert(header_name, header_value);
            }
        }
    }
}

/// Store cookies from response headers
fn store_cookies_from_headers(
    header_string: &str,
    url: &url::Url,
    cookie_storage: &std::sync::Arc<crate::backend::CookieJar>,
) {
    // Convert the header string into an http::HeaderMap to use with cookie_store
    let mut headers = http::HeaderMap::new();

    for line in header_string.lines() {
        if let Some(colon_pos) = line.find(':') {
            let name = line[..colon_pos].trim();
            let value = line[colon_pos + 1..].trim();

            if name.eq_ignore_ascii_case("set-cookie") {
                // Add each Set-Cookie header to the HeaderMap
                if let Ok(header_value) = http::HeaderValue::from_str(value) {
                    let header_name = http::HeaderName::from_static("set-cookie");
                    headers.append(header_name, header_value);
                }
            }
        }
    }

    // Process all Set-Cookie headers using the new cookie_store integration
    let _ = cookie_storage.process_response_headers(url, &headers);
}
