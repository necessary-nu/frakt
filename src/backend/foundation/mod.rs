#![allow(non_snake_case)]

//! Foundation backend using NSURLSession

/// Foundation delegate implementations
pub mod delegate;

/// Foundation cookie storage implementation
pub mod cookies;
pub use cookies::FoundationCookieStorage;

/// Foundation WebSocket implementation
pub mod websocket;
pub use websocket::{FoundationWebSocket, FoundationWebSocketBuilder};

/// Foundation error handling
pub mod error;

use crate::backend::types::{BackendRequest, BackendResponse};
use crate::{Error, Result};
use block2::StackBlock;
use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2::runtime::Bool;
use objc2::runtime::ProtocolObject;
use objc2_foundation::{NSHTTPURLResponse, NSMutableURLRequest, NSString, NSURL, NSURLSession};
use std::ptr::NonNull;
use tokio::sync::mpsc;
use url::Url;

/// Foundation backend using NSURLSession
#[derive(Clone)]
pub struct FoundationBackend {
    session: Retained<NSURLSession>,
    delegate: Retained<delegate::DataTaskDelegate>,
    cookie_jar: Option<crate::CookieJar>,
    default_headers: Option<http::HeaderMap>,
    total_timeout: Option<std::time::Duration>,
}

impl FoundationBackend {
    /// Create a new Foundation backend with default configuration
    pub fn new() -> Result<Self> {
        // Create delegate and session with delegate
        let delegate = delegate::DataTaskDelegate::new();
        let session = unsafe {
            NSURLSession::sessionWithConfiguration_delegate_delegateQueue(
                &objc2_foundation::NSURLSessionConfiguration::defaultSessionConfiguration(),
                Some(ProtocolObject::from_ref(&*delegate)),
                None,
            )
        };

        Ok(Self {
            session,
            delegate,
            cookie_jar: None,
            default_headers: None,
            total_timeout: None,
        })
    }

    /// Create a new Foundation backend with custom session configuration
    pub fn with_session(
        session: Retained<NSURLSession>,
        delegate: Retained<delegate::DataTaskDelegate>,
    ) -> Self {
        Self {
            session,
            delegate,
            cookie_jar: None,
            default_headers: None,
            total_timeout: None,
        }
    }

    /// Create a new Foundation backend with configuration
    pub fn with_config(config: crate::backend::BackendConfig) -> Result<Self> {
        let delegate = delegate::DataTaskDelegate::new();

        let session_config =
            objc2_foundation::NSURLSessionConfiguration::defaultSessionConfiguration();

        // Apply timeout configuration
        if let Some(timeout) = config.read_timeout {
            session_config.setTimeoutIntervalForRequest(timeout.as_secs_f64());
        }

        // Other configuration options like default headers are stored in the backend
        // and applied to individual requests

        let session = unsafe {
            NSURLSession::sessionWithConfiguration_delegate_delegateQueue(
                &session_config,
                Some(ProtocolObject::from_ref(&*delegate)),
                None,
            )
        };

        Ok(Self {
            session,
            delegate,
            cookie_jar: config.cookie_jar,
            default_headers: config.default_headers,
            total_timeout: config.timeout,
        })
    }

    pub(crate) fn total_timeout(&self) -> Option<std::time::Duration> {
        self.total_timeout
    }

    /// Get the underlying NSURLSession (for WebSocket support)
    pub fn session(&self) -> &Retained<NSURLSession> {
        &self.session
    }

    /// Execute an HTTP request using NSURLSession
    pub async fn execute(&self, request: BackendRequest) -> Result<BackendResponse> {
        // Create NSURLRequest and validate URL
        let nsurl = NSURL::URLWithString(&NSString::from_str(&request.url.as_str()))
            .ok_or(Error::InvalidUrl)?;

        // Validate URL scheme using Foundation APIs
        let scheme = nsurl.scheme();
        if let Some(scheme_str) = scheme {
            let scheme_string = scheme_str.to_string();
            match scheme_string.as_str() {
                "http" | "https" => {}
                _ => {
                    return Err(Error::InvalidUrl);
                }
            }
        } else {
            return Err(Error::InvalidUrl);
        }

        let nsrequest = {
            let req = NSMutableURLRequest::requestWithURL(&nsurl);
            req.setHTTPMethod(&NSString::from_str(request.method.as_str()));

            // Set default headers first
            if let Some(ref default_headers) = self.default_headers {
                for (name, value) in default_headers {
                    req.setValue_forHTTPHeaderField(
                        Some(&NSString::from_str(
                            value.to_str().expect("Invalid default header value"),
                        )),
                        &NSString::from_str(name.as_str()),
                    );
                }
            }

            // Set request headers (these can override default headers)
            for (name, value) in &request.headers {
                req.setValue_forHTTPHeaderField(
                    Some(&NSString::from_str(
                        value.to_str().expect("Invalid header value"),
                    )),
                    &NSString::from_str(name.as_str()),
                );
            }

            // Set body
            if let Some(body) = &request.body {
                match body {
                    crate::body::Body::Empty => {}
                    crate::body::Body::Bytes {
                        content,
                        content_type,
                    } => {
                        req.setValue_forHTTPHeaderField(
                            Some(&NSString::from_str(content_type)),
                            &NSString::from_str("Content-Type"),
                        );
                        let nsdata = objc2_foundation::NSData::from_vec(content.to_vec());
                        req.setHTTPBody(Some(&nsdata));
                    }
                    crate::body::Body::Form { fields } => {
                        req.setValue_forHTTPHeaderField(
                            Some(&NSString::from_str("application/x-www-form-urlencoded")),
                            &NSString::from_str("Content-Type"),
                        );
                        let encoded = encode_form_fields(fields);
                        let nsdata = objc2_foundation::NSData::from_vec(encoded.into_bytes());
                        req.setHTTPBody(Some(&nsdata));
                    }
                    crate::body::Body::Json { value } => {
                        req.setValue_forHTTPHeaderField(
                            Some(&NSString::from_str("application/json")),
                            &NSString::from_str("Content-Type"),
                        );
                        let json_bytes =
                            serde_json::to_vec(value).map_err(|e| Error::Json(e.to_string()))?;
                        let nsdata = objc2_foundation::NSData::from_vec(json_bytes);
                        req.setHTTPBody(Some(&nsdata));
                    }
                    crate::body::Body::Multipart { parts } => {
                        let boundary = generate_boundary();
                        let content_type = format!("multipart/form-data; boundary={}", boundary);
                        req.setValue_forHTTPHeaderField(
                            Some(&NSString::from_str(&content_type)),
                            &NSString::from_str("Content-Type"),
                        );
                        let multipart_data = encode_multipart_data(&boundary, parts)?;
                        let nsdata = objc2_foundation::NSData::from_vec(multipart_data);
                        req.setHTTPBody(Some(&nsdata));
                    }
                }
            }

            req
        };

        // Create task context with progress callback if present
        let task_context = if let Some(progress_callback) = request.progress_callback {
            std::sync::Arc::new(
                delegate::shared_context::TaskSharedContext::with_progress_callback(
                    progress_callback,
                ),
            )
        } else {
            std::sync::Arc::new(delegate::shared_context::TaskSharedContext::new())
        };

        // Create data task
        let data_task = self.session.dataTaskWithRequest(&nsrequest);

        // Register the task context with the delegate using the task identifier
        let task_id = data_task.taskIdentifier() as usize;
        self.delegate.register_task(task_id, task_context.clone());

        // Create channel for response body
        let (tx, rx) = mpsc::channel(32);

        // Start task
        data_task.resume();

        // Wait for response headers
        while !task_context.is_completed() && task_context.response.load_full().is_none() {
            tokio::task::yield_now().await;
        }

        // Check for errors
        if let Some(error) = task_context.error.load_full() {
            return Err(Error::from_ns_error(&*error));
        }

        // Get response
        let response = task_context
            .response
            .load_full()
            .ok_or_else(|| Error::Internal("No response received".to_string()))?;

        let status = {
            // Try to cast to NSHTTPURLResponse to get status code
            let http_response: Option<&NSHTTPURLResponse> = response.as_ref().downcast_ref();
            if let Some(http_response) = http_response {
                http_response.statusCode() as u16
            } else {
                200
            }
        };
        let status_code = http::StatusCode::from_u16(status as u16).unwrap_or(http::StatusCode::OK);

        // Extract headers from NSHTTPURLResponse
        let mut headers = http::HeaderMap::new();

        unsafe {
            if let Some(http_response) = response.as_ref().downcast_ref::<NSHTTPURLResponse>() {
                let all_headers = http_response.allHeaderFields();

                use std::cell::RefCell;
                use std::sync::Arc;

                let headers_cell = Arc::new(RefCell::new(http::HeaderMap::new()));
                let headers_cell_clone = headers_cell.clone();

                let closure = move |key: NonNull<AnyObject>,
                                    value: NonNull<AnyObject>,
                                    _stop: NonNull<Bool>| {
                    // Cast to NSString
                    if let Some(key_nsstring) = key.as_ref().downcast_ref::<NSString>() {
                        if let Some(value_nsstring) = value.as_ref().downcast_ref::<NSString>() {
                            let key_str = key_nsstring.to_string();
                            let value_str = value_nsstring.to_string();

                            // Convert to http types and insert
                            if let (Ok(header_name), Ok(header_value)) = (
                                http::HeaderName::from_bytes(key_str.as_bytes()),
                                http::HeaderValue::from_str(&value_str),
                            ) {
                                headers_cell_clone
                                    .borrow_mut()
                                    .insert(header_name, header_value);
                            }
                        }
                    }
                };

                // Transform FnMut to Fn using RefCell
                let block = StackBlock::new(
                    move |key: NonNull<AnyObject>,
                          value: NonNull<AnyObject>,
                          stop: NonNull<Bool>| { closure(key, value, stop) },
                );

                // Call enumerateKeysAndObjectsUsingBlock
                let _: () =
                    objc2::msg_send![&*all_headers, enumerateKeysAndObjectsUsingBlock: &*block];
                drop(block);

                // Extract the final headers
                headers = Arc::try_unwrap(headers_cell)
                    .unwrap_or_else(|_| panic!("Headers RefCell still has references"))
                    .into_inner();
            }
        }

        // Spawn task to read body data
        let body_context = task_context.clone();
        tokio::spawn(async move {
            while !body_context.is_completed() {
                let data = body_context.response_buffer.lock().await.clone();
                if !data.is_empty() {
                    let bytes = bytes::Bytes::from(data);
                    if tx.send(Ok(bytes)).await.is_err() {
                        break;
                    }
                    // Clear the buffer
                    body_context.response_buffer.lock().await.clear();
                }
                tokio::task::yield_now().await;
            }

            // Send any remaining data when task completes
            let final_data = body_context.response_buffer.lock().await.clone();
            if !final_data.is_empty() {
                let bytes = bytes::Bytes::from(final_data);
                let _ = tx.send(Ok(bytes)).await;
            }
        });

        Ok(BackendResponse {
            status: status_code,
            headers,
            url: request.url,
            body: crate::backend::types::channel_body(rx),
            redirect_headers: vec![],
        })
    }

    /// Execute a background download
    pub async fn execute_background_download(
        &self,
        url: Url,
        file_path: std::path::PathBuf,
        session_identifier: Option<String>,
        headers: http::HeaderMap,
        progress_callback: Option<Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
        _error_for_status: bool,
    ) -> Result<crate::client::download::DownloadResponse> {
        use delegate::background_session::BackgroundSessionDelegate;
        use delegate::shared_context::TaskSharedContext;
        use objc2_foundation::NSURLSessionConfiguration;
        use std::sync::Arc;

        // Generate session identifier if not provided
        let session_id = session_identifier.unwrap_or_else(|| {
            format!(
                "frakt-bg-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            )
        });

        // Create background session configuration
        let session_config = {
            NSURLSessionConfiguration::backgroundSessionConfigurationWithIdentifier(
                &NSString::from_str(&session_id),
            )
        };

        // Create background delegate
        let delegate = BackgroundSessionDelegate::new();
        let session = unsafe {
            NSURLSession::sessionWithConfiguration_delegate_delegateQueue(
                &session_config,
                Some(ProtocolObject::from_ref(&*delegate)),
                None,
            )
        };

        // Create download task with headers
        let nsurl =
            { NSURL::URLWithString(&NSString::from_str(url.as_str())).ok_or(Error::InvalidUrl)? };

        let nsrequest = {
            let req = NSMutableURLRequest::requestWithURL(&nsurl);

            // Set headers
            for (name, value) in &headers {
                req.setValue_forHTTPHeaderField(
                    Some(&NSString::from_str(
                        value.to_str().expect("Invalid header value"),
                    )),
                    &NSString::from_str(name.as_str()),
                );
            }

            req
        };

        let download_task = session.downloadTaskWithRequest(&nsrequest);
        let task_id = download_task.taskIdentifier() as usize;

        // Create task context
        let mut task_context = if let Some(callback) = progress_callback {
            TaskSharedContext::with_progress_callback(Arc::new(callback))
        } else {
            TaskSharedContext::new()
        };

        task_context.download_context = Some(Arc::new(
            delegate::shared_context::DownloadContext::new(Some(file_path.clone())),
        ));

        let task_context = Arc::new(task_context);

        // Register task with delegate
        delegate.register_task(task_id, task_context.clone());

        // Start download
        download_task.resume();

        // Wait for completion
        while !task_context.is_completed() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Check for errors
        if let Some(error) = task_context.error.load_full() {
            return Err(Error::from_ns_error(&*error));
        }

        // Calculate bytes downloaded
        let bytes_downloaded = task_context
            .bytes_downloaded
            .load(std::sync::atomic::Ordering::Relaxed);

        // TODO: Capture actual status and headers from NSURLSessionDownloadTask response
        // For now, we use placeholder values since background downloads don't easily
        // expose the HTTP response
        Ok(crate::client::download::DownloadResponse {
            file_path,
            bytes_downloaded,
            status: http::StatusCode::OK,
            headers: http::HeaderMap::new(),
        })
    }

    /// Get the cookie jar if configured
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        self.cookie_jar.as_ref()
    }
}

fn encode_form_fields(
    fields: &[(
        std::borrow::Cow<'static, str>,
        std::borrow::Cow<'static, str>,
    )],
) -> String {
    fields
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn generate_boundary() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("----formdata-frakt-{}", timestamp)
}

fn encode_multipart_data(boundary: &str, parts: &[crate::body::MultipartPart]) -> Result<Vec<u8>> {
    let mut data = Vec::new();

    for part in parts {
        data.extend_from_slice(format!("\r\n--{}\r\n", boundary).as_bytes());

        let mut disposition = format!("Content-Disposition: form-data; name=\"{}\"", part.name);
        if let Some(filename) = &part.filename {
            disposition.push_str(&format!("; filename=\"{}\"", filename));
        }
        data.extend_from_slice(disposition.as_bytes());
        data.extend_from_slice(b"\r\n");

        if let Some(content_type) = &part.content_type {
            data.extend_from_slice(format!("Content-Type: {}\r\n", content_type).as_bytes());
        }

        data.extend_from_slice(b"\r\n");
        data.extend_from_slice(&part.content);
    }

    data.extend_from_slice(format!("\r\n--{}--\r\n", boundary).as_bytes());
    Ok(data)
}
