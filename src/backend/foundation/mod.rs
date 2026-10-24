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
use bytes::Bytes;
use delegate::data_task::{DataTaskDelegate, DataTaskState, ResponseHead, header_value_string};
use futures_util::Stream;
use objc2::rc::Retained;
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2_foundation::{
    NSCopying, NSDictionary, NSHTTPCookieAcceptPolicy, NSMutableDictionary, NSMutableURLRequest,
    NSNumber, NSString, NSURL, NSURLRequestCachePolicy, NSURLSession, NSURLSessionConfiguration,
    NSURLSessionDataTask,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::time::Sleep;
use url::Url;

/// NSURLSession's own idle timeout, set out of reach. It keeps running while
/// a task is suspended for backpressure, so a slow consumer would trip it;
/// FoundationBody enforces the read timeout instead.
const NO_TIMEOUT: f64 = 365.0 * 24.0 * 60.0 * 60.0;

/// An NSURLSession and its delegate. The session holds its delegate
/// strongly until it is invalidated, so it is invalidated when the last
/// backend clone or response body using it goes away.
pub(crate) struct Session {
    session: Retained<NSURLSession>,
    delegate: Retained<DataTaskDelegate>,
}

impl Drop for Session {
    fn drop(&mut self) {
        // Lets tasks created from `FoundationBackend::session()` (WebSockets)
        // run to completion.
        self.session.finishTasksAndInvalidate();
    }
}

/// Foundation backend using NSURLSession
#[derive(Clone)]
pub struct FoundationBackend {
    session: Arc<Session>,
    cookie_jar: Option<crate::CookieJar>,
    default_headers: Option<http::HeaderMap>,
    user_agent: Option<String>,
    read_timeout: Option<Duration>,
    total_timeout: Option<Duration>,
}

impl FoundationBackend {
    /// Create a new Foundation backend with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(crate::backend::BackendConfig::default())
    }

    /// Create a new Foundation backend around an existing session whose
    /// delegate is `delegate`. The session is invalidated when the backend
    /// is dropped.
    pub fn with_session(
        session: Retained<NSURLSession>,
        delegate: Retained<DataTaskDelegate>,
    ) -> Self {
        let config = crate::backend::BackendConfig::default();
        Self {
            session: Arc::new(Session { session, delegate }),
            cookie_jar: None,
            default_headers: config.default_headers,
            user_agent: None,
            read_timeout: config.read_timeout,
            total_timeout: None,
        }
    }

    /// Create a new Foundation backend with configuration.
    ///
    /// The session is ephemeral (nothing is written to disk) and has no URL
    /// cache, so every request reaches the server, as on the other backends.
    /// Proxies and server trust follow the system settings unless
    /// overridden. Cookies are off unless `use_cookies` is set or a cookie
    /// jar is given.
    pub fn with_config(config: crate::backend::BackendConfig) -> Result<Self> {
        objc2::rc::autoreleasepool(|_| Self::create(config))
    }

    fn create(config: crate::backend::BackendConfig) -> Result<Self> {
        let proxy_credentials = [&config.socks_proxy, &config.https_proxy, &config.http_proxy]
            .into_iter()
            .flatten()
            .find_map(|proxy| Some((proxy.username.clone()?, proxy.password.clone()?)));
        let delegate = DataTaskDelegate::new(
            config.ignore_certificate_errors.unwrap_or(false),
            proxy_credentials,
        );

        let session_config = NSURLSessionConfiguration::ephemeralSessionConfiguration();
        session_config.setURLCache(None);
        session_config.setRequestCachePolicy(NSURLRequestCachePolicy::ReloadIgnoringLocalCacheData);
        session_config.setTimeoutIntervalForRequest(NO_TIMEOUT);

        let cookie_storage = config
            .cookie_jar
            .as_ref()
            .and_then(|jar| jar.foundation_storage());
        if config.use_cookies == Some(true) || cookie_storage.is_some() {
            if let Some(storage) = cookie_storage {
                session_config.setHTTPCookieStorage(Some(storage.ns_storage()));
            }
        } else {
            session_config.setHTTPShouldSetCookies(false);
            session_config.setHTTPCookieAcceptPolicy(NSHTTPCookieAcceptPolicy::Never);
            session_config.setHTTPCookieStorage(None);
        }

        if let Some(proxies) = proxy_dictionary(&config) {
            unsafe { session_config.setConnectionProxyDictionary(Some(&proxies)) };
        }

        let session = unsafe {
            NSURLSession::sessionWithConfiguration_delegate_delegateQueue(
                &session_config,
                Some(ProtocolObject::from_ref(&*delegate)),
                None,
            )
        };

        Ok(Self {
            session: Arc::new(Session { session, delegate }),
            cookie_jar: config.cookie_jar,
            default_headers: config.default_headers,
            user_agent: config.user_agent,
            read_timeout: config.read_timeout,
            total_timeout: config.timeout,
        })
    }

    pub(crate) fn default_headers(&self) -> Option<&http::HeaderMap> {
        self.default_headers.as_ref()
    }

    pub(crate) fn total_timeout(&self) -> Option<Duration> {
        self.total_timeout
    }

    /// Get the underlying NSURLSession (for WebSocket support)
    pub fn session(&self) -> &Retained<NSURLSession> {
        &self.session.session
    }

    /// Execute an HTTP request using NSURLSession
    pub async fn execute(&self, request: BackendRequest) -> Result<BackendResponse> {
        let requested_url = request.url.clone();
        // Foundation hands back autoreleased objects; on a runtime worker
        // thread without a pool of its own they would live until the thread
        // exits. Dropping `body`, including dropping this future at the
        // await, cancels the task.
        let mut body = objc2::rc::autoreleasepool(|_| self.start(request))?;
        let head = std::future::poll_fn(|cx| body.poll_head(cx)).await?;

        // CFNetwork decodes gzip, deflate and br bodies itself.
        let accounting = crate::backend::types::BodyAccounting::new(
            crate::backend::types::has_content_coding(&head.headers),
            crate::backend::types::content_length(&head.headers),
        );
        Ok(BackendResponse {
            status: head.status,
            headers: head.headers,
            url: head.url.unwrap_or(requested_url),
            body: Box::pin(body),
            accounting,
            redirect_headers: vec![],
        })
    }

    /// Build and start the data task. Kept synchronous so that no
    /// Objective-C object that is not Send lives across an await.
    fn start(&self, request: BackendRequest) -> Result<FoundationBody> {
        match request.url.scheme() {
            "http" | "https" => {}
            _ => return Err(Error::InvalidUrl),
        }
        let nsurl = NSURL::URLWithString(&NSString::from_str(request.url.as_str()))
            .ok_or(Error::InvalidUrl)?;

        let nsrequest = NSMutableURLRequest::requestWithURL(&nsurl);
        nsrequest.setHTTPMethod(&NSString::from_str(request.method.as_str()));
        nsrequest.setTimeoutInterval(NO_TIMEOUT);

        let mut headers = self.default_headers.clone().unwrap_or_default();
        if let Some(user_agent) = &self.user_agent {
            headers.insert(
                http::header::USER_AGENT,
                http::HeaderValue::from_str(user_agent).map_err(|_| Error::InvalidHeader)?,
            );
        }
        for name in request.headers.keys() {
            headers.remove(name);
        }
        for (name, value) in &request.headers {
            headers.append(name.clone(), value.clone());
        }

        if let Some(body) = &request.body {
            let (content_type, data) = encode_body(body)?;
            if let Some(content_type) = content_type {
                if !headers.contains_key(http::header::CONTENT_TYPE) {
                    headers.insert(
                        http::header::CONTENT_TYPE,
                        http::HeaderValue::from_str(&content_type)
                            .map_err(|_| Error::InvalidHeader)?,
                    );
                }
            }
            if let Some(data) = data {
                nsrequest.setHTTPBody(Some(&objc2_foundation::NSData::from_vec(data)));
            }
        }

        for name in headers.keys() {
            let field = NSString::from_str(name.as_str());
            for (index, value) in headers.get_all(name).iter().enumerate() {
                let value = header_value_string(value);
                if index == 0 {
                    nsrequest.setValue_forHTTPHeaderField(Some(&value), &field);
                } else {
                    nsrequest.addValue_forHTTPHeaderField(&value, &field);
                }
            }
        }

        let state = Arc::new(DataTaskState::new(request.progress_callback));
        let task = self.session.session.dataTaskWithRequest(&nsrequest);
        self.session
            .delegate
            .register(task.taskIdentifier(), state.clone());

        let body = FoundationBody {
            state,
            task,
            _session: self.session.clone(),
            finished: false,
            read_timeout: request.read_timeout.or(self.read_timeout),
            idle: None,
            waiting_since: None,
        };
        body.task.resume();
        Ok(body)
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

            for (name, value) in &headers {
                req.addValue_forHTTPHeaderField(
                    &header_value_string(value),
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

/// A response body read from a data task. Dropping it before the end
/// cancels the task, which closes its connection.
struct FoundationBody {
    state: Arc<DataTaskState>,
    task: Retained<NSURLSessionDataTask>,
    _session: Arc<Session>,
    finished: bool,
    read_timeout: Option<Duration>,
    idle: Option<Pin<Box<Sleep>>>,
    /// When the owner started waiting for the current head or chunk.
    waiting_since: Option<Instant>,
}

impl FoundationBody {
    fn poll_head(&mut self, cx: &mut Context<'_>) -> Poll<Result<ResponseHead>> {
        match self.state.poll_head(cx) {
            Poll::Ready(result) => {
                self.waiting_since = None;
                if result.is_err() {
                    self.finished = true;
                }
                Poll::Ready(result)
            }
            Poll::Pending => self.poll_read_timeout(cx).map(Err),
        }
    }

    /// Fails with a timeout once the network has been idle for the read
    /// timeout while the owner was waiting. The task is never suspended
    /// while the owner waits (it is resumed before the queue drains), so
    /// this only counts time spent waiting on the network.
    fn poll_read_timeout(&mut self, cx: &mut Context<'_>) -> Poll<Error> {
        let Some(timeout) = self.read_timeout else {
            return Poll::Pending;
        };
        let waiting_since = *self.waiting_since.get_or_insert_with(Instant::now);
        loop {
            let deadline = waiting_since.max(self.state.last_activity()) + timeout;
            if Instant::now() >= deadline {
                self.finished = true;
                objc2::rc::autoreleasepool(|_| self.task.cancel());
                self.state.discard();
                return Poll::Ready(Error::Timeout);
            }
            let idle = self
                .idle
                .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline.into())));
            idle.as_mut().reset(deadline.into());
            if idle.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
        }
    }
}

impl Stream for FoundationBody {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.state.poll_chunk(&self.task, cx) {
            Poll::Ready(item) => {
                self.waiting_since = None;
                if matches!(item, None | Some(Err(_))) {
                    self.finished = true;
                }
                Poll::Ready(item)
            }
            Poll::Pending => self.poll_read_timeout(cx).map(|error| Some(Err(error))),
        }
    }
}

impl Drop for FoundationBody {
    fn drop(&mut self) {
        if !self.finished {
            objc2::rc::autoreleasepool(|_| self.task.cancel());
            self.state.discard();
        }
    }
}

/// The Content-Type a body implies and its encoded bytes.
fn encode_body(body: &crate::body::Body) -> Result<(Option<String>, Option<Vec<u8>>)> {
    Ok(match body {
        crate::body::Body::Empty => (None, None),
        crate::body::Body::Bytes {
            content,
            content_type,
        } => (Some(content_type.clone()), Some(content.to_vec())),
        crate::body::Body::Form { fields } => (
            Some("application/x-www-form-urlencoded".to_string()),
            Some(encode_form_fields(fields).into_bytes()),
        ),
        crate::body::Body::Json { value } => (
            Some("application/json".to_string()),
            Some(serde_json::to_vec(value).map_err(|e| Error::Json(e.to_string()))?),
        ),
        crate::body::Body::Multipart { parts } => {
            let boundary = generate_boundary();
            (
                Some(format!("multipart/form-data; boundary={}", boundary)),
                Some(encode_multipart_data(&boundary, parts)?),
            )
        }
    })
}

/// `connectionProxyDictionary` for explicitly configured proxies, or `None`
/// to use the system's proxy settings (including PAC and WPAD).
fn proxy_dictionary(config: &crate::backend::BackendConfig) -> Option<Retained<NSDictionary>> {
    let proxies = [
        ("HTTP", &config.http_proxy),
        ("HTTPS", &config.https_proxy),
        ("SOCKS", &config.socks_proxy),
    ];
    if proxies.iter().all(|(_, proxy)| proxy.is_none()) {
        return None;
    }

    let dict: Retained<NSMutableDictionary<ProtocolObject<dyn NSCopying>, AnyObject>> =
        NSMutableDictionary::new();
    let set = |key: String, value: &AnyObject| unsafe {
        dict.setObject_forKey(value, ProtocolObject::from_ref(&*NSString::from_str(&key)));
    };
    for (prefix, proxy) in proxies {
        if let Some(proxy) = proxy {
            set(format!("{}Enable", prefix), &NSNumber::new_bool(true));
            set(format!("{}Proxy", prefix), &NSString::from_str(&proxy.host));
            set(
                format!("{}Port", prefix),
                &NSNumber::new_i32(i32::from(proxy.port)),
            );
        }
    }
    Some(unsafe { Retained::cast_unchecked(dict) })
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
