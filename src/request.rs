//! Request types and builders with backend abstraction

use std::sync::Arc;
use std::time::Duration;

use crate::backend::{Backend, types::BackendRequest};
use crate::{Result, body::Body};
use http::{HeaderMap, HeaderValue, Method, header};
use url::Url;

/// An HTTP request ready to be executed using any backend
pub struct Request {
    pub(crate) method: Method,
    pub(crate) url: Url,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<Body>,
    pub(crate) backend: Backend,
    pub(crate) progress_callback: Option<Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
    pub(crate) error_for_status: bool,
    pub(crate) timeout: Option<Duration>,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) require_checked_body: bool,
}

impl Request {
    /// Send the request and get a response
    pub async fn send(self) -> Result<crate::Response> {
        let error_for_status = self.error_for_status;
        let head_request = self.method == http::Method::HEAD;
        let deadline = self
            .timeout
            .or(self.backend.total_timeout())
            .map(|timeout| tokio::time::Instant::now() + timeout);
        let mut headers = self.headers;
        if headers.contains_key(header::RANGE)
            && !headers.contains_key(header::ACCEPT_ENCODING)
            && !self.backend.has_default_header(&header::ACCEPT_ENCODING)
        {
            // The range must address the stored bytes, not a decoded body.
            headers.insert(
                header::ACCEPT_ENCODING,
                HeaderValue::from_static("identity"),
            );
        }
        let backend_request = BackendRequest {
            method: self.method,
            url: self.url,
            headers,
            body: self.body,
            progress_callback: self.progress_callback,
            read_timeout: self.read_timeout,
        };

        // Dropping the backend future on timeout cancels the transfer.
        let execute = self.backend.execute(backend_request);
        let backend_response = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, execute)
                .await
                .map_err(|_| crate::Error::Timeout)??,
            None => execute.await?,
        };
        let response = crate::Response::from_backend(backend_response, deadline, head_request);

        // Check for HTTP error status if enabled (default is true)
        if error_for_status && response.status().as_u16() >= 400 {
            return Err(crate::Error::HttpError(Box::new(response)));
        }

        if self.require_checked_body
            && response.body_integrity() == crate::BodyIntegrity::Unverifiable
        {
            return Err(crate::Error::UnverifiableBody(Box::new(response)));
        }

        Ok(response)
    }
}

/// Builder for constructing HTTP requests with backend abstraction
pub struct RequestBuilder {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: Option<Body>,
    backend: Backend,
    progress_callback: Option<Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
    error_for_status: bool,
    timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    require_checked_body: bool,
}

impl RequestBuilder {
    pub(crate) fn new(method: Method, url: Url, backend: Backend) -> Self {
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: None,
            backend,
            progress_callback: None,
            error_for_status: true,
            timeout: None,
            read_timeout: None,
            require_checked_body: false,
        }
    }

    /// Add a header to the request
    pub fn header(
        mut self,
        name: impl TryInto<http::HeaderName>,
        value: impl Into<String>,
    ) -> crate::Result<Self> {
        let header_name = name.try_into().map_err(|_| crate::Error::InvalidHeader)?;
        let header_value =
            HeaderValue::from_str(&value.into()).map_err(|_| crate::Error::InvalidHeader)?;
        self.headers.insert(header_name, header_value);
        Ok(self)
    }

    /// Set the request body
    pub fn body(mut self, body: impl Into<Body>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Set a JSON body from a serializable value
    pub fn json(mut self, value: impl serde::Serialize) -> Result<Self> {
        self.body = Some(Body::json(value)?);
        Ok(self)
    }

    /// Set a form-urlencoded body from field/value pairs
    pub fn form(
        mut self,
        fields: Vec<(
            impl Into<std::borrow::Cow<'static, str>>,
            impl Into<std::borrow::Cow<'static, str>>,
        )>,
    ) -> Self {
        self.body = Some(Body::form(fields));
        self
    }

    /// Set a plain text body
    pub fn text(mut self, content: impl Into<String>) -> Self {
        self.body = Some(Body::text(content));
        self
    }

    /// Set authentication for the request
    pub fn auth(mut self, auth: crate::Auth) -> Self {
        let header_value =
            HeaderValue::from_str(&auth.to_header_value()).expect("Invalid auth header value");
        self.headers.insert(header::AUTHORIZATION, header_value);
        self
    }

    /// Set progress callback
    pub fn progress<F>(mut self, callback: F) -> Self
    where
        F: Fn(u64, Option<u64>) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(callback));
        self
    }

    /// Set a total deadline for this request, overriding the client's.
    ///
    /// The deadline covers everything from sending the request to reading the
    /// last byte of the body; when it passes, the transfer is cancelled and
    /// the pending call fails with [`Error::Timeout`](crate::Error::Timeout).
    /// Use it for small requests, not for large downloads.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the read (idle) timeout for this request, overriding the client's.
    ///
    /// This bounds how long the transfer may wait for the network while frakt
    /// wants data: for the response to start and, during the body, for the
    /// next chunk. Time the consumer spends not reading does not count.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    /// Refuse a response whose body could be cut short without an error.
    ///
    /// When set, `send` fails with
    /// [`Error::UnverifiableBody`](crate::Error::UnverifiableBody) (holding
    /// the unread response) if [`Response::body_integrity`](crate::Response::body_integrity)
    /// is `Unverifiable`: the body has no Content-Length and the backend
    /// cannot check its framing, which on Foundation is every body without
    /// Content-Length, chunked or HTTP/2 alike, because CFNetwork treats a
    /// cleanly closed connection as the end of such a body. Use it for
    /// downloads that must be whole.
    pub fn require_checked_body(mut self) -> Self {
        self.require_checked_body = true;
        self
    }

    /// Configure whether to return an error for HTTP error status codes (>= 400).
    ///
    /// When enabled (the default), responses with status codes >= 400 will return
    /// an `HttpError` containing the full response. When disabled, all status codes
    /// are treated as success and you must check the status manually.
    ///
    /// # Arguments
    ///
    /// * `enabled` - If `true`, error on status >= 400; if `false`, accept all status codes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    ///
    /// // Allow 404 responses to be treated as success
    /// let response = client
    ///     .get("https://httpbin.org/status/404")?
    ///     .error_for_status(false)
    ///     .send()
    ///     .await?;
    ///
    /// assert_eq!(response.status(), 404);
    /// # Ok(())
    /// # }
    /// ```
    pub fn error_for_status(mut self, enabled: bool) -> Self {
        self.error_for_status = enabled;
        self
    }

    /// Convenience method to allow error status codes (>= 400) to be treated as success.
    ///
    /// This is equivalent to calling `.error_for_status(false)`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use frakt::Client;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new()?;
    ///
    /// // Don't error on 404
    /// let response = client
    ///     .get("https://httpbin.org/status/404")?
    ///     .allow_error_status()
    ///     .send()
    ///     .await?;
    ///
    /// assert_eq!(response.status(), 404);
    /// # Ok(())
    /// # }
    /// ```
    pub fn allow_error_status(self) -> Self {
        self.error_for_status(false)
    }

    /// Send the request and return the response
    pub async fn send(self) -> Result<crate::Response> {
        let request = Request {
            method: self.method,
            url: self.url,
            headers: self.headers,
            body: self.body,
            backend: self.backend,
            progress_callback: self.progress_callback,
            error_for_status: self.error_for_status,
            timeout: self.timeout,
            read_timeout: self.read_timeout,
            require_checked_body: self.require_checked_body,
        };
        request.send().await
    }
}
