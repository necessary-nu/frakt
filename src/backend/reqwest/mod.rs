//! Reqwest backend for cross-platform HTTP support

mod background;
#[cfg(all(
    unix,
    not(any(
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "visionos",
        target_os = "android"
    ))
))]
mod helper;

pub mod websocket;
pub use websocket::{ReqwestWebSocket, ReqwestWebSocketBuilder};

use crate::backend::redirect;
use crate::backend::types::{BackendRequest, BackendResponse, BodyAccounting, ProgressCallback};
use crate::{Error, Result};
use bytes::Bytes;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;
use url::Url;

/// An in-memory body as a stream of chunks, so that progress can be
/// reported as hyper takes them.
fn bytes_chunks(
    data: Bytes,
    chunk_size: usize,
) -> impl Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    futures_util::stream::unfold(data, move |mut rest| async move {
        if rest.is_empty() {
            return None;
        }
        let chunk = rest.split_to(chunk_size.min(rest.len()));
        Some((Ok(chunk), rest))
    })
}

/// A request body that reports each chunk to the progress callback as hyper
/// takes it for the wire, which is as near to the network as reqwest lets a
/// caller see.
fn progress_body(
    chunks: impl Stream<Item = std::io::Result<Bytes>> + Send + 'static,
    total: u64,
    callback: Option<ProgressCallback>,
) -> reqwest::Body {
    use futures_util::StreamExt;

    if let Some(callback) = &callback {
        callback(0, Some(total));
    }
    let mut sent = 0u64;
    reqwest::Body::wrap_stream(chunks.inspect(move |chunk| {
        if let (Ok(chunk), Some(callback)) = (chunk, &callback) {
            sent += chunk.len() as u64;
            callback(sent, Some(total));
        }
    }))
}

tokio::task_local! {
    /// The URL the caller asked for, set while a request is being sent.
    /// reqwest tells its cookie provider only which hop set a cookie, and
    /// the accept policy needs to know where the chain started.
    static MAIN_DOCUMENT: Url;
}

/// frakt's cookie jar as reqwest's cookie provider, so the jar the caller
/// holds is the one requests send from and store into.
struct JarProvider(crate::backend::CookieJar);

impl reqwest::cookie::CookieStore for JarProvider {
    fn set_cookies(&self, cookie_headers: &mut dyn Iterator<Item = &http::HeaderValue>, url: &Url) {
        let main_document = MAIN_DOCUMENT.try_with(Url::clone).ok();
        self.0
            .store_set_cookies(main_document.as_ref(), url, cookie_headers);
    }

    fn cookies(&self, url: &Url) -> Option<http::HeaderValue> {
        self.0.cookie_header(url)
    }
}

/// A reqwest body polled directly by the consumer, so hyper reads from the
/// socket only as fast as the consumer reads and dropping it closes the
/// connection.
struct ReqwestBody {
    inner: BodyItems,
    read_timeout: Option<Duration>,
    /// Armed only while the consumer is waiting on the network.
    idle: Option<Pin<Box<Sleep>>>,
    done: bool,
}

impl Stream for ReqwestBody {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.idle = None;
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.done = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                let Some(timeout) = self.read_timeout else {
                    return Poll::Pending;
                };
                let idle = self
                    .idle
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
                if idle.as_mut().poll(cx).is_ready() {
                    self.done = true;
                    return Poll::Ready(Some(Err(Error::Timeout)));
                }
                Poll::Pending
            }
        }
    }
}

type BodyItems = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// The codings the reqwest backend asks for and decodes.
const ACCEPT_ENCODING: &str = "gzip, deflate, br";

#[derive(Clone, Copy)]
enum Coding {
    Gzip,
    Deflate,
    Brotli,
}

/// The coding to undo, if the body has exactly one that we decode.
fn decodable_coding(headers: &http::HeaderMap) -> Option<Coding> {
    let mut codings = headers
        .get_all(http::header::CONTENT_ENCODING)
        .iter()
        .flat_map(|value| value.to_str().unwrap_or("?").split(','))
        .map(|coding| coding.trim().to_ascii_lowercase())
        .filter(|coding| !coding.is_empty() && coding != "identity");
    let coding = match codings.next()?.as_str() {
        "gzip" | "x-gzip" => Coding::Gzip,
        "deflate" => Coding::Deflate,
        "br" => Coding::Brotli,
        _ => return None,
    };
    codings.next().is_none().then_some(coding)
}

/// The response body as delivered: decoded if `coding` is set, with every
/// raw chunk counted as wire bytes on the way in. hyper has already checked
/// the raw body against Content-Length or chunked framing.
fn body_items(
    raw: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
    coding: Option<Coding>,
    accounting: Arc<BodyAccounting>,
) -> BodyItems {
    use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder};
    use futures_util::StreamExt;

    let raw =
        raw.map(move |chunk| chunk.inspect(|bytes| accounting.add_wire_bytes(bytes.len() as u64)));
    let Some(coding) = coding else {
        return Box::pin(raw.map(|chunk| chunk.map_err(map_reqwest_error)));
    };

    let reader =
        tokio_util::io::StreamReader::new(raw.map(|chunk| chunk.map_err(std::io::Error::other)));
    let decoder: Pin<Box<dyn tokio::io::AsyncRead + Send>> = match coding {
        Coding::Gzip => {
            let mut decoder = GzipDecoder::new(reader);
            decoder.multiple_members(true);
            Box::pin(decoder)
        }
        Coding::Deflate => Box::pin(ZlibDecoder::new(reader)),
        Coding::Brotli => Box::pin(BrotliDecoder::new(reader)),
    };
    Box::pin(
        tokio_util::io::ReaderStream::with_capacity(decoder, 64 * 1024)
            .map(|chunk| chunk.map_err(map_decode_error)),
    )
}

/// A transport error from under the decoder keeps its classification;
/// anything else is a body that would not decode.
fn map_decode_error(error: std::io::Error) -> Error {
    let message = error.to_string();
    match error
        .into_inner()
        .map(|inner| inner.downcast::<reqwest::Error>())
    {
        Some(Ok(error)) => map_reqwest_error(*error),
        _ => Error::Network {
            code: -1,
            message: format!("Failed to decode the response body: {}", message),
        },
    }
}

fn body_content_type(body: &crate::body::Body) -> Option<&str> {
    match body {
        crate::body::Body::Bytes { content_type, .. }
        | crate::body::Body::File { content_type, .. } => Some(content_type),
        crate::body::Body::Form { .. } => Some("application/x-www-form-urlencoded"),
        crate::body::Body::Json { .. } => Some("application/json"),
        crate::body::Body::Empty | crate::body::Body::Multipart { .. } => None,
    }
}

/// Send a request and follow its redirects by frakt's own policy.
///
/// The client is built to follow none itself: reqwest keeps headers it does
/// not know to be credentials, such as an API key, when a redirect changes
/// origin, and cannot send a streamed body a second time. `request` builds
/// the reqwest request for each hop, with the body only if it is told to.
/// `read_timeout` bounds the wait for each hop's response to start.
pub(crate) async fn follow_redirects(
    mut method: http::Method,
    mut url: Url,
    mut headers: http::HeaderMap,
    read_timeout: Option<Duration>,
    mut request: impl FnMut(
        http::Method,
        Url,
        &http::HeaderMap,
        bool,
    ) -> Result<reqwest::RequestBuilder>,
) -> Result<reqwest::Response> {
    let main_document = url.clone();
    let mut with_body = true;
    let mut redirects = 0;

    loop {
        let send = MAIN_DOCUMENT.scope(
            main_document.clone(),
            request(method.clone(), url.clone(), &headers, with_body)?.send(),
        );
        let response = match read_timeout {
            Some(timeout) => tokio::time::timeout(timeout, send)
                .await
                .map_err(|_| Error::Timeout)?,
            None => send.await,
        }
        .map_err(map_reqwest_error)?;

        let Some(hop) = redirect::next_hop(
            &method,
            &url,
            &headers,
            response.status(),
            response.headers(),
        ) else {
            return Ok(response);
        };
        redirects += 1;
        if redirects > redirect::MAX_REDIRECTS {
            return Err(redirect::too_many_redirects());
        }
        with_body &= hop.keep_body;
        (method, url, headers) = (hop.method, hop.url, hop.headers);
    }
}

/// How much of an upload is read and handed to hyper at a time.
const UPLOAD_CHUNK: usize = 64 * 1024;

/// The encoded bytes of a body that is sent from memory.
fn body_to_bytes(body: &crate::body::Body) -> Result<Bytes> {
    Ok(match body {
        crate::body::Body::Bytes { content, .. } => content.clone(),
        crate::body::Body::Form { fields } => fields
            .iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&")
            .into(),
        crate::body::Body::Json { value } => serde_json::to_vec(value)
            .map_err(|e| Error::Json(e.to_string()))?
            .into(),
        crate::body::Body::Empty
        | crate::body::Body::Multipart { .. }
        | crate::body::Body::File { .. } => Bytes::new(),
    })
}

/// Classify a reqwest error as timeout, TLS or other network failure.
fn map_reqwest_error(error: reqwest::Error) -> Error {
    use std::error::Error as _;

    if error.is_timeout() {
        return Error::Timeout;
    }
    if error.is_builder() {
        return Error::Internal(format!("Invalid request: {}", error));
    }

    let message = error_chain_message(&error);
    let mut code = None;
    let mut source: Option<&(dyn std::error::Error + 'static)> = error.source();
    while let Some(err) = source {
        if err.is::<rustls::Error>() {
            return Error::Tls { message };
        }
        if let Some(io) = err.downcast_ref::<std::io::Error>() {
            if wraps_rustls_error(io) {
                return Error::Tls { message };
            }
            if io.kind() == std::io::ErrorKind::TimedOut {
                return Error::Timeout;
            }
            code = code.or(io.raw_os_error());
        }
        source = err.source();
    }

    Error::Network {
        code: code.map(i64::from).unwrap_or(-1),
        message,
    }
}

/// io::Error::source skips the error an io::Error wraps, and hyper-rustls
/// wraps the rustls error in two io::Errors, so unwrap them by hand.
fn wraps_rustls_error(io: &std::io::Error) -> bool {
    let mut inner = io.get_ref();
    while let Some(error) = inner {
        if error.is::<rustls::Error>() {
            return true;
        }
        inner = error
            .downcast_ref::<std::io::Error>()
            .and_then(|io| io.get_ref());
    }
    false
}

fn error_chain_message(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(err) = source {
        message.push_str(": ");
        message.push_str(&err.to_string());
        source = err.source();
    }
    message
}

/// The TLS configuration: the process's default rustls crypto provider or
/// else ring, the webpki (Mozilla) roots, and HTTP/1.1 by ALPN, as no HTTP/2
/// is built. reqwest has no feature for ring with the webpki roots.
fn tls_config(accept_invalid_certs: bool) -> Result<rustls::ClientConfig> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .map_err(|e| Error::Internal(format!("Invalid TLS versions: {}", e)))?;
    let mut config = if accept_invalid_certs {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate))
    } else {
        builder.with_root_certificates(rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        })
    }
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// Accepts every server certificate, for `ignore_certificate_errors`.
#[derive(Debug)]
struct AcceptAnyCertificate;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
    }
}

/// The plain data a `reqwest::Client` is built from, kept by the backend so
/// the background download helper process can build an equivalent client.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ClientSettings {
    user_agent: Option<String>,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    ignore_certificate_errors: bool,
    default_headers: Vec<(String, Vec<u8>)>,
    use_cookies: bool,
    http_proxy: Option<ProxySettings>,
    https_proxy: Option<ProxySettings>,
    socks_proxy: Option<ProxySettings>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ProxySettings {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

impl ProxySettings {
    /// The proxy at `scheme://host:port` for the traffic `intercept` selects.
    fn build(
        &self,
        scheme: &str,
        name: &str,
        intercept: fn(&str) -> reqwest::Result<reqwest::Proxy>,
    ) -> Result<reqwest::Proxy> {
        let url = format!("{}://{}:{}", scheme, self.host, self.port);
        let proxy = intercept(&url)
            .map_err(|e| Error::Internal(format!("Invalid {} proxy: {}", name, e)))?;
        Ok(match (&self.username, &self.password) {
            (Some(username), Some(password)) => proxy.basic_auth(username, password),
            _ => proxy,
        })
    }
}

impl ClientSettings {
    fn from_config(config: &crate::backend::BackendConfig) -> Self {
        let proxy = |proxy: &crate::client::ProxyConfig| ProxySettings {
            host: proxy.host.clone(),
            port: proxy.port,
            username: proxy.username.clone(),
            password: proxy.password.clone(),
        };
        Self {
            user_agent: config.user_agent.clone(),
            connect_timeout: config.connect_timeout,
            read_timeout: config.read_timeout,
            ignore_certificate_errors: config.ignore_certificate_errors.unwrap_or(false),
            default_headers: config
                .default_headers
                .iter()
                .flatten()
                .map(|(name, value)| (name.as_str().to_string(), value.as_bytes().to_vec()))
                .collect(),
            use_cookies: config.cookies_enabled(),
            http_proxy: config.http_proxy.as_ref().map(proxy),
            https_proxy: config.https_proxy.as_ref().map(proxy),
            socks_proxy: config.socks_proxy.as_ref().map(proxy),
        }
    }
}

/// The client `settings` describe. With `jar`, the client sends from and
/// stores into that jar; without one, as in the helper process, cookies are
/// kept only for the life of the client, so that one set along a redirect
/// chain reaches the later hops.
fn build_client(
    settings: &ClientSettings,
    jar: Option<&crate::backend::CookieJar>,
) -> Result<reqwest::Client> {
    // `follow_redirects` follows them instead.
    let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());

    // reqwest's own `timeout` is a total deadline and its `read_timeout`
    // cannot be overridden per request, so both are enforced by the backend
    // instead.
    if let Some(timeout) = settings.connect_timeout {
        builder = builder.connect_timeout(timeout);
    }

    if let Some(user_agent) = &settings.user_agent {
        builder = builder.user_agent(user_agent);
    }

    builder = builder.tls_backend_preconfigured(tls_config(settings.ignore_certificate_errors)?);

    if !settings.default_headers.is_empty() {
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &settings.default_headers {
            if let (Ok(name), Ok(value)) = (
                reqwest::header::HeaderName::from_bytes(name.as_bytes()),
                reqwest::header::HeaderValue::from_bytes(value),
            ) {
                headers.append(name, value);
            }
        }
        builder = builder.default_headers(headers);
    }

    match jar {
        Some(jar) => builder = builder.cookie_provider(Arc::new(JarProvider(jar.clone()))),
        None if settings.use_cookies => builder = builder.cookie_store(true),
        None => {}
    }

    if let Some(proxy) = &settings.http_proxy {
        builder = builder.proxy(proxy.build("http", "HTTP", |url| reqwest::Proxy::http(url))?);
    }
    if let Some(proxy) = &settings.https_proxy {
        builder = builder.proxy(proxy.build("https", "HTTPS", |url| reqwest::Proxy::https(url))?);
    }
    if let Some(proxy) = &settings.socks_proxy {
        builder = builder.proxy(proxy.build("socks5", "SOCKS", |url| reqwest::Proxy::all(url))?);
    }

    builder
        .build()
        .map_err(|e| Error::Internal(format!("Failed to create reqwest client: {}", e)))
}

/// What [`enable_background_helper`](crate::enable_background_helper) does
/// for this backend.
pub(crate) fn enable_background_helper() {
    #[cfg(all(
        unix,
        not(any(
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "visionos",
            target_os = "android"
        ))
    ))]
    helper::enable();
}

/// Reqwest backend for cross-platform HTTP
#[derive(Clone)]
pub struct ReqwestBackend {
    client: reqwest::Client,
    cookie_jar: Option<crate::CookieJar>,
    default_headers: Option<http::HeaderMap>,
    /// Shared, as the backend is cloned for every request.
    settings: Arc<ClientSettings>,
    total_timeout: Option<Duration>,
}

impl ReqwestBackend {
    /// Create a new Reqwest backend
    pub fn new() -> Result<Self> {
        Self::with_config(crate::backend::BackendConfig::default())
    }

    /// Create a new Reqwest backend with configuration
    pub fn with_config(config: crate::backend::BackendConfig) -> Result<Self> {
        let cookie_jar = config.cookie_store_jar()?;
        let settings = ClientSettings::from_config(&config);
        let client = build_client(
            &settings,
            cookie_jar.as_ref().and_then(|jar| jar.cookie_store()),
        )?;

        Ok(Self {
            client,
            cookie_jar,
            default_headers: config.default_headers,
            settings: Arc::new(settings),
            total_timeout: config.timeout,
        })
    }

    pub(crate) fn default_headers(&self) -> Option<&http::HeaderMap> {
        self.default_headers.as_ref()
    }

    pub(crate) fn total_timeout(&self) -> Option<Duration> {
        self.total_timeout
    }

    /// Execute an HTTP request using reqwest
    pub async fn execute(&self, request: BackendRequest) -> Result<BackendResponse> {
        match request.url.scheme() {
            "http" | "https" => {}
            _ => {
                return Err(Error::InvalidUrl);
            }
        }

        let read_timeout = request.read_timeout.or(self.settings.read_timeout);
        let response = follow_redirects(
            request.method,
            request.url,
            request.headers,
            read_timeout,
            |method, url, headers, with_body| {
                self.request_builder(
                    method,
                    url,
                    headers,
                    request.body.as_ref().filter(|_| with_body),
                    request.progress_callback.as_ref(),
                )
            },
        )
        .await?;

        let headers = response.headers().clone();
        let coding = decodable_coding(&headers);
        let accounting = BodyAccounting::new(
            coding.is_some(),
            crate::backend::types::content_length(&headers),
        );
        Ok(BackendResponse {
            status: response.status(),
            url: response.url().clone(),
            body: Box::pin(ReqwestBody {
                inner: body_items(response.bytes_stream(), coding, accounting.clone()),
                read_timeout,
                idle: None,
                done: false,
            }),
            headers,
            accounting,
        })
    }

    /// The reqwest request for one hop.
    fn request_builder(
        &self,
        method: http::Method,
        url: Url,
        headers: &http::HeaderMap,
        body: Option<&crate::body::Body>,
        progress_callback: Option<&ProgressCallback>,
    ) -> Result<reqwest::RequestBuilder> {
        let mut req_builder = self.client.request(method, url);
        let chosen =
            |headers: &http::HeaderMap| headers.contains_key(http::header::ACCEPT_ENCODING);
        if !chosen(headers) && !self.default_headers.as_ref().is_some_and(chosen) {
            req_builder = req_builder.header(http::header::ACCEPT_ENCODING, ACCEPT_ENCODING);
        }

        for (name, value) in headers {
            req_builder = req_builder.header(name, value);
        }

        let Some(body) = body else {
            return Ok(req_builder);
        };
        if let Some(content_type) = body_content_type(body)
            && !headers.contains_key(http::header::CONTENT_TYPE)
        {
            req_builder = req_builder.header(http::header::CONTENT_TYPE, content_type);
        }

        // A wrapped stream has no length, and hyper would otherwise send it
        // chunked.
        let streamed = |req_builder: reqwest::RequestBuilder, total: u64, body: reqwest::Body| {
            req_builder
                .header(http::header::CONTENT_LENGTH, total)
                .body(body)
        };
        Ok(match body {
            crate::body::Body::Multipart { parts } => {
                let mut form = reqwest::multipart::Form::new();
                for part in parts {
                    let mut part_builder = reqwest::multipart::Part::stream(part.content.clone());

                    if let Some(filename) = &part.filename {
                        part_builder = part_builder.file_name(filename.clone());
                    }

                    if let Some(content_type) = &part.content_type {
                        part_builder = part_builder
                            .mime_str(content_type)
                            .map_err(|e| Error::Internal(format!("Invalid content type: {}", e)))?;
                    }

                    form = form.part(part.name.clone(), part_builder);
                }
                req_builder.multipart(form)
            }
            crate::body::Body::File { file, .. } => streamed(
                req_builder,
                file.len(),
                progress_body(
                    file.chunks(UPLOAD_CHUNK),
                    file.len(),
                    progress_callback.cloned(),
                ),
            ),
            _ => {
                let bytes = body_to_bytes(body)?;
                match progress_callback {
                    Some(callback) => {
                        let total = bytes.len() as u64;
                        streamed(
                            req_builder,
                            total,
                            progress_body(
                                bytes_chunks(bytes, UPLOAD_CHUNK),
                                total,
                                Some(callback.clone()),
                            ),
                        )
                    }
                    None => req_builder.body(bytes),
                }
            }
        })
    }

    /// Get the underlying reqwest client
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Download `url` to `file_path` with a transfer that can be resumed:
    /// in a helper process that outlives this one if the host enabled it
    /// (see [`enable_background_helper`](crate::enable_background_helper)),
    /// and otherwise in the calling task.
    ///
    /// `session_identifier` means nothing to this backend. What a download
    /// leaves on disk is named after its destination alone.
    pub async fn execute_background_download(
        &self,
        url: Url,
        file_path: std::path::PathBuf,
        _session_identifier: Option<String>,
        mut headers: http::HeaderMap,
        progress_callback: Option<crate::backend::types::BoxedProgressCallback>,
        error_for_status: bool,
    ) -> Result<crate::client::download::DownloadResponse> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::InvalidUrl);
        }

        // These change how the download is carried out, so one the client
        // sends by default counts as the caller's own.
        for name in [http::header::ACCEPT_ENCODING, http::header::RANGE] {
            let defaults = self.default_headers.as_ref();
            let default = defaults.and_then(|defaults| defaults.get(&name));
            if let (Some(value), false) = (default, headers.contains_key(&name)) {
                headers.insert(name, value.clone());
            }
        }

        let download = background::Download {
            url,
            headers,
            destination: file_path,
            error_for_status,
            read_timeout: self.settings.read_timeout,
        };
        let progress = progress_callback.as_deref();

        #[cfg(all(
            unix,
            not(any(
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "visionos",
                target_os = "android"
            ))
        ))]
        {
            // The helper has no access to the jar, so it is given the
            // cookies for the URL it starts from. In this process the
            // client takes them from the jar itself, hop by hop.
            let mut job = download.clone();
            if let Some(jar) = self.cookie_jar.as_ref().and_then(|jar| jar.cookie_store()) {
                jar.add_cookie_header(&job.url, &mut job.headers);
            }
            let helped = helper::download(&self.settings, &job, progress).await;
            if let Some(finished) = helped {
                return Ok(finished?.into_response(download.destination));
            }
        }

        let finished = background::run(&self.client, &download, |event| {
            if let (background::Event::Progress { downloaded, total }, Some(progress)) =
                (event, progress)
            {
                progress(downloaded, total);
            }
        })
        .await?;
        Ok(finished.into_response(download.destination))
    }

    /// Get the cookie jar if configured
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        self.cookie_jar.as_ref()
    }
}
