//! A deterministic HTTP/1.1 server for frakt's transfer tests.
//!
//! It listens on 127.0.0.1 in the test's own process, so the tests need no
//! network access and run unchanged on macOS, Windows, Linux and inside an
//! Android app. Every response carries `Connection: close`, so each request
//! has its own connection and the server can tell when the client goes away.
//!
//! Bodies are generated from a seed rather than stored, so any byte range of
//! an arbitrarily large body can be served and checked.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// `Last-Modified` of every generated body.
pub const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

/// Body served by `/encoded`, in identity form.
pub const ENCODED_TEXT: &str = "frakt identity check: this text compresses well. \
    frakt identity check: this text compresses well. \
    frakt identity check: this text compresses well.";

/// The generated body's ETag for a seed and length.
pub fn etag(seed: u64, len: u64) -> String {
    format!("\"frakt-{}-{}\"", seed, len)
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

pub fn fill(seed: u64, offset: u64, buf: &mut [u8]) {
    let mut pos = offset;
    let mut i = 0;
    while i < buf.len() {
        let word = splitmix64(seed.wrapping_mul(0x1000_0000_01B3) ^ (pos / 8)).to_le_bytes();
        let start = (pos % 8) as usize;
        let n = (8 - start).min(buf.len() - i);
        buf[i..i + n].copy_from_slice(&word[start..start + n]);
        i += n;
        pos += n as u64;
    }
}

pub fn body(seed: u64, start: u64, end: u64) -> Vec<u8> {
    let mut buf = vec![0; (end - start) as usize];
    fill(seed, start, &mut buf);
    buf
}

pub fn body_hash(seed: u64, start: u64, end: u64) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0; 1 << 20];
    let mut pos = start;
    while pos < end {
        let n = ((end - pos) as usize).min(buf.len());
        fill(seed, pos, &mut buf[..n]);
        hasher.update(&buf[..n]);
        pos += n as u64;
    }
    hasher.finalize()
}

/// What the server saw of one connection that serves a tracked body.
#[derive(Default)]
pub struct Track {
    /// Body bytes handed to the kernel so far.
    pub written: AtomicU64,
    /// The client closed or reset the connection before the body was sent.
    pub closed_early: AtomicBool,
    /// The whole body was written.
    pub completed: AtomicBool,
    changed: tokio::sync::Notify,
}

impl Track {
    /// Wait until the connection ends, early or not. Returns false on timeout.
    pub async fn wait_finished(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.changed.notified();
            if self.closed_early.load(Ordering::SeqCst) || self.completed.load(Ordering::SeqCst) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    fn finish(&self, early: bool) {
        if early {
            self.closed_early.store(true, Ordering::SeqCst);
        } else {
            self.completed.store(true, Ordering::SeqCst);
        }
        self.changed.notify_waiters();
    }
}

/// A request as the server received it.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_slice())
    }

    pub fn header_str(&self, name: &str) -> Option<String> {
        self.header(name)
            .map(|v| String::from_utf8_lossy(v).into_owned())
    }
}

#[derive(Default)]
struct State {
    requests: Mutex<Vec<Recorded>>,
    tracks: Mutex<HashMap<String, Arc<Track>>>,
    connections: AtomicU64,
}

impl State {
    fn track(&self, id: &str) -> Arc<Track> {
        self.tracks
            .lock()
            .unwrap()
            .entry(id.to_string())
            .or_default()
            .clone()
    }
}

/// A running test server. Dropping it stops it.
pub struct Server {
    addr: SocketAddr,
    tls_addr: SocketAddr,
    state: Arc<State>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Server {
    /// Start plain and TLS listeners on 127.0.0.1. The TLS listener uses a
    /// freshly generated self-signed certificate, so clients must reject it.
    pub async fn start() -> Server {
        let state = Arc::new(State::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tls_addr = tls_listener.local_addr().unwrap();

        let plain_state = state.clone();
        let plain = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                plain_state.connections.fetch_add(1, Ordering::SeqCst);
                let state = plain_state.clone();
                tokio::spawn(async move {
                    let _ = handle(stream, state).await;
                });
            }
        });

        let acceptor = tls_acceptor();
        let tls_state = state.clone();
        let tls = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = tls_listener.accept().await else {
                    continue;
                };
                let acceptor = acceptor.clone();
                let state = tls_state.clone();
                tokio::spawn(async move {
                    if let Ok(stream) = acceptor.accept(stream).await {
                        let _ = handle(stream, state).await;
                    }
                });
            }
        });

        Server {
            addr,
            tls_addr,
            state,
            tasks: vec![plain, tls],
        }
    }

    /// `http://127.0.0.1:<port><path>`
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    /// `https://127.0.0.1:<port><path>` on the self-signed listener.
    pub fn tls_url(&self, path: &str) -> String {
        format!("https://{}{}", self.tls_addr, path)
    }

    /// The tracker for `track=<id>` bodies.
    pub fn track(&self, id: &str) -> Arc<Track> {
        self.state.track(id)
    }

    /// Requests received so far whose query has `id=<id>`.
    pub fn requests_with_id(&self, id: &str) -> Vec<Recorded> {
        let needle = format!("id={}", id);
        self.state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                r.target
                    .split_once('?')
                    .is_some_and(|(_, q)| q.split('&').any(|kv| kv == needle))
            })
            .cloned()
            .collect()
    }

    /// Connections accepted on the plain listener.
    pub fn connections(&self) -> u64 {
        self.state.connections.load(Ordering::SeqCst)
    }
}

fn tls_acceptor() -> tokio_rustls::TlsAcceptor {
    use tokio_rustls::rustls;

    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert = certified.cert.der().clone();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(certified.signing_key.serialize_der().into());
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

struct Request {
    method: String,
    target: String,
    path: String,
    query: HashMap<String, String>,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_slice())
    }

    fn header_str(&self, name: &str) -> Option<String> {
        self.header(name)
            .map(|v| String::from_utf8_lossy(v).trim().to_string())
    }

    fn q<T: std::str::FromStr>(&self, name: &str, default: T) -> T {
        self.query
            .get(name)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }
}

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Request> {
    let mut buf = Vec::with_capacity(4096);
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > 64 * 1024 {
            return Err(io::Error::other("request head too large"));
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = &buf[..head_end];
    let mut lines = head
        .split(|&b| b == b'\n')
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l));
    let request_line = String::from_utf8_lossy(lines.next().unwrap_or_default()).into_owned();
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();

    let mut headers = Vec::new();
    for line in lines {
        if let Some(colon) = line.iter().position(|&b| b == b':') {
            let name = String::from_utf8_lossy(&line[..colon]).trim().to_string();
            let value = line[colon + 1..].trim_ascii().to_vec();
            headers.push((name, value));
        }
    }

    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_string(), query),
        None => (target.clone(), ""),
    };
    let query = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (
                k.to_string(),
                urlencoding::decode(v)
                    .map(|v| v.into_owned())
                    .unwrap_or_default(),
            )
        })
        .collect();

    let mut request = Request {
        method,
        target,
        path,
        query,
        headers,
        body: buf[head_end + 4..].to_vec(),
    };

    let content_length: usize = request
        .header_str("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while request.body.len() < content_length {
        let mut chunk = vec![0u8; 64 * 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        request.body.extend_from_slice(&chunk[..n]);
    }

    Ok(request)
}

async fn write_head<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: &str,
    headers: &[(&str, String)],
) -> io::Result<()> {
    let mut head = format!("HTTP/1.1 {}\r\nConnection: close\r\n", status);
    for (name, value) in headers {
        head.push_str(&format!("{}: {}\r\n", name, value));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await
}

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    state: Arc<State>,
) -> io::Result<()> {
    let request = read_request(&mut stream).await?;
    state.requests.lock().unwrap().push(Recorded {
        method: request.method.clone(),
        target: request.target.clone(),
        headers: request.headers.clone(),
        body: request.body.clone(),
    });

    match request.path.as_str() {
        "/bytes" => serve_bytes(&mut stream, &request, &state).await?,
        "/truncate" => return serve_truncated(&mut stream, &request).await,
        "/stall-headers" => {
            let track = state.track(&request.q("track", String::from("stall-headers")));
            // Wait for the client to give up, then report the connection gone.
            let mut probe = [0u8; 1];
            let ms = request.q("ms", 10_000u64);
            let gone =
                tokio::time::timeout(Duration::from_millis(ms), stream.read(&mut probe)).await;
            if matches!(gone, Ok(Ok(0)) | Ok(Err(_))) {
                track.finish(true);
                return Ok(());
            }
            write_head(&mut stream, "200 OK", &[("Content-Length", "2".into())]).await?;
            stream.write_all(b"ok").await?;
            track.finish(false);
        }
        "/stall-body" => {
            let len: u64 = request.q("len", 1 << 20);
            let after: u64 = request.q("after", 1 << 16);
            let ms: u64 = request.q("ms", 5_000);
            write_head(
                &mut stream,
                "200 OK",
                &[("Content-Length", len.to_string())],
            )
            .await?;
            stream.write_all(&body(1, 0, after)).await?;
            stream.flush().await?;
            tokio::time::sleep(Duration::from_millis(ms)).await;
            stream.write_all(&body(1, after, len)).await?;
        }
        "/trickle" => {
            let pieces: u64 = request.q("pieces", 10);
            let piece: u64 = request.q("piece", 1024);
            let interval: u64 = request.q("interval_ms", 100);
            let len = pieces * piece;
            write_head(
                &mut stream,
                "200 OK",
                &[("Content-Length", len.to_string())],
            )
            .await?;
            for i in 0..pieces {
                stream
                    .write_all(&body(1, i * piece, (i + 1) * piece))
                    .await?;
                stream.flush().await?;
                if i + 1 < pieces {
                    tokio::time::sleep(Duration::from_millis(interval)).await;
                }
            }
        }
        "/redirect" => {
            let status: u16 = request.q("status", 302);
            let to = request.q("to", String::from("/bytes"));
            let reason = match status {
                301 => "Moved Permanently",
                303 => "See Other",
                307 => "Temporary Redirect",
                308 => "Permanent Redirect",
                _ => "Found",
            };
            write_head(
                &mut stream,
                &format!("{} {}", status, reason),
                &[("Location", to), ("Content-Length", "0".into())],
            )
            .await?;
        }
        "/encoded" => {
            let accepts_gzip = request
                .header_str("accept-encoding")
                .is_some_and(|v| v.contains("gzip"));
            if accepts_gzip {
                use std::io::Write;
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(ENCODED_TEXT.as_bytes()).unwrap();
                let gz = encoder.finish().unwrap();
                write_head(
                    &mut stream,
                    "200 OK",
                    &[
                        ("Content-Encoding", "gzip".into()),
                        ("Content-Length", gz.len().to_string()),
                        ("Content-Type", "text/plain".into()),
                    ],
                )
                .await?;
                stream.write_all(&gz).await?;
            } else {
                write_head(
                    &mut stream,
                    "200 OK",
                    &[
                        ("Content-Length", ENCODED_TEXT.len().to_string()),
                        ("Content-Type", "text/plain".into()),
                    ],
                )
                .await?;
                stream.write_all(ENCODED_TEXT.as_bytes()).await?;
            }
        }
        "/nonascii" => {
            // Raw bytes: UTF-8 in one header, a lone Latin-1 byte in another.
            let mut head =
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n".to_vec();
            head.extend_from_slice("X-Utf8: Bjørn Ångström\r\n".as_bytes());
            head.extend_from_slice(b"X-Latin1: caf\xe9\r\n\r\nok");
            stream.write_all(&head).await?;
        }
        "/echo" => {
            let headers: serde_json::Map<String, serde_json::Value> = request
                .headers
                .iter()
                .map(|(n, v)| {
                    (
                        n.to_ascii_lowercase(),
                        serde_json::Value::String(String::from_utf8_lossy(v).into_owned()),
                    )
                })
                .collect();
            let reply = serde_json::json!({
                "method": request.method,
                "headers": headers,
                "body": String::from_utf8_lossy(&request.body),
            })
            .to_string();
            write_head(
                &mut stream,
                "200 OK",
                &[
                    ("Content-Type", "application/json".into()),
                    ("Content-Length", reply.len().to_string()),
                ],
            )
            .await?;
            stream.write_all(reply.as_bytes()).await?;
        }
        "/status" => {
            let code: u16 = request.q("code", 404);
            let text = format!("status {}", code);
            write_head(
                &mut stream,
                &format!("{} Status", code),
                &[("Content-Length", text.len().to_string())],
            )
            .await?;
            stream.write_all(text.as_bytes()).await?;
        }
        _ => {
            write_head(
                &mut stream,
                "404 Not Found",
                &[("Content-Length", "0".into())],
            )
            .await?;
        }
    }

    stream.flush().await?;
    stream.shutdown().await
}

/// `/bytes?len=&seed=&chunk=&track=&etag=`: a generated body with ETag,
/// Last-Modified and byte-range support (`Range` with optional `If-Range`).
async fn serve_bytes<S: AsyncWrite + Unpin>(
    stream: &mut S,
    request: &Request,
    state: &State,
) -> io::Result<()> {
    let len: u64 = request.q("len", 1 << 20);
    let seed: u64 = request.q("seed", 1);
    let chunk: usize = request.q("chunk", 64 * 1024);
    let etag = request.q("etag", etag(seed, len));
    let track = request.query.get("track").map(|id| state.track(id));

    let validator_matches = match request.header_str("if-range") {
        None => true,
        Some(v) => v == etag || v == LAST_MODIFIED,
    };
    let range = request
        .header_str("range")
        .filter(|_| validator_matches)
        .and_then(|r| {
            let spec = r.strip_prefix("bytes=")?;
            let (start, end) = spec.split_once('-')?;
            let start: u64 = start.parse().ok()?;
            let end: Option<u64> = if end.is_empty() {
                None
            } else {
                end.parse().ok()
            };
            Some((start, end))
        });

    let common = [
        ("ETag", etag.clone()),
        ("Last-Modified", LAST_MODIFIED.to_string()),
        ("Accept-Ranges", "bytes".to_string()),
        ("Content-Type", "application/octet-stream".to_string()),
    ];

    let (start, end) = match range {
        Some((start, _)) if start >= len => {
            let mut headers = common.to_vec();
            headers.push(("Content-Range", format!("bytes */{}", len)));
            headers.push(("Content-Length", "0".into()));
            write_head(stream, "416 Range Not Satisfiable", &headers).await?;
            return Ok(());
        }
        Some((start, end)) => {
            let end = end.map(|e| (e + 1).min(len)).unwrap_or(len);
            let mut headers = common.to_vec();
            headers.push((
                "Content-Range",
                format!("bytes {}-{}/{}", start, end - 1, len),
            ));
            headers.push(("Content-Length", (end - start).to_string()));
            write_head(stream, "206 Partial Content", &headers).await?;
            (start, end)
        }
        None => {
            let mut headers = common.to_vec();
            headers.push(("Content-Length", len.to_string()));
            write_head(stream, "200 OK", &headers).await?;
            (0, len)
        }
    };

    if request.method == "HEAD" {
        return Ok(());
    }

    let mut buf = vec![0u8; chunk];
    let mut pos = start;
    while pos < end {
        let n = ((end - pos) as usize).min(buf.len());
        fill(seed, pos, &mut buf[..n]);
        if let Err(e) = stream.write_all(&buf[..n]).await {
            if let Some(track) = &track {
                track.finish(true);
            }
            return Err(e);
        }
        pos += n as u64;
        if let Some(track) = &track {
            track.written.fetch_add(n as u64, Ordering::SeqCst);
        }
    }
    if let Some(track) = &track {
        track.finish(false);
    }
    Ok(())
}

/// `/truncate?len=&after=&chunked=`: promise `len` bytes (or use chunked
/// encoding), send `after` of them, then drop the connection.
async fn serve_truncated<S: AsyncWrite + Unpin>(
    stream: &mut S,
    request: &Request,
) -> io::Result<()> {
    let len: u64 = request.q("len", 4 << 20);
    let after: u64 = request.q("after", 1 << 20);
    let chunked: bool = request.q("chunked", 0u8) == 1;
    if chunked {
        write_head(stream, "200 OK", &[("Transfer-Encoding", "chunked".into())]).await?;
        let mut pos = 0;
        while pos < after {
            let n = (after - pos).min(32 * 1024);
            stream.write_all(format!("{:x}\r\n", n).as_bytes()).await?;
            stream.write_all(&body(1, pos, pos + n)).await?;
            stream.write_all(b"\r\n").await?;
            pos += n;
        }
    } else {
        write_head(stream, "200 OK", &[("Content-Length", len.to_string())]).await?;
        stream.write_all(&body(1, 0, after)).await?;
    }
    stream.flush().await?;
    // Return without the final chunk or the promised bytes; dropping the
    // stream closes the connection.
    Ok(())
}
