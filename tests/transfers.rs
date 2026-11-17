//! Transfer tests against a local server, run once per backend that can run
//! on this host: Foundation and reqwest on macOS, WinHTTP and reqwest on
//! Windows, Cronet on Android (through vampire), reqwest on Linux.
//!
//! They check what Pahkat relies on: every byte delivered exactly once with
//! bounded buffering, failures surfacing as errors rather than a clean end,
//! byte-range resumption, cancellation on drop, and errors that can be told
//! apart.
//!
//! Tests take a process-wide lock so that memory and timing measurements are
//! not disturbed by other tests.
//!
//! On Android the suite is compiled into the library and run by vampire
//! (`vampire test --test transfers::android::<name>`), which builds with the
//! default features, so add `backend-android` to them for the run. The
//! vampire host runs
//! tests on its main thread during app startup, which Android kills after
//! about 15 seconds, so run tests one at a time. A headless emulator also
//! needs `dumpsys deviceidle disable` and
//! `settings put global hidden_api_policy 1`, or the app loses network
//! access when the device idles and cannot permit cleartext HTTP.

#[path = "support/server.rs"]
mod server;

#[cfg(vampire)]
use crate as frakt;

use frakt::{
    Auth, BackendType, BodyIntegrity, Client, ClientBuilder, Cookie, CookieAcceptPolicy, CookieJar,
    Error,
};
use server::Server;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

const MIB: u64 = 1 << 20;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn client(backend: BackendType) -> Client {
    client_with(backend, |builder| builder)
}

/// The vampire host app targets an SDK that blocks cleartext HTTP by default,
/// and the test server is plain HTTP on 127.0.0.1. The setter is a hidden
/// API.
#[cfg(all(target_os = "android", vampire, feature = "backend-android"))]
fn permit_cleartext() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let vm = unsafe { jni::JavaVM::from_raw(vampire::java_vm() as *mut _) };
        vm.attach_current_thread(|env| -> jni::errors::Result<()> {
            let policy = env
                .call_static_method(
                    jni::jni_str!("android/security/NetworkSecurityPolicy"),
                    jni::jni_str!("getInstance"),
                    jni::jni_sig!("()Landroid/security/NetworkSecurityPolicy;"),
                    &[],
                )
                .and_then(|v| v.l())?;
            env.call_method(
                &policy,
                jni::jni_str!("setCleartextTrafficPermitted"),
                jni::jni_sig!("(Z)V"),
                &[true.into()],
            )?;
            Ok(())
        })
        .unwrap();
    });
}

fn client_with(
    backend: BackendType,
    configure: impl FnOnce(ClientBuilder) -> ClientBuilder,
) -> Client {
    #[cfg(all(target_os = "android", vampire, feature = "backend-android"))]
    permit_cleartext();
    configure(Client::builder().backend(backend))
        .build()
        .expect("client")
}

/// Pahkat spawns transfers onto a multi-threaded runtime, so the client,
/// its futures and its bodies must be Send.
#[allow(dead_code)]
fn send_bounds(client: &Client) {
    fn assert_send<T: Send>(_: &T) {}
    fn assert_sync<T: Sync>(_: &T) {}
    assert_send(client);
    assert_sync(client);
    let send = client.get("http://127.0.0.1:1/").unwrap().send();
    assert_send(&send);
    let download = client
        .download("http://127.0.0.1:1/", "unused")
        .unwrap()
        .send();
    assert_send(&download);
}

#[allow(dead_code)]
fn body_bounds(response: frakt::Response) {
    fn assert_send<T: Send>(_: &T) {}
    fn assert_sync<T: Sync>(_: &T) {}
    assert_sync(&response);
    let stream = response.stream();
    assert_send(&stream);
}

fn rss() -> u64 {
    memory_stats::memory_stats()
        .map(|stats| stats.physical_mem as u64)
        .unwrap_or(0)
}

fn header(response: &frakt::Response, name: &str) -> String {
    response
        .headers()
        .get(name)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_else(|| panic!("missing {} header", name))
}

/// Read a body to the end, returning its length and BLAKE3, or the error
/// that ended it early along with the length read so far.
async fn drain(response: frakt::Response) -> Result<(u64, blake3::Hash), (u64, Error)> {
    let mut stream = response.stream();
    let mut hasher = blake3::Hasher::new();
    let mut total = 0u64;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => {
                total += chunk.len() as u64;
                hasher.update(&chunk);
            }
            Err(error) => return Err((total, error)),
        }
    }
    Ok((total, hasher.finalize()))
}

/// `require_checked_body` must refuse a response whose integrity is
/// unverifiable, before its body is read.
async fn assert_refuses_unverifiable(client: &Client, url: &str) {
    match client.get(url).unwrap().require_checked_body().send().await {
        Err(Error::UnverifiableBody(response)) => {
            assert_eq!(response.body_integrity(), BodyIntegrity::Unverifiable)
        }
        other => panic!(
            "require_checked_body accepted an unverifiable body: {:?}",
            other.map(|r| r.status())
        ),
    }
}

async fn require_checked_body_matches_integrity(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);

    // A Content-Length is checked by every backend.
    let url = server.url("/bytes?len=100000");
    let response = client
        .get(url.as_str())
        .unwrap()
        .require_checked_body()
        .send()
        .await
        .unwrap();
    assert_eq!(response.body_integrity(), BodyIntegrity::Checked);
    assert_eq!(drain(response).await.unwrap().0, 100_000);

    // Chunked framing is checked by every stack but CFNetwork.
    for keepalive in [0, 1] {
        let url = server.url(&format!("/chunked?len=100000&keepalive={}", keepalive));
        let integrity = client
            .get(url.as_str())
            .unwrap()
            .send()
            .await
            .unwrap()
            .body_integrity();
        println!("{:?}: chunked body integrity {:?}", backend, integrity);
        match integrity {
            BodyIntegrity::Checked => {
                let response = client
                    .get(url.as_str())
                    .unwrap()
                    .require_checked_body()
                    .send()
                    .await
                    .unwrap();
                assert_eq!(drain(response).await.unwrap().0, 100_000);
            }
            BodyIntegrity::Unverifiable => assert_refuses_unverifiable(&client, &url).await,
        }
    }
}

async fn large_body_is_byte_exact(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = 64 * MIB;

    let started = Instant::now();
    let response = client
        .get(server.url(&format!("/bytes?len={}&seed=7", len)).as_str())
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(header(&response, "content-length"), len.to_string());
    let (total, hash) = drain(response).await.unwrap();
    assert_eq!(total, len);
    assert_eq!(hash, server::body_hash(7, 0, len));
    println!(
        "{:?}: {} MiB as a stream in {:?}",
        backend,
        len / MIB,
        started.elapsed()
    );

    // The same through AsyncRead, as Pahkat reads it.
    let len = 16 * MIB;
    let response = client
        .get(server.url(&format!("/bytes?len={}&seed=8", len)).as_str())
        .unwrap()
        .send()
        .await
        .unwrap();
    let mut reader = response.stream();
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 100_000];
    let mut total = 0u64;
    loop {
        let n = reader.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        total += n as u64;
        hasher.update(&buf[..n]);
    }
    assert_eq!(total, len);
    assert_eq!(hasher.finalize(), server::body_hash(8, 0, len));
}

async fn slow_consumer_gets_every_byte_with_bounded_buffering(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    // A read timeout shorter than the consumer's first pause: time spent not
    // reading must not count against it.
    let client = client_with(backend, |b| b.read_timeout(Duration::from_secs(1)));
    let len = 48 * MIB;
    let rate = 16.0 * MIB as f64;

    let response = client
        .get(
            server
                .url(&format!("/bytes?len={}&seed=3&track=slow", len))
                .as_str(),
        )
        .unwrap()
        .send()
        .await
        .unwrap();
    let track = server.track("slow");
    let consumed = Arc::new(AtomicU64::new(0));
    let max_in_flight = Arc::new(AtomicU64::new(0));
    let rss_start = rss();
    let max_rss = Arc::new(AtomicU64::new(rss_start));

    let sampler = {
        let track = track.clone();
        let consumed = consumed.clone();
        let max_in_flight = max_in_flight.clone();
        let max_rss = max_rss.clone();
        tokio::spawn(async move {
            loop {
                let in_flight = track
                    .written
                    .load(Ordering::SeqCst)
                    .saturating_sub(consumed.load(Ordering::SeqCst));
                max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
                max_rss.fetch_max(rss(), Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    let mut stream = response.stream();
    let mut hasher = blake3::Hasher::new();
    let mut paused = false;
    let mut started = Instant::now();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        hasher.update(&chunk);
        let total = consumed.fetch_add(chunk.len() as u64, Ordering::SeqCst) + chunk.len() as u64;
        if !paused {
            paused = true;
            tokio::time::sleep(Duration::from_millis(1500)).await;
            started = Instant::now();
        } else {
            let due = started + Duration::from_secs_f64(total as f64 / rate);
            tokio::time::sleep_until(due.into()).await;
        }
    }
    sampler.abort();

    assert_eq!(consumed.load(Ordering::SeqCst), len);
    assert_eq!(hasher.finalize(), server::body_hash(3, 0, len));

    let max_in_flight = max_in_flight.load(Ordering::SeqCst);
    let rss_growth = max_rss.load(Ordering::SeqCst).saturating_sub(rss_start);
    println!(
        "{:?}: slow consumer, at most {:.1} MiB sent but unread, RSS grew {:.1} MiB",
        backend,
        max_in_flight as f64 / MIB as f64,
        rss_growth as f64 / MIB as f64
    );
    // Sent-but-unread bytes sit in socket buffers and the client's own
    // buffers; an unbounded client lets this approach the whole body.
    assert!(
        max_in_flight < 16 * MIB,
        "{} bytes were sent but not yet read",
        max_in_flight
    );
}

async fn range_and_if_range(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = 5 * MIB + 17;
    let seed = 11;
    let url = server.url(&format!("/bytes?len={}&seed={}", len, seed));

    let full = client.get(url.as_str()).unwrap().send().await.unwrap();
    assert_eq!(full.status(), 200);
    let etag = header(&full, "etag");
    let last_modified = header(&full, "last-modified");
    assert_eq!(etag, server::etag(seed, len));
    assert_eq!(last_modified, server::LAST_MODIFIED);
    assert_eq!(header(&full, "content-length"), len.to_string());
    drop(full);

    let offset = 1_234_567;
    for validator in [&etag, &last_modified] {
        let response = client
            .get(url.as_str())
            .unwrap()
            .header("Range", format!("bytes={}-", offset))
            .unwrap()
            .header("If-Range", validator.as_str())
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 206, "If-Range: {}", validator);
        assert_eq!(
            header(&response, "content-range"),
            format!("bytes {}-{}/{}", offset, len - 1, len)
        );
        assert_eq!(
            header(&response, "content-length"),
            (len - offset).to_string()
        );
        assert_eq!(header(&response, "etag"), etag);
        let (total, hash) = drain(response).await.unwrap();
        assert_eq!(total, len - offset);
        assert_eq!(hash, server::body_hash(seed, offset, len));
    }

    // A stale validator gets the whole current body.
    let response = client
        .get(url.as_str())
        .unwrap()
        .header("Range", format!("bytes={}-", offset))
        .unwrap()
        .header("If-Range", "\"stale\"")
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().get("content-range").is_none());
    let (total, hash) = drain(response).await.unwrap();
    assert_eq!(total, len);
    assert_eq!(hash, server::body_hash(seed, 0, len));

    // Resuming at the end: nothing left to send.
    let response = client
        .get(url.as_str())
        .unwrap()
        .header("Range", format!("bytes={}-", len))
        .unwrap()
        .header("If-Range", etag.as_str())
        .unwrap()
        .allow_error_status()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 416);
    assert_eq!(
        header(&response, "content-range"),
        format!("bytes */{}", len)
    );
}

async fn mid_body_drop_is_an_error(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = 4 * MIB;

    // Complete chunked bodies, which the cases below must not be confused
    // with, end cleanly with every byte.
    for keepalive in [0, 1] {
        let url = server.url(&format!("/chunked?len={}&keepalive={}", len, keepalive));
        let response = client.get(url.as_str()).unwrap().send().await.unwrap();
        let (total, hash) = drain(response).await.unwrap_or_else(|(n, e)| {
            panic!(
                "complete chunked body (keep-alive {}) failed after {} bytes: {}",
                keepalive, n, e
            )
        });
        assert_eq!(total, len);
        assert_eq!(hash, server::body_hash(1, 0, len));
    }

    let cases = [
        (
            "Content-Length",
            format!("/truncate?len={}&after={}&chunked=0", len, MIB),
        ),
        (
            "chunked, close, FIN",
            format!("/chunked?len={}&cut={}", len, MIB),
        ),
        (
            "chunked, keep-alive, FIN",
            format!("/chunked?len={}&cut={}&keepalive=1", len, MIB),
        ),
        // A reset is sent after a cut small enough to sit in the socket
        // buffers: one that meets full receive buffers can arrive late (on
        // macOS loopback, for every client).
        (
            "chunked, close, RST",
            format!("/chunked?len={}&cut={}&rst=1", len, 64 * 1024),
        ),
        (
            "chunked, keep-alive, RST",
            format!("/chunked?len={}&cut={}&keepalive=1&rst=1", len, 64 * 1024),
        ),
    ];
    for (case, path) in &cases {
        let url = server.url(path);

        // A stack may notice the drop before it hands over the response;
        // failing send() is as good as failing the body.
        let unverifiable = match client.get(url.as_str()).unwrap().send().await {
            Err(error) => {
                println!(
                    "{:?}: truncated ({}) failed before the body: {}",
                    backend, case, error
                );
                false
            }
            Ok(response) => {
                let integrity = response.body_integrity();
                match drain(response).await {
                    Ok((total, _)) if integrity == BodyIntegrity::Unverifiable => {
                        println!(
                            "{:?}: truncated ({}) ended cleanly after {} bytes, as its Unverifiable integrity warned",
                            backend, case, total
                        );
                        true
                    }
                    Ok((total, _)) => panic!(
                        "truncated body ({}, {:?}) ended cleanly after {} bytes",
                        case, integrity, total
                    ),
                    Err((total, error)) => {
                        assert!(
                            total <= MIB,
                            "read {} bytes of a 1 MiB truncated body",
                            total
                        );
                        println!("{:?}: truncated ({}): {}", backend, case, error);
                        false
                    }
                }
            }
        };

        if unverifiable {
            assert_refuses_unverifiable(&client, &url).await;
            continue;
        }
        if let Ok(response) = client.get(url.as_str()).unwrap().send().await {
            let mut reader = response.stream();
            let result = tokio::io::copy(&mut reader, &mut tokio::io::sink()).await;
            assert!(
                result.is_err(),
                "AsyncRead reported a clean end of a truncated body ({}): {:?}",
                case,
                result
            );
        }
    }
}

async fn dropping_the_stream_cancels_the_transfer(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = 1 << 30;

    let response = client
        .get(
            server
                .url(&format!("/bytes?len={}&seed=5&track=drop", len))
                .as_str(),
        )
        .unwrap()
        .send()
        .await
        .unwrap();
    let mut stream = response.stream();
    let mut read = 0;
    while read < 4 * MIB {
        read += stream.next().await.unwrap().unwrap().len() as u64;
    }
    drop(stream);
    let rss_at_drop = rss();
    let dropped = Instant::now();

    let track = server.track("drop");
    assert!(
        track.wait_finished(Duration::from_secs(10)).await,
        "the server was still sending 10 s after the stream was dropped"
    );
    let closed_after = dropped.elapsed();
    assert!(
        track.closed_early.load(Ordering::SeqCst),
        "the whole body was sent after the stream was dropped"
    );
    let written = track.written.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let rss_growth = rss().saturating_sub(rss_at_drop);
    println!(
        "{:?}: connection closed {:?} after drop, {:.1} MiB sent in total, RSS grew {:.1} MiB after drop",
        backend,
        closed_after,
        written as f64 / MIB as f64,
        rss_growth as f64 / MIB as f64
    );
    assert!(written < 64 * MIB, "{} bytes were sent", written);
    assert!(
        rss_growth < 32 * MIB,
        "RSS grew {} bytes after the drop",
        rss_growth
    );

    // Dropping the send() future before the response starts.
    let send = client
        .get(server.url("/stall-headers?ms=10000&track=early").as_str())
        .unwrap()
        .send();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), send)
            .await
            .is_err()
    );
    let track = server.track("early");
    assert!(
        track.wait_finished(Duration::from_secs(5)).await
            && track.closed_early.load(Ordering::SeqCst),
        "the connection stayed open after the send() future was dropped"
    );
}

async fn identity_encoding_is_honoured(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = MIB as usize;

    // An explicit identity gets the body as stored.
    let response = client
        .get(
            server
                .url(&format!("/gzip?len={}&id=explicit", len))
                .as_str(),
        )
        .unwrap()
        .header("Accept-Encoding", "identity")
        .unwrap()
        .send()
        .await
        .unwrap();
    assert!(!response.is_decoded());
    assert!(response.headers().get("content-encoding").is_none());
    assert_eq!(header(&response, "content-length"), len.to_string());
    assert_eq!(response.expected_wire_length(), Some(len as u64));
    let mut stream = response.stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(body, server::text_body(len));
    let stats = stream.stats();
    assert_eq!(stats.delivered_bytes, len as u64);
    if let Some(wire) = stats.wire_bytes {
        assert_eq!(wire, len as u64);
    }
    let seen = server.requests_with_id("explicit");
    assert_eq!(
        seen[0].header_str("accept-encoding").as_deref(),
        Some("identity")
    );

    // A Range request without an explicit Accept-Encoding goes out as
    // identity, so the range addresses the stored bytes.
    let response = client
        .get(server.url("/bytes?len=100000&id=range").as_str())
        .unwrap()
        .header("Range", "bytes=1000-")
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert!(!response.is_decoded());
    drop(response);
    let seen = server.requests_with_id("range");
    assert_eq!(
        seen[0].header_str("accept-encoding").as_deref(),
        Some("identity"),
        "Range request"
    );

    // An explicit choice on a Range request is left alone.
    let response = client
        .get(server.url("/bytes?len=100000&id=range-gzip").as_str())
        .unwrap()
        .header("Range", "bytes=1000-")
        .unwrap()
        .header("Accept-Encoding", "gzip")
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    drop(response);
    let seen = server.requests_with_id("range-gzip");
    let accept = seen[0].header_str("accept-encoding").unwrap_or_default();
    assert!(
        accept.contains("gzip"),
        "Range request with explicit gzip sent {:?}",
        accept
    );
}

async fn compressed_bodies_are_decoded_and_counted(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = 4 * MIB as usize;
    let wire_len = server::gzip_body(len).len() as u64;

    for chunked in [false, true] {
        let id = format!("compressed-{}", chunked as u8);
        let response = client
            .get(
                server
                    .url(&format!(
                        "/gzip?len={}&chunked={}&id={}",
                        len, chunked as u8, id
                    ))
                    .as_str(),
            )
            .unwrap()
            .send()
            .await
            .unwrap();
        let accept = server.requests_with_id(&id)[0]
            .header_str("accept-encoding")
            .unwrap_or_default();
        assert!(
            accept.contains("gzip"),
            "default request sent Accept-Encoding {:?}",
            accept
        );
        assert!(response.is_decoded(), "gzip body not reported as decoded");
        if chunked {
            assert_eq!(response.expected_wire_length(), None);
        } else if let Some(expected) = response.expected_wire_length() {
            assert_eq!(expected, wire_len);
        }

        let mut stream = response.stream();
        let mut hasher = blake3::Hasher::new();
        while let Some(chunk) = stream.next().await {
            hasher.update(&chunk.unwrap());
        }
        assert_eq!(hasher.finalize(), blake3::hash(&server::text_body(len)));
        let stats = stream.stats();
        assert_eq!(stats.delivered_bytes, len as u64);
        if let Some(wire) = stats.wire_bytes {
            assert_eq!(wire, wire_len, "wire bytes (chunked: {})", chunked);
        }
        println!(
            "{:?}: gzip {} -> {} bytes (chunked: {}): {:?}",
            backend, wire_len, len, chunked, stats
        );
    }
}

async fn compressed_body_cut_short_is_an_error(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = 4 * MIB as usize;
    let cut = server::gzip_body(len).len() / 2;

    for chunked in [false, true] {
        let url = server.url(&format!(
            "/gzip?len={}&chunked={}&cut={}",
            len, chunked as u8, cut
        ));
        match client.get(url.as_str()).unwrap().send().await {
            Err(error) => println!(
                "{:?}: cut gzip (chunked: {}) failed before the body: {}",
                backend, chunked, error
            ),
            Ok(response) => {
                let integrity = response.body_integrity();
                match drain(response).await {
                    Ok((total, _)) if integrity == BodyIntegrity::Unverifiable => {
                        println!(
                            "{:?}: cut gzip (chunked: {}) ended cleanly after {} bytes, as its Unverifiable integrity warned",
                            backend, chunked, total
                        );
                        assert_refuses_unverifiable(&client, &url).await;
                    }
                    Ok((total, _)) => panic!(
                        "gzip body cut at {} wire bytes (chunked: {}, {:?}) ended cleanly after {} bytes",
                        cut, chunked, integrity, total
                    ),
                    Err((total, error)) => println!(
                        "{:?}: cut gzip (chunked: {}) failed after {} bytes: {}",
                        backend, chunked, total, error
                    ),
                }
            }
        }
    }
}

async fn redirect_keeps_range(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = MIB;
    let offset = 1000;

    for status in [301, 302, 307, 308] {
        let target = format!("/bytes?len={}&seed=9", len);
        let url = server.url(&format!(
            "/redirect?status={}&to={}",
            status,
            urlencoding::encode(&target)
        ));
        let response = client
            .get(url.as_str())
            .unwrap()
            .header("Range", format!("bytes={}-", offset))
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 206, "after a {} redirect", status);
        assert_eq!(
            response.url().path(),
            "/bytes",
            "final URL after {}",
            status
        );
        assert_eq!(
            header(&response, "content-range"),
            format!("bytes {}-{}/{}", offset, len - 1, len)
        );
        let (total, hash) = drain(response).await.unwrap();
        assert_eq!(total, len - offset);
        assert_eq!(hash, server::body_hash(9, offset, len));
    }
}

async fn non_ascii_header_values_do_not_panic(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client_with(backend, |b| b.header("X-Default", "café").unwrap());

    let response = client
        .get(server.url("/nonascii").as_str())
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    println!(
        "{:?}: X-Utf8 {:?}, X-Latin1 {:?}",
        backend,
        response.headers().get("x-utf8"),
        response.headers().get("x-latin1")
    );
    assert_eq!(response.text().await.unwrap(), "ok");

    let response = client
        .get(server.url("/echo?id=nonascii").as_str())
        .unwrap()
        .header("X-Name", "Bjørn")
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let seen = server.requests_with_id("nonascii");
    println!(
        "{:?}: server saw X-Name {:?}, X-Default {:?}",
        backend,
        seen[0].header_str("x-name"),
        seen[0].header_str("x-default")
    );
}

async fn errors_are_classified(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);

    let dns = client
        .get("http://frakt-test-host.invalid/")
        .unwrap()
        .send()
        .await
        .unwrap_err();
    println!("{:?}: DNS failure: {:?}", backend, dns);
    assert!(
        matches!(dns, Error::Network { .. }),
        "DNS failure: {:?}",
        dns
    );

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let refused = client
        .get(format!("http://127.0.0.1:{}/", port).as_str())
        .unwrap()
        .send()
        .await
        .unwrap_err();
    println!("{:?}: connection refused: {:?}", backend, refused);
    assert!(
        matches!(refused, Error::Network { .. }),
        "connection refused: {:?}",
        refused
    );

    let tls = client
        .get(server.tls_url("/status?code=200").as_str())
        .unwrap()
        .send()
        .await
        .unwrap_err();
    println!("{:?}: self-signed certificate: {:?}", backend, tls);
    assert!(
        matches!(tls, Error::Tls { .. }),
        "bad certificate: {:?}",
        tls
    );

    match client
        .get(server.url("/status?code=404").as_str())
        .unwrap()
        .send()
        .await
    {
        Err(Error::HttpError(response)) => {
            assert_eq!(response.status(), 404);
            assert_eq!(response.text().await.unwrap(), "status 404");
        }
        other => panic!("404: {:?}", other),
    }

    let response = client
        .get(server.url("/status?code=503").as_str())
        .unwrap()
        .allow_error_status()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
}

async fn timeouts(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let short = client_with(backend, |b| b.read_timeout(Duration::from_secs(1)));

    // The body stalls longer than the read timeout. Only the body is
    // timed: connection setup is not what this checks.
    let response = short
        .get(
            server
                .url("/stall-body?len=1048576&after=65536&ms=6000")
                .as_str(),
        )
        .unwrap()
        .send()
        .await
        .unwrap();
    let started = Instant::now();
    match drain(response).await {
        Err((_, Error::Timeout)) => {}
        other => panic!("stalled body: {:?}", other.map(|(n, _)| n)),
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // The response never starts.
    let started = Instant::now();
    let result = short
        .get(server.url("/stall-headers?ms=6000").as_str())
        .unwrap()
        .send()
        .await;
    assert!(matches!(result, Err(Error::Timeout)), "{:?}", result);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // A slow but steady body takes longer than the read timeout in total
    // and must not trip it.
    let trickle = server.url("/trickle?pieces=8&piece=1024&interval_ms=300");
    let response = short.get(trickle.as_str()).unwrap().send().await.unwrap();
    assert_eq!(drain(response).await.unwrap().0, 8 * 1024);

    // A total deadline does cut it off, set on the client or the request.
    let deadline = client_with(backend, |b| b.timeout(Duration::from_secs(1)));
    let started = Instant::now();
    let result = match deadline.get(trickle.as_str()).unwrap().send().await {
        Ok(response) => drain(response).await.map(|(n, _)| n).map_err(|(_, e)| e),
        Err(error) => Err(error),
    };
    assert!(matches!(result, Err(Error::Timeout)), "{:?}", result);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );

    let result = match client(backend)
        .get(trickle.as_str())
        .unwrap()
        .timeout(Duration::from_secs(1))
        .send()
        .await
    {
        Ok(response) => drain(response).await.map(|(n, _)| n).map_err(|(_, e)| e),
        Err(error) => Err(error),
    };
    assert!(matches!(result, Err(Error::Timeout)), "{:?}", result);

    // A per-request read timeout overrides the client's.
    let response = client(backend)
        .get(
            server
                .url("/stall-body?len=1048576&after=65536&ms=6000")
                .as_str(),
        )
        .unwrap()
        .read_timeout(Duration::from_secs(1))
        .send()
        .await
        .unwrap();
    let started = Instant::now();
    match drain(response).await {
        Err((_, Error::Timeout)) => {}
        other => panic!("stalled body: {:?}", other.map(|(n, _)| n)),
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}

async fn small_json_requests(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client_with(backend, |b| b.user_agent("pahkat-test/1.0"));

    let reply: serde_json::Value = client
        .get(server.url("/echo").as_str())
        .unwrap()
        .header("Accept", "application/json")
        .unwrap()
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reply["method"], "GET");
    assert_eq!(reply["headers"]["user-agent"], "pahkat-test/1.0");

    let reply: serde_json::Value = client
        .post(server.url("/echo").as_str())
        .unwrap()
        .json(serde_json::json!({ "package": "pahkat", "version": [1, 2, 3] }))
        .unwrap()
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reply["method"], "POST");
    assert_eq!(reply["headers"]["content-type"], "application/json");
    let body: serde_json::Value = serde_json::from_str(reply["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["package"], "pahkat");
    assert_eq!(body["version"][2], 3);
}

async fn progress_callbacks(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let len = 3 * MIB;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("download.bin");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let download = client
        .download(
            server.url(&format!("/bytes?len={}&seed=4", len)).as_str(),
            &path,
        )
        .unwrap()
        .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
        .send()
        .await
        .unwrap();
    assert_eq!(download.bytes_downloaded, len);
    assert_eq!(
        blake3::hash(&std::fs::read(&path).unwrap()),
        server::body_hash(4, 0, len)
    );
    let calls = calls.lock().unwrap().clone();
    assert!(calls.windows(2).all(|w| w[0].0 <= w[1].0));
    assert_eq!(calls.last(), Some(&(len, Some(len))));

    let upload = vec![0x5a; (2 * MIB) as usize];
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let response = client
        .post(server.url("/echo").as_str())
        .unwrap()
        .body(upload)
        .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    let calls = calls.lock().unwrap().clone();
    assert!(calls.windows(2).all(|w| w[0].0 <= w[1].0), "{:?}", calls);
    assert_eq!(calls.last(), Some(&(2 * MIB, Some(2 * MIB))), "{:?}", calls);
}

fn temp_dir() -> tempfile::TempDir {
    if cfg!(target_os = "android") {
        tempfile::tempdir_in("/data/data/com.vampire.host/cache").unwrap()
    } else {
        tempfile::tempdir().unwrap()
    }
}

fn encode(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// The server's address under another host name: the same listener, but a
/// different origin to a client.
fn other_origin(url: &str) -> String {
    url.replace("127.0.0.1", "localhost")
}

/// Whether `backend` can reach the server under a second host name here.
/// reqwest's resolver finds no address for `localhost` inside the Android
/// emulator, where only the loopback address itself works.
fn has_second_origin(backend: BackendType) -> bool {
    #[cfg(all(target_os = "android", feature = "backend-reqwest"))]
    if matches!(backend, BackendType::Reqwest) {
        return false;
    }
    let _ = backend;
    true
}

/// GET `url` and return what `/echo` says it received.
async fn echoed(client: &Client, url: &str) -> serde_json::Value {
    let response = client.get(url).unwrap().send().await.unwrap();
    assert_eq!(response.status(), 200, "{}", url);
    response.json().await.unwrap()
}

/// The request's `Cookie` header as `/echo` reported it, or "".
fn echoed_cookie(echo: &serde_json::Value) -> String {
    echo["headers"]["cookie"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

fn cookie_pairs(jar: &CookieJar, url: &str) -> Vec<(String, String)> {
    let mut pairs: Vec<_> = jar
        .cookies_for_url(url)
        .unwrap()
        .into_iter()
        .map(|cookie| (cookie.name, cookie.value))
        .collect();
    pairs.sort();
    pairs
}

fn pair(name: &str, value: &str) -> (String, String) {
    (name.to_string(), value.to_string())
}

/// A redirect to another origin must not take the caller's credentials
/// with it; one within the origin keeps them.
async fn redirects_do_not_leak_credentials(backend: BackendType) {
    if !has_second_origin(backend) {
        return;
    }
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let elsewhere = other_origin(&server.url("/echo"));

    let send = |url: String| {
        let request = client
            .get(url.as_str())
            .unwrap()
            .auth(Auth::bearer("s3cret-token"))
            .unwrap()
            .header("cookie", "sid=s3cret-cookie")
            .unwrap()
            .header("x-api-key", "s3cret-key")
            .unwrap()
            .header("x-plain", "kept")
            .unwrap();
        async move {
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), 200, "{}", url);
            let host = response.url().host_str().map(str::to_string);
            let echo: serde_json::Value = response.json().await.unwrap();
            (host, echo)
        }
    };

    for status in [301, 302, 303, 307, 308] {
        let (host, echo) = send(server.url(&format!(
            "/redirect?status={}&to={}",
            status,
            encode(&elsewhere)
        )))
        .await;
        assert_eq!(host.as_deref(), Some("localhost"));
        let headers = &echo["headers"];
        assert!(
            headers.get("authorization").is_none(),
            "{:?} {}: Authorization crossed origins: {}",
            backend,
            status,
            headers
        );
        assert!(
            headers.get("cookie").is_none(),
            "{:?} {}: Cookie crossed origins: {}",
            backend,
            status,
            headers
        );
        assert!(
            headers.get("x-api-key").is_none(),
            "{:?} {}: an API key crossed origins: {}",
            backend,
            status,
            headers
        );
        assert_eq!(headers["x-plain"], "kept", "{:?} {}", backend, status);
    }

    // Credentials that were left behind do not come back when a chain
    // returns to the origin it started from.
    let back = format!(
        "{}?to={}",
        other_origin(&server.url("/redirect")),
        encode(&server.url("/echo"))
    );
    let (host, echo) = send(server.url(&format!("/redirect?to={}", encode(&back)))).await;
    assert_eq!(host.as_deref(), Some("127.0.0.1"));
    for name in ["authorization", "cookie", "x-api-key"] {
        assert!(
            echo["headers"].get(name).is_none(),
            "{:?}: {} came back after a detour: {}",
            backend,
            name,
            echo["headers"]
        );
    }

    for status in [302, 307] {
        let (_, echo) = send(server.url(&format!("/redirect?status={}&to=/echo", status))).await;
        let headers = &echo["headers"];
        assert_eq!(
            headers["authorization"], "Bearer s3cret-token",
            "{:?} {}: {}",
            backend, status, headers
        );
        assert_eq!(
            headers["cookie"], "sid=s3cret-cookie",
            "{:?} {}: {}",
            backend, status, headers
        );
        assert_eq!(
            headers["x-api-key"], "s3cret-key",
            "{:?} {}: {}",
            backend, status, headers
        );
    }
}

/// Every backend changes the method the same way when it follows a
/// redirect, sends the body again when the method is kept, and gives up on
/// a chain that does not end.
async fn redirects_rewrite_methods_and_end(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);

    for (method, status, arrives_as, body_kept) in [
        ("POST", 301, "GET", false),
        ("POST", 302, "GET", false),
        ("POST", 303, "GET", false),
        ("POST", 307, "POST", true),
        ("POST", 308, "POST", true),
        ("PUT", 303, "GET", false),
        ("PUT", 307, "PUT", true),
    ] {
        let url = server.url(&format!("/redirect?status={}&to=/echo", status));
        let request = match method {
            "POST" => client.post(url.as_str()),
            _ => client.put(url.as_str()),
        }
        .unwrap()
        .body("payload");
        let echo: serde_json::Value = request.send().await.unwrap().json().await.unwrap();
        let case = format!("{:?}: {} answered with {}", backend, method, status);
        assert_eq!(echo["method"], arrives_as, "{}", case);
        assert_eq!(
            echo["body"],
            if body_kept { "payload" } else { "" },
            "{}",
            case
        );
    }

    let response = client
        .get(server.url("/hops?n=10").as_str())
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{:?}", backend);
    assert!(response.url().path().ends_with("/echo"), "{:?}", backend);

    let endless = client
        .get(server.url("/hops?n=40").as_str())
        .unwrap()
        .send()
        .await;
    assert!(
        matches!(endless, Err(Error::Network { .. })),
        "{:?}: {:?}",
        backend,
        endless.map(|response| response.status())
    );
}

/// A cookie set while redirecting belongs to the origin that set it, not to
/// where the redirect led, and is sent on a later hop to its own origin.
async fn redirect_cookies_stay_with_their_origin(backend: BackendType) {
    if !has_second_origin(backend) {
        return;
    }
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client_with(backend, |b| b.use_cookies(true));
    let jar = client
        .cookie_jar()
        .expect("a client with cookies on exposes its jar");
    let a = server.url("");
    let b = other_origin(&a);

    // A sets a cookie in the response that redirects to B.
    let echo = echoed(
        &client,
        &format!(
            "{}/redirect?to={}&cookie={}",
            a,
            encode(&format!("{}/echo", b)),
            encode("sid=from-a; Path=/")
        ),
    )
    .await;
    assert_eq!(
        echoed_cookie(&echo),
        "",
        "{:?}: A's cookie was sent to B",
        backend
    );
    assert_eq!(cookie_pairs(jar, &format!("{}/", b)), []);
    assert_eq!(
        cookie_pairs(jar, &format!("{}/", a)),
        [pair("sid", "from-a")]
    );
    assert_eq!(
        echoed_cookie(&echoed(&client, &format!("{}/echo", a)).await),
        "sid=from-a"
    );

    // B sets a cookie in the response that redirects back to A.
    let echo = echoed(
        &client,
        &format!(
            "{}/redirect?to={}&cookie={}",
            b,
            encode(&format!("{}/echo", a)),
            encode("other=from-b; Path=/")
        ),
    )
    .await;
    assert_eq!(echoed_cookie(&echo), "sid=from-a", "{:?}", backend);
    assert_eq!(
        cookie_pairs(jar, &format!("{}/", b)),
        [pair("other", "from-b")]
    );

    // A cookie from one hop is sent on the next hop to the same origin.
    jar.clear();
    let echo = echoed(
        &client,
        &format!("{}/redirect?to=/echo&cookie={}", a, encode("hop=1; Path=/")),
    )
    .await;
    assert_eq!(echoed_cookie(&echo), "hop=1", "{:?}", backend);
}

/// The jar given to the builder is the one requests send from and store
/// into, and the one the client hands back.
async fn custom_cookie_jar_is_the_active_store(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let a = server.url("");
    let origin = format!("{}/", a);

    let jar =
        CookieJar::new(frakt::backend::Backend::with_config(backend, Default::default()).unwrap());
    // On Foundation this is the process's shared storage, which outlives
    // the test: use names of our own and take them out again.
    let preloaded = Cookie::new("frakt-test-pre", "1").domain("127.0.0.1");
    let updated = Cookie::new("frakt-test-upd", "2").domain("127.0.0.1");
    jar.remove_cookie(preloaded.clone()).unwrap();
    jar.remove_cookie(updated.clone()).unwrap();

    jar.add_cookie(preloaded.clone()).unwrap();
    let client = client_with(backend, |b| b.cookie_jar(jar.clone()));
    let from_client = client
        .cookie_jar()
        .expect("a client given a jar hands it back");

    let echo = echoed(&client, &format!("{}/echo", a)).await;
    assert_eq!(echoed_cookie(&echo), "frakt-test-pre=1", "{:?}", backend);

    let response = client
        .get(
            format!(
                "{}/set-cookie?cookie={}",
                a,
                encode("frakt-test-upd=2; Path=/")
            )
            .as_str(),
        )
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(response);
    let both = [pair("frakt-test-pre", "1"), pair("frakt-test-upd", "2")];
    assert_eq!(cookie_pairs(&jar, &origin), both, "{:?}", backend);
    assert_eq!(cookie_pairs(from_client, &origin), both, "{:?}", backend);

    jar.remove_cookie(preloaded).unwrap();
    let echo = echoed(&client, &format!("{}/echo", a)).await;
    assert_eq!(echoed_cookie(&echo), "frakt-test-upd=2", "{:?}", backend);

    jar.remove_cookie(updated).unwrap();
    let echo = echoed(&client, &format!("{}/echo", a)).await;
    assert_eq!(echoed_cookie(&echo), "", "{:?}", backend);
}

/// Without `use_cookies` or a jar, nothing is kept or sent, and turning
/// cookies off wins over a jar.
async fn cookies_are_off_unless_asked_for(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let a = server.url("");

    let jar =
        CookieJar::new(frakt::backend::Backend::with_config(backend, Default::default()).unwrap());
    for client in [
        client(backend),
        client_with(backend, |b| b.cookie_jar(jar.clone()).use_cookies(false)),
    ] {
        assert!(client.cookie_jar().is_none(), "{:?}", backend);
        let response = client
            .get(
                format!(
                    "{}/set-cookie?cookie={}",
                    a,
                    encode("frakt-test-off=1; Path=/")
                )
                .as_str(),
            )
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        drop(response);
        let echo = echoed(&client, &format!("{}/echo", a)).await;
        assert_eq!(echoed_cookie(&echo), "", "{:?}", backend);
    }
    assert_eq!(cookie_pairs(&jar, &format!("{}/", a)), [], "{:?}", backend);
}

/// `Never` stores nothing, and `OnlyFromMainDocumentDomain` stores cookies
/// from the host that was asked for but not from another host a redirect
/// led to.
async fn cookie_accept_policy_is_enforced(backend: BackendType) {
    if !has_second_origin(backend) {
        return;
    }
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client_with(backend, |b| b.use_cookies(true));
    let jar = client.cookie_jar().unwrap();
    let a = server.url("");
    let b = other_origin(&a);

    // The main document, A, sets `main` and redirects to B, which sets
    // `other`.
    let visit = |tag: &'static str| {
        let url = format!(
            "{}/redirect?cookie={}&to={}",
            a,
            encode(&format!("main-{}=1; Path=/", tag)),
            encode(&format!(
                "{}/set-cookie?cookie={}",
                b,
                encode(&format!("other-{}=1; Path=/", tag))
            ))
        );
        let request = client.get(url.as_str()).unwrap();
        async move {
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.url().host_str(), Some("localhost"));
        }
    };
    let stored = || {
        (
            cookie_pairs(jar, &format!("{}/", a)),
            cookie_pairs(jar, &format!("{}/", b)),
        )
    };

    jar.set_cookie_accept_policy(CookieAcceptPolicy::Never);
    visit("never").await;
    assert_eq!(stored(), (vec![], vec![]), "{:?}: Never", backend);

    jar.set_cookie_accept_policy(CookieAcceptPolicy::OnlyFromMainDocumentDomain);
    visit("main").await;
    assert_eq!(
        stored(),
        (vec![pair("main-main", "1")], vec![]),
        "{:?}: OnlyFromMainDocumentDomain",
        backend
    );

    jar.clear();
    jar.set_cookie_accept_policy(CookieAcceptPolicy::Always);
    visit("always").await;
    assert_eq!(
        stored(),
        (
            vec![pair("main-always", "1")],
            vec![pair("other-always", "1")]
        ),
        "{:?}: Always",
        backend
    );
}

/// Write `len` generated bytes to a new file in `dir`.
fn generated_file(dir: &std::path::Path, name: &str, seed: u64, len: u64) -> std::path::PathBuf {
    use std::io::Write;

    let path = dir.join(name);
    let mut file = std::fs::File::create(&path).unwrap();
    let mut buf = vec![0u8; 1 << 20];
    let mut pos = 0;
    while pos < len {
        let n = ((len - pos) as usize).min(buf.len());
        server::fill(seed, pos, &mut buf[..n]);
        file.write_all(&buf[..n]).unwrap();
        pos += n as u64;
    }
    path
}

/// A file upload arrives byte-exact with a Content-Length, reports its
/// progress, and is read from disk as it goes: memory does not grow with
/// the file.
async fn file_upload_is_streamed_from_disk(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let dir = temp_dir();
    let len = 96 * MIB;
    let path = generated_file(dir.path(), "upload.bin", 11, len);

    let calls = Arc::new(Mutex::new(Vec::new()));
    let rss_start = rss();
    let max_rss = Arc::new(AtomicU64::new(rss_start));
    let sampler = {
        let max_rss = max_rss.clone();
        tokio::spawn(async move {
            loop {
                max_rss.fetch_max(rss(), Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    let response = client
        .upload(server.url("/sink").as_str())
        .unwrap()
        .from_file(&path)
        .progress({
            let calls = calls.clone();
            move |sent, total| calls.lock().unwrap().push((sent, total))
        })
        .send()
        .await
        .unwrap();
    sampler.abort();
    assert_eq!(response.status(), 200);
    let received: serde_json::Value = response.json().await.unwrap();
    assert_eq!(received["method"], "POST");
    assert_eq!(received["len"], len);
    assert_eq!(
        received["blake3"],
        server::body_hash(11, 0, len).to_hex().to_string()
    );
    assert_eq!(received["content_type"], "application/octet-stream");
    assert!(received["transfer_encoding"].is_null(), "{}", received);

    let calls = calls.lock().unwrap().clone();
    assert!(calls.windows(2).all(|w| w[0].0 <= w[1].0), "{:?}", calls);
    assert_eq!(calls.last(), Some(&(len, Some(len))), "{:?}", calls.last());
    assert!(
        calls.iter().any(|(sent, _)| *sent > 0 && *sent < len),
        "progress was only reported at the ends: {:?}",
        calls
    );

    let rss_growth = max_rss.load(Ordering::SeqCst).saturating_sub(rss_start);
    println!(
        "{:?}: {} MiB file upload, RSS grew {:.1} MiB",
        backend,
        len / MIB,
        rss_growth as f64 / MIB as f64
    );
    assert!(
        rss_growth < len / 2,
        "RSS grew {} bytes uploading a {} byte file",
        rss_growth,
        len
    );

    // A redirect that keeps the method needs the file a second time.
    let small = generated_file(dir.path(), "small.txt", 12, MIB);
    let response = client
        .upload(server.url("/redirect?status=307&to=/sink").as_str())
        .unwrap()
        .from_file(&small)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let received: serde_json::Value = response.json().await.unwrap();
    assert_eq!(received["method"], "POST");
    assert_eq!(received["len"], MIB);
    assert_eq!(
        received["blake3"],
        server::body_hash(12, 0, MIB).to_hex().to_string()
    );
    assert_eq!(received["content_type"], "text/plain");

    assert!(matches!(
        client
            .upload(server.url("/sink").as_str())
            .unwrap()
            .from_file(dir.path().join("missing.bin"))
            .send()
            .await,
        Err(Error::Io(_))
    ));
}

/// Names in `dir`, to show a failed download left nothing behind.
fn listing(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// A download replaces its destination only once the whole body is on
/// disk: a failed, timed-out or cancelled one leaves the previous file as
/// it was and nothing else in the directory.
async fn download_replaces_the_destination_only_on_success(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client_with(backend, |b| b.read_timeout(Duration::from_millis(500)));
    let dir = temp_dir();
    let destination = dir.path().join("artifact.bin");
    const PREVIOUS: &[u8] = b"the previous, known-good artifact";
    std::fs::write(&destination, PREVIOUS).unwrap();
    let untouched = |case: &str| {
        assert_eq!(std::fs::read(&destination).unwrap(), PREVIOUS, "{}", case);
        assert_eq!(listing(dir.path()), ["artifact.bin"], "{}", case);
    };

    // The connection closes before the promised length.
    let result = client
        .download(
            server.url("/truncate?len=4194304&after=1048576").as_str(),
            &destination,
        )
        .unwrap()
        .send()
        .await;
    assert!(result.is_err(), "{:?}", result);
    untouched("short body");

    // The body stalls past the read timeout.
    let result = client
        .download(
            server
                .url("/stall-body?len=1048576&after=65536&ms=5000")
                .as_str(),
            &destination,
        )
        .unwrap()
        .send()
        .await;
    assert!(matches!(result, Err(Error::Timeout)), "{:?}", result);
    untouched("timeout");

    // An error status, with and without a body to keep.
    let result = client
        .download(server.url("/status?code=404").as_str(), &destination)
        .unwrap()
        .send()
        .await;
    assert!(matches!(result, Err(Error::HttpError(_))), "{:?}", result);
    untouched("error status");

    // The caller gives up part-way.
    let progressed = Arc::new(tokio::sync::Notify::new());
    let download = client
        .download(
            server
                .url(&format!("/bytes?len={}&rate={}", 8 * MIB, MIB))
                .as_str(),
            &destination,
        )
        .unwrap()
        .progress({
            let progressed = progressed.clone();
            move |_, _| progressed.notify_one()
        })
        .send();
    tokio::select! {
        result = download => panic!("the download finished: {:?}", result),
        _ = progressed.notified() => {}
    }
    // The temporary file's last write may still be on the blocking pool.
    let deadline = Instant::now() + Duration::from_secs(5);
    while listing(dir.path()).len() > 1 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    untouched("cancellation");

    // Success replaces the content in one step.
    let response = client
        .download(
            server.url("/bytes?len=300000&seed=5").as_str(),
            &destination,
        )
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, 300_000);
    assert_eq!(
        blake3::hash(&std::fs::read(&destination).unwrap()),
        server::body_hash(5, 0, 300_000)
    );
    assert_eq!(listing(dir.path()), ["artifact.bin"]);

    // A symlink at the destination is replaced, not written through.
    #[cfg(unix)]
    {
        let target = dir.path().join("elsewhere.bin");
        std::fs::write(&target, PREVIOUS).unwrap();
        let link = dir.path().join("link.bin");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        client
            .download(server.url("/bytes?len=1000&seed=6").as_str(), &link)
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), PREVIOUS);
        assert!(!std::fs::symlink_metadata(&link).unwrap().is_symlink());
        assert_eq!(std::fs::read(&link).unwrap().len(), 1000);
    }
}

/// Input that cannot be sent is an error from the builder, never a panic.
async fn invalid_input_is_an_error(backend: BackendType) {
    let _serial = SERIAL.lock().await;
    let server = Server::start().await;
    let client = client(backend);
    let url = server.url("/echo");

    for auth in [
        Auth::custom("x", "a\r\nb"),
        Auth::bearer("token\ninjected: 1"),
        Auth::custom("not a token", "x"),
    ] {
        assert!(matches!(
            client.get(url.as_str()).unwrap().auth(auth.clone()),
            Err(Error::InvalidHeader)
        ));
        assert!(matches!(
            client.upload(url.as_str()).unwrap().auth(auth.clone()),
            Err(Error::InvalidHeader)
        ));
        assert!(matches!(
            client.download(url.as_str(), "unused").unwrap().auth(auth),
            Err(Error::InvalidHeader)
        ));
    }

    // A header value that is valid but not ASCII goes out as it is.
    let response = client
        .upload(url.as_str())
        .unwrap()
        .from_data(b"data".to_vec())
        .header("x-name", "Bjørn")
        .unwrap()
        .send()
        .await
        .unwrap();
    let echo: serde_json::Value = response.json().await.unwrap();
    assert_eq!(echo["headers"]["x-name"], "Bjørn", "{:?}", backend);
    assert_eq!(echo["body"], "data");

    let dir = temp_dir();
    let destination = dir.path().join("echo.json");
    client
        .download(url.as_str(), &destination)
        .unwrap()
        .header("x-name", "Bjørn")
        .unwrap()
        .send()
        .await
        .unwrap();
    let echo: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&destination).unwrap()).unwrap();
    assert_eq!(echo["headers"]["x-name"], "Bjørn", "{:?}", backend);
}

macro_rules! backend_suite {
    ($module:ident, $backend:expr) => {
        mod $module {
            use super::BackendType;

            backend_suite!(@tests $backend;
                large_body_is_byte_exact,
                slow_consumer_gets_every_byte_with_bounded_buffering,
                range_and_if_range,
                mid_body_drop_is_an_error,
                dropping_the_stream_cancels_the_transfer,
                identity_encoding_is_honoured,
                compressed_bodies_are_decoded_and_counted,
                compressed_body_cut_short_is_an_error,
                require_checked_body_matches_integrity,
                redirect_keeps_range,
                non_ascii_header_values_do_not_panic,
                errors_are_classified,
                timeouts,
                small_json_requests,
                progress_callbacks,
                redirects_do_not_leak_credentials,
                redirects_rewrite_methods_and_end,
                redirect_cookies_stay_with_their_origin,
                custom_cookie_jar_is_the_active_store,
                cookies_are_off_unless_asked_for,
                cookie_accept_policy_is_enforced,
                file_upload_is_streamed_from_disk,
                download_replaces_the_destination_only_on_success,
                invalid_input_is_an_error,
            );
        }
    };
    (@tests $backend:expr; $($test:ident),* $(,)?) => {
        $(
            #[cfg_attr(
                not(target_os = "android"),
                tokio::test(flavor = "multi_thread", worker_threads = 4)
            )]
            #[cfg_attr(target_os = "android", vampire::test)]
            async fn $test() {
                super::$test($backend).await
            }
        )*
    };
}

#[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
backend_suite!(foundation, BackendType::Foundation);

#[cfg(feature = "backend-reqwest")]
backend_suite!(reqwest, BackendType::Reqwest);

#[cfg(all(feature = "backend-winhttp", windows))]
backend_suite!(winhttp, BackendType::Windows);

#[cfg(all(feature = "backend-android", target_os = "android"))]
backend_suite!(android, BackendType::Android);

/// A jar is tied to a kind of storage, and a backend that cannot use that
/// kind must say so rather than quietly keep cookies somewhere else.
#[cfg(all(
    feature = "backend-foundation",
    feature = "backend-reqwest",
    target_vendor = "apple"
))]
#[test]
fn a_jar_the_backend_cannot_use_is_refused() {
    let jar_for = |backend| {
        CookieJar::new(frakt::backend::Backend::with_config(backend, Default::default()).unwrap())
    };
    for (jar, backend) in [
        (BackendType::Foundation, BackendType::Reqwest),
        (BackendType::Reqwest, BackendType::Foundation),
    ] {
        let built = Client::builder()
            .backend(backend)
            .cookie_jar(jar_for(jar))
            .build();
        assert!(
            built.is_err(),
            "a {:?} jar was accepted by {:?}",
            jar,
            backend
        );
    }
}
