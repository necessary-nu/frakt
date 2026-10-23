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

use frakt::{BackendType, Client, ClientBuilder, Error};
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
        let vm = unsafe { jni::JavaVM::from_raw(vampire::java_vm() as *mut _) }.unwrap();
        let mut env = vm.attach_current_thread_as_daemon().unwrap();
        let policy = env
            .call_static_method(
                "android/security/NetworkSecurityPolicy",
                "getInstance",
                "()Landroid/security/NetworkSecurityPolicy;",
                &[],
            )
            .and_then(|v| v.l())
            .unwrap();
        env.call_method(
            &policy,
            "setCleartextTrafficPermitted",
            "(Z)V",
            &[true.into()],
        )
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

fn is_foundation(backend: BackendType) -> bool {
    format!("{:?}", backend) == "Foundation"
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

    for chunked in [false, true] {
        let url = server.url(&format!(
            "/truncate?len={}&after={}&chunked={}",
            4 * MIB,
            MIB,
            chunked as u8
        ));

        // A stack may notice the drop before it hands over the response;
        // failing send() is as good as failing the body.
        match client.get(url.as_str()).unwrap().send().await {
            Err(error) => println!(
                "{:?}: truncated (chunked: {}) failed before the body: {}",
                backend, chunked, error
            ),
            Ok(response) => match drain(response).await {
                Ok((total, _)) if chunked && is_foundation(backend) => {
                    // CFNetwork reports a chunked body whose connection closes
                    // before the last chunk as complete, with or without
                    // keep-alive, and exposes nothing that tells the two apart.
                    println!(
                        "{:?}: KNOWN LIMITATION: truncated chunked body ended cleanly after {} bytes",
                        backend, total
                    );
                }
                Ok((total, _)) => panic!(
                    "truncated body (chunked: {}) ended cleanly after {} bytes",
                    chunked, total
                ),
                Err((total, error)) => {
                    assert!(
                        total <= MIB,
                        "read {} bytes of a 1 MiB truncated body",
                        total
                    );
                    println!("{:?}: truncated (chunked: {}): {}", backend, chunked, error);
                }
            },
        }

        if chunked && is_foundation(backend) {
            continue;
        }
        if let Ok(response) = client.get(url.as_str()).unwrap().send().await {
            let mut reader = response.stream();
            let result = tokio::io::copy(&mut reader, &mut tokio::io::sink()).await;
            assert!(
                result.is_err(),
                "AsyncRead reported a clean end of a truncated body (chunked: {}): {:?}",
                chunked,
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

    for explicit in [false, true] {
        let id = if explicit { "explicit" } else { "default" };
        let mut request = client
            .get(server.url(&format!("/encoded?id={}", id)).as_str())
            .unwrap();
        if explicit {
            request = request.header("Accept-Encoding", "identity").unwrap();
        }
        let response = request.send().await.unwrap();
        assert!(response.headers().get("content-encoding").is_none());
        assert_eq!(
            header(&response, "content-length"),
            server::ENCODED_TEXT.len().to_string()
        );
        assert_eq!(response.text().await.unwrap(), server::ENCODED_TEXT);

        let seen = server.requests_with_id(id);
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].header_str("accept-encoding").as_deref(),
            Some("identity"),
            "{} request",
            id
        );
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
                redirect_keeps_range,
                non_ascii_header_values_do_not_panic,
                errors_are_classified,
                timeouts,
                small_json_requests,
                progress_callbacks,
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
