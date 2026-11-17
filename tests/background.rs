//! Background downloads on the reqwest backend against the local server,
//! run in the calling task: every answer a server can give to a request to
//! resume, and what a download may and may not leave on disk.
//!
//! `tests/background_helper.rs` covers the same download run in the helper
//! process.
//!
//! A download interrupted in mid-transfer waits two seconds before it tries
//! again, so most tests instead start from files laid out as an interrupted
//! download leaves them. That also pins the format of the sidecar, which
//! has to stay readable across releases for downloads to survive an update.

#![cfg(feature = "backend-reqwest")]

#[path = "support/server.rs"]
mod server;

use frakt::{Auth, BackendType, Client, DownloadResponse, Error};
use serde_json::json;
use server::{LAST_MODIFIED, Recorded, Server};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NAME: &str = "download.bin";
const PARTIAL: &str = "download.bin.frakt-partial";
const SIDECAR: &str = "download.bin.frakt-partial.meta";

fn client() -> Client {
    Client::builder()
        .backend(BackendType::Reqwest)
        .build()
        .expect("client")
}

/// A destination in a directory of its own, so that everything a download
/// leaves behind can be seen.
struct Target {
    dir: tempfile::TempDir,
    path: PathBuf,
}

impl Target {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(NAME);
        Self { dir, path }
    }

    fn file(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// The names of the files in the directory.
    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// Lay out what an interrupted download leaves: the bytes it got, and
    /// the sidecar that says what they are the start of.
    fn plant(&self, bytes: &[u8], sidecar: serde_json::Value) {
        std::fs::write(self.file(PARTIAL), bytes).unwrap();
        std::fs::write(self.file(SIDECAR), sidecar.to_string()).unwrap();
    }

    /// The destination holds exactly the generated body, and nothing else
    /// is left in its directory.
    fn assert_holds(&self, seed: u64, len: u64) {
        let content = std::fs::read(&self.path).unwrap();
        assert_eq!(content.len() as u64, len);
        assert_eq!(blake3::hash(&content), server::body_hash(seed, 0, len));
        assert_eq!(self.files(), [NAME]);
    }
}

async fn download(client: &Client, url: &str, target: &Target) -> frakt::Result<DownloadResponse> {
    client.download_background(url, &target.path)?.send().await
}

fn header(response: &DownloadResponse, name: &str) -> String {
    response
        .headers
        .get(name)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_else(|| panic!("missing {} header", name))
}

/// The offset a request asked to resume from.
fn range_start(request: &Recorded) -> u64 {
    let range = request.header_str("range").expect("a Range header");
    let start = range
        .strip_prefix("bytes=")
        .and_then(|range| range.strip_suffix('-'))
        .unwrap_or_else(|| panic!("unexpected Range {:?}", range));
    start.parse().unwrap()
}

fn assert_plain(request: &Recorded) {
    for name in ["range", "if-range", "if-none-match", "if-modified-since"] {
        assert_eq!(request.header_str(name), None, "{}", name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_download_is_byte_exact() {
    let server = Server::start().await;
    let target = Target::new();
    let len = 3 * (1 << 20) + 123;
    let url = server.url(&format!("/bytes?len={}&seed=11&id=fresh", len));

    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let response = client()
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(response.file_path, target.path);
    assert_eq!(response.bytes_downloaded, len);
    assert_eq!(header(&response, "content-length"), len.to_string());
    assert_eq!(header(&response, "etag"), server::etag(11, len));
    assert_eq!(header(&response, "last-modified"), LAST_MODIFIED);
    assert_eq!(
        header(&response, "content-type"),
        "application/octet-stream"
    );
    target.assert_holds(11, len);

    let calls = calls.lock().unwrap().clone();
    assert!(calls.len() > 1, "{:?}", calls);
    assert!(calls.windows(2).all(|w| w[0].0 <= w[1].0), "{:?}", calls);
    assert!(calls.iter().all(|call| call.1 == Some(len)), "{:?}", calls);
    assert_eq!(calls.last(), Some(&(len, Some(len))));

    let requests = server.requests_with_id("fresh");
    assert_eq!(requests.len(), 1);
    assert_plain(&requests[0]);
    assert_eq!(
        requests[0].header_str("accept-encoding").as_deref(),
        Some("identity")
    );
}

/// The published file has the permissions of one the caller created itself,
/// not the private ones of the partial file.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_file_has_ordinary_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;

    let server = Server::start().await;
    let client = client();
    let url = server.url("/bytes?len=10000&seed=12");

    let target = Target::new();
    download(&client, &url, &target).await.unwrap();
    let reference = target.file("reference");
    std::fs::File::create(&reference).unwrap();
    assert_eq!(mode(&target.path), mode(&reference));

    // An existing destination keeps the permissions it had.
    std::fs::set_permissions(&target.path, std::fs::Permissions::from_mode(0o640)).unwrap();
    download(&client, &url, &target).await.unwrap();
    assert_eq!(mode(&target.path), 0o640);
}

/// What a connection lost in mid-body leads to, with the validator the
/// response had: the attempt after it asks for the rest, and only if the
/// body is still the same one.
async fn interrupted_download_resumes(id: &str, extra: &str, validator: &str) {
    let server = Server::start().await;
    let target = Target::new();
    let (len, cut) = (600_000, 200_000);
    let url = server.url(&format!(
        "/bytes?len={}&seed=13&cut={}&id={}{}",
        len, cut, id, extra
    ));

    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let response = client()
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
        .send()
        .await
        .unwrap();

    // The response that completed the download is the partial one.
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    target.assert_holds(13, len);

    let requests = server.requests_with_id(id);
    assert_eq!(requests.len(), 2);
    assert_plain(&requests[0]);
    let resumed_at = range_start(&requests[1]);
    assert!(resumed_at > 0 && resumed_at <= cut, "{}", resumed_at);
    assert_eq!(
        requests[1].header_str("if-range").as_deref(),
        Some(validator)
    );
    assert_eq!(
        header(&response, "content-range"),
        format!("bytes {}-{}/{}", resumed_at, len - 1, len)
    );

    let calls = calls.lock().unwrap().clone();
    assert!(calls.windows(2).all(|w| w[0].0 <= w[1].0), "{:?}", calls);
    assert_eq!(calls.last(), Some(&(len, Some(len))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_download_resumes_with_range_and_if_range() {
    interrupted_download_resumes("cut-etag", "", &server::etag(13, 600_000)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_download_resumes_by_last_modified_without_an_etag() {
    interrupted_download_resumes("cut-date", "&noetag=1", LAST_MODIFIED).await;
}

/// A weak ETag does not promise the same bytes, so it is not what a resumed
/// download is made conditional on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_download_does_not_resume_by_weak_etag() {
    interrupted_download_resumes("cut-weak", "&etag=W%2F%22weak%22", LAST_MODIFIED).await;
}

/// A download dropped in mid-transfer leaves what the next one carries on
/// from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_download_is_resumed_by_the_next() {
    let server = Server::start().await;
    let client = client();
    let target = Target::new();
    let len = 400_000;
    let url = server.url(&format!(
        "/bytes?len={}&seed=14&chunk=8192&rate=400000&id=dropped",
        len
    ));

    let seen = Arc::new(AtomicU64::new(0));
    let progress = seen.clone();
    let mut first = Box::pin(
        client
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .progress(move |done, _| progress.store(done, Ordering::SeqCst))
            .send(),
    );
    while seen.load(Ordering::SeqCst) == 0 {
        tokio::select! {
            result = &mut first => panic!("finished too soon: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    drop(first);
    let seen = seen.load(Ordering::SeqCst);
    assert!(seen < len);
    assert_eq!(target.files(), [PARTIAL, SIDECAR]);

    // A write that was under way when the first was dropped holds the lock
    // until it is done.
    let started = Instant::now();
    let response = loop {
        match download(&client, &url, &target).await {
            Err(Error::Io(message))
                if message.contains("already in progress")
                    && started.elapsed() < Duration::from_secs(2) =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            other => break other.unwrap(),
        }
    };
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    target.assert_holds(14, len);

    let requests = server.requests_with_id("dropped");
    assert_eq!(requests.len(), 2);
    assert!(range_start(&requests[1]) >= seen);
    assert_eq!(
        requests[1].header_str("if-range"),
        Some(server::etag(14, len))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planted_partial_file_is_resumed() {
    let server = Server::start().await;
    let target = Target::new();
    let (len, have) = (300_000, 100_000);
    let url = server.url(&format!("/bytes?len={}&seed=15&id=planted", len));
    target.plant(
        &server::body(15, 0, have),
        json!({ "url": url, "etag": server::etag(15, len), "total": len }),
    );

    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let response = client()
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    target.assert_holds(15, len);

    let requests = server.requests_with_id("planted");
    assert_eq!(requests.len(), 1);
    assert_eq!(range_start(&requests[0]), have);
    assert_eq!(
        requests[0].header_str("if-range"),
        Some(server::etag(15, len))
    );
    assert_eq!(
        requests[0].header_str("accept-encoding").as_deref(),
        Some("identity")
    );

    // Progress counts the bytes of the file, not of this transfer.
    let calls = calls.lock().unwrap().clone();
    assert!(calls.first().unwrap().0 > have, "{:?}", calls);
    assert_eq!(calls.last(), Some(&(len, Some(len))));
}

/// A server that ignores `Range`, or whose body has changed, answers a
/// request to resume with the whole body, which must replace what was on
/// disk rather than be added to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_200_to_a_resume_request_replaces_the_partial_file() {
    let server = Server::start().await;
    let client = client();
    let len = 300_000;
    let stale = vec![0xAA; 100_000];

    // Range ignored.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=16&norange=1&id=norange", len));
    target.plant(
        &stale,
        json!({ "url": url, "etag": server::etag(16, len), "total": len }),
    );
    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    target.assert_holds(16, len);
    let requests = server.requests_with_id("norange");
    assert_eq!(requests.len(), 1);
    assert_eq!(range_start(&requests[0]), stale.len() as u64);

    // ETag changed.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=16&id=etag", len));
    target.plant(
        &stale,
        json!({ "url": url, "etag": "\"an-older-version\"", "total": len }),
    );
    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(16, len);
    let requests = server.requests_with_id("etag");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].header_str("if-range").as_deref(),
        Some("\"an-older-version\"")
    );

    // Last-Modified changed, on a server that sends no ETag.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=16&noetag=1&id=date", len));
    let older = "Tue, 20 Oct 2015 07:28:00 GMT";
    target.plant(
        &stale,
        json!({ "url": url, "last_modified": older, "total": len }),
    );
    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(16, len);
    let requests = server.requests_with_id("date");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].header_str("if-range").as_deref(), Some(older));
}

/// A 206 is appended only if it starts exactly where the partial file ends
/// and is for the body the partial file is the start of. Anything else
/// means starting again from nothing, with a request for the whole body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_206_that_does_not_fit_restarts_from_zero() {
    let server = Server::start().await;
    let client = client();
    let (len, have) = (300_000, 100_000);
    let etag = server::etag(17, len);

    let cases = [
        // It starts a thousand bytes late.
        ("skew", "&skew=1000", json!({ "etag": etag, "total": len })),
        // Its total is not the one the partial file was started with.
        ("total", "", json!({ "etag": etag, "total": len + 5 })),
        // Its ETag is not the one asked for: If-Range was ignored.
        (
            "ifrange",
            "&noifrange=1",
            json!({ "etag": "\"an-older-version\"", "total": len }),
        ),
    ];
    for (id, extra, mut sidecar) in cases {
        let target = Target::new();
        let url = server.url(&format!("/bytes?len={}&seed=17&id={}{}", len, id, extra));
        sidecar["url"] = json!(url);
        target.plant(&server::body(17, 0, have), sidecar);

        let response = download(&client, &url, &target).await.unwrap();
        assert_eq!(response.status, 200, "{}", id);
        assert_eq!(response.bytes_downloaded, len, "{}", id);
        target.assert_holds(17, len);

        let requests = server.requests_with_id(id);
        assert_eq!(requests.len(), 2, "{}", id);
        assert_eq!(range_start(&requests[0]), have, "{}", id);
        assert_plain(&requests[1]);
    }
}

/// A 206 may stop before the end of the body. What it brought is kept, but
/// the file is not published until it is as long as the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_206_that_stops_short_is_not_the_whole_body() {
    let server = Server::start().await;
    let target = Target::new();
    let (len, have) = (300_000, 100_000);
    let url = server.url(&format!(
        "/bytes?len={}&seed=25&maxrange=150000&id=short",
        len
    ));
    target.plant(
        &server::body(25, 0, have),
        json!({ "url": url, "etag": server::etag(25, len), "total": len }),
    );

    let response = download(&client(), &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    assert_eq!(
        header(&response, "content-range"),
        "bytes 250000-299999/300000"
    );
    target.assert_holds(25, len);

    let requests = server.requests_with_id("short");
    assert_eq!(requests.len(), 2);
    assert_eq!(range_start(&requests[0]), have);
    assert_eq!(range_start(&requests[1]), have + 150_000);
}

/// An error status in answer to a request to resume says nothing about the
/// partial file, which is kept for the next download unless the caller wants
/// the error's body as the download.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_error_status_does_not_cost_the_partial_file() {
    let server = Server::start().await;
    let client = client();
    let (len, have) = (300_000, 100_000);
    let plant = |target: &Target, url: &str| {
        target.plant(
            &server::body(26, 0, have),
            json!({ "url": url, "etag": server::etag(26, len), "total": len }),
        )
    };

    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=26&failfirst=503&id=busy", len));
    plant(&target, &url);
    let refused = download(&client, &url, &target).await;
    assert!(
        matches!(refused, Err(Error::HttpStatus(status)) if status == 503),
        "{:?}",
        refused
    );
    assert_eq!(target.files(), [PARTIAL, SIDECAR]);
    assert_eq!(
        std::fs::read(target.file(PARTIAL)).unwrap(),
        server::body(26, 0, have)
    );

    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(26, len);
    let requests = server.requests_with_id("busy");
    assert_eq!(requests.len(), 2);
    assert_eq!(range_start(&requests[0]), have);
    assert_eq!(range_start(&requests[1]), have);

    let target = Target::new();
    let url = server.url(&format!(
        "/bytes?len={}&seed=26&failfirst=503&id=allowed",
        len
    ));
    plant(&target, &url);
    let response = client
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .error_for_status(false)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 503);
    assert_eq!(response.bytes_downloaded, 0);
    assert_eq!(std::fs::read(&target.path).unwrap(), b"");
    assert_eq!(target.files(), [NAME]);
}

/// A 416 says the partial file is already the whole body only if it gives
/// the partial file's length as the total, for the same body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_416_completes_only_if_the_total_matches() {
    let server = Server::start().await;
    let client = client();
    let len = 200_000;
    let etag = server::etag(18, len);

    // The whole body is there, and its length was never known.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=18&id=whole", len));
    target.plant(
        &server::body(18, 0, len),
        json!({ "url": url, "etag": etag }),
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let response = client
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    assert_eq!(
        header(&response, "content-range"),
        format!("bytes */{}", len)
    );
    target.assert_holds(18, len);
    let requests = server.requests_with_id("whole");
    assert_eq!(requests.len(), 1);
    assert_eq!(range_start(&requests[0]), len);
    assert_eq!(*calls.lock().unwrap(), [(len, Some(len))]);

    let restarts = [
        // The server gives another total.
        ("total", "&total416=200007", len, etag.as_str()),
        // The partial file is longer than the body.
        ("longer", "", len + 500, etag.as_str()),
        // The body is not the one the partial file is of.
        ("ifrange", "&noifrange=1", len, "\"an-older-version\""),
    ];
    for (id, extra, have, planted_etag) in restarts {
        let target = Target::new();
        let url = server.url(&format!("/bytes?len={}&seed=18&id={}{}", len, id, extra));
        target.plant(
            &vec![0xAA; have as usize],
            json!({ "url": url, "etag": planted_etag }),
        );

        let response = download(&client, &url, &target).await.unwrap();
        assert_eq!(response.status, 200, "{}", id);
        target.assert_holds(18, len);

        let requests = server.requests_with_id(id);
        assert_eq!(requests.len(), 2, "{}", id);
        assert_eq!(range_start(&requests[0]), have, "{}", id);
        assert_plain(&requests[1]);
    }
}

/// A partial file as long as the body was said to be needs no range, only
/// the server's word that the body has not changed since.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_complete_partial_file_is_revalidated() {
    let server = Server::start().await;
    let client = client();
    let len = 200_000;
    let etag = server::etag(19, len);

    // Unchanged, by ETag: published as it is.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=19&cond=1&id=same", len));
    target.plant(
        &server::body(19, 0, len),
        json!({ "url": url, "etag": etag, "total": len }),
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let response = client
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    assert_eq!(header(&response, "etag"), etag);
    target.assert_holds(19, len);
    let requests = server.requests_with_id("same");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].header_str("if-none-match"), Some(etag.clone()));
    assert_eq!(requests[0].header_str("range"), None);
    assert_eq!(requests[0].header_str("if-range"), None);
    assert_eq!(*calls.lock().unwrap(), [(len, Some(len))]);

    // Unchanged, by date.
    let target = Target::new();
    let url = server.url(&format!(
        "/bytes?len={}&seed=19&cond=1&noetag=1&id=same-date",
        len
    ));
    target.plant(
        &server::body(19, 0, len),
        json!({ "url": url, "last_modified": LAST_MODIFIED, "total": len }),
    );
    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(19, len);
    let requests = server.requests_with_id("same-date");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].header_str("if-modified-since").as_deref(),
        Some(LAST_MODIFIED)
    );
    assert_eq!(requests[0].header_str("range"), None);

    // Changed: the answer is the new body, which replaces the old one.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=19&cond=1&id=changed", len));
    target.plant(
        &vec![0xAA; len as usize],
        json!({ "url": url, "etag": "\"an-older-version\"", "total": len }),
    );
    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.bytes_downloaded, len);
    target.assert_holds(19, len);
    let requests = server.requests_with_id("changed");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].header_str("if-none-match").as_deref(),
        Some("\"an-older-version\"")
    );
}

/// Files that are not the remains of a download of this URL count for
/// nothing: the request is for the whole body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_files_are_not_resumed() {
    let server = Server::start().await;
    let client = client();
    let len = 100_000;
    let etag = server::etag(20, len);
    let partial = server::body(20, 0, 40_000);

    for id in ["other-url", "no-validator", "not-an-object"] {
        let target = Target::new();
        let url = server.url(&format!("/bytes?len={}&seed=20&id={}", len, id));
        let sidecar = match id {
            "other-url" => json!({ "url": format!("{}&x=1", url), "etag": etag, "total": len }),
            "no-validator" => json!({ "url": url, "total": len }),
            _ => json!("frakt"),
        };
        target.plant(&partial, sidecar);

        let response = download(&client, &url, &target).await.unwrap();
        assert_eq!(response.status, 200, "{}", id);
        target.assert_holds(20, len);
        let requests = server.requests_with_id(id);
        assert_eq!(requests.len(), 1, "{}", id);
        assert_plain(&requests[0]);
    }

    // A partial file with no sidecar at all.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=20&id=bare", len));
    std::fs::write(target.file(PARTIAL), &partial).unwrap();
    download(&client, &url, &target).await.unwrap();
    target.assert_holds(20, len);
    assert_plain(&server.requests_with_id("bare")[0]);

    // An existing destination, shorter than the body, is not a partial
    // download either.
    let target = Target::new();
    let url = server.url(&format!("/bytes?len={}&seed=20&id=existing", len));
    std::fs::write(&target.path, &partial).unwrap();
    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(20, len);
    let requests = server.requests_with_id("existing");
    assert_eq!(requests.len(), 1);
    assert_plain(&requests[0]);
}

/// Nothing is written to the destination until the download is complete,
/// so one that fails leaves the file that was there as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_download_leaves_the_destination_alone() {
    let server = Server::start().await;
    let client = client();
    let target = Target::new();
    std::fs::write(&target.path, b"precious").unwrap();
    let intact = || {
        assert_eq!(std::fs::read(&target.path).unwrap(), b"precious");
        assert_eq!(target.files(), [NAME]);
    };

    let refused = download(&client, &server.url("/status?code=500"), &target).await;
    assert!(
        matches!(refused, Err(Error::HttpStatus(status)) if status == 500),
        "{:?}",
        refused
    );
    intact();

    let untrusted = download(&client, &server.tls_url("/bytes?len=1000"), &target).await;
    assert!(
        matches!(untrusted, Err(Error::Tls { .. })),
        "{:?}",
        untrusted
    );
    intact();

    // While a download is under way, too.
    let url = server.url("/bytes?len=200000&seed=21&chunk=8192&rate=400000");
    let seen = Arc::new(AtomicU64::new(0));
    let progress = seen.clone();
    let mut slow = Box::pin(
        client
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .progress(move |done, _| progress.store(done, Ordering::SeqCst))
            .send(),
    );
    while seen.load(Ordering::SeqCst) == 0 {
        tokio::select! {
            result = &mut slow => panic!("finished too soon: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    assert_eq!(std::fs::read(&target.path).unwrap(), b"precious");
    slow.await.unwrap();
    target.assert_holds(21, 200_000);
}

/// The transport failing is retried, with a growing wait, five times in
/// all; then the download fails, having published nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_failures_are_retried_before_the_download_fails() {
    let server = Server::start().await;
    let target = Target::new();
    std::fs::write(&target.path, b"precious").unwrap();
    // Always cut short, and with no validator to resume by.
    let url = server.url("/truncate?len=100000&after=20000&id=short");

    let started = Instant::now();
    let failed = download(&client(), &url, &target).await;
    assert!(matches!(failed, Err(Error::Network { .. })), "{:?}", failed);
    assert_eq!(server.requests_with_id("short").len(), 5);
    assert!(started.elapsed() >= Duration::from_secs(2 + 4 + 8 + 16));
    for request in server.requests_with_id("short") {
        assert_plain(&request);
    }
    assert_eq!(std::fs::read(&target.path).unwrap(), b"precious");
    assert_eq!(target.files(), [NAME]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_statuses_follow_error_for_status() {
    let server = Server::start().await;
    let client = client();

    // By default an error status is an error, and nothing is written.
    let target = Target::new();
    let url = server.url("/status?code=404&id=refused");
    let refused = download(&client, &url, &target).await;
    assert!(
        matches!(refused, Err(Error::HttpStatus(status)) if status == 404),
        "{:?}",
        refused
    );
    assert_eq!(target.files(), [] as [&str; 0]);
    // A status is the server's answer, not a failure to get one.
    assert_eq!(server.requests_with_id("refused").len(), 1);

    // Otherwise its body is the download.
    for allow in [false, true] {
        let target = Target::new();
        let builder = client
            .download_background(server.url("/status?code=404").as_str(), &target.path)
            .unwrap();
        let builder = if allow {
            builder.allow_error_status()
        } else {
            builder.error_for_status(false)
        };
        let response = builder.send().await.unwrap();
        assert_eq!(response.status, 404);
        assert_eq!(response.bytes_downloaded, 10);
        assert_eq!(std::fs::read(&target.path).unwrap(), b"status 404");
        assert_eq!(target.files(), [NAME]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_headers_reach_the_server() {
    let server = Server::start().await;
    let jar = frakt::CookieJar::new(frakt::backend::Backend::reqwest().unwrap());
    jar.add_cookie(frakt::Cookie::new("session", "abc123").domain("127.0.0.1"))
        .unwrap();
    let client = Client::builder()
        .backend(BackendType::Reqwest)
        .user_agent("frakt-background-test/1.0")
        .header("X-Client-Default", "from the client")
        .unwrap()
        .cookie_jar(jar)
        .build()
        .unwrap();

    let target = Target::new();
    client
        .download_background(server.url("/echo?id=headers").as_str(), &target.path)
        .unwrap()
        .auth(Auth::bearer("secret-token"))
        .unwrap()
        .header("X-Custom-Header", "custom value")
        .unwrap()
        .send()
        .await
        .unwrap();
    let sent = &server.requests_with_id("headers")[0];
    let header = |name| sent.header_str(name);
    assert_eq!(
        header("authorization").as_deref(),
        Some("Bearer secret-token")
    );
    assert_eq!(header("x-custom-header").as_deref(), Some("custom value"));
    assert_eq!(
        header("x-client-default").as_deref(),
        Some("from the client")
    );
    assert_eq!(
        header("user-agent").as_deref(),
        Some("frakt-background-test/1.0")
    );
    assert_eq!(header("cookie").as_deref(), Some("session=abc123"));
    assert_eq!(header("accept-encoding").as_deref(), Some("identity"));

    // The file is what the server echoed, so it saw the same.
    let echoed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&target.path).unwrap()).unwrap();
    assert_eq!(echoed["headers"]["authorization"], "Bearer secret-token");

    // A cookie the caller sets itself is the one sent.
    client
        .download_background(server.url("/echo?id=cookie").as_str(), &target.path)
        .unwrap()
        .header("Cookie", "mine=1")
        .unwrap()
        .send()
        .await
        .unwrap();
    let sent = &server.requests_with_id("cookie")[0];
    assert_eq!(sent.header_str("cookie").as_deref(), Some("mine=1"));
}

/// The caller's choice of encoding is passed on, and a body that comes back
/// encoded is stored decoded, as a foreground download stores it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chosen_encoding_is_honoured_and_decoded() {
    let server = Server::start().await;
    let client = client();
    let text = server::text_body(50_000);

    let target = Target::new();
    let url = server.url("/gzip?len=50000&id=identity");
    let response = download(&client, &url, &target).await.unwrap();
    assert!(response.headers.get("content-encoding").is_none());
    assert_eq!(std::fs::read(&target.path).unwrap(), text);

    let target = Target::new();
    let response = client
        .download_background(server.url("/gzip?len=50000&id=gzip").as_str(), &target.path)
        .unwrap()
        .header("Accept-Encoding", "gzip")
        .unwrap()
        .send()
        .await
        .unwrap();
    let sent = &server.requests_with_id("gzip")[0];
    assert_eq!(sent.header_str("accept-encoding").as_deref(), Some("gzip"));
    assert_eq!(header(&response, "content-encoding"), "gzip");
    assert_eq!(response.bytes_downloaded, 50_000);
    assert_eq!(std::fs::read(&target.path).unwrap(), text);
    assert_eq!(target.files(), [NAME]);
}

/// A request the caller made partial or conditional is sent as it is and
/// its answer stored as it comes, with nothing kept to resume it by.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_of_the_callers_own_is_passed_through() {
    let server = Server::start().await;
    let client = client();
    let target = Target::new();
    let url = server.url("/bytes?len=100000&seed=22&cut=50&id=own-range");

    // Cut short at first, so that there is something to resume if anything
    // were kept.
    let response = client
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .header("Range", "bytes=100-199")
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 206);
    assert_eq!(response.bytes_downloaded, 100);
    assert_eq!(header(&response, "content-range"), "bytes 100-199/100000");
    assert_eq!(
        std::fs::read(&target.path).unwrap(),
        server::body(22, 100, 200)
    );
    assert_eq!(target.files(), [NAME]);

    let requests = server.requests_with_id("own-range");
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(
            request.header_str("range").as_deref(),
            Some("bytes=100-199")
        );
        assert_eq!(request.header_str("if-range"), None);
    }

    // A 304 to the caller's own condition has no body to store, and what
    // was at the destination stays.
    let url = server.url("/bytes?len=100000&seed=22&cond=1");
    let response = client
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .header("If-None-Match", server::etag(22, 100_000))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 304);
    assert_eq!(response.bytes_downloaded, 0);
    assert_eq!(
        std::fs::read(&target.path).unwrap(),
        server::body(22, 100, 200)
    );
    assert_eq!(target.files(), [NAME]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_download_per_destination_at_a_time() {
    let server = Server::start().await;
    let client = Arc::new(client());
    let target = Target::new();
    let len = 400_000;
    let url = server.url(&format!(
        "/bytes?len={}&seed=23&chunk=8192&rate=400000",
        len
    ));

    let seen = Arc::new(AtomicU64::new(0));
    let progress = seen.clone();
    let first = client
        .download_background(url.as_str(), &target.path)
        .unwrap()
        .session_identifier("first")
        .progress(move |done, _| progress.store(done, Ordering::SeqCst))
        .send();
    let first = tokio::spawn(first);
    while seen.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // While it is being written, the partial file is the user's alone.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let partial = std::fs::metadata(target.file(PARTIAL)).unwrap();
        assert_eq!(partial.permissions().mode() & 0o777, 0o600);
    }

    // The same destination, whatever the session identifier and the URL.
    for other in [url.clone(), server.url("/bytes?len=1000&seed=24")] {
        let second = client
            .download_background(other.as_str(), &target.path)
            .unwrap()
            .session_identifier("second")
            .send()
            .await;
        match second {
            Err(Error::Io(message)) => {
                assert!(message.contains("already in progress"), "{}", message)
            }
            other => panic!("the second download was not refused: {:?}", other),
        }
    }

    // The first is none the worse for it.
    let response = first.await.unwrap().unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(23, len);

    // And once it is done, the destination is free again.
    let response = download(&client, &server.url("/bytes?len=1000&seed=24"), &target)
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(24, 1000);
}

/// The session identifier names nothing on this backend, so downloads that
/// share one do not get in each other's way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downloads_sharing_a_session_identifier_are_independent() {
    let server = Server::start().await;
    let client = client();
    let targets: Vec<Target> = (0..4).map(|_| Target::new()).collect();
    let len = 500_000;

    let downloads = targets.iter().enumerate().map(|(index, target)| {
        let url = server.url(&format!(
            "/bytes?len={}&seed={}&chunk=8192",
            len,
            30 + index
        ));
        let download = client
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .session_identifier("shared");
        async move { download.send().await }
    });
    for result in futures_util::future::join_all(downloads).await {
        assert_eq!(result.unwrap().status, 200);
    }
    for (index, target) in targets.iter().enumerate() {
        target.assert_holds(30 + index as u64, len);
    }
}

/// Every path below `root`, relative to it.
fn tree(root: &Path) -> Vec<PathBuf> {
    fn walk(root: &Path, directory: &Path, found: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            found.push(path.strip_prefix(root).unwrap().to_path_buf());
            if path.is_dir() {
                walk(root, &path, found);
            }
        }
    }
    let mut found = Vec::new();
    walk(root, root, &mut found);
    found.sort();
    found
}

/// A session identifier is the caller's to choose and may come from
/// anywhere. It must never become part of a path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hostile_session_identifiers_create_no_files_elsewhere() {
    let server = Server::start().await;
    let client = client();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("a/b/c").join(NAME);
    let expected: Vec<PathBuf> = ["a", "a/b", "a/b/c", "a/b/c/download.bin"]
        .iter()
        .map(PathBuf::from)
        .collect();

    // Where downloads once kept their state, named after the identifier.
    let shared = std::env::temp_dir().join("frakt");
    let shared_before = shared.is_dir().then(|| tree(&shared));

    let unique = format!("frakt-hostile-{}", std::process::id());
    let identifiers = [
        "../../x".to_string(),
        format!("../../../../../../../../../../tmp/{}", unique),
        root.path().join("absolute").to_string_lossy().into_owned(),
        format!("/tmp/{}", unique),
        String::new(),
        ".".to_string(),
        "..".to_string(),
        "a/b/c".to_string(),
        "nul\0byte".to_string(),
        "long-".repeat(4000),
    ];
    for (index, identifier) in identifiers.iter().enumerate() {
        let seed = 40 + index as u64;
        let url = server.url(&format!("/bytes?len=20000&seed={}", seed));
        let response = client
            .download_background(url.as_str(), &destination)
            .unwrap()
            .session_identifier(identifier.as_str())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(
            blake3::hash(&std::fs::read(&destination).unwrap()),
            server::body_hash(seed, 0, 20_000)
        );
        assert_eq!(tree(root.path()), expected, "{:?}", identifier);
    }

    assert_eq!(shared.is_dir().then(|| tree(&shared)), shared_before);
    for stray in [format!("/tmp/{}", unique), format!("/tmp/{}.state", unique)] {
        assert!(!Path::new(&stray).exists(), "{}", stray);
    }
}

/// Whatever is found where the partial file and its sidecar go, a download
/// writes to nothing but files of its own making.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planted_links_are_not_followed() {
    use std::os::unix::fs::symlink;

    let server = Server::start().await;
    let client = client();
    let url = server.url("/bytes?len=20000&seed=50");
    let elsewhere = tempfile::tempdir().unwrap();
    let victim = elsewhere.path().join("victim");
    std::fs::write(&victim, b"not yours to write").unwrap();
    let refused = |result: frakt::Result<DownloadResponse>, what: &str| match result {
        Err(Error::Io(message)) => assert!(message.contains(what), "{}", message),
        other => panic!("not refused: {:?}", other),
    };

    // A symbolic link where the partial file goes, to a file.
    let target = Target::new();
    symlink(&victim, target.file(PARTIAL)).unwrap();
    refused(download(&client, &url, &target).await, "symbolic link");
    assert_eq!(std::fs::read(&victim).unwrap(), b"not yours to write");
    assert!(!target.path.exists());

    // And to nothing, where following it would create the file.
    let target = Target::new();
    let dangling = elsewhere.path().join("created");
    symlink(&dangling, target.file(PARTIAL)).unwrap();
    refused(download(&client, &url, &target).await, "symbolic link");
    assert!(!dangling.exists());
    assert!(!target.path.exists());

    // Another name for a file that is the user's own.
    let target = Target::new();
    std::fs::hard_link(&victim, target.file(PARTIAL)).unwrap();
    refused(download(&client, &url, &target).await, "not a regular file");
    assert_eq!(std::fs::read(&victim).unwrap(), b"not yours to write");
    assert!(!target.path.exists());

    // A directory.
    let target = Target::new();
    std::fs::create_dir(target.file(PARTIAL)).unwrap();
    assert!(matches!(
        download(&client, &url, &target).await,
        Err(Error::Io(_))
    ));
    assert!(!target.path.exists());

    // A symbolic link where the sidecar goes is replaced, not written
    // through, and not believed either.
    let target = Target::new();
    let sidecar = elsewhere.path().join("sidecar");
    let claim = json!({ "url": url, "etag": server::etag(50, 20_000), "total": 20_000 });
    std::fs::write(&sidecar, claim.to_string()).unwrap();
    std::fs::write(target.file(PARTIAL), vec![0xAA; 20_000]).unwrap();
    symlink(&sidecar, target.file(SIDECAR)).unwrap();
    let response = download(&client, &url, &target).await.unwrap();
    assert_eq!(response.status, 200);
    target.assert_holds(50, 20_000);
    assert_eq!(
        std::fs::read(&sidecar).unwrap(),
        claim.to_string().as_bytes()
    );

    // A symbolic link at the destination is replaced by the file.
    let target = Target::new();
    symlink(&victim, &target.path).unwrap();
    download(&client, &url, &target).await.unwrap();
    assert!(
        !std::fs::symlink_metadata(&target.path)
            .unwrap()
            .is_symlink()
    );
    target.assert_holds(50, 20_000);
    assert_eq!(std::fs::read(&victim).unwrap(), b"not yours to write");
}

/// A URL that cannot be one is an error from the builder, not a panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_urls_are_errors() {
    let client = client();
    let target = Target::new();
    assert!(matches!(
        client.download_background("not a url", &target.path),
        Err(Error::InvalidUrl)
    ));
    let unsupported = client
        .download_background("ftp://127.0.0.1/file", &target.path)
        .unwrap()
        .send()
        .await;
    assert!(
        matches!(unsupported, Err(Error::InvalidUrl)),
        "{:?}",
        unsupported
    );
    assert_eq!(target.files(), [] as [&str; 0]);

    // The destination has to be somewhere a file can be.
    let directory = client
        .download_background("http://127.0.0.1:1/", target.dir.path())
        .unwrap()
        .send()
        .await;
    assert!(matches!(directory, Err(Error::Io(_))), "{:?}", directory);
}
