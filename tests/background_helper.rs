//! Background downloads on the reqwest backend, run in the helper process.
//!
//! The helper is the host's own executable started again, and only a `main`
//! that calls `frakt::enable_background_helper` can be one, so this test has
//! no harness: it is the host. Its `main` is also every other program the
//! tests need, chosen by `FRAKT_TEST_ROLE`: hosts that exit or are killed in
//! mid-download, a host that makes the call from inside a runtime, and
//! executables that turn out not to be helpers at all.
//!
//! Each process started as a helper leaves a file named after its process
//! id in a directory the tests watch, which is how they know that a download
//! went through one.

#[cfg(unix)]
#[path = "support/server.rs"]
mod server;

fn main() {
    #[cfg(unix)]
    host::main();
    #[cfg(not(unix))]
    frakt::enable_background_helper();
}

#[cfg(unix)]
mod host {
    use super::server::{self, LAST_MODIFIED, Server};
    use frakt::{Auth, BackendType, Client, DownloadResponse, Error};
    use serde_json::json;
    use std::future::Future;
    use std::io::BufRead;
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    const HELPER: &str = "FRAKT_BACKGROUND_HELPER";
    const DESCRIPTORS: &str = "FRAKT_BACKGROUND_HELPER_DESCRIPTORS";
    const ROLE: &str = "FRAKT_TEST_ROLE";
    const HELPER_ROLE: &str = "FRAKT_TEST_HELPER_ROLE";
    const MARKERS: &str = "FRAKT_TEST_MARKERS";
    const URL: &str = "FRAKT_TEST_URL";
    const DESTINATION: &str = "FRAKT_TEST_DESTINATION";
    const PADDING: &str = "FRAKT_TEST_PADDING";
    const SIGPIPE: &str = "FRAKT_TEST_SIGPIPE";

    const NAME: &str = "download.bin";
    const PARTIAL: &str = "download.bin.frakt-partial";
    const SIDECAR: &str = "download.bin.frakt-partial.meta";

    /// Descriptors given to a process to see whether a helper keeps them:
    /// one below and one above the 256 that the old daemon stopped at.
    const PLANTED: [i32; 2] = [200, 1500];

    pub fn main() {
        let role = std::env::var(ROLE).unwrap_or_default();

        if std::env::var_os(HELPER).is_some() {
            if let Some(markers) = std::env::var_os(MARKERS) {
                let marker = Path::new(&markers).join(std::process::id().to_string());
                std::fs::write(marker, b"").unwrap();
            }
            match std::env::var(HELPER_ROLE).as_deref() {
                // Executables that are started as the helper and are not
                // one.
                Ok("exits") => std::process::exit(0),
                Ok("silent") => {
                    std::thread::sleep(Duration::from_secs(120));
                    std::process::exit(0);
                }
                // A helper whose pipe to the host is also held by a process
                // that outlives it.
                Ok("shares-its-stdout") => {
                    #[allow(clippy::zombie_processes)]
                    Command::new("sleep")
                        .arg("30")
                        .stdin(Stdio::null())
                        .stderr(Stdio::null())
                        .spawn()
                        .unwrap();
                }
                _ => {}
            }
        }
        // A host that has not kept Rust's start-up choice to ignore it.
        if std::env::var_os(SIGPIPE).is_some() {
            unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
        }

        match role.as_str() {
            "descriptors" => {
                println!("{}", json!(open_descriptors()));
                return;
            }
            // A host that never makes the call, or makes it late.
            "killed-without-helper" | "enables-inside-a-runtime" => {}
            _ => frakt::enable_background_helper(),
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        match role.as_str() {
            "" => {
                let markers = tempfile::tempdir().unwrap();
                // The helpers inherit it. Nothing else is running yet.
                unsafe { std::env::set_var(MARKERS, markers.path()) };
                raise_descriptor_limit();
                let failed = runtime.block_on(run_tests());
                std::process::exit(if failed == 0 { 0 } else { 1 });
            }
            "enables-inside-a-runtime" => runtime.block_on(async {
                frakt::enable_background_helper();
                host(&role).await
            }),
            _ => runtime.block_on(host(&role)),
        }
    }

    /// A host program other than the test runner: one download, its result
    /// on stdout.
    async fn host(role: &str) {
        let url = std::env::var(URL).unwrap();
        let destination = PathBuf::from(std::env::var_os(DESTINATION).unwrap());
        static KILL: std::sync::Once = std::sync::Once::new();
        let padding: usize = std::env::var(PADDING).map_or(0, |n| n.parse().unwrap());
        let role = role.to_string();
        let result = client()
            .download_background(url.as_str(), &destination)
            .unwrap()
            .header("X-Padding", "p".repeat(padding))
            .unwrap()
            .progress(move |_, _| match role.as_str() {
                "exits-in-mid-download" => std::process::exit(0),
                "killed-without-helper" => unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                },
                // Once only: the process id is anyone's after that.
                "kills-its-helper" => KILL.call_once(|| {
                    for helper in helpers() {
                        unsafe { libc::kill(helper, libc::SIGKILL) };
                    }
                }),
                _ => {}
            })
            .send()
            .await;
        let report = match result {
            Ok(response) => json!({
                "status": response.status.as_u16(),
                "bytes": response.bytes_downloaded,
            }),
            Err(error) => json!({ "error": format!("{:?}", error) }),
        };
        println!("{}", report);
    }

    /// Run this executable as another program.
    fn program(role: &str, url: &str, destination: &Path) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .env(ROLE, role)
            .env(URL, url)
            .env(DESTINATION, destination)
            .stdin(Stdio::null())
            .stderr(Stdio::inherit());
        command
    }

    /// Run a host to its end and return what it reported.
    async fn run_host(mut command: Command) -> (std::process::ExitStatus, serde_json::Value) {
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        let report = serde_json::from_slice(&output.stdout).unwrap_or(serde_json::Value::Null);
        (output.status, report)
    }

    fn client() -> Client {
        Client::builder()
            .backend(BackendType::Reqwest)
            .build()
            .expect("client")
    }

    fn open_descriptors() -> Vec<i32> {
        let listing = ["/proc/self/fd", "/dev/fd"]
            .iter()
            .find_map(|path| std::fs::read_dir(path).ok())
            .expect("a list of open descriptors");
        let mut listed: Vec<i32> = listing
            .map(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect();
        // Without the one the listing was read through.
        listed.retain(|&descriptor| unsafe { libc::fcntl(descriptor, libc::F_GETFD) } != -1);
        listed.sort();
        listed
    }

    /// Room for the high descriptor numbers the tests hand to a helper.
    fn raise_descriptor_limit() {
        unsafe {
            let mut limit: libc::rlimit = std::mem::zeroed();
            assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit), 0);
            limit.rlim_cur = limit.rlim_cur.max(limit.rlim_max.min(4096));
            assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &limit), 0);
        }
    }

    /// The process ids of the helpers started so far.
    fn helpers() -> Vec<i32> {
        let markers = std::env::var_os(MARKERS).unwrap();
        let mut ids: Vec<i32> = std::fs::read_dir(markers)
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect();
        ids.sort();
        ids
    }

    async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let started = Instant::now();
        while !done() {
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "timed out waiting until {}",
                what
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A destination in a directory of its own.
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

        fn files(&self) -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(self.dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names
        }

        fn assert_holds(&self, seed: u64, len: u64) {
            let content = std::fs::read(&self.path).unwrap();
            assert_eq!(content.len() as u64, len);
            assert_eq!(blake3::hash(&content), server::body_hash(seed, 0, len));
            assert_eq!(self.files(), [NAME]);
        }
    }

    fn header(response: &DownloadResponse, name: &str) -> String {
        response
            .headers
            .get(name)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
            .unwrap_or_else(|| panic!("missing {} header", name))
    }

    type Test = fn() -> Pin<Box<dyn Future<Output = ()> + Send>>;

    macro_rules! tests {
        ($($name:ident),* $(,)?) => {
            [$((stringify!($name), (|| Box::pin($name())) as Test)),*]
        };
    }

    /// Run the tests whose names contain the first argument, or all of
    /// them, and return how many failed.
    async fn run_tests() -> usize {
        let tests = tests![
            download_is_byte_exact,
            concurrent_downloads,
            planted_partial_file_is_resumed,
            errors_keep_their_variants,
            one_download_per_destination,
            relative_destination_is_relative_to_the_host,
            helper_outlives_a_dropped_future,
            helper_outlives_its_host,
            host_may_enable_inside_a_runtime,
            killed_download_is_resumed_by_another_process,
            helper_keeps_no_inherited_descriptors,
            an_executable_that_is_no_helper_is_not_relied_on,
            a_silent_helper_is_killed,
            writing_the_job_cannot_kill_the_host,
            a_helper_that_dies_is_an_error,
            a_helper_that_dies_is_an_error_without_end_of_output,
        ];
        let filter = std::env::args()
            .nth(1)
            .filter(|argument| !argument.starts_with('-'))
            .unwrap_or_default();
        let wanted = |name: &str| name.contains(&filter);
        let mut failed = 0;
        let mut finished = |name: &str, passed: bool| {
            println!("test {} ... {}", name, if passed { "ok" } else { "FAILED" });
            failed += usize::from(!passed);
        };

        // The slowest runs alongside the rest, which run one at a time so
        // that each can count the helpers it started. Its own is started
        // before any of theirs.
        let slow = "transport_failures_are_retried_in_the_helper";
        let slow = wanted(slow).then(|| {
            println!("test {} ... started", slow);
            let task = tokio::spawn(transport_failures_are_retried_in_the_helper());
            (slow, task)
        });
        if slow.is_some() {
            wait_until("the first helper has started", || !helpers().is_empty()).await;
        }

        for (name, test) in tests.iter().filter(|(name, _)| wanted(name)) {
            finished(name, tokio::spawn(test()).await.is_ok());
        }
        if let Some((name, task)) = slow {
            finished(name, task.await.is_ok());
        }
        println!("\ntest result: {} failed", failed);
        failed
    }

    async fn download_is_byte_exact() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers().len();
        let len = 3 * (1 << 20) + 123;
        let url = server.url(&format!("/bytes?len={}&seed=61&id=exact", len));

        let jar = frakt::CookieJar::new(frakt::backend::Backend::reqwest().unwrap());
        jar.add_cookie(frakt::Cookie::new("session", "abc123").domain("127.0.0.1"))
            .unwrap();
        let client = Client::builder()
            .backend(BackendType::Reqwest)
            .user_agent("frakt-helper-test/1.0")
            .header("X-Client-Default", "from the client")
            .unwrap()
            .cookie_jar(jar)
            .build()
            .unwrap();

        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let response = client
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .session_identifier("../../hostile")
            .auth(Auth::bearer("secret-token"))
            .unwrap()
            .header("X-Custom-Header", "custom value")
            .unwrap()
            .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
            .send()
            .await
            .unwrap();

        assert_eq!(helpers().len(), before + 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.file_path, target.path);
        assert_eq!(response.bytes_downloaded, len);
        assert_eq!(header(&response, "content-length"), len.to_string());
        assert_eq!(header(&response, "etag"), server::etag(61, len));
        assert_eq!(header(&response, "last-modified"), LAST_MODIFIED);
        target.assert_holds(61, len);

        let calls = calls.lock().unwrap().clone();
        assert!(!calls.is_empty());
        assert!(calls.windows(2).all(|w| w[0].0 <= w[1].0), "{:?}", calls);
        assert_eq!(calls.last(), Some(&(len, Some(len))));

        // The helper built its client from the host's configuration, and
        // was given the request's headers.
        let requests = server.requests_with_id("exact");
        assert_eq!(requests.len(), 1);
        let sent = |name| requests[0].header_str(name);
        assert_eq!(
            sent("authorization").as_deref(),
            Some("Bearer secret-token")
        );
        assert_eq!(sent("x-custom-header").as_deref(), Some("custom value"));
        assert_eq!(sent("x-client-default").as_deref(), Some("from the client"));
        assert_eq!(sent("user-agent").as_deref(), Some("frakt-helper-test/1.0"));
        assert_eq!(sent("cookie").as_deref(), Some("session=abc123"));
        assert_eq!(sent("accept-encoding").as_deref(), Some("identity"));
        assert_eq!(sent("range"), None);
    }

    /// Helpers started from several of the runtime's threads at once.
    async fn concurrent_downloads() {
        let server = Server::start().await;
        let client = client();
        let before = helpers().len();
        let targets: Vec<Target> = (0..6).map(|_| Target::new()).collect();
        let len = 700_000;

        let downloads = targets.iter().enumerate().map(|(index, target)| {
            let url = server.url(&format!("/bytes?len={}&seed={}", len, 70 + index));
            let download = client
                .download_background(url.as_str(), &target.path)
                .unwrap()
                .session_identifier("shared");
            tokio::spawn(async move { download.send().await })
        });
        for download in downloads.collect::<Vec<_>>() {
            assert_eq!(download.await.unwrap().unwrap().status, 200);
        }
        for (index, target) in targets.iter().enumerate() {
            target.assert_holds(70 + index as u64, len);
        }
        assert_eq!(helpers().len(), before + targets.len());
    }

    async fn planted_partial_file_is_resumed() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers().len();
        let (len, have) = (300_000, 100_000);
        let url = server.url(&format!("/bytes?len={}&seed=62&id=planted", len));
        std::fs::write(target.dir.path().join(PARTIAL), server::body(62, 0, have)).unwrap();
        let sidecar = json!({ "url": url, "etag": server::etag(62, len), "total": len });
        std::fs::write(target.dir.path().join(SIDECAR), sidecar.to_string()).unwrap();

        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let response = client()
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .progress(move |done, total| recorded.lock().unwrap().push((done, total)))
            .send()
            .await
            .unwrap();
        assert_eq!(helpers().len(), before + 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.bytes_downloaded, len);
        assert_eq!(
            header(&response, "content-range"),
            format!("bytes {}-{}/{}", have, len - 1, len)
        );
        target.assert_holds(62, len);

        let requests = server.requests_with_id("planted");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].header_str("range"),
            Some(format!("bytes={}-", have))
        );
        assert_eq!(
            requests[0].header_str("if-range"),
            Some(server::etag(62, len))
        );
        let calls = calls.lock().unwrap().clone();
        assert!(calls.first().unwrap().0 > have, "{:?}", calls);
        assert_eq!(calls.last(), Some(&(len, Some(len))));
    }

    async fn errors_keep_their_variants() {
        let server = Server::start().await;
        let client = client();
        let target = Target::new();
        std::fs::write(&target.path, b"precious").unwrap();
        let before = helpers().len();
        let download = |url: String| {
            let download = client.download_background(url.as_str(), &target.path);
            async move { download.unwrap().send().await }
        };

        let refused = download(server.url("/status?code=404&id=refused")).await;
        assert!(
            matches!(refused, Err(Error::HttpStatus(status)) if status == 404),
            "{:?}",
            refused
        );
        assert_eq!(server.requests_with_id("refused").len(), 1);

        let untrusted = download(server.tls_url("/bytes?len=1000")).await;
        assert!(
            matches!(untrusted, Err(Error::Tls { .. })),
            "{:?}",
            untrusted
        );

        let directory = client
            .download_background(server.url("/bytes?len=1000").as_str(), target.dir.path())
            .unwrap()
            .send()
            .await;
        assert!(matches!(directory, Err(Error::Io(_))), "{:?}", directory);

        assert_eq!(helpers().len(), before + 3);
        assert_eq!(std::fs::read(&target.path).unwrap(), b"precious");
        assert_eq!(target.files(), [NAME]);

        // And a status the caller accepts is the download.
        let response = client
            .download_background(server.url("/status?code=404").as_str(), &target.path)
            .unwrap()
            .error_for_status(false)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 404);
        assert_eq!(response.bytes_downloaded, 10);
        assert_eq!(std::fs::read(&target.path).unwrap(), b"status 404");
    }

    /// The helper does the retrying, and what it gives up with comes back
    /// as the error it was.
    async fn transport_failures_are_retried_in_the_helper() {
        let server = Server::start().await;
        let target = Target::new();
        let url = server.url("/truncate?len=100000&after=20000&id=short");

        let failed = client()
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .send()
            .await;
        assert!(matches!(failed, Err(Error::Network { .. })), "{:?}", failed);
        assert_eq!(server.requests_with_id("short").len(), 5);
        assert_eq!(target.files(), [] as [&str; 0]);
    }

    /// The helper runs where the host does, so a destination that is not an
    /// absolute path means the same file to both.
    async fn relative_destination_is_relative_to_the_host() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers().len();
        let url = server.url("/bytes?len=500000&seed=82");

        let mut host = program("host", &url, Path::new(NAME));
        host.current_dir(target.dir.path());
        let (status, report) = run_host(host).await;
        assert!(status.success(), "{:?}", status);
        assert_eq!(report, json!({ "status": 200, "bytes": 500_000 }));
        target.assert_holds(82, 500_000);
        assert_eq!(helpers().len(), before + 1);
    }

    /// The lock on the partial file holds between processes.
    async fn one_download_per_destination() {
        let server = Server::start().await;
        let client = client();
        let target = Target::new();
        let before = helpers().len();
        let len = 400_000;
        let url = server.url(&format!(
            "/bytes?len={}&seed=63&chunk=8192&rate=400000",
            len
        ));

        let seen = Arc::new(AtomicU64::new(0));
        let progress = seen.clone();
        let first = client
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .progress(move |done, _| progress.store(done, Ordering::SeqCst))
            .send();
        let first = tokio::spawn(first);
        wait_until("the first download has begun", || {
            seen.load(Ordering::SeqCst) > 0
        })
        .await;

        let second = client
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .send()
            .await;
        match second {
            Err(Error::Io(message)) => {
                assert!(message.contains("already in progress"), "{}", message)
            }
            other => panic!("the second download was not refused: {:?}", other),
        }

        assert_eq!(first.await.unwrap().unwrap().status, 200);
        target.assert_holds(63, len);
        assert_eq!(helpers().len(), before + 2);
    }

    async fn helper_outlives_a_dropped_future() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers();
        let len = 400_000;
        let url = server.url(&format!(
            "/bytes?len={}&seed=64&chunk=8192&rate=400000&track=dropped",
            len
        ));

        let seen = Arc::new(AtomicU64::new(0));
        let progress = seen.clone();
        let mut download = Box::pin(
            client()
                .download_background(url.as_str(), &target.path)
                .unwrap()
                .progress(move |done, _| progress.store(done, Ordering::SeqCst))
                .send(),
        );
        while seen.load(Ordering::SeqCst) == 0 {
            tokio::select! {
                result = &mut download => panic!("finished too soon: {:?}", result),
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
        drop(download);
        assert!(!target.path.exists());

        // It leads a session of its own, out of reach of what ends ours.
        let started: Vec<i32> = helpers()
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect();
        assert_eq!(started.len(), 1);
        assert_eq!(unsafe { libc::getsid(started[0]) }, started[0]);
        assert_ne!(unsafe { libc::getsid(0) }, started[0]);

        wait_until("the helper has published the file", || target.path.exists()).await;
        target.assert_holds(64, len);
        assert!(server.track("dropped").completed.load(Ordering::SeqCst));
    }

    async fn helper_outlives_its_host() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers().len();
        let len = 400_000;
        let url = server.url(&format!(
            "/bytes?len={}&seed=65&chunk=8192&rate=200000&id=orphan",
            len
        ));

        let (status, _) = run_host(program("exits-in-mid-download", &url, &target.path)).await;
        assert!(status.success(), "{:?}", status);
        assert!(!target.path.exists());

        wait_until("the helper has published the file", || target.path.exists()).await;
        target.assert_holds(65, len);
        assert_eq!(server.requests_with_id("orphan").len(), 1);
        assert_eq!(helpers().len(), before + 1);
    }

    /// A `#[tokio::main]` program makes the call with the runtime running.
    async fn host_may_enable_inside_a_runtime() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers().len();
        let url = server.url("/bytes?len=500000&seed=66");

        let host = program("enables-inside-a-runtime", &url, &target.path);
        let (status, report) = run_host(host).await;
        assert!(status.success(), "{:?}", status);
        assert_eq!(report, json!({ "status": 200, "bytes": 500_000 }));
        target.assert_holds(66, 500_000);
        assert_eq!(helpers().len(), before + 1);
    }

    /// A host with no helper that dies in mid-download leaves what another
    /// process, here a helper, carries on from.
    async fn killed_download_is_resumed_by_another_process() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers().len();
        let len = 400_000;
        let url = server.url(&format!(
            "/bytes?len={}&seed=67&chunk=8192&rate=400000&id=killed",
            len
        ));

        let (status, _) = run_host(program("killed-without-helper", &url, &target.path)).await;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert_eq!(helpers().len(), before);
        assert_eq!(target.files(), [PARTIAL, SIDECAR]);
        let have = std::fs::metadata(target.dir.path().join(PARTIAL))
            .unwrap()
            .len();
        assert!(have > 0 && have < len, "{}", have);

        let response = client()
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.bytes_downloaded, len);
        target.assert_holds(67, len);
        assert_eq!(helpers().len(), before + 1);

        let requests = server.requests_with_id("killed");
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].header_str("range"),
            Some(format!("bytes={}-", have))
        );
        assert_eq!(
            requests[1].header_str("if-range"),
            Some(server::etag(67, len))
        );
    }

    /// Start this executable with descriptors that survive exec, as the
    /// program `role` names or, without one, as the helper, and return the
    /// first line it prints.
    async fn first_line_with_planted_descriptors(role: Option<&'static str>) -> serde_json::Value {
        tokio::task::spawn_blocking(move || {
            let mut command = Command::new(std::env::current_exe().unwrap());
            match role {
                Some(role) => command.env(ROLE, role),
                None => command.env(HELPER, "1").env(DESCRIPTORS, "1"),
            };
            command
                .env_remove(MARKERS)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            unsafe {
                command.pre_exec(|| {
                    for descriptor in PLANTED {
                        if libc::dup2(0, descriptor) == -1 {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    Ok(())
                });
            }
            let mut child = command.spawn().unwrap();
            // No job: the helper gives up once it has made its report.
            drop(child.stdin.take());
            let mut line = String::new();
            std::io::BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            child.wait().unwrap();
            serde_json::from_str(&line).unwrap()
        })
        .await
        .unwrap()
    }

    async fn helper_keeps_no_inherited_descriptors() {
        // Any other program has them, along with whatever whoever started
        // these tests left open for every child: a CI agent hands down
        // descriptors of its own.
        let inherited = first_line_with_planted_descriptors(Some("descriptors")).await;
        let inherited: Vec<i32> = serde_json::from_value(inherited).unwrap();
        for descriptor in PLANTED {
            assert!(inherited.contains(&descriptor), "{:?}", inherited);
        }

        let kept = first_line_with_planted_descriptors(None).await;
        assert_eq!(kept, json!({ "event": "descriptors", "open": [0, 1, 2] }));
    }

    /// If what was started does not say that it is a helper, the download
    /// is done by the host itself, once.
    async fn an_executable_that_is_no_helper_is_not_relied_on() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers().len();
        let url = server.url("/bytes?len=500000&seed=68&id=no-helper");

        let mut host = program("host", &url, &target.path);
        host.env(HELPER_ROLE, "exits");
        let (status, report) = run_host(host).await;
        assert!(status.success(), "{:?}", status);
        assert_eq!(report, json!({ "status": 200, "bytes": 500_000 }));
        target.assert_holds(68, 500_000);
        assert_eq!(server.requests_with_id("no-helper").len(), 1);
        assert_eq!(helpers().len(), before + 1);
    }

    /// One that says nothing at all is not left running beside the
    /// download that replaces it.
    async fn a_silent_helper_is_killed() {
        let server = Server::start().await;
        let target = Target::new();
        let before = helpers();
        let url = server.url("/bytes?len=500000&seed=69&id=silent");

        let mut host = program("host", &url, &target.path);
        host.env(HELPER_ROLE, "silent");
        let started = Instant::now();
        let (status, report) = run_host(host).await;
        assert!(status.success(), "{:?}", status);
        assert_eq!(report, json!({ "status": 200, "bytes": 500_000 }));
        assert!(started.elapsed() < Duration::from_secs(60));
        target.assert_holds(69, 500_000);
        assert_eq!(server.requests_with_id("silent").len(), 1);

        let silent: Vec<i32> = helpers()
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect();
        assert_eq!(silent.len(), 1);
        let gone = unsafe { libc::kill(silent[0], 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        assert!(gone, "process {} is still there", silent[0]);
    }

    /// A job too large for the pipe is still being written when a helper
    /// that never reads it is gone. A host with the default disposition for
    /// SIGPIPE must not die of that.
    async fn writing_the_job_cannot_kill_the_host() {
        let server = Server::start().await;
        let target = Target::new();
        let url = server.url("/bytes?len=500000&seed=80&id=big-job");

        let mut host = program("host", &url, &target.path);
        host.env(HELPER_ROLE, "exits")
            .env(SIGPIPE, "default")
            .env(PADDING, "40000");
        let (status, report) = run_host(host).await;
        assert!(status.success(), "{:?}", status);
        assert_eq!(report, json!({ "status": 200, "bytes": 500_000 }));
        target.assert_holds(80, 500_000);
        let requests = server.requests_with_id("big-job");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].header("x-padding").map(<[u8]>::len),
            Some(40_000)
        );
    }

    /// A helper that is gone without a result is an error for the caller.
    async fn a_helper_that_dies_is_an_error() {
        killed_helper(false).await
    }

    /// And not a wait without end when nothing shows that its output has
    /// ended.
    async fn a_helper_that_dies_is_an_error_without_end_of_output() {
        killed_helper(true).await
    }

    /// A host kills its helper in mid-download. What the helper had
    /// downloaded is there for the next download.
    async fn killed_helper(shares_its_stdout: bool) {
        let server = Server::start().await;
        let target = Target::new();
        let markers = tempfile::tempdir().unwrap();
        let len = 400_000;
        let id = if shares_its_stdout {
            "died-shared"
        } else {
            "died"
        };
        let url = server.url(&format!(
            "/bytes?len={}&seed=81&chunk=8192&rate=200000&id={}",
            len, id
        ));

        let mut host = program("kills-its-helper", &url, &target.path);
        host.env(MARKERS, markers.path());
        if shares_its_stdout {
            host.env(HELPER_ROLE, "shares-its-stdout");
        }
        let started = Instant::now();
        let (status, report) = run_host(host).await;
        assert!(status.success(), "{:?}", status);
        let error = report["error"].as_str().unwrap_or_default();
        assert!(error.contains("exited without a result"), "{}", report);
        assert!(started.elapsed() < Duration::from_secs(15));
        assert_eq!(target.files(), [PARTIAL, SIDECAR]);

        let response = client()
            .download_background(url.as_str(), &target.path)
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        target.assert_holds(81, len);
        assert_eq!(server.requests_with_id(id).len(), 2);
    }
}
