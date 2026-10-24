//! An iOS app that drives `frakt::background`, for checking background
//! downloads in the simulator; `run.sh` next to this file builds and
//! installs it. The launch arguments say what to do (`enqueue <job> <url>`,
//! `attach`, `jobs`, `cancel <job>`), and every event is appended as a JSON
//! line to `Documents/events.log` in the app's data container, with finished
//! files copied next to it.

#[cfg(not(target_os = "ios"))]
fn main() {
    eprintln!("This example is an iOS app; build it with examples/ios_background/run.sh");
}

#[cfg(target_os = "ios")]
fn main() {
    app::main()
}

#[cfg(target_os = "ios")]
mod app {
    use std::ffi::{CString, c_char, c_int};
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, LazyLock, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use block2::DynBlock;
    use frakt::background::{
        BackgroundDownloads, BackgroundHandler, BackgroundJob, FinishedDownload,
    };
    use objc2::rc::Retained;
    use objc2::runtime::{AnyObject, Bool, NSObject};
    use objc2::{ClassType, MainThreadOnly, define_class};
    use objc2_foundation::NSString;

    const IDENTIFIER: &str = "se.brendan.frakt.bgharness.downloads";

    static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    });

    static DOWNLOADS: OnceLock<BackgroundDownloads> = OnceLock::new();

    fn documents() -> PathBuf {
        let dir = PathBuf::from(std::env::var("HOME").unwrap()).join("Documents");
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    static LOG: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn log(event: serde_json::Value) {
        let _serial = LOG.lock().unwrap();
        let mut event = event;
        event["pid"] = std::process::id().into();
        event["t"] = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            .into();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(documents().join("events.log"))
            .unwrap();
        writeln!(file, "{}", event).unwrap();
    }

    fn blake3_file(path: &Path) -> std::io::Result<(String, u64)> {
        let mut file = std::fs::File::open(path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut len = 0u64;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            len += n as u64;
        }
        Ok((hasher.finalize().to_hex().to_string(), len))
    }

    struct Handler;

    impl BackgroundHandler for Handler {
        fn progress(&self, job: &str, downloaded: u64, total: Option<u64>) {
            // Log roughly every 4 MiB to keep the log short.
            if downloaded % (4 << 20) < 64 * 1024 {
                log(serde_json::json!({
                    "event": "progress", "job": job, "downloaded": downloaded, "total": total,
                }));
            }
        }

        fn finished(&self, download: FinishedDownload<'_>) {
            let hashed = blake3_file(download.file);
            let kept = documents().join(format!("{}.bin", download.job));
            let copied = std::fs::copy(download.file, &kept);
            log(serde_json::json!({
                "event": "finished",
                "job": download.job,
                "status": download.status.as_u16(),
                "file": download.file.display().to_string(),
                "blake3": hashed.as_ref().map(|(h, _)| h.clone()).unwrap_or_default(),
                "len": hashed.as_ref().map(|(_, n)| *n).unwrap_or_default(),
                "etag": download.headers.get("etag").and_then(|v| v.to_str().ok()),
                "kept": copied.is_ok(),
            }));
        }

        fn failed(&self, job: &str, error: frakt::Error, resumable: bool) {
            log(serde_json::json!({
                "event": "failed", "job": job, "error": error.to_string(), "resumable": resumable,
            }));
        }
    }

    fn downloads() -> &'static BackgroundDownloads {
        DOWNLOADS.get_or_init(|| {
            let downloads = BackgroundDownloads::new(IDENTIFIER, Arc::new(Handler)).unwrap();
            log(serde_json::json!({ "event": "attached", "identifier": IDENTIFIER }));
            downloads
        })
    }

    fn log_jobs() {
        RUNTIME.spawn(async {
            let jobs = downloads().jobs().await;
            log(serde_json::json!({
                "event": "jobs",
                "jobs": jobs.map(|jobs| jobs.iter().map(|job| serde_json::json!({
                    "id": job.id, "state": format!("{:?}", job.state),
                    "downloaded": job.downloaded, "total": job.total,
                })).collect::<Vec<_>>()).map_err(|e| e.to_string()).unwrap_or_default(),
            }));
        });
    }

    fn run_command() {
        let args: Vec<String> = std::env::args().skip(1).collect();
        log(serde_json::json!({ "event": "launched", "args": args }));
        let downloads = downloads();
        match args.first().map(String::as_str) {
            Some("enqueue") => {
                let job = BackgroundJob::new(args[1].clone(), args[2].parse().unwrap());
                let result = downloads.enqueue(job);
                log(serde_json::json!({
                    "event": "enqueued", "job": args[1], "result": format!("{:?}", result),
                }));
                // `enqueue <job> <url> interrupt <seconds>`: then cancel it as
                // a force-quit would, leaving resume data.
                if args.get(3).map(String::as_str) == Some("interrupt") {
                    let job = args[1].clone();
                    let delay: f64 = args[4].parse().unwrap();
                    std::thread::spawn(move || {
                        std::thread::sleep(std::time::Duration::from_secs_f64(delay));
                        let cancelled =
                            frakt::background::cancel_producing_resume_data(IDENTIFIER, &job);
                        log(serde_json::json!({
                            "event": "interrupted", "job": job, "had_task": cancelled,
                        }));
                    });
                }
            }
            Some("client") => {
                let url = args[1].clone();
                RUNTIME.spawn(async move {
                    let client = frakt::Client::builder()
                        .backend(frakt::BackendType::Foundation)
                        .build()
                        .unwrap();
                    let path = documents().join("client.bin");
                    let result = client
                        .download_background(url.as_str(), &path)
                        .session_identifier("se.brendan.frakt.bgharness.client")
                        .send()
                        .await;
                    log(match result {
                        Ok(response) => serde_json::json!({
                            "event": "client-finished",
                            "status": response.status.as_u16(),
                            "bytes": response.bytes_downloaded,
                            "blake3": blake3_file(&response.file_path).map(|(h, _)| h).unwrap_or_default(),
                        }),
                        Err(error) => serde_json::json!({
                            "event": "client-failed", "error": error.to_string(),
                        }),
                    });
                });
            }
            Some("cancel") => {
                let result = downloads.cancel(&args[1]);
                log(serde_json::json!({
                    "event": "cancelled", "job": args[1], "result": format!("{:?}", result),
                }));
            }
            Some("jobs") => log_jobs(),
            _ => {}
        }
        log_jobs();
    }

    define_class!(
        #[unsafe(super(NSObject))]
        #[thread_kind = MainThreadOnly]
        struct AppDelegate;

        impl AppDelegate {
            #[unsafe(method(application:didFinishLaunchingWithOptions:))]
            fn did_finish_launching(&self, _app: &AnyObject, _options: Option<&AnyObject>) -> Bool {
                run_command();
                Bool::YES
            }

            #[unsafe(method(application:handleEventsForBackgroundURLSession:completionHandler:))]
            fn handle_events(
                &self,
                _app: &AnyObject,
                identifier: &NSString,
                completion_handler: &DynBlock<dyn Fn()>,
            ) {
                log(serde_json::json!({
                    "event": "handle-events", "identifier": identifier.to_string(),
                }));
                let original = completion_handler.copy();
                let logged = block2::RcBlock::new(move || {
                    log(serde_json::json!({
                        "event": "completion-handler-called",
                        "main_thread": objc2::MainThreadMarker::new().is_some(),
                    }));
                    original.call(());
                });
                frakt::background::handle_events_for_background_session(
                    &identifier.to_string(),
                    &logged,
                );
                downloads();
            }
        }
    );

    #[link(name = "UIKit", kind = "framework")]
    unsafe extern "C" {
        fn UIApplicationMain(
            argc: c_int,
            argv: *mut *mut c_char,
            principal_class_name: *const NSString,
            delegate_class_name: *const NSString,
        ) -> c_int;
    }

    pub fn main() {
        let delegate = NSString::from_str(AppDelegate::class().name().to_str().unwrap());
        let args: Vec<CString> = std::env::args().map(|a| CString::new(a).unwrap()).collect();
        let mut argv: Vec<*mut c_char> = args.iter().map(|a| a.as_ptr() as *mut c_char).collect();
        unsafe {
            UIApplicationMain(
                argv.len() as c_int,
                argv.as_mut_ptr(),
                std::ptr::null(),
                Retained::as_ptr(&delegate),
            );
        }
    }
}
