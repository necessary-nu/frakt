//! The native half of android/bgtest, the app that exercises
//! frakt::background on a device: JNI entry points for its activity, and a
//! handler that records every event (with the finished file's size and
//! BLAKE3) in `events.jsonl` in the app's files directory.
//!
//! Built as a cdylib by android/bgtest/run-emulator-test.sh.

#[cfg(target_os = "android")]
mod harness {
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use frakt::background::{
        BackgroundDownloads, BackgroundHandler, BackgroundJob, FinishedDownload,
    };
    use jni::errors::LogErrorAndDefault;
    use jni::objects::{JClass, JObject, JString};
    use jni::{Env, EnvUnowned};

    static DOWNLOADS: Mutex<Option<BackgroundDownloads>> = Mutex::new(None);

    struct Recorder {
        log: PathBuf,
    }

    impl Recorder {
        fn record(&self, event: serde_json::Value) {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log)
            {
                let _ = writeln!(file, "{}", event);
            }
        }
    }

    impl BackgroundHandler for Recorder {
        fn progress(&self, job: &str, downloaded: u64, total: Option<u64>) {
            self.record(serde_json::json!({
                "event": "progress", "job": job, "downloaded": downloaded, "total": total,
            }));
        }

        fn finished(&self, download: FinishedDownload<'_>) {
            let bytes = std::fs::read(download.file).unwrap_or_default();
            self.record(serde_json::json!({
                "event": "finished",
                "job": download.job,
                "status": download.status.as_u16(),
                "size": bytes.len(),
                "blake3": blake3::hash(&bytes).to_hex().to_string(),
                "file": download.file,
            }));
        }

        fn failed(&self, job: &str, error: frakt::Error, resumable: bool) {
            self.record(serde_json::json!({
                "event": "failed", "job": job, "error": error.to_string(), "resumable": resumable,
            }));
        }
    }

    /// Run a native method's body with its JNI environment; a panic aborts,
    /// as from any `extern "system"` function.
    fn native<'local>(
        mut env: EnvUnowned<'local>,
        f: impl FnOnce(&mut Env<'local>) -> JString<'local>,
    ) -> JString<'local> {
        env.with_env_no_catch(|env| Ok::<_, jni::errors::Error>(f(env)))
            .resolve::<LogErrorAndDefault>()
    }

    fn string(env: &mut Env, value: &JString) -> String {
        value.try_to_string(env).unwrap_or_default()
    }

    fn reply<'local>(env: &mut Env<'local>, result: Result<String, String>) -> JString<'local> {
        let text = match result {
            Ok(text) => text,
            Err(error) => format!("error: {}", error),
        };
        env.new_string(text).unwrap_or_default()
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_se_brendan_frakt_bgtest_Harness_start<'local>(
        env: EnvUnowned<'local>,
        _class: JClass<'local>,
        context: JObject<'local>,
        identifier: JString<'local>,
        files_dir: JString<'local>,
    ) -> JString<'local> {
        native(env, |env| {
            let identifier = string(env, &identifier);
            let log = PathBuf::from(string(env, &files_dir)).join("events.jsonl");
            let result = (|| {
                frakt::backend::android::init(env, &context).map_err(|e| e.to_string())?;
                let mut downloads = DOWNLOADS.lock().unwrap();
                if downloads.is_none() {
                    *downloads = Some(
                        BackgroundDownloads::new(&identifier, Arc::new(Recorder { log }))
                            .map_err(|e| e.to_string())?,
                    );
                }
                Ok("started".to_string())
            })();
            reply(env, result)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_se_brendan_frakt_bgtest_Harness_enqueue<'local>(
        env: EnvUnowned<'local>,
        _class: JClass<'local>,
        job: JString<'local>,
        url: JString<'local>,
    ) -> JString<'local> {
        native(env, |env| {
            let job = string(env, &job);
            let url = string(env, &url);
            let result = (|| {
                let url = url.parse().map_err(|e: url::ParseError| e.to_string())?;
                let downloads = DOWNLOADS.lock().unwrap();
                let downloads = downloads.as_ref().ok_or("not started")?;
                downloads
                    .enqueue(BackgroundJob::new(job.clone(), url))
                    .map_err(|e| e.to_string())?;
                Ok(format!("enqueued {}", job))
            })();
            reply(env, result)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_se_brendan_frakt_bgtest_Harness_cancel<'local>(
        env: EnvUnowned<'local>,
        _class: JClass<'local>,
        job: JString<'local>,
    ) -> JString<'local> {
        native(env, |env| {
            let job = string(env, &job);
            let result = (|| {
                let downloads = DOWNLOADS.lock().unwrap();
                let downloads = downloads.as_ref().ok_or("not started")?;
                downloads.cancel(&job).map_err(|e| e.to_string())?;
                Ok(format!("cancelled {}", job))
            })();
            reply(env, result)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_se_brendan_frakt_bgtest_Harness_jobs<'local>(
        env: EnvUnowned<'local>,
        _class: JClass<'local>,
    ) -> JString<'local> {
        native(env, |env| {
            let result = (|| {
                let downloads = DOWNLOADS.lock().unwrap();
                let downloads = downloads.as_ref().ok_or("not started")?;
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .map_err(|e| e.to_string())?;
                let jobs = runtime
                    .block_on(downloads.jobs())
                    .map_err(|e| e.to_string())?;
                Ok(format!("{:?}", jobs))
            })();
            reply(env, result)
        })
    }
}
