// Android background downloads using WorkManager

use crate::{Error, Result};
use jni::{
    JNIEnv, JavaVM,
    objects::{GlobalRef, JClass, JObject, JString},
    sys::jint,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use url::Url;

// Global storage for progress callbacks
static PROGRESS_CALLBACKS: once_cell::sync::Lazy<
    Mutex<HashMap<i64, Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

static NEXT_PROGRESS_ID: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);

fn register_progress_callback(
    callback: Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>,
) -> i64 {
    let id = NEXT_PROGRESS_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    PROGRESS_CALLBACKS.lock().unwrap().insert(id, callback);
    id
}

fn unregister_progress_callback(id: i64) {
    PROGRESS_CALLBACKS.lock().unwrap().remove(&id);
}

/// Initialize WorkManager if not already initialized
fn ensure_workmanager_initialized(jvm: &JavaVM) -> Result<()> {
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let context = super::application_context()?;

    // Try to get WorkManager instance to see if it's initialized
    let work_manager_class = env
        .find_class("androidx/work/WorkManager")
        .map_err(|e| Error::Internal(format!("Failed to find WorkManager class: {}", e)))?;

    // Try getInstance - if it throws, we need to initialize
    let _ = env.call_static_method(
        &work_manager_class,
        "getInstance",
        "(Landroid/content/Context;)Landroidx/work/WorkManager;",
        &[context.as_obj().into()],
    );

    // Check if there was an exception (WorkManager not initialized)
    if env.exception_check().unwrap_or(false) {
        env.exception_clear().ok();

        tracing::debug!("Initializing WorkManager with custom DexWorkerFactory...");

        // Get DEX classloader from callback module
        let dex_classloader_ref = crate::backend::android::callback::get_dex_classloader(&mut env)?;

        // Load DexWorkerFactory class from DEX
        let worker_factory_class = crate::backend::android::callback::load_class_from_dex(
            &mut env,
            "se.brendan.frakt.DexWorkerFactory",
        )?;

        // Create DexWorkerFactory instance with DEX classloader
        let worker_factory = env
            .new_object(
                worker_factory_class,
                "(Ljava/lang/ClassLoader;)V",
                &[(dex_classloader_ref.as_obj()).into()],
            )
            .map_err(|e| Error::Internal(format!("Failed to create DexWorkerFactory: {}", e)))?;

        // Create WorkManager Configuration with custom worker factory
        let config_class = env
            .find_class("androidx/work/Configuration$Builder")
            .map_err(|e| Error::Internal(format!("Failed to find Configuration.Builder: {}", e)))?;

        let config_builder = env.new_object(config_class, "()V", &[]).map_err(|e| {
            Error::Internal(format!("Failed to create Configuration.Builder: {}", e))
        })?;

        // Set custom worker factory
        env.call_method(
            &config_builder,
            "setWorkerFactory",
            "(Landroidx/work/WorkerFactory;)Landroidx/work/Configuration$Builder;",
            &[(&worker_factory).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to set worker factory: {}", e)))?;

        // Set minimum logging level to DEBUG
        let log_class = env
            .find_class("android/util/Log")
            .map_err(|e| Error::Internal(format!("Failed to find Log class: {}", e)))?;

        let debug_level = env
            .get_static_field(&log_class, "DEBUG", "I")
            .map_err(|e| Error::Internal(format!("Failed to get DEBUG constant: {}", e)))?
            .i()
            .map_err(|e| Error::Internal(format!("Failed to convert DEBUG to int: {}", e)))?;

        env.call_method(
            &config_builder,
            "setMinimumLoggingLevel",
            "(I)Landroidx/work/Configuration$Builder;",
            &[(debug_level).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to set minimum logging level: {}", e)))?;

        let config = env
            .call_method(
                &config_builder,
                "build",
                "()Landroidx/work/Configuration;",
                &[],
            )
            .map_err(|e| Error::Internal(format!("Failed to build Configuration: {}", e)))?
            .l()
            .map_err(|e| Error::Internal(format!("Failed to get Configuration object: {}", e)))?;

        // Initialize WorkManager
        env.call_static_method(
            work_manager_class,
            "initialize",
            "(Landroid/content/Context;Landroidx/work/Configuration;)V",
            &[context.as_obj().into(), (&config).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to initialize WorkManager: {}", e)))?;

        tracing::debug!("WorkManager initialized successfully with DexWorkerFactory");
    }

    // Pre-register DownloadProgressCallback native methods
    let progress_class = crate::backend::android::callback::load_class_from_dex(
        &mut env,
        "se.brendan.frakt.DownloadProgressCallback",
    )?;
    register_progress_callback_methods(&mut env, &progress_class)?;

    Ok(())
}

/// Execute a background download using Android's WorkManager
pub async fn execute_background_download(
    jvm: &JavaVM,
    url: Url,
    file_path: PathBuf,
    _session_identifier: Option<String>,
    headers: http::HeaderMap,
    _error_for_status: bool,
    progress_callback: Option<Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
) -> Result<crate::client::download::DownloadResponse> {
    // Ensure WorkManager is initialized
    ensure_workmanager_initialized(jvm)?;

    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    // Get application context
    let context = super::application_context()?;

    // Get WorkManager instance
    let work_manager_class = env
        .find_class("androidx/work/WorkManager")
        .map_err(|e| Error::Internal(format!("Failed to find WorkManager class: {}", e)))?;

    let work_manager = env
        .call_static_method(
            work_manager_class,
            "getInstance",
            "(Landroid/content/Context;)Landroidx/work/WorkManager;",
            &[context.as_obj().into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to get WorkManager instance: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get WorkManager object: {}", e)))?;

    // Build input data
    let data_builder_class = env
        .find_class("androidx/work/Data$Builder")
        .map_err(|e| Error::Internal(format!("Failed to find Data.Builder class: {}", e)))?;

    let data_builder = env
        .new_object(data_builder_class, "()V", &[])
        .map_err(|e| Error::Internal(format!("Failed to create Data.Builder: {}", e)))?;

    // Add URL
    let url_key = env
        .new_string("url")
        .map_err(|e| Error::Internal(format!("Failed to create URL key string: {}", e)))?;
    let url_value = env
        .new_string(url.as_str())
        .map_err(|e| Error::Internal(format!("Failed to create URL value string: {}", e)))?;
    env.call_method(
        &data_builder,
        "putString",
        "(Ljava/lang/String;Ljava/lang/String;)Landroidx/work/Data$Builder;",
        &[(&url_key).into(), (&url_value).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to put URL in data: {}", e)))?;

    // Add file path
    let path_key = env
        .new_string("file_path")
        .map_err(|e| Error::Internal(format!("Failed to create path key string: {}", e)))?;
    let path_value = env
        .new_string(file_path.to_string_lossy().as_ref())
        .map_err(|e| Error::Internal(format!("Failed to create path value string: {}", e)))?;
    env.call_method(
        &data_builder,
        "putString",
        "(Ljava/lang/String;Ljava/lang/String;)Landroidx/work/Data$Builder;",
        &[(&path_key).into(), (&path_value).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to put file path in data: {}", e)))?;

    // Serialize headers to JSON
    let headers_json =
        serde_json::to_string(&headers_to_map(&headers)).unwrap_or_else(|_| "{}".to_string());
    let headers_key = env
        .new_string("headers")
        .map_err(|e| Error::Internal(format!("Failed to create headers key string: {}", e)))?;
    let headers_value = env
        .new_string(&headers_json)
        .map_err(|e| Error::Internal(format!("Failed to create headers value string: {}", e)))?;
    env.call_method(
        &data_builder,
        "putString",
        "(Ljava/lang/String;Ljava/lang/String;)Landroidx/work/Data$Builder;",
        &[(&headers_key).into(), (&headers_value).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to put headers in data: {}", e)))?;

    // Build data
    let input_data = env
        .call_method(&data_builder, "build", "()Landroidx/work/Data;", &[])
        .map_err(|e| Error::Internal(format!("Failed to build input data: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get Data object: {}", e)))?;

    // Load DownloadWorker class from our DEX file
    let worker_class = crate::backend::android::callback::load_class_from_dex(
        &mut env,
        "se.brendan.frakt.DownloadWorker",
    )?;
    register_download_worker_methods(&mut env, &worker_class)?;

    // Create OneTimeWorkRequest.Builder
    let request_builder_class = env
        .find_class("androidx/work/OneTimeWorkRequest$Builder")
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to find OneTimeWorkRequest.Builder class: {}",
                e
            ))
        })?;

    let request_builder = env
        .new_object(
            request_builder_class,
            "(Ljava/lang/Class;)V",
            &[(&worker_class).into()],
        )
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to create OneTimeWorkRequest.Builder: {}",
                e
            ))
        })?;

    // Set input data
    env.call_method(
        &request_builder,
        "setInputData",
        "(Landroidx/work/Data;)Landroidx/work/WorkRequest$Builder;",
        &[(&input_data).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to set input data: {}", e)))?;

    // Build work request
    let work_request = env
        .call_method(
            &request_builder,
            "build",
            "()Landroidx/work/WorkRequest;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to build work request: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get WorkRequest object: {}", e)))?;

    // Get work ID before enqueueing
    let work_id = env
        .call_method(&work_request, "getId", "()Ljava/util/UUID;", &[])
        .map_err(|e| Error::Internal(format!("Failed to get work ID: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get UUID object: {}", e)))?;

    let work_id_string = env
        .call_method(&work_id, "toString", "()Ljava/lang/String;", &[])
        .map_err(|e| Error::Internal(format!("Failed to convert UUID to string: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get string object: {}", e)))?;

    let work_id_str: String = env
        .get_string(&JString::from(work_id_string))
        .map_err(|e| Error::Internal(format!("Failed to get work ID string: {}", e)))?
        .into();

    // In a test/stub environment, we don't have a real WorkManager executor.
    // Instead, call our BackgroundDownloader.performDownload() directly.

    // Add progress callback ID if we have one
    let progress_id = if let Some(callback) = progress_callback {
        let id = register_progress_callback(callback);

        let progress_key = env
            .new_string("progress_handler_id")
            .map_err(|e| Error::Internal(format!("Failed to create progress key string: {}", e)))?;
        env.call_method(
            &data_builder,
            "putLong",
            "(Ljava/lang/String;J)Landroidx/work/Data$Builder;",
            &[(&progress_key).into(), (id as i64).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to put progress ID in data: {}", e)))?;

        Some(id)
    } else {
        None
    };

    // Build the Data object
    let input_data = env
        .call_method(&data_builder, "build", "()Landroidx/work/Data;", &[])
        .map_err(|e| Error::Internal(format!("Failed to build Data: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get Data object: {}", e)))?;

    // Load DownloadWorker class from DEX
    let worker_class = crate::backend::android::callback::load_class_from_dex(
        &mut env,
        "se.brendan.frakt.DownloadWorker",
    )?;
    register_download_worker_methods(&mut env, &worker_class)?;

    // Create OneTimeWorkRequest.Builder
    let request_builder_class = env
        .find_class("androidx/work/OneTimeWorkRequest$Builder")
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to find OneTimeWorkRequest.Builder class: {}",
                e
            ))
        })?;

    let request_builder = env
        .new_object(
            request_builder_class,
            "(Ljava/lang/Class;)V",
            &[(&worker_class).into()],
        )
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to create OneTimeWorkRequest.Builder: {}",
                e
            ))
        })?;

    // Set input data
    env.call_method(
        &request_builder,
        "setInputData",
        "(Landroidx/work/Data;)Landroidx/work/WorkRequest$Builder;",
        &[(&input_data).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to set input data: {}", e)))?;

    // Build work request
    let work_request = env
        .call_method(
            &request_builder,
            "build",
            "()Landroidx/work/WorkRequest;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to build work request: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get WorkRequest object: {}", e)))?;

    // Get work ID
    let work_id = env
        .call_method(&work_request, "getId", "()Ljava/util/UUID;", &[])
        .map_err(|e| Error::Internal(format!("Failed to get work ID: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get UUID object: {}", e)))?;

    let work_id_string = env
        .call_method(&work_id, "toString", "()Ljava/lang/String;", &[])
        .map_err(|e| Error::Internal(format!("Failed to convert UUID to string: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get string object: {}", e)))?;

    let work_id_str: String = env
        .get_string(&JString::from(work_id_string))
        .map_err(|e| Error::Internal(format!("Failed to get work ID string: {}", e)))?
        .into();

    // Enqueue work
    env.call_method(
        &work_manager,
        "enqueue",
        "(Landroidx/work/WorkRequest;)Landroidx/work/Operation;",
        &[(&work_request).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to enqueue work: {}", e)))?;

    tracing::debug!("Enqueued background download with work ID: {}", work_id_str);

    // Poll for completion
    let start_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);
    let mut iteration = 0;

    loop {
        iteration += 1;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        if start_time.elapsed() > timeout {
            if let Some(id) = progress_id {
                unregister_progress_callback(id);
            }
            return Err(Error::Internal(format!(
                "Download timed out after {} seconds",
                timeout.as_secs()
            )));
        }

        // Get WorkInfo
        let work_info_future = env
            .call_method(
                &work_manager,
                "getWorkInfoById",
                "(Ljava/util/UUID;)Lcom/google/common/util/concurrent/ListenableFuture;",
                &[(&work_id).into()],
            )
            .map_err(|e| Error::Internal(format!("Failed to get WorkInfo future: {}", e)))?
            .l()
            .map_err(|e| Error::Internal(format!("Failed to get future object: {}", e)))?;

        let work_info = env
            .call_method(&work_info_future, "get", "()Ljava/lang/Object;", &[])
            .map_err(|e| Error::Internal(format!("Failed to get WorkInfo: {}", e)))?
            .l()
            .map_err(|e| Error::Internal(format!("Failed to get WorkInfo object: {}", e)))?;

        let state = env
            .call_method(
                &work_info,
                "getState",
                "()Landroidx/work/WorkInfo$State;",
                &[],
            )
            .map_err(|e| Error::Internal(format!("Failed to get work state: {}", e)))?
            .l()
            .map_err(|e| Error::Internal(format!("Failed to get State object: {}", e)))?;

        let state_name = env
            .call_method(&state, "name", "()Ljava/lang/String;", &[])
            .map_err(|e| Error::Internal(format!("Failed to get state name: {}", e)))?
            .l()
            .map_err(|e| Error::Internal(format!("Failed to get state name string: {}", e)))?;

        let state_str: String = env
            .get_string(&JString::from(state_name))
            .map_err(|e| Error::Internal(format!("Failed to convert state name: {}", e)))?
            .into();

        if iteration % 10 == 0 {
            tracing::debug!("Download status: {} (iteration {})", state_str, iteration);
        }

        match state_str.as_str() {
            "SUCCEEDED" => {
                tracing::debug!("Background download completed successfully");
                break;
            }
            "FAILED" | "CANCELLED" => {
                if let Some(id) = progress_id {
                    unregister_progress_callback(id);
                }
                return Err(Error::Internal(format!(
                    "Download failed with state: {}",
                    state_str
                )));
            }
            _ => {
                // Still ENQUEUED, RUNNING, or BLOCKED - continue polling
            }
        }
    }

    // Unregister progress callback
    if let Some(id) = progress_id {
        unregister_progress_callback(id);
    }

    // Get the actual bytes downloaded by checking the file size
    let bytes_downloaded = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);

    // Return with success status
    Ok(crate::client::download::DownloadResponse {
        file_path,
        bytes_downloaded,
        status: http::StatusCode::OK,
        headers: http::HeaderMap::new(),
    })
}

/// Convert HeaderMap to a simple map for JSON serialization
fn headers_to_map(headers: &http::HeaderMap) -> std::collections::HashMap<String, String> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value.to_str().unwrap_or("").to_string(),
            )
        })
        .collect()
}

/// `nativeDownload` of both `BackgroundDownloader` (static) and
/// `DownloadWorker` (instance).
extern "system" fn native_download(
    env: JNIEnv,
    _this_or_class: JObject,
    url: JString,
    file_path: JString,
    headers_json: JString,
    progress_callback: JObject,
) -> jint {
    perform_download_impl(env, url, file_path, headers_json, progress_callback)
}

/// Shared implementation for both BackgroundDownloader and DownloadWorker
fn perform_download_impl(
    mut env: JNIEnv,
    url: JString,
    file_path: JString,
    headers_json: JString,
    progress_callback: JObject,
) -> jint {
    tracing::debug!("NATIVE nativeDownload() called!");

    // Get JVM
    let jvm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(e) => {
            tracing::error!("Failed to get JVM: {}", e);
            return -1;
        }
    };

    // Parse URL
    let url_str: String = match env.get_string(&url) {
        Ok(s) => s.into(),
        Err(e) => {
            tracing::error!("Failed to get URL string: {}", e);
            return -1;
        }
    };

    let url = match Url::parse(&url_str) {
        Ok(u) => u,
        Err(e) => {
            tracing::error!("Failed to parse URL: {}", e);
            return -1;
        }
    };

    // Parse file path
    let file_path_str: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(e) => {
            tracing::error!("Failed to get file path string: {}", e);
            return -1;
        }
    };

    let file_path = PathBuf::from(file_path_str);

    // Parse headers
    let headers_str: String = match env.get_string(&headers_json) {
        Ok(s) => s.into(),
        Err(e) => {
            tracing::error!("Failed to get headers JSON string: {}", e);
            return -1;
        }
    };

    let headers_map: std::collections::HashMap<String, String> =
        serde_json::from_str(&headers_str).unwrap_or_default();

    let mut headers = http::HeaderMap::new();
    for (name, value) in headers_map {
        if let (Ok(n), Ok(v)) = (
            http::header::HeaderName::from_bytes(name.as_bytes()),
            http::header::HeaderValue::from_str(&value),
        ) {
            headers.insert(n, v);
        }
    }

    // Create global ref for progress callback
    let callback_global = match env.new_global_ref(&progress_callback) {
        Ok(g) => g,
        Err(e) => {
            tracing::error!("Failed to create global ref for callback: {}", e);
            return -1;
        }
    };

    tracing::debug!("Starting download with Cronet...");

    // Spawn on the global runtime and wait for completion in a separate thread
    let runtime = crate::backend::android::get_runtime();
    let handle = runtime.spawn(async move {
        download_file_with_cronet(&jvm, url, file_path, headers, callback_global).await
    });

    // Wait for the task to complete in a blocking thread
    let thread_result =
        std::thread::spawn(move || match runtime.block_on(async { handle.await }) {
            Ok(result) => result,
            Err(e) => Err(Error::Internal(format!("Task join error: {}", e))),
        })
        .join();

    // Handle thread join errors
    let result = match thread_result {
        Ok(task_result) => task_result,
        Err(e) => {
            tracing::debug!("Thread panic: {:?}", e);
            tracing::error!("Thread panic: {:?}", e);
            return -3;
        }
    };

    // Handle task result
    match result {
        Ok(bytes_downloaded) => {
            tracing::debug!(
                "Download completed successfully: {} bytes",
                bytes_downloaded
            );
            0
        }
        Err(e) => {
            tracing::debug!("Download failed: {}", e);
            tracing::error!("Download failed: {}", e);
            -1
        }
    }
}

/// Download file using Cronet and write to disk
/// Returns the number of bytes downloaded
async fn download_file_with_cronet(
    jvm: &JavaVM,
    url: Url,
    file_path: PathBuf,
    headers: http::HeaderMap,
    progress_callback: GlobalRef,
) -> Result<u64> {
    use crate::backend::android::request;
    use crate::backend::types::BackendRequest;
    use http::Method;

    let _ = jvm;
    let jvm = super::get_global_vm()?;
    let cronet_engine = super::get_global_cronet_engine()?;

    // Create request
    let request = BackendRequest {
        method: Method::GET,
        url,
        headers,
        body: None,
        progress_callback: None,
        read_timeout: None,
    };

    // Execute request
    let response = request::execute_request(jvm, &cronet_engine, request, None).await?;

    // Create file
    if let Some(parent) = file_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Internal(format!("Failed to create parent directory: {}", e)))?;
    }

    let mut file = std::fs::File::create(&file_path)
        .map_err(|e| Error::Internal(format!("Failed to create file: {}", e)))?;

    // Stream response to file
    let mut body = response.body;
    let mut bytes_downloaded = 0u64;
    let total_bytes = response
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    while let Some(chunk_result) = futures_util::StreamExt::next(&mut body).await {
        match chunk_result {
            Ok(chunk) => {
                use std::io::Write;
                file.write_all(&chunk)
                    .map_err(|e| Error::Internal(format!("Failed to write to file: {}", e)))?;

                bytes_downloaded += chunk.len() as u64;

                // Call progress callback
                if let Ok(mut env) = jvm.attach_current_thread() {
                    let _ = env.call_method(
                        progress_callback.as_obj(),
                        "onProgress",
                        "(JJ)V",
                        &[
                            (bytes_downloaded as i64).into(),
                            (total_bytes.unwrap_or(0) as i64).into(),
                        ],
                    );
                }
            }
            Err(e) => {
                tracing::error!("Error receiving chunk: {}", e);
                return Err(e);
            }
        }
    }

    tracing::debug!(
        "Download complete: {} bytes to {}",
        bytes_downloaded,
        file_path.display()
    );
    Ok(bytes_downloaded)
}

/// JNI function called by DownloadProgressCallback to invoke Rust callback
extern "system" fn native_on_progress(
    _env: JNIEnv,
    _class: JClass,
    handler_id: i64,
    bytes_downloaded: i64,
    total_bytes: i64,
) {
    // Look up the callback
    if let Some(callback) = PROGRESS_CALLBACKS.lock().unwrap().get(&handler_id) {
        let total = if total_bytes > 0 {
            Some(total_bytes as u64)
        } else {
            None
        };
        callback(bytes_downloaded as u64, total);
    }
}

/// Register native methods for BackgroundDownloader class
fn register_background_downloader_methods(env: &mut JNIEnv, class: &JClass) -> Result<()> {
    use jni::NativeMethod;
    use jni::objects::JClass as JClassType;

    // Clone JClass to pass it by value
    let jclass = unsafe { JClassType::from_raw(class.as_raw()) };

    let native_methods = [
        NativeMethod {
            name: "nativeDownload".into(),
            sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Lse/brendan/frakt/DownloadProgressCallback;)I".into(),
            fn_ptr: native_download as *mut std::ffi::c_void,
        },
    ];

    env.register_native_methods(jclass, &native_methods)
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to register BackgroundDownloader native methods: {}",
                e
            ))
        })?;

    Ok(())
}

/// Register `nativeDownload` on `DownloadWorker`. Classes from the embedded
/// DEX cannot find natives by symbol name, so they must be registered.
fn register_download_worker_methods(env: &mut JNIEnv, class: &JClass) -> Result<()> {
    let native_methods = [jni::NativeMethod {
        name: "nativeDownload".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Lse/brendan/frakt/DownloadProgressCallback;)I".into(),
        fn_ptr: native_download as *mut std::ffi::c_void,
    }];
    env.register_native_methods(class, &native_methods)
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to register DownloadWorker native methods: {}",
                e
            ))
        })
}

/// Register native methods for DownloadProgressCallback class
fn register_progress_callback_methods(env: &mut JNIEnv, class: &JClass) -> Result<()> {
    use jni::NativeMethod;
    use jni::objects::JClass as JClassType;

    let jclass = unsafe { JClassType::from_raw(class.as_raw()) };

    let native_methods = [NativeMethod {
        name: "nativeOnProgress".into(),
        sig: "(JJJ)V".into(),
        fn_ptr: native_on_progress as *mut std::ffi::c_void,
    }];

    env.register_native_methods(jclass, &native_methods)
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to register DownloadProgressCallback native methods: {}",
                e
            ))
        })?;

    Ok(())
}
