//! Android backend using Cronet (Chromium Network Stack)

pub(crate) mod background;
mod callback;
mod cronet;
mod download;
mod jni_bindings;
mod request;

use crate::backend::BackendConfig;
use crate::backend::types::{BackendRequest, BackendResponse};
use crate::{Error, Result};
use callback::java_error;
use jni::{
    JNIEnv, JavaVM,
    objects::{GlobalRef, JClass, JObject, JValue},
};
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex, OnceLock};
use url::Url;

static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();
static APP_CONTEXT: OnceLock<GlobalRef> = OnceLock::new();
static APP_CLASS_LOADER: OnceLock<GlobalRef> = OnceLock::new();
static CALLBACK_EXECUTOR: OnceLock<GlobalRef> = OnceLock::new();
static CRONET_ENGINE: Mutex<Option<Arc<GlobalRef>>> = Mutex::new(None);

// Global Tokio runtime instance - created once for all async operations
static TOKIO_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create global tokio runtime")
});

/// Give the Android backend the JavaVM and an Android `Context`.
///
/// A host that loads frakt from its own JNI code should call this once, for
/// example from its `JNI_OnLoad` or first JNI entry point, before creating a
/// client. The application context is taken from `context`. Later calls are
/// ignored.
///
/// Without it, the backend looks the VM up with `JNI_GetCreatedJavaVMs`
/// (Android 12 and later) and the context up with
/// `ActivityThread.currentApplication()`, a non-SDK API.
pub fn init(env: &mut JNIEnv, context: &JObject) -> Result<()> {
    let vm = env
        .get_java_vm()
        .map_err(|e| Error::Internal(format!("Failed to get JavaVM: {}", e)))?;
    let _ = JAVA_VM.set(vm);

    if !context.is_null() && APP_CONTEXT.get().is_none() {
        let app = env
            .call_method(
                context,
                "getApplicationContext",
                "()Landroid/content/Context;",
                &[],
            )
            .and_then(|v| v.l())
            .map_err(|e| Error::Internal(format!("Failed to get application context: {}", e)))?;
        let app = if app.is_null() {
            env.new_global_ref(context)
        } else {
            env.new_global_ref(&app)
        }
        .map_err(|e| Error::Internal(format!("Failed to retain context: {}", e)))?;
        let _ = APP_CONTEXT.set(app);
    }

    Ok(())
}

/// Get the global JavaVM instance
pub(crate) fn get_global_vm() -> Result<&'static JavaVM> {
    if let Some(vm) = JAVA_VM.get() {
        return Ok(vm);
    }
    let vm = discover_java_vm().ok_or_else(|| {
        Error::Internal(
            "No JavaVM found; call frakt::backend::android::init before using the Android backend"
                .to_string(),
        )
    })?;
    Ok(JAVA_VM.get_or_init(|| vm))
}

/// Find the process's JavaVM without the host's help.
///
/// `JNI_GetCreatedJavaVMs` is looked up at run time instead of linked: the NDK
/// only exports it to apps from API 31, through libnativehelper, and linker
/// namespaces hide libart's copy from `RTLD_DEFAULT` lookups.
fn discover_java_vm() -> Option<JavaVM> {
    type GetCreatedJavaVms = unsafe extern "system" fn(
        *mut *mut jni::sys::JavaVM,
        jni::sys::jsize,
        *mut jni::sys::jsize,
    ) -> jni::sys::jint;

    let name = c"JNI_GetCreatedJavaVMs";
    let mut symbol = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr()) };
    if symbol.is_null() {
        // Already mapped in every app process; dlopen only takes a reference.
        let library = unsafe { libc::dlopen(c"libnativehelper.so".as_ptr(), libc::RTLD_NOW) };
        if !library.is_null() {
            symbol = unsafe { libc::dlsym(library, name.as_ptr()) };
        }
    }
    if symbol.is_null() {
        return None;
    }
    let get_created: GetCreatedJavaVms = unsafe { std::mem::transmute(symbol) };
    let mut vm: *mut jni::sys::JavaVM = std::ptr::null_mut();
    let mut count: jni::sys::jsize = 0;
    let status = unsafe { get_created(&mut vm, 1, &mut count) };
    if status != jni::sys::JNI_OK || count < 1 || vm.is_null() {
        return None;
    }
    unsafe { JavaVM::from_raw(vm) }.ok()
}

/// The application context given to [`init`], or the one found through
/// `ActivityThread`.
pub(crate) fn application_context() -> Result<&'static GlobalRef> {
    if let Some(context) = APP_CONTEXT.get() {
        return Ok(context);
    }
    let context = cronet::find_application_context()?;
    Ok(APP_CONTEXT.get_or_init(|| context))
}

/// Run `f` in its own JNI local reference frame.
///
/// Threads attached from native code never return to Java, so local
/// references they create are only freed by popping a frame.
pub(crate) fn local_frame<T>(
    env: &mut JNIEnv,
    capacity: i32,
    f: impl FnOnce(&mut JNIEnv) -> Result<T>,
) -> Result<T> {
    env.push_local_frame(capacity)
        .map_err(|e| java_error(env, "PushLocalFrame", e))?;
    let result = f(env);
    // SAFETY: nothing created in the frame outlives it; results are global
    // references or Rust values.
    let _ = unsafe { env.pop_local_frame(&JObject::null()) };
    result
}

/// The application's class loader.
///
/// JNI `FindClass` on a thread attached from native code only sees system
/// classes, so app classes such as Cronet's are loaded through this instead.
pub(crate) fn app_class_loader(env: &mut JNIEnv) -> Result<&'static GlobalRef> {
    if let Some(loader) = APP_CLASS_LOADER.get() {
        return Ok(loader);
    }
    let context = application_context()?;
    let loader = env
        .call_method(
            context.as_obj(),
            "getClassLoader",
            "()Ljava/lang/ClassLoader;",
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "Context.getClassLoader", e))?;
    let loader = env
        .new_global_ref(loader)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    Ok(APP_CLASS_LOADER.get_or_init(|| loader))
}

/// Load a class by binary name (`org.chromium.net.CronetEngine$Builder`)
/// through `loader`.
pub(crate) fn load_class_with<'a>(
    env: &mut JNIEnv<'a>,
    loader: &GlobalRef,
    class_name: &str,
) -> Result<JClass<'a>> {
    let name = env
        .new_string(class_name)
        .map_err(|e| java_error(env, "NewString", e))?;
    let class = env
        .call_method(
            loader.as_obj(),
            "loadClass",
            "(Ljava/lang/String;)Ljava/lang/Class;",
            &[JValue::Object(&name)],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, &format!("loadClass({})", class_name), e))?;
    Ok(JClass::from(class))
}

/// Load an application class, such as Cronet's, from any thread.
pub(crate) fn load_app_class<'a>(env: &mut JNIEnv<'a>, class_name: &str) -> Result<JClass<'a>> {
    let loader = app_class_loader(env)?;
    load_class_with(env, loader, class_name)
}

/// The executor every request's callbacks and uploads run on. Callbacks only
/// copy data and hand off to Rust, so one shared pool serves all requests.
pub(crate) fn callback_executor(env: &mut JNIEnv) -> Result<&'static GlobalRef> {
    if let Some(executor) = CALLBACK_EXECUTOR.get() {
        return Ok(executor);
    }
    let executor = env
        .call_static_method(
            "java/util/concurrent/Executors",
            "newCachedThreadPool",
            "()Ljava/util/concurrent/ExecutorService;",
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "Executors.newCachedThreadPool", e))?;
    let executor = env
        .new_global_ref(executor)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    Ok(CALLBACK_EXECUTOR.get_or_init(|| executor))
}

/// The process's Cronet engine, created on first use.
fn get_global_cronet_engine() -> Result<Arc<GlobalRef>> {
    let mut engine = CRONET_ENGINE.lock().unwrap();
    if let Some(engine) = engine.as_ref() {
        return Ok(engine.clone());
    }
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread_as_daemon()
        .map_err(|e| Error::Internal(format!("Failed to attach to the JVM: {}", e)))?;
    let created = Arc::new(local_frame(&mut env, 32, cronet::create_cronet_engine)?);
    *engine = Some(created.clone());
    Ok(created)
}

/// Get the global tokio runtime instance
pub(crate) fn get_runtime() -> &'static tokio::runtime::Runtime {
    &TOKIO_RUNTIME
}

/// Start NetLog for debugging network issues
/// Returns the path to the NetLog file
pub fn start_netlog() -> Result<String> {
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let engine = get_global_cronet_engine()?;

    // Get the application context to access files directory
    let context = application_context()?;

    // Get external files directory
    let files_dir = env
        .call_method(
            context.as_obj(),
            "getExternalFilesDir",
            "(Ljava/lang/String;)Ljava/io/File;",
            &[jni::objects::JValue::Object(&jni::objects::JObject::null())],
        )
        .map_err(|e| Error::Internal(format!("Failed to get external files dir: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert files dir: {}", e)))?;

    // Create temp file for NetLog
    let file_class = env
        .find_class("java/io/File")
        .map_err(|e| Error::Internal(format!("Failed to find File class: {}", e)))?;

    let prefix = env
        .new_string("cronet")
        .map_err(|e| Error::Internal(format!("Failed to create prefix string: {}", e)))?;
    let suffix = env
        .new_string(".json")
        .map_err(|e| Error::Internal(format!("Failed to create suffix string: {}", e)))?;

    let temp_file = env
        .call_static_method(
            file_class,
            "createTempFile",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/io/File;)Ljava/io/File;",
            &[(&prefix).into(), (&suffix).into(), (&files_dir).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to create temp file: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert temp file: {}", e)))?;

    // Get absolute path
    let path = env
        .call_method(&temp_file, "getAbsolutePath", "()Ljava/lang/String;", &[])
        .map_err(|e| Error::Internal(format!("Failed to get absolute path: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert path: {}", e)))?;

    let path_string: String = env
        .get_string(&path.into())
        .map_err(|e| Error::Internal(format!("Failed to get path string: {}", e)))?
        .into();

    // Start NetLog
    let path_jstring = env
        .new_string(&path_string)
        .map_err(|e| Error::Internal(format!("Failed to create path jstring: {}", e)))?;

    env.call_method(
        engine.as_obj(),
        "startNetLogToFile",
        "(Ljava/lang/String;Z)V",
        &[(&path_jstring).into(), false.into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to start NetLog: {}", e)))?;

    tracing::info!("NetLog started, writing to: {}", path_string);

    Ok(path_string)
}

/// Stop NetLog
pub fn stop_netlog() -> Result<()> {
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let engine = get_global_cronet_engine()?;

    env.call_method(engine.as_obj(), "stopNetLog", "()V", &[])
        .map_err(|e| Error::Internal(format!("Failed to stop NetLog: {}", e)))?;

    tracing::info!("NetLog stopped");

    Ok(())
}

/// Check if a specific permission is granted
pub fn check_permission(permission: &str) -> Result<bool> {
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let context = application_context()?;

    let permission_jstring = env
        .new_string(permission)
        .map_err(|e| Error::Internal(format!("Failed to create permission string: {}", e)))?;

    let result = env
        .call_method(
            context.as_obj(),
            "checkSelfPermission",
            "(Ljava/lang/String;)I",
            &[(&permission_jstring).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to check permission: {}", e)))?
        .i()
        .map_err(|e| Error::Internal(format!("Failed to get permission result: {}", e)))?;

    // PERMISSION_GRANTED = 0
    Ok(result == 0)
}

/// List all granted permissions
pub fn list_permissions() -> Result<Vec<String>> {
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let context = application_context()?;

    // Get PackageManager
    let package_manager = env
        .call_method(
            context.as_obj(),
            "getPackageManager",
            "()Landroid/content/pm/PackageManager;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to get PackageManager: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert PackageManager: {}", e)))?;

    // Get package name
    let package_name = env
        .call_method(
            context.as_obj(),
            "getPackageName",
            "()Ljava/lang/String;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to get package name: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert package name: {}", e)))?;

    // Get PackageInfo with PERMISSIONS flag
    let package_info = env
        .call_method(
            &package_manager,
            "getPackageInfo",
            "(Ljava/lang/String;I)Landroid/content/pm/PackageInfo;",
            &[(&package_name).into(), 4096i32.into()], // GET_PERMISSIONS = 4096
        )
        .map_err(|e| Error::Internal(format!("Failed to get PackageInfo: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert PackageInfo: {}", e)))?;

    // Get requestedPermissions array using reflection
    let permissions_array = env.call_method(
        &package_info,
        "requestedPermissions",
        "[Ljava/lang/String;",
        &[],
    );

    // If field doesn't exist, try getting it as a field
    let permissions_obj = if permissions_array.is_err() {
        let package_info_class = env
            .get_object_class(&package_info)
            .map_err(|e| Error::Internal(format!("Failed to get PackageInfo class: {}", e)))?;

        let field_id = env
            .get_field_id(
                &package_info_class,
                "requestedPermissions",
                "[Ljava/lang/String;",
            )
            .map_err(|e| {
                Error::Internal(format!("Failed to get requestedPermissions field: {}", e))
            })?;

        env.get_field_unchecked(&package_info, field_id, jni::signature::ReturnType::Object)
            .map_err(|e| Error::Internal(format!("Failed to read requestedPermissions: {}", e)))?
            .l()
            .map_err(|e| Error::Internal(format!("Failed to convert permissions array: {}", e)))?
    } else {
        permissions_array
            .map_err(|e| Error::Internal(format!("Failed to get requestedPermissions: {}", e)))?
            .l()
            .map_err(|e| Error::Internal(format!("Failed to convert permissions array: {}", e)))?
    };

    if permissions_obj.is_null() {
        return Ok(Vec::new());
    }

    let permissions_jarray = jni::objects::JObjectArray::from(permissions_obj);
    let array_len = env
        .get_array_length(&permissions_jarray)
        .map_err(|e| Error::Internal(format!("Failed to get array length: {}", e)))?;

    let mut permissions = Vec::new();
    for i in 0..array_len {
        let permission_obj = env
            .get_object_array_element(&permissions_jarray, i)
            .map_err(|e| {
                Error::Internal(format!("Failed to get permission at index {}: {}", i, e))
            })?;

        if !permission_obj.is_null() {
            let permission_str: String = env
                .get_string(&permission_obj.into())
                .map_err(|e| {
                    Error::Internal(format!("Failed to convert permission string: {}", e))
                })?
                .into();
            permissions.push(permission_str);
        }
    }

    Ok(permissions)
}

/// Test DNS resolution directly using Java's InetAddress API
pub fn test_dns(hostname: &str) -> Result<String> {
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    // Temporarily disable StrictMode network checks
    let strict_mode_class = env
        .find_class("android/os/StrictMode")
        .map_err(|e| Error::Internal(format!("Failed to find StrictMode class: {}", e)))?;

    let old_policy = env
        .call_static_method(
            &strict_mode_class,
            "getThreadPolicy",
            "()Landroid/os/StrictMode$ThreadPolicy;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to get thread policy: {}", e)))?;

    let policy_builder_class = env
        .find_class("android/os/StrictMode$ThreadPolicy$Builder")
        .map_err(|e| Error::Internal(format!("Failed to find ThreadPolicy.Builder: {}", e)))?;

    let policy_builder = env
        .new_object(policy_builder_class, "()V", &[])
        .map_err(|e| Error::Internal(format!("Failed to create policy builder: {}", e)))?;

    let permissive_policy_builder = env
        .call_method(
            &policy_builder,
            "permitNetwork",
            "()Landroid/os/StrictMode$ThreadPolicy$Builder;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to permit network: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get builder: {}", e)))?;

    let permissive_policy = env
        .call_method(
            &permissive_policy_builder,
            "build",
            "()Landroid/os/StrictMode$ThreadPolicy;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to build policy: {}", e)))?;

    let permissive_policy_obj = permissive_policy
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get policy object: {}", e)))?;

    env.call_static_method(
        &strict_mode_class,
        "setThreadPolicy",
        "(Landroid/os/StrictMode$ThreadPolicy;)V",
        &[(&permissive_policy_obj).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to set thread policy: {}", e)))?;

    // Call Inet4Address.getByName(hostname) to force IPv4
    let hostname_jstring = env
        .new_string(hostname)
        .map_err(|e| Error::Internal(format!("Failed to create hostname string: {}", e)))?;

    let inet4_address_class = env
        .find_class("java/net/Inet4Address")
        .map_err(|e| Error::Internal(format!("Failed to find Inet4Address class: {}", e)))?;

    let inet_address_result = env.call_static_method(
        &inet4_address_class,
        "getByName",
        "(Ljava/lang/String;)Ljava/net/InetAddress;",
        &[(&hostname_jstring).into()],
    );

    // Restore old policy
    let old_policy_obj = old_policy
        .l()
        .map_err(|e| Error::Internal(format!("Failed to get old policy object: {}", e)))?;

    env.call_static_method(
        &strict_mode_class,
        "setThreadPolicy",
        "(Landroid/os/StrictMode$ThreadPolicy;)V",
        &[(&old_policy_obj).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to restore thread policy: {}", e)))?;

    // Check DNS result
    let inet_address = inet_address_result
        .map_err(|e| Error::Internal(format!("Failed to resolve hostname '{}': {}", hostname, e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert InetAddress: {}", e)))?;

    // Get the IP address string
    let host_address = env
        .call_method(&inet_address, "getHostAddress", "()Ljava/lang/String;", &[])
        .map_err(|e| Error::Internal(format!("Failed to get host address: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert host address: {}", e)))?;

    let ip_string: String = env
        .get_string(&host_address.into())
        .map_err(|e| Error::Internal(format!("Failed to get IP string: {}", e)))?
        .into();

    Ok(ip_string)
}

/// Android backend using Cronet for HTTP requests
#[derive(Clone)]
pub struct AndroidBackend {
    pub(crate) jvm: &'static JavaVM,
    cronet_engine: Arc<GlobalRef>,
    cookie_storage: Option<Arc<crate::backend::CookieJar>>,
    config: BackendConfig,
}

impl AndroidBackend {
    /// Create a new Android backend with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(BackendConfig::default())
    }

    /// Create a new Android backend with configuration
    pub fn with_config(config: BackendConfig) -> Result<Self> {
        let jvm = get_global_vm()?;
        let cronet_engine = get_global_cronet_engine()?;

        // Create cookie storage if cookies are enabled
        let cookie_storage = if config.use_cookies.unwrap_or(false) {
            Some(Arc::new(crate::backend::CookieJar::new()))
        } else {
            None
        };

        Ok(Self {
            jvm,
            cronet_engine,
            cookie_storage,
            config,
        })
    }

    /// Execute an HTTP request using Cronet
    pub async fn execute(&self, mut request: BackendRequest) -> Result<BackendResponse> {
        // Validate URL scheme
        match request.url.scheme() {
            "http" | "https" => {}
            _ => {
                return Err(Error::InvalidUrl);
            }
        }

        let read_timeout = request.read_timeout.or(self.config.read_timeout);

        // Apply default headers from config (don't override existing headers)
        if let Some(ref default_headers) = self.config.default_headers {
            for (name, value) in default_headers {
                request.headers.entry(name).or_insert(value.clone());
            }
        }

        // The engine is shared between clients, so the user agent goes on
        // each request.
        if let Some(user_agent) = &self.config.user_agent {
            if let Ok(value) = http::HeaderValue::from_str(user_agent) {
                request
                    .headers
                    .entry(http::header::USER_AGENT)
                    .or_insert(value);
            }
        }

        // Apply cookies from storage if available
        if let Some(ref cookie_storage) = self.cookie_storage {
            if let Ok(cookie_headers) = cookie_storage.get_cookies_for_url(&request.url) {
                for (name, value) in &cookie_headers {
                    request.headers.entry(name).or_insert(value.clone());
                }
            }
        }

        let response =
            request::execute_request(self.jvm, &self.cronet_engine, request, read_timeout).await?;

        // Process Set-Cookie headers from redirect responses first
        if let Some(ref cookie_storage) = self.cookie_storage {
            for redirect_header_map in &response.redirect_headers {
                let _ = cookie_storage.process_response_headers(&response.url, redirect_header_map);
            }
            // Then process final response headers
            let _ = cookie_storage.process_response_headers(&response.url, &response.headers);
        }

        Ok(response)
    }

    /// Execute a background download using DownloadManager
    pub async fn execute_background_download(
        &self,
        url: Url,
        file_path: std::path::PathBuf,
        session_identifier: Option<String>,
        headers: http::HeaderMap,
        progress_callback: Option<Box<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>>,
        error_for_status: bool,
    ) -> Result<crate::client::download::DownloadResponse> {
        download::execute_background_download(
            self.jvm,
            url,
            file_path,
            session_identifier,
            headers,
            error_for_status,
            progress_callback,
        )
        .await
    }

    pub(crate) fn default_headers(&self) -> Option<&http::HeaderMap> {
        self.config.default_headers.as_ref()
    }

    pub(crate) fn total_timeout(&self) -> Option<std::time::Duration> {
        self.config.timeout
    }

    /// Get the cookie jar if configured (not implemented for Android)
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        None
    }
}
