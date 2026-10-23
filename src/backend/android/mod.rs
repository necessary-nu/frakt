//! Android backend using Cronet (Chromium Network Stack)

mod callback;
mod cronet;
mod download;
mod jni_bindings;
mod request;
mod response;

#[cfg(test)]
mod tests;

use crate::backend::BackendConfig;
use crate::backend::types::{BackendRequest, BackendResponse};
use crate::{Error, Result};
use jni::{JavaVM, objects::GlobalRef};
use once_cell::sync::Lazy;
use std::sync::Arc;
use url::Url;

// Global JavaVM instance for Android - lives forever
static ANDROID_JVM: Lazy<&'static JavaVM> = Lazy::new(|| {
    Box::leak(Box::new(unsafe {
        jni::JavaVM::from_raw(vampire::java_vm() as *mut _).unwrap()
    }))
});

// Global Cronet engine instance - created once and shared across all requests
static CRONET_ENGINE: Lazy<Arc<GlobalRef>> = Lazy::new(|| {
    let jvm = *ANDROID_JVM;
    let engine = cronet::create_cronet_engine(jvm).expect("Failed to create global Cronet engine");
    Arc::new(engine)
});

// Global Tokio runtime instance - created once for all async operations
static TOKIO_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create global tokio runtime")
});

/// Get the global JavaVM instance
pub(crate) fn get_global_vm() -> Result<&'static JavaVM> {
    Ok(*ANDROID_JVM)
}

/// Get the global Cronet engine instance
fn get_global_cronet_engine() -> Arc<GlobalRef> {
    CRONET_ENGINE.clone()
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

    let engine = get_global_cronet_engine();

    // Get the application context to access files directory
    let context = cronet::get_application_context()?;

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
    println!("🔍 NetLog started: {}", path_string);

    Ok(path_string)
}

/// Stop NetLog
pub fn stop_netlog() -> Result<()> {
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let engine = get_global_cronet_engine();

    env.call_method(engine.as_obj(), "stopNetLog", "()V", &[])
        .map_err(|e| Error::Internal(format!("Failed to stop NetLog: {}", e)))?;

    tracing::info!("NetLog stopped");
    println!("🔍 NetLog stopped");

    Ok(())
}

/// Check if a specific permission is granted
pub fn check_permission(permission: &str) -> Result<bool> {
    let jvm = get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let context = cronet::get_application_context()?;

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

    let context = cronet::get_application_context()?;

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
            println!("📋 Permission: {}", permission_str);
            permissions.push(permission_str);
        }
    }

    println!("📋 Total permissions: {}", permissions.len());
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
        let jvm = get_global_vm()?;
        let cronet_engine = get_global_cronet_engine();

        Ok(Self {
            jvm,
            cronet_engine,
            cookie_storage: None,
            config: BackendConfig::default(),
        })
    }

    /// Create a new Android backend with configuration
    pub fn with_config(config: BackendConfig) -> Result<Self> {
        let jvm = get_global_vm()?;
        let cronet_engine = get_global_cronet_engine();

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

        // Apply timeout from config if not already set in request
        if request.timeout.is_none() {
            request.timeout = self.config.timeout;
        }

        // Apply default headers from config (don't override existing headers)
        if let Some(ref default_headers) = self.config.default_headers {
            for (name, value) in default_headers {
                request.headers.entry(name).or_insert(value.clone());
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

        let response = request::execute_request(self.jvm, &self.cronet_engine, request).await?;

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

    /// Get the cookie jar if configured (not implemented for Android)
    pub fn cookie_jar(&self) -> Option<&crate::CookieJar> {
        None
    }
}
