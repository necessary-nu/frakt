//! Cronet engine creation and configuration

use crate::backend::BackendConfig;
use crate::{Error, Result};
use jni::{JNIEnv, JavaVM, objects::GlobalRef};

/// Create a Cronet engine with default configuration
pub fn create_cronet_engine(jvm: &JavaVM) -> Result<GlobalRef> {
    let config = BackendConfig::default();
    create_cronet_engine_with_config(jvm, &config)
}

/// Create a Cronet engine with custom configuration
pub fn create_cronet_engine_with_config(jvm: &JavaVM, config: &BackendConfig) -> Result<GlobalRef> {
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    // Get Android application context
    let context = super::application_context()?;

    // Create CronetEngine.Builder
    let builder_class = env
        .find_class("org/chromium/net/CronetEngine$Builder")
        .map_err(|e| {
            Error::Internal(format!("Failed to find CronetEngine.Builder class: {}", e))
        })?;

    let builder = env
        .new_object(
            builder_class,
            "(Landroid/content/Context;)V",
            &[(context.as_obj()).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to create CronetEngine.Builder: {}", e)))?;

    // Configure the builder
    configure_cronet_builder(&mut env, &builder, &context, config)?;

    // Build the engine
    let engine = env
        .call_method(&builder, "build", "()Lorg/chromium/net/CronetEngine;", &[])
        .map_err(|e| {
            // Try to get more details about the Java exception
            let exception_msg = if let Ok(exception) = env.exception_occurred() {
                if !exception.is_null() {
                    // Get the exception message
                    let msg = env
                        .call_method(&exception, "toString", "()Ljava/lang/String;", &[])
                        .ok()
                        .and_then(|v| v.l().ok())
                        .and_then(|s| {
                            let jstring = s.into();
                            env.get_string(&jstring)
                                .ok()
                                .map(|s| s.to_string_lossy().to_string())
                        })
                        .unwrap_or_else(|| "Unknown Java exception".to_string());
                    let _ = env.exception_clear();
                    msg
                } else {
                    "No exception details available".to_string()
                }
            } else {
                "Could not retrieve exception".to_string()
            };
            Error::Internal(format!(
                "Failed to build CronetEngine: {} (Java exception: {})",
                e, exception_msg
            ))
        })?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert CronetEngine: {}", e)))?;

    env.new_global_ref(&engine)
        .map_err(|e| Error::Internal(format!("Failed to create global reference: {}", e)))
}

/// Configure the Cronet engine builder with our settings
/// Configuration matches the official Google Cronet sample app
fn configure_cronet_builder(
    env: &mut JNIEnv,
    builder: &jni::objects::JObject,
    context: &GlobalRef,
    config: &BackendConfig,
) -> Result<()> {
    // Configure DNS to use system resolver
    let dns_options_builder_class = env
        .find_class("org/chromium/net/DnsOptions$Builder")
        .map_err(|e| Error::Internal(format!("Failed to find DnsOptions.Builder: {}", e)))?;

    let dns_options_builder = env
        .new_object(dns_options_builder_class, "()V", &[])
        .map_err(|e| Error::Internal(format!("Failed to create DnsOptions.Builder: {}", e)))?;

    // Use system resolver (NOT built-in resolver)
    env.call_method(
        &dns_options_builder,
        "useBuiltInDnsResolver",
        "(Z)Lorg/chromium/net/DnsOptions$Builder;",
        &[false.into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to disable built-in DNS resolver: {}", e)))?;

    let dns_options = env
        .call_method(
            &dns_options_builder,
            "build",
            "()Lorg/chromium/net/DnsOptions;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to build DnsOptions: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert DnsOptions: {}", e)))?;

    env.call_method(
        builder,
        "setDnsOptions",
        "(Lorg/chromium/net/DnsOptions;)Lorg/chromium/net/CronetEngine$Builder;",
        &[(&dns_options).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to set DNS options: {}", e)))?;

    // Enable HTTP/2
    env.call_method(
        builder,
        "enableHttp2",
        "(Z)Lorg/chromium/net/CronetEngine$Builder;",
        &[true.into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to enable HTTP/2: {}", e)))?;

    // Enable QUIC
    env.call_method(
        builder,
        "enableQuic",
        "(Z)Lorg/chromium/net/CronetEngine$Builder;",
        &[true.into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to enable QUIC: {}", e)))?;

    // Enable Brotli compression
    env.call_method(
        builder,
        "enableBrotli",
        "(Z)Lorg/chromium/net/CronetEngine$Builder;",
        &[true.into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to enable Brotli: {}", e)))?;

    // Set user agent if provided
    if let Some(user_agent) = &config.user_agent {
        let user_agent_jstring = env
            .new_string(user_agent)
            .map_err(|e| Error::Internal(format!("Failed to create user agent string: {}", e)))?;

        env.call_method(
            builder,
            "setUserAgent",
            "(Ljava/lang/String;)Lorg/chromium/net/CronetEngine$Builder;",
            &[(&user_agent_jstring).into()],
        )
        .map_err(|e| Error::Internal(format!("Failed to set user agent: {}", e)))?;
    }

    // Get the files directory from the Android context (more reliable than cache dir)
    let files_dir_file = env
        .call_method(context.as_obj(), "getFilesDir", "()Ljava/io/File;", &[])
        .map_err(|e| Error::Internal(format!("Failed to get files directory: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert files directory: {}", e)))?;

    let files_dir_path = env
        .call_method(
            &files_dir_file,
            "getAbsolutePath",
            "()Ljava/lang/String;",
            &[],
        )
        .map_err(|e| Error::Internal(format!("Failed to get files directory path: {}", e)))?
        .l()
        .map_err(|e| Error::Internal(format!("Failed to convert files directory path: {}", e)))?;

    let files_dir_string: String = env
        .get_string(&files_dir_path.into())
        .map_err(|e| Error::Internal(format!("Failed to get files directory string: {}", e)))?
        .into();

    // Use a consistent storage path since we have a global singleton engine
    let cronet_storage_path = format!("{}/cronet", files_dir_string);

    // Create the cronet storage directory if it doesn't exist
    let cronet_storage_path_jstring = env.new_string(&cronet_storage_path).map_err(|e| {
        Error::Internal(format!(
            "Failed to create cronet storage path string: {}",
            e
        ))
    })?;

    let cronet_storage_file = env
        .new_object(
            "java/io/File",
            "(Ljava/lang/String;)V",
            &[(&cronet_storage_path_jstring).into()],
        )
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to create cronet storage File object: {}",
                e
            ))
        })?;

    let exists = env
        .call_method(&cronet_storage_file, "exists", "()Z", &[])
        .map_err(|e| Error::Internal(format!("Failed to check if cronet storage exists: {}", e)))?
        .z()
        .map_err(|e| Error::Internal(format!("Failed to convert exists result: {}", e)))?;

    if !exists {
        let mkdirs_result = env
            .call_method(&cronet_storage_file, "mkdirs", "()Z", &[])
            .map_err(|e| {
                Error::Internal(format!("Failed to create cronet storage directory: {}", e))
            })?
            .z()
            .map_err(|e| Error::Internal(format!("Failed to convert mkdirs result: {}", e)))?;

        if !mkdirs_result {
            return Err(Error::Internal(format!(
                "Failed to create cronet storage directory at {}",
                cronet_storage_path
            )));
        }
    }

    // Set the storage path to the cronet storage directory
    let storage_path_jstring = env
        .new_string(&cronet_storage_path)
        .map_err(|e| Error::Internal(format!("Failed to create storage path string: {}", e)))?;

    env.call_method(
        builder,
        "setStoragePath",
        "(Ljava/lang/String;)Lorg/chromium/net/CronetEngine$Builder;",
        &[(&storage_path_jstring).into()],
    )
    .map_err(|e| Error::Internal(format!("Failed to set storage path: {}", e)))?;

    env.call_method(
        builder,
        "enableHttpCache",
        "(IJ)Lorg/chromium/net/CronetEngine$Builder;",
        &[
            2i32.into(),                  // HTTP_CACHE_DISK
            (50 * 1024 * 1024i64).into(), // 50MB cache size
        ],
    )
    .map_err(|e| Error::Internal(format!("Failed to enable HTTP cache: {}", e)))?;

    Ok(())
}

/// Find the application context through `ActivityThread.currentApplication()`.
pub(crate) fn find_application_context() -> Result<GlobalRef> {
    let jvm = super::get_global_vm()?;
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| Error::Internal(format!("Failed to attach to JVM thread: {}", e)))?;

    let application = env
        .call_static_method(
            "android/app/ActivityThread",
            "currentApplication",
            "()Landroid/app/Application;",
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| Error::Internal(format!("Failed to get current Application: {}", e)))?;

    if application.is_null() {
        return Err(Error::Internal(
            "No Application context; call frakt::backend::android::init".to_string(),
        ));
    }

    env.new_global_ref(&application)
        .map_err(|e| Error::Internal(format!("Failed to create global ref for context: {}", e)))
}
