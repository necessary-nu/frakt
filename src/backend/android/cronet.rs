//! Cronet engine creation and configuration

use super::callback::java_error;
use crate::{Error, Result};
use jni::JNIEnv;
use jni::objects::{GlobalRef, JObject, JValue};

/// Create the process's Cronet engine.
///
/// The engine is shared by every client, so per-client settings such as the
/// user agent are applied to each request instead.
pub fn create_cronet_engine(env: &mut JNIEnv) -> Result<GlobalRef> {
    let context = super::application_context()?;
    let builder_class = super::load_app_class(env, "org.chromium.net.CronetEngine$Builder")?;
    let builder = env
        .new_object(
            &builder_class,
            "(Landroid/content/Context;)V",
            &[JValue::Object(context.as_obj())],
        )
        .map_err(|e| java_error(env, "CronetEngine.Builder", e))?;

    configure_cronet_builder(env, &builder, context)?;

    let engine = env
        .call_method(&builder, "build", "()Lorg/chromium/net/CronetEngine;", &[])
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "CronetEngine.Builder.build", e))?;
    env.new_global_ref(&engine)
        .map_err(|e| java_error(env, "NewGlobalRef", e))
}

fn configure_cronet_builder(
    env: &mut JNIEnv,
    builder: &JObject,
    context: &GlobalRef,
) -> Result<()> {
    let dns_builder_class = super::load_app_class(env, "org.chromium.net.DnsOptions$Builder")?;
    let dns_builder = env
        .new_object(&dns_builder_class, "()V", &[])
        .map_err(|e| java_error(env, "DnsOptions.Builder", e))?;
    // The system resolver honours the device's private DNS and VPN settings.
    env.call_method(
        &dns_builder,
        "useBuiltInDnsResolver",
        "(Z)Lorg/chromium/net/DnsOptions$Builder;",
        &[JValue::Bool(0)],
    )
    .map_err(|e| java_error(env, "DnsOptions.Builder.useBuiltInDnsResolver", e))?;
    let dns_options = env
        .call_method(
            &dns_builder,
            "build",
            "()Lorg/chromium/net/DnsOptions;",
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "DnsOptions.Builder.build", e))?;
    env.call_method(
        builder,
        "setDnsOptions",
        "(Lorg/chromium/net/DnsOptions;)Lorg/chromium/net/CronetEngine$Builder;",
        &[JValue::Object(&dns_options)],
    )
    .map_err(|e| java_error(env, "CronetEngine.Builder.setDnsOptions", e))?;

    for (method, enabled) in [("enableHttp2", true), ("enableQuic", true)] {
        env.call_method(
            builder,
            method,
            "(Z)Lorg/chromium/net/CronetEngine$Builder;",
            &[JValue::Bool(enabled as u8)],
        )
        .map_err(|e| java_error(env, method, e))?;
    }

    let files_dir = env
        .call_method(context.as_obj(), "getFilesDir", "()Ljava/io/File;", &[])
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "Context.getFilesDir", e))?;
    let storage = env
        .new_string("cronet")
        .map_err(|e| java_error(env, "NewString", e))?;
    let storage = env
        .new_object(
            "java/io/File",
            "(Ljava/io/File;Ljava/lang/String;)V",
            &[JValue::Object(&files_dir), JValue::Object(&storage)],
        )
        .map_err(|e| java_error(env, "File", e))?;
    env.call_method(&storage, "mkdirs", "()Z", &[])
        .map_err(|e| java_error(env, "File.mkdirs", e))?;
    let storage_path = env
        .call_method(&storage, "getAbsolutePath", "()Ljava/lang/String;", &[])
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "File.getAbsolutePath", e))?;
    env.call_method(
        builder,
        "setStoragePath",
        "(Ljava/lang/String;)Lorg/chromium/net/CronetEngine$Builder;",
        &[JValue::Object(&storage_path)],
    )
    .map_err(|e| java_error(env, "CronetEngine.Builder.setStoragePath", e))?;

    const HTTP_CACHE_DISK: i32 = 2;
    env.call_method(
        builder,
        "enableHttpCache",
        "(IJ)Lorg/chromium/net/CronetEngine$Builder;",
        &[JValue::Int(HTTP_CACHE_DISK), JValue::Long(50 * 1024 * 1024)],
    )
    .map_err(|e| java_error(env, "CronetEngine.Builder.enableHttpCache", e))?;

    Ok(())
}

/// Find the application context through `ActivityThread.currentApplication()`.
pub(crate) fn find_application_context() -> Result<GlobalRef> {
    let jvm = super::get_global_vm()?;
    let mut env = jvm
        .attach_current_thread_as_daemon()
        .map_err(|e| Error::Internal(format!("Failed to attach to the JVM: {}", e)))?;
    super::local_frame(&mut env, 8, |env| {
        let application = env
            .call_static_method(
                "android/app/ActivityThread",
                "currentApplication",
                "()Landroid/app/Application;",
                &[],
            )
            .and_then(|v| v.l())
            .map_err(|e| java_error(env, "ActivityThread.currentApplication", e))?;
        if application.is_null() {
            return Err(Error::Internal(
                "No Application context; call frakt::backend::android::init".to_string(),
            ));
        }
        env.new_global_ref(&application)
            .map_err(|e| java_error(env, "NewGlobalRef", e))
    })
}
