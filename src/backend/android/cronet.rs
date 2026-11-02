//! Cronet engine creation and configuration

use super::callback::java_error;
use crate::{Error, Result};
use jni::objects::{Global, JObject, JValue};
use jni::{Env, jni_sig, jni_str};

/// Create the process's Cronet engine.
///
/// The engine is shared by every client, so per-client settings such as the
/// user agent are applied to each request instead.
pub fn create_cronet_engine(env: &mut Env) -> Result<Global<JObject<'static>>> {
    let context = super::application_context()?;
    let builder_class = super::load_app_class(env, "org.chromium.net.CronetEngine$Builder")?;
    let builder = env
        .new_object(
            &builder_class,
            jni_sig!("(Landroid/content/Context;)V"),
            &[JValue::Object(context.as_obj())],
        )
        .map_err(|e| java_error(env, "CronetEngine.Builder", e))?;

    configure_cronet_builder(env, &builder, context)?;

    let engine = env
        .call_method(
            &builder,
            jni_str!("build"),
            jni_sig!("()Lorg/chromium/net/CronetEngine;"),
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "CronetEngine.Builder.build", e))?;
    env.new_global_ref(&engine)
        .map_err(|e| java_error(env, "NewGlobalRef", e))
}

fn configure_cronet_builder(
    env: &mut Env,
    builder: &JObject,
    context: &Global<JObject<'static>>,
) -> Result<()> {
    let dns_builder_class = super::load_app_class(env, "org.chromium.net.DnsOptions$Builder")?;
    let dns_builder = env
        .new_object(&dns_builder_class, jni_sig!("()V"), &[])
        .map_err(|e| java_error(env, "DnsOptions.Builder", e))?;
    // The system resolver honours the device's private DNS and VPN settings.
    env.call_method(
        &dns_builder,
        jni_str!("useBuiltInDnsResolver"),
        jni_sig!("(Z)Lorg/chromium/net/DnsOptions$Builder;"),
        &[JValue::Bool(false)],
    )
    .map_err(|e| java_error(env, "DnsOptions.Builder.useBuiltInDnsResolver", e))?;
    let dns_options = env
        .call_method(
            &dns_builder,
            jni_str!("build"),
            jni_sig!("()Lorg/chromium/net/DnsOptions;"),
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "DnsOptions.Builder.build", e))?;
    env.call_method(
        builder,
        jni_str!("setDnsOptions"),
        jni_sig!("(Lorg/chromium/net/DnsOptions;)Lorg/chromium/net/CronetEngine$Builder;"),
        &[JValue::Object(&dns_options)],
    )
    .map_err(|e| java_error(env, "CronetEngine.Builder.setDnsOptions", e))?;

    for (method, enabled) in [
        (jni_str!("enableHttp2"), true),
        (jni_str!("enableQuic"), true),
    ] {
        env.call_method(
            builder,
            method,
            jni_sig!("(Z)Lorg/chromium/net/CronetEngine$Builder;"),
            &[JValue::Bool(enabled)],
        )
        .map_err(|e| java_error(env, &method.to_str(), e))?;
    }

    let files_dir = env
        .call_method(
            context.as_obj(),
            jni_str!("getFilesDir"),
            jni_sig!("()Ljava/io/File;"),
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "Context.getFilesDir", e))?;
    let storage = env
        .new_string("cronet")
        .map_err(|e| java_error(env, "NewString", e))?;
    let storage = env
        .new_object(
            jni_str!("java/io/File"),
            jni_sig!("(Ljava/io/File;Ljava/lang/String;)V"),
            &[JValue::Object(&files_dir), JValue::Object(&storage)],
        )
        .map_err(|e| java_error(env, "File", e))?;
    env.call_method(&storage, jni_str!("mkdirs"), jni_sig!("()Z"), &[])
        .map_err(|e| java_error(env, "File.mkdirs", e))?;
    let storage_path = env
        .call_method(
            &storage,
            jni_str!("getAbsolutePath"),
            jni_sig!("()Ljava/lang/String;"),
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "File.getAbsolutePath", e))?;
    env.call_method(
        builder,
        jni_str!("setStoragePath"),
        jni_sig!("(Ljava/lang/String;)Lorg/chromium/net/CronetEngine$Builder;"),
        &[JValue::Object(&storage_path)],
    )
    .map_err(|e| java_error(env, "CronetEngine.Builder.setStoragePath", e))?;

    // No HTTP cache: callers such as Pahkat stream large bodies to their own
    // files, so a disk cache would only write every package twice, and
    // byte-range resumption should see the server's answer, not a cached one.
    const HTTP_CACHE_DISABLED: i32 = 0;
    env.call_method(
        builder,
        jni_str!("enableHttpCache"),
        jni_sig!("(IJ)Lorg/chromium/net/CronetEngine$Builder;"),
        &[JValue::Int(HTTP_CACHE_DISABLED), JValue::Long(0)],
    )
    .map_err(|e| java_error(env, "CronetEngine.Builder.enableHttpCache", e))?;

    Ok(())
}

/// Find the application context through `ActivityThread.currentApplication()`.
pub(crate) fn find_application_context() -> Result<Global<JObject<'static>>> {
    super::attached(super::get_global_vm()?, 8, |env| {
        let application = env
            .call_static_method(
                jni_str!("android/app/ActivityThread"),
                jni_str!("currentApplication"),
                jni_sig!("()Landroid/app/Application;"),
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
