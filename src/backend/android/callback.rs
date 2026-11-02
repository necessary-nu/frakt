//! Bridge from Cronet's `UrlRequest.Callback` to per-request Rust state.
//!
//! `se.brendan.frakt.RustUrlRequestCallback` forwards every callback to the
//! natives here with the request's handler id. The natives are registered with
//! `RegisterNatives` on the class loaded from our embedded DEX, so they are not
//! exported from the library.
//!
//! Reads are pull-based: Cronet only reads more of the body after
//! `UrlRequest.read()`, so a read is issued only while fewer than
//! [`MAX_QUEUED_CHUNKS`] chunks wait for the consumer. Either the callback
//! thread or the consumer's thread issues it, whichever frees room first;
//! `Inner::read_outstanding` keeps it to one at a time, as Cronet requires.

use crate::backend::types::{BodyAccounting, ProgressCallback};
use crate::{Error, Result};
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use jni::errors::LogErrorAndDefault;
use jni::objects::{Global, JByteBuffer, JClass, JObject, JString, JValue};
use jni::sys::{jint, jlong};
use jni::{Env, EnvUnowned, NativeMethod, jni_sig, jni_sig_jstr, jni_str};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::task::Waker;

/// Capacity of the direct buffer each read fills.
pub(crate) const READ_BUFFER_SIZE: jint = 64 * 1024;

/// Chunks read ahead of the consumer before reading stops.
pub(crate) const MAX_QUEUED_CHUNKS: usize = 8;

pub(crate) struct ResponseHead {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub url: Option<url::Url>,
}

#[derive(Default)]
pub(crate) struct Inner {
    pub head: Option<ResponseHead>,
    pub redirect_headers: Vec<HeaderMap>,
    pub chunks: VecDeque<Bytes>,
    pub read_outstanding: bool,
    /// Set once Cronet reports success, failure or cancellation.
    pub finished: Option<Result<()>>,
    pub waker: Option<Waker>,
    /// Bumped on every sign of progress, so idle timers can tell a quiet
    /// transfer from a slow one.
    pub activity: u64,
    /// Filled in from the response head; see `body_accounting`.
    pub accounting: Option<Arc<BodyAccounting>>,
    /// `UrlResponseInfo.getReceivedByteCount()` when the response started,
    /// if the difference to later counts is exactly the body's wire bytes.
    pub wire_baseline: Option<i64>,
}

impl Inner {
    /// Update the wire count from a later `getReceivedByteCount()`.
    fn record_received(&self, received: Option<i64>) {
        if let (Some(accounting), Some(baseline), Some(received)) =
            (&self.accounting, self.wire_baseline, received)
        {
            accounting.set_wire_bytes(received.saturating_sub(baseline).max(0) as u64);
        }
    }
}

/// State shared by one request's callbacks and its Rust-side owner.
pub(crate) struct RequestShared {
    pub inner: Mutex<Inner>,
    pub url_request: OnceLock<Global<JObject<'static>>>,
    pub read_buffer: OnceLock<Global<JObject<'static>>>,
    pub upload_progress: Option<ProgressCallback>,
}

impl RequestShared {
    pub fn new(upload_progress: Option<ProgressCallback>) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            url_request: OnceLock::new(),
            read_buffer: OnceLock::new(),
            upload_progress,
        }
    }

    /// Change the state and wake whoever waits on it.
    pub fn update<T>(&self, f: impl FnOnce(&mut Inner) -> T) -> T {
        let (result, waker) = {
            let mut inner = self.inner.lock().unwrap();
            let result = f(&mut inner);
            (result, inner.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        result
    }

    pub fn finish(&self, result: Result<()>) {
        self.update(|inner| {
            if inner.finished.is_none() {
                inner.finished = Some(result);
            }
            inner.read_outstanding = false;
        });
    }

    /// Ask Cronet for the next chunk. The caller must have set
    /// `read_outstanding` while holding the lock.
    pub fn issue_read(&self, env: &mut Env) {
        let (Some(request), Some(buffer)) = (self.url_request.get(), self.read_buffer.get()) else {
            self.finish(Err(Error::Internal(
                "Cronet request is not started".to_string(),
            )));
            return;
        };
        if let Err(error) = call_read(env, request.as_obj(), buffer.as_obj()) {
            self.finish(Err(error));
        }
    }
}

fn call_read(env: &mut Env, request: &JObject, buffer: &JObject) -> Result<()> {
    env.call_method(
        request,
        jni_str!("read"),
        jni_sig!("(Ljava/nio/ByteBuffer;)V"),
        &[JValue::Object(buffer)],
    )
    .map(|_| ())
    .map_err(|e| java_error(env, "UrlRequest.read", e))
}

/// Turn a failed JNI call into an error, clearing any pending Java exception
/// so later JNI calls on this thread do not abort.
pub(crate) fn java_error(env: &mut Env, what: &str, error: jni::errors::Error) -> Error {
    let mut message = format!("{} failed: {}", what, error);
    if env.exception_check() {
        if let Some(exception) = env.exception_occurred() {
            env.exception_clear();
            if let Some(text) = env
                .call_method(
                    &exception,
                    jni_str!("toString"),
                    jni_sig!("()Ljava/lang/String;"),
                    &[],
                )
                .and_then(|v| v.l())
                .ok()
                .and_then(|s| java_string(env, &s))
            {
                message = format!("{}: {}", what, text);
            }
        }
        env.exception_clear();
    }
    Error::Internal(message)
}

pub(crate) fn java_string(env: &mut Env, object: &JObject) -> Option<String> {
    if object.is_null() {
        return None;
    }
    env.as_cast::<JString>(object).ok()?.try_to_string(env).ok()
}

static HANDLERS: LazyLock<Mutex<HashMap<jlong, Arc<RequestShared>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT_HANDLER: AtomicI64 = AtomicI64::new(1);

pub(crate) fn register(shared: Arc<RequestShared>) -> jlong {
    let id = NEXT_HANDLER.fetch_add(1, Ordering::Relaxed);
    HANDLERS.lock().unwrap().insert(id, shared);
    id
}

pub(crate) fn unregister(id: jlong) {
    HANDLERS.lock().unwrap().remove(&id);
}

fn lookup(id: jlong) -> Option<Arc<RequestShared>> {
    HANDLERS.lock().unwrap().get(&id).cloned()
}

/// All header values of a `UrlResponseInfo`, in order, repeated names
/// appended. Values that are not valid header values are skipped rather than
/// failing the request.
fn extract_headers(env: &mut Env, info: &JObject) -> Result<HeaderMap> {
    let list = env
        .call_method(
            info,
            jni_str!("getAllHeadersAsList"),
            jni_sig!("()Ljava/util/List;"),
            &[],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "getAllHeadersAsList", e))?;
    let size = env
        .call_method(&list, jni_str!("size"), jni_sig!("()I"), &[])
        .and_then(|v| v.i())
        .map_err(|e| java_error(env, "List.size", e))?;

    let mut headers = HeaderMap::new();
    for i in 0..size {
        let entry = env
            .call_method(
                &list,
                jni_str!("get"),
                jni_sig!("(I)Ljava/lang/Object;"),
                &[JValue::Int(i)],
            )
            .and_then(|v| v.l())
            .map_err(|e| java_error(env, "List.get", e))?;
        let key = env
            .call_method(
                &entry,
                jni_str!("getKey"),
                jni_sig!("()Ljava/lang/Object;"),
                &[],
            )
            .and_then(|v| v.l())
            .map_err(|e| java_error(env, "Entry.getKey", e))?;
        let value = env
            .call_method(
                &entry,
                jni_str!("getValue"),
                jni_sig!("()Ljava/lang/Object;"),
                &[],
            )
            .and_then(|v| v.l())
            .map_err(|e| java_error(env, "Entry.getValue", e))?;
        if let (Some(key), Some(value)) = (java_string(env, &key), java_string(env, &value)) {
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(key.as_bytes()),
                http::HeaderValue::from_bytes(value.as_bytes()),
            ) {
                headers.append(name, value);
            }
        }
        env.delete_local_ref(entry);
        env.delete_local_ref(key);
        env.delete_local_ref(value);
    }
    Ok(headers)
}

/// How the body relates to the wire, from the response head.
///
/// Cronet decodes gzip and deflate itself (it asks for nothing else unless
/// told to) and keeps Content-Encoding and Content-Length as the server
/// sent them. `getReceivedByteCount()` is documented as counting bytes
/// before decompression, headers and redirects included; measured on
/// Cronet 141, at `onResponseStarted` it is exactly the bytes of every
/// response head so far, even when the server sent body bytes in the same
/// packet, so later counts minus it are the body bytes read from the
/// connection. That is the wire count for HTTP/1.x without chunked
/// framing; chunk headers and HTTP/2 or QUIC frames would be counted too,
/// so no wire count is kept for those.
fn body_accounting(
    env: &mut Env,
    info: &JObject,
    headers: &HeaderMap,
    received: Option<i64>,
) -> (Arc<BodyAccounting>, Option<i64>) {
    let decoded = headers
        .get_all(http::header::CONTENT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|coding| {
            let coding = coding.trim();
            ["gzip", "x-gzip", "deflate"]
                .iter()
                .any(|known| coding.eq_ignore_ascii_case(known))
        });
    let accounting = BodyAccounting::new(decoded, crate::backend::types::content_length(headers));

    let chunked = headers
        .get_all(http::header::TRANSFER_ENCODING)
        .iter()
        .any(|value| {
            value
                .as_bytes()
                .to_ascii_lowercase()
                .windows(7)
                .any(|w| w == b"chunked")
        });
    let protocol = env
        .call_method(
            info,
            jni_str!("getNegotiatedProtocol"),
            jni_sig!("()Ljava/lang/String;"),
            &[],
        )
        .and_then(|v| v.l())
        .ok()
        .and_then(|protocol| java_string(env, &protocol))
        .unwrap_or_default();
    env.exception_clear();
    // Plain-text HTTP/1.x reports "unknown"; TLS reports the ALPN name.
    let http1 = matches!(protocol.as_str(), "unknown" | "" | "http/1.1" | "http/1.0");
    let baseline = received.filter(|_| http1 && !chunked);
    if baseline.is_some() {
        accounting.set_wire_bytes(0);
    }
    (accounting, baseline)
}

fn received_byte_count(env: &mut Env, info: &JObject) -> Option<i64> {
    let count = env
        .call_method(info, jni_str!("getReceivedByteCount"), jni_sig!("()J"), &[])
        .and_then(|v| v.j())
        .ok();
    env.exception_clear();
    count
}

fn extract_head(env: &mut Env, info: &JObject) -> Result<ResponseHead> {
    let code = env
        .call_method(info, jni_str!("getHttpStatusCode"), jni_sig!("()I"), &[])
        .and_then(|v| v.i())
        .map_err(|e| java_error(env, "getHttpStatusCode", e))?;
    let status = StatusCode::from_u16(code as u16)
        .map_err(|_| Error::Internal(format!("Invalid HTTP status {}", code)))?;
    let headers = extract_headers(env, info)?;
    let url = env
        .call_method(
            info,
            jni_str!("getUrl"),
            jni_sig!("()Ljava/lang/String;"),
            &[],
        )
        .and_then(|v| v.l())
        .ok()
        .and_then(|url| java_string(env, &url))
        .and_then(|url| url::Url::parse(&url).ok());
    env.exception_clear();
    Ok(ResponseHead {
        status,
        headers,
        url,
    })
}

/// Map a `CronetException` to an error.
fn cronet_error(env: &mut Env, exception: &JObject) -> Error {
    let message = env
        .call_method(
            exception,
            jni_str!("getMessage"),
            jni_sig!("()Ljava/lang/String;"),
            &[],
        )
        .and_then(|v| v.l())
        .ok()
        .and_then(|m| java_string(env, &m))
        .unwrap_or_else(|| "Cronet request failed".to_string());
    env.exception_clear();

    let is_network_exception = match network_exception_class(env) {
        Ok(class) => env.is_instance_of(exception, class).unwrap_or(false),
        Err(_) => false,
    };
    if !is_network_exception {
        return Error::Network { code: -1, message };
    }

    let internal = env
        .call_method(
            exception,
            jni_str!("getCronetInternalErrorCode"),
            jni_sig!("()I"),
            &[],
        )
        .and_then(|v| v.i())
        .unwrap_or(0);
    env.exception_clear();

    classify_net_error(internal, message)
}

/// Classify a Chromium net error code (`getCronetInternalErrorCode()`).
///
/// Certificate errors are -200 to -299; the `ERR_SSL_*` errors sit among the
/// connection errors, so those are recognised by name in Cronet's message.
pub(crate) fn classify_net_error(internal: i32, message: String) -> Error {
    const ERR_ABORTED: i32 = -3;
    const ERR_TIMED_OUT: i32 = -7;
    const ERR_CONNECTION_TIMED_OUT: i32 = -118;

    if (-299..=-200).contains(&internal)
        || message.contains("ERR_SSL_")
        || message.contains("ERR_CERT_")
        || message.contains("ERR_BAD_SSL_")
    {
        return Error::Tls { message };
    }
    match internal {
        ERR_TIMED_OUT | ERR_CONNECTION_TIMED_OUT => Error::Timeout,
        ERR_ABORTED => Error::Cancelled,
        _ => Error::Network {
            code: internal as i64,
            message,
        },
    }
}

static NETWORK_EXCEPTION_CLASS: OnceLock<Global<JClass<'static>>> = OnceLock::new();

fn network_exception_class(env: &mut Env) -> Result<&'static Global<JClass<'static>>> {
    if let Some(class) = NETWORK_EXCEPTION_CLASS.get() {
        return Ok(class);
    }
    let class = super::load_app_class(env, "org.chromium.net.NetworkException")?;
    let class = env
        .new_global_ref(class)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    Ok(NETWORK_EXCEPTION_CLASS.get_or_init(|| class))
}

/// Run a native callback's body with its JNI environment. The bodies handle
/// their own errors, and a panic aborts, as from any `extern "system"`
/// function.
fn native<'local>(mut env: EnvUnowned<'local>, f: impl FnOnce(&mut Env<'local>)) {
    env.with_env_no_catch(|env| {
        f(env);
        Ok::<_, jni::errors::Error>(())
    })
    .resolve::<LogErrorAndDefault>()
}

extern "system" fn native_on_redirect_received<'local>(
    env: EnvUnowned<'local>,
    _this: JObject<'local>,
    handler_id: jlong,
    request: JObject<'local>,
    info: JObject<'local>,
) {
    native(env, |env| {
        let Some(shared) = lookup(handler_id) else {
            return;
        };
        if let Ok(headers) = extract_headers(env, &info) {
            shared.update(|inner| {
                inner.redirect_headers.push(headers);
                inner.activity += 1;
            });
        }
        if let Err(error) =
            env.call_method(&request, jni_str!("followRedirect"), jni_sig!("()V"), &[])
        {
            let error = java_error(env, "UrlRequest.followRedirect", error);
            shared.finish(Err(error));
        }
    })
}

extern "system" fn native_on_response_started<'local>(
    env: EnvUnowned<'local>,
    _this: JObject<'local>,
    handler_id: jlong,
    _request: JObject<'local>,
    info: JObject<'local>,
) {
    native(env, |env| {
        let Some(shared) = lookup(handler_id) else {
            return;
        };
        let received = received_byte_count(env, &info);
        match extract_head(env, &info) {
            Ok(head) => {
                let (accounting, baseline) = body_accounting(env, &info, &head.headers, received);
                let read = shared.update(|inner| {
                    inner.accounting = Some(accounting);
                    inner.wire_baseline = baseline;
                    inner.head = Some(head);
                    inner.activity += 1;
                    let read = inner.finished.is_none() && !inner.read_outstanding;
                    inner.read_outstanding |= read;
                    read
                });
                if read {
                    shared.issue_read(env);
                }
            }
            Err(error) => shared.finish(Err(error)),
        }
    })
}

extern "system" fn native_on_read_completed<'local>(
    env: EnvUnowned<'local>,
    _this: JObject<'local>,
    handler_id: jlong,
    _request: JObject<'local>,
    info: JObject<'local>,
    buffer: JByteBuffer<'local>,
    length: jint,
) {
    native(env, |env| {
        let Some(shared) = lookup(handler_id) else {
            return;
        };
        let received = received_byte_count(env, &info);
        let chunk = match env.get_direct_buffer_address(&buffer) {
            Ok(address) if length > 0 => {
                // SAFETY: Cronet wrote `length` bytes from the start of this
                // direct buffer, and no read into it is issued until the copy
                // is queued below.
                Bytes::copy_from_slice(unsafe {
                    std::slice::from_raw_parts(address, length as usize)
                })
            }
            Ok(_) => Bytes::new(),
            Err(error) => {
                let error = java_error(env, "GetDirectBufferAddress", error);
                shared.finish(Err(error));
                return;
            }
        };
        let read = shared.update(|inner| {
            inner.activity += 1;
            inner.record_received(received);
            if !chunk.is_empty() {
                inner.chunks.push_back(chunk);
            }
            let read = inner.finished.is_none() && inner.chunks.len() < MAX_QUEUED_CHUNKS;
            inner.read_outstanding = read;
            read
        });
        if read {
            shared.issue_read(env);
        }
    })
}

extern "system" fn native_on_succeeded<'local>(
    env: EnvUnowned<'local>,
    _this: JObject<'local>,
    handler_id: jlong,
    _request: JObject<'local>,
    info: JObject<'local>,
) {
    native(env, |env| {
        if let Some(shared) = lookup(handler_id) {
            let received = received_byte_count(env, &info);
            // The count must be final before the body can end.
            shared.update(|inner| inner.record_received(received));
            shared.finish(Ok(()));
        }
        unregister(handler_id);
    })
}

extern "system" fn native_on_failed<'local>(
    env: EnvUnowned<'local>,
    _this: JObject<'local>,
    handler_id: jlong,
    _request: JObject<'local>,
    info: JObject<'local>,
    exception: JObject<'local>,
) {
    native(env, |env| {
        if let Some(shared) = lookup(handler_id) {
            let received = received_byte_count(env, &info);
            let error = cronet_error(env, &exception);
            shared.update(|inner| inner.record_received(received));
            tracing::debug!("Cronet request {} failed: {}", handler_id, error);
            shared.finish(Err(error));
        }
        unregister(handler_id);
    })
}

extern "system" fn native_on_canceled<'local>(
    _env: EnvUnowned<'local>,
    _this: JObject<'local>,
    handler_id: jlong,
    _request: JObject<'local>,
    _info: JObject<'local>,
) {
    if let Some(shared) = lookup(handler_id) {
        shared.finish(Err(Error::Cancelled));
    }
    unregister(handler_id);
}

extern "system" fn native_on_upload_progress<'local>(
    _env: EnvUnowned<'local>,
    _class: JClass<'local>,
    handler_id: jlong,
    bytes_uploaded: jlong,
    total_bytes: jlong,
) {
    let Some(shared) = lookup(handler_id) else {
        return;
    };
    shared.update(|inner| inner.activity += 1);
    if let Some(callback) = &shared.upload_progress {
        let total = (total_bytes >= 0).then_some(total_bytes as u64);
        callback(bytes_uploaded as u64, total);
    }
}

/// Embedded DEX with `RustUrlRequestCallback` and the other helper classes,
/// built from `java/` by build.rs.
const FRAKT_DEX: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/classes.dex"));

static DEX_INIT: Mutex<()> = Mutex::new(());
static DEX_CLASS_LOADER: OnceLock<Global<JObject<'static>>> = OnceLock::new();
static CALLBACK_CLASS: OnceLock<Global<JClass<'static>>> = OnceLock::new();
static UPLOAD_PROVIDER_CLASS: OnceLock<Global<JClass<'static>>> = OnceLock::new();

/// The class loader for our embedded DEX. Its parent is the application's
/// class loader, so the DEX classes see Cronet whichever thread loads them.
fn get_dex_classloader(env: &mut Env) -> Result<&'static Global<JObject<'static>>> {
    if let Some(loader) = DEX_CLASS_LOADER.get() {
        return Ok(loader);
    }
    let _init = DEX_INIT.lock().unwrap();
    if let Some(loader) = DEX_CLASS_LOADER.get() {
        return Ok(loader);
    }

    // The class loader may read the buffer after construction, so it gets
    // memory that is never freed.
    let dex: &'static mut [u8] = Box::leak(FRAKT_DEX.to_vec().into_boxed_slice());
    let buffer = unsafe { env.new_direct_byte_buffer(dex.as_mut_ptr(), dex.len()) }
        .map_err(|e| java_error(env, "NewDirectByteBuffer", e))?;
    let parent = super::app_class_loader(env)?;
    let loader = env
        .new_object(
            jni_str!("dalvik/system/InMemoryDexClassLoader"),
            jni_sig!("(Ljava/nio/ByteBuffer;Ljava/lang/ClassLoader;)V"),
            &[JValue::Object(&buffer), JValue::Object(parent.as_obj())],
        )
        .map_err(|e| java_error(env, "InMemoryDexClassLoader", e))?;
    let loader = env
        .new_global_ref(loader)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    Ok(DEX_CLASS_LOADER.get_or_init(|| loader))
}

/// Load a class from our embedded DEX by its binary name, e.g.
/// `se.brendan.frakt.RustUrlRequestCallback`.
fn load_class_from_dex<'a>(env: &mut Env<'a>, class_name: &str) -> Result<JClass<'a>> {
    let loader = get_dex_classloader(env)?;
    super::load_class_with(env, loader, class_name)
}

fn dex_class_with_natives(
    env: &mut Env,
    cell: &'static OnceLock<Global<JClass<'static>>>,
    class_name: &str,
    natives: &[NativeMethod],
) -> Result<&'static Global<JClass<'static>>> {
    if let Some(class) = cell.get() {
        return Ok(class);
    }
    // Loading the DEX takes DEX_INIT itself, so do that before locking.
    get_dex_classloader(env)?;
    let _init = DEX_INIT.lock().unwrap();
    if let Some(class) = cell.get() {
        return Ok(class);
    }
    let class = load_class_from_dex(env, class_name)?;
    // SAFETY: each native's function takes the arguments its signature
    // declares.
    unsafe { env.register_native_methods(&class, natives) }
        .map_err(|e| java_error(env, "RegisterNatives", e))?;
    let class = env
        .new_global_ref(class)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    Ok(cell.get_or_init(|| class))
}

fn callback_class(env: &mut Env) -> Result<&'static Global<JClass<'static>>> {
    // SAFETY: each function takes the arguments of its signature, after the
    // env and `this`.
    let natives = unsafe {
        [
            NativeMethod::from_raw_parts(
                jni_str!("nativeOnRedirectReceived"),
                jni_sig_jstr!(
                    "(JLorg/chromium/net/UrlRequest;Lorg/chromium/net/UrlResponseInfo;)V"
                ),
                native_on_redirect_received as *mut std::ffi::c_void,
            ),
            NativeMethod::from_raw_parts(
                jni_str!("nativeOnResponseStarted"),
                jni_sig_jstr!(
                    "(JLorg/chromium/net/UrlRequest;Lorg/chromium/net/UrlResponseInfo;)V"
                ),
                native_on_response_started as *mut std::ffi::c_void,
            ),
            NativeMethod::from_raw_parts(
                jni_str!("nativeOnReadCompleted"),
                jni_sig_jstr!(
                    "(JLorg/chromium/net/UrlRequest;Lorg/chromium/net/UrlResponseInfo;Ljava/nio/ByteBuffer;I)V"
                ),
                native_on_read_completed as *mut std::ffi::c_void,
            ),
            NativeMethod::from_raw_parts(
                jni_str!("nativeOnSucceeded"),
                jni_sig_jstr!(
                    "(JLorg/chromium/net/UrlRequest;Lorg/chromium/net/UrlResponseInfo;)V"
                ),
                native_on_succeeded as *mut std::ffi::c_void,
            ),
            NativeMethod::from_raw_parts(
                jni_str!("nativeOnFailed"),
                jni_sig_jstr!(
                    "(JLorg/chromium/net/UrlRequest;Lorg/chromium/net/UrlResponseInfo;Lorg/chromium/net/CronetException;)V"
                ),
                native_on_failed as *mut std::ffi::c_void,
            ),
            NativeMethod::from_raw_parts(
                jni_str!("nativeOnCanceled"),
                jni_sig_jstr!(
                    "(JLorg/chromium/net/UrlRequest;Lorg/chromium/net/UrlResponseInfo;)V"
                ),
                native_on_canceled as *mut std::ffi::c_void,
            ),
        ]
    };
    dex_class_with_natives(
        env,
        &CALLBACK_CLASS,
        "se.brendan.frakt.RustUrlRequestCallback",
        &natives,
    )
}

/// The `UploadDataProvider` that reports progress for a handler id.
pub(crate) fn upload_provider_class(env: &mut Env) -> Result<&'static Global<JClass<'static>>> {
    // SAFETY: the function takes the arguments of the signature, after the
    // env and the class.
    let natives = [unsafe {
        NativeMethod::from_raw_parts(
            jni_str!("nativeOnUploadProgress"),
            jni_sig_jstr!("(JJJ)V"),
            native_on_upload_progress as *mut std::ffi::c_void,
        )
    }];
    dex_class_with_natives(
        env,
        &UPLOAD_PROVIDER_CLASS,
        "se.brendan.frakt.ProgressTrackingUploadDataProvider",
        &natives,
    )
}

/// A `RustUrlRequestCallback` that forwards to the handler `handler_id`.
pub(crate) fn create_callback_instance<'a>(
    env: &mut Env<'a>,
    handler_id: jlong,
) -> Result<JObject<'a>> {
    let class = callback_class(env)?;
    env.new_object(class, jni_sig!("(J)V"), &[JValue::Long(handler_id)])
        .map_err(|e| java_error(env, "RustUrlRequestCallback.<init>", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_and_ssl_errors_are_tls() {
        let tls = |code, message: &str| {
            matches!(
                classify_net_error(code, message.to_string()),
                Error::Tls { .. }
            )
        };
        assert!(tls(-202, "net::ERR_CERT_AUTHORITY_INVALID"));
        assert!(tls(-107, "net::ERR_SSL_PROTOCOL_ERROR"));
        assert!(!tls(-105, "net::ERR_NAME_NOT_RESOLVED"));
        assert!(matches!(
            classify_net_error(-118, String::new()),
            Error::Timeout
        ));
        assert!(matches!(
            classify_net_error(-102, String::new()),
            Error::Network { code: -102, .. }
        ));
    }
}
