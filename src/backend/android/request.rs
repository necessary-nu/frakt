//! Running a request on Cronet and streaming its body.

use super::callback::{self, MAX_QUEUED_CHUNKS, READ_BUFFER_SIZE, RequestShared, java_error};
use super::jni_bindings::UrlRequestBuilder;
use crate::backend::types::{BackendRequest, BackendResponse, BodyAccounting};
use crate::{Error, Result};
use bytes::Bytes;
use futures_util::Stream;
use jni::objects::{GlobalRef, JValue};
use jni::sys::jlong;
use jni::{JNIEnv, JavaVM};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

/// A started Cronet request. Dropping it forgets the request's callbacks and
/// cancels the request unless Cronet has already finished it.
struct RequestGuard {
    jvm: &'static JavaVM,
    id: jlong,
    shared: Arc<RequestShared>,
}

impl RequestGuard {
    fn cancel(&self) {
        let Some(request) = self.shared.url_request.get() else {
            return;
        };
        if let Ok(mut env) = self.jvm.attach_current_thread_as_daemon() {
            let _ = super::local_frame(&mut env, 8, |env| {
                if let Err(error) = env.call_method(request.as_obj(), "cancel", "()V", &[]) {
                    java_error(env, "UrlRequest.cancel", error);
                }
                Ok(())
            });
        }
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        callback::unregister(self.id);
        let finished = self.shared.inner.lock().unwrap().finished.is_some();
        if !finished {
            self.cancel();
        }
    }
}

/// Run `request` and return once the response has started.
///
/// `read_timeout` bounds the wait for the response to start, restarting
/// whenever there is progress (an upload chunk or a redirect), and then each
/// wait for the next body chunk.
pub async fn execute_request(
    jvm: &'static JavaVM,
    cronet_engine: &GlobalRef,
    request: BackendRequest,
    read_timeout: Option<Duration>,
) -> Result<BackendResponse> {
    let original_url = request.url.clone();
    let shared = Arc::new(RequestShared::new(request.progress_callback.clone()));
    let id = callback::register(shared.clone());
    let guard = RequestGuard {
        jvm,
        id,
        shared: shared.clone(),
    };

    {
        let mut env = jvm
            .attach_current_thread_as_daemon()
            .map_err(|e| Error::Internal(format!("Failed to attach to the JVM: {}", e)))?;
        super::local_frame(&mut env, 32, |env| {
            start_request(env, cronet_engine, request, id, &shared)
        })?;
    }

    let mut idle = IdleTimer::new(read_timeout);
    let (head, redirect_headers, accounting) = std::future::poll_fn(|cx| {
        let activity = {
            let mut inner = shared.inner.lock().unwrap();
            if let Some(head) = inner.head.take() {
                let redirects = std::mem::take(&mut inner.redirect_headers);
                let accounting = inner
                    .accounting
                    .clone()
                    .unwrap_or_else(|| BodyAccounting::undecoded(&head.headers));
                return Poll::Ready(Ok((head, redirects, accounting)));
            }
            if let Some(result) = inner.finished.as_mut() {
                let error = match std::mem::replace(result, Ok(())) {
                    Ok(()) => Error::Internal("Cronet finished without a response".to_string()),
                    Err(error) => error,
                };
                return Poll::Ready(Err(error));
            }
            inner.waker = Some(cx.waker().clone());
            inner.activity
        };
        if idle.expired(activity, cx) {
            return Poll::Ready(Err(Error::Timeout));
        }
        Poll::Pending
    })
    .await?;

    Ok(BackendResponse {
        status: head.status,
        accounting,
        headers: head.headers,
        url: head.url.unwrap_or(original_url),
        body: Box::pin(CronetBody {
            guard,
            idle: IdleTimer::new(read_timeout),
            done: false,
        }),
        redirect_headers,
    })
}

/// A read timeout that only runs while someone waits and restarts whenever
/// the request shows progress.
struct IdleTimer {
    timeout: Option<Duration>,
    sleep: Option<Pin<Box<Sleep>>>,
    activity: u64,
}

impl IdleTimer {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            sleep: None,
            activity: 0,
        }
    }

    fn reset(&mut self) {
        self.sleep = None;
    }

    fn expired(&mut self, activity: u64, cx: &mut Context<'_>) -> bool {
        let Some(timeout) = self.timeout else {
            return false;
        };
        if activity != self.activity {
            self.activity = activity;
            self.sleep = None;
        }
        self.sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)))
            .as_mut()
            .poll(cx)
            .is_ready()
    }
}

/// The response body. Chunks are read only while the queue has room, so a
/// slow consumer holds back Cronet instead of filling memory.
struct CronetBody {
    guard: RequestGuard,
    idle: IdleTimer,
    done: bool,
}

impl Stream for CronetBody {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let shared = this.guard.shared.clone();

        let (chunk, outcome, read, activity) = {
            let mut inner = shared.inner.lock().unwrap();
            let chunk = inner.chunks.pop_front();
            let read = inner.finished.is_none()
                && !inner.read_outstanding
                && inner.chunks.len() < MAX_QUEUED_CHUNKS;
            inner.read_outstanding |= read;
            // Queued chunks arrived before any failure, so they go first.
            let outcome = match (&chunk, inner.finished.as_mut()) {
                (None, Some(result)) => Some(std::mem::replace(result, Ok(()))),
                _ => None,
            };
            if chunk.is_none() && outcome.is_none() {
                inner.waker = Some(cx.waker().clone());
            }
            (chunk, outcome, read, inner.activity)
        };

        if read {
            match this.guard.jvm.attach_current_thread_as_daemon() {
                Ok(mut env) => {
                    let _ = super::local_frame(&mut env, 8, |env| {
                        shared.issue_read(env);
                        Ok(())
                    });
                }
                Err(error) => shared.finish(Err(Error::Internal(format!(
                    "Failed to attach to the JVM: {}",
                    error
                )))),
            }
        }

        if let Some(chunk) = chunk {
            this.idle.reset();
            return Poll::Ready(Some(Ok(chunk)));
        }
        match outcome {
            Some(Ok(())) => {
                this.done = true;
                return Poll::Ready(None);
            }
            Some(Err(error)) => {
                this.done = true;
                return Poll::Ready(Some(Err(error)));
            }
            None => {}
        }

        if this.idle.expired(activity, cx) {
            this.done = true;
            this.guard.cancel();
            return Poll::Ready(Some(Err(Error::Timeout)));
        }
        Poll::Pending
    }
}

/// Header values are bytes; Cronet takes strings. Bytes that are not UTF-8
/// are taken as Latin-1 so the request still goes out.
fn header_value_string(value: &http::HeaderValue) -> String {
    match std::str::from_utf8(value.as_bytes()) {
        Ok(value) => value.to_owned(),
        Err(_) => value.as_bytes().iter().map(|&b| b as char).collect(),
    }
}

fn body_bytes(body: crate::body::Body) -> Result<Option<(Vec<u8>, Option<String>)>> {
    use crate::body::Body;
    Ok(match body {
        Body::Empty => None,
        Body::Bytes {
            content,
            content_type,
        } => Some((content.to_vec(), Some(content_type))),
        Body::Form { fields } => {
            let encoded = fields
                .iter()
                .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            Some((
                encoded.into_bytes(),
                Some("application/x-www-form-urlencoded".to_string()),
            ))
        }
        Body::Json { value } => Some((
            serde_json::to_vec(&value).map_err(|e| Error::Json(e.to_string()))?,
            Some("application/json".to_string()),
        )),
        Body::Multipart { .. } => {
            return Err(Error::Internal(
                "Multipart bodies are not supported by the Android backend".to_string(),
            ));
        }
    })
}

/// Build the `UrlRequest`, hand its read buffer and handle to `shared`, and
/// start it.
fn start_request(
    env: &mut JNIEnv,
    cronet_engine: &GlobalRef,
    request: BackendRequest,
    id: jlong,
    shared: &RequestShared,
) -> Result<()> {
    let callback = callback::create_callback_instance(env, id)?;
    let executor = super::callback_executor(env)?;
    let body = match request.body {
        Some(body) => body_bytes(body)?,
        None => None,
    };

    let mut builder = UrlRequestBuilder::new(
        env,
        cronet_engine.as_obj(),
        request.url.as_str(),
        &callback,
        executor.as_obj(),
    )
    .map_err(|e| Error::Internal(format!("UrlRequest.Builder: {}", e)))?;
    builder
        .set_http_method(request.method.as_str())
        .map_err(|e| Error::Internal(format!("setHttpMethod: {}", e)))?;
    for (name, value) in &request.headers {
        let value = header_value_string(value);
        if name == http::header::ACCEPT_ENCODING {
            let forced = builder
                .force_header(name.as_str(), &value)
                .map_err(|e| Error::Internal(format!("Accept-Encoding: {}", e)))?;
            if !forced {
                tracing::debug!("Cronet ignores Accept-Encoding: {}", value);
            }
            continue;
        }
        builder
            .add_header(name.as_str(), &value)
            .map_err(|e| Error::Internal(format!("addHeader({}): {}", name, e)))?;
    }

    if let Some((bytes, content_type)) = body {
        if let Some(content_type) = content_type {
            if !request.headers.contains_key(http::header::CONTENT_TYPE) {
                builder
                    .add_header("Content-Type", &content_type)
                    .map_err(|e| Error::Internal(format!("addHeader(Content-Type): {}", e)))?;
            }
        }
        let provider = builder.with_env(|env| {
            let data = env
                .byte_array_from_slice(&bytes)
                .map_err(|e| java_error(env, "NewByteArray", e))?;
            let class = callback::upload_provider_class(env)?;
            let class = jni::objects::JClass::from(
                env.new_local_ref(class.as_obj())
                    .map_err(|e| java_error(env, "NewLocalRef", e))?,
            );
            env.new_object(&class, "([BJ)V", &[JValue::Object(&data), JValue::Long(id)])
                .map_err(|e| java_error(env, "ProgressTrackingUploadDataProvider", e))
        })?;
        builder
            .set_upload_data_provider(&provider, executor.as_obj())
            .map_err(|e| Error::Internal(format!("setUploadDataProvider: {}", e)))?;
    }

    let url_request = builder
        .build()
        .map_err(|e| Error::Internal(format!("UrlRequest.Builder.build: {}", e)))?;

    let buffer = env
        .call_static_method(
            "java/nio/ByteBuffer",
            "allocateDirect",
            "(I)Ljava/nio/ByteBuffer;",
            &[JValue::Int(READ_BUFFER_SIZE)],
        )
        .and_then(|v| v.l())
        .map_err(|e| java_error(env, "ByteBuffer.allocateDirect", e))?;
    let buffer = env
        .new_global_ref(buffer)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    let _ = shared.read_buffer.set(buffer);
    let global_request = env
        .new_global_ref(&url_request)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    let _ = shared.url_request.set(global_request);

    env.call_method(&url_request, "start", "()V", &[])
        .map_err(|e| java_error(env, "UrlRequest.start", e))?;
    Ok(())
}
