//! Running a request on Cronet, following its redirects, and streaming its
//! body.
//!
//! Redirects are followed here, one `UrlRequest` per hop, and never by
//! Cronet: `followRedirect()` would send the headers the request was built
//! with, the jar's cookies for the first URL among them, to wherever the
//! redirect leads. Doing it here also puts each redirect's `Set-Cookie` in
//! the jar before the next hop's cookies are taken from it.

use super::callback::{
    self, MAX_QUEUED_CHUNKS, RequestShared, ResponseHead, UploadSource, java_error,
};
use super::jni_bindings::UrlRequestBuilder;
use crate::backend::redirect;
use crate::backend::types::{
    BackendRequest, BackendResponse, BodyAccounting, BodyIntegrity, ProgressCallback,
};
use crate::{Error, Result};
use bytes::Bytes;
use futures_util::Stream;
use http::{HeaderMap, Method};
use jni::objects::{Global, JObject};
use jni::sys::jlong;
use jni::{Env, JavaVM, jni_sig, jni_str};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;
use url::Url;

/// The Cronet engine, borrowed from the backend.
///
/// The futures here must be `Send`, and one that keeps a bare
/// `&Global<JObject<'static>>` across an await is not: the compiler forgets
/// the `'static` of a type it finds held in a future, and jni makes only
/// `JObject<'static>` `Send`. Inside a named type the lifetime is kept.
#[derive(Clone, Copy)]
pub struct Engine<'a>(pub &'a Global<JObject<'static>>);

/// Fails to compile if the request future is not `Send`. Callers spawn
/// requests onto multi-threaded runtimes, and nothing else in a build of the
/// library for Android asks for it.
#[allow(dead_code)]
const _: () = {
    fn assert_send<T: Send>(_: T) {}
    fn request_is_send(jvm: &'static JavaVM, engine: Engine<'static>, request: BackendRequest) {
        assert_send(execute_request(jvm, engine, request, None, None));
    }
};

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
        let _ = super::attached(self.jvm, 8, |env| {
            if let Err(error) =
                env.call_method(request.as_obj(), jni_str!("cancel"), jni_sig!("()V"), &[])
            {
                java_error(env, "UrlRequest.cancel", error);
            }
            Ok(())
        });
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        callback::unregister(self.id);
        let terminated = self.shared.inner.lock().unwrap().terminated;
        if !terminated {
            self.cancel();
        }
    }
}

/// A request body in the form Cronet is given it, once per hop that sends
/// it.
struct Upload {
    source: UploadSource,
    length: jlong,
    /// Sent as Content-Type unless the caller set one. Never empty: Cronet
    /// refuses to start an upload without a Content-Type.
    content_type: String,
}

impl Upload {
    fn new(body: crate::body::Body) -> Result<Option<Self>> {
        use crate::body::Body;
        let (source, content_type) = match body {
            Body::Empty => return Ok(None),
            Body::Bytes {
                content,
                content_type,
            } => (UploadSource::Bytes(content), content_type),
            Body::Form { fields } => {
                let encoded = fields
                    .iter()
                    .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                    .collect::<Vec<_>>()
                    .join("&");
                (
                    UploadSource::Bytes(encoded.into()),
                    "application/x-www-form-urlencoded".to_string(),
                )
            }
            Body::Json { value } => (
                UploadSource::Bytes(
                    serde_json::to_vec(&value)
                        .map_err(|e| Error::Json(e.to_string()))?
                        .into(),
                ),
                "application/json".to_string(),
            ),
            Body::File { file, content_type } => (UploadSource::File(file), content_type),
            Body::Multipart { .. } => {
                return Err(Error::Internal(
                    "Multipart bodies are not supported by the Android backend".to_string(),
                ));
            }
        };
        let length = jlong::try_from(source.len())
            .map_err(|_| Error::Internal("The request body is too large for Cronet".to_string()))?;
        let content_type = if content_type.is_empty() {
            "application/octet-stream".to_string()
        } else {
            content_type
        };
        Ok(Some(Self {
            source,
            length,
            content_type,
        }))
    }
}

/// One request of a redirect chain.
struct Outgoing<'a> {
    method: &'a Method,
    url: &'a Url,
    /// Everything to send, the jar's cookies included.
    headers: &'a HeaderMap,
    upload: Option<&'a Upload>,
    progress: Option<&'a ProgressCallback>,
}

/// How far Cronet took one request.
enum Outcome {
    /// The response has started and its body can be read.
    Response(ResponseHead, Arc<BodyAccounting>),
    /// Cronet stopped at a redirect response and waits to be told to follow
    /// it. It never reads that response's body.
    Redirect(ResponseHead),
}

/// Run `request`, following redirects, and return once the final response
/// has started.
///
/// `request.headers` are what the caller asked to send, client defaults
/// included; cookies from `cookies` are added for each hop's URL, and each
/// response's cookies stored for the URL that sent them.
///
/// `read_timeout` bounds the wait for each hop's response to start,
/// restarting whenever there is progress (an upload chunk or a redirect),
/// and then each wait for the next body chunk.
pub async fn execute_request(
    jvm: &'static JavaVM,
    engine: Engine<'_>,
    request: BackendRequest,
    cookies: Option<&crate::backend::CookieJar>,
    read_timeout: Option<Duration>,
) -> Result<BackendResponse> {
    let main_document = request.url.clone();
    let progress = request.progress_callback;
    let mut upload = match request.body {
        Some(body) => Upload::new(body)?,
        None => None,
    };
    let (mut method, mut url, mut headers) = (request.method, request.url, request.headers);
    let mut redirects = 0;

    loop {
        let mut sent = headers.clone();
        if let Some(jar) = cookies {
            jar.add_cookie_header(&url, &mut sent);
        }
        let outgoing = Outgoing {
            method: &method,
            url: &url,
            headers: &sent,
            upload: upload.as_ref(),
            progress: progress.as_ref(),
        };
        let (guard, outcome) = send(jvm, engine, &outgoing, read_timeout).await?;

        let redirect = match outcome {
            Outcome::Response(head, accounting) => {
                let url = head.url.unwrap_or(url);
                if let Some(jar) = cookies {
                    jar.store_response_cookies(&main_document, &url, &head.headers);
                }
                return Ok(BackendResponse {
                    status: head.status,
                    accounting,
                    headers: head.headers,
                    url,
                    body: Box::pin(CronetBody {
                        guard,
                        idle: IdleTimer::new(read_timeout),
                        done: false,
                    }),
                });
            }
            Outcome::Redirect(head) => head,
        };
        // Whatever comes next, this request has nothing more to give.
        drop(guard);

        let from = redirect.url.unwrap_or(url);
        if let Some(jar) = cookies {
            jar.store_response_cookies(&main_document, &from, &redirect.headers);
        }
        let Some(hop) =
            redirect::next_hop(&method, &from, &headers, redirect.status, &redirect.headers)
        else {
            // Not a redirect to follow, so it is the response; but Cronet
            // only offers a redirect's head, so the body is empty whatever
            // the server sent, and no length is promised for it.
            let accounting = BodyAccounting::new(false, None);
            accounting.set_integrity(BodyIntegrity::Unverifiable);
            return Ok(BackendResponse {
                status: redirect.status,
                accounting,
                headers: redirect.headers,
                url: from,
                body: Box::pin(futures_util::stream::empty()),
            });
        };

        redirects += 1;
        if redirects > redirect::MAX_REDIRECTS {
            return Err(redirect::too_many_redirects());
        }
        if !hop.keep_body {
            upload = None;
        }
        (method, url, headers) = (hop.method, hop.url, hop.headers);
    }
}

/// Start one Cronet request and wait until its response starts or it stops
/// at a redirect.
async fn send(
    jvm: &'static JavaVM,
    engine: Engine<'_>,
    outgoing: &Outgoing<'_>,
    read_timeout: Option<Duration>,
) -> Result<(RequestGuard, Outcome)> {
    let shared = Arc::new(RequestShared::new(
        outgoing.upload.map(|upload| upload.source.clone()),
        outgoing.progress.cloned(),
    ));
    let id = callback::register(shared.clone());
    let guard = RequestGuard {
        jvm,
        id,
        shared: shared.clone(),
    };

    super::attached(jvm, 32, |env| {
        start_request(env, engine.0, outgoing, id, &shared)
    })?;

    let mut idle = IdleTimer::new(read_timeout);
    let outcome = std::future::poll_fn(|cx| {
        let activity = {
            let mut inner = shared.inner.lock().unwrap();
            if let Some(head) = inner.redirect.take() {
                return Poll::Ready(Ok(Outcome::Redirect(head)));
            }
            if let Some(head) = inner.head.take() {
                let accounting = inner
                    .accounting
                    .clone()
                    .unwrap_or_else(|| BodyAccounting::undecoded(&head.headers));
                return Poll::Ready(Ok(Outcome::Response(head, accounting)));
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

    Ok((guard, outcome))
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
            let attached = super::attached(this.guard.jvm, 8, |env| {
                shared.issue_read(env);
                Ok(())
            });
            if let Err(error) = attached {
                shared.finish(Err(error));
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

/// Build the `UrlRequest`, hand its handle to `shared`, and start it.
fn start_request(
    env: &mut Env,
    cronet_engine: &Global<JObject<'static>>,
    outgoing: &Outgoing<'_>,
    id: jlong,
    shared: &RequestShared,
) -> Result<()> {
    let callback = callback::create_callback_instance(env, id)?;
    let executor = super::callback_executor(env)?;

    let mut builder = UrlRequestBuilder::new(
        env,
        cronet_engine.as_obj(),
        outgoing.url.as_str(),
        &callback,
        executor.as_obj(),
    )
    .map_err(|e| Error::Internal(format!("UrlRequest.Builder: {}", e)))?;
    builder
        .set_http_method(outgoing.method.as_str())
        .map_err(|e| Error::Internal(format!("setHttpMethod: {}", e)))?;
    for (name, value) in outgoing.headers {
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

    if let Some(upload) = outgoing.upload {
        if !outgoing.headers.contains_key(http::header::CONTENT_TYPE) {
            builder
                .add_header("Content-Type", &upload.content_type)
                .map_err(|e| Error::Internal(format!("addHeader(Content-Type): {}", e)))?;
        }
        let provider =
            builder.with_env(|env| callback::create_upload_provider(env, id, upload.length))?;
        builder
            .set_upload_data_provider(&provider, executor.as_obj())
            .map_err(|e| Error::Internal(format!("setUploadDataProvider: {}", e)))?;
    }

    let url_request = builder
        .build()
        .map_err(|e| Error::Internal(format!("UrlRequest.Builder.build: {}", e)))?;
    let global_request = env
        .new_global_ref(&url_request)
        .map_err(|e| java_error(env, "NewGlobalRef", e))?;
    let _ = shared.url_request.set(global_request);

    env.call_method(&url_request, jni_str!("start"), jni_sig!("()V"), &[])
        .map_err(|e| java_error(env, "UrlRequest.start", e))?;
    Ok(())
}
