//! `Debug` output that keeps credentials and payloads out of logs.

use std::fmt;

use http::HeaderMap;

/// Stands in for a value that must not be printed.
pub(crate) struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// `Some(<redacted>)` or `None`, so the output still says whether a secret
/// is set.
pub(crate) fn redacted_option<T>(value: &Option<T>) -> Option<Redacted> {
    value.as_ref().map(|_| Redacted)
}

/// Header names are lowercase. A name is taken as sensitive if it looks
/// like it carries a credential: a fixed list would miss the `x-api-key`s
/// and `x-amz-security-token`s that every service invents.
pub(crate) fn is_sensitive_header(name: &str) -> bool {
    const MARKERS: [&str; 9] = [
        "auth",
        "cookie",
        "token",
        "secret",
        "key",
        "password",
        "credential",
        "session",
        "signature",
    ];
    MARKERS.iter().any(|marker| name.contains(marker))
}

/// A header map whose sensitive values print as `<redacted>`.
pub(crate) struct RedactedHeaders<'a>(pub &'a HeaderMap);

impl fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (name, value) in self.0 {
            if is_sensitive_header(name.as_str()) {
                map.entry(&name.as_str(), &Redacted);
            } else {
                map.entry(&name.as_str(), value);
            }
        }
        map.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn sensitive_headers_are_redacted_and_others_are_not() {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("authorization", "Bearer hunter2-token"),
            ("proxy-authorization", "Basic hunter2-proxy"),
            ("cookie", "sid=hunter2-cookie"),
            ("set-cookie", "sid=hunter2-set; Path=/"),
            ("x-api-key", "hunter2-key"),
            ("x-amz-security-token", "hunter2-amz"),
            ("content-type", "text/plain"),
        ] {
            headers.append(name, HeaderValue::from_static(value));
        }

        let debug = format!("{:?}", RedactedHeaders(&headers));
        assert!(!debug.contains("hunter2"), "{}", debug);
        assert!(debug.contains("text/plain"), "{}", debug);
        assert!(debug.contains("authorization"), "{}", debug);
    }

    #[test]
    fn secret_bearing_types_do_not_print_their_secrets() {
        use crate::backend::types::{BackendResponse, BodyAccounting};

        const SECRET: &str = "hunter2";
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer hunter2"));
        headers.append(
            "set-cookie",
            HeaderValue::from_static("sid=hunter2; Path=/"),
        );
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        let proxy = crate::client::ProxyConfig {
            host: "proxy.example".to_string(),
            port: 8080,
            username: Some(SECRET.to_string()),
            password: Some(SECRET.to_string()),
        };
        let response = || {
            crate::Response::from_backend(
                BackendResponse {
                    status: http::StatusCode::UNAUTHORIZED,
                    headers: headers.clone(),
                    url: url::Url::parse("http://example.invalid/").unwrap(),
                    body: Box::pin(futures_util::stream::empty()),
                    accounting: BodyAccounting::undecoded(&headers),
                },
                None,
                false,
            )
        };
        let jar = crate::CookieJar::with_cookie_store();
        jar.add_cookie(crate::Cookie::new("sid", SECRET).domain("example.com"))
            .unwrap();

        let outputs = [
            format!("{:?}", crate::Auth::basic("alice", SECRET)),
            format!("{:?}", crate::Auth::bearer(SECRET)),
            format!("{:?}", crate::Auth::custom("ApiKey", SECRET)),
            format!("{:?}", proxy),
            format!("{:?}", crate::Cookie::new("sid", SECRET)),
            format!("{:?}", jar),
            format!("{:?}", jar.all_cookies()),
            format!("{:?}", crate::Body::text(SECRET)),
            format!(
                "{:?}",
                crate::Body::bytes(SECRET.as_bytes().to_vec(), "x/y")
            ),
            format!("{:?}", crate::Body::form(vec![("password", SECRET)])),
            format!(
                "{:?}",
                crate::Body::json(serde_json::json!({ "token": SECRET })).unwrap()
            ),
            format!(
                "{:?}",
                crate::Body::multipart(vec![crate::MultipartPart::text("field", SECRET)])
            ),
            format!(
                "{:?}",
                crate::DownloadResponse {
                    file_path: "file".into(),
                    bytes_downloaded: 0,
                    status: http::StatusCode::OK,
                    headers: headers.clone(),
                }
            ),
            format!(
                "{:?}",
                crate::backend::BackendConfig {
                    default_headers: Some(headers.clone()),
                    cookie_jar: Some(jar.clone()),
                    http_proxy: Some(proxy.clone()),
                    ..Default::default()
                }
            ),
            format!("{:?}", response()),
            format!("{:?}", crate::Error::HttpError(Box::new(response()))),
            #[cfg(any(
                all(feature = "backend-foundation", target_vendor = "apple"),
                all(feature = "backend-android", target_os = "android")
            ))]
            format!(
                "{:?}",
                crate::background::BackgroundJob {
                    id: "job".to_string(),
                    url: url::Url::parse("http://example.invalid/").unwrap(),
                    headers: headers.clone(),
                    destination: None,
                }
            ),
        ];
        for output in outputs {
            assert!(!output.contains(SECRET), "{}", output);
        }
    }
}
