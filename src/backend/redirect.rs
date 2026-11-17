//! Redirect policy for the backends that follow redirects themselves.
//!
//! WinHTTP forwards every header the caller supplied, unchanged, to whatever
//! origin a redirect names; Cronet does the same for headers it was handed
//! explicitly, as cookies from frakt's jar are; and reqwest keeps any
//! credential it does not know by name. Those backends turn their stack's
//! redirect handling off and ask [`next_hop`] what to send instead,
//! recomputing cookies for each hop. Foundation follows redirects itself
//! and applies the same header rule in its delegate.

use http::{HeaderMap, Method, StatusCode, header};
use url::Url;

use crate::Error;

/// How many redirects one request follows before failing with
/// [`too_many_redirects`]; the same limit as reqwest's.
pub(crate) const MAX_REDIRECTS: usize = 10;

/// The request that follows a redirect.
#[derive(Debug)]
pub(crate) struct Hop {
    pub method: Method,
    pub url: Url,
    /// The previous request's headers, less the ones that must not follow
    /// it. Cookies from a jar are not among them: add those per hop.
    pub headers: HeaderMap,
    /// Whether the request body is sent again. When false, the headers that
    /// described it have been removed.
    pub keep_body: bool,
}

/// The request to send after `status` and `response_headers` answered
/// `method url` sent with `headers`, or `None` if the response is not a
/// redirect that can be followed and should be delivered as it is.
///
/// `headers` must be what the caller asked to send (the request's headers
/// merged with the client's defaults) without cookies added from a jar.
pub(crate) fn next_hop(
    method: &Method,
    url: &Url,
    headers: &HeaderMap,
    status: StatusCode,
    response_headers: &HeaderMap,
) -> Option<Hop> {
    let rewrite_to_get = match status {
        StatusCode::SEE_OTHER => *method != Method::HEAD && *method != Method::GET,
        StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND => *method == Method::POST,
        StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT => false,
        _ => return None,
    };

    let location = std::str::from_utf8(response_headers.get(header::LOCATION)?.as_bytes()).ok()?;
    let mut next = url.join(location.trim()).ok()?;
    if !matches!(next.scheme(), "http" | "https") {
        return None;
    }
    if next.fragment().is_none() {
        next.set_fragment(url.fragment());
    }

    let mut headers = headers.clone();
    if rewrite_to_get {
        for name in [
            header::CONTENT_LENGTH,
            header::CONTENT_TYPE,
            header::CONTENT_ENCODING,
            header::CONTENT_LANGUAGE,
            header::CONTENT_LOCATION,
            header::TRANSFER_ENCODING,
        ] {
            headers.remove(name);
        }
    }
    if !same_origin(url, &next) {
        let credentials: Vec<_> = headers
            .keys()
            .filter(|name| crate::redact::is_sensitive_header(name.as_str()))
            .cloned()
            .collect();
        for name in credentials {
            headers.remove(name);
        }
        headers.remove(header::HOST);
    }

    Some(Hop {
        method: if rewrite_to_get {
            Method::GET
        } else {
            method.clone()
        },
        url: next,
        headers,
        keep_body: !rewrite_to_get,
    })
}

/// Scheme, host and port all equal: the boundary credentials must not
/// cross, which makes an https to http downgrade a change of origin too.
pub(crate) fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host() == b.host()
        && a.port_or_known_default() == b.port_or_known_default()
}

/// The error for a chain longer than [`MAX_REDIRECTS`].
pub(crate) fn too_many_redirects() -> Error {
    Error::Network {
        code: -1,
        message: format!("Stopped after {} redirects", MAX_REDIRECTS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, HeaderValue::from_static(value));
        }
        map
    }

    fn redirect(
        method: Method,
        from: &str,
        request: &[(&'static str, &'static str)],
        status: u16,
        location: &'static str,
    ) -> Option<Hop> {
        next_hop(
            &method,
            &Url::parse(from).unwrap(),
            &headers(request),
            StatusCode::from_u16(status).unwrap(),
            &headers(&[("location", location)]),
        )
    }

    const CREDENTIALS: [(&str, &str); 6] = [
        ("authorization", "Bearer t"),
        ("proxy-authorization", "Basic p"),
        ("cookie", "sid=1"),
        ("x-api-key", "k"),
        ("range", "bytes=5-"),
        ("accept", "*/*"),
    ];

    #[test]
    fn same_origin_keeps_every_header() {
        let hop = redirect(
            Method::GET,
            "https://a.example/one",
            &CREDENTIALS,
            302,
            "/two?x=1",
        )
        .unwrap();
        assert_eq!(hop.url.as_str(), "https://a.example/two?x=1");
        assert_eq!(hop.method, Method::GET);
        assert_eq!(hop.headers, headers(&CREDENTIALS));
        assert!(hop.keep_body);
    }

    #[test]
    fn other_host_port_or_scheme_loses_credentials() {
        for location in [
            "https://b.example/",
            "https://a.example:8443/",
            "http://a.example/",
            "//sub.a.example/",
        ] {
            let hop = redirect(
                Method::GET,
                "https://a.example/one",
                &CREDENTIALS,
                307,
                location,
            )
            .unwrap();
            assert_eq!(
                hop.headers,
                headers(&[("range", "bytes=5-"), ("accept", "*/*")]),
                "{}",
                location
            );
        }
    }

    #[test]
    fn default_port_is_the_same_origin() {
        let hop = redirect(
            Method::GET,
            "https://a.example/one",
            &CREDENTIALS,
            301,
            "https://a.example:443/two",
        )
        .unwrap();
        assert_eq!(hop.headers, headers(&CREDENTIALS));
    }

    #[test]
    fn methods_and_bodies_follow_the_status() {
        let body = [("content-type", "text/plain"), ("content-length", "3")];
        for (method, status, expected, keep_body) in [
            (Method::POST, 301, Method::GET, false),
            (Method::POST, 302, Method::GET, false),
            (Method::POST, 303, Method::GET, false),
            (Method::POST, 307, Method::POST, true),
            (Method::POST, 308, Method::POST, true),
            (Method::PUT, 301, Method::PUT, true),
            (Method::PUT, 302, Method::PUT, true),
            (Method::PUT, 303, Method::GET, false),
            (Method::DELETE, 303, Method::GET, false),
            (Method::HEAD, 303, Method::HEAD, true),
            (Method::GET, 303, Method::GET, true),
        ] {
            let hop =
                redirect(method.clone(), "http://a.example/", &body, status, "/next").unwrap();
            assert_eq!(hop.method, expected, "{} {}", method, status);
            assert_eq!(hop.keep_body, keep_body, "{} {}", method, status);
            assert_eq!(
                hop.headers.contains_key("content-length"),
                keep_body,
                "{} {}",
                method,
                status
            );
            assert_eq!(
                hop.headers.contains_key("content-type"),
                keep_body,
                "{} {}",
                method,
                status
            );
        }
    }

    #[test]
    fn responses_that_are_not_followable_redirects_are_delivered() {
        assert!(redirect(Method::GET, "http://a.example/", &[], 200, "/x").is_none());
        assert!(redirect(Method::GET, "http://a.example/", &[], 304, "/x").is_none());
        assert!(redirect(Method::GET, "http://a.example/", &[], 300, "/x").is_none());
        assert!(
            redirect(
                Method::GET,
                "http://a.example/",
                &[],
                302,
                "ftp://a.example/"
            )
            .is_none()
        );
        assert!(
            redirect(
                Method::GET,
                "http://a.example/",
                &[],
                302,
                "file:///etc/passwd"
            )
            .is_none()
        );
        assert!(redirect(Method::GET, "http://a.example/", &[], 302, "http://[bad").is_none());
        assert!(
            next_hop(
                &Method::GET,
                &Url::parse("http://a.example/").unwrap(),
                &HeaderMap::new(),
                StatusCode::FOUND,
                &HeaderMap::new(),
            )
            .is_none()
        );
    }

    #[test]
    fn fragment_carries_over_unless_replaced() {
        let hop = redirect(Method::GET, "http://a.example/x#top", &[], 302, "/y").unwrap();
        assert_eq!(hop.url.as_str(), "http://a.example/y#top");
        let hop = redirect(Method::GET, "http://a.example/x#top", &[], 302, "/y#end").unwrap();
        assert_eq!(hop.url.as_str(), "http://a.example/y#end");
    }
}
