//! Unified cookie storage implementation using cookie_store
//!
//! This module provides a common cookie storage implementation for all non-Apple
//! backends (Windows, Android, Reqwest). It wraps the cookie_store crate to provide
//! RFC 6265 compliant cookie handling with a consistent API.

use crate::{
    Error, Result,
    cookies::{Cookie, CookieAcceptPolicy},
};
use cookie_store::CookieStore;
use http::{HeaderMap, HeaderValue};
use std::sync::{Arc, Mutex, MutexGuard};
use url::{Host, Url};

/// Unified cookie storage implementation using cookie_store
///
/// This implementation is used by Windows, Android, and Reqwest backends
/// to provide consistent cookie handling across all non-Apple platforms.
/// Clones share one store.
#[derive(Clone)]
pub struct CookieJar {
    /// RFC 6265 compliant cookie storage wrapped in Arc<Mutex> for thread safety
    store: Arc<Mutex<CookieStore>>,
    /// Cookie acceptance policy (stored separately as cookie_store doesn't support this)
    accept_policy: Arc<Mutex<CookieAcceptPolicy>>,
}

/// Cookie values are credentials, so only their number is shown.
impl std::fmt::Debug for CookieJar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CookieJar")
            .field("cookies", &self.lock().iter_any().count())
            .field("accept_policy", &self.get_cookie_accept_policy())
            .finish()
    }
}

impl CookieJar {
    /// Create a new cookie storage
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(CookieStore::default())),
            accept_policy: Arc::new(Mutex::new(CookieAcceptPolicy::Always)),
        }
    }

    /// A panic while the lock was held cannot leave the store half-updated
    /// in a way that matters more than losing every cookie would.
    fn lock(&self) -> MutexGuard<'_, CookieStore> {
        self.store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Get all cookies stored in the jar
    pub fn all_cookies(&self) -> Vec<Cookie> {
        self.lock()
            .iter_any()
            .map(Self::convert_from_cookie_store)
            .collect()
    }

    /// Get cookies for a specific URL
    pub fn cookies_for_url(&self, url: &str) -> Result<Vec<Cookie>> {
        let parsed_url = Url::parse(url).map_err(|_| Error::InvalidUrl)?;

        // The store has already matched domain, path and security, so only
        // the name and value are meaningful here.
        Ok(self
            .lock()
            .get_request_values(&parsed_url)
            .map(|(name, value)| Cookie::new(name, value))
            .collect())
    }

    /// Add a cookie to the jar
    pub fn add_cookie(&self, cookie: Cookie) -> Result<()> {
        let mut set_cookie = format!("{}={}", cookie.name, cookie.value);

        if !cookie.domain.is_empty() {
            set_cookie.push_str(&format!("; Domain={}", cookie.domain));
        }

        if cookie.path != "/" {
            set_cookie.push_str(&format!("; Path={}", cookie.path));
        }

        if cookie.secure {
            set_cookie.push_str("; Secure");
        }

        if cookie.http_only {
            set_cookie.push_str("; HttpOnly");
        }

        if let Some(ref expires) = cookie.expires {
            set_cookie.push_str(&format!("; Expires={}", expires));
        }

        let url = Self::origin_url(&cookie)?;
        self.lock()
            .parse(&set_cookie, &url)
            .map_err(|e| Error::Internal(format!("Failed to parse cookie: {}", e)))?;

        Ok(())
    }

    /// Remove a cookie from the jar
    pub fn remove_cookie(&self, cookie: Cookie) -> Result<()> {
        let url = Self::origin_url(&cookie)?;

        // An expired cookie with the same name, domain and path replaces,
        // and so removes, the original.
        let mut expired_cookie = format!("{}=", cookie.name);
        if !cookie.domain.is_empty() {
            expired_cookie.push_str(&format!("; Domain={}", cookie.domain));
        }
        expired_cookie.push_str(&format!(
            "; Path={}; Expires=Thu, 01 Jan 1970 00:00:00 GMT",
            cookie.path
        ));

        self.lock()
            .parse(&expired_cookie, &url)
            .map_err(|e| Error::Internal(format!("Failed to remove cookie: {}", e)))?;

        Ok(())
    }

    /// A URL the cookie would be accepted from: the store only takes
    /// cookies as if a response had set them.
    fn origin_url(cookie: &Cookie) -> Result<Url> {
        let scheme = if cookie.secure { "https" } else { "http" };
        let domain = if cookie.domain.is_empty() {
            "localhost"
        } else {
            cookie.domain.trim_start_matches('.')
        };
        Url::parse(&format!("{}://{}{}", scheme, domain, cookie.path))
            .map_err(|_| Error::InvalidUrl)
    }

    /// Clear all cookies from the jar
    pub fn clear(&self) {
        *self.lock() = CookieStore::default();
    }

    /// Set the cookie acceptance policy, which
    /// [`store_response_cookies`](Self::store_response_cookies) enforces.
    pub fn set_cookie_accept_policy(&self, policy: CookieAcceptPolicy) {
        *self
            .accept_policy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = policy;
    }

    /// Get the current cookie acceptance policy
    pub fn get_cookie_accept_policy(&self) -> CookieAcceptPolicy {
        *self
            .accept_policy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Store the `Set-Cookie` headers of one response.
    ///
    /// `response_url` must be the URL that produced `headers`: for a
    /// redirect, the URL that answered with the redirect, not where it led.
    /// `main_document` is the URL the caller asked for, before any
    /// redirects, which
    /// [`CookieAcceptPolicy::OnlyFromMainDocumentDomain`] compares against.
    pub fn store_response_cookies(
        &self,
        main_document: &Url,
        response_url: &Url,
        headers: &HeaderMap,
    ) {
        self.store_set_cookies(
            Some(main_document),
            response_url,
            headers.get_all(http::header::SET_COOKIE).iter(),
        );
    }

    /// [`store_response_cookies`](Self::store_response_cookies) for a stack
    /// that hands over the `Set-Cookie` values themselves. Without a main
    /// document, `OnlyFromMainDocumentDomain` has nothing to compare against
    /// and stores nothing.
    pub fn store_set_cookies<'a>(
        &self,
        main_document: Option<&Url>,
        response_url: &Url,
        set_cookies: impl Iterator<Item = &'a HeaderValue>,
    ) {
        match self.get_cookie_accept_policy() {
            CookieAcceptPolicy::Always => {}
            CookieAcceptPolicy::Never => return,
            CookieAcceptPolicy::OnlyFromMainDocumentDomain => {
                if !main_document.is_some_and(|main| same_document_domain(main, response_url)) {
                    return;
                }
            }
        }

        let mut store = self.lock();
        for value in set_cookies {
            if let Ok(set_cookie) = value.to_str() {
                // The store rejects cookies the response URL may not set.
                let _ = store.parse(set_cookie, response_url);
            }
        }
    }

    /// The `Cookie` header value for a request to `url`, if any cookie
    /// applies.
    pub fn cookie_header(&self, url: &Url) -> Option<HeaderValue> {
        let cookies = self
            .lock()
            .get_request_values(url)
            .map(|(name, value)| format!("{}={}", name, value))
            .collect::<Vec<_>>()
            .join("; ");
        if cookies.is_empty() {
            return None;
        }
        HeaderValue::from_str(&cookies).ok()
    }

    /// Add the jar's cookies for `url` to a request's headers. A `Cookie`
    /// header the caller set is sent as it is, without the jar's.
    pub fn add_cookie_header(&self, url: &Url, headers: &mut HeaderMap) {
        if headers.contains_key(http::header::COOKIE) {
            return;
        }
        if let Some(cookies) = self.cookie_header(url) {
            headers.insert(http::header::COOKIE, cookies);
        }
    }

    /// Convert from cookie_store::Cookie to our Cookie type
    fn convert_from_cookie_store(cookie: &cookie_store::Cookie) -> Cookie {
        Cookie {
            name: cookie.name().to_string(),
            value: cookie.value().to_string(),
            domain: cookie.domain().unwrap_or("").to_string(),
            path: cookie.path().unwrap_or("/").to_string(),
            secure: cookie.secure().unwrap_or(false),
            http_only: cookie.http_only().unwrap_or(false),
            // cookie_store keeps expiry in its own form; it is not exposed.
            expires: None,
        }
    }
}

impl Default for CookieJar {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether `response` is in the main document's domain: the same host, or
/// one a subdomain of the other where the shorter has at least two labels
/// (so `example.com` and `www.example.com` match, `localhost` and
/// `evil.localhost` do not). Sibling hosts such as `a.example.com` and
/// `b.example.com` do not match: telling a registrable domain from a public
/// suffix needs the public suffix list, and without it the narrower rule is
/// the safe one. IP addresses only match themselves.
fn same_document_domain(main_document: &Url, response: &Url) -> bool {
    match (main_document.host(), response.host()) {
        (Some(Host::Domain(main)), Some(Host::Domain(response))) => {
            let main = main.trim_end_matches('.');
            let response = response.trim_end_matches('.');
            if main.eq_ignore_ascii_case(response) {
                return true;
            }
            let (shorter, longer) = if main.len() < response.len() {
                (main, response)
            } else {
                (response, main)
            };
            shorter.contains('.')
                && longer.len() > shorter.len()
                && longer.as_bytes()[longer.len() - shorter.len() - 1] == b'.'
                && longer[longer.len() - shorter.len()..].eq_ignore_ascii_case(shorter)
        }
        (Some(main), Some(response)) => main == response,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    fn set_cookie(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.append(http::header::SET_COOKIE, HeaderValue::from_static(value));
        headers
    }

    fn names(jar: &CookieJar) -> Vec<String> {
        let mut names: Vec<_> = jar.all_cookies().into_iter().map(|c| c.name).collect();
        names.sort();
        names
    }

    #[test]
    fn document_domain_matching() {
        let same = |a: &str, b: &str| same_document_domain(&url(a), &url(b));
        assert!(same("https://example.com/", "https://example.com/x"));
        assert!(same("https://example.com/", "http://EXAMPLE.com:8080/"));
        assert!(same("https://example.com/", "https://www.example.com/"));
        assert!(same("https://www.example.com/", "https://example.com/"));
        assert!(!same("https://a.example.com/", "https://b.example.com/"));
        assert!(!same("https://example.com/", "https://example.org/"));
        assert!(!same("https://example.com/", "https://notexample.com/"));
        assert!(!same("https://example.com/", "https://evil-example.com/"));
        assert!(!same("http://localhost/", "http://evil.localhost/"));
        assert!(same("http://127.0.0.1/", "http://127.0.0.1:81/"));
        assert!(!same("http://127.0.0.1/", "http://127.0.0.2/"));
        assert!(!same("http://127.0.0.1/", "http://example.com/"));
    }

    #[test]
    fn redirect_cookies_belong_to_the_redirecting_origin() {
        let jar = CookieJar::new();
        let a = url("https://a.example/start");
        jar.store_response_cookies(&a, &a, &set_cookie("session=attacker; Path=/"));

        assert!(jar.cookie_header(&url("https://b.example/")).is_none());
        assert_eq!(
            jar.cookie_header(&url("https://a.example/")).unwrap(),
            "session=attacker"
        );
    }

    #[test]
    fn only_from_main_document_domain_drops_other_hosts() {
        let jar = CookieJar::new();
        jar.set_cookie_accept_policy(CookieAcceptPolicy::OnlyFromMainDocumentDomain);
        let main = url("https://example.com/start");

        jar.store_response_cookies(&main, &main, &set_cookie("first=1"));
        jar.store_response_cookies(
            &main,
            &url("https://cdn.example.com/x"),
            &set_cookie("sub=1"),
        );
        jar.store_response_cookies(
            &main,
            &url("https://tracker.example.net/x"),
            &set_cookie("third=1"),
        );
        assert_eq!(names(&jar), ["first", "sub"]);

        // No main document to compare against: nothing is stored.
        jar.store_set_cookies(None, &main, set_cookie("orphan=1").values());
        assert_eq!(names(&jar), ["first", "sub"]);
    }

    #[test]
    fn never_stores_nothing_and_always_stores_everything() {
        let jar = CookieJar::new();
        let main = url("https://example.com/");
        let other = url("https://other.example.net/");

        jar.set_cookie_accept_policy(CookieAcceptPolicy::Never);
        jar.store_response_cookies(&main, &main, &set_cookie("a=1"));
        assert!(jar.all_cookies().is_empty());

        jar.set_cookie_accept_policy(CookieAcceptPolicy::Always);
        jar.store_response_cookies(&main, &main, &set_cookie("a=1"));
        jar.store_response_cookies(&main, &other, &set_cookie("b=1"));
        assert_eq!(names(&jar), ["a", "b"]);
    }

    #[test]
    fn added_cookies_are_sent_and_removed_cookies_are_not() {
        let jar = CookieJar::new();
        let cookie = Cookie::new("token", "s3cret").domain("example.com");
        jar.add_cookie(cookie.clone()).unwrap();
        assert_eq!(
            jar.cookie_header(&url("http://example.com/")).unwrap(),
            "token=s3cret"
        );

        jar.remove_cookie(cookie).unwrap();
        assert!(jar.cookie_header(&url("http://example.com/")).is_none());
    }

    #[test]
    fn debug_does_not_show_cookie_values() {
        let jar = CookieJar::new();
        jar.add_cookie(Cookie::new("token", "s3cret").domain("example.com"))
            .unwrap();
        let debug = format!("{:?}", jar);
        assert!(!debug.contains("s3cret"), "{}", debug);
    }
}
