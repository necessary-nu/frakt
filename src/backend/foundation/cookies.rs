//! Foundation cookie storage implementation using NSHTTPCookieStorage

use crate::{
    Error, Result,
    cookies::{Cookie, CookieAcceptPolicy},
};
use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2::runtime::ProtocolObject;
use objc2_foundation::{
    NSCopying, NSDictionary, NSHTTPCookie, NSHTTPCookieDomain, NSHTTPCookieName, NSHTTPCookiePath,
    NSHTTPCookieSecure, NSHTTPCookieStorage, NSHTTPCookieValue, NSMutableDictionary, NSString,
    NSURL,
};

/// Foundation cookie storage using NSHTTPCookieStorage
#[derive(Debug, Clone)]
pub struct FoundationCookieStorage {
    storage: Retained<NSHTTPCookieStorage>,
}

impl Cookie {
    /// Convert to NSHTTPCookie (Foundation specific)
    pub(crate) fn to_ns_cookie(&self) -> Result<Retained<NSHTTPCookie>> {
        unsafe {
            // Create NSMutableDictionary with NSCopying protocol keys
            let dict: Retained<NSMutableDictionary<ProtocolObject<dyn NSCopying>, AnyObject>> =
                NSMutableDictionary::new();

            // Add required properties using the actual constants
            dict.setObject_forKey(
                &*NSString::from_str(&self.name) as &AnyObject,
                ProtocolObject::from_ref(NSHTTPCookieName),
            );

            dict.setObject_forKey(
                &*NSString::from_str(&self.value) as &AnyObject,
                ProtocolObject::from_ref(NSHTTPCookieValue),
            );

            dict.setObject_forKey(
                &*NSString::from_str(&self.domain) as &AnyObject,
                ProtocolObject::from_ref(NSHTTPCookieDomain),
            );

            dict.setObject_forKey(
                &*NSString::from_str(&self.path) as &AnyObject,
                ProtocolObject::from_ref(NSHTTPCookiePath),
            );

            // Add optional properties
            if self.secure {
                dict.setObject_forKey(
                    &*NSString::from_str("TRUE") as &AnyObject,
                    ProtocolObject::from_ref(NSHTTPCookieSecure),
                );
            }

            // Cast the dictionary to NSDictionary and create the cookie
            let cookie_dict: Retained<NSDictionary<NSString, AnyObject>> =
                objc2::rc::Retained::cast_unchecked(dict);
            NSHTTPCookie::cookieWithProperties(&*cookie_dict)
                .ok_or_else(|| Error::Internal("Failed to create NSHTTPCookie".to_string()))
        }
    }

    /// Create from NSHTTPCookie (Foundation specific)
    pub(crate) fn from_ns_cookie(ns_cookie: &NSHTTPCookie) -> Option<Self> {
        let name = ns_cookie.name().to_string();
        let value = ns_cookie.value().to_string();
        let domain = ns_cookie.domain().to_string();
        let path = ns_cookie.path().to_string();
        let secure = ns_cookie.isSecure();
        let http_only = ns_cookie.isHTTPOnly();

        // Get expiration date as string if available
        let expires = ns_cookie
            .expiresDate()
            .map(|date| date.description().to_string());

        Some(Self {
            name,
            value,
            domain,
            path,
            secure,
            http_only,
            expires,
        })
    }
}

impl FoundationCookieStorage {
    pub(crate) fn ns_storage(&self) -> &NSHTTPCookieStorage {
        &self.storage
    }

    /// Create a new cookie storage with shared storage
    pub fn new() -> Self {
        Self {
            storage: NSHTTPCookieStorage::sharedHTTPCookieStorage(),
        }
    }

    /// Create a new cookie storage for a specific group container
    pub fn for_group_container(identifier: &str) -> Self {
        let storage = NSHTTPCookieStorage::sharedCookieStorageForGroupContainerIdentifier(
            &NSString::from_str(identifier),
        );
        Self { storage }
    }

    /// Get all cookies
    pub fn all_cookies(&self) -> Vec<Cookie> {
        let cookies = self.storage.cookies();
        if let Some(cookies) = cookies {
            (0..cookies.len())
                .map(|i| cookies.objectAtIndex(i))
                .filter_map(|cookie| Cookie::from_ns_cookie(&cookie))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get cookies for a specific URL
    pub fn cookies_for_url(&self, url: &str) -> Result<Vec<Cookie>> {
        let nsurl = NSURL::URLWithString(&NSString::from_str(url)).ok_or(Error::InvalidUrl)?;

        let cookies = self.storage.cookiesForURL(&nsurl);
        if let Some(cookies) = cookies {
            Ok((0..cookies.len())
                .map(|i| cookies.objectAtIndex(i))
                .filter_map(|cookie| Cookie::from_ns_cookie(&cookie))
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Add a cookie
    pub fn add_cookie(&self, cookie: Cookie) -> Result<()> {
        let ns_cookie = cookie.to_ns_cookie()?;

        self.storage.setCookie(&ns_cookie);

        Ok(())
    }

    /// Remove a cookie
    pub fn remove_cookie(&self, cookie: Cookie) -> Result<()> {
        let ns_cookie = cookie.to_ns_cookie()?;

        self.storage.deleteCookie(&ns_cookie);

        Ok(())
    }

    /// Clear all cookies
    pub fn clear(&self) {
        let cookies = self.all_cookies();
        for cookie in cookies {
            let _ = self.remove_cookie(cookie);
        }
    }

    /// Set cookie acceptance policy
    pub fn set_cookie_accept_policy(&self, policy: CookieAcceptPolicy) {
        use objc2_foundation::NSHTTPCookieAcceptPolicy;

        let ns_policy = match policy {
            CookieAcceptPolicy::Always => NSHTTPCookieAcceptPolicy::Always,
            CookieAcceptPolicy::Never => NSHTTPCookieAcceptPolicy::Never,
            CookieAcceptPolicy::OnlyFromMainDocumentDomain => {
                NSHTTPCookieAcceptPolicy::OnlyFromMainDocumentDomain
            }
        };

        self.storage.setCookieAcceptPolicy(ns_policy);
    }
}
