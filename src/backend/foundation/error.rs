//! Foundation error handling

use crate::Error;

impl Error {
    /// Convert NSError to our Error type (Foundation backend specific)
    pub(crate) fn from_ns_error(error: &objc2_foundation::NSError) -> Self {
        use objc2_foundation::NSURLErrorDomain;

        let domain = error.domain().to_string();
        let code = error.code() as i64;
        let message = error.localizedDescription().to_string();

        if domain == unsafe { NSURLErrorDomain }.to_string() {
            match code {
                -999 => Error::Cancelled,
                -1001 => Error::Timeout,
                // NSURLErrorBadURL, NSURLErrorUnsupportedURL
                -1000 | -1002 => Error::InvalidUrl,
                // NSURLErrorClientCertificateRequired (-1206) through
                // NSURLErrorSecureConnectionFailed (-1200): certificate and
                // TLS handshake failures.
                -1206..=-1200 => Error::Tls { message },
                // Everything else in the domain, including cannot find host
                // (-1003), cannot connect (-1004), connection lost (-1005),
                // DNS lookup failed (-1006) and not connected (-1009), is a
                // network failure.
                _ => Error::Network { code, message },
            }
        } else if domain == "NSPOSIXErrorDomain" || domain == "kCFErrorDomainCFNetwork" {
            Error::Network { code, message }
        } else {
            Error::Internal(format!(
                "Domain: {}, Code: {}, Message: {}",
                domain, code, message
            ))
        }
    }
}
