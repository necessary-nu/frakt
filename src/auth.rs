//! Authentication support for HTTP requests

use std::fmt;

/// Authentication methods supported by HTTP requests.
///
/// This enum provides common authentication schemes that can be used with HTTP requests.
/// Each variant automatically generates the appropriate `Authorization` header value.
///
/// # Examples
///
/// ```rust,no_run
/// use frakt::{Client, Auth};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new()?;
///
/// // Basic authentication
/// let response = client
///     .get("https://httpbin.org/basic-auth/user/pass")?
///     .auth(Auth::basic("user", "pass"))?
///     .send()
///     .await?;
///
/// // Bearer token
/// let response = client
///     .get("https://api.example.com/protected")?
///     .auth(Auth::bearer("your-jwt-token"))?
///     .send()
///     .await?;
///
/// // Custom authentication
/// let response = client
///     .get("https://api.example.com/data")?
///     .auth(Auth::custom("ApiKey", "your-api-key"))?
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub enum Auth {
    /// HTTP Basic authentication with username and password.
    ///
    /// This creates an `Authorization: Basic <base64(username:password)>` header.
    Basic {
        /// Username for basic authentication
        username: String,
        /// Password for basic authentication
        password: String,
    },
    /// Bearer token authentication (OAuth, JWT, etc.).
    ///
    /// This creates an `Authorization: Bearer <token>` header.
    Bearer {
        /// Bearer token
        token: String,
    },
    /// Custom authorization header with a custom scheme.
    ///
    /// This creates an `Authorization: <scheme> <credentials>` header.
    Custom {
        /// Authentication scheme (e.g., "ApiKey", "Digest")
        scheme: String,
        /// Credentials for the scheme
        credentials: String,
    },
}

/// Passwords, tokens and credentials are not shown.
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::redact::Redacted;
        match self {
            Auth::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &Redacted)
                .finish(),
            Auth::Bearer { .. } => f.debug_struct("Bearer").field("token", &Redacted).finish(),
            Auth::Custom { scheme, .. } => f
                .debug_struct("Custom")
                .field("scheme", scheme)
                .field("credentials", &Redacted)
                .finish(),
        }
    }
}

impl Auth {
    /// The `Authorization` header value, or [`Error::InvalidHeader`](crate::Error::InvalidHeader)
    /// if it would not be a valid one: a custom scheme that is not an HTTP
    /// token, or a token or credentials containing a line break or other
    /// control character.
    pub(crate) fn header_value(&self) -> crate::Result<http::HeaderValue> {
        if let Auth::Custom { scheme, .. } = self {
            let token_char = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c);
            if scheme.is_empty() || !scheme.chars().all(token_char) {
                return Err(crate::Error::InvalidHeader);
            }
        }
        let mut value = http::HeaderValue::from_str(&self.to_header_value())
            .map_err(|_| crate::Error::InvalidHeader)?;
        value.set_sensitive(true);
        Ok(value)
    }

    /// Create HTTP Basic authentication.
    ///
    /// This method creates Basic authentication using the provided username and password.
    /// The credentials will be base64-encoded when the header value is generated.
    ///
    /// # Arguments
    ///
    /// * `username` - The username for authentication
    /// * `password` - The password for authentication
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Auth;
    ///
    /// let auth = Auth::basic("john_doe", "secret123");
    /// ```
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Basic {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Create Bearer token authentication.
    ///
    /// This method creates Bearer token authentication using the provided token.
    /// This is commonly used for OAuth 2.0, JWT tokens, and API keys.
    ///
    /// # Arguments
    ///
    /// * `token` - The bearer token
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Auth;
    ///
    /// let auth = Auth::bearer("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...");
    /// ```
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer {
            token: token.into(),
        }
    }

    /// Create custom authentication with a custom scheme.
    ///
    /// This method allows you to create authentication using any custom scheme
    /// and credentials. The Authorization header will be formatted as `<scheme> <credentials>`.
    ///
    /// # Arguments
    ///
    /// * `scheme` - The authentication scheme (e.g., "ApiKey", "Digest", "HMAC")
    /// * `credentials` - The credentials for the scheme
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Auth;
    ///
    /// let auth = Auth::custom("ApiKey", "your-api-key-here");
    /// let auth = Auth::custom("Digest", "username=\"john\", realm=\"api\"");
    /// ```
    pub fn custom(scheme: impl Into<String>, credentials: impl Into<String>) -> Self {
        Self::Custom {
            scheme: scheme.into(),
            credentials: credentials.into(),
        }
    }

    /// Convert authentication to Authorization header value.
    ///
    /// This method generates the complete value for the `Authorization` HTTP header
    /// based on the authentication type. The resulting string can be used directly
    /// as the header value.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use frakt::Auth;
    ///
    /// let basic_auth = Auth::basic("user", "pass");
    /// assert_eq!(basic_auth.to_header_value(), "Basic dXNlcjpwYXNz");
    ///
    /// let bearer_auth = Auth::bearer("token123");
    /// assert_eq!(bearer_auth.to_header_value(), "Bearer token123");
    ///
    /// let custom_auth = Auth::custom("ApiKey", "secret");
    /// assert_eq!(custom_auth.to_header_value(), "ApiKey secret");
    /// ```
    pub fn to_header_value(&self) -> String {
        match self {
            Auth::Basic { username, password } => {
                let credentials = format!("{}:{}", username, password);
                let encoded = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    credentials.as_bytes(),
                );
                format!("Basic {}", encoded)
            }
            Auth::Bearer { token } => {
                format!("Bearer {}", token)
            }
            Auth::Custom {
                scheme,
                credentials,
            } => {
                format!("{} {}", scheme, credentials)
            }
        }
    }
}

impl fmt::Display for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Auth::Basic { username, .. } => {
                write!(f, "Basic authentication for user: {}", username)
            }
            Auth::Bearer { .. } => write!(f, "Bearer token authentication"),
            Auth::Custom { scheme, .. } => write!(f, "Custom {} authentication", scheme),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_auth() {
        let auth = Auth::basic("user", "pass");
        assert_eq!(auth.to_header_value(), "Basic dXNlcjpwYXNz");
    }

    #[test]
    fn test_bearer_auth() {
        let auth = Auth::bearer("token123");
        assert_eq!(auth.to_header_value(), "Bearer token123");
    }

    #[test]
    fn test_custom_auth() {
        let auth = Auth::custom("ApiKey", "secret123");
        assert_eq!(auth.to_header_value(), "ApiKey secret123");
    }

    #[test]
    fn invalid_credentials_are_errors_not_panics() {
        for auth in [
            Auth::custom("x", "a\r\nb"),
            Auth::bearer("token\nInjected: 1"),
            Auth::custom("two words", "ok"),
            Auth::custom("", "ok"),
            Auth::custom("x\r\n", "ok"),
        ] {
            assert!(
                matches!(auth.header_value(), Err(crate::Error::InvalidHeader)),
                "{}",
                auth
            );
        }
        assert_eq!(
            Auth::basic("user", "pa\nss").header_value().unwrap(),
            "Basic dXNlcjpwYQpzcw=="
        );
        assert!(
            Auth::bearer("token123")
                .header_value()
                .unwrap()
                .is_sensitive()
        );
    }

    #[test]
    fn debug_does_not_show_secrets() {
        for auth in [
            Auth::basic("alice", "hunter2"),
            Auth::bearer("hunter2"),
            Auth::custom("ApiKey", "hunter2"),
        ] {
            let debug = format!("{:?}", auth);
            assert!(!debug.contains("hunter2"), "{}", debug);
        }
        assert!(format!("{:?}", Auth::basic("alice", "hunter2")).contains("alice"));
    }
}
