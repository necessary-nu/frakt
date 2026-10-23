//! Windows-specific error handling and mapping

use crate::Error;
use windows::Win32::Networking::WinHttp::*;
use windows::core;

/// Map a WinHTTP error code (`ERROR_WINHTTP_*`, as reported in
/// `WINHTTP_ASYNC_RESULT::dwError` or by `GetLastError`) to a library error.
pub fn map_winhttp_error(code: u32) -> Error {
    let message = format!("{} ({})", winhttp_error_name(code), code);
    match code {
        ERROR_WINHTTP_TIMEOUT => Error::Timeout,
        ERROR_WINHTTP_OPERATION_CANCELLED => Error::Cancelled,
        ERROR_WINHTTP_INVALID_URL | ERROR_WINHTTP_UNRECOGNIZED_SCHEME => Error::InvalidUrl,
        ERROR_WINHTTP_SECURE_FAILURE
        | ERROR_WINHTTP_SECURE_FAILURE_PROXY
        | ERROR_WINHTTP_SECURE_CHANNEL_ERROR
        | ERROR_WINHTTP_SECURE_CERT_DATE_INVALID
        | ERROR_WINHTTP_SECURE_CERT_CN_INVALID
        | ERROR_WINHTTP_SECURE_INVALID_CA
        | ERROR_WINHTTP_SECURE_CERT_REV_FAILED
        | ERROR_WINHTTP_SECURE_INVALID_CERT
        | ERROR_WINHTTP_SECURE_CERT_REVOKED
        | ERROR_WINHTTP_SECURE_CERT_WRONG_USAGE
        | ERROR_WINHTTP_CLIENT_AUTH_CERT_NEEDED
        | ERROR_WINHTTP_CLIENT_AUTH_CERT_NEEDED_PROXY
        | ERROR_WINHTTP_CLIENT_CERT_NO_PRIVATE_KEY
        | ERROR_WINHTTP_CLIENT_CERT_NO_ACCESS_PRIVATE_KEY => Error::Tls { message },
        _ => Error::Network {
            code: i64::from(code),
            message,
        },
    }
}

fn winhttp_error_name(code: u32) -> &'static str {
    match code {
        ERROR_WINHTTP_OUT_OF_HANDLES => "out of handles",
        ERROR_WINHTTP_TIMEOUT => "the operation timed out",
        ERROR_WINHTTP_INTERNAL_ERROR => "internal WinHTTP error",
        ERROR_WINHTTP_INVALID_URL => "the URL is invalid",
        ERROR_WINHTTP_UNRECOGNIZED_SCHEME => "the URL scheme is not supported",
        ERROR_WINHTTP_NAME_NOT_RESOLVED => "the server name could not be resolved",
        ERROR_WINHTTP_SHUTDOWN => "WinHTTP is shutting down",
        ERROR_WINHTTP_LOGIN_FAILURE => "login failed",
        ERROR_WINHTTP_OPERATION_CANCELLED => "the operation was cancelled",
        ERROR_WINHTTP_CANNOT_CONNECT => "could not connect to the server",
        ERROR_WINHTTP_CONNECTION_ERROR => "the connection was reset or closed",
        ERROR_WINHTTP_RESEND_REQUEST => "the request must be resent",
        ERROR_WINHTTP_SECURE_CERT_DATE_INVALID => "the server certificate has expired",
        ERROR_WINHTTP_SECURE_CERT_CN_INVALID => "the server certificate name does not match",
        ERROR_WINHTTP_CLIENT_AUTH_CERT_NEEDED => "the server requires a client certificate",
        ERROR_WINHTTP_SECURE_INVALID_CA => "the server certificate is not trusted",
        ERROR_WINHTTP_SECURE_CERT_REV_FAILED => "certificate revocation could not be checked",
        ERROR_WINHTTP_HEADER_NOT_FOUND => "header not found",
        ERROR_WINHTTP_INVALID_SERVER_RESPONSE => "the server response is invalid",
        ERROR_WINHTTP_INVALID_HEADER => "invalid header",
        ERROR_WINHTTP_REDIRECT_FAILED => "the redirect failed",
        ERROR_WINHTTP_SECURE_CHANNEL_ERROR => "TLS channel error",
        ERROR_WINHTTP_BAD_AUTO_PROXY_SCRIPT => "the proxy auto-config script is invalid",
        ERROR_WINHTTP_UNABLE_TO_DOWNLOAD_SCRIPT => {
            "the proxy auto-config script could not be downloaded"
        }
        ERROR_WINHTTP_SECURE_INVALID_CERT => "the server certificate is invalid",
        ERROR_WINHTTP_SECURE_CERT_REVOKED => "the server certificate was revoked",
        ERROR_WINHTTP_SECURE_FAILURE => "TLS failure",
        ERROR_WINHTTP_AUTO_PROXY_SERVICE_ERROR => "proxy auto-detection service error",
        ERROR_WINHTTP_SECURE_CERT_WRONG_USAGE => "the server certificate is not valid for this use",
        ERROR_WINHTTP_AUTODETECTION_FAILED => "proxy auto-detection failed",
        ERROR_WINHTTP_HEADER_COUNT_EXCEEDED => "too many response headers",
        ERROR_WINHTTP_HEADER_SIZE_OVERFLOW => "response headers too large",
        ERROR_WINHTTP_RESPONSE_DRAIN_OVERFLOW => "response drain overflow",
        ERROR_WINHTTP_SECURE_FAILURE_PROXY => "TLS failure with the proxy",
        _ => "WinHTTP error",
    }
}

/// Convert Windows HRESULT errors to library errors
pub fn map_windows_error(error: core::Error) -> Error {
    // Compare unsigned: `HRESULT.0` is an i32, so 0x8007xxxx values are
    // negative and never equal a positive literal once widened.
    let hresult = error.code().0 as u32;

    // HRESULT_FROM_WIN32 puts the Win32 code in the low word.
    if hresult & 0xFFFF_0000 == 0x8007_0000 {
        let code = hresult & 0xFFFF;
        if (12000..13000).contains(&code) {
            return map_winhttp_error(code);
        }
    }

    match hresult {
        // E_ABORT
        0x8000_4004 => Error::Cancelled,
        // HRESULT_FROM_WIN32(ERROR_TIMEOUT)
        0x8007_05B4 => Error::Timeout,
        0x8007_0057 => Error::Internal(format!("Invalid argument: {}", error.message())),
        0x8007_000E => Error::Internal("Out of memory".to_string()),
        0x8007_0005 => Error::Internal("Access denied".to_string()),
        _ => Error::Internal(format!(
            "Windows error 0x{:08X}: {}",
            hresult,
            error.message()
        )),
    }
}

/// Map Windows errors with additional context
pub fn map_windows_error_with_context(error: core::Error, context: &str) -> Error {
    let mut mapped_error = map_windows_error(error);

    match &mut mapped_error {
        Error::Internal(message) | Error::Network { message, .. } | Error::Tls { message } => {
            *message = format!("{}: {}", context, message);
        }
        _ => {}
    }

    mapped_error
}
