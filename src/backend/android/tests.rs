//! Unit tests for Android backend
//!
//! These tests can run on non-Android platforms by mocking JNI calls

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::backend::types::{BackendRequest, BackendResponse};
    use http::{HeaderMap, Method};
    use url::Url;

    #[test]
    fn test_http_method_conversion() {
        use crate::backend::android::jni_bindings::HttpMethod;

        assert_eq!(HttpMethod::Get.as_str(), "GET");
        assert_eq!(HttpMethod::Post.as_str(), "POST");
        assert_eq!(HttpMethod::Put.as_str(), "PUT");
        assert_eq!(HttpMethod::Delete.as_str(), "DELETE");
        assert_eq!(HttpMethod::Head.as_str(), "HEAD");
        assert_eq!(HttpMethod::Patch.as_str(), "PATCH");
        assert_eq!(HttpMethod::Options.as_str(), "OPTIONS");
    }

    #[test]
    fn test_response_info_creation() {
        // This would test ResponseInfo parsing, but requires JNI mocking
        // For now, just test the utility functions
        use crate::backend::android::response::utils;
        use http::{HeaderMap, HeaderValue, StatusCode};

        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("content-length", HeaderValue::from_static("1024"));
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));

        assert_eq!(
            utils::get_content_type(&headers),
            Some("application/json".to_string())
        );
        assert_eq!(utils::get_content_length(&headers), Some(1024));
        assert!(utils::is_compressed(&headers));
        assert!(utils::is_success_status(StatusCode::OK));
        assert!(!utils::is_success_status(StatusCode::NOT_FOUND));
    }

    // Mock Android backend for testing (non-Android platforms)
    #[cfg(not(target_os = "android"))]
    mod mock_tests {
        use super::*;

        struct MockAndroidBackend;

        impl MockAndroidBackend {
            async fn mock_execute(
                &self,
                request: BackendRequest,
            ) -> Result<BackendResponse, Error> {
                // Mock implementation for testing
                let (sender, receiver) = tokio::sync::mpsc::channel(1);

                // Send mock response data
                let _ = sender.send(Ok(bytes::Bytes::from("mock response"))).await;
                drop(sender);

                Ok(BackendResponse {
                    status: http::StatusCode::OK,
                    headers: HeaderMap::new(),
                    url: url.clone(),
                    body: crate::backend::types::channel_body(receiver),
                    redirect_headers: vec![],
                })
            }
        }

        #[tokio::test]
        async fn test_mock_request_execution() {
            let backend = MockAndroidBackend;

            let request = BackendRequest {
                method: Method::GET,
                url: Url::parse("https://example.com").unwrap(),
                headers: HeaderMap::new(),
                body: None,
                progress_callback: None,
            };

            let response = backend.mock_execute(request).await.unwrap();
            assert_eq!(response.status, http::StatusCode::OK);
        }
    }
}
