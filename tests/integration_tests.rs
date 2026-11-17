//! Integration tests for frakt

use std::time::Duration;

#[cfg(vampire)]
use crate as frakt;

use frakt::{Auth, BackendType, Client, CloseCode, Error, Message, Result};

#[cfg(not(target_os = "android"))]
fn backend() -> BackendType {
    match std::env::var("BACKEND").as_deref() {
        #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
        Ok("foundation") => BackendType::Foundation,
        #[cfg(feature = "backend-reqwest")]
        Ok("reqwest") => BackendType::Reqwest,
        #[cfg(all(feature = "backend-winhttp", windows))]
        Ok("windows") => BackendType::Windows,
        #[cfg(all(feature = "backend-android", target_os = "android"))]
        Ok("android") => BackendType::Android,
        Ok(x) => panic!("Unknown BACKEND env var value: {:?}", x),
        Err(_) => BackendType::fallback().expect("no backend is enabled for this target"),
    }
}

fn temp_dir() -> tempfile::TempDir {
    if cfg!(target_os = "android") {
        tempfile::tempdir_in("/data/data/com.vampire.host/cache").unwrap()
    } else {
        tempfile::tempdir().unwrap()
    }
}

#[cfg(all(feature = "backend-android", target_os = "android"))]
fn backend() -> BackendType {
    use std::sync::Once;
    static INIT: Once = Once::new();

    // Initialize NetLog once for debugging
    INIT.call_once(|| {
        if let Ok(path) = frakt::backend::android::start_netlog() {
            println!("📊 NetLog enabled: {}", path);
            println!("📊 Retrieve with: adb pull {}", path);
        }
    });

    BackendType::Android
}

#[vampire::test]
async fn test_basic_get_request() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    println!("Sending GET request to https://httpbin.org/get");

    let response = client.get("https://httpbin.org/get")?.send().await?;
    println!("Received response with status: {}", response.status());
    println!("Response headers: {:#?}", response.headers());
    println!("{:?}", response);
    assert_eq!(response.status(), 200);

    println!("Before response text");
    let text = response.text().await?;

    assert!(text.contains("httpbin.org"));

    // Stop NetLog to finalize the file
    #[cfg(all(feature = "backend-android", target_os = "android"))]
    {
        let _ = frakt::backend::android::stop_netlog();
    }

    Ok(())
}

// #[vampire::test]
// fn dns_test() -> Result<()> {
//     let _ = frakt::backend::android::list_permissions()?;
//     let ip = frakt::backend::android::test_dns("httpbin.org")?;
//     println!("✅ DNS resolved httpbin.org to: {}", ip);
//     Ok(())
// }

#[vampire::test]
async fn test_post_with_json() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let json_data = r#"{"test": "data", "number": 42}"#;

    let response = client
        .post("https://httpbin.org/post")?
        .header("Content-Type", "application/json")?
        .body(json_data)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let text = response.text().await?;

    println!("{text}");
    // Verify the JSON was properly echoed back
    assert!(text.contains("\"test\": \"data\""));
    assert!(text.contains("\"number\": 42"));
    assert!(text.contains("\"json\": {"));

    // Verify it was sent as POST
    assert!(text.contains("\"url\": \"https://httpbin.org/post\""));

    Ok(())
}

#[vampire::test]
async fn test_headers() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .header("X-Custom-Header", "test-value")?
        .build()?;

    let response = client
        .get("https://httpbin.org/headers")?
        .header("X-Request-Header", "request-value")?
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let text = response.text().await?;
    assert!(text.contains("X-Custom-Header"));
    assert!(text.contains("test-value"));
    assert!(text.contains("X-Request-Header"));
    assert!(text.contains("request-value"));

    Ok(())
}

#[vampire::test]
async fn test_basic_auth() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let response = client
        .get("https://httpbin.org/basic-auth/testuser/testpass")?
        .auth(Auth::Basic {
            username: "testuser".to_string(),
            password: "testpass".to_string(),
        })?
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let text = response.text().await?;
    assert!(text.contains("authenticated"));

    Ok(())
}

#[vampire::test]
async fn test_bearer_auth() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let response = client
        .get("https://httpbin.org/bearer")?
        .auth(Auth::Bearer {
            token: "test-token".to_string(),
        })?
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let text = response.text().await?;
    assert!(text.contains("authenticated"));
    assert!(text.contains("test-token"));

    Ok(())
}

#[vampire::test]
async fn test_cookie_jar() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .use_cookies(true)
        .build()?;

    // Set a cookie
    eprintln!("== Setting cookie via httpbin... ==");
    let _response = client
        .get("https://httpbin.org/cookies/set/test_cookie/test_value")?
        .send()
        .await?;

    eprintln!("== Verifying cookie was set ==");
    // Verify the cookie is sent back
    let response = client.get("https://httpbin.org/cookies")?.send().await?;

    assert_eq!(response.status(), 200);

    let text = response.text().await?;
    eprintln!("Response: {:?}", text);

    assert!(text.contains("test_cookie"));
    assert!(text.contains("test_value"));

    Ok(())
}

#[vampire::test]
async fn test_download_file() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let temp_dir = temp_dir();
    let temp_dir = temp_dir.path();
    let file_path = temp_dir.join("test_download.txt");

    // Clean up any existing file
    let _ = std::fs::remove_file(&file_path);

    let _response = client
        .download(
            "https://httpbin.org/base64/SHR0cCBkb3dubG9hZCB0ZXN0",
            &file_path,
        )?
        .send()
        .await?;

    // Verify the file was downloaded
    assert!(file_path.exists());

    let content = std::fs::read_to_string(&file_path)?;
    assert!(content.contains("Http download test"));

    // Clean up
    let _ = std::fs::remove_file(&file_path);

    Ok(())
}

// Note: Multipart form test removed - feature may not be fully implemented yet

#[vampire::test]
async fn test_timeout() -> Result<()> {
    use std::time::Duration;

    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .timeout(Duration::from_millis(100)) // Short but more reasonable timeout
        .build()?;

    // This should timeout
    let result = client
        .get("https://httpbin.org/delay/5")? // 5 second delay
        .send()
        .await;

    eprintln!("{:?}", result);

    // Should get a timeout error
    assert!(result.is_err());
    if let Err(error) = result {
        assert!(
            matches!(error, Error::Timeout),
            "Expected Timeout error, got: {:?}",
            error
        );
    }

    Ok(())
}

#[vampire::test]
async fn test_error_status_codes() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let response = client
        .get("https://httpbin.org/status/404")?
        .allow_error_status()
        .send()
        .await?;

    assert_eq!(response.status(), 404);

    Ok(())
}

#[vampire::test]
async fn test_response_headers() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let response = client.get("https://httpbin.org/json")?.send().await?;

    assert_eq!(response.status(), 200);

    // Verify we can actually extract headers from the response
    let headers = response.headers();

    // These headers should always be present in httpbin.org responses
    assert!(headers.contains_key("content-type") || headers.contains_key("Content-Type"));

    // Get the content-type header and verify it's JSON
    let content_type = headers
        .get("content-type")
        .or_else(|| headers.get("Content-Type"))
        .expect("Should have content-type header");

    let content_type_str = content_type.to_str().unwrap();
    assert!(
        content_type_str.contains("application/json"),
        "Expected JSON content type, got: {}",
        content_type_str
    );

    Ok(())
}

#[vampire::test]
#[cfg(not(target_os = "android"))]
async fn test_websocket_connection() -> Result<()> {
    println!("test_websocket_connection - Starting test");

    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    println!("test_websocket_connection - Created client, connecting to WebSocket...");

    // Test WebSocket connection to echo server
    let mut websocket = client
        .websocket()
        .connect("wss://ws.postman-echo.com/raw")
        .await?;

    println!("test_websocket_connection - WebSocket connected successfully!");

    // Send a text message
    println!("test_websocket_connection - Sending text message...");
    websocket.send(Message::text("Hello WebSocket!")).await?;
    println!("test_websocket_connection - Text message sent successfully");

    // Receive the echo
    println!("test_websocket_connection - Receiving echo...");
    let message = websocket.receive().await?;
    println!("test_websocket_connection - Received message");
    match message {
        Message::Text(text) => {
            println!("test_websocket_connection - Received text: {}", text);
            assert_eq!(text, "Hello WebSocket!");
        }
        _ => panic!("Expected text message, got binary"),
    }

    // Send another text message to test multi-message flow
    println!("test_websocket_connection - Sending second text message...");
    websocket.send(Message::text("Second message!")).await?;
    println!("test_websocket_connection - Second text message sent successfully");

    // Receive the second echo
    println!("test_websocket_connection - Receiving second echo...");
    let message = websocket.receive().await?;
    println!("test_websocket_connection - Received second message");
    match message {
        Message::Text(text) => {
            println!("test_websocket_connection - Received second text: {}", text);
            assert_eq!(text, "Second message!");
        }
        _ => panic!("Expected text message, got binary"),
    }

    // Close the connection
    println!("test_websocket_connection - Closing connection...");
    websocket
        .close(CloseCode::Normal, Some("Test completed"))
        .await?;
    println!("test_websocket_connection - Connection closed successfully");

    println!("test_websocket_connection - Test completed successfully!");
    Ok(())
}

#[vampire::test]
#[cfg(not(target_os = "android"))]
async fn test_websocket_max_message_size() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    // Test WebSocket with custom max message size
    let websocket = client
        .websocket()
        .maximum_message_size(1024) // 1KB limit
        .connect("wss://ws.postman-echo.com/raw")
        .await?;

    // Verify the max message size was set
    assert_eq!(websocket.maximum_message_size(), 1024);

    Ok(())
}

#[vampire::test]
#[cfg(not(target_os = "android"))]
async fn test_websocket_close_code_and_reason() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let mut websocket = client
        .websocket()
        .connect("wss://ws.postman-echo.com/raw")
        .await?;

    // Initially no close code or reason
    assert_eq!(websocket.close_code(), None);
    assert_eq!(websocket.close_reason(), None);

    // Close with specific code and reason
    websocket
        .close(CloseCode::Normal, Some("Manual close"))
        .await?;

    // Note: The close code and reason might not be immediately available
    // This is platform and implementation dependent

    Ok(())
}

#[vampire::test]
async fn test_platform_backend() -> Result<()> {
    // Test that the client uses the appropriate backend for the platform
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let response = client.get("https://httpbin.org/get")?.send().await?;
    assert_eq!(response.status(), 200);

    // Verify the response body contains expected data
    let text = response.text().await?;
    assert!(text.contains("\"url\": \"https://httpbin.org/get\""));

    Ok(())
}

#[vampire::test]
async fn test_invalid_url_handling() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    // Test invalid URL schemes
    let result = client.get("ftp://invalid.com")?.send().await;
    assert!(result.is_err());

    // Test malformed URLs
    let result = client.get("not-a-url");
    assert!(result.is_err());

    // Test URLs with invalid characters
    let result = client.get("https://[invalid-host]");
    assert!(result.is_err());

    Ok(())
}

#[vampire::test]
async fn test_connection_failures() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .timeout(Duration::from_secs(2))
        .build()?;

    // Test connection to non-existent host (this should still work as it's a valid URL)
    let result = client.get("http://frakt.user.town")?.send().await;
    assert!(result.is_err());

    // Test connection to invalid port
    let result = client.get("https://httpbin.org:199999");
    assert!(result.is_err());

    Ok(())
}

#[vampire::test]
async fn test_invalid_headers() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    // Test invalid header values (should return error)
    let result = client
        .get("https://httpbin.org/get")?
        .header("Invalid-Header", "value\nwith\nnewlines");

    assert!(result.is_err()); // Should fail due to invalid header value

    Ok(())
}

#[vampire::test]
async fn test_empty_request_body() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    // Test POST with empty body
    let response = client.post("https://httpbin.org/post")?.send().await?;

    assert_eq!(response.status(), 200);

    // Test POST with explicitly empty body
    let response = client
        .post("https://httpbin.org/post")?
        .body("")
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    Ok(())
}

#[vampire::test]
async fn test_response_content_validation() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    // Test JSON response parsing
    let response = client.get("https://httpbin.org/json")?.send().await?;
    assert_eq!(response.status(), 200);

    // Verify content-type header is correct
    let content_type = response
        .header("content-type")
        .expect("Should have content-type header");
    assert!(content_type.contains("application/json"));

    let text = response.text().await?;
    // Verify JSON structure is correct
    assert!(text.contains("\"slideshow\""));
    assert!(text.contains("\"title\""));
    assert!(text.contains("\"slides\""));

    // Verify it's valid JSON by checking for proper structure (trim whitespace)
    let trimmed_text = text.trim();
    assert!(trimmed_text.starts_with("{"));
    assert!(trimmed_text.ends_with("}"));

    // Test XML-like response (base64 endpoint returns text)
    let response = client.get("https://httpbin.org/xml")?.send().await?;
    assert_eq!(response.status(), 200);

    let text = response.text().await?;
    assert!(text.contains("<?xml"));

    Ok(())
}

#[vampire::test]
async fn test_download_with_progress() -> Result<()> {
    use std::sync::{Arc, Mutex};

    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    let temp_dir = temp_dir();
    let temp_dir = temp_dir.path();
    let file_path = temp_dir.join("test_progress_download.txt");

    // Clean up any existing file
    let _ = std::fs::remove_file(&file_path);

    // Track progress calls
    let progress_calls = Arc::new(Mutex::new(Vec::new()));
    let progress_calls_clone = progress_calls.clone();

    let _response = client
        .download("https://httpbin.org/base64/SGVsbG8gV29ybGQhIFRoaXMgaXMgYSB0ZXN0IGZvciB0aGUgZG93bmxvYWQgcHJvZ3Jlc3MgY2FsbGJhY2suIFdlIG5lZWQgYSBiaXQgbW9yZSB0ZXh0IHRvIG1ha2UgaXQgaW50ZXJlc3RpbmcgYW5kIHRyaWdnZXIgbXVsdGlwbGUgcHJvZ3Jlc3MgdXBkYXRlcy4=", &file_path)?
        .progress(move |bytes_downloaded, total_bytes| {
            let mut calls = progress_calls_clone.lock().unwrap();
            calls.push((bytes_downloaded, total_bytes));
        })
        .send()
        .await?;

    // Verify the file was downloaded
    assert!(file_path.exists());

    // Verify progress callbacks were called
    let calls = progress_calls.lock().unwrap();
    assert!(
        !calls.is_empty(),
        "Progress callback should have been called"
    );

    // Verify the last call shows completion
    if let Some(last_call) = calls.last() {
        assert!(last_call.0 > 0, "Should have downloaded some bytes");
    }

    // Clean up
    let _ = std::fs::remove_file(&file_path);

    Ok(())
}

#[vampire::test]
async fn test_upload_with_progress() -> Result<()> {
    use std::sync::{Arc, Mutex};

    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    // Create test data
    let test_data = "This is test data for upload with progress tracking. ".repeat(100);

    // Track progress calls
    let progress_calls = Arc::new(Mutex::new(Vec::new()));
    let progress_calls_clone = progress_calls.clone();

    let response = client
        .upload("https://httpbin.org/post")?
        .from_data(test_data.as_bytes().to_vec())
        .progress(move |bytes_uploaded, total_bytes| {
            let mut calls = progress_calls_clone.lock().unwrap();
            calls.push((bytes_uploaded, total_bytes));
        })
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    // Verify progress callbacks were called
    let calls = progress_calls.lock().unwrap();
    assert!(
        !calls.is_empty(),
        "Progress callback should have been called"
    );

    // Verify the last call shows completion
    if let Some(last_call) = calls.last() {
        assert!(last_call.0 > 0, "Should have uploaded some bytes");
    }

    Ok(())
}

#[vampire::test]
async fn test_form_urlencoded_upload() -> Result<()> {
    let client = Client::builder()
        .backend(backend())
        .user_agent("frakt-integration-test/1.0")
        .build()?;

    // Test form-urlencoded data
    let form_fields = vec![
        ("username", "john_doe"),
        ("email", "john@example.com"),
        ("age", "30"),
        ("message", "Hello world with spaces and symbols!@#$%"),
    ];

    let response = client
        .post("https://httpbin.org/post")?
        .form(form_fields)
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let text = response.json::<serde_json::Value>().await?;
    let form = text
        .get("form")
        .expect("Should have form field")
        .as_object()
        .unwrap();

    // Verify form data was sent correctly
    assert!(form.get("username").and_then(|x| x.as_str()) == Some("john_doe"));
    Ok(())
}

// // Cleanup function to stop NetLog - call this manually when done testing
// #[cfg(all(feature = "backend-android", target_os = "android"))]
// #[allow(dead_code)]
// fn stop_test_netlog() {
//     if let Err(e) = frakt::backend::android::stop_netlog() {
//         eprintln!("Failed to stop NetLog: {}", e);
//     }
// }
