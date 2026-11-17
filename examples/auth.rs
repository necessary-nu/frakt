//! Authentication example
//!
//! This example demonstrates different authentication methods supported by the library
//! using httpbin's authentication endpoints.

use frakt::{Auth, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting authentication example...");

    let client = Client::new()?;

    // Test 1: Basic Authentication
    println!("\n=== Testing Basic Authentication ===");
    let basic_auth = Auth::basic("testuser", "testpass");
    println!("Using: {}", basic_auth);

    let response = client
        .get("https://httpbin.org/basic-auth/testuser/testpass")?
        .auth(basic_auth)?
        .send()
        .await?;

    println!("Basic auth response status: {}", response.status());
    if response.status() == 200 {
        println!("✅ Basic authentication successful!");
        if let Ok(json) = response.json::<serde_json::Value>().await {
            println!("Response: {}", json);
        }
    } else {
        println!("❌ Basic authentication failed");
    }

    // Test 2: Bearer Token Authentication
    println!("\n=== Testing Bearer Token Authentication ===");
    let bearer_auth = Auth::bearer("my-secret-token-123");
    println!("Using: {}", bearer_auth);

    let response = client
        .get("https://httpbin.org/bearer")?
        .auth(bearer_auth)?
        .send()
        .await?;

    println!("Bearer token response status: {}", response.status());
    if response.status() == 200 {
        println!("✅ Bearer token authentication successful!");
        if let Ok(json) = response.json::<serde_json::Value>().await {
            println!("Response: {}", json);
        }
    } else {
        println!("❌ Bearer token authentication failed");
    }

    // Test 3: Custom Authentication
    println!("\n=== Testing Custom Authentication ===");
    let custom_auth = Auth::custom("ApiKey", "secret-api-key-456");
    println!("Using: {}", custom_auth);

    let response = client
        .get("https://httpbin.org/headers")?
        .auth(custom_auth)?
        .send()
        .await?;

    println!("Custom auth response status: {}", response.status());
    if let Ok(json) = response.json::<serde_json::Value>().await {
        println!("Headers sent: {}", json.get("headers").unwrap());
        if let Some(auth_header) = json.get("headers").and_then(|h| h.get("Authorization")) {
            println!("✅ Custom authentication header sent: {}", auth_header);
        }
    }

    // Test 4: Upload with Authentication
    println!("\n=== Testing Upload with Authentication ===");
    let upload_data = b"Secret data that requires authentication";

    let response = client
        .upload("https://httpbin.org/post")?
        .auth(Auth::bearer("upload-token-789"))?
        .from_data(upload_data.to_vec())
        .header("Content-Type", "text/plain")?
        .send()
        .await?;

    println!("Authenticated upload status: {}", response.status());
    if let Ok(json) = response.json::<serde_json::Value>().await {
        if let Some(headers) = json.get("headers") {
            println!("Upload headers: {}", headers);
        }
        if let Some(data) = json.get("data") {
            println!("Upload data received: {}", data);
        }
    }

    // Test 5: Download with Authentication (using basic auth endpoint that returns data)
    println!("\n=== Testing Download with Authentication ===");
    let response = client
        .get("https://httpbin.org/basic-auth/download-user/download-pass")?
        .auth(Auth::basic("download-user", "download-pass"))?
        .send()
        .await?;

    println!("Authenticated download status: {}", response.status());
    if response.status() == 200 {
        let body = response.text().await?;
        println!(
            "✅ Authenticated download successful! Downloaded {} bytes",
            body.len()
        );
        println!("Response body: {}", body);
    } else {
        println!("❌ Authenticated download failed");
    }

    println!("\n🎉 Authentication example completed!");

    Ok(())
}
