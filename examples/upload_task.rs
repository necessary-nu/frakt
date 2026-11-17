//! Upload task example
//!
//! This example demonstrates how to use NSURLSessionUploadTask for efficient file uploads
//! with progress tracking.

use frakt::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting upload task example...");

    // Create some test data to upload
    let test_data = b"Hello from NSURLSessionUploadTask! This is test data for upload.".repeat(100);

    // Write test data to a temporary file
    let mut temp_file = std::env::temp_dir();
    temp_file.push("frakt_upload_test.txt");

    std::fs::write(&temp_file, &test_data)?;
    println!(
        "Created test file: {:?} ({} bytes)",
        temp_file,
        test_data.len()
    );

    // Create a client
    let client = Client::new()?;

    // Test 1: Upload from file with progress tracking
    println!("\n=== Testing file upload with progress ===");
    let response = client
        .upload("https://httpbin.org/post")?
        .from_file(&temp_file)
        .header("Content-Type", "text/plain")?
        .progress(|bytes_uploaded, total_bytes| {
            if let Some(total) = total_bytes {
                let percentage = (bytes_uploaded as f64 / total as f64) * 100.0;
                println!(
                    "Upload progress: {:.1}% ({} / {} bytes)",
                    percentage, bytes_uploaded, total
                );
            } else {
                println!("Upload progress: {} bytes", bytes_uploaded);
            }
        })
        .send()
        .await?;

    println!("✅ File upload completed!");
    println!("Status: {}", response.status());

    // Parse the response JSON to see what httpbin received
    if let Ok(json_response) = response.json::<serde_json::Value>().await {
        if let Some(files) = json_response.get("files") {
            println!("Files received by server: {}", files);
        }
        if let Some(headers) = json_response.get("headers") {
            println!("Headers: {}", headers);
        }
    }

    // Test 2: Upload from data (in-memory)
    println!("\n=== Testing data upload ===");
    let small_data = b"Small data upload test";

    let response2 = client
        .upload("https://httpbin.org/post")?
        .from_data(small_data.to_vec())
        .header("Content-Type", "application/octet-stream")?
        .progress(|bytes_uploaded, total_bytes| {
            if let Some(total) = total_bytes {
                println!("Data upload: {} / {} bytes", bytes_uploaded, total);
            }
        })
        .send()
        .await?;

    println!("✅ Data upload completed!");
    println!("Status: {}", response2.status());

    if let Ok(json_response) = response2.json::<serde_json::Value>().await
        && let Some(data) = json_response.get("data")
    {
        println!("Data received by server: {}", data);
    }

    // Clean up temporary file
    let _ = std::fs::remove_file(&temp_file);
    println!("\n🧹 Cleaned up temporary file");

    Ok(())
}
