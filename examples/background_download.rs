//! Background download example
//!
//! This example demonstrates how to use background downloads that continue
//! even when the app is suspended. Background downloads require a unique
//! session identifier.

use frakt::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // On the reqwest backend this lets the download carry on in a helper
    // process if this one exits. It has to come before everything else: the
    // helper is this program started again, and returns from this call only
    // when it is not the helper.
    frakt::enable_background_helper();

    println!("Starting background download example...");

    // Create a client
    let client = Client::new()?;

    // Download a 100KB file using httpbin (perfect for testing)
    let url = "https://httpbin.org/bytes/102400"; // 100KB

    // Set up destination path
    let mut download_path = std::env::current_dir()?;
    download_path.push("background_download_test.bin");

    println!("Downloading {} to {:?}", url, download_path);
    println!("This download will continue even if the app is suspended!");

    // Create background download with required session identifier
    let response = client
        .download_background(url, &download_path)?
        .session_identifier("com.example.frakt.background")
        .progress(|bytes_downloaded, total_bytes| {
            if let Some(total) = total_bytes {
                let percentage = (bytes_downloaded as f64 / total as f64) * 100.0;
                println!(
                    "Downloaded: {:.1}% ({} / {} bytes)",
                    percentage, bytes_downloaded, total
                );
            } else {
                println!("Downloaded: {} bytes", bytes_downloaded);
            }
        })
        .send()
        .await?;

    println!("✅ Background download completed!");
    println!("File saved to: {:?}", response.file_path);
    println!("Total bytes downloaded: {}", response.bytes_downloaded);

    // Verify the file exists and has the expected size
    if response.file_path.exists() {
        let file_size = std::fs::metadata(&response.file_path)?.len();
        println!("File size on disk: {} bytes", file_size);

        if file_size == response.bytes_downloaded {
            println!("✅ File size matches download size!");
        } else {
            println!("⚠️  File size mismatch!");
        }
    } else {
        println!("❌ Downloaded file not found!");
    }

    Ok(())
}
