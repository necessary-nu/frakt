//! Debug WebSocket connection issue

use frakt::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating Foundation client...");
    let client = Client::builder().build()?;

    println!("Attempting WebSocket connection...");

    // Try a simple connection
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.websocket().connect("wss://ws.postman-echo.com/raw"),
    )
    .await
    {
        Ok(Ok(mut websocket)) => {
            println!("✅ WebSocket connected successfully!");

            println!("Sending a test message...");
            match websocket.send(frakt::Message::text("Hello")).await {
                Ok(_) => println!("✅ Message sent successfully!"),
                Err(e) => println!("❌ Failed to send message: {:?}", e),
            }

            println!("Trying to receive a message...");
            match tokio::time::timeout(std::time::Duration::from_secs(5), websocket.receive()).await
            {
                Ok(Ok(msg)) => println!("✅ Received message: {:?}", msg),
                Ok(Err(e)) => println!("❌ Failed to receive message: {:?}", e),
                Err(_) => println!("⏰ Receive timed out"),
            }
        }
        Ok(Err(e)) => {
            println!("❌ WebSocket connection failed: {:?}", e);
        }
        Err(_) => {
            println!("⏰ WebSocket connection timed out after 10 seconds");
        }
    }

    Ok(())
}
