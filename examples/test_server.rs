//! Run the transfer suite's server on its own, for tests driven from outside
//! the process (android/bgtest).
//!
//! `test_server serve <id>...` prints the base URL, then a line for every
//! request whose query has `id=<id>`, with its Range and If-Range headers.
//! `test_server hash <seed> <len>` prints the BLAKE3 of the generated body.

#[path = "../tests/support/server.rs"]
mod server;

use std::io::Write;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("hash") => {
            let seed: u64 = args[1].parse().unwrap();
            let len: u64 = args[2].parse().unwrap();
            println!("{}", server::body_hash(seed, 0, len).to_hex());
        }
        Some("serve") => {
            let ids = &args[1..];
            let server = server::Server::start().await;
            println!("{}", server.url(""));
            std::io::stdout().flush().unwrap();
            let mut seen = vec![0; ids.len()];
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                for (i, id) in ids.iter().enumerate() {
                    let requests = server.requests_with_id(id);
                    for request in &requests[seen[i]..] {
                        println!(
                            "REQUEST {} {} range={:?} if-range={:?} accept-encoding={:?}",
                            request.method,
                            request.target,
                            request.header_str("range"),
                            request.header_str("if-range"),
                            request.header_str("accept-encoding"),
                        );
                    }
                    seen[i] = requests.len();
                    std::io::stdout().flush().unwrap();
                }
            }
        }
        _ => eprintln!("usage: test_server serve <id>... | test_server hash <seed> <len>"),
    }
}
