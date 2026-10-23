//! Time a large streamed download per backend against the transfer suite's
//! local server.
//!
//! ```text
//! cargo run --release --example bench_download --features backend-foundation -- [MiB] [runs] [MiB/s]
//! ```
//!
//! The server runs in a child process so that the CPU time reported is the
//! client's alone.

#[path = "../tests/support/server.rs"]
mod server;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use frakt::{BackendType, Client};

#[cfg(unix)]
fn cpu_time() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let to_duration = |tv: libc::timeval| {
        Duration::from_secs(tv.tv_sec as u64) + Duration::from_micros(tv.tv_usec as u64)
    };
    to_duration(usage.ru_utime) + to_duration(usage.ru_stime)
}

#[cfg(not(unix))]
fn cpu_time() -> Duration {
    Duration::ZERO
}

fn backends() -> Vec<BackendType> {
    #[allow(unused_mut)]
    let mut backends = Vec::new();
    #[cfg(all(feature = "backend-foundation", target_vendor = "apple"))]
    backends.push(BackendType::Foundation);
    #[cfg(all(feature = "backend-winhttp", windows))]
    backends.push(BackendType::Windows);
    #[cfg(feature = "backend-reqwest")]
    backends.push(BackendType::Reqwest);
    backends
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("serve") {
        let server = server::Server::start().await;
        println!("{}", server.url(""));
        std::future::pending::<()>().await;
        return;
    }

    let mib: u64 = args.first().and_then(|a| a.parse().ok()).unwrap_or(100);
    let runs: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(3);
    let rate: Option<u64> = args.get(2).and_then(|a| a.parse().ok());
    let len = mib << 20;

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("serve")
        .stdout(Stdio::piped())
        .spawn()
        .expect("start server");
    let mut base = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut base)
        .unwrap();
    let mut url = format!("{}/bytes?len={}&seed=2&chunk=262144", base.trim(), len);
    if let Some(rate) = rate {
        url.push_str(&format!("&rate={}", rate << 20));
    }

    for backend in backends() {
        let client = Client::builder().backend(backend).build().unwrap();
        for run in 1..=runs {
            let cpu_start = cpu_time();
            let started = Instant::now();
            let response = client.get(url.as_str()).unwrap().send().await.unwrap();
            let mut stream = response.stream();
            let mut total = 0u64;
            while let Some(chunk) = stream.next().await {
                total += chunk.unwrap().len() as u64;
            }
            let wall = started.elapsed();
            let cpu = cpu_time() - cpu_start;
            assert_eq!(total, len);
            println!(
                "{:?} run {}: {} MiB in {:.2} s wall, {:.2} s CPU ({:.0} MiB/s)",
                backend,
                run,
                mib,
                wall.as_secs_f64(),
                cpu.as_secs_f64(),
                mib as f64 / wall.as_secs_f64()
            );
        }
    }

    let _ = child.kill();
}
