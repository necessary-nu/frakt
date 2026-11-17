use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // Only compile Java classes when building for Android
    let target = env::var("TARGET").unwrap_or_default();
    if !target.contains("android") {
        return;
    }

    println!("cargo:rerun-if-changed=java/");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let java_dir = Path::new("java");

    // Find javac
    let javac = find_javac().expect("javac not found. Please install JDK and set JAVA_HOME");

    // OUT_DIR outlives a build, and every class found goes into the DEX, so
    // the classes get a directory that starts empty: one whose source was
    // renamed or deleted must not be packaged again.
    let class_dir = out_dir.join("classes");
    let _ = std::fs::remove_dir_all(&class_dir);
    std::fs::create_dir_all(&class_dir).expect("Failed to create the class output directory");

    // Compile Java sources
    let sources = vec![
        // Cronet stubs
        java_dir.join("stubs/org/chromium/net/UrlRequest.java"),
        java_dir.join("stubs/org/chromium/net/UrlResponseInfo.java"),
        java_dir.join("stubs/org/chromium/net/CronetException.java"),
        java_dir.join("stubs/org/chromium/net/UploadDataProvider.java"),
        java_dir.join("stubs/org/chromium/net/UploadDataSink.java"),
        // Java NIO stubs
        java_dir.join("stubs/java/nio/Buffer.java"),
        java_dir.join("stubs/java/nio/ByteBuffer.java"),
        // Our callback implementation
        java_dir.join("se/brendan/frakt/RustUrlRequestCallback.java"),
        // Request bodies, read from Rust
        java_dir.join("se/brendan/frakt/RustUploadDataProvider.java"),
    ];

    let status = Command::new(&javac)
        .arg("-d")
        .arg(&class_dir)
        .arg("-source")
        .arg("8")
        .arg("-target")
        .arg("8")
        .arg("-sourcepath")
        .arg(java_dir.join("stubs"))
        .arg("-sourcepath")
        .arg(java_dir)
        .args(&sources)
        .status()
        .expect("Failed to run javac");

    if !status.success() {
        panic!("javac failed to compile Java sources");
    }

    println!(
        "cargo:warning=Compiled Java callback class to {}",
        class_dir.display()
    );

    // Convert .class files to .dex format for Android
    convert_to_dex(&class_dir, &out_dir);
}

fn convert_to_dex(class_dir: &Path, dex_dir: &Path) {
    // Find d8 tool from Android SDK
    let d8 = find_d8().expect(
        "d8 not found. Please install Android SDK and set ANDROID_HOME or ANDROID_SDK_ROOT",
    );

    let dex_output = dex_dir.join("classes.dex");

    // Collect all .class files recursively
    let mut class_files = Vec::new();
    fn collect_class_files(dir: &Path, files: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    collect_class_files(&path, files);
                } else if path.extension().and_then(|s| s.to_str()) == Some("class") {
                    files.push(path);
                }
            }
        }
    }
    collect_class_files(class_dir, &mut class_files);

    // Run d8 to convert all class files to DEX
    let mut d8_command = Command::new(&d8);
    d8_command
        .arg("--output")
        .arg(dex_dir)
        .arg("--min-api")
        .arg("21"); // Minimum API level

    for class_file in &class_files {
        d8_command.arg(class_file);
    }

    let status = d8_command.status().expect("Failed to run d8");

    if !status.success() {
        panic!("d8 failed to convert class files to DEX");
    }

    println!("cargo:warning=Converted to DEX: {}", dex_output.display());
}

/// d8 from the newest build-tools of the SDK that `ANDROID_HOME` or, failing
/// that, `ANDROID_SDK_ROOT` names.
fn find_d8() -> Option<PathBuf> {
    ["ANDROID_HOME", "ANDROID_SDK_ROOT"]
        .into_iter()
        .filter_map(|variable| env::var(variable).ok())
        .find_map(|sdk| {
            let mut versions: Vec<_> = std::fs::read_dir(PathBuf::from(sdk).join("build-tools"))
                .ok()?
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect();
            versions.sort();
            let d8 = versions.last()?.join("d8");
            d8.exists().then_some(d8)
        })
}

fn find_javac() -> Option<PathBuf> {
    // Try JAVA_HOME first
    if let Ok(java_home) = env::var("JAVA_HOME") {
        let javac = PathBuf::from(java_home).join("bin").join("javac");
        if javac.exists() {
            return Some(javac);
        }
    }

    // Try PATH
    if let Ok(output) = Command::new("which").arg("javac").output()
        && output.status.success()
    {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }

    // Try common locations on macOS
    let macos_java = Path::new("/usr/libexec/java_home");
    if macos_java.exists()
        && let Ok(output) = Command::new(macos_java).output()
        && output.status.success()
    {
        let java_home = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let javac = PathBuf::from(java_home).join("bin").join("javac");
        if javac.exists() {
            return Some(javac);
        }
    }

    None
}
