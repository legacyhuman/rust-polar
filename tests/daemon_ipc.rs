#![cfg(unix)]

use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = PathBuf::from("/tmp").join(format!("rpdi-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn run_cli(data_dir: &std::path::Path, arguments: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rust-polar"))
        .arg("--data-dir")
        .arg(data_dir)
        .env("RUST_POLAR_SOCKET", data_dir.join("s"))
        .args(arguments)
        .output()
        .unwrap()
}

#[test]
fn status_and_archive_maintenance_cli_use_daemon_while_redb_is_owned() {
    let temp = TestDir::new();
    let mut daemon = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_rust-polar"))
            .arg("--data-dir")
            .arg(&temp.0)
            .env("RUST_POLAR_SOCKET", temp.0.join("s"))
            .args(["serve", "--sync-interval-seconds", "86400"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let socket = temp.0.join("s");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            panic!("daemon exited before binding the socket: {status}");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "daemon socket did not appear"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // The service keeps the writer handle open. A second process cannot open
    // the same redb file; successful CLI commands therefore demonstrate that
    // they reached the resident owner over its Unix socket.
    assert!(rust_polar::storage::Store::open(&temp.0).is_err());

    let status = run_cli(&temp.0, &["status"]);
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(String::from_utf8_lossy(&status.stdout).contains("provider_records"));

    for args in [
        vec!["archive", "verify"],
        vec!["archive", "diagnose", "--older-than-seconds", "1"],
        vec!["archive", "cleanup", "--older-than-seconds", "1"],
    ] {
        let output = run_cli(&temp.0, &args);
        assert!(
            output.status.success(),
            "command {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
