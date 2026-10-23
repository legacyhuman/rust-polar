use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_polar::archive::{ArchiveStore, RequestMetadata};
use rust_polar::storage::Store;

struct TestDir(PathBuf);
static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);
impl TestDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rust-polar-replay-{}-{nonce}-{}",
            std::process::id(),
            NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn archive_replay_is_idempotent_across_cli_restarts() {
    let temp = TestDir::new();
    let store = Store::open(&temp.0).unwrap();
    let archive = ArchiveStore::new(store.archive_path().to_path_buf());
    let fixture = include_bytes!("fixtures/polar/daily_activity.json");
    archive
        .store_body(
            "polar",
            "activities",
            RequestMetadata::new(),
            200,
            "2026-10-21T12:00:00Z",
            Cursor::new(fixture.as_slice()),
        )
        .unwrap();
    let large_detail = "x".repeat(5_000);
    let backfill = (1..=120)
        .map(|index| {
            let month = (index - 1) / 28 + 1;
            let day = (index - 1) % 28 + 1;
            serde_json::json!({
                "start-time": format!("2026-{month:02}-{day:02}T00:00:00"),
                "steps": day,
                "future-detail": large_detail,
            })
        })
        .collect::<Vec<_>>();
    archive
        .store_body(
            "polar",
            "activities",
            RequestMetadata::new(),
            200,
            "2026-10-21T12:01:00Z",
            Cursor::new(serde_json::to_vec(&backfill).unwrap()),
        )
        .unwrap();
    for (resource, fixture) in [
        (
            "physical-info",
            include_bytes!("fixtures/polar/physical_info.json").as_slice(),
        ),
        (
            "user-profile",
            include_bytes!("fixtures/polar/user_profile.json").as_slice(),
        ),
        (
            "continuous-heart-rate",
            include_bytes!("fixtures/polar/continuous_heart_rate.json").as_slice(),
        ),
        (
            "cardio-load",
            include_bytes!("fixtures/polar/cardio_load.json").as_slice(),
        ),
        (
            "exercises",
            include_bytes!("fixtures/polar/exercises.json").as_slice(),
        ),
        (
            "sleep",
            include_bytes!("fixtures/polar/recovery_sleep.json").as_slice(),
        ),
        (
            "nightly-recharge",
            include_bytes!("fixtures/polar/recovery_nightly_recharge.json").as_slice(),
        ),
        (
            "sleepwise-alertness",
            include_bytes!("fixtures/polar/recovery_sleepwise.json").as_slice(),
        ),
        (
            "sleepwise-circadian-bedtime",
            include_bytes!("fixtures/polar/circadian_bedtime.json").as_slice(),
        ),
    ] {
        archive
            .store_body(
                "polar",
                resource,
                RequestMetadata::new(),
                200,
                "2026-10-21T12:00:00Z",
                Cursor::new(fixture),
            )
            .unwrap();
    }
    for resource in ["exercise-fit", "exercise-tcx", "exercise-gpx"] {
        let mut metadata = RequestMetadata::new();
        metadata.insert("exercise_id".into(), "2AC312F".into());
        archive
            .store_body(
                "polar",
                resource,
                metadata,
                200,
                "2026-10-21T12:02:00Z",
                Cursor::new([0, 1, 2, 255]),
            )
            .unwrap();
    }
    drop(store);

    let binary = env!("CARGO_BIN_EXE_rust-polar");
    for _ in 0..2 {
        let output = Command::new(binary)
            .args(["--data-dir", temp.0.to_str().unwrap(), "archive", "replay"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output = Command::new(binary)
        .args([
            "--data-dir",
            temp.0.to_str().unwrap(),
            "query",
            "--limit",
            "200",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let lines = String::from_utf8(output.stdout).unwrap();
    let rows = lines
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let store = Store::open(&temp.0).unwrap();
    assert!(
        store.measurement_count().unwrap() > 0,
        "timestamp-resolved exercise metrics should enter the series index"
    );
    drop(store);
    assert_eq!(
        rows.len(),
        138,
        "keys: {:?}",
        rows.iter().map(|r| &r["key"]).collect::<Vec<_>>()
    );
    let exercise = rows
        .iter()
        .find(|row| row["key"] == "polar/exercises/2AC312F")
        .unwrap();
    assert_eq!(exercise["record"]["record"]["sport"], "OTHER");
    assert_eq!(
        exercise["record"]["record"]["samples"][1]["sample-type"],
        "FUTURE_SAMPLE_TYPE"
    );
    for format in ["fit", "tcx", "gpx"] {
        let export = rows
            .iter()
            .find(|row| row["key"] == format!("polar/exercise-{format}/2AC312F"))
            .unwrap();
        assert_eq!(export["record"]["record"]["format"], format);
        assert_eq!(export["record"]["record"]["exercise_id"], "2AC312F");
        assert!(
            export["record"]["record"]["archive_ref"]
                .as_str()
                .is_some_and(|reference| {
                    reference.len() == 64 && reference.bytes().all(|byte| byte.is_ascii_hexdigit())
                }),
            "export index: {export}"
        );
    }
    let activity = rows
        .iter()
        .find(|row| row["key"] == "polar/activities/2026-10-19")
        .unwrap();
    assert_eq!(activity["record"]["record"]["steps"], 8823);
    assert!(
        activity["record"]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["metric"] == "steps")
    );
    assert_eq!(
        activity["record"]["record"]["future-status"],
        "new-polar-code"
    );
    let sleep = rows
        .iter()
        .find(|row| row["key"] == "polar/sleep/2026-10-20")
        .unwrap();
    assert_eq!(
        sleep["record"]["record"]["hypnogram"]["2026-10-20T06:58:00"],
        "FUTURE_STAGE"
    );
    let recharge = rows
        .iter()
        .find(|row| row["key"] == "polar/nightly-recharge/2026-10-20")
        .unwrap();
    assert!(
        recharge["record"]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["metric"] == "hrv" && m["unit"] == "Milliseconds")
    );
    let sleepwise = rows
        .iter()
        .find(|row| row["key"] == "polar/sleepwise-alertness/2026-10-20")
        .unwrap();
    assert_eq!(
        sleepwise["record"]["record"]["grade_classification"],
        "FUTURE_GRADE"
    );
    let bedtime = rows
        .iter()
        .find(|row| row["key"] == "polar/sleepwise-circadian-bedtime/2022-11-20")
        .unwrap();
    assert_eq!(
        bedtime["record"]["record"]["sleep_timezone_offset_minutes"],
        120
    );
    let export = Command::new(binary)
        .args([
            "--data-dir",
            temp.0.to_str().unwrap(),
            "export",
            "--user-id",
            "polar-local",
            "--source",
            "polar",
            "--metric",
            "calories",
            "--from",
            "2008-10-13T07:00:00Z",
            "--to",
            "2008-10-13T14:00:00Z",
            "--format",
            "csv",
        ])
        .output()
        .unwrap();
    assert!(
        export.status.success(),
        "{}",
        String::from_utf8_lossy(&export.stderr)
    );
    let csv = String::from_utf8(export.stdout).unwrap();
    assert!(csv.starts_with("id,user_id,source,metric,value,unit,"));
    assert!(
        csv.lines().any(|line| line.contains(",calories,")),
        "export output: {csv}"
    );
    let hr = rows
        .iter()
        .find(|row| row["key"] == "polar/continuous-heart-rate/2026-10-20")
        .unwrap();
    assert_eq!(hr["record"]["metrics"][0]["sample_time"], "00:02:08");
    let cardio = rows
        .iter()
        .find(|row| row["key"] == "polar/cardio-load/2023-01-01")
        .unwrap();
    assert_eq!(
        cardio["record"]["record"]["cardio_load_status"],
        "LOAD_STATUS_NOT_AVAILABLE"
    );
    assert_eq!(cardio["record"]["metrics"][0]["value"], "0.2");
    let physical = rows
        .iter()
        .find(|row| row["key"] == "polar/physical-info/self")
        .unwrap();
    assert_eq!(physical["record"]["record"]["vo2_max"], 50);
    assert_eq!(physical["record"]["record"]["future-setting"], "kept");
    let profile = rows
        .iter()
        .find(|row| row["key"] == "polar/user-profile/self")
        .unwrap();
    assert_eq!(profile["record"]["record"]["first-name"], "Example");
    assert_eq!(profile["record"]["record"]["future-field"], "retained");
}

#[test]
fn corrected_activity_replay_removes_samples_omitted_by_the_revision() {
    let temp = TestDir::new();
    let store = Store::open(&temp.0).unwrap();
    let archive = ArchiveStore::new(store.archive_path().to_path_buf());
    let original = br#"[{"start_time":"2026-10-21","steps":3,"samples":{"date":"2026-10-21","steps":{"samples":[{"steps":1,"timestamp":"2026-10-21T02:30:00+02:00"},{"steps":2,"timestamp":"2026-10-21T02:35:00+02:00"}]}}}]"#;
    let first = archive
        .store_body(
            "polar",
            "activities",
            RequestMetadata::new(),
            200,
            "2026-10-21T03:00:00Z",
            Cursor::new(original.as_slice()),
        )
        .unwrap();
    rust_polar::ingest::replay_archive(
        &store,
        Some("activities"),
        "test-user",
        Some(&std::collections::HashSet::from([first.receipt_id])),
    )
    .unwrap();
    assert_eq!(store.measurement_count().unwrap(), 2); // two timestamped samples

    let corrected = br#"[{"start_time":"2026-10-21","steps":3,"samples":{"date":"2026-10-21","steps":{"samples":[{"steps":2,"timestamp":"2026-10-21T02:35:00+02:00"}]}}}]"#;
    let second = archive
        .store_body(
            "polar",
            "activities",
            RequestMetadata::new(),
            200,
            "2026-10-21T04:00:00Z",
            Cursor::new(corrected.as_slice()),
        )
        .unwrap();
    rust_polar::ingest::replay_archive(
        &store,
        Some("activities"),
        "test-user",
        Some(&std::collections::HashSet::from([second
            .receipt_id
            .clone()])),
    )
    .unwrap();
    assert_eq!(store.measurement_count().unwrap(), 1); // the removed sample is no longer indexed
    assert_eq!(
        rust_polar::ingest::replay_archive(
            &store,
            Some("activities"),
            "test-user",
            Some(&std::collections::HashSet::from([second
                .receipt_id
                .clone()])),
        )
        .unwrap(),
        1
    );
    assert_eq!(store.measurement_count().unwrap(), 1);
}
