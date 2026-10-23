//! Resident service scheduler. It owns no second database handle; each cycle
//! records durable status through the `Store` already held open by `serve`.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::storage::{StorageError, Store};

pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(30 * 60);
const ARCHIVE_CLEANUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const ARCHIVE_CLEANUP_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const STATUS_KEY: &str = "service/polar-sync";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerStatus {
    pub interval_seconds: u64,
    pub last_started_unix_seconds: Option<i64>,
    pub last_finished_unix_seconds: Option<i64>,
    pub next_run_unix_seconds: i64,
    pub state: String,
    pub detail: String,
    pub requests: u64,
    pub archived: u64,
    pub imported: u64,
    pub no_data: u64,
}

/// Run the service timer until shutdown. Tokio's `Skip` behavior avoids
/// replaying missed intervals after a pause; the next cycle starts on the next
/// scheduled boundary. Each cycle uses the open Store, archives responses
/// before replaying them, and persists state for local API clients.
pub async fn run(
    store: Arc<Store>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    sync_lock: Arc<tokio::sync::Mutex<()>>,
) -> Result<(), StorageError> {
    let seconds = interval.as_secs();
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Run one cleanup shortly after startup, then at most once per day. The
    // seven-day age threshold leaves a wide recovery window for an interrupted
    // publication or a sidecar that has not yet been reconciled.
    let mut next_archive_cleanup = 0_i64;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = timer.tick() => {
                let started = unix_seconds();
                let data_dir = store.database_path().parent().unwrap_or_else(|| std::path::Path::new(".")).to_path_buf();
                let _sync_guard = sync_lock.lock().await;
                // Persist the start before doing network or archive work. If the
                // process or Pi loses power mid-cycle, startup can report an
                // interrupted run instead of leaving an old success looking live.
                let previous = load_status(&store)?;
                let running = SchedulerStatus {
                    interval_seconds: seconds,
                    last_started_unix_seconds: Some(started),
                    last_finished_unix_seconds: previous.as_ref().and_then(|s| s.last_finished_unix_seconds),
                    next_run_unix_seconds: started.saturating_add(i64::try_from(seconds).unwrap_or(i64::MAX)),
                    state: "running".to_owned(),
                    detail: "scheduled Polar sync cycle is in progress".to_owned(),
                    requests: previous.as_ref().map_or(0, |s| s.requests),
                    archived: previous.as_ref().map_or(0, |s| s.archived),
                    imported: previous.as_ref().map_or(0, |s| s.imported),
                    no_data: previous.as_ref().map_or(0, |s| s.no_data),
                };
                persist_status(&store, running).await?;
                eprintln!("scheduled sync started interval_seconds={seconds}");
                let cycle = tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                        continue;
                    }
                    cycle = crate::polar::sync::run_cycle(store.clone(), &data_dir) => cycle,
                };
                let finished = unix_seconds();
                if started >= next_archive_cleanup {
                    match cleanup_stale_archive_files(&store).await {
                        Ok(removed) => {
                            if removed.removed_parts != 0 || removed.removed_objects != 0 {
                                eprintln!(
                                    "archive cleanup removed {} stale parts and {} orphan objects",
                                    removed.removed_parts, removed.removed_objects
                                );
                            }
                        }
                        Err(error) => eprintln!("automatic archive cleanup failed: {error}"),
                    }
                    next_archive_cleanup = finished.saturating_add(
                        i64::try_from(ARCHIVE_CLEANUP_INTERVAL.as_secs()).unwrap_or(i64::MAX),
                    );
                }
                let (state, detail, requests, archived, imported, no_data) = match cycle {
                    Ok(summary) => (summary.state, summary.detail, summary.requests, summary.archived, summary.imported, summary.no_data),
                    Err(crate::polar::sync::SyncError::NeedsAuth) => ("needs_auth".to_owned(), "Polar credentials are not present or the access token was rejected".to_owned(), 0, 0, 0, 0),
                    Err(crate::polar::sync::SyncError::Forbidden(resource)) => ("permission_error".to_owned(), format!("Polar denied access to {resource}; check the AccessLink permissions and device capability"), 0, 0, 0, 0),
                    Err(crate::polar::sync::SyncError::Transient(detail)) => ("transient_error".to_owned(), detail, 0, 0, 0, 0),
                    Err(error) => ("unknown".to_owned(), error.to_string(), 0, 0, 0, 0),
                };
                let status = SchedulerStatus {
                    interval_seconds: seconds,
                    last_started_unix_seconds: Some(started),
                    last_finished_unix_seconds: Some(finished),
                    next_run_unix_seconds: finished.saturating_add(i64::try_from(seconds).unwrap_or(i64::MAX)),
                    state,
                    detail,
                    requests,
                    archived,
                    imported,
                    no_data,
                };
                persist_status(&store, status.clone()).await?;
                eprintln!(
                    "scheduled sync finished state={} requests={} archived={} imported={} no_data={} duration_seconds={}",
                    status.state,
                    status.requests,
                    status.archived,
                    status.imported,
                    status.no_data,
                    finished.saturating_sub(started),
                );
            }
        }
    }
}

async fn cleanup_stale_archive_files(
    store: &Store,
) -> Result<crate::archive::CleanupResult, crate::archive::ArchiveError> {
    let archive = crate::archive::ArchiveStore::new(store.archive_path().to_path_buf());
    tokio::task::spawn_blocking(move || {
        let plan = archive.preview_cleanup(ARCHIVE_CLEANUP_MIN_AGE)?;
        archive.apply_cleanup(&plan)
    })
    .await
    .map_err(|error| crate::archive::ArchiveError::Io(std::io::Error::other(error)))?
}

/// Store initial status on daemon startup so local clients can distinguish a
/// running but unauthenticated service from a stale status left by a crash.
pub fn initialize(store: &Store, interval: Duration) -> Result<(), StorageError> {
    let now = unix_seconds();
    let seconds = interval.as_secs();
    let data_dir = store
        .database_path()
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let authorized = crate::polar::credentials::PolarCredentials::load(data_dir)
        .ok()
        .flatten()
        .is_some();
    let previous = load_status(store)?;
    let (state, detail) = match previous.as_ref() {
        Some(status) if status.state == "running" => (
            "interrupted".to_owned(),
            format!(
                "previous scheduled sync was interrupted after starting at Unix time {}; inspect archived receipts and retry status",
                status.last_started_unix_seconds.unwrap_or(0)
            ),
        ),
        Some(status) => (status.state.clone(), status.detail.clone()),
        None if authorized => (
            "unknown".to_owned(),
            "Polar credentials are available; the first scheduled cycle has not run".to_owned(),
        ),
        None => (
            "needs_auth".to_owned(),
            "service is running; authorize Polar with `rust-polar auth polar`".to_owned(),
        ),
    };
    let status = SchedulerStatus {
        interval_seconds: seconds,
        last_started_unix_seconds: previous.as_ref().and_then(|s| s.last_started_unix_seconds),
        last_finished_unix_seconds: previous.as_ref().and_then(|s| s.last_finished_unix_seconds),
        next_run_unix_seconds: now.saturating_add(i64::try_from(seconds).unwrap_or(i64::MAX)),
        state,
        detail,
        requests: previous.as_ref().map_or(0, |s| s.requests),
        archived: previous.as_ref().map_or(0, |s| s.archived),
        imported: previous.as_ref().map_or(0, |s| s.imported),
        no_data: previous.as_ref().map_or(0, |s| s.no_data),
    };
    store.put_sync_state(STATUS_KEY, &status)
}

fn load_status(store: &Store) -> Result<Option<SchedulerStatus>, StorageError> {
    store
        .get_sync_state(STATUS_KEY)?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(StorageError::from))
        .transpose()
}

async fn persist_status(store: &Arc<Store>, status: SchedulerStatus) -> Result<(), StorageError> {
    let writer = store.clone();
    tokio::task::spawn_blocking(move || writer.put_sync_state(STATUS_KEY, &status))
        .await
        .map_err(|error| StorageError::Io(std::io::Error::other(error)))?
}

/// Reflect an on-demand sync in the same status document used by the service
/// diagnostics endpoint, without moving the scheduler's next timer deadline.
pub async fn record_manual_result(
    store: &Arc<Store>,
    result: crate::polar::sync::SyncStatus,
) -> Result<(), StorageError> {
    let store = store.clone();
    tokio::task::spawn_blocking(move || {
        let now = unix_seconds();
        let previous = load_status(&store)?;
        let interval = previous
            .as_ref()
            .map_or(DEFAULT_SYNC_INTERVAL.as_secs(), |status| {
                status.interval_seconds
            });
        let next_run = previous.as_ref().map_or_else(
            || now.saturating_add(i64::try_from(interval).unwrap_or(i64::MAX)),
            |status| status.next_run_unix_seconds,
        );
        let status = SchedulerStatus {
            interval_seconds: interval,
            last_started_unix_seconds: Some(now),
            last_finished_unix_seconds: Some(now),
            next_run_unix_seconds: next_run,
            state: result.state,
            detail: result.detail,
            requests: result.requests,
            archived: result.archived,
            imported: result.imported,
            no_data: result.no_data,
        };
        store.put_sync_state(STATUS_KEY, &status)
    })
    .await
    .map_err(|error| StorageError::Io(std::io::Error::other(error)))?
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

    struct TestDir(std::path::PathBuf);

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "rust-polar-scheduler-{}-{}-{}",
                std::process::id(),
                unix_seconds(),
                NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn initializes_durable_status_without_secrets() {
        let root =
            std::env::temp_dir().join(format!("rust-polar-scheduler-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root).unwrap();
        let before_initialize = unix_seconds();
        initialize(&store, DEFAULT_SYNC_INTERVAL).unwrap();
        let after_initialize = unix_seconds();
        let bytes = store.get_sync_state(STATUS_KEY).unwrap().unwrap();
        let status: SchedulerStatus = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status.interval_seconds, 1800);
        assert_eq!(status.state, "needs_auth");
        assert_eq!(status.requests, 0);
        assert!(status.last_started_unix_seconds.is_none());
        assert!(status.next_run_unix_seconds >= before_initialize + 1800);
        assert!(status.next_run_unix_seconds <= after_initialize + 1800);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn startup_preserves_last_result_and_marks_an_in_progress_cycle_interrupted() {
        let root = std::env::temp_dir().join(format!(
            "rust-polar-scheduler-restart-{}-{}",
            std::process::id(),
            unix_seconds()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root).unwrap();
        store
            .put_sync_state(
                STATUS_KEY,
                &SchedulerStatus {
                    interval_seconds: 1800,
                    last_started_unix_seconds: Some(123),
                    last_finished_unix_seconds: Some(120),
                    next_run_unix_seconds: 456,
                    state: "running".to_owned(),
                    detail: "scheduled Polar sync cycle is in progress".to_owned(),
                    requests: 17,
                    archived: 16,
                    imported: 12,
                    no_data: 4,
                },
            )
            .unwrap();

        initialize(&store, DEFAULT_SYNC_INTERVAL).unwrap();

        let bytes = store.get_sync_state(STATUS_KEY).unwrap().unwrap();
        let status: SchedulerStatus = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status.state, "interrupted");
        assert!(status.detail.contains("starting at Unix time 123"));
        assert_eq!(status.last_started_unix_seconds, Some(123));
        assert_eq!(status.last_finished_unix_seconds, Some(120));
        assert_eq!(
            (
                status.requests,
                status.archived,
                status.imported,
                status.no_data
            ),
            (17, 16, 12, 4)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn manual_sync_result_updates_diagnostics_status_and_keeps_timer_deadline() {
        let root = TestDir::new();
        let store = Arc::new(Store::open(&root.0).unwrap());
        initialize(&store, DEFAULT_SYNC_INTERVAL).unwrap();
        let before: SchedulerStatus =
            serde_json::from_slice(&store.get_sync_state(STATUS_KEY).unwrap().unwrap()).unwrap();

        record_manual_result(
            &store,
            crate::polar::sync::SyncStatus {
                started_at: "2026-10-21T20:00:00Z".into(),
                finished_at: "2026-10-21T20:00:10Z".into(),
                state: "partial".into(),
                detail: "Polar cycle completed with one unavailable resource".into(),
                requests: 8,
                archived: 8,
                imported: 5,
                no_data: 2,
                last_successful_day: Some("2026-10-21".into()),
            },
        )
        .await
        .unwrap();

        let after: SchedulerStatus =
            serde_json::from_slice(&store.get_sync_state(STATUS_KEY).unwrap().unwrap()).unwrap();
        assert_eq!(after.state, "partial");
        assert_eq!(
            after.detail,
            "Polar cycle completed with one unavailable resource"
        );
        assert_eq!(
            (
                after.requests,
                after.archived,
                after.imported,
                after.no_data
            ),
            (8, 8, 5, 2)
        );
        assert_eq!(after.next_run_unix_seconds, before.next_run_unix_seconds);
        assert!(after.last_started_unix_seconds.is_some());
        assert!(after.last_finished_unix_seconds.is_some());
    }

    #[tokio::test]
    async fn automatic_archive_cleanup_removes_only_candidates_older_than_seven_days() {
        let temp = TestDir::new();
        let store = Store::open(&temp.0).unwrap();
        let archive_dir = store.archive_path();
        std::fs::create_dir_all(archive_dir.join("tmp")).unwrap();
        let stale = archive_dir.join("tmp/stale.part");
        let recent = archive_dir.join("tmp/recent.part");
        std::fs::write(&stale, b"old interrupted response").unwrap();
        std::fs::write(&recent, b"recent interrupted response").unwrap();
        let old_time =
            std::time::SystemTime::now() - ARCHIVE_CLEANUP_MIN_AGE - Duration::from_secs(1);
        std::fs::File::options()
            .write(true)
            .open(&stale)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old_time))
            .unwrap();

        let removed = cleanup_stale_archive_files(&store).await.unwrap();
        assert_eq!(removed.removed_parts, 1);
        assert!(!stale.exists());
        assert!(recent.exists());
    }
}
