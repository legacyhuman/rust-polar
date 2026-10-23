//! Bounded, archive-first polling for documented Polar AccessLink resources.
use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::{
    Date, Duration as CalendarDuration, OffsetDateTime, format_description::well_known::Iso8601,
};

use crate::archive::{ArchiveReceipt, ArchiveStore, RequestMetadata};
use crate::polar::resources::{self, ResourceRequest};
use crate::storage::{StorageError, Store};

const SYNC_STATUS_KEY: &str = "polar/sync/latest";
const CHECKPOINT_PREFIX: &str = "polar/checkpoint/";
const MAX_PAGES_PER_REQUEST: usize = 500;
const MAX_RECEIPTS_PER_CYCLE: usize = 4096;
const MAX_EXERCISES_PER_PAGE: usize = 1000;
const MAX_SLEEP_AVAILABLE_DATES_PER_CYCLE: usize = 28;
const MAX_SLEEP_AVAILABLE_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncStatus {
    pub started_at: String,
    pub finished_at: String,
    pub state: String,
    pub detail: String,
    pub requests: u64,
    pub archived: u64,
    pub imported: u64,
    pub no_data: u64,
    pub last_successful_day: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseDisposition {
    Import,
    NoData,
    NeedsAuth,
    Forbidden,
    Failed(u16),
}

fn response_disposition(receipt: &ArchiveReceipt) -> ResponseDisposition {
    match receipt.status {
        200..=299 if receipt.uncompressed_bytes > 0 => ResponseDisposition::Import,
        200..=299 | 404 => ResponseDisposition::NoData,
        401 => ResponseDisposition::NeedsAuth,
        403 => ResponseDisposition::Forbidden,
        status => ResponseDisposition::Failed(status),
    }
}

#[derive(Debug)]
pub enum SyncError {
    NeedsAuth,
    Forbidden(String),
    Transient(String),
    Storage(StorageError),
    Other(String),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeedsAuth => f.write_str("Polar returned HTTP 401; reauthorization is required"),
            Self::Forbidden(resource) => write!(
                f,
                "Polar denied access to {resource} (HTTP 403); check the AccessLink permissions and device capability"
            ),
            Self::Transient(detail) => write!(f, "temporary Polar sync failure: {detail}"),
            Self::Storage(error) => write!(f, "local storage failure: {error}"),
            Self::Other(detail) => f.write_str(detail),
        }
    }
}

impl std::error::Error for SyncError {}
impl From<StorageError> for SyncError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

/// One synchronization cycle. It deliberately uses one request at a time;
/// this is below AccessLink's documented per-resource concurrency limit and
/// keeps memory and database pressure predictable on Raspberry Pi.
pub async fn run_cycle(store: Arc<Store>, data_dir: &Path) -> Result<SyncStatus, SyncError> {
    run_cycle_with_range(store, data_dir, None).await
}

/// Run a bounded one-shot cycle for a caller-supplied inclusive provider-local
/// date range. Resource-specific history and range limits still apply.
pub async fn run_cycle_range(
    store: Arc<Store>,
    data_dir: &Path,
    from: Date,
    to: Date,
) -> Result<SyncStatus, SyncError> {
    run_cycle_with_range(store, data_dir, Some((from, to))).await
}

async fn run_cycle_with_range(
    store: Arc<Store>,
    data_dir: &Path,
    requested_range: Option<(Date, Date)>,
) -> Result<SyncStatus, SyncError> {
    if requested_range.is_some_and(|(from, to)| to < from) {
        return Err(SyncError::Other("sync range end precedes its start".into()));
    }
    let started_at = rfc3339_now();
    let credentials = crate::polar::credentials::PolarCredentials::load(data_dir)
        .map_err(|e| SyncError::Other(e.to_string()))?
        .ok_or(SyncError::NeedsAuth)?;
    let client = crate::polar::http_client()
        .map_err(|e| SyncError::Other(format!("TLS client setup failed: {e}")))?;
    let min_free_bytes = match std::env::var("RUST_POLAR_MIN_FREE_BYTES") {
        Ok(value) => value.parse::<u64>().map_err(|error| {
            SyncError::Other(format!(
                "RUST_POLAR_MIN_FREE_BYTES must be an integer: {error}"
            ))
        })?,
        Err(std::env::VarError::NotPresent) => crate::archive::DEFAULT_MIN_FREE_BYTES,
        Err(error) => {
            return Err(SyncError::Other(format!(
                "could not read RUST_POLAR_MIN_FREE_BYTES: {error}"
            )));
        }
    };
    let archive =
        ArchiveStore::new(store.archive_path().to_path_buf()).with_min_free_bytes(min_free_bytes);
    let now = OffsetDateTime::now_utc().date();
    let from = now - CalendarDuration::days(6);
    let store_for_plan = store.clone();
    let activity_checkpoint = tokio::task::spawn_blocking(move || {
        store_for_plan.get_sync_state(&format!("{CHECKPOINT_PREFIX}activity"))
    })
    .await
    .map_err(|e| SyncError::Transient(e.to_string()))??;
    let mut requests = match requested_range {
        Some((requested_from, requested_to)) => {
            build_explicit_requests(&credentials.user_id, requested_from, requested_to)?
        }
        None => build_requests(
            &credentials.user_id,
            from,
            now,
            activity_checkpoint.is_none(),
        )?,
    };
    let mut result = SyncStatus {
        started_at,
        finished_at: String::new(),
        state: "running".to_owned(),
        detail: "Polar synchronization is in progress".to_owned(),
        requests: 0,
        archived: 0,
        imported: 0,
        no_data: 0,
        last_successful_day: None,
    };
    let mut imported_receipts = HashSet::new();
    let mut exported_exercises = HashSet::new();
    let mut requested_sleep_dates = HashSet::new();
    let mut resource_errors = BTreeSet::new();
    let store_for_pending = store.clone();
    let pending_ids =
        tokio::task::spawn_blocking(move || store_for_pending.pending_archive_ids(1000))
            .await
            .map_err(|e| SyncError::Transient(e.to_string()))??;
    if !pending_ids.is_empty() {
        let store_for_ingest = store.clone();
        let user_id = credentials.user_id.clone();
        let pending_for_ingest = pending_ids.iter().cloned().collect::<HashSet<_>>();
        let imported = tokio::task::spawn_blocking(move || {
            crate::ingest::replay_archive(
                &store_for_ingest,
                None,
                &user_id,
                Some(&pending_for_ingest),
            )
        })
        .await
        .map_err(|e| SyncError::Transient(e.to_string()))?
        .map_err(|e| SyncError::Other(format!("pending archive replay failed: {e}")))?;
        result.imported = result.imported.saturating_add(imported as u64);
    }
    while let Some(request) = requests.pop() {
        let exercise_list = request.resource == "exercises";
        let sleep_available = request.resource == "sleep-available";
        let outcomes = fetch_pages(
            &store,
            &client,
            &archive,
            &credentials.access_token,
            request,
        )
        .await?;
        for (receipt, _headers) in outcomes {
            if result.requests >= MAX_RECEIPTS_PER_CYCLE as u64 {
                return Err(SyncError::Other(
                    "cycle exceeded its bounded receipt limit".into(),
                ));
            }
            let export_ids = if exercise_list
                && matches!(response_disposition(&receipt), ResponseDisposition::Import)
            {
                let archive_for_parse = archive.clone();
                let receipt_for_parse = receipt.clone();
                tokio::task::spawn_blocking(move || {
                    exercise_exports_from_receipt(&archive_for_parse, &receipt_for_parse)
                })
                .await
                .map_err(|error| SyncError::Transient(error.to_string()))??
            } else {
                Vec::new()
            };
            let sleep_dates = if sleep_available
                && matches!(response_disposition(&receipt), ResponseDisposition::Import)
            {
                let archive_for_parse = archive.clone();
                let receipt_for_parse = receipt.clone();
                tokio::task::spawn_blocking(move || {
                    sleep_dates_from_receipt(&archive_for_parse, &receipt_for_parse)
                })
                .await
                .map_err(|error| SyncError::Transient(error.to_string()))??
            } else {
                Vec::new()
            };
            if sleep_available
                && matches!(response_disposition(&receipt), ResponseDisposition::Import)
            {
                // Availability is a planning document, not a user record.
                // Its response is already durable in the archive, so mark it
                // directly instead of routing it through provider-record replay.
                result.requests = result.requests.saturating_add(1);
                result.archived = result.archived.saturating_add(1);
                if empty_sleep_availability(&receipt, &sleep_dates) {
                    set_archive_state(&store, &receipt.receipt_id, "no-data").await?;
                    result.no_data = result.no_data.saturating_add(1);
                } else {
                    set_archive_state(&store, &receipt.receipt_id, "processed").await?;
                }
            } else {
                process_receipt(
                    &store,
                    &receipt,
                    &mut imported_receipts,
                    &mut resource_errors,
                    &mut result,
                )
                .await?;
            }

            for date in sleep_dates {
                if !requested_sleep_dates.insert(date) {
                    continue;
                }
                if requested_sleep_dates.len() > MAX_SLEEP_AVAILABLE_DATES_PER_CYCLE {
                    return Err(SyncError::Other(format!(
                        "sleep/available returned more than {MAX_SLEEP_AVAILABLE_DATES_PER_CYCLE} dates in one cycle"
                    )));
                }
                let sleep_request = resources::sleep(Some(date));
                let sleep_receipts = fetch_pages(
                    &store,
                    &client,
                    &archive,
                    &credentials.access_token,
                    sleep_request,
                )
                .await?;
                for (sleep_receipt, _) in sleep_receipts {
                    if result.requests >= MAX_RECEIPTS_PER_CYCLE as u64 {
                        return Err(SyncError::Other(
                            "cycle exceeded its bounded receipt limit".into(),
                        ));
                    }
                    process_receipt(
                        &store,
                        &sleep_receipt,
                        &mut imported_receipts,
                        &mut resource_errors,
                        &mut result,
                    )
                    .await?;
                }
            }

            for (exercise_id, has_route) in export_ids {
                if !exported_exercises.insert(exercise_id.clone()) {
                    continue;
                }
                // The live `/exercises/{id}` endpoint was checked against the
                // list response: it has no extra fields and omits samples,
                // heart-rate zones, and route data. Keep its builder and
                // replay path for archived/manual fetches, but avoid spending
                // one request per exercise on every sync cycle.
                for format in [
                    resources::ExerciseExport::Fit,
                    resources::ExerciseExport::Tcx,
                    resources::ExerciseExport::Gpx,
                ] {
                    if format == resources::ExerciseExport::Gpx && !has_route {
                        continue;
                    }
                    let mut export_request = resources::exercise_export(&exercise_id, format);
                    export_request
                        .metadata
                        .insert("exercise_id".into(), exercise_id.clone());
                    let export_receipts = fetch_pages(
                        &store,
                        &client,
                        &archive,
                        &credentials.access_token,
                        export_request,
                    )
                    .await?;
                    for (export_receipt, _) in export_receipts {
                        if result.requests >= MAX_RECEIPTS_PER_CYCLE as u64 {
                            return Err(SyncError::Other(
                                "cycle exceeded its bounded receipt limit".into(),
                            ));
                        }
                        process_receipt(
                            &store,
                            &export_receipt,
                            &mut imported_receipts,
                            &mut resource_errors,
                            &mut result,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    if !imported_receipts.is_empty() {
        let store_for_ingest = store.clone();
        let user_id = credentials.user_id.clone();
        let imported_receipts_for_ingest = imported_receipts.clone();
        let imported = tokio::task::spawn_blocking(move || {
            crate::ingest::replay_archive(
                &store_for_ingest,
                None,
                &user_id,
                Some(&imported_receipts_for_ingest),
            )
        })
        .await
        .map_err(|e| SyncError::Transient(e.to_string()))?
        .map_err(|e| SyncError::Other(format!("archived response could not be imported: {e}")))?;
        result.imported = result.imported.saturating_add(imported as u64);
    }
    result.finished_at = rfc3339_now();
    if resource_errors.is_empty() {
        result.state = "available".to_owned();
        result.detail =
            "Polar cycle completed; raw responses were archived before import".to_owned();
    } else {
        result.state = "partial".to_owned();
        result.detail = format!(
            "Polar cycle completed with resource errors: {}",
            resource_errors.into_iter().collect::<Vec<_>>().join(", ")
        );
    }
    result.last_successful_day = Some(format_date(now));
    let store_for_checkpoint = store.clone();
    let day = format_date(now);
    let status = result.clone();
    tokio::task::spawn_blocking(move || {
        store_for_checkpoint.put_sync_state(SYNC_STATUS_KEY, &status)?;
        store_for_checkpoint
            .put_sync_state(&format!("{CHECKPOINT_PREFIX}last-successful-day"), &day)?;
        store_for_checkpoint.put_sync_state(&format!("{CHECKPOINT_PREFIX}activity"), &day)
    })
    .await
    .map_err(|e| SyncError::Transient(e.to_string()))??;
    Ok(result)
}

async fn set_archive_state(
    store: &Arc<Store>,
    receipt_id: &str,
    state: &'static str,
) -> Result<(), SyncError> {
    let store = store.clone();
    let receipt_id = receipt_id.to_owned();
    tokio::task::spawn_blocking(move || store.set_archive_state(&receipt_id, state))
        .await
        .map_err(|e| SyncError::Transient(e.to_string()))??;
    Ok(())
}

async fn process_receipt(
    store: &Arc<Store>,
    receipt: &ArchiveReceipt,
    imported_receipts: &mut HashSet<String>,
    resource_errors: &mut BTreeSet<String>,
    result: &mut SyncStatus,
) -> Result<(), SyncError> {
    result.requests = result.requests.saturating_add(1);
    result.archived = result.archived.saturating_add(1);
    match response_disposition(receipt) {
        ResponseDisposition::Import => {
            if imported_receipts.len() >= MAX_RECEIPTS_PER_CYCLE {
                return Err(SyncError::Other(
                    "cycle exceeded its bounded receipt limit".into(),
                ));
            }
            imported_receipts.insert(receipt.receipt_id.clone());
        }
        ResponseDisposition::NoData => {
            set_archive_state(store, &receipt.receipt_id, "no-data").await?;
            result.no_data = result.no_data.saturating_add(1);
        }
        ResponseDisposition::NeedsAuth => {
            set_archive_state(store, &receipt.receipt_id, "http-error").await?;
            return Err(SyncError::NeedsAuth);
        }
        ResponseDisposition::Forbidden => {
            set_archive_state(store, &receipt.receipt_id, "http-error").await?;
            resource_errors.insert(format!("{} HTTP 403", receipt.resource));
        }
        ResponseDisposition::Failed(status) => {
            set_archive_state(store, &receipt.receipt_id, "http-error").await?;
            resource_errors.insert(format!("{} HTTP {status}", receipt.resource));
        }
    }
    Ok(())
}

fn empty_sleep_availability(receipt: &ArchiveReceipt, dates: &[Date]) -> bool {
    receipt.resource == "sleep-available"
        && matches!(response_disposition(receipt), ResponseDisposition::Import)
        && dates.is_empty()
}

fn exercise_exports_from_receipt(
    archive: &ArchiveStore,
    receipt: &ArchiveReceipt,
) -> Result<Vec<(String, bool)>, SyncError> {
    let mut reader = archive
        .read(receipt)
        .map_err(|error| SyncError::Other(error.to_string()))?;
    let mut exports = Vec::new();
    let mut seen = HashSet::new();
    crate::polar::exercise::for_each_exercise(&mut reader, |exercise| {
        if exports.len() >= MAX_EXERCISES_PER_PAGE {
            return Err("exercise response exceeded its bounded export-planning limit".to_owned());
        }
        let id = exercise
            .id
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| exercise.id.to_string());
        if id.is_empty() || !seen.insert(id.clone()) {
            return Ok(());
        }
        let has_route = exercise.has_route == Some(true)
            || exercise
                .route
                .as_ref()
                .is_some_and(|route| !route.is_empty());
        exports.push((id, has_route));
        Ok(())
    })
    .map_err(|error| SyncError::Other(error.to_string()))?;
    Ok(exports)
}

fn build_requests(
    user_id: &str,
    from: Date,
    to: Date,
    initial_backfill: bool,
) -> Result<Vec<ResourceRequest>, SyncError> {
    let mut requests = vec![resources::user_profile(user_id), resources::physical_info()];
    for (start, end) in resources::date_windows(to - CalendarDuration::days(29), to, 28)
        .map_err(|e| SyncError::Other(e.to_string()))?
    {
        requests.push(resources::exercises(start, end));
    }
    let recent_from = if initial_backfill {
        to - CalendarDuration::days(27)
    } else {
        from
    };
    for (start, end) in
        resources::date_windows(recent_from, to, 28).map_err(|e| SyncError::Other(e.to_string()))?
    {
        requests.push(resources::activities(start, end));
        requests.push(resources::sleepwise_alertness(start, end));
        requests.push(resources::sleepwise_circadian_bedtime(start, end));
        requests.push(resources::cardio_load(start, end));
        requests.push(resources::cardio_load_history(start, end));
    }
    let sleep_from = if initial_backfill {
        to - CalendarDuration::days(27)
    } else {
        from
    };
    let mut available = resources::sleep_available();
    available
        .metadata
        .insert("from".into(), format_date(sleep_from));
    available.metadata.insert("to".into(), format_date(to));
    requests.push(available);

    let day_count = if initial_backfill { 28 } else { 7 };
    for day in (0..day_count).rev() {
        let date = to - CalendarDuration::days(day);
        requests.push(resources::continuous_heart_rate(Some(date)));
        requests.push(resources::nightly_recharge(Some(date)));
    }
    // A missing checkpoint means this is a new installation. Activity has a
    // documented 365-day history; the first cycle fetches all of it in legal
    // 28-day windows. Later cycles reread the preceding week for corrections.
    if initial_backfill {
        requests.retain(|request| request.resource != "activities");
        let historical_from = to - CalendarDuration::days(364);
        for (start, end) in resources::date_windows(historical_from, to, 28)
            .map_err(|e| SyncError::Other(e.to_string()))?
        {
            requests.push(resources::activities(start, end));
        }
    }
    Ok(requests)
}

fn build_explicit_requests(
    user_id: &str,
    from: Date,
    to: Date,
) -> Result<Vec<ResourceRequest>, SyncError> {
    if from > to {
        return Err(SyncError::Other("sync range end precedes its start".into()));
    }
    if from < to - CalendarDuration::days(364) {
        return Err(SyncError::Other(
            "explicit sync range cannot exceed the 365-day activity history".into(),
        ));
    }
    let mut requests = vec![resources::user_profile(user_id), resources::physical_info()];

    // Exercise upload history is limited to a recent 30-day window.
    let exercise_from = from.max(to - CalendarDuration::days(29));
    for (start, end) in resources::date_windows(exercise_from, to, 30)
        .map_err(|e| SyncError::Other(e.to_string()))?
    {
        requests.push(resources::exercises(start, end));
    }

    for (start, end) in
        resources::date_windows(from, to, 28).map_err(|e| SyncError::Other(e.to_string()))?
    {
        requests.push(resources::activities(start, end));
    }

    // Recovery and several date-based endpoints expose a short recent window.
    // Preserve a requested historical interval in the API, but do not invent
    // older data by issuing unbounded date requests.
    let recent_from = from.max(to - CalendarDuration::days(27));
    for (start, end) in
        resources::date_windows(recent_from, to, 28).map_err(|e| SyncError::Other(e.to_string()))?
    {
        requests.push(resources::sleepwise_alertness(start, end));
        requests.push(resources::sleepwise_circadian_bedtime(start, end));
        requests.push(resources::cardio_load(start, end));
        requests.push(resources::cardio_load_history(start, end));
    }
    for day in (0..=(to - recent_from).whole_days()).rev() {
        let date = to - CalendarDuration::days(day);
        requests.push(resources::continuous_heart_rate(Some(date)));
        requests.push(resources::nightly_recharge(Some(date)));
    }
    let mut available = resources::sleep_available();
    available
        .metadata
        .insert("from".into(), format_date(recent_from));
    available.metadata.insert("to".into(), format_date(to));
    requests.push(available);
    Ok(requests)
}

fn sleep_dates_from_receipt(
    archive: &ArchiveStore,
    receipt: &ArchiveReceipt,
) -> Result<Vec<Date>, SyncError> {
    archive.verify(receipt).map_err(|error| {
        SyncError::Other(format!(
            "sleep/available archive verification failed: {error}"
        ))
    })?;
    let reader = archive.read(receipt).map_err(|error| {
        SyncError::Other(format!("sleep/available archive read failed: {error}"))
    })?;
    let mut reader = std::io::Read::take(reader, (MAX_SLEEP_AVAILABLE_RESPONSE_BYTES + 1) as u64);
    let mut bytes = Vec::with_capacity(4096);
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| SyncError::Other(format!("sleep/available read failed: {error}")))?;
    if bytes.len() > MAX_SLEEP_AVAILABLE_RESPONSE_BYTES {
        return Err(SyncError::Other(format!(
            "sleep/available response exceeds {MAX_SLEEP_AVAILABLE_RESPONSE_BYTES} bytes"
        )));
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| SyncError::Other(format!("invalid sleep/available JSON: {error}")))?;
    let from = receipt
        .request
        .get("from")
        .and_then(|value| Date::parse(value, &Iso8601::DATE).ok())
        .ok_or_else(|| SyncError::Other("sleep/available request has no valid from date".into()))?;
    let to = receipt
        .request
        .get("to")
        .and_then(|value| Date::parse(value, &Iso8601::DATE).ok())
        .ok_or_else(|| SyncError::Other("sleep/available request has no valid to date".into()))?;
    let mut dates = std::collections::BTreeSet::new();
    if !collect_sleep_dates(&value, 0, from, to, &mut dates)? {
        return Err(SyncError::Other(
            "unrecognized sleep/available response shape".into(),
        ));
    }
    if dates.len() > MAX_SLEEP_AVAILABLE_DATES_PER_CYCLE {
        return Err(SyncError::Other(format!(
            "sleep/available returned more than {MAX_SLEEP_AVAILABLE_DATES_PER_CYCLE} dates in one cycle"
        )));
    }
    Ok(dates.into_iter().collect())
}

fn collect_sleep_dates(
    value: &serde_json::Value,
    depth: usize,
    from: Date,
    to: Date,
    dates: &mut std::collections::BTreeSet<Date>,
) -> Result<bool, SyncError> {
    if depth > 8 {
        return Err(SyncError::Other(
            "sleep/available response nesting exceeds 8 levels".into(),
        ));
    }
    Ok(match value {
        serde_json::Value::String(value) => {
            let Ok(date) = Date::parse(value, &Iso8601::DATE) else {
                return Ok(false);
            };
            if date >= from && date <= to {
                dates.insert(date);
            }
            true
        }
        serde_json::Value::Array(items) => {
            let mut recognized = items.is_empty();
            for item in items {
                match item {
                    serde_json::Value::String(_) => {
                        recognized |= collect_sleep_dates(item, depth + 1, from, to, dates)?;
                    }
                    serde_json::Value::Object(object) => {
                        let date = object
                            .get("date")
                            .or_else(|| object.get("sleep_date"))
                            .or_else(|| object.get("sleep-date"));
                        if let Some(date) = date {
                            recognized |= collect_sleep_dates(date, depth + 1, from, to, dates)?;
                        }
                    }
                    _ => {}
                }
            }
            recognized
        }
        serde_json::Value::Object(object) => {
            let mut recognized = false;
            for key in [
                "dates",
                "available_dates",
                "available-dates",
                "available",
                "sleep",
                "data",
                "items",
            ] {
                if let Some(items) = object.get(key) {
                    recognized |= collect_sleep_dates(items, depth + 1, from, to, dates)?;
                }
            }
            recognized
        }
        _ => false,
    })
}

async fn fetch_pages(
    store: &Arc<Store>,
    client: &crate::polar::AccessLinkClient,
    archive: &ArchiveStore,
    token: &str,
    request: ResourceRequest,
) -> Result<Vec<(ArchiveReceipt, crate::polar::AccessLinkResponseHeaders)>, SyncError> {
    let client = client.clone();
    let archive = archive.clone();
    let token = token.to_owned();
    fetch_pages_using(store, request, move |request, fetched_at| {
        let client = client.clone();
        let archive = archive.clone();
        let token = token.clone();
        async move {
            tokio::time::timeout(
                Duration::from_secs(60),
                crate::polar::fetch_and_archive(
                    &client,
                    archive,
                    &request.url,
                    &token,
                    &request.resource,
                    request.metadata,
                    &fetched_at,
                ),
            )
            .await
            .map_err(|_| "AccessLink request timed out after 60 seconds".to_owned())?
            .map_err(|error| error.to_string())
        }
    })
    .await
}

/// Run pagination/retry/archive bookkeeping with an injectable fetch operation.
/// Production uses the HTTPS-only AccessLink client above. Unit tests inject
/// deterministic archived responses because certificate-valid TLS localhost
/// would require weakening the production trust configuration.
async fn fetch_pages_using<F, Fut>(
    store: &Arc<Store>,
    request: ResourceRequest,
    mut fetch: F,
) -> Result<Vec<(ArchiveReceipt, crate::polar::AccessLinkResponseHeaders)>, SyncError>
where
    F: FnMut(ResourceRequest, String) -> Fut,
    Fut: Future<Output = Result<(ArchiveReceipt, crate::polar::AccessLinkResponseHeaders), String>>,
{
    let mut pending = vec![request];
    let mut receipts = Vec::new();
    let mut page_count = 0usize;
    while let Some(request) = pending.pop() {
        page_count += 1;
        if page_count > MAX_PAGES_PER_REQUEST {
            return Err(SyncError::Other(
                "pagination exceeded 500 pages for one request".into(),
            ));
        }
        let mut attempt = 0u8;
        loop {
            let fetched_at = rfc3339_now();
            let (receipt, headers) = match fetch(request.clone(), fetched_at).await {
                Ok(response) => response,
                Err(error) => {
                    attempt += 1;
                    if attempt >= 4 {
                        return Err(SyncError::Transient(error));
                    }
                    tokio::time::sleep(Duration::from_secs(2_u64.pow(u32::from(attempt)))).await;
                    continue;
                }
            };
            let store_for_receipt = store.clone();
            let receipt_for_store = receipt.clone();
            tokio::task::spawn_blocking(move || {
                store_for_receipt
                    .register_archive(&receipt_for_store.receipt_id, &receipt_for_store)
            })
            .await
            .map_err(|e| SyncError::Transient(e.to_string()))??;
            if receipt.status == 429 || receipt.status >= 500 {
                set_archive_state(store, &receipt.receipt_id, "http-error").await?;
                attempt += 1;
                if attempt >= 4 {
                    receipts.push((receipt, headers));
                    break;
                }
                let delay = headers
                    .retry_after
                    .as_deref()
                    .and_then(retry_delay_seconds)
                    .map(|seconds| Duration::from_secs(seconds.min(300)))
                    .unwrap_or_else(|| Duration::from_secs(2_u64.pow(u32::from(attempt))));
                tokio::time::sleep(delay).await;
                continue;
            }
            if (200..300).contains(&receipt.status)
                && let Some(next) = next_page(&headers.link, &receipt, &request.metadata)?
            {
                pending.push(next);
            }
            receipts.push((receipt, headers));
            break;
        }
    }
    Ok(receipts)
}

fn retry_delay_seconds(value: &str) -> Option<u64> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds);
    }
    let date = OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822).ok()?;
    u64::try_from((date - OffsetDateTime::now_utc()).whole_seconds().max(0)).ok()
}

fn next_page(
    link: &Option<String>,
    receipt: &ArchiveReceipt,
    request_metadata: &RequestMetadata,
) -> Result<Option<ResourceRequest>, SyncError> {
    let Some(link) = link else { return Ok(None) };
    for entry in link.split(',') {
        let mut parts = entry.trim().split(';');
        let Some(target) = parts.next() else { continue };
        if !parts.any(|parameter| {
            parameter
                .trim()
                .strip_prefix("rel=")
                .is_some_and(|value| value.trim_matches('"') == "next")
        }) {
            continue;
        }
        let url = target.trim().trim_start_matches('<').trim_end_matches('>');
        let uri = url
            .parse::<hyper::Uri>()
            .map_err(|_| SyncError::Other("Polar returned an invalid pagination link".into()))?;
        if uri.scheme_str() != Some("https")
            || uri
                .authority()
                .is_none_or(|authority| authority.host() != "www.polaraccesslink.com")
            || !uri.path().starts_with("/v3/")
        {
            return Err(SyncError::Other(
                "Polar pagination link escaped the AccessLink API origin".into(),
            ));
        }
        let mut metadata = request_metadata.clone();
        metadata.insert("page".to_owned(), "next".to_owned());
        return Ok(Some(ResourceRequest {
            resource: receipt.resource.clone(),
            url: url.to_owned(),
            metadata,
        }));
    }
    Ok(None)
}

fn rfc3339_now() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn format_date(date: Date) -> String {
    date.format(&Iso8601::DATE)
        .unwrap_or_else(|_| "1970-01-01".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(value: &str) -> Date {
        Date::parse(value, &Iso8601::DATE).unwrap()
    }

    #[test]
    fn request_plan_uses_documented_backfill_and_poll_windows() {
        let initial = build_requests(
            "123",
            date("2026-10-21") - CalendarDuration::days(6),
            date("2026-10-21"),
            true,
        )
        .unwrap();
        let follow_up = build_requests(
            "123",
            date("2026-10-21") - CalendarDuration::days(6),
            date("2026-10-21"),
            false,
        )
        .unwrap();
        assert!(initial.iter().any(|request| {
            request.resource == "activities"
                && request
                    .metadata
                    .get("from")
                    .is_some_and(|from| from == "2025-10-22")
        }));
        assert_eq!(
            follow_up
                .iter()
                .filter(|request| request.resource == "activities")
                .count(),
            1
        );
        assert_eq!(
            initial
                .iter()
                .filter(|request| request.resource == "sleep")
                .count(),
            0
        );
        let sleep_available = initial
            .iter()
            .find(|request| request.resource == "sleep-available")
            .unwrap();
        assert_eq!(sleep_available.url, resources::sleep_available().url);
        assert_eq!(sleep_available.metadata.get("from").unwrap(), "2026-09-24");
        assert_eq!(sleep_available.metadata.get("to").unwrap(), "2026-10-21");
        assert_eq!(
            follow_up
                .iter()
                .filter(|request| request.resource == "sleep-available")
                .count(),
            1
        );
        assert!(initial.iter().any(|request| request.resource == "exercises"
            && request.url.contains("samples=true&zones=true&route=true")));
    }

    #[test]
    fn explicit_sync_plan_splits_activity_windows_and_bounds_history() {
        let requests =
            build_explicit_requests("123", date("2026-10-01"), date("2026-10-31")).unwrap();
        let activities = requests
            .iter()
            .filter(|request| request.resource == "activities")
            .collect::<Vec<_>>();
        assert_eq!(activities.len(), 2);
        assert_eq!(activities[0].metadata.get("from").unwrap(), "2026-10-01");
        assert_eq!(activities[0].metadata.get("to").unwrap(), "2026-10-28");
        assert_eq!(activities[1].metadata.get("from").unwrap(), "2026-10-29");
        assert_eq!(activities[1].metadata.get("to").unwrap(), "2026-10-31");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.resource == "sleep")
                .count(),
            0
        );
        let available = requests
            .iter()
            .find(|request| request.resource == "sleep-available")
            .unwrap();
        assert_eq!(available.metadata.get("from").unwrap(), "2026-10-04");
        assert_eq!(available.metadata.get("to").unwrap(), "2026-10-31");
        assert!(build_explicit_requests("123", date("2025-10-30"), date("2026-10-31")).is_err());
    }

    #[test]
    fn sleep_available_receipt_is_verified_and_dates_are_deduplicated_and_bounded() {
        let root = std::env::temp_dir().join(format!(
            "rust-polar-sleep-available-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let archive = ArchiveStore::new(root.clone()).with_min_free_bytes(0);
        let mut metadata = RequestMetadata::new();
        metadata.insert("from".into(), "2026-10-01".into());
        metadata.insert("to".into(), "2026-10-28".into());
        let body = br#"{"dates":["2026-10-28","2026-10-27","2026-10-28","2026-09-30"],"data":[{"date":"2026-10-26"},{"sleep_date":"2026-10-25"}]}"#;
        let receipt = archive
            .store_body(
                "polar",
                "sleep-available",
                metadata,
                200,
                "2026-10-29T00:00:00Z",
                std::io::Cursor::new(body.as_slice()),
            )
            .unwrap();
        let dates = sleep_dates_from_receipt(&archive, &receipt).unwrap();
        assert_eq!(
            dates,
            vec![
                date("2026-10-25"),
                date("2026-10-26"),
                date("2026-10-27"),
                date("2026-10-28")
            ]
        );
        assert!(archive.verify(&receipt).is_ok());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn empty_sleep_available_success_is_no_data_but_404_uses_http_classification() {
        let receipt = |status, bytes| ArchiveReceipt {
            receipt_id: "sleep-available-receipt".into(),
            content_hash: "hash".into(),
            source: "polar".into(),
            resource: "sleep-available".into(),
            request: RequestMetadata::new(),
            status,
            fetched_at: "2026-10-21T00:00:00Z".into(),
            uncompressed_bytes: bytes,
            compressed_bytes: 0,
        };
        let empty_success = receipt(200, 2);
        assert!(empty_sleep_availability(&empty_success, &[]));
        assert!(!empty_sleep_availability(
            &empty_success,
            &[date("2026-10-21")]
        ));
        let not_found = receipt(404, 10);
        assert_eq!(
            response_disposition(&not_found),
            ResponseDisposition::NoData
        );
        assert!(!empty_sleep_availability(&not_found, &[]));
    }

    #[test]
    fn pagination_is_limited_to_accesslink_origin_and_keeps_safe_metadata() {
        let receipt = ArchiveReceipt {
            receipt_id: "r1".into(),
            content_hash: "hash".into(),
            source: "polar".into(),
            resource: "activities".into(),
            request: RequestMetadata::new(),
            status: 200,
            fetched_at: "2026-10-21T00:00:00Z".into(),
            uncompressed_bytes: 10,
            compressed_bytes: 10,
        };
        let mut metadata = RequestMetadata::new();
        metadata.insert("from".into(), "2026-10-01".into());
        let next = next_page(
            &Some(
                "<https://www.polaraccesslink.com/v3/users/activities?page=2>; rel=\"next\"".into(),
            ),
            &receipt,
            &metadata,
        )
        .unwrap()
        .unwrap();
        assert!(next.url.ends_with("page=2"));
        assert_eq!(
            next.metadata.get("from").map(String::as_str),
            Some("2026-10-01")
        );
        assert!(
            next_page(
                &Some("<https://evil.example/v3/users/activities?page=2>; rel=\"next\"".into()),
                &receipt,
                &metadata,
            )
            .is_err()
        );
    }

    #[test]
    fn exercise_export_planning_is_bounded_deduplicated_and_route_aware() {
        let root = std::env::temp_dir().join(format!(
            "rust-polar-exercise-export-plan-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let archive = ArchiveStore::new(root.clone()).with_min_free_bytes(0);
        let receipt = archive
            .store_body(
                "polar",
                "exercises",
                RequestMetadata::new(),
                200,
                "2026-10-21T00:00:00Z",
                std::io::Cursor::new(
                    include_bytes!("../../tests/fixtures/polar/exercises.json").as_slice(),
                ),
            )
            .unwrap();
        let exercises = exercise_exports_from_receipt(&archive, &receipt).unwrap();
        assert_eq!(exercises, vec![("2AC312F".to_owned(), true)]);
        let request = resources::exercise_export("2AC312F", resources::ExerciseExport::Fit);
        assert!(request.url.ends_with("/exercises/2AC312F/fit"));
        assert!(!request.url.contains("exercise_id"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn retry_after_supports_delta_seconds_and_http_dates() {
        assert_eq!(retry_delay_seconds("30"), Some(30));
        assert!(retry_delay_seconds("not a date").is_none());
    }

    #[test]
    fn fake_accesslink_response_is_archived_then_persisted_as_parsed_record() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "rust-polar-fake-sync-{}-{nonce}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&root);
            let store = Arc::new(Store::open(&root).unwrap());
            let archive = ArchiveStore::new(store.archive_path().to_path_buf());
            let mut metadata = RequestMetadata::new();
            metadata.insert("from".into(), "2026-10-19".into());
            metadata.insert("to".into(), "2026-10-21".into());
            let request = ResourceRequest {
                resource: "activities".into(),
                url: "https://www.polaraccesslink.com/v3/users/activities?from=2026-10-19&to=2026-10-21".into(),
                metadata,
            };
            let fixture = include_bytes!("../../tests/fixtures/polar/daily_activity.json");
            let archive_for_fetch = archive.clone();
            let outcomes = fetch_pages_using(&store, request, move |request, fetched_at| {
                let archive = archive_for_fetch.clone();
                async move {
                    let receipt = archive
                        .store_body(
                            "polar",
                            request.resource,
                            request.metadata,
                            200,
                            fetched_at,
                            std::io::Cursor::new(fixture.as_slice()),
                        )
                        .map_err(|error| error.to_string())?;
                    Ok((receipt, crate::polar::AccessLinkResponseHeaders::default()))
                }
            })
            .await
            .unwrap();

            assert_eq!(outcomes.len(), 1);
            let receipt = outcomes[0].0.clone();
            archive.verify(&receipt).unwrap();
            assert_eq!(
                crate::ingest::replay_archive(
                    &store,
                    None,
                    "fake-polar-user",
                    Some(&HashSet::from([receipt.receipt_id.clone()])),
                )
                .unwrap(),
                1
            );
            let record = store
                .get_record("polar/activities/2026-10-19")
                .unwrap()
                .expect("parsed activity should be committed to redb");
            let parsed: serde_json::Value = serde_json::from_slice(&record).unwrap();
            assert_eq!(parsed["record"]["steps"], 8823);
            assert_eq!(store.archive_count().unwrap(), 1);
            drop(store);
            let reopened = Store::open(&root).unwrap();
            assert!(reopened
                .get_record("polar/activities/2026-10-19")
                .unwrap()
                .is_some());
            drop(reopened);
            let _ = std::fs::remove_dir_all(root);
        });
    }

    #[test]
    fn resource_http_errors_are_recorded_without_aborting_other_resources() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "rust-polar-http-error-{}-{nonce}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&root);
            let store = Arc::new(Store::open(&root).unwrap());
            let archive = ArchiveStore::new(store.archive_path().to_path_buf());
            let receipt = archive
                .store_body(
                    "polar",
                    "cardio-load-history",
                    RequestMetadata::new(),
                    400,
                    "2026-10-21T00:00:00Z",
                    std::io::Cursor::new(b"Invalid date history for date".as_slice()),
                )
                .unwrap();
            store
                .register_archive(&receipt.receipt_id, &receipt)
                .unwrap();

            let mut imported_receipts = HashSet::new();
            let mut resource_errors = BTreeSet::new();
            let mut result = SyncStatus {
                started_at: "2026-10-21T00:00:00Z".into(),
                finished_at: String::new(),
                state: "running".into(),
                detail: String::new(),
                requests: 0,
                archived: 0,
                imported: 0,
                no_data: 0,
                last_successful_day: None,
            };
            process_receipt(
                &store,
                &receipt,
                &mut imported_receipts,
                &mut resource_errors,
                &mut result,
            )
            .await
            .unwrap();

            assert_eq!(result.requests, 1);
            assert_eq!(result.archived, 1);
            assert!(imported_receipts.is_empty());
            assert_eq!(
                resource_errors,
                BTreeSet::from(["cardio-load-history HTTP 400".to_owned()])
            );
            assert!(store.pending_archive_ids(10).unwrap().is_empty());

            drop(store);
            let _ = std::fs::remove_dir_all(root);
        });
    }

    #[test]
    fn status_handling_classifies_success_empty_auth_permission_and_server_errors() {
        let receipt = |status, uncompressed_bytes| ArchiveReceipt {
            receipt_id: "r".into(),
            content_hash: "hash".into(),
            source: "polar".into(),
            resource: "activities".into(),
            request: RequestMetadata::new(),
            status,
            fetched_at: "2026-10-21T00:00:00Z".into(),
            uncompressed_bytes,
            compressed_bytes: 0,
        };
        assert_eq!(
            response_disposition(&receipt(200, 0)),
            ResponseDisposition::NoData
        );
        assert_eq!(
            response_disposition(&receipt(200, 1)),
            ResponseDisposition::Import
        );
        assert_eq!(
            response_disposition(&receipt(204, 0)),
            ResponseDisposition::NoData
        );
        assert_eq!(
            response_disposition(&receipt(404, 20)),
            ResponseDisposition::NoData
        );
        assert_eq!(
            response_disposition(&receipt(401, 20)),
            ResponseDisposition::NeedsAuth
        );
        assert_eq!(
            response_disposition(&receipt(403, 20)),
            ResponseDisposition::Forbidden
        );
        for status in [400, 429, 500, 503] {
            assert_eq!(
                response_disposition(&receipt(status, 20)),
                ResponseDisposition::Failed(status)
            );
        }
    }

    #[test]
    fn fake_fetch_retry_and_pagination_register_every_response() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "rust-polar-fake-pagination-{}-{nonce}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&root);
            let store = Arc::new(Store::open(&root).unwrap());
            let archive = ArchiveStore::new(store.archive_path().to_path_buf());
            let request = ResourceRequest {
                resource: "activities".into(),
                url: "https://www.polaraccesslink.com/v3/users/activities?page=1".into(),
                metadata: RequestMetadata::new(),
            };
            let attempts = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::<
                String,
                usize,
            >::new()));
            let archive_for_fetch = archive.clone();
            let attempts_for_fetch = attempts.clone();
            let outcomes = fetch_pages_using(&store, request, move |request, fetched_at| {
                let archive = archive_for_fetch.clone();
                let attempts = attempts_for_fetch.clone();
                async move {
                    let page_two = request.url.contains("page=2");
                    let attempt = {
                        let mut attempts = attempts.lock().unwrap();
                        let count = attempts.entry(request.url.clone()).or_default();
                        *count += 1;
                        *count
                    };
                    let status = if !page_two && attempt == 1 { 429 } else { 200 };
                    let body: &[u8] = if status == 429 {
                        br#"{"error":"throttled"}"#
                    } else if page_two {
                        br#"[{"date":"2026-10-02","steps":20}]"#
                    } else {
                        br#"[{"date":"2026-10-01","steps":10}]"#
                    };
                    let receipt = archive
                        .store_body(
                            "polar",
                            request.resource,
                            request.metadata,
                            status,
                            fetched_at,
                            std::io::Cursor::new(body),
                        )
                        .map_err(|error| error.to_string())?;
                    let mut headers = crate::polar::AccessLinkResponseHeaders::default();
                    if status == 429 {
                        headers.retry_after = Some("0".into());
                    } else if !page_two {
                        headers.link = Some(
                            "<https://www.polaraccesslink.com/v3/users/activities?page=2>; rel=\"next\""
                                .into(),
                        );
                    }
                    Ok((receipt, headers))
                }
            })
            .await
            .unwrap();

            assert_eq!(outcomes.len(), 2);
            assert_eq!(store.archive_count().unwrap(), 3);
            assert_eq!(
                attempts.lock().unwrap().values().copied().sum::<usize>(),
                3
            );
            assert_eq!(store.pending_archive_ids(10).unwrap().len(), 2);
            let receipts = archive
                .receipts()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(receipts.len(), 3);
            for receipt in receipts {
                archive.verify(&receipt).unwrap();
            }
            drop(store);
            let reopened = Store::open(&root).unwrap();
            assert_eq!(reopened.archive_count().unwrap(), 3);
            drop(reopened);
            let _ = std::fs::remove_dir_all(root);
        });
    }
}
