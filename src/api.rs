//! Read-only HTTP/1.1 API over a private Unix domain socket.
//!
//! The daemon owns the open redb handle. Local clients query this API instead
//! of opening the database file themselves.
//!
//! Range endpoints use half-open bounds. `/v1/series` and `/v1/export` require
//! RFC3339 timestamps with an explicit offset, plus `user_id`, `source`, and
//! `metric`; `/v1/daily` takes provider-local `YYYY-MM-DD` labels and does not
//! convert them to UTC. `/v1/records` accepts optional `source` and `kind`
//! filters and an optional RFC3339 `from`/`to` pair. Records whose timestamps
//! lack an explicit offset remain available without a time filter, but are
//! excluded from UTC-filtered results.
//!
//! Examples over a connected Unix socket:
//!
//! ```text
//! GET /v1/metrics?limit=50
//! GET /v1/records?source=polar&kind=daily-activity&limit=100
//! GET /v1/records/polar%2Factivities%2F2026-10-20
//! GET /v1/daily?from=2026-10-01&to=2026-11-01
//! GET /v1/export?user_id=local&source=polar&metric=steps&from=2026-10-01T00%3A00%3A00Z&to=2026-11-01T00%3A00%3A00Z&format=csv
//! GET /v1/exercise-export?exercise_id=...&format=fit
//! ```
//!
//! List endpoints are keyset-paged with `next_cursor` (JSON) or the
//! `x-next-cursor` response header (JSONL/CSV exports). Record scans cap work
//! at 10,000 scanned rows per request, and response pages are byte bounded.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::net::UnixListener;
use tokio::sync::{Semaphore, watch};

use crate::model::MeasurementContext;
use crate::storage::{MeasurementQuery, Store};

const MAX_CONNECTIONS: usize = 16;
const MAX_PAGE_ROWS: usize = 1000;
const MAX_RECORDS_SCANNED: usize = 10_000;
const MAX_DOWNSAMPLE_BUCKETS: usize = 250;
const MAX_DOWNSAMPLE_SCAN_ROWS: usize = 1_000_000;
const MAX_METRIC_CATALOG: usize = 1000;
const MAX_API_PAGE_BYTES: usize = 5 * 1024 * 1024;
const MAX_QUERY_BYTES: usize = 16 * 1024;
const API_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
type ApiResponse = Response<UnsyncBoxBody<Bytes, io::Error>>;
const ARCHIVE_STREAM_CHUNK_BYTES: usize = 64 * 1024;
const ARCHIVE_STREAM_QUEUE_CHUNKS: usize = 2;

struct CancellationOnDrop(Arc<AtomicBool>);

impl Drop for CancellationOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

async fn spawn_blocking_cancellable<F, T>(operation: F) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce(Arc<AtomicBool>) -> T + Send + 'static,
    T: Send + 'static,
{
    let cancelled = Arc::new(AtomicBool::new(false));
    let guard = CancellationOnDrop(cancelled.clone());
    let result = tokio::task::spawn_blocking(move || operation(cancelled)).await;
    drop(guard);
    result
}

/// Start serving read-only API requests until the shutdown channel is closed
/// or set to `true`. A second live listener is never unlinked.
pub async fn serve_unix(
    store: Arc<Store>,
    socket_path: PathBuf,
    mut shutdown: watch::Receiver<bool>,
    sync_lock: Arc<tokio::sync::Mutex<()>>,
) -> io::Result<()> {
    let listener = bind_private_socket(&socket_path)?;
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = async {
                let permit = permits.clone().acquire_owned().await.map_err(io::Error::other)?;
                let (stream, _) = listener.accept().await?;
                Ok::<_, io::Error>((permit, stream))
            } => {
                let (permit, stream) = accepted?;
                let store = store.clone();
                let sync_lock = sync_lock.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    let io = TokioIo::new(stream);
                    let service = service_fn(move |request| {
                        handle_request_with_sync(request, store.clone(), sync_lock.clone())
                    });
                    let _ = http1::Builder::new()
                        .keep_alive(false)
                        .serve_connection(io, service)
                        .await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }

    drop(listener);
    let deadline = tokio::time::Instant::now() + API_SHUTDOWN_GRACE;
    while !connections.is_empty() {
        match tokio::time::timeout_at(deadline, connections.join_next()).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                // A local client can leave a request half-written forever. Let
                // active requests finish briefly, then drop their body streams
                // so bounded archive workers see the closed receiver.
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                break;
            }
        }
    }
    let _ = std::fs::remove_file(&socket_path);
    Ok(())
}

fn bind_private_socket(path: &Path) -> io::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    if path.exists() {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("another rust-polar service owns {}", path.display()),
                ));
            }
            Err(_) => {
                let metadata = std::fs::symlink_metadata(path)?;
                if !metadata.file_type().is_socket() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("refusing to replace non-socket path {}", path.display()),
                    ));
                }
                std::fs::remove_file(path)?;
            }
        }
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

async fn handle_request_with_sync(
    request: Request<Incoming>,
    store: Arc<Store>,
    sync_lock: Arc<tokio::sync::Mutex<()>>,
) -> Result<ApiResponse, hyper::Error> {
    if request.method() == Method::POST {
        let has_body = request
            .headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > 0)
            || request
                .headers()
                .contains_key(hyper::header::TRANSFER_ENCODING);
        if has_body {
            return Ok(bad_request("admin endpoints do not accept a request body"));
        }
        let path = request.uri().path().to_owned();
        let raw_query = request.uri().query().unwrap_or_default();
        if raw_query.len() > MAX_QUERY_BYTES {
            return Ok(bad_request("query string exceeds 16 KiB"));
        }
        let query = match parse_query(raw_query) {
            Ok(query) => query,
            Err(message) => return Ok(bad_request(message)),
        };
        return match path.as_str() {
            "/v1/admin/sync" => {
                let range = match (query.get("from"), query.get("to")) {
                    (None, None) => None,
                    (Some(from), Some(to)) => {
                        let date_format = match time::format_description::parse_borrowed::<1>(
                            "[year]-[month]-[day]",
                        ) {
                            Ok(format) => format,
                            Err(error) => return Ok(internal_error(error)),
                        };
                        let from = match time::Date::parse(from, &date_format) {
                            Ok(date) => date,
                            Err(_) => return Ok(bad_request("sync dates must use YYYY-MM-DD")),
                        };
                        let to = match time::Date::parse(to, &date_format) {
                            Ok(date) => date,
                            Err(_) => return Ok(bad_request("sync dates must use YYYY-MM-DD")),
                        };
                        if to < from {
                            return Ok(bad_request("to must be on or after from"));
                        }
                        Some((from, to))
                    }
                    _ => return Ok(bad_request("from and to must be supplied together")),
                };
                let Ok(_guard) = sync_lock.try_lock() else {
                    return Ok(json_response(
                        StatusCode::CONFLICT,
                        &serde_json::json!({"error":"sync_in_progress"}),
                    ));
                };
                let data_dir = store
                    .database_path()
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf();
                let result = match range {
                    Some((from, to)) => {
                        crate::polar::sync::run_cycle_range(store.clone(), &data_dir, from, to)
                            .await
                    }
                    None => crate::polar::sync::run_cycle(store.clone(), &data_dir).await,
                };
                Ok(match result {
                    Ok(status) => {
                        if let Err(error) =
                            crate::scheduler::record_manual_result(&store, status.clone()).await
                        {
                            return Ok(internal_error(error));
                        }
                        json_response(StatusCode::OK, &status)
                    }
                    Err(crate::polar::sync::SyncError::NeedsAuth) => json_response(
                        StatusCode::UNAUTHORIZED,
                        &serde_json::json!({"error":"needs_auth"}),
                    ),
                    Err(crate::polar::sync::SyncError::Forbidden(resource)) => json_response(
                        StatusCode::FORBIDDEN,
                        &serde_json::json!({"error":"permission_error","resource":resource}),
                    ),
                    Err(error) => internal_error(error),
                })
            }
            "/v1/admin/backup" => {
                let Some(destination) = query.get("destination") else {
                    return Ok(bad_request("missing destination"));
                };
                if destination.is_empty() || destination.len() > 4096 {
                    return Ok(bad_request("destination must be 1 to 4096 bytes"));
                }
                let destination = PathBuf::from(destination);
                let result = tokio::task::spawn_blocking(move || {
                    store.backup_to(&destination).map(|()| destination)
                })
                .await;
                Ok(match result {
                    Ok(Ok(destination)) => json_response(
                        StatusCode::CREATED,
                        &serde_json::json!({
                            "status": "created",
                            "destination": destination,
                            "includes_credentials": false
                        }),
                    ),
                    Ok(Err(error)) => bad_request(error.to_string()),
                    Err(error) => internal_error(error),
                })
            }
            "/v1/admin/archive/verify" => {
                let Ok(_guard) = sync_lock.try_lock() else {
                    return Ok(json_response(
                        StatusCode::CONFLICT,
                        &serde_json::json!({"error":"sync_in_progress"}),
                    ));
                };
                let archive_path = store.archive_path().to_path_buf();
                let result =
                    tokio::task::spawn_blocking(move || crate::archive::verify_all(&archive_path))
                        .await;
                Ok(match result {
                    Ok(Ok(summary)) => json_response(
                        StatusCode::OK,
                        &serde_json::json!({
                            "verified": summary.verified,
                            "failed": summary.failed
                        }),
                    ),
                    Ok(Err(error)) => internal_error(error),
                    Err(error) => internal_error(error),
                })
            }
            "/v1/admin/archive/diagnose" => {
                let older_than_seconds = match parse_admin_duration(&query) {
                    Ok(value) => value,
                    Err(message) => return Ok(bad_request(message)),
                };
                let Ok(_guard) = sync_lock.try_lock() else {
                    return Ok(json_response(
                        StatusCode::CONFLICT,
                        &serde_json::json!({"error":"sync_in_progress"}),
                    ));
                };
                let archive = crate::archive::ArchiveStore::new(store.archive_path().to_path_buf());
                let result = tokio::task::spawn_blocking(move || {
                    let mut issues = Vec::with_capacity(1000);
                    let summary = archive.diagnose(
                        std::time::Duration::from_secs(older_than_seconds),
                        |issue| {
                            if issues.len() < 1000 {
                                issues.push(serde_json::json!({
                                    "kind": format!("{:?}", issue.kind),
                                    "path": issue.path,
                                    "bytes": issue.bytes
                                }));
                            }
                        },
                    )?;
                    Ok::<_, crate::archive::ArchiveError>((summary, issues))
                })
                .await;
                Ok(match result {
                    Ok(Ok((summary, issues))) => json_response(
                        StatusCode::OK,
                        &serde_json::json!({
                            "stale_parts": summary.stale_parts,
                            "stale_part_bytes": summary.stale_part_bytes,
                            "orphan_objects": summary.orphan_objects,
                            "orphan_object_bytes": summary.orphan_object_bytes,
                            "issues": issues,
                            "issues_truncated": summary.stale_parts.saturating_add(summary.orphan_objects) > 1000
                        }),
                    ),
                    Ok(Err(error)) => internal_error(error),
                    Err(error) => internal_error(error),
                })
            }
            "/v1/admin/archive/cleanup" => {
                let older_than_seconds = match parse_admin_duration(&query) {
                    Ok(value) => value,
                    Err(message) => return Ok(bad_request(message)),
                };
                let apply = match query.get("apply").map(String::as_str) {
                    None | Some("false") | Some("0") => false,
                    Some("true") | Some("1") => true,
                    Some(_) => return Ok(bad_request("apply must be true or false")),
                };
                let Ok(_guard) = sync_lock.try_lock() else {
                    return Ok(json_response(
                        StatusCode::CONFLICT,
                        &serde_json::json!({"error":"sync_in_progress"}),
                    ));
                };
                let archive = crate::archive::ArchiveStore::new(store.archive_path().to_path_buf());
                let result = tokio::task::spawn_blocking(move || {
                    let plan = archive
                        .preview_cleanup(std::time::Duration::from_secs(older_than_seconds))?;
                    let summary = plan.summary();
                    let removed = if apply {
                        Some(archive.apply_cleanup(&plan)?)
                    } else {
                        None
                    };
                    Ok::<_, crate::archive::ArchiveError>((summary, removed))
                })
                .await;
                Ok(match result {
                    Ok(Ok((summary, removed))) => json_response(
                        StatusCode::OK,
                        &serde_json::json!({
                            "dry_run": !apply,
                            "stale_parts": summary.stale_parts,
                            "stale_part_bytes": summary.stale_part_bytes,
                            "orphan_objects": summary.orphan_objects,
                            "orphan_object_bytes": summary.orphan_object_bytes,
                            "removed": removed.map(|value| serde_json::json!({
                                "parts": value.removed_parts,
                                "part_bytes": value.removed_part_bytes,
                                "objects": value.removed_objects,
                                "object_bytes": value.removed_object_bytes
                            }))
                        }),
                    ),
                    Ok(Err(error)) => bad_request(error.to_string()),
                    Err(error) => internal_error(error),
                })
            }
            "/v1/admin/replay" => {
                let Ok(_guard) = sync_lock.try_lock() else {
                    return Ok(json_response(
                        StatusCode::CONFLICT,
                        &serde_json::json!({"error":"sync_in_progress"}),
                    ));
                };
                let resource = query.get("resource").cloned();
                if resource
                    .as_ref()
                    .is_some_and(|value| value.is_empty() || value.len() > 128)
                {
                    return Ok(bad_request("resource must be 1 to 128 bytes"));
                }
                let data_dir = store
                    .database_path()
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf();
                let credentials = match crate::polar::credentials::PolarCredentials::load(&data_dir)
                {
                    Ok(credentials) => credentials,
                    Err(error) => return Ok(internal_error(error)),
                };
                let user_id =
                    credentials.map_or_else(|| "polar-local".to_owned(), |value| value.user_id);
                let result = tokio::task::spawn_blocking(move || {
                    crate::ingest::replay_archive(&store, resource.as_deref(), &user_id, None)
                })
                .await;
                Ok(match result {
                    Ok(Ok(processed)) => {
                        json_response(StatusCode::OK, &serde_json::json!({"processed": processed}))
                    }
                    Ok(Err(error)) => internal_error(error),
                    Err(error) => internal_error(error),
                })
            }
            _ => Ok(json_response(
                StatusCode::NOT_FOUND,
                &serde_json::json!({"error": "not_found"}),
            )),
        };
    }
    if request.method() != Method::GET {
        return Ok(json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            &serde_json::json!({"error": "read_only_api"}),
        ));
    }

    let path = request.uri().path().to_owned();
    let raw_query = request.uri().query().unwrap_or_default();
    if raw_query.len() > MAX_QUERY_BYTES {
        return Ok(bad_request("query string exceeds 16 KiB"));
    }
    let query = match parse_query(raw_query) {
        Ok(query) => query,
        Err(message) => return Ok(bad_request(message)),
    };
    if path == "/v1/exercise-export" {
        let exercise_id = match query.get("exercise_id") {
            Some(value) if !value.is_empty() && value.len() <= 512 => value.clone(),
            _ => return Ok(bad_request("exercise_id must be 1 to 512 bytes")),
        };
        let format = match query.get("format").map(String::as_str) {
            Some("fit") => "fit",
            Some("tcx") => "tcx",
            Some("gpx") => "gpx",
            _ => return Ok(bad_request("format must be fit, tcx, or gpx")),
        };
        let store_for_lookup = store.clone();
        let exercise_for_lookup = exercise_id.clone();
        let format_for_lookup = format.to_owned();
        let prepared = tokio::task::spawn_blocking(move || {
            prepare_exercise_export(&store_for_lookup, &exercise_for_lookup, &format_for_lookup)
        })
        .await;
        let (archive, receipt, filename) = match prepared {
            Ok(Ok(Some(value))) => value,
            Ok(Ok(None)) => {
                return Ok(json_response(
                    StatusCode::NOT_FOUND,
                    &serde_json::json!({"error":"exercise_export_not_found"}),
                ));
            }
            Ok(Err(error)) => return Ok(internal_error(error)),
            Err(error) => return Ok(internal_error(error)),
        };
        return Ok(stream_exercise_export(archive, receipt, filename));
    }
    if let Some(encoded_id) = path.strip_prefix("/v1/records/") {
        if encoded_id.is_empty() || encoded_id.contains('/') {
            return Ok(bad_request(
                "record id must be one percent-encoded path segment",
            ));
        }
        let Some(id) = percent_decode_component(encoded_id, false) else {
            return Ok(bad_request("invalid percent-encoding in record id"));
        };
        if id.is_empty() || id.len() > 512 {
            return Ok(bad_request("record id must be 1 to 512 bytes"));
        }
        let lookup_id = id.clone();
        let result = tokio::task::spawn_blocking(move || store.get_record(&lookup_id)).await;
        return Ok(match result {
            Ok(Ok(Some(bytes))) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(record) => json_response(
                    StatusCode::OK,
                    &serde_json::json!({"id": id, "record": record}),
                ),
                Err(error) => internal_error(error),
            },
            Ok(Ok(None)) => json_response(
                StatusCode::NOT_FOUND,
                &serde_json::json!({"error": "record_not_found"}),
            ),
            Ok(Err(error)) => internal_error(error),
            Err(error) => internal_error(error),
        });
    }
    match path.as_str() {
        "/v1/status" => {
            let data_dir = store
                .database_path()
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            let archive_path = store.archive_path().to_path_buf();
            let result = tokio::task::spawn_blocking(move || {
                let archive = crate::archive::ArchiveStore::new(archive_path.clone());
                let sidecar_count = archive.receipts()?.try_fold(0_u64, |count, receipt| {
                    receipt?;
                    Ok::<_, crate::archive::ArchiveError>(count.saturating_add(1))
                })?;
                let credentials = crate::polar::credentials::PolarCredentials::load(&data_dir)?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(serde_json::json!({
                    "database": store.database_path(),
                    "archive": archive_path,
                    "archive_receipt_sidecars": sidecar_count,
                    "receipts_registered_in_database": store.archive_count()?,
                    "provider_records": store.record_count()?,
                    "normalized_measurements": store.measurement_count()?,
                    "polar": if credentials.is_some() { "authorized" } else { "not authorized" }
                }))
            })
            .await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/health" => {
            let result = tokio::task::spawn_blocking(move || {
                Ok::<_, crate::storage::StorageError>(serde_json::json!({
                    "status": "ok",
                    "schema_version": store.schema_version()?,
                    "provider_records": store.record_count()?,
                    "measurements": store.measurement_count()?
                }))
            })
            .await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/sources" => {
            let result =
                spawn_blocking_cancellable(move |cancelled| scan_sources(&store, &cancelled)).await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/sync/status" => {
            let result = tokio::task::spawn_blocking(move || {
                store
                    .get_sync_state("service/polar-sync")
                    .and_then(|status| match status {
                        Some(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes)
                            .map_err(crate::storage::StorageError::from),
                        None => Ok(serde_json::json!({
                            "state": "not_configured",
                            "detail": "service scheduler has not started"
                        })),
                    })
            })
            .await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/metrics" => {
            let after = match parse_cursor(&query) {
                Ok(cursor) => cursor,
                Err(message) => return Ok(bad_request(message)),
            };
            let limit = match parse_limit(&query) {
                Ok(value) => value,
                Err(message) => return Ok(bad_request(message)),
            };
            let result = spawn_blocking_cancellable(move |cancelled| {
                metric_catalog(&store, after.as_deref(), limit, &cancelled)
            })
            .await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/records" => {
            let filters = match RecordFilters::parse(&query) {
                Ok(filters) => filters,
                Err(message) => return Ok(bad_request(message)),
            };
            let cursor = match parse_cursor(&query) {
                Ok(cursor) => cursor,
                Err(message) => return Ok(bad_request(message)),
            };
            let limit = match parse_limit(&query) {
                Ok(value) => value,
                Err(message) => return Ok(bad_request(message)),
            };
            let result = spawn_blocking_cancellable(move |cancelled| {
                filtered_records(&store, &filters, cursor.as_deref(), limit, &cancelled)
            })
            .await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/daily" => {
            let (from, to) = match parse_date_bounds(&query) {
                Ok(bounds) => bounds,
                Err(message) => return Ok(bad_request(message)),
            };
            let cursor = match parse_cursor(&query) {
                Ok(cursor) => cursor,
                Err(message) => return Ok(bad_request(message)),
            };
            let limit = match parse_limit(&query) {
                Ok(value) => value,
                Err(message) => return Ok(bad_request(message)),
            };
            let result = spawn_blocking_cancellable(move |cancelled| {
                daily_records(&store, &from, &to, cursor.as_deref(), limit, &cancelled)
            })
            .await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/export" => {
            let export_query = match parse_export_query(&query) {
                Ok(value) => value,
                Err(message) => return Ok(bad_request(message)),
            };
            let result =
                tokio::task::spawn_blocking(move || export_page(&store, &export_query)).await;
            Ok(match result {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        "/v1/series" => {
            let required = ["user_id", "source", "metric", "from", "to"];
            if required.iter().any(|key| !query.contains_key(*key)) {
                return Ok(bad_request("missing required query parameter"));
            }
            if required[..3]
                .iter()
                .any(|key| query[*key].is_empty() || query[*key].len() > 256)
            {
                return Ok(bad_request(
                    "user_id, source, and metric must be 1 to 256 bytes",
                ));
            }
            let from = match parse_utc_micros(&query["from"]) {
                Ok(value) => value,
                Err(message) => return Ok(bad_request(message)),
            };
            let to = match parse_utc_micros(&query["to"]) {
                Ok(value) => value,
                Err(message) => return Ok(bad_request(message)),
            };
            if from >= to {
                return Ok(bad_request(
                    "from must be earlier than to; ranges are half-open [from,to)",
                ));
            }
            let limit = match parse_limit(&query) {
                Ok(value) => value,
                Err(message) => return Ok(bad_request(message)),
            };
            let context = match query.get("context") {
                None => None,
                Some(value) => match parse_context(value) {
                    Some(value) => Some(value),
                    None => return Ok(bad_request("unknown measurement context")),
                },
            };
            let user_id = query["user_id"].clone();
            let source = query["source"].clone();
            let metric = query["metric"].clone();
            let cursor = match parse_cursor(&query) {
                Ok(cursor) => cursor,
                Err(message) => return Ok(bad_request(message)),
            };
            let downsample_buckets = match query.get("downsample_buckets") {
                Some(value) => match value.parse::<usize>() {
                    Ok(value) if (1..=MAX_DOWNSAMPLE_BUCKETS).contains(&value) => Some(value),
                    _ => {
                        return Ok(bad_request("downsample_buckets must be between 1 and 250"));
                    }
                },
                None => None,
            };
            if downsample_buckets.is_some() && cursor.is_some() {
                return Ok(bad_request("cursor cannot be used with downsample_buckets"));
            }
            let result = tokio::task::spawn_blocking(move || {
                if let Some(bucket_count) = downsample_buckets {
                    return downsample_measurements(
                        &store,
                        &user_id,
                        &source,
                        &metric,
                        context.as_ref(),
                        from,
                        to,
                        bucket_count,
                    );
                }
                let page = store
                    .measurement_page(MeasurementQuery {
                        user_id: &user_id,
                        source: &source,
                        metric: &metric,
                        context: context.as_ref(),
                        from_utc_micros: from,
                        to_utc_micros: to,
                        limit,
                        after: cursor.as_deref(),
                    })
                    .map_err(|error| DownsampleError::BadRequest(error.to_string()))?;
                serde_json::to_value(page)
                    .map_err(crate::storage::StorageError::from)
                    .map_err(DownsampleError::Storage)
            })
            .await;
            Ok(match result {
                Ok(Ok(value)) => json_response(StatusCode::OK, &value),
                Ok(Err(DownsampleError::TooManyRows)) => bad_request(
                    "downsample range exceeds the 1000000-row scan limit; narrow the time range",
                ),
                Ok(Err(DownsampleError::ResponseTooLarge)) => bad_request(
                    "downsample response exceeds the 5 MiB API page limit; use fewer buckets",
                ),
                Ok(Err(DownsampleError::BadRequest(message))) => bad_request(&message),
                Ok(Err(DownsampleError::Storage(error))) => internal_error(error),
                Err(error) => internal_error(error),
            })
        }
        _ => Ok(json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": "not_found"}),
        )),
    }
}

#[derive(Debug)]
enum DownsampleError {
    Storage(crate::storage::StorageError),
    TooManyRows,
    ResponseTooLarge,
    BadRequest(String),
}

#[derive(Serialize)]
struct DownsampledPoint<'a> {
    bucket_index: usize,
    bucket_start_utc_micros: i64,
    selection: &'static str,
    measurement: &'a crate::model::Measurement,
}

#[derive(Default)]
struct BucketExtrema {
    first: Option<crate::model::Measurement>,
    minimum: Option<crate::model::Measurement>,
    maximum: Option<crate::model::Measurement>,
    last: Option<crate::model::Measurement>,
}

#[allow(clippy::too_many_arguments)]
fn downsample_measurements(
    store: &Store,
    user_id: &str,
    source: &str,
    metric: &str,
    context: Option<&MeasurementContext>,
    from: i64,
    to: i64,
    bucket_count: usize,
) -> Result<serde_json::Value, DownsampleError> {
    downsample_measurements_with_scan_limit(
        store,
        user_id,
        source,
        metric,
        context,
        from,
        to,
        bucket_count,
        MAX_DOWNSAMPLE_SCAN_ROWS,
    )
}

#[allow(clippy::too_many_arguments)]
fn downsample_measurements_with_scan_limit(
    store: &Store,
    user_id: &str,
    source: &str,
    metric: &str,
    context: Option<&MeasurementContext>,
    from: i64,
    to: i64,
    bucket_count: usize,
    max_scan_rows: usize,
) -> Result<serde_json::Value, DownsampleError> {
    if from >= to {
        return Err(DownsampleError::BadRequest(
            "from must be earlier than to; ranges are half-open [from,to)".into(),
        ));
    }
    if !(1..=MAX_DOWNSAMPLE_BUCKETS).contains(&bucket_count) {
        return Err(DownsampleError::BadRequest(
            "downsample_buckets must be between 1 and 250".into(),
        ));
    }
    let span = i128::from(to) - i128::from(from);
    let bucket_count_i128 = bucket_count as i128;
    let mut buckets = (0..bucket_count)
        .map(|_| BucketExtrema::default())
        .collect::<Vec<_>>();
    let mut after = None::<String>;
    let mut scanned = 0usize;

    loop {
        // Read at most one look-ahead row beyond the scan budget so the API
        // can distinguish an exactly complete range from a truncated one.
        let page_limit = MAX_PAGE_ROWS.min(max_scan_rows.saturating_sub(scanned).saturating_add(1));
        let page = store
            .measurement_page(MeasurementQuery {
                user_id,
                source,
                metric,
                context,
                from_utc_micros: from,
                to_utc_micros: to,
                limit: page_limit,
                after: after.as_deref(),
            })
            .map_err(DownsampleError::Storage)?;
        if page.rows.is_empty() {
            break;
        }
        scanned = scanned.saturating_add(page.rows.len());
        if scanned > max_scan_rows || (scanned == max_scan_rows && page.next_cursor.is_some()) {
            return Err(DownsampleError::TooManyRows);
        }

        for measurement in page.rows {
            let timestamp = measurement.time.start.utc_micros().ok_or_else(|| {
                DownsampleError::Storage(crate::storage::StorageError::CorruptMeasurementTime)
            })?;
            let offset = i128::from(timestamp) - i128::from(from);
            let index = ((offset * bucket_count_i128) / span) as usize;
            let bucket = &mut buckets[index.min(bucket_count - 1)];

            if bucket
                .first
                .as_ref()
                .is_none_or(|current| measurement_order(&measurement) < measurement_order(current))
            {
                bucket.first = Some(measurement.clone());
            }
            if bucket
                .minimum
                .as_ref()
                .is_none_or(|current| compare_numeric(&measurement.value, &current.value).is_lt())
            {
                bucket.minimum = Some(measurement.clone());
            }
            if bucket
                .maximum
                .as_ref()
                .is_none_or(|current| compare_numeric(&measurement.value, &current.value).is_gt())
            {
                bucket.maximum = Some(measurement.clone());
            }
            // The keyset scan groups contexts before timestamps when no
            // context filter is supplied, so determine the temporal last
            // point explicitly rather than relying on scan order.
            if bucket
                .last
                .as_ref()
                .is_none_or(|current| measurement_order(&measurement) > measurement_order(current))
            {
                bucket.last = Some(measurement);
            }
        }

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        after = Some(next_cursor);
    }

    let mut selected = Vec::with_capacity(bucket_count.saturating_mul(4));
    for (bucket_index, bucket) in buckets.iter().enumerate() {
        let bucket_start = i128::from(from) + (span * bucket_index as i128) / bucket_count_i128;
        for (selection, measurement) in [
            ("first", bucket.first.as_ref()),
            ("min", bucket.minimum.as_ref()),
            ("max", bucket.maximum.as_ref()),
            ("last", bucket.last.as_ref()),
        ] {
            if let Some(measurement) = measurement {
                selected.push(DownsampledPoint {
                    bucket_index,
                    bucket_start_utc_micros: bucket_start as i64,
                    selection,
                    measurement,
                });
            }
        }
    }
    selected.sort_by(|left, right| {
        let left_time = left.measurement.time.start.utc_micros().unwrap_or_default();
        let right_time = right
            .measurement
            .time
            .start
            .utc_micros()
            .unwrap_or_default();
        left_time
            .cmp(&right_time)
            .then(left.bucket_index.cmp(&right.bucket_index))
            .then(selection_order(left.selection).cmp(&selection_order(right.selection)))
    });

    let response = serde_json::json!({
        "downsampled": true,
        "method": "first_min_max_last",
        "bucket_count": bucket_count,
        "scanned_rows": scanned,
        "rows": selected,
        "next_cursor": null
    });
    let encoded = serde_json::to_vec(&response)
        .map_err(|error| DownsampleError::Storage(crate::storage::StorageError::from(error)))?;
    if encoded.len() > MAX_API_PAGE_BYTES {
        return Err(DownsampleError::ResponseTooLarge);
    }
    serde_json::from_slice(&encoded)
        .map_err(|error| DownsampleError::Storage(crate::storage::StorageError::from(error)))
}

fn measurement_order(measurement: &crate::model::Measurement) -> (i64, &str) {
    (
        measurement.time.start.utc_micros().unwrap_or_default(),
        measurement.id.as_str(),
    )
}

fn selection_order(selection: &str) -> u8 {
    match selection {
        "first" => 0,
        "min" => 1,
        "max" => 2,
        "last" => 3,
        _ => 4,
    }
}

/// Compare validated JSON numeric lexemes without rounding them through f64.
fn compare_numeric(
    left: &crate::model::NumericValue,
    right: &crate::model::NumericValue,
) -> std::cmp::Ordering {
    let left = DecimalParts::parse(left.as_str());
    let right = DecimalParts::parse(right.as_str());
    if left.zero || right.zero {
        return match (left.zero, right.zero) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => {
                if right.negative {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, true) => {
                if left.negative {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, false) => unreachable!(),
        };
    }
    match left.negative.cmp(&right.negative) {
        std::cmp::Ordering::Equal => {
            let magnitude = left
                .decimal_exponent
                .cmp(&right.decimal_exponent)
                .then_with(|| compare_padded_digits(&left.digits, &right.digits));
            if left.negative {
                magnitude.reverse()
            } else {
                magnitude
            }
        }
        std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
        std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
    }
}

struct DecimalParts {
    negative: bool,
    zero: bool,
    digits: String,
    decimal_exponent: i64,
}

impl DecimalParts {
    fn parse(value: &str) -> Self {
        let (negative, unsigned) = value
            .strip_prefix('-')
            .map_or((false, value), |rest| (true, rest));
        let (mantissa, exponent) = unsigned
            .split_once(['e', 'E'])
            .map_or((unsigned, 0), |(mantissa, exponent)| {
                (mantissa, parse_saturated_i64(exponent))
            });
        let fractional_digits = mantissa
            .find('.')
            .map_or(0, |point| mantissa.len().saturating_sub(point + 1));
        let mut digits = mantissa
            .chars()
            .filter(|character| *character != '.')
            .collect::<String>();
        let leading_zeroes = digits.bytes().take_while(|byte| *byte == b'0').count();
        digits.drain(..leading_zeroes);
        if digits.is_empty() {
            return Self {
                negative: false,
                zero: true,
                digits,
                decimal_exponent: 0,
            };
        }
        let trailing_zeroes = digits
            .bytes()
            .rev()
            .take_while(|byte| *byte == b'0')
            .count();
        digits.truncate(digits.len() - trailing_zeroes);
        let decimal_exponent = exponent
            .saturating_sub(fractional_digits as i64)
            .saturating_add((digits.len() + trailing_zeroes) as i64);
        Self {
            negative,
            zero: false,
            digits,
            decimal_exponent,
        }
    }
}

fn parse_saturated_i64(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or_else(|_| {
        if value.starts_with('-') {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

fn compare_padded_digits(left: &str, right: &str) -> std::cmp::Ordering {
    let width = left.len().max(right.len());
    for index in 0..width {
        let left_digit = left.as_bytes().get(index).copied().unwrap_or(b'0');
        let right_digit = right.as_bytes().get(index).copied().unwrap_or(b'0');
        match left_digit.cmp(&right_digit) {
            std::cmp::Ordering::Equal => {}
            order => return order,
        }
    }
    std::cmp::Ordering::Equal
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExportFormat {
    Jsonl,
    Csv,
}

#[derive(Clone, Debug)]
struct ExportQuery {
    user_id: String,
    source: String,
    metric: String,
    from: i64,
    to: i64,
    limit: usize,
    cursor: Option<String>,
    format: ExportFormat,
}

#[derive(Clone, Debug)]
struct RecordFilters {
    source: Option<String>,
    kind: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
}

impl RecordFilters {
    fn parse(query: &BTreeMap<String, String>) -> Result<Self, &'static str> {
        let (from, to) = parse_optional_utc_bounds(query)?;
        let source = nonempty_query(query, "source")?;
        let kind = nonempty_query(query, "kind")?;
        Ok(Self {
            source,
            kind,
            from,
            to,
        })
    }

    fn matches(&self, key: &str, payload: &serde_json::Value) -> bool {
        let key_source = key.split('/').next().unwrap_or_default();
        if self
            .source
            .as_deref()
            .is_some_and(|wanted| wanted != key_source)
        {
            return false;
        }
        let resource = payload
            .get("resource")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if self
            .kind
            .as_deref()
            .is_some_and(|wanted| !kind_matches(wanted, resource))
        {
            return false;
        }
        if self.from.is_none() && self.to.is_none() {
            return true;
        }
        let record = payload.get("record").unwrap_or(payload);
        record_overlaps_range(
            record,
            self.from.unwrap_or(i64::MIN),
            self.to.unwrap_or(i64::MAX),
        )
    }
}

fn parse_optional_utc_bounds(
    query: &BTreeMap<String, String>,
) -> Result<(Option<i64>, Option<i64>), &'static str> {
    match (query.get("from"), query.get("to")) {
        (None, None) => Ok((None, None)),
        (Some(_), None) | (None, Some(_)) => Err("from and to must be provided together"),
        (Some(from), Some(to)) => {
            let from = parse_utc_micros(from)?;
            let to = parse_utc_micros(to)?;
            if from >= to {
                return Err("from must be earlier than to; ranges are half-open [from,to)");
            }
            Ok((Some(from), Some(to)))
        }
    }
}

fn nonempty_query(
    query: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<String>, &'static str> {
    match query.get(key) {
        None => Ok(None),
        Some(value) if value.trim().is_empty() => Err("filter values must not be empty"),
        Some(value) if value.len() > 256 => Err("filter value is too long"),
        Some(value) => Ok(Some(value.clone())),
    }
}

fn kind_matches(wanted: &str, resource: &str) -> bool {
    let wanted = wanted.to_ascii_lowercase().replace('_', "-");
    let resource = resource.to_ascii_lowercase().replace('_', "-");
    let kind = match resource.as_str() {
        "activities" | "activity" | "daily-activity" => "daily-activity",
        "continuous-heart-rate" => "continuous-heart-rate",
        "exercises" | "exercise" => "exercise",
        "nightly-recharge" | "sleep" | "sleepwise" | "cardio-load" | "profile" => resource.as_str(),
        _ => resource.as_str(),
    };
    wanted == kind || wanted == resource
}

fn record_overlaps_range(record: &serde_json::Value, from: i64, to: i64) -> bool {
    let start = [
        "start_time",
        "start-time",
        "sleep_start_time",
        "timestamp",
        "date",
    ]
    .iter()
    .filter_map(|key| record.get(*key).and_then(serde_json::Value::as_str))
    .find_map(|value| parse_utc_micros(value).ok());
    let end = ["end_time", "end-time", "sleep_end_time"]
        .iter()
        .filter_map(|key| record.get(*key).and_then(serde_json::Value::as_str))
        .find_map(|value| parse_utc_micros(value).ok());
    match (start, end) {
        (Some(start), Some(end)) => start < to && end > from,
        (Some(start), None) => start >= from && start < to,
        // If only the end is known, the record may span the rest of the
        // requested interval. An end at `from` is outside the half-open range;
        // an end at or after `to` can still overlap it.
        (None, Some(end)) => end > from,
        (None, None) => false,
    }
}

fn parse_date_bounds(query: &BTreeMap<String, String>) -> Result<(String, String), &'static str> {
    let from = query.get("from").ok_or("missing from date")?;
    let to = query.get("to").ok_or("missing to date")?;
    let format = time::format_description::parse_borrowed::<1>("[year]-[month]-[day]")
        .map_err(|_| "invalid date format")?;
    let from_date = time::Date::parse(from, &format).map_err(|_| "dates must use YYYY-MM-DD")?;
    let to_date = time::Date::parse(to, &format).map_err(|_| "dates must use YYYY-MM-DD")?;
    if from_date >= to_date {
        return Err("from must be earlier than to; date ranges are half-open [from,to)");
    }
    Ok((from.clone(), to.clone()))
}

fn parse_export_query(query: &BTreeMap<String, String>) -> Result<ExportQuery, &'static str> {
    let user_id = query
        .get("user_id")
        .filter(|v| !v.is_empty())
        .ok_or("missing user_id")?
        .clone();
    let source = query
        .get("source")
        .filter(|v| !v.is_empty())
        .ok_or("missing source")?
        .clone();
    let metric = query
        .get("metric")
        .filter(|v| !v.is_empty())
        .ok_or("missing metric")?
        .clone();
    if user_id.len() > 256 || source.len() > 256 || metric.len() > 256 {
        return Err("user_id, source, and metric must not exceed 256 bytes");
    }
    let from = parse_utc_micros(query.get("from").ok_or("missing from")?)?;
    let to = parse_utc_micros(query.get("to").ok_or("missing to")?)?;
    if from >= to {
        return Err("from must be earlier than to; ranges are half-open [from,to)");
    }
    let limit = parse_limit(query)?;
    let format = match query.get("format").map(String::as_str).unwrap_or("jsonl") {
        "jsonl" => ExportFormat::Jsonl,
        "csv" => ExportFormat::Csv,
        _ => return Err("format must be jsonl or csv"),
    };
    Ok(ExportQuery {
        user_id,
        source,
        metric,
        from,
        to,
        limit,
        cursor: parse_cursor(query)?,
        format,
    })
}

fn export_page(
    store: &Store,
    query: &ExportQuery,
) -> Result<ApiResponse, crate::storage::StorageError> {
    let page = store.measurement_page(MeasurementQuery {
        user_id: &query.user_id,
        source: &query.source,
        metric: &query.metric,
        context: None,
        from_utc_micros: query.from,
        to_utc_micros: query.to,
        limit: query.limit,
        after: query.cursor.as_deref(),
    })?;
    let (content_type, bytes) = match query.format {
        ExportFormat::Jsonl => {
            let mut bytes = Vec::with_capacity(256 * page.rows.len().min(1000));
            for row in &page.rows {
                serde_json::to_writer(&mut bytes, row)?;
                bytes.push(b'\n');
            }
            ("application/x-ndjson", bytes)
        }
        ExportFormat::Csv => ("text/csv; charset=utf-8", measurements_csv(&page.rows)),
    };
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, content_type);
    if let Some(cursor) = page.next_cursor {
        builder = builder.header("x-next-cursor", cursor);
    }
    Ok(builder
        .body(
            Full::new(Bytes::from(bytes))
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .expect("static export headers are valid"))
}

fn prepare_exercise_export(
    store: &Store,
    exercise_id: &str,
    format: &str,
) -> Result<
    Option<(
        crate::archive::ArchiveStore,
        crate::archive::ArchiveReceipt,
        String,
    )>,
    String,
> {
    if !matches!(format, "fit" | "tcx" | "gpx") {
        return Err("format must be fit, tcx, or gpx".to_owned());
    }
    let resource = format!("exercise-{format}");
    let key = format!("polar/{resource}/{exercise_id}");
    let Some(bytes) = store.get_record(&key).map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    let index: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if index.get("resource").and_then(serde_json::Value::as_str) != Some(resource.as_str())
        || index.get("external_id").and_then(serde_json::Value::as_str) != Some(exercise_id)
    {
        return Err("exercise export index does not match its key".to_owned());
    }
    let Some(archive_ref) = index.get("archive_ref").and_then(serde_json::Value::as_str) else {
        return Err("exercise export index has no archive reference".to_owned());
    };
    let archive = crate::archive::ArchiveStore::new(store.archive_path().to_path_buf());
    let mut matched = None;
    for entry in archive.receipts().map_err(|error| error.to_string())? {
        let receipt = entry.map_err(|error| error.to_string())?;
        if receipt.source == "polar"
            && receipt.resource == resource
            && receipt
                .request
                .get("exercise_id")
                .is_some_and(|id| id == exercise_id)
            && receipt.content_hash == archive_ref
        {
            matched = Some(receipt);
            break;
        }
    }
    let Some(receipt) = matched else {
        return Err("exercise export archive receipt is missing".to_owned());
    };
    if !(200..300).contains(&receipt.status) {
        return Err("exercise export archive receipt is not successful".to_owned());
    }
    archive
        .verify(&receipt)
        .map_err(|error| error.to_string())?;
    let safe_id = exercise_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || "-_".contains(character) {
                character
            } else {
                '_'
            }
        })
        .take(128)
        .collect::<String>();
    Ok(Some((archive, receipt, format!("{safe_id}.{format}"))))
}

fn stream_exercise_export(
    archive: crate::archive::ArchiveStore,
    receipt: crate::archive::ArchiveReceipt,
    filename: String,
) -> ApiResponse {
    let length = receipt.uncompressed_bytes;
    let (body, _worker) = exercise_export_body(archive, receipt);
    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
        .header(hyper::header::CONTENT_LENGTH, length.to_string())
        .header(
            hyper::header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .body(body)
        .expect("static binary export headers are valid")
}

fn exercise_export_body(
    archive: crate::archive::ArchiveStore,
    receipt: crate::archive::ArchiveReceipt,
) -> (UnsyncBoxBody<Bytes, io::Error>, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = tokio::sync::mpsc::channel(ARCHIVE_STREAM_QUEUE_CHUNKS);
    let worker = tokio::task::spawn_blocking(move || {
        let mut reader = match archive.read(&receipt) {
            Ok(reader) => reader,
            Err(error) => {
                let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
                return;
            }
        };
        loop {
            let mut buffer = vec![0_u8; ARCHIVE_STREAM_CHUNK_BYTES];
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    buffer.truncate(count);
                    if sender.blocking_send(Ok(Bytes::from(buffer))).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.blocking_send(Err(error));
                    break;
                }
            }
        }
    });
    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| {
            let frame = item.map(Frame::data);
            (frame, receiver)
        })
    });
    let body = StreamBody::new(stream).boxed_unsync();
    (body, worker)
}

fn measurements_csv(rows: &[crate::model::Measurement]) -> Vec<u8> {
    let mut csv = String::from(
        "id,user_id,source,metric,value,unit,start_utc_micros,end_utc_micros,aggregation,context,quality\n",
    );
    for row in rows {
        let start = row
            .time
            .start
            .utc_micros()
            .map(|v| v.to_string())
            .unwrap_or_default();
        let end = row
            .time
            .end
            .as_ref()
            .and_then(|v| v.utc_micros())
            .map(|v| v.to_string())
            .unwrap_or_default();
        let fields = [
            row.id.as_str().to_owned(),
            row.provenance.user_id.as_str().to_owned(),
            row.provenance.source.as_str().to_owned(),
            row.metric.as_str().to_owned(),
            row.value.as_str().to_owned(),
            format!("{:?}", row.unit),
            start,
            end,
            format!("{:?}", row.aggregation),
            format!("{:?}", row.context),
            format!("{:?}", row.provenance.quality),
        ];
        for (index, field) in fields.iter().enumerate() {
            if index != 0 {
                csv.push(',');
            }
            csv_field(&mut csv, field);
        }
        csv.push('\n');
    }
    csv.into_bytes()
}

fn csv_field(out: &mut String, value: &str) {
    if value
        .bytes()
        .any(|byte| matches!(byte, b',' | b'"' | b'\n' | b'\r'))
    {
        out.push('"');
        for character in value.chars() {
            if character == '"' {
                out.push('"');
            }
            out.push(character);
        }
        out.push('"');
    } else {
        out.push_str(value);
    }
}

fn filtered_records(
    store: &Store,
    filters: &RecordFilters,
    after: Option<&str>,
    limit: usize,
    cancelled: &AtomicBool,
) -> Result<serde_json::Value, crate::storage::StorageError> {
    let mut cursor = after.map(str::to_owned);
    let mut rows = Vec::with_capacity(limit.min(128));
    let mut scanned = 0usize;
    let mut complete = false;
    let mut response_bytes = 0usize;
    let mut page_full = false;
    while scanned < MAX_RECORDS_SCANNED && rows.len() < limit {
        if cancelled.load(Ordering::Relaxed) {
            break;
        }
        let request_limit = 256.min(MAX_RECORDS_SCANNED - scanned);
        let page = store.record_page(cursor.as_deref(), request_limit)?;
        if page.is_empty() {
            complete = true;
            break;
        }
        for (key, bytes) in page {
            let previous_cursor = cursor.clone();
            cursor = Some(key.clone());
            scanned += 1;
            let payload: serde_json::Value = serde_json::from_slice(&bytes)?;
            if filters.matches(&key, &payload) {
                let row = serde_json::json!({"id": key, "record": payload});
                let row_bytes = serde_json::to_vec(&row)?.len();
                if !rows.is_empty() && response_bytes.saturating_add(row_bytes) > MAX_API_PAGE_BYTES
                {
                    cursor = previous_cursor;
                    page_full = true;
                    break;
                }
                response_bytes = response_bytes.saturating_add(row_bytes);
                rows.push(row);
                if rows.len() == limit {
                    break;
                }
            }
            if scanned == MAX_RECORDS_SCANNED {
                break;
            }
        }
        if page_full {
            break;
        }
        if scanned == MAX_RECORDS_SCANNED {
            break;
        }
    }
    let next_cursor = if complete { None } else { cursor };
    Ok(serde_json::json!({
        "rows": rows,
        "next_cursor": next_cursor,
        "scanned_records": scanned,
        "scan_limited": !complete && scanned >= MAX_RECORDS_SCANNED
    }))
}

fn daily_records(
    store: &Store,
    from: &str,
    to: &str,
    after: Option<&str>,
    limit: usize,
    cancelled: &AtomicBool,
) -> Result<serde_json::Value, crate::storage::StorageError> {
    let mut cursor = after.map(str::to_owned);
    let mut rows = Vec::with_capacity(limit.min(128));
    let mut scanned = 0usize;
    let mut complete = false;
    let mut response_bytes = 0usize;
    let mut page_full = false;
    while scanned < MAX_RECORDS_SCANNED && rows.len() < limit {
        if cancelled.load(Ordering::Relaxed) {
            break;
        }
        let page = store.record_page(cursor.as_deref(), 256.min(MAX_RECORDS_SCANNED - scanned))?;
        if page.is_empty() {
            complete = true;
            break;
        }
        for (key, bytes) in page {
            let previous_cursor = cursor.clone();
            cursor = Some(key.clone());
            scanned += 1;
            let payload: serde_json::Value = serde_json::from_slice(&bytes)?;
            let resource = payload
                .get("resource")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let record = payload.get("record").unwrap_or(&payload);
            let date = daily_date(record);
            if matches!(resource, "activities" | "daily-activity" | "daily_activity")
                && date.is_some_and(|date| date >= from && date < to)
            {
                let row = serde_json::json!({"id": key, "date": date, "record": record});
                let row_bytes = serde_json::to_vec(&row)?.len();
                if !rows.is_empty() && response_bytes.saturating_add(row_bytes) > MAX_API_PAGE_BYTES
                {
                    cursor = previous_cursor;
                    page_full = true;
                    break;
                }
                response_bytes = response_bytes.saturating_add(row_bytes);
                rows.push(row);
                if rows.len() == limit {
                    break;
                }
            }
            if scanned == MAX_RECORDS_SCANNED {
                break;
            }
        }
        if page_full {
            break;
        }
        if scanned == MAX_RECORDS_SCANNED {
            break;
        }
    }
    Ok(serde_json::json!({
        "rows": rows,
        "from": from,
        "to": to,
        "date_semantics": "provider-local calendar labels; no UTC conversion",
        "next_cursor": if complete { None } else { cursor },
        "scanned_records": scanned,
        "scan_limited": !complete && scanned >= MAX_RECORDS_SCANNED
    }))
}

fn daily_date(record: &serde_json::Value) -> Option<&str> {
    let value = record
        .get("date")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            ["start_time", "start-time"]
                .iter()
                .filter_map(|key| record.get(*key).and_then(serde_json::Value::as_str))
                .find_map(|value| value.get(..10))
        })?;
    let format = time::format_description::parse_borrowed::<1>("[year]-[month]-[day]").ok()?;
    time::Date::parse(value, &format).ok()?;
    Some(value)
}

fn metric_catalog(
    store: &Store,
    after: Option<&str>,
    limit: usize,
    cancelled: &AtomicBool,
) -> Result<serde_json::Value, crate::storage::StorageError> {
    let mut cursor: Option<String> = None;
    let mut scanned = 0usize;
    let mut complete = false;
    let mut metrics = BTreeMap::<String, serde_json::Value>::new();
    while scanned < MAX_RECORDS_SCANNED && metrics.len() < MAX_METRIC_CATALOG {
        if cancelled.load(Ordering::Relaxed) {
            break;
        }
        let page = store.record_page(cursor.as_deref(), 256.min(MAX_RECORDS_SCANNED - scanned))?;
        if page.is_empty() {
            complete = true;
            break;
        }
        for (key, bytes) in page {
            cursor = Some(key);
            scanned += 1;
            let payload: serde_json::Value = serde_json::from_slice(&bytes)?;
            let source = cursor
                .as_deref()
                .and_then(|key| key.split('/').next())
                .unwrap_or_default();
            if let Some(items) = payload.get("metrics").and_then(serde_json::Value::as_array) {
                for item in items {
                    let Some(name) = item.get("metric").and_then(serde_json::Value::as_str) else {
                        continue;
                    };
                    let unit = item.get("unit").cloned().unwrap_or(serde_json::Value::Null);
                    let unit_key = serde_json::to_string(&unit).unwrap_or_default();
                    let key = format!("{name}\0{unit_key}");
                    let entry = metrics.entry(key).or_insert_with(|| {
                        serde_json::json!({
                            "metric": name,
                            "unit": unit,
                            "sources": []
                        })
                    });
                    if let Some(sources) = entry
                        .get_mut("sources")
                        .and_then(serde_json::Value::as_array_mut)
                        && !sources.iter().any(|value| value.as_str() == Some(source))
                    {
                        sources.push(serde_json::Value::String(source.to_owned()));
                    }
                }
            }
            if scanned == MAX_RECORDS_SCANNED || metrics.len() >= MAX_METRIC_CATALOG {
                break;
            }
        }
        if scanned == MAX_RECORDS_SCANNED || metrics.len() >= MAX_METRIC_CATALOG {
            break;
        }
    }
    let mut rows = metrics
        .into_iter()
        .filter(|(key, _)| after.is_none_or(|after| key.as_str() > after))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = has_more
        .then(|| rows.last().map(|(key, _)| key.clone()))
        .flatten();
    Ok(serde_json::json!({
        "metrics": rows.into_iter().map(|(_, value)| value).collect::<Vec<_>>(),
        "next_cursor": next_cursor,
        "complete": complete,
        "scanned_records": scanned,
        "scan_limited": !complete && scanned >= MAX_RECORDS_SCANNED
    }))
}

fn scan_sources(
    store: &Store,
    cancelled: &AtomicBool,
) -> Result<serde_json::Value, crate::storage::StorageError> {
    let mut cursor: Option<String> = None;
    let mut scanned = 0usize;
    let mut sources = std::collections::BTreeSet::new();
    let mut complete = false;
    while scanned < MAX_RECORDS_SCANNED {
        if cancelled.load(Ordering::Relaxed) {
            break;
        }
        let page = store.record_page(cursor.as_deref(), 256.min(MAX_RECORDS_SCANNED - scanned))?;
        if page.is_empty() {
            complete = true;
            break;
        }
        for (key, _) in page {
            cursor = Some(key.clone());
            scanned += 1;
            if let Some(source) = key.split('/').next() {
                sources.insert(source.to_owned());
            }
            if scanned == MAX_RECORDS_SCANNED {
                break;
            }
        }
        if scanned == MAX_RECORDS_SCANNED {
            break;
        }
    }
    Ok(serde_json::json!({
        "sources": sources,
        "complete": complete,
        "scanned_records": scanned,
        "scan_limited": !complete && scanned >= MAX_RECORDS_SCANNED
    }))
}

fn parse_limit(query: &std::collections::BTreeMap<String, String>) -> Result<usize, &'static str> {
    match query.get("limit") {
        None => Ok(100),
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| (1..=MAX_PAGE_ROWS).contains(value))
            .ok_or("limit must be between 1 and 1000"),
    }
}

fn parse_cursor(query: &BTreeMap<String, String>) -> Result<Option<String>, &'static str> {
    match query.get("cursor") {
        None => Ok(None),
        Some(cursor) if cursor.is_empty() => Err("cursor must not be empty"),
        Some(cursor) if cursor.len() > 4096 => Err("cursor must not exceed 4096 bytes"),
        Some(cursor) => Ok(Some(cursor.clone())),
    }
}

fn parse_utc_micros(value: &str) -> Result<i64, &'static str> {
    let parsed = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| "timestamps must be RFC3339 with an explicit UTC offset")?;
    i64::try_from(parsed.unix_timestamp_nanos() / 1_000)
        .map_err(|_| "timestamp is outside the supported range")
}

fn parse_context(value: &str) -> Option<MeasurementContext> {
    Some(match value {
        "daily" => MeasurementContext::Daily,
        "sleep" => MeasurementContext::Sleep,
        "exercise" => MeasurementContext::Exercise,
        "nightly-recharge" => MeasurementContext::NightlyRecharge,
        "sleepwise" => MeasurementContext::SleepWise,
        "cardio-load" => MeasurementContext::CardioLoad,
        "profile" => MeasurementContext::Profile,
        "nightscout" => MeasurementContext::Nightscout,
        _ => return None,
    })
}

fn parse_query(raw: &str) -> Result<BTreeMap<String, String>, &'static str> {
    let mut values = BTreeMap::new();
    for pair in raw.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let (Some(key), Some(value)) = (
            percent_decode_component(key, true),
            percent_decode_component(value, true),
        ) else {
            return Err("query contains invalid percent-encoding or UTF-8");
        };
        if values.insert(key, value).is_some() {
            return Err("query parameter must not be repeated");
        }
    }
    Ok(values)
}

fn parse_admin_duration(query: &BTreeMap<String, String>) -> Result<u64, &'static str> {
    const DEFAULT_AGE_SECONDS: u64 = 86_400;
    const MAX_AGE_SECONDS: u64 = 31_536_000;
    let Some(value) = query.get("older_than_seconds") else {
        return Ok(DEFAULT_AGE_SECONDS);
    };
    let seconds = value
        .parse::<u64>()
        .map_err(|_| "older_than_seconds must be a non-negative integer")?;
    if seconds > MAX_AGE_SECONDS {
        return Err("older_than_seconds must not exceed 31536000");
    }
    Ok(seconds)
}

fn percent_decode_component(value: &str, plus_as_space: bool) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' if plus_as_space => {
                decoded.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let high = (bytes[i + 1] as char).to_digit(16)? as u8;
                let low = (bytes[i + 2] as char).to_digit(16)? as u8;
                decoded.push((high << 4) | low);
                i += 3;
            }
            b'%' => return None,
            byte => {
                decoded.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> ApiResponse {
    let bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(
            Full::new(Bytes::from(bytes))
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .expect("static HTTP response headers are valid")
}

fn bad_request(message: impl ToString) -> ApiResponse {
    json_response(
        StatusCode::BAD_REQUEST,
        &serde_json::json!({"error": message.to_string()}),
    )
}

fn internal_error(error: impl ToString) -> ApiResponse {
    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        &serde_json::json!({"error": error.to_string()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn query(raw: &str) -> BTreeMap<String, String> {
        parse_query(raw).unwrap()
    }

    fn series_point(
        id: &str,
        at: i64,
        value: &str,
        context: MeasurementContext,
    ) -> crate::model::Measurement {
        use crate::model::{
            Aggregation, DataQuality, EventTime, Measurement, MetricId, NumericValue, Provenance,
            SourceId, StableId, TimeInterval, Unit, UserId,
        };

        Measurement {
            schema_version: 1,
            id: StableId::new(id),
            provenance: Provenance {
                user_id: UserId::new("test-user"),
                source: SourceId::new("test-source"),
                resource: "test-resource".into(),
                provider_external_id: Some(format!("external-{id}")),
                source_device_id: None,
                fetched_at_utc_micros: 123,
                archive_ref: Some("abc123".into()),
                revision: None,
                quality: DataQuality::ProviderReported,
            },
            metric: MetricId::new("test.metric"),
            value: NumericValue::new(value).unwrap(),
            unit: Unit::Count,
            time: TimeInterval::point(EventTime::utc(at)),
            aggregation: Aggregation::Instant,
            context,
            parent_id: None,
        }
    }

    #[test]
    fn archive_admin_duration_has_safe_default_and_bounds() {
        assert_eq!(parse_admin_duration(&query("")).unwrap(), 86_400);
        assert_eq!(
            parse_admin_duration(&query("older_than_seconds=3600")).unwrap(),
            3600
        );
        assert!(parse_admin_duration(&query("older_than_seconds=-1")).is_err());
        assert!(parse_admin_duration(&query("older_than_seconds=31536001")).is_err());
    }

    #[tokio::test]
    async fn archive_admin_routes_verify_diagnose_cleanup_and_validate_requests() {
        let temp = TestStore::new();
        let archive = crate::archive::ArchiveStore::new(temp.store.archive_path().to_path_buf())
            .with_min_free_bytes(0);
        let receipt = archive
            .store_body(
                "polar",
                "sleep",
                crate::archive::RequestMetadata::new(),
                200,
                "2026-10-21T00:00:00Z",
                std::io::Cursor::new(b"{}".to_vec()),
            )
            .unwrap();
        temp.store
            .register_archive(&receipt.receipt_id, &receipt)
            .unwrap();
        let stale_part = temp.store.archive_path().join("tmp").join("stale.part");
        std::fs::write(&stale_part, b"incomplete").unwrap();
        let old_time = std::time::SystemTime::now() - std::time::Duration::from_secs(5);
        std::fs::File::options()
            .write(true)
            .open(&stale_part)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old_time))
            .unwrap();

        let sync_lock = Arc::new(tokio::sync::Mutex::new(()));
        let server = start_test_api(temp.store.clone(), sync_lock.clone()).await;

        let (status, verified) =
            admin_request(&server.socket_path, "POST", "/v1/admin/archive/verify", b"").await;
        assert_eq!(status, StatusCode::OK.as_u16());
        assert_eq!(verified["verified"], 1);
        assert_eq!(verified["failed"], 0);

        let (status, diagnosed) = admin_request(
            &server.socket_path,
            "POST",
            "/v1/admin/archive/diagnose?older_than_seconds=1",
            b"",
        )
        .await;
        assert_eq!(status, StatusCode::OK.as_u16(), "{diagnosed}");
        assert_eq!(diagnosed["stale_parts"], 1);

        let (status, preview) = admin_request(
            &server.socket_path,
            "POST",
            "/v1/admin/archive/cleanup?older_than_seconds=1&apply=false",
            b"",
        )
        .await;
        assert_eq!(status, StatusCode::OK.as_u16());
        assert_eq!(preview["dry_run"], true);
        assert!(stale_part.exists(), "dry-run must leave candidates intact");

        let (status, applied) = admin_request(
            &server.socket_path,
            "POST",
            "/v1/admin/archive/cleanup?older_than_seconds=1&apply=true",
            b"",
        )
        .await;
        assert_eq!(status, StatusCode::OK.as_u16());
        assert_eq!(applied["dry_run"], false);
        assert_eq!(applied["removed"]["parts"], 1);
        assert!(!stale_part.exists());

        let (status, _) = admin_request(
            &server.socket_path,
            "POST",
            "/v1/admin/archive/cleanup?apply=true&apply=false",
            b"",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST.as_u16());

        let (status, _) = admin_request(
            &server.socket_path,
            "POST",
            "/v1/admin/archive/verify",
            b"unexpected body",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST.as_u16());

        let (status, _) = admin_request(
            &server.socket_path,
            "POST",
            "/v1/admin/archive/cleanup%2F..",
            b"",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND.as_u16());

        let guard = sync_lock.lock().await;
        let (status, busy) =
            admin_request(&server.socket_path, "POST", "/v1/admin/archive/verify", b"").await;
        assert_eq!(status, StatusCode::CONFLICT.as_u16());
        assert_eq!(busy["error"], "sync_in_progress");
        drop(guard);

        stop_test_api(server).await;
    }

    #[tokio::test]
    async fn api_shutdown_interrupts_idle_accept_and_bounds_partial_client_drain() {
        use tokio::io::AsyncWriteExt;

        let store = TestStore::new();
        let server =
            start_test_api(store.store.clone(), Arc::new(tokio::sync::Mutex::new(()))).await;
        let mut partial = tokio::net::UnixStream::connect(&server.socket_path)
            .await
            .unwrap();
        partial
            .write_all(b"GET /v1/health HTTP/1.1\r\nHost: local\r\n")
            .await
            .unwrap();
        server.shutdown.send(true).unwrap();
        let task_result = tokio::time::timeout(std::time::Duration::from_secs(4), server.task)
            .await
            .expect("server did not stop while an idle/partial client was connected")
            .expect("server task panicked");
        task_result.expect("server shutdown failed");
        assert!(
            !server.socket_path.exists(),
            "server must remove its socket"
        );
        drop(partial);
        let _ = std::fs::remove_dir_all(server.root);
    }

    struct TestStore {
        store: Arc<Store>,
        path: PathBuf,
    }

    static NEXT_TEST_STORE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    impl TestStore {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "rust-polar-api-store-{}-{}",
                std::process::id(),
                NEXT_TEST_STORE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            Self {
                store: Arc::new(Store::open(&path).unwrap()),
                path,
            }
        }
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct TestApiServer {
        root: PathBuf,
        socket_path: PathBuf,
        shutdown: watch::Sender<bool>,
        task: tokio::task::JoinHandle<io::Result<()>>,
    }

    async fn start_test_api(
        store: Arc<Store>,
        sync_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> TestApiServer {
        let root = std::env::temp_dir().join(format!(
            "rp-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        let socket_path = root.join("s");
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(serve_unix(store, socket_path.clone(), receiver, sync_lock));
        for _ in 0..100 {
            if socket_path.exists() {
                return TestApiServer {
                    root,
                    socket_path,
                    shutdown,
                    task,
                };
            }
            if task.is_finished() {
                panic!("local API server failed before binding: {:?}", task.await);
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("local API socket was not created");
    }

    async fn admin_request(
        socket_path: &Path,
        method: &str,
        target: &str,
        body: &[u8],
    ) -> (u16, serde_json::Value) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        let request = format!(
            "{method} {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("local API response timed out")
        .unwrap();
        let response = String::from_utf8(response).unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        let status = headers
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        (status, serde_json::from_str(body).unwrap())
    }

    async fn stop_test_api(server: TestApiServer) {
        let _ = server.shutdown.send(true);
        tokio::time::timeout(std::time::Duration::from_secs(4), server.task)
            .await
            .expect("API server did not shut down after its clients disconnected")
            .expect("API server task panicked")
            .expect("API server shutdown failed");
        let _ = std::fs::remove_dir_all(server.root);
    }

    #[test]
    fn query_parser_decodes_cursor_and_rejects_bad_escapes() {
        let values = query("cursor=a%2Fb&limit=10&context=sleepwise");
        assert_eq!(values["cursor"], "a/b");
        assert_eq!(values["limit"], "10");
        assert_eq!(percent_decode_component("bad%2", true), None);
        assert!(parse_query("limit=1&limit=2").is_err());
        assert_eq!(
            percent_decode_component("a+b", false).as_deref(),
            Some("a+b")
        );
        assert_eq!(
            percent_decode_component("a+b", true).as_deref(),
            Some("a b")
        );
    }

    #[test]
    fn parses_explicit_offset_and_rejects_local_timestamps() {
        assert!(parse_utc_micros("2026-10-21T12:00:00Z").is_ok());
        assert!(parse_utc_micros("2026-10-21T12:00:00").is_err());
    }

    #[test]
    fn downsample_selects_ordered_first_min_max_last_with_original_metadata() {
        let temp = TestStore::new();
        temp.store
            .put_measurements(&[
                series_point("a", 10, "-1", MeasurementContext::Nightscout),
                series_point("b", 20, "100", MeasurementContext::Sleep),
                series_point("c", 30, "10", MeasurementContext::Nightscout),
                series_point("d", 60, "50", MeasurementContext::Sleep),
                series_point("e", 70, "-20", MeasurementContext::Nightscout),
                series_point("f", 80, "0", MeasurementContext::Sleep),
            ])
            .unwrap();

        let response = downsample_measurements(
            &temp.store,
            "test-user",
            "test-source",
            "test.metric",
            None,
            0,
            100,
            2,
        )
        .unwrap();
        let rows = response["rows"].as_array().unwrap();
        let selected = rows
            .iter()
            .map(|row| {
                (
                    row["measurement"]["id"].as_str().unwrap().to_owned(),
                    row["selection"].as_str().unwrap().to_owned(),
                    row["measurement"]["time"]["start"]["Utc"]["unix_micros"]
                        .as_i64()
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            [
                ("a".into(), "first".into(), 10),
                ("a".into(), "min".into(), 10),
                ("b".into(), "max".into(), 20),
                ("c".into(), "last".into(), 30),
                ("d".into(), "first".into(), 60),
                ("d".into(), "max".into(), 60),
                ("e".into(), "min".into(), 70),
                ("f".into(), "last".into(), 80),
            ]
        );
        assert_eq!(
            rows[0]["measurement"]["provenance"]["resource"],
            "test-resource"
        );
        assert_eq!(
            rows[0]["measurement"]["provenance"]["archive_ref"],
            "abc123"
        );
        assert_eq!(response["method"], "first_min_max_last");
        assert_eq!(response["bucket_count"], 2);
        assert!(response["next_cursor"].is_null());
    }

    #[test]
    fn downsample_is_bounded_and_handles_empty_or_invalid_ranges() {
        let temp = TestStore::new();
        let empty = downsample_measurements(&temp.store, "u", "s", "m", None, 0, 100, 4).unwrap();
        assert_eq!(empty["rows"].as_array().unwrap().len(), 0);
        assert_eq!(empty["scanned_rows"], 0);
        assert!(matches!(
            downsample_measurements(&temp.store, "u", "s", "m", None, 1, 1, 4),
            Err(DownsampleError::BadRequest(_))
        ));

        temp.store
            .put_measurements(&[
                series_point("a", 10, "1", MeasurementContext::Sleep),
                series_point("b", 20, "2", MeasurementContext::Sleep),
            ])
            .unwrap();
        assert!(matches!(
            downsample_measurements_with_scan_limit(
                &temp.store,
                "test-user",
                "test-source",
                "test.metric",
                None,
                0,
                100,
                1,
                1,
            ),
            Err(DownsampleError::TooManyRows)
        ));
        assert!(matches!(
            downsample_measurements(&temp.store, "u", "s", "m", None, 0, 100, 251),
            Err(DownsampleError::BadRequest(_))
        ));
    }

    #[test]
    fn decimal_extrema_comparison_preserves_large_and_exponent_values() {
        use crate::model::NumericValue;

        let number = |value: &str| NumericValue::new(value).unwrap();
        assert!(
            compare_numeric(
                &number("100000000000000000000"),
                &number("100000000000000000001")
            )
            .is_lt()
        );
        assert!(compare_numeric(&number("1e-100"), &number("0.00000000000000000001")).is_lt());
        assert!(compare_numeric(&number("-1.01"), &number("-1.001")).is_lt());
        assert_eq!(
            compare_numeric(&number("-0"), &number("0.0")),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn record_filters_validate_half_open_utc_bounds_and_match_resource_aliases() {
        let valid = query(
            "kind=daily_activity&source=polar&from=2026-10-01T00%3A00%3A00Z&to=2026-10-02T00%3A00%3A00Z",
        );
        let filters = RecordFilters::parse(&valid).unwrap();
        let payload = serde_json::json!({
            "resource": "activities",
            "record": {"start_time": "2026-10-01T12:00:00Z", "steps": 0}
        });
        assert!(filters.matches("polar/activities/day-1", &payload));
        assert!(!filters.matches("nightscout/activities/day-1", &payload));

        let reversed = query("from=2026-10-02T00%3A00%3A00Z&to=2026-10-01T00%3A00%3A00Z");
        assert!(RecordFilters::parse(&reversed).is_err());
        let one_sided = query("from=2026-10-01T00%3A00%3A00Z");
        assert!(RecordFilters::parse(&one_sided).is_err());
    }

    #[test]
    fn unresolved_local_record_time_does_not_match_utc_range() {
        let filters = RecordFilters {
            source: None,
            kind: None,
            from: Some(parse_utc_micros("2026-10-01T00:00:00Z").unwrap()),
            to: Some(parse_utc_micros("2026-10-02T00:00:00Z").unwrap()),
        };
        let payload = serde_json::json!({
            "resource": "activities",
            "record": {"start_time": "2026-10-01T12:00:00", "steps": 2}
        });
        assert!(!filters.matches("polar/activities/day-1", &payload));
    }

    #[test]
    fn record_intervals_use_half_open_overlap_semantics() {
        let from = parse_utc_micros("2026-10-01T00:00:00Z").unwrap();
        let to = parse_utc_micros("2026-10-02T00:00:00Z").unwrap();
        assert!(!record_overlaps_range(
            &serde_json::json!({
                "start_time":"2026-09-30T23:00:00Z",
                "end_time":"2026-10-01T00:00:00Z"
            }),
            from,
            to
        ));
        assert!(record_overlaps_range(
            &serde_json::json!({
                "start_time":"2026-09-30T23:00:00Z",
                "end_time":"2026-10-01T00:00:01Z"
            }),
            from,
            to
        ));
        assert!(!record_overlaps_range(
            &serde_json::json!({"end_time":"2026-10-01T00:00:00Z"}),
            from,
            to
        ));
        assert!(record_overlaps_range(
            &serde_json::json!({"end_time":"2026-10-03T00:00:00Z"}),
            from,
            to
        ));
    }

    #[test]
    fn daily_date_range_is_validated_as_local_half_open_dates() {
        assert!(parse_date_bounds(&query("from=2026-10-01&to=2026-11-01")).is_ok());
        assert!(parse_date_bounds(&query("from=2026-11-01&to=2026-10-01")).is_err());
        assert!(parse_date_bounds(&query("from=2026-02-30&to=2026-03-01")).is_err());
        assert_eq!(
            daily_date(&serde_json::json!({"start_time":"2026-10-01T12:30:00"})),
            Some("2026-10-01")
        );
    }

    #[test]
    fn export_query_checks_required_filters_and_supported_formats() {
        let args = query(
            "user_id=u&source=polar&metric=steps&from=2026-10-01T00%3A00%3A00Z&to=2026-10-02T00%3A00%3A00Z&format=csv&limit=2",
        );
        let parsed = parse_export_query(&args).unwrap();
        assert_eq!(parsed.format, ExportFormat::Csv);
        assert_eq!(parsed.limit, 2);
        let bad = query(
            "user_id=u&source=polar&metric=steps&from=2026-10-02T00%3A00%3A00Z&to=2026-10-01T00%3A00%3A00Z&format=xml",
        );
        assert!(parse_export_query(&bad).is_err());
    }

    #[test]
    fn csv_export_escapes_delimiters_and_quotes() {
        let mut value = String::new();
        csv_field(&mut value, "a,\"b\"");
        assert_eq!(value, "\"a,\"\"b\"\"\"");
    }

    #[test]
    fn encoded_record_id_supports_slashes_but_rejects_bad_escapes() {
        assert_eq!(
            percent_decode_component("polar%2Factivities%2Fday-1", false).as_deref(),
            Some("polar/activities/day-1")
        );
        assert_eq!(percent_decode_component("bad%2", false).as_deref(), None);
    }

    #[tokio::test]
    async fn indexed_exercise_export_is_verified_and_streamed_in_bounded_chunks() {
        let temp = TestStore::new();
        let exercise_id = "run/with unsafe chars";
        let source = (0..(ARCHIVE_STREAM_CHUNK_BYTES * 3 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut metadata = crate::archive::RequestMetadata::new();
        metadata.insert("exercise_id".into(), exercise_id.into());
        let archive = crate::archive::ArchiveStore::new(temp.store.archive_path().to_path_buf())
            .with_min_free_bytes(0);
        let receipt = archive
            .store_body(
                "polar",
                "exercise-fit",
                metadata,
                200,
                "2026-10-21T00:00:00Z",
                source.as_slice(),
            )
            .unwrap();
        temp.store
            .register_archive(&receipt.receipt_id, &receipt)
            .unwrap();
        let index = serde_json::json!({
            "resource":"exercise-fit",
            "external_id":exercise_id,
            "archive_ref":receipt.content_hash
        });
        temp.store
            .put_record(
                &format!("polar/exercise-fit/{exercise_id}"),
                &serde_json::to_vec(&index).unwrap(),
            )
            .unwrap();

        let (archive, receipt, filename) = prepare_exercise_export(&temp.store, exercise_id, "fit")
            .unwrap()
            .unwrap();
        assert_eq!(filename, "run_with_unsafe_chars.fit");
        let response = stream_exercise_export(archive, receipt, filename);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[hyper::header::CONTENT_LENGTH],
            source.len().to_string()
        );
        assert_eq!(
            response.headers()[hyper::header::CONTENT_TYPE],
            "application/octet-stream"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), source.as_slice());
    }

    #[test]
    fn exercise_export_rejects_missing_or_corrupt_archive_objects() {
        let temp = TestStore::new();
        let exercise_id = "broken-archive";
        let mut metadata = crate::archive::RequestMetadata::new();
        metadata.insert("exercise_id".into(), exercise_id.into());
        let archive = crate::archive::ArchiveStore::new(temp.store.archive_path().to_path_buf())
            .with_min_free_bytes(0);
        let receipt = archive
            .store_body(
                "polar",
                "exercise-fit",
                metadata,
                200,
                "2026-10-21T00:00:00Z",
                b"valid export body".as_slice(),
            )
            .unwrap();
        temp.store
            .register_archive(&receipt.receipt_id, &receipt)
            .unwrap();
        let index = serde_json::json!({
            "resource":"exercise-fit",
            "external_id":exercise_id,
            "archive_ref":receipt.content_hash
        });
        temp.store
            .put_record(
                &format!("polar/exercise-fit/{exercise_id}"),
                &serde_json::to_vec(&index).unwrap(),
            )
            .unwrap();

        let object = temp
            .store
            .archive_path()
            .join("objects")
            .join(&receipt.content_hash[..2])
            .join(format!("{}.gz", receipt.content_hash));
        std::fs::remove_file(&object).unwrap();
        let error = match prepare_exercise_export(&temp.store, exercise_id, "fit") {
            Err(error) => error,
            Ok(_) => panic!("missing archive object must be reported"),
        };
        assert!(
            error.contains("No such file") || error.contains("not found"),
            "{error}"
        );

        archive
            .store_body(
                "polar",
                "exercise-fit",
                crate::archive::RequestMetadata::from([(
                    "exercise_id".to_owned(),
                    exercise_id.to_owned(),
                )]),
                200,
                "2026-10-21T00:00:01Z",
                b"valid export body".as_slice(),
            )
            .unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&object)
            .unwrap()
            .write_all(b"corrupt")
            .unwrap();
        let error = match prepare_exercise_export(&temp.store, exercise_id, "fit") {
            Err(error) => error,
            Ok(_) => panic!("corrupt archive object must be rejected"),
        };
        assert!(
            error.contains("hash")
                || error.contains("gzip")
                || error.contains("descriptor mismatch"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn exercise_export_stops_archive_reader_after_client_disconnect() {
        use http_body_util::BodyExt;

        let temp = TestStore::new();
        let exercise_id = "disconnect";
        let source = vec![42_u8; ARCHIVE_STREAM_CHUNK_BYTES * (ARCHIVE_STREAM_QUEUE_CHUNKS + 4)];
        let mut metadata = crate::archive::RequestMetadata::new();
        metadata.insert("exercise_id".into(), exercise_id.into());
        let archive = crate::archive::ArchiveStore::new(temp.store.archive_path().to_path_buf())
            .with_min_free_bytes(0);
        let receipt = archive
            .store_body(
                "polar",
                "exercise-fit",
                metadata,
                200,
                "2026-10-21T00:00:00Z",
                source.as_slice(),
            )
            .unwrap();
        let (mut body, worker) = exercise_export_body(archive, receipt);

        let first_frame = tokio::time::timeout(std::time::Duration::from_secs(5), body.frame())
            .await
            .expect("first export chunk timed out")
            .expect("stream ended before its first chunk")
            .expect("archive stream failed")
            .into_data()
            .expect("export stream produced a non-data frame");
        assert!(!first_frame.is_empty());
        drop(body);
        tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .expect("archive worker remained blocked after client disconnect")
            .expect("archive worker panicked");
    }

    #[test]
    fn exercise_export_lookup_only_accepts_indexed_formats() {
        let temp = TestStore::new();
        assert!(
            prepare_exercise_export(&temp.store, "missing", "fit")
                .unwrap()
                .is_none()
        );
        assert!(prepare_exercise_export(&temp.store, "missing", "exe").is_err());
    }

    #[test]
    fn bounded_record_queries_and_catalog_use_stored_provider_records() {
        let temp = TestStore::new();
        let activity = serde_json::json!({
            "archive_ref": "b3:day",
            "resource": "activities",
            "external_id": "2026-10-20",
            "metrics": [{"metric":"steps","value":"42","unit":"Steps"}],
            "record": {"date":"2026-10-20","steps":42}
        });
        let sleep = serde_json::json!({
            "resource": "sleep",
            "metrics": [{"metric":"steps","value":"8","unit":"Steps"}],
            "record": {"sleep_start_time":"2026-10-20T22:00:00Z"}
        });
        temp.store
            .put_record(
                "polar/activities/2026-10-20",
                &serde_json::to_vec(&activity).unwrap(),
            )
            .unwrap();
        temp.store
            .put_record("polar/sleep/night-1", &serde_json::to_vec(&sleep).unwrap())
            .unwrap();
        let nightscout = serde_json::json!({
            "resource": "entries",
            "metrics": [{"metric":"steps","value":"4","unit":"Steps"}],
            "record": {"date":"2026-10-20"}
        });
        temp.store
            .put_record(
                "nightscout/entries/entry-1",
                &serde_json::to_vec(&nightscout).unwrap(),
            )
            .unwrap();

        let filters = RecordFilters {
            source: Some("polar".into()),
            kind: Some("daily-activity".into()),
            from: None,
            to: None,
        };
        let cancelled = AtomicBool::new(false);
        let page = filtered_records(&temp.store, &filters, None, 10, &cancelled).unwrap();
        assert_eq!(page["rows"].as_array().unwrap().len(), 1);
        assert_eq!(page["rows"][0]["id"], "polar/activities/2026-10-20");

        let daily = daily_records(
            &temp.store,
            "2026-10-20",
            "2026-10-21",
            None,
            10,
            &cancelled,
        )
        .unwrap();
        assert_eq!(daily["rows"].as_array().unwrap().len(), 1);
        assert_eq!(
            daily["date_semantics"],
            "provider-local calendar labels; no UTC conversion"
        );

        let catalog = metric_catalog(&temp.store, None, 10, &cancelled).unwrap();
        assert_eq!(catalog["metrics"].as_array().unwrap().len(), 1);
        assert_eq!(catalog["metrics"][0]["metric"], "steps");
        let sources = catalog["metrics"][0]["sources"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(sources, ["nightscout", "polar"].into_iter().collect());

        let detail = temp
            .store
            .get_record("polar/activities/2026-10-20")
            .unwrap()
            .unwrap();
        let detail: serde_json::Value = serde_json::from_slice(&detail).unwrap();
        assert_eq!(detail["record"]["steps"], 42);
    }

    #[test]
    fn filtered_record_response_stays_byte_bounded_and_cursor_does_not_skip() {
        let temp = TestStore::new();
        let padding = "x".repeat(3 * 1024 * 1024);
        for (id, pad) in [("a", &padding), ("b", &padding)] {
            let record = serde_json::json!({
                "resource": "activities",
                "record": {"date":"2026-10-20", "pad":pad}
            });
            temp.store
                .put_record(
                    &format!("polar/activities/{id}"),
                    &serde_json::to_vec(&record).unwrap(),
                )
                .unwrap();
        }

        let filters = RecordFilters {
            source: Some("polar".into()),
            kind: None,
            from: None,
            to: None,
        };
        let cancelled = AtomicBool::new(false);
        let page = filtered_records(&temp.store, &filters, None, 100, &cancelled).unwrap();
        let encoded = serde_json::to_vec(&page).unwrap();
        assert!(encoded.len() <= MAX_API_PAGE_BYTES);
        assert_eq!(page["rows"].as_array().unwrap().len(), 1);
        assert_eq!(page["next_cursor"], "polar/activities/a");

        let next = filtered_records(
            &temp.store,
            &filters,
            page["next_cursor"].as_str(),
            100,
            &cancelled,
        )
        .unwrap();
        assert_eq!(next["rows"].as_array().unwrap().len(), 1);
        assert_eq!(next["rows"][0]["id"], "polar/activities/b");
    }

    #[test]
    fn refuses_to_replace_regular_file_at_socket_path() {
        let root = std::env::temp_dir().join(format!("rust-polar-api-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("service.sock");
        std::fs::write(&socket, b"do not delete").unwrap();
        assert_eq!(
            bind_private_socket(&socket).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(&socket).unwrap(), b"do not delete");
        let _ = std::fs::remove_dir_all(root);
    }
}
