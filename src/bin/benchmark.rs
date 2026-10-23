use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Parser;
use rust_polar::archive::{ArchiveStore, RequestMetadata};
use rust_polar::model::{
    Aggregation, DataQuality, EventTime, Measurement, MeasurementContext, MetricId, NumericValue,
    Provenance, SourceId, StableId, TimeInterval, Unit, UserId,
};
use rust_polar::storage::{MeasurementQuery, Store};

const DEFAULT_DAYS: i64 = 1_825;
const POINTS_PER_DAY: i64 = 288;
const FIVE_MINUTES_MICROS: i64 = 5 * 60 * 1_000_000;
const DAY_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
const HYPNOGRAM_POINTS_PER_NIGHT: i64 = 8 * 60 * 2;
const BATCH_ROWS: usize = 256;
const EPOCH_START_MICROS: i64 = 1_577_836_800_000_000; // 2020-01-01T00:00:00Z
const SEED: u64 = 0x504f_4c41_5220_3236;

#[derive(Debug, Parser)]
#[command(about = "Reproducible fixed-seed rust-polar workload benchmark")]
struct Args {
    /// New, empty data directory for the benchmark run.
    #[arg(long)]
    data_dir: PathBuf,
    /// Number of independent synthetic users; 10 produces the 10x history run.
    #[arg(long, default_value_t = 1)]
    users: u32,
    /// History length. The reproducible five-year workload uses the default 1825 days.
    #[arg(long, default_value_t = DEFAULT_DAYS, value_parser = clap::value_parser!(i64).range(1..=1825))]
    days: i64,
    /// Repeat the run with an existing data directory.
    #[arg(long, default_value_t = false)]
    reuse: bool,
    /// Measure only the paginated 30-day query against an existing dataset.
    #[arg(long, default_value_t = false, requires = "reuse")]
    query_only: bool,
}

#[derive(Default)]
struct Timings {
    rows: u64,
    batches: u64,
    elapsed: Duration,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.users == 0 || args.users > 10 {
        return Err("--users must be between 1 and 10".into());
    }
    if args.data_dir.exists() && !args.reuse {
        return Err(format!(
            "benchmark data directory already exists: {}; choose a new path or pass --reuse",
            args.data_dir.display()
        )
        .into());
    }
    std::fs::create_dir_all(&args.data_dir)?;
    let store = Store::open(&args.data_dir)?;
    let started = Instant::now();

    if args.query_only {
        let latencies = query_30_days(&store, args.users, args.days)?;
        let report = serde_json::json!({
            "benchmark_version": 1,
            "mode": "query-only",
            "seed": format!("0x{SEED:016x}"),
            "environment": {
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH,
                "users": args.users,
                "release_profile": !cfg!(debug_assertions),
                "durability": "redb Immediate"
            },
            "dataset": {
                "history_days": args.days,
                "users": args.users,
                "measurements": store.measurement_count()?
            },
            "results": {
                "query_30d_all_pages_1000_p50_us": percentile(&latencies, 50),
                "query_30d_all_pages_1000_p95_us": percentile(&latencies, 95),
                "database_bytes": std::fs::metadata(store.database_path())?.len(),
                "wall_seconds": started.elapsed().as_secs_f64(),
                "peak_rss_bytes": null,
                "rss_note": "Measure the process with the platform time utility; see BENCHMARK.md"
            }
        });
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    let ingest_started = Instant::now();
    let mut initial = Timings::default();
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for user_index in 0..args.users {
        let user_id = format!("synthetic-{user_index:02}");
        let source_base = base_provenance(&user_id, "nightscout", "entries");
        for point_index in 0..(args.days * POINTS_PER_DAY) {
            let at = EPOCH_START_MICROS + point_index * FIVE_MINUTES_MICROS;
            let value = 80 + (fixed_value(user_index, 1, point_index as u64) % 141) as i64;
            batch.push(measurement(
                &source_base,
                "nightscout.glucose_mgdl",
                &format!("glucose-{point_index}"),
                value,
                Unit::MilligramsPerDeciliter,
                MeasurementContext::Nightscout,
                at,
                None,
            ));
            if batch.len() == BATCH_ROWS {
                flush_measurements(&store, &mut batch, &mut initial)?;
            }
        }
        let hr_base = base_provenance(&user_id, "polar", "continuous-heart-rate");
        for point_index in 0..(args.days * POINTS_PER_DAY) {
            let at = EPOCH_START_MICROS + point_index * FIVE_MINUTES_MICROS;
            let value = 45 + (fixed_value(user_index, 2, point_index as u64) % 111) as i64;
            batch.push(measurement(
                &hr_base,
                "polar.continuous_heart_rate",
                &format!("heart-rate-{point_index}"),
                value,
                Unit::BeatsPerMinute,
                MeasurementContext::Daily,
                at,
                None,
            ));
            if batch.len() == BATCH_ROWS {
                flush_measurements(&store, &mut batch, &mut initial)?;
            }
        }
        let activity_base = base_provenance(&user_id, "polar", "activities");
        let sleep_base = base_provenance(&user_id, "polar", "sleep");
        let recharge_base = base_provenance(&user_id, "polar", "nightly-recharge");
        for day in 0..args.days {
            let day_start = EPOCH_START_MICROS + day * DAY_MICROS;
            batch.push(measurement(
                &activity_base,
                "polar.daily_activity.steps",
                &format!("activity-{day}"),
                2_000 + (fixed_value(user_index, 3, day as u64) % 16_001) as i64,
                Unit::Steps,
                MeasurementContext::Daily,
                day_start,
                None,
            ));
            batch.push(measurement(
                &sleep_base,
                "polar.sleep.duration_seconds",
                &format!("sleep-duration-{day}"),
                18_000 + (fixed_value(user_index, 4, day as u64) % 14_401) as i64,
                Unit::Unknown("seconds".into()),
                MeasurementContext::Sleep,
                day_start,
                None,
            ));
            batch.push(measurement(
                &recharge_base,
                "polar.nightly_recharge.hrv",
                &format!("recharge-hrv-{day}"),
                15 + (fixed_value(user_index, 5, day as u64) % 71) as i64,
                Unit::Milliseconds,
                MeasurementContext::NightlyRecharge,
                day_start,
                None,
            ));
            if batch.len() >= BATCH_ROWS {
                flush_measurements(&store, &mut batch, &mut initial)?;
            }
            let sleep_start = day_start - 2 * 60 * 60 * 1_000_000;
            for sample_index in 0..HYPNOGRAM_POINTS_PER_NIGHT {
                batch.push(measurement(
                    &sleep_base,
                    "polar.sleep.hypnogram_stage_code",
                    &format!("hypnogram-{day}-{sample_index}"),
                    (fixed_value(
                        user_index,
                        6,
                        (day * HYPNOGRAM_POINTS_PER_NIGHT + sample_index) as u64,
                    ) % 5) as i64,
                    Unit::Count,
                    MeasurementContext::Sleep,
                    sleep_start + sample_index * 30 * 1_000_000,
                    Some(StableId::from_parts([&user_id, "sleep", &day.to_string()])),
                ));
                if batch.len() == BATCH_ROWS {
                    flush_measurements(&store, &mut batch, &mut initial)?;
                }
            }
        }
        let exercise_base = base_provenance(&user_id, "polar", "exercises");
        let exercise_count = (25 * args.days / DEFAULT_DAYS).max(1);
        for exercise_index in 0..exercise_count {
            let start = EPOCH_START_MICROS + exercise_index * 73 * DAY_MICROS;
            let parent =
                StableId::from_parts([&user_id, "polar", "exercises", &exercise_index.to_string()]);
            for sample_index in 0..360_i64 {
                batch.push(measurement(
                    &exercise_base,
                    "polar.exercise.heart_rate",
                    &format!("exercise-{exercise_index}-{sample_index}"),
                    70 + (fixed_value(user_index, 7, (exercise_index * 360 + sample_index) as u64)
                        % 111) as i64,
                    Unit::BeatsPerMinute,
                    MeasurementContext::Exercise,
                    start + sample_index * 5 * 1_000_000,
                    Some(parent.clone()),
                ));
                if batch.len() == BATCH_ROWS {
                    flush_measurements(&store, &mut batch, &mut initial)?;
                }
            }
        }
    }
    flush_measurements(&store, &mut batch, &mut initial)?;
    initial.elapsed = ingest_started.elapsed();

    // These points have old event times but arrive after the initial history
    // backfill, exercising the incremental-sync path for delayed provider data.
    let late_started = Instant::now();
    let (late_rows, late_batches) = insert_late_historical_points(&store, args.users, args.days)?;
    let late_elapsed = late_started.elapsed();

    let records_started = Instant::now();
    let record_count = insert_daily_records(&store, args.users, args.days)?;
    let records_elapsed = records_started.elapsed();
    let daily_provider_records = u64::from(args.users) * args.days as u64 * 3;
    let exercise_provider_records =
        u64::from(args.users) * ((25 * args.days / DEFAULT_DAYS).max(1) as u64);

    let archive_started = Instant::now();
    let (archive_count, archive_uncompressed_bytes) = archive_samples(&store)?;
    let archive_elapsed = archive_started.elapsed();

    let reimport_started = Instant::now();
    let prefix_rows = (args.days * POINTS_PER_DAY).min(10_000) as usize;
    let reimport = reimport_prefix(&store, args.users, prefix_rows)?;
    let reimport_elapsed = reimport_started.elapsed();

    let correction_started = Instant::now();
    let corrections = correct_prefix(&store, args.users, prefix_rows.min(2_000))?;
    let correction_elapsed = correction_started.elapsed();

    let query_latencies = query_30_days(&store, args.users, args.days)?;
    let query_p50_us = percentile(&query_latencies, 50);
    let query_p95_us = percentile(&query_latencies, 95);
    let database_bytes = std::fs::metadata(store.database_path())?.len();
    let archive_bytes = directory_bytes(store.archive_path())?;
    let total_measurements = store.measurement_count()?;
    let wall_seconds = started.elapsed().as_secs_f64();

    let report = serde_json::json!({
        "benchmark_version": 1,
        "seed": format!("0x{SEED:016x}"),
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "users": args.users,
            "release_profile": !cfg!(debug_assertions),
            "durability": "redb Immediate"
        },
        "dataset": {
            "history_days": args.days,
            "glucose_points_per_user": args.days * POINTS_PER_DAY,
            "heart_rate_points_per_user": args.days * POINTS_PER_DAY,
            "daily_activity_sleep_recharge_records_per_user": args.days * 3,
            "hypnogram_points_per_user": args.days * HYPNOGRAM_POINTS_PER_NIGHT,
            "exercise_samples_per_user": (25 * args.days / DEFAULT_DAYS).max(1) * 360,
            "late_historical_points_per_user": args.days,
            "late_historical_points": late_rows,
            "users": args.users,
            "total_measurements_after_corrections": total_measurements,
            "daily_provider_records": daily_provider_records,
            "exercise_provider_records": exercise_provider_records,
            "provider_records": record_count,
            "archive_receipts": archive_count
        },
        "results": {
            "initial_series_rows": initial.rows,
            "initial_series_batches": initial.batches,
            "initial_series_seconds": initial.elapsed.as_secs_f64(),
            "initial_rows_per_second": initial.rows as f64 / initial.elapsed.as_secs_f64().max(0.000_001),
            "late_historical_rows": late_rows,
            "late_historical_batches": late_batches,
            "late_historical_insert_seconds": late_elapsed.as_secs_f64(),
            "provider_record_insert_seconds": records_elapsed.as_secs_f64(),
            "archive_write_and_registration_seconds": archive_elapsed.as_secs_f64(),
            "stable_id_reimport_rows": reimport,
            "stable_id_reimport_seconds": reimport_elapsed.as_secs_f64(),
            "stable_id_corrections": corrections,
            "stable_id_correction_seconds": correction_elapsed.as_secs_f64(),
            "query_30d_all_pages_1000_p50_us": query_p50_us,
            "query_30d_all_pages_1000_p95_us": query_p95_us,
            "database_bytes": database_bytes,
            "archive_bytes": archive_bytes,
            "archive_uncompressed_bytes": archive_uncompressed_bytes,
            "wall_seconds": wall_seconds,
            "peak_rss_bytes": null,
            "rss_note": "Measure the process with the platform time utility; see BENCHMARK.md"
        }
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn insert_late_historical_points(
    store: &Store,
    users: u32,
    days: i64,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let mut timings = Timings::default();
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for user_index in 0..users {
        let user_id = format!("synthetic-{user_index:02}");
        let provenance = base_provenance(&user_id, "polar", "late-continuous-heart-rate");
        for day in 0..days {
            // Offset from the regular five-minute grid while retaining an old
            // timestamp, so this is an additional late point, not an update.
            let at = EPOCH_START_MICROS + day * DAY_MICROS + 2 * 60 * 1_000_000;
            batch.push(measurement(
                &provenance,
                "polar.continuous_heart_rate",
                &format!("late-heart-rate-{day}"),
                45 + (fixed_value(user_index, 8, day as u64) % 111) as i64,
                Unit::BeatsPerMinute,
                MeasurementContext::Daily,
                at,
                None,
            ));
            if batch.len() == BATCH_ROWS {
                flush_measurements(store, &mut batch, &mut timings)?;
            }
        }
    }
    flush_measurements(store, &mut batch, &mut timings)?;
    Ok((timings.rows, timings.batches))
}

fn base_provenance(user_id: &str, source: &str, resource: &str) -> Provenance {
    Provenance {
        user_id: UserId::new(user_id),
        source: SourceId::new(source),
        resource: resource.to_owned(),
        provider_external_id: None,
        source_device_id: Some("synthetic-device".to_owned()),
        fetched_at_utc_micros: EPOCH_START_MICROS + DEFAULT_DAYS * DAY_MICROS,
        archive_ref: None,
        revision: None,
        quality: DataQuality::ProviderReported,
    }
}

#[allow(clippy::too_many_arguments)] // Mirror the normalized row fields in this benchmark helper.
fn measurement(
    provenance: &Provenance,
    metric: &str,
    id: &str,
    value: i64,
    unit: Unit,
    context: MeasurementContext,
    at: i64,
    parent_id: Option<StableId>,
) -> Measurement {
    Measurement {
        schema_version: 1,
        id: StableId::from_parts([
            provenance.user_id.as_str(),
            provenance.source.as_str(),
            &provenance.resource,
            id,
        ]),
        provenance: provenance.clone(),
        metric: MetricId::new(metric),
        value: NumericValue::new(value.to_string()).unwrap(),
        unit,
        time: TimeInterval::point(EventTime::utc(at)),
        aggregation: Aggregation::Instant,
        context,
        parent_id,
    }
}

fn flush_measurements(
    store: &Store,
    batch: &mut Vec<Measurement>,
    timings: &mut Timings,
) -> Result<(), Box<dyn std::error::Error>> {
    if batch.is_empty() {
        return Ok(());
    }
    store.put_measurements(batch)?;
    timings.rows += batch.len() as u64;
    timings.batches += 1;
    batch.clear();
    Ok(())
}

fn insert_daily_records(
    store: &Store,
    users: u32,
    days: i64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut count = 0_u64;
    let mut batch = Vec::<(String, Vec<u8>)>::with_capacity(128);
    for user in 0..users {
        let user_id = format!("synthetic-{user:02}");
        for day in 0..days {
            let date = day.to_string();
            for resource in ["activities", "sleep", "nightly-recharge"] {
                let key = format!("polar/{resource}/{user_id}/{date}");
                let payload = serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "resource": resource,
                    "day_index": day,
                    "synthetic_user": user,
                    "source_device_id": "synthetic-device"
                }))?;
                batch.push((key, payload));
                if batch.len() == 128 {
                    store_record_batch(store, &mut batch)?;
                    count += 128;
                }
            }
        }
        let exercise_count = (25 * days / DEFAULT_DAYS).max(1);
        for exercise_index in 0..exercise_count {
            let parent_id =
                StableId::from_parts([&user_id, "polar", "exercises", &exercise_index.to_string()]);
            let key = format!("polar/exercises/{user_id}/{exercise_index}");
            let payload = serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "kind": "exercise",
                "id": exercise_index.to_string(),
                "parent_id": parent_id.as_str(),
                "source_device_id": "synthetic-device",
                "sample_count": 360
            }))?;
            batch.push((key, payload));
            if batch.len() >= 128 {
                let length = batch.len() as u64;
                store_record_batch(store, &mut batch)?;
                count += length;
            }
        }
    }
    count += batch.len() as u64;
    store_record_batch(store, &mut batch)?;
    Ok(count)
}

fn store_record_batch(
    store: &Store,
    batch: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = batch
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_slice()))
        .collect::<Vec<_>>();
    store.put_records(&rows)?;
    batch.clear();
    Ok(())
}

fn archive_samples(store: &Store) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let archive = ArchiveStore::new(store.archive_path().to_path_buf());
    let mut count = 0_u64;
    let mut uncompressed = 0_u64;
    for index in 0..20_u32 {
        let body = serde_json::to_vec(&serde_json::json!({
            "synthetic": true,
            "seed": SEED,
            "response_index": index,
            "values": (0..4096).map(|value| value + index).collect::<Vec<_>>()
        }))?;
        let receipt = archive.store_body(
            "synthetic",
            "series",
            RequestMetadata::new(),
            200,
            "2026-10-21T00:00:00Z",
            Cursor::new(body),
        )?;
        store.register_archive(&receipt.receipt_id, &receipt)?;
        uncompressed += receipt.uncompressed_bytes;
        count += 1;
    }
    Ok((count, uncompressed))
}

fn reimport_prefix(
    store: &Store,
    users: u32,
    rows_per_stream: usize,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut count = 0_u64;
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for user_index in 0..users {
        let user = format!("synthetic-{user_index:02}");
        for (source, resource, metric, unit, context) in [
            (
                "nightscout",
                "entries",
                "nightscout.glucose_mgdl",
                Unit::MilligramsPerDeciliter,
                MeasurementContext::Nightscout,
            ),
            (
                "polar",
                "continuous-heart-rate",
                "polar.continuous_heart_rate",
                Unit::BeatsPerMinute,
                MeasurementContext::Daily,
            ),
        ] {
            let provenance = base_provenance(&user, source, resource);
            for point_index in 0..rows_per_stream {
                let at = EPOCH_START_MICROS + point_index as i64 * FIVE_MINUTES_MICROS;
                batch.push(measurement(
                    &provenance,
                    metric,
                    &format!(
                        "{}-{point_index}",
                        if source == "polar" {
                            "heart-rate"
                        } else {
                            "glucose"
                        }
                    ),
                    80 + (fixed_value(
                        user_index,
                        if source == "polar" { 2 } else { 1 },
                        point_index as u64,
                    ) % 141) as i64,
                    unit.clone(),
                    context.clone(),
                    at,
                    None,
                ));
                if batch.len() == BATCH_ROWS {
                    store.put_measurements(&batch)?;
                    count += batch.len() as u64;
                    batch.clear();
                }
            }
        }
    }
    if !batch.is_empty() {
        store.put_measurements(&batch)?;
        count += batch.len() as u64;
    }
    Ok(count)
}

fn correct_prefix(
    store: &Store,
    users: u32,
    rows_per_stream: usize,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut corrected = 0_u64;
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for user_index in 0..users {
        let user = format!("synthetic-{user_index:02}");
        for (source, resource, metric, unit, context) in [
            (
                "nightscout",
                "entries",
                "nightscout.glucose_mgdl",
                Unit::MilligramsPerDeciliter,
                MeasurementContext::Nightscout,
            ),
            (
                "polar",
                "continuous-heart-rate",
                "polar.continuous_heart_rate",
                Unit::BeatsPerMinute,
                MeasurementContext::Daily,
            ),
        ] {
            let provenance = base_provenance(&user, source, resource);
            for point_index in 0..rows_per_stream {
                batch.push(measurement(
                    &provenance,
                    metric,
                    &format!(
                        "{}-{point_index}",
                        if source == "polar" {
                            "heart-rate"
                        } else {
                            "glucose"
                        }
                    ),
                    90,
                    unit.clone(),
                    context.clone(),
                    EPOCH_START_MICROS + point_index as i64 * FIVE_MINUTES_MICROS + 30_000_000,
                    None,
                ));
                if batch.len() == BATCH_ROWS {
                    store.put_measurements(&batch)?;
                    corrected += batch.len() as u64;
                    batch.clear();
                }
            }
        }
    }
    if !batch.is_empty() {
        store.put_measurements(&batch)?;
        corrected += batch.len() as u64;
    }
    Ok(corrected)
}

fn query_30_days(
    store: &Store,
    users: u32,
    days: i64,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let mut samples = Vec::with_capacity(20);
    let to = EPOCH_START_MICROS + days * DAY_MICROS;
    let from = to - days.min(30) * DAY_MICROS;
    for _ in 0..21 {
        let started = Instant::now();
        let mut cursor = None;
        let mut rows = 0_u64;
        loop {
            let page = store.measurement_page(MeasurementQuery {
                user_id: "synthetic-00",
                source: "nightscout",
                metric: "nightscout.glucose_mgdl",
                context: None,
                from_utc_micros: from,
                to_utc_micros: to,
                limit: 1000,
                after: cursor.as_deref(),
            })?;
            rows += page.rows.len() as u64;
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let _ = rows;
        let elapsed = started.elapsed().as_micros() as u64;
        if samples.len() < 20 {
            samples.push(elapsed);
        }
    }
    if users == 0 {
        return Err("query benchmark requires at least one user".into());
    }
    Ok(samples)
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let index = (ordered.len().saturating_sub(1) * percentile).div_ceil(100);
    ordered.get(index).copied().unwrap_or_default()
}

fn directory_bytes(path: &Path) -> Result<u64, std::io::Error> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path())?);
        } else if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn fixed_value(user_index: u32, stream: u64, index: u64) -> u64 {
    let mut value = SEED ^ (u64::from(user_index) << 40) ^ (stream << 32) ^ index;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
