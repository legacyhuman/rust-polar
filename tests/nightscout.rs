use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_polar::model::{
    Aggregation, DataQuality, EventTime, Measurement, MeasurementContext, MetricId, NumericValue,
    Provenance, ProviderDocument, Record, RecordKind, SourceId, StableId, TimeInterval, Unit,
    UserId,
};
use rust_polar::storage::{MeasurementQuery, Store};

const FIVE_MINUTES_MICROS: i64 = 5 * 60 * 1_000_000;
const READINGS_PER_DAY: usize = 24 * 12;

struct TestDir(PathBuf);
static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

impl TestDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rust-polar-nightscout-{}-{nonce}-{}",
            std::process::id(),
            NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn midnight_utc_micros() -> i64 {
    let date = time::Date::from_calendar_date(2026, time::Month::October, 1).unwrap();
    let midnight = date.midnight().assume_utc();
    i64::try_from(midnight.unix_timestamp_nanos() / 1_000).unwrap()
}

fn provenance(source: &str, external_id: String) -> Provenance {
    Provenance {
        user_id: UserId::new("synthetic-user"),
        source: SourceId::new(source),
        resource: "entries".into(),
        provider_external_id: Some(external_id),
        source_device_id: Some(format!("{source}-synthetic-device")),
        fetched_at_utc_micros: midnight_utc_micros(),
        archive_ref: None,
        revision: Some("synthetic-v1".into()),
        quality: DataQuality::ProviderReported,
    }
}

fn glucose_measurement(source: &str, external_id: &str, at_micros: i64, value: i64) -> Measurement {
    let parent = StableId::from_parts([source, "entries", external_id]);
    Measurement {
        schema_version: 1,
        id: StableId::from_parts(["synthetic-user", source, "glucose", external_id]),
        provenance: provenance(source, external_id.to_owned()),
        metric: MetricId::new("glucose"),
        value: NumericValue::new(value.to_string()).unwrap(),
        unit: Unit::MilligramsPerDeciliter,
        time: TimeInterval::new(
            EventTime::utc(at_micros),
            EventTime::utc(at_micros + FIVE_MINUTES_MICROS),
        ),
        aggregation: Aggregation::Instant,
        context: MeasurementContext::Nightscout,
        parent_id: Some(parent),
    }
}

fn nightscout_record(index: usize, at_micros: i64, value: i64) -> (String, Vec<u8>) {
    let external_id = format!("ns-entry-{index:04}");
    let direction = ["Flat", "FortyFiveUp", "SingleUp", "FortyFiveDown"][index % 4];
    let record = Record {
        schema_version: 1,
        id: StableId::from_parts(["synthetic-user", "nightscout", "entry", &external_id]),
        provenance: provenance("nightscout", external_id.clone()),
        kind: RecordKind::NightscoutEntry,
        time: Some(TimeInterval::point(EventTime::utc(at_micros))),
        parent_id: None,
        provider_document: Some(ProviderDocument::json(
            serde_json::json!({
                "_id": external_id,
                "date": at_micros,
                "sgv": value,
                "direction": direction,
                "trend": index % 4,
                "device": "nightscout-synthetic-device",
                "synthetic": true
            })
            .to_string(),
        )),
    };
    (
        format!("nightscout/entries/{external_id}"),
        serde_json::to_vec(&record).unwrap(),
    )
}

#[test]
fn synthetic_glucose_supports_common_query_export_and_source_isolation() {
    let temp = TestDir::new();
    let store = Store::open(&temp.0).unwrap();
    let start = midnight_utc_micros();
    let mut measurements = Vec::with_capacity(READINGS_PER_DAY + 1);
    let mut records = Vec::with_capacity(READINGS_PER_DAY);

    for index in 0..READINGS_PER_DAY {
        let at = start + index as i64 * FIVE_MINUTES_MICROS;
        let value = 85 + (index as i64 % 36);
        let external_id = format!("ns-entry-{index:04}");
        measurements.push(glucose_measurement("nightscout", &external_id, at, value));
        records.push(nightscout_record(index, at, value));
    }

    // Same user, metric, context, and time as a Nightscout point, but sourced
    // from Polar. The source dimension must keep the streams separate.
    measurements.push(glucose_measurement(
        "polar",
        "polar-exercise-glucose-0001",
        start + 6 * 60 * 60 * 1_000_000,
        111,
    ));

    let record_refs = records
        .iter()
        .map(|(key, payload)| (key.as_str(), payload.as_slice()))
        .collect::<Vec<_>>();
    store.put_records(&record_refs).unwrap();
    for batch in measurements.chunks(64) {
        store.put_measurements(batch).unwrap();
    }
    let expected_count = READINGS_PER_DAY as u64 + 1;
    assert_eq!(store.measurement_count().unwrap(), expected_count);

    // Replaying the same provider IDs is an upsert, not an aggregate duplicate.
    store.put_records(&record_refs).unwrap();
    for batch in measurements.chunks(64) {
        store.put_measurements(batch).unwrap();
    }
    assert_eq!(store.measurement_count().unwrap(), expected_count);

    let first_half = store
        .measurement_page(MeasurementQuery {
            user_id: "synthetic-user",
            source: "nightscout",
            metric: "glucose",
            context: Some(&MeasurementContext::Nightscout),
            from_utc_micros: start,
            to_utc_micros: start + 12 * 60 * 60 * 1_000_000,
            limit: 500,
            after: None,
        })
        .unwrap();
    assert_eq!(first_half.rows.len(), 144);
    assert!(first_half.rows.iter().all(|measurement| {
        measurement.provenance.source.as_str() == "nightscout"
            && measurement.time.start.utc_micros().unwrap() < start + 12 * 60 * 60 * 1_000_000
    }));

    let polar = store
        .measurement_page(MeasurementQuery {
            user_id: "synthetic-user",
            source: "polar",
            metric: "glucose",
            context: Some(&MeasurementContext::Nightscout),
            from_utc_micros: start,
            to_utc_micros: start + 24 * 60 * 60 * 1_000_000,
            limit: 10,
            after: None,
        })
        .unwrap();
    assert_eq!(polar.rows.len(), 1);
    assert_eq!(polar.rows[0].provenance.source.as_str(), "polar");

    // Walk every page through the public, source-agnostic storage query API.
    let mut cursor: Option<String> = None;
    let mut queried = Vec::with_capacity(READINGS_PER_DAY);
    loop {
        let page = store
            .measurement_page(MeasurementQuery {
                user_id: "synthetic-user",
                source: "nightscout",
                metric: "glucose",
                context: Some(&MeasurementContext::Nightscout),
                from_utc_micros: start,
                to_utc_micros: start + 24 * 60 * 60 * 1_000_000,
                limit: 37,
                after: cursor.as_deref(),
            })
            .unwrap();
        queried.extend(page.rows.into_iter().map(|measurement| measurement.id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(queried.len(), READINGS_PER_DAY);
    let unique = queried.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), READINGS_PER_DAY);

    let first_record = store
        .get_record("nightscout/entries/ns-entry-0000")
        .unwrap()
        .unwrap();
    let first_record: Record = serde_json::from_slice(&first_record).unwrap();
    assert_eq!(first_record.kind, RecordKind::NightscoutEntry);
    let provider_document: serde_json::Value =
        serde_json::from_str(&first_record.provider_document.as_ref().unwrap().body).unwrap();
    assert_eq!(provider_document["direction"], "Flat");
    assert_eq!(provider_document["trend"], 0);
    assert_eq!(
        first_record.provenance.source_device_id.as_deref(),
        Some("nightscout-synthetic-device")
    );

    drop(store);

    // Exercise the CLI's shared JSONL and CSV export path without a daemon or
    // provider connection. Every row must retain explicit Nightscout provenance.
    let binary = env!("CARGO_BIN_EXE_rust-polar");
    let jsonl = Command::new(binary)
        .args([
            "--data-dir",
            temp.0.to_str().unwrap(),
            "export",
            "--user-id",
            "synthetic-user",
            "--source",
            "nightscout",
            "--metric",
            "glucose",
            "--from",
            "2026-10-01T00:00:00Z",
            "--to",
            "2026-10-02T00:00:00Z",
            "--format",
            "jsonl",
            "--limit",
            "50",
        ])
        .output()
        .unwrap();
    assert!(
        jsonl.status.success(),
        "{}",
        String::from_utf8_lossy(&jsonl.stderr)
    );
    let json_rows = String::from_utf8(jsonl.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(json_rows.len(), READINGS_PER_DAY);
    assert!(json_rows.iter().all(|row| {
        row["provenance"]["source"] == "nightscout"
            && row["metric"] == "glucose"
            && row["unit"] == "MilligramsPerDeciliter"
    }));

    let csv = Command::new(binary)
        .args([
            "--data-dir",
            temp.0.to_str().unwrap(),
            "export",
            "--user-id",
            "synthetic-user",
            "--source",
            "nightscout",
            "--metric",
            "glucose",
            "--from",
            "2026-10-01T00:00:00Z",
            "--to",
            "2026-10-02T00:00:00Z",
            "--format",
            "csv",
            "--limit",
            "50",
        ])
        .output()
        .unwrap();
    assert!(
        csv.status.success(),
        "{}",
        String::from_utf8_lossy(&csv.stderr)
    );
    let csv = String::from_utf8(csv.stdout).unwrap();
    assert_eq!(csv.lines().count(), READINGS_PER_DAY + 1);
    assert!(
        csv.lines()
            .next()
            .unwrap()
            .starts_with("id,user_id,source,metric,value,unit,")
    );
    assert!(
        csv.lines()
            .skip(1)
            .all(|line| line.contains(",nightscout,glucose,"))
    );
}

#[test]
fn synthetic_treatments_device_status_and_profiles_remain_queryable_documents() {
    let temp = TestDir::new();
    let store = Store::open(&temp.0).unwrap();
    let fetched_at = midnight_utc_micros();
    let cases = [
        (
            "treatments",
            "ns-treatment-1",
            RecordKind::NightscoutTreatment,
            serde_json::json!({
                "_id": "ns-treatment-1",
                "eventType": "Meal Bolus",
                "insulin": 1.25,
                "carbs": 18,
                "created_at": "2026-10-01T08:00:00Z",
                "notes": "synthetic treatment",
                "future_field": {"preserve": true}
            }),
        ),
        (
            "devicestatus",
            "ns-device-status-1",
            RecordKind::DeviceStatus,
            serde_json::json!({
                "_id": "ns-device-status-1",
                "device": "loop://synthetic",
                "created_at": "2026-10-01T08:01:00Z",
                "uploader": {"battery": 92},
                "future_field": [1, 2, 3]
            }),
        ),
        (
            "profile",
            "ns-profile-2026-10-01",
            RecordKind::Profile,
            serde_json::json!({
                "_id": "ns-profile-2026-10-01",
                "defaultProfile": "Adult",
                "store": {
                    "Adult": {
                        "dia": 4.0,
                        "carbratio": [{"time": "00:00", "value": 9.0}],
                        "sens": [{"time": "00:00", "value": 45.0}]
                    }
                },
                "synthetic": true
            }),
        ),
    ];

    let encoded = cases
        .iter()
        .map(|(resource, external_id, kind, document)| {
            let record = Record {
                schema_version: 1,
                id: StableId::from_external(
                    &UserId::new("synthetic-user"),
                    &SourceId::new("nightscout"),
                    resource,
                    external_id,
                ),
                provenance: Provenance {
                    user_id: UserId::new("synthetic-user"),
                    source: SourceId::new("nightscout"),
                    resource: (*resource).into(),
                    provider_external_id: Some((*external_id).into()),
                    source_device_id: Some("nightscout-synthetic-device".into()),
                    fetched_at_utc_micros: fetched_at,
                    archive_ref: None,
                    revision: Some("synthetic-v1".into()),
                    quality: DataQuality::ProviderReported,
                },
                kind: kind.clone(),
                time: None,
                parent_id: None,
                provider_document: Some(ProviderDocument::json(document.to_string())),
            };
            (
                record.id.as_str().to_owned(),
                serde_json::to_vec(&record).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let refs = encoded
        .iter()
        .map(|(key, payload)| (key.as_str(), payload.as_slice()))
        .collect::<Vec<_>>();
    store.put_records(&refs).unwrap();

    for ((resource, external_id, expected_kind, expected_document), (key, _)) in
        cases.iter().zip(encoded.iter())
    {
        let bytes = store
            .get_record(key)
            .unwrap()
            .expect("record was persisted");
        let actual: Record = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(&actual.kind, expected_kind);
        assert_eq!(actual.provenance.resource, *resource);
        assert_eq!(
            actual.provenance.provider_external_id.as_deref(),
            Some(*external_id)
        );
        let document: serde_json::Value =
            serde_json::from_str(&actual.provider_document.as_ref().unwrap().body).unwrap();
        assert_eq!(&document, expected_document);
    }
}
