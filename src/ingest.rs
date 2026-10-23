//! Reprocess durable source archives into provider records and normalized measurements.
use crate::model::{DataQuality, Measurement, Provenance, SourceId, UserId};
use crate::storage::Store;

pub fn replay_archive(
    store: &Store,
    resource: Option<&str>,
    user_id: &str,
    receipt_ids: Option<&std::collections::HashSet<String>>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let archive = crate::archive::ArchiveStore::new(store.archive_path().to_path_buf());
    let mut processed = 0usize;
    let mut measurement_batch = MeasurementBatch::new(store);
    for receipt in archive.receipts()? {
        let receipt = receipt?;
        store.register_archive(&receipt.receipt_id, &receipt)?;
        if receipt_ids.is_some_and(|ids| !ids.contains(&receipt.receipt_id)) {
            continue;
        }
        if resource.is_some_and(|wanted| wanted != receipt.resource) {
            continue;
        }
        if receipt.source != "polar" {
            store.set_archive_state(&receipt.receipt_id, "archived-only")?;
            continue;
        }
        if receipt.status == 204
            || receipt.status == 404
            || ((200..300).contains(&receipt.status) && receipt.uncompressed_bytes == 0)
        {
            store.set_archive_state(&receipt.receipt_id, "no-data")?;
            continue;
        }
        if !(200..300).contains(&receipt.status) {
            store.set_archive_state(&receipt.receipt_id, "http-error")?;
            continue;
        }
        let previous_processed = processed;
        archive.verify(&receipt)?;
        let mut reader = archive.read(&receipt)?;
        let mut batch = ProviderRecordBatch::new(store);
        let provenance = archive_provenance(&receipt, user_id);
        match receipt.resource.as_str() {
            "exercise-fit" | "exercise-tcx" | "exercise-gpx" => {
                if let Some(exercise_id) = receipt.request.get("exercise_id") {
                    let index = serde_json::json!({
                        "format": receipt.resource.strip_prefix("exercise-").unwrap_or("unknown"),
                        "exercise_id": exercise_id,
                        "archive_ref": receipt.content_hash,
                        "status": receipt.status,
                        "fetched_at": receipt.fetched_at,
                        "compressed_bytes": receipt.compressed_bytes,
                        "uncompressed_bytes": receipt.uncompressed_bytes
                    });
                    let json = serde_json::to_string(&index)?;
                    batch
                        .push(
                            &receipt.resource,
                            Some(exercise_id),
                            &receipt.content_hash,
                            &json,
                            Vec::new(),
                        )
                        .map_err(std::io::Error::other)?;
                    processed += 1;
                }
            }
            "exercises" => {
                crate::polar::exercise::for_each_exercise(&mut reader, |item| {
                    let provider_json = item.provider_json().map_err(|e| e.to_string())?;
                    let id = item
                        .id
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| item.id.to_string());
                    let normalized = item.normalized_metrics().metrics;
                    let metrics = normalized
                        .iter()
                        .map(|metric| {
                            serde_json::json!({
                                "metric": metric.metric.as_str(),
                                "value": metric.value.as_str(),
                                "unit": format!("{:?}", metric.unit),
                                "aggregation": format!("{:?}", metric.aggregation),
                                "context": format!("{:?}", metric.context),
                                "source_time": metric.source_time,
                                "sample_type": metric.sample_type,
                                "sample_index": metric.sample_index
                            })
                        })
                        .collect();
                    // Stream corrected points into an invisible generation.
                    // Queries keep seeing the complete old generation until
                    // every current point has been written and activated.
                    measurement_batch.flush()?;
                    let replacement = store
                        .begin_parent_measurement_replacement(item.stable_id().as_str())
                        .map_err(|error| error.to_string())?;
                    let mut staged = StagedMeasurementBatch::new(store, &replacement);
                    staged
                        .extend(
                            normalized
                                .into_iter()
                                .filter_map(|metric| metric.into_measurement(provenance.clone())),
                        )
                        .map_err(|error| error.to_string())?;
                    let expected_rows = staged.rows_written();
                    staged.flush().map_err(|error| error.to_string())?;
                    store
                        .activate_parent_measurement_replacement(&replacement, expected_rows)
                        .map_err(|error| error.to_string())?;
                    // Cleanup is bounded; later runs can continue reclaiming
                    // obsolete generations without delaying this replay.
                    store
                        .cleanup_parent_measurement_versions(item.stable_id().as_str(), 1000)
                        .map_err(|error| error.to_string())?;
                    batch.push(
                        &receipt.resource,
                        Some(&id),
                        &receipt.content_hash,
                        &provider_json,
                        metrics,
                    )
                })?;
                processed += 1;
            }
            "exercise-detail" => {
                let item: crate::polar::exercise::Exercise = serde_json::from_reader(&mut reader)?;
                let id = item
                    .id
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| item.id.to_string());
                let provider_json = item.provider_json()?;
                batch
                    .push(
                        &receipt.resource,
                        Some(&id),
                        &receipt.content_hash,
                        &provider_json,
                        Vec::new(),
                    )
                    .map_err(std::io::Error::other)?;
                processed += 1;
            }
            "activities" | "daily-activity" | "activity" => {
                crate::polar::activity::for_each_daily_activity(&mut reader, |item| {
                    measurement_batch.flush()?;
                    if let Some(date) = item.date() {
                        persist_recovery_parent(
                            store,
                            &provenance,
                            "activities",
                            Some(date),
                            date,
                            item.normalize_measurements(&provenance),
                        )?;
                    }
                    let provider_json = item.provider_json().map_err(|e| e.to_string())?;
                    let mut metrics = item
                        .normalized_totals()
                        .into_iter()
                        .map(|metric| {
                            serde_json::json!({
                                "metric": metric.metric,
                                "value": metric.value.as_str(),
                                "unit": format!("{:?}", metric.unit)
                            })
                        })
                        .collect::<Vec<_>>();
                    if let Some(samples) = item.samples.as_ref().and_then(|s| s.steps.as_ref()) {
                        metrics.extend(samples.samples.iter().filter_map(|sample| {
                            Some(serde_json::json!({
                                "metric": "step-sample",
                                "value": sample.steps.as_ref()?.as_number()?.to_string(),
                                "unit": "Steps",
                                "timestamp": sample.timestamp,
                                "time_resolution": "local-unresolved"
                            }))
                        }));
                    }
                    batch.push(
                        &receipt.resource,
                        item.date(),
                        &receipt.content_hash,
                        &provider_json,
                        metrics,
                    )
                })?;
                processed += 1;
            }
            "continuous-heart-rate" => {
                let item = crate::polar::physiology::decode_continuous_heart_rate(&mut reader)?;
                measurement_batch.extend(item.normalize_measurements(&provenance))?;
                let metrics = item
                    .heart_rate_samples
                    .iter()
                    .filter_map(|sample| {
                        let value = sample.heart_rate.as_ref()?.as_number()?.to_string();
                        Some(serde_json::json!({
                            "metric": "heart-rate",
                            "value": value,
                            "unit": "BeatsPerMinute",
                            "sample_time": sample.sample_time,
                            "date": item.date
                        }))
                    })
                    .collect();
                let json = serde_json::to_string(&item)?;
                batch
                    .push(
                        &receipt.resource,
                        Some(&item.date),
                        &receipt.content_hash,
                        &json,
                        metrics,
                    )
                    .map_err(std::io::Error::other)?;
                processed += 1;
            }
            "cardio-load" | "cardio-load-history" => {
                crate::polar::physiology::for_each_cardio_load(&mut reader, |item| {
                    let metrics = [
                        ("cardio-load", item.cardio_load.as_ref()),
                        ("strain", item.strain.as_ref()),
                        ("tolerance", item.tolerance.as_ref()),
                        ("cardio-load-ratio", item.cardio_load_ratio.as_ref()),
                    ]
                    .into_iter()
                    .filter_map(|(name, value)| {
                        Some(serde_json::json!({
                            "metric": name,
                            "value": value?.as_number()?.to_string(),
                            "aggregation": "ProviderScore"
                        }))
                    })
                    .collect();
                    let date = item.date.clone();
                    let json = serde_json::to_string(&item).map_err(|e| e.to_string())?;
                    batch.push(
                        &receipt.resource,
                        Some(&date),
                        &receipt.content_hash,
                        &json,
                        metrics,
                    )
                })?;
                processed += 1;
            }
            "user-profile" => {
                let profile = crate::polar::profile::decode_user_profile(&mut reader)?;
                let json = serde_json::to_string(&profile)?;
                batch
                    .push(
                        &receipt.resource,
                        Some("self"),
                        &receipt.content_hash,
                        &json,
                        Vec::new(),
                    )
                    .map_err(std::io::Error::other)?;
                processed += 1;
            }
            "physical-info" => {
                let info = crate::polar::profile::decode_physical_info(&mut reader)?;
                let json = serde_json::to_string(&info)?;
                batch
                    .push(
                        &receipt.resource,
                        Some("self"),
                        &receipt.content_hash,
                        &json,
                        Vec::new(),
                    )
                    .map_err(std::io::Error::other)?;
                processed += 1;
            }
            "sleep" => {
                crate::polar::recovery::for_each_sleep(&mut reader, |item| {
                    let normalized = item.normalized_scalars();
                    measurement_batch.flush()?;
                    persist_recovery_parent(
                        store,
                        &provenance,
                        &receipt.resource,
                        item.date.as_deref(),
                        &receipt.content_hash,
                        recovery_measurements(
                            normalized.clone(),
                            &provenance,
                            item.date.as_deref(),
                        ),
                    )?;
                    let metrics = recovery_metrics(normalized);
                    let date = item.date.clone();
                    let json = serde_json::to_string(&item).map_err(|e| e.to_string())?;
                    batch.push(
                        &receipt.resource,
                        date.as_deref(),
                        &receipt.content_hash,
                        &json,
                        metrics,
                    )
                })?;
                processed += 1;
            }
            "nightly-recharge" => {
                crate::polar::recovery::for_each_nightly_recharge(&mut reader, |item| {
                    let normalized = item.normalized_scalars();
                    measurement_batch.flush()?;
                    persist_recovery_parent(
                        store,
                        &provenance,
                        &receipt.resource,
                        item.date.as_deref(),
                        &receipt.content_hash,
                        recovery_measurements(
                            normalized.clone(),
                            &provenance,
                            item.date.as_deref(),
                        ),
                    )?;
                    let metrics = recovery_metrics(normalized);
                    let date = item.date.clone();
                    let json = serde_json::to_string(&item).map_err(|e| e.to_string())?;
                    batch.push(
                        &receipt.resource,
                        date.as_deref(),
                        &receipt.content_hash,
                        &json,
                        metrics,
                    )
                })?;
                processed += 1;
            }
            "sleepwise-circadian-bedtime" => {
                crate::polar::recovery::for_each_circadian_bedtime(&mut reader, |item| {
                    let windows = item.resolved_windows();
                    let date = item
                        .period_start_time
                        .as_deref()
                        .and_then(|time| time.get(..10))
                        .map(str::to_owned);
                    measurement_batch.flush()?;
                    persist_recovery_parent(
                        store,
                        &provenance,
                        &receipt.resource,
                        date.as_deref(),
                        &receipt.content_hash,
                        recovery_window_measurements(&windows, &provenance),
                    )?;
                    let json = serde_json::to_string(&item).map_err(|e| e.to_string())?;
                    let metrics = recovery_window_metrics(&windows);
                    batch.push(
                        &receipt.resource,
                        date.as_deref(),
                        &receipt.content_hash,
                        &json,
                        metrics,
                    )
                })?;
                processed += 1;
            }
            resource if resource.starts_with("sleepwise") => {
                crate::polar::recovery::for_each_sleepwise(&mut reader, |item| {
                    let normalized = item.normalized_scalars();
                    let windows = item.resolved_hourly_windows();
                    let date = item.date.clone().or_else(|| {
                        item.period_start_time
                            .as_deref()
                            .and_then(|time| time.get(..10).map(str::to_owned))
                    });
                    measurement_batch.flush()?;
                    let mut measurements = recovery_measurements(
                        normalized.clone(),
                        &provenance,
                        item.date.as_deref(),
                    );
                    measurements.extend(recovery_window_measurements(&windows, &provenance));
                    persist_recovery_parent(
                        store,
                        &provenance,
                        &receipt.resource,
                        date.as_deref(),
                        &receipt.content_hash,
                        measurements,
                    )?;
                    let mut metrics = recovery_metrics(normalized);
                    metrics.extend(recovery_window_metrics(&windows));
                    let json = serde_json::to_string(&item).map_err(|e| e.to_string())?;
                    batch.push(
                        &receipt.resource,
                        date.as_deref(),
                        &receipt.content_hash,
                        &json,
                        metrics,
                    )
                })?;
                processed += 1;
            }
            _ => {}
        }
        batch.flush().map_err(std::io::Error::other)?;
        measurement_batch.flush()?;
        store.set_archive_state(
            &receipt.receipt_id,
            if processed > previous_processed {
                "processed"
            } else {
                "archived-only"
            },
        )?;
    }
    Ok(processed)
}

fn recovery_metrics(
    metrics: Vec<crate::polar::recovery::RecoveryMetric>,
) -> Vec<serde_json::Value> {
    metrics
        .into_iter()
        .map(|metric| {
            serde_json::json!({
                "metric": metric.metric,
                "value": metric.value.as_str(),
                "unit": format!("{:?}", metric.unit),
                "aggregation": format!("{:?}", metric.aggregation),
                "context": format!("{:?}", metric.context)
            })
        })
        .collect()
}

fn archive_provenance(receipt: &crate::archive::ArchiveReceipt, user_id: &str) -> Provenance {
    let parsed_fetch_time = time::OffsetDateTime::parse(
        &receipt.fetched_at,
        &time::format_description::well_known::Rfc3339,
    )
    .ok()
    .and_then(|time| i64::try_from(time.unix_timestamp_nanos() / 1_000).ok());
    Provenance {
        user_id: UserId::new(user_id),
        source: SourceId::new(receipt.source.clone()),
        resource: receipt.resource.clone(),
        provider_external_id: None,
        source_device_id: None,
        // Older/manual archive descriptors may omit a valid RFC3339 fetch
        // timestamp. Keep a sentinel and label the provenance as unknown.
        fetched_at_utc_micros: parsed_fetch_time.unwrap_or(0),
        archive_ref: Some(receipt.content_hash.clone()),
        revision: Some(receipt.content_hash.clone()),
        quality: if parsed_fetch_time.is_some() {
            DataQuality::ProviderReported
        } else {
            DataQuality::Unknown("fetch-time-unavailable".into())
        },
    }
}

/// Atomically replace one recovery/SleepWise record's queryable child rows.
/// These records can contain hundreds of samples; deleting/inserting each
/// correction directly would expose mixed generations and leave removed
/// samples queryable. The record payload itself remains independently stored
/// in the bounded provider-record batch.
fn persist_recovery_parent(
    store: &crate::storage::Store,
    provenance: &Provenance,
    resource: &str,
    record_identity: Option<&str>,
    fallback_identity: &str,
    mut measurements: Vec<Measurement>,
) -> Result<(), String> {
    let identity = record_identity.unwrap_or(fallback_identity);
    let parent_id = crate::model::StableId::from_parts([
        provenance.user_id.as_str(),
        provenance.source.as_str(),
        resource,
        identity,
    ]);
    for measurement in &mut measurements {
        measurement.parent_id = Some(parent_id.clone());
    }

    let replacement = store
        .begin_parent_measurement_replacement(parent_id.as_str())
        .map_err(|error| error.to_string())?;
    let mut staged = StagedMeasurementBatch::new(store, &replacement);
    staged.extend(measurements)?;
    let expected_rows = staged.rows_written();
    staged.flush()?;
    store
        .activate_parent_measurement_replacement(&replacement, expected_rows)
        .map_err(|error| error.to_string())?;
    while store
        .cleanup_parent_measurement_versions(parent_id.as_str(), 1000)
        .map_err(|error| error.to_string())?
        != 0
    {}
    Ok(())
}

fn recovery_measurements(
    metrics: Vec<crate::polar::recovery::RecoveryMetric>,
    provenance: &Provenance,
    record_date: Option<&str>,
) -> Vec<Measurement> {
    metrics
        .into_iter()
        .enumerate()
        .filter_map(|(index, metric)| {
            let date = metric.source_date.as_deref().or(record_date);
            let time =
                metric
                    .resolved_time
                    .clone()
                    .or_else(|| match metric.sample_time.as_deref() {
                        Some(sample_time) => {
                            crate::polar::activity::parse_event_time(date, sample_time)
                        }
                        None => date.and_then(|date| {
                            crate::polar::activity::parse_event_time(Some(date), "00:00:00")
                        }),
                    })?;
            let external_id = format!(
                "{}:{}:{}",
                date.unwrap_or("unknown-date"),
                metric.metric,
                index
            );
            let mut record_provenance = provenance.clone();
            record_provenance.provider_external_id = Some(external_id.clone());
            record_provenance.quality = DataQuality::ProviderReported;
            Some(Measurement {
                schema_version: 1,
                id: crate::model::StableId::from_parts([
                    provenance.user_id.as_str(),
                    provenance.source.as_str(),
                    &provenance.resource,
                    &external_id,
                ]),
                provenance: record_provenance,
                metric: crate::model::MetricId::new(format!(
                    "polar.{}.{}",
                    provenance.resource, metric.metric
                )),
                value: metric.value,
                unit: metric.unit,
                time: crate::model::TimeInterval::point(time),
                aggregation: metric.aggregation,
                context: metric.context,
                parent_id: None,
            })
        })
        .collect()
}

fn recovery_window_measurements(
    windows: &[crate::polar::recovery::ResolvedRecoveryWindow],
    provenance: &Provenance,
) -> Vec<Measurement> {
    windows
        .iter()
        .enumerate()
        .filter_map(|(index, window)| {
            if !window.interval.is_utc_resolved() {
                return None;
            }
            let start = window.interval.start.utc_micros()?;
            let end = window
                .interval
                .end
                .as_ref()
                .and_then(|time| time.utc_micros());
            let external_id = format!(
                "{}:{start}:{}:{index}",
                window.kind,
                end.map_or_else(|| "point".to_owned(), |value| value.to_string())
            );
            let mut row_provenance = provenance.clone();
            row_provenance.provider_external_id = Some(external_id.clone());
            row_provenance.quality = DataQuality::ProviderReported;
            Some(Measurement {
                schema_version: 1,
                id: crate::model::StableId::from_parts([
                    provenance.user_id.as_str(),
                    provenance.source.as_str(),
                    &provenance.resource,
                    &external_id,
                ]),
                provenance: row_provenance,
                metric: crate::model::MetricId::new(format!(
                    "polar.{}.{}",
                    provenance.resource, window.kind
                )),
                value: crate::model::NumericValue::new("1")
                    .expect("integer literal is a valid numeric value"),
                unit: crate::model::Unit::Count,
                time: window.interval.clone(),
                aggregation: crate::model::Aggregation::Count,
                context: crate::model::MeasurementContext::SleepWise,
                parent_id: None,
            })
        })
        .collect()
}

fn recovery_window_metrics(
    windows: &[crate::polar::recovery::ResolvedRecoveryWindow],
) -> Vec<serde_json::Value> {
    windows
        .iter()
        .map(|window| {
            serde_json::json!({
                "metric": window.kind,
                "start_utc_micros": window.interval.start.utc_micros(),
                "end_utc_micros": window.interval.end.as_ref().and_then(|time| time.utc_micros()),
                "unit": "Count",
                "aggregation": "Count"
            })
        })
        .collect()
}

struct MeasurementBatch<'a> {
    store: &'a crate::storage::Store,
    measurements: Vec<Measurement>,
    encoded_bytes: usize,
}

struct StagedMeasurementBatch<'a> {
    store: &'a crate::storage::Store,
    replacement: &'a crate::storage::ParentMeasurementReplacement,
    measurements: Vec<Measurement>,
    encoded_bytes: usize,
    rows_written: usize,
}

impl<'a> StagedMeasurementBatch<'a> {
    fn new(
        store: &'a crate::storage::Store,
        replacement: &'a crate::storage::ParentMeasurementReplacement,
    ) -> Self {
        Self {
            store,
            replacement,
            measurements: Vec::new(),
            encoded_bytes: 0,
            rows_written: 0,
        }
    }

    fn rows_written(&self) -> usize {
        self.rows_written + self.measurements.len()
    }

    fn extend(
        &mut self,
        measurements: impl IntoIterator<Item = Measurement>,
    ) -> Result<(), String> {
        for measurement in measurements {
            if !measurement.time.is_utc_resolved() {
                continue;
            }
            let bytes = serde_json::to_vec(&measurement).map_err(|error| error.to_string())?;
            if bytes.len() > MeasurementBatch::MAX_BYTES {
                return Err("one normalized measurement exceeds the 512 KiB batch limit".into());
            }
            if !self.measurements.is_empty()
                && (self.measurements.len() >= MeasurementBatch::MAX_ROWS
                    || self.encoded_bytes.saturating_add(bytes.len()) > MeasurementBatch::MAX_BYTES)
            {
                self.flush()?;
            }
            self.encoded_bytes = self.encoded_bytes.saturating_add(bytes.len());
            self.measurements.push(measurement);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.measurements.is_empty() {
            return Ok(());
        }
        self.store
            .stage_parent_measurements(self.replacement, &self.measurements)
            .map_err(|error| error.to_string())?;
        self.rows_written = self.rows_written.saturating_add(self.measurements.len());
        self.measurements.clear();
        self.encoded_bytes = 0;
        Ok(())
    }
}

impl<'a> MeasurementBatch<'a> {
    const MAX_ROWS: usize = 1000;
    const MAX_BYTES: usize = 512 * 1024;

    fn new(store: &'a crate::storage::Store) -> Self {
        Self {
            store,
            measurements: Vec::new(),
            encoded_bytes: 0,
        }
    }

    fn extend(
        &mut self,
        measurements: impl IntoIterator<Item = Measurement>,
    ) -> Result<(), String> {
        for measurement in measurements {
            // Keep provider-local times in the record document. The series
            // index accepts only times that Polar resolved to UTC.
            if !measurement.time.is_utc_resolved() {
                continue;
            }
            let bytes = serde_json::to_vec(&measurement).map_err(|error| error.to_string())?;
            if bytes.len() > Self::MAX_BYTES {
                return Err("one normalized measurement exceeds the 512 KiB batch limit".into());
            }
            if !self.measurements.is_empty()
                && (self.measurements.len() >= Self::MAX_ROWS
                    || self.encoded_bytes.saturating_add(bytes.len()) > Self::MAX_BYTES)
            {
                self.flush()?;
            }
            self.encoded_bytes = self.encoded_bytes.saturating_add(bytes.len());
            self.measurements.push(measurement);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.measurements.is_empty() {
            return Ok(());
        }
        self.store
            .put_measurements(&self.measurements)
            .map_err(|error| error.to_string())?;
        self.measurements.clear();
        self.encoded_bytes = 0;
        Ok(())
    }
}

struct ProviderRecordBatch<'a> {
    store: &'a crate::storage::Store,
    records: Vec<(String, Vec<u8>)>,
    encoded_bytes: usize,
}

impl<'a> ProviderRecordBatch<'a> {
    const MAX_ROWS: usize = 1000;
    const MAX_BYTES: usize = 512 * 1024;

    fn new(store: &'a crate::storage::Store) -> Self {
        Self {
            store,
            records: Vec::new(),
            encoded_bytes: 0,
        }
    }

    fn push(
        &mut self,
        resource: &str,
        external_id: Option<&str>,
        archive_ref: &str,
        provider_json: &str,
        metrics: Vec<serde_json::Value>,
    ) -> Result<(), String> {
        const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;
        if provider_json.len() > MAX_RECORD_BYTES {
            return Err(format!("provider record exceeds {MAX_RECORD_BYTES} bytes"));
        }
        let identity = external_id.map(str::to_owned).unwrap_or_else(|| {
            format!(
                "unknown-{}",
                blake3::hash(provider_json.as_bytes()).to_hex()
            )
        });
        let key = format!("polar/{resource}/{identity}");
        let record =
            serde_json::from_str::<serde_json::Value>(provider_json).map_err(|e| e.to_string())?;
        let payload = serde_json::json!({
            "archive_ref": archive_ref,
            "resource": resource,
            "external_id": external_id,
            "metrics": metrics,
            "record": record
        });
        let bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(format!(
                "serialized provider record exceeds {MAX_RECORD_BYTES} bytes"
            ));
        }
        let row_bytes = key.len().saturating_add(bytes.len());
        if !self.records.is_empty()
            && (self.records.len() >= Self::MAX_ROWS
                || self.encoded_bytes.saturating_add(row_bytes) > Self::MAX_BYTES)
        {
            self.flush()?;
        }
        self.encoded_bytes = self.encoded_bytes.saturating_add(row_bytes);
        self.records.push((key, bytes));
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.records.is_empty() {
            return Ok(());
        }
        let records = self
            .records
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_slice()))
            .collect::<Vec<_>>();
        self.store
            .put_records(&records)
            .map_err(|error| error.to_string())?;
        self.records.clear();
        self.encoded_bytes = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{ArchiveStore, RequestMetadata};
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn exercise_detail_replay_keeps_full_record_separate_from_list_metrics() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "rust-polar-exercise-detail-{}-{nonce}",
            std::process::id()
        ));
        let store = Store::open(&root).unwrap();
        let archive = ArchiveStore::new(store.archive_path().to_path_buf());
        let mut request = RequestMetadata::new();
        request.insert("exercise_id".into(), "exercise-123".into());
        let body = br#"{"id":"exercise-123","sport":"RUNNING","training_load":42,"new_provider_field":{"kept":true}}"#;
        let receipt = archive
            .store_body(
                "polar",
                "exercise-detail",
                request,
                200,
                "2026-10-22T12:00:00Z",
                Cursor::new(body.as_slice()),
            )
            .unwrap();
        assert_eq!(
            replay_archive(&store, Some("exercise-detail"), "user-1", None).unwrap(),
            1
        );
        let saved = store
            .get_record("polar/exercise-detail/exercise-123")
            .unwrap()
            .unwrap();
        let saved: serde_json::Value = serde_json::from_slice(&saved).unwrap();
        assert_eq!(saved["resource"], "exercise-detail");
        assert_eq!(saved["record"]["sport"], "RUNNING");
        assert_eq!(saved["record"]["new_provider_field"]["kept"], true);
        assert_eq!(saved["metrics"].as_array().unwrap().len(), 0);
        assert_eq!(store.measurement_count().unwrap(), 0);
        drop(store);
        let _ = std::fs::remove_dir_all(root);
        assert!(!receipt.receipt_id.is_empty());
    }

    #[test]
    fn corrected_sleep_samples_atomically_replace_removed_series_rows() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "rust-polar-recovery-replay-{}-{nonce}",
            std::process::id()
        ));
        let store = Store::open(&root).unwrap();
        let archive = ArchiveStore::new(store.archive_path().to_path_buf());
        let old = br#"{"nights":[{"date":"2026-10-20","heart_rate_samples":{
          "2026-10-20T02:00:00+02:00":50,
          "2026-10-20T02:05:00+02:00":51
        }}]}"#;
        let first = archive
            .store_body(
                "polar",
                "sleep",
                RequestMetadata::new(),
                200,
                "2026-10-21T12:00:00Z",
                Cursor::new(old.as_slice()),
            )
            .unwrap();
        assert_eq!(
            replay_archive(&store, Some("sleep"), "user-1", None).unwrap(),
            1
        );
        assert_eq!(store.measurement_count().unwrap(), 2);

        let corrected = br#"{"nights":[{"date":"2026-10-20","heart_rate_samples":{
          "2026-10-20T02:00:00+02:00":52
        }}]}"#;
        let second = archive
            .store_body(
                "polar",
                "sleep",
                RequestMetadata::new(),
                200,
                "2026-10-22T12:00:00Z",
                Cursor::new(corrected.as_slice()),
            )
            .unwrap();
        let receipt_ids = [second.receipt_id.clone()].into_iter().collect();
        assert_eq!(
            replay_archive(&store, Some("sleep"), "user-1", Some(&receipt_ids)).unwrap(),
            1
        );
        assert_eq!(
            store.measurement_count().unwrap(),
            1,
            "the removed sample from the prior sleep generation must not remain queryable"
        );

        let second_ids = [second.receipt_id].into_iter().collect();
        assert_eq!(
            replay_archive(&store, Some("sleep"), "user-1", Some(&second_ids)).unwrap(),
            1
        );
        assert_eq!(store.measurement_count().unwrap(), 1);
        drop(store);
        let _ = std::fs::remove_dir_all(root);
        assert!(!first.receipt_id.is_empty());
    }

    #[test]
    fn replay_marks_empty_and_not_found_responses_as_no_data() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "rust-polar-empty-replay-{}-{nonce}",
            std::process::id()
        ));
        let store = Store::open(&root).unwrap();
        let archive = ArchiveStore::new(store.archive_path().to_path_buf());
        for (status, body) in [(204, &b""[..]), (404, &b"not found"[..]), (200, &b""[..])] {
            archive
                .store_body(
                    "polar",
                    "activities",
                    RequestMetadata::new(),
                    status,
                    "2026-10-21T12:00:00Z",
                    Cursor::new(body),
                )
                .unwrap();
        }

        assert_eq!(replay_archive(&store, None, "user-1", None).unwrap(), 0);
        assert!(store.pending_archive_ids(10).unwrap().is_empty());
        assert_eq!(store.record_count().unwrap(), 0);

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }
}
