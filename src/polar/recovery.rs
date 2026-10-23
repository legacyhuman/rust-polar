//! Streaming decoders for Polar sleep, Nightly Recharge, and SleepWise data.
//!
//! Each top-level response array is consumed one record at a time. The caller
//! should retain the original response in the archive; this module also keeps
//! unrecognized fields and nested sample objects for forward compatibility.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::marker::PhantomData;

use serde::de::DeserializeSeed;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;

use crate::model::{
    Aggregation, EventTime, MeasurementContext, NumericValue, TimeInterval, TimeResolution, Unit,
};
use time::format_description::well_known::{Iso8601, Rfc3339};
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

/// Polar sleep record. Date and clock strings are kept as reported; local
/// values are intentionally not assigned an invented timezone or UTC instant.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SleepRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_score: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_charge: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_goal: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_rating: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_class: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_sleep_time: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub light_sleep: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deep_sleep: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rem_sleep: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unrecognized_sleep_stage: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_interruption_duration: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_interruption_duration: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long_interruption_duration: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_cycles: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_duration_score: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_solidity_score: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_regeneration_score: Option<Value>,
    /// Hypnogram/stage data is intentionally open-ended: new stage codes and
    /// sample shapes must survive decoding unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hypnogram: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heart_rate_samples: Option<Value>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// Nightly Recharge summary and provider samples. The source's date remains a
/// date, and sample timestamps stay in their original provider representation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NightlyRechargeRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heart_rate_avg: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beat_to_beat_avg: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heart_rate_variability_avg: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breathing_rate_avg: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ans_charge: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ans_charge_status: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nightly_recharge_status: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hrv_samples: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breathing_samples: Option<Value>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// SleepWise resource record. Scores, classifications, and time windows use
/// Polar's own scales and labels; no generic health percentage is inferred.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SleepWiseRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alertness: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grade: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grade_validity_seconds: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grade_type: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grade_classification: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validity: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_inertia: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_type: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_type: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_gate: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_gate_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_gate_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_sleep_period: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_period_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_period_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_sleep_period_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_sleep_period_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_timezone_offset_minutes: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<Value>,
    /// Hourly predictions/values retain provider codes and shape verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hourly_data: Option<Vec<SleepWiseHourlyData>>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// One documented hourly alertness interval. Classifications remain strings
/// rather than being collapsed into a fabricated numeric alertness score.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SleepWiseHourlyData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validity: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alertness_level: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    /// Current alertness endpoint shape uses `time` and a numeric `alertness`.
    /// Keep both this and the interval-style legacy fields open and lossless.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alertness: Option<Value>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// An open-ended provider sample. AccessLink evolves sample codes and may add
/// fields; `provider_fields` preserves every field not currently modeled.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecoverySample {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// A scalar with semantics that are unambiguous in the AccessLink contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryMetric {
    pub metric: &'static str,
    pub value: NumericValue,
    pub unit: Unit,
    pub aggregation: Aggregation,
    pub context: MeasurementContext,
    /// Provider calendar date when available. This is deliberately not an
    /// implicit UTC midnight or a timezone-resolved instant.
    pub source_date: Option<String>,
    /// Exact sample clock/timestamp string supplied by Polar, when present.
    pub sample_time: Option<String>,
    /// UTC-resolved time only when the timestamp carried an explicit offset or
    /// its record supplied the exact timezone offset used for that timestamp.
    pub resolved_time: Option<EventTime>,
}

/// A SleepWise time window whose source-local times were resolved with the
/// response's explicit `sleep_timezone_offset_minutes` value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedRecoveryWindow {
    pub kind: &'static str,
    pub interval: TimeInterval,
}

/// Result of bounded normalization. `truncated` signals that more provider
/// scalars or samples were present than the requested output limit allowed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedMetrics {
    pub metrics: Vec<RecoveryMetric>,
    pub truncated: bool,
}

/// Open-valued sleep-stage sample. `stage_code` is the provider's raw stage
/// code; unknown codes are deliberately not mapped to a known sleep phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SleepStageSample {
    pub source_date: Option<String>,
    pub sample_time: String,
    pub stage_code: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SleepStageSamples {
    pub samples: Vec<SleepStageSample>,
    pub truncated: bool,
}

/// Conservative cap used by compatibility accessors and default importer paths.
pub const DEFAULT_NORMALIZED_METRIC_LIMIT: usize = 4096;

impl SleepRecord {
    /// Return raw hypnogram transitions with provider time labels and codes.
    /// Polar reports these as local sleep-relative clock values in the
    /// documented shape, so this does not resolve them to UTC.
    pub fn hypnogram_bounded(&self, limit: usize) -> SleepStageSamples {
        let mut samples = Vec::with_capacity(limit.min(256));
        let mut truncated = false;
        let mut append = |time: &str, stage: &Value| {
            if samples.len() == limit {
                truncated = true;
            } else {
                samples.push(SleepStageSample {
                    source_date: self.date.clone(),
                    sample_time: time.to_owned(),
                    stage_code: stage.clone(),
                });
            }
        };
        match self.hypnogram.as_ref() {
            Some(Value::Object(object)) => {
                for (time, stage) in object {
                    append(time, stage);
                }
            }
            Some(Value::Array(array)) => {
                for stage in array {
                    let Some(time) = stage
                        .get("time")
                        .or_else(|| stage.get("sample_time"))
                        .and_then(Value::as_str)
                    else {
                        continue;
                    };
                    let Some(code) = stage.get("stage").or_else(|| stage.get("value")) else {
                        continue;
                    };
                    append(time, code);
                }
            }
            _ => {}
        }
        SleepStageSamples { samples, truncated }
    }

    /// Bounded typed outputs. Polar documents sleep durations in seconds and
    /// heart-rate values in beats/minute; provider-defined scores keep their
    /// own named units. Local sample clocks remain unresolved strings.
    pub fn normalized_scalars(&self) -> Vec<RecoveryMetric> {
        self.normalized_scalars_bounded(DEFAULT_NORMALIZED_METRIC_LIMIT)
            .metrics
    }

    pub fn normalized_scalars_bounded(&self, limit: usize) -> NormalizedMetrics {
        let mut out = MetricCollector::new(limit);
        push_value(
            &mut out,
            "sleep-score",
            &self.sleep_score,
            Unit::Unknown("polar-sleep-score".into()),
            Aggregation::ProviderScore,
            MeasurementContext::Sleep,
            self.date.as_deref(),
        );
        push_value(
            &mut out,
            "sleep-charge",
            &self.sleep_charge,
            Unit::Unknown("polar-sleep-charge".into()),
            Aggregation::ProviderScore,
            MeasurementContext::Sleep,
            self.date.as_deref(),
        );
        // Total sleep time is a documented duration scalar in seconds. It was
        // preserved in the parsed record already, but was missing from the
        // normalized metrics emitted for queries.
        push_value(
            &mut out,
            "total-sleep-time",
            &self.total_sleep_time,
            Unit::Seconds,
            Aggregation::Sum,
            MeasurementContext::Sleep,
            self.date.as_deref(),
        );
        for (name, field) in [
            ("light-sleep", &self.light_sleep),
            ("deep-sleep", &self.deep_sleep),
            ("rem-sleep", &self.rem_sleep),
            ("unrecognized-sleep-stage", &self.unrecognized_sleep_stage),
            (
                "total-interruption-duration",
                &self.total_interruption_duration,
            ),
            (
                "short-interruption-duration",
                &self.short_interruption_duration,
            ),
            (
                "long-interruption-duration",
                &self.long_interruption_duration,
            ),
            ("sleep-goal", &self.sleep_goal),
        ] {
            push_value(
                &mut out,
                name,
                field,
                Unit::Seconds,
                Aggregation::Sum,
                MeasurementContext::Sleep,
                self.date.as_deref(),
            );
        }
        for (name, field) in [
            ("sleep-rating", &self.sleep_rating),
            ("continuity", &self.continuity),
            ("group-duration-score", &self.group_duration_score),
            ("group-solidity-score", &self.group_solidity_score),
            ("group-regeneration-score", &self.group_regeneration_score),
        ] {
            push_value(
                &mut out,
                name,
                field,
                Unit::Unknown(format!("polar-{name}")),
                Aggregation::ProviderScore,
                MeasurementContext::Sleep,
                self.date.as_deref(),
            );
        }
        push_value(
            &mut out,
            "sleep-cycles",
            &self.sleep_cycles,
            Unit::Count,
            Aggregation::Count,
            MeasurementContext::Sleep,
            self.date.as_deref(),
        );
        push_timed_samples(
            &mut out,
            "heart-rate",
            self.heart_rate_samples.as_ref(),
            Unit::BeatsPerMinute,
            MeasurementContext::Sleep,
            self.date.as_deref(),
            None,
        );
        // Preserve stage labels verbatim on the provider record. Numeric stage
        // codes can additionally be indexed as opaque categorical codes when
        // the sample carries a timestamp; no human sleep phase is inferred.
        let stages = self.hypnogram_bounded(out.remaining());
        for sample in stages.samples {
            let Value::Number(number) = &sample.stage_code else {
                continue;
            };
            let Ok(value) = NumericValue::new(number.to_string()) else {
                continue;
            };
            out.push(RecoveryMetric {
                metric: "sleep-stage-code",
                value,
                unit: Unit::Unknown("polar-sleep-stage-code".into()),
                aggregation: Aggregation::Unknown("categorical-code".into()),
                context: MeasurementContext::Sleep,
                source_date: sample.source_date,
                sample_time: Some(sample.sample_time.clone()),
                resolved_time: resolve_provider_time(&sample.sample_time, None),
            });
        }
        if stages.truncated {
            out.mark_truncated();
        }
        out.finish()
    }
}

impl NightlyRechargeRecord {
    pub fn normalized_scalars(&self) -> Vec<RecoveryMetric> {
        self.normalized_scalars_bounded(DEFAULT_NORMALIZED_METRIC_LIMIT)
            .metrics
    }

    pub fn normalized_scalars_bounded(&self, limit: usize) -> NormalizedMetrics {
        let mut out = MetricCollector::new(limit);
        push_value(
            &mut out,
            "heart-rate-avg",
            &self.heart_rate_avg,
            Unit::BeatsPerMinute,
            Aggregation::Mean,
            MeasurementContext::NightlyRecharge,
            self.date.as_deref(),
        );
        push_value(
            &mut out,
            "beat-to-beat-interval-avg",
            &self.beat_to_beat_avg,
            Unit::Milliseconds,
            Aggregation::Mean,
            MeasurementContext::NightlyRecharge,
            self.date.as_deref(),
        );
        push_value(
            &mut out,
            "heart-rate-variability-avg",
            &self.heart_rate_variability_avg,
            Unit::Milliseconds,
            Aggregation::Mean,
            MeasurementContext::NightlyRecharge,
            self.date.as_deref(),
        );
        push_value(
            &mut out,
            "breathing-rate-avg",
            &self.breathing_rate_avg,
            Unit::BreathsPerMinute,
            Aggregation::Mean,
            MeasurementContext::NightlyRecharge,
            self.date.as_deref(),
        );
        push_value(
            &mut out,
            "ans-charge",
            &self.ans_charge,
            Unit::Unknown("polar-ans-charge".into()),
            Aggregation::ProviderScore,
            MeasurementContext::NightlyRecharge,
            self.date.as_deref(),
        );
        push_timed_samples(
            &mut out,
            "hrv",
            self.hrv_samples.as_ref(),
            Unit::Milliseconds,
            MeasurementContext::NightlyRecharge,
            self.date.as_deref(),
            None,
        );
        push_timed_samples(
            &mut out,
            "breathing-rate",
            self.breathing_samples.as_ref(),
            Unit::BreathsPerMinute,
            MeasurementContext::NightlyRecharge,
            self.date.as_deref(),
            None,
        );
        out.finish()
    }
}

impl SleepWiseRecord {
    pub fn normalized_scalars(&self) -> Vec<RecoveryMetric> {
        self.normalized_scalars_bounded(DEFAULT_NORMALIZED_METRIC_LIMIT)
            .metrics
    }

    pub fn normalized_scalars_bounded(&self, limit: usize) -> NormalizedMetrics {
        let mut out = MetricCollector::new(limit);
        let source_date = self.date.as_deref().or_else(|| {
            self.period_start_time
                .as_deref()
                .and_then(|time| time.get(..10))
        });
        push_value(
            &mut out,
            "sleepwise-grade",
            &self.grade,
            Unit::Unknown("polar-sleepwise-grade".into()),
            Aggregation::ProviderScore,
            MeasurementContext::SleepWise,
            source_date,
        );
        push_value(
            &mut out,
            "sleepwise-grade-validity-duration",
            &self.grade_validity_seconds,
            Unit::Seconds,
            Aggregation::Sum,
            MeasurementContext::SleepWise,
            source_date,
        );
        if let Some(hourly_data) = &self.hourly_data {
            for sample in hourly_data {
                let timestamp = sample.time.as_deref().or(sample.start_time.as_deref());
                let Some(timestamp) = timestamp else {
                    continue;
                };
                let value = sample
                    .alertness
                    .as_ref()
                    .or(sample.alertness_level.as_ref());
                let Some(Value::Number(number)) = value else {
                    continue;
                };
                let Ok(value) = NumericValue::new(number.to_string()) else {
                    continue;
                };
                let offset_minutes = value_i32(self.sleep_timezone_offset_minutes.as_ref());
                out.push(RecoveryMetric {
                    metric: "sleepwise-alertness",
                    value,
                    unit: Unit::Unknown("polar-sleepwise-alertness".into()),
                    aggregation: Aggregation::ProviderScore,
                    context: MeasurementContext::SleepWise,
                    source_date: timestamp
                        .get(..10)
                        .map(str::to_owned)
                        .or_else(|| self.date.clone()),
                    sample_time: Some(timestamp.to_owned()),
                    resolved_time: resolve_provider_time(timestamp, offset_minutes),
                });
            }
        }
        out.finish()
    }

    /// Resolve hourly alertness intervals only when an explicit timezone
    /// offset accompanies the local-looking timestamps. Unknown labels remain
    /// on the provider record and are not converted to numeric values.
    pub fn resolved_hourly_windows(&self) -> Vec<ResolvedRecoveryWindow> {
        let offset_minutes = value_i32(self.sleep_timezone_offset_minutes.as_ref());
        let mut windows = Vec::new();
        for (kind, start_source, end_source) in [
            (
                "sleepwise-period",
                self.period_start_time.as_deref(),
                self.period_end_time.as_deref(),
            ),
            (
                "sleepwise-preferred-sleep",
                self.preferred_sleep_period_start_time.as_deref(),
                self.preferred_sleep_period_end_time.as_deref(),
            ),
            (
                "sleepwise-sleep-gate",
                self.sleep_gate_start_time.as_deref(),
                self.sleep_gate_end_time.as_deref(),
            ),
        ] {
            if let (Some(start), Some(end)) = (
                start_source.and_then(|source| resolve_provider_time(source, offset_minutes)),
                end_source.and_then(|source| resolve_provider_time(source, offset_minutes)),
            ) {
                if let (Some(start_micros), Some(end_micros)) =
                    (start.utc_micros(), end.utc_micros())
                    && end_micros < start_micros
                {
                    continue;
                }
                windows.push(ResolvedRecoveryWindow {
                    kind,
                    interval: TimeInterval::new(start, end),
                });
            }
        }
        windows.extend(
            self.hourly_data
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter_map(|sample| {
                    let start_source = sample.start_time.as_deref().or(sample.time.as_deref())?;
                    let start = resolve_provider_time(start_source, offset_minutes)?;
                    let end_source = sample.end_time.as_deref();
                    let end =
                        end_source.and_then(|source| resolve_provider_time(source, offset_minutes));
                    Some(ResolvedRecoveryWindow {
                        kind: "sleepwise-hourly-alertness",
                        interval: match end {
                            Some(end) => TimeInterval::new(start, end),
                            None => TimeInterval::point(start),
                        },
                    })
                }),
        );
        windows
    }
}

struct MetricCollector {
    metrics: Vec<RecoveryMetric>,
    limit: usize,
    truncated: bool,
}

impl MetricCollector {
    fn new(limit: usize) -> Self {
        Self {
            metrics: Vec::with_capacity(limit.min(256)),
            limit,
            truncated: false,
        }
    }

    fn push(&mut self, metric: RecoveryMetric) {
        if self.metrics.len() < self.limit {
            self.metrics.push(metric);
        } else {
            self.truncated = true;
        }
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.metrics.len())
    }

    fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn finish(self) -> NormalizedMetrics {
        NormalizedMetrics {
            metrics: self.metrics,
            truncated: self.truncated,
        }
    }
}

fn push_value(
    out: &mut MetricCollector,
    metric: &'static str,
    raw: &Option<Value>,
    unit: Unit,
    aggregation: Aggregation,
    context: MeasurementContext,
    source_date: Option<&str>,
) {
    let Some(Value::Number(number)) = raw else {
        return;
    };
    if let Ok(value) = NumericValue::new(number.to_string()) {
        out.push(RecoveryMetric {
            metric,
            value,
            unit,
            aggregation,
            context,
            source_date: source_date.map(str::to_owned),
            sample_time: None,
            resolved_time: None,
        });
    }
}

fn push_timed_samples(
    out: &mut MetricCollector,
    metric: &'static str,
    samples: Option<&Value>,
    unit: Unit,
    context: MeasurementContext,
    source_date: Option<&str>,
    offset_minutes: Option<i32>,
) {
    let Some(samples) = samples else {
        return;
    };
    let mut push_sample = |time: &str, value: &Value| {
        let Value::Number(number) = value else {
            return;
        };
        let Ok(value) = NumericValue::new(number.to_string()) else {
            return;
        };
        out.push(RecoveryMetric {
            metric,
            value,
            unit: unit.clone(),
            aggregation: Aggregation::Mean,
            context: context.clone(),
            source_date: source_date.map(str::to_owned),
            sample_time: Some(time.to_owned()),
            resolved_time: resolve_provider_time(time, offset_minutes),
        });
    };
    match samples {
        Value::Object(object) => {
            for (time, value) in object {
                push_sample(time, value);
            }
        }
        Value::Array(array) => {
            for sample in array {
                let Some(time) = sample
                    .get("time")
                    .or_else(|| sample.get("sample_time"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let Some(value) = sample.get("value") else {
                    continue;
                };
                push_sample(time, value);
            }
        }
        _ => {}
    }
}

/// Resolve an ISO local datetime with a provider-supplied offset. Bare clock
/// values cannot be resolved because they lack a calendar date; callers keep
/// those samples on the parsed provider record.
fn resolve_provider_time(source: &str, offset_minutes: Option<i32>) -> Option<EventTime> {
    let (datetime, resolution) = if let Ok(datetime) = OffsetDateTime::parse(source, &Rfc3339) {
        (
            datetime,
            if datetime.offset().whole_seconds() == 0 {
                TimeResolution::ProviderUtc
            } else {
                TimeResolution::ProviderOffset
            },
        )
    } else {
        let local = PrimitiveDateTime::parse(source, &Iso8601::DEFAULT).ok()?;
        let minutes = offset_minutes?;
        // Ignore nonsensical offsets rather than manufacturing a timestamp.
        if !(-14 * 60..=14 * 60).contains(&minutes) {
            return None;
        }
        let offset = UtcOffset::from_whole_seconds(minutes.checked_mul(60)?).ok()?;
        (local.assume_offset(offset), TimeResolution::ProviderOffset)
    };
    Some(EventTime::Utc {
        unix_micros: i64::try_from(datetime.unix_timestamp_nanos() / 1_000).ok()?,
        resolution,
        source_text: Some(source.to_owned()),
        local: Some(crate::model::LocalDateTime {
            year: datetime.year(),
            month: datetime.month() as u8,
            day: datetime.day(),
            hour: datetime.hour(),
            minute: datetime.minute(),
            second: datetime.second(),
            nanosecond: datetime.nanosecond(),
        }),
        offset_seconds: Some(datetime.offset().whole_seconds()),
    })
}

fn value_i32(value: Option<&Value>) -> Option<i32> {
    i32::try_from(value?.as_i64()?).ok()
}

/// Stream a top-level response array, materializing only its current record.
pub fn for_each_sleep<R, F>(reader: R, consume: F) -> Result<(), DecodeError>
where
    R: Read,
    F: FnMut(SleepRecord) -> Result<(), String>,
{
    for_each(reader, "sleep", Some("nights"), consume)
}

pub fn for_each_nightly_recharge<R, F>(reader: R, consume: F) -> Result<(), DecodeError>
where
    R: Read,
    F: FnMut(NightlyRechargeRecord) -> Result<(), String>,
{
    for_each(reader, "nightly-recharge", Some("recharges"), consume)
}

pub fn for_each_sleepwise<R, F>(reader: R, consume: F) -> Result<(), DecodeError>
where
    R: Read,
    F: FnMut(SleepWiseRecord) -> Result<(), String>,
{
    for_each(reader, "sleepwise", None, consume)
}

/// Circadian bedtime record from Polar's documented SleepWise endpoint.
///
/// The official example contains local-looking date-time strings without an
/// explicit offset alongside `sleep_timezone_offset_minutes`. Keep those
/// pieces separate: consumers can resolve time using the documented offset,
/// while this decoder never rewrites the provider's original values.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct CircadianBedtimeRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_sleep_period_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_sleep_period_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_gate_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_gate_end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_timezone_offset_minutes: Option<i32>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// Decode the top-level array returned by
/// `GET /v3/users/sleepwise/circadian-bedtime` or its `/date` variant.
pub fn for_each_circadian_bedtime<R, F>(reader: R, consume: F) -> Result<(), DecodeError>
where
    R: Read,
    F: FnMut(CircadianBedtimeRecord) -> Result<(), String>,
{
    for_each(reader, "sleepwise/circadian-bedtime", None, consume)
}

impl CircadianBedtimeRecord {
    /// Resolve the documented bedtime windows only when the response provides
    /// its timezone offset. Original provider strings remain unchanged in the
    /// record; these are derived UTC intervals for indexed consumers.
    pub fn resolved_windows(&self) -> Vec<ResolvedRecoveryWindow> {
        let offset_minutes = self.sleep_timezone_offset_minutes;
        let mut windows = Vec::with_capacity(4);
        for (kind, start_source, end_source) in [
            (
                "sleepwise-circadian-period",
                self.period_start_time.as_deref(),
                self.period_end_time.as_deref(),
            ),
            (
                "sleepwise-preferred-sleep",
                self.preferred_sleep_period_start_time.as_deref(),
                self.preferred_sleep_period_end_time.as_deref(),
            ),
            (
                "sleepwise-sleep-gate",
                self.sleep_gate_start_time.as_deref(),
                self.sleep_gate_end_time.as_deref(),
            ),
        ] {
            let (Some(start_source), Some(end_source)) = (start_source, end_source) else {
                continue;
            };
            let (Some(start), Some(end)) = (
                resolve_provider_time(start_source, offset_minutes),
                resolve_provider_time(end_source, offset_minutes),
            ) else {
                continue;
            };
            if let (Some(start_micros), Some(end_micros)) = (start.utc_micros(), end.utc_micros())
                && end_micros < start_micros
            {
                continue;
            }
            windows.push(ResolvedRecoveryWindow {
                kind,
                interval: TimeInterval::new(start, end),
            });
        }
        windows
    }
}

fn for_each<R, T, F>(
    reader: R,
    resource: &'static str,
    wrapper: Option<&'static str>,
    mut consume: F,
) -> Result<(), DecodeError>
where
    R: Read,
    T: for<'de> Deserialize<'de>,
    F: FnMut(T) -> Result<(), String>,
{
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    deserializer
        .deserialize_any(RootVisitor::<T, F> {
            consume: &mut consume,
            wrapper,
            marker: PhantomData,
        })
        .map_err(|error| DecodeError { resource, error })?;
    deserializer
        .end()
        .map_err(|error| DecodeError { resource, error })
}

struct RootVisitor<'a, T, F> {
    consume: &'a mut F,
    wrapper: Option<&'static str>,
    marker: PhantomData<T>,
}
impl<'de, T, F> de::Visitor<'de> for RootVisitor<'_, T, F>
where
    T: Deserialize<'de>,
    F: FnMut(T) -> Result<(), String>,
{
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array or documented wrapper of Polar recovery records")
    }
    fn visit_seq<A>(self, seq: A) -> Result<(), A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        read_records(seq, self.consume)
    }
    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let Some(wrapper) = self.wrapper else {
            return Err(de::Error::custom("expected an array of Polar records"));
        };
        let mut found = false;
        let mut direct_object = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if key == wrapper {
                if found {
                    return Err(de::Error::custom(format!("duplicate {wrapper} wrapper")));
                }
                found = true;
                map.next_value_seed(RecordsSeed::<T, F> {
                    consume: self.consume,
                    marker: PhantomData,
                })?;
            } else {
                let value = map.next_value::<Value>()?;
                if !found {
                    direct_object.insert(key, value);
                }
            }
        }
        if found {
            Ok(())
        } else {
            let record = T::deserialize(Value::Object(direct_object)).map_err(de::Error::custom)?;
            (self.consume)(record).map_err(de::Error::custom)
        }
    }
}

struct RecordsSeed<'a, T, F> {
    consume: &'a mut F,
    marker: PhantomData<T>,
}
impl<'de, T, F> DeserializeSeed<'de> for RecordsSeed<'_, T, F>
where
    T: Deserialize<'de>,
    F: FnMut(T) -> Result<(), String>,
{
    type Value = ();
    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RecordsVisitor::<T, F> {
            consume: self.consume,
            marker: PhantomData,
        })
    }
}
struct RecordsVisitor<'a, T, F> {
    consume: &'a mut F,
    marker: PhantomData<T>,
}
impl<'de, T, F> de::Visitor<'de> for RecordsVisitor<'_, T, F>
where
    T: Deserialize<'de>,
    F: FnMut(T) -> Result<(), String>,
{
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array of Polar recovery records")
    }
    fn visit_seq<A>(self, seq: A) -> Result<(), A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        read_records(seq, self.consume)
    }
}
fn read_records<'de, A, T, F>(mut seq: A, consume: &mut F) -> Result<(), A::Error>
where
    A: de::SeqAccess<'de>,
    T: Deserialize<'de>,
    F: FnMut(T) -> Result<(), String>,
{
    while let Some(record) = seq.next_element::<T>()? {
        consume(record).map_err(de::Error::custom)?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct DecodeError {
    resource: &'static str,
    error: serde_json::Error,
}
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid Polar {} response: {}",
            self.resource, self.error
        )
    }
}
impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::path::PathBuf;

    fn fixture(name: &str) -> File {
        File::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/polar/{name}")),
        )
        .unwrap()
    }

    #[test]
    fn decodes_sleep_keeps_future_stage_and_does_not_fabricate_utc() {
        let mut records = Vec::new();
        for_each_sleep(fixture("recovery_sleep.json"), |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 2);
        let sleep = &records[0];
        assert_eq!(sleep.date.as_deref(), Some("2026-10-20"));
        assert_eq!(
            sleep.sleep_start_time.as_deref(),
            Some("2026-10-19T23:41:00")
        );
        assert_eq!(
            sleep.hypnogram.as_ref().unwrap()["2026-10-20T06:58:00"],
            "FUTURE_STAGE"
        );
        assert_eq!(sleep.provider_fields["future_sleep_status"], "new-code");
        assert_eq!(
            sleep
                .normalized_scalars()
                .iter()
                .find(|m| m.metric == "sleep-score")
                .unwrap()
                .value
                .as_str(),
            "84"
        );
        assert!(records[1].normalized_scalars().is_empty());
    }

    #[test]
    fn decodes_date_specific_sleep_object() {
        let input = br#"{"polar_user":"user-1","date":"2026-10-21","sleep_score":84}"#;
        let mut records = Vec::new();
        for_each_sleep(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].date.as_deref(), Some("2026-10-21"));
        assert_eq!(records[0].sleep_score.as_ref().unwrap(), 84);
        assert_eq!(records[0].provider_fields["polar_user"], "user-1");
    }

    #[test]
    fn decodes_nightly_recharge_units_and_unknown_sample_type() {
        let mut records = Vec::new();
        for_each_nightly_recharge(fixture("recovery_nightly_recharge.json"), |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 2);
        let r = &records[0];
        assert_eq!(r.ans_charge_status.as_ref().unwrap(), "future-status");
        assert_eq!(r.hrv_samples.as_ref().unwrap()["02:05"], 52.25);
        let metrics = r.normalized_scalars();
        assert!(metrics.iter().any(|m| m.metric == "hrv"
            && m.value.as_str() == "52.25"
            && m.unit == Unit::Milliseconds));
        assert!(
            metrics
                .iter()
                .any(|m| m.metric == "breathing-rate" && m.unit == Unit::BreathsPerMinute)
        );
    }

    #[test]
    fn decodes_date_specific_nightly_recharge_object() {
        let input =
            br#"{"polar_user":"user-1","date":"2026-10-21","heart_rate_avg":58,"ans_charge":72}"#;
        let mut records = Vec::new();
        for_each_nightly_recharge(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].date.as_deref(), Some("2026-10-21"));
        assert_eq!(records[0].heart_rate_avg.as_ref().unwrap(), 58);
        assert_eq!(records[0].provider_fields["polar_user"], "user-1");
    }

    #[test]
    fn decodes_sleepwise_classification_and_unknown_hourly_code() {
        let mut records = Vec::new();
        for_each_sleepwise(fixture("recovery_sleepwise.json"), |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 2);
        let r = &records[0];
        assert_eq!(r.grade_classification.as_ref().unwrap(), "FUTURE_GRADE");
        assert_eq!(
            r.hourly_data.as_ref().unwrap()[2].provider_fields["classification"],
            "future-class"
        );
        let metrics = r.normalized_scalars();
        assert_eq!(metrics.len(), 4);
        assert_eq!(
            metrics[0].unit,
            Unit::Unknown("polar-sleepwise-grade".into())
        );
        let alertness: Vec<_> = metrics
            .iter()
            .filter(|metric| metric.metric == "sleepwise-alertness")
            .collect();
        assert_eq!(alertness.len(), 3);
        assert_eq!(alertness[0].value.as_str(), "68");
        assert_eq!(
            alertness[0].sample_time.as_deref(),
            Some("2026-10-20T09:00:00")
        );
        assert!(alertness[0].resolved_time.is_none());
    }

    #[test]
    fn sleepwise_explicit_offset_resolves_hourly_measurements_and_windows() {
        let input = br#"[{
          "date":"2026-10-20",
          "sleep_timezone_offset_minutes":120,
          "period_start_time":"2026-10-20T20:00:00",
          "period_end_time":"2026-10-21T05:00:00",
          "preferred_sleep_period_start_time":"2026-10-20T22:00:00",
          "preferred_sleep_period_end_time":"2026-10-21T06:00:00",
          "sleep_gate_start_time":"2026-10-20T21:45:00",
          "sleep_gate_end_time":"2026-10-20T22:15:00",
          "hourly_data":[{
            "time":"2026-10-21T04:12:00",
            "alertness":68,
            "validity":"VALIDITY_ESTIMATE"
          }]
        }]"#;
        let mut records = Vec::new();
        for_each_sleepwise(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        let record = &records[0];
        let sample = record
            .normalized_scalars()
            .into_iter()
            .find(|metric| metric.metric == "sleepwise-alertness")
            .unwrap();
        let time = sample.resolved_time.unwrap();
        let expected = OffsetDateTime::parse("2026-10-21T02:12:00Z", &Rfc3339)
            .unwrap()
            .unix_timestamp_nanos()
            / 1_000;
        assert_eq!(time.utc_micros(), Some(i64::try_from(expected).unwrap()));
        assert_eq!(sample.sample_time.as_deref(), Some("2026-10-21T04:12:00"));
        let windows = record.resolved_hourly_windows();
        assert_eq!(windows.len(), 4);
        assert_eq!(windows[0].kind, "sleepwise-period");
        let expected_start = OffsetDateTime::parse("2026-10-20T18:00:00Z", &Rfc3339)
            .unwrap()
            .unix_timestamp_nanos()
            / 1_000;
        let expected_end = OffsetDateTime::parse("2026-10-21T03:00:00Z", &Rfc3339)
            .unwrap()
            .unix_timestamp_nanos()
            / 1_000;
        assert_eq!(
            windows[0].interval.start.utc_micros(),
            Some(i64::try_from(expected_start).unwrap())
        );
        assert_eq!(
            windows[0].interval.end.as_ref().unwrap().utc_micros(),
            Some(i64::try_from(expected_end).unwrap())
        );
        assert!(
            windows
                .iter()
                .all(|window| window.interval.is_utc_resolved())
        );
    }

    #[test]
    fn sleep_normalization_uses_documented_units_and_keeps_source_times() {
        // Independent body based on the documented Sleep response shape.
        let input = br#"{"nights":[{
          "date":"2026-10-20",
          "sleep_start_time":"2026-10-19T23:41:00+02:00",
          "sleep_end_time":"2026-10-20T07:13:00+02:00",
          "sleep_score":0,"sleep_charge":null,"sleep_goal":28800,
          "total_sleep_time":26400,
          "light_sleep":13800,"deep_sleep":7200,"rem_sleep":5400,
          "unrecognized_sleep_stage":300,"total_interruption_duration":1200,
          "short_interruption_duration":600,"long_interruption_duration":600,
          "sleep_cycles":4,"group_duration_score":88,"group_solidity_score":76,
          "group_regeneration_score":54.2,"continuity":2.1,"continuity_class":2,
          "hypnogram":{"00:39":2,"00:50":3,"01:23":"FUTURE_STAGE"},
          "heart_rate_samples":{"00:41":0,"00:46":77}
        }]}"#;
        let mut records = Vec::new();
        for_each_sleep(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        let record = &records[0];
        assert_eq!(record.hypnogram.as_ref().unwrap()["01:23"], "FUTURE_STAGE");
        let phases = record.hypnogram_bounded(2);
        assert_eq!(phases.samples.len(), 2);
        assert!(phases.truncated);
        assert_eq!(phases.samples[0].sample_time, "00:39");
        assert_eq!(phases.samples[0].source_date.as_deref(), Some("2026-10-20"));
        assert_eq!(phases.samples[1].stage_code, 3);
        let metrics = record.normalized_scalars_bounded(32);
        assert!(!metrics.truncated);
        let zero_score = metrics
            .metrics
            .iter()
            .find(|m| m.metric == "sleep-score")
            .unwrap();
        assert_eq!(zero_score.value.as_str(), "0");
        assert_eq!(zero_score.source_date.as_deref(), Some("2026-10-20"));
        assert!(!metrics.metrics.iter().any(|m| m.metric == "sleep-charge"));
        let deep = metrics
            .metrics
            .iter()
            .find(|m| m.metric == "deep-sleep")
            .unwrap();
        assert_eq!(deep.value.as_str(), "7200");
        assert_eq!(deep.unit, Unit::Seconds);
        let total_sleep = metrics
            .metrics
            .iter()
            .find(|metric| metric.metric == "total-sleep-time")
            .unwrap();
        assert_eq!(total_sleep.value.as_str(), "26400");
        assert_eq!(total_sleep.unit, Unit::Seconds);
        assert_eq!(total_sleep.aggregation, Aggregation::Sum);
        let hr = metrics
            .metrics
            .iter()
            .find(|m| m.metric == "heart-rate" && m.sample_time.as_deref() == Some("00:41"))
            .unwrap();
        assert_eq!(hr.value.as_str(), "0");
        assert_eq!(hr.unit, Unit::BeatsPerMinute);
        assert_eq!(hr.sample_time.as_deref(), Some("00:41"));
        assert_eq!(hr.source_date.as_deref(), Some("2026-10-20"));
        // Explicit offset remains byte-for-byte source text.
        assert_eq!(
            record.sleep_start_time.as_deref(),
            Some("2026-10-19T23:41:00+02:00")
        );
    }

    #[test]
    fn absent_and_null_metrics_are_omitted_but_zero_is_retained() {
        let input = br#"[
          {"date":"2026-10-20"},
          {"date":"2026-10-21","sleep_score":null},
          {"date":"2026-10-22","sleep_score":0}
        ]"#;
        let mut records = Vec::new();
        for_each_sleep(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        assert!(records[0].normalized_scalars().is_empty());
        assert!(records[1].normalized_scalars().is_empty());
        let metrics = records[2].normalized_scalars();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].value.as_str(), "0");
    }

    #[test]
    fn nightly_recharge_normalizes_means_and_reports_limit_truncation() {
        // Independent body based on the official `recharges` list example.
        let input = br#"{"recharges":[{
          "date":"2026-10-20","heart_rate_avg":0,"beat_to_beat_avg":816,
          "heart_rate_variability_avg":28,"breathing_rate_avg":14.1,
          "nightly_recharge_status":3,"ans_charge":0,"ans_charge_status":3,
          "hrv_samples":{"00:41":14,"00:46":14,"00:51":15},
          "breathing_samples":{"00:39":13.4,"00:44":13.5},
          "future_sample_code":"vNext"
        }]}"#;
        let mut records = Vec::new();
        for_each_nightly_recharge(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        let record = &records[0];
        assert_eq!(record.nightly_recharge_status.as_ref().unwrap(), 3);
        assert_eq!(record.provider_fields["future_sample_code"], "vNext");
        let full = record.normalized_scalars_bounded(32);
        assert!(!full.truncated);
        let hrv = full
            .metrics
            .iter()
            .find(|m| m.metric == "hrv" && m.sample_time.as_deref() == Some("00:41"))
            .unwrap();
        assert_eq!(hrv.unit, Unit::Milliseconds);
        assert_eq!(hrv.aggregation, Aggregation::Mean);
        assert_eq!(hrv.source_date.as_deref(), Some("2026-10-20"));
        let beat_to_beat = full
            .metrics
            .iter()
            .find(|m| m.metric == "beat-to-beat-interval-avg")
            .unwrap();
        assert_eq!(beat_to_beat.value.as_str(), "816");
        assert_eq!(beat_to_beat.unit, Unit::Milliseconds);
        let zero_ans = full
            .metrics
            .iter()
            .find(|m| m.metric == "ans-charge")
            .unwrap();
        assert_eq!(zero_ans.value.as_str(), "0");
        assert_eq!(zero_ans.unit, Unit::Unknown("polar-ans-charge".into()));
        let bounded = record.normalized_scalars_bounded(2);
        assert_eq!(bounded.metrics.len(), 2);
        assert!(bounded.truncated);
    }

    #[test]
    fn sleep_indexes_only_numeric_hypnogram_codes_with_explicit_offsets() {
        let input = br#"[{"date":"2026-10-20","hypnogram":{
          "2026-10-20T01:02:00+02:00":2,
          "2026-10-20T01:03:00":"DEEP",
          "01:04":3
        }}]"#;
        let mut records = Vec::new();
        for_each_sleep(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();

        let metrics = records[0].normalized_scalars_bounded(8);
        let stage = metrics
            .metrics
            .iter()
            .find(|metric| {
                metric.metric == "sleep-stage-code"
                    && metric.sample_time.as_deref() == Some("2026-10-20T01:02:00+02:00")
            })
            .expect("numeric stage code should be queryable");
        assert_eq!(stage.value.as_str(), "2");
        assert_eq!(stage.unit, Unit::Unknown("polar-sleep-stage-code".into()));
        assert_eq!(
            stage.aggregation,
            Aggregation::Unknown("categorical-code".into())
        );
        assert_eq!(stage.context, MeasurementContext::Sleep);
        assert_eq!(
            stage.sample_time.as_deref(),
            Some("2026-10-20T01:02:00+02:00")
        );
        assert_eq!(stage.source_date.as_deref(), Some("2026-10-20"));
        assert_eq!(
            stage.resolved_time.as_ref().unwrap().utc_micros(),
            Some(1_792_450_920_000_000)
        );
        // Numeric values with no offset and string stage labels stay in the
        // provider record without an invented code or UTC timestamp.
        assert_eq!(
            metrics
                .metrics
                .iter()
                .filter(|metric| metric.metric == "sleep-stage-code")
                .count(),
            2
        );
        assert_eq!(
            records[0].hypnogram.as_ref().unwrap()["2026-10-20T01:03:00"],
            "DEEP"
        );
    }

    #[test]
    fn nightly_recharge_resolves_samples_only_when_the_sample_has_an_offset() {
        let input = br#"[{"date":"2026-10-20","hrv_samples":[
          {"time":"2026-10-20T02:00:00+02:00","value":51.75},
          {"time":"02:05","value":52.25}
        ],"breathing_samples":{"02:00":13.8}}]"#;
        let mut records = Vec::new();
        for_each_nightly_recharge(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();

        let metrics = records[0].normalized_scalars_bounded(8);
        let resolved = metrics
            .metrics
            .iter()
            .find(|metric| metric.metric == "hrv" && metric.resolved_time.is_some())
            .unwrap();
        assert_eq!(resolved.value.as_str(), "51.75");
        assert_eq!(resolved.unit, Unit::Milliseconds);
        assert_eq!(resolved.aggregation, Aggregation::Mean);
        assert_eq!(resolved.context, MeasurementContext::NightlyRecharge);
        assert_eq!(
            resolved.resolved_time.as_ref().unwrap().utc_micros(),
            Some(1_792_454_400_000_000)
        );
        let unresolved = metrics
            .metrics
            .iter()
            .find(|metric| metric.metric == "hrv" && metric.resolved_time.is_none())
            .unwrap();
        assert_eq!(unresolved.sample_time.as_deref(), Some("02:05"));
        assert_eq!(unresolved.source_date.as_deref(), Some("2026-10-20"));
    }

    #[test]
    fn sleepwise_scores_and_hourly_classifications_keep_provider_scale_and_time() {
        // Official alertness fields: grade is provider-scored; hourly data is
        // categorical and must not be interpreted as numeric alertness.
        let input = br#"[{
          "grade":9.9,"grade_validity_seconds":57600,
          "grade_type":"GRADE_TYPE_PRIMARY",
          "grade_classification":"GRADE_CLASSIFICATION_STRONG",
          "validity":"VALIDITY_ESTIMATE","sleep_inertia":"SLEEP_INERTIA_MILD",
          "sleep_type":"SLEEP_TYPE_PRIMARY","result_type":"ALERTNESS_TYPE_HISTORY",
          "period_start_time":"2026-10-20T20:52:20",
          "period_end_time":"2026-10-21T19:15:52",
          "sleep_period_start_time":"2026-10-20T20:52:20.57",
          "sleep_period_end_time":"2026-10-21T04:11:20.57",
          "sleep_timezone_offset_minutes":120,
          "hourly_data":[{"validity":"VALIDITY_ESTIMATE",
            "alertness_level":"ALERTNESS_LEVEL_LOW",
            "start_time":"2026-10-21T04:12:00","end_time":"2026-10-21T05:00:00",
            "future_code":"FUTURE_LEVEL"}]
        }]"#;
        let mut records = Vec::new();
        for_each_sleepwise(&input[..], |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        let record = &records[0];
        let metrics = record.normalized_scalars_bounded(8);
        assert!(!metrics.truncated);
        let grade = metrics
            .metrics
            .iter()
            .find(|m| m.metric == "sleepwise-grade")
            .unwrap();
        assert_eq!(grade.value.as_str(), "9.9");
        assert_eq!(grade.unit, Unit::Unknown("polar-sleepwise-grade".into()));
        assert_eq!(grade.source_date.as_deref(), Some("2026-10-20"));
        let duration = metrics
            .metrics
            .iter()
            .find(|m| m.metric == "sleepwise-grade-validity-duration")
            .unwrap();
        assert_eq!(duration.value.as_str(), "57600");
        assert_eq!(duration.unit, Unit::Seconds);
        let hourly = &record.hourly_data.as_ref().unwrap()[0];
        assert_eq!(
            hourly.alertness_level.as_ref().unwrap(),
            "ALERTNESS_LEVEL_LOW"
        );
        assert_eq!(hourly.start_time.as_deref(), Some("2026-10-21T04:12:00"));
        assert_eq!(hourly.provider_fields["future_code"], "FUTURE_LEVEL");
    }

    #[test]
    fn decodes_official_circadian_bedtime_example_and_preserves_future_fields() {
        let mut records = Vec::new();
        for_each_circadian_bedtime(fixture("circadian_bedtime.json"), |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.validity.as_deref(), Some("VALIDITY_ESTIMATE"));
        assert_eq!(
            record.quality.as_deref(),
            Some("CIRCADIAN_BEDTIME_QUALITY_COMPROMISED")
        );
        assert_eq!(
            record.result_type.as_deref(),
            Some("CIRCADIAN_BEDTIME_TYPE_HISTORY")
        );
        assert_eq!(
            record.preferred_sleep_period_start_time.as_deref(),
            Some("2022-11-20T21:18:30")
        );
        assert_eq!(
            record.sleep_gate_end_time.as_deref(),
            Some("2022-11-20T21:33:30")
        );
        assert_eq!(record.sleep_timezone_offset_minutes, Some(120));
        assert!(
            record
                .period_start_time
                .as_deref()
                .unwrap()
                .ends_with("21:18:30")
        );
        // Preserve the exact provider value; the offset remains a separate field.
        assert!(!record.period_start_time.as_deref().unwrap().contains('+'));
        let windows = record.resolved_windows();
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].kind, "sleepwise-circadian-period");
        let expected_start = OffsetDateTime::parse("2022-11-20T19:18:30Z", &Rfc3339)
            .unwrap()
            .unix_timestamp_nanos()
            / 1_000;
        assert_eq!(
            windows[0].interval.start.utc_micros(),
            Some(i64::try_from(expected_start).unwrap())
        );
        assert!(
            windows
                .iter()
                .all(|window| window.interval.is_utc_resolved())
        );

        let future = br#"[{"validity":"VALIDITY_FUTURE","future_window_code":"NEW","future_value":{"x":1}}]"#;
        let mut record = None;
        for_each_circadian_bedtime(&future[..], |item| {
            record = Some(item);
            Ok(())
        })
        .unwrap();
        let record = record.unwrap();
        assert_eq!(record.validity.as_deref(), Some("VALIDITY_FUTURE"));
        assert_eq!(record.provider_fields["future_window_code"], "NEW");
        assert_eq!(record.provider_fields["future_value"]["x"], 1);
    }

    #[test]
    fn circadian_bedtime_decoder_streams_records_before_later_error() {
        let mut seen = 0;
        let input = br#"[{"result_type":"CIRCADIAN_BEDTIME_TYPE_HISTORY"},false]"#;
        assert!(
            for_each_circadian_bedtime(&input[..], |_| {
                seen += 1;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(seen, 1);
    }

    #[test]
    fn streams_before_later_invalid_item() {
        let mut seen = 0;
        let result = for_each_sleep(&br#"[{"date":"2026-10-21"},5]"#[..], |_| {
            seen += 1;
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(seen, 1);
    }
}
