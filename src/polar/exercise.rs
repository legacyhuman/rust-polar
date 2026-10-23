//! Streaming decoders and provider-specific types for Polar exercises.
//!
//! The AccessLink response is kept as an archive separately. These types make
//! commonly used summary, sample, zone, and route fields convenient while
//! retaining all other fields for forward compatibility. Enum-like values are
//! strings rather than closed Rust enums: Polar can add values without making
//! an otherwise useful exercise undecodable.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;

use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use time::format_description::well_known::{Iso8601, Rfc3339};
use time::{Duration, OffsetDateTime, PrimitiveDateTime, UtcOffset};

use crate::model::{
    Aggregation, EventTime, LocalDateTime, Measurement, MeasurementContext, MetricId, NumericValue,
    Provenance, StableId, TimeInterval, TimeResolution, Unit,
};

/// Default bound for provider-derived scalar and sample rows from one exercise.
pub const DEFAULT_NORMALIZED_METRIC_LIMIT: usize = 4096;

/// One Polar exercise summary, optionally including requested detail sections.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Exercise {
    /// Provider IDs may be integer or string values; preserve the returned
    /// scalar without imposing an undocumented format.
    pub id: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub polar_user: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time_utc_offset: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calories: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heart_rate: Option<HeartRateSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub training_load: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sport: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detailed_sport_info: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_route: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_rate: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<ExerciseSampleSeries>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heart_rate_zones: Option<Vec<ExerciseZone>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<Vec<RoutePoint>>,
    /// Provider keys not yet modelled by this version.
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct HeartRateSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub average: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<Value>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// A series of samples as returned for a requested sample type.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExerciseSampleSeries {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_rate: Option<Value>,
    /// Kept open as a string so future Polar sample types remain readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_type: Option<String>,
    /// AccessLink currently documents comma-separated sample text here; keep
    /// an open JSON type so provider schema changes and alternate encodings
    /// survive without losing the archived original.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExerciseSample {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExerciseZone {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower_limit: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_limit: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_zone: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct RoutePoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latitude: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub longitude: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

impl Exercise {
    /// Serialize this decoded exercise, including unknown provider fields.
    /// The original response archive remains authoritative for byte fidelity.
    pub fn provider_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// A stable ID derived from the exact provider exercise identifier.
    pub fn stable_id(&self) -> StableId {
        StableId::from_parts(["polar", "exercises", &self.id.to_string()])
    }

    /// Normalize documented summary values, supported sample types, and
    /// heart-rate zone durations. Unrecognized sample types stay in the raw
    /// provider document. Samples encoded as comma-separated values have no
    /// per-sample timestamps here, so their normalized metrics carry no time;
    /// no timestamp is synthesized from recording-rate.
    pub fn normalized_metrics(&self) -> NormalizedExerciseMetrics {
        self.normalized_metrics_bounded(DEFAULT_NORMALIZED_METRIC_LIMIT)
    }

    pub fn normalized_metrics_bounded(&self, limit: usize) -> NormalizedExerciseMetrics {
        let parent_id = self.stable_id();
        let time = self.exercise_interval();
        let source_time = self.start_time.clone();
        let mut collector = ExerciseMetricCollector::new(limit);

        push_number(
            &mut collector,
            &parent_id,
            "calories",
            &self.calories,
            Unit::Kilocalories,
            Aggregation::Sum,
            time.clone(),
            source_time.clone(),
            None,
        );
        push_number(
            &mut collector,
            &parent_id,
            "distance",
            &self.distance,
            Unit::Meters,
            Aggregation::Sum,
            time.clone(),
            source_time.clone(),
            None,
        );
        if let Some(heart_rate) = &self.heart_rate {
            push_number(
                &mut collector,
                &parent_id,
                "heart-rate-average",
                &heart_rate.average,
                Unit::BeatsPerMinute,
                Aggregation::Mean,
                time.clone(),
                source_time.clone(),
                None,
            );
            push_number(
                &mut collector,
                &parent_id,
                "heart-rate-maximum",
                &heart_rate.maximum,
                Unit::BeatsPerMinute,
                Aggregation::Maximum,
                time.clone(),
                source_time.clone(),
                None,
            );
        }
        push_number(
            &mut collector,
            &parent_id,
            "training-load",
            &self.training_load,
            Unit::Unknown("polar-training-load".into()),
            Aggregation::ProviderScore,
            time.clone(),
            source_time.clone(),
            None,
        );

        for (provider_key, metric) in [
            ("fat-percentage", "fat-percentage"),
            ("carbohydrate-percentage", "carbohydrate-percentage"),
            ("protein-percentage", "protein-percentage"),
        ] {
            if let Some(value) = self.provider_fields.get(provider_key) {
                push_value(
                    &mut collector,
                    &parent_id,
                    metric,
                    value,
                    Unit::Percent,
                    Aggregation::ProviderScore,
                    time.clone(),
                    source_time.clone(),
                    None,
                );
            }
        }
        if let Some(value) = self.provider_fields.get("running-index") {
            push_value(
                &mut collector,
                &parent_id,
                "running-index",
                value,
                Unit::Unknown("polar-running-index".into()),
                Aggregation::ProviderScore,
                time.clone(),
                source_time.clone(),
                None,
            );
        }

        if let Some(zones) = &self.heart_rate_zones {
            for (position, zone) in zones.iter().enumerate() {
                let index = zone
                    .index
                    .as_ref()
                    .and_then(json_scalar_text)
                    .unwrap_or_else(|| position.to_string());
                let Some(seconds) = zone.in_zone.as_deref().and_then(duration_seconds) else {
                    continue;
                };
                collector.push(ExerciseMetric {
                    id: StableId::from_parts([
                        parent_id.as_str(),
                        "heart-rate-zone-duration",
                        &index,
                    ]),
                    parent_id: parent_id.clone(),
                    metric: MetricId::new(format!("heart-rate-zone-{index}-duration")),
                    value: seconds,
                    unit: Unit::Seconds,
                    aggregation: Aggregation::Sum,
                    context: MeasurementContext::Exercise,
                    time: time.clone(),
                    source_time: source_time.clone(),
                    sample_type: None,
                    sample_index: None,
                });
            }
        }

        if let Some(series) = &self.samples {
            for (series_index, sample_series) in series.iter().enumerate() {
                let Some(sample_type) = sample_series.sample_type.as_deref() else {
                    continue;
                };
                let Some((metric, unit)) = known_sample_type(sample_type) else {
                    continue;
                };
                let Some(data) = &sample_series.data else {
                    continue;
                };
                match data {
                    Value::String(csv) => {
                        for (sample_index, token) in csv.split(',').enumerate() {
                            let token = token.trim();
                            if token.is_empty() {
                                continue;
                            }
                            let Ok(value) = NumericValue::new(token.to_owned()) else {
                                continue;
                            };
                            collector.push(sample_metric(
                                &parent_id,
                                metric,
                                value,
                                unit.clone(),
                                series_index,
                                sample_index,
                                sample_type,
                            ));
                        }
                    }
                    Value::Array(samples) => {
                        for (sample_index, sample) in samples.iter().enumerate() {
                            let Some(value) = sample.get("value") else {
                                continue;
                            };
                            let Some(value) = json_numeric(value) else {
                                continue;
                            };
                            let sample_time = sample
                                .get("date-time")
                                .or_else(|| sample.get("date_time"))
                                .and_then(Value::as_str);
                            let mut normalized = sample_metric(
                                &parent_id,
                                metric,
                                value,
                                unit.clone(),
                                series_index,
                                sample_index,
                                sample_type,
                            );
                            if let Some(sample_time) = sample_time {
                                normalized.source_time = Some(sample_time.to_owned());
                                normalized.time =
                                    parse_event_time(sample_time, None).map(TimeInterval::point);
                            }
                            collector.push(normalized);
                        }
                    }
                    _ => {}
                }
            }
        }

        NormalizedExerciseMetrics {
            metrics: collector.metrics,
            truncated: collector.truncated,
        }
    }

    /// Preserve an exact provider timestamp and resolve only when it carries
    /// an offset itself or the exercise explicitly supplies its UTC offset.
    pub fn event_time(&self) -> Option<EventTime> {
        let source = self.start_time.as_deref()?;
        parse_event_time(source, self.start_time_utc_offset.as_ref())
    }

    fn exercise_interval(&self) -> Option<TimeInterval> {
        let start = self.event_time()?;
        let end = self
            .duration
            .as_deref()
            .and_then(parse_iso_duration)
            .and_then(|duration| advance_event_time(&start, duration));
        Some(TimeInterval { start, end })
    }
}

/// Source-neutral metric payload tied to its parent exercise. Metrics whose
/// sample has no source timestamp remain records and cannot enter UTC series.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExerciseMetric {
    pub id: StableId,
    pub parent_id: StableId,
    pub metric: MetricId,
    pub value: NumericValue,
    pub unit: Unit,
    pub aggregation: Aggregation,
    pub context: MeasurementContext,
    pub time: Option<TimeInterval>,
    pub source_time: Option<String>,
    pub sample_type: Option<String>,
    pub sample_index: Option<u64>,
}

impl ExerciseMetric {
    /// Convert to the common storage model while attaching fetch/archive
    /// provenance. A metric without any event time remains provider data only.
    pub fn into_measurement(self, provenance: Provenance) -> Option<Measurement> {
        let time = self.time?;
        Some(Measurement {
            schema_version: 1,
            id: self.id,
            provenance,
            metric: self.metric,
            value: self.value,
            unit: self.unit,
            time,
            aggregation: self.aggregation,
            context: self.context,
            parent_id: Some(self.parent_id),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedExerciseMetrics {
    pub metrics: Vec<ExerciseMetric>,
    pub truncated: bool,
}

struct ExerciseMetricCollector {
    maximum: usize,
    metrics: Vec<ExerciseMetric>,
    truncated: bool,
}

impl ExerciseMetricCollector {
    fn new(maximum: usize) -> Self {
        Self {
            maximum,
            metrics: Vec::with_capacity(maximum.min(256)),
            truncated: false,
        }
    }

    fn push(&mut self, metric: ExerciseMetric) {
        if self.metrics.len() == self.maximum {
            self.truncated = true;
        } else {
            self.metrics.push(metric);
        }
    }
}

#[allow(clippy::too_many_arguments)] // Keep the small scalar-normalization call sites explicit.
fn push_number(
    out: &mut ExerciseMetricCollector,
    parent_id: &StableId,
    name: &str,
    value: &Option<Value>,
    unit: Unit,
    aggregation: Aggregation,
    time: Option<TimeInterval>,
    source_time: Option<String>,
    ordinal: Option<u64>,
) {
    let Some(value) = value.as_ref().and_then(json_numeric) else {
        return;
    };
    push_metric(
        out,
        parent_id,
        name,
        value,
        unit,
        aggregation,
        time,
        source_time,
        ordinal,
    );
}

#[allow(clippy::too_many_arguments)] // Mirrors push_number for provider JSON values.
fn push_value(
    out: &mut ExerciseMetricCollector,
    parent_id: &StableId,
    name: &str,
    value: &Value,
    unit: Unit,
    aggregation: Aggregation,
    time: Option<TimeInterval>,
    source_time: Option<String>,
    ordinal: Option<u64>,
) {
    let Some(value) = json_numeric(value) else {
        return;
    };
    push_metric(
        out,
        parent_id,
        name,
        value,
        unit,
        aggregation,
        time,
        source_time,
        ordinal,
    );
}

#[allow(clippy::too_many_arguments)] // Explicit fields avoid allocating a temporary descriptor per metric.
fn push_metric(
    out: &mut ExerciseMetricCollector,
    parent_id: &StableId,
    name: &str,
    value: NumericValue,
    unit: Unit,
    aggregation: Aggregation,
    time: Option<TimeInterval>,
    source_time: Option<String>,
    ordinal: Option<u64>,
) {
    let suffix = ordinal.map_or_else(|| name.to_owned(), |index| format!("{name}-{index}"));
    out.push(ExerciseMetric {
        id: StableId::from_parts([parent_id.as_str(), &suffix]),
        parent_id: parent_id.clone(),
        metric: MetricId::new(name),
        value,
        unit,
        aggregation,
        context: MeasurementContext::Exercise,
        time,
        source_time,
        sample_type: None,
        sample_index: None,
    });
}

fn sample_metric(
    parent_id: &StableId,
    metric: &str,
    value: NumericValue,
    unit: Unit,
    series_index: usize,
    sample_index: usize,
    sample_type: &str,
) -> ExerciseMetric {
    ExerciseMetric {
        id: StableId::from_parts([
            parent_id.as_str(),
            metric,
            &series_index.to_string(),
            &sample_index.to_string(),
        ]),
        parent_id: parent_id.clone(),
        metric: MetricId::new(metric),
        value,
        unit,
        aggregation: Aggregation::Instant,
        context: MeasurementContext::Exercise,
        time: None,
        source_time: None,
        sample_type: Some(sample_type.to_owned()),
        sample_index: Some(sample_index as u64),
    }
}

fn known_sample_type(sample_type: &str) -> Option<(&'static str, Unit)> {
    match sample_type {
        "HEART_RATE" => Some(("heart-rate", Unit::BeatsPerMinute)),
        _ => None,
    }
}

fn json_numeric(value: &Value) -> Option<NumericValue> {
    let Value::Number(number) = value else {
        return None;
    };
    NumericValue::new(number.to_string()).ok()
}

fn json_scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(crate) fn duration_seconds(source: &str) -> Option<NumericValue> {
    let duration = parse_iso_duration(source)?;
    let nanos = duration.whole_nanoseconds();
    let negative = nanos < 0;
    let absolute = nanos.unsigned_abs();
    let whole = absolute / 1_000_000_000;
    let fractional = absolute % 1_000_000_000;
    let mut lexeme = if negative {
        format!("-{whole}")
    } else {
        whole.to_string()
    };
    if fractional != 0 {
        let fraction = format!("{fractional:09}");
        lexeme.push('.');
        lexeme.push_str(fraction.trim_end_matches('0'));
    }
    NumericValue::new(lexeme).ok()
}

/// Parse the fixed-length day/hour/minute/second subset used by AccessLink
/// durations. Calendar months and years are deliberately unsupported because
/// their elapsed length is not fixed.
fn parse_iso_duration(source: &str) -> Option<Duration> {
    let source = source.strip_prefix('P')?;
    let (days, time) = match source.split_once('T') {
        Some((date, time)) => (date, Some(time)),
        None => (source, None),
    };
    let mut total_nanos =
        i128::from(parse_duration_component(days, 'D', false)?).checked_mul(86_400_000_000_000)?;
    if let Some(time) = time {
        let mut rest = time;
        for (designator, multiplier) in [
            ('H', 3_600_000_000_000i128),
            ('M', 60_000_000_000i128),
            ('S', 1_000_000_000i128),
        ] {
            if let Some(position) = rest.find(designator) {
                let value = &rest[..position];
                let nanos = parse_decimal_nanos(value)?;
                if designator != 'S' && nanos % 1_000_000_000 != 0 {
                    return None;
                }
                let scaled = if designator == 'S' {
                    nanos
                } else {
                    (nanos / 1_000_000_000).checked_mul(multiplier)?
                };
                total_nanos = total_nanos.checked_add(scaled)?;
                rest = &rest[position + 1..];
            }
        }
        if !rest.is_empty() {
            return None;
        }
    }
    let seconds = i64::try_from(total_nanos / 1_000_000_000).ok()?;
    let nanos = i32::try_from(total_nanos % 1_000_000_000).ok()?;
    Some(Duration::new(seconds, nanos))
}

fn parse_duration_component(source: &str, designator: char, required: bool) -> Option<i64> {
    match source.find(designator) {
        Some(position) => source[..position].parse().ok(),
        None if source.is_empty() && !required => Some(0),
        None => None,
    }
}

fn parse_decimal_nanos(source: &str) -> Option<i128> {
    let (whole, fraction) = source.split_once('.').unwrap_or((source, ""));
    let whole = i128::from(whole.parse::<u64>().ok()?);
    let mut fractional = fraction.to_owned();
    if fractional.len() > 9 {
        if fractional[9..].bytes().any(|byte| byte != b'0') {
            return None;
        }
        fractional.truncate(9);
    }
    while fractional.len() < 9 {
        fractional.push('0');
    }
    whole
        .checked_mul(1_000_000_000)?
        .checked_add(i128::from(fractional.parse::<u32>().ok()?))
}

fn parse_event_time(source: &str, offset_minutes: Option<&Value>) -> Option<EventTime> {
    if let Ok(datetime) = OffsetDateTime::parse(source, &Rfc3339) {
        return Some(event_time_from_offset_datetime(
            source,
            datetime,
            TimeResolution::ProviderOffset,
        ));
    }
    let local = PrimitiveDateTime::parse(source, &Iso8601::DEFAULT).ok()?;
    if let Some(minutes) = offset_minutes.and_then(value_i64)
        && let Ok(seconds) = i32::try_from(minutes.checked_mul(60)?)
        && let Ok(offset) = UtcOffset::from_whole_seconds(seconds)
    {
        return Some(event_time_from_offset_datetime(
            source,
            local.assume_offset(offset),
            TimeResolution::ProviderOffset,
        ));
    }
    Some(EventTime::LocalUnresolved {
        local: local_datetime(local),
        source_text: source.to_owned(),
    })
}

fn event_time_from_offset_datetime(
    source: &str,
    datetime: OffsetDateTime,
    resolution: TimeResolution,
) -> EventTime {
    EventTime::Utc {
        unix_micros: i64::try_from(datetime.unix_timestamp_nanos() / 1_000)
            .unwrap_or_else(|_| datetime.unix_timestamp().saturating_mul(1_000_000)),
        resolution,
        source_text: Some(source.to_owned()),
        local: Some(local_datetime(PrimitiveDateTime::new(
            datetime.date(),
            datetime.time(),
        ))),
        offset_seconds: Some(datetime.offset().whole_seconds()),
    }
}

fn local_datetime(datetime: PrimitiveDateTime) -> LocalDateTime {
    LocalDateTime {
        year: datetime.year(),
        month: datetime.month() as u8,
        day: datetime.day(),
        hour: datetime.hour(),
        minute: datetime.minute(),
        second: datetime.second(),
        nanosecond: datetime.nanosecond(),
    }
}

fn advance_event_time(start: &EventTime, duration: Duration) -> Option<EventTime> {
    match start {
        EventTime::Utc {
            unix_micros,
            offset_seconds,
            ..
        } => {
            let timestamp_nanos = i128::from(*unix_micros).checked_mul(1_000)?;
            let utc = OffsetDateTime::from_unix_timestamp_nanos(timestamp_nanos).ok()?;
            let offset = UtcOffset::from_whole_seconds(offset_seconds.unwrap_or(0)).ok()?;
            let end = utc.to_offset(offset) + duration;
            Some(event_time_from_offset_datetime(
                "",
                end,
                TimeResolution::RecordContext,
            ))
        }
        EventTime::LocalUnresolved { source_text, .. } => {
            let local = PrimitiveDateTime::parse(source_text, &Iso8601::DEFAULT).ok()?;
            let end = local + duration;
            let source_text = format_local_datetime(end);
            Some(EventTime::LocalUnresolved {
                local: local_datetime(end),
                source_text,
            })
        }
    }
}

fn format_local_datetime(datetime: PrimitiveDateTime) -> String {
    let mut formatted = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        datetime.year(),
        datetime.month() as u8,
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        datetime.second(),
    );
    if datetime.nanosecond() != 0 {
        let fraction = format!("{:09}", datetime.nanosecond());
        formatted.push('.');
        formatted.push_str(fraction.trim_end_matches('0'));
    }
    formatted
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

/// Decode a top-level JSON array one exercise at a time.
///
/// The callback runs before the next array item is decoded. Memory is thus
/// bounded by the largest single exercise plus serde's input buffer, rather
/// than by the number of exercises. The original response should still be
/// archived byte-for-byte by the caller.
pub fn for_each_exercise<R, F>(reader: R, mut consume: F) -> Result<(), DecodeError>
where
    R: Read,
    F: FnMut(Exercise) -> Result<(), String>,
{
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    deserializer
        .deserialize_seq(ExerciseListVisitor {
            consume: &mut consume,
        })
        .map_err(DecodeError::Json)?;
    deserializer.end().map_err(DecodeError::Json)
}

struct ExerciseListVisitor<'a, F> {
    consume: &'a mut F,
}

impl<'de, F> de::Visitor<'de> for ExerciseListVisitor<'_, F>
where
    F: FnMut(Exercise) -> Result<(), String>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of Polar exercise records")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        while let Some(exercise) = seq.next_element::<Exercise>()? {
            (self.consume)(exercise).map_err(de::Error::custom)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum DecodeError {
    Json(serde_json::Error),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(f, "invalid Polar exercise response: {error}"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::path::PathBuf;

    fn fixture() -> File {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/polar/exercises.json");
        File::open(path).unwrap()
    }

    #[test]
    fn streams_exercise_with_samples_zones_route_and_unknown_fields() {
        let mut items = Vec::new();
        for_each_exercise(fixture(), |exercise| {
            items.push(exercise);
            Ok(())
        })
        .unwrap();

        assert_eq!(items.len(), 1);
        let exercise = &items[0];
        assert_eq!(exercise.id.as_str(), Some("2AC312F"));
        assert_eq!(exercise.sport.as_deref(), Some("OTHER"));
        assert_eq!(exercise.duration.as_deref(), Some("PT2H44M"));
        assert_eq!(exercise.calories.as_ref().unwrap().as_u64(), Some(530));
        assert_eq!(exercise.distance.as_ref().unwrap().as_u64(), Some(1600));
        assert_eq!(
            exercise.samples.as_ref().unwrap()[0].sample_type.as_deref(),
            Some("1")
        );
        assert_eq!(
            exercise.samples.as_ref().unwrap()[0]
                .data
                .as_ref()
                .unwrap()
                .as_str(),
            Some(
                "0,100,102,97,97,101,103,106,96,89,88,87,98,108,113,112,114,115,118,121,121,121,121,123,117,119,122"
            )
        );
        assert_eq!(
            exercise.heart_rate_zones.as_ref().unwrap()[0]
                .in_zone
                .as_deref(),
            Some("PT4S")
        );
        assert_eq!(
            exercise.route.as_ref().unwrap()[0].provider_fields["satellites"].as_i64(),
            Some(4)
        );
        assert_eq!(exercise.provider_fields["future-enum"], "NEW_POLAR_STATE");
        assert_eq!(
            exercise.samples.as_ref().unwrap()[1].sample_type.as_deref(),
            Some("FUTURE_SAMPLE_TYPE")
        );
        assert_eq!(
            exercise.samples.as_ref().unwrap()[1].provider_fields["vendor-field"],
            "retained"
        );

        let round_trip: Value = serde_json::from_str(&exercise.provider_json().unwrap()).unwrap();
        assert_eq!(round_trip["future-enum"], "NEW_POLAR_STATE");
        assert_eq!(
            round_trip["samples"][1]["sample-type"],
            "FUTURE_SAMPLE_TYPE"
        );
        assert_eq!(round_trip["route"][0]["satellites"], 4);
    }

    #[test]
    fn emits_prior_exercises_before_a_later_malformed_element() {
        let input = br#"[{"id":1},{"id":2,"duration":7}]"#;
        let mut received = 0;
        let result = for_each_exercise(&input[..], |_| {
            received += 1;
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(received, 1);
    }

    #[test]
    fn accepts_unknown_enum_like_strings_without_mapping_them() {
        let input = br#"[{"id":1,"sport":"A_NEW_SPORT","samples":[{"sample-type":"NEW_KIND"}]}]"#;
        let mut decoded = None;
        for_each_exercise(&input[..], |exercise| {
            decoded = Some(exercise);
            Ok(())
        })
        .unwrap();
        let exercise = decoded.unwrap();
        assert_eq!(exercise.sport.as_deref(), Some("A_NEW_SPORT"));
        assert_eq!(
            exercise.samples.unwrap()[0].sample_type.as_deref(),
            Some("NEW_KIND")
        );
    }

    #[test]
    fn normalizes_supported_summary_units_and_preserves_zero() {
        let exercise: Exercise = serde_json::from_str(
            r#"{
                "id":"session-1",
                "start_time":"2026-04-03T12:00:00",
                "start_time_utc_offset":180,
                "duration":"PT1H2M3S",
                "calories":0,
                "distance":12.5,
                "heart_rate":{"average":0,"maximum":190},
                "training_load":42,
                "fat-percentage":20,
                "running-index":55,
                "heart_rate_zones":[{"index":2,"in-zone":"PT1M1.5S"}]
            }"#,
        )
        .unwrap();

        let normalized = exercise.normalized_metrics();
        let values = normalized
            .metrics
            .iter()
            .map(|metric| (metric.metric.as_str(), metric.value.as_str(), &metric.unit))
            .collect::<Vec<_>>();
        assert!(values.contains(&("calories", "0", &Unit::Kilocalories)));
        assert!(values.contains(&("distance", "12.5", &Unit::Meters)));
        assert!(values.contains(&("heart-rate-average", "0", &Unit::BeatsPerMinute)));
        assert!(values.contains(&("heart-rate-maximum", "190", &Unit::BeatsPerMinute)));
        assert!(values.contains(&("fat-percentage", "20", &Unit::Percent)));
        let zone = normalized
            .metrics
            .iter()
            .find(|metric| metric.metric.as_str() == "heart-rate-zone-2-duration")
            .unwrap();
        assert_eq!(zone.value.as_str(), "61.5");
        assert_eq!(zone.unit, Unit::Seconds);
        assert!(zone.time.as_ref().unwrap().is_utc_resolved());
        assert_eq!(zone.source_time.as_deref(), Some("2026-04-03T12:00:00"));
    }

    #[test]
    fn absent_and_null_values_are_omitted_but_zero_is_present() {
        let exercise: Exercise = serde_json::from_str(
            r#"{"id":1,"calories":null,"distance":0,"heart_rate":{"average":null}}"#,
        )
        .unwrap();
        let metrics = exercise.normalized_metrics();
        assert_eq!(metrics.metrics.len(), 1);
        assert_eq!(metrics.metrics[0].metric.as_str(), "distance");
        assert_eq!(metrics.metrics[0].value.as_str(), "0");

        let absent: Exercise = serde_json::from_str(r#"{"id":1}"#).unwrap();
        assert!(absent.normalized_metrics().metrics.is_empty());
    }

    #[test]
    fn stable_exercise_and_metric_ids_depend_on_provider_identity() {
        let first: Exercise = serde_json::from_str(r#"{"id":"x","calories":10}"#).unwrap();
        let same: Exercise = serde_json::from_str(r#"{"id":"x","calories":20}"#).unwrap();
        let other: Exercise = serde_json::from_str(r#"{"id":"y","calories":10}"#).unwrap();
        assert_eq!(first.stable_id(), same.stable_id());
        assert_ne!(first.stable_id(), other.stable_id());
        assert_eq!(
            first.normalized_metrics().metrics[0].id,
            same.normalized_metrics().metrics[0].id
        );
        assert_ne!(
            first.normalized_metrics().metrics[0].id,
            other.normalized_metrics().metrics[0].id
        );
    }

    #[test]
    fn timestamp_resolution_requires_an_offset_and_csv_samples_have_no_time() {
        let resolved: Exercise = serde_json::from_str(
            r#"{"id":1,"start_time":"2026-04-03T12:00:00","start_time_utc_offset":180,"samples":[{"sample-type":"HEART_RATE","data":"0,100,102"}]}"#,
        )
        .unwrap();
        let EventTime::Utc { resolution, .. } = resolved.event_time().unwrap() else {
            panic!("explicit provider offset should resolve to UTC");
        };
        assert_eq!(resolution, TimeResolution::ProviderOffset);
        let sample_metrics = resolved.normalized_metrics();
        assert_eq!(sample_metrics.metrics.len(), 3);
        assert!(
            sample_metrics
                .metrics
                .iter()
                .all(|metric| metric.time.is_none())
        );
        assert_eq!(sample_metrics.metrics[0].sample_index, Some(0));

        let unresolved: Exercise =
            serde_json::from_str(r#"{"id":2,"start_time":"2026-04-03T12:00:00"}"#).unwrap();
        assert!(matches!(
            unresolved.event_time(),
            Some(EventTime::LocalUnresolved { .. })
        ));
        assert!(unresolved.normalized_metrics().metrics.is_empty());
    }

    #[test]
    fn unknown_sample_types_are_not_normalized() {
        let exercise: Exercise = serde_json::from_str(
            r#"{"id":1,"samples":[{"sample-type":"FUTURE_KIND","data":"1,2,3"}]}"#,
        )
        .unwrap();
        assert!(exercise.normalized_metrics().metrics.is_empty());
        assert_eq!(
            exercise.samples.unwrap()[0].sample_type.as_deref(),
            Some("FUTURE_KIND")
        );
    }
}
