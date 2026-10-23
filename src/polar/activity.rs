//! Streaming decoder for Polar AccessLink daily activity responses.
//!
//! The response is a JSON array. [`for_each_daily_activity`] parses one array
//! element at a time and invokes its callback before decoding the next element,
//! so memory use does not grow with the requested date range. The caller should
//! archive the original response bytes separately; `provider_fields` retains
//! unknown fields semantically for forward compatibility.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;

use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;

use crate::model::{
    Aggregation, DataQuality, EventTime, LocalDateTime, Measurement, MeasurementContext, MetricId,
    NumericValue, Provenance, StableId, TimeInterval, TimeResolution, Unit,
};

/// One daily activity entry as returned by AccessLink.
///
/// Known fields remain optional because Polar may omit values. Unknown fields
/// are retained in `provider_fields`; callers must not infer an absent field to
/// be zero. Date-only daily totals deliberately have no fabricated UTC time.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DailyActivity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_duration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactive_duration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daily_activity: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calories: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_calories: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance_from_steps: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactivity_alert_count: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples: Option<ActivitySamples>,
    /// All provider properties not explicitly modelled by this version.
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// Optional sample details embedded in each activity summary.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivitySamples {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps: Option<StepSamples>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_zones: Option<ActivityZoneSamples>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactivity_stamps: Option<InactivityStamps>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// Polar has returned inactivity stamps both as a bare array and as an
/// object containing a `samples` array. Keep the response shape so provider
/// records remain faithful while normalizing the samples in either form.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum InactivityStamps {
    List(Vec<Value>),
    Object {
        #[serde(default)]
        samples: Vec<Value>,
        #[serde(flatten)]
        provider_fields: BTreeMap<String, Value>,
    },
}

impl InactivityStamps {
    fn samples(&self) -> &[Value] {
        match self {
            Self::List(samples) => samples,
            Self::Object { samples, .. } => samples,
        }
    }

    fn len(&self) -> usize {
        self.samples().len()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StepSamples {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_steps: Option<Value>,
    #[serde(default)]
    pub samples: Vec<StepSample>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StepSample {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityZoneSamples {
    #[serde(default)]
    pub samples: Vec<ActivityZoneSample>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityZoneSample {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// A daily total whose unit and meaning are explicitly supported by the field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DailyMetric {
    /// Provider field name, kept stable and unmodified.
    pub metric: &'static str,
    pub value: NumericValue,
    pub unit: Unit,
}

impl DailyActivity {
    /// Returns supported scalar totals without combining them with intraday
    /// samples. Values that are absent, null, or non-numeric are not emitted.
    pub fn normalized_totals(&self) -> Vec<DailyMetric> {
        let mut totals = Vec::with_capacity(7);
        push_numeric(&mut totals, "steps", &self.steps, Unit::Steps);
        push_numeric(&mut totals, "calories", &self.calories, Unit::Kilocalories);
        push_numeric(
            &mut totals,
            "active-calories",
            &self.active_calories,
            Unit::Kilocalories,
        );
        push_numeric(
            &mut totals,
            "distance-from-steps",
            &self.distance_from_steps,
            Unit::Meters,
        );
        push_numeric(
            &mut totals,
            "daily-activity",
            &self.daily_activity,
            Unit::Percent,
        );
        push_duration(&mut totals, "active-duration", &self.active_duration);
        push_duration(&mut totals, "inactive-duration", &self.inactive_duration);
        totals
    }

    pub fn date(&self) -> Option<&str> {
        self.samples
            .as_ref()
            .and_then(|samples| samples.date.as_deref())
            .or_else(|| self.start_time.as_deref().and_then(|time| time.get(..10)))
    }

    /// Detailed step-sample values stay separate from provider daily totals.
    pub fn normalized_step_samples(&self) -> Vec<DailyMetric> {
        self.samples
            .as_ref()
            .and_then(|samples| samples.steps.as_ref())
            .map(|steps| {
                steps
                    .samples
                    .iter()
                    .filter_map(|sample| {
                        let Value::Number(number) = sample.steps.as_ref()? else {
                            return None;
                        };
                        Some(DailyMetric {
                            metric: "step-sample",
                            value: NumericValue::new(number.to_string()).ok()?,
                            unit: Unit::Steps,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Normalize numeric daily totals and timestamped step samples into the
    /// shared measurement model. Date-only activity totals use an unresolved
    /// local calendar anchor; they are never assigned an invented UTC instant.
    /// The caller supplies fetch/archive provenance, which is copied and given
    /// stable record-specific external IDs.
    pub fn normalize_measurements(&self, provenance: &Provenance) -> Vec<Measurement> {
        let Some(date) = self.date() else {
            return Vec::new();
        };
        let parent_id = StableId::from_parts([
            provenance.user_id.as_str(),
            provenance.source.as_str(),
            "activities",
            date,
        ]);
        let Some(day_anchor) = parse_event_time(Some(date), "00:00:00") else {
            return Vec::new();
        };
        let day_interval = match (
            self.start_time
                .as_deref()
                .and_then(|value| parse_event_time(Some(date), value)),
            self.end_time
                .as_deref()
                .and_then(|value| parse_event_time(Some(date), value)),
        ) {
            (Some(start), Some(end)) => TimeInterval::new(start, end),
            (Some(start), None) => TimeInterval::point(start),
            _ => TimeInterval::point(day_anchor),
        };

        let detail_count = self
            .samples
            .as_ref()
            .map(|samples| {
                samples
                    .steps
                    .as_ref()
                    .map_or(0, |steps| steps.samples.len())
                    + samples
                        .activity_zones
                        .as_ref()
                        .map_or(0, |zones| zones.samples.len())
                    + samples
                        .inactivity_stamps
                        .as_ref()
                        .map_or(0, InactivityStamps::len)
            })
            .unwrap_or_default();
        let mut normalized = Vec::with_capacity(6 + detail_count);
        let totals = [
            ("steps", &self.steps, Unit::Steps, Aggregation::Sum),
            (
                "calories",
                &self.calories,
                Unit::Kilocalories,
                Aggregation::Sum,
            ),
            (
                "active-calories",
                &self.active_calories,
                Unit::Kilocalories,
                Aggregation::Sum,
            ),
            (
                "distance-from-steps",
                &self.distance_from_steps,
                Unit::Meters,
                Aggregation::Sum,
            ),
            (
                "daily-activity",
                &self.daily_activity,
                Unit::Percent,
                Aggregation::ProviderScore,
            ),
            (
                "inactivity-alert-count",
                &self.inactivity_alert_count,
                Unit::Count,
                Aggregation::Count,
            ),
        ];
        for (metric, value, unit, aggregation) in totals {
            let Some(value) = numeric_value(value) else {
                continue;
            };
            let mut record_provenance = provenance.clone();
            record_provenance.resource = "activities".into();
            record_provenance.provider_external_id = Some(date.to_owned());
            record_provenance.quality = DataQuality::ProviderReported;
            normalized.push(Measurement {
                schema_version: 1,
                id: StableId::from_parts([
                    provenance.user_id.as_str(),
                    provenance.source.as_str(),
                    "activities",
                    date,
                    metric,
                ]),
                provenance: record_provenance,
                metric: MetricId::new(format!("polar.daily_activity.{metric}")),
                value,
                unit,
                time: day_interval.clone(),
                aggregation,
                context: MeasurementContext::Daily,
                parent_id: Some(parent_id.clone()),
            });
        }

        for (metric, source) in [
            ("active-duration", self.active_duration.as_deref()),
            ("inactive-duration", self.inactive_duration.as_deref()),
        ] {
            let Some(value) = source.and_then(crate::polar::exercise::duration_seconds) else {
                continue;
            };
            let mut record_provenance = provenance.clone();
            record_provenance.resource = "activities".into();
            record_provenance.provider_external_id = Some(date.to_owned());
            record_provenance.quality = DataQuality::ProviderReported;
            normalized.push(Measurement {
                schema_version: 1,
                id: StableId::from_parts([
                    provenance.user_id.as_str(),
                    provenance.source.as_str(),
                    "activities",
                    date,
                    metric,
                ]),
                provenance: record_provenance,
                metric: MetricId::new(format!("polar.daily_activity.{metric}")),
                value,
                unit: Unit::Seconds,
                time: day_interval.clone(),
                aggregation: Aggregation::Sum,
                context: MeasurementContext::Daily,
                parent_id: Some(parent_id.clone()),
            });
        }

        if let Some(step_samples) = self
            .samples
            .as_ref()
            .and_then(|samples| samples.steps.as_ref())
        {
            for (index, sample) in step_samples.samples.iter().enumerate() {
                let (Some(value), Some(source_time)) =
                    (numeric_value(&sample.steps), sample.timestamp.as_deref())
                else {
                    continue;
                };
                let Some(time) = parse_event_time(Some(date), source_time) else {
                    continue;
                };
                let sample_key = format!("{date}/{source_time}/{index}");
                let mut sample_provenance = provenance.clone();
                sample_provenance.resource = "activities/samples".into();
                sample_provenance.provider_external_id = Some(sample_key.clone());
                sample_provenance.quality = DataQuality::ProviderReported;
                normalized.push(Measurement {
                    schema_version: 1,
                    id: StableId::from_parts([
                        provenance.user_id.as_str(),
                        provenance.source.as_str(),
                        "activities/samples",
                        &sample_key,
                        "steps",
                    ]),
                    provenance: sample_provenance,
                    metric: MetricId::new("polar.daily_activity.step_sample"),
                    value,
                    unit: Unit::Steps,
                    time: TimeInterval::point(time),
                    aggregation: Aggregation::Sum,
                    context: MeasurementContext::Daily,
                    parent_id: Some(parent_id.clone()),
                });
            }
        }

        if let Some(zones) = self
            .samples
            .as_ref()
            .and_then(|samples| samples.activity_zones.as_ref())
        {
            for (index, sample) in zones.samples.iter().enumerate() {
                let (Some(zone), Some(source_time)) =
                    (sample.zone.as_deref(), sample.timestamp.as_deref())
                else {
                    continue;
                };
                let Some(event_time) = parse_event_time(Some(date), source_time) else {
                    continue;
                };
                // Activity zones are categorical transitions, not durations.
                // Store one provider-reported occurrence using a zone-specific
                // metric key; the raw provider code also remains in the record.
                let zone_key = metric_component(zone);
                let external_id = format!("{date}/activity-zone/{zone}/{source_time}/{index}");
                let mut sample_provenance = provenance.clone();
                sample_provenance.resource = "activities/samples".into();
                sample_provenance.provider_external_id = Some(external_id.clone());
                sample_provenance.quality = DataQuality::ProviderReported;
                normalized.push(Measurement {
                    schema_version: 1,
                    id: StableId::from_parts([
                        provenance.user_id.as_str(),
                        provenance.source.as_str(),
                        "activities/samples",
                        &external_id,
                    ]),
                    provenance: sample_provenance,
                    metric: MetricId::new(format!("polar.daily_activity.activity_zone.{zone_key}")),
                    value: NumericValue::new("1").expect("integer literal is valid"),
                    unit: Unit::Count,
                    time: TimeInterval::point(event_time),
                    aggregation: Aggregation::Count,
                    context: MeasurementContext::Daily,
                    parent_id: Some(parent_id.clone()),
                });
            }
        }

        if let Some(stamps) = self
            .samples
            .as_ref()
            .and_then(|samples| samples.inactivity_stamps.as_ref())
        {
            for (index, stamp) in stamps.samples().iter().enumerate() {
                let Some(source_time) = stamp
                    .get("stamp")
                    .and_then(Value::as_str)
                    .or_else(|| stamp.as_str())
                else {
                    continue;
                };
                let Some(event_time) = parse_event_time(Some(date), source_time) else {
                    continue;
                };
                let external_id = format!("{date}/inactivity-stamp/{source_time}/{index}");
                let mut sample_provenance = provenance.clone();
                sample_provenance.resource = "activities/samples".into();
                sample_provenance.provider_external_id = Some(external_id.clone());
                sample_provenance.quality = DataQuality::ProviderReported;
                normalized.push(Measurement {
                    schema_version: 1,
                    id: StableId::from_parts([
                        provenance.user_id.as_str(),
                        provenance.source.as_str(),
                        "activities/samples",
                        &external_id,
                    ]),
                    provenance: sample_provenance,
                    metric: MetricId::new("polar.daily_activity.inactivity_stamp"),
                    value: NumericValue::new("1").expect("integer literal is valid"),
                    unit: Unit::Count,
                    time: TimeInterval::point(event_time),
                    aggregation: Aggregation::Count,
                    context: MeasurementContext::Daily,
                    parent_id: Some(parent_id.clone()),
                });
            }
        }
        normalized
    }

    /// Serializes this entry, including unknown properties, as a standalone
    /// JSON document. The source response should still be archived byte-for-byte
    /// because this representation does not preserve whitespace or number text.
    pub fn provider_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

fn metric_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() {
            encoded.push(char::from(byte.to_ascii_lowercase()));
        } else {
            encoded.push('_');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    if encoded.is_empty() {
        "unknown".to_owned()
    } else {
        encoded
    }
}

fn numeric_value(value: &Option<Value>) -> Option<NumericValue> {
    let Value::Number(number) = value.as_ref()? else {
        return None;
    };
    NumericValue::new(number.to_string()).ok()
}

/// Parse a provider clock as UTC only when it explicitly carries an offset.
/// Date/time strings without an offset remain unresolved local time. A time-only
/// value is combined with the date from the same provider record.
pub fn parse_event_time(date: Option<&str>, source: &str) -> Option<EventTime> {
    use time::format_description::well_known::Rfc3339;

    let has_clock = source.contains(':');
    let combined;
    let candidate = if !source.contains('T') && has_clock {
        let date = date?;
        combined = format!("{date}T{source}");
        combined.as_str()
    } else {
        source
    };
    if let Ok(timestamp) = time::OffsetDateTime::parse(candidate, &Rfc3339) {
        let local = LocalDateTime {
            year: timestamp.year(),
            month: timestamp.month() as u8,
            day: timestamp.day(),
            hour: timestamp.hour(),
            minute: timestamp.minute(),
            second: timestamp.second(),
            nanosecond: timestamp.nanosecond(),
        };
        return Some(EventTime::Utc {
            unix_micros: timestamp.unix_timestamp_nanos().div_euclid(1_000) as i64,
            resolution: if timestamp.offset().whole_seconds() == 0 {
                TimeResolution::ProviderUtc
            } else {
                TimeResolution::ProviderOffset
            },
            source_text: Some(source.to_owned()),
            local: Some(local),
            offset_seconds: Some(timestamp.offset().whole_seconds()),
        });
    }

    let (date_text, time_text) = if let Some((date_text, time_text)) = source.split_once('T') {
        (date_text, time_text)
    } else if source.len() > 10 && source.as_bytes().get(10) == Some(&b' ') {
        (&source[..10], &source[11..])
    } else {
        (date?, source)
    };
    if time_text.contains('Z') || time_text.contains('+') || time_text.rfind('-').is_some() {
        return None;
    }
    let date_format = time::format_description::parse_borrowed::<1>("[year]-[month]-[day]").ok()?;
    let parsed_date = time::Date::parse(date_text, &date_format).ok()?;
    let clock = time_text.split(['Z', '+', '-']).next()?;
    let mut fields = clock.split(':');
    let hour = fields.next()?.parse::<u8>().ok()?;
    let minute = fields.next()?.parse::<u8>().ok()?;
    let seconds_text = fields.next().unwrap_or("0");
    let (second_text, fraction_text) = seconds_text.split_once('.').unwrap_or((seconds_text, ""));
    let second = second_text.parse::<u8>().ok()?;
    let mut fraction = fraction_text.chars().take(9).collect::<String>();
    while fraction.len() < 9 {
        fraction.push('0');
    }
    let nanosecond = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u32>().ok()?
    };
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let local = LocalDateTime {
        year: parsed_date.year(),
        month: parsed_date.month() as u8,
        day: parsed_date.day(),
        hour,
        minute,
        second,
        nanosecond,
    };
    Some(EventTime::LocalUnresolved {
        local,
        source_text: source.to_owned(),
    })
}

fn push_numeric(
    out: &mut Vec<DailyMetric>,
    metric: &'static str,
    value: &Option<Value>,
    unit: Unit,
) {
    let Some(Value::Number(number)) = value else {
        return;
    };
    if let Ok(value) = NumericValue::new(number.to_string()) {
        out.push(DailyMetric {
            metric,
            value,
            unit,
        });
    }
}

fn push_duration(out: &mut Vec<DailyMetric>, metric: &'static str, value: &Option<String>) {
    if let Some(value) = value
        .as_deref()
        .and_then(crate::polar::exercise::duration_seconds)
    {
        out.push(DailyMetric {
            metric,
            value,
            unit: Unit::Seconds,
        });
    }
}

/// Decode an AccessLink daily-activities JSON array incrementally.
///
/// `consume` is called once per record, in response order. Returning an error
/// stops decoding immediately. Only one activity record (plus serde's bounded
/// input buffer) is materialized at once; preserving unknown fields requires
/// memory proportional to that single record.
pub fn for_each_daily_activity<R, F>(reader: R, mut consume: F) -> Result<(), DecodeError>
where
    R: Read,
    F: FnMut(DailyActivity) -> Result<(), String>,
{
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    deserializer
        .deserialize_seq(ActivityListVisitor {
            consume: &mut consume,
        })
        .map_err(DecodeError::Json)?;
    deserializer.end().map_err(DecodeError::Json)
}

struct ActivityListVisitor<'a, F> {
    consume: &'a mut F,
}

impl<'de, F> de::Visitor<'de> for ActivityListVisitor<'_, F>
where
    F: FnMut(DailyActivity) -> Result<(), String>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of Polar daily activity records")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        while let Some(activity) = seq.next_element::<DailyActivity>()? {
            (self.consume)(activity).map_err(de::Error::custom)?;
        }
        Ok(())
    }
}

/// Errors produced while decoding or consuming a streaming response.
#[derive(Debug)]
pub enum DecodeError {
    Json(serde_json::Error),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(f, "invalid daily activity response: {error}"),
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
    use crate::model::{SourceId, UserId};
    use std::fs::File;
    use std::path::PathBuf;

    fn fixture() -> File {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/polar/daily_activity.json");
        File::open(path).unwrap()
    }

    fn provenance() -> Provenance {
        Provenance {
            user_id: UserId::new("user-1"),
            source: SourceId::new("polar"),
            resource: "activities".into(),
            provider_external_id: None,
            source_device_id: None,
            fetched_at_utc_micros: 1_798_000_000_000_000,
            archive_ref: Some("b3:test".into()),
            revision: None,
            quality: DataQuality::ProviderReported,
        }
    }

    #[test]
    fn decodes_golden_activity_and_keeps_unknown_provider_values() {
        let mut records = Vec::new();
        for_each_daily_activity(fixture(), |entry| {
            records.push(entry);
            Ok(())
        })
        .unwrap();

        assert_eq!(records.len(), 2);
        let day = &records[0];
        assert_eq!(day.date(), Some("2026-10-19"));
        assert_eq!(day.steps.as_ref().unwrap().as_u64(), Some(8823));
        assert_eq!(
            day.samples
                .as_ref()
                .unwrap()
                .activity_zones
                .as_ref()
                .unwrap()
                .samples
                .len(),
            2
        );
        assert_eq!(
            day.samples
                .as_ref()
                .unwrap()
                .activity_zones
                .as_ref()
                .unwrap()
                .samples[0]
                .zone
                .as_deref(),
            Some("SEDENTARY")
        );
        assert_eq!(
            day.samples
                .as_ref()
                .unwrap()
                .inactivity_stamps
                .as_ref()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(day.provider_fields["future-status"], "new-polar-code");
        assert_eq!(
            day.samples
                .as_ref()
                .unwrap()
                .activity_zones
                .as_ref()
                .unwrap()
                .samples[1]
                .provider_fields["future-zone-code"],
            71
        );

        let totals = day.normalized_totals();
        assert_eq!(totals.len(), 7);
        assert!(totals.iter().any(|metric| {
            metric.metric == "steps"
                && metric.value.as_str() == "8823"
                && metric.unit == Unit::Steps
        }));
        assert!(totals.iter().any(|metric| {
            metric.metric == "distance-from-steps"
                && metric.value.as_str() == "4590.53"
                && metric.unit == Unit::Meters
        }));
        assert!(totals.iter().any(|metric| {
            metric.metric == "active-duration"
                && metric.value.as_str() == "11460"
                && metric.unit == Unit::Seconds
        }));
        assert!(totals.iter().any(|metric| {
            metric.metric == "inactive-duration"
                && metric.value.as_str() == "66210"
                && metric.unit == Unit::Seconds
        }));
        assert_eq!(day.normalized_step_samples()[1].value.as_str(), "42");

        let provider_json = day.provider_json().unwrap();
        let preserved: Value = serde_json::from_str(&provider_json).unwrap();
        assert_eq!(preserved["future-status"], "new-polar-code");
        assert_eq!(
            preserved["samples"]["activity_zones"]["samples"][1]["future-zone-code"],
            71
        );
    }

    #[test]
    fn decodes_inactivity_stamps_as_both_arrays_and_sample_objects() {
        let input = br#"[
            {"samples":{"date":"2026-10-21","inactivity_stamps":[{"stamp":"2026-10-21T03:00:00+02:00"}]}},
            {"samples":{"date":"2026-10-22","inactivity_stamps":{"samples":[{"stamp":"2026-10-22T03:00:00+02:00"}],"future-field":true}}}
        ]"#;
        let mut records = Vec::new();
        for_each_daily_activity(&input[..], |entry| {
            records.push(entry);
            Ok(())
        })
        .unwrap();

        assert_eq!(records.len(), 2);
        for (record, expected_date) in records.iter().zip(["2026-10-21", "2026-10-22"]) {
            assert_eq!(record.date(), Some(expected_date));
            assert_eq!(
                record
                    .samples
                    .as_ref()
                    .unwrap()
                    .inactivity_stamps
                    .as_ref()
                    .unwrap()
                    .len(),
                1
            );
            let measurements = record.normalize_measurements(&provenance());
            assert!(measurements.iter().any(|measurement| {
                measurement.metric.as_str() == "polar.daily_activity.inactivity_stamp"
            }));
        }
    }

    #[test]
    fn emits_prior_records_before_a_later_decode_error() {
        let input = br#"[{"start_time":"2026-10-20","steps":0},{"samples":9}]"#;
        let mut received = 0;
        let result = for_each_daily_activity(&input[..], |_| {
            received += 1;
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(received, 1);
    }

    #[test]
    fn absent_and_null_are_not_converted_to_zero() {
        let input = br#"[{"start_time":"2026-10-21","steps":null,"calories":null}]"#;
        let mut records = Vec::new();
        for_each_daily_activity(&input[..], |entry| {
            records.push(entry);
            Ok(())
        })
        .unwrap();
        assert!(records[0].normalized_totals().is_empty());
    }

    #[test]
    fn normalizes_daily_totals_and_step_samples_without_fabricating_utc() {
        let mut records = Vec::new();
        for_each_daily_activity(fixture(), |entry| {
            records.push(entry);
            Ok(())
        })
        .unwrap();
        let normalized = records[0].normalize_measurements(&provenance());
        assert_eq!(normalized.len(), 13); // totals, durations, step/zone samples, and stamps
        let steps = normalized
            .iter()
            .find(|measurement| measurement.metric.as_str() == "polar.daily_activity.steps")
            .unwrap();
        assert_eq!(steps.value.as_str(), "8823");
        assert_eq!(steps.unit, Unit::Steps);
        assert_eq!(steps.aggregation, Aggregation::Sum);
        assert_eq!(steps.context, MeasurementContext::Daily);
        assert_eq!(
            steps.provenance.provider_external_id.as_deref(),
            Some("2026-10-19")
        );
        assert_eq!(steps.provenance.archive_ref.as_deref(), Some("b3:test"));
        let EventTime::LocalUnresolved { source_text, .. } = &steps.time.start else {
            panic!("local activity day must not be assigned UTC")
        };
        assert_eq!(source_text, "2026-10-19T08:15:30");

        let sample = normalized
            .iter()
            .filter(|measurement| measurement.metric.as_str() == "polar.daily_activity.step_sample")
            .nth(1)
            .unwrap();
        assert_eq!(sample.value.as_str(), "42");
        assert_eq!(sample.time.start.utc_micros(), None);
        assert_eq!(sample.provenance.resource, "activities/samples");
        assert_ne!(sample.id, steps.id);

        let zone = normalized
            .iter()
            .find(|measurement| {
                measurement.metric.as_str() == "polar.daily_activity.activity_zone.sedentary"
            })
            .unwrap();
        assert_eq!(zone.value.as_str(), "1");
        assert_eq!(zone.unit, Unit::Count);
        assert_eq!(zone.aggregation, Aggregation::Count);
        assert_eq!(zone.time.start.utc_micros(), None);
        assert_eq!(zone.provenance.resource, "activities/samples");

        let inactivity = normalized
            .iter()
            .find(|measurement| {
                measurement.metric.as_str() == "polar.daily_activity.inactivity_stamp"
            })
            .unwrap();
        assert_eq!(inactivity.value.as_str(), "1");
        assert_eq!(inactivity.time.start.utc_micros(), None);

        let active_duration = normalized
            .iter()
            .find(|measurement| {
                measurement.metric.as_str() == "polar.daily_activity.active-duration"
            })
            .unwrap();
        assert_eq!(active_duration.value.as_str(), "11460");
        assert_eq!(active_duration.unit, Unit::Seconds);
        assert_eq!(active_duration.aggregation, Aggregation::Sum);
        assert_eq!(active_duration.time.start.utc_micros(), None);
        assert_eq!(
            active_duration.parent_id.as_ref().unwrap().as_str(),
            StableId::from_parts(["user-1", "polar", "activities", "2026-10-19"]).as_str()
        );
    }

    #[test]
    fn activity_zone_and_inactivity_events_resolve_only_with_explicit_offset() {
        let input = br#"[{"samples":{"date":"2026-10-21","activity_zones":{"samples":[{"zone":"VIGOROUS","timestamp":"2026-10-21T02:30:00+02:00"}]},"inactivity_stamps":[{"stamp":"2026-10-21T03:00:00+02:00"}]}}]"#;
        let mut records = Vec::new();
        for_each_daily_activity(&input[..], |entry| {
            records.push(entry);
            Ok(())
        })
        .unwrap();
        let normalized = records[0].normalize_measurements(&provenance());
        let zone = normalized
            .iter()
            .find(|measurement| {
                measurement.metric.as_str() == "polar.daily_activity.activity_zone.vigorous"
            })
            .unwrap();
        assert_eq!(zone.time.start.utc_micros(), Some(1_792_542_600_000_000));
        let inactivity = normalized
            .iter()
            .find(|measurement| {
                measurement.metric.as_str() == "polar.daily_activity.inactivity_stamp"
            })
            .unwrap();
        assert!(inactivity.time.is_utc_resolved());
    }

    #[test]
    fn explicit_activity_sample_offset_resolves_to_utc() {
        let input = br#"[{"start_time":"2026-10-21","samples":{"date":"2026-10-21","steps":{"samples":[{"steps":3,"timestamp":"2026-10-21T02:30:00+02:00"}]}}}]"#;
        let mut records = Vec::new();
        for_each_daily_activity(&input[..], |entry| {
            records.push(entry);
            Ok(())
        })
        .unwrap();
        let normalized = records[0].normalize_measurements(&provenance());
        let sample = normalized
            .iter()
            .find(|measurement| measurement.metric.as_str() == "polar.daily_activity.step_sample")
            .unwrap();
        let EventTime::Utc {
            unix_micros,
            resolution,
            offset_seconds,
            ..
        } = &sample.time.start
        else {
            panic!("explicit offset must resolve")
        };
        assert_eq!(*unix_micros, 1_792_542_600_000_000);
        assert_eq!(*resolution, TimeResolution::ProviderOffset);
        assert_eq!(*offset_seconds, Some(7200));
    }
}
