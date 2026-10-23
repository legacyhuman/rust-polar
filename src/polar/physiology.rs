//! AccessLink continuous-heart-rate and cardio-load decoders.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;

use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;

use crate::model::{
    Aggregation, DataQuality, Measurement, MeasurementContext, MetricId, NumericValue, Provenance,
    StableId, TimeInterval, Unit,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ContinuousHeartRate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub polar_user: Option<String>,
    pub date: String,
    #[serde(default)]
    pub heart_rate_samples: Vec<HeartRateSample>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HeartRateSample {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heart_rate: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_time: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

impl ContinuousHeartRate {
    /// Normalize every numeric, timestamped Polar continuous-HR sample.
    /// Sample times are combined with this response's date. They remain local
    /// and unresolved unless the sample itself includes an explicit offset.
    /// Missing values or timestamps are retained in the provider document but
    /// cannot become time-indexable measurements.
    pub fn normalize_measurements(&self, provenance: &Provenance) -> Vec<Measurement> {
        let mut out = Vec::with_capacity(self.heart_rate_samples.len());
        for (index, sample) in self.heart_rate_samples.iter().enumerate() {
            let (Some(Value::Number(number)), Some(sample_time)) =
                (sample.heart_rate.as_ref(), sample.sample_time.as_deref())
            else {
                continue;
            };
            let Ok(value) = NumericValue::new(number.to_string()) else {
                continue;
            };
            let Some(time) = super::activity::parse_event_time(Some(&self.date), sample_time)
            else {
                continue;
            };
            let sample_key = format!("{}/{sample_time}/{index}", self.date);
            let mut sample_provenance = provenance.clone();
            sample_provenance.resource = "continuous-heart-rate".into();
            sample_provenance.provider_external_id = Some(sample_key.clone());
            sample_provenance.quality = DataQuality::ProviderReported;
            out.push(Measurement {
                schema_version: 1,
                id: StableId::from_parts([
                    provenance.user_id.as_str(),
                    provenance.source.as_str(),
                    "continuous-heart-rate",
                    &sample_key,
                    "heart-rate",
                ]),
                provenance: sample_provenance,
                metric: MetricId::new("polar.continuous_heart_rate"),
                value,
                unit: Unit::BeatsPerMinute,
                time: TimeInterval::point(time),
                aggregation: Aggregation::Instant,
                context: MeasurementContext::Unknown("continuous-heart-rate".into()),
                parent_id: None,
            });
        }
        out
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct CardioLoad {
    pub date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardio_load_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardio_load: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strain: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerance: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardio_load_ratio: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardio_load_level: Option<Value>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub enum DecodeError {
    Json(serde_json::Error),
}
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(e) => write!(f, "invalid Polar physiology response: {e}"),
        }
    }
}
impl std::error::Error for DecodeError {}
impl From<serde_json::Error> for DecodeError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

/// Continuous-heart-rate date/range response is one object containing a day's
/// samples; the date and sample clock strings remain unresolved local time.
pub fn decode_continuous_heart_rate<R: Read>(
    reader: R,
) -> Result<ContinuousHeartRate, DecodeError> {
    Ok(serde_json::from_reader(reader)?)
}

/// Cardio-load range/history responses are arrays; consume one day at a time.
pub fn for_each_cardio_load<R, F>(reader: R, mut consume: F) -> Result<(), DecodeError>
where
    R: Read,
    F: FnMut(CardioLoad) -> Result<(), String>,
{
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    deserializer.deserialize_seq(CardioLoadVisitor {
        consume: &mut consume,
    })?;
    deserializer.end()?;
    Ok(())
}

struct CardioLoadVisitor<'a, F> {
    consume: &'a mut F,
}
impl<'de, F> de::Visitor<'de> for CardioLoadVisitor<'_, F>
where
    F: FnMut(CardioLoad) -> Result<(), String>,
{
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array of Polar cardio-load days")
    }
    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while let Some(record) = seq.next_element::<CardioLoad>()? {
            (self.consume)(record).map_err(de::Error::custom)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventTime, SourceId, TimeResolution, UserId};
    use std::fs::File;
    use std::path::PathBuf;

    fn fixture(path: &str) -> File {
        File::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/polar/{path}")),
        )
        .unwrap()
    }

    fn provenance() -> Provenance {
        Provenance {
            user_id: UserId::new("user-1"),
            source: SourceId::new("polar"),
            resource: "placeholder".into(),
            provider_external_id: None,
            source_device_id: None,
            fetched_at_utc_micros: 1_798_000_000_000_000,
            archive_ref: Some("b3:continuous-hr".into()),
            revision: None,
            quality: DataQuality::ProviderReported,
        }
    }

    #[test]
    fn continuous_hr_keeps_provider_local_times_and_all_samples() {
        let data = decode_continuous_heart_rate(fixture("continuous_heart_rate.json")).unwrap();
        assert_eq!(data.date, "2026-10-20");
        assert_eq!(data.heart_rate_samples.len(), 2);
        assert_eq!(data.heart_rate_samples[0].heart_rate.as_ref().unwrap(), 63);
        assert_eq!(
            data.heart_rate_samples[0].sample_time.as_deref(),
            Some("00:02:08")
        );
        assert_eq!(
            data.heart_rate_samples[1].provider_fields["future-quality"],
            "estimated"
        );

        let normalized = data.normalize_measurements(&provenance());
        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized[0].metric.as_str(), "polar.continuous_heart_rate");
        assert_eq!(normalized[0].value.as_str(), "63");
        assert_eq!(normalized[0].unit, Unit::BeatsPerMinute);
        assert_eq!(normalized[0].aggregation, Aggregation::Instant);
        assert_eq!(normalized[0].provenance.resource, "continuous-heart-rate");
        assert_eq!(
            normalized[0].provenance.archive_ref.as_deref(),
            Some("b3:continuous-hr")
        );
        assert_eq!(normalized[0].time.start.utc_micros(), None);
        assert_ne!(normalized[0].id, normalized[1].id);
        assert_eq!(
            normalized[0].context,
            MeasurementContext::Unknown("continuous-heart-rate".into())
        );
    }

    #[test]
    fn continuous_hr_resolves_only_sample_times_with_explicit_offsets() {
        let data = decode_continuous_heart_rate(
            &br#"{"date":"2026-10-21","heart_rate_samples":[{"heart_rate":72,"sample_time":"2026-10-21T02:30:00+02:00"}]}"#[..],
        )
        .unwrap();
        let normalized = data.normalize_measurements(&provenance());
        let EventTime::Utc {
            unix_micros,
            resolution,
            offset_seconds,
            local,
            ..
        } = &normalized[0].time.start
        else {
            panic!("explicit offset must resolve")
        };
        assert_eq!(*unix_micros, 1_792_542_600_000_000);
        assert_eq!(*resolution, TimeResolution::ProviderOffset);
        assert_eq!(*offset_seconds, Some(7200));
        assert_eq!(local.unwrap().hour, 2);
    }

    #[test]
    fn continuous_hr_omits_untimestamped_and_non_numeric_values() {
        let data = decode_continuous_heart_rate(
            &br#"{"date":"2026-10-21","heart_rate_samples":[{"heart_rate":75},{"heart_rate":"unknown","sample_time":"10:00:00"}]}"#[..],
        )
        .unwrap();
        assert!(data.normalize_measurements(&provenance()).is_empty());
    }

    #[test]
    fn cardio_load_preserves_unavailable_zero_and_unknown_status() {
        let mut rows = Vec::new();
        for_each_cardio_load(fixture("cardio_load.json"), |row| {
            rows.push(row);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].cardio_load.as_ref().unwrap().as_f64(), Some(0.2));
        assert_eq!(
            rows[0].cardio_load_status.as_deref(),
            Some("LOAD_STATUS_NOT_AVAILABLE")
        );
        assert_eq!(
            rows[0].cardio_load_level.as_ref().unwrap()["very-high"],
            0.1
        );
        assert_eq!(rows[1].cardio_load_status.as_deref(), Some("FUTURE_STATUS"));
        assert_eq!(rows[1].strain.as_ref().unwrap(), 0);
    }
}
