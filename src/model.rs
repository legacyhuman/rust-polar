//! Source-agnostic domain types shared by importers, storage, and query code.
//!
//! Values in this module are deliberately independent of a serialization codec
//! and of redb's key layout. Provider responses should be archived separately;
//! [`ProviderDocument`] also lets normalized records retain provider-specific
//! fields that do not yet have a common representation.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;

/// Stable identifier for a local user. Provider account identifiers may be used
/// directly, provided they are scoped by [`SourceId`] where stored.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserId(pub String);

impl UserId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identifier of a data provider (for example `polar` or `nightscout`).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceId(pub String);

impl SourceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable metric name. Unknown provider metrics remain representable without
/// being discarded or mapped to a semantically different common metric.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetricId(pub String);

impl MetricId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable local identifier constructed from a sequence of identity components.
/// Each component is byte-length-prefixed, so separators within values cannot
/// make two different tuples collide. This is an identity encoding, not a
/// storage key and not a cryptographic hash.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StableId(pub String);

impl StableId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn from_parts<I, S>(parts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut encoded = String::new();
        for part in parts {
            let part = part.as_ref();
            use fmt::Write as _;
            // String formatting is infallible for the in-memory String sink.
            let _ = write!(&mut encoded, "{}:{};", part.len(), part);
        }
        Self(encoded)
    }

    /// Names an object by the source and provider identity fields. The caller
    /// must supply a stable `external_id`; objects without one need a stable
    /// content-derived component from the importer.
    pub fn from_external(
        user: &UserId,
        source: &SourceId,
        resource: &str,
        external_id: &str,
    ) -> Self {
        Self::from_parts([user.as_str(), source.as_str(), resource, external_id])
    }
}

/// Calendar and clock fields as reported in a local timestamp. The exact source
/// timestamp text is kept alongside this value by [`EventTime`], preserving
/// fractions and formatting that these numeric fields cannot express.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct LocalDateTime {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nanosecond: u32,
}

/// Resolution and provenance of a timestamp's UTC interpretation.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum TimeResolution {
    /// The provider supplied an explicit UTC timestamp.
    ProviderUtc,
    /// The provider supplied a local timestamp with an explicit UTC offset.
    ProviderOffset,
    /// Resolved from an unambiguous relationship within the same provider record.
    RecordContext,
    /// Resolved using an explicitly configured IANA timezone.
    ConfiguredTimezone { timezone: String },
}

/// Event time keeps UTC microseconds for indexed queries and the original local
/// representation when available. A local time without a proven timezone has
/// no UTC value and must not be silently put into a time series.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum EventTime {
    Utc {
        unix_micros: i64,
        resolution: TimeResolution,
        /// Original ISO/RFC3339 text, if supplied. Retains sub-microsecond
        /// precision and the provider's exact offset spelling.
        source_text: Option<String>,
        local: Option<LocalDateTime>,
        offset_seconds: Option<i32>,
    },
    LocalUnresolved {
        local: LocalDateTime,
        /// Exact source text, including fractions and any provider formatting.
        source_text: String,
    },
}

impl EventTime {
    pub fn utc(unix_micros: i64) -> Self {
        Self::Utc {
            unix_micros,
            resolution: TimeResolution::ProviderUtc,
            source_text: None,
            local: None,
            offset_seconds: Some(0),
        }
    }

    pub fn utc_micros(&self) -> Option<i64> {
        match self {
            Self::Utc { unix_micros, .. } => Some(*unix_micros),
            Self::LocalUnresolved { .. } => None,
        }
    }
}

/// Half-open event interval `[start, end)`. An absent end represents a point or
/// an interval whose end is not provided by the source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimeInterval {
    pub start: EventTime,
    pub end: Option<EventTime>,
}

impl TimeInterval {
    pub fn point(at: EventTime) -> Self {
        Self {
            start: at,
            end: None,
        }
    }
    pub fn new(start: EventTime, end: EventTime) -> Self {
        Self {
            start,
            end: Some(end),
        }
    }

    /// An interval is suitable for the UTC-indexed `series` table only when
    /// both endpoints (if present) have a resolved UTC value.
    pub fn is_utc_resolved(&self) -> bool {
        self.start.utc_micros().is_some()
            && self
                .end
                .as_ref()
                .is_none_or(|end| end.utc_micros().is_some())
    }
}

/// Exact decimal representation of a provider numeric value.
///
/// The validated JSON-number lexeme is retained instead of converting through
/// `f64`; this preserves decimal precision and distinguishes provider values
/// that are not exactly representable as binary floating point.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NumericValue(String);

impl NumericValue {
    pub fn new(lexeme: impl Into<String>) -> Result<Self, InvalidNumber> {
        let lexeme = lexeme.into();
        if is_json_number(&lexeme) {
            Ok(Self(lexeme))
        } else {
            Err(InvalidNumber)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Approximate numeric conversion for calculations. Keep [`as_str`](Self::as_str)
    /// as the authoritative value when precision matters.
    pub fn to_f64(&self) -> Option<f64> {
        self.0.parse::<f64>().ok().filter(|value| value.is_finite())
    }
}

// Serialize the validated decimal lexeme as a string. serde_json's default
// floating point path cannot preserve arbitrary decimal precision, so emitting
// a JSON number here would silently alter some values. Importers should pass
// the original numeric token to `new`; deserialization accepts that lossless
// string form (and exact integer values) but refuses floating point callbacks.
impl Serialize for NumericValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NumericValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NumericValueVisitor;

        impl<'de> de::Visitor<'de> for NumericValueVisitor {
            type Value = NumericValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a validated decimal string or exact integer")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                NumericValue::new(value).map_err(|_| E::custom("invalid JSON number lexeme"))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                NumericValue::new(value).map_err(|_| E::custom("invalid JSON number lexeme"))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(NumericValue(value.to_string()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(NumericValue(value.to_string()))
            }

            fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Err(E::custom(
                    "floating point deserialization would lose the original decimal lexeme",
                ))
            }
        }

        deserializer.deserialize_any(NumericValueVisitor)
    }
}

/// A value passed to [`NumericValue::new`] was not a valid JSON number.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidNumber;

impl fmt::Display for InvalidNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid JSON number")
    }
}

impl std::error::Error for InvalidNumber {}

fn is_json_number(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if b.get(i) == Some(&b'-') {
        i += 1;
    }
    match b.get(i) {
        Some(b'0') => i += 1,
        Some(b'1'..=b'9') => {
            i += 1;
            while matches!(b.get(i), Some(b'0'..=b'9')) {
                i += 1;
            }
        }
        _ => return false,
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        let fraction_start = i;
        while matches!(b.get(i), Some(b'0'..=b'9')) {
            i += 1;
        }
        if i == fraction_start {
            return false;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let exponent_start = i;
        while matches!(b.get(i), Some(b'0'..=b'9')) {
            i += 1;
        }
        if i == exponent_start {
            return false;
        }
    }
    i == b.len()
}

/// Canonical unit names used by common queries, with an escape hatch for units
/// that are specific to a provider or are added in a future API version.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Unit {
    Unspecified,
    Seconds,
    BeatsPerMinute,
    Milliseconds,
    BreathsPerMinute,
    Steps,
    Kilocalories,
    Meters,
    Millimeters,
    MillimolesPerLiter,
    MilligramsPerDeciliter,
    Percent,
    Count,
    Celsius,
    Unknown(String),
}

/// How a numeric value summarizes or characterizes its time interval.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Aggregation {
    Instant,
    Mean,
    Sum,
    ProviderScore,
    Minimum,
    Maximum,
    Count,
    Unknown(String),
}

/// Broad context used to distinguish otherwise similar measurements.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum MeasurementContext {
    Daily,
    Sleep,
    Exercise,
    NightlyRecharge,
    SleepWise,
    CardioLoad,
    Profile,
    Nightscout,
    Unknown(String),
}

/// Quality and origin of a normalized measurement.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataQuality {
    ProviderReported,
    ProviderEstimated,
    Derived { method: String },
    Unknown(String),
}

/// Common provenance shared by normalized series points and event records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    pub user_id: UserId,
    pub source: SourceId,
    pub resource: String,
    pub provider_external_id: Option<String>,
    pub source_device_id: Option<String>,
    /// UTC microseconds when this representation was fetched from the provider.
    pub fetched_at_utc_micros: i64,
    /// Content-addressed archive reference, when the source response was archived.
    pub archive_ref: Option<String>,
    /// Provider revision or source-record hash, when supplied/available.
    pub revision: Option<String>,
    pub quality: DataQuality,
}

/// A normalized scalar measurement. Unresolved local timestamps can remain in
/// records, but series writers should require `time.is_utc_resolved()`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Measurement {
    pub schema_version: u16,
    pub id: StableId,
    pub provenance: Provenance,
    pub metric: MetricId,
    pub value: NumericValue,
    pub unit: Unit,
    pub time: TimeInterval,
    pub aggregation: Aggregation,
    pub context: MeasurementContext,
    pub parent_id: Option<StableId>,
}

/// Common event/document kinds. Unknown provider values remain intact.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum RecordKind {
    Exercise,
    Sleep,
    DailyActivity,
    NightlyRecharge,
    SleepWise,
    CardioLoad,
    Profile,
    NightscoutEntry,
    NightscoutTreatment,
    DeviceStatus,
    Other(String),
}

/// Provider-specific structured payload retained next to normalized fields.
/// `body` is the exact UTF-8 JSON document text when `media_type` is JSON.
/// Large original responses belong in the streaming archive layer; this type is
/// intended for individual provider records, not whole HTTP response bodies.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderDocument {
    pub media_type: String,
    pub body: String,
}

impl ProviderDocument {
    pub fn json(body: impl Into<String>) -> Self {
        Self {
            media_type: "application/json".into(),
            body: body.into(),
        }
    }
}

/// Event, session, profile, or provider-specific record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub schema_version: u16,
    pub id: StableId,
    pub provenance: Provenance,
    pub kind: RecordKind,
    pub time: Option<TimeInterval>,
    pub parent_id: Option<StableId>,
    pub provider_document: Option<ProviderDocument>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_value_round_trip_preserves_decimal_lexeme() {
        let original = NumericValue::new("0.123456789012345678901".to_owned()).unwrap();
        let encoded = serde_json::to_string(&original).unwrap();
        assert_eq!(encoded, r#""0.123456789012345678901""#);
        let decoded: NumericValue = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.as_str(), "0.123456789012345678901");
    }

    #[test]
    fn numeric_value_rejects_invalid_lexemes_and_lossy_float_input() {
        assert!(NumericValue::new("01".to_owned()).is_err());
        assert!(NumericValue::new("1.".to_owned()).is_err());
        assert!(NumericValue::new("1e+".to_owned()).is_err());
        assert!(NumericValue::new("NaN".to_owned()).is_err());
        assert!(serde_json::from_str::<NumericValue>(r#""01""#).is_err());
        assert!(serde_json::from_str::<NumericValue>("0.1").is_err());
    }

    #[test]
    fn source_agnostic_types_round_trip_with_provider_unknowns() {
        let record = Record {
            schema_version: 1,
            id: StableId::from_parts(["user", "polar", "exercise", "123"]),
            provenance: Provenance {
                user_id: UserId::new("user"),
                source: SourceId::new("polar"),
                resource: "exercises".into(),
                provider_external_id: Some("123".into()),
                source_device_id: None,
                fetched_at_utc_micros: 1_800_000_000_000_000,
                archive_ref: Some("blake3:abc".into()),
                revision: None,
                quality: DataQuality::ProviderReported,
            },
            kind: RecordKind::Other("future-kind".into()),
            time: Some(TimeInterval::point(EventTime::LocalUnresolved {
                local: LocalDateTime {
                    year: 2026,
                    month: 10,
                    day: 25,
                    hour: 2,
                    minute: 30,
                    second: 0,
                    nanosecond: 123_456_789,
                },
                source_text: "2026-10-25T02:30:00.123456789".into(),
            })),
            parent_id: None,
            provider_document: Some(ProviderDocument::json(r#"{"newField":true}"#)),
        };

        let encoded = serde_json::to_vec(&record).unwrap();
        let decoded: Record = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, record);
    }
}
