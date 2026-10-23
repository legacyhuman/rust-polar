//! Bounded decoders for the AccessLink user profile and physical-info objects.
//!
//! Profile/settings values are provider data, not automatically measurements.
//! Fields whose meaning is not standardized here remain in `provider_fields`.
//! With serde_json's `arbitrary_precision` feature enabled, JSON numeric tokens
//! are retained without conversion through `f64`.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Read};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_PROFILE_DOCUMENT_BYTES: usize = 4 * 1024 * 1024;

/// User identity/profile fields returned by `GET /v3/users/{user-id}`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct UserProfile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub polar_user_id: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_id: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

/// User-provided body measurements and training settings from
/// `GET /v3/users/physical-info`. These are profile settings, not time-series
/// observations. Enum-like strings are left open for future provider values.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PhysicalInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub birthday: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gender: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_heart_rate: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resting_heart_rate: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aerobic_threshold: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anaerobic_threshold: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vo2_max: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub training_background: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_day: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_goal: Option<String>,
    #[serde(flatten)]
    pub provider_fields: BTreeMap<String, Value>,
}

pub fn decode_user_profile<R: Read>(reader: R) -> Result<UserProfile, DecodeError> {
    decode_bounded_object(reader)
}

pub fn decode_physical_info<R: Read>(reader: R) -> Result<PhysicalInfo, DecodeError> {
    decode_bounded_object(reader)
}

fn decode_bounded_object<T, R>(reader: R) -> Result<T, DecodeError>
where
    T: for<'de> Deserialize<'de>,
    R: Read,
{
    let mut bytes = Vec::with_capacity(4096);
    reader
        .take((MAX_PROFILE_DOCUMENT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(DecodeError::Io)?;
    if bytes.len() > MAX_PROFILE_DOCUMENT_BYTES {
        return Err(DecodeError::TooLarge {
            limit: MAX_PROFILE_DOCUMENT_BYTES,
        });
    }
    serde_json::from_slice(&bytes).map_err(DecodeError::Json)
}

#[derive(Debug)]
pub enum DecodeError {
    Io(io::Error),
    TooLarge { limit: usize },
    Json(serde_json::Error),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "could not read Polar profile document: {error}"),
            Self::TooLarge { limit } => {
                write!(f, "Polar profile document exceeds the {limit}-byte limit")
            }
            Self::Json(error) => write!(f, "invalid Polar profile document: {error}"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::TooLarge { .. } => None,
            Self::Json(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::path::PathBuf;

    fn fixture(name: &str) -> File {
        File::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/polar")
                .join(name),
        )
        .unwrap()
    }

    #[test]
    fn decodes_user_profile_and_preserves_unrecognized_values() {
        let profile = decode_user_profile(fixture("user_profile.json")).unwrap();
        assert_eq!(profile.polar_user_id.as_ref().unwrap(), "627139");
        assert_eq!(profile.first_name.as_deref(), Some("Example"));
        assert_eq!(
            profile.registration_date.as_deref(),
            Some("2026-10-01T10:00:00Z")
        );
        assert_eq!(profile.provider_fields["future-field"], "retained");

        let round_trip: Value =
            serde_json::from_str(&serde_json::to_string(&profile).unwrap()).unwrap();
        assert_eq!(round_trip["future-field"], "retained");
        assert_eq!(round_trip["polar-user-id"], "627139");
    }

    #[test]
    fn decodes_physical_info_without_closing_enum_values_or_losing_extras() {
        let info = decode_physical_info(fixture("physical_info.json")).unwrap();
        assert_eq!(info.weight.as_ref().unwrap().to_string(), "70.5");
        assert_eq!(info.height.as_ref().unwrap(), 175);
        assert_eq!(info.gender.as_deref(), Some("MALE"));
        assert_eq!(info.vo2_max.as_ref().unwrap(), 50);
        assert_eq!(info.training_background.as_deref(), Some("REGULAR"));
        assert_eq!(info.provider_fields["future-setting"], "kept");

        let round_trip: Value =
            serde_json::from_str(&serde_json::to_string(&info).unwrap()).unwrap();
        assert_eq!(round_trip["future-setting"], "kept");
        assert_eq!(round_trip["aerobic_threshold"], 140);
    }

    #[test]
    fn bounded_decoder_rejects_oversized_and_non_object_documents() {
        let oversized = vec![b' '; MAX_PROFILE_DOCUMENT_BYTES + 1];
        assert!(matches!(
            decode_user_profile(&oversized[..]),
            Err(DecodeError::TooLarge { .. })
        ));
        assert!(matches!(
            decode_physical_info(&br#"[{"weight":70}]"#[..]),
            Err(DecodeError::Json(_))
        ));
    }
}
