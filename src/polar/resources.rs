//! Documented AccessLink v3 request construction.
//!
//! This module only builds requests; callers still decide when to fetch and
//! must archive the response before decoding it.

use std::collections::BTreeMap;
use time::{Date, Duration, format_description::well_known::Iso8601};

const API_BASE: &str = "https://www.polaraccesslink.com/v3";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceRequest {
    pub resource: String,
    pub url: String,
    /// Safe query parameters only. Never put credentials or authorization here.
    pub metadata: BTreeMap<String, String>,
}

pub fn exercises(from: Date, to: Date) -> ResourceRequest {
    request(
        "exercises",
        "/exercises",
        &[
            ("from", date(from)),
            ("to", date(to)),
            ("samples", "true".into()),
            ("zones", "true".into()),
            ("route", "true".into()),
        ],
    )
}

/// Fetch one exercise by its AccessLink exercise identifier. The returned
/// summary can be used to reconcile details missing from collection pages.
pub fn exercise_detail(exercise_id: &str) -> ResourceRequest {
    request(
        "exercise-detail",
        &format!("/exercises/{}", encode_path_segment(exercise_id)),
        &[],
    )
}

pub fn exercise_export(exercise_id: &str, format: ExerciseExport) -> ResourceRequest {
    let id = encode_path_segment(exercise_id);
    let format = format.as_str();
    request(
        format!("exercise-{format}"),
        &format!("/exercises/{id}/{format}"),
        &[],
    )
}

pub fn activities(from: Date, to: Date) -> ResourceRequest {
    request(
        "activities",
        "/users/activities",
        &[
            ("from", date(from)),
            ("to", date(to)),
            ("steps", "true".into()),
            ("activity_zones", "true".into()),
            ("inactivity_stamps", "true".into()),
        ],
    )
}

pub fn continuous_heart_rate(date_value: Option<Date>) -> ResourceRequest {
    let path = match date_value {
        Some(day) => format!("/users/continuous-heart-rate/{}", date(day)),
        None => "/users/continuous-heart-rate".to_owned(),
    };
    request("continuous-heart-rate", &path, &[])
}

pub fn sleep(date_value: Option<Date>) -> ResourceRequest {
    date_or_collection("sleep", "/users/sleep", date_value)
}

/// List dates for which Polar reports sleep data.
pub fn sleep_available() -> ResourceRequest {
    request("sleep-available", "/users/sleep/available", &[])
}

pub fn nightly_recharge(date_value: Option<Date>) -> ResourceRequest {
    date_or_collection("nightly-recharge", "/users/nightly-recharge", date_value)
}

pub fn sleepwise_alertness(from: Date, to: Date) -> ResourceRequest {
    date_range_resource(
        "sleepwise-alertness",
        "/users/sleepwise/alertness",
        from,
        to,
    )
}

pub fn sleepwise_circadian_bedtime(from: Date, to: Date) -> ResourceRequest {
    date_range_resource(
        "sleepwise-circadian-bedtime",
        "/users/sleepwise/circadian-bedtime",
        from,
        to,
    )
}

pub fn cardio_load(from: Date, to: Date) -> ResourceRequest {
    date_range_resource("cardio-load", "/users/cardio-load", from, to)
}

pub fn cardio_load_history(from: Date, to: Date) -> ResourceRequest {
    date_range_resource("cardio-load-history", "/users/cardio-load/date", from, to)
}

pub fn user_profile(user_id: &str) -> ResourceRequest {
    request(
        "user-profile",
        &format!("/users/{}", encode_path_segment(user_id)),
        &[],
    )
}

pub fn physical_info() -> ResourceRequest {
    request("physical-info", "/users/physical-info", &[])
}

/// Split an inclusive date range into consecutive inclusive windows no
/// longer than `maximum_days`. AccessLink resources can use different limits;
/// callers must pass the documented limit for the specific resource.
pub fn date_windows(
    from: Date,
    to: Date,
    maximum_days: u8,
) -> Result<Vec<(Date, Date)>, PlanError> {
    if to < from {
        return Err(PlanError::ReversedRange);
    }
    if maximum_days == 0 {
        return Err(PlanError::ZeroWindow);
    }
    let width = i64::from(maximum_days) - 1;
    let mut windows = Vec::new();
    let mut start = from;
    loop {
        let end = (start + Duration::days(width)).min(to);
        windows.push((start, end));
        if end == to {
            break;
        }
        start = end + Duration::days(1);
    }
    Ok(windows)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExerciseExport {
    Fit,
    Tcx,
    Gpx,
}
impl ExerciseExport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fit => "fit",
            Self::Tcx => "tcx",
            Self::Gpx => "gpx",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanError {
    ReversedRange,
    ZeroWindow,
}
impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReversedRange => f.write_str("date range end precedes start"),
            Self::ZeroWindow => f.write_str("date window must be at least one day"),
        }
    }
}
impl std::error::Error for PlanError {}

fn date_or_collection(resource: &str, collection_path: &str, day: Option<Date>) -> ResourceRequest {
    let path = day.map_or_else(
        || collection_path.to_owned(),
        |day| format!("{collection_path}/{}", date(day)),
    );
    request(resource, &path, &[])
}

fn date_range_resource(resource: &str, path: &str, from: Date, to: Date) -> ResourceRequest {
    request(resource, path, &[("from", date(from)), ("to", date(to))])
}

fn request(resource: impl Into<String>, path: &str, query: &[(&str, String)]) -> ResourceRequest {
    let mut metadata = BTreeMap::new();
    for (name, value) in query {
        metadata.insert((*name).to_owned(), value.clone());
    }
    let query_string = query
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let url = if query_string.is_empty() {
        format!("{API_BASE}{path}")
    } else {
        format!("{API_BASE}{path}?{query_string}")
    };
    ResourceRequest {
        resource: resource.into(),
        url,
        metadata,
    }
}

fn date(value: Date) -> String {
    value
        .format(&Iso8601::DATE)
        .expect("ISO date format is infallible")
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(value: &str) -> Date {
        Date::parse(value, &Iso8601::DATE).unwrap()
    }

    #[test]
    fn requests_include_all_detail_flags_and_redacted_metadata() {
        let request = activities(day("2026-10-01"), day("2026-10-28"));
        assert_eq!(
            request.url,
            "https://www.polaraccesslink.com/v3/users/activities?from=2026-10-01&to=2026-10-28&steps=true&activity_zones=true&inactivity_stamps=true"
        );
        assert!(!request.metadata.keys().any(|key| key.contains("token")));
        let exercise = exercises(day("2026-10-01"), day("2026-10-28"));
        assert!(exercise.url.ends_with("samples=true&zones=true&route=true"));
    }

    #[test]
    fn date_windows_cover_inclusive_range_without_gaps_or_oversize() {
        let windows = date_windows(day("2026-01-01"), day("2026-02-01"), 28).unwrap();
        assert_eq!(
            windows,
            vec![
                (day("2026-01-01"), day("2026-01-28")),
                (day("2026-01-29"), day("2026-02-01")),
            ]
        );
    }

    #[test]
    fn exercise_ids_are_escaped_as_single_path_segments() {
        let request = exercise_export("a/b ?", ExerciseExport::Fit);
        assert!(request.url.ends_with("/exercises/a%2Fb%20%3F/fit"));
    }

    #[test]
    fn exercise_detail_uses_escaped_id_path() {
        let request = exercise_detail("a/b ?");
        assert_eq!(request.resource, "exercise-detail");
        assert_eq!(
            request.url,
            "https://www.polaraccesslink.com/v3/exercises/a%2Fb%20%3F"
        );
        assert!(request.metadata.is_empty());
    }

    #[test]
    fn cardio_load_history_uses_documented_date_range_endpoint() {
        let request = cardio_load_history(day("2026-10-15"), day("2026-10-21"));
        assert_eq!(request.resource, "cardio-load-history");
        assert_eq!(
            request.url,
            "https://www.polaraccesslink.com/v3/users/cardio-load/date?from=2026-10-15&to=2026-10-21"
        );
        assert_eq!(request.metadata.get("from").unwrap(), "2026-10-15");
        assert_eq!(request.metadata.get("to").unwrap(), "2026-10-21");
    }

    #[test]
    fn sleep_available_uses_documented_path() {
        let request = sleep_available();
        assert_eq!(request.resource, "sleep-available");
        assert_eq!(
            request.url,
            "https://www.polaraccesslink.com/v3/users/sleep/available"
        );
        assert!(request.metadata.is_empty());
    }
}
