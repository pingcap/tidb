// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `util_test.go`.

use super::util::{go_duration_string, parse_go_duration, Duration};

/// Go's `example` struct: `Interval Duration` tagged `json:"interval"`.
#[derive(Default, serde_derive::Deserialize, serde_derive::Serialize)]
struct Example {
    interval: Duration,
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_util_test_TestDurationJSON() {
    let text = r#"{"interval":"1h1m1s"}"#;
    let example: Example = serde_json::from_str(text).unwrap();
    assert_eq!(example.interval.seconds(), (60 * 60 + 60 + 1) as f64);

    let b = serde_json::to_string(&example).unwrap();
    assert_eq!(b, text);

    // The Go Marshaler/Unmarshaler pair agrees with the serde form.
    let mut duration = Duration::default();
    duration.unmarshal_json(br#""1h1m1s""#).unwrap();
    assert_eq!(duration, example.interval);
    assert_eq!(duration.marshal_json(), r#""1h1m1s""#);
}

/// The TOML decoder calls `UnmarshalText` with the string value; no TOML
/// decoder is a dependency here, so the test drives `unmarshal_text` with the
/// value of `interval = "1h1m1s"`.
#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_util_test_TestDurationTOML() {
    let mut example = Example::default();
    example.interval.unmarshal_text(b"1h1m1s").unwrap();
    assert_eq!(example.interval.seconds(), (60 * 60 + 60 + 1) as f64);
    assert_eq!(example.interval.marshal_text(), "1h1m1s");
}

#[test]
fn go_duration_text_round_trips() {
    for (text, nanos) in [
        ("0s", 0),
        ("1ns", 1),
        ("1.5µs", 1_500),
        ("1.234µs", 1_234),
        ("50ms", 50_000_000),
        ("4s", 4_000_000_000),
        ("1h1m1s", 3_661_000_000_000),
        ("-1.5s", -1_500_000_000),
        ("2562047h47m16.854775807s", i64::MAX),
    ] {
        assert_eq!(go_duration_string(nanos), text);
        assert_eq!(parse_go_duration(text), Ok(nanos), "{text}");
    }
    assert_eq!(parse_go_duration("1.5h"), Ok(5_400_000_000_000));
    assert_eq!(parse_go_duration("1us"), Ok(1_000));
    assert_eq!(parse_go_duration("+5m"), Ok(300_000_000_000));
    assert!(parse_go_duration("").is_err());
    assert!(parse_go_duration("1").is_err());
    assert!(parse_go_duration("1x").is_err());
    assert!(parse_go_duration(".s").is_err());
    assert!(parse_go_duration("9223372036854775808ns").is_err());
}
