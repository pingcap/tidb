// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete semantic translation of `pkg/parser/duration` and its live parser
//! consumers.

use tidb_parser::{parse, parse_config_duration, ConfigDurationError};

const MINUTE_NANOS: i64 = 60_000_000_000;
const HOUR_NANOS: i64 = 60 * MINUTE_NANOS;
const DAY_NANOS: i64 = 24 * HOUR_NANOS;

/// Go: `pkg/parser/duration/duration_test.go::TestParseDuration`.
#[test]
fn test_parse_duration() {
    for (source, expected) in [
        ("1h", HOUR_NANOS),
        ("1h100m", HOUR_NANOS + 100 * MINUTE_NANOS),
        ("1d10000m", DAY_NANOS + 10_000 * MINUTE_NANOS),
        ("1d100h", DAY_NANOS + 100 * HOUR_NANOS),
        ("1.5d", 36 * HOUR_NANOS),
        ("1d1.5h", DAY_NANOS + HOUR_NANOS + 30 * MINUTE_NANOS),
        (
            "1d3.555h",
            DAY_NANOS + (3.555 * HOUR_NANOS as f64) as i64,
        ),
    ] {
        assert_eq!(parse_config_duration(source).unwrap(), expected, "{source}");
    }
}

#[test]
fn malformed_components_fail_without_go_runtime_emulation() {
    assert_eq!(parse_config_duration("0").unwrap(), 0);
    assert_eq!(parse_config_duration("").unwrap(), 0);
    assert_eq!(
        parse_config_duration("1s").unwrap_err(),
        ConfigDurationError::UnknownUnit('s')
    );
    for source in ["h", "1", "1..2h", ".h", "٢h", "²h", " 1h", "1h "] {
        assert!(parse_config_duration(source).is_err(), "{source:?}");
    }
    assert_eq!(
        parse_config_duration(".h").unwrap_err().to_string(),
        "strconv.ParseFloat: parsing \".\": invalid syntax"
    );
    let huge = "9".repeat(400);
    assert_eq!(
        parse_config_duration(&format!("{huge}h"))
            .unwrap_err()
            .to_string(),
        format!("strconv.ParseFloat: parsing {huge:?}: value out of range")
    );
}

#[test]
fn sql_consumers_use_the_same_duration_contract() {
    for value in ["", "0", "1h", "1h100m", "1.5d", "1d3.555h"] {
        assert!(
            parse(&format!(
                "create table t (created_at datetime) TTL_JOB_INTERVAL='{value}'"
            ))
            .is_ok(),
            "TTL_JOB_INTERVAL={value:?}"
        );
        assert!(
            parse(&format!("calibrate resource duration='{value}'")).is_ok(),
            "CALIBRATE DURATION={value:?}"
        );
    }

    for value in ["h", "10", "1s", "10YEAR", " 1h", "1h ", "1..2h"] {
        assert!(
            parse(&format!(
                "create table t (created_at datetime) TTL_JOB_INTERVAL='{value}'"
            ))
            .is_err(),
            "TTL_JOB_INTERVAL={value:?}"
        );
        assert!(
            parse(&format!("calibrate resource duration='{value}'")).is_err(),
            "CALIBRATE DURATION={value:?}"
        );
    }

    let ttl_error = parse("create table t (a int) TTL_JOB_INTERVAL='.h'").unwrap_err();
    assert_eq!(
        ttl_error.message,
        "The TTL_JOB_INTERVAL option is not a valid duration: strconv.ParseFloat: parsing \".\": invalid syntax"
    );
    let calibrate_error = parse("calibrate resource duration='.h'").unwrap_err();
    assert_eq!(
        calibrate_error.message,
        "The DURATION option is not a valid duration: strconv.ParseFloat: parsing \".\": invalid syntax"
    );
}
