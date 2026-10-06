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

//! Dependency-closed vectors for `pkg/planner/util/fixcontrol/set.go`.
//!
//! The direct Go test anchor is `TestParseToMapEmptyValue` at
//! `pkg/planner/util/fixcontrol/fixcontrol_test.go:92`.

use std::collections::BTreeMap;

use tidb_planner::fix_control::{
    parse_to_map, OptimizerFixControl, ParseError, FIX_33031, FIX_43817, FIX_44262,
    FIX_44389, FIX_44823, FIX_44830, FIX_44855, FIX_45132, FIX_45798, FIX_45822,
    FIX_46177, FIX_47400, FIX_49736, FIX_52592, FIX_52869, FIX_54337, FIX_56318,
};

#[test]
fn parse_to_map_preserves_empty_value_contract() {
    let parsed = parse_to_map("123:").expect("empty values are valid");
    assert_eq!(parsed.values, BTreeMap::from([(123, String::new())]));
    assert!(parsed.warnings.is_empty());
}

#[test]
fn parse_to_map_handles_quotes_whitespace_and_duplicate_warnings() {
    let parsed = parse_to_map("  100: 'on', 100:1, 200: OFF  ").expect("valid assignments");
    assert_eq!(
        parsed.values,
        BTreeMap::from([(100, "1".to_owned()), (200, "OFF".to_owned())])
    );
    assert_eq!(
        parsed.warnings,
        vec![
            "repeated assignment for fix control: 100. existing value: \"on\". new value: \"1\"."
                .to_owned()
        ]
    );
}

#[test]
fn parse_to_map_reports_source_boundary_errors() {
    assert_eq!(parse_to_map("123"), Err(ParseError::MissingColon));
    assert_eq!(parse_to_map("   "), Err(ParseError::MissingColon));
    assert_eq!(
        parse_to_map("abc:value")
            .expect_err("a non-decimal fix number is rejected")
            .to_string(),
        "strconv.ParseUint: parsing \"abc\": invalid syntax"
    );
    assert_eq!(
        parse_to_map("123:'unterminated"),
        Err(ParseError::MissingQuote)
    );
}

#[test]
fn source_declares_the_complete_issue_number_catalog() {
    assert_eq!(
        [
            FIX_52592, FIX_33031, FIX_43817, FIX_44262, FIX_44389, FIX_44830, FIX_44823,
            FIX_44855, FIX_45132, FIX_45822, FIX_45798, FIX_46177, FIX_47400, FIX_49736,
            FIX_52869, FIX_54337, FIX_56318,
        ],
        [
            52592, 33031, 43817, 44262, 44389, 44830, 44823, 44855, 45132, 45822,
            45798, 46177, 47400, 49736, 52869, 54337, 56318,
        ]
    );
}

#[test]
fn typed_getters_preserve_presence_and_use_defaults_only_when_needed() {
    let controls = OptimizerFixControl::from(BTreeMap::from([
        (100, "ON".to_owned()),
        (101, "1".to_owned()),
        (102, "off".to_owned()),
        (103, "-10".to_owned()),
        (104, "55.5".to_owned()),
        (105, "not-a-number".to_owned()),
        (106, "0x1p2".to_owned()),
        (107, "+Inf".to_owned()),
        (108, "-Inf".to_owned()),
        (109, "NaN".to_owned()),
    ]));

    assert_eq!(controls.get_str(100), Some("ON"));
    assert_eq!(controls.get_str_with_default(999, "default"), "default");
    assert_eq!(controls.get_bool(100), Some(true));
    assert_eq!(controls.get_bool(101), Some(true));
    assert_eq!(controls.get_bool(102), Some(false));
    assert!(controls.get_bool_with_default(999, true));
    assert_eq!(controls.get_int(103), (-10, true, None));
    assert!(controls.get_int(104).2.is_some());
    assert_eq!(controls.get_int_with_default(104, 12345), 12345);
    assert_eq!(controls.get_float(104), (55.5, true, None));
    assert!(controls.get_float(105).2.is_some());
    assert_eq!(controls.get_float_with_default(105, 1234.5), 1234.5);
    assert_eq!(controls.get_float(106), (4.0, true, None));
    assert_eq!(controls.get_float(107), (f64::INFINITY, true, None));
    assert_eq!(controls.get_float(108), (f64::NEG_INFINITY, true, None));
    let nan = controls.get_float(109);
    assert!(nan.0.is_nan() && nan.1 && nan.2.is_none());
    assert_eq!(controls.get_int(999), (0, false, None));
    assert_eq!(controls.get_float(999), (0.0, false, None));
}
