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

use tidb_planner::fix_control::{parse_to_map, ParseError};

#[test]
fn parse_to_map_preserves_empty_value_contract() {
    let parsed = parse_to_map("123:").expect("empty values are valid");
    assert_eq!(parsed.values, BTreeMap::from([(123, String::new())]));
    assert!(parsed.warnings.is_empty());
}

#[test]
fn parse_to_map_handles_quotes_whitespace_and_duplicate_warnings() {
    let parsed = parse_to_map("  +100: 'on', 100:1, 200: OFF  ").expect("valid assignments");
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
    assert_eq!(parse_to_map("abc:value"), Err(ParseError::InvalidKey));
    assert_eq!(
        parse_to_map("123:'unterminated"),
        Err(ParseError::MissingQuote)
    );
}
