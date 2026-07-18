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

//! Source-backed tests for integer SET CONFIG JSON rendering.

use tidb_exec::config_int_json::integer_config_json;

#[test]
fn integer_config_json_matches_source_boolean_and_numeric_branches() {
    // Source: pkg/executor/set_config.go:187-235.
    // Direct Go coverage: pkg/executor/set_test.go:1709
    // (TestSetClusterConfigJSONData), cases at 1719-1721.
    assert_eq!(integer_config_json("k", 1, true), r#"{"k":true}"#);
    assert_eq!(integer_config_json("k", 0, true), r#"{"k":false}"#);
    assert_eq!(integer_config_json("k", 2333, false), r#"{"k":2333}"#);
}

#[test]
fn integer_config_json_preserves_nonzero_and_signed_decimal_behavior() {
    assert_eq!(integer_config_json("k", -1, true), r#"{"k":true}"#);
    assert_eq!(integer_config_json("k", -2333, false), r#"{"k":-2333}"#);
    // Key escaping and validation are intentionally external, matching the
    // source's direct fmt.Sprintf interpolation boundary.
    assert_eq!(integer_config_json("a\"b", 1, false), r#"{"a"b":1}"#);
}
