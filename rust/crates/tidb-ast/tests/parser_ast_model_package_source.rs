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

//! Ports of `pkg/parser/ast/model_test.go` (origin/master): the `CIStr`
//! constructors, accessors, and JSON round-trip contract.

use tidb_ast::CiString;

/// `pkg/parser/ast/model_test.go::TestT`.
#[test]
fn t() {
    let abc = CiString::new("aBC");
    assert_eq!(abc.original(), "aBC");
    assert_eq!(abc.lowercase(), "abc");
    // Go's String() prints O — the Display impl preserves that.
    assert_eq!(format!("{abc}"), "aBC");
}

/// `pkg/parser/ast/model_test.go::TestUnmarshalCIStr`.
///
/// Go's `CIStr.UnmarshalJSON` accepts BOTH a bare JSON string (folding the
/// lowercase key itself) and the `{"O": .., "L": ..}` object form; its
/// `MarshalJSON` emits exactly `{"O":..,"L":..}` in that field order. The
/// serde impls on [`CiString`] transcreate both directions.
#[test]
fn unmarshal_ci_str() {
    let original = "aaBB";

    // Unmarshal from a single string.
    let buffer = serde_json::to_string(original).expect("marshal string");
    let ci: CiString = serde_json::from_str(&buffer).expect("unmarshal string form");
    assert_eq!(ci.original(), original);
    assert_eq!(ci.lowercase(), "aabb");

    // Marshal back and confirm the compact two-field object spelling.
    let round_tripped = serde_json::to_string(&ci).expect("marshal CIStr");
    assert_eq!(round_tripped, r#"{"O":"aaBB","L":"aabb"}"#);

    // Re-unmarshalling the object form keeps both fields as written.
    let ci_again: CiString = serde_json::from_str(&round_tripped).expect("unmarshal object form");
    assert_eq!(ci_again.original(), original);
    assert_eq!(ci_again.lowercase(), "aabb");
}
