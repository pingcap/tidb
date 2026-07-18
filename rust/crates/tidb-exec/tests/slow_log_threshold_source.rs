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

//! Source-backed tests for typed slow-log threshold helpers.

use tidb_exec::slow_log_threshold::{
    matches_equal, matches_greater_equal, matches_zero, uint64_from_non_negative, SlowLogValue,
};

#[test]
fn slow_log_special_type_helpers_preserve_source_matching() {
    // Source: pkg/sessionctx/variable/slow_log.go:682-714 and
    // pkg/sessionctx/variable/tests/slowlog/slow_log_test.go:83-145.
    assert!(matches_equal(
        &SlowLogValue::Boolean(true),
        &SlowLogValue::Boolean(true)
    ));
    assert!(!matches_equal(
        &SlowLogValue::Boolean(true),
        &SlowLogValue::Text("true".to_owned())
    ));
    assert!(matches_equal(
        &SlowLogValue::Text("db_test".to_owned()),
        &SlowLogValue::Text("db_test".to_owned())
    ));
    assert!(!matches_equal(
        &SlowLogValue::Text("db_test".to_owned()),
        &SlowLogValue::Text("DB_TEST".to_owned())
    ));

    for zero in [
        SlowLogValue::Signed(0),
        SlowLogValue::Unsigned(0),
        SlowLogValue::Float(0.0),
    ] {
        assert!(matches_zero(&zero));
    }
    assert!(!matches_zero(&SlowLogValue::Boolean(false)));
    assert!(!matches_zero(&SlowLogValue::Text("0".to_owned())));
}

#[test]
fn slow_log_unsigned_fields_reject_negative_values() {
    // Source: pkg/sessionctx/variable/slow_log.go:688-699, 972-983 and
    // pkg/sessionctx/variable/tests/slowlog/slow_log_test.go:320-379.
    assert_eq!(uint64_from_non_negative(-1), None);
    assert_eq!(uint64_from_non_negative(0), Some(0));
    assert_eq!(uint64_from_non_negative(2), Some(2));

    assert!(matches_greater_equal(
        &SlowLogValue::Unsigned(1),
        &SlowLogValue::Unsigned(2)
    ));
    assert!(!matches_greater_equal(
        &SlowLogValue::Unsigned(3),
        &SlowLogValue::Unsigned(2)
    ));
    assert!(!matches_greater_equal(
        &SlowLogValue::Signed(1),
        &SlowLogValue::Unsigned(2)
    ));
}
