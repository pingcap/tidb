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

//! Source-backed tests for the session option-value helpers.

use tidb_exec::option_values::{
    bool_to_on_off, on_off_to_true_false, tidb_opt_on, true_false_to_on_off, OFF, ON,
};

#[test]
fn tidb_option_on_accepts_only_on_or_one() {
    // Source: pkg/sessionctx/variable/varsutil_test.go:33-54 and
    // pkg/sessionctx/variable/varsutil.go:183-186.
    for value in ["ON", "on", "On", "1"] {
        assert!(tidb_opt_on(value), "{value}");
    }
    for value in ["off", "No", "0", "1.1", "", "true"] {
        assert!(!tidb_opt_on(value), "{value}");
    }
}

#[test]
fn boolean_and_table_text_conversions_preserve_source_spellings() {
    // Source: pkg/sessionctx/variable/varsutil.go:42-48, 148-168 and
    // pkg/sessionctx/variable/varsutil_test.go:704-718.
    assert_eq!(bool_to_on_off(true), ON);
    assert_eq!(bool_to_on_off(false), OFF);
    assert_eq!(true_false_to_on_off("TRUE"), ON);
    assert_eq!(true_false_to_on_off("TRue"), ON);
    assert_eq!(true_false_to_on_off("true"), ON);
    assert_eq!(true_false_to_on_off("FALSE"), OFF);
    assert_eq!(true_false_to_on_off("False"), OFF);
    assert_eq!(true_false_to_on_off("false"), OFF);
    assert_eq!(true_false_to_on_off("other"), "other");
    assert_eq!(on_off_to_true_false("ON"), "true");
    assert_eq!(on_off_to_true_false("on"), "true");
    assert_eq!(on_off_to_true_false("On"), "true");
    assert_eq!(on_off_to_true_false("OFF"), "false");
    assert_eq!(on_off_to_true_false("Off"), "false");
    assert_eq!(on_off_to_true_false("off"), "false");
    assert_eq!(on_off_to_true_false("other"), "other");
}
