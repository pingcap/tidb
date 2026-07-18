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

//! Focused source-table tests for translated control builtins.

use super::{e, v};
use crate::Datum;

/// Scalar rows from `pkg/expression/builtin_control_test.go:61`
/// (`TestIf`).  Go wraps the condition with `IsTrue` and each `builtinIf*Sig`
/// then evaluates only the selected branch.  Keep the numeric-prefix string
/// rows (`1abc`, `0.1`, `0.0`) explicit: Go maps string conditions to its real
/// signature, not the value-only evaluator's `truthy_of(String)` shortcut.
/// Temporal, duration, JSON, and injected Go-error rows still need typed
/// FieldType/session or non-SQL error state and remain explicit partial
/// boundaries.
#[test]
fn if_source_vectors_use_wrapped_condition_and_lazy_branch() {
    for (expr, want) in [
        ("if(1, 1, 2)", "INT:1"),
        ("if(NULL, 1, 2)", "INT:2"),
        ("if(0, 1, 2)", "INT:2"),
        ("if('abc', 1, 2)", "INT:2"),
        ("if('1abc', 1, 2)", "INT:1"),
        ("if(1.2, 1, 2)", "INT:1"),
        ("if(0.1, 1, 2)", "INT:1"),
        ("if(0.0, 1, 2)", "INT:2"),
        ("if(0.1e0, 1, 2)", "INT:1"),
        ("if(0.0e0, 1, 2)", "INT:2"),
        ("if('0.1', 1, 2)", "INT:1"),
        ("if('0.0', 1, 2)", "INT:2"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }

    // Go's IF never evaluates an unreachable result branch.  Keep both
    // directions covered so a future eager argument materialization cannot
    // reintroduce a division-by-zero error from the unused arm.
    assert_eq!(e("if(0, 1 / 0, 2)"), "INT:2");
    assert_eq!(e("if(1, 3, 1 / 0)"), "INT:3");
}

/// Scalar SQL-value rows from `pkg/expression/builtin_control_test.go:155`
/// (`TestIfNull`).  The Go table also carries typed temporal values, JSON,
/// SET, and an injected `error` datum.  Those require FieldType/session
/// metadata or a non-SQL error object, so this source leaf deliberately keeps
/// the executable value domain explicit instead of manufacturing replacements.
#[test]
fn ifnull_source_vectors_preserve_first_non_null_value() {
    for (expr, want) in [
        ("ifnull(1, 2)", "INT:1"),
        ("ifnull(null, 2)", "INT:2"),
        ("ifnull(null, null)", "NULL"),
        ("ifnull('abc', null)", "STR:abc"),
        ("ifnull(null, 123.123)", "DEC:123.123"),
        ("ifnull(null, cast(123.123 as double))", "FLOAT:123.123"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }

    // `types.NewBinaryLiteralFromUint(0x01, -1)` is a binary datum in the Go
    // source table.  Keep the Rust assertion on the raw bytes rather than
    // relying on the label's control-character rendering.
    assert_eq!(v("ifnull(null, x'01')"), Datum::new_bytes(vec![1]));
}

/// Scalar rows from `pkg/expression/builtin_control_test.go:29`
/// (`TestCaseWhen`).  Go's `builtinCaseWhen*Sig` evaluates conditions in
/// written order, skips NULL/zero conditions, returns the first matching
/// branch, and evaluates only the selected result.  The two JSON-typed
/// source rows cannot be represented when the JSON result is selected, but
/// the row whose JSON branch is dead remains executable and proves that lazy
/// evaluation does not force that unsupported value domain.  The injected
/// `error` condition and Go FieldType-driven result promotion remain explicit
/// context/non-SQL boundaries rather than being replaced with a fabricated
/// datum.
#[test]
fn case_when_source_vectors_preserve_lazy_truthiness() {
    for (expr, want) in [
        ("case when true then 1 when true then 2 else 3 end", "INT:1"),
        (
            "case when false then 1 when true then 2 else 3 end",
            "INT:2",
        ),
        ("case when null then 1 when true then 2 else 3 end", "INT:2"),
        (
            "case when false then 1 when false then 2 else 3 end",
            "INT:3",
        ),
        ("case when null then 1 when null then 2 else 3 end", "INT:3"),
        (
            "case when false then 1 when null then 2 else 3 end",
            "INT:3",
        ),
        (
            "case when null then 1 when false then 2 else 3 end",
            "INT:3",
        ),
        ("case when 0.1 then 1 else 2 end", "INT:1"),
        ("case when 0.0 then 1 when 0.1 then 2 else 3 end", "INT:2"),
        // The Go row is `{0, jsonInt, nil}`.  Its JSON result is unreachable,
        // so a JSON cast may remain syntactically present without entering
        // this seed's unsupported JSON value domain.
        ("case when 0 then cast(3 as json) else null end", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }

    // The first Go JSON row (`{1, jsonInt, nil}`) selects its JSON result and
    // therefore stays PARTIAL until `Datum` grows a JSON value variant.
    assert!(e("case when 1 then cast(3 as json) else null end").starts_with("Unsupported"));

    // Only the taken branch is evaluated: an unreachable error-producing
    // expression must not affect the scalar result.
    assert_eq!(e("case when false then 1 / 0 else 3 end"), "INT:3");
}

/// Scalar rows from `pkg/expression/builtin_compare_test.go:174`
/// (`TestCoalesce`) that this evaluator can represent without Go's
/// FieldType-driven result promotion.  The source table's temporal rows need
/// typed time/duration datums, while its mixed `int`/`decimal` row expects a
/// statically promoted decimal result; both remain explicit partial coverage
/// until the expression context carries that metadata.
#[test]
fn coalesce_source_vectors_preserve_first_non_null_value() {
    for (expr, want) in [
        ("coalesce(NULL)", "NULL"),
        ("coalesce(NULL, NULL)", "NULL"),
        ("coalesce(NULL, NULL, NULL)", "NULL"),
        ("coalesce(NULL, 1)", "INT:1"),
        ("coalesce(NULL, CAST(1.1 AS DOUBLE))", "FLOAT:1.1"),
        ("coalesce(NULL, 123.456)", "DEC:123.456"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}
