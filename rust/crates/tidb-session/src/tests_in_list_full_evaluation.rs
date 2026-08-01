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

//! `IN` evaluates its WHOLE list, not just up to the first match.
//!
//! # The signature
//!
//! ```text
//! create table t0(c1 blob);
//! insert into t0 values ('gO'), ('W');
//! select hex(t0.c1) from t0 where 0 in (select t0.c1 from t0);
//! ```
//!
//! TiDB records FOUR `1292 Truncated incorrect DOUBLE value` warnings for
//! that statement; this tier recorded TWO. Four is two outer rows times two
//! list values. Two is two outer rows times ONE value -- the probe stopped at
//! `'gO'`, which coerces to `0.0`, matches the `0` on the left, and settled
//! the boolean. `'W'` was never coerced at all.
//!
//! # Why the count is the semantics, not a diagnostics nit
//!
//! Short-circuiting an `IN` probe on the first match is only equivalent to
//! evaluating the rest if evaluating the rest has no observable effect. In
//! this tier the string-versus-number coercion lives INSIDE the element
//! comparison (`tidb_expr::ops::eval_binary_full`), so a skipped comparison
//! skips its coercion, and a coercion is observable twice over: it appends a
//! warning, and it can fail. A skipped failure is the serious one -- the
//! statement returns rows where it should have raised.
//!
//! # The Go that decides it
//!
//! Two `in` implementations exist in Go and they disagree, so the one real
//! execution uses is the one that matters. The vectorized
//! `builtinInRealSig.vecEvalInt`
//! (`pkg/expression/builtin_other_vec_generated.go`:351-372):
//!
//! ```text
//! for j := 0; j < len(args); j++ {
//!     if err := args[j].VecEvalReal(ctx, input, buf1); err != nil {
//!         return err
//!     }
//!     args1 := buf1.Float64s()
//!     buf1.MergeNulls(buf0)
//!     for i := 0; i < n; i++ {
//!         if r64s[i] != 0 {
//!             continue
//!         }
//! ```
//!
//! `args[j]` IS the coercion: `inFunctionClass.getFunction`
//! (`pkg/expression/builtin_other.go`:89-110) sets `argTps[i] =
//! args[0]...EvalType()` for every `i`, and `newBaseBuiltinFuncWithTp`
//! (`pkg/expression/builtin.go`:199-223) wraps each arg accordingly --
//! `WrapWithCastAsReal` for `ETReal`. So `args[j].VecEvalReal` runs the
//! `cast(... as double)` -- and it runs for every `j`, unconditionally, with
//! its error returned for the whole batch. `if r64s[i] != 0 { continue }`
//! skips only the COMPARISON of a row that already matched, never the
//! evaluation.
//!
//! The scalar `builtinInRealSig.evalInt` (`pkg/expression/builtin_other.go`
//! :510-541) does `return 1, false, nil` from inside its loop, so it does
//! short-circuit -- but it is the non-vectorized fallback, and the warning
//! count a client observes is the vectorized path's. The constant-list
//! `buildHashMapForConstArgs` (:212-262) also evaluates every `ConstStrict`
//! element unconditionally, because a hash set has to be fully populated.
//! Both of Go's eager paths agree: evaluate everything.

use super::Session;
use crate::tests_support::row_text;

/// The reported table, verbatim.
fn blob_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table t0(c1 blob)",
        "insert into t0 values ('gO'), ('W')",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    session
}

fn warning_texts(session: &Session) -> Vec<String> {
    session
        .warnings()
        .iter()
        .map(|w| format!("{} {}", w.code, w.message))
        .collect()
}

/// The reported statement: four warnings, one per (outer row, list value)
/// pair, because `'W'` is coerced even after `'gO'` has already matched.
///
/// UNRUN: written on a machine that cannot execute a freshly built binary.
#[test]
fn in_subquery_probe_coerces_every_value_not_just_up_to_the_match() {
    let mut session = blob_session();
    let sql = "select hex(t0.c1) from t0 where 0 in (select t0.c1 from t0)";
    assert_eq!(
        row_text(session.run(sql)),
        vec![vec!["674F".to_owned()], vec!["57".to_owned()]],
        "{sql}"
    );
    let texts = warning_texts(&session);
    assert_eq!(
        texts,
        vec![
            "1292 Truncated incorrect DOUBLE value: 'gO'".to_owned(),
            "1292 Truncated incorrect DOUBLE value: 'W'".to_owned(),
            "1292 Truncated incorrect DOUBLE value: 'gO'".to_owned(),
            "1292 Truncated incorrect DOUBLE value: 'W'".to_owned(),
        ],
        "TiDB records four; two means the probe stopped at the first match"
    );
    // The second channel. `SHOW WARNINGS` and the OK/EOF count are genuinely
    // independent here -- a fix validated through one alone is how a warning
    // deficit stays invisible.
    assert_eq!(
        session.wire_warning_count(),
        4,
        "wire count disagrees with the buffer for {sql}"
    );
}

/// `SHOW WARNINGS` is the other channel, read as rows.
///
/// UNRUN.
#[test]
fn in_subquery_probe_show_warnings_lists_all_four() {
    let mut session = blob_session();
    let sql = "select hex(t0.c1) from t0 where 0 in (select t0.c1 from t0)";
    let _ = session.run(sql);
    assert_eq!(
        row_text(session.run("show warnings")),
        vec![
            vec![
                "Warning".to_owned(),
                "1292".to_owned(),
                "Truncated incorrect DOUBLE value: 'gO'".to_owned()
            ],
            vec![
                "Warning".to_owned(),
                "1292".to_owned(),
                "Truncated incorrect DOUBLE value: 'W'".to_owned()
            ],
            vec![
                "Warning".to_owned(),
                "1292".to_owned(),
                "Truncated incorrect DOUBLE value: 'gO'".to_owned()
            ],
            vec![
                "Warning".to_owned(),
                "1292".to_owned(),
                "Truncated incorrect DOUBLE value: 'W'".to_owned()
            ],
        ]
    );
}

/// The literal-list form of the same thing -- same operand shape as the
/// reported query (int left, all-string list) so the comparison type is the
/// same `ETReal`, with the coercing value placed AFTER the matching one so
/// only full evaluation can reach it.
///
/// `'0'` is a complete valid float prefix and coerces silently; `'abc'` is
/// what a short-circuit drops.
///
/// PREDICTION, not an oracle recording: the four-warning count of the
/// reported query is recorded, this shape is not. UNRUN.
#[test]
fn literal_in_list_coerces_values_after_the_match() {
    let mut session = Session::new();
    let sql = "select 0 in ('0', 'abc')";
    assert_eq!(
        row_text(session.run(sql)),
        vec![vec!["1".to_owned()]],
        "{sql}"
    );
    assert_eq!(
        warning_texts(&session),
        vec!["1292 Truncated incorrect DOUBLE value: 'abc'".to_owned()],
        "the value after the match must still be coerced"
    );
    assert_eq!(session.wire_warning_count(), 1);
}

/// `NOT IN` takes the same path -- the negation is applied to the settled
/// boolean, not used to skip evaluation.
///
/// PREDICTION. UNRUN.
#[test]
fn literal_not_in_list_coerces_values_after_the_match() {
    let mut session = Session::new();
    let sql = "select 0 not in ('0', 'abc')";
    assert_eq!(
        row_text(session.run(sql)),
        vec![vec!["0".to_owned()]],
        "{sql}"
    );
    assert_eq!(
        warning_texts(&session),
        vec!["1292 Truncated incorrect DOUBLE value: 'abc'".to_owned()],
    );
}

/// A match still outranks a NULL, exactly as the short-circuiting form had
/// it: it returned TRUE from inside the loop even when an earlier item had
/// already seen NULL. Folding the whole list changes WHICH items are
/// evaluated, never the boolean.
///
/// UNRUN.
#[test]
fn full_evaluation_keeps_match_outranking_null() {
    let mut session = Session::new();
    for (sql, expected) in [
        ("select 1 in (null, 1)", "1"),
        ("select 1 in (1, null)", "1"),
        ("select 2 in (1, null)", "NULL"),
        ("select 2 not in (1, null)", "NULL"),
        ("select 2 in (1, 3)", "0"),
        ("select null in (1)", "NULL"),
    ] {
        assert_eq!(
            row_text(session.run(sql)),
            vec![vec![expected.to_owned()]],
            "{sql}"
        );
    }
}

/// Two coercing values after the match: both are still coerced, so the count
/// is two, not one. A count of one is the short-circuit surviving.
///
/// PREDICTION. UNRUN.
#[test]
fn every_coercing_value_after_the_match_warns_once() {
    let mut session = Session::new();
    let sql = "select 0 in ('0', 'x', 'y')";
    assert_eq!(
        row_text(session.run(sql)),
        vec![vec!["1".to_owned()]],
        "{sql}"
    );
    assert_eq!(
        warning_texts(&session),
        vec![
            "1292 Truncated incorrect DOUBLE value: 'x'".to_owned(),
            "1292 Truncated incorrect DOUBLE value: 'y'".to_owned(),
        ]
    );
    assert_eq!(session.wire_warning_count(), 2);
}
