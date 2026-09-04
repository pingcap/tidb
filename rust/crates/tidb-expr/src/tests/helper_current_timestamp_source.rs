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

//! GO PORT of `pkg/expression/helper_test.go::TestCurrentTimestampTimeZone`
//! (`helper_test.go:157`) and the deterministic `NOW()` rows of
//! `pkg/expression/integration_test/integration_test.go::TestTimestamp`
//! (`integration_test.go:2585`) (batch part10).
//!
//! Go seeds the session `"timestamp"` sysvar with a fixed UTC second count
//! (`helper.go:199` `getStmtTimestamp`) and re-renders CURRENT_TIMESTAMP in
//! the statement zone (`helper.go:73` `getTimeCurrentTimeStamp`). The Rust
//! evaluator carries that composition through its [`Columns::now`] clock and
//! [`ColumnResolver::time_zone`]; this port pins both master rows:
//!
//! - `timestamp=1234`, `time_zone=+00:00` → `1970-01-01 00:20:34`;
//! - `timestamp=1234`, `time_zone=+08:00` → `1970-01-01 08:20:34`
//!   (helper_test.go:169-180), i.e. changing ONLY the timezone changes the
//!   value, because the sysvar instant itself is UTC.
//!
//! The clock stubs make those rows exactly reproducible.

use super::*;
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_datatype::{FieldType, SessionTimeZone};

/// A session whose `timestamp` sysvar reads 1234 seconds since epoch. Go's
/// `sessionVars.StmtCtx.SetTimeZone(sessionVars.Location())` is what selects
/// which fixed offset renders it; the stub takes that offset directly.
struct TimestampSysVar {
    offset_secs: i32,
}

impl Columns for TimestampSysVar {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    /// helper.go:199: the seeded UTC instant of the `timestamp` variable.
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((1234, 0, self.offset_secs))
    }
}

struct Zone {
    inner: TimestampSysVar,
}

impl Columns for Zone {
    fn get(&self, path: &[String]) -> Option<Datum> {
        self.inner.get(path)
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.inner.now()
    }
}

impl ColumnResolver for Zone {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let _ = path;
        None
    }

    /// helper.go:174/179 -- `StmtCtx.SetTimeZone(sessionVars.Location())`.
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::Fixed {
            name: if self.inner.offset_secs == 0 {
                "+00:00".to_owned()
            } else {
                format!("{:+03}:{:02}", self.inner.offset_secs / 3600, 0)
            },
            offset_secs: self.inner.offset_secs,
        }
    }
}

/// Evaluates one select-field expression over the stubbed session.
fn eval_now(offset_secs: i32) -> String {
    let ctx = Zone {
        inner: TimestampSysVar { offset_secs },
    };
    let stmt = tidb_parser::parse("select now()").expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("no expr")
    };
    let rewritten = rewrite_expr_resolved(expr, &ctx).expect("rewrite");
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    rewritten
        .eval(&ctx, chunk.get_row(0))
        .expect("eval")
        .sql_string()
        .expect("text")
}

#[test]
fn test_current_timestamp_time_zone_case_table() {
    // helper_test.go:161-181 (TestCurrentTimestampTimeZone): exact rows for
    // +00:00 then +08:00 over timestamp=1234.
    assert_eq!(eval_now(0), "1970-01-01 00:20:34");
    assert_eq!(eval_now(8 * 3600), "1970-01-01 08:20:34");
}

#[test]
fn test_timestamp_sysvar_renders_fixed_now_literal_rows() {
    // integration_test.go:2604-2614 (`set @@timestamp = 12345; ... SELECT
    // NOW();`) with `time_zone = '+00:00'`: every repeat of the query inside
    // one statement context prints the same rendered value. 12345 s past the
    // epoch is 03:25:45 UTC.
    // (The Go test re-checks `@@timestamp` round-trips and post-DEFAULT
    // progressions; those halves are session-variable plumbing outside this
    // crate.)
    struct Sys {
        offset_secs: i32,
    }
    impl Columns for Sys {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((12_345, 0, self.offset_secs))
        }
    }
    struct ZeroZone;
    impl ColumnResolver for ZeroZone {
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let _ = path;
            None
        }
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
    }

    let ctx = Sys { offset_secs: 0 };
    let stmt = tidb_parser::parse("select now()").expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("no expr")
    };
    let rewritten = rewrite_expr_resolved(expr, &ZeroZone).expect("rewrite");
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    // Two executions within one statement context agree (Go asserts the pair
    // of SELECT NOW() calls both read 1970-01-01 03:25:45).
    for _ in 0..2 {
        let rendered = rewritten
            .eval(&ctx, chunk.get_row(0))
            .expect("eval")
            .sql_string()
            .expect("text");
        assert_eq!(rendered, "1970-01-01 03:25:45");
    }
}

// ---------------------------------------------------------------------------
// go-parity-gap carriers (Go tests without any Rust-side counterpart yet)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/helper_test.go:32 TestGetTimeValue`.
///
/// go-parity-gap: `expression.GetTimeValue(ctx BuildContext, v any, tp byte,
/// fsp int, explicitTz *time.Location)` (`helper.go:90`) is a BUILD-context
/// helper (AST value/sentinel → temporal datum, incl. the `ast.CurrentTimestamp`
/// sentinel and the `timestamp` sysvar read). No Rust symbol carries that
/// dispatch surface yet; approximating it via the constant evaluator would pin
/// different code than the Go test exercises.
#[test]
#[ignore = "go-parity-gap: helper.go GetTimeValue build-context helper has no Rust carrier"]
fn test_get_time_value_build_context_helper() {}

/// Go `pkg/expression/helper_test.go:127 TestIsCurrentTimestampExpr`.
#[test]
fn test_is_current_timestamp_expr_predicate() {
    use tidb_datatype::{FieldType, FieldTypeCode};

    let current_timestamp = |args: Vec<tidb_ast::Expr>| tidb_ast::Expr::Func {
        name: "CURRENT_TIMESTAMP".to_owned(),
        args,
        origin_position: 0,
    };
    let int = |value: &str| tidb_ast::Expr::Int(value.to_owned());

    // helper_test.go:136-139: non-function values fail, while a bare
    // CURRENT_TIMESTAMP is valid without a destination FSP.
    assert!(!is_valid_current_timestamp_expr(
        &tidb_ast::Expr::String("abc".to_owned()),
        None,
    ));
    assert!(is_valid_current_timestamp_expr(
        &current_timestamp(vec![]),
        None
    ));
    // Go treats the negative unspecified-decimal sentinel like FSP 0 for a
    // bare call (`GetDecimal() > 0` is the only precision test).
    let unspecified = FieldType::new(FieldTypeCode::Timestamp).with_decimal(-1);
    assert!(is_valid_current_timestamp_expr(
        &current_timestamp(vec![]),
        Some(&unspecified),
    ));

    let fsp3 = FieldType::new(FieldTypeCode::Timestamp).with_decimal(3);
    assert!(is_valid_current_timestamp_expr(
        &current_timestamp(vec![int("3")]),
        Some(&fsp3),
    ));
    assert!(!is_valid_current_timestamp_expr(
        &current_timestamp(vec![int("1")]),
        Some(&fsp3),
    ));
    assert!(!is_valid_current_timestamp_expr(
        &current_timestamp(vec![]),
        Some(&fsp3),
    ));

    let fsp0 = FieldType::new(FieldTypeCode::Timestamp);
    assert!(!is_valid_current_timestamp_expr(
        &current_timestamp(vec![int("2")]),
        Some(&fsp0),
    ));
    assert!(!is_valid_current_timestamp_expr(
        &current_timestamp(vec![int("2")]),
        None,
    ));

    // The Go helper reads only Args[0], so a matching first argument remains
    // valid even if a malformed extra argument is present.
    assert!(is_valid_current_timestamp_expr(
        &current_timestamp(vec![int("3"), tidb_ast::Expr::String("ignored".to_owned())]),
        Some(&fsp3),
    ));
    assert!(!is_valid_current_timestamp_expr(
        &current_timestamp(vec![tidb_ast::Expr::Unary(
            tidb_ast::UnaryOp::Minus,
            Box::new(int("1")),
        )]),
        Some(&fsp3),
    ));
}
