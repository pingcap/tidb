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

//! GO PORT of `pkg/expression/integration_test/integration_test.go:1446`
//! `TestGetLock` (batch part10) -- the per-call semantics of the advisory-lock
//! builtins.
//!
//! The Go harness drives a real unistore-backed session whose `SessionVars`
//! owns the lock map (`pkg/executor/simple.go`, `func (s *session)` lock
//! helpers behind `getLock`/`releaseLock`). The Rust evaluator carries those
//! calls as [`crate::context::Columns`] hooks, so this port evaluates the same
//! SQL against one shared session stub that records acquisitions/releases --
//! identical behavior table, no real server.
//!
//! Assertions kept from Go (with their sources):
//! - `get_lock` with ONE argument fails with `ErrWrongParamcountToNativeFct`
//!   (1582) (`integration_test.go:1458-1461`);
//! - timeout `0` acquires immediately and `-10` clamps to the max with the
//!   warning `Truncated incorrect get_lock value: '-10'`
//!   (`integration_test.go:1463-1467`);
//! - acquired locks release with answer `1` and `release_all_locks()` then
//!   reports `0` (`integration_test.go:1468-1470`);
//! - an empty-string or NULL lock name raises `ErrUserLockWrongName` (3057)
//!   (`integration_test.go:1473-1483`).

use super::*;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
use std::cell::RefCell;
use std::collections::BTreeSet;
use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode};

/// A minimal session: `ReleaseAllUserLocks` over the set of names this
/// connection holds. Every acquisition succeeds, matching Go's fresh session
/// where no competing holder exists.
#[derive(Default)]
struct LockSession {
    held: RefCell<BTreeSet<String>>,
}

impl Columns for LockSession {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn acquire_advisory_lock(&self, name: &str, _: std::time::Duration) -> Result<bool, EvalError> {
        self.held.borrow_mut().insert(name.to_owned());
        Ok(true)
    }

    fn release_advisory_lock(&self, name: &str) -> Result<bool, EvalError> {
        Ok(self.held.borrow_mut().remove(name))
    }

    fn release_all_advisory_locks(&self) -> Result<usize, EvalError> {
        let mut held = self.held.borrow_mut();
        let count = held.len();
        held.clear();
        Ok(count)
    }
}

impl LockSession {
    /// Evaluates each select-field expression of `sql` in one statement and
    /// returns the labels of the single result row.
    fn query_row(&self, sql: &str) -> Vec<String> {
        let stmt = tidb_parser::parse(sql).expect("parse");
        let Stmt::Query(query) = stmt else { panic!("not query") };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("not select")
        };
        select
            .fields
            .iter()
            .map(|field| {
                let SelectField::Expr { expr, .. } = field else {
                    panic!("all fields are expressions")
                };
                let rewritten =
                    rewrite_expr_resolved(expr, &NoResolver).expect("rewrite without columns");
                // Rewrite/evaluation constants carry VARCHAR types already;
                // give the tree one empty virtual row like testkit would.
                let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
                chunk.set_num_virtual_rows(1);
                rewritten.eval(self, chunk.get_row(0)).unwrap().label()
            })
            .collect()
    }
}

struct NoResolver;
impl ColumnResolver for NoResolver {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let _ = path;
        None
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
}

#[test]
fn test_get_lock_call_semantics_table() {
    let session = LockSession::default();

    // Go: SELECT get_lock('testlock') errors with 1582 (wrong parameter
    // count). At the Rust eval surface the same arity violation surfaces as
    // EvalError::WrongParameterCount (context.rs:72 documents the mapping).
    {
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("get_lock"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Constant(Constant::new(
                Datum::new_string("testlock".to_owned()),
                FieldType::new(FieldTypeCode::VarString),
            ))],
        );
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        let err = function
            .eval(&session, chunk.get_row(0))
            .expect_err("one-argument get_lock must fail");
        match err {
            EvalError::WrongParameterCount(_) => {}
            other => panic!("expected 1582 wrong-parameter-count, got {other:?}"),
        }
    }

    // Timeout 0 acquires immediately; -10 converts to the max value WITH the
    // truncation warning. Both answers read `1`.
    assert_eq!(session.query_row("SELECT get_lock('testlock1', 0)"), ["INT:1"]);
    let warnings = std::cell::RefCell::new(Vec::<(u16, String)>::new());
    struct WarnSession<'a> {
        inner: &'a LockSession,
        warnings: &'a std::cell::RefCell<Vec<(u16, String)>>,
    }
    impl Columns for WarnSession<'_> {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.inner.get(path)
        }
        fn acquire_advisory_lock(&self, name: &str, t: std::time::Duration) -> Result<bool, EvalError> {
            self.inner.acquire_advisory_lock(name, t)
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
        fn truncate_level(&self) -> crate::context::ErrorLevel {
            ErrorLevel::Warn
        }
    }
    let warn_session = WarnSession { inner: &session, warnings: &warnings };
    assert_eq!(session.query_row("SELECT get_lock('testlock2', -10)"), ["INT:1"]);
    // Re-run through the collecting wrapper so the emitted warning can be
    // compared with Go's SHOW WARNINGS row.
    {
        let stmt = tidb_parser::parse("SELECT get_lock('testlock2', -10)").expect("parse");
        let Stmt::Query(query) = stmt else { panic!("not query") };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("not select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("no expr")
        };
        let rewritten = rewrite_expr_resolved(expr, &NoResolver).expect("rewrite");
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        rewritten.eval(&warn_session, chunk.get_row(0)).expect("eval");
    }
    let got_warnings = warnings.borrow();
    assert_eq!(
        &*got_warnings,
        &[(1292u16, "Truncated incorrect get_lock value: '-10'".to_owned())],
        "Go pins exactly one warning 1292 row (integration_test.go:1466)"
    );
    drop(got_warnings);

    // Both acquired locks release as `1`; afterwards release_all_locks()
    // reports 0 (nothing left to release).
    assert_eq!(
        session.query_row(
            "SELECT release_lock('testlock1'), release_lock('testlock2')"
        ),
        ["INT:1", "INT:1"]
    );
    assert_eq!(session.query_row("SELECT release_all_locks()"), ["INT:0"]);
}

#[test]
fn test_get_lock_rejects_bad_names_with_3057() {
    // Go (`integration_test.go:1475-1483`): get_lock('', 10) and NULL-name
    // variants raise ErrUserLockWrongName (errno 3057).
    for sql in [
        r#"SELECT get_lock('', 10)"#,
        r#"SELECT get_lock(NULL, 10)"#,
        r#"SELECT release_lock('')"#,
        r#"SELECT release_lock(NULL)"#,
    ] {
        let session = LockSession::default();
        let stmt = tidb_parser::parse(sql).expect("parse");
        let Stmt::Query(query) = stmt else { panic!("not query") };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("not select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("no expr")
        };
        let rewritten = rewrite_expr_resolved(expr, &NoResolver).expect("rewrite");
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        let err = rewritten.eval(&session, chunk.get_row(0)).expect_err(sql);
        match err {
            EvalError::AdvisoryLock { code, .. } => assert_eq!(code, 3057, "{sql}"),
            other => panic!("{sql}: expected ErrUserLockWrongName (3057), got {other:?}"),
        }
    }
}
