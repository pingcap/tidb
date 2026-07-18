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

//! Source-owned `SET TRANSACTION READ ONLY AS OF TIMESTAMP` rows.
//!
//! Go's `parseSetTransaction` stores the timestamp expression under
//! `tx_read_ts`, not under the ordinary `tx_read_only` switch.  This test
//! keeps that distinction visible while reusing the generic SetStmt envelope.

use super::*;

#[test]
fn set_transaction_read_only_as_of_timestamp_restores_like_go() {
    for (sql, expected) in [
        (
            "set transaction read only as of timestamp '2021-04-21 00:42:12'",
            "SET @@SESSION.`tx_read_ts`=_UTF8MB4'2021-04-21 00:42:12'",
        ),
        (
            "set transaction read only as of timestamp now(6) - interval 0.1 second",
            "SET @@SESSION.`tx_read_ts`=DATE_SUB(NOW(6), INTERVAL 0.1 SECOND)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn set_transaction_read_only_as_of_timestamp_keeps_typed_assignment() {
    let statement = parse(
        "set session transaction read only as of timestamp tidb_bounded_staleness('2021-04-21 00:42:12', now())",
    )
    .expect("SET SESSION TRANSACTION AS OF parses");
    let Stmt::Session(session) = statement else {
        panic!("expected Session envelope");
    };
    let tidb_ast::SessionStmt::Set(set) = session.as_ref() else {
        panic!("expected typed SET statement");
    };
    assert_eq!(set.assignments.len(), 1);
    assert_eq!(set.assignments[0].name, "tx_read_ts");
    assert!(matches!(
        set.assignments[0].value,
        tidb_ast::SetVariableValue::Expr(_)
    ));
}

#[test]
fn set_transaction_read_only_as_of_timestamp_rejects_incomplete_clause() {
    for sql in [
        "set transaction read only as of",
        "set transaction read only as of timestamp",
        "set transaction read only as timestamp '2021-04-21'",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
}
