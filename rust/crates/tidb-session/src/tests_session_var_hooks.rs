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

//! System variables whose value Go computes at READ time (`sysvar.go`'s
//! `GetSession` hooks) or refuses at WRITE time (its `Validation` hooks),
//! rather than storing in the variable table.

use crate::tests_support::row_text;
use crate::Session;

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .unwrap_or_default()
}

/// `sql_auto_is_null` carries the same `Validation` as the five read-only
/// no-op variables: turning it ON needs `tidb_enable_noop_functions`, and the
/// refusal branch returns `Off` rather than the requested value.
///
/// The `SET_VAR` hint is where that returned value shows: Go applies a hint
/// through `SetSystemVarWithRelaxedValidation`, which keeps the value the
/// hook returned and discards its error, so the statement succeeds while the
/// variable reads `0`. Source rows: `tests/integrationtest/t/session/vars.test`.
#[test]
fn a_noop_gated_variable_refuses_to_off_and_a_hint_takes_that_value() {
    let mut session = Session::new();
    assert_eq!(
        one(&mut session, "SELECT @@tidb_enable_noop_functions"),
        "OFF"
    );

    // The plain SET is the branch that keeps the error.
    let error = session
        .run("SET sql_auto_is_null = 1")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1235);
    assert_eq!(one(&mut session, "SELECT @@sql_auto_is_null"), "0");

    // The hint is the branch that keeps only the value.
    assert_eq!(
        one(
            &mut session,
            "SELECT /*+ SET_VAR(sql_auto_is_null=1) */ @@sql_auto_is_null"
        ),
        "0"
    );

    // With the gate open, both branches take the requested value -- so this
    // is the gate speaking, not a blanket refusal of the variable.
    session.run("SET @@tidb_enable_noop_functions = 1").unwrap();
    assert_eq!(
        one(
            &mut session,
            "SELECT /*+ SET_VAR(sql_auto_is_null=1) */ @@sql_auto_is_null"
        ),
        "1"
    );
    session.run("SET sql_auto_is_null = 1").unwrap();
    assert_eq!(one(&mut session, "SELECT @@sql_auto_is_null"), "1");
}

/// `@@warning_count` is Go's `SysWarningCount`: the count of the PREVIOUS
/// statement's warnings, snapshotted by `ResetContextOfStmt` at every
/// statement start.
///
/// The warning BUFFER is inherited only by the three statements that report
/// it, so reading the buffer answers `0` for every other statement --
/// including one asked immediately after a statement that warned. The counts
/// are a separate channel for exactly that reason.
#[test]
fn warning_count_reports_the_previous_statements_warnings() {
    let mut session = Session::new();
    assert_eq!(one(&mut session, "SELECT @@warning_count"), "0");

    // A duplicated `SET_VAR` hint is warning 3126.
    session
        .run(
            "SELECT /*+ SET_VAR(group_concat_max_len = 1024) \
             SET_VAR(group_concat_max_len = 2048) */ 1",
        )
        .unwrap();
    assert_eq!(one(&mut session, "SELECT @@warning_count"), "1");
    // The reading statement itself warned about nothing, so the next read is
    // back to zero -- the recorded sequence in `session/vars`.
    assert_eq!(one(&mut session, "SELECT @@session.warning_count"), "0");
    assert_eq!(one(&mut session, "SELECT @@local.warning_count"), "0");
    // `SHOW WARNINGS` still reports the buffer it inherits, which is the
    // other channel and is unchanged.
    assert_eq!(one(&mut session, "SELECT @@error_count"), "0");
}
