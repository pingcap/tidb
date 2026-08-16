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

//! Transcreation of Go `pkg/ttl/session`'s tests
//! (`session_test.go`, `sysvar_test.go`).
//!
//! All three `session_test.go` tests reach TTL's behaviour through a live TiDB
//! (`testkit.CreateMockStore` plus a real session), which this workspace has no
//! counterpart for. Each is ported here against a scripted
//! [`SessionContext`](tidb_ttl::session::SessionContext) that records the SQL it
//! is handed, so the assertions land on exactly the behaviour `session.go`
//! owns — the statement sequence, the rollback rule, the phase transitions, the
//! time-zone short-circuit and the kill signal — rather than on the server that
//! executes them.
//!
//! Skipped, with what each would need:
//! - `TestSessionKill`'s outer half. Go starts `select sleep(123)`, polls
//!   `do.InfoSyncer().GetSessionManager().ShowProcessList()` until the
//!   statement appears, kills it and asserts the query returns `1`. That needs
//!   a running server, a session manager and the `sleep` builtin;
//!   `test_session_kill` below keeps the half `session.go` owns — that
//!   `KillStmt` raises `QueryInterrupted` on the session's `SQLKiller`.
//! - `TestSysVarTTLJobEnable`, `TestSysVarTTLScanBatchSize`,
//!   `TestSysVarTTLScanDeleteBatchSize`, `TestSysVarTTLScanDeleteLimit`. These
//!   four assert `pkg/sessionctx/variable`'s registration of
//!   `tidb_ttl_job_enable`, `tidb_ttl_scan_batch_size`,
//!   `tidb_ttl_delete_batch_size` and `tidb_ttl_delete_rate_limit`: that `SET
//!   @@global....` clamps out-of-range values and writes through to the
//!   `vardef` atomics. Not one line of `pkg/ttl/session` participates. They
//!   need the system-variable catalog plus a live session to run `SET`/`SELECT
//!   @@`, both outside this crate.

use std::sync::Mutex;

use tidb_datatype::{CoreTime, Datum, Time, TimeType};
use tidb_util::sqlkiller::{KillSignal, SqlKiller};
use tidb_util::timeutil::TimeZone;

use tidb_ttl::session::{
    Phase, PhaseTracer, ResultRow, Session, SessionContext, SessionError, TtlSession, TxnMode,
};

/// A row that is never read: these tests exercise statement flow, not results.
#[derive(Debug, Clone)]
struct NoRow;

impl ResultRow for NoRow {
    fn is_null(&self, _col_idx: usize) -> bool {
        true
    }
    fn get_int64(&self, _col_idx: usize) -> i64 {
        0
    }
    fn get_string(&self, _col_idx: usize) -> String {
        String::new()
    }
    fn get_bytes(&self, _col_idx: usize) -> Vec<u8> {
        Vec::new()
    }
    fn get_time(&self, _col_idx: usize) -> Time {
        Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()
    }
}

/// The scripted stand-in for `sessionctx.Context`.
struct MockContext {
    executed: Mutex<Vec<String>>,
    /// SQL text that must fail, with the error it fails with.
    fail_on: Option<(String, String)>,
    session_time_zone: Option<TimeZone>,
    global_time_zone_var: String,
    session_time_zone_var: String,
    killer: SqlKiller,
}

impl MockContext {
    fn new() -> Self {
        Self {
            executed: Mutex::new(Vec::new()),
            fail_on: None,
            session_time_zone: Some(shanghai()),
            global_time_zone_var: "UTC".to_owned(),
            session_time_zone_var: "Asia/Shanghai".to_owned(),
            killer: SqlKiller::default(),
        }
    }

    fn executed(&self) -> Vec<String> {
        self.executed.lock().unwrap().clone()
    }
}

/// `Asia/Shanghai`, resolved the way `GlobalTimeZone` resolves it.
fn shanghai() -> TimeZone {
    tidb_util::timeutil::parse_time_zone("Asia/Shanghai").unwrap()
}

impl SessionContext for MockContext {
    type Row = NoRow;
    type Store = ();
    type InfoSchema = ();

    fn get_store(&self) {}
    fn get_latest_info_schema(&self) {}
    fn get_txn_info_schema(&self) {}

    fn execute_internal(
        &self,
        sql: &str,
        _args: &[Datum],
    ) -> Result<Option<Vec<NoRow>>, SessionError> {
        self.executed.lock().unwrap().push(sql.to_owned());
        if let Some((failing, message)) = self.fail_on.as_ref() {
            if failing == sql {
                return Err(SessionError(message.clone()));
            }
        }
        Ok(None)
    }

    fn get_global_system_var(&self, _name: &str) -> Result<String, SessionError> {
        Ok(self.global_time_zone_var.clone())
    }

    fn get_session_or_global_system_var(&self, _name: &str) -> Result<String, SessionError> {
        Ok(self.session_time_zone_var.clone())
    }

    fn time_zone(&self) -> Option<TimeZone> {
        self.session_time_zone.clone()
    }

    fn location(&self) -> TimeZone {
        self.session_time_zone.clone().unwrap_or(TimeZone::Fixed {
            name: String::new(),
            offset_secs: 0,
        })
    }

    fn sql_killer(&self) -> &SqlKiller {
        &self.killer
    }
}

/// A tracer that records every phase it is moved into.
#[derive(Default)]
struct RecordingTracer {
    phases: Mutex<Vec<Phase>>,
    current: Mutex<Option<Phase>>,
}

impl PhaseTracer for RecordingTracer {
    fn phase(&self) -> Phase {
        self.current.lock().unwrap().unwrap_or(Phase::Other)
    }

    fn enter_phase(&self, phase: Phase) {
        *self.current.lock().unwrap() = Some(phase);
        self.phases.lock().unwrap().push(phase);
    }
}

/// Go `TestSessionRunInTxn`, without the live store.
///
/// Go asserts through the data: the first closure's insert is visible to a
/// second connection, the second closure returns `mockErr` and its insert is
/// not, and the third commits again. Visibility is the store's job; what
/// `RunInTxn` owns is that a successful body is bracketed by `BEGIN
/// OPTIMISTIC`/`COMMIT`, that a failing body is followed by `ROLLBACK` and its
/// error is returned verbatim, and that a later call is unaffected. Those are
/// the assertions here.
#[test]
fn test_session_run_in_txn() {
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    let tracer = RecordingTracer::default();

    assert!(se
        .run_in_txn(&mut || Ok(()), TxnMode::OPTIMISTIC, &tracer)
        .is_ok());
    assert_eq!(
        se.context().executed(),
        vec!["BEGIN OPTIMISTIC".to_owned(), "COMMIT".to_owned()]
    );

    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    let err = se
        .run_in_txn(
            &mut || Err(SessionError("mockErr".to_owned())),
            TxnMode::OPTIMISTIC,
            &tracer,
        )
        .unwrap_err();
    assert_eq!(err.to_string(), "mockErr");
    assert_eq!(
        se.context().executed(),
        vec!["BEGIN OPTIMISTIC".to_owned(), "ROLLBACK".to_owned()]
    );

    // A third transaction on a fresh session commits as the first one did.
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    assert!(se
        .run_in_txn(&mut || Ok(()), TxnMode::OPTIMISTIC, &tracer)
        .is_ok());
    assert_eq!(
        se.context().executed(),
        vec!["BEGIN OPTIMISTIC".to_owned(), "COMMIT".to_owned()]
    );
}

/// `RunInTxn`'s pessimistic arm and its `default` arm, which Go's own tests
/// never reach but `session.go` spells out.
#[test]
fn test_run_in_txn_modes() {
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    let tracer = RecordingTracer::default();
    assert!(se
        .run_in_txn(&mut || Ok(()), TxnMode::PESSIMISTIC, &tracer)
        .is_ok());
    assert_eq!(
        se.context().executed(),
        vec!["BEGIN PESSIMISTIC".to_owned(), "COMMIT".to_owned()]
    );

    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    let err = se
        .run_in_txn(&mut || Ok(()), TxnMode(7), &tracer)
        .unwrap_err();
    assert_eq!(err.to_string(), "unknown transaction mode");
    // Go still rolls back: the `defer` runs even when only the mode check failed.
    assert_eq!(se.context().executed(), vec!["ROLLBACK".to_owned()]);
}

/// A `BEGIN` that fails is still followed by `ROLLBACK` — the case the Go
/// comment on the `defer` exists for.
#[test]
fn test_run_in_txn_rolls_back_a_failed_begin() {
    let mut sctx = MockContext::new();
    sctx.fail_on = Some(("BEGIN OPTIMISTIC".to_owned(), "killed".to_owned()));
    let se = TtlSession::new(sctx, Box::new(|| {}));
    let tracer = RecordingTracer::default();

    let mut body_ran = false;
    let err = se
        .run_in_txn(
            &mut || {
                body_ran = true;
                Ok(())
            },
            TxnMode::OPTIMISTIC,
            &tracer,
        )
        .unwrap_err();
    assert_eq!(err.to_string(), "killed");
    assert!(!body_ran);
    assert_eq!(
        se.context().executed(),
        vec!["BEGIN OPTIMISTIC".to_owned(), "ROLLBACK".to_owned()]
    );
}

/// The phase sequence `RunInTxn` drives the tracer through.
#[test]
fn test_run_in_txn_phases() {
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    let tracer = RecordingTracer::default();
    tracer.enter_phase(Phase::Other);
    tracer.phases.lock().unwrap().clear();

    se.run_in_txn(&mut || Ok(()), TxnMode::OPTIMISTIC, &tracer)
        .unwrap();

    assert_eq!(
        *tracer.phases.lock().unwrap(),
        vec![
            Phase::BeginTxn,
            Phase::Other,
            Phase::CommitTxn,
            Phase::Other,
            // the deferred restore of the phase captured on entry
            Phase::Other,
        ]
    );
}

/// Go `TestSessionResetTimeZone`, without the live store.
///
/// Go sets the global zone to `UTC` and the session zone to `Asia/Shanghai`,
/// then asserts `select @@time_zone` reads `UTC` after the reset. The statement
/// that performs it is what `session.go` owns, so this asserts the statement is
/// issued when the two differ.
#[test]
fn test_session_reset_time_zone() {
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    se.reset_with_global_time_zone().unwrap();
    assert_eq!(
        se.context().executed(),
        vec!["SET @@time_zone=@@global.time_zone".to_owned()]
    );
}

/// The short-circuit `ResetWithGlobalTimeZone` opens with: an already-matching
/// session zone issues no statement at all.
#[test]
fn test_session_reset_time_zone_short_circuits() {
    let mut sctx = MockContext::new();
    sctx.session_time_zone_var = "UTC".to_owned();
    let se = TtlSession::new(sctx, Box::new(|| {}));
    se.reset_with_global_time_zone().unwrap();
    assert!(se.context().executed().is_empty());

    // With no session zone set at all Go skips the comparison entirely and
    // always issues the SET.
    let mut sctx = MockContext::new();
    sctx.session_time_zone = None;
    sctx.session_time_zone_var = "UTC".to_owned();
    let se = TtlSession::new(sctx, Box::new(|| {}));
    se.reset_with_global_time_zone().unwrap();
    assert_eq!(
        se.context().executed(),
        vec!["SET @@time_zone=@@global.time_zone".to_owned()]
    );
}

/// `GlobalTimeZone` parses the global variable through `timeutil`.
#[test]
fn test_session_global_time_zone() {
    let mut sctx = MockContext::new();
    sctx.global_time_zone_var = "Asia/Shanghai".to_owned();
    let se = TtlSession::new(sctx, Box::new(|| {}));
    assert_eq!(se.global_time_zone().unwrap(), shanghai());

    let mut sctx = MockContext::new();
    sctx.global_time_zone_var = "+02:00".to_owned();
    let se = TtlSession::new(sctx, Box::new(|| {}));
    assert_eq!(
        se.global_time_zone().unwrap(),
        TimeZone::Fixed {
            name: String::new(),
            offset_secs: 2 * 3600,
        }
    );

    let mut sctx = MockContext::new();
    sctx.global_time_zone_var = "Not/AZone".to_owned();
    let se = TtlSession::new(sctx, Box::new(|| {}));
    assert!(se.global_time_zone().is_err());
}

/// Go `TestSessionKill`, reduced to the half `session.go` owns.
///
/// Go's outer loop needs a running server (see the module header); what
/// `KillStmt` itself does is raise `QueryInterrupted` on the session's killer.
#[test]
fn test_session_kill() {
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    assert!(se.context().sql_killer().get_kill_signal().is_none());
    se.kill_stmt();
    assert_eq!(
        se.context().sql_killer().get_kill_signal(),
        Some(KillSignal::QueryInterrupted)
    );
}

/// `AvoidReuse` calls the hook `NewSession` was given, and tolerates its
/// absence — the `nil` check Go's method opens with.
#[test]
fn test_session_avoid_reuse() {
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = std::sync::Arc::clone(&called);
    let se = TtlSession::new(
        MockContext::new(),
        Box::new(move || flag.store(true, std::sync::atomic::Ordering::SeqCst)),
    );
    se.avoid_reuse();
    assert!(called.load(std::sync::atomic::Ordering::SeqCst));

    let se = TtlSession::without_avoid_reuse(MockContext::new());
    se.avoid_reuse();
}

/// `ExecuteSQL` turns Go's `nil` record set into no rows.
#[test]
fn test_session_execute_sql_without_result_set() {
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    let rows = se.execute_sql("SET @@x=1", &[]).unwrap();
    assert!(rows.is_empty());
    assert_eq!(se.context().executed(), vec!["SET @@x=1".to_owned()]);
}

/// `Now` reports the session location.
#[test]
fn test_session_now_uses_session_location() {
    let se = TtlSession::new(MockContext::new(), Box::new(|| {}));
    let now = se.now();
    assert_eq!(now.location, shanghai());
    assert!(now.unix_nanos > 0);
}
