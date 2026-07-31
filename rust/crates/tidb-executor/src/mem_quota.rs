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

//! `tidb_mem_quota_query` enforcement: the session and per-statement memory
//! trackers, and the OOM action `tidb_mem_oom_action` selects.
//!
//! This is the executor side of Go's `ResetContextOfStmt`
//! (`pkg/executor/select.go`), which builds exactly this shape per statement:
//!
//! ```text
//! SessionVars.MemTracker   LabelForSession, limit = tidb_mem_quota_query
//!                          action = PanicOnExceed (CANCEL) / LogOnExceed (LOG)
//!   └─ StmtCtx.MemTracker  LabelForSQLText, no limit of its own
//!        └─ operator trackers, one per memory-tracked executor
//! ```
//!
//! The limit lives on the SESSION tracker, not the statement one, and every
//! operator's tracker hangs off the statement tracker -- so an operator's
//! `Consume` bubbles to the session root, and the root is where the action
//! fires. That placement is what makes a quota a per-statement bound even
//! though it is a session-scoped tracker: `ResetContextOfStmt` detaches and
//! re-attaches a fresh statement tracker for each statement.
//!
//! FAITHFUL ADAPTATION (transport, not accounting): Go's `PanicOnExceed`
//! sends `QueryMemoryExceeded` to the session's `SQLKiller` and then PANICS
//! with the kill error, which `pkg/executor`'s recover turns back into the
//! statement's error. Panicking across an operator is Go-runtime shaping, not
//! observable behavior, so [`CancelOnExceed`] here sends the same kill signal
//! to a killer of its own and stops; the accounting operator then calls
//! [`StatementMemory::check`], which polls that killer and returns the error
//! it yields. The errno, the SQL state and the message text therefore come
//! from the ported `SqlKiller`/`exeerrors` path unchanged -- errno 8175,
//! `ErrMemoryExceedForQuery` (captured: `[executor:8175]Your query has been
//! cancelled due to exceeding the allowed memory limit for a single SQL
//! query. Please try narrowing your query scope or increase the
//! tidb_mem_quota_query limit and try again.[conn=1]`).
//!
//! NOT WIRED, and named so it is not mistaken for covered: the killer above
//! is private to the memory path, so only an operator that calls `check`
//! observes the cancellation. Go's killer is the SESSION's, which every
//! operator polls, so a Go statement stops in whatever operator notices
//! first. Until a session-wide killer is plumbed through the executor tree,
//! a statement here stops in the ACCOUNTING operator only.
//!
//! WHICH OPERATORS ACCOUNT: the sort (`crate::sort`), and the WRITE path --
//! `UPDATE`, `DELETE`, `INSERT`/`REPLACE`, and the rows a foreign-key cascade
//! reads and rewrites -- through [`WriteMemory`], whose doc states what is
//! counted and how it differs from Go's chunk arithmetic. A read path other
//! than the sort still accounts nothing, so a `SELECT` under a small quota
//! runs to completion here where TiDB cancels it.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;

use tidb_datatype::{estimated_mem_usage, Datum};
use tidb_util::memory::{
    ActionOnExceed, ArcAction, BaseOomAction, LogOnExceed, Tracker, DEF_MEM_QUOTA_QUERY,
    DEF_PANIC_PRIORITY, LABEL_FOR_SESSION, LABEL_FOR_SQL_TEXT,
};
use tidb_util::sqlkiller::{KillSignal, SqlKiller};

use crate::executor::ExecError;

/// `tidb_mem_oom_action`: what a statement that exceeds
/// `tidb_mem_quota_query` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OomAction {
    /// `CANCEL`, Go's `vardef.DefTiDBMemOOMAction` -- the PRODUCTION default.
    /// The statement fails with errno 8175.
    ///
    /// Go's own test builds report `LOG` instead, but only because
    /// `GlobalSystemVariableInitialValue` rewrites the initial value when
    /// `intest.InTest` is set; the shipped default is this one (captured).
    #[default]
    Cancel,
    /// `LOG`. The statement RUNS TO COMPLETION and the overrun is logged --
    /// no error, and no `SHOW WARNINGS` row either (captured).
    Log,
}

impl OomAction {
    /// Parses `@@tidb_mem_oom_action`. Go's sysvar is a `TypeEnum` over
    /// exactly `CANCEL`/`LOG`, so an unrecognized string cannot come from a
    /// validated `SET`; treating it as the default keeps the protective
    /// action rather than silently downgrading to logging.
    #[must_use]
    pub fn parse(value: &str) -> Self {
        if value.eq_ignore_ascii_case("LOG") {
            OomAction::Log
        } else {
            OomAction::Cancel
        }
    }
}

/// The wire form of `exeerrors.ErrMemoryExceedForQuery` for `conn_id`,
/// rendered by the SAME ported path Go's `SQLKiller.getKillError` uses -- a
/// killer carrying a pending `QueryMemoryExceeded` signal.
///
/// Going through the killer rather than formatting the message here keeps one
/// source for the text, the errno and the SQL state.
#[must_use]
pub fn memory_exceed_for_query(conn_id: u64) -> tidb_error::mysql::SqlError {
    let killer = SqlKiller::default();
    killer.conn_id.store(conn_id, SeqCst);
    killer.send_kill_signal(KillSignal::QueryMemoryExceeded);
    killer
        .handle_signal()
        .expect("a pending QueryMemoryExceeded signal always yields its error")
        .to_sql_error()
}

/// Go `PanicOnExceed` in everything but its panic: it sends
/// `QueryMemoryExceeded` to a killer, once, and logs the overrun.
struct CancelOnExceed {
    base: BaseOomAction,
    killer: Arc<SqlKiller>,
    acted: AtomicBool,
}

impl ActionOnExceed for CancelOnExceed {
    fn action(&self, t: &Arc<Tracker>) {
        if !self.acted.swap(true, SeqCst) {
            tracing::warn!(
                conn = t.session_id.load(SeqCst),
                label = t.label(),
                consumed = t.bytes_consumed(),
                limit = t.get_bytes_limit(),
                "memory exceeds quota"
            );
        }
        self.killer
            .send_kill_signal(KillSignal::QueryMemoryExceeded);
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    /// Go `PanicOnExceed.GetPriority`.
    fn get_priority(&self) -> i64 {
        DEF_PANIC_PRIORITY
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// One statement's memory budget: the session tracker holding
/// `tidb_mem_quota_query` and the statement tracker every operator attaches
/// to.
///
/// Cloning shares the same trackers, so the handle can be copied to each
/// operator that accounts; it is created ONCE per statement, by
/// [`crate::StmtContext`]'s single constructor.
#[derive(Clone)]
pub struct StatementMemory {
    session: Arc<Tracker>,
    stmt: Arc<Tracker>,
    /// The killer [`CancelOnExceed`] signals; `None` under `LOG`, where
    /// nothing cancels.
    killer: Option<Arc<SqlKiller>>,
}

impl Default for StatementMemory {
    /// A budget with the SHIPPED defaults: `tidb_mem_quota_query` = 1GiB and
    /// `tidb_mem_oom_action` = `CANCEL`.
    ///
    /// Defaulting to the real quota rather than to "unlimited" is deliberate:
    /// a context built without a session behind it still protects, so a new
    /// call site cannot opt out of the limit by forgetting to set it.
    fn default() -> Self {
        StatementMemory::new(DEF_MEM_QUOTA_QUERY, OomAction::Cancel, 0)
    }
}

impl StatementMemory {
    /// Builds the two-tracker shape `ResetContextOfStmt` builds, with
    /// `quota` on the session root and the action `oom_action` selects.
    ///
    /// A `quota <= 0` is Go's "no limit", which `SetBytesLimit` normalizes.
    #[must_use]
    pub fn new(quota: i64, oom_action: OomAction, connection_id: u64) -> Self {
        let session = Tracker::new(LABEL_FOR_SESSION, -1);
        session.set_bytes_limit(quota);
        session.is_root_tracker_of_sess.store(true, SeqCst);
        session.session_id.store(connection_id, SeqCst);

        let killer = match oom_action {
            OomAction::Cancel => {
                let killer = Arc::new(SqlKiller::default());
                killer.conn_id.store(connection_id, SeqCst);
                session.set_action_on_exceed(Some(Arc::new(CancelOnExceed {
                    base: BaseOomAction::default(),
                    killer: Arc::clone(&killer),
                    acted: AtomicBool::new(false),
                })));
                Some(killer)
            }
            OomAction::Log => {
                let action = LogOnExceed::default();
                session.set_action_on_exceed(Some(Arc::new(action)));
                None
            }
        };

        // Go `sc.InitMemTracker(memory.LabelForSQLText, -1)` then
        // `sc.MemTracker.AttachTo(vars.MemTracker)`: the statement tracker
        // carries no limit of its own, only the session root's.
        let stmt = Tracker::new(LABEL_FOR_SQL_TEXT, -1);
        stmt.session_id.store(connection_id, SeqCst);
        stmt.attach_to(&session);

        StatementMemory {
            session,
            stmt,
            killer,
        }
    }

    /// The statement tracker an operator attaches its own tracker to (Go
    /// `StmtCtx.MemTracker`).
    #[must_use]
    pub fn stmt_tracker(&self) -> &Arc<Tracker> {
        &self.stmt
    }

    /// A fresh operator tracker with no limit of its own, already attached to
    /// the statement tracker -- Go's `memory.NewTracker(e.ID(), -1)` +
    /// `AttachTo(StmtCtx.MemTracker)`.
    #[must_use]
    pub fn operator_tracker(&self, label: i64) -> Arc<Tracker> {
        let tracker = Tracker::new(label, -1);
        tracker
            .session_id
            .store(self.session.session_id.load(SeqCst), SeqCst);
        tracker.attach_to(&self.stmt);
        tracker
    }

    /// Whether the quota has been exceeded and the statement must stop, as
    /// the error Go's `SQLKiller.HandleSignal` yields.
    ///
    /// An accounting operator calls this immediately after each `Consume`.
    /// Under `LOG` it can never fail, which is exactly the captured behavior.
    pub fn check(&self) -> Result<(), ExecError> {
        let Some(killer) = self.killer.as_ref() else {
            return Ok(());
        };
        match killer.get_kill_signal() {
            Some(KillSignal::QueryMemoryExceeded) => Err(ExecError::MemoryExceedForQuery {
                conn_id: killer.conn_id.load(SeqCst),
            }),
            _ => Ok(()),
        }
    }

    /// An accountant for one write operator, labelled by the operator it
    /// stands for. See [`WriteMemory`].
    #[must_use]
    pub fn write_accountant(&self, label: i64) -> WriteMemory {
        WriteMemory {
            tracker: self.operator_tracker(label),
            memory: self.clone(),
        }
    }

    /// Bytes the whole statement currently accounts for (Go
    /// `SessionVars.MemTracker.BytesConsumed()`).
    #[must_use]
    pub fn bytes_consumed(&self) -> i64 {
        self.session.bytes_consumed()
    }

    /// The quota in force (Go `SessionVars.MemTracker.GetBytesLimit()`).
    #[must_use]
    pub fn quota(&self) -> i64 {
        self.session.get_bytes_limit()
    }
}

/// Operator labels for the write path. Go labels an operator's tracker with
/// its PLAN ID, which is per-statement and carries no meaning across
/// statements; this tier's writes are not plan nodes, so each write operator
/// gets one stable label instead. Labels are diagnostic only -- the quota is
/// enforced at the session root either way.
pub mod label {
    /// Go `UpdateExec.memTracker`.
    pub const UPDATE: i64 = 1;
    /// Go `DeleteExec.memTracker`.
    pub const DELETE: i64 = 2;
    /// Go `InsertValues.memTracker`, shared by `INSERT` and `REPLACE`.
    pub const INSERT: i64 = 3;
    /// The `UpdateExec`/`DeleteExec` a foreign-key cascade builds for its
    /// child table (Go `FKCascadeExec.buildExecutor`). The cascade itself
    /// accounts nothing in Go -- the sub-statement's own executor does.
    pub const FK_CASCADE: i64 = 4;
}

/// The write path's memory accounting: one operator tracker plus the check
/// that turns an overrun into 8175.
///
/// # What is counted, and why it is the datum rows
///
/// Go's three write executors all account the ROWS they hold, in two shapes:
///
/// * `types.EstimatedMemUsage(rows[0], len(rows))` over the datum rows a
///   statement has staged but not yet written (`InsertValues.insertRows`,
///   `UpdateExec.mergeNonGenerated`, `DeleteExec.composeTblRowMap`).
/// * `chk.MemoryUsage()` for the CHUNK the child produced, consumed on the way
///   in and released on the way out, so one chunk is held at a time
///   (`UpdateExec.updateRows`, `DeleteExec.deleteSingleTableByChunk`,
///   `InsertValues.insertRowsFromSelect`).
///
/// [`Self::account_row`] is the second shape's analogue and
/// [`Self::account_rows`] is the first, ported literally.
///
/// DELIBERATE DIVERGENCE, in the direction Go's own comment justifies: there
/// is no chunk pipeline on this tier's write path -- `scan_rows_with_handles`
/// materializes the whole table as `Vec<Datum>` rows before the statement
/// walks them -- so the per-row number here is what the process HOLDS, and it
/// is never released mid-statement the way Go releases a spent chunk. Against
/// Go that is smaller per row (a chunk over-allocates to its capacity) and
/// larger in total (every row read is still held). Both halves are the truth
/// about this process, which is the property a quota has to bound; a tracker
/// reporting Go's number while holding different memory would not protect.
/// The consequence is that the exact byte at which a statement crosses a given
/// quota differs from Go's, so only quotas far from the boundary -- which is
/// what the suite sets, 244 and 81920 against a 1GiB default -- classify
/// identically.
#[derive(Clone)]
pub struct WriteMemory {
    tracker: Arc<Tracker>,
    memory: StatementMemory,
}

impl WriteMemory {
    /// Accounts one row the statement has read or staged, and stops the
    /// statement if that crossed the quota.
    ///
    /// Called INSIDE the loop that produces rows, which is what makes a write
    /// over a large table stop before staging the rest rather than after
    /// materializing all of it.
    pub fn account_row(&self, row: &[Datum]) -> Result<(), ExecError> {
        self.consume(estimated_mem_usage(row, 1));
        self.memory.check()
    }

    /// Go `types.EstimatedMemUsage(rows[0], len(rows))`: the first row's
    /// usage taken as every row's, which is how Go prices a staged row batch.
    ///
    /// An empty `rows` accounts nothing, as Go's `if len(rows) != 0` guard
    /// does.
    pub fn account_rows(&self, rows: &[Vec<Datum>]) -> Result<(), ExecError> {
        let Some(first) = rows.first() else {
            return Ok(());
        };
        self.consume(estimated_mem_usage(first, rows.len()));
        self.memory.check()
    }

    fn consume(&self, bytes: usize) {
        self.tracker
            .consume(i64::try_from(bytes).unwrap_or(i64::MAX));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_default_is_cancel_at_one_gibibyte() {
        // Captured: `select @@tidb_mem_quota_query` -> 1073741824, and
        // `DefTiDBMemOOMAction` -> CANCEL.
        assert_eq!(OomAction::default(), OomAction::Cancel);
        assert_eq!(StatementMemory::default().quota(), 1_073_741_824);
    }

    #[test]
    fn oom_action_parses_both_enum_values_and_defaults_to_protecting() {
        assert_eq!(OomAction::parse("CANCEL"), OomAction::Cancel);
        assert_eq!(OomAction::parse("cancel"), OomAction::Cancel);
        assert_eq!(OomAction::parse("LOG"), OomAction::Log);
        assert_eq!(OomAction::parse("log"), OomAction::Log);
        assert_eq!(OomAction::parse(""), OomAction::Cancel);
    }

    #[test]
    fn the_quota_lives_on_the_session_root_and_operators_bubble_into_it() {
        let mem = StatementMemory::new(4096, OomAction::Cancel, 7);
        let op = mem.operator_tracker(3);
        assert_eq!(op.get_bytes_limit(), -1);
        assert_eq!(mem.stmt_tracker().get_bytes_limit(), -1);
        assert_eq!(mem.quota(), 4096);

        op.consume(1000);
        assert_eq!(mem.bytes_consumed(), 1000);
        assert!(mem.check().is_ok());
    }

    #[test]
    fn cancel_raises_8175_once_the_operator_crosses_the_quota() {
        let mem = StatementMemory::new(4096, OomAction::Cancel, 7);
        let op = mem.operator_tracker(3);
        op.consume(4095);
        assert!(mem.check().is_ok(), "still inside the quota");
        op.consume(1);
        match mem.check() {
            Err(ExecError::MemoryExceedForQuery { conn_id }) => assert_eq!(conn_id, 7),
            other => panic!("expected 8175, got {other:?}"),
        }
    }

    #[test]
    fn log_never_fails_the_statement_however_far_it_overruns() {
        let mem = StatementMemory::new(4096, OomAction::Log, 7);
        let op = mem.operator_tracker(3);
        op.consume(1 << 30);
        assert!(mem.check().is_ok());
        assert_eq!(mem.bytes_consumed(), 1 << 30);
    }

    #[test]
    fn a_non_positive_quota_is_go_s_no_limit() {
        let mem = StatementMemory::new(-1, OomAction::Cancel, 0);
        let op = mem.operator_tracker(3);
        op.consume(1 << 40);
        assert!(mem.check().is_ok());
    }
}
