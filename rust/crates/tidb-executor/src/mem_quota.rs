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
//! to the statement's canonical killer and stops; the accounting operator
//! then calls [`StatementMemory::check`], which polls that killer and returns
//! the error it yields. The errno, the SQL state and the message text come
//! from the ported `SqlKiller`/`exeerrors` path unchanged -- errno 8175,
//! `ErrMemoryExceedForQuery` (captured: `[executor:8175]Your query has been
//! cancelled due to exceeding the allowed memory limit for a single SQL
//! query. Please try narrowing your query scope or increase the
//! tidb_mem_quota_query limit and try again.[conn=1]`).
//!
//! WHICH OPERATORS ACCOUNT: the sort (`crate::sort`), and the WRITE path --
//! `UPDATE`, `DELETE`, `INSERT`/`REPLACE`, and the rows a foreign-key cascade
//! reads and rewrites -- through [`WriteMemory`], whose doc states what is
//! counted and how it differs from Go's chunk arithmetic. A read path other
//! than the sort still accounts nothing, so a `SELECT` under a small quota
//! runs to completion here where TiDB cancels it.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock};

use tidb_datatype::{estimated_mem_usage, Datum};
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};
use tidb_util::memory::{
    ActionOnExceed, ArcAction, BaseOomAction, KillSignalTransport, LogOnExceed, Tracker,
    DEF_MEM_QUOTA_QUERY, DEF_PANIC_PRIORITY, LABEL_FOR_SESSION, LABEL_FOR_SQL_TEXT,
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

fn fallback_spill_storage() -> Arc<SpillStorage> {
    static STORAGE: OnceLock<Arc<SpillStorage>> = OnceLock::new();
    Arc::clone(STORAGE.get_or_init(|| {
        let path =
            std::env::temp_dir().join(format!("tidb-rust-standalone-spill-{}", std::process::id()));
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path,
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("standalone executor spill storage"),
        )
    }))
}

fn install_oom_action(
    session: &Arc<Tracker>,
    oom_action: OomAction,
    connection_id: u64,
) -> Arc<SqlKiller> {
    let killer = Arc::clone(&session.killer);
    killer.reset();
    killer.conn_id.store(connection_id, SeqCst);
    match oom_action {
        OomAction::Cancel => {
            session.set_action_on_exceed(Some(Arc::new(CancelOnExceed {
                base: BaseOomAction::default(),
                killer: Arc::clone(&killer),
                acted: AtomicBool::new(false),
            })));
        }
        OomAction::Log => {
            session.set_action_on_exceed(Some(Arc::new(LogOnExceed::default())));
        }
    }
    killer
}

fn refresh_global_disk_attachment(
    disk_session: &Arc<Tracker>,
    tmp_storage_on_oom: bool,
    spill_storage: Option<&Arc<SpillStorage>>,
) {
    disk_session.detach();
    if tmp_storage_on_oom {
        if let Some(storage) = spill_storage {
            disk_session.attach_to_global_tracker(storage.global_tracker());
        }
    }
}

/// One connection's persistent memory and disk accounting roots.
///
/// Go retains `SessionVars.MemTracker`/`DiskTracker` across statements, while
/// `ResetContextOfStmt` installs a fresh statement tracker, OOM action and
/// kill signal for each execution. Keeping that split explicit prevents one
/// cancelled cursor materialization from poisoning the next execution while
/// still charging all open cursors to the same connection quota.
#[derive(Clone)]
pub struct SessionMemory {
    session: Arc<Tracker>,
    disk_session: Arc<Tracker>,
    config: Arc<Mutex<SessionMemoryConfig>>,
    query_cancellation: Arc<Mutex<QueryCancellationState>>,
}

#[derive(Default)]
struct QueryCancellationState {
    generation: u64,
    requested: bool,
}

/// One command's handle for `KILL QUERY` and connection shutdown.
///
/// The handle is installed before parsing starts. If cancellation arrives
/// before `ResetContextOfStmt` creates the statement, the request stays
/// latched and is replayed onto the freshly reset statement killer.
pub struct StatementCancellation {
    killer: Arc<SqlKiller>,
    state: Arc<Mutex<QueryCancellationState>>,
    generation: u64,
}

impl StatementCancellation {
    /// Interrupts the command that owns this handle.
    pub fn cancel(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.generation != self.generation {
            return;
        }
        state.requested = true;
        drop(state);
        self.killer.send_kill_signal(KillSignal::QueryInterrupted);
    }
}

impl Drop for StatementCancellation {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.generation == self.generation {
            state.requested = false;
        }
    }
}

#[derive(Clone)]
struct SessionMemoryConfig {
    spill_storage: Option<Arc<SpillStorage>>,
    arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    tmp_storage_on_oom: bool,
    oom_action: OomAction,
}

struct StatementLifetime {
    stmt: Arc<Tracker>,
    disk_stmt: Arc<Tracker>,
    finished: AtomicBool,
}

impl StatementLifetime {
    fn finish(&self) {
        if self.finished.swap(true, SeqCst) {
            return;
        }
        self.stmt.detach_mem_arbitrator(false);
        self.stmt.detach();
        self.disk_stmt.detach();
    }
}

impl Drop for StatementLifetime {
    fn drop(&mut self) {
        self.finish();
    }
}

impl SessionMemory {
    /// Creates persistent session roots. Call [`Self::statement`] for each
    /// execution; the returned statement owns fresh cancellation state.
    #[must_use]
    pub fn new(quota: i64, oom_action: OomAction, connection_id: u64) -> Self {
        let session = Tracker::new(LABEL_FOR_SESSION, -1);
        session.set_bytes_limit(quota);
        session.is_root_tracker_of_sess.store(true, SeqCst);
        session.session_id.store(connection_id, SeqCst);
        session.set_kill_signal_transport(KillSignalTransport::Deferred);

        let disk_session = Tracker::new(LABEL_FOR_SESSION, -1);
        disk_session.session_id.store(connection_id, SeqCst);

        Self {
            session,
            disk_session,
            config: Arc::new(Mutex::new(SessionMemoryConfig {
                spill_storage: None,
                arbitrator: None,
                tmp_storage_on_oom: true,
                oom_action,
            })),
            query_cancellation: Arc::default(),
        }
    }

    /// Starts the cancellation lifetime for one wire command.
    #[must_use]
    pub fn begin_query_cancellation(&self) -> StatementCancellation {
        let generation = {
            let mut state = self
                .query_cancellation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.generation = state.generation.wrapping_add(1);
            state.requested = false;
            state.generation
        };
        StatementCancellation {
            killer: Arc::clone(&self.session.killer),
            state: Arc::clone(&self.query_cancellation),
            generation,
        }
    }

    /// Sets `@@tidb_enable_tmp_storage_on_oom` for subsequently created
    /// statements.
    #[must_use]
    pub fn with_tmp_storage_on_oom(self, enabled: bool) -> Self {
        self.set_tmp_storage_on_oom(enabled);
        self
    }

    /// Installs the process spill authority on the persistent disk root.
    #[must_use]
    pub fn with_spill_storage(self, storage: Arc<SpillStorage>) -> Self {
        self.set_spill_storage(storage);
        self
    }

    /// Installs the server-owned memory arbitrator for subsequently created
    /// statement roots. Existing statements retain their own registration
    /// until they finish, just as Go keeps the tracker chosen at execution
    /// start.
    #[must_use]
    pub fn with_mem_arbitrator(self, arbitrator: Arc<tidb_util::memory::MemArbitrator>) -> Self {
        self.set_mem_arbitrator(arbitrator);
        self
    }

    /// Reconfigures the policy read for the next statement while retaining
    /// this connection's session roots and any cursor bytes below them.
    ///
    /// Go mutates `SessionVars.MemTracker` in `ResetContextOfStmt`; replacing
    /// the root here would strand a cursor opened by the preceding statement
    /// outside the current connection's accounting tree.
    pub fn configure(&self, quota: i64, oom_action: OomAction, tmp_storage_on_oom: bool) {
        self.session.set_bytes_limit(quota);
        let mut config = self.config.lock().unwrap();
        config.oom_action = oom_action;
        let refresh_disk = config.tmp_storage_on_oom != tmp_storage_on_oom;
        config.tmp_storage_on_oom = tmp_storage_on_oom;
        let storage = config.spill_storage.as_ref().map(Arc::clone);
        drop(config);
        if refresh_disk {
            refresh_global_disk_attachment(
                &self.disk_session,
                tmp_storage_on_oom,
                storage.as_ref(),
            );
        }
    }

    /// Gives the persistent roots the connection identity the front end just
    /// assigned. Existing children are statement-scoped and are never moved
    /// between connection ids.
    pub fn set_connection_id(&self, connection_id: u64) {
        self.session.session_id.store(connection_id, SeqCst);
        self.disk_session.session_id.store(connection_id, SeqCst);
    }

    /// Installs the server's immutable spill authority without replacing the
    /// session roots that already own open cursor accounting.
    pub fn set_spill_storage(&self, storage: Arc<SpillStorage>) {
        let mut config = self.config.lock().unwrap();
        config.spill_storage = Some(storage);
        let tmp_storage_on_oom = config.tmp_storage_on_oom;
        let storage = config.spill_storage.as_ref().map(Arc::clone);
        drop(config);
        refresh_global_disk_attachment(&self.disk_session, tmp_storage_on_oom, storage.as_ref());
    }

    /// Replaces the global arbitrator used by future statements without
    /// moving this connection's persistent tracker roots.
    pub fn set_mem_arbitrator(&self, arbitrator: Arc<tidb_util::memory::MemArbitrator>) {
        self.config.lock().unwrap().arbitrator = Some(arbitrator);
    }

    /// Updates only the future-statement spill decision.
    pub fn set_tmp_storage_on_oom(&self, enabled: bool) {
        let mut config = self.config.lock().unwrap();
        config.tmp_storage_on_oom = enabled;
        let storage = config.spill_storage.as_ref().map(Arc::clone);
        drop(config);
        refresh_global_disk_attachment(&self.disk_session, enabled, storage.as_ref());
    }

    /// Starts one statement with fresh OOM action and kill state while
    /// retaining the connection's accumulated cursor bytes.
    #[must_use]
    pub fn statement(&self) -> StatementMemory {
        self.statement_with_arbitration(Some(false), 0)
    }

    /// Starts one statement with its session-local global-memory-arbitration
    /// policy. `None` is Go's `tidb_mem_arbitrator_wait_averse=nolimit`: the
    /// statement deliberately bypasses the process arbitrator.
    #[must_use]
    pub fn statement_with_arbitration(
        &self,
        wait_averse: Option<bool>,
        reserve_size: i64,
    ) -> StatementMemory {
        let connection_id = self.session.session_id.load(SeqCst);
        let config = self.config.lock().unwrap().clone();
        let killer = install_oom_action(&self.session, config.oom_action, connection_id);
        if self
            .query_cancellation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .requested
        {
            killer.send_kill_signal(KillSignal::QueryInterrupted);
        }

        let stmt = Tracker::new(LABEL_FOR_SQL_TEXT, -1);
        stmt.session_id.store(connection_id, SeqCst);
        stmt.attach_to(&self.session);

        let disk_stmt = Tracker::new(LABEL_FOR_SQL_TEXT, -1);
        disk_stmt.session_id.store(connection_id, SeqCst);
        disk_stmt.attach_to(&self.disk_session);

        if let (Some(arbitrator), Some(wait_averse)) = (config.arbitrator.as_ref(), wait_averse) {
            if arbitrator.work_mode() != tidb_util::memory::ArbitratorWorkMode::Disable {
                let _ = stmt.init_mem_arbitrator(
                    Arc::clone(arbitrator),
                    Arc::clone(&self.session.killer),
                    tidb_util::memory::ArbitrationPriority::Medium,
                    wait_averse,
                    reserve_size.max(0),
                );
            }
        }

        StatementMemory {
            lifetime: Arc::new(StatementLifetime {
                stmt,
                disk_stmt,
                finished: AtomicBool::new(false),
            }),
            session: Arc::clone(&self.session),
            disk_session: Arc::clone(&self.disk_session),
            spill_storage: config.spill_storage,
            tmp_storage_on_oom: config.tmp_storage_on_oom,
            killer,
        }
    }

    /// Bytes retained by every statement and open cursor in this connection.
    #[must_use]
    pub fn bytes_consumed(&self) -> i64 {
        self.session.bytes_consumed()
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
    /// Shared final-clone cleanup for this statement's tracker children.
    /// Operators clone `StatementMemory`; only the last handle may detach.
    lifetime: Arc<StatementLifetime>,
    session: Arc<Tracker>,
    disk_session: Arc<Tracker>,
    spill_storage: Option<Arc<SpillStorage>>,
    /// `@@tidb_enable_tmp_storage_on_oom` (Go `vardef.EnableTmpStorageOnOOM`,
    /// default ON): whether an operator that can spill is allowed to, instead
    /// of failing the statement with 8175.
    tmp_storage_on_oom: bool,
    /// The statement's canonical SQL killer, shared by memory cancellation,
    /// `KILL QUERY`, connection shutdown, and blocking expression waits.
    killer: Arc<SqlKiller>,
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
        SessionMemory::new(quota, oom_action, connection_id).statement()
    }

    /// Sets `@@tidb_enable_tmp_storage_on_oom`. Go reads the sysvar in
    /// `SortExec.Open`; the budget carries it here so an operator does not
    /// need a session handle.
    #[must_use]
    pub fn with_tmp_storage_on_oom(mut self, enabled: bool) -> Self {
        self.tmp_storage_on_oom = enabled;
        self.refresh_global_disk_attachment();
        self
    }

    /// Installs the process spill-storage authority captured at server
    /// startup. This must happen before executor construction so every disk
    /// tracker and physical file shares one policy.
    #[must_use]
    pub fn with_spill_storage(mut self, storage: Arc<SpillStorage>) -> Self {
        self.disk_session.detach();
        self.spill_storage = Some(storage);
        self.refresh_global_disk_attachment();
        self
    }

    fn refresh_global_disk_attachment(&self) {
        refresh_global_disk_attachment(
            &self.disk_session,
            self.tmp_storage_on_oom,
            self.spill_storage.as_ref(),
        );
    }

    /// Whether spilling is enabled for this statement.
    #[must_use]
    pub fn tmp_storage_on_oom(&self) -> bool {
        self.tmp_storage_on_oom
    }

    /// The session root tracker, which carries the quota and is where Go
    /// registers a spill action (`MemTracker.FallbackOldAndSetNewAction`).
    #[must_use]
    pub fn session_tracker(&self) -> &Arc<Tracker> {
        &self.session
    }

    /// The statement tracker an operator attaches its own tracker to (Go
    /// `StmtCtx.MemTracker`).
    #[must_use]
    pub fn stmt_tracker(&self) -> &Arc<Tracker> {
        &self.lifetime.stmt
    }

    /// The session disk root, which Go exposes as `SessionVars.DiskTracker`.
    /// Long-lived cursor storage attaches directly here and retains this
    /// [`StatementMemory`] for the cursor's lifetime.
    #[must_use]
    pub fn session_disk_tracker(&self) -> &Arc<Tracker> {
        &self.disk_session
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
        tracker.attach_to(&self.lifetime.stmt);
        tracker
    }

    /// A fresh operator disk tracker attached to the statement disk root.
    #[must_use]
    pub fn operator_disk_tracker(&self, label: i64) -> Arc<Tracker> {
        let tracker = Tracker::new(label, -1);
        tracker
            .session_id
            .store(self.session.session_id.load(SeqCst), SeqCst);
        tracker.attach_to(&self.lifetime.disk_stmt);
        tracker
    }

    /// Immutable storage authority used by physical spill stores.
    ///
    /// Production sessions always install the server-owned authority. The
    /// lazy process-local fallback keeps standalone executor/unit construction
    /// safe without a mutable global configuration seam.
    #[must_use]
    pub fn spill_storage(&self) -> Arc<SpillStorage> {
        self.spill_storage
            .as_ref()
            .map_or_else(fallback_spill_storage, Arc::clone)
    }

    /// The explicitly installed server authority, without creating the
    /// standalone fallback. Statement-context rebuilds use this to preserve
    /// the session's startup policy across quota changes.
    #[must_use]
    pub(crate) fn configured_spill_storage(&self) -> Option<Arc<SpillStorage>> {
        self.spill_storage.as_ref().map(Arc::clone)
    }

    /// Whether the quota has been exceeded and the statement must stop, as
    /// the error Go's `SQLKiller.HandleSignal` yields.
    ///
    /// An accounting operator calls this immediately after each `Consume`.
    /// Under `LOG` it can never fail, which is exactly the captured behavior.
    pub fn check(&self) -> Result<(), ExecError> {
        match self.killer.get_kill_signal() {
            Some(KillSignal::QueryMemoryExceeded) => Err(ExecError::MemoryExceedForQuery {
                conn_id: self.killer.conn_id.load(SeqCst),
            }),
            Some(_) => self
                .killer
                .handle_signal()
                .map_or(Ok(()), |error| Err(ExecError::Killed(error.to_sql_error()))),
            None => Ok(()),
        }
    }

    /// Waits for a SQL `SLEEP` duration or this statement's canonical kill
    /// event, whichever happens first.
    #[must_use]
    pub fn sleep_for(&self, duration: std::time::Duration) -> bool {
        self.killer.wait_kill_event_timeout(duration)
    }

    /// Clears a handled standalone-query kill, as Go's `doSleep` does after
    /// observing the signal outside a table/DML statement.
    pub(crate) fn reset_kill_signal(&self) {
        self.killer.reset();
    }

    /// Ends the statement-scoped tracker/action lifetime after every source
    /// and operator has closed. Persistent cursor rows may remain attached to
    /// the session root; a later [`SessionMemory::statement`] installs fresh
    /// cancellation state over that retained accounting.
    pub fn finish_statement(&self) {
        self.lifetime.finish();
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

    #[derive(Default)]
    struct NoopMemStateRecorder;

    impl tidb_util::memory::RecordMemState for NoopMemStateRecorder {
        fn load(&self) -> Result<Option<tidb_util::memory::RuntimeMemStateV1>, String> {
            Ok(None)
        }

        fn store(&self, _: &tidb_util::memory::RuntimeMemStateV1) -> Result<(), String> {
            Ok(())
        }
    }

    fn test_mem_arbitrator() -> Arc<tidb_util::memory::MemArbitrator> {
        let arbitrator =
            tidb_util::memory::MemArbitrator::new(1024, 4, 3, 0, Box::new(NoopMemStateRecorder));
        assert!(arbitrator.auto_run(
            tidb_util::memory::MemArbitratorActions::default(),
            tidb_util::memory::DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
            4,
            tidb_util::memory::DEF_TASK_TICK_DUR,
        ));
        arbitrator.set_work_mode(tidb_util::memory::ArbitratorWorkMode::Standard);
        arbitrator
    }

    fn test_spill_storage(name: &str) -> Arc<SpillStorage> {
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: std::env::temp_dir()
                    .join(format!("tidb-executor-{name}-spill-{}", std::process::id())),
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .unwrap(),
        )
    }

    #[test]
    fn a_statement_root_registers_and_releases_its_global_arbitrator_pool() {
        let arbitrator = test_mem_arbitrator();
        let session = SessionMemory::new(4096, OomAction::Cancel, 97)
            .with_mem_arbitrator(Arc::clone(&arbitrator));
        let statement = session.statement();
        let operator = statement.operator_tracker(3);

        // `1024 / 1000 == 1`, so this crosses the source small-budget
        // threshold and must promote the statement into a root pool.
        operator.consume(2);
        let pool = arbitrator
            .find_root_pool(97)
            .entry
            .expect("statement must reach the process arbitrator");
        assert!(pool.pool().capacity() > 0);

        statement.finish_statement();
        assert_eq!(pool.pool().capacity(), 0);
        assert!(arbitrator.stop());
    }

    #[test]
    fn nolimit_skips_global_memory_arbitration_but_reserve_promotes_immediately() {
        let arbitrator = test_mem_arbitrator();
        let session = SessionMemory::new(4096, OomAction::Cancel, 98)
            .with_mem_arbitrator(Arc::clone(&arbitrator));

        let bypass = session.statement_with_arbitration(None, 0);
        bypass.operator_tracker(3).consume(8);
        assert!(arbitrator.find_root_pool(98).entry.is_none());
        bypass.finish_statement();

        let reserved = session.statement_with_arbitration(Some(true), 64);
        let pool = arbitrator
            .find_root_pool(98)
            .entry
            .expect("a reserved statement starts in a root pool");
        assert!(pool.pool().capacity() >= 64);
        reserved.finish_statement();
        assert!(arbitrator.stop());
    }

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
    fn memory_error_preserves_the_unsigned_connection_id() {
        let error = memory_exceed_for_query(u64::MAX);
        assert_eq!(error.code, 8175);
        assert!(
            error.message.ends_with("[conn=18446744073709551615]"),
            "{}",
            error.message
        );
    }

    #[test]
    fn a_session_gives_each_statement_fresh_cancellation_state() {
        let session = SessionMemory::new(1, OomAction::Cancel, 7);

        let first = session.statement();
        let first_op = first.operator_tracker(3);
        first_op.consume(1);
        assert!(matches!(
            first.check(),
            Err(ExecError::MemoryExceedForQuery { conn_id: 7 })
        ));
        first.finish_statement();
        assert_eq!(session.bytes_consumed(), 0);

        let second = session.statement();
        assert!(
            second.check().is_ok(),
            "the previous kill is statement-local"
        );
        let second_op = second.operator_tracker(3);
        second_op.consume(1);
        assert!(matches!(
            second.check(),
            Err(ExecError::MemoryExceedForQuery { conn_id: 7 })
        ));
    }

    #[test]
    fn session_tracker_defers_kill_to_the_typed_statement_boundary() {
        let session = SessionMemory::new(4096, OomAction::Cancel, 7);
        let cancellation = session.begin_query_cancellation();
        let statement = session.statement();
        cancellation.cancel();

        statement.operator_tracker(3).consume(1);
        match statement.check() {
            Err(ExecError::Killed(error)) => {
                assert_eq!(error.code, 1317);
                assert_eq!(error.state, "70100");
            }
            other => panic!("expected typed query interruption, got {other:?}"),
        }
    }

    #[test]
    fn closing_a_retained_cursor_cannot_remove_the_next_statement_action() {
        let session = SessionMemory::new(1, OomAction::Cancel, 7);
        let cursor = Tracker::new(tidb_util::memory::LABEL_FOR_CURSOR_FETCH, -1);
        let first = session.statement();
        cursor.attach_to(first.session_tracker());
        cursor.detach();
        first.finish_statement();

        let second = session.statement();
        let op = second.operator_tracker(3);
        op.consume(1);
        assert!(matches!(
            second.check(),
            Err(ExecError::MemoryExceedForQuery { conn_id: 7 })
        ));
    }

    #[test]
    fn retained_cursor_bytes_count_against_the_next_statement() {
        let session = SessionMemory::new(10, OomAction::Cancel, 7);
        let first = session.statement();
        let cursor = Tracker::new(tidb_util::memory::LABEL_FOR_CURSOR_FETCH, -1);
        cursor.attach_to(first.session_tracker());
        cursor.consume(6);
        first.finish_statement();
        assert_eq!(session.bytes_consumed(), 6);

        let second = session.statement();
        let op = second.operator_tracker(3);
        op.consume(4);
        assert!(matches!(
            second.check(),
            Err(ExecError::MemoryExceedForQuery { conn_id: 7 })
        ));
        cursor.detach();
        assert_eq!(session.bytes_consumed(), 4);
    }

    #[test]
    fn final_statement_handle_drop_detaches_its_accounting_tree() {
        let session = SessionMemory::new(10, OomAction::Cancel, 7);
        let statement = session.statement();
        let clone = statement.clone();
        let op = statement.operator_tracker(3);
        op.consume(4);
        drop(statement);
        assert_eq!(session.bytes_consumed(), 4, "one statement handle remains");
        drop(clone);
        assert_eq!(
            session.bytes_consumed(),
            0,
            "final handle detaches the tree"
        );

        let next = session.statement();
        assert!(next.check().is_ok());
        drop(op);
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

    #[test]
    fn statement_disk_usage_reaches_the_one_startup_global_tracker() {
        let storage = test_spill_storage("global-hierarchy");
        let mem = StatementMemory::new(4096, OomAction::Cancel, 7)
            .with_spill_storage(Arc::clone(&storage));
        let operator = mem.operator_disk_tracker(42);

        operator.consume(64);
        assert_eq!(mem.lifetime.disk_stmt.bytes_consumed(), 64);
        assert_eq!(mem.disk_session.bytes_consumed(), 64);
        assert_eq!(storage.global_tracker().bytes_consumed(), 64);

        operator.consume(-64);
        assert_eq!(storage.global_tracker().bytes_consumed(), 0);
        let path = storage.path().to_owned();
        drop(operator);
        drop(mem);
        drop(storage);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn disabling_tmp_storage_detaches_the_session_from_global_disk_quota() {
        let storage = test_spill_storage("disabled-hierarchy");
        let mem = StatementMemory::new(4096, OomAction::Cancel, 7)
            .with_spill_storage(Arc::clone(&storage))
            .with_tmp_storage_on_oom(false);
        let operator = mem.operator_disk_tracker(42);

        operator.consume(64);
        assert_eq!(mem.lifetime.disk_stmt.bytes_consumed(), 64);
        assert_eq!(mem.disk_session.bytes_consumed(), 64);
        assert_eq!(storage.global_tracker().bytes_consumed(), 0);
        let path = storage.path().to_owned();
        drop(operator);
        drop(mem);
        drop(storage);
        std::fs::remove_dir_all(path).unwrap();
    }
}
