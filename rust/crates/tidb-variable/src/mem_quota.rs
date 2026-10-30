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
//! The trackers and the killer are Go `SessionVars.MemTracker`,
//! `DiskTracker` and `SQLKiller` with the statement's `StmtCtx.MemTracker`
//! hung beneath (`pkg/sessionctx/variable`, which imports `stmtctx`); what
//! installs them is the executor side of Go's `ResetContextOfStmt`
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
//! WHICH OPERATORS ACCOUNT: the sort (`tidb_kv::sort`), and the WRITE path --
//! `UPDATE`, `DELETE`, `INSERT`/`REPLACE`, and the rows a foreign-key cascade
//! reads and rewrites -- through [`WriteMemory`], whose doc states what is
//! counted and how it differs from Go's chunk arithmetic. A read path other
//! than the sort still accounts nothing, so a `SELECT` under a small quota
//! runs to completion here where TiDB cancels it.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock};

use tidb_datatype::{estimated_mem_usage, Datum};
use tidb_memory::{
    ActionOnExceed, ArcAction, BaseOomAction, KillSignalTransport, LogOnExceed, Tracker,
    DEF_MEM_QUOTA_QUERY, DEF_PANIC_PRIORITY, LABEL_FOR_SESSION, LABEL_FOR_SQL_TEXT,
};
use tidb_spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};
use tidb_sqlkiller::{KillSignal, SqlKiller};

use tidb_vardef::OomAction;



/// Why [`StatementMemory::check`] stops a statement: the two outcomes of
/// Go `SQLKiller.HandleSignal` this layer tells apart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KillStop {
    /// `QueryMemoryExceeded`: `exeerrors.ErrMemoryExceedForQuery` (8175)
    /// for the connection Go's message names (`...try again.[conn=%d]`).
    MemoryExceedForQuery {
        /// The killer's connection id.
        conn_id: u64,
    },
    /// Any other pending kill signal, as the error the killer yields.
    Killed(tidb_mysql::errors::SqlError),
}

impl KillStop {
    /// The statement error this stop reports: Go `SQLKiller.HandleSignal`'s
    /// error, `exeerrors.ErrMemoryExceedForQuery` rendered from the ported
    /// catalog for a memory kill.
    #[must_use]
    pub fn to_mysql_error(self) -> tidb_terror::MysqlError {
        match self {
            // Go `sqlkiller`: `exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(connID)`.
            KillStop::MemoryExceedForQuery { conn_id } => tidb_terror::MysqlError::from_terror(
                &tidb_exeerrors::ERR_MEMORY_EXCEED_FOR_QUERY
                    .generate_with_stack_by_args(&[conn_id.into()]),
            ),
            KillStop::Killed(error) => tidb_terror::MysqlError::from_sql_error(error),
        }
    }
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
    query_cancellation: tidb_sqlkiller::QueryCancellation,
}

#[derive(Clone)]
struct SessionMemoryConfig {
    spill_storage: Option<Arc<SpillStorage>>,
    arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
    tmp_storage_on_oom: bool,
    oom_action: OomAction,
}

pub struct StatementLifetime {
    stmt: Arc<Tracker>,
    pub disk_stmt: Arc<Tracker>,
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
            query_cancellation: tidb_sqlkiller::QueryCancellation::default(),
        }
    }

    /// The session's SQL killer (Go `SessionVars.SQLKiller`), shared by every
    /// statement this session runs.
    #[must_use]
    pub fn sql_killer(&self) -> &Arc<SqlKiller> {
        &self.session.killer
    }

    /// Starts the cancellation lifetime for one wire command.
    #[must_use]
    pub fn begin_query_cancellation(&self) -> tidb_sqlkiller::StatementCancellation {
        self.query_cancellation.begin(Arc::clone(&self.session.killer))
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
    pub fn with_mem_arbitrator(self, arbitrator: Arc<tidb_memory::MemArbitrator>) -> Self {
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
    pub fn set_mem_arbitrator(&self, arbitrator: Arc<tidb_memory::MemArbitrator>) {
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
        self.statement_with_arbitration(Some(false), 0, "")
    }

    /// Starts one statement with its session-local global-memory-arbitration
    /// policy. `None` is Go's `tidb_mem_arbitrator_wait_averse=nolimit`: the
    /// statement deliberately bypasses the process arbitrator.
    /// Whether statements register with the process memory arbitrator
    /// (Go: `GlobalMemArbitrator() != nil && mode != disable`).
    pub fn arbitrator_enabled(&self) -> bool {
        self.config
            .lock()
            .unwrap()
            .arbitrator
            .as_ref()
            .is_some_and(|arbitrator| {
                arbitrator.work_mode() != tidb_memory::ArbitratorWorkMode::Disable
            })
    }

    #[must_use]
    pub fn statement_with_arbitration(
        &self,
        wait_averse: Option<bool>,
        reserve_size: i64,
        digest_key: &str,
    ) -> StatementMemory {
        let connection_id = self.session.session_id.load(SeqCst);
        let config = self.config.lock().unwrap().clone();
        let killer = install_oom_action(&self.session, config.oom_action, connection_id);
        if self.query_cancellation.requested() {
            killer.send_kill_signal(KillSignal::QueryInterrupted);
        }

        let stmt = Tracker::new(LABEL_FOR_SQL_TEXT, -1);
        stmt.session_id.store(connection_id, SeqCst);
        stmt.attach_to(&self.session);

        let disk_stmt = Tracker::new(LABEL_FOR_SQL_TEXT, -1);
        disk_stmt.session_id.store(connection_id, SeqCst);
        disk_stmt.attach_to(&self.disk_session);

        if let (Some(arbitrator), Some(wait_averse)) = (config.arbitrator.as_ref(), wait_averse) {
            if arbitrator.work_mode() != tidb_memory::ArbitratorWorkMode::Disable {
                let _ = stmt.init_mem_arbitrator(
                    Arc::clone(arbitrator),
                    Arc::clone(&self.session.killer),
                    tidb_memory::ArbitrationPriority::Medium,
                    wait_averse,
                    reserve_size.max(0),
                    digest_key,
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
            coprocessor_scope: None,
        }
    }

    /// Bytes retained by every statement and open cursor in this connection.
    #[must_use]
    pub fn bytes_consumed(&self) -> i64 {
        self.session.bytes_consumed()
    }

    /// The persistent memory root stored in `sessmgr.ProcessInfo`.
    #[must_use]
    pub fn session_tracker(&self) -> &Arc<Tracker> {
        &self.session
    }

    /// The persistent disk root stored in `sessmgr.ProcessInfo`.
    #[must_use]
    pub fn session_disk_tracker(&self) -> &Arc<Tracker> {
        &self.disk_session
    }
}

/// One statement's memory budget: the session tracker holding
/// `tidb_mem_quota_query` and the statement tracker every operator attaches
/// to.
///
/// Cloning shares the same trackers, so the handle can be copied to each
/// operator that accounts; it is created ONCE per statement, by
/// [`tidb_kv::StmtContext`]'s single constructor.
#[derive(Clone)]
pub struct StatementMemory {
    /// Shared final-clone cleanup for this statement's tracker children.
    /// Operators clone `StatementMemory`; only the last handle may detach.
    pub lifetime: Arc<StatementLifetime>,
    session: Arc<Tracker>,
    pub disk_session: Arc<Tracker>,
    spill_storage: Option<Arc<SpillStorage>>,
    /// `@@tidb_enable_tmp_storage_on_oom` (Go `vardef.EnableTmpStorageOnOOM`,
    /// default ON): whether an operator that can spill is allowed to, instead
    /// of failing the statement with 8175.
    tmp_storage_on_oom: bool,
    /// The statement's canonical SQL killer, shared by memory cancellation,
    /// `KILL QUERY`, connection shutdown, and blocking expression waits.
    killer: Arc<SqlKiller>,
    coprocessor_scope: Option<Arc<Mutex<Arc<tidb_txnkv::CancelHandle>>>>,
}

impl std::fmt::Debug for StatementMemory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatementMemory")
            .field("bytes_consumed", &self.bytes_consumed())
            .finish_non_exhaustive()
    }
}

/// The statement an MPP call runs for: its killer is the call's `ctx`, and
/// its tracker holds the packets the call buffers.
impl tidb_txnkv::MppContext for StatementMemory {
    fn check(&self) -> Result<(), tidb_terror::MysqlError> {
        StatementMemory::check(self).map_err(KillStop::to_mysql_error)
    }

    fn consume(&self, bytes: i64) {
        self.stmt_tracker().consume(bytes);
    }
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
    /// A child execution scope, independent of the statement's SQL killer.
    /// All worker clones see its current generation across executor reopen.
    pub fn with_coprocessor_worker_scope(mut self) -> Self {
        self.coprocessor_scope = Some(Arc::new(Mutex::new(Arc::new(
            tidb_txnkv::CancelHandle::default(),
        ))));
        self
    }
    /// New request-local child. Closing a response cancels only this child;
    /// closing its Apply owner cancels every outstanding sibling request.
    pub fn coprocessor_request_cancellation(&self) -> Arc<tidb_txnkv::CancelHandle> {
        self.coprocessor_scope.as_ref().map_or_else(
            || Arc::new(tidb_txnkv::CancelHandle::default()),
            |scope| {
                scope
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .request_child()
            },
        )
    }
    /// Rotate only after all workers from the previous Open have joined.
    pub fn renew_coprocessor_worker_scope(&self) {
        if let Some(scope) = &self.coprocessor_scope {
            *scope
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Arc::new(tidb_txnkv::CancelHandle::default());
        }
    }
    pub fn cancel_coprocessor_workers(&self) {
        if let Some(scope) = &self.coprocessor_scope {
            scope
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .cancel();
        }
    }

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
        self.set_tmp_storage_on_oom(enabled);
        self
    }

    /// Changes spill policy without replacing the statement's tracker owner.
    pub fn set_tmp_storage_on_oom(&mut self, enabled: bool) {
        self.tmp_storage_on_oom = enabled;
        self.refresh_global_disk_attachment();
    }

    /// Installs the process spill-storage authority captured at server
    /// startup. This must happen before executor construction so every disk
    /// tracker and physical file shares one policy.
    #[must_use]
    pub fn with_spill_storage(mut self, storage: Arc<SpillStorage>) -> Self {
        self.set_spill_storage(storage);
        self
    }

    /// Changes the spill authority while retaining the statement lifetime.
    pub fn set_spill_storage(&mut self, storage: Arc<SpillStorage>) {
        self.disk_session.detach();
        self.spill_storage = Some(storage);
        self.refresh_global_disk_attachment();
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

    /// The statement's canonical SQL killer.
    #[must_use]
    pub fn sql_killer(&self) -> &Arc<SqlKiller> {
        &self.killer
    }

    /// The statement tracker an operator attaches its own tracker to (Go
    /// `StmtCtx.MemTracker`).
    #[must_use]
    pub fn stmt_tracker(&self) -> &Arc<Tracker> {
        &self.lifetime.stmt
    }

    /// The statement disk tracker an operator attaches its disk tracker to
    /// (Go `StmtCtx.DiskTracker`).
    #[must_use]
    pub fn stmt_disk_tracker(&self) -> &Arc<Tracker> {
        &self.lifetime.disk_stmt
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

    /// Go's `e.memTracker` of an executor whose workers attach their own
    /// trackers to it and detach them when they finish: the statement's
    /// tracker labelled `label`, created on first use and kept on the
    /// statement tracker for its lifetime.
    #[must_use]
    pub fn persistent_operator_tracker(&self, label: i64) -> Arc<Tracker> {
        self.lifetime
            .stmt
            .search_tracker(label)
            .filter(|tracker| tracker.label() == label && !Arc::ptr_eq(tracker, &self.lifetime.stmt))
            .unwrap_or_else(|| self.operator_tracker(label))
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
    pub fn configured_spill_storage(&self) -> Option<Arc<SpillStorage>> {
        self.spill_storage.as_ref().map(Arc::clone)
    }

    /// Whether the quota has been exceeded and the statement must stop, as
    /// the error Go's `SQLKiller.HandleSignal` yields.
    ///
    /// An accounting operator calls this immediately after each `Consume`.
    /// Under `LOG` it can never fail, which is exactly the captured behavior.
    pub fn check(&self) -> Result<(), KillStop> {
        match self.killer.get_kill_signal() {
            KillSignal::QueryMemoryExceeded => Err(KillStop::MemoryExceedForQuery {
                conn_id: self.killer.conn_id.load(SeqCst),
            }),
            KillSignal::UnspecifiedKillSignal => Ok(()),
            _ => self
                .killer
                .handle_signal()
                .map_or(Ok(()), |error| Err(KillStop::Killed(error.to_sql_error()))),
        }
    }


    /// Waits for a SQL `SLEEP` duration or this statement's canonical kill
    /// signal, polling every 10ms like Go's `doSleep`.
    #[must_use]
    pub fn sleep_for(&self, duration: std::time::Duration) -> bool {
        let started = std::time::Instant::now();
        let interval = std::time::Duration::from_millis(10);
        loop {
            let elapsed = started.elapsed();
            if elapsed >= duration {
                return false;
            }
            std::thread::sleep(interval.min(duration - elapsed));
            if started.elapsed() >= duration {
                return false;
            }
            if self.killer.handle_signal().is_some() {
                return true;
            }
        }
    }

    /// Clears a handled standalone-query kill, as Go's `doSleep` does after
    /// observing the signal outside a table/DML statement.
    pub fn reset_kill_signal(&self) {
        self.killer.reset();
    }

    /// Whether this statement's tracker lifetime has already retired.
    /// Rebuilding a context must share an active lifetime, while a completed
    /// materialization must not be reused as a new statement's tracker.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.lifetime.finished.load(SeqCst)
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
    pub fn account_row(&self, row: &[Datum]) -> Result<(), KillStop> {
        self.consume(estimated_mem_usage(row, 1));
        self.memory.check()
    }

    /// Go `types.EstimatedMemUsage(rows[0], len(rows))`: the first row's
    /// usage taken as every row's, which is how Go prices a staged row batch.
    ///
    /// An empty `rows` accounts nothing, as Go's `if len(rows) != 0` guard
    /// does.
    pub fn account_rows(&self, rows: &[Vec<Datum>]) -> Result<(), KillStop> {
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
mod kill_stop_tests {
    use super::*;

    #[test]
    fn a_memory_kill_reports_the_killers_connection() {
        let memory = StatementMemory::new(1, OomAction::Cancel, 9);
        memory.killer.send_kill_signal(KillSignal::QueryMemoryExceeded);
        assert_eq!(
            memory.check(),
            Err(KillStop::MemoryExceedForQuery { conn_id: 9 })
        );
    }
}
