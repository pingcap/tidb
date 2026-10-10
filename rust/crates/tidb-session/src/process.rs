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

//! The process list: what every live connection of one server is doing, and
//! the handle a `KILL` uses to reach one of them.
//!
//! This is the seam of Go's `sessmgr.Manager` (`pkg/session/sessmgr`): the
//! server front end owns a registry of `ProcessInfo` records, `SHOW
//! PROCESSLIST` reads it (Go `ShowExec.fetchShowProcessList`) and `KILL`
//! reaches one entry through it (Go `SimpleExec.executeKillStmt` ->
//! `sessmgr.KillWithNormalCloseMsg`).
//!
//! Layering: the kill mechanism itself (a connection's cancellation carrier
//! and its socket) belongs to the server crate, which sits ABOVE this one, so
//! the registry stores it behind the [`ProcessKillTarget`] trait. A session
//! therefore kills a peer without knowing anything about sockets.
//!
//! Remote server-ID-routed KILL remains a separate integration obligation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, Utc};
use tidb_util::memory::Tracker;
use tidb_util::memoryusagealarm::{OOMAlarmVariablesInfo, ProcessInfo, SessionManager};

const MAX_TRANSACTION_STMT_HISTORY: usize = 50;

/// One live connection's kill mechanism, owned by the server front end.
///
/// Go's `KillWithNormalCloseMsg` splits exactly this way: `KILL QUERY` only
/// cancels the running statement and leaves the connection open, while `KILL`
/// / `KILL CONNECTION` also ends the connection.
pub trait ProcessKillTarget: Send + Sync {
    /// Cancels the statement currently running on the connection, leaving the
    /// connection itself open (Go `KILL QUERY`).
    fn cancel_query(&self);

    /// Cancels the running statement and ends the connection (Go `KILL` /
    /// `KILL CONNECTION`).
    fn kill_connection(&self);

    /// Cancels the running statement with a watchdog's own signal (Go server
    /// `killQuery`), so it fails as `max_execution_time` or a runaway rule
    /// rather than as `KILL QUERY`.
    fn cancel_query_with(&self, signal: tidb_util::sqlkiller::KillSignal) {
        let _ = signal;
        self.cancel_query();
    }
}

/// One snapshot shared by SHOW and information_schema.PROCESSLIST.
#[derive(Clone, Debug, Default)]
pub struct ProcessRow {
    /// Connection identity (`Id`).
    pub id: u64,
    /// Authenticated user (`User`), empty when the front end has none.
    pub user: String,
    /// Client address (`Host`), empty for a session with no front end.
    pub host: String,
    /// Selected schema (`db`), empty meaning SQL NULL.
    pub db: String,
    /// Command kind (`Command`); this tier only ever reports `Query` or
    /// `Sleep`, which are the two states a text-protocol connection has.
    pub command: String,
    /// Seconds the current command has been running (`Time`).
    pub time: u64,
    /// Session status text (`State`), Go `serverStatus2Str`.
    pub state: String,
    /// The statement currently running (`Info`), `None` for an idle
    /// connection, which Go reports as SQL NULL.
    pub info: Option<String>,
    /// Published SQL digest, including the original prepared statement digest.
    pub digest: String,
    /// Bytes consumed by the target's retained memory tracker.
    pub mem_bytes: i64,
    /// Bytes consumed by the target's retained disk tracker.
    pub disk_bytes: i64,
    /// Target transaction timestamp, zero when no transaction is published.
    pub cur_txn_start_ts: u64,
    /// Target connection's resource group.
    pub resource_group: String,
    /// Target connection's session alias.
    pub session_alias: String,
    /// Statement affected rows; absent when no statement trackers are attached.
    pub affected_rows: Option<u64>,
}

/// One live transaction exposed by `information_schema.TIDB_TRX`.
#[derive(Clone, Debug)]
pub struct TransactionRow {
    /// Transaction start timestamp (TSO).
    pub start_ts: u64,
    /// Digest of the statement currently running, if any.
    pub current_sql_digest: Option<String>,
    /// Source transaction-running-state label.
    pub state: &'static str,
    /// Lock-wait start, absent outside `LockWaiting`.
    pub waiting_start: Option<DateTime<Utc>>,
    /// Number of entries in the transaction memory buffer.
    pub mem_buffer_keys: u64,
    /// Bytes consumed by the transaction memory buffer.
    pub mem_buffer_bytes: i64,
    /// Owning connection ID.
    pub session_id: u64,
    /// Login username.
    pub user: String,
    /// Current schema.
    pub db: String,
    /// Digests executed by this transaction.
    pub all_sql_digests: Vec<String>,
    /// Physical table IDs touched by this transaction.
    pub related_table_ids: Vec<i64>,
}

struct TransactionEntry {
    start_ts: u64,
    /// Go `ProcessInfo.CurTxnCreateTime`.
    created: Instant,
    current_sql_digest: Option<String>,
    state: &'static str,
    waiting_start: Option<DateTime<Utc>>,
    mem_buffer_keys: u64,
    mem_buffer_bytes: i64,
    all_sql_digests: Vec<String>,
    related_table_ids: std::collections::HashSet<i64>,
}

/// Go `ProcessInfo.ToRowForShow`: without `FULL`, `Info` is truncated with
/// `fmt.Sprintf("%.100v", pi.Info)`, i.e. to its first 100 characters.
pub const PROCESS_INFO_SHOW_LIMIT: usize = 100;

/// Truncates a statement to what `SHOW PROCESSLIST` (no `FULL`) reports.
///
/// Go truncates by `%.100v`, which counts RUNES, not bytes, so this does too.
#[must_use]
pub fn truncate_process_info(info: &str, full: bool) -> String {
    if full {
        return info.to_owned();
    }
    match info.char_indices().nth(PROCESS_INFO_SHOW_LIMIT) {
        Some((end, _)) => info[..end].to_owned(),
        None => info.to_owned(),
    }
}

struct ProcessEntry {
    user: String,
    host: String,
    db: String,
    state: String,
    info: Option<String>,
    digest: String,
    /// When the current command started, which `Time` counts from.
    since: Instant,
    started_at: DateTime<Utc>,
    mem_tracker: Option<Arc<Tracker>>,
    disk_tracker: Option<Arc<Tracker>>,
    cur_txn_start_ts: u64,
    resource_group_name: String,
    session_alias: String,
    redact_sql: tidb_parser::RedactMode,
    affected_rows: u64,
    oom_alarm_variables_info: OOMAlarmVariablesInfo,
    kill: Option<Arc<dyn ProcessKillTarget>>,
    transaction: Option<TransactionEntry>,
    external_transaction_owner: bool,
    transaction_history: Arc<tidb_exec::txn_summary::TransactionHistoryRecorder>,
    process_plan_info: Option<Arc<Mutex<tidb_executor::ProcessPlanInfo>>>,
    /// An internal session's entry: its statements are restricted SQL (Go
    /// `StmtCtx.InRestrictedSQL`).
    restricted: bool,
    /// Result-set owners retaining the current command after execution has
    /// returned. Go clears `ProcessInfo` only when the server command ends,
    /// after `writeResultSet` has drained the executor.
    statement_holds: usize,
}

type SharedProcessEntry = Arc<Mutex<ProcessEntry>>;

impl ProcessEntry {
    fn publish_statement(&mut self, sql: &str, state: &str) {
        self.publish_statement_with_digest(sql, None, state);
    }

    /// `digest` is the statement's known digest -- a prepared statement's,
    /// fixed at PREPARE like Go's `PlanCacheStmt.SQLDigest` -- or `None` to
    /// derive it from `sql` here.
    fn publish_statement_with_digest(&mut self, sql: &str, digest: Option<&str>, state: &str) {
        // Go SetProcessInfo reuses StatementContext.SQLDigest when planning
        // republishes the active command. LazyTxn.onStmtStart records that
        // execution once, independently of process-list publication.
        let same_held_statement = self.statement_holds > 0 && self.info.as_deref() == Some(sql);
        if !same_held_statement {
            self.info = Some(sql.to_owned());
            match digest {
                Some(digest) => {
                    self.digest.clear();
                    self.digest.push_str(digest);
                }
                None => self.digest = crate::normalize_statement_digest(sql).1.to_string(),
            }
            self.since = Instant::now();
            self.started_at = Utc::now();
        }
        self.state = state.to_owned();
        self.affected_rows = 0;
        if let Some(transaction) = &mut self.transaction {
            transaction.state = "Running";
            transaction.current_sql_digest = (!self.digest.is_empty()).then(|| self.digest.clone());
        }
    }

    fn statement_started(&mut self, sql: &str, state: &str) {
        self.statement_started_with_digest(sql, None, state);
    }

    fn statement_started_with_digest(&mut self, sql: &str, digest: Option<&str>, state: &str) {
        self.publish_statement_with_digest(sql, digest, state);
        if let Some(transaction) = &mut self.transaction {
            if !self.digest.is_empty()
                && transaction.all_sql_digests.len() < MAX_TRANSACTION_STMT_HISTORY
            {
                transaction.all_sql_digests.push(self.digest.clone());
            }
        }
    }

    fn statement_finished(&mut self, db: &str, state: &str) {
        if self.statement_holds > 0 {
            return;
        }
        self.info = None;
        self.digest.clear();
        self.since = Instant::now();
        self.started_at = Utc::now();
        self.db = db.to_owned();
        self.state = state.to_owned();
        if let Some(transaction) = &mut self.transaction {
            transaction.state = "Idle";
            transaction.current_sql_digest = None;
            transaction.waiting_start = None;
        }
    }

    fn activate_transaction(&mut self, start_ts: u64) {
        if start_ts == 0 || start_ts == u64::MAX || self.transaction.is_some() {
            return;
        }
        let digest = (!self.digest.is_empty()).then(|| self.digest.clone());
        self.transaction = Some(TransactionEntry {
            start_ts,
            created: Instant::now(),
            current_sql_digest: digest.clone(),
            state: if self.info.is_some() {
                "Running"
            } else {
                "Idle"
            },
            waiting_start: None,
            mem_buffer_keys: 0,
            mem_buffer_bytes: 0,
            all_sql_digests: digest.into_iter().collect(),
            related_table_ids: std::collections::HashSet::new(),
        });
        self.cur_txn_start_ts = start_ts;
    }

    fn finish_transaction(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            self.transaction_history
                .on_transaction_end(transaction.start_ts, transaction.all_sql_digests);
        }
        self.cur_txn_start_ts = 0;
    }

    fn release_statement(&mut self, db: &str, state: &str) {
        self.statement_holds = self.statement_holds.saturating_sub(1);
        self.statement_finished(db, state);
    }
}

/// Go SessionManager's TLS configuration operation. The server retains the
/// material/transport owner; SQL supplies only the reload policy.
pub trait TlsManager: Send + Sync {
    /// Reload certificates and publish the new configuration on success.
    fn reload_tls(&self, no_rollback_on_error: bool) -> Result<(), String>;
}

/// The server's live connection registry, shared by every connection thread.
///
/// Cloning shares one registry, as every session of one TiDB instance sees
/// one `sessmgr.Manager`. Like Go session.processInfo, each connection owns
/// its publication state; the directory lock never covers entry updates.
#[derive(Clone)]
pub struct ProcessRegistry {
    entries: Arc<Mutex<HashMap<u64, SharedProcessEntry>>>,
    internal: Arc<Mutex<HashMap<usize, SharedProcessEntry>>>,
    systems: Arc<Mutex<HashMap<u64, SystemProcess>>>,
    tls: Arc<Mutex<Option<Arc<dyn TlsManager>>>>,
    transaction_history: Arc<tidb_exec::txn_summary::TransactionHistoryRecorder>,
}

impl Default for ProcessRegistry {
    fn default() -> Self {
        Self {
            entries: Arc::default(),
            internal: Arc::default(),
            systems: Arc::default(),
            tls: Arc::default(),
            transaction_history: Arc::clone(&tidb_exec::txn_summary::RECORDER),
        }
    }
}

impl std::fmt::Debug for ProcessRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessRegistry")
            .finish_non_exhaustive()
    }
}

impl ProcessRegistry {
    #[cfg(test)]
    pub(crate) fn with_transaction_history(
        history: Arc<tidb_exec::txn_summary::TransactionHistoryRecorder>,
    ) -> Self {
        Self {
            transaction_history: history,
            ..Self::default()
        }
    }

    /// Completed transaction rows from this process's shared recorder.
    pub fn transaction_history_rows(&self) -> Vec<Vec<tidb_datatype::Datum>> {
        self.transaction_history.rows()
    }

    /// Attach this server's shared TLS configuration owner.
    pub fn set_tls_manager(&self, manager: Arc<dyn TlsManager>) {
        *self
            .tls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(manager);
    }

    /// Go ALTER INSTANCE uses the same manager as the connection accept path.
    pub(crate) fn reload_tls(&self, no_rollback_on_error: bool) -> Result<(), String> {
        let manager = self
            .tls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| "TLS reload requires a running server".to_owned())?;
        manager.reload_tls(no_rollback_on_error)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, SharedProcessEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn with_entry<R>(&self, id: u64, access: impl FnOnce(&mut ProcessEntry) -> R) -> Option<R> {
        let entry = self.lock().get(&id).cloned().or_else(|| {
            self.systems
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&id)
                .map(|process| Arc::clone(&process.entry))
        })?;
        let mut entry = entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Some(access(&mut entry))
    }

    fn client_entry_snapshot(&self) -> Vec<(u64, SharedProcessEntry)> {
        self.lock()
            .iter()
            .map(|(&id, entry)| (id, Arc::clone(entry)))
            .collect()
    }

    fn entry_snapshot(&self) -> Vec<(u64, SharedProcessEntry)> {
        let mut entries = self.client_entry_snapshot();
        let clients = entries
            .iter()
            .map(|(id, _)| *id)
            .collect::<std::collections::HashSet<_>>();
        entries.extend(
            self.systems
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|(id, _)| !clients.contains(id))
                .map(|(&id, process)| (id, Arc::clone(&process.entry))),
        );
        entries
    }

    /// Go SysProcesses.Track: publish the actual internal session, rejecting an occupied ID.
    pub fn track_system_process(&self, id: u64, process: SystemProcess) -> Result<(), String> {
        let mut systems = self
            .systems
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if systems
            .get(&id)
            .is_some_and(|old| !Arc::ptr_eq(&old.entry, &process.entry))
        {
            return Err(format!("The ID is in use: {id}"));
        }
        process.cancellation.reset();
        systems.insert(id, process);
        Ok(())
    }

    /// Retires only task publication; the pooled session remains reusable.
    pub fn untrack_system_process(&self, id: u64) {
        let process = self
            .systems
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        if let Some(process) = process {
            process.cancellation.reset();
        }
    }

    /// Go GetInternalSessionStartTSList, separate from visible clients/system tasks.
    pub fn internal_session_start_ts(&self) -> Vec<u64> {
        let excluded = self
            .systems
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(id, _)| {
                tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(**id)
            })
            .map(|(_, process)| Arc::as_ptr(&process.entry) as usize)
            .collect::<std::collections::HashSet<_>>();
        let entries = self
            .internal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(key, _)| !excluded.contains(key))
            .map(|(_, entry)| Arc::clone(entry))
            .collect::<Vec<_>>();
        entries
            .into_iter()
            .filter_map(|entry| {
                let ts = entry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .cur_txn_start_ts;
                (ts != 0).then_some(ts)
            })
            .collect()
    }

    /// Registers one connection and returns the guard that removes it again.
    ///
    /// The guard is what makes the list honest: a connection leaves the
    /// process list exactly when its session is dropped, whatever ended it.
    pub fn register(
        &self,
        id: u64,
        user: String,
        host: String,
        db: String,
        kill: Option<Arc<dyn ProcessKillTarget>>,
    ) -> ProcessGuard {
        let entry = Arc::new(Mutex::new(ProcessEntry {
            user,
            host,
            db,
            state: String::new(),
            info: None,
            digest: String::new(),
            since: Instant::now(),
            started_at: Utc::now(),
            mem_tracker: None,
            disk_tracker: None,
            cur_txn_start_ts: 0,
            resource_group_name: "default".to_owned(),
            session_alias: String::new(),
            redact_sql: tidb_parser::RedactMode::Disabled,
            affected_rows: 0,
            oom_alarm_variables_info: OOMAlarmVariablesInfo::default(),
            kill,
            transaction: None,
            external_transaction_owner: false,
            transaction_history: Arc::clone(&self.transaction_history),
            process_plan_info: None,
            restricted: false,
            statement_holds: 0,
        }));
        self.lock().insert(id, Arc::clone(&entry));
        ProcessGuard {
            registry: self.clone(),
            internal_owner: None,
            id,
            entry,
        }
    }

    /// Internal sessions retain transaction history without entering the client list.
    pub fn register_internal(&self, id: u64, db: String) -> ProcessGuard {
        let mut guard = Self {
            transaction_history: Arc::clone(&self.transaction_history),
            ..Self::default()
        }
        .register(id, String::new(), String::new(), db, None);
        guard
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .restricted = true;
        self.internal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(Arc::as_ptr(&guard.entry) as usize, Arc::clone(&guard.entry));
        guard.internal_owner = Some(self.clone());
        guard
    }

    /// Records one execution in transaction history and publishes its process
    /// information. Retaining a result-set guard only publishes, without
    /// recording another execution.
    pub fn statement_started(&self, id: u64, sql: &str, state: &str) {
        self.with_entry(id, |entry| entry.statement_started(sql, state));
    }

    /// [`Self::statement_started`] with the statement's digest already
    /// computed by the caller, so the process list does not normalize the
    /// statement text a second time.
    pub fn statement_started_with_digest(
        &self,
        id: u64,
        sql: &str,
        digest: Option<&str>,
        state: &str,
    ) {
        self.with_entry(id, |entry| {
            entry.statement_started_with_digest(sql, digest, state);
        });
    }

    pub(crate) fn statement_metadata(
        &self,
        id: u64,
        cur_txn_start_ts: u64,
        resource_group_name: String,
        session_alias: String,
        redact_sql: tidb_parser::RedactMode,
        oom_alarm_variables_info: OOMAlarmVariablesInfo,
    ) {
        self.with_entry(id, |entry| {
            entry.cur_txn_start_ts = cur_txn_start_ts;
            entry.resource_group_name = resource_group_name;
            entry.session_alias = session_alias;
            entry.redact_sql = redact_sql;
            entry.oom_alarm_variables_info = oom_alarm_variables_info;
        });
    }

    /// Publishes the running statement's resource group, which a statement
    /// hint may override once the statement is parsed.
    pub(crate) fn statement_resource_group(&self, id: u64, resource_group_name: String) {
        self.with_entry(id, |entry| {
            entry.resource_group_name = resource_group_name;
        });
    }

    pub(crate) fn statement_affected_rows(&self, id: u64, affected_rows: u64) {
        self.with_entry(id, |entry| {
            entry.affected_rows = affected_rows;
        });
    }

    /// Records that a connection finished its statement: `Info` becomes NULL,
    /// and `db` and `State` are refreshed, since `USE` may have just changed
    /// the schema and the statement may have opened or closed a transaction.
    pub fn statement_finished(&self, id: u64, db: &str, state: &str) {
        self.with_entry(id, |entry| entry.statement_finished(db, state));
    }

    /// Publishes a newly activated transaction for `TIDB_TRX`.
    pub fn transaction_started(&self, id: u64, start_ts: u64) {
        self.with_entry(id, |entry| {
            if !entry.external_transaction_owner {
                entry.finish_transaction();
                entry.activate_transaction(start_ts);
            }
        });
    }

    /// Local sessions finish here; a physical owner finishes after storage.
    pub fn transaction_finished(&self, id: u64) {
        self.with_entry(id, |entry| {
            if !entry.external_transaction_owner {
                entry.finish_transaction();
            }
        });
    }

    /// Changes the source transaction-running-state label.
    pub fn transaction_state(&self, id: u64, state: &'static str) {
        self.with_entry(id, |entry| {
            if let Some(transaction) = &mut entry.transaction {
                transaction.state = state;
                transaction.waiting_start = (state == "LockWaiting").then(chrono::Utc::now);
            }
        });
    }

    /// Publishes the current transaction MemBuffer length and native memory
    /// footprint. Go updates these from `LazyTxn.Len` and the MemDB footprint
    /// hook while the transaction remains active.
    pub fn transaction_buffer_metrics(&self, id: u64, keys: u64, bytes: i64) {
        self.with_entry(id, |entry| {
            if let Some(transaction) = &mut entry.transaction {
                transaction.mem_buffer_keys = keys;
                transaction.mem_buffer_bytes = bytes;
            }
        });
    }

    /// Records one physical table used by the live transaction.
    pub fn transaction_related_table(&self, id: u64, table_id: i64) {
        self.with_entry(id, |entry| {
            if let Some(transaction) = &mut entry.transaction {
                transaction.related_table_ids.insert(table_id);
            }
        });
    }

    /// Returns every live transaction in stable connection-ID order.
    #[must_use]
    pub fn transaction_snapshot(&self) -> Vec<TransactionRow> {
        let mut rows = self
            .client_entry_snapshot()
            .into_iter()
            .filter_map(|(id, entry)| {
                let entry = entry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let transaction = entry.transaction.as_ref()?;
                Some(TransactionRow {
                    start_ts: transaction.start_ts,
                    current_sql_digest: transaction.current_sql_digest.clone(),
                    state: transaction.state,
                    waiting_start: transaction.waiting_start,
                    mem_buffer_keys: transaction.mem_buffer_keys,
                    mem_buffer_bytes: transaction.mem_buffer_bytes,
                    session_id: id,
                    user: entry.user.clone(),
                    db: entry.db.clone(),
                    all_sql_digests: transaction.all_sql_digests.clone(),
                    related_table_ids: transaction.related_table_ids.iter().copied().collect(),
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.session_id);
        rows
    }

    /// Every live connection, ordered by identity so the list is stable.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ProcessRow> {
        let now = Instant::now();
        let mut rows: Vec<ProcessRow> = self
            .entry_snapshot()
            .into_iter()
            .map(|(id, entry)| {
                let entry = entry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ProcessRow {
                    id,
                    user: entry.user.clone(),
                    host: entry.host.clone(),
                    db: entry.db.clone(),
                    // Go reports `Query` while a statement runs and `Sleep` for a
                    // connection waiting on its next command.
                    command: if entry.info.is_some() {
                        "Query".to_owned()
                    } else {
                        "Sleep".to_owned()
                    },
                    time: now.saturating_duration_since(entry.since).as_secs(),
                    state: entry.state.clone(),
                    info: entry.info.clone(),
                    digest: entry.digest.clone(),
                    mem_bytes: entry
                        .mem_tracker
                        .as_ref()
                        .map_or(0, |tracker| tracker.bytes_consumed()),
                    disk_bytes: entry
                        .disk_tracker
                        .as_ref()
                        .map_or(0, |tracker| tracker.bytes_consumed()),
                    cur_txn_start_ts: entry.cur_txn_start_ts,
                    resource_group: entry.resource_group_name.clone(),
                    session_alias: entry.session_alias.clone(),
                    affected_rows: (entry.mem_tracker.is_some() || entry.disk_tracker.is_some())
                        .then_some(entry.affected_rows),
                }
            })
            .collect();
        rows.sort_by_key(|row| row.id);
        rows
    }

    /// Kills one connection, or only its running statement when `query`.
    ///
    /// Returns whether the id was live. Captured from TiDB: an unknown id is
    /// NOT an error -- `executeKillStmt` reaches `KillWithNormalCloseMsg`,
    /// which silently ignores an id it does not hold, and the statement
    /// answers OK. (`ErrNoSuchThread`/1094 is raised by `EXPLAIN FOR
    /// CONNECTION`, not by `KILL`.)
    pub fn kill(&self, id: u64, query: bool) -> bool {
        self.kill_with_signal(
            id,
            query,
            tidb_util::sqlkiller::KillSignal::QueryInterrupted,
        )
    }

    /// [`Self::kill`] with the signal the statement observes (Go server
    /// `Kill`'s `killQuery`).
    fn kill_with_signal(
        &self,
        id: u64,
        query: bool,
        signal: tidb_util::sqlkiller::KillSignal,
    ) -> bool {
        let client = self.lock().get(&id).cloned();
        let Some(client) = client else {
            // Serialize interruption with UnTrack's reset, like Go SysProcesses.
            let systems = self
                .systems
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(process) = systems.get(&id) {
                // Go KillSysProcess always interrupts the query, even for KILL CONNECTION.
                process.cancellation.cancel();
                return true;
            }
            return false;
        };
        let target = client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .kill
            .clone();
        let Some(target) = target else {
            return true;
        };
        if query {
            target.cancel_query_with(signal);
        } else {
            target.kill_connection();
        }
        true
    }

    fn set_trackers(&self, id: u64, mem: Arc<Tracker>, disk: Arc<Tracker>) {
        self.with_entry(id, |entry| {
            entry.mem_tracker = Some(mem);
            entry.disk_tracker = Some(disk);
        });
    }

    fn set_process_plan_info(&self, id: u64, plan: Arc<Mutex<tidb_executor::ProcessPlanInfo>>) {
        self.with_entry(id, |entry| {
            entry.process_plan_info = Some(plan);
        });
    }

    fn process_info(&self, id: u64) -> Option<Arc<ProcessInfo>> {
        self.with_entry(id, |entry| {
            let plan_info = entry
                .process_plan_info
                .as_ref()
                .map(|plan| {
                    plan.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone()
                })
                .unwrap_or_default();
            // A running statement's `Time` is its execution start, which Go's
            // `ExecStmt.Exec` republishes after planning; the plan cell is
            // reset when the next statement starts.
            let running = entry.info.is_some();
            let (since, started_at) = plan_info
                .execution_started
                .filter(|_| running)
                .unwrap_or((entry.since, entry.started_at));
            Arc::new(ProcessInfo {
                id,
                user: entry.user.clone(),
                host: entry.host.clone(),
                db: entry.db.clone(),
                digest: entry.digest.clone(),
                info: entry.info.clone().unwrap_or_default(),
                redact_sql: entry.redact_sql,
                time: started_at,
                started_instant: Some(since),
                mem_tracker: entry.mem_tracker.as_ref().map(Arc::clone),
                disk_tracker: entry.disk_tracker.as_ref().map(Arc::clone),
                cur_txn_start_ts: entry.cur_txn_start_ts,
                resource_group_name: entry.resource_group_name.clone(),
                session_alias: entry.session_alias.clone(),
                affected_rows: entry.affected_rows,
                oom_alarm_variables_info: entry.oom_alarm_variables_info,
                brief_binary_plan: plan_info.brief_binary_plan,
                table_ids: plan_info.table_ids,
                index_names: plan_info.index_names,
                stats_info: plan_info.stats_info,
                max_execution_time: if running {
                    plan_info.max_execution_time
                } else {
                    0
                },
                state: entry.state.clone(),
                cur_txn_create_time: entry.transaction.as_ref().map(|txn| txn.created),
                in_restricted_sql: entry.restricted,
                ..ProcessInfo::default()
            })
        })
    }
}

impl SessionManager for ProcessRegistry {
    fn show_process_list(&self) -> Vec<Arc<ProcessInfo>> {
        let ids = self
            .entry_snapshot()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| self.process_info(id))
            .collect()
    }

    fn get_process_info(&self, id: u64) -> Option<Arc<ProcessInfo>> {
        self.process_info(id)
    }

    /// Go server `Kill` / `killQuery`: a runaway rule or `max_execution_time`
    /// interrupts the statement with its own signal; any other kill is
    /// `QueryInterrupted`.
    fn kill(&self, connection_id: u64, query: bool, max_execution_time: bool, runaway: bool) {
        use tidb_util::sqlkiller::KillSignal;
        let signal = if runaway {
            KillSignal::RunawayQueryExceeded
        } else if max_execution_time {
            KillSignal::MaxExecTimeExceeded
        } else {
            KillSignal::QueryInterrupted
        };
        self.kill_with_signal(connection_id, query, signal);
    }
}

/// Removes one connection from the process list when its session is dropped.
pub struct ProcessGuard {
    registry: ProcessRegistry,
    internal_owner: Option<ProcessRegistry>,
    id: u64,
    entry: SharedProcessEntry,
}

/// Live internal-session capability carried through restricted SQL tracking.
/// The process entry is shared; cancellation never locks the running SQL session.
#[derive(Clone)]
pub struct SystemProcess {
    entry: SharedProcessEntry,
    cancellation: Arc<tidb_executor::StatementCancellation>,
}

/// Retained physical transaction observation, independent of statement publication.
#[derive(Clone)]
pub struct ProcessTransactionObserver {
    entry: SharedProcessEntry,
}

impl std::fmt::Debug for ProcessTransactionObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessTransactionObserver")
            .finish_non_exhaustive()
    }
}

impl ProcessTransactionObserver {
    /// Marks storage, rather than the catalog copy, as the completion owner.
    pub fn use_external_owner(&self) {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .external_transaction_owner = true;
    }
    /// Publishes the first real storage timestamp, retaining it across later reads.
    pub fn activated(&self, start_ts: u64) {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .activate_transaction(start_ts);
    }
    /// Retires a transaction exactly once after storage completion or rollback.
    pub fn finished(&self) {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finish_transaction();
    }
}

/// Keeps one process-list statement active until its result set is finished.
pub struct ProcessStatementGuard {
    entry: SharedProcessEntry,
    db: String,
    state: String,
    finished: bool,
}

impl ProcessStatementGuard {
    /// Finishes the statement now. Dropping an unfinished guard does the same.
    pub fn finish(mut self) {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .release_statement(&self.db, &self.state);
        self.finished = true;
    }
}

impl Drop for ProcessStatementGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .release_statement(&self.db, &self.state);
        }
    }
}

impl ProcessGuard {
    /// Retains this connection's transaction observation for physical storage.
    pub fn transaction_observer(&self) -> ProcessTransactionObserver {
        ProcessTransactionObserver {
            entry: Arc::clone(&self.entry),
        }
    }

    /// The registered connection's identity.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Captures the live entry and the current command's cancellation lifetime.
    pub fn system_process(
        &self,
        cancellation: tidb_executor::StatementCancellation,
    ) -> SystemProcess {
        SystemProcess {
            entry: Arc::clone(&self.entry),
            cancellation: Arc::new(cancellation),
        }
    }

    /// The registry this connection is registered in.
    #[must_use]
    pub const fn registry(&self) -> &ProcessRegistry {
        &self.registry
    }

    /// Installs the session memory and disk trackers exposed by ProcessInfo.
    pub fn set_trackers(&self, mem: Arc<Tracker>, disk: Arc<Tracker>) {
        self.registry.set_trackers(self.id, mem, disk);
    }

    /// Installs the session's live `BriefBinaryPlan` publication cell.
    pub fn set_process_plan_info(&self, plan: Arc<Mutex<tidb_executor::ProcessPlanInfo>>) {
        self.registry.set_process_plan_info(self.id, plan);
    }

    /// Publishes one running statement for the lifetime of the returned guard.
    #[must_use]
    pub fn statement_started(
        &self,
        sql: &str,
        db: impl Into<String>,
        state: impl Into<String>,
    ) -> ProcessStatementGuard {
        self.statement_started_with_digest(sql, None, db, state)
    }

    /// [`Self::statement_started`] for a statement whose digest is already
    /// known, so the process list does not normalize the text again.
    #[must_use]
    pub fn statement_started_with_digest(
        &self,
        sql: &str,
        digest: Option<&str>,
        db: impl Into<String>,
        state: impl Into<String>,
    ) -> ProcessStatementGuard {
        let state = state.into();
        let db = db.into();
        {
            let mut entry = self
                .entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entry.publish_statement_with_digest(sql, digest, &state);
            entry.statement_holds = entry.statement_holds.saturating_add(1);
        }
        ProcessStatementGuard {
            entry: Arc::clone(&self.entry),
            db,
            state,
            finished: false,
        }
    }
}

impl std::fmt::Debug for ProcessGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessGuard")
            .field("id", &self.id)
            .finish()
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        self.transaction_observer().finished();
        self.registry.lock().remove(&self.id);
        if let Some(owner) = &self.internal_owner {
            owner
                .internal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&(Arc::as_ptr(&self.entry) as usize));
            owner
                .systems
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|_, process| {
                    let retained = !Arc::ptr_eq(&process.entry, &self.entry);
                    if !retained {
                        process.cancellation.reset();
                    }
                    retained
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingTarget {
        queries: AtomicUsize,
        connections: AtomicUsize,
    }

    impl ProcessKillTarget for CountingTarget {
        fn cancel_query(&self) {
            self.queries.fetch_add(1, Ordering::AcqRel);
        }
        fn kill_connection(&self) {
            self.connections.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn internal_process_batch_shares_entries_without_exposing_idle_pool_sessions() {
        let registry = ProcessRegistry::default();
        let mut session = crate::Session::new();
        let guard = registry.register_internal(71, "test".into());
        let observer = guard.transaction_observer();
        observer.activated(123);
        session.attach_process(71, guard);
        assert_eq!(registry.internal_session_start_ts(), vec![123]);
        assert!(registry.snapshot().is_empty());
        assert!(registry.transaction_snapshot().is_empty());
        let process = session.system_process().unwrap();
        registry.track_system_process(901, process.clone()).unwrap();
        registry.track_system_process(901, process).unwrap();
        let _statement = session.retain_process_statement("ANALYZE TABLE test.t");
        assert_eq!(
            registry.snapshot()[0].info.as_deref(),
            Some("ANALYZE TABLE test.t")
        );
        assert_eq!(registry.snapshot()[0].id, 901);
        assert_eq!(registry.get_process_info(901).unwrap().id, 901);
        assert_eq!(registry.show_process_list().len(), 1);
        assert!(registry.transaction_snapshot().is_empty());
        tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.tracker(901);
        let excluded = registry.internal_session_start_ts();
        tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(901);
        assert!(excluded.is_empty());
        registry.untrack_system_process(901);
        assert!(registry.snapshot().is_empty());
        assert_eq!(registry.internal_session_start_ts(), vec![123]);
        drop(session);
        assert!(registry.internal_session_start_ts().is_empty());
    }

    #[test]
    fn internal_process_batch_kills_both_modes_without_poisoning_reused_sessions() {
        let registry = ProcessRegistry::default();
        let mut session = crate::Session::new();
        session.attach_process(72, registry.register_internal(72, String::new()));
        for query in [true, false] {
            registry
                .track_system_process(902, session.system_process().unwrap())
                .unwrap();
            let memory = session.routed_statement_memory();
            assert!(memory.sql_killer().handle_signal().is_none());
            assert!(registry.kill(902, query));
            assert!(memory.sql_killer().handle_signal().is_some());
            registry.untrack_system_process(902);
            assert!(!registry.kill(902, query));
        }
        let _fresh_command = session.begin_query_cancellation();
        let memory = session.routed_statement_memory();
        assert!(memory.sql_killer().handle_signal().is_none());
    }

    #[test]
    fn internal_process_batch_rejects_conflicting_ids_and_retires_dropped_sessions() {
        let registry = ProcessRegistry::default();
        let mut first = crate::Session::new();
        let mut second = crate::Session::new();
        // Internal membership follows session identity, not a possibly reused connection ID.
        first.attach_process(1, registry.register_internal(1, String::new()));
        second.attach_process(1, registry.register_internal(1, String::new()));
        first.transaction_observer().unwrap().activated(10);
        second.transaction_observer().unwrap().activated(20);
        registry
            .track_system_process(903, first.system_process().unwrap())
            .unwrap();
        assert_eq!(
            registry
                .track_system_process(903, second.system_process().unwrap())
                .unwrap_err(),
            "The ID is in use: 903"
        );
        drop(first);
        assert!(registry.snapshot().is_empty());
        assert_eq!(registry.internal_session_start_ts(), vec![20]);
        registry
            .track_system_process(903, second.system_process().unwrap())
            .unwrap();
        drop(second);
        assert!(registry.snapshot().is_empty());
        assert!(registry.internal_session_start_ts().is_empty());
    }

    #[test]
    fn registered_connection_leaves_the_list_with_its_guard() {
        let registry = ProcessRegistry::default();
        let guard = registry.register(
            7,
            "alice".to_owned(),
            "127.0.0.1:1".to_owned(),
            "test".to_owned(),
            None,
        );
        let rows = registry.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 7);
        assert_eq!(rows[0].command, "Sleep");
        assert_eq!(rows[0].info, None);
        drop(guard);
        assert!(registry.snapshot().is_empty());
    }

    #[test]
    fn running_statement_becomes_info_and_is_cleared_again() {
        let registry = ProcessRegistry::default();
        let guard = registry.register(1, String::new(), String::new(), "test".to_owned(), None);
        registry.statement_started(1, "select 1", "autocommit");
        let rows = registry.snapshot();
        assert_eq!(rows[0].info.as_deref(), Some("select 1"));
        assert_eq!(rows[0].command, "Query");
        registry.statement_finished(1, "mysql", "autocommit");
        let rows = registry.snapshot();
        assert_eq!(rows[0].info, None);
        assert_eq!(rows[0].db, "mysql");

        {
            let _statement = guard.statement_started("select 2", "test", "autocommit");
            assert_eq!(registry.snapshot()[0].info.as_deref(), Some("select 2"));
            // The ordinary session publishes and finishes the same statement
            // while the server still owns its result set. Go keeps the
            // command visible until `writeResultSet` returns.
            registry.statement_started(1, "select 2", "autocommit");
            registry.statement_finished(1, "test", "autocommit");
            assert_eq!(registry.snapshot()[0].info.as_deref(), Some("select 2"));
        }
        assert_eq!(registry.snapshot()[0].info, None);
    }

    #[test]
    fn prepared_statement_publishes_its_prepare_time_digest() {
        let registry = ProcessRegistry::default();
        let guard = registry.register(7, String::new(), String::new(), "test".to_owned(), None);
        registry.transaction_started(7, 42);
        // Go's EXECUTE installs the digest fixed at PREPARE
        // (`InitSQLDigest`); the text is not normalized again, so the
        // published digest is exactly the caller's.
        let prepared_digest = tidb_parser::normalize_digest("select c from t where id = ?")
            .1
            .to_string();
        {
            let _statement = guard.statement_started_with_digest(
                "select c from t where id = ?",
                Some(&prepared_digest),
                "test",
                "autocommit",
            );
            let transactions = registry.transaction_snapshot();
            assert_eq!(
                transactions[0].current_sql_digest.as_deref(),
                Some(prepared_digest.as_str())
            );
        }
        // A statement without a known digest still derives it from the text.
        {
            let _statement = guard.statement_started("select 1", "test", "autocommit");
            let derived = tidb_parser::normalize_digest("select 1").1.to_string();
            assert_eq!(
                registry.transaction_snapshot()[0]
                    .current_sql_digest
                    .as_deref(),
                Some(derived.as_str())
            );
        }
    }

    #[test]
    fn kill_reaches_the_target_and_unknown_ids_report_missing() {
        let registry = ProcessRegistry::default();
        let target = Arc::new(CountingTarget::default());
        let _guard = registry.register(
            3,
            String::new(),
            String::new(),
            String::new(),
            Some(target.clone()),
        );
        assert!(registry.kill(3, true));
        assert_eq!(target.queries.load(Ordering::Acquire), 1);
        assert_eq!(target.connections.load(Ordering::Acquire), 0);
        assert!(registry.kill(3, false));
        assert_eq!(target.connections.load(Ordering::Acquire), 1);
        assert!(!registry.kill(999, false));
    }

    #[test]
    fn show_truncates_info_at_a_hundred_runes_and_full_does_not() {
        let long = "x".repeat(150);
        assert_eq!(truncate_process_info(&long, false).len(), 100);
        assert_eq!(truncate_process_info(&long, true).len(), 150);
        let wide = "é".repeat(150);
        assert_eq!(truncate_process_info(&wide, false).chars().count(), 100);
    }
}
