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
//! NOT MODELLED (Go has these on `ProcessInfo`, and inventing values would be
//! worse than omitting them): plan/digest/memory/disk columns of
//! `information_schema.processlist`, resource groups, session alias, and
//! global-kill's server-id-routed `KILL` across instances.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, Utc};
use tidb_util::memory::Tracker;
use tidb_util::memoryusagealarm::{ProcessInfo, SessionManager};

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
}

/// One row of `SHOW [FULL] PROCESSLIST`, in Go `ProcessInfo` order.
#[derive(Clone, Debug)]
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
    digest_text: String,
    /// When the current command started, which `Time` counts from.
    since: Instant,
    started_at: DateTime<Utc>,
    mem_tracker: Option<Arc<Tracker>>,
    disk_tracker: Option<Arc<Tracker>>,
    kill: Option<Arc<dyn ProcessKillTarget>>,
    transaction: Option<TransactionEntry>,
}

/// The server's live connection registry, shared by every connection thread.
///
/// Cloning shares one registry, as every session of one TiDB instance sees
/// one `sessmgr.Manager`.
#[derive(Clone, Default)]
pub struct ProcessRegistry {
    entries: Arc<Mutex<HashMap<u64, ProcessEntry>>>,
}

impl std::fmt::Debug for ProcessRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessRegistry")
            .finish_non_exhaustive()
    }
}

impl ProcessRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, ProcessEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
        self.lock().insert(
            id,
            ProcessEntry {
                user,
                host,
                db,
                state: String::new(),
                info: None,
                digest: String::new(),
                digest_text: String::new(),
                since: Instant::now(),
                started_at: Utc::now(),
                mem_tracker: None,
                disk_tracker: None,
                kill,
                transaction: None,
            },
        );
        ProcessGuard {
            registry: self.clone(),
            id,
        }
    }

    /// Records the statement a connection just started, which becomes its
    /// `Info` and restarts its `Time`.
    pub fn statement_started(&self, id: u64, sql: &str, state: &str) {
        if let Some(entry) = self.lock().get_mut(&id) {
            entry.info = Some(sql.to_owned());
            let (normalized, digest) = tidb_parser::normalize_digest(sql);
            entry.digest = digest.to_string();
            entry.digest_text = normalized;
            entry.since = Instant::now();
            entry.started_at = Utc::now();
            entry.state = state.to_owned();
            if let Some(transaction) = &mut entry.transaction {
                transaction.state = "Running";
                let digest = entry.digest.clone();
                transaction.current_sql_digest = (!digest.is_empty()).then(|| digest.clone());
                if !digest.is_empty()
                    && transaction.all_sql_digests.len() < MAX_TRANSACTION_STMT_HISTORY
                {
                    transaction.all_sql_digests.push(digest);
                }
            }
        }
    }

    /// Records that a connection finished its statement: `Info` becomes NULL,
    /// and `db` and `State` are refreshed, since `USE` may have just changed
    /// the schema and the statement may have opened or closed a transaction.
    pub fn statement_finished(&self, id: u64, db: &str, state: &str) {
        if let Some(entry) = self.lock().get_mut(&id) {
            entry.info = None;
            entry.digest.clear();
            entry.digest_text.clear();
            entry.since = Instant::now();
            entry.started_at = Utc::now();
            entry.db = db.to_owned();
            entry.state = state.to_owned();
            if let Some(transaction) = &mut entry.transaction {
                transaction.state = "Idle";
                transaction.current_sql_digest = None;
                transaction.waiting_start = None;
            }
        }
    }

    /// Publishes a newly activated transaction for `TIDB_TRX`.
    pub fn transaction_started(&self, id: u64, start_ts: u64) {
        if let Some(entry) = self.lock().get_mut(&id) {
            let running = entry.info.is_some();
            let digest = (!entry.digest.is_empty()).then(|| entry.digest.clone());
            entry.transaction = Some(TransactionEntry {
                start_ts,
                current_sql_digest: digest.clone(),
                state: if running { "Running" } else { "Idle" },
                waiting_start: None,
                mem_buffer_keys: 0,
                mem_buffer_bytes: 0,
                all_sql_digests: digest.into_iter().collect(),
                related_table_ids: std::collections::HashSet::new(),
            });
        }
    }

    /// Removes the transaction after commit or rollback.
    pub fn transaction_finished(&self, id: u64) {
        if let Some(entry) = self.lock().get_mut(&id) {
            entry.transaction = None;
        }
    }

    /// Changes the source transaction-running-state label.
    pub fn transaction_state(&self, id: u64, state: &'static str) {
        if let Some(transaction) = self
            .lock()
            .get_mut(&id)
            .and_then(|entry| entry.transaction.as_mut())
        {
            transaction.state = state;
            transaction.waiting_start = (state == "LockWaiting").then(chrono::Utc::now);
        }
    }

    /// Publishes the current transaction MemBuffer length and native memory
    /// footprint. Go updates these from `LazyTxn.Len` and the MemDB footprint
    /// hook while the transaction remains active.
    pub fn transaction_buffer_metrics(&self, id: u64, keys: u64, bytes: i64) {
        if let Some(transaction) = self
            .lock()
            .get_mut(&id)
            .and_then(|entry| entry.transaction.as_mut())
        {
            transaction.mem_buffer_keys = keys;
            transaction.mem_buffer_bytes = bytes;
        }
    }

    /// Records one physical table used by the live transaction.
    pub fn transaction_related_table(&self, id: u64, table_id: i64) {
        if let Some(transaction) = self
            .lock()
            .get_mut(&id)
            .and_then(|entry| entry.transaction.as_mut())
        {
            transaction.related_table_ids.insert(table_id);
        }
    }

    /// Returns every live transaction in stable connection-ID order.
    #[must_use]
    pub fn transaction_snapshot(&self) -> Vec<TransactionRow> {
        let mut rows = self
            .lock()
            .iter()
            .filter_map(|(id, entry)| {
                let transaction = entry.transaction.as_ref()?;
                Some(TransactionRow {
                    start_ts: transaction.start_ts,
                    current_sql_digest: transaction.current_sql_digest.clone(),
                    state: transaction.state,
                    waiting_start: transaction.waiting_start,
                    mem_buffer_keys: transaction.mem_buffer_keys,
                    mem_buffer_bytes: transaction.mem_buffer_bytes,
                    session_id: *id,
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
            .lock()
            .iter()
            .map(|(id, entry)| ProcessRow {
                id: *id,
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
        let target = match self.lock().get(&id) {
            Some(entry) => entry.kill.clone(),
            None => return false,
        };
        let Some(target) = target else {
            return true;
        };
        if query {
            target.cancel_query();
        } else {
            target.kill_connection();
        }
        true
    }

    fn set_trackers(&self, id: u64, mem: Arc<Tracker>, disk: Arc<Tracker>) {
        if let Some(entry) = self.lock().get_mut(&id) {
            entry.mem_tracker = Some(mem);
            entry.disk_tracker = Some(disk);
        }
    }

    fn process_info(&self, id: u64) -> Option<Arc<ProcessInfo>> {
        self.lock().get(&id).map(|entry| {
            Arc::new(ProcessInfo {
                id,
                user: entry.user.clone(),
                host: entry.host.clone(),
                db: entry.db.clone(),
                digest: entry.digest.clone(),
                info: entry.info.clone().unwrap_or_default(),
                time: entry.started_at,
                mem_tracker: entry.mem_tracker.as_ref().map(Arc::clone),
                disk_tracker: entry.disk_tracker.as_ref().map(Arc::clone),
                ..ProcessInfo::default()
            })
        })
    }
}

impl SessionManager for ProcessRegistry {
    fn show_process_list(&self) -> Vec<Arc<ProcessInfo>> {
        let ids = self.lock().keys().copied().collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| self.process_info(id))
            .collect()
    }

    fn get_process_info(&self, id: u64) -> Option<Arc<ProcessInfo>> {
        self.process_info(id)
    }
}

/// Removes one connection from the process list when its session is dropped.
pub struct ProcessGuard {
    registry: ProcessRegistry,
    id: u64,
}

/// Keeps one process-list statement active until its result set is finished.
pub struct ProcessStatementGuard {
    registry: ProcessRegistry,
    id: u64,
    db: String,
    state: String,
    finished: bool,
}

impl ProcessStatementGuard {
    /// Finishes the statement now. Dropping an unfinished guard does the same.
    pub fn finish(mut self) {
        self.registry
            .statement_finished(self.id, &self.db, &self.state);
        self.finished = true;
    }
}

impl Drop for ProcessStatementGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.registry
                .statement_finished(self.id, &self.db, &self.state);
        }
    }
}

impl ProcessGuard {
    /// The registered connection's identity.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
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

    /// Publishes one running statement for the lifetime of the returned guard.
    #[must_use]
    pub fn statement_started(
        &self,
        sql: &str,
        db: impl Into<String>,
        state: impl Into<String>,
    ) -> ProcessStatementGuard {
        let state = state.into();
        self.registry.statement_started(self.id, sql, &state);
        ProcessStatementGuard {
            registry: self.registry.clone(),
            id: self.id,
            db: db.into(),
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
        self.registry.lock().remove(&self.id);
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
        }
        assert_eq!(registry.snapshot()[0].info, None);
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
