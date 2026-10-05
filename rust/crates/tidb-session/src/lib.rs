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

//! The session: the single entry point that owns catalog state and runs SQL
//! statements through the wired parse -> plan -> execute pipeline.
//!
//! This is the seam of Go's `pkg/session` `session.ExecuteStmt`: one object a
//! client holds, dispatching each statement kind to its executor path.
//!
//! SEED SCOPE: [`Session::run`] dispatches `SELECT` (rows), `INSERT` (affected
//! count), and `CREATE TABLE` over the session's [`Catalog`]. DEFERRED
//! (documented): transactions (autocommit is implicit and immediate --
//! `BEGIN`/`COMMIT`/`ROLLBACK` land with the txnkv integration), session
//! variables (`SET`), prepared statements, the MySQL wire protocol, privileges,
//! and every other statement kind. Statements are currently parsed twice (once
//! here for dispatch, once in the driver's runner) -- a wiring simplification
//! to remove when the driver's runners take parsed statements.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_ast::Stmt;
use tidb_datatype::{Datum, FieldType};
use tidb_executor::{Catalog, DriverError, MysqlRng};
use tidb_executor::{SchemaErrorKind, DEFAULT_DATABASE};
pub use tidb_planner::txn_mode::{
    txn_mode_for_begin, txn_mode_for_statement, SessionTxnMode, StatementTxnModeInputs,
    OPTIMISTIC_TXN_MODE, PESSIMISTIC_TXN_MODE,
};

/// Go `approxParseSQLTokenCnt`: estimates the token count used to reserve
/// parser memory with the global memory arbitrator.
///
/// This is intentionally not the SQL lexer. It preserves Go's cheap byte
/// scan, including its core-DML admission rule, comment skipping, ten-byte
/// keyword buffer, and treating quoted strings/identifiers as one token.
#[must_use]
pub fn approx_parse_sql_token_count(sql: &str) -> i64 {
    const CORE: u8 = 1;
    const BYPASS: u8 = 2;
    const SELECT: u8 = 4;

    fn key_token(keyword: &[u8]) -> u8 {
        match keyword {
            b"select" => SELECT,
            b"from" | b"insert" | b"update" | b"delete" | b"replace" => CORE,
            b"explain" | b"desc" | b"analyze" => BYPASS,
            _ => 0,
        }
    }

    let bytes = sql.as_bytes();
    let mut token_count = 0_i64;
    let mut in_word = false;
    let mut keyword = [0_u8; 10];
    let mut keyword_len = 0_usize;
    let mut hit_core_token = false;
    let mut has_select = false;
    let mut index = 0_usize;

    while index < bytes.len() {
        let original = bytes[index];
        let folded = original.to_ascii_lowercase();
        if folded.is_ascii_lowercase() || folded.is_ascii_digit() || folded == b'_' {
            in_word = true;
            if !hit_core_token && keyword_len < keyword.len() {
                keyword[keyword_len] = folded;
                keyword_len += 1;
            }
            index += 1;
            continue;
        }

        if in_word {
            in_word = false;
            token_count += 1;
            if !hit_core_token {
                let token = key_token(&keyword[..keyword_len]);
                if token & SELECT != 0 {
                    has_select = true;
                } else if token & CORE != 0 {
                    hit_core_token = true;
                } else if token & BYPASS == 0 && !has_select {
                    return 0;
                }
                keyword_len = 0;
            }
        }

        if original == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        if original == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            index += usize::from(index < bytes.len());
            continue;
        }
        if original == b'#' {
            index += 1;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            index += usize::from(index < bytes.len());
            continue;
        }
        if original == b'\'' || original == b'"' {
            let quote = original;
            index += 1;
            while index < bytes.len() && bytes[index] != quote {
                if bytes[index] == b'\\' && index + 1 < bytes.len() {
                    index += 1;
                }
                index += 1;
            }
            index += usize::from(index < bytes.len());
            token_count += 1;
            continue;
        }
        if original == b'`' {
            index += 1;
            while index < bytes.len() && bytes[index] != b'`' {
                if bytes[index] == b'\\' && index + 1 < bytes.len() {
                    index += 1;
                }
                index += 1;
            }
            index += usize::from(index < bytes.len());
            token_count += 1;
            continue;
        }
        if original == b'?' {
            token_count += 1;
        }
        index += 1;
    }

    if in_word {
        token_count += 1;
    }
    if hit_core_token {
        token_count
    } else {
        0
    }
}

/// Go `approxCompilePlanTokenCnt`: estimates the token count of normalized
/// SQL used to reserve optimizer memory.
#[must_use]
pub fn approx_compile_plan_token_count(sql: &str, has_select: bool) -> i64 {
    const FROM: &str = "from";

    let mut token_count = 0_i64;
    let mut token_len = 0_usize;
    let mut has_select_from = false;
    for (index, character) in sql.char_indices() {
        if character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '_' | '`' | '.')
        {
            token_len += character.len_utf8();
            continue;
        }
        if token_len > 0 {
            token_count += 1;
            if has_select
                && !has_select_from
                && token_len == FROM.len()
                && &sql[index - token_len..index] == FROM
            {
                has_select_from = true;
            }
            token_len = 0;
        }
        if character == '?' {
            token_count += 1;
        }
    }
    if token_len > 0 {
        token_count += 1;
    }
    if has_select && !has_select_from {
        0
    } else {
        token_count
    }
}

/// The result of running one statement.
#[derive(Debug, PartialEq)]
pub enum StmtResult {
    /// A query's result rows.
    Rows(Vec<Vec<Datum>>),
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// The result of running one statement, with wire-facing column metadata.
///
/// [`StmtResult::Rows`] loses column names/types; a server front end needs one
/// `(name, type)` per result column to build protocol column definitions, so
/// [`Session::run_with_columns`] returns this richer shape instead.
#[derive(Debug, PartialEq)]
pub enum StmtOutput {
    /// A query's result columns and rows.
    Rows {
        /// One `(display name, field type)` per output column.
        columns: Vec<(String, FieldType)>,
        /// The result rows (one `Datum` per column).
        rows: Vec<Vec<Datum>>,
    },
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// One pessimistic wait-for edge returned by the active storage backend.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataLockWait {
    /// Transaction waiting for the lock.
    pub txn: u64,
    /// Transaction currently holding the lock.
    pub wait_for_txn: u64,
    /// Encoded locked key.
    pub key: Vec<u8>,
    /// Encoded TopSQL resource-group tag.
    pub resource_group_tag: Vec<u8>,
}

/// Storage boundary for `information_schema.DATA_LOCK_WAITS`.
pub trait DataLockWaitsProvider: Send + Sync {
    /// Reads the current wait-for edges from the backend.
    fn lock_waits(&self) -> Result<Vec<DataLockWait>, String>;
}

/// Storage-backed reader used by pinned Go `SHOW COLUMN_STATS_USAGE`.
pub trait ColumnStatsUsageProvider: Send + Sync {
    /// Loads the complete shared usage table at a fresh statement snapshot.
    fn load_column_stats_usage(
        &self,
        location: &tidb_datatype::SessionTimeZone,
        resource_group: &str,
    ) -> Result<
        std::collections::HashMap<
            tidb_model::TableItemID,
            (Option<tidb_datatype::Time>, Option<tidb_datatype::Time>),
        >,
        String,
    >;
}

/// Storage-backed reader used by pinned Go `SHOW ANALYZE STATUS`.
pub trait AnalyzeStatusProvider: Send + Sync {
    /// Runs Go's fresh restricted read of the thirty newest persisted jobs.
    fn load_analyze_status(
        &self,
        resource_group: &str,
    ) -> Result<Vec<tidb_stats::AnalyzeStatusJob>, String>;

    /// Go `GlobalPDHelper.GetApproximateTableCountFromStorage`. Errors and a
    /// backend without PD both produce zero, as its caller ignores `hasPD`.
    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> i64;
}

/// Go `TableSizeStats` values consumed by one logical table's
/// `information_schema.TABLES` and `information_schema.PARTITIONS` rows.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableStorageStatistics {
    /// Logical table ID.
    pub table_id: i64,
    /// Logical/aggregate `(rows, average row length, data length, index length)`.
    pub table: (u64, u64, u64, u64),
    /// The same tuple for each physical partition ID.
    pub partitions: Vec<(i64, (u64, u64, u64, u64))>,
}

/// Fresh restricted-storage boundary used by Go's information-schema
/// `TableSizeStats` reader.
pub trait TableStorageStatsProvider: Send + Sync {
    /// Runs the pinned restricted reads and returns estimates for the current
    /// schema image. The histogram-size read is skipped when only
    /// `TABLE_ROWS` is requested. A read failure is warning-only at the caller.
    fn load_table_storage_statistics(
        &self,
        resource_group: &str,
        need_column_lengths: bool,
    ) -> Result<Vec<TableStorageStatistics>, String>;
}

/// The statement-owned policy a server needs to retain an eager result set.
///
/// It is captured before `SET_VAR` overlays are restored, so a prepared
/// cursor uses the same quota, OOM action, temporary-storage decision, and
/// chunk bound as the statement that produced its rows.
#[derive(Clone)]
pub struct ResultMaterializationAuthority {
    current_tso: Option<tidb_executor::CurrentTso>,
    start_ts_guard: Option<Arc<tidb_txnkv::StartTsGuard>>,
    memory: tidb_executor::StatementMemory,
    init_chunk_size: usize,
    max_chunk_size: usize,
}

impl ResultMaterializationAuthority {
    /// Builds a cursor-retention policy captured by a non-pipeline session.
    ///
    /// Production callers should pass the statement's actual memory policy
    /// and the statement's chunk-size bounds; this constructor exists so an
    /// external server-session implementation can support prepared cursors
    /// without a crate-private back door.
    #[must_use]
    pub fn new(
        memory: tidb_executor::StatementMemory,
        init_chunk_size: usize,
        max_chunk_size: usize,
    ) -> Self {
        Self {
            memory,
            init_chunk_size,
            max_chunk_size,
            start_ts_guard: None,
            current_tso: None,
        }
    }

    /// Retains the snapshot across transaction completion until result/cursor close.
    #[must_use]
    pub fn with_start_ts(mut self, start_ts: u64) -> Self {
        self.start_ts_guard = (start_ts != 0 && start_ts != u64::MAX)
            .then(|| Arc::new(tidb_txnkv::ACTIVE_START_TS.hold(start_ts)));
        self
    }

    /// Tracks lazy activation as well as an already opened eager snapshot.
    #[must_use]
    pub fn with_current_tso(self, current: tidb_executor::CurrentTso) -> Self {
        let mut authority = self.with_start_ts(current.value() as u64);
        authority.current_tso = Some(current);
        authority
    }

    /// Transfers the lazy activation handle to the cursor materializer.
    pub fn take_current_tso(&mut self) -> Option<tidb_executor::CurrentTso> {
        self.current_tso.take()
    }

    /// Transfers the pin to the connection-owned cursor without releasing it.
    pub fn take_start_ts_guard(&mut self) -> Option<Arc<tidb_txnkv::StartTsGuard>> {
        self.start_ts_guard.take()
    }

    /// Consumes the authority into its retained memory policy and chunk bounds.
    #[must_use]
    pub fn into_parts(self) -> (tidb_executor::StatementMemory, usize, usize) {
        (self.memory, self.init_chunk_size, self.max_chunk_size)
    }

    /// The statement tracker retained by this authority.
    #[must_use]
    pub(crate) fn statement_memory(&self) -> tidb_executor::StatementMemory {
        self.memory.clone()
    }
}

/// A process-wide catalog shared by every session, as Go's domain-owned
/// `infoschema` is shared by every session of a TiDB instance.
pub type SharedCatalog = Arc<Mutex<Catalog>>;

/// Go `domainMap`: one domain-level catalog authority per storage UUID.
///
/// A server can use this registry when opening sessions for more than one
/// keyspace. Looking up `None` preserves the plugin-facing Go contract: reuse
/// any available domain, or return [`NoAvailableDomain`] when the process has
/// not opened one yet.
#[derive(Default)]
pub struct DomainMap {
    domains: Mutex<HashMap<String, SharedCatalog>>,
}

/// A nil-store lookup found no domain that could be reused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NoAvailableDomain;

impl std::fmt::Display for NoAvailableDomain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("can not find available domain for a nil store")
    }
}

impl std::error::Error for NoAvailableDomain {}

impl DomainMap {
    /// Gets or creates the shared catalog for `store_uuid`.
    ///
    /// `None` never creates an entry. It returns an existing one when
    /// available, matching the enterprise-plugin compatibility path in Go.
    pub fn get(&self, store_uuid: Option<&str>) -> Result<SharedCatalog, NoAvailableDomain> {
        let mut domains = self
            .domains
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(store_uuid) = store_uuid else {
            return domains.values().next().cloned().ok_or(NoAvailableDomain);
        };
        Ok(Arc::clone(
            domains.entry(store_uuid.to_owned()).or_default(),
        ))
    }
}

/// A session: runs statements against a catalog shared with its peers.
///
/// Go sessions borrow the process's schema state rather than owning private
/// copies, so a table one connection creates is visible to the others. This
/// mirrors that with a shared, mutex-guarded catalog; the statement-level lock
/// stands in for Go's schema-version/lease machinery, which is a separate
/// tier (documented deferral).
/// The node-side sink for Go's `GetRelatedTableForMDL` map: which stored
/// tables this session's live statement or transaction has bound, and at
/// which cluster schema version each was FIRST bound.
///
/// Go records into the transaction context and the session manager reads it
/// (`RemoveLockDDLJobs`); here the record crosses a crate boundary, so it is
/// a trait the node implements over its pin registry.
pub trait MdlRelatedTableSink: Send + Sync {
    /// One stored table, bound at `version`; first use wins.
    fn record_table(&self, table_id: i64, version: i64);
    /// A referenced name resolved to no stored table (a view, an unknown
    /// name); the gate falls back to blocking conservatively.
    fn record_unresolved(&self);
}

pub struct Session {
    global_config_syncer: Option<Arc<tidb_domain::globalconfigsync::GlobalConfigSyncer>>,
    catalog: SharedCatalog,
    account_storage_delegated: bool,
    /// Go `session.values`: heterogeneous values addressed by session-context
    /// stringer keys. Values must be thread-safe because the native session
    /// moves between connection workers.
    context_values: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    /// Shared with every executor context built for the current attempt.
    executor_first_run_breakpoint: Arc<std::sync::atomic::AtomicBool>,
    /// The cluster transaction layer owns the attempt boundary while this is
    /// true, so inner pessimistic retries do not re-arm the first-run hook.
    external_executor_breakpoint_scope: bool,
    /// Go `infosync.ServerInfo.StartTimestamp`: when the hosting server
    /// process started, which the server-tier `Statistics` provider turns
    /// into the `Uptime` status variable (`pkg/server/stat.go:87`). `None`
    /// (no hosting server pushed one) leaves `Uptime` unserved, matching a
    /// session with no registered server provider.
    server_start_timestamp: Option<i64>,
    /// Metadata snapshot cache keyed by the catalog mutation version.
    tidb_decode_key_cache:
        std::sync::Mutex<Option<(u64, Arc<tidb_executor::TidbDecodeKeySnapshot>)>>,
    /// One connection-wide memory/disk tracker pair. Every statement gets a
    /// fresh child below these roots, so an open cursor remains counted when
    /// the client starts its next command.
    session_memory: tidb_executor::SessionMemory,
    /// The current statement's actual result-retention authority.
    ///
    /// Go retains `SessionVars.StmtCtx` until the next statement reset and a
    /// cursor keeps that context's tracker. Rust used to construct a second
    /// statement tracker after execution; this slot lets the result keep the
    /// same tracker the executor used.
    statement_result_authority: std::cell::RefCell<Option<ResultMaterializationAuthority>>,
    /// Go `digestKey := normalizedSQL` (`session.go`, the arbitrator
    /// registration): the current statement's normalized text, which keys
    /// the memory arbitrator's digest profile so a repeat of the same
    /// statement reserves its previous peak up front. It uses ordinary
    /// normalization of the original SQL, including for cached point reads;
    /// no AST restoration or binding-specific database qualification is needed.
    /// It is computed only while the arbitrator is enabled.
    current_sql_digest_key: String,
    /// The running statement's normalized text, computed once at statement
    /// start with its digest (Go `StmtCtx.SQLDigest`) and consumed by the
    /// memory arbitration key; `None` when arbitration is off.
    statement_normalized_sql: Option<String>,
    statement_observation: Option<observation::StatementObservation>,
    pending_observation_parse: Option<(String, Duration)>,
    routed_statement_observation_depth: u32,
    previous_summary_statement: Option<(String, String)>,
    statement_stats: Arc<tidb_util::topsql_stmtstats::StatementStats>,
    /// The open transaction, if any.
    txn: Option<Transaction>,
    /// Go `LazyTxn.writeSLI`: transaction write-throughput state shared by
    /// every statement until the transaction ends.
    write_sli: tidb_util::sli::TxnWriteThroughputSli,
    /// Go `SessionVars.LocalTemporaryTables` (an `infoschema.SessionTables`):
    /// the LOCAL temporary tables this connection created, as
    /// `(folded schema, folded name, table)`.
    ///
    /// They live HERE and not in the shared catalog because that is the whole
    /// meaning of the kind: no DDL job creates one, no other session can name
    /// one, and none of them outlives the connection. The catalog sees them
    /// only for the duration of one statement --
    /// [`Session::with_catalog_mut`] attaches them and takes them back, which
    /// is Go's `AttachLocalTemporaryTableInfoSchema` /
    /// `DetachLocalTemporaryTableInfoSchema` pair around each statement.
    ///
    /// NOT MODELLED (documented): Go DETACHES the overlay again in three
    /// narrower places, and this tier keeps it attached in all three. Two are
    /// harmless here -- `SHOW CREATE VIEW`/`SHOW CREATE SEQUENCE`
    /// (`preprocess.go:551`) and the by-name `SHOW TABLES`, which
    /// `Catalog::table_names` already excludes. The third is real: while
    /// EXPANDING a view's body (`logical_plan_builder.go:4941`), so a local
    /// temporary table that SHADOWS a permanent table the view reads is
    /// followed here where Go reads the permanent one. Creating a view over a
    /// local temporary table is refused (1352), so the shadowing case is the
    /// only way to reach it, and closing it would mean handing the view
    /// builder a catalog with the overlay removed -- a whole-catalog clone
    /// per view read on this tier's shape.
    local_temporary_tables: Vec<(String, String, tidb_executor::KvTable)>,
    /// Go `SessionVars.TxnCtx.TemporaryTables`: the rows this session has
    /// written to GLOBAL temporary tables, by physical table id.
    ///
    /// A global temporary table's SCHEMA is shared -- it is created by a real
    /// DDL job and every session can name it -- while its ROWS belong to one
    /// transaction of one session. Go gets that from two places at once: the
    /// snapshot interceptor answers an EMPTY iterator for a global temporary
    /// table, so nothing outside the current transaction's own buffer is
    /// readable, and `temporaryTableKVFilter` discards every temporary-table
    /// key before two-phase commit, so nothing is ever written down. This map
    /// is both halves: it is swapped into the shared table object for the
    /// statement's duration and CLEARED at each transaction end, which is
    /// what `ON COMMIT DELETE ROWS` means here.
    global_temporary_data:
        std::collections::HashMap<i64, Box<dyn tidb_executor::storage::TableStorage>>,
    /// The session's system and user variables.
    vars: SessionVars,
    /// Go `SessionVars.ResourceGroupName`: the connection's selected resource
    /// group. A statement-level `RESOURCE_GROUP` hint may override this value
    /// for one statement, but never mutates it.
    resource_group: String,
    /// Go `StmtCtx.StmtHints`: the canonical statement-hint parse result for
    /// the statement currently executing. It is reset at every statement
    /// boundary and replaced after `hint.ParseStmtHints`.
    stmt_hints: tidb_hint::StmtHints,
    /// Go `StmtCtx.ResourceGroupName`: the group selected for the statement
    /// currently passing through the session funnel. It starts from
    /// [`Self::resource_group`] at every statement boundary and may be
    /// replaced by that statement's last `RESOURCE_GROUP` hint.
    active_resource_group: String,
    /// The warnings the last statement produced, which Go keeps in
    /// `StmtCtx.warnings` and `SHOW WARNINGS` reads.
    warnings: Vec<SqlWarning>,
    /// Go `handleQuery`'s `parserWarns`: the multi-statement 8130 warning is
    /// carried OUTSIDE the per-statement warning buffer and appended to the
    /// LAST statement's context after it runs (`conn.go:2262`), so the reset
    /// each statement performs cannot clear it.
    deferred_multi_statement_warning: bool,
    /// Go `StatementContext.InShowWarning`: set for exactly the statements
    /// that inherit the buffer, and the reason `WarningCount()` reports 0 for
    /// them. See [`Session::wire_warning_count`].
    in_show_warning: bool,
    /// Go `SessionVars.SysWarningCount` / `SysErrorCount`: the PREVIOUS
    /// statement's counts, which is what `@@warning_count` and
    /// `@@error_count` report.
    ///
    /// `ResetContextOfStmt` snapshots them from the outgoing
    /// `StmtCtx.NumErrorWarnings()` at every statement start, whatever the
    /// incoming statement is -- unlike the buffer above, which only the
    /// statements that REPORT warnings inherit. Reading the buffer instead
    /// answered `0` for every statement that is not one of those three.
    sys_warning_count: usize,
    sys_error_count: usize,
    /// Go `SessionVars.User` in its two spellings: the matched grant
    /// identity `CURRENT_USER()` reports and the login identity `USER()`
    /// reports. Empty until a front end authenticates one.
    current_user: Option<String>,
    login_user: Option<String>,
    /// Go `SessionVars.ActiveRoles`: the roles this session has activated,
    /// which every privilege check widens through and which `CURRENT_ROLE()`
    /// reports. A fresh session starts with its account's DEFAULT roles
    /// (Go activates them in `Auth`); `SET ROLE` replaces the set wholesale.
    active_roles: Arc<Vec<privilege::Account>>,
    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports.
    /// `None` for a session with no connection identity, where the builtin
    /// answers NULL like `CURRENT_USER()` does for an unauthenticated one.
    connection_id: Option<u64>,
    /// Go `SessionVars.ClientCapability & mysql.ClientFoundRows`: negotiated
    /// once by the front end and copied into every statement context.
    client_found_rows: bool,
    /// This connection's lock references over the domain/server lock service.
    advisory_locks: tidb_executor::advisory_lock_state::AdvisoryLockSession,
    selected_lock_keys: Option<tidb_executor::select_lock::SelectedLockKeys>,
    /// Go `SessionVars.PrevLastInsertID`: the id `LAST_INSERT_ID()` reports,
    /// which only a statement that ALLOCATED an auto value updates.
    last_insert_id: u64,
    /// The id the last statement allocated, which the OK packet carries and
    /// which is 0 for a statement that allocated nothing.
    statement_insert_id: u64,
    /// Go's `StatementContext.LastMessage`, retained until the next statement
    /// so the MySQL OK/EOF writer can publish UPDATE's summary text.
    statement_message: String,
    /// Go `StmtCtx.AddSetVarHintRestore`: the session overrides a `SET_VAR`
    /// hint overwrote for the duration of ONE statement, put back when that
    /// statement finishes whether it succeeded or failed.
    set_var_hint_restore: Vec<(String, Option<String>)>,
    /// Go `StmtCtx.PrevAffectedRows`, which is all `ROW_COUNT()` reports: the
    /// preceding statement's affected rows, `-1` after a SELECT, `0`
    /// otherwise. Derived once at the statement boundary from
    /// [`Session::statement_kind`], so the function and the OK packet cannot
    /// disagree about what the statement did.
    prev_row_count: i64,
    /// Go `SessionVars.LastFoundRows`: the row count of the last result set
    /// drained to EOF. Non-result statements leave it unchanged.
    last_found_rows: u64,
    /// The class of the statement currently running, which decides what
    /// `ROW_COUNT()` reports next (Go's `StmtCtx.InSelectStmt` /
    /// `InInsertStmt` / `InUpdateStmt` / `InDeleteStmt` bits, read by
    /// `ResetContextOfStmt`). It is recorded even for a statement that ends
    /// in an error, because Go's bits survive the failure too.
    statement_kind: StatementKind,
    /// Go `SessionVars.TxnCtx.StartTS`, shared with statement contexts so a
    /// lazily opened cluster snapshot becomes visible inside the statement
    /// that opened it.
    current_tso: tidb_executor::CurrentTso,
    /// Go `txn.GetMemBuffer()`: the current transaction's (or, in autocommit,
    /// the current statement's) staged-write tracker. See
    /// [`tidb_executor::kv_table::StagedWrites`].
    ///
    /// Reset by replacing the whole handle with a fresh, empty one at every
    /// transaction/statement boundary that starts clean (`Transaction::open`
    /// for `BEGIN`/lazy activation; the `!in_transaction()` branch in
    /// dispatch for a plain autocommit statement) -- never by walking and
    /// clearing table-by-table, since nothing else ever holds a reference to
    /// the handle being replaced. A statement that CONTINUES an open
    /// transaction is the one case that must NOT reset it: read-your-own-
    /// writes has to see every earlier statement's staged rows until COMMIT
    /// or ROLLBACK ends the transaction.
    staged_writes: std::sync::Arc<tidb_executor::kv_table::StagedWrites>,
    /// The node's server-info syncer, when the deployment has one.
    ///
    /// Go reads `information_schema.TIDB_SERVERS_INFO` through
    /// `infosync.GetAllServerInfo`, which answers the whole cluster when an
    /// etcd client is present and THIS node alone when it is not. `None`
    /// here is the tier that has no node identity at all -- an embedded
    /// session -- and reads back as an empty table.
    server_info_syncer: Option<std::sync::Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    cluster_topology: Option<Arc<tidb_domain::cluster_topology::ClusterTopology>>,
    cluster_config: Option<Arc<tidb_exec::cluster_config::ClusterConfigClient>>,
    /// The domain identity getter shared with server-info publication.
    server_id_getter: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// The cluster schema version this node follows, which `ADMIN SHOW DDL`
    /// reports. Absent on the in-process tier, whose catalog is not a
    /// cluster's.
    cluster_schema_version: Option<std::sync::Arc<dyn Fn() -> i64 + Send + Sync>>,
    /// Go's process-global `workloadrepo.workerCtx`, installed by Domain.
    workload_repository: Option<std::sync::Arc<tidb_workloadrepo::Worker>>,
    /// Go Domain's node-global index-usage collector, read by
    /// `information_schema.TIDB_INDEX_USAGE`.
    index_usage_collector: Arc<tidb_stats_handle_usage_indexusage::Collector>,
    /// Go session `idxUsageCollector`, present only while Domain statistics
    /// updating and execution-info collection are both enabled.
    session_index_usage_collector:
        Option<Arc<Mutex<tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector>>>,
    /// The node's storage-backed current lock-wait reader.
    data_lock_waits: Option<std::sync::Arc<dyn DataLockWaitsProvider>>,
    historical_read_provider: Option<Arc<txn::HistoricalReadProvider>>,
    snapshot_schema_provider: Option<Arc<txn::SnapshotSchemaProvider>>,
    snapshot_schema: Option<(u64, Catalog)>,
    /// The statistics handle's persisted predicate-column usage reader.
    column_stats_usage: Option<std::sync::Arc<dyn ColumnStatsUsageProvider>>,
    /// The persisted analyze-job reader shared by SHOW and ANALYZE_STATUS.
    analyze_status: Option<std::sync::Arc<dyn AnalyzeStatusProvider>>,
    /// Pinned Go's fresh information-schema table-size reader.
    table_storage_stats: Option<std::sync::Arc<dyn TableStorageStatsProvider>>,
    /// Go's session-local `SessionStatsItem`, swept into the statistics
    /// handle independently of statement execution.
    stats_collector: Option<std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>>,
    /// Go `SessionVars.TxnCtx.TableDeltaMap`, published only after commit.
    transaction_table_delta: std::sync::Arc<tidb_stats_handle_usage::TableDeltaMap>,
    /// Parsed-products cache over the raw system-variable text, keyed by
    /// [`vars::SessionVars::generation`]. Go holds these as typed fields on
    /// `SessionVars`, updated by each variable's `SetSession` hook, so a
    /// statement never re-parses `sql_mode` or re-reads thirty cost knobs
    /// through string lookups; the generation stamp buys the same read cost
    /// without a hook per variable. Measured before the cache: the two were
    /// the hottest user-code frames under sysbench, ahead of the parser.
    statement_var_cache:
        std::cell::RefCell<Option<std::sync::Arc<crate::stmt_ctx::StatementVarSnapshot>>>,
    cost_env_cache:
        std::cell::RefCell<Option<(u64, Arc<tidb_planner::find_best_task::coster::CostEnv>)>>,
    prepared_plan_cache_environment_cache:
        std::cell::RefCell<Option<crate::prepared_ast::PreparedPlanCacheEnvironmentCache>>,
    /// Go `SessionVars.LastTxnInfo` (`pkg/sessionctx/variable/session.go:1467`):
    /// client-go's `TxnInfo` JSON for the last transaction that ACTIVATED --
    /// full (with `commit_ts`) after a commit, start-only otherwise, and
    /// untouched by a statement that never took a timestamp (`SELECT 1`).
    /// Empty until the first one, as Go's is.
    last_txn_info: std::cell::RefCell<String>,
    /// Go `SessionVars.LastQueryInfo`, exposed by the read-only
    /// `tidb_last_query_info` getter. The zero value is still useful before a
    /// DML/EXECUTE/SHOW statement records query diagnostics, so retain the
    /// JSON shape Go's `json.Marshal(sessionstates.QueryInfo{})` emits.
    last_query_info: std::cell::RefCell<String>,
    /// Where this session reports the tables its statements bind, for the
    /// node's metadata-lock gate; see [`MdlRelatedTableSink`].
    mdl_related_tables: Option<std::sync::Arc<dyn MdlRelatedTableSink>>,
    /// Go `StmtCtx.LastInsertID`/`LastInsertIDSet`: the id the RUNNING
    /// statement publishes. The session owns the cell and lends it to every
    /// [`tidb_executor::StmtContext`] the statement builds, so an allocating
    /// INSERT and `LAST_INSERT_ID(expr)` write one place, not two.
    published_last_insert_id: Arc<std::sync::Mutex<Option<u64>>>,
    /// Go `SessionVars.RetryInfo`'s auto-increment half: the ids the statement
    /// running now has assigned, kept across a write-conflict replay so the
    /// replay writes the ids the losing attempt picked. It lives on the
    /// session because the retry loop is above the statement -- each attempt
    /// builds its own `StmtContext`, and this is the one thing that has to
    /// cross between them. See `tidb_executor::RetryAutoIds`.
    retry_auto_ids: Arc<std::sync::Mutex<tidb_executor::RetryAutoIds>>,
    /// Go `SessionVars.RowIDShardGenerator`: retains one random shard for
    /// `@@tidb_shard_allocate_step` generated IDs across statement contexts.
    row_id_shards: Arc<std::sync::Mutex<tidb_executor::RowIdShardGenerator>>,
    /// Go's separate non-prepared statement metadata LRU.
    non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache,
    physical_plan_cache: tidb_executor::SessionPlanCache,
    plan_cache_invalidation: Arc<tidb_executor::PlanCacheInvalidation>,
    /// Go `SessionVars.FoundInPlanCache`: whether the statement RUNNING now
    /// found its plan in the cache. Reset for every statement.
    found_in_plan_cache: bool,
    /// Go `SessionVars.PrevFoundInPlanCache`, which is what
    /// `@@last_plan_from_cache` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary, since the reading `SELECT` is
    /// itself never cacheable and would otherwise always answer 0.
    prev_found_in_plan_cache: bool,
    /// Go `SessionVars.userVars`: this session's user variables, keyed
    /// lowercased, each holding a TYPED value (`SetUserVarVal` stores a
    /// `types.Datum`, which is why `SET @i = 5` and `SET @s = '5'` differ).
    ///
    /// The session owns the map and lends the handle to every statement
    /// context, because `@x := expr` writes it from INSIDE expression
    /// evaluation -- once per row, visible to the next select-list item.
    user_vars: Arc<std::sync::Mutex<HashMap<String, Datum>>>,
    /// Go `SessionVars.SequenceState`: the last value THIS SESSION took from
    /// each sequence, keyed by lowercase `db.name`, which is what `LASTVAL`
    /// reports. It is SESSION state, not the sequence's stored counter -- a
    /// fresh session reads `NULL` from a sequence other sessions have advanced
    /// (captured: `lastval` before any `nextval` is `<nil>`).
    sequence_last_values: Arc<std::sync::Mutex<HashMap<i64, i64>>>,
    /// Go `SessionVars.CurrentDB`: the schema an unqualified name resolves in.
    /// Empty means no database is selected, which is Go's `ErrNoDB` case.
    current_db: String,
    /// Whether the front end's parse already opened THIS statement's warning
    /// boundary (go `ResetContextOfStmt` runs once per statement): a second
    /// boundary for the same statement would reset the counts the first one
    /// snapshotted.
    statement_boundary_open: bool,
    /// Privilege requests derived once per prepared text -- Go's
    /// `PlanCacheStmt.VisitInfos`, checked on every EXECUTE without
    /// re-walking the statement.
    /// This connection's registration in the server's process list, which the
    /// front end installs. `None` for a session with no server front; such a
    /// session still answers `SHOW PROCESSLIST` -- with the single row it can
    /// honestly report, itself.
    process: Option<process::ProcessGuard>,
    /// Whether this session holds the `PROCESS` privilege, which decides
    /// what `SHOW PROCESSLIST` and `information_schema.PROCESSLIST` let it
    /// see (Go `hasPriv(ctx, mysql.ProcessPriv)`).
    ///
    /// This is a direct test/front-end override. Normal SQL authorization
    /// reads the shared [`privilege::PrivilegeRegistry`] below, whose
    /// `GRANT PROCESS ON *.*` path is shared by every session.
    has_process_priv: bool,
    /// The server's account/global-privilege registry, shared by every
    /// session a front end opens (see [`privilege::PrivilegeRegistry`]).
    /// `None` for a session with no front end (unit tests, internal use),
    /// which is why every check through it falls back to the pre-existing
    /// bit above rather than treating an absent registry as "no privilege".
    privileges: Option<privilege::PrivilegeRegistry>,
    /// Go's process-wide `privileges.SkipWithGrant` admission copied onto
    /// this connection by the front end. The registry remains attached for
    /// account/role storage, while authorization readers treat the session
    /// as unrestricted.
    privilege_bypassed: bool,
    /// Whether this connection completed a TLS handshake (or an equivalent
    /// trusted gateway assertion). Go keeps the same fact in
    /// `SessionVars.TLSConnectionState`; `SET GLOBAL
    /// require_secure_transport=ON` needs it to avoid locking every current
    /// plaintext administrator out of the server.
    secure_transport: bool,
    /// Negotiated TLS `(cipher, version)` names for `Ssl_cipher` /
    /// `Ssl_version`; `None` on a plaintext connection.
    tls_status: Option<(String, String)>,
    /// Go `session.sandboxMode`: this connection logged in with an EXPIRED
    /// password while the server allowed it, so it may run nothing but the
    /// `SET PASSWORD` / `ALTER USER` that fixes the password. Set by the
    /// front end from the login's verdict, cleared by the statement that
    /// stores a new password.
    sandbox_mode: bool,
    /// Go `SessionVars.Rng`: the generator unseeded `RAND()` advances, shared
    /// across every statement of this session (unlike constant `RAND(N)`,
    /// which owns a fresh per-statement generator -- see `StmtContext`).
    rand: Arc<MysqlRng>,
    /// Go `SessionVars.PreparedStmtNameToID` / `PreparedStmts`: the SQL-level
    /// prepared statements this session holds. Per-session and not shared: a
    /// peer over the same catalog holds its own.
    prepared_statements: prepared_statements::PreparedStore,
    /// Go PlanCacheParams, shared by contexts belonging to the current execution.
    prepared_params: Option<Arc<[Datum]>>,
    /// Go's `sessionBindingHandle` (`pkg/bindinfo/session_handle.go`): the
    /// SQL bindings created with `CREATE [SESSION] BINDING`. Session-scoped
    /// and unshared. Cluster GLOBAL bindings use the node cache below.
    session_bindings: binding::SessionBindings,
    /// Node-owned committed bindings, independent of this session's transaction.
    global_binding_cache: Option<binding_cache::SharedBindingCache>,
    /// Independent transaction owner for cluster global-binding commands.
    global_binding_writer: Option<Arc<dyn binding::GlobalBindingWriter>>,
    /// Go's `DefaultExprPushDownBlacklist` and
    /// `DefaultDisabledLogicalRulesList`, published by `ADMIN RELOAD` and
    /// empty until then -- so an `INSERT` into `mysql.expr_pushdown_blacklist`
    /// alone changes no plan. SHARED across the sessions a front end opens,
    /// because Go's scope for them is one server. See [`blacklist`].
    pushdown_blacklists: blacklist::PushdownBlacklists,
    /// The channel `StmtContext::report_planned_apply` writes: whether the
    /// statement now running planned an Apply. Read by the prepared plan
    /// cache (Go's `PhysicalApply` refusal) and cleared per statement.
    planned_apply: Arc<std::sync::atomic::AtomicBool>,
    /// Go StmtCtx.MPPQueryInfo, shared by contexts of the current statement.
    mpp_query_info: Arc<tidb_executor::MppQueryInfo>,
    /// The cluster runner may retry after the session closes an attempt.
    external_mpp_query_scope: bool,
    mpp_attempt_completed: bool,
    /// Go `ProcessInfo.BriefBinaryPlan`, populated from the ordinary physical
    /// tree before executor construction.
    process_plan_info: Arc<std::sync::Mutex<tidb_executor::ProcessPlanInfo>>,
    /// Set while a binary-protocol EXECUTE runs. Go keeps the plan detail out
    /// of the process list for that path (`executeStmtImpl`,
    /// `pkg/session/session.go`: `execStmt.Name == ""` clears `currentPlan`),
    /// so its statement contexts skip rendering `BriefBinaryPlan`.
    binary_prepared_execution: bool,
    /// Go `SessionVars.FoundInBinding`: whether the statement RUNNING now
    /// took its hints from a binding.
    found_in_binding: bool,
    /// Go `SessionVars.PrevFoundInBinding`, which is what
    /// `@@last_plan_from_binding` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary for the same reason
    /// `@@last_plan_from_cache` is.
    prev_found_in_binding: bool,
}

#[cfg(test)]
thread_local! {
    static STATEMENT_DIGEST_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn normalize_statement_digest(sql: &str) -> (String, tidb_parser::Digest) {
    #[cfg(test)]
    STATEMENT_DIGEST_CALLS.with(|count| count.set(count.get() + 1));
    tidb_parser::normalize_digest(sql)
}

impl Session {
    /// Builds the Go `createSessionWithOpt` state over an already selected
    /// infoschema. Cluster bootstrap is owned by the store/domain, not by each
    /// session opened on it.
    fn unbootstrapped(catalog: SharedCatalog) -> Self {
        let plan_cache_invalidation = catalog
            .lock()
            .expect("catalog poisoned")
            .plan_cache_invalidation();
        Session {
            catalog,
            account_storage_delegated: false,
            context_values: HashMap::new(),
            executor_first_run_breakpoint: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            external_executor_breakpoint_scope: false,
            server_start_timestamp: None,
            tidb_decode_key_cache: std::sync::Mutex::new(None),
            session_memory: tidb_executor::SessionMemory::new(
                tidb_util::memory::DEF_MEM_QUOTA_QUERY,
                tidb_executor::OomAction::Cancel,
                0,
            ),
            statement_result_authority: std::cell::RefCell::new(None),
            current_sql_digest_key: String::new(),
            statement_normalized_sql: None,
            statement_observation: None,
            pending_observation_parse: None,
            routed_statement_observation_depth: 0,
            previous_summary_statement: None,
            statement_stats: tidb_util::topsql_stmtstats::create_statement_stats(),
            txn: None,
            write_sli: tidb_util::sli::TxnWriteThroughputSli::default(),
            local_temporary_tables: Vec::new(),
            global_temporary_data: std::collections::HashMap::new(),
            vars: SessionVars::new(),
            global_config_syncer: None,
            resource_group: "default".to_owned(),
            stmt_hints: tidb_hint::StmtHints::default(),
            active_resource_group: "default".to_owned(),
            warnings: Vec::new(),
            deferred_multi_statement_warning: false,
            in_show_warning: false,
            sys_warning_count: 0,
            sys_error_count: 0,
            current_user: None,
            login_user: None,
            active_roles: Arc::new(Vec::new()),
            connection_id: None,
            client_found_rows: false,
            advisory_locks: tidb_executor::advisory_lock_state::AdvisoryLockSession::default(),
            selected_lock_keys: None,
            last_insert_id: 0,
            statement_insert_id: 0,
            statement_message: String::new(),
            set_var_hint_restore: Vec::new(),
            prev_row_count: 0,
            last_found_rows: 0,
            statement_kind: StatementKind::Other,
            current_tso: tidb_executor::CurrentTso::default(),
            staged_writes: std::sync::Arc::default(),
            server_info_syncer: None,
            cluster_topology: None,
            cluster_config: None,
            server_id_getter: Arc::new(|| 0),
            cluster_schema_version: None,
            workload_repository: None,
            index_usage_collector: Arc::new(tidb_stats_handle_usage_indexusage::Collector::new()),
            session_index_usage_collector: None,
            data_lock_waits: None,
            historical_read_provider: None,
            snapshot_schema_provider: None,
            snapshot_schema: None,
            column_stats_usage: None,
            analyze_status: None,
            table_storage_stats: None,
            stats_collector: None,
            transaction_table_delta: std::sync::Arc::new(
                tidb_stats_handle_usage::TableDeltaMap::new(),
            ),
            mdl_related_tables: None,
            statement_var_cache: std::cell::RefCell::new(None),
            cost_env_cache: std::cell::RefCell::new(None),
            prepared_plan_cache_environment_cache: std::cell::RefCell::new(None),
            last_txn_info: std::cell::RefCell::new(String::new()),
            last_query_info: std::cell::RefCell::new(
                "{\"txn_scope\":\"global\",\"start_ts\":0,\"for_update_ts\":0,\"ru_consumption\":0,\"ru_v2_consumption\":0}"
                    .to_owned(),
            ),
            published_last_insert_id: Arc::default(),
            retry_auto_ids: Arc::default(),
            row_id_shards: Arc::default(),
            non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache::default(),
            physical_plan_cache: tidb_executor::SessionPlanCache::default(),
            plan_cache_invalidation,
            found_in_plan_cache: false,
            prev_found_in_plan_cache: false,
            user_vars: Arc::default(),
            sequence_last_values: Arc::default(),
            current_db: DEFAULT_DATABASE.to_owned(),
            statement_boundary_open: false,
            process: None,
            has_process_priv: false,
            privileges: None,
            privilege_bypassed: false,
            secure_transport: false,
            tls_status: None,
            sandbox_mode: false,
            rand: new_time_seeded_rand(),
            prepared_statements: prepared_statements::PreparedStore::default(),
            prepared_params: None,
            session_bindings: binding::SessionBindings::default(),
            global_binding_cache: None,
            global_binding_writer: None,
            pushdown_blacklists: blacklist::PushdownBlacklists::default(),
            planned_apply: Arc::default(),
            mpp_query_info: Arc::default(),
            external_mpp_query_scope: false,
            mpp_attempt_completed: false,
            process_plan_info: Arc::default(),
            binary_prepared_execution: false,
            found_in_binding: false,
            prev_found_in_binding: false,
        }
    }

    fn breakpoint_notify_func(&self) -> Option<Arc<dyn Fn(String) + Send + Sync + 'static>> {
        self.context_values
            .get(tidb_util::breakpoint::NOTIFY_BREAK_POINT_FUNC_KEY)
            .and_then(|value| value.downcast_ref::<Arc<dyn Fn(String) + Send + Sync + 'static>>())
            .cloned()
    }

    fn reset_mpp_query_info(&mut self) {
        if let Some(info) = Arc::get_mut(&mut self.mpp_query_info) {
            info.reset();
        } else {
            // A retained, closed record set must not share the next query's state.
            self.mpp_query_info = Arc::default();
        }
    }

    /// Holds MPP identity across all attempts owned by the cluster runner.
    #[doc(hidden)]
    pub fn begin_external_mpp_query_scope(&mut self) {
        self.external_mpp_query_scope = true;
        self.mpp_attempt_completed = false;
    }

    /// Retires a completed/failed query after the retry decision. A successful
    /// open record set still owns the query until its later Close.
    #[doc(hidden)]
    pub fn end_external_mpp_query_scope(&mut self, failed: bool) {
        self.external_mpp_query_scope = false;
        if failed || self.mpp_attempt_completed {
            self.reset_mpp_query_info();
        }
        self.mpp_attempt_completed = false;
    }

    /// Starts one server-owned execution attempt. `notify` is false for a
    /// PREPARE metadata probe, which builds no executor in Go.
    #[doc(hidden)]
    pub fn begin_external_executor_breakpoint_scope(&mut self, notify: bool) {
        self.external_executor_breakpoint_scope = true;
        self.executor_first_run_breakpoint
            .store(!notify, std::sync::atomic::Ordering::Release);
    }

    /// Ends the server-owned execution-attempt scope.
    #[doc(hidden)]
    pub fn end_external_executor_breakpoint_scope(&mut self) {
        self.external_executor_breakpoint_scope = false;
    }

    /// Notifies at a cluster pre-lock, which is executor execution in Go but
    /// must precede the fused Rust session runner to carry the locked value.
    #[doc(hidden)]
    pub fn notify_before_executor_first_run(&self) {
        if let Some(observer) = self.statement_phase_observer() {
            observer(tidb_executor::StatementPhase::ExecutorReady);
        }
        if self
            .executor_first_run_breakpoint
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        tidb_util::breakpoint::inject(self, "beforeExecutorFirstRun");
    }
}

impl tidb_util::context::ValueStoreContext for Session {
    type Key = str;

    fn set_value(&mut self, key: &Self::Key, value: Box<dyn std::any::Any + Send + Sync>) {
        self.context_values.insert(key.to_owned(), value);
    }

    fn value(&self, key: &Self::Key) -> Option<&(dyn std::any::Any + Send + Sync)> {
        self.context_values.get(key).map(Box::as_ref)
    }

    fn clear_value(&mut self, key: &Self::Key) {
        self.context_values.remove(key);
    }

    fn get_domain(&self) -> Option<&dyn std::any::Any> {
        None
    }
}

impl Default for Session {
    /// A session on its own empty catalog, with `test` selected as a fresh
    /// TiDB connection has.
    ///
    /// The standalone in-memory session owns its fresh store, so it performs
    /// the one store bootstrap that Go's `BootstrapSession` performs before
    /// serving connections.
    fn default() -> Self {
        let mut session = Session::unbootstrapped(SharedCatalog::default());
        // Go bootstraps the system tables the first time a store comes up
        // (`pkg/session/bootstrap.go`); this catalog is born here, so its
        // bootstrap runs here. See `crate::bootstrap`.
        session.bootstrap_system_tables();
        session
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.statement_stats.set_finished();
        if let Some(collector) = &self.session_index_usage_collector {
            collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .flush();
        }
        if let Some(collector) = &self.stats_collector {
            collector.delete();
        }
        self.advisory_locks.release_all();
    }
}

/// Go `mathutil.NewWithTime()`: seeds a session's unseeded-`RAND()` generator
/// from the wall clock, which is what makes two sessions' `RAND()` sequences
/// differ without either being told to.
fn new_time_seeded_rand() -> Arc<MysqlRng> {
    Arc::new(MysqlRng::new_with_time())
}

pub use tidb_executor::TxnErrorKind;

mod account;
mod account_password;
mod admin_check_arm;
mod analyze_arm;
pub mod binding;
mod binding_arm;
pub mod binding_cache;
pub mod binding_plan_evolution;
pub mod binding_utils;
pub mod blacklist;
mod bootstrap;
mod classify;
pub mod cursor;
mod dispatch;
pub mod embedding;
mod explain_arm;
mod gcutil;
mod identity;
pub mod infoschema;
mod non_prepared_plan_cache;
mod noop;
mod observation;
mod prepared_ast;
mod prepared_plan_cache;
mod prepared_statements;
mod record_set;
mod session_plan_cache;
use record_set::StatementCompletion;
pub use record_set::{OpenedStatement, SessionRecordSet, StatementExecution, StatementRecordSet};
pub mod session_vars;
mod stmt_ctx;
mod table_privilege;
mod txn;
pub use txn::{HistoricalRead, HistoricalReadProvider, SnapshotSchemaProvider};
mod user_table;
pub mod util_config;
mod variables;
pub mod varsutil;
mod warnings;
pub(crate) use classify::{
    statement_kind_of, statement_not_fill_cache, statement_priority_of, StatementKind,
};
pub use classify::{StmtKind, StoredStateChange};
pub use prepared_ast::PreparedAst;
pub(crate) use txn::Transaction;
pub(crate) use variables::datum_text;
pub use warnings::{SqlWarning, WarningLevel};
pub(crate) use warnings::{CHECK_CONSTRAINT_IS_OFF_CODE, CHECK_CONSTRAINT_IS_OFF_MESSAGE};
pub mod privilege;
pub mod process;
mod process_arm;
mod show;
pub mod show_admin;
mod show_create_database;
mod show_create_placement_policy;
mod show_index;
mod stats_lock_arm;
pub mod sysvar;
pub mod vars;
pub use vars::{
    capture_m_view_execution_session_vars, m_view_execution_session_vars_from_job, GlobalSysvars,
    SessionVars, VarError,
};

impl Session {
    /// Starts the cancellation lifetime for the next wire command.
    #[must_use]
    pub fn begin_query_cancellation(&self) -> tidb_executor::StatementCancellation {
        self.session_memory.begin_query_cancellation()
    }

    /// The live transaction timestamp authority used by storage integration.
    #[must_use]
    pub fn current_tso(&self) -> tidb_executor::CurrentTso {
        self.current_tso.clone()
    }

    /// Retains transaction observation at the physical storage boundary.
    pub fn transaction_observer(&self) -> Option<process::ProcessTransactionObserver> {
        self.process
            .as_ref()
            .map(process::ProcessGuard::transaction_observer)
    }

    /// Updates the live `TIDB_TRX` row from the cluster transaction's native
    /// MemBuffer authority.
    pub fn publish_transaction_buffer_metrics(&self, keys: usize, bytes: u64) {
        let Some(process) = self.process.as_ref() else {
            return;
        };
        process.registry().transaction_buffer_metrics(
            process.id(),
            u64::try_from(keys).unwrap_or(u64::MAX),
            i64::try_from(bytes).unwrap_or(i64::MAX),
        );
    }

    /// Enters or leaves Go's `TxnLockAcquiring` state around one synchronous
    /// pessimistic `LockKeys` call.
    pub fn publish_transaction_lock_waiting(&self, waiting: bool) {
        let Some(process) = self.process.as_ref() else {
            return;
        };
        process.registry().transaction_state(
            process.id(),
            if waiting { "LockWaiting" } else { "Running" },
        );
    }

    /// Binds the node's server-info syncer, which is what
    /// `information_schema.TIDB_SERVERS_INFO` reads.
    pub fn set_server_info_syncer(
        &mut self,
        syncer: std::sync::Arc<tidb_domain::serverinfo_syncer::Syncer>,
    ) {
        let info = syncer.local_server_info();
        self.server_id_getter = info.static_info.server_id_getter.unwrap_or_else(|| {
            let id = info.static_info.json_server_id;
            Arc::new(move || id)
        });
        self.cluster_topology = Some(Arc::new(
            tidb_domain::cluster_topology::ClusterTopology::new(Arc::clone(&syncer), None),
        ));
        self.server_info_syncer = Some(syncer);
    }

    fn effective_replica_read(
        &self,
        mode: tidb_executor::ReplicaReadType,
    ) -> tidb_executor::ReplicaReadType {
        // Go GetReplicaRead gives the statement hint priority over the domain
        // switch. Planner eligibility and execution must use the same decision.
        if self.stmt_hints.has_replica_read_hint {
            return tidb_executor::ReplicaReadType::from_raw(self.stmt_hints.replica_read);
        }
        if mode == tidb_executor::ReplicaReadType::ClosestAdaptive
            && self
                .cluster_topology
                .as_ref()
                .is_some_and(|topology| !topology.adaptive_enabled())
        {
            tidb_executor::ReplicaReadType::Leader
        } else {
            mode
        }
    }

    /// Installs the process's shared internal HTTP configuration retriever.
    pub fn set_cluster_config_client(
        &mut self,
        client: Arc<tidb_exec::cluster_config::ClusterConfigClient>,
    ) {
        self.cluster_config = Some(client);
    }

    /// Installs process-owned topology and replica-read policy.
    pub fn set_cluster_topology(
        &mut self,
        topology: Arc<tidb_domain::cluster_topology::ClusterTopology>,
    ) {
        self.cluster_topology = Some(topology);
    }

    /// The `host:port` Go persists as an analyze job's instance, or
    /// `unknown` when server-info discovery is unavailable.
    #[must_use]
    pub fn analyze_job_instance(&self) -> String {
        self.server_info_syncer.as_ref().map_or_else(
            || "unknown".to_owned(),
            |syncer| {
                let info = syncer.local_server_info();
                tidb_domain::serverinfo_syncer::join_host_port(
                    &info.static_info.ip,
                    info.static_info.port,
                )
            },
        )
    }

    /// The node's followed cluster schema version, or 0 for a tier with no
    /// cluster to follow -- the same source `ADMIN SHOW DDL` reports. This is
    /// Go's `domainSchemaVer` in `preprocess.go:2264` (the LATEST domain
    /// version, not the transaction's pinned one), which is the version the
    /// metadata-lock recording stamps a table's first use with.
    pub(crate) fn cluster_schema_version_now(&self) -> i64 {
        self.cluster_schema_version
            .as_ref()
            .map_or(0, |source| source())
    }

    /// Go's commit-side `LastTxnInfo` write: client-go marshals `TxnInfo`
    /// in its commit callback (`.oracle/client-go/txnkv/transaction/
    /// txn.go:1078-1092`), field for field. The narrow tier is one node
    /// committing locally, so the mode is the plain "2pc" and the fallback
    /// and pipeline fields are their zero values -- the same zeros Go's
    /// struct marshals for a plain transaction.
    pub(crate) fn set_last_txn_info_committed(&self, start_ts: u64, commit_ts: u64) {
        *self.last_txn_info.borrow_mut() = format!(
            "{{\"txn_scope\":\"global\",\"start_ts\":{start_ts},\"commit_ts\":{commit_ts},\"txn_commit_mode\":\"2pc\",\"async_commit_fallback\":false,\"one_pc_fallback\":false,\"pipelined\":false,\"flush_wait_ms\":0}}"
        );
    }

    /// Go `setLastTxnInfoBeforeTxnEnd` (`pkg/session/session.go:1056-1069`):
    /// a transaction that activated but did not commit -- read-only, rolled
    /// back, stale -- records the same struct with only scope and start set,
    /// zeros included, because Go marshals the whole thing.
    pub(crate) fn set_last_txn_info_started(&self, start_ts: u64) {
        *self.last_txn_info.borrow_mut() = format!(
            "{{\"txn_scope\":\"global\",\"start_ts\":{start_ts},\"commit_ts\":0,\"txn_commit_mode\":\"\",\"async_commit_fallback\":false,\"one_pc_fallback\":false,\"pipelined\":false,\"flush_wait_ms\":0}}"
        );
    }

    pub(crate) fn last_txn_info_value(&self) -> String {
        self.last_txn_info.borrow().clone()
    }

    pub(crate) fn last_query_info_value(&self) -> String {
        self.last_query_info.borrow().clone()
    }

    /// Binds where this session reports the tables its statements bind --
    /// Go `GetRelatedTableForMDL()` (`pkg/sessionctx/variable/session.go`),
    /// filled by the planner at table resolution
    /// (`pkg/planner/core/preprocess.go:2243-2270`) and read by the node's
    /// metadata-lock gate (`RemoveLockDDLJobs`). A session with no sink is a
    /// tier with no cluster DDL owner to gate.
    pub fn set_mdl_related_table_sink(&mut self, sink: std::sync::Arc<dyn MdlRelatedTableSink>) {
        self.mdl_related_tables = Some(sink);
    }

    /// Binds the node's cluster schema version, which `ADMIN SHOW DDL`
    /// reports as `SCHEMA_VER`.
    pub fn set_cluster_schema_version_source(
        &mut self,
        source: std::sync::Arc<dyn Fn() -> i64 + Send + Sync>,
    ) {
        self.cluster_schema_version = Some(source);
    }

    /// Binds the process workload-repository worker used by its sysvars and
    /// `ADMIN CREATE WORKLOAD SNAPSHOT`.
    pub fn set_workload_repository(&mut self, worker: std::sync::Arc<tidb_workloadrepo::Worker>) {
        self.workload_repository = Some(worker);
    }

    /// Installs the Domain-owned index-usage collector shared by every
    /// session on the node.
    pub fn set_index_usage_collector(
        &mut self,
        collector: Arc<tidb_stats_handle_usage_indexusage::Collector>,
    ) {
        self.index_usage_collector = collector;
    }

    /// Installs Go's Domain-owned session index-usage collector.
    pub fn set_session_index_usage_collector(
        &mut self,
        collector: tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector,
    ) {
        self.session_index_usage_collector = Some(Arc::new(Mutex::new(collector)));
    }

    /// Installs Go's Domain-owned session statistics collector.
    pub fn set_stats_collector(
        &mut self,
        collector: std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>,
    ) {
        self.stats_collector = Some(collector);
    }

    /// Publishes Go's committed `TxnCtx.TableDeltaMap` to the session collector.
    pub fn publish_table_delta(&self) {
        let delta = self.transaction_table_delta.get_delta_and_reset();
        if let Some(collector) = &self.stats_collector {
            for (table_id, item) in delta {
                if table_id > 0 {
                    collector.update(table_id, item.delta, item.count);
                }
            }
        } else if !delta.is_empty() {
            // The standalone catalog has no Domain stats worker. Preserve Go's
            // post-commit collector boundary by publishing committed modify
            // counts into the shared catalog only after its transaction lands.
            let mut catalog = self
                .catalog
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (table_id, item) in delta {
                if table_id > 0 {
                    catalog.record_stats_modify_count(table_id, item.count);
                }
            }
        }
    }

    /// Discards Go's rolled-back `TxnCtx.TableDeltaMap`.
    pub fn clear_table_delta(&self) {
        self.transaction_table_delta.reset();
    }

    /// Clones Go's table-delta part of one transaction savepoint.
    #[must_use]
    pub fn table_delta_savepoint(
        &self,
    ) -> std::collections::HashMap<i64, tidb_stats_handle_usage::TableDelta> {
        self.transaction_table_delta.snapshot()
    }

    /// Restores Go's table-delta part of one transaction savepoint.
    pub fn restore_table_delta_savepoint(
        &self,
        savepoint: std::collections::HashMap<i64, tidb_stats_handle_usage::TableDelta>,
    ) {
        self.transaction_table_delta.restore(savepoint);
    }

    /// Installs the same storage authority `kv.Storage.GetLockWaits` reads in
    /// Go for `information_schema.DATA_LOCK_WAITS`.
    pub fn set_data_lock_waits_provider(
        &mut self,
        provider: std::sync::Arc<dyn DataLockWaitsProvider>,
    ) {
        self.data_lock_waits = Some(provider);
    }

    /// Installs the node-global persisted predicate-column usage reader.
    pub fn set_column_stats_usage_provider(
        &mut self,
        provider: std::sync::Arc<dyn ColumnStatsUsageProvider>,
    ) {
        self.column_stats_usage = Some(provider);
    }

    /// Installs the node-global persisted analyze-job reader.
    pub fn set_analyze_status_provider(
        &mut self,
        provider: std::sync::Arc<dyn AnalyzeStatusProvider>,
    ) {
        self.analyze_status = Some(provider);
    }

    /// Installs the node-global restricted reader used by
    /// `information_schema.TABLES` and `PARTITIONS`.
    pub fn set_table_storage_stats_provider(
        &mut self,
        provider: std::sync::Arc<dyn TableStorageStatsProvider>,
    ) {
        self.table_storage_stats = Some(provider);
    }

    /// Go `ShowDDLExec.Next` (`executor/show_ddl.go`): one row describing the
    /// DDL owner and this node.
    ///
    /// Two of Go's six columns are structurally empty here rather than
    /// invented. `RUNNING_JOBS` and `QUERY` join the owner's in-flight job
    /// list, and this node publishes a catalog change in one transaction
    /// instead of queueing a job, so no later statement can ever observe one
    /// in flight. The owner columns name THIS node: it runs no election
    /// (`pkg/owner` is unported), and every catalog change it accepts, it
    /// performs itself, which is what a single-node deployment reports.
    pub(crate) fn show_ddl_rows(&self) -> Vec<Vec<tidb_datatype::Datum>> {
        use tidb_datatype::Datum;
        let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        let schema_version = self
            .cluster_schema_version
            .as_ref()
            .map_or(0, |source| source());
        let (id, address) = self.server_info_syncer.as_ref().map_or_else(
            || (String::new(), String::new()),
            |syncer| {
                let info = syncer.local_server_info();
                (
                    info.static_info.id.clone(),
                    tidb_domain::serverinfo_syncer::join_host_port(
                        &info.static_info.ip,
                        info.static_info.port,
                    ),
                )
            },
        );
        vec![vec![
            Datum::Int(schema_version),
            text(&id),
            text(&address),
            text(""),
            text(&id),
            text(""),
        ]]
    }

    /// Go fetchClusterConfig checks CONFIG before discovery and reports per-node
    /// failures as warnings while retaining successful nodes.
    fn cluster_config_table_rows(
        &mut self,
        filters: &[tidb_planner::cluster_table_extractor::ClusterTableFilter],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        if filters.iter().all(|filter| filter.skip_request()) {
            return Ok(Vec::new());
        }
        if !self.has_scoped_privilege("", "", privilege::GlobalPriv::Config) {
            return Err(DriverError::SpecificAccessDenied("CONFIG".into()));
        }
        let client = self.cluster_config.clone().ok_or_else(|| {
            DriverError::unsupported("CLUSTER_CONFIG live retrieval is not installed")
        })?;
        let topology = self.cluster_topology.clone().ok_or_else(|| {
            DriverError::unsupported("CLUSTER_CONFIG cluster discovery is not installed")
        })?;
        let mut warnings = Vec::new();
        let servers = topology.servers(&mut warnings);
        for warning in warnings.drain(..) {
            self.append_warning(WarningLevel::Warning, 1105, warning);
        }
        let servers: Vec<_> = servers
            .map_err(DriverError::unsupported)?
            .into_iter()
            .filter(|server| {
                filters
                    .iter()
                    .any(|filter| filter.matches(&server.server_type, &server.address))
            })
            .collect();
        let rows = client.fetch(&servers, &mut warnings);
        for warning in warnings {
            self.append_warning(WarningLevel::Warning, 1105, warning);
        }
        Ok(rows)
    }

    /// Go's seven-source GetClusterServerInfo in dataForTiDBClusterInfo column order.
    pub(crate) fn cluster_info_table_rows(
        &mut self,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use tidb_datatype::Datum;
        let Some(topology) = self.cluster_topology.as_ref() else {
            return Ok(Vec::new());
        };
        let mut warnings = Vec::new();
        let servers = topology.servers(&mut warnings);
        for warning in warnings {
            self.append_warning(WarningLevel::Warning, 1105, warning);
        }
        let servers = servers.map_err(DriverError::unsupported)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs() as i64);
        let redact = self.sem_hides_cluster_metadata();
        Ok(servers
            .into_iter()
            .map(|info| {
                let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
                let (start, uptime) = if info.start_timestamp > 0 {
                    (
                        info.start_timestamp,
                        go_uptime_string(now - info.start_timestamp),
                    )
                } else {
                    (now, String::new())
                };
                vec![
                    text(&info.server_type),
                    if redact {
                        text(&info.server_id.to_string())
                    } else {
                        text(&info.address)
                    },
                    if redact {
                        Datum::Null
                    } else {
                        text(&info.status_address)
                    },
                    text(&info.version),
                    text(&info.git_hash),
                    if redact {
                        Datum::Null
                    } else {
                        datetime_datum(start)
                    },
                    if redact { Datum::Null } else { text(&uptime) },
                    Datum::UInt(info.server_id),
                ]
            })
            .collect())
    }

    /// Go infoschema.GetInstanceAddr uses local status identity. This does not
    /// discover peers: loss of etcd cannot relabel or hide local process rows.
    pub(crate) fn cluster_instance_address(&self) -> String {
        let Some(syncer) = self.server_info_syncer.as_ref() else {
            return String::new();
        };
        let info = syncer.local_server_info();
        if self.sem_hides_cluster_metadata() {
            info.static_info.id.clone()
        } else {
            tidb_domain::serverinfo_syncer::join_host_port(
                &info.static_info.ip,
                info.static_info.status_port,
            )
        }
    }

    /// Go `setDataForServersInfo` (`infoschema_reader.go:2730`): one row per
    /// server `GetAllServerInfo` reports, in Go's column order.
    ///
    /// Without a syncer the table is EMPTY rather than invented: a tier with
    /// no node identity has no server to report. With one and no etcd
    /// client, the syncer answers this node alone -- Go's `etcdCli == nil`
    /// path -- and the same call picks up peers once a client is present.
    ///
    /// The rows are ordered by id so the table reads deterministically;
    /// Go's map iteration leaves the order unspecified.
    pub(crate) fn tidb_servers_info_table_rows(
        &self,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use tidb_datatype::Datum;
        let Some(syncer) = self.server_info_syncer.as_ref() else {
            return Ok(Vec::new());
        };
        let all = syncer.all_server_info().map_err(DriverError::unsupported)?;
        let mut ids: Vec<&String> = all.keys().collect();
        ids.sort();
        let redact = self.sem_hides_cluster_metadata();
        Ok(ids
            .into_iter()
            .map(|id| {
                let info = &all[id];
                let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
                vec![
                    text(&info.static_info.id),
                    if redact {
                        Datum::Null
                    } else {
                        text(&info.static_info.ip)
                    },
                    Datum::Int(info.static_info.port as i64),
                    Datum::Int(info.static_info.status_port as i64),
                    text(&info.static_info.lease),
                    text(&info.static_info.version_info.version),
                    text(&info.static_info.version_info.git_hash),
                    text(&tidb_domain::serverinfo_syncer::build_string_from_labels(
                        &info.dynamic_info.labels,
                    )),
                ]
            })
            .collect())
    }

    /// A fresh session with its own empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Session::default()
    }

    /// Go `SessionVars.CurrentDB`. Empty when no database is selected.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.current_db
    }

    /// Go `clientConn.useDB`: selects a schema outside the statement path.
    ///
    /// The connection front end reaches this for the handshake's initial
    /// database and for `COM_INIT_DB`, which Go both route through `useDB`.
    /// Taking the name directly instead of re-rendering `use \`name\`` keeps
    /// backquotes and other identifier syntax out of the picture entirely.
    /// The process-list row is refreshed for the same reason a statement
    /// refreshes it: a peer's `SHOW PROCESSLIST` reports the schema now
    /// selected.
    pub fn select_database(&mut self, name: &str) -> Result<(), DriverError> {
        self.use_database(name)?;
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_finished(guard.id(), &self.current_db, &self.status_text());
        }
        Ok(())
    }

    /// Refreshes the process-list row's status snapshot after a transaction
    /// control statement. The control path bypasses the ordinary statement
    /// pipeline, so the row would otherwise keep the PREVIOUS statement's
    /// `State` text (`in transaction; autocommit` after a `ROLLBACK`) — Go's
    /// `clientConn` writes the live `SessionVars.Status` word on the control
    /// statement's own OK packet.
    pub fn refresh_process_status(&self) {
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_finished(guard.id(), &self.current_db, &self.status_text());
        }
    }

    /// Selects NO schema, which is the state a connection that authenticated
    /// without an initial database is in: Go's `SessionVars.CurrentDB` is
    /// empty and every unqualified name is `ErrNoDB` (`Error 1046`) until a
    /// `USE` runs.
    ///
    /// [`Session::default`] selects `test` because that is what a `mysql`
    /// client's own default gives a fresh connection; a front end whose
    /// handshake carried no schema at all -- or a harness replaying one --
    /// needs to say so, and this is the one way to.
    pub fn deselect_database(&mut self) {
        self.current_db = String::new();
    }

    /// Go `executeUse`: an unknown schema is `ErrDatabaseNotExists`, and the
    /// switch also updates `collation_database`.
    fn use_database(&mut self, name: &str) -> Result<(), DriverError> {
        // Go `executeUse` (`executor/simple.go` around line 608) refuses an
        // invisible schema with 1044 BEFORE it looks the schema up, so a
        // schema the account cannot see is never distinguishable from one
        // that does not exist.
        self.require_visible_database(name)?;
        let exists = self.with_catalog_mut(|catalog| Ok(catalog.has_database(name)))?;
        if !exists {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                name.to_owned(),
            )));
        }
        self.current_db = name.to_owned();
        Ok(())
    }

    /// The current database, or Go's `ErrNoDB` when none is selected.
    fn require_current_database(&self) -> Result<&str, DriverError> {
        if self.current_db.is_empty() {
            return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
        }
        Ok(&self.current_db)
    }

    /// Go `LAST_INSERT_ID()`: the first id the most recent ALLOCATING
    /// statement handed out. A statement that allocated nothing -- an explicit
    /// auto value, a table with no auto column, an UPDATE -- leaves it as it
    /// was, which is what MySQL and TiDB both do.
    #[must_use]
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// The auto-increment ids the running statement has assigned, which a
    /// caller that RUNS THE STATEMENT AGAIN rewinds between attempts and
    /// clears when the statement is finally over. See
    /// `tidb_executor::RetryAutoIds`.
    #[must_use]
    pub fn retry_auto_ids(&self) -> &Arc<std::sync::Mutex<tidb_executor::RetryAutoIds>> {
        &self.retry_auto_ids
    }

    /// The id the last statement allocated, which the OK packet reports and
    /// which is 0 when the statement allocated nothing.
    #[must_use]
    pub fn statement_insert_id(&self) -> u64 {
        self.statement_insert_id
    }

    /// The info string produced by the statement most recently executed.
    #[must_use]
    pub fn statement_message(&self) -> &str {
        &self.statement_message
    }

    /// Clears the statement info for command paths that do not enter the
    /// normal executor lifecycle (for example `SET` and routed DDL).
    pub fn clear_statement_message(&mut self) {
        self.statement_message.clear();
    }

    /// Install the domain notification owner; it survives scratch globals used by SET.
    pub fn set_global_config_syncer(
        &mut self,
        syncer: Arc<tidb_domain::globalconfigsync::GlobalConfigSyncer>,
    ) {
        self.global_config_syncer = Some(syncer);
    }

    /// The session's variables.
    #[must_use]
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// Applies one already-validated internal-session variable update.
    ///
    /// Go's statistics session reset writes the typed `SessionVars` fields
    /// directly before handing a pooled session to its caller. Keeping this
    /// narrow entry point on the session preserves that ownership boundary:
    /// server-side system sessions do not manufacture a client `SET`
    /// statement merely to synchronize their state.
    pub fn set_internal_system_var(
        &mut self,
        name: &str,
        value: impl Into<String>,
    ) -> Result<(), VarError> {
        self.vars.set_system(name, value.into()).map(|_| ())
    }

    /// Installs the hosting server's start timestamp (Go
    /// `ServerInfo.StartTimestamp`), the `Uptime` provider's input.
    pub fn set_server_start_timestamp(&mut self, unix_seconds: i64) {
        self.server_start_timestamp = Some(unix_seconds);
    }

    /// The live `@@wait_timeout` used by the MySQL connection before reading
    /// its next command packet.
    ///
    /// The registry validates this as an unsigned seconds value before it can
    /// enter the session, so parsing here cannot depend on client input shape.
    #[must_use]
    pub fn wait_timeout(&self) -> Duration {
        // Read once per command (Go `getSessionVarsWaitTimeout`, one
        // `systems[name]` probe). The name-to-index step is resolved once
        // for the process so the per-command read is a plain slot read.
        static INDEX: crate::sysvar::StaticSysVarIndex =
            crate::sysvar::StaticSysVarIndex::new("wait_timeout");
        let seconds = self
            .vars
            .system_value_at(INDEX.get())
            .expect("wait_timeout is a registered session variable")
            .parse::<u64>()
            .expect("wait_timeout validation stores unsigned decimal seconds");
        Duration::from_secs(seconds)
    }

    /// Go's typed `SessionVars.MaxAllowedPacket`, used directly by the packet
    /// reader and expression builtins.
    #[must_use]
    pub const fn max_allowed_packet(&self) -> u64 {
        self.vars.max_allowed_packet()
    }

    /// A session sharing `catalog` with its peers.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        Session::unbootstrapped(catalog)
    }

    /// Installs the server-owned spill policy for every statement created by
    /// this session.
    pub fn set_spill_storage(&mut self, storage: Arc<tidb_util::spill_storage::SpillStorage>) {
        self.session_memory.set_spill_storage(storage);
    }

    /// Installs the server-owned process memory arbitrator for statements this
    /// session starts after the authority becomes available.
    pub fn set_mem_arbitrator(&mut self, arbitrator: Arc<tidb_util::memory::MemArbitrator>) {
        self.session_memory.set_mem_arbitrator(arbitrator);
    }

    /// The shared catalog handle, for opening a peer session over the same
    /// schema state.
    #[must_use]
    pub fn shared_catalog(&self) -> SharedCatalog {
        Arc::clone(&self.catalog)
    }

    /// The number of `?` markers a statement carries, which
    /// `COM_STMT_PREPARE` reports to the client.
    pub fn parameter_count(&self, sql: &str) -> Result<usize, DriverError> {
        tidb_executor::parameter_count(sql, self.scanner_sql_mode())
    }

    /// Runs one statement with its prepared-statement parameters bound.
    ///
    /// Go installs the execute-time values on the parsed statement's own
    /// markers; this tier reaches execution through SQL text, so the markers
    /// become literals and the statement is restored before it runs. A byte
    /// string that is not UTF-8 becomes a hex literal, so no value is lost in
    /// that round trip.
    pub fn run_with_params(
        &mut self,
        sql: &str,
        params: &[Datum],
    ) -> Result<StmtOutput, DriverError> {
        // The count is checked even when no values were sent, so a statement
        // with an unbound marker is Go's ErrWrongParamCount rather than a
        // parse-time surprise deeper in.
        if params.is_empty() && self.parameter_count(sql)? == 0 {
            return self.run_with_columns(sql);
        }
        let bound = tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode())?;
        self.run_with_columns(&bound)
    }

    /// Runs one parameterized statement and returns the exact policy needed
    /// to retain a row result after the statement completes.
    ///
    /// The authority is present only for [`StmtOutput::Rows`]. It is captured
    /// inside the statement lifecycle before `SET_VAR` restoration; a server
    /// must not reconstruct it from post-statement session variables.
    pub fn run_with_params_and_result_authority(
        &mut self,
        sql: &str,
        params: &[Datum],
    ) -> Result<(StmtOutput, Option<ResultMaterializationAuthority>), DriverError> {
        if params.is_empty() && self.parameter_count(sql)? == 0 {
            return self.run_with_columns_internal(sql, true);
        }
        let bound = tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode())?;
        self.run_with_columns_internal(&bound, true)
    }

    /// Executes the retained PREPARE tree and captures its result policy
    /// before the statement lifecycle restores session hints.
    pub fn run_prepared_with_result_authority(
        &mut self,
        prepared: &PreparedAst,
        params: &[Datum],
    ) -> Result<(StmtOutput, Option<ResultMaterializationAuthority>), DriverError> {
        self.restore_statement_variables();
        let statement = prepared.bind(params)?;
        let (effective_statement, binding_sql) =
            self.prepared_statement_with_binding(prepared.statement());
        let mut effective_statement = effective_statement.into_owned();
        self.rewrite_fts_for_planning(&mut effective_statement);
        if self.prepared_plan_cache_allowed_for_statement(&effective_statement) {
            if let Some(cached) = prepared.select_plan().as_ref().and_then(|plan| {
                self.bind_cached_prepared_select_for_statement(
                    plan,
                    params,
                    &effective_statement,
                    binding_sql.as_deref(),
                )
            }) {
                let result = self.execute_prepared_select_internal(
                    &cached,
                    prepared.sql(),
                    prepared.privilege_requests(),
                    true,
                );
                // The nested statement boundary consumes the binding flag.
                if binding_sql.is_some() {
                    self.found_in_binding = true;
                }
                return result;
            }
        }
        self.run_with_columns_using(prepared.sql(), true, |session| {
            session.execute_prepared_ast(prepared.sql(), statement, prepared.privilege_requests())
        })
    }

    /// Opens a prepared statement while retaining a lazy query record set for
    /// the wire front end. The collected variant above is kept for callers
    /// that need an owned row vector; COM_STMT_EXECUTE uses this door so a
    /// SELECT can stream executor chunks directly to the client.
    pub fn open_prepared_with_result_authority(
        &mut self,
        prepared: &PreparedAst,
        params: &[Datum],
    ) -> Result<(OpenedStatement, Option<ResultMaterializationAuthority>), DriverError> {
        self.restore_statement_variables();
        let statement = prepared.bind(params)?;
        let (effective_statement, binding_sql) =
            self.prepared_statement_with_binding(prepared.statement());
        let mut effective_statement = effective_statement.into_owned();
        self.rewrite_fts_for_planning(&mut effective_statement);
        if self.prepared_plan_cache_allowed_for_statement(&effective_statement) {
            if let Some(cached) = prepared.select_plan().as_ref().and_then(|plan| {
                self.bind_cached_prepared_select_for_statement(
                    plan,
                    params,
                    &effective_statement,
                    binding_sql.as_deref(),
                )
            }) {
                let opened = self.open_prepared_record_set_for(&cached, prepared)?;
                if binding_sql.is_some() {
                    self.found_in_binding = true;
                }
                let authority = matches!(opened, OpenedStatement::Rows(_))
                    .then(|| self.result_materialization_authority());
                return Ok((opened, authority));
            }
        }
        let opened = self.open_bound_record_set_for(statement, prepared)?;
        let authority = matches!(opened, OpenedStatement::Rows(_))
            .then(|| self.result_materialization_authority());
        Ok((opened, authority))
    }

    /// Plans a bound prepared query for its result metadata without opening
    /// or draining a storage reader. Go's `PrepareExec` builds the
    /// `PlanCacheStmt` and takes its schema from the plan; it does not execute
    /// the query with NULL marker values merely to discover the columns.
    /// Keeping this operation separate from statement execution
    /// prevents a large range such as sysbench's `BETWEEN ? AND ?` from
    /// scanning its table during `COM_STMT_PREPARE`.
    pub fn plan_bound_prepared_columns(
        &mut self,
        mut statement: Stmt,
    ) -> Result<Vec<(String, FieldType)>, DriverError> {
        let parameters = tidb_executor::bound_parameter_values(&mut statement)?;
        // Go's expression rewriter validates and resolves variables during
        // planning, including PREPARE's metadata-only path.
        self.bind_variables(&mut statement)?;
        let Stmt::Query(query) = statement else {
            return Err(DriverError::unsupported(
                "prepared metadata requires a query statement",
            ));
        };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
            return Err(DriverError::unsupported(
                "prepared metadata for set operations is not supported here",
            ));
        };
        let current_db = self.current_db.clone();
        let ctx = self
            .statement_context(false)
            .with_prepared_params(parameters.unwrap_or_default());
        self.with_catalog_mut(|catalog| {
            tidb_executor::plan_select_meta_stmt(&select, catalog, &current_db, &ctx)
        })
    }

    /// Resolves PREPARE result metadata with NULL markers without publishing
    /// an execution cache entry or access-path pins.
    pub fn probe_prepared(&mut self, prepared: &PreparedAst) -> Result<StmtOutput, DriverError> {
        let values = vec![Datum::Null; prepared.parameter_count()];
        let statement = tidb_executor::bind_prepared_statement(prepared.statement(), &values)?;
        self.run_with_columns_using(prepared.sql(), false, |session| {
            session.execute_prepared_ast(prepared.sql(), statement, prepared.privilege_requests())
        })
        .map(|(output, _)| output)
    }

    /// Runs an owned bound statement while reusing the prepared SQL text.
    /// Binary-protocol callers already have both values, so restoring the AST
    /// merely to obtain text would add work to every execute.
    pub fn run_parsed_bound_owned_for(
        &mut self,
        bound: tidb_ast::Stmt,
        prepared: &PreparedAst,
    ) -> Result<StmtOutput, DriverError> {
        self.run_parsed_bound_owned_with_requests(
            bound,
            prepared.sql(),
            prepared.privilege_requests(),
        )
    }

    /// [`Self::run_parsed_bound_owned_for`] with the retained definition's
    /// pieces already in hand (the text `EXECUTE` path keeps its own).
    pub(crate) fn run_parsed_bound_owned_with_requests(
        &mut self,
        bound: tidb_ast::Stmt,
        sql: &str,
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_using(sql, false, move |session| {
            session.execute_statement_parsed(bound, sql, privilege_requests)
        })
        .map(|(output, _)| output)
    }

    /// [`Session::run`] over the statement a front end already parsed from
    /// `sql`: Go's `ExecuteStmt` takes the one `ast.StmtNode` `ParseSQL`
    /// produced, so a command's text is lexed and parsed exactly once.
    pub fn run_parsed(&mut self, stmt: Stmt, sql: &str) -> Result<StmtResult, DriverError> {
        Ok(match self.run_with_columns_parsed(stmt, sql)? {
            StmtOutput::Rows { rows, .. } => StmtResult::Rows(rows),
            StmtOutput::Affected(count) => StmtResult::Affected(count),
            StmtOutput::Done(created) => StmtResult::Done(created),
        })
    }

    /// [`Session::run_with_columns`] over an already-parsed statement.
    pub fn run_with_columns_parsed(
        &mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_using(sql, false, move |session| {
            session.execute_statement_owned(stmt, sql)
        })
        .map(|(output, _)| output)
    }

    /// Runs one SQL statement (Go `session.ExecuteStmt`): parses, dispatches by
    /// statement kind, and executes over the session catalog.
    /// Go `handleQuery`'s multi-statement admission (`conn.go:1861-1904`).
    ///
    /// The COM_QUERY text parses as a WHOLE, and a text holding more than one
    /// statement is admitted by the client's `CLIENT_MULTI_STATEMENTS`
    /// capability; without it, `@@tidb_multi_statement_mode` decides — `OFF`
    /// (the default) refuses with server error 8130, `ON` admits, and any
    /// other value is Go's `default:` arm, admitting with warning 8130. Each
    /// admitted statement's own source text — the parser records it,
    /// delimiter included — is returned for the caller to run in order.
    ///
    /// A parse failure is NOT reported here: the text is handed back whole,
    /// so the ordinary single-statement path parses it again and its error
    /// carries the session's full diagnostic shape.
    pub fn split_statements(
        &mut self,
        sql: &str,
        client_multi_statements: bool,
    ) -> Result<Vec<String>, DriverError> {
        const DISABLED: &str = "client has multi-statement capability disabled. Run SET \
                                GLOBAL tidb_multi_statement_mode='ON' after you understand \
                                the security risk";
        // Go parses this text ONCE; the admission parse below is this tier's
        // second pass over every COM_QUERY. A text that provably holds exactly
        // one statement needs no admission answer from a parser -- the
        // single-statement arm returns it whole either way (parse success AND
        // parse failure both land on `vec![sql.to_owned()]`) -- so the scan
        // below skips that second pass entirely, sql_mode read included.
        if tidb_parser::is_sole_statement(sql) {
            return Ok(vec![sql.to_owned()]);
        }
        let mode = self.scanner_sql_mode();
        let Ok(statements) = tidb_parser::parse_multi_with_sql_mode(sql, mode) else {
            return Ok(vec![sql.to_owned()]);
        };
        // Go `conn.go:1874`: a text that parses to ZERO statements — all
        // whitespace, comments, or bare semicolons — is answered with a plain
        // OK packet, never a syntax error. The empty list is that answer.
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        if statements.len() == 1 {
            return Ok(vec![sql.to_owned()]);
        }
        if !client_multi_statements {
            match self.vars().multi_statement_mode() {
                0 => {
                    return Err(DriverError::ParseCoded {
                        errno: 8130,
                        message: DISABLED.to_owned(),
                    })
                }
                1 => {}
                _ => self.deferred_multi_statement_warning = true,
            }
        }
        Ok(statements
            .iter()
            .map(|statement| String::from_utf8_lossy(statement.text()).into_owned())
            .collect())
    }

    /// Go `conn.go:2262`: after the LAST statement of a multi-statement
    /// COM_QUERY runs, the parser-level warnings its admission produced are
    /// appended to that statement's context, where the next `SHOW WARNINGS`
    /// reads them. An aborted chain never reaches this, exactly as Go's
    /// error return drops `parserWarns`.
    pub fn flush_multi_statement_warning(&mut self) {
        if std::mem::take(&mut self.deferred_multi_statement_warning) {
            self.append_warning(
                WarningLevel::Warning,
                8130,
                "client has multi-statement capability disabled. Run SET GLOBAL \
                 tidb_multi_statement_mode='ON' after you understand the security risk"
                    .to_owned(),
            );
        }
    }

    /// Parses and executes one SQL statement, reducing the output to rows
    /// or an affected count.
    pub fn run(&mut self, sql: &str) -> Result<StmtResult, DriverError> {
        Ok(match self.run_with_columns(sql)? {
            StmtOutput::Rows { rows, .. } => StmtResult::Rows(rows),
            StmtOutput::Affected(count) => StmtResult::Affected(count),
            StmtOutput::Done(created) => StmtResult::Done(created),
        })
    }

    /// Applies an account statement to the installed scratch registry while
    /// its caller owns persistence in a separate system transaction. Internal
    /// mysql.user mirrors must not write through the user connection as well.
    pub fn run_with_delegated_account_storage(
        &mut self,
        sql: &str,
    ) -> Result<StmtResult, DriverError> {
        let previous = std::mem::replace(&mut self.account_storage_delegated, true);
        let result = self.run(sql);
        self.account_storage_delegated = previous;
        result
    }

    /// Go `GetTxnWriteThroughputSLI`: returns the session's transaction-wide
    /// SLI accumulator.
    pub fn txn_write_throughput_sli(&mut self) -> &mut tidb_util::sli::TxnWriteThroughputSli {
        &mut self.write_sli
    }

    /// Go server `addQueryMetrics`' SLI call at the end of one SQL command.
    pub fn finish_txn_write_throughput(&mut self, cost: Duration) {
        let cost = i64::try_from(cost.as_nanos()).unwrap_or(i64::MAX);
        let affected_rows = u64::try_from(self.prev_row_count.max(0)).unwrap_or(0);
        let in_txn = self.in_transaction();
        self.write_sli
            .finish_execute_stmt(cost, affected_rows, in_txn);
    }

    /// Like [`Session::run`], but a query result also carries its column
    /// metadata (`(name, type)` per column) for wire-protocol fronts.
    ///
    /// Captured from TiDB: a statement that fails leaves its own error in the
    /// warning buffer as an `Error`-level row, so `SHOW WARNINGS` right after
    /// a failure reports it.
    pub fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_internal(sql, false)
            .map(|(output, _)| output)
    }

    fn run_with_columns_internal(
        &mut self,
        sql: &str,
        capture_result_authority: bool,
    ) -> Result<(StmtOutput, Option<ResultMaterializationAuthority>), DriverError> {
        self.run_with_columns_using(sql, capture_result_authority, |session| {
            session.execute_statement(sql)
        })
    }

    fn run_with_columns_using(
        &mut self,
        sql: &str,
        capture_result_authority: bool,
        execute: impl FnOnce(&mut Self) -> Result<StmtOutput, DriverError>,
    ) -> Result<(StmtOutput, Option<ResultMaterializationAuthority>), DriverError> {
        self.begin_statement_execution(sql)?;
        let table_delta_savepoint = self.table_delta_savepoint();
        let result = execute(self);
        if result.is_err() {
            self.restore_table_delta_savepoint(table_delta_savepoint);
        }
        let result_authority = (capture_result_authority
            && matches!(&result, Ok(StmtOutput::Rows { .. })))
        .then(|| self.result_materialization_authority());
        self.finish_statement_execution(result)
            .map(|output| (output, result_authority))
    }

    // Go resets statement state before executor construction. Keep this
    // boundary separate from completion, which belongs to record-set close
    // for queries and to execution itself for statements without results.
    fn begin_statement_execution(&mut self, sql: &str) -> Result<(), DriverError> {
        self.mpp_attempt_completed = false;
        if !self.external_executor_breakpoint_scope {
            self.executor_first_run_breakpoint
                .store(false, std::sync::atomic::Ordering::Release);
        }
        if let Some(previous) = self.statement_result_authority.get_mut().take() {
            // Retire before installing the next root action, even when a
            // retained authority delays Drop beyond the statement boundary.
            previous.statement_memory().finish_statement();
        }
        self.stmt_hints = tidb_hint::StmtHints::default();
        *self
            .process_plan_info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            tidb_executor::ProcessPlanInfo::default();
        self.check_sandbox_mode(sql)?;
        self.begin_statement_observation(sql);
        // A statement is visible to a peer's SHOW PROCESSLIST for exactly as
        // long as it runs, which is why the process list is updated here --
        // the one door every statement of this session goes through -- rather
        // than in one front end's command loop.
        // The process registry reuses the server-held PREPARE digest, and
        // normalizes ordinary statements only when it publishes a new one.
        // Materialize normalized SQL here only when memory arbitration needs
        // the text as well; pass its digest through to avoid a second hash.
        let arbitrated = self.session_memory.arbitrator_enabled();
        let observation = self.current_statement_observation_identity(sql);
        let normalized = arbitrated.then(|| {
            observation.map_or_else(
                || normalize_statement_digest(sql),
                |(normalized, digest)| (normalized.to_owned(), digest.clone()),
            )
        });
        if let Some(guard) = &self.process {
            let registry = guard.registry();
            let digest = normalized
                .as_ref()
                .map(|(_, digest)| digest.to_string())
                .or_else(|| observation.map(|(_, digest)| digest.to_string()));
            if self.routed_statement_observation_depth == 0 {
                registry.statement_started_with_digest(
                    guard.id(),
                    sql,
                    digest.as_deref(),
                    &self.status_text(),
                );
            }
            // Go reads these off typed `SessionVars` fields; the snapshot is
            // their parsed form, refreshed only when the variable table
            // changes, so a statement start does not re-parse five values.
            let snapshot = self.statement_var_snapshot();
            registry.statement_metadata(
                guard.id(),
                u64::try_from(self.current_tso().value()).unwrap_or_default(),
                self.active_resource_group.clone(),
                snapshot.ddl_session_alias.clone(),
                snapshot.redact_sql,
                tidb_util::memoryusagealarm::OOMAlarmVariablesInfo {
                    session_analyze_version: snapshot.analyze_version,
                    session_enabled_rate_limit_action: snapshot.rate_limit_action,
                    session_mem_quota_query: snapshot.mem_quota,
                },
            );
        }
        self.statement_normalized_sql = if arbitrated {
            normalized.map(|(text, _)| text)
        } else {
            None
        };
        // Go's `ResetContextOfStmt` promotes the PRECEDING statement's
        // publication into the `Prev*` fields the next statement reads, so
        // the promotion happens at the boundary, once, for every statement.
        self.statement_kind = StatementKind::Other;
        self.statement_message.clear();
        *self
            .published_last_insert_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        // Go promotes `FoundInPlanCache` into `PrevFoundInPlanCache` in
        // `ResetContextOfStmt`, at the same boundary as the other `Prev*`
        // fields above -- which is why `select @@last_plan_from_cache`
        // reports the PRECEDING statement rather than itself.
        self.prev_found_in_plan_cache = std::mem::take(&mut self.found_in_plan_cache);
        // Go promotes `FoundInBinding` at the same boundary, which is why
        // `select @@last_plan_from_binding` reports the statement BEFORE it
        // rather than itself (that SELECT matches no binding of its own).
        self.prev_found_in_binding = std::mem::take(&mut self.found_in_binding);
        Ok(())
    }

    fn finish_statement_execution(
        &mut self,
        result: Result<StmtOutput, DriverError>,
    ) -> Result<StmtOutput, DriverError> {
        let completion = result
            .as_ref()
            .map(StatementCompletion::from)
            .map_err(Clone::clone);
        self.finish_statement_state(&completion);
        result
    }

    /// Go `session.go:1955-1968`'s parse-failure half, for front ends that
    /// parse through the `&self` [`StmtCtx::parse_statement`] door and
    /// surface the error without entering the statement lifecycle: the
    /// statement boundary opens a FRESH warning context (the previous
    /// statement's entries go — captured: after a failing `mid()` the next
    /// failed parse reports ONLY its own 1064) and the syntax error is
    /// appended into it, so `SHOW WARNINGS` after a failed parse reports the
    /// error row. Evaluation-origin errors never reach here.
    /// go `ResetContextOfStmt` opens every statement with a fresh warning
    /// context: the previous statement's entries go unless this statement
    /// reports them itself. Front-end doors that bypass the session's own
    /// statement boundary (transaction control, global-variable sets) must
    /// still open that fresh context or a parse-failure row lingers.
    pub fn drop_previous_statement_warnings(&mut self) {
        self.warnings.clear();
    }

    pub fn record_parse_failure(&mut self, error: &DriverError) {
        let reported = error.clone().to_mysql_error();
        self.record_parse_failure_coded(reported.code, reported.message);
    }

    /// [`Self::record_parse_failure`] for front ends whose parse door
    /// surfaces an already-classified `(code, message)` error.
    pub fn record_parse_failure_coded(&mut self, code: u16, message: String) {
        self.warnings.clear();
        self.append_warning(WarningLevel::Error, code, message);
    }

    /// go `driver_tidb.go:376`: every statement error lands in the statement
    /// warning buffer (DDL 1050/1007/1008/1051/1146 et al reach SHOW
    /// WARNINGS exactly as the query door's errors do).
    pub fn record_ddl_failure(&mut self, code: u16, message: String) {
        // go `driver_tidb.go:376` appends the error, but the DDL execution
        // path may have already recorded it (HandleStatusErr in the
        // executor's own error handling). Skip the duplicate so SHOW
        // WARNINGS matches go's single row.
        if self
            .warnings
            .last()
            .map_or(false, |w| w.code == code && w.message == message)
        {
            return;
        }
        self.append_warning(WarningLevel::Error, code, message);
    }

    /// Clears the statement warning buffer (go `ResetContextOfStmt`'s
    /// per-statement fresh start for write-door statements that bypass the
    /// session's own statement boundary).
    pub fn clear_statement_warnings(&mut self) {
        self.warnings.clear();
    }

    fn finish_statement_state(&mut self, result: &Result<StatementCompletion, DriverError>) {
        // Go ExecStmt.FinishExecuteStmt clears MPPQueryInfo only at completion.
        // Retained readers are detached from the next statement's counters;
        // ordinary completion reuses the allocation. Retries never reset it.
        if self.external_mpp_query_scope {
            self.mpp_attempt_completed = true;
        } else {
            self.reset_mpp_query_info();
        }
        self.publish_statement_status(result);
        if let Some(guard) = &self.process {
            let affected_rows = match result {
                Ok(StatementCompletion::Affected(count)) => *count,
                _ => 0,
            };
            guard
                .registry()
                .statement_affected_rows(guard.id(), affected_rows);
        }
        if let Some(collector) = &self.session_index_usage_collector {
            collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .report();
        }
        if self.routed_statement_observation_depth == 0 {
            if let Some(guard) = &self.process {
                guard.registry().statement_finished(
                    guard.id(),
                    &self.current_db,
                    &self.status_text(),
                );
            }
        }
        if let Err(error) = &result {
            // The statement's fold-time diagnostics precede the error row in
            // go's buffer (FORMAT('x', 'y') keeps both coercion warnings
            // beside the 1582); a plan-time failure produced no record set,
            // so this door is the drain they get.
            self.drain_fold_stash();
            let reported = error.clone().to_mysql_error();
            // Go's wire behavior is asymmetric per error class (captured on
            // the oracle with SHOW WARNINGS after each failure): 3140/3143/
            // 1411 leave the statement warning buffer EMPTY, while 3146/
            // 1305/1235 and every parse/plan/executor failure show their own
            // error row there.
            // 1148 is go's `handleLoadData` refusal: it fires BEFORE any
            // statement context reset, so the buffer keeps the PREVIOUS
            // statement's rows and never gains its own row (oracle-captured),
            // whichever classification the error carries.
            // 3057 (`Incorrect user-level lock name`) is go's
            // handleSingleGetLock refusal: it errors bare, without its own
            // warning row (oracle g-err3: get_lock('x') over a missing
            // timeout).
            // 1210 (`Incorrect arguments to <function>`) splits by origin: an
            // EVALUATION raise (PERIOD_ADD over a bad period -- g-fsp) never
            // lands a warning row, while the window executor's own build-time
            // refusal does (oracle g-window2: `NTILE(0)` errors 1210 WITH its
            // error row).
            // The completion error's own variant decides: a record-set
            // execution failure that IS the evaluation's fatal error (the
            // aggregate raised it mid-scan) takes the empty-buffer arm even
            // though `to_mysql_error` on the DRIVER error carries no
            // evaluation-origin marker.
            let eval_fatal = matches!(error, DriverError::JsonDocumentNullKey)
                || (reported.is_from_evaluation()
                    && matches!(
                        reported.code,
                        3140 | 3143 | 1411 | 1690 | 1105 | 1210 | 3158
                    ));
            if eval_fatal {
                // Go's warning buffer stays EMPTY for these: the evaluation
                // raised the error through `HandleError` at Error level, so
                // the context never filed a row. The eval-side drain here
                // did, though (the context's own sink fires before the
                // level check) — drop the row that duplicates the fatal
                // error (oracle g-group: `JSON_OBJECTAGG(NULL, 1)` answers
                // 3158 with an EMPTY SHOW WARNINGS).
                self.warnings
                    .retain(|w| !(w.code == reported.code && w.message == reported.message));
            }
            if reported.code != 1148 && reported.code != 3057 && !eval_fatal {
                // The inner execution may have already filed this exact
                // error row (the SET arm's `handleErr` append through the
                // delegated-account route); go's SHOW WARNINGS carries it
                // once.
                let duplicated = self.warnings.last().map_or(false, |w| {
                    w.code == reported.code && w.message == reported.message
                });
                if !duplicated {
                    self.append_warning(WarningLevel::Error, reported.code, reported.message);
                }
            }
        }
        self.finish_statement_observation(result);
    }
}

#[cfg(test)]
mod session_source_tests {
    use super::{
        approx_compile_plan_token_count, approx_parse_sql_token_count, DomainMap,
        NoAvailableDomain, Session,
    };

    // Go pkg/session/tidb_test.go::TestDomapHandleNil.
    #[test]
    fn test_domap_handle_nil() {
        let domains = DomainMap::default();
        assert!(matches!(domains.get(None), Err(NoAvailableDomain)));

        let opened = domains.get(Some("store-a")).unwrap();
        let available = domains.get(None).unwrap();
        assert!(std::sync::Arc::ptr_eq(&opened, &available));
    }

    // Go pkg/session/session_test.go::TestMemArbitratorSession.
    #[test]
    fn test_mem_arbitrator_session() {
        assert_eq!(
            approx_parse_sql_token_count(
                "/*select * from **/SELECT x FROM `t\\`` # abc \nwhere a = 1.23 and b = 'abc\"d\\'e' -- abc \nand c_1_2 in \"abc'd\\\"e\" # (1,2,3)\n"
            ),
            15
        );
        assert_eq!(approx_parse_sql_token_count("select @@version @a"), 0);
        assert_eq!(approx_parse_sql_token_count("set @a=1"), 0);
        assert_eq!(approx_parse_sql_token_count("desc analyze table t"), 0);
        assert_eq!(approx_parse_sql_token_count("analyze table t"), 0);
        assert_eq!(
            approx_parse_sql_token_count("/*select * from **/explain show warnings"),
            0
        );
        assert_eq!(
            approx_parse_sql_token_count("/*select * from **/desc show columns from t"),
            0
        );
        assert_eq!(approx_parse_sql_token_count("insert into t values 1"), 5);
        assert_eq!(approx_parse_sql_token_count("update t set a=1"), 5);
        assert_eq!(approx_parse_sql_token_count("delete from t where a=1"), 6);
        assert_eq!(approx_parse_sql_token_count("replace into t values 1"), 5);
        assert_eq!(
            approx_parse_sql_token_count("prepare stmt1 from 'select * from t where a=? and b=?'"),
            0
        );
        assert_eq!(
            approx_parse_sql_token_count("execute stmt1 using @a,@b,@c"),
            0
        );
        let normalized = "select * from `a_1`.`b_2` where c1 = ? and c2 = ?";
        assert_eq!(approx_parse_sql_token_count(normalized), 10);
        assert_eq!(approx_compile_plan_token_count(normalized, true), 9);
        assert_eq!(
            approx_compile_plan_token_count("select @@version @a", true),
            0
        );
        assert_eq!(
            approx_compile_plan_token_count("select @@version @a", false),
            3
        );

        struct Recorder;
        impl tidb_util::memory::RecordMemState for Recorder {
            fn load(&self) -> Result<Option<tidb_util::memory::RuntimeMemStateV1>, String> {
                Ok(None)
            }
            fn store(&self, _: &tidb_util::memory::RuntimeMemStateV1) -> Result<(), String> {
                Ok(())
            }
        }
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (id BIGINT PRIMARY KEY)")
            .unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();
        // Exercise key selection without starting a process-wide quota worker.
        session
            .run("SET tidb_mem_arbitrator_wait_averse='nolimit'")
            .unwrap();
        let arbitrator = tidb_util::memory::MemArbitrator::new(1024, 4, 3, 0, Box::new(Recorder));
        arbitrator.set_work_mode(tidb_util::memory::ArbitratorWorkMode::Standard);
        session.set_mem_arbitrator(arbitrator.clone());
        for sql in [
            "SELECT id FROM t WHERE id IN (1, 2)",
            "SELECT /* comment */ id AS n FROM t WHERE id = 1",
        ] {
            session.run(sql).unwrap();
            assert_eq!(
                session.current_sql_digest_key,
                tidb_parser::normalize_digest(sql).0
            );
        }
        let sql = "SELECT id FROM t WHERE id = ?";
        let prepared = session.prepare_ast(sql).unwrap();
        session.run("SELECT 7").unwrap();
        let execution = session
            .bind_cached_prepared_point_get(
                &prepared.point_get_plan().unwrap(),
                &[tidb_datatype::Datum::Int(1)],
            )
            .unwrap();
        let opened = session
            .open_prepared_point_get(execution, prepared.statement(), sql)
            .unwrap()
            .unwrap();
        assert_eq!(
            session.current_sql_digest_key,
            tidb_parser::normalize_digest(sql).0
        );
        drop(opened.attach(&mut session));
        arbitrator.set_work_mode(tidb_util::memory::ArbitratorWorkMode::Disable);
        session.run("SELECT 2").unwrap();
        assert!(session.current_sql_digest_key.is_empty());
    }

    #[test]
    fn statement_index_usage_collector_is_session_owned_and_flushed_on_close() {
        let global = tidb_stats_handle_usage_indexusage::Collector::new();
        global.start_worker();
        let mut session = Session::default();
        session.set_session_index_usage_collector(global.spawn_session_collector());

        let context = session.statement_context(false);
        context
            .index_usage_collector()
            .expect("positive stats updating installs a statement collector")
            .update(
                41,
                7,
                tidb_stats_handle_usage_indexusage::new_sample(0, 2, 3, 10),
            );
        drop(context);
        drop(session);

        let sample = global.get_index_usage(41, 7);
        assert_eq!(sample.query_total, 1);
        assert_eq!(sample.kv_req_total, 2);
        assert_eq!(sample.row_access_total, 3);
        global.close();
    }
}

pub mod metrics;
#[cfg(test)]
mod tests_admin_check;
#[cfg(test)]
mod tests_alter_column;
#[cfg(test)]
mod tests_analyze;
#[cfg(test)]
mod tests_auto_increment;
#[cfg(test)]
mod tests_auto_random;
#[cfg(test)]
mod tests_bad_null;
#[cfg(test)]
mod tests_binding;
#[cfg(test)]
mod tests_cast_int_truncation;
#[cfg(test)]
mod tests_cast_vector;
#[cfg(test)]
mod tests_charset;
#[cfg(test)]
mod tests_charset_introducer;
#[cfg(test)]
mod tests_check_constraints;
#[cfg(test)]
mod tests_coalesced_joins;
#[cfg(test)]
mod tests_collation;
#[cfg(test)]
mod tests_column_defaults;
#[cfg(test)]
mod tests_column_prune;
#[cfg(test)]
mod tests_compare_refinement;
#[cfg(test)]
mod tests_core;
#[cfg(test)]
mod tests_datetime_year_compare;
#[cfg(test)]
mod tests_deadlock_history;
#[cfg(test)]
mod tests_decorrelate_count_having;
#[cfg(test)]
mod tests_derived_agg_pruning;
#[cfg(test)]
mod tests_dml_lock_keys;
#[cfg(test)]
mod tests_domain_domain_utils_source;
#[cfg(test)]
mod tests_domain_plan_replayer_handle_source;
#[cfg(test)]
mod tests_domain_plan_replayer_source;
#[cfg(test)]
mod tests_domain_ru_stats_source;
#[cfg(test)]
mod tests_domain_schema_checker_source;
#[cfg(test)]
mod tests_domain_serverinfo_info_source;
#[cfg(test)]
mod tests_domain_serverinfo_syncer_source;
#[cfg(test)]
mod tests_domain_topn_slow_query_source;
#[cfg(test)]
mod tests_enum_index_range;
#[cfg(test)]
mod tests_eval_bool;
#[cfg(test)]
mod tests_explain;
#[cfg(test)]
mod tests_explain_derived;
#[cfg(test)]
mod tests_explain_merge_join;
#[cfg(test)]
mod tests_expression_indexes;
#[cfg(test)]
mod tests_extra_handle;
#[cfg(test)]
mod tests_extra_handle_access;
#[cfg(test)]
mod tests_fix_control;
#[cfg(test)]
mod tests_foreign_key;
#[cfg(test)]
mod tests_generated_columns;
#[cfg(test)]
mod tests_global_vars;
#[cfg(test)]
mod tests_grants;
#[cfg(test)]
mod tests_harvested_relation_engine;
#[cfg(test)]
mod tests_hash_join_fetcher_eof;
#[cfg(test)]
mod tests_in_list_full_evaluation;
#[cfg(test)]
mod tests_index_hints;
#[cfg(test)]
mod tests_index_join_inner_pattern;
#[cfg(test)]
mod tests_index_key_length;
#[cfg(test)]
mod tests_join_key_cast;
#[cfg(test)]
mod tests_join_predicate_placement;
#[cfg(test)]
mod tests_join_reorder_cost;
#[cfg(test)]
mod tests_json;
#[cfg(test)]
mod tests_mem_quota;
#[cfg(test)]
mod tests_merge_join_mixed_key_types;
#[cfg(test)]
mod tests_mixed_sign_index_join;
#[cfg(test)]
mod tests_modify_column_null;
#[cfg(test)]
mod tests_multi_table_dml;
#[cfg(test)]
mod tests_mview_session_vars;
#[cfg(test)]
mod tests_non_prepared_plan_cache;
#[cfg(test)]
mod tests_observation_batch;
#[cfg(test)]
mod tests_outer_join_elimination;
#[cfg(test)]
mod tests_partition;
#[cfg(test)]
mod tests_partition_processor;
#[cfg(test)]
mod tests_partition_projection;
#[cfg(test)]
mod tests_partition_prune_collation;
#[cfg(test)]
mod tests_planner_core_rewriter;
#[cfg(test)]
mod tests_positional_orderby;
#[cfg(test)]
mod tests_prepared_plan_cache;
#[cfg(test)]
mod tests_prepared_statements;
#[cfg(test)]
mod tests_pushdown_blacklist;
#[cfg(test)]
mod tests_read_cast;
#[cfg(test)]
mod tests_recursive_cte;
#[cfg(test)]
mod tests_savepoint;
#[cfg(test)]
mod tests_sem_v2;
#[cfg(test)]
mod tests_sequence;
#[cfg(test)]
mod tests_session_embedding_source;
#[cfg(test)]
mod tests_session_part1_source;
#[cfg(test)]
mod tests_session_part2_source;
#[cfg(test)]
mod tests_session_var_hooks;
#[cfg(test)]
mod tests_set_opr_precedence;
#[cfg(test)]
mod tests_show;
#[cfg(test)]
mod tests_show_admin;
#[cfg(test)]
mod tests_skew_distinct_agg;
#[cfg(test)]
mod tests_sql_mode_scanner;
#[cfg(test)]
mod tests_statement_rollback;
#[cfg(test)]
mod tests_subquery;
#[cfg(test)]
mod tests_support;
#[cfg(test)]
mod tests_sysbench_access;
#[cfg(test)]
mod tests_system_schemas;
#[cfg(test)]
mod tests_temporary_tables;
#[cfg(test)]
mod tests_tidb_decode_key;
#[cfg(test)]
mod tests_timestamp_range;
#[cfg(test)]
mod tests_timezone_storage;
#[cfg(test)]
mod tests_topn;
#[cfg(test)]
mod tests_union_all_predicate_push_down;
#[cfg(test)]
mod tests_union_scan;
#[cfg(test)]
mod tests_user_vars;
#[cfg(test)]
mod tests_views;
#[cfg(test)]
mod tests_window;
#[cfg(test)]
mod tests_write_conversion;
#[cfg(test)]
mod tests_zero_date;

/// Go `time.Duration.String()` for a whole number of seconds, which is what
/// `UPTIME` carries: `time.Since(startTime).String()` with the sub-second
/// part always zero here because the start timestamp has second granularity.
fn go_uptime_string(seconds: i64) -> String {
    let seconds = seconds.max(0);
    let (hours, minutes, seconds) = (seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}s")
    } else {
        format!("{seconds}s")
    }
}

/// A `DATETIME` cell for a unix timestamp, in the node's own clock -- Go's
/// `types.NewTime(types.FromGoTime(time.Unix(ts, 0)), mysql.TypeDatetime, 0)`.
pub(crate) fn datetime_datum(unix_seconds: i64) -> tidb_datatype::Datum {
    use chrono::{Datelike, TimeZone, Timelike};
    // go renders these datetimes through the node's own clock (`FromGoTime`),
    // i.e. the system's local zone -- the UTC read answered 13:07 where the
    // oracle shows 21:07 for the same boot instant.
    let Some(local) = chrono::Local
        .timestamp_opt(unix_seconds, 0)
        .single()
        .map(|moment| moment.naive_local())
    else {
        return tidb_datatype::Datum::Null;
    };
    tidb_datatype::Time::from_date_checked(
        local.year(),
        local.month() as i32,
        local.day() as i32,
        local.hour() as i32,
        local.minute() as i32,
        local.second() as i32,
        0,
        tidb_datatype::TimeType::DateTime,
        0,
    )
    .map_or(tidb_datatype::Datum::Null, tidb_datatype::Datum::Time)
}
