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

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::compiler::PreparedAstCompile;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_ast::Stmt;
use tidb_datatype::{Datum, FieldType};
use tidb_infoschema::Catalog;
use tidb_driver_error::DriverError;
use tidb_mathutil::MysqlRng;
pub use load_stats_arm::ClientLocalFileReader;
use txn_mode::txn_mode_for_begin;
use tidb_vardef::txn_mode::{SessionTxnMode, PESSIMISTIC_TXN_MODE};

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

use tidb_executor::StmtOutput;
use tidb_ddl::ddl_history::DdlJobHistoryStore;
use tidb_kv::lock_waits::DataLockWaitsProvider;



use tidb_stats_handle_usage_predicatecolumn::cluster_predicate_column::ColumnStatsUsageProvider;

use tidb_stats_handle_autoanalyze::analyze_jobs::AnalyzeStatusProvider;


/// The cluster's BDR role, Go's meta key `BDRRole`, where a front end keeps
/// it in a shared store rather than in its own catalog.
use tidb_domain_core::domain::GlobalIdGenerator;

use tidb_meta::BdrRoleStore;

use tidb_stats_handle_cache::table_size_stats::TableStorageStatsProvider;
use tidb_executor::stmt_ctx_build::ResultMaterializationAuthority;



/// A process-wide catalog shared by every session, as Go's domain-owned
/// `infoschema` is shared by every session of a TiDB instance.
#[derive(Clone)]
pub struct SharedCatalog {
    catalog: Arc<Mutex<Catalog>>,
    /// Go `domain.Domain`'s instance plan cache owner, shared by every
    /// session opened over this catalog; cluster servers install their own
    /// on each session instead.
    plan_cache_invalidation: Arc<tidb_executor::PlanCacheInvalidation>,
    /// Go `domain.Domain`'s statistics handle, shared by every session over
    /// this catalog.
    stats_handle: Arc<Mutex<Arc<tidb_stats_handle::StatsHandle>>>,
}

impl SharedCatalog {
    /// A domain over `catalog` with a fresh plan-cache owner.
    #[must_use]
    pub fn new(catalog: Catalog) -> Self {
        Self {
            catalog: Arc::new(Mutex::new(catalog)),
            plan_cache_invalidation: Arc::default(),
            stats_handle: Arc::default(),
        }
    }

    /// The domain's statistics handle.
    #[must_use]
    pub fn stats_handle(&self) -> Arc<tidb_stats_handle::StatsHandle> {
        Arc::clone(
            &self
                .stats_handle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Installs the hosting domain's statistics handle for every session
    /// over this catalog.
    pub fn set_stats_handle(&self, handle: Arc<tidb_stats_handle::StatsHandle>) {
        *self
            .stats_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = handle;
    }

    /// Locks the catalog image.
    pub fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, Catalog>> {
        self.catalog.lock()
    }

    /// The plan-cache owner every session over this catalog shares.
    #[must_use]
    pub fn plan_cache_invalidation(&self) -> Arc<tidb_executor::PlanCacheInvalidation> {
        Arc::clone(&self.plan_cache_invalidation)
    }

    /// The catalog image's own handle, for registries keyed by identity.
    pub(crate) fn inner(&self) -> &Arc<Mutex<Catalog>> {
        &self.catalog
    }

    /// Whether both handles are the same domain.
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.catalog, &other.catalog)
    }
}

/// Go `domainMap`: one domain-level catalog authority per storage UUID.
///
/// A server can use this registry when opening sessions for more than one
/// keyspace. Looking up `None` preserves the plugin-facing Go contract: reuse
/// any available domain, or return [`NoAvailableDomain`] when the process has
/// not opened one yet.
#[derive(Default)]
pub struct DomainMap {
    domains: Mutex<HashMap<String, Arc<tidb_domain_core::domain::Domain>>>,
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
    /// Go `domainMap.Get`: the domain of the store `store_uuid` names, booted
    /// by `open` the first time the store is asked for.
    ///
    /// `None` never creates an entry. It returns an existing one when
    /// available, matching the enterprise-plugin compatibility path in Go.
    pub fn get(
        &self,
        store_uuid: Option<&str>,
        open: impl FnOnce() -> Arc<tidb_domain_core::domain::Domain>,
    ) -> Result<Arc<tidb_domain_core::domain::Domain>, NoAvailableDomain> {
        let mut domains = self
            .domains
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(store_uuid) = store_uuid else {
            return domains.values().next().cloned().ok_or(NoAvailableDomain);
        };
        Ok(Arc::clone(
            domains.entry(store_uuid.to_owned()).or_insert_with(open),
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
use tidb_domain_core::schema_sync::MdlRelatedTableSink;

pub struct Session {
    global_config_syncer: Option<Arc<tidb_domain_globalconfigsync::GlobalConfigSyncer>>,
    catalog: SharedCatalog,
    /// The system session an account statement's executor runs its
    /// `mysql.*` SQL on while the statement runs (Go `GetSysSession`).
    account_sql: Option<Box<dyn crate::user_table::AccountSqlSession>>,
    /// The live privilege cache while an account statement runs against a
    /// scratch registry read from `mysql.*`: Go's privilege manager, which
    /// `FindEdge` consults, is the cache, not the tables.
    account_statement_privilege_cache: Option<tidb_privilege::PrivilegeRegistry>,
    /// The statement its node compiled ahead of the executor that runs it
    /// ([`Session::compile_statement`]), which that executor's own compile
    /// takes instead of building and checking the plan again.
    precompiled: Option<crate::identity::PrecompiledStatement>,
    /// What the session's executors reach through Go's `Domain`; `None` for
    /// an in-process session.
    domain_services: Option<crate::domain_services::DomainServices>,
    /// Go `Domain.NotifyUpdateAllUsersPrivilege`'s announcement half: tells
    /// every node of the cluster to reload privileges after FLUSH
    /// PRIVILEGES. `None` for a session with no peers to tell.
    privilege_change_notifier: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Go `session.values`: heterogeneous values addressed by session-context
    /// stringer keys. Values must be thread-safe because the native session
    /// moves between connection workers.
    context_values: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    /// Shared with every executor context built for the current attempt.
    executor_first_run_breakpoint: Arc<std::sync::atomic::AtomicBool>,
    /// The cluster transaction layer owns the attempt boundary while this is
    /// true, so inner pessimistic retries do not re-arm the first-run hook.
    external_executor_breakpoint_scope: bool,
    /// Metadata snapshot cache keyed by the catalog mutation version.
    tidb_decode_key_cache:
        std::sync::Mutex<Option<(u64, Arc<tidb_infoschema::tidb_decode_key::TidbDecodeKeySnapshot>)>>,
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
    statement_observation: Option<tidb_executor::exec_stmt_observation::StatementObservation>,
    pending_observation_parse: Option<(String, Duration)>,
    pending_statement_exec_details: Option<observation::PendingStatementExecDetails>,
    routed_statement_observation_depth: u32,
    /// The outer routed statement's kind, for Go `ResetContextOfStmt`'s
    /// `ROW_COUNT()` promotion that a routed statement does not reach
    /// through the executor's own boundary.
    routed_statement_kind: Option<crate::classify::StatementKind>,
    previous_summary_statement: Option<(String, String)>,
    statement_stats: Arc<tidb_topsql_stmtstats::StatementStats>,
    /// Go's statement ctx values (`RUDetails`, resource-group taggers) of the
    /// statement that runs, shared with its transactions and requests.
    statement_kv_context: tidb_txnkv::StatementKvContextCell,
    /// Go `SessionVars.TxnCtx` of the open transaction; `Some` is `InTxn`.
    txn_ctx: Option<txn::TxnCtx>,
    /// Go `LazyTxn.writeSLI`: transaction write-throughput state shared by
    /// every statement until the transaction ends.
    write_sli: tidb_sli::TxnWriteThroughputSli,
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
    /// Go DETACHES the overlay again in three narrower places: `SHOW CREATE
    /// VIEW` / `SHOW CREATE SEQUENCE` (`preprocess.go:551`), the by-name
    /// `SHOW TABLES` (which `Catalog::table_names` excludes), and while
    /// expanding a view's body (`logical_plan_builder.go:4941`), which the
    /// planner reads through the catalog's detached view.
    local_temporary_tables: tidb_infoschema::session_tables::SessionTables,
    /// Go `sctx.GetInfoSchema()`: the node's schema this session's catalog
    /// was built from, which `DDLExec` builds a LOCAL temporary table's
    /// `TableInfo` against. `None` for a session no node opened.
    info_schema: Option<Arc<tidb_infoschema::cluster_catalog::ClusterCatalog>>,
    /// Go `SessionVars.HypoIndexes`: session state, never schema.
    hypo_indexes: Arc<tidb_variable::hypo_index::HypoIndexes>,
    /// Go `SessionVars.HypoTiFlashReplicas`: session state, never schema.
    hypo_tiflash_replicas: Arc<tidb_variable::hypo_index::HypoTiFlashReplicas>,
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
    global_temporary_data: std::collections::HashMap<i64, tidb_table::kv_table::TemporaryTableTxnData>,
    /// The session's system and user variables.
    vars: SessionVars,
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
    warnings: Vec<SqlWarn>,
    /// Go `handleQuery`'s `parserWarns`: the multi-statement 8130 warning is
    /// carried OUTSIDE the per-statement warning buffer and appended to the
    /// LAST statement's context after it runs (`conn.go:2262`), so the reset
    /// each statement performs cannot clear it.
    deferred_multi_statement_warning: bool,
    /// Why Go's prepared plan cache refused the running EXECUTE (`hint_only`
    /// without `use_plan_cache()`, or an `ignore_plan_cache()` hint),
    /// reported after the uncached statement's boundary.
    pending_plan_cache_refusal: Option<&'static str>,
    /// Go `StatementContext.InShowWarning`: set for exactly the statements
    /// that inherit the buffer, and the reason `WarningCount()` reports 0 for
    /// them. See [`Session::wire_warning_count`].
    in_show_warning: bool,
    /// Go `SessionVars.ActiveRoles`: the roles this session has activated,
    /// which every privilege check widens through and which `CURRENT_ROLE()`
    /// reports. A fresh session starts with its account's DEFAULT roles
    /// (Go activates them in `Auth`); `SET ROLE` replaces the set wholesale.
    /// This connection's lock references over the domain/server lock service.
    advisory_locks: tidb_sessionctx::advisory_lock_state::AdvisoryLockSession,
    /// Go `session.lockedTables`: the tables this session holds a
    /// `LOCK TABLES` lock on, by table ID ([`crate::table_lock`]).
    locked_tables: std::collections::BTreeMap<i64, tidb_model::TableLockTpInfo>,
    selected_lock_keys: Option<tidb_sessionctx::select_lock::SelectedLockKeys>,
    /// Go `TxnCtx.IsPessimistic` for an autocommit statement whose implicit
    /// transaction `decideTxnMode` made pessimistic, set by the storage owner
    /// for the statement attempt it runs.
    autocommit_pessimistic_statement: bool,
    /// Go's `StatementContext.LastMessage`, retained until the next statement
    /// so the MySQL OK/EOF writer can publish UPDATE's summary text.
    statement_message: String,
    /// Go `StmtCtx.AffectedRows()` for a statement that answers rows -- the
    /// write under `EXPLAIN ANALYZE` -- which the result set's terminal OK
    /// packet carries. A write's own OK packet takes its count directly.
    statement_affected_rows: u64,
    /// The client file a `LOAD DATA LOCAL` statement was answered with (Go's
    /// `LoadDataReaderBuilder`), taken by the statement that runs next.
    local_infile_data: Option<Vec<u8>>,
    /// Whether the connection accepted this statement's client-file request:
    /// Go refuses a LOCAL load without the capability BEFORE the statement
    /// context resets.
    local_infile_accepted: bool,
    /// Go `StmtCtx.AddSetVarHintRestore`: the session overrides a `SET_VAR`
    /// hint overwrote for the duration of ONE statement, put back when that
    /// statement finishes whether it succeeded or failed.
    set_var_hint_restore: Vec<(String, Option<String>)>,
    /// The class of the statement currently running, which decides what
    /// `ROW_COUNT()` reports next (Go's `StmtCtx.InSelectStmt` /
    /// `InInsertStmt` / `InUpdateStmt` / `InDeleteStmt` bits, read by
    /// `ResetContextOfStmt`). It is recorded even for a statement that ends
    /// in an error, because Go's bits survive the failure too.
    statement_kind: StatementKind,
    /// Go `StmtCtx.InSelectStmt` for the running statement, which
    /// `GetMaxKeysRead` reads: a SELECT or set operation, EXPLAIN unwrapped.
    statement_in_select: bool,
    /// The running statement's processed keys, shared by every context it
    /// builds (Go `DistSQLContext.MaxKeysReadCounter`).
    statement_keys_read: Arc<std::sync::atomic::AtomicU64>,
    /// Go `SessionVars.TxnCtx.StartTS`, shared with statement contexts so a
    /// lazily opened cluster snapshot becomes visible inside the statement
    /// that opened it.
    current_tso: tidb_exprctx::CurrentTso,
    /// Go `SessionVars.TxnCtx.forUpdateTS` as the cluster tier last published
    /// it for a pessimistic statement. A value older than the open
    /// transaction's start is stale and `GetForUpdateTS` answers the start.
    current_for_update_ts: tidb_exprctx::CurrentTso,
    /// The running statement's own `TxnCtx.StartTS` when its tier tore the
    /// autocommit transaction down (clearing [`Self::current_tso`]) before
    /// the statement finished; zero otherwise.
    statement_txn_start_ts: u64,
    /// The start timestamp of the transaction the running statement began
    /// inside and itself ended (`COMMIT`, `ROLLBACK`, an implicit commit):
    /// Go's `origTxnCtx`, which outlives the transaction.
    statement_ended_txn_start_ts: Option<u64>,
    /// Whether the running statement is a `BEGIN`, whose transaction gets a
    /// `TxnCtx` of its own rather than the statement's (`origTxnCtx`).
    statement_began_txn: bool,
    /// Whether the running statement already ran Go's `CommitTxn`
    /// ([`Self::set_txn_isolation_level_one_shot_state_for_next_txn`]).
    statement_committed_txn: bool,
    /// Go `ConnectionInfo.Attributes`: the handshake's connection attributes.
    connection_attributes: std::collections::BTreeMap<String, String>,
    /// Go `txn.GetMemBuffer()`: the current transaction's (or, in autocommit,
    /// the current statement's) staged-write tracker. See
    /// [`tidb_executor::kv_table::StagedWrites`].
    ///
    /// Reset by replacing the whole handle with a fresh, empty one at every
    /// transaction/statement boundary that starts clean (`open_txn_ctx`
    /// for `BEGIN`/lazy activation; the `!in_transaction()` branch in
    /// dispatch for a plain autocommit statement) -- never by walking and
    /// clearing table-by-table, since nothing else ever holds a reference to
    /// the handle being replaced. A statement that CONTINUES an open
    /// transaction is the one case that must NOT reset it: read-your-own-
    /// writes has to see every earlier statement's staged rows until COMMIT
    /// or ROLLBACK ends the transaction.
    staged_writes: std::sync::Arc<tidb_kv::staged_writes::StagedWrites>,
    /// The node's server-info syncer, when the deployment has one.
    ///
    /// Go reads `information_schema.TIDB_SERVERS_INFO` through
    /// `infosync.GetAllServerInfo`, which answers the whole cluster when an
    /// etcd client is present and THIS node alone when it is not. `None`
    /// here is the tier that has no node identity at all -- an embedded
    /// session -- and reads back as an empty table.
    server_info_syncer: Option<std::sync::Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>>,
    cluster_topology: Option<Arc<tidb_domain::cluster_topology::ClusterTopology>>,
    cluster_config: Option<Arc<tidb_executor::cluster_config::ClusterConfigClient>>,
    cluster_diagnostics: Option<Arc<tidb_executor::cluster_diagnostics::ClusterDiagnosticsClient>>,
    cluster_peer: Option<Arc<tidb_executor::cluster_peer::ClusterPeerClient>>,
    /// The domain identity getter shared with server-info publication.
    server_id_getter: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// The cluster schema version this node follows, which `ADMIN SHOW DDL`
    /// reports. Absent on the in-process tier, whose catalog is not a
    /// cluster's.
    cluster_schema_version: Option<std::sync::Arc<dyn Fn() -> i64 + Send + Sync>>,
    /// Go `executor.TakeSnapshot`, which the workload repository installs.
    take_snapshot: Option<tidb_executor::admin::TakeSnapshot>,
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
    /// The server's activation of the statement's transaction; see
    /// [`Self::set_statement_txn_activator`].
    statement_txn_activator: Option<Arc<txn::StatementTxnActivator>>,
    snapshot_schema_provider: Option<Arc<txn::SnapshotSchemaProvider>>,
    snapshot_schema: Option<(u64, Catalog)>,
    /// Statement-only SnapshotTS override; the ordinary transaction stays intact.
    statement_snapshot: Option<(u64, txn::HistoricalRead)>,
    /// Go `GetRestrictedSQLExecutor`: internal sessions of the node, absent
    /// on the in-process tier; see [`restricted_sql`].
    restricted_sql_executor: tidb_executor::memtable_reader::RestrictedSql,
    /// The statistics handle's persisted predicate-column usage reader.
    column_stats_usage: Option<std::sync::Arc<dyn ColumnStatsUsageProvider>>,
    /// The persisted analyze-job reader shared by SHOW and ANALYZE_STATUS.
    analyze_status: Option<std::sync::Arc<dyn AnalyzeStatusProvider>>,
    /// Go `pdhelper.GlobalPDHelper`: the node's, installed when the session
    /// opens; a session with no node behind it has a store without PD.
    approximate_table_counts:
        std::sync::Arc<dyn tidb_pdhelper::ApproximateTableCountProvider>,
    /// Pinned Go's fresh information-schema table-size reader.
    table_storage_stats: Option<std::sync::Arc<dyn TableStorageStatsProvider>>,
    /// Where the BDR role lives when it is not this catalog's (a cluster
    /// node's TiKV meta).
    bdr_role_store: Option<std::sync::Arc<dyn BdrRoleStore>>,
    /// The session's store's meta global-id allocator, installed by the node
    /// that opened the session (Go's `sctx.GetStore()`).
    global_ids: Option<std::sync::Arc<dyn GlobalIdGenerator>>,
    /// The session's store's regions (Go `sctx.GetStore()` as
    /// `kv.SplittableStore` and the region cache), installed by the node; a
    /// session with no store behind it keeps one in-process region map.
    regions: std::sync::Arc<dyn tidb_kv::region_map::RegionStore>,
    /// Where `ADMIN SHOW DDL JOBS` and `information_schema.DDL_JOBS` read
    /// the cluster's DDL jobs; the catalog's own history without one.
    ddl_job_history_store: Option<std::sync::Arc<dyn DdlJobHistoryStore>>,
    /// Go's session-local `SessionStatsItem`, swept into the statistics
    /// handle independently of statement execution.
    stats_collector: Option<std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>>,
    /// Go `SessionVars.TxnCtx.TableDeltaMap`, published only after commit.
    transaction_table_delta: std::sync::Arc<tidb_variable::table_delta::TableDeltaMap>,
    /// Parsed-products cache over the raw system-variable text, keyed by
    /// [`vars::SessionVars::generation`]. Go holds these as typed fields on
    /// `SessionVars`, updated by each variable's `SetSession` hook, so a
    /// statement never re-parses `sql_mode` or re-reads thirty cost knobs
    /// through string lookups; the generation stamp buys the same read cost
    /// without a hook per variable. Measured before the cache: the two were
    /// the hottest user-code frames under sysbench, ahead of the parser.
    statement_var_cache:
        std::cell::RefCell<Option<std::sync::Arc<tidb_executor::stmt_ctx_build::StatementVarSnapshot>>>,
    cost_env_cache:
        std::cell::RefCell<Option<(u64, Arc<tidb_planner::find_best_task::coster::CostEnv>)>>,
    prepared_plan_cache_environment_cache:
        std::cell::RefCell<Option<tidb_executor::compiler::prepared_ast::PreparedPlanCacheEnvironmentCache>>,
    /// Where this session reports the tables its statements bind, for the
    /// node's metadata-lock gate; see [`MdlRelatedTableSink`].
    mdl_related_tables: Option<std::sync::Arc<dyn MdlRelatedTableSink>>,
    /// Whether the running statement locks the tables it binds (Go's
    /// `IsAutoCommitTxn && StmtCtx.IsReadOnly` exemption does not apply), so
    /// the planner records them on [`Self::mdl_related_tables`].
    mdl_statement_locks: bool,
    /// The tables the running statement has already locked: Go's
    /// `GetRelatedTableForMDL().Load(id)` check, so each is recorded once.
    mdl_statement_tables: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<i64>>>,
    /// Go `StmtCtx.LastInsertID`/`LastInsertIDSet`: the id the RUNNING
    /// statement publishes. The session owns the cell and lends it to every
    /// [`tidb_sessionctx::StmtContext`] the statement builds, so an allocating
    /// INSERT and `LAST_INSERT_ID(expr)` write one place, not two.
    published_last_insert_id: Arc<std::sync::Mutex<Option<u64>>>,
    /// Go `SessionVars.RetryInfo`'s auto-increment half: the ids the statement
    /// running now has assigned, kept across a write-conflict replay so the
    /// replay writes the ids the losing attempt picked. It lives on the
    /// session because the retry loop is above the statement -- each attempt
    /// builds its own `StmtContext`, and this is the one thing that has to
    /// cross between them. See `tidb_sessionctx::RetryAutoIds`.
    retry_auto_ids: Arc<std::sync::Mutex<tidb_sessionctx::RetryAutoIds>>,
    /// Go `SessionVars.RowIDShardGenerator`: retains one random shard for
    /// `@@tidb_shard_allocate_step` generated IDs across statement contexts.
    row_id_shards: Arc<std::sync::Mutex<tidb_sessionctx::RowIdShardGenerator>>,
    /// Go's separate non-prepared statement metadata LRU.
    non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache,
    physical_plan_cache: tidb_executor::SessionPlanCache,
    plan_cache_invalidation: Arc<tidb_executor::PlanCacheInvalidation>,
    /// Go `sessionctx.Context.GetStatsHandle()`.
    stats_handle: Arc<tidb_stats_handle::StatsHandle>,
    /// The parser warnings of the statement now running, with where they
    /// start in [`Self::warnings`]; see
    /// [`Self::move_parse_warnings_behind_execution`].
    pub(crate) parse_warnings: Option<(usize, Vec<SqlWarn>)>,
    /// Go `ExecStmt.OutputNames` of the statement's materialized query
    /// result, one per column; empty for any other result. With
    /// `last_result_current_db` they give Go `recordSet.Fields()`, which the
    /// wire's column definitions and text rows read.
    pub(crate) last_result_output_names: Vec<tidb_datatype::FieldNameMetadata>,
    /// Go `SessionVars.CurrentDB` when that result's statement opened.
    pub(crate) last_result_current_db: String,
    /// Go `SessionVars.userVars`: this session's user variables, keyed
    /// lowercased, with independent runtime values and declared SQL types.
    /// Planning may publish a type before any value exists.
    ///
    /// The session owns the store and lends the handle to every statement
    /// context, because `@x := expr` writes it from INSIDE expression
    /// evaluation -- once per row, visible to the next select-list item.
    user_vars: tidb_variable::user_vars::UserVars,
    /// Go `SessionVars.SequenceState`: the last value THIS SESSION took from
    /// each sequence, keyed by lowercase `db.name`, which is what `LASTVAL`
    /// reports. It is SESSION state, not the sequence's stored counter -- a
    /// fresh session reads `NULL` from a sequence other sessions have advanced
    /// (captured: `lastval` before any `nextval` is `<nil>`).
    sequence_last_values: Arc<std::sync::Mutex<HashMap<i64, i64>>>,
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
    /// reads the shared [`tidb_privilege::PrivilegeRegistry`] below, whose
    /// `GRANT PROCESS ON *.*` path is shared by every session.
    has_process_priv: bool,
    /// The server's account/global-privilege registry, shared by every
    /// session a front end opens (see [`tidb_privilege::PrivilegeRegistry`]).
    /// `None` for a session with no front end (unit tests, internal use),
    /// which is why every check through it falls back to the pre-existing
    /// bit above rather than treating an absent registry as "no privilege".
    privileges: Option<tidb_privilege::PrivilegeRegistry>,
    /// Go's process-wide `privileges.SkipWithGrant` admission copied onto
    /// this connection by the front end. The registry remains attached for
    /// account/role storage, while authorization readers treat the session
    /// as unrestricted.
    privilege_bypassed: bool,
    /// Go `SessionVars.Rng`: the generator unseeded `RAND()` advances, shared
    /// across every statement of this session (unlike constant `RAND(N)`,
    /// which owns a fresh per-statement generator -- see `StmtContext`).
    rand: Arc<MysqlRng>,
    /// Go `SessionVars.PreparedStmtNameToID` / `PreparedStmts`: the SQL-level
    /// prepared statements this session holds. Per-session and not shared: a
    /// peer over the same catalog holds its own.
    prepared_statements: prepared_statements::PreparedStore,
    protocol_session_states: session_states::ProtocolSessionStates,
    /// Go PlanCacheParams, shared by contexts belonging to the current execution.
    prepared_params: Option<Arc<[Datum]>>,
    /// Go's `sessionBindingHandle` (`pkg/bindinfo/session_handle.go`): the
    /// SQL bindings created with `CREATE [SESSION] BINDING`. Session-scoped
    /// and unshared. Cluster GLOBAL bindings use the node cache below.
    session_bindings: binding::SessionBindings,
    /// Node-owned committed bindings, independent of this session's transaction.
    global_binding_cache: Option<binding_cache::SharedBindingCache>,
    /// Independent transaction owner for cluster global-binding commands.
    global_binding_writer: Option<Arc<dyn crate::binding_arm::GlobalBindingWriter>>,
    /// Go's `DefaultExprPushDownBlacklist` and
    /// `DefaultDisabledLogicalRulesList`, published by `ADMIN RELOAD` and
    /// empty until then -- so an `INSERT` into `mysql.expr_pushdown_blacklist`
    /// alone changes no plan. SHARED across the sessions a front end opens,
    /// because Go's scope for them is one server. See [`blacklist`].
    pushdown_blacklists: blacklist::PushdownBlacklists,
    /// The client half of the protocol's local-file transfer, which
    /// `LOAD STATS` reads its dump through (see [`load_stats_arm`]).
    client_local_file_reader: Option<load_stats_arm::ClientLocalFileReader>,
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
    process_plan_info: Arc<std::sync::Mutex<tidb_stmtctx::ProcessPlanInfo>>,
    /// Go `SessionVars.RewritePhaseInfo`, `DurationOptimizer` and
    /// `DurationWaitTS`, set by the planner and the transaction tier.
    phase_durations: tidb_variable::phase_durations::SessionPhaseDurations,
    /// Go `SessionVars.SQLCPUUsages`: the running statement's TiDB and TiKV
    /// CPU time.
    sql_cpu_usages: Arc<tidb_ppcpuusage::SqlCpuUsages>,
    /// Go `SessionVars.RUV2PendingSessionParserTotal`.
    ruv2_pending_session_parser_total: i64,
    /// Set while a binary-protocol EXECUTE runs. Go keeps the plan detail out
    /// of the process list for that path (`executeStmtImpl`,
    /// `pkg/session/session.go`: `execStmt.Name == ""` clears `currentPlan`),
    /// so its statement contexts skip rendering `BriefBinaryPlan`.
    binary_prepared_execution: bool,
    /// The node connection a session built over a booted domain runs its
    /// statements through (Go testkit's session on `CreateMockStore`'s
    /// domain); `None` for a session that IS the node connection's own.
    pub(crate) node: Option<Box<crate::domain::cluster_session_node::NodeConn>>,
}

use tidb_sessmgr::normalize_statement_digest;
use tidb_sessmgr as process;

impl Session {
    /// Builds the Go `createSessionWithOpt` state over an already selected
    /// infoschema. Cluster bootstrap is owned by the store/domain, not by each
    /// session opened on it.
    fn unbootstrapped(
        catalog: SharedCatalog,
        node: Box<crate::domain::cluster_session_node::NodeConn>,
    ) -> Self {
        let plan_cache_invalidation = catalog.plan_cache_invalidation();
        let stats_handle = catalog.stats_handle();
        Session {
            catalog,
            account_sql: None,
            account_statement_privilege_cache: None,
            precompiled: None,
            domain_services: None,
            privilege_change_notifier: None,
            context_values: HashMap::new(),
            executor_first_run_breakpoint: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            external_executor_breakpoint_scope: false,
            tidb_decode_key_cache: std::sync::Mutex::new(None),
            session_memory: tidb_executor::SessionMemory::new(
                tidb_memory::DEF_MEM_QUOTA_QUERY,
                tidb_vardef::OomAction::Cancel,
                0,
            ),
            statement_result_authority: std::cell::RefCell::new(None),
            current_sql_digest_key: String::new(),
            statement_normalized_sql: None,
            statement_observation: None,
            pending_observation_parse: None,
            pending_statement_exec_details: None,
            routed_statement_observation_depth: 0,
            routed_statement_kind: None,
            previous_summary_statement: None,
            statement_stats: tidb_topsql_stmtstats::create_statement_stats(),
            statement_kv_context: tidb_txnkv::StatementKvContextCell::default(),
            txn_ctx: None,
            write_sli: tidb_sli::TxnWriteThroughputSli::default(),
            local_temporary_tables: Vec::new(),
            info_schema: None,
            hypo_indexes: Arc::default(),
            hypo_tiflash_replicas: Arc::default(),
            global_temporary_data: std::collections::HashMap::new(),
            // Go `createSession`: `s.sessionVars.GlobalVarsAccessor = s`,
            // the node's accessor over its domain; a factory attaches its
            // shared one (`attach_globals`).
            vars: {
                let mut vars = SessionVars::new();
                vars.swap_globals(Some(std::sync::Arc::new(GlobalSysvars::new())));
                vars
            },
            global_config_syncer: None,
            stmt_hints: tidb_hint::StmtHints::default(),
            active_resource_group: "default".to_owned(),
            warnings: Vec::new(),
            deferred_multi_statement_warning: false,
            pending_plan_cache_refusal: None,
            in_show_warning: false,
            advisory_locks: tidb_sessionctx::advisory_lock_state::AdvisoryLockSession::default(),
            locked_tables: std::collections::BTreeMap::new(),
            selected_lock_keys: None,
            autocommit_pessimistic_statement: false,
            statement_message: String::new(),
            statement_affected_rows: 0,
            local_infile_data: None,
            local_infile_accepted: false,
            set_var_hint_restore: Vec::new(),
            statement_kind: StatementKind::Other,
            statement_in_select: false,
            statement_keys_read: Arc::default(),
            current_tso: tidb_exprctx::CurrentTso::default(),
            current_for_update_ts: tidb_exprctx::CurrentTso::default(),
            statement_txn_start_ts: 0,
            statement_ended_txn_start_ts: None,
            statement_began_txn: false,
            statement_committed_txn: false,
            connection_attributes: std::collections::BTreeMap::new(),
            staged_writes: std::sync::Arc::default(),
            server_info_syncer: None,
            cluster_topology: None,
            cluster_config: None,
            cluster_diagnostics: None,
            cluster_peer: None,
            // Go's Domain reports `serverIDForStandalone` until etcd assigns
            // one (`domain.go:752-753`), and the connection-ID allocator
            // encodes that same value, so `KILL` recognizes local
            // connections. A cluster node replaces this through
            // `set_server_info_syncer`.
            server_id_getter: Arc::new(|| tidb_globalconn::SERVER_ID_FOR_STANDALONE),
            cluster_schema_version: None,
            take_snapshot: None,
            index_usage_collector: Arc::new(tidb_stats_handle_usage_indexusage::Collector::new()),
            session_index_usage_collector: None,
            data_lock_waits: None,
            historical_read_provider: None,
            statement_txn_activator: None,
            snapshot_schema_provider: None,
            snapshot_schema: None,
            statement_snapshot: None,
            restricted_sql_executor: None,
            column_stats_usage: None,
            analyze_status: None,
            approximate_table_counts: std::sync::Arc::new(
                tidb_pdhelper::NoPdApproximateTableCounts::default(),
            ),
            table_storage_stats: None,
            bdr_role_store: None,
            global_ids: None,
            regions: std::sync::Arc::new(tidb_kv::region_map::LocalRegionStore::default()),
            ddl_job_history_store: None,
            stats_collector: None,
            transaction_table_delta: std::sync::Arc::new(
                tidb_variable::table_delta::TableDeltaMap::new(),
            ),
            mdl_related_tables: None,
            mdl_statement_locks: false,
            mdl_statement_tables: std::sync::Arc::default(),
            statement_var_cache: std::cell::RefCell::new(None),
            cost_env_cache: std::cell::RefCell::new(None),
            prepared_plan_cache_environment_cache: std::cell::RefCell::new(None),
            published_last_insert_id: Arc::default(),
            retry_auto_ids: Arc::default(),
            row_id_shards: Arc::default(),
            non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache::default(),
            physical_plan_cache: tidb_executor::SessionPlanCache::default(),
            plan_cache_invalidation,
            stats_handle,
            parse_warnings: None,
            last_result_output_names: Vec::new(),
            last_result_current_db: String::new(),
            user_vars: tidb_variable::user_vars::UserVars::new(),
            sequence_last_values: Arc::default(),
            statement_boundary_open: false,
            process: None,
            has_process_priv: false,
            privileges: None,
            privilege_bypassed: false,
            rand: new_time_seeded_rand(),
            prepared_statements: prepared_statements::PreparedStore::default(),
            protocol_session_states: Default::default(),
            prepared_params: None,
            session_bindings: binding::SessionBindings::default(),
            global_binding_cache: None,
            global_binding_writer: None,
            pushdown_blacklists: blacklist::PushdownBlacklists::default(),
            client_local_file_reader: None,
            planned_apply: Arc::default(),
            mpp_query_info: Arc::default(),
            external_mpp_query_scope: false,
            mpp_attempt_completed: false,
            process_plan_info: Arc::default(),
            phase_durations: Default::default(),
            sql_cpu_usages: Arc::default(),
            ruv2_pending_session_parser_total: 0,
            binary_prepared_execution: false,
            node: Some(node),
        }
    }

    fn breakpoint_notify_func(&self) -> Option<Arc<dyn Fn(String) + Send + Sync + 'static>> {
        self.context_values
            .get(tidb_breakpoint::NOTIFY_BREAK_POINT_FUNC_KEY)
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
            observer(tidb_sessionctx::StatementPhase::ExecutorReady);
        }
        if self
            .executor_first_run_breakpoint
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        tidb_breakpoint::inject(self, "beforeExecutorFirstRun");
    }
}

impl tidb_context::ValueStoreContext for Session {
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

impl Drop for Session {
    fn drop(&mut self) {
        // A session over a booted domain closes inside its node connection,
        // which owns the transaction, pins and the table locks Go's
        // `session.Close` releases through the DDL (`UnlockTables`); a
        // session without a node cannot have taken one.
        self.node_conn().close();
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


mod account;
mod account_password;
use tidb_bindinfo::binding;
mod binding_arm;
mod compiler_session;
mod stmt_ctx_session;
mod execute_prepared;
pub use binding_arm::GlobalBindingWriter;
use tidb_bindinfo::binding_cache;
use tidb_executor::blacklist;
mod bootstrap;
mod classify;
pub mod session_errors;
mod ddl_jobs;
pub mod ddl_service;
pub mod domain_services;
pub mod stored_state_service;
pub mod analyze_service;
mod load_data;
pub mod stats_statement_service;
mod dispatch;
mod gcutil;
pub mod restricted_sql;
mod identity;
pub mod load_stats_arm;
mod account_state;
use tidb_executor::compiler::non_prepared_plan_cache as non_prepared_plan_cache;
mod nontransactional;
pub use nontransactional::{NonTransactionalDml, NonTransactionalInput, NonTransactionalStep};
mod noop;
mod observation;
use tidb_executor::compiler::prepared_ast as prepared_ast;
use tidb_executor::compiler::prepared as prepared_statements;
pub mod session_states;
mod record_set;
mod session_plan_cache;
use record_set::StatementCompletion;
pub use record_set::{SessionRecordSet, StatementExecution};
use tidb_executor::record_set::OpenedStatement;
use tidb_executor::record_set::StatementRecordSet;
mod resource_group;
mod table_lock;
mod temptable;
pub use table_lock::ClusterDdlLockEffect;
pub mod domain;
pub mod lazy_transaction;
pub mod statement_lifecycle;
pub mod store_session;
pub mod txn_manager;
pub mod txn_mode;
mod txn;
pub use txn::{
    HistoricalRead, HistoricalReadProvider, SnapshotSchemaProvider, StatementTxnActivator,
    TxnManagerOps,
};
mod user_table;
mod variables;
mod warnings;
pub(crate) use classify::{
    statement_kind_of, statement_priority_of, StatementKind,
};
pub use classify::StmtKind;
pub use prepared_ast::PreparedAst;
pub(crate) use variables::datum_text;
use tidb_context::SqlWarn;
use tidb_context::WarningLevel;
#[cfg(test)]
mod tests_expensivequery;
mod privilege_tables;
#[cfg(test)]
mod tests_privilege_tls;
mod process_arm;
mod show;
use tidb_variable::vars;
#[cfg(test)]
mod tests_variable_session;
use tidb_domain::global_sysvars::GlobalSysvars;
pub use vars::{SessionVars, VarError};

impl Session {
    /// The session's SQL killer (Go `SessionVars.SQLKiller`), where the
    /// connection installs its liveness probe.
    #[must_use]
    pub fn sql_killer(&self) -> Arc<tidb_sqlkiller::SqlKiller> {
        Arc::clone(self.session_memory.sql_killer())
    }

    /// Starts the cancellation lifetime for the next wire command.
    #[must_use]
    pub fn begin_query_cancellation(&self) -> tidb_sqlkiller::StatementCancellation {
        self.session_memory.begin_query_cancellation()
    }

    /// Captures the internal session's live process and a fresh cancellation scope.
    pub fn system_process(&self) -> Option<process::SystemProcess> {
        self.process
            .as_ref()
            .map(|guard| guard.system_process(self.begin_query_cancellation()))
    }

    /// The live transaction timestamp authority used by storage integration.
    #[must_use]
    pub fn current_tso(&self) -> tidb_exprctx::CurrentTso {
        self.current_tso.clone()
    }

    /// Go `SessionVars`' planning-phase clocks, shared with the transaction
    /// tier that waits for the statement's timestamp (`DurationWaitTS`).
    #[must_use]
    pub fn phase_durations(&self) -> tidb_variable::phase_durations::SessionPhaseDurations {
        self.phase_durations.clone()
    }

    /// Where the cluster tier publishes the open pessimistic transaction's
    /// for-update timestamp (Go `TxnCtx.SetForUpdateTS`).
    pub fn current_for_update_ts(&self) -> tidb_exprctx::CurrentTso {
        self.current_for_update_ts.clone()
    }

    /// Records the running autocommit statement's transaction start
    /// timestamp before its tier clears [`Self::current_tso`], for the
    /// statement's completion (Go `FinishExecuteStmt` reads `TxnCtx.StartTS`
    /// before the next statement replaces the transaction context).
    pub fn record_statement_txn_start_ts(&mut self, start_ts: u64) {
        self.statement_txn_start_ts = start_ts;
    }

    /// Go `ConnectionInfo.Attributes`: the connection attributes the client
    /// sent in its handshake response.
    pub fn set_connection_attributes(
        &mut self,
        attributes: std::collections::BTreeMap<String, String>,
    ) {
        self.connection_attributes = attributes;
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
    /// [`Self::publish_transaction_lock_waiting`] as an observer a statement's
    /// executors can hold (Go `LazyTxn.LockKeys`'s state updates).
    pub fn transaction_lock_state_observer(
        &self,
    ) -> Option<tidb_sessionctx::select_lock::LockStateObserver> {
        let process = self.process.as_ref()?;
        let registry = process.registry().clone();
        let id = process.id();
        Some(tidb_sessionctx::select_lock::LockStateObserver::new(
            move |waiting| {
                registry.transaction_state(id, if waiting { "LockWaiting" } else { "Running" });
            },
        ))
    }

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
        syncer: std::sync::Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>,
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
        mode: tidb_txnkv::ReplicaReadType,
    ) -> tidb_txnkv::ReplicaReadType {
        // Go GetReplicaRead gives the statement hint priority over the domain
        // switch. Planner eligibility and execution must use the same decision.
        if self.stmt_hints.has_replica_read_hint {
            return tidb_txnkv::ReplicaReadType::from_raw(self.stmt_hints.replica_read);
        }
        if mode == tidb_txnkv::ReplicaReadType::ClosestAdaptive
            && self
                .cluster_topology
                .as_ref()
                .is_some_and(|topology| !topology.adaptive_enabled())
        {
            tidb_txnkv::ReplicaReadType::Leader
        } else {
            mode
        }
    }

    /// Installs the process's shared internal HTTP configuration retriever.
    pub fn set_cluster_config_client(
        &mut self,
        client: Arc<tidb_executor::cluster_config::ClusterConfigClient>,
    ) {
        self.cluster_config = Some(client);
    }

    /// Installs the process's shared diagnostics gRPC client, which the
    /// server-info and log cluster tables read through.
    pub fn set_cluster_diagnostics_client(
        &mut self,
        client: Arc<tidb_executor::cluster_diagnostics::ClusterDiagnosticsClient>,
    ) {
        self.cluster_diagnostics = Some(client);
    }

    /// Installs the shared outgoing TiDB coprocessor capability.
    pub fn set_cluster_peer_client(
        &mut self,
        client: Arc<tidb_executor::cluster_peer::ClusterPeerClient>,
    ) {
        self.cluster_peer = Some(client);
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
                tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
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
    /// Go `GetRelatedTableForMDL().Store(id, domainSchemaVer)` as the planner
    /// reaches it from `buildDataSource`: each stored table it binds -- a
    /// view's body included -- joins the transaction's map at the node's
    /// schema version then. `None` when this statement locks nothing.
    pub(crate) fn mdl_related_table_recorder(&self) -> Option<tidb_variable::MdlRelatedTableRecorder> {
        if !self.mdl_statement_locks {
            return None;
        }
        let sink = self.mdl_related_tables.clone()?;
        let locked = self.mdl_statement_tables.clone();
        let version = self.cluster_schema_version.clone();
        let process = self
            .process
            .as_ref()
            .map(|process| (process.registry().clone(), process.id()));
        Some(std::sync::Arc::new(move |table_id: i64| {
            if !locked.lock().is_ok_and(|mut locked| locked.insert(table_id)) {
                return;
            }
            sink.record_table(table_id, version.as_ref().map_or(0, |source| source()));
            if let Some((registry, id)) = &process {
                registry.transaction_related_table(*id, table_id);
            }
        }))
    }

    pub(crate) fn cluster_schema_version_now(&self) -> i64 {
        self.cluster_schema_version
            .as_ref()
            .map_or(0, |source| source())
    }

    /// Go's `kv.CommitHook` (`sessiontxn/isolation/base.go:510`): the cluster
    /// transaction's own start and commit timestamps and the protocol that
    /// committed it, recorded once the commit is done.
    pub fn record_committed_txn_info(&mut self, start_ts: u64, commit_ts: u64, commit_mode: &str) {
        self.vars.last_txn_info = format!(
            "{{\"txn_scope\":\"global\",\"start_ts\":{start_ts},\"commit_ts\":{commit_ts},\"txn_commit_mode\":\"{commit_mode}\",\"async_commit_fallback\":false,\"one_pc_fallback\":false,\"pipelined\":false,\"flush_wait_ms\":0}}"
        );
    }

    /// [`Self::set_last_txn_info_started`] for the store transaction a server
    /// owns: Go `setLastTxnInfoBeforeTxnEnd` runs at the start of both
    /// `CommitTxn` and `RollbackTxn`, and only for a transaction that was
    /// activated.
    pub fn record_started_txn_info(&mut self, start_ts: u64) {
        self.set_last_txn_info_started(start_ts);
    }

    /// Go `setLastTxnInfoBeforeTxnEnd` (`pkg/session/session.go:1056-1069`):
    /// a transaction that activated but did not commit -- read-only, rolled
    /// back, stale -- records the same struct with only scope and start set,
    /// zeros included, because Go marshals the whole thing.
    pub(crate) fn set_last_txn_info_started(&mut self, start_ts: u64) {
        self.vars.last_txn_info = format!(
            "{{\"txn_scope\":\"global\",\"start_ts\":{start_ts},\"commit_ts\":0,\"txn_commit_mode\":\"\",\"async_commit_fallback\":false,\"one_pc_fallback\":false,\"pipelined\":false,\"flush_wait_ms\":0}}"
        );
    }

    pub(crate) fn last_txn_info_value(&self) -> String {
        self.vars.last_txn_info_value()
    }

    pub(crate) fn last_query_info_value(&self) -> String {
        self.vars.last_query_info_value()
    }

    /// `(tidb_init_chunk_size, tidb_max_chunk_size)` in effect.
    pub fn chunk_sizes(&self) -> (u64, u64) {
        let size = |name: &str, default: u64| {
            self.vars
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(default)
        };
        (size("tidb_init_chunk_size", 32), size("tidb_max_chunk_size", 1024))
    }

    /// The plan the last executed statement published (for `TRACE`).
    pub fn published_plan(&self) -> Option<std::sync::Arc<tidb_planner_core_operator_physicalop::PhysicalPlan>> {
        let cell = self
            .process_plan_info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cell.plan.as_ref().and_then(tidb_stmtctx::StoredPlan::get)
    }

    /// Go `recordLastDDLInfo`: the query of a DDL job this session waited for
    /// and its sequence number -- Go's `seqAllocator`, a per-process counter
    /// of finished jobs.
    pub fn record_last_ddl_info(&mut self, query: &str) {
        static DDL_JOB_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = DDL_JOB_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        self.vars.last_ddl_info = tidb_sessionstates::LastDDLInfo {
            query: query.to_owned(),
            seq_num: seq,
        };
    }

    /// `@@tidb_last_ddl_info`: Go's `json.Marshal(LastDDLInfo)`.
    pub(crate) fn last_ddl_info_value(&self) -> String {
        self.vars.last_ddl_info_value()
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

    /// Binds Go `executor.TakeSnapshot`, which `ADMIN CREATE WORKLOAD
    /// SNAPSHOT` runs.
    pub fn set_take_snapshot(&mut self, take_snapshot: tidb_executor::admin::TakeSnapshot) {
        self.take_snapshot = Some(take_snapshot);
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
    ) -> std::collections::HashMap<i64, tidb_variable::table_delta::TableDelta> {
        self.transaction_table_delta.snapshot()
    }

    /// Restores Go's table-delta part of one transaction savepoint.
    pub fn restore_table_delta_savepoint(
        &self,
        savepoint: std::collections::HashMap<i64, tidb_variable::table_delta::TableDelta>,
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
    /// Installs the node's `GlobalPDHelper`.
    pub fn set_approximate_table_count_provider(
        &mut self,
        provider: std::sync::Arc<dyn tidb_pdhelper::ApproximateTableCountProvider>,
    ) {
        self.approximate_table_counts = provider;
    }

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

    /// Installs the store that holds the cluster's BDR role.
    pub fn set_bdr_role_store(&mut self, store: std::sync::Arc<dyn BdrRoleStore>) {
        self.bdr_role_store = Some(store);
    }

    /// Installs the session's store's regions.
    pub fn set_region_store(
        &mut self,
        regions: std::sync::Arc<dyn tidb_kv::region_map::RegionStore>,
    ) {
        self.regions = regions;
    }

    /// The session's store's regions (Go `kv.SplittableStore`).
    #[must_use]
    pub fn region_store(&self) -> std::sync::Arc<dyn tidb_kv::region_map::RegionStore> {
        std::sync::Arc::clone(&self.regions)
    }

    /// Installs the session's store's global-id allocator.
    pub fn set_global_id_generator(&mut self, ids: std::sync::Arc<dyn GlobalIdGenerator>) {
        self.global_ids = Some(ids);
    }

    /// Go `meta.Mutator.GenGlobalID` in a new transaction on the session's
    /// store.
    ///
    /// # Panics
    /// On a session no store installed an allocator in, as Go's nil store.
    pub(crate) fn gen_global_id(&self) -> Result<i64, DriverError> {
        self.global_ids
            .as_ref()
            .expect("the session's store is installed by the node that opened it")
            .gen_global_id()
    }

    /// Installs the cluster store `ADMIN SHOW DDL JOBS` and
    /// `information_schema.DDL_JOBS` read.
    pub fn set_ddl_job_history_store(&mut self, store: std::sync::Arc<dyn DdlJobHistoryStore>) {
        self.ddl_job_history_store = Some(store);
    }

    /// Whether the BDR role lives in an installed store.
    #[must_use]
    pub fn has_bdr_role_store(&self) -> bool {
        self.bdr_role_store.is_some()
    }

    /// Go `meta.Mutator`'s `BDRRole` key on the session's store.
    fn bdr_role_store(&self) -> Result<std::sync::Arc<dyn BdrRoleStore>, DriverError> {
        self.bdr_role_store.clone().ok_or_else(|| {
            DriverError::unsupported("ADMIN SET/UNSET/SHOW BDR ROLE needs this node's TiKV meta store")
        })
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
                    tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
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
        filters: &[tidb_planner::memtable_predicate_extractor::ClusterTableFilter],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let ctx = self.statement_context(false);
        let rows = self.viewer_state().cluster_config_rows(&ctx, filters);
        self.drain_eval_warnings(&ctx);
        rows
    }

    /// Go's seven-source GetClusterServerInfo in dataForTiDBClusterInfo column order.
    #[cfg(test)]
    pub(crate) fn cluster_info_table_rows(
        &mut self,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let ctx = self.statement_context(false);
        let rows = self.viewer_state().cluster_info_rows(&ctx);
        self.drain_eval_warnings(&ctx);
        rows
    }

    /// Go infoschema.GetInstanceAddr uses local status identity. This does not
    /// discover peers: loss of etcd cannot relabel or hide local process rows.
    #[cfg(test)]
    pub(crate) fn cluster_instance_address(&self) -> String {
        self.viewer_state().cluster_instance_address()
    }

    /// Go `setDataForServersInfo`: one row per server `GetAllServerInfo`
    /// reports, in Go's column order.
    #[cfg(test)]
    pub(crate) fn tidb_servers_info_table_rows(
        &self,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        self.viewer_state().tidb_servers_info_rows()
    }

    /// A fresh session over a freshly booted embedded store, as Go's
    /// `testkit.CreateMockStoreAndDomain` + `CreateSession4Test`
    /// ([`Session::new_on_mock_store`]).
    #[must_use]
    pub fn new() -> Self {
        Session::new_on_mock_store()
    }

    /// A session on the store whose node keeps its accounts in `accounts`
    /// (booting one over that table when none does yet), with `accounts`
    /// attached: Go testkit's session on the store whose privilege tables a
    /// test provisioned.
    #[cfg(test)]
    pub(crate) fn on_accounts(accounts: &tidb_privilege::PrivilegeRegistry) -> Self {
        let domain = match crate::domain::mock_store::domain_of_accounts(accounts) {
            Some(domain) => domain,
            None => crate::domain::mock_store::MockStoreDomain::create_with_accounts(Some(
                accounts.clone(),
            ))
            .unwrap_or_else(|error| panic!("boot the mock store: {error}")),
        };
        let mut session = domain
            .new_session()
            .unwrap_or_else(|error| panic!("open a mock store session: {error}"));
        session.use_test_database();
        session
    }

    /// Go testkit's `CreateMockStore` + `NewTestKit`: a session over a
    /// freshly booted embedded store, whose statements run through the
    /// node's connection path (DDL as persisted jobs, writes committed
    /// through the store).
    ///
    /// # Panics
    ///
    /// When the embedded store cannot boot, as Go's testkit fails the test.
    #[must_use]
    pub fn new_on_mock_store() -> Self {
        let domain = crate::domain::mock_store::MockStoreDomain::create()
            .unwrap_or_else(|error| panic!("boot the mock store: {error}"));
        let mut session = domain
            .new_session()
            .unwrap_or_else(|error| panic!("open a mock store session: {error}"));
        session.use_test_database();
        session
    }

    /// [`Self::new_on_mock_store`] over a store with `nodes` TiFlash stores
    /// (Go `mockstore.WithMockTiFlash`).
    ///
    /// # Panics
    ///
    /// When the embedded store cannot boot.
    #[must_use]
    pub fn new_with_mock_tiflash(nodes: usize) -> Self {
        let domain = crate::domain::mock_store::MockStoreDomain::create_with_mock_tiflash(nodes)
            .unwrap_or_else(|error| panic!("boot the mock store: {error}"));
        let mut session = domain
            .new_session()
            .unwrap_or_else(|error| panic!("open a mock store session: {error}"));
        session.use_test_database();
        session
    }

    /// The `use test` the ported Go tests begin with (Go's `CurrentDB` starts
    /// empty): this harness's sessions select the bootstrapped `test` schema.
    fn use_test_database(&mut self) {
        self.select_database("test")
            .unwrap_or_else(|error| panic!("use test: {error:?}"));
    }

    /// Go `sctx.GetInfoSchema()`: the node's schema this session's catalog
    /// was built from.
    pub fn set_info_schema(&mut self, info_schema: Arc<tidb_infoschema::cluster_catalog::ClusterCatalog>) {
        self.info_schema = Some(info_schema);
    }

    /// Go `SessionVars.CurrentDB`. Empty when no database is selected.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.vars.current_db
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
                .statement_finished(guard.id(), &self.vars.current_db, &self.status_text());
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
                .statement_finished(guard.id(), &self.vars.current_db, &self.status_text());
        }
    }

    /// Selects NO schema, Go's `CurrentDB = ""`: every unqualified name is
    /// `ErrNoDB` (`Error 1046`) until a `USE` runs. A session starts in this
    /// state; `DROP DATABASE` of the current schema returns to it.
    pub fn deselect_database(&mut self) {
        self.vars.current_db = String::new();
    }

    /// Go `SimpleExec.executeUse` over this session and its schema.
    fn use_database(&mut self, name: &str) -> Result<(), DriverError> {
        let catalog = self.shared_catalog();
        let catalog = catalog.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        tidb_executor::simple::execute_use(self, &catalog, name)
    }

    /// The current database, or Go's `ErrNoDB` when none is selected.
    fn require_current_database(&self) -> Result<&str, DriverError> {
        if self.vars.current_db.is_empty() {
            return Err(DriverError::from(tidb_plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[])));
        }
        Ok(&self.vars.current_db)
    }

    /// Go `LAST_INSERT_ID()`: the first id the most recent ALLOCATING
    /// statement handed out. A statement that allocated nothing -- an explicit
    /// auto value, a table with no auto column, an UPDATE -- leaves it as it
    /// was, which is what MySQL and TiDB both do.
    #[must_use]
    pub fn last_insert_id(&self) -> u64 {
        self.vars.last_insert_id
    }

    /// The auto-increment ids the running statement has assigned, which a
    /// caller that RUNS THE STATEMENT AGAIN rewinds between attempts and
    /// clears when the statement is finally over. See
    /// `tidb_sessionctx::RetryAutoIds`.
    #[must_use]
    pub fn retry_auto_ids(&self) -> &Arc<std::sync::Mutex<tidb_sessionctx::RetryAutoIds>> {
        &self.retry_auto_ids
    }

    /// The id the last statement allocated, which the OK packet reports and
    /// which is 0 when the statement allocated nothing.
    #[must_use]
    pub fn statement_insert_id(&self) -> u64 {
        self.vars.statement_insert_id
    }

    /// The info string produced by the statement most recently executed.
    #[must_use]
    pub fn statement_message(&self) -> &str {
        &self.statement_message
    }

    /// The affected rows a result-set statement's terminal OK packet reports
    /// (Go `cc.ctx.AffectedRows()` in `writeEOF`).
    #[must_use]
    pub fn statement_affected_rows(&self) -> u64 {
        self.statement_affected_rows
    }

    /// Clears the statement info for command paths that do not enter the
    /// normal executor lifecycle (for example `SET` and routed DDL).
    /// Go `StmtCtx.SetMessage`: the OK packet's info for this statement.
    pub(crate) fn set_statement_message(&mut self, message: String) {
        self.statement_message = message;
    }

    pub fn clear_statement_message(&mut self) {
        self.statement_message.clear();
    }

    /// Install the domain notification owner; it survives scratch globals used by SET.
    pub fn set_global_config_syncer(
        &mut self,
        syncer: Arc<tidb_domain_globalconfigsync::GlobalConfigSyncer>,
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

    /// Go `session.SetCollation` (`pkg/session/session.go`), which
    /// `TiDBDriver.OpenCtx` runs with the handshake's collation id before the
    /// connection authenticates. The charset and collation are written
    /// straight into the session variables, without validation and without a
    /// statement, so the login touches no statement counter, warning buffer
    /// or statement-observability state.
    ///
    /// An id outside Go's collation table is Go's `GetCharsetInfoByID` error,
    /// which fails the connection; its text is the error's whole diagnostic.
    pub fn set_collation(&mut self, collation_id: i32) -> Result<(), String> {
        let (charset, collation, error) = tidb_charset::get_charset_info_by_id(collation_id);
        if let Some(error) = error {
            // Go logs this inside `GetCharsetInfoByID`; tidb-datatype has no
            // logger, so the warning is raised by its caller here.
            tidb_log::warn(
                &format!(
                    "unable to get collation name from collation ID, return default charset and collation instead ID={collation_id}"
                ),
                &[],
            );
            return Err(error.to_string());
        }
        // If new collations are enabled, switch to the default collation if
        // this one is not supported.
        let collation = tidb_datatype::substitute_missing_collation_to_default(&collation);
        for name in [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ] {
            if let Err(error) = self
                .vars
                .set_system_var_without_validation(name, charset.clone())
            {
                // Go `terror.Log`: logged at error level, not returned.
                tidb_log::error(&format!("encountered error: {error:?}"), &[]);
            }
        }
        self.vars
            .set_system_var_without_validation("collation_connection", collation)
            .map_err(|error| format!("{error:?}"))
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
        static INDEX: tidb_variable::sysvar::StaticSysVarIndex =
            tidb_variable::sysvar::StaticSysVarIndex::new("wait_timeout");
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

    /// A session over `catalog`, a schema the node built: Go
    /// `createSessionWithOpt` over the domain's infoschema. The node's
    /// connection path and internal sessions open theirs here.
    pub(crate) fn over_catalog(
        catalog: SharedCatalog,
        node: Box<crate::domain::cluster_session_node::NodeConn>,
    ) -> Self {
        Session::unbootstrapped(catalog, node)
    }

    /// Another connection to the store whose session `catalog` came from, as
    /// Go's `testkit.NewTestKit(t, store)`: a node session's catalog names
    /// its domain.
    ///
    /// # Panics
    ///
    /// When `catalog` is no store session's, or the store cannot open a
    /// session.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        let mut session = crate::domain::mock_store::domain_of_catalog(&catalog)
            .expect("a catalog of a session on a store")
            .new_session()
            .unwrap_or_else(|error| panic!("open a mock store session: {error}"));
        session.use_test_database();
        session
    }

    /// Installs the server-owned spill policy for every statement created by
    /// this session.
    pub fn set_spill_storage(&mut self, storage: Arc<tidb_spill_storage::SpillStorage>) {
        self.session_memory.set_spill_storage(storage);
    }

    /// Installs the server-owned process memory arbitrator for statements this
    /// session starts after the authority becomes available.
    pub fn set_mem_arbitrator(&mut self, arbitrator: Arc<tidb_memory::MemArbitrator>) {
        self.session_memory.set_mem_arbitrator(arbitrator);
    }

    /// The shared catalog handle, for opening a peer session over the same
    /// schema state.
    #[must_use]
    pub fn shared_catalog(&self) -> SharedCatalog {
        SharedCatalog::clone(&self.catalog)
    }

    /// The number of `?` markers a statement carries, which
    /// `COM_STMT_PREPARE` reports to the client.
    pub fn parameter_count(&self, sql: &str) -> Result<usize, DriverError> {
        Ok(tidb_executor::parsed_parameter_count(&self.parse(sql)?))
    }

    /// Go `ExecStmt.Exec` for one statement inside the `ExecuteStmt` the
    /// connection is running: the statement's executor over this session,
    /// in the transaction context and snapshot the connection prepared.
    pub(crate) fn exec_statement_parsed(
        &mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_using(sql, false, move |session| {
            session.execute_statement_owned(stmt, sql)
        })
        .map(|(output, _)| output)
    }

    /// [`Self::exec_statement_parsed`] over the text of a statement whose
    /// markers take `params`.
    pub(crate) fn exec_statement_with_params(
        &mut self,
        sql: &str,
        params: &[Datum],
    ) -> Result<StmtOutput, DriverError> {
        // The count is checked even when no values were sent, so a statement
        // with an unbound marker is Go's ErrWrongParamCount.
        if params.is_empty() && self.parameter_count(sql)? == 0 {
            return self
                .run_with_columns_using(sql, false, |session| session.execute_statement(sql))
                .map(|(output, _)| output);
        }
        let bound = tidb_executor::bind_statement(self.parse(sql)?, params)?;
        self.exec_statement_parsed(bound, sql)
    }

    /// Runs one statement with its prepared-statement parameters bound.
    ///
    /// Go installs the execute-time values on the parsed statement's own
    /// markers, so this door parses the text once -- in the client's
    /// encodings, as the prepare did -- binds the values onto that tree and
    /// runs it, with no restored text to lex again.
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
        let bound = tidb_executor::bind_statement(self.parse(sql)?, params)?;
        self.run_with_columns_parsed(bound, sql)
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
        if let Some(columns) = session_states::prepared_columns(&statement) {
            return Ok(columns);
        }
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
        let current_db = self.vars.current_db.clone();
        let ctx = self
            .statement_context(false)
            .with_prepared_params(parameters.unwrap_or_default());
        self.with_catalog_retrieving(&ctx, |catalog| {
            tidb_executor::plan_select_meta_stmt(&select, catalog, &current_db, &ctx)
        })
    }

    /// Resolves PREPARE result metadata with NULL markers without publishing
    /// an execution cache entry or access-path pins.
    pub fn probe_prepared(&mut self, prepared: &PreparedAst) -> Result<StmtOutput, DriverError> {
        if let Some(columns) = session_states::prepared_columns(prepared.statement()) {
            return Ok(StmtOutput::Rows { columns, rows: Vec::new() });
        }
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
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
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
        self.node_conn().run_parsed_statement_output(sql, stmt)
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
        // Go `handleQuery` clears `InMultiStmts` for every COM_QUERY and sets
        // it once a multi-statement text is admitted.
        self.vars.in_multi_stmts = false;
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
        let Ok(statements) = tidb_parser::parse_multi_with_sql_mode_and_config(
            sql,
            mode,
            self.vars.build_parser_config(),
        ) else {
            return Ok(vec![sql.to_owned()]);
        };
        // Go `conn.go:1874`: a text that parses to ZERO statements — all
        // whitespace, comments, or bare semicolons — is answered with a plain
        // OK packet, never a syntax error. The empty list is that answer.
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.vars.build_parser_config().charset_client;
        if statements.len() == 1 {
            return Ok(vec![sql.to_owned()]);
        }
        if !client_multi_statements {
            match self.vars().multi_statement_mode() {
                0 => {
                    return Err(DriverError::from((&tidb_server_err::servererr_errors::ERR_MULTI_STATEMENT_DISABLED).generate_with_stack(DISABLED.to_owned())))
                }
                1 => {}
                _ => self.deferred_multi_statement_warning = true,
            }
        }
        self.vars.in_multi_stmts = true;
        // Each statement's exact source bytes (Go `OriginalText`), carried as
        // the whole text was for the parse each one gets again.
        Ok(statements
            .iter()
            .map(|statement| {
                tidb_parser::carry_client_sql(statement.original_text(), client).into_owned()
            })
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

    /// Go `ExecStmt.OutputNames` of the last statement's materialized query
    /// result, one per column, and the `CurrentDB` it opened under: the
    /// inputs of `colNames2ResultFields`. The names are empty when the
    /// statement produced no query result or its plan had none.
    #[must_use]
    pub fn last_result_output_names(&self) -> (&[tidb_datatype::FieldNameMetadata], &str) {
        (&self.last_result_output_names, &self.last_result_current_db)
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

    /// Go `GetTxnWriteThroughputSLI`: returns the session's transaction-wide
    /// SLI accumulator.
    pub fn txn_write_throughput_sli(&mut self) -> &mut tidb_sli::TxnWriteThroughputSli {
        &mut self.write_sli
    }

    /// Go `ExecStmt.FinishExecuteStmt`'s read-key record: the statement's
    /// coprocessor processed keys (`ExecDetails.ScanDetail.ProcessedKeys`)
    /// reach the SLI only for a write statement that affected rows.
    pub fn add_statement_read_keys(&mut self, affected_rows: u64) {
        let processed_keys = self
            .statement_keys_read
            .load(std::sync::atomic::Ordering::Relaxed);
        if processed_keys > 0 && affected_rows > 0 {
            self.write_sli
                .add_read_keys(i64::try_from(processed_keys).unwrap_or(i64::MAX));
        }
    }

    /// Go server `addQueryMetrics`' SLI call at the end of one SQL command.
    pub fn finish_txn_write_throughput(&mut self, cost: Duration) {
        let cost = i64::try_from(cost.as_nanos()).unwrap_or(i64::MAX);
        let affected_rows = u64::try_from(self.vars.prev_row_count.max(0)).unwrap_or(0);
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
        self.node_conn().run_statement_output(sql)
    }

    /// A read outside any statement: `body` runs over this session with a
    /// fresh committed snapshot bound to its tables (Go's
    /// `tk.Session().GetStore()` snapshot at the current timestamp).
    pub fn with_committed_snapshot<R>(
        &mut self,
        body: impl FnOnce(&mut Session) -> R,
    ) -> Result<R, DriverError> {
        self.node_conn().with_read_snapshot(body)
    }

    /// Binds a fresh committed snapshot to this session's tables and leaves
    /// it bound, for a statement opened directly on the session whose record
    /// set outlives the call; the next statement's own binding replaces it.
    pub fn bind_committed_snapshot(&mut self) -> Result<(), DriverError> {
        self.node_conn().bind_open_statement_snapshot()
    }

    /// Go testkit's `dom.StatsHandle().Update(ctx, is)` for a session on a
    /// store: its domain's stats cache reloads what `mysql.stats_meta` says
    /// changed.
    pub fn update_stats_cache(&mut self) -> Result<(), DriverError> {
        self.node().update_stats_cache()
    }

    /// Go testkit's `dom.StatsHandle().UpdateStatsCache(statstypes.CacheUpdate{
    /// Updated: tables})` on this session's node: the node's statistics
    /// cache takes `tables` as they are, and every catalog over the node
    /// plans from them.
    ///
    /// # Panics
    ///
    /// On a session with no node, which has no statistics cache.
    #[doc(hidden)]
    pub fn update_stats_cache_with(&self, tables: Vec<Arc<tidb_stats::Table>>) {
        self.node().update_stats_cache_with(tables);
    }

    /// [`Session::update_stats_cache_with`] for statistics a test built in
    /// the planner's form over `catalog`'s table (or partition)
    /// `physical_id`, turned into Go's `statistics.Table` first.
    ///
    /// # Panics
    ///
    /// When `physical_id` is no table or partition of `catalog`.
    #[doc(hidden)]
    pub fn publish_table_statistics_for_test(
        &self,
        catalog: &tidb_infoschema::Catalog,
        physical_id: i64,
        statistics: &tidb_stats::Table,
    ) {
        let (schema, name) = catalog
            .table_names_by_id(physical_id)
            .or_else(|| catalog.find_table_by_partition_id(physical_id))
            .unwrap_or_else(|| panic!("{physical_id} is no table or partition of the catalog"));
        let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_in(&schema, &name) else {
            panic!("{schema}.{name} is not a stored table");
        };
        // Go's loader attaches each item's schema `Info` from `TableInfo`.
        let meta = table.meta();
        let meta = meta.read();
        let mut published = statistics.copy_as(tidb_stats::CopyIntent::AllDataWritable);
        published.hist_coll.physical_id = physical_id;
        published.is_pk_handle = table.pk_handle_offset().is_some();
        let handle_id = table
            .pk_handle_offset()
            .map(|offset| table.columns()[offset].id);
        for column in meta.columns.iter_deref() {
            let id = column.read().id;
            if let Some(item) = published.hist_coll.get_column(id) {
                let mut item = item.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                item.info = Some(column);
                item.physical_id = physical_id;
                item.is_handle = handle_id == Some(id);
            }
        }
        for index in meta.indices.iter_deref() {
            let id = index.read().id;
            if let Some(item) = published.hist_coll.get_index(id) {
                let mut item = item.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                item.info = Some(index);
                item.physical_id = physical_id;
            }
        }
        self.update_stats_cache_with(vec![Arc::new(published)]);
    }

    /// Go testkit's `dom.StatsHandle().DumpColStatsUsageToKV()` on this
    /// session's node: the predicate columns recorded so far reach
    /// `mysql.column_stats_usage`, where `ANALYZE ... PREDICATE` reads them.
    #[cfg(test)]
    pub(crate) fn dump_col_stats_usage_for_test(&self) {
        self.node()
            .dump_col_stats_usage()
            .unwrap_or_else(|error| panic!("dump column stats usage: {error}"));
    }

    /// Go `statstestutil.HandleNextDDLEventWithTxn` on this session's node:
    /// the statistics subscriber handles every pending DDL event (the
    /// `stats_meta` and placeholder histogram rows of a created table).
    ///
    /// # Panics
    ///
    /// On a session with no node, or when the events stay pending.
    #[doc(hidden)]
    pub fn handle_ddl_events_for_test(&self) {
        self.node()
            .handle_ddl_events()
            .unwrap_or_else(|error| panic!("handle DDL events: {error}"));
    }

    /// Go testkit's `dom.StatsHandle().GetPhysicalTableStats(id, tblInfo)`
    /// on this session's node, without caching a miss's `PseudoTable`.
    #[doc(hidden)]
    pub fn stats_table_for_test(&self, physical_id: i64) -> Option<Arc<tidb_stats::Table>> {
        self.node().stats_table(physical_id)
    }

    /// Observes the requests this session's store receives (`None` removes
    /// the observer).
    #[cfg(test)]
    pub(crate) fn set_store_send_hook(&self, hook: Option<tidb_unistore::rpc::SendHook>) {
        self.node().set_store_send_hook(hook);
    }

    /// Commits the delete of one raw key into this session's store, as Go's
    /// corruption tests do through a transaction of their own.
    #[cfg(test)]
    pub(crate) fn delete_store_key_for_test(&self, key: &[u8]) -> Result<(), String> {
        self.node().delete_key_for_test(key)
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
        self.last_result_output_names.clear();
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
        {
            let mut cell = self
                .process_plan_info
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let previous_plan = cell.plan.take();
            let previous_explain = cell.explain_marker;
            *cell = tidb_stmtctx::ProcessPlanInfo::default();
            cell.previous_plan = previous_plan;
            cell.previous_explain = previous_explain;
        }
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
        // Go publishes `stmtNode.Text()` (`SetProcessInfo`).
        let text = self.statement_text(sql);
        let normalized = arbitrated.then(|| {
            observation.map_or_else(
                || normalize_statement_digest(&text),
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
                    &text,
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
                tidb_sessmgr::processinfo::OOMAlarmVariablesInfo {
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
        self.vars.in_set_session_states_stmt = false;
        self.statement_message.clear();
        self.statement_affected_rows = 0;
        *self
            .published_last_insert_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        // Go promotes `FoundInPlanCache` into `PrevFoundInPlanCache` in
        // `ResetContextOfStmt`, at the same boundary as the other `Prev*`
        // fields above -- which is why `select @@last_plan_from_cache`
        // reports the PRECEDING statement rather than itself.
        self.vars.prev_found_in_plan_cache = std::mem::take(&mut self.vars.found_in_plan_cache);
        // Go `ExchangeChunkStatus`, at the same `ResetContextOfStmt` boundary.
        self.vars.pre_use_chunk_alloc = std::mem::take(&mut self.vars.use_chunk_alloc);
        // Go promotes `FoundInBinding` at the same boundary, which is why
        // `select @@last_plan_from_binding` reports the statement BEFORE it
        // rather than itself (that SELECT matches no binding of its own).
        self.vars.prev_found_in_binding = std::mem::take(&mut self.vars.found_in_binding);
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
        self.record_parse_failure_coded(reported.code, reported.message, reported.identity);
    }

    /// [`Self::record_parse_failure`] for front ends whose parse door
    /// surfaces an already-classified error.
    pub fn record_parse_failure_coded(
        &mut self,
        code: u16,
        message: String,
        identity: Option<tidb_terror::TerrorIdentity>,
    ) {
        self.warnings.clear();
        self.append_sql_warning(SqlWarn::from_error(WarningLevel::Error, code, message, identity));
    }

    /// go `driver_tidb.go:376`: every statement error lands in the statement
    /// warning buffer (DDL 1050/1007/1008/1051/1146 et al reach SHOW
    /// WARNINGS exactly as the query door's errors do).
    pub fn record_ddl_failure(
        &mut self,
        code: u16,
        message: String,
        identity: Option<tidb_terror::TerrorIdentity>,
    ) {
        // go `driver_tidb.go:376` appends the error, but the DDL execution
        // path may have already recorded it (HandleStatusErr in the
        // executor's own error handling). Skip the duplicate so SHOW
        // WARNINGS matches go's single row.
        if self
            .warnings
            .last()
            .map_or(false, |w| w.code() == code && w.message() == message)
        {
            return;
        }
        self.append_sql_warning(SqlWarn::from_error(WarningLevel::Error, code, message, identity));
    }

    /// Clears the statement warning buffer (go `ResetContextOfStmt`'s
    /// per-statement fresh start for write-door statements that bypass the
    /// session's own statement boundary).
    pub fn clear_statement_warnings(&mut self) {
        self.warnings.clear();
    }

    fn finish_statement_state(&mut self, result: &Result<StatementCompletion, DriverError>) {
        self.move_parse_warnings_behind_execution(result.is_ok());
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
                    &self.vars.current_db,
                    &self.status_text(),
                );
            }
        }
        let mut error_row = None;
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
            let eval_fatal = error.equal(&tidb_datatype::types_errors::ERR_JSON_DOCUMENT_NULL_KEY)
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
                    .retain(|w| !(w.code() == reported.code && w.message() == reported.message));
            }
            if reported.code != 1148 && reported.code != 3057 && !eval_fatal {
                // The inner execution may have already filed this exact
                // error row (the SET arm's `handleErr` append through the
                // delegated-account route); go's SHOW WARNINGS carries it
                // once.
                let duplicated = self.warnings.last().map_or(false, |w| {
                    w.code() == reported.code && w.message() == reported.message
                });
                if !duplicated {
                    error_row = Some(reported);
                }
            }
        }
        // Go records the summary inside `session.ExecuteStmt`
        // (`ExecStmt.FinishExecuteStmt` -> `SummaryStmt`), before
        // `TiDBContext.ExecuteStmt` files the error with
        // `StmtCtx.AppendError`: its warning count excludes the error row.
        self.finish_statement_observation(result);
        if let Some(reported) = error_row {
            self.append_sql_warning(SqlWarn::from_error(
                WarningLevel::Error,
                reported.code,
                reported.message,
                reported.identity,
            ));
        }
    }
}

#[cfg(test)]
mod session_source_tests {
    use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
    use tidb_executor::compiler::PreparedAstCompile;
    use super::{
        approx_compile_plan_token_count, approx_parse_sql_token_count, DomainMap,
        NoAvailableDomain, Session,
    };

    // Go pkg/session/tidb_test.go::TestDomapHandleNil.
    #[test]
    fn test_domap_handle_nil() {
        use std::sync::Arc;
        let store = crate::domain::mock_store::MockStoreDomain::create().unwrap();
        let domains = DomainMap::default();
        let unused = || -> Arc<tidb_domain_core::domain::Domain> {
            unreachable!("a nil store opens no domain")
        };
        assert!(matches!(domains.get(None, unused), Err(NoAvailableDomain)));

        let opened = domains
            .get(Some("store-a"), || Arc::clone(store.domain()))
            .unwrap();
        let available = domains.get(None, unused).unwrap();
        assert!(Arc::ptr_eq(&opened, &available));
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
        impl tidb_memory::RecordMemState for Recorder {
            fn load(&self) -> Result<Option<tidb_memory::RuntimeMemStateV1>, String> {
                Ok(None)
            }
            fn store(&self, _: &tidb_memory::RuntimeMemStateV1) -> Result<(), String> {
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
        let arbitrator = tidb_memory::MemArbitrator::new(1024, 4, 3, 0, Box::new(Recorder));
        arbitrator.set_work_mode(tidb_memory::ArbitratorWorkMode::Standard);
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
        session.bind_committed_snapshot().unwrap();
        let opened = session
            .open_prepared_point_get(execution, prepared.statement(), sql)
            .unwrap()
            .unwrap();
        assert_eq!(
            session.current_sql_digest_key,
            tidb_parser::normalize_digest(sql).0
        );
        drop(opened.attach(&mut session));
        arbitrator.set_work_mode(tidb_memory::ArbitratorWorkMode::Disable);
        session.run("SELECT 2").unwrap();
        assert!(session.current_sql_digest_key.is_empty());
    }

    #[test]
    fn statement_index_usage_collector_is_session_owned_and_flushed_on_close() {
        let global = tidb_stats_handle_usage_indexusage::Collector::new();
        global.start_worker();
        let mut session = Session::new();
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

        // The session's close flush only enqueues the delta (Go
        // `SendDeltaSync`); the global worker merges it on its own thread.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut sample = global.get_index_usage(41, 7);
        while sample.query_total == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
            sample = global.get_index_usage(41, 7);
        }
        assert_eq!(sample.query_total, 1);
        assert_eq!(sample.kv_req_total, 2);
        assert_eq!(sample.row_access_total, 3);
        global.close();
    }
}

#[cfg(test)]
mod tests_admin_check;
#[cfg(test)]
mod tests_error_class;
#[cfg(test)]
mod tests_error_codes;
#[cfg(test)]
mod tests_affinity_storage_class;
#[cfg(test)]
mod tests_alter_column;
#[cfg(test)]
mod tests_alter_schema;
#[cfg(test)]
mod tests_analyze;
#[cfg(test)]
mod tests_auto_increment;
#[cfg(test)]
mod tests_auto_random;
#[cfg(test)]
mod tests_bad_null;
#[cfg(test)]
mod tests_bdr;
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
mod tests_compile_path;
#[cfg(test)]
mod tests_core;
#[cfg(test)]
mod tests_datetime_year_compare;
#[cfg(test)]
mod tests_ddl_go_checks;
#[cfg(test)]
mod tests_ddl_preprocess;
#[cfg(test)]
mod tests_deadlock_history;
#[cfg(test)]
mod tests_decorrelate_count_having;
#[cfg(test)]
mod tests_derived_agg_pruning;
#[cfg(test)]
mod tests_dml_lock_keys;
#[cfg(test)]
mod tests_domain_plan_replayer_source;
#[cfg(test)]
mod tests_domain_serverinfo_info_source;
#[cfg(test)]
mod tests_domain_serverinfo_syncer_source;
#[cfg(test)]
mod tests_enum_index_range;
#[cfg(test)]
mod tests_multi_valued_index;
#[cfg(test)]
mod tests_index_merge_union;
#[cfg(test)]
mod tests_group_by_resolution;
#[cfg(test)]
mod tests_insert_go;
#[cfg(test)]
mod tests_max_keys_read;
#[cfg(test)]
mod tests_nontransactional;
#[cfg(test)]
mod tests_eval_bool;
#[cfg(test)]
mod tests_explain;
#[cfg(test)]
mod tests_explain_derived;
#[cfg(test)]
mod tests_explain_merge_join;
#[cfg(test)]
mod tests_expression_go_semantics;
#[cfg(test)]
mod tests_expression_indexes;
#[cfg(test)]
mod tests_expression_time;
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
mod tests_identifier_folding;
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
mod tests_mdl_related;
#[cfg(test)]
mod tests_memtable_extractor;
#[cfg(test)]
mod tests_merge_join_mixed_key_types;
#[cfg(test)]
mod tests_mixed_sign_index_join;
#[cfg(test)]
mod tests_modify_column_null;
#[cfg(test)]
mod tests_multi_table_dml;
#[cfg(test)]
mod tests_non_prepared_plan_cache;
#[cfg(test)]
mod tests_observation_batch;
#[cfg(test)]
mod tests_outer_join_elimination;
#[cfg(test)]
mod tests_partition;
#[cfg(test)]
mod tests_partition_exchange;
#[cfg(test)]
mod tests_partition_processor;
#[cfg(test)]
mod tests_partition_projection;
#[cfg(test)]
mod tests_partition_prune_collation;
#[cfg(test)]
mod tests_partition_reorganize;
#[cfg(test)]
mod tests_partition_show_go;
#[cfg(test)]
mod tests_planner_checks;
#[cfg(test)]
mod tests_planner_core_rewriter;
#[cfg(test)]
mod tests_planner_results;
#[cfg(test)]
mod tests_positional_orderby;
#[cfg(test)]
mod tests_prepared_plan_cache;
#[cfg(test)]
mod tests_stmt_ctx_build;
#[cfg(test)]
mod tests_compiler_plan_cacheable_checker;
#[cfg(test)]
mod tests_index_advisor;
#[cfg(test)]
mod tests_process_registry;
#[cfg(test)]
mod tests_prepared_statements;
#[cfg(test)]
mod tests_privilege_checks;
#[cfg(test)]
mod tests_pushdown_blacklist;
#[cfg(test)]
mod tests_read_cast;
#[cfg(test)]
mod tests_recursive_cte;
#[cfg(test)]
mod tests_region_split;
#[cfg(test)]
mod tests_resource_group;
#[cfg(test)]
mod tests_savepoint;
#[cfg(test)]
mod tests_sem_v2;
#[cfg(test)]
mod tests_sequence;
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
mod tests_slow_log;
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
mod tests_memory_tables;
#[cfg(test)]
mod tests_system_schemas;
#[cfg(test)]
mod tests_table_lock;
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
mod tests_ddl_service;
mod tests_stored_state_service;
#[cfg(test)]
mod tests_builtin_survey;
#[cfg(test)]
mod tests_window;
#[cfg(test)]
mod tests_write_conversion;
#[cfg(test)]
mod tests_zero_date;
pub mod mysql_bootstrap;
pub mod bootstrap_publish;
pub mod ddl_session_pool;
pub mod cluster_sysvar_writer;
use tidb_variable::sysvar;




impl tidb_executor::sessionctx::SessionContext for Session {
    fn session_vars(&self) -> &SessionVars {
        &self.vars
    }

    fn session_vars_mut(&mut self) -> &mut SessionVars {
        &mut self.vars
    }

    fn privilege_manager(&self) -> &dyn tidb_executor::sessionctx::PrivilegeManager {
        self
    }

    fn privilege_checks_bypassed(&self) -> bool {
        self.privilege_bypassed
    }

    fn has_privilege_registry(&self) -> bool {
        self.privileges.is_some()
    }

    fn user_vars(&self) -> &tidb_variable::user_vars::UserVars {
        &self.user_vars
    }

    fn append_sql_warning(&mut self, warning: tidb_context::SqlWarn) {
        Session::append_sql_warning(self, warning);
    }

    fn in_txn(&self) -> bool {
        self.in_transaction()
    }

    fn txn_is_staleness(&self) -> bool {
        self.historical_read_ts().is_some()
    }

    fn set_system_var(&mut self, name: &str, value: String) -> Result<(), DriverError> {
        self.set_session_sys_var(name, value)
    }

    fn set_global_sys_var(&mut self, name: &str, value: String) -> Result<(), DriverError> {
        self.set_node_wide_sys_var(name, value, true)
    }

    fn set_instance_sys_var(&mut self, name: &str, value: String) -> Result<(), DriverError> {
        self.set_node_wide_sys_var(name, value, false)
    }

    fn load_snapshot_info_schema_if_needed(
        &mut self,
        old_ts: u64,
        is_txn_read_ts: bool,
    ) -> Result<(), DriverError> {
        self.load_snapshot_schema_after_set(old_ts, is_txn_read_ts)
    }
}

impl tidb_planner::plan_builder::stmt_build::SessionPlanContext for Session {
    fn request_dynamic_verification(&self, privilege: &str, with_grant: bool) -> bool {
        self.has_dynamic_privilege(privilege, with_grant)
    }

    fn request_dynamic_verification_with_user(
        &self,
        privilege: &str,
        _with_grant: bool,
        user: &str,
        host: &str,
    ) -> bool {
        self.target_has_dynamic_privilege(user, host, privilege)
    }

    fn login_user(&self) -> Option<String> {
        // Go `SessionVars.User.Username`: the name the process list shows.
        self.current_identity()
            .is_some()
            .then(|| self.process_list_user())
    }

    fn user_identity(&self) -> Option<(String, String)> {
        self.current_identity()
            .map(|(user, host)| (user.to_owned(), host.to_owned()))
    }

    fn process_info(&self, connection_id: u64) -> Option<(String, String)> {
        let row = self.process.as_ref().and_then(|guard| {
            guard
                .registry()
                .snapshot()
                .into_iter()
                .find(|row| row.id == connection_id)
        })?;
        // The front end stores the display address (Go Host + Port); the
        // account match needs the presented host without its port.
        let host = row
            .host
            .rsplit_once(':')
            .filter(|(host, port)| {
                port.parse::<u16>().is_ok()
                    && (!host.contains(':') || (host.starts_with('[') && host.ends_with(']')))
            })
            .map_or(row.host.as_str(), |(host, _)| {
                host.trim_start_matches('[').trim_end_matches(']')
            })
            .to_owned();
        Some((row.user, host))
    }

    fn is_auto_analyze_process(&self, connection_id: u64) -> bool {
        // Go `sm != nil && handleutil.GlobalAutoAnalyzeProcessList.Contains(id)`:
        // the session manager is the process registry this session sits in.
        self.process.is_some()
            && tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(connection_id)
    }
}

impl tidb_executor::sessionctx::PrivilegeManager for Session {
    fn db_is_visible(&self, database: &str) -> bool {
        self.database_is_visible(database)
    }

    fn request_dynamic_verification(&self, privilege: &str, grantable: bool) -> bool {
        self.has_dynamic_privilege(privilege, grantable)
    }

    fn request_verification(
        &self,
        database: &str,
        table: &str,
        privilege: tidb_mysql::privilege::PrivilegeType,
    ) -> bool {
        // The registry's masks are Go's `mysql.PrivilegeType` bits.
        self.has_any_scoped_privilege(database, table, privilege.0)
    }

    fn get_all_roles(&self, user: &str, host: &str) -> Vec<(String, String)> {
        self.privileges.as_ref().map_or_else(Vec::new, |registry| {
            registry.granted_roles(&(user.to_owned(), host.to_owned()))
        })
    }

    fn get_default_roles(&self, user: &str, host: &str) -> Vec<(String, String)> {
        self.privileges.as_ref().map_or_else(Vec::new, |registry| {
            registry.default_roles(&(user.to_owned(), host.to_owned()))
        })
    }

    /// Go `checker.FindEdge` reads the privilege cache: a role edge written
    /// into `mysql.role_edges` directly counts only after FLUSH PRIVILEGES.
    fn find_edge(&self, role: &(String, String), user: &(String, String)) -> bool {
        self.account_statement_privilege_cache
            .as_ref()
            .or(self.privileges.as_ref())
            .is_some_and(|cache| cache.has_role(user, role))
    }
}
