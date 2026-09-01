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

//! What one statement is handed and what it hands back: Go's
//! `StatementContext`, built from this session's variables, and the `Prev*`
//! promotion `ResetContextOfStmt` performs at the statement boundary.
//!
//! Every session-state read a statement makes -- the clock, the time zone, the
//! sql_mode bits, the sequence names, the user variables, the last-insert-id
//! channel -- is funnelled through [`Session::statement_context`], so an
//! expression never reaches back into the session for anything.

use std::collections::HashMap;
use std::sync::Arc;

use crate::{DriverError, Session, StatementKind, StmtOutput};

/// The statement context's SESSION-VARIABLE half, parsed once per
/// variable-table generation instead of once per statement.
///
/// Go's analogue is `SessionVars` itself: every one of these is a typed
/// field there (`AllowWriteRowID`, `CTEMaxRecursionDepth`,
/// `SQLMode`...), maintained by each variable's `SetSession` hook, so a
/// Go statement reads fields where this port was doing twenty-six
/// by-name string lookups and parses -- measured as the heaviest
/// user-code frame on both the read and write paths once the metadata
/// clones were gone. GLOBAL-scope statement policies
/// (`tidb_mem_oom_action`, `tidb_enable_tmp_storage_on_oom`) stay LIVE in the
/// builder, while expressions retain the shared global accessor itself.
pub(crate) struct StatementVarSnapshot {
    generation: u64,
    version: Option<String>,
    time_zone: tidb_executor::SessionTimeZone,
    connection_charset: String,
    connection_collation: String,
    allow_write_row_id: bool,
    sysdate_is_now: bool,
    timestamp: Option<f64>,
    sql_mode: tidb_mysql::SqlMode,
    scanner_sql_mode: tidb_parser::SqlMode,
    allow_auto_random_explicit_insert: bool,
    shard_allocate_step: u64,
    like_default_escape: u8,
    week_format: i64,
    div_scale: u32,
    cte_depth: i64,
    join_reorder_threshold: i32,
    default_string_match_selectivity: f64,
    enable_pseudo_for_outdated_stats: bool,
    stats_load_sync_wait_ms: u64,
    stats_load_pseudo_timeout: bool,
    plan_replayer_capture_enabled: bool,
    opt_index_prune_threshold: i32,
    opt_prefix_index_single_scan: bool,
    always_keep_join_key: bool,
    enable_unsafe_substitute: bool,
    enable_semi_join_rewrite: bool,
    allow_in_subq_to_join_and_agg: bool,
    enable_no_decorrelate_in_select: bool,
    enable_skew_distinct_agg: bool,
    max_execution_time_ms: u64,
    advanced_join_reorder: bool,
    constraint_check_in_place: bool,
    ordering_index_selectivity_ratio: f64,
    allow_projection_push_down: bool,
    limit_push_down_threshold: u64,
    index_lookup_push_down_session: tidb_planner::access_path::IndexLookupPushDownSession,
    join_reorder_through_proj: bool,
    join_reorder_through_sel: bool,
    outer_join_reorder: bool,
    index_merge: bool,
    static_partition_prune: bool,
    new_only_full_group_by_check: bool,
    mem_quota: i64,
    replica_read: tidb_executor::ReplicaReadType,
    isolation_read_engines: String,
    init_chunk_size: usize,
    max_chunk_size: usize,
    max_allowed_packet: u64,
    group_concat_max_len: u64,
    apply_cache_capacity: i64,
    hashagg_partial_concurrency: usize,
    hashagg_final_concurrency: usize,
    block_encryption_mode: tidb_executor::BlockEncryptionMode,
    ddl_cdc_write_source: u64,
    ddl_reorg_priority: i64,
    ddl_session_alias: String,
    arbitrator_wait_averse: Option<bool>,
    arbitrator_reserved: i64,
}

impl Session {
    fn optimizer_cost_env(
        &self,
        mem_quota: i64,
        tmp_storage_on_oom: bool,
    ) -> tidb_planner::find_best_task::coster::CostEnv {
        // Everything below derives from the session's variable table, except
        // the two per-statement arguments, which are patched onto the cached
        // copy -- so a statement pays one stamp check and one clone instead
        // of thirty-odd string lookups and parses. Go's equivalents are
        // typed `SessionVars` fields maintained at `SET`.
        let generation = self.vars.generation();
        if let Some((cached_at, env)) = self.cost_env_cache.borrow().as_ref() {
            if *cached_at == generation {
                let mut env = env.clone();
                env.session.mem_quota = mem_quota;
                env.session.enable_tmp_storage_on_oom = tmp_storage_on_oom;
                return env;
            }
        }
        let number = |name: &str, default: f64| {
            self.vars
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(default)
        };
        let enabled = |name: &str, default: bool| {
            self.vars
                .get_system(name)
                .ok()
                .map(|value| value.eq_ignore_ascii_case("on") || value == "1")
                .unwrap_or(default)
        };
        let executor_concurrency = number(
            tidb_vardef::tidb_vars::TIDB_EXECUTOR_CONCURRENCY,
            tidb_vardef::defaults::DEF_EXECUTOR_CONCURRENCY as f64,
        );
        let resolved_concurrency = |name: &str| {
            let value = number(name, -1.0);
            if value > 0.0 {
                value
            } else {
                executor_concurrency
            }
        };

        let mut env = tidb_planner::find_best_task::coster::CostEnv::default();
        env.session.hash_join_concurrency = resolved_concurrency("tidb_hash_join_concurrency");
        env.session.distsql_scan_concurrency = number("tidb_distsql_scan_concurrency", 15.0);
        env.session.index_lookup_concurrency =
            resolved_concurrency("tidb_index_lookup_concurrency");
        env.session.index_lookup_join_concurrency =
            resolved_concurrency("tidb_index_lookup_join_concurrency");
        env.session.projection_concurrency = resolved_concurrency("tidb_projection_concurrency");
        env.session.hashagg_final_concurrency =
            resolved_concurrency("tidb_hashagg_final_concurrency");
        env.session.union_concurrency = executor_concurrency;
        env.session.index_lookup_size = number("tidb_index_lookup_size", 20_000.0);
        env.session.index_join_batch_size = number("tidb_index_join_batch_size", 25_000.0);
        env.session.index_join_double_read_penalty_cost_rate =
            number("tidb_index_join_double_read_penalty_cost_rate", 0.0);
        env.session.enable_tmp_storage_on_oom = tmp_storage_on_oom;
        env.session.mem_quota = mem_quota;
        env.session.enable_paging = enabled("tidb_enable_paging", true);
        env.session.mpp_enforced = enabled("tidb_enforce_mpp", false);

        env.cost_factors.index_scan = number("tidb_opt_index_scan_cost_factor", 1.0);
        env.cost_factors.table_row_id_scan = number("tidb_opt_table_rowid_scan_cost_factor", 1.0);
        env.cost_factors.table_range_scan = number("tidb_opt_table_range_scan_cost_factor", 1.0);
        env.cost_factors.table_full_scan = number("tidb_opt_table_full_scan_cost_factor", 1.0);
        env.cost_factors.table_tiflash_scan =
            number("tidb_opt_table_tiflash_scan_cost_factor", 1.0);
        env.cost_factors.index_reader = number("tidb_opt_index_reader_cost_factor", 1.0);
        env.cost_factors.table_reader = number("tidb_opt_table_reader_cost_factor", 1.0);
        env.cost_factors.index_lookup = number("tidb_opt_index_lookup_cost_factor", 1.0);
        env.cost_factors.index_merge = number("tidb_opt_index_merge_cost_factor", 1.0);
        env.cost_factors.limit = number("tidb_opt_limit_cost_factor", 1.0);
        env.cost_factors.sort = number("tidb_opt_sort_cost_factor", 1.0);
        env.cost_factors.topn = number("tidb_opt_topn_cost_factor", 1.0);
        env.cost_factors.stream_agg = number("tidb_opt_stream_agg_cost_factor", 1.0);
        env.cost_factors.hash_agg = number("tidb_opt_hash_agg_cost_factor", 1.0);
        env.cost_factors.merge_join = number("tidb_opt_merge_join_cost_factor", 1.0);
        env.cost_factors.hash_join = number("tidb_opt_hash_join_cost_factor", 1.0);
        env.cost_factors.index_join = number("tidb_opt_index_join_cost_factor", 1.0);

        *self.cost_env_cache.borrow_mut() = Some((generation, env.clone()));
        env
    }

    /// The expression context used by an immutable prepared PointGet plan.
    ///
    /// That cache admits only direct projections of stored columns. Row
    /// decoding can therefore consult only the session zone and SELECT's
    /// origin-default date flags; planner settings, sequences, user values,
    /// clocks, globals, and expression state cannot affect the result.
    pub(crate) fn prepared_point_get_context(
        &self,
    ) -> tidb_executor::kv_table::PreparedPointGetDecodeContext {
        tidb_executor::kv_table::PreparedPointGetDecodeContext::for_query(
            self.vars.sql_mode().has_allow_invalid_dates_mode(),
            self.session_time_zone(),
        )
    }

    /// Captures the policy a row result keeps after its statement finishes.
    ///
    /// Go retains the memory tracker and chunk bounds from the statement's
    /// existing context. This session currently materializes rows before the
    /// wire layer takes ownership, so refresh only the session-memory policy
    /// and create the result's statement handle here. Constructing a complete
    /// `StmtContext` would also snapshot planner, expression, sequence, and
    /// user state that result materialization never reads.
    pub fn result_materialization_authority(&self) -> crate::ResultMaterializationAuthority {
        if let Some(authority) = self.statement_result_authority.borrow().as_ref() {
            return authority.clone();
        }
        let snapshot = self.statement_var_snapshot();
        let (oom_action, tmp_storage_on_oom) = self.vars.statement_memory_policy();
        let authority = self.build_statement_result_authority(
            &snapshot,
            snapshot.mem_quota,
            oom_action,
            tmp_storage_on_oom,
        );
        self.statement_result_authority
            .replace(Some(authority.clone()));
        authority
    }

    fn build_statement_result_authority(
        &self,
        snapshot: &StatementVarSnapshot,
        mem_quota: i64,
        oom_action: tidb_executor::OomAction,
        tmp_storage_on_oom: bool,
    ) -> crate::ResultMaterializationAuthority {
        self.session_memory
            .configure(mem_quota, oom_action, tmp_storage_on_oom);
        let memory = self.session_memory.statement_with_arbitration(
            snapshot.arbitrator_wait_averse,
            snapshot.arbitrator_reserved,
        );
        crate::ResultMaterializationAuthority::new(
            memory,
            snapshot.init_chunk_size,
            snapshot.max_chunk_size,
        )
    }

    /// Go `timeutil.ParseTimeZone`: `SYSTEM` is the host zone, a named zone
    /// comes from the zone database, and a `+HH:MM`/`-HH:MM` string is a
    /// fixed offset bounded to `[-12:59, +14:00]`.
    ///
    /// Runtime `SET` validates the value before it reaches this resolver.
    /// An unparseable value can therefore only come from a foreign persisted
    /// global-variable row; in that case the host zone is the safe fallback.
    pub fn session_time_zone(&self) -> tidb_executor::SessionTimeZone {
        self.statement_var_snapshot().time_zone.clone()
    }

    fn resolve_session_time_zone(&self) -> tidb_executor::SessionTimeZone {
        use tidb_executor::SessionTimeZone;
        let written = self
            .vars
            .get_system("time_zone")
            .unwrap_or_else(|_| "SYSTEM".to_owned());
        if !written.eq_ignore_ascii_case("SYSTEM") {
            if let Ok(zone) = written.parse::<chrono_tz::Tz>() {
                return SessionTimeZone::Named(zone);
            }
            if let Some(rest) = written.strip_prefix(['+', '-']) {
                let negative = written.starts_with('-');
                let mut parts = rest.split(':');
                let hours: i32 = parts.next().unwrap_or_default().parse().unwrap_or(-1);
                let minutes: i32 = parts.next().unwrap_or("0").parse().unwrap_or(-1);
                if hours >= 0 && (0..60).contains(&minutes) {
                    let offset = hours * 3600 + minutes * 60;
                    let bounded = if negative {
                        offset <= 12 * 3600 + 59 * 60
                    } else {
                        offset <= 14 * 3600
                    };
                    if bounded {
                        return SessionTimeZone::Fixed {
                            name: written.clone(),
                            offset_secs: if negative { -offset } else { offset },
                        };
                    }
                }
            }
        }
        // SYSTEM is TiDB's process-wide `SystemLocation`, not an offset
        // snapshot. Preserve a resolved IANA zone (and therefore DST), with
        // the process-local zone as the same fallback Go uses.
        match tidb_util::timeutil::system_location() {
            tidb_util::timeutil::TimeZone::Local => SessionTimeZone::Local,
            tidb_util::timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(zone),
            tidb_util::timeutil::TimeZone::Fixed { name, offset_secs } => {
                SessionTimeZone::Fixed { name, offset_secs }
            }
        }
    }

    /// The instant every `NOW()` in one statement shares, which Go fixes on
    /// the statement context.
    ///
    /// Go `sessionexpr.getStmtTimestamp`: a `@@timestamp` left at its `0`
    /// default means the live clock, and any other value PINS the statement's
    /// whole time family (`NOW`, `CURDATE`, `UTC_TIMESTAMP`, ...) to that
    /// epoch instant. The split is `math.Modf` on a `float64`, kept here
    /// exactly: `SET timestamp = 1700000000.654321` really does land on
    /// 654320955ns, which is why the truncating readers report `.654320`
    /// while the rounding ones report `.654321`.
    pub(crate) fn statement_clock(&self, zone: &tidb_executor::SessionTimeZone) -> (i64, u32, i32) {
        use tidb_executor::SessionTimeZone;
        let pinned = self
            .vars
            .get_system("timestamp")
            .ok()
            .filter(|value| value != "0")
            .and_then(|value| value.parse::<f64>().ok());
        let utc = chrono::Utc::now();
        let (seconds, nanos) = match pinned {
            #[expect(clippy::cast_possible_truncation, reason = "Go's int64(seconds)")]
            #[expect(clippy::cast_sign_loss, reason = "@@timestamp's MinValue is 0")]
            Some(timestamp) => (
                timestamp.trunc() as i64,
                (timestamp.fract() * 1e9) as u32 % 1_000_000_000,
            ),
            None => (utc.timestamp(), utc.timestamp_subsec_nanos()),
        };
        let offset = match zone {
            SessionTimeZone::Local => {
                use chrono::TimeZone;
                let at = chrono::DateTime::from_timestamp(seconds, nanos)
                    .unwrap_or(utc)
                    .naive_utc();
                chrono::Offset::fix(&chrono::Local.offset_from_utc_datetime(&at)).local_minus_utc()
            }
            SessionTimeZone::Fixed { offset_secs, .. } => *offset_secs,
            SessionTimeZone::Named(zone) => {
                use chrono::TimeZone;
                // A named zone's offset is a property of the INSTANT (DST), so
                // it has to be taken at the statement's own instant -- the
                // pinned one when `@@timestamp` fixes the clock.
                let at = chrono::DateTime::from_timestamp(seconds, nanos)
                    .unwrap_or(utc)
                    .naive_utc();
                chrono::Offset::fix(&zone.offset_from_utc_datetime(&at)).local_minus_utc()
            }
        };
        (seconds, nanos, offset)
    }

    /// The evaluation context for one statement, which is Go's
    /// `StatementContext`.
    ///
    /// The division-by-zero level is the only group modelled so far: Go warns
    /// for a query, and for a DML statement resolves it from `sql_mode` --
    /// without `ERROR_FOR_DIVISION_BY_ZERO` the condition is ignored, a
    /// non-strict mode warns, and the default strict mode fails the statement.
    /// The sequences a statement of this session may read, over the catalog it
    /// sees (the transaction's working copy inside `BEGIN`).
    ///
    /// Only the NAMES are snapshotted: the allocators are `Arc` handles, so
    /// consuming a value through one moves the counter the catalog holds. That
    /// is deliberate and matches Go, where `NEXTVAL` allocates in its own meta
    /// transaction -- see `with_statement_stage`'s note about a storage whose
    /// clone shares a handle rather than copying by value.
    fn sequence_snapshot(&self) -> Arc<tidb_executor::SequenceSnapshot> {
        let by_name = match &self.txn {
            Some(txn) => txn.working.sequence_allocators(),
            None => match self.catalog.lock() {
                Ok(catalog) => catalog.sequence_allocators(),
                // A poisoned catalog is reported by the statement itself; an
                // empty map here just makes every name unknown.
                Err(_) => HashMap::new(),
            },
        };
        Arc::new(tidb_executor::SequenceSnapshot::new(
            by_name,
            &self.current_db,
            Arc::clone(&self.sequence_last_values),
        ))
    }

    fn tidb_decode_key_snapshot(&self) -> Arc<tidb_executor::TidbDecodeKeySnapshot> {
        let Ok(catalog) = self.catalog.lock() else {
            return Arc::default();
        };
        // Keyed on the METADATA counter, not the mutation counter: Go's
        // row-decode metadata is cached per infoschema version, which DDL
        // moves and DML never does. Keying on `version()` here would rebuild
        // this snapshot on every write statement.
        let version = catalog.metadata_version();
        {
            let cache = self
                .tidb_decode_key_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((cached_version, snapshot)) = cache.as_ref() {
                if *cached_version == version {
                    return Arc::clone(snapshot);
                }
            }
        }
        let snapshot = Arc::new(catalog.tidb_decode_key_snapshot());
        *self
            .tidb_decode_key_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((version, Arc::clone(&snapshot)));
        snapshot
    }

    /// The scanner-facing half of `@@sql_mode`: the input Go hands
    /// `Parser.SetSQLMode`, read fresh at every parse so a `SET sql_mode`
    /// changes the statements AFTER it and no AST built before it.
    ///
    /// Go reads the mode once per statement, in `session.ParseSQL`
    /// (`pkg/session/session.go`), because Go parses once and passes the AST
    /// down. This tier re-parses the raw text in the executor tiers, so the
    /// mode has to travel with the statement; it travels on
    /// [`tidb_executor::StmtContext`], which every executor entry already
    /// takes, rather than on ~30 separate parameters.
    pub(crate) fn scanner_sql_mode(&self) -> tidb_parser::SqlMode {
        scanner_sql_mode_of(self.vars.sql_mode())
    }

    /// Parses one statement of THIS session, under the `sql_mode` in force
    /// right now. Go's `session.ParseSQL` is the same single door; every
    /// session-tier parse goes through here so no call site decides on its own
    /// that a scanner flag does not apply to it.
    /// [`Self::parse`] for a front end outside this crate, so a caller that
    /// asks this session several parse-only questions about one statement can
    /// pay for the parse once and hand the tree to each. The `sql_mode` used
    /// is this session's, which is the whole point: a front end must not lex
    /// with a mode of its own.
    pub fn parse_statement(&self, sql: &str) -> Result<tidb_ast::Stmt, DriverError> {
        self.parse(sql)
    }

    pub(crate) fn parse(&self, sql: &str) -> Result<tidb_ast::Stmt, DriverError> {
        tidb_parser::parse_with_sql_mode(sql, self.scanner_sql_mode()).map_err(|e| match e.errno {
            Some(errno) => DriverError::ParseCoded {
                errno,
                message: e.message,
            },
            // Go's session layer wraps a positional parser error through
            // `util.SyntaxError`, whose message body is the parser's own
            // `line L column C near "..."` text — not a Debug dump.
            None => DriverError::Parse(e.compatibility_message(sql)),
        })
    }

    /// The statement context a session-aware DDL front end must carry through
    /// parsing, default admission, and catalog persistence.
    ///
    /// DDL uses the query-shaped context because it does not write table rows,
    /// while its default checks still consult the captured strict/date modes
    /// and session time zone.
    pub fn ddl_statement_context(&self) -> tidb_executor::StmtContext {
        self.statement_context(false)
    }

    /// Starts a statement executed by a server-owned route and returns the
    /// same statement memory and SQL killer ordinary execution receives.
    #[must_use]
    pub fn routed_statement_memory(&self) -> tidb_executor::StatementMemory {
        self.statement_context(false).statement_memory()
    }

    /// Resolves a `CREATE [OR REPLACE] VIEW` against this session's catalog
    /// for the cluster DDL route: `Ok(None)` when `sql` is not a CREATE
    /// VIEW; otherwise the `(database, name, or_replace, view)` the cluster
    /// tier publishes. This is Go's shape — `executeCreateView` preprocesses
    /// the body in the executor (failing a bad body at CREATE time) and
    /// hands DDL a finished definition.
    pub fn resolve_cluster_view(
        &mut self,
        sql: &str,
    ) -> Result<Option<(String, String, bool, tidb_executor::ViewDef)>, DriverError> {
        let stmt = self.parse(sql)?;
        let tidb_ast::Stmt::Ddl(ddl) = stmt else {
            return Ok(None);
        };
        let tidb_ast::DdlStmt::CreateView(create) = &*ddl else {
            return Ok(None);
        };
        let create = create.clone();
        let current_db = self.current_db.clone();
        let ctx = self.statement_context(false);
        let or_replace = create.or_replace;
        let (database, name, view) = self.with_catalog_mut(|catalog| {
            tidb_executor::resolve_view_definition(&create, catalog, &current_db, &ctx)
        })?;
        Ok(Some((database, name, or_replace, view)))
    }

    pub(crate) fn statement_context(&self, is_dml: bool) -> tidb_executor::StmtContext {
        self.statement_context_ignoring(is_dml, false)
    }

    fn latest_index_schema_snapshot(
        &self,
    ) -> Option<Arc<tidb_planner::domain_misc::LatestIndexSchema>> {
        let mut schema = self.catalog.lock().ok()?.latest_index_schema();
        for (_, _, table) in &self.local_temporary_tables {
            schema.table_indexes.insert(
                table.table_id,
                table
                    .indexes()
                    .iter()
                    .map(|index| tidb_planner::plan_builder::catalog::SourceIndex {
                        id: index.id,
                        is_public: true,
                        ..Default::default()
                    })
                    .collect(),
            );
        }
        Some(Arc::new(schema))
    }

    pub(crate) fn statement_context_for_update_read(
        &self,
        ignore_err: bool,
    ) -> tidb_executor::StmtContext {
        let mut ctx = self.statement_context_ignoring(true, ignore_err);
        if self.connection_id.is_some_and(|id| id > 0) && ctx.latest_index_schema().is_none() {
            if let Some(latest_index_schema) = self.latest_index_schema_snapshot() {
                ctx = ctx.with_latest_index_schema(latest_index_schema);
            }
        }
        ctx
    }

    /// [`Self::statement_context`] for a DML statement that carries the
    /// `IGNORE` modifier, which Go's `ResetContextOfStmt` reads off the AST
    /// and folds into every value-level error level.
    /// The cached [`StatementVarSnapshot`], re-derived only when a `SET`
    /// moved the variable table; see the struct's own doc for why the
    /// GLOBAL-scope reads are NOT in it.
    fn statement_var_snapshot(&self) -> Arc<StatementVarSnapshot> {
        let generation = self.vars.generation();
        if let Some(cached) = self.statement_var_cache.borrow().as_ref() {
            if cached.generation == generation {
                return Arc::clone(cached);
            }
        }
        let sql_mode = self.vars.sql_mode();
        let on = |name: &str| {
            self.vars
                .get_system(name)
                .is_ok_and(|value| value.eq_ignore_ascii_case("on") || value == "1")
        };
        let not_off = |name: &str| {
            !matches!(
                self.vars.get_system(name).as_deref(),
                Ok("OFF" | "off" | "0")
            )
        };
        let executor_concurrency = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_EXECUTOR_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(tidb_vardef::defaults::DEF_EXECUTOR_CONCURRENCY as usize);
        let resolved_concurrency = |name: &str| {
            self.vars
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|value| *value > 0)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(executor_concurrency)
        };
        let index_lookup_push_down_policy = match self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_INDEX_LOOK_UP_PUSH_DOWN_POLICY)
            .as_deref()
        {
            Ok("force") => tidb_planner::access_path::IndexLookupPushDownPolicy::Force,
            Ok("affinity-force") => {
                tidb_planner::access_path::IndexLookupPushDownPolicy::AffinityForce
            }
            _ => tidb_planner::access_path::IndexLookupPushDownPolicy::HintOnly,
        };
        let read_staleness = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_READ_STALENESS)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .is_some_and(|value| value != 0);
        let index_lookup_push_down_session =
            tidb_planner::access_path::IndexLookupPushDownSession {
                repeatable_read: self
                    .vars
                    .get_system("transaction_isolation")
                    .is_ok_and(|value| value.eq_ignore_ascii_case("REPEATABLE-READ")),
                leader_read: self
                    .vars
                    .get_system(tidb_vardef::tidb_vars::TIDB_REPLICA_READ)
                    .is_ok_and(|value| value.eq_ignore_ascii_case("leader")),
                staleness: read_staleness,
                historical_read: self
                    .vars
                    .get_system(tidb_vardef::tidb_vars::TIDB_SNAPSHOT)
                    .is_ok_and(|value| !value.is_empty()),
                max_keys_read: self
                    .vars
                    .get_system("tidb_max_keys_read")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
                policy: index_lookup_push_down_policy,
            };
        let snapshot = Arc::new(StatementVarSnapshot {
            generation,
            version: self.vars.get_system("version").ok(),
            time_zone: self.resolve_session_time_zone(),
            connection_charset: self
                .vars
                .get_system("character_set_connection")
                .unwrap_or_else(|_| "utf8mb4".to_owned()),
            connection_collation: self
                .vars
                .get_system("collation_connection")
                .unwrap_or_else(|_| "utf8mb4_bin".to_owned()),
            allow_write_row_id: on(tidb_vardef::tidb_vars::TIDB_OPT_WRITE_ROW_ID),
            sysdate_is_now: on(tidb_vardef::tidb_vars::TIDB_SYSDATE_IS_NOW),
            timestamp: self
                .vars
                .get_system("timestamp")
                .ok()
                .filter(|value| value != "0")
                .and_then(|value| value.parse::<f64>().ok()),
            allow_auto_random_explicit_insert: on(
                tidb_vardef::tidb_vars::TIDB_ALLOW_AUTO_RAND_EXPLICIT_INSERT,
            ),
            shard_allocate_step: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SHARD_ALLOCATE_STEP)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(i64::MAX as u64),
            like_default_escape: if sql_mode.has_no_backslash_escapes_mode()
                && not_off(tidb_vardef::tidb_vars::TIDB_ENABLE_NO_BACKSLASH_ESCAPES_IN_LIKE)
            {
                0
            } else {
                b'\\'
            },
            week_format: self
                .vars
                .get_system("default_week_format")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0),
            div_scale: self
                .vars
                .get_system("div_precision_increment")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(4),
            cte_depth: self
                .vars
                .get_system("cte_max_recursion_depth")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(1000),
            join_reorder_threshold: self
                .vars
                .get_system("tidb_opt_join_reorder_threshold")
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_OPT_JOIN_REORDER_THRESHOLD as i32),
            default_string_match_selectivity: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_DEFAULT_STR_MATCH_SELECTIVITY)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.0),
            enable_pseudo_for_outdated_stats: on("tidb_enable_pseudo_for_outdated_stats"),
            stats_load_sync_wait_ms: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_STATS_LOAD_SYNC_WAIT)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_STATS_LOAD_SYNC_WAIT as u64),
            stats_load_pseudo_timeout: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_STATS_LOAD_PSEUDO_TIMEOUT)
                .map_or(true, |value| {
                    !value.eq_ignore_ascii_case("OFF") && value != "0"
                }),
            plan_replayer_capture_enabled: on("tidb_enable_plan_replayer_capture")
                || on("tidb_enable_plan_replayer_continuous_capture"),
            opt_index_prune_threshold: self
                .vars
                .get_system("tidb_opt_index_prune_threshold")
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(20),
            opt_prefix_index_single_scan: on(
                tidb_vardef::tidb_vars::TIDB_OPT_PREFIX_INDEX_SINGLE_SCAN,
            ),
            always_keep_join_key: on(tidb_vardef::tidb_vars::TIDB_OPT_ALWAYS_KEEP_JOIN_KEY),
            enable_unsafe_substitute: on(tidb_vardef::tidb_vars::TIDB_ENABLE_UNSAFE_SUBSTITUTE),
            enable_semi_join_rewrite: on(tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE),
            allow_in_subq_to_join_and_agg: on(
                tidb_vardef::tidb_vars::TIDB_OPT_IN_SUBQ_TO_JOIN_AND_AGG,
            ),
            enable_no_decorrelate_in_select: on(
                tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT,
            ),
            enable_skew_distinct_agg: on(tidb_vardef::tidb_vars::TIDB_OPT_SKEW_DISTINCT_AGG),
            max_execution_time_ms: self
                .vars
                .get_system("max_execution_time")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
            advanced_join_reorder: not_off(
                tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_ADVANCED_JOIN_REORDER,
            ),
            constraint_check_in_place: on(tidb_vardef::tidb_vars::TIDB_CONSTRAINT_CHECK_IN_PLACE),
            ordering_index_selectivity_ratio: self
                .vars
                .get_system("tidb_opt_ordering_index_selectivity_ratio")
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.01),
            allow_projection_push_down: on("tidb_opt_projection_push_down"),
            limit_push_down_threshold: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_OPT_LIMIT_PUSH_DOWN_THRESHOLD)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_OPT_LIMIT_PUSH_DOWN_THRESHOLD as u64),
            index_lookup_push_down_session,
            join_reorder_through_proj: on(
                tidb_vardef::tidb_vars::TIDB_OPT_JOIN_REORDER_THROUGH_PROJ,
            ),
            join_reorder_through_sel: on(tidb_vardef::tidb_vars::TIDB_OPT_JOIN_REORDER_THROUGH_SEL),
            outer_join_reorder: not_off(
                tidb_vardef::tidb_vars::TIDB_OPTIMIZER_ENABLE_OUTER_JOIN_REORDER,
            ),
            index_merge: not_off("tidb_enable_index_merge"),
            static_partition_prune: self
                .vars
                .get_system("tidb_partition_prune_mode")
                .is_ok_and(|value| value.eq_ignore_ascii_case("static")),
            new_only_full_group_by_check: on(
                tidb_vardef::tidb_vars::TIDB_OPTIMIZER_ENABLE_NEW_ONLY_FULL_GROUP_BY_CHECK,
            ),
            mem_quota: self
                .vars
                .get_system("tidb_mem_quota_query")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(tidb_util::memory::DEF_MEM_QUOTA_QUERY),
            replica_read: match self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_REPLICA_READ)
                .unwrap_or_default()
                .as_str()
            {
                "follower" => tidb_executor::ReplicaReadType::Follower,
                "leader-and-follower" => tidb_executor::ReplicaReadType::Mixed,
                "closest-replicas" => tidb_executor::ReplicaReadType::Closest,
                "closest-adaptive" => tidb_executor::ReplicaReadType::ClosestAdaptive,
                "learner" => tidb_executor::ReplicaReadType::Learner,
                "prefer-leader" => tidb_executor::ReplicaReadType::PreferLeader,
                _ => tidb_executor::ReplicaReadType::Leader,
            },
            isolation_read_engines: self
                .vars
                .get_system("tidb_isolation_read_engines")
                .unwrap_or_else(|_| "tikv,tiflash,tidb".to_owned()),
            init_chunk_size: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_INIT_CHUNK_SIZE)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_INIT_CHUNK_SIZE as usize),
            max_chunk_size: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_MAX_CHUNK_SIZE)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_MAX_CHUNK_SIZE as usize),
            max_allowed_packet: self.vars.max_allowed_packet(),
            group_concat_max_len: self
                .vars
                .get_system("group_concat_max_len")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1024),
            apply_cache_capacity: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_APPLY_CACHE)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE),
            hashagg_partial_concurrency: resolved_concurrency("tidb_hashagg_partial_concurrency"),
            hashagg_final_concurrency: resolved_concurrency("tidb_hashagg_final_concurrency"),
            block_encryption_mode: self
                .vars
                .get_system("block_encryption_mode")
                .ok()
                .and_then(|value| tidb_executor::BlockEncryptionMode::parse(&value))
                .unwrap_or_default(),
            ddl_cdc_write_source: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_CDC_WRITE_SOURCE)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
            ddl_reorg_priority: match self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_DDL_REORG_PRIORITY)
                .as_deref()
            {
                Ok("PRIORITY_NORMAL") => 0,
                Ok("PRIORITY_HIGH") => 2,
                _ => 1,
            },
            ddl_session_alias: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SESSION_ALIAS)
                .unwrap_or_default(),
            arbitrator_wait_averse: match self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_WAIT_AVERSE)
                .unwrap_or_default()
                .as_str()
            {
                "nolimit" => None,
                "1" => Some(true),
                _ => Some(false),
            },
            arbitrator_reserved: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_QUERY_RESERVED)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default(),
            sql_mode,
            scanner_sql_mode: scanner_sql_mode_of(sql_mode),
        });
        *self.statement_var_cache.borrow_mut() = Some(Arc::clone(&snapshot));
        snapshot
    }

    pub(crate) fn statement_context_ignoring(
        &self,
        is_dml: bool,
        ignore_err: bool,
    ) -> tidb_executor::StmtContext {
        // Go hands the same `SessionVars` to every expression, which is where
        // `DATABASE()` and `VERSION()` read from.
        let current_db = if self.current_db.is_empty() {
            None
        } else {
            Some(self.current_db.clone())
        };
        // The session-variable half, parsed once per variable-table
        // generation; see [`StatementVarSnapshot`].
        let snapshot = self.statement_var_snapshot();
        let version = snapshot.version.clone();
        let connection_charset = snapshot.connection_charset.clone();
        let connection_collation = snapshot.connection_collation.clone();
        let zone = snapshot.time_zone.clone();
        let allow_write_row_id = snapshot.allow_write_row_id;
        let sysdate_is_now = snapshot.sysdate_is_now;
        let sql_mode = snapshot.sql_mode;
        let allow_auto_random_explicit_insert = snapshot.allow_auto_random_explicit_insert;
        let shard_allocate_step = snapshot.shard_allocate_step;
        let like_default_escape = snapshot.like_default_escape;
        let week_format = snapshot.week_format;
        let div_scale = snapshot.div_scale;
        let cte_depth = snapshot.cte_depth;
        let join_reorder_threshold = snapshot.join_reorder_threshold;
        let default_string_match_selectivity = snapshot.default_string_match_selectivity;
        let enable_pseudo_for_outdated_stats = snapshot.enable_pseudo_for_outdated_stats;
        let stats_load_sync_wait_ms = snapshot.stats_load_sync_wait_ms;
        let stats_load_pseudo_timeout = snapshot.stats_load_pseudo_timeout;
        let plan_replayer_capture_enabled = snapshot.plan_replayer_capture_enabled;
        let opt_index_prune_threshold = snapshot.opt_index_prune_threshold;
        let opt_prefix_index_single_scan = snapshot.opt_prefix_index_single_scan;
        let always_keep_join_key = snapshot.always_keep_join_key;
        let enable_unsafe_substitute = snapshot.enable_unsafe_substitute;
        let enable_semi_join_rewrite = snapshot.enable_semi_join_rewrite;
        let allow_in_subq_to_join_and_agg =
            if self.stmt_hints.has_allow_in_subq_to_join_and_agg_hint {
                self.stmt_hints.allow_in_subq_to_join_and_agg
            } else {
                snapshot.allow_in_subq_to_join_and_agg
            };
        let enable_no_decorrelate_in_select = snapshot.enable_no_decorrelate_in_select;
        let enable_skew_distinct_agg = snapshot.enable_skew_distinct_agg;
        let max_execution_time_ms = if self.stmt_hints.has_max_execution_time {
            self.stmt_hints.max_execution_time
        } else {
            snapshot.max_execution_time_ms
        };
        let advanced_join_reorder = snapshot.advanced_join_reorder;
        let constraint_check_in_place = snapshot.constraint_check_in_place;
        let ordering_index_selectivity_ratio = snapshot.ordering_index_selectivity_ratio;
        let allow_projection_push_down = snapshot.allow_projection_push_down;
        let limit_push_down_threshold = snapshot.limit_push_down_threshold;
        let mut index_lookup_push_down_session = snapshot.index_lookup_push_down_session;
        // Transaction state is not part of the variable-table generation
        // that keys `StatementVarSnapshot`, so read it at every statement
        // boundary like Go reads `TxnCtx.IsStaleness`.
        index_lookup_push_down_session.staleness |= self
            .txn
            .as_ref()
            .is_some_and(|transaction| transaction.is_stale_read());
        let latest_index_schema = (!index_lookup_push_down_session.repeatable_read
            && self.connection_id.is_some_and(|id| id > 0))
        .then(|| self.latest_index_schema_snapshot())
        .flatten();
        let join_reorder_through_proj = snapshot.join_reorder_through_proj;
        let join_reorder_through_sel = snapshot.join_reorder_through_sel;
        let outer_join_reorder = snapshot.outer_join_reorder;
        let index_merge = snapshot.index_merge && !self.stmt_hints.no_index_merge_hint;
        let static_partition_prune = snapshot.static_partition_prune;
        let new_only_full_group_by_check = snapshot.new_only_full_group_by_check;
        let mem_quota = if self.stmt_hints.has_mem_quota_hint {
            self.stmt_hints.mem_quota_query
        } else {
            snapshot.mem_quota
        };
        let replica_read = if self.stmt_hints.has_replica_read_hint {
            tidb_executor::ReplicaReadType::from_raw(self.stmt_hints.replica_read)
        } else {
            snapshot.replica_read
        };
        let isolation_read_engines = snapshot.isolation_read_engines.clone();
        let max_allowed_packet = snapshot.max_allowed_packet;
        let group_concat_max_len = snapshot.group_concat_max_len;
        let apply_cache_capacity = snapshot.apply_cache_capacity;
        let hashagg_partial_concurrency = snapshot.hashagg_partial_concurrency;
        let hashagg_final_concurrency = snapshot.hashagg_final_concurrency;
        let block_encryption_mode = snapshot.block_encryption_mode;
        let global_sysvar_accessor = self.vars.global_sysvar_accessor();
        let (oom_action, tmp_storage_on_oom) = self.vars.statement_memory_policy();
        let optimizer_cost_env = self.optimizer_cost_env(mem_quota, tmp_storage_on_oom);
        let result_authority = self.build_statement_result_authority(
            &snapshot,
            mem_quota,
            oom_action,
            tmp_storage_on_oom,
        );
        let statement_memory = result_authority.statement_memory();
        let index_usage_collector = self.session_index_usage_collector.as_ref().map(|session| {
            Arc::new(
                tidb_stats_handle_usage_indexusage::StmtIndexUsageCollector::new(Arc::clone(
                    session,
                )),
            )
        });
        self.statement_result_authority
            .replace(Some(result_authority));
        // The SAME three bits on both branches: a query reads them for
        // `CAST(... AS DATE/DATETIME)`, a DML statement reads them for the
        // column write. They used to be attached only below, which left every
        // read with the all-false default -- and made `NO_ZERO_DATE` silently
        // inoperative on the read path.
        let date_modes = tidb_datatype::DateModes {
            no_zero_date: sql_mode.has_no_zero_date_mode(),
            no_zero_in_date: sql_mode.has_no_zero_in_date_mode(),
            allow_invalid_dates: sql_mode.has_allow_invalid_dates_mode(),
        };
        if !is_dml {
            let mut ctx = tidb_executor::StmtContext::for_query()
                // A read's error levels do not depend on the mode, but DDL
                // takes this same context and Go's DDL checks DO read
                // `SQLMode.HasStrictMode()`. See `StmtContext::with_strict`.
                .with_strict(sql_mode.has_strict_mode())
                .with_date_modes(date_modes)
                .with_cte_max_recursion_depth(cte_depth)
                .with_join_reorder_threshold(join_reorder_threshold)
                .with_advanced_join_reorder(advanced_join_reorder)
                .with_ordering_index_selectivity_ratio(ordering_index_selectivity_ratio)
                .with_projection_push_down(allow_projection_push_down)
                .with_limit_push_down_threshold(limit_push_down_threshold)
                .with_index_lookup_push_down_session(index_lookup_push_down_session)
                .with_optimizer_fix_control(self.vars.optimizer_fix_control().clone())
                .with_optimizer_cost_env(optimizer_cost_env.clone())
                .with_hashagg_concurrency(hashagg_partial_concurrency, hashagg_final_concurrency)
                .with_join_reorder_through_proj(join_reorder_through_proj)
                .with_join_reorder_through_sel(join_reorder_through_sel)
                .with_outer_join_reorder(outer_join_reorder)
                .with_index_merge(index_merge)
                .with_pushdown_blacklists(self.pushdown_blacklists.snapshot())
                .with_planned_apply_channel(Arc::clone(&self.planned_apply))
                .with_process_plan_info_sink(Arc::clone(&self.process_plan_info))
                .with_allow_write_row_id(allow_write_row_id)
                .with_static_partition_prune(static_partition_prune)
                .with_only_full_group_by(sql_mode.has_only_full_group_by())
                .with_new_only_full_group_by_check(new_only_full_group_by_check)
                .with_session_state(current_db, version)
                .with_isolation_read_engines(isolation_read_engines)
                .with_connection_charset_info(
                    connection_charset.clone(),
                    connection_collation.clone(),
                )
                .with_user(self.current_user.clone(), self.login_user.clone())
                .with_global_sysvar_accessor(Arc::clone(&global_sysvar_accessor))
                .with_active_roles(
                    self.current_user
                        .as_ref()
                        .map(|_| Arc::clone(&self.active_roles)),
                )
                .with_connection_id(self.connection_id)
                .with_advisory_locks(self.advisory_locks.clone())
                .with_statement_memory(statement_memory.clone())
                .with_rand_session(Arc::clone(&self.rand))
                .with_last_insert_id_channel(Arc::clone(&self.published_last_insert_id))
                .with_retry_auto_ids(Arc::clone(&self.retry_auto_ids))
                .with_row_id_shards(Arc::clone(&self.row_id_shards))
                .with_auto_random_policy(allow_auto_random_explicit_insert, shard_allocate_step)
                .with_user_vars(Arc::clone(&self.user_vars))
                .with_previous_statement(self.last_insert_id, self.prev_row_count)
                .with_last_found_rows(self.last_found_rows)
                .with_current_tso(self.current_tso())
                .with_week_and_division_scale(week_format, div_scale)
                .with_max_allowed_packet(max_allowed_packet)
                .with_group_concat_max_len(group_concat_max_len)
                .with_apply_cache_capacity(apply_cache_capacity)
                .with_block_encryption_mode(block_encryption_mode)
                .with_sequences(self.sequence_snapshot())
                .with_tidb_decode_key_snapshot(self.tidb_decode_key_snapshot())
                .with_sql_mode(snapshot.scanner_sql_mode)
                .with_ddl_sql_mode(sql_mode.0)
                .with_ddl_job_context(
                    snapshot.ddl_cdc_write_source,
                    snapshot.ddl_reorg_priority,
                    snapshot.ddl_session_alias.clone(),
                    Vec::new(),
                )
                .with_no_unsigned_subtraction(sql_mode.has_no_unsigned_subtraction_mode())
                .with_like_default_escape(like_default_escape)
                .with_default_string_match_selectivity(default_string_match_selectivity)
                .with_pseudo_for_outdated_stats(enable_pseudo_for_outdated_stats)
                .with_stats_load_policy(
                    stats_load_sync_wait_ms,
                    stats_load_pseudo_timeout,
                    max_execution_time_ms,
                )
                .with_plan_replayer_capture(plan_replayer_capture_enabled)
                .with_column_stats_usage(self.stats_collector.clone())
                .with_index_usage_collector(index_usage_collector)
                .with_table_delta(std::sync::Arc::clone(&self.transaction_table_delta))
                .with_opt_index_prune_threshold(opt_index_prune_threshold)
                .with_opt_prefix_index_single_scan(opt_prefix_index_single_scan)
                .with_always_keep_join_key(always_keep_join_key)
                .with_enable_unsafe_substitute(enable_unsafe_substitute)
                .with_enable_semi_join_rewrite(enable_semi_join_rewrite)
                .with_allow_in_subq_to_join_and_agg(allow_in_subq_to_join_and_agg)
                .with_enable_no_decorrelate_in_select(enable_no_decorrelate_in_select)
                .with_enable_skew_distinct_agg(enable_skew_distinct_agg)
                .with_enable_check_constraint(self.enable_check_constraint())
                .with_sysdate_is_now(sysdate_is_now)
                .with_resource_group_name(self.active_resource_group.clone())
                .with_replica_read(replica_read)
                .with_executor_first_run_breakpoint(
                    Arc::clone(&self.executor_first_run_breakpoint),
                    self.breakpoint_notify_func(),
                )
                .with_lazy_clock(snapshot.timestamp, zone);
            if let Some(latest_index_schema) = latest_index_schema {
                ctx = ctx.with_latest_index_schema(latest_index_schema);
            }
            return ctx;
        }
        let (increment, offset) = self.auto_increment_step();
        let mut ctx = tidb_executor::StmtContext::for_dml(
            sql_mode.has_error_for_division_by_zero_mode(),
            sql_mode.has_strict_mode(),
            ignore_err,
        )
        .with_date_modes(date_modes)
        .with_planned_apply_channel(Arc::clone(&self.planned_apply))
        .with_process_plan_info_sink(Arc::clone(&self.process_plan_info))
        .with_allow_write_row_id(allow_write_row_id)
        .with_only_full_group_by(sql_mode.has_only_full_group_by())
        .with_new_only_full_group_by_check(new_only_full_group_by_check)
        .with_session_state(current_db, version)
        .with_isolation_read_engines(isolation_read_engines)
        .with_connection_charset_info(connection_charset, connection_collation)
        .with_user(self.current_user.clone(), self.login_user.clone())
        .with_global_sysvar_accessor(global_sysvar_accessor)
        .with_active_roles(
            self.current_user
                .as_ref()
                .map(|_| Arc::clone(&self.active_roles)),
        )
        .with_connection_id(self.connection_id)
        .with_advisory_locks(self.advisory_locks.clone())
        .with_statement_memory(statement_memory)
        .with_rand_session(Arc::clone(&self.rand))
        .with_last_insert_id_channel(Arc::clone(&self.published_last_insert_id))
        .with_retry_auto_ids(Arc::clone(&self.retry_auto_ids))
        .with_row_id_shards(Arc::clone(&self.row_id_shards))
        .with_auto_random_policy(allow_auto_random_explicit_insert, shard_allocate_step)
        .with_user_vars(Arc::clone(&self.user_vars))
        .with_previous_statement(self.last_insert_id, self.prev_row_count)
        .with_last_found_rows(self.last_found_rows)
        .with_current_tso(self.current_tso())
        .with_week_and_division_scale(week_format, div_scale)
        .with_max_allowed_packet(max_allowed_packet)
        .with_group_concat_max_len(group_concat_max_len)
        .with_apply_cache_capacity(apply_cache_capacity)
        .with_block_encryption_mode(block_encryption_mode)
        .with_sequences(self.sequence_snapshot())
        .with_tidb_decode_key_snapshot(self.tidb_decode_key_snapshot())
        .with_sysdate_is_now(sysdate_is_now)
        .with_resource_group_name(self.active_resource_group.clone())
        .with_replica_read(tidb_executor::ReplicaReadType::Leader)
        .with_executor_first_run_breakpoint(
            Arc::clone(&self.executor_first_run_breakpoint),
            self.breakpoint_notify_func(),
        )
        .with_lazy_clock(snapshot.timestamp, zone)
        .with_sql_mode(snapshot.scanner_sql_mode)
        .with_ddl_sql_mode(sql_mode.0)
        .with_ddl_job_context(
            snapshot.ddl_cdc_write_source,
            snapshot.ddl_reorg_priority,
            snapshot.ddl_session_alias.clone(),
            Vec::new(),
        )
        .with_no_unsigned_subtraction(sql_mode.has_no_unsigned_subtraction_mode())
        .with_like_default_escape(like_default_escape)
        .with_default_string_match_selectivity(default_string_match_selectivity)
        .with_pseudo_for_outdated_stats(enable_pseudo_for_outdated_stats)
        .with_stats_load_policy(
            stats_load_sync_wait_ms,
            stats_load_pseudo_timeout,
            max_execution_time_ms,
        )
        .with_plan_replayer_capture(plan_replayer_capture_enabled)
        .with_column_stats_usage(self.stats_collector.clone())
        .with_index_usage_collector(index_usage_collector)
        .with_table_delta(std::sync::Arc::clone(&self.transaction_table_delta))
        .with_opt_index_prune_threshold(opt_index_prune_threshold)
        .with_opt_prefix_index_single_scan(opt_prefix_index_single_scan)
        .with_always_keep_join_key(always_keep_join_key)
        .with_enable_unsafe_substitute(enable_unsafe_substitute)
        .with_enable_semi_join_rewrite(enable_semi_join_rewrite)
        .with_allow_in_subq_to_join_and_agg(allow_in_subq_to_join_and_agg)
        .with_enable_no_decorrelate_in_select(enable_no_decorrelate_in_select)
        .with_enable_skew_distinct_agg(enable_skew_distinct_agg)
        .with_auto_increment_step(increment, offset)
        .with_auto_increment_zero_explicit(sql_mode.has_no_auto_value_on_zero_mode())
        .with_foreign_key_checks(self.foreign_key_checks())
        .with_enable_check_constraint(self.enable_check_constraint())
        .with_constraint_check_in_place(constraint_check_in_place)
        // Go `optimizeDupKeyCheckForNormalInsert` + `getPessimisticLazyCheckMode`
        // (`pkg/executor/insert.go:331-337,347-350`): normal INSERT uses
        // `DupKeyCheckLazy` whenever constraint checks are disabled OR the
        // statement transaction is pessimistic. The Go auto-commit path still
        // opens a pessimistic transaction under the default `tidb_txn_mode`,
        // so the Rust context must carry that mode before `Txn()` is opened.
        .with_pessimistic_lazy_dup_check(
            self.statement_txn_mode().is_pessimistic()
                && self.connection_id.is_some_and(|id| id > 0),
        )
        .with_allow_remove_auto_inc(self.allow_remove_auto_inc())
        .with_cte_max_recursion_depth(cte_depth)
        .with_join_reorder_threshold(join_reorder_threshold)
        .with_advanced_join_reorder(advanced_join_reorder)
        .with_ordering_index_selectivity_ratio(ordering_index_selectivity_ratio)
        .with_projection_push_down(allow_projection_push_down)
        .with_limit_push_down_threshold(limit_push_down_threshold)
        .with_index_lookup_push_down_session(index_lookup_push_down_session)
        .with_optimizer_fix_control(self.vars.optimizer_fix_control().clone())
        .with_optimizer_cost_env(optimizer_cost_env)
        .with_hashagg_concurrency(hashagg_partial_concurrency, hashagg_final_concurrency)
        .with_join_reorder_through_proj(join_reorder_through_proj)
        .with_join_reorder_through_sel(join_reorder_through_sel)
        .with_outer_join_reorder(outer_join_reorder)
        .with_index_merge(index_merge)
        .with_static_partition_prune(static_partition_prune);
        if let Some(latest_index_schema) = latest_index_schema {
            ctx = ctx.with_latest_index_schema(latest_index_schema);
        }
        ctx
    }

    /// Go `SessionVars.ForeignKeyChecks`, read off `@@foreign_key_checks`.
    /// The registry stores a boolean as `ON`/`OFF`, and an unreadable value
    /// falls back to the ON default rather than silently disabling the
    /// checks.
    pub(crate) fn foreign_key_checks(&self) -> bool {
        !matches!(
            self.vars.get_system("foreign_key_checks").as_deref(),
            Ok("OFF") | Ok("off") | Ok("0")
        )
    }

    /// Go `SessionVars.AllowRemoveAutoInc`, read off
    /// `@@tidb_allow_remove_auto_inc`. The default is OFF, and unlike
    /// `foreign_key_checks` the safe fallback for an unreadable value is OFF:
    /// dropping AUTO_INCREMENT is the destructive direction.
    pub(crate) fn allow_remove_auto_inc(&self) -> bool {
        matches!(
            self.vars
                .get_system("tidb_allow_remove_auto_inc")
                .as_deref(),
            Ok("ON") | Ok("on") | Ok("1")
        )
    }

    /// Go `vardef.EnableCheckConstraint`, which is a process-wide atomic that
    /// `SetGlobal` writes: the variable is GLOBAL-scope only, so the value a
    /// statement sees is the global one, not a session copy. The registry
    /// defaults it to OFF, and unlike `foreign_key_checks` the safe fallback
    /// for an unreadable value is OFF -- that is what a stock TiDB does.
    pub(crate) fn enable_check_constraint(&self) -> bool {
        matches!(
            self.vars
                .get_global("tidb_enable_check_constraint")
                .as_deref(),
            Ok("ON") | Ok("on") | Ok("1")
        )
    }

    /// Go `SessionVars.EnableClusteredIndex`, fed to `BuildTableInfo` through
    /// `metabuild.WithClusteredIndexDefMode` (`pkg/ddl/metabuild.go`).
    ///
    /// The variable is `SESSION | GLOBAL` and an ENUM of `OFF`/`ON`/`INT_ONLY`
    /// -- not a boolean -- so it is read with the session's own value and
    /// converted by Go's own `TiDBOptEnableClustered`, which maps anything
    /// that is neither `ON` nor `OFF` (including an unreadable value) onto
    /// `INT_ONLY`. The registered default is `ON`.
    pub(crate) fn clustered_index_mode(&self) -> tidb_vardef::modes::ClusteredIndexDefMode {
        // `check_enum` stores the canonical `OFF`/`ON`/`INT_ONLY` spelling
        // whatever the user typed, so this compares against it exactly as Go
        // does rather than re-normalizing here.
        match self.vars.get_system("tidb_enable_clustered_index") {
            Ok(value) => tidb_vardef::modes::tidb_opt_enable_clustered(&value),
            Err(_) => tidb_vardef::modes::ClusteredIndexDefMode(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_CLUSTERED_INDEX,
            ),
        }
    }

    /// Go `SessionVars.AutoIncrementIncrement` / `AutoIncrementOffset`, which
    /// put an allocated id on the `offset + k * increment` progression.
    ///
    /// Both are `TypeUnsigned` sysvars validated into `[1, 65535]`, so an
    /// unreadable or out-of-range value falls back to the default of 1 --
    /// never to 0, which would divide by zero in the seek.
    pub(crate) fn auto_increment_step(&self) -> (u64, u64) {
        let read = |name: &str| {
            self.vars
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| (1..=65535).contains(value))
                .unwrap_or(1)
        };
        (
            read("auto_increment_increment"),
            read("auto_increment_offset"),
        )
    }

    /// Go `ResetContextOfStmt`'s `Prev*` promotion, run at the statement
    /// boundary: what the statement just published becomes what the next one
    /// reads.
    ///
    /// This is the ONE place either value moves. `LAST_INSERT_ID()`,
    /// `@@last_insert_id`, `@@identity` and `ROW_COUNT()` all read the fields
    /// it writes, and the OK packet reads
    /// [`Session::statement_insert_id`]'s own fallback off the same
    /// publication -- so the function and the wire can differ only where Go
    /// itself makes them differ.
    pub(crate) fn publish_statement_status(&mut self, result: &Result<StmtOutput, DriverError>) {
        // The publication outlives a failing statement, exactly as Go's
        // `StmtCtx.LastInsertID` does: `SELECT LAST_INSERT_ID(17), bad()`
        // fails and still moves the id (captured).
        if let Some(published) = (*self
            .published_last_insert_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
        {
            self.last_insert_id = published;
        }
        if let Ok(StmtOutput::Rows { rows, .. }) = result {
            self.last_found_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        }
        self.prev_row_count = match self.statement_kind {
            StatementKind::Select => -1,
            // Go reads `StmtCtx.AffectedRows()`, which a failed statement
            // leaves at whatever it managed to apply -- 0 for a statement
            // that never reached a row.
            StatementKind::Dml => match result {
                Ok(StmtOutput::Affected(rows)) => i64::try_from(*rows).unwrap_or(i64::MAX),
                _ => 0,
            },
            StatementKind::Other => 0,
        };
    }
}

/// The scanner flags Go's `Parser.SetSQLMode` consults, projected from the
/// same typed `SessionVars.SQLMode` authority every other consumer reads.
pub(crate) const fn scanner_sql_mode_of(mode: tidb_mysql::SqlMode) -> tidb_parser::SqlMode {
    tidb_parser::SqlMode {
        real_as_float: mode.has_real_as_float_mode(),
        no_backslash_escapes: mode.has_no_backslash_escapes_mode(),
        ansi_quotes: mode.has_ansi_quotes_mode(),
        high_not_precedence: mode.has_high_not_precedence_mode(),
        ignore_space: mode.has_ignore_space_mode(),
        pipes_as_concat: mode.has_pipes_as_concat_mode(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_decorrelate_in_select_reaches_the_statement_context() {
        let mut session = Session::new();
        assert!(!session
            .statement_context(false)
            .enable_no_decorrelate_in_select());
        session
            .run("set tidb_opt_enable_no_decorrelate_in_select = on")
            .unwrap();
        assert!(session
            .statement_context(false)
            .enable_no_decorrelate_in_select());
    }

    #[test]
    fn sql_mode_consumers_use_go_typed_session_state() {
        let vars = include_str!("vars.rs");
        let context = include_str!("stmt_ctx.rs");
        let session = include_str!("lib.rs");
        let prepared = include_str!("prepared_ast.rs");
        let point_context = context
            .split_once("pub(crate) fn prepared_point_get_context(")
            .expect("prepared point context")
            .1
            .split_once("pub fn result_materialization_authority(")
            .expect("prepared point context boundary")
            .0;
        let snapshot = context
            .split_once("fn statement_var_snapshot(")
            .expect("statement-variable snapshot")
            .1
            .split_once("pub(crate) fn statement_context_ignoring(")
            .expect("statement-variable snapshot boundary")
            .0;
        let builder = context
            .split_once("pub(crate) fn statement_context_ignoring(")
            .expect("statement-context builder")
            .1
            .split_once("pub(crate) const fn scanner_sql_mode_of(")
            .expect("statement-context builder boundary")
            .0;
        let splitter = session
            .split_once("pub fn split_statements(")
            .expect("multi-statement parser")
            .1
            .split_once("pub fn run(")
            .expect("multi-statement parser boundary")
            .0;

        assert!(vars.contains("sql_mode: tidb_mysql::SqlMode"));
        assert!(!session.contains("scanner_sql_mode_cache"));
        assert!(!point_context.contains("get_system(\"sql_mode\")"));
        assert!(snapshot.contains("self.vars.sql_mode()"));
        assert!(!snapshot.contains("mode_upper"));
        assert!(!builder.contains("scanner_sql_mode_of("));
        assert!(splitter.contains("self.scanner_sql_mode()"));
        assert!(!splitter.contains("get_system(\"sql_mode\")"));
        assert!(prepared.contains("self.vars.sql_mode()"));
        assert!(!prepared.contains("get_system(\"sql_mode\")"));
    }

    #[test]
    fn current_role_is_rendered_only_when_the_builtin_reads_it() {
        let context = include_str!("stmt_ctx.rs");
        let builder = context
            .split_once("pub(crate) fn statement_context_ignoring(")
            .expect("statement-context builder")
            .1
            .split_once("pub(crate) fn foreign_key_checks(")
            .expect("statement-context boundary")
            .0;
        let identity = include_str!("identity.rs");
        let executor = include_str!("../../tidb-executor/src/stmt_context.rs");

        assert!(builder.contains(".with_active_roles("));
        assert!(!builder.contains("current_role_text()"));
        assert!(!identity.contains("fn current_role_text"));
        assert!(executor.contains("active_roles: Option<Arc<Vec<(String, String)>>>"));
    }

    #[test]
    fn statement_clock_is_initialized_only_when_an_expression_reads_it() {
        let context = include_str!("stmt_ctx.rs");
        let builder = context
            .split_once("pub(crate) fn statement_context_ignoring(")
            .expect("statement-context builder")
            .1
            .split_once("pub(crate) fn foreign_key_checks(")
            .expect("statement-context boundary")
            .0;
        let executor = include_str!("../../tidb-executor/src/stmt_context.rs");

        assert!(!builder.contains("self.statement_clock("));
        assert!(builder.contains(".with_lazy_clock(snapshot.timestamp, zone)"));
        assert!(executor.contains("pub fn with_lazy_clock("));
        assert!(executor.contains("clock.get_or_init(|| {"));
        assert!(executor.contains("resolve_statement_clock("));
    }

    #[test]
    fn result_materialization_reuses_the_typed_statement_policy() {
        let source = include_str!("stmt_ctx.rs");
        let body = source
            .split_once("pub fn result_materialization_authority")
            .expect("result materialization authority exists")
            .1
            .split_once("pub fn session_time_zone")
            .expect("session time-zone accessor follows it")
            .0;

        assert!(body.contains("statement_var_snapshot"));
        assert!(body.contains("statement_memory_policy"));
        assert!(body.contains("statement_result_authority"));
        assert!(!body.contains("get_system"));
        assert!(!body.contains("get_global"));
        assert!(!body.contains("self.session_memory.statement();"));
    }

    #[test]
    fn result_materialization_retains_the_statement_context_tracker() {
        let session = Session::new();
        let context = session.statement_context(false);
        let executing_memory = context.statement_memory();
        let (retained_memory, _, _) = session.result_materialization_authority().into_parts();

        assert!(Arc::ptr_eq(
            executing_memory.stmt_tracker(),
            retained_memory.stmt_tracker(),
        ));
    }

    #[test]
    fn index_lookup_pushdown_uses_one_typed_statement_snapshot() {
        let mut session = Session::new();
        session
            .run("SET tidb_index_lookup_pushdown_policy = 'force'")
            .unwrap();
        session
            .run("SET transaction_isolation = 'READ-COMMITTED'")
            .unwrap();
        session.run("SET tidb_replica_read = 'follower'").unwrap();
        session.run("SET tidb_max_keys_read = 7").unwrap();

        let context = session.statement_context(false);
        let snapshot = context.index_lookup_push_down_session();
        assert_eq!(
            snapshot.policy,
            tidb_planner::access_path::IndexLookupPushDownPolicy::Force
        );
        assert!(!snapshot.repeatable_read);
        assert!(!snapshot.leader_read);
        assert_eq!(snapshot.max_keys_read, 7);
        assert!(!snapshot.staleness);
        assert!(!snapshot.historical_read);
    }

    #[test]
    fn read_committed_connected_session_captures_latest_index_schema() {
        let mut session = Session::new();
        assert!(session
            .statement_context(false)
            .latest_index_schema()
            .is_none());

        session.set_connection_id(7);
        session
            .run("SET transaction_isolation = 'READ-COMMITTED'")
            .unwrap();

        let latest = session
            .statement_context(false)
            .latest_index_schema()
            .expect("connected READ-COMMITTED statement has a domain snapshot");
        assert_eq!(
            latest.schema_meta_version,
            session.lock_catalog().unwrap().metadata_version()
        );

        let mut repeatable_read = Session::new();
        repeatable_read.set_connection_id(8);
        assert!(repeatable_read
            .statement_context_for_update_read(false)
            .latest_index_schema()
            .is_some());
    }

    #[test]
    fn unsafe_generated_column_substitution_uses_the_session_snapshot() {
        let mut session = Session::new();
        assert!(!session.statement_context(false).enable_unsafe_substitute());

        session
            .run("SET tidb_enable_unsafe_substitute = ON")
            .unwrap();
        assert!(session.statement_context(false).enable_unsafe_substitute());
    }

    #[test]
    fn semi_join_rewrite_uses_the_session_snapshot() {
        let mut session = Session::new();
        assert!(!session.statement_context(false).enable_semi_join_rewrite());

        session
            .run("SET tidb_opt_enable_semi_join_rewrite = ON")
            .unwrap();
        assert!(session.statement_context(false).enable_semi_join_rewrite());
    }

    #[test]
    fn skew_distinct_agg_uses_the_session_snapshot() {
        let mut session = Session::new();
        assert!(!session.statement_context(false).enable_skew_distinct_agg());

        session.run("SET tidb_opt_skew_distinct_agg = ON").unwrap();
        assert!(session.statement_context(false).enable_skew_distinct_agg());
    }

    #[test]
    fn ddl_job_metadata_uses_the_session_snapshot() {
        let mut session = Session::new();
        session.set_connection_id(77);
        session.run("SET tidb_cdc_write_source = 9").unwrap();
        session
            .run("SET tidb_ddl_reorg_priority = 'PRIORITY_HIGH'")
            .unwrap();
        session.run("SET tidb_session_alias = 'ddl-owner'").unwrap();

        let context = session.ddl_statement_context();
        assert_eq!(context.ddl_connection_id(), 77);
        assert_eq!(context.ddl_cdc_write_source(), 9);
        assert_eq!(context.ddl_reorg_priority(), 2);
        assert_eq!(context.ddl_session_alias(), "ddl-owner");
    }
}
