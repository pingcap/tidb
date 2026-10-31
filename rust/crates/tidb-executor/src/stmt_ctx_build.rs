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

use crate::StatsStmtCtxData as _;
use tidb_planner::stmt_ctx::PlannerStmtCtxData as _;
use std::sync::Arc;

use crate::sessionctx::StatementKind;
use std::collections::HashMap;
use std::sync::Mutex;

use tidb_datatype::Datum;

use tidb_variable::hypo_index::HypoIndexes;
use crate::DriverError;
use crate::MysqlRng;
use crate::StmtOutput;

use tidb_hack::GoToLower;

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
pub struct StatementVarSnapshot {
    generation: u64,
    version: Option<String>,
    time_zone: crate::SessionTimeZone,
    connection_charset: String,
    connection_collation: String,
    default_collation_for_utf8mb4: String,
    allow_write_row_id: bool,
    /// `tidb_enable_ordered_result_mode` (Go `EnableStableResultMode`).
    stable_result_mode: bool,
    /// `tidb_ignore_inlist_plan_digest`.
    ignore_inlist_plan_digest: bool,
    sysdate_is_now: bool,
    timestamp: Option<f64>,
    sql_mode: tidb_mysql::SqlMode,
    string_type_flags: tidb_datatype::ConversionFlags,
    allow_auto_random_explicit_insert: bool,
    shard_allocate_step: u64,
    like_default_escape: u8,
    week_format: i64,
    div_scale: u32,
    windowing_use_high_precision: bool,
    enable_pipelined_window_exec: bool,
    cte_depth: i64,
    join_reorder_threshold: i32,
    default_string_match_selectivity: f64,
    selectivity_factor: f64,
    enable_pseudo_for_outdated_stats: bool,
    stats_load_sync_wait_ms: u64,
    stats_load_pseudo_timeout: bool,
    plan_replayer_capture_enabled: bool,
    opt_index_prune_threshold: i32,
    range_max_size: i64,
    opt_prefix_index_single_scan: bool,
    always_keep_join_key: bool,
    allow_agg_push_down: bool,
    enable_3_stage_distinct_agg: bool,
    enable_3_stage_multi_distinct_agg: bool,
    tiflash_pre_agg_mode: String,
    partial_ordered_index_for_topn: bool,
    enable_unsafe_substitute: bool,
    enable_force_inline_cte: bool,
    advanced_join_hint: bool,
    enable_semi_join_rewrite: bool,
    allow_in_subq_to_join_and_agg: bool,
    enable_no_decorrelate_in_select: bool,
    enable_skew_distinct_agg: bool,
    max_execution_time_ms: u64,
    advanced_join_reorder: bool,
    constraint_check_in_place: bool,
    ordering_index_selectivity_ratio: f64,
    ordering_index_selectivity_threshold: f64,
    allow_projection_push_down: bool,
    enable_inl_join_inner_multi_pattern: bool,
    enable_hash_join: bool,
    enable_null_aware_anti_join: bool,
    limit_push_down_threshold: u64,
    index_lookup_push_down_session: tidb_sessionctx::IndexLookupPushDownSession,
    join_reorder_through_proj: bool,
    join_reorder_through_sel: bool,
    outer_join_reorder: bool,
    index_merge: bool,
    static_partition_prune: bool,
    skip_missing_partition_stats: bool,
    select_limit: u64,
    new_only_full_group_by_check: bool,
    auto_increment_in_generated: bool,
    /// Go `SessionVars.PrimaryKeyRequired` (`@@sql_require_primary_key`).
    primary_key_required: bool,
    tmp_table_max_size: i64,
    remove_orderby_in_subquery: bool,
    pub mem_quota: i64,
    replica_read: crate::ReplicaReadType,
    coprocessor_read_policy: tidb_distsql::remote_scan::CoprocessorReadPolicy,
    isolation_read_engines: String,
    init_chunk_size: usize,
    max_chunk_size: usize,
    max_allowed_packet: u64,
    group_concat_max_len: u64,
    apply_cache_capacity: i64,
    enable_parallel_apply: bool,
    /// Go `SessionVars.AllowDeriveTopN` (`tidb_opt_derive_topn`).
    allow_derive_topn: bool,
    hashagg_partial_concurrency: usize,
    hashagg_final_concurrency: usize,
    block_encryption_mode: crate::BlockEncryptionMode,
    ddl_cdc_write_source: u64,
    ddl_reorg_priority: i64,
    pub ddl_session_alias: String,
    arbitrator_wait_averse: Option<bool>,
    arbitrator_reserved: i64,
    /// The process-list metadata Go reads off typed `SessionVars` fields
    /// at every statement start (`EnableRedactLog`, `AnalyzeVersion`,
    /// `EnabledRateLimitAction`); parsed once per variable-table generation
    /// like every other field here.
    pub redact_sql: tidb_parser::RedactMode,
    pub analyze_version: i64,
    pub rate_limit_action: bool,
    /// Go `SessionVars.LockWaitTimeout` (`innodb_lock_wait_timeout`, in
    /// seconds), which every pessimistic lock request reads.
    pub lock_wait_timeout_secs: u64,
}


/// Go `ResetContextOfStmt`'s `sc.Priority`: the statement's own priority,
/// unless the instance forces one (`vardef.ForcePriority`).
fn statement_priority(stmt: &tidb_ast::Stmt) -> tidb_ast::StatementPriority {
    match tidb_mysql::Priority::from_enum(
        tidb_vardef::FORCE_PRIORITY.load(std::sync::atomic::Ordering::SeqCst),
    ) {
        tidb_mysql::Priority::None => crate::sessionctx::statement_priority_of(stmt),
        tidb_mysql::Priority::Low => tidb_ast::StatementPriority::Low,
        tidb_mysql::Priority::High => tidb_ast::StatementPriority::High,
        tidb_mysql::Priority::Delayed => tidb_ast::StatementPriority::Delayed,
    }
}

/// Go `ResetContextOfStmt` (`executor/select.go`) and the statement-context builders over the session.
pub trait ResetContextOfStmt: crate::stmt_ctx_build::StmtCtxSession {
    /// Installs the transaction owner's selected-row collector while it runs
    /// a statement. Clearing it also drops keys from a failed attempt.
    fn set_selected_lock_keys(
        &mut self,
        keys: Option<tidb_sessionctx::select_lock::SelectedLockKeys>,
    ) {
        *self.selected_lock_keys_mut() = keys;
    }

    /// Drains the current attempt's selected record keys for lock acquisition.
    fn take_selected_lock_keys(&self) -> Vec<Vec<u8>> {
        self.selected_lock_keys_ref()
            .as_ref()
            .map_or_else(Vec::new, tidb_sessionctx::select_lock::SelectedLockKeys::take)
    }

    /// Drains physical locking operators without discarding their wait modes.
    fn take_selected_lock_requests(
        &self,
    ) -> Vec<(tidb_txnkv::transaction::LockWaitTime, Vec<Vec<u8>>)> {
        self.selected_lock_keys_ref().as_ref().map_or_else(
            Vec::new,
            tidb_sessionctx::select_lock::SelectedLockKeys::take_requests,
        )
    }

    /// Drains the current attempt's published keys, the unchanged matched
    /// rows apart from the locking reads' (see
    /// [`crate::select_lock::TakenLocks`]).
    fn take_selected_locks(&self) -> tidb_sessionctx::select_lock::TakenLocks {
        self.selected_lock_keys_ref().as_ref().map_or_else(
            tidb_sessionctx::select_lock::TakenLocks::default,
            tidb_sessionctx::select_lock::SelectedLockKeys::take_all,
        )
    }

    fn optimizer_cost_env(
        &self,
        mem_quota: i64,
        tmp_storage_on_oom: bool,
    ) -> Arc<tidb_planner::find_best_task::coster::CostEnv> {
        // Everything below derives from the session's variable table, except
        // the two per-statement arguments. Share the immutable snapshot until
        // either source changes; retained contexts must keep their old policy.
        // Go's equivalents are typed `SessionVars` fields maintained at `SET`.
        let generation = self.session_vars().generation();
        if let Some((cached_at, env)) = self.cost_env_cache_ref().borrow_mut().as_mut() {
            if *cached_at == generation {
                if env.session.mem_quota != mem_quota
                    || env.session.enable_tmp_storage_on_oom != tmp_storage_on_oom
                {
                    let updated = Arc::make_mut(env);
                    updated.session.mem_quota = mem_quota;
                    updated.session.enable_tmp_storage_on_oom = tmp_storage_on_oom;
                }
                return Arc::clone(env);
            }
        }
        let number = |name: &str, default: f64| {
            self.session_vars()
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(default)
        };
        let enabled = |name: &str, default: bool| {
            self.session_vars()
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

        let group_ndv_skew_ratio = number("tidb_opt_group_ndv_skew_ratio", 0.0);
        let mut env = tidb_planner::find_best_task::coster::CostEnv::default();
        env.session.estimator_options =
            tidb_planner_cardinality::row_count_estimator::EstimatorOptions {
                time_zone: std::sync::Arc::new(self.session_time_zone()),
                risk_eq_skew_ratio: number(
                    tidb_vardef::tidb_vars::TIDB_OPT_RISK_EQ_SKEW_RATIO,
                    tidb_vardef::defaults::DEF_OPT_RISK_EQ_SKEW_RATIO,
                ),
                risk_range_skew_ratio: number(
                    tidb_vardef::tidb_vars::TIDB_OPT_RISK_RANGE_SKEW_RATIO,
                    tidb_vardef::defaults::DEF_OPT_RISK_RANGE_SKEW_RATIO,
                ),
                allow_use_modify_count: self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_OPT_OBJECTIVE)
                    .ok()
                    .is_none_or(|value| {
                        !value
                            .eq_ignore_ascii_case(tidb_vardef::tidb_vars::OPT_OBJECTIVE_DETERMINATE)
                    }),
                selectivity_factor: self.session_vars().selectivity_factor(),
                default_str_match_selectivity: self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_DEFAULT_STR_MATCH_SELECTIVITY)
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(
                        tidb_vardef::defaults::DEF_TIDB_DEFAULT_STR_MATCH_SELECTIVITY as f64,
                    ),
                range_max_size: number(
                    tidb_vardef::tidb_vars::TIDB_OPT_RANGE_MAX_SIZE,
                    tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_SIZE as f64,
                ) as i64,
                opt_prefix_index_single_scan: self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_OPT_PREFIX_INDEX_SINGLE_SCAN)
                    .ok()
                    .map_or(
                        tidb_vardef::defaults::DEF_TIDB_OPT_PREFIX_INDEX_SINGLE_SCAN,
                        |value| value.eq_ignore_ascii_case("ON") || value == "1",
                    ),
                regard_null_as_point: self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_REGARD_NULL_AS_POINT)
                    .ok()
                    .map_or(
                        tidb_vardef::defaults::DEF_TIDB_REGARD_NULL_AS_POINT,
                        |value| value.eq_ignore_ascii_case("ON") || value == "1",
                    ),
                fix_44389: self
                    .session_vars()
                    .optimizer_fix_control()
                    .get_bool_with_default(tidb_fixcontrol::FIX_44389, false),
                fix_54337: self
                    .session_vars()
                    .optimizer_fix_control()
                    .get_bool_with_default(tidb_fixcontrol::FIX_54337, false),
            };
        env.session.correlation_options =
            tidb_planner_cardinality::cross_estimation::CorrelationOptions {
                enabled: enabled(
                    tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_CORRELATION_ADJUSTMENT,
                    tidb_vardef::defaults::DEF_OPT_ENABLE_CORRELATION_ADJUSTMENT,
                ),
                threshold: number(
                    tidb_vardef::tidb_vars::TIDB_OPT_CORRELATION_THRESHOLD,
                    tidb_vardef::defaults::DEF_OPT_CORRELATION_THRESHOLD,
                ),
                exponent: self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_OPT_CORRELATION_EXP_FACTOR)
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(tidb_vardef::defaults::DEF_OPT_CORRELATION_EXP_FACTOR),
            };
        env.session.hash_join_concurrency = resolved_concurrency("tidb_hash_join_concurrency");
        env.session.shuffle_options = tidb_planner::shuffle_optimize::ShuffleOptions {
            window_concurrency: resolved_concurrency("tidb_window_concurrency") as usize,
            stream_agg_concurrency: resolved_concurrency("tidb_streamagg_concurrency") as usize,
            merge_join_concurrency: resolved_concurrency("tidb_merge_join_concurrency") as usize,
            group_ndv_skew_ratio,
        };
        env.session.group_ndv_skew_ratio = group_ndv_skew_ratio;
        env.session.scale_ndv_skew_ratio = number("tidb_opt_scale_ndv_skew_ratio", 1.0);
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
        env.session.use_hash_join_v2 = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_HASH_JOIN_VERSION)
            .ok()
            .is_none_or(|value| tidb_executor_join_joinversion::is_optimized_version(&value))
            && tidb_executor_join_joinversion::is_hash_join_v2_supported();
        env.session.mpp_allowed = enabled("tidb_allow_mpp", true);
        env.session.mpp_enforced = env.session.mpp_allowed && enabled("tidb_enforce_mpp", false);
        env.session.enable_chunk_rpc = enabled("tidb_enable_chunk_rpc", true);

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

        let env = Arc::new(env);
        *self.cost_env_cache_ref().borrow_mut() = Some((generation, env.clone()));
        env
    }

    /// The expression context used by an immutable prepared PointGet plan.
    ///
    /// That cache admits only direct projections of stored columns. Row
    /// decoding can therefore consult only the session zone and SELECT's
    /// origin-default date flags; planner settings, sequences, user values,
    /// clocks, globals, and expression state cannot affect the result.
    fn prepared_point_get_context(
        &self,
    ) -> tidb_table::kv_table::PreparedPointGetDecodeContext {
        tidb_table::kv_table::PreparedPointGetDecodeContext::for_query(
            self.session_vars().sql_mode().has_allow_invalid_dates_mode(),
            self.session_time_zone(),
        )
    }

    /// Captures the policy a row result keeps after its statement finishes.
    ///
    /// Go's eager cursor attaches a row-container tracker to session memory
    /// and retains the producing statement's chunk bounds. This session
    /// materializes rows before the wire layer takes ownership, so refresh
    /// only the session-memory policy and create the retention handle here.
    /// A complete `StmtContext` would also snapshot planner, expression, sequence, and
    /// user state that result materialization never reads.
    fn result_materialization_authority(&self) -> ResultMaterializationAuthority {
        if let Some(authority) = self.statement_result_authority_ref().borrow().as_ref() {
            return authority.clone().with_current_tso(self.result_read_tso());
        }
        let snapshot = self.statement_var_snapshot();
        let (oom_action, tmp_storage_on_oom) = self.session_vars().statement_memory_policy();
        let authority = self.build_statement_result_authority(
            &snapshot,
            snapshot.mem_quota,
            oom_action,
            tmp_storage_on_oom,
        );
        self.statement_result_authority_ref()
            .replace(Some(authority.clone()));
        authority.with_current_tso(self.result_read_tso())
    }

    fn result_read_tso(&self) -> crate::CurrentTso {
        if let Some(ts) = &self.statement_snapshot_ts() {
            let read = crate::CurrentTso::default();
            read.publish(*ts);
            read
        } else {
            self.current_tso()
        }
    }

    fn build_statement_result_authority(
        &self,
        snapshot: &StatementVarSnapshot,
        mem_quota: i64,
        oom_action: crate::OomAction,
        tmp_storage_on_oom: bool,
    ) -> ResultMaterializationAuthority {
        self.session_memory_ref()
            .configure(mem_quota, oom_action, tmp_storage_on_oom);
        // Go resets one StmtCtx per statement. Planning, execution and
        // metadata contexts share it; replacing it here lets the old tracker
        // detach after a new OOM action is installed and clear that action.
        if let Some(authority) = self.statement_result_authority_ref().borrow().as_ref() {
            if !authority.statement_memory().is_finished() {
                return authority.clone();
            }
        }
        let memory = self.session_memory_ref().statement_with_arbitration(
            snapshot.arbitrator_wait_averse,
            snapshot.arbitrator_reserved,
            &self.current_sql_digest_key_ref(),
        );
        ResultMaterializationAuthority::new(
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
    fn session_time_zone(&self) -> crate::SessionTimeZone {
        self.statement_var_snapshot().time_zone.clone()
    }

    /// Go `SessionVars.LockWaitTimeout` as a duration: the wait a pessimistic
    /// lock request carries, read from the parsed variable snapshot rather
    /// than the variable text on every statement.
    #[must_use]
    fn lock_wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.statement_var_snapshot().lock_wait_timeout_secs)
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
    fn statement_clock(&self, zone: &crate::SessionTimeZone) -> (i64, u32, i32) {
        use crate::SessionTimeZone;
        let pinned = self
            .session_vars()
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
    /// sees (a stale transaction's historical schema inside `BEGIN ... AS OF`).
    ///
    /// Only the NAMES are snapshotted: the allocators are `Arc` handles, so
    /// consuming a value through one moves the counter the catalog holds. That
    /// is deliberate and matches Go, where `NEXTVAL` allocates in its own meta
    /// transaction -- see `with_statement_stage`'s note about a storage whose
    /// clone shares a handle rather than copying by value.
    fn sequence_snapshot(&self) -> Arc<tidb_infoschema::SequenceSnapshot> {
        let snapshot = |catalog: &crate::Catalog| {
            Arc::new(tidb_infoschema::SequenceSnapshot::new(
                catalog,
                &self.session_vars().current_db,
            ))
        };
        // The statement itself reports a poisoned catalog.
        self.with_txn_catalog(snapshot).unwrap_or_default()
    }

    /// Go `expression.DecodeKeyFromString` over this statement's catalog
    /// version.
    fn decode_key_from_string(&self) -> tidb_exprctx::DecodeKeyFromString {
        let snapshot = self.tidb_decode_key_snapshot();
        Arc::new(move |input, zone| snapshot.decode(input, zone))
    }

    fn tidb_decode_key_snapshot(&self) -> Arc<crate::TidbDecodeKeySnapshot> {
        let domain = self.domain_info_schema();
        let Ok(catalog) = domain.lock() else {
            return Arc::default();
        };
        // Keyed on the METADATA counter, not the mutation counter: Go's
        // row-decode metadata is cached per infoschema version, which DDL
        // moves and DML never does. Keying on `version()` here would rebuild
        // this snapshot on every write statement.
        let version = catalog.metadata_version();
        {
            let cache = self
                .tidb_decode_key_cache_ref()
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
            .tidb_decode_key_cache_ref()
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
    /// [`crate::StmtContext`], which every executor entry already
    /// takes, rather than on ~30 separate parameters.
    fn scanner_sql_mode(&self) -> tidb_lexer::SqlMode {
        crate::scanner_sql_mode(self.session_vars().sql_mode())
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
    fn parse_statement(&self, sql: &str) -> Result<tidb_ast::Stmt, DriverError> {
        let started = std::time::Instant::now();
        let stmt = self.parse(sql);
        // Go observes the parse histogram only after a successful parse
        // (`session.Parse` returns on the error path before observing).
        if stmt.is_ok() {
            self.observe_parse_duration(started.elapsed().as_secs_f64(), false);
        }
        stmt
    }

    fn parse(&self, sql: &str) -> Result<tidb_ast::Stmt, DriverError> {
        self.parse_with_config(sql, self.session_vars().build_parser_config())
    }

    /// Go `ParseWithParams` for internal SQL, which calls `ParseSQL` with no
    /// parse params ("Do no respect the settings from clients, if it is for
    /// internal usage"): text this tier wrote or restored is UTF-8, so the
    /// scanner keeps its default client and connection encodings.
    fn parse_internal(&self, sql: &str) -> Result<tidb_ast::Stmt, DriverError> {
        let defaults = tidb_parser::ParserConfig::default();
        self.parse_with_config(
            sql,
            tidb_parser::ParserConfig {
                charset_client: defaults.charset_client,
                charset_connection: defaults.charset_connection,
                ..self.session_vars().build_parser_config()
            },
        )
    }

    fn parse_with_config(
        &self,
        sql: &str,
        config: tidb_parser::ParserConfig,
    ) -> Result<tidb_ast::Stmt, DriverError> {
        let parsed =
            tidb_parser::parse_with_sql_mode_and_config(sql, self.scanner_sql_mode(), config);
        parsed.map_err(|e| match e.terror {
            Some(proto) => DriverError::from((proto).generate_with_stack(e.message)),
            // Go's session layer wraps a positional parser error through
            // `util.SyntaxError`, whose message body is the parser's own
            // `line L column C near "..."` text — not a Debug dump.
            None => DriverError::from(tidb_util::misc::syntax_error(e.compatibility_message(sql))),
        })
    }

    /// The statement context a session-aware DDL front end must carry through
    /// parsing, default admission, and catalog persistence.
    ///
    /// DDL uses the query-shaped context because it does not write table rows,
    /// while its default checks still consult the captured strict/date modes
    /// and session time zone.
    fn ddl_statement_context(&self) -> crate::StmtContext {
        // Go DDL reads `SessionVars.ForeignKeyChecks` (`checkTableForeignKeysValid`,
        // `CreateTableArgs.FKCheck`); the read-shaped context leaves it at ON.
        // `metabuild.WithClusteredIndexDefMode` reads `@@tidb_enable_clustered_index`.
        // `metabuild.WithShardRowIDBits` / `WithPreSplitRegions` read
        // `@@tidb_shard_row_id_bits` / `@@tidb_pre_split_regions`
        // (`TidbOptUint64`, so an unreadable value is the default 0).
        let uint_var = |name: &str| {
            self.session_vars()
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0)
        };
        let context = self
            .statement_context(false)
            .with_foreign_key_checks(self.foreign_key_checks())
            .with_clustered_index_def_mode(self.clustered_index_mode())
            .with_shard_row_id_defaults(
                uint_var(tidb_vardef::tidb_vars::TIDB_SHARD_ROW_ID_BITS),
                uint_var(tidb_vardef::tidb_vars::TIDB_PRE_SPLIT_REGIONS),
            );
        let reorg_vars = self.ddl_reorg_vars(&context);
        context.with_ddl_reorg_vars(reorg_vars)
    }

    /// The values Go `NewDDLReorgMeta` / `initJobReorgMetaFromVariables`
    /// read off the session for a reorganizing job.
    fn ddl_reorg_vars(&self, context: &crate::StmtContext) -> crate::DdlReorgVars {
        use tidb_vardef::tidb_vars;
        let defaults = crate::DdlReorgVars::default();
        let int_var = |name: &str, default: i64| {
            self.session_vars()
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(default)
        };
        let bool_var = |name: &str, default: bool| {
            self.session_vars().get_system(name).map_or(default, |value| {
                tidb_vardef::option_values::tidb_opt_on(&value)
            })
        };
        // Go `ddlutil.GetTimeZone`: a loadable zone name with offset 0, else
        // "UTC" with the zone's current offset.
        let (time_zone_name, time_zone_offset) = match context.session_zone() {
            tidb_datatype::SessionTimeZone::Named(zone) => (zone.name().to_owned(), 0),
            tidb_datatype::SessionTimeZone::Local => ("Local".to_owned(), 0),
            tidb_datatype::SessionTimeZone::Fixed { name, offset_secs } => {
                if name.parse::<chrono_tz::Tz>().is_ok() {
                    (name, 0)
                } else {
                    ("UTC".to_owned(), i64::from(offset_secs))
                }
            }
        };
        crate::DdlReorgVars {
            worker_count: int_var(
                tidb_vars::TIDB_DDL_REORG_WORKER_COUNT,
                defaults.worker_count,
            ),
            batch_size: int_var(tidb_vars::TIDB_DDL_REORG_BATCH_SIZE, defaults.batch_size),
            max_write_speed: int_var(tidb_vars::TIDB_DDL_REORG_MAX_WRITE_SPEED, 0),
            max_dist_task_nodes: int_var(
                tidb_vars::TIDB_MAX_DIST_TASK_NODES,
                defaults.max_dist_task_nodes,
            ),
            enable_dist_task: bool_var(tidb_vars::TIDB_ENABLE_DIST_TASK, defaults.enable_dist_task),
            enable_fast_reorg: tidb_vardef::ENABLE_FAST_REORG
                .load(std::sync::atomic::Ordering::SeqCst),
            target_scope: self
                .session_vars()
                .get_system(tidb_vars::TIDB_SERVICE_SCOPE)
                .unwrap_or_default(),
            resource_group_name: self.current_resource_group().to_owned(),
            enable_ddl_analyze: self
                .session_vars()
                .get_system(tidb_vars::TIDB_ENABLE_DDL_ANALYZE)
                .unwrap_or_else(|_| defaults.enable_ddl_analyze.clone()),
            analyze_version: self
                .session_vars()
                .get_system(tidb_vars::TIDB_ANALYZE_VERSION)
                .unwrap_or_else(|_| defaults.analyze_version.clone()),
            scatter_region: self
                .session_vars()
                .get_system(tidb_vars::TIDB_SCATTER_REGION)
                .unwrap_or_else(|_| defaults.scatter_region.clone()),
            time_zone_name,
            time_zone_offset,
        }
    }

    /// Shares evaluation policy and warnings with server-owned ANALYZE sampling.
    #[must_use]
    fn analyze_statement_context(&self) -> crate::StmtContext {
        self.statement_context(false)
    }

    /// Starts a statement executed by a server-owned route and returns the
    /// same statement memory and SQL killer ordinary execution receives.
    #[must_use]
    fn routed_statement_memory(&self) -> crate::StatementMemory {
        self.statement_context(false).statement_memory()
    }

    /// Resolves a `CREATE [OR REPLACE] VIEW` against this session's catalog
    /// for the cluster DDL route: `Ok(None)` when `sql` is not a CREATE
    /// VIEW; otherwise the `(database, name, or_replace, view)` the cluster
    /// tier publishes. This is Go's shape — `executeCreateView` preprocesses
    /// the body in the executor (failing a bad body at CREATE time) and
    /// hands DDL a finished definition.
    fn resolve_cluster_view(
        &mut self,
        sql: &str,
    ) -> Result<Option<(String, String, bool, crate::ViewDef)>, DriverError> {
        let stmt = self.parse(sql)?;
        self.resolve_cluster_view_parsed(&stmt)
    }

    /// [`Self::resolve_cluster_view`] over an already-parsed statement.
    fn resolve_cluster_view_parsed(
        &mut self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<Option<(String, String, bool, crate::ViewDef)>, DriverError> {
        let tidb_ast::Stmt::Ddl(ddl) = stmt else {
            return Ok(None);
        };
        let tidb_ast::DdlStmt::CreateView(create) = &**ddl else {
            return Ok(None);
        };
        let mut scanned = stmt.clone();
        let has_as_of = tidb_bindinfo::binding::scan_statement_tables(&mut scanned).has_as_of;
        // Go's preprocessor drops each body SELECT's lock
        // (`checkCreateViewWithSelect`) before it reaches the SELECTs.
        let mut body = (*create.query).clone();
        crate::view::check_create_view_with_select(&mut body)?;
        self.check_noop_functions(&body)?;
        self.refuse_stale_view_body(create, has_as_of)?;
        let create = create.clone();
        let current_db = self.session_vars().current_db.clone();
        let ctx = self.statement_context(false);
        let or_replace = create.or_replace;
        let (database, name, view) = self.with_catalog_mut(|catalog| {
            crate::resolve_view_definition(&create, catalog, &current_db, &ctx)
        })?;
        Ok(Some((database, name, or_replace, view)))
    }

    /// Go `DDLExec.executeCreateView` preprocesses the view's SELECT as a
    /// statement of its own and refuses a stale read there (1356): a table
    /// read `AS OF`, or `tidb_read_staleness`.
    fn refuse_stale_view_body(
        &self,
        create: &tidb_ast::CreateViewStmt,
        has_as_of: bool,
    ) -> Result<(), DriverError> {
        let read_staleness = self
            .session_vars()
            .get_system("tidb_read_staleness")
            .unwrap_or_default()
            .parse::<i64>()
            .unwrap_or(0);
        if !has_as_of && read_staleness == 0 {
            return Ok(());
        }
        let (schema, name) = match create.name.as_slice() {
            [schema, name] => (schema.as_str(), name.as_str()),
            [name] => (self.session_vars().current_db.as_str(), name.as_str()),
            _ => ("", ""),
        };
        Err(DriverError::from(
            tidb_plannererrors::ERR_VIEW_INVALID.generate_with_stack_by_args(&[
                schema.go_to_lower().into(),
                name.go_to_lower().into(),
            ]),
        ))
    }

    /// `tidb_cdc_write_source`, which Go copies into each DDL job
    /// (`job.CDCWriteSource`): a DDL replicated by TiCDC skips BDR admission.
    fn ddl_cdc_write_source(&self) -> u64 {
        self.session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_CDC_WRITE_SOURCE)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn statement_context(&self, is_dml: bool) -> crate::StmtContext {
        self.statement_context_ignoring(is_dml, false)
    }

    /// Go `ResetContextOfStmt`'s statement-scoped modifiers: the session
    /// snapshot plus the statement's own priority and `SQL_NO_CACHE`. Both
    /// ride the `StmtContext` into every coprocessor request the statement
    /// issues (`SetFromSessionVars`).
    fn statement_context_for_stmt(
        &self,
        stmt: &tidb_ast::Stmt,
        is_dml: bool,
    ) -> crate::StmtContext {
        let mut ctx = self
            .statement_context(is_dml)
            .with_statement_priority(statement_priority(stmt))
            .with_not_fill_cache(crate::sessionctx::statement_not_fill_cache(stmt))
            .with_client_warning_counts(self.sys_error_count(), self.sys_warning_count());
        // Go `Optimize`: a statement that is not read-only plans without
        // TiFlash under a strict `sql_mode`.
        if self.session_vars().sql_mode().has_strict_mode() && !stmt.is_read_only(true) {
            ctx = ctx.with_tiflash_engine_removed_due_to_strict_sql_mode();
        }
        // Go installs the session's reusable chunk allocator for every
        // statement (`SetAlloc` in `conn.handleQuery`), which
        // `tidb_enable_reuse_chunk` gates; the planner may clear it.
        ctx.set_chunk_alloc_valid(
            self.session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_REUSECHUNK)
                .is_ok_and(|value| value.eq_ignore_ascii_case("ON") || value == "1"),
        );
        ctx
    }

    fn latest_index_schema_snapshot(
        &self,
    ) -> Option<Arc<tidb_infoschema::table_source::LatestIndexSchema>> {
        let mut schema = self.domain_info_schema().lock().ok()?.latest_index_schema();
        for (_, _, table) in self.local_temporary_tables_ref() {
            Arc::make_mut(&mut schema).table_indexes.insert(
                table.table_id(),
                table.meta().read().indices.iter_deref().collect(),
            );
        }
        Some(schema)
    }

    fn statement_context_for_update_read(
        &self,
        ignore_err: bool,
    ) -> crate::StmtContext {
        self.statement_context_ignoring(true, ignore_err)
    }

    /// [`Self::statement_context`] for a DML statement that carries the
    /// `IGNORE` modifier, which Go's `ResetContextOfStmt` reads off the AST
    /// and folds into every value-level error level.
    /// The cached [`StatementVarSnapshot`], re-derived only when a `SET`
    /// moved the variable table; see the struct's own doc for why the
    /// GLOBAL-scope reads are NOT in it.
    fn statement_var_snapshot(&self) -> Arc<StatementVarSnapshot> {
        let generation = self.session_vars().generation();
        if let Some(cached) = self.statement_var_cache_ref().borrow().as_ref() {
            if cached.generation == generation {
                return Arc::clone(cached);
            }
        }
        let sql_mode = self.session_vars().sql_mode();
        let on = |name: &str| {
            self.session_vars()
                .get_system(name)
                .is_ok_and(|value| value.eq_ignore_ascii_case("on") || value == "1")
        };
        let not_off = |name: &str| {
            !matches!(
                self.session_vars().get_system(name).as_deref(),
                Ok("OFF" | "off" | "0")
            )
        };
        let executor_concurrency = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_EXECUTOR_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(tidb_vardef::defaults::DEF_EXECUTOR_CONCURRENCY as usize);
        let resolved_concurrency = |name: &str| {
            self.session_vars()
                .get_system(name)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|value| *value > 0)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(executor_concurrency)
        };
        let index_lookup_push_down_policy = match self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_INDEX_LOOK_UP_PUSH_DOWN_POLICY)
            .as_deref()
        {
            Ok("force") => tidb_sessionctx::IndexLookupPushDownPolicy::Force,
            Ok("affinity-force") => {
                tidb_sessionctx::IndexLookupPushDownPolicy::AffinityForce
            }
            _ => tidb_sessionctx::IndexLookupPushDownPolicy::HintOnly,
        };
        let read_staleness = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_READ_STALENESS)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .is_some_and(|value| value != 0);
        let index_lookup_push_down_session =
            tidb_sessionctx::IndexLookupPushDownSession {
                repeatable_read: self
                    .session_vars()
                    .get_system("transaction_isolation")
                    .is_ok_and(|value| value.eq_ignore_ascii_case("REPEATABLE-READ")),
                leader_read: self.effective_replica_read(self.session_vars().replica_read())
                    == crate::ReplicaReadType::Leader,
                staleness: read_staleness,
                historical_read: self.session_vars().snapshot_ts() != 0,
                max_keys_read: self.session_vars().max_keys_read(true),
                policy: index_lookup_push_down_policy,
            };
        let snapshot = Arc::new(StatementVarSnapshot {
            generation,
            string_type_flags: tidb_datatype::ConversionFlags::default()
                .with_skip_ascii_check(on(tidb_vardef::tidb_vars::TIDB_SKIP_ASCII_CHECK))
                .with_skip_utf8_check(on(tidb_vardef::tidb_vars::TIDB_SKIP_UTF8_CHECK)),
            version: self.session_vars().get_system("version").ok(),
            time_zone: self.session_vars().session_time_zone(),
            connection_charset: self
                .session_vars()
                .get_system("character_set_connection")
                .unwrap_or_else(|_| "utf8mb4".to_owned()),
            connection_collation: self
                .session_vars()
                .get_system("collation_connection")
                .unwrap_or_else(|_| "utf8mb4_bin".to_owned()),
            default_collation_for_utf8mb4: self
                .session_vars()
                .get_system("default_collation_for_utf8mb4")
                .unwrap_or_else(|_| "utf8mb4_bin".to_owned()),
            allow_write_row_id: on(tidb_vardef::tidb_vars::TIDB_OPT_WRITE_ROW_ID),
            stable_result_mode: on(tidb_vardef::tidb_vars::TIDB_ENABLE_ORDERED_RESULT_MODE),
            ignore_inlist_plan_digest: on(tidb_vardef::tidb_vars::TIDB_IGNORE_INLIST_PLAN_DIGEST),
            sysdate_is_now: on(tidb_vardef::tidb_vars::TIDB_SYSDATE_IS_NOW),
            // Only an explicit override: this snapshot outlives the statement,
            // and the hook's live answer would pin `NOW()` to its first read.
            timestamp: self.session_vars().timestamp_override(),
            allow_auto_random_explicit_insert: on(
                tidb_vardef::tidb_vars::TIDB_ALLOW_AUTO_RAND_EXPLICIT_INSERT,
            ),
            shard_allocate_step: self
                .session_vars()
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
                .session_vars()
                .get_system("default_week_format")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0),
            div_scale: self
                .session_vars()
                .get_system("div_precision_increment")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(4),
            windowing_use_high_precision: not_off("windowing_use_high_precision"),
            enable_pipelined_window_exec: not_off("tidb_enable_pipelined_window_function"),
            cte_depth: self
                .session_vars()
                .get_system("cte_max_recursion_depth")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(1000),
            join_reorder_threshold: self
                .session_vars()
                .get_system("tidb_opt_join_reorder_threshold")
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_OPT_JOIN_REORDER_THRESHOLD as i32),
            default_string_match_selectivity: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_DEFAULT_STR_MATCH_SELECTIVITY)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.0),
            selectivity_factor: self.session_vars().selectivity_factor(),
            enable_pseudo_for_outdated_stats: on("tidb_enable_pseudo_for_outdated_stats"),
            stats_load_sync_wait_ms: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_STATS_LOAD_SYNC_WAIT)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_STATS_LOAD_SYNC_WAIT as u64),
            stats_load_pseudo_timeout: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_STATS_LOAD_PSEUDO_TIMEOUT)
                .map_or(true, |value| {
                    !value.eq_ignore_ascii_case("OFF") && value != "0"
                }),
            plan_replayer_capture_enabled: on("tidb_enable_plan_replayer_capture")
                || on("tidb_enable_plan_replayer_continuous_capture"),
            opt_index_prune_threshold: self
                .session_vars()
                .get_system("tidb_opt_index_prune_threshold")
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(20),
            range_max_size: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_OPT_RANGE_MAX_SIZE)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_SIZE),
            opt_prefix_index_single_scan: on(
                tidb_vardef::tidb_vars::TIDB_OPT_PREFIX_INDEX_SINGLE_SCAN,
            ),
            always_keep_join_key: on(tidb_vardef::tidb_vars::TIDB_OPT_ALWAYS_KEEP_JOIN_KEY),
            allow_agg_push_down: on(tidb_vardef::tidb_vars::TIDB_OPT_AGG_PUSH_DOWN),
            enable_3_stage_distinct_agg: on(tidb_vardef::tidb_vars::TIDB_OPT3_STAGE_DISTINCT_AGG),
            enable_3_stage_multi_distinct_agg: on(
                tidb_vardef::tidb_vars::TIDB_OPT_ENABLE3_STAGE_MULTI_DISTINCT_AGG,
            ),
            tiflash_pre_agg_mode: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIFLASH_HASH_AGG_PRE_AGG_MODE)
                .unwrap_or_else(|_| tidb_vardef::defaults::DEF_TIFLASH_PRE_AGG_MODE.to_owned()),
            partial_ordered_index_for_topn: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_OPT_PARTIAL_ORDERED_INDEX_FOR_TOP_N)
                .is_ok_and(|value| value.eq_ignore_ascii_case("COST")),
            enable_unsafe_substitute: on(tidb_vardef::tidb_vars::TIDB_ENABLE_UNSAFE_SUBSTITUTE),
            enable_force_inline_cte: on(tidb_vardef::tidb_vars::TIDB_OPT_FORCE_INLINE_CTE),
            advanced_join_hint: on(tidb_vardef::tidb_vars::TIDB_OPT_ADVANCED_JOIN_HINT),
            enable_semi_join_rewrite: on(tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE),
            allow_in_subq_to_join_and_agg: on(
                tidb_vardef::tidb_vars::TIDB_OPT_IN_SUBQ_TO_JOIN_AND_AGG,
            ),
            enable_no_decorrelate_in_select: on(
                tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT,
            ),
            enable_skew_distinct_agg: on(tidb_vardef::tidb_vars::TIDB_OPT_SKEW_DISTINCT_AGG),
            max_execution_time_ms: self.session_vars().max_execution_time(),
            advanced_join_reorder: not_off(
                tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_ADVANCED_JOIN_REORDER,
            ),
            constraint_check_in_place: on(tidb_vardef::tidb_vars::TIDB_CONSTRAINT_CHECK_IN_PLACE),
            ordering_index_selectivity_ratio: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_OPT_ORDERING_IDX_SEL_RATIO)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.01),
            ordering_index_selectivity_threshold: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_OPT_ORDERING_IDX_SEL_THRESH)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_OPT_ORDERING_IDX_SEL_THRESH),
            allow_projection_push_down: on("tidb_opt_projection_push_down"),
            enable_inl_join_inner_multi_pattern: not_off(
                tidb_vardef::tidb_vars::TIDB_ENABLE_INL_JOIN_INNER_MULTI_PATTERN,
            ),
            enable_hash_join: not_off(tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_HASH_JOIN),
            enable_null_aware_anti_join: on(tidb_vardef::tidb_vars::TIDB_OPTIMIZER_ENABLE_NAAJ),
            limit_push_down_threshold: self
                .session_vars()
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
            select_limit: self.session_vars().select_limit(),
            static_partition_prune: self
                .session_vars()
                .get_system("tidb_partition_prune_mode")
                .is_ok_and(|value| value.eq_ignore_ascii_case("static")),
            skip_missing_partition_stats: not_off(
                tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS,
            ),
            new_only_full_group_by_check: on(
                tidb_vardef::tidb_vars::TIDB_OPTIMIZER_ENABLE_NEW_ONLY_FULL_GROUP_BY_CHECK,
            ),
            auto_increment_in_generated: on("tidb_enable_auto_increment_in_generated"),
            primary_key_required: on("sql_require_primary_key"),
            tmp_table_max_size: self
                .session_vars()
                .get_system("tidb_tmp_table_max_size")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(64 << 20),
            remove_orderby_in_subquery: on(tidb_vardef::tidb_vars::TIDB_REMOVE_ORDERBY_IN_SUBQUERY),
            mem_quota: self
                .session_vars()
                .get_system("tidb_mem_quota_query")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(tidb_memory::DEF_MEM_QUOTA_QUERY),
            redact_sql: match self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_REDACT_LOG)
                .as_deref()
            {
                Ok("ON") => tidb_parser::RedactMode::Enabled,
                Ok("MARKER") => tidb_parser::RedactMode::Marker,
                _ => tidb_parser::RedactMode::Disabled,
            },
            analyze_version: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default(),
            rate_limit_action: on(tidb_vardef::tidb_vars::TIDB_ENABLE_RATE_LIMIT_ACTION),
            lock_wait_timeout_secs: self
                .session_vars()
                .get_system("innodb_lock_wait_timeout")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_else(|| {
                    u64::try_from(tidb_vardef::defaults::DEF_INNODB_LOCK_WAIT_TIMEOUT)
                        .unwrap_or_default()
                }),
            replica_read: self.session_vars().replica_read(),
            coprocessor_read_policy: tidb_distsql::remote_scan::CoprocessorReadPolicy {
                closest_read_threshold: self
                    .session_vars()
                    .get_system("tidb_adaptive_closest_read_threshold")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(4096),
                busy_threshold_ns: self
                    .session_vars()
                    .get_system("tidb_load_based_replica_read_threshold")
                    .ok()
                    .and_then(|value| tidb_config_configtypes::parse_go_duration(&value).ok())
                    .unwrap_or(1_000_000_000),
                read_timeout_ms: self
                    .session_vars()
                    .get_system("tikv_client_read_timeout")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0),
            },
            isolation_read_engines: self
                .session_vars()
                .get_system("tidb_isolation_read_engines")
                .unwrap_or_else(|_| "tikv,tiflash,tidb".to_owned()),
            init_chunk_size: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_INIT_CHUNK_SIZE)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_INIT_CHUNK_SIZE as usize),
            max_chunk_size: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_MAX_CHUNK_SIZE)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_MAX_CHUNK_SIZE as usize),
            max_allowed_packet: self.session_vars().max_allowed_packet(),
            group_concat_max_len: self
                .session_vars()
                .get_system("group_concat_max_len")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1024),
            enable_parallel_apply: on("tidb_enable_parallel_apply"),
            allow_derive_topn: on("tidb_opt_derive_topn"),
            apply_cache_capacity: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_APPLY_CACHE)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE),
            hashagg_partial_concurrency: resolved_concurrency("tidb_hashagg_partial_concurrency"),
            hashagg_final_concurrency: resolved_concurrency("tidb_hashagg_final_concurrency"),
            block_encryption_mode: self
                .session_vars()
                .get_system("block_encryption_mode")
                .ok()
                .and_then(|value| crate::BlockEncryptionMode::parse(&value))
                .unwrap_or_default(),
            ddl_cdc_write_source: self.ddl_cdc_write_source(),
            ddl_reorg_priority: match self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_DDL_REORG_PRIORITY)
                .as_deref()
            {
                Ok("PRIORITY_NORMAL") => 0,
                Ok("PRIORITY_HIGH") => 2,
                _ => 1,
            },
            ddl_session_alias: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_SESSION_ALIAS)
                .unwrap_or_default(),
            arbitrator_wait_averse: match self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_WAIT_AVERSE)
                .unwrap_or_default()
                .as_str()
            {
                "nolimit" => None,
                "1" => Some(true),
                _ => Some(false),
            },
            arbitrator_reserved: self
                .session_vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_QUERY_RESERVED)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default(),
            sql_mode,
        });
        *self.statement_var_cache_ref().borrow_mut() = Some(Arc::clone(&snapshot));
        snapshot
    }

    fn statement_context_ignoring(
        &self,
        is_dml: bool,
        ignore_err: bool,
    ) -> crate::StmtContext {
        // A transaction's reads that skip its own writes (`TABLESAMPLE`, Go
        // `GetSnapshot(TxnCtx.StartTS)`) go to the store snapshot under the
        // session's tables (`ClusterTableStorage::snapshot_first`).
        self.session_statement_context(is_dml, ignore_err)
            .with_hypo_state(
                self.hypo_indexes(),
                Arc::clone(&self.hypo_tiflash_replicas_ref()),
            )
    }

    fn session_statement_context(
        &self,
        is_dml: bool,
        ignore_err: bool,
    ) -> crate::StmtContext {
        // Go hands the same `SessionVars` to every expression, which is where
        // `DATABASE()` and `VERSION()` read from.
        let current_db = if self.session_vars().current_db.is_empty() {
            None
        } else {
            Some(self.session_vars().current_db.clone())
        };
        // The session-variable half, parsed once per variable-table
        // generation; see [`StatementVarSnapshot`].
        let snapshot = self.statement_var_snapshot();
        // This setting is instance-scoped, unlike the two cached SESSION
        // checks. A peer's SET GLOBAL must affect the next statement without
        // mutating a context that is already executing.
        let check_mb4 = self.session_vars().check_mb4_value_in_utf8();
        let string_type_flags = snapshot
            .string_type_flags
            .with_skip_utf8mb4_check(!check_mb4);
        let version = snapshot.version.clone();
        let connection_charset = snapshot.connection_charset.clone();
        let connection_collation = snapshot.connection_collation.clone();
        let default_collation_for_utf8mb4 = snapshot.default_collation_for_utf8mb4.clone();
        let zone = snapshot.time_zone.clone();
        let allow_write_row_id = snapshot.allow_write_row_id;
        let sysdate_is_now = snapshot.sysdate_is_now;
        let sql_mode = snapshot.sql_mode;
        let allow_auto_random_explicit_insert = snapshot.allow_auto_random_explicit_insert;
        let shard_allocate_step = snapshot.shard_allocate_step;
        let like_default_escape = snapshot.like_default_escape;
        let week_format = snapshot.week_format;
        let div_scale = snapshot.div_scale;
        let windowing_use_high_precision = snapshot.windowing_use_high_precision;
        let enable_pipelined_window_exec = snapshot.enable_pipelined_window_exec;
        let cte_depth = snapshot.cte_depth;
        let join_reorder_threshold = snapshot.join_reorder_threshold;
        let default_string_match_selectivity = snapshot.default_string_match_selectivity;
        let selectivity_factor = snapshot.selectivity_factor;
        let enable_pseudo_for_outdated_stats = snapshot.enable_pseudo_for_outdated_stats;
        let stats_load_sync_wait_ms = snapshot.stats_load_sync_wait_ms;
        let stats_load_pseudo_timeout = snapshot.stats_load_pseudo_timeout;
        let plan_replayer_capture_enabled = snapshot.plan_replayer_capture_enabled;
        let opt_index_prune_threshold = snapshot.opt_index_prune_threshold;
        let range_max_size = snapshot.range_max_size;
        let opt_prefix_index_single_scan = snapshot.opt_prefix_index_single_scan;
        let always_keep_join_key = snapshot.always_keep_join_key;
        let allow_agg_push_down = snapshot.allow_agg_push_down;
        let enable_3_stage_distinct_agg = snapshot.enable_3_stage_distinct_agg;
        let enable_3_stage_multi_distinct_agg = snapshot.enable_3_stage_multi_distinct_agg;
        let tiflash_pre_agg_mode = snapshot.tiflash_pre_agg_mode.clone();
        let partial_ordered_index_for_topn = snapshot.partial_ordered_index_for_topn;
        let enable_unsafe_substitute = snapshot.enable_unsafe_substitute;
        let enable_force_inline_cte = snapshot.enable_force_inline_cte;
        let advanced_join_hint = snapshot.advanced_join_hint;
        let enable_semi_join_rewrite = snapshot.enable_semi_join_rewrite;
        let allow_in_subq_to_join_and_agg =
            if self.stmt_hints_ref().has_allow_in_subq_to_join_and_agg_hint {
                self.stmt_hints_ref().allow_in_subq_to_join_and_agg
            } else {
                snapshot.allow_in_subq_to_join_and_agg
            };
        let enable_no_decorrelate_in_select = snapshot.enable_no_decorrelate_in_select;
        let enable_skew_distinct_agg = snapshot.enable_skew_distinct_agg;
        let max_execution_time_ms = if self.stmt_hints_ref().has_max_execution_time {
            self.stmt_hints_ref().max_execution_time
        } else {
            snapshot.max_execution_time_ms
        };
        let advanced_join_reorder = snapshot.advanced_join_reorder;
        let constraint_check_in_place = snapshot.constraint_check_in_place;
        let ordering_index_selectivity_ratio = snapshot.ordering_index_selectivity_ratio;
        let ordering_index_selectivity_threshold = snapshot.ordering_index_selectivity_threshold;
        let allow_projection_push_down = snapshot.allow_projection_push_down;
        let enable_inl_join_inner_multi_pattern = snapshot.enable_inl_join_inner_multi_pattern;
        let enable_hash_join = snapshot.enable_hash_join;
        let enable_null_aware_anti_join = snapshot.enable_null_aware_anti_join;
        let limit_push_down_threshold = snapshot.limit_push_down_threshold;
        let mut index_lookup_push_down_session = snapshot.index_lookup_push_down_session;
        // Transaction state is not part of the variable-table generation
        // that keys `StatementVarSnapshot`, so read it at every statement
        // boundary like Go reads `TxnCtx.IsStaleness`.
        index_lookup_push_down_session.staleness |= self.txn_is_stale_read();
        // Go attaches the domain to the session, not to selected AST kinds.
        // EXPLAIN and ordinary planning must see the same latest infoschema.
        let latest_index_schema = self.latest_index_schema_snapshot();
        let join_reorder_through_proj = snapshot.join_reorder_through_proj;
        let join_reorder_through_sel = snapshot.join_reorder_through_sel;
        let outer_join_reorder = snapshot.outer_join_reorder;
        let index_merge = snapshot.index_merge && !self.stmt_hints_ref().no_index_merge_hint;
        let static_partition_prune = snapshot.static_partition_prune;
        let skip_missing_partition_stats = snapshot.skip_missing_partition_stats;
        let new_only_full_group_by_check = snapshot.new_only_full_group_by_check;
        let remove_orderby_in_subquery = snapshot.remove_orderby_in_subquery;
        let mem_quota = if self.stmt_hints_ref().has_mem_quota_hint {
            self.stmt_hints_ref().mem_quota_query
        } else {
            snapshot.mem_quota
        };
        let replica_read = self.effective_replica_read(snapshot.replica_read);
        // Go's stale provider selects the instance scope independently of the
        // replica preference. tidb_snapshot uses the ordinary provider even
        // though this adapter shares the historical catalog implementation.
        let is_staleness = self.session_vars().snapshot_ts() == 0 && self.txn_is_stale_read();
        let read_replica_scope = if is_staleness || replica_read.is_closest_read() {
            tidb_config::config_tree::config::get_txn_scope_from_config()
        } else {
            "global".into()
        };
        let isolation_read_engines = snapshot.isolation_read_engines.clone();
        let max_allowed_packet = snapshot.max_allowed_packet;
        let group_concat_max_len = snapshot.group_concat_max_len;
        let apply_cache_capacity = snapshot.apply_cache_capacity;
        let enable_parallel_apply = snapshot.enable_parallel_apply;
        let allow_derive_topn = snapshot.allow_derive_topn;
        let hashagg_partial_concurrency = snapshot.hashagg_partial_concurrency;
        let hashagg_final_concurrency = snapshot.hashagg_final_concurrency;
        let block_encryption_mode = snapshot.block_encryption_mode;
        let global_sysvar_accessor = self.session_vars().global_sysvar_accessor();
        let (oom_action, tmp_storage_on_oom) = self.session_vars().statement_memory_policy();
        let optimizer_cost_env = self.optimizer_cost_env(mem_quota, tmp_storage_on_oom);
        let result_authority = self.build_statement_result_authority(
            &snapshot,
            mem_quota,
            oom_action,
            tmp_storage_on_oom,
        );
        let statement_memory = result_authority.statement_memory();
        // Go `ResetContextOfStmt` checks this process-wide instance switch on
        // every statement and clears the statement collector while it is OFF.
        // The session collector may outlive a toggle, so its presence alone
        // must not decide whether this statement reports index usage.
        let collect_execution_info = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_COLLECT_EXECUTION_INFO)
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"));
        let runtime_stats_coll = collect_execution_info
            .then(|| Arc::new(tidb_execdetails::runtime_stats::RuntimeStatsColl::new(None)));
        let runtime_stats_source: Option<
            Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>,
        > = runtime_stats_coll.clone().map(|coll| {
            coll as Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>
        });
        // Go `selectResult.updateCopRuntimeStats` feeds the statement's
        // `RuntimeStatsColl` with every response it reads.
        let cop_response_sink: Option<Arc<dyn crate::CopResponseSink>> =
            match &runtime_stats_coll {
                Some(coll) => Some(Arc::new(
                    crate::statement_cop_stats::RuntimeStatsCollSink::new(
                        Arc::clone(coll),
                        self.statement_cop_response_sink(),
                    ),
                )),
                None => self.statement_cop_response_sink(),
            };
        let index_usage_collector = if collect_execution_info {
            self.session_index_usage_collector_ref().as_ref().map(|session| {
                Arc::new(
                    tidb_stats_handle_usage_indexusage::StmtIndexUsageCollector::new(Arc::clone(
                        session,
                    )),
                )
            })
        } else {
            None
        };
        self.statement_result_authority_ref()
            .replace(Some(result_authority));
        let session_state = crate::StmtContextSessionState {
            mpp_query_info: Arc::clone(&self.mpp_query_info_ref()),
            mpp_server_id: (self.server_id_getter_ref())(),
            tiflash_compute_dispatch_policy: self
                .session_vars()
                .get_system("tiflash_compute_dispatch_policy")
                .unwrap_or_else(|_| "consistent_hash".into()),
            allow_tiflash_fallback: self
                .session_vars()
                .get_system("tidb_allow_fallback_to_tikv")
                .is_ok_and(|value| {
                    value
                        .split(',')
                        .any(|engine| engine.trim().eq_ignore_ascii_case("tiflash"))
                }),
            advisory_locks: self.advisory_locks_ref().clone(),
            before_executor_first_run: Arc::clone(&self.executor_first_run_breakpoint_ref()),
            breakpoint_notify_func: self.breakpoint_notify_func(),
            last_insert_id: Arc::clone(&self.published_last_insert_id_ref()),
            current_tso: self.current_tso(),
            staged_writes: if self.has_statement_snapshot() {
                std::sync::Arc::default()
            } else {
                std::sync::Arc::clone(&self.staged_writes_ref())
            },
            retry_auto_ids: Arc::clone(&self.retry_auto_ids_ref()),
            row_id_shards: Arc::clone(&self.row_id_shards_ref()),
            planned_apply: Arc::clone(&self.planned_apply_ref()),
            sequences: Some(self.sequence_snapshot()),
            sequence_state: Arc::clone(self.sequence_last_values_ref()),
            resource_group_name: self.active_resource_group().to_owned(),
            isolation_read_engines,
            select_limit: snapshot.select_limit,
            connection_charset,
            connection_collation,
            default_collation_for_utf8mb4,
            sequence_privilege: self.sequence_privilege_check(),
            view_definer_privilege: self.view_definer_privilege_check(),
            session_privilege: self.session_privilege_check(),
            mdl_related_table: self.mdl_related_table_recorder(),
            process_privilege: self.has_process_privilege(),
            sql_digest_text_retriever: Some(self.sql_digest_text_retrieval()),
            locked_tables: self.locked_tables(),
            ddl_sql_mode: sql_mode.0,
            regions: self.region_store(),
        };
        if !is_dml {
            return crate::StmtContext::for_query_with_session(
                statement_memory,
                session_state,
            )
            .configure(|ctx| {
                let _ = ctx
                    .with_executor_chunk_sizes(snapshot.init_chunk_size, snapshot.max_chunk_size)
                    // A read's error levels do not depend on the mode, but DDL
                    // takes this same context and Go's DDL checks DO read
                    // `SQLMode.HasStrictMode()`. See `StmtContext::with_strict`.
                    .with_strict(sql_mode.has_strict_mode())
                    // Go's DDL reads `@@tidb_allow_remove_auto_inc` from the
                    // session (`GetModifiableColumnJob`), whatever context
                    // the statement takes.
                    .with_allow_remove_auto_inc(self.allow_remove_auto_inc())
                    .with_string_type_flags(string_type_flags)
                    .with_cte_max_recursion_depth(cte_depth)
                    .with_join_reorder_threshold(join_reorder_threshold)
                    .with_advanced_join_reorder(advanced_join_reorder)
                    .with_allow_agg_push_down(allow_agg_push_down)
                    .with_enable_3_stage_distinct_agg(enable_3_stage_distinct_agg)
                    .with_enable_3_stage_multi_distinct_agg(enable_3_stage_multi_distinct_agg)
                    .with_tiflash_pre_agg_mode(tiflash_pre_agg_mode.clone())
                    .with_partial_ordered_index_for_topn(partial_ordered_index_for_topn)
                    .with_ordering_index_selectivity_ratio(ordering_index_selectivity_ratio)
                    .with_ordering_index_selectivity_threshold(ordering_index_selectivity_threshold)
                    .with_projection_push_down(allow_projection_push_down)
                    .with_inl_join_inner_multi_pattern(enable_inl_join_inner_multi_pattern)
                    .with_enable_hash_join(enable_hash_join)
                    .with_enable_null_aware_anti_join(enable_null_aware_anti_join)
                    .with_limit_push_down_threshold(limit_push_down_threshold)
                    .with_index_lookup_push_down_session(index_lookup_push_down_session)
                    .with_optimizer_fix_control(self.session_vars().optimizer_fix_control().clone())
                    .with_optimizer_cost_env(optimizer_cost_env.clone())
                    .with_enable_vectorized_expression(
                        self.session_vars()
                            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_VECTORIZED_EXPRESSION)
                            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON")),
                    )
                    .with_hashagg_concurrency(
                        hashagg_partial_concurrency,
                        hashagg_final_concurrency,
                    )
                    .with_join_reorder_through_proj(join_reorder_through_proj)
                    .with_join_reorder_through_sel(join_reorder_through_sel)
                    .with_outer_join_reorder(outer_join_reorder)
                    .with_index_merge(index_merge)
                    .with_no_index_merge_hint(self.stmt_hints_ref().no_index_merge_hint)
                    .with_pushdown_blacklists(self.pushdown_blacklists().snapshot())
                    .with_stats_handle(self.stats_handle())
                    .with_process_plan_info_sink(Arc::clone(&self.process_plan_info_ref()))
                    .with_statement_phase_observer(self.statement_phase_observer())
                    .with_phase_durations(self.phase_durations_ref().clone())
                    .with_sql_cpu_usages(Some(Arc::clone(&self.sql_cpu_usages_ref())))
                    .with_kv_exec_counter(self.statement_kv_exec_counter())
                    .with_statement_kv_context(Some(self.statement_kv_context_cell()))
                    .with_cop_response_sink(cop_response_sink.clone())
                    .with_keys_read(crate::KeysReadBudget::new(
                        Arc::clone(&self.statement_keys_read_ref()),
                        self.session_vars().max_keys_read(*self.statement_in_select_ref()),
                    ))
                    .with_enable_stable_result_mode(snapshot.stable_result_mode)
                    .with_ignore_inlist_plan_digest(snapshot.ignore_inlist_plan_digest)
                    .with_brief_binary_plan(!self.binary_prepared_execution_ref())
                    .with_allow_write_row_id(allow_write_row_id)
                    .with_static_partition_prune(static_partition_prune)
                    .with_skip_missing_partition_stats(skip_missing_partition_stats)
                    .with_new_only_full_group_by_check(new_only_full_group_by_check)
                    .with_auto_increment_in_generated(snapshot.auto_increment_in_generated)
                    .with_primary_key_required(
                        !self.session_vars().restricted_sql && snapshot.primary_key_required,
                    )
                    .with_tmp_table_max_size(snapshot.tmp_table_max_size)
                    .with_remove_orderby_in_subquery(remove_orderby_in_subquery)
                    .with_session_state(current_db, version)
                    .with_user(self.session_vars().current_user.clone(), self.session_vars().login_user.clone())
                    .with_global_sysvar_accessor(Arc::clone(&global_sysvar_accessor))
                    .with_active_roles(
                        self.session_vars().current_user
                            .as_ref()
                            .map(|_| Arc::clone(&self.session_vars().active_roles)),
                    )
                    .with_connection_id(self.session_vars().connection_id)
                    .with_selected_lock_keys(self.selected_lock_keys_ref().clone())
                    .with_pessimistic_transaction(self.statement_is_pessimistic())
                    .with_in_transaction(!self.is_autocommit() || self.in_transaction())
                    .with_optimistic_transaction(
                        (!self.is_autocommit() || self.in_transaction())
                            && !self.statement_txn_mode().is_pessimistic(),
                    )
                    .with_rand_session(Arc::clone(self.rand_ref()))
                    .with_auto_random_policy(allow_auto_random_explicit_insert, shard_allocate_step)
                    .with_user_vars(self.user_vars().clone())
                    .with_previous_statement(self.session_vars().last_insert_id, self.session_vars().prev_row_count)
                    .with_last_found_rows(self.session_vars().last_found_rows)
                    .with_client_found_rows(self.session_vars().client_found_rows)
                    .with_week_and_division_scale(week_format, div_scale)
                    .with_windowing_use_high_precision(windowing_use_high_precision)
                    .with_pipelined_window_exec(enable_pipelined_window_exec)
                    .with_max_allowed_packet(max_allowed_packet)
                    .with_group_concat_max_len(group_concat_max_len)
                    .with_apply_cache_capacity(apply_cache_capacity)
                    .with_parallel_apply(enable_parallel_apply)
                    .with_allow_derive_topn(allow_derive_topn)
                    .with_allow_derive_topn(allow_derive_topn)
                    .with_block_encryption_mode(block_encryption_mode)
                    .with_decode_key_from_string(self.decode_key_from_string())
                    .with_parser_config(self.session_vars().build_parser_switches())
                    .with_ddl_job_context(
                        snapshot.ddl_cdc_write_source,
                        snapshot.ddl_reorg_priority,
                        snapshot.ddl_session_alias.clone(),
                        Vec::new(),
                    )
                    .with_like_default_escape(like_default_escape)
                    .with_default_string_match_selectivity(default_string_match_selectivity)
                    .with_selectivity_factor(selectivity_factor)
                    .with_pseudo_for_outdated_stats(enable_pseudo_for_outdated_stats)
                    .with_stats_load_policy(
                        stats_load_sync_wait_ms,
                        stats_load_pseudo_timeout,
                        max_execution_time_ms,
                    )
                    .with_plan_replayer_capture(plan_replayer_capture_enabled)
                    .with_column_stats_usage(col_stats_usage_updater(self.stats_collector_ref()))
                    .with_index_usage_collector(index_usage_collector)
                    .with_executor_runtime_stats_source(runtime_stats_source.clone())
                    .with_table_delta(std::sync::Arc::clone(&self.transaction_table_delta_ref()))
                    .with_opt_index_prune_threshold(opt_index_prune_threshold)
                    .with_range_max_size(range_max_size)
                    .with_opt_prefix_index_single_scan(opt_prefix_index_single_scan)
                    .with_always_keep_join_key(always_keep_join_key)
                    .with_enable_unsafe_substitute(enable_unsafe_substitute)
                    .with_enable_force_inline_cte(enable_force_inline_cte)
                    .with_advanced_join_hint(advanced_join_hint)
                    .with_enable_semi_join_rewrite(enable_semi_join_rewrite)
                    .with_allow_in_subq_to_join_and_agg(allow_in_subq_to_join_and_agg)
                    .with_enable_no_decorrelate_in_select(enable_no_decorrelate_in_select)
                    .with_enable_skew_distinct_agg(enable_skew_distinct_agg)
                    .with_query_cop_store_limiter(tidb_txnkv::new_query_cop_store_limiter(
                        self.session_vars().query_cop_store_limit() as isize,
                    ))
                    .with_enable_check_constraint(self.enable_check_constraint())
                    .with_sysdate_is_now(sysdate_is_now)
                    .with_replica_read(replica_read)
                    .with_read_consistency(read_replica_scope, is_staleness)
                    .with_coprocessor_read_policy(snapshot.coprocessor_read_policy.clone())
                    .with_dist_sql_scan_concurrency(
                        self.session_vars()
                            .get_system(tidb_vardef::tidb_vars::TIDB_DIST_SQL_SCAN_CONCURRENCY)
                            .ok()
                            .and_then(|value| value.parse::<u64>().ok())
                            .unwrap_or(tidb_vardef::defaults::DEF_DIST_SQL_SCAN_CONCURRENCY as u64),
                    )
                    .with_store_batch_size(
                        self.session_vars()
                            .get_system(tidb_vardef::tidb_vars::TIDB_STORE_BATCH_SIZE)
                            .ok()
                            .and_then(|value| value.parse::<u64>().ok())
                            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_STORE_BATCH_SIZE as u64),
                    )
                    .with_tiflash_replica_read(tidb_tiflash::get_tiflash_replica_read_by_str(
                        &self
                            .session_vars()
                            .get_system(tidb_vardef::tidb_vars::TIFLASH_REPLICA_READ)
                            .unwrap_or_default(),
                    ))
                    .with_lazy_clock(snapshot.timestamp, zone);
                if let Some(latest_index_schema) = latest_index_schema {
                    let _ = ctx.with_latest_info_schema(crate::LatestInfoSchema::new(latest_index_schema));
                }
                if let Some(parameters) = &self.prepared_params_ref() {
                    let _ = ctx.with_prepared_params(Arc::clone(parameters));
                }
            });
        }
        let (increment, offset) = self.auto_increment_step();
        crate::StmtContext::for_dml_with_session(
            sql_mode.has_error_for_division_by_zero_mode(),
            sql_mode.has_strict_mode(),
            ignore_err,
            statement_memory,
            session_state,
        )
        .configure(|ctx| {
            let _ = ctx
                .with_executor_chunk_sizes(snapshot.init_chunk_size, snapshot.max_chunk_size)
                .with_string_type_flags(string_type_flags)
                .with_stats_handle(self.stats_handle())
                .with_process_plan_info_sink(Arc::clone(&self.process_plan_info_ref()))
                .with_statement_phase_observer(self.statement_phase_observer())
                .with_phase_durations(self.phase_durations_ref().clone())
                .with_sql_cpu_usages(Some(Arc::clone(&self.sql_cpu_usages_ref())))
                .with_kv_exec_counter(self.statement_kv_exec_counter())
                .with_statement_kv_context(Some(self.statement_kv_context_cell()))
                .with_cop_response_sink(cop_response_sink.clone())
                .with_keys_read(crate::KeysReadBudget::new(
                    Arc::clone(&self.statement_keys_read_ref()),
                    self.session_vars().max_keys_read(*self.statement_in_select_ref()),
                ))
                .with_enable_stable_result_mode(snapshot.stable_result_mode)
                .with_ignore_inlist_plan_digest(snapshot.ignore_inlist_plan_digest)
                .with_brief_binary_plan(!self.binary_prepared_execution_ref())
                .with_allow_write_row_id(allow_write_row_id)
                .with_new_only_full_group_by_check(new_only_full_group_by_check)
                .with_auto_increment_in_generated(snapshot.auto_increment_in_generated)
                .with_primary_key_required(
                    !self.session_vars().restricted_sql && snapshot.primary_key_required,
                )
                .with_tmp_table_max_size(snapshot.tmp_table_max_size)
                .with_remove_orderby_in_subquery(remove_orderby_in_subquery)
                .with_session_state(current_db, version)
                .with_user(self.session_vars().current_user.clone(), self.session_vars().login_user.clone())
                .with_global_sysvar_accessor(global_sysvar_accessor)
                .with_active_roles(
                    self.session_vars().current_user
                        .as_ref()
                        .map(|_| Arc::clone(&self.session_vars().active_roles)),
                )
                .with_connection_id(self.session_vars().connection_id)
                .with_selected_lock_keys(self.selected_lock_keys_ref().clone())
                .with_pessimistic_transaction(self.statement_is_pessimistic())
                .with_in_transaction(!self.is_autocommit() || self.in_transaction())
                .with_optimistic_transaction(
                    (!self.is_autocommit() || self.in_transaction())
                        && !self.statement_txn_mode().is_pessimistic(),
                )
                .with_rand_session(Arc::clone(self.rand_ref()))
                .with_auto_random_policy(allow_auto_random_explicit_insert, shard_allocate_step)
                .with_user_vars(self.user_vars().clone())
                .with_previous_statement(self.session_vars().last_insert_id, self.session_vars().prev_row_count)
                .with_last_found_rows(self.session_vars().last_found_rows)
                .with_client_found_rows(self.session_vars().client_found_rows)
                .with_week_and_division_scale(week_format, div_scale)
                .with_windowing_use_high_precision(windowing_use_high_precision)
                .with_pipelined_window_exec(enable_pipelined_window_exec)
                .with_max_allowed_packet(max_allowed_packet)
                .with_group_concat_max_len(group_concat_max_len)
                .with_apply_cache_capacity(apply_cache_capacity)
                .with_parallel_apply(enable_parallel_apply)
                .with_block_encryption_mode(block_encryption_mode)
                .with_decode_key_from_string(self.decode_key_from_string())
                .with_sysdate_is_now(sysdate_is_now)
                .with_replica_read(crate::ReplicaReadType::Leader)
                .with_coprocessor_read_policy(snapshot.coprocessor_read_policy.clone())
                .with_lazy_clock(snapshot.timestamp, zone)
                .with_parser_config(self.session_vars().build_parser_switches())
                .with_ddl_job_context(
                    snapshot.ddl_cdc_write_source,
                    snapshot.ddl_reorg_priority,
                    snapshot.ddl_session_alias.clone(),
                    Vec::new(),
                )
                .with_like_default_escape(like_default_escape)
                .with_default_string_match_selectivity(default_string_match_selectivity)
                .with_selectivity_factor(selectivity_factor)
                .with_pseudo_for_outdated_stats(enable_pseudo_for_outdated_stats)
                .with_stats_load_policy(
                    stats_load_sync_wait_ms,
                    stats_load_pseudo_timeout,
                    max_execution_time_ms,
                )
                .with_plan_replayer_capture(plan_replayer_capture_enabled)
                .with_column_stats_usage(col_stats_usage_updater(self.stats_collector_ref()))
                .with_index_usage_collector(index_usage_collector)
                .with_executor_runtime_stats_source(runtime_stats_source.clone())
                .with_table_delta(std::sync::Arc::clone(&self.transaction_table_delta_ref()))
                .with_opt_index_prune_threshold(opt_index_prune_threshold)
                .with_range_max_size(range_max_size)
                .with_opt_prefix_index_single_scan(opt_prefix_index_single_scan)
                .with_always_keep_join_key(always_keep_join_key)
                .with_enable_unsafe_substitute(enable_unsafe_substitute)
                .with_enable_force_inline_cte(enable_force_inline_cte)
                .with_advanced_join_hint(advanced_join_hint)
                .with_enable_semi_join_rewrite(enable_semi_join_rewrite)
                .with_allow_in_subq_to_join_and_agg(allow_in_subq_to_join_and_agg)
                .with_enable_no_decorrelate_in_select(enable_no_decorrelate_in_select)
                .with_enable_skew_distinct_agg(enable_skew_distinct_agg)
                .with_query_cop_store_limiter(tidb_txnkv::new_query_cop_store_limiter(
                    self.session_vars().query_cop_store_limit() as isize,
                ))
                .with_auto_increment_step(increment, offset)
                .with_foreign_key_checks(self.foreign_key_checks())
                .with_enable_check_constraint(self.enable_check_constraint())
                .with_constraint_check_in_place(constraint_check_in_place)
                .with_pessimistic_check_in_prewrite(
                    !self.pessimistic_constraint_check_in_place()
                        && self.in_transaction()
                        && !self.session_vars().restricted_sql
                        && self.session_vars().connection_id.is_some_and(|id| id > 0),
                )
                // Go `optimizeDupKeyCheckForNormalInsert` + `getPessimisticLazyCheckMode`
                // (`pkg/executor/insert.go:331-337,347-350`): normal INSERT uses
                // `DupKeyCheckLazy` whenever constraint checks are disabled OR the
                // statement transaction is pessimistic. The Go auto-commit path still
                // opens a pessimistic transaction under the default `tidb_txn_mode`,
                // so the Rust context must carry that mode before `Txn()` is opened.
                .with_pessimistic_lazy_dup_check(
                    self.statement_txn_mode().is_pessimistic()
                        && self.session_vars().connection_id.is_some_and(|id| id > 0),
                )
                .with_allow_remove_auto_inc(self.allow_remove_auto_inc())
                .with_cte_max_recursion_depth(cte_depth)
                .with_join_reorder_threshold(join_reorder_threshold)
                .with_advanced_join_reorder(advanced_join_reorder)
                .with_allow_agg_push_down(allow_agg_push_down)
                .with_enable_3_stage_distinct_agg(enable_3_stage_distinct_agg)
                .with_enable_3_stage_multi_distinct_agg(enable_3_stage_multi_distinct_agg)
                .with_tiflash_pre_agg_mode(tiflash_pre_agg_mode.clone())
                .with_partial_ordered_index_for_topn(partial_ordered_index_for_topn)
                .with_ordering_index_selectivity_ratio(ordering_index_selectivity_ratio)
                .with_ordering_index_selectivity_threshold(ordering_index_selectivity_threshold)
                .with_projection_push_down(allow_projection_push_down)
                .with_inl_join_inner_multi_pattern(enable_inl_join_inner_multi_pattern)
                .with_enable_hash_join(enable_hash_join)
                .with_enable_null_aware_anti_join(enable_null_aware_anti_join)
                .with_limit_push_down_threshold(limit_push_down_threshold)
                .with_index_lookup_push_down_session(index_lookup_push_down_session)
                .with_optimizer_fix_control(self.session_vars().optimizer_fix_control().clone())
                .with_optimizer_cost_env(optimizer_cost_env)
                .with_enable_vectorized_expression(
                    self.session_vars()
                        .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_VECTORIZED_EXPRESSION)
                        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("ON")),
                )
                .with_hashagg_concurrency(hashagg_partial_concurrency, hashagg_final_concurrency)
                .with_join_reorder_through_proj(join_reorder_through_proj)
                .with_join_reorder_through_sel(join_reorder_through_sel)
                .with_outer_join_reorder(outer_join_reorder)
                .with_index_merge(index_merge)
                .with_no_index_merge_hint(self.stmt_hints_ref().no_index_merge_hint)
                .with_static_partition_prune(static_partition_prune)
                .with_skip_missing_partition_stats(skip_missing_partition_stats);
            if let Some(parameters) = &self.prepared_params_ref() {
                let _ = ctx.with_prepared_params(Arc::clone(parameters));
            }
            if let Some(latest_index_schema) = latest_index_schema {
                let _ = ctx.with_latest_info_schema(crate::LatestInfoSchema::new(latest_index_schema));
            }
        })
    }

    /// Go `SessionVars.ForeignKeyChecks`, read off `@@foreign_key_checks`.
    /// The registry stores a boolean as `ON`/`OFF`, and an unreadable value
    /// falls back to the ON default rather than silently disabling the
    /// checks.
    fn foreign_key_checks(&self) -> bool {
        !matches!(
            self.session_vars().get_system("foreign_key_checks").as_deref(),
            Ok("OFF") | Ok("off") | Ok("0")
        )
    }

    /// Go `SessionVars.AllowRemoveAutoInc`, read off
    /// `@@tidb_allow_remove_auto_inc`. The default is OFF, and unlike
    /// `foreign_key_checks` the safe fallback for an unreadable value is OFF:
    /// dropping AUTO_INCREMENT is the destructive direction.
    fn allow_remove_auto_inc(&self) -> bool {
        matches!(
            self.session_vars()
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
    fn enable_check_constraint(&self) -> bool {
        self.session_vars().enable_check_constraint()
    }

    /// Go `SessionVars.EnableClusteredIndex`, fed to `BuildTableInfo` through
    /// `metabuild.WithClusteredIndexDefMode` (`pkg/ddl/metabuild.go`).
    ///
    /// The variable is `SESSION | GLOBAL` and an ENUM of `OFF`/`ON`/`INT_ONLY`
    /// -- not a boolean -- so it is read with the session's own value and
    /// converted by Go's own `TiDBOptEnableClustered`, which maps anything
    /// that is neither `ON` nor `OFF` (including an unreadable value) onto
    /// `INT_ONLY`. The registered default is `ON`.
    fn clustered_index_mode(&self) -> tidb_vardef::modes::ClusteredIndexDefMode {
        // `check_enum` stores the canonical `OFF`/`ON`/`INT_ONLY` spelling
        // whatever the user typed, so this compares against it exactly as Go
        // does rather than re-normalizing here.
        match self.session_vars().get_system("tidb_enable_clustered_index") {
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
    fn auto_increment_step(&self) -> (u64, u64) {
        let read = |name: &str| {
            self.session_vars()
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
    fn publish_statement_status(
        &mut self,
        result: &Result<StatementCompletion, DriverError>,
    ) {
        // The publication outlives a failing statement, exactly as Go's
        // `StmtCtx.LastInsertID` does: `SELECT LAST_INSERT_ID(17), bad()`
        // fails and still moves the id (captured).
        let published = *self
            .published_last_insert_id_ref()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(published) = published {
            self.session_vars_mut().last_insert_id = published;
        }
        if let Ok(StatementCompletion::Rows(Some(rows))) = result {
            self.session_vars_mut().last_found_rows = *rows;
        }
        // Go preserves StmtCtx.PrevAffectedRows across SET SESSION_STATES.
        if self.session_vars().in_set_session_states_stmt {
            return;
        }
        self.session_vars_mut().prev_row_count = match self.statement_kind_ref() {
            StatementKind::Select => -1,
            // Go reads `StmtCtx.AffectedRows()`, which a failed statement
            // leaves at whatever it managed to apply -- 0 for a statement
            // that never reached a row.
            StatementKind::Dml => match result {
                Ok(StatementCompletion::Affected(rows)) => {
                    i64::try_from(*rows).unwrap_or(i64::MAX)
                }
                // `EXPLAIN ANALYZE` of a write answers rows; the write added
                // its affected rows to the statement context.
                Ok(StatementCompletion::Rows(_)) => {
                    i64::try_from(*self.statement_affected_rows_ref()).unwrap_or(i64::MAX)
                }
                _ => 0,
            },
            StatementKind::Other => 0,
        };
    }
}

impl<T: crate::stmt_ctx_build::StmtCtxSession> ResetContextOfStmt for T {}

pub enum StatementCompletion {
    Rows(Option<u64>),
    Affected(u64),
    Done,
}

impl From<&StmtOutput> for StatementCompletion {
    fn from(output: &StmtOutput) -> Self {
        match output {
            StmtOutput::Rows { rows, .. } => Self::Rows(Some(rows.len() as u64)),
            StmtOutput::Affected(rows) => Self::Affected(*rows),
            StmtOutput::Done(_) => Self::Done,
        }
    }
}

/// The statement-owned policy a server needs to retain an eager result set.
///
/// It is captured before `SET_VAR` overlays are restored, so a prepared
/// cursor uses the same quota, OOM action, temporary-storage decision, and
/// chunk bound as the statement that produced its rows.
#[derive(Clone)]
pub struct ResultMaterializationAuthority {
    current_tso: Option<crate::CurrentTso>,
    start_ts_guard: Option<Arc<tidb_txnkv::StartTsGuard>>,
    memory: crate::StatementMemory,
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
        memory: crate::StatementMemory,
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
    pub fn with_current_tso(self, current: crate::CurrentTso) -> Self {
        let mut authority = self.with_start_ts(current.value() as u64);
        authority.current_tso = Some(current);
        authority
    }

    /// Transfers the lazy activation handle to the cursor materializer.
    pub fn take_current_tso(&mut self) -> Option<crate::CurrentTso> {
        self.current_tso.take()
    }

    /// Transfers the pin to the connection-owned cursor without releasing it.
    pub fn take_start_ts_guard(&mut self) -> Option<Arc<tidb_txnkv::StartTsGuard>> {
        self.start_ts_guard.take()
    }

    /// Consumes the authority into its retained memory policy and chunk bounds.
    #[must_use]
    pub fn into_parts(self) -> (crate::StatementMemory, usize, usize) {
        (self.memory, self.init_chunk_size, self.max_chunk_size)
    }

    /// The statement tracker retained by this authority.
    #[must_use]
    pub fn statement_memory(&self) -> crate::StatementMemory {
        self.memory.clone()
    }
}

/// The session state Go's `ResetContextOfStmt` and the statement-context
/// builders read: Go's `sctx` and the `SessionVars` fields this tier still
/// keeps on the session.
pub trait StmtCtxSession: crate::sessionctx::SessionContext + Sized {
    /// Go `domain.GetDomain(ctx).StatsHandle()`.
    fn stats_handle(&self) -> std::sync::Arc<tidb_stats_handle::StatsHandle>;
    /// Go `SessionVars.ResourceGroupName`.
    fn active_resource_group(&self) -> &str;
    /// Go `domain.GetDomain(ctx).InfoSchema()`: the domain's latest image.
    fn domain_info_schema(&self) -> std::sync::Arc<std::sync::Mutex<crate::Catalog>>;
    /// Go `tikv.Storage`'s region cache and PD client.
    fn region_store(&self) -> std::sync::Arc<dyn tidb_kv::region_map::RegionStore>;
    /// The server's published pushdown and optimizer-rule blacklists.
    fn pushdown_blacklists(&self) -> &crate::blacklist::PushdownBlacklists;
    /// Go `SessionVars.HypoIndexes`.
    fn hypo_indexes(&self) -> Arc<HypoIndexes>;
    fn has_statement_snapshot(&self) -> bool;
    /// The pinned snapshot timestamp (`tidb_snapshot`) the statement reads at.
    fn statement_snapshot_ts(&self) -> Option<u64>;
    /// Go `SessionVars` / `StmtCtx` state `advisory_locks`, read.
    fn advisory_locks_ref(&self) -> &tidb_sessionctx::advisory_lock_state::AdvisoryLockSession;
    /// Go `SessionVars` / `StmtCtx` state `advisory_locks`, mutably.
    fn advisory_locks_mut(&mut self) -> &mut tidb_sessionctx::advisory_lock_state::AdvisoryLockSession;
    /// Go `SessionVars` / `StmtCtx` state `binary_prepared_execution`, read.
    fn binary_prepared_execution_ref(&self) -> &bool;
    /// Go `SessionVars` / `StmtCtx` state `binary_prepared_execution`, mutably.
    fn binary_prepared_execution_mut(&mut self) -> &mut bool;
    /// Go `SessionVars` / `StmtCtx` state `cost_env_cache`, read.
    fn cost_env_cache_ref(&self) -> &std::cell::RefCell<Option<(u64, Arc<tidb_planner::find_best_task::coster::CostEnv>)>>;
    /// Go `SessionVars` / `StmtCtx` state `cost_env_cache`, mutably.
    fn cost_env_cache_mut(&mut self) -> &mut std::cell::RefCell<Option<(u64, Arc<tidb_planner::find_best_task::coster::CostEnv>)>>;
    /// Go `SessionVars` / `StmtCtx` state `current_sql_digest_key`, read.
    fn current_sql_digest_key_ref(&self) -> &String;
    /// Go `SessionVars` / `StmtCtx` state `current_sql_digest_key`, mutably.
    fn current_sql_digest_key_mut(&mut self) -> &mut String;
    /// Go `SessionVars` / `StmtCtx` state `executor_first_run_breakpoint`, read.
    fn executor_first_run_breakpoint_ref(&self) -> &Arc<std::sync::atomic::AtomicBool>;
    /// Go `SessionVars` / `StmtCtx` state `executor_first_run_breakpoint`, mutably.
    fn executor_first_run_breakpoint_mut(&mut self) -> &mut Arc<std::sync::atomic::AtomicBool>;
    /// Go `SessionVars` / `StmtCtx` state `hypo_tiflash_replicas`, read.
    fn hypo_tiflash_replicas_ref(&self) -> &Arc<tidb_variable::hypo_index::HypoTiFlashReplicas>;
    /// Go `SessionVars` / `StmtCtx` state `hypo_tiflash_replicas`, mutably.
    fn hypo_tiflash_replicas_mut(&mut self) -> &mut Arc<tidb_variable::hypo_index::HypoTiFlashReplicas>;
    /// Go `SessionVars` / `StmtCtx` state `local_temporary_tables`, read.
    fn local_temporary_tables_ref(&self) -> &Vec<(String, String, crate::KvTable)>;
    /// Go `SessionVars` / `StmtCtx` state `local_temporary_tables`, mutably.
    fn local_temporary_tables_mut(&mut self) -> &mut Vec<(String, String, crate::KvTable)>;
    /// Go `SessionVars` / `StmtCtx` state `mpp_query_info`, read.
    fn mpp_query_info_ref(&self) -> &Arc<crate::MppQueryInfo>;
    /// Go `SessionVars` / `StmtCtx` state `mpp_query_info`, mutably.
    fn mpp_query_info_mut(&mut self) -> &mut Arc<crate::MppQueryInfo>;
    /// Go `SessionVars` / `StmtCtx` state `phase_durations`, read.
    fn phase_durations_ref(&self) -> &tidb_variable::phase_durations::SessionPhaseDurations;
    /// Go `SessionVars` / `StmtCtx` state `phase_durations`, mutably.
    fn phase_durations_mut(&mut self) -> &mut tidb_variable::phase_durations::SessionPhaseDurations;
    /// Go `SessionVars` / `StmtCtx` state `planned_apply`, read.
    fn planned_apply_ref(&self) -> &Arc<std::sync::atomic::AtomicBool>;
    /// Go `SessionVars` / `StmtCtx` state `planned_apply`, mutably.
    fn planned_apply_mut(&mut self) -> &mut Arc<std::sync::atomic::AtomicBool>;
    /// Go `SessionVars` / `StmtCtx` state `prepared_params`, read.
    fn prepared_params_ref(&self) -> &Option<Arc<[Datum]>>;
    /// Go `SessionVars` / `StmtCtx` state `prepared_params`, mutably.
    fn prepared_params_mut(&mut self) -> &mut Option<Arc<[Datum]>>;
    /// Go `SessionVars` / `StmtCtx` state `process_plan_info`, read.
    fn process_plan_info_ref(&self) -> &Arc<std::sync::Mutex<crate::ProcessPlanInfo>>;
    /// Go `SessionVars` / `StmtCtx` state `process_plan_info`, mutably.
    fn process_plan_info_mut(&mut self) -> &mut Arc<std::sync::Mutex<crate::ProcessPlanInfo>>;
    /// Go `SessionVars` / `StmtCtx` state `published_last_insert_id`, read.
    fn published_last_insert_id_ref(&self) -> &Arc<std::sync::Mutex<Option<u64>>>;
    /// Go `SessionVars` / `StmtCtx` state `published_last_insert_id`, mutably.
    fn published_last_insert_id_mut(&mut self) -> &mut Arc<std::sync::Mutex<Option<u64>>>;
    /// Go `SessionVars` / `StmtCtx` state `rand`, read.
    fn rand_ref(&self) -> &Arc<MysqlRng>;
    /// Go `SessionVars` / `StmtCtx` state `rand`, mutably.
    fn rand_mut(&mut self) -> &mut Arc<MysqlRng>;
    /// Go `SessionVars` / `StmtCtx` state `retry_auto_ids`, read.
    fn retry_auto_ids_ref(&self) -> &Arc<std::sync::Mutex<crate::RetryAutoIds>>;
    /// Go `SessionVars` / `StmtCtx` state `retry_auto_ids`, mutably.
    fn retry_auto_ids_mut(&mut self) -> &mut Arc<std::sync::Mutex<crate::RetryAutoIds>>;
    /// Go `SessionVars` / `StmtCtx` state `row_id_shards`, read.
    fn row_id_shards_ref(&self) -> &Arc<std::sync::Mutex<crate::RowIdShardGenerator>>;
    /// Go `SessionVars` / `StmtCtx` state `row_id_shards`, mutably.
    fn row_id_shards_mut(&mut self) -> &mut Arc<std::sync::Mutex<crate::RowIdShardGenerator>>;
    /// Go `SessionVars` / `StmtCtx` state `selected_lock_keys`, read.
    fn selected_lock_keys_ref(&self) -> &Option<tidb_sessionctx::select_lock::SelectedLockKeys>;
    /// Go `SessionVars` / `StmtCtx` state `selected_lock_keys`, mutably.
    fn selected_lock_keys_mut(&mut self) -> &mut Option<tidb_sessionctx::select_lock::SelectedLockKeys>;
    /// Go `SessionVars` / `StmtCtx` state `sequence_last_values`, read.
    fn sequence_last_values_ref(&self) -> &Arc<std::sync::Mutex<HashMap<i64, i64>>>;
    /// Go `SessionVars` / `StmtCtx` state `sequence_last_values`, mutably.
    fn sequence_last_values_mut(&mut self) -> &mut Arc<std::sync::Mutex<HashMap<i64, i64>>>;
    /// Go `SessionVars` / `StmtCtx` state `server_id_getter`, read.
    fn server_id_getter_ref(&self) -> &Arc<dyn Fn() -> u64 + Send + Sync>;
    /// Go `SessionVars` / `StmtCtx` state `server_id_getter`, mutably.
    fn server_id_getter_mut(&mut self) -> &mut Arc<dyn Fn() -> u64 + Send + Sync>;
    /// Go `SessionVars` / `StmtCtx` state `session_index_usage_collector`, read.
    fn session_index_usage_collector_ref(&self) -> &Option<Arc<Mutex<tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector>>>;
    /// Go `SessionVars` / `StmtCtx` state `session_index_usage_collector`, mutably.
    fn session_index_usage_collector_mut(&mut self) -> &mut Option<Arc<Mutex<tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector>>>;
    /// Go `SessionVars` / `StmtCtx` state `session_memory`, read.
    fn session_memory_ref(&self) -> &crate::SessionMemory;
    /// Go `SessionVars` / `StmtCtx` state `session_memory`, mutably.
    fn session_memory_mut(&mut self) -> &mut crate::SessionMemory;
    /// Go `SessionVars` / `StmtCtx` state `sql_cpu_usages`, read.
    fn sql_cpu_usages_ref(&self) -> &Arc<tidb_ppcpuusage::SqlCpuUsages>;
    /// Go `SessionVars` / `StmtCtx` state `sql_cpu_usages`, mutably.
    fn sql_cpu_usages_mut(&mut self) -> &mut Arc<tidb_ppcpuusage::SqlCpuUsages>;
    /// Go `SessionVars` / `StmtCtx` state `staged_writes`, read.
    fn staged_writes_ref(&self) -> &std::sync::Arc<tidb_kv::staged_writes::StagedWrites>;
    /// Go `SessionVars` / `StmtCtx` state `staged_writes`, mutably.
    fn staged_writes_mut(&mut self) -> &mut std::sync::Arc<tidb_kv::staged_writes::StagedWrites>;
    /// Go `SessionVars` / `StmtCtx` state `statement_affected_rows`, read.
    fn statement_affected_rows_ref(&self) -> &u64;
    /// Go `SessionVars` / `StmtCtx` state `statement_affected_rows`, mutably.
    fn statement_affected_rows_mut(&mut self) -> &mut u64;
    /// Go `SessionVars` / `StmtCtx` state `statement_in_select`, read.
    fn statement_in_select_ref(&self) -> &bool;
    /// Go `SessionVars` / `StmtCtx` state `statement_in_select`, mutably.
    fn statement_in_select_mut(&mut self) -> &mut bool;
    /// Go `SessionVars` / `StmtCtx` state `statement_keys_read`, read.
    fn statement_keys_read_ref(&self) -> &Arc<std::sync::atomic::AtomicU64>;
    /// Go `SessionVars` / `StmtCtx` state `statement_keys_read`, mutably.
    fn statement_keys_read_mut(&mut self) -> &mut Arc<std::sync::atomic::AtomicU64>;
    /// Go `SessionVars` / `StmtCtx` state `statement_kind`, read.
    fn statement_kind_ref(&self) -> &StatementKind;
    /// Go `SessionVars` / `StmtCtx` state `statement_kind`, mutably.
    fn statement_kind_mut(&mut self) -> &mut StatementKind;
    /// Go `SessionVars` / `StmtCtx` state `statement_result_authority`, read.
    fn statement_result_authority_ref(&self) -> &std::cell::RefCell<Option<ResultMaterializationAuthority>>;
    /// Go `SessionVars` / `StmtCtx` state `statement_result_authority`, mutably.
    fn statement_result_authority_mut(&mut self) -> &mut std::cell::RefCell<Option<ResultMaterializationAuthority>>;
    /// Go `SessionVars` / `StmtCtx` state `statement_var_cache`, read.
    fn statement_var_cache_ref(&self) -> &std::cell::RefCell<Option<std::sync::Arc<StatementVarSnapshot>>>;
    /// Go `SessionVars` / `StmtCtx` state `statement_var_cache`, mutably.
    fn statement_var_cache_mut(&mut self) -> &mut std::cell::RefCell<Option<std::sync::Arc<StatementVarSnapshot>>>;
    /// Go `SessionVars` / `StmtCtx` state `stats_collector`, read.
    fn stats_collector_ref(&self) -> &Option<std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>>;
    /// Go `SessionVars` / `StmtCtx` state `stats_collector`, mutably.
    fn stats_collector_mut(&mut self) -> &mut Option<std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>>;
    /// Go `SessionVars` / `StmtCtx` state `stmt_hints`, read.
    fn stmt_hints_ref(&self) -> &tidb_hint::StmtHints;
    /// Go `SessionVars` / `StmtCtx` state `stmt_hints`, mutably.
    fn stmt_hints_mut(&mut self) -> &mut tidb_hint::StmtHints;
    /// Go `SessionVars` / `StmtCtx` state `tidb_decode_key_cache`, read.
    fn tidb_decode_key_cache_ref(&self) -> &std::sync::Mutex<Option<(u64, Arc<crate::TidbDecodeKeySnapshot>)>>;
    /// Go `SessionVars` / `StmtCtx` state `tidb_decode_key_cache`, mutably.
    fn tidb_decode_key_cache_mut(&mut self) -> &mut std::sync::Mutex<Option<(u64, Arc<crate::TidbDecodeKeySnapshot>)>>;
    /// Go `SessionVars` / `StmtCtx` state `transaction_table_delta`, read.
    fn transaction_table_delta_ref(&self) -> &std::sync::Arc<tidb_variable::table_delta::TableDeltaMap>;
    /// Go `SessionVars` / `StmtCtx` state `transaction_table_delta`, mutably.
    fn transaction_table_delta_mut(&mut self) -> &mut std::sync::Arc<tidb_variable::table_delta::TableDeltaMap>;
    /// Whether a transaction is open (Go `SessionVars.InTxn`, the wire's
    /// `SERVER_STATUS_IN_TRANS`).
    fn in_transaction(&self) -> bool;
    /// Go `SessionVars.IsAutocommit()`: whether each statement stands on its
    /// own, or joins a transaction the session keeps open for it.
    ///
    /// Public because a front end over cluster storage has to open the same
    /// transaction this session will: `SET autocommit = 0` opens one with no
    /// `BEGIN` keyword for anyone to route on, so the variable itself is the
    /// only thing there is to ask, and asking the session keeps it the ONE
    /// answer rather than a second copy that can disagree.
    fn is_autocommit(&self) -> bool;
    /// Go `TxnCtx.IsPessimistic` for the running statement: an explicit or
    /// `autocommit = 0` pessimistic transaction, or the autocommit
    /// statement's implicit transaction when `decideTxnMode` chose
    /// pessimistic (see [`Self::set_autocommit_pessimistic_statement`]).
    fn statement_is_pessimistic(&self) -> bool;
    /// Go `preprocessor.checkNoopFuncs` + `checkGroupBy`: refuses the clauses
    /// TiDB parses but only implements as no-ops, unless
    /// `tidb_enable_noop_functions` says otherwise.
    ///
    /// Captured from TiDB with the variable at its `OFF` default:
    /// `SELECT SQL_CALC_FOUND_ROWS ...`, `... FOR SHARE` and `... LOCK IN
    /// SHARE MODE` all raise 1235; `FOR UPDATE` does not.
    ///
    /// `tidb_enable_shared_lock_promotion` turns `FOR SHARE` into `FOR UPDATE`
    /// before this check. The eventual lock executor still owns the
    /// `ForShareLockEnabledByNoop` statement flag, but admission must already
    /// skip the no-op gate when promotion is enabled.
    fn check_noop_functions(&mut self, query: &tidb_ast::QueryStmt) -> Result<(), DriverError>;
    /// The statement's infoschema, mutably (the in-process tables).
    fn with_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut crate::Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError>;
    /// Session primitive.
    fn statement_txn_mode(&self) -> tidb_vardef::txn_mode::SessionTxnMode;
    /// Whether the open transaction is a stale read (Go `TxnCtx.IsStaleness`).
    fn txn_is_stale_read(&self) -> bool;
    /// Session primitive.
    fn statement_phase_observer(&self) -> Option<Arc<dyn Fn(crate::StatementPhase) + Send + Sync>>;
    /// Session primitive.
    fn statement_kv_exec_counter(&self) -> tidb_topsql_stmtstats::KvExecCounterHandle;
    /// The session's statement ctx values, shared with the transactions and
    /// snapshots its statements use; see [`tidb_txnkv::StatementKvContextCell`].
    fn statement_kv_context_cell(&self) -> tidb_txnkv::StatementKvContextCell;
    /// Go `tidb_enable_collect_execution_info`: the statement's consumer of
    /// coprocessor response details, `None` while the instance switch is OFF
    /// (Go's `copIteratorWorker.stats` is only created when it is ON).
    fn statement_cop_response_sink(&self) -> Option<Arc<dyn crate::CopResponseSink>>;
    /// Session primitive.
    fn effective_replica_read(&self, mode: crate::ReplicaReadType) -> crate::ReplicaReadType;
    /// The live transaction timestamp authority used by storage integration.
    fn current_tso(&self) -> crate::CurrentTso;
    /// Runs `body` over the catalog the open transaction reads: a stale
    /// transaction's historical schema (Go `TxnCtx.InfoSchema`), otherwise the
    /// session's catalog.
    fn with_txn_catalog<T>(&self, body: impl FnOnce(&crate::Catalog) -> T, ) -> Result<T, DriverError>;
    /// Go `privilege.Manager.RequestVerificationWithUser` for the planner's
    /// view expansion: whether an account -- matched against the stored ones by
    /// host pattern, with its default roles -- holds a privilege. `None` is a
    /// session that enforces no privileges (Go's nil manager).
    fn view_definer_privilege_check(&self) -> Option<crate::ViewDefinerPrivilegeCheck>;
    /// Go `sysvar.go`'s `GetSession` hook on `warning_count`, which returns
    /// `s.SysWarningCount`.
    fn sys_warning_count(&self) -> usize;
    /// Go's matching hook on `error_count`, `s.SysErrorCount`.
    fn sys_error_count(&self) -> usize;
    /// [`Self::sql_digest_text_retriever`] as `TIDB_DECODE_SQL_DIGESTS` runs
    /// it from the statement context (Go `RetrieveGlobal`).
    fn sql_digest_text_retrieval(&self) -> crate::SqlDigestTextRetrieval;
    /// Go `privilege.Manager.RequestVerification(activeRoles, ...)` for the
    /// planner: whether this session's account, with its active roles, holds
    /// a privilege. `None` is a session that enforces no privileges.
    fn session_privilege_check(&self) -> Option<crate::SessionPrivilegeCheck>;
    /// An owned [`Self::has_scoped_privilege`] for checks Go makes while
    /// EVALUATING an expression (`NEXTVAL`/`LASTVAL`/`SETVAL` ask
    /// `RequestVerification` from the builtin), which run with the session
    /// out of reach. `None` is an unrestricted session.
    fn sequence_privilege_check(&self) -> Option<crate::SequencePrivilegeCheck>;
    /// Go SessionVars.ConstraintCheckInPlacePessimistic, read live per statement.
    fn pessimistic_constraint_check_in_place(&self) -> bool;
    /// The node's followed cluster schema version, or 0 for a tier with no
    /// cluster to follow -- the same source `ADMIN SHOW DDL` reports. This is
    /// Go's `domainSchemaVer` in `preprocess.go:2264` (the LATEST domain
    /// version, not the transaction's pinned one), which is the version the
    /// metadata-lock recording stamps a table's first use with.
    /// Go `GetRelatedTableForMDL().Store(id, domainSchemaVer)` as the planner
    /// reaches it from `buildDataSource`: each stored table it binds -- a
    /// view's body included -- joins the transaction's map at the node's
    /// schema version then. `None` when this statement locks nothing.
    fn mdl_related_table_recorder(&self) -> Option<crate::MdlRelatedTableRecorder>;
    /// Whether this session may inspect process-wide diagnostic state.
    fn has_process_privilege(&self) -> bool;
    /// Go `TableLockReadContext`: the table locks this session holds
    /// (`StmtContext::locked_tables`); `None` with table locks disabled.
    fn locked_tables(&self) -> Option<crate::LockedTables>;
    /// The connection's persistent resource group. A statement hint does not
    /// modify this value.
    fn current_resource_group(&self) -> &str;
    /// Session primitive.
    fn breakpoint_notify_func(&self) -> Option<Arc<dyn Fn(String) + Send + Sync + 'static>>;
    /// Go `session.ExecuteStmt`'s parse observation (`session.go:2611`).
    fn observe_parse_duration(&self, seconds: f64, internal: bool);
}

/// Go `session.UpdateColStatsUsage` over the session's collector: every
/// item of one planning pass gets the same timestamp.
pub(crate) fn col_stats_usage_updater(
    usage: &Option<Arc<tidb_stats_handle_usage::SessionStatsItem>>,
) -> Option<tidb_sessionctx::ColStatsUsageUpdater> {
    let usage = Arc::clone(usage.as_ref()?);
    Some(Arc::new(move |items| {
        usage.update_col_stats_usage(items, std::time::SystemTime::now());
    }))
}
