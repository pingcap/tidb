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
use std::rc::Rc;

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
/// clones were gone. GLOBAL-scope reads (`tidb_mem_oom_action`,
/// `tidb_enable_tmp_storage_on_oom`, the password-validation set) stay
/// LIVE in the builder: a peer's `SET GLOBAL` moves them without this
/// session's generation changing, so caching them here would be wrong.
pub(crate) struct StatementVarSnapshot {
    generation: u64,
    version: Option<String>,
    connection_charset: String,
    connection_collation: String,
    allow_write_row_id: bool,
    sysdate_is_now: bool,
    mode_upper: String,
    allow_auto_random_explicit_insert: bool,
    shard_allocate_step: u64,
    like_default_escape: u8,
    week_format: i64,
    div_scale: u32,
    cte_depth: i64,
    join_reorder_threshold: i32,
    default_string_match_selectivity: f64,
    advanced_join_reorder: bool,
    ordering_index_selectivity_ratio: f64,
    join_reorder_through_proj: bool,
    join_reorder_through_sel: bool,
    outer_join_reorder: bool,
    index_merge: bool,
    static_partition_prune: bool,
    new_only_full_group_by_check: bool,
    mem_quota: i64,
    max_allowed_packet: u64,
    group_concat_max_len: u64,
    apply_cache_capacity: i64,
    block_encryption_mode: tidb_executor::BlockEncryptionMode,
    arbitrator_wait_averse: Option<bool>,
    arbitrator_reserved: i64,
}

impl Session {
    fn optimizer_cost_env(
        &self,
        mem_quota: i64,
        tmp_storage_on_oom: bool,
    ) -> (tidb_planner::candidate_cost::CostEnv, f64) {
        // Everything below derives from the session's variable table, except
        // the two per-statement arguments, which are patched onto the cached
        // copy -- so a statement pays one stamp check and one clone instead
        // of thirty-odd string lookups and parses. Go's equivalents are
        // typed `SessionVars` fields maintained at `SET`.
        let generation = self.vars.generation();
        if let Some((cached_at, env, join_concurrency)) = self.cost_env_cache.borrow().as_ref() {
            if *cached_at == generation {
                let mut env = env.clone();
                env.session.mem_quota = mem_quota;
                env.session.enable_tmp_storage_on_oom = tmp_storage_on_oom;
                return (env, *join_concurrency);
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

        let mut env = tidb_planner::candidate_cost::CostEnv::default();
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

        let join_concurrency = resolved_concurrency("tidb_hash_join_concurrency");
        *self.cost_env_cache.borrow_mut() = Some((generation, env.clone(), join_concurrency));
        (env, join_concurrency)
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
        let mode = self.vars.get_system("sql_mode").unwrap_or_default();
        let allow_invalid_dates = mode
            .split(',')
            .any(|part| part.trim().eq_ignore_ascii_case("ALLOW_INVALID_DATES"));
        tidb_executor::kv_table::PreparedPointGetDecodeContext::for_query(
            allow_invalid_dates,
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
        let quota = self
            .vars
            .get_system("tidb_mem_quota_query")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_util::memory::DEF_MEM_QUOTA_QUERY);
        let oom_action = tidb_executor::OomAction::parse(
            &self
                .vars
                .get_global("tidb_mem_oom_action")
                .unwrap_or_default(),
        );
        let tmp_storage_on_oom = {
            let value = self
                .vars
                .get_global("tidb_enable_tmp_storage_on_oom")
                .unwrap_or_default();
            !(value.eq_ignore_ascii_case("off") || value == "0")
        };
        self.session_memory
            .configure(quota, oom_action, tmp_storage_on_oom);
        let memory = self.session_memory.statement();
        let init_chunk_size = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_INIT_CHUNK_SIZE)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(32);
        let max_chunk_size = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_MAX_CHUNK_SIZE)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1024);
        crate::ResultMaterializationAuthority::new(memory, init_chunk_size, max_chunk_size)
    }

    /// Go `timeutil.ParseTimeZone`: `SYSTEM` is the host zone, a named zone
    /// comes from the zone database, and a `+HH:MM`/`-HH:MM` string is a
    /// fixed offset bounded to `[-12:59, +14:00]`.
    ///
    /// An unparseable value falls back to the host zone rather than failing
    /// the statement, because this tier accepts the variable without
    /// validating it at SET time -- Go validates there instead, and that
    /// check is the deferred half of this port.
    pub fn session_time_zone(&self) -> tidb_executor::SessionTimeZone {
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
    fn sequence_snapshot(&self) -> Rc<tidb_executor::SequenceSnapshot> {
        let by_name = match &self.txn {
            Some(txn) => txn.working.sequence_allocators(),
            None => match self.catalog.lock() {
                Ok(catalog) => catalog.sequence_allocators(),
                // A poisoned catalog is reported by the statement itself; an
                // empty map here just makes every name unknown.
                Err(_) => HashMap::new(),
            },
        };
        Rc::new(tidb_executor::SequenceSnapshot::new(
            by_name,
            &self.current_db,
            Rc::clone(&self.sequence_last_values),
        ))
    }

    fn tidb_decode_key_snapshot(&self) -> Rc<tidb_executor::TidbDecodeKeySnapshot> {
        if self.skip_tidb_decode_key_snapshot.get() {
            return Rc::default();
        }
        let Ok(catalog) = self.catalog.lock() else {
            return Rc::default();
        };
        // Keyed on the METADATA counter, not the mutation counter: Go's
        // row-decode metadata is cached per infoschema version, which DDL
        // moves and DML never does. Keying on `version()` here would rebuild
        // this snapshot on every write statement.
        let version = catalog.metadata_version();
        if let Some((cached_version, snapshot)) = self.tidb_decode_key_cache.borrow().as_ref() {
            if *cached_version == version {
                return Rc::clone(snapshot);
            }
        }
        let snapshot = Rc::new(catalog.tidb_decode_key_snapshot());
        *self.tidb_decode_key_cache.borrow_mut() = Some((version, Rc::clone(&snapshot)));
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
        // Cached against the variable table's generation: Go keeps the
        // parsed mode as `SessionVars.SQLMode`, a typed field the `SET` hook
        // maintains, so the per-statement read is a field access there and a
        // stamp check here.
        let generation = self.vars.generation();
        if let Some((cached_at, mode)) = self.scanner_sql_mode_cache.get() {
            if cached_at == generation {
                return mode;
            }
        }
        // `SET sql_mode = 'ANSI'` is stored already expanded (captured from
        // TiDB: `@@sql_mode` reads back
        // `REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI`),
        // so matching names against the stored text sees every flag a
        // combination brought in.
        let mode = scanner_sql_mode_of(
            &self
                .vars
                .get_system("sql_mode")
                .unwrap_or_default()
                .to_ascii_uppercase(),
        );
        self.scanner_sql_mode_cache.set(Some((generation, mode)));
        mode
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

    /// Builds the context used by the narrow prepared point/DML paths.
    /// Those paths do not evaluate `TIDB_DECODE_KEY`, so constructing its
    /// catalog metadata snapshot only adds per-execute work.
    pub(crate) fn fast_statement_context(
        &self,
        is_dml: bool,
        ignore_err: bool,
    ) -> tidb_executor::StmtContext {
        let previous = self.skip_tidb_decode_key_snapshot.replace(true);
        let context = self.statement_context_ignoring(is_dml, ignore_err);
        self.skip_tidb_decode_key_snapshot.set(previous);
        context
    }

    /// [`Self::statement_context`] for a DML statement that carries the
    /// `IGNORE` modifier, which Go's `ResetContextOfStmt` reads off the AST
    /// and folds into every value-level error level.
    /// The cached [`StatementVarSnapshot`], re-derived only when a `SET`
    /// moved the variable table; see the struct's own doc for why the
    /// GLOBAL-scope reads are NOT in it.
    fn statement_var_snapshot(&self) -> std::rc::Rc<StatementVarSnapshot> {
        let generation = self.vars.generation();
        if let Some(cached) = self.statement_var_cache.borrow().as_ref() {
            if cached.generation == generation {
                return std::rc::Rc::clone(cached);
            }
        }
        let mode_upper = self
            .vars
            .get_system("sql_mode")
            .unwrap_or_default()
            .to_ascii_uppercase();
        let has = |flag: &str| mode_upper.split(',').any(|part| part.trim() == flag);
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
        let snapshot = std::rc::Rc::new(StatementVarSnapshot {
            generation,
            version: self.vars.get_system("version").ok(),
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
            allow_auto_random_explicit_insert: on(
                tidb_vardef::tidb_vars::TIDB_ALLOW_AUTO_RAND_EXPLICIT_INSERT,
            ),
            shard_allocate_step: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SHARD_ALLOCATE_STEP)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(i64::MAX as u64),
            like_default_escape: if has("NO_BACKSLASH_ESCAPES")
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
            advanced_join_reorder: not_off(
                tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_ADVANCED_JOIN_REORDER,
            ),
            ordering_index_selectivity_ratio: self
                .vars
                .get_system("tidb_opt_ordering_index_selectivity_ratio")
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.01),
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
            max_allowed_packet: self
                .vars
                .get_system("max_allowed_packet")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(64 << 20),
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
            block_encryption_mode: self
                .vars
                .get_system("block_encryption_mode")
                .ok()
                .and_then(|value| tidb_executor::BlockEncryptionMode::parse(&value))
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
            mode_upper,
        });
        *self.statement_var_cache.borrow_mut() = Some(std::rc::Rc::clone(&snapshot));
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
        let tidb_info = Some(self.vars.tidb_info());
        let connection_charset = snapshot.connection_charset.clone();
        let connection_collation = snapshot.connection_collation.clone();
        let zone = self.session_time_zone();
        let clock = self.statement_clock(&zone);
        let allow_write_row_id = snapshot.allow_write_row_id;
        let sysdate_is_now = snapshot.sysdate_is_now;
        let has = |flag: &str| {
            snapshot
                .mode_upper
                .split(',')
                .any(|part| part.trim() == flag)
        };
        let allow_auto_random_explicit_insert = snapshot.allow_auto_random_explicit_insert;
        let shard_allocate_step = snapshot.shard_allocate_step;
        let like_default_escape = snapshot.like_default_escape;
        let week_format = snapshot.week_format;
        let div_scale = snapshot.div_scale;
        let cte_depth = snapshot.cte_depth;
        let join_reorder_threshold = snapshot.join_reorder_threshold;
        let default_string_match_selectivity = snapshot.default_string_match_selectivity;
        let advanced_join_reorder = snapshot.advanced_join_reorder;
        let ordering_index_selectivity_ratio = snapshot.ordering_index_selectivity_ratio;
        let join_reorder_through_proj = snapshot.join_reorder_through_proj;
        let join_reorder_through_sel = snapshot.join_reorder_through_sel;
        let outer_join_reorder = snapshot.outer_join_reorder;
        let index_merge = snapshot.index_merge;
        let static_partition_prune = snapshot.static_partition_prune;
        let new_only_full_group_by_check = snapshot.new_only_full_group_by_check;
        let mem_quota = snapshot.mem_quota;
        let max_allowed_packet = snapshot.max_allowed_packet;
        let group_concat_max_len = snapshot.group_concat_max_len;
        let apply_cache_capacity = snapshot.apply_cache_capacity;
        let block_encryption_mode = snapshot.block_encryption_mode;
        let arbitrator_wait_averse = snapshot.arbitrator_wait_averse;
        let arbitrator_reserved = snapshot.arbitrator_reserved;
        // GLOBAL-scope reads stay LIVE: a peer's `SET GLOBAL` moves them
        // without this session's generation changing.
        let password_validation_globals = tidb_util::password_validation::VALIDATE_PASSWORD_SYSVARS
            .into_iter()
            .map(|name| {
                (
                    name.to_owned(),
                    self.vars.get_global(name).unwrap_or_default(),
                )
            })
            .collect::<HashMap<_, _>>();
        let tmp_storage_on_oom = {
            let value = self
                .vars
                .get_global("tidb_enable_tmp_storage_on_oom")
                .unwrap_or_default();
            !(value.eq_ignore_ascii_case("off") || value == "0")
        };
        let (optimizer_cost_env, hash_join_concurrency) =
            self.optimizer_cost_env(mem_quota, tmp_storage_on_oom);
        let oom_action = tidb_executor::OomAction::parse(
            &self
                .vars
                .get_global("tidb_mem_oom_action")
                .unwrap_or_default(),
        );
        self.session_memory
            .configure(mem_quota, oom_action, tmp_storage_on_oom);
        // The SAME three bits on both branches: a query reads them for
        // `CAST(... AS DATE/DATETIME)`, a DML statement reads them for the
        // column write. They used to be attached only below, which left every
        // read with the all-false default -- and made `NO_ZERO_DATE` silently
        // inoperative on the read path.
        let date_modes = tidb_datatype::DateModes {
            no_zero_date: has("NO_ZERO_DATE"),
            no_zero_in_date: has("NO_ZERO_IN_DATE"),
            allow_invalid_dates: has("ALLOW_INVALID_DATES"),
        };
        if !is_dml {
            let ctx = tidb_executor::StmtContext::for_query()
                // A read's error levels do not depend on the mode, but DDL
                // takes this same context and Go's DDL checks DO read
                // `SQLMode.HasStrictMode()`. See `StmtContext::with_strict`.
                .with_strict(has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES"))
                .with_date_modes(date_modes)
                .with_cte_max_recursion_depth(cte_depth)
                .with_join_reorder_threshold(join_reorder_threshold)
                .with_advanced_join_reorder(advanced_join_reorder)
                .with_ordering_index_selectivity_ratio(ordering_index_selectivity_ratio)
                .with_optimizer_fix_control(self.vars.optimizer_fix_control().clone())
                .with_optimizer_cost_env(optimizer_cost_env.clone(), hash_join_concurrency)
                .with_join_reorder_through_proj(join_reorder_through_proj)
                .with_join_reorder_through_sel(join_reorder_through_sel)
                .with_outer_join_reorder(outer_join_reorder)
                .with_index_merge(index_merge)
                .with_pushdown_blacklists(self.pushdown_blacklists.snapshot())
                .with_planned_apply_channel(Rc::clone(&self.planned_apply))
                .with_allow_write_row_id(allow_write_row_id)
                .with_static_partition_prune(static_partition_prune)
                .with_only_full_group_by(has("ONLY_FULL_GROUP_BY"))
                .with_new_only_full_group_by_check(new_only_full_group_by_check)
                .with_session_state(current_db, version, tidb_info)
                .with_connection_charset_info(
                    connection_charset.clone(),
                    connection_collation.clone(),
                )
                .with_user(self.current_user.clone(), self.login_user.clone())
                .with_global_sysvars(password_validation_globals.clone())
                .with_current_role(self.current_user.as_ref().map(|_| self.current_role_text()))
                .with_connection_id(self.connection_id)
                .with_advisory_locks(self.advisory_locks.clone())
                .with_statement_memory(
                    self.session_memory
                        .statement_with_arbitration(arbitrator_wait_averse, arbitrator_reserved),
                )
                .with_rand_session(Rc::clone(&self.rand))
                .with_last_insert_id_channel(Rc::clone(&self.published_last_insert_id))
                .with_retry_auto_ids(Rc::clone(&self.retry_auto_ids))
                .with_row_id_shards(Rc::clone(&self.row_id_shards))
                .with_auto_random_policy(allow_auto_random_explicit_insert, shard_allocate_step)
                .with_user_vars(Rc::clone(&self.user_vars))
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
                .with_sql_mode(scanner_sql_mode_of(&snapshot.mode_upper))
                .with_no_unsigned_subtraction(has("NO_UNSIGNED_SUBTRACTION"))
                .with_like_default_escape(like_default_escape)
                .with_default_string_match_selectivity(default_string_match_selectivity)
                .with_sysdate_is_now(sysdate_is_now)
                .with_clock(clock, zone);
            return ctx;
        }
        let (increment, offset) = self.auto_increment_step();
        let ctx = tidb_executor::StmtContext::for_dml(
            has("ERROR_FOR_DIVISION_BY_ZERO"),
            has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES"),
            ignore_err,
        )
        .with_date_modes(date_modes)
        .with_planned_apply_channel(Rc::clone(&self.planned_apply))
        .with_allow_write_row_id(allow_write_row_id)
        .with_only_full_group_by(has("ONLY_FULL_GROUP_BY"))
        .with_new_only_full_group_by_check(new_only_full_group_by_check)
        .with_session_state(current_db, version, tidb_info)
        .with_connection_charset_info(connection_charset, connection_collation)
        .with_user(self.current_user.clone(), self.login_user.clone())
        .with_global_sysvars(password_validation_globals)
        .with_current_role(self.current_user.as_ref().map(|_| self.current_role_text()))
        .with_connection_id(self.connection_id)
        .with_advisory_locks(self.advisory_locks.clone())
        .with_statement_memory(
            self.session_memory
                .statement_with_arbitration(arbitrator_wait_averse, arbitrator_reserved),
        )
        .with_rand_session(Rc::clone(&self.rand))
        .with_last_insert_id_channel(Rc::clone(&self.published_last_insert_id))
        .with_retry_auto_ids(Rc::clone(&self.retry_auto_ids))
        .with_row_id_shards(Rc::clone(&self.row_id_shards))
        .with_auto_random_policy(allow_auto_random_explicit_insert, shard_allocate_step)
        .with_user_vars(Rc::clone(&self.user_vars))
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
        .with_clock(clock, zone)
        .with_sql_mode(scanner_sql_mode_of(&snapshot.mode_upper))
        .with_no_unsigned_subtraction(has("NO_UNSIGNED_SUBTRACTION"))
        .with_like_default_escape(like_default_escape)
        .with_default_string_match_selectivity(default_string_match_selectivity)
        .with_auto_increment_step(increment, offset)
        .with_auto_increment_zero_explicit(has("NO_AUTO_VALUE_ON_ZERO"))
        .with_foreign_key_checks(self.foreign_key_checks())
        .with_allow_remove_auto_inc(self.allow_remove_auto_inc())
        .with_cte_max_recursion_depth(cte_depth)
        .with_join_reorder_threshold(join_reorder_threshold)
        .with_advanced_join_reorder(advanced_join_reorder)
        .with_ordering_index_selectivity_ratio(ordering_index_selectivity_ratio)
        .with_optimizer_fix_control(self.vars.optimizer_fix_control().clone())
        .with_optimizer_cost_env(optimizer_cost_env, hash_join_concurrency)
        .with_join_reorder_through_proj(join_reorder_through_proj)
        .with_join_reorder_through_sel(join_reorder_through_sel)
        .with_outer_join_reorder(outer_join_reorder)
        .with_index_merge(index_merge)
        .with_static_partition_prune(static_partition_prune);
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
    /// for an unreadable value is OFF -- that is what a stock TiDB does and
    /// the only mode this engine models.
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
        if let Some(published) = self.published_last_insert_id.get() {
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

/// The scanner flags Go's `Parser.SetSQLMode` consults, read off an
/// already-uppercased, already-expanded `@@sql_mode` text.
pub(crate) fn scanner_sql_mode_of(mode: &str) -> tidb_parser::SqlMode {
    let has = |flag: &str| mode.split(',').any(|part| part.trim() == flag);
    tidb_parser::SqlMode {
        real_as_float: has("REAL_AS_FLOAT"),
        no_backslash_escapes: has("NO_BACKSLASH_ESCAPES"),
        ansi_quotes: has("ANSI_QUOTES"),
        high_not_precedence: has("HIGH_NOT_PRECEDENCE"),
        ignore_space: has("IGNORE_SPACE"),
        pipes_as_concat: has("PIPES_AS_CONCAT"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_statement_context_does_not_build_decode_key_metadata() {
        let session = Session::new();
        // Session bootstrap may create a normal context; isolate the fast
        // path assertion from that startup bookkeeping.
        *session.tidb_decode_key_cache.borrow_mut() = None;
        let _fast = session.fast_statement_context(false, false);
        assert!(session.tidb_decode_key_cache.borrow().is_none());

        // The suppression is scoped to one context construction; ordinary
        // statements still retain the metadata required by TIDB_DECODE_KEY.
        let _normal = session.statement_context(false);
        assert!(session.tidb_decode_key_cache.borrow().is_some());
    }
}
