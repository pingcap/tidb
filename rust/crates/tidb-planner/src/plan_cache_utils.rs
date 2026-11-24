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

//! Go `pkg/planner/core/plan_cache_utils.go`'s key side of the plan cache:
//! what `planCacheStmtProcessor` records of a prepared statement
//! ([`PlanCacheStmtInfo`]), the session facts `NewPlanCacheKey` keys an
//! entry by ([`PreparedPlanCacheEnvironment`]), and the statistics version
//! hash (`getLatestVersionFromStatsTable`).

/// Go `getLatestVersionFromStatsTable` (`logical_plan_builder.go`), the
/// plan-cache key's statistics version for one table: zero for any table
/// `GetStatsTable` answers with a pseudo table by rule 2, otherwise the
/// largest `LastUpdateVersion` among the cached column and index objects.
///
/// It is NOT the `stats_meta` version: a stats-delta flush moves that, and Go
/// keeps the cached plan across it.
#[must_use]
pub fn get_latest_version_from_stats_table(
    handle: &tidb_stats_handle::StatsHandle,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    table_id: i64,
    opt_objective_determinate: bool,
) -> u64 {
    let Some(statistics) = handle.table_statistics(table_id, || catalog.statistics_schema(table_id))
    else {
        return 0;
    };
    let realtime_row_count = if opt_objective_determinate {
        statistics.hist_coll.analyze_row_count().max(0.0) as i64
    } else {
        statistics.hist_coll.realtime_count
    };
    if realtime_row_count == 0 {
        return 0;
    }
    let mut version = 0;
    statistics.hist_coll.for_each_column(|_, column| {
        version = version.max(column.histogram.last_update_version);
        false
    });
    statistics.hist_coll.for_each_index(|_, index| {
        version = version.max(index.histogram.last_update_version);
        false
    });
    version
}

/// Go `PlanCacheStmt.limits` and `hasSubquery`, as `planCacheStmtProcessor`
/// records them at PREPARE: whether the statement has any LIMIT and any
/// subquery expression. `NewPlanCacheKey` refuses the cache at each EXECUTE
/// while the matching switch is off.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlanCacheStmtInfo {
    /// Go `PlanCacheStmt.limits` is non-empty.
    pub has_limit: bool,
    /// Go `PlanCacheStmt.hasSubquery`.
    pub has_subquery: bool,
}

impl PlanCacheStmtInfo {
    pub fn of(stmt: &tidb_ast::Stmt) -> Self {
        struct Processor(PlanCacheStmtInfo);

        impl tidb_ast::Visitor for Processor {
            fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                if node.is::<tidb_ast::Limit>() {
                    self.0.has_limit = true;
                } else if let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() {
                    if matches!(
                        expr,
                        tidb_ast::Expr::Subquery(_)
                            | tidb_ast::Expr::Exists { .. }
                            | tidb_ast::Expr::InSubquery { .. }
                            | tidb_ast::Expr::CompareSubquery { .. }
                    ) {
                        self.0.has_subquery = true;
                    }
                }
                false
            }

            fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
                true
            }
        }

        let mut statement = stmt.clone();
        let mut processor = Processor(Self::default());
        tidb_ast::Visitable::accept(&mut statement, &mut processor);
        processor.0
    }
}

/// The statistics handle a plan-cache environment reads versions from. It is
/// the source of a key component, not part of the key, so every handle
/// compares equal and hashes to nothing.
#[derive(Clone, Debug, Default)]
struct StatsHandleRef(std::sync::Arc<tidb_stats_handle::StatsHandle>);

impl PartialEq for StatsHandleRef {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for StatsHandleRef {}

impl std::hash::Hash for StatsHandleRef {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

/// Session facts in Go's prepared-plan cache key that can change physical
/// planning without changing the statement, schema, or parameter types.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PreparedPlanCacheEnvironment {
    sql_mode: tidb_mysql::SqlMode,
    user_identity: String,
    /// The statement's statistics handle, which the plan-cache key's
    /// `statsVerHash` reads table versions from.
    stats_handle: StatsHandleRef,
    /// Go's `EnableNoBackslashEscapesInLike` plan-cache key bit. The
    /// statement's implicit LIKE escape changes both the rewritten expression
    /// and any index range derived from it, so plans built under different
    /// values must never share a cache entry.
    enable_no_backslash_escapes_in_like: bool,
    foreign_key_checks: bool,
    time_zone: String,
    pushdown_blacklist_generation: u64,
    connection_charset: String,
    connection_collation: String,
    partition_prune_mode: String,
    isolation_read_engines: String,
    sql_select_limit: String,
    in_transaction: bool,
    pessimistic_transaction: bool,
    /// Go `NewPlanCacheKey` includes SharedLockPromotion.
    shared_lock_promotion: bool,
    autocommit: bool,
    invalidate_on_fresh_stats: bool,
    binding_sql: String,
    skip_stats_on_binding: bool,
    plan_cache_max_plan_size: u64,
    enable_generated_columns: bool,
    /// Go `EnablePlanCacheForParamLimit` (`tidb_enable_plan_cache_for_param_limit`).
    enable_param_limit: bool,
    /// Go `EnablePlanCacheForSubquery` (`tidb_enable_plan_cache_for_subquery`).
    enable_subquery: bool,
    /// `tidb_opt_objective = 'determinate'`, read only by the statistics
    /// version hash.
    stats_hash_objective: StatsHashObjective,
}

/// Go's `NewPlanCacheKey` reads `tidb_opt_objective` only inside
/// `getLatestVersionFromStatsTable`; the variable never keys an entry by
/// itself, so two environments that differ only here are the same key.
#[derive(Clone, Copy, Debug, Default)]
struct StatsHashObjective {
    determinate: bool,
}

impl PartialEq for StatsHashObjective {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for StatsHashObjective {}

impl std::hash::Hash for StatsHashObjective {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

impl Default for PreparedPlanCacheEnvironment {
    fn default() -> Self {
        Self::new(tidb_mysql::SqlMode::default(), String::new(), 0)
    }
}

impl PreparedPlanCacheEnvironment {
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.user_identity.capacity()
            + self.time_zone.capacity()
            + self.connection_charset.capacity()
            + self.connection_collation.capacity()
            + self.partition_prune_mode.capacity()
            + self.isolation_read_engines.capacity()
            + self.sql_select_limit.capacity()
            + self.binding_sql.capacity()
    }

    /// The statement's statistics handle (Go `sctx.GetStatsHandle()`).
    #[must_use]
    pub fn with_stats_handle(mut self, handle: std::sync::Arc<tidb_stats_handle::StatsHandle>) -> Self {
        self.stats_handle = StatsHandleRef(handle);
        self
    }

    /// Go plan-cache key's authenticated user/host identity.
    pub fn with_user_identity(mut self, identity: String) -> Self {
        self.user_identity = identity;
        self
    }

    /// Builds the non-schema portion of Go's plan-cache environment key.
    #[must_use]
    pub fn new(
        sql_mode: tidb_mysql::SqlMode,
        time_zone: String,
        pushdown_blacklist_generation: u64,
    ) -> Self {
        Self {
            sql_mode,
            user_identity: String::new(),
            stats_handle: StatsHandleRef::default(),
            enable_no_backslash_escapes_in_like: true,
            foreign_key_checks: true,
            time_zone,
            pushdown_blacklist_generation,
            connection_charset: String::new(),
            connection_collation: String::new(),
            partition_prune_mode: String::new(),
            isolation_read_engines: String::new(),
            sql_select_limit: String::new(),
            in_transaction: false,
            pessimistic_transaction: false,
            shared_lock_promotion: false,
            autocommit: true,
            invalidate_on_fresh_stats: true,
            binding_sql: String::new(),
            skip_stats_on_binding: false,
            plan_cache_max_plan_size: tidb_vardef::defaults::DEF_TIDB_PLAN_CACHE_MAX_PLAN_SIZE
                as u64,
            enable_generated_columns: true,
            enable_param_limit: true,
            enable_subquery: true,
            stats_hash_objective: StatsHashObjective::default(),
        }
    }

    /// Go NewPlanCacheKey includes ForeignKeyChecks because it changes the
    /// FK policies retained in the DML root.
    #[must_use]
    pub const fn with_foreign_key_checks(mut self, enabled: bool) -> Self {
        self.foreign_key_checks = enabled;
        self
    }

    /// Keeps DML lock plans scoped to the transaction mode that built them.
    #[must_use]
    pub const fn with_pessimistic_transaction(mut self, enabled: bool) -> Self {
        self.pessimistic_transaction = enabled;
        self
    }

    /// Go `NewPlanCacheKey`: promoted FOR SHARE plans cannot reuse no-op locks.
    #[must_use]
    pub const fn with_shared_lock_promotion(mut self, enabled: bool) -> Self {
        self.shared_lock_promotion = enabled;
        self
    }

    /// Adds the session switch that controls the implicit LIKE escape under
    /// `NO_BACKSLASH_ESCAPES`, matching Go's `NewPlanCacheKey`.
    #[must_use]
    pub const fn with_no_backslash_escapes_in_like(mut self, enabled: bool) -> Self {
        self.enable_no_backslash_escapes_in_like = enabled;
        self
    }

    /// Adds the session facts that Go's `NewPlanCacheKey` hashes because they
    /// can change access paths, partition expansion, expression semantics, or
    /// whether a UnionScan/transactional reader is required.
    #[must_use]
    pub fn with_session_state(
        mut self,
        connection_charset: String,
        connection_collation: String,
        partition_prune_mode: String,
        isolation_read_engines: String,
        sql_select_limit: String,
        in_transaction: bool,
        autocommit: bool,
        invalidate_on_fresh_stats: bool,
    ) -> Self {
        self.connection_charset = connection_charset;
        self.connection_collation = connection_collation;
        self.partition_prune_mode = partition_prune_mode;
        self.isolation_read_engines = isolation_read_engines;
        self.sql_select_limit = sql_select_limit;
        self.in_transaction = in_transaction;
        self.autocommit = autocommit;
        self.invalidate_on_fresh_stats = invalidate_on_fresh_stats;
        self
    }

    /// Adds the two post-optimization admission inputs read by Go's
    /// `isPlanCacheable` / `isPhysicalPlanCacheable`.
    #[must_use]
    pub const fn with_cache_admission(
        mut self,
        plan_cache_max_plan_size: u64,
        enable_generated_columns: bool,
    ) -> Self {
        self.plan_cache_max_plan_size = plan_cache_max_plan_size;
        self.enable_generated_columns = enable_generated_columns;
        self
    }

    /// Adds Go's matched `Binding.BindSQL` to the cache key. The stats-version
    /// component is omitted only when a binding matched and
    /// `tidb_plan_cache_skip_stats_on_binding` is enabled.
    #[must_use]
    pub fn with_binding_sql(mut self, binding_sql: Option<&str>, skip_stats: bool) -> Self {
        self.binding_sql = binding_sql.unwrap_or_default().to_owned();
        self.skip_stats_on_binding = !self.binding_sql.is_empty() && skip_stats;
        self
    }

    /// Adds `tidb_opt_objective`, which Go's statistics version hash
    /// (`getLatestVersionFromStatsTable`) reads.
    #[must_use]
    pub const fn with_opt_objective_determinate(mut self, determinate: bool) -> Self {
        self.stats_hash_objective = StatsHashObjective { determinate };
        self
    }

    /// Adds the two switches Go's `NewPlanCacheKey` reads at every EXECUTE.
    #[must_use]
    pub const fn with_cache_switches(mut self, param_limit: bool, subquery: bool) -> Self {
        self.enable_param_limit = param_limit;
        self.enable_subquery = subquery;
        self
    }

    /// Go `NewPlanCacheKey`'s refusals: a statement with a subquery while
    /// `tidb_enable_plan_cache_for_subquery` is off, or with a LIMIT while
    /// `tidb_enable_plan_cache_for_param_limit` is off, neither reads nor
    /// fills the cache. `None` admits.
    pub const fn refusal(&self, info: PlanCacheStmtInfo) -> Option<&'static str> {
        if info.has_subquery && !self.enable_subquery {
            Some("the switch 'tidb_enable_plan_cache_for_subquery' is off")
        } else if info.has_limit && !self.enable_param_limit {
            Some("the switch 'tidb_enable_plan_cache_for_param_limit' is off")
        } else {
            None
        }
    }

    pub const fn plan_cacheability(
        &self,
        parameter_count: usize,
    ) -> crate::plan_cache_rebuild::PlanCacheabilityContext {
        crate::plan_cache_rebuild::PlanCacheabilityContext {
            parameter_count,
            enable_generated_columns: self.enable_generated_columns,
            max_plan_size: self.plan_cache_max_plan_size,
        }
    }

    pub const fn hashes_fresh_statistics(&self) -> bool {
        self.invalidate_on_fresh_stats && !self.skip_stats_on_binding
    }

    /// Go `NewPlanCacheKey`'s `statsVerHash`: the sum of every table's
    /// `getLatestVersionFromStatsTable(sctx, t.Meta(), t.Meta().ID)`, zero
    /// when the key omits statistics.
    pub fn stats_version_hash(&self, catalog: &tidb_infoschema::SessionExtendedInfoSchema, keys: &[tidb_infoschema::CatalogTableKey]) -> u64 {
        if !self.hashes_fresh_statistics() {
            return 0;
        }
        keys.iter().fold(0, |hash, key| {
            let version = match catalog.get_by_key(key) {
                Some(tidb_infoschema::TableEntry::Kv(table)) => {
                    get_latest_version_from_stats_table(
                        &self.stats_handle.0,
                        catalog,
                        table.table_id(),
                        self.stats_hash_objective.determinate,
                    )
                }
                _ => 0,
            };
            hash.wrapping_add(version)
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Go `PlanCacheKey` (`NewPlanCacheKey`): the statement, its schema and
/// statistics versions, the session environment, the LIMIT parameters, the
/// transaction's dirty tables and the tables' revisions.
pub struct PhysicalPlanCacheKey {
    /// The database and the statement's normalized text.
    pub statement: String,
    /// Go `SchemaMetaVersion`.
    pub schema_version: u64,
    /// Go `statsVerHash`.
    pub stats_version_hash: u64,
    /// The session facts the key carries.
    pub environment: PreparedPlanCacheEnvironment,
    /// Go `limitParams`.
    pub limit_values: Vec<u64>,
    /// Go `NewPlanCacheKey`'s dirty tables: the statement's tables this
    /// transaction has written, which plan with a `UnionScan` over them.
    pub dirty_tables: Vec<i64>,
    /// Go `PlanCacheStmt.RelateVersion`: each of the statement's tables and
    /// its `TableInfo.Revision` in the schema this execution reads (after
    /// `GetPlanFromPlanCache` step 3 swapped in a table changed since the
    /// transaction began), so a plan built over an older definition misses.
    pub relate_version: Vec<(i64, u64)>,
}

impl crate::plan_cache_lru::PlanCacheKey for PhysicalPlanCacheKey {
    fn memory_usage(&self) -> i64 {
        (std::mem::size_of::<Self>() + self.statement.capacity() + self.environment.memory_usage()
            - std::mem::size_of::<PreparedPlanCacheEnvironment>()
            + self.limit_values.capacity() * std::mem::size_of::<u64>()
            + self.dirty_tables.capacity() * std::mem::size_of::<i64>()
            + self.relate_version.capacity() * std::mem::size_of::<(i64, u64)>()) as i64
    }
}
