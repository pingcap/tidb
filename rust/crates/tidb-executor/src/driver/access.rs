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

//! How physical reads are lowered and what work the driver hands down to
//! them.
//!
//! The shared planner owns ordinary access-path enumeration and selection.
//! Go's statement-level `TryFastPlan` is [`super::point_get_plan`]; this
//! module retains the prepared point and SELECT plans and the mechanical
//! offers to the selected reader: [`prune_scan_columns`],
//! [`negotiate_scan_filter`], and [`offer_scan_limit`].
//!
//! The offers are not commands: [`tidb_executor_internal_exec::table_access`] holds the
//! contract, every method of it is fail-closed, and the source alone decides.
//! The order matters and is fixed here -- pruning runs before the predicate
//! split so a pushed conjunct's `column_offset` is already in narrow space,
//! and the cap is offered last, after the residual `WHERE` is known, because
//! a residual filter above the source forbids one.
//!
use super::plan_cache::{CachedPhysicalPlan, PhysicalPlanCacheKey, SessionPlanCache};
use super::point_get_key::{names_no_rows, point_get_value, point_get_value_overflowed};
use super::*;
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_hack::GoCaseCompare;


/// One residual `column = ?`/`column = const` conjunct of a cached point
/// read: the equality the key lookup cannot answer, so the decoded row must.
#[derive(Clone, Debug)]
pub(crate) enum PointResidualBound {
    /// A constant whose datum is already in the column's domain.
    Literal(Datum),
    /// A `?` marker resolved against each EXECUTE's parameters.
    Param(usize),
    /// `column IS NULL`: the decoded row must carry a NULL in its slot.
    IsNull,
}

/// The immutable part of Go's cached `PointGetPlan` for one prepared handle
/// lookup. Runtime cursor state and the execute-time handle are deliberately
/// absent: both are rebuilt for every cache hit.
#[derive(Debug)]
pub struct PreparedPointGetPlan {
    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest` of the point plan
    /// (`TryFastPlan`'s), computed once and copied onto each execution.
    plan_digest: std::sync::OnceLock<(String, String)>,
    /// The catalog's KEY-DECODE METADATA version at resolve time — the
    /// counter DDL moves and DML never touches (`Catalog::metadata_version`).
    /// A transaction's own writes bump the mutation counter every statement,
    /// so keying a cache hit on it would invalidate every read after the
    /// transaction's first write; Go's plan cache invalidates on SCHEMA
    /// changes, and this counter is that boundary.
    schema_version: u64,
    current_database: String,
    database: String,
    table: String,
    table_key: CatalogTableKey,
    table_id: i64,
    /// One marker order per PINNED key column, aligned with
    /// [`Self::pin_types`] — the full row handle for [`PreparedPointTarget::
    /// RowHandle`], or every column of a unique secondary index.
    /// `None` pins that column to the matching [`Self::handle_literals`]
    /// constant instead of an EXECUTE parameter.
    parameter_orders: Vec<Option<usize>>,
    /// The pinned columns' field types, in key order; `bind` moves each
    /// execute's value into this domain before encoding the key.
    pin_types: Vec<FieldType>,
    /// Which key the pins name, and so which single read answers them all.
    pub(crate) target: PreparedPointTarget,
    /// The literal constants pinning handle columns, aligned with
    /// [`Self::parameter_orders`]; `None` where a marker pins instead. A NULL
    /// literal never matches, so it binds to an empty execution.
    handle_literals: Vec<Option<Datum>>,
    /// Empty for an integer PK handle; one offset for the supported prepared
    /// common-handle shape. Keeping this explicit lets `bind` rebuild the
    /// encoded handle without re-running the AST path matcher.
    common_handle_offsets: Vec<usize>,
    /// Equalities the key lookup leaves open (`stsrcd = ?` next to the full
    /// handle pin). Each entry positions INTO THE OUTPUT ROW (the index inside
    /// [`FastPointOutput::offsets`] the residual column decodes to), paired
    /// with the bound to compare against.
    residuals: Vec<(usize, PointResidualBound)>,
    /// A WHERE conjunct that SCHEMA contradicts (`NOT NULL` column `IS NULL`)
    /// makes every row unmatched before any key is read. The plan then binds
    /// to an always-empty execution, exactly like a NULL pin, and no storage
    /// read ever runs -- the cached shape of Go's `TableDual`.
    contradiction: bool,
    output: FastPointOutput,
    row_decoder: tidb_table::kv_table::PreparedPointGetRowDecoder,
    cached_keys: std::sync::Mutex<Vec<PreparedPointCacheKey>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPointCacheKey {
    schema_version: u64,
    environment: PreparedPlanCacheEnvironment,
    parameter_types: Vec<PreparedParameterType>,
}

impl PreparedPointCacheKey {
    fn matches(&self, actual: &Self) -> bool {
        self.schema_version == actual.schema_version
            && self.environment == actual.environment
            && prepared_parameter_types_compatible(&self.parameter_types, &actual.parameter_types)
    }
}

/// Builds the always-empty plan for a schema-contradicted WHERE: nothing is
/// pinned and nothing is residual, because no row can survive the predicate.
fn contradiction_plan(
    schema_version: u64,
    current_database: &str,
    database: &str,
    table_name: &str,
    table_id: i64,
    common_handle_offsets: Vec<usize>,
    handle_offset: Option<usize>,
    table: &crate::KvTable,
    output: FastPointOutput,
) -> Option<PreparedPointGetPlan> {
    Some(PreparedPointGetPlan {
        plan_digest: std::sync::OnceLock::new(),
        schema_version,
        current_database: current_database.to_owned(),
        database: database.to_owned(),
        table: table_name.to_owned(),
        table_key: CatalogTableKey::new(database, table_name),
        table_id,
        parameter_orders: Vec::new(),
        pin_types: Vec::new(),
        target: PreparedPointTarget::RowHandle,
        handle_literals: Vec::new(),
        common_handle_offsets,
        residuals: Vec::new(),
        contradiction: true,
        row_decoder: tidb_table::kv_table::PreparedPointGetRowDecoder::new_with_handle_prefixes(
            table.columns(),
            handle_offset,
            &[],
            &[],
            &output.offsets,
        )
        .ok()?,
        output,
        cached_keys: std::sync::Mutex::new(Vec::new()),
    })
}

impl PreparedPointGetPlan {
    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest` of this point plan.
    #[must_use]
    pub fn plan_digest(&self) -> &std::sync::OnceLock<(String, String)> {
        &self.plan_digest
    }

    /// The catalog version against which the point shape was resolved.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// The default database in force when an unqualified table was resolved.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    /// The qualified table name the plan reads, `(database, table)`.
    #[must_use]
    pub fn names(&self) -> (&str, &str) {
        (&self.database, &self.table)
    }

    /// The transaction read policy this retained point plan can safely use.
    /// A row-handle lookup performs one read and may use Go's autocommit
    /// MaxTS optimization. A secondary-unique lookup reads the index entry
    /// and then the row, so Go's `noSecondRead` guard keeps it on an ordinary
    /// timestamped snapshot even though the point executor itself is reusable.
    #[must_use]
    pub const fn statement_read_shape(&self) -> crate::access_path::StatementReadShape {
        match self.target {
            PreparedPointTarget::RowHandle => {
                crate::access_path::StatementReadShape::AutocommitPointGet
            }
            PreparedPointTarget::UniqueIndex { .. } => {
                crate::access_path::StatementReadShape::Unknown
            }
        }
    }

    /// Rebuilds the parameter-dependent handle and resolves every residual
    /// bound against this EXECUTE's parameters. A value that cannot be moved
    /// exactly into its column's domain declines the cache and must be
    /// replanned; any NULL comparison makes the statement match no rows, so it
    /// binds to an execution whose handle is `None` (or whose residuals can
    /// never pass).
    #[must_use]
    pub fn bind(
        self: &Arc<Self>,
        values: &[Datum],
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Option<PreparedPointGetExecution> {
        self.bind_with_environment(values, zone, &PreparedPlanCacheEnvironment::default())
    }

    /// Rebuilds this point plan under Go's complete prepared-plan cache key.
    #[must_use]
    pub fn bind_with_environment(
        self: &Arc<Self>,
        values: &[Datum],
        zone: &tidb_datatype::SessionTimeZone,
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedPointGetExecution> {
        let key = PreparedPointCacheKey {
            schema_version: self.schema_version,
            environment: environment.clone(),
            parameter_types: values.iter().map(PreparedParameterType::of).collect(),
        };
        let cache_hit = self
            .cached_keys
            .lock()
            .ok()
            .is_some_and(|cached| cached.iter().any(|entry| entry.matches(&key)));
        let execution = |handle, range_values, residuals| {
            // Go inserts a generated plan before building or opening its
            // executor. A runtime error therefore does not erase the plan or
            // turn the next identical EXECUTE into another miss.
            if !cache_hit {
                let mut cached = self.cached_keys.lock().ok()?;
                if !cached.iter().any(|entry| entry.matches(&key)) {
                    cached.push(key.clone());
                }
            }
            Some(PreparedPointGetExecution {
                plan: Arc::clone(self),
                handle,
                range_values,
                residuals,
                cache_hit,
                parameters: Arc::from(values),
            })
        };
        if self.contradiction {
            // `NOT NULL col IS NULL` matched no rows at PLAN time; parameters
            // cannot change a schema fact.
            return execution(None, None, Vec::new());
        }
        let mut key_values = Vec::with_capacity(self.parameter_orders.len());
        for (index, handle_type) in self.pin_types.iter().enumerate() {
            let value = match (&self.parameter_orders[index], &self.handle_literals[index]) {
                (Some(order), _) => values.get(*order)?,
                (None, Some(literal)) => literal,
                (None, None) => return None,
            };
            if value.is_null() {
                // `handle = NULL` matches nothing; an empty result IS the
                // answer, and no storage read may run for it.
                return execution(None, None, Vec::new());
            }
            key_values.push(match point_get_value(handle_type, value) {
                Some(value) => value,
                None if point_get_value_overflowed(handle_type, value) => {
                    // Go `getNameValuePairs`'s `ErrOverflow` arm: a value
                    // outside the column's domain can equal no stored row, so
                    // the empty set IS the answer (Go plans a `TableDual`).
                    return execution(None, None, Vec::new());
                }
                None if names_no_rows(handle_type, value) => {
                    // A parameter longer than the column's capacity compares
                    // equal to no stored value: the empty set IS the answer,
                    // the same observable result Go's re-optimized plan
                    // produces, served without re-planning.
                    return execution(None, None, Vec::new());
                }
                None => return None,
            });
        }
        let mut residuals = Vec::with_capacity(self.residuals.len());
        for (position, bound) in &self.residuals {
            let check = match bound {
                PointResidualBound::IsNull => ResidualCheck::IsNull,
                PointResidualBound::Literal(value) => ResidualCheck::Equal(value.clone()),
                PointResidualBound::Param(order) => {
                    let value = values.get(*order)?;
                    if value.is_null() {
                        // `residual = NULL` never passes either.
                        return execution(None, None, Vec::new());
                    }
                    ResidualCheck::Equal(
                        match point_get_value(&self.output.columns[*position].1, value) {
                            Some(value) => value,
                            None if point_get_value_overflowed(
                                &self.output.columns[*position].1,
                                value,
                            ) =>
                            {
                                // The residual equality can never hold, so
                                // no row passes; Go's `ErrOverflow` arm plans
                                // the same empty answer.
                                return execution(None, None, Vec::new());
                            }
                            None if names_no_rows(&self.output.columns[*position].1, value) => {
                                return execution(None, None, Vec::new());
                            }
                            None => return None,
                        },
                    )
                }
            };
            residuals.push((*position, check));
        }
        // A full-handle pin rebuilds the one record key; a unique-index pin
        // carries every index key value into the one closed point range.
        if matches!(self.target, PreparedPointTarget::RowHandle) {
            let first = key_values.first()?;
            let handle = if self.common_handle_offsets.is_empty() {
                match first {
                    Datum::Int(value) => Some(TableHandle::Int(*value)),
                    Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                    _ => None,
                }
            } else {
                let encoded = tidb_codec::encode_key_in_timezone(zone, &key_values).ok()?;
                let handle = tidb_txnkv::CommonHandle::new(encoded).ok()?;
                Some(TableHandle::Common(handle.encoded().to_vec()))
            };
            return execution(handle, None, residuals);
        }
        execution(None, Some(key_values), residuals)
    }

    /// Whether the catalog still names the same unpartitioned physical table.
    #[must_use]
    pub fn matches_catalog(&self, catalog: &Catalog) -> bool {
        if self.schema_version != catalog.schema_version() {
            return false;
        }
        matches!(
            catalog.get_by_key(&self.table_key),
            Some(TableEntry::Kv(table))
                if table.table_id() == self.table_id && table.partition().is_none()
        )
    }
}

/// One cache hit after its execute-time parameter has been rebuilt into a
/// handle. The executor itself is still created fresh by
/// [`run_prepared_point_get`].
/// Which key a prepared point plan's pins name, and so which single read
/// answers them: the full row handle or every column of one non-prefix unique
/// secondary index. These are exactly Go `tryPointGetPlan`'s two point-read
/// families.
#[derive(Clone, Debug)]
pub(crate) enum PreparedPointTarget {
    /// The pins cover every handle column: ONE record-key read.
    RowHandle,
    /// The pins cover every column of this non-prefix unique secondary index.
    UniqueIndex { index_id: i64 },
}

#[derive(Clone, Debug)]
pub struct PreparedPointGetExecution {
    plan: Arc<PreparedPointGetPlan>,
    /// [`PreparedPointTarget::RowHandle`] only: the execute's rebuilt handle.
    /// `None` here — or in [`Self::range_values`] on the other arms — binds a
    /// NULL pin, which matches no row and reads nothing.
    handle: Option<TableHandle>,
    /// The unique-index arm's full key values, already moved into their
    /// columns' domains.
    range_values: Option<Vec<Datum>>,
    /// This execute's residual predicates, parameters already resolved:
    /// `(offset into the output row, the check the decoded slot must pass)`.
    residuals: Vec<(usize, ResidualCheck)>,
    cache_hit: bool,
    /// This execute's parameter values.
    parameters: Arc<[Datum]>,
}

/// The retained SELECT syntax and key metadata. The session cache owns the
/// complete physical operator tree and rebuilds its ranges on a cache hit.
#[derive(Debug)]
pub struct PreparedSelectPlan {
    current_database: String,
    table_names: Vec<(String, String)>,
    // Retain folded keys alongside the original names exposed for MDL.
    table_keys: Vec<CatalogTableKey>,
    /// The tables whose statistics versions key a cache entry (Go
    /// `PlanCacheStmt.tables`), when `tidb_plan_cache_invalidation_on_fresh_stats`
    /// is on.
    stats_table_keys: Vec<CatalogTableKey>,
    parameter_count: usize,
    limit_parameter_orders: Vec<usize>,
    stmt_info: PlanCacheStmtInfo,
    statement: tidb_ast::Stmt,
    cache_key: String,
    last_limit_values: std::sync::Mutex<Vec<u64>>,
    contains_sequence_functions: bool,
}

/// Go `PlanCacheStmt.limits` and `hasSubquery`, as `planCacheStmtProcessor`
/// records them at PREPARE: whether the statement has any LIMIT and any
/// subquery expression. `NewPlanCacheKey` refuses the cache at each EXECUTE
/// while the matching switch is off.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PlanCacheStmtInfo {
    has_limit: bool,
    has_subquery: bool,
}

impl PlanCacheStmtInfo {
    pub(super) fn of(stmt: &tidb_ast::Stmt) -> Self {
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
    pub(super) fn memory_usage(&self) -> usize {
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
    pub(super) const fn refusal(&self, info: PlanCacheStmtInfo) -> Option<&'static str> {
        if info.has_subquery && !self.enable_subquery {
            Some("the switch 'tidb_enable_plan_cache_for_subquery' is off")
        } else if info.has_limit && !self.enable_param_limit {
            Some("the switch 'tidb_enable_plan_cache_for_param_limit' is off")
        } else {
            None
        }
    }

    pub(crate) const fn plan_cacheability(
        &self,
        parameter_count: usize,
    ) -> tidb_planner::physical_plan_cache::PlanCacheabilityContext {
        tidb_planner::physical_plan_cache::PlanCacheabilityContext {
            parameter_count,
            enable_generated_columns: self.enable_generated_columns,
            max_plan_size: self.plan_cache_max_plan_size,
        }
    }

    pub(crate) const fn hashes_fresh_statistics(&self) -> bool {
        self.invalidate_on_fresh_stats && !self.skip_stats_on_binding
    }

    /// Go `NewPlanCacheKey`'s `statsVerHash`: the sum of every table's
    /// `getLatestVersionFromStatsTable(sctx, t.Meta(), t.Meta().ID)`, zero
    /// when the key omits statistics.
    pub(crate) fn stats_version_hash(&self, catalog: &Catalog, keys: &[CatalogTableKey]) -> u64 {
        if !self.hashes_fresh_statistics() {
            return 0;
        }
        keys.iter().fold(0, |hash, key| {
            let version = match catalog.get_by_key(key) {
                Some(TableEntry::Kv(table)) => {
                    crate::access_cost::get_latest_version_from_stats_table(
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

/// Go's prepared-plan cache keys physical plans by the current parameter
/// types. Values with the same type rebuild one retained tree; a different
/// type gets its own physical enumeration instead of inheriting a path chosen
/// for an incompatible comparison domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedParameterType {
    Null,
    Int,
    UInt,
    Decimal {
        precision: i32,
        scale: i32,
    },
    Real,
    Float32,
    String {
        charset: tidb_charset::Charset,
        collation: tidb_charset::Collation,
    },
    Bytes,
    Raw,
    BinaryLiteral,
    Bit,
    Duration,
    Enum,
    Set,
    Date,
    DateTime,
    Timestamp,
    Json,
    VectorFloat32,
    MinNotNull,
    MaxValue,
}

/// One execution lease on a cache-owned, recursively rebuilt physical tree.
/// The generation prevents another bind from silently changing the retained
/// parameter values between admission and executor construction.
#[derive(Debug)]
pub struct PreparedSelectExecution {
    parameters: Arc<[Datum]>,
    plan: Arc<PreparedSelectPlan>,
    schema_version: u64,
    cache_hit: bool,
    cached_plan: Arc<std::sync::Mutex<super::planner_bridge::CachedSelectPlan>>,
    generation: u64,
    planning_warnings: std::sync::Mutex<Vec<tidb_context::SqlWarn>>,
}

impl PreparedSelectPlan {
    /// Go's non-prepared plan cache collects its statistics tables from the
    /// parameterized statement, where an unqualified table name has no
    /// schema and resolves to no table (`planCacheStmtProcessor`): only a
    /// name written with its database invalidates the entry on fresh
    /// statistics.
    #[must_use]
    pub fn with_non_prepared_stats_tables(mut self) -> Self {
        let mut names = Vec::new();
        if let tidb_ast::Stmt::Query(query) = &self.statement {
            collect_prepared_table_names(query, "", &mut names);
        }
        self.stats_table_keys = names
            .iter()
            .map(|(database, table)| CatalogTableKey::new(database, table))
            .collect();
        self
    }

    /// Retain Go's original/parameterized SQL identity alongside PREPARE input.
    #[must_use]
    pub fn with_sql(mut self, sql: &str) -> Self {
        self.cache_key = super::plan_cache::statement_key(&self.current_database, sql);
        self
    }

    /// Go DEALLOCATE/COM_STMT_CLOSE deletes the current environment's key,
    /// including every parameter-type variant, unless the session retains it.
    pub fn discard_cached_plan(
        &self,
        cache: &SessionPlanCache,
        catalog: &Catalog,
        environment: &PreparedPlanCacheEnvironment,
    ) {
        cache.delete(&PhysicalPlanCacheKey {
            statement: self.cache_key.clone(),
            schema_version: catalog.schema_version(),
            stats_version_hash: self.stats_version_hash(catalog, environment),
            environment: environment.clone(),
            limit_values: self
                .last_limit_values
                .lock()
                .expect("prepared limits poisoned")
                .clone(),
            dirty_tables: Vec::new(),
        });
    }

    /// The immutable statement retained at PREPARE time.
    #[must_use]
    pub const fn statement(&self) -> &tidb_ast::Stmt {
        &self.statement
    }

    /// Qualified table names read by this plan, for transaction MDL tracking.
    #[must_use]
    pub fn table_names(&self) -> &[(String, String)] {
        &self.table_names
    }

    /// Whether the prepared statement contains a sequence builtin whose name
    /// must be validated again when the physical executor is opened.
    #[must_use]
    pub const fn contains_sequence_functions(&self) -> bool {
        self.contains_sequence_functions
    }

    /// On the first execution for a schema and parameter-type key, runs the
    /// shared optimizer with the real parameter values and retains its entire
    /// physical tree. A hit recursively rebuilds every parameter-dependent
    /// range in place before extracting its lowering receipt.
    #[must_use]
    pub fn bind(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: &crate::StmtContext,
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedSelectExecution> {
        self.bind_for_statement(
            cache,
            values,
            catalog,
            current_database,
            ctx,
            &ctx.staged_writes().dirty_table_ids(),
            environment,
            &self.statement,
        )
    }

    /// Generates and retains a plan from the statement after a matched SQL
    /// binding has replaced its hints. The binding SQL itself belongs to
    /// `environment`, mirroring Go's prepared-plan cache key.
    #[must_use]
    pub fn bind_for_statement(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: &crate::StmtContext,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Option<PreparedSelectExecution> {
        self.bind_inner(
            cache,
            values,
            catalog,
            current_database,
            Some(ctx),
            dirty_ids,
            environment,
            statement,
        )
    }

    /// Rebuilds an existing cache entry without constructing a planner
    /// statement context. A miss returns `None`; the caller can then build
    /// the context and call [`Self::bind`] to enumerate and retain a plan.
    #[must_use]
    pub fn bind_cached(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedSelectExecution> {
        self.bind_cached_for_statement(
            cache,
            values,
            catalog,
            current_database,
            dirty_ids,
            environment,
            &self.statement,
        )
    }

    /// Rebuilds an entry keyed by the currently matched binding and effective
    /// statement, without allocating a planner context on a hit.
    #[must_use]
    pub fn bind_cached_for_statement(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Option<PreparedSelectExecution> {
        self.bind_inner(
            cache,
            values,
            catalog,
            current_database,
            None,
            dirty_ids,
            environment,
            statement,
        )
    }

    fn bind_inner(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: Option<&crate::StmtContext>,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Option<PreparedSelectExecution> {
        if !super::plan_cache::tables_cacheable(catalog, &self.table_keys) {
            return None;
        }
        // Go keys the entry by `PlanCacheStmt.StmtDB`, the PREPARE-time
        // database its table names were pinned to (`self.cache_key`), so a
        // `USE` between EXECUTEs keeps it; a non-prepared key names the
        // current database itself.
        if !matches!(statement, tidb_ast::Stmt::Query(_)) {
            return None;
        }
        let parameter_types: Arc<[PreparedParameterType]> =
            values.iter().map(PreparedParameterType::of).collect();
        let limit_values = self
            .limit_parameter_orders
            .iter()
            .map(|order| match values.get(*order) {
                Some(Datum::Int(value)) if (0..=10_000).contains(value) => Some(*value as u64),
                Some(Datum::UInt(value)) if *value <= 10_000 => Some(*value),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        self.last_limit_values
            .lock()
            .ok()?
            .clone_from(&limit_values);
        let schema_version = catalog.schema_version();
        let stats_version_hash = self.stats_version_hash(catalog, environment);
        let cache_key = PhysicalPlanCacheKey {
            statement: self.cache_key.clone(),
            schema_version,
            stats_version_hash,
            environment: environment.clone(),
            limit_values,
            dirty_tables: super::plan_cache::dirty_statement_tables(catalog, &self.table_keys, dirty_ids),
        };
        let refusal = environment.refusal(self.stmt_info);
        let admitted = refusal.is_none();
        let cached = match admitted.then(|| cache.get(&cache_key, &parameter_types)).flatten() {
            Some(CachedPhysicalPlan::Select(plan)) => Some(plan),
            _ => None,
        };
        // Go `GetPlanFromPlanCache` refuses with `SetSkipPlanCache`, which
        // warns why.
        if let (Some(reason), Some(ctx)) = (refusal, ctx) {
            ctx.start_prepared_range_tracking();
            ctx.set_skip_plan_cache(reason);
        }
        if cached.is_none() {
            ctx?;
        }
        let parameters: Arc<[Datum]> = Arc::from(values);
        let rebound = match cached {
            Some(plan) => {
                let generation = plan.lock().ok()?.bind(values, ctx);
                match generation {
                    Ok(generation) => Some((plan, generation)),
                    // Every hit rebuilds each range from its access
                    // conditions, so a partially rebuilt entry is safe to
                    // keep, as Go keeps it.
                    Err(failure) if failure.replan(ctx) => None,
                    // A context-free lookup leaves the entry for the bind
                    // that carries the statement.
                    Err(_) => return None,
                }
            }
            None => None,
        };
        let (cached_plan, generation, cache_hit) = match rebound {
            Some((plan, generation)) => (plan, generation, true),
            None => {
                let ctx = ctx?.clone().with_prepared_params(Arc::clone(&parameters));
                let statement = crate::bind_prepared_statement(statement, values).ok()?;
                let tidb_ast::Stmt::Query(query) = statement else {
                    return None;
                };
                let query = query.into_inner();
                let (mut plan, cacheable) = super::planner_bridge::cached_query_plan(
                    &query,
                    catalog,
                    current_database,
                    &ctx,
                    environment.plan_cacheability(self.parameter_count),
                )?;
                // A rejected cache candidate still executes its already-bound plan.
                let cacheable = cacheable && admitted;
                // Go `generateNewPlan` caches the tree it just optimized
                // without rebuilding it; a rebuild this tree cannot pass
                // (a prefix column keeps its filter) fails on the next hit
                // instead, which then replans.
                let generation = if cacheable {
                    plan.bind(values, Some(&ctx)).unwrap_or(0)
                } else {
                    0
                };
                let plan = Arc::new(std::sync::Mutex::new(plan));
                if cacheable {
                    cache.put(
                        cache_key,
                        parameter_types,
                        CachedPhysicalPlan::Select(Arc::clone(&plan)),
                    );
                }
                (plan, generation, false)
            }
        };
        Some(PreparedSelectExecution {
            parameters,
            plan: Arc::clone(self),
            schema_version,
            cache_hit,
            cached_plan,
            generation,
            planning_warnings: std::sync::Mutex::new(if cache_hit {
                Vec::new()
            } else {
                ctx.map_or_else(Vec::new, crate::StmtContext::take_warnings)
            }),
        })
    }

    fn stats_version_hash(
        &self,
        catalog: &Catalog,
        environment: &PreparedPlanCacheEnvironment,
    ) -> u64 {
        environment.stats_version_hash(catalog, &self.stats_table_keys)
    }
}

impl PreparedParameterType {
    pub(crate) fn of(value: &Datum) -> Self {
        match value {
            Datum::Null => Self::Null,
            Datum::Int(_) => Self::Int,
            Datum::UInt(_) => Self::UInt,
            Datum::Decimal(value) => {
                let (precision, scale) = value.precision_and_frac();
                Self::Decimal { precision, scale }
            }
            Datum::Real(_) => Self::Real,
            Datum::Float32(_) => Self::Float32,
            Datum::String(value) => Self::String {
                charset: value.charset(),
                collation: value.collation(),
            },
            Datum::Bytes(_) => Self::Bytes,
            Datum::Raw(_) => Self::Raw,
            Datum::BinaryLiteral(_) => Self::BinaryLiteral,
            Datum::Bit(_) => Self::Bit,
            Datum::Duration(_) => Self::Duration,
            Datum::Enum(_, _) => Self::Enum,
            Datum::Set(_, _) => Self::Set,
            Datum::Time(value) => match value.kind() {
                tidb_datatype::TimeType::Date => Self::Date,
                tidb_datatype::TimeType::DateTime => Self::DateTime,
                tidb_datatype::TimeType::Timestamp => Self::Timestamp,
            },
            Datum::Json(_) => Self::Json,
            Datum::VectorFloat32(_) => Self::VectorFloat32,
            Datum::MinNotNull => Self::MinNotNull,
            Datum::MaxValue => Self::MaxValue,
        }
    }

    /// Go `checkTypesCompatibility4PC`: decimal precision and scale are
    /// asymmetric. A plan generated for a wider decimal can serve a narrower
    /// value, but the reverse must enumerate a new physical plan.
    fn compatible_with(self, actual: Self) -> bool {
        match (self, actual) {
            (
                Self::Decimal {
                    precision: expected_precision,
                    scale: expected_scale,
                },
                Self::Decimal {
                    precision: actual_precision,
                    scale: actual_scale,
                },
            ) => expected_precision >= actual_precision && expected_scale >= actual_scale,
            _ => self == actual,
        }
    }
}

pub(crate) fn prepared_parameter_types_compatible(
    expected: &[PreparedParameterType],
    actual: &[PreparedParameterType],
) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| expected.compatible_with(*actual))
}

impl PreparedSelectExecution {
    /// Values owned by this execution, independent of later cache rebuilds.
    #[must_use]
    pub fn parameters(&self) -> Arc<[Datum]> {
        Arc::clone(&self.parameters)
    }
    /// Takes warnings produced by this execution's cache-miss planning once.
    pub fn take_planning_warnings(&self) -> Vec<tidb_context::SqlWarn> {
        std::mem::take(
            &mut *self
                .planning_warnings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// The immutable plan whose schema identity gates this execution.
    #[must_use]
    pub fn plan(&self) -> &PreparedSelectPlan {
        &self.plan
    }

    /// Whether this execute rebuilt an existing physical plan rather than
    /// generating the first plan for its schema and parameter-type key.
    #[must_use]
    pub const fn cache_hit(&self) -> bool {
        self.cache_hit
    }

    /// The schema identity used when the retained physical tree was built.
    /// The ordinary statement executor compares it with the catalog while
    /// holding the catalog guard and plans normally if a DDL moved it.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Runs a callback while the cache-owned SELECT root is pinned to the
    /// generation rebuilt for this execution. The callback is the ordinary
    /// session statement funnel; this type does not own another executor.
    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest` of the cached plan.
    #[must_use]
    pub fn plan_digest_cell(&self) -> Arc<std::sync::OnceLock<(String, String)>> {
        self.cached_plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .digest()
    }

    pub fn with_plan<R>(
        &self,
        callback: impl FnOnce(&tidb_ast::Stmt, &mut tidb_planner_core_operator_physicalop::PhysicalPlan) -> R,
    ) -> Option<R> {
        let mut cached = self.cached_plan.lock().ok()?;
        let (statement, physical) = cached.execution_mut(self.generation)?;
        Some(callback(statement, physical))
    }
}

#[cfg(test)]
pub(crate) fn run_prepared_select_for_test(
    execution: &PreparedSelectExecution,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let ctx = ctx.clone().with_prepared_params(execution.parameters());
    if execution.schema_version() != catalog.schema_version()
        || !execution
            .plan()
            .current_database
            .eq_ignore_ascii_case(current_database)
    {
        return Err(DriverError::unsupported(
            "prepared SELECT test execution saw a moved schema",
        ));
    }
    execution
        .with_plan(|statement, physical| {
            let tidb_ast::Stmt::Query(query) = statement else {
                unreachable!("a prepared query owns a query statement")
            };
            super::run_query_meta_stmt_with_physical(
                query,
                Some(physical),
                catalog,
                current_database,
                &ctx,
            )
        })
        .ok_or_else(|| {
            DriverError::unsupported(
                "prepared SELECT test generation changed before executor construction",
            )
        })?
}

/// One residual predicate bound to an EXECUTE's parameters.
#[derive(Clone, Debug)]
enum ResidualCheck {
    Equal(Datum),
    IsNull,
}

impl PreparedPointGetExecution {
    /// The immutable plan whose safety gates this execution must satisfy.
    #[must_use]
    /// This execute's parameter values.
    #[must_use]
    pub fn parameters(&self) -> &Arc<[Datum]> {
        &self.parameters
    }

    pub fn plan(&self) -> &PreparedPointGetPlan {
        &self.plan
    }

    /// Whether this execution rebuilt a point plan under an existing complete
    /// prepared-plan cache key.
    #[must_use]
    pub const fn cache_hit(&self) -> bool {
        self.cache_hit
    }
}

/// Recognizes the fail-closed subset of Go prepared plans that can reuse a
/// `PointGetExecutor`: one marker pinning an unpartitioned integer handle and
/// a source-column projection. Hints and every root operator decline.
#[must_use]
pub fn build_prepared_point_get_plan(
    stmt: &tidb_ast::Stmt,
    parameter_count: usize,
    catalog: &Catalog,
    current_database: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<PreparedPointGetPlan> {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return None;
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        return None;
    };
    // A scope-qualified index hint (`USE INDEX FOR JOIN ...`) is inert for a
    // point read: Go's hint processing skips any hint whose scope is not
    // `HintForScan` before it ever reaches the name lookup, so the plan is
    // the same Point_Get the unhinted statement gets. Treat such hints as
    // absent here; only an unscoped (FOR SCAN / no qualifier) hint can
    // change the access path.
    fn join_has_unscoped_index_hint(join: &tidb_ast::Join) -> bool {
        node_has_unscoped_index_hint(&join.left)
            || join
                .right
                .as_ref()
                .is_some_and(node_has_unscoped_index_hint)
    }

    fn node_has_unscoped_index_hint(node: &tidb_ast::JoinNode) -> bool {
        match node {
            tidb_ast::JoinNode::Table(table) => table
                .hints
                .iter()
                .any(|hint| hint.scope == tidb_ast::IndexHintScope::All),
            tidb_ast::JoinNode::Join(join) => join_has_unscoped_index_hint(join),
            _ => false,
        }
    }
    let hints_all_unscoped = select
        .from
        .as_ref()
        .is_none_or(|join| !join_has_unscoped_index_hint(join));
    if !crate::access_path::select_is_bare_point_read(select)
        || (!select.hints.is_empty() && !hints_all_unscoped)
        || select.priority != tidb_ast::StatementPriority::None
        || select.sql_small_result
        || select.sql_big_result
        || select.sql_buffer_result
        || select.sql_no_cache
        || select.straight_join
    {
        return None;
    }
    let table_ref = single_table_ref(&select.from)?;
    if !table_ref.partitions.is_empty()
        || table_ref.as_of.is_some()
        || !prepared_primary_index_hint(table_ref)
        || table_ref.sample.is_some()
    {
        return None;
    }
    let Ok((database, table_name)) = split_table_path(&table_ref.name, current_database) else {
        return None;
    };
    let entry @ TableEntry::Kv(table) = catalog.get_in(database, table_name)? else {
        return None;
    };
    if table.partition().is_some() {
        return None;
    }
    let (handle_offset, common_handle_offsets) = if let Some(offset) = table.pk_handle_offset() {
        (Some(offset), Vec::new())
    } else {
        let offsets = table.common_handle_offsets();
        // A composite common handle encodes its prefix columns in order; the
        // walker below pins each of them exactly once, so any width works.
        // A nonclustered table has no SQL-visible handle columns, but its
        // unique indexes can still supply a PointGet below, as in Go.
        (None, offsets.to_vec())
    };
    let columns = entry.column_list();
    let visible = table_ref.alias.as_deref().unwrap_or(table_name);
    let scope = single_table_scope(
        visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.clone(),
    );
    let mut output = match fast_point_output(select, &scope) {
        Some(output) => output,
        None => return None,
    };
    // The scope lists the visible columns; every offset below is a stored-row
    // offset, the one the table's keys, indexes and decoder address.
    let visible_offsets = table.visible_offsets();
    let stored_offset = |offset: usize| visible_offsets.get(offset).copied();
    for offset in &mut output.offsets {
        // The checksum column's sentinel is no column and stays as it is.
        if let Some(stored) = stored_offset(*offset) {
            *offset = stored;
        }
    }
    let columns: Vec<(String, FieldType)> = table
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    // A generated output can evaluate expressions while its stored row is
    // decoded. Keep those plans on the full statement context; the cached
    // point path below needs only SELECT's temporal/default conversion state.
    if output.offsets.iter().any(|offset| {
        table
            .columns()
            .get(*offset)
            .is_none_or(|column| column.generated.is_some())
    }) {
        return None;
    }
    // One walk flattens the WHERE conjunction into resolved equalities; the
    // handle pins exactly one per handle column and everything else filters
    // the decoded row.
    let conjuncts = match prepared_point_eq_conjuncts(select.where_clause.as_ref()?, &scope, zone) {
        Some(conjuncts) => conjuncts,
        None => return None,
    };
    let resolver = ScopeResolver { scope: &scope };
    let mut resolved = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        match conjunct {
            PreparedPointConjunct::Eq {
                path,
                order,
                literal,
            } => {
                let Some((offset, _, _)) = resolver.resolve(point_get_column_path(&path)) else {
                    return None;
                };
                resolved.push((stored_offset(offset)?, PreparedPointPredicate::Eq(order, literal)));
            }
            PreparedPointConjunct::IsNull { path } => {
                let Some((offset, _, _)) = resolver.resolve(point_get_column_path(&path)) else {
                    return None;
                };
                resolved.push((stored_offset(offset)?, PreparedPointPredicate::IsNull));
            }
        }
    }
    drop(resolver);
    // A schema contradiction ends the search before any key is named: a NOT
    // NULL column can never satisfy `IS NULL`, so the answer is always empty
    // (Go constant-folds this into a `TableDual` and its plan cache keeps it).
    if resolved.iter().any(|(offset, kind)| {
        matches!(kind, PreparedPointPredicate::IsNull)
            && columns[*offset]
                .1
                .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
    }) {
        return contradiction_plan(
            catalog.schema_version(),
            current_database,
            database,
            table_name,
            table.table_id(),
            common_handle_offsets.clone(),
            handle_offset,
            table,
            output,
        );
    }
    let handle_offsets: Vec<usize> = match handle_offset {
        Some(offset) => vec![offset],
        None => common_handle_offsets.to_vec(),
    };
    // Go tries the full primary/common handle first, then the first public
    // non-prefix UNIQUE index whose complete key is pinned. A partial key is
    // an ordinary range plan and belongs to the general cached physical tree.
    let (target, pin_offsets) = if !handle_offsets.is_empty()
        && handle_offsets
            .iter()
            .all(|offset| column_pinned_once(*offset, &resolved))
    {
        (PreparedPointTarget::RowHandle, handle_offsets.clone())
    } else {
        let index = table.point_get_indexes().find(|index| {
            index.unique
                && !index.has_prefix()
                && !index.column_offsets.is_empty()
                && index
                    .column_offsets
                    .iter()
                    .all(|offset| column_pinned_once(*offset, &resolved))
        })?;
        (
            PreparedPointTarget::UniqueIndex { index_id: index.id },
            index.column_offsets.clone(),
        )
    };
    let mut parameter_orders = Vec::with_capacity(pin_offsets.len());
    let mut handle_literals = Vec::with_capacity(pin_offsets.len());
    for offset in &pin_offsets {
        let mut hits = resolved
            .iter()
            .filter(|(pinned, kind)| *pinned == *offset && kind.eq_parts().is_some());
        let Some((_, predicate)) = hits.next() else {
            return None;
        };
        let (marker_order, literal) = match predicate.eq_parts() {
            Some(parts) => parts,
            None => return None,
        };
        if hits.next().is_some() {
            return None;
        }
        // The pin value is encoded into a KEY, so its domain has to order
        // like its bytes; otherwise the cache declines to the planner.
        if !point_byte_safe(&columns[*offset].1) {
            return None;
        }
        parameter_orders.push(marker_order);
        handle_literals.push(literal);
    }
    // Whatever equality the key lookup does not answer filters the decoded
    // row. Each residual column must survive into the output row and compare
    // byte-wise (or natively for non-strings), so the cached check is exactly
    // the scan's own `=`; anything else declines to the ordinary planner.
    let mut residuals = Vec::new();
    for (offset, kind) in &resolved {
        if pin_offsets.contains(offset) {
            continue;
        }
        let position = match output.offsets.iter().position(|o| o == offset) {
            Some(position) => position,
            None => return None,
        };
        match kind {
            PreparedPointPredicate::IsNull => {
                residuals.push((position, PointResidualBound::IsNull));
            }
            PreparedPointPredicate::Eq(marker_order, literal) => {
                let column_type = &output.columns[position].1;
                if !point_byte_safe(column_type) {
                    return None;
                }
                let bound = match marker_order {
                    Some(order) => PointResidualBound::Param(*order),
                    None => {
                        let value = literal.as_ref()?;
                        if value.is_null() {
                            // `residual = NULL` matches no row under SQL
                            // semantics; leave such statements to the ordinary
                            // planner rather than caching an always-empty
                            // answer.
                            return None;
                        }
                        PointResidualBound::Literal(value.clone())
                    }
                };
                residuals.push((position, bound));
            }
        }
    }
    // Every referenced marker must exist when the parameters arrive.
    let max_order = parameter_orders
        .iter()
        .flatten()
        .copied()
        .chain(residuals.iter().filter_map(|(_, bound)| match bound {
            PointResidualBound::Param(order) => Some(*order),
            _ => None,
        }))
        .max();
    if max_order.is_some_and(|order| order >= parameter_count) {
        return None;
    }
    Some(PreparedPointGetPlan {
        plan_digest: std::sync::OnceLock::new(),
        schema_version: catalog.schema_version(),
        current_database: current_database.to_owned(),
        database: database.to_owned(),
        table: table_name.to_owned(),
        table_key: CatalogTableKey::new(database, table_name),
        table_id: table.table_id(),
        parameter_orders,
        pin_types: pin_offsets
            .iter()
            .map(|offset| columns.get(*offset).map(|column| column.1.clone()))
            .collect::<Option<Vec<_>>>()?,
        handle_literals,
        target,
        row_decoder: tidb_table::kv_table::PreparedPointGetRowDecoder::new_with_handle_prefixes(
            table.columns(),
            handle_offset,
            &common_handle_offsets,
            table.common_handle_prefix_lengths(),
            &output.offsets,
        )
        .ok()?,
        common_handle_offsets,
        residuals,
        contradiction: false,
        output,
        cached_keys: std::sync::Mutex::new(Vec::new()),
    })
}

/// Builds the complete shared-planner tree retained by a prepared SELECT.
#[must_use]
pub fn build_prepared_select_plan(
    stmt: &tidb_ast::Stmt,
    parameter_count: usize,
    _catalog: &Catalog,
    current_database: &str,
    _ctx: &crate::StmtContext,
) -> Option<PreparedSelectPlan> {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return None;
    };
    if parsed_parameter_count(stmt) != parameter_count {
        return None;
    }
    let mut table_names = Vec::new();
    collect_prepared_table_names(query, current_database, &mut table_names);
    let limit_parameter_orders = prepared_limit_parameter_orders(stmt);

    let table_keys: Vec<CatalogTableKey> = table_names
        .iter()
        .map(|(database, table)| CatalogTableKey::new(database, table))
        .collect();
    Some(PreparedSelectPlan {
        current_database: current_database.to_owned(),
        stats_table_keys: table_keys.clone(),
        table_keys,
        table_names,
        parameter_count,
        limit_parameter_orders,
        stmt_info: PlanCacheStmtInfo::of(stmt),
        statement: stmt.clone(),
        last_limit_values: std::sync::Mutex::default(),
        cache_key: super::plan_cache::statement_key(current_database, &stmt.restore()),
        contains_sequence_functions: statement_contains_sequence_functions(stmt),
    })
}

fn statement_contains_sequence_functions(stmt: &tidb_ast::Stmt) -> bool {
    struct SequenceFunctionMarker {
        found: bool,
    }

    impl tidb_ast::Visitor for SequenceFunctionMarker {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return self.found;
            };
            let (tidb_ast::Expr::Func { name, .. }
            | tidb_ast::Expr::GenericFuncCall { name, .. }) = expr
            else {
                return self.found;
            };
            self.found = matches!(
                name.to_ascii_lowercase().as_str(),
                "nextval" | "lastval" | "setval"
            );
            self.found
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut marker = SequenceFunctionMarker { found: false };
    let mut statement = stmt.clone();
    tidb_ast::Visitable::accept(&mut statement, &mut marker);
    marker.found
}

pub(super) fn prepared_limit_parameter_orders(stmt: &tidb_ast::Stmt) -> Vec<usize> {
    struct LimitMarkerCollector {
        orders: Vec<usize>,
    }

    impl tidb_ast::Visitor for LimitMarkerCollector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(limit) = node.downcast_ref::<tidb_ast::Limit>() else {
                return false;
            };
            for expression in std::iter::once(&limit.count).chain(limit.offset.as_ref()) {
                if let tidb_ast::Expr::ParamMarker { order, .. } = expression {
                    self.orders.push(*order);
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut statement = stmt.clone();
    let mut collector = LimitMarkerCollector { orders: Vec::new() };
    tidb_ast::Visitable::accept(&mut statement, &mut collector);
    collector.orders.sort_unstable();
    collector.orders.dedup();
    collector.orders
}

fn collect_prepared_table_names(
    query: &tidb_ast::QueryStmt,
    current_database: &str,
    names: &mut Vec<(String, String)>,
) {
    fn collect_query(
        query: &tidb_ast::QueryStmt,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        match query {
            tidb_ast::QueryStmt::Select(select) => {
                collect_join(&select.from, current_database, names);
            }
            tidb_ast::QueryStmt::SetOpr(set) => collect_set(set, current_database, names),
        }
    }

    fn collect_set(
        set: &tidb_ast::SetOprStmt,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        for term in &set.terms {
            match &term.body {
                tidb_ast::SetOprTermBody::Select(select) => {
                    collect_join(&select.from, current_database, names);
                }
                tidb_ast::SetOprTermBody::Nested(set) => {
                    collect_set(set, current_database, names);
                }
            }
        }
    }

    fn collect(
        node: &tidb_ast::JoinNode,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        match node {
            tidb_ast::JoinNode::Table(table) => {
                if let Ok((database, name)) = split_table_path(&table.name, current_database) {
                    let item = (database.to_owned(), name.to_owned());
                    if !names.iter().any(|present| present == &item) {
                        names.push(item);
                    }
                }
            }
            tidb_ast::JoinNode::Join(join) => {
                collect(&join.left, current_database, names);
                if let Some(right) = &join.right {
                    collect(right, current_database, names);
                }
            }
            tidb_ast::JoinNode::Derived { subquery, .. } => {
                collect_query(subquery, current_database, names);
            }
        }
    }

    fn collect_join(
        from: &Option<tidb_ast::Join>,
        current_database: &str,
        names: &mut Vec<(String, String)>,
    ) {
        if let Some(from) = from {
            collect(&from.left, current_database, names);
            if let Some(right) = &from.right {
                collect(right, current_database, names);
            }
        }
    }

    collect_query(query, current_database, names);
}

/// One `column = ?`, `column = const`, or `column IS NULL` conjunct of a
/// prepared point read's WHERE, its column path kept UNRESOLVED until the
/// builder maps it through the statement's scope.
enum PreparedPointConjunct {
    Eq {
        path: Vec<String>,
        order: Option<usize>,
        literal: Option<Datum>,
    },
    IsNull {
        path: Vec<String>,
    },
}

/// One WHERE conjunct RESOLVED to its column offset. `Eq` may pin a key;
/// `IsNull` is inherently a row-level check (NULL never equals a key value).
enum PreparedPointPredicate {
    Eq(Option<usize>, Option<Datum>),
    IsNull,
}

type ResolvedConjunct = (usize, PreparedPointPredicate);

impl PreparedPointPredicate {
    fn eq_parts(&self) -> Option<(Option<usize>, Option<Datum>)> {
        match self {
            PreparedPointPredicate::Eq(order, literal) => Some((*order, literal.clone())),
            PreparedPointPredicate::IsNull => None,
        }
    }
}

/// Flattens a WHERE conjunction into `(column path, ? order, literal datum)`
/// triples — exactly [`point_equal_pairs`]' shape rule with markers kept
/// SYMBOLIC so a PREPARE-time plan can bind them per EXECUTE. Constants
/// evaluate once here (a NULL literal survives as `Some(Datum::Null)`);
/// anything outside `col = constant|?`, including ORs, comparisons of two
/// columns, or functions of row columns, refuses the whole conjunction.
fn prepared_point_eq_conjuncts(
    where_clause: &tidb_ast::Expr,
    scope: &FromScope,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Vec<PreparedPointConjunct>> {
    use tidb_ast::{BinaryOp, Expr};
    fn unparenthesized(expr: &Expr) -> &Expr {
        match expr {
            Expr::Paren(inner) => unparenthesized(inner),
            other => other,
        }
    }
    fn walk(
        expr: &Expr,
        zone: &tidb_datatype::SessionTimeZone,
        out: &mut Vec<PreparedPointConjunct>,
    ) -> bool {
        match expr {
            Expr::Paren(inner) => walk(inner, zone, out),
            Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
                walk(lhs, zone, out) && walk(rhs, zone, out)
            }
            Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
                let (column, value_expr) = match (unparenthesized(lhs), unparenthesized(rhs)) {
                    (Expr::Column(path), other) => (path, other),
                    (other, Expr::Column(path)) => (path, other),
                    _ => return false,
                };
                if column.is_empty() {
                    return false;
                }
                match unparenthesized(value_expr) {
                    Expr::ParamMarker { order, .. } => {
                        out.push(PreparedPointConjunct::Eq {
                            path: column.clone(),
                            order: Some(*order),
                            literal: None,
                        });
                        true
                    }
                    _ => {
                        let Ok(rewritten) = rewrite_expr_resolved(
                            value_expr,
                            &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                        ) else {
                            return false;
                        };
                        let Expression::Constant(constant) = rewritten else {
                            return false;
                        };
                        let Ok(value) = constant.eval() else {
                            return false;
                        };
                        out.push(PreparedPointConjunct::Eq {
                            path: column.clone(),
                            order: None,
                            literal: Some(value),
                        });
                        true
                    }
                }
            }
            Expr::Is { expr, target, not }
                if matches!(target, tidb_ast::IsTarget::Null) && !*not =>
            {
                // `col IS NULL`: a row-level check (NULL never equals, so it
                // can never pin a key), admitted beside the equalities.
                let Expr::Column(path) = unparenthesized(expr) else {
                    return false;
                };
                if path.is_empty() {
                    return false;
                }
                out.push(PreparedPointConjunct::IsNull { path: path.clone() });
                true
            }
            _ => false,
        }
    }
    let _ = scope;
    let mut out = Vec::new();
    if walk(where_clause, zone, &mut out) && !out.is_empty() {
        Some(out)
    } else {
        None
    }
}

/// YCSB's MySQL adapter pins every single-row lookup with
/// `FORCE INDEX(PRIMARY)`. That hint does not alter a clustered-handle point
/// read, so it is safe to retain in the prepared cache; all other hint shapes
/// stay on the ordinary planner to preserve their access-path semantics.
fn prepared_primary_index_hint(table_ref: &tidb_ast::TableRef) -> bool {
    table_ref.hints.iter().all(|hint| {
        matches!(
            (hint.kind, hint.scope, hint.indexes.as_slice()),
            (
                tidb_ast::IndexHintKind::Force | tidb_ast::IndexHintKind::Use,
                tidb_ast::IndexHintScope::All,
                [name]
            ) if name.go_lower_eq("PRIMARY")
        )
    })
}

/// Materializes explicitly for callers that require owned values.
pub fn run_prepared_point_get(
    execution: &PreparedPointGetExecution,
    catalog: &Catalog,
    ctx: &tidb_table::kv_table::PreparedPointGetDecodeContext,
    stmt_ctx: &crate::StmtContext,
) -> Result<Option<SelectMeta>, DriverError> {
    open_prepared_point_get(execution, catalog, ctx, stmt_ctx)?
        .map(super::QueryRecordSet::collect)
        .transpose()
}

/// Opens a rebound cached point plan without reading rows. None is a schema
/// cache miss; immutable metadata is retained until the record set closes.
pub fn open_prepared_point_get(
    execution: &PreparedPointGetExecution,
    catalog: &Catalog,
    ctx: &tidb_table::kv_table::PreparedPointGetDecodeContext,
    stmt_ctx: &crate::StmtContext,
) -> Result<Option<super::QueryRecordSet>, DriverError> {
    let plan = execution.plan();
    if !plan.matches_catalog(catalog) {
        return Ok(None);
    }
    let Some(table) = catalog.table_handle_by_key(&plan.table_key) else {
        return Ok(None);
    };
    let TableEntry::Kv(kv) = table.as_ref() else {
        return Ok(None);
    };
    kv.configure_snapshot_read(stmt_ctx, 0.0);
    let schema = fast_point_schema(kv, &plan.output);
    let columns = plan.output.columns.clone();
    // The point plan's output names: each column is a column of the one
    // table (Go buildSchemaFromFields).
    let output_names = plan
        .output
        .offsets
        .iter()
        .zip(&columns)
        .map(|(&offset, (name, _))| tidb_datatype::FieldNameMetadata {
            original_table: tidb_datatype::IdentifierMetadata::new(plan.table.clone()),
            original_column: tidb_datatype::IdentifierMetadata::new(
                kv.columns()
                    .get(offset)
                    .map_or_else(String::new, |column| column.name.clone()),
            ),
            database: tidb_datatype::IdentifierMetadata::new(plan.database.clone()),
            table: tidb_datatype::IdentifierMetadata::new(plan.output.table_alias.clone()),
            column: tidb_datatype::IdentifierMetadata::new(name.clone()),
        })
        .collect();
    // Go buildPointGet sets both capacities to one (point_get.go:94).
    let executor = PreparedPointGetExecutor {
        meta: crate::ExecutorMeta::new(schema, 0, 1, 1),
        table,
        execution: execution.clone(),
        decode: ctx.clone(),
        context: stmt_ctx.clone(),
        done: false,
    };
    let current_db = stmt_ctx.current_database_name().unwrap_or_default();
    super::QueryRecordSet::open(Box::new(executor), columns, stmt_ctx.statement_memory())
        .map(|record_set| Some(record_set.with_output_names(output_names, current_db)))
}

struct PreparedPointGetExecutor {
    meta: crate::ExecutorMeta,
    table: Arc<TableEntry>,
    execution: PreparedPointGetExecution,
    decode: tidb_table::kv_table::PreparedPointGetDecodeContext,
    context: crate::StmtContext,
    done: bool,
}

impl Executor for PreparedPointGetExecutor {
    fn open(&mut self) -> Result<(), ExecError> {
        self.done = false;
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if std::mem::replace(&mut self.done, true) {
            return Ok(());
        }
        let execution = &self.execution;
        let plan = execution.plan();
        // Go marks done before the read, including empty keys and failed reads.
        if execution.handle.is_none() && execution.range_values.is_none() {
            return Ok(());
        }
        let TableEntry::Kv(table) = self.table.as_ref() else {
            unreachable!("validated point table")
        };
        let mut reader = table.point_reader();
        let before = reader.point_rpc_counts();
        let decode_error = |error: tidb_table::kv_table::KvTableError| {
            ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
        };
        let handle = match plan.target {
            PreparedPointTarget::RowHandle => execution.handle.clone(),
            PreparedPointTarget::UniqueIndex { index_id } => reader
                .lookup_unique(
                    index_id,
                    execution
                        .range_values
                        .as_deref()
                        .expect("index point binds key values"),
                    self.decode.zone(),
                )
                .map_err(decode_error)?,
        };
        let row = match handle {
            Some(handle) => reader
                .get_prepared_point_row(&handle, &plan.row_decoder, &self.decode)
                .map_err(decode_error)?,
            None => None,
        }
        .filter(|row| residuals_pass(row, &plan.output.columns, &execution.residuals));
        if let Some(row) = row {
            for (column, value) in row.iter().enumerate() {
                req.append_datum(column, value);
            }
        }
        let after = reader.point_rpc_counts();
        let index_id = match plan.target {
            PreparedPointTarget::RowHandle => tidb_executor_internal_exec::index_usage_reporter::cluster_index_id(table),
            PreparedPointTarget::UniqueIndex { index_id } => Some(index_id),
        };
        if let Some(index_id) = index_id {
            tidb_executor_internal_exec::index_usage_reporter::IndexUsageReporter::new(
                self.context.index_usage_collector(),
            )
            // Go's fast point plan never runs `initStats`, and a PointGet
            // builds its reporter without `LoadTableStats`: no `UsedStatsInfo`
            // entry, so the read counts against `math.MaxInt32` rows.
            .report_point(
                table.table_id(),
                None,
                index_id,
                after.0.wrapping_sub(before.0),
                req.num_rows() as u64,
            );
        }
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
    fn schema(&self) -> &Schema {
        self.meta.schema()
    }
    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }
    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }
    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }
    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// The residual gate shared by every prepared point-read arm: each unconsumed
/// predicate must hold on the decoded row -- an `=` compared in its own
/// domain, or a NULL present where `IS NULL` demanded one.
fn residuals_pass(
    row: &[Datum],
    columns: &[(String, FieldType)],
    residuals: &[(usize, ResidualCheck)],
) -> bool {
    residuals.iter().all(|(position, check)| match check {
        ResidualCheck::IsNull => row.get(*position).is_some_and(Datum::is_null),
        ResidualCheck::Equal(expected) => row.get(*position).is_some_and(|actual| {
            actual
                .compare(expected, (&columns[*position].1).collation())
                .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
        }),
    })
}

/// Whether ONE equality conjunct names this column (an `IS NULL` beside it
/// does not compete for a key). The column pins a key only if exactly one
/// such equality exists and nothing else touches it.
fn column_pinned_once(offset: usize, resolved: &[ResolvedConjunct]) -> bool {
    let mut hits = resolved
        .iter()
        .filter(|(pinned, kind)| *pinned == offset && kind.eq_parts().is_some());
    hits.next().is_some() && hits.next().is_none()
}

/// Whether equality over this column's domain can be answered from KEY BYTES
/// alone: a binary collation (or a non-string domain). A key PIN needs this
/// because its value is encoded into the lookup key, and a residual
/// comparison needs it because it compares decoded bytes against the bound.
fn point_byte_safe(field_type: &FieldType) -> bool {
    field_type.eval_type() != tidb_datatype::EvalType::String
        || matches!(
            field_type.collation(),
            tidb_charset::Collation::Binary
                | tidb_charset::Collation::AsciiBin
                | tidb_charset::Collation::Latin1Bin
                | tidb_charset::Collation::Utf8Bin
                | tidb_charset::Collation::Utf8Mb4Bin
                | tidb_charset::Collation::Utf8Mb40900Bin
        )
}

/// Go `TryFastPlan`'s physical point plan for `statement` and the process
/// info Go derives from it (`EncodePlan`, `NormalizePlan`); `None` when the
/// statement is no fast point read.
#[must_use]
pub fn fast_point_process_plan_info(
    statement: &tidb_ast::Stmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Option<crate::ProcessPlanInfo> {
    let tidb_ast::Stmt::Query(query) = statement else {
        return None;
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        return None;
    };
    let physical = try_fast_point_physical_plan(select, catalog, current_database, ctx).ok()??;
    Some(crate::explain::process_plan_info(ctx, &physical, catalog))
}
