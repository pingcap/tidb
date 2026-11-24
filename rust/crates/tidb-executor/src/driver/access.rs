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
use super::*;
pub use tidb_planner::plan_cache_utils::{PlanCacheStmtInfo, PreparedPlanCacheEnvironment};
use std::sync::Arc;


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
        catalog: &SessionExtendedInfoSchema,
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
            relate_version: super::plan_cache::relate_versions(catalog, &self.table_keys),
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
        catalog: &SessionExtendedInfoSchema,
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
        .ok()
        .flatten()
    }

    /// Generates and retains a plan from the statement after a matched SQL
    /// binding has replaced its hints. The binding SQL itself belongs to
    /// `environment`, mirroring Go's prepared-plan cache key.
    ///
    /// # Errors
    /// The optimizer's error on a miss (Go `generateNewPlan`).
    pub fn bind_for_statement(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &SessionExtendedInfoSchema,
        current_database: &str,
        ctx: &crate::StmtContext,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Result<Option<PreparedSelectExecution>, DriverError> {
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
        catalog: &SessionExtendedInfoSchema,
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
        catalog: &SessionExtendedInfoSchema,
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
        .ok()
        .flatten()
    }

    fn bind_inner(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &SessionExtendedInfoSchema,
        current_database: &str,
        ctx: Option<&crate::StmtContext>,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &tidb_ast::Stmt,
    ) -> Result<Option<PreparedSelectExecution>, DriverError> {
        if !super::plan_cache::tables_cacheable(catalog, &self.table_keys) {
            return Ok(None);
        }
        // Go keys the entry by `PlanCacheStmt.StmtDB`, the PREPARE-time
        // database its table names were pinned to (`self.cache_key`), so a
        // `USE` between EXECUTEs keeps it; a non-prepared key names the
        // current database itself.
        if !matches!(statement, tidb_ast::Stmt::Query(_)) {
            return Ok(None);
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
            .collect::<Option<Vec<_>>>();
        let Some(limit_values) = limit_values else {
            return Ok(None);
        };
        let Ok(mut last_limit_values) = self.last_limit_values.lock() else {
            return Ok(None);
        };
        last_limit_values.clone_from(&limit_values);
        drop(last_limit_values);
        let schema_version = catalog.schema_version();
        let stats_version_hash = self.stats_version_hash(catalog, environment);
        let cache_key = PhysicalPlanCacheKey {
            statement: self.cache_key.clone(),
            schema_version,
            stats_version_hash,
            environment: environment.clone(),
            limit_values,
            dirty_tables: super::plan_cache::dirty_statement_tables(catalog, &self.table_keys, dirty_ids),
            relate_version: super::plan_cache::relate_versions(catalog, &self.table_keys),
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
        if cached.is_none() && ctx.is_none() {
            return Ok(None);
        }
        let parameters: Arc<[Datum]> = Arc::from(values);
        let rebound = match cached {
            Some(plan) => {
                let Ok(mut guard) = plan.lock() else {
                    return Ok(None);
                };
                let generation = guard.bind(values, ctx);
                drop(guard);
                match generation {
                    Ok(generation) => Some((plan, generation)),
                    // Every hit rebuilds each range from its access
                    // conditions, so a partially rebuilt entry is safe to
                    // keep, as Go keeps it.
                    Err(failure) if failure.replan(ctx) => None,
                    // A context-free lookup leaves the entry for the bind
                    // that carries the statement.
                    Err(_) => return Ok(None),
                }
            }
            None => None,
        };
        let (cached_plan, generation, cache_hit) = match rebound {
            Some((plan, generation)) => (plan, generation, true),
            None => {
                let Some(ctx) = ctx else {
                    return Ok(None);
                };
                let ctx = ctx.clone().with_prepared_params(Arc::clone(&parameters));
                let Ok(statement) = crate::bind_prepared_statement(statement, values) else {
                    return Ok(None);
                };
                // Go `GetPlanFromPlanCache` enables the cache before
                // `generateNewPlan` optimizes, so the LIKE fallback's
                // `SetSkipPlanCache` over a parameter warns on this statement.
                ctx.start_prepared_range_tracking();
                let tidb_ast::Stmt::Query(query) = statement else {
                    return Ok(None);
                };
                let query = query.into_inner();
                let Some((mut plan, cacheable)) = super::planner_bridge::cached_query_plan(&crate::driver::planner_bridge::driver_hooks(catalog), &query,
                    catalog,
                    current_database,
                    &ctx,
                    environment.plan_cacheability(self.parameter_count),
                )?
                else {
                    return Ok(None);
                };
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
        Ok(Some(PreparedSelectExecution {
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
        }))
    }

    fn stats_version_hash(
        &self,
        catalog: &SessionExtendedInfoSchema,
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
    catalog: &SessionExtendedInfoSchema,
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

/// Builds the complete shared-planner tree retained by a prepared SELECT.
#[must_use]
pub fn build_prepared_select_plan(
    stmt: &tidb_ast::Stmt,
    parameter_count: usize,
    _catalog: &SessionExtendedInfoSchema,
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
