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
use super::plan_cache::SessionPlanCache;
use super::*;
pub use tidb_planner::plan_cache_utils::{PlanCacheStmtInfo, PreparedPlanCacheEnvironment};
use std::sync::Arc;


/// The retained SELECT syntax and key metadata. The session cache owns the
/// complete physical operator tree and rebuilds its ranges on a cache hit.
#[derive(Debug)]
pub struct PreparedSelectPlan {
    /// Go `PlanCacheStmt`.
    stmt: tidb_planner::plan_cache::PlanCacheStmt,
    /// The qualified tables the statement reads, for MDL.
    table_names: Vec<(String, String)>,
    contains_sequence_functions: bool,
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
    cached_plan: Arc<std::sync::Mutex<super::planner_bridge::CachedPlan>>,
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
        if let tidb_ast::Stmt::Query(query) = &self.stmt.statement {
            collect_prepared_table_names(query, "", &mut names);
        }
        self.stmt.stats_table_keys = names
            .iter()
            .map(|(database, table)| CatalogTableKey::new(database, table))
            .collect();
        self
    }

    /// Retain Go's original/parameterized SQL identity alongside PREPARE input.
    #[must_use]
    pub fn with_sql(mut self, sql: &str) -> Self {
        self.stmt.set_sql(sql);
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
        self.stmt.discard_cached_plan(cache, catalog, environment);
    }

    /// The immutable statement retained at PREPARE time.
    #[must_use]
    pub const fn statement(&self) -> &tidb_ast::Stmt {
        &self.stmt.statement
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
            &self.stmt.statement,
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
            &self.stmt.statement,
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
        // Go keys the entry by `PlanCacheStmt.StmtDB`, the PREPARE-time
        // database its table names were pinned to, so a `USE` between
        // EXECUTEs keeps it; a non-prepared key names the current database.
        if !matches!(statement, tidb_ast::Stmt::Query(_)) {
            return Ok(None);
        }
        let parameter_count = self.stmt.parameter_count;
        let Some(found) = self.stmt.get_plan_from_plan_cache(
            cache,
            values,
            catalog,
            ctx,
            dirty_ids,
            environment,
            statement,
            |bound, ctx| {
                // Go `GetPlanFromPlanCache` enables the cache before
                // `generateNewPlan` optimizes, so the LIKE fallback's
                // `SetSkipPlanCache` over a parameter warns on this statement.
                ctx.start_prepared_range_tracking();
                let tidb_ast::Stmt::Query(query) = bound else {
                    return Ok(None);
                };
                super::planner_bridge::cached_query_plan(
                    &super::planner_bridge::driver_hooks(catalog),
                    &query.into_inner(),
                    catalog,
                    current_database,
                    ctx,
                    environment.plan_cacheability(parameter_count),
                )
            },
        )?
        else {
            return Ok(None);
        };
        Ok(Some(PreparedSelectExecution {
            parameters: found.parameters,
            plan: Arc::clone(self),
            schema_version: found.schema_version,
            cache_hit: found.cache_hit,
            cached_plan: found.cached_plan,
            generation: found.generation,
            planning_warnings: std::sync::Mutex::new(found.planning_warnings),
        }))
    }

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
            .stmt
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
        stmt: tidb_planner::plan_cache::PlanCacheStmt {
            current_database: current_database.to_owned(),
            stats_table_keys: table_keys.clone(),
            table_keys,
            parameter_count,
            limit_parameter_orders,
            stmt_info: PlanCacheStmtInfo::of(stmt),
            statement: stmt.clone(),
            last_limit_values: std::sync::Mutex::default(),
            cache_key: super::plan_cache::statement_key(current_database, &stmt.restore()),
        },
        table_names,
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
