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

//! Go `pkg/planner/core/plan_cache.go`: a prepared statement's parameter
//! values bound onto its markers (`SetParameterValuesIntoSCtx`) and the
//! cached physical plan a hit rebuilds and runs (`GetPlanFromPlanCache`,
//! `generateNewPlan`).

use crate::plan_cache_instance::InstancePlanCache;
use crate::plan_cache_lru::{LruPlanCache, PlanCacheValue};
pub use crate::plan_cache_utils::PhysicalPlanCacheKey;
use crate::plan_cache_utils::{PreparedParameterType, prepared_parameter_types_compatible};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_driver_error::DriverError;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_sessionctx::StmtContext;

use crate::plan_cache_rebuild::RebuildPlanForCache as _;

/// Binds a prepared statement's parameters, replacing every `?` marker with
/// the literal for its execute-time value.
///
/// Go keeps the parsed statement and installs the values on the markers
/// themselves; this tier reaches execution through SQL text, so the markers
/// become literals and the statement is restored. That round trip is exact
/// for every value kind `datum_to_literal` covers, and a byte string that is
/// not UTF-8 becomes a hex literal rather than a lossy conversion.
///
/// Returns the bound SQL, or `ErrWrongParamCount` when the count does not
/// match the markers the statement carries.
pub fn bind_parameters(
    sql: &str,
    values: &[Datum],
    // The scanner `sql_mode` the statement was PREPARED under: binding
    // re-parses the prepared text, and the restore below then writes it in
    // the parser's canonical form, exactly as Go's stored AST would.
    sql_mode: tidb_lexer::SqlMode,
) -> Result<String, DriverError> {
    let stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::from(tidb_util::misc::syntax_error(format!("{e:?}"))))?;
    bind_statement(stmt, values).map(|stmt| stmt.restore())
}

/// Binds execute-time values into an already parsed prepared statement.
///
/// Go keeps the parsed AST on the prepared statement and changes only its
/// parameter-marker nodes for each execute.  Keeping this seam separate from
/// [`bind_parameters`] lets the wire front end reuse that parse without
/// changing the ordinary text-query path.
pub fn bind_statement(mut stmt: Stmt, values: &[Datum]) -> Result<Stmt, DriverError> {
    let mut bound = 0usize;
    bind_statement_markers(&mut stmt, values, &mut bound)?;
    if bound != values.len() {
        return Err(DriverError::from(
            tidb_plannererrors::ERR_WRONG_PARAM_COUNT.gen_with_stack_by_args(&[]),
        ));
    }
    Ok(stmt)
}

/// Binds execute-time values into a clone of the AST retained by PREPARE.
///
/// Go stores `PlanCacheStmt.PreparedAst` and assigns values to its parameter
/// markers without lexing the SQL text again. Cloning before binding keeps the
/// retained tree immutable across executions and retries while preserving the
/// SQL mode that gave the statement its meaning at PREPARE time.
pub fn bind_prepared_statement(stmt: &Stmt, values: &[Datum]) -> Result<Stmt, DriverError> {
    let mut bound_stmt = stmt.clone();
    let mut bound = 0usize;
    install_statement_marker_values(&mut bound_stmt, values, &mut bound)?;
    if bound != values.len() {
        return Err(DriverError::from(
            tidb_plannererrors::ERR_WRONG_PARAM_COUNT.gen_with_stack_by_args(&[]),
        ));
    }
    Ok(bound_stmt)
}

/// Recover the current parameters at the owned, marker-bearing AST entry.
/// Cached executions carry this snapshot directly and do not walk the AST.
pub fn bound_parameter_values(
    stmt: &mut Stmt,
) -> Result<Option<std::sync::Arc<[Datum]>>, DriverError> {
    let mut values = Vec::new();
    walk_markers(stmt, &mut |expr| {
        let tidb_ast::Expr::ParamMarker { order, value, .. } = expr else {
            unreachable!()
        };
        if values.len() <= *order {
            values.resize_with(*order + 1, || None);
        }
        values[*order] = value
            .as_ref()
            .and_then(|value| value.downcast_ref::<Datum>())
            .cloned();
    });
    if values.is_empty() {
        return Ok(None);
    }
    values
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .map(|values| Some(values.into()))
        .ok_or(DriverError::from(
            tidb_plannererrors::ERR_WRONG_PARAM_COUNT.gen_with_stack_by_args(&[]),
        ))
}

/// Rebinds markers on the complete statement retained by a cached DML root.
/// SELECT and DML cache entries use this same complete-statement operation;
/// marker nodes remain present so their physical roots can rebuild
/// parameter-derived ranges on every execution.
pub fn bind_prepared_statement_in_place(
    stmt: &mut Stmt,
    values: &[Datum],
) -> Result<(), DriverError> {
    let mut bound = 0usize;
    install_statement_marker_values(stmt, values, &mut bound)?;
    if bound != values.len() {
        return Err(DriverError::from(
            tidb_plannererrors::ERR_WRONG_PARAM_COUNT.gen_with_stack_by_args(&[]),
        ));
    }
    Ok(())
}

/// Installs execute-time values without replacing marker nodes. Go's
/// `ParamMarkerExpr` embeds a `ValueExpr`, so the ordinary planner sees both
/// the current datum and the stable marker order. Cached physical expressions
/// can then retain `Constant.ParamMarker` and rebind it recursively.
fn install_statement_marker_values(
    stmt: &mut Stmt,
    values: &[Datum],
    bound: &mut usize,
) -> Result<(), DriverError> {
    install_marker_values(stmt, values, bound)
}

fn install_marker_values<T: tidb_ast::Visitable>(
    node: &mut T,
    values: &[Datum],
    bound: &mut usize,
) -> Result<(), DriverError> {
    let mut failure = None;
    walk_markers(node, &mut |expr| {
        let tidb_ast::Expr::ParamMarker {
            order,
            in_execute,
            value,
            ..
        } = expr
        else {
            return;
        };
        match values.get(*order) {
            Some(parameter) => {
                *value = Some(tidb_ast::ParamValue::new(parameter.clone()));
                *in_execute = true;
                *bound += 1;
            }
            None => {
                failure = Some(DriverError::from(
                    tidb_plannererrors::ERR_WRONG_PARAM_COUNT.gen_with_stack_by_args(&[]),
                ))
            }
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The number of `?` markers a statement carries, which `COM_STMT_PREPARE`
/// reports to the client.
pub fn parameter_count(sql: &str, sql_mode: tidb_lexer::SqlMode) -> Result<usize, DriverError> {
    let mut stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::from(tidb_util::misc::syntax_error(format!("{e:?}"))))?;
    let mut counted = 0usize;
    // Counting binds nothing: every marker reports itself and stays put.
    count_statement_markers(&mut stmt, &mut counted);
    Ok(counted)
}

/// Counts markers on an already parsed statement without changing its tree.
#[must_use]
pub fn parsed_parameter_count(stmt: &Stmt) -> usize {
    let mut stmt = stmt.clone();
    let mut counted = 0usize;
    count_statement_markers(&mut stmt, &mut counted);
    counted
}

/// Walks a statement's expressions, applying `visit` to every marker.
fn walk_markers<T: tidb_ast::Visitable>(node: &mut T, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    struct MarkerVisitor<'a> {
        visit: &'a mut dyn FnMut(&mut tidb_ast::Expr),
    }

    impl tidb_ast::Visitor for MarkerVisitor<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr @ tidb_ast::Expr::ParamMarker { .. }) =
                node.downcast_mut::<tidb_ast::Expr>()
            else {
                return false;
            };
            (self.visit)(expr);
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    tidb_ast::Visitable::accept(node, &mut MarkerVisitor { visit });
}

/// Replaces each marker with its value, in the parser's own left-to-right
/// marker order.
fn bind_statement_markers<N: tidb_ast::Visitable>(
    stmt: &mut N,
    values: &[Datum],
    bound: &mut usize,
) -> Result<(), DriverError> {
    let mut failure = None;
    walk_markers(stmt, &mut |expr| {
        let order = match expr {
            tidb_ast::Expr::ParamMarker { order, .. } => *order,
            _ => return,
        };
        match values.get(order) {
            Some(value) => match datum_to_literal(value) {
                Ok(literal) => {
                    *expr = literal;
                    *bound += 1;
                }
                Err(error) => failure = Some(error),
            },
            None => {
                failure = Some(DriverError::from(
                    tidb_plannererrors::ERR_WRONG_PARAM_COUNT.gen_with_stack_by_args(&[]),
                ))
            }
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Counts the markers without changing them.
fn count_statement_markers(stmt: &mut Stmt, counted: &mut usize) {
    walk_markers(stmt, &mut |_| *counted += 1);
}

#[cfg(test)]
mod tests {
    use super::{bind_parameters, parameter_count};
    use tidb_datatype::Datum;

    #[test]
    fn markers_in_derived_tables_and_join_conditions_are_counted_and_bound() {
        let sql = "SELECT COUNT(*) FROM (SELECT o.o_id FROM orders o \
                   LEFT JOIN order_line ol ON ol.ol_o_id = ? \
                   WHERE o.o_w_id = ?) AS t WHERE t.o_id > 0";
        let mode = tidb_lexer::SqlMode::default();

        assert_eq!(parameter_count(sql, mode).unwrap(), 2);
        let bound = bind_parameters(sql, &[Datum::Int(7), Datum::Int(3)], mode).unwrap();
        assert_eq!(parameter_count(&bound, mode).unwrap(), 0);
        assert!(bound.contains("`ol`.`ol_o_id`=7"), "{bound}");
        assert!(bound.contains("`o`.`o_w_id`=3"), "{bound}");
    }

    #[test]
    fn markers_inside_row_in_are_bound_in_order() {
        let sql = "SELECT o_d_id FROM orders WHERE (o_w_id, o_d_id, o_id) IN ((?,?,?),(?,?,?))";
        let mode = tidb_lexer::SqlMode::default();
        let bound = bind_parameters(
            sql,
            &[
                Datum::Int(1),
                Datum::Int(2),
                Datum::Int(3),
                Datum::Int(4),
                Datum::Int(5),
                Datum::Int(6),
            ],
            mode,
        )
        .unwrap();
        assert_eq!(parameter_count(&bound, mode).unwrap(), 0);
        assert!(bound.contains("ROW(1,2,3)"), "{bound}");
        assert!(bound.contains("ROW(4,5,6)"), "{bound}");
    }
}

/// Go `PlanCacheValue.Plan` beside the `PlanCacheStmt.PreparedAst` it was
/// planned from, for any cached statement (a query, an UPDATE or a DELETE). A
/// hit recursively rebuilds every parameter-dependent range in place
/// (`RebuildPlan4CachedPlan`); nothing is re-planned.
#[derive(Debug)]
pub struct CachedPlan {
    statement: tidb_ast::Stmt,
    physical: PhysicalPlan,
    generation: u64,
    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest`, shared by every
    /// execution of this entry.
    digest: std::sync::Arc<std::sync::OnceLock<(String, String)>>,
}

impl CachedPlan {
    /// An entry `generateNewPlan` just planned, not yet rebuilt.
    #[must_use]
    pub fn new(statement: tidb_ast::Stmt, physical: PhysicalPlan) -> Self {
        Self {
            statement,
            physical,
            generation: 0,
            digest: std::sync::Arc::default(),
        }
    }

    /// The cached plan itself.
    pub fn physical_mut(&mut self) -> &mut PhysicalPlan {
        &mut self.physical
    }

    /// A copy an execution owns, sharing the entry's digest.
    pub fn clone_for_execution(&self) -> Self {
        Self {
            statement: self.statement.clone(),
            physical: self.physical.deep_clone(),
            generation: self.generation,
            digest: std::sync::Arc::clone(&self.digest),
        }
    }

    /// Go `PlanCacheValue.MemoryUsage` of the cached plan.
    pub fn memory_usage(&self) -> i64 {
        crate::plan_cache_rebuild::cached_plan_memory_usage(&self.physical) as i64
    }

    /// Go `RebuildPlan4CachedPlan` with this execute's `values`: the
    /// entry's generation once rebuilt.
    ///
    /// # Errors
    ///
    /// Why the entry cannot serve this execute.
    pub fn bind(
        &mut self,
        values: &[tidb_datatype::Datum],
        ctx: Option<&StmtContext>,
    ) -> Result<u64, crate::plan_cache_rebuild::CachedPlanBindFailure> {
        bind_prepared_statement_in_place(&mut self.statement, values)
            .map_err(|_| crate::plan_cache_rebuild::CachedPlanBindFailure::unbound())?;
        // Rebuild through the current execute parameters, never the datum
        // cached when the marker-bearing expression was first planned.
        let needs_statement = std::sync::atomic::AtomicBool::new(false);
        let statement = crate::plan_cache_rebuild::deferred_rebuild_context(ctx, values);
        let parameters = crate::plan_cache_rebuild::CachedPlanRebuildContext::new(values);
        let evaluator = |expression: &tidb_expr::expression::Expression| {
            crate::plan_cache_rebuild::rebuild_evaluate(
                expression,
                statement.as_ref(),
                &parameters,
                &needs_statement,
            )
        };
        let rebuilt = self.physical.rebuild_plan_for_cache_in_place(
            &crate::plan_cache_rebuild::CachedPlanRebuildContext::new(values)
                .with_deferred_evaluator(&evaluator),
        );
        if needs_statement.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::plan_cache_rebuild::CachedPlanBindFailure::NeedsStatement);
        }
        rebuilt.map_err(crate::plan_cache_rebuild::CachedPlanBindFailure::rebuild_failed)?;
        self.generation = self.generation.wrapping_add(1);
        Ok(self.generation)
    }

    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest`, once computed.
    pub fn digest(&self) -> std::sync::Arc<std::sync::OnceLock<(String, String)>> {
        std::sync::Arc::clone(&self.digest)
    }

    /// The statement and plan of `generation`, when it is still current.
    pub fn execution_mut(
        &mut self,
        generation: u64,
    ) -> Option<(&tidb_ast::Stmt, &mut PhysicalPlan)> {
        (self.generation == generation).then_some((&self.statement, &mut self.physical))
    }
}

/// Builds the same logical and physical plan as ordinary execution, with
/// plan-cache-safe logical rewrites enabled so parameter markers are not
/// folded into a value-specific shape.
pub fn cached_query_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    cacheability: crate::plan_cache_rebuild::PlanCacheabilityContext,
) -> Result<Option<(CachedPlan, bool)>, tidb_driver_error::DriverError> {
    let Some((physical, cacheable)) =
        cached_physical_query_plan(hooks, query, catalog, current_database, ctx, cacheability)?
    else {
        return Ok(None);
    };
    Ok(Some((
        CachedPlan {
            statement: tidb_ast::Stmt::Query(tidb_ast::NodeBox::new(query.clone())),
            physical,
            generation: 0,
            digest: std::sync::Arc::default(),
        },
        cacheable,
    )))
}

/// Builds and admits the physical source tree shared by cached queries and
/// cached DML roots.
///
/// # Errors
/// The optimizer's error: Go `generateNewPlan` returns it as the statement's.
pub fn cached_physical_query_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    cacheability: crate::plan_cache_rebuild::PlanCacheabilityContext,
) -> Result<Option<(PhysicalPlan, bool)>, tidb_driver_error::DriverError> {
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.rollup) {
        return Ok(None);
    }
    ctx.start_prepared_range_tracking();
    // Go `optimize` tries `TryFastPlan` before the optimizer, for a cached
    // statement too; the point plan keeps its parameter markers
    // (`HandleConstant` / `IndexConstants`) so a hit rebuilds its keys.
    let fast = match query {
        tidb_ast::QueryStmt::Select(select) if ctx.isolation_read_contains_tikv() => {
            crate::point_get_plan::try_fast_plan(select, catalog, current_database, ctx)?
        }
        tidb_ast::QueryStmt::Select(_) | tidb_ast::QueryStmt::SetOpr(_) => None,
    };
    let physical = match fast {
        Some(physical) => physical,
        None => {
            crate::statement_plan::planner_physical_query(
                hooks,
                query,
                catalog,
                current_database,
                ctx,
                true,
            )
            .map_err(crate::optimize::planner_error_to_driver)?
            .1
        }
    };
    if ctx.skip_plan_cache() {
        return Ok(Some((physical, false)));
    }
    // Go `generateNewPlan`: an uncacheable physical plan still executes, and
    // `SetSkipPlanCache` says why.
    if let Err(reason) = crate::plan_cache_rebuild::is_plan_cacheable(&physical, cacheability) {
        ctx.set_skip_plan_cache(reason);
        return Ok(Some((physical, false)));
    }
    Ok(Some((physical, true)))
}

/// The lowercase, even-length hex digits an `Expr::Hex` carries.
fn hex_digits(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn datum_to_literal(value: &Datum) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match value {
        Datum::Null => Expr::Null,
        Datum::Int(v) => {
            // A negative literal is a unary minus over a positive one, which
            // is how the parser itself represents it.
            if *v < 0 {
                Expr::Unary(
                    tidb_ast::UnaryOp::Minus,
                    Box::new(Expr::Int(v.unsigned_abs().to_string())),
                )
            } else {
                Expr::Int(v.to_string())
            }
        }
        Datum::UInt(v) => Expr::Int(v.to_string()),
        Datum::Real(v) => Expr::Float(*v),
        Datum::Decimal(d) => Expr::Decimal(d.to_string()),
        // A byte string that is not UTF-8 becomes a hex literal, which is
        // lossless where a lossy string conversion would corrupt it.
        Datum::String(s) => bytes_to_literal(s.bytes()),
        Datum::Bytes(b) => bytes_to_literal(b),
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
            Expr::Hex(hex_digits(literal.as_bytes()))
        }
        _ => {
            return Err(DriverError::unsupported(
                "this subquery result kind is not supported yet",
            ));
        }
    })
}

/// Go turns a subquery's result `Datum` into an `expression.Constant`; the
/// same value has to travel back through the AST here, so it becomes the
/// literal that parses to it.
/// A byte string as a literal expression: readable text stays a string, and
/// anything that is not UTF-8 becomes a hex literal so no byte is lost.
fn bytes_to_literal(bytes: &[u8]) -> tidb_ast::Expr {
    match std::str::from_utf8(bytes) {
        Ok(text) => tidb_ast::Expr::String(text.to_owned()),
        Err(_) => tidb_ast::Expr::Hex(hex_digits(bytes)),
    }
}

/// A cached entry, shared by its executions.
pub type CachedPhysicalPlan = Arc<Mutex<CachedPlan>>;

#[derive(Clone)]
struct CachedPlanValue {
    parameter_types: Arc<[PreparedParameterType]>,
    plan: CachedPhysicalPlan,
    memory: i64,
}

impl PlanCacheValue for CachedPlanValue {
    type ParamTypes = Arc<[PreparedParameterType]>;
    fn param_types(&self) -> &Self::ParamTypes {
        &self.parameter_types
    }
    fn parameter_types_compatible(cached: &Self::ParamTypes, requested: &Self::ParamTypes) -> bool {
        prepared_parameter_types_compatible(cached, requested)
    }
    fn memory_usage(&self) -> i64 {
        self.memory
    }
}

struct CacheState {
    plans: LruPlanCache<CachedPlanValue, PhysicalPlanCacheKey>,
    monitor_memory: bool,
    epoch: u64,
}

impl CacheState {
    fn account(&self, previous_size: usize, previous_memory: i64) {
        if self.plans.size() != previous_size {
            tidb_planner_core_metrics::get_plan_cache_instance_num_counter(false)
                .add(self.plans.size() as f64 - previous_size as f64);
        }
        if self.monitor_memory && self.plans.memory_usage() != previous_memory {
            tidb_planner_core_metrics::plan_cache_instance_memory_usage(false)
                .add((self.plans.memory_usage() - previous_memory) as f64);
        }
    }
}

impl Drop for CacheState {
    fn drop(&mut self) {
        let size = self.plans.size();
        let memory = self.plans.memory_usage();
        self.plans.close();
        self.account(size, memory);
    }
}

/// Domain-owned invalidation state for ADMIN FLUSH INSTANCE PLAN_CACHE.
/// An epoch represents Go's expiry timestamp without depending on clock precision.
pub struct PlanCacheInvalidation {
    epoch: std::sync::atomic::AtomicU64,
    instance: Arc<InstancePlanCache<PhysicalPlanCacheKey, CachedPlanValue>>,
    maintenance: OnceLock<CacheMaintenance>,
}

struct CacheMaintenance {
    stop: Arc<(Mutex<bool>, Condvar)>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl CacheMaintenance {
    fn stop(&self) {
        let (lock, wake) = &*self.stop;
        *lock.lock().expect("plan cache stop poisoned") = true;
        wake.notify_all();
        if let Some(worker) = self
            .worker
            .lock()
            .expect("plan cache worker poisoned")
            .take()
        {
            let _ = worker.join();
        }
    }
}
impl Drop for CacheMaintenance {
    fn drop(&mut self) {
        self.stop();
    }
}
impl Default for PlanCacheInvalidation {
    fn default() -> Self {
        let (soft, hard) = instance_limits();
        Self {
            epoch: Default::default(),
            instance: Arc::new(InstancePlanCache::new(soft, hard)),
            maintenance: OnceLock::new(),
        }
    }
}
impl std::fmt::Debug for PlanCacheInvalidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanCacheInvalidation")
            .field("epoch", &self.epoch())
            .field("instance_size", &self.instance.size())
            .finish()
    }
}
fn instance_limits() -> (i64, i64) {
    use std::sync::atomic::Ordering::Acquire;
    let hard = tidb_vardef::INSTANCE_PLAN_CACHE_MAX_MEM_SIZE.load(Acquire);
    let reserved =
        f64::from_bits(tidb_vardef::INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE.load(Acquire));
    ((hard as f64 * (1.0 - reserved)) as i64, hard)
}

impl PlanCacheInvalidation {
    /// Activate the Domain-owned 15s limits/metrics and 30s eviction lifecycle.
    /// The stop guard joins before owner retirement; no worker retains its Domain.
    pub fn start_maintenance(&self) {
        self.maintenance.get_or_init(|| {
            let stop = Arc::new((Mutex::new(false), Condvar::new()));
            let signal = stop.clone();
            let cache = self.instance.clone();
            let worker = std::thread::Builder::new()
                .name("instance-plan-cache".to_owned())
                .spawn(move || {
                    let mut eviction_tick = false;
                    let (lock, wake) = &*signal;
                    let mut stopped = lock.lock().expect("plan cache stop poisoned");
                    loop {
                        let (guard, timeout) = wake
                            .wait_timeout_while(
                                stopped,
                                std::time::Duration::from_secs(15),
                                |stop| !*stop,
                            )
                            .expect("plan cache stop poisoned");
                        stopped = guard;
                        if *stopped {
                            break;
                        }
                        if !timeout.timed_out() {
                            continue;
                        }
                        let (soft, hard) = instance_limits();
                        cache.set_limits(soft, hard);
                        tidb_planner_core_metrics::get_plan_cache_instance_num_counter(true)
                            .set(cache.size() as f64);
                        tidb_planner_core_metrics::plan_cache_instance_memory_usage(true)
                            .set(cache.memory_usage() as f64);
                        eviction_tick = !eviction_tick;
                        if !eviction_tick {
                            let disabled = !tidb_vardef::ENABLE_INSTANCE_PLAN_CACHE
                                .load(std::sync::atomic::Ordering::Acquire);
                            let removed = cache.evict(disabled);
                            tidb_planner_core_metrics::get_plan_cache_instance_evict()
                                .set(removed as f64);
                        }
                    }
                })
                .expect("start instance plan cache maintenance");
            CacheMaintenance {
                stop,
                worker: Mutex::new(Some(worker)),
            }
        });
    }

    /// Join maintenance when the hosting Domain closes, even if a session
    /// still retains its invalidation handle.
    pub fn stop_maintenance(&self) {
        if let Some(maintenance) = self.maintenance.get() {
            maintenance.stop();
        }
    }
    /// The expiry generation sessions last observed before using their cache.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Expire existing physical entries in every session sharing this owner.
    pub fn expire(&self) {
        self.epoch.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// One session-scoped LRU, corresponding to Go GetSessionPlanCache.
/// The mutex supports immutable session planning APIs; a plan's own mutex
/// separately guards the execute-time in-place range rebuild.
#[derive(Default)]
pub struct SessionPlanCache {
    state: Mutex<Option<CacheState>>,
    instance: Mutex<Option<Arc<InstancePlanCache<PhysicalPlanCacheKey, CachedPlanValue>>>>,
    instance_schema_version: Mutex<Option<u64>>,
}

impl std::fmt::Debug for SessionPlanCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionPlanCache")
            .field("size", &self.size())
            .finish()
    }
}

impl SessionPlanCache {
    /// Select the Domain cache for this statement; the session LRU remains separate.
    pub fn select_instance(&self, owner: &PlanCacheInvalidation, enabled: bool) {
        if enabled {
            owner.start_maintenance();
        }
        *self
            .instance
            .lock()
            .expect("instance cache selection poisoned") = enabled.then(|| owner.instance.clone());
    }
    /// Persisted InfoSchema version for cluster catalogs rebuilt separately
    /// per session. Embedded catalogs retain their native metadata identity.
    pub fn set_instance_schema_version(&self, version: u64) {
        *self
            .instance_schema_version
            .lock()
            .expect("plan cache schema version poisoned") = Some(version);
    }
    fn instance_key(&self, key: &PhysicalPlanCacheKey) -> PhysicalPlanCacheKey {
        let mut key = key.clone();
        if let Some(version) = *self
            .instance_schema_version
            .lock()
            .expect("plan cache schema version poisoned")
        {
            key.schema_version = version;
        }
        key
    }
    /// Metrics label for the selected physical cache owner.
    pub fn uses_instance(&self) -> bool {
        self.instance
            .lock()
            .expect("instance cache selection poisoned")
            .is_some()
    }

    /// Apply session capacity and memory monitoring and the domain's flush
    /// generation. Physical entries expire; prepared statement metadata stays.
    pub fn configure(&self, capacity: usize, monitor_memory: bool, epoch: u64, guard: f64) {
        let mut state = self.state.lock().expect("session plan cache poisoned");
        let state = state.get_or_insert_with(|| {
            let configured = tidb_config::config_tree::config::get_global_config()
                .performance
                .server_memory_quota;
            let total = tidb_memory::mem_total().unwrap_or(u64::MAX);
            let quota = if configured == 0 {
                total
            } else {
                configured.min(total)
            };
            let mut plans = LruPlanCache::new(capacity, quota, guard);
            plans.set_memory_used(Box::new(|| {
                tidb_memory::read_mem_stats().heap_alloc.max(0) as u64
            }));
            CacheState {
                plans,
                monitor_memory,
                epoch,
            }
        });
        let size = state.plans.size();
        let memory = state.plans.memory_usage();
        if state.epoch != epoch {
            state.plans.delete_all();
            state.epoch = epoch;
        }
        // Go captures capacity when GetSessionPlanCache first constructs the LRU.
        state.account(size, memory);
        if state.monitor_memory != monitor_memory {
            tidb_planner_core_metrics::plan_cache_instance_memory_usage(false)
                .add(state.plans.memory_usage() as f64 * if monitor_memory { 1.0 } else { -1.0 });
            state.monitor_memory = monitor_memory;
        }
    }

    /// Drop every cached physical entry, preserving prepared definitions.
    pub fn clear(&self) {
        let mut state = self.state.lock().expect("session plan cache poisoned");
        if let Some(state) = state.as_mut() {
            let size = state.plans.size();
            let memory = state.plans.memory_usage();
            state.plans.delete_all();
            state.account(size, memory);
        }
    }

    /// Number of retained physical entries (parameter variants count separately).
    pub fn size(&self) -> usize {
        self.state
            .lock()
            .expect("session plan cache poisoned")
            .as_ref()
            .map_or(0, |state| state.plans.size())
    }

    /// Accounted key/plan storage owned by this cache.
    pub fn memory_usage(&self) -> i64 {
        self.state
            .lock()
            .expect("session plan cache poisoned")
            .as_ref()
            .map_or(0, |state| state.plans.memory_usage())
    }

    fn mutate<R>(
        &self,
        action: impl FnOnce(&mut LruPlanCache<CachedPlanValue, PhysicalPlanCacheKey>) -> R,
    ) -> R {
        let mut state = self.state.lock().expect("session plan cache poisoned");
        // Direct executor callers get the same default session cache. SQL
        // sessions configure the owner before their first lookup.
        let state = state.get_or_insert_with(|| CacheState {
            plans: LruPlanCache::new(100, 0, 0.0),
            monitor_memory: false,
            epoch: 0,
        });
        let size = state.plans.size();
        let memory = state.plans.memory_usage();
        let result = action(&mut state.plans);
        state.account(size, memory);
        result
    }

    /// Go `lookupPlanCache`: the entry for `key` whose parameter types fit.
    pub fn get(
        &self,
        key: &PhysicalPlanCacheKey,
        parameter_types: &Arc<[PreparedParameterType]>,
    ) -> Option<CachedPhysicalPlan> {
        if let Some(instance) = self
            .instance
            .lock()
            .expect("instance cache selection poisoned")
            .clone()
        {
            let value = instance.get(&self.instance_key(key), parameter_types)?;
            let begin = std::time::Instant::now();
            let plan = Arc::new(Mutex::new(
                value
                    .plan
                    .lock()
                    .expect("cached plan poisoned")
                    .clone_for_execution(),
            ));
            tidb_planner_core_metrics::get_plan_cache_clone_duration()
                .observe(begin.elapsed().as_secs_f64());
            return Some(plan);
        }
        self.mutate(|plans| plans.get(key, parameter_types).map(|entry| entry.plan))
    }

    /// Go `LRUPlanCache.Delete`.
    pub fn delete(&self, key: &PhysicalPlanCacheKey) {
        self.mutate(|plans| plans.delete(key));
    }

    /// Go `LRUPlanCache.Put` (or the instance cache's).
    pub fn put(
        &self,
        key: PhysicalPlanCacheKey,
        parameter_types: Arc<[PreparedParameterType]>,
        plan: CachedPhysicalPlan,
    ) {
        let memory = plan.lock().expect("cached plan poisoned").memory_usage()
            + (std::mem::size_of::<CachedPlanValue>()
                + parameter_types.len() * std::mem::size_of::<PreparedParameterType>())
                as i64;
        if let Some(instance) = self
            .instance
            .lock()
            .expect("instance cache selection poisoned")
            .clone()
        {
            instance.put(
                self.instance_key(&key),
                CachedPlanValue {
                    parameter_types,
                    plan: Arc::new(Mutex::new(
                        plan.lock()
                            .expect("cached plan poisoned")
                            .clone_for_execution(),
                    )),
                    memory,
                },
            );
            return;
        }
        self.mutate(|plans| {
            plans.put(
                key,
                Arc::clone(&parameter_types),
                CachedPlanValue {
                    parameter_types,
                    plan,
                    memory,
                },
            )
        });
    }
}

/// The statement half of Go `NewPlanCacheKey`: the database and SQL.
#[must_use]
pub fn statement_key(database: &str, sql: &str) -> String {
    // Length-prefixing prevents database/SQL boundary collisions.
    format!("{}:{database}{sql}", database.len())
}

// Go cacheable_checker.checkTable: both cache tiers refuse temporary tables.
/// The statement's tables this transaction has written, sorted, as the logical
/// table ids Go's `NewPlanCacheKey` appends (`TblInfo2UnionScan`).
pub fn dirty_statement_tables(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    keys: &[tidb_infoschema::CatalogTableKey],
    dirty: &[i64],
) -> Vec<i64> {
    if dirty.is_empty() {
        return Vec::new();
    }
    let mut ids = Vec::new();
    for key in keys {
        let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.get_by_key(key) else {
            continue;
        };
        // Go `tableHasDirtyContent`: a partitioned table is dirty when any
        // partition is; its own id is not consulted.
        let is_dirty = match table.meta().read().get_partition_info() {
            Some(pi) => pi
                .read()
                .definitions
                .map_visible(|definition| definition.id)
                .iter()
                .any(|id| dirty.contains(id)),
            None => dirty.contains(&table.table_id()),
        };
        if is_dirty {
            ids.push(table.table_id());
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Go `PlanCacheStmt.RelateVersion` of the statement's tables in `catalog`:
/// table id to `TableInfo.Revision`, sorted by id.
pub fn relate_versions(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    keys: &[tidb_infoschema::CatalogTableKey],
) -> Vec<(i64, u64)> {
    let mut versions = keys
        .iter()
        .filter_map(|key| match catalog.get_by_key(key) {
            Some(tidb_infoschema::TableEntry::Kv(table)) => {
                let info = table.meta();
                let info = info.read();
                Some((info.id, info.revision))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions.dedup();
    versions
}

/// Go `planCachePreprocess`'s temporary-table refusal: no table is temporary.
#[must_use]
pub fn tables_cacheable(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    keys: &[tidb_infoschema::CatalogTableKey],
) -> bool {
    keys.iter().all(|key| !matches!(catalog.get_by_key(key), Some(tidb_infoschema::TableEntry::Kv(table)) if table.temp_table_type() != tidb_model::TempTableType::NONE))
}

/// Go `PlanCacheStmt`: what PREPARE (or a non-prepared cache entry) keeps of
/// a statement for the plan cache, whatever the statement is.
#[derive(Debug)]
pub struct PlanCacheStmt {
    /// Go `PlanCacheStmt.StmtDB`: the database the statement's names were
    /// pinned to.
    pub current_database: String,
    /// The tables the statement reads, folded.
    pub table_keys: Vec<tidb_infoschema::CatalogTableKey>,
    /// The tables whose statistics versions key a cache entry (Go
    /// `PlanCacheStmt.tables`), when `tidb_plan_cache_invalidation_on_fresh_stats`
    /// is on.
    pub stats_table_keys: Vec<tidb_infoschema::CatalogTableKey>,
    /// Go `len(PlanCacheStmt.Params)`.
    pub parameter_count: usize,
    /// The parameters a `LIMIT` reads (Go `PlanCacheStmt.limits`).
    pub limit_parameter_orders: Vec<usize>,
    /// What `planCacheStmtProcessor` recorded.
    pub stmt_info: crate::plan_cache_utils::PlanCacheStmtInfo,
    /// Go `PlanCacheStmt.PreparedAst`.
    pub statement: Stmt,
    /// The statement half of the entry's key (Go `StmtText` under `StmtDB`).
    pub cache_key: String,
    /// The `LIMIT` values of the last execution, which `DEALLOCATE` deletes
    /// the entry under.
    pub last_limit_values: std::sync::Mutex<Vec<u64>>,
}

/// What [`PlanCacheStmt::get_plan_from_plan_cache`] hands an execution: the
/// entry it rebuilt or the plan `generateNewPlan` made.
#[derive(Debug)]
pub struct PlanFromCache {
    /// This execution's parameter values.
    pub parameters: std::sync::Arc<[Datum]>,
    /// The plan, shared with the cache when it was admitted.
    pub cached_plan: std::sync::Arc<std::sync::Mutex<CachedPlan>>,
    /// The rebuild generation the execution may run.
    pub generation: u64,
    /// Go `SessionVars.FoundInPlanCache`.
    pub cache_hit: bool,
    /// The schema version the entry is keyed by.
    pub schema_version: u64,
    /// The warnings a cache miss's planning raised.
    pub planning_warnings: Vec<tidb_context::SqlWarn>,
}

impl PlanCacheStmt {
    /// Retains Go's original or parameterized SQL identity.
    pub fn set_sql(&mut self, sql: &str) {
        self.cache_key = statement_key(&self.current_database, sql);
    }

    /// The statistics version hash an entry of this statement is keyed by.
    #[must_use]
    pub fn stats_version_hash(
        &self,
        catalog: &SessionExtendedInfoSchema,
        environment: &crate::plan_cache_utils::PreparedPlanCacheEnvironment,
    ) -> u64 {
        environment.stats_version_hash(catalog, &self.stats_table_keys)
    }

    /// Go `DEALLOCATE` / `COM_STMT_CLOSE`: the current environment's entry,
    /// every parameter-type variant of it, is deleted.
    pub fn discard_cached_plan(
        &self,
        cache: &SessionPlanCache,
        catalog: &SessionExtendedInfoSchema,
        environment: &crate::plan_cache_utils::PreparedPlanCacheEnvironment,
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
            relate_version: relate_versions(catalog, &self.table_keys),
        });
    }

    /// Go `GetPlanFromPlanCache` over `statement` (this one, or the one a
    /// matched binding rewrote) with `values`: the cached entry rebuilt in
    /// place (`RebuildPlan4CachedPlan`), else, given a statement context,
    /// `generate_new_plan`'s plan over the bound statement, admitted to the
    /// cache when it is cacheable. `None` when the statement cannot be served
    /// from the cache (or, without a context, on a miss).
    ///
    /// # Errors
    ///
    /// `generate_new_plan`'s (Go `generateNewPlan`'s optimizer error).
    #[allow(clippy::too_many_arguments)]
    pub fn get_plan_from_plan_cache(
        &self,
        cache: &SessionPlanCache,
        values: &[Datum],
        catalog: &SessionExtendedInfoSchema,
        ctx: Option<&StmtContext>,
        dirty_ids: &[i64],
        environment: &crate::plan_cache_utils::PreparedPlanCacheEnvironment,
        statement: &Stmt,
        generate_new_plan: impl FnOnce(
            Stmt,
            &StmtContext,
        ) -> Result<Option<(CachedPlan, bool)>, DriverError>,
    ) -> Result<Option<PlanFromCache>, DriverError> {
        use std::sync::{Arc, Mutex};
        if !tables_cacheable(catalog, &self.table_keys) {
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
        let cache_key = PhysicalPlanCacheKey {
            statement: self.cache_key.clone(),
            schema_version,
            stats_version_hash: self.stats_version_hash(catalog, environment),
            environment: environment.clone(),
            limit_values,
            dirty_tables: dirty_statement_tables(catalog, &self.table_keys, dirty_ids),
            relate_version: relate_versions(catalog, &self.table_keys),
        };
        let refusal = environment.refusal(self.stmt_info);
        let admitted = refusal.is_none();
        let cached = admitted
            .then(|| cache.get(&cache_key, &parameter_types))
            .flatten();
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
                // An INSERT's expressions are the statement's, not the entry's.
                if let PhysicalPlan::Dml(root) = guard.physical_mut() {
                    root.insert_expressions = None;
                }
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
                let Ok(bound) = bind_prepared_statement(statement, values) else {
                    return Ok(None);
                };
                let Some((mut plan, cacheable)) = generate_new_plan(bound, &ctx)? else {
                    return Ok(None);
                };
                // A rejected cache candidate still executes its plan once.
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
                let plan = Arc::new(Mutex::new(plan));
                if cacheable {
                    cache.put(cache_key, parameter_types, Arc::clone(&plan));
                }
                (plan, generation, false)
            }
        };
        Ok(Some(PlanFromCache {
            parameters,
            cached_plan,
            generation,
            cache_hit,
            schema_version,
            planning_warnings: if cache_hit {
                Vec::new()
            } else {
                ctx.map_or_else(Vec::new, StmtContext::take_warnings)
            },
        }))
    }
}

/// Go `generateNewPlan` for an INSERT, UPDATE or DELETE: its plan built
/// with the plan cache on, and whether the cache may keep it.
///
/// # Errors
///
/// The optimizer's error.
pub fn cached_dml_physical_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    statement: &Stmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    cacheability: crate::plan_cache_rebuild::PlanCacheabilityContext,
) -> Result<Option<(tidb_planner_core_operator_physicalop::PhysicalPlan, bool)>, DriverError> {
    let Stmt::Dml(dml) = statement else {
        return Ok(None);
    };
    if let tidb_ast::DmlStmt::Insert(insert) = dml.as_ref() {
        ctx.start_prepared_range_tracking();
        let (built, _, _) =
            crate::statement_plan::insert_plan(hooks, insert, catalog, current_database, ctx, true)
                .map_err(crate::optimize::planner_error_to_driver)?;
        let mut root = built;
        hooks.prepare_plan(&mut root, ctx)?;
        if ctx.skip_plan_cache() {
            return Ok(Some((root, false)));
        }
        if crate::plan_cache_rebuild::is_plan_cacheable(&root, cacheability).is_err() {
            return Ok(None);
        }
        return Ok(Some((root, true)));
    }
    let (operator, source, write) = match dml.as_ref() {
        tidb_ast::DmlStmt::Update(update) => (
            "Update",
            Some(match crate::dml_optimize::update_source_query(update) {
                Some(source) => source,
                None => return Ok(None),
            }),
            Some(crate::dml_optimize::MultiDmlRef::Update(update.as_ref())),
        ),
        tidb_ast::DmlStmt::Delete(delete) => (
            "Delete",
            Some(match crate::dml_optimize::delete_source_query(delete) {
                Some(source) => source,
                None => return Ok(None),
            }),
            Some(crate::dml_optimize::MultiDmlRef::Delete(delete.as_ref())),
        ),
        _ => return Ok(None),
    };
    let fk_spec = match dml.as_ref() {
        tidb_ast::DmlStmt::Update(update) => {
            crate::dml_optimize::build_tbl_2_update_columns(update, current_database)
        }
        tidb_ast::DmlStmt::Delete(delete) => {
            crate::dml_optimize::fk_spec_for_delete(delete, current_database)
        }
        _ => return Ok(None),
    };
    let Ok(fk_spec) = fk_spec else {
        return Ok(None);
    };
    ctx.start_prepared_range_tracking();
    let root = crate::dml_optimize::physical_dml_plan_with_cache_mode(
        hooks,
        operator,
        source.as_ref(),
        write,
        catalog,
        current_database,
        ctx,
        true,
        std::slice::from_ref(&fk_spec),
        crate::dml_optimize::DmlKeep::Auto,
        crate::optimize::BuildVisits::Discard,
        None,
    )?;
    if ctx.skip_plan_cache() {
        return Ok(Some((root, false)));
    }
    if crate::plan_cache_rebuild::is_plan_cacheable(&root, cacheability).is_err() {
        return Ok(None);
    }
    Ok(Some((root, true)))
}
