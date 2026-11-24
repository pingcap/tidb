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

/// The complete physical tree retained by the prepared-plan cache. A hit
/// recursively rebuilds every parameter-dependent range in place and derives
/// the executor receipt from that rebuilt tree. No access,
/// aggregation, join, sort, or reader policy is re-run in the executor.
#[derive(Debug)]
pub struct CachedSelectPlan {
    statement: tidb_ast::Stmt,
    physical: PhysicalPlan,
    generation: u64,
    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest`, shared by every
    /// execution of this entry.
    digest: std::sync::Arc<std::sync::OnceLock<(String, String)>>,
}

impl CachedSelectPlan {
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
) -> Result<Option<(CachedSelectPlan, bool)>, tidb_driver_error::DriverError> {
    let Some((physical, cacheable)) =
        cached_physical_query_plan(hooks, query, catalog, current_database, ctx, cacheability)?
    else {
        return Ok(None);
    };
    Ok(Some((
        CachedSelectPlan {
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
