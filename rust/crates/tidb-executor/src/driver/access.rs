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

//! How a single base table is read: which access path is committed, and what
//! work the driver then hands down to it.
//!
//! This is the whole decision, in one file, in the order it happens:
//!
//! 1. [`commit_fast_path_source`] -- Go's `TryFastPlan`. A single-table
//!    `SELECT` whose `WHERE` pins the handle or a whole unique index reads
//!    those rows directly ([`try_batch_point_get`], [`try_point_get`]);
//!    otherwise the cheapest access path [`crate::access_cost`] enumerates
//!    supplies ranges ([`choose_index_range_path`]), which may be the full
//!    scan itself. Each fast path installs a *streaming*
//!    source over the narrowed path. A complete Go-style point plan consumes
//!    its exact key predicate and simple select list; a merely narrowed path
//!    leaves the `WHERE` in the pipeline above.
//! 2. [`prune_scan_columns`] -- the kept-column offer.
//! 3. [`negotiate_scan_filter`] -- the pushed-conjunct offer, and the residual
//!    `WHERE` left above.
//! 4. [`offer_scan_limit`] -- the row-cap offer, gated by [`scan_limit_cap`].
//!
//! Steps 2-4 are offers, not commands: [`crate::table_access`] holds the
//! contract, every method of it is fail-closed, and the source alone decides.
//! The order matters and is fixed here -- pruning runs before the predicate
//! split so a pushed conjunct's `column_offset` is already in narrow space,
//! and the cap is offered last, after the residual `WHERE` is known, because
//! a residual filter above the source forbids one.
//!
//! # Why this is its own file
//!
//! The path choice is Go's cost-based one ([`crate::access_cost`] holds the
//! enumeration, the estimates and the cost formula); this file is where that
//! choice meets the executor it commits to and the negotiation that follows
//! it. Keeping the two together is what keeps a costed path and a runnable
//! path from drifting apart.

use super::point_get_key::point_get_value;
use super::*;
use crate::access_path::{IndexMergeKind, IndexMergeSourceExec};
use crate::predicate_pushdown::ScanColumnComparison;
use std::sync::Arc;

/// What the single-table access-path decision committed.
#[derive(Default)]
pub(crate) struct AccessPathCommit {
    /// The complete reader task costed by `access_cost`, for a parent physical
    /// operator that compares alternatives with different child properties.
    pub(crate) candidate: Option<tidb_planner::candidate_cost::Candidate>,
    /// The order an index path establishes for the ordinary pipeline.
    pub(crate) index_order: Option<IndexAccessOrder>,
    /// Result metadata when a complete Go-style point plan absorbed the
    /// simple select list and can be returned without root wrappers.
    pub(crate) direct_output: Option<Vec<(String, FieldType)>>,
    /// Source offsets for a range scan whose simple projection was pushed
    /// into the coprocessor read. Point plans project in their own source and
    /// therefore leave this `None`.
    pub(crate) direct_output_offsets: Option<Vec<usize>>,
    /// Source offsets for an exact range whose simple projection executes in
    /// the cop task while a root operator (currently ORDER BY) remains above
    /// the TableReader boundary.
    pub(crate) cop_projection_offsets: Option<Vec<usize>>,
    /// Source offsets for the same cop projection when a residual Selection
    /// remains after the clustered-handle prefix. The driver installs the
    /// Selection before it records this projection/reader boundary.
    pub(crate) filtered_cop_projection_offsets: Option<Vec<usize>>,
    /// Whether the chosen access ranges represent the complete WHERE rather
    /// than a superset that still needs a residual Selection.
    pub(crate) consumed_where: bool,
    /// The conjuncts left after a clustered-handle range consumed its access
    /// prefix, joined back in written order. This is the Selection Go places
    /// above the range rather than the complete WHERE it started from.
    pub(crate) handle_range_residual: Option<tidb_ast::Expr>,
    /// The predicates left after the chosen secondary-index ranges. Go keeps
    /// these as Build-side filters when the index covers them, or evaluates
    /// them on Probe when table columns are required.
    pub(crate) access_residual: Option<tidb_ast::Expr>,
    /// The logical data source's estimated output rows after its complete
    /// predicate, before physical access-path lower bounds are applied.
    pub(crate) logical_rows: Option<f64>,
    /// Whether a bare covering scan is ready to move below its root reader.
    pub(crate) reader_ready: bool,
    /// Whether the committed access order satisfies the written ORDER BY.
    pub(crate) order_satisfied: bool,
}

#[derive(Clone, Debug)]
struct FastPointOutput {
    offsets: Vec<usize>,
    columns: Vec<(String, FieldType)>,
}

/// The immutable part of Go's cached `PointGetPlan` for one prepared handle
/// lookup. Runtime cursor state and the execute-time handle are deliberately
/// absent: both are rebuilt for every cache hit.
#[derive(Clone, Debug)]
pub struct PreparedPointGetPlan {
    schema_version: u64,
    current_database: String,
    database: String,
    table: String,
    table_id: i64,
    parameter_order: usize,
    handle_type: FieldType,
    output: FastPointOutput,
    row_decoder: crate::kv_table::PreparedPointGetRowDecoder,
}

impl PreparedPointGetPlan {
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

    /// Rebuilds the parameter-dependent handle. A value that cannot be moved
    /// exactly into the PK domain declines the cache and must be replanned.
    #[must_use]
    pub fn bind(self: &Arc<Self>, values: &[Datum]) -> Option<PreparedPointGetExecution> {
        let value = values.get(self.parameter_order)?;
        let handle = if value.is_null() {
            None
        } else {
            match point_get_value(&self.handle_type, value)? {
                Datum::Int(value) => Some(TableHandle::Int(value)),
                Datum::UInt(value) => Some(TableHandle::Int(value as i64)),
                _ => return None,
            }
        };
        Some(PreparedPointGetExecution {
            plan: Arc::clone(self),
            handle,
        })
    }

    /// Whether the catalog still names the same unpartitioned physical table.
    #[must_use]
    pub fn matches_catalog(&self, catalog: &Catalog, current_database: &str) -> bool {
        if self.schema_version != catalog.version()
            || !self.current_database.eq_ignore_ascii_case(current_database)
        {
            return false;
        }
        matches!(
            catalog.get_in(&self.database, &self.table),
            Some(TableEntry::Kv(table))
                if table.table_id == self.table_id && table.partition().is_none()
        )
    }
}

/// One cache hit after its execute-time parameter has been rebuilt into a
/// handle. The executor itself is still created fresh by
/// [`run_prepared_point_get`].
#[derive(Clone, Debug)]
pub struct PreparedPointGetExecution {
    plan: Arc<PreparedPointGetPlan>,
    handle: Option<TableHandle>,
}

impl PreparedPointGetExecution {
    /// The immutable plan whose safety gates this execution must satisfy.
    #[must_use]
    pub fn plan(&self) -> &PreparedPointGetPlan {
        &self.plan
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
) -> Option<PreparedPointGetPlan> {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return None;
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        return None;
    };
    if parameter_count != 1
        || !crate::access_path::select_is_bare_point_read(select)
        || !select.hints.is_empty()
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
        || !table_ref.hints.is_empty()
        || table_ref.sample.is_some()
    {
        return None;
    }
    let (database, table_name) = split_table_path(&table_ref.name, current_database).ok()?;
    let entry @ TableEntry::Kv(table) = catalog.get_in(database, table_name)? else {
        return None;
    };
    if table.partition().is_some() {
        return None;
    }
    let handle_offset = table.pk_handle_offset()?;
    let columns = entry.column_list();
    let visible = table_ref.alias.as_deref().unwrap_or(table_name);
    let scope = PlanTrace::single_table_scope(
        visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.clone(),
    );
    let output = fast_point_output(select, &scope)?;
    // A generated output can evaluate expressions while its stored row is
    // decoded. Keep those plans on the full statement context; the cached
    // point path below needs only SELECT's temporal/default conversion state.
    if output.offsets.iter().any(|offset| {
        table
            .visible_columns()
            .get(*offset)
            .is_none_or(|column| column.generated.is_some())
    }) {
        return None;
    }
    let (column, parameter_order) = prepared_handle_marker(select.where_clause.as_ref()?)?;
    let (offset, _, _) = ScopeResolver { scope: &scope }.resolve(column)?;
    if offset != handle_offset || parameter_order != 0 {
        return None;
    }

    Some(PreparedPointGetPlan {
        schema_version: catalog.version(),
        current_database: current_database.to_owned(),
        database: database.to_owned(),
        table: table_name.to_owned(),
        table_id: table.table_id,
        parameter_order,
        handle_type: columns.get(handle_offset)?.1.clone(),
        row_decoder: crate::kv_table::PreparedPointGetRowDecoder::new(
            table.visible_columns(),
            handle_offset,
            &output.offsets,
        )
        .ok()?,
        output,
    })
}

fn prepared_handle_marker(expr: &tidb_ast::Expr) -> Option<(&[String], usize)> {
    match expr {
        tidb_ast::Expr::Paren(inner) => prepared_handle_marker(inner),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, left, right) => match (&**left, &**right) {
            (tidb_ast::Expr::Column(column), tidb_ast::Expr::ParamMarker { order, .. })
            | (tidb_ast::Expr::ParamMarker { order, .. }, tidb_ast::Expr::Column(column)) => {
                Some((column, *order))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Executes one rebound prepared point plan with fresh mutable runtime state.
/// `None` means the schema identity moved after the cache decision.
pub fn run_prepared_point_get(
    execution: &PreparedPointGetExecution,
    catalog: &mut Catalog,
    current_database: &str,
    ctx: &crate::kv_table::PreparedPointGetDecodeContext,
) -> Result<Option<SelectMeta>, DriverError> {
    let plan = execution.plan();
    if !plan.matches_catalog(catalog, current_database) {
        return Ok(None);
    }
    let Some(TableEntry::Kv(table)) = catalog.get_mut_in_for_read(&plan.database, &plan.table)
    else {
        return Ok(None);
    };
    let rows = match execution.handle.as_ref() {
        None => Vec::new(),
        Some(handle) => match table
            .get_prepared_point_row(handle, &plan.row_decoder, ctx)
            .map_err(|error| {
                ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
            })? {
            None => Vec::new(),
            Some(row) => vec![row],
        },
    };
    Ok(Some((plan.output.columns.clone(), rows)))
}

/// Go `planner.optimize` calls `TryFastPlan` before constructing the ordinary
/// logical plan. Keep that ordering for a complete point-read SELECT: only a
/// source-column projection whose whole predicate is owned by the point key is
/// returned here. Every residual operator or unsupported table shape declines
/// to the ordinary planner below.
pub(crate) fn try_fast_point_select(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<Option<SelectMeta>, DriverError> {
    if ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
        || select.into_outfile.is_some()
        || select.lock.is_some()
    {
        return Ok(None);
    }
    let Some(table_ref) = single_table_ref(&select.from) else {
        return Ok(None);
    };
    // The ordinary source builder owns these refusal diagnostics. A fast plan
    // must not turn either unsupported clause into an ordinary current read.
    if table_ref.as_of.is_some() || table_ref.sample.is_some() || !table_ref.partitions.is_empty() {
        return Ok(None);
    }
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    let Some(entry @ TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return Ok(None);
    };
    let table = table.clone();
    let columns = entry.column_list();
    let visible = table_ref.alias.as_deref().unwrap_or(name);
    let mut scope = PlanTrace::single_table_scope(
        visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.clone(),
    );
    scope.zone = ctx.session_zone();
    let Some(output) = fast_point_output(select, &scope) else {
        return Ok(None);
    };

    // Go attempts BatchPointGet before PointGet. Its integer-handle arm
    // returns before consulting index hints, while secondary and clustered
    // indexes still pass through `indexIsAvailableByHints`.
    let mut batch = fast_batch_partition_supported(&table)
        .then(|| try_batch_point_get(select, &table, &columns, &scope.zone))
        .transpose()?
        .flatten();
    if batch.as_ref().is_some_and(|batch| !batch.ignores_hints()) {
        let hints = crate::index_hints::single_table_scan_hints(
            select,
            Some(table_ref),
            &table,
            current_db,
            ctx,
        )?;
        batch = batch.filter(|batch| batch.allowed_by(&hints));
    }
    if let Some(batch) = batch {
        let BatchPointLookup {
            handles,
            index,
            plan_rows,
            ..
        } = batch;
        let exec = HandleSourceExec::new_projected_with_context(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&output.columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handles.clone(),
            output.offsets,
            crate::kv_table::RowDecodeContext::for_query(ctx),
        );
        if let Some(trace) = trace.as_deref_mut() {
            let partitions = table.handle_partition_names(&handles, &scope.zone, ctx);
            match index {
                Some((_, index)) => trace.push_fast_index_batch_point_get(
                    source_table_name(&scope, &table.name),
                    plan_rows,
                    &partitions,
                    &index,
                    ctx.static_partition_prune(),
                    &batch_point_branch_estimates(catalog, &table, &partitions, plan_rows),
                ),
                None => trace.push_fast_batch_point_get(
                    source_table_name(&scope, &table.name),
                    &table,
                    &handles,
                    plan_rows,
                    &partitions,
                ),
            }
            trace.set_scan_act_rows(exec.produced_rows());
        }
        crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
        if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
            return Ok(Some((output.columns, Vec::new())));
        }
        let types = output
            .columns
            .iter()
            .map(|(_, field_type)| field_type.clone())
            .collect::<Vec<_>>();
        let rows = drain_executor_rows(Box::new(exec), &types, &ctx.statement_memory())?;
        return Ok(Some((output.columns, rows)));
    }

    let hints = crate::index_hints::single_table_scan_hints(
        select,
        Some(table_ref),
        &table,
        current_db,
        ctx,
    )?;
    if !hints.allows_table() {
        return Ok(None);
    }
    let Some(handle) = try_point_get(
        &PointPlanStmt::of_select(select),
        &table,
        &columns,
        &scope.zone,
    )?
    else {
        return Ok(None);
    };
    if !point_get_consumes_where(select, &table, &columns, &scope.zone) {
        return Ok(None);
    }

    let exec = HandleSourceExec::new_projected_with_context(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(&output.columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        handle.handle.clone().into_iter().collect(),
        output.offsets,
        crate::kv_table::RowDecodeContext::for_query(ctx),
    );
    if let Some(trace) = trace.as_deref_mut() {
        trace.push_fast_point_get(
            source_table_name(&scope, &table.name),
            &table,
            handle.handle.as_ref(),
        );
        trace.set_scan_act_rows(exec.produced_rows());
    }
    crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
    if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
        return Ok(Some((output.columns, Vec::new())));
    }
    let types = output
        .columns
        .iter()
        .map(|(_, field_type)| field_type.clone())
        .collect::<Vec<_>>();
    let rows = drain_executor_rows(Box::new(exec), &types, &ctx.statement_memory())?;
    Ok(Some((output.columns, rows)))
}

/// Whether Rust can route Go's partitioned fast batch point plan from the
/// handles retained after key lookup. Secondary-index values are no longer
/// available at that point, so every partition dependency must be part of a
/// clustered handle; other valid plans fall back to the ordinary index path.
fn fast_batch_partition_supported(table: &KvTable) -> bool {
    table.partition().is_none_or(|partition| {
        let handle_offsets = table
            .pk_handle_offset()
            .into_iter()
            .chain(table.common_handle_offsets().iter().copied())
            .collect::<Vec<_>>();
        !handle_offsets.is_empty()
            && !partition.dependencies.is_empty()
            && matches!(partition.expr, tidb_expr::expression::Expression::Column(_))
            && partition.dependencies.iter().all(|dependency| {
                handle_offsets
                    .iter()
                    .any(|offset| table.columns[*offset].name.eq_ignore_ascii_case(dependency))
            })
    })
}

/// Commits the narrowed access path a single-table `SELECT` qualifies for,
/// replacing `from_source`, and reports the row order the committed path
/// produces plus any complete point plan that absorbed the select list.
///
/// Go's `TryFastPlan` runs before the ordinary plan and this mirrors its
/// order: the batch point get is tried first, then an index range when no
/// point get applies, and finally the single point get -- which supersedes an
/// index range already committed, and its ordering claim with it.
///
/// A complete point plan consumes its exact key predicate and simple select
/// list, as Go does. A path with work the point plan cannot own keeps that
/// work in the ordinary pipeline, so an unsatisfied extra condition still
/// filters the row out. Each path installs a streaming source over the
/// narrowed path (see [`crate::access_path`]), not a `Vec` of rows it already
/// read, so an index range over a huge table costs one chunk of memory and a
/// pushed `LIMIT` never reads past its cap.

/// Go `rule_predicate_push_down.go`'s `(*PPDSolver).Name()`, the string an
/// operator writes into `mysql.opt_rule_blacklist` to switch the rule off.
const PREDICATE_PUSH_DOWN_RULE: &str = "predicate_push_down";

/// Go `DataSource.PredicatePushDown`: `ds.PushedDownConds, predicates =
/// expression.PushDownExprs(...)`, and every access path is derived from
/// `PushedDownConds` alone.
///
/// Returns a `SELECT` whose `WHERE` is that subset, or `None` when it is the
/// whole `WHERE` -- which is every session that never ran an `ADMIN RELOAD`,
/// so the ordinary path neither clones nor re-resolves anything.
///
/// The full `WHERE` stays with the caller as Go's `AllConds`, and is what the
/// residual `Selection` above the scan applies. That split is the point: a
/// condition the blacklist refuses still filters correctly, it just stops
/// bounding any scan, so the index whose leading column it constrained is no
/// longer a candidate.
fn pushed_down_conds(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    ctx: &crate::StmtContext,
) -> Option<tidb_ast::SelectStmt> {
    let where_clause = select.where_clause.as_ref()?;
    // Go `isLogicalRuleDisabled`: the rule does not run at all, so the
    // `DataSource` is handed no predicate and every path is a full scan.
    if ctx.logical_rule_disabled(PREDICATE_PUSH_DOWN_RULE) {
        let mut filtered = select.clone();
        filtered.where_clause = None;
        return Some(filtered);
    }
    if ctx.expr_pushdown_blacklist().is_empty() {
        return None;
    }
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    let total = conjuncts.len();
    let resolver = scope_resolver(scope);
    let kept: Vec<&tidb_ast::Expr> = conjuncts
        .into_iter()
        .filter(|conjunct| {
            crate::pushdown_blacklist::blacklist_admits(
                conjunct,
                &resolver,
                ctx,
                tidb_expr::infer_pushdown::PushDownStore::Unspecified,
            )
        })
        .collect();
    if kept.len() == total {
        return None;
    }
    let mut filtered = select.clone();
    filtered.where_clause = join_predicates(&kept);
    Some(filtered)
}

/// Go `MaxMinEliminator.eliminateSingleMaxMin`
/// (`pkg/planner/core/rule/rule_max_min_eliminate.go`), as the ACCESS view of
/// the statement: the ungrouped single `MAX(col)`/`MIN(col)` is what
/// `SELECT ... ORDER BY col [DESC] LIMIT 1` reads, so the path chooser is
/// handed the statement with exactly that order and limit spliced in. The
/// select list, `WHERE` and hints stay as written -- the demanded column set
/// of `max(col)` IS `{col}`, and every non-costing consumer of the statement
/// reads the original.
///
/// The gates are the rule's own, in its order (`eliminateMaxMin` +
/// `eliminateSingleMaxMin`): no `GROUP BY`, exactly one aggregate which is
/// `MAX`/`MIN`, a non-`ENUM`/`SET` argument. `checkColCanUseIndex` is NOT
/// among them -- with a single aggregate Go transforms unconditionally
/// ("this transformation won't be worse than previous") and lets the cost
/// model decide, which is exactly what handing the rewritten view to the
/// chooser does.
///
/// Arms of the Go rule this view does not carry, each still costed and
/// executed as before (the executor-side rewrite in
/// [`super::agg_select::single_max_min_elimination`] is independent of this
/// view and keeps its own coverage):
///
/// * several `MAX`/`MIN` functions (`splitAggFuncAndCheckIndices` +
///   `composeAggsByInnerJoin`): the split produces one cartesian join of
///   single-aggregate blocks, a plan shape this tier does not build;
/// * a NULLABLE argument: Go inserts `Selection(not(isnull(col)))` between
///   the `Limit` and the source, and this view has no way to carry that
///   extra conjunct without desynchronizing the caller's residual-`WHERE`
///   accounting, so the rewrite fires only for a `NOT NULL` argument;
/// * a non-column argument (`len(expression.ExtractColumns(f.Args[0])) > 0`
///   arms): no access path can satisfy an order over an expression, so the
///   view would change nothing the chooser reads;
/// * an aggregate over a JOIN: Go's single-aggregate arm rewrites it too
///   ("we don't need to guarantee that the child of it is a data source"),
///   but this caller is the SINGLE-TABLE path chooser and a join's leaves
///   are costed elsewhere, so the eliminated order never reaches them here.
fn max_min_eliminated_access_select(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    columns: &[(String, FieldType)],
) -> Option<tidb_ast::SelectStmt> {
    if !select.group_by.is_empty()
        || select.distinct
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || crate::window::select_has_window(select)
    {
        return None;
    }
    let [SelectField::Expr { expr, .. }] = select.fields.fields() else {
        return None;
    };
    let tidb_ast::Expr::Aggregate {
        name,
        distinct: false,
        args,
    } = expr
    else {
        return None;
    };
    let desc = if name.eq_ignore_ascii_case("max") {
        true
    } else if name.eq_ignore_ascii_case("min") {
        false
    } else {
        return None;
    };
    let [argument] = args.as_slice() else {
        return None;
    };
    let tidb_ast::Expr::Column(path) = argument else {
        return None;
    };
    // The argument must be a column of THIS table: unqualified, or qualified
    // by the sole table's scope name. A correlated outer column must not
    // resolve here by name accident.
    let column_name = match path.as_slice() {
        [name] => name,
        [qualifier, name] if scope.tables[0].name.eq_ignore_ascii_case(qualifier) => name,
        _ => return None,
    };
    let (_, field_type) = columns
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column_name))?;
    // Go `eliminateMaxMin`: "Limit+Sort operators are sorted by value, but
    // ENUM/SET field types are sorted by name."
    if matches!(
        field_type.code(),
        tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set
    ) {
        return None;
    }
    if !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL) {
        return None;
    }
    let mut rewritten = select.clone();
    rewritten.order_by = vec![tidb_ast::OrderItem {
        expr: argument.clone(),
        desc,
    }];
    rewritten.limit = Some(tidb_ast::Limit {
        offset: None,
        count: tidb_ast::Expr::Int("1".to_owned()),
    });
    Some(rewritten)
}

pub(crate) fn commit_fast_path_source(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
    from_source: &mut Option<Box<dyn Executor>>,
    mut trace: Option<&mut PlanTrace>,
    ctx: &crate::StmtContext,
    // Go `findBestTask`'s own `prop` when this SELECT is a child a parent
    // asked for an ORDER of (a merge join's side, an index join's outer
    // side): the required column sequence in this table's own offsets, or
    // `None` for the empty property. `convertToIndexScan` /
    // `convertToTableScan` both open with `if !prop.IsSortItemEmpty() &&
    // !candidate.matchPropResult.Matched() { return invalidTask }` -- a path
    // that does not walk in the required order is not a candidate AT ALL, so
    // the ordered scan `build_from` already installed must not be replaced
    // by a cheaper unordered one here.
    required_order: Option<&[usize]>,
) -> Result<AccessPathCommit, DriverError> {
    #[cfg(test)]
    ORDINARY_ACCESS_PATH_ENTRIES.with(|entries| entries.set(entries.get() + 1));
    // Go's `PlanBuilder` reads the zone off the same `sessionctx` every other
    // decision here reads; taking it from `ctx` keeps the two from being
    // separately supplied and separately wrong.
    let zone = &ctx.session_zone();
    let disable_point_get = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false);
    let mut index_order: Option<IndexAccessOrder> = None;
    let mut direct_output = None;
    let mut direct_output_offsets = None;
    let mut cop_projection_offsets = None;
    let mut filtered_cop_projection_offsets = None;
    let mut consumed_where = false;
    let mut handle_range_residual = None;
    let mut access_residual = None;
    let mut reader_ready = false;
    let mut candidate = None;
    let Some(table_ref) = sole_table_ref(&select.from) else {
        return Ok(AccessPathCommit::default());
    };
    let Some(table) = sole_kv_table(&select.from, catalog, current_db) else {
        return Ok(AccessPathCommit::default());
    };
    // Go's `PushedDownConds`, which is what the ranger sees. `select` stays
    // the full `AllConds` for every residual and consumed-WHERE decision
    // below.
    let narrowed;
    let (path_select, conds_narrowed) = match pushed_down_conds(select, scope, ctx) {
        Some(filtered) => {
            narrowed = filtered;
            (&narrowed, true)
        }
        None => (select, false),
    };
    let table_name = table_ref
        .name
        .last()
        .map(String::as_str)
        .unwrap_or(table.name.as_str());
    let mut table =
        super::from::restricted_to_partitions(&table, &table_ref.partitions, table_name)?;
    // Go's `PartitionProcessor` runs in LOGICAL optimization, so by the time
    // `DeriveStats` asks for a histogram the `DataSource` is already the
    // surviving partition and `ds.PhysicalTableID` names it -- that id is
    // what `stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)`
    // is handed. Narrowing the handle here is that ordering: the pruning was
    // applied further down, to the SOURCE only, so the statistics lookup
    // above it still asked under the LOGICAL id. Static pruning stores a
    // histogram per physical partition and no merged one, so the lookup
    // missed and every pruned read was costed as `stats:pseudo` -- after
    // `analyze table tint all columns` had computed the very histogram it
    // wanted.
    if let Some(ids) = pruned_partition_ids(select, &table, zone) {
        table.restrict_read_to_partitions(&ids);
    }
    let table = table;
    let statistics = catalog.table_statistics(table.stats_physical_id());
    let mut logical_rows = Some(
        crate::access_cost::realtime_row_count(statistics.map(AsRef::as_ref))
            * stats_selectivity_with_default_string_match_selectivity(
                catalog,
                &table,
                scope,
                select.where_clause.as_ref(),
                ctx.default_string_match_selectivity(),
            )
            .unwrap_or(1.0),
    );
    let columns = scope.column_list();
    // Go's `getPossibleAccessPaths`: the statement's own `USE`/`FORCE`/
    // `IGNORE INDEX` decide which paths exist before any of them is costed.
    // The names were already validated for every table of the `FROM`
    // (`index_hints::validate_join_index_hints`), so this cannot be the site
    // that raises 1176.
    let hints = crate::index_hints::single_table_scan_hints(
        select,
        Some(table_ref),
        &table,
        current_db,
        ctx,
    )?;
    // Go's `MaxMinEliminator` (`pkg/planner/core/rule/rule_max_min_eliminate.go`)
    // runs in LOGICAL optimization, so by the time `findBestTask` costs this
    // table's paths an ungrouped `MAX(col)`/`MIN(col)` has already become
    // `Agg -> Limit 1 -> Sort col [desc] -> DataSource`, and the paths are
    // priced under that sort property with `ExpectedCnt` 1. This tier runs
    // the same rewrite in the executor pipeline
    // (`super::agg_select::single_max_min_elimination` builds the
    // TopN/Limit-over-source shape), but the path choice happens HERE, first
    // -- so the eliminated ORDER/LIMIT is spliced into the ACCESS view alone.
    // Only the costing questions read `path_select`'s order and limit
    // (`costing_limit_cap`, `PushedLimit::satisfied_by`); every point-get,
    // residual and consumed-`WHERE` decision keeps reading the statement as
    // written, and no row cap is pushed into the executor from this view, so
    // a fired rewrite can only change which path is committed.
    let max_min_access = max_min_eliminated_access_select(path_select, scope, &columns);
    let path_select = max_min_access.as_ref().unwrap_or(path_select);
    // Go's `PredicateSimplification` plans a `TableDual rows:0` before any path
    // is costed when the `WHERE` is provably contradictory on some column
    // (`b = 1 AND b = 2`), which is index-independent: it reads no row whether
    // or not `b` is indexed, and holds for a partition key over a partitioned
    // table just the same. Committing the dual here supersedes every access
    // path below, exactly as Go's whole-`DataSource`-to-dual replacement does.
    if let Some(where_clause) = select.where_clause.as_ref() {
        if crate::index_range::where_is_unsatisfiable(&columns, where_clause, zone) {
            logical_rows = Some(
                crate::access_cost::realtime_row_count(statistics.map(AsRef::as_ref))
                    * stats_selectivity_with_default_string_match_selectivity(
                        catalog,
                        &table,
                        scope,
                        select.where_clause.as_ref(),
                        ctx.default_string_match_selectivity(),
                    )
                    .unwrap_or(1.0),
            );
            install_contradiction_dual(&columns, from_source, trace.as_deref_mut());
            return Ok(AccessPathCommit {
                consumed_where: true,
                logical_rows,
                ..AccessPathCommit::default()
            });
        }
    }
    // Go's `PartitionProcessor` prunes before any access path is costed, and
    // so does this: an offer refused leaves the source reading every
    // partition, which is a superset and still every row the statement
    // admits.
    // Go `getTableScanPenalty`'s `hasPartitionScan` reads
    // `PlanPartInfo.PruningConds`, which the `PartitionProcessor` leaves
    // behind whenever it had conditions to prune WITH -- exactly when
    // `pruned_partition_ids` answers.
    let partition_scan = pruned_partition_ids(select, &table, zone).is_some();
    if let Some(ids) = pruned_partition_ids(select, &table, zone) {
        if let Some(access) = from_source
            .as_mut()
            .and_then(|source| source.table_access())
        {
            access.accept_partition_pruning(&ids);
        }
    }
    // Go tries the batch point get before the single one.
    //
    // Go `newBatchPointGetPlan`'s partitioned gate: the plan is built only
    // when `PartitionExpr().Expr` is a bare `*expression.Column`. Measured
    // through gorun under dynamic pruning: `HASH(b)` and `RANGE(b)` answer
    // `Batch_Point_Get`, while `KEY(b)` (whose partition expression is nil
    // there and a placeholder Constant here) and `HASH(b+1)` (a scalar
    // function) fall back to the index range path.
    if let Some(batch) = (!disable_point_get && fast_batch_partition_supported(&table))
        .then(|| try_batch_point_get(select, &table, &columns, zone))
        .transpose()?
        .flatten()
        .filter(|batch| batch.allowed_by(&hints))
    {
        let BatchPointLookup {
            handles,
            index,
            plan_rows,
            ..
        } = batch;
        let output = fast_point_output(select, scope);
        let (handles, physical_ids, partitions) = if table.partition().is_some() {
            let mut routed_handles = Vec::with_capacity(handles.len());
            let mut physical_ids = Vec::with_capacity(handles.len());
            let mut ordinals = Vec::new();
            let routes = table.handle_partition_routes(&handles, zone, ctx);
            for (handle, route) in handles.into_iter().zip(routes) {
                let Some((ordinal, physical_id)) = route else {
                    continue;
                };
                routed_handles.push(handle);
                physical_ids.push(physical_id);
                if !ordinals.contains(&ordinal) {
                    ordinals.push(ordinal);
                }
            }
            if routed_handles.is_empty() {
                install_contradiction_dual(&columns, from_source, trace.as_deref_mut());
                return Ok(AccessPathCommit {
                    consumed_where: true,
                    logical_rows,
                    ..AccessPathCommit::default()
                });
            }
            ordinals.sort_unstable();
            let partitions = ordinals
                .into_iter()
                .filter_map(|ordinal| table.partition()?.definitions.get(ordinal))
                .map(|definition| definition.name.clone())
                .collect();
            (routed_handles, Some(physical_ids), partitions)
        } else {
            (handles, None, Vec::new())
        };
        let output_offsets = output.as_ref().map(|output| output.offsets.clone());
        let output_columns = output.as_ref().map_or(&columns, |output| &output.columns);
        let meta = ExecutorMeta::new(
            Schema::new(source_schema_columns(output_columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        );
        let decode_context = crate::kv_table::RowDecodeContext::for_query(ctx);
        let exec = match physical_ids {
            Some(physical_ids) => HandleSourceExec::new_partitioned_projected_with_context(
                meta,
                table.clone(),
                handles.clone(),
                physical_ids,
                output_offsets,
                decode_context,
            ),
            None => match output_offsets {
                Some(offsets) => HandleSourceExec::new_projected_with_context(
                    meta,
                    table.clone(),
                    handles.clone(),
                    offsets,
                    decode_context,
                ),
                None => HandleSourceExec::new_with_context(
                    meta,
                    table.clone(),
                    handles.clone(),
                    decode_context,
                ),
            },
        };
        if let Some(trace) = trace.as_deref_mut() {
            match index {
                Some((_, index)) => trace.index_batch_point_get(
                    source_table_name(scope, &table.name),
                    plan_rows,
                    &partitions,
                    &index,
                    ctx.static_partition_prune(),
                    &batch_point_branch_estimates(catalog, &table, &partitions, plan_rows),
                ),
                None => trace.batch_point_get(
                    source_table_name(scope, &table.name),
                    &table,
                    &handles,
                    plan_rows,
                    &partitions,
                ),
            }
            // The rows are read lazily, so the count is the source's live one
            // rather than a `Vec`'s length.
            trace.set_scan_act_rows(exec.produced_rows());
        }
        direct_output = output.map(|output| output.columns);
        *from_source = Some(Box::new(exec));
    } else
    // An index range scan, when no point get applies: the ranges replace the
    // full scan with the rows the index covers, and the WHERE stays above to
    // apply the conditions the ranges did not consume.
    //
    // A point get over the handle IS the table path taken to its limit, so a
    // hint that deleted the table path deletes it too -- Go gates it on the
    // same `indexIsAvailableByHints` (`point_get_plan.go:571`), which is why
    // `FORCE INDEX(idx_b) WHERE a = 2` reads idx_b instead of the row. The
    // The integer-handle BATCH point path is deliberately not gated: Go's
    // `newBatchPointGetPlan` returns from that arm before consulting hints.
    // Secondary and clustered-index batch paths were gated above.
    if disable_point_get
        || !hints.allows_table()
        || try_point_get(&PointPlanStmt::of_select(select), &table, &columns, zone)?.is_none()
    {
        if !crate::index_hints::no_index_merge(select) {
            if let Some(plan) = choose_index_merge_union(
                select,
                catalog,
                scope,
                &table,
                &columns,
                partition_scan,
                current_db,
            ) {
                commit_index_merge_source(
                    &table,
                    scope,
                    &columns,
                    plan,
                    from_source,
                    trace.as_deref_mut(),
                    ctx,
                );
                return Ok(AccessPathCommit {
                    logical_rows,
                    ..AccessPathCommit::default()
                });
            }
            if let Some(plan) = choose_index_merge_intersection(
                select,
                catalog,
                scope,
                &table,
                &columns,
                partition_scan,
                current_db,
            ) {
                commit_index_merge_source(
                    &table,
                    scope,
                    &columns,
                    plan,
                    from_source,
                    trace.as_deref_mut(),
                    ctx,
                );
                return Ok(AccessPathCommit {
                    logical_rows,
                    ..AccessPathCommit::default()
                });
            }
            if ctx.index_merge() {
                let automatic = AutomaticIndexMergeContext {
                    catalog,
                    scope,
                    table: &table,
                    columns: &columns,
                    partition_scan,
                    hints: &hints,
                    current_db,
                    ordering_index_selectivity_ratio: ctx.ordering_index_selectivity_ratio(),
                    default_string_match_selectivity: ctx.default_string_match_selectivity(),
                };
                if let Some(plan) = choose_automatic_index_merge_union(path_select, &automatic) {
                    commit_index_merge_source(
                        &table,
                        scope,
                        &columns,
                        plan,
                        from_source,
                        trace.as_deref_mut(),
                        ctx,
                    );
                    return Ok(AccessPathCommit {
                        logical_rows,
                        ..AccessPathCommit::default()
                    });
                }
            }
        }
        match choose_index_range_path(
            path_select,
            catalog,
            scope,
            &table,
            &columns,
            &hints,
            partition_scan,
            ctx,
            None,
            required_order,
        ) {
            // A table path the ranger narrowed. The source already installed
            // by `build_from` IS the right executor -- a `TableRangeScan` is
            // Go's same `PhysicalTableScan` with ranges -- so this offers it
            // the ranges rather than replacing it, and only renames the
            // traced node once the source has taken them. A source that
            // refuses keeps reading the whole table, which is still every row
            // the statement admits.
            Some(ChosenPath::HandleRange(ranges, estimate, planner_candidate, source_rows)) => {
                logical_rows = Some(source_rows);
                candidate = Some(planner_candidate);
                let accepted = from_source
                    .as_mut()
                    .and_then(|source| source.table_access())
                    .is_some_and(|access| access.accept_handle_ranges(&ranges));
                if accepted {
                    if let Some(access) = from_source
                        .as_mut()
                        .and_then(|source| source.table_access())
                    {
                        access.accept_scan_estimate(estimate.rows);
                    }
                    // Go returns a PhysicalTableDual as soon as the chosen
                    // path has no ranges. No residual predicate survives
                    // above a source that is already known to return no row.
                    // Never once the conditions were narrowed: the ones
                    // `PushDownExprs` refused are not in `path_select` at
                    // all, so nothing the ranger did can have accounted for
                    // them and they MUST survive as the `Selection` above.
                    // Go's `len(path.Ranges) == 0` short-circuit reads an
                    // empty range set as "the predicate is contradictory";
                    // here the emptiness can instead mean "there was no
                    // predicate left to range", and dropping the filter on
                    // that reading answered every row.
                    consumed_where = !conds_narrowed
                        && (ranges.is_empty() || handle_path_consumes_where(select, &table, zone));
                    handle_range_residual = select.where_clause.as_ref().and_then(|predicate| {
                        let built =
                            crate::handle_range::build_handle_ranges(&table, predicate, zone)?;
                        join_predicates(&built.residual)
                    });
                    index_order = handle_range_order(&table, &columns, &ranges);
                    if let Some(trace) = trace.as_deref_mut() {
                        // Go's `findBestTask` returns a `PhysicalTableDual`
                        // the moment a chosen path has NO ranges
                        // (`find_best_task.go`: `if len(path.Ranges) == 0`),
                        // the same short-circuit the index arm below takes.
                        // On the TABLE path this is what `id IS NULL` over an
                        // integer handle reaches: `points2TableRanges` drops
                        // the NULL-ended interval, leaving nothing to read.
                        if ranges.is_empty() {
                            trace.empty_range_table_dual();
                        } else if let Some(handle) = (!disable_point_get
                            && table.pk_handle_offset().is_some())
                        .then(|| single_point_handle(&ranges))
                        .flatten()
                        {
                            // Go's `isPointGetPath` converts a table path whose
                            // one range is a single non-null point on the
                            // integer handle to a `Point_Get`
                            // (`find_best_task.go`: `convertToPointGet`), even
                            // when an extra conjunct stays a filter above --
                            // `c1 = 1 AND c2 > 1` reads `Point_Get`, not a
                            // `TableRangeScan` over `[1,1]`.
                            trace.point_get(
                                source_table_name(scope, &table.name),
                                &table,
                                Some(&handle),
                                None,
                            );
                        } else if !disable_point_get
                            && !table.common_handle_offsets().is_empty()
                            && ranges.len() == 1
                            && ranges[0].is_point(false)
                            && ranges[0].low.len() == table.common_handle_offsets().len()
                        {
                            // Go converts a COMMON-handle table path the same
                            // way (`find_best_task.go:2202`: the clustered
                            // PRIMARY is unique with no prefix, and the one
                            // range must pin every key column and be a
                            // non-nullable point, `:2248`). The print names
                            // the clustered index, not a handle value, and a
                            // pruned partitioned table names its partition --
                            // both [`PlanTrace::point_get`]'s own contract.
                            trace.point_get(
                                source_table_name(scope, &table.name),
                                &table,
                                None,
                                None,
                            );
                        } else {
                            trace.table_range_scan(
                                source_table_name(scope, &table.name),
                                &ranges,
                                estimate,
                            );
                        }
                    }
                    if consumed_where && range_can_return_direct(select) {
                        if let Some(output) = fast_point_output(select, scope) {
                            direct_output_offsets = Some(output.offsets);
                            direct_output = Some(output.columns);
                        }
                    } else if consumed_where {
                        cop_projection_offsets = range_order_projection(select, scope);
                    } else {
                        filtered_cop_projection_offsets = range_order_projection(select, scope);
                    }
                }
            }
            Some(ChosenPath::Index(
                index_id,
                ranges,
                estimate,
                covering,
                planner_candidate,
                source_rows,
            )) => {
                logical_rows = Some(source_rows);
                candidate = Some(planner_candidate);
                // Go's empty-range task is the whole DataSource result, so it
                // consumes the WHERE even when the ordinary index-detach
                // check would leave that predicate as a residual.
                // See the handle-range arm: an empty range set after
                // narrowing is not a contradiction.
                consumed_where = !conds_narrowed
                    && (ranges.is_empty()
                        || index_path_consumes_where(select, &table, index_id, zone));
                reader_ready = covering && consumed_where;
                let index_residual = crate::access_cost::index_residual_filters_for_path(
                    &table,
                    index_id,
                    select.where_clause.as_ref(),
                    &ScopeResolver { scope },
                );
                access_residual = index_residual.iter().cloned().reduce(|left, right| {
                    tidb_ast::Expr::Binary(
                        tidb_ast::BinaryOp::LogicAnd,
                        Box::new(left),
                        Box::new(right),
                    )
                });
                // Go's point plans exist ONLY while every WHERE conjunct is a
                // `column = constant` pair over the chosen key
                // (`point_get_plan.go` getNameValuePairs: one non-pair
                // conjunct makes `pairs == nil` and tryPointGetPlan bails).
                // A leftover conjunct therefore prints the ordinary shape --
                // `IndexRangeScan` inside an `IndexLookUp`, with the residual
                // as its Probe-side `Selection` -- never a `Point_Get` or
                // `Batch_Point_Get` carrying a filter.
                let index_point_allowed = index_residual.is_empty();
                commit_index_range_source(
                    &table,
                    catalog,
                    scope,
                    &columns,
                    index_id,
                    ranges,
                    estimate,
                    covering,
                    index_point_allowed,
                    hints.lookup_pushdown_hinted(index_id),
                    from_source,
                    trace.as_deref_mut(),
                    &mut index_order,
                    ctx,
                );
            }
            Some(ChosenPath::FullTable(planner_candidate, source_rows)) => {
                logical_rows = Some(source_rows);
                // The full scan source is already installed. Keep the task
                // receipt so a physical parent can still cost this child.
                candidate = Some(planner_candidate);
                // Go `matchProperty`: a scan over the clustered handle walks
                // in handle order with no narrowing at all, so `ORDER BY`
                // the handle prefix is discharged by the scan itself and a
                // `LIMIT` becomes a pushed Limit rather than a TopN. One
                // unbounded range is trivially a single range.
                index_order = full_table_handle_order(&table, &columns);
            }
            None => {}
        }
    }
    if let Some(handle) = (!disable_point_get && hints.allows_table())
        .then(|| try_point_get(&PointPlanStmt::of_select(select), &table, &columns, zone))
        .transpose()?
        .flatten()
    {
        // A `None` handle is a WHERE that pins a handle no row can have: the
        // plan is a point get over an empty handle list.
        consumed_where = point_get_consumes_where(select, &table, &columns, zone);
        let output = consumed_where
            .then(|| fast_point_output(select, scope))
            .flatten();
        let exec = handle_source_exec(
            &table,
            handle.handle.clone().into_iter().collect(),
            &columns,
            output.as_ref(),
            ctx,
        );
        if let Some(trace) = trace {
            trace.point_get(
                source_table_name(scope, &table.name),
                &table,
                handle.handle.as_ref(),
                handle.index.as_ref(),
            );
            trace.set_scan_act_rows(exec.produced_rows());
        }
        // The index-range path above may have already committed a source; a
        // point get supersedes it, and so does its ordering claim.
        index_order = None;
        reader_ready = false;
        direct_output = output.map(|output| output.columns);
        *from_source = Some(Box::new(exec));
    }
    let order_satisfied = !select.order_by.is_empty()
        && index_order
            .as_ref()
            .is_some_and(|order| order_is_index_order(select, order, &ScopeResolver { scope }));
    Ok(AccessPathCommit {
        index_order,
        candidate,
        direct_output,
        direct_output_offsets,
        cop_projection_offsets,
        filtered_cop_projection_offsets,
        consumed_where,
        handle_range_residual,
        access_residual,
        logical_rows,
        reader_ready,
        order_satisfied,
    })
}

struct IndexMergePlan {
    kind: IndexMergeKind,
    partials: Vec<(i64, Vec<IndexRange>)>,
}

fn choose_index_merge_union(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    partition_scan: bool,
    current_db: &str,
) -> Option<IndexMergePlan> {
    if table.partition().is_some() {
        return None;
    }
    let index_ids = crate::index_hints::single_table_index_merge_indexes(
        select,
        single_table_ref(&select.from),
        table,
        current_db,
    );
    if index_ids.len() < 2 {
        return None;
    }
    let where_clause = select.where_clause.as_ref()?;
    let mut branches = Vec::new();
    collect_index_merge_disjuncts(where_clause, &mut branches);
    if branches.len() < 2 {
        return None;
    }
    let demand = crate::driver::leaf_demand::LeafDemand::of_select(select);
    let needed = demand.needed(&scope.tables[0].name, columns);
    let resolver = ScopeResolver { scope };
    let stats = catalog.table_statistics(table.stats_physical_id());
    let stats = stats.as_ref().map(AsRef::as_ref);
    let hints = crate::index_hints::AvailablePaths::index_merge_only(index_ids);
    let mut partials = Vec::with_capacity(branches.len());
    for branch in branches {
        let candidates = crate::access_cost::enumerate_paths(
            table,
            columns,
            Some(branch),
            &needed,
            &resolver,
            None,
            stats,
            &hints,
            false,
            partition_scan,
            true,
            // A branch enumeration builds one IndexMerge PARTIAL, which Go's
            // heuristic never runs through -- see `enumerate_paths`.
            false,
            None,
        )
        .into_iter()
        .filter(|candidate| !candidate.access_columns.is_empty())
        .collect();
        let path = crate::access_cost::choose_access_path(candidates, stats, false, false)?;
        let (index_id, ranges) = path.index?;
        partials.push((index_id, ranges));
    }
    Some(IndexMergePlan {
        kind: IndexMergeKind::Union,
        partials,
    })
}

fn choose_index_merge_intersection(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    partition_scan: bool,
    current_db: &str,
) -> Option<IndexMergePlan> {
    if table.partition().is_some() {
        return None;
    }
    let index_ids = crate::index_hints::single_table_index_merge_indexes(
        select,
        single_table_ref(&select.from),
        table,
        current_db,
    );
    if index_ids.len() < 2 {
        return None;
    }
    let where_clause = select.where_clause.as_ref()?;
    let demand = crate::driver::leaf_demand::LeafDemand::of_select(select);
    let needed = demand.needed(&scope.tables[0].name, columns);
    let resolver = ScopeResolver { scope };
    let stats = catalog.table_statistics(table.stats_physical_id());
    let stats = stats.as_ref().map(AsRef::as_ref);
    let mut partials = Vec::new();
    for index_id in index_ids {
        let hints = crate::index_hints::AvailablePaths::index_merge_only(vec![index_id]);
        let candidates = crate::access_cost::enumerate_paths(
            table,
            columns,
            Some(where_clause),
            &needed,
            &resolver,
            None,
            stats,
            &hints,
            false,
            partition_scan,
            true,
            // An IndexMerge partial; the heuristic never runs through one.
            false,
            None,
        )
        .into_iter()
        .filter(|candidate| !candidate.access_columns.is_empty())
        .collect();
        let Some(path) = crate::access_cost::choose_access_path(candidates, stats, false, false)
        else {
            continue;
        };
        let Some((index_id, ranges)) = path.index else {
            continue;
        };
        partials.push((index_id, ranges));
    }
    (partials.len() >= 2).then_some(IndexMergePlan {
        kind: IndexMergeKind::Intersection,
        partials,
    })
}

/// Inputs shared by automatic non-MV OR IndexMerge path generation.
struct AutomaticIndexMergeContext<'a> {
    catalog: &'a Catalog,
    scope: &'a FromScope,
    table: &'a KvTable,
    columns: &'a [(String, FieldType)],
    partition_scan: bool,
    hints: &'a crate::index_hints::AvailablePaths,
    current_db: &'a str,
    ordering_index_selectivity_ratio: f64,
    default_string_match_selectivity: f64,
}

fn choose_automatic_index_merge_union(
    select: &tidb_ast::SelectStmt,
    input: &AutomaticIndexMergeContext<'_>,
) -> Option<IndexMergePlan> {
    // Each partial must be able to return the integer row handle without a
    // table lookup. Common handles need a distinct lowering and are declined.
    let handle = input.table.pk_handle_offset()?;
    if input.table.partition().is_some() {
        return None;
    }
    if crate::index_hints::has_single_table_index_merge_hint(
        select,
        single_table_ref(&select.from),
        input.current_db,
    ) {
        return None;
    }
    let where_clause = select.where_clause.as_ref()?;
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    let dnf = conjuncts
        .iter()
        .enumerate()
        .find_map(|(index, candidate)| {
            let mut branches = Vec::new();
            collect_index_merge_disjuncts(candidate, &mut branches);
            (branches.len() >= 2).then_some((index, branches))
        })?;
    if conjuncts.iter().enumerate().any(|(index, candidate)| {
        index != dnf.0 && {
            let mut branches = Vec::new();
            collect_index_merge_disjuncts(candidate, &mut branches);
            branches.len() >= 2
        }
    }) {
        return None;
    }
    let common = conjuncts
        .iter()
        .enumerate()
        .filter_map(|(index, condition)| (index != dnf.0).then_some(*condition))
        .collect::<Vec<_>>();
    let resolver = ScopeResolver { scope: input.scope };
    let stats = input
        .catalog
        .table_statistics(input.table.stats_physical_id());
    let stats = stats.as_ref().map(AsRef::as_ref);
    let mut paths = Vec::with_capacity(dnf.1.len());
    for branch in dnf.1 {
        let branch = combine_index_merge_conjuncts(&common, branch);
        let candidates = crate::access_cost::enumerate_paths(
            input.table,
            input.columns,
            Some(&branch),
            &[handle],
            &resolver,
            None,
            stats,
            input.hints,
            false,
            input.partition_scan,
            input.hints.has_forced_path(),
            // An IndexMerge partial; the heuristic never runs through one.
            false,
            None,
        )
        .into_iter()
        .filter(|candidate| {
            candidate.path.index.is_some()
                && !candidate.access_columns.is_empty()
                && candidate.path.index.as_ref().is_some_and(|(index_id, _)| {
                    crate::access_cost::index_is_covering(input.table, *index_id, &[handle])
                })
        })
        .collect();
        paths.push(crate::access_cost::choose_access_path(
            candidates, stats, false, false,
        )?);
    }
    let index_ids = paths
        .iter()
        .filter_map(|path| path.index.as_ref().map(|(index_id, _)| *index_id))
        .collect::<std::collections::BTreeSet<_>>();
    if index_ids.len() < 2 {
        return None;
    }
    let (regular, needed) = best_single_table_access_path(
        select,
        input.catalog,
        input.scope,
        input.table,
        input.columns,
        input.hints,
        input.partition_scan,
        input.ordering_index_selectivity_ratio,
        None,
        None,
    )?;
    let rows = crate::access_cost::realtime_row_count(stats)
        * crate::access_cost::selectivity_with_default_string_match_selectivity(
            where_clause,
            input.table,
            &resolver,
            stats,
            input.default_string_match_selectivity,
        );
    if crate::access_cost::index_merge_cost(input.table, &needed, stats, rows, &paths)
        >= regular.cost
    {
        return None;
    }
    let partials = paths.into_iter().filter_map(|path| path.index).collect();
    Some(IndexMergePlan {
        kind: IndexMergeKind::Union,
        partials,
    })
}

fn collect_index_merge_disjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_index_merge_disjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, left, right) => {
            collect_index_merge_disjuncts(left, out);
            collect_index_merge_disjuncts(right, out);
        }
        other => out.push(other),
    }
}

fn combine_index_merge_conjuncts(
    common: &[&tidb_ast::Expr],
    branch: &tidb_ast::Expr,
) -> tidb_ast::Expr {
    common.iter().rev().fold(branch.clone(), |right, left| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new((*left).clone()),
            Box::new(right),
        )
    })
}

fn commit_index_merge_source(
    table: &KvTable,
    scope: &FromScope,
    columns: &[(String, FieldType)],
    plan: IndexMergePlan,
    from_source: &mut Option<Box<dyn Executor>>,
    trace: Option<&mut PlanTrace>,
    ctx: &crate::StmtContext,
) {
    let exec = IndexMergeSourceExec::new_with_context(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        plan.kind,
        plan.partials.clone(),
        crate::kv_table::RowDecodeContext::for_query(ctx),
    );
    if let Some(trace) = trace {
        let indexes = plan
            .partials
            .iter()
            .filter_map(|(id, _)| table.indexes().iter().find(|index| index.id == *id))
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        trace.index_merge(
            source_table_name(scope, &table.name),
            &indexes,
            matches!(plan.kind, IndexMergeKind::Intersection),
        );
        trace.set_scan_act_rows(exec.produced_rows());
    }
    *from_source = Some(Box::new(exec));
}

#[cfg(test)]
thread_local! {
    static ORDINARY_ACCESS_PATH_ENTRIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_ordinary_access_path_entries() {
    ORDINARY_ACCESS_PATH_ENTRIES.with(|entries| entries.set(0));
}

#[cfg(test)]
pub(crate) fn ordinary_access_path_entries() -> usize {
    ORDINARY_ACCESS_PATH_ENTRIES.with(std::cell::Cell::get)
}

fn join_predicates(predicates: &[&tidb_ast::Expr]) -> Option<tidb_ast::Expr> {
    predicates.iter().cloned().cloned().reduce(|left, right| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    })
}

/// Builds the narrowed handle source. A simple select list is part of Go's
/// point plan itself; every other field shape keeps the ordinary all-column
/// source so the driver can build its real Selection and Projection above it.
fn handle_source_exec(
    table: &KvTable,
    handles: Vec<TableHandle>,
    columns: &[(String, FieldType)],
    output: Option<&FastPointOutput>,
    ctx: &crate::StmtContext,
) -> HandleSourceExec {
    // Go's extra handle column reports the record HANDLE, and nothing in the
    // decoded row fills it, so a schema that names `_tidb_rowid` has a slot
    // only the source can write. Both arms need it: the projected one's
    // offsets are into the same source row, and one of them may BE the slot.
    let extra_handle = crate::access_path::extra_handle_slot(columns);
    match output {
        Some(output) => HandleSourceExec::new_projected_with_context(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&output.columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handles,
            output.offsets.clone(),
            crate::kv_table::RowDecodeContext::for_query(ctx),
        )
        .reporting_extra_handle_at(extra_handle),
        None => HandleSourceExec::new_with_context(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handles,
            crate::kv_table::RowDecodeContext::for_query(ctx),
        )
        .reporting_extra_handle_at(extra_handle),
    }
}

/// Go `buildSchemaFromFields` for a point plan: only source columns and
/// wildcards can be owned by the lookup. Expressions decline the complete
/// fast plan and leave the normal projection pipeline intact.
fn fast_point_output(select: &tidb_ast::SelectStmt, scope: &FromScope) -> Option<FastPointOutput> {
    let resolver = ScopeResolver { scope };
    let mut offsets = Vec::new();
    let mut columns = Vec::new();
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        match field {
            SelectField::Expr {
                expr: expr @ tidb_ast::Expr::Column(path),
                alias,
            } => {
                let (offset, field_type, _) = resolver.resolve(path)?;
                let name = alias.clone().unwrap_or_else(|| {
                    default_field_display_name(&select.fields, field_index, expr)
                });
                offsets.push(offset);
                columns.push((name, field_type));
            }
            SelectField::Expr { .. } => return None,
            SelectField::Wildcard(qualifier) => {
                if qualifier.last().is_none() {
                    for (offset, name, field_type) in scope.star_columns() {
                        offsets.push(offset);
                        columns.push((name, field_type));
                    }
                    continue;
                }
                let table_name = qualifier.last()?;
                let mut matched = false;
                for table in scope
                    .tables
                    .iter()
                    .filter(|table| table.name.eq_ignore_ascii_case(table_name))
                {
                    matched = true;
                    for (local_offset, (name, field_type)) in table.columns.iter().enumerate() {
                        offsets.push(table.offset + local_offset);
                        columns.push((name.clone(), field_type.clone()));
                    }
                }
                if !matched {
                    return None;
                }
            }
        }
    }
    Some(FastPointOutput { offsets, columns })
}

/// A plain column projection over an exact range can be returned directly by
/// the coprocessor read. Every clause that needs another root operator keeps
/// the ordinary pipeline for that operator to be built in the right place.
fn range_can_return_direct(select: &tidb_ast::SelectStmt) -> bool {
    !select.distinct
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && !crate::window::select_has_window(select)
}

/// The ordered-range shape whose only root work is sorting the same simple
/// columns the statement returns. The source can therefore emit exactly the
/// projected row from the cop task, while Sort remains above TableReader.
fn range_order_projection(select: &tidb_ast::SelectStmt, scope: &FromScope) -> Option<Vec<usize>> {
    if select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.order_by.is_empty()
        || select.limit.is_some()
        || crate::window::select_has_window(select)
    {
        return None;
    }
    let output = fast_point_output(select, scope)?;
    let resolver = ScopeResolver { scope };
    for item in &select.order_by {
        let tidb_ast::Expr::Column(path) = &item.expr else {
            return None;
        };
        let (offset, _, _) = resolver.resolve(path)?;
        if !output.offsets.contains(&offset) {
            return None;
        }
    }
    Some(output.offsets)
}

fn handle_path_consumes_where(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    handle_predicate_is_consumed(select.where_clause.as_ref(), table, zone)
}

fn handle_predicate_is_consumed(
    where_clause: Option<&tidb_ast::Expr>,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    where_clause.is_some_and(|where_clause| {
        predicate_is_exact_range(where_clause)
            && crate::handle_range::build_handle_ranges(table, where_clause, zone)
                .is_some_and(|built| built.access_count > 0 && built.residual.is_empty())
    })
}

fn index_path_consumes_where(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    index_id: i64,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    let Some(where_clause) = select.where_clause.as_ref() else {
        return false;
    };
    let Some(index) = table.indexes().iter().find(|index| index.id == index_id) else {
        return false;
    };
    // A prefix range is a superset even when the detacher consumed the
    // written condition; the whole value must still be checked above it.
    if index.has_prefix() {
        return false;
    }
    let range_columns: Vec<crate::index_range::RangeColumn> = index
        .column_offsets
        .iter()
        .filter_map(|offset| {
            let column = table.columns.get(*offset)?;
            Some(crate::index_range::RangeColumn::whole(
                column.name.clone(),
                column.field_type.clone(),
            ))
        })
        .collect();
    if range_columns.len() != index.column_offsets.len() {
        return false;
    }
    crate::index_range::detach_cond_and_build_range_for_index(&range_columns, where_clause, zone)
        .is_some_and(|built| {
            predicate_is_exact_range(where_clause)
                && built.access_count > 0
                && built.residual.is_empty()
        })
}

/// Whether a ranger-owned predicate describes exactly the rows in its ranges.
///
/// The ranger may report no residual for a lossy bound such as `LIKE 'abc%'.`
/// That means the expression helped construct the access range, not that the
/// range alone proves the predicate. Keep this proof deliberately closed over
/// exact comparison shapes so every approximation retains its Selection.
fn predicate_is_exact_range(predicate: &tidb_ast::Expr) -> bool {
    match predicate {
        tidb_ast::Expr::Paren(inner) => predicate_is_exact_range(inner),
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd | tidb_ast::BinaryOp::LogicOr,
            left,
            right,
        ) => predicate_is_exact_range(left) && predicate_is_exact_range(right),
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::Eq
            | tidb_ast::BinaryOp::NullEq
            | tidb_ast::BinaryOp::Ge
            | tidb_ast::BinaryOp::Gt
            | tidb_ast::BinaryOp::Le
            | tidb_ast::BinaryOp::Lt,
            ..,
        )
        | tidb_ast::Expr::In { .. }
        | tidb_ast::Expr::Between { .. }
        | tidb_ast::Expr::Is {
            target: tidb_ast::IsTarget::Null,
            ..
        } => true,
        _ => false,
    }
}

/// Whether every equality in a single-point `WHERE` is one of the key parts
/// that produced the handle. `try_point_get` may also be used as a narrowed
/// source when a common/unique key is pinned alongside an extra predicate;
/// that shape must retain its Selection above the source.
fn point_get_consumes_where(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    point_get_predicate_is_consumed(
        select.where_clause.as_ref(),
        table,
        columns,
        zone,
        sole_table_ref(&select.from).map_or(&[][..], |table_ref| table_ref.partitions.as_slice()),
    )
}

/// Whether a write's narrowed read path consumed its complete predicate.
/// Go's update/delete fast plan has no Selection in this case; a range path
/// or a point lookup with any extra equality must still evaluate the WHERE.
pub(crate) fn write_read_path_consumes_predicate(
    read_path: Option<&WriteReadPath>,
    stmt: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    match read_path {
        Some(WriteReadPath::Batch(_)) => true,
        Some(WriteReadPath::Point(_)) => {
            point_get_predicate_is_consumed(
                stmt.where_clause,
                table,
                columns,
                zone,
                stmt.named_partitions,
            )
        }
        Some(WriteReadPath::Ranges(..)) => {
            handle_predicate_is_consumed(stmt.where_clause, table, zone)
        }
        Some(WriteReadPath::IndexRanges(..)) | None => false,
    }
}

pub(crate) fn point_get_predicate_is_consumed(
    where_clause: Option<&tidb_ast::Expr>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    named_partitions: &[String],
) -> bool {
    let Some(where_clause) = where_clause else {
        return false;
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs, zone) || pairs.is_empty() {
        return false;
    }
    let key_matches = |offsets: &[usize]| {
        offsets.len() == pairs.len()
            && offsets.iter().all(|offset| {
                columns.get(*offset).is_some_and(|(name, _)| {
                    pairs
                        .iter()
                        .any(|pair| pair.column.eq_ignore_ascii_case(name))
                })
            })
    };
    if table
        .pk_handle_offset()
        .is_some_and(|offset| key_matches(std::slice::from_ref(&offset)))
        || key_matches(table.common_handle_offsets())
    {
        return true;
    }
    // The extra handle pins a row as completely as an integer primary key
    // does, so Go's point plan over `_tidb_rowid = c` carries no `Selection`
    // either -- its recorded plan for `select * from t where _tidb_rowid = 0`
    // is a bare `Point_Get table:t handle:0`.
    if table.pk_handle_offset().is_none()
        && table.common_handle_offsets().is_empty()
        && (table.partition().is_none() || named_partitions.len() == 1)
        && pairs.len() == 1
        && pairs[0]
            .column
            .eq_ignore_ascii_case(crate::driver::leaf_demand::EXTRA_HANDLE_NAME)
    {
        return true;
    }
    let matches_unique_index = table
        .plan_indexes()
        .any(|index| index.unique && !index.has_prefix() && key_matches(&index.column_offsets));
    matches_unique_index
}

/// The partitions a single-table `SELECT` still reads, named as declared and
/// in definition order -- Go's `PartitionProcessor` output, which is the list
/// `EXPLAIN` fans a static-mode plan out over.
///
/// Two narrowings compose, in either order and both cumulative, exactly as
/// [`crate::KvTable::restrict_read_to_partitions`] composes them for the read
/// itself: the statement's own `PARTITION (p, ...)` list, and whatever the
/// `WHERE` pruned. Empty for an unpartitioned table.
///
/// An unresolvable `PARTITION (p)` name answers the FULL list rather than
/// failing here: the read has already raised 1735 for it, and this is only
/// ever asked for a plan that got built.
pub(crate) fn surviving_partitions(
    select: &tidb_ast::SelectStmt,
    table_ref: Option<&tidb_ast::TableRef>,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<(String, i64)> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let selected = table_ref
        .map(|table_ref| table_ref.partitions.as_slice())
        .filter(|names| !names.is_empty())
        .and_then(|names| {
            crate::partition_pruning::ids_for_selected_partitions(partition, names).ok()
        });
    let pruned = pruned_partition_ids(select, table, zone);
    partition
        .definitions
        .iter()
        .filter(|def| selected.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .filter(|def| pruned.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .map(|def| (def.name.clone(), def.id))
        .collect()
}

/// The partitions ONE LEAF of a multi-table `FROM` reads, named as declared
/// and in definition order.
///
/// Go's `PartitionProcessor.rewriteDataSource` walks the WHOLE logical plan
/// and divides every `DataSource` it finds, so a partitioned table inside a
/// join is fanned out exactly as a single-table `SELECT`'s is -- captured
/// over `PARTITION BY LIST (ltype)` with a predicate on a non-partitioning
/// column, where TiDB prints
/// `TableFullScan table:tx2, partition:p1` and `... partition:p2` under a
/// `PartitionUnion(Probe)`. Recognising only the single-table shape is what
/// printed one partition-less `TableFullScan table:tx2` there.
///
/// This is [`surviving_partitions`] MINUS the `WHERE` narrowing: a join
/// leaf's read is restricted by its `PARTITION (p, ...)` list alone
/// (`restricted_to_partitions` at the leaf build site), so the list named
/// here is exactly the set the leaf's executor walks. Go additionally prunes
/// the leaf by its own pushed-down conditions; doing that here would have to
/// narrow the leaf's READ in the same breath, which the index and lookup
/// arms do not yet route through one restriction point. Naming more
/// partitions than Go is a plan that over-describes the read, never one that
/// reads too few rows.
pub(crate) fn leaf_read_partitions(
    table: &KvTable,
    named_partitions: &[String],
) -> Vec<(String, i64)> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let selected = Some(named_partitions)
        .filter(|names| !names.is_empty())
        .and_then(|names| {
            crate::partition_pruning::ids_for_selected_partitions(partition, names).ok()
        });
    partition
        .definitions
        .iter()
        .filter(|def| selected.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .map(|def| (def.name.clone(), def.id))
        .collect()
}

/// The estimate each surviving partition's own `DataSource` carries, in the
/// order [`surviving_partitions`] lists them.
///
/// Go reads it from that partition's `PhysicalTableID`
/// (`stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)`),
/// which under static pruning is the only id `ANALYZE` ever stored a
/// histogram under.
pub(crate) fn surviving_partition_estimates(
    catalog: &Catalog,
    partitions: &[(String, i64)],
) -> Vec<crate::access_cost::ScanEstimate> {
    partitions
        .iter()
        .map(|(_, id)| {
            let stats = catalog.table_statistics(*id);
            crate::access_cost::ScanEstimate {
                rows: crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref)),
                pseudo: stats.is_none_or(|stats| stats.pseudo),
            }
        })
        .collect()
}

/// Installs a zero-row [`TableDualExec`] for a contradictory `WHERE` and
/// records the `TableDual rows:0` node in place of the scan `build_from` traced.
///
/// The `WHERE` stays in the pipeline above (as every fast path leaves it), so
/// the `Selection` over this source is fed no rows and produces none -- the
/// same answer the full scan gave, reached without reading the table.
fn install_contradiction_dual(
    columns: &[(String, FieldType)],
    from_source: &mut Option<Box<dyn Executor>>,
    trace: Option<&mut PlanTrace>,
) {
    let exec = crate::table_dual::TableDualExec::new(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        0,
    );
    if let Some(trace) = trace {
        trace.empty_range_table_dual();
    }
    *from_source = Some(Box::new(exec));
}

/// The clustered integer handle a single-point table range names, when the
/// range list is exactly one non-null point -- Go's `IsPointNonNullable` over
/// an `IsIntHandlePath` in `isPointGetPath`.
///
/// `None` for anything else: several ranges, an open bound, a NULL endpoint, or
/// a non-integer bound. Callers must separately prove this range belongs to
/// an integer-handle table: common-handle prefix ranges can have the same
/// one-datum encoding without naming a complete row key.
pub(crate) fn single_point_handle(ranges: &[IndexRange]) -> Option<TableHandle> {
    let [range] = ranges else {
        return None;
    };
    if range.low_exclusive || range.high_exclusive {
        return None;
    }
    match (range.low.as_slice(), range.high.as_slice()) {
        ([Datum::Int(low)], [Datum::Int(high)]) if low == high => Some(TableHandle::Int(*low)),
        ([Datum::UInt(low)], [Datum::UInt(high)]) if low == high => {
            Some(TableHandle::Int(*low as i64))
        }
        _ => None,
    }
}

/// Installs the streaming index-range source for a committed index path, and
/// records the node `EXPLAIN` prints for it.
#[allow(clippy::too_many_arguments)]
fn commit_index_range_source(
    table: &KvTable,
    // For the per-partition statistics a fanned-out batch point get is
    // estimated from; the scan estimate itself arrives precomputed.
    catalog: &Catalog,
    scope: &FromScope,
    columns: &[(String, FieldType)],
    index_id: i64,
    ranges: Vec<IndexRange>,
    estimate: crate::access_cost::ScanEstimate,
    // Go's `path.IsSingleScan`; see [`ChosenPath::Index`].
    covering: bool,
    // Whether the WHERE left no conjunct unconsumed by this index's ranges,
    // which is what gates Go's fast point shapes.
    index_point_allowed: bool,
    // Whether an `INDEX_LOOKUP_PUSHDOWN` hint elected this index's lookup
    // for Go's `LocalIndexLookUp` execution (`AvailablePaths::
    // lookup_pushdown_hinted`). Meaningless for a covering path, which
    // builds no handle batch to push down.
    lookup_pushdown: bool,
    from_source: &mut Option<Box<dyn Executor>>,
    trace: Option<&mut PlanTrace>,
    index_order: &mut Option<IndexAccessOrder>,
    ctx: &crate::StmtContext,
) {
    let mut exec = IndexRangeSourceExec::new_with_statement(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        index_id,
        ranges.clone(),
        crate::kv_table::RowDecodeContext::for_query(ctx),
        crate::remote_scan::PushdownStatementContext::from_stmt(ctx),
    );
    // The schema above may already be NARROWER than the table (the leaf
    // demand prunes before the access path replaces the source), so the
    // reader is told which stored column each slot is rather than assuming
    // the first n. `_tidb_rowid` has no stored column at all -- it is the
    // record HANDLE, which this reader already holds for every row it looks
    // up -- so it is named separately.
    let handle_slot = crate::access_path::extra_handle_slot(columns);
    if let Some(slot) = handle_slot {
        exec.read_extra_handle(slot);
    }
    let stored = handle_slot.map_or(columns, |slot| &columns[..slot]);
    if let Some(offsets) = crate::access_path::stored_column_offsets(table, stored) {
        exec.read_table_columns(offsets);
    }
    crate::table_access::TableAccess::accept_scan_estimate(&mut exec, estimate.rows);
    if covering {
        exec.mark_covering();
    }
    // A covering path is Go's `PhysicalIndexReader`: the index answers on its
    // own, no handle batch is ever built, and the rows leave in INDEX order.
    // This tier reads the row either way (it has no index-only reader), so the
    // difference has to be declared here rather than shown by the executor's
    // shape.
    //
    // A DIRTY table reaches the same answer by the other door: Go's
    // `tableHasDirtyContent` (`pkg/planner/core/logical_plan_builder.go:5316`)
    // puts a `UnionScanExec` above the reader, and its `compare()` orders on
    // the index's own columns before the handle -- so a double read inside a
    // transaction that has written this table answers in index order too.
    if covering || table.has_dirty_content() {
        exec.answer_in_index_order();
    }
    if lookup_pushdown && !covering {
        // Go plans a `LocalIndexLookUp` for the hinted index and its
        // executor truncates a pushed LIMIT per partition AFTER the handle
        // sort, where the plain lookup truncates the index stream before
        // it; see `IndexRangeSourceExec::lookup_pushdown`.
        exec.mark_lookup_pushdown();
    }
    let index = table
        .indexes()
        .iter()
        .find(|index| index.id == index_id)
        .expect("the chosen path names an index of this table");
    // An index entry always carries its table's handle in the key suffix --
    // that is what makes `a` readable from an `idx_b(b)` entry of a
    // clustered-PK table, and it is the same fact Go's `IsCoveringIndex`
    // relies on when it counts `pkIsHandle` columns as covered
    // (`pkg/planner/core/find_best_task.go`). The ORDER claim stays the
    // index's own key order; the handle offsets ride along separately so a
    // RESIDUAL over them is known to be answerable from the index source.
    let mut order = IndexAccessOrder::from_ranges(index.ordered_column_offsets(), &ranges);
    order.handle_covered_offsets = table
        .pk_handle_offset()
        .into_iter()
        .chain(table.common_handle_offsets().iter().copied())
        .collect();
    *index_order = Some(order);
    if let Some(trace) = trace {
        let index_columns: Vec<String> = index
            .column_offsets
            .iter()
            .map(|offset| index_key_part_name(table, *offset))
            .collect();
        let index_columns: Vec<&str> = index_columns.iter().map(String::as_str).collect();
        let point_ranges = index.unique
            && !index.has_prefix()
            && !ranges.is_empty()
            && ranges.iter().all(|range| {
                range.low.len() == index.column_offsets.len() && range.is_point(false)
            });
        let point_partitions = if point_ranges {
            index_range_partition_names(table, index, &ranges, ctx)
        } else {
            Vec::new()
        };
        let clustered = index.name.eq_ignore_ascii_case("PRIMARY")
            && index.column_offsets == table.common_handle_offsets();
        // Go's `findBestTask` returns a `PhysicalTableDual` the moment a
        // chosen path has NO ranges (`find_best_task.go`: `if
        // len(path.Ranges) == 0`), so a contradictory `WHERE` prints no scan
        // at all. Reached only through the `USE INDEX` cases in
        // `tests/integrationtest/t/util/ranger.test`, where an UNSIGNED key
        // part meets a negative bound.
        if ranges.is_empty() {
            trace.empty_range_table_dual();
            trace.set_scan_act_rows(exec.produced_rows());
            *from_source = Some(Box::new(exec));
            return;
        }
        // Fix 52592 disables Go's fast point/batch plans (`tryFastPlan`
        // returns early), so the SAME point ranges print as an ordinary
        // index range scan while it is on.
        let fast_point_allowed = !ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false);
        // A path the ranger narrowed nothing on reads the whole index, which
        // Go names `IndexFullScan` and prints without a `range:`.
        let fast_point_trace = point_ranges && fast_point_allowed && index_point_allowed;
        // Go's NORMAL planner converts a multi-point path on a unique index
        // into a `Batch_Point_Get` even when conjuncts REMAIN:
        // `findBestTask`'s `canConvertPointGet` never asks whether the WHERE
        // was consumed, and `convertToBatchPointGet` moves the leftover
        // `IndexFilters`/`TableFilters` into a ROOT `Selection` above the
        // batch read (`pkg/planner/core/find_best_task.go`). For a
        // partitioned table with several point ranges that conversion
        // additionally requires static pruning (dynamic refuses
        // `len(path.Ranges) > 1`) and hash/key partitioning over one plain
        // column (`getHashOrKeyPartitionColumnName`).
        let residual_batch_point = point_ranges
            && fast_point_allowed
            && !index_point_allowed
            && ranges.len() > 1
            && ctx.static_partition_prune()
            && hash_or_key_partition_column(table)
            && !point_partitions.is_empty();
        if fast_point_trace && ranges.len() == 1 {
            trace.index_point_get(
                source_table_name(scope, &table.name),
                &point_partitions,
                &format!(
                    "{}index:{}({})",
                    if clustered { "clustered " } else { "" },
                    index.name,
                    index_columns.join(", ")
                ),
            );
        } else if fast_point_trace && ctx.static_partition_prune() || residual_batch_point {
            trace.index_batch_point_get(
                source_table_name(scope, &table.name),
                ranges.len(),
                &point_partitions,
                &format!(
                    "{}index:{}({})",
                    if clustered { "clustered " } else { "" },
                    index.name,
                    index_columns.join(", ")
                ),
                true,
                &batch_point_branch_estimates(catalog, table, &point_partitions, ranges.len()),
            );
        } else if ranges.len() == 1 && ranges[0].is_full() {
            trace.index_full_scan(
                source_table_name(scope, &table.name),
                &index.name,
                &index_columns,
                estimate,
                false,
            );
        } else {
            trace.index_range_scan(
                source_table_name(scope, &table.name),
                &index.name,
                &index_columns,
                &ranges,
                estimate,
            );
        }
        trace.set_scan_act_rows(exec.produced_rows());
        if !covering
            && !fast_point_trace
            && !residual_batch_point
            && !trace.index_lookup(source_table_name(scope, &table.name), estimate)
        {
            trace.refuse("a non-covering index path did not produce an index scan");
        }
    }
    *from_source = Some(Box::new(exec));
}

fn index_range_partition_names(
    table: &KvTable,
    index: &crate::kv_table::KvIndex,
    ranges: &[IndexRange],
    ctx: &impl tidb_expr::Columns,
) -> Vec<String> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let mut ordinals = Vec::new();
    for range in ranges {
        let mut row = vec![Datum::Null; table.columns.len()];
        for (offset, value) in index.column_offsets.iter().zip(&range.low) {
            row[*offset] = value.clone();
        }
        if let Ok(ordinal) = partition.locate_ordinal(&row, &table.columns, ctx) {
            if !ordinals.contains(&ordinal) {
                ordinals.push(ordinal);
            }
        }
    }
    ordinals.sort_unstable();
    ordinals
        .into_iter()
        .filter_map(|ordinal| partition.definitions.get(ordinal))
        .map(|definition| definition.name.clone())
        .collect()
}

/// Go `getHashOrKeyPartitionColumnName`
/// (`pkg/planner/core/point_get_plan.go:1440`): a partitioned
/// `Batch_Point_Get` over SEVERAL ranges exists only when the table is HASH
/// partitioned over a bare column or KEY partitioned over exactly one column
/// -- the only routings `BatchPointGetExec::initialize`'s `getPhysID` can
/// evaluate per handle.
fn hash_or_key_partition_column(table: &KvTable) -> bool {
    table.partition().is_some_and(|partition| match partition.kind {
        crate::partition_routing::PartitionKind::Hash => {
            matches!(partition.expr, Expression::Column(_))
        }
        crate::partition_routing::PartitionKind::Key => partition.dependencies.len() == 1,
        _ => false,
    })
}

/// The estimate each partition branch of a fanned-out batch point get
/// carries, in the order `partitions` names them.
///
/// Go's static prune gives every partition its own `DataSource` whose
/// `CountAfterAccess` comes from THAT partition's statistics:
/// `getIndexRowCountForStatsV2` counts exactly one row per full-length
/// non-null point range on a unique index and clamps the sum into
/// `[1, realtimeRowCount]` (`pkg/planner/cardinality/row_count_index.go`),
/// and `convertToBatchPointGet`'s `min(CountAfterAccess, len(ranges))` cap
/// is already inside that clamp. A partition without analyzed statistics
/// keeps the range count -- the pseudo estimator has no row count to clamp
/// with.
fn batch_point_branch_estimates(
    catalog: &Catalog,
    table: &KvTable,
    partitions: &[String],
    point_count: usize,
) -> Vec<f64> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    partitions
        .iter()
        .map(|name| {
            partition
                .definitions
                .iter()
                .find(|definition| definition.name == *name)
                .and_then(|definition| catalog.table_statistics(definition.id))
                .filter(|stats| !stats.pseudo)
                .map_or(point_count as f64, |stats| {
                    let rows = crate::access_cost::realtime_row_count(Some(stats.as_ref()));
                    (point_count as f64).min(rows).max(1.0)
                })
        })
        .collect()
}

/// The schema a fast-path source emits: the scope's columns in scope order,
/// each carrying the unique id the driver's resolver hands expressions.
pub(crate) fn source_schema_columns(columns: &[(String, FieldType)]) -> Vec<Column> {
    columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect()
}

/// Offers the source only the columns the statement reads, narrowing `scope`
/// with it when the source takes the offer (Go's `rule_column_pruning.go`).
///
/// This runs BEFORE any expression is built, which is the whole point: every
/// offset below is resolved against the narrowed scope from the start, so no
/// already-built index has to be renumbered. It also runs before the predicate
/// push-down, so a pushed conjunct's `column_offset` is already in narrow
/// space -- and the kept set contains the `WHERE`'s columns because the gate
/// collected them.
///
/// No "was the source replaced?" flag is needed: `accept_column_prune`
/// defaults to refusing, so a fast-path source that cannot project simply says
/// no and the full-width path stands. Each source answers for itself,
/// fail-closed -- the same rule the pushed filter and row cap follow.
pub(crate) fn prune_scan_columns(
    select: &tidb_ast::SelectStmt,
    scope: &mut FromScope,
    from_source: &mut Option<Box<dyn Executor>>,
) {
    let Some(source) = from_source.as_mut() else {
        return;
    };
    let Some(keep) = crate::column_prune::prunable_columns(select, scope) else {
        return;
    };
    if keep.len() < scope.width()
        && source
            .table_access()
            .is_some_and(|access| access.accept_column_prune(&keep))
    {
        *scope = crate::column_prune::pruned_scope(scope, &keep);
    }
}

/// Offers the source the conjuncts it can apply itself, and reports both the
/// `WHERE` that must still run above it (`None`: the source took all of it)
/// and the physical Selection conditions the source accepted. Execution keeps
/// the original built filters inside [`PushedScanFilter`]; the returned view
/// uses the paired scan descriptions to expose Go's folded comparison
/// constants and top-level CNF without repeating conversions or warnings.
///
/// Over a single base table every source below is a real streaming scan, so
/// each answers for itself whether it can keep the promise
/// [`crate::table_access`] describes -- an index range can (it tests every row
/// it emits), a point get's handle source refuses. Only the residual then
/// needs a `Selection`; when the scan takes the whole `WHERE` there is no
/// `Selection` executor left, but the recorded plan is unchanged either way --
/// Go prints one `Selection` over the scan for both halves (captured,
/// `pkg/executor/zz_dump_pushdown_test.go`), and this tier prints no
/// `TableReader`/`cop[tikv]` task to distinguish them.
pub(crate) fn negotiate_scan_filter(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    source: &mut Box<dyn Executor>,
    ctx: &crate::StmtContext,
    access_consumed_where: bool,
    trace: Option<&mut PlanTrace>,
) -> (Option<tidb_ast::Expr>, Vec<Expression>) {
    if access_consumed_where {
        return (None, Vec::new());
    }
    match (&select.where_clause, scope.tables.len()) {
        (Some(predicate), 1) => {
            let (pushed, residual) = split_scan_predicates(predicate, &scope_resolver(scope), ctx);
            let accepted = !pushed.is_empty()
                && source
                    .table_access()
                    .is_some_and(|access| access.accept_scan_filter(&pushed, ctx));
            if accepted {
                // `TableFullScan`'s `actRows` counts rows read, not rows kept,
                // so it is taken from the scan itself rather than from the
                // (now filtered) chunks leaving it.
                if let (Some(trace), Some(scanned)) = (
                    trace,
                    source
                        .table_access()
                        .and_then(|access| access.scanned_rows_counter()),
                ) {
                    trace.set_scan_act_rows(scanned);
                }
                (residual, pushed.selection_conditions())
            } else {
                (Some(predicate.clone()), Vec::new())
            }
        }
        (where_clause, _) => (where_clause.clone(), Vec::new()),
    }
}

/// Offers the source the `LIMIT`'s row cap, when [`scan_limit_cap`] finds one
/// is sound.
///
/// This must run before any wrapper goes over the source, because the cap is a
/// promise only the source itself can keep.
pub(crate) fn offer_scan_limit(
    select: &tidb_ast::SelectStmt,
    residual_where: Option<&tidb_ast::Expr>,
    index_order: Option<&IndexAccessOrder>,
    resolver: &ScopeResolver<'_>,
    source: &mut Box<dyn Executor>,
) -> bool {
    let Some(cap) = scan_limit_cap(select, residual_where, index_order, resolver) else {
        return false;
    };
    source
        .table_access()
        .is_some_and(|access| access.accept_scan_limit(cap))
}

/// Offers Go's `PhysicalIndexLookUpReader.PushedLimit` to a non-covering
/// ordered access. The source skips the SQL offset in its index handle stream
/// and never builds table tasks outside the requested window.
pub(crate) fn offer_embedded_lookup_limit(
    select: &tidb_ast::SelectStmt,
    residual_where: Option<&tidb_ast::Expr>,
    index_order: Option<&IndexAccessOrder>,
    resolver: &ScopeResolver<'_>,
    source: &mut Box<dyn Executor>,
) -> Option<(u64, u64)> {
    scan_limit_cap(select, residual_where, index_order, resolver)?;
    let limit = select.limit.as_ref()?;
    let count = eval_limit_bound(&limit.count).ok()?;
    let offset = limit.offset.as_ref().map_or(Ok(0), eval_limit_bound).ok()?;
    source
        .table_access()?
        .accept_embedded_lookup_limit(offset, count)
        .then_some((offset, count))
}

/// Tells the source whether the order it walks in is the order the statement
/// asked for -- Go's `keep order:true`, which for an `IndexLookUp` decides
/// whether the handle batch is answered in index order or in handle order
/// (see [`crate::table_access::TableAccess::accept_keep_order`]).
///
/// The condition is the SAME [`order_is_index_order`] the limit push-down
/// asks, because it is the same question: Go derives both from one required
/// physical property. It is asked here without the limit, since `keep order`
/// is a property of the read and not of any cap on it.
///
/// Like `offer_scan_limit` this must run before any wrapper goes over the
/// source, and unlike it, nothing above depends on the answer -- a source
/// that refuses is still correct.
pub(crate) fn offer_keep_order(
    select: &tidb_ast::SelectStmt,
    index_order: Option<&IndexAccessOrder>,
    resolver: &ScopeResolver<'_>,
    source: &mut Box<dyn Executor>,
) -> bool {
    let Some(order) = index_order else {
        return false;
    };
    if select.order_by.is_empty() || !order_is_index_order(select, order, resolver) {
        return false;
    }
    source.table_access().is_some_and(|access| {
        access.accept_keep_order(select.order_by.first().is_some_and(|item| item.desc))
    })
}
/// The index access path a `WHERE` should be read through, when an index
/// beats the full table scan.
///
/// Go's `DetachCondAndBuildRangeForIndex` splits a predicate into access
/// conditions, which become index ranges, and filter conditions, which stay
/// above the read; `findBestTask` then costs every path that split produced
/// and keeps the cheapest. This does the same through
/// [`crate::access_cost`], and returns `None` when the winner is the full
/// scan -- so a filter too broad to pay for an index simply leaves the scan
/// in place, which is the case a "first index that fits" rule always got
/// wrong.
///
/// The whole `WHERE` stays in the pipeline either way, so the filter half of
/// the split is applied by the selection rather than dropped.
/// The narrowed source [`choose_access_path`] committed to, when it narrowed
/// one at all.
///
/// Go's `findBestTask` returns ONE path over a data source and the reader it
/// lowers to follows from which; splitting the two here keeps the driver from
/// having to ask an `Option<index>` what kind of scan it is holding.
pub(crate) enum ChosenPath {
    /// An index path: the index's id, the ranges of it to read, its
    /// estimate, and Go's `path.IsSingleScan` -- whether the index alone
    /// answers the statement (`PhysicalIndexReader`) or a row lookup follows
    /// it (`PhysicalIndexLookUpReader`), which is what decides the row ORDER
    /// (see [`crate::access_path::IndexRangeSourceExec`]).
    Index(
        i64,
        Vec<IndexRange>,
        crate::access_cost::ScanEstimate,
        bool,
        tidb_planner::candidate_cost::Candidate,
        f64,
    ),
    /// A table path the ranger narrowed, over the clustered integer handle.
    /// An EMPTY range list is the contradictory `WHERE` that reads nothing.
    HandleRange(
        Vec<IndexRange>,
        crate::access_cost::ScanEstimate,
        tidb_planner::candidate_cost::Candidate,
        f64,
    ),
    /// The whole-table path already installed by `build_from`. The executor
    /// needs no replacement, but its complete scan/reader task must remain
    /// available to parent physical candidates.
    FullTable(tidb_planner::candidate_cost::Candidate, f64),
}

pub(crate) fn choose_index_range_path(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    hints: &crate::index_hints::AvailablePaths,
    // Go `getTableScanPenalty`'s `hasPartitionScan`, decided by the caller
    // because it is the caller that ran the pruning.
    partition_scan: bool,
    ctx: &crate::StmtContext,
    source_rows: Option<f64>,
    required_order: Option<&[usize]>,
) -> Option<ChosenPath> {
    let (best, needed) = best_single_table_access_path(
        select,
        catalog,
        scope,
        table,
        columns,
        hints,
        partition_scan,
        ctx.ordering_index_selectivity_ratio(),
        source_rows,
        required_order,
    )?;
    let estimate = best.estimate;
    let planner_candidate = best.planner_candidate;
    let source_rows = best.source_rows;
    match (best.index, best.table_ranges) {
        (Some((index_id, ranges)), _) => {
            let covering = crate::access_cost::index_is_covering(table, index_id, &needed);
            Some(ChosenPath::Index(
                index_id,
                ranges,
                estimate,
                covering,
                planner_candidate,
                source_rows,
            ))
        }
        (None, Some(ranges)) => Some(ChosenPath::HandleRange(
            ranges,
            estimate,
            planner_candidate,
            source_rows,
        )),
        (None, None) => Some(ChosenPath::FullTable(planner_candidate, source_rows)),
    }
}

/// Returns the regular access-path candidate and the projected columns used
/// to cost it. Automatic IndexMerge compares against this exact candidate so
/// both alternatives share the same hints, order, limit, and skyline rules.
fn best_single_table_access_path(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    hints: &crate::index_hints::AvailablePaths,
    partition_scan: bool,
    ordering_index_selectivity_ratio: f64,
    source_rows: Option<f64>,
    required_order: Option<&[usize]>,
) -> Option<(crate::access_cost::AccessPath, Vec<usize>)> {
    // No `WHERE` at all is not a reason to stop: a covering index is still a
    // candidate, and reading the whole of a narrow index beats reading the
    // whole table (Go's `path.IsSingleScan` arm of `keepIndex`).
    let where_clause = select.where_clause.as_ref();
    // The columns the statement reads, which decides whether an index path
    // covers (Go `isCoveringIndex`) and therefore whether it pays for a
    // double read.
    //
    // This is the SAME analysis a leaf of a multi-table `FROM` uses
    // ([`crate::driver::leaf_demand`]), and deliberately not
    // [`crate::column_prune::prunable_columns`]. The two answer different
    // questions: the pruner NARROWS the source's output, so it must be exact
    // in both directions and refuses every shape it cannot prove -- any
    // subquery above all -- and a refusal reads as "every column". Go has no
    // such refusal: `rule_column_pruning` walks the correlated subquery like
    // any other expression and hands the `DataSource` the columns its parents
    // still need, which is what `isCoveringIndex` then reads. Answering the
    // COST question with the pruner's refusal made `select c2 = (select ...)
    // from t1` declare that it needs `c1, c2, c3`, so `KEY c2(c2)` -- which
    // covers `c1, c2` on an integer-handle table -- was never even a
    // candidate. Captured TiDB reads `IndexFullScan` there.
    //
    // Over-approximating is the safe direction here for the same reason it is
    // at a join leaf: the source still emits the whole row, so a demand that
    // is too wide costs a covering index as a double read and falls back to
    // the scan that would have run anyway.
    let demand = crate::driver::leaf_demand::LeafDemand::of_select(select);
    let needed: Vec<usize> = demand.needed(&scope.tables[0].name, columns);
    let resolver = ScopeResolver { scope };
    // The `LIMIT` an index path may be costed under. `scan_limit_cap`'s own
    // refusals for things between the source and the LIMIT apply here too;
    // the residual `WHERE` is the one it cannot know yet, because which
    // conjuncts the source accepts is settled after the path is chosen. Go
    // has the same ordering and resolves it through the physical property.
    let cap = costing_limit_cap(select);
    let satisfied_by = |offsets: &[usize], ranges: &[IndexRange]| {
        select.order_by.is_empty()
            || order_is_index_order(
                select,
                &IndexAccessOrder::from_ranges(offsets, ranges),
                &resolver,
            )
    };
    let limit = cap.map(|cap| crate::access_cost::PushedLimit {
        cap,
        has_order: !select.order_by.is_empty(),
        ordering_selectivity_ratio: ordering_index_selectivity_ratio,
        satisfied_by: &satisfied_by,
    });
    let stats = catalog.table_statistics(table.stats_physical_id());
    let stats = stats.as_ref().map(AsRef::as_ref);
    let mut paths = crate::access_cost::enumerate_paths(
        table,
        columns,
        where_clause,
        &needed,
        &resolver,
        limit.as_ref(),
        stats,
        hints,
        !select.order_by.is_empty(),
        partition_scan,
        demand.statement_forces_an_index(),
        // This IS a whole `DataSource`'s path selection, so Go's heuristic
        // point-range pruning applies before skyline and cost.
        true,
        source_rows,
    );
    if let Some(wanted) = required_order {
        // `matchProperty` as a FILTER over the enumeration, exactly as
        // [`crate::driver::leaf_access::leaf_index_path`] applies it for a
        // leaf of a multi-table `FROM`: a path that does not already walk in
        // the parent-required order could only ever have become Go's
        // `invalidTask`. Without this, pruning a wrapped relation to a
        // covering set let the covering `IndexFullScan` REPLACE the ordered
        // table scan under a merge join's key order -- a plan whose merge
        // executor would silently interleave unsorted rows.
        paths.retain(|candidate| match &candidate.path.index {
            Some((index_id, _)) => table
                .indexes()
                .iter()
                .find(|index| index.id == *index_id)
                .is_some_and(|index| {
                    crate::driver::leaf_access::leaf_index_order(table, index, columns)
                        .starts_with(wanted)
                }),
            None => {
                crate::driver::leaf_access::leaf_handle_order(table, columns).starts_with(wanted)
            }
        });
    }
    // Go `findBestTask`'s TWO LEGS under a required sort property with no row
    // cap (`pkg/planner/core/find_best_task.go`):
    //
    //  * the UNENFORCED leg converts only candidates whose walk already
    //    delivers the order (`convertToIndexScan`/`convertToTableScan` both
    //    open with `if !prop.IsSortItemEmpty() &&
    //    !candidate.matchPropResult.Matched() { return invalidTask }`), with
    //    skyline pruning run under the ordered property;
    //  * the ENFORCED leg re-enters `findBestTask` with the EMPTY property --
    //    its own skyline pruning, where `preferRange` retains range paths
    //    exactly as an orderless statement's would -- and wraps the winner in
    //    the Sort enforcer (`EnforceProperty`). The Sort's price is the SAME
    //    `PhysicalSort` `tidb_planner::enforce` builds for a merge join's
    //    side, through the same `Candidate::Sort` cost node.
    //
    // `getTaskPlanCost` then compares the legs' totals; the min below is that
    // comparison. Collapsing both legs into ONE ordered-property pruning pass
    // was measured wrong on `expression/vitess_hash`: the ordered-property
    // `preferRange` refuses to retain a non-matching range path, so full
    // scans survived into the enforced comparison that Go's empty-property
    // pruning would have removed.
    //
    // A LIMIT cap is the other regime -- Go's `ExpectedCnt` TopN-vs-ordered-
    // Limit comparison -- and [`PushedLimit`] already carries it, so under a
    // cap the enumeration is pruned in ONE pass that marks each candidate's
    // property match (the skyline `matchResult` dimension and the
    // `preferRange` post-filter read it). A parent-required order
    // (`required_order`) already FILTERED above, modeling `matchProperty`'s
    // invalid-task refusal; the parent prices its own enforcer
    // (`driver::from::enforced_merge_sort`).
    if !select.order_by.is_empty() && required_order.is_none() {
        let full = [IndexRange::full()];
        let delivers = |candidate: &crate::skyline::Candidate<crate::access_cost::AccessPath>| {
            match &candidate.path.index {
                Some((index_id, ranges)) => table
                    .indexes()
                    .iter()
                    .find(|index| index.id == *index_id)
                    .is_some_and(|index| {
                        satisfied_by(
                            &crate::driver::leaf_access::leaf_index_order(table, index, columns),
                            ranges,
                        )
                    }),
                None => satisfied_by(
                    &crate::driver::leaf_access::leaf_handle_order(table, columns),
                    candidate.path.table_ranges.as_deref().unwrap_or(&full),
                ),
            }
        };
        if cap.is_some() {
            for candidate in &mut paths {
                candidate.match_property = delivers(candidate);
            }
            return crate::access_cost::choose_access_path(paths, stats, true, true)
                .map(|best| (best, needed));
        }
        // The unenforced leg: only matching candidates, under the ordered
        // property.
        let mut matching: Vec<_> = paths
            .iter()
            .filter(|candidate| delivers(candidate))
            .cloned()
            .collect();
        for candidate in &mut matching {
            candidate.match_property = true;
        }
        let best_matching = crate::access_cost::choose_access_path(matching, stats, false, true);
        // The enforced leg: every candidate, under the EMPTY property, each
        // priced as its reader UNDER the Sort enforcer.
        let by_items: Vec<bool> = select
            .order_by
            .iter()
            .map(|item| !matches!(item.expr, tidb_ast::Expr::Column(_)))
            .collect();
        let best_enforced =
            crate::access_cost::choose_access_path(paths, stats, false, false).map(|mut best| {
                let child = best.planner_candidate.clone();
                let costed = tidb_planner::candidate_cost::evaluate(
                    &child,
                    &tidb_planner::candidate_cost::CostEnv::default(),
                    tidb_planner::task_type::TaskType::Root,
                );
                let enforced = tidb_planner::candidate_cost::Candidate::Sort {
                    child: Box::new(child),
                    rows: costed.rows,
                    row_size: tidb_planner::candidate_cost::RowSize::Fixed(costed.row_size),
                    by_items,
                };
                best.cost = tidb_planner::candidate_cost::evaluate(
                    &enforced,
                    &tidb_planner::candidate_cost::CostEnv::default(),
                    tidb_planner::task_type::TaskType::Root,
                )
                .est_cost();
                best
            });
        let best = match (best_matching, best_enforced) {
            (Some(matching), Some(enforced)) => Some(if matching.cost <= enforced.cost {
                matching
            } else {
                enforced
            }),
            (matching, enforced) => matching.or(enforced),
        };
        return best.map(|best| (best, needed));
    }
    // Go's `prop.ExpectedCnt != math.MaxFloat64`: a row cap on the required
    // property is what disables Fix45132's row-ratio rule inside pruning.
    crate::access_cost::choose_access_path(paths, stats, cap.is_some(), false)
        .map(|best| (best, needed))
}

/// The partitions a single-table `SELECT`'s `WHERE` proves it has to read,
/// or `None` when nothing narrows them.
///
/// The ranges come from the crate's ONE range builder
/// ([`crate::index_range::detach_cond_and_build_range_for_index`]), asked for
/// the partition expression's column exactly as it would be asked for a
/// single-column index on it. That reuse is the point: Go prunes with the
/// same `ranger` machinery it builds index ranges with, and a second range
/// implementation here would be a second answer to disagree with.
///
/// Pruning is declined -- reading everything -- in two cases, each a
/// SUPERSET and so never a wrong answer:
///
/// * a table with no partitioning;
/// * a partition expression that is not a bare COLUMN. Go prunes `year(a)`
///   through `MakePartitionByFnCol`'s monotonicity analysis, which this tier
///   does not port; a monotonicity claim that is wrong drops a partition
///   holding matching rows;
/// * a `SELECT` with no `WHERE`, which constrains nothing.
fn pruned_partition_ids(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Vec<i64>> {
    let partition = table.partition()?;
    let where_clause = select.where_clause.as_ref()?;
    let tuple_partitioning = matches!(
        partition.kind,
        crate::PartitionKind::Key
            | crate::PartitionKind::ListColumns { .. }
            | crate::PartitionKind::RangeColumns { .. }
    );
    // A bare column is the one scalar partition expression whose own value a
    // range over a column is. Tuple partitioning owns its named tuple.
    let mut range_columns = Vec::with_capacity(partition.dependencies.len());
    for dependency in &partition.dependencies {
        let column = table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(dependency))?;
        if !tuple_partitioning && partition.expr_text != format!("`{}`", column.name) {
            return None;
        }
        range_columns.push(crate::index_range::RangeColumn::whole(
            column.name.clone(),
            column.field_type.clone(),
        ));
    }
    if range_columns.is_empty() {
        return None;
    }
    // Go `PartitionProcessor.prune` runs its conditions through
    // `applyPredicateSimplification` -- whose first act is
    // `expression.PushDownNot` -- BEFORE handing them to the pruner, and its
    // own comment gives the reason: a `not (a < 5)` the ranger cannot read
    // yields no range at all, which reads here as "prune nothing" and leaves
    // the `values less than (0)` partition in a plan TiDB prunes it out of.
    let normalized = crate::partition_pruning::push_down_not(where_clause);
    // Go `DetachCondAndBuildRangeForPartition`, which is the one ranger entry
    // that does NOT convert its points to sort keys: a partition bound is a
    // written value compared under the partition column's own collation, not
    // an index's stored form.
    let built = crate::index_range::detach_cond_and_build_range_for_partition(
        &range_columns,
        &normalized,
        zone,
    )?;
    crate::partition_pruning::pruned_ids(partition, &built.ranges)
}

/// How `EXPLAIN` names one key part of an index.
///
/// An ordinary key part is the column's name. An expression index's key part
/// is the EXPRESSION, not the hidden column the DDL rewrote it into: Go
/// prints `` index:k1(`a` + 1, b) ``, and the hidden column's generated name
/// appears in no user-visible output at all. The text is the one the column
/// already stores, so the plan and `SHOW CREATE TABLE` cannot disagree.
pub(crate) fn index_key_part_name(table: &KvTable, offset: usize) -> String {
    let Some(column) = table.columns.get(offset) else {
        return String::new();
    };
    match &column.generated {
        Some(generated) if table.is_hidden(offset) => generated.expr_text.clone(),
        _ => column.name.clone(),
    }
}

/// The `offset + count` an index path may be costed under, when nothing
/// between the source and the `LIMIT` can drop or add a row.
///
/// This is [`scan_limit_cap`]'s rule minus the two halves that are not known
/// until a path is committed: the residual `WHERE`, and which index supplies
/// the order (the caller supplies that as `satisfied_by`).
fn costing_limit_cap(select: &tidb_ast::SelectStmt) -> Option<f64> {
    let limit = select.limit.as_ref()?;
    let count = eval_limit_bound(&limit.count).ok()?;
    let offset = match &limit.offset {
        Some(expr) => eval_limit_bound(expr).ok()?,
        None => 0,
    };
    if select.distinct
        || select.having.is_some()
        || !select.group_by.is_empty()
        || crate::window::select_has_window(select)
    {
        return None;
    }
    Some(offset.checked_add(count)? as f64)
}

/// The estimate `EXPLAIN` prints for a table read that stayed a full scan.
///
/// This is the same [`crate::access_cost`] answer the path choice used, so
/// the printed plan and the costed plan cannot disagree. A table with no
/// loaded statistics is Go's `PseudoTable`, and the estimate says so.
pub(crate) fn full_scan_estimate(
    catalog: &Catalog,
    entry: &TableEntry,
) -> crate::access_cost::ScanEstimate {
    let stats = match entry {
        TableEntry::Kv(table) => catalog.table_statistics(table.stats_physical_id()),
        // A memory table's rows are computed at query time and an
        // INFORMATION_SCHEMA view has no `mysql.stats_*` row, so there is
        // nothing to have analyzed; Go prints the pseudo constant for these
        // too.
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    };
    // The row count is real whenever a `mysql.stats_meta` row carries one,
    // even when no histogram was ever analyzed -- and in that state Go prints
    // the real count AND `stats:pseudo`. `realtime_row_count` owns the rule,
    // so this row and the cost that chose it agree by construction.
    crate::access_cost::ScanEstimate {
        rows: crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref)),
        pseudo: stats.is_none_or(|stats| stats.pseudo),
    }
}

/// `cardinality.Selectivity` for a single base table's `WHERE`.
///
/// This is what makes a `Selection` over a full scan print the estRows Go
/// prints. `None` means there is no `WHERE` to estimate, and nothing else:
/// a table with no analyzed histograms is Go's `PseudoTable`, which
/// `Selectivity` estimates through the SAME body using pseudo histograms
/// (`pkg/statistics/table.go:1034-1061` fills one per column), so routing it
/// anywhere else is what made `a = 1 and b = 2` print 10.00 against TiDB's
/// 1.00. [`crate::access_cost::selectivity`] owns both arms, and the
/// `stats:pseudo` flag stays where it was decided
/// ([`full_scan_estimate`]) -- which statistics exist is unchanged here, only
/// what is computed from them.

/// Whether this scope carries Go's extra handle column, `_tidb_rowid`.
///
/// It is named rather than counted because it is the one scope column with no
/// stored offset behind it; see [`crate::driver::from`]'s leaf, which appends
/// it, and `TableAccess::accept_extra_handle`, which fills it.
pub(crate) fn scope_carries_extra_handle(scope: &FromScope) -> bool {
    scope.tables.iter().any(|table| {
        table.columns.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case(crate::driver::leaf_demand::EXTRA_HANDLE_NAME)
        })
    })
}

pub(crate) fn stats_selectivity(
    catalog: &Catalog,
    table: &KvTable,
    scope: &FromScope,
    where_clause: Option<&tidb_ast::Expr>,
) -> Option<f64> {
    stats_selectivity_with_default_string_match_selectivity(
        catalog,
        table,
        scope,
        where_clause,
        0.0,
    )
}

pub(crate) fn stats_selectivity_with_default_string_match_selectivity(
    catalog: &Catalog,
    table: &KvTable,
    scope: &FromScope,
    where_clause: Option<&tidb_ast::Expr>,
    default_string_match_selectivity: f64,
) -> Option<f64> {
    let predicate = where_clause?;
    let stats = catalog.table_statistics(table.stats_physical_id());
    Some(
        crate::access_cost::selectivity_with_default_string_match_selectivity(
            predicate,
            table,
            &scope_resolver(scope),
            stats.as_ref().map(AsRef::as_ref),
            default_string_match_selectivity,
        ),
    )
}

/// `cardinality.Selectivity` for a `SELECT`'s `WHERE` over a single base
/// table, when that table has loaded statistics.
pub(crate) fn select_stats_selectivity(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Option<f64> {
    select_predicate_stats_selectivity(
        select,
        select.where_clause.as_ref()?,
        catalog,
        current_db,
        scope,
    )
    // A predicate spanning a join has no single DataSource statistics node.
    // Go's `Selectivity` leaves it uncovered and charges the global
    // `selectionFactor` once.
    .or_else(|| (scope.tables.len() > 1).then_some(tidb_planner::cost_factors::SELECTION_FACTOR))
}

/// `cardinality.Selectivity` for one residual predicate of a single-table
/// `SELECT`. Unlike [`select_stats_selectivity`], this deliberately does not
/// re-price access conditions already represented by a range scan.
/// The `KvTable` a scan of this `SELECT` will actually read: the catalog
/// handle narrowed by an explicit `PARTITION (...)` clause and then by
/// pruning.
///
/// Go runs `PartitionProcessor` during LOGICAL optimization, so by the time
/// anything asks `Selectivity` or
/// `stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)` the
/// `DataSource` IS the surviving partition and its id names that partition.
/// Reading the catalog handle straight, as [`single_kv_table`] does, skips
/// that step -- and static pruning stores a histogram per PHYSICAL partition
/// and no merged one, so the lookup missed and a pruned scan printed
/// `stats:pseudo` over 10000 rows after `ANALYZE` had just measured two.
/// [`plan_access_path`] already narrows before its own lookup; this is the
/// same narrowing for the estimate callers that build their own handle.
fn pruned_single_kv_table(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<KvTable> {
    let mut table = single_kv_table(&select.from, catalog, current_db)?;
    if table.partition().is_none() {
        return Some(table);
    }
    if let Some(table_ref) = single_table_ref(&select.from) {
        if !table_ref.partitions.is_empty() {
            let name = table_ref
                .name
                .last()
                .map(String::as_str)
                .unwrap_or(table.name.as_str());
            table =
                super::from::restricted_to_partitions(&table, &table_ref.partitions, name).ok()?;
        }
    }
    if let Some(ids) = pruned_partition_ids(select, &table, zone) {
        table.restrict_read_to_partitions(&ids);
    }
    Some(table)
}

pub(crate) fn select_predicate_stats_selectivity(
    select: &tidb_ast::SelectStmt,
    predicate: &tidb_ast::Expr,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Option<f64> {
    select_predicate_stats_selectivity_in_session(select, predicate, catalog, current_db, scope, 0.0)
}

/// [`select_predicate_stats_selectivity`] with the session's raw
/// `tidb_default_string_match_selectivity`, which Go's `Selectivity` reads
/// for every string-match conjunct it cannot cover with statistics
/// (`pkg/planner/cardinality/selectivity.go`: `GetStrMatchDefaultSelectivity`).
pub(crate) fn select_predicate_stats_selectivity_in_session(
    select: &tidb_ast::SelectStmt,
    predicate: &tidb_ast::Expr,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
    default_string_match_selectivity: f64,
) -> Option<f64> {
    let table = pruned_single_kv_table(select, catalog, current_db, &scope.zone)?;
    stats_selectivity_with_default_string_match_selectivity(
        catalog,
        &table,
        scope,
        Some(predicate),
        default_string_match_selectivity,
    )
}

/// The loaded-statistics row count for a single-table predicate.
///
/// A decorrelated `EXISTS`/`NOT EXISTS` is a separate logical semi join in Go;
/// its preserved `DataSource` therefore owns only the ordinary local
/// predicates.  Callers that still hold the original SELECT (which also
/// contains the subquery) use this helper with the local conjuncts so the
/// semi join does not charge its `0.8` factor twice.  `None` means the source
/// is not one base table or statistics are unavailable.
pub(crate) fn select_predicate_stats_rows(
    select: &tidb_ast::SelectStmt,
    predicate: Option<&tidb_ast::Expr>,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Option<f64> {
    let table = pruned_single_kv_table(select, catalog, current_db, &scope.zone)?;
    let stats = catalog.table_statistics(table.stats_physical_id());
    let realtime = crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref));
    let selectivity = predicate
        .map(|predicate| stats_selectivity(catalog, &table, scope, Some(predicate)).unwrap_or(1.0))
        .unwrap_or(1.0);
    Some(realtime * selectivity)
}

/// The full-scan estimate and stats-backed selectivity a single-table write's
/// recorded read plan prints, resolved from the catalog by name.
pub(crate) fn single_table_trace_estimate(
    catalog: &Catalog,
    database: &str,
    name: &str,
    visible: &str,
    columns: &[(String, FieldType)],
    where_clause: Option<&tidb_ast::Expr>,
) -> (crate::access_cost::ScanEstimate, Option<f64>) {
    let Some(entry) = catalog.get_in(database, name) else {
        return (
            crate::access_cost::ScanEstimate::pseudo(crate::plan_trace::PSEUDO_ROW_COUNT),
            None,
        );
    };
    let estimate = full_scan_estimate(catalog, entry);
    let TableEntry::Kv(table) = entry else {
        return (estimate, None);
    };
    let scope = PlanTrace::single_table_scope(visible, None, columns.to_vec());
    (
        estimate,
        stats_selectivity(catalog, table, &scope, where_clause),
    )
}

/// How a single-table `UPDATE`/`DELETE` FETCHES the records it then filters.
///
/// Both arms narrow only which records are fetched. The write's own per-row
/// `WHERE` evaluation is unchanged and still decides which rows the statement
/// acts on, so the affected row set is the full scan's either way -- see
/// [`write_read_path`].
pub(crate) enum WriteReadPath {
    /// Go's `Point_Get`: one record, read by key -- carrying HOW it was
    /// pinned, because the plan prints the pin (`AccessObject`). A `None`
    /// handle is a key no row can carry, which Go also plans as a
    /// `Point_Get` that reads nothing.
    Point(PointGetPin),
    /// Go's `Batch_Point_Get`: several records read directly by their
    /// clustered or unique handles.
    Batch(Vec<TableHandle>),
    /// Go's `TableRangeScan`: the handle intervals the `WHERE` implies, and
    /// the estimate `EXPLAIN` prints for them.
    Ranges(Vec<IndexRange>, crate::access_cost::ScanEstimate),
    /// Go's `IndexRangeScan`: the id of the index the chooser preferred, the
    /// ranges of it the `WHERE` implies, and the estimate `EXPLAIN` prints. A
    /// write fetches the candidate records through the index and still filters
    /// per row above, so the ranges are a superset of the affected rows.
    IndexRanges(i64, Vec<IndexRange>, crate::access_cost::ScanEstimate),
}

/// The read a single-table `UPDATE`/`DELETE` performs to find its target
/// rows; `None` when nothing narrows it and the write reads the whole table.
///
/// Go plans a write's read from the same predicate, with the same functions,
/// as a read's. `tryUpdatePointPlan`/`tryDeletePointPlan`
/// (`pkg/planner/core/point_get_plan.go`) synthesize an `ast.SelectStmt` out
/// of the write's `TableRefs`/`Where`/`Order`/`Limit` and hand it to
/// `tryPointGetPlan` -- the SAME function a `SELECT` reaches through
/// `TryFastPlan` -- and only when that declines does the ordinary path plan a
/// `DataSource` whose table path gets its ranges from `deriveTablePathStats`
/// exactly as a `SELECT`'s does. This function is that order, and it calls
/// the same two builders the read side calls: [`try_point_get`] and
/// [`crate::handle_range`], the crate's single range algebra.
///
/// The point arm is what makes `WHERE id = 500` one key lookup instead of a
/// scan over the degenerate range `[500,500]`. A single-key range still costs
/// a range scan against storage; a key lookup does not, and that difference
/// is the whole reason Go replaces the read rather than narrowing it.
///
/// Neither arm may change the answer. A point plan is decided ONLY from
/// equalities that pin a whole key ([`try_point_get`] is Go's
/// `getNameValuePairs` rule: `AND` of `column = constant`, nothing else), the
/// key's constant is moved into the column's domain first or the plan is
/// abandoned ([`super::point_get_key`]), and the `WHERE` is still evaluated
/// per row above the fetch -- so an extra conjunct the key did not pin still
/// filters, and a key naming a row that does not exist simply reads nothing.
pub(crate) fn write_read_path(
    catalog: &Catalog,
    database: &str,
    name: &str,
    stmt: &PointPlanStmt<'_>,
    ctx: &crate::StmtContext,
) -> Result<Option<WriteReadPath>, DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return Ok(None);
    };
    // Go's order: the fast plan first, the table path only when it declines.
    // The column list is the table's own, because `try_point_get` reads it at
    // the offsets `pk_handle_offset`/`KvIndex::column_offsets` name, and those
    // are offsets into `KvTable::columns`.
    let columns: Vec<(String, FieldType)> = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    let zone = &ctx.session_zone();
    let disable_point_get = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false);
    if let Some(batch) = (!disable_point_get)
        .then(|| try_batch_point_get_stmt(stmt, table, &columns, zone))
        .transpose()?
        .flatten()
    {
        return Ok(Some(WriteReadPath::Batch(batch.into_handles())));
    }
    if let Some(handle) = (!disable_point_get)
        .then(|| try_point_get(stmt, table, &columns, zone))
        .transpose()?
        .flatten()
    {
        return Ok(Some(WriteReadPath::Point(handle)));
    }
    // Go's write plan costs the index paths beside the table path, through the
    // same chooser a `SELECT` reaches (`tryUpdatePointPlan` falls through to
    // the ordinary `DataSource`). When the winner is an index, read through it;
    // otherwise fall back to the clustered-handle table path below, unchanged.
    if let Some(index_path) = write_index_range_path(table, &columns, stmt.where_clause, name, ctx)
    {
        return Ok(Some(index_path));
    }
    Ok(
        write_handle_ranges(catalog, database, name, stmt.where_clause, zone)
            .map(|(ranges, estimate)| WriteReadPath::Ranges(ranges, estimate)),
    )
}

/// The index range a single-table `UPDATE`/`DELETE` should read through, when
/// the cost chooser prefers an index over the table path -- Go's write plan
/// reusing the read side's `findBestTask`.
///
/// Returns `Some` only when the winner is an INDEX; a table-path winner (the
/// clustered handle, or nothing) is left to [`write_handle_ranges`] so that
/// path's estimate and its `skipNull` handling are unchanged. Every column is
/// declared needed, because a write reads the whole row to act on it -- Go's
/// write is always a double read -- so the index never covers and the chooser
/// prices it honestly.
fn write_index_range_path(
    table: &KvTable,
    columns: &[(String, FieldType)],
    where_clause: Option<&tidb_ast::Expr>,
    table_name: &str,
    ctx: &crate::StmtContext,
) -> Option<WriteReadPath> {
    let where_clause = where_clause?;
    let resolver = TableResolver {
        table_name,
        columns,
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
    };
    let needed: Vec<usize> = (0..columns.len()).collect();
    let hints = crate::index_hints::AvailablePaths::unrestricted();
    let paths = crate::access_cost::enumerate_paths(
        table,
        columns,
        Some(where_clause),
        &needed,
        &resolver,
        None,
        None,
        &hints,
        false,
        false,
        // An `UPDATE`/`DELETE` carries no `FROM`-clause index hint in the
        // grammar this tier accepts, so no path of it is `path.Forced`.
        false,
        // Go's write plan falls through to the ordinary `DataSource`, so its
        // `DeriveStats` -- heuristic included -- runs exactly as a read's.
        true,
        None,
    );
    let best = crate::access_cost::choose_access_path(paths, None, false, false)?;
    match best.index {
        Some((index_id, ranges)) => {
            Some(WriteReadPath::IndexRanges(index_id, ranges, best.estimate))
        }
        None => None,
    }
}

/// The handle ranges a single-table `UPDATE`/`DELETE` reads through, and the
/// estimate `EXPLAIN` prints for that read; `None` when the `WHERE` narrows
/// the clustered integer handle by nothing and the write reads the whole
/// table.
///
/// This is the table-path half of [`write_read_path`]; see its doc for the
/// order the two halves run in and why neither can change the answer.
fn write_handle_ranges(
    catalog: &Catalog,
    database: &str,
    name: &str,
    where_clause: Option<&tidb_ast::Expr>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Vec<IndexRange>, crate::access_cost::ScanEstimate)> {
    let where_clause = where_clause?;
    let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return None;
    };
    let ranges = crate::handle_range::build_handle_ranges(table, where_clause, zone)?.ranges;
    let stats = catalog
        .table_statistics(table.stats_physical_id())
        .map(AsRef::as_ref);
    let realtime = crate::access_cost::realtime_row_count(stats);
    let columns = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect::<Vec<_>>();
    let scope = PlanTrace::single_table_scope(name, None, columns);
    let source_rows =
        realtime * stats_selectivity(catalog, table, &scope, Some(where_clause)).unwrap_or(1.0);
    let raw_rows = crate::handle_range::handle_range_row_count(table, &ranges, stats);
    let estimate = crate::access_cost::ScanEstimate {
        rows: crate::access_cost::adjust_count_after_access(raw_rows, source_rows, realtime),
        pseudo: stats.is_none_or(|stats| stats.pseudo),
    };
    Some((ranges, estimate))
}

/// Splits a `WHERE` over one base table into the conjuncts the scan can apply
/// itself and the predicate that must stay above it.
///
/// This is Go's `rule_predicate_push_down` split narrowed to the shape the
/// bounded TiKV Selection lowering already speaks -- see
/// [`crate::predicate_pushdown`] for the rule and for why the pushed half may be
/// removed from the `Selection` only when the source promises to apply it to
/// every row, staged writes included.
///
/// The residual is the remaining conjuncts re-joined with `AND` in their
/// original order, so what runs above the scan is the `WHERE` minus exactly
/// what moved into it. `None` means every conjunct was pushed.
pub(crate) fn split_scan_predicates(
    where_clause: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
    ctx: &crate::StmtContext,
) -> (PushedScanFilter, Option<tidb_ast::Expr>) {
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    let mut predicates = Vec::new();
    let mut filters = Vec::new();
    let mut residual: Vec<&tidb_ast::Expr> = Vec::new();
    for conjunct in conjuncts {
        // Go `find_best_task.go`'s two `expression.PushDownExprs(pctx,
        // ..., kv.TiKV)` calls, which split the index and table filters into
        // what the coprocessor may run and what stays above it.
        if !crate::pushdown_blacklist::blacklist_admits(
            conjunct,
            resolver,
            ctx,
            tidb_expr::infer_pushdown::PushDownStore::TiKv,
        ) {
            residual.push(conjunct);
            continue;
        }
        match scan_predicate(conjunct, resolver).and_then(|mut predicate| {
            let mut filter = rewrite_expr_resolved(conjunct, resolver).ok()?;
            // Go `refineArgs`: `int column <cmp> non-int constant` folds the
            // constant into the column's type ONCE here, so the filter this
            // scan runs on every row compares int to int. Without it the
            // string is re-coerced per row -- the same work, and the same
            // 1292 truncation, once for each row scanned.
            let unrefined = filter.clone();
            tidb_expr::builtin_compare::refine_comparisons(&mut filter, ctx).ok()?;
            // ... and the DESCRIPTION beside it has to say the same thing:
            // Go refines before it builds the comparison at all, so the
            // constant it sends TiKV -- and prints -- is the refined one.
            crate::predicate_pushdown::adopt_refined_literals(
                &mut predicate,
                &unrefined,
                &filter,
            );
            Some((predicate, filter))
        }) {
            Some((predicate, filter)) => {
                predicates.push(predicate);
                filters.push(filter);
            }
            None => residual.push(conjunct),
        }
    }
    let residual = residual.into_iter().cloned().reduce(|left, right| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    });
    (PushedScanFilter::new(predicates, filters), residual)
}

/// One conjunct as a coprocessor-describable predicate, when it is one.
///
/// The describable shapes are a column-versus-constant comparison,
/// `IS [NOT] NULL`, `[NOT] IN` over constants, and the `OR`/`NOT` composition
/// of those -- exactly the set TiKV's whitelist admits unconditionally
/// (`infer_pushdown.go`'s `scalarExprSupportedByTiKV`). `AND` is absent
/// because the caller already flattened the top-level `AND` into separate
/// conjuncts, and a nested one inside an `OR` is described by recursing into
/// the branch as its own conjunct list would not be.
fn scan_predicate(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanPredicate> {
    match conjunct {
        tidb_ast::Expr::Paren(inner) => scan_predicate(inner, resolver),
        // `NOT x` and `!x`; the arithmetic unary operators are not predicates.
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Not | tidb_ast::UnaryOp::NotKeyword, inner) => {
            Some(ScanPredicate::Not(Box::new(scan_predicate(
                inner, resolver,
            )?)))
        }
        // `x OR y`, flattened: the chain is left-associative, so flattening
        // and re-folding preserves the same disjunction.
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, ..) => {
            let mut branches = Vec::new();
            collect_disjuncts(conjunct, &mut branches);
            Some(ScanPredicate::Or(
                branches
                    .into_iter()
                    .map(|branch| scan_predicate(branch, resolver))
                    .collect::<Option<Vec<_>>>()?,
            ))
        }
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, ..) => {
            let mut branches = Vec::new();
            collect_conjuncts(conjunct, &mut branches);
            Some(ScanPredicate::And(
                branches
                    .into_iter()
                    .map(|branch| scan_predicate(branch, resolver))
                    .collect::<Option<Vec<_>>>()?,
            ))
        }
        // Only `IS [NOT] NULL`. `IS TRUE`/`IS FALSE`/`IS UNKNOWN` are separate
        // Go functions with their own signatures and their own NULL handling.
        tidb_ast::Expr::Is {
            expr,
            target: tidb_ast::IsTarget::Null,
            not,
        } => {
            let (offset, column_type) = resolve_column(expr, resolver)?;
            Some(ScanPredicate::IsNull {
                column_offset: offset,
                column_type,
                negated: *not,
            })
        }
        tidb_ast::Expr::In { expr, list, not } => {
            if list.is_empty() {
                return None;
            }
            let mut literals = Vec::with_capacity(list.len());
            for element in list {
                let (literal, literal_type) =
                    constant_value_and_type(element, &resolver.time_zone())?;
                // A NULL member makes `IN` UNKNOWN rather than false for a
                // non-matching row, and `NOT IN` UNKNOWN for every row; that
                // is not the membership test this description promises.
                if literal == Datum::Null {
                    return None;
                }
                literals.push((literal, literal_type));
            }
            // Keep the existing integer-column description unchanged. Other
            // column families continue to fail closed in the TiPB lowering.
            if let Some((offset, column_type)) = resolve_column(expr, resolver) {
                if column_type.eval_type() != tidb_datatype::EvalType::String {
                    return Some(ScanPredicate::In {
                        column_offset: offset,
                        // A non-string column compares under `binary`, and
                        // no `COLLATE` can change that; the adoption step
                        // leaves it alone.
                        collation: column_type.collation(),
                        column_type,
                        literals: literals.into_iter().map(|(value, _)| value).collect(),
                        negated: *not,
                    });
                }
            }

            // Go `inFunctionClass.getFunction` selects `InString` from the
            // tested expression, not from whether that expression is a bare
            // column. Every list item is coerced to that same evaluation type.
            let tested = scan_operand(expr, resolver)?;
            if tested.eval_type() != tidb_datatype::EvalType::String
                || literals.iter().any(|(value, field_type)| {
                    field_type.eval_type() != tidb_datatype::EvalType::String
                        || !matches!(value, Datum::String(_) | Datum::Bytes(_))
                })
            {
                return None;
            }
            Some(ScanPredicate::ScalarIn {
                // The tested expression's own collation, which is the derived
                // one whenever no argument is explicit;
                // `adopt_refined_literals` replaces it with the built
                // expression's.
                collation: match &tested {
                    tidb_expr::pushdown_catalog::PbScalar::Column { field_type, .. } => {
                        field_type.collation()
                    }
                    _ => tidb_datatype::Collation::Utf8Mb4Bin,
                },
                tested,
                literals: literals.into_iter().map(|(value, _)| value).collect(),
                negated: *not,
            })
        }
        tidb_ast::Expr::Like {
            expr,
            pattern,
            not,
            ilike: false,
            escape,
        } => {
            let (column_offset, column_type) = resolve_column(expr, resolver)?;
            if column_type.eval_type() != tidb_datatype::EvalType::String {
                return None;
            }
            let mut pattern_expr = &**pattern;
            while let tidb_ast::Expr::Paren(inner) = pattern_expr {
                pattern_expr = inner;
            }
            let pattern = match pattern_expr {
                tidb_ast::Expr::String(pattern) | tidb_ast::Expr::RawString(pattern) => {
                    pattern.as_bytes().to_vec()
                }
                _ => return None,
            };
            let predicate = ScanPredicate::Like {
                column_offset,
                // The column's, which is the derived collation whenever no
                // argument is explicit; `adopt_refined_literals` replaces it
                // with the built expression's.
                collation: column_type.collation(),
                column_type,
                pattern,
                escape: escape.unwrap_or_else(|| resolver.like_default_escape()),
            };
            Some(if *not {
                ScanPredicate::Not(Box::new(predicate))
            } else {
                predicate
            })
        }
        tidb_ast::Expr::Between {
            expr,
            low,
            high,
            not: false,
        } => {
            let (column_offset, column_type) = resolve_column(expr, resolver)?;
            let zone = resolver.time_zone();
            let (low, low_type) = comparison_constant(low, &column_type, &zone)?;
            let (high, high_type) = comparison_constant(high, &column_type, &zone)?;
            Some(ScanPredicate::And(vec![
                ScanPredicate::Compare(ScanComparison {
                    column_offset,
                    collation: column_type.collation(),
                    column_type: column_type.clone(),
                    literal_type: low_type,
                    op: ScanComparisonOp::Ge,
                    literal: low,
                    column_on_left: true,
                }),
                ScanPredicate::Compare(ScanComparison {
                    column_offset,
                    collation: column_type.collation(),
                    column_type,
                    literal_type: high_type,
                    op: ScanComparisonOp::Le,
                    literal: high,
                    column_on_left: true,
                }),
            ]))
        }
        // A builtin call, when the push-down catalog resolves a signature TiKV
        // evaluates for it. The whole `WHERE sin(a)` conjunct is then the
        // Selection condition, evaluated for truth exactly as a `Selection`
        // above the scan would evaluate it.
        _ => scan_column_comparison(conjunct, resolver)
            .map(ScanPredicate::ColumnCompare)
            .or_else(|| scan_comparison(conjunct, resolver).map(ScanPredicate::Compare))
            .or_else(|| scan_operand_call(conjunct, resolver).map(ScanPredicate::Builtin)),
    }
}

/// One argument of a described builtin call: a column of the scanned table, an
/// already-folded integer constant, or a nested call the catalog also resolves.
///
/// Anything else -- a non-integer constant, a subquery, a call whose signature
/// TiKV does not evaluate -- makes the whole conjunct residual, which is Go's
/// own rule: `scalarFuncToPBExpr` returns nil as soon as one child does.
fn scan_operand(
    argument: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<tidb_expr::pushdown_catalog::PbScalar> {
    use tidb_expr::pushdown_catalog::PbScalar;
    if let tidb_ast::Expr::Paren(inner) = argument {
        return scan_operand(inner, resolver);
    }
    if let tidb_ast::Expr::Column(_) = argument {
        let (offset, field_type) = resolve_column(argument, resolver)?;
        return Some(PbScalar::Column { offset, field_type });
    }
    // A constant subtree first, so a folded literal argument (`MOD(a, 3 + 1)`)
    // is the constant Go would have folded rather than a `plus` call. Only an
    // integer is describable: every other constant family needs the TiPB
    // literal encoding this tier does not build.
    if let Some(Datum::Int(value)) = constant_value(argument, &resolver.time_zone()) {
        return Some(PbScalar::IntLiteral(value));
    }
    scan_operand_call(argument, resolver)
}

/// A builtin call as an operand, in either of the two spellings the parser
/// produces for one: an explicit `Expr::Func`, and the operator form real TiDB
/// also desugars to a named scalar function -- `MOD(a, b)` parses as the `%`
/// binary operator, and Go's `ScalarFunction` for it is named `mod` either way.
fn scan_operand_call(
    argument: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<tidb_expr::pushdown_catalog::PbScalar> {
    let (name, args): (String, Vec<&tidb_ast::Expr>) = match argument {
        tidb_ast::Expr::Func { name, args, .. } => {
            (name.to_ascii_lowercase(), args.iter().collect())
        }
        tidb_ast::Expr::Binary(op, lhs, rhs) => (
            tidb_expr::scalar_function::binary_op_name(*op).to_owned(),
            vec![lhs, rhs],
        ),
        _ => return None,
    };
    let operands = args
        .into_iter()
        .map(|nested| scan_operand(nested, resolver))
        .collect::<Option<Vec<_>>>()?;
    tidb_expr::pushdown_catalog::build_call(&name, operands)
}

/// Flattens an `OR` chain into its branches, in source order.
fn collect_disjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_disjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, lhs, rhs) => {
            collect_disjuncts(lhs, out);
            collect_disjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The scan-input offset and declared type of `expr`, when it is a plain
/// reference to a column of the scanned table.
fn resolve_column(
    expr: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<(u32, FieldType)> {
    match expr {
        tidb_ast::Expr::Paren(inner) => resolve_column(inner, resolver),
        tidb_ast::Expr::Column(path) => {
            let (offset, column_type, _) = resolver.resolve(path)?;
            Some((u32::try_from(offset).ok()?, column_type))
        }
        _ => None,
    }
}

/// The already-evaluated value of `expr`, when it is a constant.
///
/// A negated integer literal is folded here rather than left as the unary
/// minus the parser produced, because Go's expression rewriter folds it too
/// (`foldConstant` over a deterministic function of constants) and the
/// coprocessor is therefore sent the negative constant, not a `UnaryMinus`
/// node. Without this, `WHERE a > -1` describes nothing at all.
fn constant_value(expr: &tidb_ast::Expr, zone: &tidb_datatype::SessionTimeZone) -> Option<Datum> {
    constant_value_and_type(expr, zone).map(|(value, _)| value)
}

/// A constant expression's value and the exact type Go's expression builder
/// assigns it. Evaluating the rewritten tree also admits folded arithmetic and
/// `DATE_ADD`, rather than restricting this boundary to bare literal nodes.
fn constant_value_and_type(
    expr: &tidb_ast::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Datum, FieldType)> {
    let resolver = tidb_expr::rewriter::ZonedNoResolver::new(zone.clone());
    let rewritten = rewrite_expr_resolved(expr, &resolver).ok()?;
    let field_type = rewritten.static_type()?.clone();
    let value =
        tidb_expr::eval_expression_once(&rewritten, &tidb_expr::ZonedNoColumns(zone.clone()))
            .ok()?;
    Some((value, field_type))
}

/// One conjunct as a column-versus-constant comparison, when it is one.
fn scan_comparison(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanComparison> {
    let tidb_ast::Expr::Binary(op, lhs, rhs) = conjunct else {
        return None;
    };
    let op = ScanComparisonOp::from_ast(*op)?;
    // Go accepts the constant on either side and the protobuf preserves the
    // operand order it was written in, so the side is recorded rather than
    // normalized away.
    let (column, value, column_on_left) = match (&**lhs, &**rhs) {
        (tidb_ast::Expr::Column(path), other) => (path, other, true),
        (other, tidb_ast::Expr::Column(path)) => (path, other, false),
        _ => return None,
    };
    // A second column reference on the "constant" side leaves the shape.
    let (offset, column_type, _) = resolver.resolve(column)?;
    let zone = resolver.time_zone();
    let (literal, literal_type) = comparison_constant(value, &column_type, &zone)?;
    // A NULL constant makes the comparison unknown for every row; that is a
    // whole-predicate property Go handles in the ranger, not a filter shape.
    if literal == Datum::Null {
        return None;
    }
    Some(ScanComparison {
        column_offset: u32::try_from(offset).ok()?,
        collation: column_type.collation(),
        column_type,
        literal_type,
        op,
        literal,
        column_on_left,
    })
}

/// One conjunct as a source-ordered comparison between two scan columns.
///
/// Go's `columnToPBExpr` sends both `ColumnRef` children when the comparison
/// is supported by TiKV. The TiPB lowering applies the type-family gate;
/// refusing there keeps a comparison local when the two declared types need a
/// coercion this tier does not yet model.
fn scan_column_comparison(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanColumnComparison> {
    let tidb_ast::Expr::Binary(op, lhs, rhs) = conjunct else {
        return None;
    };
    let op = ScanComparisonOp::from_ast(*op)?;
    let (tidb_ast::Expr::Column(left), tidb_ast::Expr::Column(right)) = (&**lhs, &**rhs) else {
        return None;
    };
    let (left_offset, left_type, _) = resolver.resolve(left)?;
    let (right_offset, right_type, _) = resolver.resolve(right)?;
    Some(ScanColumnComparison {
        left_offset: u32::try_from(left_offset).ok()?,
        left_type,
        right_offset: u32::try_from(right_offset).ok()?,
        right_type,
        op,
    })
}

fn comparison_constant(
    value: &tidb_ast::Expr,
    column_type: &FieldType,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Datum, FieldType)> {
    let (mut literal, mut literal_type) = constant_value_and_type(value, zone)?;
    if column_type.code() == FieldTypeCode::NewDecimal
        && literal_type.eval_type() == tidb_datatype::EvalType::Int
    {
        // `GetAccurateCmpType` selects ETDecimal for DECIMAL versus INT, and
        // `WrapWithCastAsDecimal` folds a constant cast. Its final type is
        // refined from the resulting MyDecimal's own precision and scale.
        let decimal = match literal {
            Datum::Int(value) => tidb_datatype::Decimal::from_int(value),
            Datum::UInt(value) => tidb_datatype::Decimal::from_uint(value),
            _ => return None,
        };
        let (precision, scale) = decimal.precision_and_frac();
        literal_type = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flags(literal_type.flags())
            .with_flen(i64::from(precision))
            .with_decimal(i64::from(scale));
        literal = Datum::Decimal(decimal);
    }
    if matches!(
        column_type.code(),
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
    ) && literal_type.eval_type() == tidb_datatype::EvalType::String
    {
        // `GetAccurateCmpType` selects ETDatetime for a temporal column
        // compared with a string constant. `WrapWithCastAsTime` then builds
        // DATETIME(26,6), and constant folding leaves a MysqlTime literal.
        let target = FieldType::new(FieldTypeCode::Datetime)
            .with_flen(26)
            .with_decimal(tidb_datatype::MAX_FSP)
            .with_added_flags(tidb_datatype::FieldTypeFlags::BINARY);
        let converted = literal
            .convert_to_in(&target, tidb_datatype::DEFAULT_STATEMENT_FLAGS, &zone)
            .ok()?;
        if converted.event.is_some() {
            return None;
        }
        literal = converted.value;
        literal_type = target;
    }
    Some((literal, literal_type))
}

/// Flattens an `AND` chain into its conjuncts.
fn collect_conjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_conjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) => {
            collect_conjuncts(lhs, out);
            collect_conjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The single TiKV-backed table a `FROM` names, when it names exactly one.
/// A point get applies only to that shape (Go `getSingleTableNameAndAlias`).
/// The one plain table a `FROM` names, when it names exactly one.
///
/// Split out of [`single_kv_table`] because the access-path decision needs the
/// REFERENCE, not just the table it resolves to: the `USE`/`FORCE`/`IGNORE
/// INDEX` hints that decide which paths exist live on the reference.
pub(crate) fn single_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let table_ref = sole_table_ref(from)?;
    // A `PARTITION (...)` restriction is refused by `build_from`; declining
    // the fast path here too keeps a point get from answering a statement the
    // scan would have rejected.
    if !table_ref.partitions.is_empty() {
        return None;
    }
    Some(table_ref)
}

/// [`single_table_ref`] WITHOUT its fast-path refusal: the one table a `FROM`
/// names, whether or not the statement narrowed it with `PARTITION (...)`.
///
/// The refusal above is about which ACCESS PATHS may be chosen. Callers that
/// only want to know which table -- and which partitions of it -- the
/// statement reads want this one, so that a `PARTITION (p)` narrowing is
/// reported rather than silently read as "no single table".
pub(crate) fn sole_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let join = from.as_ref()?;
    if join.right.is_some() {
        return None;
    }
    let JoinNode::Table(table_ref) = &join.left else {
        return None;
    };
    Some(table_ref)
}

/// [`single_kv_table`] over [`sole_table_ref`]: the stored table a `FROM`
/// names even when a `PARTITION (...)` list narrowed it.
pub(crate) fn sole_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let table_ref = sole_table_ref(from)?;
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    }
}

pub(crate) fn single_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let table_ref = single_table_ref(from)?;
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        // A view stores no rows, so there is no point get to try.
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    }
}

/// Go `tryWhereIn2BatchPointGet`: a single-table `SELECT` whose whole `WHERE`
/// is `column IN (constants)` over the handle or a single-column unique index
/// reads those rows directly instead of scanning.
///
/// Go rejects the fast plan when `ORDER BY`, `GROUP BY`, `LIMIT`, `HAVING`,
/// `DISTINCT` or a window spec is present, when the `IN` is negated, and when
/// its list is empty. The handle path applies when the table's primary key IS
/// the handle and the column names it; otherwise a unique index whose only
/// column it is.
///
/// The row form, `(a, b) IN ((1, 2), (3, 4))`, is a composite-key
/// `Batch_Point_Get` when the tuples pin every column of a unique index or a
/// clustered common handle.
pub(crate) struct BatchPointLookup {
    handles: Vec<TableHandle>,
    index: Option<(i64, String)>,
    common_handle: bool,
    plan_rows: usize,
}

impl BatchPointLookup {
    fn handle(handles: Vec<TableHandle>, plan_rows: usize) -> Self {
        Self {
            handles,
            index: None,
            common_handle: false,
            plan_rows,
        }
    }

    fn common_handle(handles: Vec<TableHandle>, plan_rows: usize) -> Self {
        Self {
            handles,
            index: None,
            common_handle: true,
            plan_rows,
        }
    }

    fn index(
        handles: Vec<TableHandle>,
        plan_rows: usize,
        table: &KvTable,
        columns: &[(String, FieldType)],
        index: &crate::kv_table::KvIndex,
    ) -> Self {
        let index_columns = index
            .column_offsets
            .iter()
            .map(|offset| {
                columns.get(*offset).map_or_else(
                    || index_key_part_name(table, *offset),
                    |column| column.0.clone(),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            handles,
            index: Some((index.id, format!("index:{}({index_columns})", index.name))),
            common_handle: false,
            plan_rows,
        }
    }

    fn ignores_hints(&self) -> bool {
        self.index.is_none() && !self.common_handle
    }

    fn allowed_by(&self, hints: &crate::index_hints::AvailablePaths) -> bool {
        match &self.index {
            Some((index_id, _)) => hints.allows_index(*index_id),
            None if self.common_handle => hints.allows_common_primary(),
            None => true,
        }
    }

    pub(crate) fn into_handles(self) -> Vec<TableHandle> {
        self.handles
    }
}

pub(crate) fn try_batch_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<BatchPointLookup>, DriverError> {
    let stmt = PointPlanStmt::of_select(select);
    try_batch_point_get_stmt(&stmt, table, columns, zone)
}

pub(crate) fn try_batch_point_get_stmt(
    select: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<BatchPointLookup>, DriverError> {
    if select.having.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.limit.is_some()
        || select.distinct
    {
        return Ok(None);
    }
    let Some(where_clause) = select.where_clause else {
        return Ok(None);
    };
    // The WHERE must be exactly the IN, as Go requires a PatternInExpr.
    let tidb_ast::Expr::In { expr, list, not } = where_clause else {
        return Ok(None);
    };
    if *not || list.is_empty() {
        return Ok(None);
    }
    // The row form is Go's composite-key Batch_Point_Get. Each tuple value
    // is converted into the indexed column's domain before the key lookup;
    // any value that cannot round-trip exactly declines the fast path and
    // leaves the ordinary scan to preserve the written predicate's answer.
    if let tidb_ast::Expr::Row(left) = &**expr {
        let mut names = Vec::with_capacity(left.len());
        for column in left {
            let tidb_ast::Expr::Column(path) = column else {
                return Ok(None);
            };
            let Some(name) = path.last() else {
                return Ok(None);
            };
            names.push(name);
        }
        let mut table = table.clone();
        // A clustered composite primary key is represented by the common
        // handle offsets, not by a KvIndex. Its encoded datum key is the
        // record handle itself, so it can use the same direct lookup source as
        // a unique index without manufacturing a redundant index entry.
        let common_offsets = table.common_handle_offsets().to_vec();
        if common_offsets.len() == names.len() {
            let mut positions = Vec::with_capacity(common_offsets.len());
            for offset in &common_offsets {
                let Some((column_name, _)) = columns.get(*offset) else {
                    positions.clear();
                    break;
                };
                let Some(position) = names
                    .iter()
                    .position(|name| column_name.eq_ignore_ascii_case(name))
                else {
                    positions.clear();
                    break;
                };
                positions.push(position);
            }
            if positions.len() == common_offsets.len() {
                let mut handles = Vec::with_capacity(list.len());
                for candidate in list {
                    let tidb_ast::Expr::Row(values) = candidate else {
                        return Ok(None);
                    };
                    if values.len() != left.len() {
                        return Ok(None);
                    }
                    let mut key_values = Vec::with_capacity(common_offsets.len());
                    for (offset, position) in common_offsets.iter().zip(&positions) {
                        let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
                            &values[*position],
                            &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                        ) else {
                            return Ok(None);
                        };
                        let Ok(value) = constant.eval() else {
                            return Ok(None);
                        };
                        let Some(value) = point_get_value(&columns[*offset].1, &value) else {
                            return Ok(None);
                        };
                        key_values.push(value);
                    }
                    let encoded =
                        tidb_codec::encode_key_in_timezone(zone, &key_values).map_err(|e| {
                            DriverError::Parse(format!("common handle encode failed: {e:?}"))
                        })?;
                    let handle = tidb_txnkv::CommonHandle::new(encoded).map_err(|e| {
                        DriverError::Parse(format!("common handle build failed: {e:?}"))
                    })?;
                    let handle = TableHandle::Common(handle.encoded().to_vec());
                    if !handles.contains(&handle) {
                        handles.push(handle);
                    }
                }
                return Ok(Some(BatchPointLookup::common_handle(handles, list.len())));
            }
        }
        for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
            if !index.unique || index.has_prefix() || index.column_offsets.len() != names.len() {
                continue;
            }
            let mut positions = Vec::with_capacity(index.column_offsets.len());
            for offset in &index.column_offsets {
                let Some((column_name, _)) = columns.get(*offset) else {
                    positions.clear();
                    break;
                };
                let Some(position) = names
                    .iter()
                    .position(|name| column_name.eq_ignore_ascii_case(name))
                else {
                    positions.clear();
                    break;
                };
                positions.push(position);
            }
            if positions.len() != index.column_offsets.len() {
                continue;
            }
            let mut handles = Vec::with_capacity(list.len());
            for candidate in list {
                let tidb_ast::Expr::Row(values) = candidate else {
                    return Ok(None);
                };
                if values.len() != left.len() {
                    return Ok(None);
                }
                let mut key_values = Vec::with_capacity(index.column_offsets.len());
                for (offset, position) in index.column_offsets.iter().zip(&positions) {
                    let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
                        &values[*position],
                        &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                    ) else {
                        return Ok(None);
                    };
                    let Ok(value) = constant.eval() else {
                        return Ok(None);
                    };
                    let Some(value) = point_get_value(&columns[*offset].1, &value) else {
                        return Ok(None);
                    };
                    key_values.push(value);
                }
                if let Some(handle) = table
                    .lookup_unique(index.id, &key_values, zone)
                    .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
                {
                    if !handles.contains(&handle) {
                        handles.push(handle);
                    }
                }
            }
            return Ok(Some(BatchPointLookup::index(
                handles,
                list.len(),
                &table,
                columns,
                &index,
            )));
        }
        return Ok(None);
    }
    let tidb_ast::Expr::Column(path) = &**expr else {
        return Ok(None);
    };
    let Some(name) = path.last() else {
        return Ok(None);
    };

    // Every list element must be a constant, or this is not a point plan.
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
            item,
            &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
        ) else {
            return Ok(None);
        };
        let Ok(value) = constant.eval() else {
            return Ok(None);
        };
        values.push(value);
    }

    // The handle path.
    if let Some(offset) = table.pk_handle_offset() {
        if columns[offset].0.eq_ignore_ascii_case(name) {
            // Go `newBatchPointGetPlan` runs every list element through
            // `getPointGetValue` and returns `nil` -- no batch plan at all --
            // as soon as one of them is not exactly representable, so a list
            // mixing `1.0` with `1.5` still answers from a scan rather than
            // silently dropping the element it cannot key.
            let mut handles = Vec::with_capacity(values.len());
            for value in &values {
                match point_get_value(&columns[offset].1, value) {
                    Some(Datum::Int(v)) => {
                        let handle = TableHandle::Int(v);
                        if !handles.contains(&handle) {
                            handles.push(handle);
                        }
                    }
                    Some(Datum::UInt(v)) => {
                        let handle = TableHandle::Int(v as i64);
                        if !handles.contains(&handle) {
                            handles.push(handle);
                        }
                    }
                    _ => return Ok(None),
                }
            }
            return Ok(Some(BatchPointLookup::handle(handles, list.len())));
        }
    }

    // The unique-index path.
    let mut table = table.clone();
    for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
        if !index.unique || index.column_offsets.len() != 1 {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        // Resolved through `get` for the same reason the single point get
        // does: an EXPRESSION key part's hidden generated column sits past
        // the end of the scope's visible columns, and no `IN` list names it.
        let Some((index_column, field_type)) = columns.get(index.column_offsets[0]) else {
            continue;
        };
        if !index_column.eq_ignore_ascii_case(name) {
            continue;
        }
        let mut converted = Vec::with_capacity(values.len());
        for value in &values {
            let Some(value) = point_get_value(field_type, value) else {
                return Ok(None);
            };
            converted.push(value);
        }
        let values = converted;
        let mut handles = Vec::new();
        for value in &values {
            if let Some(handle) = table
                .lookup_unique(index.id, std::slice::from_ref(value), zone)
                .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
            {
                if !handles.contains(&handle) {
                    handles.push(handle);
                }
            }
        }
        return Ok(Some(BatchPointLookup::index(
            handles,
            list.len(),
            &table,
            columns,
            &index,
        )));
    }
    Ok(None)
}

/// Moves every pair's constant into its column's domain, in place.
///
/// Returns false when any pair names an unknown column or holds a constant
/// the column cannot represent exactly, which is Go's "no point plan; let the
/// scan decide" answer.
pub(crate) fn convert_pairs_to_column_domain(
    pairs: &mut [NameValuePair],
    columns: &[(String, FieldType)],
) -> bool {
    for pair in pairs {
        let Some((_, field_type)) = columns
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&pair.column))
        else {
            return false;
        };
        let Some(value) = point_get_value(field_type, &pair.value) else {
            return false;
        };
        pair.value = value;
    }
    true
}

/// One `column = constant` equality from a `WHERE`, Go's `nameValuePair`.
pub(crate) struct NameValuePair {
    column: String,
    value: Datum,
}

impl NameValuePair {
    /// The column this equality pinned.
    pub(crate) fn column(&self) -> &str {
        &self.column
    }

    /// The value it pinned the column to, already moved into that column's
    /// domain by [`convert_pairs_to_column_domain`].
    pub(crate) const fn value(&self) -> &Datum {
        &self.value
    }
}

/// Go `getNameValuePairs`: flattens a `WHERE` that is a conjunction of
/// `column = constant` equalities into pairs, returning `None` for any other
/// shape.
///
/// Go accepts the constant on either side of the `=`, and recurses only
/// through `AND`; anything else (an `OR`, a comparison, a function call)
/// makes the statement ineligible for a point get, which is what returning
/// `None` means here.
pub(crate) fn name_value_pairs(
    expr: &tidb_ast::Expr,
    pairs: &mut Vec<NameValuePair>,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    use tidb_ast::{BinaryOp, Expr};
    fn unparenthesized(expr: &Expr) -> &Expr {
        match expr {
            Expr::Paren(inner) => unparenthesized(inner),
            other => other,
        }
    }
    match expr {
        Expr::Paren(inner) => name_value_pairs(inner, pairs, zone),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            name_value_pairs(lhs, pairs, zone) && name_value_pairs(rhs, pairs, zone)
        }
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
            // Stripped on both sides for the reason the arm above recurses
            // through `Expr::Paren`: parentheses are syntax, and Go has
            // unwrapped them before a point-get key is ever looked for, so
            // `(a)=1` names the same key that `a=1` does.
            let (column, value) = match (unparenthesized(lhs), unparenthesized(rhs)) {
                (Expr::Column(path), other) => (path, other),
                (other, Expr::Column(path)) => (path, other),
                _ => return false,
            };
            let Some(name) = column.last() else {
                return false;
            };
            // Only a literal qualifies; anything needing evaluation against a
            // row is not a point-get key.
            let Ok(value) = rewrite_expr_resolved(
                value,
                &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
            ) else {
                return false;
            };
            let Expression::Constant(constant) = value else {
                return false;
            };
            let Ok(value) = constant.eval() else {
                return false;
            };
            pairs.push(NameValuePair {
                column: name.clone(),
                value,
            });
            true
        }
        _ => false,
    }
}

/// The clauses [`try_point_get`] decides a point plan from.
///
/// This exists because Go decides a WRITE's point plan from the SAME
/// function as a read's: `tryUpdatePointPlan`/`tryDeletePointPlan`
/// (`pkg/planner/core/point_get_plan.go`) build an `ast.SelectStmt` out of
/// the write's own `TableRefs`/`Where`/`Order`/`Limit` and hand it to
/// `tryPointGetPlan`. This struct IS that synthesis, expressed as the field
/// copy Go performs rather than as a second point-plan builder -- there is
/// one rule here and one implementation of it, and a write cannot drift from
/// a read about which statements are point plans.
pub(crate) struct PointPlanStmt<'a> {
    where_clause: Option<&'a tidb_ast::Expr>,
    order_by: &'a [tidb_ast::OrderItem],
    limit: Option<&'a tidb_ast::Limit>,
    /// Go's synthesized statement carries no select list, so it has neither
    /// of these; only a real `SELECT` can.
    having: Option<&'a tidb_ast::Expr>,
    group_by: &'a [tidb_ast::GroupByItem],
    /// `DISTINCT` is present only on a real `SELECT`; writes set this false.
    distinct: bool,
    /// Go's `DataSource.PartitionNames`: the statement's own
    /// `PARTITION (p, ...)` list, EXACTLY as written and before any pruning.
    ///
    /// It is a point-plan input rather than a read restriction because
    /// `find_best_task.go`'s point-get conversion tests its LENGTH: "Partition
    /// table can't use `_tidb_rowid` to generate PointGet Plan unless one
    /// partition is explicitly specified" -- `len(ds.PartitionNames) != 1`
    /// disables the conversion. What the `WHERE` happened to prune is NOT
    /// that list, which is why the restricted `KvTable` cannot answer this.
    named_partitions: &'a [String],
}

impl<'a> PointPlanStmt<'a> {
    /// A `SELECT`'s own clauses.
    pub(crate) fn of_select(select: &'a tidb_ast::SelectStmt) -> Self {
        PointPlanStmt {
            where_clause: select.where_clause.as_ref(),
            order_by: &select.order_by,
            limit: select.limit.as_ref(),
            having: select.having.as_ref(),
            group_by: &select.group_by,
            distinct: select.distinct,
            named_partitions: sole_table_ref(&select.from)
                .map_or(&[][..], |table_ref| table_ref.partitions.as_slice()),
        }
    }

    /// Go's synthesized `ast.SelectStmt` for a single-table write: the three
    /// clauses `tryUpdatePointPlan`/`tryDeletePointPlan` copy across, and
    /// nothing else.
    pub(crate) fn of_write(
        where_clause: Option<&'a tidb_ast::Expr>,
        order_by: &'a [tidb_ast::OrderItem],
        limit: Option<&'a tidb_ast::Limit>,
    ) -> Self {
        PointPlanStmt {
            where_clause,
            order_by,
            limit,
            having: None,
            group_by: &[],
            distinct: false,
            // Go's synthesized statement for a write copies `TableRefs`, but
            // the `_tidb_rowid` exception below belongs to the READ planner's
            // `convertToPointGet`; `tryPointGetPlan` -- the only rule a write
            // goes through -- refuses `_tidb_rowid` on a partitioned table
            // outright (`point_get_plan.go`: "Partition table can't use
            // `_tidb_rowid` to generate PointGet Plan"). An empty list is that
            // refusal.
            named_partitions: &[],
        }
    }
}

/// The row a point get reads, when the statement qualifies for one.
///
/// Go `TryFastPlan`/`tryPointGetPlan`: a single-table statement with no
/// `HAVING` and no `ORDER BY`, whose `WHERE` is a conjunction of equalities
/// that pins either the handle or every column of a unique index, reads one
/// row directly instead of scanning. `LIMIT` is allowed only when it cannot
/// remove the row (`count > 0` and `offset == 0`), matching Go's check.
///
/// Returns `Ok(None)` when the statement does not qualify, so the caller
/// falls back to the ordinary scan.
/// What pinned a point get: Go `PointGetPlan`'s split between a HANDLE plan
/// and an INDEX plan, which is exactly what its `AccessObject` prints --
/// `table:t handle:N` for the first, `table:t, index:idx(cols)` for the
/// second, never both.
#[derive(Clone, Debug)]
pub(crate) struct PointGetPin {
    /// The resolved record handle (`None` = a key no row can carry; Go still
    /// plans the `Point_Get` and reads nothing).
    pub(crate) handle: Option<TableHandle>,
    /// The UNIQUE INDEX that pinned the row, when one did: its name and its
    /// column names, in index order. `None` is the handle pin.
    pub(crate) index: Option<(String, Vec<String>)>,
}

pub(crate) fn try_point_get(
    select: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<PointGetPin>, DriverError> {
    if select.having.is_some() || !select.order_by.is_empty() || !select.group_by.is_empty() {
        return Ok(None);
    }
    if let Some(limit) = select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        if count == 0 || offset > 0 {
            return Ok(None);
        }
    }
    let Some(where_clause) = select.where_clause else {
        return Ok(None);
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs, zone) || pairs.is_empty() {
        return Ok(None);
    }
    // Go `findPKHandle`'s `!tblInfo.PKIsHandle` branch: when the table has no
    // primary-key handle, the pair naming `_tidb_rowid` IS the handle pair,
    // and its type is `TypeLonglong` rather than any stored column's -- which
    // is why this runs BEFORE the column-domain conversion below, where
    // `_tidb_rowid` names nothing to convert against.
    //
    // Go refuses it for a PARTITIONED table (`point_get_plan.go`: "Partition
    // table can't use `_tidb_rowid` to generate PointGet Plan"), because a row
    // id alone does not say which partition holds the row -- UNLESS the
    // statement said which one. `find_best_task.go`'s point-get conversion
    // carries the exception verbatim:
    //
    // ```go
    // // Partition table can't use `_tidb_rowid` to generate PointGet Plan
    // // unless one partition is explicitly specified.
    // if canConvertPointGet && path.IsIntHandlePath &&
    //     !ds.Table.Meta().PKIsHandle && len(ds.PartitionNames) != 1 {
    //     canConvertPointGet = false
    // }
    // ```
    //
    // The test is on the WRITTEN list's length, not on how many partitions
    // survived: TiDB's own recording gives `Point_Get table:t, partition:p0`
    // for `select *,_tidb_rowid from t partition(p0) where _tidb_rowid=1`,
    // and a `TableRangeScan` for both the bare form and `partition(p0,p1)`.
    let single_named_partition = select.named_partitions.len() == 1;
    if table.pk_handle_offset().is_none()
        && table.common_handle_offsets().is_empty()
        && (table.partition().is_none() || single_named_partition)
        && pairs.len() == 1
        && pairs[0]
            .column
            .eq_ignore_ascii_case(crate::driver::leaf_demand::EXTRA_HANDLE_NAME)
    {
        let handle_type = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let Some(value) = point_get_value(&handle_type, &pairs[0].value) else {
            return Ok(None);
        };
        return Ok(Some(PointGetPin {
            handle: match value {
                Datum::Int(value) => Some(TableHandle::Int(value)),
                Datum::UInt(value) => Some(TableHandle::Int(value as i64)),
                _ => return Ok(None),
            },
            index: None,
        }));
    }

    // Go `getNameValuePairs` moves every constant into its column's domain
    // before the pair is usable as a key, and abandons the whole point plan
    // when one of them will not survive the round trip. Doing it here, once
    // for every pair, is what keeps the handle arm below dealing only in
    // integers and the unique-index arm dealing only in column-typed values.
    if !convert_pairs_to_column_domain(&mut pairs, columns) {
        return Ok(None);
    }

    // The handle path: the primary key pinned by exactly one equality, which
    // is Go's `len(pairs) == 1` condition on the handle pair.
    if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if pairs.len() == 1 && pairs[0].column.eq_ignore_ascii_case(handle_column) {
            return Ok(Some(PointGetPin {
                handle: match &pairs[0].value {
                    Datum::Int(value) => Some(TableHandle::Int(*value)),
                    Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                    // Unreachable: the conversion above has already put the
                    // value in the handle column's integer domain or refused
                    // the plan.
                    _ => return Ok(None),
                },
                index: None,
            }));
        }
        // Go's `else if handlePair.value.Kind() != KindNull { return nil }`:
        // once a HANDLE pair exists among the conjuncts, the unique-index arm
        // is never tried -- the fast point plan is refused outright and the
        // ordinary planner takes over, whose `convertToPointGet` prints the
        // bare handle plan (`Point_Get table:t`) with the extra conjunct as a
        // filter. Falling through to the unique index here instead printed
        // `index:i(i, j)` where TiDB's recorded plan names no index.
        if pairs
            .iter()
            .any(|pair| pair.column.eq_ignore_ascii_case(handle_column))
        {
            return Ok(None);
        }
    }

    // A clustered composite primary key is encoded directly as a common
    // handle rather than materialized as a secondary `KvIndex`. When every
    // handle column is pinned, it is the same one-row lookup as an integer
    // point get; extra equalities remain in the filter above the source.
    let common_offsets = table.common_handle_offsets().to_vec();
    if !common_offsets.is_empty() {
        let mut values = Vec::with_capacity(common_offsets.len());
        for offset in common_offsets {
            let Some((name, _)) = columns.get(offset) else {
                values.clear();
                break;
            };
            let Some(pair) = pairs
                .iter()
                .find(|pair| pair.column.eq_ignore_ascii_case(name))
            else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() == table.common_handle_offsets().len() {
            let encoded = tidb_codec::encode_key_in_timezone(zone, &values)
                .map_err(|e| DriverError::Parse(format!("common handle encode failed: {e:?}")))?;
            let handle = tidb_txnkv::CommonHandle::new(encoded)
                .map_err(|e| DriverError::Parse(format!("common handle build failed: {e:?}")))?;
            // A clustered common handle IS the record key, so it prints as
            // a handle plan, not an index one.
            return Ok(Some(PointGetPin {
                handle: Some(TableHandle::Common(handle.encoded().to_vec())),
                index: None,
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    let mut table = table.clone();
    for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
        if !index.unique {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        let mut values = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            // Go `getIndexValues` resolves each key part by NAME against the
            // `WHERE`'s pairs, so a key part the statement cannot name
            // declines the whole index. The hidden generated column an
            // EXPRESSION key part was rewritten into is exactly such a part:
            // it lives past the end of the scope's visible column list, and
            // `tidb_shard(a)` is not a name any `WHERE` writes. Resolving the
            // name through `get` makes "no visible column at that offset" and
            // "not pinned by the WHERE" the same answer -- without it the
            // offset indexes past the end and panics, which is what
            // `explain_shard_index`'s `where a=100` reached.
            let Some(pair) = columns.get(*offset).and_then(|(name, _)| {
                pairs
                    .iter()
                    .find(|pair| pair.column.eq_ignore_ascii_case(name))
            }) else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() != index.column_offsets.len() {
            continue;
        }
        let handle = table
            .lookup_unique(index.id, &values, zone)
            .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?;
        // Go `PointGetPlan.AccessObject` prints the pinning index --
        // `table:t, index:idx(cols)` -- and no handle, even though execution
        // resolved one through the index entry.
        let index_columns = index
            .column_offsets
            .iter()
            .filter_map(|offset| table.columns.get(*offset))
            .map(|column| column.name.clone())
            .collect();
        return Ok(Some(PointGetPin {
            handle,
            index: Some((index.name.clone(), index_columns)),
        }));
    }
    Ok(None)
}

/// The row order a committed access path produces, for the `ORDER BY` half of
/// the `LIMIT` push-down rule.
pub(crate) struct IndexAccessOrder {
    /// The unfixed key columns as offsets into the source row, in key order.
    column_offsets: Vec<usize>,
    /// Whether one range covers the access path. Several ranges are each
    /// internally in index order but are walked one after another, and their
    /// concatenation is not index order, so only a single range establishes
    /// the total order an `ORDER BY` can be discharged against.
    single_range: bool,
    /// Key positions fixed to one value by the single range. Go carries the
    /// equivalent fact in `AccessPath.ConstCols` and skips those positions in
    /// `matchProperty` Case 2.
    constant_positions: Vec<bool>,
    /// The table's handle column offsets. An index entry stores them in its
    /// key suffix regardless of the indexed columns, so a residual filter may
    /// read them straight off the index source; they are NOT part of the
    /// order claim.
    handle_covered_offsets: Vec<usize>,
}

impl IndexAccessOrder {
    fn from_ranges(column_offsets: &[usize], ranges: &[IndexRange]) -> Self {
        // Go `matchProperty` Case 2: a key part fixed to one value does not
        // participate in the varying row order, so ORDER BY may start after
        // it. Case 3 (different point values in several ranges) requires a
        // merge-sort operator this tier does not have and remains excluded by
        // `single_range`.
        let constant_positions = if let [range] = ranges {
            column_offsets
                .iter()
                .enumerate()
                .map(|(position, _)| {
                    range
                        .low
                        .get(position)
                        .zip(range.high.get(position))
                        .is_some_and(|(low, high)| low == high)
                })
                .collect()
        } else {
            Vec::new()
        };
        Self {
            column_offsets: column_offsets.to_vec(),
            single_range: ranges.len() == 1,
            constant_positions,
            handle_covered_offsets: Vec::new(),
        }
    }

    /// Maps the delivered key order through a compact scan projection. As in
    /// Go's projection property propagation, only the consecutive surviving
    /// prefix remains an order promise.
    pub(crate) fn remap_columns(&mut self, keep: &[usize]) {
        let remapped: Vec<_> = self
            .column_offsets
            .iter()
            .enumerate()
            .map_while(|(position, offset)| {
                keep.iter().position(|kept| kept == offset).map(|mapped| {
                    (
                        mapped,
                        self.constant_positions
                            .get(position)
                            .copied()
                            .unwrap_or(false),
                    )
                })
            })
            .collect();
        (self.column_offsets, self.constant_positions) = remapped.into_iter().unzip();
        // The handle rides in every index KEY suffix, not in the projected
        // row shape, so pruning the scan projection cannot take it away.
    }

    /// Whether a residual predicate can run against the index entry before
    /// the handle stream is capped. Every referenced column must be present
    /// in the chosen index, matching Go's Build-side Selection rule.
    pub(crate) fn residual_uses_only_index(
        &self,
        predicate: &tidb_ast::Expr,
        resolver: &ScopeResolver<'_>,
    ) -> bool {
        crate::column_prune::expr_column_offsets(predicate, resolver).is_some_and(|offsets| {
            offsets.iter().all(|offset| {
                self.column_offsets.contains(offset)
                    || self.handle_covered_offsets.contains(offset)
            })
        })
    }
}

/// The order a single clustered-handle range still provides after its equal
/// leading columns have been fixed by the range.
///
/// A common handle `(w_id, d_id, o_id)` is the record key itself. The range
/// `[1 1, 1 1]` therefore walks in `o_id` order, which is the TPC-C Delivery
/// `ORDER BY o_id LIMIT 1` property. More than one range is declined for the
/// same conservative reason as an index path: each range is ordered, but this
/// layer does not promise that their concatenation is one total order.
/// The order claim of a WHOLE-table scan: the clustered int handle, over the
/// one unbounded range the scan is.
///
/// Go's `matchProperty` makes this claim without looking at the handle's
/// SIGNEDNESS (`find_best_task.go:1084`, the `path.IsIntHandlePath` arm): an
/// unsigned handle above `i64::MAX` is stored under a negative record key and
/// walks FIRST, but the table reader cuts the domain at that point and reads
/// the two halves in value order (`table_reader.go:295`). This tier makes the
/// same cut, in `KvTable::record_key_ranges`, so the claim holds here for the
/// same reason.
///
/// A COMMON handle is still refused: its key order matches its datum order
/// only for the column families the ranger admits, which the range path
/// proves per statement; the whole-table claim has no such proof.
fn full_table_handle_order(
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Option<IndexAccessOrder> {
    if !table.common_handle_offsets().is_empty() {
        return None;
    }
    if let Some(offset) = table.pk_handle_offset() {
        table.columns.get(offset)?;
        return Some(IndexAccessOrder::from_ranges(
            &[offset],
            &[IndexRange::full()],
        ));
    }
    // A HEAP table's handle IS `_tidb_rowid`: Go's `matchProperty` makes the
    // same `path.IsIntHandlePath` claim through `ds.HandleCols`, which
    // `buildDataSource` built from `NewExtraHandleSchemaCol` for such a
    // table. The extra handle is the one column the scope appends past the
    // table's own, so it is found in the scope's list by name; a scope that
    // does not carry it has no order the statement could name.
    let offset = extra_handle_scope_offset(columns)?;
    Some(IndexAccessOrder::from_ranges(
        &[offset],
        &[IndexRange::full()],
    ))
}

/// The scope offset carrying `_tidb_rowid`, when the scope carries it at all.
fn extra_handle_scope_offset(columns: &[(String, FieldType)]) -> Option<usize> {
    columns.iter().position(|(name, _)| {
        name.eq_ignore_ascii_case(crate::driver::leaf_demand::EXTRA_HANDLE_NAME)
    })
}

fn handle_range_order(
    table: &KvTable,
    columns: &[(String, FieldType)],
    ranges: &[IndexRange],
) -> Option<IndexAccessOrder> {
    let [range] = ranges else {
        return None;
    };
    let handle_columns: Vec<usize> = if !table.common_handle_offsets().is_empty() {
        table.common_handle_offsets().to_vec()
    } else if let Some(offset) = table.pk_handle_offset() {
        vec![offset]
    } else {
        // The heap table's `_tidb_rowid` arm of [`full_table_handle_order`],
        // over a ranger-narrowed walk of the same record keys.
        extra_handle_scope_offset(columns).into_iter().collect()
    };
    let fixed_prefix = range
        .low
        .iter()
        .zip(&range.high)
        .take_while(|(low, high)| low == high)
        .count()
        .min(handle_columns.len());
    (fixed_prefix < handle_columns.len())
        .then(|| IndexAccessOrder::from_ranges(&handle_columns, ranges))
}

/// The row cap a `LIMIT` may push into the source, or `None` to leave all the
/// work to the `LimitExec`.
///
/// Go pushes a `Limit` into the cop task below the scan, and a `TopN` when an
/// order has to be established first. Captured from TiDB (mock store,
/// `pkg/executor/zz_dump_limit_test.go`):
///
/// ```text
/// select a, b from t where b > 4 limit 3
///   Limit_8            root       offset:0, count:3
///   └─IndexReader_13   root       index:Limit_12
///     └─Limit_12       cop[tikv]  offset:0, count:3
///       └─IndexRangeScan_11  cop[tikv]  range:(4,+inf], keep order:false
///
/// select a, b from t where b > 4 order by b limit 2, 3
///   Limit_13           root       offset:2, count:3
///     └─Limit_22       cop[tikv]  offset:0, count:5      <- cap is offset+count
///       └─IndexRangeScan_21  cop[tikv]  range:(4,+inf], keep order:true
///
/// select a, b from t order by c limit 3                  <- NOT pushed
///   TopN_8             root       test.t.c, offset:0, count:3
///     └─TopN_17        cop[tikv]  test.t.c, offset:0, count:3
///       └─TableFullScan_16  cop[tikv]  keep order:false  <- reads all 20 rows
///
/// select a, b from t where c > 4 order by b limit 3      <- NOT pushed
///   TopN_8             root       test.t.b, offset:0, count:3
///     └─TopN_18        cop[tikv]  test.t.b, offset:0, count:3
///       └─Selection_17 cop[tikv]  gt(test.t.c, 4)        <- filter below the TopN
/// ```
///
/// # The rule
///
/// The cap is `offset + count`, because the offset rows are dropped above and
/// must still be produced -- Go's cop-side `Limit` carries exactly that
/// (`limit 2, 3` lowers to `offset:0, count:5`).
///
/// A cap is only sound when every row the source emits reaches the `LIMIT`,
/// in the order the `LIMIT` selects from. So it is refused when anything
/// between them can drop rows (a residual `Selection`, `DISTINCT`, `HAVING`),
/// or add them (a window function's materialize-and-append), and when the
/// query has an `ORDER BY` the access path does not already satisfy -- a sort
/// must see every row before it can name the first one, which is why Go turns
/// that case into a `TopN` and leaves the scan reading everything.
///
/// An `ORDER BY` is satisfied when the source is a single index range and the
/// by-items are a prefix of that index's columns, all ascending: the storage
/// iterator walks encoded index keys in ascending order, and the codec's
/// order is the collation order the sort would have used (NULLs lowest, as
/// `ORDER BY ... ASC` puts them first).
///
/// # Divergence from Go
///
/// Go decides this in the planner and *prints* it (`Limit` inside `cop[tikv]`,
/// or `keep order:true` on the scan). This tier has no cop task or
/// `TableReader` in its plan text, so the push-down changes only what runs:
/// the printed plan keeps the `Limit`-over-scan shape either way, and the
/// truncation shows up in `EXPLAIN ANALYZE`'s `actRows` instead.
fn scan_limit_cap(
    select: &tidb_ast::SelectStmt,
    residual_where: Option<&tidb_ast::Expr>,
    index_order: Option<&IndexAccessOrder>,
    resolver: &ScopeResolver<'_>,
) -> Option<u64> {
    let limit = select.limit.as_ref()?;
    let count = eval_limit_bound(&limit.count).ok()?;
    let offset = match &limit.offset {
        Some(expr) => eval_limit_bound(expr).ok()?,
        None => 0,
    };
    let cap = offset.checked_add(count)?;
    // Anything that can drop or add a row between the source and the LIMIT.
    if residual_where.is_some()
        || select.distinct
        || select.having.is_some()
        || crate::window::select_has_window(select)
    {
        return None;
    }
    if select.order_by.is_empty() {
        return Some(cap);
    }
    // An ORDER BY the access path already produces.
    let order = index_order?;
    order_is_index_order(select, order, resolver).then_some(cap)
}

/// Whether an index access path over `column_offsets` already produces the
/// order the `ORDER BY` asks for.
///
/// The by-items must be a prefix of the index's columns, all in the same
/// direction, over a single range. The storage iterator walks the encoded
/// interval forwards or backwards, and the codec's order is the collation
/// order the sort would have used.
fn order_is_index_order(
    select: &tidb_ast::SelectStmt,
    order: &IndexAccessOrder,
    resolver: &ScopeResolver<'_>,
) -> bool {
    if !order.single_range || select.order_by.len() > order.column_offsets.len() {
        return false;
    }
    let descending = select.order_by.first().is_some_and(|item| item.desc);
    let mut key_position = 0;
    for item in &select.order_by {
        if item.desc != descending {
            return false;
        }
        let Ok(Expression::Column(column)) = rewrite_expr_resolved(&item.expr, resolver) else {
            return false;
        };
        let Some(wanted_offset) = usize::try_from(column.index).ok() else {
            return false;
        };
        loop {
            let Some(offset) = order.column_offsets.get(key_position) else {
                return false;
            };
            // Case 1 precedes Case 2 in Go: ORDER BY the fixed column itself
            // still matches that key part. Only a different requested column
            // causes the fixed key part to be skipped.
            if *offset == wanted_offset {
                key_position += 1;
                break;
            }
            if order
                .constant_positions
                .get(key_position)
                .copied()
                .unwrap_or(false)
            {
                key_position += 1;
                continue;
            }
            return false;
        }
    }
    true
}

#[cfg(test)]
mod find_best_task_property_tests {
    use super::*;

    fn select(sql: &str) -> tidb_ast::SelectStmt {
        let statement = tidb_parser::parse(sql).expect("query parses");
        let Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let QueryStmt::Select(select) = &*query else {
            panic!("expected select")
        };
        (**select).clone()
    }

    #[test]
    fn match_property_skips_a_single_constant_index_prefix_only() {
        let columns = vec![
            ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
        ];
        let scope = PlanTrace::single_table_scope("t", None, columns);
        let resolver = ScopeResolver { scope: &scope };
        let query = select("SELECT b FROM t WHERE a = 1 ORDER BY b");
        let order_by_fixed = select("SELECT a FROM t WHERE a = 1 ORDER BY a");
        let fixed_prefix = IndexAccessOrder {
            column_offsets: vec![0, 1],
            single_range: true,
            constant_positions: vec![true, false],
            handle_covered_offsets: Vec::new(),
        };
        assert!(order_is_index_order(&query, &fixed_prefix, &resolver));
        assert!(order_is_index_order(
            &order_by_fixed,
            &fixed_prefix,
            &resolver
        ));

        let varying_prefix = IndexAccessOrder {
            constant_positions: vec![false, false],
            ..fixed_prefix
        };
        assert!(!order_is_index_order(&query, &varying_prefix, &resolver));

        let several_ranges = IndexAccessOrder {
            single_range: false,
            constant_positions: vec![true, false],
            ..varying_prefix
        };
        assert!(!order_is_index_order(&query, &several_ranges, &resolver));
    }

    #[test]
    fn point_get_rejects_either_open_endpoint() {
        let closed = IndexRange {
            low: vec![Datum::Int(7)],
            high: vec![Datum::Int(7)],
            low_exclusive: false,
            high_exclusive: false,
        };
        assert_eq!(
            single_point_handle(std::slice::from_ref(&closed)),
            Some(TableHandle::Int(7))
        );

        let mut low_open = closed.clone();
        low_open.low_exclusive = true;
        assert_eq!(single_point_handle(&[low_open]), None);

        let mut high_open = closed;
        high_open.high_exclusive = true;
        assert_eq!(single_point_handle(&[high_open]), None);
    }
}
