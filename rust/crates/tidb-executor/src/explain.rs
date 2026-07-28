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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `EXPLAIN <select>`: the plan this tier would execute, printed in the five
//! columns Go's row format uses (`id | estRows | task | access object |
//! operator info`).
//!
//! # What this is, and what it deliberately is not
//!
//! This tier has no plan object and no cost model: [`crate::driver`] decides
//! point-get vs batch-point-get vs index-range vs full scan, and whether a
//! selection/sort/projection/aggregate/limit is needed, WHILE it builds the
//! executor pipeline. So EXPLAIN here is a **plan recorder**, not an
//! optimizer trace: it re-runs the very same decision functions
//! ([`crate::driver::try_batch_point_get`], [`crate::driver::try_point_get`],
//! [`crate::driver::try_index_ranges`]) that the executing path calls and
//! records the operator each one selects. Nothing is executed -- no row is
//! read -- which is also what Go does (`EXPLAIN INSERT` plans without
//! inserting).
//!
//! # Divergences from Go's EXPLAIN, each deliberate and named
//!
//! 1. **No `cop[tikv]` task, no `TableReader`.** Go pushes scans, selections,
//!    limits and the first aggregate phase into a coprocessor task under a
//!    `TableReader_N` root operator. This tier reads rows in-process through
//!    [`crate::kv_table::KvTable`]; there is no coprocessor and no
//!    `TableReader` executor. Every row therefore reports task `root`, and
//!    the scan appears directly under its parent. Printing `cop[tikv]` would
//!    describe an executor that does not exist here.
//! 2. **`Sort` + `Limit`, never `TopN`.** Go's optimizer merges `ORDER BY` +
//!    `LIMIT` into one `TopN`. The driver builds a
//!    [`crate::sort::SortExec`] and a [`crate::limit::LimitExec`], so the
//!    plan shows both.
//! 3. **`Projection` is always present.** Go elides the projection when a
//!    query selects exactly the source columns (`select * from t` shows no
//!    `Projection`). The driver always builds a
//!    [`crate::projection::ProjectionExec`], so the recorder prints it.
//! 4. **One-phase `HashAgg`.** Go splits an aggregate into a cop-side and a
//!    root-side `HashAgg` communicating through `Column#N` slots allocated by
//!    the planner. This tier has one [`crate::hash_agg::HashAggExec`] and no
//!    column-id allocator, so `funcs:` prints the aggregate as written
//!    (`count(*)`) rather than Go's `count(1)->Column#6`.
//! 5. **Operator ids are build-order, not Go's plan-construction order.**
//!    Ids are assigned bottom-up in the order the driver builds executors,
//!    starting at 1. Go's counter also advances for logical operators that
//!    optimization later removes, so `TableFullScan_4` (Go) is
//!    `TableFullScan_1` here. The NAMES are Go's.
//! 6. **Join `estRows` is `N/A`.** Go's `12500.00` for a two-table equi-join
//!    comes from NDV-based cardinality estimation, which needs statistics
//!    this tier does not have. Rather than invent a number, the recorder
//!    prints Go's own not-available sentinel, the same one Go prints for
//!    `Insert_1`.
//! 7. **A point get keeps its Selection.** Go's fast plan REPLACES the whole
//!    pipeline, so `explain select * from t where a = 1` is one
//!    `Point_Get_1` row. Here the point get only narrows the SOURCE:
//!    `run_select_stmt` deliberately leaves the WHERE in place so a conjunct
//!    the handle did not pin still filters. The plan therefore shows
//!    `Projection > Selection > Point_Get`. Because the access path already
//!    priced those conditions, the selection does not reduce the estimate
//!    again -- see `plan_select`.
//!
//! 8. **`UPDATE`/`DELETE` always show `TableFullScan`, never `Point_Get` or
//!    `IndexRangeScan`.** Go's planner finds the same fast access paths for
//!    a write as for a `SELECT`. This tier's write drivers
//!    ([`crate::driver::run_update_in`], [`crate::driver::run_delete_in`])
//!    do not: both unconditionally `KvTable::scan_rows_with_handles` the
//!    whole table and filter each row with the `WHERE` in a plain iterator,
//!    with no access-path selection at all. The recorder mirrors that: a
//!    write's read plan is always `TableFullScan` (+ `Selection` for a
//!    `WHERE`), even for `WHERE <primary key> = <literal>`, where Go itself
//!    prints `Point_Get` (captured).
//!
//! # Where the estRows numbers come from
//!
//! Every value printed is a stats-less default read from Go's source, not a
//! guess, and each was confirmed against a `testkit.CreateMockStore` capture
//! of the real `EXPLAIN` output on a table with no analyzed statistics:
//!
//! * table row count: `statistics.PseudoRowCount = 10000`
//!   (`pkg/statistics/table.go`).
//! * a comparison filter (`>`, `>=`, `<`, `<=`): `1.0 / pseudoLessRate` with
//!   `pseudoLessRate = 3` (`pkg/planner/cardinality/pseudo.go`), giving
//!   10000/3 = 3333.33 -- matching the capture.
//! * an equality filter: `1.0 / pseudoEqualRate` with
//!   `pseudoEqualRate = 1000` (same file).
//! * anything else: `SelectivityFactor`, whose default is 0.8
//!   (`vardef.DefOptSelectivityFactor`).
//! * a GROUP BY's output cardinality: `distinctFactor = 0.8`
//!   (`pkg/planner/cardinality/ndv.go`), giving 8000.00 -- matching the
//!   capture.
//! * a point get: 1.00; a batch point get: the number of handles -- both
//!   exact, not estimates.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};

use crate::driver::{
    eval_limit_bound, row_chunk, run_insert_stmt, single_kv_table, split_table_path_pub,
    try_batch_point_get, try_index_ranges, try_point_get, Catalog, DriverError, FromScope,
    FromTable, SelectMeta, TableEntry,
};
use crate::executor::ExecError;
use crate::kv_table::TableHandle;

/// Go `statistics.PseudoRowCount` (`pkg/statistics/table.go`): the row count
/// assumed for a table with no analyzed statistics.
const PSEUDO_ROW_COUNT: f64 = 10000.0;
/// Go `pseudoLessRate` (`pkg/planner/cardinality/pseudo.go`).
const PSEUDO_LESS_RATE: f64 = 3.0;
/// Go `pseudoEqualRate` (same file).
const PSEUDO_EQUAL_RATE: f64 = 1000.0;
/// Go `vardef.DefOptSelectivityFactor`, the fallback selectivity for a
/// condition the pseudo model cannot classify.
const SELECTIVITY_FACTOR: f64 = 0.8;
/// Go `distinctFactor` (`pkg/planner/cardinality/ndv.go`): the assumed NDV
/// ratio of a grouping key without statistics.
const DISTINCT_FACTOR: f64 = 0.8;

/// One node of the recorded plan, before ids are assigned.
struct PlanNode {
    /// Go's operator name without the `_N` suffix (`TableFullScan`).
    name: &'static str,
    /// The `estRows` cell; `None` prints Go's `N/A`.
    est_rows: Option<f64>,
    /// The `access object` cell.
    access: String,
    /// The `operator info` cell.
    info: String,
    /// Children, in the order Go prints them (build side first for a join).
    children: Vec<PlanNode>,
}

impl PlanNode {
    fn leaf(name: &'static str, est_rows: Option<f64>, access: String, info: String) -> Self {
        Self {
            name,
            est_rows,
            access,
            info,
            children: Vec::new(),
        }
    }

    /// A unary operator over `child`, inheriting its estimate unless one is
    /// given.
    fn unary(name: &'static str, est_rows: Option<f64>, info: String, child: PlanNode) -> Self {
        Self {
            name,
            est_rows,
            access: String::new(),
            info,
            children: vec![child],
        }
    }
}

/// The header Go's row-format EXPLAIN reports, captured from TiDB.
const EXPLAIN_COLUMNS: [&str; 5] = ["id", "estRows", "task", "access object", "operator info"];

/// The `EXPLAIN FORMAT = '...'` this tier accepts. Go's `'row'` (the
/// default, also the explicit spelling) and `'brief'` render the identical
/// tree; `'brief'` merely drops each operator's `_N` build-order suffix
/// (captured: `explain format = 'brief' select ...` prints `Point_Get` where
/// the default prints `Point_Get_1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    /// Go's default: every operator id carries its `_N` build-order suffix.
    Row,
    /// Same tree, ids printed without the `_N` suffix.
    Brief,
}

impl ExplainFormat {
    /// Parses Go's `ExplainStmt.Format` string, case-insensitively as Go's
    /// preprocessor does. `None` for a format this tier does not recognize
    /// -- the caller reports Go's own "Unknown EXPLAIN format name" error
    /// (captured verbatim from `explain format = 'bogus' ...`).
    pub fn parse(format: &str) -> Option<Self> {
        if format.eq_ignore_ascii_case("row") {
            Some(Self::Row)
        } else if format.eq_ignore_ascii_case("brief") {
            Some(Self::Brief)
        } else {
            None
        }
    }
}

/// Plans `select` and reports the plan as EXPLAIN rows, executing nothing.
pub fn explain_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let plan = plan_select(select, catalog, current_db)?;
    Ok(render(plan, format))
}

/// `EXPLAIN ANALYZE <select>`: the same plan tree [`explain_select_stmt`]
/// records, but the query actually RUNS (real `EXPLAIN ANALYZE` executes
/// the wrapped statement to gather its runtime counters, confirmed by
/// capture), and each operator's `actRows` is the REAL row count that
/// stage produced -- never an estimate, never fabricated.
///
/// [`compute_act_rows`] computes those real counts for a single
/// (non-`JOIN`) `KV`-backed table read through a plain `TableFullScan`
/// (the access path a bare `WHERE` on a non-indexed column takes); every
/// other shape (a `JOIN`, a `Point_Get`/`Batch_Point_Get`/
/// `IndexRangeScan` access path, or a grouped aggregate/`DISTINCT`) prints
/// `actRows` as `N/A` for the nodes it cannot count precisely rather than
/// guess -- the same honest-placeholder choice `EXPLAIN` itself already
/// makes for a join's `estRows`.
pub fn explain_analyze_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let plan = plan_select(select, catalog, current_db)?;
    let act_rows = compute_act_rows(select, catalog, current_db, ctx)?;
    Ok(render_analyze(plan, &act_rows, format))
}

/// Plans an `INSERT` and reports the plan as EXPLAIN rows, executing
/// nothing. Go's `Insert_N` row carries none of the estimate/access/info a
/// read operator would (captured: `[Insert_1 N/A root  N/A]`, both for a
/// plain `VALUES` insert and for the `Insert ... SELECT` form, where the
/// select's own plan appears as `Insert`'s one child).
pub fn explain_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let children = match &insert.source {
        Some(query) => {
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                return Err(DriverError::Unsupported(
                    "EXPLAIN of a set-operation INSERT source is not supported yet",
                ));
            };
            vec![plan_select(select, catalog, current_db)?]
        }
        None => Vec::new(),
    };
    let plan = PlanNode {
        name: "Insert",
        est_rows: None,
        access: String::new(),
        info: "N/A".to_owned(),
        children,
    };
    Ok(render(plan, format))
}

/// `EXPLAIN ANALYZE <insert>`: unlike [`explain_insert_stmt`], this really
/// inserts the row(s) -- real `EXPLAIN ANALYZE INSERT` executes the
/// statement (captured: the table has the new row afterward). The
/// `Insert_N` node's `actRows` is always `0`: Go's own `Insert_1` row shows
/// `actRows` `0` too (captured), because the insert executor's `Next()`
/// produces no rows of its own -- the write is a side effect, not this
/// operator's row-producing interface.
pub fn explain_analyze_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let children = match &insert.source {
        Some(query) => {
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                return Err(DriverError::Unsupported(
                    "EXPLAIN of a set-operation INSERT source is not supported yet",
                ));
            };
            vec![plan_select(select, catalog, current_db)?]
        }
        None => Vec::new(),
    };
    run_insert_stmt(insert, catalog, current_db, ctx)?;
    let plan = PlanNode {
        name: "Insert",
        est_rows: None,
        access: String::new(),
        info: "N/A".to_owned(),
        children,
    };
    // Bottom-up numbering (`assign_ids`) builds every child before the
    // `Insert` node itself, so `Insert`'s own entry is always the LAST slot;
    // an `INSERT ... SELECT` source's real per-stage counts are not
    // recovered here (the source already ran, inside `run_insert_stmt`, so
    // re-deriving them would mean running the query twice) -- `N/A`, same
    // as every other not-precisely-tracked operator.
    let total = count_nodes(&plan);
    let mut act_rows = vec![None; total];
    act_rows[total - 1] = Some(0);
    Ok(render_analyze(plan, &act_rows, format))
}

/// The number of nodes in a plan tree, for sizing an `act_rows` vector that
/// [`render_analyze`]/[`assign_ids`] will index by bottom-up build order.
fn count_nodes(node: &PlanNode) -> usize {
    1 + node.children.iter().map(count_nodes).sum::<usize>()
}

/// Plans an `UPDATE` and reports the plan as EXPLAIN rows, executing
/// nothing. `Update_N`'s one child is the same read the driver would run to
/// find the rows to update: the access path `plan_source` picks, with a
/// `Selection` above it only when the access path did not already consume
/// the `WHERE` (captured: a `WHERE` on the primary key gives one
/// `Point_Get` child with no `Selection`; a `WHERE` on any other column
/// gives `Selection` over `TableFullScan`/`IndexRangeScan`).
pub fn explain_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &Catalog,
    current_db: &str,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let tidb_ast::UpdateKind::Single(table_ref) = &update.kind else {
        return Err(DriverError::Unsupported(
            "EXPLAIN of a multi-table UPDATE is not supported yet",
        ));
    };
    let child = plan_dml_source(table_ref, &update.where_clause, catalog, current_db)?;
    let plan = PlanNode {
        name: "Update",
        est_rows: None,
        access: String::new(),
        info: "N/A".to_owned(),
        children: vec![child],
    };
    Ok(render(plan, format))
}

/// Plans a `DELETE` and reports the plan as EXPLAIN rows, executing nothing.
/// See [`explain_update_stmt`]: `Delete_N`'s child is the same read-path
/// plan.
pub fn explain_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &Catalog,
    current_db: &str,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let tidb_ast::DeleteKind::Single(table_ref) = &delete.kind else {
        return Err(DriverError::Unsupported(
            "EXPLAIN of a multi-table DELETE is not supported yet",
        ));
    };
    let child = plan_dml_source(table_ref, &delete.where_clause, catalog, current_db)?;
    let plan = PlanNode {
        name: "Delete",
        est_rows: None,
        access: String::new(),
        info: "N/A".to_owned(),
        children: vec![child],
    };
    Ok(render(plan, format))
}

/// The read plan an `UPDATE`/`DELETE` builds to find its target rows.
///
/// Unlike a `SELECT`, the write drivers (`driver::run_update_in`,
/// `driver::run_delete_in`) never call the point-get/batch-point-get/
/// index-range fast paths at all: both unconditionally
/// `KvTable::scan_rows_with_handles` the whole table and filter each row
/// with the `WHERE` in a plain iterator (confirmed by reading those
/// functions -- there is no access-path selection to mirror). So this
/// recorder always shows `TableFullScan`, with a `Selection` above it for
/// the `WHERE` (a full scan consumes nothing, so the selection is the one
/// place that narrows the estimate) -- which is what a real capture
/// confirms for anything but a bare primary-key equality (`explain update t
/// set b = 100 where c = 1` -> `TableFullScan` + `Selection`). Go's own
/// planner instead finds a `Point_Get` for a primary-key equality even
/// inside `UPDATE`/`DELETE`; printing that here would describe an access
/// path this tier's write executors do not take.
fn plan_dml_source(
    table_ref: &tidb_ast::TableRef,
    where_clause: &Option<tidb_ast::Expr>,
    catalog: &Catalog,
    current_db: &str,
) -> Result<PlanNode, DriverError> {
    let (database, name) = crate::driver::single_table_name(table_ref, current_db)?;
    let entry = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::Unsupported("table not found in catalog"))?;
    let visible = table_ref.alias.clone().unwrap_or_else(|| name.clone());
    let scope = FromScope {
        tables: vec![FromTable {
            name: visible.clone(),
            database: table_ref.alias.is_none().then(|| database.clone()),
            columns: entry.column_list(),
            offset: 0,
        }],
    };
    let mut node = PlanNode::leaf(
        "TableFullScan",
        Some(PSEUDO_ROW_COUNT),
        format!("table:{visible}"),
        "keep order:false, stats:pseudo".to_owned(),
    );
    if let Some(predicate) = where_clause {
        let qualify = Qualifier {
            db: current_db,
            scope: &scope,
        };
        let rows = node.est_rows.map(|r| r * selectivity(predicate));
        node = PlanNode::unary("Selection", rows, qualify.expr(predicate), node);
    }
    Ok(node)
}

/// Assigns ids bottom-up and flattens the tree into the five text columns.
fn render(plan: PlanNode, format: ExplainFormat) -> SelectMeta {
    let mut counter = 0;
    let plan = assign_ids(plan, &mut counter);
    let mut rows = Vec::new();
    flatten(&plan, String::new(), true, true, format, &mut rows);
    let field_type = FieldType::new(FieldTypeCode::VarString);
    let columns = EXPLAIN_COLUMNS
        .iter()
        .map(|name| ((*name).to_owned(), field_type.clone()))
        .collect();
    (columns, rows)
}

/// The header real `EXPLAIN ANALYZE` reports (captured from TiDB): the same
/// five `EXPLAIN` columns, with `actRows` inserted after `estRows` and
/// `execution info`/`memory`/`disk` appended after `operator info`.
const EXPLAIN_ANALYZE_COLUMNS: [&str; 9] = [
    "id",
    "estRows",
    "actRows",
    "task",
    "access object",
    "execution info",
    "operator info",
    "memory",
    "disk",
];

/// Like [`render`], but for `EXPLAIN ANALYZE`: threads a real `actRows`
/// value (or `None` for an operator this tier does not track precisely,
/// see [`compute_act_rows`]) alongside each node, indexed by the SAME
/// bottom-up `counter` [`assign_ids`] already assigns -- `act_rows[i]` is
/// the `i`-th node built, exactly matching `compute_act_rows`'s own push
/// order because it mirrors [`plan_select`]'s control flow node-for-node.
///
/// `execution info`, `memory`, and `disk` always print `N/A`: this tier
/// collects no runtime timing, memory, or spill counters at all (captured
/// Go values for those columns are non-deterministic timings/byte counts
/// this tier has no machinery to produce, and inventing numbers for them
/// would be worse than an honest placeholder -- the same reasoning
/// `EXPLAIN`'s own `est_rows: None` -> `"N/A"` already uses for a join's
/// cardinality).
fn render_analyze(plan: PlanNode, act_rows: &[Option<u64>], format: ExplainFormat) -> SelectMeta {
    let mut counter = 0;
    let plan = assign_ids(plan, &mut counter);
    let mut rows = Vec::new();
    flatten_analyze(
        &plan,
        String::new(),
        true,
        true,
        format,
        act_rows,
        &mut rows,
    );
    let field_type = FieldType::new(FieldTypeCode::VarString);
    let columns = EXPLAIN_ANALYZE_COLUMNS
        .iter()
        .map(|name| ((*name).to_owned(), field_type.clone()))
        .collect();
    (columns, rows)
}

/// [`flatten`], plus the real `actRows`/`execution info`/`memory`/`disk`
/// columns `EXPLAIN ANALYZE` adds.
fn flatten_analyze(
    node: &IdNode,
    prefix: String,
    is_root: bool,
    is_last: bool,
    format: ExplainFormat,
    act_rows: &[Option<u64>],
    out: &mut Vec<Vec<Datum>>,
) {
    let name = match format {
        ExplainFormat::Row => format!("{}_{}", node.name, node.counter),
        ExplainFormat::Brief => node.name.to_owned(),
    };
    let id = if is_root {
        name
    } else if is_last {
        format!("{prefix}└─{name}")
    } else {
        format!("{prefix}├─{name}")
    };
    let est = match node.est_rows {
        Some(value) => format!("{value:.2}"),
        None => "N/A".to_owned(),
    };
    let act = match act_rows.get(node.counter - 1).copied().flatten() {
        Some(value) => value.to_string(),
        None => "N/A".to_owned(),
    };
    out.push(vec![
        text(&id),
        text(&est),
        text(&act),
        text("root"),
        text(&node.access),
        text("N/A"),
        text(&node.info),
        text("N/A"),
        text("N/A"),
    ]);
    let child_prefix = if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}  ")
    } else {
        format!("{prefix}│ ")
    };
    let last = node.children.len().saturating_sub(1);
    for (i, child) in node.children.iter().enumerate() {
        flatten_analyze(
            child,
            child_prefix.clone(),
            false,
            i == last,
            format,
            act_rows,
            out,
        );
    }
}

/// A plan node whose id is fixed.
struct IdNode {
    /// Go's operator name without the `_N` suffix (`TableFullScan`).
    name: &'static str,
    /// The build-order number `assign_ids` gave this node.
    counter: usize,
    est_rows: Option<f64>,
    access: String,
    info: String,
    children: Vec<IdNode>,
}

/// Numbers the tree bottom-up in the driver's own build order: a node's
/// children are built before it, so they take the lower ids.
fn assign_ids(node: PlanNode, counter: &mut usize) -> IdNode {
    let children: Vec<IdNode> = node
        .children
        .into_iter()
        .map(|child| assign_ids(child, counter))
        .collect();
    *counter += 1;
    IdNode {
        name: node.name,
        counter: *counter,
        est_rows: node.est_rows,
        access: node.access,
        info: node.info,
        children,
    }
}

/// Go's tree drawing: the last child gets `└─`, an earlier sibling `├─`, and
/// a non-last child's descendants are prefixed with `│ ` so the branch line
/// continues past them.
fn flatten(
    node: &IdNode,
    prefix: String,
    is_root: bool,
    is_last: bool,
    format: ExplainFormat,
    out: &mut Vec<Vec<Datum>>,
) {
    // Divergence: `'brief'` drops the `_N` suffix Go's `'row'`/default
    // format prints (captured: `Point_Get` vs `Point_Get_1`).
    let name = match format {
        ExplainFormat::Row => format!("{}_{}", node.name, node.counter),
        ExplainFormat::Brief => node.name.to_owned(),
    };
    let id = if is_root {
        name
    } else if is_last {
        format!("{prefix}└─{name}")
    } else {
        format!("{prefix}├─{name}")
    };
    let est = match node.est_rows {
        Some(value) => format!("{value:.2}"),
        None => "N/A".to_owned(),
    };
    out.push(vec![
        text(&id),
        text(&est),
        // Divergence 1: every operator here runs in the TiDB process.
        text("root"),
        text(&node.access),
        text(&node.info),
    ]);
    let child_prefix = if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}  ")
    } else {
        format!("{prefix}│ ")
    };
    let last = node.children.len().saturating_sub(1);
    for (i, child) in node.children.iter().enumerate() {
        flatten(child, child_prefix.clone(), false, i == last, format, out);
    }
}

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

/// Resolves an unqualified/`t.`-qualified column name against one table's
/// schema, for evaluating a real `WHERE` predicate against real rows in
/// [`compute_act_rows`] -- the same shape as `driver::TableResolver`,
/// reimplemented locally rather than exposing that private type.
struct RowResolver<'a> {
    table_name: &'a str,
    columns: &'a [(String, FieldType)],
}

impl ColumnResolver for RowResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] => (Some(table), name),
            _ => return None,
        };
        if let Some(q) = qualifier {
            if !q.eq_ignore_ascii_case(self.table_name) {
                return None;
            }
        }
        self.columns
            .iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|i| (i, self.columns[i].1.clone(), (i + 1) as i64))
    }
}

/// Computes the real, per-node `actRows` for [`explain_analyze_select_stmt`],
/// mirroring [`plan_select`]'s control flow node-for-node so the returned
/// vector's order matches [`assign_ids`]'s bottom-up numbering exactly:
/// this function pushes one entry per `PlanNode` that `plan_select` would
/// build, in the same order (leaf/source, then `Selection`, then either
/// `HashAgg` (+ `Limit`) or `Sort`/`Projection`/distinct-`HashAgg` (+
/// `Limit`)), so [`render_analyze`] can index `act_rows[counter - 1]`
/// directly.
///
/// Real counts are computed only for a `FROM`-less `SELECT` (`TableDual`,
/// always exactly 1 real row) and a single (non-`JOIN`) KV-backed table
/// read through a plain `TableFullScan` -- the access path an unindexed
/// `WHERE` takes. For that shape, every downstream node's real row count
/// can also be tracked exactly without re-running the query multiple
/// times: `Selection` is the real predicate evaluated against the real
/// scanned rows (via the same `rewrite_expr_resolved`/[`row_chunk`]
/// machinery `UPDATE`/`DELETE` use to test a row for real), `Sort` and
/// `Projection` never change the row count, a whole-table (no `GROUP BY`)
/// `HashAgg` always collapses to exactly 1 row, and `Limit` is `min(rows,
/// count)` after skipping `offset`.
///
/// Everything else -- a `JOIN`, a `Point_Get`/`Batch_Point_Get`/
/// `IndexRangeScan` access path, a grouped `HashAgg`, or `DISTINCT` (whose
/// real output needs the actual distinct projected tuples, not just an
/// input row count) -- pushes `None` for the nodes it cannot count
/// precisely rather than guess, and every node downstream of a `None`
/// also gets `None`.
fn compute_act_rows(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<Option<u64>>, DriverError> {
    if select.with.is_some() {
        return Err(DriverError::Unsupported(
            "EXPLAIN of a WITH clause is not supported yet",
        ));
    }
    let scope = explain_scope(&select.from, catalog, current_db)?;
    let mut counts: Vec<Option<u64>> = Vec::new();

    let mut rows: Option<Vec<Vec<Datum>>> = if select.from.is_none() {
        counts.push(Some(1));
        None
    } else if let Some(mut table) = single_kv_table(&select.from, catalog, current_db) {
        let columns = scope.column_list();
        if try_batch_point_get(select, &table, &columns)?.is_some()
            || try_point_get(select, &table, &columns)?.is_some()
            || try_index_ranges(select, &table, &columns).is_some()
        {
            counts.push(None);
            None
        } else {
            let scanned = table
                .scan_rows()
                .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
            counts.push(Some(scanned.len() as u64));
            Some(scanned)
        }
    } else {
        // A `JOIN` (or a non-KV single table) builds a node shape this
        // function does not re-derive; report every node in the real tree
        // as untracked rather than guess at it.
        let plan = plan_select(select, catalog, current_db)?;
        return Ok(vec![None; count_nodes(&plan)]);
    };

    let columns = scope.column_list();
    let visible = scope
        .tables
        .first()
        .map(|t| t.name.clone())
        .unwrap_or_default();
    let field_types: Vec<FieldType> = columns.iter().map(|(_, ft)| ft.clone()).collect();

    if let Some(predicate) = &select.where_clause {
        match rows.take() {
            Some(source_rows) => {
                let resolver = RowResolver {
                    table_name: &visible,
                    columns: &columns,
                };
                let expr = rewrite_expr_resolved(predicate, &resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let mut filtered = Vec::with_capacity(source_rows.len());
                for row in source_rows {
                    let chunk = row_chunk(&row, &field_types)?;
                    let value = expr
                        .eval(ctx, chunk.get_row(0))
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                    if crate::truthy_of(&value)
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?
                        == Some(true)
                    {
                        filtered.push(row);
                    }
                }
                counts.push(Some(filtered.len() as u64));
                rows = Some(filtered);
            }
            None => counts.push(None),
        }
    }

    let is_aggregate = !select.group_by.is_empty()
        || select.fields.fields().iter().any(|f| {
            matches!(
                f,
                tidb_ast::SelectField::Expr {
                    expr: tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. },
                    ..
                }
            )
        });

    if is_aggregate {
        if select.group_by.is_empty() {
            // A whole-table aggregate always collapses to exactly 1 row,
            // whatever the input -- real, not an estimate.
            counts.push(Some(1));
            rows = rows.map(|_| Vec::new());
        } else {
            // The real number of distinct groups needs the real grouping
            // key values, which this function does not compute.
            counts.push(None);
            rows = None;
        }
        push_limit_count(select, &mut rows, &mut counts);
        return Ok(counts);
    }

    if !select.order_by.is_empty() {
        // `Sort` never changes the row count.
        counts.push(rows.as_ref().map(|r| r.len() as u64));
    }

    // `Projection` never changes the row count either (divergence 3: this
    // tier always builds one).
    counts.push(rows.as_ref().map(|r| r.len() as u64));

    if select.distinct {
        // Real `DISTINCT` output needs the actual distinct projected
        // tuples, not just an input row count.
        counts.push(None);
        rows = None;
    }

    push_limit_count(select, &mut rows, &mut counts);
    Ok(counts)
}

/// Applies `plan_select`'s `apply_limit` to a real row set, pushing the
/// `Limit` node's real `actRows` (or nothing, if there is no `LIMIT`).
fn push_limit_count(
    select: &tidb_ast::SelectStmt,
    rows: &mut Option<Vec<Vec<Datum>>>,
    counts: &mut Vec<Option<u64>>,
) {
    let Some(limit) = &select.limit else {
        return;
    };
    let (Ok(count), offset) = (
        eval_limit_bound(&limit.count),
        limit
            .offset
            .as_ref()
            .and_then(|e| eval_limit_bound(e).ok())
            .unwrap_or(0),
    ) else {
        counts.push(None);
        *rows = None;
        return;
    };
    match rows.take() {
        Some(source_rows) => {
            let remaining = (source_rows.len() as u64).saturating_sub(offset);
            let limited = remaining.min(count);
            counts.push(Some(limited));
        }
        None => counts.push(None),
    }
}

/// Builds the plan tree, mirroring `driver::run_select_stmt`'s decisions in
/// the same order it makes them.
fn plan_select(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Result<PlanNode, DriverError> {
    if select.with.is_some() {
        return Err(DriverError::Unsupported(
            "EXPLAIN of a WITH clause is not supported yet",
        ));
    }
    let scope = explain_scope(&select.from, catalog, current_db)?;
    let Source { mut node, consumed } = plan_source(select, catalog, current_db, &scope)?;

    let qualify = Qualifier {
        db: current_db,
        scope: &scope,
    };

    // Aggregate path: GROUP BY, or any aggregate in the select list. It
    // consumes the whole tail of the pipeline in the driver, so it is
    // recorded the same way.
    let is_aggregate = !select.group_by.is_empty()
        || select.fields.fields().iter().any(|f| {
            matches!(
                f,
                tidb_ast::SelectField::Expr {
                    expr: tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. },
                    ..
                }
            )
        });

    // WHERE: a selection over the source.
    if let Some(predicate) = &select.where_clause {
        // The access path's own estimate already reflects the conditions it
        // consumed (Go's DetachCondAndBuildRange split: access conditions are
        // priced once, by the read). A selection above such a path re-checks
        // them, so it must not multiply the estimate a second time. Only a
        // plain full scan consumed nothing, so only there does the filter
        // reduce the estimate -- which is exactly how Go's 10000 -> 3333.33
        // arises.
        let rows = node.est_rows.map(|r| {
            if consumed {
                r
            } else {
                r * selectivity(predicate)
            }
        });
        node = PlanNode::unary("Selection", rows, qualify.expr(predicate), node);
    }

    if is_aggregate {
        let mut info = String::new();
        if !select.group_by.is_empty() {
            info.push_str("group by:");
            let keys: Vec<String> = select
                .group_by
                .iter()
                .map(|e| qualify.expr(&e.expr))
                .collect();
            info.push_str(&keys.join(", "));
            info.push_str(", ");
        }
        // Divergence 4: one phase, and the function as written.
        let funcs: Vec<String> = select
            .fields
            .fields()
            .iter()
            .filter_map(|f| match f {
                tidb_ast::SelectField::Expr { expr, .. } => Some(qualify.expr(expr)),
                tidb_ast::SelectField::Wildcard(_) => None,
            })
            .collect();
        info.push_str("funcs:");
        info.push_str(&funcs.join(", "));
        let rows = if select.group_by.is_empty() {
            // A whole-table aggregate collapses to one row.
            Some(1.0)
        } else {
            node.est_rows.map(|r| r * DISTINCT_FACTOR)
        };
        node = PlanNode::unary("HashAgg", rows, info, node);
        return Ok(apply_limit(select, node));
    }

    // ORDER BY: a sort below the projection (divergence 2: never a TopN).
    if !select.order_by.is_empty() {
        let items: Vec<String> = select
            .order_by
            .iter()
            .map(|item| {
                let rendered = qualify.expr(&item.expr);
                if item.desc {
                    format!("{rendered}:desc")
                } else {
                    rendered
                }
            })
            .collect();
        let rows = node.est_rows;
        node = PlanNode::unary("Sort", rows, items.join(", "), node);
    }

    // Divergence 3: the driver always builds a projection.
    let fields: Vec<String> = select
        .fields
        .fields()
        .iter()
        .map(|f| match f {
            tidb_ast::SelectField::Expr { expr, .. } => qualify.expr(expr),
            tidb_ast::SelectField::Wildcard(path) => match path.last() {
                Some(table) => format!("{table}.*"),
                None => "*".to_owned(),
            },
        })
        .collect();
    let rows = node.est_rows;
    node = PlanNode::unary("Projection", rows, fields.join(", "), node);

    if select.distinct {
        // Go's buildDistinct is an aggregation grouping by every projected
        // column, so it carries the same NDV assumption.
        let rows = node.est_rows.map(|r| r * DISTINCT_FACTOR);
        let info = format!("group by:{}, funcs:firstrow", fields.join(", "));
        node = PlanNode::unary("HashAgg", rows, info, node);
    }

    Ok(apply_limit(select, node))
}

/// LIMIT caps the child's estimate at the requested count, as Go's does.
fn apply_limit(select: &tidb_ast::SelectStmt, node: PlanNode) -> PlanNode {
    let Some(limit) = &select.limit else {
        return node;
    };
    let (Ok(count), offset) = (
        eval_limit_bound(&limit.count),
        limit
            .offset
            .as_ref()
            .and_then(|e| eval_limit_bound(e).ok())
            .unwrap_or(0),
    ) else {
        return node;
    };
    let rows = node.est_rows.map(|r| r.min(count as f64));
    PlanNode::unary(
        "Limit",
        rows,
        format!("offset:{offset}, count:{count}"),
        node,
    )
}

/// A source operator plus whether its access path already priced the WHERE.
struct Source {
    node: PlanNode,
    /// True when the read itself consumed the conditions that selected it (a
    /// handle lookup or an index range), so a selection above it re-checks
    /// rather than filters further.
    consumed: bool,
}

/// The source operator: whichever read `run_select_stmt` would pick, decided
/// by the same functions it calls, in the same order.
fn plan_source(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Result<Source, DriverError> {
    if select.from.is_none() {
        return Ok(Source {
            node: PlanNode::leaf("TableDual", Some(1.0), String::new(), "rows:1".to_owned()),
            consumed: false,
        });
    }
    if let Some(table) = single_kv_table(&select.from, catalog, current_db) {
        let columns = scope.column_list();
        let access = format!("table:{}", visible_name(scope, &table.name));
        if let Some(handles) = try_batch_point_get(select, &table, &columns)? {
            let printed: Vec<String> = handles.iter().map(handle_text).collect();
            return Ok(Source {
                consumed: true,
                node: PlanNode::leaf(
                    "Batch_Point_Get",
                    Some(handles.len() as f64),
                    access,
                    format!(
                        "handle:[{}], keep order:false, desc:false",
                        printed.join(" ")
                    ),
                ),
            });
        }
        if let Some(handle) = try_point_get(select, &table, &columns)? {
            let printed = match &handle {
                Some(handle) => handle_text(handle),
                // The WHERE pinned a handle no row can carry (Go still plans
                // a Point_Get and reads nothing).
                None => "NULL".to_owned(),
            };
            return Ok(Source {
                consumed: true,
                node: PlanNode::leaf("Point_Get", Some(1.0), access, format!("handle:{printed}")),
            });
        }
        if let Some((index_id, ranges)) = try_index_ranges(select, &table, &columns) {
            let index = table
                .indexes()
                .iter()
                .find(|i| i.id == index_id)
                .expect("try_index_ranges returns an index of this table");
            let index_columns: Vec<&str> = index
                .column_offsets
                .iter()
                .map(|offset| columns[*offset].0.as_str())
                .collect();
            let access = format!(
                "{access}, index:{}({})",
                index.name,
                index_columns.join(", ")
            );
            let printed: Vec<String> = ranges.iter().map(range_text).collect();
            return Ok(Source {
                consumed: true,
                node: PlanNode::leaf(
                    "IndexRangeScan",
                    // The ranges narrow the read, but by how much needs the
                    // per-column histogram this tier has no statistics for, so
                    // the estimate is the same stats-less one Go falls back to.
                    Some(PSEUDO_ROW_COUNT / PSEUDO_LESS_RATE),
                    access,
                    format!(
                        "range:{}, keep order:false, stats:pseudo",
                        printed.join(", ")
                    ),
                ),
            });
        }
        return Ok(Source {
            consumed: false,
            node: PlanNode::leaf(
                "TableFullScan",
                Some(PSEUDO_ROW_COUNT),
                access,
                "keep order:false, stats:pseudo".to_owned(),
            ),
        });
    }
    let node = plan_from(
        select.from.as_ref().expect("checked above"),
        catalog,
        current_db,
        scope,
    )?;
    Ok(Source {
        node,
        consumed: false,
    })
}

/// A join, or a single non-KV (in-memory) table.
fn plan_from(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Result<PlanNode, DriverError> {
    let left = plan_join_node(&join.left, catalog, current_db)?;
    let Some(right_node) = &join.right else {
        return Ok(left);
    };
    if join.natural || !join.using.is_empty() {
        return Err(DriverError::Unsupported(
            "NATURAL and USING joins are not supported yet",
        ));
    }
    let right = plan_join_node(right_node, catalog, current_db)?;
    let qualify = Qualifier {
        db: current_db,
        scope,
    };
    let kind = match join.tp {
        tidb_ast::JoinType::Cross => "inner join",
        tidb_ast::JoinType::Left => "left outer join",
        tidb_ast::JoinType::Right => "right outer join",
    };
    let info = match &join.on {
        Some(expr) => format!("{kind}, conditions:{}", qualify.expr(expr)),
        None => kind.to_owned(),
    };
    Ok(PlanNode {
        // The driver builds a nested-loop JoinExec; Go's own name for the
        // shape with no hash or merge structure.
        name: "HashJoin",
        // Divergence 6: an equi-join's cardinality needs statistics.
        est_rows: None,
        access: String::new(),
        info,
        children: vec![left, right],
    })
}

fn plan_join_node(
    node: &tidb_ast::JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> Result<PlanNode, DriverError> {
    match node {
        tidb_ast::JoinNode::Table(table_ref) => {
            let (database, name) = split_table_path_pub(&table_ref.name, current_db)?;
            catalog
                .get_in(database, name)
                .ok_or(DriverError::Unsupported("table not found in catalog"))?;
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            Ok(PlanNode::leaf(
                "TableFullScan",
                Some(PSEUDO_ROW_COUNT),
                format!("table:{visible}"),
                "keep order:false, stats:pseudo".to_owned(),
            ))
        }
        tidb_ast::JoinNode::Join(join) => {
            let scope = explain_scope(&Some((**join).clone()), catalog, current_db)?;
            plan_from(join, catalog, current_db, &scope)
        }
        tidb_ast::JoinNode::Derived { .. } => Err(DriverError::Unsupported(
            "derived tables are not supported yet",
        )),
    }
}

/// The FROM scope, computed exactly as `driver::build_join` computes it but
/// without building any executor -- EXPLAIN must not read a row.
fn explain_scope(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Result<FromScope, DriverError> {
    let Some(join) = from else {
        return Ok(FromScope::default());
    };
    scope_of_join(join, catalog, current_db)
}

fn scope_of_join(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
) -> Result<FromScope, DriverError> {
    let left = scope_of_node(&join.left, catalog, current_db)?;
    let Some(right_node) = &join.right else {
        return Ok(left);
    };
    let right = scope_of_node(right_node, catalog, current_db)?;
    let left_width = left.width();
    let mut scope = left;
    for table in right.tables {
        scope.tables.push(FromTable {
            name: table.name,
            database: table.database,
            columns: table.columns,
            offset: table.offset + left_width,
        });
    }
    Ok(scope)
}

fn scope_of_node(
    node: &tidb_ast::JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> Result<FromScope, DriverError> {
    match node {
        tidb_ast::JoinNode::Table(table_ref) => {
            let (database, name) = split_table_path_pub(&table_ref.name, current_db)?;
            let entry: &TableEntry = catalog
                .get_in(database, name)
                .ok_or(DriverError::Unsupported("table not found in catalog"))?;
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            Ok(FromScope {
                tables: vec![FromTable {
                    name: visible,
                    database: table_ref.alias.is_none().then(|| database.to_owned()),
                    columns: entry.column_list(),
                    offset: 0,
                }],
            })
        }
        tidb_ast::JoinNode::Join(join) => scope_of_join(join, catalog, current_db),
        tidb_ast::JoinNode::Derived { .. } => Err(DriverError::Unsupported(
            "derived tables are not supported yet",
        )),
    }
}

/// The name the `access object` prints: the alias when the FROM gave one,
/// which is what Go prints too.
fn visible_name<'a>(scope: &'a FromScope, table: &'a str) -> &'a str {
    match scope.tables.first() {
        Some(first) => &first.name,
        None => table,
    }
}

fn handle_text(handle: &TableHandle) -> String {
    match handle {
        TableHandle::Int(value) => value.to_string(),
        // A clustered-index handle is a byte string; Go prints its decoded
        // datums, which needs the handle codec this printer does not carry.
        TableHandle::Common(_) => "<common handle>".to_owned(),
    }
}

/// Go's range notation: a square bracket includes the bound, a parenthesis
/// excludes it, and an absent bound is an infinity.
fn range_text(range: &crate::kv_table::IndexRange) -> String {
    let low = bound_text(&range.low, "-inf");
    let high = bound_text(&range.high, "+inf");
    let open = if range.low_exclusive { '(' } else { '[' };
    let close = if range.high_exclusive { ')' } else { ']' };
    format!("{open}{low},{high}{close}")
}

fn bound_text(values: &[Datum], infinity: &str) -> String {
    if values.is_empty() {
        return infinity.to_owned();
    }
    values
        .iter()
        .map(datum_go_text)
        .collect::<Vec<_>>()
        .join(" ")
}

/// A constant as Go's explain prints it: a string in double quotes, a number
/// bare.
fn datum_go_text(value: &Datum) -> String {
    match value {
        Datum::Null => "NULL".to_owned(),
        // Go's range printer spells the open-ended bounds this way.
        Datum::MaxValue => "+inf".to_owned(),
        Datum::MinNotNull => "-inf".to_owned(),
        Datum::Int(v) => v.to_string(),
        Datum::UInt(v) => v.to_string(),
        Datum::Real(v) => v.to_string(),
        Datum::Decimal(d) => d.to_string(),
        Datum::String(s) => format!("\"{}\"", String::from_utf8_lossy(s.bytes())),
        Datum::Bytes(b) => format!("\"{}\"", String::from_utf8_lossy(b)),
        other => format!("{other:?}"),
    }
}

/// Go's stats-less selectivity for one predicate, from
/// `cardinality.pseudoSelectivity`: the minimum over the conjuncts of the
/// per-operator rate, starting at `SelectivityFactor`.
fn selectivity(predicate: &tidb_ast::Expr) -> f64 {
    let mut factor = SELECTIVITY_FACTOR;
    let mut conjuncts = Vec::new();
    collect_and(predicate, &mut conjuncts);
    for conjunct in conjuncts {
        let rate = match conjunct {
            tidb_ast::Expr::Binary(op, _, _) => match op {
                tidb_ast::BinaryOp::Eq | tidb_ast::BinaryOp::NullEq => 1.0 / PSEUDO_EQUAL_RATE,
                tidb_ast::BinaryOp::Ge
                | tidb_ast::BinaryOp::Gt
                | tidb_ast::BinaryOp::Le
                | tidb_ast::BinaryOp::Lt => 1.0 / PSEUDO_LESS_RATE,
                _ => continue,
            },
            tidb_ast::Expr::In { .. } => 1.0 / PSEUDO_EQUAL_RATE,
            _ => continue,
        };
        factor = factor.min(rate);
    }
    factor
}

fn collect_and<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    if let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) = expr {
        collect_and(lhs, out);
        collect_and(rhs, out);
        return;
    }
    out.push(expr);
}

/// Renders an expression the way Go's `ExplainInfo` does: a comparison as its
/// function name (`gt(a, b)`), a column fully qualified as `db.table.column`,
/// a string constant in double quotes.
struct Qualifier<'a> {
    db: &'a str,
    scope: &'a FromScope,
}

impl Qualifier<'_> {
    fn expr(&self, expr: &tidb_ast::Expr) -> String {
        match expr {
            tidb_ast::Expr::Column(path) => self.column(path),
            tidb_ast::Expr::Int(text) => text.clone(),
            tidb_ast::Expr::Decimal(text) => text.clone(),
            tidb_ast::Expr::Float(value) => value.to_string(),
            tidb_ast::Expr::String(value) => format!("\"{value}\""),
            tidb_ast::Expr::Binary(op, lhs, rhs) => match binary_func_name(*op) {
                Some(name) => format!("{name}({}, {})", self.expr(lhs), self.expr(rhs)),
                // A shape Go prints differently is restored from the AST
                // rather than mislabelled with an invented function name.
                None => expr.restore(),
            },
            tidb_ast::Expr::Aggregate {
                name,
                distinct,
                args,
            } => {
                let rendered: Vec<String> = args.iter().map(|a| self.expr(a)).collect();
                let prefix = if *distinct { "distinct " } else { "" };
                format!(
                    "{}({prefix}{})",
                    name.to_lowercase(),
                    if rendered.is_empty() {
                        "*".to_owned()
                    } else {
                        rendered.join(", ")
                    }
                )
            }
            other => other.restore(),
        }
    }

    /// `db.table.column`, resolving an unqualified name against the scope --
    /// the qualification Go's explain always prints in full.
    fn column(&self, path: &[String]) -> String {
        match path {
            [name] => {
                let owner = self
                    .scope
                    .tables
                    .iter()
                    .find(|t| t.columns.iter().any(|(c, _)| c.eq_ignore_ascii_case(name)));
                match owner {
                    Some(table) => format!("{}.{}.{}", self.db, table.name, name),
                    None => name.clone(),
                }
            }
            [table, name] => format!("{}.{table}.{name}", self.db),
            _ => path.join("."),
        }
    }
}

/// Go's function name for a comparison operator, which is what `ExplainInfo`
/// prints instead of the infix spelling.
fn binary_func_name(op: tidb_ast::BinaryOp) -> Option<&'static str> {
    Some(match op {
        tidb_ast::BinaryOp::Eq => "eq",
        tidb_ast::BinaryOp::NullEq => "nulleq",
        tidb_ast::BinaryOp::Ge => "ge",
        tidb_ast::BinaryOp::Gt => "gt",
        tidb_ast::BinaryOp::Le => "le",
        tidb_ast::BinaryOp::Lt => "lt",
        tidb_ast::BinaryOp::Ne => "ne",
        tidb_ast::BinaryOp::LogicAnd => "and",
        tidb_ast::BinaryOp::LogicOr => "or",
        tidb_ast::BinaryOp::Plus => "plus",
        tidb_ast::BinaryOp::Minus => "minus",
        tidb_ast::BinaryOp::Mul => "mul",
        tidb_ast::BinaryOp::Div => "div",
        _ => return None,
    })
}
