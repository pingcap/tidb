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
//! optimizer trace -- but it does not re-run those decisions to find out what
//! they were. It runs the driver's own build path with a
//! [`crate::plan_trace::PlanTrace`] attached: every site that commits to an
//! executor records the matching node as a byproduct of building it. The
//! described plan and the executed plan are therefore the same control flow,
//! and cannot drift apart.
//!
//! Plain `EXPLAIN` executes nothing: the trace is built in plan-only mode, so
//! the driver assembles the pipeline, records it, and returns before draining
//! it -- and a write returns before writing (`EXPLAIN INSERT` inserts no row,
//! as in Go). `EXPLAIN ANALYZE` runs the statement for real, and each node's
//! `actRows` is metered off the executor built at that site.
//!
//! This module keeps only what printing needs: the format shapes, the
//! build-order id assignment, and the tree drawing. Every operator's name,
//! estimate and info text lives in [`crate::plan_trace`], next to the
//! estimation model it applies.
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
//!    starting at 1 -- literally the order the trace recorded them in. Go's
//!    counter also advances for logical operators that optimization later
//!    removes, so `TableFullScan_4` (Go) is `TableFullScan_1` here. The NAMES
//!    are Go's.
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
//!    `Projection > Selection > Point_Get` -- and it shows that because the
//!    driver really does build all three, in that order. Because the access
//!    path already priced those conditions, the selection does not reduce the
//!    estimate again (see `PlanTrace::selection`).
//! 8. **`UPDATE`/`DELETE` always show `TableFullScan`, never `Point_Get` or
//!    `IndexRangeScan`.** Go's planner finds the same fast access paths for
//!    a write as for a `SELECT`. This tier's write drivers
//!    ([`crate::driver::run_update_in`], [`crate::driver::run_delete_in`])
//!    do not: both unconditionally scan the whole table and filter each row
//!    with the `WHERE` in a plain iterator, with no access-path selection at
//!    all. The recorder IS those functions, so a write's read plan is always
//!    `TableFullScan` (+ `Selection` for a `WHERE`), even for `WHERE
//!    <primary key> = <literal>`, where Go itself prints `Point_Get`
//!    (captured). This divergence can no longer drift: there is no second
//!    description of the write's read path left to drift from.
//!
//! # Shapes EXPLAIN refuses
//!
//! The driver executes more than this recorder has ever printed: derived
//! tables, lateral joins, `WITH` clauses, set operations. Those build sites
//! mark the trace refused rather than inventing a node, and the entry points
//! below answer with the refusal they have always answered with -- the
//! surface EXPLAIN describes is unchanged by the fact that the trace now
//! rides the real driver. Operators the driver builds but the recorder has
//! never printed (an Apply for a correlated subquery, the window stage, an
//! aggregate query's HAVING and final projection) record no node at all,
//! which is exactly the plan text this tier has always produced for them.
//!
//! # Where the estRows numbers come from
//!
//! Every value printed is a stats-less default read from Go's source, not a
//! guess, and each was confirmed against a `testkit.CreateMockStore` capture
//! of the real `EXPLAIN` output on a table with no analyzed statistics. The
//! constants and the arithmetic over them live in [`crate::plan_trace`]:
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

use std::cell::Cell;
use std::rc::Rc;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::driver::{
    run_delete_traced, run_insert_traced, run_select_traced, run_update_traced, Catalog,
    DriverError, SelectMeta,
};
use crate::plan_trace::{PlanNode, PlanTrace};

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

/// The plan a finished trace recorded, or the refusal a build site left in it.
fn recorded(trace: PlanTrace) -> Result<PlanNode, DriverError> {
    if let Some(reason) = trace.refusal() {
        return Err(DriverError::Unsupported(reason));
    }
    trace
        .into_root()
        .ok_or(DriverError::Unsupported("EXPLAIN recorded no plan"))
}

/// A `WITH` clause's CTEs are materialized before the query that reads them is
/// built, so there is no one trace to print for the pair. EXPLAIN has always
/// refused this shape, and refuses it before the driver runs anything.
fn refuse_untraced_select(select: &tidb_ast::SelectStmt) -> Result<(), DriverError> {
    if select.with.is_some() {
        return Err(DriverError::Unsupported(
            "EXPLAIN of a WITH clause is not supported yet",
        ));
    }
    Ok(())
}

/// Plans `select` and reports the plan as EXPLAIN rows, executing nothing:
/// the driver builds the pipeline, the trace records it, and it is dropped
/// undrained.
pub fn explain_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    refuse_untraced_select(select)?;
    let mut trace = PlanTrace::planning();
    run_select_traced(select, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render(recorded(trace)?, format))
}

/// `EXPLAIN ANALYZE <select>`: the same plan [`explain_select_stmt`] records,
/// but the query actually RUNS (real `EXPLAIN ANALYZE` executes the wrapped
/// statement to gather its runtime counters, confirmed by capture), and each
/// operator's `actRows` is the REAL row count that stage produced -- metered
/// by the trace on the executor it recorded, during that one execution.
/// Nothing is estimated, and nothing is run a second time to be counted.
pub fn explain_analyze_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    refuse_untraced_select(select)?;
    let mut trace = PlanTrace::analyzing();
    run_select_traced(select, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render_analyze(recorded(trace)?, format))
}

/// Plans an `INSERT` and reports the plan as EXPLAIN rows, executing nothing:
/// the driver returns before it writes. Go's `Insert_N` row carries none of
/// the estimate/access/info a read operator would (captured:
/// `[Insert_1 N/A root  N/A]`, both for a plain `VALUES` insert and for the
/// `Insert ... SELECT` form, where the select's own plan appears as
/// `Insert`'s one child).
pub fn explain_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut trace = PlanTrace::planning();
    run_insert_traced(insert, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render(recorded(trace)?, format))
}

/// `EXPLAIN ANALYZE <insert>`: unlike [`explain_insert_stmt`], this really
/// inserts the row(s) -- real `EXPLAIN ANALYZE INSERT` executes the
/// statement (captured: the table has the new row afterward). The `Insert_N`
/// node's `actRows` is always `0`: Go's own `Insert_1` row shows `actRows`
/// `0` too (captured), because the insert executor's `Next()` produces no
/// rows of its own -- the write is a side effect, not this operator's
/// row-producing interface.
///
/// An `INSERT ... SELECT` source's `actRows` is metered on the SAME run that
/// feeds the insert, so a row this statement is about to write can never be
/// counted as one it read, and the source is read exactly once.
pub fn explain_analyze_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut trace = PlanTrace::analyzing();
    run_insert_traced(insert, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render_analyze(recorded(trace)?, format))
}

/// Plans an `UPDATE` and reports the plan as EXPLAIN rows, executing nothing.
/// `Update_N`'s one child is the read the driver performs to find the rows to
/// update: a `TableFullScan`, with a `Selection` above it for the `WHERE`
/// (divergence 8).
pub fn explain_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut trace = PlanTrace::planning();
    run_update_traced(update, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render(recorded(trace)?, format))
}

/// Plans a `DELETE` and reports the plan as EXPLAIN rows, executing nothing.
/// See [`explain_update_stmt`]: `Delete_N`'s child is the same read plan.
pub fn explain_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut trace = PlanTrace::planning();
    run_delete_traced(delete, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render(recorded(trace)?, format))
}

/// `EXPLAIN ANALYZE <update>`: unlike [`explain_update_stmt`], this really
/// runs the `UPDATE` (captured: the table's rows change afterward, both for a
/// primary-key `WHERE` and an ordinary-column one). The read child's
/// `actRows` come off that same run: `TableFullScan`'s is the number of rows
/// the update's own scan examined, and `Selection`'s the number its own
/// `WHERE` passed -- both confirmed by capture. Like `Insert_1`, `Update_N`'s
/// own `actRows` is always `0` (captured): the write is a side effect, not a
/// row this operator's `Next()` produces.
pub fn explain_analyze_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut trace = PlanTrace::analyzing();
    run_update_traced(update, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render_analyze(recorded(trace)?, format))
}

/// `EXPLAIN ANALYZE <delete>`: see [`explain_analyze_update_stmt`] -- the
/// same real read-then-write shape, over the delete driver.
pub fn explain_analyze_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut trace = PlanTrace::analyzing();
    run_delete_traced(delete, catalog, current_db, ctx, Some(&mut trace))?;
    Ok(render_analyze(recorded(trace)?, format))
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

/// Like [`render`], but for `EXPLAIN ANALYZE`: each node prints the real row
/// count the trace metered on it during the execution that just finished, or
/// `N/A` for an operator the driver builds without metering.
///
/// `execution info`, `memory`, and `disk` always print `N/A`: this tier
/// collects no runtime timing, memory, or spill counters at all (captured Go
/// values for those columns are non-deterministic timings/byte counts this
/// tier has no machinery to produce, and inventing numbers for them would be
/// worse than an honest placeholder -- the same reasoning `EXPLAIN`'s own
/// `est_rows: None` -> `"N/A"` already uses for a join's cardinality).
fn render_analyze(plan: PlanNode, format: ExplainFormat) -> SelectMeta {
    let mut counter = 0;
    let plan = assign_ids(plan, &mut counter);
    let mut rows = Vec::new();
    flatten_analyze(&plan, String::new(), true, true, format, &mut rows);
    let field_type = FieldType::new(FieldTypeCode::VarString);
    let columns = EXPLAIN_ANALYZE_COLUMNS
        .iter()
        .map(|name| ((*name).to_owned(), field_type.clone()))
        .collect();
    (columns, rows)
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
    left_side_child: Option<usize>,
    info_tail: String,
    label: &'static str,
    children: Vec<IdNode>,
    /// The counter the trace metered this operator with, if it metered it.
    act_rows: Option<Rc<Cell<u64>>>,
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
        left_side_child: node.left_side_child,
        info_tail: node.info_tail,
        label: node.label,
        children,
        act_rows: node.act_rows,
    }
}

/// Go's tree drawing: the last child gets `└─`, an earlier sibling `├─`.
///
/// Divergence: `'brief'` drops the `_N` suffix Go's `'row'`/default format
/// prints (captured: `Point_Get` vs `Point_Get_1`).
fn draw_id(
    node: &IdNode,
    prefix: &str,
    is_root: bool,
    is_last: bool,
    format: ExplainFormat,
) -> String {
    let name = format!("{}{}", explain_id(node, format), node.label);
    if is_root {
        name
    } else if is_last {
        format!("{prefix}└─{name}")
    } else {
        format!("{prefix}├─{name}")
    }
}

/// A non-last child's descendants are prefixed with `│ ` so the branch line
/// continues past them.
fn child_prefix(prefix: &str, is_root: bool, is_last: bool) -> String {
    if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}  ")
    } else {
        format!("{prefix}│ ")
    }
}

/// Go `Plan.ExplainID().String()`: the operator name, carrying its `_N`
/// suffix outside `format='brief'`.
fn explain_id(node: &IdNode, format: ExplainFormat) -> String {
    match format {
        ExplainFormat::Row => format!("{}_{}", node.name, node.counter),
        ExplainFormat::Brief => node.name.to_owned(),
    }
}

/// The `operator info` cell. A join splices Go's `, left side:<operator>`
/// clause into the middle of its own info here rather than at record time,
/// because the operator that clause NAMES only has its id once the whole
/// tree is numbered.
fn info_text(node: &IdNode, format: ExplainFormat) -> String {
    let left_side = match node.left_side_child {
        Some(index) => {
            let name = node
                .children
                .get(index)
                .map_or_else(String::new, |child| explain_id(child, format));
            format!(", left side:{name}")
        }
        None => String::new(),
    };
    format!("{}{left_side}{}", node.info, node.info_tail)
}

fn est_text(est_rows: Option<f64>) -> String {
    match est_rows {
        Some(value) => format!("{value:.2}"),
        None => "N/A".to_owned(),
    }
}

fn flatten(
    node: &IdNode,
    prefix: String,
    is_root: bool,
    is_last: bool,
    format: ExplainFormat,
    out: &mut Vec<Vec<Datum>>,
) {
    out.push(vec![
        text(&draw_id(node, &prefix, is_root, is_last, format)),
        text(&est_text(node.est_rows)),
        // Divergence 1: every operator here runs in the TiDB process.
        text("root"),
        text(&node.access),
        text(&info_text(node, format)),
    ]);
    let child_prefix = child_prefix(&prefix, is_root, is_last);
    let last = node.children.len().saturating_sub(1);
    for (i, child) in node.children.iter().enumerate() {
        flatten(child, child_prefix.clone(), false, i == last, format, out);
    }
}

/// [`flatten`], plus the `actRows`/`execution info`/`memory`/`disk` columns
/// `EXPLAIN ANALYZE` adds.
fn flatten_analyze(
    node: &IdNode,
    prefix: String,
    is_root: bool,
    is_last: bool,
    format: ExplainFormat,
    out: &mut Vec<Vec<Datum>>,
) {
    let act = match &node.act_rows {
        Some(counter) => counter.get().to_string(),
        None => "N/A".to_owned(),
    };
    out.push(vec![
        text(&draw_id(node, &prefix, is_root, is_last, format)),
        text(&est_text(node.est_rows)),
        text(&act),
        text("root"),
        text(&node.access),
        text("N/A"),
        text(&info_text(node, format)),
        text("N/A"),
        text("N/A"),
    ]);
    let child_prefix = child_prefix(&prefix, is_root, is_last);
    let last = node.children.len().saturating_sub(1);
    for (i, child) in node.children.iter().enumerate() {
        flatten_analyze(child, child_prefix.clone(), false, i == last, format, out);
    }
}

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}
