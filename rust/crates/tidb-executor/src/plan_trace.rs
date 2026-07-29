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

//! The plan trace: the record [`crate::driver`] leaves behind of the
//! executors it built, which [`crate::explain`] prints.
//!
//! This tier has no plan object: the driver decides point-get vs
//! batch-point-get vs index-range vs full scan, and whether a selection /
//! sort / projection / aggregate / limit is needed, WHILE it builds the
//! executor pipeline. EXPLAIN therefore cannot ask a planner what the plan
//! is -- it has to watch the driver build one. A [`PlanTrace`] is that
//! watcher: the driver threads `Option<&mut PlanTrace>` down its build path
//! and calls one of the node constructors below at each site where it
//! commits to an operator, so a described plan and an executed plan cannot
//! drift apart -- they are the same control flow.
//!
//! Three things live here, and only here:
//!
//! 1. **The node shape and text.** Every operator's Go name, `access
//!    object`, and `operator info` is produced by one constructor
//!    ([`PlanTrace::table_full_scan`], [`PlanTrace::selection`], ...), so
//!    the printed text has a single definition.
//! 2. **The `estRows` model.** The stats-less Go constants and the
//!    arithmetic over them ([`Est`]) are applied by those same
//!    constructors, never re-derived by a caller.
//! 3. **The `actRows` counters.** In `EXPLAIN ANALYZE` mode each node
//!    carries an [`Rc<Cell<u64>>`] and the executor built at that site is
//!    wrapped in a [`CountExec`], so the number printed is the number of
//!    rows that operator really produced during the real execution. No
//!    second, mirrored traversal counts rows.
//!
//! When the driver runs with no trace (`None`), none of this costs anything:
//! every call site is an `if let Some(trace)`.

use std::cell::Cell;
use std::rc::Rc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

use crate::driver::{FromScope, FromTable};
use crate::executor::{ExecError, Executor};
use crate::kv_table::TableHandle;

/// Go `statistics.PseudoRowCount` (`pkg/statistics/table.go`): the row count
/// assumed for a table with no analyzed statistics.
pub(crate) const PSEUDO_ROW_COUNT: f64 = 10000.0;
/// Go `pseudoLessRate` (`pkg/planner/cardinality/pseudo.go`).
pub(crate) const PSEUDO_LESS_RATE: f64 = 3.0;
/// Go `pseudoEqualRate` (same file).
pub(crate) const PSEUDO_EQUAL_RATE: f64 = 1000.0;
/// Go `vardef.DefOptSelectivityFactor`, the fallback selectivity for a
/// condition the pseudo model cannot classify.
pub(crate) const SELECTIVITY_FACTOR: f64 = 0.8;
/// Go `distinctFactor` (`pkg/planner/cardinality/ndv.go`): the assumed NDV
/// ratio of a grouping key without statistics.
pub(crate) const DISTINCT_FACTOR: f64 = 0.8;

/// One node of the recorded plan, before ids are assigned.
pub(crate) struct PlanNode {
    /// Go's operator name without the `_N` suffix (`TableFullScan`).
    pub(crate) name: &'static str,
    /// The `estRows` cell; `None` prints Go's `N/A`.
    pub(crate) est_rows: Option<f64>,
    /// The `access object` cell.
    pub(crate) access: String,
    /// The `operator info` cell, up to the point a join splices its
    /// `, left side:<child>` in.
    pub(crate) info: String,
    /// A join's `, left side:` insertion point: the index in `children` of
    /// the operator that is this join's LEFT input. `None` for every node
    /// that does not print one (Go omits it for an inner join).
    pub(crate) left_side_child: Option<usize>,
    /// The rest of the `operator info` cell, after the `left side:` clause.
    pub(crate) info_tail: String,
    /// The suffix Go appends to this operator's NAME to mark its role in the
    /// parent join (`(Build)` / `(Probe)`); empty for every other node.
    pub(crate) label: &'static str,
    /// Children, in the order Go prints them (build side first for a join).
    pub(crate) children: Vec<PlanNode>,
    /// The real row count this operator produced, live while the pipeline
    /// runs. `None` outside `EXPLAIN ANALYZE`, and for an operator this tier
    /// builds but does not meter.
    pub(crate) act_rows: Option<Rc<Cell<u64>>>,
}

impl PlanNode {
    fn new(name: &'static str, est_rows: Option<f64>, access: String, info: String) -> Self {
        Self {
            name,
            est_rows,
            access,
            info,
            left_side_child: None,
            info_tail: String::new(),
            label: "",
            children: Vec::new(),
            act_rows: None,
        }
    }
}

/// How an operator's `estRows` follows from its child's.
///
/// Every estimate this tier prints is one of these four moves over the
/// stats-less constants above; naming them keeps the model in one place
/// instead of spread across the build sites that apply it.
#[derive(Clone, Copy)]
pub(crate) enum Est {
    /// The operator does not change the row count (`Sort`, `Projection`).
    Inherit,
    /// A known exact count (a point get's 1, a batch point get's handles).
    Fixed(f64),
    /// The child's estimate times a selectivity/NDV factor.
    Scale(f64),
    /// The child's estimate, capped (`LIMIT`).
    CapAt(f64),
}

impl Est {
    fn apply(self, child: Option<f64>) -> Option<f64> {
        match self {
            Est::Inherit => child,
            Est::Fixed(value) => Some(value),
            Est::Scale(factor) => child.map(|rows| rows * factor),
            Est::CapAt(cap) => child.map(|rows| rows.min(cap)),
        }
    }
}

/// The plan the driver is building, recorded as it builds it.
///
/// Nodes arrive bottom-up: a source is [`PlanTrace::push`]ed, and each
/// operator built over it [`PlanTrace::wrap`]s whatever is on top. The stack
/// holds one entry per completed subtree, so a join simply wraps two.
pub(crate) struct PlanTrace {
    /// Completed subtrees, innermost-last.
    stack: Vec<PlanNode>,
    /// `EXPLAIN ANALYZE`: meter every operator's real output.
    counting: bool,
    /// Plain `EXPLAIN`: build the pipeline, record it, and stop before
    /// draining it -- no row of the result is ever produced, and no write
    /// executes.
    plan_only: bool,
    /// Set by the access-path site when the read itself consumed the
    /// conditions that selected it, so a `Selection` above re-checks rather
    /// than filters further (and must not price them twice).
    consumed: bool,
    /// A shape the recorder cannot describe; the EXPLAIN entry point turns
    /// this into the refusal it has always answered with.
    refused: Option<&'static str>,
}

impl PlanTrace {
    /// A trace for plain `EXPLAIN`: record the build, execute nothing.
    pub(crate) fn planning() -> Self {
        Self {
            stack: Vec::new(),
            counting: false,
            plan_only: true,
            consumed: false,
            refused: None,
        }
    }

    /// A trace for `EXPLAIN ANALYZE`: record the build, meter it, and let the
    /// statement really run.
    pub(crate) fn analyzing() -> Self {
        Self {
            stack: Vec::new(),
            counting: true,
            plan_only: false,
            consumed: false,
            refused: None,
        }
    }

    /// Whether the driver must stop before draining its pipeline (and before
    /// performing any write).
    pub(crate) fn is_plan_only(&self) -> bool {
        self.plan_only
    }

    /// Records a shape this recorder has never described, for the entry
    /// point to refuse with -- EXPLAIN's surface is not widened by the fact
    /// that the driver can execute more than the recorder can print.
    pub(crate) fn refuse(&mut self, reason: &'static str) {
        if self.refused.is_none() {
            self.refused = Some(reason);
        }
    }

    /// The refusal, if any shape below could not be described.
    pub(crate) fn refusal(&self) -> Option<&'static str> {
        self.refused
    }

    /// The recorded plan. `None` when the driver committed to no operator at
    /// all (an empty trace).
    pub(crate) fn into_root(mut self) -> Option<PlanNode> {
        self.stack.pop()
    }

    fn top_est(&self) -> Option<f64> {
        self.stack.last().and_then(|node| node.est_rows)
    }

    fn push(&mut self, node: PlanNode) {
        self.stack.push(node);
    }

    /// Replaces the top subtree, for the fast paths that REPLACE the source
    /// the FROM clause built rather than sitting above it -- exactly what
    /// `run_select_stmt` does to `from_source` at the same moment.
    fn replace_top(&mut self, node: PlanNode) {
        self.stack.pop();
        self.stack.push(node);
    }

    fn wrap(&mut self, name: &'static str, est: Est, info: String) {
        let est_rows = est.apply(self.top_est());
        let child = self.stack.pop();
        let mut node = PlanNode::new(name, est_rows, String::new(), info);
        node.children.extend(child);
        self.stack.push(node);
    }

    /// Meters the executor built for the node on top of the stack: outside
    /// `EXPLAIN ANALYZE` this hands the executor straight back.
    pub(crate) fn meter(&mut self, exec: Box<dyn Executor>) -> Box<dyn Executor> {
        if !self.counting {
            return exec;
        }
        let counter = Rc::new(Cell::new(0));
        if let Some(node) = self.stack.last_mut() {
            node.act_rows = Some(Rc::clone(&counter));
        }
        Box::new(CountExec {
            child: exec,
            counter,
        })
    }

    /// Records the real row counts a single-table write's read plan produced:
    /// the rows its scan examined and, when it has a `WHERE`, the rows that
    /// predicate passed. A write has no pull-based pipeline to meter -- it
    /// scans and filters in a plain loop -- so its counts are handed over
    /// once the loop is done, still off the one real execution.
    pub(crate) fn set_dml_source_act_rows(&mut self, scanned: u64, matched: u64, has_where: bool) {
        if !self.counting {
            return;
        }
        let Some(source) = self
            .stack
            .last_mut()
            .and_then(|write| write.children.first_mut())
        else {
            return;
        };
        if !has_where {
            source.act_rows = Some(Rc::new(Cell::new(scanned)));
            return;
        }
        source.act_rows = Some(Rc::new(Cell::new(matched)));
        if let Some(scan) = source.children.first_mut() {
            scan.act_rows = Some(Rc::new(Cell::new(scanned)));
        }
    }

    /// Replaces the top node's `actRows` source with a scan's own row
    /// counter, for a scan that filters internally: `TableFullScan` reports
    /// the rows it read, not the rows a pushed predicate let through.
    pub(crate) fn set_scan_act_rows(&mut self, scanned: Rc<Cell<u64>>) {
        if !self.counting {
            return;
        }
        if let Some(node) = self.stack.last_mut() {
            node.act_rows = Some(scanned);
        }
    }
}

/// The node constructors: every operator's printed name, access object,
/// operator info and estimate, defined once.
impl PlanTrace {
    /// A `FROM`-less `SELECT`'s one virtual row.
    pub(crate) fn table_dual(&mut self) {
        self.push(PlanNode::new(
            "TableDual",
            Some(1.0),
            String::new(),
            "rows:1".to_owned(),
        ));
    }

    /// A whole-table read.
    pub(crate) fn table_full_scan(&mut self, visible: &str) {
        self.push(PlanNode::new(
            "TableFullScan",
            Some(PSEUDO_ROW_COUNT),
            format!("table:{visible}"),
            "keep order:false, stats:pseudo".to_owned(),
        ));
    }

    /// Go's `Batch_Point_Get` fast path, which REPLACES the source scan.
    pub(crate) fn batch_point_get(&mut self, visible: &str, handles: &[TableHandle]) {
        let printed: Vec<String> = handles.iter().map(handle_text).collect();
        self.replace_top(PlanNode::new(
            "Batch_Point_Get",
            Some(handles.len() as f64),
            format!("table:{visible}"),
            format!(
                "handle:[{}], keep order:false, desc:false",
                printed.join(" ")
            ),
        ));
        self.consumed = true;
    }

    /// Go's `Point_Get` fast path. `None` is a handle no row can carry --
    /// Go still plans a `Point_Get` and reads nothing.
    pub(crate) fn point_get(&mut self, visible: &str, handle: Option<&TableHandle>) {
        let printed = match handle {
            Some(handle) => handle_text(handle),
            None => "NULL".to_owned(),
        };
        self.replace_top(PlanNode::new(
            "Point_Get",
            Some(1.0),
            format!("table:{visible}"),
            format!("handle:{printed}"),
        ));
        self.consumed = true;
    }

    /// An index range read, which also REPLACES the source scan.
    pub(crate) fn index_range_scan(
        &mut self,
        visible: &str,
        index_name: &str,
        index_columns: &[&str],
        ranges: &[crate::kv_table::IndexRange],
    ) {
        let printed: Vec<String> = ranges.iter().map(range_text).collect();
        self.replace_top(PlanNode::new(
            "IndexRangeScan",
            // The ranges narrow the read, but by how much needs the
            // per-column histogram this tier has no statistics for, so the
            // estimate is the same stats-less one Go falls back to.
            Some(PSEUDO_ROW_COUNT / PSEUDO_LESS_RATE),
            format!(
                "table:{visible}, index:{index_name}({})",
                index_columns.join(", ")
            ),
            format!(
                "range:{}, keep order:false, stats:pseudo",
                printed.join(", ")
            ),
        ));
        self.consumed = true;
    }

    /// A `WHERE` over whatever the access path produced.
    ///
    /// The access path's own estimate already reflects the conditions it
    /// consumed (Go's `DetachCondAndBuildRange` split: access conditions are
    /// priced once, by the read). A selection above such a path re-checks
    /// them, so it must not multiply the estimate a second time. Only a
    /// plain full scan consumed nothing, so only there does the filter
    /// reduce the estimate -- which is exactly how Go's 10000 -> 3333.33
    /// arises.
    pub(crate) fn selection(&mut self, predicate: &tidb_ast::Expr, qualify: &Qualifier<'_>) {
        let est = if self.consumed {
            Est::Inherit
        } else {
            Est::Scale(selectivity(predicate))
        };
        self.wrap("Selection", est, qualify.expr(predicate));
    }

    /// The one-phase aggregate this tier builds for `GROUP BY` / an
    /// aggregate select field.
    pub(crate) fn hash_agg(&mut self, select: &tidb_ast::SelectStmt, qualify: &Qualifier<'_>) {
        let mut info = String::new();
        if !select.group_by.is_empty() {
            info.push_str("group by:");
            let keys: Vec<String> = select
                .group_by
                .iter()
                .map(|item| qualify.expr(&item.expr))
                .collect();
            info.push_str(&keys.join(", "));
            info.push_str(", ");
        }
        // Divergence 4: one phase, and the function as written.
        let funcs: Vec<String> = select
            .fields
            .fields()
            .iter()
            .filter_map(|field| match field {
                tidb_ast::SelectField::Expr { expr, .. } => Some(qualify.expr(expr)),
                tidb_ast::SelectField::Wildcard(_) => None,
            })
            .collect();
        info.push_str("funcs:");
        info.push_str(&funcs.join(", "));
        let est = if select.group_by.is_empty() {
            // A whole-table aggregate collapses to one row.
            Est::Fixed(1.0)
        } else {
            Est::Scale(DISTINCT_FACTOR)
        };
        self.wrap("HashAgg", est, info);
    }

    /// `ORDER BY` (divergence 2: a `Sort`, never a `TopN`).
    pub(crate) fn sort(&mut self, order_by: &[tidb_ast::OrderItem], qualify: &Qualifier<'_>) {
        let items: Vec<String> = order_by
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
        self.wrap("Sort", Est::Inherit, items.join(", "));
    }

    /// The projection the driver always builds (divergence 3).
    pub(crate) fn projection(&mut self, fields: &[tidb_ast::SelectField], qualify: &Qualifier<'_>) {
        self.wrap("Projection", Est::Inherit, field_list(fields, qualify));
    }

    /// `SELECT DISTINCT`: Go's `buildDistinct` is an aggregation grouping by
    /// every projected column, so it carries the same NDV assumption.
    pub(crate) fn distinct(&mut self, fields: &[tidb_ast::SelectField], qualify: &Qualifier<'_>) {
        let info = format!("group by:{}, funcs:firstrow", field_list(fields, qualify));
        self.wrap("HashAgg", Est::Scale(DISTINCT_FACTOR), info);
    }

    /// `LIMIT [offset,] count`, which caps the child's estimate as Go's does.
    pub(crate) fn limit(&mut self, offset: u64, count: u64) {
        self.wrap(
            "Limit",
            Est::CapAt(count as f64),
            format!("offset:{offset}, count:{count}"),
        );
    }

    /// The nested-loop join the driver builds, over the two subtrees already
    /// on the stack.
    pub(crate) fn join(
        &mut self,
        join: &tidb_ast::Join,
        scope: &FromScope,
        current_db: &str,
        equal_mask: &[bool],
        build_is_left: bool,
    ) -> Result<(), ()> {
        let qualify = Qualifier {
            db: current_db,
            scope,
        };
        let kind = match join.tp {
            tidb_ast::JoinType::Cross => "inner join",
            tidb_ast::JoinType::Left => "left outer join",
            tidb_ast::JoinType::Right => "right outer join",
        };
        // `equal_mask` comes from the executor's own condition split, so the
        // conjuncts printed under `equal:[...]` are exactly the ones the hash
        // table indexes and `other cond:` is exactly the residue it still
        // evaluates per candidate pair.
        let mut conjuncts = Vec::new();
        if let Some(expr) = &join.on {
            collect_and(expr, &mut conjuncts);
        }
        if conjuncts.len() != equal_mask.len() {
            return Err(());
        }
        let mut equal = Vec::new();
        let mut other = Vec::new();
        for (conjunct, is_equal) in conjuncts.iter().zip(equal_mask) {
            let rendered = qualify.expr(conjunct);
            if *is_equal {
                equal.push(rendered);
            } else {
                other.push(rendered);
            }
        }
        // Go `PhysicalHashJoin.explainInfo`: the `CARTESIAN` prefix marks a
        // join with NO equal condition -- the shape that has to compare
        // every pair -- and `other cond:` is a SORTED list
        // (`SortedExplainExpressionList`), while `equal:` keeps `ON` order.
        other.sort();
        let mut info = String::new();
        if equal.is_empty() {
            info.push_str("CARTESIAN ");
        }
        info.push_str(kind);
        let mut tail = String::new();
        if !equal.is_empty() {
            tail.push_str(", equal:[");
            tail.push_str(&equal.join(" "));
            tail.push(']');
        }
        if !other.is_empty() {
            tail.push_str(", other cond:");
            tail.push_str(&other.join(", "));
        }
        let (Some(mut right), Some(mut left)) = (self.stack.pop(), self.stack.pop()) else {
            return Err(());
        };
        // Go prints the BUILD child first and labels both sides
        // (`flat_plan.go`'s `BuildSide`/`ProbeSide`).
        left.label = if build_is_left { "(Build)" } else { "(Probe)" };
        right.label = if build_is_left { "(Probe)" } else { "(Build)" };
        let children = if build_is_left {
            vec![left, right]
        } else {
            vec![right, left]
        };
        self.stack.push(PlanNode {
            name: "HashJoin",
            // Divergence 6: an equi-join's cardinality needs statistics.
            est_rows: None,
            access: String::new(),
            info,
            // `explainJoinLeftSide` names the LEFT child's operator, and only
            // for an OUTER join. The name carries its own id in `format='row'`,
            // which is not assigned yet, so the renderer splices it in.
            left_side_child: (join.tp != tidb_ast::JoinType::Cross)
                .then_some(usize::from(!build_is_left)),
            info_tail: tail,
            label: "",
            children,
            act_rows: None,
        });
        Ok(())
    }

    /// The write operator itself (`Insert`/`Update`/`Delete`), over the read
    /// subtree already on the stack (if any).
    ///
    /// Go's own row carries none of the estimate/access/info a read operator
    /// would (captured: `[Insert_1 N/A root  N/A]`), and its `actRows` is
    /// `0`: the write is a side effect, not a row this operator's `Next()`
    /// produces.
    pub(crate) fn write(&mut self, name: &'static str, has_source: bool) {
        let child = if has_source { self.stack.pop() } else { None };
        let mut node = PlanNode::new(name, None, String::new(), "N/A".to_owned());
        node.children.extend(child);
        if self.counting {
            node.act_rows = Some(Rc::new(Cell::new(0)));
        }
        self.stack.push(node);
    }

    /// The scope a single-table write reads through, for qualifying its
    /// `WHERE` -- a write has no `FROM` clause to have built one.
    pub(crate) fn single_table_scope(
        visible: &str,
        database: Option<String>,
        columns: Vec<(String, FieldType)>,
    ) -> FromScope {
        FromScope {
            tables: vec![FromTable {
                name: visible.to_owned(),
                database,
                columns,
                offset: 0,
            }],
        }
    }
}

/// The select list as `operator info` prints it, `*`/`t.*` left as written.
fn field_list(fields: &[tidb_ast::SelectField], qualify: &Qualifier<'_>) -> String {
    let rendered: Vec<String> = fields
        .iter()
        .map(|field| match field {
            tidb_ast::SelectField::Expr { expr, .. } => qualify.expr(expr),
            tidb_ast::SelectField::Wildcard(path) => match path.last() {
                Some(table) => format!("{table}.*"),
                None => "*".to_owned(),
            },
        })
        .collect();
    rendered.join(", ")
}

/// Counts the rows its child really produced, without touching them.
///
/// This is how `EXPLAIN ANALYZE`'s `actRows` is measured: the trace wraps
/// the executor built at each recorded site, so the count is a byproduct of
/// the one real execution rather than a mirrored re-run of the query.
struct CountExec {
    child: Box<dyn Executor>,
    counter: Rc<Cell<u64>>,
}

impl Executor for CountExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.child.next(req)?;
        self.counter.set(self.counter.get() + req.num_rows() as u64);
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.child.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.child.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.child.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.child.new_chunk()
    }

    /// Metering must not change what runs, so the whole negotiation passes
    /// through to the real source -- otherwise `EXPLAIN ANALYZE` would
    /// measure a differently-planned query than the one it was asked about.
    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        self.child.table_access()
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
pub(crate) fn range_text(range: &crate::kv_table::IndexRange) -> String {
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
pub(crate) struct Qualifier<'a> {
    pub(crate) db: &'a str,
    pub(crate) scope: &'a FromScope,
}

impl Qualifier<'_> {
    pub(crate) fn expr(&self, expr: &tidb_ast::Expr) -> String {
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
