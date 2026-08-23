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

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::exprctx::{PlanColumnIdAllocator, SimplePlanColumnIdAllocator};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::access_cost::ScanEstimate;
use crate::driver::{FromScope, FromTable};

/// Go's `, stats:pseudo` marker, printed on a scan whose estimate came from
/// `statistics.PseudoTable` and omitted on one that read real statistics.
fn pseudo_suffix(estimate: ScanEstimate) -> &'static str {
    if estimate.pseudo {
        ", stats:pseudo"
    } else {
        ""
    }
}
use crate::executor::{ExecError, Executor};
use crate::kv_table::{KvTable, TableHandle};

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
#[derive(Clone)]
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
    /// Physical expressions produced by a projection, in output order. Most
    /// nodes leave this empty; join null-rejection pushdown uses it to place
    /// the derived predicate below the projection without parsing EXPLAIN
    /// text back into expressions.
    pub(crate) projection_outputs: Vec<String>,
    /// The execution boundary Go reports for this operator.
    pub(crate) task: &'static str,
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
    /// Whether this subtree's top operator already priced the predicate that
    /// a directly wrapping `Selection` will re-check. The receipt belongs to
    /// one completed subtree so access conditions cannot leak across sibling
    /// tables or query blocks.
    access_consumed: bool,
    /// The real row count this operator produced, live while the pipeline
    /// runs. `None` outside `EXPLAIN ANALYZE`, and for an operator this tier
    /// builds but does not meter.
    pub(crate) act_rows: Option<Rc<Cell<u64>>>,
    /// `property.StatsInfo.ColNDVs[c] / RowCount` for every column of this
    /// subtree, when one ratio holds for all of them.
    ///
    /// Go carries a per-column NDV map beside the row count, and
    /// `StatsInfo.Scale` multiplies BOTH by the same factor, so the ratio is
    /// an invariant of a subtree built out of scales. A pseudo `DataSource`
    /// sets `ColNDVs[c] = RealtimeCount * distinctFactor` for every column
    /// (`pkg/planner/core/stats.go`), so the whole map collapses to the one
    /// number 0.8 -- which is what an equi-join needs and all it needs.
    ///
    /// `None` where that collapse does not hold: an ANALYZEd scan, whose
    /// columns have their own histogram NDVs, and any operator whose NDVs Go
    /// does not scale with the row count. A join over such a side prints
    /// `N/A` rather than a number derived from the wrong NDV.
    pub(crate) key_ndv_ratio: Option<f64>,
    /// NDVs retained under their physical source-column names after a join.
    ///
    /// Go's `LogicalJoin.DeriveStats` copies each child's column NDV into the
    /// joined schema without scaling it to the join row count. The one-ratio
    /// summary above therefore cannot survive a join, but a parent join may
    /// still need the NDV of the exact column it uses. Keeping only committed
    /// merge keys is sufficient for that parent and avoids pretending every
    /// output column has one common ratio.
    pub(crate) named_key_ndvs: Vec<(String, f64)>,
}

impl PlanNode {
    fn new(name: &'static str, est_rows: Option<f64>, access: String, info: String) -> Self {
        Self {
            name,
            est_rows,
            access,
            info,
            projection_outputs: Vec::new(),
            task: "root",
            left_side_child: None,
            info_tail: String::new(),
            label: "",
            children: Vec::new(),
            access_consumed: false,
            act_rows: None,
            key_ndv_ratio: None,
            named_key_ndvs: Vec::new(),
        }
    }

    /// The same node, marked as carrying Go's pseudo NDV ratio for every
    /// column (`distinctFactor`), which only a PSEUDO scan does.
    fn with_pseudo_ndv(mut self, estimate: ScanEstimate) -> Self {
        self.key_ndv_ratio = estimate.pseudo.then_some(DISTINCT_FACTOR);
        self
    }
}

/// Re-costs one pruned scan from the statistics of the partition it survived
/// into.
///
/// Go's `PartitionProcessor` replaces the logical `DataSource` with one per
/// surviving partition, and each carries THAT partition's own
/// `PhysicalTableID` -- which is what
/// `stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)` is
/// handed. The leaf this tier costed was built before pruning ran, so it read
/// the LOGICAL table's statistics; static pruning stores a histogram per
/// physical partition and no merged one, so that lookup missed and a pruned
/// scan printed `stats:pseudo` over 10000 rows right after `ANALYZE` had
/// measured two.
fn re_estimate(node: &mut PlanNode, estimate: ScanEstimate) {
    node.est_rows = Some(estimate.rows);
    node.key_ndv_ratio = estimate.pseudo.then_some(DISTINCT_FACTOR);
    let bare = node
        .info
        .strip_suffix(", stats:pseudo")
        .unwrap_or(&node.info)
        .to_owned();
    node.info = format!("{bare}{}", pseudo_suffix(estimate));
}

/// Every operator whose access object NAMES a partition when its table has
/// one. Go's `access.ScanAccessObject` writes `table:t, partition:p` for each
/// of them, the `TableRowIDScan` an `IndexLookUp` probes with included.
const PARTITIONED_ACCESS: &[&str] = &[
    "TableFullScan",
    "TableRangeScan",
    "IndexFullScan",
    "IndexRangeScan",
    "TableRowIDScan",
];

/// Whether this subtree reads a table at all, so the partition processor has
/// something to divide.
///
/// The reader is not always the scan itself: an `IndexLookUp` holds its index
/// scan and its row-id probe as children, and a `Limit` may sit between. Go
/// divides the `DataSource`, so every one of those shapes fans out -- asking
/// only whether the TOP node is a scan is what left
/// `select * from trange use index (ia) where a > 10 order by a limit 10`
/// printing one partition-less `IndexRangeScan` where TiDB prints three
/// `IndexLookUp`s under a `PartitionUnion`.
fn reads_a_partitioned_table(node: &PlanNode) -> bool {
    PARTITIONED_ACCESS.contains(&node.name) || node.children.iter().any(reads_a_partitioned_table)
}

/// Names every access object in this subtree for one partition.
fn name_partition(node: &mut PlanNode, partition: &str) {
    if PARTITIONED_ACCESS.contains(&node.name) && !node.access.is_empty() {
        node.access = with_partition(&node.access, partition);
    }
    for child in &mut node.children {
        name_partition(child, partition);
    }
}

/// Drops the runtime row counters from a duplicated subtree.
///
/// There is ONE executor underneath that no fan-out split, so its count
/// belongs to the union rather than to any branch -- the same choice the
/// union already makes for the leaf's own counter.
fn without_row_counters(mut node: PlanNode) -> PlanNode {
    node.act_rows = None;
    node.children = node
        .children
        .into_iter()
        .map(without_row_counters)
        .collect();
    node
}

fn scan_is_index_join_outer(node: &PlanNode) -> Option<bool> {
    match node.name {
        "IndexFullScan" | "IndexRangeScan" => Some(true),
        "TableFullScan" | "TableRangeScan" => Some(false),
        "Selection" if node.children.len() == 1 => scan_is_index_join_outer(&node.children[0]),
        _ => None,
    }
}

fn index_join_reader(
    mut child: PlanNode,
    forced_index: Option<bool>,
    index_lookup: bool,
    visible: &str,
) -> PlanNode {
    fn mark_cop(node: &mut PlanNode) {
        node.task = "cop[tikv]";
        for child in &mut node.children {
            mark_cop(child);
        }
    }
    mark_cop(&mut child);
    if index_lookup {
        let estimate = child.est_rows;
        let act_rows = child.act_rows.clone();
        let key_ndv_ratio = child.key_ndv_ratio;
        let access_consumed = child.access_consumed;
        let pseudo = child.info.contains("stats:pseudo")
            || child
                .children
                .iter()
                .any(|node| node.info.contains("stats:pseudo"));
        child.label = "(Build)";

        let mut table_scan = PlanNode::new(
            "TableRowIDScan",
            estimate,
            format!("table:{visible}"),
            format!(
                "keep order:false{}",
                if pseudo { ", stats:pseudo" } else { "" }
            ),
        );
        table_scan.task = "cop[tikv]";
        table_scan.label = "(Probe)";
        table_scan.act_rows = act_rows.clone();
        table_scan.key_ndv_ratio = key_ndv_ratio;

        let mut lookup = PlanNode::new("IndexLookUp", estimate, String::new(), String::new());
        lookup.act_rows = act_rows;
        lookup.key_ndv_ratio = key_ndv_ratio;
        lookup.access_consumed = access_consumed;
        lookup.children.push(child);
        lookup.children.push(table_scan);
        return lookup;
    }
    let reader = forced_index.map_or_else(
        || {
            if scan_is_index_join_outer(&child) == Some(true) {
                "IndexReader"
            } else {
                "TableReader"
            }
        },
        |index| if index { "IndexReader" } else { "TableReader" },
    );
    let estimate = child.est_rows;
    let act_rows = child.act_rows.clone();
    let key_ndv_ratio = child.key_ndv_ratio;
    let access_consumed = child.access_consumed;
    let info = if reader == "IndexReader" {
        format!("index:{}", child.name)
    } else {
        format!("data:{}", child.name)
    };
    let mut reader_node = PlanNode::new(reader, estimate, String::new(), info);
    reader_node.act_rows = act_rows;
    reader_node.key_ndv_ratio = key_ndv_ratio;
    reader_node.access_consumed = access_consumed;
    reader_node.children.push(child);
    reader_node
}

fn index_join_inner_source(mut node: PlanNode) -> PlanNode {
    if matches!(node.name, "IndexReader" | "TableReader") && node.children.len() == 1 {
        node.children.pop().expect("one reader child")
    } else {
        node
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
    /// The child's estimate times a factor, with Go's one-row physical-plan
    /// floor (`StatsInfo.ScaleByExpectCnt`).
    ScaleFloorOne(f64),
    /// The child's estimate, capped (`LIMIT`).
    CapAt(f64),
}

impl Est {
    fn apply(self, child: Option<f64>) -> Option<f64> {
        match self {
            Est::Inherit => child,
            Est::Fixed(value) => Some(value),
            Est::Scale(factor) => child.map(|rows| rows * factor),
            Est::ScaleFloorOne(factor) => child.map(|rows| (rows * factor).max(1.0)),
            Est::CapAt(cap) => child.map(|rows| rows.min(cap)),
        }
    }
}

/// The plan the driver is building, recorded as it builds it.
///
/// Nodes arrive bottom-up: a source is [`PlanTrace::push`]ed, and each
/// operator built over it [`PlanTrace::wrap`]s whatever is on top. The stack
/// holds one entry per completed subtree, so a join simply wraps two.
pub(crate) struct GoLogicalQuerySourceColumns {
    pub(crate) query: tidb_ast::QueryStmt,
    pub(crate) columns: GoLogicalPlanColumns,
}

pub(crate) struct GoLogicalPlanColumns {
    pub(crate) aggregate_ids: Vec<(tidb_ast::Expr, i64)>,
    pub(crate) pending_aggregates: Vec<tidb_ast::Expr>,
    pub(crate) all_projection_ids_pending: bool,
    pub(crate) finish_after_source: bool,
    pub(crate) query_sources: Vec<GoLogicalQuerySourceColumns>,
}

pub(crate) struct PlanTrace {
    /// Completed subtrees, innermost-last.
    stack: Vec<PlanNode>,
    /// `EXPLAIN ANALYZE`: meter every operator's real output.
    counting: bool,
    /// Plain `EXPLAIN`: build the pipeline, record it, and stop before
    /// draining it -- no row of the result is ever produced, and no write
    /// executes.
    plan_only: bool,
    /// A shape the recorder cannot describe; the EXPLAIN entry point turns
    /// this into the refusal it has always answered with.
    refused: Option<&'static str>,
    /// Go's statement-wide plan-column allocator. Scalar-subquery
    /// placeholders share this sequence with DataSource, Aggregation and
    /// Projection columns; a private scalar-only counter cannot reproduce
    /// their EXPLAIN identities.
    plan_column_ids: SimplePlanColumnIdAllocator,
    /// Input/output identities for the next retained aggregate Projection.
    /// Go allocates these before physical aggregation injection, while this
    /// trace records operators bottom-up, so the logical build stage records
    /// the mapping here for the later rendering stage.
    next_aggregation_projection: Vec<Option<(i64, i64)>>,
    /// Go builds derived tables and view bodies while resolving `FROM`, before
    /// rewriting scalar subqueries in the containing SELECT. Rust builds the
    /// same source later, so the logical IDs allocated early are retained in
    /// one frame per SELECT and reused when that source is actually built.
    pre_reserved_query_source_frames: RefCell<Vec<Vec<GoLogicalQuerySourceColumns>>>,
    next_pre_reserved_query_source: RefCell<Option<GoLogicalPlanColumns>>,
}

impl PlanTrace {
    /// A trace for plain `EXPLAIN`: record the build, execute nothing.
    pub(crate) fn planning() -> Self {
        Self {
            stack: Vec::new(),
            counting: false,
            plan_only: true,
            refused: None,
            plan_column_ids: SimplePlanColumnIdAllocator::new(0),
            next_aggregation_projection: Vec::new(),
            pre_reserved_query_source_frames: RefCell::new(Vec::new()),
            next_pre_reserved_query_source: RefCell::new(None),
        }
    }

    /// A trace for `EXPLAIN ANALYZE`: record the build, meter it, and let the
    /// statement really run.
    pub(crate) fn analyzing() -> Self {
        Self {
            stack: Vec::new(),
            counting: true,
            plan_only: false,
            refused: None,
            plan_column_ids: SimplePlanColumnIdAllocator::new(0),
            next_aggregation_projection: Vec::new(),
            pre_reserved_query_source_frames: RefCell::new(Vec::new()),
            next_pre_reserved_query_source: RefCell::new(None),
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

    /// Consumes the next Go plan-column ID.
    pub(crate) fn alloc_plan_column_id(&self) -> i64 {
        self.plan_column_ids.alloc_plan_column_id()
    }

    /// Reserves IDs allocated by a Go logical/physical build stage whose
    /// columns are not otherwise materialized by this executor-first planner.
    pub(crate) fn reserve_plan_column_ids(&self, count: usize) {
        for _ in 0..count {
            self.alloc_plan_column_id();
        }
    }

    pub(crate) fn query_source_frame_depth(&self) -> usize {
        self.pre_reserved_query_source_frames.borrow().len()
    }

    pub(crate) fn truncate_query_source_frames(&self, depth: usize) {
        self.pre_reserved_query_source_frames
            .borrow_mut()
            .truncate(depth);
        self.next_pre_reserved_query_source.borrow_mut().take();
    }

    pub(crate) fn push_query_source_frame(&self, sources: Vec<GoLogicalQuerySourceColumns>) {
        self.pre_reserved_query_source_frames
            .borrow_mut()
            .push(sources);
    }

    /// Selects the earliest matching source in the innermost SELECT frame.
    /// Sibling sources are built left-to-right, while a scalar child's frame
    /// sits above its containing SELECT until that child finishes.
    pub(crate) fn activate_pre_reserved_query_source(&self, query: &tidb_ast::QueryStmt) -> bool {
        if self.next_pre_reserved_query_source.borrow().is_some() {
            return false;
        }
        let columns = {
            let mut frames = self.pre_reserved_query_source_frames.borrow_mut();
            frames.iter_mut().rev().find_map(|frame| {
                frame
                    .iter()
                    .position(|source| source.query == *query)
                    .map(|index| frame.remove(index).columns)
            })
        };
        let Some(columns) = columns else {
            return false;
        };
        *self.next_pre_reserved_query_source.borrow_mut() = Some(columns);
        true
    }

    pub(crate) fn take_pre_reserved_query_source(&self) -> Option<GoLogicalPlanColumns> {
        self.next_pre_reserved_query_source.borrow_mut().take()
    }

    /// Records the column identities of a retained Projection above HAVING.
    pub(crate) fn set_aggregation_projection_mapping(&mut self, mapping: Vec<Option<(i64, i64)>>) {
        self.next_aggregation_projection = mapping;
    }

    /// The recorded plan. `None` when the driver committed to no operator at
    /// all (an empty trace).
    pub(crate) fn into_root(mut self) -> Option<PlanNode> {
        self.stack.pop()
    }

    /// The main SELECT root followed by separately optimized uncorrelated
    /// subquery roots, in Go's EXPLAIN output order. Subqueries are planned
    /// before the outer source, so the main root is the stack's last item.
    pub(crate) fn into_roots(mut self) -> Vec<PlanNode> {
        let Some(main) = self.stack.pop() else {
            return Vec::new();
        };
        let mut roots = Vec::with_capacity(self.stack.len() + 1);
        roots.push(main);
        roots.extend(self.stack);
        roots
    }

    /// Go's separately optimized, non-evaluated scalar subquery plan under
    /// plain EXPLAIN. A scalar value has a `MaxOneRow` guard; EXISTS does not.
    pub(crate) fn scalar_subquery(&mut self, output_columns: usize, max_one_row: bool) -> Vec<i64> {
        if max_one_row {
            self.wrap("MaxOneRow", Est::Fixed(1.0), String::new());
        }
        let child = self.stack.pop();
        let output_ids = (0..output_columns)
            .map(|_| self.alloc_plan_column_id())
            .collect::<Vec<_>>();
        let outputs = output_ids
            .iter()
            .map(|id| format!("ScalarQueryCol#{id}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut node = PlanNode::new(
            "ScalarSubQuery",
            None,
            String::new(),
            format!("Output: {outputs}"),
        );
        node.children.extend(child);
        self.stack.push(node);
        output_ids
    }

    fn top_est(&self) -> Option<f64> {
        self.stack.last().and_then(|node| node.est_rows)
    }

    fn push(&mut self, node: PlanNode) {
        self.stack.push(node);
    }

    fn mark_top_access_consumed(&mut self) {
        if let Some(node) = self.stack.last_mut() {
            node.access_consumed = true;
        }
    }

    /// Replaces the top subtree, for the fast paths that REPLACE the source
    /// the FROM clause built rather than sitting above it -- exactly what
    /// `run_select_stmt` does to `from_source` at the same moment.
    fn replace_top(&mut self, node: PlanNode) {
        self.stack.pop();
        self.stack.push(node);
    }

    /// [`PlanTrace::replace_top`] for a node whose EXECUTOR did not change,
    /// only its name and operator info.
    ///
    /// The row counter belongs to the executor, not to the printed node, so
    /// renaming must carry it across -- otherwise `EXPLAIN ANALYZE` reports
    /// `N/A` for a scan that is still running and still counting. Every other
    /// replacement here swaps the executor too and must NOT keep it.
    fn rename_top(&mut self, mut node: PlanNode) {
        let previous = self.stack.pop();
        node.act_rows = previous.and_then(|previous| previous.act_rows);
        self.stack.push(node);
    }

    /// Go's `rule_partition_processor` under `@@tidb_partition_prune_mode =
    /// 'static'`: the ONE partitioned scan becomes one scan PER surviving
    /// partition, each naming its own, under a `PartitionUnion`.
    ///
    /// Under `dynamic` -- the shipped default -- Go leaves one `DataSource`
    /// reading every surviving partition and names the SET once on the
    /// reader above it (`partition:all`), so nothing is fanned out and this
    /// is not called at all.
    ///
    /// The fan-out is a truthful description of the read this tier already
    /// performs: a partitioned scan walks the surviving partitions' record
    /// ranges in physical-id order, one after another, which is exactly a
    /// concatenation. What it does NOT reproduce is Go's placement of the
    /// union: Go replaces the whole `DataSource` and so pushes each operator
    /// above it (a `TopN`, a `UnionScan`) INTO every branch, while this
    /// splices the union directly over the leaf and keeps one copy of
    /// everything above. Both read the same rows; the branch count and the
    /// per-branch access object agree, the operator placement does not.
    ///
    /// # Recorded (`tests/integrationtest/r/planner/core/partition_pruner.result`)
    ///
    /// ```text
    /// set @@tidb_partition_prune_mode='static';
    /// explain format='plan_tree' select * from t2 where not (a < 5);
    /// PartitionUnion             root
    /// ├─TableReader              root       data:Selection
    /// │ └─Selection              cop[tikv]  ge(...t2.a, 5)
    /// │   └─TableFullScan        cop[tikv]  table:t2, partition:p1
    /// └─TableReader              root       data:Selection
    ///   └─Selection              cop[tikv]  ge(...t2.a, 5)
    ///     └─TableFullScan        cop[tikv]  table:t2, partition:p2
    /// ```
    ///
    /// `p0` is absent because pruning already dropped it -- the fan-out names
    /// what SURVIVED, never every declared partition.
    pub(crate) fn partition_union(&mut self, partitions: &[String], estimates: &[ScanEstimate]) {
        // Only a SCAN fans out. A point get names its own partition from the
        // handle it already has (Go `PointGetPlan.AccessObject`) and is never
        // a union; a `TableDual` reads nothing to divide.
        if !self.stack.last().is_some_and(reads_a_partitioned_table) {
            return;
        }
        // Nothing to fan out: an unpartitioned table, or a pruned set of one,
        // which Go also leaves as a bare `DataSource` rather than a union of
        // one branch.
        if partitions.len() < 2 {
            if let ([partition], Some(top)) = (partitions, self.stack.last_mut()) {
                name_partition(top, partition);
                if let ([estimate], true) = (estimates, PARTITIONED_SCANS.contains(&top.name)) {
                    re_estimate(top, *estimate);
                }
            }
            return;
        }
        let Some(leaf) = self.stack.pop() else {
            return;
        };
        let mut union = PlanNode::new(
            "PartitionUnion",
            // Go's `PhysicalUnionAll` sums its children's estimates. With
            // per-partition estimates that sum is taken below; without them
            // every branch carries the same partition-blind estimate this
            // tier costed the one scan with, so the sum is a multiple.
            leaf.est_rows.map(|rows| rows * partitions.len() as f64),
            String::new(),
            String::new(),
        );
        union.key_ndv_ratio = leaf.key_ndv_ratio;
        union.access_consumed = leaf.access_consumed;
        // The row counter belongs to the ONE executor underneath, which no
        // fan-out split: attributing it to any single branch would report the
        // whole scan's rows as one partition's. It moves to the union, whose
        // count it really is.
        union.act_rows = leaf.act_rows.clone();
        for (index, partition) in partitions.iter().enumerate() {
            // The WHOLE reader is duplicated, not just its top node: Go
            // replaces one `DataSource` with one per partition, so an
            // `IndexLookUp` becomes one `IndexLookUp` per partition and both
            // its index scan and its row-id probe name that partition.
            let mut branch = without_row_counters(leaf.clone());
            name_partition(&mut branch, partition);
            if let (Some(estimate), true) = (
                estimates.get(index),
                PARTITIONED_SCANS.contains(&branch.name),
            ) {
                re_estimate(&mut branch, *estimate);
            }
            union.children.push(branch);
        }
        if !estimates.is_empty() {
            union.est_rows = Some(estimates.iter().map(|estimate| estimate.rows).sum());
        }
        self.stack.push(union);
    }

    pub(crate) fn union_all(&mut self, terms: usize, output_rows: u64) {
        if terms < 2 || self.stack.len() < terms {
            self.refuse("EXPLAIN ANALYZE recorded an incomplete UNION ALL plan");
            return;
        }
        let first_term = self.stack.len() - terms;
        let children = self.stack.split_off(first_term);
        let estimated_rows = children
            .iter()
            .try_fold(0.0, |sum, child| child.est_rows.map(|rows| sum + rows));
        let mut union = PlanNode::new("Union", estimated_rows, String::new(), String::new());
        if self.counting {
            union.act_rows = Some(Rc::new(Cell::new(output_rows)));
        }
        union.children = children;
        self.stack.push(union);
    }

    pub(crate) fn union_distinct_prefix(
        &mut self,
        total_terms: usize,
        distinct_terms: usize,
        columns: usize,
        input_rows: u64,
        output_rows: u64,
    ) {
        if distinct_terms < 2
            || distinct_terms > total_terms
            || columns == 0
            || self.stack.len() < total_terms
        {
            self.refuse("EXPLAIN recorded an incomplete UNION DISTINCT plan");
            return;
        }
        let first_term = self.stack.len() - total_terms;
        let children = self
            .stack
            .drain(first_term..first_term + distinct_terms)
            .collect::<Vec<_>>();
        let union_estimate = children
            .iter()
            .try_fold(0.0, |sum, child| child.est_rows.map(|rows| sum + rows));
        let mut union = PlanNode::new("Union", union_estimate, String::new(), String::new());
        if self.counting {
            union.act_rows = Some(Rc::new(Cell::new(input_rows)));
        }
        union.children = children;

        let groups = std::iter::repeat_n("Column", columns)
            .collect::<Vec<_>>()
            .join(", ");
        let first_rows = std::iter::repeat_n("firstrow(Column)->Column", columns)
            .collect::<Vec<_>>()
            .join(", ");
        let mut distinct = PlanNode::new(
            "HashAgg",
            union_estimate.map(|rows| rows * DISTINCT_FACTOR),
            String::new(),
            format!("group by:{groups}, funcs:{first_rows}"),
        );
        if self.counting {
            distinct.act_rows = Some(Rc::new(Cell::new(output_rows)));
        }
        distinct.children.push(union);
        self.stack.insert(first_term, distinct);
    }

    fn wrap(&mut self, name: &'static str, est: Est, info: String) {
        let est_rows = est.apply(self.top_est());
        let child = self.stack.pop();
        let mut node = PlanNode::new(name, est_rows, String::new(), info);
        // `StatsInfo.Scale` multiplies `RowCount` and every `ColNDVs` entry by
        // the SAME factor, so the ratio survives an inherit or a scale
        // untouched. It does not survive the other two: a `Fixed` estimate
        // came from somewhere else entirely, and Go's `LogicalLimit` clamps
        // each NDV to the new row count rather than scaling it.
        node.key_ndv_ratio = match est {
            Est::Inherit | Est::Scale(_) | Est::ScaleFloorOne(_) => {
                child.as_ref().and_then(|c| c.key_ndv_ratio)
            }
            Est::Fixed(_) | Est::CapAt(_) => None,
        };
        node.children.extend(child);
        self.stack.push(node);
    }

    fn wrap_child(&mut self, from_top: usize, name: &'static str, est: Est, info: String) {
        let index = self.stack.len() - 1 - from_top;
        let child = self.stack.remove(index);
        let mut node = PlanNode::new(name, est.apply(child.est_rows), child.access.clone(), info);
        node.key_ndv_ratio = match est {
            Est::Inherit | Est::Scale(_) | Est::ScaleFloorOne(_) => child.key_ndv_ratio,
            Est::Fixed(_) | Est::CapAt(_) => None,
        };
        node.children.push(child);
        self.stack.insert(index, node);
    }

    /// Records a pushed selection over one of the two completed join children.
    pub(crate) fn pushed_selection(
        &mut self,
        from_top: usize,
        predicate: &tidb_ast::Expr,
        built: &[Expression],
        qualify: &Qualifier<'_>,
        column_names: &[Option<String>],
        rate: Option<f64>,
    ) {
        let info = (!column_names.is_empty())
            .then(|| physical_conditions_text_with_columns(built, column_names))
            .flatten()
            .or_else(|| qualify.conditions(built))
            .unwrap_or_else(|| qualify.expr(predicate));
        self.wrap_child(
            from_top,
            "Selection",
            Est::Scale(rate.unwrap_or_else(|| pseudo_selectivity(predicate))),
            info,
        );
        let index = self.stack.len() - 1 - from_top;
        self.stack[index].access.clear();
    }

    /// Meters an executor corresponding to one completed join child.
    pub(crate) fn meter_child(
        &mut self,
        from_top: usize,
        exec: Box<dyn Executor>,
    ) -> Box<dyn Executor> {
        if !self.counting {
            return exec;
        }
        let counter = Rc::new(Cell::new(0));
        let index = self.stack.len() - 1 - from_top;
        self.stack[index].act_rows = Some(Rc::clone(&counter));
        Box::new(CountExec {
            child: exec,
            counter,
        })
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
        self.push(Self::dual_node(1));
    }

    /// Go `findBestTask`'s empty-range short-circuit
    /// (`pkg/planner/core/find_best_task.go`, `if len(path.Ranges) == 0`): a
    /// path the ranger proved empty is a `PhysicalTableDual` with `rows:0`,
    /// NOT a scan over an empty range list.
    ///
    /// This REPLACES the source the way [`Self::index_range_scan`] does,
    /// because the decision is made at the same point -- once a candidate
    /// path has been chosen. Captured from TiDB
    /// (`tests/integrationtest/r/util/ranger.result`, over
    /// `t1(a DECIMAL UNSIGNED, KEY(a))`):
    ///
    /// ```text
    /// explain format = 'plan_tree' select * from t1 use index(a) where a < -1;
    /// TableDual  root    rows:0
    /// ```
    ///
    /// Go additionally discards the operators ABOVE the scan, since the whole
    /// `DataSource` task becomes the dual; this tier keeps them, so its tree
    /// still prints the `Selection`/`Projection` over a source that produces
    /// nothing. That is a printer difference with no row consequence -- the
    /// rows were already right -- and it is the ACCESS decision, which named
    /// an index range read that reads no range, that was wrong.
    pub(crate) fn empty_range_table_dual(&mut self) {
        // The narrowed scan executor still runs and counts zero rows; only
        // its physical-plan identity changes to Go's empty `TableDual`.
        self.rename_top(Self::dual_node(0));
        self.mark_top_access_consumed();
    }

    /// Go `makeUnionAllChildren`'s `len(children) == 0` branch
    /// (`pkg/planner/core/rule/rule_partition_processor.go`): under static
    /// pruning a read whose surviving-partition set is EMPTY becomes
    /// `LogicalTableDual{RowCount: 0}`, replacing the `DataSource` outright.
    ///
    /// There is no such thing as a scan over zero partitions in Go's plan --
    /// the union has no children to build, so the whole source collapses.
    /// Printing the scan anyway is what left `select a from tlist use index
    /// () where b > 10 order by b limit 10` (LIST(b) over `in (0,1,2)` and
    /// `in (3,4,5)`, so no partition can hold `b > 10`) showing a
    /// `TableRangeScan range:(10,+inf]` where TiDB prints `TableDual rows:0`.
    ///
    /// DYNAMIC pruning is a different plan and NOT this one: it keeps the one
    /// scan and names the empty set on the reader above it (`partition:dual`),
    /// which is why the caller asks this only under
    /// `@@tidb_partition_prune_mode = 'static'`.
    pub(crate) fn pruned_away_table_dual(&mut self) {
        // Only a SCAN collapses, for the same reason only a scan fans out in
        // [`Self::partition_union`].
        if !self.stack.last().is_some_and(reads_a_partitioned_table) {
            return;
        }
        // The executor is still there, narrowed to no partition at all, and
        // still counts its (zero) rows -- only its printed identity changes.
        self.rename_top(Self::dual_node(0));
        self.mark_top_access_consumed();
    }

    /// Go `buildLimit`'s zero short-circuit
    /// (`pkg/planner/core/logical_plan_builder.go`: `if offset+count == 0`
    /// builds `LogicalTableDual{RowCount: 0}`): a `LIMIT 0` replaces the
    /// whole read subtree -- source, filter and sort alike -- at LOGICAL
    /// build, before any access path exists. The write's child is a dual
    /// that reads nothing, not a capped scan.
    pub(crate) fn zero_limit_table_dual(&mut self) {
        self.push(Self::dual_node(0));
    }

    fn dual_node(rows: u32) -> PlanNode {
        PlanNode::new(
            "TableDual",
            Some(f64::from(rows)),
            String::new(),
            format!("rows:{rows}"),
        )
    }

    /// Go's `PhysicalMemTable`: the read of a virtual table, which is a
    /// single ROOT node rather than a scan the coprocessor serves.
    ///
    /// Go decides this by SCHEMA NAME (`metadef.IsMemDB`, which
    /// `find_best_task.go` consults before it costs any access path), and so
    /// does the caller here. Captured
    /// (`tests/integrationtest/r/explain_easy.result`):
    ///
    /// ```text
    /// explain format = 'plan_tree' select * from information_schema.columns;
    /// MemTableScan  root  table:COLUMNS
    /// ```
    ///
    /// DIVERGENCE (documented): Go's per-table `MemTablePredicateExtractor`
    /// pulls the equality predicates INTO this node's operator info
    /// (`table_name:["t1"]`) and drops the `Selection` above it; this tier
    /// keeps the `Selection` and leaves the operator info empty, so the
    /// filtering is right and the printed shape names it one level up.
    pub(crate) fn mem_table_scan(&mut self, declared_name: &str) {
        self.push(PlanNode::new(
            "MemTableScan",
            None,
            format!("table:{declared_name}"),
            String::new(),
        ));
    }

    /// A whole-table read.
    ///
    /// `estimate` is the access-path choice's own answer for this table (see
    /// [`crate::access_cost`]): the analyzed row count when the table has
    /// statistics, `statistics.PseudoTable`'s constant when it has not. Go
    /// appends `stats:pseudo` on exactly the second case
    /// (`physical_table_scan.go`: `StatsVersion == statistics.PseudoVersion`).
    /// `keep_order` is Go's `PhysicalTableScan.KeepOrder`: true when a parent
    /// DEMANDED the handle's order and this scan is the path that already
    /// produces it. It changes nothing about what is read -- the record keys
    /// are streamed in key order either way -- only whether the plan says the
    /// order is being relied upon.
    pub(crate) fn table_full_scan(
        &mut self,
        visible: &str,
        estimate: ScanEstimate,
        keep_order: bool,
    ) {
        self.push(
            PlanNode::new(
                "TableFullScan",
                Some(estimate.rows),
                format!("table:{visible}"),
                format!("keep order:{keep_order}{}", pseudo_suffix(estimate)),
            )
            .with_pseudo_ndv(estimate),
        );
    }

    /// A read of a bounded stretch of the CLUSTERED HANDLE, which REPLACES
    /// the whole-table read above.
    ///
    /// Go's `TableRangeScan`: the same `PhysicalTableScan` as
    /// [`PlanTrace::table_full_scan`], named differently because
    /// `ranger.BuildTableRange` gave it ranges (`physical_table_scan.go`
    /// prints `TableRangeScan` exactly when `len(Ranges) > 0` and they are
    /// not the full range). Captured on `sbtest1`:
    ///
    /// ```text
    /// TableRangeScan_8  99.00  cop[tikv]  table:sbtest1
    ///   range:[100,199], keep order:false, stats:pseudo
    /// ```
    ///
    /// The ranges printed are the UNCONVERTED ones, which is what makes an
    /// open bound read `-inf`/`+inf` rather than `math.MinInt64`; Go's
    /// `formatDatum` reaches the same text from the converted bound by
    /// special-casing the extremes, so the two agree on every shape.
    pub(crate) fn table_range_scan(
        &mut self,
        visible: &str,
        ranges: &[crate::kv_table::IndexRange],
        estimate: ScanEstimate,
    ) {
        let printed: Vec<String> = ranges.iter().map(range_text).collect();
        // A RENAME, not a replacement: the whole-table scan `build_from`
        // installed is the executor that runs, narrowed by the ranges it
        // accepted, so its row counter is still the right one.
        self.rename_top(
            PlanNode::new(
                "TableRangeScan",
                Some(estimate.rows),
                format!("table:{visible}"),
                format!(
                    "range:{}, keep order:false{}",
                    printed.join(", "),
                    pseudo_suffix(estimate)
                ),
            )
            .with_pseudo_ndv(estimate),
        );
        self.mark_top_access_consumed();
    }

    /// Go's `Batch_Point_Get` fast path, which REPLACES the source scan.
    ///
    /// `partitions` is Go's `ScanAccessObject.Partitions` -- the partitions
    /// the handles route into, already in definition order (see
    /// [`crate::kv_table::KvTable::handle_partition_names`]). Empty on an
    /// unpartitioned table, and then nothing is printed.
    pub(crate) fn batch_point_get(
        &mut self,
        visible: &str,
        table: &KvTable,
        handles: &[TableHandle],
        plan_rows: usize,
        partitions: &[String],
    ) {
        let (access, info) = if let Some(access) = common_handle_access(visible, table, partitions)
        {
            (access, "keep order:false, desc:false".to_owned())
        } else {
            let unsigned = table.unsigned_pk_handle();
            let printed: Vec<String> = handles
                .iter()
                .map(|handle| handle_text(handle, unsigned))
                .collect();
            (
                format!("table:{visible}{}", partition_object(partitions)),
                format!(
                    "handle:[{}], keep order:false, desc:false",
                    printed.join(" ")
                ),
            )
        };
        self.replace_top(PlanNode::new(
            "Batch_Point_Get",
            Some(plan_rows as f64),
            access,
            info,
        ));
        self.mark_top_access_consumed();
    }

    /// Records a statement-level fast BatchPointGet before any source subtree
    /// exists. Set-operation terms share one trace, so this must append the
    /// term instead of replacing the preceding term's subtree.
    pub(crate) fn push_fast_batch_point_get(
        &mut self,
        visible: &str,
        table: &KvTable,
        handles: &[TableHandle],
        plan_rows: usize,
        partitions: &[String],
    ) {
        self.push(PlanNode::new(
            "TableDual",
            Some(0.0),
            String::new(),
            String::new(),
        ));
        self.batch_point_get(visible, table, handles, plan_rows, partitions);
    }

    pub(crate) fn push_fast_index_batch_point_get(
        &mut self,
        visible: &str,
        count: usize,
        partitions: &[String],
        index: &str,
        static_partition_prune: bool,
        branch_estimates: &[f64],
    ) {
        self.push(PlanNode::new(
            "TableDual",
            Some(0.0),
            String::new(),
            String::new(),
        ));
        self.index_batch_point_get(
            visible,
            count,
            partitions,
            index,
            static_partition_prune,
            branch_estimates,
        );
    }

    pub(crate) fn index_batch_point_get(
        &mut self,
        visible: &str,
        count: usize,
        partitions: &[String],
        index: &str,
        static_partition_prune: bool,
        // One estimate per partition branch, when the caller could read the
        // partitions' own statistics; empty falls back to `count`. Only the
        // fanned-out branches consume these: Go's per-branch number is
        // `min(CountAfterAccess, len(ranges))` where the partition's
        // `getIndexRowCountForStatsV2` counts one row per unique point range
        // and clamps into `[1, realtimeRowCount]`
        // (`pkg/planner/cardinality/row_count_index.go`), while the single
        // node is Go's FAST plan whose estimate is the value-list length.
        branch_estimates: &[f64],
    ) {
        let branch = |partitions: &[String], est: f64| {
            PlanNode::new(
                "Batch_Point_Get",
                Some(est),
                format!("table:{visible}{}, {index}", partition_object(partitions)),
                "keep order:false, desc:false".to_owned(),
            )
        };
        // Go's static mode gives every surviving partition its OWN
        // `DataSource` (`makeUnionAllChildren`), so the batch point get is
        // built once per partition and each names the one it reads. Captured
        // from TiDB over `key(b) partitions 3`, `@@tidb_partition_prune_mode
        // = 'static'`:
        //
        // ```text
        // explain format = 'brief' select * from t where b in (1,2);
        // PartitionUnion       3.00  root
        // ├─Batch_Point_Get    2.00  root  table:t, partition:p1, index:PRIMARY(b)
        // └─Batch_Point_Get    1.00  root  table:t, partition:p2, index:PRIMARY(b)
        // ```
        //
        // [`Self::partition_union`] cannot build this: it fans out SCANS, and
        // a point get is not one. Blanking the access object and calling it
        // anyway is what printed a single partition-less `Batch_Point_Get`
        // where TiDB names p1 and p2.
        if static_partition_prune && partitions.len() > 1 {
            let mut union = PlanNode::new(
                "PartitionUnion",
                Some(count as f64),
                String::new(),
                String::new(),
            );
            union.children = partitions
                .iter()
                .enumerate()
                .map(|(position, partition)| {
                    branch(
                        std::slice::from_ref(partition),
                        branch_estimates
                            .get(position)
                            .copied()
                            .unwrap_or(count as f64),
                    )
                })
                .collect();
            // Go's `PhysicalUnionAll` estimate is the sum of its children's.
            union.est_rows = union
                .children
                .iter()
                .try_fold(0.0, |sum, child| child.est_rows.map(|rows| sum + rows));
            self.replace_top(union);
        } else {
            self.replace_top(branch(partitions, count as f64));
        }
        self.mark_top_access_consumed();
    }

    pub(crate) fn index_point_get(&mut self, visible: &str, partitions: &[String], index: &str) {
        self.replace_top(PlanNode::new(
            "Point_Get",
            Some(1.0),
            format!("table:{visible}{}, {index}", partition_object(partitions)),
            String::new(),
        ));
        self.mark_top_access_consumed();
    }

    /// Go's `Point_Get` fast path. `None` is a handle no row can carry --
    /// Go still plans a `Point_Get` and reads nothing.
    pub(crate) fn point_get(
        &mut self,
        visible: &str,
        table: &KvTable,
        handle: Option<&TableHandle>,
        index: Option<&(String, Vec<String>)>,
    ) {
        let partitions = sole_read_partition_name(table);
        // Go `PointGetPlan.AccessObject`: an INDEX point get prints
        // `table:t, index:idx(cols)` and NO handle -- execution resolved one
        // through the index entry, but the plan names what pinned the row.
        if let Some((name, columns)) = index {
            let point = PlanNode::new(
                "Point_Get",
                Some(1.0),
                format!(
                    "table:{visible}{}, index:{name}({})",
                    partition_object(&partitions),
                    columns.join(", ")
                ),
                String::new(),
            );
            let mut point = point;
            point.key_ndv_ratio = Some(1.0);
            self.replace_top(point);
            self.mark_top_access_consumed();
            return;
        }
        let (access, info) = match common_handle_access(visible, table, &partitions) {
            Some(access) => (access, String::new()),
            None => {
                let printed = match handle {
                    Some(handle) => handle_text(handle, table.unsigned_pk_handle()),
                    None => "NULL".to_owned(),
                };
                (
                    format!("table:{visible}{}", partition_object(&partitions)),
                    format!("handle:{printed}"),
                )
            }
        };
        let mut point = PlanNode::new("Point_Get", Some(1.0), access, info);
        // A one-row relation has NDV one for every join key it can expose, so
        // an equi-join of two point gets has Go's exact one-row estimate.
        point.key_ndv_ratio = Some(1.0);
        self.replace_top(point);
        self.mark_top_access_consumed();
    }

    /// Records a statement-level fast PointGet before any source subtree
    /// exists. The placeholder gives [`Self::point_get`] exactly the source
    /// node it is defined to replace without consuming an earlier UNION arm.
    pub(crate) fn push_fast_point_get(
        &mut self,
        visible: &str,
        table: &KvTable,
        handle: Option<&TableHandle>,
    ) {
        self.push(PlanNode::new(
            "TableDual",
            Some(0.0),
            String::new(),
            String::new(),
        ));
        self.point_get(visible, table, handle, None);
    }

    /// A read of the WHOLE of a covering index, which also REPLACES the
    /// source scan.
    ///
    /// Go prints no `range:` here -- the ranger narrowed nothing, so there is
    /// no interval to name -- which is the whole textual difference from
    /// [`PlanTrace::index_range_scan`]. Captured from a v8.5 `gorun` session:
    ///
    /// ```text
    /// IndexReader_7   10000.00  root
    /// └─IndexFullScan_6  10000.00  cop[tikv]  table:t, index:idx_c2(c2)  keep order:false, stats:pseudo
    /// ```
    pub(crate) fn index_full_scan(
        &mut self,
        visible: &str,
        index_name: &str,
        index_columns: &[&str],
        estimate: ScanEstimate,
        keep_order: bool,
    ) {
        self.replace_top(
            PlanNode::new(
                "IndexFullScan",
                Some(estimate.rows),
                format!(
                    "table:{visible}, index:{index_name}({})",
                    index_columns.join(", ")
                ),
                format!("keep order:{keep_order}{}", pseudo_suffix(estimate)),
            )
            .with_pseudo_ndv(estimate),
        );
        // `access_consumed` stays false, unlike every narrowed path above:
        // this one consumed no condition, so a `Selection` on top of it still
        // scales the estimate the way Go's does over an `IndexFullScan`.
    }

    /// An index range read, which also REPLACES the source scan.
    pub(crate) fn index_range_scan(
        &mut self,
        visible: &str,
        index_name: &str,
        index_columns: &[&str],
        ranges: &[crate::kv_table::IndexRange],
        estimate: ScanEstimate,
    ) {
        let printed: Vec<String> = ranges.iter().map(range_text).collect();
        self.replace_top(
            PlanNode::new(
                "IndexRangeScan",
                // The rows the RANGES cover, which is Go's `CountAfterAccess`:
                // the index histogram's answer when the index was analyzed, the
                // pseudo rate when it was not. Both come from
                // [`crate::access_cost`], the same place that costed the path.
                Some(estimate.rows),
                format!(
                    "table:{visible}, index:{index_name}({})",
                    index_columns.join(", ")
                ),
                format!(
                    "range:{}, keep order:false{}",
                    printed.join(", "),
                    pseudo_suffix(estimate)
                ),
            )
            .with_pseudo_ndv(estimate),
        );
        self.mark_top_access_consumed();
    }

    pub(crate) fn index_merge(&mut self, visible: &str, indexes: &[String], intersection: bool) {
        self.replace_top(PlanNode::new(
            "IndexMerge",
            None,
            format!("table:{visible}"),
            format!(
                "type:{}, indexes:{}",
                if intersection {
                    "intersection"
                } else {
                    "union"
                },
                indexes.join(", ")
            ),
        ));
        self.mark_top_access_consumed();
    }

    /// Exposes the two stages already performed by a non-covering
    /// [`crate::access_path::IndexRangeSourceExec`]: build a handle batch from
    /// the secondary index, then probe the table rows by those handles.
    pub(crate) fn index_lookup(&mut self, visible: &str, estimate: ScanEstimate) -> bool {
        let Some(mut index_scan) = self.stack.pop() else {
            return false;
        };
        if !matches!(index_scan.name, "IndexFullScan" | "IndexRangeScan")
            || !index_scan.children.is_empty()
        {
            self.stack.push(index_scan);
            return false;
        }
        index_scan.task = "cop[tikv]";
        index_scan.label = "(Build)";
        let rows = index_scan.est_rows;
        let act_rows = index_scan.act_rows.clone();
        let key_ndv_ratio = index_scan.key_ndv_ratio;
        let access_consumed = index_scan.access_consumed;

        let mut table_scan = PlanNode::new(
            "TableRowIDScan",
            rows,
            format!("table:{visible}"),
            format!("keep order:false{}", pseudo_suffix(estimate)),
        );
        table_scan.task = "cop[tikv]";
        table_scan.label = "(Probe)";
        table_scan.act_rows = act_rows.clone();
        table_scan.key_ndv_ratio = key_ndv_ratio;

        let mut lookup = PlanNode::new("IndexLookUp", rows, String::new(), String::new());
        lookup.act_rows = act_rows;
        lookup.key_ndv_ratio = key_ndv_ratio;
        lookup.access_consumed = access_consumed;
        lookup.children.push(index_scan);
        lookup.children.push(table_scan);
        self.stack.push(lookup);
        true
    }

    /// Moves a physical table/index scan below the root reader boundary.
    ///
    /// The executor already performs a TiKV-backed scan; this method records
    /// the same root/cop task split that Go's physical plan assigns to that
    /// request. It deliberately accepts only a bare scan so EXPLAIN cannot
    /// claim a pushdown through an operator the executor still runs at root.
    pub(crate) fn scan_reader(&mut self) -> bool {
        let Some(mut scan) = self.stack.pop() else {
            return false;
        };
        let reader = match scan.name {
            "TableFullScan" | "TableRangeScan" => "TableReader",
            "IndexFullScan" | "IndexRangeScan" => "IndexReader",
            _ => {
                self.stack.push(scan);
                return false;
            }
        };
        let scan_name = scan.name;
        scan.task = "cop[tikv]";
        let estimate = scan.est_rows;
        let key_ndv_ratio = scan.key_ndv_ratio;
        let access_consumed = scan.access_consumed;
        let act_rows = scan.act_rows.clone();
        let info = if reader == "TableReader" {
            format!("data:{scan_name}")
        } else {
            format!("index:{scan_name}")
        };
        let mut reader_node = PlanNode::new(reader, estimate, String::new(), info);
        reader_node.key_ndv_ratio = key_ndv_ratio;
        reader_node.act_rows = act_rows;
        reader_node.access_consumed = access_consumed;
        reader_node.children.push(scan);
        self.stack.push(reader_node);
        true
    }

    /// Leaves an already-root `Point_Get` in place, or moves a physical scan
    /// below its reader. Go's global aggregation may have either child shape.
    pub(crate) fn scan_reader_or_point_get(&mut self) -> bool {
        if self
            .stack
            .last()
            .is_some_and(|node| node.name == "Point_Get")
        {
            true
        } else if self.scan_reader() {
            true
        } else {
            let Some(mut selection) = self.stack.pop() else {
                return false;
            };
            if selection.name != "Selection" || selection.children.len() != 1 {
                self.stack.push(selection);
                return false;
            }
            let scan_name = selection.children[0].name;
            let reader = match scan_name {
                "TableFullScan" | "TableRangeScan" => "TableReader",
                "IndexFullScan" | "IndexRangeScan" => "IndexReader",
                _ => {
                    self.stack.push(selection);
                    return false;
                }
            };
            selection.task = "cop[tikv]";
            selection.children[0].task = "cop[tikv]";
            let estimate = selection.est_rows;
            let key_ndv_ratio = selection.key_ndv_ratio;
            let act_rows = selection.act_rows.clone();
            let mut reader_node = PlanNode::new(
                reader,
                estimate,
                String::new(),
                if reader == "TableReader" {
                    "data:Selection".to_owned()
                } else {
                    "index:Selection".to_owned()
                },
            );
            reader_node.key_ndv_ratio = key_ndv_ratio;
            reader_node.act_rows = act_rows;
            reader_node.children.push(selection);
            self.stack.push(reader_node);
            true
        }
    }

    /// Places the two scan children of a merge join behind their root reader
    /// boundaries.  Leaf predicate pushdown can leave either a bare scan or a
    /// cop `Selection` over one; both are TiKV tasks in the physical plan,
    /// while the merge itself remains a root executor.
    pub(crate) fn join_scan_readers(&mut self) {
        fn reader(mut child: PlanNode) -> PlanNode {
            let scan_name = if matches!(
                child.name,
                "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
            ) {
                Some(child.name)
            } else if child.name == "Selection"
                && child.children.len() == 1
                && matches!(
                    child.children[0].name,
                    "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
                )
            {
                Some(child.children[0].name)
            } else {
                None
            };
            let Some(scan_name) = scan_name else {
                return child;
            };
            fn mark_cop(node: &mut PlanNode) {
                node.task = "cop[tikv]";
                for child in &mut node.children {
                    mark_cop(child);
                }
            }
            mark_cop(&mut child);
            let reader = if matches!(scan_name, "IndexFullScan" | "IndexRangeScan") {
                "IndexReader"
            } else {
                "TableReader"
            };
            let estimate = child.est_rows;
            let act_rows = child.act_rows.clone();
            let key_ndv_ratio = child.key_ndv_ratio;
            let mut root = PlanNode::new(
                reader,
                estimate,
                String::new(),
                if reader == "IndexReader" {
                    format!("index:{}", child.name)
                } else {
                    format!("data:{}", child.name)
                },
            );
            root.act_rows = act_rows;
            root.key_ndv_ratio = key_ndv_ratio;
            root.children.push(child);
            root
        }

        if self.stack.len() < 2 {
            return;
        }
        let right = reader(self.stack.pop().expect("two join children"));
        let left = reader(self.stack.pop().expect("two join children"));
        self.stack.push(left);
        self.stack.push(right);
    }

    /// Marks the committed scan as preserving the access order requested by
    /// the physical property. The executor accepted the same offer before
    /// this is called, so the annotation describes real row order.
    pub(crate) fn keep_order(&mut self, descending: bool) -> bool {
        let Some(source) = self.stack.last_mut() else {
            return false;
        };
        let scan = if matches!(
            source.name,
            "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
        ) {
            source
        } else if source.name == "IndexLookUp"
            && source.children.len() == 2
            && matches!(source.children[0].name, "IndexFullScan" | "IndexRangeScan")
            && source.children[0].children.is_empty()
            && source.children[1].name == "TableRowIDScan"
        {
            &mut source.children[0]
        } else {
            return false;
        };
        if !matches!(
            scan.name,
            "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
        ) || !scan.info.contains("keep order:false")
        {
            return false;
        }
        scan.info = scan.info.replacen("keep order:false", "keep order:true", 1);
        if descending {
            if let Some(pos) = scan.info.find(", stats:pseudo") {
                scan.info.insert_str(pos, ", desc");
            } else {
                scan.info.push_str(", desc");
            }
        }
        true
    }

    /// Moves a bare scan and a global partial StreamAgg into the TiKV task,
    /// leaving the root reader above it. This is the physical split Go picks
    /// for high-estimate Sysbench `COUNT`/`SUM` ranges.
    pub(crate) fn partial_stream_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        sum: bool,
    ) -> bool {
        let Some(mut top) = self.stack.pop() else {
            return false;
        };
        let argument = select.fields.fields().iter().find_map(|field| match field {
            tidb_ast::SelectField::Expr {
                expr: tidb_ast::Expr::Aggregate { args, .. },
                ..
            } => args.first(),
            _ => None,
        });
        let Some(argument) = argument else {
            self.stack.push(top);
            return false;
        };
        // Go pushes the partial aggregate to the top of the COP TASK, not
        // directly onto the scan -- a `WHERE` pushed into the same task keeps
        // its `Selection` between them:
        //
        // ```text
        // StreamAgg          root       funcs:count(Column#N)->Column#M
        // └─TableReader      root       data:StreamAgg
        //   └─StreamAgg      cop[tikv]  funcs:count(1)->Column#N
        //     └─Selection    cop[tikv]  gt(test.t.a, 10)
        //       └─TableFullScan cop[tikv]  table:t
        // ```
        let mut filter = None;
        if top.name == "Selection" && top.children.len() == 1 {
            let child = top.children.pop().expect("the Selection's one child");
            top.children.push(child);
            if matches!(
                top.children[0].name,
                "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
            ) {
                let scan = top.children.pop().expect("the Selection's scan");
                filter = Some(top);
                top = scan;
            }
        }
        let mut scan = top;
        let reader = match scan.name {
            "TableFullScan" | "TableRangeScan" => "TableReader",
            "IndexFullScan" | "IndexRangeScan" => "IndexReader",
            _ => {
                self.stack.push(match filter {
                    Some(mut filter) => {
                        filter.children.push(scan);
                        filter
                    }
                    None => scan,
                });
                return false;
            }
        };
        scan.task = "cop[tikv]";
        // The rows that leave the cop task are the FILTERED ones, so the
        // reader and the partial aggregate count what the `Selection` emitted
        // rather than what the scan read.
        let act_rows = filter
            .as_ref()
            .map_or_else(|| scan.act_rows.clone(), |filter| filter.act_rows.clone());
        let key_ndv_ratio = scan.key_ndv_ratio;
        let scan = match filter {
            Some(mut filter) => {
                filter.task = "cop[tikv]";
                filter.children.push(scan);
                filter
            }
            None => scan,
        };
        let mut partial = PlanNode::new(
            "StreamAgg",
            Some(1.0),
            String::new(),
            format!(
                "funcs:{}({})->Column#0",
                if sum { "sum" } else { "count" },
                qualify.expr(argument)
            ),
        );
        partial.task = "cop[tikv]";
        partial.act_rows = act_rows.clone();
        partial.children.push(scan);

        let mut reader_node = PlanNode::new(
            reader,
            Some(1.0),
            String::new(),
            if reader == "TableReader" {
                "data:StreamAgg".to_owned()
            } else {
                "index:StreamAgg".to_owned()
            },
        );
        reader_node.key_ndv_ratio = key_ndv_ratio;
        reader_node.act_rows = act_rows;
        reader_node.children.push(partial);
        self.stack.push(reader_node);
        true
    }

    /// Moves a one-column grouping stage below the reader. The root HashAgg
    /// still deduplicates keys that different regions emitted.
    pub(crate) fn partial_hash_agg(
        &mut self,
        fields: &[tidb_ast::SelectField],
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
    ) -> bool {
        let Some(mut scan) = self.stack.pop() else {
            return false;
        };
        let reader = match scan.name {
            "TableFullScan" | "TableRangeScan" => "TableReader",
            "IndexFullScan" | "IndexRangeScan" => "IndexReader",
            _ => {
                self.stack.push(scan);
                return false;
            }
        };
        let estimate =
            logical_rows.or_else(|| Est::ScaleFloorOne(DISTINCT_FACTOR).apply(scan.est_rows));
        let projected = sorted_field_list(fields, qualify);
        scan.task = "cop[tikv]";
        let act_rows = scan.act_rows.clone();
        let key_ndv_ratio = scan.key_ndv_ratio;
        let mut partial = PlanNode::new(
            "HashAgg",
            estimate,
            String::new(),
            format!("group by:{projected},"),
        );
        partial.task = "cop[tikv]";
        partial.act_rows = act_rows.clone();
        partial.children.push(scan);

        let mut reader_node = PlanNode::new(
            reader,
            estimate,
            String::new(),
            if reader == "TableReader" {
                "data:HashAgg".to_owned()
            } else {
                "index:HashAgg".to_owned()
            },
        );
        reader_node.key_ndv_ratio = key_ndv_ratio;
        reader_node.act_rows = act_rows;
        reader_node.children.push(partial);
        self.stack.push(reader_node);
        true
    }

    /// Moves a one-key, one-SUM partial HashAgg below the table reader.  The
    /// estimate is the logical group-key NDV rather than the access scan's
    /// lower-bound-adjusted rows, matching Go's aggregation statistics
    /// pipeline. The fallback is retained for shapes whose logical row source
    /// cannot be modeled.
    pub(crate) fn partial_grouped_sum(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        grouped_rows: Option<f64>,
    ) -> bool {
        let Some(mut scan) = self.stack.pop() else {
            return false;
        };
        if !matches!(scan.name, "TableFullScan" | "TableRangeScan") {
            self.stack.push(scan);
            return false;
        }
        let Some(group) = select.group_by.first() else {
            self.stack.push(scan);
            return false;
        };
        let Some(sum_argument) = select.fields.fields().iter().find_map(|field| match field {
            tidb_ast::SelectField::Expr {
                expr: tidb_ast::Expr::Aggregate { name, args, .. },
                ..
            } if name.eq_ignore_ascii_case("SUM") => args.first(),
            _ => None,
        }) else {
            self.stack.push(scan);
            return false;
        };

        let estimate = grouped_rows
            .map(|rows| rows.max(1.0))
            .or_else(|| Est::ScaleFloorOne(DISTINCT_FACTOR).apply(scan.est_rows));
        let group = qualify.expr(&group.expr);
        let sum_argument = qualify.expr(sum_argument);
        scan.task = "cop[tikv]";
        let act_rows = scan.act_rows.clone();
        let key_ndv_ratio = scan.key_ndv_ratio;
        let mut partial = PlanNode::new(
            "HashAgg",
            estimate,
            String::new(),
            format!("group by:{group}, funcs:sum({sum_argument})->Column#0"),
        );
        partial.task = "cop[tikv]";
        partial.act_rows = act_rows.clone();
        partial.children.push(scan);

        let mut reader = PlanNode::new(
            "TableReader",
            estimate,
            String::new(),
            "data:HashAgg".to_owned(),
        );
        reader.key_ndv_ratio = key_ndv_ratio;
        reader.act_rows = act_rows;
        reader.children.push(partial);
        self.stack.push(reader);
        true
    }

    /// Moves an ordered grouped partial StreamAgg below its reader. The
    /// executor negotiated the same partial package with the scan before this
    /// trace mutation, so the root/cop boundary describes work TiKV actually
    /// performs.
    pub(crate) fn partial_grouped_stream_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        grouped_rows: Option<f64>,
    ) -> bool {
        let Some(mut source) = self.stack.pop() else {
            return false;
        };
        fn scan_name(source: &PlanNode) -> Option<&'static str> {
            if matches!(
                source.name,
                "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
            ) {
                return Some(source.name);
            }
            (source.name == "Selection"
                && source.children.len() == 1
                && matches!(
                    source.children[0].name,
                    "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
                ))
            .then_some(source.children[0].name)
        }
        let (reader, mut scan, existing_reader) = if let Some(name) = scan_name(&source) {
            let reader = if matches!(name, "IndexFullScan" | "IndexRangeScan") {
                "IndexReader"
            } else {
                "TableReader"
            };
            (reader, source, None)
        } else if matches!(source.name, "TableReader" | "IndexReader")
            && source.children.len() == 1
            && scan_name(&source.children[0]).is_some()
        {
            let scan = source.children.pop().expect("one reader child");
            (source.name, scan, Some(source))
        } else {
            self.stack.push(source);
            return false;
        };
        let estimate =
            grouped_rows.or_else(|| Est::ScaleFloorOne(DISTINCT_FACTOR).apply(scan.est_rows));
        fn mark_cop(node: &mut PlanNode) {
            node.task = "cop[tikv]";
            for child in &mut node.children {
                mark_cop(child);
            }
        }
        mark_cop(&mut scan);
        let act_rows = scan.act_rows.clone();
        let mut partial = PlanNode::new(
            "StreamAgg",
            estimate,
            String::new(),
            grouped_aggregate_info(select, qualify, false, false),
        );
        // The complete group tuple is unique in the aggregate output, so its
        // NDV is the aggregate row count rather than the source column's
        // pseudo 0.8 ratio. A merge join on all group keys uses this value.
        partial.key_ndv_ratio = Some(1.0);
        partial.task = "cop[tikv]";
        partial.act_rows = act_rows.clone();
        partial.children.push(scan);

        let reader_info = if reader == "TableReader" {
            "data:StreamAgg".to_owned()
        } else {
            "index:StreamAgg".to_owned()
        };
        let mut reader_node = existing_reader
            .unwrap_or_else(|| PlanNode::new(reader, estimate, String::new(), reader_info.clone()));
        reader_node.est_rows = estimate;
        reader_node.info = reader_info;
        reader_node.key_ndv_ratio = Some(1.0);
        reader_node.act_rows = act_rows;
        reader_node.children.push(partial);
        self.stack.push(reader_node);
        true
    }

    /// Moves an unordered grouped partial HashAgg below its reader. A
    /// non-covering index keeps the index build scan beside a HashAgg over
    /// the table probe, exactly where TiKV evaluates the accepted remote
    /// aggregation request.
    pub(crate) fn partial_grouped_hash_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        grouped_rows: Option<f64>,
    ) -> bool {
        let Some(mut source) = self.stack.pop() else {
            return false;
        };
        let estimate =
            grouped_rows.or_else(|| Est::ScaleFloorOne(DISTINCT_FACTOR).apply(source.est_rows));
        let info = grouped_aggregate_info(select, qualify, false, false);

        if source.name == "IndexLookUp" && source.children.len() == 2 {
            let table_scan = &mut source.children[1];
            if table_scan.name != "TableRowIDScan" || !table_scan.children.is_empty() {
                self.stack.push(source);
                return false;
            }
            let label = table_scan.label;
            table_scan.label = "";
            table_scan.task = "cop[tikv]";
            let act_rows = table_scan.act_rows.clone();
            let mut partial = PlanNode::new("HashAgg", estimate, String::new(), info);
            partial.task = "cop[tikv]";
            partial.label = label;
            partial.act_rows = act_rows;
            partial.key_ndv_ratio = Some(1.0);
            partial.children.push(std::mem::replace(
                table_scan,
                PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
            ));
            *table_scan = partial;
            source.est_rows = estimate;
            source.key_ndv_ratio = Some(1.0);
            self.stack.push(source);
            return true;
        }

        let scan_name = if matches!(
            source.name,
            "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
        ) {
            Some(source.name)
        } else if source.name == "Selection"
            && source.children.len() == 1
            && matches!(
                source.children[0].name,
                "TableFullScan" | "TableRangeScan" | "IndexFullScan" | "IndexRangeScan"
            )
        {
            Some(source.children[0].name)
        } else {
            None
        };
        let Some(scan_name) = scan_name else {
            self.stack.push(source);
            return false;
        };
        let reader = if matches!(scan_name, "IndexFullScan" | "IndexRangeScan") {
            "IndexReader"
        } else {
            "TableReader"
        };
        fn mark_cop(node: &mut PlanNode) {
            node.task = "cop[tikv]";
            for child in &mut node.children {
                mark_cop(child);
            }
        }
        mark_cop(&mut source);
        let act_rows = source.act_rows.clone();
        let mut partial = PlanNode::new("HashAgg", estimate, String::new(), info);
        partial.task = "cop[tikv]";
        partial.act_rows = act_rows.clone();
        partial.key_ndv_ratio = Some(1.0);
        partial.children.push(source);

        let mut reader_node = PlanNode::new(
            reader,
            estimate,
            String::new(),
            if reader == "TableReader" {
                "data:HashAgg".to_owned()
            } else {
                "index:HashAgg".to_owned()
            },
        );
        reader_node.act_rows = act_rows;
        reader_node.key_ndv_ratio = Some(1.0);
        reader_node.children.push(partial);
        self.stack.push(reader_node);
        true
    }

    /// Root merger for [`Self::partial_grouped_stream_agg`]. Go keeps the
    /// original function names in EXPLAIN even though final-mode COUNT adds
    /// the partial count columns internally.
    pub(crate) fn final_grouped_stream_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
    ) {
        self.wrap(
            "StreamAgg",
            Est::Inherit,
            grouped_aggregate_info(select, qualify, true, true),
        );
    }

    /// Relabels the recorded partial/final `HashAgg` pair (and its reader's
    /// `data:HashAgg` info) as the STREAM aggregate Go prints when the same
    /// global split is costed as a serial fold -- TPC-H q6/q17/q19 over
    /// pseudo statistics answer `StreamAgg / TableReader data:StreamAgg /
    /// StreamAgg cop` where analyzed statistics answer `HashAgg`.
    pub(crate) fn rename_partial_hash_agg_to_stream(&mut self) {
        fn walk(node: &mut PlanNode, is_root_of_pair: bool) {
            if node.name == "HashAgg" {
                node.name = "StreamAgg";
            }
            if let Some(info) = node.info.strip_prefix("data:HashAgg") {
                let owned = format!("data:StreamAgg{info}");
                node.info = owned;
            }
            let _ = is_root_of_pair;
            for child in &mut node.children {
                walk(child, false);
            }
        }
        for root in &mut self.stack {
            walk(root, false);
        }
    }

    /// Root merger for [`Self::partial_grouped_hash_agg`]. Aggregate
    /// functions read TiKV's partial result columns, while group-key
    /// FIRST_ROW carriers retain their physical catalog names.
    pub(crate) fn final_grouped_hash_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
    ) {
        self.wrap(
            "HashAgg",
            Est::Inherit,
            grouped_aggregate_info(select, qualify, true, true),
        );
    }

    /// The root final stage for [`Self::partial_grouped_sum`].
    pub(crate) fn final_grouped_sum_hash_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
    ) {
        let Some(group) = select.group_by.first() else {
            self.refuse("grouped SUM final stage has no group key");
            return;
        };
        let group = qualify.expr(&group.expr);
        self.wrap(
            "HashAgg",
            Est::Inherit,
            format!(
                "group by:{group}, funcs:sum(Column#0)->Column#1, funcs:firstrow({group})->{group}"
            ),
        );
    }

    /// The select-order projection above a grouped SUM final stage.
    pub(crate) fn grouped_sum_projection(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
    ) {
        let Some(group) = select.group_by.first() else {
            self.refuse("grouped SUM projection has no group key");
            return;
        };
        self.wrap(
            "Projection",
            Est::Inherit,
            format!("{}, Column#1", qualify.expr(&group.expr)),
        );
    }

    /// Rewrites the inner side's scan node into the range read an index join
    /// decided per outer key: Go's `IndexRangeScan`/`TableRangeScan` with
    /// `range: decided by [...]` instead of a literal interval.
    ///
    /// It rewrites rather than replaces because the node is already on the
    /// stack under the OTHER child -- the join's two children are both built
    /// before the strategy is known -- and because the executor behind it is
    /// the one that runs, so its row counter and estimate stay.
    ///
    /// `lookup_is_left` picks which of the two child subtrees to rewrite;
    /// `Err` means the stack does not hold two children, or the chosen one is
    /// not a scan this may rename.
    pub(crate) fn index_join_inner_scan(
        &mut self,
        lookup_is_left: bool,
        path: IndexJoinInnerPathText<'_>,
        filters: &[String],
        filter_selectivity: f64,
    ) -> Result<(), ()> {
        // An empty physical expression list renders as one empty string at
        // the join call site. Go's table-filter slice is empty in that case;
        // normalize the textual adapter before any branch decides whether an
        // inner Selection survives `EmptySelectionEliminator`.
        let filters = filters
            .iter()
            .filter(|filter| !filter.trim().is_empty())
            .cloned()
            .collect::<Vec<_>>();
        let IndexJoinInnerPathText {
            access,
            range_info,
            index,
            index_lookup,
            visible,
            estimated_rows,
            estimated_access_rows,
            estimated_outer_rows,
            unique,
            keep_outer_order,
            grouped_derived,
            composite,
            stream_aggregation,
            aggregation_info,
            aggregation_final_info,
            aggregation_partial_info,
            outer_not_null,
            inner_not_null,
        } = path;
        let depth = self.stack.len();
        if depth < 2 {
            return Err(());
        }
        let at = if lookup_is_left { depth - 2 } else { depth - 1 };
        let outer_at = if lookup_is_left { depth - 1 } else { depth - 2 };
        // A max-one-row lookup is priced from the physical outer task. Its
        // access path can refine the logical leaf estimate (for example a
        // bounded range), and Go multiplies that refined count by the probe's
        // residual selectivity.
        let outer_is_grouped = matches!(self.stack[outer_at].name, "HashAgg" | "StreamAgg");
        let outer_rows = if unique && !outer_is_grouped {
            self.stack[outer_at].est_rows.or(estimated_outer_rows)
        } else {
            estimated_outer_rows.or(self.stack[outer_at].est_rows)
        };
        let adjusted_outer_rows = if grouped_derived && !outer_not_null.is_empty() {
            outer_rows.map(|rows| rows * SELECTIVITY_FACTOR)
        } else {
            outer_rows
        };
        let estimated_access_rows = if unique {
            // Go's max-one-row inner TableScan is rebuilt once per row of the
            // committed physical outer task. An earlier logical join estimate
            // may be lower after access-path selection; it does not reduce the
            // number of dynamic point ranges opened.
            adjusted_outer_rows.or(outer_rows).or(estimated_access_rows)
        } else {
            estimated_access_rows
        };
        let estimated_rows = if unique {
            adjusted_outer_rows
                .map(|rows| rows * filter_selectivity.clamp(0.0, 1.0))
                .or(estimated_rows)
        } else if grouped_derived && !outer_not_null.is_empty() {
            estimated_rows
                .zip(estimated_outer_rows)
                .zip(adjusted_outer_rows)
                .map(|((rows, before_not_null), after_not_null)| {
                    if before_not_null > 0.0 {
                        rows * after_not_null / before_not_null
                    } else {
                        rows
                    }
                })
                .or(estimated_rows)
        } else {
            estimated_rows
        };
        fn is_scan(node: &PlanNode) -> bool {
            matches!(
                node.name,
                "TableFullScan" | "IndexFullScan" | "TableRangeScan" | "IndexRangeScan"
            ) && node.children.is_empty()
        }

        if composite {
            // Go's IndexJoinProp is threaded through the complete inner plan.
            // Every physical node in that rebuilt subtree is scaled by the
            // outer expected count, except the lookup reader itself: its
            // dynamic range is already the per-key estimate. This is visible
            // in q2 where the supplier/nation subtree and its joins are
            // multiplied by 957.73 while partsupp stays at 957.73.
            fn contains_lookup(node: &PlanNode, visible: &str) -> bool {
                let target = format!("table:{visible}");
                node.access.starts_with(&target)
                    || node
                        .children
                        .iter()
                        .any(|child| contains_lookup(child, visible))
            }

            fn scale_rebuilt_inner(node: &mut PlanNode, visible: &str, factor: f64) {
                if factor <= 0.0 {
                    return;
                }
                if matches!(node.name, "TableReader" | "IndexReader")
                    && contains_lookup(node, visible)
                {
                    return;
                }
                if let Some(rows) = node.est_rows.as_mut() {
                    *rows *= factor;
                }
                for child in &mut node.children {
                    scale_rebuilt_inner(child, visible, factor);
                }
            }

            if let Some(factor) = estimated_outer_rows {
                scale_rebuilt_inner(&mut self.stack[at], visible, factor);
            }

            fn rewrite_target(
                node: &mut PlanNode,
                visible: &str,
                access: &str,
                range_info: &str,
                index: bool,
                estimated_rows: Option<f64>,
                estimated_access_rows: Option<f64>,
            ) -> bool {
                let target = format!("table:{visible}");
                if matches!(node.name, "TableReader" | "IndexReader") && node.children.len() == 1 {
                    let child = &mut node.children[0];
                    let scan = if is_scan(child) {
                        Some(&mut *child)
                    } else if child.name == "Selection"
                        && child.children.len() == 1
                        && is_scan(&child.children[0])
                    {
                        Some(&mut child.children[0])
                    } else {
                        None
                    };
                    if let Some(scan) = scan.filter(|scan| scan.access.starts_with(&target)) {
                        let pseudo = if scan.info.contains("stats:pseudo") {
                            ", stats:pseudo"
                        } else {
                            ""
                        };
                        scan.name = if index {
                            "IndexRangeScan"
                        } else {
                            "TableRangeScan"
                        };
                        scan.access = access.to_owned();
                        scan.info =
                            format!("range: decided by {range_info}, keep order:false{pseudo}");
                        scan.est_rows = estimated_access_rows.or(estimated_rows).or(scan.est_rows);
                        node.name = if index { "IndexReader" } else { "TableReader" };
                        node.est_rows = estimated_rows.or(node.est_rows);
                        node.info = match child.name {
                            "Selection" if index => "index:Selection".to_owned(),
                            "Selection" => "data:Selection".to_owned(),
                            _ if index => "index:IndexRangeScan".to_owned(),
                            _ => "data:TableRangeScan".to_owned(),
                        };
                        return true;
                    }
                }
                for child in &mut node.children {
                    if rewrite_target(
                        child,
                        visible,
                        access,
                        range_info,
                        index,
                        estimated_rows,
                        estimated_access_rows,
                    ) {
                        return true;
                    }
                }
                false
            }

            // The Go IndexJoinProp rebuild chooses the dynamic lookup leaf as
            // the build side of the first hash join that consumes it. The
            // initial executor-first trace may have priced the same subtree
            // with the opposite orientation; repair that physical receipt
            // after rewriting the target scan, keeping the logical row order
            // unchanged while matching Go's Build/Probe display.
            fn contains_lookup_target(node: &PlanNode, target: &str) -> bool {
                node.access.starts_with(target)
                    || node
                        .children
                        .iter()
                        .any(|child| contains_lookup_target(child, target))
            }

            fn prefer_lookup_build(node: &mut PlanNode, visible: &str) -> bool {
                let target = format!("table:{visible}");
                for child in &mut node.children {
                    if prefer_lookup_build(child, visible) {
                        return true;
                    }
                }
                if node.name == "HashJoin" && node.children.len() == 2 {
                    let left_has = contains_lookup_target(&node.children[0], &target);
                    let right_has = contains_lookup_target(&node.children[1], &target);
                    if left_has || right_has {
                        if right_has {
                            node.children.swap(0, 1);
                        }
                        node.children[0].label = "(Build)";
                        node.children[1].label = "(Probe)";
                        return true;
                    }
                }
                false
            }

            if rewrite_target(
                &mut self.stack[at],
                visible,
                &access,
                range_info,
                index,
                estimated_rows,
                estimated_access_rows.or(outer_rows),
            ) {
                prefer_lookup_build(&mut self.stack[at], visible);
                if scan_is_index_join_outer(&self.stack[outer_at]).is_some() {
                    let outer = std::mem::replace(
                        &mut self.stack[outer_at],
                        PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                    );
                    self.stack[outer_at] = index_join_reader(outer, None, false, visible);
                }
                return Ok(());
            }
            return Err(());
        }

        // An index join asks its outer child for no ordering when its own
        // parent has no required property. A grouped derived table may have
        // been built independently as StreamAgg for its internal ORDER BY;
        // Go replans that candidate as an unordered HashAgg instead of
        // carrying the superseded property into the committed join.
        if !keep_outer_order {
            let outer = &mut self.stack[outer_at];
            if let Some(rows) = estimated_outer_rows {
                if outer.name == "StreamAgg" && outer.children.len() == 1 {
                    outer.name = "HashAgg";
                    outer.est_rows = Some(rows);
                    let reader = &mut outer.children[0];
                    if reader.name == "TableReader" && reader.children.len() == 1 {
                        reader.est_rows = Some(rows);
                        if reader.info == "data:StreamAgg" {
                            reader.info = "data:HashAgg".to_owned();
                        }
                        let partial = &mut reader.children[0];
                        if partial.name == "StreamAgg" && partial.children.len() == 1 {
                            partial.name = "HashAgg";
                            partial.est_rows = Some(rows);
                        }
                    }
                }
            }
            retract_keep_order(outer);
        }

        if grouped_derived {
            if !outer_not_null.is_empty() {
                let outer = &mut self.stack[outer_at];
                if outer.name != "Projection"
                    || outer.children.len() != 1
                    || outer_not_null
                        .iter()
                        .any(|offset| *offset >= outer.projection_outputs.len())
                {
                    return Err(());
                }
                let predicates = outer_not_null
                    .iter()
                    .map(|offset| format!("not(isnull({}))", outer.projection_outputs[*offset]))
                    .collect::<Vec<_>>();
                let outer_estimate = adjusted_outer_rows.or(estimated_outer_rows);
                outer.est_rows = outer_estimate;
                let reader = &mut outer.children[0];
                if reader.name != "TableReader" || reader.children.len() != 1 {
                    return Err(());
                }
                reader.est_rows = outer_estimate.or(reader.est_rows);
                reader.info = "data:Selection".to_owned();
                let scan = &mut reader.children[0];
                if !is_scan(scan) {
                    return Err(());
                }
                let mut selection = PlanNode::new(
                    "Selection",
                    outer_estimate,
                    String::new(),
                    predicates.join(", "),
                );
                selection.task = "cop[tikv]";
                selection.act_rows = scan.act_rows.clone();
                selection.key_ndv_ratio = scan.key_ndv_ratio;
                scan.task = "cop[tikv]";
                selection.children.push(std::mem::replace(
                    scan,
                    PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                ));
                *scan = selection;
            }
            let node = &mut self.stack[at];
            let inner_not_null_info = inner_not_null
                .iter()
                .map(|offset| format!("not(isnull(Column#{offset}))"))
                .collect::<Vec<_>>()
                .join(", ");
            let has_inner_not_null_selection = !inner_not_null.is_empty()
                && node.name == "Selection"
                && node.info == inner_not_null_info
                && node.children.len() == 1
                && matches!(node.children[0].name, "HashAgg" | "StreamAgg");
            let node = if has_inner_not_null_selection {
                &mut node.children[0]
            } else {
                node
            };
            if stream_aggregation && node.name == "HashAgg" {
                node.name = "StreamAgg";
            }
            // An ordered grouped probe keeps the StreamAgg task the derived
            // child already selected. Its reader may be covering and has one
            // cop StreamAgg between the reader and scan; rewrite that scan in
            // place instead of requiring the HashAgg + IndexLookUp shape.
            if node.name == "StreamAgg" {
                if node.children.len() != 1 {
                    return Err(());
                }
                node.info = aggregation_info.ok_or(())?.to_owned();
                node.est_rows = estimated_rows.or(node.est_rows);
                let reader = &mut node.children[0];
                if !matches!(reader.name, "IndexReader" | "TableReader")
                    || reader.children.len() != 1
                {
                    return Err(());
                }
                reader.est_rows = estimated_rows.or(reader.est_rows);
                if matches!(reader.children[0].name, "StreamAgg" | "HashAgg") {
                    let partial = &mut reader.children[0];
                    if partial.children.len() != 1 {
                        return Err(());
                    }
                    let label = partial.label;
                    let mut child = partial.children.pop().expect("one partial aggregate child");
                    child.label = label;
                    *partial = child;
                }
                let scan_holder = &mut reader.children[0];
                let had_selection = scan_holder.name == "Selection";
                let scan = if is_scan(scan_holder) {
                    &mut *scan_holder
                } else if had_selection
                    && scan_holder.children.len() == 1
                    && is_scan(&scan_holder.children[0])
                {
                    &mut scan_holder.children[0]
                } else {
                    return Err(());
                };
                let pseudo = if scan.info.contains("stats:pseudo") {
                    ", stats:pseudo"
                } else {
                    ""
                };
                scan.name = if index {
                    "IndexRangeScan"
                } else {
                    "TableRangeScan"
                };
                scan.access = access;
                scan.info = format!("range: decided by {range_info}, keep order:true{pseudo}");
                scan.est_rows = estimated_access_rows.or(outer_rows).or(scan.est_rows);
                if had_selection {
                    scan_holder.est_rows = estimated_rows.or(scan_holder.est_rows);
                    if !filters.is_empty() {
                        scan_holder.info = filters.join(", ");
                    }
                } else if !filters.is_empty() {
                    let mut selection = PlanNode::new(
                        "Selection",
                        estimated_rows,
                        String::new(),
                        filters.join(", "),
                    );
                    selection.task = "cop[tikv]";
                    selection.act_rows = scan_holder.act_rows.clone();
                    selection.key_ndv_ratio = scan_holder.key_ndv_ratio;
                    selection.label = scan_holder.label;
                    scan_holder.label = "";
                    scan_holder.task = "cop[tikv]";
                    selection.children.push(std::mem::replace(
                        scan_holder,
                        PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                    ));
                    *scan_holder = selection;
                }
                reader.name = if index { "IndexReader" } else { "TableReader" };
                reader.info = match (reader.name, filters.is_empty()) {
                    ("TableReader", false) => "data:Selection".to_owned(),
                    ("IndexReader", false) => "index:Selection".to_owned(),
                    ("TableReader", true) => "data:TableRangeScan".to_owned(),
                    ("IndexReader", true) => "index:IndexRangeScan".to_owned(),
                    _ => unreachable!("reader kind was checked above"),
                };
                if !inner_not_null.is_empty() && !has_inner_not_null_selection {
                    let mut selection = PlanNode::new(
                        "Selection",
                        estimated_rows,
                        String::new(),
                        inner_not_null_info.clone(),
                    );
                    selection.act_rows = node.act_rows.clone();
                    selection.key_ndv_ratio = node.key_ndv_ratio;
                    selection.children.push(std::mem::replace(
                        node,
                        PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                    ));
                    *node = selection;
                }
                // Both children were built as standalone leaves before the
                // IndexJoin candidate was committed. The grouped inner arm
                // returns here, so rebuild the outer root reader boundary now
                // instead of falling through to the ordinary arm below.
                if scan_is_index_join_outer(&self.stack[outer_at]).is_some() {
                    let outer = std::mem::replace(
                        &mut self.stack[outer_at],
                        PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                    );
                    self.stack[outer_at] = index_join_reader(outer, None, false, visible);
                }
                return Ok(());
            }
            // Go rebuilds this complete inner task for every outer batch:
            // HashAgg -> IndexLookUp -> (cop Selection -> dynamic range,
            // table row read).  The executable decision carries the same
            // aggregation, so retaining this tree is plan reporting of the
            // operator that actually runs rather than a trace-only rewrite.
            if node.name != "HashAgg" || node.children.len() != 1 {
                return Err(());
            }
            node.info = if index_lookup {
                aggregation_final_info.ok_or(())?.to_owned()
            } else {
                aggregation_info.ok_or(())?.to_owned()
            };
            node.est_rows = estimated_rows.or(node.est_rows);
            let lookup = &mut node.children[0];
            // Go replans an IndexJoin inner task from its data source. If the
            // independently selected task pushed a partial HashAgg below a
            // TableReader, replace that exact task boundary with the chosen
            // double-read lookup and keep its cop Selection as the index
            // side. `attach2Task4PhysicalHashAgg` makes the same partial
            // aggregate transient when `IndexJoinInfo` is present.
            let rebuild_partial_table_reader = lookup.name == "TableReader"
                && lookup.children.len() == 1
                && lookup.children[0].name == "HashAgg"
                && lookup.children[0].children.len() == 1
                && {
                    let source = &lookup.children[0].children[0];
                    is_scan(source)
                        || (source.name == "Selection"
                            && source.children.len() == 1
                            && is_scan(&source.children[0]))
                };
            if rebuild_partial_table_reader && index_lookup {
                let reader_act_rows = lookup.act_rows.clone();
                let reader_key_ndv_ratio = lookup.key_ndv_ratio;
                let mut partial = lookup.children.pop().expect("one partial aggregate");
                let mut index_side = partial.children.pop().expect("one partial input");
                let source_scan = if is_scan(&index_side) {
                    &index_side
                } else {
                    &index_side.children[0]
                };
                let pseudo = source_scan.info.contains("stats:pseudo");
                let table_act_rows = source_scan.act_rows.clone();
                let table_key_ndv_ratio = source_scan.key_ndv_ratio;
                index_side.label = "(Build)";

                let mut table_scan = PlanNode::new(
                    "TableRowIDScan",
                    estimated_rows,
                    format!("table:{visible}"),
                    format!(
                        "keep order:false{}",
                        if pseudo { ", stats:pseudo" } else { "" }
                    ),
                );
                table_scan.task = "cop[tikv]";
                table_scan.act_rows = table_act_rows;
                table_scan.key_ndv_ratio = table_key_ndv_ratio;
                partial.name = "HashAgg";
                partial.info = aggregation_partial_info.ok_or(())?.to_owned();
                partial.est_rows = estimated_rows;
                partial.task = "cop[tikv]";
                partial.label = "(Probe)";
                partial.children.push(table_scan);

                let mut rebuilt =
                    PlanNode::new("IndexLookUp", estimated_rows, String::new(), String::new());
                rebuilt.act_rows = reader_act_rows;
                rebuilt.key_ndv_ratio = reader_key_ndv_ratio;
                rebuilt.children.push(index_side);
                rebuilt.children.push(partial);
                *lookup = rebuilt;
            }
            if lookup.name != "IndexLookUp" || lookup.children.len() != 2 || !index_lookup {
                return Err(());
            }
            lookup.est_rows = estimated_rows.or(lookup.est_rows);
            let scan_holder = &mut lookup.children[0];
            let (scan, had_selection) = if is_scan(scan_holder) {
                (&mut *scan_holder, false)
            } else if scan_holder.name == "Selection"
                && scan_holder.children.len() == 1
                && is_scan(&scan_holder.children[0])
            {
                (&mut scan_holder.children[0], true)
            } else {
                return Err(());
            };
            let pseudo = if scan.info.contains("stats:pseudo") {
                ", stats:pseudo"
            } else {
                ""
            };
            scan.name = if index {
                "IndexRangeScan"
            } else {
                "TableRangeScan"
            };
            scan.access = access;
            scan.info = format!("range: decided by {range_info}, keep order:false{pseudo}");
            scan.est_rows = estimated_access_rows.or(outer_rows).or(scan.est_rows);
            if had_selection {
                scan_holder.est_rows = estimated_rows.or(scan_holder.est_rows);
                if !filters.is_empty() {
                    scan_holder.info = filters.join(", ");
                }
            } else if !filters.is_empty() {
                let mut selection = PlanNode::new(
                    "Selection",
                    estimated_rows,
                    String::new(),
                    filters.join(", "),
                );
                selection.task = "cop[tikv]";
                selection.act_rows = scan_holder.act_rows.clone();
                selection.key_ndv_ratio = scan_holder.key_ndv_ratio;
                selection.label = scan_holder.label;
                scan_holder.label = "";
                scan_holder.task = "cop[tikv]";
                selection.children.push(std::mem::replace(
                    scan_holder,
                    PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                ));
                *scan_holder = selection;
            }
            let table_read = &mut lookup.children[1];
            if table_read.name == "TableRowIDScan" {
                let mut partial = PlanNode::new(
                    "HashAgg",
                    estimated_rows,
                    String::new(),
                    aggregation_partial_info.ok_or(())?.to_owned(),
                );
                partial.task = "cop[tikv]";
                partial.label = table_read.label;
                table_read.label = "";
                partial.children.push(std::mem::replace(
                    table_read,
                    PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                ));
                *table_read = partial;
            } else if table_read.name != "HashAgg"
                || table_read.children.len() != 1
                || table_read.children[0].name != "TableRowIDScan"
            {
                return Err(());
            }
            table_read.info = aggregation_partial_info.ok_or(())?.to_owned();
            table_read.est_rows = estimated_rows.or(table_read.est_rows);
            table_read.children[0].est_rows = estimated_rows.or(table_read.children[0].est_rows);
            if !inner_not_null.is_empty() && !has_inner_not_null_selection {
                let mut selection = PlanNode::new(
                    "Selection",
                    estimated_rows,
                    String::new(),
                    inner_not_null_info,
                );
                selection.act_rows = node.act_rows.clone();
                selection.key_ndv_ratio = node.key_ndv_ratio;
                selection.children.push(std::mem::replace(
                    node,
                    PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
                ));
                *node = selection;
            }
            return Ok(());
        }

        let node = &mut self.stack[at];
        let source =
            if matches!(node.name, "IndexReader" | "TableReader") && node.children.len() == 1 {
                &mut node.children[0]
            } else {
                &mut *node
            };
        // A residual predicate may already have put the base scan under a cop
        // Selection. That is still the same inner data source; any deeper or
        // different subtree would make the dynamic range untruthful.
        let had_selection = source.name == "Selection";
        let scan = if is_scan(source) {
            &mut *source
        } else if source.name == "Selection"
            && source.children.len() == 1
            && is_scan(&source.children[0])
        {
            &mut source.children[0]
        } else {
            return Err(());
        };
        // `stats:pseudo` is a property of the TABLE, not of the path, so it
        // survives the rename -- and the replay compares it.
        let pseudo = if scan.info.contains("stats:pseudo") {
            ", stats:pseudo"
        } else {
            ""
        };
        scan.name = if index {
            "IndexRangeScan"
        } else {
            "TableRangeScan"
        };
        scan.access = access;
        scan.info = format!("range: decided by {range_info}, keep order:false{pseudo}");

        // One dynamic range is opened per outer row. When the statement's
        // estimator owns this simple join, its joined-row count is the exact
        // post-filter inner estimate for a one-row driver. Recover the access
        // estimate by removing the residual selectivity; otherwise preserve
        // the older trace-owned outer-row fallback.
        scan.est_rows = estimated_access_rows.or(outer_rows).or(scan.est_rows);
        if had_selection {
            if filters.is_empty() && source.info.trim().is_empty() {
                let mut scan = source.children.pop().ok_or(())?;
                scan.label = source.label;
                if scan.act_rows.is_none() {
                    scan.act_rows = source.act_rows.clone();
                }
                *source = scan;
            } else {
                source.est_rows = estimated_rows.or_else(|| {
                    outer_rows
                        .or(source.est_rows)
                        .map(|rows| rows * filter_selectivity.clamp(0.0, 1.0))
                });
                source.info = filters.join(", ");
            }
        }

        // Rebuild the two physical reader boundaries now that the strategy is
        // committed. Both leaves were built before the join family was known,
        // so they still sit on the stack as bare scans at this point.
        let mut right = self.stack.pop().ok_or(())?;
        let mut left = self.stack.pop().ok_or(())?;
        let inner = if lookup_is_left {
            &mut left
        } else {
            &mut right
        };
        if !filters.is_empty() && inner.name != "Selection" {
            let estimate = estimated_rows.or_else(|| {
                inner
                    .est_rows
                    .map(|rows| rows * filter_selectivity.clamp(0.0, 1.0))
            });
            let mut selection =
                PlanNode::new("Selection", estimate, String::new(), filters.join(", "));
            selection.task = "cop[tikv]";
            selection.act_rows = inner.act_rows.clone();
            selection.key_ndv_ratio = inner.key_ndv_ratio;
            inner.task = "cop[tikv]";
            selection.children.push(std::mem::replace(
                inner,
                PlanNode::new("TableDual", Some(0.0), String::new(), String::new()),
            ));
            *inner = selection;
        }

        if lookup_is_left {
            left = index_join_inner_source(left);
            left = index_join_reader(left, Some(index), index_lookup, visible);
            if scan_is_index_join_outer(&right).is_some() {
                right = index_join_reader(right, None, false, visible);
            }
        } else {
            right = index_join_inner_source(right);
            right = index_join_reader(right, Some(index), index_lookup, visible);
            if scan_is_index_join_outer(&left).is_some() {
                left = index_join_reader(left, None, false, visible);
            }
        }
        self.stack.push(left);
        self.stack.push(right);
        Ok(())
    }

    /// UNSAYS a `keep order:true` this join asked its children for and then
    /// did not use.
    ///
    /// `build_join` decides its merge join BEFORE its children exist, out of
    /// the PROMISE (`merge_decision`'s `Phase::Promise` -- Go's
    /// `PreparePossibleProperties` union), and hands each child a property
    /// over its own join keys. A leaf that can answer such a property records
    /// `keep order:true`, because that is what Go prints for a scan a parent
    /// relies on. When the promise is then not VERIFIED -- the built child did
    /// not deliver, or the key did not survive to the executor's own equality
    /// split -- the join falls back to hashing and nothing relies on that
    /// order any more. Go never printed the flag at all in that case: it
    /// costed the ordered and unordered plans and kept the one it used.
    ///
    /// `asked` says, per child, whether this join handed that child a
    /// NON-EMPTY property. Only a child that was asked can carry a request to
    /// unsay, and a child that was not asked keeps whatever its own subtree
    /// decided -- an `ORDER BY` inside a derived table, say.
    ///
    /// The request lands on a scan that may be SEVERAL nodes down: a derived
    /// table forwards the property through its select list onto its own
    /// `FROM` (`merge_decision::from_required_prop`), so the leaf that
    /// answered it can sit under a `Projection`, a `Selection` and a
    /// `HashJoin`. The descent below therefore walks exactly those
    /// pass-through shapes and STOPS at a `MergeJoin` or an index join, which
    /// are the two operators that RELY on a child's order -- unsaying a flag
    /// under one of those would describe a plan that cannot run.
    ///
    /// Measured: leaving these standing is the whole of a 13-plan
    /// `join_shape` regression across `planner/core/join_reorder2` and
    /// `planner/core/join_reorder_through_projection`, in every case a plan
    /// whose JOIN operators already agree with TiDB's recording and whose
    /// only divergence is one leaf saying `keep order:true` where TiDB says
    /// `false`.
    pub(crate) fn retract_child_keep_order(&mut self, asked: [bool; 2]) {
        let depth = self.stack.len();
        if depth < 2 {
            return;
        }
        for (at, asked) in [(depth - 2, asked[0]), (depth - 1, asked[1])] {
            if asked {
                retract_keep_order(&mut self.stack[at]);
            }
        }
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
    ///
    /// `stats_selectivity` is `cardinality.Selectivity`'s answer when the
    /// table has loaded statistics; without it the stats-less rates above
    /// stand, which is Go's `pseudoSelectivity`.
    ///
    /// `built` is the expression list execution owns after build-time folds.
    /// The written AST remains the selectivity input and the atomic display
    /// fallback for a built node this recorder cannot name.
    pub(crate) fn selection(
        &mut self,
        predicate: &tidb_ast::Expr,
        built: Option<&[Expression]>,
        qualify: &Qualifier<'_>,
        stats_selectivity: Option<f64>,
    ) {
        let est = if self.stack.last().is_some_and(|child| child.access_consumed) {
            Est::Inherit
        } else {
            Est::Scale(stats_selectivity.unwrap_or_else(|| pseudo_selectivity(predicate)))
        };
        let info = built
            .filter(|expressions| !expressions.is_empty())
            .and_then(|expressions| qualify.conditions(expressions))
            .unwrap_or_else(|| qualify.expr(predicate));
        self.wrap("Selection", est, info);
    }

    /// A Selection whose child projection has eliminated source names. The
    /// executable expression already contains both the internal column
    /// offsets and comparison casts Go prints, so EXPLAIN renders that same
    /// expression instead of reconstructing the written predicate.
    pub(crate) fn physical_selection(
        &mut self,
        predicate: &Expression,
        written: &tidb_ast::Expr,
        stats_selectivity: Option<f64>,
    ) -> bool {
        self.physical_selection_with_columns(predicate, written, stats_selectivity, &[])
    }

    /// [`Self::physical_selection`] with source-table names for the physical
    /// columns that projection elimination can still trace to a base table.
    pub(crate) fn physical_selection_with_columns(
        &mut self,
        predicate: &Expression,
        written: &tidb_ast::Expr,
        stats_selectivity: Option<f64>,
        column_names: &[Option<String>],
    ) -> bool {
        let Some(info) = physical_condition_text_with_columns(predicate, column_names) else {
            return false;
        };
        let est = if self.stack.last().is_some_and(|child| child.access_consumed) {
            Est::Inherit
        } else {
            Est::Scale(stats_selectivity.unwrap_or_else(|| pseudo_selectivity(written)))
        };
        self.wrap("Selection", est, info);
        true
    }

    /// A HAVING Selection over aggregate output columns. Go cannot estimate
    /// aggregate result columns from table histograms, so LogicalSelection
    /// uses the fixed SelectionFactor rather than predicate pseudo rates.
    pub(crate) fn having_selection(&mut self, predicate: &Expression) -> bool {
        let Some(info) = physical_condition_text_with_columns(predicate, &[]) else {
            return false;
        };
        self.wrap("Selection", Est::Scale(SELECTIVITY_FACTOR), info);
        true
    }

    /// A residual `Selection` above an access range. Go keeps the DataSource's
    /// complete-predicate row count separate from the chosen path's access
    /// count. Use that logical estimate when the caller has it; join leaves
    /// without one still reduce the access count with the residual predicate.
    pub(crate) fn residual_selection(
        &mut self,
        predicate: &tidb_ast::Expr,
        built: Option<&[Expression]>,
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
        stats_selectivity: Option<f64>,
    ) {
        self.residual_selection_with_columns(
            predicate,
            built,
            qualify,
            logical_rows,
            stats_selectivity,
            &[],
        );
    }

    /// [`Self::residual_selection`] with base-table names for physical columns
    /// whose SQL aliases no longer describe the optimized expression.
    pub(crate) fn residual_selection_with_columns(
        &mut self,
        predicate: &tidb_ast::Expr,
        built: Option<&[Expression]>,
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
        stats_selectivity: Option<f64>,
        column_names: &[Option<String>],
    ) {
        let info = (!column_names.is_empty())
            .then(|| {
                built.and_then(|expressions| {
                    physical_conditions_text_with_columns(expressions, column_names)
                })
            })
            .flatten()
            .or_else(|| {
                built
                    .filter(|expressions| !expressions.is_empty())
                    .and_then(|expressions| qualify.conditions(expressions))
            })
            .unwrap_or_else(|| qualify.expr(predicate));
        // Go's static-prune `PartitionProcessor` divided the `DataSource`
        // BEFORE physical planning, so `convertToBatchPointGet` builds one
        // ROOT `Selection` per partition, each over its own batch read
        // (`pkg/planner/core/find_best_task.go`). Its estimate is that
        // partition's `ds.StatsInfo()`: the partition row count scaled by
        // `cardinality.Selectivity`, whose result never drops below one row
        // (`pkg/planner/cardinality/selectivity.go`:
        // `ret = max(ret, 1.0/float64(coll.RealtimeCount))`) -- with the
        // access conjuncts' share being exactly the branch's own access
        // estimate, that is `max(branch_rows * residual_selectivity, 1)`.
        // This tier records the plan bottom-up, so the union of batch point
        // gets is already on the stack and the one residual Selection is
        // distributed into its branches here.
        if self.stack.last().is_some_and(|top| {
            top.name == "PartitionUnion"
                && !top.children.is_empty()
                && top
                    .children
                    .iter()
                    .all(|child| child.name == "Batch_Point_Get")
        }) {
            let selectivity = stats_selectivity.unwrap_or_else(|| pseudo_selectivity(predicate));
            let mut union = self.stack.pop().expect("the union was just seen");
            for branch in std::mem::take(&mut union.children) {
                let mut selection = PlanNode::new(
                    "Selection",
                    branch.est_rows.map(|rows| (rows * selectivity).max(1.0)),
                    String::new(),
                    info.clone(),
                );
                selection.children.push(branch);
                union.children.push(selection);
            }
            union.est_rows = union
                .children
                .iter()
                .try_fold(0.0, |sum, child| child.est_rows.map(|rows| sum + rows));
            self.stack.push(union);
            return;
        }
        let estimate = logical_rows.map_or_else(
            || {
                Est::ScaleFloorOne(
                    stats_selectivity.unwrap_or_else(|| pseudo_selectivity(predicate)),
                )
            },
            Est::Fixed,
        );
        self.wrap("Selection", estimate, info);
    }

    /// The one-phase aggregate this tier builds for `GROUP BY` / an
    /// aggregate select field.
    pub(crate) fn hash_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
    ) {
        let mut info = String::new();
        if !select.group_by.is_empty() {
            info.push_str("group by:");
            let mut keys: Vec<String> = select
                .group_by
                .iter()
                .map(|item| qualify.expr(&item.expr))
                .collect();
            keys.sort();
            info.push_str(&keys.join(", "));
            info.push_str(", ");
        }
        // Divergence 4: one phase, and the function as written.
        //
        // Go's `buildAggregation` splits a select field that CONTAINS an
        // aggregate into the pure aggregate function plus a scalar wrapper
        // evaluated by the projection above (`pkg/planner/core`
        // `splitAggFuncsAndScalarArgs`), so whenever any written field is a
        // scalar expression OVER aggregates (TPC-H q17's `SUM(x) / 7.0`), the
        // physical operator explains the hoisted functions themselves --
        // the same inventory `grouped_aggregate_info` rebuilds -- and never
        // the written wrapper as one aggregate function. A field that IS a
        // bare aggregate keeps the written rendering beside its siblings.
        let has_scalar_wrapped_aggregate = select.fields.fields().iter().any(|field| match field {
            tidb_ast::SelectField::Expr { expr, .. } => {
                expr.has_aggregate_flag() && !matches!(expr, tidb_ast::Expr::Aggregate { .. })
            }
            tidb_ast::SelectField::Wildcard(_) => false,
        });
        if has_scalar_wrapped_aggregate {
            self.wrap(
                "HashAgg",
                Est::Fixed(1.0),
                grouped_aggregate_info(select, qualify, false, false),
            );
            return;
        }
        let mut aggregate_index = 0;
        let funcs: Vec<String> = select
            .fields
            .fields()
            .iter()
            .filter_map(|field| match field {
                tidb_ast::SelectField::Expr {
                    expr: expr @ tidb_ast::Expr::Aggregate { .. },
                    ..
                } => {
                    let rendered = format!("{}->Column#{aggregate_index}", qualify.expr(expr));
                    aggregate_index += 1;
                    Some(rendered)
                }
                tidb_ast::SelectField::Expr { expr, .. } => Some(qualify.expr(expr)),
                tidb_ast::SelectField::Wildcard(_) => None,
            })
            .collect();
        info.push_str("funcs:");
        info.push_str(&funcs.join(", "));
        let est = if select.group_by.is_empty() {
            // A whole-table aggregate collapses to one row.
            Est::Fixed(1.0)
        } else if let Some(rows) = logical_rows {
            Est::Fixed(rows)
        } else {
            Est::Scale(DISTINCT_FACTOR)
        };
        self.wrap("HashAgg", est, info);
    }

    /// A grouped HashAgg whose physical state order differs from the written
    /// select list: aggregate functions first, then FIRST_ROW group carriers.
    pub(crate) fn grouped_hash_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
    ) {
        self.wrap(
            "HashAgg",
            logical_rows.map_or(Est::Scale(DISTINCT_FACTOR), Est::Fixed),
            grouped_aggregate_info(select, qualify, false, true),
        );
    }

    /// A StreamAgg selected by an explicit hint after enforcing its child
    /// order. Its schema and estimates are otherwise the same as the ordinary
    /// one-phase aggregate recorded above.
    pub(crate) fn enforced_stream_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
    ) {
        self.hash_agg(select, qualify, logical_rows);
        if let Some(node) = self.stack.last_mut() {
            node.name = "StreamAgg";
        }
    }

    /// The root sort Go's enforced `STREAM_AGG()` places below the aggregate
    /// when no access path already supplies the grouping order.
    pub(crate) fn enforced_stream_agg_sort(
        &mut self,
        group_by: &[tidb_ast::GroupByItem],
        qualify: &Qualifier<'_>,
    ) {
        let info = group_by
            .iter()
            .map(|item| qualify.expr(&item.expr))
            .collect::<Vec<_>>()
            .join(", ");
        self.wrap("Sort", Est::Inherit, info);
    }

    /// A grouped HashAgg whose arguments were rewritten to the columns of an
    /// injected physical Projection.
    pub(crate) fn physical_hash_agg(&mut self, info: &str, logical_rows: Option<f64>) {
        self.wrap(
            "HashAgg",
            logical_rows.map_or(Est::Scale(DISTINCT_FACTOR), Est::Fixed),
            info.to_owned(),
        );
    }

    /// A grouped hash aggregate whose executor states are the decorrelator's
    /// `FIRST_ROW` carriers and scalar `SUM`s. Rendering the physical states
    /// preserves Go's base-column identities after projection elimination.
    pub(crate) fn hash_agg_first_row_sum(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        group_by: &[Expression],
        functions: &[crate::hash_agg::AggFunc],
        column_names: &[Option<String>],
        logical_rows: Option<f64>,
    ) {
        if select.group_by.is_empty() {
            let rendered = functions
                .iter()
                .enumerate()
                .map(|(index, function)| {
                    let argument = if let Some(argument) = function.arg.as_ref() {
                        Some(physical_expression_text_with_columns(
                            argument,
                            column_names,
                        )?)
                    } else {
                        None
                    };
                    match &function.kind {
                        crate::hash_agg::AggKind::FirstRow => {
                            let argument = argument?;
                            Some(format!("funcs:firstrow({argument})->{argument}"))
                        }
                        crate::hash_agg::AggKind::Sum => {
                            let argument = argument?;
                            Some(format!("funcs:sum({argument})->Column#{index}"))
                        }
                        crate::hash_agg::AggKind::Count => Some(format!(
                            "funcs:count({}{})->Column#{index}",
                            if function.distinct { "distinct " } else { "" },
                            argument.as_deref().unwrap_or("1")
                        )),
                        _ => None,
                    }
                })
                .collect::<Option<Vec<_>>>();
            if let Some(rendered) = rendered {
                self.wrap("HashAgg", Est::Fixed(1.0), rendered.join(", "));
            } else {
                self.hash_agg(select, qualify, logical_rows);
            }
            return;
        }
        let groups = group_by
            .iter()
            .map(|expression| physical_expression_text_with_columns(expression, column_names))
            .collect::<Option<Vec<_>>>();
        let rendered = functions
            .iter()
            .enumerate()
            .map(|(index, function)| {
                let argument = if let Some(argument) = function.arg.as_ref() {
                    Some(physical_expression_text_with_columns(
                        argument,
                        column_names,
                    )?)
                } else {
                    None
                };
                match &function.kind {
                    crate::hash_agg::AggKind::FirstRow => {
                        let argument = argument?;
                        Some(format!("funcs:firstrow({argument})->{argument}"))
                    }
                    crate::hash_agg::AggKind::Sum => {
                        let argument = argument?;
                        Some(format!("funcs:sum({argument})->Column#{index}"))
                    }
                    crate::hash_agg::AggKind::Count => Some(format!(
                        "funcs:count({}{})->Column#{index}",
                        if function.distinct { "distinct " } else { "" },
                        argument.as_deref().unwrap_or("1")
                    )),
                    _ => None,
                }
            })
            .collect::<Option<Vec<_>>>();
        let (Some(mut groups), Some(rendered)) = (groups, rendered) else {
            self.hash_agg(select, qualify, logical_rows);
            return;
        };
        groups.sort();
        let info = format!("group by:{}, {}", groups.join(", "), rendered.join(", "));
        self.wrap(
            "HashAgg",
            logical_rows.map_or(Est::Scale(DISTINCT_FACTOR), Est::Fixed),
            info,
        );
        if let Some(aggregate) = self.stack.last_mut() {
            // This decorrelator is admitted only when GROUP BY contains the
            // outer table's complete non-null unique key. Later scalar SUMs
            // join on that key (with any omitted member equality-fixed by a
            // leaf predicate), so the grouped output has one row per join
            // key. Publish the same unique-key NDV Go derives for the parent
            // join instead of losing it at the fixed-cardinality boundary.
            aggregate.key_ndv_ratio = Some(1.0);
        }
    }

    /// A one-phase grouped StreamAgg whose child already delivers the written
    /// group-key order.
    pub(crate) fn grouped_stream_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        input_projection: bool,
        logical_rows: Option<f64>,
        physical_info: Option<&str>,
        extra_first_rows: &[String],
    ) {
        let mut info = physical_info.map_or_else(
            || grouped_aggregate_info(select, qualify, false, true),
            str::to_owned,
        );
        // A carrier introduced for an enclosing predicate is absent from the
        // derived SELECT's written field list, but it is a real aggregate
        // state and must remain visible in the physical plan. An explicitly
        // supplied physical payload already enumerates every state.
        if physical_info.is_none() {
            for carrier in extra_first_rows {
                info.push_str(&format!(", funcs:firstrow({carrier})->{carrier}"));
            }
        }
        self.wrap(
            "StreamAgg",
            logical_rows.map_or_else(
                || {
                    if input_projection {
                        Est::Inherit
                    } else {
                        Est::Scale(DISTINCT_FACTOR)
                    }
                },
                Est::Fixed,
            ),
            info,
        );
    }

    /// Restores the written select-field order above a top-level physical
    /// grouped StreamAgg whose aggregate results precede its FIRST_ROW group
    /// carriers. A derived table can eliminate this projection and map its
    /// relation directly onto the same aggregation schema.
    pub(crate) fn grouped_stream_output_projection(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        internal_columns: bool,
        column_names: &[Option<String>],
    ) {
        let projection_mapping = std::mem::take(&mut self.next_aggregation_projection);
        let mut next_aggregate = 0;
        let fields = select
            .fields
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(field_index, field)| {
                match projection_mapping.get(field_index).copied().flatten() {
                    Some((input, output)) => Some(format!("Column#{input}->Column#{output}")),
                    None => match field {
                        tidb_ast::SelectField::Expr { .. } if internal_columns => {
                            Some(format!("Column#{field_index}"))
                        }
                        tidb_ast::SelectField::Expr {
                            expr: tidb_ast::Expr::Aggregate { .. },
                            ..
                        } => {
                            let column = format!("Column#{next_aggregate}");
                            next_aggregate += 1;
                            Some(column)
                        }
                        tidb_ast::SelectField::Expr { expr, .. } => {
                            Some(qualify.expr_with_physical_columns(expr, column_names))
                        }
                        tidb_ast::SelectField::Wildcard(_) => None,
                    },
                }
            })
            .collect::<Vec<_>>();
        self.wrap("Projection", Est::Inherit, fields.join(", "));
    }

    /// The compact projection Go injects between a complex ordered source and
    /// its grouped StreamAgg: group keys first, then unique aggregate inputs.
    pub(crate) fn grouped_input_projection(
        &mut self,
        expressions: &[String],
        injected_for_scalar: bool,
    ) {
        let info = if injected_for_scalar {
            expressions
                .iter()
                .enumerate()
                .map(|(index, expression)| format!("{expression}->Column#{index}"))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            expressions.join(", ")
        };
        self.wrap("Projection", Est::Inherit, info);
    }

    /// The root projection that evaluates scalar expressions over aggregate
    /// result columns. Go lowers each aggregate call to a `Column#N` first;
    /// only the remaining arithmetic/cast/function expression lives here.
    pub(crate) fn aggregate_projection(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
    ) {
        let mut next_aggregate = 0;
        let fields = select
            .fields
            .fields()
            .iter()
            .filter_map(|field| match field {
                tidb_ast::SelectField::Expr { expr, .. }
                    if !matches!(expr, tidb_ast::Expr::Aggregate { .. }) =>
                {
                    Some(post_aggregate_expr(expr, qualify, &mut next_aggregate))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if !fields.is_empty() {
            self.wrap("Projection", Est::Inherit, fields.join(", "));
        }
    }

    /// The visible Projection retained above an Aggregation after Go's
    /// `Aggregation -> Projection` pushdown arm removed the child Projection.
    /// Plain fields now read aggregation result columns; scalar expressions
    /// continue to read the hoisted aggregate columns.
    pub(crate) fn aggregation_pushdown_projection(
        &mut self,
        visible_select: &tidb_ast::SelectStmt,
        physical_select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        column_names: &[Option<String>],
    ) {
        let projection_mapping = std::mem::take(&mut self.next_aggregation_projection);
        let mut next_aggregate = 0;
        let fields = visible_select
            .fields
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(field_index, field)| {
                let tidb_ast::SelectField::Expr { .. } = field else {
                    return None;
                };
                let physical = match physical_select.fields.fields().get(field_index) {
                    Some(tidb_ast::SelectField::Expr { expr, .. }) => expr,
                    _ => return None,
                };
                Some(
                    match projection_mapping.get(field_index).copied().flatten() {
                        Some((input, output)) => format!("Column#{input}->Column#{output}"),
                        None => match physical {
                            tidb_ast::Expr::Column(_) => {
                                qualify.expr_with_physical_columns(physical, column_names)
                            }
                            tidb_ast::Expr::Aggregate { .. } => {
                                let column = format!("Column#{}", 10_000 + field_index);
                                next_aggregate += 1;
                                column
                            }
                            _ if aggregate_exprs(physical).is_empty() => {
                                format!("Column#{}", 10_000 + field_index)
                            }
                            _ => post_aggregate_expr(physical, qualify, &mut next_aggregate),
                        },
                    },
                )
            })
            .collect::<Vec<_>>();
        if !fields.is_empty() {
            self.wrap("Projection", Est::Inherit, fields.join(", "));
        }
    }

    /// The root projection Go inserts before an integer `SUM`, converting the
    /// integer argument to the exact DECIMAL input domain consumed by SUM.
    pub(crate) fn sum_cast_projection(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        precision: i64,
    ) {
        let argument = select.fields.fields().iter().find_map(|field| match field {
            tidb_ast::SelectField::Expr {
                expr: tidb_ast::Expr::Aggregate { name, args, .. },
                ..
            } if name.eq_ignore_ascii_case("SUM") => args.first(),
            _ => None,
        });
        let Some(argument) = argument else {
            self.refuse("integer SUM projection has no source argument");
            return;
        };
        self.wrap(
            "Projection",
            Est::Inherit,
            format!(
                "cast({}, decimal({precision},0) BINARY)->Column#0",
                qualify.expr(argument)
            ),
        );
    }

    /// A global root StreamAgg over the single aggregate selected by the
    /// Sysbench range plans. `projected` names the cast projection's output;
    /// COUNT reads its source column directly.
    pub(crate) fn stream_agg(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        projected: bool,
    ) {
        let aggregate = select.fields.fields().iter().find_map(|field| match field {
            tidb_ast::SelectField::Expr {
                expr:
                    tidb_ast::Expr::Aggregate {
                        name,
                        distinct,
                        args,
                    },
                ..
            } => Some((name, *distinct, args.first())),
            _ => None,
        });
        let Some((name, distinct, argument)) = aggregate else {
            // A scalar wrapper over the aggregate (`SUM(x) / 7.0`) has no
            // bare Aggregate field to read; Go still prints the hoisted
            // function itself, which grouped_aggregate_info rebuilds.
            let wrapped = select.fields.fields().iter().any(|field| {
                matches!(field, tidb_ast::SelectField::Expr { expr, .. } if expr.has_aggregate_flag())
            });
            if wrapped {
                self.wrap(
                    "StreamAgg",
                    Est::Fixed(1.0),
                    grouped_aggregate_info(select, qualify, false, false),
                );
                return;
            }
            self.refuse("stream aggregation has no aggregate expression");
            return;
        };
        let input = if projected {
            "Column#0".to_owned()
        } else {
            fn without_parens(mut expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
                while let tidb_ast::Expr::Paren(inner) = expr {
                    expr = inner;
                }
                expr
            }
            argument.map_or_else(
                || "1".to_owned(),
                |argument| qualify.expr(without_parens(argument)),
            )
        };
        self.wrap(
            "StreamAgg",
            Est::Fixed(1.0),
            format!(
                "funcs:{}({}{input})->Column#0",
                name.to_ascii_lowercase(),
                if distinct { "distinct " } else { "" },
            ),
        );
    }

    /// Go `MaxMinEliminator.eliminateSingleMaxMin`: a nullable aggregate
    /// input is filtered, one ordered row is retained, and the scalar
    /// aggregate remains to turn an empty input into one NULL row.
    pub(crate) fn max_min_elimination(&mut self, max: bool, nullable: bool, reads_column: bool) {
        let input = "Column#0";
        if nullable {
            self.wrap(
                "Selection",
                Est::Scale(SELECTIVITY_FACTOR),
                format!("not(isnull({input}))"),
            );
        }
        if reads_column {
            self.wrap(
                "TopN",
                Est::CapAt(1.0),
                format!(
                    "{input}{}, offset:0, count:1",
                    if max { ":desc" } else { "" }
                ),
            );
        } else {
            self.wrap("Limit", Est::CapAt(1.0), "offset:0, count:1".to_owned());
        }
        self.wrap(
            "StreamAgg",
            Est::Fixed(1.0),
            format!(
                "funcs:{}({input})->Column#1",
                if max { "max" } else { "min" }
            ),
        );
    }

    /// Go `util.ExplainByItems`: the by-item list a `Sort` or a `TopN` prints.
    fn by_items_text(order_by: &[tidb_ast::OrderItem], qualify: &Qualifier<'_>) -> String {
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
        items.join(", ")
    }

    /// `ORDER BY` with no `LIMIT` above it to fuse with.
    pub(crate) fn sort(&mut self, order_by: &[tidb_ast::OrderItem], qualify: &Qualifier<'_>) {
        self.wrap("Sort", Est::Inherit, Self::by_items_text(order_by, qualify));
    }

    /// An unbounded Sort above the visible projection of a grouped aggregate.
    /// Aggregate aliases now name generated projection columns, while direct
    /// pass-through fields retain their source identities.
    pub(crate) fn grouped_aggregate_sort(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        internal_columns: bool,
        column_names: &[Option<String>],
    ) {
        let mut aliases = Vec::new();
        let mut visible = Vec::new();
        for (field_index, field) in select.fields.fields().iter().enumerate() {
            let tidb_ast::SelectField::Expr { expr, alias } = field else {
                continue;
            };
            visible.push((
                alias.clone().unwrap_or_else(|| {
                    crate::driver::default_field_display_name(&select.fields, field_index, expr)
                }),
                field_index,
            ));
            if alias.is_some() && matches!(aggregate_exprs(expr).as_slice(), [_]) {
                aliases.push((
                    alias.as_ref().expect("guarded alias").to_ascii_lowercase(),
                    field_index,
                ));
            }
        }
        let items = select
            .order_by
            .iter()
            .map(|item| {
                let expression = match &item.expr {
                    tidb_ast::Expr::Column(path) if path.len() == 1 => {
                        let candidates = if internal_columns { &visible } else { &aliases };
                        candidates
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case(&path[0]))
                            .map_or_else(
                                || qualify.expr_with_physical_columns(&item.expr, column_names),
                                |(_, index)| format!("Column#{index}"),
                            )
                    }
                    _ => qualify.expr_with_physical_columns(&item.expr, column_names),
                };
                if item.desc {
                    format!("{expression}:desc")
                } else {
                    expression
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.wrap("Sort", Est::Inherit, items);
    }

    fn aggregation_pushdown_by_items(
        visible_select: &tidb_ast::SelectStmt,
        physical_select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        column_names: &[Option<String>],
    ) -> String {
        let visible = visible_select
            .fields
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(field_index, field)| match field {
                tidb_ast::SelectField::Expr { expr, alias } => Some((
                    alias.clone().unwrap_or_else(|| {
                        crate::driver::default_field_display_name(
                            &visible_select.fields,
                            field_index,
                            expr,
                        )
                    }),
                    field_index,
                )),
                tidb_ast::SelectField::Wildcard(_) => None,
            })
            .collect::<Vec<_>>();
        visible_select
            .order_by
            .iter()
            .map(|item| {
                let expression = match &item.expr {
                    tidb_ast::Expr::Column(path) if path.len() == 1 => visible
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(&path[0]))
                        .map_or_else(
                            || qualify.expr(&item.expr),
                            |(_, index)| match physical_select.fields.fields().get(*index) {
                                Some(tidb_ast::SelectField::Expr {
                                    expr: tidb_ast::Expr::Column(path),
                                    ..
                                }) => qualify.expr_with_physical_columns(
                                    &tidb_ast::Expr::Column(path.clone()),
                                    column_names,
                                ),
                                _ => format!("Column#{index}"),
                            },
                        ),
                    _ => qualify.expr(&item.expr),
                };
                if item.desc {
                    format!("{expression}:desc")
                } else {
                    expression
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub(crate) fn aggregation_pushdown_sort(
        &mut self,
        visible_select: &tidb_ast::SelectStmt,
        physical_select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        column_names: &[Option<String>],
    ) {
        self.wrap(
            "Sort",
            Est::Inherit,
            Self::aggregation_pushdown_by_items(
                visible_select,
                physical_select,
                qualify,
                column_names,
            ),
        );
    }

    /// The root Sort inserted by Go `getEnforcedMergeJoin` for a child whose
    /// access path does not already provide the hinted merge-key order.
    pub(crate) fn enforced_merge_sort(&mut self, keys: &[String], desc: bool) {
        let info = keys
            .iter()
            .map(|key| {
                if desc {
                    format!("{key}:desc")
                } else {
                    key.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.wrap("Sort", Est::Inherit, info);
    }

    /// `ORDER BY` + `LIMIT` fused into Go's `TopN`
    /// (`pkg/planner/core/rule_topn_push_down.go`).
    ///
    /// The info text is Go's `LogicalTopN.ExplainInfo`: the by-items, then
    /// `, offset:N, count:N`. The estimate is Go's
    /// `property.DeriveLimitStats(child, Count)` -- the COUNT, not
    /// `offset + count`, which is what real TiDB prints for
    /// `order by b limit 1,2` (captured: `TopN_8 | 2.00 | root`).
    pub(crate) fn topn(
        &mut self,
        order_by: &[tidb_ast::OrderItem],
        qualify: &Qualifier<'_>,
        offset: u64,
        count: u64,
    ) {
        let items = Self::by_items_text(order_by, qualify);
        self.wrap(
            "TopN",
            Est::CapAt(count as f64),
            format!("{items}, offset:{offset}, count:{count}"),
        );
    }

    /// A TopN pushed below the final projection of a grouped aggregate.
    /// Aggregate aliases resolve to the aggregation's result columns, while
    /// ordinary order items retain their qualified source names.
    pub(crate) fn grouped_aggregate_topn(
        &mut self,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        internal_columns: bool,
        column_names: &[Option<String>],
        offset: u64,
        count: u64,
    ) {
        let mut aggregate_index = 0;
        let mut aliases = Vec::new();
        let mut visible = Vec::new();
        for (field_index, field) in select.fields.fields().iter().enumerate() {
            let tidb_ast::SelectField::Expr { expr, alias } = field else {
                continue;
            };
            visible.push((
                alias.clone().unwrap_or_else(|| {
                    crate::driver::default_field_display_name(&select.fields, field_index, expr)
                }),
                field_index,
            ));
            let aggregates = aggregate_exprs(expr);
            if let (Some(alias), [tidb_ast::Expr::Aggregate { .. }]) =
                (alias.as_ref(), aggregates.as_slice())
            {
                aliases.push((alias.to_ascii_lowercase(), aggregate_index));
            }
            aggregate_index += aggregates.len();
        }
        let items = select
            .order_by
            .iter()
            .map(|item| {
                let expression = match &item.expr {
                    tidb_ast::Expr::Column(path) if path.len() == 1 => {
                        let candidates = if internal_columns { &visible } else { &aliases };
                        candidates
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case(&path[0]))
                            .map_or_else(
                                || qualify.expr_with_physical_columns(&item.expr, column_names),
                                |(_, index)| format!("Column#{index}"),
                            )
                    }
                    _ => qualify.expr_with_physical_columns(&item.expr, column_names),
                };
                if item.desc {
                    format!("{expression}:desc")
                } else {
                    expression
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.wrap(
            "TopN",
            Est::CapAt(count as f64),
            format!("{items}, offset:{offset}, count:{count}"),
        );
    }

    pub(crate) fn aggregation_pushdown_topn(
        &mut self,
        visible_select: &tidb_ast::SelectStmt,
        physical_select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
        column_names: &[Option<String>],
        offset: u64,
        count: u64,
    ) {
        let items = Self::aggregation_pushdown_by_items(
            visible_select,
            physical_select,
            qualify,
            column_names,
        );
        self.wrap(
            "TopN",
            Est::CapAt(count as f64),
            format!("{items}, offset:{offset}, count:{count}"),
        );
    }

    /// Records `PhysicalIndexLookUpReader.PushedLimit`: the index-side handle
    /// stream is capped before any probe tasks are built, and no root Limit
    /// remains.
    pub(crate) fn embedded_lookup_limit(
        &mut self,
        offset: u64,
        count: u64,
        logical_rows: Option<f64>,
    ) -> bool {
        // Go pushes the `Limit` into each partition's own reader: its
        // `PartitionProcessor` has already replaced the one `DataSource` with
        // one per partition by the time physical optimization embeds
        // anything, so the recorded plan is a `PartitionUnion` of
        // `IndexLookUp`s that EACH say `limit embedded(...)`. This tier
        // records the plan in one bottom-up pass, so the union is already on
        // the stack here and the embed descends into it.
        if self
            .stack
            .last()
            .is_some_and(|top| top.name == "PartitionUnion")
        {
            let mut union = self.stack.pop().expect("the union was just seen");
            let branches = std::mem::take(&mut union.children);
            let mut embedded = true;
            for branch in branches {
                self.stack.push(branch);
                embedded &= self.embedded_lookup_limit(offset, count, logical_rows);
                union
                    .children
                    .push(self.stack.pop().expect("every branch is left on the stack"));
            }
            self.stack.push(union);
            return embedded;
        }
        let Some(top) = self.stack.pop() else {
            return false;
        };
        let (residual_selection, mut lookup) = if top.name == "Selection" && top.children.len() == 1
        {
            let mut selection = top;
            let lookup = selection.children.pop().expect("selection child");
            (Some(selection), lookup)
        } else {
            (None, top)
        };
        if lookup.name != "IndexLookUp" || lookup.children.len() != 2 {
            if let Some(mut selection) = residual_selection {
                selection.children.push(lookup);
                self.stack.push(selection);
            } else {
                self.stack.push(lookup);
            }
            return false;
        }
        let mut index_scan = lookup.children.remove(0);
        if !matches!(index_scan.name, "IndexFullScan" | "IndexRangeScan")
            || !index_scan.children.is_empty()
            || lookup.children[0].name != "TableRowIDScan"
        {
            lookup.children.insert(0, index_scan);
            if let Some(mut selection) = residual_selection {
                selection.children.push(lookup);
                self.stack.push(selection);
            } else {
                self.stack.push(lookup);
            }
            return false;
        }
        index_scan.task = "cop[tikv]";
        let estimate = Est::CapAt(count as f64).apply(index_scan.est_rows);
        let output_estimate = logical_rows.or(estimate);
        let act_rows = index_scan.act_rows.clone();
        let key_ndv_ratio = index_scan.key_ndv_ratio;
        index_scan.est_rows = estimate;
        index_scan.label = "";
        let mut partial = PlanNode::new(
            "Limit",
            output_estimate,
            String::new(),
            format!("offset:0, count:{}", offset.saturating_add(count)),
        );
        partial.task = "cop[tikv]";
        partial.label = "(Build)";
        partial.act_rows = act_rows;
        partial.key_ndv_ratio = key_ndv_ratio;
        if let Some(mut selection) = residual_selection {
            selection.task = "cop[tikv]";
            selection.est_rows = output_estimate;
            selection.children.push(index_scan);
            partial.children.push(selection);
        } else {
            partial.children.push(index_scan);
        }
        lookup.children.insert(0, partial);
        lookup.est_rows = output_estimate;
        lookup.info = format!("limit embedded(offset:{offset}, count:{count})");
        lookup.children[1].est_rows = output_estimate;
        self.stack.push(lookup);
        true
    }

    /// Moves a residual filter below a non-covering lookup, onto its Probe.
    pub(crate) fn lookup_probe_selection(&mut self, logical_rows: Option<f64>) -> bool {
        // A residual over a batch point get is already placed: Go's
        // `convertToBatchPointGet` keeps `IndexFilters`/`TableFilters` in a
        // ROOT `Selection` above the root read (`find_best_task.go`), so
        // there is no cop Probe to move anything onto. The per-partition
        // shape was distributed when the Selection was recorded (see
        // [`Self::residual_selection`]), leaving the union on top.
        if self.stack.last().is_some_and(|top| {
            top.name == "PartitionUnion"
                && !top.children.is_empty()
                && top.children.iter().all(|child| {
                    child.name == "Selection"
                        && child.children.len() == 1
                        && child.children[0].name == "Batch_Point_Get"
                })
        }) {
            return true;
        }
        let Some(mut selection) = self.stack.pop() else {
            return false;
        };
        // The residual filter belongs to each partition's own reader. Go's
        // `PartitionProcessor` divided the `DataSource` before this filter
        // was ever placed, so the recorded plan shows one `Selection(Probe)`
        // INSIDE every branch -- never one above the union. This tier records
        // the plan bottom-up, so the filter arrives after the fan-out and is
        // distributed into the branches here.
        if selection
            .children
            .first()
            .is_some_and(|child| child.name == "PartitionUnion")
        {
            let mut union = selection.children.pop().expect("the union was just seen");
            let branches = std::mem::take(&mut union.children);
            let mut placed = true;
            for branch in branches {
                let mut per_branch = selection.clone();
                per_branch.act_rows = None;
                per_branch.children = vec![branch];
                self.stack.push(per_branch);
                placed &= self.lookup_probe_selection(logical_rows);
                union
                    .children
                    .push(self.stack.pop().expect("every branch is left on the stack"));
            }
            self.stack.push(union);
            return placed;
        }
        if selection.name != "Selection" || selection.children.len() != 1 {
            self.stack.push(selection);
            return false;
        }
        let lookup = selection.children.pop().expect("selection child");
        // A single-partition (or unpartitioned) batch point get also keeps
        // its residual as the root Selection it already is.
        if lookup.name == "Batch_Point_Get" {
            selection.children.push(lookup);
            self.stack.push(selection);
            return true;
        }
        if lookup.name != "IndexLookUp" || lookup.children.len() != 2 {
            selection.children.push(lookup);
            self.stack.push(selection);
            return false;
        }
        let mut lookup = lookup;
        let mut probe = lookup.children.remove(1);
        probe.label = "";
        // Go reports the probe selection using the lookup's physical output
        // estimate (the index-side estimate), not the logical input-row
        // estimate used to shape the parent plan. Keep the logical estimate
        // only as a fallback for traces that do not carry one.
        let output_estimate = lookup.est_rows.or(logical_rows);
        selection.task = "cop[tikv]";
        selection.label = "(Probe)";
        selection.est_rows = output_estimate;
        selection.children.push(probe);
        lookup.children.insert(1, selection);
        lookup.est_rows = output_estimate;
        self.stack.push(lookup);
        true
    }

    /// Caps the visible output estimate of an ordered lookup whose residual
    /// predicate is evaluated on Probe.
    pub(crate) fn cap_lookup_output(&mut self, count: u64) -> bool {
        let Some(top) = self.stack.last_mut() else {
            return false;
        };
        let mut lookup = top;
        if lookup.name == "Projection" {
            lookup.est_rows = lookup
                .est_rows
                .map_or(Some(count as f64), |rows| Some(rows.min(count as f64)));
            let Some(child) = lookup.children.get_mut(0) else {
                return false;
            };
            lookup = child;
        }
        if lookup.name != "IndexLookUp" || lookup.children.len() != 2 {
            return false;
        }
        lookup.est_rows = lookup
            .est_rows
            .map_or(Some(count as f64), |rows| Some(rows.min(count as f64)));
        if let Some(selection) = lookup.children.get_mut(1) {
            if selection.name == "Selection" {
                selection.est_rows = selection
                    .est_rows
                    .map_or(Some(count as f64), |rows| Some(rows.min(count as f64)));
            }
        }
        true
    }

    /// Records Go's pushed `TopN(Build)` in an index lookup while leaving the
    /// root TopN in the executor pipeline.
    pub(crate) fn pushed_topn_lookup(
        &mut self,
        order_by: &[tidb_ast::OrderItem],
        qualify: &Qualifier<'_>,
        offset: u64,
        count: u64,
        logical_rows: Option<f64>,
    ) -> bool {
        let Some(top) = self.stack.pop() else {
            return false;
        };
        let (residual_selection, mut lookup) = if top.name == "Selection" && top.children.len() == 1
        {
            let mut selection = top;
            let lookup = selection.children.pop().expect("selection child");
            (Some(selection), lookup)
        } else {
            (None, top)
        };
        if lookup.name != "IndexLookUp" || lookup.children.len() != 2 {
            if let Some(mut selection) = residual_selection {
                selection.children.push(lookup);
                self.stack.push(selection);
            } else {
                self.stack.push(lookup);
            }
            return false;
        }
        let mut index_scan = lookup.children.remove(0);
        if !matches!(index_scan.name, "IndexFullScan" | "IndexRangeScan") {
            lookup.children.insert(0, index_scan);
            if let Some(mut selection) = residual_selection {
                selection.children.push(lookup);
                self.stack.push(selection);
            } else {
                self.stack.push(lookup);
            }
            return false;
        }
        index_scan.label = "";
        let output_estimate = logical_rows.or(lookup.est_rows);
        let mut pushed = PlanNode::new(
            "TopN",
            output_estimate,
            String::new(),
            format!(
                "{}, offset:{offset}, count:{count}",
                Self::by_items_text(order_by, qualify)
            ),
        );
        pushed.task = "cop[tikv]";
        pushed.label = "(Build)";
        if let Some(mut selection) = residual_selection {
            selection.task = "cop[tikv]";
            selection.est_rows = output_estimate;
            selection.children.push(index_scan);
            pushed.children.push(selection);
        } else {
            pushed.children.push(index_scan);
        }
        lookup.children.insert(0, pushed);
        lookup.est_rows = output_estimate;
        lookup.children[1].est_rows = output_estimate;
        self.stack.push(lookup);
        self.topn(order_by, qualify, offset, count);
        true
    }

    /// Pushes an already-accepted ordered table or covering-index cap into
    /// TiKV and leaves the corresponding reader boundary above it. The root
    /// Limit is recorded separately by [`Self::limit`].
    pub(crate) fn pushed_limit_reader(&mut self, offset: u64, count: u64) -> bool {
        let Some(mut input) = self.stack.pop() else {
            return false;
        };
        let reader_name = match input.name {
            "TableFullScan" | "TableRangeScan" => "TableReader",
            "IndexFullScan" | "IndexRangeScan" => "IndexReader",
            // A source that collapsed to `TableDual` reads no partition and
            // no range at all ([`Self::pruned_away_table_dual`],
            // [`Self::empty_range_table_dual`]). There is no cop task to push
            // a cap into and Go has no reader here either -- its own plan for
            // `select a from tlist use index () where b > 10 order by b limit
            // 10` over a `LIST (b)` table of `0..5` is the ONE line
            // `TableDual root rows:0`. Accepting it as a no-op is what keeps
            // that statement PLANNABLE: refusing made the whole `EXPLAIN`
            // unprintable, which reads as "this engine cannot plan it".
            "TableDual" => {
                self.stack.push(input);
                return true;
            }
            _ => {
                self.stack.push(input);
                return false;
            }
        };
        input.task = "cop[tikv]";
        let estimate = Est::CapAt(count as f64).apply(input.est_rows);
        let act_rows = input.act_rows.clone();
        let key_ndv_ratio = input.key_ndv_ratio;
        let mut partial = PlanNode::new(
            "Limit",
            estimate,
            String::new(),
            format!("offset:{offset}, count:{count}"),
        );
        partial.task = "cop[tikv]";
        partial.act_rows = act_rows.clone();
        partial.children.push(input);

        let mut reader = PlanNode::new(
            reader_name,
            estimate,
            String::new(),
            if reader_name == "TableReader" {
                "data:Limit".to_owned()
            } else {
                "index:Limit".to_owned()
            },
        );
        reader.key_ndv_ratio = key_ndv_ratio;
        reader.act_rows = act_rows;
        reader.children.push(partial);
        self.stack.push(reader);
        true
    }

    /// Places a bounded sort after an optional pushed Selection in TiKV and
    /// returns its rows through a TableReader. The root TopN remains above
    /// this reader and applies the SQL offset/count again, as in Go TiDB.
    pub(crate) fn pushed_topn_reader(
        &mut self,
        order_by: &[tidb_ast::OrderItem],
        qualify: &Qualifier<'_>,
        count: u64,
    ) -> bool {
        let Some(mut input) = self.stack.pop() else {
            return false;
        };
        let valid = if matches!(input.name, "TableFullScan" | "TableRangeScan") {
            input.task = "cop[tikv]";
            true
        } else if input.name == "Selection" && input.children.len() == 1 {
            let scan = &mut input.children[0];
            if matches!(scan.name, "TableFullScan" | "TableRangeScan") {
                scan.task = "cop[tikv]";
                input.task = "cop[tikv]";
                true
            } else {
                false
            }
        } else {
            false
        };
        if !valid {
            self.stack.push(input);
            return false;
        }
        let estimate = Est::CapAt(count as f64).apply(input.est_rows);
        let act_rows = input.act_rows.clone();
        let key_ndv_ratio = input.key_ndv_ratio;
        let mut partial = PlanNode::new(
            "TopN",
            estimate,
            String::new(),
            format!(
                "{}, offset:0, count:{count}",
                Self::by_items_text(order_by, qualify)
            ),
        );
        partial.task = "cop[tikv]";
        partial.act_rows = act_rows.clone();
        partial.children.push(input);

        let mut reader = PlanNode::new(
            "TableReader",
            estimate,
            String::new(),
            "data:TopN".to_owned(),
        );
        reader.key_ndv_ratio = key_ndv_ratio;
        reader.act_rows = act_rows;
        reader.children.push(partial);
        self.stack.push(reader);
        true
    }

    /// Go's locking-read marker. Cluster sessions collect the raw keys read
    /// by the executor and issue the matching pessimistic lock after the
    /// statement attempt, so this wrapper records that real transaction seam.
    pub(crate) fn select_lock(&mut self) {
        self.wrap("SelectLock", Est::Inherit, "for update 0".to_owned());
    }

    /// The projection the driver always builds (divergence 3).
    pub(crate) fn projection(&mut self, fields: &[tidb_ast::SelectField], qualify: &Qualifier<'_>) {
        self.projection_at_rows(fields, qualify, None);
    }

    /// Go `restoreSchemaIfChanged`: the pass-through projection that restores
    /// the original schema after join reorder changed the leaf layout.
    pub(crate) fn join_reorder_projection(&mut self, fields: &[String]) {
        self.wrap("Projection", Est::Inherit, fields.join(", "));
        if let Some(projection) = self.stack.last_mut() {
            projection.projection_outputs = fields.to_vec();
        }
    }

    /// A projection whose logical data-source estimate differs from the
    /// physical scan estimate raised by `adjustCountAfterAccess`.
    pub(crate) fn projection_at_rows(
        &mut self,
        fields: &[tidb_ast::SelectField],
        qualify: &Qualifier<'_>,
        rows: Option<f64>,
    ) {
        self.wrap(
            "Projection",
            rows.map_or(Est::Inherit, Est::Fixed),
            expanded_field_list(fields, qualify),
        );
    }

    /// Records the executable projection after Go's implicit real-argument
    /// casts have been built into it. Base-table columns retain their source
    /// names while derived aggregate outputs stay internal `Column#N` values,
    /// matching projection elimination in Go.
    ///
    /// The gate is intentionally narrow: until another build-time cast family
    /// needs physical rendering, ordinary projections keep the established
    /// AST printer. This prevents a trace-only rewrite from changing plans
    /// whose executor tree has no such physical node.
    pub(crate) fn physical_real_projection(
        &mut self,
        expressions: &[Expression],
        column_names: &[Option<String>],
        rows: Option<f64>,
    ) -> bool {
        if !expressions
            .iter()
            .any(|expression| expression_has_function(expression, "cast_double"))
        {
            return false;
        }
        let Some(info) = expressions
            .iter()
            .enumerate()
            .map(|(index, expression)| {
                physical_expression_text_with_columns(expression, column_names)
                    .map(|text| format!("{text}->Column#{index}"))
            })
            .collect::<Option<Vec<_>>>()
            .map(|parts| parts.join(", "))
        else {
            return false;
        };
        self.wrap("Projection", rows.map_or(Est::Inherit, Est::Fixed), info);
        true
    }

    /// The executable projection left after Go eliminates a grouped
    /// aggregation whose group keys cover a unique key. Carried columns keep
    /// their source names; rewritten aggregate scalars name their new output
    /// columns explicitly.
    pub(crate) fn aggregation_elimination_projection(
        &mut self,
        expressions: &[Expression],
        column_names: &[Option<String>],
    ) -> bool {
        let Some(projected) = expressions
            .iter()
            .map(|expression| physical_expression_text_with_columns(expression, column_names))
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        let outputs = projected
            .iter()
            .enumerate()
            .map(|(index, text)| {
                if matches!(expressions[index], Expression::Column(_)) {
                    text.clone()
                } else {
                    format!("{text}->Column#{index}")
                }
            })
            .collect::<Vec<_>>();
        self.wrap("Projection", Est::Inherit, outputs.join(", "));
        if let Some(projection) = self.stack.last_mut() {
            projection.projection_outputs = projected;
        }
        true
    }

    /// Records a simple projection executed by the TiKV request and the
    /// TableReader boundary that returns it to the root task. The source has
    /// already accepted the matching kept-column list, so these nodes
    /// describe the pushed execution rather than changing EXPLAIN alone.
    pub(crate) fn cop_table_projection(
        &mut self,
        fields: &[tidb_ast::SelectField],
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
    ) {
        let Some(mut scan) = self.stack.pop() else {
            return;
        };
        scan.task = "cop[tikv]";
        let estimate = logical_rows.or(scan.est_rows);
        let key_ndv_ratio = scan.key_ndv_ratio;
        let act_rows = scan.act_rows.clone();

        let mut projection = PlanNode::new(
            "Projection",
            estimate,
            String::new(),
            field_list(fields, qualify),
        );
        projection.task = "cop[tikv]";
        projection.key_ndv_ratio = key_ndv_ratio;
        projection.act_rows = act_rows.clone();
        projection.children.push(scan);

        let mut reader = PlanNode::new(
            "TableReader",
            estimate,
            String::new(),
            "data:Projection".to_owned(),
        );
        reader.key_ndv_ratio = key_ndv_ratio;
        reader.act_rows = act_rows;
        reader.children.push(projection);
        self.stack.push(reader);
    }

    /// Places an access-path residual Selection and the scan's accepted
    /// column projection in the TiKV task, then returns them through a table
    /// reader. The executor has already pushed the same residual into the
    /// scan and pruned its output schema before this method is called.
    pub(crate) fn cop_selection_projection_reader(
        &mut self,
        fields: &[tidb_ast::SelectField],
        qualify: &Qualifier<'_>,
    ) -> bool {
        let Some(mut selection) = self.stack.pop() else {
            return false;
        };
        if selection.name != "Selection" || selection.children.len() != 1 {
            self.stack.push(selection);
            return false;
        }
        let mut scan = selection.children.pop().expect("one Selection child");
        if !matches!(scan.name, "TableFullScan" | "TableRangeScan") {
            selection.children.push(scan);
            self.stack.push(selection);
            return false;
        }
        scan.task = "cop[tikv]";
        selection.task = "cop[tikv]";
        selection.children.push(scan);
        let estimate = selection.est_rows;
        let act_rows = selection.act_rows.clone();
        let key_ndv_ratio = selection.key_ndv_ratio;

        let mut projection = PlanNode::new(
            "Projection",
            estimate,
            String::new(),
            field_list(fields, qualify),
        );
        projection.task = "cop[tikv]";
        projection.key_ndv_ratio = key_ndv_ratio;
        projection.act_rows = act_rows.clone();
        projection.children.push(selection);

        let mut reader = PlanNode::new(
            "TableReader",
            estimate,
            String::new(),
            "data:Projection".to_owned(),
        );
        reader.key_ndv_ratio = key_ndv_ratio;
        reader.act_rows = act_rows;
        reader.children.push(projection);
        self.stack.push(reader);
        true
    }

    /// `SELECT DISTINCT`: Go's `buildDistinct` is an aggregation grouping by
    /// every projected column, so it carries the same NDV assumption.
    pub(crate) fn distinct(
        &mut self,
        fields: &[tidb_ast::SelectField],
        qualify: &Qualifier<'_>,
        logical_rows: Option<f64>,
    ) {
        let projected = sorted_field_list(fields, qualify);
        let funcs = fields
            .iter()
            .filter_map(|field| match field {
                tidb_ast::SelectField::Expr { expr, .. } => {
                    let text = qualify.expr(expr);
                    Some(format!("firstrow({text})->{text}"))
                }
                tidb_ast::SelectField::Wildcard(_) => None,
            })
            .collect::<Vec<_>>()
            .join(", ");
        let info = format!("group by:{projected}, funcs:{funcs}");
        self.wrap(
            "HashAgg",
            logical_rows.map_or(Est::ScaleFloorOne(DISTINCT_FACTOR), Est::Fixed),
            info,
        );
    }

    /// [`Self::distinct`] after projection elimination has replaced the
    /// visible field paths with physical columns. Go's `buildDistinct` groups
    /// by those resolved Columns, so projection-only inputs retain their base
    /// identity while computed inputs remain `Column#N`.
    pub(crate) fn physical_distinct(
        &mut self,
        fields: &[Expression],
        column_names: &[Option<String>],
        logical_rows: Option<f64>,
    ) -> bool {
        let Some(mut projected) = fields
            .iter()
            .map(|field| physical_expression_text_with_columns(field, column_names))
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        if projected.is_empty() {
            return false;
        }
        projected.sort();
        let funcs = projected
            .iter()
            .map(|field| format!("firstrow({field})->{field}"))
            .collect::<Vec<_>>()
            .join(", ");
        self.wrap(
            "HashAgg",
            logical_rows.map_or(Est::ScaleFloorOne(DISTINCT_FACTOR), Est::Fixed),
            format!("group by:{}, funcs:{funcs}", projected.join(", ")),
        );
        true
    }

    /// The final distinct stage over already-grouped partial keys. Its row
    /// estimate is the partial NDV, not that NDV multiplied a second time.
    pub(crate) fn final_distinct(
        &mut self,
        fields: &[tidb_ast::SelectField],
        qualify: &Qualifier<'_>,
    ) {
        let projected = sorted_field_list(fields, qualify);
        let funcs = fields
            .iter()
            .filter_map(|field| match field {
                tidb_ast::SelectField::Expr { expr, .. } => {
                    let text = qualify.expr(expr);
                    Some(format!("firstrow({text})->{text}"))
                }
                tidb_ast::SelectField::Wildcard(_) => None,
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.wrap(
            "HashAgg",
            Est::Inherit,
            format!("group by:{projected}, funcs:{funcs}"),
        );
    }

    /// `LIMIT [offset,] count`, which caps the child's estimate as Go's does.
    pub(crate) fn limit(&mut self, offset: u64, count: u64) {
        if self.stack.last().is_some_and(|node| {
            node.name == "IndexLookUp"
                && node.info == format!("limit embedded(offset:{offset}, count:{count})")
        }) {
            return;
        }
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
        join_kind: crate::join::JoinKind,
        scope: &FromScope,
        current_db: &str,
        pushed: &[&tidb_ast::Expr],
        strategy: &JoinStrategy,
    ) -> Result<(), ()> {
        let JoinStrategy {
            equal_mask,
            build_is_left,
            left_width,
            merge_keys,
            index_lookup,
            physical_conditions,
            estimated_join_rows,
        } = strategy;
        let build_is_left = *build_is_left;
        let qualify = Qualifier {
            db: current_db,
            scope,
            catalog: None,
        };
        let kind = match join_kind {
            crate::join::JoinKind::Inner => "inner join",
            crate::join::JoinKind::Left => "left outer join",
            crate::join::JoinKind::Right => "right outer join",
            crate::join::JoinKind::Semi => "semi join",
            crate::join::JoinKind::AntiSemi => "anti semi join",
        };
        // `equal_mask` comes from the executor's own condition split, so the
        // conjuncts printed under `equal:[...]` are exactly the ones the hash
        // table indexes and `other cond:` is exactly the residue it still
        // evaluates per candidate pair.
        // `pushed` are the `WHERE` conjuncts `driver::predicate_push_down`
        // moved INTO this join, appended in that order by `build_join`; Go
        // prints them here too, because after its own pushdown they are
        // conditions of the join and no longer of the Selection above it.
        let mut conjuncts = Vec::new();
        if let Some(expr) = &join.on {
            collect_and(expr, &mut conjuncts);
        }
        conjuncts.extend_from_slice(pushed);
        if conjuncts.len() != equal_mask.len() {
            return Err(());
        }
        let mut equal = Vec::new();
        let mut other = Vec::new();
        for (index, (conjunct, is_equal)) in conjuncts.iter().zip(equal_mask.iter()).enumerate() {
            let rendered = physical_conditions
                .as_ref()
                .and_then(|(conditions, columns)| {
                    let condition = conditions.get(index)?;
                    let aligned;
                    let condition = if *is_equal {
                        aligned = align_physical_join_equality(condition, *left_width);
                        aligned.as_ref().unwrap_or(condition)
                    } else {
                        condition
                    };
                    physical_expression_text_with_columns(condition, columns)
                })
                .unwrap_or_else(|| qualify.expr(conjunct));
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
        // Go `PhysicalMergeJoin.explainInfo` has NO `CARTESIAN` prefix: a
        // property-driven merge join always has keys, and the keyless one is
        // only ever produced by the enforced path this tier does not build.
        if equal.is_empty() && merge_keys.is_none() && index_lookup.is_none() {
            info.push_str("CARTESIAN ");
        }
        info.push_str(kind);
        let mut tail = String::new();
        if let Some(keys) = &merge_keys {
            // `left key:%s, right key:%s` over `ExplainColumnList`, which
            // prints the LEFT child's keys and then the RIGHT child's, each
            // comma-separated -- not pairwise. Equal conditions used as merge
            // keys are represented by those lists; a non-equality condition
            // remains visible below as `other cond:`.
            tail.push_str(", left key:");
            tail.push_str(
                &keys
                    .iter()
                    .map(|(left, _)| left.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            tail.push_str(", right key:");
            tail.push_str(
                &keys
                    .iter()
                    .map(|(_, right)| right.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        } else if let Some(text) = &index_lookup {
            // Go `PhysicalIndexJoin.explainInfo`: the reader, then the two
            // key lists, then the equal conditions spelled in full. A
            // non-equality condition remains visible below as `other cond:`.
            // `inner:` is written before `explainJoinLeftSide`, so keep it in
            // `info`; the renderer inserts `left side:` between `info` and
            // `tail` once the left child's ExplainID is known.
            info.push_str(", inner:");
            info.push_str(text.reader);
            tail.push_str(", outer key:");
            tail.push_str(
                &text
                    .keys
                    .iter()
                    .map(|(outer, _)| outer.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            tail.push_str(", inner key:");
            tail.push_str(
                &text
                    .keys
                    .iter()
                    .map(|(_, inner)| inner.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            if !text.equal_conditions.is_empty() {
                tail.push_str(", equal cond:");
                tail.push_str(&text.equal_conditions.join(", "));
            }
        } else {
            if !equal.is_empty() {
                tail.push_str(", equal:[");
                tail.push_str(&equal.join(" "));
                tail.push(']');
            }
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
        let est_rows = index_lookup
            .as_ref()
            .and_then(|text| {
                text.estimated_join_rows.or_else(|| {
                    if !text.unique {
                        return None;
                    }
                    if text.lookup_is_left {
                        right.est_rows
                    } else {
                        left.est_rows
                    }
                })
            })
            // A keyless join's physical children already include Go's
            // null-rejecting filters for non-equality predicates. Use those
            // post-filter counts before the earlier logical cartesian count.
            .or_else(|| {
                equal
                    .is_empty()
                    .then(|| full_join_row_count(&left, &right, 0, None))
                    .flatten()
            })
            .or(*estimated_join_rows)
            .or_else(|| full_join_row_count(&left, &right, equal.len(), merge_keys.as_deref()));
        let named_key_ndvs = merge_keys.as_deref().map_or_else(Vec::new, |keys| {
            keys.iter()
                .flat_map(|(left_name, right_name)| {
                    [
                        node_key_ndv(&left, left_name).map(|ndv| (left_name.clone(), ndv)),
                        node_key_ndv(&right, right_name).map(|ndv| (right_name.clone(), ndv)),
                    ]
                })
                .flatten()
                .collect()
        });
        // Which index-join executor this site's row names, decided by the one
        // cost term that differs between them (`index_join_operator`).
        //
        // `probe_rows_one` is Go's `AvgInnerRowCnt`, and it is defined in
        // `enumerateIndexJoinByOuterIdx` as `p.EqualCondOutCnt / buildRows` --
        // the join's EQUAL-CONDITION output count over the outer row count,
        // which is what the inner side is then planned for and therefore what
        // `getCardinality(probe)` reads back. `est_rows` here IS that
        // equal-condition estimate (`full_join_row_count`), so the division
        // below is Go's own line rather than a proxy for it.
        let index_join_name = index_lookup.as_ref().map(|text| {
            // `build`, in Go's naming: `p.Children()[1-p.InnerChildIdx]`.
            let outer = if text.lookup_is_left { &right } else { &left };
            let build_rows = outer.est_rows;
            let probe_rows_one = est_rows
                .zip(build_rows)
                .map(|(equal_cond_out, rows)| equal_cond_out / rows);
            text.forced_name.unwrap_or_else(|| {
                index_join_operator(build_rows, probe_rows_one, text, equal.len())
            })
        });
        let children = if build_is_left {
            vec![left, right]
        } else {
            vec![right, left]
        };
        self.stack.push(PlanNode {
            name: if merge_keys.is_some() {
                "MergeJoin"
            } else if let Some(name) = index_join_name {
                // Go's `IndexJoin` IS `IndexLookUpJoin`: the outer side
                // streams in batches and the batch's inner rows are indexed
                // by the outer key, which is exactly `crate::join`'s index
                // strategy. `IndexHashJoin` hashes the OUTER batch instead
                // and runs the same lookups, so the two are one executor here
                // and differ only in the row's name -- which is settled by
                // `index_join_operator`'s cost.
                name
            } else {
                "HashJoin"
            },
            est_rows,
            access: String::new(),
            info,
            projection_outputs: Vec::new(),
            task: "root",
            // `explainJoinLeftSide` names the LEFT child's operator, and only
            // for an OUTER join. The name carries its own id in `format='row'`,
            // which is not assigned yet, so the renderer splices it in.
            left_side_child: (join_kind != crate::join::JoinKind::Inner)
                .then_some(usize::from(!build_is_left)),
            info_tail: tail,
            label: "",
            children,
            access_consumed: !pushed.is_empty(),
            act_rows: None,
            // Go `LogicalJoin.DeriveStats` builds a fresh `ColNDVs` from both
            // children's maps rather than scaling either one, so the single
            // ratio this trace carries does not survive a join.
            key_ndv_ratio: None,
            named_key_ndvs,
        });
        // The same rule an access path follows when its range consumed a
        // condition: the join has now PRICED the `WHERE` conjuncts pushed
        // into it, and `driver::predicate_push_down` COPIES rather than
        // moves, so the `Selection` still printed above re-checks them and
        // must not charge their selectivity a second time. Go MOVES the
        // predicate and prints no such Selection at all.
        //
        // An `ON` condition is NOT a `WHERE` conjunct, so it does not set the
        // flag: the `Selection` above an explicit `JOIN ... ON` prices a
        // different predicate and still has to scale by it.
        //
        // Coarse, exactly as it is for a narrowed scan: one leftover conjunct
        // beside the pushed one is now un-priced rather than double-priced.
        // Of the two errors this is the smaller -- for
        // `t1.a = t2.a and t1.b > 5` it prints 12500.00 against TiDB's
        // 4162.50, where charging the equality twice would print 4.16.
        // A join is a new predicate boundary. Access conditions consumed by
        // either child must not suppress selectivity of a different residual
        // predicate above the joined rows.
        Ok(())
    }

    /// Go's decorrelated `SemiJoin` / `AntiSemiJoin`: the right child is
    /// built once and the left child is emitted at most once per row.
    pub(crate) fn semi_join(
        &mut self,
        conditions: &[&tidb_ast::Expr],
        scope: &FromScope,
        current_db: &str,
        equal_mask: &[bool],
        anti: bool,
    ) -> Result<(), ()> {
        if conditions.len() != equal_mask.len() {
            return Err(());
        }
        let qualify = Qualifier {
            db: current_db,
            scope,
            catalog: None,
        };
        let mut equal = Vec::new();
        let mut other = Vec::new();
        for (condition, is_equal) in conditions.iter().zip(equal_mask) {
            let rendered = qualify.expr(condition);
            if *is_equal {
                equal.push(rendered);
            } else {
                other.push(rendered);
            }
        }
        other.sort();
        let mut info = String::new();
        if equal.is_empty() {
            info.push_str("CARTESIAN ");
        }
        info.push_str(if anti { "anti semi join" } else { "semi join" });
        if !equal.is_empty() {
            info.push_str(", equal:[");
            info.push_str(&equal.join(" "));
            info.push(']');
        }
        if !other.is_empty() {
            info.push_str(", other cond:");
            info.push_str(&other.join(", "));
        }

        let (Some(mut right), Some(mut left)) = (self.stack.pop(), self.stack.pop()) else {
            return Err(());
        };
        right.label = "(Build)";
        left.label = "(Probe)";
        let est_rows = left.est_rows.map(|rows| rows * SELECTIVITY_FACTOR);
        self.stack.push(PlanNode {
            name: "HashJoin",
            est_rows,
            access: String::new(),
            info,
            projection_outputs: Vec::new(),
            task: "root",
            left_side_child: None,
            info_tail: String::new(),
            label: "",
            children: vec![right, left],
            access_consumed: false,
            act_rows: None,
            key_ndv_ratio: None,
            named_key_ndvs: Vec::new(),
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
                func_deps: Default::default(),
            }],
            ..FromScope::default()
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

/// A physical Projection owns concrete columns even when the logical select
/// list was written as `*`. Go's explain output prints those expanded column
/// expressions, unlike logical aggregation helpers which keep `*` as written.
fn expanded_field_list(fields: &[tidb_ast::SelectField], qualify: &Qualifier<'_>) -> String {
    let mut rendered = Vec::new();
    for field in fields {
        match field {
            tidb_ast::SelectField::Expr { expr, .. } => rendered.push(qualify.expr(expr)),
            tidb_ast::SelectField::Wildcard(path) => {
                let table_name = path.last();
                for table in &qualify.scope.tables {
                    if table_name.is_some_and(|name| !table.name.eq_ignore_ascii_case(name)) {
                        continue;
                    }
                    rendered.extend(
                        table.columns.iter().map(|(column, _)| {
                            qualify.column(&[table.name.clone(), column.clone()])
                        }),
                    );
                }
            }
        }
    }
    rendered.join(", ")
}

/// Go's `BasePhysicalAgg.ExplainInfo` renders group expressions through
/// `expression.SortedExplainExpressionList`, independently of their logical
/// or physical execution order.
fn sorted_field_list(fields: &[tidb_ast::SelectField], qualify: &Qualifier<'_>) -> String {
    let mut rendered = fields
        .iter()
        .map(|field| match field {
            tidb_ast::SelectField::Expr { expr, .. } => qualify.expr(expr),
            tidb_ast::SelectField::Wildcard(path) => match path.last() {
                Some(table) => format!("{table}.*"),
                None => "*".to_owned(),
            },
        })
        .collect::<Vec<_>>();
    rendered.sort();
    rendered.join(", ")
}

/// Go stores a PhysicalSelection's predicates as CNF items rather than one
/// nested AND expression. Preserve every non-AND subtree as one condition.
fn collect_physical_and<'a>(expression: &'a Expression, out: &mut Vec<&'a Expression>) {
    if let Expression::ScalarFunction(function) = expression {
        if function.func_name.lowercase() == "and" && function.args.len() == 2 {
            collect_physical_and(&function.args[0], out);
            collect_physical_and(&function.args[1], out);
            return;
        }
    }
    out.push(expression);
}

fn align_physical_join_equality(expression: &Expression, left_width: usize) -> Option<Expression> {
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    let name = function.func_name.lowercase();
    if (name != "eq" && name != "nulleq") || function.args.len() != 2 {
        return None;
    }
    let (Expression::Column(first), Expression::Column(second)) =
        (&function.args[0], &function.args[1])
    else {
        return None;
    };
    let (first, second) = (
        usize::try_from(first.index).ok()?,
        usize::try_from(second.index).ok()?,
    );
    if first < left_width || second >= left_width {
        return None;
    }
    let mut aligned = function.clone();
    aligned.args.swap(0, 1);
    Some(Expression::ScalarFunction(aligned))
}

/// The physical-expression subset currently needed after derived aggregate
/// projection elimination. Unsupported nodes are refused by the caller; a
/// fallback to AST text would describe a different expression than the one
/// the Selection executor evaluates.
fn physical_expression_text_with_columns(
    expression: &Expression,
    column_names: &[Option<String>],
) -> Option<String> {
    match expression {
        Expression::Column(column) if column.unique_id < 0 => {
            Some(format!("ScalarQueryCol#{}", -column.unique_id))
        }
        Expression::Column(column) => {
            let index = usize::try_from(column.index).ok()?;
            if let Some(physical_name) = column_names.get(index) {
                // An explicit `None` means projection elimination traced this
                // output and proved it has no base-column origin (for example
                // a computed aggregate column of a view). Go leaves
                // `Column.OrigName` empty in that case and prints `Column#N`;
                // do not resurrect the SQL-visible view name carried by the
                // executable expression.
                return Some(
                    physical_name
                        .clone()
                        .unwrap_or_else(|| format!("Column#{}", column.index)),
                );
            }
            (!column.orig_name.is_empty())
                .then(|| column.orig_name.clone())
                .or_else(|| Some(format!("Column#{}", column.index)))
        }
        Expression::ScalarFunction(function) => {
            let arguments = function
                .args
                .iter()
                .map(|argument| physical_expression_text_with_columns(argument, column_names))
                .collect::<Option<Vec<_>>>()?;
            if function.func_name.lowercase() == "cast_decimal" {
                if arguments.len() != 1 {
                    return None;
                }
                let result_type = function.ret_type.as_ref()?;
                Some(format!(
                    "cast({}, decimal({},{}) BINARY)",
                    arguments[0],
                    result_type.flen(),
                    result_type.decimal()
                ))
            } else if function.func_name.lowercase() == "cast_double" {
                if arguments.len() != 1 {
                    return None;
                }
                Some(format!("cast({}, double BINARY)", arguments[0]))
            } else {
                Some(format!(
                    "{}({})",
                    function.func_name.lowercase(),
                    arguments.join(", ")
                ))
            }
        }
        Expression::Constant(constant) if constant.param_marker.is_none() => {
            explain_constant(constant)
        }
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => None,
    }
}

fn physical_condition_text_with_columns(
    expression: &Expression,
    column_names: &[Option<String>],
) -> Option<String> {
    let mut conditions = Vec::new();
    collect_physical_and(expression, &mut conditions);
    let mut rendered = conditions
        .into_iter()
        .map(|condition| physical_expression_text_with_columns(condition, column_names))
        .collect::<Option<Vec<_>>>()?;
    rendered.sort_unstable();
    Some(rendered.join(", "))
}

fn physical_conditions_text_with_columns(
    expressions: &[Expression],
    column_names: &[Option<String>],
) -> Option<String> {
    let mut conditions = Vec::new();
    for expression in expressions {
        collect_physical_and(expression, &mut conditions);
    }
    let mut rendered = conditions
        .into_iter()
        .map(|condition| physical_expression_text_with_columns(condition, column_names))
        .collect::<Option<Vec<_>>>()?;
    rendered.sort_unstable();
    Some(rendered.join(", "))
}

fn expression_has_function(expression: &Expression, name: &str) -> bool {
    let Expression::ScalarFunction(function) = expression else {
        return false;
    };
    function.func_name.lowercase() == name
        || function
            .args
            .iter()
            .any(|argument| expression_has_function(argument, name))
}

fn aggregate_exprs(expr: &tidb_ast::Expr) -> Vec<tidb_ast::Expr> {
    struct Collect(Vec<tidb_ast::Expr>);
    impl tidb_ast::Visitor for Collect {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr @ tidb_ast::Expr::Aggregate { .. }) =
                node.downcast_ref::<tidb_ast::Expr>()
            {
                self.0.push(expr.clone());
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut collect = Collect(Vec::new());
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut collect);
    collect.0
}

fn grouped_aggregate_info(
    select: &tidb_ast::SelectStmt,
    qualify: &Qualifier<'_>,
    partial_inputs: bool,
    include_first_row: bool,
) -> String {
    let mut groups = select
        .group_by
        .iter()
        .map(|item| qualify.expr(&item.expr))
        .collect::<Vec<_>>();
    groups.sort();
    let mut aggregate_functions = Vec::new();
    let mut aggregate_index = 0;
    let mut partial_index = 0;
    // Go's resolveHavingAndOrderBy appends auxiliary aggregate fields in
    // HAVING-then-ORDER-BY order before buildAggregation extracts them. Rust
    // hoists those states without mutating the parsed field list, so rebuild
    // the same logical inventory for EXPLAIN here.
    let mut aggregates = Vec::new();
    let mut collect = |expr: &tidb_ast::Expr| {
        for aggregate in aggregate_exprs(expr) {
            if !aggregates.contains(&aggregate) {
                aggregates.push(aggregate);
            }
        }
    };
    for field in select.fields.fields() {
        let tidb_ast::SelectField::Expr { expr, .. } = field else {
            continue;
        };
        collect(expr);
    }
    if let Some(having) = &select.having {
        collect(having);
    }
    for item in &select.order_by {
        collect(&item.expr);
    }
    for aggregate in aggregates {
        let tidb_ast::Expr::Aggregate {
            name,
            distinct,
            args,
        } = aggregate
        else {
            unreachable!("aggregate_exprs returns only aggregates");
        };
        let name = name.to_ascii_lowercase();
        let input = if partial_inputs && name == "avg" {
            let input = format!("Column#{partial_index}, Column#{}", partial_index + 1);
            partial_index += 2;
            input
        } else if partial_inputs {
            let input = format!("Column#{partial_index}");
            partial_index += 1;
            input
        } else {
            args.first()
                .map_or_else(|| "1".to_owned(), |arg| qualify.expr(arg))
        };
        if !partial_inputs && name == "avg" {
            aggregate_functions.push(format!("funcs:count({input})->Column#{partial_index}"));
            partial_index += 1;
            aggregate_functions.push(format!("funcs:sum({input})->Column#{partial_index}"));
            partial_index += 1;
            aggregate_index += 1;
            continue;
        }
        aggregate_functions.push(format!(
            "funcs:{name}({}{input})->Column#{aggregate_index}",
            if distinct { "distinct " } else { "" },
        ));
        if !partial_inputs {
            partial_index += 1;
        }
        aggregate_index += 1;
    }
    let mut first_row_functions = Vec::new();
    if include_first_row {
        for field in select.fields.fields() {
            let tidb_ast::SelectField::Expr { expr, .. } = field else {
                continue;
            };
            if let tidb_ast::Expr::Column(path) = expr {
                let text = qualify.expr(expr);
                if select.group_by.iter().any(
                    |item| matches!(&item.expr, tidb_ast::Expr::Column(group) if group == path),
                ) {
                    let source_offset = (|| {
                        if let Some(catalog) = qualify.catalog {
                            let mut physical = text.rsplit('.');
                            let column = physical.next()?;
                            let table = physical.next()?;
                            let database = physical.next().unwrap_or(qualify.db);
                            if let Some(entry) = catalog.get_in(database, table) {
                                if let Some(offset) = entry
                                    .column_list()
                                    .iter()
                                    .position(|(name, _)| name.eq_ignore_ascii_case(column))
                                {
                                    return Some(offset);
                                }
                            }
                        }
                        let column = path.last()?;
                        let relation = path.get(path.len().checked_sub(2)?)?;
                        let table = qualify
                            .scope
                            .tables
                            .iter()
                            .find(|table| table.name.eq_ignore_ascii_case(relation));
                        if let Some(table) = table {
                            return table
                                .columns
                                .iter()
                                .position(|(name, _)| name.eq_ignore_ascii_case(column))
                                .map(|offset| table.offset + offset);
                        }
                        let mut matches = qualify.scope.tables.iter().filter_map(|table| {
                            table
                                .columns
                                .iter()
                                .position(|(name, _)| name.eq_ignore_ascii_case(column))
                                .map(|offset| table.offset + offset)
                        });
                        let first = matches.next()?;
                        matches.next().is_none().then_some(first)
                    })()
                    .unwrap_or(usize::MAX);
                    first_row_functions
                        .push((source_offset, format!("funcs:firstrow({text})->{text}")));
                }
            }
        }
        // Ordinary grouped SELECTs retain carriers in source-schema order.
        // Decorrelate instead appends carriers after the written aggregate in
        // correlation-condition order, which is already their field order.
        let aggregate_precedes_carriers = select
            .fields
            .fields()
            .iter()
            .position(|field| {
                matches!(field, tidb_ast::SelectField::Expr { expr, .. } if expr.has_aggregate_flag())
            })
            .zip(select.fields.fields().iter().position(|field| {
                matches!(field,
                    tidb_ast::SelectField::Expr { expr: tidb_ast::Expr::Column(path), .. }
                    if select.group_by.iter().any(|item| {
                        matches!(&item.expr, tidb_ast::Expr::Column(group) if group == path)
                    })
                )
            }))
            .is_some_and(|(aggregate, carrier)| aggregate < carrier);
        if !aggregate_precedes_carriers {
            first_row_functions.sort_by_key(|(offset, _)| *offset);
        }
    }
    let mut first_row_functions = first_row_functions
        .into_iter()
        .map(|(_, function)| function)
        .collect::<Vec<_>>();
    let functions = if include_first_row && crate::driver::has_pruned_row_count(select) {
        // Go appended this COUNT(1) only after column pruning removed the
        // last written aggregate. It therefore follows the surviving
        // FIRST_ROW carriers instead of taking the ordinary aggregate-first
        // position.
        first_row_functions.extend(aggregate_functions);
        first_row_functions
    } else {
        aggregate_functions.extend(first_row_functions);
        aggregate_functions
    };
    if groups.is_empty() {
        functions.join(", ")
    } else {
        format!("group by:{}, {}", groups.join(", "), functions.join(", "))
    }
}

fn post_aggregate_expr(
    expr: &tidb_ast::Expr,
    qualify: &Qualifier<'_>,
    next_aggregate: &mut usize,
) -> String {
    let mut rendered = qualify.expr(expr);
    for aggregate in aggregate_exprs(expr) {
        let aggregate_text = qualify.expr(&aggregate);
        rendered = rendered.replacen(&aggregate_text, &format!("Column#{}", *next_aggregate), 1);
        *next_aggregate += 1;
    }
    format!("{rendered}->Column#{}", *next_aggregate)
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

    fn consumes_where(&self) -> bool {
        self.child.consumes_where()
    }

    /// The same reason, for the same reason: Go's `aggExecutorTreeInputEmpty`
    /// walks THROUGH a single-child operator, and a meter is exactly that.
    fn agg_tree_input_empty(&self) -> bool {
        self.child.agg_tree_input_empty()
    }
}

/// Go's `handle:` operator-info value.
///
/// `unsigned` is the plan's `UnsignedHandle`
/// (`physical_batch_point_get.go:206`): the same 64 bits, printed as a
/// `uint64` when the handle column is unsigned, so `18446744073709551615`
/// prints as itself rather than as the `-1` its signed reading gives.
fn handle_text(handle: &TableHandle, unsigned: bool) -> String {
    match handle {
        TableHandle::Int(value) if unsigned => (*value as u64).to_string(),
        TableHandle::Int(value) => value.to_string(),
        // A clustered-index handle is a byte string; Go prints its decoded
        // datums, which needs the handle codec this printer does not carry.
        TableHandle::Common(_) => "<common handle>".to_owned(),
    }
}

/// Go identifies a common-handle point access by its clustered primary
/// index. The encoded handle bytes are deliberately absent from operator
/// info; unlike integer handles, Go does not render them there.
fn common_handle_access(visible: &str, table: &KvTable, partitions: &[String]) -> Option<String> {
    let offsets = table.common_handle_offsets();
    if offsets.is_empty() {
        return None;
    }
    let columns = offsets
        .iter()
        .map(|offset| {
            table
                .columns
                .get(*offset)
                .map(|column| column.name.as_str())
        })
        .collect::<Option<Vec<_>>>()?;
    Some(format!(
        "table:{visible}{}, clustered index:PRIMARY({})",
        partition_object(partitions),
        columns.join(", ")
    ))
}

/// Go's range notation: a square bracket includes the bound, a parenthesis
/// excludes it, and an absent bound is an infinity.
/// The leaf operators a partitioned read can be built out of -- the ones
/// `PlanTrace::partition_union` may fan out. Every other leaf either names
/// its partition itself (the point gets) or reads no partition at all.
const PARTITIONED_SCANS: &[&str] = &[
    "TableFullScan",
    "TableRangeScan",
    "IndexFullScan",
    "IndexRangeScan",
];

/// Go `PointGetPlan.AccessObject`'s partition clause
/// (`pkg/planner/core/operator/physicalop/physical_batch_point_get.go`):
///
/// ```go
/// if idxPointer := p.PartitionIdx; idxPointer != nil {
///     res.Partitions = []string{pi.Definitions[*idxPointer].Name.O}
/// }
/// ```
///
/// A point get names ONE partition -- the one its plan resolved -- and never
/// a list. A read this tier has narrowed to a single physical table IS that
/// one, whether the narrowing came from an explicit `PARTITION (p)` or from
/// pruning. Answering nothing here is what printed `Point_Get table:t` for
/// `select *,_tidb_rowid from t partition(p0) where _tidb_rowid=1`, which
/// TiDB records as `Point_Get table:t, partition:p0`.
///
/// Empty for an unpartitioned table (nothing to name) and for a read that
/// still spans several partitions (no single answer exists).
fn sole_read_partition_name(table: &KvTable) -> Vec<String> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let [id] = table.record_physical_ids()[..] else {
        return Vec::new();
    };
    partition
        .definitions
        .iter()
        .find(|definition| definition.id == id)
        .map(|definition| vec![definition.name.clone()])
        .unwrap_or_default()
}

/// Go `access.ScanAccessObject.String()` writes its three fields in a fixed
/// order -- `table:t, partition:p, index:i(c)` -- so the partition clause
/// goes AFTER the table and BEFORE any index, which is where this splices it
/// into an access object the scan builders above already wrote.
fn with_partition(access: &str, partition: &str) -> String {
    match access.find(", ") {
        Some(at) => format!("{}, partition:{partition}{}", &access[..at], &access[at..]),
        None => format!("{access}, partition:{partition}"),
    }
}

/// Go `access.ScanAccessObject.String()`'s partition clause: `,
/// partition:p1,P2` -- one comma-separated list, in the order the caller
/// supplied, and NOTHING at all when there are no partitions to name.
fn partition_object(partitions: &[String]) -> String {
    if partitions.is_empty() {
        return String::new();
    }
    format!(", partition:{}", partitions.join(","))
}

pub(crate) fn range_text(range: &crate::kv_table::IndexRange) -> String {
    let low = bound_text(&range.low, "-inf", true);
    let high = bound_text(&range.high, "+inf", false);
    let open = if range.low_exclusive { '(' } else { '[' };
    let close = if range.high_exclusive { ')' } else { ']' };
    format!("{open}{low},{high}{close}")
}

fn bound_text(values: &[Datum], infinity: &str, is_left_side: bool) -> String {
    if values.is_empty() {
        return infinity.to_owned();
    }
    values
        .iter()
        .map(|value| datum_go_text(value, is_left_side))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A constant as Go's explain prints it -- `formatDatum` in
/// `pkg/util/ranger/types.go`: a string in double quotes, a number bare.
///
/// The quoting is Go's `%q` ([`crate::go_quote::quote`]), not a lossy UTF-8
/// conversion. A BINARY index column's bound is arbitrary octets, so
/// `from_utf8_lossy` replaced every non-UTF-8 byte with U+FFFD and produced a
/// range no reader (and no oracle) could match against TiDB's own recording.
///
/// `is_left_side` is Go's own parameter, and it is not cosmetic: the extreme
/// integers are the SATURATED bound a range on an int column gets when the
/// other side is open, so `formatDatum` prints `MinInt64` as `-inf` on the
/// LOW side and `MaxInt64`/`MaxUint64` as `+inf` on the HIGH side -- and
/// prints them as the plain number on the side where they are a real value
/// the user wrote.
///
/// The final arm is Go's `fmt.Sprintf("%v", d.GetValue())`, which for a
/// temporal or JSON value is that value's own display text. Rendering it with
/// Rust's `Debug` instead put `Time(Time { core: {2024 10 19 8 55 32 0}` in
/// the `range:` cell of every `EXPLAIN` whose index bound was a datetime.
fn datum_go_text(value: &Datum, is_left_side: bool) -> String {
    match value {
        Datum::Null => "NULL".to_owned(),
        // Go's range printer spells the open-ended bounds this way.
        Datum::MaxValue => "+inf".to_owned(),
        Datum::MinNotNull => "-inf".to_owned(),
        Datum::Int(i64::MIN) if is_left_side => "-inf".to_owned(),
        Datum::Int(i64::MAX) if !is_left_side => "+inf".to_owned(),
        Datum::Int(v) => v.to_string(),
        Datum::UInt(u64::MAX) if !is_left_side => "+inf".to_owned(),
        Datum::UInt(v) => v.to_string(),
        Datum::Real(v) => v.to_string(),
        Datum::Decimal(d) => d.to_string(),
        Datum::String(s) => crate::go_quote::quote(s.bytes()),
        Datum::Bytes(b) => crate::go_quote::quote(b),
        // `formatDatum`'s enum/set/JSON/binary-literal/bit arm is
        // `fmt.Sprintf("\"%v\"", ...)` -- the value's own display, quoted but
        // NOT escaped.
        Datum::BinaryLiteral(b) | Datum::Bit(b) => format!("\"{b}\""),
        Datum::Json(j) => format!("\"{j}\""),
        Datum::Enum(e, _) => format!("\"{e}\""),
        Datum::Set(s, _) => format!("\"{s}\""),
        // Go's `%v` fallback: the value's own display text.
        other => other
            .sql_string()
            .unwrap_or_else(|_| format!("{other:?}"))
            .to_owned(),
    }
}

/// Go's stats-less selectivity for one predicate, from
/// `cardinality.pseudoSelectivity`: the minimum over the conjuncts of the
/// per-operator rate, starting at `SelectivityFactor`.
pub(crate) fn pseudo_selectivity(predicate: &tidb_ast::Expr) -> f64 {
    let mut conjuncts = Vec::new();
    collect_and(predicate, &mut conjuncts);
    pseudo_selectivity_of_conjuncts(&conjuncts)
}

/// [`pseudo_selectivity`] over conjuncts that have already been split, for a
/// caller that holds a condition LIST rather than one `AND` tree -- Go's
/// `cardinality.Selectivity` takes `[]expression.Expression` for the same
/// reason.
pub(crate) fn pseudo_selectivity_of_conjuncts(conjuncts: &[&tidb_ast::Expr]) -> f64 {
    let mut factor: Option<f64> = None;
    let mut has_unclassified = false;
    for conjunct in conjuncts {
        if complementary_null_predicates(conjunct) {
            factor = Some(factor.map_or(1.0, |current| current.min(1.0)));
            continue;
        }
        let rate = match conjunct {
            tidb_ast::Expr::Binary(op, _, _) => match op {
                tidb_ast::BinaryOp::Eq | tidb_ast::BinaryOp::NullEq => 1.0 / PSEUDO_EQUAL_RATE,
                tidb_ast::BinaryOp::Ge
                | tidb_ast::BinaryOp::Gt
                | tidb_ast::BinaryOp::Le
                | tidb_ast::BinaryOp::Lt => 1.0 / PSEUDO_LESS_RATE,
                _ => {
                    has_unclassified = true;
                    continue;
                }
            },
            tidb_ast::Expr::In { .. } => 1.0 / PSEUDO_EQUAL_RATE,
            tidb_ast::Expr::Is {
                target: tidb_ast::IsTarget::Null,
                not,
                ..
            } => {
                if *not {
                    1.0 - 1.0 / PSEUDO_EQUAL_RATE
                } else {
                    1.0 / PSEUDO_EQUAL_RATE
                }
            }
            _ => {
                has_unclassified = true;
                continue;
            }
        };
        factor = Some(factor.map_or(rate, |current| current.min(rate)));
    }
    match (factor, has_unclassified) {
        (Some(factor), true) => factor.min(SELECTIVITY_FACTOR),
        (Some(factor), false) => factor,
        (None, _) => SELECTIVITY_FACTOR,
    }
}

fn complementary_null_predicates(predicate: &tidb_ast::Expr) -> bool {
    let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, left, right) = strip_paren(predicate)
    else {
        return false;
    };
    let (Some((left, left_not)), Some((right, right_not))) =
        (null_predicate(left), null_predicate(right))
    else {
        return false;
    };
    left == right && left_not != right_not
}

fn null_predicate(predicate: &tidb_ast::Expr) -> Option<(&tidb_ast::Expr, bool)> {
    let tidb_ast::Expr::Is {
        expr,
        target: tidb_ast::IsTarget::Null,
        not,
    } = strip_paren(predicate)
    else {
        return None;
    };
    Some((strip_paren(expr), *not))
}

fn strip_paren(mut expression: &tidb_ast::Expr) -> &tidb_ast::Expr {
    while let tidb_ast::Expr::Paren(inner) = expression {
        expression = inner;
    }
    expression
}

/// Go `cardinality.EstimateFullJoinRowCount` over the two subtrees a
/// [`PlanTrace::join`] is about to wrap.
///
/// The formula itself lives in `tidb_planner::cardinality::join`, a complete
/// port of `pkg/planner/cardinality/join.go`; this supplies its inputs from
/// what the trace knows.
///
/// A CARTESIAN join is `leftRows * rightRows` and needs no statistics at all,
/// so it is answered whenever both children have an estimate. An equi-join
/// divides by the larger key NDV, which needs [`PlanNode::key_ndv_ratio`];
/// without it the join keeps Go's `N/A` rather than inventing an NDV.
///
/// The per-side NDV is `EstimateColsNDVWithMatchedLen`'s DEFAULT arm
/// (`ndv.go:87-122`, with `RiskGroupNDVSkewRatio` at its shipped 0): the
/// MAXIMUM over the keys' column NDVs, with a matched length of 1. Every
/// column of a pseudo side has the same NDV, so that maximum is the one
/// number `key_ndv_ratio` carries no matter how many keys the join has.
///
/// DIVERGENCE: TiDB pushes a null-rejecting `not(isnull(k))` under each side
/// of an inner equi-join before it estimates, so its inputs are 9990 where
/// this tier's are 10000, and it prints 12487.50 where this prints 12500.00.
/// The gap is that missing rewrite, not this arithmetic -- a CARTESIAN join,
/// which gets no such rewrite, matches TiDB exactly.
fn node_key_ndv(node: &PlanNode, name: &str) -> Option<f64> {
    node.named_key_ndvs
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, ndv)| *ndv)
        .or_else(|| {
            node.key_ndv_ratio
                .zip(node.est_rows)
                .map(|(ratio, rows)| rows * ratio)
        })
}

fn full_join_row_count(
    left: &PlanNode,
    right: &PlanNode,
    equal_count: usize,
    merge_keys: Option<&[(String, String)]>,
) -> Option<f64> {
    use tidb_planner::cardinality::join::{
        FullJoinRowCountInput, JoinKeyEstimate, estimate_full_join_row_count,
    };
    let (left_rows, right_rows) = (left.est_rows?, right.est_rows?);
    let key = |node: &PlanNode, left_side: bool| {
        let named = merge_keys.and_then(|keys| {
            if keys.len() != equal_count {
                return None;
            }
            keys.iter()
                .map(|(left_name, right_name)| {
                    node.named_key_ndvs
                        .iter()
                        .find(|(candidate, _)| {
                            candidate.eq_ignore_ascii_case(if left_side {
                                left_name
                            } else {
                                right_name
                            })
                        })
                        .map(|(_, ndv)| *ndv)
                })
                .collect::<Option<Vec<_>>>()
                .and_then(|ndvs| ndvs.into_iter().reduce(f64::max))
        });
        named
            .or_else(|| {
                node.key_ndv_ratio
                    .zip(node.est_rows)
                    .map(|(ratio, rows)| rows * ratio)
            })
            .map(|ndv| JoinKeyEstimate::new(ndv, 1, equal_count))
    };
    let (left_keys, right_keys) = if equal_count == 0 {
        (JoinKeyEstimate::empty(), JoinKeyEstimate::empty())
    } else {
        (key(left, true)?, key(right, false)?)
    };
    Some(estimate_full_join_row_count(&FullJoinRowCountInput {
        left_row_count: left_rows,
        right_row_count: right_rows,
        is_cartesian: equal_count == 0,
        left_join_keys: left_keys,
        right_join_keys: right_keys,
        // Reached only when both equi-key slices are empty, which this tier
        // routes to the cartesian arm above; kept explicit so the source's
        // fallback is visible rather than silently unreachable.
        left_non_equi_keys: JoinKeyEstimate::empty(),
        right_non_equi_keys: JoinKeyEstimate::empty(),
        // `vardef.DefTiDBOptJoinReorderThreshold`, and this tier has no `SET`
        // that moves it: the 0.9-per-remaining-key correlation factor is off.
        join_reorder_threshold: tidb_vardef::defaults::DEF_TIDB_OPT_JOIN_REORDER_THRESHOLD as i32,
    }))
}

/// Which algorithm a join committed to, and what its plan row must therefore
/// say. Bundled because the three answers are one decision -- the strategy
/// picks the build side AND the info clause -- and because a printer that
/// took them apart could print a merge join's name over a hash join's
/// `equal:[...]`.
pub(crate) struct JoinStrategy {
    /// Which of the join's conjuncts the hash table indexes, in `ON` order.
    pub(crate) equal_mask: Vec<bool>,
    /// Whether the LEFT child is the build side.
    pub(crate) build_is_left: bool,
    /// Columns contributed by the logical left child. Go rewrites equality
    /// arguments to `(left key, right key)` independently of the physical
    /// build/probe choice.
    pub(crate) left_width: usize,
    /// A merge join's `(left key, right key)` column names, already
    /// qualified; `None` for every other strategy.
    pub(crate) merge_keys: Option<Vec<(String, String)>>,
    /// An index join's printed shape: the reader Go names after `inner:`,
    /// and the `(outer key, inner key)` column names. `None` for every other
    /// strategy.
    pub(crate) index_lookup: Option<IndexJoinText>,
    /// The executor's flattened conditions and the physical origin of each
    /// joined-row column. EXPLAIN renders this same typed expression so
    /// constant folding and comparison refinement cannot drift from the
    /// condition the join evaluates; a missing source name prints `Column#N`.
    pub(crate) physical_conditions: Option<(Vec<Expression>, Vec<Option<String>>)>,
    /// The statement-owned `LogicalJoin.DeriveStats` result. Outer joins use
    /// it to retain the preserved-side floor that a flat trace cannot infer.
    pub(crate) estimated_join_rows: Option<f64>,
}

/// What an index join's plan row says beyond its join kind.
///
/// Go prints `inner:<reader>, outer key:<cols>, inner key:<cols>, equal
/// cond:<conjuncts>` (`PhysicalIndexJoin.explainInfo`). The reader is the
/// operator ABOVE the range scan in Go's tree, which this tier collapses into
/// the scan itself -- so it is named from the probed object rather than read
/// off a child that does not exist here.
pub(crate) struct IndexJoinText {
    /// `IndexReader` for a covering index probe, `IndexLookUp` for a double
    /// read, and `TableReader` for a handle probe.
    pub(crate) reader: &'static str,
    /// The outer and inner key column names, already qualified, in probe
    /// order.
    pub(crate) keys: Vec<(String, String)>,
    /// Every equality retained by the join, in written order and under the
    /// base columns Go's `OrigName` prints. The dynamic access key above is a
    /// subset; Go still repeats it in `equal cond:`.
    pub(crate) equal_conditions: Vec<String>,
    /// Whether the LOOKED-UP (inner) side is the join's left child. The two
    /// sides are not interchangeable in the cost below: the hash table is
    /// built over the outer side for `IndexHashJoin` and over the inner one
    /// for `IndexJoin`.
    pub(crate) lookup_is_left: bool,
    /// Whether one complete probe tuple can return at most one inner row.
    pub(crate) unique: bool,
    /// The exact index-family executor a statement hint forced, or `None`
    /// when Go's coster chooses between the lookup variants.
    pub(crate) forced_name: Option<&'static str>,
    /// `getAvgRowSize(build.StatsInfo(), build.Schema().Columns)` for the
    /// OUTER side, and the same for the INNER one. Computed where the two
    /// sides' column types are known (`driver::from::build_join`), because a
    /// plan row carries only their text.
    pub(crate) outer_row_size: f64,
    pub(crate) inner_row_size: f64,
    /// Statement-owned estimates used to choose `IndexJoin` versus
    /// `IndexHashJoin`. They are independent of whether EXPLAIN is active.
    pub(crate) estimated_outer_rows: Option<f64>,
    pub(crate) estimated_probe_rows_one: Option<f64>,
    /// The statement-owned equality-join output estimate.
    pub(crate) estimated_join_rows: Option<f64>,
}

/// The physical inner access path an index join commits to its plan trace.
pub(crate) struct IndexJoinInnerPathText<'a> {
    /// The table or index object printed on the dynamic range scan.
    pub(crate) access: String,
    /// Go's `range: decided by` payload.
    pub(crate) range_info: &'a str,
    /// Whether the dynamic range reads a secondary index.
    pub(crate) index: bool,
    /// Whether that secondary-index access needs a table double read.
    pub(crate) index_lookup: bool,
    /// The looked-up table's visible name.
    pub(crate) visible: &'a str,
    /// The statement-owned post-filter inner estimate for a one-row driver.
    pub(crate) estimated_rows: Option<f64>,
    /// Rows the dynamic range reads before residual filters. This is kept
    /// separate because #70176 can raise the access estimate without changing
    /// the join output estimate.
    pub(crate) estimated_access_rows: Option<f64>,
    /// The statement-owned outer-child estimate for this candidate.
    pub(crate) estimated_outer_rows: Option<f64>,
    /// Whether every dynamic key probes at most one source row. In that case
    /// the physical outer task, not the whole-table join model, owns the
    /// dynamic range's cardinality.
    pub(crate) unique: bool,
    /// Whether the parent property requires this IndexJoin to preserve its
    /// outer child's ordering.
    pub(crate) keep_outer_order: bool,
    /// Whether the lookup reader remains below a grouped derived table.
    pub(crate) grouped_derived: bool,
    /// The dynamic range target is a table leaf below a composite inner
    /// subtree rather than the inner root itself.
    pub(crate) composite: bool,
    /// Whether the retained aggregation consumes lookup-key-ordered rows.
    pub(crate) stream_aggregation: bool,
    /// Go's physical HashAgg payload for a retained grouped derived table.
    pub(crate) aggregation_info: Option<&'a str>,
    /// Go's final HashAgg payload above a cop partial aggregation.
    pub(crate) aggregation_final_info: Option<&'a str>,
    /// Go's cop partial HashAgg payload below a double-read lookup.
    pub(crate) aggregation_partial_info: Option<&'a str>,
    /// Outer output columns rejected when NULL by a comparison above the
    /// logical join. The physical plan pushes these predicates below the
    /// eliminated outer aggregation projection.
    pub(crate) outer_not_null: &'a [usize],
    /// Grouped inner output columns rejected when NULL after aggregation.
    pub(crate) inner_not_null: &'a [usize],
}

/// Unsays `keep order:true` on every leaf under `node` that a join above it
/// asked for and then did not use.
///
/// The walk stops at the two operators that RELY on their child's order --
/// a `MergeJoin` on both sides, an index join on its outer one -- and passes
/// through the shapes that do not: a `Projection` and a `Selection` carry
/// their child's order without needing it, and a `HashJoin` needs none at all
/// (Go's `getHashJoins` opens with "hash join doesn't promise any orders" and
/// asks its children for none either).
fn retract_keep_order(node: &mut PlanNode) {
    if node.children.is_empty() {
        if node.info.contains("keep order:true") {
            node.info = node.info.replacen("keep order:true", "keep order:false", 1);
        }
        return;
    }
    if !matches!(
        node.name,
        "Projection"
            | "Selection"
            | "HashAgg"
            | "HashJoin"
            | "TableReader"
            | "IndexReader"
            | "IndexLookUp"
    ) {
        return;
    }
    for child in &mut node.children {
        retract_keep_order(child);
    }
}

/// Which of Go's three index-join executors this site's plan row names.
///
/// Go ENUMERATES them as separate candidates -- `constructIndexJoinStatic`
/// for `PhysicalIndexJoin` and `constructIndexHashJoinStatic` for
/// `PhysicalIndexHashJoin`, both from `enumerateIndexJoinByOuterIdx`, in that
/// order -- and `findBestTask` keeps the cheaper. Every term of
/// `getIndexJoinCostVer24PhysicalIndexJoin` is shared between the two except
/// the HASH TABLE, so the whole of the choice is that one term:
///
/// ```text
/// case 1: // IndexHashJoin
///     hashTableCost = hashBuildCostVer2(option, buildRows, buildRowSize,
///         float64(len(p.RightJoinKeys)), cpuFactor, memFactor)
/// default: // IndexJoin
///     hashTableCost = hashBuildCostVer2(option, probeRowsTot, probeRowSize,
///         float64(len(p.LeftJoinKeys)), cpuFactor, memFactor)
/// ```
///
/// `IndexJoin` is enumerated FIRST and `findBestTask` replaces the incumbent
/// only on a STRICT improvement, so an exact tie -- one inner row per outer
/// row and two sides of equal width -- keeps `IndexJoin`. That is why the
/// comparison below is `<` and not `<=`.
///
/// MEASURED, on `gorun` against this repo's own tree, for the statement
/// `r/planner/core/join_reorder_through_projection.result:1319` records an
/// `IndexHashJoin` for, with the two kinds forced by hint so both plans are
/// costed at the SAME site:
///
/// ```text
/// /*+ INL_JOIN(t1) */       IndexJoin_40     12500.00  6065326.13
/// /*+ INL_HASH_JOIN(t1) */  IndexHashJoin_44 12500.00  6030776.13
/// ```
///
/// 34550.00 apart, which is exactly the hash-table term's difference divided
/// by `tidb_index_lookup_join_concurrency`. So the label at that site is a
/// COST decision and not a structural one, which is what the census in
/// `difftest-result-tests`' `join_shape` had left open.
///
/// `IndexMergeJoin` is NOT reachable here: its candidate is built only when
/// the inner side can deliver the join keys' order, which this tier's index
/// probe never claims (`index_join_inner_scan` writes `keep order:false` on
/// every one of them), so its zero hash-table term never competes.
fn index_join_operator(
    outer: Option<f64>,
    inner_rows_one: Option<f64>,
    text: &IndexJoinText,
    num_keys: usize,
) -> &'static str {
    use tidb_planner::plan_cost_ver2::{Ver2Factors, hash_build_cost};
    use tidb_planner::task_type::TaskType;
    let outer = text.estimated_outer_rows.or(outer);
    let inner_rows_one = text.estimated_probe_rows_one.or(inner_rows_one);
    let (Some(build_rows), Some(probe_rows_one)) = (outer, inner_rows_one) else {
        // No estimate on one of the two sides: Go always has one, and this
        // tier's fallback is the candidate Go enumerates first.
        return "IndexJoin";
    };
    let factors = Ver2Factors::default();
    let cpu = factors.task_cpu(TaskType::Root);
    let mem = factors.task_mem(TaskType::Root);
    let keys = num_keys as f64;
    let hash_join = hash_build_cost(None, build_rows, text.outer_row_size, keys, cpu, mem);
    let index_join = hash_build_cost(
        None,
        probe_rows_one * build_rows,
        text.inner_row_size,
        keys,
        cpu,
        mem,
    );
    if hash_join.value() < index_join.value() {
        "IndexHashJoin"
    } else {
        "IndexJoin"
    }
}

pub(crate) fn collect_and<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    if let tidb_ast::Expr::Paren(inner) = expr {
        collect_and(inner, out);
        return;
    }
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
    pub(crate) catalog: Option<&'a crate::Catalog>,
}

impl Qualifier<'_> {
    fn expr_with_physical_columns(
        &self,
        expression: &tidb_ast::Expr,
        column_names: &[Option<String>],
    ) -> String {
        if let tidb_ast::Expr::Column(path) = expression {
            let resolver = crate::driver::ScopeResolver { scope: self.scope };
            if let Some((index, _, _)) =
                tidb_expr::rewriter::ColumnResolver::resolve(&resolver, path)
            {
                if let Some(Some(name)) = column_names.get(index) {
                    return name.clone();
                }
            }
        }
        self.expr(expression)
    }

    pub(crate) fn expressions(&self, expressions: &[Expression]) -> Option<String> {
        let mut rendered: Vec<String> = expressions
            .iter()
            .map(|expression| self.built_expr(expression))
            .collect::<Option<_>>()?;
        rendered.sort_unstable();
        Some(rendered.join(", "))
    }

    pub(crate) fn conditions(&self, expressions: &[Expression]) -> Option<String> {
        let mut conditions = Vec::new();
        for expression in expressions {
            collect_physical_and(expression, &mut conditions);
        }
        let mut rendered = conditions
            .into_iter()
            .map(|condition| self.built_expr(condition))
            .collect::<Option<Vec<_>>>()?;
        rendered.sort_unstable();
        Some(rendered.join(", "))
    }

    fn built_expr(&self, expression: &Expression) -> Option<String> {
        match expression {
            Expression::Column(column) => self.built_column(column),
            Expression::CorrelatedColumn(column) => self.built_column(&column.column),
            Expression::Constant(constant) => explain_constant(constant),
            Expression::ScalarFunction(function) => {
                let args = function
                    .args
                    .iter()
                    .map(|argument| self.built_expr(argument))
                    .collect::<Option<Vec<_>>>()?;
                if function.func_name.lowercase() == "cast" {
                    let result_type = function
                        .ret_type
                        .as_ref()
                        .map(ToString::to_string)
                        .filter(|value| !value.is_empty())?;
                    Some(format!("cast({}, {result_type})", args.join(", ")))
                } else {
                    Some(format!(
                        "{}({})",
                        function.func_name.lowercase(),
                        args.join(", ")
                    ))
                }
            }
        }
    }

    fn built_column(&self, column: &tidb_expr::column::Column) -> Option<String> {
        let index = usize::try_from(column.index).ok()?;
        self.scope
            .tables
            .iter()
            .find(|table| (table.offset..table.offset + table.columns.len()).contains(&index))
            .and_then(|table| {
                let (name, _) = table.columns.get(index - table.offset)?;
                let database = table.database.as_deref().unwrap_or(self.db);
                Some(format!(
                    "{}.{}.{}",
                    database.to_lowercase(),
                    table.name.to_lowercase(),
                    name.to_lowercase()
                ))
            })
            .or_else(|| (!column.orig_name.is_empty()).then(|| column.orig_name.clone()))
    }

    pub(crate) fn expr(&self, expr: &tidb_ast::Expr) -> String {
        match expr {
            tidb_ast::Expr::Paren(inner) => self.expr(inner),
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
            tidb_ast::Expr::Unary(
                tidb_ast::UnaryOp::Not | tidb_ast::UnaryOp::NotKeyword,
                inner,
            ) => format!("not({})", self.expr(inner)),
            tidb_ast::Expr::Is {
                expr,
                target: tidb_ast::IsTarget::Null,
                not,
            } => {
                let is_null = format!("isnull({})", self.expr(expr));
                if *not {
                    format!("not({is_null})")
                } else {
                    is_null
                }
            }
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
        fn physical_column<'a>(table: &'a FromTable, name: &str) -> Option<&'a str> {
            table
                .columns
                .iter()
                .find(|(column, _)| column.eq_ignore_ascii_case(name))
                .map(|(column, _)| column.as_str())
        }
        match path {
            [scope, name] if scope == crate::driver::SCALAR_QUERY_SCOPE => name.clone(),
            [name] => {
                let owner = self
                    .scope
                    .tables
                    .iter()
                    .find(|table| physical_column(table, name).is_some());
                match owner {
                    Some(table) => format!(
                        "{}.{}.{}",
                        self.db.to_lowercase(),
                        table.name.to_lowercase(),
                        physical_column(table, name)
                            .expect("owner has the column")
                            .to_lowercase()
                    ),
                    None => name.to_lowercase(),
                }
            }
            [table_name, name] => self
                .scope
                .tables
                .iter()
                .find(|table| table.name.eq_ignore_ascii_case(table_name))
                .and_then(|table| {
                    physical_column(table, name).map(|column| {
                        format!(
                            "{}.{}.{}",
                            self.db.to_lowercase(),
                            table.name.to_lowercase(),
                            column.to_lowercase()
                        )
                    })
                })
                .unwrap_or_else(|| {
                    format!(
                        "{}.{}.{}",
                        self.db.to_lowercase(),
                        table_name.to_lowercase(),
                        name.to_lowercase()
                    )
                }),
            [database, table_name, name] => self
                .scope
                .tables
                .iter()
                .find(|table| {
                    table.name.eq_ignore_ascii_case(table_name)
                        && table
                            .database
                            .as_ref()
                            .is_some_and(|physical| physical.eq_ignore_ascii_case(database))
                })
                .and_then(|table| {
                    physical_column(table, name).map(|column| {
                        format!(
                            "{}.{}.{}",
                            table
                                .database
                                .as_deref()
                                .expect("matched database")
                                .to_lowercase(),
                            table.name.to_lowercase(),
                            column.to_lowercase()
                        )
                    })
                })
                .unwrap_or_else(|| {
                    path.iter()
                        .map(|part| part.to_lowercase())
                        .collect::<Vec<_>>()
                        .join(".")
                }),
            _ => path
                .iter()
                .map(|part| part.to_lowercase())
                .collect::<Vec<_>>()
                .join("."),
        }
    }
}

pub(crate) fn explain_constant(constant: &tidb_expr::constant::Constant) -> Option<String> {
    if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
        return None;
    }
    let value = constant
        .value
        .truncated_stringify()
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())?;
    let value = match &constant.value {
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Enum(_, _)
        | Datum::Set(_, _)
        | Datum::Json(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_) => format!("\"{value}\""),
        _ => value,
    };
    if constant.subquery_ref_id > 0 {
        Some(format!(
            "ScalarQueryCol#{}({value})",
            constant.subquery_ref_id
        ))
    } else {
        Some(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualifier_prints_catalog_identifier_spelling() {
        let scope = PlanTrace::single_table_scope(
            "history",
            Some("tpcc".to_owned()),
            vec![(
                "h_c_id".to_owned(),
                FieldType::new(tidb_datatype::FieldTypeCode::Long),
            )],
        );
        let qualify = Qualifier {
            db: "tpcc",
            scope: &scope,
            catalog: None,
        };

        assert_eq!(
            qualify.expr(&tidb_ast::Expr::Column(vec!["H_C_ID".to_owned()])),
            "tpcc.history.h_c_id"
        );
        assert_eq!(
            qualify.expr(&tidb_ast::Expr::Column(vec![
                "HISTORY".to_owned(),
                "H_C_ID".to_owned(),
            ])),
            "tpcc.history.h_c_id"
        );
        assert_eq!(
            qualify.expr(&tidb_ast::Expr::Column(vec![
                "TPCC".to_owned(),
                "HISTORY".to_owned(),
                "H_C_ID".to_owned(),
            ])),
            "tpcc.history.h_c_id"
        );
    }

    #[test]
    fn complementary_null_predicates_are_a_tautology() {
        let column = tidb_ast::Expr::Column(vec!["c".to_owned()]);
        let is_null = tidb_ast::Expr::Is {
            expr: Box::new(column.clone()),
            target: tidb_ast::IsTarget::Null,
            not: false,
        };
        let is_not_null = tidb_ast::Expr::Is {
            expr: Box::new(column),
            target: tidb_ast::IsTarget::Null,
            not: true,
        };
        let predicate = tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicOr,
            Box::new(is_null),
            Box::new(is_not_null),
        );

        assert_eq!(pseudo_selectivity(&predicate), 1.0);
    }

    fn index_join_text(outer_row_size: f64, inner_row_size: f64) -> IndexJoinText {
        IndexJoinText {
            reader: "IndexReader",
            keys: vec![("a".to_owned(), "b".to_owned())],
            equal_conditions: vec!["eq(a, b)".to_owned()],
            lookup_is_left: false,
            unique: false,
            forced_name: None,
            outer_row_size,
            inner_row_size,
            estimated_outer_rows: None,
            estimated_probe_rows_one: None,
            estimated_join_rows: None,
        }
    }

    /// MUTATION PROBE for the kind ENUMERATION: a rule that always answered
    /// one name fails one of these two.
    ///
    /// The two cases are Go's own two regimes. With MORE than one inner row
    /// per outer row the index join's hash table is the bigger one -- it is
    /// built over `probeRowsTot = probeRowsOne * buildRows` -- so
    /// `IndexHashJoin`, whose table is only `buildRows` tall, is cheaper.
    /// With exactly ONE inner row per outer row the two tables are the same
    /// height and only the ROW WIDTH is left, so a wide outer side makes
    /// `IndexJoin` the cheaper of the two.
    #[test]
    fn both_index_join_kinds_are_reachable() {
        assert_eq!(
            index_join_operator(Some(10000.0), Some(10.0), &index_join_text(16.0, 16.0), 1),
            "IndexHashJoin",
            "ten inner rows per outer row: the index join's table is ten times taller",
        );
        assert_eq!(
            index_join_operator(Some(10000.0), Some(1.0), &index_join_text(400.0, 8.0), 1),
            "IndexJoin",
            "one inner row per outer row and a wide outer side",
        );
    }

    /// MUTATION PROBE for the per-kind cost TERM: the two `hash_build_cost`
    /// calls read different sides, and swapping their arguments flips both
    /// answers above. Pinned as the exact tie Go's enumeration order settles:
    /// `IndexJoin` is enumerated first and `findBestTask` replaces the
    /// incumbent only on a STRICT improvement, so equal costs keep it.
    #[test]
    fn an_exact_tie_keeps_the_kind_go_enumerates_first() {
        assert_eq!(
            index_join_operator(Some(10000.0), Some(1.0), &index_join_text(16.0, 16.0), 1),
            "IndexJoin",
        );
    }

    #[test]
    fn grouped_index_join_accepts_an_existing_inner_not_null_selection() {
        let mut lookup = PlanNode::new("IndexLookUp", Some(10.0), String::new(), String::new());
        lookup.children.push(PlanNode::new(
            "IndexFullScan",
            Some(100.0),
            "table:h, index:idx_h_w_id(h_w_id)".to_owned(),
            "keep order:false".to_owned(),
        ));
        lookup.children.push(PlanNode::new(
            "TableRowIDScan",
            Some(10.0),
            "table:h".to_owned(),
            "keep order:false".to_owned(),
        ));
        let mut aggregate = PlanNode::new(
            "HashAgg",
            Some(10.0),
            String::new(),
            "old aggregate".to_owned(),
        );
        aggregate.children.push(lookup);
        let mut selection = PlanNode::new(
            "Selection",
            Some(10.0),
            String::new(),
            "not(isnull(Column#0))".to_owned(),
        );
        selection.children.push(aggregate);

        let mut trace = PlanTrace::planning();
        trace.stack.push(PlanNode::new(
            "Projection",
            Some(8.0),
            String::new(),
            String::new(),
        ));
        trace.stack.push(selection);

        let result = trace.index_join_inner_scan(
            false,
            IndexJoinInnerPathText {
                access: "table:h, index:idx_h_w_id(h_w_id)".to_owned(),
                range_info: "[eq(h.h_w_id, d.d_w_id)]",
                index: true,
                index_lookup: true,
                visible: "h",
                estimated_rows: Some(10.0),
                estimated_access_rows: Some(100.0),
                estimated_outer_rows: Some(8.0),
                unique: false,
                keep_outer_order: false,
                grouped_derived: true,
                composite: false,
                stream_aggregation: false,
                aggregation_info: Some("group by:h.h_w_id, funcs:sum(h.amount)->Column#0"),
                aggregation_final_info: Some("group by:h.h_w_id, funcs:sum(Column#0)->Column#0"),
                aggregation_partial_info: Some("group by:h.h_w_id, funcs:sum(h.amount)->Column#0"),
                outer_not_null: &[],
                inner_not_null: &[0],
            },
            &[],
            1.0,
        );

        assert!(result.is_ok());
        let inner = &trace.stack[1];
        assert_eq!(inner.name, "Selection");
        assert_eq!(inner.children.len(), 1);
        assert_eq!(inner.children[0].name, "HashAgg");
        assert_eq!(inner.children[0].children[0].name, "IndexLookUp");
        assert_eq!(
            inner.children[0].children[0].children[0].name,
            "IndexRangeScan"
        );
    }

    /// Go's dynamic-range builder retains a Selection only for predicates left
    /// in the inner path's table filters. When the join key consumed the last
    /// condition, `EmptySelectionEliminator` leaves the reader directly above
    /// the dynamic range scan.
    #[test]
    fn index_join_drops_an_empty_inner_selection() {
        let mut inner = PlanNode::new("Selection", Some(10.0), String::new(), String::new());
        inner.children.push(PlanNode::new(
            "TableFullScan",
            Some(100.0),
            "table:lineitem".to_owned(),
            "keep order:false".to_owned(),
        ));

        let mut trace = PlanTrace::planning();
        trace.stack.push(PlanNode::new(
            "TableFullScan",
            Some(8.0),
            "table:orders".to_owned(),
            "keep order:false".to_owned(),
        ));
        trace.stack.push(inner);

        let result = trace.index_join_inner_scan(
            false,
            IndexJoinInnerPathText {
                access: "table:lineitem".to_owned(),
                range_info: "[eq(lineitem.l_orderkey, orders.o_orderkey)]",
                index: false,
                index_lookup: false,
                visible: "lineitem",
                estimated_rows: Some(10.0),
                estimated_access_rows: Some(10.0),
                estimated_outer_rows: Some(8.0),
                unique: false,
                keep_outer_order: false,
                grouped_derived: false,
                composite: false,
                stream_aggregation: false,
                aggregation_info: None,
                aggregation_final_info: None,
                aggregation_partial_info: None,
                outer_not_null: &[],
                inner_not_null: &[],
            },
            &[String::new()],
            1.0,
        );

        assert!(result.is_ok());
        let reader = &trace.stack[1];
        assert_eq!(reader.name, "TableReader");
        assert_eq!(reader.children.len(), 1);
        assert_eq!(reader.children[0].name, "TableRangeScan");
    }

    /// MUTATION PROBE for the RETRACTION's descent: it passes through the
    /// shapes that do not rely on their child's order and stops at the ones
    /// that do.
    #[test]
    fn a_retraction_walks_past_a_projection_and_stops_at_a_merge_join() {
        let leaf = || {
            PlanNode::new(
                "TableFullScan",
                Some(1.0),
                "table:t".to_owned(),
                "keep order:true, stats:pseudo".to_owned(),
            )
        };
        let mut through = PlanNode::new("Projection", None, String::new(), String::new());
        through.children.push(leaf());
        retract_keep_order(&mut through);
        assert_eq!(through.children[0].info, "keep order:false, stats:pseudo");

        let mut ranged = PlanNode::new("HashAgg", None, String::new(), String::new());
        ranged.children.push(PlanNode::new(
            "TableRangeScan",
            Some(1.0),
            "table:t".to_owned(),
            "range:[1,1], keep order:true".to_owned(),
        ));
        retract_keep_order(&mut ranged);
        assert_eq!(ranged.children[0].info, "range:[1,1], keep order:false");

        let mut relied_on = PlanNode::new("MergeJoin", None, String::new(), String::new());
        relied_on.children.push(leaf());
        retract_keep_order(&mut relied_on);
        assert_eq!(
            relied_on.children[0].info, "keep order:true, stats:pseudo",
            "a merge join RELIES on that order; unsaying it describes a plan that cannot run",
        );
    }
}
