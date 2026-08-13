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
            key_ndv_ratio: None,
        }
    }

    /// The same node, marked as carrying Go's pseudo NDV ratio for every
    /// column (`distinctFactor`), which only a PSEUDO scan does.
    fn with_pseudo_ndv(mut self, estimate: ScanEstimate) -> Self {
        self.key_ndv_ratio = estimate.pseudo.then_some(DISTINCT_FACTOR);
        self
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
    pub(crate) fn partition_union(&mut self, partitions: &[String]) {
        // Only a SCAN fans out. A point get names its own partition from the
        // handle it already has (Go `PointGetPlan.AccessObject`) and is never
        // a union; a `TableDual` reads nothing to divide.
        if !self
            .stack
            .last()
            .is_some_and(|top| PARTITIONED_SCANS.contains(&top.name))
        {
            return;
        }
        // Nothing to fan out: an unpartitioned table, or a pruned set of one,
        // which Go also leaves as a bare `DataSource` rather than a union of
        // one branch.
        if partitions.len() < 2 {
            if let ([partition], Some(top)) = (partitions, self.stack.last_mut()) {
                top.access = with_partition(&top.access, partition);
            }
            return;
        }
        let Some(leaf) = self.stack.pop() else {
            return;
        };
        let mut union = PlanNode::new(
            "PartitionUnion",
            // Go's `PhysicalUnionAll` sums its children's estimates, and each
            // branch reads the same partition-blind estimate this tier costed
            // the one scan with.
            leaf.est_rows.map(|rows| rows * partitions.len() as f64),
            String::new(),
            String::new(),
        );
        union.key_ndv_ratio = leaf.key_ndv_ratio;
        // The row counter belongs to the ONE executor underneath, which no
        // fan-out split: attributing it to any single branch would report the
        // whole scan's rows as one partition's. It moves to the union, whose
        // count it really is.
        union.act_rows = leaf.act_rows.clone();
        for partition in partitions {
            let mut branch = PlanNode::new(
                leaf.name,
                leaf.est_rows,
                with_partition(&leaf.access, partition),
                leaf.info.clone(),
            );
            branch.key_ndv_ratio = leaf.key_ndv_ratio;
            union.children.push(branch);
        }
        self.stack.push(union);
    }

    /// Go's root `PhysicalUnionAll`: the completed operand subtrees are
    /// siblings, and the real execution emits their rows in term order.
    ///
    /// This is intentionally separate from `partition_union`: partition
    /// branches describe one scan split by storage partition, whereas a SQL
    /// `UNION ALL` has independently planned query operands.
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

    /// Go's `buildUnion` for a direct `UNION DISTINCT`: first a physical
    /// `Union` combines the operand pipelines, then `buildDistinct` places a
    /// root `HashAgg` above it.  Keeping both nodes here mirrors that plan
    /// shape instead of representing deduplication as a property of Union.
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
            Est::Inherit | Est::Scale(_) => child.as_ref().and_then(|c| c.key_ndv_ratio),
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
            Est::Inherit | Est::Scale(_) => child.key_ndv_ratio,
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
        rate: Option<f64>,
    ) {
        let info = qualify
            .expressions(built)
            .unwrap_or_else(|| qualify.expr(predicate));
        self.wrap_child(
            from_top,
            "Selection",
            Est::Scale(rate.unwrap_or_else(|| pseudo_selectivity(predicate))),
            info,
        );
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
        self.replace_top(Self::dual_node(0));
        self.consumed = true;
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
        self.consumed = true;
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
        handles: &[TableHandle],
        partitions: &[String],
        index: Option<&str>,
    ) {
        let printed: Vec<String> = handles.iter().map(handle_text).collect();
        let index = index.map_or_else(String::new, |index| format!(", {index}"));
        let info = if index.is_empty() {
            format!(
                "handle:[{}], keep order:false, desc:false",
                printed.join(" ")
            )
        } else {
            "keep order:false, desc:false".to_owned()
        };
        self.replace_top(PlanNode::new(
            "Batch_Point_Get",
            Some(handles.len() as f64),
            format!("table:{visible}{}{index}", partition_object(partitions)),
            info,
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
        // `consumed` stays false, unlike every narrowed path above: this one
        // consumed no condition, so a `Selection` on top of it still scales
        // the estimate the way Go's does over an `IndexFullScan`.
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
        self.consumed = true;
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
        self.consumed = true;
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
        access: String,
        range_info: &str,
        index: bool,
    ) -> Result<(), ()> {
        let depth = self.stack.len();
        if depth < 2 {
            return Err(());
        }
        let at = if lookup_is_left { depth - 2 } else { depth - 1 };
        let node = &mut self.stack[at];
        // The decision only ever names a bare base table, so its child
        // subtree is the single scan node `build_from` pushed for it. Anything
        // else means the two disagree, and renaming it would print a range
        // over an operator that reads none.
        if !matches!(
            node.name,
            "TableFullScan" | "IndexFullScan" | "TableRangeScan" | "IndexRangeScan"
        ) || !node.children.is_empty()
        {
            return Err(());
        }
        // `stats:pseudo` is a property of the TABLE, not of the path, so it
        // survives the rename -- and the replay compares it.
        let pseudo = if node.info.contains("stats:pseudo") {
            ", stats:pseudo"
        } else {
            ""
        };
        node.name = if index {
            "IndexRangeScan"
        } else {
            "TableRangeScan"
        };
        node.access = access;
        node.info = format!("range: decided by {range_info}, keep order:false{pseudo}");
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
        let est = if self.consumed {
            Est::Inherit
        } else {
            Est::Scale(stats_selectivity.unwrap_or_else(|| pseudo_selectivity(predicate)))
        };
        let info = built
            .filter(|expressions| !expressions.is_empty())
            .and_then(|expressions| qualify.expressions(expressions))
            .unwrap_or_else(|| qualify.expr(predicate));
        self.wrap("Selection", est, info);
    }

    /// Records a one-phase aggregation this tier builds for `GROUP BY` / an
    /// aggregate select field.
    fn aggregation(
        &mut self,
        name: &'static str,
        select: &tidb_ast::SelectStmt,
        qualify: &Qualifier<'_>,
    ) {
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
        self.wrap(name, est, info);
    }

    /// The one-phase hash aggregation this tier builds by default.
    pub(crate) fn hash_agg(&mut self, select: &tidb_ast::SelectStmt, qualify: &Qualifier<'_>) {
        self.aggregation("HashAgg", select, qualify);
    }

    /// The source-ordered stream aggregation selected by `STREAM_AGG()`.
    pub(crate) fn stream_agg(&mut self, select: &tidb_ast::SelectStmt, qualify: &Qualifier<'_>) {
        self.aggregation("StreamAgg", select, qualify);
    }

    /// The root sort Go's enforced `STREAM_AGG()` places below the stream
    /// aggregate when no access path already supplies the grouping order.
    pub(crate) fn stream_agg_sort(
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
        pushed: &[&tidb_ast::Expr],
        strategy: &JoinStrategy,
    ) -> Result<(), ()> {
        let JoinStrategy {
            equal_mask,
            build_is_left,
            merge_keys,
            index_lookup,
        } = strategy;
        let build_is_left = *build_is_left;
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
        for (conjunct, is_equal) in conjuncts.iter().zip(equal_mask.iter()) {
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
            // comma-separated -- not pairwise. The residual `ON` conjuncts
            // this tier still evaluates are NOT printed as `other cond:`,
            // because Go moved the USED equal conditions out of that list and
            // this tier keeps them (see `crate::join`); printing them would
            // report conditions Go's plan does not carry.
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
            // key lists, then the equal conditions spelled in full. The
            // residual `ON` conjuncts this tier still evaluates are not
            // printed, for the same reason the merge arm does not print them.
            tail.push_str(", inner:");
            tail.push_str(text.reader);
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
            if !equal.is_empty() {
                tail.push_str(", equal cond:");
                tail.push_str(&equal.join(", "));
            }
        } else {
            if !equal.is_empty() {
                tail.push_str(", equal:[");
                tail.push_str(&equal.join(" "));
                tail.push(']');
            }
            if !other.is_empty() {
                tail.push_str(", other cond:");
                tail.push_str(&other.join(", "));
            }
        }
        let (Some(mut right), Some(mut left)) = (self.stack.pop(), self.stack.pop()) else {
            return Err(());
        };
        // Go prints the BUILD child first and labels both sides
        // (`flat_plan.go`'s `BuildSide`/`ProbeSide`).
        left.label = if build_is_left { "(Build)" } else { "(Probe)" };
        right.label = if build_is_left { "(Probe)" } else { "(Build)" };
        let est_rows = full_join_row_count(&left, &right, equal.len());
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
            index_join_operator(build_rows, probe_rows_one, text, equal.len())
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
            // `explainJoinLeftSide` names the LEFT child's operator, and only
            // for an OUTER join. The name carries its own id in `format='row'`,
            // which is not assigned yet, so the renderer splices it in.
            left_side_child: (join.tp != tidb_ast::JoinType::Cross)
                .then_some(usize::from(!build_is_left)),
            info_tail: tail,
            label: "",
            children,
            act_rows: None,
            // Go `LogicalJoin.DeriveStats` builds a fresh `ColNDVs` from both
            // children's maps rather than scaling either one, so the single
            // ratio this trace carries does not survive a join.
            key_ndv_ratio: None,
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
        self.consumed |= !pushed.is_empty();
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

    /// The same reason, for the same reason: Go's `aggExecutorTreeInputEmpty`
    /// walks THROUGH a single-child operator, and a meter is exactly that.
    fn agg_tree_input_empty(&self) -> bool {
        self.child.agg_tree_input_empty()
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
/// The leaf operators a partitioned read can be built out of -- the ones
/// `PlanTrace::partition_union` may fan out. Every other leaf either names
/// its partition itself (the point gets) or reads no partition at all.
const PARTITIONED_SCANS: &[&str] = &[
    "TableFullScan",
    "TableRangeScan",
    "IndexFullScan",
    "IndexRangeScan",
];

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
    let mut factor = SELECTIVITY_FACTOR;
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
fn full_join_row_count(left: &PlanNode, right: &PlanNode, equal_count: usize) -> Option<f64> {
    use tidb_planner::cardinality::join::{
        estimate_full_join_row_count, FullJoinRowCountInput, JoinKeyEstimate,
    };
    let (left_rows, right_rows) = (left.est_rows?, right.est_rows?);
    let key = |node: &PlanNode| {
        node.key_ndv_ratio
            .zip(node.est_rows)
            .map(|(ratio, rows)| JoinKeyEstimate::new(rows * ratio, 1, equal_count))
    };
    let (left_keys, right_keys) = if equal_count == 0 {
        (JoinKeyEstimate::empty(), JoinKeyEstimate::empty())
    } else {
        (key(left)?, key(right)?)
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
    /// A merge join's `(left key, right key)` column names, already
    /// qualified; `None` for every other strategy.
    pub(crate) merge_keys: Option<Vec<(String, String)>>,
    /// An index join's printed shape: the reader Go names after `inner:`,
    /// and the `(outer key, inner key)` column names. `None` for every other
    /// strategy.
    pub(crate) index_lookup: Option<IndexJoinText>,
}

/// What an index join's plan row says beyond its join kind.
///
/// Go prints `inner:<reader>, outer key:<cols>, inner key:<cols>, equal
/// cond:<conjuncts>` (`PhysicalIndexJoin.explainInfo`). The reader is the
/// operator ABOVE the range scan in Go's tree, which this tier collapses into
/// the scan itself -- so it is named from the probed object rather than read
/// off a child that does not exist here.
pub(crate) struct IndexJoinText {
    /// `IndexReader` for an index probe, `TableReader` for a handle probe.
    pub(crate) reader: &'static str,
    /// The outer and inner key column names, already qualified, in probe
    /// order.
    pub(crate) keys: Vec<(String, String)>,
    /// Whether the LOOKED-UP (inner) side is the join's left child. The two
    /// sides are not interchangeable in the cost below: the hash table is
    /// built over the outer side for `IndexHashJoin` and over the inner one
    /// for `IndexJoin`.
    pub(crate) lookup_is_left: bool,
    /// `getAvgRowSize(build.StatsInfo(), build.Schema().Columns)` for the
    /// OUTER side, and the same for the INNER one. Computed where the two
    /// sides' column types are known (`driver::from::build_join`), because a
    /// plan row carries only their text.
    pub(crate) outer_row_size: f64,
    pub(crate) inner_row_size: f64,
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
        if let Some(rest) = node.info.strip_prefix("keep order:true") {
            node.info = format!("keep order:false{rest}");
        }
        return;
    }
    if !matches!(node.name, "Projection" | "Selection" | "HashJoin") {
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
    use tidb_planner::plan_cost_ver2::{hash_build_cost, Ver2Factors};
    use tidb_planner::task_type::TaskType;
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
    fn expressions(&self, expressions: &[Expression]) -> Option<String> {
        let mut rendered: Vec<String> = expressions
            .iter()
            .map(|expression| self.built_expr(expression))
            .collect::<Option<_>>()?;
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
                Some(format!("{database}.{}.{}", table.name, name))
            })
            .or_else(|| (!column.orig_name.is_empty()).then(|| column.orig_name.clone()))
    }

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

fn explain_constant(constant: &tidb_expr::constant::Constant) -> Option<String> {
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

    fn index_join_text(outer_row_size: f64, inner_row_size: f64) -> IndexJoinText {
        IndexJoinText {
            reader: "IndexReader",
            keys: vec![("a".to_owned(), "b".to_owned())],
            lookup_is_left: false,
            outer_row_size,
            inner_row_size,
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

        let mut relied_on = PlanNode::new("MergeJoin", None, String::new(), String::new());
        relied_on.children.push(leaf());
        retract_keep_order(&mut relied_on);
        assert_eq!(
            relied_on.children[0].info, "keep order:true, stats:pseudo",
            "a merge join RELIES on that order; unsaying it describes a plan that cannot run",
        );
    }
}
