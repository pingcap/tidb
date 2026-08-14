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

//! `GetPlanCostVer2` walked over a WHOLE candidate plan: the recursion
//! `find_best_task` performs before `compareTaskCost` picks between two
//! physical alternatives.
//!
//! # Why this exists beside [`crate::plan_cost_ver2`]
//!
//! [`crate::plan_cost_ver2`] owns every per-operator FORMULA, expressed over
//! explicit source-shaped inputs because this workspace has no physical-plan
//! IR. That is enough to price one operator whose child costs are already
//! known, and it is what `r/planner/core/plan_cost_ver2.result` validates.
//! It is not enough to answer "is the hash-join candidate or the index-join
//! candidate cheaper", because that question is about two TREES: Go calls
//! `GetPlanCostVer2` on the root of each and the recursion visits every node
//! below it, with the reader operators switching their children from the
//! root task's factors to the coprocessor task's on the way down.
//!
//! This module is that recursion, and only that recursion. It introduces no
//! arithmetic of its own: every leaf, every operator and every factor lookup
//! is a call into [`crate::plan_cost_ver2`], and every row size resolves
//! through [`crate::cardinality::row_size`]'s `plan_avg_row_size`. A
//! divergence found here is a divergence in one of those, not here.
//!
//! # What a caller supplies, and what it does NOT
//!
//! Go reads a node's row count off `p.StatsInfo().RowCount` and its row size
//! off `p.Schema().Columns`; both were derived by the cardinality code long
//! before costing runs. The same split holds here: [`Candidate`] carries the
//! rows and the schema of each node, and this module reads them. Deriving
//! them stays with [`crate::cardinality`], as it is in Go.
//!
//! One consequence is load-bearing and easy to get wrong, so it is written
//! down: the row count an index join's INNER subtree is costed with is the
//! count for ONE outer row, not the total `EXPLAIN` prints. For
//! `join_reorder_through_projection`'s `result:1042` the recorded
//! `IndexReader_58` prints `estRows 10000.00`, and the cost trace of the same
//! node reads
//!
//! ```text
//! (net(1.25*rowsize(32)*tidb_kv_net_factor(3.96)))
//! ```
//!
//! -- `1.25`, which is `10000 / 8000` outer rows. `getCardinality(probe)` in
//! `getIndexJoinCostVer24PhysicalIndexJoin` returns that same `1.25` and
//! multiplies it back up by `buildRows` for `probeRowsTot`. A caller that
//! feeds the printed number instead prices the inner side 8000 times over.

use crate::cardinality::row_size::RowSizeColumn;
use crate::cost_usage::{CostVer2, PlanCostOption};
use crate::physical_table_reader::StoreType;
use crate::plan_cost_ver2::{
    self as ver2, CostFactorVars, CostSessionOpts, HashJoinInput, IndexJoinInput, IndexLookUpInput,
    NetOwner, TableScanInput, TableScanPenaltyInput, Ver2Factors,
};
use crate::task_type::TaskType;

/// The session and factor state one costing run reads, Go's
/// `SCtx().GetSessionVars()` reduced to what cost model ver2 asks it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CostEnv {
    /// `costVer2Factors`.
    pub factors: Ver2Factors,
    /// The per-operator `tidb_opt_*_cost_factor` multipliers.
    pub cost_factors: CostFactorVars,
    /// The concurrency and size variables the formulas divide by.
    pub session: CostSessionOpts,
}

/// A node's row size: either already resolved, or the schema `getAvgRowSize`
/// resolves it from.
///
/// Both spellings exist because Go has both: a scan reads `p.TblCols` through
/// the histogram collection, while an operator whose output is an expression
/// has no collection at all and falls back to the static type widths. See
/// [`ver2::plan_avg_row_size`], which is what [`RowSize::resolve`] calls.
#[derive(Clone, Debug, PartialEq)]
pub enum RowSize {
    /// A width the caller already computed.
    Fixed(f64),
    /// `getAvgRowSize(stats, columns)`: `hist_coll` is
    /// `Some((pseudo, realtime_count))` when the node's `StatsInfo().HistColl`
    /// exists.
    Schema {
        /// The columns whose widths are averaged.
        columns: Vec<RowSizeColumn>,
        /// The histogram collection, when the node has one.
        hist_coll: Option<(bool, i64)>,
    },
}

impl RowSize {
    /// The width in bytes.
    #[must_use]
    pub fn resolve(&self) -> f64 {
        match self {
            Self::Fixed(size) => *size,
            Self::Schema { columns, hist_coll } => ver2::plan_avg_row_size(columns, *hist_coll),
        }
    }
}

/// Which reader is being costed, which decides the net factor, the row-size
/// clamp and the cost factor -- Go's three separate `getPlanCostVer24*Reader`
/// functions over one shared formula.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderKind {
    /// `PhysicalIndexReader`.
    Index,
    /// `PhysicalTableReader` reading TiKV.
    Table,
}

/// One physical plan the chooser is asked to price.
///
/// The shapes are the ones a join-strategy decision compares over; an
/// operator this tier's driver cannot build is deliberately absent rather
/// than approximated.
#[derive(Clone, Debug, PartialEq)]
pub enum Candidate {
    /// An access-path subtree already priced by its owning scan/reader model.
    ///
    /// The join search needs the subtree's total, cardinality, width, and
    /// range count. Keeping those exact values avoids duplicating the access
    /// planner's table/index/lookup branching in this crate.
    CostedAccess {
        /// `getCardinality(reader)`.
        rows: f64,
        /// `getAvgRowSize(reader.StatsInfo(), reader.Schema().Columns)`.
        row_size: f64,
        /// `GetPlanCostVer2(reader)`.
        cost: f64,
        /// `getNumberOfRanges(reader)`.
        num_ranges: usize,
    },
    /// `PhysicalTableScan` on TiKV.
    TableScan {
        /// `getCardinality(p)`.
        rows: f64,
        /// `getAvgRowSize(p.StatsInfo(), p.TblCols)`.
        row_size: RowSize,
        /// Go's variadic `isChildOfINL`.
        is_child_of_inl: Option<bool>,
        /// `ranger.HasFullRange(p.Ranges, unsignedIntHandle)`.
        has_full_range_scan: bool,
        /// The `getTableScanPenalty` inputs.
        penalty: TableScanPenaltyInput,
        /// `len(p.Ranges)`, which `getNumberOfRanges` sums.
        num_ranges: usize,
        /// `p.Desc`, which picks the descending scan factor.
        desc: bool,
    },
    /// `PhysicalIndexScan`.
    IndexScan {
        /// `getCardinality(p)`.
        rows: f64,
        /// `getAvgRowSize(p.StatsInfo(), p.Schema().Columns)`.
        row_size: RowSize,
        /// `p.Index.ID`, the source's untraced tie-breaker; `None` is
        /// `p.Index == nil`.
        index_id: Option<i64>,
        /// `len(p.Ranges)`.
        num_ranges: usize,
        /// `p.Desc`.
        desc: bool,
    },
    /// `PhysicalSelection`.
    Selection {
        /// The single child.
        child: Box<Candidate>,
        /// `getCardinality(p.Children()[0])` -- the filter runs on the INPUT.
        input_rows: f64,
        /// One flag per condition: whether it is a scalar function.
        conditions: Vec<bool>,
    },
    /// `PhysicalProjection`, always a root-task operator here.
    Projection {
        /// The single child.
        child: Box<Candidate>,
        /// `getCardinality(p.Children()[0])`.
        input_rows: f64,
        /// One flag per output expression: whether it is a scalar function.
        exprs: Vec<bool>,
    },
    /// `PhysicalIndexReader` / `PhysicalTableReader`: the root-task boundary,
    /// below which the child is costed with the coprocessor's factors.
    Reader {
        /// The coprocessor plan.
        child: Box<Candidate>,
        /// `getCardinality(p)`.
        rows: f64,
        /// `getAvgRowSize(p.StatsInfo(), p.Schema().Columns)`.
        row_size: RowSize,
        /// Which reader this is.
        kind: ReaderKind,
    },
    /// `PhysicalIndexLookUpReader`: an index scan followed by a row-handle
    /// table scan. Both children run in TiKV; the lookup reader itself runs
    /// at the root and charges the batched double read.
    IndexLookup {
        /// `p.IndexPlan`.
        index: Box<Candidate>,
        /// `p.TablePlan`.
        table: Box<Candidate>,
        /// The reader cardinalities, transfer widths, and pushed-limit state.
        input: IndexLookUpInput,
    },
    /// `PhysicalHashJoin` on a root task.
    HashJoin {
        /// The BUILD child, already resolved through `InnerChildIdx` /
        /// `UseOuterToBuild` as [`HashJoinInput`] documents.
        build: Box<Candidate>,
        /// The PROBE child.
        probe: Box<Candidate>,
        /// The rows, sizes, key counts and concurrency the formula reads.
        input: HashJoinInput,
        /// `p.LeftConditions` resolved to the build side.
        build_filters: Vec<bool>,
        /// `p.RightConditions` resolved to the probe side.
        probe_filters: Vec<bool>,
    },
    /// `PhysicalIndexJoin` and its `IndexHashJoin` / `IndexMergeJoin`
    /// variants, on a root task.
    IndexJoin {
        /// The OUTER child, which Go builds from and reads once.
        build: Box<Candidate>,
        /// The INNER child, costed for ONE outer row -- see the module doc.
        probe: Box<Candidate>,
        /// Everything `getIndexJoinCostVer24PhysicalIndexJoin` reads except
        /// `num_ranges`, which [`number_of_ranges`] takes off `probe`.
        input: IndexJoinInput,
        /// `p.LeftConditions`.
        build_filters: Vec<bool>,
        /// `p.RightConditions`.
        probe_filters: Vec<bool>,
    },
    /// `PhysicalSort`: the ENFORCER `EnforceProperty` puts on a task that
    /// cannot satisfy a required order by itself.
    ///
    /// It exists here for one reason: `findBestTask`'s enforced branch costs
    /// the sorted alternative, and a comparison that omits the `Sort` prices a
    /// plan nobody would run. See [`crate::find_best_task`].
    Sort {
        /// The single child.
        child: Box<Candidate>,
        /// `getCardinality(p)`, which for a sort is its child's count.
        rows: f64,
        /// `getAvgRowSize(p.StatsInfo(), p.Schema().Columns)`.
        row_size: RowSize,
        /// One flag per `ByItems` entry: whether it is a scalar function.
        by_items: Vec<bool>,
    },
    /// `PhysicalMergeJoin` on a root task.
    MergeJoin {
        /// The left child.
        left: Box<Candidate>,
        /// The right child.
        right: Box<Candidate>,
        /// `(getCardinality(left), getCardinality(right))`.
        child_rows: (f64, f64),
        /// `p.LeftConditions`.
        left_conditions: Vec<bool>,
        /// `p.RightConditions`.
        right_conditions: Vec<bool>,
        /// `p.OtherConditions`, which run on both sides' rows.
        other_conditions: Vec<bool>,
        /// `(len(p.LeftJoinKeys), len(p.RightJoinKeys))`.
        num_join_keys: (usize, usize),
    },
}

/// A node's cost together with its children's, so a caller can read the
/// per-node table `EXPLAIN FORMAT='verbose'` prints.
///
/// Shaped like [`crate::cardinality::derive_stats::DerivedNode`] for the same
/// reason: a whole-tree answer is only checkable node by node.
#[derive(Clone, Debug, PartialEq)]
pub struct CostedNode {
    /// `getCardinality(p)`.
    pub rows: f64,
    /// `getAvgRowSize` of this node's own schema.
    pub row_size: f64,
    /// This node's `PlanCostVer2`, which already includes its children's.
    pub cost: CostVer2,
    /// Children in the order the variant declares them.
    pub children: Vec<CostedNode>,
}

impl CostedNode {
    /// The number `EXPLAIN FORMAT='verbose'` prints in the `estCost` column.
    #[must_use]
    pub fn est_cost(&self) -> f64 {
        self.cost.value()
    }

    /// Every node's cost, parent before children, depth first.
    #[must_use]
    pub fn est_costs(&self) -> Vec<f64> {
        let mut out = vec![self.est_cost()];
        for child in &self.children {
            out.extend(child.est_costs());
        }
        out
    }
}

/// Go `getNumberOfRanges`: the ranges of every scan in a subtree, summed. A
/// reader forwards to its coprocessor plan; every other operator sums its
/// children.
#[must_use]
pub fn number_of_ranges(node: &Candidate) -> usize {
    match node {
        Candidate::CostedAccess { num_ranges, .. } => *num_ranges,
        Candidate::TableScan { num_ranges, .. } | Candidate::IndexScan { num_ranges, .. } => {
            *num_ranges
        }
        Candidate::Reader { child, .. }
        | Candidate::Selection { child, .. }
        | Candidate::Projection { child, .. }
        | Candidate::Sort { child, .. } => number_of_ranges(child),
        Candidate::IndexLookup { index, table, .. } => {
            number_of_ranges(index) + number_of_ranges(table)
        }
        Candidate::HashJoin { build, probe, .. } | Candidate::IndexJoin { build, probe, .. } => {
            number_of_ranges(build) + number_of_ranges(probe)
        }
        Candidate::MergeJoin { left, right, .. } => {
            number_of_ranges(left) + number_of_ranges(right)
        }
    }
}

/// `GetPlanCostVer2` on the root of a candidate plan: children first, then the
/// node's own operator cost over them.
///
/// `task` is the task the ROOT sits on, which for every candidate a join
/// chooser compares is [`TaskType::Root`]; a [`Candidate::Reader`] switches
/// its child to the coprocessor task itself, exactly as Go's readers do.
#[must_use]
pub fn evaluate(node: &Candidate, env: &CostEnv, task: TaskType) -> CostedNode {
    evaluate_traced(node, env, task, None)
}

/// [`evaluate`] with Go's `PlanCostOption`, which records the formula text
/// `EXPLAIN FORMAT='cost_trace'` prints.
#[must_use]
pub fn evaluate_traced(
    node: &Candidate,
    env: &CostEnv,
    task: TaskType,
    option: Option<&PlanCostOption>,
) -> CostedNode {
    match node {
        Candidate::CostedAccess {
            rows,
            row_size,
            cost,
            ..
        } => leaf(*rows, *row_size, crate::cost_usage::fixed_cost_ver2(*cost)),
        Candidate::TableScan {
            rows,
            row_size,
            is_child_of_inl,
            has_full_range_scan,
            penalty,
            desc,
            ..
        } => {
            let row_size = row_size.resolve();
            let scan_factor = env.factors.task_scan(false, StoreType::TiKv, task, *desc);
            let cost = ver2::table_scan_cost(
                option,
                TableScanInput {
                    rows: *rows,
                    row_size,
                    is_child_of_inl: *is_child_of_inl,
                    has_full_range_scan: *has_full_range_scan,
                    penalty: *penalty,
                },
                scan_factor,
                &env.cost_factors,
            );
            leaf(*rows, row_size, cost)
        }
        Candidate::IndexScan {
            rows,
            row_size,
            index_id,
            desc,
            ..
        } => {
            let row_size = row_size.resolve();
            let scan_factor = env.factors.task_scan(false, StoreType::TiKv, task, *desc);
            let cost = ver2::index_scan_cost(
                option,
                *rows,
                row_size,
                scan_factor,
                env.cost_factors.index_scan,
                *index_id,
            );
            leaf(*rows, row_size, cost)
        }
        Candidate::Selection {
            child,
            input_rows,
            conditions,
        } => {
            let child = evaluate_traced(child, env, task, option);
            let cost = ver2::selection_cost(
                option,
                *input_rows,
                conditions,
                env.factors.task_cpu(task),
                &child.cost,
            );
            // A selection keeps its child's row layout; only the count moves,
            // and the count is the caller's to declare on the parent.
            let row_size = child.row_size;
            CostedNode {
                rows: *input_rows,
                row_size,
                cost,
                children: vec![child],
            }
        }
        Candidate::Projection {
            child,
            input_rows,
            exprs,
        } => {
            let child = evaluate_traced(child, env, task, option);
            let cost = ver2::projection_cost(
                option,
                *input_rows,
                exprs,
                env.factors.task_cpu(task),
                env.session.projection_concurrency,
                &child.cost,
            );
            let row_size = child.row_size;
            CostedNode {
                rows: *input_rows,
                row_size,
                cost,
                children: vec![child],
            }
        }
        Candidate::Reader {
            child,
            rows,
            row_size,
            kind,
        } => {
            // Below a reader the plan runs on the coprocessor, which is what
            // switches the cpu/scan factors from TiDB's to TiKV's.
            let child = evaluate_traced(child, env, TaskType::CopSingleRead, option);
            let row_size = row_size.resolve();
            let (row_size, cost_factor) = match kind {
                ReaderKind::Index => (row_size, env.cost_factors.index_reader),
                ReaderKind::Table => (
                    row_size.max(ver2::MIN_ROW_SIZE),
                    env.cost_factors.table_reader,
                ),
            };
            let cost = ver2::reader_cost(
                option,
                *rows,
                row_size,
                env.factors.task_net(false, NetOwner::TiDbToTiKv),
                env.session.distsql_scan_concurrency,
                &child.cost,
                cost_factor,
            );
            CostedNode {
                rows: *rows,
                row_size,
                cost,
                children: vec![child],
            }
        }
        Candidate::IndexLookup {
            index,
            table,
            input,
        } => {
            let index = evaluate_traced(index, env, TaskType::CopSingleRead, option);
            let table = evaluate_traced(table, env, TaskType::CopSingleRead, option);
            let cost = ver2::index_lookup_reader_cost(
                option,
                *input,
                (&index.cost, &table.cost),
                (&env.factors, &env.cost_factors),
                &env.session,
                task,
            );
            CostedNode {
                rows: input.table_rows,
                row_size: table.row_size,
                cost,
                children: vec![index, table],
            }
        }
        Candidate::HashJoin {
            build,
            probe,
            input,
            build_filters,
            probe_filters,
        } => {
            let build = evaluate_traced(build, env, task, option);
            let probe = evaluate_traced(probe, env, task, option);
            let cost = ver2::hash_join_cost(
                option,
                *input,
                (build_filters, probe_filters),
                (
                    env.factors.task_cpu(task),
                    env.factors.task_mem(task),
                    env.cost_factors.hash_join,
                ),
                task,
                (&build.cost, &probe.cost),
            );
            CostedNode {
                rows: input.build_rows.max(input.probe_rows),
                row_size: build.row_size + probe.row_size,
                cost,
                children: vec![build, probe],
            }
        }
        Candidate::IndexJoin {
            build,
            probe,
            input,
            build_filters,
            probe_filters,
        } => {
            let build_costed = evaluate_traced(build, env, task, option);
            let probe_costed = evaluate_traced(probe, env, task, option);
            // `getNumberOfRanges(probe)` is read off the plan, not declared,
            // so a caller cannot disagree with the tree it handed in.
            let input = IndexJoinInput {
                num_ranges: number_of_ranges(probe) as f64,
                ..*input
            };
            let cost = ver2::index_join_cost(
                option,
                input,
                (build_filters, probe_filters),
                (&env.factors, &env.cost_factors),
                &env.session,
                task,
                (&build_costed.cost, &probe_costed.cost),
            );
            CostedNode {
                rows: input.build_rows * input.probe_rows_one,
                row_size: build_costed.row_size + probe_costed.row_size,
                cost,
                children: vec![build_costed, probe_costed],
            }
        }
        Candidate::Sort {
            child,
            rows,
            row_size,
            by_items,
        } => {
            let child = evaluate_traced(child, env, task, option);
            let row_size = row_size.resolve();
            let cost = ver2::sort_cost(
                option,
                (*rows, row_size),
                by_items,
                (&env.factors, &env.cost_factors),
                &env.session,
                task,
                &child.cost,
            );
            CostedNode {
                rows: *rows,
                row_size,
                cost,
                children: vec![child],
            }
        }
        Candidate::MergeJoin {
            left,
            right,
            child_rows,
            left_conditions,
            right_conditions,
            other_conditions,
            num_join_keys,
        } => {
            let left = evaluate_traced(left, env, task, option);
            let right = evaluate_traced(right, env, task, option);
            let cost = ver2::merge_join_cost(
                option,
                *child_rows,
                (left_conditions, right_conditions, other_conditions),
                *num_join_keys,
                (env.factors.task_cpu(task), env.cost_factors.merge_join),
                (&left.cost, &right.cost),
            );
            CostedNode {
                rows: child_rows.0.max(child_rows.1),
                row_size: left.row_size + right.row_size,
                cost,
                children: vec![left, right],
            }
        }
    }
}

fn leaf(rows: f64, row_size: f64, cost: CostVer2) -> CostedNode {
    CostedNode {
        rows,
        row_size,
        cost,
        children: Vec::new(),
    }
}

/// `compareTaskCost` over two candidate PLANS: whether `current` should
/// replace `best`.
///
/// The tie direction is [`ver2::compare_task_cost`]'s strict `<`, so an
/// exactly equal alternative never displaces the incumbent -- which is what
/// makes the enumeration order, not the cost, decide a tie.
#[must_use]
pub fn prefer(current: &CostedNode, best: &CostedNode) -> bool {
    ver2::compare_task_cost(
        ver2::TaskPlanCost::valid(current.est_cost()),
        ver2::TaskPlanCost::valid(best.est_cost()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_cost_ver2::IndexJoinKind;

    /// The session the recordings were made in: mysql-tester puts
    /// `tidb_hash_join_concurrency = 1` in every connection's DSN, and a hash
    /// join is charged what five workers would have shared.
    const RECORDED_HASH_JOIN_CONCURRENCY: f64 = 1.0;

    fn env() -> CostEnv {
        CostEnv::default()
    }

    #[test]
    fn a_costed_access_path_preserves_its_owner_computed_total() {
        let path = Candidate::CostedAccess {
            rows: 12.5,
            row_size: 48.0,
            cost: 345.75,
            num_ranges: 3,
        };
        let costed = evaluate(&path, &env(), TaskType::Root);
        assert_eq!(costed.rows, 12.5);
        assert_eq!(costed.row_size, 48.0);
        assert_eq!(costed.est_cost(), 345.75);
        assert_eq!(number_of_ranges(&path), 3);
    }

    /// `EXPLAIN` prints two decimals, so agreement is asserted at the printed
    /// digit -- the same resolution the oracle was read at.
    #[track_caller]
    fn assert_prints(node: &CostedNode, expected: &str) {
        assert_eq!(format!("{:.2}", node.est_cost()), expected);
    }

    /// The pseudo-statistics full scan of a `(a int, b int, c varchar(32))`
    /// table: `scan(10000*logrowsize(72)*tikv_scan_factor(40.7))` twice, the
    /// second being `getTableScanPenalty`'s 10000 penalty rows.
    fn pseudo_full_scan(rows: f64) -> Candidate {
        Candidate::TableScan {
            rows,
            row_size: RowSize::Fixed(72.0),
            is_child_of_inl: None,
            has_full_range_scan: true,
            penalty: TableScanPenaltyInput {
                has_range_info: false,
                allow_prefer_range_scan: true,
                pseudo_stats: true,
                analyze_row_count: 0,
                modify_count: 0,
                has_partition_scan: false,
                has_index_force: false,
            },
            num_ranges: 1,
            desc: false,
        }
    }

    /// `Projection(TableReader(Selection(TableFullScan t2)))` -- the build
    /// side of `result:1042`'s index join, and of its hash-join alternative
    /// once the scan is swapped.
    fn projected_t2(scan: Candidate, reader: ReaderKind) -> Candidate {
        Candidate::Projection {
            child: Box::new(Candidate::Reader {
                child: Box::new(Candidate::Selection {
                    child: Box::new(scan),
                    input_rows: 10000.0,
                    conditions: vec![true],
                }),
                rows: 8000.0,
                row_size: RowSize::Fixed(32.0),
                kind: reader,
            }),
            input_rows: 8000.0,
            // `t2.a`, `t2.b`, `plus(mul(t2.b, 2), 10)`: `filters(1.02)`.
            exprs: vec![false, false, true],
        }
    }

    /// `join_reorder_through_projection` `result:1042`, the plan
    /// `tidb_opt_join_reorder_through_proj = on` records and the one this
    /// tier's reordered tree now builds.
    ///
    /// Every number is read off `EXPLAIN FORMAT='cost_trace'` of a
    /// `tidb-server` built from this tree, in a session carrying
    /// mysql-tester's DSN variables.
    #[test]
    fn the_recorded_index_hash_join_of_result_1042_costs_what_go_prints() {
        // └─IndexReader_58(Probe) 10000.00 31.70 root index:Selection_57
        //   └─Selection_57 10000.00 317.07 cop[tikv]
        //     └─IndexRangeScan_56 10010.01 254.63 cop[tikv]
        //
        // The printed `estRows` are TOTALS; the costs are per outer row --
        // `1.25 = 10000/8000` and `1.2512512512512513 = 10010.01/8000`.
        let inner = Candidate::Reader {
            child: Box::new(Candidate::Selection {
                child: Box::new(Candidate::IndexScan {
                    rows: 1.2512512512512513,
                    row_size: RowSize::Fixed(32.0),
                    index_id: None,
                    num_ranges: 1,
                    desc: false,
                }),
                input_rows: 1.2512512512512513,
                conditions: vec![true],
            }),
            rows: 1.25,
            row_size: RowSize::Fixed(32.0),
            kind: ReaderKind::Index,
        };
        let outer = projected_t2(pseudo_full_scan(10000.0), ReaderKind::Table);

        let join = Candidate::IndexJoin {
            build: Box::new(outer),
            probe: Box::new(inner),
            input: IndexJoinInput {
                build_rows: 8000.0,
                // `hashmem(8000*24*tidb_mem_factor(0.2))`: three 8-byte
                // columns, and the projection has no histogram collection.
                build_row_size: 24.0,
                probe_rows_one: 1.25,
                probe_row_size: 32.0,
                // `hashkey(8000*0*tidb_cpu_factor(49.9))`: this construction
                // leaves `p.RightJoinKeys` EMPTY, and the trace shows it.
                num_right_join_keys: 0,
                num_left_join_keys: 0,
                num_ranges: 0.0,
                is_semi_join: false,
                kind: IndexJoinKind::IndexHashJoin,
            },
            build_filters: Vec::new(),
            probe_filters: Vec::new(),
        };
        let costed = evaluate(&join, &env(), TaskType::Root);

        // └─IndexHashJoin_51(Probe) 10000.00 4606578.48
        assert_prints(&costed, "4606578.48");
        // ├─Projection_52(Build) 8000.00 517108.73
        assert_prints(&costed.children[0], "517108.73");
        // │ └─TableReader_55 8000.00 435671.93
        assert_prints(&costed.children[0].children[0], "435671.93");
        // │   └─Selection_54 8000.00 5521318.95
        assert_prints(&costed.children[0].children[0].children[0], "5521318.95");
        // │     └─TableFullScan_53 10000.00 5022318.95
        assert_prints(
            &costed.children[0].children[0].children[0].children[0],
            "5022318.95",
        );
        // └─IndexReader_58(Probe) 10000.00 31.70
        assert_prints(&costed.children[1], "31.70");
        // └─Selection_57 10000.00 317.07
        assert_prints(&costed.children[1].children[0], "317.07");
        // └─IndexRangeScan_56 10010.01 254.63
        assert_prints(&costed.children[1].children[0].children[0], "254.63");
    }

    /// The non-covering secondary-index probe printed by the accepted Go
    /// wide-row case. Its cost is per outer row even though `EXPLAIN` prints
    /// the totals after multiplying by 8000 outer rows.
    #[test]
    fn a_secondary_index_join_probe_costs_the_recorded_double_read() {
        let index = Candidate::Selection {
            child: Box::new(Candidate::IndexScan {
                rows: 1.2512512512512513,
                row_size: RowSize::Fixed(32.0),
                index_id: Some(2),
                num_ranges: 1,
                desc: false,
            }),
            input_rows: 1.2512512512512513,
            conditions: vec![true],
        };
        let table = Candidate::TableScan {
            rows: 1.25,
            row_size: RowSize::Fixed(72.0),
            is_child_of_inl: Some(true),
            has_full_range_scan: false,
            penalty: TableScanPenaltyInput::default(),
            num_ranges: 0,
            desc: false,
        };
        let lookup = Candidate::IndexLookup {
            index: Box::new(index),
            table: Box::new(table),
            input: IndexLookUpInput {
                index_rows: 1.25,
                table_rows: 1.25,
                index_row_size: 16.25,
                table_row_size: 24.375,
                pushed_limit: None,
                expected_cnt: f64::MAX,
            },
        };
        let costed = evaluate(&lookup, &env(), TaskType::Root);

        assert_prints(&costed, "2444.77");
        assert_prints(&costed.children[0], "317.07");
        assert_prints(&costed.children[1], "313.89");
        assert_eq!(number_of_ranges(&lookup), 1);
    }

    /// The HASH-join candidate at the SAME decision site, which a
    /// `hash_join(outer_t)` hint makes the same server print. This is the
    /// alternative a cost comparison at that join has to price.
    #[test]
    fn the_hash_join_alternative_of_result_1042_costs_what_go_prints() {
        let index_full_scan = |rows: f64| Candidate::IndexScan {
            rows,
            row_size: RowSize::Fixed(32.0),
            index_id: None,
            num_ranges: 1,
            desc: false,
        };
        let build = projected_t2(index_full_scan(10000.0), ReaderKind::Index);
        let probe = Candidate::Reader {
            child: Box::new(index_full_scan(9990.0)),
            rows: 9990.0,
            row_size: RowSize::Fixed(32.0),
            kind: ReaderKind::Index,
        };
        let join = Candidate::HashJoin {
            build: Box::new(build),
            probe: Box::new(probe),
            input: HashJoinInput {
                build_rows: 8000.0,
                probe_rows: 9990.0,
                build_row_size: 24.0,
                num_build_keys: 1,
                num_probe_keys: 1,
                tidb_concurrency: RECORDED_HASH_JOIN_CONCURRENCY,
            },
            build_filters: Vec::new(),
            probe_filters: Vec::new(),
        };
        let costed = evaluate(&join, &env(), TaskType::Root);

        // └─HashJoin_94(Probe) 10000.00 2373179.65
        assert_prints(&costed, "2373179.65");
        // ├─Projection_61(Build) 8000.00 317954.13
        assert_prints(&costed.children[0], "317954.13");
        // │ └─IndexReader_67 8000.00 236517.33
        assert_prints(&costed.children[0].children[0], "236517.33");
        // │   └─Selection_66 8000.00 2534000.00
        assert_prints(&costed.children[0].children[0].children[0], "2534000.00");
        // │     └─IndexFullScan_65 10000.00 2035000.00
        assert_prints(
            &costed.children[0].children[0].children[0].children[0],
            "2035000.00",
        );
        // └─IndexReader_69(Probe) 9990.00 219926.52
        assert_prints(&costed.children[1], "219926.52");
        // └─IndexFullScan_68 9990.00 2032965.00
        assert_prints(&costed.children[1].children[0], "2032965.00");
    }

    /// `join_reorder_through_projection` `result:1169`: the same shape over
    /// ANALYZEd tables, and the `IndexJoin` variant rather than
    /// `IndexHashJoin` -- so the hash table is built over the probe rows.
    #[test]
    fn the_recorded_index_join_of_result_1169_costs_what_go_prints() {
        let outer = Candidate::Projection {
            child: Box::new(Candidate::Reader {
                child: Box::new(Candidate::Selection {
                    child: Box::new(Candidate::TableScan {
                        rows: 6.0,
                        row_size: RowSize::Fixed(32.0),
                        is_child_of_inl: None,
                        has_full_range_scan: true,
                        // ANALYZEd: no `getTableScanPenalty` rows, which is
                        // why the trace shows ONE `scan(...)` here and two
                        // for the pseudo tables of `result:1042`.
                        penalty: TableScanPenaltyInput {
                            has_range_info: false,
                            allow_prefer_range_scan: true,
                            pseudo_stats: false,
                            analyze_row_count: 6,
                            modify_count: 0,
                            has_partition_scan: false,
                            has_index_force: false,
                        },
                        num_ranges: 1,
                        desc: false,
                    }),
                    input_rows: 6.0,
                    conditions: vec![true],
                }),
                rows: 4.800000000000001,
                row_size: RowSize::Fixed(32.0),
                kind: ReaderKind::Table,
            }),
            input_rows: 4.800000000000001,
            exprs: vec![false, false, true],
        };
        let inner = Candidate::Reader {
            child: Box::new(Candidate::TableScan {
                rows: 0.41666666666666663,
                row_size: RowSize::Fixed(16.0),
                is_child_of_inl: None,
                has_full_range_scan: false,
                penalty: TableScanPenaltyInput::default(),
                num_ranges: 1,
                desc: false,
            }),
            rows: 0.41666666666666663,
            row_size: RowSize::Fixed(16.0),
            kind: ReaderKind::Table,
        };
        let join = Candidate::IndexJoin {
            build: Box::new(outer),
            probe: Box::new(inner),
            input: IndexJoinInput {
                build_rows: 4.800000000000001,
                build_row_size: 24.0,
                probe_rows_one: 0.41666666666666663,
                // `hashmem(2*16*tidb_mem_factor(0.2))`, over
                // `probeRowsTot = 0.4166... * 4.8 = 2`.
                probe_row_size: 16.0,
                num_right_join_keys: 0,
                num_left_join_keys: 0,
                num_ranges: 0.0,
                is_semi_join: false,
                kind: IndexJoinKind::IndexJoin,
            },
            build_filters: Vec::new(),
            probe_filters: Vec::new(),
        };
        let costed = evaluate(&join, &env(), TaskType::Root);

        // └─IndexJoin_31(Probe) 2.00 4106.23
        assert_prints(&costed, "4106.23");
        // ├─Projection_39(Build) 4.80 190.77
        assert_prints(&costed.children[0], "190.77");
        // │ └─TableReader_42 4.80 141.91
        assert_prints(&costed.children[0].children[0], "141.91");
        // │   └─Selection_41 4.80 1520.40
        assert_prints(&costed.children[0].children[0].children[0], "1520.40");
        // │     └─TableFullScan_40 6.00 1221.00
        assert_prints(
            &costed.children[0].children[0].children[0].children[0],
            "1221.00",
        );
        // └─TableReader_38(Probe) 2.00 12.61
        assert_prints(&costed.children[1], "12.61");
        // └─TableRangeScan_37 2.00 162.80
        assert_prints(&costed.children[1].children[0], "162.80");
    }

    /// `getNumberOfRanges` is read off the tree, so a caller cannot declare a
    /// range count its own plan contradicts.
    #[test]
    fn the_range_count_comes_off_the_plan_not_off_the_input() {
        let probe = Candidate::Reader {
            child: Box::new(Candidate::IndexScan {
                rows: 1.0,
                row_size: RowSize::Fixed(16.0),
                index_id: None,
                num_ranges: 7,
                desc: false,
            }),
            rows: 1.0,
            row_size: RowSize::Fixed(16.0),
            kind: ReaderKind::Index,
        };
        assert_eq!(number_of_ranges(&probe), 7);
    }

    /// The task switch a reader performs is INVISIBLE in the cost value --
    /// `tikv_cpu_factor` and `tidb_cpu_factor` are both `49.90`, and
    /// `getTaskScanFactorVer2` answers `tikv_scan_factor` on either task -- so
    /// only the trace text distinguishes them. Go's
    /// `EXPLAIN FORMAT='cost_trace'` prints the coprocessor names below a
    /// reader, and this pins that.
    #[test]
    fn a_reader_costs_its_child_on_the_coprocessor_task() {
        let plan = Candidate::Reader {
            child: Box::new(Candidate::Selection {
                child: Box::new(Candidate::TableScan {
                    rows: 6.0,
                    row_size: RowSize::Fixed(32.0),
                    is_child_of_inl: None,
                    has_full_range_scan: false,
                    penalty: TableScanPenaltyInput::default(),
                    num_ranges: 1,
                    desc: false,
                }),
                input_rows: 6.0,
                conditions: vec![true],
            }),
            rows: 4.8,
            row_size: RowSize::Fixed(32.0),
            kind: ReaderKind::Table,
        };
        let option = PlanCostOption::new().with_cost_flag(crate::cost_usage::COST_FLAG_TRACE);
        let costed = evaluate_traced(&plan, &env(), TaskType::Root, Some(&option));
        let formula = costed
            .cost
            .trace()
            .expect("the trace flag was set")
            .formula()
            .to_owned();
        // Go, for the same shape: `(cpu(6*filters(1)*tikv_cpu_factor(49.9)))
        // + ((scan(6*logrowsize(32)*tikv_scan_factor(40.7)))*1.00)` under
        // `.../15.00`.
        assert!(
            formula.contains("tikv_cpu_factor"),
            "the selection below the reader must be priced on the coprocessor: {formula}"
        );
        assert!(
            !formula.contains("tidb_cpu_factor"),
            "nothing on this plan runs on TiDB's CPU: {formula}"
        );
        assert!(
            formula.contains("tidb_kv_net_factor"),
            "the reader itself pays the TiDB-to-TiKV hop: {formula}"
        );
    }

    /// `getPlanCostVer24PhysicalTableReader` clamps its row size to
    /// `MinRowSize` and `getPlanCostVer24PhysicalIndexReader` does not -- the
    /// one line that is not shared between two otherwise identical readers.
    #[test]
    fn only_the_table_reader_clamps_its_row_size() {
        let narrow = |kind| Candidate::Reader {
            child: Box::new(Candidate::IndexScan {
                rows: 100.0,
                row_size: RowSize::Fixed(16.0),
                index_id: None,
                num_ranges: 1,
                desc: false,
            }),
            rows: 100.0,
            row_size: RowSize::Fixed(1.0),
            kind,
        };
        let table = evaluate(&narrow(ReaderKind::Table), &env(), TaskType::Root);
        let index = evaluate(&narrow(ReaderKind::Index), &env(), TaskType::Root);
        assert_eq!(table.row_size, ver2::MIN_ROW_SIZE);
        assert_eq!(index.row_size, 1.0);
        // `net(100*rowsize(2)*3.96)/15` against `net(100*rowsize(1)*3.96)/15`.
        assert!((table.est_cost() - index.est_cost() - 100.0 * 3.96 / 15.0).abs() < 1e-9);
    }

    /// The tie direction is `compareTaskCost`'s strict `<`.
    #[test]
    fn an_exactly_equal_alternative_never_displaces_the_incumbent() {
        let plan = Candidate::IndexScan {
            rows: 10.0,
            row_size: RowSize::Fixed(16.0),
            index_id: None,
            num_ranges: 1,
            desc: false,
        };
        let costed = evaluate(&plan, &env(), TaskType::CopSingleRead);
        assert!(!prefer(&costed, &costed));
    }
}
