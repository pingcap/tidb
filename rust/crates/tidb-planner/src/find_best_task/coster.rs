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

//! The production [`TaskCoster`]: Go `getPlanCostVer2` over the enum tree.
//!
//! Go prices a task by dispatching `GetPlanCostVer2` down the built plan;
//! each operator's formula lives in `pkg/planner/core/plan_cost_ver2.go`
//! and is ALREADY ported formula by formula in
//! [`crate::plan_cost_ver2`]. This module is the missing walk: recurse the
//! ported operators, apply their formulas with the default factor set, and
//! hand [`super::dispatch::compare_task_cost`] real numbers — which is
//! what makes the dispatcher's index-vs-table and TopN-vs-Limit choices
//! COST decisions rather than first-found ones.
//!
//! # Narrowings, each naming its Go symbol
//!
//! * `getAvgRowSize` (statistics-backed byte widths) is unported; every
//!   row prices at the source's `MIN_ROW_SIZE` floor. Widths shift
//!   absolute numbers, not the operator-shape comparisons the dispatcher
//!   makes between plans over the SAME data.
//! * The reader arms divide by `DistSQLScanConcurrency` exactly as Go's
//!   `getPlanCostVer24Physical{Table,Index}Reader` do — the divisor is what
//!   makes pushed-down work cheap enough for a partial-aggregate push to
//!   beat a root attach, as Go's plans have it. The double-read
//!   (`doubleReadConcurrency`) divisor remains unported with the lookup
//!   tier.
//! * Operators outside the priced set — joins arrive through
//!   [`crate::find_best_task`]'s own model — price as the sum of their
//!   children, conservative and shape-neutral.

use crate::cost_usage::CostVer2;
use crate::physical::PhysicalPlan;
use crate::plan_base::PlanError;
use crate::plan_cost_ver2::{
    filter_cost, hash_agg_cost, net_cost, projection_cost, scan_cost, sort_cost, stream_agg_cost,
    top_n_cost, CostFactorVars, CostSessionOpts, HashAggInput, Ver2Factors,
};
use crate::task::Task;
use crate::task_type::TaskType;

use super::dispatch::TaskCoster;

/// The source `MIN_ROW_SIZE` floor, restated here for the width narrowing.
const ROW_SIZE_FLOOR: f64 = 2.0;

/// The default-factor coster.
#[derive(Default)]
pub struct Ver2Coster {
    factors: Ver2Factors,
    session_factors: CostFactorVars,
    session: CostSessionOpts,
}

impl TaskCoster for Ver2Coster {
    fn task_cost(&self, task: &Task) -> Result<f64, PlanError> {
        let Some(plan) = task.plan() else {
            return Ok(f64::MAX);
        };
        Ok(self.price(plan).value())
    }
}

impl Ver2Coster {
    fn rows(plan: &PhysicalPlan) -> f64 {
        plan.stats_info().map_or(1.0, |stats| stats.row_count())
    }

    fn children_cost(&self, plan: &PhysicalPlan) -> CostVer2 {
        let mut total = crate::cost_usage::zero_cost_ver2();
        for child in plan.children() {
            total = crate::cost_usage::sum_cost_ver2(&[total, self.price(child)]);
        }
        total
    }

    fn price(&self, plan: &PhysicalPlan) -> CostVer2 {
        let rows = Self::rows(plan);
        match plan {
            // `getPlanCostVer24PhysicalTableScan`, Go's own body over the
            // ported input struct: this slice's scans carry full ranges and
            // no probe RangeInfo, and the penalty inputs default exactly as
            // a pseudo-stats session's do.
            PhysicalPlan::TableScan(scan) => crate::plan_cost_ver2::table_scan_cost(
                None,
                crate::plan_cost_ver2::TableScanInput {
                    rows,
                    row_size: ROW_SIZE_FLOOR,
                    is_child_of_inl: None,
                    // Go `ranger.HasFullRange(ts.Ranges, unsignedIntHandle)`;
                    // an unfilled range list reads as the full scan.
                    has_full_range_scan: scan.ranges.is_empty()
                        || crate::ranger::types::has_full_range(&scan.ranges, false),
                    penalty: crate::plan_cost_ver2::TableScanPenaltyInput::default(),
                },
                if scan.desc {
                    &self.factors.tikv_desc_scan
                } else {
                    &self.factors.tikv_scan
                },
                &self.session_factors,
            ),
            // `getPlanCostVer24PhysicalIndexScan`, including Go's untraced
            // `(index-id % 100) / 1e6` tie-breaker between same-cost
            // indexes.
            PhysicalPlan::IndexScan(scan) => crate::plan_cost_ver2::index_scan_cost(
                None,
                rows,
                ROW_SIZE_FLOOR,
                if scan.desc {
                    &self.factors.tikv_desc_scan
                } else {
                    &self.factors.tikv_scan
                },
                1.0,
                Some(scan.index_id),
            ),
            // `getPlanCostVer24PhysicalTableReader` / `...IndexReader`:
            // the pushed side's cost plus the network term (undivided —
            // the concurrency divisor narrows, module header).
            PhysicalPlan::TableReader(reader) => {
                // Go reads the CHILD plan's cardinality for the net rows and
                // divides the whole reader by `DistSQLScanConcurrency`.
                let child_rows = reader
                    .table_plan
                    .as_deref()
                    .map_or(rows, |plan| Self::rows(plan));
                let inner = reader
                    .table_plan
                    .as_deref()
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| self.price(plan));
                crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        inner,
                        net_cost(
                            None,
                            child_rows,
                            ROW_SIZE_FLOOR,
                            &self.factors.tidb_to_kv_net,
                        ),
                    ]),
                    self.session.distsql_scan_concurrency,
                )
            }
            // `getPlanCostVer24PhysicalIndexLookUpReader`
            // (`plan_cost_ver2.go:359`): index side + (table side +
            // double-read CPU/request) / IndexLookupConcurrency, each side
            // divided by DistSQLScanConcurrency; the paging discount when the
            // expected count sits under the paging threshold. row widths at the floor (module
            // header).
            PhysicalPlan::IndexLookUpReader(reader) => {
                let mut index_rows = reader
                    .index_plan
                    .as_deref()
                    .map_or(rows, |plan| Self::rows(plan));
                let mut table_rows = reader
                    .table_plan
                    .as_deref()
                    .map_or(rows, |plan| Self::rows(plan));
                if let Some(pushed) = &reader.pushed_limit {
                    // Go clamps both sides to the pushed count.
                    index_rows = index_rows.min(pushed.count as f64);
                    table_rows = table_rows.min(pushed.count as f64);
                }
                let dist_concurrency = self.session.distsql_scan_concurrency;
                let double_read_concurrency = self.session.index_lookup_concurrency;

                let index_child = reader
                    .index_plan
                    .as_deref()
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| self.price(plan));
                let index_side = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        net_cost(
                            None,
                            index_rows,
                            ROW_SIZE_FLOOR,
                            &self.factors.tidb_to_kv_net,
                        ),
                        index_child,
                    ]),
                    dist_concurrency,
                );

                let table_child = reader
                    .table_plan
                    .as_deref()
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| self.price(plan));
                let table_side = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        net_cost(
                            None,
                            table_rows,
                            ROW_SIZE_FLOOR,
                            &self.factors.tidb_to_kv_net,
                        ),
                        table_child,
                    ]),
                    dist_concurrency,
                );

                let double_read_rows = index_rows;
                let cpu_factor = &self.factors.tidb_cpu;
                let double_read_cpu = crate::cost_usage::new_cost_ver2(
                    None,
                    cpu_factor,
                    double_read_rows * cpu_factor.value(),
                    || format!("double-read-cpu({double_read_rows}*{cpu_factor})"),
                );
                let batch_size = self.session.index_lookup_size;
                let task_per_batch = 32.0;
                let double_read_tasks = double_read_rows / batch_size * task_per_batch;
                let double_read = crate::cost_usage::sum_cost_ver2(&[
                    double_read_cpu,
                    crate::plan_cost_ver2::double_read_cost(
                        None,
                        double_read_tasks,
                        &self.factors.tidb_request,
                    ),
                ]);

                let mut cost = crate::cost_usage::sum_cost_ver2(&[
                    index_side,
                    crate::cost_usage::div_cost_ver2(
                        &crate::cost_usage::sum_cost_ver2(&[table_side, double_read]),
                        double_read_concurrency,
                    ),
                ]);
                let expect = reader.expect_cnt as f64;
                if self.session.enable_paging
                    && expect > 0.0
                    && expect <= crate::plan_cost_ver2::PAGING_THRESHOLD as f64
                {
                    cost = crate::cost_usage::mul_cost_ver2(&cost, 0.6);
                }
                cost
            }
            PhysicalPlan::IndexReader(reader) => {
                let child_rows = reader
                    .index_plan
                    .as_deref()
                    .map_or(rows, |plan| Self::rows(plan));
                let inner = reader
                    .index_plan
                    .as_deref()
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| self.price(plan));
                crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        inner,
                        net_cost(
                            None,
                            child_rows,
                            ROW_SIZE_FLOOR,
                            &self.factors.tidb_to_kv_net,
                        ),
                    ]),
                    self.session.distsql_scan_concurrency,
                )
            }
            // `getPlanCostVer24PhysicalLimit` is the child's cost: a limit
            // adds no work of its own in ver2.
            PhysicalPlan::Limit(_) => self.children_cost(plan),
            // `getPlanCostVer24PhysicalTopN`: the heap's CPU and memory.
            PhysicalPlan::TopN(topn) => {
                let child_cost = self.children_cost(plan);
                let child_rows = plan
                    .children()
                    .first()
                    .map_or(rows, |child| Self::rows(child));
                let by_scalar: Vec<bool> = topn
                    .by_items
                    .iter()
                    .map(|item| {
                        matches!(
                            item.expr,
                            tidb_expr::expression::Expression::ScalarFunction(_)
                        )
                    })
                    .collect();
                top_n_cost(
                    None,
                    child_rows,
                    (topn.count, topn.offset),
                    ROW_SIZE_FLOOR,
                    &by_scalar,
                    (&self.factors.tidb_cpu, &self.factors.tidb_mem, 1.0),
                    &child_cost,
                )
            }
            // `getPlanCostVer24PhysicalSort`, Go's own body: the ported
            // formula with the default session options Go reads. A Sort
            // built by the dispatcher sits on a root task.
            PhysicalPlan::Sort(sort) => {
                let child_cost = self.children_cost(plan);
                let child_rows = plan
                    .children()
                    .first()
                    .map_or(rows, |child| Self::rows(child));
                let by_scalar = vec![false; sort.by_items.len()];
                sort_cost(
                    None,
                    (child_rows, ROW_SIZE_FLOOR),
                    &by_scalar,
                    (&self.factors, &self.session_factors),
                    &self.session,
                    TaskType::Root,
                    &child_cost,
                )
            }
            // `getPlanCostVer24PhysicalSelection`.
            PhysicalPlan::Selection(selection) => {
                let is_scalar: Vec<bool> = selection
                    .conditions
                    .iter()
                    .map(|cond| {
                        matches!(cond, tidb_expr::expression::Expression::ScalarFunction(_))
                    })
                    .collect();
                let child_rows = plan
                    .children()
                    .first()
                    .map_or(rows, |child| Self::rows(child));
                crate::cost_usage::sum_cost_ver2(&[
                    self.children_cost(plan),
                    filter_cost(None, child_rows, &is_scalar, &self.factors.tidb_cpu),
                ])
            }
            // `getPlanCostVer24PhysicalProjection` (concurrency divisor
            // narrows).
            PhysicalPlan::Projection(projection) => {
                let is_scalar: Vec<bool> = projection
                    .exprs
                    .iter()
                    .map(|expr| {
                        matches!(expr, tidb_expr::expression::Expression::ScalarFunction(_))
                    })
                    .collect();
                projection_cost(
                    None,
                    rows,
                    &is_scalar,
                    &self.factors.tidb_cpu,
                    self.session.projection_concurrency,
                    &self.children_cost(plan),
                )
            }
            // `getPlanCostVer24PhysicalStreamAgg`: per-row aggregate and
            // grouping CPU, no hash table.
            PhysicalPlan::StreamAgg(agg) => {
                let child_rows = plan
                    .children()
                    .first()
                    .map_or(rows, |child| Self::rows(child));
                let group_scalar: Vec<bool> = agg
                    .group_by_items
                    .iter()
                    .map(|item| {
                        matches!(item, tidb_expr::expression::Expression::ScalarFunction(_))
                    })
                    .collect();
                stream_agg_cost(
                    None,
                    child_rows,
                    agg.agg_funcs.len(),
                    &group_scalar,
                    (&self.factors.tidb_cpu, 1.0),
                    &self.children_cost(plan),
                )
            }
            // `getPlanCostVer24PhysicalHashAgg`: the hash table's CPU and
            // memory over the grouped output; the statistics-backed output
            // width narrows to the floor, and `child_can_provide_order`
            // reads the built child exactly as Go's
            // `childCanProvideOrderForStreamAgg` asks it.
            PhysicalPlan::HashAgg(agg) => {
                let child_rows = plan
                    .children()
                    .first()
                    .map_or(rows, |child| Self::rows(child));
                let group_scalar: Vec<bool> = agg
                    .group_by_items
                    .iter()
                    .map(|item| {
                        matches!(item, tidb_expr::expression::Expression::ScalarFunction(_))
                    })
                    .collect();
                let child_can_provide_order = plan.children().first().is_some_and(|child| {
                    matches!(child, PhysicalPlan::Sort(_) | PhysicalPlan::StreamAgg(_))
                        || matches!(child, PhysicalPlan::TableReader(reader)
                        if matches!(reader.table_plan.as_deref(),
                            Some(PhysicalPlan::TableScan(scan)) if scan.keep_order))
                });
                hash_agg_cost(
                    None,
                    HashAggInput {
                        input_rows: child_rows,
                        output_rows: rows,
                        output_row_size: ROW_SIZE_FLOOR,
                        num_agg_funcs: agg.agg_funcs.len(),
                        child_can_provide_order,
                    },
                    &group_scalar,
                    (&self.factors.tidb_cpu, &self.factors.tidb_mem, 1.0),
                    // Go reads HashAggFinalConcurrency(), which resolves to
                    // tidb_executor_concurrency's default of 5
                    // (vardef.DefExecutorConcurrency).
                    5.0,
                    TaskType::Root,
                    &self.children_cost(plan),
                )
            }
            // Leaves with no work of their own.
            PhysicalPlan::TableDual(_)
            | PhysicalPlan::CTETable(_)
            | PhysicalPlan::Show(_)
            | PhysicalPlan::ShowDDLJobs(_) => crate::cost_usage::zero_cost_ver2(),
            // Everything else prices as its children, conservative.
            _ => self.children_cost(plan),
        }
    }
}
