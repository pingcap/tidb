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
//! * `getAvgRowSize` uses every physical operator's output schema and
//!   `chunk.EstimateTypeWidth`, matching Go's nil-`HistColl` branch. The
//!   planner stats representation does not yet retain per-column histogram
//!   byte counts, so analyzed widths still fall back to static types.
//! * The reader arms divide by `DistSQLScanConcurrency` exactly as Go's
//!   `getPlanCostVer24Physical{Table,Index}Reader` do — the divisor is what
//!   makes pushed-down work cheap enough for a partial-aggregate push to
//!   beat a root attach, as Go's plans have it. The double-read
//!   (`doubleReadConcurrency`) divisor remains unported with the lookup
//!   tier.
//! * Operators outside the priced set — joins arrive through
//!   [`crate::find_best_task`]'s own model — price as the sum of their
//!   children, conservative and shape-neutral.

use crate::cardinality::row_size::RowSizeColumn;
use crate::cost_usage::CostVer2;
use crate::physical::PhysicalPlan;
use crate::plan_base::PlanError;
use crate::plan_cost_ver2::{
    filter_cost, hash_agg_cost, hash_join_cost, index_join_cost, merge_join_cost, net_cost,
    projection_cost, sort_cost, stream_agg_cost, top_n_cost, CostFactorVars, CostSessionOpts,
    HashAggInput, HashJoinInput, IndexJoinInput, Ver2Factors,
};
use crate::task::Task;
use crate::task_type::TaskType;
use tidb_expr::column::Column;

use super::dispatch::TaskCoster;

/// The statement-local session and factor state read by cost model v2.
///
/// Go reads these values from `SessionVars` while recursively pricing the
/// selected physical-plan candidates. Keeping the snapshot beside the real
/// physical-plan coster avoids the former executor-local `Candidate` tree.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CostEnv {
    /// Go `costVer2Factors`.
    pub factors: Ver2Factors,
    /// The per-operator `tidb_opt_*_cost_factor` multipliers.
    pub cost_factors: CostFactorVars,
    /// Concurrency, quota, paging, and size variables used by the formulas.
    pub session: CostSessionOpts,
}

/// The default-factor coster.
#[derive(Default)]
pub struct Ver2Coster {
    factors: Ver2Factors,
    session_factors: CostFactorVars,
    session: CostSessionOpts,
}

impl TaskCoster for Ver2Coster {
    fn task_cost(&self, task: &Task) -> Result<f64, PlanError> {
        let cost = match task {
            Task::Root(_) => task
                .plan()
                .map_or(f64::MAX, |plan| self.price(plan, TaskType::Root).value()),
            Task::Mpp(_) => task
                .plan()
                .map_or(f64::MAX, |plan| self.price(plan, TaskType::Mpp).value()),
            Task::Cop(cop) => match (&cop.index_plan, &cop.table_plan) {
                (Some(index), Some(table)) if cop.index_plan_finished => {
                    crate::cost_usage::sum_cost_ver2(&[
                        self.price(index, TaskType::CopMultiRead),
                        self.price_with_scan_context(table, TaskType::CopMultiRead, Some(true)),
                    ])
                    .value()
                }
                (Some(index), Some(_)) => self.price(index, TaskType::CopMultiRead).value(),
                (Some(index), None) => self.price(index, TaskType::CopSingleRead).value(),
                (None, Some(table)) => self.price(table, TaskType::CopSingleRead).value(),
                (None, None) => f64::MAX,
            },
        };
        Ok(cost)
    }
}

impl Ver2Coster {
    /// Builds the physical-plan coster from one statement's session snapshot.
    #[must_use]
    pub fn from_env(env: &CostEnv) -> Self {
        Self {
            factors: env.factors.clone(),
            session_factors: env.cost_factors.clone(),
            session: env.session,
        }
    }

    fn rows(plan: &PhysicalPlan) -> f64 {
        plan.stats_info().map_or(1.0, |stats| stats.row_count())
    }

    /// Go `getAvgRowSize(plan.StatsInfo(), plan.Schema().Columns)`: a plan
    /// carrying `StatsInfo.HistColl` uses DataInDiskByRows width, while a plan
    /// with nil HistColl uses only the static type widths.
    fn row_size(plan: &PhysicalPlan) -> f64 {
        Self::row_size_for_columns(
            plan.schema().into_iter().flat_map(|schema| &schema.columns),
            plan.stats_info()
                .and_then(crate::stats_info::StatsInfo::hist_coll),
        )
    }

    /// Go `childCanProvideOrderForStreamAgg`: look through order-preserving
    /// unary nodes and recognize every physical reader boundary. Possible-
    /// property preparation already proved whether a matching StreamAgg can
    /// be built, so the cost model deliberately does not re-check the scan's
    /// `KeepOrder` bit here.
    fn child_can_provide_order_for_stream_agg(mut child: &PhysicalPlan) -> bool {
        loop {
            match child {
                PhysicalPlan::Projection(_) | PhysicalPlan::Selection(_) => {
                    let [next] = child.children() else {
                        return false;
                    };
                    child = next;
                }
                PhysicalPlan::IndexReader(_)
                | PhysicalPlan::IndexLookUpReader(_)
                | PhysicalPlan::IndexMergeReader(_)
                | PhysicalPlan::TableReader(_) => return true,
                _ => return false,
            }
        }
    }

    fn row_size_for_columns<'a>(
        columns: impl Iterator<Item = &'a Column>,
        hist_coll: Option<&crate::stats_info::HistColl>,
    ) -> f64 {
        let columns = columns
            .map(|column| {
                let width = column.ret_type.as_ref().map_or(0.0, |field_type| {
                    tidb_chunk::codec::estimate_type_width(field_type) as f64
                });
                RowSizeColumn {
                    stats: hist_coll.and_then(|hist_coll| hist_coll.column(column.unique_id)),
                    estimated_width: width,
                }
            })
            .collect::<Vec<_>>();
        crate::plan_cost_ver2::plan_avg_row_size(
            &columns,
            hist_coll.map(|hist_coll| (hist_coll.pseudo(), hist_coll.realtime_count())),
        )
    }

    fn children_cost(
        &self,
        plan: &PhysicalPlan,
        task_type: TaskType,
        is_child_of_inl: Option<bool>,
    ) -> CostVer2 {
        let mut total = crate::cost_usage::zero_cost_ver2();
        for child in plan.children() {
            total = crate::cost_usage::sum_cost_ver2(&[
                total,
                self.price_with_scan_context(child, task_type, is_child_of_inl),
            ]);
        }
        total
    }

    fn price(&self, plan: &PhysicalPlan, task_type: TaskType) -> CostVer2 {
        self.price_with_scan_context(plan, task_type, None)
    }

    /// Go passes the variadic `isChildOfINL` flag from an
    /// `IndexLookUpReader` through every operator on its table side until it
    /// reaches the `PhysicalTableScan`.
    fn price_with_scan_context(
        &self,
        plan: &PhysicalPlan,
        task_type: TaskType,
        is_child_of_inl: Option<bool>,
    ) -> CostVer2 {
        let rows = Self::rows(plan);
        match plan {
            // `getPlanCostVer24PhysicalTableScan`, Go's own body over the
            // ported input struct: this slice's scans carry full ranges and
            // no probe RangeInfo, and the penalty inputs default exactly as
            // a pseudo-stats session's do.
            PhysicalPlan::TableScan(scan) => {
                crate::plan_cost_ver2::table_scan_cost(
                    None,
                    crate::plan_cost_ver2::TableScanInput {
                        rows,
                        row_size: if scan.cost_columns.is_empty() {
                            Self::row_size(plan)
                        } else {
                            Self::row_size_for_columns(
                                scan.cost_columns.iter().filter(|column| {
                                    column.id != crate::plan_builder::EXTRA_COMMIT_TS_ID
                                }),
                                plan.stats_info()
                                    .and_then(crate::stats_info::StatsInfo::hist_coll),
                            )
                        },
                        is_child_of_inl,
                        // Go `ranger.HasFullRange(ts.Ranges, unsignedIntHandle)`;
                        // an unfilled range list reads as the full scan.
                        has_full_range_scan: scan.ranges.is_empty()
                            || crate::ranger::types::has_full_range(&scan.ranges, false),
                        penalty: scan.table_scan_penalty,
                    },
                    if scan.desc {
                        &self.factors.tikv_desc_scan
                    } else {
                        &self.factors.tikv_scan
                    },
                    &self.session_factors,
                )
            }
            // `getPlanCostVer24PhysicalIndexScan`, including Go's untraced
            // `(index-id % 100) / 1e6` tie-breaker between same-cost
            // indexes.
            PhysicalPlan::IndexScan(scan) => crate::plan_cost_ver2::index_scan_cost(
                None,
                rows,
                if scan.cost_columns.is_empty() {
                    Self::row_size(plan)
                } else {
                    Self::row_size_for_columns(
                        scan.cost_columns.iter(),
                        plan.stats_info()
                            .and_then(crate::stats_info::StatsInfo::hist_coll),
                    )
                },
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
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| {
                        self.price(plan, TaskType::CopSingleRead)
                    });
                crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        inner,
                        net_cost(
                            None,
                            child_rows,
                            Self::row_size(plan),
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
            // expected count sits under the paging threshold.
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

                let index_plan = reader.index_plan.as_deref();
                let index_child = index_plan
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| {
                        self.price_with_scan_context(plan, TaskType::CopMultiRead, Some(false))
                    });
                let index_side = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        net_cost(
                            None,
                            index_rows,
                            index_plan.map_or_else(|| Self::row_size(plan), Self::row_size),
                            &self.factors.tidb_to_kv_net,
                        ),
                        index_child,
                    ]),
                    dist_concurrency,
                );

                let table_plan = reader.table_plan.as_deref();
                let table_child = table_plan
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| {
                        self.price_with_scan_context(plan, TaskType::CopMultiRead, Some(true))
                    });
                let table_side = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        net_cost(
                            None,
                            table_rows,
                            table_plan.map_or_else(|| Self::row_size(plan), Self::row_size),
                            &self.factors.tidb_to_kv_net,
                        ),
                        table_child,
                    ]),
                    dist_concurrency,
                );

                let double_read_rows = index_rows;
                let cpu_factor = self.factors.task_cpu(task_type);
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
                    .map_or_else(crate::cost_usage::zero_cost_ver2, |plan| {
                        self.price(plan, TaskType::CopSingleRead)
                    });
                crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        inner,
                        net_cost(
                            None,
                            child_rows,
                            Self::row_size(plan),
                            &self.factors.tidb_to_kv_net,
                        ),
                    ]),
                    self.session.distsql_scan_concurrency,
                )
            }
            // `getPlanCostVer24PhysicalLimit` is the child's cost: a limit
            // adds no work of its own in ver2.
            PhysicalPlan::Limit(_) => self.children_cost(plan, task_type, is_child_of_inl),
            // `getPlanCostVer24PhysicalTopN`: the heap's CPU and memory.
            PhysicalPlan::TopN(topn) => {
                let child_cost = self.children_cost(plan, task_type, is_child_of_inl);
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
                    Self::row_size(plan),
                    &by_scalar,
                    (
                        self.factors.task_cpu(task_type),
                        self.factors.task_mem(task_type),
                        1.0,
                    ),
                    &child_cost,
                )
            }
            // `getPlanCostVer24PhysicalSort`, Go's own body: the ported
            // formula with the default session options Go reads. A Sort
            // built by the dispatcher sits on a root task.
            PhysicalPlan::Sort(sort) => {
                let child_cost = self.children_cost(plan, task_type, is_child_of_inl);
                let child_rows = plan
                    .children()
                    .first()
                    .map_or(rows, |child| Self::rows(child));
                let by_scalar = sort
                    .by_items
                    .iter()
                    .map(|item| {
                        matches!(
                            item.expr,
                            tidb_expr::expression::Expression::ScalarFunction(_)
                        )
                    })
                    .collect::<Vec<_>>();
                sort_cost(
                    None,
                    (child_rows, Self::row_size(plan)),
                    &by_scalar,
                    (&self.factors, &self.session_factors),
                    &self.session,
                    task_type,
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
                    self.children_cost(plan, task_type, is_child_of_inl),
                    filter_cost(
                        None,
                        child_rows,
                        &is_scalar,
                        self.factors.task_cpu(task_type),
                    ),
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
                    self.factors.task_cpu(task_type),
                    self.session.projection_concurrency,
                    &self.children_cost(plan, task_type, is_child_of_inl),
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
                    (self.factors.task_cpu(task_type), 1.0),
                    &self.children_cost(plan, task_type, is_child_of_inl),
                )
            }
            // `getPlanCostVer24PhysicalHashAgg`: the hash table's CPU and
            // memory over the grouped output; `child_can_provide_order`
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
                let child_can_provide_order = plan
                    .children()
                    .first()
                    .is_some_and(Self::child_can_provide_order_for_stream_agg);
                hash_agg_cost(
                    None,
                    HashAggInput {
                        input_rows: child_rows,
                        output_rows: rows,
                        output_row_size: Self::row_size(plan),
                        num_agg_funcs: agg.agg_funcs.len(),
                        child_can_provide_order,
                    },
                    &group_scalar,
                    (
                        self.factors.task_cpu(task_type),
                        self.factors.task_mem(task_type),
                        1.0,
                    ),
                    // Go reads HashAggFinalConcurrency(), which resolves to
                    // tidb_executor_concurrency's default of 5
                    // (vardef.DefExecutorConcurrency).
                    5.0,
                    task_type,
                    &self.children_cost(plan, task_type, is_child_of_inl),
                )
            }
            PhysicalPlan::MergeJoin(join) => {
                let [left, right] = plan.children() else {
                    return self.children_cost(plan, task_type, is_child_of_inl);
                };
                let scalar_flags = |conditions: &[tidb_expr::expression::Expression]| {
                    conditions
                        .iter()
                        .map(|condition| {
                            matches!(
                                condition,
                                tidb_expr::expression::Expression::ScalarFunction(_)
                            )
                        })
                        .collect::<Vec<_>>()
                };
                let left_conditions = scalar_flags(&join.left_conditions);
                let right_conditions = scalar_flags(&join.right_conditions);
                let other_conditions = scalar_flags(&join.other_conditions);
                let left_cost = self.price_with_scan_context(left, task_type, is_child_of_inl);
                let right_cost = self.price_with_scan_context(right, task_type, is_child_of_inl);
                merge_join_cost(
                    None,
                    (Self::rows(left), Self::rows(right)),
                    (&left_conditions, &right_conditions, &other_conditions),
                    (join.left_join_keys.len(), join.right_join_keys.len()),
                    (
                        self.factors.task_cpu(task_type),
                        self.session_factors.merge_join,
                    ),
                    (&left_cost, &right_cost),
                )
            }
            PhysicalPlan::HashJoin(join) => {
                let [left, right] = plan.children() else {
                    return self.children_cost(plan, task_type, is_child_of_inl);
                };
                let swap = (join.inner_child_idx == 1 && !join.use_outer_to_build)
                    || (join.inner_child_idx == 0 && join.use_outer_to_build);
                let (build, probe, build_conditions, probe_conditions, build_keys, probe_keys) =
                    if swap {
                        (
                            right,
                            left,
                            &join.right_conditions,
                            &join.left_conditions,
                            &join.right_join_keys,
                            &join.left_join_keys,
                        )
                    } else {
                        (
                            left,
                            right,
                            &join.left_conditions,
                            &join.right_conditions,
                            &join.left_join_keys,
                            &join.right_join_keys,
                        )
                    };
                let scalar_flags = |conditions: &[tidb_expr::expression::Expression]| {
                    conditions
                        .iter()
                        .map(|condition| {
                            matches!(
                                condition,
                                tidb_expr::expression::Expression::ScalarFunction(_)
                            )
                        })
                        .collect::<Vec<_>>()
                };
                let build_filters = scalar_flags(build_conditions);
                let probe_filters = scalar_flags(probe_conditions);
                let build_cost = self.price_with_scan_context(build, task_type, is_child_of_inl);
                let probe_cost = self.price_with_scan_context(probe, task_type, is_child_of_inl);
                hash_join_cost(
                    None,
                    HashJoinInput {
                        build_rows: Self::rows(build),
                        probe_rows: Self::rows(probe),
                        build_row_size: Self::row_size(build),
                        num_build_keys: build_keys.len(),
                        num_probe_keys: probe_keys.len(),
                        tidb_concurrency: join.concurrency as f64,
                    },
                    (&build_filters, &probe_filters),
                    (
                        self.factors.task_cpu(task_type),
                        self.factors.task_mem(task_type),
                        self.session_factors.hash_join,
                    ),
                    task_type,
                    (&build_cost, &probe_cost),
                )
            }
            PhysicalPlan::IndexJoin(join) => {
                let [left, right] = plan.children() else {
                    return self.children_cost(plan, task_type, is_child_of_inl);
                };
                let (build, probe, build_conditions, probe_conditions) =
                    if join.inner_child_idx == 0 {
                        (right, left, &join.right_conditions, &join.left_conditions)
                    } else {
                        (left, right, &join.left_conditions, &join.right_conditions)
                    };
                let scalar_flags = |conditions: &[tidb_expr::expression::Expression]| {
                    conditions
                        .iter()
                        .map(|condition| {
                            matches!(
                                condition,
                                tidb_expr::expression::Expression::ScalarFunction(_)
                            )
                        })
                        .collect::<Vec<_>>()
                };
                let build_filters = scalar_flags(build_conditions);
                let probe_filters = scalar_flags(probe_conditions);
                let build_cost = self.price_with_scan_context(build, task_type, is_child_of_inl);
                let probe_cost = self.price_with_scan_context(probe, task_type, is_child_of_inl);
                index_join_cost(
                    None,
                    IndexJoinInput {
                        build_rows: Self::rows(build),
                        build_row_size: Self::row_size(build),
                        probe_rows_one: Self::rows(probe),
                        probe_row_size: Self::row_size(probe),
                        num_right_join_keys: join.right_join_keys.len(),
                        num_left_join_keys: join.left_join_keys.len(),
                        num_ranges: 1.0,
                        is_semi_join: matches!(
                            join.join_type,
                            crate::find_best_task::LogicalJoinType::Semi
                                | crate::find_best_task::LogicalJoinType::AntiSemi
                                | crate::find_best_task::LogicalJoinType::LeftOuterSemi
                                | crate::find_best_task::LogicalJoinType::AntiLeftOuterSemi
                        ),
                        kind: join.kind,
                    },
                    (&build_filters, &probe_filters),
                    (&self.factors, &self.session_factors),
                    &self.session,
                    task_type,
                    (&build_cost, &probe_cost),
                )
            }
            // Leaves with no work of their own.
            PhysicalPlan::TableDual(_)
            | PhysicalPlan::CTETable(_)
            | PhysicalPlan::Show(_)
            | PhysicalPlan::ShowDDLJobs(_) => crate::cost_usage::zero_cost_ver2(),
            // Everything else prices as its children, conservative.
            _ => self.children_cost(plan, task_type, is_child_of_inl),
        }
    }
}
