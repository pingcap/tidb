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
use crate::cost_usage::{CostVer2, PlanCostOption};
use crate::physical::PhysicalPlan;
use crate::plan_base::PlanError;
use crate::plan_cost_ver2::{
    exchange_receiver_cost, filter_cost, hash_agg_cost, hash_join_cost, index_join_cost,
    merge_join_cost, net_cost, projection_cost, sort_cost, stream_agg_cost, top_n_cost,
    CostFactorVars, CostSessionOpts, HashAggInput, HashJoinInput, IndexJoinInput, NetOwner,
    Ver2Factors,
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
#[derive(Clone, Default)]
pub struct Ver2Coster {
    factors: Ver2Factors,
    session_factors: CostFactorVars,
    session: CostSessionOpts,
    cost_option: Option<crate::cost_usage::PlanCostOption>,
    // Go stores costs on physical nodes. EXPLAIN uses a temporary cache over
    // its immutable retained tree so child rendering reuses recursive costs.
    explain_costs: Option<std::cell::RefCell<std::collections::HashMap<usize, CostVer2>>>,
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
            // Go getTaskPlanCost prices unfinished merge partials alone;
            // after table-side attachment it includes both phases.
            Task::Cop(cop) if !cop.idx_merge_part_plans.is_empty() => {
                let mut cost = cop.idx_merge_part_plans.iter()
                    .map(|partial| self.price(partial, TaskType::CopSingleRead).value())
                    .sum::<f64>();
                if cop.index_plan_finished {
                    if let Some(table) = &cop.table_plan {
                        cost += self.price(table, TaskType::CopSingleRead).value();
                    }
                }
                cost
            }
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
    /// Cost native tasks with the current optimizer session options.
    #[must_use]
    pub fn new(
        factors: Ver2Factors,
        session_factors: CostFactorVars,
        session: CostSessionOpts,
    ) -> Self {
        Self {
            factors,
            session_factors,
            session,
            cost_option: None,
            explain_costs: None,
        }
    }

    /// Builds the physical-plan coster from one statement's session snapshot.
    #[must_use]
    pub fn from_env(env: &CostEnv) -> Self {
        Self {
            factors: env.factors.clone(),
            session_factors: env.cost_factors.clone(),
            session: env.session.clone(),
            cost_option: None,
            explain_costs: None,
        }
    }

    /// Cache costs while rendering one immutable retained plan forest.
    /// Create a fresh coster for each EXPLAIN; do not reuse it after the forest
    /// is changed or dropped. Node addresses preserve identity across readers.
    #[must_use]
    pub fn with_explain_cache(mut self) -> Self {
        self.explain_costs = Some(Default::default());
        self
    }

    /// Prices one physical plan with the same recursive operator dispatch the
    /// volcano task comparison uses. The standalone physical-plan API has no
    /// session owner in Rust, so callers that have a statement snapshot use
    /// [`Self::from_env`] first; the default constructor retains Go's default
    /// factor/session values for detached plans and unit tests.
    #[must_use]
    pub fn plan_cost(
        &self,
        plan: &PhysicalPlan,
        task_type: TaskType,
        is_child_of_inl: bool,
    ) -> CostVer2 {
        self.price_with_scan_context(plan, task_type, Some(is_child_of_inl))
    }

    /// Prices one detached plan while honoring Go's recalculation/trace
    /// option flags. The production task comparator keeps this unset because
    /// it compares only numeric costs; the physical-plan compatibility API
    /// supplies the caller's option here.
    #[must_use]
    pub fn plan_cost_with_option(
        &self,
        plan: &PhysicalPlan,
        task_type: TaskType,
        is_child_of_inl: bool,
        option: PlanCostOption,
    ) -> CostVer2 {
        let mut coster = self.clone();
        coster.cost_option = Some(option);
        coster.plan_cost(plan, task_type, is_child_of_inl)
    }

    fn cost_option(&self) -> Option<&crate::cost_usage::PlanCostOption> {
        self.cost_option.as_ref()
    }

    fn rows(plan: &PhysicalPlan) -> f64 {
        crate::plan_cost_ver2::cardinality(plan.stats_info().map_or(1.0, |stats| stats.row_count()))
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
        if std::env::var("TIDB_DEBUG_NDV").is_ok() {
            let misses = columns.iter().filter(|c| c.stats.is_none()).count();
            let pseudo = hist_coll.is_none_or(crate::stats_info::HistColl::pseudo);
            let realtime = hist_coll.map_or(0, |h| h.realtime_count());
            eprintln!(
                "ROWSIZE cols={} misses={} pseudo={} realtime={}",
                columns.len(),
                misses,
                pseudo,
                realtime
            );
        }
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
        let mut total = crate::cost_usage::ZERO_COST_VER2;
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
        let key = std::ptr::from_ref(plan) as usize;
        if let Some(cache) = &self.explain_costs {
            if let Some(cost) = cache.borrow().get(&key) {
                return cost.clone();
            }
        }
        let cost = self.price_uncached(plan, task_type, is_child_of_inl);
        if let Some(cache) = &self.explain_costs {
            cache.borrow_mut().insert(key, cost.clone());
        }
        cost
    }

    fn price_uncached(
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
                let row_size = if scan.cost_columns.is_empty() {
                    Self::row_size(plan)
                } else {
                    Self::row_size_for_columns(
                        scan.cost_columns
                            .iter()
                            .filter(|column| column.id != crate::plan_builder::EXTRA_COMMIT_TS_ID),
                        plan.stats_info()
                            .and_then(crate::stats_info::StatsInfo::hist_coll),
                    )
                };
                if scan.store_type == crate::physical_table_reader::StoreType::TiFlash {
                    crate::plan_cost_ver2::tiflash_table_scan_cost(
                        self.cost_option(),
                        rows,
                        Self::row_size(plan),
                        &self.factors.tiflash_scan,
                        self.session_factors.table_tiflash_scan,
                    )
                } else {
                    crate::plan_cost_ver2::table_scan_cost(
                        self.cost_option(),
                        crate::plan_cost_ver2::TableScanInput {
                            rows,
                            row_size,
                            is_child_of_inl,
                            // Go `ranger.HasFullRange(ts.Ranges, unsignedIntHandle)`;
                            // an unfilled range list reads as the full scan.
                            has_full_range_scan: scan.ranges.is_empty()
                                || crate::ranger::types::has_full_range(&scan.ranges, false),
                            penalty: scan.table_scan_penalty,
                        },
                        self.factors
                            .task_scan(false, scan.store_type, task_type, scan.desc),
                        &self.session_factors,
                    )
                }
            }
            // `getPlanCostVer24PhysicalIndexScan`, including Go's untraced
            // `(index-id % 100) / 1e6` tie-breaker between same-cost
            // indexes.
            PhysicalPlan::IndexScan(scan) => crate::plan_cost_ver2::index_scan_cost(
                self.cost_option(),
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
                self.factors.task_scan(
                    false,
                    crate::physical_table_reader::StoreType::TiKv,
                    task_type,
                    scan.desc,
                ),
                self.session_factors.index_scan,
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
                let child_task_type =
                    if reader.store_type == crate::physical_table_reader::StoreType::TiFlash {
                        TaskType::Mpp
                    } else {
                        TaskType::CopSingleRead
                    };
                let inner = reader.table_plan.as_deref().map_or_else(
                    || crate::cost_usage::ZERO_COST_VER2,
                    |plan| self.price(plan, child_task_type),
                );
                let net_owner = if reader
                    .table_plan
                    .as_deref()
                    .is_some_and(|plan| matches!(plan, PhysicalPlan::ExchangeSender(_)))
                {
                    crate::plan_cost_ver2::NetOwner::TiDbToTiFlash
                } else {
                    crate::plan_cost_ver2::NetOwner::TiDbToTiKv
                };
                let mut cost = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        inner,
                        net_cost(
                            self.cost_option(),
                            child_rows,
                            Self::row_size(plan),
                            self.factors.task_net(false, net_owner),
                        ),
                    ]),
                    self.session.distsql_scan_concurrency,
                );
                if reader.store_type == crate::physical_table_reader::StoreType::TiFlash
                    && self.session.mpp_enforced
                    && !self.cost_option().is_some_and(|option| {
                        option.cost_flag() & crate::cost_usage::COST_FLAG_RECALCULATE != 0
                    })
                {
                    cost = crate::cost_usage::div_cost_ver2(
                        &cost,
                        crate::plan_cost_ver2::MPP_ENFORCED_DISCOUNT,
                    );
                }
                crate::cost_usage::mul_cost_ver2(&cost, self.session_factors.table_reader)
            }
            PhysicalPlan::ExchangeReceiver(_receiver) => {
                let child = plan.children().first();
                let child_cost = child.map_or(crate::cost_usage::ZERO_COST_VER2, |child| {
                    self.price(child, TaskType::Mpp)
                });
                let is_broadcast = child.is_some_and(|child| {
                    matches!(
                        child,
                        PhysicalPlan::ExchangeSender(sender)
                            if sender.exchange_type == crate::physical::ExchangeType::Broadcast
                    )
                });
                exchange_receiver_cost(
                    self.cost_option(),
                    rows,
                    Self::row_size(plan),
                    self.factors.task_net(false, NetOwner::TiFlashMpp),
                    is_broadcast,
                    &child_cost,
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
                let net_factor = self
                    .factors
                    .task_net(false, crate::plan_cost_ver2::NetOwner::TiDbToTiKv);

                let index_plan = reader.index_plan.as_deref();
                let index_child = index_plan.map_or_else(
                    || crate::cost_usage::ZERO_COST_VER2,
                    |plan| self.price_with_scan_context(plan, TaskType::CopMultiRead, Some(false)),
                );
                let index_side = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        net_cost(
                            self.cost_option(),
                            index_rows,
                            index_plan.map_or_else(|| Self::row_size(plan), Self::row_size),
                            net_factor,
                        ),
                        index_child,
                    ]),
                    dist_concurrency,
                );

                let table_plan = reader.table_plan.as_deref();
                let table_child = table_plan.map_or_else(
                    || crate::cost_usage::ZERO_COST_VER2,
                    |plan| self.price_with_scan_context(plan, TaskType::CopMultiRead, Some(true)),
                );
                let table_side = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        net_cost(
                            self.cost_option(),
                            table_rows,
                            table_plan.map_or_else(|| Self::row_size(plan), Self::row_size),
                            net_factor,
                        ),
                        table_child,
                    ]),
                    dist_concurrency,
                );

                let double_read_rows = index_rows;
                let cpu_factor = self.factors.task_cpu(task_type);
                let double_read_cpu = crate::cost_usage::new_cost_ver2(
                    self.cost_option(),
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
                        self.cost_option(),
                        double_read_tasks,
                        self.factors.task_request(false),
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
                if reader
                    .pushed_limit
                    .is_some_and(|limit| table_rows <= limit.count as f64)
                {
                    cost = crate::cost_usage::mul_cost_ver2(&cost, self.session_factors.limit);
                }
                crate::cost_usage::mul_cost_ver2(&cost, self.session_factors.index_lookup)
            }
            PhysicalPlan::IndexReader(reader) => {
                let child_rows = reader
                    .index_plan
                    .as_deref()
                    .map_or(rows, |plan| Self::rows(plan));
                let inner = reader.index_plan.as_deref().map_or_else(
                    || crate::cost_usage::ZERO_COST_VER2,
                    |plan| self.price(plan, TaskType::CopSingleRead),
                );
                let cost = crate::cost_usage::div_cost_ver2(
                    &crate::cost_usage::sum_cost_ver2(&[
                        inner,
                        net_cost(
                            self.cost_option(),
                            child_rows,
                            Self::row_size(plan),
                            self.factors
                                .task_net(false, crate::plan_cost_ver2::NetOwner::TiDbToTiKv),
                        ),
                    ]),
                    self.session.distsql_scan_concurrency,
                );
                crate::cost_usage::mul_cost_ver2(&cost, self.session_factors.index_reader)
            }
            // `getPlanCostVer24PhysicalIndexMergeReader`: every partial and
            // the table side price as (net + child) / dist-concurrency.
            PhysicalPlan::IndexMergeReader(reader) => {
                let side = |plan: &PhysicalPlan, is_child_of_inl: bool| {
                    crate::plan_cost_ver2::IndexMergeSide {
                        rows: Self::rows(plan).max(crate::plan_cost_ver2::MIN_NUM_ROWS),
                        row_size: Self::row_size(plan),
                        child_cost: self.price_with_scan_context(
                            plan,
                            TaskType::CopMultiRead,
                            Some(is_child_of_inl),
                        ),
                    }
                };
                let index_sides: Vec<crate::plan_cost_ver2::IndexMergeSide> = reader
                    .partial_plans_raw
                    .iter()
                    .map(|plan| side(plan, false))
                    .collect();
                let table_side = reader.table_plan.as_deref().map(|plan| side(plan, true));
                crate::plan_cost_ver2::index_merge_reader_cost(
                    self.cost_option(),
                    table_side.as_ref(),
                    &index_sides,
                    &self.factors.tidb_to_kv_net,
                    (
                        self.session.distsql_scan_concurrency as f64,
                        reader.pushed_limit.is_some(),
                        &self.session_factors,
                    ),
                )
            }
            // `getPlanCostVer24PhysicalLimit` is the child's cost: a limit
            // adds no work of its own in ver2.
            PhysicalPlan::Limit(_) => self.children_cost(plan, task_type, is_child_of_inl),
            // `getPlanCostVer24PhysicalTopN`: the heap's CPU and memory.
            PhysicalPlan::TopN(topn) => {
                let child_cost = self.children_cost(plan, task_type, is_child_of_inl);
                // Go `getPlanCostVer24PhysicalTopN` indexes
                // `p.Children()[0]` for the child cardinality
                // (`plan_cost_ver2.go:561`).
                let child_rows = Self::rows(&plan.children()[0]);
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
                    self.cost_option(),
                    child_rows,
                    (topn.count, topn.offset),
                    Self::row_size(plan),
                    &by_scalar,
                    (
                        self.factors.task_cpu(task_type),
                        self.factors.task_mem(task_type),
                        self.session_factors.topn,
                    ),
                    &child_cost,
                )
            }
            // `getPlanCostVer24PhysicalSort`, Go's own body: the ported
            // formula with the default session options Go reads. A Sort
            // built by the dispatcher sits on a root task.
            PhysicalPlan::Sort(sort) => {
                let child_cost = self.children_cost(plan, task_type, is_child_of_inl);
                // Go `getPlanCostVer24PhysicalTopN` indexes
                // `p.Children()[0]` for the child cardinality
                // (`plan_cost_ver2.go:561`).
                let child_rows = Self::rows(&plan.children()[0]);
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
                    self.cost_option(),
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
                // Go `getPlanCostVer24PhysicalTopN` indexes
                // `p.Children()[0]` for the child cardinality
                // (`plan_cost_ver2.go:561`).
                let child_rows = Self::rows(&plan.children()[0]);
                crate::cost_usage::sum_cost_ver2(&[
                    self.children_cost(plan, task_type, is_child_of_inl),
                    filter_cost(
                        self.cost_option(),
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
                    self.cost_option(),
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
                // Go `getPlanCostVer24PhysicalTopN` indexes
                // `p.Children()[0]` for the child cardinality
                // (`plan_cost_ver2.go:561`).
                let child_rows = Self::rows(&plan.children()[0]);
                let group_scalar: Vec<bool> = agg
                    .group_by_items
                    .iter()
                    .map(|item| {
                        matches!(item, tidb_expr::expression::Expression::ScalarFunction(_))
                    })
                    .collect();
                stream_agg_cost(
                    self.cost_option(),
                    child_rows,
                    agg.agg_funcs.len(),
                    &group_scalar,
                    (
                        self.factors.task_cpu(task_type),
                        self.session_factors.stream_agg,
                    ),
                    &self.children_cost(plan, task_type, is_child_of_inl),
                )
            }
            // `getPlanCostVer24PhysicalHashAgg`: the hash table's CPU and
            // memory over the grouped output; `child_can_provide_order`
            // reads the built child exactly as Go's
            // `childCanProvideOrderForStreamAgg` asks it.
            PhysicalPlan::HashAgg(agg) => {
                // Go `getPlanCostVer24PhysicalTopN` indexes
                // `p.Children()[0]` for the child cardinality
                // (`plan_cost_ver2.go:561`).
                let child_rows = Self::rows(&plan.children()[0]);
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
                    self.cost_option(),
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
                    // Go reads `HashAggFinalConcurrency()`, which is the
                    // session's resolved `tidb_hashagg_final_concurrency` (the
                    // unset value falls back to `tidb_executor_concurrency`).
                    // It is NOT always 5: a serial session makes the HashAgg's
                    // divided CPU cost lose to a StreamAgg, which is exactly
                    // how Go picks the serial root StreamAgg.
                    self.session.hashagg_final_concurrency,
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
                    self.cost_option(),
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
                    self.cost_option(),
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
            // `getPlanCostVer24PhysicalApply`: an Apply re-runs its inner
            // child once per outer row, so the probe cost is multiplied by
            // the build cardinality without the index-join batch discount.
            PhysicalPlan::Apply(apply) => {
                let [build, probe] = plan.children() else {
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
                let build_cost = self.price(build, task_type);
                let probe_cost = self.price(probe, task_type);
                crate::plan_cost_ver2::apply_cost(
                    self.cost_option(),
                    Self::rows(build),
                    Self::rows(probe),
                    (
                        &scalar_flags(&apply.hash_join.left_conditions),
                        &scalar_flags(&apply.hash_join.right_conditions),
                    ),
                    self.factors.task_cpu(task_type),
                    (&build_cost, &probe_cost),
                )
            }
            // `getPlanCostVer24PhysicalUnionAll`: child costs are shared by
            // the union workers, with Go's enforced-MPP comparison discount.
            PhysicalPlan::UnionAll(union) => {
                let child_costs = plan
                    .children()
                    .iter()
                    .map(|child| self.price(child, task_type))
                    .collect::<Vec<_>>();
                crate::plan_cost_ver2::union_all_cost_with_option(
                    self.cost_option(),
                    &child_costs,
                    self.session.union_concurrency,
                    union.mpp && self.session.mpp_enforced,
                )
            }
            // Go's fast-plan distinction is represented by `access_cols`:
            // plans built by the statement fast path are free, while
            // optimizer-created point plans pay one network read.
            PhysicalPlan::PointGet(point) => crate::plan_cost_ver2::point_get_cost(
                self.cost_option(),
                1.0,
                Self::row_size(plan),
                &self.factors.tidb_to_kv_net,
                point.access_cols.is_some(),
            ),
            PhysicalPlan::BatchPointGet(point) => crate::plan_cost_ver2::point_get_cost(
                self.cost_option(),
                Self::rows(plan),
                Self::row_size(plan),
                &self.factors.tidb_to_kv_net,
                point.access_cols.is_some(),
            ),
            // `getPlanCostVer24PhysicalCTE`: reading a CTE charges only the
            // projection work for the CTE's output schema; producer work is
            // owned by the sequence and is not paid by each consumer.
            PhysicalPlan::CTE(_) => crate::plan_cost_ver2::cte_cost(
                self.cost_option(),
                rows,
                plan.schema().map_or(0, |schema| schema.columns.len()),
                self.factors.task_cpu(task_type),
            ),
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
                let cost = index_join_cost(
                    self.cost_option(),
                    IndexJoinInput {
                        build_rows: Self::rows(build),
                        build_row_size: Self::row_size(build),
                        probe_rows_one: Self::rows(probe),
                        probe_row_size: Self::row_size(probe),
                        // Go `constructIndexJoinStatic` fills `BasePhysicalJoin`'s
                        // `OuterJoinKeys`/`InnerJoinKeys` and leaves
                        // `LeftJoinKeys`/`RightJoinKeys` EMPTY until
                        // `completePhysicalIndexJoin` runs after the inner task is
                        // chosen. The v2 cost reads `len(p.RightJoinKeys)` /
                        // `len(p.LeftJoinKeys)`, so a static candidate's hash table
                        // is priced with ZERO keys.
                        num_right_join_keys: 0,
                        num_left_join_keys: 0,
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
                );
                if std::env::var("TIDB_DEBUG_NDV").is_ok() {
                    let probe_cols: Vec<String> = probe
                        .schema()
                        .map(|schema| {
                            schema
                                .columns
                                .iter()
                                .map(|column| column.orig_name.clone())
                                .collect()
                        })
                        .unwrap_or_default();
                    eprintln!(
                        "Q50COST kind={:?} build_rows={} build_row_size={} probe_rows_one={} probe_row_size={} cost={} probe_cols={:?}",
                        join.kind,
                        Self::rows(build),
                        Self::row_size(build),
                        Self::rows(probe),
                        Self::row_size(probe),
                        cost.value(),
                        probe_cols
                    );
                }
                cost
            }
            // Leaves with no work of their own.
            PhysicalPlan::TableDual(_)
            | PhysicalPlan::CTETable(_)
            | PhysicalPlan::Show(_)
            | PhysicalPlan::ShowDDLJobs(_) => crate::cost_usage::ZERO_COST_VER2,
            // Everything else prices as its children, conservative.
            _ => self.children_cost(plan, task_type, is_child_of_inl),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{
        BasePhysicalPlan, PhysicalCTE, PhysicalPlan, PhysicalPointGet, PhysicalTableDual,
        PhysicalUnionAll,
    };
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::StatsInfo;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;

    fn point(id: i32, access_cols: bool) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "Point_Get", 0);
        base.base.set_stats(Some(StatsInfo::new(1.0, [])));
        let column = Column::new(id.into(), FieldType::new(FieldTypeCode::LongLong));
        base.base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        PhysicalPlan::PointGet(PhysicalPointGet {
            base,
            lock: false,
            table_id: i64::from(id),
            partition: None,
            index_id: None,
            access_cols: access_cols.then_some(vec![column]),
            ranges: Vec::new(),
            range_rebuild: None,
        })
    }

    fn root(plan: PhysicalPlan) -> Task {
        let mut task = crate::task::RootTask::default();
        task.set_plan(plan);
        Task::Root(task)
    }

    #[test]
    fn explain_cache_retains_reader_context_without_repricing_children() {
        let mut base = BasePhysicalPlan::with_id(1, "TableScan", 0);
        base.base.set_stats(Some(StatsInfo::new(100.0, [])));
        base.base.set_schema(Some(Schema::new(vec![Column::new(
            1, FieldType::new(FieldTypeCode::LongLong),
        )])));
        let scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
            base,
            ..Default::default()
        });
        let plan = PhysicalPlan::TableReader(crate::physical::PhysicalTableReader {
            table_plan: Some(Box::new(scan)),
            ..Default::default()
        });
        let coster = Ver2Coster::default().with_explain_cache();
        let root_cost = coster.plan_cost(&plan, TaskType::Root, false).value();
        let PhysicalPlan::TableReader(reader) = &plan else { unreachable!() };
        let scan = reader.table_plan.as_deref().unwrap();
        let scan_key = std::ptr::from_ref(scan) as usize;
        let cached = coster.explain_costs.as_ref().unwrap().borrow()[&scan_key].value();
        assert_eq!(coster.plan_cost(scan, TaskType::Root, false).value(), cached);
        assert_eq!(coster.explain_costs.as_ref().unwrap().borrow().len(), 2);
        assert_eq!(root_cost, Ver2Coster::default().plan_cost(&plan, TaskType::Root, false).value());
    }

    #[test]
    fn merge_cop_cost_follows_index_and_table_phases() {
        let coster = Ver2Coster::default();
        let partial = point(1, true);
        let table = point(2, true);
        let partial_cost = coster.price(&partial, TaskType::CopSingleRead).value();
        let table_cost = coster.price(&table, TaskType::CopSingleRead).value();
        assert!(partial_cost > 0.0 && table_cost > 0.0);
        let mut cop = crate::task::CopTask {
            idx_merge_part_plans: vec![partial.clone(), partial],
            table_plan: Some(Box::new(table)),
            ..Default::default()
        };
        // Go getTaskPlanCost handles a nil live plan as unfinished merge:
        // all partial work counts, while table work starts in the next phase.
        assert!(cop.plan().is_none());
        assert_eq!(
            coster.task_cost(&Task::Cop(cop.clone())).unwrap(),
            2.0 * partial_cost
        );
        cop.finish_index_plan();
        assert_eq!(
            coster.task_cost(&Task::Cop(cop)).unwrap(),
            2.0 * partial_cost + table_cost
        );
    }

    #[test]
    fn package_specific_ver2_operators_use_their_go_cost_bodies() {
        let coster = Ver2Coster::default();
        let fast = coster
            .task_cost(&root(point(1, false)))
            .expect("fast point plan cost");
        let point_cost = coster
            .task_cost(&root(point(2, true)))
            .expect("optimizer point plan cost");
        assert_eq!(fast, 0.0);
        assert!(point_cost > 0.0);

        let allocator = PlanIdAllocator::new();
        let mut union_base = BasePhysicalPlan::new(&allocator, "Union", 0);
        union_base.set_children(vec![point(3, true), point(4, true)]);
        let union = PhysicalPlan::UnionAll(PhysicalUnionAll {
            base: union_base,
            mpp: false,
        });
        let union_cost = coster.task_cost(&root(union)).expect("union-all cost");
        assert!((union_cost - point_cost * 2.0 / 5.0).abs() < 1e-9);

        let mut cte_base = BasePhysicalPlan::new(&allocator, "CTE", 0);
        cte_base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        cte_base.base.set_schema(Some(Schema::new(vec![Column::new(
            10,
            FieldType::new(FieldTypeCode::LongLong),
        )])));
        let cte = PhysicalPlan::CTE(PhysicalCTE {
            base: cte_base,
            seed_plan: Box::new(PhysicalPlan::TableDual(PhysicalTableDual {
                base: BasePhysicalPlan::new(&allocator, "TableDual", 0),
                row_count: 0,
            })),
            recursive_plan: None,
            id_for_storage: 1,
            is_distinct: false,
            has_limit: false,
            limit_beg: 0,
            limit_end: 0,
            cte_as_name: String::new(),
            cte_name: String::new(),
        });
        let cte_cost = coster.task_cost(&root(cte)).expect("cte cost");
        assert!(cte_cost > 0.0);

        let mut option = PlanCostOption::new();
        option.with_cost_flag(crate::cost_usage::COST_FLAG_TRACE);
        let detached = point(5, true);
        let traced = detached
            .get_plan_cost_ver2(TaskType::Root, option, false)
            .expect("detached plan cost");
        assert!(traced.trace().is_some());
    }

    #[test]
    fn exchange_receiver_cost_includes_child_and_broadcast_network_work() {
        let allocator = PlanIdAllocator::new();
        let child = point(6, true);
        let mut sender_base = BasePhysicalPlan::new(&allocator, "ExchangeSender", 0);
        sender_base.set_children(vec![child]);
        let sender = PhysicalPlan::ExchangeSender(crate::physical::PhysicalExchangeSender {
            base: sender_base,
            exchange_type: crate::physical::ExchangeType::Broadcast,
            hash_cols: Vec::new(),
        });
        let mut receiver_base = BasePhysicalPlan::new(&allocator, "ExchangeReceiver", 0);
        receiver_base.base.set_stats(Some(StatsInfo::new(5.0, [])));
        receiver_base.set_children(vec![sender]);
        let receiver = PhysicalPlan::ExchangeReceiver(crate::physical::PhysicalExchangeReceiver {
            base: receiver_base,
            tasks: Vec::new(),
        });

        let coster = Ver2Coster::default();
        let actual = coster.price(&receiver, TaskType::Mpp);
        let child_cost = coster.price(&receiver.children()[0], TaskType::Mpp);
        let expected_network = crate::plan_cost_ver2::exchange_receiver_cost(
            None,
            5.0,
            Ver2Coster::row_size(&receiver),
            &coster.factors.tiflash_mpp_net,
            true,
            &child_cost,
        );
        assert_eq!(actual, expected_network);
    }
}
