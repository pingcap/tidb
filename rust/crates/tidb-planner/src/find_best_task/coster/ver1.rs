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

//! Go `pkg/planner/core/plan_cost_ver1.go`: each operator's
//! `getPlanCostVer14<Operator>` and `getCost4<Operator>` over the enum tree,
//! the costs [`super::Ver2Coster`] answers when the session runs cost model
//! version 1. Go's `CostFlagUseTrueCardinality` (runtime row counts, for
//! `EXPLAIN ANALYZE FORMAT = 'true_card_cost'`) is not read: costs use the
//! estimates, as every planning-time call does.

use tidb_kv::store_type::StoreType;
use tidb_planner_core_base::LogicalJoinType;
use tidb_planner_core_cost::{DISTINCT_FACTOR, SELECTION_FACTOR, SMALL_SCAN_THRESHOLD};
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_planner_property::stats_info::HistCollExt as _;
use tidb_planner_property::TaskType;

use crate::plan_cost_ver1::Ver1Factors;

use super::{plan_is_temporary, Ver2Coster};

/// Go `getCardinality` for version 1: the estimate as it is (version 2 alone
/// turns a non-positive estimate into one row).
fn card(plan: &PhysicalPlan) -> f64 {
    plan.stats_count().unwrap_or(0.0)
}

fn is_semi(join_type: LogicalJoinType) -> bool {
    matches!(
        join_type,
        LogicalJoinType::Semi
            | LogicalJoinType::AntiSemi
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiLeftOuterSemi
    )
}

/// Go `estimateNetSeekCost`.
fn estimate_net_seek_cost(plan: &PhysicalPlan, f: &Ver1Factors) -> f64 {
    match plan {
        PhysicalPlan::TableScan(x) => {
            if x.store_type == StoreType::TiFlash {
                // the old TiFlash interface uses cop-task protocol
                return x.ranges.len() as f64 * x.columns.len() as f64 * f.seek(x.is_temporary);
            }
            x.ranges.len() as f64 * f.seek(x.is_temporary)
        }
        PhysicalPlan::IndexScan(x) => x.ranges.len() as f64 * f.seek(x.is_temporary),
        _ => plan.children().first().map_or(0.0, |child| estimate_net_seek_cost(child, f)),
    }
}

/// Go `getTableNetFactor`.
fn table_net_factor(plan: &PhysicalPlan, f: &Ver1Factors) -> f64 {
    match plan {
        PhysicalPlan::TableScan(x) => f.network(x.is_temporary),
        PhysicalPlan::IndexScan(x) => f.network(x.is_temporary),
        _ => plan.children().first().map_or(f.network, |child| table_net_factor(child, f)),
    }
}

/// Go `extractRows`.
fn extract_rows(plan: &PhysicalPlan) -> f64 {
    plan.children()
        .iter()
        .map(|child| if child.children().is_empty() { card(child) } else { extract_rows(child) })
        .sum()
}

impl Ver2Coster {
    fn child_ver1(&self, plan: &PhysicalPlan, task_type: TaskType) -> f64 {
        self.price(plan, task_type).value()
    }

    /// Go `calcPagingCost`.
    fn calc_paging_cost(&self, index_plan: &PhysicalPlan, mut expect_cnt: u64, f: &Ver1Factors) -> f64 {
        let index_rows = card(index_plan);
        let source_rows = extract_rows(index_plan);
        // with paging, the scanned rows is always less than or equal to source rows.
        if (source_rows as u64) < expect_cnt {
            expect_cnt = source_rows as u64;
        }
        let seek_cnt = tidb_paging::calculate_seek_cnt(expect_cnt);
        let mut index_selectivity = 1.0;
        if source_rows > index_rows {
            index_selectivity = index_rows / source_rows;
        }
        let mut paging_cst = seek_cnt * f.seek + expect_cnt as f64 * f.cpu;
        paging_cst *= index_selectivity;
        (paging_cst - f.seek).max(0.0)
    }

    /// Go `PhysicalTableScan.GetScanRowSize` / `PhysicalIndexScan.GetScanRowSize`.
    fn scan_row_size(&self, plan: &PhysicalPlan) -> f64 {
        use tidb_planner_cardinality::row_size::{
            get_index_avg_row_size, get_table_avg_row_size, RowSizeColumn, RowSizeStore,
        };
        let chunk_rpc = self.session.enable_chunk_rpc;
        let columns = |cols: &mut dyn Iterator<Item = &tidb_expr::column::Column>,
                       hist: Option<&tidb_planner_property::stats_info::HistColl>| {
            cols.map(|column| RowSizeColumn {
                stats: hist.and_then(|hist| hist.column(column.unique_id)),
                estimated_width: 0.0,
            })
            .collect::<Vec<_>>()
        };
        match plan {
            PhysicalPlan::TableScan(x) => {
                let hist = x.tbl_col_hists.as_deref();
                let pseudo = hist.is_none_or(|hist| hist.pseudo());
                let count = hist.map_or(0, |hist| hist.realtime_count());
                if x.store_type == StoreType::TiKv {
                    // _tidb_commit_ts is not stored, so it costs nothing.
                    let cols = columns(
                        &mut x.tbl_cols.iter().filter(|c| c.id != crate::plan_builder::EXTRA_COMMIT_TS_ID),
                        hist,
                    );
                    return get_table_avg_row_size(&cols, pseudo, count, RowSizeStore::TiKv, true, chunk_rpc);
                }
                let schema_cols = plan.schema().map(|s| s.columns.as_slice()).unwrap_or_default();
                let cols = columns(&mut schema_cols.iter(), hist);
                let store = if x.store_type == StoreType::TiFlash { RowSizeStore::TiFlash } else { RowSizeStore::TiKv };
                get_table_avg_row_size(&cols, pseudo, count, store, x.handle_cols.is_some(), chunk_rpc)
            }
            PhysicalPlan::IndexScan(x) => {
                let hist = x.tbl_col_hists.as_deref();
                let pseudo = hist.is_none_or(|hist| hist.pseudo());
                let count = hist.map_or(0, |hist| hist.realtime_count());
                let schema_cols = plan.schema().map(|s| s.columns.as_slice()).unwrap_or_default();
                let index_cols = x.index.as_ref().map_or(0, |index| index.read().columns.len());
                // If `initSchema` has already appended the handle column in the
                // schema, use the schema columns, otherwise add the handle column.
                let cols = if index_cols == schema_cols.len() {
                    columns(&mut schema_cols.iter().chain(x.pk_is_handle_col.iter()), hist)
                } else {
                    columns(&mut schema_cols.iter(), hist)
                };
                let unique = x.index.as_ref().is_some_and(|index| index.read().unique);
                get_index_avg_row_size(&cols, pseudo, count, unique, chunk_rpc)
            }
            _ => 0.0,
        }
    }

    /// Go `GetPlanCostVer1(taskType, option)` dispatched per operator.
    pub(super) fn price_ver1(&self, plan: &PhysicalPlan, task_type: TaskType, f: &Ver1Factors) -> f64 {
        let s = &self.session;
        match plan {
            PhysicalPlan::Selection(sel) => {
                let cpu_factor = match task_type {
                    TaskType::CopSingleRead | TaskType::CopMultiRead => f.cop_cpu,
                    _ => f.cpu,
                };
                let child = &plan.children()[0];
                // for compatibility, see https://github.com/pingcap/tidb/issues/36243
                let self_cost = if sel.from_data_source { 0.0 } else { card(child) * cpu_factor };
                self.child_ver1(child, task_type) + self_cost
            }
            PhysicalPlan::Projection(_) => {
                let child_cost = self.child_ver1(&plan.children()[0], task_type);
                // getCost4PhysicalProjection
                let count = card(plan);
                let mut cpu_cost = count * f.cpu;
                let concurrency = s.projection_concurrency;
                if concurrency <= 0.0 {
                    return child_cost + cpu_cost;
                }
                cpu_cost /= concurrency;
                child_cost + cpu_cost + (1.0 + concurrency) * f.concurrency
            }
            PhysicalPlan::IndexLookUpReader(reader) => {
                let (Some(index_plan), Some(table_plan)) = (reader.index_plan.as_deref(), reader.table_plan.as_deref()) else {
                    return 0.0;
                };
                let mut cost = self.child_ver1(index_plan, TaskType::CopMultiRead)
                    + self.child_ver1(table_plan, TaskType::CopMultiRead);
                // to keep compatible with the previous cost implementation,
                // re-calculate table-scan cost by using index stats-count again.
                let mut ts = table_plan;
                while let Some(child) = ts.children().first() {
                    ts = child;
                }
                if let PhysicalPlan::TableScan(scan) = ts {
                    cost -= self.child_ver1(ts, TaskType::CopMultiRead);
                    cost += card(index_plan) * self.scan_row_size(ts) * f.scan(scan.is_temporary);
                }
                // index-side net I/O cost: rows * row-size * net-factor
                let net_factor = table_net_factor(table_plan, f);
                cost += card(index_plan) * index_plan.cop_avg_row_size(true, s.enable_chunk_rpc) * net_factor;
                // index-side net seek cost
                cost += estimate_net_seek_cost(index_plan, f);
                // table-side net I/O cost: rows * row-size * net-factor
                cost += card(table_plan) * table_plan.cop_avg_row_size(false, s.enable_chunk_rpc) * net_factor;
                // table-side seek cost
                cost += estimate_net_seek_cost(table_plan, f);
                // consider concurrency
                cost /= s.distsql_scan_concurrency;
                // lookup-cpu-cost in TiDB: getCost4PhysicalIndexLookUpReader
                let index_rows = card(index_plan);
                let mut idx_cst = index_rows * f.cpu;
                if s.enable_paging && reader.expected_cnt > 0 && reader.expected_cnt <= tidb_paging::THRESHOLD {
                    idx_cst = idx_cst.min(self.calc_paging_cost(index_plan, reader.expected_cnt, f));
                }
                cost += idx_cst;
                let num_tbl_workers = s.index_lookup_concurrency;
                cost += (num_tbl_workers + 1.0) * f.concurrency;
                let index_lookup_size = s.index_lookup_size;
                let batch_size = index_lookup_size.min(index_rows);
                if batch_size > 2.0 {
                    cost += (index_rows * batch_size.log2() * f.cpu) / num_tbl_workers;
                }
                let table_rows = card(table_plan);
                let selectivity = table_rows / index_rows;
                let batch_size = (index_lookup_size * selectivity).min(table_rows);
                if reader.keep_order && batch_size > 2.0 {
                    cost += (table_rows * batch_size.log2() * f.cpu) / num_tbl_workers;
                }
                cost
            }
            PhysicalPlan::IndexReader(reader) => {
                let Some(index_plan) = reader.index_plan.as_deref() else {
                    return 0.0;
                };
                let mut cost = self.child_ver1(index_plan, TaskType::CopSingleRead);
                cost += card(index_plan)
                    * index_plan.cop_avg_row_size(true, s.enable_chunk_rpc)
                    * table_net_factor(index_plan, f);
                cost += estimate_net_seek_cost(index_plan, f);
                cost / s.distsql_scan_concurrency
            }
            PhysicalPlan::TableReader(reader) => {
                let Some(table_plan) = reader.table_plan.as_deref() else {
                    return 0.0;
                };
                let net_factor = table_net_factor(table_plan, f);
                match reader.store_type {
                    StoreType::TiFlash => {
                        let is_mpp = matches!(table_plan, PhysicalPlan::ExchangeSender(_));
                        let (concurrency, row_size, seek_cost, child_cost) = if is_mpp {
                            (
                                f.cop_tiflash_concurrency,
                                collect_row_size_from_mpp_plan(table_plan, s.enable_chunk_rpc),
                                accumulate_net_seek_cost_4_mpp(table_plan, f),
                                self.child_ver1(table_plan, TaskType::Mpp),
                            )
                        } else {
                            (
                                s.distsql_scan_concurrency,
                                table_plan.cop_avg_row_size(false, s.enable_chunk_rpc),
                                estimate_net_seek_cost(table_plan, f),
                                self.child_ver1(table_plan, TaskType::CopSingleRead),
                            )
                        };
                        let mut cost = child_cost + card(table_plan) * row_size * net_factor + seek_cost;
                        cost /= concurrency;
                        // show the real cost in explain-statements
                        if is_mpp && s.mpp_enforced && !self.recalculates() {
                            cost /= 1_000_000_000.0;
                        }
                        cost
                    }
                    _ => {
                        let mut cost = self.child_ver1(table_plan, TaskType::CopSingleRead);
                        cost += card(table_plan) * table_plan.cop_avg_row_size(false, s.enable_chunk_rpc) * net_factor;
                        cost += estimate_net_seek_cost(table_plan, f);
                        cost / s.distsql_scan_concurrency
                    }
                }
            }
            PhysicalPlan::IndexMergeReader(reader) => {
                let mut cost = 0.0;
                if let Some(tbl_scan) = reader.table_plan.as_deref() {
                    cost += self.child_ver1(tbl_scan, TaskType::CopSingleRead);
                    cost += card(tbl_scan)
                        * tbl_scan.cop_avg_row_size(false, s.enable_chunk_rpc)
                        * table_net_factor(tbl_scan, f);
                }
                for partial in &reader.partial_plans_raw {
                    cost += self.child_ver1(partial, TaskType::CopSingleRead);
                    let mut leaf = partial;
                    while let Some(child) = leaf.children().first() {
                        leaf = child;
                    }
                    let is_idx_scan = matches!(leaf, PhysicalPlan::IndexScan(_));
                    cost += card(partial)
                        * partial.cop_avg_row_size(is_idx_scan, s.enable_chunk_rpc)
                        * table_net_factor(partial, f);
                }
                if reader.pushed_limit.is_some() {
                    cost *= 0.99;
                }
                cost / s.distsql_scan_concurrency
            }
            PhysicalPlan::TableScan(x) => {
                let desc_big = x.desc && x.prop.as_ref().is_some_and(|prop| prop.expected_cnt >= SMALL_SCAN_THRESHOLD);
                let scan_factor = if desc_big { f.desc_scan(x.is_temporary) } else { f.scan(x.is_temporary) };
                card(plan) * self.scan_row_size(plan) * scan_factor
            }
            PhysicalPlan::IndexScan(x) => {
                let desc_big = x.desc && x.prop.as_ref().is_some_and(|prop| prop.expected_cnt >= SMALL_SCAN_THRESHOLD);
                let scan_factor = if desc_big { f.desc_scan(x.is_temporary) } else { f.scan(x.is_temporary) };
                card(plan) * self.scan_row_size(plan) * scan_factor
            }
            PhysicalPlan::IndexJoin(_) | PhysicalPlan::IndexHashJoin(_) => {
                let join = plan.as_index_join().expect("an index-join operator");
                let outer = &plan.children()[1 - join.inner_child_idx];
                let inner = &plan.children()[join.inner_child_idx];
                let outer_cost = self.child_ver1(outer, task_type);
                let inner_cost = self.child_ver1(inner, task_type);
                let mut outer_cnt = card(outer);
                let inner_cnt = card(inner);
                let mut cpu_cost = 0.0;
                if join.left_conditions.len() + join.right_conditions.len() > 0 {
                    cpu_cost += f.cpu * outer_cnt;
                    outer_cnt *= SELECTION_FACTOR;
                }
                let mut inner_cpu_cost = f.cpu * outer_cnt;
                let batch_size = s.index_join_batch_size.min(outer_cnt);
                if batch_size > 2.0 {
                    inner_cpu_cost += outer_cnt * (batch_size.log2() + 1.0) * f.cpu;
                }
                inner_cpu_cost += outer_cnt * DISTINCT_FACTOR * f.cpu;
                let concurrency = s.index_lookup_join_concurrency;
                let mut num_pairs = outer_cnt * inner_cnt;
                if is_semi(join.join_type) {
                    num_pairs = if join.other_conditions.is_empty() { 0.0 } else { num_pairs * 0.5 };
                }
                if matches!(plan, PhysicalPlan::IndexHashJoin(_)) {
                    // getCost4PhysicalIndexHashJoin
                    cpu_cost += inner_cpu_cost / concurrency;
                    cpu_cost += outer_cnt * f.cpu / concurrency;
                    let probe_cost = if outer_cnt / batch_size >= concurrency {
                        (num_pairs - batch_size * inner_cnt * (concurrency - 1.0)) * f.cpu
                    } else {
                        batch_size * inner_cnt * f.cpu
                    };
                    cpu_cost += probe_cost;
                    cpu_cost += (concurrency + 1.0) * f.concurrency;
                } else {
                    // getCost4PhysicalIndexJoin
                    inner_cpu_cost += outer_cnt * DISTINCT_FACTOR * inner_cnt * f.cpu;
                    cpu_cost += inner_cpu_cost / concurrency;
                    cpu_cost += num_pairs * f.cpu + (concurrency + 1.0) * f.concurrency;
                }
                let memory_cost = concurrency * (batch_size * DISTINCT_FACTOR) * inner_cnt * f.memory;
                outer_cost + outer_cnt * inner_cost + cpu_cost + memory_cost
            }
            PhysicalPlan::Apply(apply) => {
                let join = &apply.hash_join;
                let outer = &plan.children()[1 - join.inner_child_idx];
                let inner = &plan.children()[join.inner_child_idx];
                let l_cost = self.child_ver1(outer, task_type);
                let r_cost = self.child_ver1(inner, task_type);
                let mut l_count = card(outer);
                let mut r_count = card(inner);
                let mut cpu_cost = 0.0;
                if !join.left_conditions.is_empty() {
                    cpu_cost += l_count * f.cpu;
                    l_count *= SELECTION_FACTOR;
                }
                if !join.right_conditions.is_empty() {
                    cpu_cost += l_count * r_count * f.cpu;
                    r_count *= SELECTION_FACTOR;
                }
                if join.equal_conditions.len() + join.other_conditions.len() + join.na_equal_conditions.len() > 0 {
                    if is_semi(join.join_type) {
                        cpu_cost += l_count * r_count * f.cpu * 0.5;
                    } else {
                        cpu_cost += l_count * r_count * f.cpu;
                    }
                }
                cpu_cost + l_cost + l_count * r_cost
            }
            PhysicalPlan::MergeJoin(join) => {
                let [left, right] = plan.children() else {
                    return 0.0;
                };
                let children_cost = self.child_ver1(left, task_type) + self.child_ver1(right, task_type);
                let (l_cnt, r_cnt) = (card(left), card(right));
                let (outer_cnt, inner_cnt, inner_keys, inner) = if join.join_type == LogicalJoinType::RightOuter {
                    (r_cnt, l_cnt, &join.left_join_keys, left)
                } else {
                    (l_cnt, r_cnt, &join.right_join_keys, right)
                };
                let mut num_pairs = self.full_join_row_count(left, right, &join.left_join_keys, &join.right_join_keys);
                if is_semi(join.join_type) {
                    num_pairs = if join.other_conditions.is_empty() { 0.0 } else { num_pairs * 0.5 };
                }
                let mut probe_cost = num_pairs * f.cpu;
                let mut cpu_cost = 0.0;
                if join.left_conditions.len() + join.right_conditions.len() > 0 {
                    probe_cost *= SELECTION_FACTOR;
                    cpu_cost += outer_cnt * f.cpu;
                }
                cpu_cost += probe_cost;
                let ndv = inner.stats_info().map_or(1.0, |stats| {
                    let ids: Vec<i64> = inner_keys.iter().map(|key| key.unique_id).collect();
                    tidb_planner_cardinality::derive_stats::estimate_cols_ndv_with_matched_len_and_skew_ratio(
                        &ids,
                        stats,
                        s.group_ndv_skew_ratio,
                    )
                    .0
                });
                children_cost + cpu_cost + (inner_cnt / ndv) * f.memory
            }
            PhysicalPlan::HashJoin(join) => {
                let [left, right] = plan.children() else {
                    return 0.0;
                };
                let children_cost = self.child_ver1(left, task_type) + self.child_ver1(right, task_type);
                let (l_cnt, r_cnt) = (card(left), card(right));
                let (build_cnt, probe_cnt, build) = if (join.inner_child_idx == 1 && !join.use_outer_to_build)
                    || (join.inner_child_idx == 0 && join.use_outer_to_build)
                {
                    (r_cnt, l_cnt, right)
                } else {
                    (l_cnt, r_cnt, left)
                };
                let mem_quota = s.mem_quota;
                let row_size = Self::row_size(build);
                let spill = s.enable_tmp_storage_on_oom
                    && mem_quota > 0
                    && row_size * build_cnt > mem_quota as f64
                    && join.store_tp != StoreType::TiFlash;
                let mut cpu_cost = build_cnt * f.cpu;
                let mut memory_cost = build_cnt * f.memory;
                let mut disk_cost = build_cnt * f.disk * row_size;
                let mut num_pairs = self.full_join_row_count(left, right, &join.left_join_keys, &join.right_join_keys);
                if is_semi(join.join_type) {
                    num_pairs = if join.other_conditions.is_empty() { 0.0 } else { num_pairs * 0.5 };
                }
                let mut probe_cost = num_pairs * f.cpu;
                let mut probe_disk_cost = num_pairs * f.disk * row_size;
                if join.left_conditions.len() + join.right_conditions.len() > 0 {
                    probe_cost *= SELECTION_FACTOR;
                    probe_disk_cost *= SELECTION_FACTOR;
                    probe_cost += probe_cnt * f.cpu;
                }
                disk_cost += probe_disk_cost;
                let concurrency = join.concurrency as f64;
                probe_cost /= concurrency;
                cpu_cost += probe_cost + (concurrency + 1.0) * f.concurrency;
                if join.use_outer_to_build {
                    if spill {
                        cpu_cost += build_cnt * f.cpu;
                    } else {
                        cpu_cost += build_cnt * f.cpu / concurrency;
                    }
                    disk_cost += build_cnt * f.disk * row_size;
                }
                if spill {
                    memory_cost *= mem_quota as f64 / (row_size * build_cnt);
                } else {
                    disk_cost = 0.0;
                }
                children_cost + cpu_cost + memory_cost + disk_cost
            }
            PhysicalPlan::StreamAgg(agg) => {
                let child = &plan.children()[0];
                let input_rows = card(child);
                let agg_func_factor = agg_func_cost_factor(&agg.agg_funcs, false);
                let cpu_cost = if task_type == TaskType::Root {
                    input_rows * f.cpu * agg_func_factor
                } else {
                    input_rows * f.cop_cpu * agg_func_factor
                };
                let rows_per_group = input_rows / card(plan);
                let num_distinct = agg.agg_funcs.iter().filter(|func| func.has_distinct).count() as f64;
                let memory_cost = rows_per_group * DISTINCT_FACTOR * f.memory * num_distinct;
                self.child_ver1(child, task_type) + cpu_cost + memory_cost
            }
            PhysicalPlan::HashAgg(agg) => {
                let child = &plan.children()[0];
                let input_rows = card(child);
                let (is_root, is_mpp) = match task_type {
                    TaskType::Root => (true, false),
                    TaskType::Mpp => (false, true),
                    _ => (false, false),
                };
                let cardinality = card(plan);
                let num_distinct = agg.agg_funcs.iter().filter(|func| func.has_distinct).count();
                let agg_func_factor = agg_func_cost_factor(&agg.agg_funcs, is_mpp);
                let mut cpu_cost;
                if is_root {
                    cpu_cost = input_rows * f.cpu * agg_func_factor;
                    // CPUCostDivisor
                    let (final_con, partial_con) = (s.hashagg_final_concurrency, s.hashagg_partial_concurrency);
                    if num_distinct == 0 && !(final_con == 1.0 && partial_con == 1.0) {
                        let divisor = final_con.min(partial_con);
                        let con = final_con + partial_con;
                        if divisor > 0.0 {
                            cpu_cost /= divisor;
                            cpu_cost += (con + 1.0) * f.concurrency;
                        }
                    }
                } else {
                    cpu_cost = input_rows * f.cop_cpu * agg_func_factor;
                }
                let mut memory_cost = cardinality * f.memory * agg.agg_funcs.len() as f64;
                memory_cost += input_rows * DISTINCT_FACTOR * f.memory * num_distinct as f64;
                self.child_ver1(child, task_type) + cpu_cost + memory_cost
            }
            PhysicalPlan::Sort(_) => {
                let child = &plan.children()[0];
                let mut count = card(child);
                if count < 2.0 {
                    count = 2.0;
                }
                let cpu_cost = count * count.log2() * f.cpu;
                let mut memory_cost = count * f.memory;
                let mem_quota = s.mem_quota;
                let row_size = Self::row_size(plan);
                let spill = s.enable_tmp_storage_on_oom && mem_quota > 0 && row_size * count > mem_quota as f64;
                let mut disk_cost = count * f.disk * row_size;
                if spill {
                    memory_cost *= mem_quota as f64 / (row_size * count);
                } else {
                    disk_cost = 0.0;
                }
                self.child_ver1(child, task_type) + cpu_cost + memory_cost + disk_cost
            }
            PhysicalPlan::TopN(topn) => {
                let child = &plan.children()[0];
                let count = card(child);
                let heap_size = ((topn.offset + topn.count) as f64).max(2.0);
                let cpu_cost = if task_type == TaskType::Root {
                    count * heap_size.log2() * f.cpu
                } else {
                    count * heap_size.log2() * f.cop_cpu
                };
                self.child_ver1(child, task_type) + cpu_cost + heap_size * f.memory
            }
            PhysicalPlan::PointGet(point) => {
                // the cost of a PointGet generated in fast plan optimization is always 0
                if point.access_cols.is_none() {
                    return 0.0;
                }
                let temp = plan_is_temporary(plan);
                let row_size = plan.point_avg_row_size(s.enable_chunk_rpc);
                (row_size * f.network(temp) + f.seek(temp)) / s.distsql_scan_concurrency
            }
            PhysicalPlan::BatchPointGet(point) => {
                if point.access_cols.is_none() {
                    return 0.0;
                }
                let temp = plan_is_temporary(plan);
                let row_count = point.ranges.len() as f64;
                let row_size = plan.point_avg_row_size(s.enable_chunk_rpc);
                (row_count * row_size * f.network(temp) + row_count * f.seek(temp)) / s.distsql_scan_concurrency
            }
            PhysicalPlan::UnionAll(union) => {
                let child_max_cost = plan
                    .children()
                    .iter()
                    .map(|child| self.child_ver1(child, task_type))
                    .fold(0.0_f64, f64::max);
                let mut cost = child_max_cost + (1 + plan.children().len()) as f64 * f.concurrency;
                // show the real cost in explain-statements
                if union.mpp && s.mpp_enforced && !self.recalculates() {
                    cost /= 1_000_000_000.0;
                }
                cost
            }
            PhysicalPlan::ExchangeReceiver(_) => {
                let child = &plan.children()[0];
                self.child_ver1(child, task_type) + card(child) * f.network
            }
            // Go `BasePhysicalPlan.GetPlanCostVer1`: the operator costs nothing,
            // the plan is the sum of its children.
            _ => plan.children().iter().map(|child| self.child_ver1(child, task_type)).sum(),
        }
    }

    /// Go `cardinality.EstimateFullJoinRowCount(sctx, false, ...)` over two
    /// join children and their keys (no null-aware keys).
    fn full_join_row_count(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        left_keys: &[tidb_expr::column::Column],
        right_keys: &[tidb_expr::column::Column],
    ) -> f64 {
        let (Some(left_stats), Some(right_stats)) = (left.stats_info(), right.stats_info()) else {
            return card(left) * card(right);
        };
        let skew = self.session.group_ndv_skew_ratio;
        let estimate = tidb_planner_cardinality::join::join_key_estimate;
        tidb_planner_cardinality::join::estimate_full_join_row_count(
            &tidb_planner_cardinality::join::FullJoinRowCountInput {
                left_row_count: left_stats.row_count(),
                right_row_count: right_stats.row_count(),
                is_cartesian: false,
                left_join_keys: estimate(left_keys, left_stats, skew),
                right_join_keys: estimate(right_keys, right_stats, skew),
                left_non_equi_keys: estimate(&[], left_stats, skew),
                right_non_equi_keys: estimate(&[], right_stats, skew),
                join_reorder_threshold: self.session.join_reorder_threshold,
            },
        )
    }
}

/// Go `BasePhysicalAgg.GetAggFuncCostFactor`.
fn agg_func_cost_factor(funcs: &[tidb_expression_aggregation::AggFuncDesc], is_mpp: bool) -> f64 {
    let mut factor: f64 = funcs
        .iter()
        .map(|agg| tidb_planner_core_cost::aggregation_factor(&agg.base.name).unwrap_or_else(|| {
            tidb_planner_core_cost::aggregation_factor("default").expect("the default factor")
        }))
        .sum();
    if factor == 0.0 {
        factor = if is_mpp {
            tidb_planner_core_cost::aggregation_factor(tidb_expression_aggregation::names::FIRST_ROW)
                .expect("firstrow has a factor")
        } else {
            1.0
        };
    }
    factor
}

/// Go `collectRowSizeFromMPPPlan`.
fn collect_row_size_from_mpp_plan(plan: &PhysicalPlan, enable_chunk_rpc: bool) -> f64 {
    let Some(hist) = plan.stats_info().and_then(tidb_planner_property::stats_info::StatsInfo::hist_coll) else {
        return 1.0; // use 1 as lower-bound for safety
    };
    let columns: Vec<_> = plan
        .schema()
        .into_iter()
        .flat_map(|schema| &schema.columns)
        .filter(|column| column.id != crate::plan_builder::EXTRA_COMMIT_TS_ID)
        .map(|column| tidb_planner_cardinality::row_size::RowSizeColumn {
            stats: hist.column(column.unique_id),
            estimated_width: 0.0,
        })
        .collect();
    tidb_planner_cardinality::row_size::get_avg_row_size(
        &columns,
        hist.pseudo(),
        hist.realtime_count(),
        false,
        false,
        enable_chunk_rpc,
    )
}

/// Go `accumulateNetSeekCost4MPP`.
fn accumulate_net_seek_cost_4_mpp(plan: &PhysicalPlan, f: &Ver1Factors) -> f64 {
    match plan {
        PhysicalPlan::TableScan(x) => x.ranges.len() as f64 * x.columns.len() as f64 * f.seek(x.is_temporary),
        _ => plan.children().iter().map(|child| accumulate_net_seek_cost_4_mpp(child, f)).sum(),
    }
}
