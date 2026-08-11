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

//! Cost model version 2 -- TiDB's DEFAULT cost model -- from
//! `pkg/planner/core/plan_cost_ver2.go`, plus `compareTaskCost` /
//! `getTaskPlanCost` from `pkg/planner/core/find_best_task.go`.
//!
//! # What this owns, and what it deliberately does not
//!
//! Go reads every cost input off a physical plan node: `getCardinality(p)`,
//! `getAvgRowSize(p.StatsInfo(), p.Schema().Columns)`, the session variables,
//! and the child plans it recurses into. This tier has no physical-plan IR,
//! so the SAME arithmetic is expressed over explicit source-shaped inputs:
//! every function takes the rows, row sizes, factors and already-computed
//! child costs that Go would have read, and returns the same
//! [`CostVer2`](crate::cost_usage::CostVer2). Nothing about the formulas,
//! the factor values, the min/max clamps or the division order is changed.
//!
//! The row inputs have owners already: row counts come from
//! [`crate::cardinality::row_count_estimator`] and row sizes from
//! [`crate::cardinality::row_size`] -- this module never re-derives either.
//!
//! # Relationship to the other cost code in this workspace
//!
//! * [`crate::cost_usage`] is Go's `pkg/planner/util/costusage` -- the
//!   cost/trace arithmetic this module is written on top of. It was ported
//!   but never declared in `lib.rs`; this module is its first caller.
//! * `tidb-executor`'s `access_cost` is Go's cost model version ONE
//!   (`plan_cost_ver1.go` factors and the skyline/penalty rules the current
//!   single-table choosers use). It is a different Go source file modelling a
//!   different, non-default cost model -- not a fork of this one.
//! * [`crate::implementation_cost`] is the cascades `Implementation` cost
//!   accumulator (`pkg/planner/implementation/base.go`), unrelated arithmetic.
//!
//! # Named residue
//!
//! Session cost-factor variables (`tidb_opt_*_cost_factor`) and the
//! concurrency variables are not owned by `tidb-session` yet, so
//! [`CostSessionOpts`] carries the Go defaults from
//! `pkg/sessionctx/vardef/tidb_vars.go`. Callers that later gain real session
//! variables set the struct instead of changing a formula.

use crate::cardinality::row_size::{get_avg_row_size_data_in_disk_by_rows, RowSizeColumn};
use crate::cost_usage::{
    add_cost_without_trace, div_cost_ver2, mul_cost_ver2, new_cost_ver2, new_zero_cost_ver2,
    sum_cost_ver2, trace_cost, zero_cost_ver2, CostVer2, CostVer2Factor, PlanCostOption,
};
use crate::physical_table_reader::StoreType;
use crate::task_type::TaskType;
pub use tidb_util::paging::THRESHOLD as PAGING_THRESHOLD;

/// Minimum row count used to avoid underestimation (`MinNumRows`).
pub const MIN_NUM_ROWS: f64 = 1.0;
/// Minimum column length used by costing (`MinRowSize`).
pub const MIN_ROW_SIZE: f64 = 2.0;
/// Startup row penalty that steers small scans away from TiFlash.
pub const TIFLASH_STARTUP_ROW_PENALTY: f64 = 10000.0;
/// Row count added as a penalty to a high-risk full table scan.
pub const MAX_PENALTY_ROW_COUNT: f64 = 1000.0;
/// Go's `getAvgRowSize` from `pkg/planner/core/task.go` -- the ONE row-size
/// entry point cost model ver2 uses for a plan's own schema.
///
/// `hist_coll` is `Some((pseudo, realtime_count))` when the operator's
/// `StatsInfo().HistColl` exists. When it does NOT -- an aggregate or a
/// projection whose output is an expression -- Go falls back to the static
/// type width alone, WITHOUT the eight-byte-per-column record overhead that
/// `GetAvgRowSizeDataInDiskByRows` adds.
#[must_use]
pub fn plan_avg_row_size(columns: &[RowSizeColumn], hist_coll: Option<(bool, i64)>) -> f64 {
    match hist_coll {
        Some((pseudo, realtime_count)) => {
            get_avg_row_size_data_in_disk_by_rows(columns, pseudo, realtime_count).max(0.0)
        }
        None => columns
            .iter()
            .map(|column| column.estimated_width.max(0.0))
            .sum(),
    }
}

/// Go's `getCardinality` for cost model ver2: a non-positive estimate becomes
/// one row, because a zero-cost operator makes plan choice unstable.
#[must_use]
pub fn cardinality(stats_count: f64) -> f64 {
    if stats_count <= 0.0 {
        1.0
    } else {
        stats_count
    }
}

/// The `costVer2Factors` table. Values are `defaultVer2Factors`.
#[derive(Clone, Debug, PartialEq)]
pub struct Ver2Factors {
    /// Operations on a TiDB temporary table.
    pub tidb_temp: CostVer2Factor,
    /// TiKV ascending scan, per byte.
    pub tikv_scan: CostVer2Factor,
    /// TiKV descending scan, per byte.
    pub tikv_desc_scan: CostVer2Factor,
    /// TiFlash scan, per byte.
    pub tiflash_scan: CostVer2Factor,
    /// TiDB CPU, per column or expression.
    pub tidb_cpu: CostVer2Factor,
    /// TiKV CPU, per column or expression.
    pub tikv_cpu: CostVer2Factor,
    /// TiFlash CPU, per column or expression.
    pub tiflash_cpu: CostVer2Factor,
    /// TiDB-to-TiKV network, per byte.
    pub tidb_to_kv_net: CostVer2Factor,
    /// TiDB-to-TiFlash network, per byte.
    pub tidb_to_flash_net: CostVer2Factor,
    /// TiFlash MPP network, per byte.
    pub tiflash_mpp_net: CostVer2Factor,
    /// TiDB memory, per byte.
    pub tidb_mem: CostVer2Factor,
    /// TiKV memory, per byte.
    pub tikv_mem: CostVer2Factor,
    /// TiFlash memory, per byte.
    pub tiflash_mem: CostVer2Factor,
    /// TiDB disk, per byte.
    pub tidb_disk: CostVer2Factor,
    /// TiDB request, per network request.
    pub tidb_request: CostVer2Factor,
    /// ANN index warm-up cost, related to row count.
    pub ann_index_start: CostVer2Factor,
    /// ANN index scan cost, by row.
    pub ann_index_scan_row: CostVer2Factor,
    /// ANN index without top-k: the source uses `math.MaxUint64`.
    pub ann_index_no_topk: CostVer2Factor,
    /// Inverted index search cost, related to row count.
    pub inverted_index_search: CostVer2Factor,
    /// Inverted index scan penalty, related to row count.
    pub inverted_index_scan: CostVer2Factor,
    /// Late-materialization rest-column scan penalty.
    pub late_materialization_scan: CostVer2Factor,
}

impl Default for Ver2Factors {
    fn default() -> Self {
        Self {
            tidb_temp: CostVer2Factor::new("tidb_temp_table_factor", 0.00),
            tikv_scan: CostVer2Factor::new("tikv_scan_factor", 40.70),
            tikv_desc_scan: CostVer2Factor::new("tikv_desc_scan_factor", 61.05),
            tiflash_scan: CostVer2Factor::new("tiflash_scan_factor", 11.60),
            tidb_cpu: CostVer2Factor::new("tidb_cpu_factor", 49.90),
            tikv_cpu: CostVer2Factor::new("tikv_cpu_factor", 49.90),
            tiflash_cpu: CostVer2Factor::new("tiflash_cpu_factor", 2.40),
            tidb_to_kv_net: CostVer2Factor::new("tidb_kv_net_factor", 3.96),
            tidb_to_flash_net: CostVer2Factor::new("tidb_flash_net_factor", 2.20),
            tiflash_mpp_net: CostVer2Factor::new("tiflash_mpp_net_factor", 1.00),
            tidb_mem: CostVer2Factor::new("tidb_mem_factor", 0.20),
            tikv_mem: CostVer2Factor::new("tikv_mem_factor", 0.20),
            tiflash_mem: CostVer2Factor::new("tiflash_mem_factor", 0.05),
            tidb_disk: CostVer2Factor::new("tidb_disk_factor", 200.00),
            tidb_request: CostVer2Factor::new("tidb_request_factor", 6000000.00),
            ann_index_start: CostVer2Factor::new("ann_index_start_factor", 0.000144),
            ann_index_scan_row: CostVer2Factor::new("ann_index_scan_factor", 1.65),
            ann_index_no_topk: CostVer2Factor::new("ann_index_no_topk_factor", u64::MAX as f64),
            inverted_index_search: CostVer2Factor::new("inverted_index_search_factor", 139.2),
            inverted_index_scan: CostVer2Factor::new("inverted_index_scan_factor", 1.5),
            late_materialization_scan: CostVer2Factor::new("lm_scan_factor", 1.5),
        }
    }
}

/// Which network hop a `netCostVer2` caller sits on, replacing Go's
/// `getTaskNetFactorVer2` plan-type switch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetOwner {
    /// A `PhysicalExchangeReceiver`: TiFlash MPP traffic.
    TiFlashMpp,
    /// A `PhysicalTableReader` whose table plan is an exchange sender.
    TiDbToTiFlash,
    /// Everything else: TiDB reading from TiKV.
    TiDbToTiKv,
}

impl Ver2Factors {
    /// `getTaskCPUFactorVer2`.
    #[must_use]
    pub const fn task_cpu(&self, task_type: TaskType) -> &CostVer2Factor {
        match task_type {
            TaskType::Root => &self.tidb_cpu,
            TaskType::Mpp => &self.tiflash_cpu,
            _ => &self.tikv_cpu,
        }
    }

    /// `getTaskMemFactorVer2`.
    #[must_use]
    pub const fn task_mem(&self, task_type: TaskType) -> &CostVer2Factor {
        match task_type {
            TaskType::Root => &self.tidb_mem,
            TaskType::Mpp => &self.tiflash_mem,
            _ => &self.tikv_mem,
        }
    }

    /// `getTaskScanFactorVer2`. `desc` is the scan's own `Desc` flag, which
    /// only a table or index scan carries.
    #[must_use]
    pub const fn task_scan(
        &self,
        is_temporary_table: bool,
        store_type: StoreType,
        task_type: TaskType,
        desc: bool,
    ) -> &CostVer2Factor {
        if is_temporary_table {
            return &self.tidb_temp;
        }
        if matches!(store_type, StoreType::TiFlash) {
            return &self.tiflash_scan;
        }
        match task_type {
            TaskType::Mpp => &self.tiflash_scan,
            _ => {
                if desc {
                    &self.tikv_desc_scan
                } else {
                    &self.tikv_scan
                }
            }
        }
    }

    /// `getTaskNetFactorVer2`.
    #[must_use]
    pub const fn task_net(&self, is_temporary_table: bool, owner: NetOwner) -> &CostVer2Factor {
        if is_temporary_table {
            return &self.tidb_temp;
        }
        match owner {
            NetOwner::TiFlashMpp => &self.tiflash_mpp_net,
            NetOwner::TiDbToTiFlash => &self.tidb_to_flash_net,
            NetOwner::TiDbToTiKv => &self.tidb_to_kv_net,
        }
    }

    /// `getTaskRequestFactorVer2`.
    #[must_use]
    pub const fn task_request(&self, is_temporary_table: bool) -> &CostVer2Factor {
        if is_temporary_table {
            &self.tidb_temp
        } else {
            &self.tidb_request
        }
    }
}

/// The session state the ver2 cost functions read, with the Go defaults from
/// `pkg/sessionctx/vardef/tidb_vars.go`.
///
/// `ConcurrencyUnset` variables (`tidb_index_lookup_concurrency`,
/// `tidb_index_lookup_join_concurrency`, `tidb_projection_concurrency`,
/// `tidb_hashagg_final_concurrency`, union concurrency) resolve to
/// `tidb_executor_concurrency`, whose default is 5; that resolution is
/// already applied here.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostSessionOpts {
    /// `tidb_distsql_scan_concurrency`.
    pub distsql_scan_concurrency: f64,
    /// `tidb_index_lookup_concurrency`, resolved.
    pub index_lookup_concurrency: f64,
    /// `tidb_index_lookup_join_concurrency`, resolved.
    pub index_lookup_join_concurrency: f64,
    /// `tidb_projection_concurrency`, resolved.
    pub projection_concurrency: f64,
    /// `tidb_hashagg_final_concurrency`, resolved.
    pub hashagg_final_concurrency: f64,
    /// Union-all concurrency, resolved.
    pub union_concurrency: f64,
    /// `tidb_index_lookup_size`.
    pub index_lookup_size: f64,
    /// `tidb_index_join_batch_size`.
    pub index_join_batch_size: f64,
    /// `tidb_index_join_double_read_penalty_cost_rate`.
    pub index_join_double_read_penalty_cost_rate: f64,
    /// `tidb_enable_tmp_storage_on_oom`.
    pub enable_tmp_storage_on_oom: bool,
    /// The statement memory quota; `<= 0` disables sort spilling.
    pub mem_quota: i64,
    /// `tidb_enable_paging`.
    pub enable_paging: bool,
    /// Whether `tidb_enforce_mpp` is on.
    pub mpp_enforced: bool,
}

impl Default for CostSessionOpts {
    fn default() -> Self {
        Self {
            distsql_scan_concurrency: 15.0,
            index_lookup_concurrency: 5.0,
            index_lookup_join_concurrency: 5.0,
            projection_concurrency: 5.0,
            hashagg_final_concurrency: 5.0,
            union_concurrency: 5.0,
            index_lookup_size: 20000.0,
            index_join_batch_size: 25000.0,
            index_join_double_read_penalty_cost_rate: 0.0,
            enable_tmp_storage_on_oom: true,
            mem_quota: 0,
            enable_paging: true,
            mpp_enforced: false,
        }
    }
}

/// The per-operator `tidb_opt_*_cost_factor` multipliers, all defaulting to
/// `1.0`. Each field is applied at exactly the point its Go operator applies
/// it, after the operator's own cost is complete.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostFactorVars {
    /// `tidb_opt_index_scan_cost_factor`.
    pub index_scan: f64,
    /// `tidb_opt_table_row_id_scan_cost_factor`.
    pub table_row_id_scan: f64,
    /// `tidb_opt_table_range_scan_cost_factor`.
    pub table_range_scan: f64,
    /// `tidb_opt_table_full_scan_cost_factor`.
    pub table_full_scan: f64,
    /// `tidb_opt_table_tiflash_scan_cost_factor`.
    pub table_tiflash_scan: f64,
    /// `tidb_opt_index_reader_cost_factor`.
    pub index_reader: f64,
    /// `tidb_opt_table_reader_cost_factor`.
    pub table_reader: f64,
    /// `tidb_opt_index_lookup_cost_factor`.
    pub index_lookup: f64,
    /// `tidb_opt_index_merge_cost_factor`.
    pub index_merge: f64,
    /// `tidb_opt_limit_cost_factor`.
    pub limit: f64,
    /// `tidb_opt_sort_cost_factor`.
    pub sort: f64,
    /// `tidb_opt_topn_cost_factor`.
    pub topn: f64,
    /// `tidb_opt_stream_agg_cost_factor`.
    pub stream_agg: f64,
    /// `tidb_opt_hash_agg_cost_factor`.
    pub hash_agg: f64,
    /// `tidb_opt_merge_join_cost_factor`.
    pub merge_join: f64,
    /// `tidb_opt_hash_join_cost_factor`.
    pub hash_join: f64,
    /// `tidb_opt_index_join_cost_factor`.
    pub index_join: f64,
}

impl Default for CostFactorVars {
    fn default() -> Self {
        Self {
            index_scan: 1.0,
            table_row_id_scan: 1.0,
            table_range_scan: 1.0,
            table_full_scan: 1.0,
            table_tiflash_scan: 1.0,
            index_reader: 1.0,
            table_reader: 1.0,
            index_lookup: 1.0,
            index_merge: 1.0,
            limit: 1.0,
            sort: 1.0,
            topn: 1.0,
            stream_agg: 1.0,
            hash_agg: 1.0,
            merge_join: 1.0,
            hash_join: 1.0,
            index_join: 1.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Primitive cost expressions
// ---------------------------------------------------------------------------

/// `scanCostVer2`: `rows * max(log2(row-size), 0) * scan-factor`, with a
/// row size below one byte raised to one byte first.
#[must_use]
pub fn scan_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    scan_factor: &CostVer2Factor,
) -> CostVer2 {
    let row_size = if row_size < 1.0 { 1.0 } else { row_size };
    new_cost_ver2(
        option,
        scan_factor,
        rows * row_size.log2().max(0.0) * scan_factor.value(),
        || format!("scan({rows}*logrowsize({row_size})*{scan_factor})"),
    )
}

/// `netCostVer2`: `rows * row-size * net-factor`.
#[must_use]
pub fn net_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    net_factor: &CostVer2Factor,
) -> CostVer2 {
    new_cost_ver2(
        option,
        net_factor,
        rows * row_size * net_factor.value(),
        || format!("net({rows}*rowsize({row_size})*{net_factor})"),
    )
}

/// `numFunctions`: a scalar function counts one, a column or constant counts
/// the source's empirical `0.01`.
#[must_use]
pub fn num_functions(is_scalar_function: &[bool]) -> f64 {
    let mut num = 0.0;
    for scalar in is_scalar_function {
        if *scalar {
            num += 1.0;
        } else {
            num += 0.01;
        }
    }
    num
}

/// `filterCostVer2`: `rows * num-functions(filters) * cpu-factor`.
#[must_use]
pub fn filter_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    filter_is_scalar_function: &[bool],
    cpu_factor: &CostVer2Factor,
) -> CostVer2 {
    let num_funcs = num_functions(filter_is_scalar_function);
    new_cost_ver2(
        option,
        cpu_factor,
        rows * num_funcs * cpu_factor.value(),
        || format!("cpu({rows}*filters({num_funcs})*{cpu_factor})"),
    )
}

/// `aggCostVer2`: `rows * len(agg-funcs) * cpu-factor`.
#[must_use]
pub fn agg_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    num_agg_funcs: usize,
    cpu_factor: &CostVer2Factor,
) -> CostVer2 {
    new_cost_ver2(
        option,
        cpu_factor,
        rows * num_agg_funcs as f64 * cpu_factor.value(),
        || format!("agg({rows}*aggs({num_agg_funcs})*{cpu_factor})"),
    )
}

/// `groupCostVer2`: `rows * num-functions(group-items) * cpu-factor`.
#[must_use]
pub fn group_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    group_item_is_scalar_function: &[bool],
    cpu_factor: &CostVer2Factor,
) -> CostVer2 {
    let num_funcs = num_functions(group_item_is_scalar_function);
    new_cost_ver2(
        option,
        cpu_factor,
        rows * num_funcs * cpu_factor.value(),
        || format!("group({rows}*cols({num_funcs})*{cpu_factor})"),
    )
}

/// `orderCostVer2`: the by-item expression CPU plus `rows * log2(n)` compares.
/// Unlike [`num_functions`], a non-function by-item costs NOTHING here.
#[must_use]
pub fn order_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    n: f64,
    by_item_is_scalar_function: &[bool],
    cpu_factor: &CostVer2Factor,
) -> CostVer2 {
    let num_funcs = by_item_is_scalar_function.iter().filter(|f| **f).count();
    let expr_cost = new_cost_ver2(
        option,
        cpu_factor,
        rows * num_funcs as f64 * cpu_factor.value(),
        || format!("exprCPU({rows}*{num_funcs}*{cpu_factor})"),
    );
    let order_cost = new_cost_ver2(
        option,
        cpu_factor,
        (rows * n.log2()).max(0.0) * cpu_factor.value(),
        || format!("orderCPU({rows}*log({n})*{cpu_factor})"),
    );
    sum_cost_ver2(&[expr_cost, order_cost])
}

/// `hashBuildCostVer2`: hash-key CPU, hash-table memory, and build CPU.
#[must_use]
pub fn hash_build_cost(
    option: Option<&PlanCostOption>,
    build_rows: f64,
    build_row_size: f64,
    num_keys: f64,
    cpu_factor: &CostVer2Factor,
    mem_factor: &CostVer2Factor,
) -> CostVer2 {
    let hash_key_cost = new_cost_ver2(
        option,
        cpu_factor,
        build_rows * num_keys * cpu_factor.value(),
        || format!("hashkey({build_rows}*{num_keys}*{cpu_factor})"),
    );
    let hash_mem_cost = new_cost_ver2(
        option,
        mem_factor,
        build_rows * build_row_size * mem_factor.value(),
        || format!("hashmem({build_rows}*{build_row_size}*{mem_factor})"),
    );
    let hash_build_cost =
        new_cost_ver2(option, cpu_factor, build_rows * cpu_factor.value(), || {
            format!("hashbuild({build_rows}*{cpu_factor})")
        });
    sum_cost_ver2(&[hash_key_cost, hash_mem_cost, hash_build_cost])
}

/// `hashProbeCostVer2`: hash-key CPU plus probe CPU.
#[must_use]
pub fn hash_probe_cost(
    option: Option<&PlanCostOption>,
    probe_rows: f64,
    num_keys: f64,
    cpu_factor: &CostVer2Factor,
) -> CostVer2 {
    let hash_key_cost = new_cost_ver2(
        option,
        cpu_factor,
        probe_rows * num_keys * cpu_factor.value(),
        || format!("hashkey({probe_rows}*{num_keys}*{cpu_factor})"),
    );
    let hash_probe_cost =
        new_cost_ver2(option, cpu_factor, probe_rows * cpu_factor.value(), || {
            format!("hashprobe({probe_rows}*{cpu_factor})")
        });
    sum_cost_ver2(&[hash_key_cost, hash_probe_cost])
}

/// `doubleReadCostVer2`: `num-tasks * request-factor`.
#[must_use]
pub fn double_read_cost(
    option: Option<&PlanCostOption>,
    num_tasks: f64,
    request_factor: &CostVer2Factor,
) -> CostVer2 {
    new_cost_ver2(
        option,
        request_factor,
        num_tasks * request_factor.value(),
        || format!("doubleRead(tasks({num_tasks})*{request_factor})"),
    )
}

/// `indexJoinSeekingCostVer2`: a seek is charged as a scan of ten 8-byte rows,
/// and only when both the build side and the range count exceed one.
#[must_use]
pub fn index_join_seeking_cost(
    option: Option<&PlanCostOption>,
    build_rows: f64,
    num_ranges: f64,
    scan_factor: &CostVer2Factor,
) -> CostVer2 {
    if build_rows <= 1.0 || num_ranges <= 1.0 {
        return zero_cost_ver2();
    }
    new_cost_ver2(
        option,
        scan_factor,
        build_rows * 10.0 * 8.0_f64.log2() * num_ranges * scan_factor.value(),
        || format!("seeking({build_rows}*{num_ranges}*10*log2(8)*{scan_factor})"),
    )
}

// ---------------------------------------------------------------------------
// Operator costs
// ---------------------------------------------------------------------------

/// `getPlanCostVer24PhysicalSelection`: child cost plus filter cost over the
/// CHILD's row count.
#[must_use]
pub fn selection_cost(
    option: Option<&PlanCostOption>,
    input_rows: f64,
    condition_is_scalar_function: &[bool],
    cpu_factor: &CostVer2Factor,
    child_cost: &CostVer2,
) -> CostVer2 {
    let filter = filter_cost(option, input_rows, condition_is_scalar_function, cpu_factor);
    sum_cost_ver2(&[filter, child_cost.clone()])
}

/// `getPlanCostVer24PhysicalProjection`: child cost plus the projection's own
/// cost divided by projection concurrency (zero concurrency means serial).
#[must_use]
pub fn projection_cost(
    option: Option<&PlanCostOption>,
    input_rows: f64,
    expr_is_scalar_function: &[bool],
    cpu_factor: &CostVer2Factor,
    concurrency: f64,
    child_cost: &CostVer2,
) -> CostVer2 {
    let concurrency = if concurrency == 0.0 { 1.0 } else { concurrency };
    let proj = filter_cost(option, input_rows, expr_is_scalar_function, cpu_factor);
    sum_cost_ver2(&[child_cost.clone(), div_cost_ver2(&proj, concurrency)])
}

/// `getPlanCostVer24PhysicalIndexScan`. `index_id` supplies the source's
/// untraced tie-breaker `(index-id % 100) / 1e6`, which separates indexes that
/// would otherwise cost the same; `None` is Go's `p.Index == nil`.
#[must_use]
pub fn index_scan_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    scan_factor: &CostVer2Factor,
    index_scan_cost_factor: f64,
    index_id: Option<i64>,
) -> CostVer2 {
    let cost = scan_cost(option, rows, row_size, scan_factor);
    let cost = mul_cost_ver2(&cost, index_scan_cost_factor);
    match index_id {
        Some(id) => add_cost_without_trace(cost, (id % 100) as f64 / 1_000_000.0),
        None => cost,
    }
}

/// The inputs `getTableScanPenalty` reads off the scan and the session.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TableScanPenaltyInput {
    /// Whether the scan carries `RangeInfo` (an index-join probe range).
    pub has_range_info: bool,
    /// `tidb_opt_prefer_range_scan`.
    pub allow_prefer_range_scan: bool,
    /// Whether the table's histogram collection is pseudo.
    pub pseudo_stats: bool,
    /// `GetAnalyzeRowCount()` truncated to an integer, as Go does.
    pub analyze_row_count: i64,
    /// The table's modify count.
    pub modify_count: i64,
    /// Whether partition pruning conditions exist (a partition-level scan).
    pub has_partition_scan: bool,
    /// Whether the statement used `USE`/`FORCE INDEX`.
    pub has_index_force: bool,
}

/// `getTableScanPenalty`: the extra row count charged to a risky full scan.
#[must_use]
pub fn table_scan_penalty(input: TableScanPenaltyInput, rows: f64) -> f64 {
    if input.has_range_info {
        return 0.0;
    }
    let has_unreliable_stats = input.pseudo_stats || input.analyze_row_count < 1;
    let has_high_modify_count = input.modify_count > input.analyze_row_count;
    let has_low_estimate = rows > 1.0
        && input.modify_count < input.analyze_row_count
        && (rows as i64) <= input.modify_count;
    let prefer_range_scan_condition = input.allow_prefer_range_scan
        && (has_unreliable_stats || has_high_modify_count || has_low_estimate);
    if !(input.has_index_force || prefer_range_scan_condition) {
        return 0.0;
    }
    let min_rows = MAX_PENALTY_ROW_COUNT.max(rows);
    if input.has_partition_scan {
        return min_rows;
    }
    min_rows.max(input.modify_count as f64)
}

/// Everything `getPlanCostVer24PhysicalTableScan` reads for a TiKV scan.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TableScanInput {
    /// `getCardinality(p)`.
    pub rows: f64,
    /// `getAvgRowSize(p.StatsInfo(), p.TblCols)`, `_tidb_commit_ts` excluded.
    pub row_size: f64,
    /// Go's variadic `isChildOfINL`: `None` when the argument is absent.
    pub is_child_of_inl: Option<bool>,
    /// `ranger.HasFullRange(p.Ranges, unsignedIntHandle)`.
    pub has_full_range_scan: bool,
    /// The penalty inputs.
    pub penalty: TableScanPenaltyInput,
}

/// `getPlanCostVer24PhysicalTableScan` for a non-TiFlash store.
///
/// The row and row-size minimums, the full-range penalty and the choice of
/// row-id / range / full-scan cost factor all key off the same variadic
/// `isChildOfINL` signal Go threads down from `IndexLookUpReader`.
#[must_use]
pub fn table_scan_cost(
    option: Option<&PlanCostOption>,
    input: TableScanInput,
    scan_factor: &CostVer2Factor,
    factors: &CostFactorVars,
) -> CostVer2 {
    let is_row_id_scan = input.is_child_of_inl == Some(true);
    let (rows, row_size) = if is_row_id_scan {
        (input.rows, input.row_size)
    } else {
        (
            input.rows.max(MIN_NUM_ROWS),
            input.row_size.max(MIN_ROW_SIZE),
        )
    };

    let mut cost = scan_cost(option, rows, row_size, scan_factor);
    if !is_row_id_scan && input.has_full_range_scan {
        let penalty_rows = table_scan_penalty(input.penalty, rows);
        if penalty_rows > 0.0 {
            cost = sum_cost_ver2(&[cost, scan_cost(option, penalty_rows, row_size, scan_factor)]);
        }
    }
    let cost_factor = if is_row_id_scan {
        factors.table_row_id_scan
    } else if input.has_full_range_scan {
        factors.table_full_scan
    } else {
        factors.table_range_scan
    };
    mul_cost_ver2(&cost, cost_factor)
}

/// `getPlanCostVer24PhysicalTableScan` for TiFlash WITHOUT late
/// materialization or a columnar index: the scan plus a fixed startup penalty
/// of [`TIFLASH_STARTUP_ROW_PENALTY`] rows, which keeps small scans on TiKV.
#[must_use]
pub fn tiflash_table_scan_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    scan_factor: &CostVer2Factor,
    table_tiflash_scan_cost_factor: f64,
) -> CostVer2 {
    let rows = rows.max(MIN_NUM_ROWS);
    let row_size = row_size.max(MIN_ROW_SIZE);
    let cost = sum_cost_ver2(&[
        scan_cost(option, rows, row_size, scan_factor),
        scan_cost(option, TIFLASH_STARTUP_ROW_PENALTY, row_size, scan_factor),
    ]);
    mul_cost_ver2(&cost, table_tiflash_scan_cost_factor)
}

/// `getPlanCostVer24PhysicalTableScan`'s late-materialization branch: the
/// filter columns are scanned over the whole pre-filter row count, and the
/// remaining columns pay [`Ver2Factors::late_materialization_scan`] because
/// the surviving rows are discrete.
#[must_use]
pub fn tiflash_late_materialization_scan_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    lm_row_size: f64,
    lm_selectivity: f64,
    factors: &Ver2Factors,
) -> CostVer2 {
    let scan_factor = &factors.tiflash_scan;
    let lm_factor = &factors.late_materialization_scan;
    let total_row_count = rows / lm_selectivity + TIFLASH_STARTUP_ROW_PENALTY;
    let lm_scan = new_cost_ver2(
        option,
        scan_factor,
        total_row_count * lm_row_size.log2().max(0.0) * scan_factor.value(),
        || format!("lm_col_scan({total_row_count}*logrowsize({lm_row_size})*{scan_factor})"),
    );
    let rest_row_size = row_size - lm_row_size;
    let rest_scan = new_cost_ver2(
        option,
        scan_factor,
        rows * rest_row_size.log2().max(0.0) * scan_factor.value() * lm_factor.value(),
        || {
            format!(
                "lm_rest_col_scan({rows}*logrowsize({rest_row_size})*{scan_factor}*lm_scan_factor({}))",
                lm_factor.value()
            )
        },
    );
    sum_cost_ver2(&[lm_scan, rest_scan])
}

/// `getPlanCostVer24PhysicalIndexReader` and
/// `getPlanCostVer24PhysicalTableReader` share one formula:
/// `(child-cost + net-cost) / dist-concurrency`, then the reader's cost
/// factor. The table reader clamps the row size to [`MIN_ROW_SIZE`] first;
/// the index reader does not.
#[must_use]
pub fn reader_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    net_factor: &CostVer2Factor,
    concurrency: f64,
    child_cost: &CostVer2,
    reader_cost_factor: f64,
) -> CostVer2 {
    let net = net_cost(option, rows, row_size, net_factor);
    let cost = div_cost_ver2(&sum_cost_ver2(&[child_cost.clone(), net]), concurrency);
    mul_cost_ver2(&cost, reader_cost_factor)
}

/// The `tidb_enforce_mpp` discount a TiFlash `PhysicalTableReader` applies
/// before its cost factor, so an enforced MPP plan still compares through the
/// normal cost path.
pub const MPP_ENFORCED_DISCOUNT: f64 = 1_000_000_000.0;

/// Everything `getPlanCostVer24PhysicalIndexLookUpReader` reads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexLookUpInput {
    /// `getCardinality(p.IndexPlan)`, before the pushed-limit clamp.
    pub index_rows: f64,
    /// `getCardinality(p.TablePlan)`, before the pushed-limit clamp.
    pub table_rows: f64,
    /// `cardinality.GetAvgRowSize(.., isEncodedKey = true, isForScan = false)`
    /// over the index plan's schema.
    pub index_row_size: f64,
    /// `cardinality.GetAvgRowSize(.., isEncodedKey = false, isForScan = false)`
    /// over the table plan's schema.
    pub table_row_size: f64,
    /// `p.PushedLimit.Count`, when a limit was pushed into the lookup.
    pub pushed_limit: Option<u64>,
    /// `p.ExpectedCnt`, which decides the paging discount.
    pub expected_cnt: f64,
}

/// `getPlanCostVer24PhysicalIndexLookUpReader`.
///
/// `plan-cost = index-side + (table-side + double-read) / double-read-concurrency`,
/// where the double read charges one CPU per index row plus a request cost for
/// `index-rows / batch-size * 32` tasks.
#[must_use]
pub fn index_lookup_reader_cost(
    option: Option<&PlanCostOption>,
    input: IndexLookUpInput,
    costs: (&CostVer2, &CostVer2),
    factors: (&Ver2Factors, &CostFactorVars),
    session: &CostSessionOpts,
    task_type: TaskType,
) -> CostVer2 {
    let (index_child_cost, table_child_cost) = costs;
    let (ver2, cost_factors) = factors;
    let (mut index_rows, mut table_rows) = (input.index_rows, input.table_rows);
    if let Some(limit) = input.pushed_limit {
        index_rows = index_rows.min(limit as f64);
        table_rows = table_rows.min(limit as f64);
    }
    let cpu_factor = ver2.task_cpu(task_type);
    let net_factor = ver2.task_net(false, NetOwner::TiDbToTiKv);
    let request_factor = ver2.task_request(false);
    let dist_concurrency = session.distsql_scan_concurrency;
    let double_read_concurrency = session.index_lookup_concurrency;

    let index_net = net_cost(option, index_rows, input.index_row_size, net_factor);
    let index_side = div_cost_ver2(
        &sum_cost_ver2(&[index_net, index_child_cost.clone()]),
        dist_concurrency,
    );

    let table_net = net_cost(option, table_rows, input.table_row_size, net_factor);
    let table_side = div_cost_ver2(
        &sum_cost_ver2(&[table_net, table_child_cost.clone()]),
        dist_concurrency,
    );

    let double_read_rows = index_rows;
    let double_read_cpu =
        new_cost_ver2(option, cpu_factor, index_rows * cpu_factor.value(), || {
            format!("double-read-cpu({double_read_rows}*{cpu_factor})")
        });
    let task_per_batch = 32.0;
    let double_read_tasks = double_read_rows / session.index_lookup_size * task_per_batch;
    let double_read_request = double_read_cost(option, double_read_tasks, request_factor);
    let double_read = sum_cost_ver2(&[double_read_cpu, double_read_request]);

    let mut cost = sum_cost_ver2(&[
        index_side,
        div_cost_ver2(
            &sum_cost_ver2(&[table_side, double_read]),
            double_read_concurrency,
        ),
    ]);
    if session.enable_paging
        && input.expected_cnt > 0.0
        && input.expected_cnt <= PAGING_THRESHOLD as f64
    {
        cost = mul_cost_ver2(&cost, 0.6);
    }
    if let Some(limit) = input.pushed_limit {
        if table_rows <= limit as f64 {
            cost = mul_cost_ver2(&cost, cost_factors.limit);
        }
    }
    mul_cost_ver2(&cost, cost_factors.index_lookup)
}

/// One side of `GetPlanCostVer24PhysicalIndexMergeReader`.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexMergeSide {
    /// `getCardinality(path)`.
    pub rows: f64,
    /// `getAvgRowSize(path.StatsInfo(), path.Schema().Columns)`.
    pub row_size: f64,
    /// The path's own child cost.
    pub child_cost: CostVer2,
}

/// `GetPlanCostVer24PhysicalIndexMergeReader`: the table side plus every
/// partial index side, each `(child + net) / dist-concurrency`. A pushed-down
/// limit earns a `0.99` bias so it beats the identically-costed alternative
/// that keeps the limit outside.
#[must_use]
pub fn index_merge_reader_cost(
    option: Option<&PlanCostOption>,
    table_side: Option<&IndexMergeSide>,
    index_sides: &[IndexMergeSide],
    net_factor: &CostVer2Factor,
    args: (f64, bool, &CostFactorVars),
) -> CostVer2 {
    let (dist_concurrency, has_pushed_limit, cost_factors) = args;
    let side_cost = |side: &IndexMergeSide| {
        let net = net_cost(option, side.rows, side.row_size, net_factor);
        div_cost_ver2(
            &sum_cost_ver2(&[net, side.child_cost.clone()]),
            dist_concurrency,
        )
    };
    let table_cost = table_side.map_or_else(zero_cost_ver2, side_cost);
    let index_costs: Vec<CostVer2> = index_sides.iter().map(side_cost).collect();
    let mut cost = sum_cost_ver2(&[table_cost, sum_cost_ver2(&index_costs)]);
    if has_pushed_limit {
        cost = mul_cost_ver2(&cost, 0.99);
        cost = mul_cost_ver2(&cost, cost_factors.limit);
    }
    mul_cost_ver2(&cost, cost_factors.index_merge)
}

/// `getPlanCostVer24PhysicalSort`. Spilling replaces the in-memory row cost
/// with the memory quota and adds a disk cost; only a root task can spill.
#[must_use]
pub fn sort_cost(
    option: Option<&PlanCostOption>,
    rows_and_size: (f64, f64),
    by_item_is_scalar_function: &[bool],
    factors: (&Ver2Factors, &CostFactorVars),
    session: &CostSessionOpts,
    task_type: TaskType,
    child_cost: &CostVer2,
) -> CostVer2 {
    let (ver2, cost_factors) = factors;
    let rows = rows_and_size.0.max(MIN_NUM_ROWS);
    let row_size = rows_and_size.1.max(MIN_ROW_SIZE);
    let cpu_factor = ver2.task_cpu(task_type);
    let mem_factor = ver2.task_mem(task_type);
    let disk_factor = &ver2.tidb_disk;
    let mem_quota = session.mem_quota;
    let spill = matches!(task_type, TaskType::Root)
        && session.enable_tmp_storage_on_oom
        && mem_quota > 0
        && row_size * rows > mem_quota as f64;

    let sort_cpu = order_cost(option, rows, rows, by_item_is_scalar_function, cpu_factor);
    let (sort_mem, sort_disk) = if spill {
        (
            new_cost_ver2(
                option,
                mem_factor,
                mem_quota as f64 * mem_factor.value(),
                || format!("sortMem({mem_quota}*{mem_factor})"),
            ),
            new_cost_ver2(
                option,
                disk_factor,
                rows * row_size * disk_factor.value(),
                || format!("sortDisk({rows}*{row_size}*{disk_factor})"),
            ),
        )
    } else {
        (
            new_cost_ver2(
                option,
                mem_factor,
                rows * row_size * mem_factor.value(),
                || format!("sortMem({rows}*{row_size}*{mem_factor})"),
            ),
            zero_cost_ver2(),
        )
    };
    let cost = sum_cost_ver2(&[child_cost.clone(), sort_cpu, sort_mem, sort_disk]);
    mul_cost_ver2(&cost, cost_factors.sort)
}

/// `getPlanCostVer24PhysicalTopN`. `n` is `count + offset`, raised off small
/// values by the source's 100-row floor so an under-estimated child cannot
/// make the heap look free.
#[must_use]
pub fn top_n_cost(
    option: Option<&PlanCostOption>,
    child_rows: f64,
    count_and_offset: (u64, u64),
    row_size: f64,
    by_item_is_scalar_function: &[bool],
    factors: (&CostVer2Factor, &CostVer2Factor, f64),
    child_cost: &CostVer2,
) -> CostVer2 {
    let (cpu_factor, mem_factor, topn_cost_factor) = factors;
    let (count, offset) = count_and_offset;
    let rows = child_rows.max(MIN_NUM_ROWS);
    let mut n = MIN_NUM_ROWS.max((count + offset) as f64);
    let min_topn_threshold = 100.0;
    if n > min_topn_threshold {
        if rows < offset as f64 {
            n = rows + offset as f64;
        } else {
            n = n.min(rows).max(min_topn_threshold);
        }
    }
    let row_size = row_size.max(MIN_ROW_SIZE);

    let topn_cpu = order_cost(option, rows, n, by_item_is_scalar_function, cpu_factor);
    let topn_mem = new_cost_ver2(
        option,
        mem_factor,
        n * row_size * mem_factor.value(),
        || format!("topMem({n}*{row_size}*{mem_factor})"),
    );
    let cost = sum_cost_ver2(&[child_cost.clone(), topn_cpu, topn_mem]);
    mul_cost_ver2(&cost, topn_cost_factor)
}

/// `getPlanCostVer24PhysicalStreamAgg`: child cost plus per-row aggregate and
/// grouping CPU. A stream aggregate keeps no hash table, so it has no memory
/// term at all.
#[must_use]
pub fn stream_agg_cost(
    option: Option<&PlanCostOption>,
    input_rows: f64,
    num_agg_funcs: usize,
    group_item_is_scalar_function: &[bool],
    factors: (&CostVer2Factor, f64),
    child_cost: &CostVer2,
) -> CostVer2 {
    let (cpu_factor, stream_agg_cost_factor) = factors;
    let agg = agg_cost(option, input_rows, num_agg_funcs, cpu_factor);
    let group = group_cost(
        option,
        input_rows,
        group_item_is_scalar_function,
        cpu_factor,
    );
    let cost = sum_cost_ver2(&[child_cost.clone(), agg, group]);
    mul_cost_ver2(&cost, stream_agg_cost_factor)
}

/// Everything `getPlanCostVer24PhysicalHashAgg` reads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HashAggInput {
    /// `getCardinality(child)`.
    pub input_rows: f64,
    /// `getCardinality(p)` -- the aggregate's own NDV-shaped output.
    pub output_rows: f64,
    /// `getAvgRowSize(p.StatsInfo(), p.Schema().Columns)`.
    pub output_row_size: f64,
    /// `len(p.AggFuncs)`.
    pub num_agg_funcs: usize,
    /// Whether `childCanProvideOrderForStreamAgg(p.Children()[0])` holds, which
    /// is the only case a root hash aggregate pays the per-worker hash-table
    /// memory penalty.
    pub child_can_provide_order: bool,
}

/// `getPlanCostVer24PhysicalHashAgg`.
///
/// On a ROOT task the per-worker hash-table memory sits OUTSIDE the
/// concurrency division -- each partial worker keeps its own table -- and is
/// charged only when the child could have fed a stream aggregate instead. On
/// MPP and cop tasks the source's original all-inside-the-division formula
/// applies.
#[must_use]
pub fn hash_agg_cost(
    option: Option<&PlanCostOption>,
    input: HashAggInput,
    group_item_is_scalar_function: &[bool],
    factors: (&CostVer2Factor, &CostVer2Factor, f64),
    concurrency: f64,
    task_type: TaskType,
    child_cost: &CostVer2,
) -> CostVer2 {
    let (cpu_factor, mem_factor, hash_agg_cost_factor) = factors;
    let input_rows = input.input_rows.max(MIN_NUM_ROWS);
    let output_rows = input.output_rows.max(MIN_NUM_ROWS);
    let output_row_size = input.output_row_size.max(MIN_ROW_SIZE);
    let num_keys = group_item_is_scalar_function.len() as f64;

    let agg = agg_cost(option, input_rows, input.num_agg_funcs, cpu_factor);
    let group = group_cost(
        option,
        input_rows,
        group_item_is_scalar_function,
        cpu_factor,
    );
    let hash_probe = hash_probe_cost(option, input_rows, num_keys, cpu_factor);
    let start = new_cost_ver2(option, cpu_factor, 10.0 * 3.0 * cpu_factor.value(), || {
        format!("cpu(10*3*{cpu_factor})")
    });

    let cost = if matches!(task_type, TaskType::Root) {
        let hash_mem = if input.child_can_provide_order {
            new_cost_ver2(
                option,
                mem_factor,
                concurrency * output_rows * output_row_size * mem_factor.value(),
                || format!("hashmem({concurrency}*{output_rows}*{output_row_size}*{mem_factor})"),
            )
        } else {
            zero_cost_ver2()
        };
        let hash_build_cpu = new_cost_ver2(
            option,
            cpu_factor,
            output_rows * num_keys * cpu_factor.value() + output_rows * cpu_factor.value(),
            || {
                format!(
                    "hashkey({output_rows}*{num_keys}*{cpu_factor})+hashbuild({output_rows}*{cpu_factor})"
                )
            },
        );
        sum_cost_ver2(&[
            start,
            child_cost.clone(),
            hash_mem,
            div_cost_ver2(
                &sum_cost_ver2(&[agg, group, hash_build_cpu, hash_probe]),
                concurrency,
            ),
        ])
    } else {
        let hash_build = hash_build_cost(
            option,
            output_rows,
            output_row_size,
            num_keys,
            cpu_factor,
            mem_factor,
        );
        sum_cost_ver2(&[
            start,
            child_cost.clone(),
            div_cost_ver2(
                &sum_cost_ver2(&[agg, group, hash_build, hash_probe]),
                concurrency,
            ),
        ])
    };
    mul_cost_ver2(&cost, hash_agg_cost_factor)
}

/// `getPlanCostVer24PhysicalMergeJoin`: both child costs, the three condition
/// groups, and one grouping cost per side's join keys. `other_conditions`
/// apply to `left-rows + right-rows`, because they run on both sides.
#[must_use]
pub fn merge_join_cost(
    option: Option<&PlanCostOption>,
    child_rows: (f64, f64),
    conditions: (&[bool], &[bool], &[bool]),
    num_join_keys: (usize, usize),
    factors: (&CostVer2Factor, f64),
    child_costs: (&CostVer2, &CostVer2),
) -> CostVer2 {
    let (cpu_factor, merge_join_cost_factor) = factors;
    let left_rows = child_rows.0.max(MIN_NUM_ROWS);
    let right_rows = child_rows.1.max(MIN_NUM_ROWS);
    let (left_conditions, right_conditions, other_conditions) = conditions;
    let filter = sum_cost_ver2(&[
        filter_cost(option, left_rows, left_conditions, cpu_factor),
        filter_cost(option, right_rows, right_conditions, cpu_factor),
        filter_cost(option, left_rows + right_rows, other_conditions, cpu_factor),
    ]);
    // Join keys are columns, so each contributes `numFunctions`' 0.01.
    let left_keys = vec![false; num_join_keys.0];
    let right_keys = vec![false; num_join_keys.1];
    let group = sum_cost_ver2(&[
        group_cost(option, left_rows, &left_keys, cpu_factor),
        group_cost(option, right_rows, &right_keys, cpu_factor),
    ]);
    let cost = sum_cost_ver2(&[child_costs.0.clone(), child_costs.1.clone(), filter, group]);
    mul_cost_ver2(&cost, merge_join_cost_factor)
}

/// Everything `getPlanCostVer24PhysicalHashJoin` reads, already resolved to
/// build/probe sides (Go swaps them for `InnerChildIdx`/`UseOuterToBuild`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HashJoinInput {
    /// `getCardinality(build)`.
    pub build_rows: f64,
    /// `getCardinality(probe)`.
    pub probe_rows: f64,
    /// `getAvgRowSize(build.StatsInfo(), build.Schema().Columns)`.
    pub build_row_size: f64,
    /// `len(buildKeys)`.
    pub num_build_keys: usize,
    /// `len(probeKeys)`.
    pub num_probe_keys: usize,
    /// `p.Concurrency`, used by a TiDB hash join.
    pub tidb_concurrency: f64,
}

/// The MPP hash-join concurrency, an empirical value in the source.
pub const MPP_CONCURRENCY: f64 = 3.0;

/// `getPlanCostVer24PhysicalHashJoin`.
///
/// A TiDB hash join pays the build side in full and divides only the probe
/// filter and probe hash by concurrency; an MPP join divides all four.
#[must_use]
pub fn hash_join_cost(
    option: Option<&PlanCostOption>,
    input: HashJoinInput,
    build_and_probe_filters: (&[bool], &[bool]),
    factors: (&CostVer2Factor, &CostVer2Factor, f64),
    task_type: TaskType,
    child_costs: (&CostVer2, &CostVer2),
) -> CostVer2 {
    let (cpu_factor, mem_factor, hash_join_cost_factor) = factors;
    let (build_filters, probe_filters) = build_and_probe_filters;
    let (build_child_cost, probe_child_cost) = child_costs;
    let build_rows = input.build_rows.max(MIN_NUM_ROWS);
    let probe_rows = input.probe_rows;
    let build_row_size = input.build_row_size.max(MIN_ROW_SIZE);

    let build_filter = filter_cost(option, build_rows, build_filters, cpu_factor);
    let build_hash = hash_build_cost(
        option,
        build_rows,
        build_row_size,
        input.num_build_keys as f64,
        cpu_factor,
        mem_factor,
    );
    let probe_filter = filter_cost(option, probe_rows, probe_filters, cpu_factor);
    let probe_hash = hash_probe_cost(option, probe_rows, input.num_probe_keys as f64, cpu_factor);

    let cost = if matches!(task_type, TaskType::Mpp) {
        sum_cost_ver2(&[
            build_child_cost.clone(),
            probe_child_cost.clone(),
            div_cost_ver2(
                &sum_cost_ver2(&[build_hash, build_filter, probe_hash, probe_filter]),
                MPP_CONCURRENCY,
            ),
        ])
    } else {
        let start = new_cost_ver2(option, cpu_factor, 10.0 * 3.0 * cpu_factor.value(), || {
            format!("cpu(10*3*{cpu_factor})")
        });
        sum_cost_ver2(&[
            start,
            build_child_cost.clone(),
            probe_child_cost.clone(),
            build_hash,
            build_filter,
            div_cost_ver2(
                &sum_cost_ver2(&[probe_filter, probe_hash]),
                input.tidb_concurrency,
            ),
        ])
    };
    mul_cost_ver2(&cost, hash_join_cost_factor)
}

/// Which index-join executor is being costed, matching Go's `indexJoinType`
/// argument. Only the hash table term differs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexJoinKind {
    /// `IndexJoin`: the hash table is built over ALL probe rows.
    IndexJoin,
    /// `IndexHashJoin`: the hash table is built over the build side.
    IndexHashJoin,
    /// `IndexMergeJoin`: no hash table.
    IndexMergeJoin,
}

/// Everything `getIndexJoinCostVer24PhysicalIndexJoin` reads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexJoinInput {
    /// `getCardinality(build)` -- the OUTER side, read once.
    pub build_rows: f64,
    /// `getAvgRowSize(build.StatsInfo(), build.Schema().Columns)`.
    pub build_row_size: f64,
    /// `getCardinality(probe)` -- inner rows PER outer row.
    pub probe_rows_one: f64,
    /// `getAvgRowSize(probe.StatsInfo(), probe.Schema().Columns)`.
    pub probe_row_size: f64,
    /// `len(p.RightJoinKeys)`, used by `IndexHashJoin`.
    pub num_right_join_keys: usize,
    /// `len(p.LeftJoinKeys)`, used by `IndexJoin`.
    pub num_left_join_keys: usize,
    /// `getNumberOfRanges(probe)`, which drives the seeking cost.
    pub num_ranges: f64,
    /// Whether the join is any semi/anti-semi variant, which cannot stop at
    /// the first match per key and so over-reads the probe side.
    pub is_semi_join: bool,
    /// Which index-join executor is being costed.
    pub kind: IndexJoinKind,
}

/// The empirical batch ratio: an index join reads a BATCH of outer keys at a
/// time, so the inner cost is not paid once per outer row.
pub const INDEX_JOIN_BATCH_RATIO: f64 = 6.0;

/// `getIndexJoinCostVer24PhysicalIndexJoin`.
///
/// The dominant terms are `build-rows * 10 * cpu-factor` (the task-building
/// cost Go charges per outer row) and the probe-side hash table over
/// `probe-rows-one * build-rows` rows. The double-read request penalty is off
/// by default (`tidb_index_join_double_read_penalty_cost_rate = 0`).
#[must_use]
pub fn index_join_cost(
    option: Option<&PlanCostOption>,
    input: IndexJoinInput,
    build_and_probe_filters: (&[bool], &[bool]),
    factors: (&Ver2Factors, &CostFactorVars),
    session: &CostSessionOpts,
    task_type: TaskType,
    child_costs: (&CostVer2, &CostVer2),
) -> CostVer2 {
    let (ver2, cost_factors) = factors;
    let (build_filters, probe_filters) = build_and_probe_filters;
    let (build_child_cost, probe_child_cost) = child_costs;
    let build_rows = input.build_rows;
    let probe_rows_tot = input.probe_rows_one * build_rows;
    let probe_concurrency = session.index_lookup_join_concurrency;
    let cpu_factor = ver2.task_cpu(task_type);
    let mem_factor = ver2.task_mem(task_type);
    let request_factor = ver2.task_request(false);
    let scan_factor = ver2.task_scan(false, StoreType::TiKv, task_type, false);

    let build_filter = filter_cost(option, build_rows, build_filters, cpu_factor);
    let build_task = new_cost_ver2(
        option,
        cpu_factor,
        build_rows * 10.0 * cpu_factor.value(),
        || format!("cpu({build_rows}*10*{cpu_factor})"),
    );
    let start = new_cost_ver2(option, cpu_factor, 10.0 * 3.0 * cpu_factor.value(), || {
        format!("cpu(10*3*{cpu_factor})")
    });
    let probe_filter = filter_cost(option, probe_rows_tot, probe_filters, cpu_factor);

    let hash_table = match input.kind {
        IndexJoinKind::IndexHashJoin => hash_build_cost(
            option,
            build_rows,
            input.build_row_size,
            input.num_right_join_keys as f64,
            cpu_factor,
            mem_factor,
        ),
        IndexJoinKind::IndexMergeJoin => new_zero_cost_ver2(trace_cost(option)),
        IndexJoinKind::IndexJoin => hash_build_cost(
            option,
            probe_rows_tot,
            input.probe_row_size,
            input.num_left_join_keys as f64,
            cpu_factor,
            mem_factor,
        ),
    };

    let mut probe = div_cost_ver2(
        &mul_cost_ver2(probe_child_cost, build_rows),
        INDEX_JOIN_BATCH_RATIO,
    );
    if input.probe_rows_one > 1.0 && input.is_semi_join {
        probe = mul_cost_ver2(&probe, input.probe_rows_one);
    }

    let mut double_read = new_zero_cost_ver2(trace_cost(option));
    if session.index_join_double_read_penalty_cost_rate > 0.0 {
        let task_per_batch = 1024.0;
        let double_read_tasks = build_rows / session.index_join_batch_size * task_per_batch;
        double_read = double_read_cost(option, double_read_tasks, request_factor);
        double_read = mul_cost_ver2(
            &double_read,
            session.index_join_double_read_penalty_cost_rate,
        );
    }

    let seeking = index_join_seeking_cost(option, build_rows, input.num_ranges, scan_factor);

    let cost = sum_cost_ver2(&[
        start,
        build_child_cost.clone(),
        build_filter,
        build_task,
        seeking,
        div_cost_ver2(
            &sum_cost_ver2(&[double_read, probe, probe_filter, hash_table]),
            probe_concurrency,
        ),
    ]);
    mul_cost_ver2(&cost, cost_factors.index_join)
}

/// `getPlanCostVer24PhysicalApply`: unlike an index join, the probe side is
/// re-executed once per build row with no batching discount.
#[must_use]
pub fn apply_cost(
    option: Option<&PlanCostOption>,
    build_rows: f64,
    probe_rows_one: f64,
    conditions: (&[bool], &[bool]),
    cpu_factor: &CostVer2Factor,
    child_costs: (&CostVer2, &CostVer2),
) -> CostVer2 {
    let (build_conditions, probe_conditions) = conditions;
    let probe_rows_tot = build_rows * probe_rows_one;
    let build_filter = filter_cost(option, build_rows, build_conditions, cpu_factor);
    let probe_filter = filter_cost(option, probe_rows_tot, probe_conditions, cpu_factor);
    let probe = mul_cost_ver2(child_costs.1, build_rows);
    sum_cost_ver2(&[child_costs.0.clone(), build_filter, probe, probe_filter])
}

/// `getPlanCostVer24PhysicalUnionAll`: the summed child costs divided by union
/// concurrency, then the enforced-MPP discount when it applies.
#[must_use]
pub fn union_all_cost(child_costs: &[CostVer2], concurrency: f64, mpp_enforced: bool) -> CostVer2 {
    let cost = div_cost_ver2(&sum_cost_ver2(child_costs), concurrency);
    if mpp_enforced {
        div_cost_ver2(&cost, MPP_ENFORCED_DISCOUNT)
    } else {
        cost
    }
}

/// `getPlanCostVer24PointGetPlan` and `getPlanCostVer24BatchPointGetPlan`:
/// pure network cost. A plan from the fast path has no access columns and
/// costs nothing.
#[must_use]
pub fn point_get_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    net_factor: &CostVer2Factor,
    has_access_cols: bool,
) -> CostVer2 {
    if !has_access_cols {
        return zero_cost_ver2();
    }
    net_cost(option, rows, row_size, net_factor)
}

/// `getPlanCostVer2PhysicalExchangeReceiver`: child cost plus network cost,
/// tripled for a broadcast exchange (the source's empirical three nodes).
#[must_use]
pub fn exchange_receiver_cost(
    option: Option<&PlanCostOption>,
    rows: f64,
    row_size: f64,
    net_factor: &CostVer2Factor,
    is_broadcast: bool,
    child_cost: &CostVer2,
) -> CostVer2 {
    let num_node = 3.0;
    let mut net = net_cost(option, rows, row_size, net_factor);
    if is_broadcast {
        net = mul_cost_ver2(&net, num_node);
    }
    sum_cost_ver2(&[child_cost.clone(), net])
}

/// `getPlanCostVer24PhysicalCTE`: the projection cost of its own schema
/// columns, and nothing else -- a CTE consumer does not re-pay its producer.
#[must_use]
pub fn cte_cost(
    option: Option<&PlanCostOption>,
    input_rows: f64,
    num_schema_columns: usize,
    cpu_factor: &CostVer2Factor,
) -> CostVer2 {
    let columns = vec![false; num_schema_columns];
    filter_cost(option, input_rows, &columns, cpu_factor)
}

// ---------------------------------------------------------------------------
// Task comparison
// ---------------------------------------------------------------------------

/// A task's cost and whether the task is valid, as `getTaskPlanCost` returns
/// them. An invalid task reports `f64::MAX`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TaskPlanCost {
    /// The task's total plan cost.
    pub cost: f64,
    /// Whether the task is invalid (`t.Invalid()`).
    pub invalid: bool,
}

impl TaskPlanCost {
    /// A valid task with the given cost.
    #[must_use]
    pub const fn valid(cost: f64) -> Self {
        Self {
            cost,
            invalid: false,
        }
    }

    /// `getTaskPlanCost`'s invalid-task answer.
    #[must_use]
    pub const fn invalid() -> Self {
        Self {
            cost: f64::MAX,
            invalid: true,
        }
    }
}

/// `compareTaskCost`: whether `current` should replace `best`.
///
/// The tie direction is STRICT `<`, so an exactly equal alternative never
/// displaces the incumbent -- that is what makes the enumeration order, not
/// the cost, decide a tie. An invalid current task never wins; an invalid best
/// task always loses, even to a more expensive current task.
#[must_use]
pub fn compare_task_cost(current: TaskPlanCost, best: TaskPlanCost) -> bool {
    if current.invalid {
        return false;
    }
    if best.invalid {
        return true;
    }
    current.cost < best.cost
}

#[cfg(test)]
mod golden_tests;
