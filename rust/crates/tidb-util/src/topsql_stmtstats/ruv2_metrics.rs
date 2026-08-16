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

//! boundary: Go `pkg/util/execdetails` `RUV2Metrics` and `RUV2Weights`.
//!
//! `stmtstats` carries a statement's RU v2 metrics pointer through
//! [`super::ExecutionContext`] and calls exactly one method on it,
//! `RUV2Metrics.TotalRU(weights, tiKVRU, tiFlashRU)`. `execdetails` lands in
//! `tidb-exec`, which sits above this crate, so the two types are recovered
//! here as local snapshots holding precisely what `TotalRU` needs:
//! [`RuV2Weights`] is the full Go weight set field-for-field, and
//! [`RuV2Metrics`] carries the twelve counters
//! `calculateRUValuesWithWeights` multiplies with, plus the bypass flag.
//!
//! Narrowing inside the narrowing: Go's `executorL1`/`executorL2`/`executorL3`
//! are per-executor label groups, but `calculateRUValuesWithWeights` only ever
//! multiplies each group's *sum* by one weight, so each group is a single
//! aggregate counter here. The per-label breakdown is `tidb-exec`'s concern
//! (`ruv2_metrics::RuV2Metrics`) and is invisible to RU totals.
//!
//! Go keeps these counters in `atomic.Int64` because a statement's metrics are
//! written by the executor while the Top-RU aggregator samples them; the same
//! atomics are used here for the same reason.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// Go `execdetails.RUV2Weights`: the per-counter weights
/// `calculateRUValuesWithWeights` multiplies with.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RuV2Weights {
    /// Go `RUV2Weights.RUScale`.
    pub ru_scale: f64,
    /// Go `RUV2Weights.ResultChunkCells`.
    pub result_chunk_cells: f64,
    /// Go `RUV2Weights.ExecutorL1`.
    pub executor_l1: f64,
    /// Go `RUV2Weights.ExecutorL2`.
    pub executor_l2: f64,
    /// Go `RUV2Weights.ExecutorL3`.
    pub executor_l3: f64,
    /// Go `RUV2Weights.ExecutorL5InsertRows`.
    pub executor_l5_insert_rows: f64,
    /// Go `RUV2Weights.PlanCnt`.
    pub plan_cnt: f64,
    /// Go `RUV2Weights.PlanDeriveStatsPaths`.
    pub plan_derive_stats_paths: f64,
    /// Go `RUV2Weights.ResourceManagerReadCnt`.
    pub resource_manager_read_cnt: f64,
    /// Go `RUV2Weights.ResourceManagerWriteCnt`.
    pub resource_manager_write_cnt: f64,
    /// Go `RUV2Weights.WriteKeys`.
    pub write_keys: f64,
    /// Go `RUV2Weights.SessionParserTotal`.
    pub session_parser_total: f64,
    /// Go `RUV2Weights.TxnCnt`.
    pub txn_cnt: f64,
}

/// boundary: Go `execdetails.RUV2Metrics`, narrowed to the counters that
/// contribute to `CalculateRUValues`.
#[derive(Debug, Default)]
pub struct RuV2Metrics {
    bypass: AtomicBool,
    result_chunk_cells: AtomicI64,
    executor_l1: AtomicI64,
    executor_l2: AtomicI64,
    executor_l3: AtomicI64,
    executor_l5_insert_rows: AtomicI64,
    plan_cnt: AtomicI64,
    plan_derive_stats_paths: AtomicI64,
    resource_manager_read_cnt: AtomicI64,
    resource_manager_write_cnt: AtomicI64,
    write_keys: AtomicI64,
    session_parser_total: AtomicI64,
    txn_cnt: AtomicI64,
}

macro_rules! counter {
    ($add:ident, $get:ident, $field:ident, $go_add:literal, $go_get:literal) => {
        #[doc = concat!("Go `", $go_add, "`.")]
        pub fn $add(&self, delta: i64) {
            self.$field.fetch_add(delta, Ordering::Relaxed);
        }

        #[doc = concat!("Go `", $go_get, "`.")]
        #[must_use]
        pub fn $get(&self) -> i64 {
            self.$field.load(Ordering::Relaxed)
        }
    };
}

impl RuV2Metrics {
    /// Go `NewRUV2Metrics`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `RUV2Metrics.SetBypass`.
    pub fn set_bypass(&self, enabled: bool) {
        self.bypass.store(enabled, Ordering::Relaxed);
    }

    /// Go `RUV2Metrics.Bypass`.
    #[must_use]
    pub fn bypass(&self) -> bool {
        self.bypass.load(Ordering::Relaxed)
    }

    counter!(
        add_result_chunk_cells,
        result_chunk_cells,
        result_chunk_cells,
        "AddResultChunkCells",
        "ResultChunkCells"
    );
    counter!(
        add_executor_l1,
        executor_l1,
        executor_l1,
        "AddExecutorMetric (level 1)",
        "sumRUV2ExtraLabelCounter(executorL1)"
    );
    counter!(
        add_executor_l2,
        executor_l2,
        executor_l2,
        "AddExecutorMetric (level 2)",
        "sumRUV2ExtraLabelCounter(executorL2)"
    );
    counter!(
        add_executor_l3,
        executor_l3,
        executor_l3,
        "AddExecutorMetric (level 3)",
        "sumRUV2ExtraLabelCounter(executorL3)"
    );
    counter!(
        add_executor_l5_insert_rows,
        executor_l5_insert_rows,
        executor_l5_insert_rows,
        "AddExecutorL5InsertRows",
        "ExecutorL5InsertRows"
    );
    counter!(add_plan_cnt, plan_cnt, plan_cnt, "AddPlanCnt", "PlanCnt");
    counter!(
        add_plan_derive_stats_paths,
        plan_derive_stats_paths,
        plan_derive_stats_paths,
        "AddPlanDeriveStatsPaths",
        "PlanDeriveStatsPaths"
    );
    counter!(
        add_resource_manager_read_cnt,
        resource_manager_read_cnt,
        resource_manager_read_cnt,
        "AddResourceManagerReadCnt",
        "ResourceManagerReadCnt"
    );
    counter!(
        add_resource_manager_write_cnt,
        resource_manager_write_cnt,
        resource_manager_write_cnt,
        "AddResourceManagerWriteCnt",
        "ResourceManagerWriteCnt"
    );
    counter!(
        add_write_keys,
        write_keys,
        write_keys,
        "AddWriteKeys",
        "WriteKeys"
    );
    counter!(
        add_session_parser_total,
        session_parser_total,
        session_parser_total,
        "AddSessionParserTotal",
        "SessionParserTotal"
    );
    counter!(add_txn_cnt, txn_cnt, txn_cnt, "AddTxnCnt", "TxnCnt");

    /// Go `RUV2Metrics.CalculateRUValues`: the TiDB-side RU, zero when
    /// bypassed. Go's nil receiver is the caller's `Option`, handled by
    /// [`total_ru`].
    #[must_use]
    #[expect(clippy::cast_precision_loss, reason = "Go float64(int64) conversion")]
    pub fn calculate_ru_values(&self, weights: RuV2Weights) -> f64 {
        if self.bypass() {
            return 0.0;
        }
        let tidb_ru = self.result_chunk_cells() as f64 * weights.result_chunk_cells
            + self.executor_l1() as f64 * weights.executor_l1
            + self.executor_l2() as f64 * weights.executor_l2
            + self.executor_l3() as f64 * weights.executor_l3
            + self.executor_l5_insert_rows() as f64 * weights.executor_l5_insert_rows
            + self.plan_cnt() as f64 * weights.plan_cnt
            + self.plan_derive_stats_paths() as f64 * weights.plan_derive_stats_paths
            + self.resource_manager_read_cnt() as f64 * weights.resource_manager_read_cnt
            + self.resource_manager_write_cnt() as f64 * weights.resource_manager_write_cnt
            + self.write_keys() as f64 * weights.write_keys
            + self.session_parser_total() as f64 * weights.session_parser_total
            + self.txn_cnt() as f64 * weights.txn_cnt;
        tidb_ru * weights.ru_scale
    }
}

/// Go `RUV2Metrics.TotalRU`: the statement RU v2 total as TiDB + TiKV +
/// TiFlash, on a possibly-nil (Go) receiver.
#[must_use]
pub fn total_ru(
    metrics: Option<&RuV2Metrics>,
    weights: RuV2Weights,
    tikv_ru: f64,
    tiflash_ru: f64,
) -> f64 {
    match metrics {
        None => tikv_ru + tiflash_ru,
        Some(m) if m.bypass() => 0.0,
        Some(m) => m.calculate_ru_values(weights) + tikv_ru + tiflash_ru,
    }
}
