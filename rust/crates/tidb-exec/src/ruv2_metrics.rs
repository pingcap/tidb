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

//! SEED of Go `pkg/util/execdetails`, covering `ruv2_metrics.go`: the live
//! statement-level `RUV2Metrics` container with its mutation/accessor surface
//! (`NewRUV2Metrics`, `SetBypass`/`Bypass`, every `Add*` counter method, every
//! getter, `IsZero`, `Clone`, `Merge`, `CalculateRUValues`, `TotalRU`), the
//! ingest functions (`UpdateRUV2MetricsFromRUV2`, `applyRawCounters`,
//! `SyncRUV2MetricsFromRUDetails`, `UpdateRUV2MetricsFromCommitDetails`), the
//! hot-path recorder (`ExecutorMetricRecorder`, `ResolveExecutorMetric`,
//! `execL1Kind`/`execL1KindForLabel`), the internal counter structures
//! (`ruv2ExecutorL1Counter`, `ruv2ExtraLabelCounter`, `ruv2MetricsExtra` and
//! their add/snapshot/sum/clone helpers), and the formatting entry points
//! (`FormatRUV2Summary`, `FormatRUV2Total`, `FormatRUV2Metrics`).
//!
//! Relationship to [`crate::slow_log_format`]: that module already carries the
//! read-only narrowing of this type — [`RuV2Weights`], the plain-value
//! [`RuV2MetricsSnapshot`], and `format_ruv2_summary` over snapshots. This
//! module defines the richer live (atomic, mutable) production container and
//! REUSES those types instead of redefining them: [`RuV2Metrics::snapshot`]
//! bridges live counters to `RuV2MetricsSnapshot`, and the three `format_*`
//! functions delegate to `slow_log_format::format_ruv2_summary`, whose output
//! is byte-identical to Go `FormatRUV2Summary` (the private Go helper
//! `formatRUV2LabelMap` is therefore not re-ported here — it lives there).
//!
//! Narrowings and boundaries, by name:
//! - Prometheus process-global counters (Go `pkg/metrics` `RUV2*` vectors,
//!   `metrics.RUV2ExecutorCounter`,
//!   `metrics.RUV2TiKVCoprocessorWorkTotalCounter`): omitted. Every `Add*`
//!   method updates only the statement-local counters;
//!   [`ExecutorMetricRecorder`] carries no `prometheus.Counter`, and
//!   [`resolve_executor_metric`] drops Go's counter-nil check (in Go,
//!   `RUV2ExecutorCounter` never returns nil for level-1 labels, so
//!   `Available` is kind-only there too).
//! - `kvrpcpb.RUV2` / `kvrpcpb.ExecutorInputs` protobufs are not available:
//!   [`Ruv2`] and [`ExecutorInputs`] are minimal snapshot input structs with
//!   exactly the fields `applyRawCounters` reads.
//! - `// boundary:` client-go `util.RUDetails` (owned by the runtime-stats
//!   half of the package): [`RuDetails`] is a minimal local stand-in exposing
//!   only `AddRUV2`/`DrainRUV2`, with the drain-the-delta-since-last-drain
//!   semantics pinned by Go `TestSyncRUV2MetricsFromRUDetailsIncremental`.
//! - `// boundary:` `RUV2MetricsFromContext`, `RUV2MetricsCtxKey`,
//!   `ruv2MetricsKeyType`, and `StmtExecDetails.getRUV2Metrics` are not
//!   ported: they are Go `context.Context` plumbing, and `StmtExecDetails`
//!   belongs to the runtime-stats half of the package.
//! - Concurrency mapping: Go `atomic.Bool`/`atomic.AddInt64` become
//!   [`AtomicBool`]/[`AtomicI64`] with `Ordering::Relaxed` — the counters are
//!   commutative adds and independent loads, so no cross-field ordering is
//!   needed; Go's lazily CAS-allocated `atomic.Pointer[ruv2MetricsExtra]`
//!   becomes a [`OnceLock`]; Go's `sync.Map` of `*int64` label counters
//!   flattens into a `Mutex<BTreeMap<String, i64>>` (sorted iteration matches
//!   Go's sort-before-render).
//! - Go nil-receiver methods and nil-argument functions become free functions
//!   over `Option<&RuV2Metrics>` ([`total_ru`], [`update_ruv2_metrics_from_ruv2`],
//!   [`sync_ruv2_metrics_from_ru_details`],
//!   [`update_ruv2_metrics_from_commit_details`], [`format_ruv2_summary`]);
//!   Go `Clone` (nil returns nil) becomes `impl Clone` on the non-nil case.
//! - `ExecutorMetricRecorder::record` on an unavailable recorder is a no-op
//!   here where Go would nil-panic (Go documents "caller must check
//!   Available"; the safe no-op preserves every defined behavior).
//! - Test narrowings: Go `defaultRUV2WeightsForTest` reads
//!   `config.DefaultRUV2Config()`; its weight values are copied here as exact
//!   literals. The Go subtest "known executor labels avoid per statement map
//!   allocations" (`testing.AllocsPerRun`) is not portable and is skipped.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::exec_details::CommitDetails;
use crate::slow_log_format::{RuV2MetricsSnapshot, RuV2Weights};

/// Go `kvrpcpb.RUV2` — a minimal snapshot input struct with exactly the
/// fields `applyRawCounters` reads (protobuf not available here).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ruv2 {
    /// Go `RUV2.ReadRpcCount`.
    pub read_rpc_count: u64,
    /// Go `RUV2.WriteRpcCount`.
    pub write_rpc_count: u64,
    /// Go `RUV2.KvEngineCacheMiss`.
    pub kv_engine_cache_miss: u64,
    /// Go `RUV2.CoprocessorExecutorIterations`.
    pub coprocessor_executor_iterations: u64,
    /// Go `RUV2.CoprocessorResponseBytes`.
    pub coprocessor_response_bytes: u64,
    /// Go `RUV2.RaftstoreStoreWriteTriggerWbBytes`.
    pub raftstore_store_write_trigger_wb_bytes: u64,
    /// Go `RUV2.StorageProcessedKeysBatchGet`.
    pub storage_processed_keys_batch_get: u64,
    /// Go `RUV2.StorageProcessedKeysGet`.
    pub storage_processed_keys_get: u64,
    /// Go `RUV2.ExecutorInputs` (`*kvrpcpb.ExecutorInputs`).
    pub executor_inputs: Option<ExecutorInputs>,
}

/// Go `kvrpcpb.ExecutorInputs` — the seven per-executor work counters
/// `applyRawCounters` reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutorInputs {
    /// Go `ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan`.
    pub tikv_coprocessor_executor_work_total_batch_index_scan: u64,
    /// Go `ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchTableScan`.
    pub tikv_coprocessor_executor_work_total_batch_table_scan: u64,
    /// Go `ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchSelection`.
    pub tikv_coprocessor_executor_work_total_batch_selection: u64,
    /// Go `ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchTopN`.
    pub tikv_coprocessor_executor_work_total_batch_top_n: u64,
    /// Go `ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchLimit`.
    pub tikv_coprocessor_executor_work_total_batch_limit: u64,
    /// Go `ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchSimpleAggr`.
    pub tikv_coprocessor_executor_work_total_batch_simple_aggr: u64,
    /// Go `ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchFastHashAggr`.
    pub tikv_coprocessor_executor_work_total_batch_fast_hash_aggr: u64,
}

impl Ruv2 {
    fn merge_from(&mut self, other: &Ruv2) {
        self.read_rpc_count += other.read_rpc_count;
        self.write_rpc_count += other.write_rpc_count;
        self.kv_engine_cache_miss += other.kv_engine_cache_miss;
        self.coprocessor_executor_iterations += other.coprocessor_executor_iterations;
        self.coprocessor_response_bytes += other.coprocessor_response_bytes;
        self.raftstore_store_write_trigger_wb_bytes += other.raftstore_store_write_trigger_wb_bytes;
        self.storage_processed_keys_batch_get += other.storage_processed_keys_batch_get;
        self.storage_processed_keys_get += other.storage_processed_keys_get;
        if let Some(other_inputs) = &other.executor_inputs {
            let inputs = self
                .executor_inputs
                .get_or_insert_with(ExecutorInputs::default);
            inputs.tikv_coprocessor_executor_work_total_batch_index_scan +=
                other_inputs.tikv_coprocessor_executor_work_total_batch_index_scan;
            inputs.tikv_coprocessor_executor_work_total_batch_table_scan +=
                other_inputs.tikv_coprocessor_executor_work_total_batch_table_scan;
            inputs.tikv_coprocessor_executor_work_total_batch_selection +=
                other_inputs.tikv_coprocessor_executor_work_total_batch_selection;
            inputs.tikv_coprocessor_executor_work_total_batch_top_n +=
                other_inputs.tikv_coprocessor_executor_work_total_batch_top_n;
            inputs.tikv_coprocessor_executor_work_total_batch_limit +=
                other_inputs.tikv_coprocessor_executor_work_total_batch_limit;
            inputs.tikv_coprocessor_executor_work_total_batch_simple_aggr +=
                other_inputs.tikv_coprocessor_executor_work_total_batch_simple_aggr;
            inputs.tikv_coprocessor_executor_work_total_batch_fast_hash_aggr +=
                other_inputs.tikv_coprocessor_executor_work_total_batch_fast_hash_aggr;
        }
    }
}

// boundary: client-go `util.RUDetails` (Go `tikvutil.RUDetails`, owned by the
// runtime-stats half of the package). Only the two entry points
// `ruv2_metrics.go` and its tests exercise are represented: `AddRUV2`
// accumulates raw RUv2 counters, and `DrainRUV2` returns the counters
// accumulated since the last drain (nil/None when nothing is new), the delta
// semantics pinned by Go `TestSyncRUV2MetricsFromRUDetailsIncremental`.
/// Minimal local stand-in for client-go `util.RUDetails`.
#[derive(Debug, Default)]
pub struct RuDetails {
    pending: Mutex<Option<Ruv2>>,
}

impl RuDetails {
    /// Go client-go `util.NewRUDetails`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go client-go `RUDetails.AddRUV2`: accumulates raw RUv2 counters.
    pub fn add_ruv2(&self, ru: &Ruv2) {
        let mut pending = self.pending.lock().expect("RuDetails mutex poisoned");
        pending.get_or_insert_with(Ruv2::default).merge_from(ru);
    }

    /// Go client-go `RUDetails.DrainRUV2`: takes the counters accumulated
    /// since the last drain, or `None` when nothing is new.
    #[must_use]
    pub fn drain_ruv2(&self) -> Option<Ruv2> {
        self.pending
            .lock()
            .expect("RuDetails mutex poisoned")
            .take()
    }
}

/// Go `ruv2LabelBatchPointGetExec`.
const RUV2_LABEL_BATCH_POINT_GET_EXEC: &str = "BatchPointGetExec";
/// Go `ruv2LabelPointGetExecutor`.
const RUV2_LABEL_POINT_GET_EXECUTOR: &str = "PointGetExecutor";
/// Go `ruv2LabelLimitExec`.
const RUV2_LABEL_LIMIT_EXEC: &str = "LimitExec";

/// Go `execL1Kind`: selects one of the hot L1 executor counter fields;
/// `None` (Go `execL1None`) means none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ExecL1Kind {
    #[default]
    None,
    BatchPointGet,
    PointGet,
    Limit,
}

/// Go `execL1KindForLabel`.
fn exec_l1_kind_for_label(label: &str) -> ExecL1Kind {
    match label {
        RUV2_LABEL_BATCH_POINT_GET_EXEC => ExecL1Kind::BatchPointGet,
        RUV2_LABEL_POINT_GET_EXECUTOR => ExecL1Kind::PointGet,
        RUV2_LABEL_LIMIT_EXEC => ExecL1Kind::Limit,
        _ => ExecL1Kind::None,
    }
}

/// Go `ruv2ExtraLabelCounter`: Go's lazily CAS-allocated `sync.Map` of
/// `*int64` flattens into a mutexed sorted map (see module header).
#[derive(Debug, Default)]
struct Ruv2ExtraLabelCounter {
    values: Mutex<BTreeMap<String, i64>>,
}

impl Ruv2ExtraLabelCounter {
    /// Go `addRUV2ExtraLabelCounter`.
    fn add(&self, label: &str, delta: i64) {
        let mut values = self.values.lock().expect("label counter mutex poisoned");
        *values.entry(label.to_owned()).or_insert(0) += delta;
    }

    /// Go `snapshotRUV2ExtraLabelCounter`: copies non-zero labels into `out`.
    fn snapshot_into(&self, out: &mut BTreeMap<String, i64>) {
        let values = self.values.lock().expect("label counter mutex poisoned");
        for (label, value) in values.iter() {
            if *value != 0 {
                out.insert(label.clone(), *value);
            }
        }
    }

    /// Go `sumRUV2ExtraLabelCounter`.
    fn sum(&self) -> i64 {
        let values = self.values.lock().expect("label counter mutex poisoned");
        values.values().sum()
    }

    /// Go `cloneRUV2ExtraLabelCounter`: adds this counter's non-zero labels
    /// into `dst`.
    fn clone_into(&self, dst: &Ruv2ExtraLabelCounter) {
        let values = self.values.lock().expect("label counter mutex poisoned");
        for (label, value) in values.iter() {
            if *value != 0 {
                dst.add(label, *value);
            }
        }
    }
}

/// Go `ruv2ExecutorL1Counter`: three hot fixed slots plus an extra label map.
#[derive(Debug, Default)]
struct Ruv2ExecutorL1Counter {
    batch_point_get_exec: AtomicI64,
    point_get_executor: AtomicI64,
    limit_exec: AtomicI64,
    extra: Ruv2ExtraLabelCounter,
}

impl Ruv2ExecutorL1Counter {
    /// Go `(*ruv2ExecutorL1Counter).add`.
    fn add(&self, label: &str, delta: i64) {
        if let Some(field) = self.field_by_kind(exec_l1_kind_for_label(label)) {
            field.fetch_add(delta, Ordering::Relaxed);
            return;
        }
        self.extra.add(label, delta);
    }

    /// Go `(*ruv2ExecutorL1Counter).fieldByKind`.
    fn field_by_kind(&self, kind: ExecL1Kind) -> Option<&AtomicI64> {
        match kind {
            ExecL1Kind::BatchPointGet => Some(&self.batch_point_get_exec),
            ExecL1Kind::PointGet => Some(&self.point_get_executor),
            ExecL1Kind::Limit => Some(&self.limit_exec),
            ExecL1Kind::None => None,
        }
    }

    /// Go `(*ruv2ExecutorL1Counter).snapshot` (with `addRUV2LabelValue`'s
    /// skip-zero behavior).
    fn snapshot(&self) -> BTreeMap<String, i64> {
        let mut out = BTreeMap::new();
        add_ruv2_label_value(
            &mut out,
            RUV2_LABEL_BATCH_POINT_GET_EXEC,
            self.batch_point_get_exec.load(Ordering::Relaxed),
        );
        add_ruv2_label_value(
            &mut out,
            RUV2_LABEL_POINT_GET_EXECUTOR,
            self.point_get_executor.load(Ordering::Relaxed),
        );
        add_ruv2_label_value(
            &mut out,
            RUV2_LABEL_LIMIT_EXEC,
            self.limit_exec.load(Ordering::Relaxed),
        );
        self.extra.snapshot_into(&mut out);
        out
    }

    /// Go `(*ruv2ExecutorL1Counter).sum`.
    fn sum(&self) -> i64 {
        self.batch_point_get_exec.load(Ordering::Relaxed)
            + self.point_get_executor.load(Ordering::Relaxed)
            + self.limit_exec.load(Ordering::Relaxed)
            + self.extra.sum()
    }

    /// Go `(*ruv2ExecutorL1Counter).isZero`.
    fn is_zero(&self) -> bool {
        self.sum() == 0
    }

    /// Go `cloneRUV2ExecutorL1Counter`: adds this counter into `dst`.
    fn clone_into(&self, dst: &Ruv2ExecutorL1Counter) {
        add_ruv2_fixed_counter(
            &dst.batch_point_get_exec,
            self.batch_point_get_exec.load(Ordering::Relaxed),
        );
        add_ruv2_fixed_counter(
            &dst.point_get_executor,
            self.point_get_executor.load(Ordering::Relaxed),
        );
        add_ruv2_fixed_counter(&dst.limit_exec, self.limit_exec.load(Ordering::Relaxed));
        self.extra.clone_into(&dst.extra);
    }
}

/// Go `addRUV2LabelValue`: skip-zero map insert.
fn add_ruv2_label_value(out: &mut BTreeMap<String, i64>, label: &str, value: i64) {
    if value != 0 {
        out.insert(label.to_owned(), value);
    }
}

/// Go `addRUV2FixedCounter`: skip-zero atomic add.
fn add_ruv2_fixed_counter(dst: &AtomicI64, delta: i64) {
    if delta != 0 {
        dst.fetch_add(delta, Ordering::Relaxed);
    }
}

/// Go `ruv2MetricsExtra`: the cold counters Go allocates lazily.
#[derive(Debug, Default)]
struct Ruv2MetricsExtra {
    executor_l2: Ruv2ExtraLabelCounter,
    executor_l3: Ruv2ExtraLabelCounter,

    executor_l5_insert_rows: AtomicI64,
    plan_derive_stats_paths: AtomicI64,

    resource_manager_write_cnt: AtomicI64,
    write_keys: AtomicI64,
    write_size: AtomicI64,

    tikv_coprocessor_executor_iterations: AtomicI64,
    tikv_coprocessor_response_bytes: AtomicI64,
    tikv_raftstore_store_write_trigger_wb: AtomicI64,
    tikv_coprocessor_work_total: Ruv2ExtraLabelCounter,
}

impl Ruv2MetricsExtra {
    /// Go `cloneRUV2MetricsExtra`: adds this extra block into `dst`.
    fn clone_into(&self, dst: &Ruv2MetricsExtra) {
        self.executor_l2.clone_into(&dst.executor_l2);
        self.executor_l3.clone_into(&dst.executor_l3);
        add_ruv2_fixed_counter(
            &dst.executor_l5_insert_rows,
            self.executor_l5_insert_rows.load(Ordering::Relaxed),
        );
        add_ruv2_fixed_counter(
            &dst.plan_derive_stats_paths,
            self.plan_derive_stats_paths.load(Ordering::Relaxed),
        );
        add_ruv2_fixed_counter(
            &dst.resource_manager_write_cnt,
            self.resource_manager_write_cnt.load(Ordering::Relaxed),
        );
        add_ruv2_fixed_counter(&dst.write_keys, self.write_keys.load(Ordering::Relaxed));
        add_ruv2_fixed_counter(&dst.write_size, self.write_size.load(Ordering::Relaxed));
        add_ruv2_fixed_counter(
            &dst.tikv_coprocessor_executor_iterations,
            self.tikv_coprocessor_executor_iterations
                .load(Ordering::Relaxed),
        );
        add_ruv2_fixed_counter(
            &dst.tikv_coprocessor_response_bytes,
            self.tikv_coprocessor_response_bytes.load(Ordering::Relaxed),
        );
        add_ruv2_fixed_counter(
            &dst.tikv_raftstore_store_write_trigger_wb,
            self.tikv_raftstore_store_write_trigger_wb
                .load(Ordering::Relaxed),
        );
        self.tikv_coprocessor_work_total
            .clone_into(&dst.tikv_coprocessor_work_total);
    }
}

/// Go `RUV2Metrics`: stores statement-level RUv2 metrics.
#[derive(Debug, Default)]
pub struct RuV2Metrics {
    bypass: AtomicBool,

    result_chunk_cells: AtomicI64,

    executor_l1: Ruv2ExecutorL1Counter,

    plan_cnt: AtomicI64,
    session_parser_total: AtomicI64,
    txn_cnt: AtomicI64,

    resource_manager_read_cnt: AtomicI64,

    tikv_kv_engine_cache_miss: AtomicI64,
    tikv_storage_processed_keys_batch_get: AtomicI64,
    tikv_storage_processed_keys_get: AtomicI64,

    /// Go `extra atomic.Pointer[ruv2MetricsExtra]` (CAS-once allocation).
    extra: OnceLock<Ruv2MetricsExtra>,
}

/// Go `int64(v)` over a `uint64` protobuf counter: two's-complement wrap.
#[expect(clippy::cast_possible_wrap, reason = "Go int64(uint64) conversion")]
fn go_i64(v: u64) -> i64 {
    v as i64
}

impl RuV2Metrics {
    /// Go `NewRUV2Metrics`: creates a new RUv2 metrics container.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `loadExtra`.
    fn load_extra(&self) -> Option<&Ruv2MetricsExtra> {
        self.extra.get()
    }

    /// Go `ensureExtra`.
    fn ensure_extra(&self) -> &Ruv2MetricsExtra {
        self.extra.get_or_init(Ruv2MetricsExtra::default)
    }

    /// Go `SetBypass`: marks whether statement-level RU accounting should be
    /// skipped.
    pub fn set_bypass(&self, enabled: bool) {
        self.bypass.store(enabled, Ordering::Relaxed);
    }

    /// Go `Bypass`: whether statement-level RU accounting should be skipped.
    #[must_use]
    pub fn bypass(&self) -> bool {
        self.bypass.load(Ordering::Relaxed)
    }

    /// Go `AddResultChunkCells`: records result cells written by the current
    /// statement.
    pub fn add_result_chunk_cells(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.result_chunk_cells.fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddExecutorMetric`: records a statement-level executor metric for
    /// the given RUv2 level.
    pub fn add_executor_metric(&self, level: i64, label: &str, delta: i64) {
        if self.bypass() || delta == 0 || label.is_empty() {
            return;
        }
        match level {
            1 => self.executor_l1.add(label, delta),
            2 => self.ensure_extra().executor_l2.add(label, delta),
            3 => self.ensure_extra().executor_l3.add(label, delta),
            _ => {}
        }
    }

    /// Go `AddExecutorL5InsertRows`: records insert rows multiplied by
    /// inserted column count for RUv2 accounting.
    pub fn add_executor_l5_insert_rows(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .executor_l5_insert_rows
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddPlanCnt`: records plan builder invocations for the current
    /// statement.
    pub fn add_plan_cnt(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.plan_cnt.fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddPlanDeriveStatsPaths`: records derived stats paths for the
    /// current statement.
    pub fn add_plan_derive_stats_paths(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .plan_derive_stats_paths
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddSessionParserTotal`: records parser executions for the current
    /// statement.
    pub fn add_session_parser_total(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.session_parser_total
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTxnCnt`: records transaction completions attributed to the
    /// current statement.
    pub fn add_txn_cnt(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.txn_cnt.fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddResourceManagerReadCnt`: records TiKV read RPCs charged to
    /// resource management.
    pub fn add_resource_manager_read_cnt(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.resource_manager_read_cnt
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddResourceManagerWriteCnt`: records TiKV write RPCs charged to
    /// resource management.
    pub fn add_resource_manager_write_cnt(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .resource_manager_write_cnt
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddWriteKeys`: records commit write keys for RUv2 accounting.
    pub fn add_write_keys(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .write_keys
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddWriteSize`: records commit write size for RUv2 shadow
    /// accounting.
    pub fn add_write_size(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .write_size
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTiKVKVEngineCacheMiss`: records TiKV kv_engine_cache_miss
    /// counters from ExecDetailsV2.
    pub fn add_tikv_kv_engine_cache_miss(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.tikv_kv_engine_cache_miss
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTiKVCoprocessorExecutorIterations`: records TiKV coprocessor
    /// iteration counters.
    pub fn add_tikv_coprocessor_executor_iterations(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .tikv_coprocessor_executor_iterations
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTiKVCoprocessorResponseBytes`: records TiKV coprocessor
    /// response bytes.
    pub fn add_tikv_coprocessor_response_bytes(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .tikv_coprocessor_response_bytes
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTiKVRaftstoreStoreWriteTriggerWB`: records TiKV raftstore
    /// write trigger bytes.
    pub fn add_tikv_raftstore_store_write_trigger_wb(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.ensure_extra()
            .tikv_raftstore_store_write_trigger_wb
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTiKVStorageProcessedKeysBatchGet`: records TiKV batch-get
    /// processed keys.
    pub fn add_tikv_storage_processed_keys_batch_get(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.tikv_storage_processed_keys_batch_get
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTiKVStorageProcessedKeysGet`: records TiKV get processed keys.
    pub fn add_tikv_storage_processed_keys_get(&self, delta: i64) {
        if self.bypass() {
            return;
        }
        self.tikv_storage_processed_keys_get
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Go `AddTiKVCoprocessorWorkTotal`: records TiKV executor input
    /// counters by executor type.
    pub fn add_tikv_coprocessor_work_total(&self, label: &str, delta: i64) {
        if self.bypass() || delta == 0 || label.is_empty() {
            return;
        }
        self.ensure_extra()
            .tikv_coprocessor_work_total
            .add(label, delta);
    }

    /// Go `applyRawCounters`: writes `ru` into `self`. Caller must check
    /// bypass. (The Go per-counter prometheus `Add` calls are narrowed away;
    /// see module header.)
    fn apply_raw_counters(&self, ru: &Ruv2) {
        if ru.read_rpc_count != 0 {
            self.resource_manager_read_cnt
                .fetch_add(go_i64(ru.read_rpc_count), Ordering::Relaxed);
        }
        if ru.kv_engine_cache_miss != 0 {
            self.tikv_kv_engine_cache_miss
                .fetch_add(go_i64(ru.kv_engine_cache_miss), Ordering::Relaxed);
        }
        if ru.storage_processed_keys_batch_get != 0 {
            self.tikv_storage_processed_keys_batch_get.fetch_add(
                go_i64(ru.storage_processed_keys_batch_get),
                Ordering::Relaxed,
            );
        }
        if ru.storage_processed_keys_get != 0 {
            self.tikv_storage_processed_keys_get
                .fetch_add(go_i64(ru.storage_processed_keys_get), Ordering::Relaxed);
        }

        if ru.write_rpc_count != 0 {
            self.ensure_extra()
                .resource_manager_write_cnt
                .fetch_add(go_i64(ru.write_rpc_count), Ordering::Relaxed);
        }
        if ru.coprocessor_executor_iterations != 0 {
            self.ensure_extra()
                .tikv_coprocessor_executor_iterations
                .fetch_add(
                    go_i64(ru.coprocessor_executor_iterations),
                    Ordering::Relaxed,
                );
        }
        if ru.coprocessor_response_bytes != 0 {
            self.ensure_extra()
                .tikv_coprocessor_response_bytes
                .fetch_add(go_i64(ru.coprocessor_response_bytes), Ordering::Relaxed);
        }
        if ru.raftstore_store_write_trigger_wb_bytes != 0 {
            self.ensure_extra()
                .tikv_raftstore_store_write_trigger_wb
                .fetch_add(
                    go_i64(ru.raftstore_store_write_trigger_wb_bytes),
                    Ordering::Relaxed,
                );
        }
        if let Some(inputs) = &ru.executor_inputs {
            let add_work = |label: &str, v: u64| {
                if v == 0 {
                    return;
                }
                self.ensure_extra()
                    .tikv_coprocessor_work_total
                    .add(label, go_i64(v));
            };
            add_work(
                "BatchIndexScan",
                inputs.tikv_coprocessor_executor_work_total_batch_index_scan,
            );
            add_work(
                "BatchTableScan",
                inputs.tikv_coprocessor_executor_work_total_batch_table_scan,
            );
            add_work(
                "BatchSelection",
                inputs.tikv_coprocessor_executor_work_total_batch_selection,
            );
            add_work(
                "BatchTopN",
                inputs.tikv_coprocessor_executor_work_total_batch_top_n,
            );
            add_work(
                "BatchLimit",
                inputs.tikv_coprocessor_executor_work_total_batch_limit,
            );
            add_work(
                "BatchSimpleAggr",
                inputs.tikv_coprocessor_executor_work_total_batch_simple_aggr,
            );
            add_work(
                "BatchFastHashAggr",
                inputs.tikv_coprocessor_executor_work_total_batch_fast_hash_aggr,
            );
        }
    }

    /// Go `Merge`: merges another metrics container into the receiver.
    pub fn merge(&self, other: &RuV2Metrics) {
        if self.bypass() || other.bypass() {
            return;
        }
        self.result_chunk_cells
            .fetch_add(other.result_chunk_cells(), Ordering::Relaxed);
        other.executor_l1.clone_into(&self.executor_l1);
        self.plan_cnt.fetch_add(other.plan_cnt(), Ordering::Relaxed);
        self.session_parser_total
            .fetch_add(other.session_parser_total(), Ordering::Relaxed);
        self.txn_cnt.fetch_add(other.txn_cnt(), Ordering::Relaxed);
        self.resource_manager_read_cnt
            .fetch_add(other.resource_manager_read_cnt(), Ordering::Relaxed);
        self.tikv_kv_engine_cache_miss
            .fetch_add(other.tikv_kv_engine_cache_miss(), Ordering::Relaxed);
        self.tikv_storage_processed_keys_batch_get.fetch_add(
            other.tikv_storage_processed_keys_batch_get(),
            Ordering::Relaxed,
        );
        self.tikv_storage_processed_keys_get
            .fetch_add(other.tikv_storage_processed_keys_get(), Ordering::Relaxed);
        if let Some(extra) = other.load_extra() {
            extra.clone_into(self.ensure_extra());
        }
    }

    /// Go `ResultChunkCells`.
    #[must_use]
    pub fn result_chunk_cells(&self) -> i64 {
        self.result_chunk_cells.load(Ordering::Relaxed)
    }

    /// Go `ExecutorL5InsertRows`.
    #[must_use]
    pub fn executor_l5_insert_rows(&self) -> i64 {
        self.load_extra().map_or(0, |extra| {
            extra.executor_l5_insert_rows.load(Ordering::Relaxed)
        })
    }

    /// Go `PlanCnt`.
    #[must_use]
    pub fn plan_cnt(&self) -> i64 {
        self.plan_cnt.load(Ordering::Relaxed)
    }

    /// Go `PlanDeriveStatsPaths`.
    #[must_use]
    pub fn plan_derive_stats_paths(&self) -> i64 {
        self.load_extra().map_or(0, |extra| {
            extra.plan_derive_stats_paths.load(Ordering::Relaxed)
        })
    }

    /// Go `SessionParserTotal`.
    #[must_use]
    pub fn session_parser_total(&self) -> i64 {
        self.session_parser_total.load(Ordering::Relaxed)
    }

    /// Go `TxnCnt`.
    #[must_use]
    pub fn txn_cnt(&self) -> i64 {
        self.txn_cnt.load(Ordering::Relaxed)
    }

    /// Go `ResourceManagerReadCnt`.
    #[must_use]
    pub fn resource_manager_read_cnt(&self) -> i64 {
        self.resource_manager_read_cnt.load(Ordering::Relaxed)
    }

    /// Go `ResourceManagerWriteCnt`.
    #[must_use]
    pub fn resource_manager_write_cnt(&self) -> i64 {
        self.load_extra().map_or(0, |extra| {
            extra.resource_manager_write_cnt.load(Ordering::Relaxed)
        })
    }

    /// Go `WriteKeys`.
    #[must_use]
    pub fn write_keys(&self) -> i64 {
        self.load_extra()
            .map_or(0, |extra| extra.write_keys.load(Ordering::Relaxed))
    }

    /// Go `WriteSize`.
    #[must_use]
    pub fn write_size(&self) -> i64 {
        self.load_extra()
            .map_or(0, |extra| extra.write_size.load(Ordering::Relaxed))
    }

    /// Go `TiKVKVEngineCacheMiss`.
    #[must_use]
    pub fn tikv_kv_engine_cache_miss(&self) -> i64 {
        self.tikv_kv_engine_cache_miss.load(Ordering::Relaxed)
    }

    /// Go `TiKVCoprocessorExecutorIterations`.
    #[must_use]
    pub fn tikv_coprocessor_executor_iterations(&self) -> i64 {
        self.load_extra().map_or(0, |extra| {
            extra
                .tikv_coprocessor_executor_iterations
                .load(Ordering::Relaxed)
        })
    }

    /// Go `TiKVCoprocessorResponseBytes`.
    #[must_use]
    pub fn tikv_coprocessor_response_bytes(&self) -> i64 {
        self.load_extra().map_or(0, |extra| {
            extra
                .tikv_coprocessor_response_bytes
                .load(Ordering::Relaxed)
        })
    }

    /// Go `TiKVRaftstoreStoreWriteTriggerWB`.
    #[must_use]
    pub fn tikv_raftstore_store_write_trigger_wb(&self) -> i64 {
        self.load_extra().map_or(0, |extra| {
            extra
                .tikv_raftstore_store_write_trigger_wb
                .load(Ordering::Relaxed)
        })
    }

    /// Go `TiKVStorageProcessedKeysBatchGet`.
    #[must_use]
    pub fn tikv_storage_processed_keys_batch_get(&self) -> i64 {
        self.tikv_storage_processed_keys_batch_get
            .load(Ordering::Relaxed)
    }

    /// Go `TiKVStorageProcessedKeysGet`.
    #[must_use]
    pub fn tikv_storage_processed_keys_get(&self) -> i64 {
        self.tikv_storage_processed_keys_get.load(Ordering::Relaxed)
    }

    /// Go `IsZero`: checks whether all metrics are zero (a bypassed
    /// container counts as zero).
    #[must_use]
    pub fn is_zero(&self) -> bool {
        if self.bypass() {
            return true;
        }
        if self.result_chunk_cells() != 0
            || !self.executor_l1.is_zero()
            || self.plan_cnt() != 0
            || self.session_parser_total() != 0
            || self.txn_cnt() != 0
            || self.resource_manager_read_cnt() != 0
            || self.tikv_kv_engine_cache_miss() != 0
            || self.tikv_storage_processed_keys_batch_get() != 0
            || self.tikv_storage_processed_keys_get() != 0
        {
            return false;
        }
        match self.load_extra() {
            None => true,
            Some(extra) => {
                extra.executor_l2.sum() == 0
                    && extra.executor_l3.sum() == 0
                    && self.executor_l5_insert_rows() == 0
                    && self.plan_derive_stats_paths() == 0
                    && self.resource_manager_write_cnt() == 0
                    && self.write_keys() == 0
                    && self.write_size() == 0
                    && self.tikv_coprocessor_executor_iterations() == 0
                    && self.tikv_coprocessor_response_bytes() == 0
                    && self.tikv_raftstore_store_write_trigger_wb() == 0
                    && extra.tikv_coprocessor_work_total.sum() == 0
            }
        }
    }

    /// Go `CalculateRUValues`: calculates the current TiDB RU from the
    /// metrics using the provided weights (0 when bypassed; Go's nil
    /// receiver is handled by [`total_ru`]).
    #[must_use]
    pub fn calculate_ru_values(&self, weights: RuV2Weights) -> f64 {
        if self.bypass() {
            return 0.0;
        }
        self.calculate_ru_values_with_weights(weights)
    }

    /// Go `calculateRUValuesWithWeights`.
    #[expect(clippy::cast_precision_loss, reason = "Go float64(int64) conversion")]
    fn calculate_ru_values_with_weights(&self, weights: RuV2Weights) -> f64 {
        let mut executor_l2 = 0i64;
        let mut executor_l3 = 0i64;
        let mut executor_l5_insert_rows = 0i64;
        let mut plan_derive_stats_paths = 0i64;
        let mut resource_manager_write_cnt = 0i64;
        let mut write_keys = 0i64;
        if let Some(extra) = self.load_extra() {
            executor_l2 = extra.executor_l2.sum();
            executor_l3 = extra.executor_l3.sum();
            executor_l5_insert_rows = extra.executor_l5_insert_rows.load(Ordering::Relaxed);
            plan_derive_stats_paths = extra.plan_derive_stats_paths.load(Ordering::Relaxed);
            resource_manager_write_cnt = extra.resource_manager_write_cnt.load(Ordering::Relaxed);
            write_keys = extra.write_keys.load(Ordering::Relaxed);
        }
        let tidb_ru_float = self.result_chunk_cells() as f64 * weights.result_chunk_cells
            + self.executor_l1.sum() as f64 * weights.executor_l1
            + executor_l2 as f64 * weights.executor_l2
            + executor_l3 as f64 * weights.executor_l3
            + executor_l5_insert_rows as f64 * weights.executor_l5_insert_rows
            + self.plan_cnt() as f64 * weights.plan_cnt
            + plan_derive_stats_paths as f64 * weights.plan_derive_stats_paths
            + self.resource_manager_read_cnt() as f64 * weights.resource_manager_read_cnt
            + resource_manager_write_cnt as f64 * weights.resource_manager_write_cnt
            + write_keys as f64 * weights.write_keys
            + self.session_parser_total() as f64 * weights.session_parser_total
            + self.txn_cnt() as f64 * weights.txn_cnt;

        tidb_ru_float * weights.ru_scale
    }

    /// The plain-value view of the live counters, in exactly the shape Go
    /// `FormatRUV2Summary` snapshots them: the bridge onto
    /// [`crate::slow_log_format`]'s [`RuV2MetricsSnapshot`].
    #[must_use]
    pub fn snapshot(&self) -> RuV2MetricsSnapshot {
        let mut snapshot = RuV2MetricsSnapshot {
            bypass: self.bypass(),
            result_chunk_cells: self.result_chunk_cells(),
            executor_l1: self.executor_l1.snapshot(),
            plan_cnt: self.plan_cnt(),
            session_parser_total: self.session_parser_total(),
            txn_cnt: self.txn_cnt(),
            resource_manager_read_cnt: self.resource_manager_read_cnt(),
            tikv_kv_engine_cache_miss: self.tikv_kv_engine_cache_miss(),
            tikv_storage_processed_keys_batch_get: self.tikv_storage_processed_keys_batch_get(),
            tikv_storage_processed_keys_get: self.tikv_storage_processed_keys_get(),
            ..RuV2MetricsSnapshot::default()
        };
        if let Some(extra) = self.load_extra() {
            extra.executor_l2.snapshot_into(&mut snapshot.executor_l2);
            extra.executor_l3.snapshot_into(&mut snapshot.executor_l3);
            snapshot.executor_l5_insert_rows =
                extra.executor_l5_insert_rows.load(Ordering::Relaxed);
            snapshot.plan_derive_stats_paths =
                extra.plan_derive_stats_paths.load(Ordering::Relaxed);
            snapshot.resource_manager_write_cnt =
                extra.resource_manager_write_cnt.load(Ordering::Relaxed);
            snapshot.write_keys = extra.write_keys.load(Ordering::Relaxed);
            snapshot.write_size = extra.write_size.load(Ordering::Relaxed);
            snapshot.tikv_coprocessor_executor_iterations = extra
                .tikv_coprocessor_executor_iterations
                .load(Ordering::Relaxed);
            snapshot.tikv_coprocessor_response_bytes = extra
                .tikv_coprocessor_response_bytes
                .load(Ordering::Relaxed);
            snapshot.tikv_raftstore_store_write_trigger_wb = extra
                .tikv_raftstore_store_write_trigger_wb
                .load(Ordering::Relaxed);
            extra
                .tikv_coprocessor_work_total
                .snapshot_into(&mut snapshot.tikv_coprocessor_executor_work_total);
        }
        snapshot
    }
}

/// Go `Clone`: a copy of the current metrics for reporting (Go's nil-in
/// nil-out is the caller's `Option`).
impl Clone for RuV2Metrics {
    fn clone(&self) -> Self {
        let cloned = RuV2Metrics::new();
        cloned.bypass.store(self.bypass(), Ordering::Relaxed);
        cloned
            .result_chunk_cells
            .store(self.result_chunk_cells(), Ordering::Relaxed);
        self.executor_l1.clone_into(&cloned.executor_l1);
        cloned.plan_cnt.store(self.plan_cnt(), Ordering::Relaxed);
        cloned
            .session_parser_total
            .store(self.session_parser_total(), Ordering::Relaxed);
        cloned.txn_cnt.store(self.txn_cnt(), Ordering::Relaxed);
        cloned
            .resource_manager_read_cnt
            .store(self.resource_manager_read_cnt(), Ordering::Relaxed);
        cloned
            .tikv_kv_engine_cache_miss
            .store(self.tikv_kv_engine_cache_miss(), Ordering::Relaxed);
        cloned.tikv_storage_processed_keys_batch_get.store(
            self.tikv_storage_processed_keys_batch_get(),
            Ordering::Relaxed,
        );
        cloned
            .tikv_storage_processed_keys_get
            .store(self.tikv_storage_processed_keys_get(), Ordering::Relaxed);
        if let Some(extra) = self.load_extra() {
            extra.clone_into(cloned.ensure_extra());
        }
        cloned
    }
}

/// Go `TotalRU`: the statement RU v2 total as TiDB + TiKV + TiFlash, on a
/// possibly-nil (Go) receiver.
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

/// Go `UpdateRUV2MetricsFromRUV2`: adds raw RUv2 counters into the
/// statement-level metrics snapshot.
pub fn update_ruv2_metrics_from_ruv2(metrics: Option<&RuV2Metrics>, ru: Option<&Ruv2>) {
    let (Some(metrics), Some(ru)) = (metrics, ru) else {
        return;
    };
    if metrics.bypass() {
        return;
    }
    metrics.apply_raw_counters(ru);
}

/// Go `SyncRUV2MetricsFromRUDetails`: drains the raw RUv2 counters
/// accumulated in `RUDetails` since the last drain and adds them into the
/// statement-level metrics. Safe to call multiple times; each call transfers
/// only the delta.
pub fn sync_ruv2_metrics_from_ru_details(
    metrics: Option<&RuV2Metrics>,
    ru_details: Option<&RuDetails>,
) {
    let (Some(metrics), Some(ru_details)) = (metrics, ru_details) else {
        return;
    };
    if metrics.bypass() {
        return;
    }
    update_ruv2_metrics_from_ruv2(Some(metrics), ru_details.drain_ruv2().as_ref());
}

/// Go `UpdateRUV2MetricsFromCommitDetails`: adds commit write counters into
/// RUv2 metrics.
pub fn update_ruv2_metrics_from_commit_details(
    metrics: Option<&RuV2Metrics>,
    commit_details: Option<&CommitDetails>,
) {
    let (Some(metrics), Some(commit_details)) = (metrics, commit_details) else {
        return;
    };
    if metrics.bypass() {
        return;
    }
    if commit_details.write_keys != 0 {
        metrics.add_write_keys(commit_details.write_keys);
    }
    if commit_details.write_size != 0 {
        metrics.add_write_size(commit_details.write_size);
    }
}

/// Go `ExecutorMetricRecorder`: a pre-resolved recorder for one hot L1
/// executor metric. The zero value records nothing. (Go also carries a
/// pre-resolved `prometheus.Counter`; narrowed away here — and where Go's
/// `Record` on the zero value would nil-panic, this one is a no-op.)
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorMetricRecorder {
    kind: ExecL1Kind,
}

impl ExecutorMetricRecorder {
    /// Go `Available`: reports whether this recorder was resolved.
    #[must_use]
    pub fn available(&self) -> bool {
        self.kind != ExecL1Kind::None
    }

    /// Go `Record`: applies `delta`. Caller must ensure `m` is not bypassed.
    pub fn record(&self, m: &RuV2Metrics, delta: i64) {
        if let Some(field) = m.executor_l1.field_by_kind(self.kind) {
            field.fetch_add(delta, Ordering::Relaxed);
        }
    }
}

/// Go `ResolveExecutorMetric`: returns a pre-resolved recorder for hot L1
/// executor labels, or the zero recorder for everything else. (Go's final
/// counter-nil check against `metrics.RUV2ExecutorCounter` is narrowed away;
/// it never trips for level-1 labels there.)
#[must_use]
pub fn resolve_executor_metric(level: i64, label: &str) -> ExecutorMetricRecorder {
    if level != 1 {
        return ExecutorMetricRecorder::default();
    }
    ExecutorMetricRecorder {
        kind: exec_l1_kind_for_label(label),
    }
}

/// Go `FormatRUV2Summary`: formats the RUv2 total and detailed metrics in
/// one pass, delegating the byte-exact rendering to
/// [`crate::slow_log_format::format_ruv2_summary`] over [`RuV2Metrics::snapshot`].
#[must_use]
pub fn format_ruv2_summary(
    metrics: Option<&RuV2Metrics>,
    weights: RuV2Weights,
    tikv_ru: f64,
    tiflash_ru: f64,
) -> (String, String) {
    let snapshot = metrics.map(RuV2Metrics::snapshot);
    crate::slow_log_format::format_ruv2_summary(snapshot.as_ref(), &weights, tikv_ru, tiflash_ru)
}

/// Go `FormatRUV2Total`: formats the RUv2 total into a slow log string.
#[must_use]
pub fn format_ruv2_total(
    metrics: Option<&RuV2Metrics>,
    weights: RuV2Weights,
    tikv_ru: f64,
    tiflash_ru: f64,
) -> String {
    format_ruv2_summary(metrics, weights, tikv_ru, tiflash_ru).0
}

/// Go `FormatRUV2Metrics`: formats RUv2 metrics into a compact detail string.
#[must_use]
pub fn format_ruv2_metrics(
    metrics: Option<&RuV2Metrics>,
    weights: RuV2Weights,
    tikv_ru: f64,
    tiflash_ru: f64,
) -> String {
    format_ruv2_summary(metrics, weights, tikv_ru, tiflash_ru).1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `defaultRUV2WeightsForTest`, with the values of Go
    /// `config.DefaultRUV2Config()` copied as exact literals (the config
    /// package is not ported here).
    fn default_ruv2_weights_for_test() -> RuV2Weights {
        RuV2Weights {
            ru_scale: 2.01,
            result_chunk_cells: 0.000_100_00,
            executor_l1: 0.000_132_78,
            executor_l2: 0.000_003_83,
            executor_l3: 0.001_417_39,
            executor_l5_insert_rows: 0.004_725_72,
            plan_cnt: 0.153_922_17,
            plan_derive_stats_paths: 0.249_681_82,
            resource_manager_read_cnt: 0.020_720_03,
            resource_manager_write_cnt: 0.071_797_79,
            write_keys: 0.330_760_861_554_226,
            session_parser_total: 0.192_304_99,
            txn_cnt: 0.030_137_09,
        }
    }

    /// Go `require.InEpsilon`: relative error of `actual` against `expected`
    /// must not exceed `epsilon`.
    fn assert_in_epsilon(expected: f64, actual: f64, epsilon: f64) {
        let rel = ((expected - actual) / expected).abs();
        assert!(
            rel <= epsilon,
            "expected {expected}, got {actual} (relative error {rel} > {epsilon})"
        );
    }

    /// Port of Go `TestRUV2MetricsSnapshotCalculateRUValues`, including its
    /// "zero scale stays zero", "bypass keeps total zero", and "nil metrics
    /// keep tikv and tiflash ru" subtests. The Go "known executor labels
    /// avoid per statement map allocations" subtest (`testing.AllocsPerRun`)
    /// is not portable and is skipped.
    #[test]
    fn ruv2_metrics_snapshot_calculate_ru_values() {
        let weights = default_ruv2_weights_for_test();
        let metrics = RuV2Metrics::new();
        metrics.add_result_chunk_cells(1000);
        metrics.add_executor_metric(1, "TableReader", 5);
        metrics.add_executor_metric(1, "Projection", 7);
        metrics.add_executor_metric(2, "Selection", 11);
        metrics.add_executor_metric(3, "HashJoin", 13);
        metrics.add_executor_l5_insert_rows(17);
        metrics.add_plan_cnt(19);
        metrics.add_plan_derive_stats_paths(23);
        metrics.add_resource_manager_read_cnt(29);
        metrics.add_resource_manager_write_cnt(31);
        metrics.add_write_keys(3);
        metrics.add_write_size(66);
        metrics.add_session_parser_total(37);
        metrics.add_txn_cnt(41);
        metrics.add_tikv_kv_engine_cache_miss(43);
        metrics.add_tikv_coprocessor_work_total("BatchSelection", 53);
        metrics.add_tikv_coprocessor_work_total("BatchTopN", 59);
        metrics.add_tikv_coprocessor_executor_iterations(61);
        metrics.add_tikv_coprocessor_response_bytes(67);
        metrics.add_tikv_raftstore_store_write_trigger_wb(71);
        metrics.add_tikv_storage_processed_keys_batch_get(73);
        metrics.add_tikv_storage_processed_keys_get(79);

        let tidb_ru = metrics.calculate_ru_values(weights);
        let tikv_ru = 157_258.0_f64;
        let tiflash_ru = 24_680.0_f64;
        let total = total_ru(Some(&metrics), weights, tikv_ru, tiflash_ru);
        assert_in_epsilon(42.285_178_330_9, tidb_ru, 0.01);
        assert_in_epsilon(157_258.0, tikv_ru, 0.01);
        assert_in_epsilon(24_680.0, tiflash_ru, 0.01);
        assert_in_epsilon(181_980.285_178_330_9, total, 0.01);
        assert_eq!(3, metrics.write_keys());
        assert_eq!(66, metrics.write_size());

        // Go subtest "zero scale stays zero".
        let mut zero_scale_weights = weights;
        zero_scale_weights.ru_scale = 0.0;
        assert_eq!(0.0, metrics.calculate_ru_values(zero_scale_weights));
        assert_eq!(
            tikv_ru + tiflash_ru,
            total_ru(Some(&metrics), zero_scale_weights, tikv_ru, tiflash_ru)
        );

        // Go subtest "bypass keeps total zero".
        let bypassed = RuV2Metrics::new();
        bypassed.set_bypass(true);
        bypassed.add_result_chunk_cells(1000);
        bypassed.add_plan_cnt(2);

        assert_eq!(0.0, bypassed.calculate_ru_values(weights));
        assert_eq!(0.0, total_ru(Some(&bypassed), weights, tikv_ru, tiflash_ru));
        let (total, detail) = format_ruv2_summary(Some(&bypassed), weights, tikv_ru, tiflash_ru);
        assert!(total.is_empty());
        assert!(detail.is_empty());

        // Go subtest "nil metrics keep tikv and tiflash ru".
        assert_eq!(
            tikv_ru + tiflash_ru,
            total_ru(None, weights, tikv_ru, tiflash_ru)
        );
    }

    /// Port of Go `TestUpdateRUV2MetricsFromCommitDetails`.
    #[test]
    fn update_ruv2_metrics_from_commit_details_test() {
        let metrics = RuV2Metrics::new();
        let weights = default_ruv2_weights_for_test();
        let before_ru = metrics.calculate_ru_values(weights);

        update_ruv2_metrics_from_commit_details(
            Some(&metrics),
            Some(&CommitDetails {
                write_keys: 3,
                write_size: 66,
                ..CommitDetails::default()
            }),
        );

        assert_eq!(3, metrics.write_keys());
        assert_eq!(66, metrics.write_size());
        assert_in_epsilon(
            before_ru + 3.0 * weights.write_keys * weights.ru_scale,
            metrics.calculate_ru_values(weights),
            0.01,
        );

        let detail = format_ruv2_metrics(Some(&metrics), weights, 0.0, 0.0);
        assert!(detail.contains("write_keys:3"), "detail: {detail}");
        assert!(detail.contains("write_size:66"), "detail: {detail}");

        let bypassed = RuV2Metrics::new();
        bypassed.set_bypass(true);
        update_ruv2_metrics_from_commit_details(
            Some(&bypassed),
            Some(&CommitDetails {
                write_keys: 1,
                write_size: 2,
                ..CommitDetails::default()
            }),
        );
        assert_eq!(0, bypassed.write_keys());
        assert_eq!(0, bypassed.write_size());
    }

    /// Port of Go `TestRUV2MetricsSnapshotFreezesRUValues`.
    #[test]
    fn ruv2_metrics_snapshot_freezes_ru_values() {
        let weights = default_ruv2_weights_for_test();
        let metrics = RuV2Metrics::new();
        metrics.add_result_chunk_cells(1000);
        metrics.add_plan_cnt(2);

        let baseline = metrics.calculate_ru_values(weights);

        let mut updated = weights;
        updated.result_chunk_cells *= 10.0;
        updated.plan_cnt *= 10.0;

        assert_ne!(baseline, metrics.calculate_ru_values(updated));
    }

    /// Port of Go `TestUpdateRUV2MetricsFromRUV2`.
    #[test]
    fn update_ruv2_metrics_from_ruv2_test() {
        let metrics = RuV2Metrics::new();
        update_ruv2_metrics_from_ruv2(
            Some(&metrics),
            Some(&Ruv2 {
                read_rpc_count: 2,
                write_rpc_count: 3,
                kv_engine_cache_miss: 5,
                coprocessor_executor_iterations: 7,
                coprocessor_response_bytes: 11,
                raftstore_store_write_trigger_wb_bytes: 13,
                storage_processed_keys_batch_get: 17,
                storage_processed_keys_get: 19,
                executor_inputs: Some(ExecutorInputs {
                    tikv_coprocessor_executor_work_total_batch_index_scan: 23,
                    tikv_coprocessor_executor_work_total_batch_table_scan: 29,
                    tikv_coprocessor_executor_work_total_batch_selection: 31,
                    tikv_coprocessor_executor_work_total_batch_top_n: 37,
                    tikv_coprocessor_executor_work_total_batch_limit: 41,
                    tikv_coprocessor_executor_work_total_batch_simple_aggr: 43,
                    tikv_coprocessor_executor_work_total_batch_fast_hash_aggr: 47,
                }),
            }),
        );
        assert_eq!(2, metrics.resource_manager_read_cnt());
        assert_eq!(3, metrics.resource_manager_write_cnt());
        assert_eq!(5, metrics.tikv_kv_engine_cache_miss());
        assert_eq!(7, metrics.tikv_coprocessor_executor_iterations());
        assert_eq!(11, metrics.tikv_coprocessor_response_bytes());
        assert_eq!(13, metrics.tikv_raftstore_store_write_trigger_wb());
        assert_eq!(17, metrics.tikv_storage_processed_keys_batch_get());
        assert_eq!(19, metrics.tikv_storage_processed_keys_get());

        let detail = format_ruv2_metrics(Some(&metrics), default_ruv2_weights_for_test(), 0.0, 0.0);
        assert!(
            detail.contains("resource_manager_read_cnt:2"),
            "detail: {detail}"
        );
        assert!(
            detail.contains("resource_manager_write_cnt:3"),
            "detail: {detail}"
        );
        assert!(
            detail.contains("tikv_storage_processed_keys_batch_get:17"),
            "detail: {detail}"
        );
        assert!(
            detail.contains("tikv_storage_processed_keys_get:19"),
            "detail: {detail}"
        );
        assert!(detail.contains("BatchFastHashAggr:47"), "detail: {detail}");
    }

    /// Port of Go `TestSyncRUV2MetricsFromRUDetailsIncremental`.
    #[test]
    fn sync_ruv2_metrics_from_ru_details_incremental() {
        let metrics = RuV2Metrics::new();
        let ru_details = RuDetails::new();
        ru_details.add_ruv2(&Ruv2 {
            read_rpc_count: 2,
            write_rpc_count: 3,
            kv_engine_cache_miss: 5,
            raftstore_store_write_trigger_wb_bytes: 17,
            storage_processed_keys_batch_get: 7,
            storage_processed_keys_get: 19,
            executor_inputs: Some(ExecutorInputs {
                tikv_coprocessor_executor_work_total_batch_index_scan: 11,
                tikv_coprocessor_executor_work_total_batch_fast_hash_aggr: 23,
                ..ExecutorInputs::default()
            }),
            ..Ruv2::default()
        });

        // First drain picks up all counters.
        sync_ruv2_metrics_from_ru_details(Some(&metrics), Some(&ru_details));
        assert_eq!(2, metrics.resource_manager_read_cnt());
        assert_eq!(3, metrics.resource_manager_write_cnt());
        assert_eq!(5, metrics.tikv_kv_engine_cache_miss());
        assert_eq!(17, metrics.tikv_raftstore_store_write_trigger_wb());
        assert_eq!(7, metrics.tikv_storage_processed_keys_batch_get());
        assert_eq!(19, metrics.tikv_storage_processed_keys_get());

        // Second drain without new data is a no-op.
        sync_ruv2_metrics_from_ru_details(Some(&metrics), Some(&ru_details));
        assert_eq!(2, metrics.resource_manager_read_cnt());
        assert_eq!(3, metrics.resource_manager_write_cnt());

        // New counters accumulate after the first drain.
        ru_details.add_ruv2(&Ruv2 {
            read_rpc_count: 10,
            storage_processed_keys_batch_get: 100,
            ..Ruv2::default()
        });
        sync_ruv2_metrics_from_ru_details(Some(&metrics), Some(&ru_details));
        assert_eq!(12, metrics.resource_manager_read_cnt());
        assert_eq!(107, metrics.tikv_storage_processed_keys_batch_get());

        let detail = format_ruv2_metrics(Some(&metrics), default_ruv2_weights_for_test(), 0.0, 0.0);
        assert!(
            detail.contains("resource_manager_read_cnt:12"),
            "detail: {detail}"
        );
        assert!(
            detail.contains("resource_manager_write_cnt:3"),
            "detail: {detail}"
        );
        assert!(
            detail.contains("tikv_storage_processed_keys_batch_get:107"),
            "detail: {detail}"
        );
        assert!(
            detail.contains("tikv_storage_processed_keys_get:19"),
            "detail: {detail}"
        );
        assert!(detail.contains("BatchIndexScan:11"), "detail: {detail}");
        assert!(detail.contains("BatchFastHashAggr:23"), "detail: {detail}");
    }

    /// Port of Go `TestSyncRUV2MetricsFromRUDetailsBypass`.
    #[test]
    fn sync_ruv2_metrics_from_ru_details_bypass() {
        let metrics = RuV2Metrics::new();
        metrics.set_bypass(true);
        let ru_details = RuDetails::new();
        ru_details.add_ruv2(&Ruv2 {
            storage_processed_keys_batch_get: 7,
            ..Ruv2::default()
        });

        sync_ruv2_metrics_from_ru_details(Some(&metrics), Some(&ru_details));
        assert_eq!(0, metrics.resource_manager_read_cnt());
        assert_eq!(0, metrics.resource_manager_write_cnt());
        assert_eq!(0, metrics.tikv_storage_processed_keys_batch_get());
    }

    /// Port of Go `TestUpdateRUV2MetricsFromRUV2Bypass`.
    #[test]
    fn update_ruv2_metrics_from_ruv2_bypass() {
        let metrics = RuV2Metrics::new();
        metrics.set_bypass(true);
        update_ruv2_metrics_from_ruv2(
            Some(&metrics),
            Some(&Ruv2 {
                read_rpc_count: 1,
                write_rpc_count: 1,
                storage_processed_keys_batch_get: 1,
                ..Ruv2::default()
            }),
        );
        assert_eq!(0, metrics.resource_manager_read_cnt());
        assert_eq!(0, metrics.resource_manager_write_cnt());
        assert_eq!(0, metrics.tikv_storage_processed_keys_batch_get());
    }

    /// Port of Go `TestExecutorMetricRecorderFastPath`.
    #[test]
    fn executor_metric_recorder_fast_path() {
        for label in [
            RUV2_LABEL_BATCH_POINT_GET_EXEC,
            RUV2_LABEL_POINT_GET_EXECUTOR,
            RUV2_LABEL_LIMIT_EXEC,
        ] {
            assert!(resolve_executor_metric(1, label).available(), "{label}");
        }

        assert!(!resolve_executor_metric(1, "Unknown").available());
        assert!(!resolve_executor_metric(2, "HashAggExec").available());
        assert!(!resolve_executor_metric(3, "SortExec").available());
        assert!(!resolve_executor_metric(0, RUV2_LABEL_BATCH_POINT_GET_EXEC).available());

        let zero = ExecutorMetricRecorder::default();
        assert!(!zero.available());

        let fast = RuV2Metrics::new();
        let slow = RuV2Metrics::new();
        resolve_executor_metric(1, RUV2_LABEL_BATCH_POINT_GET_EXEC).record(&fast, 7);
        resolve_executor_metric(1, RUV2_LABEL_POINT_GET_EXECUTOR).record(&fast, 3);
        resolve_executor_metric(1, RUV2_LABEL_LIMIT_EXEC).record(&fast, 5);
        slow.add_executor_metric(1, RUV2_LABEL_BATCH_POINT_GET_EXEC, 7);
        slow.add_executor_metric(1, RUV2_LABEL_POINT_GET_EXECUTOR, 3);
        slow.add_executor_metric(1, RUV2_LABEL_LIMIT_EXEC, 5);

        assert_eq!(slow.executor_l1.snapshot(), fast.executor_l1.snapshot());
    }

    /// Port of Go `TestFormatRUV2MetricsIncludesRUValuesFirst`.
    #[test]
    fn format_ruv2_metrics_includes_ru_values_first() {
        let weights = default_ruv2_weights_for_test();
        let metrics = RuV2Metrics::new();
        metrics.add_result_chunk_cells(1000);
        metrics.add_resource_manager_write_cnt(20);
        metrics.add_tikv_coprocessor_work_total("BatchTopN", 10);
        let (total, formatted) = format_ruv2_summary(Some(&metrics), weights, 10987.0, 246.0);

        assert_eq!("11236.09", total);
        assert_eq!(
            total,
            format_ruv2_total(Some(&metrics), weights, 10987.0, 246.0)
        );
        assert_eq!(
            formatted,
            format_ruv2_metrics(Some(&metrics), weights, 10987.0, 246.0)
        );
        assert!(formatted.contains("tidb_ru:"));
        assert!(formatted.contains("tikv_ru:"));
        assert!(formatted.contains("tiflash_ru:"));
        assert!(formatted.contains("total_ru:"));
        assert!(formatted.starts_with("total_ru:"));

        let parts: Vec<&str> = formatted.split(", ").collect();
        assert_eq!(7, parts.len(), "formatted: {formatted}");
        assert_eq!("total_ru:11236.09", parts[0]);
        assert_eq!("tidb_ru:3.09", parts[1]);
        assert_eq!("tikv_ru:10987.00", parts[2]);
        assert_eq!("tiflash_ru:246.00", parts[3]);
    }
}
