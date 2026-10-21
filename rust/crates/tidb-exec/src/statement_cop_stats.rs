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

//! The statement's consumer of coprocessor response details: what Go's
//! `selectResult.fetchResp` feeds `StmtCtx.ExecDetails` for every response a
//! select reads (`MergeCopExecDetails`), which the statement summary later
//! reads back as `CopTasksSummary` and `ExecDetails`.

use std::collections::HashMap;
use std::time::Duration;

use tidb_distsql::{CopResponseSink, ResponseRuntimeStats, SelectResultMetadata};
use tidb_proto::ExecutorExecutionSummary;

use crate::exec_details::{
    CopExecDetails, CopTasksDetails, CopTasksSummary, ExecDetails, SyncExecDetails,
};

/// Go `StmtCtx.mu.execDetails` as a [`CopResponseSink`].
#[derive(Debug, Default)]
pub struct StatementCopStats {
    exec_details: SyncExecDetails,
    /// Go `ctx.Value(util.RUDetailsCtxKey)` of the statement whose responses
    /// these are, which TiFlash's reported RU is merged into.
    ru_details: Option<std::sync::Arc<tikv_client::RuDetails>>,
}

impl StatementCopStats {
    /// The statement's sink, merging TiFlash RU into `ru_details`.
    #[must_use]
    pub fn with_ru_details(ru_details: std::sync::Arc<tikv_client::RuDetails>) -> Self {
        Self {
            exec_details: SyncExecDetails::default(),
            ru_details: Some(ru_details),
        }
    }

    /// Go `StmtCtx.GetExecDetails`.
    #[must_use]
    pub fn exec_details(&self) -> ExecDetails {
        self.exec_details.exec_details()
    }

    /// Go `StmtCtx.CopTasksDetails`: `None` when no cop task reported.
    #[must_use]
    pub fn cop_tasks_details(&self) -> Option<CopTasksDetails> {
        self.exec_details.cop_tasks_details()
    }

    /// Go `StmtCtx.CopTasksSummary`: `None` when no cop task reported.
    #[must_use]
    pub fn cop_tasks_summary(&self) -> Option<CopTasksSummary> {
        self.exec_details.cop_tasks_summary()
    }
}

impl CopResponseSink for StatementCopStats {
    fn record_cop_response(
        &self,
        metadata: &SelectResultMetadata,
        sample: &ResponseRuntimeStats,
        fetch_duration: Duration,
        execution_summaries: &[ExecutorExecutionSummary],
    ) {
        // Go `selectResult.updateCopRuntimeStats`: a TiFlash response's
        // summaries carry TiFlash's RU, merged into the statement's
        // `RUDetails` (`MergeTiFlashRUConsumption`). This node keeps no
        // statement RU v2 metrics, Go's nil-metrics case, so the merge always
        // runs. A summary that fails to decode leaves the details unchanged;
        // the sink has no error path for Go's returned error.
        if metadata.store_type == tidb_txnkv::StoreType::TiFlash {
            if let Some(ru_details) = &self.ru_details {
                let _ = crate::tiflash_stats::merge_tiflash_ru_consumption(
                    execution_summaries,
                    ru_details,
                );
            }
        }
        let backoff_sleep = sample
            .backoff_sleep_ns
            .iter()
            .map(|(kind, nanos)| (kind.clone(), Duration::from_nanos(*nanos)))
            .collect::<HashMap<_, _>>();
        let details = CopExecDetails {
            scan_detail: sample.scan_detail,
            time_detail: sample.time_detail,
            callee_address: sample.callee_address.clone(),
            backoff_time: backoff_sleep.values().sum(),
            backoff_times: backoff_sleep.keys().map(|kind| (kind.clone(), 1)).collect(),
            backoff_sleep,
        };
        self.exec_details
            .merge_cop_exec_details(Some(&details), fetch_duration);
    }
}

/// Go `selectResult.updateCopRuntimeStats` as a [`CopResponseSink`]: every
/// consumed response's execution summaries are recorded in the statement's
/// [`RuntimeStatsColl`](crate::runtime_stats::RuntimeStatsColl) under the
/// plan IDs the select result was built with (`copPlanIDs`), after the
/// statement's own exec-details consumer has seen the response.
pub struct RuntimeStatsCollSink {
    coll: std::sync::Arc<crate::runtime_stats::RuntimeStatsColl>,
    inner: Option<std::sync::Arc<dyn CopResponseSink>>,
}

impl std::fmt::Debug for RuntimeStatsCollSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RuntimeStatsCollSink").finish()
    }
}

impl RuntimeStatsCollSink {
    /// A sink recording into `coll` after `inner`.
    #[must_use]
    pub fn new(
        coll: std::sync::Arc<crate::runtime_stats::RuntimeStatsColl>,
        inner: Option<std::sync::Arc<dyn CopResponseSink>>,
    ) -> Self {
        Self { coll, inner }
    }
}

/// Go `kv.StoreType` of the select result, as the runtime-stats kind.
fn runtime_store_type(store_type: tidb_txnkv::StoreType) -> crate::runtime_stats::StoreType {
    if store_type == tidb_txnkv::StoreType::TiFlash {
        crate::runtime_stats::StoreType::TiFlash
    } else if store_type == tidb_txnkv::StoreType::TiDb {
        crate::runtime_stats::StoreType::TiDb
    } else if store_type == tidb_txnkv::StoreType::TiKv {
        crate::runtime_stats::StoreType::TiKv
    } else {
        crate::runtime_stats::StoreType::Unspecified
    }
}

impl CopResponseSink for RuntimeStatsCollSink {
    fn record_cop_response(
        &self,
        metadata: &SelectResultMetadata,
        sample: &ResponseRuntimeStats,
        fetch_duration: Duration,
        execution_summaries: &[ExecutorExecutionSummary],
    ) {
        if let Some(inner) = &self.inner {
            inner.record_cop_response(metadata, sample, fetch_duration, execution_summaries);
        }
        // Go `updateCopRuntimeStats`' gate.
        if metadata.root_plan_id.is_none_or(|plan_id| plan_id <= 0)
            || (sample.callee_address.is_empty() && !sample.request_rpc_stats_present)
        {
            return;
        }
        // Only TiKV's positional summaries are recorded here: TiFlash's
        // carry executor IDs and have their own arm.
        if metadata.store_type != tidb_txnkv::StoreType::TiKv {
            return;
        }
        let store_type = runtime_store_type(metadata.store_type);
        let plan_ids = &metadata.cop_plan_ids;
        let Some(&root_plan_id) = plan_ids.last() else {
            return;
        };
        let record_root = |summary| {
            if root_plan_id > 0 {
                self.coll.record_cop_stats(
                    root_plan_id as i64,
                    store_type,
                    sample.scan_detail.as_ref(),
                    sample.time_detail,
                    summary,
                );
            }
        };
        if execution_summaries.is_empty() {
            // The response contributes no operator rows, but its
            // request-root scan and time evidence remain.
            record_root(None);
            return;
        }
        let valid = |summary: &ExecutorExecutionSummary| {
            summary.time_processed_ns.is_some()
                && summary.num_produced_rows.is_some()
                && summary.num_iterations.is_some()
        };
        if execution_summaries.len() != plan_ids.len() || !execution_summaries.iter().all(valid) {
            return;
        }
        for (index, (summary, plan_id)) in execution_summaries.iter().zip(plan_ids).enumerate() {
            if index + 1 == plan_ids.len() {
                record_root(Some(summary));
            } else if *plan_id > 0 {
                self.coll
                    .record_one_cop_task(*plan_id as i64, store_type, summary);
            }
        }
    }
}

#[cfg(test)]
mod runtime_stats_coll_sink_tests {
    use super::*;
    use std::sync::Arc;
    use tidb_distsql::select_with_runtime_stats;
    use tidb_distsql::{SelectInput, StoreType};

    fn metadata(cop_plan_ids: Vec<isize>, root: isize) -> SelectResultMetadata {
        select_with_runtime_stats(
            SelectInput {
                store_type: StoreType::TiKv,
                ..SelectInput::default()
            },
            cop_plan_ids,
            root,
        )
    }

    fn summary(rows: u64, iterations: u64) -> ExecutorExecutionSummary {
        ExecutorExecutionSummary {
            time_processed_ns: Some(1_000),
            num_produced_rows: Some(rows),
            num_iterations: Some(iterations),
            ..ExecutorExecutionSummary::default()
        }
    }

    fn sample() -> ResponseRuntimeStats {
        ResponseRuntimeStats {
            callee_address: "127.0.0.1:20160".to_owned(),
            scan_detail: Some(tikv_client::util::ScanDetail {
                processed_keys: 12,
                processed_keys_size: 555,
                total_keys: 13,
                rocksdb_delete_skipped_count: 12,
                rocksdb_key_skipped_count: 24,
                ..tikv_client::util::ScanDetail::default()
            }),
            ..ResponseRuntimeStats::default()
        }
    }

    /// Go `selectResult.updateCopRuntimeStats` for a `Selection(TableScan)`
    /// reader: the scan (`copPlanIDs[0]`) reports only its summary, the root
    /// cop executor (`copPlanIDs[last]`) also the response's scan and time
    /// details (Go's `EXPLAIN ANALYZE` of `select * from t where b = 3`).
    #[test]
    fn positional_summaries_record_scan_detail_on_the_root_cop_executor() {
        let coll = Arc::new(crate::runtime_stats::RuntimeStatsColl::new(None));
        let sink = RuntimeStatsCollSink::new(Arc::clone(&coll), None);
        sink.record_cop_response(
            &metadata(vec![5, 6], 7),
            &sample(),
            Duration::ZERO,
            &[summary(12, 1), summary(1, 1)],
        );
        let scan = coll.get_cop_stats(5).unwrap();
        assert_eq!(scan.lock().unwrap().get_act_rows(), 12);
        assert_eq!(scan.lock().unwrap().string(), "tikv_task:{time:1µs, loops:1}");
        let selection = coll.get_cop_stats(6).unwrap();
        assert_eq!(selection.lock().unwrap().get_act_rows(), 1);
        assert_eq!(
            selection.lock().unwrap().string(),
            "tikv_task:{time:1µs, loops:1}, scan_detail: {total_process_keys: 12, \
             total_process_keys_size: 555, total_keys: 13, rocksdb: {delete_skipped_count: 12, \
             key_skipped_count: 24, block: {}}}"
        );
        assert!(coll.get_cop_stats(7).is_none());
    }

    /// Go's `malformed` arm: a summary slice that does not line up with
    /// `copPlanIDs` records nothing, and a response without summaries still
    /// records its request-root scan evidence.
    #[test]
    fn malformed_summaries_record_nothing_and_missing_ones_keep_root_evidence() {
        let coll = Arc::new(crate::runtime_stats::RuntimeStatsColl::new(None));
        let sink = RuntimeStatsCollSink::new(Arc::clone(&coll), None);
        sink.record_cop_response(
            &metadata(vec![5, 6], 7),
            &sample(),
            Duration::ZERO,
            &[summary(12, 1)],
        );
        assert!(coll.get_cop_stats(5).is_none());
        assert!(coll.get_cop_stats(6).is_none());
        sink.record_cop_response(&metadata(vec![5, 6], 7), &sample(), Duration::ZERO, &[]);
        assert!(coll.get_cop_stats(5).is_none());
        let root = coll.get_cop_stats(6).unwrap();
        assert!(root
            .lock()
            .unwrap()
            .string()
            .starts_with(", scan_detail: {total_process_keys: 12"));
        // Go's first gate: no root plan, nothing recorded.
        sink.record_cop_response(
            &metadata(vec![8], 0),
            &sample(),
            Duration::ZERO,
            &[summary(1, 1)],
        );
        assert!(coll.get_cop_stats(8).is_none());
    }
}
