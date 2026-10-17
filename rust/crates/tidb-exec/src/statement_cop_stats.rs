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

use crate::exec_details::{CopExecDetails, CopTasksSummary, ExecDetails, SyncExecDetails};

/// Go `StmtCtx.mu.execDetails` as a [`CopResponseSink`].
#[derive(Debug, Default)]
pub struct StatementCopStats {
    exec_details: SyncExecDetails,
}

impl StatementCopStats {
    /// Go `StmtCtx.GetExecDetails`.
    #[must_use]
    pub fn exec_details(&self) -> ExecDetails {
        self.exec_details.exec_details()
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
        _metadata: &SelectResultMetadata,
        sample: &ResponseRuntimeStats,
        fetch_duration: Duration,
        _execution_summaries: &[ExecutorExecutionSummary],
    ) {
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
