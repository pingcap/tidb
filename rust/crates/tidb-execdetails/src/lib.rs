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

//! Go `pkg/util/execdetails`' `RuntimeStats` interface, its `Tp*` kind
//! constants, `FormatDuration` and `RuntimeStatsWithConcurrencyInfo`: what an
//! executor registers on the statement's `RuntimeStatsColl`
//! ([`crate::executor::ExecutorRuntimeStatsSource::register_stats`]). The
//! collection itself and the cop-side stats live in `tidb_execdetails::runtime_stats`.

pub mod ruv2_metrics;
pub mod exec_details;
pub mod runtime_stats;
pub mod tiflash_stats;

/// The cop-side runtime info of one plan id: its actual rows and display
/// text (Go `CopRuntimeStats.GetActRows` and `String`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopRuntimeInfo {
    /// Go `CopRuntimeStats.GetActRows`.
    pub act_rows: i64,
    /// Go `CopRuntimeStats.String`.
    pub info: String,
}

use std::any::Any;
use std::sync::Mutex;
use std::time::Duration as StdDuration;

/// Go `TpBasicRuntimeStats`: the tp for `BasicRuntimeStats`.
pub const TP_BASIC_RUNTIME_STATS: i32 = 0;
/// Go `TpRuntimeStatsWithCommit`: the tp for `RuntimeStatsWithCommit`.
pub const TP_RUNTIME_STATS_WITH_COMMIT: i32 = 1;
/// Go `TpRuntimeStatsWithConcurrencyInfo`: the tp for
/// `RuntimeStatsWithConcurrencyInfo`.
pub const TP_RUNTIME_STATS_WITH_CONCURRENCY_INFO: i32 = 2;
/// Go `TpSnapshotRuntimeStats`: the tp for `SnapshotRuntimeStats`.
pub const TP_SNAPSHOT_RUNTIME_STATS: i32 = 3;
/// Go `TpHashJoinRuntimeStats`: the tp for `HashJoinRuntimeStats`.
pub const TP_HASH_JOIN_RUNTIME_STATS: i32 = 4;
/// Go `TpHashJoinRuntimeStatsV2`: the tp for `hashJoinRuntimeStatsV2`.
pub const TP_HASH_JOIN_RUNTIME_STATS_V2: i32 = 5;
/// Go `TpIndexLookUpJoinRuntimeStats`: the tp for
/// `IndexLookUpJoinRuntimeStats`.
pub const TP_INDEX_LOOK_UP_JOIN_RUNTIME_STATS: i32 = 6;
/// Go `TpRuntimeStatsWithSnapshot`: the tp for `RuntimeStatsWithSnapshot`.
pub const TP_RUNTIME_STATS_WITH_SNAPSHOT: i32 = 7;
/// Go `TpJoinRuntimeStats`: the tp for `JoinRuntimeStats`.
pub const TP_JOIN_RUNTIME_STATS: i32 = 8;
/// Go `TpSelectResultRuntimeStats`: the tp for `SelectResultRuntimeStats`.
pub const TP_SELECT_RESULT_RUNTIME_STATS: i32 = 9;
/// Go `TpInsertRuntimeStat`: the tp for `InsertRuntimeStat`.
pub const TP_INSERT_RUNTIME_STAT: i32 = 10;
/// Go `TpIndexLookUpRunTimeStats`: the tp for `IndexLookUpRunTimeStats`.
pub const TP_INDEX_LOOK_UP_RUN_TIME_STATS: i32 = 11;
/// Go `TpSlowQueryRuntimeStat`: the tp for `SlowQueryRuntimeStat`.
pub const TP_SLOW_QUERY_RUNTIME_STAT: i32 = 12;
/// Go `TpHashAggRuntimeStat`: the tp for `HashAggRuntimeStat`.
pub const TP_HASH_AGG_RUNTIME_STAT: i32 = 13;
/// Go `TpIndexMergeRunTimeStats`: the tp for `IndexMergeRunTimeStats`.
pub const TP_INDEX_MERGE_RUN_TIME_STATS: i32 = 14;
/// Go `TpBasicCopRunTimeStats`: the tp for `BasicCopRunTimeStats`.
pub const TP_BASIC_COP_RUN_TIME_STATS: i32 = 15;
/// Go `TpUpdateRuntimeStats`: the tp for `UpdateRuntimeStats`.
pub const TP_UPDATE_RUNTIME_STATS: i32 = 16;
/// Go `TpFKCheckRuntimeStats`: the tp for `FKCheckRuntimeStats`.
pub const TP_FK_CHECK_RUNTIME_STATS: i32 = 17;
/// Go `TpFKCascadeRuntimeStats`: the tp for `FKCascadeRuntimeStats`.
pub const TP_FK_CASCADE_RUNTIME_STATS: i32 = 18;
/// Go `TpRURuntimeStats`: the tp for `RURuntimeStats`.
pub const TP_RU_RUNTIME_STATS: i32 = 19;

/// Go `execdetails.FormatDuration` (`util.go`): formats a duration for
/// explain output, pruning precision for human readability — under 1µs the
/// plain Go spelling; otherwise 2 decimals below 10 units and 1 decimal
/// from 10 units up.
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    reason = "Go time.Duration uses signed int64 nanoseconds"
)]
pub fn format_duration(d: StdDuration) -> String {
    let ns = d.as_nanos() as i64;
    if ns <= 1_000 {
        return tidb_model::go_duration::format_go_duration(ns);
    }
    tidb_model::go_duration::format_explain_duration(ns)
}

/// Go `RuntimeStats`: the executor runtime information interface.
pub trait RuntimeStats: Any + Send {
    /// Go `String`.
    fn string(&self) -> String;
    /// Go `Merge`.
    fn merge(&mut self, other: &dyn RuntimeStats);
    /// Go `Clone`.
    fn clone_box(&self) -> Box<dyn RuntimeStats>;
    /// Go `Tp`.
    fn tp(&self) -> i32;
    /// The Go type switch's `rs.(*ConcreteType)` seam.
    fn as_any(&self) -> &dyn Any;
}

/// Go `RURuntimeStats`'s `RuntimeStats` methods; the type and its formatting
/// live with the RU v2 metrics in [`crate::ruv2_metrics`].
impl RuntimeStats for crate::ruv2_metrics::RuRuntimeStats {
    fn string(&self) -> String {
        crate::ruv2_metrics::RuRuntimeStats::string(self)
    }

    fn merge(&mut self, other: &dyn RuntimeStats) {
        if let Some(tmp) = other
            .as_any()
            .downcast_ref::<crate::ruv2_metrics::RuRuntimeStats>()
        {
            crate::ruv2_metrics::RuRuntimeStats::merge(self, tmp);
        }
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone_stats())
    }

    fn tp(&self) -> i32 {
        TP_RU_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `ConcurrencyInfo`: the concurrency information of one executor
/// operator.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConcurrencyInfo {
    /// Go `ConcurrencyInfo.concurrencyName`.
    concurrency_name: String,
    /// Go `ConcurrencyInfo.concurrencyNum`.
    concurrency_num: i64,
}

impl ConcurrencyInfo {
    /// Go `NewConcurrencyInfo`.
    #[must_use]
    pub fn new(name: &str, num: i64) -> ConcurrencyInfo {
        ConcurrencyInfo {
            concurrency_name: name.to_owned(),
            concurrency_num: num,
        }
    }
}

/// Go `RuntimeStatsWithConcurrencyInfo`: concurrency info attached to the
/// runtime stats.
#[derive(Debug, Default)]
pub struct RuntimeStatsWithConcurrencyInfo {
    /// Go `RuntimeStatsWithConcurrencyInfo.concurrency`.
    concurrency: Mutex<Vec<ConcurrencyInfo>>,
}

impl RuntimeStatsWithConcurrencyInfo {
    /// Go `SetConcurrencyInfo`: replaces the concurrency information.
    /// `num <= 0` means the operator is not executed in parallel.
    pub fn set_concurrency_info(&self, infos: Vec<ConcurrencyInfo>) {
        *self.concurrency.lock().unwrap() = infos;
    }
}

impl Clone for RuntimeStatsWithConcurrencyInfo {
    fn clone(&self) -> Self {
        Self {
            concurrency: Mutex::new(self.concurrency.lock().unwrap().clone()),
        }
    }
}

impl RuntimeStats for RuntimeStatsWithConcurrencyInfo {
    fn string(&self) -> String {
        let mut buf = String::with_capacity(8);
        let concurrency = self.concurrency.lock().unwrap();
        for (i, concurrency) in concurrency.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            if concurrency.concurrency_num > 0 {
                buf.push_str(&concurrency.concurrency_name);
                buf.push(':');
                buf.push_str(&concurrency.concurrency_num.to_string());
            } else {
                buf.push_str(&concurrency.concurrency_name);
                buf.push_str(":OFF");
            }
        }
        buf
    }

    fn merge(&mut self, _other: &dyn RuntimeStats) {}

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_RUNTIME_STATS_WITH_CONCURRENCY_INFO
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
