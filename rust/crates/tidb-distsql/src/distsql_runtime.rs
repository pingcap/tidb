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

//! Result metadata and statistics consumed by the live DistSQL request owners.
//!
//! This module implements a subset of `pkg/distsql/distsql.go`. Chunk encoding
//! and transport belong to their concrete request/response owners; complete
//! TiFlash configuration propagation remains an unimplemented Go obligation.

use std::collections::BTreeMap;

use tidb_proto::ExecutorExecutionSummary;

use crate::{RequestSource, StoreType};

/// Result labels used by the source `selectResult` constructors.
pub const DAG_RESULT_LABEL: &str = "dag";
/// MPP result label.
pub const MPP_RESULT_LABEL: &str = "mpp";
/// ANALYZE result label.
pub const ANALYZE_RESULT_LABEL: &str = "analyze";
/// CHECKSUM result label.
pub const CHECKSUM_RESULT_LABEL: &str = "checksum";
/// General SQL result metric label.
pub const GENERAL_SQL_TYPE: &str = "general";
/// Restricted/internal SQL result metric label.
pub const INTERNAL_SQL_TYPE: &str = "internal";
/// KV request source set by ANALYZE requests.
pub const INTERNAL_TXN_STATS_SOURCE: &str = "stats";

/// Result encoding selected by the chunk-RPC policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodeType {
    /// Columnar chunk encoding.
    Chunk,
    /// Row-by-row default encoding.
    Default,
}

/// Endianness marker attached to a chunk-memory layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SystemEndian {
    /// Big-endian host.
    Big,
    /// Little-endian host.
    Little,
}

/// Inputs needed to construct a DAG select result envelope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SelectInput {
    /// Store selected by the request.
    pub store_type: StoreType,
    /// Number of result fields.
    pub row_len: usize,
    /// Whether the SQL runs in restricted/internal mode.
    pub in_restricted_sql: bool,
    /// Whether a coprocessor memory tracker was attached to the request.
    pub mem_tracker_bound: bool,
    /// Row paging enable flag.
    pub paging_enabled: bool,
    /// Byte paging size, which enables paging even when row paging is off.
    pub paging_size_bytes: u64,
    /// Effective DistSQL concurrency.
    pub dist_sql_concurrency: u64,
}

/// Metadata attached to a source `selectResult`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectResultMetadata {
    /// Source result label (`dag`, `mpp`, `analyze`, or `checksum`).
    pub label: &'static str,
    /// SQL metric label; MPP has no restricted/general override.
    pub sql_type: Option<&'static str>,
    /// Store selected by the request.
    pub store_type: StoreType,
    /// Number of result fields.
    pub row_len: usize,
    /// Whether the coprocessor memory tracker was bound.
    pub mem_tracker_bound: bool,
    /// Whether row or byte paging is enabled.
    pub paging: bool,
    /// Effective DistSQL concurrency.
    pub dist_sql_concurrency: u64,
    /// Coprocessor plan IDs used for runtime-stat collection.
    pub cop_plan_ids: Vec<isize>,
    /// Root plan ID used for runtime-stat collection.
    pub root_plan_id: Option<isize>,
}

impl SelectResultMetadata {
    /// Creates the runtime-stat accumulator consumed by select responses.
    #[must_use]
    pub fn runtime_stats(&self) -> SelectResultRuntimeStats {
        SelectResultRuntimeStats::default()
    }
}

/// Aggregated blocking time from the Go coprocessor request limiter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LimiterWaitStats {
    /// Total nanoseconds spent waiting for request permits.
    pub total_ns: u64,
    /// Longest single permit wait in nanoseconds.
    pub max_ns: u64,
}

impl LimiterWaitStats {
    /// Records one blocking wait, preserving the source total/max contract.
    pub fn record(&mut self, wait_ns: u64) {
        self.total_ns = self.total_ns.saturating_add(wait_ns);
        self.max_ns = self.max_ns.max(wait_ns);
    }

    /// Merges another aggregate into this one.
    pub fn merge(&mut self, other: Self) {
        self.total_ns = self.total_ns.saturating_add(other.total_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
    }

    /// Reports whether no blocking wait was recorded.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.total_ns == 0
    }
}

/// The bounded runtime statistics updated while consuming select responses.
///
/// Durations are nanoseconds, matching the checked-in tipb summary contract.
/// TiKV scan/RPC detail, RU accounting, telemetry, and percentile histograms
/// remain with their unported concrete owners.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SelectResultRuntimeStats {
    backoff_sleep_ns: BTreeMap<String, u64>,
    plan_summaries: BTreeMap<isize, Vec<ExecutorExecutionSummary>>,
    /// Request-limiter wait aggregate, equivalent to Go's `limiterWait`.
    pub limiter_wait: LimiterWaitStats,
}

impl SelectResultRuntimeStats {
    /// Applies the source `updateCopRuntimeStats` gates and merge order.
    ///
    /// Backoff totals are merged once a collector and root plan exist, even
    /// when summary count mismatches the plan list. Complete summaries are
    /// recorded only when the lengths match.
    pub fn update(
        &mut self,
        metadata: &SelectResultMetadata,
        collector_enabled: bool,
        callee_address: &str,
        request_rpc_stats_present: bool,
        backoff_sleep_ns: impl IntoIterator<Item = (String, u64)>,
        execution_summaries: &[ExecutorExecutionSummary],
    ) {
        if !collector_enabled
            || metadata.root_plan_id.is_none_or(|plan_id| plan_id <= 0)
            || (callee_address.is_empty() && !request_rpc_stats_present)
        {
            return;
        }
        for (kind, duration) in backoff_sleep_ns {
            *self.backoff_sleep_ns.entry(kind).or_default() += duration;
        }
        if execution_summaries.len() != metadata.cop_plan_ids.len() {
            return;
        }
        for (plan_id, summary) in metadata
            .cop_plan_ids
            .iter()
            .copied()
            .zip(execution_summaries)
        {
            if summary.time_processed_ns.is_some()
                && summary.num_produced_rows.is_some()
                && summary.num_iterations.is_some()
            {
                self.plan_summaries
                    .entry(plan_id)
                    .or_default()
                    .push(summary.clone());
            }
        }
    }

    /// Returns accumulated sleep for one backoff kind.
    #[must_use]
    pub fn backoff_sleep_ns(&self, kind: &str) -> u64 {
        self.backoff_sleep_ns.get(kind).copied().unwrap_or_default()
    }

    /// Returns the latest complete task summary recorded for one plan.
    #[must_use]
    pub fn plan_summary(&self, plan_id: isize) -> Option<&ExecutorExecutionSummary> {
        self.plan_summaries
            .get(&plan_id)
            .and_then(|summaries| summaries.last())
    }

    /// Returns all complete task samples recorded for one plan in arrival
    /// order. Keeping source protobufs distinct avoids fabricating a summary
    /// whose executor ID or nested TiFlash detail belongs to only one task.
    #[must_use]
    pub fn plan_summaries(&self, plan_id: isize) -> &[ExecutorExecutionSummary] {
        self.plan_summaries.get(&plan_id).map_or(&[], Vec::as_slice)
    }

    /// Go `RuntimeStatsColl.GetCopCountAndRows`.
    #[must_use]
    pub fn cop_count_and_rows(&self, plan_id: isize) -> (u64, u64) {
        let summaries = self.plan_summaries(plan_id);
        (
            summaries.len() as u64,
            summaries.iter().fold(0_u64, |rows, summary| {
                rows.wrapping_add(summary.num_produced_rows.unwrap_or_default())
            }),
        )
    }

    /// Records one request-limiter wait from a coprocessor response.
    pub fn record_limiter_wait(&mut self, wait_ns: u64) {
        self.limiter_wait.record(wait_ns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cop_count_and_rows_matches_runtime_stats_collection() {
        let mut stats = SelectResultRuntimeStats::default();
        let metadata = SelectResultMetadata {
            label: "dag",
            sql_type: None,
            store_type: StoreType::TiKv,
            row_len: 1,
            mem_tracker_bound: false,
            paging: false,
            dist_sql_concurrency: 1,
            cop_plan_ids: vec![7],
            root_plan_id: Some(9),
        };
        for rows in [3, 5] {
            stats.update(
                &metadata,
                true,
                "store",
                false,
                [],
                &[ExecutorExecutionSummary {
                    time_processed_ns: Some(1),
                    num_produced_rows: Some(rows),
                    num_iterations: Some(1),
                    ..ExecutorExecutionSummary::default()
                }],
            );
        }
        assert_eq!(stats.cop_count_and_rows(7), (2, 8));
        assert_eq!(stats.cop_count_and_rows(8), (0, 0));
    }

    #[test]
    fn source_mpp_result_preserves_store_and_runtime_plan_metadata() {
        let metadata: SelectResultMetadata = mpp_result_metadata(2, vec![7], 8);
        assert_eq!(metadata.label, MPP_RESULT_LABEL);
        assert_eq!(metadata.sql_type, None);
        assert_eq!(metadata.store_type, StoreType::TiFlash);
        assert_eq!(metadata.cop_plan_ids, vec![7]);
        assert_eq!(metadata.root_plan_id, Some(8));
    }

    #[test]
    fn source_limiter_wait_stats_merge_total_and_max() {
        let mut stats = SelectResultRuntimeStats::default();
        assert!(LimiterWaitStats::default().is_zero());
        stats.record_limiter_wait(5);
        stats.record_limiter_wait(3);
        assert_eq!(
            stats.limiter_wait,
            LimiterWaitStats {
                total_ns: 8,
                max_ns: 5
            }
        );

        let mut additional = LimiterWaitStats::default();
        additional.record(10);
        additional.record(2);
        stats.limiter_wait.merge(additional);
        assert_eq!(stats.limiter_wait.total_ns, 20);
        assert_eq!(stats.limiter_wait.max_ns, 10);
        assert!(!stats.limiter_wait.is_zero());
    }
}

/// Builds the DAG result metadata created by `Select`.
#[must_use]
pub fn select_result_metadata(input: SelectInput) -> SelectResultMetadata {
    SelectResultMetadata {
        label: DAG_RESULT_LABEL,
        sql_type: Some(if input.in_restricted_sql {
            INTERNAL_SQL_TYPE
        } else {
            GENERAL_SQL_TYPE
        }),
        store_type: input.store_type,
        row_len: input.row_len,
        mem_tracker_bound: input.mem_tracker_bound,
        paging: input.paging_enabled || input.paging_size_bytes > 0,
        dist_sql_concurrency: input.dist_sql_concurrency,
        cop_plan_ids: Vec::new(),
        root_plan_id: None,
    }
}

/// Builds the MPP result metadata created by `GenSelectResultFromMPPResponse`.
#[must_use]
pub fn mpp_result_metadata(
    row_len: usize,
    cop_plan_ids: Vec<isize>,
    root_plan_id: isize,
) -> SelectResultMetadata {
    SelectResultMetadata {
        label: MPP_RESULT_LABEL,
        sql_type: None,
        store_type: StoreType::TiFlash,
        row_len,
        mem_tracker_bound: false,
        paging: false,
        dist_sql_concurrency: 0,
        cop_plan_ids,
        root_plan_id: Some(root_plan_id),
    }
}

/// Adds runtime-stat plan metadata as `SelectWithRuntimeStats` does.
#[must_use]
pub fn select_with_runtime_stats(
    input: SelectInput,
    cop_plan_ids: Vec<isize>,
    root_plan_id: isize,
) -> SelectResultMetadata {
    let mut metadata = select_result_metadata(input);
    metadata.cop_plan_ids = cop_plan_ids;
    metadata.root_plan_id = Some(root_plan_id);
    metadata
}

/// Builds ANALYZE result metadata.
#[must_use]
pub fn analyze_result_metadata(
    store_type: StoreType,
    in_restricted_sql: bool,
) -> SelectResultMetadata {
    SelectResultMetadata {
        label: ANALYZE_RESULT_LABEL,
        sql_type: Some(if in_restricted_sql {
            INTERNAL_SQL_TYPE
        } else {
            GENERAL_SQL_TYPE
        }),
        store_type,
        row_len: 0,
        mem_tracker_bound: false,
        paging: false,
        dist_sql_concurrency: 0,
        cop_plan_ids: Vec::new(),
        root_plan_id: None,
    }
}

/// Returns the source mutation applied to an ANALYZE KV request.
#[must_use]
pub fn analyze_request_source() -> RequestSource {
    RequestSource {
        internal: true,
        source_type: INTERNAL_TXN_STATS_SOURCE.to_owned(),
        explicit_source_type: String::new(),
    }
}

/// Builds CHECKSUM result metadata.
#[must_use]
pub fn checksum_result_metadata(store_type: StoreType) -> SelectResultMetadata {
    SelectResultMetadata {
        label: CHECKSUM_RESULT_LABEL,
        sql_type: Some(GENERAL_SQL_TYPE),
        store_type,
        row_len: 0,
        mem_tracker_bound: false,
        paging: false,
        dist_sql_concurrency: 0,
        cop_plan_ids: Vec::new(),
        root_plan_id: None,
    }
}

/// Returns the compile-target endian used in the chunk-memory layout.
#[must_use]
pub const fn system_endian() -> SystemEndian {
    if cfg!(target_endian = "big") {
        SystemEndian::Big
    } else {
        SystemEndian::Little
    }
}
