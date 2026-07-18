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

//! Dependency-closed runtime contracts from `pkg/distsql/distsql.go`.
//!
//! This leaf preserves the result metadata and request-option decisions made
//! around a DAG/MPP/ANALYZE/CHECKSUM send. Client transport, response streaming,
//! protobuf encoding, memory trackers, and unsafe ABI checks stay explicit
//! boundaries until their concrete protocol consumers are available.

use std::collections::BTreeMap;

use tidb_proto::ExecutorExecutionSummary;

use crate::{DistSqlContext, RequestSource, StoreType};

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

/// The bounded runtime statistics updated while consuming select responses.
///
/// Durations are nanoseconds, matching the checked-in tipb summary contract.
/// TiKV scan/RPC detail, RU accounting, telemetry, and percentile histograms
/// remain with their unported concrete owners.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SelectResultRuntimeStats {
    backoff_sleep_ns: BTreeMap<String, u64>,
    plan_summaries: BTreeMap<isize, Vec<ExecutorExecutionSummary>>,
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

/// One outgoing metadata key/value appended by TiFlash settings propagation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutgoingMetadata {
    /// Metadata key.
    pub key: String,
    /// Metadata value.
    pub value: String,
}

/// TiFlash settings copied to outgoing gRPC metadata by `SetTiFlashConfVarsInContext`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TiFlashSettings {
    /// Maximum TiFlash threads; `-1` means unset.
    pub max_threads: i64,
    /// External-join byte threshold; `-1` means unset.
    pub max_bytes_before_external_join: i64,
    /// External-group-by byte threshold; `-1` means unset.
    pub max_bytes_before_external_group_by: i64,
    /// External-sort byte threshold; `-1` means unset.
    pub max_bytes_before_external_sort: i64,
    /// Query memory quota per node.
    pub max_query_memory_per_node: i64,
    /// Automatic spill ratio.
    pub query_spill_ratio: f64,
    /// Whether the optimized hash join is selected.
    pub hash_join_optimized: bool,
}

impl Default for TiFlashSettings {
    fn default() -> Self {
        Self {
            max_threads: -1,
            max_bytes_before_external_join: -1,
            max_bytes_before_external_group_by: -1,
            max_bytes_before_external_sort: -1,
            max_query_memory_per_node: 0,
            query_spill_ratio: 0.0,
            hash_join_optimized: false,
        }
    }
}

/// Appends TiFlash settings in the source order.
#[must_use]
pub fn tiflash_conf_metadata(settings: TiFlashSettings) -> Vec<OutgoingMetadata> {
    let mut metadata = Vec::new();
    append_if_set(
        &mut metadata,
        "tidb_max_tiflash_threads",
        settings.max_threads,
    );
    append_if_set(
        &mut metadata,
        "tidb_max_bytes_before_tiflash_external_join",
        settings.max_bytes_before_external_join,
    );
    append_if_set(
        &mut metadata,
        "tidb_max_bytes_before_tiflash_external_group_by",
        settings.max_bytes_before_external_group_by,
    );
    append_if_set(
        &mut metadata,
        "tidb_max_bytes_before_tiflash_external_sort",
        settings.max_bytes_before_external_sort,
    );
    metadata.push(OutgoingMetadata {
        key: "tiflash_mem_quota_query_per_node".to_owned(),
        value: settings.max_query_memory_per_node.max(0).to_string(),
    });
    metadata.push(OutgoingMetadata {
        key: "tiflash_query_spill_ratio".to_owned(),
        value: format_go_float_f_shortest(settings.query_spill_ratio),
    });
    metadata.push(OutgoingMetadata {
        key: "tiflash_use_hash_join_v2".to_owned(),
        value: settings.hash_join_optimized.to_string(),
    });
    metadata
}

// `strconv.FormatFloat(value, 'f', -1, 64)` and Rust's shortest decimal
// formatting agree for finite values, but Go spells positive infinity with an
// explicit plus sign. Keep the wire metadata source-exact for all f64 inputs.
fn format_go_float_f_shortest(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}

fn append_if_set(metadata: &mut Vec<OutgoingMetadata>, key: &str, value: i64) {
    if value != -1 {
        metadata.push(OutgoingMetadata {
            key: key.to_owned(),
            value: value.to_string(),
        });
    }
}

/// Returns whether the chunk-RPC path can use the host memory layout.
#[must_use]
pub const fn can_use_chunk_rpc(context: &DistSqlContext, alignment_ok: bool) -> bool {
    context.request.enable_chunk_rpc && alignment_ok
}

/// Selects the source DAG encoding policy.
#[must_use]
pub const fn set_encode_type(context: &DistSqlContext, alignment_ok: bool) -> EncodeType {
    if can_use_chunk_rpc(context, alignment_ok) {
        EncodeType::Chunk
    } else {
        EncodeType::Default
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

/// Returns whether the SQL KV execution counter interceptor would be bound.
#[must_use]
pub const fn with_sql_kv_exec_counter_interceptor(counter_present: bool) -> bool {
    counter_present
}
