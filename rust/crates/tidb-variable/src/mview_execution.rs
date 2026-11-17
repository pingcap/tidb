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

//! Go master `variable.MViewExecutionSessionVars` (`variable.go`): the
//! session settings a materialized view's maintenance runs under.

use tidb_vardef::{defaults, tidb_vars};

/// Go master `MViewExecutionSessionVars`.
#[derive(Clone, Debug, PartialEq)]
pub struct MViewExecutionSessionVars {
    pub maintain_mem_quota: i64,
    pub isolation_read_engines: String,
    pub tiflash_max_threads: i64,
    pub tiflash_max_bytes_before_ext_join: i64,
    pub tiflash_max_bytes_before_ext_agg: i64,
    pub tiflash_max_bytes_before_ext_sort: i64,
    pub tiflash_mem_quota_query_per_node: i64,
    pub tiflash_query_spill_ratio: f64,
    pub fine_grained_stream_count: i64,
    pub fine_grained_batch_size: u64,
    pub import_threads: i64,
    pub import_disk_quota: String,
    pub track_aggregate_memory_usage: bool,
    pub enable_parallel_hashagg_spill: bool,
}

impl Default for MViewExecutionSessionVars {
    fn default() -> Self {
        Self {
            maintain_mem_quota: 0,
            isolation_read_engines: String::new(),
            tiflash_max_threads: 0,
            tiflash_max_bytes_before_ext_join: 0,
            tiflash_max_bytes_before_ext_agg: 0,
            tiflash_max_bytes_before_ext_sort: 0,
            tiflash_mem_quota_query_per_node: 0,
            tiflash_query_spill_ratio: 0.0,
            fine_grained_stream_count: 0,
            fine_grained_batch_size: 0,
            import_threads: 0,
            import_disk_quota: String::new(),
            track_aggregate_memory_usage: false,
            enable_parallel_hashagg_spill: false,
        }
    }
}

impl MViewExecutionSessionVars {
    /// Go master `CaptureMViewExecutionSessionVars`: the session's values,
    /// read from its system variables (`get` answers a variable's session
    /// value), each as Go's `SetSession` keeps it.
    #[must_use]
    pub fn capture(get: impl Fn(&str) -> Option<String>) -> Self {
        let int = |name: &str, default: i64| {
            get(name).map_or(default, |value| crate::varsutil::tidb_opt_int64(&value, default))
        };
        let on = |name: &str, default: bool| get(name).map_or(default, |value| tidb_vardef::option_values::tidb_opt_on(&value));
        Self {
            maintain_mem_quota: int(tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, defaults::DEF_TIDB_MVIEW_MAINTAIN_MEM_QUOTA),
            isolation_read_engines: get(tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES).unwrap_or_default(),
            tiflash_max_threads: int(tidb_vars::TIDB_MAX_TIFLASH_THREADS, defaults::DEF_TIFLASH_MAX_THREADS),
            tiflash_max_bytes_before_ext_join: int(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN,
                defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ),
            tiflash_max_bytes_before_ext_agg: int(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY,
                defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ),
            tiflash_max_bytes_before_ext_sort: int(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT,
                defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ),
            tiflash_mem_quota_query_per_node: int(
                tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
                defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
            ),
            tiflash_query_spill_ratio: get(tidb_vars::TIFLASH_QUERY_SPILL_RATIO)
                .and_then(|value| value.parse().ok())
                .unwrap_or(defaults::DEF_TIFLASH_QUERY_SPILL_RATIO),
            fine_grained_stream_count: int(
                tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
                defaults::DEF_TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
            ),
            fine_grained_batch_size: u64::try_from(int(
                tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
                defaults::DEF_TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
            ))
            .unwrap_or_default(),
            import_threads: int(tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, defaults::DEF_TIDB_MVIEW_MAINTAIN_IMPORT_THREADS),
            import_disk_quota: get(tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA)
                .unwrap_or_else(|| defaults::DEF_TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA.to_owned()),
            track_aggregate_memory_usage: on(
                tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
                defaults::DEF_TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
            ),
            enable_parallel_hashagg_spill: on(
                tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL,
                defaults::DEF_TIDB_ENABLE_PARALLEL_HASHAGG_SPILL,
            ),
        }
    }

    /// Go master `AddMViewExecutionSessionVarsToJob`'s entries, in Go's order:
    /// the job's `SessionVars` that carry these settings.
    #[must_use]
    pub fn job_system_vars(&self) -> Vec<(&'static str, String)> {
        let on_off = |value: bool| if value { "ON" } else { "OFF" }.to_owned();
        vec![
            (tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, self.maintain_mem_quota.to_string()),
            (tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, self.isolation_read_engines.clone()),
            (tidb_vars::TIDB_MAX_TIFLASH_THREADS, self.tiflash_max_threads.to_string()),
            (tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN, self.tiflash_max_bytes_before_ext_join.to_string()),
            (tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY, self.tiflash_max_bytes_before_ext_agg.to_string()),
            (tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT, self.tiflash_max_bytes_before_ext_sort.to_string()),
            (tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE, self.tiflash_mem_quota_query_per_node.to_string()),
            (tidb_vars::TIFLASH_QUERY_SPILL_RATIO, tidb_goruntime::go_strconv::format_float(self.tiflash_query_spill_ratio, b'f', -1, 64)),
            (tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT, self.fine_grained_stream_count.to_string()),
            (tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE, self.fine_grained_batch_size.to_string()),
            (tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, self.import_threads.to_string()),
            (tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA, self.import_disk_quota.clone()),
            (tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE, on_off(self.track_aggregate_memory_usage)),
            (tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL, on_off(self.enable_parallel_hashagg_spill)),
        ]
    }
}
