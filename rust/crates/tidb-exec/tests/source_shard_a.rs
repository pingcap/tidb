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

//! Deterministic source-test shard; individual Go-owned test files remain the modules.

#[path = "advisory_lock_state_source.rs"]
mod advisory_lock_state_source;
#[path = "aggregate_distinct_source.rs"]
mod aggregate_distinct_source;
#[path = "alternative_plan_signals_source.rs"]
mod alternative_plan_signals_source;
#[path = "analyze_panic_error_source.rs"]
mod analyze_panic_error_source;
#[path = "apply_cache_source.rs"]
mod apply_cache_source;
#[path = "avg_float64_source.rs"]
mod avg_float64_source;
#[path = "bit_agg_source.rs"]
mod bit_agg_source;
#[path = "broadcast_query_error_source.rs"]
mod broadcast_query_error_source;
#[path = "charset_variable_groups_source.rs"]
mod charset_variable_groups_source;
#[path = "chunk_alloc_status_source.rs"]
mod chunk_alloc_status_source;
#[path = "cluster_index_id_source.rs"]
mod cluster_index_id_source;
#[path = "concurrent_entry_map_source.rs"]
mod concurrent_entry_map_source;
#[path = "config_int_json_source.rs"]
mod config_int_json_source;
#[path = "context_id_source.rs"]
mod context_id_source;
#[path = "count_distinct_int_source.rs"]
mod count_distinct_int_source;
#[path = "cte_first_error_source.rs"]
mod cte_first_error_source;
#[path = "cume_dist_source.rs"]
mod cume_dist_source;
#[path = "cursor_tracker_source.rs"]
mod cursor_tracker_source;
#[path = "ddl_job_comments_source.rs"]
mod ddl_job_comments_source;
#[path = "delete_rows_col_multiply_source.rs"]
mod delete_rows_col_multiply_source;
#[path = "distsql_query_runtime_source.rs"]
mod distsql_query_runtime_source;
#[path = "distsql_recordset_source.rs"]
mod distsql_recordset_source;
#[path = "effective_auth_plugin_source.rs"]
mod effective_auth_plugin_source;
#[path = "error_context_source.rs"]
mod error_context_source;
#[path = "error_conversion_source.rs"]
mod error_conversion_source;
#[path = "first_row_source.rs"]
mod first_row_source;
#[path = "global_sysvar_initial_source.rs"]
mod global_sysvar_initial_source;
#[path = "group_concat_live_source.rs"]
mod group_concat_live_source;
#[path = "group_concat_source.rs"]
mod group_concat_source;
