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

//! All topology-independent `tidb-executor` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod base_join_probe_source;
mod concurrent_entry_map_source;
mod cteutil_source;
mod ddl_job_queue_executor_source;
mod ddl_ttl_info_options_source;
mod default_on_update_source;
mod hash_join_v2_source;
mod hash_join_version_source;
mod hash_table_v2_source;
mod index_entry_go_bytes;
mod index_nokit_disk_full_pause_source;
mod join_row_table_source;
mod join_table_meta_source;
mod partial_aggregate_primary_ids_source;
mod partition_exchange_global_index_source;
mod physical_expand_source;
mod physical_union_scan_execution_source;
mod ranger_types_source;
mod reorg_partition_ddl_failures_source;
mod row_decoder_source;
mod shard_row_id_bits_source;
mod shared_physical_select_builder_source;
mod statement_pushdown_source;
mod table_mode_transition_source;
mod table_options_source;
mod tagged_ptr_source;
mod taobench_repro_source;
mod used_stats_source;
mod window_executor_source;
