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

#[path = "placement_labels_source.rs"]
mod placement_labels_source;
#[path = "plan_cache_params_source.rs"]
mod plan_cache_params_source;
#[path = "privilege_set_source.rs"]
mod privilege_set_source;
#[path = "process_info_source.rs"]
mod process_info_source;
#[path = "read_consistency_source.rs"]
mod read_consistency_source;
#[path = "readable_size_source.rs"]
mod readable_size_source;
#[path = "removed_sysvar_source.rs"]
mod removed_sysvar_source;
#[path = "reserved_row_id_source.rs"]
mod reserved_row_id_source;
#[path = "result_response_source.rs"]
mod result_response_source;
#[path = "retry_info_source.rs"]
mod retry_info_source;
#[path = "sequence_state_source.rs"]
mod sequence_state_source;
#[path = "session_context_key_source.rs"]
mod session_context_key_source;
#[path = "session_metrics_source.rs"]
mod session_metrics_source;
#[path = "session_pool_capacity_source.rs"]
mod session_pool_capacity_source;
#[path = "session_reuse_state_source.rs"]
mod session_reuse_state_source;
#[path = "session_status_source.rs"]
mod session_status_source;
#[path = "session_token_timing_source.rs"]
mod session_token_timing_source;
#[path = "setvar_hint_restore_source.rs"]
mod setvar_hint_restore_source;
#[path = "slow_log_match_source.rs"]
mod slow_log_match_source;
#[path = "slow_log_rules_source.rs"]
mod slow_log_rules_source;
#[path = "slow_log_split_source.rs"]
mod slow_log_split_source;
#[path = "slow_log_threshold_source.rs"]
mod slow_log_threshold_source;
#[path = "spill_count_source.rs"]
mod spill_count_source;
#[path = "statement_pushdown_source.rs"]
mod statement_pushdown_source;
#[path = "statement_refcount_source.rs"]
mod statement_refcount_source;
#[path = "statement_rows_reader_source.rs"]
mod statement_rows_reader_source;
#[path = "statement_status_source.rs"]
mod statement_status_source;
#[path = "stats_load_result_source.rs"]
mod stats_load_result_source;
#[path = "status_registry_source.rs"]
mod status_registry_source;
