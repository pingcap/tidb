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

#[path = "hash_join_version_source.rs"]
mod hash_join_version_source;
#[path = "hint_updatable_vars_source.rs"]
mod hint_updatable_vars_source;
#[path = "insert_rows_col_multiply_source.rs"]
mod insert_rows_col_multiply_source;
#[path = "isolation_state_source.rs"]
mod isolation_state_source;
#[path = "join_table_meta_source.rs"]
mod join_table_meta_source;
#[path = "json_arrayagg_source.rs"]
mod json_arrayagg_source;
#[path = "json_objectagg_source.rs"]
mod json_objectagg_source;
#[path = "lack_handles_source.rs"]
mod lack_handles_source;
#[path = "lazy_txn_state_source.rs"]
mod lazy_txn_state_source;
#[path = "lead_lag_source.rs"]
mod lead_lag_source;
#[path = "max_min_runtime_source.rs"]
mod max_min_runtime_source;
#[path = "minmax_deque_source.rs"]
mod minmax_deque_source;
#[path = "mock_global_accessor_source.rs"]
mod mock_global_accessor_source;
#[path = "next_io_acc_source.rs"]
mod next_io_acc_source;
#[path = "nextgen_readonly_vars_source.rs"]
mod nextgen_readonly_vars_source;
#[path = "nontransactional_source.rs"]
mod nontransactional_source;
#[path = "noop_read_only_source.rs"]
mod noop_read_only_source;
#[path = "ntile_source.rs"]
mod ntile_source;
#[path = "option_values_source.rs"]
mod option_values_source;
#[path = "ordered_apply_buffer_source.rs"]
mod ordered_apply_buffer_source;
#[path = "pd_approximate_count_source.rs"]
mod pd_approximate_count_source;
#[path = "percentile_source.rs"]
mod percentile_source;
