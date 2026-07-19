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

// Campaign 25's modules are source-included until the executor routing steward
// exports them. Bridge their two existing crate-local dependencies.
mod dag_request {
    pub use tidb_exec::dag_request::*;
}
mod distsql_recordset {
    pub use tidb_exec::distsql_recordset::*;
}

#[path = "../src/real_tikv_multi_read.rs"]
mod real_tikv_multi_read;
#[path = "real_tikv_multi_relation_scan_source.rs"]
mod real_tikv_multi_relation_scan_source;
#[allow(dead_code)]
#[path = "../src/real_tikv_read.rs"]
mod real_tikv_read;

#[path = "bit_live_aggregate_source.rs"]
mod bit_live_aggregate_source;
#[path = "cume_dist_source.rs"]
mod cume_dist_source;
#[path = "first_row_live_aggregate_source.rs"]
mod first_row_live_aggregate_source;
#[path = "lead_lag_source.rs"]
mod lead_lag_source;
#[path = "ntile_source.rs"]
mod ntile_source;
#[path = "status_result_source.rs"]
mod status_result_source;
#[path = "stddevpop_source.rs"]
mod stddevpop_source;
#[path = "stddevsamp_source.rs"]
mod stddevsamp_source;
#[path = "sum_float64_source.rs"]
mod sum_float64_source;
#[path = "sum_int_source.rs"]
mod sum_int_source;
#[path = "system_db_filter_source.rs"]
mod system_db_filter_source;
#[path = "sysvar_error_source.rs"]
mod sysvar_error_source;
#[path = "sysvar_scope_source.rs"]
mod sysvar_scope_source;
#[path = "sysvar_type_source.rs"]
mod sysvar_type_source;
#[path = "tagged_ptr_source.rs"]
mod tagged_ptr_source;
#[path = "traffic_form_source.rs"]
mod traffic_form_source;
#[path = "txn_read_ts_source.rs"]
mod txn_read_ts_source;
#[path = "txn_running_state_source.rs"]
mod txn_running_state_source;
#[path = "txn_summary_source.rs"]
mod txn_summary_source;
#[path = "typed_condition_eval_source.rs"]
mod typed_condition_eval_source;
#[path = "upgrade_versions_source.rs"]
mod upgrade_versions_source;
#[path = "used_stats_source.rs"]
mod used_stats_source;
#[path = "variance_live_aggregate_source.rs"]
mod variance_live_aggregate_source;
#[path = "varpop_source.rs"]
mod varpop_source;
#[path = "varsamp_source.rs"]
mod varsamp_source;
#[path = "vec_group_checker_int_source.rs"]
mod vec_group_checker_int_source;
#[path = "warning_publication_source.rs"]
mod warning_publication_source;
#[path = "window_complete_live_source.rs"]
mod window_complete_live_source;
#[path = "window_ranking_live_source.rs"]
mod window_ranking_live_source;
#[path = "window_value_int_source.rs"]
mod window_value_int_source;
