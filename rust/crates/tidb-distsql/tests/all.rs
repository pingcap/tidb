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

//! All ordinary `tidb-distsql` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod active_cancellation_source;
mod channel_iter_source;
mod chblock_source;
mod chunk_decode_source;
mod cop_paging_source;
mod cop_read_task_runtime_source;
mod copr_cache_source;
mod coprocessor_request_source;
mod default_datum_source;
mod direct_unary_batch_admission;
mod direct_unary_client_fixture;
mod direct_unary_dispatch_contract;
mod direct_unary_forwarding_source;
mod direct_unary_paging_and_close;
mod direct_unary_query_seed;
mod direct_unary_region_errors;
mod direct_unary_request_refusals;
mod direct_unary_retry_budget;
mod direct_unary_store_not_match;
mod direct_unary_store_selection;
mod direct_unary_transport_failures;
mod distsql_runtime_source;
mod kv_request_source;
mod mock_response_iteration_source;
mod paging_source;
mod query_runtime_source;
mod range_encoding_literals_source;
mod read_bytes_ema_source;
mod region_location_coverage_source;
mod region_task_builder_source;
mod region_task_source;
mod request_builder_source;
mod response_channel_source;
mod select_iter_source;
mod select_result_source;
mod signed_handle_range_request_source;
mod stream_decode_source;
mod table_handle_ranges_source;
mod tiflash_replica_read_source;
mod tikv_rpc_contract_source;
mod transport_failure_source;
