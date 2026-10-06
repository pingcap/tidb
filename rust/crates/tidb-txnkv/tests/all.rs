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

//! All topology-independent `tidb-txnkv` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod async_commit_one_pc_realtikv_source;
mod async_commit_one_pc_source;
mod async_completion_source;
mod background_region_gc_source;
mod background_runner_source;
mod background_store_maintenance_source;
mod batch_coprocessor_dispatch_source;
mod batch_getter_source;
mod batch_inflight_source;
mod batch_observability_source;
mod batch_priority_queue_source;
mod batch_scheduler_source;
mod batch_tonic_stream_source;
mod batch_wire_source;
mod driver_error_source;
mod driver_transaction_error_source;
mod forwarding_metadata_source;
mod gc_safe_point_realtikv_source;
mod gc_safe_point_source;
mod interface_source;
mod key_ranges_source;
mod kv_package_source;
mod lock_model_source;
mod mutable_transaction_runtime_source;
mod optimistic_2pc_failure_branch_source;
mod optimistic_2pc_realtikv_source;
mod pd_region_loader_source;
mod pessimistic_lock_realtikv_source;
mod pessimistic_lock_source;
mod pessimistic_prewrite_recovery_realtikv_source;
mod physical_channel_evidence_source;
mod prefix_filter_source;
mod prefix_ops_source;
mod primitives;
mod region_batch_locate_source;
mod region_bucket_source;
mod region_cache_source;
mod region_cache_ttl_source;
mod region_cache_validity_source;
mod region_end_key_source;
mod region_epoch_bucket_inheritance_source;
mod region_error_recovery_source;
mod region_scan_residual_source;
mod region_topology_observation_source;
mod region_topology_source;
mod replica_health_scoring_source;
mod replica_selector_source;
mod request_selector_source;
mod resource_group_tag_source;
mod route_policy_seam_source;
mod shared_read_runtime_source;
mod snapshot_scan_page_deadline_source;
mod start_ts_conflict_fidelity_source;
mod store_failure_state_source;
mod tikv_active_cancellation_source;
mod tikv_client_contract_source;
mod tikv_client_coprocessor_transport_source;
mod tikv_client_transaction_stack_source;
mod tikv_commit_outcome_parity_source;
mod tikv_mem_buffer_backend_source;
mod tikv_tonic_coprocessor_source;
mod tikv_transaction_driver_source;
mod tikv_transaction_opener_source;
mod tikv_transaction_rpc_realtikv_source;
mod tikv_transport_failure_source;
mod tikv_unary_command_source;
mod transaction_read_source;
mod transaction_send_source;
mod transport_authority_concurrency_source;
mod trxevents_source_test_contract;
mod union_iter_source;
mod unlocked_region_lookup_source;
