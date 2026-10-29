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
mod driver_error_source;
mod gc_safe_point_realtikv_source;
mod gc_safe_point_source;
mod interface_source;
mod kv_package_source;
mod primitives;
mod resource_group_tag_source;
mod tikv_client_coprocessor_transport_source;
mod tikv_client_transaction_stack_source;
mod tikv_unary_command_source;
mod trxevents_source_test_contract;
