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

//! All topology-independent `tidb-store-driver-txn` integration tests in one process.

mod async_commit_one_pc_realtikv_source;
mod batch_getter_source;
mod driver_transaction_error_source;
mod mutable_transaction_runtime_source;
mod optimistic_2pc_realtikv_source;
mod pessimistic_lock_realtikv_source;
mod pessimistic_prewrite_recovery_realtikv_source;
mod realtikv_fixture;
mod tikv_mem_buffer_backend_source;
mod tikv_transaction_driver_source;
mod transaction_read_source;
mod transaction_send_source;
mod union_iter_source;
