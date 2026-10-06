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

//! All topology-independent `tidb-meta` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod go_vectors;
mod iter_databases_source;
mod job_name_go_vectors;
mod key_prefix_and_element_source;
mod meta_test_go_parity;
mod meta_test_part2_go_parity;
mod meta_test_part3_go_parity;
mod structure_source;
mod transaction_source;
