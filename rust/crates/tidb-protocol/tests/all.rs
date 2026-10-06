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

//! All `tidb-protocol` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod binary_params_source;
mod column_metadata_source;
mod error_packet_source;
mod packetio_source;
mod prepared_statement_protocol_source;
mod result_source;
mod resultset_source;
mod resultset_stream_source;
mod server_internal_testutil_source;
mod server_internal_util_source;
mod textrow_go_vectors;
mod textrow_source;
