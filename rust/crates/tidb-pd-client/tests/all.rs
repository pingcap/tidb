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

//! All topology-independent `tidb-pd-client` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod engine_source;
mod pd_client_source;
mod pd_worker_lifecycle_source;
mod tls_handshake_source;
mod tso_source;
