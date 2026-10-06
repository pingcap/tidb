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

//! All topology-independent `tidb-expr` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod benchmark_source;
mod field_name_resolution_source;
mod info_metadata_source;
mod pb_int_predicate_source;
mod pb_string_predicate_source;
