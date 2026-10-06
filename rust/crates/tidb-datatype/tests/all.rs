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

//! Single integration-test binary for all `tidb-datatype` source tests.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod collation_key_go_vectors;
mod conversion_context_source;
mod datum_sentinel_order_source;
mod field_type_source;
mod json_binary_go_vectors;
mod json_ops_go_source;
mod parser_charset_package_source;
mod parser_format_package_source;
mod truncate_policy_source;
mod value_context_format_source;
mod value_expr_source;
mod vector_source;
