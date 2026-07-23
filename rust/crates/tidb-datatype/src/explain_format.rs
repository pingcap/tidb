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

//! EXPLAIN format authority from `pkg/types/explain_format.go`.

/// Row format without explain-ID suffixes.
pub const EXPLAIN_FORMAT_BRIEF: &str = "brief";
/// Graphviz DOT format.
pub const EXPLAIN_FORMAT_DOT: &str = "dot";
/// Optimizer hint format.
pub const EXPLAIN_FORMAT_HINT: &str = "hint";
/// JSON format.
pub const EXPLAIN_FORMAT_JSON: &str = "json";
/// Tabular row format.
pub const EXPLAIN_FORMAT_ROW: &str = "row";
/// Verbose row format.
pub const EXPLAIN_FORMAT_VERBOSE: &str = "verbose";
/// Traditional alias for row format.
pub const EXPLAIN_FORMAT_TRADITIONAL: &str = "traditional";
/// Cost format using true cardinality.
pub const EXPLAIN_FORMAT_TRUE_CARD_COST: &str = "true_card_cost";
/// Binary-plan protobuf format.
pub const EXPLAIN_FORMAT_BINARY: &str = "binary";
/// TiDB JSON wrapper format.
pub const EXPLAIN_FORMAT_TIDB_JSON: &str = "tidb_json";
/// Cost and cost-formula trace.
pub const EXPLAIN_FORMAT_COST_TRACE: &str = "cost_trace";
/// Non-prepared plan-cache diagnostics.
pub const EXPLAIN_FORMAT_PLAN_CACHE: &str = "plan_cache";
/// Tree-structured plan format.
pub const EXPLAIN_FORMAT_PLAN_TREE: &str = "plan_tree";

/// Complete validator order from source `ExplainFormats`.
pub const EXPLAIN_FORMATS: [&str; 13] = [
    EXPLAIN_FORMAT_BRIEF,
    EXPLAIN_FORMAT_DOT,
    EXPLAIN_FORMAT_HINT,
    EXPLAIN_FORMAT_JSON,
    EXPLAIN_FORMAT_ROW,
    EXPLAIN_FORMAT_VERBOSE,
    EXPLAIN_FORMAT_TRADITIONAL,
    EXPLAIN_FORMAT_TRUE_CARD_COST,
    EXPLAIN_FORMAT_BINARY,
    EXPLAIN_FORMAT_TIDB_JSON,
    EXPLAIN_FORMAT_COST_TRACE,
    EXPLAIN_FORMAT_PLAN_CACHE,
    EXPLAIN_FORMAT_PLAN_TREE,
];
