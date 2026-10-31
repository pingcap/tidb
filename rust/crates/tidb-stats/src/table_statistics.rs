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

//! Go `pkg/statistics`' statistics-version constant and the planner's
//! predicate-column usage result. The statistics objects themselves are Go's
//! `statistics.Table`, `Column` and `Index` ([`crate::Table`]).

/// Go `statistics.Version2`.
pub const VERSION2: i64 = 2;

/// Go `CollectColumnStatsUsage`'s statistics-loading result.
#[derive(Debug, Default)]
pub struct ColumnStatsUsage {
    /// Base-table column -> whether its complete histogram is required.
    pub predicate_columns: std::collections::HashMap<tidb_model::TableItemID, bool>,
    /// Logical table IDs visited by data sources.
    pub visited_logical_table_ids: std::collections::HashSet<i64>,
    /// Logical table ID -> physical partition IDs, for static pruning.
    pub table_partition_ids: std::collections::HashMap<i64, Vec<i64>>,
    /// Physical table ID -> index IDs retained by Go's first pruning phase.
    /// A table is present only when pruning actually removed a path.
    pub kept_index_ids: std::collections::HashMap<i64, std::collections::HashSet<i64>>,
    /// Number of logical operators visited.
    pub operator_count: u64,
}
