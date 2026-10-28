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

//! One table's loaded statistics in the shape the estimator reads them:
//! Go `statistics.Table` reduced to the fields `cardinality` consumes.


pub use tidb_stats::table_statistics::TableStatistics;

/// [`TableStatistics`]' planner half.
pub trait TableStatisticsExt {
    /// Go `cardinality.EstimateColumnNDV` and its `getTotalRowCount` helper.
    ///
    /// Lite statistics initialization keeps every histogram's NDV metadata but
    /// loads buckets and TopN only for the items the query needs. An evicted
    /// column therefore borrows the analyzed row count from the first fully
    /// loaded, same-version index, then from a column. The Rust catalog keeps
    /// the payloads eagerly, so callers pass the items Go's predicate-statistics
    /// collection would have marked fully loaded for this query.
    fn estimate_column_ndv(
        &self,
        column_id: i64,
        full_loaded_columns: &std::collections::BTreeSet<i64>,
        full_loaded_indexes: &std::collections::BTreeSet<i64>,
    ) -> Option<f64>;
}

impl TableStatisticsExt for TableStatistics {
    fn estimate_column_ndv(
        &self,
        column_id: i64,
        full_loaded_columns: &std::collections::BTreeSet<i64>,
        full_loaded_indexes: &std::collections::BTreeSet<i64>,
    ) -> Option<f64> {
        crate::logical::init_stats::estimate_column_ndv(
            self,
            column_id,
            full_loaded_columns,
            full_loaded_indexes,
        )
    }
}
