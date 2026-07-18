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

//! Logical datasource input for the bounded index access-task transition.

use crate::{
    access_path::DataSourceAccessPath,
    index_task::IndexTask,
    logical_data_source_task::{build_index_task, IndexTaskProperty},
};

/// A logical datasource with the access paths already produced by ranger.
///
/// This intentionally owns no expressions, table metadata, partition state,
/// or optimizer cache.  Those source responsibilities are prerequisites for a
/// full `logicalop.DataSource`; this type is the smallest real owner needed to
/// drive `IndexAccessPath` into a physical index task.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalDataSource {
    physical_plan_id: i32,
    query_block_offset: i32,
    possible_access_paths: Vec<DataSourceAccessPath>,
}

impl LogicalDataSource {
    /// Creates a datasource and its already-enumerated access paths.
    #[must_use]
    pub fn new(
        physical_plan_id: i32,
        query_block_offset: i32,
        possible_access_paths: impl IntoIterator<Item = DataSourceAccessPath>,
    ) -> Self {
        Self {
            physical_plan_id,
            query_block_offset,
            possible_access_paths: possible_access_paths.into_iter().collect(),
        }
    }

    /// Returns the plan ID assigned to its physical index scan.
    #[must_use]
    pub const fn physical_plan_id(&self) -> i32 {
        self.physical_plan_id
    }

    /// Returns the source query-block offset.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns paths in source enumeration order.
    ///
    /// Order is observable because equal-cost task comparison retains the
    /// first valid task, matching Go `compareTaskCost`.
    #[must_use]
    pub fn possible_access_paths(&self) -> &[DataSourceAccessPath] {
        &self.possible_access_paths
    }

    /// Builds the best dependency-closed index task for this datasource.
    ///
    /// This is the production-facing `LogicalDataSource -> IndexAccessPath ->
    /// IndexTask` transition.  Unsupported source alternatives are reported
    /// as an explicit invalid task by the builder.
    #[must_use]
    pub fn build_index_task(&self, property: IndexTaskProperty) -> IndexTask {
        build_index_task(self, property)
    }
}
