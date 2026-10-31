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

//! Go `physicalop.PhysPlanPartInfo` (`physical_plan_misc.go:79`).
//!
//! Go attaches one of these to a cop task, to the table scan under it and to
//! the reader above it, built from the DataSource (`buildPhysPlanPartInfo`,
//! `find_best_task.go:90`): `PruningConds` is `ds.AllConds` for EVERY table,
//! partitioned or not, and `Columns` / `ColumnNames` are the table's full
//! column list. Nothing in this port reads it for planning -- dynamic
//! partition access is retained separately -- but the plan cache charges a
//! plan for every copy it holds, so the same values are kept here and shared
//! behind an [`Arc`] the way Go shares the pointer.

use std::sync::Arc;

use tidb_datatype::{FieldName, FieldNameMetadata, IdentifierMetadata};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

use tidb_planner_core_operator_logicalop::DataSource;

/// Go `unsafe.Sizeof(PhysPlanPartInfo{})`.
const GO_EMPTY_PARTITION_INFO_SIZE: i64 = 96;

/// Go `PhysPlanPartInfo`.
#[derive(Clone, Debug, Default)]
pub struct PhysPlanPartInfo {
    /// Go `PruningConds`.
    pub pruning_conds: Vec<Expression>,
    /// Go `PartitionNames`.
    pub partition_names: Vec<IdentifierMetadata>,
    /// Go `Columns`.
    pub columns: Vec<Column>,
    /// Go `ColumnNames`.
    pub column_names: Vec<FieldName>,
}

impl PhysPlanPartInfo {
    /// Go `buildPhysPlanPartInfo(ds)` (`find_best_task.go:90`) with
    /// `rule.ReconstructTableColNames` (`rule_partition_processor.go:460`):
    /// one name per table column, the extra handle, physical-table-id and
    /// commit-ts columns under their internal names.
    #[must_use]
    pub fn from_data_source(ds: &DataSource) -> Arc<Self> {
        let db = IdentifierMetadata::new(ds.db_name.as_str());
        let table = IdentifierMetadata::new(ds.table_name.as_str());
        let column_names = ds
            .table_columns
            .iter()
            .map(|column| {
                let (name, original_table) = match column.id {
                    tidb_model::column::EXTRA_HANDLE_ID => {
                        (tidb_model::column::EXTRA_HANDLE_NAME.to_owned(), None)
                    }
                    tidb_model::column::EXTRA_PHYS_TBL_ID => {
                        (tidb_model::column::EXTRA_PHYS_TBL_ID_NAME.to_owned(), None)
                    }
                    tidb_model::column::EXTRA_COMMIT_TS_ID => {
                        (tidb_model::column::EXTRA_COMMIT_TS_NAME.to_owned(), None)
                    }
                    id => (
                        ds.columns
                            .iter()
                            .find(|info| info.read().id == id)
                            .map(|info| info.read().name.original().to_owned())
                            .unwrap_or_default(),
                        Some(&table),
                    ),
                };
                let name = IdentifierMetadata::new(name.as_str());
                FieldName::new(FieldNameMetadata {
                    original_table: original_table.cloned().unwrap_or_default(),
                    original_column: name.clone(),
                    database: db.clone(),
                    table: table.clone(),
                    column: name,
                })
            })
            .collect();
        Arc::new(Self {
            pruning_conds: ds.all_conds.clone(),
            partition_names: ds
                .partition_names
                .iter()
                .map(|name| IdentifierMetadata::new(name.as_str()))
                .collect(),
            columns: ds.table_columns.clone(),
            column_names,
        })
    }

    /// Go `PhysPlanPartInfo.MemoryUsage` (`physical_plan_misc.go:136`).
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        GO_EMPTY_PARTITION_INFO_SIZE
            + self
                .pruning_conds
                .iter()
                .map(Expression::memory_usage)
                .sum::<i64>()
            + self
                .partition_names
                .iter()
                .map(IdentifierMetadata::memory_usage)
                .sum::<i64>()
            + self.columns.iter().map(Column::memory_usage).sum::<i64>()
            + self
                .column_names
                .iter()
                .map(FieldName::memory_usage)
                .sum::<i64>()
    }
}

/// Go `(*PhysPlanPartInfo).MemoryUsage` on a possibly nil pointer.
#[must_use]
pub fn part_info_memory_usage(info: Option<&Arc<PhysPlanPartInfo>>) -> i64 {
    info.map_or(0, |info| info.memory_usage())
}
