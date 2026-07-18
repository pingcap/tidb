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

//! Bounded datasource-to-scan task construction.
//!
//! This is the dependency-closed index-only transition from Go
//! `findBestTask4LogicalDataSource` through `convertToIndexScan`. It preserves
//! Go's empty-range TableDual control flow, but does not invent PointGet,
//! IndexReader, or limited-cardinality behavior without their owners. The
//! table branch directly translates `convertToTableScan` followed by the
//! table-only branch of `CopTask.convertToRootTaskImpl` for one admitted TiKV
//! path.

use crate::{
    access_path::{
        AccessPathStore, DataSourceAccessPath, IndexAccessPath, IndexReadShape, PointGetAdmission,
        TableAccessPath,
    },
    index_task::{
        CopIndexTask, CopTableTask, IndexTask, IndexTaskRejection, ScanReadTask,
        ScanReadTaskRejection, TableTaskRejection,
    },
    logical_data_source::LogicalDataSource,
    physical_index_scan::PhysicalIndexScanPlan,
    physical_property::IndexOrderingRequirement,
    physical_table_dual::PhysicalTableDualPlan,
    physical_table_scan::PhysicalTableScanPlan,
    task_type::TaskType,
};

/// Dependency-closed task-property input accepted by the index task builder.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexTaskProperty {
    task_type: TaskType,
    ordering: IndexOrderingRequirement,
    expected_cnt: f64,
}

impl IndexTaskProperty {
    /// Creates a property with no ordering requirement and Go's unbounded
    /// `ExpectedCnt` default (`math.MaxFloat64`).
    #[must_use]
    pub const fn new(task_type: TaskType) -> Self {
        Self {
            task_type,
            ordering: IndexOrderingRequirement::None,
            expected_cnt: f64::MAX,
        }
    }

    /// Adds an ordering requirement.
    #[must_use]
    pub const fn with_ordering(mut self, ordering: IndexOrderingRequirement) -> Self {
        self.ordering = ordering;
        self
    }

    /// Sets the source `PhysicalProperty.ExpectedCnt` value.
    #[must_use]
    pub const fn with_expected_cnt(mut self, expected_cnt: f64) -> Self {
        self.expected_cnt = expected_cnt;
        self
    }

    /// Returns the requested execution task type.
    #[must_use]
    pub const fn task_type(&self) -> TaskType {
        self.task_type
    }

    /// Returns the ordering requirement.
    #[must_use]
    pub const fn ordering(&self) -> IndexOrderingRequirement {
        self.ordering
    }

    /// Returns the source expected row count.
    #[must_use]
    pub const fn expected_cnt(&self) -> f64 {
        self.expected_cnt
    }

    const fn has_unbounded_expected_cnt(&self) -> bool {
        self.expected_cnt == f64::MAX
    }
}

/// Builds the best supported index task in source path enumeration order.
///
/// An empty ranger result immediately returns the zero-row TableDual, exactly
/// as Go does before trying PointGet, table, or index alternatives. Other
/// unsupported alternatives are ignored while a valid index alternative
/// exists; if none exists the first rejection explains the boundary.
#[must_use]
pub fn build_index_task(source: &LogicalDataSource, property: IndexTaskProperty) -> IndexTask {
    let mut best_task = None;
    let mut first_rejection = None;

    for path in source.possible_access_paths() {
        if let DataSourceAccessPath::Index(index_path) = path {
            if index_path.has_empty_ranges() {
                return IndexTask::TableDual(PhysicalTableDualPlan::init(
                    0,
                    source.query_block_offset(),
                ));
            }
        }

        let task = build_path_task(source, property, path);
        if task.is_valid() {
            best_task = Some(match best_task {
                Some(current) => IndexTask::choose_lower_cost(current, task),
                None => task,
            });
        } else if first_rejection.is_none() {
            first_rejection = task.rejection();
        }
    }

    best_task.unwrap_or_else(|| {
        IndexTask::Invalid(first_rejection.unwrap_or(IndexTaskRejection::NoAccessPaths))
    })
}

/// Builds the dependency-closed table/index read task.
///
/// Empty ranges retain Go's early TableDual return. Index-only inputs delegate
/// to [`build_index_task`] unchanged. A table path is accepted only when it is
/// the sole candidate because comparing it with another table or index path
/// needs cost authorities outside this bounded transition.
#[must_use]
pub fn build_scan_read_task(
    source: &LogicalDataSource,
    property: IndexTaskProperty,
) -> ScanReadTask {
    for path in source.possible_access_paths() {
        let empty = match path {
            DataSourceAccessPath::Index(path) => path.has_empty_ranges(),
            DataSourceAccessPath::Table(path) => path.has_empty_ranges(),
            DataSourceAccessPath::IndexMerge => false,
        };
        if empty {
            return ScanReadTask::TableDual(PhysicalTableDualPlan::init(
                0,
                source.query_block_offset(),
            ));
        }
    }

    let mut table_path = None;
    let mut has_index = false;
    let mut has_index_merge = false;
    for path in source.possible_access_paths() {
        match path {
            DataSourceAccessPath::Table(path) => {
                if table_path.is_some() {
                    return ScanReadTask::Invalid(ScanReadTaskRejection::MultipleTablePaths);
                }
                table_path = Some(path);
            }
            DataSourceAccessPath::Index(_) => has_index = true,
            DataSourceAccessPath::IndexMerge => has_index_merge = true,
        }
    }

    let Some(table_path) = table_path else {
        return ScanReadTask::from_index_task(build_index_task(source, property));
    };
    if has_index_merge {
        return ScanReadTask::Invalid(ScanReadTaskRejection::IndexMergePath);
    }
    if has_index {
        return ScanReadTask::Invalid(ScanReadTaskRejection::MixedAccessPaths);
    }

    build_supported_table_task(source, property, table_path)
}

fn build_supported_table_task(
    source: &LogicalDataSource,
    property: IndexTaskProperty,
    path: &TableAccessPath,
) -> ScanReadTask {
    match path.point_get_admission() {
        PointGetAdmission::Unproven => {
            return invalid_table(TableTaskRejection::UnprovenPointGetAdmission);
        }
        PointGetAdmission::Eligible => {
            return invalid_table(TableTaskRejection::PointGetRequired);
        }
        PointGetAdmission::NotEligible => {}
    }
    if path.store() != AccessPathStore::TiKv {
        return invalid_table(TableTaskRejection::TiFlashStore);
    }
    if path.is_partitioned()
        || matches!(
            path.pushdown().unsupported,
            Some(crate::scan_pushdown::UnsupportedScanFeature::Partition)
        )
    {
        return invalid_table(TableTaskRejection::Partition);
    }
    if path.is_table_sample() {
        return invalid_table(TableTaskRejection::TableSample);
    }
    if property.ordering() != IndexOrderingRequirement::None
        || path.pushdown().keep_order
        || path.pushdown().desc
    {
        return invalid_table(TableTaskRejection::RequiredOrdering);
    }
    if property.task_type() != TaskType::Root {
        return invalid_table(TableTaskRejection::UnsupportedTaskType);
    }
    if !property.has_unbounded_expected_cnt() {
        return invalid_table(TableTaskRejection::ExpectedCountUnsupported);
    }
    if path.has_filters() {
        return invalid_table(TableTaskRejection::Filters);
    }
    if let Some(feature) = path.pushdown().unsupported {
        return invalid_table(TableTaskRejection::UnsupportedScanFeature(feature));
    }

    let Some(scan) = PhysicalTableScanPlan::init(
        source.physical_plan_id(),
        source.query_block_offset(),
        path.pushdown().clone(),
    )
    .try_with_source_estimated_rows(path.count_after_access()) else {
        return invalid_table(TableTaskRejection::InvalidCountAfterAccess);
    };
    let cop_task = CopTableTask::new(scan);
    ScanReadTask::TableReader(cop_task.convert_to_root())
}

const fn invalid_table(reason: TableTaskRejection) -> ScanReadTask {
    ScanReadTask::Invalid(ScanReadTaskRejection::Table(reason))
}

fn build_path_task(
    source: &LogicalDataSource,
    property: IndexTaskProperty,
    path: &DataSourceAccessPath,
) -> IndexTask {
    let index_path = match path {
        DataSourceAccessPath::Index(path) => path,
        DataSourceAccessPath::Table(_) => {
            return IndexTask::Invalid(IndexTaskRejection::TablePath);
        }
        DataSourceAccessPath::IndexMerge => {
            return IndexTask::Invalid(IndexTaskRejection::IndexMergePath);
        }
    };

    build_supported_index_task(source, property, index_path)
}

fn build_supported_index_task(
    source: &LogicalDataSource,
    property: IndexTaskProperty,
    path: &IndexAccessPath,
) -> IndexTask {
    if property.ordering() != IndexOrderingRequirement::None {
        return IndexTask::Invalid(IndexTaskRejection::RequiredOrdering);
    }
    match path.point_get_admission() {
        PointGetAdmission::Unproven => {
            return IndexTask::Invalid(IndexTaskRejection::UnprovenPointGetAdmission);
        }
        PointGetAdmission::Eligible => {
            return IndexTask::Invalid(IndexTaskRejection::PointGetRequired);
        }
        PointGetAdmission::NotEligible => {}
    }
    if path.store() == AccessPathStore::TiFlash {
        return IndexTask::Invalid(IndexTaskRejection::TiFlashStore);
    }
    if path.read_shape() == IndexReadShape::DoubleRead {
        return IndexTask::Invalid(IndexTaskRejection::DoubleRead);
    }
    if path.is_multi_valued() {
        return IndexTask::Invalid(IndexTaskRejection::MultiValuedIndex);
    }
    if property.task_type() == TaskType::Root {
        return IndexTask::Invalid(IndexTaskRejection::RootIndexReaderUnsupported);
    }
    if property.task_type() != TaskType::CopSingleRead {
        return IndexTask::Invalid(IndexTaskRejection::UnsupportedTaskType);
    }

    let Some(rows) = scan_rows_for_property(path, property) else {
        return IndexTask::Invalid(if property.has_unbounded_expected_cnt() {
            IndexTaskRejection::InvalidCountAfterAccess
        } else {
            IndexTaskRejection::ExpectedCountUnsupported
        });
    };
    if !rows.is_finite() || rows < 0.0 {
        return IndexTask::Invalid(IndexTaskRejection::InvalidCountAfterAccess);
    }

    let mut scan = PhysicalIndexScanPlan::init(
        source.physical_plan_id(),
        source.query_block_offset(),
        path.candidate(),
        rows,
    );
    if let Some(pushdown) = path.pushdown() {
        scan = scan.with_validated_pushdown(pushdown.clone());
    }
    IndexTask::CopSingleRead(CopIndexTask::new(scan))
}

fn scan_rows_for_property(path: &IndexAccessPath, property: IndexTaskProperty) -> Option<f64> {
    if property.has_unbounded_expected_cnt() {
        return path.count_after_access();
    }
    if !property.expected_cnt().is_finite() || property.expected_cnt() < 0.0 {
        return None;
    }
    let rows = path.expected_count_rows()?;
    (rows.expected_cnt() == property.expected_cnt()).then_some(rows.scan_rows())
}
