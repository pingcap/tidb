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

//! Typed task shapes for the dependency-closed index-only planner branch.

use crate::{
    physical_index_scan::PhysicalIndexScanPlan,
    physical_table_dual::PhysicalTableDualPlan,
    physical_table_reader::{MissingTableDescriptorError, PhysicalTableReaderPlan},
    physical_table_scan::PhysicalTableScanPlan,
    scan_pushdown::UnsupportedScanFeature,
};

/// Why the bounded index task builder did not construct a task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexTaskRejection {
    /// The datasource has no paths to enumerate.
    NoAccessPaths,
    /// The path is a table scan, which needs the table-scan task owner.
    TablePath,
    /// The path needs the index-merge task owner.
    IndexMergePath,
    /// The upstream point-get decision was not provided.
    UnprovenPointGetAdmission,
    /// Go admitted PointGet or BatchPointGet, whose task/cost owner is absent.
    PointGetRequired,
    /// TiFlash does not support an index scan.
    TiFlashStore,
    /// A double read needs a physical table plan and index lookup task.
    DoubleRead,
    /// A multi-valued index must use index merge in the source path.
    MultiValuedIndex,
    /// Source `CountAfterAccess` was absent or not a valid finite row count.
    InvalidCountAfterAccess,
    /// A limited `ExpectedCnt` lacks an exact upstream adjusted cardinality.
    ExpectedCountUnsupported,
    /// This bounded slice has no keep-order or merge-sort attachment owner.
    RequiredOrdering,
    /// This task type needs a task shape not represented by this slice.
    UnsupportedTaskType,
    /// Root requires the full Go IndexReader cost/attachment owner.
    RootIndexReaderUnsupported,
}

/// Why the bounded table-path transition did not construct a reader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableTaskRejection {
    /// The upstream PointGet decision was not supplied.
    UnprovenPointGetAdmission,
    /// Go admitted PointGet or BatchPointGet, whose owner is absent.
    PointGetRequired,
    /// The bounded table reader accepts only TiKV row-store paths.
    TiFlashStore,
    /// Filters require a Selection tree that this transition does not own.
    Filters,
    /// Ordered or descending table scans need the full property matcher.
    RequiredOrdering,
    /// Only a root property reaches the represented TableReader conversion.
    UnsupportedTaskType,
    /// Limited `ExpectedCnt` adjustment needs source statistics/cost authority.
    ExpectedCountUnsupported,
    /// Source `CountAfterAccess` was not a finite nonnegative row count.
    InvalidCountAfterAccess,
    /// Partition pruning and physical-table identity are not represented.
    Partition,
    /// `TABLESAMPLE` uses a separate physical plan and executor path.
    TableSample,
    /// The pre-resolved scan named a feature outside the bounded TiKV path.
    UnsupportedScanFeature(UnsupportedScanFeature),
    /// A raw DAG scan lacks the source descriptor required by TableReader.
    MissingSourceDescriptor,
}

/// Why the unified datasource scan transition failed before task construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanReadTaskRejection {
    /// The existing index-only builder rejected the represented paths.
    Index(IndexTaskRejection),
    /// Table and index candidates need a shared cost comparison not yet owned.
    MixedAccessPaths,
    /// More than one table candidate needs source cost comparison.
    MultipleTablePaths,
    /// Index merge requires its own multi-plan task shape.
    IndexMergePath,
    /// The single table candidate failed one explicit source admission.
    Table(TableTaskRejection),
}

/// A single-read Cop task holding one physical index scan.
#[derive(Clone, Debug, PartialEq)]
pub struct CopIndexTask {
    index_plan: PhysicalIndexScanPlan,
}

impl CopIndexTask {
    /// Creates a single-read Cop task from its physical index scan.
    #[must_use]
    pub const fn new(index_plan: PhysicalIndexScanPlan) -> Self {
        Self { index_plan }
    }

    /// Returns the physical index-scan plan sent to the Cop task.
    #[must_use]
    pub const fn index_plan(&self) -> &PhysicalIndexScanPlan {
        &self.index_plan
    }

    /// Returns the task cost owned by the physical index scan.
    #[must_use]
    pub const fn cost(&self) -> f64 {
        self.index_plan.cost()
    }
}

/// A single-read Cop task holding one physical table scan before root
/// conversion.
#[derive(Clone, Debug, PartialEq)]
pub struct CopTableTask {
    table_plan: PhysicalTableScanPlan,
}

impl CopTableTask {
    /// Creates Go's table-side Cop task from its physical table scan.
    #[must_use]
    pub const fn new(table_plan: PhysicalTableScanPlan) -> Self {
        Self { table_plan }
    }

    /// Returns the physical table scan attached to the Cop task.
    #[must_use]
    pub const fn table_plan(&self) -> &PhysicalTableScanPlan {
        &self.table_plan
    }

    /// Performs the bounded table-only branch of
    /// `CopTask.convertToRootTaskImpl`.
    pub fn convert_to_root(self) -> Result<PhysicalTableReaderPlan, MissingTableDescriptorError> {
        PhysicalTableReaderPlan::from_table_scan(self.table_plan)
    }
}

/// A dependency-closed table/index scan task returned by a datasource.
#[derive(Clone, Debug, PartialEq)]
pub enum ScanReadTask {
    /// The existing source-admitted index-only Cop task.
    Index(CopIndexTask),
    /// One TiKV table Cop task converted to a root TableReader.
    TableReader(PhysicalTableReaderPlan),
    /// An empty ranger result returned before any task construction.
    TableDual(PhysicalTableDualPlan),
    /// A deliberately unsupported path/property combination.
    Invalid(ScanReadTaskRejection),
}

impl ScanReadTask {
    /// Converts the established index-only result into the unified scan task
    /// without changing its behavior.
    #[must_use]
    pub fn from_index_task(task: IndexTask) -> Self {
        match task {
            IndexTask::CopSingleRead(task) => Self::Index(task),
            IndexTask::TableDual(plan) => Self::TableDual(plan),
            IndexTask::Invalid(reason) => Self::Invalid(ScanReadTaskRejection::Index(reason)),
        }
    }

    /// Returns the represented physical index scan.
    #[must_use]
    pub const fn index_plan(&self) -> Option<&PhysicalIndexScanPlan> {
        match self {
            Self::Index(task) => Some(task.index_plan()),
            Self::TableReader(_) | Self::TableDual(_) | Self::Invalid(_) => None,
        }
    }

    /// Returns the root table reader built from one table Cop task.
    #[must_use]
    pub const fn table_reader(&self) -> Option<&PhysicalTableReaderPlan> {
        match self {
            Self::TableReader(reader) => Some(reader),
            Self::Index(_) | Self::TableDual(_) | Self::Invalid(_) => None,
        }
    }

    /// Returns the zero-row TableDual when ranger proved a path empty.
    #[must_use]
    pub const fn table_dual(&self) -> Option<PhysicalTableDualPlan> {
        match self {
            Self::TableDual(plan) => Some(*plan),
            Self::Index(_) | Self::TableReader(_) | Self::Invalid(_) => None,
        }
    }

    /// Returns the explicit invalid reason, if this is not a task.
    #[must_use]
    pub const fn rejection(&self) -> Option<ScanReadTaskRejection> {
        match self {
            Self::Invalid(reason) => Some(*reason),
            Self::Index(_) | Self::TableReader(_) | Self::TableDual(_) => None,
        }
    }

    /// Reports whether this is a constructed task.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        !matches!(self, Self::Invalid(_))
    }
}

/// A task returned from the bounded datasource index path.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexTask {
    /// An index scan retained as a single-read Cop task.
    CopSingleRead(CopIndexTask),
    /// An empty ranger result returned as Go's zero-row TableDual root task.
    TableDual(PhysicalTableDualPlan),
    /// A deliberately unsupported path or property.
    Invalid(IndexTaskRejection),
}

impl IndexTask {
    /// Returns the physical index scan when this is a valid index task.
    #[must_use]
    pub fn index_plan(&self) -> Option<&PhysicalIndexScanPlan> {
        match self {
            Self::CopSingleRead(task) => Some(task.index_plan()),
            Self::TableDual(_) | Self::Invalid(_) => None,
        }
    }

    /// Returns the zero-row TableDual when ranger proved the path empty.
    #[must_use]
    pub const fn table_dual(&self) -> Option<PhysicalTableDualPlan> {
        match self {
            Self::TableDual(plan) => Some(*plan),
            Self::CopSingleRead(_) | Self::Invalid(_) => None,
        }
    }

    /// Returns the task cost for valid tasks.
    #[must_use]
    pub fn cost(&self) -> Option<f64> {
        match self {
            Self::CopSingleRead(task) => Some(task.cost()),
            Self::TableDual(_) => Some(0.0),
            Self::Invalid(_) => None,
        }
    }

    /// Returns the explicit invalid reason, if this is not a task.
    #[must_use]
    pub const fn rejection(&self) -> Option<IndexTaskRejection> {
        match self {
            Self::Invalid(reason) => Some(*reason),
            Self::CopSingleRead(_) | Self::TableDual(_) => None,
        }
    }

    /// Reports whether this is a constructed task.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        !matches!(self, Self::Invalid(_))
    }

    /// Selects the strictly lower-cost task, preserving `current` on a tie.
    #[must_use]
    pub fn choose_lower_cost(current: Self, challenger: Self) -> Self {
        match (current.cost(), challenger.cost()) {
            (Some(_), None) => current,
            (None, Some(_)) => challenger,
            (None, None) => current,
            (Some(current_cost), Some(challenger_cost)) => {
                if challenger_cost < current_cost {
                    challenger
                } else {
                    current
                }
            }
        }
    }
}
