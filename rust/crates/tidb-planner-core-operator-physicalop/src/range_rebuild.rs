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

//! The access conditions a cached physical plan keeps so the plan cache can
//! rebuild its ranges for new parameters -- Go's `PhysicalTableScan` /
//! `PhysicalIndexScan` / `PointGetPlan` / index-join fields that
//! `plan_cache_rebuild.go` reads. Rebuilding them is core's
//! (`physical_plan_cache`, `index_join_path`).

use tidb_datatype::{Datum, FieldType};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

/// The table-scan facts retained specifically so a cache hit can rebuild its
/// parameter-dependent ranges without re-running logical optimization.
#[derive(Clone, Debug)]
pub struct TableRangeRebuild {
    /// Go `PhysicalTableScan.AccessCondition`.
    pub access_conditions: Vec<Expression>,
    /// Integer-handle type.  `None` selects the common-handle index columns.
    pub handle_type: Option<FieldType>,
    /// Go primary-index columns for a common handle.
    pub common_handle_columns: Vec<Column>,
    /// Prefix lengths aligned with [`Self::common_handle_columns`].
    pub common_handle_lengths: Vec<i64>,
    /// Whether the integer handle is unsigned.
    pub unsigned_int_handle: bool,
}

impl TableRangeRebuild {
    /// Retains an integer primary-key handle range source.
    #[must_use]
    pub fn int_handle(
        access_conditions: Vec<Expression>,
        handle_type: FieldType,
        unsigned_int_handle: bool,
    ) -> Self {
        Self {
            access_conditions,
            handle_type: Some(handle_type),
            common_handle_columns: Vec::new(),
            common_handle_lengths: Vec::new(),
            unsigned_int_handle,
        }
    }

    /// Retains a clustered common-handle range source.
    #[must_use]
    pub fn common_handle(
        access_conditions: Vec<Expression>,
        columns: Vec<Column>,
        lengths: Vec<i64>,
    ) -> Self {
        Self {
            access_conditions,
            handle_type: None,
            common_handle_columns: columns,
            common_handle_lengths: lengths,
            unsigned_int_handle: false,
        }
    }
}

/// The index-scan facts retained specifically so a cache hit can rebuild its
/// parameter-dependent ranges.
#[derive(Clone, Debug)]
pub struct IndexRangeRebuild {
    /// Go `PhysicalIndexScan.AccessCondition`.
    pub access_conditions: Vec<Expression>,
    /// Go `PhysicalIndexScan.IdxCols`.
    pub index_columns: Vec<Column>,
    /// Go `PhysicalIndexScan.IdxColLens`.
    pub index_column_lengths: Vec<i64>,
}

impl IndexRangeRebuild {
    /// Retains a complete index range source.
    #[must_use]
    pub fn new(
        access_conditions: Vec<Expression>,
        index_columns: Vec<Column>,
        index_column_lengths: Vec<i64>,
    ) -> Self {
        Self {
            access_conditions,
            index_columns,
            index_column_lengths,
        }
    }
}

/// Range metadata retained by point, batch-point and index-join plans. Go rebuilds CBO
/// point plans from access conditions exactly like their originating table or
/// index scan, then additionally verifies that the number of point ranges did
/// not change.
#[derive(Clone, Debug)]
pub enum PointRangeRebuild {
    /// A primary-key/table-handle point range.
    Table(TableRangeRebuild),
    /// A unique-index point range.
    Index(IndexRangeRebuild),
    /// Go `mutableIndexJoinRange`: the template ranges rebuilt by rerunning
    /// `indexJoinPathBuild` with the new parameters.
    IndexJoin(Box<IndexJoinPathRebuild>),
    /// A fast point plan's keys (`TryFastPlan`), rebuilt from its constants.
    Constants(PointConstantsRebuild),
}

/// One key part of a fast point plan: Go `PointGetPlan.HandleConstant` /
/// `IndexConstants[i]`, or `BatchPointGetPlan.HandleParams[i]` /
/// `IndexValueParams[i][j]`.
#[derive(Clone, Debug)]
pub struct PointKeyConstant {
    /// The EXECUTE parameter this part reads, or `None` for a literal.
    pub marker: Option<usize>,
    /// The planned value: the literal, or the planning execution's
    /// parameter, already in the key column's domain.
    pub value: Datum,
}

/// A fast point plan's keys, which Go's `buildRangesForPointGet` /
/// `buildRangesForBatchGet` move into each execution's parameters through
/// `convertConstant2Datum`; a parameter that does not survive that
/// conversion rebuilds no plan.
#[derive(Clone, Debug)]
pub struct PointConstantsRebuild {
    /// Every candidate key in written order. A batch keeps its repeated
    /// keys here, as Go's plan does, so each execution dedups its own.
    pub rows: Vec<Vec<PointKeyConstant>>,
    /// The key columns' types, aligned with each row.
    pub field_types: Vec<FieldType>,
    /// A batch over a record handle: a repeated key is read once, and an
    /// unsigned integer handle is keyed by its signed bit pattern, as the
    /// planned batch was.
    pub handle_batch: bool,
}

impl PointConstantsRebuild {
    /// Whether any key part reads an EXECUTE parameter.
    #[must_use]
    pub fn has_parameters(&self) -> bool {
        self.rows.iter().flatten().any(|part| part.marker.is_some())
    }
}

/// The inputs `mutableIndexJoinRange.Rebuild` reruns `indexJoinPathBuild`
/// with when a cached plan is reused with new parameters.
#[derive(Clone, Debug)]
pub struct IndexJoinPathRebuild {
    /// Go `IdxCols`: the inner index's columns.
    pub idx_cols: Vec<Column>,
    /// Go `IdxColLens`.
    pub idx_col_lens: Vec<i64>,
    /// The inner side's pushed conditions.
    pub pushed_conditions: Vec<Expression>,
    /// The join's other conditions.
    pub other_conditions: Vec<Expression>,
    /// The outer side's join keys.
    pub outer_join_keys: Vec<Column>,
    /// The inner side's join keys.
    pub inner_join_keys: Vec<Column>,
    /// The inner side's schema.
    pub inner_schema: Schema,
    /// Go `SessionVars.OptPrefixIndexSingleScan`.
    pub opt_prefix_index_single_scan: bool,
}

impl IndexJoinPathRebuild {
    /// The conditions whose parameters a reuse binds.
    pub fn conditions_mut(&mut self) -> [&mut Vec<Expression>; 2] {
        [&mut self.pushed_conditions, &mut self.other_conditions]
    }
}
