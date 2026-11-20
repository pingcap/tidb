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

//! Go `merge.go`: the merger interface and the dependency plumbing every
//! merger shares.

use std::collections::HashMap;
use std::ops::Deref;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::column::Column;
use tidb_chunk::ColumnRead;
use tidb_datatype::{EvalType, FieldType};
use tidb_driver_error::ExecError;

/// Go `errors.Errorf` / `errors.New`.
pub(crate) fn err(message: impl Into<String>) -> ExecError {
    ExecError::internal(message.into())
}

/// Go `err.Error()`.
pub(crate) fn error_text(error: &ExecError) -> String {
    match error {
        ExecError::Internal(message) => message.to_string(),
        other => format!("{other:?}"),
    }
}

/// Go `errors.Annotate(err, message)`: `message: err`.
pub(crate) fn annotate(error: ExecError, message: impl std::fmt::Display) -> ExecError {
    err(format!("{message}: {}", error_text(&error)))
}

/// Go `aggMerger`. `output_cols` holds one slot per output column; a merger
/// fills its slots with the computed columns.
pub(crate) trait AggMerger: Send + Sync {
    /// Go `outputColIDs`.
    fn output_col_ids(&self) -> &[i64];
    /// Go `mergeChunk`.
    fn merge_chunk(
        &self,
        input: &Chunk,
        computed_by_order: &[Column],
        output_cols: &mut [Option<Column>],
        worker_data: &mut crate::exec::MergeWorkerData,
    ) -> Result<(), ExecError>;
}

/// Go `depRefSource`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DepRefSource {
    /// Go `depFromInput`.
    FromInput,
    /// Go `depFromComputed`.
    FromComputed,
}

/// Go `depRef`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DepRef {
    pub(crate) source: DepRefSource,
    pub(crate) idx: i64,
}

impl Default for DepRef {
    fn default() -> Self {
        Self { source: DepRefSource::FromInput, idx: 0 }
    }
}

/// Go `resolveDepRef`.
pub(crate) fn resolve_dep_ref(
    dep_col_id: i64,
    col_id_to_computed_idx: &HashMap<i64, usize>,
    delta_agg_col_count: i64,
) -> Result<DepRef, ExecError> {
    if let Some(&idx) = col_id_to_computed_idx.get(&dep_col_id) {
        return Ok(DepRef { source: DepRefSource::FromComputed, idx: idx as i64 });
    }
    if dep_col_id < 0 || dep_col_id >= delta_agg_col_count {
        return Err(err(format!(
            "dependency col id {dep_col_id} is invalid: expect delta agg range [0,{delta_agg_col_count}) or previously computed columns"
        )));
    }
    Ok(DepRef { source: DepRefSource::FromInput, idx: dep_col_id })
}

/// A dependency column: an input column of the chunk or a column computed
/// earlier in this chunk's merge.
pub(crate) enum DepColumn<'a> {
    Input(ColumnRead<'a>),
    Computed(&'a Column),
}

impl Deref for DepColumn<'_> {
    type Target = Column;

    fn deref(&self) -> &Column {
        match self {
            DepColumn::Input(column) => column,
            DepColumn::Computed(column) => column,
        }
    }
}

/// Go `getDepColumn`.
pub(crate) fn get_dep_column<'a>(
    input: &'a Chunk,
    computed_by_order: &'a [Column],
    dep: DepRef,
) -> Result<DepColumn<'a>, ExecError> {
    match dep.source {
        DepRefSource::FromInput => {
            if dep.idx < 0 || dep.idx >= input.num_cols() as i64 {
                return Err(err(format!("input dependency col idx {} out of range [0,{})", dep.idx, input.num_cols())));
            }
            Ok(DepColumn::Input(input.column(dep.idx as usize)))
        }
        DepRefSource::FromComputed => {
            if dep.idx < 0 || dep.idx >= computed_by_order.len() as i64 {
                return Err(err(format!(
                    "computed dependency idx {} out of range [0,{})",
                    dep.idx,
                    computed_by_order.len()
                )));
            }
            Ok(DepColumn::Computed(&computed_by_order[dep.idx as usize]))
        }
    }
}

/// Go `resolveSingleOutputOldColumn`.
pub(crate) fn resolve_single_output_old_column<'a>(
    merge_name: &str,
    input: &'a Chunk,
    output_cols: &[Option<Column>],
    merger_output_cols: &[i64],
) -> Result<ColumnRead<'a>, ExecError> {
    if output_cols.len() != 1 {
        return Err(err(format!("{merge_name} merger expects exactly 1 output column slot, got {}", output_cols.len())));
    }
    if merger_output_cols.len() != 1 {
        return Err(err(format!(
            "{merge_name} merger expects exactly 1 output column id, got {}",
            merger_output_cols.len()
        )));
    }
    let output_col_id = merger_output_cols[0];
    if output_col_id < 0 || output_col_id >= input.num_cols() as i64 {
        return Err(err(format!("{merge_name} output col {output_col_id} out of input range")));
    }
    Ok(input.column(output_col_id as usize))
}

/// Go `resolveFieldTypeByColID`.
pub(crate) fn resolve_field_type_by_col_id(col_id: i64, child_types: &[FieldType]) -> Result<&FieldType, ExecError> {
    if col_id < 0 || col_id >= child_types.len() as i64 {
        return Err(err(format!("col id {col_id} out of range [0,{})", child_types.len())));
    }
    Ok(&child_types[col_id as usize])
}

/// Go `validateSignedIntType`.
pub(crate) fn validate_signed_int_type(tp: &FieldType) -> Result<(), ExecError> {
    if tp.eval_type() != EvalType::Int {
        return Err(err(format!("eval type must be int, got {}", tp.eval_type())));
    }
    if tidb_mysql::types::has_unsigned_flag(tp.flags() as usize) {
        return Err(err("type must be signed integer"));
    }
    Ok(())
}

/// Go `validateSignedIntNullableType`.
pub(crate) fn validate_signed_int_nullable_type(tp: &FieldType) -> Result<(), ExecError> {
    validate_signed_int_type(tp)?;
    if tidb_mysql::types::has_not_null_flag(tp.flags() as usize) {
        return Err(err("type must be nullable"));
    }
    Ok(())
}

/// Go `depRefSourceName`.
fn dep_ref_source_name(source: DepRefSource) -> &'static str {
    match source {
        DepRefSource::FromInput => "delta aggregation input",
        DepRefSource::FromComputed => "previously computed columns",
    }
}

/// Go `validateDepRefSource`.
pub(crate) fn validate_dep_ref_source(dep: DepRef, expected: DepRefSource) -> Result<(), ExecError> {
    if dep.source != expected {
        return Err(err(format!("must come from {}", dep_ref_source_name(expected))));
    }
    Ok(())
}

/// Go `firstNegativeCountValue`.
pub(crate) fn first_negative_count_value(col: &Column, num_rows: usize) -> Option<i64> {
    (0..num_rows).map(|row| col.get_int64(row)).find(|&value| value < 0)
}

/// Go `chunk.Column.HasNull`.
pub(crate) fn has_null(col: &Column) -> bool {
    col.null_count() > 0
}
