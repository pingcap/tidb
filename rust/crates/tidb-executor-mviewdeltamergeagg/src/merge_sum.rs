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

//! Go `merge_sum.go`.

use std::collections::HashMap;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::column::Column;
use tidb_datatype::{EvalType, FieldType, MyDecimal};
use tidb_driver_error::ExecError;

use crate::exec::{Exec, Mapping, MergeWorkerData};
use crate::merge::{
    annotate, err, first_negative_count_value, get_dep_column, has_null, resolve_dep_ref,
    resolve_field_type_by_col_id, resolve_single_output_old_column, validate_dep_ref_source,
    validate_signed_int_type, AggMerger, DepColumn, DepRef, DepRefSource,
};

/// Go `validateCountColumn`.
fn validate_count_column(count_col: &Column, num_rows: usize) -> Result<(), ExecError> {
    if has_null(count_col) {
        return Err(err("sum count dependency contains null"));
    }
    if let Some(neg) = first_negative_count_value(count_col, num_rows) {
        return Err(err(format!("sum count dependency becomes negative ({neg})")));
    }
    Ok(())
}

/// Go `validateSumValueTypes`.
fn validate_sum_value_types(output_tp: &FieldType, delta_tp: &FieldType) -> Result<(), ExecError> {
    let output_eval_tp = output_tp.eval_type();
    let delta_eval_tp = delta_tp.eval_type();
    if output_eval_tp != delta_eval_tp {
        return Err(err(format!(
            "SUM mapping type mismatch: output eval type {output_eval_tp} differs from delta eval type {delta_eval_tp}"
        )));
    }
    if output_eval_tp == EvalType::Int && tidb_mysql::types::has_unsigned_flag(delta_tp.flags() as usize) {
        return Err(err("SUM int merge requires signed delta dependency"));
    }
    Ok(())
}

/// Go `resolveSumMergeColumns`'s columns.
struct SumMergeColumns<'a> {
    old_col: tidb_chunk::ColumnRead<'a>,
    delta_col: DepColumn<'a>,
    count_col: Option<DepColumn<'a>>,
    num_rows: usize,
}

/// Go `resolveSumMergeColumns`.
fn resolve_sum_merge_columns<'a>(
    input: &'a Chunk,
    computed_by_order: &'a [Column],
    output_cols: &[Option<Column>],
    merger: &SumMerger,
) -> Result<SumMergeColumns<'a>, ExecError> {
    let old_col = resolve_single_output_old_column("sum", input, output_cols, &merger.output_cols)?;
    let delta_col = get_dep_column(input, computed_by_order, merger.delta_ref)?;
    let count_col = if merger.has_count_ref {
        Some(get_dep_column(input, computed_by_order, merger.count_ref)?)
    } else {
        None
    };
    Ok(SumMergeColumns { old_col, delta_col, count_col, num_rows: input.num_rows() })
}

/// Go's per-eval-type SUM mergers (`sumIntMerger`, `sumUintMerger`,
/// `sumFloat64Merger`, `sumDecimalMerger`): one shape, the kind picks the
/// column representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SumKind {
    Int,
    Uint,
    Float64,
    Decimal,
}

struct SumMerger {
    kind: SumKind,
    output_cols: Vec<i64>,
    delta_ref: DepRef,
    count_ref: DepRef,
    has_count_ref: bool,
    ret_tp: FieldType,
}

impl Exec<'_> {
    /// Go `buildSumMerger`.
    pub(crate) fn build_sum_merger(
        &self,
        mapping: &Mapping,
        col_id_to_computed_idx: &HashMap<i64, usize>,
        child_types: &[FieldType],
    ) -> Result<Box<dyn AggMerger>, ExecError> {
        if mapping.col_id.len() != 1 {
            return Err(err(format!("SUM mapping expects exactly 1 output column, got {}", mapping.col_id.len())));
        }
        if mapping.dependency_col_id.is_empty() || mapping.dependency_col_id.len() > 2 {
            return Err(err(format!("SUM mapping expects 1 or 2 dependencies, got {}", mapping.dependency_col_id.len())));
        }
        let output_col_id = mapping.col_id[0];
        let delta_ref = resolve_dep_ref(mapping.dependency_col_id[0], col_id_to_computed_idx, self.delta_agg_col_count)?;
        let delta_col_id = mapping.dependency_col_id[0];
        validate_dep_ref_source(delta_ref, DepRefSource::FromInput)
            .map_err(|e| annotate(e, format!("SUM mapping delta dependency col {delta_col_id}")))?;
        let delta_tp = resolve_field_type_by_col_id(delta_col_id, child_types)
            .map_err(|e| annotate(e, format!("SUM mapping dependency col {delta_col_id}")))?;
        let ret_tp = resolve_field_type_by_col_id(output_col_id, child_types)
            .map_err(|e| annotate(e, format!("SUM mapping output col {output_col_id}")))?;
        validate_sum_value_types(ret_tp, delta_tp)?;
        let is_nullable_sum = mapping.dependency_col_id.len() == 2;

        let mut count_ref = DepRef::default();
        if is_nullable_sum {
            count_ref = resolve_dep_ref(mapping.dependency_col_id[1], col_id_to_computed_idx, self.delta_agg_col_count)?;
            let count_col_id = mapping.dependency_col_id[1];
            validate_dep_ref_source(count_ref, DepRefSource::FromComputed)
                .map_err(|e| annotate(e, format!("SUM mapping final-count dependency col {count_col_id}")))?;
            let count_tp = resolve_field_type_by_col_id(count_col_id, child_types)
                .map_err(|e| annotate(e, format!("SUM mapping count dependency col {count_col_id}")))?;
            validate_signed_int_type(count_tp)
                .map_err(|e| annotate(e, format!("SUM mapping count dependency col {count_col_id}")))?;
        }

        let kind = match ret_tp.eval_type() {
            EvalType::Int => {
                if delta_tp.eval_type() != EvalType::Int {
                    return Err(err(format!(
                        "SUM int merge expects integer delta dependency, got {}",
                        delta_tp.eval_type()
                    )));
                }
                if tidb_mysql::types::has_unsigned_flag(ret_tp.flags() as usize) {
                    SumKind::Uint
                } else {
                    SumKind::Int
                }
            }
            EvalType::Real => SumKind::Float64,
            EvalType::Decimal => SumKind::Decimal,
            other => return Err(err(format!("SUM merge does not support eval type {other}"))),
        };
        Ok(Box::new(SumMerger {
            kind,
            output_cols: vec![output_col_id],
            delta_ref,
            count_ref,
            has_count_ref: is_nullable_sum,
            ret_tp: ret_tp.clone(),
        }))
    }
}

/// One row's merged SUM, before it is written into the result column.
enum SumValue {
    Null,
    Int(i64),
    Uint(u64),
    Float64(f64),
    Decimal(MyDecimal),
}

fn overflow(error: tidb_datatype::OverflowError) -> ExecError {
    err(error.to_string())
}

/// Go `types.DecimalAdd`.
fn decimal_add(old: &MyDecimal, delta: &MyDecimal) -> Result<MyDecimal, ExecError> {
    let (sum, warning) =
        tidb_datatype::Decimal::from_my_decimal(old).add_mysql(&tidb_datatype::Decimal::from_my_decimal(delta));
    if let Some(warning) = warning {
        return Err(err(format!("{warning:?}")));
    }
    sum.to_my_decimal().map_err(|e| err(format!("{e:?}")))
}

impl SumMerger {
    /// The SUM of the old and delta values of `row_idx`, both present.
    fn add(&self, cols: &SumMergeColumns<'_>, row_idx: usize) -> Result<SumValue, ExecError> {
        Ok(match self.kind {
            SumKind::Int => SumValue::Int(
                tidb_datatype::add_int64(cols.old_col.get_int64(row_idx), cols.delta_col.get_int64(row_idx))
                    .map_err(overflow)?,
            ),
            SumKind::Uint => SumValue::Uint(
                tidb_datatype::add_integer(cols.old_col.get_uint64(row_idx), cols.delta_col.get_int64(row_idx))
                    .map_err(overflow)?,
            ),
            SumKind::Float64 => {
                SumValue::Float64(cols.old_col.get_float64(row_idx) + cols.delta_col.get_float64(row_idx))
            }
            SumKind::Decimal => SumValue::Decimal(decimal_add(
                &cols.old_col.get_my_decimal(row_idx),
                &cols.delta_col.get_my_decimal(row_idx),
            )?),
        })
    }

    /// The delta value alone (the old value is NULL).
    fn delta_only(&self, cols: &SumMergeColumns<'_>, row_idx: usize) -> Result<SumValue, ExecError> {
        Ok(match self.kind {
            SumKind::Int => SumValue::Int(cols.delta_col.get_int64(row_idx)),
            // Go `types.AddInteger(0, delta)`.
            SumKind::Uint => {
                SumValue::Uint(tidb_datatype::add_integer(0, cols.delta_col.get_int64(row_idx)).map_err(overflow)?)
            }
            SumKind::Float64 => SumValue::Float64(cols.delta_col.get_float64(row_idx)),
            SumKind::Decimal => SumValue::Decimal(cols.delta_col.get_my_decimal(row_idx)),
        })
    }

    /// The old value alone (the delta is NULL).
    fn old_only(&self, cols: &SumMergeColumns<'_>, row_idx: usize) -> SumValue {
        match self.kind {
            SumKind::Int => SumValue::Int(cols.old_col.get_int64(row_idx)),
            SumKind::Uint => SumValue::Uint(cols.old_col.get_uint64(row_idx)),
            SumKind::Float64 => SumValue::Float64(cols.old_col.get_float64(row_idx)),
            SumKind::Decimal => SumValue::Decimal(cols.old_col.get_my_decimal(row_idx)),
        }
    }

    /// Go `chunk.NewColumn(m.retTp, numRows)` resized for the kind and filled.
    fn result_column(&self, values: Vec<SumValue>) -> Column {
        let num_rows = values.len();
        let mut result_col = Column::new_column(&self.ret_tp, num_rows);
        match self.kind {
            SumKind::Int => {
                result_col.resize_int64(num_rows, false);
                result_col.with_int64s_mut(|vals| {
                    for (slot, value) in vals.iter_mut().zip(&values) {
                        if let SumValue::Int(v) = value {
                            *slot = *v;
                        }
                    }
                });
            }
            SumKind::Uint => {
                result_col.resize_uint64(num_rows, false);
                result_col.with_uint64s_mut(|vals| {
                    for (slot, value) in vals.iter_mut().zip(&values) {
                        if let SumValue::Uint(v) = value {
                            *slot = *v;
                        }
                    }
                });
            }
            SumKind::Float64 => {
                result_col.resize_float64(num_rows, false);
                result_col.with_float64s_mut(|vals| {
                    for (slot, value) in vals.iter_mut().zip(&values) {
                        if let SumValue::Float64(v) = value {
                            *slot = *v;
                        }
                    }
                });
            }
            SumKind::Decimal => {
                result_col.resize_decimal(num_rows, false);
                result_col.with_decimals_mut(|vals| {
                    for (slot, value) in vals.iter_mut().zip(&values) {
                        if let SumValue::Decimal(v) = value {
                            *slot = v.clone();
                        }
                    }
                });
            }
        }
        for (row_idx, value) in values.iter().enumerate() {
            if matches!(value, SumValue::Null) {
                result_col.set_null(row_idx, true);
            }
        }
        result_col
    }
}

impl AggMerger for SumMerger {
    fn output_col_ids(&self) -> &[i64] {
        &self.output_cols
    }

    fn merge_chunk(
        &self,
        input: &Chunk,
        computed_by_order: &[Column],
        output_cols: &mut [Option<Column>],
        _: &mut MergeWorkerData,
    ) -> Result<(), ExecError> {
        let cols = resolve_sum_merge_columns(input, computed_by_order, output_cols, self)?;
        let num_rows = cols.num_rows;
        let mut values = Vec::with_capacity(num_rows);
        let Some(count_col) = &cols.count_col else {
            // Fast path for SUM(non-null expr).
            for row_idx in 0..num_rows {
                values.push(if cols.old_col.is_null(row_idx) {
                    self.delta_only(&cols, row_idx)?
                } else {
                    self.add(&cols, row_idx)?
                });
            }
            output_cols[0] = Some(self.result_column(values));
            return Ok(());
        };

        validate_count_column(count_col, num_rows)?;
        for row_idx in 0..num_rows {
            if count_col.get_int64(row_idx) == 0 {
                values.push(SumValue::Null);
                continue;
            }
            let old_is_null = cols.old_col.is_null(row_idx);
            let delta_is_null = cols.delta_col.is_null(row_idx);
            values.push(match (old_is_null, delta_is_null) {
                (true, true) => SumValue::Null,
                (true, false) => self.delta_only(&cols, row_idx)?,
                (false, true) => self.old_only(&cols, row_idx),
                (false, false) => self.add(&cols, row_idx)?,
            });
        }
        output_cols[0] = Some(self.result_column(values));
        Ok(())
    }
}
