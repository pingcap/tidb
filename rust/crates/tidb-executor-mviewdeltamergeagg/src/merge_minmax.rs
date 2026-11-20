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

//! Go `merge_minmax.go`.
//!
//! MIN/MAX delta-merge update overview:
//!
//! old: current MIN/MAX stored in the MView row.
//! add/remove: MIN/MAX aggregate outputs of inserted/deleted base rows in the
//! current delta batch.
//! addCnt/removeCnt: counts of rows equal to add/remove extremum in those
//! delta aggregates.
//! countRef: final group count reference used by merger (COUNT(expr) when
//! provided; otherwise COUNT(*)).
//!
//! Decision flow:
//!  1. Derive oldExists/addExists/removeExists from value nullability and
//!     non-negative counts.
//!  2. Compute chooseAddRemove: +1 => add side dominates, -1 => remove side
//!     dominates, 0 => no dominant side (both sides absent, or same extremum
//!     with balanced counts).
//!  3. Use fast-path update when old/add/remove (+ counts) is sufficient to
//!     prove final extremum.
//!  4. Otherwise fallback to recomputing, because delta aggregates do not
//!     include the successor extremum.
//!
//! Go writes one merger per value representation (`minMaxIntMerger`,
//! `minMaxUintMerger`, `minMaxFloat32Merger`, `minMaxFloat64Merger`,
//! `minMaxDecimalMerger`, `minMaxTimeMerger`, `minMaxDurationMerger`,
//! `minMaxStringMerger`); they share the decision flow and differ in how two
//! values compare and in the name their errors carry, which [`MinMaxKind`]
//! holds. The result cells are copied from the column the decision picks.

use std::cmp::Ordering;
use std::collections::HashMap;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::column::Column;
use tidb_datatype::{Collator, EvalType, FieldType, FieldTypeCode};
use tidb_driver_error::ExecError;
use tidb_expr::expression::Expression;
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;

use crate::exec::{Exec, Mapping, MergeWorkerData};
use crate::merge::{
    annotate, err, first_negative_count_value, get_dep_column, has_null, resolve_dep_ref,
    resolve_field_type_by_col_id, validate_dep_ref_source, validate_signed_int_type, AggMerger, DepRef,
    DepRefSource,
};

/// Go's merger types, by how their values compare.
#[derive(Clone, Copy, Debug)]
enum MinMaxKind {
    Int,
    Uint,
    Float32,
    Float64,
    Decimal,
    Time,
    Duration,
    String(Collator),
}

impl MinMaxKind {
    /// The name Go's error and log texts carry.
    fn name(self) -> &'static str {
        match self {
            MinMaxKind::Int => "int",
            MinMaxKind::Uint => "uint",
            MinMaxKind::Float32 => "float32",
            MinMaxKind::Float64 => "float64",
            MinMaxKind::Decimal => "decimal",
            MinMaxKind::Time => "time",
            MinMaxKind::Duration => "duration",
            MinMaxKind::String(_) => "string",
        }
    }

    /// Go `cmp.Compare` / `cmpFloat32` / `cmpFloat64` / `Compare` /
    /// `collator.Compare` of two non-NULL cells.
    fn compare(self, a: &Column, a_row: usize, b: &Column, b_row: usize) -> Ordering {
        let float = |x: f64, y: f64| {
            if x > y {
                Ordering::Greater
            } else if x < y {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        };
        match self {
            // Go `GoDurations()` compares the stored nanoseconds as int64.
            MinMaxKind::Int | MinMaxKind::Duration => a.get_int64(a_row).cmp(&b.get_int64(b_row)),
            MinMaxKind::Uint => a.get_uint64(a_row).cmp(&b.get_uint64(b_row)),
            MinMaxKind::Float32 => float(f64::from(a.get_float32(a_row)), f64::from(b.get_float32(b_row))),
            MinMaxKind::Float64 => float(a.get_float64(a_row), b.get_float64(b_row)),
            MinMaxKind::Decimal => a.get_my_decimal(a_row).compare(&b.get_my_decimal(b_row)),
            MinMaxKind::Time => a.get_time(a_row).compare(b.get_time(b_row)),
            MinMaxKind::String(collator) => {
                collator.compare(a.get_string(a_row).as_bytes(), b.get_string(b_row).as_bytes())
            }
        }
    }
}

/// Go `minMaxDecision`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MinMaxDecision {
    UseOld,
    UseAdded,
    Recompute,
}

/// Go `minMaxMergerBase` with its kind.
struct MinMaxMerger {
    kind: MinMaxKind,
    output_cols: Vec<i64>,
    added_ref: DepRef,
    added_cnt_ref: DepRef,
    removed_ref: DepRef,
    removed_cnt_ref: DepRef,
    count_ref: DepRef,
    mapping_idx: usize,
    is_max: bool,
    ret_tp: FieldType,
}

/// Go `validateMinMaxValueTypes`.
fn validate_min_max_value_types(output_tp: &FieldType, added_tp: &FieldType, removed_tp: &FieldType) -> Result<(), ExecError> {
    if !output_tp.equal(added_tp) || !output_tp.equal(removed_tp) {
        return Err(err(format!("value type mismatch: output={output_tp} added={added_tp} removed={removed_tp}")));
    }
    Ok(())
}

/// Go `Expression.GetType`.
fn expression_type(expr: &Expression) -> Option<&FieldType> {
    match expr {
        Expression::Column(column) => column.ret_type.as_ref(),
        Expression::Constant(constant) => constant.ret_type.as_ref(),
        Expression::CorrelatedColumn(column) => column.column.ret_type.as_ref(),
        Expression::ScalarFunction(function) => function.ret_type.as_ref(),
        Expression::ScalarSubQuery(subquery) => subquery.ret_type(),
    }
}

/// Go `isMinMaxArgNullable`.
fn is_min_max_arg_nullable(mapping: &Mapping) -> Result<bool, ExecError> {
    let Some(agg_func) = &mapping.agg_func else {
        return Err(err("AggFunc is nil"));
    };
    if agg_func.base.args.len() != 1 {
        return Err(err(format!("expects exactly 1 non-nil argument, got {}", agg_func.base.args.len())));
    }
    let Some(arg_tp) = expression_type(&agg_func.base.args[0]) else {
        return Err(err("argument type is unavailable"));
    };
    Ok(!tidb_mysql::types::has_not_null_flag(arg_tp.flags() as usize))
}

impl Exec<'_> {
    /// Go `buildMinMaxMerger`.
    pub(crate) fn build_min_max_merger(
        &self,
        mapping_idx: usize,
        mapping: &Mapping,
        col_id_to_computed_idx: &HashMap<i64, usize>,
        child_types: &[FieldType],
    ) -> Result<Box<dyn AggMerger>, ExecError> {
        let name = mapping.agg_func.as_ref().map_or("", |agg| agg.base.name.as_str());
        if mapping.col_id.len() != 1 {
            return Err(err(format!("{name} mapping expects exactly 1 output column, got {}", mapping.col_id.len())));
        }
        let expr_nullable =
            is_min_max_arg_nullable(mapping).map_err(|e| annotate(e, format!("{name} mapping expression nullability")))?;
        let deps = &mapping.dependency_col_id;
        if expr_nullable {
            if deps.len() != 4 && deps.len() != 5 {
                return Err(err(format!("{name}(nullable expr) expects 4 or 5 dependencies, got {}", deps.len())));
            }
        } else if deps.len() != 4 {
            return Err(err(format!(
                "{name}(non-nullable expr) must not set final-count dependency (exactly 4 dependencies), got {}",
                deps.len()
            )));
        }

        let output_col_id = mapping.col_id[0];
        let ret_tp = resolve_field_type_by_col_id(output_col_id, child_types)
            .map_err(|e| annotate(e, format!("{name} mapping output col {output_col_id}")))?;
        let added_col_id = deps[0];
        let added_tp = resolve_field_type_by_col_id(added_col_id, child_types)
            .map_err(|e| annotate(e, format!("{name} mapping added dependency col {added_col_id}")))?;
        let removed_col_id = deps[2];
        let removed_tp = resolve_field_type_by_col_id(removed_col_id, child_types)
            .map_err(|e| annotate(e, format!("{name} mapping removed dependency col {removed_col_id}")))?;
        validate_min_max_value_types(ret_tp, added_tp, removed_tp)
            .map_err(|e| annotate(e, format!("{name} mapping value dependencies")))?;
        let input_ref = |col_id: i64, what: &str| -> Result<DepRef, ExecError> {
            let annotate_with = |e| annotate(e, format!("{name} mapping {what} dependency col {col_id}"));
            let dep = resolve_dep_ref(col_id, col_id_to_computed_idx, self.delta_agg_col_count).map_err(annotate_with)?;
            validate_dep_ref_source(dep, DepRefSource::FromInput).map_err(annotate_with)?;
            Ok(dep)
        };
        let signed_int = |col_id: i64, what: &str| -> Result<(), ExecError> {
            let annotate_with = |e| annotate(e, format!("{name} mapping {what} dependency col {col_id}"));
            let tp = resolve_field_type_by_col_id(col_id, child_types).map_err(annotate_with)?;
            validate_signed_int_type(tp).map_err(annotate_with)
        };
        let added_ref = input_ref(added_col_id, "added")?;
        let added_cnt_col_id = deps[1];
        signed_int(added_cnt_col_id, "added-count")?;
        let added_cnt_ref = input_ref(added_cnt_col_id, "added-count")?;
        let removed_ref = input_ref(removed_col_id, "removed")?;
        let removed_cnt_col_id = deps[3];
        signed_int(removed_cnt_col_id, "removed-count")?;
        let removed_cnt_ref = input_ref(removed_cnt_col_id, "removed-count")?;

        let computed_ref = |col_id: i64, what: String| -> Result<DepRef, ExecError> {
            let annotate_with = |e| annotate(e, &what);
            let tp = resolve_field_type_by_col_id(col_id, child_types).map_err(annotate_with)?;
            validate_signed_int_type(tp).map_err(annotate_with)?;
            let dep = resolve_dep_ref(col_id, col_id_to_computed_idx, self.delta_agg_col_count).map_err(annotate_with)?;
            validate_dep_ref_source(dep, DepRefSource::FromComputed).map_err(annotate_with)?;
            Ok(dep)
        };
        let count_ref = if expr_nullable && deps.len() == 5 {
            computed_ref(deps[4], format!("{name} mapping final-count dependency col {}", deps[4]))?
        } else {
            // Fall back to count(*) when there is no explicit COUNT(expr) dependency. For nullable
            // MIN/MAX this may trigger extra recomputes, but remains semantically correct.
            let count_all_rows_col_id = self.agg_mappings[0].col_id[0];
            computed_ref(
                count_all_rows_col_id,
                format!("{name} mapping fallback count(*) output col {count_all_rows_col_id}"),
            )?
        };

        // Match ordinary MIN/MAX aggregation: BIT values use byte literals in chunks,
        // even though their evaluation type is ETInt. Keep that layout for writes and recomputation.
        let eval_type = if ret_tp.code() == FieldTypeCode::Bit { EvalType::String } else { ret_tp.eval_type() };
        let kind = match eval_type {
            EvalType::Int if tidb_mysql::types::has_unsigned_flag(ret_tp.flags() as usize) => MinMaxKind::Uint,
            EvalType::Int => MinMaxKind::Int,
            EvalType::Real if ret_tp.code() == FieldTypeCode::Float => MinMaxKind::Float32,
            EvalType::Real => MinMaxKind::Float64,
            EvalType::Decimal => MinMaxKind::Decimal,
            EvalType::Datetime | EvalType::Timestamp => MinMaxKind::Time,
            EvalType::Duration => MinMaxKind::Duration,
            EvalType::String => match ret_tp.code() {
                FieldTypeCode::Bit
                | FieldTypeCode::Varchar
                | FieldTypeCode::VarString
                | FieldTypeCode::String
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob => {
                    MinMaxKind::String(tidb_datatype::get_collator(ret_tp.collation_name()))
                }
                other => {
                    return Err(err(format!("{name} merge does not support string type {}", other.mysql_type())));
                }
            },
            _ => return Err(err(format!("{name} merge does not support eval type {}", ret_tp.eval_type()))),
        };
        Ok(Box::new(MinMaxMerger {
            kind,
            output_cols: vec![output_col_id],
            added_ref,
            added_cnt_ref,
            removed_ref,
            removed_cnt_ref,
            count_ref,
            mapping_idx,
            is_max: name == tidb_expression_aggregation::names::MAX,
            ret_tp: ret_tp.clone(),
        }))
    }
}

/// Go `validateCountColumns`.
fn validate_count_columns(count_col: &Column, added_cnt_col: &Column, removed_cnt_col: &Column, num_rows: usize) -> Result<(), ExecError> {
    if has_null(count_col) {
        return Err(err("min/max final count contains null"));
    }
    if has_null(added_cnt_col) {
        return Err(err("min/max added count contains null"));
    }
    if has_null(removed_cnt_col) {
        return Err(err("min/max removed count contains null"));
    }
    if let Some(neg) = first_negative_count_value(count_col, num_rows) {
        return Err(err(format!("min/max final count becomes negative ({neg})")));
    }
    if let Some(neg) = first_negative_count_value(added_cnt_col, num_rows) {
        return Err(err(format!("min/max added count becomes negative ({neg})")));
    }
    if let Some(neg) = first_negative_count_value(removed_cnt_col, num_rows) {
        return Err(err(format!("min/max removed count becomes negative ({neg})")));
    }
    Ok(())
}

impl AggMerger for MinMaxMerger {
    fn output_col_ids(&self) -> &[i64] {
        &self.output_cols
    }

    fn merge_chunk(
        &self,
        input: &Chunk,
        computed_by_order: &[Column],
        output_cols: &mut [Option<Column>],
        worker_data: &mut MergeWorkerData,
    ) -> Result<(), ExecError> {
        if output_cols.len() != 1 {
            return Err(err(format!("min/max merger expects exactly 1 output column slot, got {}", output_cols.len())));
        }
        let rows_len = worker_data.min_max_mapping_idx_to_rows.len();
        if self.mapping_idx >= rows_len {
            return Err(err(format!(
                "min/max mapping idx {} out of recompute slice range [0,{rows_len})",
                self.mapping_idx
            )));
        }
        let mut recompute_rows = std::mem::take(&mut worker_data.min_max_mapping_idx_to_rows[self.mapping_idx]);
        // Go `resolveColumns`.
        let output_col_id = self.output_cols[0];
        if output_col_id < 0 || output_col_id >= input.num_cols() as i64 {
            return Err(err(format!("min/max output col {output_col_id} out of input range")));
        }
        let old_col = input.column(output_col_id as usize);
        let added_col = get_dep_column(input, computed_by_order, self.added_ref)?;
        let added_cnt_col = get_dep_column(input, computed_by_order, self.added_cnt_ref)?;
        let removed_col = get_dep_column(input, computed_by_order, self.removed_ref)?;
        let removed_cnt_col = get_dep_column(input, computed_by_order, self.removed_cnt_ref)?;
        let count_col = get_dep_column(input, computed_by_order, self.count_ref)?;
        let num_rows = input.num_rows();
        validate_count_columns(&count_col, &added_cnt_col, &removed_cnt_col, num_rows)?;

        let kind = self.kind;
        let compare = |a: &Column, b: &Column, row: usize| kind.compare(a, row, b, row);
        // An extremum that outranks another: greater for MAX, smaller for MIN.
        let outranks = |ordering: Ordering| {
            (self.is_max && ordering == Ordering::Greater) || (!self.is_max && ordering == Ordering::Less)
        };
        let mut result_col = Column::new_column(&self.ret_tp, num_rows);
        for row_idx in 0..num_rows {
            if count_col.get_int64(row_idx) == 0 {
                result_col.append_null();
                continue;
            }
            let added_cnt = added_cnt_col.get_int64(row_idx);
            let removed_cnt = removed_cnt_col.get_int64(row_idx);
            let old_exists = !old_col.is_null(row_idx);
            let add_exists = !added_col.is_null(row_idx);
            let remove_exists = !removed_col.is_null(row_idx);

            // chooseAddRemove indicates which delta side dominates the candidate extremum.
            // 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
            // 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
            let mut choose_add_remove = Ordering::Equal;
            // sameExtremumValue means added and removed share the same extremum value.
            let mut same_extremum_value = false;
            if add_exists != remove_exists {
                choose_add_remove = if add_exists { Ordering::Greater } else { Ordering::Less };
            } else if add_exists && remove_exists {
                choose_add_remove = if self.is_max {
                    compare(&added_col, &removed_col, row_idx)
                } else {
                    compare(&removed_col, &added_col, row_idx)
                };
                if choose_add_remove == Ordering::Equal {
                    choose_add_remove = added_cnt.cmp(&removed_cnt);
                    same_extremum_value = true;
                }
            }

            let mut decision = MinMaxDecision::UseOld;
            match choose_add_remove {
                Ordering::Equal => {
                    // added/removed have the same extremum value and the same count.
                    // If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
                    // than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
                    // aggregates do not carry the replacement extremum value.
                    // In this case, recompute is required.
                    // If sameExtremumValue is false, it means both sides are absent, and old value should be kept
                    if same_extremum_value && (!old_exists || outranks(compare(&added_col, &old_col, row_idx))) {
                        decision = MinMaxDecision::Recompute;
                    }
                }
                Ordering::Greater => {
                    if !old_exists || outranks(compare(&added_col, &old_col, row_idx)) {
                        decision = MinMaxDecision::UseAdded;
                    }
                }
                Ordering::Less => {
                    let state_error = |what: &str| {
                        bg_logger().error(
                            &format!("invalid min/max {} fast-path state ({what})", kind.name()),
                            &[
                                Field::new("is_max", Value::Bool(self.is_max)),
                                Field::new("added_cnt", Value::I64(added_cnt)),
                                Field::new("removed_cnt", Value::I64(removed_cnt)),
                            ],
                        );
                        err(format!("invalid min/max {} fast-path state ({what}) at row {row_idx}", kind.name()))
                    };
                    if !old_exists {
                        return Err(state_error("remove-dominant without old value"));
                    }
                    let cmp_old = compare(&removed_col, &old_col, row_idx);
                    if outranks(cmp_old) {
                        return Err(state_error("remove-dominant outranks old value"));
                    }
                    if cmp_old == Ordering::Equal {
                        decision = MinMaxDecision::Recompute;
                    }
                }
            }

            match decision {
                MinMaxDecision::UseOld if old_exists => result_col.append_cell_n_times(&old_col, row_idx, 1),
                MinMaxDecision::UseOld => result_col.append_null(),
                MinMaxDecision::UseAdded => result_col.append_cell_n_times(&added_col, row_idx, 1),
                MinMaxDecision::Recompute => {
                    recompute_rows.push(row_idx);
                    result_col.append_null();
                }
            }
        }
        worker_data.min_max_mapping_idx_to_rows[self.mapping_idx] = recompute_rows;
        output_cols[0] = Some(result_col);
        Ok(())
    }
}
