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

//! Go `merge_count.go`.

use std::collections::HashMap;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::column::Column;
use tidb_datatype::FieldType;
use tidb_driver_error::ExecError;

use crate::exec::{Exec, Mapping, MergeWorkerData};
use crate::merge::{
    annotate, err, get_dep_column, has_null, resolve_dep_ref, resolve_field_type_by_col_id,
    resolve_single_output_old_column, validate_dep_ref_source, validate_signed_int_nullable_type,
    validate_signed_int_type, AggMerger, DepRef, DepRefSource,
};

impl Exec {
    /// Go `buildCountMerger`.
    pub(crate) fn build_count_merger(
        &self,
        mapping: &Mapping,
        col_id_to_computed_idx: &HashMap<i64, usize>,
        child_types: &[FieldType],
    ) -> Result<Box<dyn AggMerger>, ExecError> {
        if mapping.col_id.len() != 1 {
            return Err(err(format!("COUNT mapping expects exactly 1 output column, got {}", mapping.col_id.len())));
        }
        if mapping.dependency_col_id.len() != 1 {
            return Err(err(format!(
                "COUNT mapping expects exactly 1 dependency column, got {}",
                mapping.dependency_col_id.len()
            )));
        }
        let output_col_id = mapping.col_id[0];
        let delta_ref = resolve_dep_ref(mapping.dependency_col_id[0], col_id_to_computed_idx, self.delta_agg_col_count)?;
        let delta_col_id = mapping.dependency_col_id[0];
        validate_dep_ref_source(delta_ref, DepRefSource::FromInput)
            .map_err(|e| annotate(e, format!("COUNT mapping dependency col {delta_col_id}")))?;
        let delta_tp = resolve_field_type_by_col_id(delta_col_id, child_types)
            .map_err(|e| annotate(e, format!("COUNT mapping dependency col {delta_col_id}")))?;
        let ret_tp = resolve_field_type_by_col_id(output_col_id, child_types)
            .map_err(|e| annotate(e, format!("COUNT mapping output col {output_col_id}")))?;
        validate_signed_int_nullable_type(ret_tp).map_err(|e| annotate(e, "COUNT mapping output"))?;
        validate_signed_int_type(delta_tp).map_err(|e| annotate(e, "COUNT mapping dependency"))?;
        Ok(Box::new(CountMerger { output_cols: vec![output_col_id], delta_ref, ret_tp: ret_tp.clone() }))
    }
}

/// Go `countMerger`.
struct CountMerger {
    output_cols: Vec<i64>,
    delta_ref: DepRef,
    ret_tp: FieldType,
}

impl AggMerger for CountMerger {
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
        let old_col = resolve_single_output_old_column("count", input, output_cols, &self.output_cols)?;
        let delta_col = get_dep_column(input, computed_by_order, self.delta_ref)?;

        let num_rows = input.num_rows();
        if has_null(&delta_col) {
            return Err(err("count delta contains null"));
        }

        let mut result_vals = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let old_val = if old_col.is_null(row_idx) { 0 } else { old_col.get_int64(row_idx) };
            let delta_val = delta_col.get_int64(row_idx);
            let new_val = tidb_datatype::add_int64(old_val, delta_val).map_err(|e| err(e.to_string()))?;
            if new_val < 0 {
                return Err(err(format!("count becomes negative ({new_val})")));
            }
            result_vals.push(new_val);
        }
        let mut result_col = Column::new_column(&self.ret_tp, num_rows);
        result_col.resize_int64(num_rows, false);
        result_col.with_int64s_mut(|vals| vals.copy_from_slice(&result_vals));
        output_cols[0] = Some(result_col);
        Ok(())
    }
}
