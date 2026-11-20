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

//! Go `exec.go`'s MIN/MAX recompute: the metadata types,
//! `validateMinMaxRecompute`, `recomputeMinMaxRows` (single-row and batch),
//! `encodeChunkKeyRows` and the recompute overrides.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::column::Column;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::Executor;
use tidb_expr::column::CorrelatedColumn;

use crate::exec::{is_min_max_agg, Mapping};
use crate::merge::err;

/// Go `MinMaxRecomputeStrategy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MinMaxRecomputeStrategy {
    /// Go `MinMaxRecomputeUnknown`: unset or invalid.
    #[default]
    Unknown,
    /// Go `MinMaxRecomputeSingleRow`: one group key per execution.
    SingleRow,
    /// Go `MinMaxRecomputeBatch`: multiple group keys in one execution.
    Batch,
}

/// Go `MinMaxBatchLookupContent`: one batch lookup key.
#[derive(Clone, Debug, Default)]
pub struct MinMaxBatchLookupContent {
    /// Go `Keys`: group-key datums in planner-defined key order.
    pub keys: Vec<Datum>,
}

/// Go `MinMaxBatchBuildRequest`.
#[derive(Clone, Debug, Default)]
pub struct MinMaxBatchBuildRequest {
    /// Go `LookupKeys`: deduplicated keys for this batch.
    pub lookup_keys: Vec<MinMaxBatchLookupContent>,
}

/// Go `MinMaxBatchExecBuilder`: builds one opened recompute executor for one
/// batch request. Its result schema puts the group-key columns first, in the
/// order of [`MinMaxRecomputeExec::key_input_col_ids`].
pub trait MinMaxBatchExecBuilder: Send + Sync {
    /// Go `Build`: must return an opened executor; safe for concurrent calls
    /// across MView merge workers.
    ///
    /// # Errors
    /// The executor could not be built or opened.
    fn build(&self, req: &MinMaxBatchBuildRequest) -> Result<Box<dyn Executor>, ExecError>;
}

/// Go `MinMaxRecomputeSingleRowWorker`: one worker-local single-row slot.
pub struct MinMaxRecomputeSingleRowWorker {
    /// Go `KeyCols`: correlated key columns bound to this worker slot.
    pub key_cols: Vec<CorrelatedColumn>,
    /// Go `Exec`: the worker-local reusable executor (only its worker locks it).
    pub exec: Mutex<Box<dyn Executor>>,
}

/// Go `MinMaxRecomputeSingleRowExec`.
pub struct MinMaxRecomputeSingleRowExec {
    /// Go `Workers`: one slot per MView merge worker.
    pub workers: Vec<MinMaxRecomputeSingleRowWorker>,
}

/// Go `MinMaxRecomputeSpec`: recompute metadata for one MIN/MAX mapping.
#[derive(Default)]
pub struct MinMaxRecomputeSpec {
    /// Go `Strategy`.
    pub strategy: MinMaxRecomputeStrategy,
    /// Go `SingleRow`: set for [`MinMaxRecomputeStrategy::SingleRow`].
    pub single_row: Option<MinMaxRecomputeSingleRowExec>,
    /// Go `BatchResultColIdxes`: result-column indexes in batch recompute
    /// output; valid only for [`MinMaxRecomputeStrategy::Batch`].
    pub batch_result_col_idxes: Vec<i64>,
}

/// Go `MinMaxRecomputeExec`: shared MIN/MAX recompute metadata.
#[derive(Default)]
pub struct MinMaxRecomputeExec {
    /// Go `KeyInputColIDs`: group-key columns in child schema.
    pub key_input_col_ids: Vec<i64>,
    /// Go `KeyResultColIdxes`: group-key columns in batch recompute result
    /// schema, in the order of `key_input_col_ids`.
    pub key_result_col_idxes: Vec<i64>,
    /// Go `BatchBuilder`.
    pub batch_builder: Option<Box<dyn MinMaxBatchExecBuilder>>,
}

/// Go `mappingRecomputeOverride`.
#[derive(Default)]
pub(crate) struct MappingRecomputeOverride {
    /// Strictly increasing.
    pub(crate) row_idxes: Vec<usize>,
    pub(crate) values_by_output: Vec<Vec<Datum>>,
}

/// Go `validateMinMaxRecompute`: the key types it resolves.
pub(crate) fn validate_min_max_recompute(
    meta: Option<&MinMaxRecomputeExec>,
    agg_mappings: &[Mapping],
    worker_cnt: i64,
    child_types: &[FieldType],
) -> Result<Vec<FieldType>, ExecError> {
    let Some(meta) = meta else {
        for (mapping_idx, agg) in agg_mappings.iter().enumerate() {
            if agg.min_max_recompute.is_some() {
                return Err(err(format!("AggMappings[{mapping_idx}].MinMaxRecompute is set but MinMaxRecompute is nil")));
            }
        }
        return Ok(Vec::new());
    };
    if meta.key_input_col_ids.is_empty() {
        return Err(err("MinMaxRecompute requires non-empty KeyInputColIDs"));
    }
    let mut key_types = Vec::with_capacity(meta.key_input_col_ids.len());
    let mut seen_key = HashSet::new();
    for &key_col_id in &meta.key_input_col_ids {
        if key_col_id < 0 || key_col_id >= child_types.len() as i64 {
            return Err(err(format!("MinMaxRecompute key col {key_col_id} out of range [0,{})", child_types.len())));
        }
        if !seen_key.insert(key_col_id) {
            return Err(err(format!("duplicate MinMaxRecompute key col {key_col_id}")));
        }
        key_types.push(child_types[key_col_id as usize].clone());
    }
    if meta.key_result_col_idxes.len() != meta.key_input_col_ids.len() {
        return Err(err(format!(
            "MinMaxRecompute key result column count mismatch: expect {}, got {}",
            meta.key_input_col_ids.len(),
            meta.key_result_col_idxes.len()
        )));
    }
    let mut seen_key_result = HashSet::new();
    for &result_col_idx in &meta.key_result_col_idxes {
        if result_col_idx < 0 {
            return Err(err(format!("MinMaxRecompute key result col idx {result_col_idx} must be non-negative")));
        }
        if !seen_key_result.insert(result_col_idx) {
            return Err(err(format!("duplicate MinMaxRecompute key result col idx {result_col_idx}")));
        }
    }
    let mut has_batch = false;
    for (mapping_idx, agg) in agg_mappings.iter().enumerate() {
        let Some(agg_func) = &agg.agg_func else {
            return Err(err(format!("MinMaxRecompute validation requires AggMappings[{mapping_idx}].AggFunc")));
        };
        let agg_name = agg_func.base.name.as_str();
        if !is_min_max_agg(agg_name) {
            if agg.min_max_recompute.is_some() {
                return Err(err(format!("AggMappings[{mapping_idx}].MinMaxRecompute is set for non-MIN/MAX agg={agg_name}")));
            }
            continue;
        }
        let Some(recompute) = &agg.min_max_recompute else {
            return Err(err(format!("missing AggMappings[{mapping_idx}].MinMaxRecompute for agg={agg_name}")));
        };
        match recompute.strategy {
            MinMaxRecomputeStrategy::SingleRow => {
                let Some(single_row) = &recompute.single_row else {
                    return Err(err(format!("MinMaxRecompute mapping {mapping_idx} requires SingleRow execution metadata")));
                };
                if !recompute.batch_result_col_idxes.is_empty() {
                    return Err(err(format!(
                        "MinMaxRecompute mapping {mapping_idx} single-row strategy should not set BatchResultColIdxes"
                    )));
                }
                if worker_cnt > 0 && single_row.workers.len() != worker_cnt as usize {
                    return Err(err(format!(
                        "MinMaxRecompute mapping {mapping_idx} single-row worker slots mismatch: expect {worker_cnt}, got {}",
                        single_row.workers.len()
                    )));
                }
                for (worker_idx, worker) in single_row.workers.iter().enumerate() {
                    if worker.key_cols.len() != meta.key_input_col_ids.len() {
                        return Err(err(format!(
                            "MinMaxRecompute mapping {mapping_idx} single-row worker {worker_idx} key column mismatch: expect {}, got {}",
                            meta.key_input_col_ids.len(),
                            worker.key_cols.len()
                        )));
                    }
                    for (key_pos, key_col) in worker.key_cols.iter().enumerate() {
                        if key_col.data.is_none() {
                            return Err(err(format!(
                                "MinMaxRecompute mapping {mapping_idx} single-row worker {worker_idx} key column {key_pos} is not initialized"
                            )));
                        }
                    }
                }
            }
            MinMaxRecomputeStrategy::Batch => {
                has_batch = true;
                if recompute.single_row.is_some() {
                    return Err(err(format!(
                        "MinMaxRecompute mapping {mapping_idx} batch strategy should not set SingleRow metadata"
                    )));
                }
                if recompute.batch_result_col_idxes.is_empty() {
                    return Err(err(format!("MinMaxRecompute mapping {mapping_idx} batch strategy requires BatchResultColIdxes")));
                }
                if recompute.batch_result_col_idxes.len() != agg.col_id.len() {
                    return Err(err(format!(
                        "MinMaxRecompute mapping {mapping_idx} batch result column mismatch: Mapping.ColID={} BatchResultColIdxes={}",
                        agg.col_id.len(),
                        recompute.batch_result_col_idxes.len()
                    )));
                }
                let mut seen_batch_result = HashSet::new();
                for &result_col_idx in &recompute.batch_result_col_idxes {
                    if result_col_idx < 0 {
                        return Err(err(format!(
                            "MinMaxRecompute mapping {mapping_idx} batch result col idx {result_col_idx} must be non-negative"
                        )));
                    }
                    if seen_key_result.contains(&result_col_idx) {
                        return Err(err(format!(
                            "MinMaxRecompute mapping {mapping_idx} batch result col idx {result_col_idx} conflicts with key prefix/result columns"
                        )));
                    }
                    if !seen_batch_result.insert(result_col_idx) {
                        return Err(err(format!(
                            "duplicate batch result col idx {result_col_idx} in MinMaxRecompute mapping {mapping_idx}"
                        )));
                    }
                }
            }
            MinMaxRecomputeStrategy::Unknown => {
                return Err(err(format!("MinMaxRecompute mapping {mapping_idx} has unknown strategy 0")));
            }
        }
    }
    if has_batch && meta.batch_builder.is_none() {
        return Err(err("MinMaxRecompute batch strategy requires BatchBuilder"));
    }
    if !has_batch && meta.batch_builder.is_some() {
        return Err(err("MinMaxRecompute BatchBuilder is set but no batch strategy mapping exists"));
    }
    Ok(key_types)
}

/// What the recompute reads beside the merged chunk.
pub(crate) struct RecomputeCtx<'a> {
    pub(crate) agg_mappings: &'a [Mapping],
    pub(crate) meta: Option<&'a MinMaxRecomputeExec>,
    pub(crate) key_types: &'a [FieldType],
    pub(crate) child_types: &'a [FieldType],
}

/// Go `encodeChunkKeyRows`: one key per used row, each key column a
/// NULL marker (0 / 1) followed by Go `codec.SerializeKeys`'s
/// `KeepVarColumnLength` bytes.
pub(crate) fn encode_chunk_key_rows(
    chk: &Chunk,
    key_col_idxes: &[i64],
    key_types: &[FieldType],
    used_rows: &[usize],
) -> Result<Vec<Vec<u8>>, ExecError> {
    let mut key_buf = vec![Vec::new(); used_rows.len()];
    if used_rows.is_empty() {
        return Ok(key_buf);
    }
    for (key_pos, &key_col_idx) in key_col_idxes.iter().enumerate() {
        let col = chk.column(key_col_idx as usize);
        let rows: Vec<Vec<Datum>> = used_rows
            .iter()
            .map(|&row| {
                let mut datum = Datum::Null;
                col.datum_with_buffer(row, &key_types[key_pos], &mut datum);
                vec![datum]
            })
            .collect();
        let (serialized, _) = tidb_codec::serialize_keys(
            &rows,
            &[0],
            std::slice::from_ref(&key_types[key_pos]),
            &[tidb_codec::SerializeMode::KeepVarColumnLength],
            None,
        )
        .map_err(|e| err(format!("{e:?}")))?;
        for (logical_pos, &physical_row) in used_rows.iter().enumerate() {
            key_buf[logical_pos].push(u8::from(!col.is_null(physical_row)));
            key_buf[logical_pos].extend_from_slice(&serialized[logical_pos]);
        }
    }
    Ok(key_buf)
}

/// Go `appendDatumToColumn`.
fn append_datum_to_column(col: &mut Column, datum: &Datum, ft: &FieldType) {
    let mut chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(ft), 1);
    chk.append_datum(0, datum);
    col.append_cell_n_times(&chk.column(0), 0, 1);
}

/// Go `rebuildColumnWithOverrides`.
fn rebuild_column_with_overrides(old_col: &Column, ft: &FieldType, row_idxes: &[usize], values: &[Datum]) -> Result<Column, ExecError> {
    if row_idxes.len() != values.len() {
        return Err(err(format!(
            "override row/value count mismatch: rows={} values={}",
            row_idxes.len(),
            values.len()
        )));
    }
    let row_cnt = old_col.rows();
    let mut new_col = Column::new_column(ft, row_cnt);
    let mut overrides = row_idxes.iter().zip(values).peekable();
    for row in 0..row_cnt {
        match overrides.peek() {
            Some(&(&override_row, value)) if override_row == row => {
                append_datum_to_column(&mut new_col, value, ft);
                overrides.next();
            }
            _ => new_col.append_cell_n_times(old_col, row, 1),
        }
    }
    if new_col.rows() != row_cnt {
        return Err(err(format!("override row apply row count mismatch: expected={row_cnt} actual={}", new_col.rows())));
    }
    Ok(new_col)
}

impl RecomputeCtx<'_> {
    /// Go `recomputeMinMaxRows`.
    pub(crate) fn recompute_min_max_rows(
        &self,
        input: &Chunk,
        computed_by_col_id: &mut [Option<Column>],
        min_max_rows: &[Vec<usize>],
        worker_idx: usize,
    ) -> Result<(), ExecError> {
        let Some(meta) = self.meta else {
            return Ok(());
        };
        let mut overrides: Vec<Option<MappingRecomputeOverride>> = (0..self.agg_mappings.len()).map(|_| None).collect();
        let mut batch_mappings = Vec::new();
        for (mapping_idx, rows) in min_max_rows.iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            if mapping_idx >= self.agg_mappings.len() {
                return Err(err(format!(
                    "min/max recompute mapping idx {mapping_idx} out of range [0,{})",
                    self.agg_mappings.len()
                )));
            }
            let Some(recompute_meta) = &self.agg_mappings[mapping_idx].min_max_recompute else {
                return Err(err(format!("missing AggMappings[{mapping_idx}].MinMaxRecompute")));
            };
            match recompute_meta.strategy {
                MinMaxRecomputeStrategy::SingleRow => {
                    overrides[mapping_idx] =
                        Some(self.recompute_min_max_single_row(input, meta, worker_idx, mapping_idx, rows)?);
                }
                MinMaxRecomputeStrategy::Batch => batch_mappings.push(mapping_idx),
                MinMaxRecomputeStrategy::Unknown => {
                    return Err(err(format!("unknown MinMaxRecompute strategy 0 for mapping {mapping_idx}")));
                }
            }
        }
        if !batch_mappings.is_empty() {
            self.recompute_min_max_batch(input, meta, &batch_mappings, min_max_rows, &mut overrides)?;
        }
        self.apply_min_max_recompute_overrides(computed_by_col_id, &overrides)
    }

    /// Go `recomputeMinMaxSingleRow`.
    fn recompute_min_max_single_row(
        &self,
        input: &Chunk,
        meta: &MinMaxRecomputeExec,
        worker_idx: usize,
        mapping_idx: usize,
        recompute_rows: &[usize],
    ) -> Result<MappingRecomputeOverride, ExecError> {
        let mapping = &self.agg_mappings[mapping_idx];
        let Some(single_row) = mapping.min_max_recompute.as_ref().and_then(|spec| spec.single_row.as_ref()) else {
            return Err(err(format!("single-row MinMaxRecompute metadata is missing for mapping {mapping_idx}")));
        };
        let Some(worker) = single_row.workers.get(worker_idx) else {
            return Err(err(format!(
                "min/max single-row worker idx {worker_idx} out of range [0,{}) for mapping {mapping_idx}",
                single_row.workers.len()
            )));
        };
        let mut over = MappingRecomputeOverride {
            row_idxes: recompute_rows.to_vec(),
            values_by_output: vec![Vec::with_capacity(recompute_rows.len()); mapping.col_id.len()],
        };
        let row_cnt = input.num_rows();
        let mut exec = worker.exec.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut result_chk = exec.new_first_chunk();
        for &row_idx in recompute_rows {
            if row_idx >= row_cnt {
                return Err(err(format!("min/max single-row recompute row idx {row_idx} out of range [0,{row_cnt})")));
            }
            let input_row = input.get_row(row_idx);
            for (key_pos, &key_col_id) in meta.key_input_col_ids.iter().enumerate() {
                worker.key_cols[key_pos].bind(input_row.get_datum(key_col_id as usize, &self.key_types[key_pos]));
            }
            exec.open()?;
            result_chk.reset();
            let mut ret = exec.next(&mut result_chk);
            if ret.is_ok() {
                if result_chk.num_rows() == 0 {
                    ret = Err(err(format!(
                        "min/max single-row recompute returns no row for mapping {mapping_idx} row {row_idx}"
                    )));
                } else if result_chk.num_rows() > 1 {
                    ret = Err(err(format!(
                        "min/max single-row recompute returns more than one row for mapping {mapping_idx} row {row_idx}"
                    )));
                }
            }
            if ret.is_ok() {
                let result_row = result_chk.get_row(0);
                for (col_pos, &output_col_id) in mapping.col_id.iter().enumerate() {
                    over.values_by_output[col_pos]
                        .push(result_row.get_datum(col_pos, &self.child_types[output_col_id as usize]));
                }
                result_chk.reset();
                ret = exec.next(&mut result_chk);
                if ret.is_ok() && result_chk.num_rows() != 0 {
                    ret = Err(err(format!(
                        "min/max single-row recompute returns more than one row for mapping {mapping_idx} row {row_idx}"
                    )));
                }
            }
            let closed = exec.close();
            ret?;
            closed?;
        }
        Ok(over)
    }

    /// Go `recomputeMinMaxBatch`.
    fn recompute_min_max_batch(
        &self,
        input: &Chunk,
        meta: &MinMaxRecomputeExec,
        batch_mappings: &[usize],
        min_max_rows: &[Vec<usize>],
        overrides: &mut [Option<MappingRecomputeOverride>],
    ) -> Result<(), ExecError> {
        let Some(batch_builder) = &meta.batch_builder else {
            return Err(err("min/max batch recompute requires BatchBuilder"));
        };
        let row_cnt = input.num_rows();
        let mut seen = vec![false; row_cnt];
        let mut unique_rows = Vec::new();
        for &mapping_idx in batch_mappings {
            let rows = &min_max_rows[mapping_idx];
            if rows.is_empty() {
                continue;
            }
            for &row_idx in rows {
                if row_idx >= row_cnt {
                    return Err(err(format!("min/max batch recompute row idx {row_idx} out of range [0,{row_cnt})")));
                }
                if !seen[row_idx] {
                    unique_rows.push(row_idx);
                    seen[row_idx] = true;
                }
            }
            let mapping = &self.agg_mappings[mapping_idx];
            overrides[mapping_idx] = Some(MappingRecomputeOverride {
                row_idxes: rows.clone(),
                values_by_output: vec![vec![Datum::Null; rows.len()]; mapping.col_id.len()],
            });
        }
        unique_rows.sort_unstable();
        if unique_rows.is_empty() {
            return Ok(());
        }

        let encoded_input_keys = encode_chunk_key_rows(input, &meta.key_input_col_ids, self.key_types, &unique_rows)?;
        let mut encoded_key_to_lookup_key_pos: HashMap<Vec<u8>, usize> = HashMap::with_capacity(unique_rows.len());
        let mut row_idx_to_lookup_key_pos = vec![0_usize; row_cnt];
        let mut lookup_key_contents = Vec::with_capacity(unique_rows.len());
        for (row_pos, &row_idx) in unique_rows.iter().enumerate() {
            let encoded_key = &encoded_input_keys[row_pos];
            if encoded_key_to_lookup_key_pos.contains_key(encoded_key) {
                return Err(err("duplicate lookup key for min/max batch recompute at input"));
            }
            encoded_key_to_lookup_key_pos.insert(encoded_key.clone(), row_pos);
            row_idx_to_lookup_key_pos[row_idx] = row_pos;
            let input_row = input.get_row(row_idx);
            let keys = meta
                .key_input_col_ids
                .iter()
                .enumerate()
                .map(|(key_pos, &key_col_id)| input_row.get_datum(key_col_id as usize, &self.key_types[key_pos]))
                .collect();
            lookup_key_contents.push(MinMaxBatchLookupContent { keys });
        }
        // Go `lookupKeyPosToRowRefs`: (mapping, row position) per lookup key.
        let mut lookup_key_pos_to_row_refs: Vec<Vec<(usize, usize)>> = vec![Vec::new(); unique_rows.len()];
        for &mapping_idx in batch_mappings {
            if let Some(over) = &overrides[mapping_idx] {
                for (row_pos, &row_idx) in over.row_idxes.iter().enumerate() {
                    lookup_key_pos_to_row_refs[row_idx_to_lookup_key_pos[row_idx]].push((mapping_idx, row_pos));
                }
            }
        }

        let mut seen_lookup = vec![false; lookup_key_contents.len()];
        let mut seen_result_key_cnt = 0;
        let lookup_key_cnt = lookup_key_contents.len();
        let mut batch_exec = batch_builder.build(&MinMaxBatchBuildRequest { lookup_keys: lookup_key_contents })?;
        let result = (|| -> Result<(), ExecError> {
            let batch_types = batch_exec.ret_field_types().to_vec();
            self.validate_min_max_batch_schema_types(meta, batch_mappings, &batch_types)?;
            let mut result_chk = batch_exec.new_first_chunk();
            loop {
                result_chk.reset();
                batch_exec.next(&mut result_chk)?;
                let result_row_cnt = result_chk.num_rows();
                if result_row_cnt == 0 {
                    break;
                }
                let result_rows: Vec<usize> = (0..result_row_cnt).collect();
                let encoded_result_keys =
                    encode_chunk_key_rows(&result_chk, &meta.key_result_col_idxes, self.key_types, &result_rows)?;
                for row_idx in 0..result_row_cnt {
                    let Some(&lookup_key_pos) = encoded_key_to_lookup_key_pos.get(&encoded_result_keys[row_idx]) else {
                        return Err(err(format!(
                            "min/max batch recompute returns a key outside lookup set at result row {row_idx}"
                        )));
                    };
                    if seen_lookup[lookup_key_pos] {
                        return Err(err(format!("min/max batch recompute returns duplicate key at result row {row_idx}")));
                    }
                    seen_lookup[lookup_key_pos] = true;
                    seen_result_key_cnt += 1;
                    let result_row = result_chk.get_row(row_idx);
                    for &(mapping_idx, row_pos) in &lookup_key_pos_to_row_refs[lookup_key_pos] {
                        let mapping = &self.agg_mappings[mapping_idx];
                        let spec = mapping.min_max_recompute.as_ref().expect("validated");
                        let over = overrides[mapping_idx].as_mut().expect("set above");
                        for (col_pos, &output_col_id) in mapping.col_id.iter().enumerate() {
                            let result_col_idx = spec.batch_result_col_idxes[col_pos] as usize;
                            over.values_by_output[col_pos][row_pos] =
                                result_row.get_datum(result_col_idx, &self.child_types[output_col_id as usize]);
                        }
                    }
                }
            }
            Ok(())
        })();
        let closed = batch_exec.close();
        result?;
        closed?;
        if seen_result_key_cnt != lookup_key_cnt {
            return Err(err(format!(
                "min/max batch recompute result key count mismatch: expected {lookup_key_cnt}, got {seen_result_key_cnt}"
            )));
        }
        Ok(())
    }

    /// Go `validateMinMaxBatchSchemaTypes`.
    fn validate_min_max_batch_schema_types(
        &self,
        meta: &MinMaxRecomputeExec,
        batch_mappings: &[usize],
        batch_types: &[FieldType],
    ) -> Result<(), ExecError> {
        let result_key_col_idxes = &meta.key_result_col_idxes;
        if result_key_col_idxes.len() != self.key_types.len() {
            return Err(err(format!(
                "min/max batch key column count mismatch: key types={} result key indexes={}",
                self.key_types.len(),
                result_key_col_idxes.len()
            )));
        }
        let mut key_result_set = HashSet::new();
        for (i, key_tp) in self.key_types.iter().enumerate() {
            let result_col_idx = result_key_col_idxes[i];
            if result_col_idx < 0 || result_col_idx >= batch_types.len() as i64 {
                return Err(err(format!(
                    "min/max batch result key col idx {result_col_idx} out of schema range [0,{}) at key position {i}",
                    batch_types.len()
                )));
            }
            if !key_result_set.insert(result_col_idx) {
                return Err(err(format!("duplicate min/max batch result key col idx {result_col_idx}")));
            }
            let result_tp = &batch_types[result_col_idx as usize];
            if !key_tp.equal(result_tp) {
                return Err(err(format!(
                    "min/max batch key type mismatch at position {i}: expected={key_tp} result={result_tp}"
                )));
            }
        }
        for &mapping_idx in batch_mappings {
            let mapping = &self.agg_mappings[mapping_idx];
            let Some(spec) = &mapping.min_max_recompute else {
                return Err(err(format!("min/max batch recompute metadata is missing for mapping {mapping_idx}")));
            };
            if spec.batch_result_col_idxes.len() != mapping.col_id.len() {
                return Err(err(format!(
                    "min/max batch result column count mismatch for mapping {mapping_idx}: BatchResultColIdxes={} Mapping.ColID={}",
                    spec.batch_result_col_idxes.len(),
                    mapping.col_id.len()
                )));
            }
            for (pos, &result_col_idx) in spec.batch_result_col_idxes.iter().enumerate() {
                if key_result_set.contains(&result_col_idx) {
                    return Err(err(format!(
                        "min/max batch result col idx {result_col_idx} for mapping {mapping_idx} conflicts with key prefix/result columns"
                    )));
                }
                if result_col_idx < 0 || result_col_idx >= batch_types.len() as i64 {
                    return Err(err(format!(
                        "min/max batch result col idx {result_col_idx} out of schema range [0,{}) for mapping {mapping_idx}",
                        batch_types.len()
                    )));
                }
                let output_col_id = mapping.col_id[pos];
                if output_col_id < 0 || output_col_id >= self.child_types.len() as i64 {
                    return Err(err(format!(
                        "mapping {mapping_idx} output col id {output_col_id} out of range [0,{})",
                        self.child_types.len()
                    )));
                }
                let output_tp = &self.child_types[output_col_id as usize];
                let result_tp = &batch_types[result_col_idx as usize];
                if !output_tp.equal(result_tp) {
                    return Err(err(format!(
                        "min/max batch result type mismatch for mapping {mapping_idx} output position {pos}: output={output_tp} result={result_tp}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Go `applyMinMaxRecomputeOverrides`.
    fn apply_min_max_recompute_overrides(
        &self,
        computed_by_col_id: &mut [Option<Column>],
        overrides: &[Option<MappingRecomputeOverride>],
    ) -> Result<(), ExecError> {
        if self.child_types.len() != computed_by_col_id.len() {
            return Err(err(format!(
                "min/max override child/computed column count mismatch: child_types={} computed_cols={}",
                self.child_types.len(),
                computed_by_col_id.len()
            )));
        }
        for (mapping_idx, over) in overrides.iter().enumerate() {
            let Some(over) = over.as_ref().filter(|over| !over.row_idxes.is_empty()) else {
                continue;
            };
            let mapping = &self.agg_mappings[mapping_idx];
            for (col_pos, &output_col_id) in mapping.col_id.iter().enumerate() {
                if output_col_id < 0 || output_col_id >= computed_by_col_id.len() as i64 {
                    return Err(err(format!(
                        "min/max override output col id {output_col_id} out of range [0,{})",
                        computed_by_col_id.len()
                    )));
                }
                let slot = &mut computed_by_col_id[output_col_id as usize];
                let old_col = slot.as_ref().ok_or_else(|| err(format!("min/max override output col {output_col_id} is nil")))?;
                let new_col = rebuild_column_with_overrides(
                    old_col,
                    &self.child_types[output_col_id as usize],
                    &over.row_idxes,
                    &over.values_by_output[col_pos],
                )?;
                *slot = Some(new_col);
            }
        }
        Ok(())
    }
}
