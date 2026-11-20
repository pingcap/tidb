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

//! Go `exec.go`: the sink executor for incremental MView merge.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tidb_chunk::chunk::Chunk;
use tidb_chunk::column::Column;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::Executor;
use tidb_expr::expression::{ConstLevel, Expression};
use tidb_expression_aggregation::{names, AggFuncDesc};

use crate::merge::{err, AggMerger};

/// Go `Mapping`: one aggregate merge rule.
#[derive(Clone, Debug, Default)]
pub struct Mapping {
    /// Go `AggFunc`; must be set.
    pub agg_func: Option<AggFuncDesc>,
    /// Go `ColID`: output column IDs in child schema.
    pub col_id: Vec<i64>,
    /// Go `DependencyColID`: a delta-agg column (< `DeltaAggColCount`) or a
    /// previously computed output column.
    pub dependency_col_id: Vec<i64>,
}

/// Go `RowOpType`: the row write operation on the MView table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowOpType {
    /// Go `RowOpInsert`.
    Insert,
    /// Go `RowOpUpdate`.
    Update,
    /// Go `RowOpDelete`.
    Delete,
}

/// Go `RowOp`: one row-level write action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowOp {
    /// Go `RowIdx`.
    pub row_idx: usize,
    /// Go `Tp`.
    pub tp: RowOpType,
    /// Go `updateOrdinal`: the index in update candidates for this chunk.
    pub(crate) update_ordinal: i32,
}

/// Go `ChunkResult`: one worker result for one input chunk.
pub struct ChunkResult {
    /// Go `Input`: the original joined chunk.
    pub input: Chunk,
    /// Go `ComputedCols`, indexed by input column ID; `None` means not computed.
    pub computed_cols: Vec<Option<Column>>,
    /// Go `RowOps`.
    pub row_ops: Vec<RowOp>,
    /// Go `UpdateTouchedBitmap`: touched-column bitmaps for update candidates.
    pub update_touched_bitmap: Vec<u8>,
    /// Go `UpdateTouchedStride`: bytes per update row.
    pub update_touched_stride: usize,
    /// Go `UpdateTouchedBitCnt`: touched-bit count per update row.
    pub update_touched_bit_cnt: usize,
}

/// Go `ResultWriter`: consumes merged chunk results.
pub trait ResultWriter: Send {
    /// Go `WriteChunk`.
    ///
    /// # Errors
    /// The write failed.
    fn write_chunk(&mut self, result: &ChunkResult, stats: &mut MergeWriterStats) -> Result<(), ExecError>;
}

/// Go `noopWriter`.
pub struct NoopWriter;

impl ResultWriter for NoopWriter {
    fn write_chunk(&mut self, _: &ChunkResult, _: &mut MergeWriterStats) -> Result<(), ExecError> {
        Ok(())
    }
}

/// Go `mergeWorkerData`.
#[derive(Default)]
pub struct MergeWorkerData {
    /// Go `updateRows`: the update candidates of the chunk being merged.
    pub(crate) update_rows: Vec<usize>,
}

/// Go `mergeWriterStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeWriterStats {
    pub chunks: i64,
    pub row_ops: i64,
    pub noop_rows: i64,
    pub insert_rows: i64,
    pub update_rows: i64,
    pub delete_rows: i64,
}

impl MergeWriterStats {
    /// Go `merge`.
    pub fn merge(&mut self, other: MergeWriterStats) {
        self.chunks += other.chunks;
        self.row_ops += other.row_ops;
        self.noop_rows += other.noop_rows;
        self.insert_rows += other.insert_rows;
        self.update_rows += other.update_rows;
        self.delete_rows += other.delete_rows;
    }

    /// Go `affectedRows`.
    #[must_use]
    pub fn affected_rows(&self) -> u64 {
        (self.insert_rows + self.update_rows + self.delete_rows) as u64
    }

    /// Go `stmtMessage`.
    #[must_use]
    pub fn stmt_message(&self) -> String {
        format!("Rows inserted: {}  Updated: {}  Deleted: {}", self.insert_rows, self.update_rows, self.delete_rows)
    }
}

/// Go `mergeRuntimeStats`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergeRuntimeStats {
    pub reader_time: Duration,
    pub writer_time: Duration,
    pub merge_worker_time: Vec<Duration>,
    pub writer_detail: MergeWriterStats,
}

impl MergeRuntimeStats {
    /// Go `newMergeRuntimeStats`.
    #[must_use]
    pub fn new(worker_cnt: i64) -> Self {
        Self { merge_worker_time: vec![Duration::ZERO; worker_cnt.max(0) as usize], ..Self::default() }
    }

    /// Go `reset`.
    pub fn reset(&mut self, worker_cnt: i64) {
        self.reader_time = Duration::ZERO;
        self.writer_time = Duration::ZERO;
        self.writer_detail = MergeWriterStats::default();
        self.merge_worker_time.clear();
        self.merge_worker_time.resize(worker_cnt.max(0) as usize, Duration::ZERO);
    }

    /// Go `Merge`.
    pub fn merge(&mut self, other: &MergeRuntimeStats) {
        self.reader_time += other.reader_time;
        self.writer_time += other.writer_time;
        self.writer_detail.merge(other.writer_detail);
        if self.merge_worker_time.len() < other.merge_worker_time.len() {
            self.merge_worker_time.resize(other.merge_worker_time.len(), Duration::ZERO);
        }
        for (idx, d) in other.merge_worker_time.iter().enumerate() {
            self.merge_worker_time[idx] += *d;
        }
    }
}

impl std::fmt::Display for MergeRuntimeStats {
    /// Go `String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use tidb_execdetails::format_duration;
        write!(f, "mview_delta_merge_agg:{{merge_worker:{{total:{}", self.merge_worker_time.len())?;
        let active: Vec<Duration> = self.merge_worker_time.iter().copied().filter(|d| !d.is_zero()).collect();
        write!(f, ", active:{}", active.len())?;
        if !active.is_empty() {
            let min = active.iter().min().copied().unwrap_or_default();
            let max = active.iter().max().copied().unwrap_or_default();
            let sum: Duration = active.iter().sum();
            write!(
                f,
                ", min:{}, max:{}, avg:{}",
                format_duration(min),
                format_duration(max),
                format_duration(sum / active.len() as u32)
            )?;
        }
        let w = &self.writer_detail;
        write!(
            f,
            "}}, reader:{}, writer:{{time:{}, chunks:{}, row_ops:{}, rows:{{insert:{}, update:{}, delete:{}, noop:{}}}}}}}",
            format_duration(self.reader_time),
            format_duration(self.writer_time),
            w.chunks,
            w.row_ops,
            w.insert_rows,
            w.update_rows,
            w.delete_rows,
            w.noop_rows
        )
    }
}

/// Go `Exec`: the sink executor for incremental MView merge.
pub struct Exec {
    /// Go `Children(0)`: the joined delta/view input.
    pub child: Box<dyn Executor>,
    /// Go `MLogEmpty`: set only when planning proved that the mlog input is
    /// empty.
    pub mlog_empty: bool,
    /// Go `AggMappings`.
    pub agg_mappings: Vec<Mapping>,
    /// Go `DeltaAggColCount`.
    pub delta_agg_col_count: i64,
    /// Go `WorkerCnt`; `<= 0` takes the session's executor concurrency.
    pub worker_cnt: i64,
    /// Go `Writer`; when unset, `Open` writes through `target_table`, or
    /// nowhere (Go's `noopWriter`) without one.
    pub writer: Option<Box<dyn ResultWriter>>,
    /// Go `TargetTable` (with the session it writes through).
    pub target_table: Option<Box<dyn crate::writer::TargetTable>>,
    /// Go `TargetHandleCols`: builds row handles for update/delete from child
    /// input rows.
    pub target_handle_cols: Option<tidb_planner_util::handle_cols::HandleCols>,

    core: MergeCore,
    prepared: bool,
    executed: bool,
    runtime_stats: Option<MergeRuntimeStats>,
}

/// Go `isFirstAggCountAllRows`.
fn is_first_agg_count_all_rows(agg_func: &AggFuncDesc) -> bool {
    if agg_func.base.name != names::COUNT || agg_func.has_distinct || agg_func.base.args.len() != 1 {
        return false;
    }
    let Expression::Constant(con) = &agg_func.base.args[0] else {
        return false;
    };
    // Require a strict non-NULL compile-time constant.
    con.const_level() == ConstLevel::STRICT && !con.value.is_null()
}

/// Go `aggFuncForErr`: `StringWithCtx(EmptyParamValues, RedactLogDisable)`.
fn agg_func_for_err(agg_func: &AggFuncDesc) -> String {
    let arg = |expr: &Expression| match expr {
        Expression::Column(column) => format!("Column#{}", column.unique_id),
        Expression::Constant(constant) => match &constant.value {
            Datum::Null => "<nil>".to_owned(),
            Datum::Int(v) => v.to_string(),
            Datum::UInt(v) => v.to_string(),
            other => format!("{other:?}"),
        },
        other => format!("{other:?}"),
    };
    let distinct = if agg_func.has_distinct { "distinct " } else { "" };
    let args: Vec<String> = agg_func.base.args.iter().map(arg).collect();
    format!("{}({distinct}{})", agg_func.base.name, args.join(", "))
}

/// Go `isMinMaxAgg`.
pub(crate) fn is_min_max_agg(agg_name: &str) -> bool {
    agg_name == names::MIN || agg_name == names::MAX
}

impl Exec {
    /// An unopened `Exec` over `child`.
    #[must_use]
    pub fn new(child: Box<dyn Executor>) -> Self {
        Self {
            child,
            mlog_empty: false,
            agg_mappings: Vec::new(),
            delta_agg_col_count: 0,
            worker_cnt: 0,
            writer: None,
            target_table: None,
            target_handle_cols: None,
            core: MergeCore::default(),
            prepared: false,
            executed: false,
            runtime_stats: None,
        }
    }

    /// Go `Open`. `executor_concurrency` is Go's
    /// `GetSessionVars().ExecutorConcurrency`.
    ///
    /// # Errors
    /// The child fails to open or the mappings are invalid.
    pub fn open(&mut self, executor_concurrency: i64) -> Result<(), ExecError> {
        self.child.open()?;
        if self.mlog_empty {
            self.prepared = true;
            self.executed = false;
            return Ok(());
        }
        if self.worker_cnt <= 0 {
            self.worker_cnt = if executor_concurrency <= 0 { 1 } else { executor_concurrency };
        }
        self.prepare_mergers()?;
        if self.writer.is_none() {
            self.writer = Some(match self.target_table.take() {
                Some(table) => {
                    let Some(handle_cols) = self.target_handle_cols.clone() else {
                        return Err(err("TargetHandleCols is nil"));
                    };
                    Box::new(crate::writer::build_table_result_writer(
                        table,
                        handle_cols,
                        &self.core.child_types,
                        self.delta_agg_col_count,
                        &self.core.agg_output_col_ids,
                    )?)
                }
                None => Box::new(NoopWriter),
            });
        }
        self.prepared = true;
        self.executed = false;
        Ok(())
    }

    /// Go `Next`: a sink, so it produces no rows; the merge runs once. The
    /// writer statistics (Go's `StmtCtx.SetAffectedRows` / `SetMessage`
    /// source) come back.
    ///
    /// # Errors
    /// The executor is not opened, or the merge pipeline failed.
    pub fn next(&mut self, collect_runtime_stats: bool) -> Result<MergeWriterStats, ExecError> {
        if !self.prepared {
            return Err(err("Exec is not opened"));
        }
        if self.executed {
            return Ok(MergeWriterStats::default());
        }
        self.executed = true;
        if collect_runtime_stats {
            match &mut self.runtime_stats {
                Some(stats) => stats.reset(self.worker_cnt),
                None => self.runtime_stats = Some(MergeRuntimeStats::new(self.worker_cnt)),
            }
        }
        if self.mlog_empty {
            return Ok(MergeWriterStats::default());
        }
        let stats = self.run_merge_pipeline()?;
        let writer_detail = stats.writer_detail;
        if collect_runtime_stats {
            self.runtime_stats = Some(stats);
        }
        Ok(writer_detail)
    }

    /// The statistics Go registers with `RuntimeStatsColl` after `Next`.
    #[must_use]
    pub fn runtime_stats(&self) -> Option<&MergeRuntimeStats> {
        self.runtime_stats.as_ref()
    }

    /// Go `Close`.
    ///
    /// # Errors
    /// The child fails to close.
    pub fn close(&mut self) -> Result<(), ExecError> {
        self.core = MergeCore::default();
        self.runtime_stats = None;
        self.prepared = false;
        self.executed = false;
        self.child.close()
    }

    /// Go `prepareMergers`.
    pub(crate) fn prepare_mergers(&mut self) -> Result<(), ExecError> {
        if self.agg_mappings.is_empty() {
            return Err(err("Exec requires non-empty AggMappings"));
        }
        let child_types = self.child.ret_field_types().to_vec();
        if self.delta_agg_col_count < 0 || self.delta_agg_col_count > child_types.len() as i64 {
            return Err(err(format!(
                "DeltaAggColCount {} out of range [0,{}]",
                self.delta_agg_col_count,
                child_types.len()
            )));
        }
        let mut col_id_to_computed_idx: HashMap<i64, usize> = HashMap::with_capacity(self.agg_mappings.len());
        let mut core = MergeCore { child_types: child_types.clone(), ..MergeCore::default() };

        let Some(first_agg) = &self.agg_mappings[0].agg_func else {
            return Err(err("MViewDeltaMergeAgg mapping requires AggFunc"));
        };
        if !is_first_agg_count_all_rows(first_agg) {
            return Err(err(format!(
                "the first MViewDeltaMergeAgg mapping must be COUNT(*)/COUNT(non-NULL constant), got {}",
                agg_func_for_err(first_agg)
            )));
        }
        if self.agg_mappings[0].col_id.len() != 1 {
            return Err(err(format!(
                "the first MViewDeltaMergeAgg mapping must output exactly 1 column, got {}",
                self.agg_mappings[0].col_id.len()
            )));
        }
        let min_max_output_cols: HashSet<i64> = self
            .agg_mappings
            .iter()
            .filter(|mapping| mapping.agg_func.as_ref().is_some_and(|agg| is_min_max_agg(&agg.base.name)))
            .flat_map(|mapping| mapping.col_id.iter().copied())
            .collect();
        if !min_max_output_cols.is_empty() {
            for (mapping_idx, mapping) in self.agg_mappings.iter().enumerate() {
                for dep_col_id in &mapping.dependency_col_id {
                    if min_max_output_cols.contains(dep_col_id) {
                        return Err(err(format!(
                            "AggMappings[{mapping_idx}] depends on MIN/MAX output col {dep_col_id}, which is unsupported"
                        )));
                    }
                }
            }
        }

        let mut mergers = Vec::with_capacity(self.agg_mappings.len());
        for mapping in &self.agg_mappings {
            let Some(agg_func) = &mapping.agg_func else {
                return Err(err("MViewDeltaMergeAgg mapping requires AggFunc"));
            };
            let agg_name = agg_func.base.name.as_str();
            if mapping.col_id.is_empty() {
                return Err(err(format!("mapping for agg={agg_name} must output at least 1 column")));
            }
            let merger = match agg_name {
                names::COUNT => self.build_count_merger(mapping, &col_id_to_computed_idx, &child_types)?,
                names::SUM => self.build_sum_merger(mapping, &col_id_to_computed_idx, &child_types)?,
                names::MIN | names::MAX => {
                    return Err(err(format!("{agg_name} merge requires MinMaxRecompute metadata")));
                }
                other => return Err(err(format!("unsupported agg function in Exec: {other}"))),
            };
            let output_col_ids = merger.output_col_ids();
            if output_col_ids.len() != mapping.col_id.len() {
                return Err(err(format!(
                    "agg={agg_name} output col count mismatch: mapping has {} but merger has {}",
                    mapping.col_id.len(),
                    output_col_ids.len()
                )));
            }
            for (idx, &output_col_id) in output_col_ids.iter().enumerate() {
                if output_col_id != mapping.col_id[idx] {
                    return Err(err(format!(
                        "agg={agg_name} output col mismatch at position {idx}: mapping={} merger={output_col_id}",
                        mapping.col_id[idx]
                    )));
                }
                if col_id_to_computed_idx.contains_key(&output_col_id) {
                    return Err(err(format!("duplicate output col id {output_col_id} in AggMappings")));
                }
                col_id_to_computed_idx.insert(output_col_id, core.compiled_output_col_cnt);
                core.compiled_output_col_cnt += 1;
                core.agg_output_col_ids.push(output_col_id);
            }
            mergers.push(merger);
        }
        core.compiled_mergers = mergers;
        self.core = core;
        Ok(())
    }

    /// Go `aggOutputColIDs`.
    pub(crate) fn agg_output_col_ids(&self) -> &[i64] {
        &self.core.agg_output_col_ids
    }

    /// Go `mergeOneChunk`.
    pub(crate) fn merge_one_chunk(&self, chk: Chunk, worker_data: &mut MergeWorkerData) -> Result<ChunkResult, ExecError> {
        self.core.merge_one_chunk(chk, worker_data)
    }
}

/// The compiled merge state the merge workers share (Go keeps it on `Exec`).
#[derive(Default)]
pub(crate) struct MergeCore {
    compiled_mergers: Vec<Box<dyn AggMerger>>,
    compiled_output_col_cnt: usize,
    agg_output_col_ids: Vec<i64>,
    /// Go `Children(0).RetFieldTypes()`.
    child_types: Vec<FieldType>,
}

impl MergeCore {

    /// Go `mergeOneChunk`.
    pub(crate) fn merge_one_chunk(&self, chk: Chunk, worker_data: &mut MergeWorkerData) -> Result<ChunkResult, ExecError> {
        let mut computed_by_order: Vec<Column> = Vec::with_capacity(self.compiled_output_col_cnt);
        let mut computed_by_col_id: Vec<Option<Column>> = (0..chk.num_cols()).map(|_| None).collect();
        for merger in &self.compiled_mergers {
            let output_col_ids = merger.output_col_ids();
            let output_cnt = output_col_ids.len();
            if output_cnt == 0 {
                return Err(err("agg merger has no output columns"));
            }
            if computed_by_order.len() + output_cnt > self.compiled_output_col_cnt {
                return Err(err(format!(
                    "computed output overflow: built={} current={output_cnt} total={}",
                    computed_by_order.len(),
                    self.compiled_output_col_cnt
                )));
            }
            let mut output_cols: Vec<Option<Column>> = (0..output_cnt).map(|_| None).collect();
            merger.merge_chunk(&chk, &computed_by_order, &mut output_cols, worker_data)?;
            for (idx, col) in output_cols.into_iter().enumerate() {
                let Some(col) = col else {
                    return Err(err(format!("agg merger output column is nil at position {idx}")));
                };
                let output_col_id = output_col_ids[idx] as usize;
                computed_by_col_id[output_col_id] = Some(col.clone());
                computed_by_order.push(col);
            }
        }
        if computed_by_order.len() != self.compiled_output_col_cnt {
            return Err(err(format!(
                "computed output count mismatch, expected={} got={}",
                self.compiled_output_col_cnt,
                computed_by_order.len()
            )));
        }
        let (row_ops, update_touched_bitmap, update_touched_stride, update_touched_bit_cnt) =
            self.build_row_ops(&chk, &computed_by_col_id, worker_data)?;
        Ok(ChunkResult {
            input: chk,
            computed_cols: computed_by_col_id,
            row_ops,
            update_touched_bitmap,
            update_touched_stride,
            update_touched_bit_cnt,
        })
    }

    /// Go `buildRowOps`.
    #[allow(clippy::type_complexity)]
    pub(crate) fn build_row_ops(
        &self,
        input: &Chunk,
        computed_by_col_id: &[Option<Column>],
        worker_data: &mut MergeWorkerData,
    ) -> Result<(Vec<RowOp>, Vec<u8>, usize, usize), ExecError> {
        let Some(&count_star_output_col_id) = self.agg_output_col_ids.first() else {
            return Err(err("no aggregate outputs in Exec"));
        };
        if count_star_output_col_id < 0 || count_star_output_col_id >= input.num_cols() as i64 {
            return Err(err(format!("count(*) output col {count_star_output_col_id} out of input chunk range")));
        }
        if count_star_output_col_id >= computed_by_col_id.len() as i64 {
            return Err(err(format!(
                "count(*) output col {count_star_output_col_id} out of computed-by-col-id range [0,{})",
                computed_by_col_id.len()
            )));
        }
        let Some(new_count_star_col) = &computed_by_col_id[count_star_output_col_id as usize] else {
            return Err(err(format!("count(*) output col {count_star_output_col_id} is missing in computed columns")));
        };
        let old_count_star_col = input.column(count_star_output_col_id as usize);
        let mut row_ops = Vec::with_capacity(input.num_rows());
        let update_rows = &mut worker_data.update_rows;
        update_rows.clear();
        for row_idx in 0..input.num_rows() {
            let new_count = new_count_star_col.get_int64(row_idx);
            if new_count < 0 {
                return Err(err(format!("count(*) becomes negative ({new_count})")));
            }
            let old_exists = !old_count_star_col.is_null(row_idx);
            if new_count == 0 {
                if old_exists {
                    row_ops.push(RowOp { row_idx, tp: RowOpType::Delete, update_ordinal: -1 });
                }
            } else if !old_exists {
                row_ops.push(RowOp { row_idx, tp: RowOpType::Insert, update_ordinal: -1 });
            } else {
                row_ops.push(RowOp { row_idx, tp: RowOpType::Update, update_ordinal: update_rows.len() as i32 });
                update_rows.push(row_idx);
            }
        }

        let update_touched_bit_cnt = self.agg_output_col_ids.len();
        let update_touched_stride = (update_touched_bit_cnt + 7) >> 3;
        if update_rows.is_empty() {
            return Ok((row_ops, Vec::new(), update_touched_stride, update_touched_bit_cnt));
        }
        let mut update_touched_bitmap = vec![0_u8; update_rows.len() * update_touched_stride];
        let field_types: &[FieldType] = &self.child_types;
        for (bit_pos, &col_id) in self.agg_output_col_ids.iter().enumerate() {
            if col_id < 0 || col_id >= input.num_cols() as i64 {
                return Err(err(format!("agg output col {col_id} out of input chunk range")));
            }
            if col_id >= computed_by_col_id.len() as i64 {
                return Err(err(format!(
                    "agg output col {col_id} out of computed-by-col-id range [0,{})",
                    computed_by_col_id.len()
                )));
            }
            if col_id >= field_types.len() as i64 {
                return Err(err(format!("agg output col {col_id} out of field type range [0,{})", field_types.len())));
            }
            let old_col = input.column(col_id as usize);
            let Some(new_col) = &computed_by_col_id[col_id as usize] else {
                return Err(err(format!("computed agg col {col_id} is nil")));
            };
            tidb_executor_internal_util::mark_touched_rows_by_column(
                update_rows,
                &mut update_touched_bitmap,
                update_touched_stride,
                bit_pos,
                &old_col,
                new_col,
                &field_types[col_id as usize],
                // TODO: Use the MV target column metadata for the not-null quick path once the
                // aggregate output col ID to MV column offset mapping is tracked explicitly.
                false,
                "aggregate change",
            )?;
        }
        Ok((row_ops, update_touched_bitmap, update_touched_stride, update_touched_bit_cnt))
    }

}

impl Exec {
    /// Go `runMergePipeline`: a reader feeding chunks to merge workers whose
    /// results one writer applies, the input chunks recycled through a free
    /// list. Every stage owns its channel ends, so the first failure ends the
    /// others.
    fn run_merge_pipeline(&mut self) -> Result<MergeRuntimeStats, ExecError> {
        use std::sync::atomic::{AtomicBool, Ordering};
        let worker_cnt = self.worker_cnt.max(1) as usize;
        let mut stats = MergeRuntimeStats::new(worker_cnt as i64);
        let input_buf_size = (worker_cnt * 2).max(2);
        let (input_tx, input_rx) = crossbeam_channel::bounded::<Chunk>(input_buf_size);
        let (free_tx, free_rx) = crossbeam_channel::bounded::<Chunk>(input_buf_size);
        let (result_tx, result_rx) = crossbeam_channel::bounded::<ChunkResult>(worker_cnt);
        for _ in 0..input_buf_size {
            free_tx.send(self.child.new_first_chunk()).expect("the free list has room");
        }
        let failed = AtomicBool::new(false);
        let mut writer = self.writer.take().unwrap_or_else(|| Box::new(NoopWriter));
        let child = &mut self.child;
        let core = &self.core;
        let writer_stats = &mut stats.writer_detail;
        let (reader_result, worker_results, writer_result) = std::thread::scope(|scope| {
            let failed = &failed;
            let reader = scope.spawn(move || -> Result<Duration, ExecError> {
                let mut total = Duration::ZERO;
                while let Ok(mut chk) = free_rx.recv() {
                    if failed.load(Ordering::SeqCst) {
                        break;
                    }
                    chk.reset();
                    let start = Instant::now();
                    if let Err(error) = child.next(&mut chk) {
                        failed.store(true, Ordering::SeqCst);
                        return Err(error);
                    }
                    total += start.elapsed();
                    if chk.num_rows() == 0 || input_tx.send(chk).is_err() {
                        break;
                    }
                }
                Ok(total)
            });
            let workers: Vec<_> = (0..worker_cnt)
                .map(|_| {
                    let input_rx = input_rx.clone();
                    let result_tx = result_tx.clone();
                    scope.spawn(move || -> Result<Duration, ExecError> {
                        let mut worker_data = MergeWorkerData::default();
                        let mut total = Duration::ZERO;
                        while let Ok(chk) = input_rx.recv() {
                            if failed.load(Ordering::SeqCst) {
                                break;
                            }
                            let start = Instant::now();
                            let result = match core.merge_one_chunk(chk, &mut worker_data) {
                                Ok(result) => result,
                                Err(error) => {
                                    failed.store(true, Ordering::SeqCst);
                                    return Err(error);
                                }
                            };
                            total += start.elapsed();
                            if result_tx.send(result).is_err() {
                                break;
                            }
                        }
                        Ok(total)
                    })
                })
                .collect();
            drop(input_rx);
            drop(result_tx);
            let writer_handle = scope.spawn(move || -> Result<(Duration, Box<dyn ResultWriter>), ExecError> {
                let mut total = Duration::ZERO;
                while let Ok(result) = result_rx.recv() {
                    if failed.load(Ordering::SeqCst) {
                        break;
                    }
                    let start = Instant::now();
                    if let Err(error) = writer.write_chunk(&result, writer_stats) {
                        failed.store(true, Ordering::SeqCst);
                        return Err(error);
                    }
                    total += start.elapsed();
                    if free_tx.send(result.input).is_err() {
                        break;
                    }
                }
                Ok((total, writer))
            });
            let reader_result = reader.join().expect("the reader does not panic");
            let worker_results: Vec<_> =
                workers.into_iter().map(|worker| worker.join().expect("a merge worker does not panic")).collect();
            let writer_result = writer_handle.join().expect("the writer does not panic");
            (reader_result, worker_results, writer_result)
        });
        let (writer_time, writer) = writer_result?;
        self.writer = Some(writer);
        stats.writer_time = writer_time;
        stats.reader_time = reader_result?;
        for (idx, result) in worker_results.into_iter().enumerate() {
            stats.merge_worker_time[idx] = result?;
        }
        Ok(stats)
    }
}
