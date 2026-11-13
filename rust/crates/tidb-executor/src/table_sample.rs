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

//! Go `pkg/executor/sample.go`: `TableSampleExecutor` over its
//! `rowSampler`, the `REGIONS` method's `tableRegionSampler` (one record from
//! each region of the table's record range) and the global temporary table's
//! `emptySampler`.

use std::sync::Arc;

use crate::KvTableReader;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::schema::Schema;
use tidb_kv::region_map::RegionStore;
use tidb_table::tables::{RowDecodeContext, TableCommon, TableHandle};

/// One output column of a sampled physical row (Go
/// `buildSampleColAndDecodeColMap`'s decode column for a schema column).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SampleOutputColumn {
    /// A stored table-column offset.
    Stored(usize),
    /// Go's synthetic `_tidb_rowid`.
    ExtraHandle,
    /// Go's synthetic `_tidb_commit_ts`.
    ExtraCommitTs,
}

/// A `kv.KeyRange` of Go's `splitIntoMultiRanges`: a region clipped to one
/// physical table's record range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SampleRange {
    /// The position in the sampler's tables of the physical table whose rows
    /// the range holds.
    pub(crate) table: usize,
    /// The inclusive start key.
    pub(crate) start: Vec<u8>,
    /// The exclusive end key.
    pub(crate) end: Vec<u8>,
}

/// Go `rowSampler`.
pub(crate) trait RowSampler: Send {
    /// Go `writeChunk`.
    fn write_chunk(&mut self, req: &mut Chunk) -> Result<(), ExecError>;
    /// Go `finished`.
    fn finished(&self) -> bool;
}

/// Go `emptySampler`: a global temporary table holds no committed rows.
pub(crate) struct EmptySampler;

impl RowSampler for EmptySampler {
    fn write_chunk(&mut self, _req: &mut Chunk) -> Result<(), ExecError> {
        Ok(())
    }

    fn finished(&self) -> bool {
        true
    }
}

/// Go `tableRegionSampler`.
pub(crate) struct TableRegionSampler {
    /// Go `ctx.GetStore()`'s region cache.
    region_store: Arc<dyn RegionStore>,
    /// The physical tables Go's `splitTableRanges` reads: the table itself,
    /// or the selected partitions.
    physical_ids: Vec<i64>,
    /// Go `table`, one per physical table.
    tables: Vec<TableCommon>,
    /// Go `GetSnapshot(startTS)`, one per physical table.
    stores: Vec<Box<dyn tidb_kv::storage::TableStorage>>,
    /// Go `isDesc`.
    is_desc: bool,
    /// Go `buildSampleColAndDecodeColMap`'s decode columns.
    output_columns: Vec<SampleOutputColumn>,
    /// Go's row decoder settings (`decLoc`, the expression context).
    decode_context: RowDecodeContext,
    /// Go `restKVRanges`; `None` before `initRanges`.
    rest_kv_ranges: Option<Vec<SampleRange>>,
    /// Go `isFinished`.
    is_finished: bool,
}

impl TableRegionSampler {
    /// Go `newTableRegionSampler`.
    pub(crate) fn new(
        region_store: Arc<dyn RegionStore>,
        physical_ids: Vec<i64>,
        tables: Vec<TableCommon>,
        stores: Vec<Box<dyn tidb_kv::storage::TableStorage>>,
        output_columns: Vec<SampleOutputColumn>,
        decode_context: RowDecodeContext,
        is_desc: bool,
    ) -> Self {
        Self {
            region_store,
            physical_ids,
            tables,
            stores,
            is_desc,
            output_columns,
            decode_context,
            rest_kv_ranges: None,
            is_finished: false,
        }
    }

    /// Go `initRanges`.
    fn init_ranges(&mut self) -> Result<(), ExecError> {
        if self.rest_kv_ranges.is_none() {
            let mut ranges = self.split_table_ranges()?;
            sort_ranges(&mut ranges, self.is_desc);
            self.rest_kv_ranges = Some(ranges);
        }
        Ok(())
    }

    /// Go `pickRanges`.
    fn pick_ranges(&mut self, count: usize) -> Vec<SampleRange> {
        let rest = self.rest_kv_ranges.get_or_insert_with(Vec::new);
        let cut_point = rest.len().min(count);
        rest.drain(..cut_point).collect()
    }

    /// Go `splitTableRanges`: every physical table's record range
    /// `[t{id}_r, PrefixNext)`, cut at the store's regions.
    fn split_table_ranges(&self) -> Result<Vec<SampleRange>, ExecError> {
        let mut ranges = Vec::with_capacity(self.physical_ids.len());
        for (table, physical_id) in self.physical_ids.iter().enumerate() {
            let start = tidb_tablecodec::table_key::gen_table_record_prefix(*physical_id);
            let end = tidb_txnkv::Key::from_bytes(start.clone()).prefix_next();
            ranges.extend(
                split_into_multi_ranges(&*self.region_store, &start, end.as_bytes())?
                    .into_iter()
                    .map(|(start, end)| SampleRange { table, start, end }),
            );
        }
        Ok(ranges)
    }

    /// Go `writeChunkFromRanges` over `scanFirstKVForEachRange`: the first
    /// record of each range in the read snapshot, scanned forward whatever
    /// the sample's order, decoded with generated columns recomputed, its
    /// schema columns appended in the ranges' order.
    fn write_chunk_from_ranges(&mut self, ranges: &[SampleRange], req: &mut Chunk) -> Result<(), ExecError> {
        for range in ranges {
            let (Some(table), Some(store)) =
                (self.tables.get_mut(range.table), self.stores.get_mut(range.table))
            else {
                return Err(ExecError::internal("a sampled range names no physical table"));
            };
            let sampled = table
                .first_snapshot_row_in_range(&mut **store, &range.start, &range.end, &self.decode_context)
                .map_err(|error| ExecError::internal(format!("table bytes failed to decode: {error:?}")))?;
            let Some((handle, row)) = sampled else {
                continue;
            };
            for (output, source) in self.output_columns.iter().copied().enumerate() {
                match source {
                    SampleOutputColumn::Stored(source) => {
                        let value = row.get(source).ok_or_else(|| {
                            ExecError::internal("table-sample output column is outside the row")
                        })?;
                        req.append_datum(output, value);
                    }
                    SampleOutputColumn::ExtraHandle => match handle {
                        TableHandle::Int(value) => req.append_datum(output, &Datum::Int(value)),
                        TableHandle::Common(_) => {
                            return Err(ExecError::internal(
                                "an extra row handle is not an integer handle",
                            ));
                        }
                    },
                    SampleOutputColumn::ExtraCommitTs => {
                        // Go's `DecodeAndEvalRowWithMap` decodes no commit
                        // version; the local TableStorage seam has none.
                        req.append_datum(output, &Datum::UInt(0));
                    }
                }
            }
        }
        Ok(())
    }
}

impl RowSampler for TableRegionSampler {
    fn write_chunk(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.init_ranges()?;
        let mut expected_row_count = req.required_rows();
        while expected_row_count > 0 && self.rest_kv_ranges.as_ref().is_some_and(|rest| !rest.is_empty()) {
            let ranges = self.pick_ranges(expected_row_count);
            self.write_chunk_from_ranges(&ranges, req)?;
            expected_row_count = req.required_rows().saturating_sub(req.num_rows());
        }
        if self.rest_kv_ranges.as_ref().is_none_or(Vec::is_empty) {
            self.is_finished = true;
        }
        Ok(())
    }

    fn finished(&self) -> bool {
        self.is_finished
    }
}

/// Go `splitIntoMultiRanges`: `[start_key, end_key)` cut at the regions
/// overlapping it, each clipped to the range.
fn split_into_multi_ranges(
    store: &dyn RegionStore,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecError> {
    let regions = store
        .load_regions_in_key_range(start_key, end_key)
        .map_err(|error| ExecError::Mysql(crate::MysqlError::unknown(error)))?;
    let ranges = tidb_kv::region_map::clip_regions(&regions, start_key, end_key);
    if ranges.is_empty() {
        return Err(ExecError::Mysql(crate::MysqlError::unknown("no regions found")));
    }
    Ok(ranges)
}

/// Go `sortRanges`: by start key, descending for a descending sample.
fn sort_ranges(ranges: &mut [SampleRange], is_desc: bool) {
    ranges.sort_by(|i, j| {
        let order = i.start.cmp(&j.start);
        if is_desc { order.reverse() } else { order }
    });
}

/// Go `TableSampleExecutor`.
pub struct TableSampleExecutor {
    meta: ExecutorMeta,
    /// Go `sampler`.
    sampler: Box<dyn RowSampler>,
}

impl TableSampleExecutor {
    /// Go `buildTableSample`'s executor.
    #[must_use]
    pub(crate) fn new(meta: ExecutorMeta, sampler: Box<dyn RowSampler>) -> Self {
        Self { meta, sampler }
    }
}

impl Executor for TableSampleExecutor {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.sampler.finished() {
            return Ok(());
        }
        self.sampler.write_chunk(req)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}
