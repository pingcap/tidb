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

//! Go `TableSampleExecutor`: one record from each storage range.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{KvTable, RowDecodeContext, TableHandle};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

/// One output column of a sampled physical row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SampleOutputColumn {
    /// A stored table-column offset.
    Stored(usize),
    /// Go's synthetic `_tidb_rowid`.
    ExtraHandle,
    /// Go's synthetic `_tidb_commit_ts`.
    ExtraCommitTs,
}

/// One key range Go `splitIntoMultiRanges` cut out of a physical table's
/// record range: a region, clipped to the record prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SampleRange {
    /// The position in [`TableSampleExec`]'s tables of the physical table
    /// whose rows the range holds.
    pub(crate) table: usize,
    /// The inclusive start key.
    pub(crate) start: Vec<u8>,
    /// The exclusive end key.
    pub(crate) end: Vec<u8>,
}

/// Go `TableSampleExecutor` for the `REGIONS` method (`tableRegionSampler`):
/// the first record of each range, in the ranges' order.
///
/// The builder supplies the ranges already sorted by start key (descending
/// for a descending sample, Go `sortRanges`), and each range is scanned
/// forward from its start, as Go's `sampleFetcher` does.
pub struct TableSampleExec {
    meta: ExecutorMeta,
    tables: Vec<KvTable>,
    ranges: Vec<SampleRange>,
    output_columns: Vec<SampleOutputColumn>,
    decode_context: RowDecodeContext,
    rows: Vec<(TableHandle, Vec<Datum>)>,
    cursor: usize,
}

impl TableSampleExec {
    /// Builds a region-sampling source over the selected physical tables.
    #[must_use]
    pub(crate) fn new(
        meta: ExecutorMeta,
        tables: Vec<KvTable>,
        ranges: Vec<SampleRange>,
        output_columns: Vec<SampleOutputColumn>,
        decode_context: RowDecodeContext,
    ) -> Self {
        Self {
            meta,
            tables,
            ranges,
            output_columns,
            decode_context,
            rows: Vec::new(),
            cursor: 0,
        }
    }
}

impl Executor for TableSampleExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.rows.clear();
        self.cursor = 0;
        for range in &self.ranges {
            let table = self
                .tables
                .get_mut(range.table)
                .ok_or_else(|| ExecError::unsupported("a sampled range names no physical table"))?;
            let sampled = table
                .first_snapshot_row_in_range(&range.start, &range.end, &self.decode_context)
                .map_err(|error| {
                    ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
                })?;
            if let Some(row) = sampled {
                self.rows.push(row);
            }
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while req.num_rows() < self.meta.max_chunk_size() && self.cursor < self.rows.len() {
            let (handle, row) = &self.rows[self.cursor];
            for (output, source) in self.output_columns.iter().copied().enumerate() {
                match source {
                    SampleOutputColumn::Stored(source) => {
                        let value = row.get(source).ok_or_else(|| {
                            ExecError::unsupported("table-sample output column is outside the row")
                        })?;
                        req.append_datum(output, value);
                    }
                    SampleOutputColumn::ExtraHandle => match handle {
                        TableHandle::Int(value) => {
                            req.append_datum(output, &Datum::Int(*value));
                        }
                        TableHandle::Common(_) => {
                            return Err(ExecError::unsupported(
                                "an extra row handle is not an integer handle",
                            ));
                        }
                    },
                    SampleOutputColumn::ExtraCommitTs => {
                        // The local TableStorage seam has no MVCC version; its
                        // ordinary read timestamp is the zero version.
                        req.append_datum(output, &Datum::UInt(0));
                    }
                }
            }
            self.cursor += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.rows.clear();
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
