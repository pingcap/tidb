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

/// Go `TableSampleExecutor` for the `REGIONS` method.
///
/// Each `KvTable` is one physical table range in the local backend. This is
/// Go's non-TiKV fallback from `splitIntoMultiRanges`: the full physical table
/// key range, whose first record is the sample.
pub struct TableSampleExec {
    meta: ExecutorMeta,
    tables: Vec<KvTable>,
    output_columns: Vec<SampleOutputColumn>,
    desc: bool,
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
        output_columns: Vec<SampleOutputColumn>,
        desc: bool,
        decode_context: RowDecodeContext,
    ) -> Self {
        Self {
            meta,
            tables,
            output_columns,
            desc,
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
        for table in &mut self.tables {
            let sampled = table
                .first_row_with_handle_recomputed(self.desc, &self.decode_context)
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
