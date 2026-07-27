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

//! `pkg/executor` `LimitExec`: ignores `offset` rows from its child, then
//! returns at most `count` rows -- the `LIMIT [offset,] count` operator.
//!
//! Faithful to Go's window arithmetic: `begin = offset`, `end = offset + count`,
//! with a `cursor` counting child rows consumed. A child chunk that straddles
//! the window boundary is truncated row-by-row, exactly where Go slices with
//! `Append(childResult, begin, end)` / `TruncateTo`.
//!
//! Simplified serial path (established crate pattern): rows are copied with
//! `append_row` instead of Go's `SwapColumns` zero-copy handoff, and the
//! `adjustRequiredRows` chunk-size negotiation, inline projection
//! (`columnIdxsUsedByChild` / `ColumnSwapHelper`), opentracing span, and
//! slow-close logging are deferred (documented).

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

/// Go `LimitExec`: skips `begin` child rows, emits rows until `end`.
pub struct LimitExec {
    meta: ExecutorMeta,
    /// Go `begin`: the offset -- rows `[0, begin)` are skipped.
    begin: u64,
    /// Go `end`: `offset + count` -- rows `[begin, end)` are emitted.
    end: u64,
    /// Go `cursor`: how many child rows have been consumed.
    cursor: u64,
    /// Go `meetFirstBatch`: whether the first chunk overlapping the window has
    /// been seen (true immediately when `begin == 0`).
    meet_first_batch: bool,
    child: Box<dyn Executor>,
    child_result: Chunk,
}

impl LimitExec {
    /// Builds a limit over `child` skipping `offset` rows and emitting at most
    /// `count` rows (Go builds `begin`/`end` from the plan's Offset/Count the
    /// same way).
    #[must_use]
    pub fn new(meta: ExecutorMeta, offset: u64, count: u64, child: Box<dyn Executor>) -> Self {
        let child_result = child.new_chunk();
        LimitExec {
            meta,
            begin: offset,
            end: offset + count,
            cursor: 0,
            meet_first_batch: offset == 0,
            child,
            child_result,
        }
    }
}

impl Executor for LimitExec {
    /// Go `Open`/`open`: opens the child and resets the window cursor.
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_result.reset();
        self.cursor = 0;
        self.meet_first_batch = self.begin == 0;
        Ok(())
    }

    /// Go `Next`: the offset-skipping loop, then per-call pass-through with
    /// end-of-window truncation.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.cursor >= self.end {
            return Ok(());
        }
        // Skip whole child chunks until one overlaps [begin, end); emit its
        // in-window suffix (possibly truncated at end), as Go's
        // `Append(childResult, begin, end)` does.
        while !self.meet_first_batch {
            self.child.next(&mut self.child_result)?;
            let batch_size = self.child_result.num_rows() as u64;
            if batch_size == 0 {
                return Ok(());
            }
            let new_cursor = self.cursor + batch_size;
            if new_cursor >= self.begin {
                self.meet_first_batch = true;
                let begin = self.begin - self.cursor;
                let end = if new_cursor > self.end {
                    self.end - self.cursor
                } else {
                    batch_size
                };
                self.cursor += end;
                if begin == end {
                    // Empty slice (begin lands exactly at the chunk end): fall
                    // through to the plain pass-through below, as Go's `break`.
                    break;
                }
                for r in begin..end {
                    req.append_row(
                        self.child_result
                            .get_row(usize::try_from(r).expect("row index fits usize")),
                    );
                }
                return Ok(());
            }
            self.cursor += batch_size;
        }
        // Past the offset: forward the next child chunk, truncated to `end`
        // (Go `TruncateTo` + `SwapColumns`; copied row-by-row here).
        self.child_result.reset();
        self.child.next(&mut self.child_result)?;
        let mut batch_size = self.child_result.num_rows() as u64;
        if batch_size == 0 {
            return Ok(());
        }
        if self.cursor + batch_size > self.end {
            batch_size = self.end - self.cursor;
        }
        self.cursor += batch_size;
        for r in 0..usize::try_from(batch_size).expect("row count fits usize") {
            req.append_row(self.child_result.get_row(r));
        }
        Ok(())
    }

    /// Go `Close` (minus the slow-close logging/tracing, deferred).
    fn close(&mut self) -> Result<(), ExecError> {
        self.child.close()
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

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A test-only source that emits one prebuilt chunk, then EOF (same helper
    /// pattern as the selection tests).
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }

    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
            }
            Ok(())
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

    fn one_long_col_schema() -> Schema {
        let mut c = Column::new(1, long());
        c.index = 0;
        Schema::new(vec![c])
    }

    /// A limit over a single source chunk holding col0 = 1..=n.
    fn limit_over(n: i64, offset: u64, count: u64) -> LimitExec {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), n as usize);
        for v in 1..=n {
            data.append_int64(0, v);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, n as usize, 1024),
            data: Some(data),
        };
        LimitExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, n as usize, 1024),
            offset,
            count,
            Box::new(source),
        )
    }

    fn collect(exec: &mut LimitExec) -> Vec<i64> {
        exec.open().unwrap();
        let mut out = Vec::new();
        let mut req = exec.new_chunk();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                out.push(req.get_row(r).get_int64(0));
            }
        }
        exec.close().unwrap();
        out
    }

    #[test]
    fn count_truncates_mid_chunk() {
        // offset 0, count 3 over 5 rows: the source chunk is truncated.
        let mut e = limit_over(5, 0, 3);
        assert_eq!(collect(&mut e), vec![1, 2, 3]);
    }

    #[test]
    fn offset_skips_into_chunk() {
        // offset 2, count 2 over 5 rows: window lands inside the chunk.
        let mut e = limit_over(5, 2, 2);
        assert_eq!(collect(&mut e), vec![3, 4]);
    }

    #[test]
    fn offset_beyond_all_rows_is_empty() {
        let mut e = limit_over(3, 5, 2);
        assert_eq!(collect(&mut e), Vec::<i64>::new());
    }

    #[test]
    fn count_larger_than_remaining_rows() {
        let mut e = limit_over(4, 1, 100);
        assert_eq!(collect(&mut e), vec![2, 3, 4]);
    }

    #[test]
    fn exhaustion_stays_eof() {
        let mut e = limit_over(3, 0, 2);
        e.open().unwrap();
        let mut req = e.new_chunk();
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 2);
        // cursor >= end: every further call is EOF.
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        e.close().unwrap();
    }

    #[test]
    fn zero_count_is_empty() {
        let mut e = limit_over(3, 0, 0);
        assert_eq!(collect(&mut e), Vec::<i64>::new());
    }
}
