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
//! The first overlapping range is copied because it is only part of a child
//! batch. Every later whole batch transfers column owners, including Limit's
//! inline reordered/duplicate child-column projection.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::ColumnSwapHelper;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

const INVALID_INLINE_PROJECTION: &str =
    "limit output schema contains a column absent from its child schema";

/// One constructor-derived authority for both Limit output paths.
enum InlineProjection {
    /// Output and child schemas have every column once in the same order.
    Identity,
    /// Partial batches copy these ordered indexes; full batches use the helper.
    Projected {
        column_indexes: Vec<usize>,
        column_swap_helper: ColumnSwapHelper,
    },
    /// An invalid physical plan: output names a column the child cannot emit.
    Invalid,
}

impl InlineProjection {
    fn derive(output: &Schema, child: &Schema) -> Self {
        let Some(column_indexes) = child.columns_indices(&output.columns) else {
            return InlineProjection::Invalid;
        };
        if column_indexes.iter().copied().eq(0..child.len()) {
            InlineProjection::Identity
        } else {
            let column_swap_helper = ColumnSwapHelper::new(&column_indexes);
            InlineProjection::Projected {
                column_indexes,
                column_swap_helper,
            }
        }
    }

    fn invalid(&self) -> bool {
        matches!(self, InlineProjection::Invalid)
    }
}

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
    inline_projection: InlineProjection,
}

impl LimitExec {
    /// Builds a limit over `child` skipping `offset` rows and emitting at most
    /// `count` rows (Go builds `begin`/`end` from the plan's Offset/Count the
    /// same way).
    ///
    /// `count` is SATURATED against the offset first, because Go's `end` is a
    /// plain `v.Offset + v.Count` on `uint64`
    /// (`pkg/executor/builder.go` `buildLimit`) that would WRAP -- and it never
    /// gets the chance to, because the planner already clamped the count:
    /// `pkg/planner/core/logical_plan_builder.go` `buildLimit` has
    /// `if count > math.MaxUint64-offset { count = math.MaxUint64 - offset }`.
    /// The distinction decides real rows, not just whether this panics:
    /// `select * from t limit 18446744073709551615 offset 1` over three rows
    /// returns rows 2 and 3 in real TiDB (captured via `gorun`), whereas a
    /// wrapping `offset + count` makes `end` 0, `cursor >= end` true on the
    /// first call, and the statement return NOTHING.
    #[must_use]
    pub fn new(meta: ExecutorMeta, offset: u64, count: u64, child: Box<dyn Executor>) -> Self {
        let count = count.min(u64::MAX - offset);
        let child_result = child.new_chunk();
        let inline_projection = InlineProjection::derive(meta.schema(), child.schema());
        LimitExec {
            meta,
            begin: offset,
            end: offset + count,
            cursor: 0,
            meet_first_batch: offset == 0,
            child,
            child_result,
            inline_projection,
        }
    }

    /// Go `adjustRequiredRows`: include the rows still to skip, then cap the
    /// request by the remaining LIMIT window and this executor's chunk size.
    fn set_child_required_rows(&mut self, parent_required_rows: usize) {
        let max_chunk_size = self.max_chunk_size();
        let parent_required_rows = if (1..=max_chunk_size).contains(&parent_required_rows) {
            parent_required_rows
        } else {
            max_chunk_size
        };
        let parent_required_rows = u64::try_from(parent_required_rows).unwrap_or(u64::MAX);
        let rows_to_skip = self.begin.saturating_sub(self.cursor);
        let wanted = rows_to_skip.saturating_add(parent_required_rows);
        let remaining = self.end.saturating_sub(self.cursor);
        let max_chunk_size = u64::try_from(max_chunk_size).unwrap_or(u64::MAX);
        let required_rows = remaining.min(wanted).min(max_chunk_size);
        let required_rows = isize::try_from(required_rows).unwrap_or(isize::MAX);
        self.child_result
            .set_required_rows(required_rows, self.max_chunk_size());
    }

    fn append_partial_batch(
        &mut self,
        req: &mut Chunk,
        begin: usize,
        end: usize,
    ) -> Result<(), ExecError> {
        match &self.inline_projection {
            InlineProjection::Identity => {
                req.append_range_from(&self.child_result, begin, end);
            }
            InlineProjection::Projected { column_indexes, .. } => {
                let projected = self.child_result.prune(column_indexes);
                req.append_range_from(&projected, begin, end);
            }
            InlineProjection::Invalid => {
                return Err(ExecError::internal(INVALID_INLINE_PROJECTION))
            }
        }
        Ok(())
    }

    fn transfer_full_batch(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        match &self.inline_projection {
            InlineProjection::Identity => req.swap_columns(&mut self.child_result),
            InlineProjection::Projected {
                column_swap_helper, ..
            } => column_swap_helper
                .swap_columns(&mut self.child_result, req)
                .map_err(ExecError::internal)?,
            InlineProjection::Invalid => {
                return Err(ExecError::internal(INVALID_INLINE_PROJECTION))
            }
        }
        Ok(())
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
        if self.inline_projection.invalid() {
            return Err(ExecError::internal(INVALID_INLINE_PROJECTION));
        }
        if self.cursor >= self.end {
            return Ok(());
        }
        let parent_required_rows = req.required_rows();
        // Skip whole child chunks until one overlaps [begin, end); emit its
        // in-window suffix (possibly truncated at end), as Go's
        // `Append(childResult, begin, end)` does.
        while !self.meet_first_batch {
            self.set_child_required_rows(parent_required_rows);
            self.child.next(&mut self.child_result)?;
            let batch_size = u64::try_from(self.child_result.num_rows()).unwrap_or(u64::MAX);
            if batch_size == 0 {
                return Ok(());
            }
            let new_cursor = self.cursor.saturating_add(batch_size);
            if new_cursor >= self.begin {
                self.meet_first_batch = true;
                let begin = self.begin - self.cursor;
                let end = batch_size.min(self.end - self.cursor);
                self.cursor = self.cursor.saturating_add(end);
                if begin == end {
                    // Empty slice (begin lands exactly at the chunk end): fall
                    // through to the plain pass-through below, as Go's `break`.
                    break;
                }
                let begin = usize::try_from(begin).unwrap_or(usize::MAX);
                let end = usize::try_from(end).unwrap_or(usize::MAX);
                self.append_partial_batch(req, begin, end)?;
                return Ok(());
            }
            self.cursor = new_cursor;
        }
        // Past the offset: forward the next child chunk, truncated to `end`
        // (Go `TruncateTo` + `SwapColumns`/`ColumnSwapHelper`).
        self.child_result.reset();
        self.set_child_required_rows(parent_required_rows);
        self.child.next(&mut self.child_result)?;
        let mut batch_size = u64::try_from(self.child_result.num_rows()).unwrap_or(u64::MAX);
        if batch_size == 0 {
            return Ok(());
        }
        if self.cursor.saturating_add(batch_size) > self.end {
            batch_size = self.end - self.cursor;
            let rows = usize::try_from(batch_size).unwrap_or(usize::MAX);
            self.child_result.truncate_to(rows);
        }
        self.cursor = self.cursor.saturating_add(batch_size);
        self.transfer_full_batch(req)
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

    /// A deterministic multi-batch source whose `open` rewinds it. Fixed
    /// batch boundaries let the tests force Limit's partial-copy and
    /// whole-owner-transfer paths independently of required-row negotiation.
    struct ReplaySource {
        meta: ExecutorMeta,
        batches: Vec<Vec<Vec<i64>>>,
        next_batch: usize,
    }

    impl ReplaySource {
        fn new(schema: Schema, batches: Vec<Vec<Vec<i64>>>) -> Self {
            ReplaySource {
                meta: ExecutorMeta::new(schema, 0, 8, 8),
                batches,
                next_batch: 0,
            }
        }
    }

    impl Executor for ReplaySource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.next_batch = 0;
            Ok(())
        }

        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let Some(batch) = self.batches.get(self.next_batch) else {
                return Ok(());
            };
            self.next_batch += 1;
            for row in batch {
                for (column, &value) in row.iter().enumerate() {
                    req.append_int64(column, value);
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

    fn long_col_schema(columns: usize) -> Schema {
        Schema::new(
            (0..columns)
                .map(|index| {
                    let mut column = Column::new((index + 1) as i64, long());
                    column.index = index as i64;
                    column
                })
                .collect(),
        )
    }

    fn replay_limit(
        child_schema: Schema,
        output_schema: Schema,
        batches: Vec<Vec<Vec<i64>>>,
        offset: u64,
        count: u64,
    ) -> LimitExec {
        let source = ReplaySource::new(child_schema, batches);
        LimitExec::new(
            ExecutorMeta::new(output_schema, 1, 8, 8),
            offset,
            count,
            Box::new(source),
        )
    }

    fn int_rows(chunk: &Chunk) -> Vec<Vec<i64>> {
        (0..chunk.num_rows())
            .map(|row_index| {
                let row = chunk.get_row(row_index);
                (0..chunk.num_cols())
                    .map(|column_index| row.get_int64(column_index))
                    .collect()
            })
            .collect()
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

    /// `LIMIT 18446744073709551615 OFFSET 1` -- TiDB's own
    /// `executor/executor` script writes exactly this. The count is
    /// `u64::MAX`, so `offset + count` neither fits nor may WRAP: Go's planner
    /// clamps the count to `MaxUint64 - offset` before the executor's
    /// unchecked add ever sees it, and real TiDB returns EVERY REMAINING ROW
    /// (captured through `gorun` over a three-row table: rows 2 and 3). A
    /// wrapping add would make `end` 0 and return NOTHING -- a silently empty
    /// result set, which is why this asserts the ROWS and not merely that the
    /// executor did not panic.
    #[test]
    fn count_at_u64_max_saturates_against_the_offset_instead_of_wrapping() {
        let mut e = limit_over(3, 1, u64::MAX);
        assert_eq!(collect(&mut e), vec![2, 3]);
    }

    /// The offset side of the same clamp: an offset past every row is empty,
    /// not a panic, even when the count is `u64::MAX` as well.
    #[test]
    fn offset_at_u64_max_is_empty() {
        let mut e = limit_over(3, u64::MAX, u64::MAX);
        assert_eq!(collect(&mut e), Vec::<i64>::new());
    }

    #[test]
    fn inline_projection_reorders_and_duplicates_partial_and_full_batches() {
        let child_schema = long_col_schema(3);
        let output_schema = Schema::new(vec![
            child_schema.columns[2].clone(),
            child_schema.columns[0].clone(),
            child_schema.columns[2].clone(),
        ]);
        let mut limit = replay_limit(
            child_schema,
            output_schema,
            vec![
                vec![
                    vec![10, 100, 1000],
                    vec![11, 101, 1001],
                    vec![12, 102, 1002],
                ],
                vec![vec![13, 103, 1003], vec![14, 104, 1004]],
            ],
            1,
            4,
        );

        limit.open().unwrap();
        let mut req = limit.new_chunk();
        limit.next(&mut req).unwrap();
        assert_eq!(
            int_rows(&req),
            vec![vec![1001, 11, 1001], vec![1002, 12, 1002]]
        );
        // The first overlap is a physical range copy, so duplicate values do
        // not share their destination allocation.
        assert!(!req.columns_share_identity(0, &req, 2));

        limit.next(&mut req).unwrap();
        assert_eq!(
            int_rows(&req),
            vec![vec![1003, 13, 1003], vec![1004, 14, 1004]]
        );
        // A whole batch uses ColumnSwapHelper, which preserves the duplicate
        // projection as two slots sharing one owner.
        assert!(req.columns_share_identity(0, &req, 2));
        limit.close().unwrap();
    }

    #[test]
    fn offset_at_batch_end_fetches_the_next_batch_in_the_same_call() {
        let schema = one_long_col_schema();
        let mut limit = replay_limit(
            schema.clone(),
            schema,
            vec![vec![vec![1], vec![2], vec![3]], vec![vec![4], vec![5]]],
            3,
            2,
        );

        limit.open().unwrap();
        let mut req = limit.new_chunk();
        limit.next(&mut req).unwrap();
        assert_eq!(int_rows(&req), vec![vec![4], vec![5]]);
        limit.close().unwrap();
    }

    #[test]
    fn reopen_resets_limit_and_child_lifecycle() {
        let schema = one_long_col_schema();
        let mut limit = replay_limit(
            schema.clone(),
            schema,
            vec![vec![vec![1], vec![2]], vec![vec![3], vec![4]]],
            1,
            3,
        );

        assert_eq!(collect(&mut limit), vec![2, 3, 4]);
        assert_eq!(collect(&mut limit), vec![2, 3, 4]);
    }

    #[test]
    fn absent_inline_projection_column_fails_closed() {
        let child_schema = one_long_col_schema();
        let mut missing = Column::new(99, long());
        missing.index = 0;
        let mut limit = replay_limit(
            child_schema,
            Schema::new(vec![missing]),
            vec![vec![vec![1]]],
            0,
            1,
        );

        limit.open().unwrap();
        let mut req = limit.new_chunk();
        assert!(matches!(
            limit.next(&mut req),
            Err(ExecError::Internal(message)) if message == INVALID_INLINE_PROJECTION
        ));
        limit.close().unwrap();
    }
}
