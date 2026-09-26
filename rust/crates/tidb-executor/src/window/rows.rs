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

//! Owned chunk ranges for Go's retained window rows. No partition data copy.

use crate::executor::ExecError;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use tidb_chunk::{chunk::Chunk, row::Row};

/// Frame access independent of whether rows span one or several child chunks.
/// Aggregate evaluators can bind typed column readers once per contiguous range.
pub(crate) trait FrameRows {
    fn num_rows(&self) -> usize;

    /// Go subtracts frame offsets as uint64. A backward offset visits the
    /// remaining rows before its wrapped shift reaches the first invalid row.
    fn sliding_end(&self, start: usize, end: usize) -> usize {
        if end < start {
            self.num_rows()
        } else {
            end
        }
    }

    fn check_sliding_end(&self, start: usize, end: usize) -> Result<(), ExecError> {
        if end < start {
            let len = self.num_rows();
            Err(ExecError::internal(format!(
                "runtime error: index out of range [{len}] with length {len}"
            )))
        } else {
            Ok(())
        }
    }

    fn get_row(&self, index: usize) -> Row<'_>;
    fn visit_chunks(
        &self,
        range: Range<usize>,
        visit: impl FnMut(&Chunk, Range<usize>) -> Result<(), ExecError>,
    ) -> Result<(), ExecError>;
}

impl FrameRows for Chunk {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }
    fn get_row(&self, index: usize) -> Row<'_> {
        self.get_row(index)
    }
    fn visit_chunks(
        &self,
        range: Range<usize>,
        mut visit: impl FnMut(&Chunk, Range<usize>) -> Result<(), ExecError>,
    ) -> Result<(), ExecError> {
        if range.is_empty() {
            Ok(())
        } else {
            visit(self, range)
        }
    }
}

struct Batch {
    chunk: Arc<Chunk>,
    source: Range<usize>,
    start: usize,
}

impl Batch {
    fn end(&self) -> usize {
        self.start + self.source.len()
    }
}

/// A partition's live rows, with stable logical indexes after prefix expiry.
/// A child chunk is released once its last live range has expired. Output
/// aliases must not be returned to a caller before their input ranges expire.
#[derive(Default)]
pub(crate) struct WindowRows {
    batches: VecDeque<Batch>,
    end: usize,
}

impl WindowRows {
    pub(crate) fn reset(&mut self) {
        self.batches.clear();
        self.end = 0;
    }
    /// Exclusive logical end, including any prefix already discarded.
    pub(crate) fn num_rows(&self) -> usize {
        self.end
    }
    pub(crate) fn start(&self) -> usize {
        self.batches.front().map_or(self.end, |batch| batch.start)
    }

    pub(crate) fn push_range(&mut self, chunk: &Arc<Chunk>, source: Range<usize>) {
        assert!(source.start <= source.end && source.end <= chunk.num_rows());
        if source.is_empty() {
            return;
        }
        let length = source.len();
        if let Some(last) = self
            .batches
            .back_mut()
            .filter(|last| Arc::ptr_eq(&last.chunk, chunk) && last.source.end == source.start)
        {
            last.source.end = source.end;
        } else {
            self.batches.push_back(Batch {
                chunk: Arc::clone(chunk),
                source,
                start: self.end,
            });
        }
        self.end += length;
    }

    /// Returns the number of rows dropped, for Go's output-readiness counter.
    pub(crate) fn discard_before(&mut self, index: usize) -> usize {
        let start = self.start();
        assert!(index >= start && index <= self.end);
        while self
            .batches
            .front()
            .is_some_and(|batch| batch.end() <= index)
        {
            self.batches.pop_front();
        }
        if let Some(batch) = self.batches.front_mut() {
            batch.source.start += index - batch.start;
            batch.start = index;
        }
        index - start
    }

    fn batch_index(&self, index: usize) -> usize {
        assert!(index >= self.start() && index < self.end);
        self.batches.partition_point(|batch| batch.end() <= index)
    }
}

impl FrameRows for WindowRows {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }
    fn get_row(&self, index: usize) -> Row<'_> {
        let batch = &self.batches[self.batch_index(index)];
        batch
            .chunk
            .get_row(batch.source.start + index - batch.start)
    }

    fn visit_chunks(
        &self,
        range: Range<usize>,
        mut visit: impl FnMut(&Chunk, Range<usize>) -> Result<(), ExecError>,
    ) -> Result<(), ExecError> {
        if range.is_empty() {
            return Ok(());
        }
        assert!(range.end <= self.end);
        let mut position = range.start;
        let mut batch_index = self.batch_index(position);
        while position < range.end {
            let batch = &self.batches[batch_index];
            let end = range.end.min(batch.end());
            let source = batch.source.start + position - batch.start;
            visit(&batch.chunk, source..source + end - position)?;
            position = end;
            batch_index += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    fn chunk(start: i64, len: usize) -> Chunk {
        let mut chunk = Chunk::new_with_capacity(&[FieldType::new(FieldTypeCode::LongLong)], len);
        for index in 0..len {
            chunk.append_int64(0, start + index as i64);
        }
        chunk
    }

    #[test]
    fn retained_ranges_expire_chunks_without_reindexing_live_rows() {
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let first = Arc::new(chunk(10, 4));
        let mut selected = chunk(20, 3);
        selected.set_sel(Some(vec![2, 0]));
        let second = Arc::new(selected);
        let first_lifetime = Arc::downgrade(&first);
        let second_lifetime = Arc::downgrade(&second);
        let mut rows = WindowRows::default();
        rows.push_range(&first, 1..2);
        rows.push_range(&first, 2..4);
        // One retained owner per contiguous input range, not one per row.
        assert_eq!(Arc::strong_count(&first), 2);
        rows.push_range(&second, 0..2);
        drop(first);
        drop(second);
        let mut visited = Vec::new();
        rows.visit_chunks(1..5, |chunk, range| {
            for index in range {
                visited.push(chunk.get_row(index).get_datum(0, &ty));
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, [12, 13, 22, 20].map(Datum::Int));
        assert_eq!(rows.discard_before(2), 2);
        assert!(first_lifetime.upgrade().is_some());
        assert_eq!(rows.get_row(2).get_datum(0, &ty), Datum::Int(13));
        assert_eq!(rows.discard_before(3), 1);
        assert!(first_lifetime.upgrade().is_none());
        assert_eq!(rows.get_row(3).get_datum(0, &ty), Datum::Int(22));
        let third = Arc::new(chunk(30, 1));
        rows.push_range(&third, 0..1);
        assert_eq!(rows.get_row(5).get_datum(0, &ty), Datum::Int(30));
        assert_eq!(rows.discard_before(5), 2);
        assert!(second_lifetime.upgrade().is_none());
        assert_eq!(rows.num_rows(), 6);
        assert_eq!(rows.start(), 5);
        assert_eq!(rows.discard_before(6), 1);
        rows.reset();
        assert_eq!(rows.num_rows(), 0);
        assert_eq!(rows.start(), 0);
    }

    #[test]
    fn range_visits_stop_at_the_first_evaluation_error() {
        let mut rows = WindowRows::default();
        rows.push_range(&Arc::new(chunk(0, 2)), 0..2);
        rows.push_range(&Arc::new(chunk(2, 2)), 0..2);
        let mut calls = 0;
        let result = rows.visit_chunks(0..4, |_, _| {
            calls += 1;
            Err(ExecError::internal("frame evaluation failed"))
        });
        assert!(result.is_err());
        assert_eq!(calls, 1);
        // An empty frame never evaluates a row, including an empty buffer.
        rows.reset();
        rows.visit_chunks(0..0, |_, _| panic!("empty frame visited"))
            .unwrap();
    }

    #[test]
    fn sliding_states_keep_absolute_indexes_after_input_expiry() {
        use crate::hash_agg::{AggFunc, AggKind, WindowAggState};
        use tidb_expr::{column::Column, expression::Expression, NoColumns};
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
        decimal.set_decimal(0);
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        let funcs: Vec<_> = [AggKind::Count, AggKind::Sum, AggKind::Min, AggKind::Max]
            .into_iter()
            .map(|kind| AggFunc::new(kind, Some(Expression::Column(column.clone()))))
            .collect();
        let mut states: Vec<_> = funcs.iter().map(WindowAggState::new).collect();
        let mut rows = WindowRows::default();
        let mut lifetimes = Vec::new();
        for batch in 0..4 {
            let input = Arc::new(chunk(batch * 5, 5));
            lifetimes.push(Arc::downgrade(&input));
            rows.push_range(&input, 0..5);
        }
        for index in 0_usize..20 {
            let start = index.saturating_sub(2);
            let expected = [
                Datum::Int((index + 1 - start) as i64),
                Datum::Decimal(tidb_datatype::Decimal::from_int(
                    (start..=index).sum::<usize>() as i64,
                )),
                Datum::Int(start as i64),
                Datum::Int(index as i64),
            ];
            for (position, (func, state)) in funcs.iter().zip(&mut states).enumerate() {
                let output_type = if position == 1 { &decimal } else { &ty };
                assert_eq!(
                    state
                        .value(func, &NoColumns, &rows, start, index + 1, output_type)
                        .unwrap(),
                    expected[position]
                );
            }
            // Go retains lastStart so the next inverse update can read every
            // departing row, while input chunks wholly before it can be freed.
            rows.discard_before(start);
            for (batch, lifetime) in lifetimes.iter().enumerate() {
                assert_eq!(lifetime.upgrade().is_none(), (batch + 1) * 5 <= start);
            }
        }
    }
}
