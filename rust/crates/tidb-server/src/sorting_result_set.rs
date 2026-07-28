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

//! A SQL-layer `ORDER BY` (without `LIMIT`) over a result-set source.
//!
//! TiDB executes an `ORDER BY` that has no `LIMIT` as a SQL-layer `SortExec`
//! (a coprocessor TopN needs a limit to bound its heap). This mirrors that: the
//! source stream is buffered once, ordered by the planner-resolved keys using
//! the shared [`stable_order_prepared_rows`] — which compares signed integers
//! numerically and `CHAR` columns under their `utf8mb4_bin` collation — then
//! served in batches. It wraps any [`ResultSetSource`], exactly as the
//! multi-relation path's ordered result set wraps its joined stream.

use tidb_datatype::Datum;
use tidb_exec::order::stable_order_prepared_rows;
use tidb_planner::read_only_scan::PreparedOrderColumn;
use tidb_protocol::ColumnInfo;

use crate::resultset_source::ResultSetSource;

/// Rows drained from the inner source per pull before ordering.
const SORT_DRAIN_BATCH: usize = 1024;

/// Buffers, orders, then serves a wrapped [`ResultSetSource`].
pub struct SortingResultSetSource<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
    keys: Vec<PreparedOrderColumn>,
    output_width: usize,
    ordered: Option<std::vec::IntoIter<Vec<Datum>>>,
}

impl<'a> SortingResultSetSource<'a> {
    /// Wraps `inner` with an order over `keys`, whose offsets index each row's
    /// `output_width` projected columns.
    #[must_use]
    pub fn new(
        inner: Box<dyn ResultSetSource + 'a>,
        keys: Vec<PreparedOrderColumn>,
        output_width: usize,
    ) -> Self {
        Self {
            inner,
            keys,
            output_width,
            ordered: None,
        }
    }

    /// Drains and orders the inner source exactly once, on the first pull.
    fn ensure_ordered(&mut self) -> Result<(), String> {
        if self.ordered.is_some() {
            return Ok(());
        }
        let mut rows: Vec<Vec<Datum>> = Vec::new();
        loop {
            let batch = self.inner.next_batch(SORT_DRAIN_BATCH)?;
            if batch.is_empty() {
                break;
            }
            rows.extend(batch);
        }
        stable_order_prepared_rows(&mut rows, self.output_width, &self.keys)
            .map_err(|error| error.to_string())?;
        self.ordered = Some(rows.into_iter());
        Ok(())
    }
}

impl ResultSetSource for SortingResultSetSource<'_> {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        self.ensure_ordered()?;
        let ordered = self
            .ordered
            .as_mut()
            .expect("ensure_ordered populates the ordered buffer");
        Ok(ordered.by_ref().take(max_rows).collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        // Ordering never changes the schema.
        self.inner.columns()
    }

    fn finish(&mut self) -> Result<(), String> {
        self.inner.finish()
    }

    fn close(&mut self) -> Result<(), String> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_planner::configured_order_limit_contract::ConfiguredOrderDirection;
    use tidb_planner::read_only_scan::ConfiguredScalarType;

    /// A drainable in-memory source for exercising the buffering sort.
    struct MockSource {
        rows: Vec<Vec<Datum>>,
        finished: bool,
    }

    impl ResultSetSource for MockSource {
        fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
            let take = max_rows.min(self.rows.len());
            Ok(self.rows.drain(..take).collect())
        }
        fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
            Ok(Vec::new())
        }
        fn finish(&mut self) -> Result<(), String> {
            self.finished = true;
            Ok(())
        }
        fn close(&mut self) -> Result<(), String> {
            Ok(())
        }
    }

    fn int_row(value: i64) -> Vec<Datum> {
        vec![Datum::new_int(value)]
    }

    fn char_row(value: &str) -> Vec<Datum> {
        vec![Datum::new_bytes(value.as_bytes().to_vec())]
    }

    fn int_key(direction: ConfiguredOrderDirection) -> PreparedOrderColumn {
        PreparedOrderColumn::new(0, direction, ConfiguredScalarType::BigInt, false)
    }

    #[test]
    fn buffers_orders_ascending_then_serves_in_batches() {
        let inner = Box::new(MockSource {
            rows: vec![int_row(3), int_row(1), int_row(2)],
            finished: false,
        });
        let mut sorted = SortingResultSetSource::new(
            inner,
            vec![int_key(ConfiguredOrderDirection::Ascending)],
            1,
        );

        // The first batch of two returns the two smallest, in order.
        let first = sorted.next_batch(2).expect("ordered pull");
        assert_eq!(first, vec![int_row(1), int_row(2)]);
        // The remainder, then an empty batch signalling the buffer is drained.
        assert_eq!(
            sorted.next_batch(2).expect("ordered pull"),
            vec![int_row(3)]
        );
        assert!(sorted.next_batch(2).expect("ordered pull").is_empty());
    }

    #[test]
    fn descending_key_reverses_the_order() {
        let inner = Box::new(MockSource {
            rows: vec![int_row(1), int_row(3), int_row(2)],
            finished: false,
        });
        let mut sorted = SortingResultSetSource::new(
            inner,
            vec![int_key(ConfiguredOrderDirection::Descending)],
            1,
        );
        assert_eq!(
            sorted.next_batch(10).expect("ordered pull"),
            vec![int_row(3), int_row(2), int_row(1)]
        );
    }

    #[test]
    fn orders_a_utf8mb4_bin_char_column() {
        // sysbench read 4 shape: one projected CHAR column, `ORDER BY c`.
        let inner = Box::new(MockSource {
            rows: vec![char_row("banana"), char_row("apple"), char_row("cherry")],
            finished: false,
        });
        let key = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 120 },
            false,
        );
        let mut sorted = SortingResultSetSource::new(inner, vec![key], 1);
        assert_eq!(
            sorted.next_batch(10).expect("ordered pull"),
            vec![char_row("apple"), char_row("banana"), char_row("cherry")]
        );
    }
}
