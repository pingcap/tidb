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

//! A single-column aggregate over a result-set source.
//!
//! The prepared read has no `GROUP BY`, so `SELECT SUM(k) ...` collapses the
//! whole scan to one output row. This drains the inner scan, folds the summed
//! column's values through the crate-shared [`fold_values`] (Go `AggFuncSum`:
//! integers promote to an exact `DECIMAL`, an empty set yields `NULL`), and
//! serves the single result row. Its schema is the aggregate's own — a
//! `DECIMAL` — not the summed column's, so it carries its own [`ColumnInfo`]
//! rather than delegating to the inner source's.

use tidb_datatype::Datum;
use tidb_exec::aggregate::runtime::fold_values;
use tidb_planner::aggregation_descriptor::AggregateKind;
use tidb_protocol::ColumnInfo;

use crate::resultset_source::ResultSetSource;

/// Rows drained from the inner source per pull while folding.
const AGGREGATE_DRAIN_BATCH: usize = 1024;

/// `div_precision_increment` is only consulted by `AVG`; a `SUM` fold ignores
/// it, so the MySQL default is passed purely to satisfy the shared signature.
const UNUSED_DIV_PRECISION_INCREMENT: u32 = 4;

/// Folds one scan column into a single aggregate result row.
pub struct AggregateResultSetSource<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
    kind: AggregateKind,
    source_offset: usize,
    columns: Vec<ColumnInfo>,
    emitted: bool,
}

impl<'a> AggregateResultSetSource<'a> {
    /// Wraps `inner`, folding its column at `source_offset` with `kind` into a
    /// single row typed by `columns`.
    #[must_use]
    pub fn new(
        inner: Box<dyn ResultSetSource + 'a>,
        kind: AggregateKind,
        source_offset: usize,
        columns: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            inner,
            kind,
            source_offset,
            columns,
            emitted: false,
        }
    }

    /// Drains the inner source and folds the summed column into one datum.
    fn fold(&mut self) -> Result<Datum, String> {
        let mut values: Vec<Datum> = Vec::new();
        loop {
            let batch = self.inner.next_batch(AGGREGATE_DRAIN_BATCH)?;
            if batch.is_empty() {
                break;
            }
            for row in batch {
                let value = row.get(self.source_offset).ok_or_else(|| {
                    format!(
                        "aggregate source row is narrower than offset {}",
                        self.source_offset
                    )
                })?;
                values.push(value.clone());
            }
        }
        fold_values(self.kind, false, &values, UNUSED_DIV_PRECISION_INCREMENT)
            .map_err(|error| format!("{error:?}"))
    }
}

impl ResultSetSource for AggregateResultSetSource<'_> {
    fn next_batch(&mut self, _max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        // An aggregate with no GROUP BY always emits exactly one row, regardless
        // of the requested batch size; a second pull reports the source drained.
        if self.emitted {
            return Ok(Vec::new());
        }
        let result = self.fold()?;
        self.emitted = true;
        Ok(vec![vec![result]])
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(self.columns.clone())
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

    struct MockSource {
        rows: Vec<Vec<Datum>>,
    }

    impl ResultSetSource for MockSource {
        fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
            let take = max_rows.min(self.rows.len());
            Ok(self.rows.drain(..take).collect())
        }
        fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
            // The inner scan's column (k, an INT); the aggregate replaces it.
            Ok(vec![ColumnInfo::default()])
        }
        fn finish(&mut self) -> Result<(), String> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), String> {
            Ok(())
        }
    }

    fn decimal_columns() -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            name: "sum(k)".to_owned(),
            column_length: 32,
            charset: 63,
            type_code: 246,
            ..ColumnInfo::default()
        }]
    }

    fn sum_source(rows: Vec<i64>) -> AggregateResultSetSource<'static> {
        let inner = Box::new(MockSource {
            rows: rows.into_iter().map(|k| vec![Datum::new_int(k)]).collect(),
        });
        AggregateResultSetSource::new(inner, AggregateKind::Sum, 0, decimal_columns())
    }

    #[test]
    fn sums_the_scan_column_into_one_decimal_row() {
        let mut source = sum_source(vec![10, 20, 12]);
        let batch = source.next_batch(100).expect("fold");
        assert_eq!(batch.len(), 1, "one aggregate row");
        assert_eq!(batch[0].len(), 1, "one aggregate column");
        match &batch[0][0] {
            Datum::Decimal(value) => assert_eq!(value.to_string(), "42"),
            other => panic!("SUM of integers must be a DECIMAL, got {other:?}"),
        }
        // A second pull reports the single-row aggregate drained.
        assert!(source.next_batch(100).expect("drained").is_empty());
    }

    #[test]
    fn sum_over_an_empty_scan_is_a_single_null_row() {
        // Go `AggFuncSum` over an empty group yields NULL, not zero.
        let mut source = sum_source(Vec::new());
        let batch = source.next_batch(100).expect("fold");
        assert_eq!(batch, vec![vec![Datum::Null]]);
    }

    #[test]
    fn the_result_schema_is_the_aggregate_decimal_not_the_scan_column() {
        let mut source = sum_source(vec![1]);
        let columns = source.columns().expect("columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].type_code, 246, "NEWDECIMAL");
        assert_eq!(columns[0].name, "sum(k)");
    }
}
