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

//! A `SELECT DISTINCT` dedup over a result-set source.
//!
//! TiDB plans `SELECT DISTINCT <cols>` as an aggregation grouped by the whole
//! select tuple (`PlanBuilder.buildDistinct` -> `LogicalAggregation`), and the
//! HashAgg executor forms each group key with `codec.HashGroupKey`: an integer
//! contributes its value, a string its collator's key. For the bounded node's
//! only string collation, `utf8mb4_bin`, that key is the value with trailing
//! spaces trimmed (`binPaddingCollator`, PAD SPACE) — so `"a"` and `"a "` group
//! together. This mirrors that grouping by normalizing every row through the
//! shared [`Collation`] authority (each `Datum::Bytes` here is a `utf8mb4_bin
//! CHAR`, exactly as the ORDER BY sort compares `Datum::Bytes` under that same
//! collation) and deduping the normalized tuple with the crate-shared
//! [`DistinctChecker`] — first occurrence wins, and the original (untrimmed)
//! value of that first row is what is emitted. The dedup is a hash set, so it is
//! order-independent and stays correct whether or not an `ORDER BY` sorted the
//! stream first — and it is composed OUTSIDE any [`SortingResultSetSource`] so
//! `DISTINCT ... ORDER BY` returns the distinct rows already in sorted order.
//!
//! That "only string collation" is an enforced invariant, not an assumption:
//! `tidb_exec::cluster_catalog`'s loader refuses any stored string column whose
//! collation is neither `utf8mb4_bin` nor `binary`. It has to — a
//! `utf8mb4_general_ci` column groups `'B'` and `'b'` into ONE group in Go, and
//! the `utf8mb4_bin` key here would emit two.

use tidb_datatype::{Collation, Datum};
use tidb_exec::aggregate::aggregate_distinct::DistinctChecker;
use tidb_protocol::ColumnInfo;

use crate::resultset_source::ResultSetSource;

/// Streams first-occurrence rows from a wrapped [`ResultSetSource`].
pub struct DistinctResultSetSource<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
    checker: DistinctChecker,
}

impl<'a> DistinctResultSetSource<'a> {
    /// Wraps `inner`, dropping every row whose whole-tuple identity has already
    /// been emitted.
    #[must_use]
    pub fn new(inner: Box<dyn ResultSetSource + 'a>) -> Self {
        Self {
            inner,
            checker: DistinctChecker::new(),
        }
    }
}

impl ResultSetSource for DistinctResultSetSource<'_> {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        let mut distinct = Vec::new();
        // Dedup drops rows, so one inner batch can yield fewer than `max_rows`
        // new rows. Keep pulling until the batch is full or the inner source is
        // drained — an empty return then means genuinely drained, never merely
        // "this pull was all duplicates". Requesting exactly the remaining need
        // guarantees the batch never overshoots `max_rows`.
        while distinct.len() < max_rows {
            let batch = self.inner.next_batch(max_rows - distinct.len())?;
            if batch.is_empty() {
                break;
            }
            for row in batch {
                // Dedup on the collation-normalized group key, but emit the
                // original row so a `CHAR` value keeps its stored trailing bytes.
                if self.checker.check(&distinct_key(&row)) {
                    distinct.push(row);
                }
            }
        }
        Ok(distinct)
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        // Dedup never changes the schema.
        self.inner.columns()
    }

    fn finish(&mut self) -> Result<(), String> {
        self.inner.finish()
    }

    fn close(&mut self) -> Result<(), String> {
        self.inner.close()
    }
}

/// The collation-normalized group key of one projected row.
///
/// Mirrors `codec.HashGroupKey`'s string handling: a `utf8mb4_bin CHAR` column
/// contributes its collation key, which for `binPaddingCollator` is the value
/// with trailing spaces trimmed (PAD SPACE). Every `Datum::Bytes` a prepared
/// projection produces is such a `CHAR`, so normalizing all of them through the
/// shared [`Collation`] authority is exact — and leaves integer datums, which
/// are never `Datum::Bytes`, untouched. Two rows share a `DISTINCT` group iff
/// their normalized tuples are equal.
fn distinct_key(row: &[Datum]) -> Vec<Datum> {
    row.iter()
        .map(|datum| match datum {
            Datum::Bytes(bytes) => Datum::new_bytes(Collation::Utf8Mb4Bin.key(bytes)),
            other => other.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A drainable in-memory source for exercising the dedup.
    struct MockSource {
        rows: Vec<Vec<Datum>>,
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
            Ok(())
        }
        fn close(&mut self) -> Result<(), String> {
            Ok(())
        }
    }

    fn char_row(value: &str) -> Vec<Datum> {
        vec![Datum::new_bytes(value.as_bytes().to_vec())]
    }

    #[test]
    fn keeps_first_occurrence_of_each_char_value() {
        // sysbench read 5 shape after the ORDER BY sort: a sorted CHAR stream.
        let inner = Box::new(MockSource {
            rows: vec![
                char_row("apple"),
                char_row("apple"),
                char_row("banana"),
                char_row("banana"),
                char_row("cherry"),
            ],
        });
        let mut distinct = DistinctResultSetSource::new(inner);
        assert_eq!(
            distinct.next_batch(10).expect("dedup pull"),
            vec![char_row("apple"), char_row("banana"), char_row("cherry")]
        );
        assert!(distinct.next_batch(10).expect("dedup pull").is_empty());
    }

    #[test]
    fn refills_across_batches_and_never_returns_empty_early() {
        // A batch of pure duplicates must not be mistaken for a drained source:
        // requesting two distinct rows must keep pulling past the duplicate run.
        let inner = Box::new(MockSource {
            rows: vec![
                char_row("a"),
                char_row("a"),
                char_row("a"),
                char_row("b"),
                char_row("c"),
            ],
        });
        let mut distinct = DistinctResultSetSource::new(inner);
        assert_eq!(
            distinct.next_batch(2).expect("dedup pull"),
            vec![char_row("a"), char_row("b")],
            "the batch is filled to max_rows despite the leading duplicate run"
        );
        assert_eq!(
            distinct.next_batch(2).expect("dedup pull"),
            vec![char_row("c")]
        );
        assert!(distinct.next_batch(2).expect("dedup pull").is_empty());
    }

    #[test]
    fn trailing_space_only_differences_share_one_pad_space_group() {
        // utf8mb4_bin is PAD SPACE (binPaddingCollator), so "a", "a ", and "a  "
        // form ONE DISTINCT group, exactly as codec.HashGroupKey's collation key
        // groups them. The first-seen row is emitted with its bytes untouched.
        let inner = Box::new(MockSource {
            rows: vec![char_row("a "), char_row("a"), char_row("a  ")],
        });
        let mut distinct = DistinctResultSetSource::new(inner);
        assert_eq!(
            distinct.next_batch(10).expect("dedup pull"),
            vec![char_row("a ")],
            "trailing-space variants collapse to the first-seen row, kept verbatim"
        );
    }

    #[test]
    fn only_trailing_spaces_are_padded_not_leading_or_interior() {
        // PAD SPACE trims the right only: a leading or interior space is a real
        // character that keeps the values in separate groups.
        let inner = Box::new(MockSource {
            rows: vec![
                char_row("a"),
                char_row(" a"),
                char_row("a b"),
                char_row("ab"),
            ],
        });
        let mut distinct = DistinctResultSetSource::new(inner);
        assert_eq!(
            distinct.next_batch(10).expect("dedup pull"),
            vec![
                char_row("a"),
                char_row(" a"),
                char_row("a b"),
                char_row("ab")
            ],
            "leading and interior spaces keep the values distinct"
        );
    }

    #[test]
    fn dedups_multi_column_tuples_by_the_whole_row() {
        let row =
            |k: i64, c: &str| vec![Datum::new_int(k), Datum::new_bytes(c.as_bytes().to_vec())];
        let inner = Box::new(MockSource {
            rows: vec![row(1, "x"), row(1, "x"), row(1, "y"), row(2, "x")],
        });
        let mut distinct = DistinctResultSetSource::new(inner);
        assert_eq!(
            distinct.next_batch(10).expect("dedup pull"),
            vec![row(1, "x"), row(1, "y"), row(2, "x")]
        );
    }

    #[test]
    fn multi_column_tuple_normalizes_only_its_char_component() {
        // The CHAR component is PAD SPACE-normalized within the tuple key while
        // the integer component stays an exact identity: (1,"x ") and (1,"x")
        // share a group, but (2,"x") differs by the integer.
        let row =
            |k: i64, c: &str| vec![Datum::new_int(k), Datum::new_bytes(c.as_bytes().to_vec())];
        let inner = Box::new(MockSource {
            rows: vec![row(1, "x "), row(1, "x"), row(2, "x")],
        });
        let mut distinct = DistinctResultSetSource::new(inner);
        assert_eq!(
            distinct.next_batch(10).expect("dedup pull"),
            vec![row(1, "x "), row(2, "x")]
        );
    }
}
