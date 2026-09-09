// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/statistics/handle/cache/stats_table_row_cache.go`: the
//! cross-statement cache of table row counts and per-column sizes that the
//! information-schema `TABLES`/`PARTITIONS` readers serve their size columns
//! from.
//!
//! Go's `TableRowStatsCache` is one process-wide instance refreshed in batch by
//! `UpdateByID` before any row is built; a failed restricted refresh only logs
//! a warning and the reader then serves the PREVIOUS cached values. This
//! module keeps that contract: [`StatsTableRowCache::update_by_id`] copies the
//! two reads into the maps only when both succeed, and every read path answers
//! from whatever the cache last held.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Go `tableHistID`: the `(table_id, hist_id)` key of one variable-width
/// column's stored size.
pub type TableHistId = (i64, i64);

#[derive(Default)]
struct State {
    table_rows: HashMap<i64, u64>,
    col_length: HashMap<TableHistId, u64>,
}

/// Go `StatsTableRowCache`, including the package-level `TableRowStatsCache`
/// instance's role: the cache of table row counts.
///
/// Go guards both maps with one `syncutil.RWMutex`; this port keeps the same
/// shape by locking both maps as one state. The estimated-size helpers
/// (`EstimateDataLength`/`GetDataAndIndexLength`) are shared with the
/// statement-local `TableSizeStats` estimators in `tidb-exec`: a snapshot
/// feeds those pure functions, so the width/partition accounting has exactly
/// one implementation.
pub struct StatsTableRowCache {
    state: RwLock<State>,
}

impl Default for StatsTableRowCache {
    fn default() -> Self {
        Self::new()
    }
}

/// One consistent view of both maps: `(row counts, column lengths)`, feeding
/// the shared `TableSizeStats` estimators on the consumer side.
pub type SizeSnapshot = (Vec<(i64, u64)>, Vec<(TableHistId, u64)>);

/// The restricted reads Go's `UpdateByID` performs inside
/// `getRowCountTables`/`getColLengthTables`.
pub trait StatsTableRowSizeSource {
    /// Source error type.
    type Error;

    /// Reads `select table_id, count from mysql.stats_meta` for the given
    /// physical IDs (every row when the slice is empty).
    fn read_row_counts(&self, ids: &[i64]) -> Result<Vec<(i64, u64)>, Self::Error>;

    /// Reads `select table_id, hist_id, tot_col_size from mysql.stats_histograms
    /// where is_index = 0` for the given physical IDs (every row when the slice
    /// is empty). Negative stored sizes are clamped to zero at this boundary,
    /// like Go's `max(row.GetInt64(2), 0)`.
    fn read_column_lengths(&self, ids: &[i64]) -> Result<Vec<(TableHistId, u64)>, Self::Error>;
}

impl StatsTableRowCache {
    /// Go's `TableRowStatsCache` initializer: two empty maps.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(State::default()),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, State> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, State> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `GetTableRows`. An absent ID reads as zero.
    #[must_use]
    pub fn get_table_rows(&self, table_id: i64) -> u64 {
        self.read().table_rows.get(&table_id).copied().unwrap_or(0)
    }

    /// Go `GetColLength`. An absent ID reads as zero.
    #[must_use]
    pub fn get_col_length(&self, id: TableHistId) -> u64 {
        self.read().col_length.get(&id).copied().unwrap_or(0)
    }

    /// Go `UpdateByID`'s copy step: `maps.Copy` both maps under one write
    /// lock. Entries absent from the fresh reads keep their previous cached
    /// values, and both maps move together.
    pub fn update(&self, table_rows: &[(i64, u64)], col_lengths: &[(TableHistId, u64)]) {
        let mut state = self.write();
        for (table_id, count) in table_rows {
            state.table_rows.insert(*table_id, *count);
        }
        for (id, length) in col_lengths {
            state.col_length.insert(*id, *length);
        }
    }

    /// Go `UpdateByID`: perform both restricted reads, then copy both maps
    /// into the cache. When either read fails, nothing is copied — Go returns
    /// before its two `maps.Copy` calls, so the caller keeps serving the
    /// previous values after logging the warning.
    pub fn update_by_id<S: StatsTableRowSizeSource>(
        &self,
        source: &S,
        ids: &[i64],
    ) -> Result<(), S::Error> {
        let table_rows = source.read_row_counts(ids)?;
        let col_lengths = source.read_column_lengths(ids)?;
        self.update(&table_rows, &col_lengths);
        Ok(())
    }

    /// One consistent view of both maps, feeding the shared
    /// `TableSizeStats` estimators on the consumer side.
    #[must_use]
    pub fn snapshot(&self) -> SizeSnapshot {
        let state = self.read();
        (
            state.table_rows.iter().map(|(&k, &v)| (k, v)).collect(),
            state.col_length.iter().map(|(&k, &v)| (k, v)).collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_ids_read_as_zero() {
        let cache = StatsTableRowCache::new();
        assert_eq!(cache.get_table_rows(42), 0);
        assert_eq!(cache.get_col_length((42, 1)), 0);
    }

    #[test]
    fn update_upserts_and_retains_entries_absent_from_the_batch() {
        let cache = StatsTableRowCache::new();
        cache.update(&[(1, 10), (2, 20)], &[((1, 11), 100), ((2, 22), 200)]);
        // A later batch that only mentions one table must not erase the other,
        // because Go's maps.Copy only overwrites the keys the fresh reads carry.
        cache.update(&[(1, 15)], &[((1, 11), 150)]);
        assert_eq!(cache.get_table_rows(1), 15);
        assert_eq!(cache.get_table_rows(2), 20);
        assert_eq!(cache.get_col_length((1, 11)), 150);
        assert_eq!(cache.get_col_length((2, 22)), 200);
    }

    #[test]
    fn update_by_id_copies_both_maps_only_when_both_reads_succeed() {
        struct Source {
            rows: Result<Vec<(i64, u64)>, &'static str>,
            lengths: Result<Vec<(TableHistId, u64)>, &'static str>,
        }
        impl StatsTableRowSizeSource for Source {
            type Error = &'static str;
            fn read_row_counts(&self, _ids: &[i64]) -> Result<Vec<(i64, u64)>, Self::Error> {
                self.rows.clone()
            }
            fn read_column_lengths(
                &self,
                _ids: &[i64],
            ) -> Result<Vec<(TableHistId, u64)>, Self::Error> {
                self.lengths.clone()
            }
        }

        let cache = StatsTableRowCache::new();
        cache.update(&[(1, 10)], &[((1, 1), 100)]);

        cache
            .update_by_id(
                &Source {
                    rows: Ok(vec![(1, 12), (2, 20)]),
                    lengths: Ok(vec![((1, 1), 120)]),
                },
                &[1, 2],
            )
            .expect("both reads succeed");
        assert_eq!(cache.get_table_rows(1), 12);
        assert_eq!(cache.get_table_rows(2), 20);
        assert_eq!(cache.get_col_length((1, 1)), 120);

        // A row-count failure must not copy anything, not even the rows the
        // failed read did return.
        cache
            .update_by_id(
                &Source {
                    rows: Err("row read failed"),
                    lengths: Ok(vec![((1, 1), 999)]),
                },
                &[1],
            )
            .unwrap_err();
        assert_eq!(cache.get_table_rows(1), 12);
        assert_eq!(cache.get_col_length((1, 1)), 120);

        // A column-length failure likewise leaves both maps untouched, which
        // is what lets Go's reader serve the previous values after a failed
        // refresh.
        cache
            .update_by_id(
                &Source {
                    rows: Ok(vec![(1, 30)]),
                    lengths: Err("length read failed"),
                },
                &[1],
            )
            .unwrap_err();
        assert_eq!(cache.get_table_rows(1), 12);
        assert_eq!(cache.get_col_length((1, 1)), 120);
    }

    #[test]
    fn snapshot_reflects_every_cached_entry() {
        let cache = StatsTableRowCache::new();
        cache.update(&[(1, 10), (2, 20)], &[((1, 11), 100)]);
        let (mut rows, lengths) = cache.snapshot();
        rows.sort_unstable();
        assert_eq!(rows, vec![(1, 10), (2, 20)]);
        assert_eq!(lengths, vec![((1, 11), 100)]);
    }
}
