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

//! Read-your-own-writes: a transaction's staged rows over its own snapshot.
//!
//! Inside an explicit transaction a read must see the transaction's uncommitted
//! writes, but the rows come from a coprocessor scan running at `start_ts` on
//! TiKV, which by construction cannot see them. TiDB resolves this the same way:
//! `UnionScanExec` sits above the reader and merges the `MemBuffer`'s dirty rows
//! into the snapshot stream — a rewritten row replaces the snapshot's, a deleted
//! row is dropped, and a row inserted in this transaction is added even though
//! the snapshot has never held it.
//!
//! That is exactly this stage, over the already-decoded overlay the executor's
//! transaction produced. It is only ever installed when the transaction really
//! has staged writes, so an ordinary read inside a transaction keeps the plain
//! streaming path untouched.

use std::collections::BTreeMap;

use tidb_datatype::Datum;
use tidb_exec::multi_statement_transaction::StagedRowOverlay;
use tidb_protocol::ColumnInfo;

use crate::resultset_source::ResultSetSource;

/// How each snapshot row's clustered handle is known.
///
/// The overlay is keyed by handle, so every snapshot row has to be identified by
/// one. Either the read projects the clustered primary key — then the handle is
/// in the row — or the plan resolved to exactly one point handle, in which case
/// every row it can return is that handle. A read that satisfies neither cannot
/// be overlaid and is refused before it runs, rather than silently returning
/// pre-transaction rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayHandleSource {
    /// The clustered primary key is projected at this output offset.
    ProjectedAt(usize),
    /// The plan resolved to this single point handle.
    SinglePoint(i64),
}

/// Merges a transaction's staged rows into its own snapshot scan.
pub struct TransactionOverlayResultSet<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
    /// Staged rows by handle; `None` is a row this transaction deleted.
    overlay: BTreeMap<i64, Option<Vec<Datum>>>,
    handles: OverlayHandleSource,
    /// Staged handles already emitted in place of a snapshot row, so the
    /// inserted-row pass at the end does not emit them twice.
    replaced: BTreeMap<i64, ()>,
    /// Whether the snapshot stream is exhausted and the rows this transaction
    /// inserted have been appended.
    appended_inserts: bool,
}

impl<'a> TransactionOverlayResultSet<'a> {
    /// Wraps `inner`, applying `overlay` to every row it produces.
    #[must_use]
    pub fn new(
        inner: Box<dyn ResultSetSource + 'a>,
        overlay: StagedRowOverlay,
        handles: OverlayHandleSource,
    ) -> Self {
        Self {
            inner,
            overlay: overlay.into_iter().collect(),
            handles,
            replaced: BTreeMap::new(),
            appended_inserts: false,
        }
    }

    /// The clustered handle of one snapshot row.
    fn row_handle(&self, row: &[Datum]) -> Result<i64, String> {
        match self.handles {
            OverlayHandleSource::SinglePoint(handle) => Ok(handle),
            OverlayHandleSource::ProjectedAt(offset) => row
                .get(offset)
                .and_then(Datum::as_int)
                .ok_or_else(|| "scanned row has no clustered handle to overlay".to_owned()),
        }
    }

    /// Rows this transaction inserted that the snapshot could not produce, in
    /// ascending handle order so the appended tail is deterministic.
    fn inserted_rows(&mut self) -> Vec<Vec<Datum>> {
        self.appended_inserts = true;
        self.overlay
            .iter()
            .filter(|(handle, row)| row.is_some() && !self.replaced.contains_key(*handle))
            .filter_map(|(_, row)| row.clone())
            .collect()
    }
}

impl ResultSetSource for TransactionOverlayResultSet<'_> {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        loop {
            let batch = self.inner.next_batch(max_rows)?;
            if batch.is_empty() {
                // The snapshot is exhausted; what remains are the rows this
                // transaction inserted, which no snapshot at `start_ts` holds.
                if self.appended_inserts {
                    return Ok(Vec::new());
                }
                return Ok(self.inserted_rows());
            }
            let mut overlaid = Vec::with_capacity(batch.len());
            for row in batch {
                let handle = self.row_handle(&row)?;
                match self.overlay.get(&handle) {
                    // Unstaged: the snapshot's row is the transaction's row.
                    None => overlaid.push(row),
                    // Deleted in this transaction: the read must not return it.
                    Some(None) => {
                        self.replaced.insert(handle, ());
                    }
                    // Rewritten in this transaction: its own value wins.
                    Some(Some(staged)) => {
                        self.replaced.insert(handle, ());
                        overlaid.push(staged.clone());
                    }
                }
            }
            // A batch whose every row was deleted must not be mistaken for the
            // end of the stream, so keep pulling until there is a row or the
            // snapshot really is exhausted.
            if !overlaid.is_empty() {
                return Ok(overlaid);
            }
        }
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
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
    use super::{OverlayHandleSource, TransactionOverlayResultSet};
    use crate::resultset_source::ResultSetSource;
    use tidb_datatype::Datum;
    use tidb_protocol::ColumnInfo;

    /// A snapshot scan that hands out already-decoded rows in fixed batches.
    struct SnapshotRows {
        batches: Vec<Vec<Vec<Datum>>>,
    }

    impl ResultSetSource for SnapshotRows {
        fn next_batch(&mut self, _max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
            if self.batches.is_empty() {
                return Ok(Vec::new());
            }
            Ok(self.batches.remove(0))
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

    fn row(handle: i64, balance: i64) -> Vec<Datum> {
        vec![Datum::new_int(handle), Datum::new_int(balance)]
    }

    fn drain(source: &mut impl ResultSetSource) -> Vec<Vec<Datum>> {
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(16).expect("the overlay must not fail");
            if batch.is_empty() {
                return rows;
            }
            rows.extend(batch);
        }
    }

    #[test]
    fn a_transaction_sees_its_own_rewritten_deleted_and_inserted_rows() {
        // The snapshot at start_ts holds rows 1, 2 and 3. This transaction
        // rewrote 2, deleted 3, and inserted 4 — so its own read returns the new
        // 2, no 3 at all, and a 4 the snapshot has never held.
        let inner = SnapshotRows {
            batches: vec![vec![row(1, 10), row(2, 20)], vec![row(3, 30)]],
        };
        let mut overlay = TransactionOverlayResultSet::new(
            Box::new(inner),
            vec![(2, Some(row(2, 99))), (3, None), (4, Some(row(4, 40)))],
            OverlayHandleSource::ProjectedAt(0),
        );

        assert_eq!(
            drain(&mut overlay),
            vec![row(1, 10), row(2, 99), row(4, 40)],
            "rewritten rows carry the transaction's value, deletes vanish, \
             inserts are appended"
        );
    }

    #[test]
    fn a_point_read_without_the_key_projected_uses_the_plans_own_handle() {
        // `SELECT balance FROM t WHERE id = 7` projects no handle, but the plan
        // resolved to exactly one, so every row it can return is that row.
        let inner = SnapshotRows {
            batches: vec![vec![vec![Datum::new_int(20)]]],
        };
        let mut overlay = TransactionOverlayResultSet::new(
            Box::new(inner),
            vec![(7, Some(vec![Datum::new_int(99)]))],
            OverlayHandleSource::SinglePoint(7),
        );
        assert_eq!(drain(&mut overlay), vec![vec![Datum::new_int(99)]]);
    }

    #[test]
    fn a_point_read_of_a_row_this_transaction_deleted_returns_nothing() {
        let inner = SnapshotRows {
            batches: vec![vec![row(7, 20)]],
        };
        let mut overlay = TransactionOverlayResultSet::new(
            Box::new(inner),
            vec![(7, None)],
            OverlayHandleSource::ProjectedAt(0),
        );
        assert!(
            drain(&mut overlay).is_empty(),
            "a batch emptied entirely by the overlay is not the end of the stream"
        );
    }

    #[test]
    fn an_inserted_row_is_emitted_once_even_when_the_snapshot_also_held_it() {
        // A row staged as an INSERT whose key the snapshot somehow also returns
        // must appear exactly once, carrying the transaction's own value.
        let inner = SnapshotRows {
            batches: vec![vec![row(5, 1)]],
        };
        let mut overlay = TransactionOverlayResultSet::new(
            Box::new(inner),
            vec![(5, Some(row(5, 2)))],
            OverlayHandleSource::ProjectedAt(0),
        );
        assert_eq!(drain(&mut overlay), vec![row(5, 2)]);
    }
}
