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

//! Go `txn.GetMemBuffer()` narrowed to the record keys row writes stage.

/// One staged record key, stored inline rather than as a fresh heap
/// allocation.
///
/// Go answers `HasDirtyContent`/`GetLocal` straight from the transaction's
/// own write buffer -- the SAME tree its real mutation already updated, at
/// no extra cost. [`StagedWrites`] is a deliberately separate side index (see
/// its own item doc), so unlike Go it does pay a second write per staged
/// key; keeping that key inline instead of a heap `Vec<u8>` keeps the second
/// cost close to zero for a plain record key's usual width. An
/// integer-handle record key is `t{id}_r{handle}`: 11 bytes of prefix plus an
/// 8-byte memcomparable handle, 19 bytes total, well under this inline
/// capacity; a longer common-handle or index-entry key spills to the heap
/// like an ordinary `Vec` would, so correctness never depends on the size.
pub type StagedKey = smallvec::SmallVec<[u8; 32]>;

/// Go `txn.GetMemBuffer()`: the ONE membuffer a transaction owns, narrowed to
/// the record keys row writes stage, keyed by the logical table id they
/// belong to.
///
/// This does NOT live on `KvTable`. Go's `HasDirtyContent`/`memBufSnap.Get`
/// answer per table by seeking ONE transaction-scoped structure, never by
/// storing a flag on the table itself -- a fresh transaction gets a fresh,
/// EMPTY membuffer and the old one is simply dropped, so there is nothing to
/// walk or reset at a transaction boundary, ever, regardless of how many
/// tables the catalog holds. Embedding the equivalent state on `KvTable`
/// instead (an earlier shape of this type) forced exactly that walk: every
/// autocommit statement and `BEGIN` had to visit every kv table in the whole
/// catalog to reset a mark a committed table might still be carrying from
/// its previous writer. See `StmtContext::staged_writes` for how one shared
/// handle reaches every write and read site for the CURRENT transaction (or,
/// in autocommit, the current statement) without threading a new parameter
/// through the whole executor.
#[derive(Debug, Default)]
pub struct StagedWrites(
    std::sync::Mutex<std::collections::HashMap<i64, std::collections::HashSet<StagedKey>>>,
);

/// The transaction's [`StagedWrites`] attached to one table's store for one
/// write, Go's membuffer for a backend that writes through (see
/// [`crate::storage::TableStorage::attach_txn_staging`]).
#[derive(Clone, Debug)]
pub struct TxnStaging {
    /// The transaction's staged writes.
    pub writes: std::sync::Arc<StagedWrites>,
    /// The logical table whose store is written.
    pub table_id: i64,
    /// Whether the transaction is optimistic. Its log also answers Go's
    /// `GetLocal`; a pessimistic transaction's log only records the keys it
    /// wrote.
    pub optimistic: bool,
}

impl StagedWrites {
    /// Records that `table_id` staged a row write under `key` -- Go
    /// `membuf.Set(key)`.
    pub fn note(&self, table_id: i64, key: &[u8]) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(table_id)
            .or_default()
            .insert(StagedKey::from_slice(key));
    }

    /// Marks `table_id` dirty with no specific key -- DDL that rewrites a
    /// table's rows or partition layout in place (a partition rehash,
    /// COALESCE/ADD/DROP PARTITION) rather than staging individual record
    /// keys.
    pub fn mark_dirty(&self, table_id: i64) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(table_id)
            .or_default();
    }

    /// Every table id this transaction has staged anything under, in order.
    #[must_use]
    pub fn dirty_table_ids(&self) -> Vec<i64> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }

    /// Go `session.HasDirtyContent(tid)`: has this transaction staged
    /// anything under `table_id`.
    #[must_use]
    pub fn has_dirty_content(&self, table_id: i64) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&table_id)
    }

    /// Go `memBufSnap.Get(checkKey)`: does `table_id`'s staged set hold
    /// exactly `key`.
    #[must_use]
    pub fn contains(&self, table_id: i64, key: &[u8]) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&table_id)
            .is_some_and(|keys| keys.contains(key))
    }

    /// The keys `table_id` staged -- Go's membuffer walked for one table --
    /// or `None` when the transaction never wrote it.
    #[must_use]
    pub fn keys_of(&self, table_id: i64) -> Option<Vec<Vec<u8>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&table_id)
            .map(|keys| keys.iter().map(|key| key.to_vec()).collect())
    }
}
