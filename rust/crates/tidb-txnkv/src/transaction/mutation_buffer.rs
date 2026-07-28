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

//! Transaction-local mutation staging — the bounded optimistic membuffer.
//!
//! An explicit multi-statement transaction issues several writes and commits
//! them together, so it must coalesce the per-statement mutations into exactly
//! one committed entry per key. TiDB does this in its `MemBuffer`: each `Set`/
//! `Delete` overwrites the key's prior staged entry, and at commit
//! `twoPhaseCommitter.initKeysAndMutations` emits one `kvrpcpb.Mutation` per key
//! from that final state. The concrete commit path here already enforces exactly
//! one mutation per key ([`super::mutation::validate_and_sort`] rejects a
//! `DuplicateKey`), so without this coalescing the sysbench transaction — which
//! `DELETE`s a row id and re-`INSERT`s the same id — would present two mutations
//! for one key and fail before ever reaching TiKV.
//!
//! This is a **bounded** membuffer: it models only the coalescing the deployable
//! optimistic node actually produces and that is verified against the Go source.
//! Every other same-key transition fails closed ([`MutationBufferError::
//! UnsupportedCoalesce`]) rather than silently committing a possibly-clobbering
//! value — the memdb flag bookkeeping for insert-then-delete
//! (`Op_CheckNotExists`) and same-key index rewrites are later slices, not
//! silently-wrong behavior here.
//!
//! The buffer is also the transaction's own read source: [`Self::staged`] hands
//! back the entry a key currently carries, which is what makes read-your-own-
//! writes possible. TiDB reads exactly this way — `txn.Get` consults the
//! `MemBuffer` before the snapshot — and it is what makes the repeated-update
//! coalescing below correct rather than clobbering: the second update computed
//! its new row from the first one's staged value.

use std::collections::BTreeMap;

use super::mutation::{OptimisticMutation, OptimisticMutationKind};

/// Rejects a same-key mutation combination this bounded buffer does not model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationBufferError {
    /// Two mutations target one key in a combination beyond the verified
    /// coalescing set. Only a row `DELETE` followed by an `INSERT` of the same
    /// key is coalesced; anything else fails closed so a transaction never
    /// commits a value whose correctness this buffer cannot vouch for.
    UnsupportedCoalesce {
        /// The encoded key already carrying an incompatible staged mutation.
        key: Vec<u8>,
    },
}

impl std::fmt::Display for MutationBufferError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedCoalesce { .. } => formatter
                .write_str("transaction stages an unsupported same-key mutation combination"),
        }
    }
}

impl std::error::Error for MutationBufferError {}

/// A transaction-local staging buffer that coalesces per-key mutations into the
/// one committed entry per key that TiDB's `MemBuffer` produces.
///
/// Keys are held sorted, so [`Self::into_mutations`] hands the commit path its
/// mutations already in ascending key order.
#[derive(Debug, Default)]
pub struct TransactionMutationBuffer {
    staged: BTreeMap<Vec<u8>, OptimisticMutation>,
}

impl TransactionMutationBuffer {
    /// An empty buffer, before the transaction stages any write.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the transaction has staged no writes (a read-only transaction, or
    /// one before its first write).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    /// The number of distinct keys staged so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.staged.len()
    }

    /// The entry this transaction currently has staged for `key`, if any.
    ///
    /// This is the membuffer half of TiDB's `txn.Get`: a reader inside the
    /// transaction consults it before falling back to the snapshot at
    /// `start_ts`, so the transaction observes its own uncommitted writes.
    #[must_use]
    pub fn staged(&self, key: &[u8]) -> Option<&OptimisticMutation> {
        self.staged.get(key)
    }

    /// Stages one mutation, coalescing it with any mutation already staged for
    /// the same key.
    ///
    /// A key not yet staged takes `mutation` verbatim. A key already staged is
    /// coalesced by [`coalesce`]; an unmodeled combination fails closed.
    pub fn stage(&mut self, mutation: OptimisticMutation) -> Result<(), MutationBufferError> {
        match self.staged.get(mutation.key()) {
            None => {
                self.staged.insert(mutation.key().to_vec(), mutation);
                Ok(())
            }
            Some(existing) => {
                let coalesced = coalesce(existing, &mutation).ok_or_else(|| {
                    MutationBufferError::UnsupportedCoalesce {
                        key: mutation.key().to_vec(),
                    }
                })?;
                self.staged.insert(mutation.key().to_vec(), coalesced);
                Ok(())
            }
        }
    }

    /// Every staged entry in ascending key order.
    ///
    /// A reader inside the transaction uses this to find which of its own
    /// uncommitted rows fall in the range it is about to scan, the way Go's
    /// `UnionScanExec` walks the `MemBuffer` over the scanned key range.
    pub fn staged_entries(&self) -> impl Iterator<Item = &OptimisticMutation> {
        self.staged.values()
    }

    /// The coalesced mutations in ascending key order, ready for `commit`.
    #[must_use]
    pub fn into_mutations(self) -> Vec<OptimisticMutation> {
        self.staged.into_values().collect()
    }
}

/// Coalesces `next` onto the `existing` staged mutation for one key, returning
/// `None` for a combination this bounded buffer does not model.
///
/// Every modeled combination follows one rule of TiDB's `MemBuffer`: the newest
/// `Set`/`Delete` decides the key's final *value*, while the *assertion* is the
/// one the first staged entry established ("only the first assertion takes
/// effect", `memBuffer.UpdateFlags`). The four cases below are exactly the ones
/// this node's statements can produce.
///
/// * `DELETE` then `INSERT` — sysbench's delete-and-reinsert of one id. TiDB's
///   `AddRecord` checks the key through `GetMemBuffer().GetLocal`/`txn.Get`,
///   finds the in-transaction delete, and so does **not** set
///   `flagPresumeKeyNotExists`; `initKeysAndMutations` then emits `Op_Put`, not
///   `Op_Insert`. The delete already set `kv.AssertExist`, which holds because
///   the key existed at `start_ts`. That is [`OptimisticMutationKind::
///   PutExisting`] (`Op_Put` + `Exist`).
/// * `UPDATE` then `UPDATE` — the later value wins outright. This is safe
///   precisely because the second statement read the first one's staged row
///   through [`TransactionMutationBuffer::staged`], so its replacement row was
///   computed from the value it is overwriting rather than from the stale
///   snapshot.
/// * `INSERT` then `UPDATE` — the row is still new to the cluster, so
///   `flagPresumeKeyNotExists` survives and the committed mutation stays
///   `Op_Insert` (`AssertNotExist`), carrying the updated value.
/// * `UPDATE` then `DELETE` — the row existed at `start_ts`, so the final entry
///   is the plain row delete (`Op_Del` + `Exist`), discarding the intermediate
///   value that was never published.
fn coalesce(
    existing: &OptimisticMutation,
    next: &OptimisticMutation,
) -> Option<OptimisticMutation> {
    match (existing.kind(), next.kind()) {
        (OptimisticMutationKind::Delete, OptimisticMutationKind::Insert) => {
            // `next` is a validated Insert, so the same key/value is a valid
            // PutExisting; a validation error here can only mean an unmodeled
            // input, which fails closed like any other unsupported combination.
            OptimisticMutation::put_existing(next.key().to_vec(), next.value().to_vec()).ok()
        }
        (OptimisticMutationKind::PutExisting, OptimisticMutationKind::PutExisting)
        | (OptimisticMutationKind::PutExisting, OptimisticMutationKind::Delete) => {
            Some(next.clone())
        }
        (OptimisticMutationKind::Insert, OptimisticMutationKind::PutExisting) => {
            OptimisticMutation::insert(next.key().to_vec(), next.value().to_vec()).ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_proto::{KvrpcAssertion, KvrpcOp};

    fn key_of(mutation: &OptimisticMutation) -> Vec<u8> {
        mutation.key().to_vec()
    }

    #[test]
    fn distinct_keys_accumulate_in_ascending_key_order() {
        // The common transaction shape: writes to distinct keys, each kept
        // verbatim and handed to commit sorted by key.
        let mut buffer = TransactionMutationBuffer::new();
        assert!(buffer.is_empty());
        buffer
            .stage(OptimisticMutation::put_existing(b"c".to_vec(), b"3".to_vec()).unwrap())
            .unwrap();
        buffer
            .stage(OptimisticMutation::insert(b"a".to_vec(), b"1".to_vec()).unwrap())
            .unwrap();
        buffer
            .stage(OptimisticMutation::delete(b"b".to_vec()).unwrap())
            .unwrap();
        assert_eq!(buffer.len(), 3);

        let mutations = buffer.into_mutations();
        assert_eq!(
            mutations.iter().map(key_of).collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "commit receives one mutation per key in ascending key order"
        );
        assert_eq!(mutations[0].kind(), OptimisticMutationKind::Insert);
        assert_eq!(mutations[1].kind(), OptimisticMutationKind::Delete);
        assert_eq!(mutations[2].kind(), OptimisticMutationKind::PutExisting);
    }

    #[test]
    fn delete_then_insert_same_key_coalesces_to_put_existing() {
        // sysbench oltp_read_write deletes one id then re-inserts the same id in
        // one transaction. TiDB coalesces this to a single Op_Put (not Op_Insert)
        // whose assertion stays Exist. The committed value is the INSERT's value.
        let mut buffer = TransactionMutationBuffer::new();
        buffer
            .stage(OptimisticMutation::delete(b"row".to_vec()).unwrap())
            .unwrap();
        buffer
            .stage(OptimisticMutation::insert(b"row".to_vec(), b"reinserted".to_vec()).unwrap())
            .unwrap();

        let mutations = buffer.into_mutations();
        assert_eq!(mutations.len(), 1, "the two ops coalesce to one key");
        assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
        assert_eq!(mutations[0].value(), b"reinserted");

        let proto = mutations[0].to_proto();
        assert_eq!(proto.op, KvrpcOp::Put as i32, "Op_Put, never Op_Insert");
        assert_eq!(
            proto.assertion,
            KvrpcAssertion::Exist as i32,
            "the delete's AssertExist wins; the key existed at start_ts"
        );
    }

    #[test]
    fn repeated_updates_of_one_key_keep_the_last_value() {
        // Two UPDATEs of one row in one transaction. The second read the first's
        // staged row (read-your-own-writes), so its value is the transaction's
        // final one and one Op_Put carries it.
        let mut buffer = TransactionMutationBuffer::new();
        buffer
            .stage(OptimisticMutation::put_existing(b"row".to_vec(), b"v1".to_vec()).unwrap())
            .unwrap();
        buffer
            .stage(OptimisticMutation::put_existing(b"row".to_vec(), b"v2".to_vec()).unwrap())
            .unwrap();
        assert_eq!(buffer.staged(b"row").unwrap().value(), b"v2");

        let mutations = buffer.into_mutations();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].kind(), OptimisticMutationKind::PutExisting);
        assert_eq!(mutations[0].value(), b"v2");
    }

    #[test]
    fn a_new_rows_later_update_still_commits_as_an_insert() {
        // INSERT then UPDATE of the same new row: the key is still absent in the
        // cluster, so flagPresumeKeyNotExists survives and the committed
        // mutation stays Op_Insert while carrying the updated value.
        let mut buffer = TransactionMutationBuffer::new();
        buffer
            .stage(OptimisticMutation::insert(b"row".to_vec(), b"fresh".to_vec()).unwrap())
            .unwrap();
        buffer
            .stage(OptimisticMutation::put_existing(b"row".to_vec(), b"updated".to_vec()).unwrap())
            .unwrap();

        let mutations = buffer.into_mutations();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].kind(), OptimisticMutationKind::Insert);
        assert_eq!(mutations[0].value(), b"updated");
        assert_eq!(
            mutations[0].to_proto().assertion,
            KvrpcAssertion::NotExist as i32,
            "the row is still new to the cluster"
        );
    }

    #[test]
    fn updating_then_deleting_an_existing_row_commits_only_the_delete() {
        let mut buffer = TransactionMutationBuffer::new();
        buffer
            .stage(OptimisticMutation::put_existing(b"row".to_vec(), b"v1".to_vec()).unwrap())
            .unwrap();
        buffer
            .stage(OptimisticMutation::delete(b"row".to_vec()).unwrap())
            .unwrap();

        let mutations = buffer.into_mutations();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].kind(), OptimisticMutationKind::Delete);
        assert!(
            mutations[0].value().is_empty(),
            "the intermediate value was never published"
        );
    }

    #[test]
    fn an_unstaged_key_reads_as_absent_from_the_buffer() {
        let mut buffer = TransactionMutationBuffer::new();
        assert!(buffer.staged(b"row").is_none());
        buffer
            .stage(OptimisticMutation::delete(b"row".to_vec()).unwrap())
            .unwrap();
        assert_eq!(
            buffer.staged(b"row").map(OptimisticMutation::kind),
            Some(OptimisticMutationKind::Delete),
            "a staged delete is visible to the transaction's own reads"
        );
    }

    #[test]
    fn insert_then_delete_same_key_fails_closed() {
        // Insert-then-delete of a fresh key becomes Op_CheckNotExists in TiDB
        // (flagPresumeKeyNotExists on a now-empty value); that memdb flag state
        // is a later slice, so it fails closed rather than emitting a plain Del.
        let mut buffer = TransactionMutationBuffer::new();
        buffer
            .stage(OptimisticMutation::insert(b"row".to_vec(), b"v".to_vec()).unwrap())
            .unwrap();
        assert_eq!(
            buffer.stage(OptimisticMutation::delete(b"row".to_vec()).unwrap()),
            Err(MutationBufferError::UnsupportedCoalesce {
                key: b"row".to_vec()
            })
        );
    }

    #[test]
    fn same_key_index_rewrite_fails_closed() {
        // A DELETE+INSERT that leaves an indexed value unchanged would rewrite one
        // index key (IndexDelete then IndexPut). That coalescing is faithful but
        // unverified here, so it fails closed for now instead of guessing.
        let mut buffer = TransactionMutationBuffer::new();
        buffer
            .stage(OptimisticMutation::index_delete(b"idx".to_vec()).unwrap())
            .unwrap();
        assert_eq!(
            buffer.stage(OptimisticMutation::index_put(b"idx".to_vec(), b"0".to_vec()).unwrap()),
            Err(MutationBufferError::UnsupportedCoalesce {
                key: b"idx".to_vec()
            })
        );
    }

    #[test]
    fn an_empty_buffer_stages_nothing() {
        let buffer = TransactionMutationBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert!(buffer.into_mutations().is_empty());
    }
}
