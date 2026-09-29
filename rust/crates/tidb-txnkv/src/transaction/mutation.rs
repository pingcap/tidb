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

/// One normal optimistic mutation admitted by the concrete TiKV coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimisticMutationKind {
    /// Create a value and fail if the key already exists at `start_ts`.
    Insert,
    /// Replace a value and fail if the key does not exist at `start_ts`.
    PutExisting,
    /// Delete a value and assert the key exists at `start_ts`, matching Go
    /// `TableCommon.removeRecord`, which sets `kv.AssertExist` before
    /// `txn.Delete(key)`.
    Delete,
    /// Write a non-unique secondary index entry. Go `tables.index.create` does a
    /// plain `MemBuffer.Set` (`Op_Put`) — not the row key's `Op_Insert` — and,
    /// on the default optimistic lazy-check path, leaves the assertion
    /// unresolved (`kv.AssertUnknown` -> proto `None`): the index key already
    /// carries the row handle, so a new row's entry cannot collide.
    IndexPut,
    /// Create a unique secondary index entry and assert its key did not exist.
    /// The index key omits the row handle, so a plain put would permit a
    /// concurrent duplicate to overwrite the original entry.
    UniqueIndexInsert,
    /// Delete a non-unique secondary index entry. Go `tables.index.Delete` does a
    /// plain `MemBuffer.Delete` (`Op_Del`) with an unresolved assertion
    /// (`None`), unlike the row delete's `Exist`.
    IndexDelete,
    /// Write one catalog meta key in the `m` namespace. Go's whole meta layer
    /// (`structure.Set`, `structure.HSet`, `kv.IncInt64`) reaches storage as a
    /// plain `txn.Set` with no assertion: a meta key may or may not already
    /// exist (`SchemaVersionKey` on a fresh cluster, every `Diff:<ver>`), and
    /// the DDL's own snapshot reads have already established what is there.
    MetaPut,
    /// Delete one catalog meta key. Go `structure.HDel` performs a plain
    /// `txn.Delete` with no assertion, and only for a field it just observed.
    MetaDelete,
    /// Replace an unindexed clustered system-table row with no existence
    /// assertion. Go SQL `REPLACE` uses a plain mem-buffer `Set`: the row may
    /// be absent on the first phase or present when a preceding best-effort
    /// cleanup failed.
    SystemRowPut,
    /// Delete an unindexed clustered system-table row with no existence
    /// assertion. SQL `DELETE` succeeds when the row is already absent.
    SystemRowDelete,
    /// Prewrite a key this transaction locked but never wrote (`Op_Lock`).
    ///
    /// Go `twoPhaseCommitter.initKeysAndMutations` (`2pc.go`) emits exactly
    /// this for every membuffer entry carrying `HasLocked()` and no value
    /// change. It is what guarantees the pinned pessimistic primary is always
    /// among the prewritten mutations, so the primary lock — the transaction's
    /// only recovery entry point — actually exists after prewrite.
    LockOnly,
}

/// One immutable encoded-key mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimisticMutation {
    kind: OptimisticMutationKind,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl OptimisticMutation {
    /// Creates an optimistic Insert with TiKV's not-exists assertion.
    pub fn insert(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        Self::new(OptimisticMutationKind::Insert, key.into(), value.into())
    }

    /// Creates an optimistic UPDATE Put with TiKV's exists assertion.
    pub fn put_existing(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        Self::new(
            OptimisticMutationKind::PutExisting,
            key.into(),
            value.into(),
        )
    }

    /// Creates an optimistic DELETE with TiKV's exists assertion. A delete
    /// carries no value.
    pub fn delete(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        Self::new(OptimisticMutationKind::Delete, key.into(), Vec::new())
    }

    /// Creates a non-unique secondary index entry write (`Op_Put`, no assertion).
    pub fn index_put(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        Self::new(OptimisticMutationKind::IndexPut, key.into(), value.into())
    }

    /// Creates a unique secondary index entry (`Op_Insert`, `NotExist`).
    pub fn unique_index_insert(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        Self::new(
            OptimisticMutationKind::UniqueIndexInsert,
            key.into(),
            value.into(),
        )
    }

    /// Creates a non-unique secondary index entry delete (`Op_Del`, no
    /// assertion). A delete carries no value.
    pub fn index_delete(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        Self::new(OptimisticMutationKind::IndexDelete, key.into(), Vec::new())
    }

    /// Creates a catalog meta-key write (`Op_Put`, no assertion).
    pub fn meta_put(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        Self::new(OptimisticMutationKind::MetaPut, key.into(), value.into())
    }

    /// Creates a catalog meta-key delete (`Op_Del`, no assertion).
    pub fn meta_delete(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        Self::new(OptimisticMutationKind::MetaDelete, key.into(), Vec::new())
    }

    /// Creates a system-row `Op_Put` with no existence assertion.
    pub fn system_row_put(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, MutationSetError> {
        Self::new(
            OptimisticMutationKind::SystemRowPut,
            key.into(),
            value.into(),
        )
    }

    /// Creates a system-row `Op_Del` with no existence assertion.
    pub fn system_row_delete(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        Self::new(
            OptimisticMutationKind::SystemRowDelete,
            key.into(),
            Vec::new(),
        )
    }

    /// Creates an `Op_Lock` mutation for a locked-but-unwritten key.
    ///
    /// Go `2pc.go` `initKeysAndMutations`: `} else if it.Flags().HasLocked() {
    /// op = kvrpcpb.Op_Lock }`. Carries no value and no assertion — it exists
    /// so that prewrite writes a lock on the key, nothing more.
    pub fn lock_only(key: impl Into<Vec<u8>>) -> Result<Self, MutationSetError> {
        Self::new(OptimisticMutationKind::LockOnly, key.into(), Vec::new())
    }

    fn new(
        kind: OptimisticMutationKind,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Self, MutationSetError> {
        validate_key_value(&key, &value)?;
        Ok(Self { kind, key, value })
    }

    /// Mutation operation.
    #[must_use]
    pub const fn kind(&self) -> OptimisticMutationKind {
        self.kind
    }

    /// Encoded TiKV key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Encoded TiKV value.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Input errors rejected before allocating a transaction timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationSetError {
    /// Normal 2PC cannot commit an empty transaction.
    Empty,
    /// TiKV user keys must not be empty.
    EmptyKey,
    /// One encoded key exceeds the checked transaction bound.
    KeyTooLarge {
        /// Observed encoded key bytes.
        size: usize,
        /// Maximum admitted encoded key bytes.
        limit: usize,
    },
    /// One encoded value exceeds the checked transaction bound.
    ValueTooLarge {
        /// Observed encoded value bytes.
        size: usize,
        /// Maximum admitted encoded value bytes.
        limit: usize,
    },
    /// The planned or actual mutation count exceeds the checked bound.
    TooManyMutations {
        /// Observed mutation count.
        count: usize,
        /// Maximum admitted mutation count.
        limit: usize,
    },
    /// The planned or actual aggregate encoded bytes exceed the checked bound.
    TransactionTooLarge {
        /// Observed aggregate encoded bytes.
        size: usize,
        /// Maximum admitted aggregate encoded bytes.
        limit: usize,
    },
}

impl std::fmt::Display for MutationSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("optimistic transaction requires mutations"),
            Self::EmptyKey => formatter.write_str("optimistic mutation key is empty"),
            Self::KeyTooLarge { size, limit } => {
                write!(
                    formatter,
                    "optimistic mutation key size {size} exceeds {limit}"
                )
            }
            Self::ValueTooLarge { size, limit } => {
                write!(
                    formatter,
                    "optimistic mutation value size {size} exceeds {limit}"
                )
            }
            Self::TooManyMutations { count, limit } => write!(
                formatter,
                "optimistic transaction mutation count {count} exceeds {limit}"
            ),
            Self::TransactionTooLarge { size, limit } => write!(
                formatter,
                "optimistic transaction encoded size {size} exceeds {limit}"
            ),
        }
    }
}

impl std::error::Error for MutationSetError {}

/// The mutation-count budget the ordinary bounded normal-2PC callers declare.
///
/// This is a *caller's declared plan*, enforced per transaction by
/// [`crate::transaction::ProductionOptimisticTransaction::commit`] against the
/// budget that transaction was opened with -- not a global ceiling. A path
/// whose legitimate plan is larger declares its own; see
/// `tidb_exec::real_tikv_analyze::ANALYZE_MAX_MUTATIONS`, which is what one
/// `ANALYZE TABLE` of a real table needs and what Go's own analyze save
/// (`pkg/statistics/handle/storage/save.go`, one transaction per table)
/// places no count limit on at all.
///
/// Go and client-go enforce no such per-transaction mutation *count*: the real
/// limits are byte-based (`txn-entry-size-limit`, default 6MiB per entry;
/// `txn-total-size-limit`, default 100MiB per transaction — mirrored here by
/// [`MAX_OPTIMISTIC_VALUE_BYTES`] and [`MAX_OPTIMISTIC_TRANSACTION_BYTES`]).
/// This count is this path's own sanity ceiling, not a port of anything Go
/// does. It was raised from an initial 256 because a single-owner bootstrap
/// transaction plans one mutation per `mysql.global_variables` row (plus its
/// index entry) for every global-scope system variable — echoing Go's own
/// `doDMLWorks`, all ~720 rows land in the one transaction it seeds the
/// cluster with — and 256 rejected that legitimate plan before the byte
/// budget ever saw it. The client transaction engine already groups
/// an arbitrary mutation count into per-region, byte-bounded RPC batches, so
/// this ceiling is not load-bearing for that path either; it stays as a cheap
/// guard against a plan large enough to be a bug rather than a scaling limit.
pub const MAX_OPTIMISTIC_MUTATIONS: usize = 4096;
/// Maximum encoded TiKV key size admitted by this path.
pub const MAX_OPTIMISTIC_KEY_BYTES: usize = 4 * 1024;
/// Maximum encoded TiKV value size admitted by this path.
pub const MAX_OPTIMISTIC_VALUE_BYTES: usize = 6 * 1024 * 1024;
/// Maximum aggregate encoded key/value bytes admitted by one transaction.
pub const MAX_OPTIMISTIC_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn validate_plan(count: usize, aggregate_bytes: usize) -> Result<(), MutationSetError> {
    if count == 0 {
        return Err(MutationSetError::Empty);
    }
    // No count ceiling here on purpose. Go and client-go enforce none either
    // (`txn-entry-size-limit` / `txn-total-size-limit` are byte-based), and a
    // ceiling applied to *every* mutation set would override the budget a
    // caller declared for its own transaction -- which is what made a
    // real-sized `ANALYZE TABLE` uncommittable while a toy one worked. The
    // bound each transaction is actually held to is the plan it was opened
    // with, checked in `commit`.
    if aggregate_bytes > MAX_OPTIMISTIC_TRANSACTION_BYTES {
        return Err(MutationSetError::TransactionTooLarge {
            size: aggregate_bytes,
            limit: MAX_OPTIMISTIC_TRANSACTION_BYTES,
        });
    }
    Ok(())
}

fn validate_key_value(key: &[u8], value: &[u8]) -> Result<(), MutationSetError> {
    if key.is_empty() {
        return Err(MutationSetError::EmptyKey);
    }
    if key.len() > MAX_OPTIMISTIC_KEY_BYTES {
        return Err(MutationSetError::KeyTooLarge {
            size: key.len(),
            limit: MAX_OPTIMISTIC_KEY_BYTES,
        });
    }
    if value.len() > MAX_OPTIMISTIC_VALUE_BYTES {
        return Err(MutationSetError::ValueTooLarge {
            size: value.len(),
            limit: MAX_OPTIMISTIC_VALUE_BYTES,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_keys_fail_before_storage() {
        assert_eq!(
            OptimisticMutation::insert(Vec::new(), b"v".to_vec()),
            Err(MutationSetError::EmptyKey)
        );
    }

    #[test]
    fn mutation_and_transaction_bounds_are_exact() {
        assert!(OptimisticMutation::insert(
            vec![1; MAX_OPTIMISTIC_KEY_BYTES],
            vec![2; MAX_OPTIMISTIC_VALUE_BYTES]
        )
        .is_ok());
        assert!(matches!(
            OptimisticMutation::insert(vec![1; MAX_OPTIMISTIC_KEY_BYTES + 1], Vec::new()),
            Err(MutationSetError::KeyTooLarge { .. })
        ));
        assert!(matches!(
            OptimisticMutation::insert(b"k".to_vec(), vec![2; MAX_OPTIMISTIC_VALUE_BYTES + 1]),
            Err(MutationSetError::ValueTooLarge { .. })
        ));
        assert!(validate_plan(MAX_OPTIMISTIC_MUTATIONS, 1).is_ok());
        // A count past the ordinary callers' budget is not rejected here: the
        // budget belongs to the transaction that declared it, and `commit`
        // holds the mutation set to that one.
        assert!(validate_plan(MAX_OPTIMISTIC_MUTATIONS + 1, 1).is_ok());
        assert!(validate_plan(1, MAX_OPTIMISTIC_TRANSACTION_BYTES).is_ok());
        assert!(matches!(
            validate_plan(1, MAX_OPTIMISTIC_TRANSACTION_BYTES + 1),
            Err(MutationSetError::TransactionTooLarge { .. })
        ));
    }
}
