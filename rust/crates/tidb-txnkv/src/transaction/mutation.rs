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

use std::collections::BTreeSet;

use tidb_proto::{KvrpcAssertion, KvrpcMutation, KvrpcOp};

/// One normal optimistic mutation admitted by the concrete TiKV coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimisticMutationKind {
    /// Create a value and fail if the key already exists at `start_ts`.
    Insert,
    /// Replace a value and fail if the key does not exist at `start_ts`.
    PutExisting,
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

    pub(super) fn to_proto(&self) -> KvrpcMutation {
        let (op, assertion) = match self.kind {
            OptimisticMutationKind::Insert => (KvrpcOp::Insert, KvrpcAssertion::NotExist),
            OptimisticMutationKind::PutExisting => (KvrpcOp::Put, KvrpcAssertion::Exist),
        };
        KvrpcMutation {
            op: op as i32,
            key: self.key.clone(),
            value: self.value.clone(),
            assertion: assertion as i32,
        }
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
    /// A transaction has exactly one immutable mutation per encoded key.
    DuplicateKey(Vec<u8>),
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
            Self::DuplicateKey(_) => {
                formatter.write_str("optimistic transaction contains a duplicate encoded key")
            }
        }
    }
}

impl std::error::Error for MutationSetError {}

pub(super) fn validate_and_sort(
    mutations: Vec<OptimisticMutation>,
) -> Result<Vec<OptimisticMutation>, MutationSetError> {
    if mutations.is_empty() {
        return Err(MutationSetError::Empty);
    }
    validate_plan(mutations.len(), checked_aggregate_bytes(&mutations))?;
    let mut sorted = mutations;
    sorted.sort_by(|left, right| left.key.cmp(&right.key));
    let mut keys = BTreeSet::new();
    for mutation in &sorted {
        if !keys.insert(mutation.key.clone()) {
            return Err(MutationSetError::DuplicateKey(mutation.key.clone()));
        }
    }
    Ok(sorted)
}

fn checked_aggregate_bytes(mutations: &[OptimisticMutation]) -> usize {
    checked_aggregate_sizes(
        mutations
            .iter()
            .map(|mutation| (mutation.key.len(), mutation.value.len())),
    )
}

fn checked_aggregate_sizes(sizes: impl IntoIterator<Item = (usize, usize)>) -> usize {
    sizes
        .into_iter()
        .try_fold(0usize, |size, (key, value)| {
            size.checked_add(key)?.checked_add(value)
        })
        .unwrap_or(usize::MAX)
}

/// Maximum mutations admitted by the first bounded normal-2PC path.
pub const MAX_OPTIMISTIC_MUTATIONS: usize = 256;
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
    if count > MAX_OPTIMISTIC_MUTATIONS {
        return Err(MutationSetError::TooManyMutations {
            count,
            limit: MAX_OPTIMISTIC_MUTATIONS,
        });
    }
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
    fn deterministic_primary_is_the_smallest_unique_nonempty_key() {
        let sorted = validate_and_sort(vec![
            OptimisticMutation::put_existing(b"z".to_vec(), b"3".to_vec()).unwrap(),
            OptimisticMutation::insert(b"a".to_vec(), b"1".to_vec()).unwrap(),
            OptimisticMutation::put_existing(b"m".to_vec(), b"2".to_vec()).unwrap(),
        ])
        .unwrap();
        assert_eq!(sorted[0].key(), b"a");
        assert_eq!(sorted[1].key(), b"m");
        assert_eq!(sorted[2].key(), b"z");
        assert_eq!(sorted[0].to_proto().op, KvrpcOp::Insert as i32);
        assert_eq!(
            sorted[0].to_proto().assertion,
            KvrpcAssertion::NotExist as i32
        );
    }

    #[test]
    fn duplicate_or_empty_keys_fail_before_storage() {
        assert_eq!(validate_and_sort(Vec::new()), Err(MutationSetError::Empty));
        assert_eq!(
            OptimisticMutation::insert(Vec::new(), b"v".to_vec()),
            Err(MutationSetError::EmptyKey)
        );
        assert_eq!(
            validate_and_sort(vec![
                OptimisticMutation::insert(b"k".to_vec(), b"1".to_vec()).unwrap(),
                OptimisticMutation::put_existing(b"k".to_vec(), b"2".to_vec()).unwrap(),
            ]),
            Err(MutationSetError::DuplicateKey(b"k".to_vec()))
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
        assert!(matches!(
            validate_plan(MAX_OPTIMISTIC_MUTATIONS + 1, 1),
            Err(MutationSetError::TooManyMutations { .. })
        ));
        assert!(validate_plan(1, MAX_OPTIMISTIC_TRANSACTION_BYTES).is_ok());
        assert!(matches!(
            validate_plan(1, MAX_OPTIMISTIC_TRANSACTION_BYTES + 1),
            Err(MutationSetError::TransactionTooLarge { .. })
        ));

        assert_eq!(
            checked_aggregate_sizes([(usize::MAX, 0), (1, 0)]),
            usize::MAX
        );
    }

    #[test]
    fn put_existing_is_not_an_unasserted_put() {
        let mutation = OptimisticMutation::put_existing(b"k".to_vec(), b"v".to_vec())
            .unwrap()
            .to_proto();
        assert_eq!(mutation.op, KvrpcOp::Put as i32);
        assert_eq!(mutation.assertion, KvrpcAssertion::Exist as i32);
    }
}
