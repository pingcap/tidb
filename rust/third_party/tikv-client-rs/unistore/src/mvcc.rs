//! In-memory versioned-key core shared by UniStore consumers.
//!
//! This native convenience facade exposes committed-version visibility for
//! consumers that do not need the complete mock TiKV lock/prewrite protocol.
//! The source-mapped protocol-independent engine is `MockEngine` in this crate.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use thiserror::Error;

pub type Timestamp = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Mutation {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedValue {
    pub start_ts: Timestamp,
    pub commit_ts: Timestamp,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MvccError {
    #[error("commit timestamp {commit_ts} must be greater than start timestamp {start_ts}")]
    InvalidCommitTimestamp {
        start_ts: Timestamp,
        commit_ts: Timestamp,
    },
    #[error("write conflict on key {key:?}: latest commit {conflicting_commit_ts} exceeds start timestamp {start_ts}")]
    WriteConflict {
        key: Vec<u8>,
        start_ts: Timestamp,
        conflicting_commit_ts: Timestamp,
    },
}

/// Thread-safe committed-version store.
///
/// Version vectors are maintained in commit order, which makes snapshot reads
/// deterministic and allows the crate to be used by independent client test
/// modules without borrowing a transaction-local buffer.
#[derive(Clone, Default)]
pub struct MvccStore {
    versions: Arc<RwLock<BTreeMap<Vec<u8>, Vec<VersionedValue>>>>,
}

impl MvccStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically commits mutations that began at `start_ts`.
    ///
    /// The complete lock/prewrite protocol is available through `MockEngine`;
    /// this smaller API preserves its committed-version conflict boundary.
    pub fn commit(
        &self,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<(), MvccError> {
        if commit_ts <= start_ts {
            return Err(MvccError::InvalidCommitTimestamp {
                start_ts,
                commit_ts,
            });
        }
        let mutations: Vec<_> = mutations.into_iter().collect();
        let mut versions = self.versions.write().expect("MVCC store lock poisoned");
        for mutation in &mutations {
            if let Some(conflicting_commit_ts) = versions
                .get(mutation.key())
                .and_then(|entries| entries.last())
                .map(|entry| entry.commit_ts)
                .filter(|&timestamp| timestamp > start_ts)
            {
                return Err(MvccError::WriteConflict {
                    key: mutation.key().to_vec(),
                    start_ts,
                    conflicting_commit_ts,
                });
            }
        }
        for mutation in mutations {
            let (key, value) = match mutation {
                Mutation::Put { key, value } => (key, Some(value)),
                Mutation::Delete { key } => (key, None),
            };
            versions.entry(key).or_default().push(VersionedValue {
                start_ts,
                commit_ts,
                value,
            });
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8], read_ts: Timestamp) -> Option<Vec<u8>> {
        self.versions
            .read()
            .expect("MVCC store lock poisoned")
            .get(key)
            .and_then(|entries| {
                entries
                    .iter()
                    .rev()
                    .find(|entry| entry.commit_ts <= read_ts)
            })
            .and_then(|entry| entry.value.clone())
    }

    pub fn scan(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.versions
            .read()
            .expect("MVCC store lock poisoned")
            .iter()
            .filter(|(key, _)| {
                lower.is_none_or(|bound| key.as_slice() >= bound)
                    && upper.is_none_or(|bound| key.as_slice() < bound)
            })
            .filter_map(|(key, entries)| {
                entries
                    .iter()
                    .rev()
                    .find(|entry| entry.commit_ts <= read_ts)
                    .and_then(|entry| entry.value.clone())
                    .map(|value| (key.clone(), value))
            })
            .collect()
    }

    pub fn versions(&self, key: &[u8]) -> Vec<VersionedValue> {
        self.versions
            .read()
            .expect("MVCC store lock poisoned")
            .get(key)
            .cloned()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_versions_are_visible_at_the_correct_snapshot_and_tombstones_scan_out() {
        let store = MvccStore::new();
        store
            .commit(
                1,
                2,
                [Mutation::Put {
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                }],
            )
            .unwrap();
        store
            .commit(
                3,
                4,
                [
                    Mutation::Put {
                        key: b"a".to_vec(),
                        value: b"two".to_vec(),
                    },
                    Mutation::Put {
                        key: b"b".to_vec(),
                        value: b"value".to_vec(),
                    },
                ],
            )
            .unwrap();
        store
            .commit(5, 6, [Mutation::Delete { key: b"b".to_vec() }])
            .unwrap();

        assert_eq!(store.get(b"a", 2), Some(b"one".to_vec()));
        assert_eq!(store.get(b"a", 4), Some(b"two".to_vec()));
        assert_eq!(store.get(b"b", 6), None);
        assert_eq!(
            store.scan(None, None, 6),
            vec![(b"a".to_vec(), b"two".to_vec())]
        );
    }

    #[test]
    fn commits_validate_timestamps_and_write_conflicts_before_mutation() {
        let store = MvccStore::new();
        assert_eq!(
            store.commit(2, 2, Vec::new()),
            Err(MvccError::InvalidCommitTimestamp {
                start_ts: 2,
                commit_ts: 2,
            })
        );
        store
            .commit(
                1,
                5,
                [Mutation::Put {
                    key: b"a".to_vec(),
                    value: b"value".to_vec(),
                }],
            )
            .unwrap();
        assert_eq!(
            store.commit(
                3,
                6,
                [Mutation::Put {
                    key: b"a".to_vec(),
                    value: b"other".to_vec(),
                }],
            ),
            Err(MvccError::WriteConflict {
                key: b"a".to_vec(),
                start_ts: 3,
                conflicting_commit_ts: 5,
            })
        );
        assert_eq!(store.get(b"a", 6), Some(b"value".to_vec()));
    }
}
