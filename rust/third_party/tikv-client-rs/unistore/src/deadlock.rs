//! Synchronized wait-for graph used by the in-process transaction engine.
//!
//! This is a direct transcreation of client-go's
//! `internal/mockstore/deadlock` package. Keeping the detector in the reusable
//! crate ensures every protocol adapter observes the same graph semantics.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Mutex;

/// One edge in the transaction wait-for graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitForEntry {
    pub transaction: u64,
    pub wait_for_transaction: u64,
    pub key_hash: u64,
    pub key: Vec<u8>,
    pub resource_group_tag: Vec<u8>,
    pub wait_time: u64,
}

/// The key hash associated with the edge which closes a deadlock cycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlockError {
    pub key_hash: u64,
    pub wait_chain: Vec<WaitForEntry>,
}

impl fmt::Display for DeadlockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deadlock({})", self.key_hash)
    }
}

impl std::error::Error for DeadlockError {}

/// Detects cycles in a transaction wait-for graph.
#[derive(Default)]
pub struct DeadlockDetector {
    wait_for: Mutex<HashMap<u64, Vec<WaitForEntry>>>,
    deadlocked: Mutex<HashSet<u64>>,
}

impl DeadlockDetector {
    /// Creates an empty detector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Detects and, when accepted, registers one wait-for edge.
    ///
    /// Exact `(wait_for_transaction, key_hash)` duplicates collapse. Different
    /// key hashes remain distinct, matching client-go's transaction list.
    pub fn detect(
        &self,
        source_transaction: u64,
        wait_for_transaction: u64,
        key_hash: u64,
    ) -> Result<(), DeadlockError> {
        self.detect_with_context(
            source_transaction,
            wait_for_transaction,
            key_hash,
            Vec::new(),
            Vec::new(),
        )
    }

    /// Detects and registers an edge while retaining the metadata TiKV reports
    /// in a deadlock wait chain.
    pub fn detect_with_context(
        &self,
        source_transaction: u64,
        wait_for_transaction: u64,
        key_hash: u64,
        key: Vec<u8>,
        resource_group_tag: Vec<u8>,
    ) -> Result<(), DeadlockError> {
        let mut wait_for = self.wait_for.lock().expect("deadlock graph lock poisoned");
        let edge = WaitForEntry {
            transaction: source_transaction,
            wait_for_transaction,
            key_hash,
            key,
            resource_group_tag,
            wait_time: 0,
        };
        if let Some((deadlock_key_hash, mut wait_chain)) = Self::detect_from(
            &wait_for,
            source_transaction,
            wait_for_transaction,
            &mut HashSet::new(),
        ) {
            wait_chain.push(edge);
            let mut deadlocked = self
                .deadlocked
                .lock()
                .expect("deadlocked transaction set poisoned");
            deadlocked.extend(wait_chain.iter().map(|entry| entry.transaction));
            return Err(DeadlockError {
                key_hash: deadlock_key_hash,
                wait_chain,
            });
        }
        Self::register(&mut wait_for, edge);
        Ok(())
    }

    fn detect_from(
        wait_for: &HashMap<u64, Vec<WaitForEntry>>,
        source_transaction: u64,
        wait_for_transaction: u64,
        visited: &mut HashSet<u64>,
    ) -> Option<(u64, Vec<WaitForEntry>)> {
        if !visited.insert(wait_for_transaction) {
            return None;
        }
        let next_transactions = wait_for.get(&wait_for_transaction)?;
        for next in next_transactions {
            if next.wait_for_transaction == source_transaction {
                return Some((next.key_hash, vec![next.clone()]));
            }
            if let Some((key_hash, mut path)) = Self::detect_from(
                wait_for,
                source_transaction,
                next.wait_for_transaction,
                visited,
            ) {
                path.insert(0, next.clone());
                return Some((key_hash, path));
            }
        }
        None
    }

    fn register(wait_for: &mut HashMap<u64, Vec<WaitForEntry>>, edge: WaitForEntry) {
        let edges = wait_for.entry(edge.transaction).or_default();
        if !edges.iter().any(|candidate| {
            candidate.wait_for_transaction == edge.wait_for_transaction
                && candidate.key_hash == edge.key_hash
        }) {
            edges.push(edge);
        }
    }

    /// Removes every outbound wait-for edge for one transaction.
    pub fn clean_up(&self, transaction: u64) {
        self.wait_for
            .lock()
            .expect("deadlock graph lock poisoned")
            .remove(&transaction);
        self.deadlocked
            .lock()
            .expect("deadlocked transaction set poisoned")
            .remove(&transaction);
    }

    /// Reports whether a transaction participated in a detected cycle. A
    /// waiter in that cycle must return its original conflict after wakeup
    /// instead of silently acquiring the released key.
    pub fn was_deadlocked(&self, transaction: u64) -> bool {
        self.deadlocked
            .lock()
            .expect("deadlocked transaction set poisoned")
            .contains(&transaction)
    }

    /// Removes the first exact wait-for edge and its now-empty transaction.
    pub fn clean_up_wait_for(&self, transaction: u64, wait_for_transaction: u64, key_hash: u64) {
        let mut wait_for = self.wait_for.lock().expect("deadlock graph lock poisoned");
        let remove_transaction = if let Some(edges) = wait_for.get_mut(&transaction) {
            if let Some(index) = edges.iter().position(|candidate| {
                candidate.wait_for_transaction == wait_for_transaction
                    && candidate.key_hash == key_hash
            }) {
                edges.remove(index);
            }
            edges.is_empty()
        } else {
            false
        };
        if remove_transaction {
            wait_for.remove(&transaction);
        }
    }

    /// Removes transaction entries whose timestamp is strictly below
    /// `minimum_ts`.
    pub fn expire(&self, minimum_ts: u64) {
        self.wait_for
            .lock()
            .expect("deadlock graph lock poisoned")
            .retain(|transaction, _| *transaction >= minimum_ts);
        self.deadlocked
            .lock()
            .expect("deadlocked transaction set poisoned")
            .retain(|transaction| *transaction >= minimum_ts);
    }

    #[cfg(test)]
    fn edge_count(&self, transaction: u64) -> Option<usize> {
        self.wait_for
            .lock()
            .expect("deadlock graph lock poisoned")
            .get(&transaction)
            .map(Vec::len)
    }

    #[cfg(test)]
    fn transaction_count(&self) -> usize {
        self.wait_for
            .lock()
            .expect("deadlock graph lock poisoned")
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn source_test_deadlock() {
        let detector = DeadlockDetector::new();
        assert_eq!(detector.detect(1, 2, 100), Ok(()));
        assert_eq!(detector.detect(2, 3, 200), Ok(()));
        assert_eq!(
            detector.detect(3, 1, 300),
            Err(DeadlockError {
                key_hash: 200,
                wait_chain: vec![
                    WaitForEntry {
                        transaction: 1,
                        wait_for_transaction: 2,
                        key_hash: 100,
                        key: Vec::new(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    },
                    WaitForEntry {
                        transaction: 2,
                        wait_for_transaction: 3,
                        key_hash: 200,
                        key: Vec::new(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    },
                    WaitForEntry {
                        transaction: 3,
                        wait_for_transaction: 1,
                        key_hash: 300,
                        key: Vec::new(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    },
                ],
            })
        );
        assert_eq!(
            detector.detect(3, 1, 300).unwrap_err().to_string(),
            "deadlock(200)"
        );
        assert!(detector.was_deadlocked(1));
        assert!(detector.was_deadlocked(2));
        assert!(detector.was_deadlocked(3));

        detector.clean_up(2);
        assert_eq!(detector.edge_count(2), None);
        assert!(!detector.was_deadlocked(2));

        assert_eq!(detector.detect(3, 1, 300), Ok(()));
        assert_eq!(detector.edge_count(3), Some(1));
        assert_eq!(detector.detect(3, 1, 400), Ok(()));
        assert_eq!(detector.edge_count(3), Some(2));
        assert_eq!(detector.detect(3, 1, 400), Ok(()));
        assert_eq!(detector.edge_count(3), Some(2));

        detector.clean_up_wait_for(3, 1, 300);
        assert_eq!(detector.edge_count(3), Some(1));
        detector.clean_up_wait_for(3, 1, 400);
        assert_eq!(detector.edge_count(3), None);
        detector.expire(1);
        assert_eq!(detector.transaction_count(), 1);
        detector.expire(2);
        assert_eq!(detector.transaction_count(), 0);
    }

    #[test]
    fn multiple_wait_edges_are_retained_in_registration_order() {
        let detector = DeadlockDetector::new();
        detector.detect(1, 2, 11).unwrap();
        detector.detect(1, 3, 12).unwrap();

        assert_eq!(detector.edge_count(1), Some(2));
        assert_eq!(
            detector.detect(2, 1, 21),
            Err(DeadlockError {
                key_hash: 11,
                wait_chain: vec![
                    WaitForEntry {
                        transaction: 1,
                        wait_for_transaction: 2,
                        key_hash: 11,
                        key: Vec::new(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    },
                    WaitForEntry {
                        transaction: 2,
                        wait_for_transaction: 1,
                        key_hash: 21,
                        key: Vec::new(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    },
                ],
            })
        );
        assert_eq!(detector.edge_count(2), None);
    }

    #[test]
    fn concurrent_registration_is_synchronized_and_exact_duplicates_collapse() {
        let detector = Arc::new(DeadlockDetector::new());
        let threads = (0..8)
            .map(|_| {
                let detector = detector.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        detector.detect(10, 20, 30).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(detector.edge_count(10), Some(1));

        detector.clean_up_wait_for(99, 100, 101);
        detector.clean_up(99);
        detector.expire(0);
        assert_eq!(detector.edge_count(10), Some(1));
    }

    #[test]
    fn first_self_edge_is_accepted_and_the_second_reports_its_hash() {
        let detector = DeadlockDetector::new();
        detector.detect(9, 9, 99).unwrap();
        assert_eq!(
            detector.detect(9, 9, 100),
            Err(DeadlockError {
                key_hash: 99,
                wait_chain: vec![
                    WaitForEntry {
                        transaction: 9,
                        wait_for_transaction: 9,
                        key_hash: 99,
                        key: Vec::new(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    },
                    WaitForEntry {
                        transaction: 9,
                        wait_for_transaction: 9,
                        key_hash: 100,
                        key: Vec::new(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    },
                ],
            })
        );
        assert_eq!(detector.edge_count(9), Some(1));
    }
}
