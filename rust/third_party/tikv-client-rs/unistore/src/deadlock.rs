//! Synchronized wait-for graph used by the in-process transaction engine.
//!
//! This is a direct transcreation of client-go's
//! `internal/mockstore/deadlock` package. Keeping the detector in the reusable
//! crate ensures every protocol adapter observes the same graph semantics.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TransactionKeyHash {
    transaction: u64,
    key_hash: u64,
}

/// The key hash associated with the edge which closes a deadlock cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadlockError {
    pub key_hash: u64,
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
    wait_for: Mutex<HashMap<u64, Vec<TransactionKeyHash>>>,
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
        let mut wait_for = self.wait_for.lock().expect("deadlock graph lock poisoned");
        Self::detect_from(&wait_for, source_transaction, wait_for_transaction)?;
        Self::register(
            &mut wait_for,
            source_transaction,
            wait_for_transaction,
            key_hash,
        );
        Ok(())
    }

    fn detect_from(
        wait_for: &HashMap<u64, Vec<TransactionKeyHash>>,
        source_transaction: u64,
        wait_for_transaction: u64,
    ) -> Result<(), DeadlockError> {
        let Some(next_transactions) = wait_for.get(&wait_for_transaction) else {
            return Ok(());
        };
        for next in next_transactions {
            if next.transaction == source_transaction {
                return Err(DeadlockError {
                    key_hash: next.key_hash,
                });
            }
            Self::detect_from(wait_for, source_transaction, next.transaction)?;
        }
        Ok(())
    }

    fn register(
        wait_for: &mut HashMap<u64, Vec<TransactionKeyHash>>,
        source_transaction: u64,
        wait_for_transaction: u64,
        key_hash: u64,
    ) {
        let edge = TransactionKeyHash {
            transaction: wait_for_transaction,
            key_hash,
        };
        let edges = wait_for.entry(source_transaction).or_default();
        if !edges.contains(&edge) {
            edges.push(edge);
        }
    }

    /// Removes every outbound wait-for edge for one transaction.
    pub fn clean_up(&self, transaction: u64) {
        self.wait_for
            .lock()
            .expect("deadlock graph lock poisoned")
            .remove(&transaction);
    }

    /// Removes the first exact wait-for edge and its now-empty transaction.
    pub fn clean_up_wait_for(&self, transaction: u64, wait_for_transaction: u64, key_hash: u64) {
        let edge = TransactionKeyHash {
            transaction: wait_for_transaction,
            key_hash,
        };
        let mut wait_for = self.wait_for.lock().expect("deadlock graph lock poisoned");
        let remove_transaction = if let Some(edges) = wait_for.get_mut(&transaction) {
            if let Some(index) = edges.iter().position(|candidate| *candidate == edge) {
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
            Err(DeadlockError { key_hash: 200 })
        );
        assert_eq!(
            detector.detect(3, 1, 300).unwrap_err().to_string(),
            "deadlock(200)"
        );

        detector.clean_up(2);
        assert_eq!(detector.edge_count(2), None);

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
            Err(DeadlockError { key_hash: 11 })
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
            Err(DeadlockError { key_hash: 99 })
        );
        assert_eq!(detector.edge_count(9), Some(1));
    }
}
