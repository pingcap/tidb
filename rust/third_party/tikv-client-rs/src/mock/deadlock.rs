// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Synchronized wait-for graph used by the mock TiKV transaction engine.

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
pub(crate) struct DeadlockError {
    pub(crate) key_hash: u64,
}

impl fmt::Display for DeadlockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deadlock({})", self.key_hash)
    }
}

impl std::error::Error for DeadlockError {}

/// Detects cycles in a transaction wait-for graph.
#[derive(Default)]
pub(crate) struct Detector {
    wait_for: Mutex<HashMap<u64, Vec<TransactionKeyHash>>>,
}

impl Detector {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Detect an edge before registering it.
    ///
    /// A rejected edge is not inserted. On a cycle, the returned key hash is
    /// taken from the existing edge which reaches `source_transaction`, exactly
    /// as in client-go.
    pub(crate) fn detect(
        &self,
        source_transaction: u64,
        wait_for_transaction: u64,
        key_hash: u64,
    ) -> Result<(), DeadlockError> {
        let mut wait_for = self.wait_for.lock().unwrap();
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

    /// Remove every outbound wait-for edge for one transaction.
    pub(crate) fn clean_up(&self, transaction: u64) {
        self.wait_for.lock().unwrap().remove(&transaction);
    }

    /// Remove the first matching wait-for edge and its now-empty transaction.
    pub(crate) fn clean_up_wait_for(
        &self,
        transaction: u64,
        wait_for_transaction: u64,
        key_hash: u64,
    ) {
        let edge = TransactionKeyHash {
            transaction: wait_for_transaction,
            key_hash,
        };
        let mut wait_for = self.wait_for.lock().unwrap();
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

    /// Remove transaction entries whose timestamp is below `minimum_ts`.
    pub(crate) fn expire(&self, minimum_ts: u64) {
        self.wait_for
            .lock()
            .unwrap()
            .retain(|transaction, _| *transaction >= minimum_ts);
    }

    #[cfg(test)]
    fn edge_count(&self, transaction: u64) -> Option<usize> {
        self.wait_for
            .lock()
            .unwrap()
            .get(&transaction)
            .map(Vec::len)
    }

    #[cfg(test)]
    fn transaction_count(&self) -> usize {
        self.wait_for.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn original_deadlock_cleanup_deduplication_and_expiry_scenario() {
        let detector = Detector::new();
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
    fn concurrent_registration_is_synchronized_and_exact_duplicates_collapse() {
        let detector = Arc::new(Detector::new());
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
    fn direct_cycles_and_the_source_self_edge_quirk_match_client_go() {
        let detector = Detector::new();
        detector.detect(1, 2, 11).unwrap();
        assert_eq!(
            detector.detect(2, 1, 22),
            Err(DeadlockError { key_hash: 11 })
        );
        assert_eq!(detector.edge_count(2), None);

        // The source checks outgoing edges before adding a new edge. Therefore
        // the first self-edge is accepted and a repeated one reports its hash.
        detector.detect(9, 9, 99).unwrap();
        assert_eq!(
            detector.detect(9, 9, 100),
            Err(DeadlockError { key_hash: 99 })
        );
        assert_eq!(detector.edge_count(9), Some(1));
    }
}
