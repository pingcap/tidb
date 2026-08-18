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

//! Go `pkg/store/mockstore/unistore/tikv/detector.go`, whole: the wait-for
//! graph that turns a cycle of blocked transactions into a deadlock error.
//!
//! Every `Detect` call walks the graph from the transaction we are waiting
//! for; reaching ourselves is the cycle. The walk expires stale edges as it
//! passes them, so an abandoned waiter never keeps a false cycle alive.
//!
//! Two Go shapes are reproduced exactly because they are observable:
//!
//! - The wait chain arrives REVERSED and with the closing edge appended, so
//!   the caller reads it as "each entry waits for the next one".
//! - `register` is skipped entirely when a deadlock is found, so a refused
//!   waiter leaves no edge behind.
//!
//! Go's `container/list` is a doubly-linked list only because it removes
//! entries mid-iteration; a `Vec` with retain-style removal has the same
//! observable order and is idiomatic here.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::lockwaiter::WaitForEntry;

/// Go `diagnosticContext`: the key and resource-group tag carried into the
/// wait chain for diagnostics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiagnosticContext {
    /// `key`.
    pub key: Vec<u8>,
    /// `resourceGroupTag`.
    pub resource_group_tag: Vec<u8>,
}

/// Go `kverrors.ErrDeadlock`, narrowed to the fields this package fills.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErrDeadlock {
    /// `DeadlockKeyHash`.
    pub deadlock_key_hash: u64,
    /// `WaitChain`, ordered so each entry waits for the next one.
    pub wait_chain: Vec<WaitForEntry>,
}

impl std::fmt::Display for ErrDeadlock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Go `func (ErrDeadlock) Error() string { return "deadlock" }`.
        formatter.write_str("deadlock")
    }
}

impl std::error::Error for ErrDeadlock {}

/// Go `txnKeyHashPair`: one wait edge.
#[derive(Clone, Debug)]
struct TxnKeyHashPair {
    txn: u64,
    key_hash: u64,
    register_time: Instant,
    diag_ctx: DiagnosticContext,
}

impl TxnKeyHashPair {
    /// Go `isExpired`: registered longer than the TTL ago.
    fn is_expired(&self, ttl: Duration, now: Instant) -> bool {
        self.register_time + ttl < now
    }

    /// The wait-chain entry this edge contributes, as seen from `txn`.
    fn wait_chain_entry(&self, txn: u64) -> WaitForEntry {
        WaitForEntry {
            txn,
            key_hash: self.key_hash,
            key: self.diag_ctx.key.clone(),
            resource_group_tag: self.diag_ctx.resource_group_tag.clone(),
            wait_for_txn: self.txn,
        }
    }
}

/// Go `Detector`'s guarded state, so the lock wraps exactly what Go's does.
#[derive(Debug, Default)]
struct DetectorState {
    wait_for_map: HashMap<u64, Vec<TxnKeyHashPair>>,
    total_size: u64,
    last_active_expire: Option<Instant>,
}

/// Go `Detector`.
#[derive(Debug)]
pub struct Detector {
    state: Mutex<DetectorState>,
    entry_ttl: Duration,
    urgent_size: u64,
    expire_interval: Duration,
}

impl Detector {
    /// Go `NewDetector`.
    #[must_use]
    pub fn new(ttl: Duration, urgent_size: u64, expire_interval: Duration) -> Self {
        Self {
            state: Mutex::new(DetectorState {
                last_active_expire: Some(Instant::now()),
                ..DetectorState::default()
            }),
            entry_ttl: ttl,
            urgent_size,
            expire_interval,
        }
    }

    /// Go `Detect`: answer a deadlock, or record the wait edge.
    ///
    /// On a deadlock the chain is reversed and the current edge appended, so
    /// the caller reads it front-to-back as a cycle; the edge is NOT
    /// registered, since the waiter is about to be refused.
    pub fn detect(
        &self,
        source_txn: u64,
        wait_for_txn: u64,
        key_hash: u64,
        diag_ctx: &DiagnosticContext,
    ) -> Option<ErrDeadlock> {
        let mut state = self.state.lock().expect("the detector lock");
        let now = Instant::now();
        self.active_expire(&mut state, now);
        match self.do_detect(&mut state, now, source_txn, wait_for_txn) {
            None => {
                self.register(
                    &mut state,
                    source_txn,
                    wait_for_txn,
                    key_hash,
                    diag_ctx,
                    now,
                );
                None
            }
            Some(mut err) => {
                err.wait_chain.reverse();
                err.wait_chain.push(WaitForEntry {
                    txn: source_txn,
                    key: diag_ctx.key.clone(),
                    key_hash,
                    resource_group_tag: diag_ctx.resource_group_tag.clone(),
                    wait_for_txn,
                });
                Some(err)
            }
        }
    }

    /// Go `doDetect`: walk the wait-for graph depth-first, expiring stale
    /// edges as it passes them. Reaching `source_txn` is the cycle.
    fn do_detect(
        &self,
        state: &mut DetectorState,
        now: Instant,
        source_txn: u64,
        wait_for_txn: u64,
    ) -> Option<ErrDeadlock> {
        // Go iterates the live list, removing expired entries in place. The
        // walk recurses, so the edges are cloned first and the removals
        // applied after — same order, same removals.
        let edges = state.wait_for_map.get(&wait_for_txn)?.clone();
        let mut expired = Vec::new();
        for edge in &edges {
            if edge.is_expired(self.entry_ttl, now) {
                expired.push((edge.txn, edge.key_hash));
                continue;
            }
            if edge.txn == source_txn {
                self.remove_edges(state, wait_for_txn, &expired);
                return Some(ErrDeadlock {
                    deadlock_key_hash: edge.key_hash,
                    wait_chain: vec![edge.wait_chain_entry(wait_for_txn)],
                });
            }
            self.remove_edges(state, wait_for_txn, &expired);
            expired.clear();
            if let Some(mut err) = self.do_detect(state, now, source_txn, edge.txn) {
                err.wait_chain.push(edge.wait_chain_entry(wait_for_txn));
                return Some(err);
            }
        }
        self.remove_edges(state, wait_for_txn, &expired);
        if state
            .wait_for_map
            .get(&wait_for_txn)
            .is_some_and(Vec::is_empty)
        {
            state.wait_for_map.remove(&wait_for_txn);
        }
        None
    }

    /// Drops the named edges of one transaction, keeping `total_size` in
    /// step — Go's mid-iteration `list.Remove` plus `d.totalSize--`.
    fn remove_edges(&self, state: &mut DetectorState, txn: u64, dropped: &[(u64, u64)]) {
        if dropped.is_empty() {
            return;
        }
        if let Some(edges) = state.wait_for_map.get_mut(&txn) {
            let before = edges.len();
            edges.retain(|edge| !dropped.contains(&(edge.txn, edge.key_hash)));
            state.total_size -= (before - edges.len()) as u64;
        }
    }

    /// Go `register`: append the edge unless the identical
    /// `(waitForTxn, keyHash)` pair is already recorded.
    fn register(
        &self,
        state: &mut DetectorState,
        source_txn: u64,
        wait_for_txn: u64,
        key_hash: u64,
        diag_ctx: &DiagnosticContext,
        now: Instant,
    ) {
        let pair = TxnKeyHashPair {
            txn: wait_for_txn,
            key_hash,
            register_time: now,
            diag_ctx: diag_ctx.clone(),
        };
        let edges = state.wait_for_map.entry(source_txn).or_default();
        if edges
            .iter()
            .any(|edge| edge.txn == wait_for_txn && edge.key_hash == key_hash)
        {
            return;
        }
        edges.push(pair);
        state.total_size += 1;
    }

    /// Go `CleanUp`: drop every edge of one transaction.
    pub fn clean_up(&self, txn: u64) {
        let mut state = self.state.lock().expect("the detector lock");
        if let Some(edges) = state.wait_for_map.remove(&txn) {
            state.total_size -= edges.len() as u64;
        }
    }

    /// Go `CleanUpWaitFor`: drop ONE edge, and the transaction's entry with
    /// it when that was its last.
    pub fn clean_up_wait_for(&self, txn: u64, wait_for_txn: u64, key_hash: u64) {
        let mut state = self.state.lock().expect("the detector lock");
        let Some(edges) = state.wait_for_map.get_mut(&txn) else {
            return;
        };
        if let Some(index) = edges
            .iter()
            .position(|edge| edge.txn == wait_for_txn && edge.key_hash == key_hash)
        {
            edges.remove(index);
            state.total_size -= 1;
        }
        if state.wait_for_map.get(&txn).is_some_and(Vec::is_empty) {
            state.wait_for_map.remove(&txn);
        }
    }

    /// Go `activeExpire`: a periodic sweep, run only once the graph has
    /// grown past `urgentSize` AND the interval has passed.
    fn active_expire(&self, state: &mut DetectorState, now: Instant) {
        let due = state
            .last_active_expire
            .is_some_and(|last| now.duration_since(last) > self.expire_interval);
        if !(due && state.total_size >= self.urgent_size) {
            return;
        }
        let mut removed = 0_u64;
        state.wait_for_map.retain(|_, edges| {
            let before = edges.len();
            edges.retain(|edge| !edge.is_expired(self.entry_ttl, now));
            removed += (before - edges.len()) as u64;
            !edges.is_empty()
        });
        state.total_size -= removed;
        state.last_active_expire = Some(now);
    }

    /// Go's test reads `detector.totalSize` directly; the field is guarded
    /// here, so the reads go through this.
    #[must_use]
    pub fn total_size(&self) -> u64 {
        self.state.lock().expect("the detector lock").total_size
    }

    /// Go's test reads `detector.waitForMap[txn]`; `None` is Go's nil list.
    #[must_use]
    pub fn wait_for_len(&self, txn: u64) -> Option<usize> {
        self.state
            .lock()
            .expect("the detector lock")
            .wait_for_map
            .get(&txn)
            .map(Vec::len)
    }

    /// Go's test reads `len(detector.waitForMap)`.
    #[must_use]
    pub fn tracked_txns(&self) -> usize {
        self.state
            .lock()
            .expect("the detector lock")
            .wait_for_map
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::{Detector, DiagnosticContext};
    use crate::lockwaiter::WaitForEntry;
    use std::time::Duration;

    fn diag(key: &str, tag: &str) -> DiagnosticContext {
        DiagnosticContext {
            key: key.as_bytes().to_vec(),
            resource_group_tag: tag.as_bytes().to_vec(),
        }
    }

    fn check_wait_chain_entry(
        entry: &WaitForEntry,
        txn: u64,
        wait_for_txn: u64,
        key: &str,
        tag: &str,
    ) {
        assert_eq!(entry.txn, txn);
        assert_eq!(entry.wait_for_txn, wait_for_txn);
        assert_eq!(entry.key, key.as_bytes());
        assert_eq!(entry.resource_group_tag, tag.as_bytes());
    }

    /// Go `TestDeadlock` (`detector_test.go:38`), whole.
    #[test]
    fn test_deadlock() {
        let ttl = Duration::from_millis(50);
        let expire_interval = Duration::from_millis(100);
        let urgent_size = 1_u64;
        let detector = Detector::new(ttl, urgent_size, expire_interval);

        assert!(detector.detect(1, 2, 100, &diag("k1", "tag1")).is_none());
        assert_eq!(detector.total_size(), 1);
        assert!(detector.detect(2, 3, 200, &diag("k2", "tag2")).is_none());
        assert_eq!(detector.total_size(), 2);

        let result = detector
            .detect(3, 1, 300, &diag("k3", "tag3"))
            .expect("the cycle closes");
        assert_eq!(result.to_string(), "deadlock");
        assert_eq!(result.wait_chain.len(), 3);
        // Each entry waits for the next one.
        check_wait_chain_entry(&result.wait_chain[0], 1, 2, "k1", "tag1");
        check_wait_chain_entry(&result.wait_chain[1], 2, 3, "k2", "tag2");
        check_wait_chain_entry(&result.wait_chain[2], 3, 1, "k3", "tag3");
        // The refused waiter registered nothing.
        assert_eq!(detector.total_size(), 2);

        detector.clean_up(2);
        assert_eq!(detector.wait_for_len(2), None);
        assert_eq!(detector.total_size(), 1);

        // With the cycle broken there is no deadlock.
        let empty = DiagnosticContext::default();
        assert!(detector.detect(3, 1, 300, &empty).is_none());
        assert_eq!(detector.wait_for_len(3), Some(1));
        assert_eq!(detector.total_size(), 2);

        // A different key hash grows the list.
        assert!(detector.detect(3, 1, 400, &empty).is_none());
        assert_eq!(detector.wait_for_len(3), Some(2));
        assert_eq!(detector.total_size(), 3);

        // The same wait-for and key hash does not.
        assert!(detector.detect(3, 1, 400, &empty).is_none());
        assert_eq!(detector.wait_for_len(3), Some(2));
        assert_eq!(detector.total_size(), 3);

        detector.clean_up_wait_for(3, 1, 300);
        assert_eq!(detector.wait_for_len(3), Some(1));
        assert_eq!(detector.total_size(), 2);
        detector.clean_up_wait_for(3, 1, 400);
        assert_eq!(detector.total_size(), 1);
        assert_eq!(detector.wait_for_len(3), None);

        // After the TTL every entry is expired, so a detect over unrelated
        // transactions finds no edge.
        std::thread::sleep(Duration::from_millis(100));
        assert!(detector.detect(100, 200, 100, &empty).is_none());
        assert_eq!(detector.total_size(), 1);
        assert_eq!(detector.tracked_txns(), 1);

        // An expired edge never reports a deadlock: the walk removes it,
        // and this does not wait for the active-expire interval.
        std::thread::sleep(Duration::from_millis(60));
        assert!(detector.detect(200, 100, 200, &empty).is_none());
        assert_eq!(detector.total_size(), 1);
        assert_eq!(detector.tracked_txns(), 1);
    }
}
