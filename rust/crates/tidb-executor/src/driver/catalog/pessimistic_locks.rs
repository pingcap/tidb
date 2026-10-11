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

//! The in-process store's pessimistic locks: TiKV's lock column family and
//! waiter manager for the catalog every session of one store shares.
//!
//! Go's pessimistic statement locks the keys it read for writing
//! (`pkg/executor/adapter.go` `handlePessimisticDML`, `SelectLockExec`)
//! through `KVTxn.LockKeys`; TiKV answers a key another live transaction
//! holds by queueing the request until that transaction ends, failing a
//! `NOWAIT` request at once (`ErrLockAcquireFailAndNoWaitSet`, 3572) and a
//! waiting one after `innodb_lock_wait_timeout` (`ErrLockWaitTimeout`,
//! 1205). A request takes all of its keys or none of them. The owner releases
//! every key when its transaction ends, by commit or rollback.
//!
//! NOT MODELLED (documented): TiKV's deadlock detector -- a cycle of waiters
//! here ends by the lock-wait timeout rather than 1213 -- and the lock a
//! COMMIT or an autocommit statement meets: Go makes such a writer wait for
//! the holder, while this store's writers that take no locks of their own
//! write through.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

/// Every pessimistic lock live in one store, by encoded key.
#[derive(Debug, Default)]
pub struct PessimisticLocks {
    state: Mutex<LockState>,
    /// Signalled whenever an owner releases its keys.
    released: Condvar,
}

#[derive(Debug, Default)]
struct LockState {
    /// The owning transaction's start timestamp for each locked key.
    holders: HashMap<Vec<u8>, u64>,
    /// Each owner's keys, so a transaction's end releases them without a
    /// walk of the whole table.
    owned: HashMap<u64, Vec<Vec<u8>>>,
}

/// The outcome of one lock request that does not wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockAcquire {
    /// Every key is now held by the requester.
    Acquired,
    /// The first key another live transaction holds; nothing was taken.
    Blocked {
        /// That key's owner.
        holder: u64,
    },
}

impl PessimisticLocks {
    fn state(&self) -> MutexGuard<'_, LockState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes every one of `keys` for `owner`, or none when another owner
    /// holds one of them. A key `owner` already holds is held again at no
    /// cost.
    pub fn try_acquire(&self, owner: u64, keys: &[Vec<u8>]) -> LockAcquire {
        let mut state = self.state();
        for key in keys {
            if let Some(&holder) = state.holders.get(key) {
                if holder != owner {
                    return LockAcquire::Blocked { holder };
                }
            }
        }
        let LockState { holders, owned } = &mut *state;
        for key in keys {
            if holders.insert(key.clone(), owner).is_none() {
                owned.entry(owner).or_default().push(key.clone());
            }
        }
        LockAcquire::Acquired
    }

    /// Waits until `holder` no longer holds any key, or until `deadline`.
    /// Returns whether the holder released them in time; `None` waits as
    /// long as it takes.
    pub fn wait_released(&self, holder: u64, deadline: Option<Instant>) -> bool {
        let mut state = self.state();
        while state.owned.contains_key(&holder) {
            match deadline {
                None => {
                    state = self
                        .released
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return false;
                    }
                    state = self
                        .released
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            }
        }
        true
    }

    /// Whether `owner` holds `key`.
    #[must_use]
    pub fn holds(&self, owner: u64, key: &[u8]) -> bool {
        self.state().holders.get(key) == Some(&owner)
    }

    /// Whether `owner` holds any key.
    #[must_use]
    pub fn holds_any(&self, owner: u64) -> bool {
        self.state().owned.contains_key(&owner)
    }

    /// Releases every key `owner` holds and wakes the waiters.
    pub fn release_all(&self, owner: u64) {
        let mut state = self.state();
        let LockState { holders, owned } = &mut *state;
        let Some(keys) = owned.remove(&owner) else {
            return;
        };
        for key in keys {
            holders.remove(&key);
        }
        drop(state);
        self.released.notify_all();
    }
}

/// One transaction's claim on [`PessimisticLocks`]: dropping it releases the
/// transaction's keys, so every way a transaction ends -- COMMIT, ROLLBACK,
/// an implicit commit, a closed connection -- frees them.
#[derive(Debug)]
pub struct PessimisticLockGuard {
    locks: Arc<PessimisticLocks>,
    owner: u64,
}

impl PessimisticLockGuard {
    /// Claims `locks` for the transaction that started at `owner`.
    #[must_use]
    pub fn new(locks: Arc<PessimisticLocks>, owner: u64) -> Self {
        Self { locks, owner }
    }

    /// The owning transaction's start timestamp.
    #[must_use]
    pub fn owner(&self) -> u64 {
        self.owner
    }

    /// The lock table this claim is on.
    #[must_use]
    pub fn locks(&self) -> &Arc<PessimisticLocks> {
        &self.locks
    }
}

impl Drop for PessimisticLockGuard {
    fn drop(&mut self) {
        self.locks.release_all(self.owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_takes_all_keys_or_none_and_release_wakes_the_queue() {
        let locks = Arc::new(PessimisticLocks::default());
        let first = PessimisticLockGuard::new(Arc::clone(&locks), 1);
        assert_eq!(
            locks.try_acquire(1, &[b"a".to_vec(), b"b".to_vec()]),
            LockAcquire::Acquired
        );
        assert_eq!(
            locks.try_acquire(1, &[b"a".to_vec()]),
            LockAcquire::Acquired
        );
        assert_eq!(
            locks.try_acquire(2, &[b"c".to_vec(), b"b".to_vec()]),
            LockAcquire::Blocked { holder: 1 }
        );
        assert!(!locks.holds(2, b"c"), "a blocked request takes nothing");
        assert!(!locks.wait_released(1, Some(Instant::now())));

        let waiter = {
            let locks = Arc::clone(&locks);
            std::thread::spawn(move || locks.wait_released(1, None))
        };
        drop(first);
        assert!(waiter.join().unwrap());
        assert_eq!(
            locks.try_acquire(2, &[b"c".to_vec(), b"b".to_vec()]),
            LockAcquire::Acquired
        );
    }
}
