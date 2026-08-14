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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// A backend failure while acquiring or inspecting an advisory lock.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdvisoryLockError {
    /// The requested wait budget elapsed while another session held the lock.
    Timeout,
    /// The storage lock manager detected a deadlock.
    Deadlock,
    /// The lock backend could not complete the operation.
    Internal(String),
}

impl fmt::Display for AdvisoryLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("advisory lock wait timed out"),
            Self::Deadlock => formatter.write_str("advisory lock deadlock"),
            Self::Internal(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for AdvisoryLockError {}

/// One acquired backend lock. Dropping it must release the physical lock.
pub trait AdvisoryLockLease {
    /// Releases the physical lock now.
    fn release(self: Box<Self>);
}

/// The shared authority that arbitrates lock names between sessions.
pub trait AdvisoryLockService: Send + Sync {
    /// Acquires `name`, waiting at most `timeout`.
    fn acquire(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError>;

    /// Returns whether some session currently owns `name`. Backends report an
    /// inspection failure as used, matching Go's `IsUsedAdvisoryLock`.
    fn is_used(&self, name: &str) -> bool;
}

#[derive(Debug, Default)]
struct LocalAdvisoryLocks {
    names: Mutex<HashSet<String>>,
    changed: Condvar,
}

/// Process-local backend used by the in-memory pipeline engine.
#[derive(Clone, Debug, Default)]
pub struct LocalAdvisoryLockService(Arc<LocalAdvisoryLocks>);

struct LocalAdvisoryLockLease {
    service: Arc<LocalAdvisoryLocks>,
    name: String,
}

impl Drop for LocalAdvisoryLockLease {
    fn drop(&mut self) {
        let mut names = self
            .service
            .names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if names.remove(&self.name) {
            self.service.changed.notify_all();
        }
    }
}

impl AdvisoryLockLease for LocalAdvisoryLockLease {
    fn release(self: Box<Self>) {
        drop(self);
    }
}

impl AdvisoryLockService for LocalAdvisoryLockService {
    fn acquire(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError> {
        let started = Instant::now();
        let mut names = self
            .0
            .names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while names.contains(name) {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(AdvisoryLockError::Timeout);
            }
            let (next, wait) = self
                .0
                .changed
                .wait_timeout(names, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            names = next;
            if wait.timed_out() && names.contains(name) {
                return Err(AdvisoryLockError::Timeout);
            }
        }
        names.insert(name.to_owned());
        Ok(Box::new(LocalAdvisoryLockLease {
            service: Arc::clone(&self.0),
            name: name.to_owned(),
        }))
    }

    fn is_used(&self, name: &str) -> bool {
        self.0
            .names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(name)
    }
}

struct HeldLock {
    references: usize,
    owner: u64,
    lease: Box<dyn AdvisoryLockLease>,
}

struct AdvisoryLockSessionState {
    service: Arc<dyn AdvisoryLockService>,
    owner: u64,
    held: HashMap<String, HeldLock>,
}

/// One SQL session's advisory-lock ownership and reference counts.
#[derive(Clone)]
pub struct AdvisoryLockSession(Rc<RefCell<AdvisoryLockSessionState>>);

impl fmt::Debug for AdvisoryLockSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.0.borrow();
        formatter
            .debug_struct("AdvisoryLockSession")
            .field("owner", &state.owner)
            .field("held", &state.held.len())
            .finish()
    }
}

impl Default for AdvisoryLockSession {
    fn default() -> Self {
        Self::new(Arc::new(LocalAdvisoryLockService::default()))
    }
}

impl AdvisoryLockSession {
    /// Creates session ownership over `service`.
    #[must_use]
    pub fn new(service: Arc<dyn AdvisoryLockService>) -> Self {
        Self(Rc::new(RefCell::new(AdvisoryLockSessionState {
            service,
            owner: 0,
            held: HashMap::new(),
        })))
    }

    /// Sets the connection identifier reported by `IS_USED_LOCK` for locks
    /// owned by this session.
    pub fn set_owner(&self, owner: u64) {
        self.0.borrow_mut().owner = owner;
    }

    /// Acquires or reference-increments `name`.
    pub fn acquire(&self, name: &str, timeout: Duration) -> Result<(), AdvisoryLockError> {
        let mut state = self.0.borrow_mut();
        if let Some(held) = state.held.get_mut(name) {
            held.references = held.references.saturating_add(1);
            return Ok(());
        }
        let lease = state.service.acquire(name, timeout)?;
        let owner = state.owner;
        state.held.insert(
            name.to_owned(),
            HeldLock {
                references: 1,
                owner,
                lease,
            },
        );
        Ok(())
    }

    /// Returns the source-visible owner: this connection's id, `1` for a
    /// different owner, or `None` when the name is free.
    pub fn owner(&self, name: &str) -> Option<u64> {
        let state = self.0.borrow();
        if let Some(held) = state.held.get(name) {
            return Some(held.owner);
        }
        state.service.is_used(name).then_some(1)
    }

    /// Decrements this session's reference count, releasing at zero.
    #[must_use]
    pub fn release(&self, name: &str) -> bool {
        let lease = {
            let mut state = self.0.borrow_mut();
            let Some(held) = state.held.get_mut(name) else {
                return false;
            };
            held.references -= 1;
            if held.references > 0 {
                return true;
            }
            state.held.remove(name).map(|held| held.lease)
        };
        if let Some(lease) = lease {
            lease.release();
        }
        true
    }

    /// Releases every unique lock and returns the total reference count.
    pub fn release_all(&self) -> usize {
        let held = std::mem::take(&mut self.0.borrow_mut().held);
        let count = held.values().map(|lock| lock.references).sum();
        for lock in held.into_values() {
            lock.lease.release();
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_and_cross_session_contention_share_one_authority() {
        let service: Arc<dyn AdvisoryLockService> = Arc::new(LocalAdvisoryLockService::default());
        let first = AdvisoryLockSession::new(Arc::clone(&service));
        let second = AdvisoryLockSession::new(service);
        first.set_owner(42);
        second.set_owner(7);

        first.acquire("a", Duration::ZERO).unwrap();
        first.acquire("a", Duration::ZERO).unwrap();
        assert_eq!(first.owner("a"), Some(42));
        assert_eq!(second.owner("a"), Some(1));
        assert_eq!(
            second.acquire("a", Duration::ZERO),
            Err(AdvisoryLockError::Timeout)
        );
        assert!(first.release("a"));
        assert_eq!(second.owner("a"), Some(1));
        assert_eq!(first.release_all(), 1);
        second.acquire("a", Duration::ZERO).unwrap();
    }

    #[test]
    fn a_waiting_local_session_wakes_when_the_owner_releases() {
        let service = Arc::new(LocalAdvisoryLockService::default());
        let held = service.acquire("a", Duration::ZERO).unwrap();
        let waiter = {
            let service = Arc::clone(&service);
            std::thread::spawn(move || {
                let acquired = service
                    .acquire("a", Duration::from_secs(1))
                    .expect("the waiter acquires");
                acquired.release();
            })
        };

        std::thread::sleep(Duration::from_millis(20));
        held.release();
        waiter.join().unwrap();
        assert!(!service.is_used("a"));
    }

    #[test]
    fn each_lock_keeps_the_connection_id_captured_at_acquisition() {
        let session = AdvisoryLockSession::default();
        session.set_owner(42);
        session.acquire("first", Duration::ZERO).unwrap();
        session.set_owner(7);
        session.acquire("second", Duration::ZERO).unwrap();

        assert_eq!(session.owner("first"), Some(42));
        assert_eq!(session.owner("second"), Some(7));
    }
}
