// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Lock-resolution operations shared by transactions and external read callers.
//! The resolver algorithm and store-owned state live in `transaction::lock`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::proto::kvrpcpb;
use crate::util::ResolveLockDetail;

pub use crate::transaction::{Lock, LockResolver, ResolveLocksContext, ResolvingLocksGuard};

/// The exact transaction IDs encoded in the physical request that met a lock.
#[derive(Clone, Debug, Default)]
pub struct LockHintsInRequest {
    pub resolved: HashSet<u64>,
    pub committed: HashSet<u64>,
}

impl LockHintsInRequest {
    pub fn new(resolved: &[u64], committed: &[u64]) -> Self {
        Self {
            resolved: resolved.iter().copied().collect(),
            committed: committed.iter().copied().collect(),
        }
    }

    pub(crate) fn reported_lock_type(&self, txn_id: u64) -> Option<&'static str> {
        if self.resolved.contains(&txn_id) {
            Some("resolved")
        } else if self.committed.contains(&txn_id) {
            Some("committed")
        } else {
            None
        }
    }
}

/// client-go `txnlock.ResolveLocksOptions`, distinct from GC cleanup options.
/// Locks use logical keys, like TiKV responses after the client's codec.
#[derive(Clone, Default)]
pub struct ResolveLocksOptions {
    pub caller_start_ts: u64,
    pub locks: Vec<kvrpcpb::LockInfo>,
    pub lite: bool,
    pub for_read: bool,
    pub detail: Option<Arc<Mutex<ResolveLockDetail>>>,
    pub pessimistic_region_resolve: bool,
    pub lock_hints_in_request: LockHintsInRequest,
}

/// One resolution pass. The caller owns any subsequent lock wait and retry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolveLockResult {
    pub ttl: i64,
    pub ignore_locks: Vec<u64>,
    pub access_locks: Vec<u64>,
}
