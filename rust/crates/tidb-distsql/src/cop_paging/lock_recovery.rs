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

//! DistSQL continuation for bounded optimistic lock recovery.

use std::cell::RefCell;

use tidb_txnkv::lock::{
    decode_lock_observation, resolve_optimistic_locks, LockRecoveryClient, TimestampSource,
};
use tidb_txnkv::region::{RegionBackoffBudget, RegionBackoffKind, RegionRecoveryLoader};
use tidb_txnkv::SharedReadRuntime;

use super::{LockedResponseAction, LockedResponseDelegate, LockedResponseObservation};

/// Installs one injected TSO authority over the existing shared read runtime.
#[derive(Debug)]
pub struct OptimisticLockRecovery<S> {
    timestamp_source: S,
    lock_backoff: RefCell<RegionBackoffBudget>,
}

impl<S> OptimisticLockRecovery<S> {
    /// Creates a bounded recovery policy without another client or cache.
    #[must_use]
    pub fn new(timestamp_source: S) -> Self {
        Self {
            timestamp_source,
            lock_backoff: RefCell::new(RegionBackoffBudget::campaign_default()),
        }
    }

    /// Returns the injected timestamp authority.
    #[must_use]
    pub const fn timestamp_source(&self) -> &S {
        &self.timestamp_source
    }
}

impl<C, L, S> LockedResponseDelegate<C, L> for OptimisticLockRecovery<S>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    S: TimestampSource,
{
    fn handle_locked_response(
        &self,
        runtime: &SharedReadRuntime<C, L>,
        observation: LockedResponseObservation,
    ) -> Result<LockedResponseAction, String> {
        let locks =
            decode_lock_observation(&observation.lock).map_err(|error| error.to_string())?;
        let result = resolve_optimistic_locks(
            runtime,
            &locks,
            observation.caller_start_ts,
            &observation.request_context,
            &observation.call,
            &self.timestamp_source,
            // Go `ClientHelper` is read-only by construction
            // (`client_helper.go:57-58`), and a Cop request is a read.
            true,
        )
        .map_err(|error| error.to_string())?;
        if result.is_alive() {
            // Go `BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), ...)`:
            // retry a short transaction after a millisecond-scale backoff,
            // capped by its TTL, instead of sleeping the whole TTL while the
            // owner is normally about to commit.
            let wait = self
                .lock_backoff
                .borrow_mut()
                .next_delay_capped(RegionBackoffKind::TxnLockFast, result.ttl)
                .map_err(|exhausted| {
                    format!(
                        "optimistic lock recovery exhausted {:?} backoff budget {:?}",
                        exhausted.kind, exhausted.max_sleep
                    )
                })?;
            let deadline_budget = observation.call.timeout();
            if wait > deadline_budget {
                return Err("optimistic lock recovery exceeded the unary deadline".to_owned());
            }
            if observation.call.cancellation().wait_timeout(wait) {
                return Err("optimistic lock TTL wait cancelled by caller".to_owned());
            }
            if !wait.is_zero() && observation.call.timeout().is_zero() {
                return Err("optimistic lock recovery exceeded the unary deadline".to_owned());
            }
        }
        // Go `ClientHelper.ResolveLocks` hands the two lists straight to the
        // snapshot's `TSSet`s, which the *next* send stamps onto its context.
        // The retry below is that next send.
        Ok(LockedResponseAction::RetrySameTask { recovered: result })
    }
}
