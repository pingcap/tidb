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

use tidb_txnkv::lock::{
    decode_lock_observation, resolve_optimistic_locks, LockRecoveryClient, LockRecoveryResult,
    TimestampSource,
};
use tidb_txnkv::region::RegionRecoveryLoader;
use tidb_txnkv::SharedReadRuntime;

use super::{LockedResponseAction, LockedResponseDelegate, LockedResponseObservation};

/// Installs one injected TSO authority over the existing shared read runtime.
#[derive(Debug)]
pub struct OptimisticLockRecovery<S> {
    timestamp_source: S,
}

impl<S> OptimisticLockRecovery<S> {
    /// Creates a bounded recovery policy without another client or cache.
    #[must_use]
    pub const fn new(timestamp_source: S) -> Self {
        Self { timestamp_source }
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
        )
        .map_err(|error| error.to_string())?;
        if let LockRecoveryResult::Alive(ttl) = result {
            if observation.call.cancellation().wait_timeout(ttl) {
                return Err("optimistic lock TTL wait cancelled by caller".to_owned());
            }
        }
        Ok(LockedResponseAction::RetrySameTask)
    }
}
