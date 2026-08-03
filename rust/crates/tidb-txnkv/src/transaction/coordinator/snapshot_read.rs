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

//! Reads pinned to the transaction's exact `start_ts`: point Get and ranged
//! Scan, including the lock resolution and GC-visibility check each one owes.
//!
//! Go boundary: client-go's `snapshot.go` — `KVSnapshot.get` / `KVSnapshot.scan`,
//! whose lock-retry budget and post-read `CheckVisibility` placement this
//! mirrors exactly.

use tidb_proto::{
    KvrpcGetRequest, KvrpcGetResponse, KvrpcKeyError, KvrpcScanRequest, KvrpcScanResponse,
};

use crate::lock::{
    decode_lock_observation, resolve_optimistic_locks, LockRecoveryClient, TimestampSource,
};
use crate::region::RegionRecoveryLoader;
use crate::rpc::{TransactionBatchPublication, TransactionBatchResponse, UnaryCallContext};

use super::super::command_client::{PublishedCommand, TransactionCommandClient};
use super::super::region_batches::{point_route, RegionKeyBatch};
use super::super::state::{CoordinatorState, SnapshotReadReceipt};
use super::{
    alive_retry_delay, wait_with_call, OptimisticCoordinatorError, RealOptimisticTransaction,
    RecoveryPhase, MAX_LOCK_ATTEMPTS,
};

/// Pairs one Scan page may return. client-go's `scanBatchSize`.
const SCAN_PAGE_LIMIT: u32 = 256;

/// Result of one real transactional point Get at the transaction start timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotGetResult {
    /// Real PD snapshot timestamp shared with later Prewrite.
    pub start_ts: u64,
    /// `None` means TiKV returned `not_found` at exactly `start_ts`.
    pub value: Option<Vec<u8>>,
    /// Exact region epoch that served the successful Get.
    pub region: crate::region::RegionVerId,
    /// Physical BatchCommands publication that produced the value.
    pub publication: TransactionBatchPublication,
}

/// Key/value pairs one snapshot scan returned, in key order.
pub type SnapshotScanPairs = Vec<(Vec<u8>, Vec<u8>)>;

impl<C, L, T> RealOptimisticTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    /// Reads one encoded key at this transaction's exact start timestamp.
    pub fn snapshot_get(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<SnapshotGetResult, OptimisticCoordinatorError> {
        if key.is_empty() {
            return Err(OptimisticCoordinatorError::SnapshotGet(
                "encoded key is empty".to_owned(),
            ));
        }
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let mut lock_attempts = 0usize;
        loop {
            let route = point_route(&self.runtime, key)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            // Go `ClientHelper.SendReqCtx` stamps both sets onto the context
            // immediately before every send, so a lock this snapshot already
            // classified is never met a second time.
            let mut context = route.context().clone();
            self.resolved_locks.stamp(&mut context);
            let request = KvrpcGetRequest {
                key: key.to_vec(),
                version: self.start_ts,
                need_commit_ts: true,
                ..KvrpcGetRequest::default()
            };
            let response = self.begin_get(&route, &context, &request, call)?;
            if let Some(region_error) = response.response.region_error.as_ref() {
                self.recover_region_error(
                    RecoveryPhase::Forward,
                    region_error,
                    route.attempt(),
                    call,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                continue;
            }
            if let Some(key_error) = response.response.error.as_ref() {
                if let Some(lock_info) = key_error.locked.as_ref() {
                    let locks = decode_lock_observation(lock_info).map_err(|error| {
                        OptimisticCoordinatorError::SnapshotGet(error.to_string())
                    })?;
                    let recovery = resolve_optimistic_locks(
                        &self.runtime,
                        &locks,
                        self.start_ts,
                        &context,
                        call,
                        &self.timestamps,
                        true,
                    )
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                    // Go `ClientHelper.ResolveLocks`: record before retrying,
                    // or the retry meets the same lock and never terminates.
                    self.resolved_locks.absorb(&recovery);
                    if lock_attempts >= MAX_LOCK_ATTEMPTS {
                        return Err(OptimisticCoordinatorError::SnapshotGet(
                            "snapshot lock retry budget exhausted".to_owned(),
                        ));
                    }
                    if recovery.is_alive() {
                        wait_with_call(call, alive_retry_delay(recovery.ttl)).map_err(|error| {
                            OptimisticCoordinatorError::SnapshotGet(error.to_string())
                        })?;
                    }
                    lock_attempts += 1;
                    continue;
                }
                return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                    "TiKV key error: {key_error:?}"
                )));
            }
            self.check_visibility()?;
            let value = if response.response.not_found {
                None
            } else {
                Some(response.response.value)
            };
            let result = SnapshotGetResult {
                start_ts: self.start_ts,
                value,
                region: route.region(),
                publication: response.publication,
            };
            self.snapshot_reads.push(SnapshotReadReceipt {
                key: key.to_vec(),
                region: route.region(),
                publication: result.publication.clone(),
            });
            return Ok(result);
        }
    }

    /// Reads every pair in `[start_key, end_key)` at this transaction's exact
    /// start timestamp.
    ///
    /// One Scan is answered by one region, so this walks the range region by
    /// region and, inside a region, page by page until the region is drained.
    /// Because every page is read at the same `start_ts`, a concurrent DDL
    /// cannot make the caller see half of one schema version and half of
    /// another — that single-snapshot property is what makes this usable as a
    /// catalog read.
    ///
    /// `limit` caps how many pairs come back, which is what makes an
    /// incremental caller possible: a cursor that asks for one batch, then
    /// asks again from the key after the last one it got, spends only the
    /// pages it actually consumes. `None` reads the whole range, which is what
    /// a catalog load wants. Fewer pairs than `limit` means the range is
    /// drained, never that one page came back short.
    pub fn snapshot_scan(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        limit: Option<usize>,
        call: &UnaryCallContext,
    ) -> Result<SnapshotScanPairs, OptimisticCoordinatorError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        if start_key.is_empty() {
            return Err(OptimisticCoordinatorError::SnapshotGet(
                "scan start key is empty".to_owned(),
            ));
        }
        if end_key.is_empty() || end_key <= start_key {
            return Err(OptimisticCoordinatorError::SnapshotGet(
                "scan range must be a non-empty [start, end)".to_owned(),
            ));
        }
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let mut pairs = Vec::new();
        let mut cursor = start_key.to_vec();
        let mut lock_attempts = 0usize;
        while cursor.as_slice() < end_key {
            let route = point_route(&self.runtime, &cursor)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            let mut context = route.context().clone();
            self.resolved_locks.stamp(&mut context);
            // TiKV stops at the region boundary anyway; naming it keeps the
            // cursor advance exact when a page ends flush with the region.
            let region_end = route.region_end_key().to_vec();
            let page_end = if region_end.is_empty() || region_end.as_slice() > end_key {
                end_key.to_vec()
            } else {
                region_end.clone()
            };
            // A caller that wants fewer rows than a full page must not make
            // TiKV read a full page: the page itself shrinks to what is left
            // of the caller's budget.
            let page_limit = limit.map_or(SCAN_PAGE_LIMIT, |limit| {
                u32::try_from(limit - pairs.len())
                    .unwrap_or(SCAN_PAGE_LIMIT)
                    .min(SCAN_PAGE_LIMIT)
            });
            let request = KvrpcScanRequest {
                start_key: cursor.clone(),
                end_key: page_end.clone(),
                limit: page_limit,
                version: self.start_ts,
                ..KvrpcScanRequest::default()
            };
            let response = self.begin_scan(&route, &context, &request, call)?;
            if let Some(region_error) = response.response.region_error.as_ref() {
                self.recover_region_error(
                    RecoveryPhase::Forward,
                    region_error,
                    route.attempt(),
                    call,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                continue;
            }
            let mut locked = Vec::new();
            if let Some(key_error) = response.response.error.as_ref() {
                collect_scan_lock(key_error, &mut locked)?;
            }
            for pair in &response.response.pairs {
                if let Some(key_error) = pair.error.as_ref() {
                    collect_scan_lock(key_error, &mut locked)?;
                }
            }
            if !locked.is_empty() {
                if lock_attempts >= MAX_LOCK_ATTEMPTS {
                    return Err(OptimisticCoordinatorError::SnapshotGet(
                        "scan lock retry budget exhausted".to_owned(),
                    ));
                }
                lock_attempts += 1;
                let recovery = resolve_optimistic_locks(
                    &self.runtime,
                    &locked,
                    self.start_ts,
                    &context,
                    call,
                    &self.timestamps,
                    true,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                self.resolved_locks.absorb(&recovery);
                if recovery.is_alive() {
                    wait_with_call(call, alive_retry_delay(recovery.ttl)).map_err(|error| {
                        OptimisticCoordinatorError::SnapshotGet(error.to_string())
                    })?;
                }
                // Redo this page: a locked scan returns no trustworthy pairs.
                continue;
            }
            // Per page, as client-go checks per `scan.Next` batch: a long scan
            // must not spend its whole range on the strength of one check made
            // before the first page.
            self.check_visibility()?;
            let page_len = response.response.pairs.len();
            let last_key = response
                .response
                .pairs
                .last()
                .map(|pair| pair.key.clone())
                .unwrap_or_default();
            for pair in response.response.pairs {
                pairs.push((pair.key, pair.value));
            }
            self.snapshot_reads.push(SnapshotReadReceipt {
                key: cursor.clone(),
                region: route.region(),
                publication: response.publication,
            });
            if limit.is_some_and(|limit| pairs.len() >= limit) {
                break;
            }
            if page_len == page_limit as usize {
                // The page filled up; the next key after the last one served
                // is the smallest key this page could not have covered.
                cursor = last_key;
                cursor.push(0);
            } else {
                // The region (or the requested range) is drained.
                if page_end.as_slice() >= end_key {
                    break;
                }
                cursor = page_end;
            }
        }
        Ok(pairs)
    }

    fn begin_get(
        &self,
        route: &RegionKeyBatch,
        context: &tidb_proto::KvrpcContext,
        request: &KvrpcGetRequest,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchResponse<KvrpcGetResponse>, OptimisticCoordinatorError> {
        let published = self
            .runtime
            .client()
            .try_borrow_mut()
            .map_err(|_| {
                OptimisticCoordinatorError::SnapshotGet(
                    "TiKV client is already borrowed".to_owned(),
                )
            })?
            .publish_transaction_get(route.address(), request, context, call);
        match published {
            PublishedCommand::Response(response) => Ok(response),
            PublishedCommand::BeforePublication(error)
            | PublishedCommand::AfterPublication { error, .. } => {
                Err(OptimisticCoordinatorError::SnapshotGet(error))
            }
        }
    }

    fn begin_scan(
        &self,
        route: &RegionKeyBatch,
        context: &tidb_proto::KvrpcContext,
        request: &KvrpcScanRequest,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchResponse<KvrpcScanResponse>, OptimisticCoordinatorError> {
        let published = self
            .runtime
            .client()
            .try_borrow_mut()
            .map_err(|_| {
                OptimisticCoordinatorError::SnapshotGet(
                    "TiKV client is already borrowed".to_owned(),
                )
            })?
            .publish_transaction_scan(route.address(), request, context, call);
        match published {
            PublishedCommand::Response(response) => Ok(response),
            PublishedCommand::BeforePublication(error)
            | PublishedCommand::AfterPublication { error, .. } => {
                Err(OptimisticCoordinatorError::SnapshotGet(error))
            }
        }
    }
}

/// Gathers the locks named by one Scan key error so they can be resolved.
///
/// A key error without lock information is not something a snapshot read can
/// recover from, so it fails the scan instead of being retried forever.
fn collect_scan_lock(
    key_error: &KvrpcKeyError,
    locked: &mut Vec<crate::lock::OptimisticLock>,
) -> Result<(), OptimisticCoordinatorError> {
    let Some(lock_info) = key_error.locked.as_ref() else {
        return Err(OptimisticCoordinatorError::SnapshotGet(format!(
            "TiKV scan key error: {key_error:?}"
        )));
    };
    locked.extend(
        decode_lock_observation(lock_info)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?,
    );
    Ok(())
}
