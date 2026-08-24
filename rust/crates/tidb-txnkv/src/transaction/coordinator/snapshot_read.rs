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

//! Reads pinned to one transaction timestamp: point Get and ranged Scan,
//! including the lock resolution and GC-visibility check each one owes.
//!
//! Go boundary: client-go's `snapshot.go` — `KVSnapshot.get` / `KVSnapshot.scan`,
//! whose lock-retry budget and post-read `CheckVisibility` placement this
//! mirrors exactly.

use tidb_proto::{
    KvrpcBatchGetRequest, KvrpcGetRequest, KvrpcGetResponse, KvrpcKeyError, KvrpcScanRequest,
    KvrpcScanResponse,
};

use crate::gc_state::GcStateCache;
use crate::lock::{
    decode_blocking_lock_observation, resolve_blocking_locks, LockRecoveryClient, TimestampSource,
};
use crate::region::{RegionBackoffBudget, RegionRecoveryLoader};
use crate::rpc::{TransactionBatchPublication, TransactionBatchResponse, UnaryCallContext};
use crate::SharedReadRuntime;

use super::super::command_client::{
    PublishedCommand, TransactionBatchGetRequest, TransactionCommandClient,
};
use super::super::region_batches::{group_keys, point_route, RegionKeyBatch};
use super::super::state::{CoordinatorState, SnapshotReadReceipt};
use super::{
    alive_retry_delay, recover_region_error_with, wait_with_call, OptimisticCoordinatorError,
    RealOptimisticTransaction, RecoveryPhase,
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

/// Runs one MaxTS point snapshot without constructing transaction state.
///
/// The caller supplies the thread-local runtime and lock timestamp authority;
/// this function owns only the per-snapshot recovery sets and backoff budget.
/// Ordinary transactions call the same lower helper with their retained sets.
pub(super) fn direct_snapshot_get<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    gc_state: &GcStateCache,
    key: &[u8],
    call: &UnaryCallContext,
) -> Result<SnapshotGetResult, OptimisticCoordinatorError>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    let mut forward_backoff = RegionBackoffBudget::campaign_default();
    let mut resolved_locks = crate::lock::SnapshotLockSet::default();
    snapshot_get_with(
        runtime,
        timestamps,
        u64::MAX,
        gc_state,
        &mut forward_backoff,
        &mut resolved_locks,
        key,
        call,
    )
}

#[allow(clippy::too_many_arguments)]
fn snapshot_get_with<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    start_ts: u64,
    gc_state: &GcStateCache,
    forward_backoff: &mut RegionBackoffBudget,
    resolved_locks: &mut crate::lock::SnapshotLockSet,
    key: &[u8],
    call: &UnaryCallContext,
) -> Result<SnapshotGetResult, OptimisticCoordinatorError>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    if key.is_empty() {
        return Err(OptimisticCoordinatorError::SnapshotGet(
            "encoded key is empty".to_owned(),
        ));
    }
    // Go scopes the resolved/committed lock sets per KVSnapshot; a read at a
    // new timestamp is a new snapshot.
    resolved_locks.rescope(start_ts);
    loop {
        let route = point_route(runtime, key)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        // Go `ClientHelper.SendReqCtx` stamps both sets immediately before
        // every send, so a lock this snapshot classified is not met again.
        let mut context = route.context().clone();
        resolved_locks.stamp(&mut context);
        let request = KvrpcGetRequest {
            key: key.to_vec(),
            version: start_ts,
            ..KvrpcGetRequest::default()
        };
        let response = begin_get(runtime, &route, &context, &request, call)?;
        if let Some(region_error) = response.response.region_error.as_ref() {
            recover_region_error_with(
                runtime,
                forward_backoff,
                region_error,
                route.attempt(),
                call,
            )
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            continue;
        }
        if let Some(key_error) = response.response.error.as_ref() {
            if let Some(lock_info) = key_error.locked.as_ref() {
                let locks = decode_blocking_lock_observation(lock_info)
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                let recovery = resolve_blocking_locks(
                    runtime, &locks, start_ts, &context, call, timestamps, true,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                resolved_locks.absorb(&recovery);
                // Go `KVSnapshot.get` (client-go `txnkv/txnsnapshot/
                // snapshot.go`): a still-alive lock's wait is a `BoTxnLockFast`
                // draw against the SAME backoffer that absorbs the call's
                // region errors -- `bo.BackoffWithCfgAndMaxSleep(retry.
                // BoTxnLockFast, int(msBeforeExpired), ...)` -- so the budget
                // is TIME (20s effective), the per-wait sleep starts at 2ms and
                // grows exponentially capped by the lock's own remaining TTL,
                // and there is NO attempt cap. A fixed four-attempt cap here
                // exhausted in milliseconds under sysbench's hot-row
                // contention, failing statements Go simply waits out; a lock
                // already resolved retries immediately with no draw, which is
                // Go's `msBeforeExpired == 0` arm.
                if recovery.is_alive() {
                    // client-go's `BackoffWithCfgAndMaxSleep`: the TTL cap
                    // bounds the CHARGED sleep too; see the sibling arms.
                    let delay = forward_backoff
                        .next_delay_capped(
                            crate::retry::RegionBackoffKind::TxnLockFast,
                            alive_retry_delay(recovery.ttl),
                        )
                        .map_err(|exhausted| {
                            OptimisticCoordinatorError::SnapshotGet(format!(
                                "snapshot lock retry budget exhausted: {exhausted:?}"
                            ))
                        })?;
                    wait_with_call(call, delay).map_err(|error| {
                        OptimisticCoordinatorError::SnapshotGet(error.to_string())
                    })?;
                }
                continue;
            }
            return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                "TiKV key error: {key_error:?}"
            )));
        }
        gc_state
            .check_visibility(start_ts)
            .map_err(OptimisticCoordinatorError::Visibility)?;
        return Ok(SnapshotGetResult {
            start_ts,
            // client-go's rule, verbatim: a zero-length value IS not-found.
            // TiKV sets `not_found` explicitly; Go's unistore never does and
            // relies on this mapping, so honoring only the flag would turn
            // every missing key into Some(empty) over an embedded store.
            value: if response.response.not_found || response.response.value.is_empty() {
                None
            } else {
                Some(response.response.value)
            },
            region: route.region(),
            publication: response.publication,
        });
    }
}

fn begin_get<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    route: &RegionKeyBatch,
    context: &tidb_proto::KvrpcContext,
    request: &KvrpcGetRequest,
    call: &UnaryCallContext,
) -> Result<TransactionBatchResponse<KvrpcGetResponse>, OptimisticCoordinatorError>
where
    C: TransactionCommandClient,
    L: RegionRecoveryLoader,
{
    let published = runtime
        .client()
        .try_lock()
        .map_err(|_| {
            OptimisticCoordinatorError::SnapshotGet("TiKV client is already borrowed".to_owned())
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

/// Runs one MaxTS range snapshot without constructing transaction state.
///
/// The bounded single-row path uses this alongside [`direct_snapshot_get`].
/// It keeps the same region retry, lock resolution, and post-page GC checks as
/// an ordinary transaction scan, while avoiding a PD timestamp and a pinned
/// transaction worker for each YCSB E operation.
#[allow(clippy::too_many_arguments)]
pub(super) fn direct_snapshot_scan<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    gc_state: &GcStateCache,
    start_key: &[u8],
    end_key: &[u8],
    limit: Option<usize>,
    call: &UnaryCallContext,
) -> Result<SnapshotScanPairs, OptimisticCoordinatorError>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    let mut forward_backoff = RegionBackoffBudget::campaign_default();
    let mut resolved_locks = crate::lock::SnapshotLockSet::default();
    let mut snapshot_reads = Vec::new();
    snapshot_scan_with(
        runtime,
        timestamps,
        gc_state,
        &mut forward_backoff,
        &mut resolved_locks,
        &mut snapshot_reads,
        start_key,
        end_key,
        limit,
        u64::MAX,
        call,
    )
}

#[allow(clippy::too_many_arguments)]
fn snapshot_scan_with<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    gc_state: &GcStateCache,
    forward_backoff: &mut RegionBackoffBudget,
    resolved_locks: &mut crate::lock::SnapshotLockSet,
    snapshot_reads: &mut Vec<SnapshotReadReceipt>,
    start_key: &[u8],
    end_key: &[u8],
    limit: Option<usize>,
    read_ts: u64,
    call: &UnaryCallContext,
) -> Result<SnapshotScanPairs, OptimisticCoordinatorError>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
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
    resolved_locks.rescope(read_ts);
    let mut pairs = Vec::new();
    let mut cursor = start_key.to_vec();
    let mut lock_backoff = RegionBackoffBudget::campaign_default();
    while cursor.as_slice() < end_key {
        let route = point_route(runtime, &cursor)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let mut context = route.context().clone();
        resolved_locks.stamp(&mut context);
        let region_end = route.region_end_key().to_vec();
        let page_end = if region_end.is_empty() || region_end.as_slice() > end_key {
            end_key.to_vec()
        } else {
            region_end.clone()
        };
        let page_limit = limit.map_or(SCAN_PAGE_LIMIT, |limit| {
            u32::try_from(limit - pairs.len())
                .unwrap_or(SCAN_PAGE_LIMIT)
                .min(SCAN_PAGE_LIMIT)
        });
        let request = KvrpcScanRequest {
            start_key: cursor.clone(),
            end_key: page_end.clone(),
            limit: page_limit,
            version: read_ts,
            ..KvrpcScanRequest::default()
        };
        let response = begin_scan_direct(runtime, &route, &context, &request, call)?;
        if let Some(region_error) = response.response.region_error.as_ref() {
            recover_region_error_with(
                runtime,
                forward_backoff,
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
            // `collect_scan_lock` now yields BlockingLocks, so a scan that
            // meets a pessimistic lock resolves it through that lock's own
            // protocol instead of refusing it. `true` is the reader's
            // step-over permission, as at every other read site.
            let recovery =
                resolve_blocking_locks(runtime, &locked, read_ts, &context, call, timestamps, true)
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            resolved_locks.absorb(&recovery);
            if recovery.is_alive() {
                let delay = lock_backoff
                    .next_delay_capped(
                        crate::retry::RegionBackoffKind::TxnLockFast,
                        alive_retry_delay(recovery.ttl),
                    )
                    .map_err(|exhausted| {
                        OptimisticCoordinatorError::SnapshotGet(format!(
                            "scan lock retry budget exhausted: {exhausted:?}"
                        ))
                    })?;
                wait_with_call(call, delay)
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            }
            continue;
        }
        gc_state
            .check_visibility(read_ts)
            .map_err(OptimisticCoordinatorError::Visibility)?;
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
        snapshot_reads.push(SnapshotReadReceipt {
            key: cursor.clone(),
            region: route.region(),
            publication: response.publication,
        });
        if limit.is_some_and(|limit| pairs.len() >= limit) {
            break;
        }
        if page_len == page_limit as usize {
            cursor = last_key;
            cursor.push(0);
        } else {
            if page_end.as_slice() >= end_key {
                break;
            }
            cursor = page_end;
        }
    }
    Ok(pairs)
}

fn begin_scan_direct<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    route: &RegionKeyBatch,
    context: &tidb_proto::KvrpcContext,
    request: &KvrpcScanRequest,
    call: &UnaryCallContext,
) -> Result<TransactionBatchResponse<KvrpcScanResponse>, OptimisticCoordinatorError>
where
    C: TransactionCommandClient,
    L: RegionRecoveryLoader,
{
    let published = runtime
        .client()
        .try_lock()
        .map_err(|_| {
            OptimisticCoordinatorError::SnapshotGet("TiKV client is already borrowed".to_owned())
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
        self.snapshot_get_at(key, self.start_ts, call)
    }

    /// Reads one encoded key at `read_ts` through this transaction's shared
    /// region, lock-resolution, and RPC authority.
    ///
    /// A pessimistic statement advances its `for_update_ts` after a write
    /// conflict while retaining the transaction's original start timestamp for
    /// Prewrite. Its retry must therefore read the new statement timestamp,
    /// not the stale transaction snapshot -- Go rebuilds the retried
    /// statement's executor reading at `forUpdateTS`
    /// (`pkg/executor/adapter.go` `handlePessimisticLockError` ->
    /// `UpdateForUpdateTS`), and that applies to every read the retried
    /// statement performs, which is why [`Self::snapshot_batch_get_at`] and
    /// [`Self::snapshot_scan_at`] exist beside this.
    pub fn snapshot_get_at(
        &mut self,
        key: &[u8],
        read_ts: u64,
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
        self.resolved_locks.rescope(read_ts);
        // Go gives each `KVSnapshot.get` ONE backoffer (`getMaxBackoff`,
        // 20s) shared by its region errors and lock waits. Region recovery
        // here draws on `self`'s own budget seam, so the lock half carries
        // its own 20s TIME budget: the accounting is split where Go's is
        // shared, but the per-call ceiling and the `BoTxnLockFast` growth
        // are Go's.
        let mut lock_backoff = RegionBackoffBudget::campaign_default();
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
                version: read_ts,
                ..KvrpcGetRequest::default()
            };
            let response = begin_get(&self.runtime, &route, &context, &request, call)?;
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
                    let locks = decode_blocking_lock_observation(lock_info).map_err(|error| {
                        OptimisticCoordinatorError::SnapshotGet(error.to_string())
                    })?;
                    let recovery = resolve_blocking_locks(
                        &self.runtime,
                        &locks,
                        read_ts,
                        &context,
                        call,
                        &self.timestamps,
                        true,
                    )
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                    // Go `ClientHelper.ResolveLocks`: record before retrying,
                    // or the retry meets the same lock and never terminates.
                    self.resolved_locks.absorb(&recovery);
                    // See `snapshot_get`'s lock arm: Go's `BoTxnLockFast`
                    // time budget, no attempt cap.
                    if recovery.is_alive() {
                        // client-go's `BackoffWithCfgAndMaxSleep`: the TTL
                        // cap bounds the CHARGED sleep too; see the sibling
                        // arms.
                        let delay = lock_backoff
                            .next_delay_capped(
                                crate::retry::RegionBackoffKind::TxnLockFast,
                                alive_retry_delay(recovery.ttl),
                            )
                            .map_err(|exhausted| {
                                OptimisticCoordinatorError::SnapshotGet(format!(
                                    "snapshot lock retry budget exhausted: {exhausted:?}"
                                ))
                            })?;
                        wait_with_call(call, delay).map_err(|error| {
                            OptimisticCoordinatorError::SnapshotGet(error.to_string())
                        })?;
                    }
                    continue;
                }
                return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                    "TiKV key error: {key_error:?}"
                )));
            }
            self.check_visibility_at(read_ts)?;
            // Same client-go zero-length rule as the optimistic read above.
            let value = if response.response.not_found || response.response.value.is_empty() {
                None
            } else {
                Some(response.response.value)
            };
            let result = SnapshotGetResult {
                // This receipt identifies the transaction which owns the
                // coordinator, not the point-read version. A pessimistic
                // statement may read at a newer for-update timestamp while
                // Prewrite still belongs to this original transaction.
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

    /// Reads a set of encoded keys at one transaction timestamp. Keys are
    /// grouped by serving region and admitted in one BatchCommands packet per
    /// region, matching Go's `BatchGet` transport shape while retaining the
    /// same lock, region-retry, and visibility checks as point Get.
    pub fn snapshot_batch_get(
        &mut self,
        keys: &[Vec<u8>],
        call: &UnaryCallContext,
    ) -> Result<SnapshotScanPairs, OptimisticCoordinatorError> {
        self.snapshot_batch_get_at(keys, self.start_ts, call)
    }

    /// [`Self::snapshot_batch_get`] at an explicit statement timestamp; see
    /// [`Self::snapshot_get_at`] for the pessimistic-retry contract that
    /// needs one.
    pub fn snapshot_batch_get_at(
        &mut self,
        keys: &[Vec<u8>],
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<SnapshotScanPairs, OptimisticCoordinatorError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.iter().any(Vec::is_empty) {
            return Err(OptimisticCoordinatorError::SnapshotGet(
                "encoded batch-get key is empty".to_owned(),
            ));
        }
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        self.resolved_locks.rescope(read_ts);
        // Go `KVSnapshot.BatchGet` (`batchGetMaxBackoff`, 20s): the same
        // time-budgeted `BoTxnLockFast` wait as `snapshot_get`'s lock arm.
        let mut lock_backoff = RegionBackoffBudget::campaign_default();
        loop {
            let groups = group_keys(&self.runtime, keys)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            let mut values = Vec::new();
            let mut successful_reads = Vec::new();
            let mut locked = Vec::new();
            let mut lock_context = None;
            let mut retry_region = false;
            let mut requests = Vec::with_capacity(groups.len());
            for batch in &groups {
                let mut context = batch.context().clone();
                self.resolved_locks.stamp(&mut context);
                let request = KvrpcBatchGetRequest {
                    keys: batch.keys().to_vec(),
                    version: read_ts,
                    need_commit_ts: true,
                    ..KvrpcBatchGetRequest::default()
                };
                requests.push(TransactionBatchGetRequest {
                    address: batch.address(),
                    request,
                    context,
                });
            }
            let published = self
                .runtime
                .client()
                .try_lock()
                .map_err(|_| {
                    OptimisticCoordinatorError::SnapshotGet(
                        "TiKV client is already borrowed".to_owned(),
                    )
                })?
                .publish_transaction_batch_gets(&requests, call);
            for ((batch, request), published) in groups.iter().zip(&requests).zip(published) {
                let response = match published {
                    PublishedCommand::Response(response) => response,
                    PublishedCommand::BeforePublication(error)
                    | PublishedCommand::AfterPublication { error, .. } => {
                        return Err(OptimisticCoordinatorError::SnapshotGet(error));
                    }
                };
                if let Some(region_error) = response.response.region_error.as_ref() {
                    self.recover_region_error(
                        RecoveryPhase::Forward,
                        region_error,
                        batch.attempt(),
                        call,
                    )
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                    retry_region = true;
                    break;
                }
                if let Some(key_error) = response.response.error.as_ref() {
                    if let Some(lock_info) = key_error.locked.as_ref() {
                        locked.extend(decode_blocking_lock_observation(lock_info).map_err(
                            |error| OptimisticCoordinatorError::SnapshotGet(error.to_string()),
                        )?);
                        lock_context = Some(request.context.clone());
                        continue;
                    }
                    return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                        "TiKV key error: {key_error:?}"
                    )));
                }
                let mut batch_has_locks = false;
                for pair in response.response.pairs {
                    if let Some(key_error) = pair.error.as_ref() {
                        if let Some(lock_info) = key_error.locked.as_ref() {
                            locked.extend(decode_blocking_lock_observation(lock_info).map_err(
                                |error| OptimisticCoordinatorError::SnapshotGet(error.to_string()),
                            )?);
                            lock_context = Some(request.context.clone());
                            batch_has_locks = true;
                            continue;
                        }
                        return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                            "TiKV key error: {key_error:?}"
                        )));
                    }
                    values.push((pair.key, pair.value));
                }
                if !batch_has_locks {
                    successful_reads.extend(batch.keys().iter().cloned().map(|key| {
                        SnapshotReadReceipt {
                            key,
                            region: batch.region(),
                            publication: response.publication.clone(),
                        }
                    }));
                }
            }
            if retry_region {
                continue;
            }
            if !locked.is_empty() {
                let context = lock_context.unwrap_or_default();
                let recovery = resolve_blocking_locks(
                    &self.runtime,
                    &locked,
                    read_ts,
                    &context,
                    call,
                    &self.timestamps,
                    true,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                self.resolved_locks.absorb(&recovery);
                // See `snapshot_get`'s lock arm: Go's `BoTxnLockFast` time
                // budget, no attempt cap.
                if recovery.is_alive() {
                    // client-go's `BackoffWithCfgAndMaxSleep`: the TTL cap
                    // bounds the CHARGED sleep too, so the 20s budget measures
                    // real waiting -- charging the unclamped exponential
                    // exhausted it after under a second against short-TTL
                    // locks.
                    let delay = lock_backoff
                        .next_delay_capped(
                            crate::retry::RegionBackoffKind::TxnLockFast,
                            alive_retry_delay(recovery.ttl),
                        )
                        .map_err(|exhausted| {
                            OptimisticCoordinatorError::SnapshotGet(format!(
                                "snapshot lock retry budget exhausted: {exhausted:?}"
                            ))
                        })?;
                    wait_with_call(call, delay).map_err(|error| {
                        OptimisticCoordinatorError::SnapshotGet(error.to_string())
                    })?;
                }
                continue;
            }
            self.check_visibility_at(read_ts)?;
            self.snapshot_reads.extend(successful_reads);
            return Ok(values);
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
        self.snapshot_scan_at(start_key, end_key, limit, self.start_ts, call)
    }

    /// [`Self::snapshot_scan`] at an explicit statement timestamp; see
    /// [`Self::snapshot_get_at`] for the pessimistic-retry contract that
    /// needs one.
    pub fn snapshot_scan_at(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        limit: Option<usize>,
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<SnapshotScanPairs, OptimisticCoordinatorError> {
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        snapshot_scan_with(
            &self.runtime,
            &self.timestamps,
            &self.gc_state,
            &mut self.forward_backoff,
            &mut self.resolved_locks,
            &mut self.snapshot_reads,
            start_key,
            end_key,
            limit,
            read_ts,
            call,
        )
    }
}

/// Gathers the locks named by one Scan key error so they can be resolved.
///
/// A key error without lock information is not something a snapshot read can
/// recover from, so it fails the scan instead of being retried forever.
fn collect_scan_lock(
    key_error: &KvrpcKeyError,
    locked: &mut Vec<crate::lock::BlockingLock>,
) -> Result<(), OptimisticCoordinatorError> {
    let Some(lock_info) = key_error.locked.as_ref() else {
        return Err(OptimisticCoordinatorError::SnapshotGet(format!(
            "TiKV scan key error: {key_error:?}"
        )));
    };
    locked.extend(
        decode_blocking_lock_observation(lock_info)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?,
    );
    Ok(())
}
