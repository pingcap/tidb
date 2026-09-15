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

use std::collections::HashMap;
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
use super::super::region_batches::{group_snapshot_keys, point_route, RegionKeyBatch};
use super::super::state::CoordinatorState;
use super::{
    alive_retry_delay, recover_region_error_with, wait_with_call, OptimisticCoordinatorError,
    RealOptimisticTransaction, RecoveryPhase, RPC_READ_TIMEOUT_MEDIUM,
};

/// Pairs one Scan page may return. client-go's `scanBatchSize`.
const SCAN_PAGE_LIMIT: u32 = 256;

/// Result of one real transactional point Get at the transaction start timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotGetResult {
    /// Real PD snapshot timestamp shared with later Prewrite.
    pub start_ts: u64,
    /// `None` means missing at the requested read timestamp, from TiKV or
    /// its previously validated snapshot-cache entry.
    pub value: Option<Vec<u8>>,
    /// Serving region for an RPC result; absent on a snapshot-cache hit.
    pub region: Option<crate::region::RegionVerId>,
    /// Physical publication for this call; absent on a snapshot-cache hit.
    pub publication: Option<TransactionBatchPublication>,
    /// Number of physical Get RPCs, including region/lock retries.
    pub rpc_count: u64,
}

/// Key/value pairs one snapshot scan returned, in key order.
pub type SnapshotScanPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Go KVSnapshot's read cache, owned by the transaction rather than a table
/// or statement. The caller already has exclusive snapshot access.
#[derive(Default)]
pub(super) struct SnapshotCache {
    read_ts: u64,
    values: HashMap<Vec<u8>, Option<Vec<u8>>>,
    bytes: u64,
}

impl SnapshotCache {
    fn rescope(&mut self, read_ts: u64) {
        if self.read_ts != read_ts {
            *self = Self {
                read_ts,
                ..Self::default()
            };
        }
    }

    fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.values.get(key)
    }

    fn insert(&mut self, key: &[u8], value: Option<&[u8]>) {
        let value = value.filter(|value| !value.is_empty()).map(<[u8]>::to_vec);
        self.bytes += key.len() as u64 + Self::value_size(&value);
        if let Some(old) = self.values.insert(key.to_vec(), value) {
            self.bytes -= key.len() as u64 + Self::value_size(&old);
        }
    }

    fn value_size(value: &Option<Vec<u8>>) -> u64 {
        std::mem::size_of::<crate::ValueEntry>() as u64
            + value.as_ref().map_or(0, |value| value.len() as u64)
    }

    fn evict(&mut self, keep: impl Fn(&[u8]) -> bool, limit: u64) {
        // Go's 10 GiB soft limit retains values returned by the current
        // operation even if that operation alone exceeds the limit.
        if self.bytes < limit {
            return;
        }
        let bytes = &mut self.bytes;
        self.values.retain(|key, value| {
            if *bytes < limit || keep(key) {
                true
            } else {
                *bytes -= key.len() as u64 + Self::value_size(value);
                false
            }
        });
    }

    fn update_point(&mut self, key: &[u8], value: Option<&[u8]>) {
        if self.read_ts == u64::MAX {
            return;
        }
        self.insert(key, value);
        self.evict(|cached| cached == key, 10 << 30);
    }

    fn update_batch(&mut self, keys: &[Vec<u8>], values: &HashMap<Vec<u8>, Vec<u8>>) {
        if self.read_ts == u64::MAX {
            return;
        }
        for key in keys {
            self.insert(key, values.get(key).map(Vec::as_slice));
        }
        self.evict(|key| values.contains_key(key), 10 << 30);
    }
}

/// One serving region's complete contribution to a snapshot range scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotScanRegion {
    /// Exact region epoch that served every pair in this fragment.
    pub region: crate::region::RegionVerId,
    /// Exclusive end of this fragment, clipped to the requested range end.
    pub end_key: Vec<u8>,
    /// Key/value pairs in key order.
    pub pairs: SnapshotScanPairs,
}

#[derive(Default)]
struct SnapshotScanResult {
    regions: Vec<SnapshotScanRegion>,
}

impl SnapshotScanResult {
    fn into_pairs(self) -> SnapshotScanPairs {
        self.regions
            .into_iter()
            .flat_map(|region| region.pairs)
            .collect()
    }
}

struct PendingScanRegion {
    region: crate::region::RegionVerId,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    pairs: SnapshotScanPairs,
}

impl PendingScanRegion {
    fn finish(self) -> SnapshotScanRegion {
        SnapshotScanRegion {
            region: self.region,
            end_key: self.end_key,
            pairs: self.pairs,
        }
    }
}

/// Runs one MaxTS point snapshot without constructing transaction state.
///
/// The caller supplies the thread-local runtime and lock timestamp authority;
/// this function owns only the per-snapshot recovery sets and backoff budget.
/// Ordinary transactions call the same lower helper with their retained sets.
pub(super) fn direct_snapshot_get<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    gc_state: &GcStateCache,
    resource_group_name: Option<&str>,
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
        resource_group_name,
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
    resource_group_name: Option<&str>,
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
    let mut rpc_count = 0_u64;
    loop {
        let route = point_route(runtime, key)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        // Go `ClientHelper.SendReqCtx` stamps both sets immediately before
        // every send, so a lock this snapshot classified is not met again.
        let mut context = context_with_resource_group(route.context(), resource_group_name);
        resolved_locks.stamp(&mut context);
        let request = KvrpcGetRequest {
            key: key.to_vec(),
            version: start_ts,
            ..KvrpcGetRequest::default()
        };
        rpc_count = rpc_count.wrapping_add(1);
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
            region: Some(route.region()),
            publication: Some(response.publication),
            rpc_count,
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
/// an ordinary transaction scan, while avoiding a PD timestamp and transaction
/// construction for each YCSB E operation.
#[allow(clippy::too_many_arguments)]
pub(super) fn direct_snapshot_scan<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    gc_state: &GcStateCache,
    resource_group_name: Option<&str>,
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
    snapshot_scan_with(
        runtime,
        timestamps,
        gc_state,
        &mut forward_backoff,
        &mut resolved_locks,
        resource_group_name,
        start_key,
        end_key,
        limit,
        u64::MAX,
        call,
    )
    .map(SnapshotScanResult::into_pairs)
}

#[allow(clippy::too_many_arguments)]
fn snapshot_scan_with<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    gc_state: &GcStateCache,
    forward_backoff: &mut RegionBackoffBudget,
    resolved_locks: &mut crate::lock::SnapshotLockSet,
    resource_group_name: Option<&str>,
    start_key: &[u8],
    end_key: &[u8],
    limit: Option<usize>,
    read_ts: u64,
    call: &UnaryCallContext,
) -> Result<SnapshotScanResult, OptimisticCoordinatorError>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    if limit == Some(0) {
        return Ok(SnapshotScanResult::default());
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
    let mut result = SnapshotScanResult::default();
    let mut pending: Option<PendingScanRegion> = None;
    let mut pair_count = 0_usize;
    let mut cursor = start_key.to_vec();
    let mut lock_backoff = RegionBackoffBudget::campaign_default();
    while cursor.as_slice() < end_key {
        let route = point_route(runtime, &cursor)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let mut context = context_with_resource_group(route.context(), resource_group_name);
        resolved_locks.stamp(&mut context);
        let region_end = route.region_end_key().to_vec();
        let page_end = if region_end.is_empty() || region_end.as_slice() > end_key {
            end_key.to_vec()
        } else {
            region_end.clone()
        };
        if let Some(current) = pending.as_ref() {
            if current.region != route.region() {
                if cursor.as_slice() >= current.end_key.as_slice() {
                    result
                        .regions
                        .push(pending.take().expect("pending region exists").finish());
                } else {
                    // The serving epoch changed before this fragment drained.
                    // A coprocessor Analyze response would be discarded and
                    // retried on the replacement regions, so do the same.
                    cursor.clone_from(&current.start_key);
                    pending = None;
                    continue;
                }
            }
        }
        if pending.is_none() {
            pending = Some(PendingScanRegion {
                region: route.region(),
                start_key: cursor.clone(),
                end_key: page_end.clone(),
                pairs: Vec::new(),
            });
        }
        let page_limit = limit.map_or(SCAN_PAGE_LIMIT, |limit| {
            u32::try_from(limit - pair_count)
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
        // Go bounds each region page by its own fresh `ReadTimeoutMedium`
        // (`SendReq(bo, req, loc.Region, ReadTimeoutMedium)` per iteration of
        // `KVSnapshot.scan`): no absolute deadline spans the whole logical
        // range, only the caller's cancellation does. Sharing ONE context here
        // let a table-wide ANALYZE sample -- tens of thousands of honest
        // pages -- saturate its shared deadline to zero mid-scan, and every
        // later page then answered instantly with "timed out after 0ms". Each
        // page now opens its own budget on the caller's cancellation carrier.
        let page_call = UnaryCallContext::with_deadline(
            std::time::Instant::now() + RPC_READ_TIMEOUT_MEDIUM,
            call.cancellation().clone(),
        );
        let response = begin_scan_direct(runtime, &route, &context, &request, &page_call)?;
        if let Some(region_error) = response.response.region_error.as_ref() {
            recover_region_error_with(
                runtime,
                forward_backoff,
                region_error,
                route.attempt(),
                &page_call,
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
            let recovery = resolve_blocking_locks(
                runtime, &locked, read_ts, &context, &page_call, timestamps, true,
            )
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
                wait_with_call(&page_call, delay)
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
        let pending_region = pending.as_mut().expect("the serving region is pending");
        for pair in response.response.pairs {
            pending_region.pairs.push((pair.key, pair.value));
            pair_count += 1;
        }
        if limit.is_some_and(|limit| pair_count >= limit) {
            break;
        }
        if page_len == page_limit as usize {
            cursor = last_key;
            cursor.push(0);
        } else {
            result
                .regions
                .push(pending.take().expect("pending region exists").finish());
            if page_end.as_slice() >= end_key {
                break;
            }
            cursor = page_end;
        }
    }
    if let Some(pending) = pending {
        result.regions.push(pending.finish());
    }
    Ok(result)
}

fn context_with_resource_group(
    context: &tidb_proto::KvrpcContext,
    resource_group_name: Option<&str>,
) -> tidb_proto::KvrpcContext {
    let mut context = context.clone();
    if let Some(resource_group_name) = resource_group_name {
        resource_group_name.clone_into(
            &mut context
                .resource_control_context
                .get_or_insert_with(Default::default)
                .resource_group_name,
        );
    }
    context
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
        self.snapshot_cache.rescope(read_ts);
        if let Some(value) = self.snapshot_cache.get(key) {
            // Like Go, a hit has no new RPC or post-response visibility
            // check: only successfully checked responses enter this cache.
            return Ok(SnapshotGetResult {
                start_ts: self.start_ts,
                value: value.clone(),
                region: None,
                publication: None,
                rpc_count: 0,
            });
        }
        // Go gives each `KVSnapshot.get` ONE backoffer (`getMaxBackoff`,
        // 20s) shared by its region errors and lock waits. Region recovery
        // here draws on `self`'s own budget seam, so the lock half carries
        // its own 20s TIME budget: the accounting is split where Go's is
        // shared, but the per-call ceiling and the `BoTxnLockFast` growth
        // are Go's.
        let mut lock_backoff = RegionBackoffBudget::campaign_default();
        let mut rpc_count = 0_u64;
        loop {
            let route = point_route(&self.runtime, key)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            // Go `ClientHelper.SendReqCtx` stamps both sets onto the context
            // immediately before every send, so a lock this snapshot already
            // classified is never met a second time.
            let mut context = self.write_context(route.context());
            self.resolved_locks.stamp(&mut context);
            let request = KvrpcGetRequest {
                key: key.to_vec(),
                version: read_ts,
                ..KvrpcGetRequest::default()
            };
            self.snapshot_get_rpc_count = self.snapshot_get_rpc_count.wrapping_add(1);
            rpc_count = rpc_count.wrapping_add(1);
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
            self.snapshot_cache.update_point(key, value.as_deref());
            return Ok(SnapshotGetResult {
                // This receipt identifies the transaction which owns the
                // coordinator, not the point-read version. A pessimistic
                // statement may read at a newer for-update timestamp while
                // Prewrite still belongs to this original transaction.
                start_ts: self.start_ts,
                value,
                region: Some(route.region()),
                publication: Some(response.publication),
                rpc_count,
            });
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
        self.snapshot_cache.rescope(read_ts);
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
        let mut cached = HashMap::new();
        let missing = (!self.snapshot_cache.values.is_empty()).then(|| {
            keys.iter()
                .filter_map(|key| match self.snapshot_cache.get(key) {
                    Some(Some(value)) => {
                        cached.insert(key.clone(), value.clone());
                        None
                    }
                    Some(None) => None,
                    None => Some(key.clone()),
                })
                .collect::<Vec<_>>()
        });
        let keys = missing.as_deref().unwrap_or(keys);
        if keys.is_empty() {
            return Ok(cached.into_iter().collect());
        }
        // Go `KVSnapshot.BatchGet` (`batchGetMaxBackoff`, 20s): the same
        // time-budgeted `BoTxnLockFast` wait as `snapshot_get`'s lock arm.
        let mut lock_backoff = RegionBackoffBudget::campaign_default();
        let mut pending = std::borrow::Cow::Borrowed(keys);
        let mut values = HashMap::new();
        loop {
            let mut groups = group_snapshot_keys(&self.runtime, &pending)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            let mut locked = Vec::new();
            let mut lock_context = None;
            let mut retry_keys = Vec::new();
            // Move routed keys into the wire requests; the route/attempt
            // metadata stays available for each response's own recovery.
            let request_keys: Vec<_> = groups.iter_mut().map(RegionKeyBatch::take_keys).collect();
            let mut requests = Vec::with_capacity(groups.len());
            for (batch, keys) in groups.iter().zip(request_keys) {
                let mut context = self.write_context(batch.context());
                self.resolved_locks.stamp(&mut context);
                let request = KvrpcBatchGetRequest {
                    keys,
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
            self.snapshot_batch_get_rpc_count = self
                .snapshot_batch_get_rpc_count
                .wrapping_add(requests.len() as u64);
            for ((batch, request), published) in groups.iter().zip(requests).zip(published) {
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
                    retry_keys.extend(request.request.keys);
                    continue;
                }
                if let Some(key_error) = response.response.error.as_ref() {
                    if let Some(lock_info) = key_error.locked.as_ref() {
                        locked.extend(decode_blocking_lock_observation(lock_info).map_err(
                            |error| OptimisticCoordinatorError::SnapshotGet(error.to_string()),
                        )?);
                        lock_context = Some(request.context.clone());
                        // Go cannot trust any pairs when the response itself
                        // reports an error: retry this entire physical batch.
                        retry_keys.extend(request.request.keys);
                        continue;
                    }
                    return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                        "TiKV key error: {key_error:?}"
                    )));
                }
                for pair in response.response.pairs {
                    if let Some(key_error) = pair.error.as_ref() {
                        if let Some(lock_info) = key_error.locked.as_ref() {
                            let observations = decode_blocking_lock_observation(lock_info)
                                .map_err(|error| {
                                    OptimisticCoordinatorError::SnapshotGet(error.to_string())
                                })?;
                            // The lock owns the key; a pair's outer key may
                            // be empty. Keep clean values and absent keys done.
                            retry_keys.extend(observations.iter().map(|lock| lock.key().to_vec()));
                            locked.extend(observations);
                            lock_context = Some(request.context.clone());
                            continue;
                        }
                        return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                            "TiKV key error: {key_error:?}"
                        )));
                    }
                    if !pair.value.is_empty() {
                        values.insert(pair.key, pair.value);
                    }
                }
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
            }
            if !retry_keys.is_empty() {
                pending = std::borrow::Cow::Owned(retry_keys);
                continue;
            }
            self.check_visibility_at(read_ts)?;
            self.snapshot_cache.update_batch(keys, &values);
            cached.extend(values);
            return Ok(cached.into_iter().collect());
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
        self.snapshot_cache.rescope(read_ts);
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        snapshot_scan_with(
            &self.runtime,
            &self.timestamps,
            &self.gc_state,
            &mut self.forward_backoff,
            &mut self.resolved_locks,
            self.resource_group_name.as_deref(),
            start_key,
            end_key,
            limit,
            read_ts,
            call,
        )
        .map(SnapshotScanResult::into_pairs)
    }

    /// Reads `[start_key, end_key)` grouped by the exact serving regions.
    ///
    /// Unlike the bounded flat scan, this always drains every region so each
    /// element has the same boundary as one successful coprocessor response.
    pub fn snapshot_scan_regions(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Vec<SnapshotScanRegion>, OptimisticCoordinatorError> {
        self.snapshot_cache.rescope(self.start_ts);
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        snapshot_scan_with(
            &self.runtime,
            &self.timestamps,
            &self.gc_state,
            &mut self.forward_backoff,
            &mut self.resolved_locks,
            self.resource_group_name.as_deref(),
            start_key,
            end_key,
            None,
            self.start_ts,
            call,
        )
        .map(|result| result.regions)
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

#[cfg(test)]
mod tests {
    use super::SnapshotCache;
    use std::collections::HashMap;

    #[test]
    fn snapshot_cache_update_and_timestamp_reset() {
        // Go UpdateSnapshotCache protects this operation's returned values
        // while evicting older entries at its soft limit. Use a small limit
        // at the same private eviction boundary instead of allocating GiBs.
        let mut cache = SnapshotCache::default();
        cache.rescope(100);
        cache.update_point(b"keep", Some(b"value"));
        let retained = cache.bytes;
        cache.update_point(b"keep", Some(b"value"));
        assert_eq!(cache.bytes, retained);
        cache.insert(b"old", None);
        cache.evict(|key| key == b"keep", retained + 1);
        assert!(cache.get(b"old").is_none());
        assert_eq!(cache.bytes, retained);
        cache.evict(|key| key == b"keep", 1);
        assert!(cache.get(b"keep").is_some());

        cache.rescope(101);
        assert!(cache.values.is_empty());
        assert_eq!(cache.bytes, 0);
        let keys = vec![b"value".to_vec(), b"missing".to_vec(), b"value".to_vec()];
        let values = HashMap::from([(b"value".to_vec(), b"bytes".to_vec())]);
        cache.update_batch(&keys, &values);
        assert_eq!(cache.values.len(), 2);
        assert_eq!(cache.get(b"missing"), Some(&None));
        let accounted: u64 = cache
            .values
            .iter()
            .map(|(key, value)| key.len() as u64 + SnapshotCache::value_size(value))
            .sum();
        assert_eq!(cache.bytes, accounted);
        cache.rescope(u64::MAX);
        cache.update_point(b"keep", Some(b"latest"));
        cache.update_batch(&keys, &values);
        assert!(cache.values.is_empty());
        assert_eq!(cache.bytes, 0);
    }
}
