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
//! Go boundaries: client-go `snapshot.go` (`KVSnapshot.get` / BatchGet) and
//! `scan.go` (`Scanner.Next` / getData). Request hints and retry budgets belong
//! to each read; locked scan pairs are reread through Get.

use std::collections::HashMap;
use std::sync::Arc;
use tidb_proto::{
    KvrpcGetRequest, KvrpcGetResponse, KvrpcKeyError, KvrpcScanRequest, KvrpcScanResponse,
};

use crate::gc_state::GcStateCache;
use crate::lock::{
    decode_blocking_lock_observation, record_blocking_locks, resolve_blocking_locks_recorded,
    LockRecoveryClient, TimestampSource,
};
use crate::region::{RegionBackoffBudget, RegionRecoveryLoader};
use crate::rpc::{TransactionBatchPublication, TransactionBatchResponse, UnaryCallContext};
use crate::SharedReadRuntime;

use super::super::command_client::{PublishedCommand, TransactionCommandClient};
use super::super::region_batches::{point_route, RegionKeyBatch};
use super::super::state::{CoordinatorState, TransactionCause};
use super::{
    recover_region_error_with, wait_with_call, OptimisticCoordinatorError,
    RealOptimisticTransaction, RPC_READ_TIMEOUT_MEDIUM,
};

/// Retains the registered category instead of making exhaustion look retryable
/// merely because its formatted text contains "lock" or "region".
pub(super) fn snapshot_recovery_error(cause: TransactionCause) -> OptimisticCoordinatorError {
    match cause {
        TransactionCause::BackoffExhausted { kind, detail } => {
            OptimisticCoordinatorError::SnapshotBackoff { kind, detail }
        }
        other => OptimisticCoordinatorError::SnapshotGet(other.to_string()),
    }
}

fn snapshot_backoff_error(
    error: crate::region::RegionBackoffExhausted,
) -> OptimisticCoordinatorError {
    OptimisticCoordinatorError::SnapshotBackoff {
        kind: error.kind,
        detail: format!("{error:?}"),
    }
}

fn backoff_ignored_snapshot_hints(
    locks: &[crate::lock::BlockingLock],
    context: &tidb_proto::KvrpcContext,
    backoff: &mut RegionBackoffBudget,
    call: &UnaryCallContext,
) -> Result<(), OptimisticCoordinatorError> {
    if locks.iter().any(|lock| {
        context.resolved_locks.contains(&lock.txn_id())
            || context.committed_locks.contains(&lock.txn_id())
    }) {
        wait_with_call(call, std::time::Duration::ZERO).map_err(snapshot_recovery_error)?;
        let delay = backoff
            .next_delay(crate::region::RegionBackoffKind::TxnLockFast)
            .map_err(snapshot_backoff_error)?;
        let result = wait_with_call(call, delay);
        backoff.finish_wait(result.is_ok());
        result.map_err(snapshot_recovery_error)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn resolve_snapshot_locks<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[crate::lock::BlockingLock],
    read_ts: u64,
    context: &tidb_proto::KvrpcContext,
    call: &UnaryCallContext,
    timestamps: &T,
    backoff: &mut RegionBackoffBudget,
    for_read: bool,
    lite: bool,
    record: &mut Option<crate::ResolvingLocksGuard>,
    stats: Option<&Arc<tikv_client::SnapshotRuntimeStats>>,
) -> Result<crate::lock::LockRecoveryResult, OptimisticCoordinatorError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    record_blocking_locks(runtime, locks, read_ts, record);
    let _observation = crate::rpc::SnapshotRpcObservation::start(
        stats,
        tikv_client::SnapshotRpcCommand::ResolveLock,
    );
    // Only the hints sent by this physical read can have been ignored.
    if for_read {
        backoff_ignored_snapshot_hints(locks, context, backoff, call)?;
    }
    // Go ResolveLockDetail excludes ignored-hint backoff and empty lock sets;
    // ClientHelper's outer ResolveLock observation includes the hint wait.
    let started = stats
        .filter(|_| !locks.is_empty())
        .map(|_| std::time::Instant::now());
    let recovery = resolve_blocking_locks_recorded(
        runtime, locks, read_ts, context, call, timestamps, for_read, lite, backoff,
    );
    if let (Some(stats), Some(started)) = (stats, started) {
        stats.record_resolve_lock(started.elapsed());
    }
    recovery.map_err(|error| match error {
        crate::lock::LockRecoveryError::BackoffExhausted(error) => snapshot_backoff_error(error),
        other => OptimisticCoordinatorError::SnapshotGet(other.to_string()),
    })
}

pub(super) fn wait_snapshot_lock_ttl(
    recovery: &crate::lock::LockRecoveryResult,
    backoff: &mut RegionBackoffBudget,
    call: &UnaryCallContext,
) -> Result<(), OptimisticCoordinatorError> {
    if recovery.is_alive() {
        wait_with_call(call, std::time::Duration::ZERO).map_err(snapshot_recovery_error)?;
        let delay = backoff
            .next_delay_capped(crate::region::RegionBackoffKind::TxnLockFast, recovery.ttl)
            .map_err(snapshot_backoff_error)?;
        let result = wait_with_call(call, delay);
        backoff.finish_wait(result.is_ok());
        result.map_err(snapshot_recovery_error)?;
    }
    Ok(())
}

pub(super) fn record_snapshot_backoff(
    stats: Option<&tikv_client::SnapshotRuntimeStats>,
    backoff: &RegionBackoffBudget,
) {
    // snapshot.recordBackoffInfo skips even zero-sleep attempts unless the
    // selected history contains at least one completed positive sleep.
    if let Some(stats) = stats.filter(|_| !backoff.total_sleep().is_zero()) {
        for (kind, count, duration) in backoff.runtime_stats() {
            stats.record_backoff_totals(kind, count, duration);
        }
    }
}

/// Go KVSnapshot.get's latest-committed shortcut applies only after this
/// Get chose its first blocking transaction, and only until TiKV ignores it.
fn ignore_later_max_ts_lock(
    first_lock: &mut Option<u64>,
    read_ts: u64,
    lock_ts: u64,
    request_context: &tidb_proto::KvrpcContext,
) -> bool {
    match *first_lock {
        None => {
            *first_lock = Some(lock_ts);
            false
        }
        Some(first) => {
            read_ts == u64::MAX
                && first != lock_ts
                && !request_context.resolved_locks.contains(&lock_ts)
                && !request_context.committed_locks.contains(&lock_ts)
        }
    }
}

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
        None,
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
    stats: Option<&Arc<tikv_client::SnapshotRuntimeStats>>,
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
    let mut first_lock = None;
    let mut resolving_record = None;
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
        let response = begin_get(runtime, &route, &context, &request, call, stats)?;
        if let Some(region_error) = response.response.region_error.as_ref() {
            recover_region_error_with(
                runtime,
                forward_backoff,
                region_error,
                route.attempt(),
                call,
            )
            .map_err(snapshot_recovery_error)?;
            continue;
        }
        if let Some(key_error) = response.response.error.as_ref() {
            if let Some(lock_info) = key_error.locked.as_ref() {
                if ignore_later_max_ts_lock(
                    &mut first_lock,
                    start_ts,
                    lock_info.lock_version,
                    &context,
                ) {
                    resolved_locks.ignore_lock(lock_info.lock_version);
                    continue;
                }
                let locks = decode_blocking_lock_observation(lock_info)
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                let recovery = resolve_snapshot_locks(
                    runtime,
                    &locks,
                    start_ts,
                    &context,
                    call,
                    timestamps,
                    forward_backoff,
                    true,
                    true,
                    &mut resolving_record,
                    stats,
                )?;
                resolved_locks.absorb(&recovery);
                wait_snapshot_lock_ttl(&recovery, forward_backoff, call)?;
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
    stats: Option<&Arc<tikv_client::SnapshotRuntimeStats>>,
) -> Result<TransactionBatchResponse<KvrpcGetResponse>, OptimisticCoordinatorError>
where
    C: TransactionCommandClient,
    L: RegionRecoveryLoader,
{
    let published = {
        let mut client = runtime.client().try_lock().map_err(|_| {
            OptimisticCoordinatorError::SnapshotGet("TiKV client is already borrowed".to_owned())
        })?;
        let _observation =
            crate::rpc::SnapshotRpcObservation::start(stats, tikv_client::SnapshotRpcCommand::Get);
        client.publish_transaction_get(route.address(), request, context, call)
    };
    match published {
        PublishedCommand::Response(response) => {
            if let Some(stats) = stats.filter(|_| response.response.region_error.is_none()) {
                let payload = if response.response.error.is_some() {
                    0
                } else {
                    response.response.value.len() as u64
                };
                stats.record_point_response(response.response.exec_details_v2.as_ref(), payload);
            }
            Ok(response)
        }
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
        None,
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
    stats: Option<&Arc<tikv_client::SnapshotRuntimeStats>>,
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
    while cursor.as_slice() < end_key {
        let route = point_route(runtime, &cursor)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let context = context_with_resource_group(route.context(), resource_group_name);
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
            .map_err(snapshot_recovery_error)?;
            continue;
        }
        gc_state
            .check_visibility(read_ts)
            .map_err(OptimisticCoordinatorError::Visibility)?;
        if let Some(key_error) = response.response.error.as_ref() {
            let mut locked = Vec::new();
            collect_scan_lock(key_error, &mut locked)?;
            let recovery = resolve_snapshot_locks(
                runtime,
                &locked,
                read_ts,
                &context,
                &page_call,
                timestamps,
                forward_backoff,
                false,
                false,
                &mut None,
                None,
            )?;
            wait_snapshot_lock_ttl(&recovery, forward_backoff, &page_call)?;
            continue;
        }
        let page_len = response.response.pairs.len();
        let mut last_key = Vec::new();
        let pending_region = pending.as_mut().expect("the serving region is pending");
        for (index, mut pair) in response.response.pairs.into_iter().enumerate() {
            if let Some(error) = pair.error.as_ref() {
                if pair.key.is_empty() {
                    let lock = error.locked.as_ref().ok_or_else(|| {
                        OptimisticCoordinatorError::SnapshotGet(format!(
                            "TiKV scan key error: {error:?}"
                        ))
                    })?;
                    pair.key.clone_from(&lock.key);
                }
            }
            if index + 1 == page_len {
                last_key.clone_from(&pair.key);
            }
            if pair.error.is_some() {
                // Go Scanner.Next keeps clean page rows and rereads only this
                // key through snapshot.get, including its exact-request hints.
                let read = snapshot_get_with(
                    runtime,
                    timestamps,
                    read_ts,
                    gc_state,
                    forward_backoff,
                    resolved_locks,
                    resource_group_name,
                    stats,
                    &pair.key,
                    &page_call,
                )?;
                let Some(value) = read.value else {
                    continue;
                };
                pair.value = value;
            }
            pending_region.pairs.push((pair.key, pair.value));
            pair_count += 1;
            // A returned row ends Go Scanner.Next's backoff scope. Clean
            // rows can reuse the untouched budget without drawing a new seed.
            if !forward_backoff.total_sleep().is_zero() {
                *forward_backoff = RegionBackoffBudget::campaign_default();
            }
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
        // One call owns both its region-error and lock-wait budget.
        let mut read_backoff = RegionBackoffBudget::campaign_default();
        let mut rpc_count = 0_u64;
        let mut first_lock = None;
        let mut resolving_record = None;
        let result = (|| loop {
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
            let response = begin_get(
                &self.runtime,
                &route,
                &context,
                &request,
                call,
                self.snapshot_runtime_stats.as_ref(),
            )?;
            if let Some(region_error) = response.response.region_error.as_ref() {
                recover_region_error_with(
                    &self.runtime,
                    &mut read_backoff,
                    region_error,
                    route.attempt(),
                    call,
                )
                .map_err(snapshot_recovery_error)?;
                continue;
            }
            if let Some(key_error) = response.response.error.as_ref() {
                if let Some(lock_info) = key_error.locked.as_ref() {
                    if ignore_later_max_ts_lock(
                        &mut first_lock,
                        read_ts,
                        lock_info.lock_version,
                        &context,
                    ) {
                        self.resolved_locks.ignore_lock(lock_info.lock_version);
                        continue;
                    }
                    let locks = decode_blocking_lock_observation(lock_info).map_err(|error| {
                        OptimisticCoordinatorError::SnapshotGet(error.to_string())
                    })?;
                    let recovery = resolve_snapshot_locks(
                        &self.runtime,
                        &locks,
                        read_ts,
                        &context,
                        call,
                        &self.timestamps,
                        &mut read_backoff,
                        true,
                        true,
                        &mut resolving_record,
                        self.snapshot_runtime_stats.as_ref(),
                    )?;
                    self.resolved_locks.absorb(&recovery);
                    wait_snapshot_lock_ttl(&recovery, &mut read_backoff, call)?;
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
        })();
        record_snapshot_backoff(self.snapshot_runtime_stats.as_deref(), &read_backoff);
        result
    }

    /// Reads a set of encoded keys at one transaction timestamp. Keys are
    /// grouped by serving region and admitted in one BatchCommands packet per
    /// region, matching Go's `BatchGet` transport shape while retaining the
    /// same lock, region-retry, and visibility checks as point Get.
    pub fn snapshot_batch_get(
        &mut self,
        keys: &[Vec<u8>],
        call: &UnaryCallContext,
    ) -> Result<SnapshotScanPairs, OptimisticCoordinatorError>
    where
        C: Clone + Send,
        L: Send + Sync,
        T: Sync,
    {
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
    ) -> Result<SnapshotScanPairs, OptimisticCoordinatorError>
    where
        C: Clone + Send,
        L: Send + Sync,
        T: Sync,
    {
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
        let values = super::snapshot_batch_get::snapshot_batch_get_with(
            &self.runtime,
            &self.timestamps,
            read_ts,
            self.resource_group_name.as_deref(),
            self.snapshot_runtime_stats.as_ref(),
            &mut self.resolved_locks,
            &mut self.snapshot_batch_get_rpc_count,
            keys,
            call,
        )?;
        self.check_visibility_at(read_ts)?;
        self.snapshot_cache.update_batch(keys, &values);
        cached.extend(values);
        Ok(cached.into_iter().collect())
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
            &mut RegionBackoffBudget::campaign_default(),
            &mut self.resolved_locks,
            self.resource_group_name.as_deref(),
            self.snapshot_runtime_stats.as_ref(),
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
            &mut RegionBackoffBudget::campaign_default(),
            &mut self.resolved_locks,
            self.resource_group_name.as_deref(),
            self.snapshot_runtime_stats.as_ref(),
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
    fn snapshot_lock_wait_charges_the_actual_remaining_ttl() {
        use super::*;
        use std::time::Duration;
        let recovery = crate::lock::LockRecoveryResult {
            ttl: Duration::from_millis(1),
            ..Default::default()
        };
        let mut budget = RegionBackoffBudget::new(Duration::from_millis(4));
        let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
        for _ in 0..4 {
            wait_snapshot_lock_ttl(&recovery, &mut budget, &call).unwrap();
        }
        assert_eq!(budget.total_sleep(), Duration::from_millis(4));
        assert!(matches!(
            wait_snapshot_lock_ttl(&recovery, &mut budget, &call),
            Err(OptimisticCoordinatorError::SnapshotBackoff {
                kind: crate::region::RegionBackoffKind::TxnLockFast,
                ..
            })
        ));
    }

    #[test]
    fn ignored_snapshot_hints_share_the_existing_budget_and_cancel_waits() {
        use super::*;
        use crate::region::RegionBackoffKind;
        use std::time::Duration;
        for committed in [false, true] {
            for shared in [false, true] {
                let lock = tidb_proto::KvrpcLockInfo {
                    key: b"row".to_vec(),
                    primary_lock: b"row".to_vec(),
                    lock_version: 42,
                    ..Default::default()
                };
                let lock = if shared {
                    tidb_proto::KvrpcLockInfo {
                        shared_lock_infos: vec![lock.clone(), lock],
                        ..Default::default()
                    }
                } else {
                    lock
                };
                let locks = decode_blocking_lock_observation(&lock).unwrap();
                let mut context = tidb_proto::KvrpcContext::default();
                let mut budget = RegionBackoffBudget::new(Duration::from_millis(1));
                let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
                backoff_ignored_snapshot_hints(&locks, &context, &mut budget, &call).unwrap();
                assert_eq!(budget.total_sleep(), Duration::ZERO);
                if committed {
                    context.committed_locks.push(42);
                } else {
                    context.resolved_locks.push(42);
                }
                backoff_ignored_snapshot_hints(&locks, &context, &mut budget, &call).unwrap();
                assert_eq!(
                    budget.total_sleep(),
                    Duration::from_millis(1),
                    "once per physical response"
                );
                assert!(matches!(
                    backoff_ignored_snapshot_hints(&locks, &context, &mut budget, &call),
                    Err(OptimisticCoordinatorError::SnapshotBackoff {
                        kind: RegionBackoffKind::TxnLockFast,
                        ..
                    })
                ));
                let mut mixed = RegionBackoffBudget::new(Duration::from_millis(1));
                mixed.next_delay(RegionBackoffKind::RegionMiss).unwrap();
                assert!(matches!(
                    backoff_ignored_snapshot_hints(&locks, &context, &mut mixed, &call),
                    Err(OptimisticCoordinatorError::SnapshotBackoff {
                        kind: RegionBackoffKind::RegionMiss,
                        ..
                    })
                ));
                let cancelled = UnaryCallContext::with_timeout(Duration::from_secs(1));
                cancelled.cancellation().cancel();
                assert!(backoff_ignored_snapshot_hints(
                    &locks,
                    &context,
                    &mut RegionBackoffBudget::campaign_default(),
                    &cancelled
                )
                .is_err());
            }
        }
    }

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

    #[test]
    fn snapshot_backoff_records_zero_sleep_attempts_only_with_positive_history() {
        use super::*;
        use crate::region::RegionBackoffKind;
        use std::time::Duration;
        let stats = tikv_client::SnapshotRuntimeStats::new();
        let mut budget = RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 1);
        budget.next_delay(RegionBackoffKind::RegionMiss).unwrap();
        budget.finish_wait(false);
        record_snapshot_backoff(Some(&stats), &budget);
        assert_eq!(stats.backoff_count("regionMiss"), 0);
        budget.next_delay(RegionBackoffKind::TxnLockFast).unwrap();
        budget.finish_wait(true);
        record_snapshot_backoff(None, &budget);
        assert_eq!(stats.backoff_count("txnLockFast"), 0);
        record_snapshot_backoff(Some(&stats), &budget);
        assert_eq!(stats.backoff_count("regionMiss"), 1);
        assert_eq!(stats.backoff_duration("regionMiss"), Duration::ZERO);
        assert_eq!(stats.backoff_count("txnLockFast"), 1);
        assert_eq!(
            stats.backoff_duration("txnLockFast"),
            Duration::from_millis(1)
        );
        record_snapshot_backoff(Some(&stats), &budget);
        assert_eq!(stats.backoff_count("regionMiss"), 2);
        assert_eq!(stats.backoff_count("txnLockFast"), 2);

        let mut cancelled_budget = RegionBackoffBudget::campaign_default();
        let cancelled = UnaryCallContext::with_timeout(Duration::from_secs(1));
        cancelled.cancellation().cancel();
        assert!(wait_snapshot_lock_ttl(
            &crate::lock::LockRecoveryResult::alive(Duration::from_millis(50)),
            &mut cancelled_budget,
            &cancelled,
        )
        .is_err());
        assert_eq!(cancelled_budget.runtime_stats().count(), 0);
    }
}
