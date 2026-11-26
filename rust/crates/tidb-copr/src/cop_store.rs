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

//! The client-go store surface Go's coprocessor (`pkg/store/copr`) uses --
//! the region cache, `SendReq`, the lock resolver and the backoffer -- over
//! the node's client-rust store. The coprocessor holds it as a trait object,
//! so a transaction opener can hand its node's store to the reads it serves.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use tidb_proto::{CoprocessorRequest, CoprocessorResponse};
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::lock::LockRecoveryResult;

use crate::coprocessor_request::CoprocessorRequestEnvelope;

/// One task's `Backoffer` (Go `bo`), shared with the store's sender and lock
/// resolver.
pub type CopBackoffer = Arc<tokio::sync::Mutex<tikv_client::tikv::Backoffer>>;

/// What the store's sender answered for one task.
#[derive(Debug)]
pub enum NativeCopSend {
    /// The native response and the address of the store that answered
    /// (client-go `rpcCtx.Addr`).
    Response(CoprocessorResponse, Option<String>),
    /// A region error the caller handles: the task's ranges are located again.
    RegionError(String),
}

/// The client-go store surface Go's coprocessor uses: the region cache, the
/// region sender, the lock resolver and the backoffer.
pub trait NativeCopStore: Send + Sync + 'static {
    /// Go `KVStore.GetClusterID`.
    fn cluster_id(&self) -> u64;

    /// Go `RegionCache.SplitKeyRangesByLocations`' locations: the regions
    /// covering borrowed logical bounds, in key order, located with the `bo` whose ctx
    /// holds `kv_context` (its PD waits are the statement's).
    fn locate<'a>(
        &self,
        ranges: &mut (dyn Iterator<Item = (&'a [u8], &'a [u8])> + Send),
        kv_context: &tidb_txnkv::StatementKvContext,
    ) -> Result<Vec<tikv_client::RegionWithLeader>, tikv_client::Error>;

    /// Go `KVStore.SendReq(bo, req, region, timeout)`.
    /// `kv_context` holds Go's `bo.GetCtx()` values: the
    /// `util.RUDetailsCtxKey` value the resource-control interceptor charges
    /// for each RPC and the `util.ExecDetailsKey` value.
    fn send(
        &self,
        backoffer: &CopBackoffer,
        request: CoprocessorRequest,
        region: (u64, u64, u64),
        timeout: Duration,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<NativeCopSend, tikv_client::Error>;

    /// Sends a prepared envelope with the transport's final request context.
    #[allow(clippy::too_many_arguments)]
    fn send_prepared(
        &self,
        backoffer: &CopBackoffer,
        request: &CoprocessorRequestEnvelope,
        context: &tidb_proto::KvrpcContext,
        region: (u64, u64, u64),
        timeout: Duration,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<NativeCopSend, tikv_client::Error> {
        let mut request = request.to_proto();
        request.context = Some(context.clone());
        self.send(backoffer, request, region, timeout, kv_context, stats)
    }

    /// Go `ResolveLocksWithOpts` for a read, from `handleLockErr`, under the
    /// same `bo` ctx as [`Self::send`].
    fn resolve_lock(
        &self,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        lock: &tidb_proto::KvrpcLockInfo,
        caller_start_ts: u64,
        kv_context: &tidb_txnkv::StatementKvContext,
    ) -> Result<LockRecoveryResult, tikv_client::Error>;

    /// Go `bo.Backoff(BoRegionMiss, err)`.
    fn backoff_region_miss(
        &self,
        backoffer: &CopBackoffer,
        reason: String,
    ) -> Result<(), tikv_client::Error>;

    /// Go `bo.BackoffWithMaxSleepTxnLockFast(msBeforeExpired, err)`.
    fn backoff_lock_fast(
        &self,
        backoffer: &CopBackoffer,
        max_sleep_ms: u64,
        reason: String,
    ) -> Result<(), tikv_client::Error>;

    /// Go `RegionCache.UpdateBucketsIfNeeded(region, requestVer, latestVer)`.
    fn update_buckets_if_needed(
        &self,
        region: (u64, u64, u64),
        request_version: u64,
        latest_version: u64,
    );

    /// Go `RegionCache.InvalidateCachedRegion(region)`.
    fn invalidate_region(&self, region: (u64, u64, u64));

    /// Go `KVStore.CheckVisibility(startTS)`: whether data read at
    /// `start_ts` may be handed back, against the store's txn safe point.
    fn check_visibility(&self, start_ts: u64) -> Result<(), tikv_client::Error>;
}

/// Runs `future` under Go's `context.WithValue(ctx, util.RUDetailsCtxKey,
/// ruDetails)`: client-rust's dispatch charges the trace context's
/// `RuDetails`, as client-go's resource-control interceptor charges the ctx's.
pub async fn with_ru_details<F: std::future::Future>(
    ru_details: Option<&Arc<tikv_client::RuDetails>>,
    future: F,
) -> F::Output {
    match ru_details {
        Some(ru_details) => {
            let context = tikv_client::util::context_with_ru_details(
                &tikv_client::trace::TraceContext::new(),
                Arc::clone(ru_details),
            );
            tikv_client::trace::with_trace_context(context, future).await
        }
        None => future.await,
    }
}

/// Runs `future` under Go's statement ctx ([`tidb_txnkv::StatementKvContext::trace_context`]):
/// client-rust charges the trace context's `RuDetails` (Go
/// `util.RUDetailsCtxKey`) on every RPC, and its `ExecDetails` (Go
/// `util.ExecDetailsKey`) where client-go does -- KV waits and traffic per
/// send, PD waits, backoff sleeps of the Backoffers made under it.
pub async fn with_statement_kv_context<'a, F>(
    kv_context: &tidb_txnkv::StatementKvContext,
    future: F,
) -> F::Output
where
    F: std::future::Future + Send + 'a,
{
    // Erased, so the ctx scope does not hold the caller's (possibly large)
    // future inline on the blocking caller's stack.
    let future: std::pin::Pin<Box<dyn std::future::Future<Output = F::Output> + Send + 'a>> =
        Box::pin(future);
    match kv_context.trace_context() {
        Some(context) => tikv_client::trace::with_trace_context(context, future).await,
        None => future.await,
    }
}

fn send_coprocessor_request<PdC: tikv_client::PdClient>(
    store: &TikvStore<PdC>,
    backoffer: &CopBackoffer,
    request: CoprocessorRequest,
    region: (u64, u64, u64),
    timeout: Duration,
    kv_context: &tidb_txnkv::StatementKvContext,
    stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
) -> Result<NativeCopSend, tikv_client::Error> {
    let region = tikv_client::RegionVerId {
        id: region.0,
        conf_ver: region.1,
        ver: region.2,
    };
    let send = store
        .store()
        .send_req_ctx_with_stats(Arc::clone(backoffer), request, &region, timeout, stats);
    match store.block_on(with_statement_kv_context(kv_context, send)) {
        Ok((response, address)) => Ok(NativeCopSend::Response(response, address)),
        Err(tikv_client::Error::RegionError(error)) => {
            Ok(NativeCopSend::RegionError(format!("{error:?}")))
        }
        Err(error) => Err(error),
    }
}

impl<PdC: tikv_client::PdClient> NativeCopStore for TikvStore<PdC> {
    fn cluster_id(&self) -> u64 {
        self.block_on(self.store().pd_client().cluster_id())
    }

    fn locate<'a>(
        &self,
        ranges: &mut (dyn Iterator<Item = (&'a [u8], &'a [u8])> + Send),
        kv_context: &tidb_txnkv::StatementKvContext,
    ) -> Result<Vec<tikv_client::RegionWithLeader>, tikv_client::Error> {
        self.block_on(with_statement_kv_context(kv_context, async {
            let pd = self.store().pd_client();
            let codec = match self.store().keyspace() {
                tikv_client::request::Keyspace::Enable { keyspace_id } => Some(
                    tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, keyspace_id)?,
                ),
                _ => None,
            };
            let prefix = codec.as_ref().map(tikv_client::request::ApiV2Codec::prefix);
            let prefix = prefix.as_ref().map(|prefix| prefix.as_slice());
            let mut regions: Vec<tikv_client::RegionWithLeader> = Vec::new();
            // The physical bounds of the location in use (Go's `loc`).
            let mut current: Option<(Vec<u8>, Vec<u8>)> = None;
            for (start, end) in ranges {
                if current.as_ref().is_some_and(|bounds| {
                    location_covers_logical_range(bounds, start, end, prefix)
                }) {
                    continue;
                }
                // Go `CodecPDClient`: TiDB sees logical boundaries, while
                // the native client's region cache retains physical keys.
                let (mut next, end) = if let Some(codec) = &codec {
                    let (start, end) = codec.encode_range(start, end);
                    (Cow::Owned(start), Cow::Owned(end))
                } else {
                    (Cow::Borrowed(start), Cow::Borrowed(end))
                };
                loop {
                    // Go `SplitKeyRangesByLocations`: a key the current
                    // location contains stays with it, and only a key past it
                    // is located again. Two locations therefore never overlap,
                    // however the region cache changed between two lookups (a
                    // split of the table being read).
                    if let Some((_, current_end)) = current
                        .as_ref()
                        .filter(|(start, end)| location_contains(start, end, &next))
                    {
                        if current_end.is_empty()
                            || (!end.is_empty() && current_end.as_slice() >= end.as_ref())
                        {
                            break;
                        }
                        next = Cow::Owned(current_end.clone());
                        continue;
                    }
                    let lookup_key = tikv_client::Key::from(next.into_owned());
                    let mut region = pd.region_for_key(&lookup_key).await?;
                    let located_from: &[u8] = (&lookup_key).into();
                    let region_end = region.region.end_key.clone();
                    // A location refreshed since the previous one was read may
                    // begin before that one ends; it is used from this key on.
                    if region.region.start_key.as_slice() < located_from
                        && current.as_ref().is_some_and(|(_, current_end)| {
                            region.region.start_key < *current_end
                        })
                    {
                        region.region.start_key.clear();
                        region.region.start_key.extend_from_slice(located_from);
                    }
                    current = Some((region.region.start_key.clone(), region_end.clone()));
                    if regions.last().is_none_or(|last| last.region.id != region.region.id) {
                        if let Some(codec)=&codec {
                            (region.region.start_key,region.region.end_key)=codec.decode_range(&region.region.start_key,&region.region.end_key)?;
                            if let Some(buckets)=&mut region.buckets {
                                let wire=tikv_client::request::ApiV1Codec::new(tikv_client::request::KeyMode::Txn);
                                let encoded=buckets.keys.iter().map(|key|wire.encode_region_key(key)).collect::<Vec<_>>();
                                buckets.keys=codec.decode_bucket_keys(&encoded)?;
                            }
                        }
                        regions.push(region);
                    }
                    if region_end.is_empty() || (!end.is_empty() && region_end.as_slice() >= end.as_ref()) {
                        break;
                    }
                    next = Cow::Owned(region_end);
                }
            }
            Ok(regions)
        }))
    }

    fn send(
        &self,
        backoffer: &CopBackoffer,
        request: CoprocessorRequest,
        region: (u64, u64, u64),
        timeout: Duration,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<NativeCopSend, tikv_client::Error> {
        // Go `codecV2.EncodeRequest(CmdCop)`: prefix every unary/batched
        // range before dispatch. The native plan decodes response key fields.
        let request = match self.store().keyspace() {
            tikv_client::request::Keyspace::Enable { keyspace_id } => {
                tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, keyspace_id)?
                    .encode_owned_coprocessor_request(request)
            }
            _ => request,
        };
        send_coprocessor_request(self, backoffer, request, region, timeout, kv_context, stats)
    }

    fn send_prepared(
        &self,
        backoffer: &CopBackoffer,
        request: &CoprocessorRequestEnvelope,
        context: &tidb_proto::KvrpcContext,
        region: (u64, u64, u64),
        timeout: Duration,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<NativeCopSend, tikv_client::Error> {
        let mut request = request.to_proto_for_keyspace(self.store().keyspace())?;
        request.context = Some(context.clone());
        send_coprocessor_request(self, backoffer, request, region, timeout, kv_context, stats)
    }

    fn resolve_lock(
        &self,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        lock: &tidb_proto::KvrpcLockInfo,
        caller_start_ts: u64,
        kv_context: &tidb_txnkv::StatementKvContext,
    ) -> Result<LockRecoveryResult, tikv_client::Error> {
        use tikv_client::txnkv::txnlock::{LockHintsInRequest, ResolveLocksOptions};
        let opts = ResolveLocksOptions {
            caller_start_ts,
            locks: vec![lock.clone()],
            for_read: true,
            lock_hints_in_request: LockHintsInRequest::new(
                &context.resolved_locks,
                &context.committed_locks,
            ),
            ..Default::default()
        };
        let result = self.block_on(with_statement_kv_context(
            kv_context,
            self.store()
                .resolve_locks_with_opts(context, Arc::clone(backoffer), opts),
        ))?;
        Ok(LockRecoveryResult {
            ttl: Duration::from_millis(u64::try_from(result.ttl).unwrap_or(0)),
            ignore_locks: result.ignore_locks,
            access_locks: result.access_locks,
        })
    }

    fn backoff_region_miss(
        &self,
        backoffer: &CopBackoffer,
        reason: String,
    ) -> Result<(), tikv_client::Error> {
        self.block_on(async {
            backoffer
                .lock()
                .await
                .backoff(tikv_client::tikv::BO_REGION_MISS, reason)
                .await
                .map_err(tikv_client::Error::from)
        })
    }

    fn check_visibility(&self, start_ts: u64) -> Result<(), tikv_client::Error> {
        self.store().check_visibility(start_ts)
    }

    fn invalidate_region(&self, region: (u64, u64, u64)) {
        let pd = self.store().pd_client();
        self.block_on(async move {
            pd.invalidate_region_cache(tikv_client::tikv::new_region_ver_id(
                region.0, region.1, region.2,
            ))
            .await;
        });
    }

    fn update_buckets_if_needed(
        &self,
        region: (u64, u64, u64),
        request_version: u64,
        latest_version: u64,
    ) {
        // The reload is spawned on the store's runtime.
        let _guard = self.runtime().enter();
        self.store().pd_client().update_buckets_if_needed(
            tikv_client::tikv::new_region_ver_id(region.0, region.1, region.2),
            request_version,
            latest_version,
        );
    }

    fn backoff_lock_fast(
        &self,
        backoffer: &CopBackoffer,
        max_sleep_ms: u64,
        reason: String,
    ) -> Result<(), tikv_client::Error> {
        self.block_on(async {
            backoffer
                .lock()
                .await
                .backoff_with_max_sleep_txn_lock_fast(max_sleep_ms, reason)
                .await
                .map_err(tikv_client::Error::from)
        })
    }
}

/// Go `KeyLocation.Contains`: whether `key` is in `[start, end)`, an empty
/// end being unbounded.
fn location_contains(start: &[u8], end: &[u8], key: &[u8]) -> bool {
    key >= start && (end.is_empty() || key < end)
}

fn location_covers_logical_range(
    (physical_start, physical_end): &(Vec<u8>, Vec<u8>),
    range_start: &[u8],
    range_end: &[u8],
    prefix: Option<&[u8]>,
) -> bool {
    // A physical boundary outside this keyspace clips to its logical edge,
    // like ApiV2Codec::decode_range. The current region intersects the keyspace.
    let (start, end) = if let Some(prefix) = prefix {
        (
            physical_start.strip_prefix(prefix).unwrap_or_default(),
            physical_end.strip_prefix(prefix).unwrap_or_default(),
        )
    } else {
        (physical_start.as_slice(), physical_end.as_slice())
    };
    location_contains(start, end, range_start)
        && (end.is_empty() || (!range_end.is_empty() && range_end <= end))
}

#[cfg(test)]
mod location_tests {
    use super::{location_contains, location_covers_logical_range};

    #[test]
    fn a_location_contains_its_half_open_range() {
        assert!(location_contains(b"b", b"d", b"b"));
        assert!(location_contains(b"b", b"d", b"c"));
        assert!(!location_contains(b"b", b"d", b"d"));
        assert!(!location_contains(b"b", b"d", b"a"));
        assert!(location_contains(b"b", b"", b"zz"));
    }

    #[test]
    fn logical_keyspace_coverage_matches_encoded_boundaries() {
        let codec = tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, 42)
            .unwrap();
        let prefix = codec.prefix();
        let regions = [
            (vec![], vec![]),
            (vec![10], vec![250, 250]),
            (prefix.to_vec(), codec.encode_key(b"m")),
            (codec.encode_key(b"m"), codec.encode_key(b"w")),
            (codec.encode_key(b"m"), codec.end_key().to_vec()),
        ];
        let keys: &[&[u8]] = &[b"", b"a", b"m", b"n", b"w", b"z"];
        for bounds in &regions {
            for &start in keys {
                for &end in keys.iter().filter(|&&end| end.is_empty() || start <= end) {
                    let (physical_start, physical_end) = codec.encode_range(start, end);
                    let encoded_coverage = location_contains(&bounds.0, &bounds.1, &physical_start)
                        && (bounds.1.is_empty() || physical_end <= bounds.1);
                    assert_eq!(
                        location_covers_logical_range(bounds, start, end, Some(&prefix)),
                        encoded_coverage,
                        "location {bounds:?} and range {start:?}..{end:?}",
                    );
                }
            }
        }
    }
}
