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

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;

use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::lock::LockRecoveryResult;

/// One task's `Backoffer` (Go `bo`), shared with the store's sender and lock
/// resolver.
pub type CopBackoffer = Arc<tokio::sync::Mutex<tikv_client::tikv::Backoffer>>;

/// What the store's sender answered for one task.
#[derive(Debug)]
pub enum NativeCopSend {
    /// The response, encoded (`coprocessor.Response`), and the address of
    /// the store that answered (client-go `rpcCtx.Addr`).
    Response(Vec<u8>, Option<String>),
    /// A region error the caller handles: the task's ranges are located again.
    RegionError(String),
}

/// The client-go store surface Go's coprocessor uses: the region cache, the
/// region sender, the lock resolver and the backoffer.
pub trait NativeCopStore: Send + Sync + 'static {
    /// Go `KVStore.GetClusterID`.
    fn cluster_id(&self) -> u64;

    /// Go `RegionCache.SplitKeyRangesByLocations`' locations: the regions
    /// covering `ranges`, in key order, located with the `bo` whose ctx
    /// holds `kv_context` (its PD waits are the statement's).
    fn locate(
        &self,
        ranges: &[(Vec<u8>, Vec<u8>)],
        kv_context: &tidb_txnkv::StatementKvContext,
    ) -> Result<Vec<tikv_client::RegionWithLeader>, tikv_client::Error>;

    /// Go `KVStore.SendReq(bo, req, region, timeout)`, `request` encoded.
    /// `kv_context` holds Go's `bo.GetCtx()` values: the
    /// `util.RUDetailsCtxKey` value the resource-control interceptor charges
    /// for each RPC and the `util.ExecDetailsKey` value.
    fn send(
        &self,
        backoffer: &CopBackoffer,
        request: &[u8],
        region: (u64, u64, u64),
        timeout: Duration,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<NativeCopSend, tikv_client::Error>;

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

impl<PdC: tikv_client::PdClient> NativeCopStore for TikvStore<PdC> {
    fn cluster_id(&self) -> u64 {
        self.block_on(self.store().pd_client().cluster_id())
    }

    fn locate(
        &self,
        ranges: &[(Vec<u8>, Vec<u8>)],
        kv_context: &tidb_txnkv::StatementKvContext,
    ) -> Result<Vec<tikv_client::RegionWithLeader>, tikv_client::Error> {
        self.block_on(with_statement_kv_context(kv_context, async {
            let pd = self.store().pd_client();
            let mut regions: Vec<tikv_client::RegionWithLeader> = Vec::new();
            for (start, end) in ranges {
                let mut next = start.clone();
                loop {
                    let region = pd.region_for_key(&next.clone().into()).await?;
                    let region_end = region.region.end_key.clone();
                    if regions
                        .last()
                        .is_none_or(|last| last.region.id != region.region.id)
                    {
                        regions.push(region);
                    }
                    if region_end.is_empty() || (!end.is_empty() && region_end >= *end) {
                        break;
                    }
                    next = region_end;
                }
            }
            Ok(regions)
        }))
    }

    fn send(
        &self,
        backoffer: &CopBackoffer,
        request: &[u8],
        region: (u64, u64, u64),
        timeout: Duration,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<NativeCopSend, tikv_client::Error> {
        let request = tikv_client::proto::coprocessor::Request::decode(request)
            .map_err(|error| tikv_client::Error::StringError(error.to_string()))?;
        let region = tikv_client::RegionVerId {
            id: region.0,
            conf_ver: region.1,
            ver: region.2,
        };
        let send = self
            .store()
            .send_req_ctx_with_stats(Arc::clone(backoffer), request, &region, timeout, stats);
        let sent = self.block_on(with_statement_kv_context(kv_context, send));
        match sent {
            Ok((response, address)) => {
                Ok(NativeCopSend::Response(response.encode_to_vec(), address))
            }
            Err(tikv_client::Error::RegionError(error)) => {
                Ok(NativeCopSend::RegionError(format!("{error:?}")))
            }
            Err(error) => Err(error),
        }
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
        let lock = tikv_client::proto::kvrpcpb::LockInfo::decode(lock.encode_to_vec().as_slice())
            .map_err(|error| tikv_client::Error::StringError(error.to_string()))?;
        let native_context =
            tikv_client::proto::kvrpcpb::Context::decode(context.encode_to_vec().as_slice())
                .map_err(|error| tikv_client::Error::StringError(error.to_string()))?;
        let opts = ResolveLocksOptions {
            caller_start_ts,
            locks: vec![lock],
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
                .resolve_locks_with_opts(&native_context, Arc::clone(backoffer), opts),
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
