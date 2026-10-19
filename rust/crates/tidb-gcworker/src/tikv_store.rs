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

//! The GC worker's store, PD and TiKV halves over client-rust: Go's
//! `GCWorker` holds `tikvStore tikv.Storage` and `pdClient pd.Client` and
//! calls them directly; [`TikvStoreClient`] is those two over client-rust's
//! [`KvStore`] and its PD client, whichever PD client the store was built on
//! (a real cluster's, or the embedded store's).

use std::sync::Arc;
use std::time::Duration;

use tikv_client::async_util::Cancellation;
use tikv_client::proto::keyspacepb::KeyspaceMeta;
use tikv_client::proto::{kvrpcpb, metapb, pdpb};
use tikv_client::tikv::KvStore;
use tikv_client::transaction::gc::{
    new_gc_resolve_lock_max_backoff, resolve_locks_for_range, BaseRegionLockResolver,
    RegionLockResolver,
};
use tikv_client::transaction::range_task::{RangeTaskHandler, Runner, TaskStat};
use tikv_client::{PdClient, TimestampExt};

use crate::seams::{
    AdvanceGcSafePointResult, AdvanceTxnSafePointResult, GcError, GcKvClient, GcPdClient, GcResult,
    GcStore, ResolveLocksRunner,
};

/// Go `gcTimeout`.
const GC_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Go `GCWorker`'s `tikvStore` and `pdClient` over client-rust: the calls
/// block on `runtime`, the worker running on its own thread.
pub struct TikvStoreClient<PdC: PdClient> {
    runtime: Arc<tokio::runtime::Runtime>,
    store: KvStore<PdC>,
    keyspace_meta: Option<KeyspaceMeta>,
    /// Go `GCWorker.regionLockResolver`.
    region_lock_resolver: Arc<dyn RegionLockResolver>,
}

impl<PdC: PdClient> TikvStoreClient<PdC> {
    /// The worker's halves over `store`; `keyspace_meta` is Go's
    /// `store.GetCodec().GetKeyspaceMeta()`, `None` for the null keyspace.
    #[must_use]
    pub fn new(
        runtime: Arc<tokio::runtime::Runtime>,
        store: KvStore<PdC>,
        keyspace_meta: Option<KeyspaceMeta>,
    ) -> Self {
        // Go `NewGCWorker`: `tikv.NewRegionLockResolver("gc-worker", store)`.
        let region_lock_resolver =
            Arc::new(BaseRegionLockResolver::new("gc-worker", (*store).clone()));
        Self {
            runtime,
            store,
            keyspace_meta,
            region_lock_resolver,
        }
    }

    /// The worker's halves with `resolver` resolving locks, as Go's tests and
    /// `RunGCJob` hand the worker their own `regionLockResolver`.
    #[must_use]
    pub fn with_region_lock_resolver(mut self, resolver: Arc<dyn RegionLockResolver>) -> Self {
        self.region_lock_resolver = resolver;
        self
    }

    /// The store beneath.
    #[must_use]
    pub fn store(&self) -> &KvStore<PdC> {
        &self.store
    }

    /// The runtime the calls block on.
    #[must_use]
    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.runtime
    }
}

fn gc_error(error: impl std::fmt::Display) -> GcError {
    GcError::new(error.to_string())
}

impl<PdC: PdClient> GcStore for TikvStoreClient<PdC> {
    fn current_version(&self) -> GcResult<u64> {
        self.runtime
            .block_on(self.store.current_timestamp())
            .map(|timestamp| timestamp.version())
            .map_err(gc_error)
    }

    fn keyspace_meta(&self) -> Option<KeyspaceMeta> {
        self.keyspace_meta.clone()
    }
}

impl<PdC: PdClient> GcPdClient for TikvStoreClient<PdC> {
    fn get_all_stores(&self) -> GcResult<Vec<metapb::Store>> {
        self.runtime
            .block_on(self.store.pd_client().get_all_stores())
            .map_err(gc_error)
    }

    fn update_gc_safe_point(&self, safe_point: u64) -> GcResult<u64> {
        self.runtime
            .block_on(self.store.pd_client().update_safepoint_value(safe_point))
            .map_err(gc_error)
    }

    fn update_service_gc_safe_point(
        &self,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
    ) -> GcResult<u64> {
        self.runtime
            .block_on(
                self.store
                    .pd_client()
                    .update_service_gc_safe_point(service_id, ttl, safe_point),
            )
            .map_err(gc_error)
    }

    fn advance_txn_safe_point(&self, target: u64) -> GcResult<AdvanceTxnSafePointResult> {
        let response: pdpb::AdvanceTxnSafePointResponse = self
            .runtime
            .block_on(
                self.store
                    .pd_client()
                    .advance_transaction_safe_point(target),
            )
            .map_err(gc_error)?;
        Ok(AdvanceTxnSafePointResult {
            old_txn_safe_point: response.old_txn_safe_point,
            target,
            new_txn_safe_point: response.new_txn_safe_point,
            blocker_description: response.blocker_description,
        })
    }

    fn advance_gc_safe_point(&self, target: u64) -> GcResult<AdvanceGcSafePointResult> {
        let response: pdpb::AdvanceGcSafePointResponse = self
            .runtime
            .block_on(self.store.pd_client().advance_gc_safe_point(target))
            .map_err(gc_error)?;
        Ok(AdvanceGcSafePointResult {
            old_gc_safe_point: response.old_gc_safe_point,
            target,
            new_gc_safe_point: response.new_gc_safe_point,
        })
    }

    fn get_all_keyspaces(&self, start_id: u32, limit: u32) -> GcResult<Vec<KeyspaceMeta>> {
        self.runtime
            .block_on(self.store.pd_client().get_all_keyspaces(start_id, limit))
            .map_err(gc_error)
    }
}

/// Go `resolveLocks`'s range-task handler: client-go
/// `tikv.ResolveLocksForRange` through the worker's lock resolver.
#[derive(Clone)]
struct ResolveLocksForRange {
    resolver: Arc<dyn RegionLockResolver>,
    max_version: u64,
    scan_limit: u32,
}

#[async_trait::async_trait]
impl RangeTaskHandler for ResolveLocksForRange {
    async fn handle(
        &self,
        cancellation: Cancellation,
        range: (Vec<u8>, Vec<u8>),
    ) -> (TaskStat, tikv_client::Result<()>) {
        resolve_locks_for_range(
            cancellation,
            self.resolver.as_ref(),
            self.max_version,
            range.0,
            range.1,
            &new_gc_resolve_lock_max_backoff,
            self.scan_limit,
        )
        .await
    }
}

struct TikvResolveLocksRunner<'a, PdC: PdClient> {
    runtime: &'a tokio::runtime::Runtime,
    runner: Runner<PdC, ResolveLocksForRange>,
}

impl<PdC: PdClient> ResolveLocksRunner for TikvResolveLocksRunner<'_, PdC> {
    fn run_on_range(&self, start: &[u8], end: &[u8]) -> GcResult<()> {
        self.runtime
            .block_on(self.runner.run_on_range(start.to_vec(), end.to_vec()))
            .map_err(gc_error)
    }

    fn completed_regions(&self) -> usize {
        self.runner.completed_regions()
    }
}

impl<PdC: PdClient> GcKvClient for TikvStoreClient<PdC> {
    /// Go `doUnsafeDestroyRangeRequest`'s send: one `UnsafeDestroyRange` to
    /// the store at `address`.
    fn unsafe_destroy_range(
        &self,
        address: &str,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> GcResult<Option<String>> {
        let request = kvrpcpb::UnsafeDestroyRangeRequest {
            context: Some(kvrpcpb::Context {
                disk_full_opt: kvrpcpb::DiskFullOpt::AllowedOnAlmostFull as i32,
                ..kvrpcpb::Context::default()
            }),
            start_key: start.to_vec(),
            end_key: end.to_vec(),
        };
        let response = self
            .runtime
            .block_on(self.store.send_request(address, &request, timeout))
            .map_err(gc_error)?;
        Ok(response
            .downcast::<kvrpcpb::UnsafeDestroyRangeResponse>()
            .ok()
            .map(|response| response.error))
    }

    /// Go `tikv.NewDeleteRangeTask(...).Execute`.
    fn delete_range_task(&self, start: &[u8], end: &[u8], concurrency: usize) -> GcResult<()> {
        self.runtime
            .block_on(
                self.store
                    .delete_range_with_concurrency((start.to_vec(), end.to_vec()), concurrency),
            )
            .map(|_| ())
            .map_err(gc_error)
    }

    /// Go `rangetask.NewRangeTaskRunner(name, store, concurrency,
    /// ResolveLocksForRange)`.
    fn resolve_locks_runner(
        &self,
        name: &str,
        concurrency: usize,
        max_version: u64,
        scan_limit: u32,
    ) -> Box<dyn ResolveLocksRunner + '_> {
        let handler = ResolveLocksForRange {
            resolver: Arc::clone(&self.region_lock_resolver),
            max_version,
            scan_limit,
        };
        Box::new(TikvResolveLocksRunner {
            runtime: &self.runtime,
            runner: Runner::new(name, self.store.pd_client(), concurrency.max(1), handler),
        })
    }

    /// Go `doGCForRegion`: `CmdGC` to the region holding `region_key`, with
    /// Go's `gcTimeout`; the region error, or an error, never both.
    fn gc_region(&self, region_key: &[u8], safe_point: u64) -> GcResult<Option<String>> {
        self.runtime.block_on(async {
            let store = self
                .store
                .pd_client()
                .store_for_key(&region_key.to_vec().into())
                .await
                .map_err(gc_error)?;
            let region = &store.region_with_leader.region;
            let request = kvrpcpb::GcRequest {
                context: Some(kvrpcpb::Context {
                    region_id: region.id,
                    region_epoch: region.region_epoch,
                    peer: store.target_peer,
                    ..kvrpcpb::Context::default()
                }),
                safe_point,
            };
            let response = store
                .client
                .dispatch_with_timeout(&request, Some(GC_TIMEOUT))
                .await
                .map_err(gc_error)?
                .downcast::<kvrpcpb::GcResponse>()
                .map_err(|_| GcError::new("[tikv:9005]response body is missing"))?;
            if let Some(region_error) = &response.region_error {
                return Ok(Some(format!("{region_error:?}")));
            }
            if let Some(error) = &response.error {
                return Err(GcError::new(format!("unexpected gc error: {error:?}")));
            }
            Ok(None)
        })
    }
}
