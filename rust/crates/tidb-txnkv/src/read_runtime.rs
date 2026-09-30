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

//! Shared ownership boundary for the retained TiKV read path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::region::{
    BackgroundRegionCache, BackgroundRegionCacheError, BackgroundRegionCacheOwner, KeyRange,
    LeaderRequest, RegionCache, RegionLoader, RegionLocation, RegionQueryLoader,
    RegionRecoveryError, RegionRecoveryLoader, RegionRouteError, RequestSelection, RequestSelector,
    StoreLiveness, StoreLivenessProbe,
};
use crate::{DirectUnaryClient, DEFAULT_STORE_LIVENESS_TIMEOUT};

const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_GC_LIMIT: usize = 50;
static NEXT_READ_AUTHORITY_ID: AtomicU64 = AtomicU64::new(1);

/// One lock currently being resolved on behalf of a transaction.
///
/// This is client-go's `txnlock.ResolvingLock`: the caller transaction is
/// waiting for `lock_txn_id`'s lock on `key` to be classified or cleaned up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvingLock {
    /// Start timestamp of the transaction attempting the read or write.
    pub txn_id: u64,
    /// Start timestamp of the transaction that owns the encountered lock.
    pub lock_txn_id: u64,
    /// Encountered locked key.
    pub key: Vec<u8>,
    /// Primary key of the lock-owning transaction.
    pub primary: Vec<u8>,
}

type ResolvingLocks = tikv_client::transaction::ResolveLocksContext;

/// Type adapter for the native resolver's observation slot lifetime.
pub struct ResolvingLocksGuard(tikv_client::txnkv::txnlock::ResolvingLocksGuard);

fn native_resolving_locks(
    locks: impl IntoIterator<Item = ResolvingLock>,
) -> Vec<tikv_client::proto::kvrpcpb::LockInfo> {
    locks
        .into_iter()
        .map(|lock| tikv_client::proto::kvrpcpb::LockInfo {
            lock_version: lock.lock_txn_id,
            key: lock.key,
            primary_lock: lock.primary,
            ..Default::default()
        })
        .collect()
}

impl ResolvingLocksGuard {
    /// Replace the current observation while retaining the native slot token.
    pub fn update(&mut self, locks: impl Iterator<Item = ResolvingLock>) {
        self.0.update(&native_resolving_locks(locks));
    }
}

struct DirectUnaryStoreLivenessProbe<C>(C);

impl<C> StoreLivenessProbe for DirectUnaryStoreLivenessProbe<C>
where
    C: DirectUnaryClient + Send + 'static,
{
    fn probe(&self, address: &str, timeout: Duration) -> StoreLiveness {
        self.0
            .liveness(address, timeout)
            .unwrap_or(StoreLiveness::Unknown)
    }
}

fn next_read_authority_id() -> u64 {
    NEXT_READ_AUTHORITY_ID.fetch_add(1, Ordering::Relaxed)
}

/// Process-owned region-cache and TiKV-client capability authority.
///
/// This value is intentionally not `Clone`: it owns the lifetime of the sole
/// maintenance worker. A server distributes [`SharedReadOpener`] values to
/// connection workers. Returned session runtimes contain only the cheap client
/// capability and a counted cache lease, so they cannot terminate a process
/// worker.
pub struct SharedReadAuthority<C, L> {
    opener: SharedReadOpener<C, L>,
    region_cache: BackgroundRegionCacheOwner<L>,
    cluster_id: u64,
    authority_id: u64,
}

/// Cloneable session-opening capability without process shutdown authority.
pub struct SharedReadOpener<C, L> {
    client: C,
    region_cache: BackgroundRegionCache<L>,
    resolving_locks: ResolvingLocks,
    authority_id: u64,
}

impl<C: Clone, L> Clone for SharedReadOpener<C, L> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            region_cache: self.region_cache.clone_opener(),
            resolving_locks: self.resolving_locks.clone(),
            authority_id: self.authority_id,
        }
    }
}

impl<C, L> SharedReadAuthority<C, L>
where
    C: Clone,
    L: RegionQueryLoader + Send + 'static,
{
    /// Starts the one production maintenance worker over the canonical cache.
    pub fn start(
        client: C,
        region_cache: RegionCache<L>,
    ) -> Result<Self, BackgroundRegionCacheError> {
        let cluster_id = region_cache.cluster_id();
        let region_cache = BackgroundRegionCache::start(
            region_cache,
            DEFAULT_MAINTENANCE_INTERVAL,
            DEFAULT_GC_LIMIT,
        )?;
        Ok(Self::from_started(client, region_cache, cluster_id))
    }

    fn from_started(
        client: C,
        region_cache: BackgroundRegionCacheOwner<L>,
        cluster_id: u64,
    ) -> Self {
        let authority_id = next_read_authority_id();
        let opener = SharedReadOpener {
            client,
            region_cache: region_cache.opener_handle(),
            resolving_locks: ResolvingLocks::default(),
            authority_id,
        };
        Self {
            opener,
            region_cache,
            cluster_id,
            authority_id,
        }
    }

    /// Creates one synchronized session lease over process-owned capabilities.
    pub fn open_session(&self) -> Result<SharedReadRuntime<C, L>, BackgroundRegionCacheError> {
        self.opener.open_session()
    }

    /// Returns a cloneable opener with no worker shutdown or join authority.
    #[must_use]
    pub fn opener(&self) -> SharedReadOpener<C, L> {
        self.opener.clone()
    }

    /// Cluster identity owned by the canonical region cache.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Stable process authority identity for lifecycle evidence.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }

    /// Stops and joins the maintenance worker after every session is drained.
    pub fn shutdown(&self) -> Result<(), BackgroundRegionCacheError> {
        self.close_native_lock_resolver();
        self.region_cache.shutdown()
    }

    /// Starts the shared resolver with detached read-side cleanup enabled.
    pub fn start_with_lock_resolver(
        client: C,
        region_cache: RegionCache<L>,
    ) -> Result<Self, BackgroundRegionCacheError>
    where
        C: crate::lock::LockRecoveryClient + Send + 'static,
        L: crate::region::RegionRecoveryLoader,
    {
        Self::start(client, region_cache)
    }
}

impl<C, L> SharedReadAuthority<C, L> {
    fn close_native_lock_resolver(&self) {
        futures::executor::block_on(self.opener.resolving_locks.close());
    }
}

impl<C, L> Drop for SharedReadAuthority<C, L> {
    fn drop(&mut self) {
        self.close_native_lock_resolver();
    }
}

impl<C, L> SharedReadAuthority<C, L>
where
    C: Clone + DirectUnaryClient + crate::lock::LockRecoveryClient + Send + 'static,
    L: RegionQueryLoader + RegionRecoveryLoader + Send + 'static,
{
    /// Starts production maintenance with the same retained TiKV transport as
    /// the foreground sessions, including stale-safe recovery of stores that
    /// become reachable again at the same address.
    pub fn start_with_store_liveness(
        client: C,
        region_cache: RegionCache<L>,
    ) -> Result<Self, BackgroundRegionCacheError> {
        let cluster_id = region_cache.cluster_id();
        let region_cache = BackgroundRegionCache::start_with_liveness(
            region_cache,
            DirectUnaryStoreLivenessProbe(client.clone()),
            DEFAULT_MAINTENANCE_INTERVAL,
            DEFAULT_GC_LIMIT,
            DEFAULT_STORE_LIVENESS_TIMEOUT,
        )?;
        Ok(Self::from_started(client, region_cache, cluster_id))
    }
}

impl<C, L> SharedReadOpener<C, L>
where
    C: Clone,
    L: RegionLoader,
{
    /// Creates one synchronized session lease over process-owned capabilities.
    pub fn open_session(&self) -> Result<SharedReadRuntime<C, L>, BackgroundRegionCacheError> {
        SharedReadRuntime::from_shared_authorities(
            self.client.clone(),
            self.region_cache.open_lease()?,
            self.resolving_locks.clone(),
            self.authority_id,
        )
    }

    /// Stable process authority identity for lifecycle evidence.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }
}

/// One client handle and one region-cache handle shared by read-path policies.
///
/// Cloning this value clones only the handles. It cannot create another
/// client, channel pool, region cache, topology map, or retry authority.
pub struct SharedReadRuntime<C, L> {
    client: Arc<Mutex<C>>,
    region_cache: BackgroundRegionCache<L>,
    resolving_locks: ResolvingLocks,
    cluster_id: u64,
    authority_id: u64,
}

impl<C, L> Clone for SharedReadRuntime<C, L> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            region_cache: self.region_cache.clone(),
            resolving_locks: self.resolving_locks.clone(),
            cluster_id: self.cluster_id,
            authority_id: self.authority_id,
        }
    }
}

impl<C, L: RegionLoader> SharedReadRuntime<C, L> {
    /// Creates the synchronized cache authority for an injected runtime.
    /// Without a process shutdown owner, native cleanup uses its inline fallback.
    #[must_use]
    pub fn new_injected(client: C, region_cache: RegionCache<L>) -> Self {
        let cluster_id = region_cache.cluster_id();
        let mut resolving_locks = ResolvingLocks::default();
        resolving_locks.set_async_resolve_pool_size(0);
        Self {
            client: Arc::new(Mutex::new(client)),
            region_cache: BackgroundRegionCache::without_worker(region_cache),
            resolving_locks,
            cluster_id,
            authority_id: next_read_authority_id(),
        }
    }

    /// Creates a worker-local session over already-owned process authorities.
    fn from_shared_authorities(
        client: C,
        region_cache: BackgroundRegionCache<L>,
        resolving_locks: ResolvingLocks,
        authority_id: u64,
    ) -> Result<Self, BackgroundRegionCacheError> {
        let cluster_id = region_cache.with_cache_read(|cache| cache.cluster_id())?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            region_cache,
            resolving_locks,
            cluster_id,
            authority_id,
        })
    }

    /// Returns a handle to the same client authority.
    #[must_use]
    pub fn client_handle(&self) -> Arc<Mutex<C>> {
        Arc::clone(&self.client)
    }

    /// Locks the same client authority without cloning a handle.
    #[must_use]
    pub fn client(&self) -> &Mutex<C> {
        self.client.as_ref()
    }

    /// Gives an independent policy worker its own client capability while
    /// retaining the same transport, region cache and process authority.
    #[must_use]
    pub fn fork_client(&self) -> Self
    where
        C: Clone,
    {
        Self {
            client: Arc::new(Mutex::new(
                self.client
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone(),
            )),
            region_cache: self.region_cache.clone(),
            resolving_locks: self.resolving_locks.clone(),
            cluster_id: self.cluster_id,
            authority_id: self.authority_id,
        }
    }

    /// Register observations in the process store's native resolver.
    pub fn record_resolving_locks(
        &self,
        txn_id: u64,
        locks: impl IntoIterator<Item = ResolvingLock>,
    ) -> ResolvingLocksGuard {
        ResolvingLocksGuard(tikv_client::txnkv::txnlock::ResolvingLocksGuard::new(
            self.resolving_locks.clone(),
            &native_resolving_locks(locks),
            txn_id,
        ))
    }

    /// Returns all native transaction and coprocessor observations.
    #[must_use]
    pub fn resolving_locks(&self) -> Vec<ResolvingLock> {
        futures::executor::block_on(self.resolving_locks.resolving_locks())
            .into_iter()
            .map(|lock| ResolvingLock {
                txn_id: lock.txn_id,
                lock_txn_id: lock.lock_txn_id,
                key: lock.key,
                primary: lock.primary,
            })
            .collect()
    }

    /// Native transaction sessions share the process store's resolver state.
    pub(crate) fn native_lock_resolver_context(
        &self,
    ) -> tikv_client::transaction::ResolveLocksContext {
        self.resolving_locks.clone()
    }

    /// Returns a handle to the same region-cache authority.
    #[must_use]
    pub fn region_cache_handle(&self) -> BackgroundRegionCache<L> {
        self.region_cache.clone()
    }

    /// Runs one bounded foreground cache operation under the canonical lock.
    pub fn with_region_cache<R>(
        &self,
        operation: impl FnOnce(&mut RegionCache<L>) -> R,
    ) -> Result<R, BackgroundRegionCacheError> {
        self.region_cache.with_cache(operation)
    }

    /// Reads shared canonical metadata without excluding independent requests.
    pub fn inspect_region_cache<R>(
        &self,
        operation: impl FnOnce(&RegionCache<L>) -> R,
    ) -> Result<R, BackgroundRegionCacheError> {
        self.region_cache.with_cache_read(operation)
    }

    /// Keeps route selection and its observation under the same cache borrow.
    pub fn with_request_selection<R>(
        &self,
        selector: &mut RequestSelector,
        observe: impl FnOnce(&RegionCache<L>, Result<RequestSelection, RegionRouteError>) -> R,
    ) -> Result<R, BackgroundRegionCacheError> {
        self.region_cache.with_request_selection(selector, observe)
    }

    /// Publishes only the metadata changes required by successful routing.
    pub fn on_request_success(
        &self,
        request: &LeaderRequest,
    ) -> Result<Result<(), RegionRecoveryError>, BackgroundRegionCacheError> {
        self.region_cache.on_request_success(request)
    }

    /// Finds one key without holding the canonical cache lock across loader I/O.
    pub fn locate_key(
        &self,
        key: &[u8],
    ) -> Result<Result<RegionLocation, RegionRouteError>, BackgroundRegionCacheError> {
        self.region_cache.locate_key(key)
    }

    /// Resolves ranges without holding the canonical cache lock across loader I/O.
    pub fn locate_ranges(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Result<Vec<RegionLocation>, RegionRouteError>, BackgroundRegionCacheError> {
        self.region_cache.locate_ranges(ranges)
    }

    /// Resolves many ranges through the PD batch-scan path.
    pub fn batch_locate_ranges(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Result<Vec<RegionLocation>, RegionRouteError>, BackgroundRegionCacheError> {
        self.region_cache.batch_locate_ranges(ranges)
    }

    /// Coalesces a store-check request into the sole maintenance worker.
    pub fn trigger_store_check(&self) -> Result<bool, BackgroundRegionCacheError> {
        self.region_cache.trigger_store_check()
    }

    /// Cluster identity owned by the sole region cache.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Stable identity of the process authority that opened this session.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }
}

#[cfg(test)]
mod async_resolve_tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::lock::{
        resolve_optimistic_locks, FixedTimestampSource, LockRecoveryClient, OptimisticLock,
    };
    use crate::region::{
        Peer, PeerRole, RegionEpoch, RegionLoadError, RegionMetadata, RegionRecoveryLoader,
        RegionVerId, Store, StoreMetadata,
    };
    use crate::rpc::{DirectUnaryClientError, UnaryCallContext, UnaryCancellation};
    use tidb_proto::{
        KvrpcCheckSecondaryLocksRequest, KvrpcCheckSecondaryLocksResponse,
        KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcLockInfo,
        KvrpcPessimisticRollbackRequest, KvrpcPessimisticRollbackResponse, KvrpcResolveLockRequest,
        KvrpcResolveLockResponse,
    };

    #[derive(Clone)]
    struct Loader;

    impl RegionLoader for Loader {
        fn cluster_id(&self) -> u64 {
            7
        }

        fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
            let (id, start_key, end_key) = if key < b"n".as_slice() {
                (1, Vec::new(), b"n".to_vec())
            } else {
                (2, b"n".to_vec(), Vec::new())
            };
            Ok(RegionLocation {
                region: RegionVerId {
                    id,
                    epoch: RegionEpoch {
                        conf_ver: 1,
                        version: 1,
                    },
                },
                start_key,
                end_key,
                peers: vec![Peer {
                    id: id + 10,
                    store_id: id + 20,
                    role: PeerRole::Voter,
                    is_witness: false,
                    store_epoch: 1,
                }],
                leader_peer_id: Some(id + 10),
                stores: vec![Store {
                    id: id + 20,
                    address: format!("store-{id}"),
                    epoch: 1,
                }],
                ..RegionLocation::default()
            })
        }
    }

    impl RegionQueryLoader for Loader {
        fn query_region(
            &mut self,
            query: crate::region::RegionQuery<'_>,
            _: crate::region::RegionQueryOptions,
        ) -> Result<RegionLocation, RegionLoadError> {
            match query {
                crate::region::RegionQuery::Key(key) => self.load_region(key),
                crate::region::RegionQuery::Id(id) => {
                    self.load_region(if id == 1 { b"a" } else { b"z" })
                }
                _ => unreachable!(),
            }
        }
        fn scan_regions_once(
            &mut self,
            _: &KeyRange,
            _: usize,
            _: crate::region::RegionQueryOptions,
        ) -> Result<Vec<RegionLocation>, RegionLoadError> {
            unreachable!()
        }
        fn load_store(&mut self, _: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
            unreachable!()
        }
    }

    impl RegionRecoveryLoader for Loader {
        fn hydrate_region(
            &mut self,
            _metadata: &RegionMetadata,
            _leader_store_id: u64,
            _resolved_stores: &mut BTreeMap<u64, Option<StoreMetadata>>,
        ) -> Result<RegionLocation, RegionLoadError> {
            Err(RegionLoadError::new(
                "test",
                "unexpected metadata hydration",
            ))
        }
    }

    #[test]
    fn resolving_observation_uses_the_native_registry_and_guard_lifetime() {
        let runtime = SharedReadRuntime::new_injected((), RegionCache::new(Loader));
        let native = runtime.native_lock_resolver_context();
        let lock = |key| ResolvingLock {
            txn_id: 999,
            lock_txn_id: 10,
            key: vec![key],
            primary: vec![1],
        };
        let mut first = runtime.record_resolving_locks(20, [lock(2)]);
        let second = runtime.record_resolving_locks(20, [lock(3)]);
        let observed = futures::executor::block_on(native.resolving_locks());
        assert_eq!(observed.len(), 2, "TiDB registered in a separate owner");
        assert!(observed.iter().all(|lock| lock.txn_id == 20));
        first.update([lock(4)].into_iter());
        drop(second);
        let observed = futures::executor::block_on(native.resolving_locks());
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].txn_id, 20);
        assert_eq!(observed[0].key, [4]);
        assert_eq!(runtime.resolving_locks()[0].key, [4]);
        drop(first);
        assert!(futures::executor::block_on(native.resolving_locks()).is_empty());
        assert!(runtime.resolving_locks().is_empty());
    }

    #[derive(Clone)]
    struct Client {
        check_calls: Arc<AtomicUsize>,
        resolve_calls: Arc<AtomicUsize>,
        resolve_requests: Arc<Mutex<Vec<(u64, Vec<Vec<u8>>, bool)>>>,
    }

    impl LockRecoveryClient for Client {
        fn check_txn_status_for_lock(
            &mut self,
            _address: &str,
            _request: &KvrpcCheckTxnStatusRequest,
            _context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
            self.check_calls.fetch_add(1, Ordering::Relaxed);
            Ok(KvrpcCheckTxnStatusResponse {
                commit_version: 150,
                ..KvrpcCheckTxnStatusResponse::default()
            })
        }

        fn check_secondary_locks_for_lock(
            &mut self,
            _address: &str,
            _request: &KvrpcCheckSecondaryLocksRequest,
            _context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
            Ok(KvrpcCheckSecondaryLocksResponse::default())
        }

        fn resolve_lock_for_read(
            &mut self,
            _address: &str,
            request: &KvrpcResolveLockRequest,
            context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
            self.resolve_requests.lock().unwrap().push((
                context.region_id,
                request.keys.clone(),
                request.is_async,
            ));
            self.resolve_calls.fetch_add(1, Ordering::Relaxed);
            Ok(KvrpcResolveLockResponse::default())
        }

        fn pessimistic_rollback_for_lock(
            &mut self,
            _address: &str,
            _request: &KvrpcPessimisticRollbackRequest,
            _context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
            Ok(KvrpcPessimisticRollbackResponse::default())
        }
    }

    #[derive(Clone)]
    struct ParallelAsyncCommitClient {
        max_secondary_active: Arc<AtomicUsize>,
        secondary_active: Arc<AtomicUsize>,
        max_resolve_active: Arc<AtomicUsize>,
        resolve_active: Arc<AtomicUsize>,
    }

    struct ActiveRpc<'a>(&'a AtomicUsize);

    impl Drop for ActiveRpc<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn enter_rpc<'a>(active: &'a AtomicUsize, maximum: &AtomicUsize) -> ActiveRpc<'a> {
        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
        maximum.fetch_max(now, Ordering::SeqCst);
        ActiveRpc(active)
    }

    impl LockRecoveryClient for ParallelAsyncCommitClient {
        fn fork_for_async_worker(&self) -> Option<Box<dyn LockRecoveryClient + Send>> {
            Some(Box::new(self.clone()))
        }

        fn check_txn_status_for_lock(
            &mut self,
            _address: &str,
            request: &KvrpcCheckTxnStatusRequest,
            _context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
            Ok(KvrpcCheckTxnStatusResponse {
                lock_ttl: 1,
                lock_info: Some(KvrpcLockInfo {
                    lock_version: request.lock_ts,
                    primary_lock: request.primary_key.clone(),
                    secondaries: vec![b"a-secondary".to_vec(), b"z-secondary".to_vec()],
                    use_async_commit: true,
                    min_commit_ts: 100,
                    ..KvrpcLockInfo::default()
                }),
                ..KvrpcCheckTxnStatusResponse::default()
            })
        }

        fn check_secondary_locks_for_lock(
            &mut self,
            _address: &str,
            request: &KvrpcCheckSecondaryLocksRequest,
            _context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
            let _active = enter_rpc(&self.secondary_active, &self.max_secondary_active);
            std::thread::sleep(Duration::from_millis(30));
            Ok(KvrpcCheckSecondaryLocksResponse {
                locks: request
                    .keys
                    .iter()
                    .map(|key| KvrpcLockInfo {
                        key: key.clone(),
                        lock_version: request.start_version,
                        use_async_commit: true,
                        min_commit_ts: 150,
                        ..KvrpcLockInfo::default()
                    })
                    .collect(),
                ..KvrpcCheckSecondaryLocksResponse::default()
            })
        }

        fn resolve_lock_for_read(
            &mut self,
            _address: &str,
            _request: &KvrpcResolveLockRequest,
            _context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
            let _active = enter_rpc(&self.resolve_active, &self.max_resolve_active);
            std::thread::sleep(Duration::from_millis(30));
            Ok(KvrpcResolveLockResponse::default())
        }

        fn pessimistic_rollback_for_lock(
            &mut self,
            _address: &str,
            _request: &KvrpcPessimisticRollbackRequest,
            _context: &KvrpcContext,
            _call: &UnaryCallContext,
        ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
            Ok(KvrpcPessimisticRollbackResponse::default())
        }
    }

    #[test]
    fn async_commit_secondary_checks_and_cleanup_run_per_region_concurrently() {
        #[derive(Debug)]
        struct Oracle(AtomicU64);
        impl crate::lock::TimestampSource for Oracle {
            fn current_ts(&self) -> Result<u64, String> {
                Ok(self.0.fetch_add(1 << 18, Ordering::SeqCst))
            }
        }
        let client = ParallelAsyncCommitClient {
            max_secondary_active: Arc::new(AtomicUsize::new(0)),
            secondary_active: Arc::new(AtomicUsize::new(0)),
            max_resolve_active: Arc::new(AtomicUsize::new(0)),
            resolve_active: Arc::new(AtomicUsize::new(0)),
        };
        let cache = BackgroundRegionCache::without_worker(RegionCache::new(Loader));
        let resolving_locks = ResolvingLocks::default();
        let authority_id = next_read_authority_id();
        let runtime = SharedReadRuntime::from_shared_authorities(
            client.clone(),
            cache.open_lease().expect("the test opens a cache lease"),
            resolving_locks,
            authority_id,
        )
        .expect("the test runtime shares the resolver opener");

        let result = resolve_optimistic_locks(
            &runtime,
            &[OptimisticLock {
                key: b"primary".to_vec(),
                primary: b"primary".to_vec(),
                txn_id: 100,
                ttl_ms: 0,
                txn_size: 3,
                lock_type: 2,
                min_commit_ts: 0,
                use_async_commit: true,
                secondaries: Vec::new(),
            }],
            200,
            &KvrpcContext::default(),
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
            &Arc::new(Oracle(AtomicU64::new(1 << 18))),
            true,
        )
        .expect("the async-commit secondary checks determine the commit timestamp");
        assert_eq!(result.access_locks, vec![100]);
        assert!(
            client.max_secondary_active.load(Ordering::SeqCst) > 1,
            "CheckSecondaryLocks calls for different regions overlap"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while client.resolve_active.load(Ordering::SeqCst) != 0
            || client.max_resolve_active.load(Ordering::SeqCst) < 2
        {
            assert!(
                std::time::Instant::now() < deadline,
                "cleanup completes concurrently"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        futures::executor::block_on(runtime.native_lock_resolver_context().close());
    }

    #[test]
    fn resolved_txn_status_cache_reuses_check_status_results() {
        let check_calls = Arc::new(AtomicUsize::new(0));
        let runtime = SharedReadRuntime::new_injected(
            Client {
                check_calls: Arc::clone(&check_calls),
                resolve_calls: Arc::new(AtomicUsize::new(0)),
                resolve_requests: Arc::new(Mutex::new(Vec::new())),
            },
            RegionCache::new(Loader),
        );
        let lock = OptimisticLock {
            key: b"secondary".to_vec(),
            primary: b"primary".to_vec(),
            txn_id: 100,
            ttl_ms: 0,
            txn_size: 1,
            lock_type: 2,
            min_commit_ts: 0,
            use_async_commit: false,
            secondaries: Vec::new(),
        };

        for _ in 0..2 {
            let result = resolve_optimistic_locks(
                &runtime,
                std::slice::from_ref(&lock),
                200,
                &KvrpcContext::default(),
                &UnaryCallContext::with_timeout(Duration::from_secs(1)),
                &Arc::new(FixedTimestampSource::new(1)),
                true,
            )
            .expect("both reads resolve using the same determined transaction status");
            assert_eq!(result.access_locks, vec![100]);
        }

        assert_eq!(check_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn small_read_cleanup_is_detached_and_keeps_request_source() {
        struct BlockingCleanup(mpsc::Sender<(KvrpcResolveLockRequest, UnaryCallContext)>);
        impl LockRecoveryClient for BlockingCleanup {
            fn check_txn_status_for_lock(
                &mut self,
                _: &str,
                _: &KvrpcCheckTxnStatusRequest,
                _: &KvrpcContext,
                _: &UnaryCallContext,
            ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
                Ok(KvrpcCheckTxnStatusResponse {
                    commit_version: 150,
                    ..Default::default()
                })
            }
            fn check_secondary_locks_for_lock(
                &mut self,
                _: &str,
                _: &KvrpcCheckSecondaryLocksRequest,
                _: &KvrpcContext,
                _: &UnaryCallContext,
            ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
                unreachable!()
            }
            fn pessimistic_rollback_for_lock(
                &mut self,
                _: &str,
                _: &KvrpcPessimisticRollbackRequest,
                _: &KvrpcContext,
                _: &UnaryCallContext,
            ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
                unreachable!()
            }
            fn resolve_lock_for_read(
                &mut self,
                _: &str,
                request: &KvrpcResolveLockRequest,
                _: &KvrpcContext,
                call: &UnaryCallContext,
            ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
                self.0.send((request.clone(), call.clone())).unwrap();
                assert!(call.cancellation().wait_timeout(Duration::from_secs(5)));
                Err(DirectUnaryClientError::CallerCancelled)
            }
        }
        let (sent, received) = mpsc::channel();
        let mut runtime =
            SharedReadRuntime::new_injected(BlockingCleanup(sent), RegionCache::new(Loader));
        runtime.resolving_locks = ResolvingLocks::default();
        let caller_cancellation = UnaryCancellation::new();
        let result = resolve_optimistic_locks(
            &runtime,
            &[OptimisticLock {
                key: b"secondary".to_vec(),
                primary: b"primary".to_vec(),
                txn_id: 100,
                ttl_ms: 0,
                txn_size: 1,
                lock_type: 2,
                min_commit_ts: 0,
                use_async_commit: false,
                secondaries: Vec::new(),
            }],
            200,
            &KvrpcContext {
                request_source: "foreground-read".to_owned(),
                ..Default::default()
            },
            &UnaryCallContext::new(Duration::from_secs(1), caller_cancellation.clone()),
            &Arc::new(FixedTimestampSource::new(1)),
            true,
        )
        .unwrap();
        assert_eq!(result.access_locks, vec![100]);
        let (request, background_call) = received.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(request.start_version, 100);
        assert_eq!(request.commit_version, 150);
        assert_eq!(request.keys, vec![b"secondary".to_vec()]);
        assert_eq!(request.context.unwrap().request_source, "foreground-read");
        caller_cancellation.cancel();
        assert!(!background_call.cancellation().is_cancelled());
        futures::executor::block_on(runtime.native_lock_resolver_context().close());
        assert!(background_call.cancellation().is_cancelled());
    }

    #[test]
    fn large_read_cleanup_is_detached_and_scans_the_region() {
        let resolve_calls = Arc::new(AtomicUsize::new(0));
        let resolve_requests = Arc::new(Mutex::new(Vec::new()));
        let client = Client {
            check_calls: Arc::new(AtomicUsize::new(0)),
            resolve_calls: Arc::clone(&resolve_calls),
            resolve_requests: Arc::clone(&resolve_requests),
        };
        let cache = BackgroundRegionCache::without_worker(RegionCache::new(Loader));
        let resolving_locks = ResolvingLocks::default();
        let authority_id = next_read_authority_id();
        let runtime = SharedReadRuntime::from_shared_authorities(
            client,
            cache.open_lease().expect("the test opens a cache lease"),
            resolving_locks,
            authority_id,
        )
        .expect("the test runtime shares the resolver opener");

        let result = resolve_optimistic_locks(
            &runtime,
            &[OptimisticLock {
                key: b"large-secondary".to_vec(),
                primary: b"primary".to_vec(),
                txn_id: 100,
                ttl_ms: 0,
                txn_size: u64::MAX,
                lock_type: 2,
                min_commit_ts: 0,
                use_async_commit: false,
                secondaries: Vec::new(),
            }],
            200,
            &KvrpcContext {
                request_source: "foreground-read".to_owned(),
                ..KvrpcContext::default()
            },
            &UnaryCallContext::with_timeout(Duration::from_secs(1)),
            &Arc::new(FixedTimestampSource::new(1)),
            true,
        )
        .expect("the read returns once the large transaction status is known");
        assert_eq!(result.access_locks, vec![100]);

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while resolve_calls.load(Ordering::Relaxed) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(resolve_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            *resolve_requests.lock().unwrap(),
            vec![(1, Vec::new(), tidb_config::kerneltype::is_next_gen())]
        );
        futures::executor::block_on(runtime.native_lock_resolver_context().close());
    }

    #[test]
    fn large_read_cleanup_falls_back_to_a_region_scan_without_a_pool() {
        let resolve_calls = Arc::new(AtomicUsize::new(0));
        let resolve_requests = Arc::new(Mutex::new(Vec::new()));
        let runtime = SharedReadRuntime::new_injected(
            Client {
                check_calls: Arc::new(AtomicUsize::new(0)),
                resolve_calls: Arc::clone(&resolve_calls),
                resolve_requests: Arc::clone(&resolve_requests),
            },
            RegionCache::new(Loader),
        );

        let result = resolve_optimistic_locks(
            &runtime,
            &[OptimisticLock {
                key: b"large-secondary".to_vec(),
                primary: b"primary".to_vec(),
                txn_id: 100,
                ttl_ms: 0,
                txn_size: u64::MAX,
                lock_type: 2,
                min_commit_ts: 0,
                use_async_commit: false,
                secondaries: Vec::new(),
            }],
            200,
            &KvrpcContext::default(),
            &UnaryCallContext::with_timeout(Duration::from_secs(1)),
            &Arc::new(FixedTimestampSource::new(1)),
            true,
        )
        .expect("a runtime without the async pool resolves the lock inline");
        assert_eq!(result.access_locks, vec![100]);
        assert_eq!(resolve_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            *resolve_requests.lock().unwrap(),
            vec![(1, Vec::new(), tidb_config::kerneltype::is_next_gen())]
        );
    }

    #[test]
    fn small_read_cleanup_schedules_one_task_per_region() {
        let resolve_calls = Arc::new(AtomicUsize::new(0));
        let resolve_requests = Arc::new(Mutex::new(Vec::new()));
        let client = Client {
            check_calls: Arc::new(AtomicUsize::new(0)),
            resolve_calls: Arc::clone(&resolve_calls),
            resolve_requests: Arc::clone(&resolve_requests),
        };
        let cache = BackgroundRegionCache::without_worker(RegionCache::new(Loader));
        let resolving_locks = ResolvingLocks::default();
        let authority_id = next_read_authority_id();
        let runtime = SharedReadRuntime::from_shared_authorities(
            client,
            cache.open_lease().expect("the test opens a cache lease"),
            resolving_locks,
            authority_id,
        )
        .expect("the test runtime shares the resolver opener");

        let locks = [b"a".as_slice(), b"z".as_slice()].map(|key| OptimisticLock {
            key: key.to_vec(),
            primary: b"primary".to_vec(),
            txn_id: 100,
            ttl_ms: 0,
            txn_size: 1,
            lock_type: 2,
            min_commit_ts: 0,
            use_async_commit: false,
            secondaries: Vec::new(),
        });
        let result = resolve_optimistic_locks(
            &runtime,
            &locks,
            200,
            &KvrpcContext {
                request_source: "foreground-read".to_owned(),
                ..KvrpcContext::default()
            },
            &UnaryCallContext::with_timeout(Duration::from_secs(1)),
            &Arc::new(FixedTimestampSource::new(1)),
            true,
        )
        .expect("the read returns before the grouped cleanup finishes");
        assert_eq!(result.access_locks, vec![100, 100]);

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while resolve_calls.load(Ordering::Relaxed) < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(resolve_calls.load(Ordering::Relaxed), 2);
        let mut requests = resolve_requests.lock().unwrap().clone();
        requests.sort_by_key(|(region_id, _, _)| *region_id);
        assert_eq!(
            requests,
            vec![
                (1, vec![b"a".to_vec()], false),
                (2, vec![b"z".to_vec()], false),
            ]
        );

        futures::executor::block_on(runtime.native_lock_resolver_context().close());
    }
}
