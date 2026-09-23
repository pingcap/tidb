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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::lock::async_resolve::{AsyncLockResolveTask, AsyncResolvePool};
use crate::lock::ResolvedTxnStatus;
use crate::region::{
    BackgroundRegionCache, BackgroundRegionCacheError, BackgroundRegionCacheOwner, KeyRange,
    LeaderRequest, RegionCache, RegionLoader, RegionLocation, RegionQueryLoader,
    RegionRecoveryError, RegionRecoveryLoader, RegionRouteError, RequestSelection, RequestSelector,
    StoreLiveness, StoreLivenessProbe,
};
use crate::{DirectUnaryClient, DEFAULT_STORE_LIVENESS_TIMEOUT};
use tidb_proto::KvrpcCheckTxnStatusResponse;

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

#[derive(Default)]
struct ResolvingLocks {
    next_token: u64,
    entries: std::collections::HashMap<u64, Vec<ResolvingLock>>,
}

const RESOLVED_TXN_STATUS_CACHE_SIZE: usize = 2048;

#[derive(Default)]
struct ResolvedTxnStatusCache {
    entries: HashMap<u64, (ResolvedTxnStatus, KvrpcCheckTxnStatusResponse)>,
    insertion_order: VecDeque<u64>,
}

impl ResolvedTxnStatusCache {
    fn get(&self, txn_id: u64) -> Option<(ResolvedTxnStatus, KvrpcCheckTxnStatusResponse)> {
        self.entries
            .get(&txn_id)
            .map(|(status, response)| (*status, response.clone()))
    }

    fn insert(
        &mut self,
        txn_id: u64,
        status: ResolvedTxnStatus,
        response: KvrpcCheckTxnStatusResponse,
    ) {
        if let Some((saved_status, _)) = self.entries.get(&txn_id) {
            assert_eq!(
                *saved_status, status,
                "a transaction's determined lock status changed"
            );
            return;
        }

        self.entries.insert(txn_id, (status, response));
        self.insertion_order.push_back(txn_id);
        if self.entries.len() > RESOLVED_TXN_STATUS_CACHE_SIZE {
            if let Some(oldest_txn_id) = self.insertion_order.pop_front() {
                self.entries.remove(&oldest_txn_id);
            }
        }
    }
}

/// Scope guard for client-go's `RecordResolvingLocks` / `ResolveLocksDone`.
pub struct ResolvingLocksGuard {
    registry: Arc<Mutex<ResolvingLocks>>,
    token: u64,
}

impl ResolvingLocksGuard {
    /// Go UpdateResolvingLocks replaces this worker's current observation while
    /// preserving the same RecordResolvingLocks / ResolveLocksDone token.
    pub fn update(&mut self, locks: impl Iterator<Item = ResolvingLock>) {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .insert(self.token, locks.collect());
    }
}

impl Drop for ResolvingLocksGuard {
    fn drop(&mut self) {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .remove(&self.token);
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
    async_resolve_pool: Option<Arc<AsyncResolvePool>>,
    cluster_id: u64,
    authority_id: u64,
}

/// Cloneable session-opening capability without process shutdown authority.
pub struct SharedReadOpener<C, L> {
    client: C,
    region_cache: BackgroundRegionCache<L>,
    resolving_locks: Arc<Mutex<ResolvingLocks>>,
    async_resolve_pool: Option<Arc<AsyncResolvePool>>,
    resolved_txn_statuses: Arc<Mutex<ResolvedTxnStatusCache>>,
    authority_id: u64,
}

impl<C: Clone, L> Clone for SharedReadOpener<C, L> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            region_cache: self.region_cache.clone_opener(),
            resolving_locks: Arc::clone(&self.resolving_locks),
            async_resolve_pool: self.async_resolve_pool.clone(),
            resolved_txn_statuses: Arc::clone(&self.resolved_txn_statuses),
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
            resolving_locks: Arc::new(Mutex::new(ResolvingLocks::default())),
            async_resolve_pool: None,
            resolved_txn_statuses: Arc::new(Mutex::new(ResolvedTxnStatusCache::default())),
            authority_id,
        };
        Self {
            opener,
            region_cache,
            async_resolve_pool: None,
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
        if let Some(pool) = &self.async_resolve_pool {
            pool.close_and_wait();
        }
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
        let mut authority = Self::start(client, region_cache)?;
        authority.enable_async_lock_resolver()?;
        Ok(authority)
    }

    fn enable_async_lock_resolver(&mut self) -> Result<(), BackgroundRegionCacheError>
    where
        C: crate::lock::LockRecoveryClient + Send + 'static,
        L: crate::region::RegionRecoveryLoader,
    {
        let pool = crate::lock::async_resolve_pool(self.opener.clone());
        self.opener.async_resolve_pool = Some(Arc::clone(&pool));
        self.async_resolve_pool = Some(pool);
        Ok(())
    }
}

impl<C, L> Drop for SharedReadAuthority<C, L> {
    fn drop(&mut self) {
        if let Some(pool) = &self.async_resolve_pool {
            pool.close_and_wait();
        }
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
        let mut authority = Self::from_started(client, region_cache, cluster_id);
        authority.enable_async_lock_resolver()?;
        Ok(authority)
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
            Arc::clone(&self.resolving_locks),
            self.async_resolve_pool.clone(),
            Arc::clone(&self.resolved_txn_statuses),
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
    resolving_locks: Arc<Mutex<ResolvingLocks>>,
    async_resolve_pool: Option<Arc<AsyncResolvePool>>,
    resolved_txn_statuses: Arc<Mutex<ResolvedTxnStatusCache>>,
    cluster_id: u64,
    authority_id: u64,
}

impl<C, L> Clone for SharedReadRuntime<C, L> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            region_cache: self.region_cache.clone(),
            resolving_locks: Arc::clone(&self.resolving_locks),
            async_resolve_pool: self.async_resolve_pool.clone(),
            resolved_txn_statuses: Arc::clone(&self.resolved_txn_statuses),
            cluster_id: self.cluster_id,
            authority_id: self.authority_id,
        }
    }
}

impl<C, L: RegionLoader> SharedReadRuntime<C, L> {
    /// Creates the synchronized cache authority for an injected runtime.
    #[must_use]
    pub fn new_injected(client: C, region_cache: RegionCache<L>) -> Self {
        let cluster_id = region_cache.cluster_id();
        Self {
            client: Arc::new(Mutex::new(client)),
            region_cache: BackgroundRegionCache::without_worker(region_cache),
            resolving_locks: Arc::new(Mutex::new(ResolvingLocks::default())),
            async_resolve_pool: None,
            resolved_txn_statuses: Arc::new(Mutex::new(ResolvedTxnStatusCache::default())),
            cluster_id,
            authority_id: next_read_authority_id(),
        }
    }

    /// Creates a worker-local session over already-owned process authorities.
    fn from_shared_authorities(
        client: C,
        region_cache: BackgroundRegionCache<L>,
        resolving_locks: Arc<Mutex<ResolvingLocks>>,
        async_resolve_pool: Option<Arc<AsyncResolvePool>>,
        resolved_txn_statuses: Arc<Mutex<ResolvedTxnStatusCache>>,
        authority_id: u64,
    ) -> Result<Self, BackgroundRegionCacheError> {
        let cluster_id = region_cache.with_cache_read(|cache| cache.cluster_id())?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            region_cache,
            resolving_locks,
            async_resolve_pool,
            resolved_txn_statuses,
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
            resolving_locks: Arc::clone(&self.resolving_locks),
            async_resolve_pool: self.async_resolve_pool.clone(),
            resolved_txn_statuses: Arc::clone(&self.resolved_txn_statuses),
            cluster_id: self.cluster_id,
            authority_id: self.authority_id,
        }
    }

    /// Schedules detached read cleanup when this runtime belongs to a
    /// production lock-resolver authority.
    pub(crate) fn try_schedule_async_resolve(&self, task: AsyncLockResolveTask) -> bool {
        self.async_resolve_pool
            .as_ref()
            .is_some_and(|pool| pool.try_spawn(task))
    }

    /// Returns a cached determined CheckTxnStatus response for this resolver.
    pub(crate) fn cached_lock_status(
        &self,
        txn_id: u64,
    ) -> Option<(ResolvedTxnStatus, KvrpcCheckTxnStatusResponse)> {
        self.resolved_txn_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(txn_id)
    }

    /// Saves one determined transaction status using client-go's FIFO cache.
    pub(crate) fn cache_lock_status(
        &self,
        txn_id: u64,
        status: ResolvedTxnStatus,
        response: KvrpcCheckTxnStatusResponse,
    ) {
        self.resolved_txn_statuses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(txn_id, status, response);
    }

    /// Records one resolve attempt until the returned guard is dropped.
    pub fn record_resolving_locks(
        &self,
        txn_id: u64,
        locks: impl IntoIterator<Item = ResolvingLock>,
    ) -> ResolvingLocksGuard {
        let mut registry = self
            .resolving_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let token = registry.next_token;
        registry.next_token = registry.next_token.wrapping_add(1);
        registry.entries.insert(
            token,
            locks
                .into_iter()
                .map(|mut lock| {
                    lock.txn_id = txn_id;
                    lock
                })
                .collect(),
        );
        drop(registry);
        ResolvingLocksGuard {
            registry: Arc::clone(&self.resolving_locks),
            token,
        }
    }

    /// Returns a point-in-time copy of all locks currently being resolved.
    #[must_use]
    pub fn resolving_locks(&self) -> Vec<ResolvingLock> {
        self.resolving_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .values()
            .flatten()
            .cloned()
            .collect()
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
        ResolvedTxnStatus,
    };
    use crate::region::{
        Peer, PeerRole, RegionEpoch, RegionLoadError, RegionMetadata, RegionRecoveryLoader,
        RegionVerId, Store, StoreMetadata,
    };
    use crate::rpc::{DirectUnaryClientError, UnaryCallContext, UnaryCancellation};
    use tidb_proto::{
        KvrpcCheckSecondaryLocksRequest, KvrpcCheckSecondaryLocksResponse,
        KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcContext,
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

    #[derive(Clone)]
    struct Client {
        check_calls: Arc<AtomicUsize>,
        resolve_calls: Arc<AtomicUsize>,
        resolve_requests: Arc<Mutex<Vec<(u64, Vec<Vec<u8>>)>>>,
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
            self.resolve_requests
                .lock()
                .unwrap()
                .push((context.region_id, request.keys.clone()));
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
                &FixedTimestampSource::new(1),
                true,
            )
            .expect("both reads resolve using the same determined transaction status");
            assert_eq!(result.statuses, vec![ResolvedTxnStatus::Committed(150)]);
        }

        assert_eq!(check_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn resolved_txn_status_cache_evicts_the_oldest_entry_at_capacity() {
        let mut cache = ResolvedTxnStatusCache::default();
        for txn_id in 0..=(RESOLVED_TXN_STATUS_CACHE_SIZE as u64) {
            cache.insert(
                txn_id,
                ResolvedTxnStatus::Committed(txn_id + 1),
                KvrpcCheckTxnStatusResponse {
                    commit_version: txn_id + 1,
                    ..KvrpcCheckTxnStatusResponse::default()
                },
            );
        }

        assert!(cache.get(0).is_none());
        assert_eq!(
            cache.get(RESOLVED_TXN_STATUS_CACHE_SIZE as u64),
            Some((
                ResolvedTxnStatus::Committed(RESOLVED_TXN_STATUS_CACHE_SIZE as u64 + 1),
                KvrpcCheckTxnStatusResponse {
                    commit_version: RESOLVED_TXN_STATUS_CACHE_SIZE as u64 + 1,
                    ..KvrpcCheckTxnStatusResponse::default()
                }
            ))
        );
    }

    #[test]
    fn small_read_cleanup_is_detached_and_keeps_request_source() {
        let (task_tx, task_rx) = mpsc::channel();
        let pool = AsyncResolvePool::new(move |task, cancellation| {
            task_tx
                .send((task, cancellation.clone()))
                .expect("the test receives the scheduled cleanup");
            while !cancellation.is_cancelled() {
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let resolve_calls = Arc::new(AtomicUsize::new(0));
        let resolve_requests = Arc::new(Mutex::new(Vec::new()));
        let mut runtime = SharedReadRuntime::new_injected(
            Client {
                check_calls: Arc::new(AtomicUsize::new(0)),
                resolve_calls: Arc::clone(&resolve_calls),
                resolve_requests,
            },
            RegionCache::new(Loader),
        );
        runtime.async_resolve_pool = Some(Arc::clone(&pool));
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
                ..KvrpcContext::default()
            },
            &UnaryCallContext::new(Duration::from_secs(1), caller_cancellation.clone()),
            &FixedTimestampSource::new(1),
            true,
        )
        .expect("the read returns its resolved status without waiting for cleanup");
        assert_eq!(result.statuses, vec![ResolvedTxnStatus::Committed(150)]);
        assert_eq!(result.access_locks, vec![100]);

        let (task, background_cancellation) = task_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the small-lock cleanup is scheduled");
        assert_eq!(task.txn_id, 100);
        assert_eq!(task.commit_version, 150);
        assert_eq!(task.keys, vec![b"secondary".to_vec()]);
        assert_eq!(task.request_source, "foreground-read");
        assert!(task.include_keys);
        assert!(task.schedule_regions);
        assert!(!background_cancellation.is_cancelled());
        caller_cancellation.cancel();
        assert!(!background_cancellation.is_cancelled());
        assert_eq!(resolve_calls.load(Ordering::Relaxed), 0);

        pool.close_and_wait();
        assert!(background_cancellation.is_cancelled());
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
        let resolving_locks = Arc::new(Mutex::new(ResolvingLocks::default()));
        let resolved_txn_statuses = Arc::new(Mutex::new(ResolvedTxnStatusCache::default()));
        let authority_id = next_read_authority_id();
        let opener = SharedReadOpener {
            client: client.clone(),
            region_cache: cache.clone_opener(),
            resolving_locks: Arc::clone(&resolving_locks),
            async_resolve_pool: None,
            resolved_txn_statuses: Arc::clone(&resolved_txn_statuses),
            authority_id,
        };
        let pool = crate::lock::async_resolve_pool(opener.clone());
        let runtime = SharedReadRuntime::from_shared_authorities(
            client,
            cache.open_lease().expect("the test opens a cache lease"),
            resolving_locks,
            Some(Arc::clone(&pool)),
            resolved_txn_statuses,
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
            &FixedTimestampSource::new(1),
            true,
        )
        .expect("the read returns once the large transaction status is known");
        assert_eq!(result.statuses, vec![ResolvedTxnStatus::Committed(150)]);

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while resolve_calls.load(Ordering::Relaxed) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(resolve_calls.load(Ordering::Relaxed), 1);
        assert_eq!(*resolve_requests.lock().unwrap(), vec![(1, Vec::new())]);
        pool.close_and_wait();
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
            &FixedTimestampSource::new(1),
            true,
        )
        .expect("a runtime without the async pool resolves the lock inline");
        assert_eq!(result.statuses, vec![ResolvedTxnStatus::Committed(150)]);
        assert_eq!(resolve_calls.load(Ordering::Relaxed), 1);
        assert_eq!(*resolve_requests.lock().unwrap(), vec![(1, Vec::new())]);
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
        let resolving_locks = Arc::new(Mutex::new(ResolvingLocks::default()));
        let resolved_txn_statuses = Arc::new(Mutex::new(ResolvedTxnStatusCache::default()));
        let authority_id = next_read_authority_id();
        let opener = SharedReadOpener {
            client: client.clone(),
            region_cache: cache.clone_opener(),
            resolving_locks: Arc::clone(&resolving_locks),
            async_resolve_pool: None,
            resolved_txn_statuses: Arc::clone(&resolved_txn_statuses),
            authority_id,
        };
        let pool = crate::lock::async_resolve_pool(opener.clone());
        let runtime = SharedReadRuntime::from_shared_authorities(
            client,
            cache.open_lease().expect("the test opens a cache lease"),
            resolving_locks,
            Some(Arc::clone(&pool)),
            resolved_txn_statuses,
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
            &FixedTimestampSource::new(1),
            true,
        )
        .expect("the read returns before the grouped cleanup finishes");
        assert_eq!(result.statuses.len(), 2);

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while resolve_calls.load(Ordering::Relaxed) < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(resolve_calls.load(Ordering::Relaxed), 2);
        let mut requests = resolve_requests.lock().unwrap().clone();
        requests.sort_by_key(|(region_id, _)| *region_id);
        assert_eq!(
            requests,
            vec![(1, vec![b"a".to_vec()]), (2, vec![b"z".to_vec()]),]
        );

        pool.close_and_wait();
    }
}
