// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::debug;
use rand::Rng;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::async_util::Cancellation;
use crate::common::Error;
use crate::kv::ReplicaReadType;
use crate::locate::{
    HealthStatusDetail, MixedReplicaSelection, ReplicaCandidate, ReplicaSelectorState,
    StoreHealthStatus,
};
use crate::pd::Cluster;
use crate::pd::RegionScanOptions;
use crate::pd::RetryClient;
use crate::pd::RetryClientTrait;
use crate::proto::kvrpcpb;
use crate::proto::metapb::Store;
use crate::proto::metapb::{self};
use crate::proto::pdpb;
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::retry::{RetryBackoffer, BO_PD_RPC};
use crate::store::{ClientEventListener, EndpointType};
use crate::Key;
use crate::Result;

const MAX_RETRY_WAITING_CONCURRENT_REQUEST: usize = 4;
const REGION_CACHE_TTL_SECS: i64 = 600;
const REGION_CACHE_TTL_JITTER_SECS: i64 = 60;
const CLEAN_CACHE_INTERVAL: Duration = Duration::from_secs(1);
const CLEAN_REGION_NUM_PER_ROUND: usize = 50;
const DEFAULT_REGIONS_PER_BATCH: usize = 128;
const MAX_RANGES_PER_BATCH: usize = 16 * DEFAULT_REGIONS_PER_BATCH;
const NEED_RELOAD_ON_ACCESS: u8 = 1 << 0;
const NEED_EXPIRE_AFTER_TTL: u8 = 1 << 1;
const NEED_DELAYED_RELOAD_PENDING: u8 = 1 << 2;
const NEED_DELAYED_RELOAD_READY: u8 = 1 << 3;

/// Cache-local state that client-go keeps beside immutable PD region metadata.
#[derive(Clone, Debug)]
struct CachedRegion {
    region: RegionWithLeader,
    /// Snapshot of every peer store's failure epoch at the time this region
    /// entered the cache. A later failure on any of those stores invalidates
    /// only regions that still carry this old snapshot.
    store_epochs: HashMap<StoreId, u32>,
    /// Epoch seconds; client-go treats `now == ttl` as still live.
    ttl: i64,
    /// Source `Region.syncFlags`. Cache mutation is serialized by the Rust
    /// cache lock, so this does not need client-go's atomic representation.
    sync_flags: u8,
    tiflash_cursor: Arc<AtomicUsize>,
}

impl CachedRegion {
    fn new(region: RegionWithLeader, store_epochs: HashMap<StoreId, u32>, now: i64) -> Self {
        let sync_flags = if region.down_peers.is_empty() {
            0
        } else {
            NEED_EXPIRE_AFTER_TTL
        };
        Self {
            region,
            store_epochs,
            ttl: next_region_cache_ttl(now),
            sync_flags,
            tiflash_cursor: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn has_sync_flags(&self, flags: u8) -> bool {
        self.sync_flags & flags != 0
    }

    fn set_sync_flags(&mut self, flags: u8) {
        self.sync_flags |= flags;
    }

    fn take_sync_flags(&mut self, flags: u8) -> u8 {
        let taken = self.sync_flags & flags;
        self.sync_flags &= !flags;
        taken
    }

    /// Source `Region.checkRegionCacheTTL`, serialized by the cache lock.
    fn check_ttl(&mut self, now: i64) -> bool {
        if now > self.ttl {
            return false;
        }
        if !self.has_sync_flags(NEED_EXPIRE_AFTER_TTL) && self.ttl <= now + REGION_CACHE_TTL_SECS {
            self.ttl = next_region_cache_ttl(now);
        }
        true
    }
}

/// The cache-local outcome of client-go's `GetTiFlashRPCContext` peer walk.
/// Transport connection/address failures remain owned by `PdRpcClient`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TiFlashSelectionError {
    CachedRegionMissing,
    CacheExpired,
    NoTiFlashPeer,
    AllStoresFiltered,
}

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs() as i64
}

fn next_region_cache_ttl(now: i64) -> i64 {
    let jitter = rand::thread_rng().gen_range(0..REGION_CACHE_TTL_JITTER_SECS);
    now + REGION_CACHE_TTL_SECS + jitter
}

/// Source `regionsHaveGapInRanges`. A response that reaches its positive
/// limit may omit later ranges; the returned prefix must still be gap-free.
fn regions_have_gap_in_ranges(
    ranges: &[pdpb::KeyRange],
    regions: &[RegionWithLeader],
    limit: Option<usize>,
) -> bool {
    if ranges.is_empty() {
        return false;
    }
    if regions.is_empty() {
        return true;
    }

    let mut range_index = 0;
    let mut check_key = ranges[0].start_key.clone();
    for region in regions {
        if region.region.start_key > check_key {
            return true;
        }
        if region.region.end_key.is_empty() {
            return false;
        }
        check_key.clone_from(&region.region.end_key);
        while !ranges[range_index].end_key.is_empty() && check_key >= ranges[range_index].end_key {
            range_index += 1;
            if range_index == ranges.len() {
                return false;
            }
        }
        if check_key < ranges[range_index].start_key {
            check_key.clone_from(&ranges[range_index].start_key);
        }
    }

    if limit.is_some_and(|limit| limit > 0 && regions.len() == limit) {
        return false;
    }
    if range_index + 1 < ranges.len() {
        return true;
    }
    if check_key.is_empty() {
        false
    } else if ranges[range_index].end_key.is_empty() {
        true
    } else {
        check_key < ranges[range_index].end_key
    }
}

fn ranges_after_key(mut ranges: Vec<pdpb::KeyRange>, split_key: &[u8]) -> Vec<pdpb::KeyRange> {
    let Some(last) = ranges.last() else {
        return Vec::new();
    };
    if split_key.is_empty() || (!last.end_key.is_empty() && split_key >= last.end_key.as_slice()) {
        return Vec::new();
    }

    let first_remaining = ranges.partition_point(|range| {
        !range.end_key.is_empty() && range.end_key.as_slice() <= split_key
    });
    ranges.drain(..first_remaining);
    if ranges
        .first()
        .is_some_and(|range| split_key > range.start_key.as_slice())
    {
        ranges[0].start_key = split_key.to_vec();
    }
    ranges
}

fn contains_by_end(region: &RegionWithLeader, key: &[u8]) -> bool {
    if key.is_empty() {
        return region.region.end_key.is_empty();
    }
    region.region.start_key.as_slice() < key
        && (region.region.end_key.is_empty() || key <= region.region.end_key.as_slice())
}

fn is_unimplemented_batch_scan(error: &Error) -> bool {
    matches!(error, Error::Unimplemented)
        || matches!(error, Error::GrpcAPI(status) if status.code() == tonic::Code::Unimplemented)
}

struct BatchLocateRegionMerger {
    last_end_key: Option<Vec<u8>>,
    cached_index: usize,
    cached_regions: Vec<RegionWithLeader>,
    merged_regions: Vec<RegionWithLeader>,
}

impl BatchLocateRegionMerger {
    fn new(cached_regions: Vec<RegionWithLeader>, size_hint: usize) -> Self {
        Self {
            last_end_key: None,
            cached_index: 0,
            cached_regions,
            merged_regions: Vec::with_capacity(size_hint),
        }
    }

    fn append_region(&mut self, loaded_region: RegionWithLeader) {
        let start_key = loaded_region.region.start_key.as_slice();
        if start_key.is_empty()
            || self
                .last_end_key
                .as_ref()
                .is_some_and(|last_end| last_end.as_slice() >= start_key)
        {
            self.merged_regions.push(loaded_region);
            self.record_loaded_end();
            return;
        }

        while self.cached_index < self.cached_regions.len() {
            let cached = &self.cached_regions[self.cached_index];
            if self
                .last_end_key
                .as_ref()
                .is_some_and(|last_end| last_end.as_slice() >= cached.region.end_key.as_slice())
            {
                self.cached_index += 1;
                continue;
            }
            if cached.region.start_key >= loaded_region.region.start_key {
                break;
            }
            self.merged_regions.push(cached.clone());
            self.cached_index += 1;
        }
        self.merged_regions.push(loaded_region);
        self.record_loaded_end();
    }

    fn record_loaded_end(&mut self) {
        let end_key = self
            .merged_regions
            .last()
            .expect("a loaded region was just appended")
            .region
            .end_key
            .clone();
        if end_key.is_empty() {
            self.cached_index = self.cached_regions.len();
        } else {
            self.last_end_key = Some(end_key);
        }
    }

    fn build(mut self) -> Vec<RegionWithLeader> {
        while self.cached_index < self.cached_regions.len() {
            let cached = &self.cached_regions[self.cached_index];
            if !self
                .last_end_key
                .as_ref()
                .is_some_and(|last_end| last_end.as_slice() >= cached.region.end_key.as_slice())
            {
                self.merged_regions.push(cached.clone());
            }
            self.cached_index += 1;
        }
        self.merged_regions
    }
}

struct RegionCacheMap {
    /// RegionVerID -> Region. It stores the concrete region caches.
    /// RegionVerID is the unique identifer of a region *across time*.
    // TODO: does it need TTL?
    ver_id_to_region: HashMap<RegionVerId, CachedRegion>,
    /// Start_key -> RegionVerID
    ///
    /// Invariant: there are no intersecting regions in the map at any time.
    key_to_ver_id: BTreeMap<Key, RegionVerId>,
    /// RegionID -> RegionVerID. Note: regions with identical ID doesn't necessarily
    /// mean they are the same, they can be different regions across time.
    id_to_ver_id: HashMap<RegionId, RegionVerId>,
    /// We don't want to spawn multiple queries querying a same region id. If a
    /// request is on its way, others will wait for its completion.
    on_my_way_id: HashMap<RegionId, Arc<Notify>>,
}

struct CachedStore {
    meta: Store,
    health_status: Arc<StoreHealthStatus>,
    epoch: AtomicU32,
    liveness: AtomicU8,
    health_check_running: AtomicBool,
    load_stats: StdMutex<Option<StoreLoadStats>>,
}

#[derive(Clone, Copy, Debug)]
struct StoreLoadStats {
    estimated_wait: Duration,
    updated_at: Instant,
}

fn is_down_peer(region: &RegionWithLeader, candidate: &metapb::Peer) -> bool {
    region.down_peers.iter().any(|down| {
        down.peer
            .as_ref()
            .is_some_and(|peer| peer.id == candidate.id && peer.store_id == candidate.store_id)
    })
}

fn is_unroutable_peer(region: &RegionWithLeader, candidate: &metapb::Peer) -> bool {
    is_down_peer(region, candidate)
        || (candidate.is_witness
            && region
                .leader
                .as_ref()
                .is_none_or(|leader| leader.id != candidate.id))
}

impl CachedStore {
    fn new(meta: Store) -> Self {
        Self {
            meta,
            health_status: Arc::new(StoreHealthStatus::default()),
            epoch: AtomicU32::new(0),
            liveness: AtomicU8::new(StoreLiveness::Reachable as u8),
            health_check_running: AtomicBool::new(false),
            load_stats: StdMutex::new(None),
        }
    }

    fn update_metadata(&mut self, meta: Store) {
        self.meta = meta;
    }

    fn update_server_load(&self, estimated_wait_ms: u32, now: Instant) {
        *self.load_stats.lock().unwrap() = Some(StoreLoadStats {
            estimated_wait: Duration::from_millis(u64::from(estimated_wait_ms)),
            updated_at: now,
        });
    }

    /// client-go's optimistic estimate subtracts elapsed wall time from the
    /// last TiKV-provided server queue delay.
    fn estimated_wait(&self, now: Instant) -> Duration {
        let Some(stats) = *self.load_stats.lock().unwrap() else {
            return Duration::ZERO;
        };
        stats
            .estimated_wait
            .saturating_sub(now.saturating_duration_since(stats.updated_at))
    }
}

/// Cached source `livenessState`; new stores begin reachable and only a
/// request/health-check transition changes that state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum StoreLiveness {
    Reachable = 0,
    Unreachable = 1,
    Unknown = 2,
}

impl StoreLiveness {
    fn from_encoded(value: u8) -> Self {
        match value {
            0 => Self::Reachable,
            1 => Self::Unreachable,
            2 => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

impl std::fmt::Display for StoreLiveness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reachable => formatter.write_str("reachable"),
            Self::Unreachable => formatter.write_str("unreachable"),
            Self::Unknown => formatter.write_str("unknown"),
        }
    }
}

impl RegionCacheMap {
    fn new() -> RegionCacheMap {
        RegionCacheMap {
            ver_id_to_region: HashMap::new(),
            key_to_ver_id: BTreeMap::new(),
            id_to_ver_id: HashMap::new(),
            on_my_way_id: HashMap::new(),
        }
    }
}

pub struct RegionCache<Client = RetryClient<Cluster>> {
    region_cache: RwLock<RegionCacheMap>,
    store_cache: StdRwLock<HashMap<StoreId, CachedStore>>,
    bucket_refreshes: StdMutex<HashSet<RegionId>>,
    gc_cursor: StdMutex<Option<Key>>,
    background_cancellation: Cancellation,
    background_task: StdMutex<Option<JoinHandle<()>>>,
    inner_client: Arc<Client>,
}

impl<Client> RegionCache<Client> {
    pub fn new(inner_client: Arc<Client>) -> RegionCache<Client> {
        RegionCache {
            region_cache: RwLock::new(RegionCacheMap::new()),
            store_cache: StdRwLock::new(HashMap::new()),
            bucket_refreshes: StdMutex::new(HashSet::new()),
            gc_cursor: StdMutex::new(None),
            background_cancellation: Cancellation::default(),
            background_task: StdMutex::new(None),
            inner_client,
        }
    }
}

impl<C: Send + Sync> RegionCache<C> {
    pub(crate) fn start_background_gc(self: &Arc<Self>)
    where
        C: 'static,
    {
        let mut task = self.background_task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let cache = Arc::downgrade(self);
        let cancellation = self.background_cancellation.child();
        *task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(CLEAN_CACHE_INTERVAL) => {}
                }
                let Some(cache) = cache.upgrade() else {
                    return;
                };
                cache
                    .gc_round_at(now_epoch_secs(), CLEAN_REGION_NUM_PER_ROUND)
                    .await;
            }
        }));
    }

    pub(crate) async fn close_background_task(&self) {
        self.background_cancellation.cancel();
        let task = self.background_task.lock().unwrap().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// One bounded source `gcRoundFunc` pass. The cursor retains the first
    /// unscanned start key for the next pass and resets after reaching the end.
    /// Live regions backed by a stale or unreachable store stop renewing and
    /// are removed only after their existing TTL expires.
    pub(crate) async fn gc_round_at(&self, now: i64, limit: usize) -> (usize, usize, bool) {
        let limit = limit.max(1);
        let cursor = self.gc_cursor.lock().unwrap().clone().unwrap_or_default();
        let stores = self
            .store_cache
            .read()
            .unwrap()
            .iter()
            .map(|(id, store)| {
                (
                    *id,
                    (
                        store.epoch.load(Ordering::Acquire),
                        StoreLiveness::from_encoded(store.liveness.load(Ordering::Acquire)),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut cache = self.region_cache.write().await;
        let candidates = cache
            .key_to_ver_id
            .range(cursor..)
            .take(limit + 1)
            .map(|(key, ver_id)| (key.clone(), ver_id.clone()))
            .collect::<Vec<_>>();
        let has_more = candidates.len() > limit;
        let scanned = candidates.len().min(limit);
        let next_cursor = has_more.then(|| candidates[limit].0.clone());
        let mut removed = 0;

        for (start_key, ver_id) in candidates.into_iter().take(limit) {
            let expired = cache
                .ver_id_to_region
                .get(&ver_id)
                .is_some_and(|region| now > region.ttl);
            if expired {
                if let Some(region) = cache.ver_id_to_region.remove(&ver_id) {
                    cache.key_to_ver_id.remove(&start_key);
                    if cache.id_to_ver_id.get(&region.region.id()) == Some(&ver_id) {
                        cache.id_to_ver_id.remove(&region.region.id());
                    }
                    removed += 1;
                }
                continue;
            }
            if let Some(region) = cache.ver_id_to_region.get_mut(&ver_id) {
                if region.has_sync_flags(NEED_DELAYED_RELOAD_READY) {
                    continue;
                }
                if region.has_sync_flags(NEED_DELAYED_RELOAD_PENDING) {
                    region.set_sync_flags(NEED_DELAYED_RELOAD_READY);
                    continue;
                }
                if !region.has_sync_flags(NEED_EXPIRE_AFTER_TTL)
                    && region
                        .store_epochs
                        .iter()
                        .any(|(store_id, expected_epoch)| {
                            stores.get(store_id).is_some_and(|(epoch, liveness)| {
                                epoch != expected_epoch || *liveness != StoreLiveness::Reachable
                            })
                        })
                {
                    region.set_sync_flags(NEED_EXPIRE_AFTER_TTL);
                }
            }
        }
        drop(cache);
        *self.gc_cursor.lock().unwrap() = next_cursor;
        (scanned, removed, has_more)
    }
}

impl<C: RetryClientTrait + Send + Sync> RegionCache<C> {
    pub(crate) fn start_background_refresh(self: &Arc<Self>, interval: Duration)
    where
        C: 'static,
    {
        if interval.is_zero() {
            return;
        }
        let mut task = self.background_task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let cache = Arc::downgrade(self);
        let cancellation = self.background_cancellation.child();
        *task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
                let Some(cache) = cache.upgrade() else {
                    return;
                };
                let max_sleep_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
                let mut backoffer = RetryBackoffer::new(cancellation.child(), max_sleep_ms);
                if let Err(error) = cache.refresh_region_index(&mut backoffer).await {
                    debug!("periodic region-cache refresh failed: {error}");
                }
            }
        }));
    }

    // Retrieve cache entry by key. If there's no entry, query PD and update cache.
    pub async fn get_region_by_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let mut region_cache_guard = self.region_cache.write().await;
        let res = {
            region_cache_guard
                .key_to_ver_id
                .range::<Key, _>(..=key)
                .next_back()
                .map(|(x, y)| (x.clone(), y.clone()))
        };

        let reload = if let Some((_, candidate_region_ver_id)) = res {
            let region = region_cache_guard
                .ver_id_to_region
                .get_mut(&candidate_region_ver_id)
                .unwrap();

            if region.region.contains(key) && region.check_ttl(now_epoch_secs()) {
                let flags =
                    region.take_sync_flags(NEED_RELOAD_ON_ACCESS | NEED_DELAYED_RELOAD_READY);
                if flags == 0 {
                    return Ok(region.region.clone());
                }
                Some((candidate_region_ver_id, region.region.clone()))
            } else {
                None
            }
        } else {
            None
        };
        drop(region_cache_guard);
        if let Some((ver_id, old_region)) = reload {
            return match self
                .inner_client
                .clone()
                .get_region(key.clone().into())
                .await
            {
                Ok(region) => {
                    self.add_region(region.clone()).await;
                    Ok(region)
                }
                Err(_) => {
                    if let Some(region) = self
                        .region_cache
                        .write()
                        .await
                        .ver_id_to_region
                        .get_mut(&ver_id)
                    {
                        region.set_sync_flags(NEED_RELOAD_ON_ACCESS);
                    }
                    Ok(old_region)
                }
            };
        }
        self.read_through_region_by_key(key.clone()).await
    }

    /// Retrieves a region with PD bucket metadata. A valid cached bucket is
    /// reused; otherwise this intentionally performs the source `WithBuckets`
    /// lookup rather than treating ordinary region metadata as bucket-aware.
    pub async fn get_region_by_key_with_buckets(&self, key: &Key) -> Result<RegionWithLeader> {
        let region = self.get_region_by_key(key).await?;
        if region.buckets.is_some() {
            return Ok(region);
        }
        let region = self
            .inner_client
            .clone()
            .get_region_with_buckets(key.clone().into())
            .await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    /// Locate the region whose range contains `key` when its end is inclusive.
    ///
    /// This is the region-cache counterpart to client-go's `LocateEndKey`, used
    /// by reverse scans so an exact region boundary belongs to the preceding
    /// region rather than the following one.
    pub async fn get_region_by_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let mut region_cache_guard = self.region_cache.write().await;
        let candidate = region_cache_guard
            .key_to_ver_id
            .range::<Key, _>(..key)
            .next_back()
            .map(|(_, version)| version.clone());
        let reload = if let Some(candidate) = candidate {
            let region = region_cache_guard
                .ver_id_to_region
                .get_mut(&candidate)
                .unwrap();
            if region.region.start_key() < *key
                && (region.region.end_key().is_empty() || *key <= region.region.end_key())
                && region.check_ttl(now_epoch_secs())
            {
                let flags =
                    region.take_sync_flags(NEED_RELOAD_ON_ACCESS | NEED_DELAYED_RELOAD_READY);
                if flags == 0 {
                    return Ok(region.region.clone());
                }
                Some((candidate, region.region.clone()))
            } else {
                None
            }
        } else {
            None
        };
        drop(region_cache_guard);
        if let Some((ver_id, old_region)) = reload {
            return match self
                .inner_client
                .clone()
                .get_prev_region(key.clone().into())
                .await
            {
                Ok(region) => {
                    self.add_region(region.clone()).await;
                    Ok(region)
                }
                Err(_) => {
                    if let Some(region) = self
                        .region_cache
                        .write()
                        .await
                        .ver_id_to_region
                        .get_mut(&ver_id)
                    {
                        region.set_sync_flags(NEED_RELOAD_ON_ACCESS);
                    }
                    Ok(old_region)
                }
            };
        }
        let region = self
            .load_region_by_end_key_with_stale_retry(key.clone())
            .await?;
        Ok(region)
    }

    // Retrieve cache entry by RegionId. If there's no entry, query PD and update cache.
    pub async fn get_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        for _ in 0..=MAX_RETRY_WAITING_CONCURRENT_REQUEST {
            let mut region_cache_guard = self.region_cache.write().await;

            // check cache
            let ver_id = region_cache_guard.id_to_ver_id.get(&id).cloned();
            if let Some(ver_id) = ver_id {
                let region = region_cache_guard
                    .ver_id_to_region
                    .get_mut(&ver_id)
                    .unwrap();
                if region.check_ttl(now_epoch_secs()) {
                    let flags =
                        region.take_sync_flags(NEED_RELOAD_ON_ACCESS | NEED_DELAYED_RELOAD_READY);
                    if flags == 0 {
                        return Ok(region.region.clone());
                    }
                    let old_region = region.region.clone();
                    drop(region_cache_guard);
                    return match self.inner_client.clone().get_region_by_id(id).await {
                        Ok(region) => {
                            self.add_region(region.clone()).await;
                            Ok(region)
                        }
                        Err(_) => {
                            if let Some(region) = self
                                .region_cache
                                .write()
                                .await
                                .ver_id_to_region
                                .get_mut(&ver_id)
                            {
                                region.set_sync_flags(NEED_RELOAD_ON_ACCESS);
                            }
                            Ok(old_region)
                        }
                    };
                }
            }

            // check concurrent requests
            let notify = region_cache_guard.on_my_way_id.get(&id).cloned();
            let notified = notify.as_ref().map(|notify| notify.notified());
            drop(region_cache_guard);

            if let Some(n) = notified {
                n.await;
                continue;
            } else {
                return self.read_through_region_by_id(id).await;
            }
        }
        Err(Error::StringError(format!(
            "Concurrent PD requests failed for {MAX_RETRY_WAITING_CONCURRENT_REQUEST} times"
        )))
    }

    pub async fn get_region_by_id_with_buckets(&self, id: RegionId) -> Result<RegionWithLeader> {
        let region = self.get_region_by_id(id).await?;
        if region.buckets.is_some() {
            return Ok(region);
        }
        let region = self
            .inner_client
            .clone()
            .get_region_by_id_with_buckets(id)
            .await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    pub async fn get_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self
            .store_cache
            .read()
            .unwrap()
            .get(&id)
            .map(|store| store.meta.clone());
        match store {
            Some(store) => Ok(store),
            None => self.read_through_store_by_id(id).await,
        }
    }

    /// Force read through (query from PD) and update cache
    pub async fn read_through_region_by_key(&self, key: Key) -> Result<RegionWithLeader> {
        self.load_region_by_key_with_stale_retry(key).await
    }

    /// Source `findRegionByKey` retries one rejected cache-miss load. The
    /// second request is the leader-only equivalent in client-go; Rust's PD
    /// trait has no router/follower option, but preserves the acceptance and
    /// retry boundary.
    async fn load_region_by_key_with_stale_retry(&self, key: Key) -> Result<RegionWithLeader> {
        let region = self
            .inner_client
            .clone()
            .get_region(key.clone().into())
            .await?;
        if self.add_region(region.clone()).await {
            return Ok(region);
        }
        let region = self.inner_client.clone().get_region(key.into()).await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    async fn load_region_by_end_key_with_stale_retry(&self, key: Key) -> Result<RegionWithLeader> {
        let region = self
            .inner_client
            .clone()
            .get_prev_region(key.clone().into())
            .await?;
        if self.add_region(region.clone()).await {
            return Ok(region);
        }
        let region = self
            .inner_client
            .clone()
            .get_prev_region(key.into())
            .await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    /// Source `BatchLoadRegionsFromKey`: always refreshes a bounded run of
    /// consecutive regions from PD, then caches only regions with a known
    /// leader.
    pub async fn batch_load_regions_from_key(
        &self,
        start_key: Key,
        count: usize,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        self.batch_load_regions_with_key_range(start_key, Key::default(), count, backoffer)
            .await
    }

    /// Source `BatchLoadRegionsWithKeyRange`: refresh at most `count`
    /// consecutive leader-bearing regions from PD and install accepted
    /// metadata in the cache.
    pub async fn batch_load_regions_with_key_range(
        &self,
        start_key: Key,
        end_key: Key,
        count: usize,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        let regions = self
            .scan_regions(start_key.clone(), end_key.clone(), count, backoffer)
            .await?;
        if regions.is_empty() {
            return Err(Error::StringError(format!(
                "PD returned no region, start_key: {start_key:?}, end_key: {end_key:?}"
            )));
        }
        Ok(regions)
    }

    /// Source `LoadRegionsInKeyRange`: repeatedly issue bounded PD scans until
    /// the complete half-open range has been refreshed.
    pub async fn load_regions_in_key_range(
        &self,
        mut start_key: Key,
        end_key: Key,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        let mut regions = Vec::new();
        loop {
            let loaded = self
                .batch_load_regions_with_key_range(
                    start_key.clone(),
                    end_key.clone(),
                    DEFAULT_REGIONS_PER_BATCH,
                    backoffer,
                )
                .await?;
            let last = loaded
                .last()
                .expect("batch_load_regions_with_key_range rejects empty results");
            let complete = contains_by_end(last, end_key.as_ref());
            let next_key = last.end_key();
            regions.extend(loaded);
            if complete {
                return Ok(regions);
            }
            if next_key.is_empty() || next_key <= start_key {
                return Err(Error::StringError(
                    "PD returned a region that does not advance LoadRegionsInKeyRange".to_owned(),
                ));
            }
            start_key = next_key;
        }
    }

    /// Source `ListRegionIDsInKeyRange` uses an inclusive upper key, unlike
    /// the half-open range-loading APIs.
    pub async fn list_region_ids_in_key_range(
        &self,
        mut start_key: Key,
        end_key: Key,
    ) -> Result<Vec<RegionId>> {
        let mut region_ids = Vec::new();
        loop {
            let region = self.get_region_by_key(&start_key).await?;
            region_ids.push(region.id());
            if region.contains(&end_key) {
                return Ok(region_ids);
            }
            let next_key = region.end_key();
            if next_key.is_empty() || next_key <= start_key {
                return Err(Error::StringError(
                    "region does not advance ListRegionIDsInKeyRange".to_owned(),
                ));
            }
            start_key = next_key;
        }
    }

    /// Source `LocateRegionByIDFromPD`: bypass the cache for diagnostics and
    /// deliberately leave the returned metadata uncached.
    pub async fn load_region_by_id_from_pd(&self, id: RegionId) -> Result<RegionWithLeader> {
        self.inner_client.clone().get_region_by_id(id).await
    }

    async fn scan_regions(
        &self,
        start_key: Key,
        end_key: Key,
        count: usize,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let ranges = [pdpb::KeyRange {
            start_key: start_key.clone().into(),
            end_key: end_key.clone().into(),
        }];
        loop {
            let scan_started = Instant::now();
            let scanned = self
                .inner_client
                .clone()
                .scan_regions(start_key.clone().into(), end_key.clone().into(), count)
                .await;
            crate::stats::observe_region_cache_scan(scan_started.elapsed(), scanned.is_ok());
            let regions = match scanned {
                Ok(regions) if !regions_have_gap_in_ranges(&ranges, &regions, Some(count)) => {
                    regions
                }
                Ok(_) => {
                    crate::stats::increment_stale_region_from_pd();
                    backoffer
                        .backoff(
                            BO_PD_RPC,
                            "PD returned regions with gaps while batch loading",
                        )
                        .await
                        .map_err(|error| Error::StringError(error.to_string()))?;
                    continue;
                }
                Err(error) => {
                    backoffer
                        .backoff(BO_PD_RPC, format!("PD ScanRegions failed: {error}"))
                        .await
                        .map_err(|error| Error::StringError(error.to_string()))?;
                    continue;
                }
            };
            let valid_regions = regions
                .into_iter()
                .filter(|region| region.leader.as_ref().is_some_and(|leader| leader.id != 0))
                .collect::<Vec<_>>();
            if valid_regions.is_empty() {
                crate::stats::increment_stale_region_from_pd();
                backoffer
                    .backoff(
                        BO_PD_RPC,
                        "PD returned only leaderless regions while batch loading",
                    )
                    .await
                    .map_err(|error| Error::StringError(error.to_string()))?;
                continue;
            }
            for region in &valid_regions {
                self.add_region(region.clone()).await;
            }
            return Ok(valid_regions);
        }
    }

    /// Source `refreshRegionIndex`: scan the complete keyspace in bounded
    /// 10,000-region pages and replace all three indexes in one write-locked
    /// step. In-flight by-ID request notifications survive the replacement.
    pub(crate) async fn refresh_region_index(&self, backoffer: &mut RetryBackoffer) -> Result<()> {
        let mut all_regions = Vec::new();
        let mut start_key = Key::default();
        loop {
            let regions = self
                .scan_regions(start_key.clone(), Key::default(), 10_000, backoffer)
                .await?;
            let last = regions.last().ok_or_else(|| {
                Error::StringError("PD returned no region while refreshing region index".to_owned())
            })?;
            let next_key = last.end_key();
            all_regions.extend(regions);
            if next_key.is_empty() {
                break;
            }
            if next_key <= start_key {
                return Err(Error::StringError(
                    "PD returned a region that does not advance refreshRegionIndex".to_owned(),
                ));
            }
            start_key = next_key;
        }

        let store_epochs = self
            .store_cache
            .read()
            .unwrap()
            .iter()
            .map(|(id, store)| (*id, store.epoch.load(Ordering::Acquire)))
            .collect::<HashMap<_, _>>();
        let now = now_epoch_secs();
        let mut refreshed = RegionCacheMap::new();
        for region in all_regions {
            let version = region.ver_id();
            let epochs = region
                .region
                .peers
                .iter()
                .filter_map(|peer| {
                    store_epochs
                        .get(&peer.store_id)
                        .map(|epoch| (peer.store_id, *epoch))
                })
                .collect();
            refreshed
                .key_to_ver_id
                .insert(region.start_key(), version.clone());
            refreshed.id_to_ver_id.insert(region.id(), version.clone());
            refreshed
                .ver_id_to_region
                .insert(version, CachedRegion::new(region, epochs, now));
        }

        let mut current = self.region_cache.write().await;
        refreshed.on_my_way_id = std::mem::take(&mut current.on_my_way_id);
        *current = refreshed;
        *self.gc_cursor.lock().unwrap() = None;
        Ok(())
    }

    async fn batch_scan_regions(
        &self,
        ranges: &[pdpb::KeyRange],
        count: usize,
        need_buckets: bool,
        need_region_has_leader: bool,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        if count == 0 || ranges.is_empty() {
            return Ok(Vec::new());
        }
        loop {
            let scan_started = Instant::now();
            let scanned = self
                .inner_client
                .clone()
                .batch_scan_regions(
                    ranges.to_vec(),
                    count,
                    RegionScanOptions {
                        need_buckets,
                        contain_all_key_range: true,
                    },
                )
                .await;
            crate::stats::observe_region_cache_batch_scan(scan_started.elapsed(), scanned.is_ok());
            let regions = match scanned {
                Err(error) if is_unimplemented_batch_scan(&error) => {
                    return self
                        .batch_scan_regions_fallback(ranges, count, backoffer)
                        .await;
                }
                Ok(regions) if !regions_have_gap_in_ranges(ranges, &regions, Some(count)) => {
                    regions
                }
                Ok(_) => {
                    crate::stats::increment_stale_region_from_pd();
                    backoffer
                        .backoff(
                            BO_PD_RPC,
                            "PD returned regions with gaps while batch scanning",
                        )
                        .await
                        .map_err(|error| Error::StringError(error.to_string()))?;
                    continue;
                }
                Err(error) => {
                    backoffer
                        .backoff(BO_PD_RPC, format!("PD BatchScanRegions failed: {error}"))
                        .await
                        .map_err(|error| Error::StringError(error.to_string()))?;
                    continue;
                }
            };

            let valid_regions = regions
                .into_iter()
                .filter(|region| {
                    !need_region_has_leader
                        || region.leader.as_ref().is_some_and(|leader| leader.id != 0)
                })
                .collect::<Vec<_>>();
            if valid_regions.is_empty() {
                crate::stats::increment_stale_region_from_pd();
                backoffer
                    .backoff(
                        BO_PD_RPC,
                        "PD returned only leaderless regions while batch scanning",
                    )
                    .await
                    .map_err(|error| Error::StringError(error.to_string()))?;
                continue;
            }
            for region in &valid_regions {
                self.add_region(region.clone()).await;
            }
            return Ok(valid_regions);
        }
    }

    async fn batch_scan_regions_fallback(
        &self,
        ranges: &[pdpb::KeyRange],
        mut count: usize,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        let mut result = Vec::with_capacity(ranges.len());
        let mut last_region: Option<RegionWithLeader> = None;
        for range in ranges {
            let mut range = range.clone();
            if let Some(last_region) = &last_region {
                let end_key = last_region.region.end_key.as_slice();
                if end_key.is_empty() {
                    break;
                }
                if end_key >= range.end_key.as_slice() {
                    continue;
                }
                if end_key > range.start_key.as_slice() {
                    range.start_key = end_key.to_vec();
                }
            }
            let regions = self
                .scan_regions(
                    range.start_key.into(),
                    range.end_key.into(),
                    count,
                    backoffer,
                )
                .await?;
            if let Some(region) = regions.last() {
                last_region = Some(region.clone());
            }
            let loaded = regions.len();
            result.extend(regions);
            if loaded >= count {
                return Ok(result);
            }
            count -= loaded;
        }
        Ok(result)
    }

    async fn try_cached_region_by_key(&self, key: &Key) -> Option<RegionWithLeader> {
        let mut cache = self.region_cache.write().await;
        let version = cache
            .key_to_ver_id
            .range::<Key, _>(..=key)
            .next_back()
            .map(|(_, version)| version.clone())?;
        let region = cache.ver_id_to_region.get_mut(&version)?;
        (region.region.contains(key)
            && region.check_ttl(now_epoch_secs())
            && !region.has_sync_flags(NEED_RELOAD_ON_ACCESS | NEED_DELAYED_RELOAD_READY))
        .then(|| region.region.clone())
    }

    async fn scan_regions_from_cache(
        &self,
        start_key: &Key,
        end_key: &Key,
        limit: usize,
    ) -> Vec<RegionWithLeader> {
        if limit == 0 {
            return Vec::new();
        }
        let mut cache = self.region_cache.write().await;
        let entries = cache
            .key_to_ver_id
            .range(start_key.clone()..)
            .take_while(|(key, _)| end_key.is_empty() || *key < end_key)
            .take(limit)
            .map(|(key, version)| (key.clone(), version.clone()))
            .collect::<Vec<_>>();
        let mut regions = Vec::with_capacity(entries.len());
        let mut last_start_key = start_key.clone();
        let now = now_epoch_secs();
        for (_, version) in entries {
            let Some(region) = cache.ver_id_to_region.get_mut(&version) else {
                break;
            };
            if !region.check_ttl(now) || !region.region.contains(&last_start_key) {
                break;
            }
            last_start_key = region.region.end_key();
            if !region.has_sync_flags(NEED_RELOAD_ON_ACCESS | NEED_DELAYED_RELOAD_READY) {
                regions.push(region.region.clone());
            }
        }
        regions
    }

    /// Source `LocateKeyRange`: combine cache hits with bounded multi-range PD
    /// loading and require every returned region to have a leader peer.
    pub async fn locate_key_range(
        &self,
        start_key: Key,
        end_key: Key,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        self.batch_locate_key_ranges(
            vec![pdpb::KeyRange {
                start_key: start_key.into(),
                end_key: end_key.into(),
            }],
            false,
            true,
            backoffer,
        )
        .await
    }

    /// Source `BatchLoadRegionsWithKeyRanges`: refresh a bounded ordered set
    /// of ranges and optionally require leader and bucket metadata.
    pub async fn batch_load_regions_with_key_ranges(
        &self,
        ranges: Vec<pdpb::KeyRange>,
        count: usize,
        need_buckets: bool,
        need_region_has_leader: bool,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let regions = self
            .batch_scan_regions(
                &ranges,
                count,
                need_buckets,
                need_region_has_leader,
                backoffer,
            )
            .await?;
        if regions.is_empty() {
            return Err(Error::StringError(format!(
                "PD returned no region, range num: {}, count: {count}",
                ranges.len()
            )));
        }
        Ok(regions)
    }

    /// Source `BatchLocateKeyRanges`: reuse a gap-free cached prefix, load the
    /// remaining ordered ranges in bounded PD batches, and merge newer PD
    /// metadata over intersecting cached entries.
    pub async fn batch_locate_key_ranges(
        &self,
        ranges: Vec<pdpb::KeyRange>,
        need_buckets: bool,
        need_region_has_leader: bool,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        let mut uncached_ranges = Vec::with_capacity(ranges.len());
        let mut cached_regions = Vec::with_capacity(ranges.len());
        let mut last_region: Option<RegionWithLeader> = None;

        for mut range in ranges {
            if let Some(last) = &last_region {
                if contains_by_end(last, &range.end_key) {
                    continue;
                }
                if last.contains(&range.start_key.clone().into()) {
                    range.start_key.clone_from(&last.region.end_key);
                }
            }

            let start_key: Key = range.start_key.clone().into();
            let Some(mut region) = self.try_cached_region_by_key(&start_key).await else {
                uncached_ranges.push(range);
                continue;
            };
            cached_regions.push(region.clone());
            last_region = Some(region.clone());
            if contains_by_end(&region, &range.end_key) {
                continue;
            }
            range.start_key.clone_from(&region.region.end_key);

            let mut contains_all = false;
            loop {
                let start_key: Key = range.start_key.clone().into();
                let end_key: Key = range.end_key.clone().into();
                let cached = self
                    .scan_regions_from_cache(&start_key, &end_key, DEFAULT_REGIONS_PER_BATCH)
                    .await;
                let mut cache_hole = false;
                for candidate in &cached {
                    if !candidate.contains(&range.start_key.clone().into()) {
                        cache_hole = true;
                        break;
                    }
                    region = candidate.clone();
                    cached_regions.push(region.clone());
                    last_region = Some(region.clone());
                    if contains_by_end(&region, &range.end_key) {
                        contains_all = true;
                        break;
                    }
                    range.start_key.clone_from(&region.region.end_key);
                }
                if contains_all || cache_hole || cached.len() < DEFAULT_REGIONS_PER_BATCH {
                    break;
                }
            }
            if !contains_all {
                uncached_ranges.push(range);
            }
        }

        let size_hint = cached_regions.len() + uncached_ranges.len();
        let mut merger = BatchLocateRegionMerger::new(cached_regions, size_hint);
        while !uncached_ranges.is_empty() {
            let range_count = uncached_ranges.len().min(MAX_RANGES_PER_BATCH);
            let to_send = &uncached_ranges[..range_count];
            let regions = self
                .batch_load_regions_with_key_ranges(
                    to_send.to_vec(),
                    DEFAULT_REGIONS_PER_BATCH,
                    need_buckets,
                    need_region_has_leader,
                    backoffer,
                )
                .await?;
            let Some(last) = regions.last() else {
                return Err(Error::StringError(
                    "BatchLoadRegionsWithKeyRanges returned no regions".to_owned(),
                ));
            };
            let split_key = last.region.end_key.clone();
            for region in regions {
                merger.append_region(region);
            }
            uncached_ranges = ranges_after_key(uncached_ranges, &split_key);
        }
        Ok(merger.build())
    }

    /// Force read through (query from PD) and update cache
    async fn read_through_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        // put a notify to let others know the region id is being queried
        let notify = Arc::new(Notify::new());
        {
            let mut region_cache_guard = self.region_cache.write().await;
            region_cache_guard.on_my_way_id.insert(id, notify.clone());
        }

        let result = self.inner_client.clone().get_region_by_id(id).await;
        if let Ok(region) = &result {
            self.add_region(region.clone()).await;
        }

        // Notify waiters even when PD failed. Leaving the singleflight entry
        // behind would strand every subsequent by-ID lookup indefinitely.
        {
            let mut region_cache_guard = self.region_cache.write().await;
            notify.notify_waiters();
            region_cache_guard.on_my_way_id.remove(&id);
        }

        result
    }

    async fn read_through_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self.inner_client.clone().get_store(id).await?;
        let mut cache = self.store_cache.write().unwrap();
        match cache.get_mut(&id) {
            Some(cached) => cached.update_metadata(store.clone()),
            None => {
                cache.insert(id, CachedStore::new(store.clone()));
            }
        }
        Ok(store)
    }

    /// Source `Store.reResolve` updates address, peer/status addresses, type,
    /// and labels on the existing cache entry. Health, liveness, failure
    /// epoch, token/load state, and in-flight references must survive.
    pub(crate) async fn refresh_store_by_id(&self, id: StoreId) -> Result<Store> {
        self.read_through_store_by_id(id).await
    }

    /// Insert a PD region unless it is older than the cached region epoch.
    ///
    /// This mirrors `internal/locate.regionIndexMu.insertRegionToCache`: a
    /// delayed PD response must not replace a newer version, and a region whose
    /// end key is empty intersects every following key.
    pub async fn add_region(&self, region: RegionWithLeader) -> bool {
        let store_epochs = {
            let stores = self.store_cache.read().unwrap();
            region
                .region
                .peers
                .iter()
                .map(|peer| {
                    (
                        peer.store_id,
                        stores
                            .get(&peer.store_id)
                            .map_or(0, |store| store.epoch.load(Ordering::Acquire)),
                    )
                })
                .collect::<HashMap<_, _>>()
        };
        let mut cache = self.region_cache.write().await;
        let new_ver_id = region.ver_id();

        let end_key = region.end_key();
        let mut to_be_removed: HashSet<RegionVerId> = HashSet::new();

        if let Some(ver_id) = cache.id_to_ver_id.get(&region.id()) {
            if ver_id.ver > new_ver_id.ver || ver_id.conf_ver > new_ver_id.conf_ver {
                return false;
            }
            if ver_id != &new_ver_id {
                to_be_removed.insert(ver_id.clone());
            }
        }

        let mut search_range = {
            if end_key.is_empty() {
                cache.key_to_ver_id.range::<Key, _>(..)
            } else {
                cache.key_to_ver_id.range::<Key, _>(..end_key)
            }
        };
        while let Some((_, ver_id_in_cache)) = search_range.next_back() {
            let region_in_cache = &cache.ver_id_to_region.get(ver_id_in_cache).unwrap().region;

            if region_in_cache.region.end_key.is_empty()
                || region_in_cache.region.end_key > region.region.start_key
            {
                if region_in_cache
                    .region
                    .region_epoch
                    .as_ref()
                    .unwrap()
                    .version
                    > new_ver_id.ver
                {
                    return false;
                }
                to_be_removed.insert(ver_id_in_cache.clone());
            } else {
                break;
            }
        }

        for ver_id in to_be_removed {
            let cached_region = cache.ver_id_to_region.remove(&ver_id).unwrap();
            cache
                .key_to_ver_id
                .remove(&cached_region.region.start_key());
            if cache.id_to_ver_id.get(&cached_region.region.id()) == Some(&ver_id) {
                cache.id_to_ver_id.remove(&cached_region.region.id());
            }
        }
        cache
            .key_to_ver_id
            .insert(region.start_key(), new_ver_id.clone());
        cache.id_to_ver_id.insert(region.id(), new_ver_id.clone());
        cache.ver_id_to_region.insert(
            new_ver_id,
            CachedRegion::new(region, store_epochs, now_epoch_secs()),
        );
        true
    }

    pub async fn update_leader(
        &self,
        ver_id: crate::region::RegionVerId,
        leader: metapb::Peer,
    ) -> Result<()> {
        let mut cache = self.region_cache.write().await;
        let Some(region) = cache.ver_id_to_region.get(&ver_id) else {
            return Ok(());
        };
        // `replicaSelector.updateLeader` only accepts a NotLeader hint that
        // refers to a peer in the cached region. A different peer proves this
        // region is stale and must be reloaded instead of poisoning its
        // cached leader with unrelated metadata.
        let is_known_peer = region
            .region
            .region
            .peers
            .iter()
            .any(|peer| peer.id == leader.id && peer.store_id == leader.store_id);
        if is_known_peer {
            let region = cache
                .ver_id_to_region
                .get_mut(&ver_id)
                .expect("cached region disappeared while write-locked");
            region.region.leader = Some(leader);
            debug!("updated cached region leader, region: {:?}", ver_id);
        } else {
            let region = cache
                .ver_id_to_region
                .remove(&ver_id)
                .expect("cached region disappeared while write-locked");
            let id = region.region.id();
            let start_key = region.region.start_key();
            cache.key_to_ver_id.remove(&start_key);
            if cache.id_to_ver_id.get(&id) == Some(&ver_id) {
                cache.id_to_ver_id.remove(&id);
            }
            debug!(
                "invalidated cached region after NotLeader hint for unknown peer, region: {:?}",
                ver_id
            );
        }

        Ok(())
    }

    /// Applies TiKV's direct `BucketVersionNotMatch` refresh to the cached
    /// region. Source only replaces missing or older metadata, and binds the
    /// returned keys to the cached region ID rather than trusting a response
    /// to name one.
    pub async fn update_buckets(
        &self,
        ver_id: crate::region::RegionVerId,
        version: u64,
        keys: Vec<Vec<u8>>,
    ) {
        let mut cache = self.region_cache.write().await;
        let Some(cached) = cache.ver_id_to_region.get_mut(&ver_id) else {
            return;
        };
        if cached
            .region
            .buckets
            .as_ref()
            .is_some_and(|buckets| buckets.version >= version)
        {
            return;
        }
        cached.region.buckets = Some(metapb::Buckets {
            region_id: cached.region.id(),
            version,
            keys,
            ..Default::default()
        });
    }

    /// Source `UpdateBucketsIfNeeded`: coalesce background PD reloads when a
    /// response advertises a newer bucket version than the cached region.
    pub(crate) fn update_buckets_if_needed(
        self: &Arc<Self>,
        ver_id: RegionVerId,
        request_version: u64,
        latest_version: u64,
    ) where
        C: 'static,
    {
        let cache = self.clone();
        tokio::spawn(async move {
            let needs_refresh = {
                let regions = cache.region_cache.read().await;
                let Some(region) = regions.ver_id_to_region.get(&ver_id) else {
                    return;
                };
                let cached_version = region.region.buckets_version();
                !(request_version != 0 && request_version < cached_version)
                    && cached_version < latest_version
            };
            if !needs_refresh || !cache.bucket_refreshes.lock().unwrap().insert(ver_id.id) {
                return;
            }
            let _ = cache.get_region_by_id_with_buckets(ver_id.id).await;
            cache.bucket_refreshes.lock().unwrap().remove(&ver_id.id);
        });
    }

    pub async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
        let mut cache = self.region_cache.write().await;
        let region_entry = cache.ver_id_to_region.get(&ver_id);
        if let Some(region) = region_entry {
            let id = region.region.id();
            let start_key = region.region.start_key();
            cache.ver_id_to_region.remove(&ver_id);
            cache.id_to_ver_id.remove(&id);
            cache.key_to_ver_id.remove(&start_key);
            debug!("invalidated region cache entry, region: {:?}", ver_id);
        }
    }

    pub(crate) async fn mark_region_reload_on_access(&self, ver_id: &RegionVerId) -> bool {
        self.region_cache
            .write()
            .await
            .ver_id_to_region
            .get_mut(ver_id)
            .is_some_and(|region| {
                region.set_sync_flags(NEED_RELOAD_ON_ACCESS);
                true
            })
    }

    pub(crate) async fn mark_region_delayed_reload(&self, ver_id: &RegionVerId) -> bool {
        self.region_cache
            .write()
            .await
            .ver_id_to_region
            .get_mut(ver_id)
            .is_some_and(|region| {
                region.set_sync_flags(NEED_DELAYED_RELOAD_PENDING);
                true
            })
    }

    pub async fn invalidate_store_cache(&self, store_id: StoreId) -> Option<Store> {
        let mut cache = self.store_cache.write().unwrap();
        let removed = cache.remove(&store_id).map(|store| store.meta);
        if removed.is_some() {
            debug!("invalidated store cache entry, store: {:?}", store_id);
        }
        removed
    }

    pub async fn read_through_all_stores(&self) -> Result<Vec<Store>> {
        let stores = self
            .inner_client
            .clone()
            .get_all_stores()
            .await?
            .into_iter()
            .filter(is_valid_data_store)
            .collect::<Vec<_>>();
        let mut cache = self.store_cache.write().unwrap();
        for store in &stores {
            match cache.get_mut(&store.id) {
                Some(cached) => cached.update_metadata(store.clone()),
                None => {
                    cache.insert(store.id, CachedStore::new(store.clone()));
                }
            }
        }
        Ok(stores)
    }

    /// Records client-go's stream-delivered TiKV health feedback for the
    /// matching cached store. Unknown stores are intentionally ignored.
    pub(crate) fn record_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback) {
        self.record_health_feedback_at(feedback, Instant::now());
    }

    fn record_health_feedback_at(&self, feedback: &kvrpcpb::HealthFeedback, now: Instant) {
        let health_status = self
            .store_cache
            .read()
            .unwrap()
            .get(&feedback.store_id)
            .map(|store| store.health_status.clone());
        if let Some(health_status) = health_status {
            health_status.record_tikv_slow_score(i64::from(feedback.slow_score), now);
        }
    }

    pub(crate) fn store_health(&self, store_id: StoreId) -> Option<HealthStatusDetail> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.health_status.detail())
    }

    pub(crate) fn store_health_status(&self, store_id: StoreId) -> Option<Arc<StoreHealthStatus>> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.health_status.clone())
    }

    pub(crate) fn store_liveness(&self, store_id: StoreId) -> Option<StoreLiveness> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| StoreLiveness::from_encoded(store.liveness.load(Ordering::Acquire)))
    }

    pub(crate) fn set_store_liveness(&self, store_id: StoreId, liveness: StoreLiveness) -> bool {
        let store = self
            .store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| {
                store.liveness.store(liveness as u8, Ordering::Release);
            });
        store.is_some()
    }

    /// Returns whether a cached region captured an older failure epoch for a
    /// peer store. Missing snapshots retain compatibility with callers that
    /// supplied an uncached region directly.
    pub(crate) async fn store_epoch_is_stale(
        &self,
        ver_id: &RegionVerId,
        store_id: StoreId,
    ) -> bool {
        let expected = self
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .get(ver_id)
            .and_then(|region| region.store_epochs.get(&store_id).copied());
        let current = self
            .store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.epoch.load(Ordering::Acquire));
        expected
            .zip(current)
            .is_some_and(|(expected, current)| expected != current)
    }

    /// Source `invalidateReplicaStore`: advance a store failure epoch only
    /// when this route still owns the captured epoch. That CAS prevents a
    /// stale in-flight request from invalidating a region reloaded after a
    /// newer failure.
    pub(crate) async fn invalidate_store_epoch_for_region(
        &self,
        ver_id: &RegionVerId,
        store_id: StoreId,
    ) -> bool {
        let expected = self
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .get(ver_id)
            .and_then(|region| region.store_epochs.get(&store_id).copied());
        let Some(expected) = expected else {
            return false;
        };
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .is_some_and(|store| {
                let advanced = store
                    .epoch
                    .compare_exchange(
                        expected,
                        expected.wrapping_add(1),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok();
                if advanced {
                    store.health_status.mark_already_slow();
                }
                advanced
            })
    }

    /// Claims the single source health-check loop for an unhealthy TiKV
    /// store. A later send failure observes the existing loop rather than
    /// creating another connection probe for the same destination.
    pub(crate) fn begin_store_health_check(&self, store_id: StoreId) -> bool {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .is_some_and(|store| {
                store
                    .health_check_running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
    }

    pub(crate) fn finish_store_health_check(&self, store_id: StoreId) {
        if let Some(store) = self.store_cache.read().unwrap().get(&store_id) {
            store.health_check_running.store(false, Ordering::Release);
        }
    }

    /// Records the source `ServerIsBusy.estimated_wait_ms` signal for the
    /// resolved store. Unknown stores intentionally remain untouched.
    pub(crate) fn record_server_load(&self, store_id: StoreId, estimated_wait_ms: u32) {
        self.record_server_load_at(store_id, estimated_wait_ms, Instant::now());
    }

    fn record_server_load_at(&self, store_id: StoreId, estimated_wait_ms: u32, now: Instant) {
        if let Some(store) = self.store_cache.read().unwrap().get(&store_id) {
            store.update_server_load(estimated_wait_ms, now);
        }
    }

    pub(crate) fn estimated_store_wait(&self, store_id: StoreId) -> Option<Duration> {
        self.estimated_store_wait_at(store_id, Instant::now())
    }

    fn estimated_store_wait_at(&self, store_id: StoreId, now: Instant) -> Option<Duration> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.estimated_wait(now))
    }

    /// Runs the source store-health periodic update for every cached store.
    /// The owning region-cache scheduler will call this at its configured
    /// health-check cadence once store liveness is transcreated.
    pub(crate) fn tick_store_health(&self, now: Instant) {
        let health_statuses = self
            .store_cache
            .read()
            .unwrap()
            .values()
            .map(|store| store.health_status.clone())
            .collect::<Vec<_>>();
        for health_status in health_statuses {
            health_status.tick(now);
        }
    }

    /// Produces source replica-selector input snapshots from a cached region
    /// and its resolved stores. The request sender owns retry-attempt counters;
    /// callers provide them keyed by peer ID.
    pub(crate) async fn replica_candidates(
        &self,
        region: &RegionWithLeader,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        selector_state: &ReplicaSelectorState,
    ) -> Result<Vec<ReplicaCandidate>> {
        for peer in &region.region.peers {
            self.get_store_by_id(peer.store_id).await?;
        }
        let leader_peer_id = region.leader.as_ref().map(|leader| leader.id);
        let store_epochs = self
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .get(&region.ver_id())
            .map(|cached| cached.store_epochs.clone())
            .unwrap_or_default();
        let (candidates, needs_delayed_reload) = {
            let cached_stores = self.store_cache.read().unwrap();
            let needs_delayed_reload = region.region.peers.iter().any(|peer| {
                let Some(store) = cached_stores.get(&peer.store_id) else {
                    return false;
                };
                let epoch_stale = store_epochs
                    .get(&peer.store_id)
                    .is_some_and(|epoch| *epoch != store.epoch.load(Ordering::Acquire));
                if !epoch_stale {
                    return false;
                }
                let is_leader = leader_peer_id == Some(peer.id);
                let reachable = StoreLiveness::from_encoded(store.liveness.load(Ordering::Acquire))
                    == StoreLiveness::Reachable;
                is_leader || reachable
            });
            let candidates = region
                .region
                .peers
                .iter()
                .filter_map(|peer| {
                    if is_unroutable_peer(region, peer) {
                        return None;
                    }
                    let store = cached_stores.get(&peer.store_id)?;
                    if store_epochs
                        .get(&peer.store_id)
                        .is_some_and(|epoch| *epoch != store.epoch.load(Ordering::Acquire))
                    {
                        return None;
                    }
                    // `GetTiKVRPCContext` builds its candidate set from the
                    // source `tiKVOnly` access mode. TiFlash peers are cached
                    // alongside TiKV peers but must never become transactional
                    // follower-read candidates.
                    if EndpointType::from_store(&store.meta) != EndpointType::TiKv {
                        return None;
                    }
                    let store_matches = stores.is_empty() || stores.contains(&store.meta.id);
                    let label_matches = store_matches
                        && labels.iter().all(|label| {
                            store.meta.labels.iter().any(|current| {
                                current.key == label.key && current.value == label.value
                            })
                        });
                    Some(ReplicaCandidate {
                        peer_id: peer.id,
                        is_leader: leader_peer_id == Some(peer.id),
                        is_learner: metapb::PeerRole::try_from(peer.role)
                            .is_ok_and(|role| role == metapb::PeerRole::Learner),
                        label_matches,
                        is_slow: store.health_status.is_slow(),
                        reachable: StoreLiveness::from_encoded(
                            store.liveness.load(Ordering::Acquire),
                        ) == StoreLiveness::Reachable,
                        attempts: selector_state.attempts(peer.id),
                        data_is_not_ready: selector_state.data_is_not_ready(peer.id),
                    })
                })
                .collect();
            (candidates, needs_delayed_reload)
        };
        if needs_delayed_reload {
            self.mark_region_delayed_reload(&region.ver_id()).await;
        }
        Ok(candidates)
    }

    /// Selects a TiFlash peer from a still-live cached region. This is kept
    /// separate from `replica_candidates`: client-go's `tiFlashOnly` access
    /// mode never participates in TiKV follower-read selection.
    pub(crate) async fn select_tiflash_peer(
        &self,
        region: &RegionWithLeader,
        load_balance: bool,
        labels: &[metapb::StoreLabel],
    ) -> std::result::Result<metapb::Peer, TiFlashSelectionError> {
        let cursor = {
            let mut regions = self.region_cache.write().await;
            let Some(cached) = regions.ver_id_to_region.get_mut(&region.ver_id()) else {
                return Err(TiFlashSelectionError::CachedRegionMissing);
            };
            if !cached.check_ttl(now_epoch_secs()) {
                return Err(TiFlashSelectionError::CacheExpired);
            }
            cached.tiflash_cursor.clone()
        };

        for peer in &region.region.peers {
            self.get_store_by_id(peer.store_id)
                .await
                .map_err(|_| TiFlashSelectionError::NoTiFlashPeer)?;
        }
        let stores = self.store_cache.read().unwrap();
        let peers = region
            .region
            .peers
            .iter()
            .filter(|peer| {
                if is_unroutable_peer(region, peer) {
                    return false;
                }
                stores.get(&peer.store_id).is_some_and(|store| {
                    EndpointType::from_store(&store.meta) == EndpointType::TiFlash
                })
            })
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return Err(TiFlashSelectionError::NoTiFlashPeer);
        }
        let start = if load_balance {
            cursor.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
        } else {
            cursor.load(Ordering::Relaxed)
        };
        for offset in 0..peers.len() {
            let peer = peers[(start + offset) % peers.len()];
            let store = stores.get(&peer.store_id).unwrap();
            if labels.iter().all(|wanted| {
                store
                    .meta
                    .labels
                    .iter()
                    .any(|actual| actual.key == wanted.key && actual.value == wanted.value)
            }) {
                cursor.store((start + offset) % peers.len(), Ordering::Relaxed);
                return Ok(peer.clone());
            }
        }
        Err(TiFlashSelectionError::AllStoresFiltered)
    }

    /// Returns client-go's `GetAllValidTiFlashStores` result: `all` begins
    /// with the current store and `non_pending` excludes PD pending peers so
    /// batch work can prefer replicas that have caught up with TiKV.
    pub(crate) async fn valid_tiflash_store_ids(
        &self,
        region: &RegionWithLeader,
        current_store_id: StoreId,
        labels: &[metapb::StoreLabel],
    ) -> (Vec<StoreId>, Vec<StoreId>) {
        let mut all = vec![current_store_id];
        let cached_live = {
            let mut regions = self.region_cache.write().await;
            regions
                .ver_id_to_region
                .get_mut(&region.ver_id())
                .is_some_and(|cached| cached.check_ttl(now_epoch_secs()))
        };
        if !cached_live {
            return (all, vec![]);
        }
        let stores = self.store_cache.read().unwrap();
        for peer in &region.region.peers {
            if is_unroutable_peer(region, peer) {
                continue;
            }
            let Some(store) = stores.get(&peer.store_id) else {
                continue;
            };
            if peer.store_id != current_store_id
                && EndpointType::from_store(&store.meta) == EndpointType::TiFlash
                && labels.iter().all(|wanted| {
                    store
                        .meta
                        .labels
                        .iter()
                        .any(|actual| actual.key == wanted.key && actual.value == wanted.value)
                })
            {
                all.push(peer.store_id);
            }
        }
        let non_pending = all
            .iter()
            .copied()
            .filter(|id| !region.pending_peers.iter().any(|peer| peer.store_id == *id))
            .collect();
        (all, non_pending)
    }

    /// Resolves the source mixed-selector decision back to its region peer.
    /// Retry-attempt state remains owned by the request sender, while this
    /// cache method owns the peer/store snapshot and keeps PD resolution out
    /// of the score calculation.
    pub(crate) async fn select_mixed_replica(
        &self,
        region: &RegionWithLeader,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        selector_state: &ReplicaSelectorState,
        selection: MixedReplicaSelection,
    ) -> Result<Option<metapb::Peer>> {
        let candidates = self
            .replica_candidates(region, labels, stores, selector_state)
            .await?;
        let Some(selected) = selection.choose(&candidates) else {
            return Ok(None);
        };
        Ok(region
            .region
            .peers
            .iter()
            .find(|peer| peer.id == selected.peer_id)
            .cloned())
    }

    /// Source `ReplicaSelectMixedStrategy` path used when a leader read has a
    /// configured busy threshold. The leader itself and every overloaded
    /// replica are excluded; the ordinary mixed score chooses among the
    /// remaining idle followers.
    pub(crate) async fn select_idle_replica(
        &self,
        region: &RegionWithLeader,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        selector_state: &ReplicaSelectorState,
        busy_threshold: Duration,
    ) -> Result<Option<metapb::Peer>> {
        let candidates = self
            .replica_candidates(region, labels, stores, selector_state)
            .await?;
        let cached_stores = self.store_cache.read().unwrap();
        let idle = candidates
            .into_iter()
            .filter(|candidate| {
                let Some(peer) = region
                    .region
                    .peers
                    .iter()
                    .find(|peer| peer.id == candidate.peer_id)
                else {
                    return false;
                };
                !candidate.is_leader
                    && candidate.reachable
                    && candidate.attempts == 0
                    && !selector_state.is_server_busy(candidate.peer_id)
                    && cached_stores
                        .get(&peer.store_id)
                        .is_some_and(|store| store.estimated_wait(Instant::now()) <= busy_threshold)
            })
            .collect::<Vec<_>>();
        let Some(selected) = (MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: !labels.is_empty(),
        })
        .choose(&idle) else {
            return Ok(None);
        };
        Ok(region
            .region
            .peers
            .iter()
            .find(|peer| peer.id == selected.peer_id)
            .cloned())
    }

    /// Returns a source-compatible forwarding proxy only for a leader known
    /// to be unreachable. Unknown leader liveness deliberately does not
    /// authorize forwarding. The returned peer is always a non-leader whose
    /// cached store is reachable; callers retain the leader as the logical
    /// request peer.
    pub(crate) async fn proxy_for_unreachable_leader(
        &self,
        region: &RegionWithLeader,
    ) -> Result<Option<metapb::Peer>> {
        let Some(leader) = region.leader.as_ref() else {
            return Ok(None);
        };
        if self.store_liveness(leader.store_id) != Some(StoreLiveness::Unreachable) {
            return Ok(None);
        }
        let proxy = self
            .select_mixed_replica(
                region,
                &[],
                &[],
                &ReplicaSelectorState::default(),
                MixedReplicaSelection {
                    read_type: ReplicaReadType::Follower,
                    leader_only: false,
                    prefer_leader: false,
                    labels_requested: false,
                },
            )
            .await?
            .filter(|peer| peer.id != leader.id);
        if proxy.is_none() {
            self.mark_region_reload_on_access(&region.ver_id()).await;
        }
        Ok(proxy)
    }
}

struct RegionCacheClientEventListener<Client> {
    cache: std::sync::Weak<RegionCache<Client>>,
}

impl<Client: RetryClientTrait + Send + Sync + 'static> ClientEventListener
    for RegionCacheClientEventListener<Client>
{
    fn on_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback) {
        if let Some(cache) = self.cache.upgrade() {
            cache.record_health_feedback(feedback);
        }
    }
}

impl<Client: RetryClientTrait + Send + Sync + 'static> RegionCache<Client> {
    pub(crate) fn client_event_listener(self: &Arc<Self>) -> Arc<dyn ClientEventListener> {
        Arc::new(RegionCacheClientEventListener {
            cache: Arc::downgrade(self),
        })
    }
}

/// Source `RegionCache.GetAllStores` exposes resolved TiKV and TiFlash stores
/// but excludes tombstones and TiFlash-compute nodes.
fn is_valid_data_store(store: &metapb::Store) -> bool {
    if metapb::StoreState::try_from(store.state).unwrap() == metapb::StoreState::Tombstone {
        return false;
    }
    matches!(
        EndpointType::from_store(store),
        EndpointType::TiKv | EndpointType::TiFlash
    )
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::collections::VecDeque;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::{Mutex, Notify};

    use super::{
        now_epoch_secs, ranges_after_key, regions_have_gap_in_ranges, BatchLocateRegionMerger,
        CachedStore, MixedReplicaSelection, RegionCache, ReplicaCandidate, ReplicaSelectorState,
        StoreLiveness, NEED_DELAYED_RELOAD_READY, NEED_EXPIRE_AFTER_TTL, NEED_RELOAD_ON_ACCESS,
        REGION_CACHE_TTL_SECS,
    };
    use crate::async_util::Cancellation;
    use crate::common::Error;
    use crate::kv::ReplicaReadType;
    use crate::pd::RegionScanOptions;
    use crate::pd::RetryClientTrait;
    use crate::proto::keyspacepb;
    use crate::proto::metapb::RegionEpoch;
    use crate::proto::metapb::{self};
    use crate::proto::pdpb;
    use crate::region::RegionId;
    use crate::region::RegionWithLeader;
    use crate::region_cache::is_valid_data_store;
    use crate::retry::RetryBackoffer;
    use crate::Key;
    use crate::Result;

    #[derive(Default)]
    struct MockRetryClient {
        pub regions: Mutex<HashMap<RegionId, RegionWithLeader>>,
        pub stores: Mutex<Vec<metapb::Store>>,
        pub get_region_count: AtomicU64,
        pub get_region_with_buckets_count: AtomicU64,
        pub get_region_responses: Mutex<VecDeque<Result<RegionWithLeader>>>,
        pub get_prev_region_responses: Mutex<VecDeque<Result<RegionWithLeader>>>,
        pub get_region_by_id_responses: Mutex<VecDeque<Result<RegionWithLeader>>>,
        pub batch_scan_count: AtomicU64,
        pub batch_scan_unimplemented: AtomicBool,
        pub batch_scan_options: StdMutex<Vec<RegionScanOptions>>,
    }

    #[async_trait]
    impl RetryClientTrait for MockRetryClient {
        async fn get_region(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            if let Some(response) = self.get_region_responses.lock().await.pop_front() {
                return response;
            }
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(_, r)| r.contains(&key.clone().into()))
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_with_buckets(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_with_buckets_count.fetch_add(1, SeqCst);
            let mut region = self.get_region(key).await?;
            region.buckets = Some(metapb::Buckets {
                region_id: region.id(),
                version: 9,
                keys: vec![
                    region.region.start_key.clone(),
                    region.region.end_key.clone(),
                ],
                ..Default::default()
            });
            Ok(region)
        }

        async fn get_prev_region(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            if let Some(response) = self.get_prev_region_responses.lock().await.pop_front() {
                return response;
            }
            let key: Key = key.into();
            self.regions
                .lock()
                .await
                .values()
                .filter(|region| {
                    region.start_key() < key
                        && (region.end_key().is_empty() || key <= region.end_key())
                })
                .cloned()
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            region_id: crate::region::RegionId,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            if let Some(response) = self.get_region_by_id_responses.lock().await.pop_front() {
                return response;
            }
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(id, _)| id == &&region_id)
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_by_id_with_buckets(
            self: Arc<Self>,
            id: RegionId,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_with_buckets_count.fetch_add(1, SeqCst);
            let mut region = self.get_region_by_id(id).await?;
            region.buckets = Some(metapb::Buckets {
                region_id: id,
                version: 9,
                keys: vec![
                    region.region.start_key.clone(),
                    region.region.end_key.clone(),
                ],
                ..Default::default()
            });
            Ok(region)
        }

        async fn batch_scan_regions(
            self: Arc<Self>,
            ranges: Vec<pdpb::KeyRange>,
            limit: usize,
            options: RegionScanOptions,
        ) -> Result<Vec<RegionWithLeader>> {
            self.batch_scan_count.fetch_add(1, SeqCst);
            self.batch_scan_options.lock().unwrap().push(options);
            if self.batch_scan_unimplemented.load(SeqCst) {
                return Err(Error::Unimplemented);
            }
            let mut regions = self
                .regions
                .lock()
                .await
                .values()
                .filter(|region| {
                    ranges.iter().any(|range| {
                        (region.region.end_key.is_empty()
                            || region.region.end_key.as_slice() > range.start_key.as_slice())
                            && (range.end_key.is_empty()
                                || region.region.start_key.as_slice() < range.end_key.as_slice())
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            regions.sort_by(|left, right| left.region.start_key.cmp(&right.region.start_key));
            regions.dedup_by_key(|region| region.id());
            regions.truncate(limit);
            Ok(regions)
        }

        async fn get_store(
            self: Arc<Self>,
            id: crate::region::StoreId,
        ) -> Result<crate::proto::metapb::Store> {
            self.stores
                .lock()
                .await
                .iter()
                .find(|store| store.id == id)
                .cloned()
                .ok_or_else(|| Error::StringError("MockRetryClient: store not found".to_owned()))
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<crate::proto::metapb::Store>> {
            Ok(self.stores.lock().await.clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            todo!()
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            todo!()
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn cache_is_used() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());
        retry_client.regions.lock().await.insert(
            1,
            RegionWithLeader {
                region: metapb::Region {
                    id: 1,
                    start_key: vec![],
                    end_key: vec![100],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
                buckets: None,
                pending_peers: vec![],
                down_peers: vec![],
            },
        );
        retry_client.regions.lock().await.insert(
            2,
            RegionWithLeader {
                region: metapb::Region {
                    id: 2,
                    start_key: vec![101],
                    end_key: vec![],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    peers: vec![
                        metapb::Peer {
                            id: 2,
                            store_id: 2,
                            ..Default::default()
                        },
                        metapb::Peer {
                            id: 102,
                            store_id: 102,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    id: 2,
                    store_id: 2,
                    ..Default::default()
                }),
                buckets: None,
                pending_peers: vec![],
                down_peers: vec![],
            },
        );

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        // first query, read through
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // should read from cache
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // invalidate, should read through
        cache
            .invalidate_region_cache(cache.get_region_by_id(1).await?.ver_id())
            .await;
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);

        // update leader should work
        cache
            .update_leader(
                cache.get_region_by_id(2).await?.ver_id(),
                metapb::Peer {
                    id: 102,
                    store_id: 102,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            cache.get_region_by_id(2).await?.leader.unwrap().store_id,
            102
        );

        let ver_id = cache.get_region_by_id(2).await?.ver_id();
        cache
            .update_leader(
                ver_id,
                metapb::Peer {
                    id: 999,
                    store_id: 999,
                    ..Default::default()
                },
            )
            .await?;
        // The subsequent lookup must read PD metadata rather than accepting
        // an out-of-region leader hint.
        assert_eq!(cache.get_region_by_id(2).await?.leader.unwrap().store_id, 2);

        Ok(())
    }

    #[tokio::test]
    async fn source_by_id_singleflight_is_released_after_pd_failure() {
        let client = Arc::new(MockRetryClient::default());
        client.get_region_by_id_responses.lock().await.extend([
            Err(Error::StringError("injected PD failure".to_owned())),
            Ok(region_with_leader(42, b"a", b"b")),
        ]);
        let cache = RegionCache::new(client);

        assert_eq!(
            cache.get_region_by_id(42).await.unwrap_err().to_string(),
            "injected PD failure"
        );
        assert!(cache.region_cache.read().await.on_my_way_id.is_empty());
        assert_eq!(cache.get_region_by_id(42).await.unwrap().id(), 42);
        assert!(cache.region_cache.read().await.on_my_way_id.is_empty());
    }

    #[tokio::test]
    async fn source_bucket_aware_pd_lookup_refreshes_only_missing_bucket_metadata() -> Result<()> {
        let client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(client.clone());
        let region = region(1, vec![], vec![]);
        client.regions.lock().await.insert(1, region.clone());
        cache.add_region(region).await;

        let with_buckets = cache
            .get_region_by_key_with_buckets(&vec![1].into())
            .await?;
        assert_eq!(with_buckets.buckets_version(), 9);
        assert_eq!(client.get_region_with_buckets_count.load(SeqCst), 1);

        cache
            .get_region_by_key_with_buckets(&vec![1].into())
            .await?;
        assert_eq!(client.get_region_with_buckets_count.load(SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn source_background_bucket_refresh_is_deduplicated() -> Result<()> {
        let client = Arc::new(MockRetryClient::default());
        let cache = Arc::new(RegionCache::new(client.clone()));
        let region = region(1, vec![], vec![]);
        client.regions.lock().await.insert(1, region.clone());
        cache.add_region(region.clone()).await;
        let ver_id = region.ver_id();
        cache.update_buckets_if_needed(ver_id.clone(), 0, 9);
        cache.update_buckets_if_needed(ver_id, 0, 9);
        for _ in 0..20 {
            if cache.get_region_by_id(1).await?.buckets_version() == 9 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(cache.get_region_by_id(1).await?.buckets_version(), 9);
        assert_eq!(client.get_region_with_buckets_count.load(SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_disjoint_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());
        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;

        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region1);
        expected_cache.insert(vec![10].into(), region2);
        expected_cache.insert(vec![30].into(), region3);

        assert(&cache, &expected_cache).await
    }

    #[tokio::test]
    async fn source_bucket_mismatch_only_replaces_older_cached_metadata() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        let region = region(1, vec![], vec![10]);
        let ver_id = region.ver_id();
        cache.add_region(region).await;

        cache
            .update_buckets(ver_id.clone(), 2, vec![vec![], vec![5], vec![10]])
            .await;
        let current = cache.get_region_by_id(1).await.unwrap();
        assert_eq!(current.buckets_version(), 2);
        assert_eq!(
            current.buckets.unwrap().keys,
            vec![vec![], vec![5], vec![10]]
        );

        cache.update_buckets(ver_id.clone(), 2, vec![vec![9]]).await;
        assert_eq!(
            cache
                .get_region_by_id(1)
                .await
                .unwrap()
                .buckets
                .unwrap()
                .keys,
            vec![vec![], vec![5], vec![10]]
        );

        cache
            .update_buckets(ver_id, 3, vec![vec![], vec![10]])
            .await;
        let current = cache.get_region_by_id(1).await.unwrap();
        assert_eq!(current.buckets_version(), 3);
        assert_eq!(current.buckets.unwrap().region_id, 1);
    }

    #[tokio::test]
    async fn test_add_intersecting_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());

        cache.add_region(region(1, vec![], vec![10])).await;
        cache.add_region(region(2, vec![10], vec![20])).await;
        cache.add_region(region(3, vec![30], vec![40])).await;
        cache.add_region(region(4, vec![50], vec![60])).await;
        cache.add_region(region(5, vec![20], vec![35])).await;

        let mut expected_cache: BTreeMap<Key, _> = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![10].into(), region(2, vec![10], vec![20]));
        expected_cache.insert(vec![20].into(), region(5, vec![20], vec![35]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(6, vec![15], vec![25])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![15].into(), region(6, vec![15], vec![25]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(7, vec![20], vec![])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(8, vec![], vec![15])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(8, vec![], vec![15]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;
    }

    #[tokio::test]
    async fn source_region_insert_rejects_stale_epochs_and_removes_max_end_intersections() {
        let cache = Arc::new(RegionCache::new(Arc::new(MockRetryClient::default())));

        let mut latest = region(1, vec![10], vec![20]);
        latest.region.region_epoch.as_mut().unwrap().version = 2;
        assert!(cache.add_region(latest.clone()).await);

        let mut stale = region(1, vec![10], vec![20]);
        stale.region.region_epoch.as_mut().unwrap().version = 1;
        assert!(!cache.add_region(stale).await);

        let mut expected = BTreeMap::new();
        expected.insert(vec![10].into(), latest);
        assert(&cache, &expected).await;

        let max_end = region(2, vec![20], vec![]);
        assert!(cache.add_region(max_end).await);
        let mut replacement = region(3, vec![15], vec![30]);
        replacement.region.region_epoch.as_mut().unwrap().version = 2;
        assert!(cache.add_region(replacement.clone()).await);

        expected.clear();
        expected.insert(vec![15].into(), replacement);
        assert(&cache, &expected).await;
    }

    #[tokio::test]
    async fn source_cache_miss_retries_pd_metadata_rejected_as_stale() -> Result<()> {
        let setup = |id, start: &[u8], end: &[u8], version| {
            let mut region = region_with_leader(id, start, end);
            region.region.region_epoch.as_mut().unwrap().version = version;
            region
        };

        let client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(client.clone());
        assert!(cache.add_region(setup(10, b"b", b"d", 2)).await);
        client
            .get_region_responses
            .lock()
            .await
            .extend([Ok(setup(11, b"", b"c", 1)), Ok(setup(12, b"", b"b", 3))]);
        assert_eq!(
            cache.get_region_by_key(&b"a".to_vec().into()).await?.id(),
            12
        );
        assert_eq!(client.get_region_count.load(SeqCst), 2);
        assert_eq!(
            cache
                .region_cache
                .read()
                .await
                .key_to_ver_id
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![b"".to_vec().into(), b"b".to_vec().into()]
        );

        let client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(client.clone());
        assert!(cache.add_region(setup(20, b"b", b"d", 2)).await);
        client
            .get_prev_region_responses
            .lock()
            .await
            .extend([Ok(setup(21, b"", b"c", 1)), Ok(setup(22, b"", b"b", 3))]);
        assert_eq!(
            cache
                .get_region_by_end_key(&b"b".to_vec().into())
                .await?
                .id(),
            22
        );
        assert_eq!(client.get_region_count.load(SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn source_region_ttl_refreshes_live_entries_and_reloads_expired_ones() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());
        let cached = region(1, vec![], vec![]);
        retry_client
            .regions
            .lock()
            .await
            .insert(cached.id(), cached.clone());
        cache.add_region(cached.clone()).await;

        let now = now_epoch_secs();
        {
            let mut guard = cache.region_cache.write().await;
            guard
                .ver_id_to_region
                .get_mut(&cached.ver_id())
                .unwrap()
                .ttl = now;
        }
        assert_eq!(cache.get_region_by_key(&vec![1].into()).await?, cached);
        {
            let guard = cache.region_cache.read().await;
            assert!(guard.ver_id_to_region[&cached.ver_id()].ttl >= now + REGION_CACHE_TTL_SECS);
        }

        {
            let mut guard = cache.region_cache.write().await;
            guard
                .ver_id_to_region
                .get_mut(&cached.ver_id())
                .unwrap()
                .ttl = now_epoch_secs() - 1;
        }
        assert_eq!(cache.get_region_by_key(&vec![1].into()).await?, cached);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn source_region_sync_flags_delay_reload_and_preserve_old_region_on_pd_failure() {
        let client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(client.clone());
        let old = region(1, vec![], vec![]);
        assert!(cache.add_region(old.clone()).await);

        assert!(cache.mark_region_delayed_reload(&old.ver_id()).await);
        assert_eq!(cache.get_region_by_key(&vec![1].into()).await.unwrap(), old);
        assert_eq!(client.get_region_count.load(SeqCst), 0);

        assert_eq!(cache.gc_round_at(now_epoch_secs(), 50).await, (1, 0, false));
        assert!(
            cache.region_cache.read().await.ver_id_to_region[&old.ver_id()]
                .has_sync_flags(NEED_DELAYED_RELOAD_READY)
        );

        // A source reload failure is deliberately hidden: callers retain the
        // old live region and the cache requests another synchronous reload.
        assert_eq!(cache.get_region_by_id(1).await.unwrap(), old);
        assert_eq!(client.get_region_count.load(SeqCst), 1);
        assert!(
            cache.region_cache.read().await.ver_id_to_region[&old.ver_id()]
                .has_sync_flags(NEED_RELOAD_ON_ACCESS)
        );

        let mut refreshed = old.clone();
        refreshed.region.region_epoch.as_mut().unwrap().version = 1;
        client.regions.lock().await.insert(1, refreshed.clone());
        assert_eq!(cache.get_region_by_id(1).await.unwrap(), refreshed);
        assert_eq!(client.get_region_count.load(SeqCst), 2);
        assert!(
            cache.region_cache.read().await.ver_id_to_region[&refreshed.ver_id()].sync_flags == 0
        );
    }

    #[tokio::test]
    async fn source_down_peers_freeze_region_ttl_on_insert() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        let mut cached = region(1, vec![], vec![]);
        cached.down_peers.push(crate::proto::pdpb::PeerStats {
            peer: Some(metapb::Peer {
                id: 9,
                store_id: 9,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(cache.add_region(cached.clone()).await);
        let now = now_epoch_secs();
        cache
            .region_cache
            .write()
            .await
            .ver_id_to_region
            .get_mut(&cached.ver_id())
            .unwrap()
            .ttl = now;

        assert_eq!(cache.get_region_by_id(1).await.unwrap(), cached);
        let regions = cache.region_cache.read().await;
        let cached = &regions.ver_id_to_region[&cached.ver_id()];
        assert_eq!(cached.ttl, now);
        assert!(cached.has_sync_flags(NEED_EXPIRE_AFTER_TTL));
    }

    #[tokio::test]
    async fn source_gc_round_is_bounded_and_expires_regions_with_unhealthy_stores() {
        let cache = Arc::new(RegionCache::new(Arc::new(MockRetryClient::default())));
        cache.store_cache.write().unwrap().insert(
            9,
            CachedStore::new(metapb::Store {
                id: 9,
                ..Default::default()
            }),
        );
        let first = region(1, vec![], vec![10]);
        let mut unhealthy = region(2, vec![10], vec![20]);
        unhealthy.region.peers.push(metapb::Peer {
            id: 2,
            store_id: 9,
            ..Default::default()
        });
        let last = region(3, vec![20], vec![]);
        assert!(cache.add_region(first.clone()).await);
        assert!(cache.add_region(unhealthy.clone()).await);
        assert!(cache.add_region(last.clone()).await);

        let now = now_epoch_secs();
        cache
            .region_cache
            .write()
            .await
            .ver_id_to_region
            .get_mut(&first.ver_id())
            .unwrap()
            .ttl = now - 1;
        assert!(cache.set_store_liveness(9, StoreLiveness::Unreachable));

        assert_eq!(cache.gc_round_at(now, 1).await, (1, 1, true));
        assert_eq!(cache.gc_round_at(now, 1).await, (1, 0, true));
        assert!(
            cache.region_cache.read().await.ver_id_to_region[&unhealthy.ver_id()]
                .has_sync_flags(NEED_EXPIRE_AFTER_TTL)
        );
        assert_eq!(cache.gc_round_at(now, 1).await, (1, 0, false));

        cache
            .region_cache
            .write()
            .await
            .ver_id_to_region
            .get_mut(&unhealthy.ver_id())
            .unwrap()
            .ttl = now - 1;
        assert_eq!(cache.gc_round_at(now, 1).await, (1, 1, true));
        assert!(!cache
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .contains_key(&unhealthy.ver_id()));

        cache.start_background_gc();
        assert!(cache.background_task.lock().unwrap().is_some());
        cache.close_background_task().await;
        assert!(cache.background_task.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn source_full_region_refresh_replaces_indexes_and_can_run_periodically() -> Result<()> {
        let client = Arc::new(MockRetryClient::default());
        for region in [
            region_with_leader(1, b"", b"b"),
            region_with_leader(2, b"b", b"d"),
            region_with_leader(3, b"d", b""),
        ] {
            client.regions.lock().await.insert(region.id(), region);
        }
        let cache = Arc::new(RegionCache::new(client));
        assert!(cache.add_region(region_with_leader(99, b"x", b"z")).await);
        cache
            .region_cache
            .write()
            .await
            .on_my_way_id
            .insert(7, Arc::new(Notify::new()));

        let mut backoffer = RetryBackoffer::noop(Cancellation::default());
        cache.refresh_region_index(&mut backoffer).await?;
        let index = cache.region_cache.read().await;
        assert_eq!(
            index
                .key_to_ver_id
                .values()
                .map(|version| version.id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(index.on_my_way_id.contains_key(&7));
        drop(index);

        let client = Arc::new(MockRetryClient::default());
        let only = region_with_leader(8, b"", b"");
        client.regions.lock().await.insert(only.id(), only);
        let periodic = Arc::new(RegionCache::new(client));
        periodic.start_background_refresh(Duration::from_millis(5));
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if periodic
                    .region_cache
                    .read()
                    .await
                    .id_to_ver_id
                    .contains_key(&8)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("periodic region refresh should run");
        periodic.close_background_task().await;
        assert!(periodic.background_task.lock().unwrap().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn source_health_feedback_updates_only_the_owning_cached_store() {
        let cache = Arc::new(RegionCache::new(Arc::new(MockRetryClient::default())));
        cache.store_cache.write().unwrap().insert(
            7,
            CachedStore::new(metapb::Store {
                id: 7,
                ..Default::default()
            }),
        );
        let feedback = crate::proto::kvrpcpb::HealthFeedback {
            store_id: 7,
            slow_score: 80,
            ..Default::default()
        };
        cache.client_event_listener().on_health_feedback(&feedback);
        assert_eq!(cache.store_health(7).unwrap().tikv_side_slow_score, 80);
        assert!(cache.store_health(7).unwrap().is_slow());
        assert_eq!(cache.store_liveness(7), Some(StoreLiveness::Reachable));
        assert!(cache.set_store_liveness(7, StoreLiveness::Unreachable));
        assert_eq!(cache.store_liveness(7), Some(StoreLiveness::Unreachable));
        assert!(cache.begin_store_health_check(7));
        assert!(!cache.begin_store_health_check(7));
        cache.finish_store_health_check(7);
        assert!(cache.begin_store_health_check(7));
        cache.finish_store_health_check(7);
        assert_eq!(StoreLiveness::Unknown.to_string(), "unknown");
        cache
            .store_cache
            .write()
            .unwrap()
            .get_mut(&7)
            .unwrap()
            .update_metadata(metapb::Store {
                id: 7,
                address: "new-address".to_owned(),
                ..Default::default()
            });
        assert_eq!(cache.store_liveness(7), Some(StoreLiveness::Unreachable));
        assert_eq!(cache.store_health(7).unwrap().tikv_side_slow_score, 80);

        cache.record_health_feedback(&crate::proto::kvrpcpb::HealthFeedback {
            store_id: 8,
            slow_score: 100,
            ..Default::default()
        });
        assert!(cache.store_health(8).is_none());

        cache.store_cache.write().unwrap().insert(
            9,
            CachedStore::new(metapb::Store {
                id: 9,
                ..Default::default()
            }),
        );
        let start = std::time::Instant::now();
        cache.record_health_feedback_at(
            &crate::proto::kvrpcpb::HealthFeedback {
                store_id: 9,
                slow_score: 40,
                ..Default::default()
            },
            start,
        );
        cache.tick_store_health(start + std::time::Duration::from_secs(15));
        assert_eq!(cache.store_health(9).unwrap().tikv_side_slow_score, 35);
    }

    #[tokio::test]
    async fn source_store_reresolve_updates_metadata_without_resetting_runtime_state() -> Result<()>
    {
        let client = Arc::new(MockRetryClient::default());
        client.stores.lock().await.push(metapb::Store {
            id: 7,
            address: "old-address".to_owned(),
            peer_address: "old-peer".to_owned(),
            status_address: "old-status".to_owned(),
            labels: vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "old".to_owned(),
            }],
            ..Default::default()
        });
        let cache = RegionCache::new(client.clone());
        assert_eq!(cache.get_store_by_id(7).await?.address, "old-address");
        let health = cache.store_health_status(7).unwrap();
        cache.record_health_feedback(&crate::proto::kvrpcpb::HealthFeedback {
            store_id: 7,
            slow_score: 80,
            ..Default::default()
        });
        assert!(cache.set_store_liveness(7, StoreLiveness::Unreachable));
        cache.record_server_load(7, 1_000);
        cache.store_cache.read().unwrap()[&7]
            .epoch
            .store(9, std::sync::atomic::Ordering::Release);

        *client.stores.lock().await = vec![metapb::Store {
            id: 7,
            address: "new-address".to_owned(),
            peer_address: "new-peer".to_owned(),
            status_address: "new-status".to_owned(),
            labels: vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "new".to_owned(),
            }],
            ..Default::default()
        }];
        let refreshed = cache.refresh_store_by_id(7).await?;
        assert_eq!(refreshed.address, "new-address");
        assert_eq!(refreshed.peer_address, "new-peer");
        assert_eq!(refreshed.status_address, "new-status");
        assert_eq!(refreshed.labels[0].value, "new");
        assert!(Arc::ptr_eq(&health, &cache.store_health_status(7).unwrap()));
        assert_eq!(cache.store_health(7).unwrap().tikv_side_slow_score, 80);
        assert_eq!(cache.store_liveness(7), Some(StoreLiveness::Unreachable));
        assert_eq!(
            cache.store_cache.read().unwrap()[&7]
                .epoch
                .load(std::sync::atomic::Ordering::Acquire),
            9
        );
        assert!(cache.estimated_store_wait(7).unwrap() > Duration::ZERO);
        Ok(())
    }

    #[test]
    fn source_server_load_estimate_decays_from_the_last_tikv_wait() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        cache.store_cache.write().unwrap().insert(
            7,
            CachedStore::new(metapb::Store {
                id: 7,
                ..Default::default()
            }),
        );
        let start = std::time::Instant::now();
        cache.record_server_load_at(7, 1_000, start);
        assert_eq!(
            cache.estimated_store_wait_at(7, start + std::time::Duration::from_millis(250)),
            Some(std::time::Duration::from_millis(750))
        );
        assert_eq!(
            cache.estimated_store_wait_at(7, start + std::time::Duration::from_millis(1_001)),
            Some(std::time::Duration::ZERO)
        );
        cache.record_server_load(8, 100);
        assert_eq!(cache.estimated_store_wait(8), None);
    }

    #[tokio::test]
    async fn source_replica_candidates_join_region_peers_to_cached_store_state() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        cache.store_cache.write().unwrap().insert(
            1,
            CachedStore::new(metapb::Store {
                id: 1,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "a".to_owned(),
                }],
                ..Default::default()
            }),
        );
        cache.store_cache.write().unwrap().insert(
            2,
            CachedStore::new(metapb::Store {
                id: 2,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "b".to_owned(),
                }],
                ..Default::default()
            }),
        );
        cache.set_store_liveness(2, StoreLiveness::Unreachable);
        cache.store_cache.write().unwrap().insert(
            3,
            CachedStore::new(metapb::Store {
                id: 3,
                labels: vec![metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash".to_owned(),
                }],
                ..Default::default()
            }),
        );

        let leader = metapb::Peer {
            id: 11,
            store_id: 1,
            ..Default::default()
        };
        let follower = metapb::Peer {
            id: 12,
            store_id: 2,
            role: metapb::PeerRole::Learner.into(),
            ..Default::default()
        };
        let tiflash_peer = metapb::Peer {
            id: 13,
            store_id: 3,
            ..Default::default()
        };
        let mut region = region(1, vec![], vec![]);
        region.region.peers = vec![leader.clone(), follower.clone(), tiflash_peer];
        region.leader = Some(leader);
        let mut selector_state = ReplicaSelectorState::default();
        selector_state.record_attempt(12);
        let candidates = cache
            .replica_candidates(
                &region,
                &[metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "a".to_owned(),
                }],
                &[],
                &selector_state,
            )
            .await
            .unwrap();
        assert_eq!(
            candidates,
            vec![
                ReplicaCandidate {
                    peer_id: 11,
                    is_leader: true,
                    is_learner: false,
                    label_matches: true,
                    is_slow: false,
                    reachable: true,
                    attempts: 0,
                    data_is_not_ready: false,
                },
                ReplicaCandidate {
                    peer_id: 12,
                    is_leader: false,
                    is_learner: true,
                    label_matches: false,
                    is_slow: false,
                    reachable: false,
                    attempts: 1,
                    data_is_not_ready: false,
                },
            ]
        );

        assert!(cache.set_store_liveness(2, StoreLiveness::Reachable));
        let selected = cache
            .select_mixed_replica(
                &region,
                &[],
                &[],
                &ReplicaSelectorState::default(),
                MixedReplicaSelection {
                    read_type: ReplicaReadType::Follower,
                    leader_only: false,
                    prefer_leader: false,
                    labels_requested: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(selected, Some(follower.clone()));

        cache.record_server_load(1, 1_000);
        let idle = cache
            .select_idle_replica(
                &region,
                &[],
                &[],
                &ReplicaSelectorState::default(),
                std::time::Duration::from_millis(500),
            )
            .await
            .unwrap();
        assert_eq!(idle, Some(follower.clone()));

        let mut busy_follower = ReplicaSelectorState::default();
        busy_follower.record_server_busy(follower.id);
        let no_idle = cache
            .select_idle_replica(
                &region,
                &[],
                &[],
                &busy_follower,
                std::time::Duration::from_millis(500),
            )
            .await
            .unwrap();
        assert_eq!(no_idle, None);

        let selected = cache
            .select_mixed_replica(
                &region,
                &[],
                &[1],
                &ReplicaSelectorState::default(),
                MixedReplicaSelection {
                    read_type: ReplicaReadType::Follower,
                    leader_only: false,
                    prefer_leader: false,
                    labels_requested: false,
                },
            )
            .await
            .unwrap();
        // Source `WithMatchStores` raises the matching store's score; it does
        // not make non-matching stores ineligible.
        assert_eq!(selected, region.leader.clone());

        assert!(cache.set_store_liveness(1, StoreLiveness::Unreachable));
        let proxy = cache.proxy_for_unreachable_leader(&region).await.unwrap();
        assert_eq!(proxy, Some(follower));
    }

    #[tokio::test]
    async fn source_tiflash_selection_rotates_only_tiflash_peers() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        let leader = metapb::Peer {
            id: 11,
            store_id: 1,
            ..Default::default()
        };
        let flash_one = metapb::Peer {
            id: 12,
            store_id: 2,
            ..Default::default()
        };
        let flash_two = metapb::Peer {
            id: 13,
            store_id: 3,
            ..Default::default()
        };
        let mut region = region(1, vec![], vec![]);
        region.region.peers = vec![leader.clone(), flash_one.clone(), flash_two.clone()];
        region.leader = Some(leader);
        region.pending_peers = vec![flash_two.clone()];
        cache.add_region(region.clone()).await;
        for (id, engine, zone) in [(1, "tikv", "a"), (2, "tiflash", "a"), (3, "tiflash", "b")] {
            cache.store_cache.write().unwrap().insert(
                id,
                CachedStore::new(metapb::Store {
                    id,
                    labels: vec![
                        metapb::StoreLabel {
                            key: "engine".into(),
                            value: engine.into(),
                        },
                        metapb::StoreLabel {
                            key: "zone".into(),
                            value: zone.into(),
                        },
                    ],
                    ..Default::default()
                }),
            );
        }
        assert_eq!(
            cache.select_tiflash_peer(&region, true, &[]).await,
            Ok(flash_two.clone())
        );
        assert_eq!(
            cache.select_tiflash_peer(&region, true, &[]).await,
            Ok(flash_one.clone())
        );
        assert_eq!(
            cache
                .select_tiflash_peer(
                    &region,
                    false,
                    &[metapb::StoreLabel {
                        key: "zone".into(),
                        value: "b".into()
                    }]
                )
                .await,
            Ok(flash_two.clone())
        );
        assert_eq!(
            cache
                .select_tiflash_peer(
                    &region,
                    false,
                    &[metapb::StoreLabel {
                        key: "zone".into(),
                        value: "missing".into()
                    }]
                )
                .await,
            Err(super::TiFlashSelectionError::AllStoresFiltered)
        );
        assert_eq!(
            cache.valid_tiflash_store_ids(&region, 2, &[]).await,
            (vec![2, 3], vec![2])
        );
        region.region.peers[2].is_witness = true;
        assert_eq!(
            cache.select_tiflash_peer(&region, false, &[]).await,
            Ok(flash_one.clone())
        );
        assert_eq!(
            cache.valid_tiflash_store_ids(&region, 2, &[]).await,
            (vec![2], vec![2])
        );
        region.region.peers[2].is_witness = false;
        region.down_peers = vec![crate::proto::pdpb::PeerStats {
            peer: Some(flash_two),
            ..Default::default()
        }];
        assert_eq!(
            cache.select_tiflash_peer(&region, false, &[]).await,
            Ok(flash_one)
        );
        assert_eq!(
            cache.valid_tiflash_store_ids(&region, 2, &[]).await,
            (vec![2], vec![2])
        );
    }

    #[tokio::test]
    async fn source_store_failure_epoch_invalidates_only_its_cached_snapshot() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        cache.store_cache.write().unwrap().insert(
            1,
            CachedStore::new(metapb::Store {
                id: 1,
                ..Default::default()
            }),
        );
        let peer = metapb::Peer {
            id: 11,
            store_id: 1,
            ..Default::default()
        };
        let mut region = region(1, vec![], vec![]);
        region.region.peers = vec![peer.clone()];
        region.leader = Some(peer);
        assert!(cache.add_region(region.clone()).await);

        assert!(!cache.store_epoch_is_stale(&region.ver_id(), 1).await);
        assert!(
            cache
                .invalidate_store_epoch_for_region(&region.ver_id(), 1)
                .await
        );
        assert!(cache.store_epoch_is_stale(&region.ver_id(), 1).await);
        assert!(
            !cache
                .invalidate_store_epoch_for_region(&region.ver_id(), 1)
                .await
        );
        assert!(cache
            .replica_candidates(&region, &[], &[], &ReplicaSelectorState::default())
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_get_region_by_key() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());

        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![40]);
        let region4 = region(4, vec![50], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;
        cache.add_region(region4.clone()).await;

        assert_eq!(
            cache.get_region_by_key(&vec![].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![5].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![10].into()).await?,
            region2.clone()
        );
        assert!(cache.get_region_by_key(&vec![20].into()).await.is_err());
        assert!(cache.get_region_by_key(&vec![25].into()).await.is_err());
        assert_eq!(cache.get_region_by_key(&vec![60].into()).await?, region4);
        Ok(())
    }

    #[tokio::test]
    async fn end_key_lookup_treats_region_starts_as_belonging_to_the_previous_region() -> Result<()>
    {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client);
        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;

        assert_eq!(
            cache.get_region_by_end_key(&vec![10].into()).await?,
            region1
        );
        assert_eq!(
            cache.get_region_by_end_key(&vec![11].into()).await?,
            region2.clone()
        );
        assert_eq!(
            cache.get_region_by_end_key(&vec![20].into()).await?,
            region2
        );
        Ok(())
    }

    // a helper function to assert the cache is in expected state
    async fn assert(
        cache: &RegionCache<MockRetryClient>,
        expected_cache: &BTreeMap<Key, RegionWithLeader>,
    ) {
        let guard = cache.region_cache.read().await;
        let mut actual_keys = guard
            .ver_id_to_region
            .values()
            .map(|cached| &cached.region)
            .collect::<Vec<_>>();
        let mut expected_keys = expected_cache.values().collect::<Vec<_>>();
        actual_keys.sort_by_cached_key(|r| r.id());
        expected_keys.sort_by_cached_key(|r| r.id());

        assert_eq!(actual_keys, expected_keys);
        assert_eq!(
            guard.key_to_ver_id.keys().collect::<HashSet<_>>(),
            expected_cache.keys().collect::<HashSet<_>>()
        )
    }

    fn region(id: RegionId, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = id;
        region.region.start_key = start_key;
        region.region.end_key = end_key;
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });
        // We don't care about other fields here

        region
    }

    fn key_range(start_key: &[u8], end_key: &[u8]) -> pdpb::KeyRange {
        pdpb::KeyRange {
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
        }
    }

    fn region_with_leader(id: RegionId, start_key: &[u8], end_key: &[u8]) -> RegionWithLeader {
        let mut region = region(id, start_key.to_vec(), end_key.to_vec());
        region.leader = Some(metapb::Peer {
            id: id + 100,
            store_id: id + 200,
            ..Default::default()
        });
        region
    }

    fn region_ids(regions: &[RegionWithLeader]) -> Vec<RegionId> {
        regions.iter().map(RegionWithLeader::id).collect()
    }

    #[test]
    fn source_batch_scan_detects_gaps_across_ranges() {
        let check = |ranges: &[&str], regions: &[&str], limit: isize, expected: bool| {
            let ranges = ranges
                .chunks_exact(2)
                .map(|pair| key_range(pair[0].as_bytes(), pair[1].as_bytes()))
                .collect::<Vec<_>>();
            let regions = regions
                .chunks_exact(2)
                .enumerate()
                .map(|(index, pair)| {
                    region(
                        index as u64 + 1,
                        pair[0].as_bytes().to_vec(),
                        pair[1].as_bytes().to_vec(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                regions_have_gap_in_ranges(
                    &ranges,
                    &regions,
                    (limit >= 0).then_some(limit as usize),
                ),
                expected,
                "ranges={ranges:?}, regions={regions:?}, limit={limit}"
            );
        };

        for bounds in [
            vec!["a", "c"],
            vec!["a", "b", "b", "c"],
            vec!["a", "a1", "a1", "b", "b", "b1", "b1", "c"],
        ] {
            for regions in [
                vec!["a", "c"],
                vec!["a", ""],
                vec!["", "c"],
                vec!["a", "b", "b", "c"],
                vec!["", "b", "b", "c"],
                vec!["a", "b", "b", ""],
                vec!["", "b", "b", ""],
            ] {
                check(&bounds, &regions, -1, false);
            }
            for regions in [
                vec!["a", "b"],
                vec!["b", "c"],
                vec!["b", ""],
                vec!["", "b"],
                vec!["a", "b", "b1", "c"],
                vec!["", "b", "b1", "c"],
                vec!["a", "b", "b1", ""],
                vec!["", "b", "b1", ""],
                vec![],
            ] {
                check(&bounds, &regions, -1, true);
            }
        }

        for ranges in [
            vec!["a", "b", "c", "d"],
            vec!["a", "b1", "b1", "b", "c", "d"],
            vec!["a", "b", "c", "c1", "c1", "d"],
            vec!["a", "b1", "b1", "b", "c", "c1", "c1", "d"],
        ] {
            for regions in [vec!["a", "d"], vec!["", "d"], vec!["a", ""], vec!["", ""]] {
                check(&ranges, &regions, -1, false);
            }
            for regions in [
                vec!["a", "b"],
                vec!["b", "c"],
                vec!["c", "d"],
                vec!["", "b"],
                vec!["c", ""],
            ] {
                check(&ranges, &regions, -1, true);
            }
        }

        for ranges in [
            vec!["", ""],
            vec!["", "b", "b", ""],
            vec!["", "a1", "a1", "b", "b", "b1", "b1", ""],
        ] {
            for regions in [vec!["", ""], vec!["", "b", "b", ""]] {
                check(&ranges, &regions, -1, false);
            }
            for regions in [
                vec!["a", "c"],
                vec!["a", ""],
                vec!["", "c"],
                vec!["", "b", "b1", ""],
                vec!["a", "b", "b", ""],
                vec!["", "b", "b", "c"],
                vec![],
            ] {
                check(&ranges, &regions, -1, true);
            }
        }

        check(&["", "b"], &["", "a"], -1, true);
        check(&["", "b"], &["", "a"], 1, false);
        check(&["", "b"], &["", "a"], 2, true);
        check(&["a", ""], &["b", ""], -1, true);
        check(&["a", ""], &["b", ""], 1, true);
        check(&["a", ""], &["b", "c"], 1, true);
        check(&["a", ""], &["a", ""], -1, false);
    }

    #[test]
    fn source_ranges_after_key_splits_and_discards_finished_ranges() {
        let check = |range_keys: &[&str], split_key: &str, expected: &[&str]| {
            let ranges = range_keys
                .chunks_exact(2)
                .map(|pair| key_range(pair[0].as_bytes(), pair[1].as_bytes()))
                .collect::<Vec<_>>();
            let actual = ranges_after_key(ranges, split_key.as_bytes())
                .into_iter()
                .flat_map(|range| [range.start_key, range.end_key])
                .map(|key| String::from_utf8(key).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        };
        for (ranges, split, expected) in [
            (vec!["a", "c"], "a", vec!["a", "c"]),
            (vec!["b", "c"], "a", vec!["b", "c"]),
            (vec!["a", "c"], "b", vec!["b", "c"]),
            (vec!["a", "c"], "c", vec![]),
            (vec!["a", "c"], "", vec![]),
            (vec!["a", ""], "b", vec!["b", ""]),
            (vec!["a", ""], "", vec![]),
            (vec!["a", "b", "c", "f"], "a1", vec!["a1", "b", "c", "f"]),
            (vec!["a", "b", "c", "f"], "b", vec!["c", "f"]),
            (vec!["a", "b", "c", "f"], "b1", vec!["c", "f"]),
            (vec!["a", "b", "c", "f"], "c", vec!["c", "f"]),
            (vec!["a", "b", "c", "f"], "d", vec!["d", "f"]),
        ] {
            check(&ranges, split, &expected);
        }
    }

    #[test]
    fn source_batch_locate_merger_prefers_loaded_regions_over_stale_cache() {
        let check = |loaded: &[&str], cached: &[&str], expected: &[&str]| {
            let to_regions = |keys: &[&str]| {
                keys.chunks_exact(2)
                    .enumerate()
                    .map(|(index, pair)| {
                        region(
                            index as u64 + 1,
                            pair[0].as_bytes().to_vec(),
                            pair[1].as_bytes().to_vec(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let mut merger = BatchLocateRegionMerger::new(to_regions(cached), 0);
            for region in to_regions(loaded) {
                merger.append_region(region);
            }
            let actual = merger
                .build()
                .into_iter()
                .flat_map(|region| [region.region.start_key, region.region.end_key])
                .map(|key| String::from_utf8(key).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        };
        for (loaded, cached, expected) in [
            (
                vec!["b", "c", "c", "d"],
                vec!["a", "b"],
                vec!["a", "b", "b", "c", "c", "d"],
            ),
            (
                vec!["a", "b", "c", "d"],
                vec!["b", "c"],
                vec!["a", "b", "b", "c", "c", "d"],
            ),
            (
                vec!["a", "b", "b", "c"],
                vec!["c", "d"],
                vec!["a", "b", "b", "c", "c", "d"],
            ),
            (vec!["", ""], vec!["a", "b", "b", "c"], vec!["", ""]),
            (
                vec!["", "b"],
                vec!["a", "b", "b", "c"],
                vec!["", "b", "b", "c"],
            ),
            (
                vec!["b", ""],
                vec!["a", "b", "b", "c"],
                vec!["a", "b", "b", ""],
            ),
            (
                vec!["b", ""],
                vec!["a", "b", "c", "d"],
                vec!["a", "b", "b", ""],
            ),
            (
                vec!["b", "e"],
                vec!["a", "b", "c", "d"],
                vec!["a", "b", "b", "e"],
            ),
            (
                vec!["b", "i"],
                vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
                vec!["a", "b", "b", "i", "i", "j"],
            ),
            (
                vec!["b", "d"],
                vec!["a", "b", "c", "e"],
                vec!["a", "b", "b", "d", "c", "e"],
            ),
            (
                vec!["b", "d", "d", "f"],
                vec!["a", "b", "c", "e"],
                vec!["a", "b", "b", "d", "d", "f"],
            ),
            (
                vec!["b", "d", "d", "e", "e", "g"],
                vec!["a", "b", "c", "f"],
                vec!["a", "b", "b", "d", "d", "e", "e", "g"],
            ),
            (
                vec!["b", "d", "d", "e", "f", "h"],
                vec!["a", "b", "c", "g"],
                vec!["a", "b", "b", "d", "d", "e", "c", "g", "f", "h"],
            ),
        ] {
            check(&loaded, &cached, &expected);
        }
    }

    #[tokio::test]
    async fn source_batch_locate_reuses_cache_and_falls_back_to_scan_regions() -> Result<()> {
        let client = Arc::new(MockRetryClient::default());
        for (id, start, end) in [
            (1, b"".as_slice(), b"a".as_slice()),
            (2, b"a".as_slice(), b"b".as_slice()),
            (3, b"b".as_slice(), b"c".as_slice()),
            (4, b"c".as_slice(), b"d".as_slice()),
            (5, b"d".as_slice(), b"e".as_slice()),
            (6, b"e".as_slice(), b"f".as_slice()),
            (7, b"f".as_slice(), b"g".as_slice()),
            (8, b"g".as_slice(), b"".as_slice()),
        ] {
            client
                .regions
                .lock()
                .await
                .insert(id, region_with_leader(id, start, end));
        }
        let ranges = vec![key_range(b"a", b"d"), key_range(b"e", b"g")];

        let cache = RegionCache::new(client.clone());
        cache
            .add_region(client.regions.lock().await[&2].clone())
            .await;
        cache
            .add_region(client.regions.lock().await[&6].clone())
            .await;
        let mut backoffer = RetryBackoffer::noop(Cancellation::default());
        let located = cache
            .batch_locate_key_ranges(ranges.clone(), true, true, &mut backoffer)
            .await?;
        assert_eq!(region_ids(&located), vec![2, 3, 4, 6, 7]);
        assert_eq!(client.batch_scan_count.load(SeqCst), 1);
        assert_eq!(
            client.batch_scan_options.lock().unwrap().as_slice(),
            &[RegionScanOptions {
                need_buckets: true,
                contain_all_key_range: true,
            }]
        );

        client.batch_scan_unimplemented.store(true, SeqCst);
        let fallback_cache = RegionCache::new(client.clone());
        let mut backoffer = RetryBackoffer::noop(Cancellation::default());
        let located = fallback_cache
            .batch_locate_key_ranges(ranges, false, true, &mut backoffer)
            .await?;
        assert_eq!(region_ids(&located), vec![2, 3, 4, 6, 7]);
        assert_eq!(client.batch_scan_count.load(SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn source_cached_range_scan_stops_at_expiration_and_holes() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        for region in [
            region_with_leader(2, b"a", b"b"),
            region_with_leader(3, b"b", b"c"),
            region_with_leader(5, b"d", b"e"),
        ] {
            cache.add_region(region).await;
        }

        assert_eq!(
            region_ids(
                &cache
                    .scan_regions_from_cache(&b"a".to_vec().into(), &b"e".to_vec().into(), 128)
                    .await
            ),
            vec![2, 3]
        );

        let version = region_with_leader(3, b"b", b"c").ver_id();
        cache
            .region_cache
            .write()
            .await
            .ver_id_to_region
            .get_mut(&version)
            .unwrap()
            .ttl = now_epoch_secs() - 1;
        assert_eq!(
            region_ids(
                &cache
                    .scan_regions_from_cache(&b"a".to_vec().into(), &b"e".to_vec().into(), 128)
                    .await
            ),
            vec![2]
        );
    }

    #[tokio::test]
    async fn source_region_range_helpers_preserve_half_open_and_inclusive_bounds() -> Result<()> {
        let client = Arc::new(MockRetryClient::default());
        for (id, start, end) in [
            (1, b"".as_slice(), b"a".as_slice()),
            (2, b"a".as_slice(), b"b".as_slice()),
            (3, b"b".as_slice(), b"c".as_slice()),
            (4, b"c".as_slice(), b"d".as_slice()),
            (5, b"d".as_slice(), b"".as_slice()),
        ] {
            client
                .regions
                .lock()
                .await
                .insert(id, region_with_leader(id, start, end));
        }
        let cache = RegionCache::new(client.clone());

        let mut backoffer = RetryBackoffer::noop(Cancellation::default());
        let loaded = cache
            .batch_load_regions_with_key_range(
                b"a".to_vec().into(),
                b"c".to_vec().into(),
                2,
                &mut backoffer,
            )
            .await?;
        assert_eq!(region_ids(&loaded), vec![2, 3]);

        let mut backoffer = RetryBackoffer::noop(Cancellation::default());
        let loaded = cache
            .load_regions_in_key_range(b"a".to_vec().into(), b"d".to_vec().into(), &mut backoffer)
            .await?;
        assert_eq!(region_ids(&loaded), vec![2, 3, 4]);

        let mut backoffer = RetryBackoffer::noop(Cancellation::default());
        let located = cache
            .locate_key_range(b"a".to_vec().into(), b"d".to_vec().into(), &mut backoffer)
            .await?;
        assert_eq!(region_ids(&located), vec![2, 3, 4]);

        assert_eq!(
            cache
                .list_region_ids_in_key_range(b"a".to_vec().into(), b"c".to_vec().into())
                .await?,
            vec![2, 3, 4]
        );

        let uncached = RegionCache::new(client);
        assert_eq!(uncached.load_region_by_id_from_pd(3).await?.id(), 3);
        assert!(uncached
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .is_empty());
        assert_eq!(uncached.get_region_by_id(3).await?.id(), 3);
        assert_eq!(uncached.region_cache.read().await.ver_id_to_region.len(), 1);
        Ok(())
    }

    #[test]
    fn source_all_stores_includes_tikv_and_tiflash_only() {
        let mut store = metapb::Store::default();
        assert!(is_valid_data_store(&store));

        store.state = metapb::StoreState::Tombstone.into();
        assert!(!is_valid_data_store(&store));

        store.state = metapb::StoreState::Up.into();
        assert!(is_valid_data_store(&store));

        store.labels.push(metapb::StoreLabel {
            key: "some_key".to_owned(),
            value: "some_value".to_owned(),
        });
        assert!(is_valid_data_store(&store));

        store.labels.push(metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        });
        assert!(is_valid_data_store(&store));

        store.labels[1].value = "tiflash_compute".to_string();
        assert!(!is_valid_data_store(&store));

        store.labels[1].value = "other".to_string();
        assert!(is_valid_data_store(&store));
    }

    #[tokio::test]
    async fn source_all_store_refresh_caches_tikv_and_tiflash_not_compute() {
        let client = Arc::new(MockRetryClient::default());
        client.stores.lock().await.extend([
            metapb::Store {
                id: 1,
                ..Default::default()
            },
            metapb::Store {
                id: 2,
                labels: vec![metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash".to_owned(),
                }],
                ..Default::default()
            },
            metapb::Store {
                id: 3,
                labels: vec![metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash_compute".to_owned(),
                }],
                ..Default::default()
            },
        ]);
        let cache = RegionCache::new(client);

        assert_eq!(
            cache
                .read_through_all_stores()
                .await
                .unwrap()
                .into_iter()
                .map(|store| store.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(cache.store_cache.read().unwrap().contains_key(&1));
        assert!(cache.store_cache.read().unwrap().contains_key(&2));
        assert!(!cache.store_cache.read().unwrap().contains_key(&3));
    }
}
