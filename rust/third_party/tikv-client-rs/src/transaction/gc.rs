// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `tikv/gc.go`'s lock resolution: every lock of a range at or
//! below a version is resolved region by region through a
//! [`RegionLockResolver`] -- the store's own ([`BaseRegionLockResolver`]),
//! the GC worker's, or a test's.
//!
//! client-go's `RegionLockResolver.GetStore` is absent: a trait object here
//! cannot name the store's PD client type, and callers that need the store
//! hold it themselves.

use async_trait::async_trait;
use log::{debug, info};

use super::lock::LockResolver;
use super::range_task::TaskStat;
use super::Client;
use crate::async_util::Cancellation;
use crate::backoff::Backoff;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::region::{RegionVerId, RegionWithLeader};
use crate::request::{EncodeKeyspace, KeyMode, Plan, PlanBuilder, TruncateKeyspace};
use crate::{Error, Key, Result};

/// client-go `locate.KeyLocation`: a region and its bounds, as the client
/// sees them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyLocation {
    /// `Region`.
    pub region: RegionVerId,
    /// `StartKey`.
    pub start_key: Vec<u8>,
    /// `EndKey`; empty is the keyspace's end.
    pub end_key: Vec<u8>,
}

impl KeyLocation {
    /// client-go `KeyLocation.Contains`.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start_key.as_slice() <= key
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }
}

/// client-go `RegionLockResolver`: how `ResolveLocksForRange` scans and
/// resolves one region's locks.
#[async_trait]
pub trait RegionLockResolver: Send + Sync {
    /// `Identifier`: the name of this resolver.
    fn identifier(&self) -> String;

    /// `ResolveLocksInOneRegion`: resolves `locks` (sorted by key) of the
    /// region at `location`, answering the location the locks were resolved
    /// in, or `None` when they no longer share one region and the caller
    /// must scan again.
    async fn resolve_locks_in_one_region(
        &self,
        backoff: &mut Backoff,
        locks: Vec<kvrpcpb::LockInfo>,
        location: Option<KeyLocation>,
    ) -> Result<Option<KeyLocation>>;

    /// `ScanLocksInOneRegion`: the locks at or below `max_version` from `key`
    /// to `end_key` (empty: no bound) within the first region the range
    /// touches, at most `scan_limit`, and that region's location.
    async fn scan_locks_in_one_region(
        &self,
        backoff: &mut Backoff,
        key: &[u8],
        end_key: &[u8],
        max_version: u64,
        scan_limit: u32,
    ) -> Result<(Vec<kvrpcpb::LockInfo>, Option<KeyLocation>)>;
}

/// client-go `BaseRegionLockResolver`: scans with `CmdScanLock` and resolves
/// with the store's lock resolver (`BatchResolveLocks`).
pub struct BaseRegionLockResolver<PdC: PdClient> {
    identifier: String,
    store: Client<PdC>,
}

impl<PdC: PdClient> BaseRegionLockResolver<PdC> {
    /// client-go `NewRegionLockResolver(identifier, store)`.
    pub fn new(identifier: impl Into<String>, store: Client<PdC>) -> Self {
        Self {
            identifier: identifier.into(),
            store,
        }
    }

    /// The store this resolver works on.
    pub fn store(&self) -> &Client<PdC> {
        &self.store
    }

    fn location(&self, region: &RegionWithLeader) -> KeyLocation {
        let keyspace = self.store.keyspace();
        KeyLocation {
            region: region.ver_id(),
            start_key: region.start_key().truncate_keyspace(keyspace).into(),
            end_key: region.end_key().truncate_keyspace(keyspace).into(),
        }
    }

    async fn locate_key(&self, key: &[u8]) -> Result<RegionWithLeader> {
        let encoded = Key::from(key.to_vec()).encode_keyspace(self.store.keyspace(), KeyMode::Txn);
        self.store.pd_client().region_for_key(&encoded).await
    }
}

/// One backoff step, or the error once the budget is spent.
async fn backoff_or(backoff: &mut Backoff, error: Error) -> Result<()> {
    match backoff.next_delay_duration() {
        Some(delay) => {
            tokio::time::sleep(delay).await;
            Ok(())
        }
        None => Err(error),
    }
}

#[async_trait]
impl<PdC: PdClient> RegionLockResolver for BaseRegionLockResolver<PdC> {
    fn identifier(&self) -> String {
        self.identifier.clone()
    }

    /// client-go `batchResolveLocksInOneRegion`.
    async fn resolve_locks_in_one_region(
        &self,
        backoff: &mut Backoff,
        locks: Vec<kvrpcpb::LockInfo>,
        location: Option<KeyLocation>,
    ) -> Result<Option<KeyLocation>> {
        let Some(mut resolved_location) = location else {
            return Ok(None);
        };
        let pd = self.store.pd_client();
        loop {
            // client-go sends `BatchResolveLock` to the location's region at
            // its epoch; a changed region answers a region error.
            let region = pd.region_for_id(resolved_location.region.id).await?;
            let ok = if region.ver_id() != resolved_location.region {
                false
            } else {
                let store = pd.clone().map_region_to_store(region).await?;
                let mut resolver = LockResolver::new(self.store.lock_resolver_context());
                match resolver
                    .cleanup_locks(
                        store,
                        locks.clone(),
                        pd.clone(),
                        self.store.keyspace(),
                        self.store.keyspace_name(),
                    )
                    .await
                {
                    Ok(()) => true,
                    Err(Error::RegionError(_)) => false,
                    Err(error) => return Err(error),
                }
            };
            if ok {
                return Ok(Some(resolved_location));
            }
            backoff_or(
                backoff,
                Error::StringError(format!("remain locks: {}", locks.len())),
            )
            .await?;
            let region = self.locate_key(&locks[0].key).await?;
            let location = self.location(&region);
            if location.contains(&locks[locks.len() - 1].key) {
                resolved_location = location;
            } else {
                return Ok(None);
            }
        }
    }

    /// client-go `scanLocksInOneRegionWithRange`.
    async fn scan_locks_in_one_region(
        &self,
        backoff: &mut Backoff,
        key: &[u8],
        end_key: &[u8],
        max_version: u64,
        scan_limit: u32,
    ) -> Result<(Vec<kvrpcpb::LockInfo>, Option<KeyLocation>)> {
        let keyspace = self.store.keyspace();
        loop {
            let region = self.locate_key(key).await?;
            let location = self.location(&region);
            let mut req_end_key = location.end_key.clone();
            if !end_key.is_empty() && (req_end_key.is_empty() || end_key < req_end_key.as_slice()) {
                req_end_key = end_key.to_vec();
            }
            let request = kvrpcpb::ScanLockRequest {
                max_version,
                limit: scan_limit,
                start_key: Key::from(key.to_vec())
                    .encode_keyspace(keyspace, KeyMode::Txn)
                    .into(),
                end_key: if req_end_key.is_empty() {
                    Vec::new()
                } else {
                    Key::from(req_end_key)
                        .encode_keyspace(keyspace, KeyMode::Txn)
                        .into()
                },
                ..kvrpcpb::ScanLockRequest::default()
            };
            let store = self.store.pd_client().map_region_to_store(region).await?;
            let response: kvrpcpb::ScanLockResponse =
                PlanBuilder::new(self.store.pd_client(), keyspace, request)
                    .keyspace_name_option(self.store.keyspace_name())
                    .single_region_with_store(store.clone())
                    .await?
                    .plan()
                    .execute()
                    .await?;
            if let Some(region_error) = response.region_error {
                // client-go's `SendReq` applies the region error to the
                // region cache (`onRegionError`) before returning it, so the
                // next `LocateKey` sees the split or the new leader. Its
                // retry decision is not this loop's: client-go backs off
                // `BoRegionMiss` and locates again, as below.
                let _ = crate::request::plan::handle_region_error(
                    self.store.pd_client(),
                    region_error.clone(),
                    store,
                )
                .await;
                backoff_or(backoff, Error::RegionError(Box::new(region_error))).await?;
                continue;
            }
            if response.error.is_some() {
                return Err(Error::StringError(format!(
                    "unexpected scanlock error: {response:?}"
                )));
            }
            let mut locks = Vec::with_capacity(response.locks.len());
            for lock in response.locks {
                if lock.shared_lock_infos.is_empty() {
                    locks.push(lock);
                } else {
                    // A shared lock expands into its members; the wrapper goes.
                    locks.extend(lock.shared_lock_infos);
                }
            }
            return Ok((locks, Some(location)));
        }
    }
}

/// client-go `ResolveLocksForRange`: every lock at or below `max_version`
/// in `[start_key, end_key)` (empty end: no bound) is resolved through
/// `resolver`, region by region; a region with `scan_limit` locks or more is
/// scanned again from its last lock. Each scan and resolve gets a fresh
/// backoff from `create_backoff`. The stat counts the regions completed.
pub async fn resolve_locks_for_range(
    cancellation: Cancellation,
    resolver: &dyn RegionLockResolver,
    max_version: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    create_backoff: &(dyn Fn() -> Backoff + Send + Sync),
    scan_limit: u32,
) -> (TaskStat, Result<()>) {
    // A scan lock request must return every lock, even several of one
    // transaction, because GC must make sure all locks are cleaned.
    let mut stat = TaskStat::default();
    let mut key = start_key;
    let mut backoff = create_backoff();
    loop {
        if cancellation.is_cancelled() {
            return (
                stat,
                Err(Error::StringError("[gc worker] gc job canceled".to_owned())),
            );
        }
        let (locks, location) = match resolver
            .scan_locks_in_one_region(&mut backoff, &key, &end_key, max_version, scan_limit)
            .await
        {
            Ok(scanned) => scanned,
            Err(error) => return (stat, Err(error)),
        };
        let lock_count = locks.len();
        let last_lock_key = locks.last().map(|lock| lock.key.clone());
        let resolved_location = match resolver
            .resolve_locks_in_one_region(&mut backoff, locks, location.clone())
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return (stat, Err(error)),
        };
        // The locks are not in one region anymore; scan again.
        let Some(resolved_location) = resolved_location else {
            continue;
        };
        if lock_count < scan_limit as usize {
            stat.completed_regions += 1;
            key = location
                .map(|location| location.end_key)
                .unwrap_or_default();
            debug!(
                "resolve one region finshed, identifier: {}, regionID: {}, resolvedLocksNum: {}",
                resolver.identifier(),
                resolved_location.region.id,
                lock_count
            );
        } else {
            info!(
                "region has more than limit locks, identifier: {}, regionID: {}, resolvedLocksNum: {}, scan lock limit: {}",
                resolver.identifier(),
                resolved_location.region.id,
                lock_count,
                scan_limit
            );
            key = last_lock_key.unwrap_or_default();
        }
        if key.is_empty() || (!end_key.is_empty() && key >= end_key) {
            break;
        }
        backoff = create_backoff();
    }
    (stat, Ok(()))
}

/// client-go `NewGcResolveLockMaxBackoffer`.
pub fn new_gc_resolve_lock_max_backoff() -> Backoff {
    Backoff::equal_jitter_backoff(100, 10_000, 50)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::mock::mocktikv::{bootstrap_with_multi_regions, new_tikv_and_pd_client};
    use crate::store::KvClient as _;
    use crate::{Config, TimestampExt as _};

    /// A store over client-go's mock cluster split at `split_keys`, its RPC
    /// client, and a prewrite leaving a lock of `start_ts` on each key.
    async fn store_with_locks(
        split_keys: &[&[u8]],
        locks: &[&[u8]],
    ) -> (Client<crate::mock::mocktikv::MockPdClient>, u64) {
        let (rpc, cluster, pd) = new_tikv_and_pd_client("", None).unwrap();
        bootstrap_with_multi_regions(
            &cluster,
            &split_keys
                .iter()
                .map(|key| key.to_vec())
                .collect::<Vec<_>>(),
        );
        let pd = Arc::new(pd);
        let store = Client::new_with_pd_client(pd.clone(), Config::default())
            .await
            .unwrap();
        let start_ts = pd.clone().get_timestamp().await.unwrap().version();
        for key in locks {
            let region = pd.region_for_key(&key.to_vec().into()).await.unwrap();
            let prewrite = kvrpcpb::PrewriteRequest {
                context: Some(kvrpcpb::Context {
                    region_id: region.region.id,
                    region_epoch: region.region.region_epoch.clone(),
                    peer: region.leader.clone(),
                    ..Default::default()
                }),
                mutations: vec![kvrpcpb::Mutation {
                    op: kvrpcpb::Op::Put as i32,
                    key: key.to_vec(),
                    value: b"v".to_vec(),
                    ..Default::default()
                }],
                primary_lock: key.to_vec(),
                start_version: start_ts,
                lock_ttl: 60_000,
                ..Default::default()
            };
            rpc.dispatch(&prewrite).await.unwrap();
        }
        (store, start_ts)
    }

    async fn remaining_locks(
        resolver: &BaseRegionLockResolver<crate::mock::mocktikv::MockPdClient>,
    ) -> usize {
        let mut backoff = new_gc_resolve_lock_max_backoff();
        let mut count = 0;
        let mut key = Vec::new();
        loop {
            let (locks, location) = resolver
                .scan_locks_in_one_region(&mut backoff, &key, &[], u64::MAX, 100)
                .await
                .unwrap();
            count += locks.len();
            key = location.unwrap().end_key;
            if key.is_empty() {
                return count;
            }
        }
    }

    /// client-go `ResolveLocksForRange` over the base resolver: every lock
    /// below the version is resolved, one region at a time.
    #[tokio::test]
    async fn resolve_locks_for_range_resolves_every_region() {
        let (store, start_ts) = store_with_locks(&[b"m"], &[b"a", b"x"]).await;
        let resolver = BaseRegionLockResolver::new("test", store);
        assert_eq!(remaining_locks(&resolver).await, 2);
        let (stat, result) = resolve_locks_for_range(
            Cancellation::default(),
            &resolver,
            start_ts,
            Vec::new(),
            Vec::new(),
            &new_gc_resolve_lock_max_backoff,
            100,
        )
        .await;
        result.unwrap();
        assert_eq!(stat.completed_regions, 2);
        assert_eq!(remaining_locks(&resolver).await, 0);
    }

    /// A region holding `scan_limit` locks or more is scanned again from its
    /// last lock rather than counted complete.
    #[tokio::test]
    async fn a_region_at_the_scan_limit_is_scanned_again_from_its_last_lock() {
        let (store, start_ts) = store_with_locks(&[], &[b"a", b"b", b"c"]).await;
        let resolver = BaseRegionLockResolver::new("test", store);
        let (stat, result) = resolve_locks_for_range(
            Cancellation::default(),
            &resolver,
            start_ts,
            Vec::new(),
            Vec::new(),
            &new_gc_resolve_lock_max_backoff,
            2,
        )
        .await;
        result.unwrap();
        assert_eq!(stat.completed_regions, 1);
        assert_eq!(remaining_locks(&resolver).await, 0);
    }

    /// Locks above the version stay.
    #[tokio::test]
    async fn locks_above_the_version_stay() {
        let (store, start_ts) = store_with_locks(&[], &[b"a"]).await;
        let resolver = BaseRegionLockResolver::new("test", store);
        let (_, result) = resolve_locks_for_range(
            Cancellation::default(),
            &resolver,
            start_ts - 1,
            Vec::new(),
            Vec::new(),
            &new_gc_resolve_lock_max_backoff,
            100,
        )
        .await;
        result.unwrap();
        assert_eq!(remaining_locks(&resolver).await, 1);
    }

    /// `GCScanLockLimit` is half the resolved cache, as in client-go.
    #[test]
    fn gc_scan_lock_limit_is_half_the_resolved_cache() {
        assert_eq!(crate::tikv::GC_SCAN_LOCK_LIMIT, 1024);
    }
}
