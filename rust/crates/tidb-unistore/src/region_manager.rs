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

//! Go unistore's `MockRegionManager` (`tikv/mock_region.go`) with the
//! `regionCtx` it holds (`tikv/region.go`).
//!
//! Regions are Go's `metapb.Region`s with memcomparable-encoded bounds, as a
//! TiKV store keeps them, so a region error carries the region exactly as
//! TiKV's would and a client decodes it the same way. Lookups by key take
//! encoded keys, the form client-go's codec PD client sends;
//!
//! # Narrowings, by name
//!
//! * `saveRegions` / `loadFromLocal` / `IsBootstrapped`'s badger reads: the
//!   store is in memory and starts empty on every run, so there is nothing to
//!   persist or reload; bootstrap state is a flag.
//! * `regionCtx.AcquireLatches` / `ReleaseLatches`: the store beneath is
//!   single-writer (`&mut MvccStore` behind one lock), which excludes the
//!   interleavings the latches guard.
//! * `SplitTable` / `SplitIndex` / `SplitKeys` (`calculateSplitKeys` walks the
//!   badger bundle the Go manager holds) and the MPP task sets: this manager
//!   does not hold the store; they arrive with the store-holding cluster.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use tidb_proto::{errorpb, kvrpcpb, metapb};

use crate::mvcc_store::INTERNAL_KEY_PREFIX;

/// The store every region's one peer lives on after
/// [`crate::cluster::bootstrap_with_single_store`].
pub const STORE_ID: u64 = 1;
/// The bootstrap region.
pub const BOOTSTRAP_REGION_ID: u64 = 2;
/// The bootstrap region's peer.
pub const BOOTSTRAP_PEER_ID: u64 = 3;

/// Go `regionCtx`: a region's meta with its decoded raw bounds.
#[derive(Clone, Debug, PartialEq)]
pub struct RegionCtx {
    meta: metapb::Region,
    raw_start_key: Vec<u8>,
    raw_end_key: Vec<u8>,
}

impl RegionCtx {
    /// Go `newRegionCtx`: the last region's raw end is `InternalKeyPrefix`,
    /// so no read reaches the store's internal meta keys.
    fn new(meta: metapb::Region) -> Self {
        let raw_start_key = decode_region_key(&meta.start_key, "invalid region start key");
        let mut raw_end_key = decode_region_key(&meta.end_key, "invalid region end key");
        if raw_end_key.is_empty() {
            raw_end_key = INTERNAL_KEY_PREFIX.to_vec();
        }
        Self {
            meta,
            raw_start_key,
            raw_end_key,
        }
    }

    /// Go `regionCtx.Meta`.
    #[must_use]
    pub fn meta(&self) -> &metapb::Region {
        &self.meta
    }

    /// Go `regionCtx.RawStart`.
    #[must_use]
    pub fn raw_start(&self) -> &[u8] {
        &self.raw_start_key
    }

    /// Go `regionCtx.RawEnd`.
    #[must_use]
    pub fn raw_end(&self) -> &[u8] {
        &self.raw_end_key
    }

    /// Go `regionCtx.getRegionEpoch`.
    fn region_epoch(&self) -> metapb::RegionEpoch {
        self.meta.region_epoch.clone().unwrap_or_default()
    }

    /// Go `regionCtx.incConfVer`.
    fn inc_conf_ver(&mut self) {
        let epoch = self.region_epoch();
        self.meta.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: epoch.conf_ver + 1,
            version: epoch.version,
        });
    }

    /// Go `regionCtx.addPeer`.
    fn add_peer(&mut self, peer_id: u64, store_id: u64) {
        self.meta.peers.push(new_peer_meta(peer_id, store_id));
        self.inc_conf_ver();
    }
}

/// Go `regionCtx.decodeRawStartKey` / `decodeRawEndKey`: an empty bound
/// stays empty; anything else must be memcomparable bytes.
fn decode_region_key(key: &[u8], message: &str) -> Vec<u8> {
    if key.is_empty() {
        return Vec::new();
    }
    tidb_codec::decode_bytes(key).expect(message).1
}

/// Go `codec.EncodeBytes(nil, key)`.
#[must_use]
pub fn encode_region_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    tidb_codec::encode_bytes(&mut encoded, key);
    encoded
}

/// Go `newPeerMeta`.
pub(crate) fn new_peer_meta(peer_id: u64, store_id: u64) -> metapb::Peer {
    metapb::Peer {
        id: peer_id,
        store_id,
        ..metapb::Peer::default()
    }
}

/// Go `btreeItem`'s order: by end key, the empty end (`inf`) last.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EndKey {
    Key(Vec<u8>),
    Inf,
}

impl EndKey {
    /// Go `newBtreeItem`.
    fn of(region: &metapb::Region) -> Self {
        if region.end_key.is_empty() {
            Self::Inf
        } else {
            Self::Key(region.end_key.clone())
        }
    }
}

/// Go `regionManager.isEpochStale`.
fn is_epoch_stale(lhs: &metapb::RegionEpoch, rhs: Option<&metapb::RegionEpoch>) -> bool {
    let rhs = rhs.cloned().unwrap_or_default();
    lhs.conf_ver != rhs.conf_ver || lhs.version != rhs.version
}

fn store_not_match() -> errorpb::Error {
    errorpb::Error {
        message: "store not match".to_owned(),
        store_not_match: Some(errorpb::StoreNotMatch::default()),
        ..errorpb::Error::default()
    }
}

fn region_not_found(region_id: u64) -> errorpb::Error {
    errorpb::Error {
        message: "region not found".to_owned(),
        region_not_found: Some(errorpb::RegionNotFound { region_id }),
        ..errorpb::Error::default()
    }
}

/// Go's "stale epoch" error: the region as the store holds it now.
fn stale_epoch(region: &RegionCtx) -> errorpb::Error {
    errorpb::Error {
        message: "stale epoch".to_owned(),
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![metapb::Region {
                id: region.meta.id,
                start_key: region.meta.start_key.clone(),
                end_key: region.meta.end_key.clone(),
                region_epoch: Some(region.region_epoch()),
                peers: region.meta.peers.clone(),
                ..metapb::Region::default()
            }],
        }),
        ..errorpb::Error::default()
    }
}

#[derive(Debug, Default)]
struct State {
    /// Go `regionManager.storeMeta`: the store requests are checked against.
    store_meta: metapb::Store,
    /// Go `regionManager.regions`.
    regions: HashMap<u64, RegionCtx>,
    /// Go `sortedRegions`: end key -> region id.
    sorted_regions: BTreeMap<EndKey, u64>,
    /// Go `stores`.
    stores: BTreeMap<u64, metapb::Store>,
    /// Go `id`: the last id `AllocID` handed out.
    id: u64,
    /// Go `IsBootstrapped`'s `InternalStoreMetaKey`.
    bootstrapped: bool,
}

impl State {
    /// Go `AllocID`.
    fn alloc_id(&mut self) -> u64 {
        self.id += 1;
        self.id
    }

    /// Go `AllocIDs`.
    fn alloc_ids(&mut self, n: usize) -> Vec<u64> {
        let base = self.id + 1;
        self.id += n as u64;
        (base..=self.id).collect()
    }

    /// Go `GetStoreAddrByStoreID`.
    fn store_addr_by_store_id(&self, store_id: u64) -> Result<String, String> {
        self.stores
            .get(&store_id)
            .map(|store| store.address.clone())
            .ok_or_else(|| "Store not match".to_owned())
    }

    /// Go `GetRegionFromCtx`'s store, region and epoch checks, which
    /// `SplitRegion` repeats under the write lock.
    fn checked_region(&self, ctx: &kvrpcpb::Context) -> Result<&RegionCtx, errorpb::Error> {
        if let Some(peer) = ctx.peer.as_ref() {
            if self.store_addr_by_store_id(peer.store_id).is_err() {
                return Err(store_not_match());
            }
        }
        let Some(region) = self.regions.get(&ctx.region_id) else {
            return Err(region_not_found(ctx.region_id));
        };
        if is_epoch_stale(&region.region_epoch(), ctx.region_epoch.as_ref()) {
            return Err(stale_epoch(region));
        }
        Ok(region)
    }

    fn insert(&mut self, region: RegionCtx) {
        self.sorted_regions
            .insert(EndKey::of(&region.meta), region.meta.id);
        self.regions.insert(region.meta.id, region);
    }

    /// Go `sortedRegions.AscendGreaterOrEqual(newBtreeSearchItem(key), ..)`:
    /// regions in end-key order from the first whose end is `>= key`.
    fn ascend_from<'a>(&'a self, key: &[u8]) -> impl Iterator<Item = &'a RegionCtx> + 'a {
        self.sorted_regions
            .range(EndKey::Key(key.to_vec())..)
            .map(|(_, id)| &self.regions[id])
    }

    /// Go `splitKeys`: `keys` are encoded, sorted split keys; every region
    /// from the one holding `keys[0]` takes the keys inside it.
    fn split_keys(&mut self, mut keys: Vec<Vec<u8>>) -> Vec<RegionCtx> {
        let mut new_regions: Vec<RegionCtx> = Vec::with_capacity(keys.len());
        let Some(first) = keys.first().cloned() else {
            return new_regions;
        };
        let regions: Vec<metapb::Region> = self
            .ascend_from(&first)
            .map(|region| region.meta.clone())
            .collect();
        for region in regions {
            if keys.is_empty() {
                break;
            }
            let inside = keys
                .iter()
                .position(|key| !region.end_key.is_empty() && *key >= region.end_key)
                .unwrap_or(keys.len());
            let mut splits: Vec<Vec<u8>> = keys.drain(..inside).collect();
            if splits.is_empty() {
                continue;
            }
            let start_key = region.start_key.clone();
            if start_key == splits[0] {
                splits.remove(0);
            }
            if splits.is_empty() {
                continue;
            }
            let epoch = region.region_epoch.clone().unwrap_or_default();
            new_regions.push(RegionCtx::new(metapb::Region {
                id: region.id,
                start_key,
                end_key: splits[0].clone(),
                region_epoch: Some(metapb::RegionEpoch {
                    conf_ver: epoch.conf_ver,
                    version: epoch.version + 1,
                }),
                peers: region.peers.clone(),
                ..metapb::Region::default()
            }));
            for pair in splits.windows(2) {
                let meta = self.new_region_meta(pair[0].clone(), pair[1].clone());
                new_regions.push(RegionCtx::new(meta));
            }
            let last = splits.last().expect("non-empty splits").clone();
            if last != region.end_key {
                let meta = self.new_region_meta(last, region.end_key.clone());
                new_regions.push(RegionCtx::new(meta));
            }
        }
        for region in &new_regions {
            self.insert(region.clone());
        }
        new_regions
    }

    /// A region `splitKeys` creates: a fresh id, epoch (1, 1), one fresh
    /// peer on the checked store.
    fn new_region_meta(&mut self, start_key: Vec<u8>, end_key: Vec<u8>) -> metapb::Region {
        let id = self.alloc_id();
        let peer_id = self.alloc_id();
        metapb::Region {
            id,
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 1,
                version: 1,
            }),
            peers: vec![new_peer_meta(peer_id, self.store_meta.id)],
            start_key,
            end_key,
            ..metapb::Region::default()
        }
    }
}

/// Go `regionContainsKey`.
fn region_contains_key(region: &metapb::Region, key: &[u8]) -> bool {
    region.start_key.as_slice() <= key
        && (key < region.end_key.as_slice() || region.end_key.is_empty())
}

/// Go `regionContainsKeyByEnd`.
fn region_contains_key_by_end(region: &metapb::Region, key: &[u8]) -> bool {
    region.start_key.as_slice() < key
        && (key <= region.end_key.as_slice() || region.end_key.is_empty())
}

/// The embedded store's region manager, Go `MockRegionManager`.
#[derive(Debug)]
pub struct InProcessRegionManager {
    state: Mutex<State>,
}

impl Default for InProcessRegionManager {
    /// [`Self::new`]: the bootstrapped single-store cluster.
    fn default() -> Self {
        Self::new()
    }
}

impl InProcessRegionManager {
    /// Go `NewMockRegionManager` followed by `BootstrapWithSingleStore`, the
    /// cluster TiDB's unistore starts with: store 1 (`store1`), region 2 over
    /// the whole keyspace, peer 3.
    #[must_use]
    pub fn new() -> Self {
        let manager = Self::new_mock_region_manager();
        let (store_id, peer_id, region_id) = crate::cluster::bootstrap_with_single_store(&manager);
        debug_assert_eq!(
            (store_id, region_id, peer_id),
            (STORE_ID, BOOTSTRAP_REGION_ID, BOOTSTRAP_PEER_ID)
        );
        manager
    }

    /// Go `NewMockRegionManager`: no store, no region, ids from zero.
    #[must_use]
    pub fn new_mock_region_manager() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `AllocID`.
    pub fn alloc_id(&self) -> u64 {
        self.state().alloc_id()
    }

    /// Go `AllocIDs`.
    pub fn alloc_ids(&self, n: usize) -> Vec<u64> {
        self.state().alloc_ids(n)
    }

    /// Go `GetStoreIDByAddr`.
    pub fn get_store_id_by_addr(&self, addr: &str) -> Result<u64, String> {
        self.state()
            .stores
            .values()
            .find(|store| store.address == addr)
            .map(|store| store.id)
            .ok_or_else(|| "Store not match".to_owned())
    }

    /// Go `GetStoreAddrByStoreID`.
    pub fn get_store_addr_by_store_id(&self, store_id: u64) -> Result<String, String> {
        self.state().store_addr_by_store_id(store_id)
    }

    /// Go `GetStoreInfoFromCtx`: the store a request addresses -- its peer's
    /// store, or the checked store when the request names no peer.
    pub fn get_store_info_from_ctx(
        &self,
        ctx: &kvrpcpb::Context,
    ) -> Result<(String, u64), errorpb::Error> {
        let state = self.state();
        if let Some(peer) = ctx.peer.as_ref() {
            let addr = state
                .store_addr_by_store_id(peer.store_id)
                .map_err(|_| store_not_match())?;
            return Ok((addr, peer.store_id));
        }
        Ok((state.store_meta.address.clone(), state.store_meta.id))
    }

    /// Go `GetRegionFromCtx`: the region a request addresses, or the region
    /// error TiKV answers -- store not match, region not found, stale epoch.
    pub fn get_region_from_ctx(&self, ctx: &kvrpcpb::Context) -> Result<RegionCtx, errorpb::Error> {
        self.state().checked_region(ctx).cloned()
    }

    /// Go `GetRegion(id)`.
    #[must_use]
    pub fn get_region(&self, id: u64) -> Option<metapb::Region> {
        self.state()
            .regions
            .get(&id)
            .map(|region| region.meta.clone())
    }

    /// Go `GetRegionByKey`: the region holding the encoded `key` and its
    /// leader (the first peer).
    #[must_use]
    pub fn get_region_by_key(&self, key: &[u8]) -> Option<(metapb::Region, metapb::Peer)> {
        let state = self.state();
        let region = state
            .ascend_from(key)
            .map(RegionCtx::meta)
            .find(|region| region.end_key != key)?;
        if !region_contains_key(region, key) {
            return None;
        }
        Some((region.clone(), region.peers[0].clone()))
    }

    /// Go `GetRegionByEndKey`: the region whose `(start, end]` holds the
    /// encoded `key`.
    #[must_use]
    pub fn get_region_by_end_key(&self, key: &[u8]) -> Option<(metapb::Region, metapb::Peer)> {
        let state = self.state();
        let region = state.ascend_from(key).next()?.meta();
        if !region_contains_key_by_end(region, key) {
            return None;
        }
        Some((region.clone(), region.peers[0].clone()))
    }

    /// Go `Bootstrap`: the first store becomes the checked store; the region
    /// starts at epoch (1, 1). A second call is a no-op.
    pub fn bootstrap(&self, stores: Vec<metapb::Store>, mut region: metapb::Region) {
        let mut state = self.state();
        if state.bootstrapped {
            return;
        }
        state.store_meta = stores[0].clone();
        for store in stores {
            state.stores.insert(store.id, store);
        }
        region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 1,
            version: 1,
        });
        state.insert(RegionCtx::new(region));
        state.bootstrapped = true;
    }

    /// Go `IsBootstrapped`.
    #[must_use]
    pub fn is_bootstrapped(&self) -> bool {
        self.state().bootstrapped
    }

    /// Go `Split`: splits region `region_id` at the raw `key` into a new
    /// region `new_region_id` whose peers take `peer_ids`.
    pub fn split(
        &self,
        region_id: u64,
        new_region_id: u64,
        key: &[u8],
        peer_ids: &[u64],
        _leader_peer_id: u64,
    ) {
        if let Err(error) =
            self.split_inner(region_id, new_region_id, encode_region_key(key), peer_ids)
        {
            panic!("{error}");
        }
    }

    /// Go `SplitRaw`: [`Self::split`] at a key taken as already encoded.
    pub fn split_raw(
        &self,
        region_id: u64,
        new_region_id: u64,
        raw_key: &[u8],
        peer_ids: &[u64],
        _leader_peer_id: u64,
    ) -> metapb::Region {
        match self.split_inner(region_id, new_region_id, raw_key.to_vec(), peer_ids) {
            Ok(region) => region,
            Err(error) => panic!("{error}"),
        }
    }

    /// Go `SplitArbitrary`: splits at the given raw keys.
    pub fn split_arbitrary(&self, keys: &[Vec<u8>]) {
        let split_keys = keys.iter().map(|key| encode_region_key(key)).collect();
        self.state().split_keys(split_keys);
    }

    /// Go `SplitRegion` (the `CmdSplitRegion` handler): the raw split keys,
    /// encoded and sorted, cut every region from the one holding the first.
    pub fn split_region(&self, req: &kvrpcpb::SplitRegionRequest) -> kvrpcpb::SplitRegionResponse {
        let mut split_keys: Vec<Vec<u8>> = req
            .split_keys
            .iter()
            .map(|key| encode_region_key(key))
            .collect();
        split_keys.sort();
        let ctx = req.context.clone().unwrap_or_default();
        let mut state = self.state();
        if let Err(region_error) = state.checked_region(&ctx) {
            return kvrpcpb::SplitRegionResponse {
                region_error: Some(region_error),
                ..kvrpcpb::SplitRegionResponse::default()
            };
        }
        let new_regions = state.split_keys(split_keys);
        kvrpcpb::SplitRegionResponse {
            regions: new_regions.into_iter().map(|region| region.meta).collect(),
            ..kvrpcpb::SplitRegionResponse::default()
        }
    }

    /// Go `split`.
    fn split_inner(
        &self,
        region_id: u64,
        new_region_id: u64,
        key: Vec<u8>,
        peer_ids: &[u64],
    ) -> Result<metapb::Region, String> {
        let mut state = self.state();
        let old = state.regions[&region_id].meta.clone();
        if old.start_key >= key || (!old.end_key.is_empty() && key >= old.end_key) {
            return Err(format!(
                "split key is out of region range, region start key: {}, end key: {}, split key: {}",
                hex(&old.start_key),
                hex(&old.end_key),
                hex(&key)
            ));
        }
        let epoch = old.region_epoch.clone().unwrap_or_default();
        let left = metapb::Region {
            id: old.id,
            start_key: old.start_key.clone(),
            end_key: key.clone(),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: epoch.conf_ver,
                version: epoch.version + 1,
            }),
            peers: old.peers.clone(),
            ..metapb::Region::default()
        };
        let right = metapb::Region {
            id: new_region_id,
            start_key: key,
            end_key: old.end_key.clone(),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 1,
                version: 1,
            }),
            peers: left
                .peers
                .iter()
                .zip(peer_ids)
                .map(|(peer, id)| new_peer_meta(*id, peer.store_id))
                .collect(),
            ..metapb::Region::default()
        };
        state.insert(RegionCtx::new(left));
        state.insert(RegionCtx::new(right.clone()));
        Ok(right)
    }

    /// Go `ScanRegions`: from the region holding the encoded `start_key`,
    /// every region starting before `end_key` (empty: no bound), at most
    /// `limit` when `limit > 0`, each with its leader (the first peer).
    #[must_use]
    pub fn scan_regions(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Vec<(metapb::Region, metapb::Peer)> {
        let state = self.state();
        let mut regions = Vec::new();
        for region in state.ascend_from(start_key).map(RegionCtx::meta) {
            if !end_key.is_empty() && region.start_key.as_slice() >= end_key {
                break;
            }
            if regions.is_empty() && region.end_key == start_key {
                continue;
            }
            regions.push((region.clone(), region.peers[0].clone()));
            if limit > 0 && regions.len() >= limit {
                break;
            }
        }
        regions
    }

    /// Go `GetAllStores`.
    #[must_use]
    pub fn get_all_stores(&self) -> Vec<metapb::Store> {
        self.state().stores.values().cloned().collect()
    }

    /// Go `AddStore`.
    pub fn add_store(&self, store_id: u64, addr: &str, labels: Vec<metapb::StoreLabel>) {
        self.state().stores.insert(
            store_id,
            metapb::Store {
                id: store_id,
                address: addr.to_owned(),
                labels,
                ..metapb::Store::default()
            },
        );
    }

    /// Go `RemoveStore`.
    pub fn remove_store(&self, store_id: u64) {
        self.state().stores.remove(&store_id);
    }

    /// Go `AddPeer`.
    pub fn add_peer(&self, region_id: u64, store_id: u64, peer_id: u64) {
        let mut state = self.state();
        let region = state
            .regions
            .get_mut(&region_id)
            .expect("the region exists");
        region.add_peer(peer_id, store_id);
    }
}

/// Go `hex.EncodeToString`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{bootstrap_with_multi_regions, bootstrap_with_multi_stores};

    // WRITTEN: mock_region.go's coverage in Go rides the store suites.

    fn ctx(region_id: u64, conf_ver: u64, version: u64) -> kvrpcpb::Context {
        kvrpcpb::Context {
            region_id,
            region_epoch: Some(metapb::RegionEpoch { conf_ver, version }),
            peer: Some(new_peer_meta(BOOTSTRAP_PEER_ID, STORE_ID)),
            ..kvrpcpb::Context::default()
        }
    }

    fn split_request(ctx: kvrpcpb::Context, keys: &[&[u8]]) -> kvrpcpb::SplitRegionRequest {
        kvrpcpb::SplitRegionRequest {
            context: Some(ctx),
            split_keys: keys.iter().map(|key| key.to_vec()).collect(),
            ..kvrpcpb::SplitRegionRequest::default()
        }
    }

    fn id_version_peer(region: &metapb::Region) -> (u64, u64, u64) {
        (
            region.id,
            region.region_epoch.as_ref().unwrap().version,
            region.peers[0].id,
        )
    }

    /// Go `BootstrapWithSingleStore` and `SplitRegion`: store 1 at `store1`,
    /// region 2 and peer 3; the left part keeps its id at the next version,
    /// new regions take fresh id pairs at epoch (1, 1), and bounds are
    /// memcomparable-encoded as a TiKV store keeps them.
    #[test]
    fn bootstraps_and_splits_like_go_mock_region_manager() {
        let manager = InProcessRegionManager::new();
        assert_eq!(manager.get_all_stores()[0].address, "store1");
        let (whole, leader) = manager.get_region_by_key(&encode_region_key(b"x")).unwrap();
        assert_eq!(id_version_peer(&whole), (2, 1, 3));
        assert_eq!(leader.id, 3);

        let resp = manager.split_region(&split_request(ctx(2, 1, 1), &[b"t5", b"t1"]));
        assert!(resp.region_error.is_none());
        let ids: Vec<_> = resp.regions.iter().map(id_version_peer).collect();
        assert_eq!(ids, vec![(2, 2, 3), (4, 1, 5), (6, 1, 7)]);
        assert_eq!(resp.regions[0].end_key, encode_region_key(b"t1"));
        assert_eq!(resp.regions[1].start_key, encode_region_key(b"t1"));

        let lookup = |key: &[u8]| {
            manager
                .get_region_by_key(&encode_region_key(key))
                .unwrap()
                .0
                .id
        };
        assert_eq!(lookup(b"t3"), 4);
        assert_eq!(lookup(b"t1"), 4, "a region's start key is inside it");
        assert_eq!(lookup(b"a"), 2);
        let by_end = manager
            .get_region_by_end_key(&encode_region_key(b"t5"))
            .unwrap()
            .0;
        assert_eq!(by_end.id, 4, "an end key belongs to the region it ends");
        let scanned: Vec<_> = manager
            .scan_regions(&encode_region_key(b"t2"), &encode_region_key(b"t6"), 0)
            .iter()
            .map(|(region, _)| region.id)
            .collect();
        assert_eq!(scanned, vec![4, 6]);
        assert_eq!(manager.scan_regions(b"", b"", 2).len(), 2);
    }

    /// Go `GetRegionFromCtx`: an unknown store, an unknown region and a stale
    /// epoch each answer TiKV's region error; the stale epoch carries the
    /// region as the store holds it now.
    #[test]
    fn get_region_from_ctx_answers_tikvs_region_errors() {
        let manager = InProcessRegionManager::new();
        let region = manager.get_region_from_ctx(&ctx(2, 1, 1)).unwrap();
        assert!(region.raw_start().is_empty());
        assert_eq!(region.raw_end(), INTERNAL_KEY_PREFIX);

        let mut other_store = ctx(2, 1, 1);
        other_store.peer = Some(new_peer_meta(3, 9));
        let error = manager.get_region_from_ctx(&other_store).unwrap_err();
        assert!(error.store_not_match.is_some());
        assert_eq!(error.message, "store not match");

        let error = manager.get_region_from_ctx(&ctx(99, 1, 1)).unwrap_err();
        assert_eq!(error.region_not_found.unwrap().region_id, 99);

        manager.split_region(&split_request(ctx(2, 1, 1), &[b"m"]));
        let error = manager.get_region_from_ctx(&ctx(2, 1, 1)).unwrap_err();
        assert_eq!(error.message, "stale epoch");
        let current = &error.epoch_not_match.unwrap().current_regions[0];
        assert_eq!(current.id, 2);
        assert_eq!(current.end_key, encode_region_key(b"m"));
        assert_eq!(current.region_epoch.as_ref().unwrap().version, 2);

        let left = manager.get_region_from_ctx(&ctx(2, 1, 2)).unwrap();
        assert_eq!(left.raw_end(), b"m");
        let (right, _) = manager.get_region_by_key(&encode_region_key(b"m")).unwrap();
        let right = manager.get_region_from_ctx(&ctx(right.id, 1, 1)).unwrap();
        assert_eq!(right.raw_start(), b"m");
        assert_eq!(right.raw_end(), INTERNAL_KEY_PREFIX);

        let response = manager.split_region(&split_request(ctx(2, 1, 1), &[b"c"]));
        assert!(response.region_error.unwrap().epoch_not_match.is_some());
    }

    /// Go `GetStoreInfoFromCtx`: the peer's store, else the checked store.
    #[test]
    fn get_store_info_from_ctx_resolves_the_peers_store() {
        let manager = InProcessRegionManager::new();
        assert_eq!(
            manager.get_store_info_from_ctx(&ctx(2, 1, 1)).unwrap(),
            ("store1".to_owned(), 1)
        );
        assert_eq!(
            manager
                .get_store_info_from_ctx(&kvrpcpb::Context::default())
                .unwrap(),
            ("store1".to_owned(), 1)
        );
        let mut other_store = ctx(2, 1, 1);
        other_store.peer = Some(new_peer_meta(3, 9));
        assert!(manager
            .get_store_info_from_ctx(&other_store)
            .unwrap_err()
            .store_not_match
            .is_some());
        assert_eq!(manager.get_store_id_by_addr("store1"), Ok(1));
        assert!(manager.get_store_id_by_addr("store9").is_err());
    }

    /// Go `splitKeys` walks every region from the one holding the first key,
    /// so one request cuts each region its keys fall in.
    #[test]
    fn one_split_request_cuts_every_region_its_keys_fall_in() {
        let manager = InProcessRegionManager::new();
        manager.split_region(&split_request(ctx(2, 1, 1), &[b"m"]));
        let resp = manager.split_region(&split_request(ctx(2, 1, 2), &[b"c", b"p"]));
        let bounds: Vec<_> = resp
            .regions
            .iter()
            .map(|region| (region.id, region.start_key.clone(), region.end_key.clone()))
            .collect();
        assert_eq!(
            bounds,
            vec![
                (2, Vec::new(), encode_region_key(b"c")),
                (6, encode_region_key(b"c"), encode_region_key(b"m")),
                (4, encode_region_key(b"m"), encode_region_key(b"p")),
                (8, encode_region_key(b"p"), Vec::new()),
            ]
        );
    }

    /// Go `BootstrapWithMultiRegions` and `BootstrapWithMultiStores`.
    #[test]
    fn bootstraps_multi_region_and_multi_store_clusters() {
        let manager = InProcessRegionManager::new_mock_region_manager();
        let (store_id, region_ids, peer_ids) =
            bootstrap_with_multi_regions(&manager, &[b"b".to_vec(), b"d".to_vec()]);
        assert_eq!(
            (store_id, region_ids, peer_ids),
            (1, vec![2, 4, 5], vec![3, 6, 7])
        );
        let ranges: Vec<_> = manager
            .scan_regions(b"", b"", 0)
            .iter()
            .map(|(region, leader)| (region.id, leader.id))
            .collect();
        assert_eq!(ranges, vec![(2, 3), (4, 3), (5, 6)]);

        let manager = InProcessRegionManager::new_mock_region_manager();
        let (store_ids, peer_ids, region_id, leader) = bootstrap_with_multi_stores(&manager, 3);
        assert_eq!(store_ids, vec![1, 2, 3]);
        assert_eq!(peer_ids, vec![4, 5, 6]);
        assert_eq!((region_id, leader), (7, 4));
        assert_eq!(manager.get_all_stores().len(), 3);
        let (region, _) = manager.get_region_by_key(&encode_region_key(b"")).unwrap();
        assert_eq!(region.peers.len(), 3);
    }
}
