use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::kv::codec;
use crate::mock::cluster::Cluster as ClusterContract;
use crate::proto::metapb::{self, Buckets, Peer, Region, RegionEpoch, Store, StoreLabel};
use unistore::{IsolationLevel, MockEngine};

pub type RegionLookup = (Region, Option<Peer>, Option<Buckets>, Vec<Peer>);

#[derive(Clone, Debug, PartialEq)]
pub struct RegionState {
    pub meta: Region,
    pub leader_id: u64,
    pub buckets: Option<Buckets>,
}

impl RegionState {
    fn new(
        region_id: u64,
        store_ids: &[u64],
        peer_ids: &[u64],
        leader_id: u64,
        epoch: Option<(u64, u64)>,
    ) -> Self {
        assert_eq!(store_ids.len(), peer_ids.len(), "store/peer count mismatch");
        Self {
            meta: Region {
                id: region_id,
                peers: store_ids
                    .iter()
                    .zip(peer_ids)
                    .map(|(store_id, peer_id)| Peer {
                        id: *peer_id,
                        store_id: *store_id,
                        ..Default::default()
                    })
                    .collect(),
                region_epoch: Some(RegionEpoch {
                    conf_ver: epoch.map_or(0, |value| value.0),
                    version: epoch.map_or(0, |value| value.1),
                }),
                ..Default::default()
            },
            leader_id,
            buckets: None,
        }
    }

    pub fn leader(&self) -> Option<Peer> {
        self.meta
            .peers
            .iter()
            .find(|peer| peer.id == self.leader_id)
            .cloned()
    }

    fn add_peer(&mut self, peer_id: u64, store_id: u64, role: metapb::PeerRole) {
        self.meta.peers.push(Peer {
            id: peer_id,
            store_id,
            role: role as i32,
            ..Default::default()
        });
        self.inc_conf_ver();
    }

    fn remove_peer(&mut self, peer_id: u64) {
        self.meta.peers.retain(|peer| peer.id != peer_id);
        if self.leader_id == peer_id {
            self.leader_id = 0;
        }
        self.inc_conf_ver();
    }

    fn split(
        &mut self,
        new_region_id: u64,
        key: Vec<u8>,
        peer_ids: &[u64],
        leader_id: u64,
    ) -> Self {
        assert_eq!(self.meta.peers.len(), peer_ids.len(), "peer count mismatch");
        let store_ids: Vec<_> = self.meta.peers.iter().map(|peer| peer.store_id).collect();
        let mut right = Self::new(new_region_id, &store_ids, peer_ids, leader_id, None);
        right.update_key_range(key.clone(), self.meta.end_key.clone());
        self.update_key_range(self.meta.start_key.clone(), key);
        right
    }

    fn update_key_range(&mut self, start: Vec<u8>, end: Vec<u8>) {
        self.meta.start_key = start;
        self.meta.end_key = end;
        self.inc_version();
    }

    fn inc_conf_ver(&mut self) {
        let epoch = self.meta.region_epoch.get_or_insert_default();
        epoch.conf_ver += 1;
    }

    fn inc_version(&mut self) {
        let epoch = self.meta.region_epoch.get_or_insert_default();
        epoch.version += 1;
    }
}

#[derive(Clone)]
struct StoreState {
    meta: Store,
    cancelled: bool,
}

#[derive(Default)]
struct ClusterState {
    id: u64,
    stores: HashMap<u64, StoreState>,
    regions: HashMap<u64, RegionState>,
    down_peers: HashSet<u64>,
}

#[derive(Clone)]
pub struct Cluster {
    state: Arc<RwLock<ClusterState>>,
    delays: Arc<Mutex<HashMap<(u64, u64), Duration>>>,
    engine: MockEngine,
}

impl Cluster {
    pub fn new(engine: MockEngine) -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::default())),
            delays: Arc::new(Mutex::new(HashMap::new())),
            engine,
        }
    }

    pub fn engine(&self) -> MockEngine {
        self.engine.clone()
    }

    pub fn alloc_id(&self) -> u64 {
        let mut state = self.state.write().expect("cluster lock poisoned");
        state.id += 1;
        state.id
    }

    pub fn alloc_ids(&self, count: usize) -> Vec<u64> {
        (0..count).map(|_| self.alloc_id()).collect()
    }

    pub fn all_regions(&self) -> Vec<RegionState> {
        self.state
            .read()
            .expect("cluster lock poisoned")
            .regions
            .values()
            .cloned()
            .collect()
    }

    pub fn store(&self, store_id: u64) -> Option<Store> {
        self.state
            .read()
            .expect("cluster lock poisoned")
            .stores
            .get(&store_id)
            .map(|store| store.meta.clone())
    }

    pub fn all_stores(&self) -> Vec<Store> {
        self.state
            .read()
            .expect("cluster lock poisoned")
            .stores
            .values()
            .map(|store| store.meta.clone())
            .collect()
    }

    pub fn stop_store(&self, store_id: u64) {
        self.set_store_state(store_id, metapb::StoreState::Offline);
    }

    pub fn start_store(&self, store_id: u64) {
        self.set_store_state(store_id, metapb::StoreState::Up);
    }

    pub fn mark_tombstone(&self, store_id: u64) {
        self.set_store_state(store_id, metapb::StoreState::Tombstone);
    }

    fn set_store_state(&self, store_id: u64, state_value: metapb::StoreState) {
        if let Some(store) = self
            .state
            .write()
            .expect("cluster lock poisoned")
            .stores
            .get_mut(&store_id)
        {
            store.meta.state = state_value as i32;
        }
    }

    pub fn cancel_store(&self, store_id: u64) {
        if let Some(store) = self
            .state
            .write()
            .expect("cluster lock poisoned")
            .stores
            .get_mut(&store_id)
        {
            store.cancelled = true;
        }
    }

    pub fn uncancel_store(&self, store_id: u64) {
        if let Some(store) = self
            .state
            .write()
            .expect("cluster lock poisoned")
            .stores
            .get_mut(&store_id)
        {
            store.cancelled = false;
        }
    }

    pub fn store_by_addr(&self, address: &str) -> Option<Store> {
        self.state
            .read()
            .expect("cluster lock poisoned")
            .stores
            .values()
            .find(|store| store.meta.address == address)
            .map(|store| store.meta.clone())
    }

    pub fn checked_stores_by_addr(&self, address: &str) -> Result<Vec<Store>, tonic::Status> {
        let state = self.state.read().expect("cluster lock poisoned");
        if state.stores.values().any(|store| store.cancelled) {
            return Err(tonic::Status::cancelled("context canceled"));
        }
        Ok(state
            .stores
            .values()
            .filter(|store| store.meta.address == address)
            .map(|store| store.meta.clone())
            .collect())
    }

    pub fn add_store(&self, store_id: u64, address: impl Into<String>, labels: Vec<StoreLabel>) {
        let address = address.into();
        self.state
            .write()
            .expect("cluster lock poisoned")
            .stores
            .insert(
                store_id,
                StoreState {
                    meta: Store {
                        id: store_id,
                        address: address.clone(),
                        peer_address: address,
                        labels,
                        ..Default::default()
                    },
                    cancelled: false,
                },
            );
    }

    pub fn remove_store(&self, store_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .stores
            .remove(&store_id);
    }

    pub fn update_store_addr(
        &self,
        store_id: u64,
        address: impl Into<String>,
        labels: Vec<StoreLabel>,
    ) {
        self.add_store(store_id, address, labels);
    }

    pub fn update_store_peer_addr(
        &self,
        store_id: u64,
        peer_address: impl Into<String>,
        labels: Vec<StoreLabel>,
    ) {
        let address = self
            .store(store_id)
            .map_or_else(String::new, |store| store.address);
        self.add_store(store_id, address, labels);
        if let Some(store) = self
            .state
            .write()
            .expect("cluster lock poisoned")
            .stores
            .get_mut(&store_id)
        {
            store.meta.peer_address = peer_address.into();
        }
    }

    pub fn update_store_labels(&self, store_id: u64, labels: Vec<StoreLabel>) {
        let mut state = self.state.write().expect("cluster lock poisoned");
        let Some(store) = state.stores.get_mut(&store_id) else {
            return;
        };
        let mut merged: BTreeMap<String, String> = store
            .meta
            .labels
            .drain(..)
            .map(|label| (label.key, label.value))
            .collect();
        merged.extend(labels.into_iter().map(|label| (label.key, label.value)));
        store.meta.labels = merged
            .into_iter()
            .map(|(key, value)| StoreLabel { key, value })
            .collect();
    }

    pub fn mark_peer_down(&self, peer_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .down_peers
            .insert(peer_id);
    }

    pub fn remove_down_peer(&self, peer_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .down_peers
            .remove(&peer_id);
    }

    pub fn region(&self, region_id: u64) -> Option<(Region, u64)> {
        self.state
            .read()
            .expect("cluster lock poisoned")
            .regions
            .get(&region_id)
            .map(|region| (region.meta.clone(), region.leader_id))
    }

    pub fn region_by_key(&self, key: &[u8]) -> Option<RegionLookup> {
        let state = self.state.read().expect("cluster lock poisoned");
        state
            .regions
            .values()
            .find(|region| region_contains(&region.meta.start_key, &region.meta.end_key, key))
            .map(|region| region_tuple(&state, region))
    }

    pub fn previous_region_by_key(&self, key: &[u8]) -> Option<RegionLookup> {
        let state = self.state.read().expect("cluster lock poisoned");
        let current = state
            .regions
            .values()
            .find(|region| region_contains(&region.meta.start_key, &region.meta.end_key, key))?;
        if current.meta.start_key.is_empty() {
            return None;
        }
        state
            .regions
            .values()
            .find(|region| region.meta.end_key == current.meta.start_key)
            .map(|region| region_tuple(&state, region))
    }

    pub fn region_by_id(&self, region_id: u64) -> Option<RegionLookup> {
        let state = self.state.read().expect("cluster lock poisoned");
        state
            .regions
            .get(&region_id)
            .map(|region| region_tuple(&state, region))
    }

    pub fn scan_regions(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Vec<(Region, Peer, Option<Buckets>, Vec<Peer>)> {
        let state = self.state.read().expect("cluster lock poisoned");
        let mut regions: Vec<_> = state
            .regions
            .values()
            .filter(|region| {
                (region.meta.end_key.is_empty() || region.meta.end_key.as_slice() > start)
                    && (end.is_empty() || region.meta.start_key.as_slice() < end)
            })
            .collect();
        regions.sort_by(|left, right| left.meta.start_key.cmp(&right.meta.start_key));
        if limit > 0 {
            regions.truncate(limit);
        }
        regions
            .into_iter()
            .map(|region| {
                let tuple = region_tuple(&state, region);
                (tuple.0, tuple.1.unwrap_or_default(), tuple.2, tuple.3)
            })
            .collect()
    }

    pub fn bootstrap(&self, region_id: u64, store_ids: &[u64], peer_ids: &[u64], leader_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .regions
            .insert(
                region_id,
                RegionState::new(region_id, store_ids, peer_ids, leader_id, None),
            );
    }

    pub fn put_region(
        &self,
        region_id: u64,
        conf_ver: u64,
        version: u64,
        store_ids: &[u64],
        peer_ids: &[u64],
        leader_id: u64,
    ) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .regions
            .insert(
                region_id,
                RegionState::new(
                    region_id,
                    store_ids,
                    peer_ids,
                    leader_id,
                    Some((conf_ver, version)),
                ),
            );
    }

    pub fn add_peer(&self, region_id: u64, store_id: u64, peer_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .regions
            .get_mut(&region_id)
            .expect("region must exist")
            .add_peer(peer_id, store_id, metapb::PeerRole::Voter);
    }

    pub fn add_learner(&self, region_id: u64, store_id: u64, peer_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .regions
            .get_mut(&region_id)
            .expect("region must exist")
            .add_peer(peer_id, store_id, metapb::PeerRole::Learner);
    }

    pub fn remove_peer(&self, region_id: u64, peer_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .regions
            .get_mut(&region_id)
            .expect("region must exist")
            .remove_peer(peer_id);
    }

    pub fn change_leader(&self, region_id: u64, leader_id: u64) {
        self.state
            .write()
            .expect("cluster lock poisoned")
            .regions
            .get_mut(&region_id)
            .expect("region must exist")
            .leader_id = leader_id;
    }

    pub fn give_up_leader(&self, region_id: u64) {
        self.change_leader(region_id, 0);
    }

    pub fn split(
        &self,
        region_id: u64,
        new_region_id: u64,
        raw_key: &[u8],
        peer_ids: &[u64],
        leader_id: u64,
    ) {
        let mut encoded = Vec::new();
        codec::encode_bytes(&mut encoded, raw_key);
        self.split_raw(region_id, new_region_id, &encoded, peer_ids, leader_id);
    }

    pub fn split_raw(
        &self,
        region_id: u64,
        new_region_id: u64,
        key: &[u8],
        peer_ids: &[u64],
        leader_id: u64,
    ) -> Region {
        let mut state = self.state.write().expect("cluster lock poisoned");
        let right = state
            .regions
            .get_mut(&region_id)
            .expect("region must exist")
            .split(new_region_id, key.to_vec(), peer_ids, leader_id);
        let meta = right.meta.clone();
        state.regions.insert(new_region_id, right);
        meta
    }

    pub fn split_region_buckets(&self, region_id: u64, keys: &[Vec<u8>], version: u64) {
        let mut encoded_keys = Vec::with_capacity(keys.len());
        for key in keys {
            let mut encoded = Vec::new();
            codec::encode_bytes(&mut encoded, key);
            encoded_keys.push(encoded);
        }
        self.state
            .write()
            .expect("cluster lock poisoned")
            .regions
            .get_mut(&region_id)
            .expect("region must exist")
            .buckets = Some(Buckets {
            region_id,
            version,
            keys: encoded_keys,
            ..Default::default()
        });
    }

    pub fn merge(&self, left_id: u64, right_id: u64) {
        let mut state = self.state.write().expect("cluster lock poisoned");
        let end = state
            .regions
            .get(&right_id)
            .expect("right region must exist")
            .meta
            .end_key
            .clone();
        let left = state
            .regions
            .get_mut(&left_id)
            .expect("left region must exist");
        left.meta.end_key = end;
        left.inc_version();
        state.regions.remove(&right_id);
    }

    pub fn split_keys(&self, start: &[u8], end: &[u8], count: usize) {
        assert!(count > 0, "split count must be positive");
        let pairs = self.engine.scan(
            start,
            end,
            i32::MAX as usize,
            u64::MAX,
            IsolationLevel::SnapshotIsolation,
            &[],
            false,
        );
        let mut encoded_start = Vec::new();
        let mut encoded_end = Vec::new();
        if !start.is_empty() {
            codec::encode_bytes(&mut encoded_start, start);
        }
        if !end.is_empty() {
            codec::encode_bytes(&mut encoded_end, end);
        }
        let mut state = self.state.write().expect("cluster lock poisoned");
        evacuate_old_regions(&mut state, &encoded_start, &encoded_end);
        let quotient = pairs.len() / count;
        let mut remainder = pairs.len() % count;
        let store_id = state.stores.keys().next().copied().unwrap_or_default();
        let mut cursor = 0;
        while cursor < pairs.len() {
            let group_size = quotient + usize::from(remainder > 0);
            remainder = remainder.saturating_sub(1);
            let next_cursor = cursor + group_size;
            state.id += 1;
            let peer_id = state.id;
            state.id += 1;
            let region_id = state.id;
            let mut region = RegionState::new(region_id, &[store_id], &[peer_id], peer_id, None);
            let region_start = if cursor == 0 {
                encoded_start.clone()
            } else {
                let mut encoded = Vec::new();
                codec::encode_bytes(&mut encoded, &pairs[cursor].key);
                encoded
            };
            let region_end = if next_cursor == pairs.len() {
                encoded_end.clone()
            } else {
                let mut encoded = Vec::new();
                codec::encode_bytes(&mut encoded, &pairs[next_cursor].key);
                encoded
            };
            region.update_key_range(region_start, region_end);
            state.regions.insert(region_id, region);
            cursor = next_cursor;
        }
    }

    pub fn schedule_delay(&self, start_ts: u64, region_id: u64, duration: Duration) {
        self.delays
            .lock()
            .expect("delay lock poisoned")
            .insert((start_ts, region_id), duration);
    }

    pub async fn handle_delay(&self, start_ts: u64, region_id: u64) {
        let delay = self
            .delays
            .lock()
            .expect("delay lock poisoned")
            .remove(&(start_ts, region_id));
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }
}

impl ClusterContract for Cluster {
    fn alloc_id(&self) -> u64 {
        Cluster::alloc_id(self)
    }

    fn region_by_key(
        &self,
        key: &[u8],
    ) -> (Option<Region>, Option<Peer>, Option<Buckets>, Vec<Peer>) {
        Cluster::region_by_key(self, key)
            .map(|(region, leader, buckets, down)| (Some(region), leader, buckets, down))
            .unwrap_or_default()
    }

    fn all_stores(&self) -> Vec<Store> {
        Cluster::all_stores(self)
    }

    fn schedule_delay(&self, start_ts: u64, region_id: u64, duration: Duration) {
        Cluster::schedule_delay(self, start_ts, region_id, duration);
    }

    fn split(
        &self,
        region_id: u64,
        new_region_id: u64,
        key: &[u8],
        peer_ids: &[u64],
        leader_peer_id: u64,
    ) {
        Cluster::split(
            self,
            region_id,
            new_region_id,
            key,
            peer_ids,
            leader_peer_id,
        );
    }

    fn split_raw(
        &self,
        region_id: u64,
        new_region_id: u64,
        raw_key: &[u8],
        peer_ids: &[u64],
        leader_peer_id: u64,
    ) -> Option<Region> {
        Some(Cluster::split_raw(
            self,
            region_id,
            new_region_id,
            raw_key,
            peer_ids,
            leader_peer_id,
        ))
    }

    fn split_keys(&self, start: &[u8], end: &[u8], count: isize) {
        Cluster::split_keys(
            self,
            start,
            end,
            usize::try_from(count).expect("positive split count"),
        );
    }

    fn add_store(&self, store_id: u64, address: &str, labels: Vec<StoreLabel>) {
        Cluster::add_store(self, store_id, address, labels);
    }

    fn remove_store(&self, store_id: u64) {
        Cluster::remove_store(self, store_id);
    }
}

pub fn region_contains(start: &[u8], end: &[u8], key: &[u8]) -> bool {
    start <= key && (end.is_empty() || key < end)
}

fn region_tuple(
    state: &ClusterState,
    region: &RegionState,
) -> (Region, Option<Peer>, Option<Buckets>, Vec<Peer>) {
    let down = region
        .meta
        .peers
        .iter()
        .filter(|peer| state.down_peers.contains(&peer.id))
        .cloned()
        .collect();
    (
        region.meta.clone(),
        region.leader(),
        region.buckets.clone(),
        down,
    )
}

fn evacuate_old_regions(state: &mut ClusterState, start: &[u8], end: &[u8]) {
    let ids: Vec<u64> = state
        .regions
        .iter()
        .filter(|(_, region)| {
            (region.meta.end_key.is_empty() || region.meta.end_key.as_slice() > start)
                && (end.is_empty() || region.meta.start_key.as_slice() < end)
        })
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        let region = state.regions.get(&id).expect("region exists").clone();
        let starts_before = region.meta.start_key.as_slice() < start;
        let ends_after = region.meta.end_key.is_empty()
            || (!end.is_empty() && region.meta.end_key.as_slice() > end);
        match (starts_before, ends_after) {
            (false, false) => {
                state.regions.remove(&id);
            }
            (true, true) => {
                state
                    .regions
                    .get_mut(&id)
                    .expect("region exists")
                    .update_key_range(region.meta.start_key.clone(), start.to_vec());
                state.id += 1;
                let peer_id = state.id;
                state.id += 1;
                let region_id = state.id;
                let store_id = state.stores.keys().next().copied().unwrap_or_default();
                let mut right = RegionState::new(region_id, &[store_id], &[peer_id], peer_id, None);
                right.update_key_range(end.to_vec(), region.meta.end_key);
                state.regions.insert(region_id, right);
            }
            (true, false) => state
                .regions
                .get_mut(&id)
                .expect("region exists")
                .update_key_range(region.meta.start_key, start.to_vec()),
            (false, true) => state
                .regions
                .get_mut(&id)
                .expect("region exists")
                .update_key_range(end.to_vec(), region.meta.end_key),
        }
    }
}

pub fn bootstrap_with_single_store(cluster: &Cluster) -> (u64, u64, u64) {
    let ids = cluster.alloc_ids(3);
    let (store_id, peer_id, region_id) = (ids[0], ids[1], ids[2]);
    cluster.add_store(store_id, format!("store{store_id}"), Vec::new());
    cluster.bootstrap(region_id, &[store_id], &[peer_id], peer_id);
    (store_id, peer_id, region_id)
}

pub fn bootstrap_with_multi_stores(
    cluster: &Cluster,
    count: usize,
) -> (Vec<u64>, Vec<u64>, u64, u64) {
    let stores = cluster.alloc_ids(count);
    let peers = cluster.alloc_ids(count);
    let leader = peers[0];
    let region = cluster.alloc_id();
    for store_id in &stores {
        cluster.add_store(
            *store_id,
            format!("store{store_id}"),
            vec![StoreLabel {
                key: "id".to_owned(),
                value: store_id.to_string(),
            }],
        );
    }
    cluster.bootstrap(region, &stores, &peers, leader);
    (stores, peers, region, leader)
}

pub fn bootstrap_with_multi_regions(
    cluster: &Cluster,
    split_keys: &[Vec<u8>],
) -> (u64, Vec<u64>, Vec<u64>) {
    let (store, first_peer, first_region) = bootstrap_with_single_store(cluster);
    let mut regions = vec![first_region];
    regions.extend(cluster.alloc_ids(split_keys.len()));
    let mut peers = vec![first_peer];
    peers.extend(cluster.alloc_ids(split_keys.len()));
    for (index, key) in split_keys.iter().enumerate() {
        cluster.split(
            regions[index],
            regions[index + 1],
            key,
            &[peers[index]],
            peers[index],
        );
    }
    (store, regions, peers)
}

pub fn bootstrap_with_multi_zones(
    cluster: &Cluster,
    zone_count: usize,
    stores_per_zone: usize,
) -> (Vec<u64>, Vec<u64>, u64, u64, HashMap<u64, String>) {
    let stores = cluster.alloc_ids(zone_count * stores_per_zone);
    let peers = cluster.alloc_ids(zone_count);
    let leader = peers[0];
    let region = cluster.alloc_id();
    let mut store_zones = HashMap::new();
    for (index, store_id) in stores.iter().enumerate() {
        let zone = format!("z{}", index % zone_count + 1);
        store_zones.insert(*store_id, zone.clone());
        cluster.add_store(
            *store_id,
            format!("store{store_id}"),
            vec![
                StoreLabel {
                    key: "id".to_owned(),
                    value: store_id.to_string(),
                },
                StoreLabel {
                    key: "zone".to_owned(),
                    value: zone,
                },
            ],
        );
    }
    cluster.bootstrap(region, &stores[..zone_count], &peers, leader);
    (stores, peers, region, leader, store_zones)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_region_boundaries_topology_and_bootstrap_helpers() {
        assert!(region_contains(b"", b"", b""));
        assert!(region_contains(b"a", b"", b"a"));
        assert!(!region_contains(b"a", b"b", b"b"));

        let cluster = Cluster::new(MockEngine::new());
        let (store, peer, region) = bootstrap_with_single_store(&cluster);
        assert_eq!(
            cluster.store(store).unwrap().address,
            format!("store{store}")
        );
        assert_eq!(cluster.region(region).unwrap().1, peer);
        let right = cluster.split_raw(
            region,
            cluster.alloc_id(),
            b"m",
            &[cluster.alloc_id()],
            peer,
        );
        assert_eq!(right.start_key, b"m");
    }

    #[test]
    fn source_split_keys_distributes_remainder_to_earlier_regions() {
        let engine = MockEngine::new();
        for (timestamp, key) in [b"c", b"d", b"e", b"f", b"g"].into_iter().enumerate() {
            let start_ts = timestamp as u64 * 2 + 1;
            assert_eq!(
                engine.prewrite(&unistore::PrewriteRequest {
                    mutations: vec![unistore::TxnMutation::put(key, key)],
                    primary: key.to_vec(),
                    start_ts,
                    ..Default::default()
                }),
                vec![None]
            );
            engine
                .commit(&[key.to_vec()], start_ts, start_ts + 1)
                .unwrap();
        }
        let cluster = Cluster::new(engine);
        bootstrap_with_single_store(&cluster);
        cluster.split_keys(b"b", b"y", 3);

        let encode = |raw: &[u8]| {
            let mut encoded = Vec::new();
            codec::encode_bytes(&mut encoded, raw);
            encoded
        };
        let range_start = encode(b"b");
        let range_end = encode(b"y");
        let mut ranges: Vec<_> = cluster
            .all_regions()
            .into_iter()
            .filter(|region| {
                region.meta.start_key >= range_start
                    && !region.meta.end_key.is_empty()
                    && region.meta.end_key <= range_end
            })
            .map(|region| (region.meta.start_key, region.meta.end_key))
            .collect();
        ranges.sort();
        assert_eq!(
            ranges,
            vec![
                (encode(b"b"), encode(b"e")),
                (encode(b"e"), encode(b"g")),
                (encode(b"g"), encode(b"y")),
            ]
        );
    }
}
