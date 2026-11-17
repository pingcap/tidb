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


//! Go `br/pkg/utiltest/fakecluster`: an in-memory cluster of stores and
//! regions with checkpoints, serving the log backup RPCs. Go mutates the
//! regions shared between stores without locks; here the whole cluster
//! sits under one mutex and the stores hold region ids.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use tidb_gocontext::Context;
use tidb_proto::{errorpb, logbackup, metapb};

use crate::errors::{self, Result};
use crate::spans::sorted::Span;
use crate::streamhelper::advancer_env::{FlushEventStream, LogBackupClient, LogBackupService};
use crate::streamhelper::regioniter::{RegionWithLeader, Store as MetaStore, TiKVClusterMeta};

/// A hook on a store id.
pub type StoreHook = Arc<dyn Fn(u64) -> Result<()> + Send + Sync>;

/// Go `Store.OnGetRegionCheckpoint`.
pub type CheckpointHook = Arc<dyn Fn(&logbackup::GetLastFlushTsOfRegionRequest) -> Result<()> + Send + Sync>;

/// Go `rand`: a small xorshift, seeded per process.
pub fn rand_u64() -> u64 {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64 | 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    STATE.store(x, Ordering::Relaxed);
    x
}

fn shuffle<T>(v: &mut [T]) {
    for i in (1..v.len()).rev() {
        let j = (rand_u64() % (i as u64 + 1)) as usize;
        v.swap(i, j);
    }
}

/// Go `codec.EncodeBytes(nil, key)`.
fn encode(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    tidb_codec::encode_bytes(&mut out, key);
    out
}

/// Go `FlushSimulator`.
#[derive(Clone, Debug, Default)]
pub struct FlushSimulator {
    /// Go `FlushedEpoch`.
    pub flushed_epoch: u64,
    /// Go `Enabled`.
    pub enabled: bool,
}

impl FlushSimulator {
    fn make_error(&self, requested_epoch: u64) -> Option<errorpb::Error> {
        if !self.enabled {
            return None;
        }
        if self.flushed_epoch == 0 {
            return Some(errorpb::Error { message: "not flushed".into(), ..Default::default() });
        }
        if self.flushed_epoch != requested_epoch {
            return Some(errorpb::Error { message: "flushed epoch not match".into(), ..Default::default() });
        }
        None
    }

    fn fork(&self) -> Self {
        Self { flushed_epoch: 0, enabled: self.enabled }
    }
}

/// Go `Region`.
#[derive(Clone, Debug, Default)]
pub struct Region {
    /// Go `Range`.
    pub range: Span,
    /// Go `Leader`.
    pub leader: u64,
    /// Go `Epoch`.
    pub epoch: u64,
    /// Go `ID`.
    pub id: u64,
    /// Go `Checkpoint`.
    pub checkpoint: u64,
    /// Go `FlushSim`.
    pub flush_sim: FlushSimulator,
}

impl Region {
    /// Go `Region.SplitAt`.
    fn split_at(&mut self, new_id: u64, key: &str) -> Self {
        let new_region = Self {
            range: Span { start_key: key.as_bytes().to_vec(), end_key: self.range.end_key.clone() },
            leader: self.leader,
            epoch: self.epoch + 1,
            id: new_id,
            checkpoint: self.checkpoint,
            flush_sim: self.flush_sim.fork(),
        };
        self.range.end_key = key.as_bytes().to_vec();
        self.epoch += 1;
        self.flush_sim = self.flush_sim.fork();
        new_region
    }

    /// Go `Region.Flush`.
    fn flush(&mut self) {
        self.flush_sim.flushed_epoch = self.epoch;
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}):[{}, {});{}L{}F{}",
            self.id,
            self.epoch,
            hex::encode(&self.range.start_key),
            hex::encode(&self.range.end_key),
            self.checkpoint,
            self.leader,
            self.flush_sim.flushed_epoch
        )
    }
}

/// Go `Store`, without the regions (they live in the cluster).
#[derive(Default)]
pub struct Store {
    /// Go `ID`.
    pub id: u64,
    /// Go `RegionMap`'s keys.
    pub region_ids: BTreeSet<u64>,
    /// Go `SupportsSub`.
    pub supports_sub: bool,
    /// Go `BootstrapAt`.
    pub bootstrap_at: u64,
    /// Go `OnGetRegionCheckpoint`.
    pub on_get_region_checkpoint: Option<CheckpointHook>,
    subscribers: Vec<crossbeam_channel::Sender<logbackup::SubscribeFlushEventResponse>>,
}

/// The state of Go `Cluster`.
#[derive(Default)]
pub struct State {
    /// Go `IDAlloced`.
    pub id_alloced: u64,
    /// Go `StoreMap`.
    pub store_map: BTreeMap<u64, Store>,
    /// Go `Regions`.
    pub regions: Vec<Region>,
    /// Go `ServiceGCSafePoint`.
    pub service_gc_safe_point: u64,
    /// Go `ServiceGCSafePointSet`.
    pub service_gc_safe_point_set: bool,
    /// Go `ServiceGCSafePointDeleted`.
    pub service_gc_safe_point_deleted: bool,
    /// Go `CurrentTS`.
    pub current_ts: u64,
}

impl State {
    fn alloc_id(&mut self) -> u64 {
        self.id_alloced += 1;
        self.id_alloced
    }

    /// Go `RegionList`: by start key.
    pub fn region_list(&self) -> Vec<Region> {
        let mut result = self.regions.clone();
        result.sort_by(|a, b| a.range.start_key.cmp(&b.range.start_key));
        result
    }

    fn find_region_by_key(&mut self, key: &[u8]) -> &mut Region {
        self.regions
            .iter_mut()
            .find(|r| key >= r.range.start_key.as_slice() && (r.range.end_key.is_empty() || key < r.range.end_key.as_slice()))
            .unwrap_or_else(|| panic!("inconsistent key space; key = {}", hex::encode(key)))
    }

    fn find_region_by_id(&mut self, rid: u64) -> Option<&mut Region> {
        self.regions.iter_mut().find(|r| r.id == rid)
    }

    /// Go `SplitAt`.
    fn split_at(&mut self, key: &str) {
        let new_id = self.alloc_id();
        let r = self.find_region_by_key(key.as_bytes());
        let old_id = r.id;
        let new_region = r.split_at(new_id, key);
        for store in self.store_map.values_mut() {
            if store.region_ids.contains(&old_id) {
                store.region_ids.insert(new_id);
            }
        }
        self.regions.push(new_region);
    }

    /// Go `FindPeers`.
    pub fn find_peers(&self, rid: u64) -> Vec<u64> {
        self.store_map.values().filter(|s| s.region_ids.contains(&rid)).map(|s| s.id).collect()
    }

    /// Go `TransferRegionTo`.
    fn transfer_region_to(&mut self, rid: u64, new_peers: &[u64]) {
        for store in self.store_map.values_mut() {
            if new_peers.contains(&store.id) {
                store.region_ids.insert(rid);
            } else {
                store.region_ids.remove(&rid);
            }
        }
    }

    /// Go `FlushExcept` of one store.
    fn store_flush_except(&mut self, store_id: u64, keys: &[&str]) {
        let Some(store) = self.store_map.get(&store_id) else { return };
        let ids: Vec<u64> = store.region_ids.iter().copied().collect();
        let mut resp = Vec::new();
        'outer: for id in ids {
            let Some(r) = self.find_region_by_id(id) else { continue };
            if r.leader != store_id {
                continue;
            }
            for key in keys {
                let k = key.as_bytes();
                if crate::spans::compare_bytes_ext(&r.range.start_key, false, k, false).is_le()
                    && crate::spans::compare_bytes_ext(k, false, &r.range.end_key, true).is_lt()
                {
                    continue 'outer;
                }
            }
            r.flush();
            resp.push(logbackup::FlushEvent {
                start_key: encode(&r.range.start_key),
                end_key: encode(&r.range.end_key),
                checkpoint: r.checkpoint,
            });
        }
        let resp = logbackup::SubscribeFlushEventResponse { events: resp };
        if let Some(store) = self.store_map.get_mut(&store_id) {
            store.subscribers.retain(|ch| !matches!(ch.try_send(resp.clone()), Err(crossbeam_channel::TrySendError::Disconnected(_))));
        }
    }
}

/// Go `Cluster`.
#[derive(Default)]
pub struct Cluster {
    state: Mutex<State>,
    on_get_client: Mutex<Option<StoreHook>>,
    on_clear_cache: Mutex<Option<StoreHook>>,
}

impl Cluster {
    /// The cluster's state.
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `NewBasicCluster`: `n` stores, one region over everything on the
    /// first three, led by the first.
    pub fn new_basic(n: usize, sim_enabled: bool) -> Arc<Self> {
        let c = Self::default();
        {
            let mut st = c.lock();
            let ids: Vec<u64> = (0..n).map(|_| st.alloc_id()).collect();
            let rid = st.alloc_id();
            let region = Region {
                id: rid,
                leader: ids[0],
                flush_sim: FlushSimulator { enabled: sim_enabled, flushed_epoch: 0 },
                ..Default::default()
            };
            for (i, id) in ids.iter().enumerate() {
                let mut s = Store { id: *id, ..Default::default() };
                if i < 3 {
                    s.region_ids.insert(rid);
                }
                st.store_map.insert(*id, s);
            }
            st.regions.push(region);
        }
        Arc::new(c)
    }

    /// Go `SetOnGetClient`.
    pub fn set_on_get_client(&self, hook: Option<StoreHook>) {
        *self.on_get_client.lock().unwrap() = hook;
    }

    /// Go `SetOnClearCache`.
    pub fn set_on_clear_cache(&self, hook: Option<StoreHook>) {
        *self.on_clear_cache.lock().unwrap() = hook;
    }

    /// Go `StoreIDs`.
    pub fn store_ids(&self) -> Vec<u64> {
        self.lock().store_map.keys().copied().collect()
    }

    /// Go `SplitAndScatter`.
    pub fn split_and_scatter(&self, keys: &[&str]) {
        let mut st = self.lock();
        for key in keys {
            st.split_at(key);
        }
        let ids: Vec<u64> = st.regions.iter().map(|r| r.id).collect();
        for rid in ids {
            let mut stores: Vec<u64> = st.store_map.keys().copied().collect();
            shuffle(&mut stores);
            stores.truncate(3);
            st.transfer_region_to(rid, &stores);
            let mut peers = st.find_peers(rid);
            shuffle(&mut peers);
            if let Some(r) = st.find_region_by_id(rid) {
                r.leader = peers[0];
            }
        }
    }

    /// Go `RemoveStore`: leaders move to another peer.
    pub fn remove_store(&self, id: u64) {
        let mut st = self.lock();
        let rids: Vec<u64> = st.store_map[&id].region_ids.iter().copied().collect();
        for rid in rids {
            let peers = st.find_peers(rid);
            if let Some(r) = st.find_region_by_id(rid).filter(|r| r.leader == id) {
                if let Some(p) = peers.into_iter().find(|p| *p != id) {
                    r.leader = p;
                }
            }
        }
        st.store_map.remove(&id);
    }

    /// Go `AdvanceCheckpoints`: every region moves by 1..=256; returns the
    /// minimum.
    pub fn advance_checkpoints(&self) -> u64 {
        let mut st = self.lock();
        let mut min_checkpoint = u64::MAX;
        for r in &mut st.regions {
            r.checkpoint += rand_u64() % 256 + 1;
            min_checkpoint = min_checkpoint.min(r.checkpoint);
            r.flush_sim.flushed_epoch = 0;
        }
        min_checkpoint
    }

    /// Go `AdvanceCheckpointBy`.
    pub fn advance_checkpoint_by(&self, duration: Duration) -> u64 {
        let mut st = self.lock();
        let mut min_checkpoint = u64::MAX;
        for r in &mut st.regions {
            let t = tikv_client::oracle::get_time_from_ts(r.checkpoint) + duration;
            r.checkpoint = tikv_client::oracle::go_time_to_ts(t);
            min_checkpoint = min_checkpoint.min(r.checkpoint);
            r.flush_sim.flushed_epoch = 0;
        }
        min_checkpoint
    }

    /// Go `AdvanceClusterTimeBy`; a negative duration moves time back.
    pub fn advance_cluster_time_by(&self, duration: Duration, back: bool) -> u64 {
        let mut st = self.lock();
        let t = tikv_client::oracle::get_time_from_ts(st.current_ts);
        let t = if back { t - duration } else { t + duration };
        st.current_ts = tikv_client::oracle::go_time_to_ts(t);
        st.current_ts
    }

    /// Go `Store.Flush` of one store.
    pub fn flush_store(&self, id: u64) {
        self.lock().store_flush_except(id, &[]);
    }

    /// Go `FlushAll`.
    pub fn flush_all(&self) {
        self.flush_all_except(&[]);
    }

    /// Go `FlushAllExcept`.
    pub fn flush_all_except(&self, keys: &[&str]) {
        let mut st = self.lock();
        let ids: Vec<u64> = st.store_map.keys().copied().collect();
        for id in ids {
            st.store_flush_except(id, keys);
        }
    }

    /// Go `Store.SetSupportFlushSub`.
    pub fn set_support_flush_sub(&self, id: u64, b: bool) {
        let mut st = self.lock();
        let s = st.store_map.get_mut(&id).expect("store exists");
        s.bootstrap_at += 1;
        s.supports_sub = b;
    }
}

impl std::fmt::Display for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = self.lock();
        write!(f, ">>> fake cluster <<<\nregions: ")?;
        for r in &st.regions {
            write!(f, "{r} ")?;
        }
        writeln!(f)?;
        for s in st.store_map.values() {
            writeln!(f, "{}: {:?}", s.id, s.region_ids)?;
        }
        Ok(())
    }
}

/// Go `trivialFlushStream`.
struct TrivialFlushStream {
    c: crossbeam_channel::Receiver<logbackup::SubscribeFlushEventResponse>,
    cx: Context,
}

impl FlushEventStream for TrivialFlushStream {
    fn recv(&mut self) -> Result<logbackup::SubscribeFlushEventResponse> {
        crossbeam_channel::select! {
            recv(self.c) -> item => item.map_err(|_| errors::errorf("EOF")),
            recv(self.cx.done()) -> _ => match self.c.try_recv() {
                Ok(item) => Ok(item),
                Err(_) => Err(errors::grpc(tonic::Status::cancelled("context canceled"))),
            },
        }
    }
}

/// Go `*Store` as a `LogBackupClient`.
struct StoreClient {
    cluster: Arc<Cluster>,
    id: u64,
}

impl LogBackupClient for StoreClient {
    fn get_last_flush_ts_of_region(
        &self,
        _ctx: &Context,
        req: logbackup::GetLastFlushTsOfRegionRequest,
    ) -> Result<logbackup::GetLastFlushTsOfRegionResponse> {
        let hook = self.cluster.lock().store_map.get(&self.id).and_then(|s| s.on_get_region_checkpoint.clone());
        if let Some(hook) = hook {
            hook(&req)?;
        }
        let mut st = self.cluster.lock();
        let region_ids = st.store_map.get(&self.id).map(|s| s.region_ids.clone()).unwrap_or_default();
        let mut resp = logbackup::GetLastFlushTsOfRegionResponse::default();
        let identity = |id, epoch_version| Some(logbackup::RegionIdentity { id, epoch_version });
        for r in req.regions {
            let region = region_ids.contains(&r.id).then(|| st.find_region_by_id(r.id)).flatten().filter(|reg| reg.leader == self.id);
            let Some(region) = region else {
                resp.checkpoints.push(logbackup::RegionCheckpoint {
                    err: Some(errorpb::Error { message: "not found".into(), ..Default::default() }),
                    region: identity(r.id, r.epoch_version),
                    ..Default::default()
                });
                continue;
            };
            if let Some(err) = region.flush_sim.make_error(r.epoch_version) {
                resp.checkpoints.push(logbackup::RegionCheckpoint {
                    err: Some(err),
                    region: identity(region.id, region.epoch),
                    ..Default::default()
                });
                continue;
            }
            if region.epoch != r.epoch_version {
                resp.checkpoints.push(logbackup::RegionCheckpoint {
                    err: Some(errorpb::Error { message: "epoch not match".into(), ..Default::default() }),
                    region: identity(region.id, region.epoch),
                    ..Default::default()
                });
                continue;
            }
            resp.checkpoints.push(logbackup::RegionCheckpoint {
                checkpoint: region.checkpoint,
                region: identity(region.id, region.epoch),
                ..Default::default()
            });
        }
        Ok(resp)
    }

    fn subscribe_flush_event(
        &self,
        ctx: &Context,
        _req: logbackup::SubscribeFlushEventRequest,
    ) -> Result<Box<dyn FlushEventStream>> {
        let mut st = self.cluster.lock();
        let store = st.store_map.get_mut(&self.id).ok_or_else(|| errors::errorf("store not found"))?;
        if !store.supports_sub {
            return Err(errors::grpc(tonic::Status::unimplemented("meow?")));
        }
        let (tx, rx) = crossbeam_channel::bounded(1024);
        store.subscribers.push(tx);
        Ok(Box::new(TrivialFlushStream { c: rx, cx: ctx.clone() }))
    }
}

impl TiKVClusterMeta for Arc<Cluster> {
    /// Go `RegionScan`.
    fn region_scan(&self, _ctx: &Context, key: &[u8], end_key: &[u8], limit: usize) -> Result<Vec<RegionWithLeader>> {
        let regions = self.lock().region_list();
        let want = Span { start_key: key.to_vec(), end_key: end_key.to_vec() };
        let mut result = Vec::new();
        for region in regions {
            if crate::spans::overlaps(&want, &region.range) && result.len() < limit {
                result.push(RegionWithLeader {
                    region: metapb::Region {
                        id: region.id,
                        start_key: region.range.start_key.clone(),
                        end_key: region.range.end_key.clone(),
                        region_epoch: Some(metapb::RegionEpoch { version: region.epoch, ..Default::default() }),
                        ..Default::default()
                    },
                    leader: metapb::Peer { store_id: region.leader, ..Default::default() },
                });
            } else if region.range.start_key.as_slice() > key {
                break;
            }
        }
        Ok(result)
    }

    fn stores(&self, _ctx: &Context) -> Result<Vec<MetaStore>> {
        Ok(self.lock().store_map.values().map(|s| MetaStore { id: s.id, boot_at: s.bootstrap_at }).collect())
    }

    fn block_gc_until(&self, _ctx: &Context, at: u64) -> Result<u64> {
        let mut st = self.lock();
        if st.service_gc_safe_point > at {
            return Err(errors::errorf(format!(
                "minimal safe point {} is greater than the target {at}",
                st.service_gc_safe_point
            )));
        }
        st.service_gc_safe_point = at;
        st.service_gc_safe_point_set = true;
        Ok(at)
    }

    fn unblock_gc(&self, _ctx: &Context) -> Result<()> {
        self.lock().service_gc_safe_point_deleted = true;
        Ok(())
    }

    fn fetch_current_ts(&self, _ctx: &Context) -> Result<u64> {
        Ok(self.lock().current_ts)
    }
}

impl LogBackupService for Arc<Cluster> {
    fn get_log_backup_client(&self, _ctx: &Context, store_id: u64) -> Result<Arc<dyn LogBackupClient>> {
        let hook = self.on_get_client.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(store_id)?;
        }
        if !self.lock().store_map.contains_key(&store_id) {
            return Err(errors::errorf(format!("the store {store_id} doesn't exist")));
        }
        Ok(Arc::new(StoreClient { cluster: Arc::clone(self), id: store_id }))
    }

    fn clear_cache(&self, _ctx: &Context, store_id: u64) -> Result<()> {
        let hook = self.on_clear_cache.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(store_id)?;
        }
        Ok(())
    }
}
