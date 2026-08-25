use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::kv::codec;
use crate::pd::PdClient;
use crate::proto::{keyspacepb, metapb, pdpb, resource_manager};
use crate::region::RegionWithLeader;
use crate::store::{RegionStore, Store};
use crate::timestamp::TimestampExt;
use crate::{Error, Key, Result, Timestamp};

use super::{Cluster, RpcClient};

const DEFAULT_RESOURCE_GROUP_NAME: &str = "default";
static GLOBAL_TSO: Mutex<(i64, i64)> = Mutex::new((0, 0));

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcBarrier {
    pub id: String,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcState {
    pub transaction_safe_point: u64,
    pub gc_safe_point: u64,
    pub barriers: Vec<GcBarrier>,
}

#[derive(Default)]
struct SafePointState {
    gc_safe_point: u64,
    transaction_safe_point: u64,
    barriers: HashMap<String, u64>,
}

#[derive(Clone)]
pub struct MockPdClient {
    cluster: Cluster,
    rpc: RpcClient,
    external_timestamp: Arc<Mutex<u64>>,
    safe_points: Arc<Mutex<SafePointState>>,
    resource_groups: Arc<Mutex<HashMap<String, resource_manager::ResourceGroup>>>,
    delay: Option<Arc<AtomicBool>>,
}

impl MockPdClient {
    pub fn new(cluster: Cluster) -> Self {
        Self {
            rpc: RpcClient::new(cluster.clone(), cluster.engine()),
            cluster,
            external_timestamp: Arc::new(Mutex::new(0)),
            safe_points: Arc::new(Mutex::new(SafePointState::default())),
            resource_groups: Arc::new(Mutex::new(HashMap::from([(
                DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
                default_resource_group(),
            )]))),
            delay: None,
        }
    }

    pub fn with_delay(mut self, delay: Arc<AtomicBool>) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn get_timestamp_parts(&self) -> (i64, i64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_millis() as i64;
        let mut tso = GLOBAL_TSO.lock().expect("TSO lock poisoned");
        if tso.0 >= now {
            tso.1 += 1;
        } else {
            *tso = (now, 0);
        }
        *tso
    }

    pub fn resource_group(&self, name: &str) -> Result<resource_manager::ResourceGroup> {
        self.resource_groups
            .lock()
            .expect("resource-group lock poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| Error::StringError(format!("the group {name} does not exist")))
    }

    pub fn resource_groups(&self) -> Vec<resource_manager::ResourceGroup> {
        self.resource_groups
            .lock()
            .expect("resource-group lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn set_external_timestamp_value(&self, timestamp: u64) -> Result<()> {
        let (physical, logical) = self.get_timestamp_parts();
        let global = (u64::try_from(physical).expect("positive physical") << 18)
            | u64::try_from(logical).expect("positive logical");
        if timestamp > global {
            return Err(Error::StringError(
                "external timestamp is greater than global tso".to_owned(),
            ));
        }
        let mut external = self
            .external_timestamp
            .lock()
            .expect("external timestamp lock poisoned");
        if timestamp < *external {
            return Err(Error::StringError(
                "cannot decrease the external timestamp".to_owned(),
            ));
        }
        *external = timestamp;
        Ok(())
    }

    pub fn external_timestamp(&self) -> u64 {
        *self
            .external_timestamp
            .lock()
            .expect("external timestamp lock poisoned")
    }

    pub fn update_gc_safe_point(&self, safe_point: u64) -> u64 {
        let mut state = self.safe_points.lock().expect("GC state lock poisoned");
        state.gc_safe_point = state.gc_safe_point.max(safe_point);
        state.gc_safe_point
    }

    pub fn update_service_safe_point(&self, service_id: &str, ttl: i64, safe_point: u64) -> u64 {
        let mut state = self.safe_points.lock().expect("GC state lock poisoned");
        if ttl == 0 {
            state.barriers.remove(service_id);
        } else {
            let minimum = state.barriers.values().copied().min();
            if minimum.is_none_or(|minimum| minimum <= safe_point) {
                state.barriers.insert(service_id.to_owned(), safe_point);
            }
        }
        state.barriers.values().copied().min().unwrap_or(u64::MAX)
    }

    pub fn set_gc_barrier(&self, id: &str, timestamp: u64) -> Result<GcBarrier> {
        if id.is_empty() || timestamp == 0 {
            return Err(Error::StringError("invalid arguments".to_owned()));
        }
        let mut state = self.safe_points.lock().expect("GC state lock poisoned");
        if timestamp < state.transaction_safe_point {
            return Err(Error::StringError(format!(
                "trying to set a GC barrier on ts {timestamp} which is already behind the txn safe point {}",
                state.transaction_safe_point
            )));
        }
        state.barriers.insert(id.to_owned(), timestamp);
        Ok(GcBarrier {
            id: id.to_owned(),
            timestamp,
        })
    }

    pub fn delete_gc_barrier(&self, id: &str) -> Option<GcBarrier> {
        self.safe_points
            .lock()
            .expect("GC state lock poisoned")
            .barriers
            .remove(id)
            .map(|timestamp| GcBarrier {
                id: id.to_owned(),
                timestamp,
            })
    }

    pub fn gc_state(&self) -> GcState {
        let state = self.safe_points.lock().expect("GC state lock poisoned");
        let mut barriers: Vec<_> = state
            .barriers
            .iter()
            .map(|(id, timestamp)| GcBarrier {
                id: id.clone(),
                timestamp: *timestamp,
            })
            .collect();
        barriers.sort_by(|left, right| left.id.cmp(&right.id));
        GcState {
            transaction_safe_point: state.transaction_safe_point,
            gc_safe_point: state.gc_safe_point,
            barriers,
        }
    }

    pub fn advance_transaction_safe_point(&self, target: u64) -> Result<(u64, u64, String)> {
        let mut state = self.safe_points.lock().expect("GC state lock poisoned");
        if target < state.transaction_safe_point {
            return Err(Error::StringError(format!(
                "trying to update txn safe point to a smaller value, current value: {}, given: {target}",
                state.transaction_safe_point
            )));
        }
        let old = state.transaction_safe_point;
        let blocker = state
            .barriers
            .iter()
            .min_by_key(|(_, timestamp)| *timestamp)
            .map(|(name, timestamp)| (name.clone(), *timestamp));
        let mut new = target;
        let mut description = String::new();
        if let Some((name, timestamp)) = blocker.filter(|(_, timestamp)| *timestamp < new) {
            new = timestamp.max(old);
            description = format!(
                "GCBarrier {{ BarrierID: {name:?}, BarrierTS: {timestamp}, ExpirationTime: <nil> }}"
            );
        }
        state.transaction_safe_point = new;
        Ok((old, new, description))
    }

    pub fn advance_gc_safe_point(&self, target: u64) -> Result<(u64, u64)> {
        let mut state = self.safe_points.lock().expect("GC state lock poisoned");
        if target < state.gc_safe_point {
            return Err(Error::StringError(format!(
                "trying to update gc safe point to a smaller value, current value: {}, given: {target}",
                state.gc_safe_point
            )));
        }
        if target > state.transaction_safe_point {
            return Err(Error::StringError(format!(
                "trying to update GC safe point to a too large value that exceeds the txn safe point, current value: {}, given: {target}, current txn safe point: {}",
                state.gc_safe_point, state.transaction_safe_point
            )));
        }
        let old = state.gc_safe_point;
        state.gc_safe_point = target;
        Ok((old, target))
    }

    fn logical_region(&self, mut region: metapb::Region) -> Result<metapb::Region> {
        region.start_key = decode_boundary(&region.start_key)?;
        region.end_key = decode_boundary(&region.end_key)?;
        Ok(region)
    }

    fn region_with_leader(
        &self,
        region: metapb::Region,
        leader: Option<metapb::Peer>,
        buckets: Option<metapb::Buckets>,
        down: Vec<metapb::Peer>,
    ) -> Result<RegionWithLeader> {
        Ok(RegionWithLeader {
            region: self.logical_region(region)?,
            leader,
            buckets: buckets.map(|mut buckets| {
                buckets.keys = buckets
                    .keys
                    .into_iter()
                    .map(|key| decode_boundary(&key).expect("bucket key must decode"))
                    .collect();
                buckets
            }),
            pending_peers: Vec::new(),
            down_peers: down
                .into_iter()
                .map(|peer| pdpb::PeerStats {
                    peer: Some(peer),
                    ..Default::default()
                })
                .collect(),
        })
    }
}

#[async_trait]
impl PdClient for MockPdClient {
    type KvClient = RpcClient;

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        let store_id = region
            .leader
            .as_ref()
            .ok_or_else(|| Error::StringError("mock region has no leader".to_owned()))?
            .store_id;
        let store = self
            .cluster
            .store(store_id)
            .ok_or_else(|| Error::StringError(format!("invalid store ID {store_id}, not found")))?;
        let mut mapped = RegionStore::new(region, Arc::new(self.rpc.clone()));
        mapped.target = store.address;
        Ok(mapped)
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let mut encoded = Vec::new();
        codec::encode_bytes(&mut encoded, key.into());
        let (region, leader, buckets, down) = self
            .cluster
            .region_by_key(&encoded)
            .ok_or_else(|| Error::StringError("mock region not found for key".to_owned()))?;
        let result = self.region_with_leader(region, leader, buckets, down)?;
        if self
            .delay
            .as_ref()
            .is_some_and(|delay| delay.load(Ordering::Acquire))
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(result)
    }

    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let bytes: &[u8] = key.into();
        let mut encoded = Vec::new();
        if !bytes.is_empty() {
            codec::encode_bytes(&mut encoded, bytes);
        }
        let region = if encoded.is_empty() {
            let mut regions = self.cluster.scan_regions(b"", b"", usize::MAX);
            regions.pop()
        } else {
            self.cluster
                .previous_region_by_key(&encoded)
                .map(|value| (value.0, value.1.unwrap_or_default(), value.2, value.3))
        }
        .ok_or_else(|| Error::StringError("mock region not found for end key".to_owned()))?;
        self.region_with_leader(region.0, Some(region.1), region.2, region.3)
    }

    async fn region_for_id(&self, id: u64) -> Result<RegionWithLeader> {
        let (region, leader, buckets, down) = self
            .cluster
            .region_by_id(id)
            .ok_or(Error::RegionNotFoundInResponse { region_id: id })?;
        self.region_with_leader(region, leader, buckets, down)
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        let (physical, logical) = self.get_timestamp_parts();
        Ok(Timestamp {
            physical,
            logical,
            ..Default::default()
        })
    }

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::from_version(0))
    }

    async fn cluster_id(&self) -> u64 {
        1
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        self.set_external_timestamp_value(timestamp)
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Ok(self.external_timestamp())
    }

    async fn update_safepoint(self: Arc<Self>, safe_point: u64) -> Result<bool> {
        Ok(self.update_gc_safe_point(safe_point) == safe_point)
    }

    async fn update_safepoint_value(self: Arc<Self>, safe_point: u64) -> Result<u64> {
        Ok(self.update_gc_safe_point(safe_point))
    }

    async fn get_gc_state(self: Arc<Self>) -> Result<pdpb::GcState> {
        let state = self.gc_state();
        Ok(pdpb::GcState {
            txn_safe_point: state.transaction_safe_point,
            gc_safe_point: state.gc_safe_point,
            gc_barriers: state
                .barriers
                .into_iter()
                .map(|barrier| pdpb::GcBarrierInfo {
                    barrier_id: barrier.id,
                    barrier_ts: barrier.timestamp,
                    ttl_seconds: -1,
                })
                .collect(),
            ..Default::default()
        })
    }

    async fn advance_transaction_safe_point(
        self: Arc<Self>,
        target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        let (old, new, blocker_description) =
            MockPdClient::advance_transaction_safe_point(&self, target)?;
        Ok(pdpb::AdvanceTxnSafePointResponse {
            old_txn_safe_point: old,
            new_txn_safe_point: new,
            blocker_description,
            ..Default::default()
        })
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        let (old, new) = MockPdClient::advance_gc_safe_point(&self, target)?;
        Ok(pdpb::AdvanceGcSafePointResponse {
            old_gc_safe_point: old,
            new_gc_safe_point: new,
            ..Default::default()
        })
    }

    async fn scatter_regions(
        self: Arc<Self>,
        _region_ids: Vec<u64>,
        _group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        Ok(pdpb::ScatterRegionResponse::default())
    }

    async fn get_operator(self: Arc<Self>, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        Ok(pdpb::GetOperatorResponse {
            status: pdpb::OperatorStatus::Success as i32,
            ..Default::default()
        })
    }

    async fn split_regions(
        self: Arc<Self>,
        _split_keys: Vec<Vec<u8>>,
        _retry_limit: u64,
    ) -> Result<Vec<u64>> {
        Ok(Vec::new())
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Err(Error::KeyspaceNotFound(keyspace.to_owned()))
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(self
            .cluster
            .all_stores()
            .into_iter()
            .map(|meta| {
                Store::new(Arc::new(self.rpc.clone()))
                    .with_target(meta.address.clone())
                    .with_metadata(&meta)
            })
            .collect())
    }

    async fn update_leader(
        &self,
        ver_id: crate::region::RegionVerId,
        leader: metapb::Peer,
    ) -> Result<()> {
        self.cluster.change_leader(ver_id.id, leader.id);
        Ok(())
    }

    async fn invalidate_region_cache(&self, _ver_id: crate::region::RegionVerId) {}

    async fn invalidate_store_cache(&self, _store_id: u64) {}
}

fn default_resource_group() -> resource_manager::ResourceGroup {
    resource_manager::ResourceGroup {
        name: DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
        mode: resource_manager::GroupMode::RuMode as i32,
        r_u_settings: Some(resource_manager::GroupRequestUnitSettings {
            r_u: Some(resource_manager::TokenBucket {
                settings: Some(resource_manager::TokenLimitSettings {
                    fill_rate: i32::MAX as u64,
                    burst_limit: -1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }),
        priority: 8,
        ..Default::default()
    }
}

fn decode_boundary(encoded: &[u8]) -> Result<Vec<u8>> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    let mut decoded = Vec::new();
    codec::decode_bytes(encoded, &mut decoded)?;
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::mocktikv::{bootstrap_with_multi_regions, bootstrap_with_single_store};

    #[test]
    fn source_tso_external_timestamp_and_gc_barriers() {
        let cluster = Cluster::new(unistore::MockEngine::new());
        bootstrap_with_single_store(&cluster);
        let pd = MockPdClient::new(cluster);
        let first = pd.get_timestamp_parts();
        let second = pd.get_timestamp_parts();
        assert!(second >= first);
        let other = MockPdClient::new(pd.cluster.clone());
        assert!(other.get_timestamp_parts() >= second);
        assert!(pd.set_external_timestamp_value(u64::MAX).is_err());

        let group = pd.resource_group(DEFAULT_RESOURCE_GROUP_NAME).unwrap();
        assert_eq!(group.name, DEFAULT_RESOURCE_GROUP_NAME);
        assert_eq!(group.mode, resource_manager::GroupMode::RuMode as i32);
        assert_eq!(group.priority, 8);
        assert_eq!(
            group
                .r_u_settings
                .unwrap()
                .r_u
                .unwrap()
                .settings
                .unwrap()
                .burst_limit,
            -1
        );
        assert!(pd.resource_group("missing").is_err());

        pd.set_gc_barrier("service", 10).unwrap();
        assert_eq!(pd.advance_transaction_safe_point(20).unwrap().1, 10);
        assert!(pd.advance_gc_safe_point(11).is_err());
        assert_eq!(pd.advance_gc_safe_point(10).unwrap(), (0, 10));
        assert_eq!(pd.gc_state().barriers[0].id, "service");
    }

    #[tokio::test]
    async fn source_get_prev_region_uses_the_region_before_the_end_key() {
        let cluster = Cluster::new(unistore::MockEngine::new());
        let (_, regions, _) = bootstrap_with_multi_regions(&cluster, &[b"m".to_vec()]);
        let pd = MockPdClient::new(cluster);
        let region = pd
            .region_for_end_key(&Key::from(b"m".to_vec()))
            .await
            .unwrap();
        assert_eq!(region.region.id, regions[0]);
    }
}
