use crate::kv::codec;
use crate::proto::{errorpb, kvrpcpb};
use unistore::{IsolationLevel, MockEngine};

use super::cluster::{region_contains, Cluster};

#[derive(Clone)]
pub struct Session {
    cluster: Cluster,
    engine: MockEngine,
    store_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    raw_start_key: Vec<u8>,
    raw_end_key: Vec<u8>,
    isolation: IsolationLevel,
    resolved_locks: Vec<u64>,
}

impl Session {
    pub fn new(cluster: Cluster, engine: MockEngine, store_id: u64) -> Self {
        Self {
            cluster,
            engine,
            store_id,
            start_key: Vec::new(),
            end_key: Vec::new(),
            raw_start_key: Vec::new(),
            raw_end_key: Vec::new(),
            isolation: IsolationLevel::SnapshotIsolation,
            resolved_locks: Vec::new(),
        }
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation
    }

    pub fn engine(&self) -> MockEngine {
        self.engine.clone()
    }

    pub fn raw_start_key(&self) -> &[u8] {
        &self.raw_start_key
    }

    pub fn raw_end_key(&self) -> &[u8] {
        &self.raw_end_key
    }

    pub fn resolved_locks(&self) -> &[u64] {
        &self.resolved_locks
    }

    pub fn check_request_context(&mut self, context: &kvrpcpb::Context) -> Option<errorpb::Error> {
        if context
            .peer
            .as_ref()
            .is_some_and(|peer| peer.store_id != self.store_id)
        {
            return Some(errorpb::Error {
                message: "store not match".to_owned(),
                store_not_match: Some(errorpb::StoreNotMatch {
                    request_store_id: context.peer.as_ref().map_or(0, |peer| peer.store_id),
                    actual_store_id: self.store_id,
                }),
                ..Default::default()
            });
        }
        let Some((region, leader_id)) = self.cluster.region(context.region_id) else {
            return Some(errorpb::Error {
                message: "region not found".to_owned(),
                region_not_found: Some(errorpb::RegionNotFound {
                    region_id: context.region_id,
                }),
                ..Default::default()
            });
        };
        let store_peer = region
            .peers
            .iter()
            .find(|peer| peer.store_id == self.store_id)
            .cloned();
        let leader = region
            .peers
            .iter()
            .find(|peer| peer.id == leader_id)
            .cloned();
        let Some(store_peer) = store_peer else {
            return Some(errorpb::Error {
                message: "region not found".to_owned(),
                region_not_found: Some(errorpb::RegionNotFound {
                    region_id: context.region_id,
                }),
                ..Default::default()
            });
        };
        let Some(leader) = leader else {
            return Some(errorpb::Error {
                message: "no leader".to_owned(),
                not_leader: Some(errorpb::NotLeader {
                    region_id: context.region_id,
                    leader: None,
                }),
                ..Default::default()
            });
        };
        let tiflash = self
            .cluster
            .store(store_peer.store_id)
            .is_some_and(|store| {
                store.labels.iter().any(|label| {
                    label.key == "engine"
                        && matches!(label.value.as_str(), "tiflash" | "tiflash_mpp")
                })
            });
        if store_peer.id != leader.id && !tiflash {
            return Some(errorpb::Error {
                message: "not leader".to_owned(),
                not_leader: Some(errorpb::NotLeader {
                    region_id: context.region_id,
                    leader: Some(leader),
                }),
                ..Default::default()
            });
        }
        if region.region_epoch != context.region_epoch {
            let mut current_regions = vec![region.clone()];
            if let Some((next, _, _, _)) = self.cluster.region_by_key(&region.end_key) {
                current_regions.push(next);
            }
            return Some(errorpb::Error {
                message: "epoch not match".to_owned(),
                epoch_not_match: Some(errorpb::EpochNotMatch { current_regions }),
                ..Default::default()
            });
        }
        self.start_key = region.start_key;
        self.end_key = region.end_key;
        self.raw_start_key = decode_key(&self.start_key);
        self.raw_end_key = decode_key(&self.end_key);
        self.isolation = if context.isolation_level == kvrpcpb::IsolationLevel::Rc as i32 {
            IsolationLevel::ReadCommitted
        } else {
            IsolationLevel::SnapshotIsolation
        };
        self.resolved_locks.clone_from(&context.resolved_locks);
        None
    }

    pub fn check_request(
        &mut self,
        context: Option<&kvrpcpb::Context>,
        encoded_size: usize,
    ) -> Option<errorpb::Error> {
        let context = context.cloned().unwrap_or_default();
        if let Some(error) = self.check_request_context(&context) {
            return Some(error);
        }
        if encoded_size >= super::REQUEST_MAX_SIZE {
            return Some(errorpb::Error {
                raft_entry_too_large: Some(errorpb::RaftEntryTooLarge {
                    region_id: context.region_id,
                    entry_size: encoded_size as u64,
                }),
                ..Default::default()
            });
        }
        None
    }

    pub fn key_in_region(&self, key: &[u8]) -> bool {
        let mut encoded = Vec::new();
        codec::encode_bytes(&mut encoded, key);
        region_contains(&self.start_key, &self.end_key, &encoded)
    }

    pub fn raw_key_in_region(&self, key: &[u8]) -> bool {
        region_contains(&self.start_key, &self.end_key, key)
    }

    pub fn raw_end_key_in_region(&self, end: &[u8]) -> bool {
        if end.is_empty() {
            return self.end_key.is_empty();
        }
        self.start_key.as_slice() < end
            && (self.end_key.is_empty() || end <= self.end_key.as_slice())
    }
}

fn decode_key(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return Vec::new();
    }
    let mut raw = Vec::new();
    codec::decode_bytes(key, &mut raw).expect("cluster key must be memcomparable");
    raw
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::mocktikv::bootstrap_with_single_store;

    #[test]
    fn request_context_reports_store_leader_and_epoch_errors() {
        let engine = MockEngine::new();
        let cluster = Cluster::new(engine.clone());
        let (store, peer, region) = bootstrap_with_single_store(&cluster);
        let (meta, _) = cluster.region(region).unwrap();
        let mut session = Session::new(cluster, engine, store);
        let mut context = kvrpcpb::Context {
            region_id: region,
            region_epoch: meta.region_epoch,
            peer: Some(crate::proto::metapb::Peer {
                id: peer,
                store_id: store,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(session.check_request_context(&context).is_none());
        context.peer.as_mut().unwrap().store_id += 1;
        assert!(session
            .check_request_context(&context)
            .unwrap()
            .store_not_match
            .is_some());
    }
}
