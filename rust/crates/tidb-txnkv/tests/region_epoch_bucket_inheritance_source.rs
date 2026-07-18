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

//! Exact-observation bucket inheritance for EpochNotMatch replacement.

use std::collections::VecDeque;

use tidb_codec::encode_bytes;
use tidb_proto::{errorpb, metapb};
use tidb_txnkv::region::{
    BucketMetadata, Peer, PeerRole, RegionAttempt, RegionBackoffBudget, RegionCache,
    RegionLoadError, RegionLoader, RegionLocation, RegionMetadata, RegionRecoveryLoader,
    RegionVerId, Store,
};

struct Loader {
    initial: VecDeque<RegionLocation>,
    hydrated: VecDeque<RegionLocation>,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.initial
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-initial", "script exhausted"))
    }
}

impl RegionRecoveryLoader for Loader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        _leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        let loaded = self
            .hydrated
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-hydrated", "script exhausted"))?;
        assert_eq!(loaded.region, metadata.region);
        Ok(loaded)
    }
}

fn location(
    id: u64,
    version: u64,
    start: &[u8],
    end: &[u8],
    bucket_version: Option<u64>,
) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, version, version),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: vec![Peer {
            id: id * 10 + 1,
            store_id: 101,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: 0,
        }],
        leader_peer_id: Some(id * 10 + 1),
        stores: vec![Store {
            id: 101,
            address: "store-101".to_owned(),
            epoch: 0,
        }],
        buckets: bucket_version.map(|version| BucketMetadata {
            region_id: id,
            version,
            keys: vec![start.to_vec(), end.to_vec()],
            stats: None,
            period_in_ms: 1_000,
        }),
        ..RegionLocation::default()
    }
}

fn current(location: &RegionLocation) -> metapb::Region {
    metapb::Region {
        id: location.region.id,
        start_key: encoded_boundary(&location.start_key),
        end_key: encoded_boundary(&location.end_key),
        region_epoch: Some(metapb::RegionEpoch {
            conf_ver: location.region.epoch.conf_ver,
            version: location.region.epoch.version,
        }),
        peers: vec![metapb::Peer {
            id: location.region.id * 10 + 1,
            store_id: 101,
            role: 0,
            is_witness: false,
        }],
    }
}

fn encoded_boundary(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return Vec::new();
    }
    let mut encoded = Vec::new();
    encode_bytes(&mut encoded, key);
    encoded
}

fn attempt(location: &RegionLocation) -> RegionAttempt {
    RegionAttempt {
        region: location.region,
        peer_id: location.region.id * 10 + 1,
        store_id: 101,
        address: "store-101".to_owned(),
        store_epoch: 0,
    }
}

#[test]
fn split_children_both_inherit_the_exact_observed_parent_buckets() {
    let parent = location(1, 1, b"a", b"z", Some(11));
    let left = location(2, 2, b"a", b"m", None);
    let right = location(3, 2, b"m", b"z", Some(99));
    let mut cache = RegionCache::new(Loader {
        initial: VecDeque::from([parent.clone()]),
        hydrated: VecDeque::from([left.clone(), right.clone()]),
    });
    cache.locate_key(b"b").unwrap();
    let error = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![current(&left), current(&right)],
        }),
        ..Default::default()
    };
    cache
        .on_region_error(
            &error,
            attempt(&parent),
            &mut RegionBackoffBudget::campaign_default(),
        )
        .unwrap();

    assert_eq!(cache.locate_key(b"b").unwrap().bucket_version(), 11);
    assert_eq!(cache.locate_key(b"x").unwrap().bucket_version(), 11);
}

#[test]
fn observed_right_merge_does_not_inherit_from_first_intersecting_left_sibling() {
    let left = location(1, 1, b"a", b"m", Some(22));
    let right = location(2, 1, b"m", b"z", Some(11));
    let merged = location(3, 2, b"a", b"z", None);
    let mut cache = RegionCache::new(Loader {
        initial: VecDeque::from([left, right.clone()]),
        hydrated: VecDeque::from([merged.clone()]),
    });
    cache.locate_key(b"b").unwrap();
    cache.locate_key(b"x").unwrap();
    let error = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![current(&merged)],
        }),
        ..Default::default()
    };
    cache
        .on_region_error(
            &error,
            attempt(&right),
            &mut RegionBackoffBudget::campaign_default(),
        )
        .unwrap();

    assert_eq!(cache.locate_key(b"x").unwrap().bucket_version(), 11);
}
