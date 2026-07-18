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

//! Direct transition of client-go RegionCache TTL and delayed-reload flags.

use tidb_txnkv::region::{
    CacheEntryState, CacheReloadState, RegionCache, RegionEpoch, RegionLoadError, RegionLoader,
    RegionLocation, RegionVerId,
};

struct Loader {
    loads: usize,
    down: bool,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.loads += 1;
        Ok(RegionLocation {
            region: RegionVerId {
                id: 1,
                epoch: RegionEpoch {
                    conf_ver: self.loads as u64,
                    version: self.loads as u64,
                },
            },
            start_key: Vec::new(),
            end_key: Vec::new(),
            peers: Vec::new(),
            leader_peer_id: None,
            stores: Vec::new(),
            buckets: None,
            down_peer_ids: self.down.then_some(9).into_iter().collect(),
            pending_peer_ids: Vec::new(),
        })
    }
}

#[test]
fn strict_expiry_and_near_boundary_renewal_match_go() {
    let mut state = CacheEntryState::new(102);
    assert!(state.check_and_renew(102, 2, 104));
    assert_eq!(state.expires_at_seconds(), 104);
    assert!(!state.check_and_renew(105, 2, 107));

    let mut fixed = CacheEntryState::new(102);
    fixed.mark(CacheReloadState::ExpireAfterTtl);
    assert!(fixed.check_and_renew(102, 2, 104));
    assert_eq!(fixed.expires_at_seconds(), 102);
    assert!(!fixed.check_and_renew(103, 2, 105));
}

#[test]
fn source_reload_flags_do_not_collapse_into_one_boolean() {
    let mut delayed = CacheEntryState::new(200);
    delayed.mark(CacheReloadState::DelayedReloadPending);
    assert!(delayed.check_and_renew(100, 10, 110));
    assert!(delayed.release_delayed_reload());
    assert!(!delayed.check_and_renew(100, 10, 110));

    let mut immediate = CacheEntryState::new(200);
    immediate.mark(CacheReloadState::ReloadOnAccess);
    assert!(!immediate.check_and_renew(100, 10, 110));
}

#[test]
fn down_peer_entry_expires_even_under_continuous_access() {
    let mut cache = RegionCache::with_ttl(
        Loader {
            loads: 0,
            down: true,
        },
        2,
        0,
    );
    let first = cache.locate_key_at(b"a", 100).unwrap().region;
    assert_eq!(first.epoch.version, 1);
    assert_eq!(cache.locate_key_at(b"a", 102).unwrap().region, first);
    let reloaded = cache.locate_key_at(b"a", 103).unwrap().region;
    assert_eq!(reloaded.epoch.version, 2);
}
