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

//! Direct transition of ContainsByEnd and ListRegionIDsInCache boundaries.

use tidb_txnkv::region::{RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionVerId};

fn region(id: u64, start: &[u8], end: &[u8]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, 1, 1),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        ..RegionLocation::default()
    }
}

type EndKeyProbe<'a> = (&'a [u8], bool);
type EndKeyCase<'a> = (&'a [u8], &'a [u8], &'a [EndKeyProbe<'a>]);

#[test]
fn contains_by_end_ports_the_original_table() {
    let cases: &[EndKeyCase<'_>] = &[
        (b"", b"", &[(b"", true), (b"10", true)]),
        (b"10", b"", &[(b"", true), (b"10", false), (b"11", true)]),
        (b"", b"10", &[(b"", false), (b"10", true), (b"11", false)]),
        (b"10", b"20", &[(b"", false), (b"15", true), (b"30", false)]),
    ];
    for (start, end, probes) in cases {
        let location = region(1, start, end);
        for (key, expected) in *probes {
            assert_eq!(location.contains_end_key(key), *expected);
        }
    }
}

struct Loader {
    regions: Vec<RegionLocation>,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.regions
            .iter()
            .find(|region| region.contains_key(key))
            .cloned()
            .ok_or_else(|| RegionLoadError::new("missing-region", "ordinary key is uncovered"))
    }

    fn load_region_by_end_key(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.regions
            .iter()
            .find(|region| region.contains_end_key(key))
            .cloned()
            .ok_or_else(|| RegionLoadError::new("missing-region", "end key is uncovered"))
    }
}

#[test]
fn cache_end_lookup_and_region_id_listing_preserve_boundary_ownership() {
    let left = region(1, b"", b"m");
    let right = region(2, b"m", b"");
    let mut cache = RegionCache::with_ttl(
        Loader {
            regions: vec![left.clone(), right.clone()],
        },
        600,
        0,
    );

    assert_eq!(cache.locate_end_key(b"m").unwrap().region, left.region);
    assert_eq!(cache.locate_key(b"m").unwrap().region, right.region);
    assert_eq!(cache.list_region_ids(b"a", b"z").unwrap(), [1, 2]);
    assert_eq!(cache.list_region_ids(b"m", b"z").unwrap(), [2]);
    assert_eq!(cache.list_region_ids(b"a", b"m").unwrap(), [1, 2]);
}
