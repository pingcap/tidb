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

//! Direct transition of client-go's bounded rotating region-cache GC.

use std::collections::VecDeque;

use tidb_txnkv::region::{RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionVerId};

struct Loader(VecDeque<RegionLocation>);

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.0
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-region", "GC script exhausted"))
    }
}

fn region(id: u64, start: &[u8], end: &[u8]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, 1, 1),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        ..RegionLocation::default()
    }
}

#[test]
fn gc_never_scans_more_than_limit_and_resumes_at_next_entry() {
    let mut cache = RegionCache::with_ttl(
        Loader(
            [
                region(1, b"", b"b"),
                region(2, b"b", b"d"),
                region(3, b"d", b""),
            ]
            .into(),
        ),
        10,
        0,
    );
    cache.locate_key_at(b"a", 0).unwrap();
    cache.locate_key_at(b"c", 0).unwrap();
    cache.locate_key_at(b"e", 0).unwrap();

    let first = cache.maintain_entries_bounded_at(11, 2);
    assert_eq!(first.scanned, 2);
    assert_eq!(first.expired, 2);
    assert!(first.has_more);
    assert_eq!(cache.len(), 1);

    let second = cache.maintain_entries_bounded_at(11, 2);
    assert_eq!(second.scanned, 1);
    assert_eq!(second.expired, 1);
    assert!(!second.has_more);
    assert!(cache.is_empty());
}
