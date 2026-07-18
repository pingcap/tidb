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

//! Direct transition of client-go `TestBuckets`, `TestLocateBucket`, and all
//! `TestBucketClampingToRegion` table rows.

use std::collections::VecDeque;

use tidb_txnkv::region::{
    Bucket, BucketMetadata, RegionCache, RegionEpoch, RegionLoadError, RegionLoader,
    RegionLocation, RegionVerId,
};

fn location(start: &str, end: &str, keys: &[&str]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId {
            id: 7,
            epoch: RegionEpoch {
                conf_ver: 1,
                version: 1,
            },
        },
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
        peers: Vec::new(),
        leader_peer_id: None,
        stores: Vec::new(),
        buckets: Some(BucketMetadata {
            region_id: 7,
            version: 9,
            keys: keys.iter().map(|key| key.as_bytes().to_vec()).collect(),
            stats: None,
            period_in_ms: 1_000,
        }),
        down_peer_ids: Vec::new(),
        pending_peer_ids: Vec::new(),
    }
}

#[test]
fn bucket_version_and_stale_holes_follow_source_boundaries() {
    let complete = location("", "", &["", "a", "b", ""]);
    assert_eq!(complete.bucket_version(), 9);
    for key in [b"".as_slice(), b"`", b"a", b"a0", b"b", b"c"] {
        let bucket = complete.locate_bucket(key).expect("key belongs to region");
        assert!(bucket.contains(key));
    }

    let stale = location("", "z", &["b", "c", "d"]);
    assert_eq!(
        stale.locate_bucket(b"a"),
        Some(Bucket {
            start_key: Vec::new(),
            end_key: b"b".to_vec(),
        })
    );
    assert_eq!(
        stale.locate_bucket(b"e"),
        Some(Bucket {
            start_key: b"d".to_vec(),
            end_key: b"z".to_vec(),
        })
    );
    assert_eq!(stale.locate_bucket(b"z"), None);
}

#[test]
fn bucket_clamping_ports_every_original_table_shape() {
    struct Case {
        region_start: &'static str,
        region_end: &'static str,
        bucket_start: &'static str,
        bucket_end: &'static str,
        expected_start: &'static str,
        expected_end: &'static str,
    }
    let cases = [
        Case {
            region_start: "a",
            region_end: "z",
            bucket_start: "f",
            bucket_end: "m",
            expected_start: "f",
            expected_end: "m",
        },
        Case {
            region_start: "f",
            region_end: "z",
            bucket_start: "a",
            bucket_end: "m",
            expected_start: "f",
            expected_end: "m",
        },
        Case {
            region_start: "a",
            region_end: "m",
            bucket_start: "f",
            bucket_end: "z",
            expected_start: "f",
            expected_end: "m",
        },
        Case {
            region_start: "f",
            region_end: "m",
            bucket_start: "a",
            bucket_end: "z",
            expected_start: "f",
            expected_end: "m",
        },
        Case {
            region_start: "",
            region_end: "m",
            bucket_start: "",
            bucket_end: "z",
            expected_start: "",
            expected_end: "m",
        },
        Case {
            region_start: "f",
            region_end: "",
            bucket_start: "a",
            bucket_end: "",
            expected_start: "f",
            expected_end: "",
        },
        Case {
            region_start: "f",
            region_end: "m",
            bucket_start: "a",
            bucket_end: "b",
            expected_start: "f",
            expected_end: "m",
        },
        Case {
            region_start: "f",
            region_end: "m",
            bucket_start: "",
            bucket_end: "h",
            expected_start: "f",
            expected_end: "h",
        },
        Case {
            region_start: "f",
            region_end: "m",
            bucket_start: "h",
            bucket_end: "",
            expected_start: "h",
            expected_end: "m",
        },
        Case {
            region_start: "",
            region_end: "m",
            bucket_start: "",
            bucket_end: "h",
            expected_start: "",
            expected_end: "h",
        },
        Case {
            region_start: "f",
            region_end: "",
            bucket_start: "h",
            bucket_end: "",
            expected_start: "h",
            expected_end: "",
        },
        Case {
            region_start: "",
            region_end: "",
            bucket_start: "",
            bucket_end: "",
            expected_start: "",
            expected_end: "",
        },
        Case {
            region_start: "a",
            region_end: "",
            bucket_start: "f",
            bucket_end: "",
            expected_start: "f",
            expected_end: "",
        },
    ];

    for case in cases {
        let location = location(
            case.region_start,
            case.region_end,
            &[case.bucket_start, case.bucket_end],
        );
        let probe = if case.expected_start.is_empty() {
            b"a".as_slice()
        } else {
            case.expected_start.as_bytes()
        };
        let actual = location
            .locate_bucket(probe)
            .expect("probe belongs to region");
        assert_eq!(actual.start_key, case.expected_start.as_bytes());
        assert_eq!(actual.end_key, case.expected_end.as_bytes());
    }
}

struct ScriptedLoader {
    regions: VecDeque<RegionLocation>,
}

impl RegionLoader for ScriptedLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.regions.pop_front().ok_or_else(|| {
            RegionLoadError::new("missing-scripted-region", "test loader was exhausted")
        })
    }
}

#[test]
fn cache_insertion_and_epoch_refresh_retain_only_newer_bucket_versions() {
    let mut cache = RegionCache::with_ttl(
        ScriptedLoader {
            regions: VecDeque::from([
                location_with_buckets(7, 1, "", "", Some((9, &["", "a", "b", ""]))),
                location_with_buckets(7, 2, "", "", None),
                location_with_buckets(7, 3, "", "", Some((8, &["", "a", "b", ""]))),
                location_with_buckets(7, 4, "", "", Some((10, &["", "a", ""]))),
                location_with_buckets(7, 5, "", "", None),
            ]),
        },
        600,
        0,
    );

    let mut region = cache.locate_key_at(b"a", 100).unwrap().region;
    assert_eq!(cache.locate_key_at(b"a", 100).unwrap().bucket_version(), 9);
    for expected in [9, 9, 10, 10] {
        assert!(cache.mark_reload_on_access(region));
        let refreshed = cache.locate_key_at(b"a", 100).unwrap();
        region = refreshed.region;
        assert_eq!(refreshed.bucket_version(), expected);
    }
    assert_eq!(region.epoch.version, 5);
}

#[test]
fn split_child_inherits_buckets_from_intersecting_parent_with_a_new_id() {
    let mut cache = RegionCache::with_ttl(
        ScriptedLoader {
            regions: VecDeque::from([
                location_with_buckets(1, 1, "a", "z", Some((11, &["a", "m", "z"]))),
                location_with_buckets(2, 2, "a", "m", None),
            ]),
        },
        600,
        0,
    );
    let parent = cache.locate_key_at(b"b", 100).unwrap().region;
    assert!(cache.mark_reload_on_access(parent));
    let child = cache.locate_key_at(b"b", 100).unwrap();
    assert_eq!(child.region.id, 2);
    assert_eq!(child.bucket_version(), 11);
}

#[test]
fn merge_inherits_buckets_from_first_intersected_cached_region() {
    let mut cache = RegionCache::with_ttl(
        ScriptedLoader {
            regions: VecDeque::from([
                location_with_buckets(1, 1, "a", "m", Some((7, &["a", "m"]))),
                location_with_buckets(2, 1, "m", "z", Some((12, &["m", "z"]))),
                location_with_buckets(3, 2, "a", "z", None),
            ]),
        },
        600,
        0,
    );
    let left = cache.locate_key_at(b"b", 100).unwrap().region;
    cache.locate_key_at(b"x", 100).unwrap();
    assert!(cache.mark_reload_on_access(left));

    let merged = cache.locate_key_at(b"b", 100).unwrap();
    assert_eq!(merged.region.id, 3);
    assert_eq!(merged.bucket_version(), 7);
}

fn location_with_buckets(
    id: u64,
    version: u64,
    start: &str,
    end: &str,
    buckets: Option<(u64, &[&str])>,
) -> RegionLocation {
    RegionLocation {
        region: RegionVerId {
            id,
            epoch: RegionEpoch {
                conf_ver: version,
                version,
            },
        },
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
        peers: Vec::new(),
        leader_peer_id: None,
        stores: Vec::new(),
        buckets: buckets.map(|(bucket_version, keys)| BucketMetadata {
            region_id: id,
            version: bucket_version,
            keys: keys.iter().map(|key| key.as_bytes().to_vec()).collect(),
            stats: None,
            period_in_ms: 1_000,
        }),
        down_peer_ids: Vec::new(),
        pending_peer_ids: Vec::new(),
    }
}
