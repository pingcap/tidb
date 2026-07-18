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

use tidb_txnkv::region::{Bucket, BucketMetadata, RegionEpoch, RegionLocation, RegionVerId};

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
