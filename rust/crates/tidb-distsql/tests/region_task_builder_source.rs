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

//! Direct cases from `pkg/store/copr/coprocessor_test.go` for the connected
//! request -> region/bucket task -> protobuf-envelope path.

use std::collections::BTreeMap;

use prost::Message;
use tidb_distsql::{
    KvRequestBuilder, KvRequestMetadata, ReadBytesEma, RegionTaskEnvelope,
    RegionTaskPeer, RegionTaskTopology, RequestKeyRange, RequestKeyRanges, StoreType,
    TransportBinding, TransportRequest,
};

fn transport_request(metadata: KvRequestMetadata) -> TransportRequest {
    TransportRequest::new(
        metadata,
        std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
    )
}
use tidb_proto::{CoprocessorRequest, StoreBatchTask};

const COP_SMALL_TASK_ROW: usize = 32;

type KeyPair<'a> = (&'a str, &'a str);
type ExpectedRegionTask<'a> = (u64, &'a [KeyPair<'a>]);
type RegionTaskCase<'a> = (&'a [&'a str], &'a [ExpectedRegionTask<'a>]);
type RegionBatchCase<'a> = (&'a [&'a str], isize, &'a [&'a [KeyPair<'a>]]);
type RegionPagingCase<'a> = (&'a [&'a str], isize, &'a [KeyPair<'a>]);

fn kr(start: &str, end: &str) -> RequestKeyRange {
    RequestKeyRange {
        start_key: start.as_bytes().to_vec().into(),
        end_key: end.as_bytes().to_vec().into(),
    }
}

fn ranges(keys: &[&str]) -> Vec<RequestKeyRange> {
    assert_eq!(keys.len() % 2, 0);
    keys.chunks_exact(2)
        .map(|pair| kr(pair[0], pair[1]))
        .collect()
}

fn request(keys: &[&str], hints: Option<Vec<usize>>) -> TransportRequest {
    let key_ranges = hints.map_or_else(
        || RequestKeyRanges::new_non_partitioned(ranges(keys)),
        |hints| RequestKeyRanges::new_non_partitioned_with_hints(ranges(keys), hints),
    );
    let mut builder = KvRequestBuilder::new();
    builder.set_key_ranges(key_ranges);
    transport_request(builder.build().expect("built request"))
}

fn topology(boundaries: &[&str]) -> Vec<RegionTaskTopology> {
    boundaries
        .windows(2)
        .enumerate()
        .map(|(index, boundary)| RegionTaskTopology {
            region_id: u64::try_from(index + 1).unwrap(),
            peer: Some(RegionTaskPeer {
                id: u64::try_from(index + 101).unwrap(),
                store_id: 1,
                ..Default::default()
            }),
            start_key: boundary[0].as_bytes().to_vec(),
            end_key: boundary[1].as_bytes().to_vec(),
            ..Default::default()
        })
        .collect()
}

fn bucket_region(region_id: u64, bucket_version: u64, boundaries: &[&str]) -> RegionTaskTopology {
    assert!(boundaries.len() >= 2);
    bucket_region_with_raw(
        region_id,
        bucket_version,
        boundaries[0],
        boundaries[boundaries.len() - 1],
        &boundaries[1..boundaries.len() - 1],
    )
}

fn bucket_region_with_raw(
    region_id: u64,
    bucket_version: u64,
    start: &str,
    end: &str,
    raw_bucket_keys: &[&str],
) -> RegionTaskTopology {
    RegionTaskTopology {
        region_id,
        peer: Some(RegionTaskPeer {
            id: region_id + 100,
            store_id: 1,
            ..Default::default()
        }),
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
        bucket_keys: raw_bucket_keys
            .iter()
            .map(|key| key.as_bytes().to_vec())
            .collect(),
        buckets_version: bucket_version,
        ..Default::default()
    }
}

fn task_ranges(task: &RegionTaskEnvelope) -> Vec<(String, String)> {
    task.ranges
        .iter()
        .map(|range| {
            (
                String::from_utf8(range.start_key.to_vec()).unwrap(),
                String::from_utf8(range.end_key.to_vec()).unwrap(),
            )
        })
        .collect()
}

fn assert_tasks(tasks: &[RegionTaskEnvelope], expected: &[ExpectedRegionTask<'_>]) {
    assert_eq!(tasks.len(), expected.len());
    for (task, (region_id, expected_ranges)) in tasks.iter().zip(expected) {
        assert_eq!(task.region_id, *region_id);
        assert_eq!(
            task_ranges(task),
            expected_ranges
                .iter()
                .map(|(start, end)| ((*start).to_owned(), (*end).to_owned()))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn ensure_monotonic_key_ranges_reorders_only_invalid_input() {
    // TestEnsureMonotonicKeyRanges: the production builder owns the reorder;
    // the sorted result is observable in the first task and invalidates hints.
    let topo = topology(&["", ""]);
    let reordered = request(&["b", "d", "a", "b"], Some(vec![7, 9]))
        .build_region_tasks(&topo)
        .unwrap();
    assert_tasks(&reordered, &[(1, &[("a", "b"), ("b", "d")])]);
    assert_eq!(reordered[0].row_count_hint, -1);

    let sorted = request(&["a", "b", "b", "c"], Some(vec![7, 9]))
        .build_region_tasks(&topo)
        .unwrap();
    assert_tasks(&sorted, &[(1, &[("a", "b"), ("b", "c")])]);
    assert_eq!(sorted[0].row_count_hint, 16);
}

#[test]
fn build_tasks_without_buckets_matches_every_original_range_case() {
    let topo = topology(&["", "g", "n", "t", ""]);
    let cases: &[RegionTaskCase<'_>] = &[
        (&["a", "c"], &[(1, &[("a", "c")])]),
        (&["g", "n"], &[(2, &[("g", "n")])]),
        (&["m", "n"], &[(2, &[("m", "n")])]),
        (&["a", "k"], &[(1, &[("a", "g")]), (2, &[("g", "k")])]),
        (
            &["a", "x"],
            &[
                (1, &[("a", "g")]),
                (2, &[("g", "n")]),
                (3, &[("n", "t")]),
                (4, &[("t", "x")]),
            ],
        ),
        (&["a", "b", "b", "c"], &[(1, &[("a", "b"), ("b", "c")])]),
        (&["a", "b", "e", "f"], &[(1, &[("a", "b"), ("e", "f")])]),
        (
            &["g", "n", "o", "p"],
            &[(2, &[("g", "n")]), (3, &[("o", "p")])],
        ),
        (
            &["h", "k", "m", "p"],
            &[(2, &[("h", "k"), ("m", "n")]), (3, &[("n", "p")])],
        ),
    ];
    for (keys, expected) in cases {
        let tikv = request(keys, None).build_region_tasks(&topo).unwrap();
        assert_tasks(&tikv, expected);

        let mut tiflash_request = request(keys, None);
        let mut metadata = tiflash_request.metadata().clone();
        metadata.store_type = StoreType::TiFlash;
        tiflash_request = transport_request(metadata);
        let tiflash = tiflash_request.build_region_tasks(&topo).unwrap();
        assert_tasks(&tiflash, expected);
    }

    let bound_request = request(&["a", "k"], None)
        .bind(TransportBinding::new())
        .unwrap();
    let tasks = bound_request.build_region_tasks(&topo).unwrap();
    let encoded = bound_request.encode_region_task_request(&tasks[0]).unwrap();
    let decoded = CoprocessorRequest::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded.ranges.len(), 1);
    assert_eq!(decoded.ranges[0].start, b"a");
    assert_eq!(decoded.ranges[0].end, b"g");

    // A checked snapshot may omit unrelated keyspace. Coverage is required
    // only for requested ranges, not as a global region-contiguity rule.
    let sparse = vec![
        topology(&["", "g"])[0].clone(),
        topology(&["n", ""])[0].clone(),
    ];
    assert!(request(&["a", "c", "t", "z"], None)
        .build_region_tasks(&sparse)
        .is_ok());
    assert!(request(&["a", "z"], None)
        .build_region_tasks(&sparse)
        .is_err());
}

#[test]
fn build_tasks_by_buckets_matches_original_tables() {
    let mut topo = vec![
        bucket_region(1, 1, &["", "c", "g", "k", "n"]),
        bucket_region(2, 2, &["n", "t", "x"]),
        bucket_region(3, 3, &["x", ""]),
    ];
    let one_per_bucket = request(
        &[
            "a", "b", "c", "d", "h", "i", "k", "n", "o", "p", "u", "x", "x", "",
        ],
        None,
    )
    .build_region_tasks(&topo)
    .unwrap();
    assert_eq!(one_per_bucket.len(), 7);
    assert_eq!(
        one_per_bucket
            .iter()
            .map(|task| task.region_id)
            .collect::<Vec<_>>(),
        vec![1, 1, 1, 1, 2, 2, 3]
    );

    let grouped = request(
        &[
            "", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "k", "l", "m", "n",
        ],
        None,
    )
    .build_region_tasks(&topo)
    .unwrap();
    assert_tasks(
        &grouped,
        &[
            (1, &[("", "a"), ("b", "c")]),
            (1, &[("d", "e"), ("f", "g")]),
            (1, &[("h", "i"), ("j", "k")]),
            (1, &[("k", "l"), ("m", "n")]),
        ],
    );

    let crossing = request(&["", "d", "e", "h", "i", "j"], None)
        .build_region_tasks(&topo)
        .unwrap();
    assert_tasks(
        &crossing,
        &[
            (1, &[("", "c")]),
            (1, &[("c", "d"), ("e", "g")]),
            (1, &[("g", "h"), ("i", "j")]),
        ],
    );

    topo[1] = bucket_region(2, 2, &["n", "q", "r", "t", "u", "v", "x"]);
    let many = request(&["n", "o", "p", "q", "s", "w"], None)
        .build_region_tasks(&topo)
        .unwrap();
    assert_tasks(
        &many,
        &[
            (2, &[("n", "o"), ("p", "q")]),
            (2, &[("s", "t")]),
            (2, &[("t", "u")]),
            (2, &[("u", "v")]),
            (2, &[("v", "w")]),
        ],
    );

    topo[1] = bucket_region(2, 2, &["n", "q", "s", "u", "x"]);
    let outside = request(&["n", "o", "p", "s", "t", "v", "w", "x"], None)
        .build_region_tasks(&topo)
        .unwrap();
    assert_tasks(
        &outside,
        &[
            (2, &[("n", "o"), ("p", "q")]),
            (2, &[("q", "s")]),
            (2, &[("t", "u")]),
            (2, &[("u", "v"), ("w", "x")]),
        ],
    );

    // Source bucket metadata `g,t,z` extends outside region `n,x`; the
    // region-cache boundary normalizes it to `n,t,x` before task splitting.
    topo[1] = bucket_region_with_raw(2, 2, "n", "x", &["g", "t", "z"]);
    let outside_region = request(&["o", "p", "u", "w"], None)
        .build_region_tasks(&topo)
        .unwrap();
    assert_tasks(&outside_region, &[(2, &[("o", "p")]), (2, &[("u", "w")])]);

    topo[1] = bucket_region(2, 2, &["n", "q", "r", "x"]);
    let whole = request(&["n", "x"], None)
        .build_region_tasks(&topo)
        .unwrap();
    assert_tasks(
        &whole,
        &[(2, &[("n", "q")]), (2, &[("q", "r")]), (2, &[("r", "x")])],
    );
}

#[test]
fn split_key_ranges_by_locations_without_buckets_matches_original_table() {
    let topo = topology(&["", "g", "n", "t", ""]);
    let cases: &[RegionBatchCase<'_>] = &[
        (&["a", "c"], -1, &[&[("a", "c")]]),
        (&["a", "c"], 0, &[]),
        (
            &["h", "y"],
            -1,
            &[&[("h", "n")], &[("n", "t")], &[("t", "y")]],
        ),
        (&["h", "n"], -1, &[&[("h", "n")]]),
        (&["s", "s"], -1, &[&[("s", "s")]]),
        (
            &["a", "z"],
            -1,
            &[&[("a", "g")], &[("g", "n")], &[("n", "t")], &[("t", "z")]],
        ),
        (
            &["a", "z"],
            3,
            &[&[("a", "g")], &[("g", "n")], &[("n", "t")]],
        ),
        (
            &[
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
                "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
            ],
            -1,
            &[
                &[("a", "b"), ("c", "d"), ("e", "f")],
                &[("g", "h"), ("i", "j"), ("k", "l"), ("m", "n")],
                &[("o", "p"), ("q", "r"), ("s", "t")],
                &[("u", "v"), ("w", "x"), ("y", "z")],
            ],
        ),
        (
            &["a", "b", "b", "h", "h", "m", "n", "t", "v", "w"],
            -1,
            &[
                &[("a", "b"), ("b", "g")],
                &[("g", "h"), ("h", "m")],
                &[("n", "t")],
                &[("v", "w")],
            ],
        ),
        (&["a", "b", "v", "w"], -1, &[&[("a", "b")], &[("v", "w")]]),
    ];
    for (keys, limit, expected) in cases {
        let actual = request(keys, None)
            .split_key_ranges_by_regions(&topo, *limit)
            .unwrap();
        assert_eq!(actual.len(), expected.len(), "{keys:?}");
        for (actual, expected) in actual.iter().zip(*expected) {
            assert_eq!(
                actual
                    .iter()
                    .map(|range| (
                        std::str::from_utf8(&range.start_key).unwrap(),
                        std::str::from_utf8(&range.end_key).unwrap()
                    ))
                    .collect::<Vec<_>>(),
                *expected,
                "{keys:?}"
            );
        }
    }
}

#[test]
fn split_region_ranges_matches_original_table() {
    let topo = topology(&["", "g", "n", "t", ""]);
    let cases: &[RegionPagingCase<'_>] = &[
        (&["a", "c"], -1, &[("a", "c")]),
        (&["a", "c"], 0, &[]),
        (&["h", "y"], -1, &[("h", "n"), ("n", "t"), ("t", "y")]),
        (&["s", "z"], -1, &[("s", "t"), ("t", "z")]),
        (&["s", "s"], -1, &[("s", "s")]),
        (&["t", "t"], -1, &[("t", "t")]),
        (&["t", "u"], -1, &[("t", "u")]),
        (&["u", "z"], -1, &[("u", "z")]),
        (
            &["a", "z"],
            -1,
            &[("a", "g"), ("g", "n"), ("n", "t"), ("t", "z")],
        ),
        (&["a", "z"], 3, &[("a", "g"), ("g", "n"), ("n", "t")]),
    ];
    for (keys, limit, expected) in cases {
        let actual = request(keys, None)
            .split_region_ranges(&topo, *limit)
            .unwrap();
        assert_eq!(
            actual
                .iter()
                .map(|range| (
                    std::str::from_utf8(&range.start_key).unwrap(),
                    std::str::from_utf8(&range.end_key).unwrap()
                ))
                .collect::<Vec<_>>(),
            *expected,
            "{keys:?}"
        );
    }
}

#[test]
fn rebuild_consumes_refreshed_topology_and_reverses_descending_tasks() {
    let request = request(&["a", "z"], None);
    let initial = request
        .build_region_tasks(&topology(&["", "m", ""]))
        .unwrap();
    assert_tasks(&initial, &[(1, &[("a", "m")]), (2, &[("m", "z")])]);

    let mut metadata = request.metadata().clone();
    metadata.desc = true;
    let rebuilt = transport_request(metadata)
        .build_region_tasks(&topology(&["", "m", "q", ""]))
        .unwrap();
    assert_tasks(
        &rebuilt,
        &[(3, &[("q", "z")]), (2, &[("m", "q")]), (1, &[("a", "m")])],
    );
}

#[test]
fn build_paging_tasks_preserves_minimum_page_size() {
    let mut metadata = request(&["a", "c"], None).metadata().clone();
    metadata.paging.enabled = true;
    let tasks = transport_request(metadata)
        .build_region_tasks(&topology(&["", "g", "n", "t", ""]))
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert!(tasks[0].paging);
    assert_eq!(tasks[0].paging_size, 128);
}

#[test]
fn task_build_does_not_mutate_or_cancel_request_state() {
    let mut metadata = request(&["a", "c"], None).metadata().clone();
    metadata.paging.enabled = true;
    metadata.max_execution_time_ms = 10_000;
    let request = transport_request(metadata.clone());
    let _ = request
        .build_region_tasks(&topology(&["", "g", "n", "t", ""]))
        .unwrap();
    assert_eq!(request.metadata().max_execution_time_ms, 10_000);
    assert_eq!(request.metadata().paging, metadata.paging);
}

#[test]
fn small_limit_disables_row_paging_without_byte_prediction() {
    let mut metadata = request(&["a", "c"], None).metadata().clone();
    metadata.paging.enabled = true;
    metadata.limit_size = 1;
    let tasks = transport_request(metadata)
        .build_region_tasks(&topology(&["", "g", "n", "t", ""]))
        .unwrap();
    assert!(!tasks[0].paging);
    assert_eq!(tasks[0].paging_size, 0);
    assert_eq!(tasks[0].response_channel_capacity, 0);
    let ema = ReadBytesEma::new(1_048_576);
    assert_eq!(tasks[0].predicted_read_bytes(0, &ema), 0);
}

#[test]
fn byte_paging_budget_enlarges_channel_and_survives_row_paging_downgrade() {
    let mut metadata = request(&["a", "c"], None).metadata().clone();
    metadata.keep_order = true;
    metadata.paging.size_bytes = 4 * 1024 * 1024;
    let tasks = transport_request(metadata.clone())
        .build_region_tasks(&topology(&["", "g", "n", "t", ""]))
        .unwrap();
    assert!(!tasks[0].paging);
    assert_eq!(tasks[0].paging_size, 0);
    assert_eq!(tasks[0].response_channel_capacity, 18);
    let ema = ReadBytesEma::new(4 * 1024 * 1024);
    assert_eq!(
        tasks[0].predicted_read_bytes(metadata.paging.size_bytes, &ema),
        4 * 1024 * 1024
    );

    metadata.paging.enabled = true;
    metadata.limit_size = 1;
    let request = transport_request(metadata);
    let tasks = request
        .build_region_tasks(&topology(&["", "g", "n", "t", ""]))
        .unwrap();
    assert!(!tasks[0].paging);
    assert_eq!(tasks[0].response_channel_capacity, 18);
    assert_eq!(
        request.metadata().paging.size_bytes,
        4 * 1024 * 1024
    );
    assert_eq!(
        tasks[0].predicted_read_bytes(request.metadata().paging.size_bytes, &ema),
        4 * 1024 * 1024
    );
}

#[test]
fn row_count_hints_follow_every_original_split_and_concurrency_case() {
    let topo = topology(&["", "g", "n", "t", ""]);
    let keys = &["a", "c", "d", "e", "h", "x", "y", "z"];
    let tasks = request(keys, Some(vec![1, 1, 3, COP_SMALL_TASK_ROW]))
        .build_region_tasks(&topo)
        .unwrap();
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.row_count_hint)
            .collect::<Vec<_>>(),
        vec![2, 3, 3, 35]
    );
    assert_eq!(
        RegionTaskEnvelope::small_task_concurrency(&tasks, 16),
        (3, 1)
    );

    let tasks = request(keys, Some(vec![1, 1, 3, 3]))
        .build_region_tasks(&topo)
        .unwrap();
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.row_count_hint)
            .collect::<Vec<_>>(),
        vec![2, 3, 3, 6]
    );
    assert_eq!(
        RegionTaskEnvelope::small_task_concurrency(&tasks, 16),
        (4, 2)
    );

    let tasks = request(&["a", "z"], Some(vec![10]))
        .build_region_tasks(&topo)
        .unwrap();
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.row_count_hint)
            .collect::<Vec<_>>(),
        vec![10, 10, 10, 10]
    );
}

#[test]
fn small_task_predicate_and_per_core_limit_match_source() {
    for (hint, expected_count) in [(-1, 0), (0, 0), (1, 1), (6, 1), (32, 1), (33, 0)] {
        let task = RegionTaskEnvelope {
            row_count_hint: hint,
            ..Default::default()
        };
        assert_eq!(
            RegionTaskEnvelope::small_task_concurrency(&[task], 16).0,
            expected_count
        );
    }
    assert_eq!(RegionTaskEnvelope::small_task_concurrency(&[], 16), (0, 0));
    let tasks = (0..1000)
        .map(|_| RegionTaskEnvelope {
            row_count_hint: 1,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        RegionTaskEnvelope::small_task_concurrency(&tasks, 1),
        (1000, 20)
    );
    assert_eq!(
        RegionTaskEnvelope::small_task_concurrency(&tasks, 0),
        (1000, 20)
    );

    // Preserve Go's exact operator grouping:
    // (positive && no children && <= 32) || (has children && <= 64).
    for hint in [-1, 0, 64] {
        let batched = RegionTaskEnvelope {
            row_count_hint: hint,
            batch_task_list: vec![RegionTaskEnvelope::default()],
            ..Default::default()
        };
        assert_eq!(
            RegionTaskEnvelope::small_task_concurrency(&[batched], 16).0,
            1,
            "batched hint {hint}"
        );
    }
    let too_large = RegionTaskEnvelope {
        row_count_hint: 65,
        batch_task_list: vec![RegionTaskEnvelope::default()],
        ..Default::default()
    };
    assert_eq!(
        RegionTaskEnvelope::small_task_concurrency(&[too_large], 16).0,
        0
    );
}

#[test]
fn store_batching_requires_a_leader_selected_peer() {
    let mut topo = topology(&["", "g", "n", "t", ""]);
    let mut metadata = request(
        &["a", "c", "d", "e", "h", "x", "y", "z"],
        Some(vec![1, 1, 3, 3]),
    )
    .metadata()
    .clone();
    metadata.store_batch_size = 3;
    metadata.store_busy_threshold_ns = 1_000_000_000;

    let batched = transport_request(metadata.clone())
        .build_region_tasks(&topo)
        .unwrap();
    assert_eq!(batched.len(), 1);
    assert_eq!(batched[0].batch_task_list.len(), 3);
    assert_eq!(batched[0].store_busy_threshold_ms, 0);
    assert!(batched[0]
        .batch_task_list
        .iter()
        .all(|task| task.store_busy_threshold_ms == 0));

    for region in &mut topo {
        region.store_batch_eligible = false;
    }
    let unbatched = transport_request(metadata)
        .build_region_tasks(&topo)
        .unwrap();
    assert_eq!(unbatched.len(), 4);
    assert!(unbatched.iter().all(|task| task.batch_task_list.is_empty()));
    assert!(unbatched
        .iter()
        .all(|task| task.store_busy_threshold_ms == 1_000));
}

#[test]
fn store_batch_children_preserve_each_region_bucket_version_on_wire() {
    let topo = vec![
        bucket_region(1, 101, &["", "n"]),
        bucket_region(2, 202, &["n", "x"]),
        bucket_region(3, 303, &["x", ""]),
    ];
    let mut metadata = request(&["a", "b", "o", "p", "y", "z"], Some(vec![1, 1, 1]))
        .metadata()
        .clone();
    metadata.store_batch_size = 3;
    let tasks = transport_request(metadata)
        .build_region_tasks(&topo)
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].batch_task_list.len(), 2);

    let mut versions = BTreeMap::new();
    for encoded in tasks[0].encode_batch_tasks() {
        let task = StoreBatchTask::decode(encoded.as_slice()).unwrap();
        versions.insert(task.region_id, task.buckets_version);
    }
    assert_eq!(versions, BTreeMap::from([(2, 202), (3, 303)]));

    // Go assigns IDs in ascending region order, batches only small tasks by
    // selected store even when same-store regions are nonconsecutive, and
    // reverses only the completed parent list for Desc.
    let mut sparse_stores = topo.clone();
    sparse_stores[1].peer.as_mut().unwrap().store_id = 2;
    let mut metadata = request(&["a", "b", "o", "p", "y", "z"], Some(vec![1, 40, 1]))
        .metadata()
        .clone();
    metadata.store_batch_size = 3;
    metadata.desc = true;
    let tasks = transport_request(metadata)
        .build_region_tasks(&sparse_stores)
        .unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].region_id, 2);
    assert_eq!(tasks[0].task_id, 2);
    assert!(tasks[0].batch_task_list.is_empty());
    assert_eq!(tasks[1].region_id, 1);
    assert_eq!(tasks[1].task_id, 1);
    assert_eq!(tasks[1].batch_task_list.len(), 1);
    assert_eq!(tasks[1].batch_task_list[0].region_id, 3);
    assert_eq!(tasks[1].batch_task_list[0].task_id, 3);
}
