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

//! Direct paging-continuation tables from `pkg/store/copr`.

use std::time::Duration;

use tidb_distsql::{
    calculate_paging_remain, calculate_paging_retry, paging_response_read_bytes,
    BatchBucketVersionUpdate, CopPagingError, CopPagingState, ReadEngineGeneration,
    RegionTaskEnvelope, RegionTaskTopology, RequestKeyRange, ResponseChannelEvent,
};
use tidb_proto::{
    CoprocessorExecDetailsV2, CoprocessorKeyRange, CoprocessorResponse, CoprocessorScanDetailV2,
};
use tidb_txnkv::{Key, KeyRange, KeyRanges};

fn ranges(keys: &[&str]) -> KeyRanges {
    KeyRanges::new(
        keys.chunks_exact(2)
            .map(|pair| {
                KeyRange::new(
                    Key::from_bytes(pair[0].as_bytes()),
                    Key::from_bytes(pair[1].as_bytes()),
                )
            })
            .collect(),
    )
}

fn split(start: &str, end: &str) -> CoprocessorKeyRange {
    CoprocessorKeyRange {
        start: start.as_bytes().to_vec(),
        end: end.as_bytes().to_vec(),
    }
}

fn strings(ranges: &KeyRanges) -> Vec<(String, String)> {
    ranges
        .to_ranges()
        .into_iter()
        .map(|range| {
            (
                String::from_utf8(range.start_key.as_bytes().to_vec()).unwrap(),
                String::from_utf8(range.end_key.as_bytes().to_vec()).unwrap(),
            )
        })
        .collect()
}

fn owned_ranges(ranges: Vec<(&str, &str)>) -> Vec<(String, String)> {
    ranges
        .into_iter()
        .map(|(start, end)| (start.to_owned(), end.to_owned()))
        .collect()
}

#[test]
fn retry_and_remain_match_every_directional_source_case() {
    let source = ranges(&["a", "c", "e", "g"]);
    let retry_cases = [
        ("b", "c", false, vec![("b", "c"), ("e", "g")]),
        ("e", "f", true, vec![("a", "c"), ("e", "f")]),
        ("b", "f", false, vec![("b", "c"), ("e", "g")]),
        ("b", "f", true, vec![("a", "c"), ("e", "f")]),
        ("a", "g", false, vec![("a", "c"), ("e", "g")]),
        ("a", "g", true, vec![("a", "c"), ("e", "g")]),
    ];
    for (start, end, desc, expected) in retry_cases {
        assert_eq!(
            strings(&calculate_paging_retry(
                &source,
                Some(&split(start, end)),
                desc
            )),
            owned_ranges(expected)
        );
    }
    assert_eq!(calculate_paging_retry(&source, None, false), source);

    let source = ranges(&["a", "c", "e", "g"]);
    let remain_cases = [
        ("a", "b", false, vec![("b", "c"), ("e", "g")]),
        ("f", "g", true, vec![("a", "c"), ("e", "f")]),
        ("a", "f", false, vec![("f", "g")]),
        ("b", "g", true, vec![("a", "b")]),
        ("a", "g", false, vec![]),
        ("a", "g", true, vec![]),
    ];
    for (start, end, desc, expected) in remain_cases {
        assert_eq!(
            strings(&calculate_paging_remain(
                &source,
                Some(&split(start, end)),
                desc
            )),
            owned_ranges(expected)
        );
    }
    assert_eq!(calculate_paging_remain(&source, None, true), source);
}

#[test]
fn response_read_bytes_matches_classic_and_next_generation_tables() {
    assert_eq!(
        paging_response_read_bytes(None, ReadEngineGeneration::Classic),
        0
    );
    let empty = CoprocessorResponse::default();
    assert_eq!(
        paging_response_read_bytes(Some(&empty), ReadEngineGeneration::Classic),
        0
    );
    for (processed, total, classic, next_generation) in [
        (1_048_576, 2_097_152, 1_048_576, 2_097_152),
        (1_048_576, 512 * 1024, 1_048_576, 1_048_576),
    ] {
        let response = CoprocessorResponse {
            exec_details_v2: Some(CoprocessorExecDetailsV2 {
                scan_detail_v2: Some(CoprocessorScanDetailV2 {
                    processed_versions_size: processed,
                    total_versions_size: total,
                }),
            }),
            ..Default::default()
        };
        assert_eq!(
            paging_response_read_bytes(Some(&response), ReadEngineGeneration::Classic),
            classic
        );
        assert_eq!(
            paging_response_read_bytes(Some(&response), ReadEngineGeneration::NextGeneration),
            next_generation
        );
    }
}

#[test]
fn successful_page_updates_ema_grows_size_and_feeds_response_channel() {
    let task = RegionTaskEnvelope {
        ranges: vec![
            RequestKeyRange {
                start_key: b"a".to_vec().into(),
                end_key: b"c".to_vec().into(),
            },
            RequestKeyRange {
                start_key: b"e".to_vec().into(),
                end_key: b"g".to_vec().into(),
            },
        ],
        paging: true,
        paging_size: 128,
        ..Default::default()
    };
    let mut state =
        CopPagingState::new(&task, false, 1024, ReadEngineGeneration::Classic, 4_194_304);
    let response = CoprocessorResponse {
        data: b"select-response".to_vec(),
        range: Some(split("a", "b")),
        exec_details_v2: Some(CoprocessorExecDetailsV2 {
            scan_detail_v2: Some(CoprocessorScanDetailV2 {
                processed_versions_size: 1_000_000,
                total_versions_size: 2_000_000,
            }),
        }),
        ..Default::default()
    };
    let outcome = state
        .accept_response(&response, Duration::from_secs(1_000_000))
        .unwrap();
    assert_eq!(outcome.observed_read_bytes, 1_000_000);
    assert_eq!(outcome.next_paging_size, 256);
    assert_eq!(outcome.remaining_ranges[0].start_key, b"b");
    assert_eq!(state.predicted_read_bytes(), 1_000_000);
    assert_eq!(
        state.next_response(),
        Some(ResponseChannelEvent::Result(b"select-response".to_vec()))
    );

    let retry = state.retry_after_error(Some(&split("b", "c")));
    assert_eq!(retry.next_paging_size, 256);
    assert_eq!(retry.observed_read_bytes, 0);
}

#[test]
fn terminal_nil_range_cannot_resurrect_completed_paging_state() {
    let task = RegionTaskEnvelope {
        ranges: vec![RequestKeyRange {
            start_key: b"a".to_vec().into(),
            end_key: b"z".to_vec().into(),
        }],
        paging: true,
        paging_size: 128,
        ..Default::default()
    };
    let mut state =
        CopPagingState::new(&task, false, 1024, ReadEngineGeneration::Classic, 4_194_304);
    let terminal = state
        .accept_response(
            &CoprocessorResponse {
                data: b"final-page".to_vec(),
                range: None,
                ..Default::default()
            },
            Duration::from_secs(1_000_000),
        )
        .unwrap();
    assert!(terminal.remaining_ranges.is_empty());
    assert_eq!(terminal.next_paging_size, 0);

    let retry = state.retry_after_error(Some(&split("m", "z")));
    assert!(retry.remaining_ranges.is_empty());
    assert_eq!(retry.next_paging_size, 0);

    let duplicate = state
        .accept_response(
            &CoprocessorResponse {
                data: b"must-not-reopen".to_vec(),
                range: Some(split("a", "m")),
                ..Default::default()
            },
            Duration::from_secs(1_000_001),
        )
        .unwrap();
    assert!(duplicate.remaining_ranges.is_empty());
    assert_eq!(duplicate.next_paging_size, 0);
    assert_eq!(duplicate.observed_read_bytes, 0);
}

#[test]
fn bounded_multi_page_channel_drains_then_emits_terminal_closed() {
    let task = RegionTaskEnvelope {
        ranges: vec![RequestKeyRange {
            start_key: b"a".to_vec().into(),
            end_key: b"z".to_vec().into(),
        }],
        paging: true,
        paging_size: 128,
        response_channel_capacity: 2,
        ..Default::default()
    };
    let mut state = CopPagingState::new(&task, false, 1024, ReadEngineGeneration::Classic, 0);
    state
        .accept_response(
            &CoprocessorResponse {
                data: b"page-1".to_vec(),
                range: Some(split("a", "m")),
                ..Default::default()
            },
            Duration::from_secs(1_000_000),
        )
        .unwrap();
    state
        .accept_response(
            &CoprocessorResponse {
                data: b"page-2".to_vec(),
                range: None,
                ..Default::default()
            },
            Duration::from_secs(1_000_001),
        )
        .unwrap();

    assert_eq!(
        state.next_response(),
        Some(ResponseChannelEvent::Result(b"page-1".to_vec()))
    );
    assert_eq!(
        state.next_response(),
        Some(ResponseChannelEvent::Result(b"page-2".to_vec()))
    );
    assert_eq!(state.next_response(), Some(ResponseChannelEvent::Closed));
    assert_eq!(state.next_response(), None);
}

#[test]
fn bounded_channel_applies_backpressure_before_continuation_mutation() {
    let task = RegionTaskEnvelope {
        ranges: vec![RequestKeyRange {
            start_key: b"a".to_vec().into(),
            end_key: b"z".to_vec().into(),
        }],
        paging: true,
        paging_size: 128,
        response_channel_capacity: 1,
        ..Default::default()
    };
    let mut state = CopPagingState::new(&task, false, 1024, ReadEngineGeneration::Classic, 0);
    let first = CoprocessorResponse {
        data: b"page-1".to_vec(),
        range: Some(split("a", "m")),
        ..Default::default()
    };
    state
        .accept_response(&first, Duration::from_secs(1_000_000))
        .unwrap();

    let second = CoprocessorResponse {
        data: b"page-2".to_vec(),
        range: Some(split("m", "t")),
        ..Default::default()
    };
    assert_eq!(
        state.accept_response(&second, Duration::from_secs(1_000_001)),
        Err(CopPagingError::Backpressure { capacity: 1 })
    );
    assert_eq!(
        state.next_response(),
        Some(ResponseChannelEvent::Result(b"page-1".to_vec()))
    );
    let accepted = state
        .accept_response(&second, Duration::from_secs(1_000_001))
        .unwrap();
    assert_eq!(accepted.remaining_ranges[0].start_key, b"t");
    assert_eq!(
        state.next_response(),
        Some(ResponseChannelEvent::Result(b"page-2".to_vec()))
    );
}

#[test]
fn bucket_version_mismatch_targets_the_child_region_not_parent() {
    let child = RegionTaskEnvelope {
        region_id: 2,
        task_id: 1,
        buckets_version: 11,
        ..Default::default()
    };
    let parent = RegionTaskEnvelope {
        region_id: 1,
        buckets_version: 7,
        batch_task_list: vec![child],
        ..Default::default()
    };
    let keys = vec![b"m".to_vec(), b"n".to_vec(), Vec::new()];
    let update = BatchBucketVersionUpdate::for_child(&parent, 1, 99, keys.clone()).unwrap();
    assert_eq!(update.region_id, 2);
    assert_eq!(update.request_version, 11);
    let mut topology = vec![
        RegionTaskTopology {
            region_id: 1,
            buckets_version: 7,
            ..Default::default()
        },
        RegionTaskTopology {
            region_id: 2,
            buckets_version: 11,
            ..Default::default()
        },
    ];
    assert!(update.apply(&mut topology));
    assert_eq!(topology[0].buckets_version, 7);
    assert_eq!(topology[1].buckets_version, 99);
    assert_eq!(topology[1].bucket_keys, keys);
}
