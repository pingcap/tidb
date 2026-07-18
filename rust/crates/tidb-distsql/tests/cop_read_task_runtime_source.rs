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

//! Focused direct composition for the pre-transport coprocessor read-task owner.

use std::time::Duration;

use tidb_distsql::cop_paging::{CopReadTaskError, CopReadTaskResponse, CopReadTaskRuntime};
use tidb_distsql::{
    CopPagingState, CoprCache, CoprCacheConfig, CoprCacheResponseOutcome, KvRequestMetadata,
    ReadEngineGeneration, RegionTaskTopology, RequestKeyRange, RequestKeyRanges, RequestType,
    ResponseChannelEvent, StoreType,
};
use tidb_proto::{
    CoprocessorExecDetailsV2, CoprocessorKeyRange, CoprocessorResponse, CoprocessorScanDetailV2,
};

fn range(start: &str, end: &str) -> RequestKeyRange {
    RequestKeyRange {
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
    }
}

fn metadata(ranges: Vec<RequestKeyRange>) -> KvRequestMetadata {
    let mut metadata = KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(b"dag".to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(ranges)),
        keep_order: true,
        cacheable: true,
        store_type: StoreType::TiKv,
        start_ts: 100,
        ..KvRequestMetadata::default()
    };
    metadata.session.paging.enabled = true;
    metadata.session.paging.min_size = 2;
    metadata.session.paging.max_size = 8;
    metadata
}

fn topology(region_id: u64, start: &str, end: &str) -> RegionTaskTopology {
    RegionTaskTopology {
        region_id,
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
        ..RegionTaskTopology::default()
    }
}

fn response(data: &str, start: &str, end: &str, read_bytes: u64) -> CoprocessorResponse {
    CoprocessorResponse {
        data: data.as_bytes().to_vec(),
        range: Some(CoprocessorKeyRange {
            start: start.as_bytes().to_vec(),
            end: end.as_bytes().to_vec(),
        }),
        exec_details_v2: Some(CoprocessorExecDetailsV2 {
            scan_detail_v2: Some(CoprocessorScanDetailV2 {
                processed_versions_size: read_bytes,
                total_versions_size: read_bytes,
            }),
        }),
        ..CoprocessorResponse::default()
    }
}

fn cache() -> CoprCache {
    CoprCache::from_config(&CoprCacheConfig {
        capacity_mb: 1.0,
        admission_max_ranges: 0,
        admission_max_result_mb: 1.0,
        admission_min_process_ms: 0,
    })
    .unwrap()
    .unwrap()
}

fn prepare(
    metadata: &KvRequestMetadata,
    topology: &[RegionTaskTopology],
    cache: Option<CoprCache>,
) -> Result<CopReadTaskRuntime, CopReadTaskError> {
    CopPagingState::prepare_read_tasks(metadata, topology, cache, ReadEngineGeneration::Classic, 0)
}

#[test]
fn checked_tasks_share_ema_and_publish_only_to_their_ordered_channels() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut runtime = prepare(
        &metadata,
        &[topology(1, "a", "m"), topology(2, "m", "z")],
        None,
    )
    .unwrap();
    assert_eq!(runtime.in_flight_attempt_ids(), [1, 2]);
    assert_eq!(runtime.prepared_attempt(1).unwrap().logical_task_id(), 1);
    assert_eq!(runtime.prepared_attempt(2).unwrap().logical_task_id(), 2);
    assert_eq!(runtime.prepared_attempt(1).unwrap().page_index(), 1);
    assert_eq!(runtime.prepared_attempt(2).unwrap().page_index(), 2);

    let accepted = runtime
        .accept_response(
            1,
            response("region-1", "a", "m", 4096),
            None,
            Duration::from_secs(100),
        )
        .unwrap();
    assert_eq!(accepted.logical_task_id, 1);
    assert_eq!(accepted.next_attempt_id, None);
    assert_eq!(runtime.predicted_read_bytes(), 4096);
    assert_eq!(runtime.task_predicted_read_bytes(2), Some(4096));
    assert_eq!(
        runtime.next_response(1),
        Some(ResponseChannelEvent::Result(b"region-1".to_vec()))
    );
    assert_eq!(runtime.next_response(2), None);
}

#[test]
fn continuation_uses_the_next_iterator_wide_paging_task_index() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut runtime = prepare(
        &metadata,
        &[topology(1, "a", "m"), topology(2, "m", "z")],
        None,
    )
    .unwrap();

    let accepted = runtime
        .accept_response(
            1,
            response("partial", "a", "g", 1),
            None,
            Duration::from_secs(1),
        )
        .unwrap();
    let continuation = runtime
        .prepared_attempt(accepted.next_attempt_id.unwrap())
        .unwrap();
    assert_eq!(continuation.logical_task_id(), 1);
    assert_eq!(continuation.page_index(), 3);
    assert_eq!(continuation.request().ranges, [range("g", "m")]);
}

#[test]
fn cache_is_prepared_per_attempt_restored_before_paging_and_rebuilt_for_continuation() {
    let metadata = metadata(vec![range("a", "z")]);
    let topology = [topology(7, "a", "z")];
    let mut runtime = prepare(&metadata, &topology, Some(cache())).unwrap();
    let initial = runtime.prepared_attempt(1).unwrap();
    let initial_key = initial.cache_key().unwrap().to_vec();
    assert!(initial.request().is_cache_enabled);
    assert_eq!(initial.request().cache_if_match_version, 0);

    let mut first = response("cached-page", "a", "m", 1024);
    first.can_be_cached = true;
    first.cache_last_version = 9;
    let accepted = runtime
        .accept_response(1, first, Some(1), Duration::from_secs(100))
        .unwrap();
    let continuation = accepted.next_attempt_id.unwrap();
    assert_eq!(
        accepted.cache_outcome,
        Some(CoprCacheResponseOutcome::Stored)
    );
    let continued = runtime.prepared_attempt(continuation).unwrap();
    assert_eq!(continued.page_index(), 2);
    assert_eq!(continued.request().ranges, [range("m", "z")]);
    assert_eq!(continued.request().paging_size, 4);
    assert_ne!(continued.cache_key().unwrap(), initial_key);
    assert_eq!(
        runtime.next_response(1),
        Some(ResponseChannelEvent::Result(b"cached-page".to_vec()))
    );

    runtime
        .accept_response(
            continuation,
            response("last-page", "m", "z", 2048),
            Some(1),
            Duration::from_secs(101),
        )
        .unwrap();
    let cache = runtime.into_cache().unwrap().unwrap();

    let mut replay = prepare(&metadata, &topology, Some(cache)).unwrap();
    assert_eq!(
        replay
            .prepared_attempt(1)
            .unwrap()
            .request()
            .cache_if_match_version,
        9
    );
    let replayed = replay
        .accept_response(
            1,
            CoprocessorResponse {
                is_cache_hit: true,
                ..CoprocessorResponse::default()
            },
            None,
            Duration::from_secs(102),
        )
        .unwrap();
    assert_eq!(replayed.cache_outcome, Some(CoprCacheResponseOutcome::Hit));
    let replay_continuation = replayed.next_attempt_id.unwrap();
    let replay_initial_key = replay.prepared_attempt(1).unwrap().cache_key().unwrap();
    let replay_continued = replay.prepared_attempt(replay_continuation).unwrap();
    assert_eq!(replay_continued.page_index(), 2);
    assert_eq!(replay_continued.request().ranges, [range("m", "z")]);
    assert_ne!(replay_continued.cache_key().unwrap(), replay_initial_key);
    assert_eq!(
        replay.next_response(1),
        Some(ResponseChannelEvent::Result(b"cached-page".to_vec()))
    );
}

#[test]
fn unsupported_request_shapes_fail_before_task_or_cache_mutation() {
    let valid_topology = [topology(1, "a", "z")];
    let mut cases = Vec::new();

    let mut request = metadata(vec![range("a", "z")]);
    request.store_type = StoreType::TiFlash;
    cases.push((request, "unsupported_store"));

    let mut request = metadata(vec![range("a", "z")]);
    request.request_type = RequestType::Analyze;
    cases.push((request, "unsupported_request_type"));

    let mut request = metadata(vec![range("a", "z")]);
    request.batch_cop = true;
    cases.push((request, "batch_coprocessor"));

    let mut request = metadata(vec![range("a", "z")]);
    request.session.store_batch_size = 2;
    cases.push((request, "store_batching"));

    let mut request = metadata(vec![range("a", "z")]);
    request.keep_order = false;
    cases.push((request, "unordered_response"));

    let mut request = metadata(vec![range("a", "z")]);
    request.key_ranges = Some(RequestKeyRanges::new_partitioned(vec![vec![range(
        "a", "z",
    )]]));
    cases.push((request, "partitioned_ranges"));

    let mut request = metadata(vec![range("a", "z")]);
    request.session.max_keys_read = 10;
    cases.push((request, "max_keys_read"));

    for (request, expected) in cases {
        assert_eq!(
            prepare(&request, &valid_topology, Some(cache()))
                .unwrap_err()
                .kind(),
            expected
        );
    }

    let too_many = vec![range("a", "b"); 25_001];
    assert_eq!(
        prepare(&metadata(too_many), &valid_topology, None)
            .unwrap_err()
            .kind(),
        "too_many_ranges"
    );
}

#[test]
fn malformed_stale_outside_and_nonmonotonic_topology_fails_closed() {
    let nonmonotonic = metadata(vec![range("m", "z"), range("a", "b")]);
    assert_eq!(
        prepare(&nonmonotonic, &[topology(1, "a", "z")], None)
            .unwrap_err()
            .kind(),
        "invalid_ranges"
    );

    let metadata = metadata(vec![range("a", "z")]);
    let mut stale_bucket = topology(1, "m", "z");
    stale_bucket.bucket_keys = vec![b"b".to_vec()];
    stale_bucket.buckets_version = 3;
    assert_eq!(
        prepare(&metadata, &[stale_bucket], None)
            .unwrap_err()
            .kind(),
        "invalid_topology"
    );

    let overlapping = [topology(1, "a", "n"), topology(2, "m", "z")];
    assert_eq!(
        prepare(&metadata, &overlapping, None).unwrap_err().kind(),
        "invalid_topology"
    );

    let uncovered = [topology(1, "a", "m")];
    assert_eq!(
        prepare(&metadata, &uncovered, None).unwrap_err().kind(),
        "invalid_topology"
    );
}

#[test]
fn embedded_response_errors_precede_cache_ema_paging_and_attempt_mutation() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut runtime = prepare(&metadata, &[topology(1, "a", "z")], Some(cache())).unwrap();
    let cache_key = runtime
        .prepared_attempt(1)
        .unwrap()
        .cache_key()
        .unwrap()
        .to_vec();
    let mut body = response("must-not-publish", "a", "m", 4096);
    body.can_be_cached = true;
    body.cache_last_version = 9;

    let errors = [
        (
            CopReadTaskResponse::region_error(body.clone()),
            "region_error",
        ),
        (CopReadTaskResponse::lock_error(body.clone()), "lock_error"),
        (
            CopReadTaskResponse::other_error(body.clone(), "boom"),
            "other_error",
        ),
        (CopReadTaskResponse::batch(body), "batch_response"),
    ];
    for (response, expected_kind) in errors {
        assert_eq!(
            runtime
                .accept_response(1, response, Some(1), Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            expected_kind
        );
        assert_eq!(runtime.in_flight_attempt_ids(), [1]);
        assert_eq!(runtime.predicted_read_bytes(), 0);
        assert!(runtime.cache().unwrap().get(&cache_key).is_none());
        assert_eq!(runtime.next_response(1), None);
        assert_eq!(runtime.prepared_attempts().count(), 1);
    }
}

#[test]
fn response_errors_newer_buckets_invalid_hits_and_attempt_mismatches_fail_closed() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut topology = topology(1, "a", "z");
    topology.bucket_keys = vec![b"m".to_vec()];
    topology.buckets_version = 7;
    let mut runtime = prepare(&metadata, &[topology], None).unwrap();

    assert_eq!(
        runtime.accept_region_error(1).unwrap_err().kind(),
        "region_error"
    );
    assert_eq!(
        runtime.accept_lock_error(1).unwrap_err().kind(),
        "lock_error"
    );
    assert_eq!(
        runtime.accept_other_error(1, "boom").unwrap_err().kind(),
        "other_error"
    );
    assert_eq!(
        runtime.accept_batch_response(1).unwrap_err().kind(),
        "batch_response"
    );
    assert_eq!(
        runtime
            .accept_response(
                1,
                CoprocessorResponse {
                    latest_buckets_version: 8,
                    ..CoprocessorResponse::default()
                },
                None,
                Duration::ZERO,
            )
            .unwrap_err()
            .kind(),
        "newer_buckets"
    );
    assert_eq!(
        runtime
            .accept_response(
                1,
                CoprocessorResponse {
                    is_cache_hit: true,
                    ..CoprocessorResponse::default()
                },
                None,
                Duration::ZERO,
            )
            .unwrap_err()
            .kind(),
        "cache"
    );
    assert_eq!(
        runtime
            .accept_response(999, CoprocessorResponse::default(), None, Duration::ZERO)
            .unwrap_err()
            .kind(),
        "unmatched_response"
    );

    runtime
        .accept_response(
            1,
            response("done", "a", "m", 1),
            None,
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(
        runtime
            .accept_response(1, CoprocessorResponse::default(), None, Duration::ZERO)
            .unwrap_err()
            .kind(),
        "duplicate_response"
    );
}

#[test]
fn region_error_attempt_is_consumed_once_without_success_state_mutation() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut runtime = prepare(&metadata, &[topology(1, "a", "z")], Some(cache())).unwrap();
    let cache_key = runtime
        .prepared_attempt(1)
        .unwrap()
        .cache_key()
        .unwrap()
        .to_vec();

    let failed = runtime.consume_region_error(1).unwrap();
    assert_eq!(failed.attempt_id(), 1);
    assert_eq!(failed.logical_task_id(), 1);
    assert_eq!(failed.ranges(), [range("a", "z")]);
    assert_eq!(failed.paging_size(), 2);
    assert!(runtime.in_flight_attempt_ids().is_empty());
    assert_eq!(runtime.predicted_read_bytes(), 0);
    assert!(runtime.cache().unwrap().get(&cache_key).is_none());
    assert_eq!(runtime.next_response(1), None);
    assert_eq!(
        runtime.consume_region_error(1).unwrap_err().kind(),
        "duplicate_response"
    );
}

#[test]
fn known_leader_retry_rebinds_one_logical_task_without_growing_paging() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut runtime = prepare(&metadata, &[topology(1, "a", "z")], None).unwrap();
    let failed = runtime.consume_region_error(1).unwrap();
    let replacement = runtime
        .retry_region_attempt(failed, &[topology(1, "a", "z")])
        .unwrap();

    assert_eq!(replacement.logical_task_ids, [1]);
    assert_eq!(replacement.active_attempt_ids, [2]);
    let resent = runtime.prepared_attempt(2).unwrap();
    assert_eq!(resent.logical_task_id(), 1);
    assert_eq!(resent.request().ranges, [range("a", "z")]);
    assert_eq!(resent.request().paging_size, 2);
    assert_eq!(resent.page_index(), 2);
}

#[test]
fn rebuild_splits_only_failed_task_and_preserves_prior_page_and_future_attempt() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut runtime = prepare(
        &metadata,
        &[topology(1, "a", "m"), topology(2, "m", "z")],
        None,
    )
    .unwrap();
    let accepted = runtime
        .accept_response(
            1,
            response("prior-page", "a", "g", 4096),
            None,
            Duration::from_secs(100),
        )
        .unwrap();
    assert_eq!(accepted.next_attempt_id, Some(3));
    let failed = runtime.consume_region_error(3).unwrap();

    let replacement = runtime
        .rebuild_region_attempts(failed, &[topology(10, "g", "j"), topology(11, "j", "m")])
        .unwrap();

    assert_eq!(replacement.logical_task_ids, [1, 3]);
    assert_eq!(replacement.active_attempt_ids, [4, 5]);
    assert_eq!(runtime.in_flight_attempt_ids(), [2, 4, 5]);
    assert_eq!(
        runtime.prepared_attempt(4).unwrap().request().ranges,
        [range("g", "j")]
    );
    assert_eq!(
        runtime.prepared_attempt(5).unwrap().request().ranges,
        [range("j", "m")]
    );
    assert_eq!(
        runtime.prepared_attempt(4).unwrap().request().paging_size,
        4
    );
    assert_eq!(
        runtime.prepared_attempt(5).unwrap().request().paging_size,
        4
    );
    assert_eq!(
        runtime.prepared_attempt(2).unwrap().request().ranges,
        [range("m", "z")]
    );
    assert_eq!(runtime.prepared_attempt(2).unwrap().logical_task_id(), 2);
    assert_eq!(
        runtime.prepared_attempt(2).unwrap().request().paging_size,
        2
    );
    assert_eq!(runtime.predicted_read_bytes(), 4096);
    assert_eq!(
        runtime.next_response(1),
        Some(ResponseChannelEvent::Result(b"prior-page".to_vec()))
    );
    runtime
        .accept_response(
            2,
            response("future-task", "m", "z", 1),
            None,
            Duration::from_secs(101),
        )
        .unwrap();
    assert_eq!(
        runtime.next_response(2),
        Some(ResponseChannelEvent::Result(b"future-task".to_vec()))
    );
}

#[test]
fn backpressure_rejects_before_cache_or_paging_mutation() {
    let metadata = metadata(vec![range("a", "z")]);
    let mut runtime = prepare(&metadata, &[topology(1, "a", "z")], Some(cache())).unwrap();
    let mut attempt_id = 1;

    // Ordered paging tasks have eighteen response slots. Fill all of them
    // without draining while retaining one continuation attempt.
    for offset in 0..18_u8 {
        let start = char::from(b'a' + offset).to_string();
        let end = char::from(b'b' + offset).to_string();
        attempt_id = runtime
            .accept_response(
                attempt_id,
                response("queued", &start, &end, 1024),
                None,
                Duration::from_secs(u64::from(offset) + 1),
            )
            .unwrap()
            .next_attempt_id
            .unwrap();
    }

    let failed_key = runtime
        .prepared_attempt(attempt_id)
        .unwrap()
        .cache_key()
        .unwrap()
        .to_vec();
    assert!(runtime.cache().unwrap().get(&failed_key).is_none());
    let prediction = runtime.predicted_read_bytes();
    let mut rejected = response("must-not-store", "s", "t", 4096);
    rejected.can_be_cached = true;
    rejected.cache_last_version = 99;

    assert_eq!(
        runtime.accept_response(
            attempt_id,
            rejected.clone(),
            Some(1),
            Duration::from_secs(100),
        ),
        Err(CopReadTaskError::Paging(
            tidb_distsql::CopPagingError::Backpressure { capacity: 18 }
        ))
    );
    assert!(runtime.cache().unwrap().get(&failed_key).is_none());
    assert_eq!(runtime.predicted_read_bytes(), prediction);
    assert!(runtime.in_flight_attempt_ids().contains(&attempt_id));

    assert!(matches!(
        runtime.next_response(1),
        Some(ResponseChannelEvent::Result(_))
    ));
    let accepted = runtime
        .accept_response(attempt_id, rejected, Some(1), Duration::from_secs(100))
        .unwrap();
    assert_eq!(
        accepted.cache_outcome,
        Some(CoprCacheResponseOutcome::Stored)
    );
    assert!(runtime.cache().unwrap().get(&failed_key).is_some());
}
