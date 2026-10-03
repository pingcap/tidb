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

//! Replica candidates are selected independently for every task and reload.
//! Read-byte estimation and result caching retain their own observation state.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn fresh_queries_select_eligible_replicas_when_dispatched() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Arc::clone(&calls),
        [
            Ok(response(b"second-dispatched-first")),
            Ok(response(b"first-dispatched-second")),
        ],
        [location_with_three_peers(1, "a", "z", "tikv")],
    ));
    let request = transport_request(request_metadata);
    let mut first = select_result(&mut runtime, &request);
    let mut second = select_result(&mut runtime, &request);
    assert_eq!(
        second.next_raw().unwrap(),
        Some(b"second-dispatched-first".to_vec())
    );
    assert_eq!(second.next_raw().unwrap(), None);
    assert_eq!(
        first.next_raw().unwrap(),
        Some(b"first-dispatched-second".to_vec())
    );
    assert_eq!(first.next_raw().unwrap(), None);

    let addresses: Vec<_> = calls
        .read()
        .unwrap()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(addresses.len(), 2);
    for address in addresses {
        assert!([
            "tikv-leader:20160",
            "tikv-follower:20160",
            "tikv-learner:20160"
        ]
        .contains(&address.as_str()));
    }
}

#[test]
fn logical_tasks_select_from_their_own_region_candidates() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Arc::clone(&calls),
        [Ok(response(b"left")), Ok(response(b"right"))],
        [
            location_with_three_peers(1, "a", "m", "left"),
            location_with_three_peers(100, "m", "z", "right"),
        ],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let addresses: Vec<_> = calls
        .read()
        .unwrap()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(addresses.len(), 2);
    for (address, prefix) in addresses.iter().zip(["left", "right"]) {
        assert!(["leader", "follower", "learner"]
            .iter()
            .any(|role| address == &format!("{prefix}-{role}:20160")));
    }
}

#[test]
fn region_reload_selects_from_fresh_region_candidates() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Arc::clone(&calls),
        [Ok(region_not_found(1)), Ok(response(b"fresh"))],
        [
            location_with_three_peers(1, "a", "z", "old"),
            location_with_three_peers(1, "a", "z", "new"),
        ],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let addresses: Vec<_> = calls
        .read()
        .unwrap()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(addresses.len(), 2);
    for (address, prefix) in addresses.iter().zip(["old", "new"]) {
        assert!(["leader", "follower", "learner"]
            .iter()
            .any(|role| address == &format!("{prefix}-{role}:20160")));
    }
}

#[test]
fn first_real_unary_response_replaces_the_seed_before_continuation() {
    // pkg/store/copr/ema.go:33-36 newRUEMA leaves lastObsAt at zero so the
    // first time.Now observation has unit alpha and replaces the byte seed.
    let calls = Arc::new(RwLock::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec().into(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        exec_details_v2: Some(CoprocessorExecDetailsV2 {
            scan_detail_v2: Some(CoprocessorScanDetailV2 {
                processed_versions_size: 1_000_000,
                total_versions_size: 1_000_000,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.paging.enabled = true;
    metadata.paging.min_size = 2;
    metadata.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Arc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.read().unwrap();
    assert_eq!(calls[0].predicted_read_bytes, 4096);
    assert_eq!(calls[1].predicted_read_bytes, 1_000_000);
}

#[test]
fn process_time_admits_a_response_for_the_next_query_on_the_shared_cache() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let miss = CoprocessorResponse {
        data: b"cached-result".to_vec().into(),
        cache_last_version: 9,
        can_be_cached: true,
        exec_details_v2: Some(CoprocessorExecDetailsV2 {
            time_detail_v2: Some(CoprocessorTimeDetailV2 {
                process_wall_time_ns: 6_000_000,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let hit = CoprocessorResponse {
        is_cache_hit: true,
        ..Default::default()
    }
    .encode_to_vec();
    let shared_cache = CoprCache::from_config(&CoprCacheConfig {
        capacity_mb: 1.0,
        admission_max_result_mb: 1.0,
        admission_min_process_ms: 5,
        ..Default::default()
    })
    .unwrap()
    .unwrap();
    let transport = transport_with_loader_calls_and_config(
        Arc::clone(&calls),
        [Ok(miss), Ok(hit)],
        [location(1, "a", "z", "tikv-1:20160")],
        9001,
        Arc::new(RwLock::new(Vec::new())),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            shared_cache: Some(shared_cache.clone()),
            observation_time,
            ..Default::default()
        },
    );
    let mut request_metadata = metadata("a", "z");
    request_metadata.cacheable = true;
    let request = transport_request(request_metadata);
    let mut runtime = InjectedQueryRuntime::new(transport);

    let mut first = select_result(&mut runtime, &request);
    assert_eq!(first.next_raw().unwrap(), Some(b"cached-result".to_vec()));
    assert_eq!(first.next_raw().unwrap(), None);
    shared_cache.wait();
    let mut second = select_result(&mut runtime, &request);
    assert_eq!(second.next_raw().unwrap(), Some(b"cached-result".to_vec()));
    assert_eq!(second.next_raw().unwrap(), None);

    let calls = calls.read().unwrap();
    assert!(calls[0].is_cache_enabled);
    assert_eq!(calls[0].cache_if_match_version, 0);
    assert!(calls[1].is_cache_enabled);
    assert_eq!(calls[1].cache_if_match_version, 9);
}
