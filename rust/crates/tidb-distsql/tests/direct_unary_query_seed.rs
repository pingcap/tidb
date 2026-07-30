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

//! The per-query transport seed: one query advances it exactly once, every
//! logical task in that query shares the seed it was bound to, a region reload
//! reuses it rather than taking a new one, and the first real unary response
//! replaces it before any continuation is sent.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn fresh_queries_advance_the_transport_seed_once_each() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
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
        .borrow()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(
        addresses,
        ["tikv-learner:20160", "tikv-follower:20160"],
        "fresh query bindings must rotate before either response is pulled"
    );
}

#[test]
fn logical_tasks_in_one_query_share_the_bound_seed() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
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
        .borrow()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(
        addresses,
        ["left-follower:20160", "right-follower:20160"],
        "all logical tasks in one query must use the same immutable seed"
    );
}

#[test]
fn region_reload_reuses_the_bound_query_seed() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
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
        .borrow()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(
        addresses,
        ["old-follower:20160", "new-follower:20160"],
        "a rebuilt selector must retain the response-bound seed"
    );
}

#[test]
fn first_real_unary_response_replaces_the_seed_before_continuation() {
    // pkg/store/copr/ema.go:33-36 newRUEMA leaves lastObsAt at zero so the
    // first time.Now observation has unit alpha and replaces the byte seed.
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        exec_details_v2: Some(CoprocessorExecDetailsV2 {
            scan_detail_v2: Some(CoprocessorScanDetailV2 {
                processed_versions_size: 1_000_000,
                total_versions_size: 1_000_000,
            }),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.paging.enabled = true;
    metadata.paging.min_size = 2;
    metadata.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.borrow();
    assert_eq!(calls[0].predicted_read_bytes, 4096);
    assert_eq!(calls[1].predicted_read_bytes, 1_000_000);
}
