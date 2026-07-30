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

//! Region errors that keep the query alive: `DataIsNotReady` falling through
//! or retrying one selector, a known-leader region error resent immediately in
//! the same query, and the recovered route a batch republishes after a region
//! error or a connection failure.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn cached_leader_data_is_not_ready_falls_through_without_reload_or_backoff() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut initial =
        location_with_second_peer(1, "a", "z", "tikv-leader:20160", "tikv-follower:20160");
    initial.peers.swap(0, 1);
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(data_is_not_ready()), Ok(response(b"fresh"))],
        [initial],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Leader;
    request_metadata.is_staleness = true;
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let calls = calls.borrow();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].address, "tikv-leader:20160");
    assert_eq!(calls[0].replica_read_type, ClientReplicaReadType::Mixed);
    assert!(!calls[0].replica_read);
    assert!(calls[0].stale_read);
    assert_eq!(calls[1].address, "tikv-follower:20160");
    assert_eq!(calls[1].replica_read_type, ClientReplicaReadType::Mixed);
    assert!(calls[1].replica_read);
    assert!(!calls[1].stale_read);
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec()],
        "leader DataIsNotReady must not invalidate or reload the region"
    );
    assert!(
        retry_control.sleeps.borrow().is_empty(),
        "DataIsNotReady fallthrough must not back off"
    );
}

#[test]
fn stale_data_not_ready_then_known_leader_retries_one_selector_and_publishes_once() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Leader;
    request_metadata.is_staleness = true;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [
            Ok(data_is_not_ready()),
            Ok(not_leader(1, Some((102, 202)))),
            Ok(response(b"fresh")),
        ],
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-leader:20160",
            "tikv-follower:20160",
        )],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let calls = calls.borrow();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].address, "tikv-follower:20160");
    assert!(!calls[0].replica_read);
    assert!(calls[0].stale_read);
    assert_eq!(calls[1].address, "tikv-leader:20160");
    assert!(!calls[1].replica_read);
    assert!(!calls[1].stale_read);
    assert_eq!(calls[2].address, "tikv-follower:20160");
    assert!(!calls[2].replica_read);
    assert!(!calls[2].stale_read);
    assert_eq!(
        calls
            .iter()
            .map(|call| call.replica_read_type)
            .collect::<Vec<_>>(),
        [
            ClientReplicaReadType::Mixed,
            ClientReplicaReadType::Mixed,
            ClientReplicaReadType::Leader,
        ],
        "stale and ordinary fallback attempts stay Mixed until known-NotLeader transitions the selector to Leader"
    );
}

#[test]
fn known_leader_region_error_resends_immediately_in_the_same_query() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let first = location_with_second_peer(1, "a", "z", "tikv-old:20160", "tikv-new:20160");
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, Some((102, 202)))), Ok(response(b"fresh"))],
        [first],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(60),
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let request = transport_request(metadata("a", "z"));

    let mut result = select_result(&mut runtime, &request);
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec()],
        "known-leader retry must use the exact cache update without PD reload"
    );
    assert_eq!(calls.borrow()[0].address, "tikv-old:20160");
    assert_eq!(calls.borrow()[1].address, "tikv-new:20160");
    assert_eq!(calls.borrow()[1].peer_id, 102);
    assert_eq!(calls.borrow()[1].store_id, 202);
    assert!(retry_control.sleeps.borrow().is_empty());
}

#[test]
fn batch_known_leader_region_error_republishes_the_recovered_route() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let transport = DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::from([
                Ok(not_leader(1, Some((102, 202)))),
                Ok(response(b"fresh-batch-route")),
            ]),
            events: Rc::new(RefCell::new(Vec::new())),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::new()),
            batch_completion_gate: None,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::clone(&loader_calls),
            regions: VecDeque::from([location_with_second_peer(
                1,
                "a",
                "z",
                "tikv-old:20160",
                "tikv-new:20160",
            )]),
        }),
        DirectUnaryRuntimeConfig::default(),
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let evidence = transport.evidence_handle();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"fresh-batch-route".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(loader_calls.borrow().as_slice(), [b"a".to_vec()]);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160"]
    );

    let evidence = evidence.snapshot();
    assert_eq!(evidence.batch_attempts, 2);
    assert_eq!(evidence.unary_attempts, 0);
    assert_eq!(
        evidence
            .published_attempts
            .iter()
            .map(|published| published.publication.physical_address())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160"],
        "the cache-recovered leader must be sent and published through BatchCommands"
    );
}

#[test]
fn batch_connection_failure_republishes_the_cache_recovered_route() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::from([
                Err(connection_failure(
                    "tikv-old:20160",
                    9,
                    DirectUnaryTransportClass::Connection,
                    None,
                )),
                Ok(response(b"recovered-batch-route")),
            ]),
            events: Rc::clone(&events),
            liveness: RefCell::new(VecDeque::from([Ok(StoreLiveness::Unreachable)])),
            batch_errors: RefCell::new(VecDeque::new()),
            batch_completion_gate: None,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: VecDeque::from([location_with_second_peer(
                1,
                "a",
                "z",
                "tikv-old:20160",
                "tikv-new:20160",
            )]),
        }),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let evidence = transport.evidence_handle();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"recovered-batch-route".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160"]
    );
    assert_eq!(
        events.borrow()[..2],
        [
            ClientEvent::Send("tikv-old:20160".to_owned()),
            ClientEvent::Liveness {
                address: "tikv-old:20160".to_owned(),
                timeout: Duration::from_secs(1),
            },
        ]
    );
    assert_eq!(retry_control.sleeps.borrow().len(), 1);

    let evidence = evidence.snapshot();
    assert_eq!(evidence.batch_attempts, 2);
    assert_eq!(evidence.unary_attempts, 0);
    assert_eq!(
        evidence
            .published_attempts
            .iter()
            .map(|published| published.publication.physical_address())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160"],
        "a recoverable physical-route failure must republish the cache-selected route through BatchCommands"
    );
}
