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
fn unordered_region_retry_delivers_the_replacement_once() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.keep_order = false;
    request_metadata.concurrency = 1;
    let mut runtime = InjectedQueryRuntime::new(batch_first_transport(
        Arc::clone(&calls),
        [Ok(not_leader(1, Some((102, 202)))), Ok(response(b"fresh"))],
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-1:20160",
            "tikv-2:20160",
        )],
        [true, true],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.read().unwrap().len(), 2);
}

#[test]
fn cached_leader_data_is_not_ready_falls_through_without_reload_or_backoff() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let loader_calls = Arc::new(RwLock::new(Vec::new()));
    let retry_control = Arc::new(RecordingRetryControl::default());
    let initial =
        location_with_second_peer(1, "a", "z", "tikv-leader:20160", "tikv-follower:20160");
    let transport = transport_with_loader_calls_and_config(
        Arc::clone(&calls),
        [Ok(data_is_not_ready()), Ok(response(b"fresh"))],
        [initial],
        9001,
        Arc::clone(&loader_calls),
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
    // Both stores lack this label. Go's mixed policy prefers the leader when
    // label matching is requested and all candidates have the same match result.
    request_metadata
        .match_store_labels
        .push(tidb_distsql::StoreLabel {
            key: "zone".to_owned(),
            value: "test-zone".to_owned(),
        });
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let calls = calls.read().unwrap();
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
        loader_calls.read().unwrap().as_slice(),
        [b"a".to_vec()],
        "leader DataIsNotReady must not invalidate or reload the region"
    );
    assert!(
        retry_control.sleeps.lock().unwrap().is_empty(),
        "DataIsNotReady fallthrough must not back off"
    );
}

#[test]
fn stale_data_not_ready_then_known_leader_retries_one_selector_and_publishes_once() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.replica_read = ReplicaReadType::Leader;
    request_metadata.is_staleness = true;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Arc::clone(&calls),
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

    let calls = calls.read().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(["tikv-leader:20160", "tikv-follower:20160"].contains(&calls[0].address.as_str()));
    assert!(!calls[0].replica_read);
    assert!(calls[0].stale_read);
    let leader_was_attempted = calls[0].address == "tikv-leader:20160";
    assert_eq!(
        calls[1].address,
        if leader_was_attempted {
            "tikv-follower:20160"
        } else {
            "tikv-leader:20160"
        }
    );
    assert_eq!(calls[1].replica_read, leader_was_attempted);
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
    let calls = Arc::new(RwLock::new(Vec::new()));
    let loader_calls = Arc::new(RwLock::new(Vec::new()));
    let first = location_with_second_peer(1, "a", "z", "tikv-old:20160", "tikv-new:20160");
    let retry_control = Arc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Arc::clone(&calls),
        [Ok(not_leader(1, Some((102, 202)))), Ok(response(b"fresh"))],
        [first],
        9001,
        Arc::clone(&loader_calls),
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
        loader_calls.read().unwrap().as_slice(),
        [b"a".to_vec()],
        "known-leader retry must use the exact cache update without PD reload"
    );
    assert_eq!(calls.read().unwrap()[0].address, "tikv-old:20160");
    assert_eq!(calls.read().unwrap()[1].address, "tikv-new:20160");
    assert_eq!(calls.read().unwrap()[1].peer_id, 102);
    assert_eq!(calls.read().unwrap()[1].store_id, 202);
    assert!(retry_control.sleeps.lock().unwrap().is_empty());
}

#[test]
fn batch_known_leader_region_error_republishes_the_recovered_route() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let loader_calls = Arc::new(RwLock::new(Vec::new()));
    let batch_begins = Arc::new(AtomicUsize::new(0));
    let transport = DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls: Arc::clone(&calls),
            responses: VecDeque::from([
                Ok(not_leader(1, Some((102, 202)))),
                Ok(response(b"fresh-batch-route")),
            ]),
            events: Arc::new(RwLock::new(Vec::new())),
            liveness: RwLock::new(VecDeque::new()),
            batch_errors: RwLock::new(VecDeque::new()),
            batch_ready_immediately: RwLock::new(VecDeque::new()),
            batch_begin_count: Some(Arc::clone(&batch_begins)),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Arc::clone(&loader_calls),
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
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"fresh-batch-route".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(loader_calls.read().unwrap().as_slice(), [b"a".to_vec()]);
    assert_eq!(
        calls
            .read()
            .unwrap()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160"]
    );

    assert_eq!(batch_begins.load(Ordering::SeqCst), 2);
}

#[test]
fn batch_connection_failure_republishes_the_cache_recovered_route() {
    let calls = Arc::new(RwLock::new(Vec::new()));
    let events = Arc::new(RwLock::new(Vec::new()));
    let retry_control = Arc::new(RecordingRetryControl::default());
    let batch_begins = Arc::new(AtomicUsize::new(0));
    let transport = DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls: Arc::clone(&calls),
            responses: VecDeque::from([
                Err(connection_failure(
                    "tikv-old:20160",
                    9,
                    DirectUnaryTransportClass::Connection,
                    None,
                )),
                Ok(response(b"recovered-batch-route")),
            ]),
            events: Arc::clone(&events),
            liveness: RwLock::new(VecDeque::from([Ok(StoreLiveness::Unreachable)])),
            batch_errors: RwLock::new(VecDeque::new()),
            batch_ready_immediately: RwLock::new(VecDeque::new()),
            batch_begin_count: Some(Arc::clone(&batch_begins)),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Arc::new(RwLock::new(Vec::new())),
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
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"recovered-batch-route".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .read()
            .unwrap()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160"]
    );
    assert_eq!(
        events.read().unwrap()[..2],
        [
            ClientEvent::Send("tikv-old:20160".to_owned()),
            ClientEvent::Liveness {
                address: "tikv-old:20160".to_owned(),
                timeout: Duration::from_secs(1),
            },
        ]
    );
    assert_eq!(retry_control.sleeps.lock().unwrap().len(), 1);

    assert_eq!(batch_begins.load(Ordering::SeqCst), 2);
}
