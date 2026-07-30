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

//! Choosing another store when one is unreachable: the alternate a failed
//! store's region reselects and keeps for the next query, and the later bound
//! regions one store's failure stales without reordering them.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn unreachable_store_reselects_an_alternate_and_promotes_it_for_the_next_query() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "tikv-old:20160",
                9,
                DirectUnaryTransportClass::Connection,
                None,
            )),
            Ok(response(b"alternate")),
            Ok(response(b"promoted")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-old:20160",
            "tikv-new:20160",
        )],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let request = transport_request(metadata("a", "z"));
    let mut first = select_result(&mut runtime, &request);

    assert_eq!(first.next_raw().unwrap(), Some(b"alternate".to_vec()));
    assert_eq!(first.next_raw().unwrap(), None);
    drop(first);
    let mut second = select_result(&mut runtime, &request);
    assert_eq!(second.next_raw().unwrap(), Some(b"promoted".to_vec()));
    assert_eq!(second.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160", "tikv-new:20160"]
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
}

#[test]
fn one_store_failure_stales_later_bound_regions_without_reordering_them() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let shared_leader = Store {
        id: 201,
        address: "tikv-dead:20160".to_owned(),
        epoch: 7,
    };
    let first = RegionLocation {
        region: RegionVerId::new(1, 1, 2),
        start_key: b"a".to_vec(),
        end_key: b"m".to_vec(),
        peers: vec![
            Peer {
                id: 101,
                store_id: 201,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
            Peer {
                id: 102,
                store_id: 202,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
        ],
        leader_peer_id: Some(101),
        stores: vec![
            shared_leader.clone(),
            Store {
                id: 202,
                address: "tikv-first-alternate:20160".to_owned(),
                epoch: 7,
            },
        ],
        ..RegionLocation::default()
    };
    let second = RegionLocation {
        region: RegionVerId::new(2, 1, 2),
        start_key: b"m".to_vec(),
        end_key: b"z".to_vec(),
        peers: vec![
            Peer {
                id: 201,
                store_id: 201,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
            Peer {
                id: 202,
                store_id: 203,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
        ],
        leader_peer_id: Some(201),
        stores: vec![
            shared_leader,
            Store {
                id: 203,
                address: "tikv-second-alternate:20160".to_owned(),
                epoch: 7,
            },
        ],
        ..RegionLocation::default()
    };
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "tikv-dead:20160",
                4,
                DirectUnaryTransportClass::Connection,
                None,
            )),
            Ok(response(b"first")),
            Ok(response(b"second")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [first, second],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control,
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"first".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"second".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| (call.region_id, call.address.as_str()))
            .collect::<Vec<_>>(),
        [
            (1, "tikv-dead:20160"),
            (1, "tikv-first-alternate:20160"),
            (2, "tikv-second-alternate:20160"),
        ]
    );
    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, ClientEvent::Send(_)))
            .count(),
        3
    );
}
