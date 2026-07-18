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

//! Source-shaped single-region request sender tests.

use std::cell::Cell;

use tidb_proto::KvrpcContext;
use tidb_txnkv::region::{
    Peer, PeerRole, PendingRegionRequest, ReadPolicy, RegionLocation, RegionRouteError,
    RegionSendError, RegionVerId, ReplicaReadMode, SingleRegionRequestSender, Store,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectUnaryFailure {
    kind: &'static str,
    address: String,
    version: u64,
}

impl std::fmt::Display for DirectUnaryFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} at {} version {}",
            self.kind, self.address, self.version
        )
    }
}

impl std::error::Error for DirectUnaryFailure {}

fn location() -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(7, 11, 13),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: vec![Peer {
            id: 17,
            store_id: 19,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: 23,
        }],
        leader_peer_id: Some(17),
        stores: vec![Store {
            id: 19,
            address: "tikv-19:20160".to_owned(),
            epoch: 23,
        }],
    }
}

#[test]
fn final_attachment_preserves_caller_fields_and_propagates_cluster_identity() {
    let mut context = KvrpcContext {
        task_id: 29,
        request_source: "internal_ddl".to_owned(),
        not_fill_cache: true,
        replica_read: true,
        stale_read: true,
        ..KvrpcContext::default()
    };
    context.resolved_locks = vec![31, 37];
    let mut request = PendingRegionRequest::new(location().region, ReadPolicy::default(), context);
    let sender = SingleRegionRequestSender::new(41);
    let calls = Cell::new(0);

    let result = sender
        .send(&location(), &mut request, |address, attached| {
            calls.set(calls.get() + 1);
            assert_eq!(address, "tikv-19:20160");
            assert_eq!(attached.region_id, 7);
            assert_eq!(attached.region_epoch.as_ref().unwrap().conf_ver, 11);
            assert_eq!(attached.region_epoch.as_ref().unwrap().version, 13);
            assert_eq!(attached.peer.as_ref().unwrap().id, 17);
            assert_eq!(attached.peer.as_ref().unwrap().store_id, 19);
            assert_eq!(attached.cluster_id, 41);
            assert_eq!(attached.task_id, 29);
            assert_eq!(attached.request_source, "internal_ddl");
            assert!(attached.not_fill_cache);
            assert_eq!(attached.resolved_locks, [31, 37]);
            assert!(!attached.replica_read);
            assert!(!attached.stale_read);
            Ok::<_, DirectUnaryFailure>("response")
        })
        .unwrap();

    assert_eq!(result, "response");
    assert_eq!(calls.get(), 1);
    assert!(request.is_attached());
}

#[test]
fn typed_direct_unary_failure_is_preserved_and_context_is_attached_once() {
    let mut request = PendingRegionRequest::new(
        location().region,
        ReadPolicy::default(),
        KvrpcContext::default(),
    );
    let sender = SingleRegionRequestSender::new(41);
    let calls = Cell::new(0);

    let expected = DirectUnaryFailure {
        kind: "connection",
        address: "tikv-19:20160".to_owned(),
        version: 47,
    };
    let error = sender
        .send(&location(), &mut request, |_, _| {
            calls.set(calls.get() + 1);
            Err::<(), _>(expected.clone())
        })
        .unwrap_err();
    assert_eq!(error, RegionSendError::DirectUnary(expected));
    assert!(request.is_attached());

    let error = sender
        .send(&location(), &mut request, |_, _| {
            calls.set(calls.get() + 1);
            Ok::<_, DirectUnaryFailure>(())
        })
        .unwrap_err();
    assert_eq!(
        error,
        RegionSendError::Route(RegionRouteError::ContextAlreadyAttached)
    );
    assert_eq!(calls.get(), 1);
}

#[test]
fn stale_task_epoch_fails_before_context_mutation_or_rpc() {
    let mut request = PendingRegionRequest::new(
        RegionVerId::new(7, 11, 12),
        ReadPolicy::default(),
        KvrpcContext {
            task_id: 99,
            ..KvrpcContext::default()
        },
    );
    let calls = Cell::new(0);
    let error = SingleRegionRequestSender::new(41)
        .send(&location(), &mut request, |_, _| {
            calls.set(calls.get() + 1);
            Ok::<_, DirectUnaryFailure>(())
        })
        .unwrap_err();

    assert_eq!(
        error,
        RegionSendError::Route(RegionRouteError::StaleRequestEpoch {
            expected: RegionVerId::new(7, 11, 12),
            actual: RegionVerId::new(7, 11, 13),
        })
    );
    assert_eq!(calls.get(), 0);
    assert!(!request.is_attached());
    assert_eq!(request.context().task_id, 99);
    assert_eq!(request.context().region_id, 0);
}

#[test]
fn missing_cluster_id_fails_before_context_mutation_or_rpc() {
    let context = KvrpcContext {
        task_id: 101,
        request_source: "cluster-id-regression".to_owned(),
        ..KvrpcContext::default()
    };
    let mut request =
        PendingRegionRequest::new(location().region, ReadPolicy::default(), context.clone());
    let calls = Cell::new(0);

    let error = SingleRegionRequestSender::new(0)
        .send(&location(), &mut request, |_, _| {
            calls.set(calls.get() + 1);
            Ok::<_, DirectUnaryFailure>(())
        })
        .unwrap_err();

    assert_eq!(
        error,
        RegionSendError::Route(RegionRouteError::MissingClusterId)
    );
    assert_eq!(calls.get(), 0);
    assert!(!request.is_attached());
    assert_eq!(request.context(), &context);
}

#[test]
fn attachment_preserves_every_peer_role_and_witness_flag() {
    for (role, encoded, is_witness) in [
        (PeerRole::Voter, 0, false),
        (PeerRole::Learner, 1, true),
        (PeerRole::IncomingVoter, 2, false),
        (PeerRole::DemotingVoter, 3, true),
    ] {
        let mut candidate = location();
        candidate.peers[0].role = role;
        candidate.peers[0].is_witness = is_witness;
        let mut request = PendingRegionRequest::new(
            candidate.region,
            ReadPolicy::default(),
            KvrpcContext::default(),
        );

        SingleRegionRequestSender::new(41)
            .send(&candidate, &mut request, |_, context| {
                let peer = context.peer.as_ref().unwrap();
                assert_eq!(peer.role, encoded);
                assert_eq!(peer.is_witness, is_witness);
                Ok::<_, DirectUnaryFailure>(())
            })
            .unwrap();
    }
}

#[test]
fn constructor_canonicalizes_policy_owned_context_fields() {
    let contradictory = KvrpcContext {
        replica_read: true,
        stale_read: true,
        task_id: 53,
        ..KvrpcContext::default()
    };
    let leader = PendingRegionRequest::new(location().region, ReadPolicy::default(), contradictory);
    assert!(!leader.context().replica_read);
    assert!(!leader.context().stale_read);
    assert_eq!(leader.context().task_id, 53);
    assert_eq!(leader.read_policy(), ReadPolicy::default());
    assert_eq!(leader.expected_region(), location().region);

    let follower_policy = ReadPolicy {
        mode: ReplicaReadMode::Follower,
        stale_read: true,
        ..ReadPolicy::default()
    };
    let follower =
        PendingRegionRequest::new(location().region, follower_policy, KvrpcContext::default());
    assert!(follower.context().replica_read);
    assert!(follower.context().stale_read);
    assert_eq!(follower.read_policy(), follower_policy);
}
