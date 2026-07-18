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
    RegionVerId, SingleRegionRequestSender, Store,
};

fn location() -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(7, 11, 13),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: vec![Peer {
            id: 17,
            store_id: 19,
            role: PeerRole::Voter,
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
            Ok("response")
        })
        .unwrap();

    assert_eq!(result, "response");
    assert_eq!(calls.get(), 1);
    assert!(request.is_attached());
}

#[test]
fn context_is_attached_once_even_when_rpc_fails() {
    let mut request = PendingRegionRequest::new(
        location().region,
        ReadPolicy::default(),
        KvrpcContext::default(),
    );
    let sender = SingleRegionRequestSender::new(41);
    let calls = Cell::new(0);

    let error = sender
        .send(&location(), &mut request, |_, _| -> Result<(), String> {
            calls.set(calls.get() + 1);
            Err("transport".to_owned())
        })
        .unwrap_err();
    assert_eq!(error, RegionRouteError::Rpc("transport".to_owned()));
    assert!(request.is_attached());

    let error = sender
        .send(&location(), &mut request, |_, _| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error, RegionRouteError::ContextAlreadyAttached);
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
            Ok(())
        })
        .unwrap_err();

    assert_eq!(
        error,
        RegionRouteError::StaleRequestEpoch {
            expected: RegionVerId::new(7, 11, 12),
            actual: RegionVerId::new(7, 11, 13),
        }
    );
    assert_eq!(calls.get(), 0);
    assert!(!request.is_attached());
    assert_eq!(request.context.task_id, 99);
    assert_eq!(request.context.region_id, 0);
}
