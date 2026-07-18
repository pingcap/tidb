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

#![allow(missing_docs)]

use std::time::Duration;

use tidb_distsql::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryRequest, DirectUnaryResponse,
    UnaryCallContext,
};
use tidb_txnkv::region::{
    LeaderRequest, PeerRole, RegionAttempt, RegionVerId, ReplicaReadMode, StoreLiveness,
};
use tidb_txnkv::{ClientReplicaReadType, EndpointType};

#[derive(Default)]
struct RecordingClient {
    access_path: Vec<String>,
}

impl DirectUnaryClient for RecordingClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        _timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.access_path.push(path(address, None, request));
        Ok(DirectUnaryResponse::default())
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.send_request_with_route(address, None, request, call)
    }

    fn send_request_with_route(
        &mut self,
        address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        if call.cancellation().is_cancelled() {
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        self.access_path
            .push(path(address, forwarded_host, request));
        Ok(DirectUnaryResponse::default())
    }

    fn close_address(&mut self, _address: &str) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn close_address_version(
        &mut self,
        _address: &str,
        _version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        Ok(StoreLiveness::Reachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

fn path(address: &str, forwarded_host: Option<&str>, request: &DirectUnaryRequest) -> String {
    match forwarded_host {
        Some(target) => format!(
            "{{addr: {address}, replica-read: {}, stale-read: {}, forward_addr: {target}}}",
            request.replica_read, request.stale_read
        ),
        None => format!(
            "{{addr: {address}, replica-read: {}, stale-read: {}}}",
            request.replica_read, request.stale_read
        ),
    }
}

fn attempt(peer_id: u64, store_id: u64) -> RegionAttempt {
    RegionAttempt {
        region: RegionVerId::new(1, 1, 1),
        peer_id,
        store_id,
        address: format!("store{store_id}"),
        store_epoch: 1,
    }
}

fn route(target_store: u64, proxy_store: Option<u64>) -> LeaderRequest {
    LeaderRequest {
        attempt: attempt(target_store, target_store),
        proxy: proxy_store.map(|store| attempt(store, store)),
        role: PeerRole::Voter,
        is_witness: false,
        replica_read: false,
        stale_read: false,
        cached_leader: true,
        read_mode: ReplicaReadMode::Leader,
    }
}

fn request() -> DirectUnaryRequest {
    DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Leader,
        replica_read: false,
        stale_read: false,
        input_request_source: "campaign14_proxy_paths".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: tidb_proto::KvrpcContext::default(),
        encoded_request: vec![1],
    }
}

fn run(routes: &[LeaderRequest]) -> Vec<String> {
    let mut client = RecordingClient::default();
    let request = request();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
    for route in routes {
        client
            .send_request_with_route(
                route.dispatch_address(),
                route.forwarded_host(),
                &request,
                &call,
            )
            .unwrap();
    }
    client.access_path
}

#[test]
fn all_source_proxy_access_path_rows_keep_target_and_connection_distinct() {
    // client-go/internal/locate/replica_selector_test.go:2566
    let cases = [
        (
            vec![route(1, None), route(1, Some(2))],
            vec![
                "{addr: store1, replica-read: false, stale-read: false}",
                "{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
            ],
        ),
        (
            vec![route(1, Some(2))],
            vec!["{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}"],
        ),
        (
            vec![route(1, None), route(1, Some(2)), route(1, Some(3))],
            vec![
                "{addr: store1, replica-read: false, stale-read: false}",
                "{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
                "{addr: store3, replica-read: false, stale-read: false, forward_addr: store1}",
            ],
        ),
        (
            vec![
                route(1, None),
                route(1, Some(2)),
                route(2, None),
                route(2, Some(3)),
            ],
            vec![
                "{addr: store1, replica-read: false, stale-read: false}",
                "{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
                "{addr: store2, replica-read: false, stale-read: false}",
                "{addr: store3, replica-read: false, stale-read: false, forward_addr: store2}",
            ],
        ),
        (
            vec![route(1, None), route(1, Some(2)), route(1, Some(3))],
            vec![
                "{addr: store1, replica-read: false, stale-read: false}",
                "{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
                "{addr: store3, replica-read: false, stale-read: false, forward_addr: store1}",
            ],
        ),
        (
            vec![route(1, None), route(2, None)],
            vec![
                "{addr: store1, replica-read: false, stale-read: false}",
                "{addr: store2, replica-read: false, stale-read: false}",
            ],
        ),
        (
            vec![route(1, None), route(1, Some(2)), route(3, None)],
            vec![
                "{addr: store1, replica-read: false, stale-read: false}",
                "{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
                "{addr: store3, replica-read: false, stale-read: false}",
            ],
        ),
        (
            vec![route(1, None), route(2, None), route(2, Some(3))],
            vec![
                "{addr: store1, replica-read: false, stale-read: false}",
                "{addr: store2, replica-read: false, stale-read: false}",
                "{addr: store3, replica-read: false, stale-read: false, forward_addr: store2}",
            ],
        ),
    ];

    for (routes, expected) in cases {
        assert_eq!(run(&routes), expected);
    }
}

#[test]
fn cancellation_wins_before_a_forwarded_dispatch_is_observed() {
    // client-go/internal/locate/region_request_test.go:310,350,798
    let route = route(1, Some(2));
    let mut client = RecordingClient::default();
    let request = request();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
    call.cancellation().cancel();

    let error = client
        .send_request_with_route(
            route.dispatch_address(),
            route.forwarded_host(),
            &request,
            &call,
        )
        .unwrap_err();
    assert_eq!(error.kind(), "caller_cancelled");
    assert!(client.access_path.is_empty());
}
