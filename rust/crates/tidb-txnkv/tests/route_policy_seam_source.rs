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

#![allow(missing_docs)]

use tidb_txnkv::region::{
    LeaderRequest, PeerRole, RegionAttempt, RegionVerId, ReplicaReadMode, RouteFeedback,
    RouteOutcome,
};

fn attempt(peer_id: u64, store_id: u64, address: &str, store_epoch: u64) -> RegionAttempt {
    RegionAttempt {
        region: RegionVerId::new(91, 7, 12),
        peer_id,
        store_id,
        address: address.to_owned(),
        store_epoch,
    }
}

fn request(proxy: Option<RegionAttempt>) -> LeaderRequest {
    LeaderRequest {
        attempt: attempt(11, 101, "leader:20160", 3),
        proxy,
        role: PeerRole::Voter,
        is_witness: false,
        replica_read: false,
        stale_read: false,
        cached_leader: true,
        read_mode: ReplicaReadMode::Leader,
    }
}

#[test]
fn direct_route_uses_the_logical_target_as_its_physical_dispatch() {
    let route = request(None);

    assert_eq!(route.target(), &route.attempt);
    assert_eq!(route.proxy(), None);
    assert_eq!(route.dispatch_attempt(), route.target());
    assert_eq!(route.dispatch_address(), "leader:20160");
    assert_eq!(route.forwarded_host(), None);

    let feedback = RouteFeedback::from_request(&route, RouteOutcome::Success);
    assert_eq!(feedback.target(), route.target());
    assert_eq!(feedback.proxy(), None);
    assert_eq!(feedback.dispatch_attempt(), route.target());
    assert_eq!(feedback.outcome(), RouteOutcome::Success);
}

#[test]
fn proxied_route_separates_logical_target_from_physical_dispatch() {
    let proxy = attempt(12, 102, "proxy:20160", 9);
    let route = request(Some(proxy.clone()));

    assert_eq!(route.target().store_id, 101);
    assert_eq!(route.proxy(), Some(&proxy));
    assert_eq!(route.dispatch_attempt(), &proxy);
    assert_eq!(route.dispatch_address(), "proxy:20160");
    assert_eq!(route.forwarded_host(), Some("leader:20160"));

    let feedback = RouteFeedback::from_request(&route, RouteOutcome::Failure);
    assert_eq!(feedback.target(), route.target());
    assert_eq!(feedback.proxy(), Some(&proxy));
    assert_eq!(feedback.dispatch_attempt(), &proxy);
    assert_eq!(feedback.target().store_epoch, 3);
    assert_eq!(feedback.proxy().map(|attempt| attempt.store_epoch), Some(9));
    assert_eq!(feedback.outcome(), RouteOutcome::Failure);
}
