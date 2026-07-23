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

//! Real PD/TiKV proof for the typed BatchCommands transaction RPC leaf.

use std::time::Duration;

use tidb_pd_client::PdClient;
use tidb_proto::{
    KvrpcAssertion, KvrpcAssertionLevel, KvrpcBatchRollbackRequest, KvrpcCommitRequest,
    KvrpcCommitRole, KvrpcContext, KvrpcGetRequest, KvrpcMutation, KvrpcOp, KvrpcPeer,
    KvrpcPrewriteRequest, KvrpcRegionEpoch, KvrpcRequestOrigin,
};
use tidb_txnkv::region::{RegionLoader, RegionLocation};
use tidb_txnkv::rpc::{TonicCoprocessorClient, TransactionBatchPublication, UnaryCallContext};
use tidb_txnkv::{DirectUnaryClient, PdRegionLoader};

const RPC_TIMEOUT: Duration = Duration::from_secs(20);

struct RealRoute {
    address: String,
    context: KvrpcContext,
}

fn route_for_key(loader: &mut PdRegionLoader, key: &[u8], cluster_id: u64) -> RealRoute {
    let location = loader
        .load_region(key)
        .expect("load real key region from PD");
    route_from_location(location, cluster_id)
}

fn route_from_location(location: RegionLocation, cluster_id: u64) -> RealRoute {
    let leader_id = location
        .leader_peer_id
        .expect("PD region must have a leader for transaction RPC");
    let leader = location
        .peers
        .iter()
        .find(|peer| peer.id == leader_id)
        .expect("PD leader must be present in region peers");
    let store = location
        .stores
        .iter()
        .find(|store| store.id == leader.store_id)
        .expect("PD leader store must have a resolved TiKV address");
    RealRoute {
        address: store.address.clone(),
        context: KvrpcContext {
            region_id: location.region.id,
            region_epoch: Some(KvrpcRegionEpoch {
                conf_ver: location.region.epoch.conf_ver,
                version: location.region.epoch.version,
            }),
            peer: Some(KvrpcPeer {
                id: leader.id,
                store_id: leader.store_id,
                role: leader.role.as_i32(),
                is_witness: leader.is_witness,
            }),
            request_source: "realtikv_transaction_rpc".to_owned(),
            request_origin: KvrpcRequestOrigin::TiDb as i32,
            cluster_id,
            ..KvrpcContext::default()
        },
    }
}

fn assert_publication(
    command: &str,
    publication: &TransactionBatchPublication,
    route: &RealRoute,
    start_ts: u64,
    commit_ts: u64,
) {
    assert_ne!(publication.request_id(), 0);
    assert_eq!(publication.physical_address(), route.address);
    assert_ne!(publication.physical_channel_version(), 0);
    assert_ne!(publication.batch_stream_generation(), 0);
    assert_eq!(publication.forwarded_host(), None);
    println!(
        "realtikv_transaction_rpc command={command} tag={} request_id={} physical_address={} channel_version={} stream_generation={} region_id={} peer_id={} start_ts={} commit_ts={}",
        publication.tag().field_number(),
        publication.request_id(),
        publication.physical_address(),
        publication.physical_channel_version(),
        publication.batch_stream_generation(),
        route.context.region_id,
        route.context.peer.as_ref().expect("route peer").id,
        start_ts,
        commit_ts,
    );
}

#[test]
#[ignore = "requires run-realtikv-transaction-rpc.sh"]
fn typed_transaction_commands_reach_real_tikv_and_leave_no_lock() {
    let pd_address =
        std::env::var("TXN_RPC_PD_ADDR").expect("runner must provide TXN_RPC_PD_ADDR");
    let pd = PdClient::connect(pd_address, Duration::from_secs(10))
        .expect("connect sole process-owned PD worker");
    let cluster_id = pd.cluster_id();
    assert_ne!(cluster_id, 0);
    let mut loader = PdRegionLoader::from_client(pd.clone());
    let mut client =
        TonicCoprocessorClient::new().expect("construct sole process-owned TiKV transport");

    let start_ts = pd.get_timestamp().expect("allocate real prewrite TSO");
    let committed_key = format!("transaction-rpc-committed-{start_ts}").into_bytes();
    let committed_value = b"real-batchcommands-value".to_vec();
    let committed_route = route_for_key(&mut loader, &committed_key, cluster_id);
    let prewrite = KvrpcPrewriteRequest {
        mutations: vec![KvrpcMutation {
            op: KvrpcOp::Put as i32,
            key: committed_key.clone(),
            value: committed_value.clone(),
            assertion: KvrpcAssertion::None as i32,
        }],
        primary_lock: committed_key.clone(),
        start_version: start_ts,
        lock_ttl: 3_000,
        txn_size: 1,
        assertion_level: KvrpcAssertionLevel::Strict as i32,
        ..KvrpcPrewriteRequest::default()
    };
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = client
        .begin_transaction_prewrite(
            &committed_route.address,
            None,
            &prewrite,
            &committed_route.context,
            &call,
        )
        .expect("publish real Prewrite through BatchCommands");
    let publication = pending
        .publication()
        .expect("Prewrite publication receipt")
        .clone();
    let prewrite_response = pending
        .complete(&call)
        .expect("drive Prewrite completion")
        .expect("receive typed Prewrite response");
    assert_eq!(prewrite_response.publication, publication);
    assert!(prewrite_response.response.region_error.is_none());
    assert!(prewrite_response.response.errors.is_empty());
    assert_publication("Prewrite", &publication, &committed_route, start_ts, 0);

    let commit_ts = pd.get_timestamp().expect("allocate real commit TSO");
    assert!(commit_ts > start_ts);
    let commit = KvrpcCommitRequest {
        start_version: start_ts,
        keys: vec![committed_key.clone()],
        commit_version: commit_ts,
        commit_role: KvrpcCommitRole::Primary as i32,
        primary_key: committed_key.clone(),
        ..KvrpcCommitRequest::default()
    };
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = client
        .begin_transaction_commit(
            &committed_route.address,
            None,
            &commit,
            &committed_route.context,
            &call,
        )
        .expect("publish real Commit through BatchCommands");
    let publication = pending
        .publication()
        .expect("Commit publication receipt")
        .clone();
    let commit_response = pending
        .complete(&call)
        .expect("drive Commit completion")
        .expect("receive typed Commit response");
    assert_eq!(commit_response.publication, publication);
    assert!(commit_response.response.region_error.is_none());
    assert!(commit_response.response.error.is_none());
    assert_publication(
        "Commit",
        &publication,
        &committed_route,
        start_ts,
        commit_ts,
    );

    let read_ts = pd.get_timestamp().expect("allocate real read TSO");
    assert!(read_ts > commit_ts);
    let get = KvrpcGetRequest {
        key: committed_key,
        version: read_ts,
        need_commit_ts: true,
        ..KvrpcGetRequest::default()
    };
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = client
        .begin_transaction_get(
            &committed_route.address,
            None,
            &get,
            &committed_route.context,
            &call,
        )
        .expect("publish real Get through BatchCommands");
    let publication = pending
        .publication()
        .expect("Get publication receipt")
        .clone();
    let get_response = pending
        .complete(&call)
        .expect("drive Get completion")
        .expect("receive typed Get response");
    assert_eq!(get_response.publication, publication);
    assert!(get_response.response.region_error.is_none());
    assert!(get_response.response.error.is_none());
    assert!(!get_response.response.not_found);
    assert_eq!(get_response.response.value, committed_value);
    assert_publication("Get", &publication, &committed_route, read_ts, commit_ts);

    let rollback_start_ts = pd.get_timestamp().expect("allocate rollback prewrite TSO");
    let rolled_back_key = format!("transaction-rpc-rolled-back-{rollback_start_ts}").into_bytes();
    let rollback_route = route_for_key(&mut loader, &rolled_back_key, cluster_id);
    let prewrite = KvrpcPrewriteRequest {
        mutations: vec![KvrpcMutation {
            op: KvrpcOp::Put as i32,
            key: rolled_back_key.clone(),
            value: b"must-not-survive".to_vec(),
            assertion: KvrpcAssertion::None as i32,
        }],
        primary_lock: rolled_back_key.clone(),
        start_version: rollback_start_ts,
        lock_ttl: 3_000,
        txn_size: 1,
        assertion_level: KvrpcAssertionLevel::Strict as i32,
        ..KvrpcPrewriteRequest::default()
    };
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = client
        .begin_transaction_prewrite(
            &rollback_route.address,
            None,
            &prewrite,
            &rollback_route.context,
            &call,
        )
        .expect("publish rollback probe Prewrite");
    let publication = pending
        .publication()
        .expect("rollback probe Prewrite publication")
        .clone();
    let response = pending
        .complete(&call)
        .expect("drive rollback probe Prewrite")
        .expect("receive rollback probe Prewrite");
    assert!(response.response.region_error.is_none());
    assert!(response.response.errors.is_empty());
    assert_publication(
        "PrewriteRollbackProbe",
        &publication,
        &rollback_route,
        rollback_start_ts,
        0,
    );

    let rollback = KvrpcBatchRollbackRequest {
        start_version: rollback_start_ts,
        keys: vec![rolled_back_key.clone()],
        ..KvrpcBatchRollbackRequest::default()
    };
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = client
        .begin_transaction_batch_rollback(
            &rollback_route.address,
            None,
            &rollback,
            &rollback_route.context,
            &call,
        )
        .expect("publish real BatchRollback");
    let publication = pending
        .publication()
        .expect("BatchRollback publication receipt")
        .clone();
    let rollback_response = pending
        .complete(&call)
        .expect("drive BatchRollback completion")
        .expect("receive typed BatchRollback response");
    assert!(rollback_response.response.region_error.is_none());
    assert!(rollback_response.response.error.is_none());
    assert_publication(
        "BatchRollback",
        &publication,
        &rollback_route,
        rollback_start_ts,
        0,
    );

    let after_rollback_ts = pd.get_timestamp().expect("allocate post-rollback read TSO");
    let get = KvrpcGetRequest {
        key: rolled_back_key,
        version: after_rollback_ts,
        ..KvrpcGetRequest::default()
    };
    let call = UnaryCallContext::with_timeout(RPC_TIMEOUT);
    let mut pending = client
        .begin_transaction_get(
            &rollback_route.address,
            None,
            &get,
            &rollback_route.context,
            &call,
        )
        .expect("publish post-rollback Get");
    let publication = pending
        .publication()
        .expect("post-rollback Get publication")
        .clone();
    let response = pending
        .complete(&call)
        .expect("drive post-rollback Get")
        .expect("receive post-rollback Get");
    assert!(response.response.region_error.is_none());
    assert!(response.response.error.is_none());
    assert!(response.response.not_found);
    assert!(response.response.value.is_empty());
    assert_publication(
        "GetAfterRollback",
        &publication,
        &rollback_route,
        after_rollback_ts,
        0,
    );

    client.close().expect("close sole TiKV transport owner");
    drop(loader);
    pd.shutdown().expect("close sole PD worker owner");
    println!(
        "realtikv_transaction_rpc status=passed cluster_id={cluster_id} start_ts={start_ts} commit_ts={commit_ts} rollback_start_ts={rollback_start_ts}"
    );
}
