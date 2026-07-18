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
    DirectUnaryQueryTransport, DirectUnaryRuntimeConfig, InjectedQueryRuntime, KvRequestMetadata,
    QueryResultContext, RegionTaskEpoch, RegionTaskPeer, RegionTaskTopology, RequestKeyRange,
    RequestKeyRanges, RequestType, ResolvedRegionRoute, SelectInput, StoreType, TransportRequest,
    WarningCollector,
};
use tidb_txnkv::rpc::TonicCoprocessorClient;

fn required_u64(name: &str) -> u64 {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be supplied by run-campaign09-realtikv.sh"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
}

fn required_hex(name: &str) -> Vec<u8> {
    let value = std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be supplied by run-campaign09-realtikv.sh"));
    if value == "-" {
        return Vec::new();
    }
    assert!(
        value.len().is_multiple_of(2),
        "{name} must have even length"
    );
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("PD hex key must be UTF-8");
            u8::from_str_radix(pair, 16).expect("PD key must be hexadecimal")
        })
        .collect()
}

#[test]
#[ignore = "requires the cleanup-safe Campaign 09 TiUP runner"]
fn realtikv_unary_distql_chain_reaches_tikv() {
    let address = std::env::var("C09_TIKV_ADDR")
        .expect("C09_TIKV_ADDR must be supplied by run-campaign09-realtikv.sh");
    let region_id = required_u64("C09_REGION_ID");
    let region_epoch = RegionTaskEpoch {
        conf_ver: required_u64("C09_REGION_CONF_VER"),
        version: required_u64("C09_REGION_VERSION"),
    };
    let peer = RegionTaskPeer {
        id: required_u64("C09_PEER_ID"),
        store_id: required_u64("C09_STORE_ID"),
        role: i32::try_from(required_u64("C09_PEER_ROLE")).expect("peer role fits i32"),
        is_witness: std::env::var("C09_PEER_IS_WITNESS")
            .expect("C09_PEER_IS_WITNESS must be supplied by the runner")
            .parse()
            .expect("C09_PEER_IS_WITNESS must be true or false"),
    };
    let region_start = required_hex("C09_REGION_START_HEX");
    let region_end = required_hex("C09_REGION_END_HEX");
    let route = ResolvedRegionRoute {
        topology: RegionTaskTopology {
            region_id,
            region_epoch: Some(region_epoch),
            peer: Some(peer),
            // The runner selects the fresh region containing the empty key
            // and passes its exact PD-published encoded boundaries.
            start_key: region_start.clone(),
            end_key: region_end.clone(),
            ..RegionTaskTopology::default()
        },
        address,
    };
    let client = TonicCoprocessorClient::new().expect("construct live unary client");
    let transport = DirectUnaryQueryTransport::new(
        client,
        [route],
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(5),
            cluster_id: required_u64("C09_CLUSTER_ID"),
            ..DirectUnaryRuntimeConfig::default()
        },
    )
    .expect("construct checked single-region transport");
    let mut runtime = InjectedQueryRuntime::new(transport);
    let metadata = KvRequestMetadata {
        request_type: RequestType::Dag,
        // An empty DAG body is deliberately rejected by TiKV after the RPC
        // succeeds. That application-level error proves the exact DistSQL ->
        // DirectUnary -> tikvpb.Tikv/Coprocessor chain without pretending this
        // campaign already owns the tipb DAG request encoder.
        data: Some(Vec::new()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
            RequestKeyRange {
                start_key: region_start,
                end_key: region_end,
            },
        ])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: 1,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..KvRequestMetadata::default()
    };
    let mut result = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(metadata),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .expect("bind live DistSQL result");

    let error = result
        .next_raw()
        .expect_err("TiKV must reject the deliberately empty DAG body");
    let message = error.to_string();
    assert!(
        message.contains("coprocessor other error"),
        "expected an application-level TiKV response, got: {message}"
    );
    assert!(
        !message.contains("unary client failed") && !message.contains("connection"),
        "the live proof must cross transport successfully: {message}"
    );
}
