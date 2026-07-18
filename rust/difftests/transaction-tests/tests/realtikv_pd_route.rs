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
    QueryResultContext, RequestKeyRange, RequestKeyRanges, RequestType, SelectInput, StoreType,
    TransportRequest, WarningCollector,
};
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::PdRegionLoader;

#[test]
#[ignore = "requires the cleanup-safe Campaign 10 TiUP runner"]
fn pd_only_input_discovers_route_and_reaches_tikv() {
    let pd_address = std::env::var("C10_PD_ADDR")
        .expect("C10_PD_ADDR must be supplied by run-campaign10-realtikv.sh");
    let loader = PdRegionLoader::connect(pd_address, Duration::from_secs(5))
        .expect("bootstrap live PD region loader");
    let region_cache = RegionCache::new(loader);
    let client = TonicCoprocessorClient::new().expect("construct live unary client");
    let transport = DirectUnaryQueryTransport::new(
        client,
        region_cache,
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(5),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_distsql::FixedTimestampSource(1 << 18),
    )
    .expect("construct PD-backed direct-unary transport");
    let mut runtime = InjectedQueryRuntime::new(transport);
    let metadata = KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(Vec::new()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
            RequestKeyRange {
                start_key: Vec::new(),
                end_key: Vec::new(),
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
            &TransportRequest::new(
                metadata,
                std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
            ),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .expect("bind live PD-backed DistSQL result");

    let error = result
        .next_raw()
        .expect_err("TiKV must reject the deliberately empty DAG body");
    let message = error.to_string();
    assert!(
        message.contains("coprocessor other error"),
        "expected an application-level TiKV response, got: {message}"
    );
    assert!(
        !message.contains("unary client failed")
            && !message.contains("connection")
            && !message.contains("region loader"),
        "the live proof must cross PD discovery and TiKV transport: {message}"
    );
}
