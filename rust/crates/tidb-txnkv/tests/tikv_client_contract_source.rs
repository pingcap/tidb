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

use std::collections::BTreeMap;
use std::time::Duration;

use tidb_proto::{KvrpcContext, KvrpcSourceStmt};
use tidb_txnkv::region::StoreLiveness;
use tidb_txnkv::{
    endpoint_type, inject_source_stmt, map_replica_read_type, BackoffMetadata,
    ClientReplicaReadType, DirectUnaryClient, DirectUnaryClientError, DirectUnaryRequest,
    DirectUnaryResponse, DriverDefaults, DriverOptions, EndpointType, PdClientConfig,
    SecurityConfig, TikvClientConfig, TikvDriverConfig, TraceInfo, TxnLocalLatchesConfig,
};

#[derive(Default)]
struct RecordingUnaryClient {
    calls: Vec<(String, DirectUnaryRequest, Duration)>,
    exact_closes: Vec<(String, u64)>,
}

impl DirectUnaryClient for RecordingUnaryClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.calls
            .push((address.to_owned(), request.clone(), timeout));
        Ok(DirectUnaryResponse {
            encoded_response: b"raw-response".to_vec(),
        })
    }

    fn close_address(&mut self, _address: &str) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.exact_closes.push((address.to_owned(), version));
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        Ok(StoreLiveness::Unknown)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

#[test]
fn unary_client_contract_requires_exact_generation_close_and_liveness() {
    let mut client = RecordingUnaryClient::default();
    client.close_address_version("tikv-1:20160", 7).unwrap();
    assert_eq!(client.exact_closes, [("tikv-1:20160".to_owned(), 7)]);
    assert_eq!(
        client
            .liveness("tikv-1:20160", Duration::from_millis(23))
            .unwrap(),
        StoreLiveness::Unknown
    );
}

#[test]
fn unary_client_contract_keeps_address_request_timeout_and_result_separate() {
    // client-go/internal/client/client.go:96-105 Client.SendRequest
    let request = DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Leader,
        replica_read: false,
        stale_read: false,
        input_request_source: "external".to_owned(),
        predicted_read_bytes: 4096,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: KvrpcContext {
            region_id: 42,
            ..KvrpcContext::default()
        },
        encoded_request: b"immutable-cop-request".to_vec(),
    };
    let mut client = RecordingUnaryClient::default();
    let response = client
        .send_request("tikv-1:20160", &request, Duration::from_millis(777))
        .unwrap();
    assert_eq!(request.encoded_request, b"immutable-cop-request");
    assert_eq!(request.context.region_id, 42);
    assert_eq!(request.predicted_read_bytes, 4096);
    assert_eq!(response.encoded_response, b"raw-response");
    assert_eq!(
        client.calls,
        [(
            "tikv-1:20160".to_owned(),
            request,
            Duration::from_millis(777),
        )]
    );
}

#[test]
fn replica_and_endpoint_mappings_match_tidb_and_client_go_discriminants() {
    assert_eq!(map_replica_read_type(0), ClientReplicaReadType::Leader);
    assert_eq!(map_replica_read_type(1), ClientReplicaReadType::Follower);
    assert_eq!(map_replica_read_type(2), ClientReplicaReadType::Mixed);
    assert_eq!(map_replica_read_type(3), ClientReplicaReadType::Mixed);
    assert_eq!(map_replica_read_type(4), ClientReplicaReadType::Mixed);
    assert_eq!(map_replica_read_type(5), ClientReplicaReadType::Learner);
    assert_eq!(
        map_replica_read_type(6),
        ClientReplicaReadType::PreferLeader
    );
    assert_eq!(map_replica_read_type(255), ClientReplicaReadType::Leader);

    assert_eq!(endpoint_type(0, false), EndpointType::TiKv);
    assert_eq!(endpoint_type(1, false), EndpointType::TiFlash);
    assert_eq!(endpoint_type(1, true), EndpointType::TiFlashCompute);
    assert_eq!(endpoint_type(2, false), EndpointType::TiDb);
    assert_eq!(endpoint_type(255, true), EndpointType::TiKv);
}

#[test]
fn defaults_are_cloned_before_options_and_pd_labels_are_projected() {
    let defaults = DriverDefaults {
        security: SecurityConfig {
            cluster_ssl_ca: "original".to_owned(),
            ..SecurityConfig::default()
        },
        tikv_client: TikvClientConfig {
            grpc_keep_alive_time_secs: 10,
            grpc_keep_alive_timeout_secs: 3,
        },
        pd_client: PdClientConfig {
            server_timeout_secs: 5,
        },
        txn_local_latches: TxnLocalLatchesConfig {
            enabled: true,
            capacity: 1024,
        },
        enable_forwarding: true,
    };
    let effective = TikvDriverConfig::from_defaults(
        &defaults,
        DriverOptions {
            security: Some(SecurityConfig {
                cluster_ssl_ca: "test".to_owned(),
                ..SecurityConfig::default()
            }),
            ..DriverOptions::default()
        },
    );
    assert_eq!(effective.security.cluster_ssl_ca, "test");
    assert_eq!(effective.tikv_client, defaults.tikv_client);
    assert_eq!(effective.pd_client, defaults.pd_client);
    assert_eq!(effective.txn_local_latches, defaults.txn_local_latches);
    assert_eq!(defaults.security.cluster_ssl_ca, "original");

    let labels = BTreeMap::from([
        ("keyspace_id".to_owned(), "42".to_owned()),
        ("keyspace_name".to_owned(), "ks".to_owned()),
    ]);
    let pd = effective.pd_options(labels.clone());
    assert_eq!(pd.max_receive_message_size, i32::MAX);
    assert_eq!(pd.grpc_keep_alive_time_secs, 10);
    assert_eq!(pd.grpc_keep_alive_timeout_secs, 3);
    assert_eq!(pd.server_timeout_secs, 5);
    assert!(pd.enable_forwarding);
    assert_eq!(pd.metrics_labels, labels);
}

#[test]
fn trace_injection_is_a_nil_noop_and_preserves_existing_source_stmt_fields() {
    let mut context = KvrpcContext::default();
    inject_source_stmt(&mut context, None);
    assert_eq!(context.source_stmt, None);

    inject_source_stmt(
        &mut context,
        Some(&TraceInfo {
            connection_id: 123,
            session_alias: "alias123".to_owned(),
        }),
    );
    assert_eq!(context.source_stmt.as_ref().unwrap().connection_id, 123);
    assert_eq!(
        context.source_stmt.as_ref().unwrap().session_alias,
        "alias123"
    );

    inject_source_stmt(
        &mut context,
        Some(&TraceInfo {
            connection_id: 456,
            session_alias: String::new(),
        }),
    );
    assert_eq!(context.source_stmt.as_ref().unwrap().connection_id, 456);
    assert_eq!(context.source_stmt.as_ref().unwrap().session_alias, "");

    context.source_stmt = Some(KvrpcSourceStmt {
        start_ts: 11,
        connection_id: 0,
        stmt_id: 17,
        session_alias: String::new(),
    });
    inject_source_stmt(
        &mut context,
        Some(&TraceInfo {
            connection_id: 0,
            session_alias: "alias456".to_owned(),
        }),
    );
    let source_stmt = context.source_stmt.unwrap();
    assert_eq!(source_stmt.start_ts, 11);
    assert_eq!(source_stmt.stmt_id, 17);
    assert_eq!(source_stmt.connection_id, 0);
    assert_eq!(source_stmt.session_alias, "alias456");
}

#[test]
fn backoff_metadata_records_observations_without_scheduling_or_sleeping() {
    let mut backoff = BackoffMetadata::new(2_000);
    backoff.observe("region_miss", 10);
    backoff.observe("region_miss", 20);
    backoff.observe("txn_lock_fast", 5);
    assert_eq!(backoff.max_sleep_ms(), 2_000);
    assert_eq!(backoff.times()["region_miss"], 2);
    assert_eq!(backoff.sleep_ms()["region_miss"], 30);
    assert_eq!(backoff.total_sleep_ms(), 35);
}
