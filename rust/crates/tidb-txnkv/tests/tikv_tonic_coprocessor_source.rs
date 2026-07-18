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

use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use prost::Message;
use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
use tidb_proto::{
    CoprocessorKeyRange, CoprocessorRequest, CoprocessorResponse, KvrpcContext, KvrpcPeer,
    KvrpcRegionEpoch,
};
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryClient, DirectUnaryClientError, DirectUnaryGrpcCode,
    DirectUnaryRequest, DirectUnaryTransportClass, EndpointType,
};

#[derive(Clone)]
struct RecordingTikv {
    requests: Arc<Mutex<Vec<CoprocessorRequest>>>,
    delay: Duration,
    response_size: Option<usize>,
    failure: Option<tonic::Code>,
}

#[tonic::async_trait]
impl Tikv for RecordingTikv {
    async fn coprocessor(
        &self,
        request: tonic::Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if let Some(code) = self.failure {
            return Err(tonic::Status::new(code, "injected remote status"));
        }
        let request = request.into_inner();
        request
            .context
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing request context"))?;
        self.requests.lock().unwrap().push(request.clone());
        let data = self
            .response_size
            .map_or(request.data, |size| vec![0x5a; size]);
        Ok(tonic::Response::new(CoprocessorResponse {
            data,
            ..CoprocessorResponse::default()
        }))
    }
}

struct TestServer {
    address: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TestServer {
    fn start(service: RecordingTikv) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let server = tonic::transport::Server::builder()
                    .add_service(TikvServer::new(service))
                    .serve_with_shutdown(address, async {
                        let _ = shutdown_rx.await;
                    });
                started_tx.send(()).unwrap();
                server.await.unwrap();
            });
        });
        started_rx.recv().unwrap();
        let mut accepting = false;
        for _ in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                accepting = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(accepting, "test gRPC server did not accept connections");
        Self {
            address: address.to_string(),
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn request(data: &[u8]) -> DirectUnaryRequest {
    DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Leader,
        replica_read: false,
        stale_read: false,
        input_request_source: "external_test".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: authoritative_context(),
        encoded_request: CoprocessorRequest {
            tp: 103,
            data: data.to_vec(),
            ranges: vec![CoprocessorKeyRange {
                start: b"sentinel-start".to_vec(),
                end: b"sentinel-end".to_vec(),
            }],
            is_cache_enabled: true,
            cache_if_match_version: 88,
            start_ts: 77,
            paging_size: 16,
            connection_id: 1234,
            connection_alias: "sentinel-alias".to_owned(),
            max_keys_read: 99,
            paging_size_bytes: 4096,
            context: Some(stale_context()),
            ..CoprocessorRequest::default()
        }
        .encode_to_vec(),
    }
}

fn authoritative_context() -> KvrpcContext {
    KvrpcContext {
        region_id: 42,
        region_epoch: Some(KvrpcRegionEpoch {
            conf_ver: 7,
            version: 8,
        }),
        peer: Some(KvrpcPeer {
            id: 9,
            store_id: 10,
            role: 0,
            is_witness: false,
        }),
        priority: 2,
        isolation_level: 1,
        not_fill_cache: true,
        replica_read: true,
        stale_read: true,
        request_source: "external_campaign09".to_owned(),
        cluster_id: 11,
        ..KvrpcContext::default()
    }
}

fn stale_context() -> KvrpcContext {
    KvrpcContext {
        region_id: 999,
        region_epoch: Some(KvrpcRegionEpoch {
            conf_ver: 998,
            version: 997,
        }),
        peer: Some(KvrpcPeer {
            id: 996,
            store_id: 995,
            role: 1,
            is_witness: true,
        }),
        priority: 1,
        isolation_level: 2,
        replica_read: false,
        stale_read: false,
        request_source: "stale-sentinel".to_owned(),
        cluster_id: 994,
        ..KvrpcContext::default()
    }
}

#[test]
fn unary_rpc_attaches_context_once_reuses_address_and_recreates_after_close() {
    // client-go/internal/client/client_test.go:68 TestConn
    // client-go/internal/client/client_test.go:103 TestGetConnAfterClose
    // client-go/tikvrpc/tikvrpc_test.go:124 TestAttachContextSetsRequestContext
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::start(RecordingTikv {
        requests: Arc::clone(&requests),
        delay: Duration::ZERO,
        response_size: None,
        failure: None,
    });
    let mut client = TonicCoprocessorClient::new().unwrap();

    for data in [b"first".as_slice(), b"second".as_slice()] {
        let raw = client
            .send_request(&server.address, &request(data), Duration::from_secs(2))
            .unwrap();
        let response = CoprocessorResponse::decode(raw.encoded_response.as_slice()).unwrap();
        assert_eq!(response.data, data);
    }
    assert_eq!(client.connection_version(&server.address), Some(1));
    assert_eq!(client.active_address_count(), 1);
    let received = requests.lock().unwrap();
    assert_eq!(received.len(), 2);
    assert!(received.iter().all(|body| {
        body.context.as_ref() == Some(&authoritative_context())
            && body.tp == 103
            && body.start_ts == 77
            && !body.data.is_empty()
            && body.ranges
                == [CoprocessorKeyRange {
                    start: b"sentinel-start".to_vec(),
                    end: b"sentinel-end".to_vec(),
                }]
            && body.is_cache_enabled
            && body.cache_if_match_version == 88
            && body.paging_size == 16
            && body.connection_id == 1234
            && body.connection_alias == "sentinel-alias"
            && body.max_keys_read == 99
            && body.paging_size_bytes == 4096
    }));
    drop(received);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        client
            .send_request(
                &server.address,
                &request(b"async-hosted"),
                Duration::from_secs(2),
            )
            .unwrap();
    });

    client.close_address_version(&server.address, 0).unwrap();
    assert_eq!(client.connection_version(&server.address), Some(1));
    client.close_address(&server.address).unwrap();
    assert_eq!(client.active_address_count(), 0);
    client
        .send_request(&server.address, &request(b"third"), Duration::from_secs(2))
        .unwrap();
    assert_eq!(client.connection_version(&server.address), Some(2));

    client.close().unwrap();
    assert_eq!(client.active_address_count(), 0);
    assert_eq!(
        client
            .send_request(
                &server.address,
                &request(b"after-close"),
                Duration::from_secs(2),
            )
            .unwrap_err(),
        DirectUnaryClientError::Closed
    );
}

#[test]
fn caller_timeout_and_malformed_body_have_typed_fail_closed_results() {
    // client-go/internal/client/client_test.go:1272 TestErrConn
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::start(RecordingTikv {
        requests,
        delay: Duration::from_millis(200),
        response_size: None,
        failure: None,
    });
    let mut client = TonicCoprocessorClient::new().unwrap();

    let zero_timeout = client
        .send_request(&server.address, &request(b"zero"), Duration::ZERO)
        .unwrap_err();
    assert_eq!(zero_timeout.kind(), "timeout");
    assert_eq!(zero_timeout.connection().unwrap().version, 1);
    assert_eq!(
        zero_timeout.transport_class(),
        Some(DirectUnaryTransportClass::LocalDeadline)
    );
    assert_eq!(zero_timeout.grpc_code(), None);
    assert!(!zero_timeout.requires_generation_close());
    assert_eq!(client.connection_version(&server.address), Some(1));

    let error = client
        .send_request(
            &server.address,
            &request(b"slow"),
            Duration::from_millis(20),
        )
        .unwrap_err();
    assert_eq!(
        error.kind(),
        "timeout",
        "unexpected timeout mapping: {error}"
    );
    let connection = error.connection().unwrap();
    assert_eq!(connection.address, server.address);
    assert_eq!(connection.version, 1);

    let mut malformed = request(b"malformed");
    malformed.encoded_request = vec![0xff];
    let error = client
        .send_request(&server.address, &malformed, Duration::from_secs(1))
        .unwrap_err();
    assert_eq!(error.kind(), "invalid_request");
    assert_eq!(client.connection_version(&server.address), Some(1));
}

#[test]
fn invalid_address_fails_before_pool_insertion() {
    let mut client = TonicCoprocessorClient::new().unwrap();
    let address = "invalid address\n";
    let error = client
        .send_request(address, &request(b"invalid"), Duration::from_secs(1))
        .unwrap_err();
    assert!(matches!(
        error,
        DirectUnaryClientError::InvalidAddress {
            address: ref original,
            ..
        } if original == address
    ));
    assert_eq!(client.active_address_count(), 0);
}

#[test]
fn response_larger_than_tonic_default_limit_is_returned() {
    // client-go/internal/client/client.go:72-74 MaxRecvMsgSize.
    let response_size = 5 * 1024 * 1024;
    let server = TestServer::start(RecordingTikv {
        requests: Arc::new(Mutex::new(Vec::new())),
        delay: Duration::ZERO,
        response_size: Some(response_size),
        failure: None,
    });
    let mut client = TonicCoprocessorClient::new().unwrap();

    let raw = client
        .send_request(
            &server.address,
            &request(b"large-response"),
            Duration::from_secs(2),
        )
        .unwrap();
    let response = CoprocessorResponse::decode(raw.encoded_response.as_slice()).unwrap();
    assert_eq!(response.data.len(), response_size);
    assert!(response.data.iter().all(|byte| *byte == 0x5a));
}

#[test]
fn remote_grpc_failures_preserve_code_address_and_generation_without_implicit_close() {
    for (tonic_code, expected) in [
        (tonic::Code::Cancelled, DirectUnaryGrpcCode::Canceled),
        (
            tonic::Code::DeadlineExceeded,
            DirectUnaryGrpcCode::DeadlineExceeded,
        ),
        (tonic::Code::Unavailable, DirectUnaryGrpcCode::Unavailable),
    ] {
        let server = TestServer::start(RecordingTikv {
            requests: Arc::new(Mutex::new(Vec::new())),
            delay: Duration::ZERO,
            response_size: None,
            failure: Some(tonic_code),
        });
        let mut client = TonicCoprocessorClient::new().unwrap();
        let error = client
            .send_request(&server.address, &request(b"fail"), Duration::from_secs(2))
            .unwrap_err();

        assert_eq!(error.kind(), "connection");
        assert_eq!(
            error.transport_class(),
            Some(DirectUnaryTransportClass::RemoteGrpc)
        );
        assert_eq!(error.grpc_code(), Some(expected));
        let connection = error.connection().unwrap();
        assert_eq!(connection.address, server.address);
        assert_eq!(connection.version, 1);
        assert_eq!(client.connection_version(&server.address), Some(1));
        assert_eq!(
            error.requires_generation_close(),
            expected == DirectUnaryGrpcCode::Canceled
        );
    }
}

#[test]
fn delayed_exact_generation_close_cannot_close_a_newer_channel() {
    // client-go/internal/client/client.go:553-579 CloseAddrVer.
    let server = TestServer::start(RecordingTikv {
        requests: Arc::new(Mutex::new(Vec::new())),
        delay: Duration::ZERO,
        response_size: None,
        failure: Some(tonic::Code::Cancelled),
    });
    let mut client = TonicCoprocessorClient::new().unwrap();

    let first = client
        .send_request(&server.address, &request(b"first"), Duration::from_secs(2))
        .unwrap_err();
    assert!(first.requires_generation_close());
    let failed_version = first.connection().unwrap().version;
    client
        .close_address_version(&server.address, failed_version)
        .unwrap();
    assert_eq!(client.connection_version(&server.address), None);

    let second = client
        .send_request(&server.address, &request(b"second"), Duration::from_secs(2))
        .unwrap_err();
    assert_eq!(second.connection().unwrap().version, failed_version + 1);
    client
        .close_address_version(&server.address, failed_version)
        .unwrap();
    assert_eq!(
        client.connection_version(&server.address),
        Some(failed_version + 1)
    );
}

#[test]
fn local_deadline_and_terminal_control_errors_are_distinct_from_remote_status() {
    let caller_cancelled = DirectUnaryClientError::CallerCancelled;
    assert_eq!(caller_cancelled.kind(), "caller_cancelled");
    assert_eq!(
        caller_cancelled.transport_class(),
        Some(DirectUnaryTransportClass::CallerCancelled)
    );
    assert_eq!(caller_cancelled.grpc_code(), None);
    assert_eq!(caller_cancelled.connection(), None);

    for terminal in [
        DirectUnaryClientError::Closed,
        DirectUnaryClientError::InvalidRequest("bad wire".to_owned()),
        DirectUnaryClientError::Runtime("runtime stopped".to_owned()),
    ] {
        assert_eq!(terminal.transport_class(), None);
        assert_eq!(terminal.grpc_code(), None);
        assert_eq!(terminal.connection(), None);
        assert!(!terminal.requires_generation_close());
    }
}
