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

use std::convert::Infallible;
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::{Buf, BufMut};
use prost::Message;
use tidb_txnkv::region::StoreLiveness;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::{DirectUnaryClient, DirectUnaryClientError, DEFAULT_STORE_LIVENESS_TIMEOUT};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::{Body, BoxFuture, Service, StdError};

#[derive(Clone, Copy, PartialEq, Message)]
struct HealthCheckResponse {
    #[prost(int32, tag = "1")]
    status: i32,
}

#[derive(Clone, Copy, Debug, Default)]
struct RawCodec;

#[derive(Clone, Copy, Debug, Default)]
struct RawEncoder;

#[derive(Clone, Copy, Debug, Default)]
struct RawDecoder;

impl Codec for RawCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

impl Encoder for RawEncoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        destination.put_slice(&item);
        Ok(())
    }
}

impl Decoder for RawDecoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(source.copy_to_bytes(source.remaining()).to_vec()))
    }
}

#[derive(Clone)]
struct FixedHealthService {
    status: Result<i32, tonic::Code>,
}

impl<B> Service<tonic::codegen::http::Request<B>> for FixedHealthService
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        if request.uri().path() != "/grpc.health.v1.Health/Check" {
            return Box::pin(async move {
                let mut response =
                    tonic::codegen::http::Response::new(tonic::body::Body::default());
                response.headers_mut().insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                response.headers_mut().insert(
                    tonic::codegen::http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            });
        }

        struct CheckService {
            result: Result<i32, tonic::Code>,
        }

        impl tonic::server::UnaryService<Vec<u8>> for CheckService {
            type Response = Vec<u8>;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

            fn call(&mut self, _request: tonic::Request<Vec<u8>>) -> Self::Future {
                let result = self.result;
                Box::pin(async move {
                    match result {
                        Ok(status) => Ok(tonic::Response::new(
                            HealthCheckResponse { status }.encode_to_vec(),
                        )),
                        Err(code) => Err(tonic::Status::new(code, "injected health failure")),
                    }
                })
            }
        }

        let result = self.status;
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(RawCodec);
            Ok(grpc.unary(CheckService { result }, request).await)
        })
    }
}

impl tonic::server::NamedService for FixedHealthService {
    const NAME: &'static str = "grpc.health.v1.Health";
}

struct TestHealthServer {
    address: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TestHealthServer {
    fn start(status: Result<i32, tonic::Code>) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let server = tonic::transport::Server::builder()
                    .add_service(FixedHealthService { status })
                    .serve_with_shutdown(address, async {
                        let _ = shutdown_rx.await;
                    });
                started_tx.send(()).unwrap();
                server.await.unwrap();
            });
        });
        started_rx.recv().unwrap();
        for _ in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return Self {
                    address: address.to_string(),
                    shutdown: Some(shutdown),
                    thread: Some(thread),
                };
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("test health server did not accept connections");
    }
}

impl Drop for TestHealthServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

#[test]
fn foreground_health_check_maps_serving_unknown_non_serving_and_rpc_failure() {
    // client-go/internal/locate/store_cache.go:693-797 invokeKVStatusAPI.
    for (wire_status, expected) in [
        (0, StoreLiveness::Unknown),
        (1, StoreLiveness::Reachable),
        (2, StoreLiveness::Unreachable),
        (3, StoreLiveness::Unknown),
        (99, StoreLiveness::Unreachable),
    ] {
        let server = TestHealthServer::start(Ok(wire_status));
        let client = TonicCoprocessorClient::new().unwrap();
        assert_eq!(
            client
                .liveness(&server.address, Duration::from_secs(1))
                .unwrap(),
            expected
        );
        assert_eq!(
            client.active_address_count(),
            0,
            "health checking must not mutate the request channel pool"
        );
    }

    let server = TestHealthServer::start(Err(tonic::Code::Unavailable));
    let client = TonicCoprocessorClient::new().unwrap();
    assert_eq!(
        client
            .liveness(&server.address, Duration::from_secs(1))
            .unwrap(),
        StoreLiveness::Unreachable
    );
}

#[test]
fn foreground_health_check_uses_one_second_default_and_maps_connection_failure() {
    assert_eq!(DEFAULT_STORE_LIVENESS_TIMEOUT, Duration::from_secs(1));
    let server = TestHealthServer::start(Ok(1));
    let client = TonicCoprocessorClient::new().unwrap();
    assert_eq!(
        client.liveness_default(&server.address).unwrap(),
        StoreLiveness::Reachable
    );

    let unused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = unused.local_addr().unwrap().to_string();
    drop(unused);
    assert_eq!(
        client
            .liveness(&address, Duration::from_millis(50))
            .unwrap(),
        StoreLiveness::Unreachable
    );
}

#[test]
fn liveness_after_client_close_is_terminal_not_a_store_verdict() {
    let mut client = TonicCoprocessorClient::new().unwrap();
    client.close().unwrap();
    assert_eq!(
        client
            .liveness("127.0.0.1:20160", Duration::from_secs(1))
            .unwrap_err(),
        DirectUnaryClientError::Closed
    );
}
