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

//! Times `PdClient::get_timestamp` against a loopback PD that answers every
//! TSO request immediately.
//!
//! The mock removes PD's own scheduling and the network, so what remains is
//! exactly the client-side cost every statement pays for its start-ts: the
//! command-channel hop to the single PD worker thread, `block_on` on the
//! worker's runtime, and one gRPC stream request/response over loopback.
//! A real cluster only adds PD's own latency on top of this floor.
//!
//! Run: `cargo run --release --example tso_profile -- [calls] [threads]`

#![allow(missing_docs)]

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use tidb_pd_client::PdClient;
use tidb_proto::pdpb::{
    self,
    pd_server::{Pd, PdServer},
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

const CLUSTER_ID: u64 = 42;

#[derive(Clone)]
struct MockPd {
    address: String,
    physical: Arc<AtomicI64>,
}

#[tonic::async_trait]
impl Pd for MockPd {
    type TsoStream = ReceiverStream<Result<pdpb::TsoResponse, tonic::Status>>;

    async fn tso(
        &self,
        request: tonic::Request<tonic::Streaming<pdpb::TsoRequest>>,
    ) -> Result<tonic::Response<Self::TsoStream>, tonic::Status> {
        let mut requests = request.into_inner();
        let (responses, response_rx) = tokio::sync::mpsc::channel(1);
        let physical = Arc::clone(&self.physical);
        tokio::spawn(async move {
            while let Some(Ok(request)) = requests.next().await {
                // Instrument change for batching: answer `count` timestamps in
                // one reply, reporting the LAST logical of the batch exactly as
                // PD does, so the client's range split is exercised.
                let count = request.count.max(1);
                let next = physical.fetch_add(1, Ordering::Relaxed) + 1;
                let response = pdpb::TsoResponse {
                    header: Some(pdpb::ResponseHeader {
                        cluster_id: CLUSTER_ID,
                        error: None,
                    }),
                    count,
                    timestamp: Some(pdpb::Timestamp {
                        physical: next,
                        logical: i64::from(count),
                        suffix_bits: 0,
                    }),
                };
                if responses.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(response_rx)))
    }

    async fn get_members(
        &self,
        _request: tonic::Request<pdpb::GetMembersRequest>,
    ) -> Result<tonic::Response<pdpb::GetMembersResponse>, tonic::Status> {
        let member = pdpb::Member {
            name: "pd-1".to_owned(),
            member_id: 1,
            client_urls: vec![self.address.clone()],
            ..pdpb::Member::default()
        };
        Ok(tonic::Response::new(pdpb::GetMembersResponse {
            header: Some(pdpb::ResponseHeader {
                cluster_id: CLUSTER_ID,
                error: None,
            }),
            members: vec![member.clone()],
            leader: Some(member),
            ..pdpb::GetMembersResponse::default()
        }))
    }

    async fn get_store(
        &self,
        _request: tonic::Request<pdpb::GetStoreRequest>,
    ) -> Result<tonic::Response<pdpb::GetStoreResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_store"))
    }

    async fn get_region(
        &self,
        _request: tonic::Request<pdpb::GetRegionRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_region"))
    }

    async fn get_prev_region(
        &self,
        _request: tonic::Request<pdpb::GetRegionRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_prev_region"))
    }

    async fn get_region_by_id(
        &self,
        _request: tonic::Request<pdpb::GetRegionByIdRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_region_by_id"))
    }

    async fn scan_regions(
        &self,
        _request: tonic::Request<pdpb::ScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::ScanRegionsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("scan_regions"))
    }

    async fn batch_scan_regions(
        &self,
        _request: tonic::Request<pdpb::BatchScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::BatchScanRegionsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("batch_scan_regions"))
    }

    async fn get_gc_state(
        &self,
        _request: tonic::Request<pdpb::GetGcStateRequest>,
    ) -> Result<tonic::Response<pdpb::GetGcStateResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_gc_state"))
    }

    async fn get_operator(
        &self,
        _request: tonic::Request<pdpb::GetOperatorRequest>,
    ) -> Result<tonic::Response<pdpb::GetOperatorResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_operator"))
    }

    async fn split_and_scatter_regions(
        &self,
        _request: tonic::Request<pdpb::SplitAndScatterRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::SplitAndScatterRegionsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("split_and_scatter_regions"))
    }
}

fn start_server() -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("http://{address}");
    drop(listener);
    let service = MockPd {
        address: endpoint.clone(),
        physical: Arc::new(AtomicI64::new(1_000_000)),
    };
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let (started, started_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let server = tonic::transport::Server::builder()
                .add_service(PdServer::new(service))
                .serve_with_shutdown(address, async {
                    let _ = shutdown_rx.await;
                });
            started.send(()).unwrap();
            server.await.unwrap();
        });
    });
    started_rx.recv().unwrap();
    for _ in 0..200 {
        if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
            return (endpoint, shutdown);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("mock PD did not accept connections");
}

/// Drives the same loopback PD over one raw tonic TSO stream from the calling
/// thread's own runtime, with no worker-thread hop and no retry wrapper. This
/// is the gRPC-over-loopback floor the client architecture is layered on.
fn direct(endpoint: &str, calls: usize) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let mut client = pdpb::pd_client::PdClient::connect(endpoint.to_owned())
            .await
            .unwrap();
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let request = pdpb::TsoRequest {
            header: Some(pdpb::RequestHeader {
                cluster_id: CLUSTER_ID,
                ..pdpb::RequestHeader::default()
            }),
            count: 1,
            dc_location: String::new(),
        };
        requests.send(request.clone()).await.unwrap();
        let mut responses = client
            .tso(ReceiverStream::new(receiver))
            .await
            .unwrap()
            .into_inner();
        responses.message().await.unwrap().unwrap();
        for _ in 0..200 {
            requests.send(request.clone()).await.unwrap();
            responses.message().await.unwrap().unwrap();
        }
        let start = Instant::now();
        for _ in 0..calls {
            requests.send(request.clone()).await.unwrap();
            responses.message().await.unwrap().unwrap();
        }
        start.elapsed()
    })
}

fn main() {
    let mut args = std::env::args().skip(1);
    let calls: usize = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    let threads: usize = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);

    let (endpoint, shutdown) = start_server();
    if args.next().as_deref() == Some("direct") {
        let elapsed = direct(&endpoint, calls);
        #[allow(clippy::cast_precision_loss)]
        let per_call = elapsed.as_secs_f64() * 1e6 / calls as f64;
        println!("direct calls={calls} elapsed={elapsed:?} per_call={per_call:.3}us");
        let _ = shutdown.send(());
        return;
    }
    let client = Arc::new(PdClient::connect(&endpoint, Duration::from_secs(5)).unwrap());

    // Warm the stream and the channel caches.
    for _ in 0..200 {
        client.get_timestamp().unwrap();
    }

    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let client = Arc::clone(&client);
            scope.spawn(move || {
                for _ in 0..calls {
                    client.get_timestamp().unwrap();
                }
            });
        }
    });
    let elapsed = start.elapsed();

    let total = calls * threads;
    #[allow(clippy::cast_precision_loss)]
    let per_call = elapsed.as_secs_f64() * 1e6 / total as f64;
    #[allow(clippy::cast_precision_loss)]
    let rate = total as f64 / elapsed.as_secs_f64();
    println!("threads={threads} calls/thread={calls} total={total}");
    println!("elapsed={elapsed:?} per_call={per_call:.3}us throughput={rate:.0}/s");

    let _ = shutdown.send(());
}
