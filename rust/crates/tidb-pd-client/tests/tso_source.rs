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

use std::collections::VecDeque;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use prost::Message;
use tidb_pd_client::{PdClient, TSO_PATH};
use tidb_proto::pdpb::{
    self,
    pd_server::{Pd, PdServer},
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

const CLUSTER_ID: u64 = 42;

#[derive(Clone)]
enum TsoReply {
    Response(pdpb::TsoResponse),
    Status(tonic::Code, &'static str),
    Delayed(Duration, pdpb::TsoResponse),
}

struct State {
    replies: VecDeque<TsoReply>,
    requests: Vec<pdpb::TsoRequest>,
    stream_opens: usize,
    withhold_headers_until_request: bool,
}

#[derive(Clone)]
struct MockPd {
    state: Arc<Mutex<State>>,
    address: String,
}

#[tonic::async_trait]
impl Pd for MockPd {
    type TsoStream = ReceiverStream<Result<pdpb::TsoResponse, tonic::Status>>;

    async fn tso(
        &self,
        request: tonic::Request<tonic::Streaming<pdpb::TsoRequest>>,
    ) -> Result<tonic::Response<Self::TsoStream>, tonic::Status> {
        self.state.lock().unwrap().stream_opens += 1;
        let state = Arc::clone(&self.state);
        let mut requests = request.into_inner();
        let (responses, response_rx) = tokio::sync::mpsc::channel(1);
        if self.state.lock().unwrap().withhold_headers_until_request {
            let request = match requests.next().await {
                Some(Ok(request)) => request,
                Some(Err(status)) => return Err(status),
                None => {
                    return Err(tonic::Status::unavailable(
                        "TSO request stream closed before its first request",
                    ));
                }
            };
            let reply = {
                let mut state = self.state.lock().unwrap();
                state.requests.push(request);
                state.replies.pop_front()
            };
            match reply {
                Some(TsoReply::Response(response)) => {
                    responses.send(Ok(response)).await.unwrap();
                }
                Some(TsoReply::Status(code, message)) => {
                    responses
                        .send(Err(tonic::Status::new(code, message)))
                        .await
                        .unwrap();
                }
                Some(TsoReply::Delayed(delay, response)) => {
                    tokio::time::sleep(delay).await;
                    responses.send(Ok(response)).await.unwrap();
                }
                None => panic!("mock PD has no reply for the first TSO request"),
            }
        }
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                let request = match request {
                    Ok(request) => request,
                    Err(_) => break,
                };
                let reply = {
                    let mut state = state.lock().unwrap();
                    state.requests.push(request);
                    state.replies.pop_front()
                };
                match reply {
                    Some(TsoReply::Response(response)) => {
                        if responses.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Some(TsoReply::Status(code, message)) => {
                        let _ = responses.send(Err(tonic::Status::new(code, message))).await;
                        break;
                    }
                    Some(TsoReply::Delayed(delay, response)) => {
                        tokio::time::sleep(delay).await;
                        if responses.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
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
            header: Some(header()),
            members: vec![member.clone()],
            leader: Some(member),
            ..pdpb::GetMembersResponse::default()
        }))
    }

    async fn get_store(
        &self,
        _request: tonic::Request<pdpb::GetStoreRequest>,
    ) -> Result<tonic::Response<pdpb::GetStoreResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused GetStore"))
    }

    async fn get_region(
        &self,
        _request: tonic::Request<pdpb::GetRegionRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused GetRegion"))
    }

    async fn get_prev_region(
        &self,
        _request: tonic::Request<pdpb::GetRegionRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused GetPrevRegion"))
    }

    async fn get_region_by_id(
        &self,
        _request: tonic::Request<pdpb::GetRegionByIdRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused GetRegionByID"))
    }

    async fn scan_regions(
        &self,
        _request: tonic::Request<pdpb::ScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::ScanRegionsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused ScanRegions"))
    }

    async fn batch_scan_regions(
        &self,
        _request: tonic::Request<pdpb::BatchScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::BatchScanRegionsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused BatchScanRegions"))
    }
}

struct Server {
    address: String,
    state: Arc<Mutex<State>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    fn start(replies: impl IntoIterator<Item = TsoReply>) -> Self {
        Self::start_with_header_behavior(replies, false)
    }

    fn start_after_first_request(replies: impl IntoIterator<Item = TsoReply>) -> Self {
        Self::start_with_header_behavior(replies, true)
    }

    fn start_with_header_behavior(
        replies: impl IntoIterator<Item = TsoReply>,
        withhold_headers_until_request: bool,
    ) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}");
        let state = Arc::new(Mutex::new(State {
            replies: replies.into_iter().collect(),
            requests: Vec::new(),
            stream_opens: 0,
            withhold_headers_until_request,
        }));
        let service = MockPd {
            state: Arc::clone(&state),
            address: endpoint.clone(),
        };
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
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
        for _ in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return Self {
                    address: endpoint,
                    state,
                    shutdown: Some(shutdown),
                    thread: Some(thread),
                };
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("mock PD did not accept connections");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn header() -> pdpb::ResponseHeader {
    pdpb::ResponseHeader {
        cluster_id: CLUSTER_ID,
        error: None,
    }
}

fn timestamp(physical: i64, logical: i64) -> pdpb::TsoResponse {
    pdpb::TsoResponse {
        header: Some(header()),
        count: 1,
        timestamp: Some(pdpb::Timestamp {
            physical,
            logical,
            suffix_bits: 0,
        }),
    }
}

#[test]
fn tso_wire_keeps_the_pinned_stream_path_and_field_numbers() {
    assert_eq!(TSO_PATH, "/pdpb.PD/Tso");
    let request = pdpb::TsoRequest {
        header: Some(pdpb::RequestHeader {
            cluster_id: CLUSTER_ID,
            ..pdpb::RequestHeader::default()
        }),
        count: 1,
        dc_location: String::new(),
    };
    assert_eq!(
        request.encode_to_vec(),
        [0x0a, 0x02, 0x08, 0x2a, 0x10, 0x01]
    );
    let response = timestamp(10, 3);
    assert_eq!(
        response.encode_to_vec(),
        [0x0a, 0x02, 0x08, 0x2a, 0x10, 0x01, 0x1a, 0x04, 0x08, 0x0a, 0x10, 0x03,]
    );
}

#[test]
fn request_handles_share_one_stream_and_one_monotonic_owner() {
    let server = Server::start([
        TsoReply::Response(timestamp(10, 1)),
        TsoReply::Response(timestamp(10, 2)),
    ]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    let clone = client.clone();
    assert_eq!(client.get_timestamp().unwrap(), (10_u64 << 18) + 1);
    assert_eq!(clone.get_timestamp().unwrap(), (10_u64 << 18) + 2);

    let state = server.state.lock().unwrap();
    assert_eq!(state.stream_opens, 1);
    assert_eq!(state.requests.len(), 2);
    assert!(state.requests.iter().all(|request| {
        request.count == 1
            && request.dc_location.is_empty()
            && request.header.as_ref().is_some_and(|header| {
                header.cluster_id == CLUSTER_ID
                    && header.sender_id == 0
                    && header.caller_id.is_empty()
                    && header.caller_component.is_empty()
            })
    }));
    drop(state);
    drop(clone);
    client.shutdown().unwrap();
}

#[test]
fn sends_first_request_before_waiting_for_response_headers() {
    let server = Server::start_after_first_request([TsoReply::Response(timestamp(15, 1))]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();

    assert_eq!(client.get_timestamp().unwrap(), (15_u64 << 18) + 1);
    let state = server.state.lock().unwrap();
    assert_eq!(state.stream_opens, 1);
    assert_eq!(state.requests.len(), 1);
}

#[test]
fn retry_retires_the_broken_stream_before_reopening() {
    let server = Server::start([
        TsoReply::Status(tonic::Code::Unavailable, "not leader"),
        TsoReply::Response(timestamp(20, 1)),
    ]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    assert_eq!(client.get_timestamp().unwrap(), (20_u64 << 18) + 1);
    assert_eq!(server.state.lock().unwrap().stream_opens, 2);
}

#[test]
fn malformed_or_fallback_timestamps_are_terminal() {
    let mut missing = timestamp(30, 1);
    missing.timestamp = None;
    let server = Server::start([
        TsoReply::Response(timestamp(30, 2)),
        TsoReply::Response(timestamp(30, 1)),
        TsoReply::Response(missing),
    ]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    assert_eq!(client.get_timestamp().unwrap(), (30_u64 << 18) + 2);
    assert_eq!(client.get_timestamp().unwrap_err().kind(), "tso_fallback");
    assert_eq!(
        client.get_timestamp().unwrap_err().kind(),
        "missing_tso_timestamp"
    );
}

#[test]
fn configured_timeout_bounds_the_whole_timestamp_request() {
    let server = Server::start([TsoReply::Delayed(
        Duration::from_millis(500),
        timestamp(40, 1),
    )]);
    let client = PdClient::connect(&server.address, Duration::from_millis(100)).unwrap();
    assert_eq!(client.get_timestamp().unwrap_err().kind(), "timeout");
}
