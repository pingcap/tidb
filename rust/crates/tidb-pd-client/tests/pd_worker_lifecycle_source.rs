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

use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_pd_client::{PdClient, PdClientError, PdClientShutdownError};
use tidb_proto::pdpb::{
    self,
    pd_server::{Pd, PdServer},
};
use tokio_stream::wrappers::ReceiverStream;

const CLUSTER_ID: u64 = 42;

#[derive(Clone)]
struct MembershipPd {
    address: String,
}

#[tonic::async_trait]
impl Pd for MembershipPd {
    type TsoStream = ReceiverStream<Result<pdpb::TsoResponse, tonic::Status>>;

    async fn tso(
        &self,
        _request: tonic::Request<tonic::Streaming<pdpb::TsoRequest>>,
    ) -> Result<tonic::Response<Self::TsoStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused Tso"))
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

    async fn get_gc_state(
        &self,
        _request: tonic::Request<pdpb::GetGcStateRequest>,
    ) -> Result<tonic::Response<pdpb::GetGcStateResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused GetGCState"))
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
        Err(tonic::Status::unimplemented("unused GetRegionById"))
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
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}");
        let service = MembershipPd {
            address: endpoint.clone(),
        };
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started, observed) = mpsc::channel();
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
        observed.recv().unwrap();
        for _ in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return Self {
                    address: endpoint,
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

#[test]
fn unique_owner_explicitly_closes_and_joins_the_real_worker() {
    let server = Server::start();
    let owner = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    assert!(owner.is_worker_owner());

    let request = owner.clone();
    assert!(!request.is_worker_owner());
    assert_eq!(request.shutdown(), Err(PdClientShutdownError::NotOwner));

    owner.shutdown().unwrap();
}

#[test]
fn retained_request_prevents_success_and_is_closed_by_owner_fallback() {
    let server = Server::start();
    let owner = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    let retained = owner.clone();

    assert_eq!(
        owner.shutdown(),
        Err(PdClientShutdownError::SharedOwners { owners: 2 })
    );
    assert_eq!(retained.refresh_members(), Err(PdClientError::Closed));
}
