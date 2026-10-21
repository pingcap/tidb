// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};

use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use futures::prelude::*;
use tonic::transport::Channel;

use super::dispatcher::DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL;
use super::*;
use crate::async_util::Cancellation;
use crate::pd::{Connection, RetryClient, RetryClientTrait};
use crate::proto::pdpb::*;
use crate::proto::{keyspacepb, metapb, pdpb, tsopb};
use crate::SecurityManager;
use crate::{Error, Timestamp};

#[derive(Clone, Copy)]
enum Reply {
    Timestamp,
    StallBody,
    StallHeaders,
    End,
    /// Reads and counts every request that arrives but answers none.
    ///
    /// `StallBody` stalls BEFORE reading, so it cannot tell a client that
    /// pipelines from one that waits: either way the server counts one
    /// request. This keeps reading, so `received` reports exactly how many
    /// batches the client was willing to have outstanding at once.
    DrainRequests,
}

#[derive(Clone)]
struct PdServer {
    endpoint: String,
    user_agents: Arc<std::sync::Mutex<Vec<String>>>,
    wire_routes: Arc<std::sync::Mutex<Vec<(String, Option<String>, bool)>>>,
    leader_urls: Arc<std::sync::RwLock<Vec<String>>>,
    region_members: Arc<std::sync::RwLock<Option<Vec<pdpb::Member>>>>,
    region_metadata: Arc<std::sync::Mutex<Vec<bool>>>,
    region_failure: Arc<AtomicUsize>,
    health_status: Arc<AtomicUsize>,
    health_requests: Arc<AtomicUsize>,
    health_stall: Arc<std::sync::atomic::AtomicBool>,
    member_failures: Arc<AtomicUsize>,
    member_requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    stall_members: Arc<std::sync::atomic::AtomicBool>,
    stall_scans: Arc<std::sync::atomic::AtomicBool>,
    region_entered: Arc<tokio::sync::Semaphore>,
    region_release: Arc<tokio::sync::Semaphore>,
    region_id: Arc<AtomicUsize>,
    group: Arc<std::sync::RwLock<tsopb::KeyspaceGroup>>,
    revision: Arc<AtomicUsize>,
    discovery_requests: Arc<AtomicUsize>,
    discovery_error: Arc<std::sync::Mutex<Option<String>>>,
    requested_revisions: Arc<std::sync::Mutex<Vec<u64>>>,
    stall_discovery: Arc<std::sync::atomic::AtomicBool>,
    tso_headers: Arc<std::sync::Mutex<Vec<tsopb::RequestHeader>>>,
    cluster_info: Arc<std::sync::RwLock<Option<pdpb::GetClusterInfoResponse>>>,
    min_response:
        Arc<std::sync::RwLock<std::result::Result<pdpb::GetMinTsResponse, tonic::Status>>>,
    min_requests: Arc<AtomicUsize>,
    omit_metadata_header: Arc<std::sync::atomic::AtomicBool>,
    required_keyspace: Arc<AtomicUsize>,
    keyspace_loads: Arc<AtomicUsize>,
    scripted_tso: Arc<std::sync::Mutex<VecDeque<TsoResponse>>>,
    next_logical: Arc<AtomicUsize>,
    reply: Reply,
    tso_failure: Arc<std::sync::Mutex<Option<tonic::Code>>>,
    cluster_info_failure: Arc<std::sync::atomic::AtomicBool>,
    stall_mode: Arc<std::sync::atomic::AtomicBool>,
    mode_requests: Arc<AtomicUsize>,
    received: Arc<AtomicUsize>,
    /// Every TSO request's count, in arrival order.
    request_counts: Arc<StdMutex<Vec<u32>>>,
    dropped: Arc<AtomicUsize>,
}

struct ActiveStream(Arc<AtomicUsize>);
impl Drop for ActiveStream {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl tonic::server::NamedService for PdServer {
    const NAME: &'static str = "pdpb.PD";
}

impl<B> Service<http::Request<B>> for PdServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        self.user_agents.lock().unwrap().push(
            request
                .headers()
                .get("user-agent")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
        );
        self.wire_routes.lock().unwrap().push((
            request.uri().path().to_owned(),
            request
                .headers()
                .get("pd-forwarded-host")
                .map(|v| v.to_str().unwrap().to_owned()),
            request.headers().contains_key("pd-allow-follower-handle"),
        ));
        let service = self.clone();
        match request.uri().path() {
            "/pdpb.PD/GetMinTS" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pdpb::GetMinTsResponse,
                    pdpb::GetMinTsRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetClusterInfo" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pdpb::GetClusterInfoResponse,
                    pdpb::GetClusterInfoRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetMembers" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pdpb::GetMembersResponse,
                    pdpb::GetMembersRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetRegion" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pdpb::GetRegionResponse,
                    pdpb::GetRegionRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetAllStores" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pdpb::GetAllStoresResponse,
                    pdpb::GetAllStoresRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetOperator" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pdpb::GetOperatorResponse,
                    pdpb::GetOperatorRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/ScanRegions" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pdpb::ScanRegionsResponse,
                    pdpb::ScanRegionsRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/Tso" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .streaming(service, request)
                        .await,
                )
            }),
            _ => Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::Body::empty())
                    .unwrap())
            }),
        }
    }
}

impl tonic::server::UnaryService<GetMembersRequest> for PdServer {
    type Response = GetMembersResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, _: tonic::Request<GetMembersRequest>) -> Self::Future {
        self.member_requests.fetch_add(1, Ordering::SeqCst);
        if self.stall_members.load(Ordering::SeqCst) {
            return Box::pin(std::future::pending());
        }
        let mut remaining = self.member_failures.load(Ordering::SeqCst);
        while remaining > 0 {
            match self.member_failures.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Box::pin(async { Err(tonic::Status::unavailable("PD is starting")) })
                }
                Err(actual) => remaining = actual,
            }
        }
        let member = Member {
            member_id: 1,
            client_urls: vec![self.endpoint.clone()],
            ..Default::default()
        };
        let leader = Member {
            client_urls: self.leader_urls.read().unwrap().clone(),
            ..member.clone()
        };
        let members = self
            .region_members
            .read()
            .unwrap()
            .clone()
            .unwrap_or_else(|| vec![member.clone()]);
        Box::pin(async move {
            Ok(tonic::Response::new(GetMembersResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                members,
                leader: Some(leader),
                ..Default::default()
            }))
        })
    }
}

impl tonic::server::UnaryService<pdpb::GetRegionRequest> for PdServer {
    type Response = pdpb::GetRegionResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<pdpb::GetRegionRequest>) -> Self::Future {
        self.region_metadata
            .lock()
            .unwrap()
            .push(request.metadata().contains_key("pd-allow-follower-handle"));
        assert!(request.metadata().contains_key("grpc-timeout"));
        let forwarded = request.metadata().contains_key("pd-forwarded-host");
        let request = request.into_inner();
        assert_eq!(request.header.unwrap().cluster_id, 42);
        let service = self.clone();
        Box::pin(async move {
            if request.region_key == b"blocked" {
                service.region_entered.add_permits(1);
                service.region_release.acquire().await.unwrap().forget();
            }
            if !forwarded && service.region_failure.load(Ordering::SeqCst) == 1 {
                return Err(tonic::Status::unknown("follower transport failure"));
            }
            Ok(tonic::Response::new(pdpb::GetRegionResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    error: (!forwarded && service.region_failure.load(Ordering::SeqCst) == 2)
                        .then_some(pdpb::Error {
                            r#type: pdpb::ErrorType::RegionNotFound as i32,
                            message: "missing follower region".to_owned(),
                        }),
                }),
                region: Some(metapb::Region {
                    id: service.region_id.load(Ordering::SeqCst) as u64,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        })
    }
}

impl tonic::server::UnaryService<pdpb::GetAllStoresRequest> for PdServer {
    type Response = pdpb::GetAllStoresResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<pdpb::GetAllStoresRequest>) -> Self::Future {
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        let omit = self.omit_metadata_header.load(Ordering::SeqCst);
        Box::pin(async move {
            Ok(tonic::Response::new(pdpb::GetAllStoresResponse {
                header: (!omit).then_some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                stores: vec![metapb::Store {
                    id: 9,
                    ..Default::default()
                }],
            }))
        })
    }
}

/// PD's `GetOperator` for a region with no operator: the header carries
/// `UNKNOWN: Not Found` and the response nothing else.
impl tonic::server::UnaryService<pdpb::GetOperatorRequest> for PdServer {
    type Response = pdpb::GetOperatorResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, _request: tonic::Request<pdpb::GetOperatorRequest>) -> Self::Future {
        Box::pin(async move {
            Ok(tonic::Response::new(pdpb::GetOperatorResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    error: Some(pdpb::Error {
                        r#type: pdpb::ErrorType::Unknown as i32,
                        message: "Not Found".to_owned(),
                    }),
                }),
                ..Default::default()
            }))
        })
    }
}

impl tonic::server::UnaryService<pdpb::ScanRegionsRequest> for PdServer {
    type Response = pdpb::ScanRegionsResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<pdpb::ScanRegionsRequest>) -> Self::Future {
        self.region_metadata
            .lock()
            .unwrap()
            .push(request.metadata().contains_key("pd-allow-follower-handle"));
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        let request = request.into_inner();
        let service = self.clone();
        Box::pin(async move {
            if service.stall_scans.load(Ordering::SeqCst) {
                service.region_entered.add_permits(1);
                service.region_release.acquire().await.unwrap().forget();
            }
            Ok(tonic::Response::new(pdpb::ScanRegionsResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                region_metas: vec![metapb::Region {
                    id: 7,
                    start_key: request.start_key,
                    end_key: request.end_key,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        })
    }
}

impl tonic::server::StreamingService<TsoRequest> for PdServer {
    type Response = TsoResponse;
    type ResponseStream =
        Pin<Box<dyn Stream<Item = std::result::Result<TsoResponse, tonic::Status>> + Send>>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<tonic::Streaming<TsoRequest>>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let active = ActiveStream(service.dropped.clone());
            if let Some(code) = *service.tso_failure.lock().unwrap() {
                return Err(tonic::Status::new(code, "injected TSO connection failure"));
            }
            let mut requests = request.into_inner();
            // A server is allowed to wait for the first request before returning headers.
            let first = requests.message().await?;
            if let Some(first) = &first {
                service.received.fetch_add(1, Ordering::SeqCst);
                service.request_counts.lock().unwrap().push(first.count);
            }
            if matches!(service.reply, Reply::StallHeaders) {
                futures::future::pending::<()>().await;
            }
            let responses = futures::stream::unfold(
                (requests, first, active, service),
                |(mut requests, first, active, service)| async move {
                    if matches!(service.reply, Reply::End) {
                        return None;
                    }
                    if matches!(service.reply, Reply::StallBody) {
                        futures::future::pending::<()>().await;
                    }
                    if matches!(service.reply, Reply::DrainRequests) {
                        while let Ok(Some(_)) = requests.message().await {
                            service.received.fetch_add(1, Ordering::SeqCst);
                        }
                        futures::future::pending::<()>().await;
                    }
                    let request = match first {
                        Some(first) => first,
                        None => match requests.message().await {
                            Ok(Some(request)) => {
                                service.received.fetch_add(1, Ordering::SeqCst);
                                service.request_counts.lock().unwrap().push(request.count);
                                request
                            }
                            Ok(None) => return None,
                            Err(error) => {
                                return Some((Err(error), (requests, None, active, service)))
                            }
                        },
                    };
                    let logical = (service
                        .next_logical
                        .fetch_add(request.count as usize, Ordering::SeqCst)
                        + request.count as usize) as i64;
                    let response = TsoResponse {
                        header: Some(ResponseHeader {
                            cluster_id: 42,
                            ..Default::default()
                        }),
                        count: request.count,
                        timestamp: Some(Timestamp {
                            physical: 100,
                            logical,
                            suffix_bits: 0,
                        }),
                    };
                    let response = service
                        .scripted_tso
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or(response);
                    Some((Ok(response), (requests, None, active, service)))
                },
            );
            Ok(tonic::Response::new(
                Box::pin(responses) as Self::ResponseStream
            ))
        })
    }
}

// The pinned gRPC health protocol has two scalar fields; exercise the actual
// endpoint, including cancellation of a server that never replies.
#[derive(Clone, PartialEq, prost::Message)]
struct HealthRequest {
    #[prost(string, tag = "1")]
    service: String,
}
#[derive(Clone, PartialEq, prost::Message)]
struct HealthResponse {
    #[prost(int32, tag = "1")]
    status: i32,
}
#[derive(Clone)]
struct HealthServer(PdServer);
impl tonic::server::NamedService for HealthServer {
    const NAME: &'static str = "grpc.health.v1.Health";
}
impl<B> Service<http::Request<B>> for HealthServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            Ok(
                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(service, request)
                    .await,
            )
        })
    }
}
impl tonic::server::UnaryService<HealthRequest> for HealthServer {
    type Response = HealthResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<HealthRequest>) -> Self::Future {
        assert!(request.get_ref().service.is_empty());
        let service = self.0.clone();
        Box::pin(async move {
            service.health_requests.fetch_add(1, Ordering::SeqCst);
            if service.health_stall.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(tonic::Response::new(HealthResponse {
                status: service.health_status.load(Ordering::SeqCst) as i32,
            }))
        })
    }
}

struct Server {
    service: PdServer,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(reply: Reply) -> Self {
        Self::start_with_clock(reply, Arc::default()).await
    }

    async fn start_with_clock(reply: Reply, next_logical: Arc<AtomicUsize>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = PdServer {
            endpoint: endpoint.clone(),
            user_agents: Arc::default(),
            leader_urls: Arc::new(std::sync::RwLock::new(vec![endpoint.clone()])),
            region_members: Arc::default(),
            region_metadata: Arc::default(),
            region_failure: Arc::default(),
            health_status: Arc::new(AtomicUsize::new(1)),
            wire_routes: Arc::default(),
            health_requests: Arc::default(),
            health_stall: Arc::default(),
            member_failures: Arc::new(AtomicUsize::new(0)),
            member_requests: Arc::new(AtomicUsize::new(0)),
            connections: Arc::new(AtomicUsize::new(0)),
            stall_members: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stall_scans: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            region_entered: Arc::new(tokio::sync::Semaphore::new(0)),
            region_release: Arc::new(tokio::sync::Semaphore::new(0)),
            region_id: Arc::new(AtomicUsize::new(1)),
            group: Arc::new(std::sync::RwLock::new(tsopb::KeyspaceGroup {
                members: vec![tsopb::KeyspaceGroupMember {
                    address: endpoint.clone(),
                    is_primary: true,
                }],
                ..Default::default()
            })),
            revision: Arc::new(AtomicUsize::new(1)),
            discovery_requests: Arc::new(AtomicUsize::new(0)),
            discovery_error: Arc::default(),
            requested_revisions: Arc::default(),
            stall_discovery: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tso_headers: Arc::new(std::sync::Mutex::new(Vec::new())),
            cluster_info: Arc::new(std::sync::RwLock::new(None)),
            min_response: Arc::new(std::sync::RwLock::new(Err(tonic::Status::unimplemented(
                "old API",
            )))),
            min_requests: Arc::new(AtomicUsize::new(0)),
            omit_metadata_header: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            required_keyspace: Arc::new(AtomicUsize::new(0)),
            keyspace_loads: Arc::new(AtomicUsize::new(0)),
            scripted_tso: Arc::default(),
            next_logical,
            reply,
            tso_failure: Arc::default(),
            cluster_info_failure: Arc::default(),
            stall_mode: Arc::default(),
            mode_requests: Arc::default(),
            received: Arc::new(AtomicUsize::new(0)),
            request_counts: Arc::default(),
            dropped: Arc::new(AtomicUsize::new(0)),
        };
        let task_service = service.clone();
        let connections = service.connections.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealthServer(task_service.clone()))
                .add_service(KeyspaceServer(task_service.clone()))
                .add_service(TsoServer(task_service.clone()))
                .add_service(task_service)
                .serve_with_incoming(TcpListenerStream::new(listener).map(move |socket| {
                    if socket.is_ok() {
                        connections.fetch_add(1, Ordering::SeqCst);
                    }
                    socket
                }))
                .await
                .unwrap();
        });
        Self { service, task }
    }

    async fn cluster(&self, timeout: Duration) -> crate::pd::Cluster {
        Connection::new(Arc::new(SecurityManager::default()))
            .connect_cluster(&[self.service.endpoint.clone()], timeout)
            .await
            .unwrap()
    }
}

/// A fixed `ServiceDiscovery` over one PD server (the TSO client's view of a
/// single-member cluster).
struct FixedDiscovery {
    url: String,
    channel: Channel,
    schedules: AtomicUsize,
}

#[async_trait::async_trait]
impl ServiceDiscovery for FixedDiscovery {
    fn get_cluster_id(&self) -> u64 {
        42
    }
    fn get_keyspace_id(&self) -> u32 {
        0
    }
    fn get_keyspace_group_id(&self) -> u32 {
        0
    }
    fn get_service_urls(&self) -> Vec<String> {
        vec![self.url.clone()]
    }
    fn get_serving_url(&self) -> String {
        self.url.clone()
    }
    fn get_backup_urls(&self) -> Vec<String> {
        Vec::new()
    }
    async fn get_or_create_grpc_conn(&self, _url: &str) -> crate::Result<Channel> {
        Ok(self.channel.clone())
    }
    fn remove_client_conn(&self, _url: &str) {}
    fn schedule_check_member_changed(&self) {
        self.schedules.fetch_add(1, Ordering::SeqCst);
    }
    async fn check_member_changed(&self) -> crate::Result<()> {
        Ok(())
    }
}

fn fixed_discovery(server: &Server) -> Arc<FixedDiscovery> {
    Arc::new(FixedDiscovery {
        url: server.service.endpoint.clone(),
        channel: Channel::from_shared(server.service.endpoint.clone())
            .unwrap()
            .connect_lazy(),
        schedules: AtomicUsize::new(0),
    })
}

/// A started TSO client over one PD server.
async fn test_cli(
    server: &Server,
    timeout: Duration,
    mut options: crate::pd::opt::Options,
) -> Arc<Cli> {
    options.timeout = timeout;
    let cli = Cli::new(
        &Cancellation::default(),
        Arc::new(options),
        timeout,
        fixed_discovery(server),
        StreamBuilderFactory::Pd,
    );
    cli.update_tso_leader_url(&server.service.endpoint);
    cli.setup().await;
    cli
}

/// The dispatcher's provider without a `Cli`: one stream to one server.
struct TestProvider {
    options: crate::pd::opt::Options,
    discovery: Arc<dyn ServiceDiscovery>,
    manager: crate::pd::connectionctx::Manager<Arc<super::stream::TsoStream>>,
    url: String,
    channel: Channel,
}

impl TestProvider {
    fn new(server: &Server, options: crate::pd::opt::Options) -> Arc<Self> {
        let discovery = fixed_discovery(server);
        Arc::new(Self {
            options,
            url: discovery.url.clone(),
            channel: discovery.channel.clone(),
            discovery,
            manager: crate::pd::connectionctx::Manager::new(),
        })
    }

    fn manager(&self) -> &crate::pd::connectionctx::Manager<Arc<super::stream::TsoStream>> {
        &self.manager
    }
}

#[async_trait::async_trait]
impl super::dispatcher::TsoServiceProvider for TestProvider {
    fn get_option(&self) -> &crate::pd::opt::Options {
        &self.options
    }
    fn get_timeout(&self) -> Duration {
        self.options.timeout
    }
    fn get_service_discovery(&self) -> &Arc<dyn ServiceDiscovery> {
        &self.discovery
    }
    fn get_connection_ctx_mgr(
        &self,
    ) -> &crate::pd::connectionctx::Manager<Arc<super::stream::TsoStream>> {
        &self.manager
    }
    async fn update_connection_ctxs(&self, ctx: &Cancellation) -> bool {
        let cctx = ctx.child();
        let Ok(stream) = StreamBuilderFactory::Pd
            .make_builder(self.channel.clone(), &self.url)
            .build(&cctx, None, self.options.timeout)
            .await
        else {
            return false;
        };
        let cancel = cctx.clone();
        self.manager
            .clean_all_and_store(&Arc::new(crate::pd::connectionctx::ConnectionCtx::new(
                cctx,
                move || cancel.cancel(),
                self.url.clone(),
                stream,
            )));
        true
    }
}

/// A dispatcher for RPC-concurrency checks alone, with its options.
fn concurrency_dispatcher() -> (
    Arc<super::dispatcher::TsoDispatcher>,
    Arc<crate::pd::opt::Options>,
    Cancellation,
) {
    struct OptionsProvider {
        options: Arc<crate::pd::opt::Options>,
        discovery: Arc<dyn ServiceDiscovery>,
        manager: crate::pd::connectionctx::Manager<Arc<super::stream::TsoStream>>,
    }
    #[async_trait::async_trait]
    impl super::dispatcher::TsoServiceProvider for OptionsProvider {
        fn get_option(&self) -> &crate::pd::opt::Options {
            &self.options
        }
        fn get_timeout(&self) -> Duration {
            self.options.timeout
        }
        fn get_service_discovery(&self) -> &Arc<dyn ServiceDiscovery> {
            &self.discovery
        }
        fn get_connection_ctx_mgr(
            &self,
        ) -> &crate::pd::connectionctx::Manager<Arc<super::stream::TsoStream>> {
            &self.manager
        }
        async fn update_connection_ctxs(&self, _ctx: &Cancellation) -> bool {
            false
        }
    }
    let options = Arc::new(crate::pd::opt::Options::new());
    let provider = Arc::new(OptionsProvider {
        options: options.clone(),
        discovery: Arc::new(FixedDiscovery {
            url: "http://127.0.0.1:1".to_owned(),
            channel: Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            schedules: AtomicUsize::new(0),
        }),
        manager: crate::pd::connectionctx::Manager::new(),
    });
    let cancellation = Cancellation::default();
    (
        super::dispatcher::TsoDispatcher::new(&cancellation, provider),
        options,
        cancellation,
    )
}

/// Waits until the TSO client's connection contexts are exactly `urls`.
async fn wait_for_streams(cli: &Arc<Cli>, urls: &[String]) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut current = cli.connection_urls_for_test();
            current.sort();
            let mut want = urls.to_vec();
            want.sort();
            if current == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "streams {:?}, want {urls:?}",
            cli.connection_urls_for_test()
        )
    });
}

/// A leader/follower `ServiceDiscovery` whose leader connection a test can
/// swap, to make the leader unreachable at the transport and back.
struct PairDiscovery {
    leader_url: String,
    follower_url: String,
    leader_channel: StdMutex<Channel>,
    follower_channel: Channel,
}

impl PairDiscovery {
    fn new(leader: &Server, follower: &Server, leader_channel: Channel) -> Arc<Self> {
        Arc::new(Self {
            leader_url: leader.service.endpoint.clone(),
            follower_url: follower.service.endpoint.clone(),
            leader_channel: StdMutex::new(leader_channel),
            follower_channel: Channel::from_shared(follower.service.endpoint.clone())
                .unwrap()
                .connect_lazy(),
        })
    }

    fn set_leader_channel(&self, channel: Channel) {
        *self.leader_channel.lock().unwrap() = channel;
    }
}

#[async_trait::async_trait]
impl ServiceDiscovery for PairDiscovery {
    fn get_cluster_id(&self) -> u64 {
        42
    }
    fn get_keyspace_id(&self) -> u32 {
        0
    }
    fn get_keyspace_group_id(&self) -> u32 {
        0
    }
    fn get_service_urls(&self) -> Vec<String> {
        vec![self.leader_url.clone(), self.follower_url.clone()]
    }
    fn get_serving_url(&self) -> String {
        self.leader_url.clone()
    }
    fn get_backup_urls(&self) -> Vec<String> {
        vec![self.follower_url.clone()]
    }
    async fn get_or_create_grpc_conn(&self, url: &str) -> crate::Result<Channel> {
        if url == self.leader_url {
            Ok(self.leader_channel.lock().unwrap().clone())
        } else {
            Ok(self.follower_channel.clone())
        }
    }
    fn remove_client_conn(&self, _url: &str) {}
    fn schedule_check_member_changed(&self) {}
    async fn check_member_changed(&self) -> crate::Result<()> {
        Ok(())
    }
}

/// A channel to a port nothing listens on: building a stream fails with
/// `Unavailable`.
fn dead_channel() -> Channel {
    Channel::from_static("http://127.0.0.1:1").connect_lazy()
}

async fn pair_cli(discovery: Arc<PairDiscovery>, forwarding: bool, timeout: Duration) -> Arc<Cli> {
    let mut options = crate::pd::opt::Options::new();
    options.enable_forwarding = forwarding;
    let cli = Cli::new(
        &Cancellation::default(),
        Arc::new(options),
        timeout,
        discovery.clone(),
        StreamBuilderFactory::Pd,
    );
    cli.update_tso_leader_url(&discovery.leader_url);
    cli.setup().await;
    cli
}

/// Asks for timestamps until one is served or `within` elapses.
async fn get_timestamp_eventually(cli: &Arc<Cli>, within: Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), cli.get_timestamp()).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn metadata_client(server: &Server) -> Arc<RetryClient> {
    let timeout = Duration::from_secs(10);
    Arc::new(RetryClient::new_with_cluster(
        Arc::new(SecurityManager::default()),
        timeout,
        server.cluster(timeout).await,
    ))
}

async fn wait_region_entered(server: &Server) {
    tokio::time::timeout(
        Duration::from_secs(1),
        server.service.region_entered.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
}

#[tokio::test]
async fn source_pd_concurrency_metadata_requests_overlap() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let blocked = tokio::spawn(client.clone().get_region(b"blocked".to_vec()));
    wait_region_entered(&server).await;
    let others = tokio::time::timeout(Duration::from_millis(500), async {
        tokio::try_join!(
            client.clone().get_region(b"free".to_vec()),
            client.clone().get_all_stores(),
            client.clone().scan_regions(b"a".to_vec(), b"z".to_vec(), 1),
            client.clone().get_timestamp(),
        )
    })
    .await;
    server.service.region_release.add_permits(1);
    blocked.await.unwrap().unwrap();
    let (region, stores, scan, ts) = others
        .expect("Go does not serialize metadata RPCs")
        .unwrap();
    assert_eq!(region.region.id, 1);
    assert_eq!(stores[0].id, 9);
    assert_eq!(scan[0].region.start_key, b"a");
    assert_eq!(scan[0].region.end_key, b"z");
    assert_eq!(ts.physical, 100);
}

#[tokio::test]
async fn source_pd_concurrency_timestamp_wait_releases_cluster() {
    let server = Server::start(Reply::StallBody).await;
    let client = metadata_client(&server).await;
    let timestamp = tokio::spawn(client.clone().get_timestamp());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let metadata =
        tokio::time::timeout(Duration::from_millis(500), client.clone().get_all_stores()).await;
    timestamp.abort();
    assert!(timestamp.await.unwrap_err().is_cancelled());
    assert_eq!(
        metadata
            .expect("a pending TSO must not block metadata")
            .unwrap()[0]
            .id,
        9
    );
}

#[tokio::test]
async fn source_pd_concurrency_replacement_during_metadata_request() {
    let first = Server::start(Reply::Timestamp).await;
    let second =
        Server::start_with_clock(Reply::Timestamp, first.service.next_logical.clone()).await;
    second.service.region_id.store(2, Ordering::SeqCst);
    let client = metadata_client(&first).await;
    client.clone().get_timestamp().await.unwrap();
    let blocked = tokio::spawn(client.clone().get_region(b"blocked".to_vec()));
    wait_region_entered(&first).await;
    *first.service.leader_urls.write().unwrap() = vec![second.service.endpoint.clone()];
    let refresh = tokio::time::timeout(Duration::from_secs(1), client.reconnect_for_test()).await;
    first.service.region_release.add_permits(1);
    let old = blocked.await.unwrap().unwrap();
    refresh
        .expect("a retained request must not prevent leader replacement")
        .unwrap();
    assert_eq!(old.region.id, 1);
    assert_eq!(
        client
            .clone()
            .get_region(b"new".to_vec())
            .await
            .unwrap()
            .region
            .id,
        2
    );
    client.clone().get_timestamp().await.unwrap();
    assert_eq!(second.service.received.load(Ordering::SeqCst), 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while first.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replacement must still retire the previous TSO stream");
}

#[tokio::test]
async fn source_pd_concurrency_discovery_does_not_block_requests() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let first = client.clone().get_timestamp().await.unwrap();
    let members_before = server.service.member_requests.load(Ordering::SeqCst);
    server.service.stall_members.store(true, Ordering::SeqCst);
    let refreshing = client.clone();
    let refresh = tokio::spawn(async move { refreshing.reconnect_for_test().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.member_requests.load(Ordering::SeqCst) == members_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let metadata =
        tokio::time::timeout(Duration::from_millis(500), client.clone().get_all_stores()).await;
    refresh.abort();
    assert!(refresh.await.unwrap_err().is_cancelled());
    server.service.stall_members.store(false, Ordering::SeqCst);
    assert_eq!(
        metadata
            .expect("discovery must not hold the request publication lock")
            .unwrap()[0]
            .id,
        9
    );
    assert_eq!(
        client.clone().get_timestamp().await.unwrap().logical,
        first.logical + 1
    );
}

#[tokio::test]
async fn source_tso_batch_deadline_cancels_stalled_response() {
    let server = Server::start(Reply::StallBody).await;
    let cluster = server.cluster(Duration::from_millis(20)).await;
    let result = tokio::time::timeout(Duration::from_millis(500), cluster.get_timestamp()).await;
    assert_eq!(server.service.received.load(Ordering::SeqCst), 1);
    assert!(
        result.is_ok(),
        "a sent TSO batch must be bounded by the configured PD timeout"
    );
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn source_tso_batch_deadline_includes_response_headers() {
    let server = Server::start(Reply::StallHeaders).await;
    let cluster = server.cluster(Duration::from_millis(20)).await;
    let result = tokio::time::timeout(Duration::from_millis(500), cluster.get_timestamp()).await;
    assert_eq!(server.service.received.load(Ordering::SeqCst), 1);
    assert!(
        result.is_ok(),
        "a sent TSO batch must time out even before response headers arrive"
    );
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn source_tso_completed_batch_disarms_deadline_on_live_stream() {
    let server = Server::start(Reply::Timestamp).await;
    let cluster = server.cluster(Duration::from_millis(50)).await;
    let first = tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
        .await
        .unwrap()
        .unwrap();
    assert!(second.logical > first.logical);
    assert_eq!(
        server.service.dropped.load(Ordering::SeqCst),
        0,
        "batch completion must not close the stream"
    );
}

#[tokio::test]
async fn source_tso_drop_retires_stalled_stream_before_its_deadline() {
    let server = Server::start(Reply::StallBody).await;
    let cluster = server.cluster(Duration::from_secs(100)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), cluster.get_timestamp())
            .await
            .is_err()
    );
    assert_eq!(server.service.received.load(Ordering::SeqCst), 1);
    drop(cluster);
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the last oracle owner must retire its stalled stream");
}

#[tokio::test]
async fn source_tso_shared_stream_completes_concurrent_batches() {
    let server = Server::start(Reply::Timestamp).await;
    let cluster = server.cluster(Duration::from_secs(1)).await;
    let timestamps = tokio::time::timeout(
        Duration::from_secs(2),
        futures::future::try_join_all((0..256).map(|_| cluster.get_timestamp())),
    )
    .await
    .unwrap()
    .unwrap();
    let mut logical = timestamps.iter().map(|ts| ts.logical).collect::<Vec<_>>();
    logical.sort_unstable();
    assert_eq!(logical, (1..=256).collect::<Vec<_>>());
    assert!(timestamps.iter().all(|ts| ts.physical == 100));
    // The source-sized collector can complete this workload in one batch.
    assert!(server.service.received.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn source_tso_close_joins_worker_and_deadline_watcher() {
    let server = Server::start(Reply::StallBody).await;
    let cli = test_cli(
        &server,
        Duration::from_secs(100),
        crate::pd::opt::Options::new(),
    )
    .await;
    let pending = tokio::spawn({
        let cli = cli.clone();
        async move { cli.get_timestamp().await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(cli.close(), cli.close());
    })
    .await
    .unwrap();
    assert!(pending.await.unwrap().is_err());
    assert!(cli.is_closed_for_test());
    assert!(cli.get_timestamp().await.is_err());
}

#[tokio::test]
async fn source_tso_completed_result_survives_stream_retirement() {
    // Go Request waits for request/client context, not a retired stream's
    // context. An already completed batch must retain its timestamp.
    for _ in 0..64 {
        let client_ctx = Cancellation::default();
        let (mut request, future) = super::request::Request::new();
        let timestamp = Timestamp {
            physical: 123,
            logical: 456,
            suffix_bits: 0,
        };
        request.try_done(Ok(timestamp.clone()));
        client_ctx.cancel();
        assert_eq!(future.wait(&client_ctx).await.unwrap(), timestamp);
    }
}

#[tokio::test]
async fn source_connectionctx_metadata_refresh_reuses_healthy_tso() {
    let server = Server::start(Reply::Timestamp).await;
    let mut cluster = server.cluster(Duration::from_secs(1)).await;
    let first = cluster.get_timestamp().await.unwrap();
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    let second = cluster.get_timestamp().await.unwrap();
    assert_eq!(server.service.received.load(Ordering::SeqCst), 2);
    assert_eq!(
        second.logical,
        first.logical + 1,
        "metadata refresh must keep the same healthy leader stream"
    );
    assert_eq!(server.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_changed_leader_releases_old_stream() {
    let first = Server::start(Reply::Timestamp).await;
    let second =
        Server::start_with_clock(Reply::Timestamp, first.service.next_logical.clone()).await;
    let mut cluster = first.cluster(Duration::from_secs(1)).await;
    cluster.get_timestamp().await.unwrap();
    *first.service.leader_urls.write().unwrap() = vec![second.service.endpoint.clone()];
    let tso = cluster.tso_for_test();
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    // pd-client: the leader switch is the TSO client's leader-switched
    // callback; its connection-contexts updater replaces the stream.
    assert!(Arc::ptr_eq(&tso, &cluster.tso_for_test()));
    wait_for_streams(&tso, &[second.service.endpoint.clone()]).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while first.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("changing leaders must retire the previous stream");
    cluster.get_timestamp().await.unwrap();
    assert_eq!(first.service.received.load(Ordering::SeqCst), 1);
    assert_eq!(second.service.received.load(Ordering::SeqCst), 1);
    assert_eq!(second.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_cancelled_same_url_gets_a_new_stream() {
    let server = Server::start(Reply::StallBody).await;
    let timeout = Duration::from_millis(20);
    let mut cluster = server.cluster(timeout).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
            .await
            .unwrap()
            .is_err()
    );
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, timeout)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(
        server.service.received.load(Ordering::SeqCst),
        2,
        "a canceled same-URL context must not prevent stream replacement"
    );
}

#[tokio::test]
async fn source_connectionctx_failed_metadata_refresh_preserves_live_stream() {
    let server = Server::start(Reply::Timestamp).await;
    let mut cluster = server.cluster(Duration::from_secs(1)).await;
    let first = cluster.get_timestamp().await.unwrap();
    // Fail the membership observation itself. An accepted new leader is not a
    // failed observation merely because its timestamp connection is unavailable.
    server.service.member_failures.store(100, Ordering::SeqCst);
    assert!(Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .is_err());
    let second = cluster.get_timestamp().await.unwrap();
    assert_eq!(second.logical, first.logical + 1);
    assert_eq!(server.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_retains_selected_url_without_alias_failover() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.leader_urls.write().unwrap() = vec![
        "http://127.0.0.1:0".to_owned(),
        server.service.endpoint.clone(),
    ];
    // Go updateServiceClient/PickMatchedURL chooses the first matching scheme,
    // without probing aliases for reachability. A later working URL must not
    // silently replace that logical leader identity.
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    assert!(connection
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .is_err());
    *server.service.leader_urls.write().unwrap() = vec![
        server.service.endpoint.clone(),
        "http://127.0.0.1:0".to_owned(),
    ];
    let mut cluster = server.cluster(Duration::from_secs(1)).await;
    let first = cluster.get_timestamp().await.unwrap();
    *server.service.leader_urls.write().unwrap() = vec![server.service.endpoint.clone()];
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    let second = cluster.get_timestamp().await.unwrap();
    assert_eq!(second.logical, first.logical + 1);
    assert_eq!(server.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_release_cancels_retained_pending_stream_and_joins() {
    let server = Server::start(Reply::StallBody).await;
    let cli = test_cli(
        &server,
        Duration::from_secs(100),
        crate::pd::opt::Options::new(),
    )
    .await;
    let pending = tokio::spawn({
        let cli = cli.clone();
        async move { cli.get_timestamp().await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Go `conCtxMgr.ReleaseAll`: cancelling the stream's connection ctx ends
    // its receive loop, which fails the batch it was waiting on.
    let urls = cli.connection_urls_for_test();
    assert_eq!(urls, vec![server.service.endpoint.clone()]);
    cli.release_all_for_test();
    assert!(tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a released stream is retired on the wire");
    tokio::time::timeout(Duration::from_secs(1), cli.close())
        .await
        .unwrap();
    assert!(cli.is_closed_for_test());
}

#[tokio::test]
async fn tso_requests_report_get_ts_async_and_process_requests_spans() {
    let server = Server::start(Reply::Timestamp).await;
    let cli = test_cli(
        &server,
        Duration::from_secs(10),
        crate::pd::opt::Options::new(),
    )
    .await;
    let (handler, recorded) = crate::trace::client_span_testing::recorder();
    let timestamp = crate::trace::with_client_spans(handler, async {
        crate::trace::client_span("TiKVStore.getTimestampWithRetry", cli.get_timestamp()).await
    })
    .await;
    assert_eq!(timestamp.unwrap().logical, 1);
    // pd-client's `GetTSAsync` span is not put into the request ctx, so the
    // dispatcher's span is its sibling under the caller's span.
    assert_eq!(
        crate::trace::client_span_testing::render(&recorded),
        [
            "TiKVStore.getTimestampWithRetry",
            "  pdclient.GetTSAsync",
            "  pdclient.processRequests",
        ]
    );
    cli.close().await;
}

#[tokio::test]
async fn source_batch_default_tso_collects_twenty_thousand_prequeued_requests() {
    let server = Server::start(Reply::Timestamp).await;
    let provider = TestProvider::new(&server, crate::pd::opt::Options::new());
    let ctx = Cancellation::default();
    let dispatcher = super::dispatcher::TsoDispatcher::new(&ctx, provider.clone());
    // Go's request channel holds defaultMaxTSOBatchSize*2 requests.
    let mut futures = Vec::new();
    for _ in 0..20_000 {
        let (request, future) = super::request::Request::new();
        assert!(dispatcher.push(request).await.is_ok());
        futures.push(future);
    }
    let worker = tokio::spawn(dispatcher.clone().handle_dispatcher());
    let (request, future) = super::request::Request::new();
    assert!(dispatcher.push(request).await.is_ok());
    futures.push(future);
    for (index, future) in futures.into_iter().enumerate() {
        assert_eq!(future.wait(&ctx).await.unwrap().logical, index as i64 + 1);
    }
    assert_eq!(
        *server.service.request_counts.lock().unwrap(),
        vec![20_000, 1],
        "Go's default TSO controller takes the full prequeued source-sized batch"
    );
    ctx.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn source_batch_default_tso_holds_one_rpc_token_until_completion() {
    let server = Server::start(Reply::DrainRequests).await;
    let provider = TestProvider::new(&server, crate::pd::opt::Options::new());
    let ctx = Cancellation::default();
    let dispatcher = super::dispatcher::TsoDispatcher::new(&ctx, provider);
    let worker = tokio::spawn(dispatcher.clone().handle_dispatcher());
    let (first, _first) = super::request::Request::new();
    assert!(dispatcher.push(first).await.is_ok());
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let (second, _second) = super::request::Request::new();
    assert!(dispatcher.push(second).await.is_ok());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server.service.received.load(Ordering::SeqCst),
        1,
        "default Go TSO mode must not send a second RPC before completing the first"
    );
    assert_eq!(dispatcher.concurrency_for_test().await, (1, 1));
    assert_eq!(dispatcher.available_tokens_for_test(), 0);
    ctx.cancel();
    worker.await.unwrap();
}

/// Go `cancelCollectedRequests` returns the RPC token as it fails the batch,
/// so the dispatcher can put the next batch on the wire right away.
#[tokio::test]
async fn source_batch_discard_returns_rpc_token_before_request_completion() {
    let server = Server::start(Reply::StallBody).await;
    let provider = TestProvider::new(&server, crate::pd::opt::Options::new());
    let ctx = Cancellation::default();
    let dispatcher = super::dispatcher::TsoDispatcher::new(&ctx, provider.clone());
    let worker = tokio::spawn(dispatcher.clone().handle_dispatcher());
    let (request, future) = super::request::Request::new();
    assert!(dispatcher.push(request).await.is_ok());
    let received = |want: usize| {
        let received = server.service.received.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(5), async {
                while received.load(Ordering::SeqCst) < want {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .is_ok()
        }
    };
    assert!(received(1).await);
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        0,
        "the batch holds the token"
    );
    // Retire the stream under the batch: its receive loop fails the batch.
    provider.manager().release_all();
    assert!(future.wait(&ctx).await.is_err());
    let (request, _future) = super::request::Request::new();
    assert!(dispatcher.push(request).await.is_ok());
    assert!(
        received(2).await,
        "the failed batch returned its token, so the next batch is sent"
    );
    ctx.cancel();
    worker.await.unwrap();
}

/// Go puts a finished batch's controller back into `batchBufferPool`; the
/// next batch reuses it, without the finished requests.
#[tokio::test]
async fn source_batch_completed_controller_reuses_its_buffer_without_old_senders() {
    let server = Server::start(Reply::Timestamp).await;
    let provider = TestProvider::new(&server, crate::pd::opt::Options::new());
    let ctx = Cancellation::default();
    let dispatcher = super::dispatcher::TsoDispatcher::new(&ctx, provider);
    let worker = tokio::spawn(dispatcher.clone().handle_dispatcher());
    for expected in 1..=3 {
        let (request, future) = super::request::Request::new();
        assert!(dispatcher.push(request).await.is_ok());
        assert_eq!(future.wait(&ctx).await.unwrap().logical, expected);
    }
    // One controller collects while the other's batch is in flight.
    assert!(dispatcher.pooled_controllers_for_test() <= 2);
    assert_eq!(dispatcher.pooled_requests_for_test(), 0);
    ctx.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn source_retry_initialization_recovers_from_a_failed_member_probe() {
    let server = Server::start(Reply::Timestamp).await;
    server.service.member_failures.store(1, Ordering::SeqCst);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        crate::pd::RetryClient::connect(
            &[server.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            Duration::from_millis(100),
        ),
    )
    .await
    .expect("initialization retries must remain bounded");
    assert!(
        result.is_ok(),
        "Go retries initialization: {:?}",
        result.err()
    );
    assert!(server.service.member_requests.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn source_retry_member_probe_honors_its_timeout() {
    let server = Server::start(Reply::Timestamp).await;
    server.service.stall_members.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        Connection::new(Arc::new(SecurityManager::default()))
            .connect_cluster(
                &[server.service.endpoint.clone()],
                Duration::from_millis(50),
            )
            .await
    })
    .await
    .expect("GetMembers must honor the PD timeout");
    assert!(result.is_err());
}

#[tokio::test]
async fn source_pd_error_owner_reports_stream_eof() {
    let server = Server::start(Reply::End).await;
    let cli = test_cli(
        &server,
        Duration::from_secs(2),
        crate::pd::opt::Options::new(),
    )
    .await;
    let error = tokio::time::timeout(Duration::from_secs(5), cli.get_timestamp())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "[PD:client:ErrClientTSOStreamClosed]encountered TSO stream being closed unexpectedly"
    );
    assert!(
        matches!(error, Error::Pd(ref owner) if owner.definition() == crate::pd::errs::ERR_CLIENT_TSO_STREAM_CLOSED && owner.backtrace().is_some())
    );
    assert!(
        !crate::pd::errs::is_leader_change(Some(&error)),
        "Go's EOF path adds a stack wrapper; the helper compares the direct sentinel"
    );
    cli.close().await;
}

#[tokio::test]
async fn source_pd_error_owner_reports_tso_length() {
    let server = Server::start(Reply::Timestamp).await;
    server
        .service
        .scripted_tso
        .lock()
        .unwrap()
        .push_back(TsoResponse {
            count: 2,
            timestamp: Some(Timestamp {
                physical: 1,
                logical: 2,
                suffix_bits: 0,
            }),
            ..Default::default()
        });
    let cli = test_cli(
        &server,
        Duration::from_secs(5),
        crate::pd::opt::Options::new(),
    )
    .await;
    let error = tokio::time::timeout(Duration::from_secs(5), cli.get_timestamp())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "[pd] tso length in rpc response is incorrect"
    );
    assert!(
        matches!(error, Error::Pd(owner) if owner.definition() == crate::pd::errs::ERR_TSO_LENGTH && owner.backtrace().is_some())
    );
    cli.close().await;
}

async fn owning_pd_client(
    server: &Server,
) -> Arc<crate::pd::PdRpcClient<crate::mock::MockKvConnect>> {
    let timeout = Duration::from_secs(100);
    let cluster = server.cluster(timeout).await;
    Arc::new(
        crate::pd::PdRpcClient::new(
            crate::Config::default(),
            |_| crate::mock::MockKvConnect,
            |security| async move { Ok(RetryClient::new_with_cluster(security, timeout, cluster)) },
            false,
        )
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn source_pd_shutdown_closes_stalled_requests_and_retained_handles() {
    use crate::pd::PdClient as _;
    let server = Server::start(Reply::StallBody).await;
    let client = owning_pd_client(&server).await;
    let mut pending = tokio::spawn(client.clone().get_timestamp());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.close().await;
    let result = tokio::time::timeout(Duration::from_millis(500), &mut pending).await;
    if result.is_err() {
        pending.abort();
        let _ = pending.await;
    }
    assert!(
        matches!(result, Ok(Ok(Err(_)))),
        "close must finish pending TSO before its 100-second deadline: {result:?}"
    );
    assert_eq!(client.cluster_id().await, 42);
    assert!(
        client.all_stores().await.is_err(),
        "retained handles must not send metadata after close"
    );
    assert!(client.clone().get_timestamp().await.is_err());
    client.close().await;
}

#[tokio::test]
async fn source_pd_shutdown_concurrent_close_joins_background_tasks() {
    let server = Server::start(Reply::Timestamp).await;
    let client = owning_pd_client(&server).await;
    let stopped = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let task_stopped = stopped.clone();
    let task_release = release.clone();
    assert!(client
        .region_cache()
        .spawn_background_task(move |cancellation| async move {
            cancellation.cancelled().await;
            task_stopped.add_permits(1);
            task_release.acquire().await.unwrap().forget();
        }));
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.close().await });
    tokio::time::timeout(Duration::from_secs(1), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let second = client.close();
    tokio::pin!(second);
    let early = tokio::time::timeout(Duration::from_millis(50), &mut second)
        .await
        .is_ok();
    release.add_permits(1);
    first.await.unwrap();
    if !early {
        second.await;
    }
    assert!(
        !early,
        "every close caller must wait for the same background completion"
    );
}

#[tokio::test]
async fn source_pd_shutdown_interrupted_close_can_finish_joining() {
    let server = Server::start(Reply::Timestamp).await;
    let client = owning_pd_client(&server).await;
    let stopped = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (task_stopped, task_release, task_completed) =
        (stopped.clone(), release.clone(), completed.clone());
    assert!(client
        .region_cache()
        .spawn_background_task(move |cancellation| async move {
            cancellation.cancelled().await;
            task_stopped.add_permits(1);
            task_release.acquire().await.unwrap().forget();
            task_completed.store(true, Ordering::SeqCst);
        }));
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.close().await });
    tokio::time::timeout(Duration::from_secs(1), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let resumed = client.close();
    tokio::pin!(resumed);
    let early = tokio::time::timeout(Duration::from_millis(50), &mut resumed)
        .await
        .is_ok();
    release.add_permits(1);
    if !early {
        resumed.await;
    }
    assert!(
        !early,
        "cancelling close must not discard its unfinished joins"
    );
    assert!(completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn source_pd_shutdown_cancels_metadata_and_discovery_without_reconnecting() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let retained_oracle = client.tso_for_test().await;
    client.clone().get_timestamp().await.unwrap();
    let request = tokio::spawn(client.clone().get_region(b"blocked".to_vec()));
    wait_region_entered(&server).await;
    let members_before = server.service.member_requests.load(Ordering::SeqCst);
    server.service.stall_members.store(true, Ordering::SeqCst);
    let refresh_client = client.clone();
    let refresh = tokio::spawn(async move { refresh_client.reconnect_for_test().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.member_requests.load(Ordering::SeqCst) == members_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), client.close())
        .await
        .unwrap();
    assert!(matches!(
        request.await.unwrap(),
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        refresh.await.unwrap(),
        Err(Error::ContextCanceled)
    ));
    assert!(retained_oracle.is_closed_for_test());
    let members_after = server.service.member_requests.load(Ordering::SeqCst);
    assert!(matches!(
        client.reconnect_for_test().await,
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        client.clone().get_timestamp().await,
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        client.load_keyspace("DEFAULT").await,
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        client.clone().get_all_stores().await,
        Err(Error::ContextCanceled)
    ));
    assert_eq!(
        server.service.member_requests.load(Ordering::SeqCst),
        members_after
    );
    assert_eq!(client.cluster_id().await, 42);
}

#[tokio::test]
async fn source_pd_shutdown_joins_retirement_after_interrupted_reconnect() {
    let first = Server::start(Reply::Timestamp).await;
    let second =
        Server::start_with_clock(Reply::Timestamp, first.service.next_logical.clone()).await;
    let client = metadata_client(&first).await;
    client.clone().get_timestamp().await.unwrap();
    let old = client.tso_for_test().await;
    // Stop retirement at its join, after replacement has already been published.
    let held_join = old.close_gate_for_test().lock().await;
    // A new keyspace is a new TSO provider: pd-client replaces the client
    // (`resetTSOClientLocked`), retiring the old one. A leader change alone
    // only switches the same client's leader.
    let _ = &second;
    let meta = crate::proto::keyspacepb::KeyspaceMeta {
        keyspace: Some(crate::proto::keyspacepb::keyspace_meta::Keyspace::Id(5)),
        ..Default::default()
    };
    let refreshing = client.clone();
    let refresh = tokio::spawn(async move { refreshing.set_keyspace(&meta).await });
    tokio::time::timeout(Duration::from_secs(1), old.ctx().cancelled())
        .await
        .unwrap();
    refresh.abort();
    assert!(refresh.await.unwrap_err().is_cancelled());
    let current = client.tso_for_test().await;
    client.clone().get_timestamp().await.unwrap();
    let close = client.close();
    tokio::pin!(close);
    let early = tokio::time::timeout(Duration::from_millis(50), &mut close)
        .await
        .is_ok();
    drop(held_join);
    if !early {
        tokio::time::timeout(Duration::from_secs(1), close)
            .await
            .unwrap();
    }
    assert!(
        !early,
        "close must retain and join the retired connection too"
    );
    assert!(old.is_closed_for_test());
    assert!(current.is_closed_for_test());
}

#[tokio::test]
async fn source_pd_shutdown_cancels_background_rpc_before_closing_pd() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let cache = Arc::new(crate::region_cache::RegionCache::new(client.clone()));
    server.service.stall_scans.store(true, Ordering::SeqCst);
    cache.start_background_refresh(Duration::from_millis(5));
    wait_region_entered(&server).await;
    let closed =
        tokio::time::timeout(Duration::from_millis(500), cache.close_background_task()).await;
    // Cleanup the failure path before asserting, without shortening the RPC deadline.
    server.service.stall_scans.store(false, Ordering::SeqCst);
    server.service.region_release.add_permits(1);
    assert!(
        closed.is_ok(),
        "cache close must cancel its RPC before closing the PD owner"
    );
    assert!(!cache.spawn_background_task(|_| async { panic!("closed cache accepted work") }));
    assert_eq!(client.clone().get_all_stores().await.unwrap()[0].id, 9);
    client.close().await;
}

impl tonic::server::UnaryService<pdpb::GetClusterInfoRequest> for PdServer {
    type Response = pdpb::GetClusterInfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<pdpb::GetClusterInfoRequest>) -> Self::Future {
        let response = self.cluster_info.read().unwrap().clone();
        let failure = self.cluster_info_failure.load(Ordering::SeqCst);
        self.mode_requests.fetch_add(1, Ordering::SeqCst);
        let stall = self.stall_mode.load(Ordering::SeqCst);
        Box::pin(async move {
            if stall {
                futures::future::pending::<()>().await;
            }
            if failure {
                return Err(tonic::Status::unavailable(
                    "PD mode observation unavailable",
                ));
            }
            response
                .map(tonic::Response::new)
                .ok_or_else(|| tonic::Status::unimplemented("old PD"))
        })
    }
}

#[tokio::test]
async fn source_service_empty_modes_rejected() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse::default());
    let result = Connection::new(Arc::new(SecurityManager::default()))
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await;
    assert!(
        result.is_err(),
        "empty service modes must not select classic PD Tso"
    );
}

#[tokio::test]
async fn source_service_api_missing_tso_never_uses_pd_leader() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        ..Default::default()
    });
    let result = Connection::new(Arc::new(SecurityManager::default()))
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await;
    assert!(
        result.is_err(),
        "failed TSO discovery must not select classic PD Tso"
    );
    assert_eq!(server.service.received.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct TsoServer(PdServer);
impl tonic::server::NamedService for TsoServer {
    const NAME: &'static str = "tsopb.TSO";
}
impl<B> Service<http::Request<B>> for TsoServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        self.0.wire_routes.lock().unwrap().push((
            request.uri().path().to_owned(),
            request
                .headers()
                .get("pd-forwarded-host")
                .map(|v| v.to_str().unwrap().to_owned()),
            request.headers().contains_key("pd-allow-follower-handle"),
        ));

        let service = self.clone();
        match request.uri().path() {
            "/tsopb.TSO/FindGroupByKeyspaceID" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(service, request)
                        .await,
                )
            }),
            "/tsopb.TSO/Tso" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .streaming(service, request)
                        .await,
                )
            }),
            _ => unreachable!(),
        }
    }
}
impl tonic::server::UnaryService<tsopb::FindGroupByKeyspaceIdRequest> for TsoServer {
    type Response = tsopb::FindGroupByKeyspaceIdResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(
        &mut self,
        request: tonic::Request<tsopb::FindGroupByKeyspaceIdRequest>,
    ) -> Self::Future {
        let service = self.0.clone();
        Box::pin(async move {
            assert!(request.metadata().contains_key("grpc-timeout"));
            let request = request.into_inner();
            service
                .requested_revisions
                .lock()
                .unwrap()
                .push(request.mod_revision);
            let header = request.header.unwrap();
            let required = service.required_keyspace.load(Ordering::SeqCst);
            if required != 0
                && header.keyspace
                    != Some(tsopb::request_header::Keyspace::KeyspaceId(required as u32))
            {
                return Err(tonic::Status::not_found("default keyspace has no group"));
            }
            assert_eq!(header.cluster_id, 42);
            assert_eq!(
                header.callee_id,
                service.endpoint.strip_prefix("http://").unwrap()
            );
            service.discovery_requests.fetch_add(1, Ordering::SeqCst);
            if service.stall_discovery.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(tonic::Response::new(tsopb::FindGroupByKeyspaceIdResponse {
                header: Some(tsopb::ResponseHeader {
                    cluster_id: 42,
                    error: service
                        .discovery_error
                        .lock()
                        .unwrap()
                        .clone()
                        .map(|message| tsopb::Error {
                            message,
                            ..Default::default()
                        }),
                    ..Default::default()
                }),
                keyspace_group: Some(service.group.read().unwrap().clone()),
                mod_revision: service.revision.load(Ordering::SeqCst) as u64,
            }))
        })
    }
}
impl tonic::server::StreamingService<tsopb::TsoRequest> for TsoServer {
    type Response = tsopb::TsoResponse;
    type ResponseStream =
        Pin<Box<dyn Stream<Item = std::result::Result<Self::Response, tonic::Status>> + Send>>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(
        &mut self,
        request: tonic::Request<tonic::Streaming<tsopb::TsoRequest>>,
    ) -> Self::Future {
        let service = self.0.clone();
        Box::pin(async move {
            let active = ActiveStream(service.dropped.clone());
            let stream = futures::stream::unfold(
                (request.into_inner(), active, service),
                |(mut requests, active, service)| async move {
                    let request = requests.message().await.unwrap()?;
                    service.received.fetch_add(1, Ordering::SeqCst);
                    let header = request.header.unwrap();
                    assert_eq!(header.cluster_id, 42);
                    assert_eq!(
                        header.callee_id,
                        service.endpoint.strip_prefix("http://").unwrap()
                    );
                    service.tso_headers.lock().unwrap().push(header.clone());
                    let logical = (service
                        .next_logical
                        .fetch_add(request.count as usize, Ordering::SeqCst)
                        + request.count as usize) as i64;
                    Some((
                        Ok(tsopb::TsoResponse {
                            header: Some(tsopb::ResponseHeader {
                                cluster_id: 42,
                                keyspace_group_id: header.keyspace_group_id,
                                ..Default::default()
                            }),
                            count: request.count,
                            timestamp: Some(pdpb::Timestamp {
                                physical: 200,
                                logical,
                                suffix_bits: 0,
                            }),
                        }),
                        (requests, active, service),
                    ))
                },
            );
            Ok(tonic::Response::new(
                Box::pin(stream) as Self::ResponseStream
            ))
        })
    }
}

fn api_mode(pd: &Server, tso: &Server) {
    *pd.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![tso.service.endpoint.clone()],
        ..Default::default()
    });
}

#[tokio::test]
async fn source_service_routes_keyspace_and_retires_on_group_move() {
    crate::logutil::test_capture::install();
    let pd = Server::start(Reply::Timestamp).await;
    let first = Server::start(Reply::Timestamp).await;
    let second =
        Server::start_with_clock(Reply::Timestamp, first.service.next_logical.clone()).await;
    api_mode(&pd, &first);
    let client = metadata_client(&pd).await;
    let timestamp = client.clone().get_timestamp().await.unwrap();
    assert_eq!(timestamp.physical, 200);
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
    let old = client.tso_for_test().await;
    client.reconnect_for_test().await.unwrap();
    assert!(
        !old.ctx().is_cancelled(),
        "unchanged route must retain stream"
    );
    let meta = crate::proto::keyspacepb::KeyspaceMeta {
        keyspace: Some(crate::proto::keyspacepb::keyspace_meta::Keyspace::Id(27)),
        config: [("tso_keyspace_group_id".into(), "9".into())].into(),
        ..Default::default()
    };
    client.set_keyspace(&meta).await.unwrap();
    assert!(old.ctx().is_cancelled());
    client.clone().get_timestamp().await.unwrap();
    let headers = first.service.tso_headers.lock().unwrap().clone();
    assert_eq!(
        headers.last().unwrap().keyspace,
        Some(tsopb::request_header::Keyspace::KeyspaceId(27))
    );
    let old = client.tso_for_test().await;
    *first.service.group.write().unwrap() = tsopb::KeyspaceGroup {
        id: 9,
        members: vec![tsopb::KeyspaceGroupMember {
            address: second.service.endpoint.clone(),
            is_primary: true,
        }],
        ..Default::default()
    };
    first.service.revision.store(2, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    // pd-client: a primary switch is the TSO client's leader-switched
    // callback; the same client moves its stream to the new primary.
    let current = client.tso_for_test().await;
    assert!(Arc::ptr_eq(&old, &current));
    assert_eq!(current.leader_url(), second.service.endpoint);
    wait_for_streams(&current, &[second.service.endpoint.clone()]).await;
    client.clone().get_timestamp().await.unwrap();
    assert_eq!(
        second.service.tso_headers.lock().unwrap()[0].keyspace_group_id,
        9
    );
    // pd-client tsoServiceDiscovery's lines for the primary switch.
    for expected in [
        format!(
            "INFO [tso] switch primary new-primary={} old-primary={}",
            second.service.endpoint, first.service.endpoint
        ),
        format!(
            "INFO [tso] switch the tso leader serving url new-url={}",
            second.service.endpoint
        ),
    ] {
        assert!(
            crate::logutil::test_capture::any(|line| line == expected),
            "{expected:?} not in {:?}",
            crate::logutil::test_capture::lines()
        );
    }
    let group_changed = "INFO [tso] the keyspace group changed keyspace-id=27 new-keyspace-group-id=9 old-keyspace-group-id=";
    assert!(crate::logutil::test_capture::any(
        |line| line.starts_with(group_changed)
    ));
    let updated = "INFO [tso] updated keyspace group service discovery info keyspace-id-in-request=27 keyspace-group-service=id:9 ";
    assert!(crate::logutil::test_capture::any(
        |line| line.starts_with(updated)
    ));
    first.service.revision.store(1, Ordering::SeqCst);
    assert!(client.reconnect_for_test().await.is_err());
    // A stale observation must not replace the accepted route.
    client.clone().get_timestamp().await.unwrap();
    assert_eq!(second.service.received.load(Ordering::SeqCst), 2);
    client.close().await;
}

#[tokio::test]
async fn source_service_background_mode_switch_and_close() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    let client = Arc::new(
        RetryClient::connect(
            &[pd.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            Duration::from_secs(1),
        )
        .await
        .unwrap(),
    );
    let classic = client.clone().get_timestamp().await.unwrap();
    assert_eq!(classic.physical, 100);
    let old = client.tso_for_test().await;
    api_mode(&pd, &tso);
    tokio::time::timeout(Duration::from_secs(6), async {
        while !old.ctx().is_cancelled() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(client.clone().get_timestamp().await.unwrap().physical, 200);
    let active = client.tso_for_test().await;
    client.close().await;
    assert!(active.ctx().is_cancelled());
    let probes = tso.service.discovery_requests.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(3100)).await;
    assert_eq!(
        tso.service.discovery_requests.load(Ordering::SeqCst),
        probes
    );
    assert!(client.clone().get_timestamp().await.is_err());
}

#[tokio::test]
async fn source_service_header_errors_do_not_fall_back() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        header: Some(pdpb::ResponseHeader {
            error: Some(pdpb::Error {
                message: "discovery unavailable".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        service_modes: vec![pdpb::ServiceMode::PdSvcMode as i32],
        ..Default::default()
    });
    assert!(Connection::new(Arc::new(SecurityManager::default()))
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .is_err());
}

#[tokio::test]
async fn source_service_failed_initial_probe_rotates_to_healthy_endpoint() {
    let pd = Server::start(Reply::Timestamp).await;
    let a = Server::start(Reply::Timestamp).await;
    let b = Server::start(Reply::Timestamp).await;
    let (stalled, healthy) = if a.service.endpoint < b.service.endpoint {
        (&a, &b)
    } else {
        (&b, &a)
    };
    stalled
        .service
        .stall_discovery
        .store(true, Ordering::SeqCst);
    *pd.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![
            stalled.service.endpoint.clone(),
            healthy.service.endpoint.clone(),
        ],
        ..Default::default()
    });
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    let endpoints = [pd.service.endpoint.clone()];
    assert!(connection
        .connect_cluster(&endpoints, Duration::from_millis(100))
        .await
        .is_err());
    let cluster = connection
        .connect_cluster(&endpoints, Duration::from_secs(1))
        .await
        .expect("failed probes must advance selection even before initial publication");
    let client = Arc::new(RetryClient::new_with_cluster(
        Arc::new(SecurityManager::default()),
        Duration::from_secs(1),
        cluster,
    ));
    assert_eq!(client.clone().get_timestamp().await.unwrap().physical, 200);
    assert_eq!(stalled.service.discovery_requests.load(Ordering::SeqCst), 1);
    assert_eq!(healthy.service.discovery_requests.load(Ordering::SeqCst), 1);
    client.close().await;
}

impl tonic::server::UnaryService<pdpb::GetMinTsRequest> for PdServer {
    type Response = pdpb::GetMinTsResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<pdpb::GetMinTsRequest>) -> Self::Future {
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        self.min_requests.fetch_add(1, Ordering::SeqCst);
        let response = self.min_response.read().unwrap().clone();
        Box::pin(async move { response.map(tonic::Response::new) })
    }
}

#[derive(Clone)]
struct KeyspaceServer(PdServer);
impl tonic::server::NamedService for KeyspaceServer {
    const NAME: &'static str = "keyspacepb.Keyspace";
}
impl<B> Service<http::Request<B>> for KeyspaceServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let service = self.clone();
        assert_eq!(request.uri().path(), "/keyspacepb.Keyspace/LoadKeyspace");
        Box::pin(async move {
            Ok(
                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(service, request)
                    .await,
            )
        })
    }
}
impl tonic::server::UnaryService<keyspacepb::LoadKeyspaceRequest> for KeyspaceServer {
    type Response = keyspacepb::LoadKeyspaceResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<keyspacepb::LoadKeyspaceRequest>) -> Self::Future {
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        self.0.keyspace_loads.fetch_add(1, Ordering::SeqCst);
        let name = request.into_inner().name;
        Box::pin(async move {
            Ok(tonic::Response::new(keyspacepb::LoadKeyspaceResponse {
                // Go's generated getters accept a nil error header.
                header: None,
                keyspace: Some(keyspacepb::KeyspaceMeta {
                    keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(7)),
                    name,
                    state: keyspacepb::KeyspaceState::Enabled as i32,
                    config: [("tso_keyspace_group_id".into(), "4".into())].into(),
                    ..Default::default()
                }),
            }))
        })
    }
}

#[tokio::test]
async fn source_provider_classic_minimum_uses_tso() {
    let pd = Server::start(Reply::Timestamp).await;
    let mut cluster = pd.cluster(Duration::from_secs(1)).await;
    let result = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    cluster.start_close().await;
    assert!(
        result.is_ok(),
        "classic minimum must allocate ordinary TSO: {result:?}"
    );
    assert_eq!(pd.service.min_requests.load(Ordering::SeqCst), 0);
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn source_provider_api_minimum_compatibility_and_errors() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    let mut cluster = pd.cluster(Duration::from_secs(1)).await;
    let compatible = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    *pd.service.min_response.write().unwrap() = Ok(pdpb::GetMinTsResponse {
        timestamp: Some(Timestamp {
            physical: 99,
            logical: 3,
            suffix_bits: 0,
        }),
        ..Default::default()
    });
    let minimum = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    *pd.service.min_response.write().unwrap() = Err(tonic::Status::unavailable("API unavailable"));
    let unavailable = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    *pd.service.min_response.write().unwrap() = Ok(pdpb::GetMinTsResponse {
        header: Some(ResponseHeader {
            error: Some(pdpb::Error {
                message: "not ready".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    });
    let header_error = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    cluster.start_close().await;
    assert_eq!(compatible.unwrap().physical, 200);
    assert_eq!(minimum.unwrap().physical, 99);
    assert!(unavailable.is_err());
    assert!(header_error.is_err());
    assert_eq!(
        tso.service.received.load(Ordering::SeqCst),
        1,
        "only compatibility may allocate TSO"
    );
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
}

/// pd-client `GetOperator` returns PD's response as it is: a region with no
/// operator answers a header error, which `CheckRegionInScattering` and
/// `WaitScatterRegionFinish` read as "not scattering" -- not a failure to
/// retry until the backoff runs out.
#[tokio::test]
async fn get_operator_returns_pds_header_error_in_the_response() {
    let pd = Server::start(Reply::Timestamp).await;
    let mut cluster = pd.cluster(Duration::from_secs(1)).await;
    let response = cluster.get_operator(7, Duration::from_secs(1)).await;
    cluster.start_close().await;
    let response = response.unwrap();
    assert_eq!(response.header.unwrap().error.unwrap().message, "Not Found");
    assert!(response.desc.is_empty());
}

#[tokio::test]
async fn source_provider_optional_metadata_header_preserves_payload() {
    let pd = Server::start(Reply::Timestamp).await;
    pd.service
        .omit_metadata_header
        .store(true, Ordering::SeqCst);
    let mut cluster = pd.cluster(Duration::from_secs(1)).await;
    let stores = cluster.get_all_stores(Duration::from_secs(1)).await;
    cluster.start_close().await;
    assert_eq!(stores.unwrap().stores[0].id, 9);
}

#[tokio::test]
async fn source_provider_v2_bootstraps_without_default_group() {
    use crate::pd::PdClient as _;
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    tso.service.required_keyspace.store(7, Ordering::SeqCst);
    tso.service.group.write().unwrap().id = 4;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        crate::pd::PdRpcClient::connect_with_keyspace(
            &[pd.service.endpoint.clone()],
            crate::Config::default(),
            crate::request::KeyMode::Txn,
            "tenant".into(),
        ),
    )
    .await;
    let client = Arc::new(
        result
            .expect("V2 bootstrap must not wait for the nonexistent default group")
            .unwrap(),
    );
    let ts = client.clone().get_timestamp().await.unwrap();
    client.close().await;
    assert_eq!(ts.physical, 200);
    assert_eq!(pd.service.keyspace_loads.load(Ordering::SeqCst), 1);
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
    let headers = tso.service.tso_headers.lock().unwrap();
    assert!(headers.iter().all(|h| h.keyspace_group_id == 4
        && h.keyspace == Some(tsopb::request_header::Keyspace::KeyspaceId(7))));
}

#[tokio::test]
async fn source_channel_batch_membership_keyspace_and_tso_share_connection() {
    let pd = Server::start(Reply::Timestamp).await;
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    let (mut cluster, _) = connection
        .connect_cluster_for_keyspace(
            &[pd.service.endpoint.clone()],
            Duration::from_secs(1),
            Some("app"),
        )
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    cluster.start_close().await;
    assert_eq!(pd.service.keyspace_loads.load(Ordering::SeqCst), 1);
    assert_eq!(
        pd.service.connections.load(Ordering::SeqCst),
        1,
        "membership, keyspace, discovery, TSO and refresh must share a channel"
    );
}

#[tokio::test]
async fn source_channel_batch_periodic_discovery_keeps_bootstrap_connection() {
    let pd = Server::start(Reply::Timestamp).await;
    let client = Arc::new(
        RetryClient::connect(
            &[pd.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            Duration::from_secs(1),
        )
        .await
        .unwrap(),
    );
    client.clone().get_timestamp().await.unwrap();
    let previous = pd.service.member_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(65), async {
        while pd.service.member_requests.load(Ordering::SeqCst) <= previous {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    client.clone().get_timestamp().await.unwrap();
    client.close().await;
    assert_eq!(
        pd.service.connections.load(Ordering::SeqCst),
        1,
        "the retry owner must retain the bootstrap channel map"
    );
}

#[tokio::test]
async fn source_channel_batch_failed_or_cancelled_dial_does_not_poison_cache() {
    use crate::pd::service_discovery::ChannelCache;
    let cache = ChannelCache::default();
    let pd = Server::start(Reply::Timestamp).await;
    let endpoint = &pd.service.endpoint;
    assert!(cache
        .get_or_connect(endpoint, || async {
            Err(tonic::Status::unavailable("dial failed"))
        })
        .await
        .is_err());
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        cache.get_or_connect(endpoint, || std::future::pending()),
    )
    .await;
    assert!(result.is_err());
    let channel = cache
        .get_or_connect(endpoint, || async {
            SecurityManager::default()
                .connect(endpoint, |channel| channel)
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))
        })
        .await
        .unwrap();
    pdpb::pd_client::PdClient::new(channel)
        .get_members(GetMembersRequest::default())
        .await
        .unwrap();
    cache
        .get_or_insert_with(endpoint, || panic!("successful channel must be retained"))
        .unwrap();
    assert_eq!(pd.service.connections.load(Ordering::SeqCst), 1);
    cache.close();
    cache.close();
    assert_eq!(
        cache
            .get_or_insert_with(endpoint, || panic!("closed cache must not dial"))
            .unwrap_err()
            .code(),
        tonic::Code::Cancelled
    );
}

#[tokio::test]
async fn source_channel_batch_close_rejects_inflight_publication() {
    use crate::pd::service_discovery::ChannelCache;
    let cache = ChannelCache::default();
    let pd = Server::start(Reply::Timestamp).await;
    let result = cache
        .get_or_connect(&pd.service.endpoint, || async {
            cache.close();
            Ok(
                tonic::transport::Channel::from_shared(pd.service.endpoint.clone())
                    .unwrap()
                    .connect_lazy(),
            )
        })
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Cancelled);
    assert!(cache
        .get_or_insert_with(&pd.service.endpoint, || panic!("closed cache"))
        .is_err());
}

#[tokio::test]
async fn source_channel_batch_concurrent_dials_publish_one_winner() {
    use crate::pd::service_discovery::ChannelCache;
    let cache = ChannelCache::default();
    let a = Server::start(Reply::Timestamp).await;
    let b = Server::start(Reply::Timestamp).await;
    // Both calls enter construction before either may publish. The second
    // candidate deliberately addresses another fixture so observed RPC peers
    // prove that both returned handles use the winner, not their own candidate.
    let barrier = tokio::sync::Barrier::new(2);
    let dial = |endpoint: String| {
        let barrier = &barrier;
        async move {
            barrier.wait().await;
            tonic::transport::Channel::from_shared(endpoint)
                .unwrap()
                .connect()
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))
        }
    };
    let (first, second) = tokio::join!(
        cache.get_or_connect("same-discovery-key", || dial(a.service.endpoint.clone())),
        cache.get_or_connect("same-discovery-key", || dial(b.service.endpoint.clone())),
    );
    for channel in [first.unwrap(), second.unwrap()] {
        pdpb::pd_client::PdClient::new(channel)
            .get_members(GetMembersRequest::default())
            .await
            .unwrap();
    }
    let counts = (
        a.service.member_requests.load(Ordering::SeqCst),
        b.service.member_requests.load(Ordering::SeqCst),
    );
    assert!(
        matches!(counts, (2, 0) | (0, 2)),
        "only the published winner serves both RPCs: {counts:?}"
    );
    cache.close();
}

#[tokio::test]
async fn pd_region_batch_native_cache_permission_fallback_and_close() {
    let (leader, follower, client) = availability_pair().await;
    for _ in 0..2 {
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await
            .unwrap();
    }
    assert_eq!(
        *follower.service.region_metadata.lock().unwrap(),
        vec![true]
    );
    client
        .clone()
        .get_region_for_cache(b"wire".to_vec(), false, true)
        .await
        .unwrap();
    client.set_enable_follower_handle(false);
    client
        .clone()
        .get_region_for_cache(b"wire".to_vec(), false, false)
        .await
        .unwrap();
    assert_eq!(follower.service.region_metadata.lock().unwrap().len(), 1);
    client.set_enable_follower_handle(true);
    for failure in [1, 2] {
        follower
            .service
            .region_failure
            .store(failure, Ordering::SeqCst);
        for _ in 0..2 {
            client
                .clone()
                .get_region_for_cache(b"wire".to_vec(), false, false)
                .await
                .unwrap();
        }
    }
    assert_eq!(follower.service.region_metadata.lock().unwrap().len(), 3);
    follower.service.region_failure.store(0, Ordering::SeqCst);
    for _ in 0..2 {
        client
            .clone()
            .scan_regions(b"a".to_vec(), b"z".to_vec(), 1)
            .await
            .unwrap();
    }
    assert_eq!(
        follower.service.region_metadata.lock().unwrap().len(),
        3,
        "REGION_NOT_FOUND suppresses the follower across region API variants"
    );
    assert!(leader
        .service
        .region_metadata
        .lock()
        .unwrap()
        .iter()
        .all(|allowed| !allowed));
    client.close().await;
    assert!(matches!(
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await,
        Err(crate::Error::ContextCanceled)
    ));
    drop(leader);
    drop(follower);
}

async fn availability_pair() -> (Server, Server, Arc<RetryClient>) {
    availability_pair_with_forwarding(false).await
}

async fn availability_pair_with_forwarding(enabled: bool) -> (Server, Server, Arc<RetryClient>) {
    availability_pair_with_proxy(enabled, false).await
}

async fn availability_pair_with_proxy(
    enabled: bool,
    proxy: bool,
) -> (Server, Server, Arc<RetryClient>) {
    availability_pair_with_reply(enabled, proxy, Reply::Timestamp).await
}

async fn availability_pair_with_reply(
    enabled: bool,
    proxy: bool,
    reply: Reply,
) -> (Server, Server, Arc<RetryClient>) {
    let leader = Server::start(reply).await;
    let follower =
        Server::start_with_clock(Reply::Timestamp, leader.service.next_logical.clone()).await;
    if proxy {
        leader.service.health_status.store(2, Ordering::SeqCst);
    }
    let members = vec![
        pdpb::Member {
            member_id: 1,
            client_urls: vec![leader.service.endpoint.clone()],
            ..Default::default()
        },
        pdpb::Member {
            member_id: 2,
            client_urls: vec![follower.service.endpoint.clone()],
            ..Default::default()
        },
    ];
    for server in [&leader, &follower] {
        *server.service.region_members.write().unwrap() = Some(members.clone());
        *server.service.leader_urls.write().unwrap() = vec![leader.service.endpoint.clone()];
    }
    let client = Arc::new(
        RetryClient::connect_with_options(
            &[leader.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            {
                let mut options = crate::pd::opt::Options::new();
                options.timeout = Duration::from_secs(1);
                options.enable_forwarding = enabled;
                options.set_enable_tso_follower_proxy(proxy);
                options
            },
            None,
        )
        .await
        .unwrap(),
    );
    client.set_enable_follower_handle(true);
    (leader, follower, client)
}

#[tokio::test]
async fn pd_availability_batch_health_recovers_and_close_joins_stalled_probe() {
    let (leader, follower, client) = availability_pair().await;
    follower.service.health_status.store(2, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while follower.service.health_requests.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Let the health response be consumed before asserting the next route.
        tokio::time::sleep(Duration::from_millis(30)).await;
    })
    .await
    .expect("idle PD client must check health independently of TSO discovery");
    for _ in 0..4 {
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await
            .unwrap();
    }
    assert!(follower.service.region_metadata.lock().unwrap().is_empty());
    follower.service.health_status.store(1, Ordering::SeqCst);
    let probes = follower.service.health_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while follower.service.health_requests.load(Ordering::SeqCst) == probes {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    })
    .await
    .unwrap();
    for _ in 0..2 {
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await
            .unwrap();
    }
    assert_eq!(follower.service.region_metadata.lock().unwrap().len(), 1);
    leader.service.health_stall.store(true, Ordering::SeqCst);
    let probes = leader.service.health_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while leader.service.health_requests.load(Ordering::SeqCst) == probes {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_millis(250), client.close())
        .await
        .expect("close must cancel and join a stalled health RPC");
    let probes = leader.service.health_requests.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        leader.service.health_requests.load(Ordering::SeqCst),
        probes
    );
}

async fn unavailable_leader(leader: &Server) {
    leader.service.health_status.store(2, Ordering::SeqCst);
    // The TSO client's stream build probes health too, so wait for two more
    // probes: at least one is the member health loop's tick (every second)
    // after the status changed.
    let previous = leader.service.health_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        while leader.service.health_requests.load(Ordering::SeqCst) < previous + 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pd_forwarding_batch_metadata_policy_and_recovery() {
    for enabled in [false, true] {
        let (leader, follower, client) = availability_pair_with_forwarding(enabled).await;
        unavailable_leader(&leader).await;
        client.clone().get_all_stores().await.unwrap();
        let seen = follower.service.wire_routes.lock().unwrap().clone();
        let forwarded: Vec<_> = seen
            .iter()
            .filter(|(path, _, _)| path.ends_with("/GetAllStores"))
            .collect();
        assert_eq!(
            forwarded.len(),
            usize::from(enabled),
            "startup forwarding must reach metadata routing"
        );
        if enabled {
            assert_eq!(
                forwarded[0].1.as_deref(),
                Some(leader.service.endpoint.as_str())
            );
            assert!(!forwarded[0].2);
        }
        if enabled {
            follower.service.region_failure.store(2, Ordering::SeqCst);
            for _ in 0..2 {
                client
                    .clone()
                    .get_region_for_cache(b"wire".to_vec(), false, false)
                    .await
                    .unwrap();
            }
            let seen = follower.service.wire_routes.lock().unwrap().clone();
            let regions: Vec<_> = seen
                .iter()
                .filter(|(p, _, _)| p.ends_with("/GetRegion"))
                .collect();
            assert_eq!(regions.len(), 4);
            for pair in regions.chunks_exact(2) {
                assert!(
                    pair[0].2 && pair[0].1.is_none(),
                    "first region attempt preserves local permission even on API-ring fallback"
                );
                assert!(
                    !pair[1].2 && pair[1].1.as_deref() == Some(leader.service.endpoint.as_str()),
                    "error retry requires leader forwarding"
                );
            }
        }
        client.load_keyspace("DEFAULT").await.unwrap();
        assert_eq!(
            leader.service.keyspace_loads.load(Ordering::SeqCst),
            1,
            "Go keyspaceClient stays on the serving leader connection"
        );
        assert_eq!(follower.service.keyspace_loads.load(Ordering::SeqCst), 0);
        leader.service.health_status.store(1, Ordering::SeqCst);
        let previous = leader.service.health_requests.load(Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), async {
            while leader.service.health_requests.load(Ordering::SeqCst) == previous {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        })
        .await
        .unwrap();
        client.clone().get_all_stores().await.unwrap();
        let count = follower
            .service
            .wire_routes
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _, _)| p.ends_with("/GetAllStores"))
            .count();
        assert_eq!(
            count,
            usize::from(enabled),
            "healthy leader restores direct requests"
        );
        client.close().await;
    }
}

#[tokio::test]
async fn pd_forwarding_batch_leader_required_region_is_forwarded() {
    let (leader, follower, client) = availability_pair_with_forwarding(true).await;
    unavailable_leader(&leader).await;
    client
        .clone()
        .get_region_for_cache(b"wire".to_vec(), false, true)
        .await
        .unwrap();
    let seen = follower.service.wire_routes.lock().unwrap().clone();
    let forwarded = seen
        .iter()
        .find(|(path, _, _)| path.ends_with("/GetRegion"))
        .expect("leader-only region lookup must use forwarding");
    assert_eq!(
        forwarded.1.as_deref(),
        Some(leader.service.endpoint.as_str())
    );
    assert!(
        !forwarded.2,
        "forwarding cannot ask follower to handle locally"
    );
    client.set_enable_follower_handle(false);
    follower.service.stall_scans.store(true, Ordering::SeqCst);
    let request_client = client.clone();
    let request = tokio::spawn(async move {
        request_client
            .scan_regions(b"a".to_vec(), b"z".to_vec(), 1)
            .await
    });
    tokio::time::timeout(
        Duration::from_millis(500),
        follower.service.region_entered.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    tokio::time::timeout(Duration::from_millis(250), client.close())
        .await
        .unwrap();
    assert!(matches!(
        request.await.unwrap(),
        Err(crate::Error::ContextCanceled)
    ));
}

#[tokio::test]
async fn tso_proxy_batch_healthy_follower_receives_logical_leader_metadata() {
    let (leader, follower, client) = availability_pair_with_proxy(false, true).await;
    let stamp = client.clone().get_timestamp().await.unwrap();
    assert!(stamp.physical > 0);
    let routed = follower.service.wire_routes.lock().unwrap().clone();
    assert!(
        routed
            .iter()
            .any(|(path, forward, local)| path == "/pdpb.PD/Tso"
                && forward.as_deref() == Some(leader.service.endpoint.as_str())
                && !local),
        "healthy follower must proxy TSO even with ordinary forwarding disabled: {routed:?}"
    );
    assert_eq!(leader.service.received.load(Ordering::SeqCst), 0);
    let old = client.tso_for_test().await;
    client.reconnect_for_test().await.unwrap();
    let retained = client.tso_for_test().await;
    assert!(
        Arc::ptr_eq(&old, &retained),
        "unchanged healthy proxy is reused"
    );
    client.set_enable_tso_follower_proxy(false);
    client.reconnect_for_test().await.unwrap();
    assert!(
        !old.ctx().is_cancelled(),
        "proxy changes retain the single dispatcher"
    );
    // The client's connection-contexts updater reacts to the option change
    // (Go `EnableTSOFollowerProxyCh`) by connecting to the leader alone.
    wait_for_streams(&old, &[leader.service.endpoint.clone()]).await;
    client.clone().get_timestamp().await.unwrap();
    wait_for_proxy_drop(&follower).await;
    assert_eq!(leader.service.received.load(Ordering::SeqCst), 1);
    assert!(leader
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path == "/pdpb.PD/Tso")
        .all(|(_, host, _)| host.is_none()));
    client.set_enable_tso_follower_proxy(true);
    client.reconnect_for_test().await.unwrap();
    client.clone().get_timestamp().await.unwrap();
    let active = client.tso_for_test().await;
    client.close().await;
    assert!(active.ctx().is_cancelled());
    assert!(active.is_closed_for_test());
    assert!(active.connection_urls_for_test().is_empty());
}

#[tokio::test]
async fn tso_proxy_batch_microservice_group_and_health_retirement() {
    let pd = Server::start(Reply::Timestamp).await;
    let primary = Server::start(Reply::Timestamp).await;
    let secondary =
        Server::start_with_clock(Reply::Timestamp, primary.service.next_logical.clone()).await;
    let group = tsopb::KeyspaceGroup {
        id: 7,
        members: vec![
            tsopb::KeyspaceGroupMember {
                address: primary.service.endpoint.clone(),
                is_primary: true,
            },
            tsopb::KeyspaceGroupMember {
                address: secondary.service.endpoint.clone(),
                is_primary: false,
            },
        ],
        ..Default::default()
    };
    *primary.service.group.write().unwrap() = group.clone();
    *secondary.service.group.write().unwrap() = group;
    primary.service.health_status.store(2, Ordering::SeqCst);
    *pd.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![primary.service.endpoint.clone()],
        ..Default::default()
    });
    let options = crate::pd::opt::Options::new();
    options.set_enable_tso_follower_proxy(true);
    let client = Arc::new(
        RetryClient::connect_with_options(
            &[pd.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            options,
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(client.clone().get_timestamp().await.unwrap().physical, 200);
    assert_eq!(
        secondary.service.tso_headers.lock().unwrap()[0].keyspace_group_id,
        7
    );
    assert!(secondary
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .any(|(path, host, local)| path == "/tsopb.TSO/Tso"
            && host.as_ref() == Some(&primary.service.endpoint)
            && !local));
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
    assert_eq!(primary.service.received.load(Ordering::SeqCst), 0);
    let old = client.tso_for_test().await;
    // No stale stream survives a successful topology/health refresh with no healthy endpoints.
    secondary.service.health_status.store(2, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    assert!(
        !old.ctx().is_cancelled(),
        "proxy changes retain the single dispatcher"
    );
    wait_for_proxy_drop(&secondary).await;
    primary.service.health_status.store(1, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    client.clone().get_timestamp().await.unwrap();
    assert!(primary
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path == "/tsopb.TSO/Tso")
        .all(|(_, host, _)| host.is_none()));
    client.close().await;
}

async fn wait_for_proxy_drop(server: &Server) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bootstrap_policy_batch_offline_primary_keeps_healthy_proxy() {
    let pd = Server::start(Reply::Timestamp).await;
    let secondary = Server::start(Reply::Timestamp).await;
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary = format!("http://{}", unused.local_addr().unwrap());
    drop(unused);
    api_mode(&pd, &secondary);
    *secondary.service.group.write().unwrap() = tsopb::KeyspaceGroup {
        id: 7,
        members: vec![
            tsopb::KeyspaceGroupMember {
                address: primary.clone(),
                is_primary: true,
            },
            tsopb::KeyspaceGroupMember {
                address: secondary.service.endpoint.clone(),
                is_primary: false,
            },
        ],
        ..Default::default()
    };
    let options = crate::pd::opt::Options::new();
    options.set_enable_tso_follower_proxy(true);
    let mut connection = Connection::new(Arc::new(SecurityManager::default()));
    connection.options = Arc::new(options);
    let mut cluster = connection
        .connect_cluster(&[pd.service.endpoint.clone()], Duration::from_millis(500))
        .await
        .expect("TSO primary reachability must not gate group publication");
    assert_eq!(cluster.get_timestamp().await.unwrap().physical, 200);
    assert!(secondary
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .any(|(path, host, _)| path == "/tsopb.TSO/Tso" && host.as_ref() == Some(&primary)));
    cluster.start_close().await;
}

#[tokio::test]
async fn bootstrap_policy_batch_membership_does_not_require_leader_probe() {
    let leader = Server::start(Reply::Timestamp).await;
    let follower = Server::start(Reply::Timestamp).await;
    *follower.service.leader_urls.write().unwrap() = vec![leader.service.endpoint.clone()];
    leader.service.member_failures.store(100, Ordering::SeqCst);
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    let mut cluster = connection
        .connect_cluster(
            &[follower.service.endpoint.clone()],
            Duration::from_millis(500),
        )
        .await
        .expect("accepted follower membership must initialize the serving connection");
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_millis(500))
        .await
        .unwrap();
    assert_eq!(leader.service.member_requests.load(Ordering::SeqCst), 0);
    cluster.start_close().await;
}

#[tokio::test]
async fn bootstrap_policy_batch_forced_pd_provider_survives_api_refresh() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    let mut options = crate::pd::opt::Options::new();
    options.use_tso_server_proxy = true;
    let mut connection = Connection::new(Arc::new(SecurityManager::default()));
    connection.options = Arc::new(options);
    let mut cluster = connection
        .connect_cluster(&[pd.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    cluster
        .get_min_timestamp(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        pd.service.received.load(Ordering::SeqCst),
        2,
        "forced provider must serve ordinary and minimum timestamps through PD"
    );
    assert_eq!(pd.service.min_requests.load(Ordering::SeqCst), 0);
    assert_eq!(tso.service.discovery_requests.load(Ordering::SeqCst), 0);
    assert!(tso.service.tso_headers.lock().unwrap().is_empty());
    cluster.start_close().await;
}

#[tokio::test]
async fn bootstrap_policy_batch_dial_options_apply_once_in_order() {
    let pd = Server::start(Reply::Timestamp).await;
    *pd.service.leader_urls.write().unwrap() = vec![
        "https://127.0.0.1:0".to_owned(),
        pd.service.endpoint.clone(),
    ];
    let applied = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut options = crate::pd::opt::Options::new();
    for index in [1, 2] {
        let applied = applied.clone();
        options.grpc_dial_options.push(Arc::new(move |endpoint| {
            applied.lock().unwrap().push(index);
            endpoint.user_agent(format!("policy-{index}")).unwrap()
        }));
    }
    let mut connection = Connection::new(Arc::new(SecurityManager::default()));
    connection.options = Arc::new(options);
    let mut cluster = connection
        .connect_cluster(&[pd.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(*applied.lock().unwrap(), vec![1, 2]);
    assert!(pd
        .service
        .user_agents
        .lock()
        .unwrap()
        .iter()
        .all(|agent| agent.starts_with("policy-2")));
    assert_eq!(pd.service.connections.load(Ordering::SeqCst), 1);
    cluster.start_close().await;
}

/// pd-client `tryConnectToTSO`: after `MaxRetryTimes` failures to build the
/// leader's stream for network reasons, a healthy follower forwards to the
/// leader (`[tso] fall back to use follower to forward tso stream`), and
/// `checkLeader` restores the leader's own stream once it is healthy again.
#[tokio::test]
async fn tso_failure_batch_forwards_after_network_failures_and_recovers() {
    crate::logutil::test_capture::install();
    let leader = Server::start(Reply::Timestamp).await;
    let follower =
        Server::start_with_clock(Reply::Timestamp, leader.service.next_logical.clone()).await;
    let discovery = PairDiscovery::new(&leader, &follower, dead_channel());
    let cli = pair_cli(discovery.clone(), true, Duration::from_secs(1)).await;
    assert!(
        get_timestamp_eventually(&cli, Duration::from_secs(10)).await,
        "six network failures must admit a healthy backup: {:?}",
        crate::logutil::test_capture::lines()
            .into_iter()
            .filter(|line| line.contains("tso"))
            .collect::<Vec<_>>()
    );
    let routes = follower.service.wire_routes.lock().unwrap().clone();
    assert!(routes.iter().any(|(path, host, _)| path == "/pdpb.PD/Tso"
        && host.as_deref() == Some(leader.service.endpoint.as_str())));
    let fallback = format!(
        "INFO [tso] fall back to use follower to forward tso stream follower-url={}",
        follower.service.endpoint
    );
    assert!(crate::logutil::test_capture::any(|line| line == fallback));
    assert!(cli.get_timestamp().await.is_ok());

    // Let the connection attempts already scheduled (the updater's pending
    // wake-up retries the leader `MaxRetryTimes` times) run out first, so
    // only checkLeader can bring the leader back.
    tokio::time::sleep(Duration::from_secs(4)).await;
    wait_for_streams(&cli, &[follower.service.endpoint.clone()]).await;
    // The leader becomes reachable again: checkLeader switches back.
    discovery.set_leader_channel(
        Channel::from_shared(leader.service.endpoint.clone())
            .unwrap()
            .connect_lazy(),
    );
    wait_for_streams(&cli, &[leader.service.endpoint.clone()]).await;
    let recovered = format!(
        "INFO [tso] recover the original tso stream since the network has become normal url={}",
        leader.service.endpoint
    );
    assert!(
        crate::logutil::test_capture::any(|line| line == recovered),
        "{:?}",
        crate::logutil::test_capture::lines()
            .into_iter()
            .filter(|line| line.contains("tso"))
            .collect::<Vec<_>>()
    );
    assert!(cli.get_timestamp().await.is_ok());
    assert_eq!(leader.service.received.load(Ordering::SeqCst), 1);
    cli.close().await;
    assert!(cli.connection_urls_for_test().is_empty());
}

/// Only a stream that cannot be built for network reasons (Go
/// `errs.IsNetworkError(code) || code == codes.Canceled`, where Canceled is the
/// build's own timeout) counts towards forwarding, and only with
/// `EnableForwarding`.
#[tokio::test]
async fn tso_failure_batch_error_codes_and_disabled_policy() {
    crate::logutil::test_capture::install();
    // A leader that accepts TCP but never speaks HTTP/2: the build times out.
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_url = format!("http://{}", silent.local_addr().unwrap());
    let silent_task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = silent.accept().await {
            held.push(socket);
        }
    });
    for (enabled, leader_channel_name, leader_channel, forwards) in [
        (true, "dead", dead_channel(), true),
        (
            true,
            "silent",
            Channel::from_shared(silent_url.clone())
                .unwrap()
                .connect_lazy(),
            true,
        ),
        (false, "dead", dead_channel(), false),
    ] {
        let leader = Server::start(Reply::Timestamp).await;
        let follower =
            Server::start_with_clock(Reply::Timestamp, leader.service.next_logical.clone()).await;
        let discovery = PairDiscovery::new(&leader, &follower, leader_channel);
        let cli = pair_cli(discovery, enabled, Duration::from_millis(200)).await;
        assert_eq!(
            get_timestamp_eventually(&cli, Duration::from_secs(8)).await,
            forwards,
            "{enabled} {:?} {:?}",
            leader_channel_name,
            crate::logutil::test_capture::lines()
                .into_iter()
                .filter(|line| line.contains("tso"))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            follower.service.received.load(Ordering::SeqCst) > 0,
            forwards
        );
        assert_eq!(leader.service.received.load(Ordering::SeqCst), 0);
        cli.close().await;
    }
    silent_task.abort();
}

#[tokio::test]
async fn tso_failure_batch_retains_provider_when_mode_observation_fails() {
    let (leader, _follower, client) = availability_pair_with_forwarding(true).await;
    leader
        .service
        .cluster_info_failure
        .store(true, Ordering::SeqCst);
    let original = client.tso_for_test().await;
    for _ in 0..3 {
        client
            .reconnect_for_test()
            .await
            .expect("failed mode observation must preserve accepted provider");
        assert!(Arc::ptr_eq(&original, &client.tso_for_test().await));
        assert!(client.tso_for_test().await.get_timestamp().await.is_ok());
    }
    leader.service.health_stall.store(true, Ordering::SeqCst);
    let checks = leader.service.health_requests.load(Ordering::SeqCst);
    let probe = tokio::spawn({
        let client = client.clone();
        async move { client.reconnect_for_test().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while leader.service.health_requests.load(Ordering::SeqCst) == checks {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), client.close())
        .await
        .unwrap();
    let _ = probe.await;
}

#[tokio::test]
async fn tso_failure_batch_established_eof_does_not_enable_forwarding() {
    let (leader, follower, client) = availability_pair_with_reply(true, false, Reply::End).await;
    leader.service.health_status.store(2, Ordering::SeqCst);
    for _ in 0..8 {
        assert!(client.tso_for_test().await.get_timestamp().await.is_err());
        client.reconnect_for_test().await.unwrap();
    }
    assert_eq!(follower.service.received.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn collection_batch_live_wait_option_reaches_existing_oracle() {
    let server = Server::start(Reply::Timestamp).await;
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    let cluster = connection
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(3))
        .await
        .unwrap();
    let oracle = cluster.tso_for_test();
    // Establish transport before measuring the collection policy itself.
    oracle.clone().get_timestamp().await.unwrap();
    connection
        .options
        .set_max_tso_batch_wait_interval(Duration::from_millis(10))
        .unwrap();
    // Go samples the option before waiting for a first request. Complete
    // the collector already waiting under the old value before measuring.
    oracle.clone().get_timestamp().await.unwrap();
    let started = std::time::Instant::now();
    oracle.clone().get_timestamp().await.unwrap();
    let waited = started.elapsed();
    oracle.close().await;
    assert!(
        waited >= Duration::from_millis(9),
        "live collector ignored configured wait: {waited:?}"
    );
}

#[tokio::test]
async fn observation_batch_failed_modes_preserve_accepted_provider() {
    for microservice in [false, true] {
        let pd = Server::start(Reply::Timestamp).await;
        let tso = Server::start(Reply::Timestamp).await;
        if microservice {
            api_mode(&pd, &tso);
        }
        let client = metadata_client(&pd).await;
        let original = client.tso_for_test().await;
        for observation in [
            pdpb::GetClusterInfoResponse::default(),
            pdpb::GetClusterInfoResponse {
                header: Some(pdpb::ResponseHeader {
                    cluster_id: 42,
                    error: Some(pdpb::Error {
                        message: "observation rejected".into(),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            },
        ] {
            *pd.service.cluster_info.write().unwrap() = Some(observation);
            let result = client.reconnect_for_test().await;
            assert!(
                result.is_ok(),
                "failed observation revoked accepted provider: {result:?}"
            );
            assert!(Arc::ptr_eq(&original, &client.tso_for_test().await));
            assert_eq!(
                client.clone().get_timestamp().await.unwrap().physical,
                if microservice { 200 } else { 100 }
            );
        }
        client.close().await;
    }
}

#[tokio::test]
async fn independent_observation_mode_failure_does_not_freeze_group() {
    let pd = Server::start(Reply::Timestamp).await;
    let first = Server::start(Reply::Timestamp).await;
    let second =
        Server::start_with_clock(Reply::Timestamp, first.service.next_logical.clone()).await;
    api_mode(&pd, &first);
    let client = metadata_client(&pd).await;
    let old = client.tso_for_test().await;
    *first.service.group.write().unwrap() = tsopb::KeyspaceGroup {
        id: 9,
        members: vec![tsopb::KeyspaceGroupMember {
            address: second.service.endpoint.clone(),
            is_primary: true,
        }],
        ..Default::default()
    };
    first.service.revision.store(20, Ordering::SeqCst);
    pd.service
        .cluster_info_failure
        .store(true, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    // The group's new primary is the TSO client's leader switch.
    let switched = old.leader_url() == second.service.endpoint;
    if switched {
        wait_for_streams(&old, &[second.service.endpoint.clone()]).await;
    }
    client.clone().get_timestamp().await.unwrap();
    let headers = second.service.tso_headers.lock().unwrap().clone();
    client.close().await;
    assert!(
        switched,
        "failed PD mode observation froze the accepted TSO group"
    );
    assert_eq!(headers[0].keyspace_group_id, 9);
}

#[tokio::test]
async fn independent_observation_stalled_members_do_not_block_mode_switch() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    let client = Arc::new(
        RetryClient::connect(
            &[pd.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            Duration::from_secs(10),
        )
        .await
        .unwrap(),
    );
    let old = client.tso_for_test().await;
    pd.service.stall_members.store(true, Ordering::SeqCst);
    let requests = pd.service.member_requests.load(Ordering::SeqCst);
    let refreshing = client.clone();
    let refresh = tokio::spawn(async move { refreshing.reconnect_for_test().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while pd.service.member_requests.load(Ordering::SeqCst) <= requests {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    api_mode(&pd, &tso);
    let switched = tokio::time::timeout(Duration::from_secs(6), async {
        while !old.ctx().is_cancelled() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    client.close().await;
    refresh.abort();
    let _ = refresh.await;
    assert!(
        switched.is_ok(),
        "GetMembers prevented background TSO discovery"
    );
}

#[tokio::test]
async fn observation_batch_secondary_must_reach_newly_observed_revision() {
    let pd = Server::start(Reply::Timestamp).await;
    let discovery = Server::start(Reply::Timestamp).await;
    let secondary = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &discovery);
    let client = metadata_client(&pd).await;
    let original = client.tso_for_test().await;
    discovery.service.revision.store(20, Ordering::SeqCst);
    secondary.service.revision.store(19, Ordering::SeqCst);
    *discovery.service.group.write().unwrap() = tsopb::KeyspaceGroup {
        id: 7,
        members: vec![tsopb::KeyspaceGroupMember {
            address: secondary.service.endpoint.clone(),
            is_primary: false,
        }],
        ..Default::default()
    };
    assert!(
        client.reconnect_for_test().await.is_err(),
        "secondary regressed revision 20 to 19"
    );
    assert_eq!(
        *secondary.service.requested_revisions.lock().unwrap(),
        vec![20]
    );
    assert!(Arc::ptr_eq(&original, &client.tso_for_test().await));
    // A failed follow-up must not roll back the observed floor in a clone.
    let primaryless = discovery.service.group.read().unwrap().clone();
    discovery.service.revision.store(19, Ordering::SeqCst);
    *discovery.service.group.write().unwrap() = secondary.service.group.read().unwrap().clone();
    assert!(
        client.reconnect_for_test().await.is_err(),
        "failed refresh forgot the newer revision"
    );
    discovery.service.revision.store(20, Ordering::SeqCst);
    *discovery.service.group.write().unwrap() = primaryless;
    secondary.service.revision.store(20, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    assert_eq!(client.clone().get_timestamp().await.unwrap().physical, 200);
    client.close().await;
}

#[tokio::test]
async fn observation_batch_callee_mismatch_retires_only_stale_connection() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    let client = metadata_client(&pd).await;
    let initial = tso.service.connections.load(Ordering::SeqCst);
    let pd_connections = pd.service.connections.load(Ordering::SeqCst);
    for (message, should_reconnect) in [
        ("unrelated header error", false),
        ("wrapped mismatch callee id: stale DNS", true),
    ] {
        *tso.service.discovery_error.lock().unwrap() = Some(message.into());
        assert!(client.reconnect_for_test().await.is_err());
        *tso.service.discovery_error.lock().unwrap() = None;
        client.reconnect_for_test().await.unwrap();
        assert_eq!(
            tso.service.connections.load(Ordering::SeqCst) > initial,
            should_reconnect,
            "{message}"
        );
        assert_eq!(
            pd.service.connections.load(Ordering::SeqCst),
            pd_connections
        );
    }
    client.close().await;
}

#[tokio::test]
async fn completion_batch_rejects_regression_and_invalid_timestamp_ranges() {
    for (physical, logical) in [
        (100, 0),
        (-1, 2),
        (100, -1),
        (100, 1 << 18),
        (1 << 46, 2),
        (0, 0),
    ] {
        let server = Server::start(Reply::Timestamp).await;
        let cluster = server.cluster(Duration::from_secs(2)).await;
        let oracle = cluster.tso_for_test();
        assert_eq!(oracle.clone().get_timestamp().await.unwrap().logical, 1);
        server
            .service
            .scripted_tso
            .lock()
            .unwrap()
            .push_back(TsoResponse {
                count: 1,
                timestamp: Some(Timestamp {
                    physical,
                    logical,
                    suffix_bits: 0,
                }),
                ..Default::default()
            });
        assert!(
            oracle.clone().get_timestamp().await.is_err(),
            "accepted ({physical}, {logical})"
        );
        oracle.close().await;
    }
}

/// pd-client's dispatcher keeps `latestTSOInfo` and rejects a batch that is
/// not after the ts allocated before its request began (Go `log.Panic`s
/// `[tso] timestamp fallback`; here the batch fails with that line). The
/// record belongs to the dispatcher, so a replaced TSO client starts afresh,
/// as Go's does.
#[tokio::test]
async fn completion_batch_rejects_a_fallen_back_batch() {
    crate::logutil::test_capture::install();
    let server = Server::start(Reply::Timestamp).await;
    let cli = test_cli(
        &server,
        Duration::from_secs(5),
        crate::pd::opt::Options::new(),
    )
    .await;
    let first = cli.get_timestamp().await.unwrap();
    server
        .service
        .scripted_tso
        .lock()
        .unwrap()
        .push_back(TsoResponse {
            count: 1,
            timestamp: Some(first.clone()),
            ..Default::default()
        });
    assert!(cli.get_timestamp().await.is_err());
    let expected = format!(
        "last-ts=({}, {}) cur-ts=({}, {})",
        first.physical, first.logical, first.physical, first.logical
    );
    assert!(
        crate::logutil::test_capture::any(|line| line
            .starts_with("ERROR [tso] timestamp fallback keyspace=0 ")
            && line.contains(&expected)),
        "{:?}",
        crate::logutil::test_capture::lines()
    );
    cli.close().await;
}

#[tokio::test]
async fn mode_scheduling_stalled_mode_does_not_freeze_group() {
    let pd = Server::start(Reply::Timestamp).await;
    let first = Server::start(Reply::Timestamp).await;
    let second =
        Server::start_with_clock(Reply::Timestamp, first.service.next_logical.clone()).await;
    api_mode(&pd, &first);
    let client = RetryClient::connect(
        &[pd.service.endpoint.clone()],
        Arc::new(SecurityManager::default()),
        Duration::from_secs(15),
    )
    .await
    .unwrap();
    let old = client.tso_for_test().await;
    pd.service.stall_mode.store(true, Ordering::SeqCst);
    *first.service.group.write().unwrap() = tsopb::KeyspaceGroup {
        id: 9,
        members: vec![tsopb::KeyspaceGroupMember {
            address: second.service.endpoint.clone(),
            is_primary: true,
        }],
        ..Default::default()
    };
    first.service.revision.store(20, Ordering::SeqCst);
    let switched = tokio::time::timeout(Duration::from_secs(7), async {
        while old.leader_url() != second.service.endpoint {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let mode_polled = pd.service.mode_requests.load(Ordering::SeqCst) > 1;
    client.close().await;
    assert!(
        switched.is_ok(),
        "stalled mode RPC froze accepted group refresh"
    );
    assert!(mode_polled, "independent mode polling stopped");
}

#[tokio::test]
async fn mode_scheduling_provider_change_cancels_stalled_group() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    let client = RetryClient::connect(
        &[pd.service.endpoint.clone()],
        Arc::new(SecurityManager::default()),
        Duration::from_secs(15),
    )
    .await
    .unwrap();
    let old = client.tso_for_test().await;
    tso.service.stall_discovery.store(true, Ordering::SeqCst);
    let requests = tso.service.discovery_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), async {
        while tso.service.discovery_requests.load(Ordering::SeqCst) == requests {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    *pd.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::PdSvcMode as i32],
        ..Default::default()
    });
    let switched = tokio::time::timeout(Duration::from_secs(7), async {
        while !old.ctx().is_cancelled() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    client.close().await;
    assert!(
        switched.is_ok(),
        "stalled group RPC prevented provider replacement"
    );
}

#[tokio::test]
async fn mode_scheduling_failed_mode_wakes_member_refresh() {
    let pd = Server::start(Reply::Timestamp).await;
    let client = RetryClient::connect(
        &[pd.service.endpoint.clone()],
        Arc::new(SecurityManager::default()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let requests = pd.service.member_requests.load(Ordering::SeqCst);
    pd.service
        .cluster_info_failure
        .store(true, Ordering::SeqCst);
    let refreshed = tokio::time::timeout(Duration::from_secs(7), async {
        while pd.service.member_requests.load(Ordering::SeqCst) == requests {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    client.close().await;
    assert!(
        refreshed.is_ok(),
        "mode error waited for the one-minute member timer"
    );
}

// ---------------------------------------------------------------------------
// Go `pd/client/clients/tso/dispatcher.go::checkTSORPCConcurrency`.
// ---------------------------------------------------------------------------

/// Go sizes the token pool at the concurrency itself when that is 1, and at
/// twice the concurrency above 1, so the count of ongoing requests may
/// fluctuate around the target while an RPC's duration jitters. The first
/// dispatcher circle adopts the option from Go's zero state, logging
/// `switching tso rpc concurrency old=0 new=1`.
#[tokio::test]
async fn rpc_concurrency_adopts_the_option_and_sizes_gos_token_pool() {
    crate::logutil::test_capture::install();
    let (dispatcher, options, cancellation) = concurrency_dispatcher();
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        0,
        "Go starts without tokens"
    );
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, Instant::now())
        .await
        .unwrap();
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        1,
        "Go's default concurrency"
    );
    assert!(crate::logutil::test_capture::any(
        |line| line == "INFO [tso] switching tso rpc concurrency old=0 new=1"
    ));

    options.set_tso_client_rpc_concurrency(2);
    let later = Instant::now() + DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL;
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, later)
        .await
        .unwrap();
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        4,
        "two concurrency, 2x tokens"
    );

    // A later change is rate-limited: Go checks at most once per interval.
    options.set_tso_client_rpc_concurrency(4);
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, later)
        .await
        .unwrap();
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        4,
        "the interval has not elapsed, so Go does not re-check"
    );
    let later = later + DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL;
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, later)
        .await
        .unwrap();
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        8,
        "four concurrency, 2x tokens"
    );
}

/// Concurrent RPCs and an additional collection wait pursue opposite goals, so
/// Go forces concurrency to 1 while a wait is configured -- and does it
/// IMMEDIATELY, bypassing the check interval, when the wait has just been
/// enabled over a concurrency above 1.
#[tokio::test]
async fn a_collection_wait_disables_concurrent_rpcs_immediately() {
    let (dispatcher, options, cancellation) = concurrency_dispatcher();
    options.set_tso_client_rpc_concurrency(4);
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, Instant::now())
        .await
        .unwrap();
    assert_eq!(dispatcher.available_tokens_for_test(), 8);
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::from_millis(1), Instant::now())
        .await
        .unwrap();
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        1,
        "a positive collection wait forces Go's single ongoing request"
    );
}

/// Go applies the same override for follower proxy, which multiplies the
/// streams a single batch already fans out to.
#[tokio::test]
async fn follower_proxy_disables_concurrent_rpcs() {
    let (dispatcher, options, cancellation) = concurrency_dispatcher();
    options.set_tso_client_rpc_concurrency(2);
    options.set_enable_tso_follower_proxy(true);
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, Instant::now())
        .await
        .unwrap();
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        1,
        "follower proxy keeps one ongoing request"
    );
}

/// `Options` stores a concurrency without validating it, and a permit count is
/// unsigned, so the conversion answers for a nonpositive setting with Go's own
/// default of one ongoing request rather than draining the pool forever.
#[tokio::test]
async fn a_nonpositive_concurrency_keeps_one_ongoing_request() {
    let (dispatcher, options, cancellation) = concurrency_dispatcher();
    options.set_tso_client_rpc_concurrency(0);
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, Instant::now())
        .await
        .unwrap();
    assert_eq!(dispatcher.available_tokens_for_test(), 1);
    options.set_tso_client_rpc_concurrency(-4);
    let later = Instant::now() + DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL;
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, later)
        .await
        .unwrap();
    assert_eq!(dispatcher.available_tokens_for_test(), 1);
}

/// Cancellation abandons a reduction instead of waiting for tokens that an
/// in-flight request will never return, which is Go's `select` on the
/// dispatcher context.
#[tokio::test]
async fn cancellation_abandons_a_pending_token_reduction() {
    let (dispatcher, options, cancellation) = concurrency_dispatcher();
    options.set_tso_client_rpc_concurrency(4);
    dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, Instant::now())
        .await
        .unwrap();
    assert_eq!(dispatcher.available_tokens_for_test(), 8);
    // Hold every token, so a reduction cannot make progress.
    let held = dispatcher.hold_tokens_for_test(8).await;
    options.set_tso_client_rpc_concurrency(1);
    cancellation.cancel();
    let later = Instant::now() + DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL;
    assert!(dispatcher
        .check_tso_rpc_concurrency(&cancellation, Duration::ZERO, later)
        .await
        .is_err());
    drop(held);
    assert_eq!(
        dispatcher.available_tokens_for_test(),
        8,
        "the abandoned reduction left the pool as it stood"
    );
}

/// A granted concurrency above 1 must put a second batch on the wire while the
/// first is unanswered, which is what Go's separate send and receive loops do.
/// The server reads every request and answers none, so `received` counts the
/// batches the client was willing to leave outstanding.
#[tokio::test]
async fn concurrent_rpcs_put_a_second_batch_on_the_wire_before_the_first_answers() {
    let server = Server::start(Reply::DrainRequests).await;
    let options = crate::pd::opt::Options::new();
    options.set_tso_client_rpc_concurrency(2);
    let cli = test_cli(&server, Duration::from_secs(30), options).await;
    let first = tokio::spawn({
        let cli = cli.clone();
        async move { cli.get_timestamp().await }
    });
    let reached = |want: usize| {
        let received = server.service.received.clone();
        async move {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                if received.load(Ordering::SeqCst) >= want {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            received.load(Ordering::SeqCst) >= want
        }
    };
    // The second timestamp is asked for after the first is on the wire;
    // asking both up front would let the collector merge them into one batch.
    assert!(reached(1).await, "the first batch must reach the server");
    let second = tokio::spawn({
        let cli = cli.clone();
        async move { cli.get_timestamp().await }
    });
    assert!(
        reached(2).await,
        "a granted concurrency of 2 must send a second batch while the first is \
         unanswered; the server saw {} request(s)",
        server.service.received.load(Ordering::SeqCst)
    );
    first.abort();
    second.abort();
    cli.close().await;
}

/// pd-client's TSO client lifecycle lines, with their zap fields.
#[tokio::test]
async fn tso_client_lifecycle_logs_pd_clients_lines() {
    crate::logutil::test_capture::install();
    let server = Server::start(Reply::Timestamp).await;
    let cli = test_cli(
        &server,
        Duration::from_secs(5),
        crate::pd::opt::Options::new(),
    )
    .await;
    cli.get_timestamp().await.unwrap();
    // A stream that ends is logged by its receive loop, and the batch error
    // by the dispatcher.
    cli.release_all_for_test();
    cli.close().await;
    let endpoint = &server.service.endpoint;
    for expected in [
        format!("INFO [tso] switch the tso leader serving url new-url={endpoint}"),
        "INFO [tso] tso dispatcher created".to_owned(),
        "INFO [tso] start tso connection contexts updater".to_owned(),
        "INFO [tso] closing tso client".to_owned(),
        "INFO [tso] exit tso connection contexts updater".to_owned(),
        "INFO [tso] exit tso dispatcher".to_owned(),
        "INFO [tso] close tso client".to_owned(),
        "INFO [tso] tso client is closed".to_owned(),
    ] {
        assert!(
            crate::logutil::test_capture::any(|line| line == expected),
            "{expected:?} not in {:?}",
            crate::logutil::test_capture::lines()
        );
    }
    let stream_end = format!("INFO tsoStream.recvLoop ended stream={endpoint}-");
    assert!(crate::logutil::test_capture::any(|line| line
        .starts_with(&stream_end)
        && line.ends_with(" error=context canceled")));
}

/// Go `rcFilter`: the first sample passes through; later ones move by at most
/// the configured weight.
#[test]
fn rc_filter_follows_gos_low_pass() {
    let start = Instant::now();
    let mut filter = super::stream::RcFilter::new(1.0, 0.2);
    assert_eq!(filter.update(start, 10.0), 10.0);
    // dt = 1s is far above rc = 1/(2*pi): alpha is capped at 0.2.
    let value = filter.update(start + Duration::from_secs(1), 20.0);
    assert!((value - 12.0).abs() < 1e-9, "{value}");
}

/// Go `checkMonotonicity`: a batch must start after the latest ts allocated
/// before its RPC began; overlapping RPCs may finish out of order, and
/// `latestTSOInfo` keeps the largest; a keyspace-group change is logged.
#[tokio::test]
async fn check_monotonicity_keeps_the_maximum_across_overlaps() {
    use super::dispatcher::TsoInfo;
    crate::logutil::test_capture::install();
    let (dispatcher, _, _) = concurrency_dispatcher();
    // A batch of 2 ending at logical 2, then two overlapping batches that
    // both began after it.
    dispatcher
        .check_monotonicity(None, TsoInfo::for_test(100, 2, 0), 1)
        .unwrap();
    let before = dispatcher.latest_tso_info_for_test();
    dispatcher
        .check_monotonicity(before.clone(), TsoInfo::for_test(100, 8, 0), 7)
        .unwrap();
    dispatcher
        .check_monotonicity(before, TsoInfo::for_test(100, 4, 0), 3)
        .unwrap();
    assert_eq!(dispatcher.latest_tso_info_for_test().unwrap().logical, 8);
    // A batch starting at 8 is not after 8.
    assert!(dispatcher
        .check_monotonicity(
            dispatcher.latest_tso_info_for_test(),
            TsoInfo::for_test(100, 9, 0),
            8
        )
        .is_err());
    dispatcher
        .check_monotonicity(
            dispatcher.latest_tso_info_for_test(),
            TsoInfo::for_test(100, 10, 7),
            9,
        )
        .unwrap();
    assert!(crate::logutil::test_capture::any(|line| {
        line == "INFO [tso] keyspace group changed old-group-id=0 new-group-id=7"
    }));
}

/// pd-client's client-level discovery lines: the service mode it adopts
/// (`[pd] service mode changed`), the router service discovery created at
/// init, and the resource manager discovery created for microservice mode.
#[tokio::test]
async fn client_discovery_lines_follow_the_service_mode() {
    crate::logutil::test_capture::install();
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    let client = RetryClient::connect(
        &[pd.service.endpoint.clone()],
        Arc::new(SecurityManager::default()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let pd_mode = "INFO [pd] service mode changed new-mode=PD_SVC_MODE";
    crate::logutil::test_capture::wait_for(pd_mode, |line| line == pd_mode).await;
    assert!(crate::logutil::test_capture::any(|line| line.starts_with(
        "INFO [router service] created router service discovery cluster-id=42 "
    )));
    api_mode(&pd, &tso);
    let api_mode_line = "INFO [pd] service mode changed new-mode=API_SVC_MODE";
    tokio::time::timeout(Duration::from_secs(6), async {
        while !crate::logutil::test_capture::any(|line| line == api_mode_line) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("API mode is adopted");
    assert!(crate::logutil::test_capture::any(|line| {
        line
        == "INFO [resource-manager] created resource manager discovery cluster-id=42 discovery-key=/ms/42/resource_manager/primary"
    }));
    client.close().await;
    assert!(crate::logutil::test_capture::any(
        |line| line == "INFO [router service] is closed"
    ));
}
