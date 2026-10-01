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

use super::*;
use crate::pd::Connection;
use crate::SecurityManager;

#[derive(Clone, Copy)]
enum Reply {
    Timestamp,
    StallBody,
    StallHeaders,
}

#[derive(Clone)]
struct PdServer {
    endpoint: String,
    reply: Reply,
    received: Arc<AtomicUsize>,
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
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let service = self.clone();
        match request.uri().path() {
            "/pdpb.PD/GetMembers" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(service, request)
                        .await,
                )
            }),
            "/pdpb.PD/Tso" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .streaming(service, request)
                        .await,
                )
            }),
            _ => Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::empty_body())
                    .unwrap())
            }),
        }
    }
}

impl tonic::server::UnaryService<GetMembersRequest> for PdServer {
    type Response = GetMembersResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, _: tonic::Request<GetMembersRequest>) -> Self::Future {
        let member = Member {
            member_id: 1,
            client_urls: vec![self.endpoint.clone()],
            ..Default::default()
        };
        Box::pin(async move {
            Ok(tonic::Response::new(GetMembersResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                members: vec![member.clone()],
                leader: Some(member),
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
            let mut requests = request.into_inner();
            // A server is allowed to wait for the first request before returning headers.
            let first = requests.message().await?;
            if first.is_some() {
                service.received.fetch_add(1, Ordering::SeqCst);
            }
            if matches!(service.reply, Reply::StallHeaders) {
                futures::future::pending::<()>().await;
            }
            let responses = futures::stream::unfold(
                (requests, first, active, 0_i64, service),
                |(mut requests, first, active, mut logical, service)| async move {
                    if matches!(service.reply, Reply::StallBody) {
                        futures::future::pending::<()>().await;
                    }
                    let request = match first {
                        Some(first) => first,
                        None => match requests.message().await {
                            Ok(Some(request)) => {
                                service.received.fetch_add(1, Ordering::SeqCst);
                                request
                            }
                            Ok(None) => return None,
                            Err(error) => {
                                return Some((
                                    Err(error),
                                    (requests, None, active, logical, service),
                                ))
                            }
                        },
                    };
                    logical += i64::from(request.count);
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
                    Some((Ok(response), (requests, None, active, logical, service)))
                },
            );
            Ok(tonic::Response::new(
                Box::pin(responses) as Self::ResponseStream
            ))
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let service = PdServer {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            reply,
            received: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
        };
        let task_service = service.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(task_service)
                .serve_with_incoming(TcpListenerStream::new(listener))
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
    assert!(server.service.received.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn source_tso_close_joins_worker_and_deadline_watcher() {
    let server = Server::start(Reply::StallBody).await;
    let channel = tonic::transport::Channel::from_shared(server.service.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = PdClient::new(channel);
    let oracle = TimestampOracle::new(42, &client, Duration::from_secs(100)).unwrap();
    let pending = tokio::spawn(oracle.clone().get_timestamp());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(oracle.close(), oracle.close());
    })
    .await
    .unwrap();
    assert!(pending.await.unwrap().is_err());
    assert!(oracle.inner.worker.lock().await.is_none());
    assert!(oracle.get_timestamp().await.is_err());
}

#[tokio::test]
async fn source_tso_completed_result_survives_stream_retirement() {
    // Go Request waits for request/client context, not a retired stream's
    // context. An already completed batch must retain its timestamp.
    for _ in 0..64 {
        let (request_tx, mut request_rx) = mpsc::channel(1);
        let cancellation = Cancellation::default();
        let oracle = TimestampOracle {
            inner: Arc::new(OracleInner {
                request_tx,
                cancellation: cancellation.clone(),
                worker: Mutex::new(None),
            }),
        };
        let request = oracle.get_timestamp();
        tokio::pin!(request);
        assert!(futures::poll!(request.as_mut()).is_pending());
        let timestamp = Timestamp {
            physical: 123,
            logical: 456,
            suffix_bits: 0,
        };
        request_rx
            .try_recv()
            .unwrap()
            .send(timestamp.clone())
            .unwrap();
        cancellation.cancel();
        assert_eq!(request.await.unwrap(), timestamp);
    }
}
