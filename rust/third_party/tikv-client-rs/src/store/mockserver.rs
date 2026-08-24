//! Reusable behavior core for client-go `internal/client/mockserver`.
//!
//! The generated Tonic service facade is intentionally kept separate: this
//! state machine supplies the source's BatchCommands hook and default health
//! feedback to every transport mount.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::transport::Server;
use tonic_prost::ProstCodec;

use crate::proto::{coprocessor, kvrpcpb, tikvpb};

pub(crate) type BatchCommandsHandler = Arc<
    dyn Fn(
            tikvpb::BatchCommandsRequest,
        ) -> std::result::Result<tikvpb::BatchCommandsResponse, tonic::Status>
        + Send
        + Sync,
>;

pub(crate) type MetadataChecker = Arc<
    dyn Fn(&tonic::metadata::MetadataMap) -> std::result::Result<(), tonic::Status> + Send + Sync,
>;

/// Source MockServer's BatchCommands behavior, independently reusable by the
/// Tonic test transport. Each default response owns an increasing feedback
/// sequence, while an installed handler replaces the complete response.
pub(crate) struct MockServerCore {
    batch_handler: RwLock<Option<BatchCommandsHandler>>,
    metadata_checker: RwLock<Option<MetadataChecker>>,
    feedback_sequence: AtomicU64,
}

/// Transport-backed counterpart to client-go's internal `MockServer`.
///
/// The source service deliberately implements only the RPCs needed by its
/// client tests. Keeping this router equally narrow prevents unimplemented
/// generated-TiKV methods from acquiring invented behavior.
pub(crate) struct MockServer {
    core: Arc<MockServerCore>,
    address: Option<SocketAddr>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
    running: Arc<AtomicBool>,
    connections: Arc<ForceCloseRegistry>,
}

impl Default for MockServer {
    fn default() -> Self {
        Self {
            core: Arc::new(MockServerCore::default()),
            address: None,
            shutdown: None,
            task: None,
            running: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(ForceCloseRegistry::default()),
        }
    }
}

impl MockServer {
    pub(crate) fn set_batch_commands_handler(&self, handler: Option<BatchCommandsHandler>) {
        self.core.set_batch_commands_handler(handler);
    }

    pub(crate) fn set_metadata_checker(&self, checker: Option<MetadataChecker>) {
        self.core.set_metadata_checker(checker);
    }

    /// Starts the source-compatible service. An empty address has the same
    /// meaning as client-go's `Start("")`: bind loopback on an ephemeral port.
    pub(crate) async fn start(&mut self, address: &str) -> std::io::Result<u16> {
        if self.is_running() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "mock TiKV server is already running",
            ));
        }
        let listener = tokio::net::TcpListener::bind(if address.is_empty() {
            "127.0.0.1:0"
        } else {
            address
        })
        .await?;
        let bound_address = listener.local_addr()?;
        let (shutdown, shutdown_requested) = oneshot::channel();
        let running = self.running.clone();
        let connections = self.connections.clone();
        let service = MockTikvServer {
            core: self.core.clone(),
        };
        let task = tokio::spawn(async move {
            let incoming = TcpListenerStream::new(listener).map(move |connection| {
                connection.map(|stream| {
                    let forced = Arc::new(ForceClose::default());
                    connections.register(&forced);
                    CancellableTcpStream { stream, forced }
                })
            });
            let result = Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_requested.await;
                })
                .await;
            running.store(false, Ordering::Release);
            result
        });
        self.address = Some(bound_address);
        self.shutdown = Some(shutdown);
        self.task = Some(task);
        self.running.store(true, Ordering::Release);
        Ok(bound_address.port())
    }

    pub(crate) async fn start_loopback(&mut self) -> std::io::Result<u16> {
        self.start("").await
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn addr(&self) -> Option<String> {
        self.address.map(|address| address.to_string())
    }

    /// Force-stops the service, matching grpc-go's `Server.Stop`: active RPCs
    /// are terminated rather than being allowed to drain indefinitely.
    pub(crate) async fn stop(&mut self) -> Result<(), tonic::transport::Error> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.running.store(false, Ordering::Release);
        self.connections.close_all();
        if let Some(task) = self.task.take() {
            task.abort();
            match task.await {
                Ok(result) => result?,
                // Cancellation is the expected result of the forced-stop
                // path above. Dropping the server task closes all active h2
                // connections just as grpc-go's Stop does.
                Err(error) if error.is_cancelled() => {}
                Err(error) => panic!("mock TiKV server task must not panic: {error}"),
            }
        }
        Ok(())
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.running.store(false, Ordering::Release);
        self.connections.close_all();
    }
}

#[derive(Default)]
struct ForceCloseRegistry {
    connections: std::sync::Mutex<Vec<Weak<ForceClose>>>,
}

impl ForceCloseRegistry {
    fn register(&self, connection: &Arc<ForceClose>) {
        self.connections
            .lock()
            .unwrap()
            .push(Arc::downgrade(connection));
    }

    fn close_all(&self) {
        let mut connections = self.connections.lock().unwrap();
        connections.retain(|connection| {
            if let Some(connection) = connection.upgrade() {
                connection.close();
                true
            } else {
                false
            }
        });
    }
}

#[derive(Default)]
struct ForceClose {
    closed: AtomicBool,
    waker: AtomicWaker,
}

impl ForceClose {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.waker.wake();
    }

    fn is_closed(&self, cx: &Context<'_>) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return true;
        }
        self.waker.register(cx.waker());
        self.closed.load(Ordering::Acquire)
    }
}

struct CancellableTcpStream {
    stream: tokio::net::TcpStream,
    forced: Arc<ForceClose>,
}

impl Connected for CancellableTcpStream {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}

impl AsyncRead for CancellableTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.forced.is_closed(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "mock TiKV server stopped",
            )));
        }
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for CancellableTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.forced.is_closed(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "mock TiKV server stopped",
            )));
        }
        Pin::new(&mut self.stream).poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.forced.is_closed(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "mock TiKV server stopped",
            )));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// Source-compatible convenience constructor for an ephemeral loopback mock.
pub(crate) async fn start_mock_tikv_service() -> std::io::Result<(MockServer, u16)> {
    let mut server = MockServer::default();
    let port = server.start_loopback().await?;
    Ok((server, port))
}

type RpcStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, tonic::Status>> + Send>>;

#[derive(Clone)]
struct MockTikvServer {
    core: Arc<MockServerCore>,
}

impl tonic::server::NamedService for MockTikvServer {
    const NAME: &'static str = "tikvpb.Tikv";
}

impl<B> Service<http::Request<B>> for MockTikvServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        match request.uri().path() {
            "/tikvpb.Tikv/KvGet" => {
                let service = MockKvGet {
                    core: self.core.clone(),
                };
                Box::pin(async move {
                    Ok(
                        tonic::server::Grpc::new(ProstCodec::default())
                            .unary(service, request)
                            .await,
                    )
                })
            }
            "/tikvpb.Tikv/KvPrewrite" => {
                let service = MockKvPrewrite {
                    core: self.core.clone(),
                };
                Box::pin(async move {
                    Ok(
                        tonic::server::Grpc::new(ProstCodec::default())
                            .unary(service, request)
                            .await,
                    )
                })
            }
            "/tikvpb.Tikv/CoprocessorStream" => {
                let service = MockCoprocessorStream {
                    core: self.core.clone(),
                };
                Box::pin(async move {
                    Ok(
                        tonic::server::Grpc::new(ProstCodec::default())
                            .server_streaming(service, request)
                            .await,
                    )
                })
            }
            "/tikvpb.Tikv/BatchCommands" => {
                let service = MockBatchCommands {
                    core: self.core.clone(),
                };
                Box::pin(async move {
                    Ok(
                        tonic::server::Grpc::new(ProstCodec::default())
                            .streaming(service, request)
                            .await,
                    )
                })
            }
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

#[derive(Clone)]
struct MockKvGet {
    core: Arc<MockServerCore>,
}

impl tonic::server::UnaryService<kvrpcpb::GetRequest> for MockKvGet {
    type Response = kvrpcpb::GetResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<kvrpcpb::GetRequest>) -> Self::Future {
        let core = self.core.clone();
        Box::pin(async move {
            core.check_metadata(request.metadata())?;
            Ok(tonic::Response::new(kvrpcpb::GetResponse::default()))
        })
    }
}

#[derive(Clone)]
struct MockKvPrewrite {
    core: Arc<MockServerCore>,
}

impl tonic::server::UnaryService<kvrpcpb::PrewriteRequest> for MockKvPrewrite {
    type Response = kvrpcpb::PrewriteResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<kvrpcpb::PrewriteRequest>) -> Self::Future {
        let core = self.core.clone();
        Box::pin(async move {
            core.check_metadata(request.metadata())?;
            Ok(tonic::Response::new(kvrpcpb::PrewriteResponse::default()))
        })
    }
}

#[derive(Clone)]
struct MockCoprocessorStream {
    core: Arc<MockServerCore>,
}

impl tonic::server::ServerStreamingService<coprocessor::Request> for MockCoprocessorStream {
    type Response = coprocessor::Response;
    type ResponseStream = RpcStream<Self::Response>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<coprocessor::Request>) -> Self::Future {
        let core = self.core.clone();
        Box::pin(async move {
            core.check_metadata(request.metadata())?;
            let response: RpcStream<_> = Box::pin(futures::stream::once(async {
                Ok(coprocessor::Response::default())
            }));
            Ok(tonic::Response::new(response))
        })
    }
}

#[derive(Clone)]
struct MockBatchCommands {
    core: Arc<MockServerCore>,
}

impl tonic::server::StreamingService<tikvpb::BatchCommandsRequest> for MockBatchCommands {
    type Response = tikvpb::BatchCommandsResponse;
    type ResponseStream = RpcStream<Self::Response>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

    fn call(
        &mut self,
        request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
    ) -> Self::Future {
        let core = self.core.clone();
        Box::pin(async move {
            core.check_metadata(request.metadata())?;
            let responses: RpcStream<_> = Box::pin(futures::stream::unfold(
                (Some(request.into_inner()), core),
                |(requests, core)| async move {
                    let mut requests = requests?;
                    match requests.message().await {
                        Ok(Some(request)) => {
                            let response = core.batch_commands(request);
                            Some((response, (Some(requests), core)))
                        }
                        Ok(None) => None,
                        Err(status) => Some((Err(status), (None, core))),
                    }
                },
            ));
            Ok(tonic::Response::new(responses))
        })
    }
}

impl Default for MockServerCore {
    fn default() -> Self {
        Self {
            batch_handler: RwLock::new(None),
            metadata_checker: RwLock::new(None),
            feedback_sequence: AtomicU64::new(1),
        }
    }
}

impl MockServerCore {
    pub(crate) fn set_batch_commands_handler(&self, handler: Option<BatchCommandsHandler>) {
        *self.batch_handler.write().unwrap() = handler;
    }

    pub(crate) fn set_metadata_checker(&self, checker: Option<MetadataChecker>) {
        *self.metadata_checker.write().unwrap() = checker;
    }

    pub(crate) fn check_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> std::result::Result<(), tonic::Status> {
        self.metadata_checker
            .read()
            .unwrap()
            .as_ref()
            .map_or(Ok(()), |checker| checker(metadata))
    }

    pub(crate) fn batch_commands(
        &self,
        request: tikvpb::BatchCommandsRequest,
    ) -> std::result::Result<tikvpb::BatchCommandsResponse, tonic::Status> {
        if let Some(handler) = self.batch_handler.read().unwrap().clone() {
            return handler(request);
        }
        let feedback_sequence = self.feedback_sequence.fetch_add(1, Ordering::Relaxed);
        Ok(tikvpb::BatchCommandsResponse {
            responses: request
                .request_ids
                .iter()
                .map(|_| tikvpb::batch_commands_response::Response {
                    cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                        tikvpb::BatchCommandsEmptyResponse::default(),
                    )),
                })
                .collect(),
            request_ids: request.request_ids,
            health_feedback: Some(kvrpcpb::HealthFeedback {
                store_id: 1,
                feedback_seq_no: feedback_sequence,
                slow_score: 1,
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::tikvpb::tikv_client::TikvClient;
    use std::sync::atomic::AtomicUsize;

    async fn client_for(server: &MockServer) -> TikvClient<tonic::transport::Channel> {
        let address = server.addr().expect("started server has an address");
        TikvClient::connect(format!("http://{address}"))
            .await
            .expect("mock server accepts connections")
    }

    fn forwarded_request<T>(message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .metadata_mut()
            .insert("tikv-forwarded-host", "store-2".parse().unwrap());
        request
    }

    #[test]
    fn source_default_batch_response_echoes_ids_and_increments_feedback() {
        let server = MockServerCore::default();
        let first = server
            .batch_commands(tikvpb::BatchCommandsRequest {
                request_ids: vec![4, 9],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(first.request_ids, [4, 9]);
        assert_eq!(first.responses.len(), 2);
        assert_eq!(first.health_feedback.unwrap().feedback_seq_no, 1);
        let second = server
            .batch_commands(tikvpb::BatchCommandsRequest::default())
            .unwrap();
        assert_eq!(second.health_feedback.unwrap().feedback_seq_no, 2);
    }

    #[test]
    fn source_batch_handler_replaces_default_response() {
        let server = MockServerCore::default();
        server.set_batch_commands_handler(Some(Arc::new(|request| {
            Ok(tikvpb::BatchCommandsResponse {
                request_ids: request.request_ids,
                transport_layer_load: 42,
                ..Default::default()
            })
        })));
        let response = server
            .batch_commands(tikvpb::BatchCommandsRequest {
                request_ids: vec![7],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(response.request_ids, [7]);
        assert_eq!(response.transport_layer_load, 42);
        assert!(response.health_feedback.is_none());
    }

    #[test]
    fn source_metadata_checker_is_replaceable_and_propagates_errors() {
        let server = MockServerCore::default();
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("tikv-forwarded-host", "store-2".parse().unwrap());
        server.set_metadata_checker(Some(Arc::new(|metadata| {
            (metadata.get("tikv-forwarded-host") == Some(&"store-2".parse().unwrap()))
                .then_some(())
                .ok_or_else(|| tonic::Status::permission_denied("missing forwarding metadata"))
        })));
        assert!(server.check_metadata(&metadata).is_ok());
        assert_eq!(
            server
                .check_metadata(&tonic::metadata::MetadataMap::new())
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[tokio::test]
    async fn source_mock_server_serves_all_source_rpc_routes_and_lifecycle() {
        let checks = Arc::new(AtomicUsize::new(0));
        let (mut server, port) = start_mock_tikv_service().await.unwrap();
        let checker_checks = checks.clone();
        server.set_metadata_checker(Some(Arc::new(move |metadata| {
            (metadata.get("tikv-forwarded-host") == Some(&"store-2".parse().unwrap()))
                .then_some(())
                .ok_or_else(|| tonic::Status::permission_denied("missing forwarding metadata"))?;
            checker_checks.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })));
        assert_ne!(port, 0);
        assert!(server.is_running());
        assert!(server.addr().unwrap().ends_with(&format!(":{port}")));
        assert_eq!(
            server.start_loopback().await.unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );

        let mut client = client_for(&server).await;
        assert_eq!(
            client
                .kv_get(forwarded_request(kvrpcpb::GetRequest::default()))
                .await
                .unwrap()
                .into_inner(),
            kvrpcpb::GetResponse::default()
        );
        assert_eq!(
            client
                .kv_prewrite(forwarded_request(kvrpcpb::PrewriteRequest::default()))
                .await
                .unwrap()
                .into_inner(),
            kvrpcpb::PrewriteResponse::default()
        );
        let mut coprocessor = client
            .coprocessor_stream(forwarded_request(coprocessor::Request::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            coprocessor.message().await.unwrap(),
            Some(coprocessor::Response::default())
        );
        assert_eq!(coprocessor.message().await.unwrap(), None);

        let batch = tikvpb::BatchCommandsRequest {
            request_ids: vec![5, 8],
            ..Default::default()
        };
        let mut responses = client
            .batch_commands(forwarded_request(futures::stream::iter([batch])))
            .await
            .unwrap()
            .into_inner();
        let response = responses.message().await.unwrap().unwrap();
        assert_eq!(response.request_ids, [5, 8]);
        assert_eq!(response.responses.len(), 2);
        assert_eq!(response.health_feedback.unwrap().feedback_seq_no, 1);
        assert_eq!(checks.load(Ordering::Relaxed), 4);

        server.stop().await.unwrap();
        assert!(!server.is_running());

        let address = format!("127.0.0.1:{port}");
        assert_eq!(server.start(&address).await.unwrap(), port);
        assert!(server.is_running());
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_mock_server_exposes_batch_hook_and_metadata_errors_over_grpc() {
        let mut server = MockServer::default();
        server.set_batch_commands_handler(Some(Arc::new(|request| {
            Ok(tikvpb::BatchCommandsResponse {
                request_ids: request.request_ids,
                transport_layer_load: 99,
                ..Default::default()
            })
        })));
        server.start_loopback().await.unwrap();
        let mut client = client_for(&server).await;
        let mut responses = client
            .batch_commands(futures::stream::iter([tikvpb::BatchCommandsRequest {
                request_ids: vec![11],
                ..Default::default()
            }]))
            .await
            .unwrap()
            .into_inner();
        let response = responses.message().await.unwrap().unwrap();
        assert_eq!(response.request_ids, [11]);
        assert_eq!(response.transport_layer_load, 99);

        server.set_metadata_checker(Some(Arc::new(|_| {
            Err(tonic::Status::permission_denied("denied by mock"))
        })));
        let error = client
            .kv_get(kvrpcpb::GetRequest::default())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_stop_forcibly_terminates_an_active_batch_stream() {
        let (mut server, _) = start_mock_tikv_service().await.unwrap();
        let mut client = client_for(&server).await;
        let mut responses = client
            .batch_commands(futures::stream::pending::<tikvpb::BatchCommandsRequest>())
            .await
            .unwrap()
            .into_inner();

        tokio::time::timeout(std::time::Duration::from_secs(1), server.stop())
            .await
            .expect("forced mock-server stop must not wait for active streams")
            .unwrap();
        assert!(!server.is_running());
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), responses.message()).await,
            Ok(Err(_)) | Ok(Ok(None))
        ));
    }
}
