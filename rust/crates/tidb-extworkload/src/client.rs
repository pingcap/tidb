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

//! Go `pkg/extworkload/client`: the gRPC client to the external workload
//! controller. Requests that act on workload state carry keyspace identity in
//! `externalworkloadpb.RequestHeader` so the controller can route to the
//! right pool.

use std::time::Duration;

use tikv_client::proto::externalworkloadpb as pb;
use tikv_client::proto::externalworkloadpb::external_workload_controller_client::ExternalWorkloadControllerClient;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// The client's failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    /// Go `ErrControllerPaused`: the controller answered `ErrorType_PAUSED`
    /// to a temporarily paused worker.
    ControllerPaused,
    /// Every other failure, with Go's message.
    Other(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ControllerPaused => f.write_str("external workload controller: worker paused"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ClientError {}

/// Go `Option`: configures the gRPC client.
#[derive(Clone, Default)]
pub struct ClientOption {
    /// Go `KeyspaceID`.
    pub keyspace_id: u32,
    /// Go `KeyspaceName`.
    pub keyspace_name: String,
    /// Go `TiDBPool`: names the serving pool, for example vip-tidb-pool or
    /// super-vip-tidb-pool.
    pub tidb_pool: String,
    /// Go `ControllerAddr`: the external workload controller address.
    pub controller_addr: String,
    /// Go `TLSConfig`.
    pub tls_config: Option<ClientTlsConfig>,
    /// Go `Interceptors`: called before each unary call with the call's
    /// labels (the manager's metrics interceptor).
    pub interceptor: Option<fn(&CallLabels)>,
}

/// Go `metricLabels`, which the manager puts on a call's context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CallLabels {
    /// Go `workerType`; empty when the call carries no labels.
    pub worker_type: &'static str,
    /// Go `action`.
    pub action: &'static str,
}

/// One call's context: Go's `context.WithTimeout` and `withMetric` values.
#[derive(Clone, Copy, Debug)]
pub struct CallContext {
    /// The call's deadline from now.
    pub timeout: Duration,
    /// The metric labels, when the manager set them.
    pub labels: Option<CallLabels>,
}

/// Go `Client`: wraps the kvproto `ExternalWorkloadController` service.
pub trait Client: Send + Sync {
    /// Go `Close`.
    ///
    /// # Errors
    ///
    /// The connection could not be closed.
    fn close(&self) -> Result<(), ClientError>;
    /// Go `Ping`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn ping(&self, ctx: CallContext) -> Result<(), ClientError>;
    /// Go `RegisterGCV2`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn register_gcv2(&self, ctx: CallContext, safe_point: u64, gc_life_time: i64) -> Result<(), ClientError>;
    /// Go `RecycleGCV2`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn recycle_gcv2(&self, ctx: CallContext, safe_point: u64) -> Result<(), ClientError>;
    /// Go `UpdateGCLifeTime`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn update_gc_life_time(&self, ctx: CallContext, gc_life_time: i64) -> Result<(), ClientError>;
    /// Go `RegisterTTLTask`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn register_ttl_task(&self, ctx: CallContext, table_id: i64, ttl_job_enable: bool) -> Result<(), ClientError>;
    /// Go `DeleteTTLTableInfo`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn delete_ttl_table_info(&self, ctx: CallContext, table_id: i64) -> Result<(), ClientError>;
    /// Go `RecycleTTLTask`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn recycle_ttl_task(&self, ctx: CallContext, completed_job_create_time: u64) -> Result<(), ClientError>;
    /// Go `UpdateTTLJobEnable`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn update_ttl_job_enable(&self, ctx: CallContext, ttl_job_enable: bool) -> Result<(), ClientError>;
    /// Go `RegisterAutoAnalyze`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn register_auto_analyze(&self, ctx: CallContext, task_id: u64) -> Result<(), ClientError>;
    /// Go `RecycleAutoAnalyze`.
    ///
    /// # Errors
    ///
    /// See [`map_response`].
    fn recycle_auto_analyze(&self, ctx: CallContext, task_id: u64) -> Result<(), ClientError>;
}

/// Go `New`: a controller client for `opt.controller_addr`. The connection is
/// made lazily, as Go's `grpc.NewClient`'s is.
///
/// # Errors
///
/// The address is empty or cannot be parsed.
pub fn new(opt: &ClientOption) -> Result<Box<dyn Client>, ClientError> {
    let host = normalize_addr(&opt.controller_addr)?;
    let scheme = if opt.tls_config.is_some() { "https" } else { "http" };
    let annotate =
        |error: String| ClientError::Other(format!("create external workload controller client: {error}"));
    let mut endpoint = Endpoint::from_shared(format!("{scheme}://{host}")).map_err(|error| annotate(error.to_string()))?;
    if let Some(tls) = &opt.tls_config {
        endpoint = endpoint.tls_config(tls.clone()).map_err(|error| annotate(error.to_string()))?;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("ext-workload-client")
        .enable_all()
        .build()
        .map_err(|error| annotate(error.to_string()))?;
    let stub = {
        let _guard = runtime.enter();
        ExternalWorkloadControllerClient::new(endpoint.connect_lazy())
    };
    Ok(Box::new(GrpcClient { opt: opt.clone(), runtime, stub }))
}

/// Go `normalizeAddr`.
pub(crate) fn normalize_addr(addr: &str) -> Result<String, ClientError> {
    let addr = addr.trim();
    if addr.is_empty() {
        return Err(ClientError::Other("external workload client: empty controller address".to_owned()));
    }
    let Some((_, rest)) = addr.split_once("://") else {
        return Ok(addr.to_owned());
    };
    // Go `url.Parse(addr).Host`: the authority, without user info.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    if host.is_empty() {
        return Err(ClientError::Other(format!("external workload client: controller address {addr:?} has no host")));
    }
    Ok(host.to_owned())
}

struct GrpcClient {
    opt: ClientOption,
    runtime: tokio::runtime::Runtime,
    stub: ExternalWorkloadControllerClient<Channel>,
}

impl GrpcClient {
    fn header(&self) -> Option<pb::RequestHeader> {
        Some(pb::RequestHeader {
            keyspace: Some(pb::request_header::Keyspace::KeyspaceId(self.opt.keyspace_id)),
            keyspace_name: self.opt.keyspace_name.clone(),
            tidb_pool: self.opt.tidb_pool.clone(),
        })
    }

    /// One unary call under the context's deadline, mapped as Go's
    /// `mapResponse(resp, err)` maps it.
    fn call<Fut>(
        &self,
        ctx: CallContext,
        rpc: &'static str,
        invoke: impl FnOnce(ExternalWorkloadControllerClient<Channel>) -> Fut,
    ) -> Result<(), ClientError>
    where
        Fut: std::future::Future<Output = Result<tonic::Response<pb::Response>, tonic::Status>>,
    {
        if let (Some(interceptor), Some(labels)) = (self.opt.interceptor, ctx.labels.as_ref()) {
            interceptor(labels);
        }
        let stub = self.stub.clone();
        let outcome = self.runtime.block_on(async move { tokio::time::timeout(ctx.timeout, invoke(stub)).await });
        let response = match outcome {
            Ok(Ok(response)) => Ok(response.into_inner()),
            Ok(Err(status)) => Err(status.to_string()),
            Err(_) => Err("context deadline exceeded".to_owned()),
        };
        map_response(rpc, response)
    }
}

/// Go `mapResponse`: an RPC failure annotated with the RPC's name, a paused
/// controller as [`ClientError::ControllerPaused`], any other error type with
/// its message.
pub(crate) fn map_response(rpc: &str, response: Result<pb::Response, String>) -> Result<(), ClientError> {
    let response =
        response.map_err(|error| ClientError::Other(format!("external workload rpc {rpc}: {error}")))?;
    let Some(error) = response.error else {
        return Ok(());
    };
    if error.r#type == pb::ErrorType::Ok as i32 {
        return Ok(());
    }
    if error.r#type == pb::ErrorType::Paused as i32 {
        return Err(ClientError::ControllerPaused);
    }
    Err(ClientError::Other(format!("external workload rpc {rpc}: {}", error.message)))
}

impl Client for GrpcClient {
    fn close(&self) -> Result<(), ClientError> {
        Ok(())
    }

    fn ping(&self, ctx: CallContext) -> Result<(), ClientError> {
        self.call(ctx, "Ping", |mut stub| async move { stub.ping(pb::PingRequest {}).await })
    }

    fn register_gcv2(&self, ctx: CallContext, safe_point: u64, gc_life_time: i64) -> Result<(), ClientError> {
        let request = pb::RegisterGcv2Request { header: self.header(), safe_point, gc_life_time };
        self.call(ctx, "RegisterGCV2", |mut stub| async move { stub.register_gcv2(request).await })
    }

    fn recycle_gcv2(&self, ctx: CallContext, safe_point: u64) -> Result<(), ClientError> {
        let request = pb::RecycleGcv2Request { header: self.header(), safe_point };
        self.call(ctx, "RecycleGCV2", |mut stub| async move { stub.recycle_gcv2(request).await })
    }

    fn update_gc_life_time(&self, ctx: CallContext, gc_life_time: i64) -> Result<(), ClientError> {
        let request = pb::UpdateGcLifeTimeRequest { header: self.header(), gc_life_time };
        self.call(ctx, "UpdateGCLifeTime", |mut stub| async move { stub.update_gc_life_time(request).await })
    }

    fn register_ttl_task(&self, ctx: CallContext, table_id: i64, ttl_job_enable: bool) -> Result<(), ClientError> {
        let request = pb::RegisterTtlTaskRequest { header: self.header(), table_id, ttl_job_enable };
        self.call(ctx, "RegisterTTLTask", |mut stub| async move { stub.register_ttl_task(request).await })
    }

    fn delete_ttl_table_info(&self, ctx: CallContext, table_id: i64) -> Result<(), ClientError> {
        let request = pb::DeleteTtlTableInfoRequest { header: self.header(), table_id };
        self.call(ctx, "DeleteTTLTableInfo", |mut stub| async move { stub.delete_ttl_table_info(request).await })
    }

    fn recycle_ttl_task(&self, ctx: CallContext, completed_job_create_time: u64) -> Result<(), ClientError> {
        let request = pb::RecycleTtlTaskRequest { header: self.header(), completed_job_create_time };
        self.call(ctx, "RecycleTTLTask", |mut stub| async move { stub.recycle_ttl_task(request).await })
    }

    fn update_ttl_job_enable(&self, ctx: CallContext, ttl_job_enable: bool) -> Result<(), ClientError> {
        let request = pb::UpdateTtlJobEnableRequest { header: self.header(), ttl_job_enable };
        self.call(ctx, "UpdateTTLJobEnable", |mut stub| async move { stub.update_ttl_job_enable(request).await })
    }

    fn register_auto_analyze(&self, ctx: CallContext, task_id: u64) -> Result<(), ClientError> {
        let request = pb::RegisterAutoAnalyzeRequest { header: self.header(), task_id };
        self.call(ctx, "RegisterAutoAnalyze", |mut stub| async move { stub.register_auto_analyze(request).await })
    }

    fn recycle_auto_analyze(&self, ctx: CallContext, task_id: u64) -> Result<(), ClientError> {
        let request = pb::RecycleAutoAnalyzeRequest { header: self.header(), task_id };
        self.call(ctx, "RecycleAutoAnalyze", |mut stub| async move { stub.recycle_auto_analyze(request).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tikv_client::proto::externalworkloadpb::external_workload_controller_server::{
        ExternalWorkloadController, ExternalWorkloadControllerServer,
    };

    /// Go `stubServer`: records each request, and answers `error` when set.
    #[derive(Default)]
    struct StubServer {
        seen: Mutex<Vec<String>>,
        error: Option<pb::Error>,
    }

    impl StubServer {
        fn answer(&self, seen: String) -> Result<tonic::Response<pb::Response>, tonic::Status> {
            self.seen.lock().unwrap().push(seen);
            Ok(tonic::Response::new(pb::Response { error: self.error.clone() }))
        }
    }

    fn header(header: Option<&pb::RequestHeader>) -> String {
        let header = header.expect("a request header");
        format!("{:?}/{}/{}", header.keyspace, header.keyspace_name, header.tidb_pool)
    }

    type Answer = Result<tonic::Response<pb::Response>, tonic::Status>;

    struct Stub(Arc<StubServer>);

    #[tonic::async_trait]
    impl ExternalWorkloadController for Stub {
        async fn ping(&self, _: tonic::Request<pb::PingRequest>) -> Answer {
            self.0.answer("Ping".to_owned())
        }
        async fn register_gc(&self, _: tonic::Request<pb::RegisterGcRequest>) -> Answer {
            self.0.answer("RegisterGC".to_owned())
        }
        async fn recycle_gc(&self, _: tonic::Request<pb::RecycleGcRequest>) -> Answer {
            self.0.answer("RecycleGC".to_owned())
        }
        async fn register_gcv2(&self, request: tonic::Request<pb::RegisterGcv2Request>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("RegisterGCV2 {} {} {}", header(r.header.as_ref()), r.safe_point, r.gc_life_time))
        }
        async fn recycle_gcv2(&self, request: tonic::Request<pb::RecycleGcv2Request>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("RecycleGCV2 {} {}", header(r.header.as_ref()), r.safe_point))
        }
        async fn update_gc_life_time(&self, request: tonic::Request<pb::UpdateGcLifeTimeRequest>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("UpdateGCLifeTime {} {}", header(r.header.as_ref()), r.gc_life_time))
        }
        async fn get_bg_task_config(
            &self,
            _: tonic::Request<pb::GetBgTaskConfigRequest>,
        ) -> Result<tonic::Response<pb::GetBgTaskConfigResponse>, tonic::Status> {
            Ok(tonic::Response::new(pb::GetBgTaskConfigResponse::default()))
        }
        async fn register_bg_task(&self, _: tonic::Request<pb::RegisterBgTaskRequest>) -> Answer {
            self.0.answer("RegisterBgTask".to_owned())
        }
        async fn recycle_bg_task(&self, _: tonic::Request<pb::RecycleBgTaskRequest>) -> Answer {
            self.0.answer("RecycleBgTask".to_owned())
        }
        async fn update_bg_task_exec_id(&self, _: tonic::Request<pb::UpdateBgTaskExecIdRequest>) -> Answer {
            self.0.answer("UpdateBgTaskExecID".to_owned())
        }
        async fn register_remote_query(&self, _: tonic::Request<pb::RegisterRemoteQueryRequest>) -> Answer {
            self.0.answer("RegisterRemoteQuery".to_owned())
        }
        async fn register_ttl_task(&self, request: tonic::Request<pb::RegisterTtlTaskRequest>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("RegisterTTLTask {} {} {}", header(r.header.as_ref()), r.table_id, r.ttl_job_enable))
        }
        async fn delete_ttl_table_info(&self, request: tonic::Request<pb::DeleteTtlTableInfoRequest>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("DeleteTTLTableInfo {} {}", header(r.header.as_ref()), r.table_id))
        }
        async fn recycle_ttl_task(&self, request: tonic::Request<pb::RecycleTtlTaskRequest>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("RecycleTTLTask {} {}", header(r.header.as_ref()), r.completed_job_create_time))
        }
        async fn update_ttl_job_enable(&self, request: tonic::Request<pb::UpdateTtlJobEnableRequest>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("UpdateTTLJobEnable {} {}", header(r.header.as_ref()), r.ttl_job_enable))
        }
        async fn register_auto_analyze(&self, request: tonic::Request<pb::RegisterAutoAnalyzeRequest>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("RegisterAutoAnalyze {} {}", header(r.header.as_ref()), r.task_id))
        }
        async fn recycle_auto_analyze(&self, request: tonic::Request<pb::RecycleAutoAnalyzeRequest>) -> Answer {
            let r = request.into_inner();
            self.0.answer(format!("RecycleAutoAnalyze {} {}", header(r.header.as_ref()), r.task_id))
        }
    }

    /// Go `startStubServer`: the stub on a local port, and a client of it.
    fn start_stub_server(stub: &Arc<StubServer>, interceptor: Option<fn(&CallLabels)>) -> (Box<dyn Client>, tokio::runtime::Runtime) {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
        let listener = runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
        let addr = listener.local_addr().unwrap();
        let service = ExternalWorkloadControllerServer::new(Stub(Arc::clone(stub)));
        runtime.spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = new(&ClientOption {
            keyspace_id: 42,
            keyspace_name: "ks".to_owned(),
            tidb_pool: "vip-tidb-pool".to_owned(),
            controller_addr: addr.to_string(),
            tls_config: None,
            interceptor,
        })
        .unwrap();
        (client, runtime)
    }

    fn test_context() -> CallContext {
        CallContext { timeout: Duration::from_secs(5), labels: None }
    }

    /// Go `TestClientRoundTrip`: each call reaches the controller with the
    /// keyspace header and its arguments.
    #[test]
    fn test_client_round_trip() {
        let stub = Arc::new(StubServer::default());
        let (cli, _runtime) = start_stub_server(&stub, None);
        let ctx = test_context();
        cli.ping(ctx).unwrap();
        cli.register_gcv2(ctx, 12, 600).unwrap();
        cli.recycle_gcv2(ctx, 1234).unwrap();
        cli.update_gc_life_time(ctx, 900).unwrap();
        cli.register_ttl_task(ctx, 7, true).unwrap();
        cli.delete_ttl_table_info(ctx, 7).unwrap();
        cli.recycle_ttl_task(ctx, 99).unwrap();
        cli.update_ttl_job_enable(ctx, false).unwrap();
        cli.register_auto_analyze(ctx, 5).unwrap();
        cli.recycle_auto_analyze(ctx, 5).unwrap();
        cli.close().unwrap();
        let h = "Some(KeyspaceId(42))/ks/vip-tidb-pool";
        assert_eq!(
            *stub.seen.lock().unwrap(),
            [
                "Ping".to_owned(),
                format!("RegisterGCV2 {h} 12 600"),
                format!("RecycleGCV2 {h} 1234"),
                format!("UpdateGCLifeTime {h} 900"),
                format!("RegisterTTLTask {h} 7 true"),
                format!("DeleteTTLTableInfo {h} 7"),
                format!("RecycleTTLTask {h} 99"),
                format!("UpdateTTLJobEnable {h} false"),
                format!("RegisterAutoAnalyze {h} 5"),
                format!("RecycleAutoAnalyze {h} 5"),
            ]
        );
    }

    static INTERCEPTED: Mutex<Vec<CallLabels>> = Mutex::new(Vec::new());

    fn record_labels(labels: &CallLabels) {
        INTERCEPTED.lock().unwrap().push(*labels);
    }

    /// Go `TestClientInterceptor`.
    #[test]
    fn test_client_interceptor() {
        let stub = Arc::new(StubServer::default());
        let (cli, _runtime) = start_stub_server(&stub, Some(record_labels));
        let labels = CallLabels { worker_type: "gcv2", action: "register" };
        cli.register_gcv2(CallContext { timeout: Duration::from_secs(5), labels: Some(labels) }, 1, 1).unwrap();
        cli.ping(test_context()).unwrap();
        assert_eq!(*INTERCEPTED.lock().unwrap(), [labels]);
    }

    /// Go `TestClientErrorMapping`.
    #[test]
    fn test_client_error_mapping() {
        let paused = Arc::new(StubServer {
            error: Some(pb::Error { r#type: pb::ErrorType::Paused as i32, message: "paused".to_owned() }),
            ..Default::default()
        });
        let (cli, _runtime) = start_stub_server(&paused, None);
        assert_eq!(cli.ping(test_context()), Err(ClientError::ControllerPaused));

        let unknown = Arc::new(StubServer {
            error: Some(pb::Error { r#type: pb::ErrorType::Unknown as i32, message: "boom".to_owned() }),
            ..Default::default()
        });
        let (cli, _runtime) = start_stub_server(&unknown, None);
        assert_eq!(
            cli.recycle_gcv2(test_context(), 1).unwrap_err().to_string(),
            "external workload rpc RecycleGCV2: boom"
        );
    }

    /// Go `TestNewClientValidation`.
    #[test]
    fn test_new_client_validation() {
        let error = new(&ClientOption::default()).err().unwrap();
        assert_eq!(error.to_string(), "external workload client: empty controller address");
    }

    /// Go `TestNormalizeAddr`.
    #[test]
    fn test_normalize_addr() {
        assert_eq!(normalize_addr(" controller:8080 ").unwrap(), "controller:8080");
        assert_eq!(normalize_addr("http://controller:8080/path").unwrap(), "controller:8080");
        assert_eq!(normalize_addr("https://user@controller:8080").unwrap(), "controller:8080");
        assert_eq!(
            normalize_addr("  ").unwrap_err().to_string(),
            "external workload client: empty controller address"
        );
        assert_eq!(
            normalize_addr("http:///path").unwrap_err().to_string(),
            "external workload client: controller address \"http:///path\" has no host"
        );
    }

    /// Go `mapResponse`.
    #[test]
    fn map_response_answers_as_go() {
        let error = |r#type: pb::ErrorType, message: &str| {
            Ok(pb::Response { error: Some(pb::Error { r#type: r#type as i32, message: message.to_owned() }) })
        };
        assert_eq!(map_response("Ping", Ok(pb::Response { error: None })), Ok(()));
        assert_eq!(map_response("Ping", error(pb::ErrorType::Ok, "")), Ok(()));
        assert_eq!(map_response("Ping", error(pb::ErrorType::Paused, "p")), Err(ClientError::ControllerPaused));
        assert_eq!(
            map_response("RegisterGCV2", error(pb::ErrorType::Unknown, "boom")).unwrap_err().to_string(),
            "external workload rpc RegisterGCV2: boom"
        );
        assert_eq!(
            map_response("Ping", Err("unavailable".to_owned())).unwrap_err().to_string(),
            "external workload rpc Ping: unavailable"
        );
    }
}
