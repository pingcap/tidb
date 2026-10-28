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

//! The diagnostics gRPC client side of the cluster tables: Go
//! `infoschema.FetchClusterServerInfoWithoutPrivilegeCheck` and
//! `getServerInfoByGRPC` (`CLUSTER_HARDWARE`, `CLUSTER_LOAD`,
//! `CLUSTER_SYSTEMINFO`), and `executor.clusterLogRetriever`'s
//! `startRetrieving` and heap merge (`CLUSTER_LOG`).
//!
//! TiDB and TiProxy serve the diagnostics service on their status address;
//! every other component serves it on its client address.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tidb_datatype::Datum;
use tidb_domain::cluster_topology::ClusterServer;
use tidb_pd_client::ClusterSecurity;
use tidb_proto::diagnosticspb as pb;

/// Owns the gRPC runtime the synchronous SQL executors borrow.
pub struct ClusterDiagnosticsClient {
    security: ClusterSecurity,
    runtime: tokio::runtime::Runtime,
}

impl std::fmt::Debug for ClusterDiagnosticsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterDiagnosticsClient")
            .finish_non_exhaustive()
    }
}

/// Go's `status.Error` text for a gRPC failure.
fn go_status_error(status: &tonic::Status) -> String {
    let code = match status.code() {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "Canceled",
        tonic::Code::Unknown => "Unknown",
        tonic::Code::InvalidArgument => "InvalidArgument",
        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
        tonic::Code::NotFound => "NotFound",
        tonic::Code::AlreadyExists => "AlreadyExists",
        tonic::Code::PermissionDenied => "PermissionDenied",
        tonic::Code::ResourceExhausted => "ResourceExhausted",
        tonic::Code::FailedPrecondition => "FailedPrecondition",
        tonic::Code::Aborted => "Aborted",
        tonic::Code::OutOfRange => "OutOfRange",
        tonic::Code::Unimplemented => "Unimplemented",
        tonic::Code::Internal => "Internal",
        tonic::Code::Unavailable => "Unavailable",
        tonic::Code::DataLoss => "DataLoss",
        tonic::Code::Unauthenticated => "Unauthenticated",
    };
    format!("rpc error: code = {code} desc = {}", status.message())
}

/// Go `context.WithTimeout`'s expiry as a gRPC call reports it.
const DEADLINE_EXCEEDED: &str =
    "rpc error: code = DeadlineExceeded desc = context deadline exceeded";

/// grpc-go's default `MinConnectTimeout`: the time a new connection has to
/// connect and deliver the server's HTTP/2 preface before it fails.
const MIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// A TCP connector that records whether the peer has sent anything.
#[derive(Clone)]
struct TrackingConnector {
    heard: Arc<AtomicBool>,
}

/// The connection's socket, flagging the first byte it reads.
struct TrackingTcp {
    tcp: tokio::net::TcpStream,
    heard: Arc<AtomicBool>,
}

impl tokio::io::AsyncRead for TrackingTcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let this = &mut *self;
        let polled = Pin::new(&mut this.tcp).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            this.heard.store(true, Ordering::Release);
        }
        polled
    }
}

impl tokio::io::AsyncWrite for TrackingTcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.tcp).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.tcp).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.tcp).poll_shutdown(cx)
    }
}

impl tonic::codegen::Service<tonic::transport::Uri> for TrackingConnector {
    type Response = hyper_util::rt::TokioIo<TrackingTcp>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, std::io::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: tonic::transport::Uri) -> Self::Future {
        let heard = Arc::clone(&self.heard);
        Box::pin(async move {
            let host = uri
                .host()
                .unwrap_or_default()
                .trim_matches(['[', ']'])
                .to_owned();
            let port = uri.port_u16().unwrap_or(80);
            let tcp = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
            tcp.set_nodelay(true)?;
            Ok(hyper_util::rt::TokioIo::new(TrackingTcp { tcp, heard }))
        })
    }
}

/// grpc-go's readiness wait for a call without its own deadline: the call
/// proceeds without limit once the peer has spoken, but a connection that
/// has delivered nothing after [`MIN_CONNECT_TIMEOUT`] fails Unavailable.
async fn await_ready<F, T>(call: F, heard: &AtomicBool) -> Result<T, String>
where
    F: Future<Output = Result<T, tonic::Status>>,
{
    tokio::pin!(call);
    tokio::select! {
        result = &mut call => return result.map_err(|status| go_status_error(&status)),
        _ = tokio::time::sleep(MIN_CONNECT_TIMEOUT) => {}
    }
    if !heard.load(Ordering::Acquire) {
        return Err("rpc error: code = Unavailable desc = connection error: desc = \"error reading server preface: deadline exceeded\"".to_owned());
    }
    call.await.map_err(|status| go_status_error(&status))
}

/// Go `serverInfoItemToRows`: one row per pair.
fn server_info_item_to_rows(items: &[pb::ServerInfoItem], tp: &str, addr: &str) -> Vec<Vec<Datum>> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut rows = Vec::new();
    for v in items {
        for item in &v.pairs {
            rows.push(vec![
                text(tp),
                text(addr),
                text(&v.tp),
                text(&v.name),
                text(&item.key),
                text(&item.value),
            ]);
        }
    }
    rows
}

/// One server's `SearchLog` stream: each response's messages, then the
/// error that ended it, if any.
#[derive(Debug, Default)]
struct LogStream {
    batches: std::collections::VecDeque<Result<Vec<pb::LogMessage>, String>>,
}

/// Go `logStreamResult`: the unread messages of a batch and the stream to
/// read the next one from.
struct LogStreamResult {
    stream: usize,
    addr: String,
    typ: String,
    messages: std::collections::VecDeque<pb::LogMessage>,
}

/// Go `logResponseHeap.Less`.
fn less(h: &[LogStreamResult], i: usize, j: usize) -> bool {
    let (lhs, rhs) = (h[i].messages[0].time, h[j].messages[0].time);
    if lhs != rhs {
        return lhs < rhs;
    }
    h[i].typ < h[j].typ
}

/// Go `container/heap` `up`.
fn heap_up(h: &mut [LogStreamResult], mut j: usize) {
    while j > 0 {
        let i = (j - 1) / 2; // parent
        if i == j || !less(h, j, i) {
            break;
        }
        h.swap(i, j);
        j = i;
    }
}

/// Go `container/heap` `down`.
fn heap_down(h: &mut [LogStreamResult], i0: usize, n: usize) -> bool {
    let mut i = i0;
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1; // left child
        let j2 = j1 + 1;
        if j2 < n && less(h, j2, j1) {
            j = j2; // = 2*i + 2  // right child
        }
        if !less(h, j, i) {
            break;
        }
        h.swap(i, j);
        i = j;
    }
    i > i0
}

/// Go `heap.Init`.
fn heap_init(h: &mut [LogStreamResult]) {
    let n = h.len();
    for i in (0..n / 2).rev() {
        heap_down(h, i, n);
    }
}

/// Go `heap.Pop`.
fn heap_pop(h: &mut Vec<LogStreamResult>) -> LogStreamResult {
    let n = h.len() - 1;
    h.swap(0, n);
    heap_down(h, 0, n);
    h.pop().expect("non-empty heap")
}

/// Go `heap.Push`.
fn heap_push(h: &mut Vec<LogStreamResult>, x: LogStreamResult) {
    h.push(x);
    let n = h.len() - 1;
    heap_up(h, n);
}

impl ClusterDiagnosticsClient {
    /// Uses the same cluster TLS policy as the other internal transports.
    pub fn new(security: &ClusterSecurity) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("tidb-diagnostics-client")
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            security: security.clone(),
            runtime,
        })
    }

    /// Runs `future` on the owned runtime from any calling context, including
    /// a thread that is itself driving a runtime.
    fn run<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        std::thread::scope(|scope| {
            scope
                .spawn(|| self.runtime.block_on(future))
                .join()
                .expect("diagnostics client worker panicked")
        })
    }

    /// Go `grpc.Dial`: lazy, so a dead peer fails its first call instead.
    /// The returned flag turns true once the peer has sent any byte, which
    /// is how [`await_ready`] tells grpc-go's "connection not ready" apart.
    fn client(
        &self,
        remote: &str,
    ) -> Result<
        (
            pb::diagnostics_client::DiagnosticsClient<tonic::transport::Channel>,
            Arc<AtomicBool>,
        ),
        String,
    > {
        let endpoint = tidb_pd_client::secure_endpoint(remote, &self.security)
            .map_err(|error| error.to_string())?
            .connect_timeout(MIN_CONNECT_TIMEOUT);
        let heard = Arc::new(AtomicBool::new(false));
        let channel = endpoint.connect_with_connector_lazy(TrackingConnector {
            heard: Arc::clone(&heard),
        });
        Ok((
            pb::diagnostics_client::DiagnosticsClient::new(channel),
            heard,
        ))
    }

    /// Go `FetchClusterServerInfoWithoutPrivilegeCheck` with statement
    /// warnings: every server is asked concurrently with a ten-second limit;
    /// a failed server becomes a warning and the others keep their order.
    pub fn fetch_server_info(
        &self,
        servers: &[ClusterServer],
        tp: pb::ServerInfoType,
        warnings: &mut Vec<String>,
    ) -> Vec<Vec<Datum>> {
        let results = self.run(async {
            let calls = servers.iter().map(|srv| async move {
                let remote = if srv.server_type == "tidb" || srv.server_type == "tiproxy" {
                    &srv.status_address
                } else {
                    &srv.address
                };
                // The ten-second deadline is shorter than the connect wait.
                let (mut cli, _) = self.client(remote)?;
                let call = cli.server_info(pb::ServerInfoRequest { tp: tp as i32 });
                let items = match tokio::time::timeout(Duration::from_secs(10), call).await {
                    Err(_) => return Err(DEADLINE_EXCEEDED.to_owned()),
                    Ok(Err(status)) => return Err(go_status_error(&status)),
                    Ok(Ok(response)) => response.into_inner().items,
                };
                Ok(server_info_item_to_rows(
                    &items,
                    &srv.server_type,
                    &srv.address,
                ))
            });
            futures::future::join_all(calls).await
        });
        let mut rows = Vec::new();
        for result in results {
            match result {
                Ok(part) => rows.extend(part),
                Err(error) => warnings.push(error),
            }
        }
        rows
    }

    /// Go `clusterLogRetriever.startRetrieving` and `retrieve`: one
    /// `SearchLog` stream per server with a status address, merged by
    /// message time (then component type) through Go's heap. Stream
    /// failures become warnings in the order Go records them.
    pub fn search_log(
        &self,
        servers: &[ClusterServer],
        req: &pb::SearchLogRequest,
        warnings: &mut Vec<String>,
    ) -> Vec<Vec<Datum>> {
        let mut targets = Vec::new();
        for srv in servers {
            if srv.status_address.is_empty() {
                warnings.push(format!(
                    "{} node {} does not contain status address",
                    srv.server_type, srv.address
                ));
                continue;
            }
            targets.push(srv);
        }
        let mut streams: Vec<LogStream> = self.run(async {
            let calls = targets.iter().map(|srv| async move {
                // TiDB and TiProxy provide diagnostics service via status address
                let remote = if srv.server_type == "tidb" || srv.server_type == "tiproxy" {
                    &srv.status_address
                } else {
                    &srv.address
                };
                let mut stream = LogStream::default();
                let (mut cli, heard) = match self.client(remote) {
                    Ok(client) => client,
                    Err(error) => {
                        stream.batches.push_back(Err(error));
                        return stream;
                    }
                };
                let mut responses = match await_ready(cli.search_log(req.clone()), &heard).await {
                    Ok(responses) => responses.into_inner(),
                    Err(error) => {
                        stream.batches.push_back(Err(error));
                        return stream;
                    }
                };
                loop {
                    match responses.message().await {
                        Ok(None) => return stream,
                        Ok(Some(res)) => stream.batches.push_back(Ok(res.messages)),
                        Err(status) => {
                            stream.batches.push_back(Err(go_status_error(&status)));
                            return stream;
                        }
                    }
                }
            });
            futures::future::join_all(calls).await
        });

        // initialize the heap
        let mut heap: Vec<LogStreamResult> = Vec::new();
        for (index, srv) in targets.iter().enumerate() {
            match streams[index].batches.pop_front() {
                None => {}
                Some(Err(error)) => warnings.push(error),
                Some(Ok(messages)) if messages.is_empty() => {}
                Some(Ok(messages)) => heap.push(LogStreamResult {
                    stream: index,
                    addr: srv.address.clone(),
                    typ: srv.server_type.clone(),
                    messages: messages.into(),
                }),
            }
        }
        heap_init(&mut heap);

        // Merge the results
        let mut final_rows = Vec::new();
        let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        while !heap.is_empty() {
            let mut min_time_item = heap_pop(&mut heap);
            let head_message = min_time_item
                .messages
                .pop_front()
                .expect("heap items hold messages");
            let level = pb::LogLevel::try_from(head_message.level).map_or_else(
                |_| head_message.level.to_string(),
                |level| level.as_str_name().to_owned(),
            );
            final_rows.push(vec![
                text(&format_logging_time(head_message.time)),
                text(&min_time_item.typ),
                text(&min_time_item.addr),
                text(&level.to_uppercase()),
                text(&head_message.message),
            ]);
            // Current streaming result is drained, read the next to supply.
            if min_time_item.messages.is_empty() {
                match streams[min_time_item.stream].batches.pop_front() {
                    None => {}
                    Some(Err(error)) => warnings.push(error),
                    Some(Ok(messages)) => {
                        if !messages.is_empty() {
                            min_time_item.messages = messages.into();
                            heap_push(&mut heap, min_time_item);
                        }
                    }
                }
            } else {
                heap_push(&mut heap, min_time_item);
            }
        }
        final_rows
    }
}

/// Go `time.UnixMilli(t).Format("2006/01/02 15:04:05.000")` in the
/// process's local zone.
fn format_logging_time(millis: i64) -> String {
    use chrono::TimeZone;
    match chrono::Local.timestamp_millis_opt(millis) {
        chrono::LocalResult::Single(time) | chrono::LocalResult::Ambiguous(time, _) => {
            time.format("%Y/%m/%d %H:%M:%S%.3f").to_string()
        }
        chrono::LocalResult::None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(time: i64, text: &str) -> pb::LogMessage {
        pb::LogMessage {
            time,
            level: pb::LogLevel::Info as i32,
            message: text.to_owned(),
        }
    }

    fn result(stream: usize, typ: &str, messages: Vec<pb::LogMessage>) -> LogStreamResult {
        LogStreamResult {
            stream,
            addr: format!("addr-{stream}"),
            typ: typ.to_owned(),
            messages: messages.into(),
        }
    }

    /// Go's heap orders the heads by time, then component type.
    #[test]
    fn log_heap_merges_by_time_then_type() {
        let mut heap = vec![
            result(0, "tikv", vec![message(3, "c")]),
            result(1, "pd", vec![message(1, "a")]),
            result(2, "tidb", vec![message(3, "b")]),
        ];
        heap_init(&mut heap);
        let order: Vec<String> = std::iter::from_fn(|| {
            (!heap.is_empty()).then(|| heap_pop(&mut heap).messages[0].message.clone())
        })
        .collect();
        assert_eq!(order, ["a", "b", "c"]);
    }

    #[test]
    fn grpc_errors_render_as_go_status_errors() {
        assert_eq!(
            go_status_error(&tonic::Status::unimplemented("unexpected HTTP status code received from server: 404 (Not Found)")),
            "rpc error: code = Unimplemented desc = unexpected HTTP status code received from server: 404 (Not Found)"
        );
        assert_eq!(
            go_status_error(&tonic::Status::cancelled("context canceled")),
            "rpc error: code = Canceled desc = context canceled"
        );
    }

    /// End to end over a real diagnostics server: rows per pair for server
    /// info, merged log rows, and a warning for the unreachable node.
    #[test]
    fn fetches_server_info_and_merges_logs_over_grpc() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("tidb.log");
        std::fs::write(
            &log,
            "[2019/08/26 06:19:13.011 -04:00] [INFO] [a.go:1] [\"one\"]\n\
             [2019/08/26 06:19:14.011 -04:00] [WARN] [a.go:2] [\"two\"]\n",
        )
        .unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tidb_sysutil::DiagnosticsServer::new(log.to_string_lossy().into_owned());
        runtime.spawn(
            tonic::transport::Server::builder()
                .add_service(pb::diagnostics_server::DiagnosticsServer::new(server))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = ClusterDiagnosticsClient::new(&ClusterSecurity::plaintext()).unwrap();
        let tidb = ClusterServer {
            server_type: "tidb".to_owned(),
            address: "127.0.0.1:4000".to_owned(),
            status_address: address.clone(),
            version: String::new(),
            git_hash: String::new(),
            start_timestamp: 0,
            server_id: 0,
        };
        let dead = ClusterServer {
            server_type: "tikv".to_owned(),
            address: "127.0.0.1:1".to_owned(),
            status_address: "127.0.0.1:1".to_owned(),
            ..tidb.clone()
        };
        let no_status = ClusterServer {
            server_type: "pd".to_owned(),
            status_address: String::new(),
            ..dead.clone()
        };
        let mut warnings = Vec::new();
        let rows = client.fetch_server_info(
            &[tidb.clone(), dead.clone()],
            pb::ServerInfoType::SystemInfo,
            &mut warnings,
        );
        assert!(!rows.is_empty());
        assert!(rows
            .iter()
            .all(|row| row[0] == Datum::Bytes(b"tidb".to_vec())
                && row[1] == Datum::Bytes(b"127.0.0.1:4000".to_vec())
                && row[2] == Datum::Bytes(b"system".to_vec())));
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].starts_with("rpc error: code = Unavailable"),
            "{warnings:?}"
        );

        let mut warnings = Vec::new();
        // 2019/08/26 06:19:00.000 -04:00 and one minute later.
        let (start, end) = (1_566_814_740_000, 1_566_814_800_000);
        let rows = client.search_log(
            &[no_status, tidb, dead],
            &pb::SearchLogRequest {
                start_time: start,
                end_time: end,
                levels: vec![],
                patterns: vec![".*".to_owned()],
                target: 0,
            },
            &mut warnings,
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Datum::Bytes(b"tidb".to_vec()));
        assert_eq!(rows[0][2], Datum::Bytes(b"127.0.0.1:4000".to_vec()));
        assert_eq!(rows[0][3], Datum::Bytes(b"INFO".to_vec()));
        assert_eq!(rows[1][3], Datum::Bytes(b"WARN".to_vec()));
        assert_eq!(rows[0][4], Datum::Bytes(b"[a.go:1] [\"one\"]".to_vec()));
        assert_eq!(
            rows[0][0],
            Datum::Bytes(format_logging_time(1_566_814_753_011).into_bytes())
        );
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert_eq!(
            warnings[0],
            "pd node 127.0.0.1:1 does not contain status address"
        );
        assert!(
            warnings[1].starts_with("rpc error: code = Unavailable"),
            "{warnings:?}"
        );
    }
}
