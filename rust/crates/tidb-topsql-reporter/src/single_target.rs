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

//! Go `pkg/util/topsql/reporter/single_target.go`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use prometheus::Histogram;
use tidb_proto::tipb::top_sql_agent_client::TopSqlAgentClient;
use tidb_proto::tipb::TopRuRecord;
use tidb_topsql_reporter_metrics::{
    IGNORE_REPORT_CHANNEL_FULL_COUNTER, REPORT_ALL_DURATION_FAILED_HISTOGRAM,
    REPORT_ALL_DURATION_SUCC_HISTOGRAM, REPORT_PLAN_DURATION_FAILED_HISTOGRAM,
    REPORT_PLAN_DURATION_SUCC_HISTOGRAM, REPORT_RECORD_DURATION_FAILED_HISTOGRAM,
    REPORT_RECORD_DURATION_SUCC_HISTOGRAM, REPORT_SQL_DURATION_FAILED_HISTOGRAM,
    REPORT_SQL_DURATION_SUCC_HISTOGRAM, TOPSQL_REPORT_PLAN_COUNT_HISTOGRAM,
    TOPSQL_REPORT_RECORD_COUNTER_HISTOGRAM, TOPSQL_REPORT_SQL_COUNT_HISTOGRAM,
};
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

use crate::datasink::{DataSink, DataSinkRegisterer, ReportData};
use crate::reporter::{category, Context};

/// Go `dialTimeout`.
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
/// Go `grpcInitialWindowSize`.
const GRPC_INITIAL_WINDOW_SIZE: u32 = 1 << 30;
/// Go `grpcInitialConnWindowSize`.
const GRPC_INITIAL_CONN_WINDOW_SIZE: u32 = 1 << 30;

/// Go `sendTask`.
pub struct SendTask {
    /// Go `sendTask.data`.
    pub data: Arc<ReportData>,
    /// Go `sendTask.deadline`.
    pub deadline: Instant,
}

/// Go `config.GetGlobalConfig().TopSQL.ReceiverAddress`.
fn receiver_address() -> String {
    tidb_config::config_tree::config::get_global_config().top_sql.receiver_address.clone()
}

/// Go `SingleTargetDataSink` reports data to grpc servers.
pub struct SingleTargetDataSink {
    ctx: Arc<Context>,
    registerer: Arc<dyn DataSinkRegisterer>,
    this: Weak<SingleTargetDataSink>,
    conn: Mutex<Option<Channel>>,
    send_task_ch: mpsc::Sender<SendTask>,
    send_task_rx: Mutex<Option<mpsc::Receiver<SendTask>>>,
    registered: AtomicBool,
    cur_rpc_addr: Mutex<String>,
}

impl SingleTargetDataSink {
    /// Go `NewSingleTargetDataSink` returns a new SingleTargetDataSink.
    #[must_use]
    pub fn new(registerer: Arc<dyn DataSinkRegisterer>) -> Arc<Self> {
        let (send_task_ch, send_task_rx) = mpsc::channel(1);
        Arc::new_cyclic(|this| Self {
            ctx: Context::new(),
            registerer,
            this: this.clone(),
            conn: Mutex::new(None),
            send_task_ch,
            send_task_rx: Mutex::new(Some(send_task_rx)),
            registered: AtomicBool::new(false),
            cur_rpc_addr: Mutex::default(),
        })
    }

    fn as_sink(&self) -> Option<Arc<dyn DataSink>> {
        self.this.upgrade().map(|this| this as Arc<dyn DataSink>)
    }

    /// Go `Start` starts to run SingleTargetDataSink.
    pub fn start(self: &Arc<Self>) {
        let addr = receiver_address();
        if !addr.is_empty() {
            *self.lock_cur_rpc_addr() = addr;
            match self.registerer.register(Arc::clone(self) as Arc<dyn DataSink>) {
                Ok(()) => self.registered.store(true, Ordering::SeqCst),
                Err(error) => tidb_logutil::bg_logger()
                    .warn("failed to register single target datasink", &[tidb_logutil::zap_error(error)]),
            }
        }
        let Some(send_task_rx) = self.send_task_rx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
        else {
            return;
        };
        let ds = Arc::clone(self);
        let _ = std::thread::Builder::new()
            .name("topsql-single-target".to_owned())
            .spawn(move || ds.recover_run(send_task_rx));
    }

    fn lock_cur_rpc_addr(&self) -> std::sync::MutexGuard<'_, String> {
        self.cur_rpc_addr.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_conn(&self) -> std::sync::MutexGuard<'_, Option<Channel>> {
        self.conn.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `recoverRun` will run until SingleTargetDataSink is closed.
    fn recover_run(self: Arc<Self>, send_task_rx: mpsc::Receiver<SendTask>) {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
            return;
        };
        let send_task_rx = Arc::new(tokio::sync::Mutex::new(send_task_rx));
        runtime.block_on(async {
            loop {
                let (ds, rx) = (Arc::clone(&self), Arc::clone(&send_task_rx));
                match tokio::spawn(async move { ds.run(&mut *rx.lock().await).await }).await {
                    Ok(()) => break,
                    Err(error) => tidb_logutil::bg_logger().error(
                        "panic in SingleTargetDataSink, rerun",
                        &[
                            tidb_log::Field::new("r", tidb_log::Value::Str(error.to_string())),
                            tidb_logutil::zap_stack(),
                        ],
                    ),
                }
            }
        });
        // Go drops the connection when the run ends.
        self.lock_conn().take();
    }

    /// Go `SingleTargetDataSink.run`.
    async fn run(&self, send_task_rx: &mut mpsc::Receiver<SendTask>) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        loop {
            let target_rpc_addr;
            tokio::select! {
                () = self.ctx.cancelled() => return,
                task = send_task_rx.recv() => {
                    let Some(task) = task else { return };
                    target_rpc_addr = receiver_address();
                    self.do_send(&target_rpc_addr, task).await;
                }
                _ = ticker.tick() => target_rpc_addr = receiver_address(),
            }
            if self.try_switch_registration(&target_rpc_addr).is_err() {
                return;
            }
        }
    }

    /// Go `trySwitchRegistration`.
    fn try_switch_registration(&self, addr: &str) -> Result<(), String> {
        let Some(sink) = self.as_sink() else {
            return Ok(());
        };
        // deregister if `addr` is empty and registered before
        if addr.is_empty() && self.registered.load(Ordering::SeqCst) {
            self.registerer.deregister(&sink);
            self.registered.store(false, Ordering::SeqCst);
            return Ok(());
        }
        // register if `addr` is not empty and not registered before
        if !addr.is_empty() && !self.registered.load(Ordering::SeqCst) {
            if let Err(error) = self.registerer.register(sink) {
                tidb_logutil::bg_logger().warn(
                    "failed to register the single target datasink",
                    &[tidb_logutil::zap_error(&error)],
                );
                return Err(error);
            }
            self.registered.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Go `Close` uses to close grpc connection.
    pub fn close(&self) {
        self.ctx.cancel();
        if self.registered.load(Ordering::SeqCst) {
            if let Some(sink) = self.as_sink() {
                self.registerer.deregister(&sink);
            }
            self.registered.store(false, Ordering::SeqCst);
        }
    }

    /// Go `SingleTargetDataSink.doSend`.
    async fn do_send(&self, addr: &str, task: SendTask) {
        if addr.is_empty() {
            return;
        }
        let start = Instant::now();
        let deadline = tokio::time::Instant::from_std(task.deadline);
        let result = match tokio::time::timeout_at(deadline, self.send_all(addr, &task.data)).await {
            Ok(result) => result,
            Err(_) => Err("context deadline exceeded".to_owned()),
        };
        match result {
            Ok(()) => REPORT_ALL_DURATION_SUCC_HISTOGRAM.observe(start.elapsed().as_secs_f64()),
            Err(error) => {
                tidb_logutil::bg_logger().warn(
                    "single target data sink failed to send data to receiver",
                    &[category(), tidb_logutil::zap_error(error)],
                );
                REPORT_ALL_DURATION_FAILED_HISTOGRAM.observe(start.elapsed().as_secs_f64());
            }
        }
    }

    /// The body of Go's `doSend` under its deadline: connect, then the four
    /// batches concurrently; the first error in Go's channel order wins.
    async fn send_all(&self, addr: &str, data: &ReportData) -> Result<(), String> {
        let conn = self.try_establish_connection(addr).await?;
        let client = TopSqlAgentClient::new(conn).max_decoding_message_size(usize::MAX);
        let (sql, plan, records, ru) = tokio::join!(
            send_batch(
                client.clone(),
                data.sql_metas.clone(),
                &TOPSQL_REPORT_SQL_COUNT_HISTOGRAM,
                (&REPORT_SQL_DURATION_SUCC_HISTOGRAM, &REPORT_SQL_DURATION_FAILED_HISTOGRAM),
                |mut client, stream| async move { client.report_sql_meta(stream).await },
            ),
            send_batch(
                client.clone(),
                data.plan_metas.clone(),
                &TOPSQL_REPORT_PLAN_COUNT_HISTOGRAM,
                (&REPORT_PLAN_DURATION_SUCC_HISTOGRAM, &REPORT_PLAN_DURATION_FAILED_HISTOGRAM),
                |mut client, stream| async move { client.report_plan_meta(stream).await },
            ),
            send_batch(
                client.clone(),
                data.data_records.clone(),
                &TOPSQL_REPORT_RECORD_COUNTER_HISTOGRAM,
                (&REPORT_RECORD_DURATION_SUCC_HISTOGRAM, &REPORT_RECORD_DURATION_FAILED_HISTOGRAM),
                |mut client, stream| async move { client.report_top_sql_records(stream).await },
            ),
            send_batch_top_ru_record(&data.ru_records),
        );
        sql.and(plan).and(records).and(ru)
    }

    /// Go `tryEstablishConnection` establishes the gRPC connection if
    /// connection is not established.
    async fn try_establish_connection(&self, target_rpc_addr: &str) -> Result<Channel, String> {
        if *self.lock_cur_rpc_addr() == target_rpc_addr {
            if let Some(conn) = self.lock_conn().clone() {
                // Address is not changed, skip.
                return Ok(conn);
            }
        }
        if self.lock_conn().take().is_some() {
            // Go logs its `conn.Close()` result, nil included.
            tidb_logutil::bg_logger().warn(
                "grpc dataSink close connection failed",
                &[category(), tidb_logutil::zap_error("<nil>")],
            );
        }
        let conn = dial(target_rpc_addr).await?;
        *self.lock_conn() = Some(conn.clone());
        target_rpc_addr.clone_into(&mut self.lock_cur_rpc_addr());
        Ok(conn)
    }
}

/// Go `SingleTargetDataSink.dial`.
async fn dial(target_rpc_addr: &str) -> Result<Channel, String> {
    let endpoint = Endpoint::from_shared(format!("http://{target_rpc_addr}"))
        .map_err(|error| error.to_string())?
        .connect_timeout(DIAL_TIMEOUT)
        .initial_stream_window_size(GRPC_INITIAL_WINDOW_SIZE)
        .initial_connection_window_size(GRPC_INITIAL_CONN_WINDOW_SIZE);
    // Go `grpc.WithBlock()` under a `dialTimeout` context.
    match tokio::time::timeout(DIAL_TIMEOUT, endpoint.connect()).await {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(_) => Err("context deadline exceeded".to_owned()),
    }
}

/// Go `sendBatchSQLMeta` / `sendBatchPlanMeta` / `sendBatchTopSQLRecord`:
/// a non-empty batch is one client stream, counted per message sent.
async fn send_batch<T, F, Fut>(
    client: TopSqlAgentClient<Channel>,
    items: Vec<T>,
    count: &Histogram,
    (succ, failed): (&Histogram, &Histogram),
    report: F,
) -> Result<(), String>
where
    T: Send + 'static,
    F: FnOnce(TopSqlAgentClient<Channel>, tokio_stream::Iter<std::vec::IntoIter<T>>) -> Fut,
    Fut: std::future::Future<Output = Result<tonic::Response<tidb_proto::tipb::EmptyResponse>, tonic::Status>>,
{
    if items.is_empty() {
        return Ok(());
    }
    let total = items.len();
    let start = Instant::now();
    let result = report(client, tokio_stream::iter(items)).await;
    // The client stream sends every item before the server's reply.
    let sent_count = if result.is_ok() { total } else { 0 };
    count.observe(sent_count as f64);
    let elapsed = start.elapsed().as_secs_f64();
    match result {
        Ok(_) => {
            succ.observe(elapsed);
            Ok(())
        }
        Err(status) => {
            failed.observe(elapsed);
            Err(status.to_string())
        }
    }
}

/// Go `sendBatchTopRURecord`: TopRU over SingleTarget is intentionally
/// unsupported now.
async fn send_batch_top_ru_record(_records: &[TopRuRecord]) -> Result<(), String> {
    Ok(())
}

impl DataSink for SingleTargetDataSink {
    /// The gRPC connection will be established (or reused) in doSend via
    /// tryEstablishConnection. This is suitable for a per-minute sending
    /// period.
    fn try_send(&self, data: Arc<ReportData>, deadline: Instant) -> Result<(), String> {
        if self.ctx.is_done() {
            return Err(Context::err());
        }
        match self.send_task_ch.try_send(SendTask { data, deadline }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Context::err()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                IGNORE_REPORT_CHANNEL_FULL_COUNTER.inc();
                Err("the channel of single target dataSink is full".to_owned())
            }
        }
    }

    fn on_reporter_closing(&self) {
        self.ctx.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporter::tests::new_reporter;
    use crate::reporter::TopSqlReporter;
    use tidb_proto::tipb::top_sql_agent_server::{TopSqlAgent, TopSqlAgentServer};
    use tidb_proto::tipb::{EmptyResponse, PlanMeta, SqlMeta, TopSqlRecord};

    /// Go `mockAgentServer`: keeps what each report stream carried.
    #[derive(Default)]
    struct MockAgent {
        received: Arc<Mutex<Vec<String>>>,
    }

    async fn drain<T: prost::Message + Default>(
        received: &Mutex<Vec<String>>,
        kind: &str,
        mut stream: tonic::Streaming<T>,
        key: impl Fn(&T) -> Vec<u8>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        while let Some(item) = stream.message().await? {
            received
                .lock()
                .unwrap()
                .push(format!("{kind}:{}", String::from_utf8_lossy(&key(&item))));
        }
        Ok(tonic::Response::new(EmptyResponse {}))
    }

    #[tonic::async_trait]
    impl TopSqlAgent for MockAgent {
        async fn report_top_sql_records(
            &self,
            request: tonic::Request<tonic::Streaming<TopSqlRecord>>,
        ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
            drain(&self.received, "record", request.into_inner(), |r| r.sql_digest.clone()).await
        }

        async fn report_sql_meta(
            &self,
            request: tonic::Request<tonic::Streaming<SqlMeta>>,
        ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
            drain(&self.received, "sql", request.into_inner(), |m| m.sql_digest.clone()).await
        }

        async fn report_plan_meta(
            &self,
            request: tonic::Request<tonic::Streaming<PlanMeta>>,
        ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
            drain(&self.received, "plan", request.into_inner(), |m| m.plan_digest.clone()).await
        }
    }

    /// Go `TestSingleTargetDataSink`: with `top-sql.receiver-address` set the
    /// sink registers (turning Top SQL on), pushes each report to the agent,
    /// and deregisters once the address is cleared.
    #[test]
    fn single_target_data_sink_reports_to_the_receiver_address() {
        tidb_topsql_state::disable_top_sql();
        let agent = MockAgent::default();
        let received = Arc::clone(&agent.received);
        let (ready, address) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready.send(listener.local_addr().unwrap()).unwrap();
                tonic::transport::Server::builder()
                    .add_service(TopSqlAgentServer::new(agent))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            });
        });
        let address = address.recv().unwrap();
        tidb_config::config_tree::config::update_global(|config| {
            config.top_sql.receiver_address = address.to_string();
        });

        let reporter = new_reporter();
        let ds = SingleTargetDataSink::new(Arc::clone(&reporter).data_sink_registerer().unwrap());
        ds.start();
        assert!(tidb_topsql_state::top_sql_enabled());
        let data = Arc::new(ReportData {
            data_records: vec![TopSqlRecord { sql_digest: b"S1".to_vec(), ..Default::default() }],
            sql_metas: vec![SqlMeta { sql_digest: b"S1".to_vec(), ..Default::default() }],
            plan_metas: vec![PlanMeta { plan_digest: b"P1".to_vec(), ..Default::default() }],
            ..ReportData::default()
        });
        ds.try_send(data, Instant::now() + Duration::from_secs(10)).unwrap();
        let mut got = Vec::new();
        for _ in 0..250 {
            got = received.lock().unwrap().clone();
            if got.len() == 3 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        got.sort();
        assert_eq!(got, ["plan:P1", "record:S1", "sql:S1"]);

        tidb_config::config_tree::config::update_global(|config| config.top_sql.receiver_address.clear());
        for _ in 0..150 {
            if !tidb_topsql_state::top_sql_enabled() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(!tidb_topsql_state::top_sql_enabled());
        ds.close();
        reporter.close();
    }
}
