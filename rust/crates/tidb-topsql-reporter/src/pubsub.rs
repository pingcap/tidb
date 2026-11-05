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

//! Go `pkg/util/topsql/reporter/pubsub.go`.

use std::sync::{Arc, Weak};
use std::time::Instant;

use prometheus::Histogram;
use tidb_proto::tipb::top_sql_pub_sub_server::TopSqlPubSub;
use tidb_proto::tipb::top_sql_sub_response::RespOneof;
use tidb_proto::tipb::{CollectorType, TopSqlSubRequest, TopSqlSubResponse};
use tidb_topsql_reporter_metrics::{
    IGNORE_REPORT_CHANNEL_FULL_COUNTER, REPORT_ALL_DURATION_FAILED_HISTOGRAM,
    REPORT_ALL_DURATION_SUCC_HISTOGRAM, REPORT_PLAN_DURATION_FAILED_HISTOGRAM,
    REPORT_PLAN_DURATION_SUCC_HISTOGRAM, REPORT_RECORD_DURATION_FAILED_HISTOGRAM,
    REPORT_RECORD_DURATION_SUCC_HISTOGRAM, REPORT_RU_RECORD_DURATION_FAILED_HISTOGRAM,
    REPORT_RU_RECORD_DURATION_SUCC_HISTOGRAM, REPORT_SQL_DURATION_FAILED_HISTOGRAM,
    REPORT_SQL_DURATION_SUCC_HISTOGRAM, TOPSQL_REPORT_PLAN_COUNT_HISTOGRAM,
    TOPSQL_REPORT_RECORD_COUNTER_HISTOGRAM, TOPSQL_REPORT_RU_RECORD_COUNTER_HISTOGRAM,
    TOPSQL_REPORT_SQL_COUNT_HISTOGRAM,
};
use tidb_topsql_state::ItemInterval;
use futures::StreamExt as _;
use tokio::sync::{mpsc, oneshot};

use crate::datasink::{DataSink, DataSinkRegisterer, PubSubSubscription, ReportData};
use crate::reporter::Context;
use crate::single_target::SendTask;

/// The subscriber side of Go's `tipb.TopSQLPubSub_SubscribeServer`.
type SubscribeStream = mpsc::Sender<Result<TopSqlSubResponse, tonic::Status>>;

/// Go `TopSQLPubSubService` implements tipb.TopSQLPubSubServer.
///
/// If a client subscribes to TopSQL records, the TopSQLPubSubService is
/// responsible for registering an associated DataSink to the reporter. Then
/// the DataSink sends data to the client periodically.
pub struct TopSqlPubSubService {
    data_sink_registerer: Arc<dyn DataSinkRegisterer>,
}

impl TopSqlPubSubService {
    /// Go `NewTopSQLPubSubService` creates a new TopSQLPubSubService.
    #[must_use]
    pub fn new(data_sink_registerer: Arc<dyn DataSinkRegisterer>) -> Self {
        Self { data_sink_registerer }
    }
}

#[tonic::async_trait]
impl TopSqlPubSub for TopSqlPubSubService {
    /// Go `Subscribe` registers dataSinks to the reporter and redirects data
    /// received from reporter to subscribers associated with those
    /// dataSinks.
    ///
    /// Go's handler goroutine runs `ds.run()` until the stream ends; here
    /// that loop is a task feeding the response stream.
    async fn subscribe(
        &self,
        request: tonic::Request<TopSqlSubRequest>,
    ) -> Result<tonic::Response<tonic::codegen::BoxStream<TopSqlSubResponse>>, tonic::Status> {
        // Go's gRPC stream buffers one message per Send; so does this one.
        let (stream, responses) = mpsc::channel(1);
        let (ds, send_task_ch) =
            new_pub_sub_data_sink(Some(request.get_ref()), Arc::clone(&self.data_sink_registerer))
                .map_err(tonic::Status::unknown)?;
        let sink: Arc<dyn DataSink> = ds.clone();
        self.data_sink_registerer.register(sink).map_err(tonic::Status::unknown)?;
        let (status, ended) = oneshot::channel();
        tokio::spawn(ds.run(send_task_ch, stream, status));
        // The handler's error is the stream's trailing status, sent after the
        // buffered responses however full they are.
        let ended = futures::stream::once(ended).filter_map(|status| async move { status.ok().map(Err) });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(responses).chain(ended),
        )))
    }
}

/// Go `pubSubDataSink`.
pub struct PubSubDataSink {
    ctx: Arc<Context>,
    send_task_ch: mpsc::Sender<SendTask>,
    /// For deregister.
    registerer: Arc<dyn DataSinkRegisterer>,
    this: Weak<PubSubDataSink>,
    /// TopSQL subscription config.
    enable_top_sql: bool,
    /// TopRU subscription config.
    enable_top_ru: bool,
    item_interval: ItemInterval,
}

/// Go `parseTopSQLSubscription` returns true when the request opts in to
/// TopSQL. Empty collectors default to TopSQL enabled for backward
/// compatibility.
#[must_use]
pub fn parse_top_sql_subscription(req: Option<&TopSqlSubRequest>) -> bool {
    let Some(req) = req else {
        return true;
    };
    req.collectors.is_empty()
        || req.collectors.iter().any(|collector| {
            *collector == CollectorType::Topsql as i32 || *collector == CollectorType::Unspecified as i32
        })
}

/// Go `ErrTopRUConfig` indicates the subscription requests TopRU but omits
/// the Topru config.
pub const ERR_TOP_RU_CONFIG: &str = "topru config is empty";

/// Go `parseTopRUSubscription`.
///
/// # Errors
/// [`ERR_TOP_RU_CONFIG`] when TOPRU is requested without its config.
pub fn parse_top_ru_subscription(req: Option<&TopSqlSubRequest>) -> Result<(bool, ItemInterval), String> {
    let Some(req) = req else {
        return Ok((false, ItemInterval::UNSPECIFIED));
    };
    if !req.collectors.contains(&(CollectorType::Topru as i32)) {
        return Ok((false, ItemInterval::UNSPECIFIED));
    }
    let Some(cfg) = req.topru else {
        return Err(ERR_TOP_RU_CONFIG.to_owned());
    };
    Ok((true, ItemInterval(cfg.item_interval_seconds)))
}

/// Go `newPubSubDataSink` creates a DataSink for PubSub subscription.
///
/// It parses TopSQL/TopRU options from the request and stores them in the
/// sink. Register enables TopRU when the sink has enableTopRU; Deregister
/// disables it when the last such sink is removed. item_interval_seconds
/// controls TopRURecordItem.timestamp_sec (15s/30s/60s). Requests without
/// the TOPRU collector entry keep TopRU disabled for backward compatibility.
/// It returns an error when the request includes TOPRU but omits the Topru
/// config.
///
/// # Errors
/// [`ERR_TOP_RU_CONFIG`].
pub fn new_pub_sub_data_sink(
    req: Option<&TopSqlSubRequest>,
    registerer: Arc<dyn DataSinkRegisterer>,
) -> Result<(Arc<PubSubDataSink>, mpsc::Receiver<SendTask>), String> {
    let enable_top_sql = parse_top_sql_subscription(req);
    let (enable_top_ru, item_interval) = parse_top_ru_subscription(req).inspect_err(|error| {
        tidb_logutil::bg_logger().warn(
            "[top-sql] pubsub datasink failed to parse top-ru config",
            &[tidb_logutil::zap_error(error)],
        );
    })?;
    let (send_task_ch, receiver) = mpsc::channel(1);
    let ds = Arc::new_cyclic(|this| PubSubDataSink {
        ctx: Context::new(),
        send_task_ch,
        registerer,
        this: this.clone(),
        enable_top_sql,
        enable_top_ru,
        item_interval,
    });
    Ok((ds, receiver))
}

impl DataSink for PubSubDataSink {
    fn try_send(&self, data: Arc<ReportData>, deadline: Instant) -> Result<(), String> {
        if self.ctx.is_done() {
            return Err(Context::err());
        }
        match self.send_task_ch.try_send(SendTask { data, deadline }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Context::err()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                IGNORE_REPORT_CHANNEL_FULL_COUNTER.inc();
                Err("the channel of pubsub dataSink is full".to_owned())
            }
        }
    }

    fn on_reporter_closing(&self) {
        self.ctx.cancel();
    }

    fn pub_sub_subscription(&self) -> Option<PubSubSubscription> {
        Some(PubSubSubscription {
            enable_top_sql: self.enable_top_sql,
            enable_top_ru: self.enable_top_ru,
            item_interval: self.item_interval,
        })
    }
}

/// Why a send loop ended (Go's `context.Cause`).
enum SendError {
    /// Go `context.Canceled`: the sink closed while sending.
    Canceled,
    /// The subscriber's stream failed.
    Stream(String),
}

impl PubSubDataSink {
    /// Go `pubSubDataSink.run`: the error it returns ends the subscriber's
    /// stream with Go's status (`status.FromContextError` for context errors).
    async fn run(
        self: Arc<Self>,
        mut send_task_ch: mpsc::Receiver<SendTask>,
        stream: SubscribeStream,
        ended: oneshot::Sender<tonic::Status>,
    ) {
        if let Err(status) = self.run_loop(&mut send_task_ch, &stream).await {
            let _ = ended.send(status);
        }
        if let Some(this) = self.this.upgrade() {
            let sink: Arc<dyn DataSink> = this;
            self.registerer.deregister(&sink);
        }
        self.ctx.cancel();
    }

    async fn run_loop(
        &self,
        send_task_ch: &mut mpsc::Receiver<SendTask>,
        stream: &SubscribeStream,
    ) -> Result<(), tonic::Status> {
        loop {
            tokio::select! {
                task = send_task_ch.recv() => {
                    let Some(task) = task else { return Err(tonic::Status::cancelled(Context::err())) };
                    let start = Instant::now();
                    let deadline = tokio::time::Instant::from_std(task.deadline);
                    let sent = tokio::select! {
                        result = tokio::time::timeout_at(deadline, self.do_send(stream, &task.data)) => result,
                        () = self.ctx.cancelled() => Ok(Err(SendError::Canceled)),
                    };
                    match sent {
                        Err(_) => {
                            REPORT_ALL_DURATION_FAILED_HISTOGRAM.observe(start.elapsed().as_secs_f64());
                            tidb_logutil::bg_logger().warn(
                                "[top-sql] pubsub datasink failed to send data to subscriber due to deadline exceeded",
                                &[tidb_log::Field::new(
                                    "deadline",
                                    tidb_log::Value::Str(format!("{:?}", task.deadline)),
                                )],
                            );
                            return Err(tonic::Status::deadline_exceeded("context deadline exceeded"));
                        }
                        Ok(Ok(())) => REPORT_ALL_DURATION_SUCC_HISTOGRAM.observe(start.elapsed().as_secs_f64()),
                        Ok(Err(SendError::Canceled)) => {
                            REPORT_ALL_DURATION_FAILED_HISTOGRAM.observe(start.elapsed().as_secs_f64());
                        }
                        Ok(Err(SendError::Stream(error))) => {
                            REPORT_ALL_DURATION_FAILED_HISTOGRAM.observe(start.elapsed().as_secs_f64());
                            tidb_logutil::bg_logger().warn(
                                "[top-sql] pubsub datasink failed to send data to subscriber",
                                &[tidb_logutil::zap_error(&error)],
                            );
                            return Err(tonic::Status::unknown(error));
                        }
                    }
                }
                () = self.ctx.cancelled() => return Err(tonic::Status::cancelled(Context::err())),
                // Go's ctx derives from the stream's: a gone subscriber cancels it.
                () = stream.closed() => return Err(tonic::Status::cancelled(Context::err())),
            }
        }
    }

    /// Go `pubSubDataSink.doSend`.
    async fn do_send(&self, stream: &SubscribeStream, data: &ReportData) -> Result<(), SendError> {
        if self.enable_top_sql {
            send(
                stream,
                &self.ctx,
                data.data_records.iter().cloned().map(RespOneof::Record),
                &TOPSQL_REPORT_RECORD_COUNTER_HISTOGRAM,
                (&REPORT_RECORD_DURATION_SUCC_HISTOGRAM, &REPORT_RECORD_DURATION_FAILED_HISTOGRAM),
            )
            .await?;
        }
        // Defense in depth: only send RU records when TopRU is enabled.
        if !data.ru_records.is_empty() && self.enable_top_ru && tidb_topsql_state::top_ru_enabled() {
            send(
                stream,
                &self.ctx,
                data.ru_records.iter().cloned().map(RespOneof::RuRecord),
                &TOPSQL_REPORT_RU_RECORD_COUNTER_HISTOGRAM,
                (&REPORT_RU_RECORD_DURATION_SUCC_HISTOGRAM, &REPORT_RU_RECORD_DURATION_FAILED_HISTOGRAM),
            )
            .await?;
        }
        send(
            stream,
            &self.ctx,
            data.sql_metas.iter().cloned().map(RespOneof::SqlMeta),
            &TOPSQL_REPORT_SQL_COUNT_HISTOGRAM,
            (&REPORT_SQL_DURATION_SUCC_HISTOGRAM, &REPORT_SQL_DURATION_FAILED_HISTOGRAM),
        )
        .await?;
        send(
            stream,
            &self.ctx,
            data.plan_metas.iter().cloned().map(RespOneof::PlanMeta),
            &TOPSQL_REPORT_PLAN_COUNT_HISTOGRAM,
            (&REPORT_PLAN_DURATION_SUCC_HISTOGRAM, &REPORT_PLAN_DURATION_FAILED_HISTOGRAM),
        )
        .await
    }
}

/// Go's `sendTopSQLRecords` / `sendTopRURecords` / `sendSQLMeta` /
/// `sendPlanMeta`: one response per item, observing the count and duration
/// of a non-empty batch.
async fn send(
    stream: &SubscribeStream,
    ctx: &Context,
    items: impl ExactSizeIterator<Item = RespOneof>,
    count: &Histogram,
    (succ, failed): (&Histogram, &Histogram),
) -> Result<(), SendError> {
    if items.len() == 0 {
        return Ok(());
    }
    let start = Instant::now();
    let mut sent_count = 0;
    let mut result = Ok(());
    for item in items {
        if stream.send(Ok(TopSqlSubResponse { resp_oneof: Some(item) })).await.is_err() {
            result = Err(SendError::Stream("transport is closing".to_owned()));
            break;
        }
        sent_count += 1;
        if ctx.is_done() {
            result = Err(SendError::Canceled);
            break;
        }
    }
    count.observe(f64::from(sent_count));
    let elapsed = start.elapsed().as_secs_f64();
    if result.is_ok() { succ.observe(elapsed) } else { failed.observe(elapsed) }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporter::tests::new_reporter;
    use crate::reporter::TopSqlReporter;
    use tidb_proto::tipb::top_sql_pub_sub_client::TopSqlPubSubClient;
    use tidb_proto::tipb::top_sql_pub_sub_server::TopSqlPubSubServer;
    use tidb_proto::tipb::{SqlMeta, TopRuConfig, TopSqlRecord};

    fn request(collectors: &[CollectorType], topru: Option<i32>) -> TopSqlSubRequest {
        TopSqlSubRequest {
            collectors: collectors.iter().map(|c| *c as i32).collect(),
            topru: topru.map(|item_interval_seconds| TopRuConfig { item_interval_seconds }),
        }
    }

    /// Go `TestParseTopSQLSubscription` / `TestParseTopRUSubscription`.
    #[test]
    fn parse_subscriptions() {
        assert!(parse_top_sql_subscription(None));
        assert!(parse_top_sql_subscription(Some(&request(&[], None))));
        assert!(parse_top_sql_subscription(Some(&request(&[CollectorType::Unspecified], None))));
        assert!(!parse_top_sql_subscription(Some(&request(&[CollectorType::Topru], None))));
        assert!(parse_top_sql_subscription(Some(&request(&[CollectorType::Topru, CollectorType::Topsql], None))));

        assert_eq!(parse_top_ru_subscription(None), Ok((false, ItemInterval::UNSPECIFIED)));
        assert_eq!(
            parse_top_ru_subscription(Some(&request(&[CollectorType::Topsql], Some(15)))),
            Ok((false, ItemInterval::UNSPECIFIED))
        );
        assert_eq!(
            parse_top_ru_subscription(Some(&request(&[CollectorType::Topru], Some(30)))),
            Ok((true, ItemInterval::THIRTY_SECONDS))
        );
        assert_eq!(
            parse_top_ru_subscription(Some(&request(&[CollectorType::Topru], None))),
            Err(ERR_TOP_RU_CONFIG.to_owned())
        );
    }

    /// Go `TestPubSubDataSink` over a real gRPC stream: a subscriber turns
    /// Top SQL on, receives each report as records, then SQL metas, then
    /// plan metas, and its going away deregisters the sink.
    #[test]
    fn subscriber_streams_reports_and_deregisters_when_gone() {
        tidb_topsql_state::disable_top_sql();
        let reporter = new_reporter();
        let registerer = Arc::clone(&reporter).data_sink_registerer().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(TopSqlPubSubServer::new(TopSqlPubSubService::new(Arc::clone(&registerer))))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
            );
            let mut client = TopSqlPubSubClient::connect(format!("http://{address}")).await.unwrap();

            // TOPRU without its config fails the subscription.
            let status = client.subscribe(request(&[CollectorType::Topru], None)).await.unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unknown);
            assert_eq!(status.message(), ERR_TOP_RU_CONFIG);

            let mut stream = client.subscribe(request(&[], None)).await.unwrap().into_inner();
            assert!(tidb_topsql_state::top_sql_enabled());
            let data = Arc::new(ReportData {
                data_records: vec![TopSqlRecord { sql_digest: b"S1".to_vec(), ..Default::default() }],
                sql_metas: vec![SqlMeta { sql_digest: b"S1".to_vec(), ..Default::default() }],
                plan_metas: vec![tidb_proto::tipb::PlanMeta { plan_digest: b"P1".to_vec(), ..Default::default() }],
                ..ReportData::default()
            });
            let sinks = reporter.registerer.lock().data_sinks.clone();
            assert_eq!(sinks.len(), 1);
            sinks[0].try_send(data, Instant::now() + std::time::Duration::from_secs(10)).unwrap();
            let mut received = Vec::new();
            for _ in 0..3 {
                received.push(stream.message().await.unwrap().unwrap().resp_oneof.unwrap());
            }
            assert!(matches!(&received[0], RespOneof::Record(r) if r.sql_digest == b"S1"));
            assert!(matches!(&received[1], RespOneof::SqlMeta(m) if m.sql_digest == b"S1"));
            assert!(matches!(&received[2], RespOneof::PlanMeta(m) if m.plan_digest == b"P1"));

            drop(stream);
            for _ in 0..100 {
                if !tidb_topsql_state::top_sql_enabled() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(!tidb_topsql_state::top_sql_enabled());
            assert!(reporter.registerer.lock().data_sinks.is_empty());
        });
        reporter.close();
    }

    /// Go's deadline path: a send that cannot finish by the task's deadline
    /// ends the subscription with DeadlineExceeded.
    #[test]
    fn a_send_past_its_deadline_ends_the_subscription() {
        let reporter = new_reporter();
        let registerer = Arc::clone(&reporter).data_sink_registerer().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        runtime.block_on(async {
            let (ds, send_task_ch) = new_pub_sub_data_sink(None, Arc::clone(&registerer)).unwrap();
            registerer.register(ds.clone()).unwrap();
            // Nobody reads the stream: the second response blocks.
            let (stream, _responses) = mpsc::channel(1);
            let (status, ended) = oneshot::channel();
            let run = tokio::spawn(ds.clone().run(send_task_ch, stream, status));
            let records = (0..3).map(|_| TopSqlRecord::default()).collect();
            let data = Arc::new(ReportData { data_records: records, ..ReportData::default() });
            ds.try_send(data, Instant::now() + std::time::Duration::from_millis(200)).unwrap();
            run.await.unwrap();
            let status = ended.await.unwrap();
            assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
            assert!(reporter.registerer.lock().data_sinks.is_empty());
            assert!(ds.try_send(Arc::default(), Instant::now()).is_err());
        });
        reporter.close();
    }
}
