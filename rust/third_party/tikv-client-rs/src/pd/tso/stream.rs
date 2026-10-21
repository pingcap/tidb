// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `clients/tso/stream.go`: the TSO stream builders and the
//! `tsoStream` whose `processRequests` hands a batch to the wire without
//! waiting and whose `recvLoop` pairs each response with the oldest pending
//! batch, in order.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::transport::Channel;

use super::dup_error;
use crate::async_util::Cancellation;
use crate::pd::errs;
use crate::proto::{pdpb, tsopb};
use crate::{Error, Result};

/// Go `tsoRequestResult`. `suffix_bits` travels with the timestamp so the
/// allocated `pdpb::Timestamp`s keep it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TsoRequestResult {
    pub(crate) physical: i64,
    pub(crate) logical: i64,
    pub(crate) count: u32,
    pub(crate) resp_keyspace_group_id: u32,
    pub(crate) suffix_bits: u32,
}

/// Go `onFinishedCallback`: invoked exactly once per batch, with the result
/// or the error that ended it.
pub(crate) type OnFinishedCallback =
    Box<dyn FnOnce(TsoRequestResult, u32, Option<Error>) + Send + 'static>;

struct BatchedRequests {
    start_time: Instant,
    count: i64,
    req_keyspace_group_id: u32,
    callback: OnFinishedCallback,
}

/// The wire-neutral request the stream's sender hands to its adapter (Go
/// `grpcTSOStreamAdapter.Send`'s arguments).
#[derive(Clone, Copy, Debug)]
struct WireRequest {
    cluster_id: u64,
    keyspace_id: u32,
    keyspace_group_id: u32,
    count: i64,
}

/// Go `tsoStreamBuilderFactory`: PD's `pdpb.PD/Tso` or the microservice's
/// `tsopb.TSO/Tso`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamBuilderFactory {
    /// Go `PDStreamBuilderFactory`.
    Pd,
    /// Go `MSStreamBuilderFactory`.
    Ms,
}

impl StreamBuilderFactory {
    /// Go `makeBuilder(cc)`; `server_url` is `cc.Target()`.
    pub(crate) fn make_builder(self, channel: Channel, server_url: &str) -> TsoStreamBuilder {
        TsoStreamBuilder {
            factory: self,
            channel,
            server_url: server_url.to_owned(),
        }
    }
}

/// Go `pdStreamBuilder` / `msStreamBuilder`.
#[derive(Clone)]
pub(crate) struct TsoStreamBuilder {
    factory: StreamBuilderFactory,
    channel: Channel,
    server_url: String,
}

impl TsoStreamBuilder {
    /// Go `build(ctx, cancel, timeout)`. `forwarded_host` is the
    /// `grpcutil.BuildForwardContext` metadata a follower stream carries.
    ///
    /// grpc-go's `client.Tso(ctx)` returns once the connection's transport is
    /// ready, before any response header: it fails fast with `Unavailable`
    /// on an unreachable server, and `checkStreamTimeout` cancels a transport
    /// that does not become ready within `timeout` (`Canceled`). tonic only
    /// resolves a streaming call when the server's headers arrive -- PD
    /// sends them with its first response -- and its channel reports ready
    /// before connecting. So the transport is established here by one
    /// round trip on the same channel, the gRPC health check every PD and
    /// TSO server serves (any answer means the transport is ready), and the
    /// call itself is opened by the stream's receive loop.
    pub(crate) async fn build(
        &self,
        ctx: &Cancellation,
        forwarded_host: Option<String>,
        timeout: Duration,
    ) -> Result<Arc<TsoStream>> {
        let transport_ready = async {
            match crate::pd::service_discovery::health_check(self.channel.clone(), timeout).await {
                // The server answered: whatever its health, the transport is up.
                Ok(_) => Ok(()),
                // The probe's own deadline (tonic reports an expired request
                // timeout as Cancelled).
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
                    ) =>
                {
                    Err(None)
                }
                Err(status) if status.code() == tonic::Code::Unavailable => Err(Some(status)),
                Err(_) => Ok(()),
            }
        };
        let cancelled = tonic::Status::cancelled("context canceled");
        tokio::select! {
            result = tokio::time::timeout(timeout, transport_ready) => match result {
                Ok(Ok(())) => {}
                Ok(Err(Some(status))) => return Err(Error::GrpcAPI(status)),
                Ok(Err(None)) | Err(_) => {
                    // checkStreamTimeout: `cancel()`, then the call fails
                    // with the ctx's error.
                    ctx.cancel();
                    return Err(Error::GrpcAPI(cancelled));
                }
            },
            _ = ctx.cancelled() => return Err(Error::GrpcAPI(cancelled)),
        }
        Ok(TsoStream::new(
            ctx,
            self.server_url.clone(),
            self.factory,
            self.channel.clone(),
            forwarded_host,
        ))
    }
}

const STREAM_STATE_IDLE: i32 = 0;
const STREAM_STATE_SENDING: i32 = 1;
const STREAM_STATE_CLOSING: i32 = 2;

/// Go `invalidStreamID`.
pub(crate) const INVALID_STREAM_ID: &str = "<invalid>";
/// Go `maxPendingRequestsInTSOStream`.
pub(crate) const MAX_PENDING_REQUESTS_IN_TSO_STREAM: usize = 64;

static STREAM_ID_ALLOC: AtomicI32 = AtomicI32::new(0);

/// Go `tsoStream`.
pub(crate) struct TsoStream {
    server_url: String,
    /// Go `streamID`: `<serverURL>-<n>`.
    pub(crate) stream_id: String,
    outbound: mpsc::UnboundedSender<WireRequest>,
    pending_requests: mpsc::Sender<BatchedRequests>,
    /// The stream's own ctx, distinct from its connection ctx (Go derives one
    /// so the dispatcher's error handling can tell them apart).
    cancel: Cancellation,
    state: AtomicI32,
    stopped_with_err: Mutex<Option<Error>>,
    estimated_latency_micros: AtomicU64,
    ongoing_requests: AtomicI32,
    recv_loop: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl TsoStream {
    fn new(
        ctx: &Cancellation,
        server_url: String,
        factory: StreamBuilderFactory,
        channel: Channel,
        forwarded_host: Option<String>,
    ) -> Arc<Self> {
        let stream_id = format!(
            "{}-{}",
            server_url,
            STREAM_ID_ALLOC.fetch_add(1, Ordering::Relaxed) + 1
        );
        let (outbound, outbound_rx) = mpsc::unbounded_channel();
        let (pending_requests, pending_rx) = mpsc::channel(MAX_PENDING_REQUESTS_IN_TSO_STREAM);
        let stream = Arc::new(Self {
            server_url: server_url.clone(),
            stream_id,
            outbound,
            pending_requests,
            cancel: ctx.child(),
            state: AtomicI32::new(STREAM_STATE_IDLE),
            stopped_with_err: Mutex::new(None),
            estimated_latency_micros: AtomicU64::new(0),
            ongoing_requests: AtomicI32::new(0),
            recv_loop: Mutex::new(None),
        });
        let responses = open_responses(
            factory,
            channel,
            server_url,
            forwarded_host,
            outbound_rx,
            stream.cancel.clone(),
        );
        let handle = tokio::spawn(recv_loop(Arc::clone(&stream), responses, pending_rx));
        *stream.recv_loop.lock().unwrap() = Some(handle);
        stream
    }

    /// Go `getServerURL`.
    pub(crate) fn server_url(&self) -> &str {
        &self.server_url
    }

    /// Go `processRequests`: put the batch in `pendingRequests` and send it,
    /// without waiting for the response. Not concurrency-safe, as in Go. When
    /// the batch cannot be scheduled the callback comes back with the error
    /// and the caller finishes the batch itself (Go "the callback will be
    /// ignored").
    pub(crate) fn process_requests(
        &self,
        cluster_id: u64,
        keyspace_id: u32,
        keyspace_group_id: u32,
        count: i64,
        batch_start_time: Option<Instant>,
        callback: OnFinishedCallback,
    ) -> std::result::Result<(), (Error, OnFinishedCallback)> {
        let start = Instant::now();
        let prev_state = self.state.swap(STREAM_STATE_SENDING, Ordering::AcqRel);
        match prev_state {
            STREAM_STATE_IDLE => {}
            STREAM_STATE_CLOSING => {
                self.state.store(prev_state, Ordering::Release);
                let err = self.get_recv_error();
                log::info!(
                    stream = self.stream_id.as_str(),
                    error:% = err.as_ref().map_or_else(|| "<nil>".to_owned(), ToString::to_string);
                    "sending to closed tsoStream"
                );
                let err = err.unwrap_or_else(|| {
                    errs::ERR_CLIENT_TSO_STREAM_CLOSED
                        .error()
                        .with_stack()
                        .into()
                });
                return Err((err, callback));
            }
            STREAM_STATE_SENDING => {
                log::error!(stream = self.stream_id.as_str(); "unexpected concurrent sending on tsoStream");
                panic!("unexpected concurrent sending on tsoStream");
            }
            state => {
                log::error!(stream = self.stream_id.as_str(), state = state; "unknown tsoStream state");
                panic!("unknown tsoStream state");
            }
        }
        if let Err(error) = self.pending_requests.try_send(BatchedRequests {
            start_time: start,
            count,
            req_keyspace_group_id: keyspace_group_id,
            callback,
        }) {
            self.state.store(prev_state, Ordering::Release);
            let callback = match error {
                mpsc::error::TrySendError::Full(batch)
                | mpsc::error::TrySendError::Closed(batch) => batch.callback,
            };
            return Err((
                Error::StringError("unexpected channel full".to_owned()),
                callback,
            ));
        }
        self.state.store(prev_state, Ordering::Release);

        if let Err(error) = self.outbound.send(WireRequest {
            cluster_id,
            keyspace_id,
            keyspace_group_id,
            count,
        }) {
            // The batch is already pending; the receive loop cancels it.
            log::warn!(
                stream = self.stream_id.as_str(),
                error:% = error;
                "failed to send RPC request through tsoStream"
            );
            return Ok(());
        }
        let metrics = crate::pd::metrics::global_metrics();
        if let Some(batch_start_time) = batch_start_time {
            metrics
                .tso_batch_send_latency
                .observe(batch_start_time.elapsed().as_secs_f64());
        }
        let ongoing = self.ongoing_requests.fetch_add(1, Ordering::AcqRel) + 1;
        metrics
            .ongoing_request_count_gauge
            .with_label_values(&[&self.stream_id])
            .set(f64::from(ongoing));
        Ok(())
    }

    /// Go `EstimatedRPCLatency`: at least 100us.
    pub(crate) fn estimated_rpc_latency(&self) -> Duration {
        let micros = self
            .estimated_latency_micros
            .load(Ordering::Acquire)
            .max(100);
        Duration::from_micros(micros)
    }

    /// Go `GetRecvError`.
    pub(crate) fn get_recv_error(&self) -> Option<Error> {
        self.stopped_with_err
            .lock()
            .unwrap()
            .as_ref()
            .map(dup_error)
    }

    /// Go `WaitForClosed`.
    pub(crate) async fn wait_for_closed(&self) {
        let handle = self.recv_loop.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }

    /// The stream's internal ctx, cancelled when its receive loop ends.
    pub(crate) fn ctx(&self) -> &Cancellation {
        &self.cancel
    }
}

type Responses =
    futures::stream::BoxStream<'static, std::result::Result<TsoRequestResult, tonic::Status>>;

/// Opens the call and adapts both protocols (Go `pdTSOStreamAdapter` /
/// `tsoTSOStreamAdapter`). The call resolves inside the receive loop.
fn open_responses(
    factory: StreamBuilderFactory,
    channel: Channel,
    server_url: String,
    forwarded_host: Option<String>,
    outbound: mpsc::UnboundedReceiver<WireRequest>,
    cancel: Cancellation,
) -> futures::future::BoxFuture<'static, std::result::Result<Responses, tonic::Status>> {
    // The request half ends with the stream's ctx, so dropping the response
    // half then resets the call, as cancelling a grpc-go stream's ctx does.
    let requests = futures::stream::unfold(outbound, |mut outbound| async move {
        outbound.recv().await.map(|request| (request, outbound))
    })
    .take_until(Box::pin(async move { cancel.cancelled().await }));
    let interceptor = crate::pd::region_service::Forwarding(forwarded_host);
    match factory {
        StreamBuilderFactory::Pd => Box::pin(async move {
            let requests = requests.map(|request: WireRequest| pdpb::TsoRequest {
                header: Some(pdpb::RequestHeader {
                    cluster_id: request.cluster_id,
                    ..Default::default()
                }),
                count: request.count as u32,
                dc_location: String::new(),
            });
            let responses = pdpb::pd_client::PdClient::with_interceptor(channel, interceptor)
                .tso(requests)
                .await?
                .into_inner();
            Ok(responses
                .map(|response| {
                    let response = response?;
                    let timestamp = response.timestamp.unwrap_or_default();
                    Ok(TsoRequestResult {
                        physical: timestamp.physical,
                        logical: timestamp.logical,
                        count: response.count,
                        resp_keyspace_group_id: super::DEFAULT_KEYSPACE_GROUP_ID,
                        suffix_bits: timestamp.suffix_bits,
                    })
                })
                .boxed())
        }),
        StreamBuilderFactory::Ms => Box::pin(async move {
            let callee_id = crate::pd::service_discovery::callee_id(&server_url);
            let requests = requests.map(move |request: WireRequest| tsopb::TsoRequest {
                header: Some(tsopb::RequestHeader {
                    cluster_id: request.cluster_id,
                    keyspace: Some(tsopb::request_header::Keyspace::KeyspaceId(
                        request.keyspace_id,
                    )),
                    keyspace_group_id: request.keyspace_group_id,
                    callee_id: callee_id.clone(),
                    ..Default::default()
                }),
                count: request.count as u32,
                dc_location: String::new(),
            });
            let responses = tsopb::tso_client::TsoClient::with_interceptor(channel, interceptor)
                .tso(requests)
                .await?
                .into_inner();
            Ok(responses
                .map(|response| {
                    let response = response?;
                    let timestamp = response.timestamp.unwrap_or_default();
                    Ok(TsoRequestResult {
                        physical: timestamp.physical,
                        logical: timestamp.logical,
                        count: response.count,
                        resp_keyspace_group_id: response
                            .header
                            .map_or(0, |header| header.keyspace_group_id),
                        suffix_bits: timestamp.suffix_bits,
                    })
                })
                .boxed())
        }),
    }
}

/// Go `recvLoop`.
async fn recv_loop(
    stream: Arc<TsoStream>,
    open: futures::future::BoxFuture<'static, std::result::Result<Responses, tonic::Status>>,
    mut pending_requests: mpsc::Receiver<BatchedRequests>,
) {
    let metrics = crate::pd::metrics::global_metrics();
    let mut filter = RcFilter::new(FILTER_CUTOFF_FREQ, FILTER_NEW_SAMPLE_WEIGHT_UPPER_BOUND);
    let mut current_req: Option<BatchedRequests> = None;
    let finish_with_err: Error = 'recv: {
        let mut responses = tokio::select! {
            opened = open => match opened {
                Ok(responses) => responses,
                Err(status) => break 'recv Error::GrpcAPI(status),
            },
            _ = stream.cancel.cancelled() => break 'recv Error::ContextCanceled,
        };
        loop {
            if stream.cancel.is_cancelled() {
                break 'recv Error::ContextCanceled;
            }
            let received = tokio::select! {
                received = responses.next() => received,
                _ = stream.cancel.cancelled() => break 'recv Error::ContextCanceled,
            };
            // A successful Recv implies a pending batch.
            current_req = pending_requests.try_recv().ok();
            let latency = current_req
                .as_ref()
                .map_or(Duration::ZERO, |request| request.start_time.elapsed());
            let result = match received {
                None => {
                    if current_req.is_some() {
                        metrics
                            .request_failed_duration_tso
                            .observe(latency.as_secs_f64());
                    }
                    // Go maps io.EOF to ErrClientTSOStreamClosed.
                    break 'recv errs::ERR_CLIENT_TSO_STREAM_CLOSED
                        .error()
                        .with_stack()
                        .into();
                }
                Some(Err(status)) => {
                    if current_req.is_some() {
                        metrics
                            .request_failed_duration_tso
                            .observe(latency.as_secs_f64());
                    }
                    break 'recv Error::GrpcAPI(status);
                }
                Some(Ok(result)) => result,
            };
            let Some(request) = current_req.take() else {
                break 'recv Error::StringError("tsoStream timing order broken".to_owned());
            };
            metrics.request_duration_tso.observe(latency.as_secs_f64());
            metrics.tso_batch_size.observe(f64::from(result.count));
            // Go `updateEstimatedLatency`, over log(latency in micros).
            let micros = latency.as_micros().max(1) as f64;
            let filtered = filter.update(request.start_time, micros.ln());
            let estimated = filtered.exp();
            stream
                .estimated_latency_micros
                .store(estimated as u64, Ordering::Release);
            metrics
                .estimate_tso_latency_gauge
                .with_label_values(&[&stream.stream_id])
                .set(estimated * 1e-6);
            if i64::from(result.count) != request.count {
                current_req = Some(request);
                break 'recv errs::ERR_TSO_LENGTH.error().with_stack().into();
            }
            (request.callback)(result, request.req_keyspace_group_id, None);
            let ongoing = stream.ongoing_requests.fetch_sub(1, Ordering::AcqRel) - 1;
            metrics
                .ongoing_request_count_gauge
                .with_label_values(&[&stream.stream_id])
                .set(f64::from(ongoing));
        }
    };

    // The deferred half of Go's recvLoop.
    if let Some(request) = current_req.take() {
        (request.callback)(
            TsoRequestResult::default(),
            request.req_keyspace_group_id,
            Some(dup_error(&finish_with_err)),
        );
    }
    let message = finish_with_err.to_string();
    *stream.stopped_with_err.lock().unwrap() = Some(dup_error(&finish_with_err));
    stream.cancel.cancel();
    loop {
        match stream.state.compare_exchange(
            STREAM_STATE_IDLE,
            STREAM_STATE_CLOSING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(STREAM_STATE_SENDING) => std::hint::spin_loop(),
            Err(STREAM_STATE_CLOSING) => {
                log::warn!(stream = stream.stream_id.as_str(); "unexpected double closing of tsoStream");
                break;
            }
            Err(state) => {
                log::error!(stream = stream.stream_id.as_str(), state = state; "unknown tsoStream state");
                break;
            }
        }
    }
    log::info!(
        stream = stream.stream_id.as_str(),
        error:% = message;
        "tsoStream.recvLoop ended"
    );
    pending_requests.close();
    while let Some(request) = pending_requests.recv().await {
        (request.callback)(
            TsoRequestResult::default(),
            request.req_keyspace_group_id,
            Some(dup_error(&finish_with_err)),
        );
    }
    stream.ongoing_requests.store(0, Ordering::Release);
    metrics
        .ongoing_request_count_gauge
        .with_label_values(&[&stream.stream_id])
        .set(0.0);
}

const FILTER_CUTOFF_FREQ: f64 = 1.0;
const FILTER_NEW_SAMPLE_WEIGHT_UPPER_BOUND: f64 = 0.2;

/// Go `rcFilter`: a discrete low-pass filter over unevenly spaced samples.
pub(crate) struct RcFilter {
    rc: f64,
    new_sample_weight_upper_bound: f64,
    value: f64,
    last_sample_time: Option<Instant>,
}

impl RcFilter {
    /// Go `newRCFilter`.
    pub(crate) fn new(cutoff: f64, new_sample_weight_upper_bound: f64) -> Self {
        Self {
            rc: 1.0 / (2.0 * std::f64::consts::PI * cutoff),
            new_sample_weight_upper_bound,
            value: 0.0,
            last_sample_time: None,
        }
    }

    /// Go `update`.
    pub(crate) fn update(&mut self, sample_time: Instant, new_sample: f64) -> f64 {
        let Some(last) = self.last_sample_time else {
            self.last_sample_time = Some(sample_time);
            self.value = new_sample;
            return new_sample;
        };
        let dt = sample_time.saturating_duration_since(last).as_secs_f64();
        let alpha = (dt / (self.rc + dt)).min(self.new_sample_weight_upper_bound);
        self.value = (1.0 - alpha) * self.value + alpha * new_sample;
        self.last_sample_time = Some(sample_time);
        self.value
    }
}
