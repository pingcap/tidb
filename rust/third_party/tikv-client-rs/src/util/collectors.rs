// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Prometheus metrics collected synchronously from gRPC channelz.
//!
//! This is the native Rust counterpart of client-go's `util/collectors`
//! package. A scrape remains synchronous and uncached. Tonic's asynchronous
//! RPC client is isolated on a dedicated worker runtime so collection also
//! works when Prometheus gathers metrics from a Tokio runtime thread.

use std::collections::{BTreeMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use prometheus::core::{Collector, Desc};
use prometheus::proto::{Counter as ProtoCounter, Gauge as ProtoGauge, LabelPair, Metric};
use prometheus::proto::{MetricFamily, MetricType};
use tonic::transport::Channel as TonicChannel;

/// Channelz protobuf and Tonic client/server types used by this collector.
pub mod proto {
    pub use crate::proto::grpc::channelz::v1::*;
}

use proto::address;
use proto::channel_connectivity_state;
use proto::channel_trace_event;
use proto::channelz_client::ChannelzClient;
use proto::{Channel, ChannelData, Socket, Subchannel};

const CHANNELZ_SUBSYSTEM: &str = "grpc_channelz";

/// One node supplied to a [`ChannelzFilter`].
#[derive(Clone, Copy, Debug)]
pub enum ChannelzNode<'a> {
    /// A logical channel.
    Channel(&'a Channel),
    /// A logical subchannel.
    Subchannel(&'a Subchannel),
    /// A transport socket.
    Socket(&'a Socket),
}

/// Decides whether to collect a node and whether to walk its children.
pub type ChannelzFilter =
    Arc<dyn for<'a> Fn(ChannelzNode<'a>) -> (bool, bool) + Send + Sync + 'static>;

/// Configuration for a channelz collector.
#[derive(Clone, Default)]
pub struct ChannelzCollectorOptions {
    /// Optional metric namespace.
    pub namespace: String,
    /// Optional node filter.
    pub filter: Option<ChannelzFilter>,
    /// Include per-channel trace statistics.
    pub include_channel_trace: bool,
    /// Include one-hot per-channel connectivity state.
    pub include_channel_state: bool,
    /// Suppress the `local` label on socket metrics.
    pub disable_local_label: bool,
    /// Suppress the `remote` label on socket metrics.
    pub disable_remote_label: bool,
}

/// Constructs a Prometheus collector backed by a Tonic channelz client.
///
/// The channel should address a server exposing `grpc.channelz.v1.Channelz`.
pub fn new_channelz_collector(
    channel: TonicChannel,
    options: ChannelzCollectorOptions,
) -> ChannelzCollector {
    ChannelzCollector::try_new(channel, options)
        .expect("client-go channelz metric descriptors must be valid")
}

/// Synchronous, uncached Prometheus collector for gRPC channelz.
pub struct ChannelzCollector {
    options: ChannelzCollectorOptions,
    descriptors: BTreeMap<Family, Desc>,
    descriptor_order: Vec<Family>,
    worker: RpcWorker,
    get_top_channels_errors_total: AtomicU64,
    get_channel_errors_total: AtomicU64,
    get_subchannel_errors_total: AtomicU64,
    get_socket_errors_total: AtomicU64,
}

impl ChannelzCollector {
    /// Fallible native constructor for callers that want descriptor errors.
    pub fn try_new(
        channel: TonicChannel,
        options: ChannelzCollectorOptions,
    ) -> prometheus::Result<Self> {
        let include_local = !options.disable_local_label;
        let include_remote = !options.disable_remote_label;
        let mut descriptor_order = vec![
            Family::ChannelCalls,
            Family::ChannelLastCallStarted,
            Family::SocketStreams,
            Family::SocketMessages,
            Family::SocketKeepAlives,
            Family::SocketLastStreamCreated,
            Family::SocketLastMessage,
            Family::SocketFlowControlWindow,
            Family::FetchErrors,
        ];
        if options.include_channel_state {
            descriptor_order.push(Family::ChannelState);
        }
        if options.include_channel_trace {
            descriptor_order.extend([
                Family::ChannelTraceEventsLogged,
                Family::ChannelTraceCreation,
                Family::ChannelTraceEvents,
            ]);
        }

        let descriptors = descriptor_order
            .iter()
            .copied()
            .map(|family| {
                let labels = family.label_names(include_local, include_remote);
                Desc::new(
                    family.fq_name(&options.namespace),
                    family.help().to_owned(),
                    labels.into_iter().map(str::to_owned).collect(),
                    Default::default(),
                )
                .map(|desc| (family, desc))
            })
            .collect::<prometheus::Result<BTreeMap<_, _>>>()?;

        Ok(Self {
            options,
            descriptors,
            descriptor_order,
            worker: RpcWorker::new(channel),
            get_top_channels_errors_total: AtomicU64::new(0),
            get_channel_errors_total: AtomicU64::new(0),
            get_subchannel_errors_total: AtomicU64::new(0),
            get_socket_errors_total: AtomicU64::new(0),
        })
    }

    fn collect_walk(&self) -> WalkResult {
        self.worker
            .collect(
                self.options.filter.clone(),
                WalkSettings {
                    include_local_label: !self.options.disable_local_label,
                    include_remote_label: !self.options.disable_remote_label,
                    include_channel_trace: self.options.include_channel_trace,
                    include_channel_state: self.options.include_channel_state,
                },
            )
            .unwrap_or_else(|| WalkResult {
                fetch_errors: FetchErrors {
                    get_top_channels: 1,
                    ..Default::default()
                },
                ..Default::default()
            })
    }

    fn add_fetch_errors(&self, errors: FetchErrors) {
        self.get_top_channels_errors_total
            .fetch_add(errors.get_top_channels, Ordering::Relaxed);
        self.get_channel_errors_total
            .fetch_add(errors.get_channel, Ordering::Relaxed);
        self.get_subchannel_errors_total
            .fetch_add(errors.get_subchannel, Ordering::Relaxed);
        self.get_socket_errors_total
            .fetch_add(errors.get_socket, Ordering::Relaxed);
    }

    fn fetch_error_samples(&self) -> [Sample; 4] {
        [
            Sample::new(
                Family::FetchErrors,
                [("rpc", "GetTopChannels")],
                self.get_top_channels_errors_total.load(Ordering::Relaxed) as f64,
            ),
            Sample::new(
                Family::FetchErrors,
                [("rpc", "GetChannel")],
                self.get_channel_errors_total.load(Ordering::Relaxed) as f64,
            ),
            Sample::new(
                Family::FetchErrors,
                [("rpc", "GetSubchannel")],
                self.get_subchannel_errors_total.load(Ordering::Relaxed) as f64,
            ),
            Sample::new(
                Family::FetchErrors,
                [("rpc", "GetSocket")],
                self.get_socket_errors_total.load(Ordering::Relaxed) as f64,
            ),
        ]
    }
}

impl Collector for ChannelzCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descriptor_order
            .iter()
            .map(|family| &self.descriptors[family])
            .collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut result = self.collect_walk();
        self.add_fetch_errors(result.fetch_errors);
        result.samples.extend(self.fetch_error_samples());
        build_metric_families(
            result.samples,
            &self.options.namespace,
            !self.options.disable_local_label,
            !self.options.disable_remote_label,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Family {
    ChannelCalls,
    ChannelLastCallStarted,
    SocketStreams,
    SocketMessages,
    SocketKeepAlives,
    SocketLastStreamCreated,
    SocketLastMessage,
    SocketFlowControlWindow,
    FetchErrors,
    ChannelState,
    ChannelTraceEventsLogged,
    ChannelTraceCreation,
    ChannelTraceEvents,
}

impl Family {
    fn suffix(self) -> &'static str {
        match self {
            Self::ChannelCalls => "channel_calls_total",
            Self::ChannelLastCallStarted => "channel_last_call_started_timestamp_seconds",
            Self::SocketStreams => "socket_streams_total",
            Self::SocketMessages => "socket_messages_total",
            Self::SocketKeepAlives => "socket_keepalives_total",
            Self::SocketLastStreamCreated => "socket_last_stream_created_timestamp_seconds",
            Self::SocketLastMessage => "socket_last_message_timestamp_seconds",
            Self::SocketFlowControlWindow => "socket_flow_control_window_bytes",
            Self::FetchErrors => "fetch_errors_total",
            Self::ChannelState => "channel_state",
            Self::ChannelTraceEventsLogged => "channel_trace_events_logged_total",
            Self::ChannelTraceCreation => "channel_trace_creation_timestamp_seconds",
            Self::ChannelTraceEvents => "channel_trace_event_count",
        }
    }

    fn help(self) -> &'static str {
        match self {
            Self::ChannelCalls => "Total calls observed by the channelz channel or subchannel.",
            Self::ChannelLastCallStarted => {
                "Unix timestamp of the last call started on the channelz channel or subchannel."
            }
            Self::SocketStreams => "Total streams observed by the channelz socket.",
            Self::SocketMessages => "Total messages observed by the channelz socket.",
            Self::SocketKeepAlives => "Total keepalive pings sent on the channelz socket.",
            Self::SocketLastStreamCreated => {
                "Unix timestamp of the last stream created on the channelz socket."
            }
            Self::SocketLastMessage => {
                "Unix timestamp of the last message activity observed by the channelz socket."
            }
            Self::SocketFlowControlWindow => {
                "HTTP/2 flow control window exposed by the channelz socket."
            }
            Self::FetchErrors => "Total RPC fetch errors encountered by the channelz collector.",
            Self::ChannelState => {
                "Connectivity state of the channelz channel or subchannel, exported as a one-hot gauge."
            }
            Self::ChannelTraceEventsLogged => {
                "Total number of events ever logged in the channelz trace object."
            }
            Self::ChannelTraceCreation => {
                "Unix timestamp when the channelz trace object was created."
            }
            Self::ChannelTraceEvents => {
                "Current number of trace events in the channelz trace buffer, partitioned by severity."
            }
        }
    }

    fn metric_type(self) -> MetricType {
        match self {
            Self::ChannelCalls
            | Self::SocketStreams
            | Self::SocketMessages
            | Self::SocketKeepAlives
            | Self::FetchErrors
            | Self::ChannelTraceEventsLogged => MetricType::COUNTER,
            _ => MetricType::GAUGE,
        }
    }

    fn fq_name(self, namespace: &str) -> String {
        [namespace, CHANNELZ_SUBSYSTEM, self.suffix()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("_")
    }

    fn label_names(self, include_local: bool, include_remote: bool) -> Vec<&'static str> {
        let mut socket = vec!["id"];
        if include_local {
            socket.push("local");
        }
        if include_remote {
            socket.push("remote");
        }
        match self {
            Self::ChannelCalls => vec!["kind", "id", "target", "type"],
            Self::ChannelLastCallStarted => vec!["kind", "id", "target"],
            Self::SocketStreams => append_label(socket, "type"),
            Self::SocketMessages => append_label(socket, "direction"),
            Self::SocketKeepAlives => socket,
            Self::SocketLastStreamCreated => append_label(socket, "side"),
            Self::SocketLastMessage => append_label(socket, "direction"),
            Self::SocketFlowControlWindow => append_label(socket, "side"),
            Self::FetchErrors => vec!["rpc"],
            Self::ChannelState => vec!["kind", "id", "target", "state"],
            Self::ChannelTraceEventsLogged | Self::ChannelTraceCreation => {
                vec!["kind", "id", "target"]
            }
            Self::ChannelTraceEvents => vec!["kind", "id", "target", "severity"],
        }
    }
}

fn append_label(mut labels: Vec<&'static str>, label: &'static str) -> Vec<&'static str> {
    labels.push(label);
    labels
}

#[derive(Debug)]
struct Sample {
    family: Family,
    labels: Vec<(String, String)>,
    value: f64,
}

impl Sample {
    fn new<K, V, I>(family: Family, labels: I, value: f64) -> Self
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        Self {
            family,
            labels: labels
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
            value,
        }
    }
}

fn build_metric_families(
    samples: Vec<Sample>,
    namespace: &str,
    include_local: bool,
    include_remote: bool,
) -> Vec<MetricFamily> {
    let mut families = BTreeMap::<Family, MetricFamily>::new();
    for sample in samples {
        let family = families.entry(sample.family).or_insert_with(|| {
            let mut family = MetricFamily::default();
            family.set_name(sample.family.fq_name(namespace));
            family.set_help(sample.family.help().to_owned());
            family.set_field_type(sample.family.metric_type());
            family
        });
        let expected = sample.family.label_names(include_local, include_remote);
        debug_assert_eq!(
            sample
                .labels
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        let mut labels = sample
            .labels
            .into_iter()
            .map(|(name, value)| {
                let mut pair = LabelPair::default();
                pair.set_name(name);
                pair.set_value(value);
                pair
            })
            .collect::<Vec<_>>();
        labels.sort();
        let mut metric = Metric::default();
        metric.set_label(labels.into());
        match sample.family.metric_type() {
            MetricType::COUNTER => {
                let mut counter = ProtoCounter::default();
                counter.set_value(sample.value);
                metric.set_counter(counter);
            }
            MetricType::GAUGE => {
                let mut gauge = ProtoGauge::default();
                gauge.set_value(sample.value);
                metric.set_gauge(gauge);
            }
            _ => unreachable!("channelz emits only counters and gauges"),
        }
        family.mut_metric().push(metric);
    }
    families.into_values().collect()
}

#[derive(Clone, Copy, Debug, Default)]
struct FetchErrors {
    get_top_channels: u64,
    get_channel: u64,
    get_subchannel: u64,
    get_socket: u64,
}

#[derive(Debug, Default)]
struct WalkResult {
    samples: Vec<Sample>,
    fetch_errors: FetchErrors,
}

enum WorkerCommand {
    Collect {
        filter: Option<ChannelzFilter>,
        settings: WalkSettings,
        reply: mpsc::Sender<WalkResult>,
    },
    Shutdown,
}

struct RpcWorker {
    commands: tokio::sync::mpsc::UnboundedSender<WorkerCommand>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RpcWorker {
    fn new(channel: TonicChannel) -> Self {
        let (commands, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let thread = thread::Builder::new()
            .name("tikv-channelz-collector".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("channelz worker runtime creation failed");
                let client = ChannelzClient::new(channel);
                runtime.block_on(async move {
                    while let Some(command) = receiver.recv().await {
                        match command {
                            WorkerCommand::Collect {
                                filter,
                                settings,
                                reply,
                            } => {
                                let mut client = client.clone();
                                tokio::spawn(async move {
                                    let result =
                                        Walker::new(filter, settings).walk(&mut client).await;
                                    let _ = reply.send(result);
                                });
                            }
                            WorkerCommand::Shutdown => break,
                        }
                    }
                });
            })
            .expect("channelz worker thread creation failed");
        Self {
            commands,
            thread: Mutex::new(Some(thread)),
        }
    }

    fn collect(
        &self,
        filter: Option<ChannelzFilter>,
        settings: WalkSettings,
    ) -> Option<WalkResult> {
        let (reply, response) = mpsc::channel();
        self.commands
            .send(WorkerCommand::Collect {
                filter,
                settings,
                reply,
            })
            .ok()?;
        response.recv().ok()
    }
}

impl Drop for RpcWorker {
    fn drop(&mut self) {
        let _ = self.commands.send(WorkerCommand::Shutdown);
        if let Some(thread) = self.thread.lock().expect("worker lock poisoned").take() {
            let _ = thread.join();
        }
    }
}

struct Walker {
    filter: Option<ChannelzFilter>,
    settings: WalkSettings,
    seen_channels: HashSet<i64>,
    seen_subchannels: HashSet<i64>,
    seen_sockets: HashSet<i64>,
    result: WalkResult,
}

impl Walker {
    fn new(filter: Option<ChannelzFilter>, settings: WalkSettings) -> Self {
        Self {
            filter,
            settings,
            seen_channels: HashSet::new(),
            seen_subchannels: HashSet::new(),
            seen_sockets: HashSet::new(),
            result: WalkResult::default(),
        }
    }

    async fn walk(mut self, client: &mut ChannelzClient<TonicChannel>) -> WalkResult {
        let mut start_channel_id = 0;
        loop {
            let response = client
                .get_top_channels(proto::GetTopChannelsRequest {
                    start_channel_id,
                    max_results: 0,
                })
                .await;
            let response = match response {
                Ok(response) => response.into_inner(),
                Err(_) => {
                    self.result.fetch_errors.get_top_channels += 1;
                    break;
                }
            };
            let mut max_id = 0;
            let channel_count = response.channel.len();
            for channel in response.channel {
                max_id = max_id.max(channel_id(&channel));
                self.walk_channel(client, channel).await;
            }
            if response.end || channel_count == 0 || max_id <= start_channel_id {
                break;
            }
            start_channel_id = max_id.wrapping_add(1);
        }
        self.result
    }

    #[async_recursion::async_recursion]
    async fn walk_channel(&mut self, client: &mut ChannelzClient<TonicChannel>, channel: Channel) {
        let id = channel_id(&channel);
        if id <= 0 || !self.seen_channels.insert(id) {
            return;
        }
        let (collect, walk_children) = self.apply_filter(ChannelzNode::Channel(&channel));
        if collect {
            if let Some(data) = channel.data.as_ref() {
                self.collect_channel_metrics("channel", id, data);
            }
        }
        if !walk_children {
            return;
        }
        for child in channel.channel_ref {
            match client
                .get_channel(proto::GetChannelRequest {
                    channel_id: child.channel_id,
                })
                .await
            {
                Ok(response) => {
                    if let Some(child) = response.into_inner().channel {
                        self.walk_channel(client, child).await;
                    }
                }
                Err(_) => self.result.fetch_errors.get_channel += 1,
            }
        }
        for child in channel.subchannel_ref {
            match client
                .get_subchannel(proto::GetSubchannelRequest {
                    subchannel_id: child.subchannel_id,
                })
                .await
            {
                Ok(response) => {
                    if let Some(child) = response.into_inner().subchannel {
                        self.walk_subchannel(client, child).await;
                    }
                }
                Err(_) => self.result.fetch_errors.get_subchannel += 1,
            }
        }
        for child in channel.socket_ref {
            self.fetch_socket(client, child.socket_id).await;
        }
    }

    #[async_recursion::async_recursion]
    async fn walk_subchannel(
        &mut self,
        client: &mut ChannelzClient<TonicChannel>,
        subchannel: Subchannel,
    ) {
        let id = subchannel_id(&subchannel);
        if id <= 0 || !self.seen_subchannels.insert(id) {
            return;
        }
        let (collect, walk_children) = self.apply_filter(ChannelzNode::Subchannel(&subchannel));
        if collect {
            if let Some(data) = subchannel.data.as_ref() {
                self.collect_channel_metrics("subchannel", id, data);
            }
        }
        if !walk_children {
            return;
        }
        for child in subchannel.channel_ref {
            match client
                .get_channel(proto::GetChannelRequest {
                    channel_id: child.channel_id,
                })
                .await
            {
                Ok(response) => {
                    if let Some(child) = response.into_inner().channel {
                        self.walk_channel(client, child).await;
                    }
                }
                Err(_) => self.result.fetch_errors.get_channel += 1,
            }
        }
        for child in subchannel.subchannel_ref {
            match client
                .get_subchannel(proto::GetSubchannelRequest {
                    subchannel_id: child.subchannel_id,
                })
                .await
            {
                Ok(response) => {
                    if let Some(child) = response.into_inner().subchannel {
                        self.walk_subchannel(client, child).await;
                    }
                }
                Err(_) => self.result.fetch_errors.get_subchannel += 1,
            }
        }
        for child in subchannel.socket_ref {
            self.fetch_socket(client, child.socket_id).await;
        }
    }

    async fn fetch_socket(&mut self, client: &mut ChannelzClient<TonicChannel>, socket_id: i64) {
        match client
            .get_socket(proto::GetSocketRequest {
                socket_id,
                summary: false,
            })
            .await
        {
            Ok(response) => {
                if let Some(socket) = response.into_inner().socket {
                    self.walk_socket(socket);
                }
            }
            Err(_) => self.result.fetch_errors.get_socket += 1,
        }
    }

    fn walk_socket(&mut self, socket: Socket) {
        let id = socket_id(&socket);
        if id <= 0 || !self.seen_sockets.insert(id) {
            return;
        }
        if self.apply_filter(ChannelzNode::Socket(&socket)).0 {
            self.collect_socket_metrics(id, &socket);
        }
    }

    fn apply_filter(&self, node: ChannelzNode<'_>) -> (bool, bool) {
        self.filter
            .as_ref()
            .map_or((true, true), |filter| filter(node))
    }

    fn collect_channel_metrics(&mut self, kind: &str, id: i64, data: &ChannelData) {
        let id = id.to_string();
        let base = [
            ("kind", kind),
            ("id", id.as_str()),
            ("target", data.target.as_str()),
        ];
        for (result, value) in [
            ("started", data.calls_started),
            ("succeeded", data.calls_succeeded),
            ("failed", data.calls_failed),
        ] {
            self.result.samples.push(Sample::new(
                Family::ChannelCalls,
                base.into_iter().chain([("type", result)]),
                value as f64,
            ));
        }
        if let Some(timestamp) = data.last_call_started_timestamp.as_ref() {
            self.result.samples.push(Sample::new(
                Family::ChannelLastCallStarted,
                base,
                timestamp_seconds(timestamp),
            ));
        }
        if self.options_channel_state() {
            let current = normalize_connectivity_state(data.state.as_ref());
            for state in [
                "unknown",
                "idle",
                "connecting",
                "ready",
                "transient_failure",
                "shutdown",
            ] {
                self.result.samples.push(Sample::new(
                    Family::ChannelState,
                    base.into_iter().chain([("state", state)]),
                    f64::from(state == current),
                ));
            }
        }
        if !self.options_channel_trace() {
            return;
        }
        let Some(trace) = data.trace.as_ref() else {
            return;
        };
        self.result.samples.push(Sample::new(
            Family::ChannelTraceEventsLogged,
            base,
            trace.num_events_logged as f64,
        ));
        if let Some(timestamp) = trace.creation_timestamp.as_ref() {
            self.result.samples.push(Sample::new(
                Family::ChannelTraceCreation,
                base,
                timestamp_seconds(timestamp),
            ));
        }
        let mut counts = [0.0; 4];
        for event in &trace.events {
            counts[severity_index(event.severity)] += 1.0;
        }
        for (severity, value) in ["unknown", "info", "warning", "error"]
            .into_iter()
            .zip(counts)
        {
            self.result.samples.push(Sample::new(
                Family::ChannelTraceEvents,
                base.into_iter().chain([("severity", severity)]),
                value,
            ));
        }
    }

    fn collect_socket_metrics(&mut self, id: i64, socket: &Socket) {
        let Some(data) = socket.data.as_ref() else {
            return;
        };
        let labels = self.socket_labels(id, socket);
        for (kind, value) in [
            ("started", data.streams_started),
            ("succeeded", data.streams_succeeded),
            ("failed", data.streams_failed),
        ] {
            self.result.samples.push(Sample::new(
                Family::SocketStreams,
                labels.iter().map(pair_refs).chain([("type", kind)]),
                value as f64,
            ));
        }
        for (direction, value) in [
            ("sent", data.messages_sent),
            ("received", data.messages_received),
        ] {
            self.result.samples.push(Sample::new(
                Family::SocketMessages,
                labels
                    .iter()
                    .map(pair_refs)
                    .chain([("direction", direction)]),
                value as f64,
            ));
        }
        self.result.samples.push(Sample::new(
            Family::SocketKeepAlives,
            labels.iter().map(pair_refs),
            data.keep_alives_sent as f64,
        ));

        for (side, timestamp) in [
            ("local", data.last_local_stream_created_timestamp.as_ref()),
            ("remote", data.last_remote_stream_created_timestamp.as_ref()),
        ] {
            if timestamp.is_some_and(has_usable_timestamp) {
                self.result.samples.push(Sample::new(
                    Family::SocketLastStreamCreated,
                    labels.iter().map(pair_refs).chain([("side", side)]),
                    timestamp_seconds(timestamp.unwrap()),
                ));
            }
        }
        for (direction, timestamp) in [
            ("sent", data.last_message_sent_timestamp.as_ref()),
            ("received", data.last_message_received_timestamp.as_ref()),
        ] {
            if let Some(timestamp) = timestamp {
                self.result.samples.push(Sample::new(
                    Family::SocketLastMessage,
                    labels
                        .iter()
                        .map(pair_refs)
                        .chain([("direction", direction)]),
                    timestamp_seconds(timestamp),
                ));
            }
        }
        for (side, window) in [
            ("local", data.local_flow_control_window),
            ("remote", data.remote_flow_control_window),
        ] {
            if let Some(window) = window {
                self.result.samples.push(Sample::new(
                    Family::SocketFlowControlWindow,
                    labels.iter().map(pair_refs).chain([("side", side)]),
                    window as f64,
                ));
            }
        }
    }

    fn socket_labels(&self, id: i64, socket: &Socket) -> Vec<(String, String)> {
        let mut labels = vec![("id".to_owned(), id.to_string())];
        if self.include_local_label() {
            labels.push(("local".to_owned(), format_address(socket.local.as_ref())));
        }
        if self.include_remote_label() {
            labels.push(("remote".to_owned(), format_address(socket.remote.as_ref())));
        }
        labels
    }

    fn include_local_label(&self) -> bool {
        self.settings.include_local_label
    }

    fn include_remote_label(&self) -> bool {
        self.settings.include_remote_label
    }

    fn options_channel_state(&self) -> bool {
        self.settings.include_channel_state
    }

    fn options_channel_trace(&self) -> bool {
        self.settings.include_channel_trace
    }
}

#[derive(Clone, Copy)]
struct WalkSettings {
    include_local_label: bool,
    include_remote_label: bool,
    include_channel_trace: bool,
    include_channel_state: bool,
}

fn pair_refs(pair: &(String, String)) -> (&str, &str) {
    (&pair.0, &pair.1)
}

fn channel_id(channel: &Channel) -> i64 {
    channel
        .r#ref
        .as_ref()
        .map_or(0, |reference| reference.channel_id)
}

fn subchannel_id(channel: &Subchannel) -> i64 {
    channel
        .r#ref
        .as_ref()
        .map_or(0, |reference| reference.subchannel_id)
}

fn socket_id(socket: &Socket) -> i64 {
    socket
        .r#ref
        .as_ref()
        .map_or(0, |reference| reference.socket_id)
}

fn timestamp_seconds(timestamp: &prost_types::Timestamp) -> f64 {
    timestamp
        .seconds
        .wrapping_mul(1_000_000_000)
        .wrapping_add(timestamp.nanos as i64) as f64
        / 1_000_000_000.0
}

fn has_usable_timestamp(timestamp: &prost_types::Timestamp) -> bool {
    const MIN_SECONDS: i64 = -62_135_596_800;
    const MAX_SECONDS: i64 = 253_402_300_799;
    (MIN_SECONDS..=MAX_SECONDS).contains(&timestamp.seconds)
        && (0..1_000_000_000).contains(&timestamp.nanos)
        && (timestamp.seconds != 0 || timestamp.nanos != 0)
}

fn normalize_connectivity_state(state: Option<&proto::ChannelConnectivityState>) -> &'static str {
    match state.and_then(|state| channel_connectivity_state::State::try_from(state.state).ok()) {
        Some(channel_connectivity_state::State::Idle) => "idle",
        Some(channel_connectivity_state::State::Connecting) => "connecting",
        Some(channel_connectivity_state::State::Ready) => "ready",
        Some(channel_connectivity_state::State::TransientFailure) => "transient_failure",
        Some(channel_connectivity_state::State::Shutdown) => "shutdown",
        _ => "unknown",
    }
}

fn severity_index(severity: i32) -> usize {
    match channel_trace_event::Severity::try_from(severity).ok() {
        Some(channel_trace_event::Severity::CtInfo) => 1,
        Some(channel_trace_event::Severity::CtWarning) => 2,
        Some(channel_trace_event::Severity::CtError) => 3,
        _ => 0,
    }
}

fn format_address(address: Option<&proto::Address>) -> String {
    match address.and_then(|address| address.address.as_ref()) {
        Some(address::Address::TcpipAddress(address)) => {
            let ip = match address.ip_address.as_slice() {
                [a, b, c, d] => Ipv4Addr::new(*a, *b, *c, *d).to_string(),
                bytes if bytes.len() == 16 => {
                    let mut octets = [0; 16];
                    octets.copy_from_slice(bytes);
                    let address = Ipv6Addr::from(octets);
                    address
                        .to_ipv4_mapped()
                        .map_or_else(|| address.to_string(), |address| address.to_string())
                }
                [] => "<nil>".to_owned(),
                bytes => format!(
                    "?{}",
                    bytes
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                ),
            };
            if address.port < 0 {
                ip
            } else if ip.contains(':') {
                format!("[{ip}]:{}", address.port)
            } else {
                format!("{ip}:{}", address.port)
            }
        }
        Some(address::Address::UdsAddress(address)) => address.filename.clone(),
        Some(address::Address::OtherAddress(address)) => address.name.clone(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap, HashSet};

    use prometheus::Registry;
    use proto::channelz_server::{Channelz, ChannelzServer};
    use proto::{ChannelConnectivityState, ChannelRef, ChannelTrace, ChannelTraceEvent};
    use proto::{GetChannelRequest, GetChannelResponse, GetServerRequest, GetServerResponse};
    use proto::{GetServerSocketsRequest, GetServerSocketsResponse, GetServersRequest};
    use proto::{GetServersResponse, GetSocketRequest, GetSocketResponse};
    use proto::{GetSubchannelRequest, GetSubchannelResponse, GetTopChannelsRequest};
    use proto::{GetTopChannelsResponse, SocketData, SocketRef, SubchannelRef};
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    #[derive(Default)]
    struct FakeChannelzServer {
        top_channels: HashMap<i64, GetTopChannelsResponse>,
        top_channel_errors: HashSet<i64>,
        channels: HashMap<i64, Channel>,
        channel_errors: HashSet<i64>,
        subchannels: HashMap<i64, Subchannel>,
        subchannel_errors: HashSet<i64>,
        sockets: HashMap<i64, Socket>,
        socket_errors: HashSet<i64>,
    }

    #[tonic::async_trait]
    impl Channelz for FakeChannelzServer {
        async fn get_top_channels(
            &self,
            request: Request<GetTopChannelsRequest>,
        ) -> Result<Response<GetTopChannelsResponse>, Status> {
            let id = request.into_inner().start_channel_id;
            if self.top_channel_errors.contains(&id) {
                return Err(Status::unavailable("top channels unavailable"));
            }
            Ok(Response::new(
                self.top_channels
                    .get(&id)
                    .cloned()
                    .unwrap_or(GetTopChannelsResponse {
                        channel: Vec::new(),
                        end: true,
                    }),
            ))
        }

        async fn get_servers(
            &self,
            _: Request<GetServersRequest>,
        ) -> Result<Response<GetServersResponse>, Status> {
            Err(Status::unimplemented("unused by collector"))
        }

        async fn get_server(
            &self,
            _: Request<GetServerRequest>,
        ) -> Result<Response<GetServerResponse>, Status> {
            Err(Status::unimplemented("unused by collector"))
        }

        async fn get_server_sockets(
            &self,
            _: Request<GetServerSocketsRequest>,
        ) -> Result<Response<GetServerSocketsResponse>, Status> {
            Err(Status::unimplemented("unused by collector"))
        }

        async fn get_channel(
            &self,
            request: Request<GetChannelRequest>,
        ) -> Result<Response<GetChannelResponse>, Status> {
            let id = request.into_inner().channel_id;
            if self.channel_errors.contains(&id) {
                return Err(Status::unavailable("channel unavailable"));
            }
            self.channels
                .get(&id)
                .cloned()
                .map(|channel| {
                    Response::new(GetChannelResponse {
                        channel: Some(channel),
                    })
                })
                .ok_or_else(|| Status::not_found("channel not found"))
        }

        async fn get_subchannel(
            &self,
            request: Request<GetSubchannelRequest>,
        ) -> Result<Response<GetSubchannelResponse>, Status> {
            let id = request.into_inner().subchannel_id;
            if self.subchannel_errors.contains(&id) {
                return Err(Status::unavailable("subchannel unavailable"));
            }
            self.subchannels
                .get(&id)
                .cloned()
                .map(|subchannel| {
                    Response::new(GetSubchannelResponse {
                        subchannel: Some(subchannel),
                    })
                })
                .ok_or_else(|| Status::not_found("subchannel not found"))
        }

        async fn get_socket(
            &self,
            request: Request<GetSocketRequest>,
        ) -> Result<Response<GetSocketResponse>, Status> {
            let id = request.into_inner().socket_id;
            if self.socket_errors.contains(&id) {
                return Err(Status::unavailable("socket unavailable"));
            }
            self.sockets
                .get(&id)
                .cloned()
                .map(|socket| {
                    Response::new(GetSocketResponse {
                        socket: Some(socket),
                    })
                })
                .ok_or_else(|| Status::not_found("socket not found"))
        }
    }

    async fn gather(
        server: FakeChannelzServer,
        options: ChannelzCollectorOptions,
    ) -> BTreeMap<String, MetricFamily> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ChannelzServer::new(server))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let registry = Registry::new();
        registry
            .register(Box::new(new_channelz_collector(channel, options)))
            .unwrap();
        let families = registry
            .gather()
            .into_iter()
            .map(|family| (family.get_name().to_owned(), family))
            .collect();
        drop(registry);
        let _ = shutdown.send(());
        task.await.unwrap();
        families
    }

    fn channel(id: i64, target: &str) -> Channel {
        Channel {
            r#ref: Some(ChannelRef {
                channel_id: id,
                name: String::new(),
            }),
            data: Some(ChannelData {
                target: target.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn subchannel(id: i64, target: &str) -> Subchannel {
        Subchannel {
            r#ref: Some(SubchannelRef {
                subchannel_id: id,
                name: String::new(),
            }),
            data: Some(ChannelData {
                target: target.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn socket(id: i64, streams_started: i64) -> Socket {
        Socket {
            r#ref: Some(SocketRef {
                socket_id: id,
                name: String::new(),
            }),
            data: Some(SocketData {
                streams_started,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn timestamp(seconds: i64) -> prost_types::Timestamp {
        prost_types::Timestamp { seconds, nanos: 0 }
    }

    fn tcp_address(ip: Vec<u8>, port: i32) -> proto::Address {
        proto::Address {
            address: Some(address::Address::TcpipAddress(address::TcpIpAddress {
                ip_address: ip,
                port,
            })),
        }
    }

    fn uds_address(filename: &str) -> proto::Address {
        proto::Address {
            address: Some(address::Address::UdsAddress(address::UdsAddress {
                filename: filename.to_owned(),
            })),
        }
    }

    fn other_address(name: &str) -> proto::Address {
        proto::Address {
            address: Some(address::Address::OtherAddress(address::OtherAddress {
                name: name.to_owned(),
                value: None,
            })),
        }
    }

    fn labels(metric: &Metric) -> BTreeMap<&str, &str> {
        metric
            .get_label()
            .iter()
            .map(|pair| (pair.get_name(), pair.get_value()))
            .collect()
    }

    fn metric_value(family: &MetricFamily, want: &[(&str, &str)]) -> Option<f64> {
        family.get_metric().iter().find_map(|metric| {
            let labels = labels(metric);
            want.iter()
                .all(|(name, value)| labels.get(name) == Some(value))
                .then(|| match family.get_field_type() {
                    MetricType::COUNTER => metric.get_counter().get_value(),
                    MetricType::GAUGE => metric.get_gauge().get_value(),
                    kind => panic!("unexpected metric type {kind:?}"),
                })
        })
    }

    fn count_metrics(family: Option<&MetricFamily>, want: &[(&str, &str)]) -> usize {
        family.map_or(0, |family| {
            family
                .get_metric()
                .iter()
                .filter(|metric| {
                    let labels = labels(metric);
                    want.iter()
                        .all(|(name, value)| labels.get(name) == Some(value))
                })
                .count()
        })
    }

    fn assert_value(
        families: &BTreeMap<String, MetricFamily>,
        family: &str,
        labels: &[(&str, &str)],
        expected: f64,
    ) {
        let actual = metric_value(&families[family], labels)
            .unwrap_or_else(|| panic!("missing {family} labels={labels:?}"));
        assert!(
            (actual - expected).abs() < 0.000_001,
            "{actual} != {expected}"
        );
    }

    #[test]
    fn address_format_matches_go_for_mapped_and_invalid_ips() {
        let mapped = tcp_address(
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 127, 0, 0, 1],
            80,
        );
        assert_eq!(format_address(Some(&mapped)), "127.0.0.1:80");
        let invalid = tcp_address(vec![1, 2], -1);
        assert_eq!(format_address(Some(&invalid)), "?0102");
        let empty = tcp_address(Vec::new(), -1);
        assert_eq!(format_address(Some(&empty)), "<nil>");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channelz_collector_collects_and_deduplicates_the_source_graph() {
        let base = 1_700_000_000;
        let mut root = channel(1, "cluster-a");
        root.subchannel_ref.push(SubchannelRef {
            subchannel_id: 2,
            name: String::new(),
        });
        root.socket_ref.push(SocketRef {
            socket_id: 10,
            name: String::new(),
        });
        root.data = Some(ChannelData {
            target: "cluster-a".to_owned(),
            calls_started: 12,
            calls_succeeded: 10,
            calls_failed: 2,
            last_call_started_timestamp: Some(timestamp(base - 20)),
            state: Some(ChannelConnectivityState {
                state: channel_connectivity_state::State::Ready as i32,
            }),
            trace: Some(ChannelTrace {
                num_events_logged: 4,
                creation_timestamp: Some(timestamp(base - 300)),
                events: vec![
                    ChannelTraceEvent {
                        severity: channel_trace_event::Severity::CtInfo as i32,
                        ..Default::default()
                    },
                    ChannelTraceEvent {
                        severity: channel_trace_event::Severity::CtWarning as i32,
                        ..Default::default()
                    },
                    ChannelTraceEvent {
                        severity: channel_trace_event::Severity::CtError as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut sub = subchannel(2, "backend-a");
        sub.data.as_mut().unwrap().calls_started = 3;
        sub.data.as_mut().unwrap().calls_succeeded = 2;
        sub.data.as_mut().unwrap().calls_failed = 1;
        sub.socket_ref = vec![
            SocketRef {
                socket_id: 10,
                name: String::new(),
            },
            SocketRef {
                socket_id: 11,
                name: String::new(),
            },
        ];
        let mut socket10 = socket(10, 5);
        socket10.local = Some(tcp_address(vec![127, 0, 0, 1], 8080));
        socket10.remote = Some(tcp_address(
            vec![0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            443,
        ));
        socket10.data = Some(SocketData {
            streams_started: 5,
            streams_succeeded: 3,
            streams_failed: 1,
            messages_sent: 9,
            messages_received: 8,
            keep_alives_sent: 4,
            last_local_stream_created_timestamp: Some(timestamp(base - 11)),
            last_remote_stream_created_timestamp: Some(timestamp(base - 13)),
            last_message_sent_timestamp: Some(timestamp(base - 5)),
            last_message_received_timestamp: Some(timestamp(base - 7)),
            local_flow_control_window: Some(128),
            remote_flow_control_window: Some(256),
            ..Default::default()
        });
        let mut socket11 = socket(11, 2);
        socket11.local = Some(uds_address("/tmp/channelz.sock"));
        socket11.remote = Some(other_address("passthrough:///peer"));
        socket11.data.as_mut().unwrap().streams_succeeded = 1;
        socket11.data.as_mut().unwrap().streams_failed = 1;
        socket11.data.as_mut().unwrap().last_message_sent_timestamp = Some(timestamp(base - 9));
        socket11
            .data
            .as_mut()
            .unwrap()
            .last_message_received_timestamp = Some(timestamp(base - 3));

        let mut server = FakeChannelzServer::default();
        server.top_channels.insert(
            0,
            GetTopChannelsResponse {
                channel: vec![root],
                end: false,
            },
        );
        server.top_channels.insert(
            2,
            GetTopChannelsResponse {
                channel: vec![channel(3, "cluster-b")],
                end: true,
            },
        );
        server.subchannels.insert(2, sub);
        server.sockets.insert(10, socket10);
        server.sockets.insert(11, socket11);

        let families = gather(server, ChannelzCollectorOptions::default()).await;
        assert_value(
            &families,
            "grpc_channelz_channel_calls_total",
            &[
                ("kind", "channel"),
                ("id", "1"),
                ("target", "cluster-a"),
                ("type", "started"),
            ],
            12.0,
        );
        assert_value(
            &families,
            "grpc_channelz_channel_calls_total",
            &[
                ("kind", "subchannel"),
                ("id", "2"),
                ("target", "backend-a"),
                ("type", "failed"),
            ],
            1.0,
        );
        assert!(!families.contains_key("grpc_channelz_channel_state"));
        assert!(!families.contains_key("grpc_channelz_channel_trace_event_count"));
        let socket10_labels = [
            ("id", "10"),
            ("local", "127.0.0.1:8080"),
            ("remote", "[2001:db8::1]:443"),
        ];
        assert_value(
            &families,
            "grpc_channelz_socket_streams_total",
            &[socket10_labels.as_slice(), &[("type", "started")]].concat(),
            5.0,
        );
        assert_value(
            &families,
            "grpc_channelz_socket_messages_total",
            &[socket10_labels.as_slice(), &[("direction", "sent")]].concat(),
            9.0,
        );
        assert_value(
            &families,
            "grpc_channelz_socket_keepalives_total",
            &socket10_labels,
            4.0,
        );
        assert_value(
            &families,
            "grpc_channelz_socket_flow_control_window_bytes",
            &[socket10_labels.as_slice(), &[("side", "remote")]].concat(),
            256.0,
        );
        assert_eq!(
            count_metrics(
                families.get("grpc_channelz_socket_streams_total"),
                &[socket10_labels.as_slice(), &[("type", "started")]].concat(),
            ),
            1
        );
        assert_value(
            &families,
            "grpc_channelz_socket_streams_total",
            &[
                ("id", "11"),
                ("local", "/tmp/channelz.sock"),
                ("remote", "passthrough:///peer"),
                ("type", "failed"),
            ],
            1.0,
        );
        for absent in [
            "grpc_channelz_socket_active_streams",
            "grpc_channelz_socket_failed_stream_ratio",
            "grpc_channelz_socket_message_receive_lag_seconds",
        ] {
            assert!(!families.contains_key(absent));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_options_emit_one_hot_state_and_trace_statistics() {
        let mut root = channel(1, "cluster-a");
        root.data = Some(ChannelData {
            target: "cluster-a".to_owned(),
            calls_started: 1,
            state: Some(ChannelConnectivityState {
                state: channel_connectivity_state::State::Ready as i32,
            }),
            trace: Some(ChannelTrace {
                num_events_logged: 2,
                creation_timestamp: Some(timestamp(1_699_999_700)),
                events: vec![ChannelTraceEvent {
                    severity: channel_trace_event::Severity::CtWarning as i32,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        });
        let mut server = FakeChannelzServer::default();
        server.top_channels.insert(
            0,
            GetTopChannelsResponse {
                channel: vec![root],
                end: true,
            },
        );
        let families = gather(
            server,
            ChannelzCollectorOptions {
                include_channel_state: true,
                include_channel_trace: true,
                ..Default::default()
            },
        )
        .await;
        assert_value(
            &families,
            "grpc_channelz_channel_state",
            &[
                ("kind", "channel"),
                ("id", "1"),
                ("target", "cluster-a"),
                ("state", "ready"),
            ],
            1.0,
        );
        assert_value(
            &families,
            "grpc_channelz_channel_state",
            &[("id", "1"), ("state", "idle")],
            0.0,
        );
        assert_value(
            &families,
            "grpc_channelz_channel_trace_events_logged_total",
            &[("id", "1")],
            2.0,
        );
        assert_value(
            &families,
            "grpc_channelz_channel_trace_event_count",
            &[("id", "1"), ("severity", "warning")],
            1.0,
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn filter_controls_collection_and_child_walks() {
        let mut root = channel(1, "root");
        root.channel_ref.push(ChannelRef {
            channel_id: 5,
            name: String::new(),
        });
        root.subchannel_ref.push(SubchannelRef {
            subchannel_id: 2,
            name: String::new(),
        });
        let mut blocked = channel(4, "blocked");
        blocked.socket_ref.push(SocketRef {
            socket_id: 11,
            name: String::new(),
        });
        let mut child = channel(5, "child");
        child.socket_ref.push(SocketRef {
            socket_id: 13,
            name: String::new(),
        });
        let mut sub = subchannel(2, "sub");
        sub.socket_ref.push(SocketRef {
            socket_id: 10,
            name: String::new(),
        });
        let mut server = FakeChannelzServer::default();
        server.top_channels.insert(
            0,
            GetTopChannelsResponse {
                channel: vec![root, blocked],
                end: true,
            },
        );
        server.channels.insert(5, child);
        server.subchannels.insert(2, sub);
        server.sockets.insert(10, socket(10, 1));
        server.sockets.insert(11, socket(11, 1));
        server.sockets.insert(13, socket(13, 2));
        let filter: ChannelzFilter = Arc::new(|node| match node {
            ChannelzNode::Channel(channel) if channel_id(channel) == 1 => (false, true),
            ChannelzNode::Channel(channel) if channel_id(channel) == 4 => (false, false),
            ChannelzNode::Subchannel(subchannel) if subchannel_id(subchannel) == 2 => (true, false),
            _ => (true, true),
        });
        let families = gather(
            server,
            ChannelzCollectorOptions {
                filter: Some(filter),
                ..Default::default()
            },
        )
        .await;
        let calls = families.get("grpc_channelz_channel_calls_total");
        let streams = families.get("grpc_channelz_socket_streams_total");
        assert_eq!(count_metrics(calls, &[("id", "1")]), 0);
        assert_eq!(
            count_metrics(
                calls,
                &[("kind", "subchannel"), ("id", "2"), ("type", "started")]
            ),
            1
        );
        assert_eq!(count_metrics(streams, &[("id", "10")]), 0);
        assert_eq!(
            count_metrics(
                calls,
                &[("kind", "channel"), ("id", "5"), ("type", "started")]
            ),
            1
        );
        assert_eq!(
            count_metrics(streams, &[("id", "13"), ("type", "started")]),
            1
        );
        assert_eq!(count_metrics(calls, &[("id", "4")]), 0);
        assert_eq!(count_metrics(streams, &[("id", "11")]), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn socket_label_toggles_remove_local_and_remote_dimensions() {
        let mut root = channel(1, "root");
        root.socket_ref.push(SocketRef {
            socket_id: 10,
            name: String::new(),
        });
        let mut tracked = socket(10, 1);
        tracked.local = Some(tcp_address(vec![127, 0, 0, 1], 1000));
        tracked.remote = Some(tcp_address(vec![127, 0, 0, 2], 2000));
        let mut server = FakeChannelzServer::default();
        server.top_channels.insert(
            0,
            GetTopChannelsResponse {
                channel: vec![root],
                end: true,
            },
        );
        server.sockets.insert(10, tracked);
        let families = gather(
            server,
            ChannelzCollectorOptions {
                disable_local_label: true,
                disable_remote_label: true,
                ..Default::default()
            },
        )
        .await;
        let family = &families["grpc_channelz_socket_streams_total"];
        assert_value(
            &families,
            "grpc_channelz_socket_streams_total",
            &[("id", "10"), ("type", "started")],
            1.0,
        );
        assert_eq!(
            labels(&family.get_metric()[0])
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            ["id", "type"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_stream_timestamp_is_skipped() {
        let mut root = channel(1, "root");
        root.socket_ref.push(SocketRef {
            socket_id: 10,
            name: String::new(),
        });
        let mut tracked = socket(10, 1);
        tracked
            .data
            .as_mut()
            .unwrap()
            .last_local_stream_created_timestamp = Some(prost_types::Timestamp::default());
        tracked
            .data
            .as_mut()
            .unwrap()
            .last_remote_stream_created_timestamp = Some(timestamp(1_700_000_000));
        let mut server = FakeChannelzServer::default();
        server.top_channels.insert(
            0,
            GetTopChannelsResponse {
                channel: vec![root],
                end: true,
            },
        );
        server.sockets.insert(10, tracked);
        let families = gather(server, ChannelzCollectorOptions::default()).await;
        let family = families.get("grpc_channelz_socket_last_stream_created_timestamp_seconds");
        assert_eq!(count_metrics(family, &[("id", "10"), ("side", "local")]), 0);
        assert_eq!(
            count_metrics(family, &[("id", "10"), ("side", "remote")]),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_errors_are_counted_without_discarding_successful_metrics() {
        let mut root = channel(1, "root");
        root.data.as_mut().unwrap().calls_started = 1;
        root.channel_ref.push(ChannelRef {
            channel_id: 2,
            name: String::new(),
        });
        root.subchannel_ref.push(SubchannelRef {
            subchannel_id: 3,
            name: String::new(),
        });
        root.socket_ref.push(SocketRef {
            socket_id: 10,
            name: String::new(),
        });
        let mut server = FakeChannelzServer::default();
        server.top_channels.insert(
            0,
            GetTopChannelsResponse {
                channel: vec![root],
                end: true,
            },
        );
        server.channel_errors.insert(2);
        server.subchannel_errors.insert(3);
        server.socket_errors.insert(10);
        let families = gather(server, ChannelzCollectorOptions::default()).await;
        assert_value(
            &families,
            "grpc_channelz_channel_calls_total",
            &[("id", "1"), ("type", "started")],
            1.0,
        );
        for (rpc, expected) in [
            ("GetTopChannels", 0.0),
            ("GetChannel", 1.0),
            ("GetSubchannel", 1.0),
            ("GetSocket", 1.0),
        ] {
            assert_value(
                &families,
                "grpc_channelz_fetch_errors_total",
                &[("rpc", rpc)],
                expected,
            );
        }
    }
}
