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

//! Concrete tonic BatchCommands stream inside the retained transport runtime.

use std::collections::{HashMap, HashSet};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_proto::tikvpb::{tikv_client::TikvClient, BatchCommandsRequest};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::rpc::channel_pool::{ChannelPool, VersionedChannel};
use crate::rpc::forwarding;
use crate::rpc::transport_runtime::WorkerCommand;
use crate::rpc::{CompletionRequest, DirectUnaryClientError, DirectUnaryConnectionError};

use super::{
    BatchEntry, BatchGroup, BatchInflightError, BatchInflightTable, BatchRoute, BatchScheduler,
    BatchWireRequest, BatchWireResponse, OpaqueBatchCommand, PendingBatchCommand,
};

const MAX_RECV_MESSAGE_SIZE: usize = (i64::MAX as usize).saturating_sub(1);
// Pinned client-go internal/client/client.go uses dialTimeout = 5s for
// waitConnReady before BatchCommands stream creation.
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// The original once-only completion carried from admission through receive.
pub type BatchCommandCompletion = CompletionRequest<OpaqueBatchCommand, BatchInflightError>;

/// One command admitted to the retained scheduler and duplex transport.
pub type BatchCommandEntry = BatchEntry<OpaqueBatchCommand, BatchCommandCompletion>;

/// Observable identity assigned to one submitted direct or forwarded packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchPublicationReceipt {
    route: BatchRoute,
    request_ids: Vec<u64>,
}

impl BatchPublicationReceipt {
    /// Exact physical, forwarding, and stream-generation identity.
    #[must_use]
    pub const fn route(&self) -> &BatchRoute {
        &self.route
    }

    /// Scheduler IDs published before the packet was offered to tonic.
    #[must_use]
    pub fn request_ids(&self) -> &[u64] {
        &self.request_ids
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StreamKey {
    physical_address: String,
    forwarded_host: Option<String>,
}

impl StreamKey {
    fn new(address: &str, forwarded_host: Option<&str>) -> Self {
        Self {
            physical_address: address.to_owned(),
            forwarded_host: forwarded_host.map(str::to_owned),
        }
    }

    fn route(&self, generation: u64) -> BatchRoute {
        match &self.forwarded_host {
            Some(host) => {
                BatchRoute::forwarded(self.physical_address.clone(), host.clone(), generation)
            }
            None => BatchRoute::direct(self.physical_address.clone(), generation),
        }
    }
}

struct ActiveStream {
    route: BatchRoute,
    connection_version: u64,
    terminal: Arc<Mutex<Option<BatchInflightError>>>,
    outbound: mpsc::UnboundedSender<BatchCommandsRequest>,
}

/// Stream-map bookkeeping returned after the receive task retires in-flight work.
pub(in crate::rpc) enum BatchStreamEvent {
    Retired { route: BatchRoute },
}

struct PreparedBatch {
    commands: Vec<OpaqueBatchCommand>,
    request_ids: Vec<u64>,
    pending: Vec<PendingBatchCommand>,
}

impl PreparedBatch {
    fn from_group(group: BatchGroup<OpaqueBatchCommand, BatchCommandCompletion>) -> Self {
        let mut commands = Vec::with_capacity(group.len());
        let mut request_ids = Vec::with_capacity(group.len());
        let mut pending = Vec::with_capacity(group.len());
        for scheduled in group.into_entries() {
            request_ids.push(scheduled.request_id());
            let (command, request) = PendingBatchCommand::from_scheduled(scheduled);
            commands.push(command);
            pending.push(request);
        }
        Self {
            commands,
            request_ids,
            pending,
        }
    }

    fn fail(self, error: BatchInflightError) {
        for request in self.pending {
            request.fail(error.clone());
        }
    }
}

/// Scheduler, route generation, stream, and pending ownership for one worker.
pub(in crate::rpc) struct BatchTransportState {
    scheduler: BatchScheduler<OpaqueBatchCommand, BatchCommandCompletion>,
    streams: HashMap<StreamKey, ActiveStream>,
    generations: HashMap<StreamKey, u64>,
    reconnect_budget: HashSet<StreamKey>,
    inflight: Arc<Mutex<BatchInflightTable>>,
    shutdown: watch::Receiver<bool>,
}

impl BatchTransportState {
    pub(in crate::rpc) fn new(shutdown: watch::Receiver<bool>) -> Self {
        Self {
            scheduler: BatchScheduler::new(),
            streams: HashMap::new(),
            generations: HashMap::new(),
            reconnect_budget: HashSet::new(),
            inflight: Arc::new(Mutex::new(BatchInflightTable::new())),
            shutdown,
        }
    }

    pub(in crate::rpc) async fn submit(
        &mut self,
        channels: &mut ChannelPool,
        runtime: &tokio::runtime::Runtime,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        commands: &std_mpsc::Sender<WorkerCommand>,
    ) -> Vec<BatchPublicationReceipt> {
        for entry in entries {
            self.scheduler.push(entry);
        }
        let groups = self.scheduler.build_with_limit(usize::MAX).into_parts();
        let mut receipts =
            Vec::with_capacity(groups.forwarded.len() + usize::from(groups.direct.is_some()));
        if let Some(group) = groups.direct {
            if let Some(receipt) = self
                .send_group(channels, runtime, address, None, group, commands)
                .await
            {
                receipts.push(receipt);
            }
        }
        for (forwarded_host, group) in groups.forwarded {
            if let Some(receipt) = self
                .send_group(
                    channels,
                    runtime,
                    address,
                    Some(forwarded_host.as_str()),
                    group,
                    commands,
                )
                .await
            {
                receipts.push(receipt);
            }
        }
        receipts
    }

    async fn send_group(
        &mut self,
        channels: &mut ChannelPool,
        runtime: &tokio::runtime::Runtime,
        address: &str,
        forwarded_host: Option<&str>,
        group: BatchGroup<OpaqueBatchCommand, BatchCommandCompletion>,
        commands: &std_mpsc::Sender<WorkerCommand>,
    ) -> Option<BatchPublicationReceipt> {
        let prepared = PreparedBatch::from_group(group);
        let key = StreamKey::new(address, forwarded_host);
        let retired_route = self.streams.get(&key).and_then(|stream| {
            stream
                .terminal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .map(|_| stream.route.clone())
        });
        if let Some(retired_route) = retired_route {
            self.remove_stream_if_current(&key, &retired_route);
            self.reconnect_budget.remove(&key);
        }

        if !self.streams.contains_key(&key) {
            if let Err(error) = self
                .recreate_stream(channels, runtime, key.clone(), commands)
                .await
            {
                prepared.fail(BatchInflightError::Transport(error));
                return None;
            }
        }

        let stream = self
            .streams
            .get(&key)
            .expect("stream recreation succeeded before publication");
        let route = stream.route.clone();
        let terminal = Arc::clone(&stream.terminal);
        let outbound = stream.outbound.clone();
        let connection_version = stream.connection_version;
        let mut terminal_guard = terminal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(error) = terminal_guard.as_ref().cloned() {
            drop(terminal_guard);
            self.remove_stream_if_current(&key, &route);
            self.reconnect_budget.remove(&key);
            prepared.fail(error);
            return None;
        }

        let PreparedBatch {
            commands: batch_commands,
            request_ids,
            pending,
        } = prepared;
        let request =
            match BatchWireRequest::new(batch_commands, request_ids.clone(), client_send_time_ns())
            {
                Ok(request) => request,
                Err(error) => {
                    drop(terminal_guard);
                    for request in pending {
                        request.fail(BatchInflightError::Protocol(error.clone()));
                    }
                    return None;
                }
            };
        if BatchInflightTable::publish_shared(&self.inflight, route.clone(), pending).is_err() {
            drop(terminal_guard);
            return None;
        }
        let send_error = outbound.send(request.into_proto()).err().map(|_| {
            BatchInflightError::Transport(stream_error(
                address,
                connection_version,
                "BatchCommands request stream closed",
            ))
        });
        if let Some(error) = &send_error {
            *terminal_guard = Some(error.clone());
            self.inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .fail_route(&route, error.clone());
        }
        drop(terminal_guard);
        let receipt = BatchPublicationReceipt {
            route: route.clone(),
            request_ids,
        };

        if send_error.is_some() {
            self.reconnect_budget.remove(&key);
            self.remove_stream_if_current(&key, &route);
        } else {
            self.reconnect_budget.insert(key);
        }
        Some(receipt)
    }

    fn route_for_submission(&mut self, key: &StreamKey) -> BatchRoute {
        if let Some(stream) = self.streams.get(key) {
            return stream.route.clone();
        }
        let generation = self
            .generations
            .entry(key.clone())
            .and_modify(|generation| *generation = generation.saturating_add(1))
            .or_insert(1);
        key.route(*generation)
    }

    pub(in crate::rpc) async fn handle_event(
        &mut self,
        channels: &mut ChannelPool,
        runtime: &tokio::runtime::Runtime,
        commands: &std_mpsc::Sender<WorkerCommand>,
        event: BatchStreamEvent,
    ) {
        let BatchStreamEvent::Retired { route } = event;
        let key = StreamKey::new(route.physical_address(), route.forwarded_host());
        if self.remove_stream_if_current(&key, &route) && self.reconnect_budget.remove(&key) {
            let _ = self.recreate_stream(channels, runtime, key, commands).await;
        }
    }

    async fn recreate_stream(
        &mut self,
        channels: &mut ChannelPool,
        runtime: &tokio::runtime::Runtime,
        key: StreamKey,
        commands: &std_mpsc::Sender<WorkerCommand>,
    ) -> Result<(), DirectUnaryClientError> {
        // PARTIAL: open is bounded and shutdown-cancelable, but prolonged
        // outage retry/backoff policy remains above this transport slice.
        if self.streams.contains_key(&key) {
            return Ok(());
        }
        let route = self.route_for_submission(&key);
        let selected = channels.get_or_create(&key.physical_address, runtime)?;
        let stream = open_stream(
            &key,
            &route,
            selected,
            commands.clone(),
            Arc::clone(&self.inflight),
            self.shutdown.clone(),
        )
        .await?;
        self.streams.insert(key, stream);
        Ok(())
    }

    pub(in crate::rpc) fn close_address(&mut self, address: &str) {
        self.streams
            .retain(|key, _| key.physical_address != address);
        self.reconnect_budget
            .retain(|key| key.physical_address != address);
        self.inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .fail_address(
                address,
                BatchInflightError::Transport(DirectUnaryClientError::Closed),
            );
    }

    pub(in crate::rpc) fn close(&mut self) {
        self.scheduler.cancel_all(BatchInflightError::Transport(
            DirectUnaryClientError::Closed,
        ));
        self.streams.clear();
        self.reconnect_budget.clear();
        self.inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .close();
    }

    pub(in crate::rpc) fn active_generation(
        &self,
        address: &str,
        forwarded_host: Option<&str>,
    ) -> Option<u64> {
        self.streams
            .get(&StreamKey::new(address, forwarded_host))
            .map(|stream| stream.route.generation())
    }

    fn remove_stream_if_current(&mut self, key: &StreamKey, route: &BatchRoute) -> bool {
        if self
            .streams
            .get(key)
            .is_some_and(|stream| stream.route == *route)
        {
            self.streams.remove(key);
            return true;
        }
        false
    }
}

async fn open_stream(
    key: &StreamKey,
    route: &BatchRoute,
    selected: VersionedChannel,
    commands: std_mpsc::Sender<WorkerCommand>,
    inflight: Arc<Mutex<BatchInflightTable>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<ActiveStream, DirectUnaryClientError> {
    let (outbound, receiver) = mpsc::unbounded_channel();
    let mut request = tonic::Request::new(UnboundedReceiverStream::new(receiver));
    forwarding::attach_forwarded_host(&mut request, key.forwarded_host.as_deref())?;
    let version = selected.version;
    let mut client =
        TikvClient::new(selected.channel).max_decoding_message_size(MAX_RECV_MESSAGE_SIZE);
    if *shutdown.borrow() {
        return Err(DirectUnaryClientError::Closed);
    }
    let response = tokio::select! {
        _ = shutdown.changed() => return Err(DirectUnaryClientError::Closed),
        result = tokio::time::timeout(STREAM_OPEN_TIMEOUT, client.batch_commands(request)) => {
            match result {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    return Err(stream_error(&key.physical_address, version, error));
                }
                Err(_) => {
                    return Err(stream_open_timeout(&key.physical_address, version));
                }
            }
        }
    };
    let mut inbound = response.into_inner();
    let receive_route = route.clone();
    let receive_address = key.physical_address.clone();
    let terminal = Arc::new(Mutex::new(None));
    let receive_terminal = Arc::clone(&terminal);
    tokio::spawn(async move {
        loop {
            match inbound.message().await {
                Ok(Some(response)) => match BatchWireResponse::try_from(response) {
                    Ok(response) => {
                        let _ = inflight
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .receive(&receive_route, response);
                    }
                    Err(error) => {
                        retire_stream(
                            &receive_terminal,
                            &inflight,
                            &receive_route,
                            BatchInflightError::Protocol(error),
                        );
                        let _ =
                            commands.send(WorkerCommand::BatchEvent(BatchStreamEvent::Retired {
                                route: receive_route,
                            }));
                        return;
                    }
                },
                Ok(None) => {
                    let error = stream_error(
                        &receive_address,
                        version,
                        "BatchCommands response stream closed",
                    );
                    retire_stream(
                        &receive_terminal,
                        &inflight,
                        &receive_route,
                        BatchInflightError::Transport(error),
                    );
                    let _ = commands.send(WorkerCommand::BatchEvent(BatchStreamEvent::Retired {
                        route: receive_route,
                    }));
                    return;
                }
                Err(error) => {
                    let error = stream_error(&receive_address, version, error);
                    retire_stream(
                        &receive_terminal,
                        &inflight,
                        &receive_route,
                        BatchInflightError::Transport(error),
                    );
                    let _ = commands.send(WorkerCommand::BatchEvent(BatchStreamEvent::Retired {
                        route: receive_route,
                    }));
                    return;
                }
            }
        }
    });
    Ok(ActiveStream {
        route: route.clone(),
        connection_version: version,
        terminal,
        outbound,
    })
}

fn stream_open_timeout(address: &str, version: u64) -> DirectUnaryClientError {
    DirectUnaryClientError::Timeout {
        connection: DirectUnaryConnectionError::local_deadline(
            address,
            version,
            "BatchCommands stream open timed out".to_owned(),
        ),
        timeout_ms: u64::try_from(STREAM_OPEN_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
    }
}

fn retire_stream(
    terminal: &Arc<Mutex<Option<BatchInflightError>>>,
    inflight: &Arc<Mutex<BatchInflightTable>>,
    route: &BatchRoute,
    error: BatchInflightError,
) {
    let mut terminal = terminal
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if terminal.is_some() {
        return;
    }
    *terminal = Some(error.clone());
    inflight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .fail_route(route, error);
}

fn stream_error(
    address: &str,
    version: u64,
    error: impl std::fmt::Display,
) -> DirectUnaryClientError {
    DirectUnaryClientError::Connection(DirectUnaryConnectionError::connection(
        address,
        version,
        error.to_string(),
    ))
}

fn client_send_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}
