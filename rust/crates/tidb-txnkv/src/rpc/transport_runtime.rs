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

//! Channel pools and their asynchronous transport lifecycle owner.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tidb_pd_client::ClusterSecurity;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::execution::{wait, ConnectionRuntime};

use crate::region::StoreLiveness;

use super::batch::{
    BatchCommandEntry, BatchPublicationReceipt, BatchStreamEvent, BatchSubmission,
    BatchTransportState,
};
use super::channel_pool::ChannelPool;
use super::liveness::check_liveness;
use super::unary::{prepare_unary, RawUnaryRequest, RawUnaryResponse, UnaryCallContext};
use super::{DirectUnaryClientError, TransportShutdownError};

mod batching;

/// Env-gated admission diagnostics (`TIKV_ADMISSION_LOG=1`). Purely additive:
/// relaxed atomics on the measured paths plus one stderr dumper thread.
pub mod admit_diag {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    static ADMIT_WAIT_US: AtomicU64 = AtomicU64::new(0);
    static ADMIT_COUNT: AtomicU64 = AtomicU64::new(0);
    static ADMIT_MAX_US: AtomicU64 = AtomicU64::new(0);
    static WORKER_SUBMIT_US: AtomicU64 = AtomicU64::new(0);
    static WORKER_SUBMIT_COUNT: AtomicU64 = AtomicU64::new(0);
    static WORKER_SUBMIT_MAX_US: AtomicU64 = AtomicU64::new(0);
    static WORKER_EVENT_US: AtomicU64 = AtomicU64::new(0);
    static WORKER_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

    fn bump(total: &AtomicU64, count: &AtomicU64, max: &AtomicU64, us: u64) {
        total.fetch_add(us, Ordering::Relaxed);
        count.fetch_add(1, Ordering::Relaxed);
        max.fetch_max(us, Ordering::Relaxed);
    }

    /// Client side: full wait from command send to the worker's reply.
    pub fn note_admit_wait(wait: std::time::Duration) {
        if !enabled() {
            return;
        }
        bump(
            &ADMIT_WAIT_US,
            &ADMIT_COUNT,
            &ADMIT_MAX_US,
            wait.as_micros() as u64,
        );
        start_dumper();
    }

    /// Worker side: one batch publication duration.
    pub fn note_worker_submit(elapsed: std::time::Duration) {
        if !enabled() {
            return;
        }
        bump(
            &WORKER_SUBMIT_US,
            &WORKER_SUBMIT_COUNT,
            &WORKER_SUBMIT_MAX_US,
            elapsed.as_micros() as u64,
        );
    }

    /// Worker side: one stream retirement/recreation duration.
    pub fn note_worker_event(elapsed: std::time::Duration) {
        if !enabled() {
            return;
        }
        bump(
            &WORKER_EVENT_US,
            &WORKER_EVENT_COUNT,
            &WORKER_EVENT_MAX_US,
            elapsed.as_micros() as u64,
        );
    }

    static WORKER_EVENT_MAX_US: AtomicU64 = AtomicU64::new(0);

    fn enabled() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| std::env::var_os("TIKV_ADMISSION_LOG").is_some())
    }

    fn snapshot_line() -> String {
        let aw = ADMIT_WAIT_US.swap(0, Ordering::Relaxed);
        let ac = ADMIT_COUNT.swap(0, Ordering::Relaxed);
        let am = ADMIT_MAX_US.swap(0, Ordering::Relaxed);
        let ws = WORKER_SUBMIT_US.swap(0, Ordering::Relaxed);
        let wc = WORKER_SUBMIT_COUNT.swap(0, Ordering::Relaxed);
        let wm = WORKER_SUBMIT_MAX_US.swap(0, Ordering::Relaxed);
        let we = WORKER_EVENT_US.swap(0, Ordering::Relaxed);
        let ec = WORKER_EVENT_COUNT.swap(0, Ordering::Relaxed);
        let em = WORKER_EVENT_MAX_US.swap(0, Ordering::Relaxed);
        let avg = |t: u64, c: u64| if c > 0 { t / c } else { 0 };
        format!(
            "ADMISSION admit_n={ac} admit_avg_us={} admit_max_us={am} wsubmit_n={wc} wsubmit_avg_us={} wsubmit_max_us={wm} wevent_n={ec} wevent_avg_us={} wevent_max_us={em}",
            avg(aw, ac),
            avg(ws, wc),
            avg(we, ec),
        )
    }

    fn start_dumper() {
        static START: OnceLock<()> = OnceLock::new();
        START.get_or_init(|| {
            if !enabled() {
                return;
            }
            let period_ms = std::env::var("TIKV_ADMISSION_LOG_PERIOD_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(2_000)
                .clamp(200, 60_000);
            let spawned = std::thread::Builder::new()
                .name("admission-log".to_owned())
                .spawn(move || loop {
                    std::thread::sleep(std::time::Duration::from_millis(period_ms));
                    eprintln!("{}", snapshot_line());
                });
            if let Err(err) = spawned {
                eprintln!("admission-log failed to spawn: {err}");
            }
        });
    }
}

pub(super) enum WorkerCommand {
    UnarySend {
        address: String,
        request: RawUnaryRequest,
        call: UnaryCallContext,
        reply: oneshot::Sender<Result<RawUnaryResponse, DirectUnaryClientError>>,
    },
    BatchSubmit {
        address: String,
        entries: Vec<BatchCommandEntry>,
        call: Option<UnaryCallContext>,
        reply: Option<oneshot::Sender<Vec<BatchPublicationReceipt>>>,
    },
    BatchEvent(BatchStreamEvent),
    CloseAddress {
        address: String,
        reply: oneshot::Sender<()>,
    },
    CloseAddressVersion {
        address: String,
        version: u64,
        reply: oneshot::Sender<()>,
    },
    Liveness {
        address: String,
        timeout: Duration,
        reply: oneshot::Sender<StoreLiveness>,
    },
    Inspect {
        address: String,
        reply: oneshot::Sender<(Option<u64>, usize)>,
    },
    InspectBatch {
        address: String,
        forwarded_host: Option<String>,
        reply: oneshot::Sender<(Option<u64>, u64)>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

/// Unique owner of the one retained transport worker.
pub(super) struct TransportRuntime {
    commands: Option<mpsc::UnboundedSender<WorkerCommand>>,
    worker: Option<JoinHandle<()>>,
    cancellation: TransportShutdownCancellation,
    io: Vec<ConnectionRuntime>,
}

/// Cloneable request capability for the retained transport worker.
///
/// This handle deliberately contains neither the worker join handle nor its
/// shutdown cancellation. Dropping every request handle does not stop the
/// worker, and no request handle can join it.
#[derive(Clone)]
pub(super) struct TransportHandle {
    commands: mpsc::UnboundedSender<WorkerCommand>,
}

/// Cloneable direct cancellation for interrupting a blocked transport open.
#[derive(Clone)]
pub struct TransportShutdownCancellation {
    shutdown: watch::Sender<bool>,
}

impl TransportShutdownCancellation {
    /// Interrupts runtime-owned operations before orderly close is queued.
    pub fn cancel(&self) {
        let _ = self.shutdown.send(true);
    }

    pub(super) fn detached() -> Self {
        let (shutdown, _) = watch::channel(false);
        Self { shutdown }
    }
}

impl TransportRuntime {
    pub(super) fn new(
        security: Arc<ClusterSecurity>,
        connection_count: NonZeroUsize,
    ) -> Result<Self, DirectUnaryClientError> {
        let runtime = super::execution_runtime().map_err(DirectUnaryClientError::Runtime)?;
        let io = (0..connection_count.get())
            .map(|_| ConnectionRuntime::new())
            .collect::<Result<Vec<_>, _>>()
            .map_err(DirectUnaryClientError::Runtime)?;
        let (commands, receiver) = mpsc::unbounded_channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let worker = runtime.spawn(run_worker(
            io.iter().map(|driver| driver.handle.clone()).collect(),
            receiver,
            commands.clone(),
            shutdown_rx,
            security,
        ));
        Ok(Self {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: TransportShutdownCancellation { shutdown },
            io,
        })
    }

    pub(super) fn handle(&self) -> TransportHandle {
        TransportHandle {
            commands: self
                .commands
                .as_ref()
                .expect("live transport owner must retain its command sender")
                .clone(),
        }
    }

    pub(super) fn shutdown_cancellation(&self) -> TransportShutdownCancellation {
        self.cancellation.clone()
    }

    pub(super) fn shutdown(&mut self) -> Result<(), DirectUnaryClientError> {
        self.cancellation.cancel();
        let commands = self.commands.take();
        let worker = self.worker.take();
        let mut shutdown_errors = wait(async {
            let mut shutdown_errors = Vec::new();
            if let Some(commands) = commands {
                let (reply, response) = oneshot::channel();
                match commands.send(WorkerCommand::Close { reply }) {
                    Ok(()) => {
                        if response.await.is_err() {
                            shutdown_errors.push(TransportShutdownError::CloseAcknowledgementLost);
                        }
                    }
                    Err(_) => shutdown_errors.push(TransportShutdownError::CommandChannelClosed),
                }
            }
            if let Some(worker) = worker {
                if let Err(error) = worker.await {
                    shutdown_errors.push(if error.is_panic() {
                        TransportShutdownError::WorkerPanicked {
                            message: panic_message(&error.into_panic()).to_owned(),
                        }
                    } else {
                        TransportShutdownError::WorkerCancelled
                    });
                }
            }
            shutdown_errors
        });
        for mut driver in self.io.drain(..) {
            if let Err(panic) = driver.shutdown() {
                shutdown_errors.push(TransportShutdownError::WorkerPanicked {
                    message: format!("connection I/O: {}", panic_message(&panic)),
                });
            }
        }
        match shutdown_errors.len() {
            0 => Ok(()),
            1 => Err(DirectUnaryClientError::Shutdown(
                shutdown_errors.pop().unwrap(),
            )),
            _ => Err(DirectUnaryClientError::Shutdown(
                TransportShutdownError::Multiple(shutdown_errors),
            )),
        }
    }
}

impl Drop for TransportRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// On-demand barrier for an observer that needs identity before a response.
/// Ordinary requests read their entry's publication state without a second ACK.
pub(super) struct PublicationBarrier {
    handle: TransportHandle,
    address: String,
}

impl PublicationBarrier {
    pub(super) fn new(handle: TransportHandle, address: &str) -> Self {
        Self {
            handle,
            address: address.to_owned(),
        }
    }

    pub(super) fn wait(self) {
        // Inspect is ordered after admission and flushes the address collector.
        // It does not create/recreate a connection or wait for a response.
        self.handle.inspect(&self.address);
    }
}

impl TransportHandle {
    pub(super) fn unary_send(
        &self,
        address: &str,
        request: RawUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<RawUnaryResponse, DirectUnaryClientError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(WorkerCommand::UnarySend {
                address: address.to_owned(),
                request,
                call: call.clone(),
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        wait(response).unwrap_or(Err(DirectUnaryClientError::Closed))
    }

    pub(super) fn batch_submit(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        let started = std::time::Instant::now();
        let response = self.batch_submit_with_receipts(address, entries, None)?;
        let receipts = wait(response).map_err(|_| DirectUnaryClientError::Closed);
        admit_diag::note_admit_wait(started.elapsed());
        receipts
    }

    fn batch_submit_with_receipts(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: Option<UnaryCallContext>,
    ) -> Result<oneshot::Receiver<Vec<BatchPublicationReceipt>>, DirectUnaryClientError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(WorkerCommand::BatchSubmit {
                address: address.to_owned(),
                entries,
                call,
                reply: Some(reply),
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        Ok(response)
    }

    /// Go sendBatchRequest queues the entry and waits only for its response.
    /// No publication acknowledgement is allocated or sent on this path.
    pub(super) fn batch_submit_with_call(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: &UnaryCallContext,
    ) -> Result<PublicationBarrier, DirectUnaryClientError> {
        self.commands
            .send(WorkerCommand::BatchSubmit {
                address: address.to_owned(),
                entries,
                call: Some(call.clone()),
                reply: None,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        Ok(PublicationBarrier::new(self.clone(), address))
    }

    pub(super) fn close_address(&self, address: &str) -> Result<(), DirectUnaryClientError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(WorkerCommand::CloseAddress {
                address: address.to_owned(),
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        wait(response).map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn close_address_version(
        &self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(WorkerCommand::CloseAddressVersion {
                address: address.to_owned(),
                version,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        wait(response).map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(WorkerCommand::Liveness {
                address: address.to_owned(),
                timeout,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        wait(response).map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn inspect(&self, address: &str) -> (Option<u64>, usize) {
        let (reply, response) = oneshot::channel();
        if self
            .commands
            .send(WorkerCommand::Inspect {
                address: address.to_owned(),
                reply,
            })
            .is_err()
        {
            return (None, 0);
        }
        wait(response).unwrap_or((None, 0))
    }

    pub(super) fn inspect_batch(
        &self,
        address: &str,
        forwarded_host: Option<&str>,
    ) -> (Option<u64>, u64) {
        let (reply, response) = oneshot::channel();
        if self
            .commands
            .send(WorkerCommand::InspectBatch {
                address: address.to_owned(),
                forwarded_host: forwarded_host.map(str::to_owned),
                reply,
            })
            .is_err()
        {
            return (None, 0);
        }
        wait(response).unwrap_or((None, 0))
    }
}

/// Env gate for wire-level stall tracing (`TIKV_QUERY_TRACE`).
pub(in crate::rpc) fn wtrace_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("TIKV_QUERY_TRACE").is_some())
}

struct TransportConnection {
    channels: ChannelPool,
    batch: BatchTransportState,
    runtime: tokio::runtime::Handle,
}

fn select_connection(
    cursors: &mut HashMap<String, usize>,
    address: &str,
    count: NonZeroUsize,
) -> usize {
    let cursor = cursors.entry(address.to_owned()).or_default();
    let selected = *cursor;
    *cursor = (*cursor + 1) % count.get();
    selected
}

fn publish_batch(
    connections: &mut [TransportConnection],
    cursors: &mut HashMap<String, usize>,
    address: &str,
    submissions: Vec<BatchSubmission>,
    commands: &mpsc::UnboundedSender<WorkerCommand>,
) {
    let started = std::time::Instant::now();
    let count = NonZeroUsize::new(connections.len()).expect("nonempty connection fleet");
    let index = select_connection(cursors, address, count);
    let connection = &mut connections[index];
    // Publication only queues a packet; the retained stream task owns I/O.
    // Go getClientAndSend likewise publishes directly from the send loop.
    connection.batch.submit(
        &mut connection.channels,
        &connection.runtime,
        address,
        submissions,
        commands,
    );
    admit_diag::note_worker_submit(started.elapsed());
}

async fn run_worker(
    runtimes: Vec<tokio::runtime::Handle>,
    mut receiver: mpsc::UnboundedReceiver<WorkerCommand>,
    commands: mpsc::UnboundedSender<WorkerCommand>,
    shutdown: watch::Receiver<bool>,
    security: Arc<ClusterSecurity>,
) {
    let versions = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let request_ids = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let connection_count = NonZeroUsize::new(runtimes.len()).expect("nonempty connection fleet");
    let mut connections: Vec<_> = runtimes
        .into_iter()
        .map(|runtime| TransportConnection {
            runtime,
            channels: ChannelPool::with_security(Arc::clone(&security), Arc::clone(&versions)),
            batch: BatchTransportState::new(shutdown.clone(), Arc::clone(&request_ids)),
        })
        .collect();
    let result = std::panic::AssertUnwindSafe(async {
        let mut cursors = HashMap::new();
        let mut pending = None;
        let mut collectors = batching::Collectors::default();
        let mut timer: Option<(std::time::Instant, futures_timer::Delay)> = None;
        loop {
            let now = std::time::Instant::now();
            if let Some((address, submissions)) = collectors.take_due(now) {
                publish_batch(
                    &mut connections,
                    &mut cursors,
                    &address,
                    submissions,
                    &commands,
                );
                continue;
            }
            let command = if let Some(command) = pending.take() {
                command
            } else if let Some(deadline) = collectors.next_deadline() {
                // Go uses a sub-millisecond batch deadline. Tokio Sleep rounds to
                // milliseconds; this native timer retains the Instant deadline.
                tokio::select! {
                    biased;
                    command = receiver.recv() => match command {
                        Some(command) => command,
                        None => break None,
                    },
                    () = async {
                        // Poll ready commands before registering a timer. The
                        // loop checks expired deadlines before every command;
                        // timer I/O is only needed when collection must wait.
                        let delay = match &mut timer {
                            Some((armed, delay)) => {
                                if *armed != deadline {
                                    delay.reset(deadline.saturating_duration_since(std::time::Instant::now()));
                                    *armed = deadline;
                                }
                                delay
                            }
                            slot @ None => &mut slot.insert((
                                deadline,
                                futures_timer::Delay::new(deadline.saturating_duration_since(std::time::Instant::now())),
                            )).1,
                        };
                        delay.await;
                    } => continue,
                }
            } else {
                timer = None;
                let Some(command) = receiver.recv().await else {
                    break None;
                };
                command
            };
            // A waiting batch is address-local. Preserve preceding publication
            // before observation/invalidation, without holding other stores behind
            // its collection timer. Retirement events still name exact channels.
            match &command {
                WorkerCommand::UnarySend { address, .. }
                | WorkerCommand::CloseAddress { address, .. }
                | WorkerCommand::CloseAddressVersion { address, .. }
                | WorkerCommand::Liveness { address, .. }
                | WorkerCommand::Inspect { address, .. }
                | WorkerCommand::InspectBatch { address, .. } => {
                    if let Some(submissions) = collectors.finish_address(address) {
                        publish_batch(
                            &mut connections,
                            &mut cursors,
                            address,
                            submissions,
                            &commands,
                        );
                    }
                }
                WorkerCommand::Close { .. } => {
                    for (address, submissions) in collectors.finish_all() {
                        publish_batch(
                            &mut connections,
                            &mut cursors,
                            &address,
                            submissions,
                            &commands,
                        );
                    }
                }
                _ => {}
            }
            match command {
                WorkerCommand::UnarySend {
                    address,
                    request,
                    call,
                    reply,
                } => {
                    let index = select_connection(&mut cursors, &address, connection_count);
                    let connection = &mut connections[index];
                    match prepare_unary(
                        &connection.runtime,
                        &mut connection.channels,
                        &address,
                        request,
                        &call,
                    ) {
                        Ok(prepared) => {
                            prepared.spawn(reply, shutdown.clone());
                        }
                        Err(error) => {
                            let _ = reply.send(Err(error));
                        }
                    }
                }
                WorkerCommand::BatchSubmit {
                    address,
                    entries,
                    call,
                    reply,
                } => {
                    let submissions = batching::collect(
                        &address,
                        BatchSubmission {
                            entries,
                            call,
                            reply,
                        },
                        &mut receiver,
                        &mut pending,
                    );
                    match collectors.push(&address, submissions, std::time::Instant::now()) {
                        batching::Admission::Publish(submissions) => {
                            publish_batch(
                                &mut connections,
                                &mut cursors,
                                &address,
                                submissions,
                                &commands,
                            );
                        }
                        batching::Admission::Yield => {
                            // Go fetchMorePendingRequests yields once and drains
                            // again. No synthetic command or flush token is needed.
                            tokio::task::yield_now().await;
                            if let Some(submissions) =
                                collectors.finish_turn(&address, &mut receiver, &mut pending)
                            {
                                publish_batch(
                                    &mut connections,
                                    &mut cursors,
                                    &address,
                                    submissions,
                                    &commands,
                                );
                            }
                        }
                        batching::Admission::Pending => {}
                    }
                }
                WorkerCommand::BatchEvent(event) => {
                    let event_started = std::time::Instant::now();
                    let BatchStreamEvent::Retired { route } = &event;
                    // Channel versions are unique within this owner. A stale event
                    // after exact invalidation cannot match another connection.
                    if let Some(connection) = connections.iter_mut().find(|connection| {
                        connection.channels.version(route.physical_address())
                            == Some(route.physical_channel_version())
                    }) {
                        connection.batch.handle_event(
                            &mut connection.channels,
                            &connection.runtime,
                            &commands,
                            event,
                        );
                    }
                    admit_diag::note_worker_event(event_started.elapsed());
                }
                WorkerCommand::CloseAddress { address, reply } => {
                    for connection in &mut connections {
                        if let Some(channel) = connection.channels.close_address(&address).await {
                            connection.batch.close_physical_channel(&channel);
                        }
                    }
                    cursors.remove(&address);
                    collectors.remove(&address);
                    let _ = reply.send(());
                }
                WorkerCommand::CloseAddressVersion {
                    address,
                    version,
                    reply,
                } => {
                    for connection in &mut connections {
                        if let Some(channel) = connection
                            .channels
                            .close_address_version(&address, version)
                            .await
                        {
                            connection.batch.close_physical_channel(&channel);
                        }
                    }
                    let _ = reply.send(());
                }
                WorkerCommand::Liveness {
                    address,
                    timeout,
                    reply,
                } => {
                    let _ = reply.send(
                        check_liveness(&connections[0].runtime, &address, timeout, &security).await,
                    );
                }
                WorkerCommand::Inspect { address, reply } => {
                    let version = connections
                        .iter()
                        .filter_map(|connection| connection.channels.version(&address))
                        .max();
                    let addresses: std::collections::HashSet<_> = connections
                        .iter()
                        .flat_map(|connection| connection.channels.addresses())
                        .collect();
                    let _ = reply.send((version, addresses.len()));
                }
                WorkerCommand::InspectBatch {
                    address,
                    forwarded_host,
                    reply,
                } => {
                    let observation = connections
                        .iter()
                        .map(|connection| {
                            connection
                                .batch
                                .inspect(&address, forwarded_host.as_deref())
                        })
                        .fold((None, 0), |(generation, watermark), (next, id)| {
                            (generation.max(next), watermark.max(id))
                        });
                    let _ = reply.send(observation);
                }
                WorkerCommand::Close { reply } => break Some(reply),
            }
        }
    })
    .catch_unwind()
    .await;
    // Normal close, receiver termination and panic all retire the same owners
    // before acknowledgment or join reports completion.
    for connection in &mut connections {
        connection.batch.close();
        connection.channels.close().await;
    }
    match result {
        Ok(Some(reply)) => {
            let _ = reply.send(());
        }
        Ok(None) => {}
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn panic_message<'a>(panic: &'a Box<dyn std::any::Any + Send + 'static>) -> &'a str {
    panic
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_submissions_share_a_batch_without_crossing_lifecycle_barriers() {
        use crate::rpc::batch::{BatchCommandTag, OpaqueBatchCommand};
        use crate::rpc::{completion_pair, CompletionRunLoop};

        let (commands, receiver) = mpsc::unbounded_channel();
        let handle = TransportHandle {
            commands: commands.clone(),
        };
        let mut progress = Vec::new();
        let mut pending = Vec::new();
        let mut receipts = Vec::new();
        for index in 0..5 {
            if index == 2 {
                let (reply, _) = oneshot::channel();
                commands
                    .send(WorkerCommand::Inspect {
                        address: "invalid address".to_owned(),
                        reply,
                    })
                    .unwrap();
            }
            let (completion, pull) = completion_pair(CompletionRunLoop::new(), || {});
            let entry = BatchCommandEntry::new(
                OpaqueBatchCommand::new(BatchCommandTag::Empty, vec![index]),
                completion.into(),
            );
            progress.push(entry.progress());
            pending.push(pull);
            let address = if index == 3 {
                "another invalid address"
            } else {
                "invalid address"
            };
            receipts.push(
                handle
                    .batch_submit_with_receipts(address, vec![entry], None)
                    .unwrap(),
            );
        }
        let (reply, _) = oneshot::channel();
        commands.send(WorkerCommand::Close { reply }).unwrap();
        let (_, shutdown) = watch::channel(false);
        wait(super::super::execution_runtime().unwrap().spawn(run_worker(
            vec![super::super::execution_runtime().unwrap().handle().clone()],
            receiver,
            commands,
            shutdown,
            Arc::new(ClusterSecurity::default()),
        )))
        .unwrap();

        // Selection happens before the deliberately invalid endpoint fails.
        // All commands were queued before dispatch, so no timing assumptions
        // determine whether the first two callers can be coalesced.
        assert_eq!(progress[0].batch_state().unwrap().batch_size(), 2);
        assert_eq!(progress[1].batch_state().unwrap().batch_size(), 2);
        assert_eq!(progress[3].batch_state().unwrap().batch_size(), 1);
        // Go owns collection per store. Work for the other address is not a
        // barrier between entries 2 and 4; either one or two entries per batch
        // is valid depending on the adaptive interval. The explicit Inspect
        // above remains a publication barrier for entries 0/1 versus 2/4.
        for index in [2, 4] {
            let state = progress[index].batch_state().unwrap();
            assert!(matches!(state.batch_size(), 1 | 2));
            assert!(!state.shares_state_with(&progress[0].batch_state().unwrap()));
            assert!(!state.shares_state_with(&progress[3].batch_state().unwrap()));
        }
        assert!(progress[0]
            .batch_state()
            .unwrap()
            .shares_state_with(&progress[1].batch_state().unwrap()));
        for receipt in receipts {
            assert!(wait(receipt).unwrap().is_empty());
        }
        for mut pull in pending {
            assert!(pull.try_complete().unwrap().unwrap().is_err());
        }
    }

    #[test]
    fn coalesced_callers_keep_cancellation_deadlines_forwarding_and_receipts() {
        use crate::rpc::batch::{BatchCommandTag, OpaqueBatchCommand};
        use crate::rpc::{completion_pair, CompletionRunLoop};

        let (commands, receiver) = mpsc::unbounded_channel();
        let handle = TransportHandle {
            commands: commands.clone(),
        };
        let canceled = UnaryCallContext::with_timeout(Duration::from_secs(30));
        canceled.cancellation().cancel();
        let expired = UnaryCallContext::with_timeout(Duration::ZERO);
        let live = UnaryCallContext::with_timeout(Duration::from_secs(30));
        let mut progress = Vec::new();
        let mut pending = Vec::new();
        let mut receipts = Vec::new();
        for (index, call) in [None, Some(canceled), Some(expired), Some(live), None, None]
            .into_iter()
            .enumerate()
        {
            let (completion, mut pull) = completion_pair(CompletionRunLoop::new(), || {});
            let mut entry = BatchCommandEntry::new(
                OpaqueBatchCommand::new(BatchCommandTag::Empty, vec![index as u8]),
                completion.into(),
            );
            if index == 3 {
                entry = entry.with_forwarded_host("logical-store:20160");
            }
            if index == 4 {
                pull.cancel();
            }
            progress.push(entry.progress());
            pending.push(pull);
            receipts.push(
                handle
                    .batch_submit_with_receipts("127.0.0.1:1", vec![entry], call)
                    .unwrap(),
            );
        }
        let (reply, _) = oneshot::channel();
        commands.send(WorkerCommand::Close { reply }).unwrap();
        // Publication is synchronous; inspect admission and orderly retirement.
        let (_shutdown, shutdown_rx) = watch::channel(false);
        wait(super::super::execution_runtime().unwrap().spawn(run_worker(
            vec![super::super::execution_runtime().unwrap().handle().clone()],
            receiver,
            commands,
            shutdown_rx,
            Arc::new(ClusterSecurity::default()),
        )))
        .unwrap();

        for (index, receipt) in receipts.into_iter().enumerate() {
            let receipt = wait(receipt).unwrap();
            if matches!(index, 0 | 3 | 5) {
                assert_eq!(receipt.len(), 1);
                assert_eq!(receipt[0].request_ids(), &[progress[index].request_id()]);
                assert_eq!(
                    receipt[0].route().forwarded_host(),
                    (index == 3).then_some("logical-store:20160")
                );
                assert_eq!(receipt[0].route().physical_channel_version(), 1);
                assert_eq!(receipt[0].route().generation(), 1);
            } else {
                assert!(receipt.is_empty());
                assert_eq!(progress[index].request_id(), 0);
            }
        }
        assert!(matches!(
            pending[1].try_complete().unwrap().unwrap(),
            Err(crate::BatchInflightError::Transport(
                DirectUnaryClientError::CallerCancelled
            ))
        ));
        assert!(matches!(
            pending[2].try_complete().unwrap().unwrap(),
            Err(crate::BatchInflightError::Transport(
                DirectUnaryClientError::Timeout { .. }
            ))
        ));
        assert!(pending[4].try_complete().unwrap().is_none());
        assert!(progress[0]
            .batch_state()
            .unwrap()
            .shares_state_with(&progress[5].batch_state().unwrap()));
        assert!(!progress[0]
            .batch_state()
            .unwrap()
            .shares_state_with(&progress[3].batch_state().unwrap()));
    }

    fn cancellation() -> TransportShutdownCancellation {
        let (shutdown, _) = watch::channel(false);
        TransportShutdownCancellation { shutdown }
    }

    #[test]
    fn shutdown_reports_closed_command_channel() {
        let (commands, receiver) = mpsc::unbounded_channel();
        drop(receiver);
        let worker = super::super::execution_runtime().unwrap().spawn(async {});
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            io: Vec::new(),
        };

        assert_eq!(
            runtime.shutdown(),
            Err(DirectUnaryClientError::Shutdown(
                TransportShutdownError::CommandChannelClosed
            ))
        );
    }

    #[test]
    fn shutdown_reports_lost_close_acknowledgement() {
        let (commands, mut receiver) = mpsc::unbounded_channel();
        let worker = super::super::execution_runtime()
            .unwrap()
            .spawn(async move {
                if let Some(WorkerCommand::Close { reply }) = receiver.recv().await {
                    drop(reply);
                }
            });
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            io: Vec::new(),
        };

        assert_eq!(
            runtime.shutdown(),
            Err(DirectUnaryClientError::Shutdown(
                TransportShutdownError::CloseAcknowledgementLost
            ))
        );
    }

    #[test]
    fn shutdown_reports_worker_panic() {
        let (commands, mut receiver) = mpsc::unbounded_channel();
        let worker = super::super::execution_runtime()
            .unwrap()
            .spawn(async move {
                if let Some(WorkerCommand::Close { reply }) = receiver.recv().await {
                    reply.send(()).unwrap();
                }
                panic!("injected transport worker panic");
            });
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            io: Vec::new(),
        };

        assert_eq!(
            runtime.shutdown(),
            Err(DirectUnaryClientError::Shutdown(
                TransportShutdownError::WorkerPanicked {
                    message: "injected transport worker panic".to_owned(),
                }
            ))
        );
    }

    #[test]
    fn shutdown_retains_lost_acknowledgement_and_worker_panic() {
        let (commands, mut receiver) = mpsc::unbounded_channel();
        let worker = super::super::execution_runtime()
            .unwrap()
            .spawn(async move {
                if let Some(WorkerCommand::Close { reply }) = receiver.recv().await {
                    drop(reply);
                }
                panic!("panic after dropping close acknowledgement");
            });
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            io: Vec::new(),
        };

        assert_eq!(
            runtime.shutdown(),
            Err(DirectUnaryClientError::Shutdown(
                TransportShutdownError::Multiple(vec![
                    TransportShutdownError::CloseAcknowledgementLost,
                    TransportShutdownError::WorkerPanicked {
                        message: "panic after dropping close acknowledgement".to_owned(),
                    },
                ])
            ))
        );
    }
}
