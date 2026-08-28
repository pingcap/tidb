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

//! Sole Tokio runtime, channel pool, and transport lifecycle worker.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_pd_client::ClusterSecurity;
use tokio::sync::watch;

use crate::region::StoreLiveness;

use super::batch::{
    BatchCommandEntry, BatchPublicationReceipt, BatchStreamEvent, BatchTransportState,
};
use super::channel_pool::ChannelPool;
use super::liveness::check_liveness;
use super::unary::{prepare_unary, RawUnaryRequest, RawUnaryResponse, UnaryCallContext};
use super::{DirectUnaryClientError, TransportShutdownError};

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
    }

    /// Publisher side: one direct BatchCommands submission duration.
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
        start_dumper();
    }

    /// Worker side: one BatchEvent block_on duration.
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
        reply: mpsc::Sender<Result<RawUnaryResponse, DirectUnaryClientError>>,
    },
    BatchEvent(BatchStreamEvent),
    CloseAddress {
        address: String,
        reply: mpsc::Sender<()>,
    },
    CloseAddressVersion {
        address: String,
        version: u64,
        reply: mpsc::Sender<()>,
    },
    Liveness {
        address: String,
        timeout: Duration,
        reply: mpsc::Sender<StoreLiveness>,
    },
    Inspect {
        address: String,
        reply: mpsc::Sender<(Option<u64>, usize)>,
    },
    InspectBatch {
        address: String,
        forwarded_host: Option<String>,
        reply: mpsc::Sender<(Option<u64>, u64)>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

/// Unique owner of the one retained transport worker.
pub(super) struct TransportRuntime {
    commands: Option<mpsc::Sender<WorkerCommand>>,
    worker: Option<JoinHandle<()>>,
    cancellation: TransportShutdownCancellation,
    core: Arc<TransportCore>,
}

/// Cloneable request capability for the retained transport worker.
///
/// This handle deliberately contains neither the worker join handle nor its
/// shutdown cancellation. Dropping every request handle does not stop the
/// worker, and no request handle can join it.
#[derive(Clone)]
pub(super) struct TransportHandle {
    commands: mpsc::Sender<WorkerCommand>,
    core: Arc<TransportCore>,
}

struct TransportWorkerState {
    channels: ChannelPool,
    batch: BatchTransportState,
}

/// State which must stay serialized per transport shard, shared with callers
/// so warm BatchCommands publication does not need a second OS-thread wakeup.
struct TransportCore {
    runtime: tokio::runtime::Runtime,
    state: Mutex<TransportWorkerState>,
    security: Arc<ClusterSecurity>,
    closed: AtomicBool,
}

impl TransportCore {
    fn submit_batch_direct(
        &self,
        commands: &mpsc::Sender<WorkerCommand>,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: Option<&UnaryCallContext>,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        let submit_started = std::time::Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.closed.load(Ordering::Acquire) {
            return Err(DirectUnaryClientError::Closed);
        }
        let _entered = self.runtime.enter();
        let TransportWorkerState { channels, batch } = &mut *state;
        let receipts = batch.submit(channels, &self.runtime, address, entries, call, commands);
        admit_diag::note_worker_submit(submit_started.elapsed());
        Ok(receipts)
    }
}

/// Cloneable direct cancellation for interrupting a blocked transport open.
#[derive(Clone)]
pub struct TransportShutdownCancellation {
    shutdown: watch::Sender<bool>,
}

impl TransportShutdownCancellation {
    /// Builds one cancellation from its own watch sender, for an owner that
    /// fans ONE watch out to every shard of a sharded transport.
    #[must_use]
    pub(super) fn from_sender(shutdown: watch::Sender<bool>) -> Self {
        Self { shutdown }
    }

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
    /// Creates one shard of a sharded transport. The worker listens on the
    /// caller's shutdown watch so one top-level cancellation stops every
    /// shard at once. This runtime's own cancellation stays wired to a sender
    /// nobody else holds, preserving this shard's orderly-close path.
    pub(super) fn new_with_shutdown_receiver(
        security: Arc<ClusterSecurity>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Self, DirectUnaryClientError> {
        let (commands, receiver) = mpsc::channel();
        let worker_commands = commands.clone();
        let (shutdown, _private_rx) = watch::channel(false);
        let cancellation = TransportShutdownCancellation { shutdown };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| DirectUnaryClientError::Runtime(error.to_string()))?;
        let core = Arc::new(TransportCore {
            runtime,
            state: Mutex::new(TransportWorkerState {
                channels: ChannelPool::with_security(Arc::clone(&security)),
                batch: BatchTransportState::new(shutdown_rx),
            }),
            security,
            closed: AtomicBool::new(false),
        });
        let worker_core = Arc::clone(&core);
        let worker = std::thread::spawn(move || {
            run_worker(receiver, worker_commands, worker_core);
        });
        Ok(Self {
            commands: Some(commands),
            worker: Some(worker),
            cancellation,
            core,
        })
    }

    pub(super) fn handle(&self) -> TransportHandle {
        TransportHandle {
            commands: self
                .commands
                .as_ref()
                .expect("live transport owner must retain its command sender")
                .clone(),
            core: Arc::clone(&self.core),
        }
    }

    pub(super) fn shutdown(&mut self) -> Result<(), DirectUnaryClientError> {
        self.core.closed.store(true, Ordering::Release);
        self.cancellation.cancel();
        let mut shutdown_errors = Vec::new();
        if let Some(commands) = self.commands.take() {
            let (reply, response) = mpsc::channel();
            match commands.send(WorkerCommand::Close { reply }) {
                Ok(()) => {
                    if response.recv().is_err() {
                        shutdown_errors.push(TransportShutdownError::CloseAcknowledgementLost);
                    }
                }
                Err(_) => {
                    shutdown_errors.push(TransportShutdownError::CommandChannelClosed);
                }
            }
        }
        if let Some(worker) = self.worker.take() {
            if let Err(panic) = worker.join() {
                shutdown_errors.push(TransportShutdownError::WorkerPanicked {
                    message: panic_message(&panic).to_owned(),
                });
            }
        }
        match shutdown_errors.len() {
            0 => Ok(()),
            1 => Err(DirectUnaryClientError::Shutdown(
                shutdown_errors.pop().expect("one shutdown error"),
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

/// The publication receipt of a submission that has not been collected yet.
///
/// Holding one is the whole of client-go's post-send state: the entry is on
/// the BatchCommands stream, its own completion is the only thing the caller
/// waits on, and the receipt is read only if the attempt has to be named.
pub(super) struct DeferredReceipts {
    receipts: Vec<BatchPublicationReceipt>,
}

impl DeferredReceipts {
    /// Collects the already-published route evidence.
    pub(super) fn wait(self) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        Ok(self.receipts)
    }
}

impl TransportHandle {
    pub(super) fn unary_send(
        &self,
        address: &str,
        request: RawUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<RawUnaryResponse, DirectUnaryClientError> {
        let (reply, response) = mpsc::channel();
        self.commands
            .send(WorkerCommand::UnarySend {
                address: address.to_owned(),
                request,
                call: call.clone(),
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response
            .recv()
            .unwrap_or(Err(DirectUnaryClientError::Closed))
    }

    pub(super) fn batch_submit(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        self.batch_submit_inner(address, entries, None)
    }

    pub(super) fn batch_submit_deferred_with_call(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: &UnaryCallContext,
    ) -> Result<DeferredReceipts, DirectUnaryClientError> {
        self.batch_submit_deferred(address, entries, Some(call.clone()))
    }

    fn batch_submit_inner(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: Option<UnaryCallContext>,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        let started = std::time::Instant::now();
        let deferred = self.batch_submit_deferred(address, entries, call)?;
        let receipts = deferred.wait();
        admit_diag::note_admit_wait(started.elapsed());
        receipts
    }

    /// Publishes the entries under this shard's shared transport-state lock
    /// and returns without crossing the transport command queue.
    ///
    /// The retained runtime and channel pool are still the sole shard
    /// authority. Sharing their short state lock lets the query worker perform
    /// the same scheduler/in-flight/outbound publication atomically, without
    /// waking a second OS thread before every network request. Receive tasks
    /// and lifecycle events continue to run on the retained runtime.
    pub(super) fn batch_submit_deferred(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: Option<UnaryCallContext>,
    ) -> Result<DeferredReceipts, DirectUnaryClientError> {
        let receipts =
            self.core
                .submit_batch_direct(&self.commands, address, entries, call.as_ref())?;
        Ok(DeferredReceipts { receipts })
    }

    pub(super) fn close_address(&self, address: &str) -> Result<(), DirectUnaryClientError> {
        let (reply, response) = mpsc::channel();
        self.commands
            .send(WorkerCommand::CloseAddress {
                address: address.to_owned(),
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response.recv().map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn close_address_version(
        &self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        let (reply, response) = mpsc::channel();
        self.commands
            .send(WorkerCommand::CloseAddressVersion {
                address: address.to_owned(),
                version,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response.recv().map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        let (reply, response) = mpsc::channel();
        self.commands
            .send(WorkerCommand::Liveness {
                address: address.to_owned(),
                timeout,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response.recv().map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn inspect(&self, address: &str) -> (Option<u64>, usize) {
        let (reply, response) = mpsc::channel();
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
        response.recv().unwrap_or((None, 0))
    }

    pub(super) fn inspect_batch(
        &self,
        address: &str,
        forwarded_host: Option<&str>,
    ) -> (Option<u64>, u64) {
        let (reply, response) = mpsc::channel();
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
        response.recv().unwrap_or((None, 0))
    }
}

/// Env gate for wire-level stall tracing (`TIKV_QUERY_TRACE`).
pub(in crate::rpc) fn wtrace_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("TIKV_QUERY_TRACE").is_some())
}

fn run_worker(
    receiver: mpsc::Receiver<WorkerCommand>,
    commands: mpsc::Sender<WorkerCommand>,
    core: Arc<TransportCore>,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::UnarySend {
                address,
                request,
                call,
                reply,
            } => match {
                let mut state = core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                prepare_unary(&core.runtime, &mut state.channels, &address, request, &call)
            } {
                Ok(prepared) => {
                    // The runtime, not an unbounded per-call OS thread, owns
                    // the wait. The worker immediately resumes command
                    // dispatch while channel-pool mutation remains serialized.
                    core.runtime.spawn(async move {
                        let _ = reply.send(prepared.execute().await);
                    });
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            },
            WorkerCommand::BatchEvent(event) => {
                let event_started = std::time::Instant::now();
                let mut state = core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let _entered = core.runtime.enter();
                let TransportWorkerState { channels, batch } = &mut *state;
                batch.handle_event(channels, &core.runtime, &commands, event);
                admit_diag::note_worker_event(event_started.elapsed());
            }
            WorkerCommand::CloseAddress { address, reply } => {
                let mut state = core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let TransportWorkerState { channels, batch } = &mut *state;
                if let Some(physical_channel) = channels.close_address(&address) {
                    batch.close_physical_channel(&physical_channel);
                }
                let _ = reply.send(());
            }
            WorkerCommand::CloseAddressVersion {
                address,
                version,
                reply,
            } => {
                let mut state = core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let TransportWorkerState { channels, batch } = &mut *state;
                if let Some(physical_channel) = channels.close_address_version(&address, version) {
                    batch.close_physical_channel(&physical_channel);
                }
                let _ = reply.send(());
            }
            WorkerCommand::Liveness {
                address,
                timeout,
                reply,
            } => {
                let result = check_liveness(&core.runtime, &address, timeout, &core.security);
                let _ = reply.send(result);
            }
            WorkerCommand::Inspect { address, reply } => {
                let state = core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let _ = reply.send((state.channels.version(&address), state.channels.len()));
            }
            WorkerCommand::InspectBatch {
                address,
                forwarded_host,
                reply,
            } => {
                let state = core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let _ = reply.send(state.batch.inspect(&address, forwarded_host.as_deref()));
            }
            WorkerCommand::Close { reply } => {
                let mut state = core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                core.closed.store(true, Ordering::Release);
                state.batch.close();
                state.channels.close();
                let _ = reply.send(());
                break;
            }
        }
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

    fn cancellation() -> TransportShutdownCancellation {
        let (shutdown, _) = watch::channel(false);
        TransportShutdownCancellation { shutdown }
    }

    fn core() -> Arc<TransportCore> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (_, shutdown) = watch::channel(false);
        let security = Arc::new(ClusterSecurity::plaintext());
        Arc::new(TransportCore {
            runtime,
            state: Mutex::new(TransportWorkerState {
                channels: ChannelPool::with_security(Arc::clone(&security)),
                batch: BatchTransportState::new(shutdown),
            }),
            security,
            closed: AtomicBool::new(false),
        })
    }

    #[test]
    fn shutdown_reports_closed_command_channel() {
        let (commands, receiver) = mpsc::channel();
        drop(receiver);
        let worker = std::thread::spawn(|| {});
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            core: core(),
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
        let (commands, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            if let Ok(WorkerCommand::Close { reply }) = receiver.recv() {
                drop(reply);
            }
        });
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            core: core(),
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
        let (commands, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            if let Ok(WorkerCommand::Close { reply }) = receiver.recv() {
                reply.send(()).unwrap();
            }
            panic!("injected transport worker panic");
        });
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            core: core(),
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
        let (commands, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            if let Ok(WorkerCommand::Close { reply }) = receiver.recv() {
                drop(reply);
            }
            panic!("panic after dropping close acknowledgement");
        });
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
            core: core(),
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

    #[test]
    fn direct_batch_publication_refuses_a_closed_transport() {
        let core = core();
        core.closed.store(true, Ordering::Release);
        let (commands, _receiver) = mpsc::channel();

        assert!(matches!(
            core.submit_batch_direct(&commands, "127.0.0.1:20160", Vec::new(), None),
            Err(DirectUnaryClientError::Closed)
        ));
    }
}
