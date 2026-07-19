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

use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::watch;

use crate::region::StoreLiveness;

use super::batch::{
    BatchCommandEntry, BatchPublicationReceipt, BatchStreamEvent, BatchTransportState,
};
use super::channel_pool::ChannelPool;
use super::liveness::check_liveness;
use super::unary::{prepare_unary, RawUnaryRequest, RawUnaryResponse, UnaryCallContext};
use super::DirectUnaryClientError;

pub(super) enum WorkerCommand {
    UnarySend {
        address: String,
        request: RawUnaryRequest,
        call: UnaryCallContext,
        reply: mpsc::Sender<Result<RawUnaryResponse, DirectUnaryClientError>>,
    },
    BatchSubmit {
        address: String,
        entries: Vec<BatchCommandEntry>,
        call: Option<UnaryCallContext>,
        reply: mpsc::Sender<Vec<BatchPublicationReceipt>>,
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
}

/// Cloneable request capability for the retained transport worker.
///
/// This handle deliberately contains neither the worker join handle nor its
/// shutdown cancellation. Dropping every request handle does not stop the
/// worker, and no request handle can join it.
#[derive(Clone)]
pub(super) struct TransportHandle {
    commands: mpsc::Sender<WorkerCommand>,
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
    pub(super) fn new() -> Result<Self, DirectUnaryClientError> {
        let (commands, receiver) = mpsc::channel();
        let worker_commands = commands.clone();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let cancellation = TransportShutdownCancellation { shutdown };
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(DirectUnaryClientError::Runtime(error.to_string())));
                    return;
                }
            };
            if ready_tx.send(Ok(())).is_err() {
                return;
            }
            run_worker(runtime, receiver, worker_commands, shutdown_rx);
        });
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                commands: Some(commands),
                worker: Some(worker),
                cancellation,
            }),
            Ok(Err(error)) => {
                let _ = worker.join();
                Err(error)
            }
            Err(error) => {
                let _ = worker.join();
                Err(DirectUnaryClientError::Runtime(error.to_string()))
            }
        }
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
        let mut shutdown_error = None;
        if let Some(commands) = self.commands.take() {
            let (reply, response) = mpsc::channel();
            match commands.send(WorkerCommand::Close { reply }) {
                Ok(()) => {
                    if response.recv().is_err() {
                        shutdown_error = Some(DirectUnaryClientError::Runtime(
                            "TiKV transport worker exited without acknowledging close".to_owned(),
                        ));
                    }
                }
                Err(_) => {
                    shutdown_error = Some(DirectUnaryClientError::Runtime(
                        "TiKV transport command channel closed before shutdown".to_owned(),
                    ));
                }
            }
        }
        if let Some(worker) = self.worker.take() {
            if let Err(panic) = worker.join() {
                return Err(DirectUnaryClientError::Runtime(format!(
                    "TiKV transport worker panicked during shutdown: {}",
                    panic_message(&panic)
                )));
            }
        }
        shutdown_error.map_or(Ok(()), Err)
    }
}

impl Drop for TransportRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
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

    pub(super) fn batch_submit_with_call(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: &UnaryCallContext,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        self.batch_submit_inner(address, entries, Some(call.clone()))
    }

    fn batch_submit_inner(
        &self,
        address: &str,
        entries: Vec<BatchCommandEntry>,
        call: Option<UnaryCallContext>,
    ) -> Result<Vec<BatchPublicationReceipt>, DirectUnaryClientError> {
        let (reply, response) = mpsc::channel();
        self.commands
            .send(WorkerCommand::BatchSubmit {
                address: address.to_owned(),
                entries,
                call,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response.recv().map_err(|_| DirectUnaryClientError::Closed)
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

fn run_worker(
    runtime: tokio::runtime::Runtime,
    receiver: mpsc::Receiver<WorkerCommand>,
    commands: mpsc::Sender<WorkerCommand>,
    shutdown: watch::Receiver<bool>,
) {
    let mut channels = ChannelPool::new();
    let mut batch = BatchTransportState::new(shutdown);
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::UnarySend {
                address,
                request,
                call,
                reply,
            } => match prepare_unary(&runtime, &mut channels, &address, request, &call) {
                Ok(prepared) => {
                    // The runtime, not an unbounded per-call OS thread, owns
                    // the wait. The worker immediately resumes command
                    // dispatch while channel-pool mutation remains serialized.
                    runtime.spawn(async move {
                        let _ = reply.send(prepared.execute().await);
                    });
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            },
            WorkerCommand::BatchSubmit {
                address,
                entries,
                call,
                reply,
            } => {
                let receipts = runtime.block_on(batch.submit(
                    &mut channels,
                    &runtime,
                    &address,
                    entries,
                    call.as_ref(),
                    &commands,
                ));
                let _ = reply.send(receipts);
            }
            WorkerCommand::BatchEvent(event) => {
                runtime.block_on(batch.handle_event(&mut channels, &runtime, &commands, event))
            }
            WorkerCommand::CloseAddress { address, reply } => {
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
                let result = check_liveness(&runtime, &address, timeout);
                let _ = reply.send(result);
            }
            WorkerCommand::Inspect { address, reply } => {
                let _ = reply.send((channels.version(&address), channels.len()));
            }
            WorkerCommand::InspectBatch {
                address,
                forwarded_host,
                reply,
            } => {
                let _ = reply.send(batch.inspect(&address, forwarded_host.as_deref()));
            }
            WorkerCommand::Close { reply } => {
                batch.close();
                channels.close();
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send + 'static>) -> &str {
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

    #[test]
    fn shutdown_reports_closed_command_channel() {
        let (commands, receiver) = mpsc::channel();
        drop(receiver);
        let worker = std::thread::spawn(|| {});
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
        };

        let error = runtime.shutdown().unwrap_err().to_string();
        assert!(
            error.contains("command channel closed before shutdown"),
            "{error}"
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
        };

        let error = runtime.shutdown().unwrap_err().to_string();
        assert!(error.contains("without acknowledging close"), "{error}");
    }

    #[test]
    fn shutdown_reports_worker_panic() {
        let (commands, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            drop(receiver);
            panic!("injected transport worker panic");
        });
        let mut runtime = TransportRuntime {
            commands: Some(commands),
            worker: Some(worker),
            cancellation: cancellation(),
        };

        let error = runtime.shutdown().unwrap_err().to_string();
        assert!(error.contains("injected transport worker panic"), "{error}");
    }
}
