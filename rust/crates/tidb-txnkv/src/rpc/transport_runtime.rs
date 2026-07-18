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

use crate::region::StoreLiveness;

use super::batch::transport::{
    BatchCommandEntry, BatchPublicationReceipt, BatchStreamEvent, BatchTransportState,
};
use super::channel_pool::ChannelPool;
use super::liveness::check_liveness;
use super::unary::{send_unary, RawUnaryRequest, RawUnaryResponse, UnaryCallContext};
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
        reply: mpsc::Sender<Option<u64>>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

/// Synchronous handle to the one retained transport worker.
pub(super) struct TransportRuntime {
    commands: Option<mpsc::Sender<WorkerCommand>>,
    worker: Option<JoinHandle<()>>,
}

impl TransportRuntime {
    pub(super) fn new() -> Result<Self, DirectUnaryClientError> {
        let (commands, receiver) = mpsc::channel();
        let worker_commands = commands.clone();
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
            run_worker(runtime, receiver, worker_commands);
        });
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                commands: Some(commands),
                worker: Some(worker),
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

    pub(super) fn unary_send(
        &self,
        address: &str,
        request: RawUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<RawUnaryResponse, DirectUnaryClientError> {
        let commands = self.sender()?;
        let (reply, response) = mpsc::channel();
        commands
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
        let commands = self.sender()?;
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::BatchSubmit {
                address: address.to_owned(),
                entries,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response.recv().map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn close_address(&self, address: &str) -> Result<(), DirectUnaryClientError> {
        let Some(commands) = &self.commands else {
            return Ok(());
        };
        let (reply, response) = mpsc::channel();
        commands
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
        let Some(commands) = &self.commands else {
            return Ok(());
        };
        let (reply, response) = mpsc::channel();
        commands
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
        let commands = self.sender()?;
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::Liveness {
                address: address.to_owned(),
                timeout,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response.recv().map_err(|_| DirectUnaryClientError::Closed)
    }

    pub(super) fn inspect(&self, address: &str) -> (Option<u64>, usize) {
        let Ok(commands) = self.sender() else {
            return (None, 0);
        };
        let (reply, response) = mpsc::channel();
        if commands
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

    pub(super) fn inspect_batch(&self, address: &str, forwarded_host: Option<&str>) -> Option<u64> {
        let Ok(commands) = self.sender() else {
            return None;
        };
        let (reply, response) = mpsc::channel();
        if commands
            .send(WorkerCommand::InspectBatch {
                address: address.to_owned(),
                forwarded_host: forwarded_host.map(str::to_owned),
                reply,
            })
            .is_err()
        {
            return None;
        }
        response.recv().unwrap_or(None)
    }

    pub(super) fn shutdown(&mut self) {
        if let Some(commands) = self.commands.take() {
            let (reply, response) = mpsc::channel();
            if commands.send(WorkerCommand::Close { reply }).is_ok() {
                let _ = response.recv();
            }
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }

    fn sender(&self) -> Result<&mpsc::Sender<WorkerCommand>, DirectUnaryClientError> {
        self.commands.as_ref().ok_or(DirectUnaryClientError::Closed)
    }
}

impl Drop for TransportRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_worker(
    runtime: tokio::runtime::Runtime,
    receiver: mpsc::Receiver<WorkerCommand>,
    commands: mpsc::Sender<WorkerCommand>,
) {
    let mut channels = ChannelPool::new();
    let mut batch = BatchTransportState::new();
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::UnarySend {
                address,
                request,
                call,
                reply,
            } => {
                let result = send_unary(&runtime, &mut channels, &address, request, &call);
                let _ = reply.send(result);
            }
            WorkerCommand::BatchSubmit {
                address,
                entries,
                reply,
            } => {
                let receipts = runtime.block_on(batch.submit(
                    &mut channels,
                    &runtime,
                    &address,
                    entries,
                    &commands,
                ));
                let _ = reply.send(receipts);
            }
            WorkerCommand::BatchEvent(event) => {
                runtime.block_on(batch.handle_event(&mut channels, &runtime, &commands, event))
            }
            WorkerCommand::CloseAddress { address, reply } => {
                if channels.close_address(&address) {
                    batch.close_address(&address);
                }
                let _ = reply.send(());
            }
            WorkerCommand::CloseAddressVersion {
                address,
                version,
                reply,
            } => {
                if channels.close_address_version(&address, version) {
                    batch.close_address(&address);
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
                let _ = reply.send(batch.active_generation(&address, forwarded_host.as_deref()));
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
