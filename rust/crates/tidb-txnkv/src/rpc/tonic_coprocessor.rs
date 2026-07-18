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

use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use prost::Message;
use tidb_proto::CoprocessorRequest;

use crate::{DirectUnaryClient, DirectUnaryRequest, DirectUnaryResponse};

use super::channel_pool::ChannelPool;
use super::{DirectUnaryClientError, DirectUnaryConnectionError};

enum WorkerCommand {
    Send {
        address: String,
        request: Box<DirectUnaryRequest>,
        timeout: Duration,
        reply: mpsc::Sender<Result<DirectUnaryResponse, DirectUnaryClientError>>,
    },
    CloseAddress {
        address: String,
        reply: mpsc::Sender<()>,
    },
    CloseAddressVersion {
        address: String,
        version: u64,
        reply: mpsc::Sender<()>,
    },
    Inspect {
        address: String,
        reply: mpsc::Sender<(Option<u64>, usize)>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

/// Synchronous client-go-shaped unary capability backed by tonic.
///
/// A dedicated worker thread owns the Tokio runtime and every tonic channel.
/// Consequently the synchronous trait is safe to call from either ordinary or
/// already-async-hosted threads: it never nests `Runtime::block_on`. Channels
/// are created lazily, reused by address, and versioned on recreation.
pub struct TonicCoprocessorClient {
    commands: Option<mpsc::Sender<WorkerCommand>>,
    worker: Option<JoinHandle<()>>,
}

impl TonicCoprocessorClient {
    /// Constructs a live client without opening a socket.
    pub fn new() -> Result<Self, DirectUnaryClientError> {
        let (commands, receiver) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
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
            run_worker(runtime, receiver);
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

    /// Closes the current generation only when it is not newer than `version`.
    pub fn close_address_version(&mut self, address: &str, version: u64) {
        let Some(commands) = &self.commands else {
            return;
        };
        let (reply, response) = mpsc::channel();
        if commands
            .send(WorkerCommand::CloseAddressVersion {
                address: address.to_owned(),
                version,
                reply,
            })
            .is_ok()
        {
            let _ = response.recv();
        }
    }

    /// Returns the active generation for focused lifecycle diagnostics.
    #[must_use]
    pub fn connection_version(&self, address: &str) -> Option<u64> {
        self.inspect(address).0
    }

    /// Returns the number of active address-keyed channels.
    #[must_use]
    pub fn active_address_count(&self) -> usize {
        self.inspect("").1
    }

    fn inspect(&self, address: &str) -> (Option<u64>, usize) {
        let Some(commands) = &self.commands else {
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

    fn shutdown(&mut self) {
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
}

impl DirectUnaryClient for TonicCoprocessorClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let Some(commands) = &self.commands else {
            return Err(DirectUnaryClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::Send {
                address: address.to_owned(),
                request: Box::new(request.clone()),
                timeout,
                reply,
            })
            .map_err(|_| DirectUnaryClientError::Closed)?;
        response
            .recv()
            .unwrap_or(Err(DirectUnaryClientError::Closed))
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
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

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.shutdown();
        Ok(())
    }
}

impl Drop for TonicCoprocessorClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_worker(runtime: tokio::runtime::Runtime, receiver: mpsc::Receiver<WorkerCommand>) {
    let mut channels = ChannelPool::new();
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::Send {
                address,
                request,
                timeout,
                reply,
            } => {
                let result = send_coprocessor(&runtime, &mut channels, &address, &request, timeout);
                let _ = reply.send(result);
            }
            WorkerCommand::CloseAddress { address, reply } => {
                channels.close_address(&address);
                let _ = reply.send(());
            }
            WorkerCommand::CloseAddressVersion {
                address,
                version,
                reply,
            } => {
                channels.close_address_version(&address, version);
                let _ = reply.send(());
            }
            WorkerCommand::Inspect { address, reply } => {
                let _ = reply.send((channels.version(&address), channels.len()));
            }
            WorkerCommand::Close { reply } => {
                channels.close();
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn send_coprocessor(
    runtime: &tokio::runtime::Runtime,
    channels: &mut ChannelPool,
    address: &str,
    request: &DirectUnaryRequest,
    timeout: Duration,
) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
    let mut body = CoprocessorRequest::decode(request.encoded_request.as_slice())
        .map_err(|error| DirectUnaryClientError::InvalidRequest(error.to_string()))?;
    // client-go's AttachContext overwrites the underlying command context at
    // every send. Wrapper Context is the single authority; every other body
    // field survives unchanged.
    body.context = Some(request.context.clone());

    let selected = channels.get_or_create(address, runtime)?;
    let mut client = tidb_proto::tikvpb::tikv_client::TikvClient::new(selected.channel);
    // The caller-owned local timer is the sole timeout authority. Setting the
    // gRPC timeout header to the same duration races tonic's internal timer,
    // which can surface transport-specific Cancelled instead of our stable
    // typed Timeout result.
    let rpc_request = tonic::Request::new(body);
    let result = runtime
        .block_on(async { tokio::time::timeout(timeout, client.coprocessor(rpc_request)).await });
    let response = match result {
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(error)) if error.code() == tonic::Code::DeadlineExceeded => {
            return Err(timeout_error(address, selected.version, timeout, error));
        }
        Ok(Err(error)) => {
            return Err(connection_error(address, selected.version, error));
        }
        Err(error) => {
            return Err(timeout_error(address, selected.version, timeout, error));
        }
    };
    Ok(DirectUnaryResponse {
        encoded_response: response.encode_to_vec(),
    })
}

fn connection_error(
    address: &str,
    version: u64,
    error: impl std::fmt::Display,
) -> DirectUnaryClientError {
    DirectUnaryClientError::Connection(connection_identity(address, version, error))
}

fn timeout_error(
    address: &str,
    version: u64,
    timeout: Duration,
    error: impl std::fmt::Display,
) -> DirectUnaryClientError {
    DirectUnaryClientError::Timeout {
        connection: connection_identity(address, version, error),
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    }
}

fn connection_identity(
    address: &str,
    version: u64,
    error: impl std::fmt::Display,
) -> DirectUnaryConnectionError {
    DirectUnaryConnectionError {
        address: address.to_owned(),
        version,
        message: error.to_string(),
    }
}
