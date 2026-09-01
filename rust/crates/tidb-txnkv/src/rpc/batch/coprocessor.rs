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

//! One physical Coprocessor attempt over the retained BatchCommands transport.
//!
//! This adapter owns no selection, retry, fallback, stream, or in-flight state.
//! Its one completion is carried unchanged by [`BatchCommandEntry`] into the
//! transport's sole route-scoped in-flight table.

use crate::{rpc::DirectUnaryClientError, DirectUnaryResponse};

use super::{
    synchronous_batch_completion_pair, BatchCommandEntry, BatchCommandTag, BatchInflightError,
    BatchPublicationReceipt, BatchRoute, OpaqueBatchCommand, SynchronousBatchPull,
};
use crate::rpc::{
    completion_pair, AsyncRequestPublication, CompletionError, CompletionPull, CompletionRunLoop,
    DirectUnaryConnectionError, PendingRequest,
};

/// Pull-side owner of one concrete Coprocessor BatchCommands attempt.
pub struct BatchCoprocessorPending {
    completion: CompletionPull<OpaqueBatchCommand, BatchInflightError>,
    publication_route: Option<BatchRoute>,
    deferred: Option<crate::rpc::transport_runtime::DeferredReceipts>,
}

impl BatchCoprocessorPending {
    pub(in crate::rpc) fn entry(
        encoded_request: Vec<u8>,
        forwarded_host: Option<&str>,
        run_loop: CompletionRunLoop,
    ) -> (BatchCommandEntry, Self) {
        let (completion, pull) = completion_pair(run_loop, || {});
        let command = OpaqueBatchCommand::new(BatchCommandTag::Coprocessor, encoded_request);
        let mut entry = BatchCommandEntry::new(command, completion);
        if let Some(forwarded_host) = forwarded_host {
            entry = entry.with_forwarded_host(forwarded_host);
        }
        (
            entry,
            Self {
                completion: pull,
                publication_route: None,
                deferred: None,
            },
        )
    }

    pub(in crate::rpc) fn retain_deferred(
        &mut self,
        deferred: crate::rpc::transport_runtime::DeferredReceipts,
    ) {
        self.deferred = Some(deferred);
    }

    fn resolve_publication(&mut self) -> Result<(), DirectUnaryClientError> {
        if self.publication_route.is_some() {
            return Ok(());
        }
        let Some(deferred) = self.deferred.take() else {
            return Ok(());
        };
        let receipts = deferred.wait()?;
        self.bind_publication(&receipts)
    }

    pub(in crate::rpc) fn bind_publication(
        &mut self,
        receipts: &[BatchPublicationReceipt],
    ) -> Result<(), DirectUnaryClientError> {
        let [receipt] = receipts else {
            return Err(DirectUnaryClientError::InvalidRequest(format!(
                "one Coprocessor command requires exactly one BatchCommands publication receipt, got {}",
                receipts.len()
            )));
        };
        if receipt.request_ids().len() != 1 {
            return Err(DirectUnaryClientError::InvalidRequest(format!(
                "one Coprocessor command requires exactly one published request ID, got {}",
                receipt.request_ids().len()
            )));
        }
        if self.publication_route.is_some() {
            return Err(DirectUnaryClientError::InvalidRequest(
                "Coprocessor pending attempt was bound to publication twice".to_owned(),
            ));
        }
        self.publication_route = Some(receipt.route().clone());
        Ok(())
    }

    fn map_result(
        &self,
        result: Result<OpaqueBatchCommand, BatchInflightError>,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        match result {
            Ok(command) if command.tag() == BatchCommandTag::Coprocessor => {
                let route = self.publication_route.as_ref().ok_or_else(|| {
                    DirectUnaryClientError::InvalidRequest(
                        "successful Coprocessor completion has no publication identity".to_owned(),
                    )
                })?;
                Ok(DirectUnaryResponse::from_physical_channel(
                    command.into_body(),
                    route.physical_channel().clone(),
                ))
            }
            Ok(command) => Err(DirectUnaryClientError::InvalidRequest(format!(
                "BatchCommands Coprocessor attempt returned {:?}",
                command.tag()
            ))),
            Err(BatchInflightError::Protocol(error)) => {
                Err(DirectUnaryClientError::InvalidRequest(format!(
                    "invalid BatchCommands Coprocessor response: {error}"
                )))
            }
            Err(BatchInflightError::Transport(error)) => Err(error),
        }
    }
}

impl PendingRequest for BatchCoprocessorPending {
    fn publication(&self) -> Option<AsyncRequestPublication> {
        self.publication_route.as_ref().map(|route| {
            AsyncRequestPublication::new(
                route.physical_address(),
                route.physical_channel_version(),
                route.generation(),
                route.forwarded_host().map(str::to_owned),
            )
        })
    }

    fn try_complete(
        &mut self,
    ) -> Result<Option<Result<DirectUnaryResponse, DirectUnaryClientError>>, CompletionError> {
        let Some(result) = self.completion.try_complete()? else {
            return Ok(None);
        };
        if result.is_ok() {
            if let Err(error) = self.resolve_publication() {
                return Ok(Some(Err(error)));
            }
        }
        Ok(Some(self.map_result(result)))
    }

    fn cancel(&mut self) {
        self.completion.cancel();
    }

    fn complete(
        &mut self,
        call: &crate::rpc::UnaryCallContext,
    ) -> Result<Result<DirectUnaryResponse, DirectUnaryClientError>, CompletionError> {
        let result = self.completion.complete(call)?;
        if result.is_ok() {
            if let Err(error) = self.resolve_publication() {
                return Ok(Err(error));
            }
        }
        Ok(self.map_result(result))
    }
}

/// Response-channel owner used by client-go's synchronous `SendRequest` path.
pub(in crate::rpc) struct SynchronousBatchCoprocessorPending {
    completion: SynchronousBatchPull,
    publication_route: Option<BatchRoute>,
    deferred: Option<crate::rpc::transport_runtime::DeferredReceipts>,
}

impl SynchronousBatchCoprocessorPending {
    pub(in crate::rpc) fn entry(
        encoded_request: Vec<u8>,
        forwarded_host: Option<&str>,
    ) -> (BatchCommandEntry, Self) {
        let (completion, pull) = synchronous_batch_completion_pair();
        let command = OpaqueBatchCommand::new(BatchCommandTag::Coprocessor, encoded_request);
        let mut entry = BatchCommandEntry::new(command, completion);
        if let Some(forwarded_host) = forwarded_host {
            entry = entry.with_forwarded_host(forwarded_host);
        }
        (
            entry,
            Self {
                completion: pull,
                publication_route: None,
                deferred: None,
            },
        )
    }

    pub(in crate::rpc) fn retain_deferred(
        &mut self,
        deferred: crate::rpc::transport_runtime::DeferredReceipts,
    ) {
        self.deferred = Some(deferred);
    }

    fn resolve_publication(&mut self) -> Result<(), DirectUnaryClientError> {
        if self.publication_route.is_some() {
            return Ok(());
        }
        let Some(deferred) = self.deferred.take() else {
            return Ok(());
        };
        let receipts = deferred.wait()?;
        let [receipt] = receipts.as_slice() else {
            return Err(DirectUnaryClientError::InvalidRequest(format!(
                "one Coprocessor command requires exactly one BatchCommands publication receipt, got {}",
                receipts.len()
            )));
        };
        if receipt.request_ids().len() != 1 {
            return Err(DirectUnaryClientError::InvalidRequest(format!(
                "one Coprocessor command requires exactly one published request ID, got {}",
                receipt.request_ids().len()
            )));
        }
        self.publication_route = Some(receipt.route().clone());
        Ok(())
    }

    pub(in crate::rpc) fn complete(
        &mut self,
        physical_address: &str,
        call: &crate::rpc::UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let result = match self.completion.complete(call) {
            Ok(result) => result,
            Err(CompletionError::Cancelled) => return Err(DirectUnaryClientError::CallerCancelled),
            Err(CompletionError::DeadlineExceeded) => {
                return Err(DirectUnaryClientError::Timeout {
                    connection: DirectUnaryConnectionError::local_deadline(
                        physical_address,
                        0,
                        "BatchCommands response deadline elapsed".to_owned(),
                    ),
                    timeout_ms: 0,
                });
            }
            Err(error) => return Err(DirectUnaryClientError::Runtime(error.to_string())),
        };
        if result.is_ok() {
            self.resolve_publication()?;
        }
        match result {
            Ok(command) if command.tag() == BatchCommandTag::Coprocessor => {
                let route = self.publication_route.as_ref().ok_or_else(|| {
                    DirectUnaryClientError::InvalidRequest(
                        "successful Coprocessor completion has no publication identity".to_owned(),
                    )
                })?;
                Ok(DirectUnaryResponse::from_physical_channel(
                    command.into_body(),
                    route.physical_channel().clone(),
                ))
            }
            Ok(command) => Err(DirectUnaryClientError::InvalidRequest(format!(
                "BatchCommands Coprocessor attempt returned {:?}",
                command.tag()
            ))),
            Err(BatchInflightError::Protocol(error)) => {
                Err(DirectUnaryClientError::InvalidRequest(format!(
                    "invalid BatchCommands Coprocessor response: {error}"
                )))
            }
            Err(BatchInflightError::Transport(error)) => Err(error),
        }
    }

    pub(in crate::rpc) fn cancel(&mut self) {
        self.completion.cancel();
    }
}

impl Drop for SynchronousBatchCoprocessorPending {
    fn drop(&mut self) {
        self.cancel();
    }
}
