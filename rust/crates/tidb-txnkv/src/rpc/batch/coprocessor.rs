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

use crate::client::PhysicalChannelIdentity;
use crate::{rpc::DirectUnaryClientError, DirectUnaryResponse};

use super::{
    BatchCommandEntry, BatchCommandTag, BatchInflightError, BatchPublicationReceipt,
    OpaqueBatchCommand,
};
use crate::rpc::{
    completion_pair, CompletionError, CompletionPull, CompletionRunLoop, PendingRequest,
};

/// Pull-side owner of one concrete Coprocessor BatchCommands attempt.
pub struct BatchCoprocessorPending {
    completion: CompletionPull<OpaqueBatchCommand, BatchInflightError>,
    physical_channel: Option<PhysicalChannelIdentity>,
}

impl BatchCoprocessorPending {
    pub(in crate::rpc) fn entry(
        encoded_request: Vec<u8>,
        forwarded_host: Option<&str>,
    ) -> (BatchCommandEntry, Self) {
        let (completion, pull) = completion_pair(CompletionRunLoop::new(), || {});
        let command = OpaqueBatchCommand::new(BatchCommandTag::Coprocessor, encoded_request);
        let mut entry = BatchCommandEntry::new(command, completion);
        if let Some(forwarded_host) = forwarded_host {
            entry = entry.with_forwarded_host(forwarded_host);
        }
        (
            entry,
            Self {
                completion: pull,
                physical_channel: None,
            },
        )
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
        if self.physical_channel.is_some() {
            return Err(DirectUnaryClientError::InvalidRequest(
                "Coprocessor pending attempt was bound to publication twice".to_owned(),
            ));
        }
        self.physical_channel = Some(receipt.route().physical_channel().clone());
        Ok(())
    }

    fn map_result(
        &self,
        result: Result<OpaqueBatchCommand, BatchInflightError>,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        match result {
            Ok(command) if command.tag() == BatchCommandTag::Coprocessor => {
                let physical_channel = self.physical_channel.clone().ok_or_else(|| {
                    DirectUnaryClientError::InvalidRequest(
                        "successful Coprocessor completion has no publication identity".to_owned(),
                    )
                })?;
                Ok(DirectUnaryResponse::from_physical_channel(
                    command.body().to_vec(),
                    physical_channel,
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
    fn try_complete(
        &mut self,
    ) -> Result<Option<Result<DirectUnaryResponse, DirectUnaryClientError>>, CompletionError> {
        let result = self.completion.try_complete()?;
        Ok(result.map(|result| self.map_result(result)))
    }

    fn cancel(&mut self) {
        self.completion.cancel();
    }

    fn complete(
        &mut self,
        call: &crate::rpc::UnaryCallContext,
    ) -> Result<Result<DirectUnaryResponse, DirectUnaryClientError>, CompletionError> {
        let result = self.completion.complete(call)?;
        Ok(self.map_result(result))
    }
}
