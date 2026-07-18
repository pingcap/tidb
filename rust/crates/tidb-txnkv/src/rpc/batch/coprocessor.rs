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

use super::{BatchCommandEntry, BatchCommandTag, BatchInflightError, OpaqueBatchCommand};
use crate::rpc::{
    completion_pair, CompletionError, CompletionPull, CompletionRunLoop, PendingRequest,
};

/// Pull-side owner of one concrete Coprocessor BatchCommands attempt.
pub struct BatchCoprocessorPending {
    completion: CompletionPull<OpaqueBatchCommand, BatchInflightError>,
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
        (entry, Self { completion: pull })
    }

    fn map_result(
        result: Result<OpaqueBatchCommand, BatchInflightError>,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        match result {
            Ok(command) if command.tag() == BatchCommandTag::Coprocessor => {
                Ok(DirectUnaryResponse {
                    encoded_response: command.body().to_vec(),
                })
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
        self.completion
            .try_complete()
            .map(|result| result.map(Self::map_result))
    }

    fn cancel(&mut self) {
        self.completion.cancel();
    }

    fn complete(
        &mut self,
        call: &crate::rpc::UnaryCallContext,
    ) -> Result<Result<DirectUnaryResponse, DirectUnaryClientError>, CompletionError> {
        self.completion.complete(call).map(Self::map_result)
    }
}
