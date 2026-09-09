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
use std::{cell::RefCell, sync::Arc};

use super::{
    reply_pair, BatchCommandEntry, BatchCommandTag, BatchInflightError, BatchReply,
    BatchRequestProgress, OpaqueBatchCommand,
};
use crate::rpc::{
    completion_pair, AsyncRequestPublication, CompletionError, CompletionNotifier, CompletionPull,
    CompletionRunLoop, PendingRequest,
};

enum CoprocessorCompletion {
    Response(BatchReply),
    Callback(CompletionPull<OpaqueBatchCommand, BatchInflightError>),
}

/// Pull-side owner of one concrete Coprocessor BatchCommands attempt.
pub struct BatchCoprocessorPending {
    completion: CoprocessorCompletion,
    progress: Arc<BatchRequestProgress>,
    barrier: RefCell<Option<crate::rpc::transport_runtime::PublicationBarrier>>,
}

impl BatchCoprocessorPending {
    pub(in crate::rpc) fn entry(
        encoded_request: Vec<u8>,
        forwarded_host: Option<&str>,
    ) -> (BatchCommandEntry, Self) {
        let (completion, pull) = reply_pair();
        Self::entry_with_completion(
            encoded_request,
            forwarded_host,
            completion,
            CoprocessorCompletion::Response(pull),
        )
    }

    pub(in crate::rpc) fn entry_with_run_loop(
        encoded_request: Vec<u8>,
        forwarded_host: Option<&str>,
        run_loop: CompletionRunLoop,
    ) -> (BatchCommandEntry, Self) {
        let (completion, pull) = completion_pair(run_loop, || {});
        Self::entry_with_completion(
            encoded_request,
            forwarded_host,
            completion.into(),
            CoprocessorCompletion::Callback(pull),
        )
    }

    fn entry_with_completion(
        encoded_request: Vec<u8>,
        forwarded_host: Option<&str>,
        completion: super::BatchCommandCompletion,
        pull: CoprocessorCompletion,
    ) -> (BatchCommandEntry, Self) {
        let command = OpaqueBatchCommand::new(BatchCommandTag::Coprocessor, encoded_request);
        let mut entry = BatchCommandEntry::new(command, completion);
        if let Some(forwarded_host) = forwarded_host {
            entry = entry.with_forwarded_host(forwarded_host);
        }
        let progress = entry.progress();
        (
            entry,
            Self {
                completion: pull,
                progress,
                barrier: RefCell::new(None),
            },
        )
    }

    pub(in crate::rpc) fn retain_barrier(
        &mut self,
        barrier: crate::rpc::transport_runtime::PublicationBarrier,
    ) {
        *self.barrier.get_mut() = Some(barrier);
    }

    fn map_result(
        &self,
        result: Result<OpaqueBatchCommand, BatchInflightError>,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        match result {
            Ok(command) if command.tag() == BatchCommandTag::Coprocessor => {
                let route = self.progress.publication_route().ok_or_else(|| {
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
    fn set_notifier(&mut self, notifier: CompletionNotifier, token: u64) {
        match &mut self.completion {
            CoprocessorCompletion::Response(pull) => pull.set_notifier(notifier, token),
            CoprocessorCompletion::Callback(pull) => pull.set_notifier(notifier, token),
        }
    }

    fn publication(&self) -> Option<AsyncRequestPublication> {
        if self.progress.publication_route().is_none() {
            // Only an explicit pre-response observer needs this barrier.
            // Ordinary completion reads the route recorded by in-flight
            // publication, without a separate worker acknowledgement.
            if let Some(barrier) = self.barrier.borrow_mut().take() {
                barrier.wait();
            }
        }
        self.try_publication()
    }

    fn try_publication(&self) -> Option<AsyncRequestPublication> {
        self.progress.publication_route().map(|route| {
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
        let result = match &mut self.completion {
            CoprocessorCompletion::Response(pull) => pull.try_complete()?,
            CoprocessorCompletion::Callback(pull) => pull.try_complete()?,
        };
        Ok(result.map(|result| self.map_result(result)))
    }

    fn cancel(&mut self) {
        match &mut self.completion {
            CoprocessorCompletion::Response(pull) => pull.cancel(),
            CoprocessorCompletion::Callback(pull) => pull.cancel(),
        }
    }

    fn complete(
        &mut self,
        call: &crate::rpc::UnaryCallContext,
    ) -> Result<Result<DirectUnaryResponse, DirectUnaryClientError>, CompletionError> {
        let result = match &mut self.completion {
            CoprocessorCompletion::Response(pull) => pull.complete(call)?,
            CoprocessorCompletion::Callback(pull) => pull.complete(call)?,
        };
        Ok(self.map_result(result))
    }
}

impl Drop for BatchCoprocessorPending {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::batch::{
        BatchInflightTable, BatchRoute, BatchScheduler, BatchWireResponse, PendingBatchCommand,
    };

    #[test]
    fn explicit_callback_uses_its_supplied_run_loop() {
        let run_loop = CompletionRunLoop::new();
        let (entry, mut pending) =
            BatchCoprocessorPending::entry_with_run_loop(vec![], None, run_loop.clone());
        entry
            .completion()
            .schedule_error(BatchInflightError::Transport(
                DirectUnaryClientError::AdmissionBusy {
                    address: "physical:20160".to_owned(),
                },
            ));

        assert_eq!(run_loop.num_runnable(), 1);
        assert!(matches!(
            pending.try_complete().unwrap(),
            Some(Err(DirectUnaryClientError::AdmissionBusy { .. }))
        ));
        assert_eq!(run_loop.num_runnable(), 0);
    }

    #[test]
    fn completed_response_keeps_route_without_collecting_submission_receipt() {
        let (entry, mut pending) = BatchCoprocessorPending::entry(vec![], Some("logical:20160"));
        assert!(pending.try_publication().is_none());
        assert!(pending.try_complete().unwrap().is_none());
        let mut scheduler = BatchScheduler::new();
        scheduler.push(entry);
        let groups = scheduler.build_with_limit(1).into_parts();
        let (_, group) = groups.forwarded.into_iter().next().unwrap();
        let scheduled = group.into_entries().into_iter().next().unwrap();
        let request_id = scheduled.request_id();
        let (_, request) = PendingBatchCommand::from_scheduled(scheduled);
        let route = BatchRoute::forwarded("physical:20160", "logical:20160", 17);
        let mut inflight = BatchInflightTable::new();
        inflight.publish(route.clone(), vec![request]).unwrap();
        let response = BatchWireResponse::new(
            vec![OpaqueBatchCommand::new(
                BatchCommandTag::Coprocessor,
                b"response".to_vec(),
            )],
            vec![request_id],
            0,
            None,
            0,
        )
        .unwrap();
        assert_eq!(inflight.receive(&route, response).completed, 1);
        // No separate submission acknowledgement is needed for completion.
        let response = pending.try_complete().unwrap().unwrap().unwrap();
        assert_eq!(response.physical_address(), route.physical_address());
        assert_eq!(
            response.physical_channel_version(),
            route.physical_channel_version()
        );
        assert_eq!(pending.publication().unwrap().batch_stream_generation(), 17);
    }

    #[test]
    fn admission_failure_completes_without_receipt_or_invented_route() {
        let (entry, mut pending) = BatchCoprocessorPending::entry(vec![], None);
        entry
            .completion()
            .schedule_error(BatchInflightError::Transport(
                DirectUnaryClientError::AdmissionBusy {
                    address: "physical:20160".to_owned(),
                },
            ));
        assert!(matches!(
            pending.try_complete().unwrap(),
            Some(Err(DirectUnaryClientError::AdmissionBusy { .. }))
        ));
        assert!(pending.try_publication().is_none());
    }
}
