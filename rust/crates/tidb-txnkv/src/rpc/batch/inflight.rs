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

//! Route-scoped BatchCommands in-flight ownership.
//!
//! This table is the single boundary between scheduling and a later duplex
//! stream. Callers publish the complete request-ID set before attempting
//! `Send`; every response, send failure, stream failure, cancellation, and
//! close then retires through the same Campaign 15 completion authority.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::rpc::{CompletionRequest, DirectUnaryClientError};

use super::{BatchRequestProgress, BatchStreamState, ScheduledEntry};

use super::wire::{BatchWireError, BatchWireResponse, OpaqueBatchCommand};

/// One physical stream generation and its optional forwarded TiKV target.
///
/// Generation is part of identity so late receive/failure work from a retired
/// stream cannot touch a replacement stream at the same address and target.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BatchRoute {
    physical_address: String,
    forwarded_host: Option<String>,
    generation: u64,
}

impl BatchRoute {
    /// Creates a direct route to one physical TiKV address.
    #[must_use]
    pub fn direct(physical_address: impl Into<String>, generation: u64) -> Self {
        Self {
            physical_address: physical_address.into(),
            forwarded_host: None,
            generation,
        }
    }

    /// Creates a forwarding-specific stream route.
    #[must_use]
    pub fn forwarded(
        physical_address: impl Into<String>,
        forwarded_host: impl Into<String>,
        generation: u64,
    ) -> Self {
        Self {
            physical_address: physical_address.into(),
            forwarded_host: Some(forwarded_host.into()),
            generation,
        }
    }

    /// Physical address owning the underlying channel.
    #[must_use]
    pub fn physical_address(&self) -> &str {
        &self.physical_address
    }

    /// TiKV target selected through forwarding metadata, if any.
    #[must_use]
    pub fn forwarded_host(&self) -> Option<&str> {
        self.forwarded_host.as_deref()
    }

    /// Address-local stream generation owning this route handle.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

/// Terminal failure published to one pending BatchCommands completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchInflightError {
    /// Malformed protobuf or command/ID cardinality from the stream.
    Protocol(BatchWireError),
    /// Send or receive failure supplied by the concrete stream owner.
    Transport(DirectUnaryClientError),
}

impl fmt::Display for BatchInflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => write!(formatter, "BatchCommands protocol error: {error}"),
            Self::Transport(error) => write!(formatter, "BatchCommands transport error: {error}"),
        }
    }
}

impl std::error::Error for BatchInflightError {}

/// Publication rejected before any new ID became visible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchPublishError {
    /// Request ID zero is the scheduler's unassigned sentinel.
    ZeroRequestId,
    /// A request ID appeared twice in the new group or was already pending.
    DuplicateRequestId(u64),
}

impl fmt::Display for BatchPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRequestId => formatter.write_str("BatchCommands request ID is zero"),
            Self::DuplicateRequestId(request_id) => {
                write!(formatter, "duplicate BatchCommands request ID {request_id}")
            }
        }
    }
}

impl std::error::Error for BatchPublishError {}

/// One scheduler-assigned request ready to be published before stream send.
#[derive(Debug)]
pub struct PendingBatchCommand {
    request_id: u64,
    completion: CompletionRequest<OpaqueBatchCommand, BatchInflightError>,
    progress: Arc<BatchRequestProgress>,
}

impl PendingBatchCommand {
    /// Binds the scheduler ID to its sole completion and observation state.
    #[must_use]
    pub fn new(
        request_id: u64,
        completion: CompletionRequest<OpaqueBatchCommand, BatchInflightError>,
        progress: Arc<BatchRequestProgress>,
    ) -> Self {
        Self {
            request_id,
            completion,
            progress,
        }
    }

    /// Scheduler-assigned stream request ID.
    #[must_use]
    pub const fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Moves one scheduler-selected command into in-flight state.
    ///
    /// The returned wire body and pending record are split from the same
    /// scheduled entry; no second completion pair or terminal authority exists.
    #[must_use]
    pub fn from_scheduled(
        scheduled: ScheduledEntry<
            OpaqueBatchCommand,
            CompletionRequest<OpaqueBatchCommand, BatchInflightError>,
        >,
    ) -> (OpaqueBatchCommand, Self) {
        let (request_id, entry) = scheduled.into_parts();
        let (command, completion, progress) = entry.into_payload_completion();
        (command, Self::new(request_id, completion, progress))
    }
}

#[derive(Debug, Default)]
struct RouteInflight {
    stream_state: BatchStreamState,
    pending: HashMap<u64, PendingBatchCommand>,
}

/// Result of retiring one validated response envelope.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BatchRetirementReport {
    /// Active completions scheduled with responses.
    pub completed: usize,
    /// Canceled requests removed without publishing a response.
    pub canceled: usize,
    /// Response IDs that were no longer pending on this exact route.
    pub outdated: usize,
    /// Greatest request ID carried by this response envelope.
    pub max_response_request_id: u64,
}

/// Sole route-keyed pending request and stream-acknowledgement authority.
#[derive(Debug, Default)]
pub struct BatchInflightTable {
    routes: HashMap<BatchRoute, RouteInflight>,
}

impl BatchInflightTable {
    /// Creates an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically publishes a complete request group before stream send.
    ///
    /// Duplicate IDs reject the whole group; no prefix becomes visible.
    pub fn publish(
        &mut self,
        route: BatchRoute,
        pending: Vec<PendingBatchCommand>,
    ) -> Result<(), BatchPublishError> {
        let mut new_ids = HashSet::with_capacity(pending.len());
        for request in &pending {
            if request.request_id == 0 {
                return Err(BatchPublishError::ZeroRequestId);
            }
            if !new_ids.insert(request.request_id)
                || self
                    .routes
                    .values()
                    .any(|state| state.pending.contains_key(&request.request_id))
            {
                return Err(BatchPublishError::DuplicateRequestId(request.request_id));
            }
        }

        let state = self.routes.entry(route).or_default();
        for request in pending {
            if let Some(batch_state) = request.progress.batch_state() {
                batch_state.attach_stream_state(state.stream_state.clone());
            }
            state.pending.insert(request.request_id, request);
        }
        Ok(())
    }

    /// Number of requests pending across every direct and forwarded route.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.values().map(|state| state.pending.len()).sum()
    }

    /// Whether no request remains pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.values().all(|state| state.pending.is_empty())
    }

    /// Number of requests pending on one exact stream route.
    #[must_use]
    pub fn route_len(&self, route: &BatchRoute) -> usize {
        self.routes
            .get(route)
            .map_or(0, |state| state.pending.len())
    }

    /// Stream acknowledgement state retained for progress inspection.
    #[must_use]
    pub fn stream_state(&self, route: &BatchRoute) -> Option<BatchStreamState> {
        self.routes
            .get(route)
            .map(|state| state.stream_state.clone())
    }

    /// Decodes one response and retires it, failing only this route on protocol error.
    pub fn receive_encoded(
        &mut self,
        route: &BatchRoute,
        bytes: &[u8],
    ) -> Result<BatchRetirementReport, BatchInflightError> {
        match BatchWireResponse::decode(bytes) {
            Ok(response) => Ok(self.receive(route, response)),
            Err(error) => {
                let failure = BatchInflightError::Protocol(error);
                self.fail_route(route, failure.clone());
                Err(failure)
            }
        }
    }

    /// Retires one already validated response envelope.
    #[must_use]
    pub fn receive(
        &mut self,
        route: &BatchRoute,
        response: BatchWireResponse,
    ) -> BatchRetirementReport {
        let max_response_request_id = response.request_ids().iter().copied().max().unwrap_or(0);
        let state = self.routes.entry(route.clone()).or_default();
        if max_response_request_id > 0 {
            state
                .stream_state
                .record_max_response_request_id(max_response_request_id);
        }

        let mut report = BatchRetirementReport {
            max_response_request_id,
            ..BatchRetirementReport::default()
        };
        for (request_id, response) in response
            .request_ids()
            .iter()
            .copied()
            .zip(response.commands().iter().cloned())
        {
            let Some(pending) = state.pending.remove(&request_id) else {
                report.outdated += 1;
                continue;
            };
            if pending.completion.is_cancelled() {
                report.canceled += 1;
            } else {
                pending.completion.schedule(Ok(response));
                report.completed += 1;
            }
        }
        report
    }

    /// Fast-fails only the listed IDs after a concrete stream send error.
    pub fn fail_ids(
        &mut self,
        route: &BatchRoute,
        request_ids: &[u64],
        error: BatchInflightError,
    ) -> usize {
        let Some(state) = self.routes.get_mut(route) else {
            return 0;
        };
        let mut failed = 0;
        for request_id in request_ids {
            if let Some(pending) = state.pending.remove(request_id) {
                pending.completion.schedule_error(error.clone());
                failed += 1;
            }
        }
        failed
    }

    /// Fails one direct or forwarded stream without touching sibling routes.
    pub fn fail_route(&mut self, route: &BatchRoute, error: BatchInflightError) -> usize {
        let Some(state) = self.routes.remove(route) else {
            return 0;
        };
        let failed = state.pending.len();
        for pending in state.pending.into_values() {
            pending.completion.schedule_error(error.clone());
        }
        failed
    }

    /// Fails every remaining async request when the owning client closes.
    pub fn close(&mut self) -> usize {
        let routes = std::mem::take(&mut self.routes);
        let mut failed = 0;
        for state in routes.into_values() {
            failed += state.pending.len();
            for pending in state.pending.into_values() {
                pending
                    .completion
                    .schedule_error(BatchInflightError::Transport(
                        DirectUnaryClientError::Closed,
                    ));
            }
        }
        failed
    }
}
