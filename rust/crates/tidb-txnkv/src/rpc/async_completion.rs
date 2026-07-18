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

//! Source-shaped completion boundary for client-go asynchronous requests.
//!
//! The production dispatcher is deliberately not implemented by the unary
//! transport. Pinned client-go starts `SendRequestAsync` only through one
//! BatchCommands connection; the later batch-stream owner must implement this
//! contract without taking over RegionCache or RequestSelector policy.

use std::error::Error;
use std::fmt;

use crate::client::{DirectUnaryRequest, DirectUnaryResponse};

use super::{DirectUnaryClientError, UnaryCallContext};

/// Failure in the local once-only completion driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionError {
    /// A second driver attempted to own the same completion queue.
    ConcurrentDriver,
    /// A terminal value had already been delivered.
    AlreadyCompleted,
}

impl fmt::Display for CompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConcurrentDriver => formatter.write_str("completion already has a driver"),
            Self::AlreadyCompleted => formatter.write_str("completion already fulfilled"),
        }
    }
}

impl Error for CompletionError {}

/// One in-flight source request returned by [`AsyncRequestDispatcher::begin`].
pub trait PendingRequest {
    /// Polls without blocking. `None` means the exact attempt is still pending.
    fn try_complete(
        &mut self,
    ) -> Result<Option<Result<DirectUnaryResponse, DirectUnaryClientError>>, CompletionError>;

    /// Cancels this exact attempt without inventing a terminal response.
    fn cancel(&mut self);
}

/// Address-directed BatchCommands attempt boundary used by async policy.
pub trait AsyncRequestDispatcher {
    /// Concrete once-only pending handle.
    type Pending: PendingRequest;

    /// Begins exactly one attempt using an already selected target/proxy route.
    fn begin(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<Self::Pending, DirectUnaryClientError>;

    /// Executes a retry synchronously after async policy advances its state.
    fn send_retry_sync(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError>;
}
