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

//! Transport-failure facts consumed by the response-owned retry loop.
//!
//! This module deliberately classifies only facts exposed by the TiKV client.
//! It owns no cache mutation, backoff, peer selection, task ordering, or
//! cancellation state.

use tidb_txnkv::{DirectUnaryClientError, DirectUnaryConnectionError};

/// Policy-neutral action implied by one address-directed send failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportFailureAction {
    /// The failure has no selected connection and cannot be retried safely.
    Terminal,
    /// A selected connection requires foreground liveness before retry.
    RetryConnection {
        /// Exact immutable connection observation from the failed RPC.
        connection: DirectUnaryConnectionError,
        /// Remote gRPC Canceled closes this generation before liveness.
        close_generation: bool,
    },
}

/// Preserves pinned client-go failure precedence without importing tonic.
#[must_use]
pub fn classify_transport_failure(error: &DirectUnaryClientError) -> TransportFailureAction {
    let Some(connection) = error.connection() else {
        return TransportFailureAction::Terminal;
    };
    TransportFailureAction::RetryConnection {
        connection: connection.clone(),
        close_generation: error.requires_generation_close(),
    }
}
