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

//! Execution-owned cancellation adapter for address-directed unary reads.

use std::sync::Arc;

use tidb_txnkv::UnaryCancellation;

use super::ActiveUnaryCancellation;
use crate::CancelHandle;

/// Connects one execution owner's canonical cancellation state to unary RPCs.
///
/// This adapter owns no worker or second cancellation registry. Every call
/// receives a clone of the exact carrier retained by [`CancelHandle`].
#[derive(Clone, Debug)]
pub struct ExecutionUnaryCancellation {
    owner: Arc<CancelHandle>,
}

impl ExecutionUnaryCancellation {
    /// Retains the execution cancellation owner used by detached execution.
    #[must_use]
    pub const fn new(owner: Arc<CancelHandle>) -> Self {
        Self { owner }
    }
}

impl ActiveUnaryCancellation for ExecutionUnaryCancellation {
    fn cancellation_for_call(&self) -> UnaryCancellation {
        self.owner.unary_cancellation()
    }
}
