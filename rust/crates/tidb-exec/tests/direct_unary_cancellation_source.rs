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

#![allow(missing_docs)]

use std::sync::Arc;

use tidb_distsql::cop_paging::ActiveUnaryCancellation;
use tidb_distsql::{CancelHandle, ExecutionState};

#[path = "../../tidb-distsql/src/cop_paging/active_cancellation.rs"]
mod active_cancellation;

use active_cancellation::ExecutionUnaryCancellation;

#[test]
fn detached_executor_cancellation_reaches_the_exact_direct_unary_carrier() {
    let execution = ExecutionState::new();
    let detached = execution.detach();
    let adapter = ExecutionUnaryCancellation::new(Arc::clone(&detached.cancel));
    let in_flight = adapter.cancellation_for_call();

    assert!(in_flight.shares_state_with(&execution.cancel.unary_cancellation()));
    assert!(!in_flight.is_cancelled());
    execution.cancel.cancel();
    assert!(detached.cancel.is_cancelled());
    assert!(in_flight.is_cancelled());
}

#[test]
fn already_cancelled_executor_yields_a_terminal_unary_carrier() {
    let cancel = Arc::new(CancelHandle::default());
    cancel.cancel();
    let adapter = ExecutionUnaryCancellation::new(Arc::clone(&cancel));

    let call = adapter.cancellation_for_call();
    assert!(call.is_cancelled());
    assert!(call.shares_state_with(&cancel.unary_cancellation()));
}
