// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-backed tests for session reuse/close state.

use tidb_exec::session_reuse_state::SessionReuseState;

#[test]
fn session_reuse_state_preserves_owner_gates_and_idempotent_close() {
    // Source: pkg/session/syssession/session.go:221-237,398-425 and
    // pkg/session/syssession/session_test.go:400-451 (TestInternalSessionClose),
    // :691-730 (TestInternalSessionAvoidReuse).
    let mut state = SessionReuseState::new();
    assert!(!state.is_closed());
    assert!(!state.is_avoid_reuse());

    // Invalid owners cannot close or mark a live session.
    state.owner_mark_avoid_reuse(false);
    state.owner_close(false);
    assert!(!state.is_closed());
    assert!(!state.is_avoid_reuse());

    // A panic path marks avoid-reuse while keeping the session usable.
    state.owner_mark_avoid_reuse(true);
    assert!(state.is_avoid_reuse());
    assert!(!state.is_closed());

    // The current owner can close, and subsequent closes are no-ops.
    state.owner_close(true);
    assert!(state.is_closed());
    assert!(state.is_avoid_reuse());
    state.close();
    state.owner_close(true);
    state.owner_mark_avoid_reuse(true);
    assert!(state.is_closed());
    assert!(state.is_avoid_reuse());
}
