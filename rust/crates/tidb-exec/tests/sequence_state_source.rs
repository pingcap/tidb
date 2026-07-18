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

//! Source-shaped tests for `variable.SequenceState`.

use std::collections::BTreeMap;

use tidb_exec::sequence_state::SequenceState;

#[test]
fn sequence_values_update_and_missing_values_are_distinct() {
    // Source: pkg/sessionctx/variable/sequence_state.go:35-52.
    let mut state = SequenceState::new();
    assert_eq!(state.get_last_value(7), None);
    state.update_state(7, 101);
    assert_eq!(state.get_last_value(7), Some(101));
    state.update_state(7, 202);
    assert_eq!(state.get_last_value(7), Some(202));
}

#[test]
fn get_all_states_returns_a_copy_and_set_merges_like_maps_copy() {
    // Source: pkg/sessionctx/variable/sequence_state.go:54-69.
    let mut state = SequenceState::new();
    state.update_state(1, 10);
    state.update_state(2, 20);

    let mut snapshot = state.get_all_states();
    snapshot.insert(1, 999);
    snapshot.insert(3, 30);
    assert_eq!(state.get_last_value(1), Some(10));
    assert_eq!(state.get_last_value(3), None);

    state.set_all_states(&snapshot);
    assert_eq!(state.get_last_value(1), Some(999));
    assert_eq!(state.get_last_value(2), Some(20));
    assert_eq!(state.get_last_value(3), Some(30));
}

#[test]
fn empty_state_has_a_stable_serialization_snapshot() {
    // Source: pkg/sessionctx/variable/sequence_state.go:30-32 and
    // pkg/sessionctx/sessionstates/session_states.go:88-91.
    let state = SequenceState::default();
    assert_eq!(state.get_all_states(), BTreeMap::new());
}
