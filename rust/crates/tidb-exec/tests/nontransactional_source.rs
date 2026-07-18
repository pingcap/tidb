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

//! Source-shaped tests for the non-transactional DML admission boundary.
//!
//! These tests intentionally stop before AST table-reference validation,
//! shard selection, worker execution, metrics, and job-error aggregation.

use tidb_exec::nontransactional::{
    admit_non_transactional_dml, NonTransactionalAdmissionError, NonTransactionalDmlKind,
    NonTransactionalSessionState,
};

#[test]
fn default_session_admits_all_source_dml_families() {
    // Source: pkg/session/nontransactional.go:191-242 and
    // pkg/session/test/nontransactionaltest/nontransactional_test.go:32-124,
    // 429-510. REPLACE follows the Go InsertStmt branch with IsReplace set.
    let state = NonTransactionalSessionState::default();
    for kind in [
        NonTransactionalDmlKind::InsertSelect,
        NonTransactionalDmlKind::ReplaceSelect,
        NonTransactionalDmlKind::Update,
        NonTransactionalDmlKind::Delete,
    ] {
        assert_eq!(admit_non_transactional_dml(state, kind), Ok(()));
    }
}
#[test]
fn autocommit_and_transaction_state_are_checked_before_other_policy() {
    // Source: pkg/session/nontransactional.go:193-195 and
    // pkg/session/test/nontransactionaltest/nontransactional_test.go:319-367.
    let mut state = NonTransactionalSessionState {
        autocommit: false,
        ..NonTransactionalSessionState::default()
    };
    assert_eq!(
        admit_non_transactional_dml(state, NonTransactionalDmlKind::Delete),
        Err(NonTransactionalAdmissionError::NotAutocommit {
            autocommit: false,
            in_txn: false,
        })
    );

    state.autocommit = true;
    state.in_txn = true;
    assert_eq!(
        admit_non_transactional_dml(state, NonTransactionalDmlKind::Delete),
        Err(NonTransactionalAdmissionError::NotAutocommit {
            autocommit: true,
            in_txn: true,
        })
    );
}

#[test]
fn batch_dml_weak_read_and_snapshot_gates_match_source_order() {
    // Source: pkg/session/nontransactional.go:197-205 and
    // pkg/session/test/nontransactionaltest/nontransactional_test.go:314-367.
    let mut state = NonTransactionalSessionState {
        batch_dml_enabled: true,
        dml_batch_size: 1,
        batch_insert: true,
        ..NonTransactionalSessionState::default()
    };
    assert_eq!(
        admit_non_transactional_dml(state, NonTransactionalDmlKind::InsertSelect),
        Err(NonTransactionalAdmissionError::BatchDmlAlreadyEnabled)
    );

    state.batch_dml_enabled = false;
    state.weak_read_consistency = true;
    assert_eq!(
        admit_non_transactional_dml(state, NonTransactionalDmlKind::InsertSelect),
        Err(NonTransactionalAdmissionError::WeakReadConsistency)
    );

    state.weak_read_consistency = false;
    state.snapshot_ts = 42;
    assert_eq!(
        admit_non_transactional_dml(state, NonTransactionalDmlKind::InsertSelect),
        Err(NonTransactionalAdmissionError::SnapshotPinned)
    );
}

#[test]
fn insert_requires_select_and_unknown_dml_is_rejected() {
    // Source: pkg/session/nontransactional.go:225-242 and
    // pkg/session/test/nontransactionaltest/nontransactional_test.go:364-421.
    let state = NonTransactionalSessionState::default();
    assert_eq!(
        admit_non_transactional_dml(state, NonTransactionalDmlKind::InsertWithoutSelect),
        Err(NonTransactionalAdmissionError::InsertRequiresSelect)
    );
    assert_eq!(
        admit_non_transactional_dml(state, NonTransactionalDmlKind::Unsupported),
        Err(NonTransactionalAdmissionError::UnsupportedStatement)
    );
}
