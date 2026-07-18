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

//! Source-shaped tests for TiDB's isolation enum and one-shot state boundary.
//!
//! These tests intentionally stop at value semantics.  They do not construct
//! a KV transaction, session context, storage snapshot, or isolation provider.

use tidb_exec::isolation_state::{IsolationLevel, OneShotIsolation, OneShotState};

#[test]
fn enum_normalization_matches_type_enum_names_and_ordinals() {
    // Source: pkg/sessionctx/variable/variable.go:398-409 and
    // pkg/sessionctx/variable/sysvar.go:2100-2110.
    let expected = [
        ("read-uncommitted", IsolationLevel::ReadUncommitted),
        ("READ-COMMITTED", IsolationLevel::ReadCommitted),
        ("Repeatable-Read", IsolationLevel::RepeatableRead),
        ("serializable", IsolationLevel::Serializable),
    ];
    for (input, level) in expected {
        assert_eq!(IsolationLevel::parse(input), Some(level));
        assert_eq!(level.canonical_name(), input.to_ascii_uppercase());
    }
    assert_eq!(
        IsolationLevel::parse("0"),
        Some(IsolationLevel::ReadUncommitted)
    );
    assert_eq!(
        IsolationLevel::parse("1"),
        Some(IsolationLevel::ReadCommitted)
    );
    assert_eq!(
        IsolationLevel::parse("2"),
        Some(IsolationLevel::RepeatableRead)
    );
    assert_eq!(
        IsolationLevel::parse("3"),
        Some(IsolationLevel::Serializable)
    );
    assert_eq!(IsolationLevel::parse(" read-committed"), None);
    assert_eq!(IsolationLevel::parse("not-an-isolation-level"), None);
}

#[test]
fn enum_keeps_source_values_separate_from_storage_capability() {
    // Source: pkg/sessionctx/variable/varsutil.go:116-124.  TiDB retains all
    // four enum values but rejects the two unsupported levels unless the
    // skip-isolation check is enabled; this leaf does not own warning/error
    // publication, so it exposes the capability fact separately.
    assert_eq!(IsolationLevel::all().len(), 4);
    assert!(!IsolationLevel::ReadUncommitted.storage_supported());
    assert!(IsolationLevel::ReadCommitted.storage_supported());
    assert!(IsolationLevel::RepeatableRead.storage_supported());
    assert!(!IsolationLevel::Serializable.storage_supported());
}

#[test]
fn one_shot_state_matches_source_boundary_transitions() {
    // Source: pkg/sessionctx/variable/session.go:678-684, 2827-2858.
    let mut setting = OneShotIsolation::set(IsolationLevel::ReadCommitted);
    assert_eq!(setting.state(), OneShotState::Set);
    assert_eq!(setting.value(), Some(IsolationLevel::ReadCommitted));
    assert_eq!(setting.readback(), "READ-COMMITTED");

    // Set is selected while the session is already entering a transaction;
    // outside one, the source falls back until the boundary advances it.
    assert_eq!(
        setting.level_for_new_txn(true, IsolationLevel::RepeatableRead),
        IsolationLevel::ReadCommitted
    );
    assert_eq!(
        setting.level_for_new_txn(false, IsolationLevel::RepeatableRead),
        IsolationLevel::RepeatableRead
    );

    setting.advance_for_next_txn();
    assert_eq!(setting.state(), OneShotState::Use);
    assert_eq!(setting.readback(), "READ-COMMITTED");
    assert_eq!(
        setting.level_for_new_txn(false, IsolationLevel::RepeatableRead),
        IsolationLevel::ReadCommitted
    );

    setting.advance_for_next_txn();
    assert_eq!(setting, OneShotIsolation::default());
    assert_eq!(setting.readback(), "");
    assert_eq!(
        setting.level_for_new_txn(false, IsolationLevel::RepeatableRead),
        IsolationLevel::RepeatableRead
    );
}

#[test]
fn default_one_shot_state_is_empty_and_stable() {
    let mut setting = OneShotIsolation::default();
    assert_eq!(setting.state(), OneShotState::Default);
    assert_eq!(setting.value(), None);
    assert_eq!(setting.readback(), "");
    setting.advance_for_next_txn();
    assert_eq!(setting, OneShotIsolation::default());
}
