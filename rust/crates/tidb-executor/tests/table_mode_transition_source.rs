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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_model::TableMode;

/// Go `TestTableModeBasic`'s transition rows
/// (`pkg/ddl/table_mode_test.go:167-181,188-200`): `SetTableMode` succeeds
/// for Restore->Normal, Normal->Restore and Restore->Restore, and fails with
/// `Invalid mode set from (or by default) Restore to Import for table
/// t1_restore_import` for Restore->Import; re-creating an IMPORT-mode table
/// as RESTORE (`CreateTableWithInfo`) fails with `Invalid mode set from (or
/// by default) Import to Restore` (`pkg/ddl/table_mode_test.go:196`).
/// Both refusals are Go `CanTransitionTo` returning false
/// (`pkg/meta/model/table_mode.go:38-48`), which gates the job at
/// `pkg/ddl/jobsubmit/table_mode.go:30-33` with `ErrInvalidTableModeSet`
/// (8259, `pkg/infoschema/error.go:114`).
#[test]
fn table_mode_transitions_block_import_restore_swaps() {
    // Restore -> Import is refused: the exact Go message names the pair.
    assert!(!TableMode::RESTORE.can_transition_to(TableMode::IMPORT));
    // Import -> Restore is refused the same way.
    assert!(!TableMode::IMPORT.can_transition_to(TableMode::RESTORE));
    // Everything the Go test drives to success is allowed:
    assert!(TableMode::RESTORE.can_transition_to(TableMode::NORMAL));
    assert!(TableMode::NORMAL.can_transition_to(TableMode::RESTORE));
    assert!(TableMode::RESTORE.can_transition_to(TableMode::RESTORE));
    assert!(TableMode::NORMAL.can_transition_to(TableMode::NORMAL));
    assert!(TableMode::NORMAL.can_transition_to(TableMode::IMPORT));
    assert!(TableMode::IMPORT.can_transition_to(TableMode::IMPORT));
    assert!(TableMode::IMPORT.can_transition_to(TableMode::NORMAL));
}

/// Go `TestTableModeConcurrent`
/// (`pkg/ddl/table_mode_test.go:185-305`): four rounds of two CONCURRENT
/// `SetTableMode` calls over one table. The predicate decides each outcome
/// deterministically: two Import targets over a Normal table both pass
/// (round 1: 2 successes), two Normal targets pass (round 2), two Restore
/// targets pass (round 3), and a Restore+Import pair over a Restore table
/// yields exactly ONE success with the failed one reporting
/// `ErrInvalidTableModeSet` (round 4: `checkErrorCode(...,
/// errno.ErrInvalidTableModeSet)` at `:300`). The race only picks WHICH
/// request lands first; the allowed/refused split is fixed by
/// `CanTransitionTo`.
#[test]
fn table_mode_concurrent_transition_outcomes_are_decided_by_the_gate() {
    // Round 1: t1 is Normal; both racers target Import -> both pass.
    assert!(TableMode::NORMAL.can_transition_to(TableMode::IMPORT));
    assert!(TableMode::NORMAL.can_transition_to(TableMode::IMPORT));
    // Round 2: t1 is Normal again; both racers target Normal -> both pass.
    assert!(TableMode::NORMAL.can_transition_to(TableMode::NORMAL));
    assert!(TableMode::NORMAL.can_transition_to(TableMode::NORMAL));
    // Round 3: t1 is Normal; both racers target Restore -> both pass.
    assert!(TableMode::NORMAL.can_transition_to(TableMode::RESTORE));
    assert!(TableMode::NORMAL.can_transition_to(TableMode::RESTORE));
    // Round 4: t1 is Restore; racers target {Restore, Import} ->
    // exactly one passes (Restore) and one is refused with 8259 (Import).
    let outcomes: Vec<bool> = [TableMode::RESTORE, TableMode::IMPORT]
        .iter()
        .map(|target| TableMode::RESTORE.can_transition_to(*target))
        .collect();
    assert_eq!(outcomes, vec![true, false]);
}
