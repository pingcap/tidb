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

//! Ports of Go `pkg/ddl/table_mode_test.go` (master): `TestTableModeBasic`
//! (`:57`), `TestTableModeConcurrent` (`:185`) and
//! `TestTableModeWithRefreshMeta` (`:312`). Go drives a full DDL executor
//! (`domain.DDLExecutor`) with `CreateTableWithInfo`/`SetTableMode` jobs; the
//! carried piece of that machinery in this tier is the mode-TRANSITION
//! predicate `pkg/meta/model/table_mode.go:38 CanTransitionTo`
//! (`tidb_model::TableMode::can_transition_to`), which is what decides every
//! `SetTableMode` outcome the Go tests observe. The executor-level halves are
//! recorded as `#[ignore]` gap tests with the contract re-derived from the Go
//! source. Nothing is approximated.

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

/// Go `TestTableModeBasic`'s denial matrix
/// (`pkg/ddl/table_mode_test.go:89-129`): against a table in ModeRestore
/// (or ModeImport), reads (`select`, `explain`, `desc`), writes (`insert`,
/// `replace`, `update`, `delete`), `truncate`, and every mutating DDL
/// (`drop table`, `rename`, `modify/add/drop column`, `drop/add index`,
/// `partition by`, `comment`, `convert character set`, `rename column`,
/// `alter column set default`, `add foreign key`) fail with
/// `ErrProtectedTableMode` (8258, `pkg/infoschema/error.go:112`) -- including
/// inside an explicit transaction -- while metadata access (`show create
/// table`, `show table status`, `show columns`, `show index`, `describe`,
/// `create table like`, `create view`, FK child creation, and
/// `admin checksum table`) stays allowed (`:82-99`). Dropping a foreign key
/// of an Import-mode table is refused too (`:74`).
// go-parity-gap: the table-mode access gate lives in the DDL executor and
// planner fast paths (`infoschema.ErrProtectedTableMode` checks), which are
// not transcreated in this tier; no statement here consults `TableInfo.Mode`.
#[test]
#[ignore]
fn protected_table_mode_denials_match_go_matrix() {
}

/// Go `TestTableModeBasic`'s job half (`pkg/ddl/table_mode_test.go:65-77,
/// 142-160`): `CreateTableWithInfo` PRESERVES the incoming mode (a table
/// cloned with `Mode = TableModeImport` lands as Import; a Restore-mode
/// clone lands as Restore, keeps its metadata accessible, and its FK child
/// can be created and dropped), `BatchCreateTableWithInfo` lands three tables
/// with modes Normal/Import/Restore respectively, and creating a
/// Restore-mode clone over an existing Import-mode name fails with
/// `Invalid mode set from (or by default) Import to Restore`.
// go-parity-gap: no CreateTableWithInfo/BatchCreateTableWithInfo carrier;
// the Rust create path never sets `TableInfo.Mode`.
#[test]
#[ignore]
fn create_table_with_info_preserves_and_gates_the_mode() {
}

/// Go `TestTableModeWithRefreshMeta` (`pkg/ddl/table_mode_test.go:312-350`):
/// after a table's ID is swapped to a partition ID by hand (the exchange-
/// partition ID trick), `SetTableMode` fails with "doesn't exist" until the
/// domain REFRESHES its metadata; after `RefreshMeta` the mode change
/// succeeds and the table's read gate follows the mode (select refuses under
/// Import with 8258, succeeds after returning to Normal).
// go-parity-gap: no DDL job queue, no SetTableMode/RefreshMeta carrier, and
// no meta-txn table-ID swap harness in this tier.
#[test]
#[ignore]
fn table_mode_change_follows_a_refresh_meta() {
}
