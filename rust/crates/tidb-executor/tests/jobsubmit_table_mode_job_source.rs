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

//! Port ledger for `pkg/ddl/jobsubmit/table_mode_test.go` (pkg/ddl.part7
//! item 420 of the local enumeration: `TestBuildAlterTableModeJob` at line
//! 37). The builder itself has no Rust carrier, but its first gate — the
//! `TableMode.CanTransitionTo` check that decides whether the job may be
//! built at all — is the transcreated `tidb_model::TableMode`
//! (pkg/meta/model/table_mode.go:38-48), so that arm is a functional port.

use tidb_model::{AlterTableModeTarget, TableMode};

/// GO PORT of `pkg/ddl/jobsubmit/table_mode_test.go:37
/// TestBuildAlterTableModeJob` (mode-gate half; the builder half is the gap
/// port below).
///
/// Re-derived contract: `BuildAlterTableModeJob` runs
/// `target.CurrentMode.CanTransitionTo(target.TargetMode)` first and fails
/// with `infoschema.ErrInvalidTableModeSet` when it refuses
/// (pkg/ddl/jobsubmit/table_mode.go:30-33). The Go test's target is
/// CurrentMode Normal -> TargetMode Import, which must PASS the gate and
/// reach the builder (the test requires `err == nil` and `noop == false`).
/// The transcreated predicate (pkg/meta/model/table_mode.go:38-48, carried
/// at tidb_model::table_mode::TableMode::can_transition_to) allows every
/// pair except import<->restore swaps, and reports Normal -> Import as
/// allowed — the exact arm this Go test depends on. The noop/invalid-mode
/// arms belong to `TestBuildAlterTableModeJobNoopAndInvalidMode`, a
/// different Go test outside this batch's slice.
#[test]
fn table_mode_gate_admits_normal_to_import_for_the_alter_table_mode_job() {
    // The Go test's target, exactly: schema 101 / table 202 named
    // TestDB.T1, Normal -> Import.
    let target = AlterTableModeTarget {
        schema_id: 101,
        table_id: 202,
        current_mode: TableMode::NORMAL,
        target_mode: TableMode::IMPORT,
        ..Default::default()
    };
    // BuildAlterTableModeJob (table_mode.go:30) errors only when this is
    // false; the Go test requires the call to succeed with noop=false.
    assert!(target.current_mode.can_transition_to(target.target_mode));

    // The equality arm (table_mode.go:34-36) is checked AFTER the gate, so
    // an equal-mode pair must not be rejected by the gate either — this is
    // what keeps the noop path reachable rather than erroring.
    assert!(TableMode::IMPORT.can_transition_to(TableMode::IMPORT));
}

/// GO PORT of `pkg/ddl/jobsubmit/table_mode_test.go:37
/// TestBuildAlterTableModeJob` (builder half).
///
/// Re-derived contract (pkg/ddl/jobsubmit/table_mode.go:38-61): on the
/// success arm the builder returns `(job, args, noop=false)` where args are
/// exactly `AlterTableModeArgs{TableMode: Import, SchemaID: 101, TableID:
/// 202}`, and the job is Version2, SchemaID/TableID from the target,
/// SchemaName/TableName lowercased ("testdb"/"t1"), type
/// ActionAlterTableMode, Query "skip" (internal DDL placeholder), BinlogInfo
/// non-nil, CDCWriteSource 7 and SQLMode ANSIQuotes copied from the session,
/// and InvolvingSchemaInfo [{testdb, t1}] lowercased; the session's
/// QueryString value is left unset.
#[test]
#[ignore = "go-parity-gap: BuildAlterTableModeJob (pkg/ddl/jobsubmit/table_mode.go:24-61) and the mock sessionctx it reads are not transcreated"]
fn build_alter_table_mode_job_stamps_job_metadata_from_the_session() {}
