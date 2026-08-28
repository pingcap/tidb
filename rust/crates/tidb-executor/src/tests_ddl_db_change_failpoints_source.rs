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

//! Port of Go `pkg/ddl/db_change_failpoints_test.go` (all three `TestXxx`
//! functions, lines 37–111).
//!
//! Each test needs Go's failpoint layer plus a piece of the DDL framework
//! this crate does not model — job-arg preservation on internal error, the
//! domain's TiFlash replica bookkeeping, and FLASHBACK — hence ignored gaps
//! with the recipes kept.

/// `pkg/ddl/db_change_failpoints_test.go::TestModifyColumnTypeArgs` (line
/// 37): when `updateVersionAndTableInfo` fails (mockUpdateVersionAndTableInfoErr
/// `return(2)`) during `modify column a varchar(16)`, the statement fails
/// with `[ddl:-1]mock update version and tableInfo error,jobID=<id>`, the
/// table keeps its 1 column + 1 index, and the HISTORY job's raw args still
/// decode via `model.GetModifyColumnArgs` with `ChangingColumn` and
/// `ChangingIdxs` NIL (the failure must not persist partial changing-column
/// args).
// go-parity-gap: DDL job args and history-job persistence are not modeled
// in this crate.
#[test]
#[ignore = "go-parity-gap: DDL job args and history jobs are not modeled in this crate"]
fn modify_column_type_args_preserved_on_internal_error() {}

/// `pkg/ddl/db_change_failpoints_test.go::TestParallelUpdateTableReplica`
/// (line 74): two concurrent `UpdateTableReplicaInfo(highest, true)` calls on
/// the same TiFlash-replica table — one succeeds, the other fails with
/// `[ddl:-1]the replica available status of table t1 is already updated`.
// go-parity-gap: TiFlash replica status and the domain DDL executor are not
// modeled in this crate.
#[test]
#[ignore = "go-parity-gap: TiFlash replica bookkeeping is not modeled in this crate"]
fn parallel_update_table_replica() {}

/// `pkg/ddl/db_change_failpoints_test.go::TestParallelFlashbackTable` (line
/// 111): with emulator GC disabled and a GC safe point 48h in the past, two
/// parallel FLASHBACK statements (first the same target name, then
/// `flashback t_flashback` racing `flashback t_flashback to t_flashback2`)
/// land exactly one success, the loser reporting
/// `[schema:1050]Table 't_flashback' already exists`.
// go-parity-gap: FLASHBACK and GC safe-point machinery are not modeled in
// this crate.
#[test]
#[ignore = "go-parity-gap: FLASHBACK/GC machinery is not modeled in this crate"]
fn parallel_flashback_table() {}
