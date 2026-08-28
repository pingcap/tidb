// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Ports of the three `pkg/ddl/column_test.go` tests assigned to this batch
//! (origin/master): `TestDropColumns`, `TestWriteDataWriteOnlyMode`,
//! `TestModifyColumnWithIndex`.
//!
//! All three drive live DDL jobs (`testDropColumns`/`testDropTable` helpers,
//! `beforeRunOneJobStep`/`addIndexTxnWorkerBackfillData` failpoints) and
//! observe intermediate `model.SchemaState`s — machinery this tier replaced
//! with synchronous, direct metadata application (see `crate::ddl`'s module
//! doc: "the schema-version/DDL-job machinery" is deferred). Each Go test is
//! recorded as an `#[ignore]`d gap with its re-derived contract.

/// `column_test.go:828::TestDropColumns`.
#[test]
#[ignore = "go-parity-gap: observes column visibility across the DROP COLUMNS job's schema states via the afterWaitSchemaSynced hook and testCheckJobDone's history read; this tier has no DDL job queue (Go column_test.go:828-883)"]
fn drop_columns_drops_both_columns_once_the_job_completes() {
    // Derivation: t1 (c1..c4 int); one row written through table.AddRecord
    // with a default-filled c3 (4). testDropColumns runs
    // `alter table t1 drop column c3, drop column c4`; the hook polls the
    // table meta until c3/c4 disappear, then testCheckJobDone reads the
    // history job as synced; finally testDropTable drops t1. The metadata
    // END state (both columns gone, remaining data intact) is the contract;
    // the multi-state observation window is the gap.
}

/// `column_test.go:892::TestWriteDataWriteOnlyMode`.
#[test]
#[ignore = "go-parity-gap: writes via insert ignore inside beforeRunOneJobStep exactly at StateWriteOnly of two different jobs (change column, drop column); needs the schema-state machine (Go column_test.go:892-918)"]
fn write_data_write_only_mode_keeps_unique_index_consistent() {
    // Derivation: t (col1 bigint default 1, col2 float, unique key key1
    // (col1)). During `alter table t change column col1 col1 varchar(20)`'s
    // write-only state two `insert ignore into t values (1, 2)` / (2, 2) run;
    // the same is repeated during `alter table t drop column col1` with
    // (1) / (2). The test pins that mid-state writes see the write-only
    // column's on/off state consistently for reads of the unique index — no
    // error escapes either ALTER.
}

/// `column_test.go:920::TestModifyColumnWithIndex`.
#[test]
#[ignore = "go-parity-gap: counts index-record backfills per modify-column-type job via the addIndexTxnWorkerBackfillData failpoint; this tier rewrites metadata without a backfill (Go column_test.go:920-944)"]
fn modify_column_with_index_rebuilds_exactly_the_covering_indexes() {
    // Derivation: t (a varchar(4), b int) with idx1-idx3 over (a), idx4-idx6
    // over (b), idx7-idx9 over (a, b), one row ('a ', 1).
    // `alter table t modify column a char(4)` backfills 6 index records (the
    // three a-indexes x 1 row + the three (a, b)-indexes x 1 row);
    // `modify column b bigint` backfills 0 (widening is lossless in place);
    // `modify column b int UNSIGNED` backfills 6; the combined
    // `modify column a varchar(2), modify column b int` backfills 18.
}
