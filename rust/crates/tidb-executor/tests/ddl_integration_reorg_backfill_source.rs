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

//! Port ledger for `pkg/ddl/integration_test.go` (pkg/ddl.part7 items
//! 396-400 of the local enumeration, lines 61-239). All five are mockstore
//! SQL tests over the DDL job lifecycle; the crate's DDL tier applies
//! metadata directly with no job queue, so the four runnable-in-Go tests are
//! documentary gap ports and the nextgen-gated one inherits Go's own skip.

use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::{Catalog, StmtContext};

/// GO PORT of `pkg/ddl/integration_test.go:61 TestDDLStatementsBackFill`.
///
/// Re-derived contract: of `alter table t modify column a bigint`,
/// `modify column b char(255)`, `create table t1`, and
/// `alter table t1 drop primary key`, none enters write-reorganization;
/// `modify column a varchar(100)` (int -> varchar needs a data rewrite),
/// `add index idx_a(a)`, and `add primary key(b) nonclustered` do —
/// observed through the `afterWaitSchemaSynced` failpoint flipping a flag
/// when the job's SchemaState is StateWriteReorganization.
#[test]
#[ignore = "go-parity-gap: needs the DDL job state machine (SchemaState write-reorg transitions); the crate applies metadata directly with no job states"]
fn ddl_statements_backfill_exactly_when_write_reorganization_is_entered() {}

/// GO PORT of `pkg/ddl/integration_test.go:92 TestPartialIndex`.
///
/// Re-derived contract: `create table t (a int, b int, key(b) where a = 1)`
/// is ACCEPTED (partial indexes exist), while a partial index whose WHERE
/// references an unknown column, or a PRIMARY KEY with a WHERE, fails with
/// `dbterror.ErrUnsupportedAddPartialIndex`; the literal/column-type matrix
/// requires the comparison literal's type to match the column's type family
/// (int literal for int/year columns, string for strings, float for
/// float/double, binary literal for binary columns, nothing for other
/// types), with timestamp/datetime/date/time accepting string literals and
/// enum/set accepting int and string literals — anything else is refused.
///
/// go-parity-gap note: the Rust carrier now accepts non-partitioned partial
/// indexes and maintains their predicate-aware entries. The broader Go matrix
/// still covers literal/type compatibility, generated-column and primary-key
/// rejection, partition guards, and reorg/job-state behavior that is not fully
/// transcreated here, so this remains a documented divergence rather than an
/// approximation against a refuse-everything carrier.
#[test]
#[ignore = "go-parity-gap: partial-index literal/type/shape validation and reorg lifecycle are not fully transcreated; FK IS NOT NULL predicate semantics are covered"]
fn partial_index_accepts_only_type_matched_where_comparisons() {}

/// Focused shape/type slice of Go `TestPartialIndex`: unsupported conditions
/// are rejected with the dedicated 8200 errno, while the accepted type-family
/// combinations remain executable through both CREATE TABLE and ALTER TABLE.
#[test]
fn partial_index_condition_validation_matches_go() {
    let ctx = StmtContext::for_query();
    let assert_create = |sql: &str, allowed: bool| {
        let mut catalog = Catalog::default();
        let result = ddl::run_create_table_in(
            sql,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        );
        if allowed {
            result.unwrap_or_else(|error| panic!("{sql} should be accepted: {error:?}"));
        } else {
            let error = result.expect_err("Go rejects this partial-index condition");
            assert_eq!(error.clone().to_mysql_error().code, 8200, "{sql}: {error:?}");
        }
    };

    assert_create("create table t (a int, b int, key idx (b) where a = 1)", true);
    assert_create("create table t (a int, b int, key idx (b) where a = '1')", false);
    assert_create("create table t (a float, b int, key idx (b) where a = 1.0)", true);
    assert_create("create table t (a int, b int, key idx (b) where a = 1.0)", false);
    assert_create("create table t (a binary(8), b int, key idx (b) where a = 0x01)", true);
    assert_create("create table t (a varchar(8), b int, key idx (b) where a = 0x01)", false);
    assert_create("create table t (a text, b int, key idx (b) where a = '1')", true);
    assert_create(
        "create table t (a char(8) collate binary, b int, key idx (b) where a = 0x01)",
        true,
    );
    assert_create(
        "create table t (a char(8) collate binary, b int, key idx (b) where a = '1')",
        false,
    );
    assert_create(
        "create table t (a datetime, b int, key idx (b) where a = '2025-07-28')",
        true,
    );
    assert_create("create table t (a datetime, b int, key idx (b) where a = 1)", false);
    assert_create(
        "create table t (a enum('a','b'), b int, key idx (b) where a = 'a')",
        true,
    );
    assert_create("create table t (a enum('a','b'), b int, key idx (b) where a = null)", false);
    assert_create("create table t (a int, b int, key idx (b) where missing = 1)", false);
    assert_create("create table t (a int, b int, primary key (b) where a = 1)", false);
    assert_create("create table t (a int, b int, key idx (b) where a > b)", false);
    assert_create("create table t (a int, b int, key idx (b) where a like '1')", false);
    assert_create("create table t (a int, b int, key idx (b) where a is true)", false);
    assert_create(
        "create table t (a int, c int as (a + 1), b int, key idx (b) where c = 1)",
        false,
    );

    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t add index idx (b) where a = 1",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let error = ddl::run_alter_table_in(
        "alter table t add index bad (b) where a = '1'",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go rejects the ALTER partial-index type mismatch");
    assert_eq!(error.to_mysql_error().code, 8200);
}

/// GO PORT of `pkg/ddl/integration_test.go:178
/// TestDropTableAdminCheckTableFastCheckTable`.
///
/// Re-derived contract: with `config.CheckTableBeforeDrop` enabled, DROP
/// TABLE runs the admin-check/fast-check path first (one session with
/// `tidb_fast_check_table` off, one on) and the drop still succeeds.
#[test]
#[ignore = "go-parity-gap: config.CheckTableBeforeDrop and the drop-time fast-check path are not transcreated"]
fn drop_table_with_check_before_drop_runs_the_fast_check() {}

/// GO PORT of `pkg/ddl/integration_test.go:209 TestMaintainAffectColumns`.
///
/// Re-derived contract: a partial index over `col2` records
/// `AffectColumn[0].Offset` = col2's position, and that offset is
/// maintained as `add column col1 int first` shifts it to 1,
/// `add column col3 int after col1` shifts it to 2, and
/// `drop column col1` brings it back to 1 — index metadata offsets track
/// column insertion/removal positions.
#[test]
#[ignore = "go-parity-gap: affect-column offset maintenance on add/drop column is not transcreated"]
fn maintain_affect_columns_tracks_offsets_across_column_changes() {}

/// GO PORT of `pkg/ddl/integration_test.go:239
/// TestJobVersionAndGlobalIndexV1SupportForNextGen`.
///
/// Re-derived contract: Go itself skips this test unless
/// `kerneltype.IsNextGen()` ("nextgen only"); on nextgen it sets the global
/// config's store to TiKV for DDL init and checks job-version and
/// global-index V1 support interactions. The crate has no kernel-type
/// switch and no job-version surface, so there is nothing to pin.
#[test]
#[ignore = "go-parity-gap: Go test is nextgen-kernel-gated (t.Skip otherwise) and drives DDL job-version/global-index V1 machinery the crate does not carry"]
fn job_version_and_global_index_v1_support_is_nextgen_gated() {}
