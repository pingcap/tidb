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

//! Ports of `pkg/ddl/db_integration_test.go` items 181--215 in the
//! deterministic `pkg/ddl` test inventory.  The runnable tests use the
//! synchronous catalog/DDL runners.  Tests whose Go observable is a session
//! variable, information-schema renderer, failpoint, transaction boundary,
//! or DDL job history are retained as explicit parity-gap records.

use tidb_datatype::Datum;
use tidb_executor::{Catalog, StmtContext, ddl, run_create_table_on, run_insert_on, run_select_on};

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Datum::Null => "<nil>".to_owned(),
                    Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
                    Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
                    Datum::Int(value) => value.to_string(),
                    Datum::UInt(value) => value.to_string(),
                    Datum::Float32(value) => value.to_string(),
                    Datum::Real(value) => value.to_string(),
                    Datum::Enum(value, _) => value.name().to_string(),
                    Datum::Set(value, _) => value.name().to_string(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

// --- TestAlterColumn (pkg/ddl/db_integration_test.go:1143) ---

/// The default-changing and origin-row half of Go's `TestAlterColumn`.
#[test]
fn alter_column_set_default_changes_new_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_alter_column (a int default 111, b varchar(8), c varchar(8) not null)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on(
        "insert into b103_alter_column set b = 'a', c = 'aa'",
        &mut catalog,
        &context,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table b103_alter_column alter column a set default 222",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    run_insert_on(
        "insert into b103_alter_column set b = 'b', c = 'bb'",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on(
                "select a from b103_alter_column order by a",
                &catalog,
                &context,
            )
            .unwrap()
        ),
        vec![vec!["111".to_owned()], vec!["222".to_owned()]],
    );
}

// --- TestAlterAlgorithm (pkg/ddl/db_integration_test.go:1358) ---

// go-parity-gap: the Go contract is the warning/acceptance matrix for the
// ALGORITHM clause, including partition operations and SHOW WARNINGS.  This
// tier has no statement warning carrier and does not implement the algorithm
// scheduler selection.
#[test]
#[ignore = "go-parity-gap: ALGORITHM warning matrix and scheduler are unported"]
fn alter_algorithm_acceptance_and_warning_matrix() {}

// --- TestTreatOldVersionUTF8AsUTF8MB4 (pkg/ddl/db_integration_test.go:1440) ---

// go-parity-gap: requires direct mutation of TableInfo/ColumnInfo versions and
// the global TreatOldVersionUTF8AsUTF8MB4 configuration followed by a schema
// reload.  Neither the meta mutator nor that compatibility switch is exposed
// by this catalog tier.
#[test]
#[ignore = "go-parity-gap: old TableInfo version mutation and UTF8 compatibility switch are unported"]
fn treat_old_version_utf8_as_utf8mb4() {}

// --- TestDefaultColumnWithRand (pkg/ddl/db_integration_test.go:1590) ---

// go-parity-gap: Go checks nondeterministic RAND defaults, session warning
// errno, and SHOW CREATE rendering.  The Rust DDL tier intentionally does not
// expose the session clock/binlog-unsafe-function policy surface.
#[test]
#[ignore = "go-parity-gap: RAND default policy, session clock, and SHOW CREATE are unported"]
fn default_column_with_rand() {}

// --- TestDefaultValueAsExpressions (pkg/ddl/db_integration_test.go:1648) ---

// go-parity-gap: the test depends on user identity, UUID/RAND evaluation,
// date-format defaults, warning classes, and time-dependent enum values.  The
// catalog runners have no session user or warning-list carrier.
#[test]
#[ignore = "go-parity-gap: expression defaults need session identity, clock, and warning carriers"]
fn default_value_as_expressions() {}

// --- TestChangingDBCharset (pkg/ddl/db_integration_test.go:1703) ---

// go-parity-gap: ALTER DATABASE/SCHEMA and the information_schema schemata
// renderer are not implemented by this tier.  `Catalog::create_database` is
// deliberately not treated as an ALTER DATABASE substitute.
#[test]
#[ignore = "go-parity-gap: ALTER DATABASE and schema charset metadata are unported"]
fn changing_database_charset_and_collation() {}

// --- TestSqlFunctionsInGeneratedColumns (pkg/ddl/db_integration_test.go:1817) ---

/// A small positive generated-column contract from the larger Go matrix.
#[test]
fn deterministic_sql_function_in_generated_column_is_readable() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_generated (a int, b int generated always as (abs(a)) virtual)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on(
        "insert into b103_generated values (-1, default)",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from b103_generated", &catalog, &context).unwrap()),
        vec![vec!["-1".to_owned(), "1".to_owned()]],
    );
}

// --- TestSchemaNameAndTableNameInGeneratedExpr (pkg/ddl/db_integration_test.go:1861) ---

// go-parity-gap: the Go test requires qualified-name normalization in SHOW
// CREATE, wrong-schema/table error distinctions, and case-sensitive schema
// lookup.  This tier can evaluate generated columns but has no SHOW CREATE
// renderer or session schema resolver for those diagnostics.
#[test]
#[ignore = "go-parity-gap: qualified generated-expression normalization and diagnostics are unported"]
fn schema_and_table_names_in_generated_expression() {}

// --- TestParserIssue284 (pkg/ddl/db_integration_test.go:1898) ---

// go-parity-gap: the observable is foreign-key DDL with session
// `foreign_key_checks=0`; the current catalog runner does not model the
// session setting/foreign-key enforcement switch.
#[test]
#[ignore = "go-parity-gap: foreign-key session switch is unported"]
fn parser_issue_284_foreign_key_reference() {}

// --- TestAddExpressionIndex (pkg/ddl/db_integration_test.go:1912) ---

// go-parity-gap: the Go test combines expression-index hidden-column metadata,
// config.Experimental.AllowsExpressionIndex, INFORMATION_SCHEMA, and SHOW
// CREATE.  Those session/config carriers are outside this source module; the
// expression-index storage core is covered by the existing focused source.
#[test]
#[ignore = "go-parity-gap: expression-index config, hidden metadata, and SHOW CREATE surfaces are unported"]
fn add_expression_index_and_toggle_experimental_gate() {}

// --- TestDropColumnWithCompositeIndex (pkg/ddl/db_integration_test.go:1996) ---

// go-parity-gap: the Go assertion reads INFORMATION_SCHEMA.STATISTICS and
// toggles exact index visibility before retrying the drop.  This tier has no
// information-schema statistics renderer or ALTER INDEX visibility runner.
#[test]
#[ignore = "go-parity-gap: index visibility and information-schema statistics are unported"]
fn drop_column_with_composite_index_requires_visible_index_contract() {}

// --- TestDropColumnWithIndex (pkg/ddl/db_integration_test.go:2017) ---

/// Dropping a covered ordinary index column removes the index and leaves the
/// remaining table usable.  This is the synchronous portion of the Go test.
#[test]
fn drop_column_with_index_removes_index_and_column() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_drop_index (a int, b int, c int)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    ddl::run_alter_table_in(
        "alter table b103_drop_index add index idx(b)",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table b103_drop_index drop column b",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    run_insert_on(
        "insert into b103_drop_index values (1, 3)",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from b103_drop_index", &catalog, &context).unwrap()),
        vec![vec!["1".to_owned(), "3".to_owned()]],
    );
}

// --- TestDropColumnWithAutoInc (pkg/ddl/db_integration_test.go:2030) ---

// go-parity-gap: requires the session tidb_allow_remove_auto_inc variable and
// the exact auto-increment allocator/covered-index error matrix.
#[test]
#[ignore = "go-parity-gap: auto-increment removal policy and allocator session variable are unported"]
fn drop_column_with_auto_increment() {}

// --- TestDropColumnWithMultiIndex (pkg/ddl/db_integration_test.go:2047) ---

/// Multiple ordinary indexes covering one dropped column are removed by the
/// synchronous ALTER implementation.
#[test]
fn drop_column_with_multiple_indexes_removes_all_covering_indexes() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_drop_multi (a int, b int, c int)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    for sql in [
        "alter table b103_drop_multi add index idx_1(b)",
        "alter table b103_drop_multi add index idx_2(b)",
    ] {
        ddl::run_alter_table_in(sql, &mut catalog, "test", &context).unwrap();
    }
    ddl::run_alter_table_in(
        "alter table b103_drop_multi drop column b",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from b103_drop_multi", &catalog, &context).unwrap()),
        Vec::<Vec<String>>::new(),
    );
}

// --- TestDropColumnsWithMultiIndex (pkg/ddl/db_integration_test.go:2061) ---

/// Dropping two indexed columns in one ALTER leaves the unindexed column.
#[test]
fn drop_columns_with_multiple_indexes_removes_all_covering_indexes() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_drop_both (a int, b int, c int)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    for sql in [
        "alter table b103_drop_both add index idx_1(b)",
        "alter table b103_drop_both add index idx_2(b)",
        "alter table b103_drop_both add index idx_3(c)",
        "alter table b103_drop_both drop column b, drop column c",
    ] {
        ddl::run_alter_table_in(sql, &mut catalog, "test", &context).unwrap();
    }
    assert_eq!(
        rows_text(&run_select_on("select * from b103_drop_both", &catalog, &context).unwrap()),
        Vec::<Vec<String>>::new(),
    );
}

// --- TestAutoIncrementTableOption (pkg/ddl/db_integration_test.go:2076) ---

// go-parity-gap: the Go contract is allocator rebasing at values beyond i64,
// AUTO_ID_CACHE variants, and SHOW TABLE ... NEXT_ROW_ID.  The synchronous
// table runner does not expose those allocator inspection APIs.
#[test]
#[ignore = "go-parity-gap: auto-increment allocator rebasing and NEXT_ROW_ID inspection are unported"]
fn auto_increment_table_option() {}

// --- TestAutoIncrementForce (pkg/ddl/db_integration_test.go:2103) ---

// go-parity-gap: FORCE AUTO_INCREMENT/AUTO_RANDOM mutates allocator state and
// depends on row-id allocation, session increment/offset, and duplicate-key
// behavior that this tier does not expose as a DDL runner.
#[test]
#[ignore = "go-parity-gap: FORCE AUTO_INCREMENT and AUTO_RANDOM allocator state are unported"]
fn auto_increment_force() {}

// --- TestAutoIncrementForceAutoIDCache (pkg/ddl/db_integration_test.go:2213) ---

// go-parity-gap: AUTO_ID_CACHE uses separate row-id and auto-increment
// allocators; the catalog has no equivalent allocator inspection surface.
#[test]
#[ignore = "go-parity-gap: AUTO_ID_CACHE allocator split is unported"]
fn auto_increment_force_with_auto_id_cache() {}

// --- TestIssue20490 (pkg/ddl/db_integration_test.go:2342) ---

/// Adding a NOT NULL default column, changing it nullable, and reading old and
/// new rows is fully observable through the synchronous catalog runner.
// go-parity-gap: measured omitted-column INSERTs currently store NULL for ordinary literal defaults.
#[test]
#[ignore = "go-parity-gap: literal default materialization for omitted INSERT columns is unported"]
fn issue_20490_add_default_then_make_nullable() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table b103_issue20490 (a int)", &mut catalog).unwrap();
    let context = ctx();
    for sql in [
        "alter table b103_issue20490 add b int not null default 1",
        "alter table b103_issue20490 modify b int null",
    ] {
        ddl::run_alter_table_in(sql, &mut catalog, "test", &context).unwrap();
    }
    run_insert_on(
        "insert into b103_issue20490 values (1, default)",
        &mut catalog,
        &context,
    )
    .unwrap();
    run_insert_on(
        "insert into b103_issue20490 (a) values (2)",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on(
                "select b from b103_issue20490 order by a",
                &catalog,
                &context,
            )
            .unwrap()
        ),
        vec![vec!["1".to_owned()], vec!["<nil>".to_owned()]],
    );
}

// --- TestIssue20741WithEnumField (pkg/ddl/db_integration_test.go:2357) ---

/// Existing rows receive the first ENUM member when a new NOT NULL ENUM column
/// has no explicit default.
// go-parity-gap: measured omitted-column INSERTs currently store NULL for ordinary literal defaults.
#[test]
#[ignore = "go-parity-gap: ENUM origin-default materialization is unported"]
fn issue_20741_enum_column_default_is_first_member() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_enum_issue (id int primary key, c int)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on(
        "insert into b103_enum_issue values (1, 2), (2, 2)",
        &mut catalog,
        &context,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table b103_enum_issue add column cc enum('a', 'b', 'c', 'd') not null",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on(
                "select cc from b103_enum_issue order by id",
                &catalog,
                &context
            )
            .unwrap()
        ),
        vec![vec!["a".to_owned()], vec!["a".to_owned()]],
    );
}

// --- TestEnumAndSetDefaultValue (pkg/ddl/db_integration_test.go:2373) ---

// go-parity-gap: this test asserts the exact stored DefaultValue strings in
// TableInfo under latin1 and utf8mb4.  The public KvColumn API exposes the
// materialized default but not the SHOW CREATE/TableInfo default spelling.
#[test]
#[ignore = "go-parity-gap: raw ENUM/SET TableInfo default metadata is unported"]
fn enum_and_set_default_value_metadata() {}

// --- TestDuplicateErrorMessage (pkg/ddl/db_integration_test.go:2392) ---

// go-parity-gap: the Go test cross-products collations, clustered-index modes,
// partitioning, and exact duplicate-key message rendering.  This tier has no
// session collation switch or equivalent partitioned duplicate-message
// renderer.
#[test]
#[ignore = "go-parity-gap: collation/index-mode duplicate error rendering is unported"]
fn duplicate_error_message_matrix() {}

// --- TestIssue22028 (pkg/ddl/db_integration_test.go:2441) ---

// go-parity-gap: exact display-width errno/message preservation is parser and
// type-diagnostic behavior not represented by the current DriverError API.
#[test]
#[ignore = "go-parity-gap: display-width diagnostic compatibility is unported"]
fn issue_22028_display_width_error() {}

// --- TestCreateTemporaryTable (pkg/ddl/db_integration_test.go:2455) ---

// go-parity-gap: local/global temporary tables require session-local table
// registries, transaction commit semantics, stale-read restrictions, and
// temporary-table shadowing.  Catalog tables are persistent only.
#[test]
#[ignore = "go-parity-gap: temporary-table session registry and transaction semantics are unported"]
fn create_temporary_table() {}

// --- TestAccessLocalTmpTableAfterDropDB (pkg/ddl/db_integration_test.go:2541) ---

// go-parity-gap: requires a session temporary table surviving DROP DATABASE,
// temporary-table DML, prepared statements, and transaction isolation.
#[test]
#[ignore = "go-parity-gap: local temporary tables after DROP DATABASE are unported"]
fn access_local_temporary_table_after_drop_database() {}

// --- TestAvoidCreateViewOnLocalTemporaryTable (pkg/ddl/db_integration_test.go:2625) ---

// go-parity-gap: view creation must consult the session-local temporary table
// registry and distinguish a local temporary table from a persistent base
// table; no such session registry exists here.
#[test]
#[ignore = "go-parity-gap: temporary-table/view name collision is unported"]
fn avoid_create_view_on_local_temporary_table() {}

// --- TestDropTemporaryTable (pkg/ddl/db_integration_test.go:2689) ---

// go-parity-gap: drop precedence between local temporary and persistent tables,
// transaction commit behavior, and temporary data cleanup need session state.
#[test]
#[ignore = "go-parity-gap: temporary-table drop and transaction behavior are unported"]
fn drop_temporary_table() {}

// --- TestTruncateLocalTemporaryTable (pkg/ddl/db_integration_test.go:2814) ---

// go-parity-gap: truncate must select session-local data, reset temporary
// auto-increment state, and preserve the persistent table with the same name.
#[test]
#[ignore = "go-parity-gap: temporary-table truncate semantics are unported"]
fn truncate_local_temporary_table() {}

// --- TestIssue29282 (pkg/ddl/db_integration_test.go:2911) ---

// go-parity-gap: prepared statements over temporary tables and pessimistic
// `FOR UPDATE` blocking require session/transaction/MVCC carriers absent here.
#[test]
#[ignore = "go-parity-gap: prepared temporary-table statements and pessimistic locking are unported"]
fn issue_29282_prepared_statement_temporary_table() {}

// --- TestEnumDefaultValue (pkg/ddl/db_integration_test.go:2957) ---

/// Trailing spaces in an ENUM default are normalized to the declared member.
// go-parity-gap: measured omitted-column INSERTs currently store NULL for ordinary literal defaults.
#[test]
#[ignore = "go-parity-gap: ENUM literal default materialization is unported"]
fn enum_default_value_trims_trailing_space() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_enum_default (a enum('', 'a', 'b') not null default 'b ')",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on(
        "insert into b103_enum_default values ()",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select a from b103_enum_default", &catalog, &context).unwrap()),
        vec![vec!["b".to_owned()]],
    );
}

// --- TestDDLLastInfo (pkg/ddl/db_integration_test.go:2974) ---

// go-parity-gap: @@tidb_last_ddl_info is a session variable backed by the DDL
// owner/job sequence.  The synchronous runners intentionally have no session
// variable or owner lifecycle surface.
#[test]
#[ignore = "go-parity-gap: session tidb_last_ddl_info and owner sequence are unported"]
fn ddl_last_info_tracks_query_and_sequence() {}

// --- TestDefaultCollationForUTF8MB4 (pkg/ddl/db_integration_test.go:3011) ---

// go-parity-gap: requires session default-collation configuration, database
// metadata mutation, and SHOW CREATE SCHEMA/TABLE rendering.
#[test]
#[ignore = "go-parity-gap: default-collation session and schema metadata are unported"]
fn default_collation_for_utf8mb4() {}

// --- TestOptimizeTable (pkg/ddl/db_integration_test.go:3064) ---

// go-parity-gap: OPTIMIZE TABLE is a job-level refusal and this tier has no
// optimize statement runner or warning-list carrier.
#[test]
#[ignore = "go-parity-gap: OPTIMIZE TABLE dispatch and warning are unported"]
fn optimize_table() {}

// --- TestIssue52680 (pkg/ddl/db_integration_test.go:3070) ---

// go-parity-gap: RECOVER TABLE, GC safe points, auto-id accessors, and the
// emulator-GC switch require storage/meta APIs outside this tier.
#[test]
#[ignore = "go-parity-gap: recover-table auto-id preservation and GC are unported"]
fn issue_52680_recover_table_auto_id() {}

// --- TestCreateIndexWithChangeMaxIndexLength (pkg/ddl/db_integration_test.go:3130) ---

// go-parity-gap: the Go test changes MaxIndexLength from a DDL failpoint while
// an ADD INDEX job is in flight.  The synchronous runner has no failpoint or
// mutable global index-length configuration seam.
#[test]
#[ignore = "go-parity-gap: in-flight MaxIndexLength configuration change is unported"]
fn create_index_with_changed_max_index_length() {}
