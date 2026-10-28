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

use crate::support::testkit::TestKit;
use tidb_executor::TableEntry;

/// Go `serial_test.go:108-230::TestCreateTableWithLike`, ported over
/// same-schema names (Go uses `ctwl_db`/`ctwl_db1`; this port stays in
/// `test`):
///
/// * the copy inherits columns/indexes/PK but starts empty, resets the
///   auto-increment counter (Go rows `10 1` then `1 11`), and strips the
///   source's foreign keys while keeping `PKIsHandle` and the NOT NULL flag
///   (Go `serial_test.go:122-139`);
/// * a partitioned source copies the partitioning and no rows (Go
///   `:154-157`);
/// * a missing source table is 1146 whatever the target shape, a missing
///   target schema is 1049, an existing target is 1050 (Go `:203-212`); and
/// * a view or sequence source is 1347 `'test.v' is not BASE TABLE` (Go
///   `:215-220`, message from `dbterror.ErrWrongObject.GenWithStackByArgs` at
///   `pkg/ddl/create_table.go:1259` with MySQL's ErrWrongObject template).
#[test]
fn create_table_like_copies_structure_without_rows_fks_or_autoinc_and_reports_go_errors() {
    let mut tk = TestKit::new();

    // Go `serial_test.go:114-124` (same-database arms).
    tk.must_exec("create table tt(id int primary key)");
    tk.must_exec(
        "create table t (c1 int not null auto_increment, c2 int, constraint cc foreign key (c2) references tt(id), primary key(c1)) auto_increment = 10",
    );
    tk.must_exec("set @@foreign_key_checks=0");
    tk.must_exec("insert into t set c2=1");
    tk.must_exec("create table t1 like test.t");
    tk.must_exec("insert into t1 set c2=11");
    tk.must_exec("create table t2 (like test.t)");
    tk.must_exec("insert into t2 set c2=12");
    tk.must_query("select * from t").check(["10 1"]);
    tk.must_query("select * from t1").check(["1 11"]);
    tk.must_query("select * from t2").check(["1 12"]);

    // Go `serial_test.go:125-139`: the copies have NO foreign keys, ARE
    // PKIsHandle, and their first column keeps NOT NULL.
    for name in ["t1", "t2"] {
        let TableEntry::Kv(table) = tk.get_table_by_name("test", name) else {
            panic!("{name} should be a KV table");
        };
        assert!(table.foreign_keys().is_empty(), "{name} must strip FKs");
        assert_eq!(table.pk_handle_offset(), Some(0), "{name} PKIsHandle");
        assert!(
            table.columns()[0]
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL),
            "{name} first column NOT NULL"
        );
    }

    // Go `serial_test.go:154-157`: a partitioned source copies its
    // partitioning, with no rows.
    tk.must_exec(
        "create table pt1 (id int) partition by range columns (id) (partition p0 values less than (10))",
    );
    tk.must_exec("insert into pt1 values (1),(2),(3),(4)");
    tk.must_exec("create table pt2 like test.pt1");
    // Go: the like copy of a partitioned table starts empty.
    tk.must_query("select * from pt2").check_empty();
    let TableEntry::Kv(pt2) = tk.get_table_by_name("test", "pt2") else {
        panic!("pt2 should be a KV table");
    };
    assert!(pt2.partition().is_some(), "pt2 must copy the partitioning");

    // These failure cases use fresh target names. Go's existing-target
    // source-resolution precedence is a separate coverage obligation.
    let error = tk.must_get_err_code("create table lk1 like test_not_exist.t", 1146);
    assert_eq!(
        error.message, "Table 'test_not_exist.t' doesn't exist",
        "Go mysql.ErrNoSuchTable"
    );
    tk.must_get_err_code("create table lk2 like test.t_not_exist", 1146);
    tk.must_get_err_code("create table lk3 (like test_not_exist.t)", 1146);
    // Go mysql.ErrBadDB.
    tk.must_get_err_code("create table test_not_exis.lk4 like test.t", 1049);
    // Go mysql.ErrTableExists.
    tk.must_get_err_code("create table t1 like test.t", 1050);

    // Go `serial_test.go:215-220`: wrong object kinds.
    tk.must_exec("create view v as select 1 from dual");
    let error = tk.must_get_err_code("create table viewTable like v", 1347);
    assert_eq!(error.message, "'test.v' is not BASE TABLE", "Go mysql.ErrWrongObject");

    tk.must_exec("create sequence seq");
    let error = tk.must_get_err_code("create table sequenceTable like seq", 1347);
    assert_eq!(error.message, "'test.seq' is not BASE TABLE");

    // Go `serial_test.go:227-229`: `create table cc like
    // information_schema.columns` parses and creates (the insert-from-columns
    // arm needs an information_schema scan, recorded as a gap).
    tk.must_exec("create table cc like information_schema.columns");
}

/// CREATE TABLE LIKE temporary-scope cases from Go
/// `TestCreateTableWithLikeAtTemporaryMode`; remaining scope combinations
/// are separate coverage obligations in the parity audit.
#[test]
fn create_table_like_at_temporary_mode_refusals_match_go() {
    let mut tk = TestKit::new();

    // Go `serial_test.go:339-349`: like a GLOBAL temporary source is refused
    // with ErrOptOnTemporaryTable("create table like") whatever the target
    // scope (Go `checkCreateTableGrammar`, preprocess).
    tk.must_exec("create global temporary table tb5(id int) on commit delete rows");
    for sql in [
        "create table tb6 like tb5",
        "create global temporary table tb8 like tb5 on commit delete rows",
    ] {
        let error = tk.must_get_err_code(sql, 8006);
        assert_eq!(
            error.message, "`create table like` is unsupported on temporary tables.",
            "{sql}"
        );
    }

    // Go `serial_test.go:247-252`: an AUTO_RANDOM source cannot be copied
    // into a temporary table (checkReferInfoForTemporaryTable, "auto_random").
    tk.must_exec(
        "create table auto_random_table (a bigint primary key auto_random(3), b varchar(255))",
    );
    let error = tk.must_get_err_code(
        "create global temporary table auto_random_temporary_global like auto_random_table on commit delete rows",
        8006,
    );
    assert_eq!(error.message, "`auto_random` is unsupported on temporary tables.");

    // Go `serial_test.go:277` and `:389`: a partitioned source answers
    // errno.ErrPartitionNoTemporary (1562) for both temporary scopes.
    tk.must_exec(
        "create table global_partition_table (a int, b int) partition by hash(a) partitions 3",
    );
    for sql in [
        "create global temporary table global_partition_temp_table like global_partition_table ON COMMIT DELETE ROWS;",
        "create temporary table tmp_partition_table like global_partition_table",
    ] {
        let error = tk.must_get_err_code(sql, 1562);
        assert_eq!(
            error.message, "Cannot create temporary table with partitions",
            "{sql}"
        );
    }

    // Go `serial_test.go:268-271` (`alter table <global temp> shard_row_id_bits = 4`):
    // refused with ErrOptOnTemporaryTable("shard_row_id_bits") from the DDL
    // layer (pkg/ddl/executor.go:2172).
    tk.must_exec(
        "create global temporary table shard_row_id_temporary_table_plus (a int) on commit delete rows",
    );
    let error = tk.must_get_err_code(
        "alter table shard_row_id_temporary_table_plus shard_row_id_bits = 4",
        8006,
    );
    assert_eq!(
        error.message,
        "`shard_row_id_bits` is unsupported on temporary tables."
    );

    // Go `serial_test.go:404-414`: a placement-policy source is refused for
    // both temporary scopes with ErrOptOnTemporaryTable("placement").
    tk.must_exec("create placement policy p1 primary_region='r1' regions='r1,r2'");
    tk.must_exec("create table placement_table1(id int) placement policy p1");
    for sql in [
        "create global temporary table g_tmp_placement1 like placement_table1 on commit delete rows",
        "create temporary table l_tmp_placement1 like placement_table1",
    ] {
        let error = tk.must_get_err_code(sql, 8006);
        assert_eq!(
            error.message, "`placement` is unsupported on temporary tables.",
            "{sql}"
        );
    }

    // Go `serial_test.go:352-357` (tb11/tb12): a LOCAL temporary copy of a
    // normal table works; Go observes its kind through `show create table`
    // (`CREATE TEMPORARY TABLE`).
    tk.must_exec("create table tb11 (i int primary key, j int)");
    tk.must_exec("create temporary table tb12 like tb11");
    tk.must_query("show create table tb12")
        .check_contain("CREATE TEMPORARY TABLE `tb12`");

    // Go `serial_test.go:363-373` (tb13..tb16): local-to-local and
    // local-source copies are refused with "create table like".
    tk.must_exec("create temporary table tb13 (i int primary key, j int)");
    tk.must_get_err_code("create temporary table tb14 like tb13", 8006);
    tk.must_get_err_code("create table tb16 like tb13", 8006);

    // Go `serial_test.go:392-401`: a source carrying foreign keys copies
    // WITHOUT them into a LOCAL temporary table. Go reads the session's
    // txn info schema; the local temporary table is session state, so the
    // port observes it through the session's `show create table`.
    tk.must_exec("create table foreign_key_table1 (a int, b int, index(b))");
    tk.must_exec(
        "create table foreign_key_table2 (c int, d int, foreign key (d) references foreign_key_table1 (b))",
    );
    tk.must_exec("create temporary table foreign_key_tmp like foreign_key_table2");
    // Go: the copy carries 0 foreign keys.
    tk.must_query("show create table foreign_key_tmp")
        .check_not_contain("FOREIGN KEY");

    // Go `serial_test.go:279-304` (test_gv_ddl): the generated-column shape
    // survives the copy (virtual stays virtual, stored stays stored). Go
    // checks this through `DESC` output plus the persisted GeneratedExpr
    // strings; the port pins the persisted metadata, which is what DESC
    // renders from.
    tk.must_exec(
        "create table test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored)",
    );
    tk.must_exec(
        "create global temporary table test_gv_ddl_temp like test_gv_ddl on commit delete rows;",
    );
    let TableEntry::Kv(gv) = tk.get_table_by_name("test", "test_gv_ddl_temp") else {
        panic!("test_gv_ddl_temp should be a KV table");
    };
    let generated: Vec<Option<bool>> = gv
        .columns()
        .iter()
        .map(|column| column.generated.as_ref().map(|generated| generated.stored))
        .collect();
    assert_eq!(
        generated,
        vec![None, Some(false), Some(true)],
        "Go serial_test.go:290-297: ``, `a` + 8` VIRTUAL, ``b` + 2` STORED"
    );
    // Go `serial_test.go:300-304`: rows written inside a transaction are
    // readable before commit (ON COMMIT DELETE ROWS clears them at commit).
    tk.must_exec("begin");
    tk.must_exec("insert into test_gv_ddl_temp values (1, default, default)");
    let rows = tk.must_query("select * from test_gv_ddl_temp");
    assert_eq!(rows.rows().len(), 1, "Go: `select *` -> `1 9 11`");
    tk.must_exec("commit");
}

/// Go `serial_test.go:255-260` (`table_pre_split`, shard_row_id_bits=2
/// pre_split_regions=2): copying it into a GLOBAL temporary table answers
/// ErrOptOnTemporaryTable("pre split regions") from
/// `checkReferInfoForTemporaryTable` (pkg/planner/core/preprocess.go:1560).
#[test]
fn create_global_temporary_like_pre_split_source_answers_pre_split_regions_error() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table table_pre_split (id int) shard_row_id_bits = 2 pre_split_regions = 2",
    );
    let error = tk.must_get_err_code(
        "create global temporary table table_pre_split_tmp like table_pre_split on commit delete rows",
        8006,
    );
    assert_eq!(
        error.message,
        "`pre split regions` is unsupported on temporary tables."
    );
}

/// Go `serial_test.go:263-266` (`shard_row_id_table`, shard_row_id_bits=5):
/// copying it into a GLOBAL temporary table answers
/// ErrOptOnTemporaryTable("shard_row_id_bits")
/// (pkg/planner/core/preprocess.go:1566-1568), and the same refusal hits a
/// LOCAL temporary copy (`tmp_shard_row_id`, Go `:381-384`).
#[test]
fn create_temporary_like_shard_row_id_source_answers_shard_row_id_bits_error() {
    let mut tk = TestKit::new();
    tk.must_exec("create table shard_row_id_table (id int) shard_row_id_bits = 5");

    for sql in [
        "create global temporary table shard_row_id_global like shard_row_id_table on commit delete rows",
        "create temporary table shard_row_id_local like shard_row_id_table",
    ] {
        let error = tk.must_get_err_code(sql, 8006);
        assert_eq!(
            error.message, "`shard_row_id_bits` is unsupported on temporary tables.",
            "{sql}"
        );
    }
}

/// Go `serial_test.go:358-360`: `create temporary table if not exists tb12
/// like tb11` over the existing `test.tb12` answers OK with warning[0] ==
/// `infoschema.ErrTableExists.GenWithStackByArgs("test.tb12").Error()`
/// ("Table 'test.tb12' already exists").
#[test]
fn create_temporary_if_not_exists_over_existing_table_files_a_1050_warning() {
    let mut tk = TestKit::new();
    tk.must_exec("create table tb11 (i int primary key, j int)");
    tk.must_exec("create temporary table tb12 like tb11");

    // The IF NOT EXISTS duplicate is successful, with one Note.
    tk.must_exec("create temporary table if not exists tb12 like tb11");
    tk.must_query("show warnings")
        .check(["Note 1050 Table 'test.tb12' already exists"]);
}

/// Go `serial_test.go:203-204` runs `create table t1 like test_not_exist.t`
/// over an EXISTING `t1` and still answers `mysql.ErrNoSuchTable` (1146):
/// preprocessing resolves the LIKE source before the DDL layer's
/// `infoschema.ErrTableExists` check fires.
#[test]
fn create_table_like_missing_source_wins_over_existing_target() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t1 (a int)");
    let error = tk.must_get_err_code("create table t1 like test_not_exist.t", 1146);
    assert_eq!(error.message, "Table 'test_not_exist.t' doesn't exist");
}

/// Go `createSessionTemporaryTable` finds its schema through
/// `is.SchemaByName`, which serves the virtual schemas too: a local temporary
/// table can be created in information_schema (gorun).
#[test]
fn local_temporary_table_schema_is_found_among_virtual_schemas() {
    let mut tk = TestKit::new();
    tk.must_exec("create temporary table information_schema.zz (a int)");
}

/// Go `CreateTable` resolves a LIKE source through `is.TableByName`, which
/// serves the virtual schemas every infoschema carries (`initVirtualTables`):
/// information_schema, performance_schema and metrics_schema tables are all
/// copyable sources (gorun).
#[test]
fn create_table_like_a_virtual_schema_table_copies_its_columns() {
    let mut tk = TestKit::new();
    for (target, source) in [
        ("v_is", "information_schema.tables"),
        ("v_ps", "performance_schema.events_statements_summary_by_digest"),
        ("v_ms", "metrics_schema.up"),
        ("v_mysql", "mysql.user"),
    ] {
        tk.must_exec(&format!("create table {target} like {source}"));
        let (schema, table) = source.split_once('.').unwrap();
        let expected = tk
            .must_query(&format!(
                "select count(*) from information_schema.columns \
                 where table_schema = '{schema}' and table_name = '{table}'"
            ))
            .rows()[0][0]
            .clone();
        tk.must_query(&format!(
            "select count(*) from information_schema.columns \
             where table_schema = 'test' and table_name = '{target}'"
        ))
        .check([expected.as_str()]);
    }
}
