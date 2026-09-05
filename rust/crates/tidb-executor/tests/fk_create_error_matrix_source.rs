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

//! Ports of Go `TestCreateTableWithForeignKeyError`
//! (`pkg/ddl/tests/fk/foreign_key_test.go:476`, part12 item 714 of
//! `pkg/ddl`'s `func Test*`/`func Benchmark*` declarations sorted by file
//! and line), read from `origin/master`.
//!
//! The Go test is a 37-row error matrix plus a 9-row pass matrix, each row a
//! `CREATE TABLE` whose FOREIGN KEY clause must fail with an exact
//! `[code]message` (or succeed). This tier's carrier
//! (`rust/crates/tidb-executor/src/ddl/table_constraints.rs:434`
//! `build_foreign_key`, reached from both CREATE TABLE and ALTER) implements
//! a subset of Go's validations; where a row's behavior exists it is pinned
//! in the running test below with Go's exact errno and message, and where it
//! does not (or where the tier answers a DIFFERENT errno than Go) the row is
//! recorded as an explicit gap naming the divergence. Nothing is
//! approximated.

use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::{Catalog, StmtContext};

fn err(error: &tidb_executor::DriverError) -> (u16, String) {
    let mysql = error.clone().to_mysql_error();
    (mysql.code, mysql.message)
}

// The matrix rows the tier answers EXACTLY as Go does:
//   * parent-side VIRTUAL generated referenced column → Go row 7,
//     `[schema:3733]Foreign key 'fk_b' uses virtual column 'b' which is not
//     supported.`;
//   * child-side VIRTUAL generated referencing column → Go row 8, same 3733;
//   * referencing/referenced column-count mismatch → Go row 15,
//     `[schema:1239]Incorrect foreign key definition for 'fk_b': Key
//     reference and table reference don't match`.
//
// (Go rows counted from the `cases` slice at
// pkg/ddl/tests/fk/foreign_key_test.go:482.)
#[test]
fn fk_create_error_rows_the_tier_answers_like_go() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();

    // Go row 7: the referenced column is VIRTUAL generated.
    ddl::run_create_table_in(
        "create table t1 (id int key, a int, b int as (a) virtual, index(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let error = ddl::run_create_table_in(
        "create table t2 (a int, b int, foreign key fk_b(b) references t1(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go row 7: [schema:3733]");
    assert_eq!(
        err(&error),
        (
            3733,
            "Foreign key 'fk_b' uses virtual column 'b' which is not supported.".to_owned()
        )
    );

    // Go row 8: the REFERENCING column is VIRTUAL generated (unconditional
    // in both engines — Go reaches this from buildFKInfo, off the switch).
    ddl::run_create_table_in(
        "create table t1b (id int key, a int, b int, index(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let error = ddl::run_create_table_in(
        "create table t2b (a int, b int as (a) virtual, foreign key fk_b(b) references t1b(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go row 8: [schema:3733]");
    assert_eq!(
        err(&error),
        (
            3733,
            "Foreign key 'fk_b' uses virtual column 'b' which is not supported.".to_owned()
        )
    );

    // Go row 15: key reference and table reference don't match (one
    // referencing column against two referenced ones).
    ddl::run_create_table_in(
        "create table t1c (id int key, a int, index(a))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let error = ddl::run_create_table_in(
        "create table t2c (a int, b int, foreign key fk_b(b) references t1c(id, a))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go row 15: [schema:1239]");
    assert_eq!(
        err(&error),
        (
            1239,
            "Incorrect foreign key definition for 'fk_b': Key reference and table reference don't match"
                .to_owned()
        )
    );
}

// Go rows 1-2 (`pkg/ddl/tests/fk/foreign_key_test.go:484-491`): a reference
// to a missing TABLE fails `[schema:1824]Failed to open the referenced table
// 'T_unknown'`, and a reference to a missing COLUMN of an existing table
// fails `[schema:3734]Failed to add the foreign key constraint. Missing
// column 'c_unknown' for constraint 'fk_b' in the referenced table 't1'`.
//
#[test]
fn fk_create_missing_referenced_table_and_column_report_fk_errnos() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t1 (id int, a int, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // Go row 1: "[schema:1824]Failed to open the referenced table 'T_unknown'".
    let error = ddl::run_create_table_in(
        "create table t2 (a int, b int, foreign key fk_b(b) references T_unknown(b))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go row 1");
    assert_eq!(
        err(&error),
        (
            1824,
            "Failed to open the referenced table 'T_unknown'".to_owned()
        )
    );

    // Go row 2: "[schema:3734]Failed to add the foreign key constraint.
    // Missing column 'c_unknown' for constraint 'fk_b' in the referenced
    // table 't1'".
    let error = ddl::run_create_table_in(
        "create table t2 (a int, b int, foreign key fk_b(b) references t1(c_unknown))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go row 2");
    assert_eq!(
        err(&error),
        (
            3734,
            "Failed to add the foreign key constraint. Missing column 'c_unknown' for constraint 'fk_b' in the referenced table 't1'".to_owned()
        )
    );
}

// Go rows 3-6, 9-14 (`pkg/ddl/tests/fk/foreign_key_test.go:492-528`):
//   * a referencing column the table does not define → `[ddl:1072]Key
//     column 'c_unknown' doesn't exist in table`;
//   * a referenced table whose columns lack a covering index →
//     `[schema:1822]Failed to add the foreign key constraint. Missing index
//     for constraint 'fk_b' in the referenced table 't1'` (rows 4, 13, 14 —
//     including the prefix-index row, where `index (a(5))` does NOT cover);
//   * SET NULL against a NOT NULL referenced/referencing column →
//     `[schema:1830]Column 'b' cannot be NOT NULL: needed in a foreign key
//     constraint 'fk_b' SET NULL` (rows 5-6, delete and update);
//   * type incompatibilities between referencing and referenced columns →
//     `[ddl:3780]Referencing column 'b' and referenced column '...' in
//     foreign key constraint '...' are incompatible.` (rows 9-12: varchar
//     vs int, signed vs unsigned, int vs bigint, charset utf8 vs utf8mb4,
//     collate utf8_bin vs utf8mb4_bin).
//
#[test]
fn fk_create_reference_compatibility_rows_match_go() {
    let cases = [
        (
            "create table t1 (id int key, a int, b int);",
            "create table t2 (a int, b int, foreign key fk(c_unknown) references t1(id));",
            1072,
            "Key column 'c_unknown' doesn't exist in table",
        ),
        (
            "create table t1 (id int, a int, b int);",
            "create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
            1822,
            "Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
        ),
        (
            "create table t1 (id int key, a int, b int not null, index(b));",
            "create table t2 (a int, b int not null, foreign key fk_b(b) references t1(b) on update set null);",
            1830,
            "Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
        ),
        (
            "create table t1 (id int key, a int, b int not null, index(b));",
            "create table t2 (a int, b int not null, foreign key fk_b(b) references t1(b) on delete set null);",
            1830,
            "Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
        ),
        (
            "create table t1 (id int key, a int);",
            "create table t2 (a int, b varchar(10), foreign key fk(b) references t1(id));",
            3780,
            "Referencing column 'b' and referenced column 'id' in foreign key constraint 'fk' are incompatible.",
        ),
        (
            "create table t1 (id int key, a int not null, index(a));",
            "create table t2 (a int, b int unsigned, foreign key fk_b(b) references t1(a));",
            3780,
            "Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
        ),
        (
            "create table t1 (id int key, a bigint, index(a));",
            "create table t2 (a int, b int, foreign key fk_b(b) references t1(a));",
            3780,
            "Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
        ),
        (
            "create table t1 (id int key, a varchar(10) charset utf8, index(a));",
            "create table t2 (a int, b varchar(10) charset utf8mb4, foreign key fk_b(b) references t1(a));",
            3780,
            "Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
        ),
        (
            "create table t1 (id int key, a varchar(10) collate utf8_bin, index(a));",
            "create table t2 (a int, b varchar(10) collate utf8mb4_bin, foreign key fk_b(b) references t1(a));",
            3780,
            "Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
        ),
        (
            "create table t1 (id int key, a varchar(10), index (a(5)));",
            "create table t2 (a int, b varchar(10), foreign key fk_b(b) references t1(a));",
            1822,
            "Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
        ),
    ];

    for (refer, create, code, message) in cases {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query();
        ddl::run_create_table_in(
            refer,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        )
        .unwrap();
        let error = ddl::run_create_table_in(
            create,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        )
        .expect_err("Go's compatibility matrix row must fail");
        assert_eq!(err(&error), (code, message.to_owned()), "{create}");
    }
}

// Go rows 16-18 (`pkg/ddl/tests/fk/foreign_key_test.go:530-538`): a table
// referencing ITSELF — `(a int key, foreign key (a) references t2(a))` and
// the two-column shapes — fails `[schema:1215]Cannot add foreign key
// constraint`, except the (a,b)→(b,a) shape which fails
// `[schema:1822]Failed to add the foreign key constraint. Missing index for
// constraint 'fk_1' in the referenced table 't2'`.
//
#[test]
fn fk_create_self_reference_rows_match_go() {
    let cases = [
        (
            "create table t2 (a int key, foreign key (a) references t2(a));",
            1215,
            "Cannot add foreign key constraint",
        ),
        (
            "create table t2 (a int, b int, index(a,b), index(b,a), foreign key (a,b) references t2(a,b));",
            1215,
            "Cannot add foreign key constraint",
        ),
        (
            "create table t2 (a int, b int, index(a,b), foreign key (a,b) references t2(b,a));",
            1822,
            "Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't2'",
        ),
    ];
    for (create, code, message) in cases {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query();
        let error = ddl::run_create_table_in(
            create,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        )
        .expect_err("Go's self-reference row must fail");
        assert_eq!(err(&error), (code, message.to_owned()), "{create}");
    }
}

// Go rows 19-20 (`pkg/ddl/tests/fk/foreign_key_test.go:539-551`): with
// `@@foreign_key_checks=0` a child may be created against a MISSING parent
// (`t2 ... references t1(id)` first), and then CREATING the parent must
// validate the deferred constraint: `(id int, a int)` → 1822 (no index on
// id) and `(id bigint key, a int)` → 3780 (type mismatch).
//
#[test]
fn fk_create_with_checks_off_defers_validation_to_the_parent() {
    let cases = [
        (
            "create table t1 (id int, a int);",
            1822,
            "Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'",
        ),
        (
            "create table t1 (id bigint key, a int);",
            3780,
            "Referencing column 'a' and referenced column 'id' in foreign key constraint 'fk_1' are incompatible.",
        ),
    ];
    for (parent, code, message) in cases {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query().with_foreign_key_checks(false);
        let settings = CreateTableSettings {
            foreign_key_checks: false,
            ..CreateTableSettings::default()
        };
        ddl::run_create_table_in(
            "create table t2 (a int, foreign key (a) references t1(id));",
            &mut catalog,
            "test",
            settings,
            &ctx,
        )
        .expect("Go permits the child-first create with checks off");
        let error = ddl::run_create_table_in(parent, &mut catalog, "test", settings, &ctx)
            .expect_err("Go re-validates deferred children when the parent lands");
        assert_eq!(err(&error), (code, message.to_owned()), "{parent}");
    }
}

// Go rows 21-24, 32-33 (`pkg/ddl/tests/fk/foreign_key_test.go:553-575,
// 596-606`): FOREIGN KEY is refused on TEMPORARY tables in every
// direction — a reference TO a (global) temporary table is 1824 or 1215,
// and a temporary table (local or global) DECLARING one is 1215.
//
// go-parity-gap: this tier's runners do not lower `CREATE TEMPORARY TABLE`
// through the FK builder at all (temporary-table registration lives behind
// the session overlay, `Catalog::register_local_temporary_in`), so the
// temporary-table FK refusals are unreachable from the CREATE path.
#[test]
#[ignore = "go-parity-gap: temporary-table FK refusals are unreachable from the create runner"]
fn fk_create_refuses_temporary_tables_in_both_directions() {
    // Contract (foreign_key_test.go:553-575, 596-606): FK to a temp parent
    // is 1824 (local) / 1215 (global); a temp child declaring an FK is 1215.
}

// Go rows 25-28, 31, 34-35 (`pkg/ddl/tests/fk/foreign_key_test.go:577-611`):
//   * an EMPTY constraint name (`foreign key \`\`(a) ...` or
//     `constraint \`\` foreign key ...`) → `[ddl:1280]Incorrect index name ''`;
//   * duplicate referencing columns (`foreign key (a,a) references t1(a,
//     b)`) → `[schema:1060]Duplicate column name 'a'`;
//   * an auto-created support index colliding with an explicit one
//     (`index fk_1(a), foreign key (b) references t1(b)`) →
//     `[ddl:1061]duplicate key name fk_1`;
//   * any identifier over MySQL's 64-char limit — the FK name, the
//     constraint name, the referenced schema/table, or a referenced column
//     — → `[ddl:1059]Identifier name '...' is too long` (5 rows).
#[test]
fn fk_create_name_shape_rows_match_go() {
    let long = "name5678901234567890123456789012345678901234567890123456789012345";
    let cases = vec![
        (
            None,
            "create table t1 (a int, foreign key ``(a) references t1(a));".to_owned(),
            1280,
            "Incorrect index name ''".to_owned(),
        ),
        (
            None,
            "create table t1 (a int, constraint `` foreign key (a) references t1(a));".to_owned(),
            1280,
            "Incorrect index name ''".to_owned(),
        ),
        (
            Some("create table t1 (a int, b int, index(a,b));"),
            "create table t2 (a int, b int, constraint fk foreign key (a,a) references t1(a,b));".to_owned(),
            1060,
            "Duplicate column name 'a'".to_owned(),
        ),
        (
            Some("create table t1 (a int, b int, index(a,b));"),
            "create table t2 (a int, b int, foreign key (a,b) references t1(a,a));".to_owned(),
            1822,
            "Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'".to_owned(),
        ),
        (
            Some("create table t1 (id int key, b int, index(b));"),
            "create table t2 (a int, b int, index fk_1(a), foreign key (b) references t1(b));".to_owned(),
            1061,
            "duplicate key name fk_1".to_owned(),
        ),
        (
            Some("create table t1 (id int key);"),
            format!("create table t2 (id int key, foreign key {long}(id) references t1(id));"),
            1059,
            format!("Identifier name '{long}' is too long"),
        ),
        (
            Some("create table t1 (id int key);"),
            format!("create table t2 (id int key, constraint {long} foreign key (id) references t1(id));"),
            1059,
            format!("Identifier name '{long}' is too long"),
        ),
        (
            None,
            format!("create table t2 (id int key, constraint fk foreign key (id) references {long}.t1(id));"),
            1059,
            format!("Identifier name '{long}' is too long"),
        ),
        (
            None,
            format!("create table t2 (id int key, constraint fk foreign key (id) references t1.{long}(id));"),
            1059,
            format!("Identifier name '{long}' is too long"),
        ),
        (
            Some("create table t1 (id int key);"),
            format!("create table t2 (id int key, constraint fk foreign key (id) references t1({long}));"),
            1059,
            format!("Identifier name '{long}' is too long"),
        ),
    ];

    for (refer, create, code, message) in cases {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query();
        if let Some(refer) = refer {
            ddl::run_create_table_in(
                refer,
                &mut catalog,
                "test",
                CreateTableSettings::default(),
                &ctx,
            )
            .unwrap();
        }
        let error = ddl::run_create_table_in(
            &create,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        )
        .expect_err("Go's FK name/shape row must fail");
        assert_eq!(err(&error), (code, message), "{create}");
    }
}

// Go rows 29-30 (`pkg/ddl/tests/fk/foreign_key_test.go:612-618`): FOREIGN
// KEY with PARTITIONING on either side fails
// `[schema:1506]Foreign key clause is not yet supported in conjunction with
// partitioning` — a child referencing a partitioned parent, and a
// partitioned child.
//
#[test]
fn fk_create_refuses_partitioning_on_either_side() {
    let cases = [
        (
            "create table t1 (id int key) partition by hash(id) partitions 3;",
            "create table t2 (id int key, constraint fk foreign key (id) references t1(id));",
        ),
        (
            "create table t1 (id int key);",
            "create table t2 (id int key, constraint fk foreign key (id) references t1(id)) partition by hash(id) partitions 3;",
        ),
    ];
    for (refer, create) in cases {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query();
        ddl::run_create_table_in(
            refer,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        )
        .unwrap();
        let error = ddl::run_create_table_in(
            create,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        )
        .expect_err("Go's partitioned FK row must fail");
        assert_eq!(
            err(&error),
            (
                1506,
                "Foreign key clause is not yet supported in conjunction with partitioning".to_owned()
            ),
            "{create}"
        );
    }
}

// Go's pass matrix (`pkg/ddl/tests/fk/foreign_key_test.go:645-747`): nine
// scenarios that must SUCCEED — self-referencing FK, a NOT NULL referenced
// column with SET NULL absent, wider varchar/decimal referencing columns,
// prefix-index coverage with the full length, an FK created with
// `@@foreign_key_checks=0` against a missing table, the (a,b)→(b,a) shape
// with both covering indexes, two FKs sharing one parent index, and a
// 64-character (legal-length) FK name.
//
#[test]
fn fk_create_pass_matrix_succeeds() {
    let cases = [
        (
            Some("create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))"),
            "create table t2 (id int key);",
            true,
        ),
        (
            Some("create table t1 (id int key, b int not null, index(b))"),
            "create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
            true,
        ),
        (
            Some("create table t1 (id int key, a varchar(10), index(a));"),
            "create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
            true,
        ),
        (
            Some("create table t1 (id int key, a decimal(10,5), index(a));"),
            "create table t2 (a int, b decimal(20, 10), foreign key fk_b(b) references t1(a));",
            true,
        ),
        (
            Some("create table t1 (id int key, a varchar(10), index (a(10)));"),
            "create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
            true,
        ),
        (
            None,
            "create table t2 (a int, b int, foreign key fk_b(b) references t_unknown(b));",
            false,
        ),
        (
            None,
            "create table t2 (a int, b int, index(a,b), index(b,a), foreign key (a,b) references t2(b,a));",
            true,
        ),
        (
            Some("create table t1 (a int key, b int, index(b))"),
            "create table t2 (a int, b int, foreign key (a) references t1(a), foreign key (b) references t1(b));",
            true,
        ),
        (
            Some("create table t1 (id int key);"),
            "create table t2 (id int key, foreign key name567890123456789012345678901234567890123456789012345678901234(id) references t1(id));",
            true,
        ),
    ];
    for (parent, create, checks) in cases {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query().with_foreign_key_checks(checks);
        let settings = CreateTableSettings {
            foreign_key_checks: checks,
            ..CreateTableSettings::default()
        };
        if let Some(parent) = parent {
            ddl::run_create_table_in(parent, &mut catalog, "test", settings, &ctx).unwrap();
        }
        ddl::run_create_table_in(create, &mut catalog, "test", settings, &ctx)
            .unwrap_or_else(|error| panic!("Go pass-matrix row must succeed: {create}: {error:?}"));
    }
}
