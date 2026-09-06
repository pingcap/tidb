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

//! Go `pkg/util/schemacmp/table_test.go` near 1:1.
//!
//! Go's `toTableInfo` runs `ddl.BuildTableInfoFromAST` under
//! `metabuild.NewNonStrictContext()` (non-strict mode, clustered index
//! default ON). The Rust counterpart is `tidb_exec`'s `build_table_info`,
//! whose bootstrap context is likewise non-strict; generated columns are
//! outside that builder's admission set, so this helper stages them exactly
//! the way Go's DDL does: strip the AST option, then stamp
//! `generated_expr_string`/`generated_stored` on the built column.

use tidb_exec::table_info_build::{build_table_info, ClusteredIndexDefMode};
use tidb_model::table_info::TableInfo;
use tidb_schemacmp::{decode_column_field_types, encode, IncompatibleError, Table};

fn to_table_info(create_table_stmt: &str) -> TableInfo {
    let statement = tidb_parser::parse(create_table_stmt).expect("the test SQL parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("not a create table statement");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("not a create table statement");
    };
    let mut create = create.clone();

    // Stage generated columns around the builder (see the module doc).
    let mut generated: Vec<(String, String, bool)> = Vec::new();
    for column in &mut create.columns {
        let name = column.name.clone();
        column.options.retain(|option| match option {
            tidb_ast::ColumnOption::Generated {
                expression, stored, ..
            } => {
                generated.push((name.clone(), expression.restore(), *stored));
                false
            }
            _ => true,
        });
    }

    let table_info = build_table_info(&create, "utf8mb4", "utf8mb4_bin", ClusteredIndexDefMode::On)
        .expect("the test SQL builds a table");
    for (name, expr, stored) in generated {
        let column = table_info
            .columns
            .iter_deref()
            .find(|column| column.read().name.original() == name)
            .expect("the generated column exists");
        let mut column = column.write();
        column.generated_expr_string = expr;
        column.generated_stored = stored;
    }
    table_info
}

fn check_decode_field_types(info: &TableInfo, tt: &Table) {
    let field_typs = decode_column_field_types(tt);
    assert_eq!(field_typs.len(), info.columns.len());
    for col in info.columns.iter_deref() {
        let col = col.read();
        let typ = field_typs
            .get(col.name.original())
            .expect("every column decodes");
        assert_eq!(typ, &col.field_type);
    }
}

fn assert_regexp(pattern: &str, error: &IncompatibleError) {
    let re = regex::Regex::new(pattern).expect("the Go test pattern compiles");
    assert!(
        re.is_match(&error.to_string()),
        "error {error:?} does not match {pattern:?}"
    );
}

fn table_eq(a: &Table, b: &Table) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

struct Case {
    name: &'static str,
    a: &'static str,
    b: &'static str,
    cmp: i32,
    cmp_err: &'static str,
    join: &'static str,
    join_err: &'static str,
}

impl Case {
    const fn ordered(
        name: &'static str,
        a: &'static str,
        b: &'static str,
        cmp: i32,
        join: &'static str,
    ) -> Self {
        Self {
            name,
            a,
            b,
            cmp,
            cmp_err: "",
            join,
            join_err: "",
        }
    }

    const fn cmp_err(
        name: &'static str,
        a: &'static str,
        b: &'static str,
        cmp_err: &'static str,
        join: &'static str,
    ) -> Self {
        Self {
            name,
            a,
            b,
            cmp: 0,
            cmp_err,
            join,
            join_err: "",
        }
    }

    const fn errs(
        name: &'static str,
        a: &'static str,
        b: &'static str,
        cmp_err: &'static str,
        join_err: &'static str,
    ) -> Self {
        Self {
            name,
            a,
            b,
            cmp: 0,
            cmp_err,
            join: "",
            join_err,
        }
    }
}

// Go `TestJoinSchemas`.
#[test]
fn test_join_schemas() {
    let test_cases = vec![
        Case::ordered(
            "DM_002/1",
            "CREATE TABLE tb1 (col1 INT)",
            "CREATE TABLE tb2 (col1 INT, new_col1 INT)",
            -1,
            "CREATE TABLE tb3 (col1 INT, new_col1 INT)",
        ),
        Case::ordered(
            "DM_002/1/unordered",
            "CREATE TABLE tb1 (col1 INT)",
            "CREATE TABLE tb2 (new_col1 INT, col1 INT)",
            -1,
            "CREATE TABLE tb3 (new_col1 INT, col1 INT)",
        ),
        Case::ordered(
            "DM_002/2",
            "CREATE TABLE tb1 (col1 INT, new_col1 INT)",
            "CREATE TABLE tb2 (col1 INT, new_col1 INT)",
            0,
            "CREATE TABLE tb3 (col1 INT, new_col1 INT)",
        ),
        Case::ordered(
            "DM_002/2/unordered",
            "CREATE TABLE tb1 (col1 INT, new_col1 INT)",
            "CREATE TABLE tb2 (new_col1 INT, col1 INT)",
            0,
            "CREATE TABLE tb3 (col1 INT, new_col1 INT)",
        ),
        Case::ordered(
            "DM_010",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
            1,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
        ),
        Case::errs(
            "DM_011",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 FLOAT)",
            r#".*"new_col1".*incompatible mysql type.*"#,
            r#".*"new_col1".*incompatible mysql type.*"#,
        ),
        Case::cmp_err(
            "DM_014",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col2 INT)",
            r".*combining contradicting orders.*",
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
        ),
        Case::errs(
            "DM_031/VARCHAR",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 VARCHAR(10))",
            r#".*"new_col1".*incompatible mysql type.*"#,
            r#".*"new_col1".*incompatible mysql type.*"#,
        ),
        Case::errs(
            "DM_031/TEXT",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 TEXT)",
            r#".*"new_col1".*incompatible mysql type.*"#,
            r#".*"new_col1".*incompatible mysql type.*"#,
        ),
        Case::errs(
            "DM_031/JSON",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 JSON)",
            r#".*"new_col1".*incompatible mysql type.*"#,
            r#".*"new_col1".*incompatible mysql type.*"#,
        ),
        Case::cmp_err(
            "DM_033",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), c FLOAT NOT NULL)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
            r#".*"c": column with no default value cannot be missing"#,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), c FLOAT NOT NULL DEFAULT 0)",
        ),
        Case::errs(
            "DM_034",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT UNIQUE AUTO_INCREMENT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
            r".*combining contradicting orders.*",
            r#".*"new_col1".*auto type but not defined as a key"#,
        ),
        Case::ordered(
            "DM_035",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 INT, col2 INT)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), col2 INT, col1 INT)",
            0,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), col1 INT, col2 INT)",
        ),
        Case::errs(
            "DM_037",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 INT DEFAULT 0)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), col1 INT DEFAULT -1)",
            r#".*"col1".*distinct singletons.*"#,
            r#".*"col1".*distinct singletons.*"#,
        ),
        Case::ordered(
            "DM_039/1",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
            1,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
        ),
        Case::ordered(
            "DM_039/2",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
            0,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
        ),
        Case::ordered(
            "DM_040",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8mb4 COLLATE utf8mb4_bin)",
            -1,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8mb4 COLLATE utf8mb4_bin)",
        ),
        Case::ordered(
            "latin1_to_utf8mb4",
            "CREATE TABLE tb1 (a INT, col1 VARCHAR(10) CHARSET latin1 COLLATE latin1_bin)",
            "CREATE TABLE tb2 (a INT, col1 VARCHAR(10) CHARSET utf8mb4 COLLATE utf8mb4_bin)",
            -1,
            "CREATE TABLE tb3 (a INT, col1 VARCHAR(10) CHARSET utf8mb4 COLLATE utf8mb4_bin)",
        ),
        Case::ordered(
            "DM_041/1",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
            1,
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
        ),
        Case::ordered(
            "DM_041/2",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
            0,
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
        ),
        Case::ordered(
            "DM_042",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) STORED)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
            1,
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) STORED)",
        ),
        Case::errs(
            "DM_043",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 INT AS (a + 2))",
            r#".*"new_col1".*distinct singletons.*"#,
            r#".*"new_col1".*distinct singletons.*"#,
        ),
        Case::errs(
            "DM_044",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) VIRTUAL)",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) STORED)",
            r#".*"new_col1".*distinct singletons.*"#,
            r#".*"new_col1".*distinct singletons.*"#,
        ),
        Case::cmp_err(
            "DM_052",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
            "CREATE TABLE tb2 (c BIGINT, b VARCHAR(10))",
            r".*combining contradicting orders.*",
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), c BIGINT)",
        ),
        Case::errs(
            "DM_053",
            "CREATE TABLE tb1 (c BIGINT, b VARCHAR(10))",
            "CREATE TABLE tb2 (c DOUBLE, b VARCHAR(10))",
            r#".*"c".*incompatible mysql type.*"#,
            r#".*"c".*incompatible mysql type.*"#,
        ),
        Case::ordered(
            "DM_055",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
            "CREATE TABLE tb2 (a BIGINT, b VARCHAR(10))",
            -1,
            "CREATE TABLE tb2 (a BIGINT, b VARCHAR(10))",
        ),
        Case::cmp_err(
            "DM_057",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
            "CREATE TABLE tb2 (c INT DEFAULT 1, b VARCHAR(10))",
            r".*combining contradicting orders.*",
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), c INT DEFAULT 1)",
        ),
        Case::ordered(
            "DM_061",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10) CHARSET utf8)",
            1,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10))",
        ),
        Case::ordered(
            "DM_066",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
            "CREATE TABLE tb2 (a INT DEFAULT 1, b VARCHAR(10))",
            -1,
            "CREATE TABLE tb3 (a INT DEFAULT 1, b VARCHAR(10))",
        ),
        // The Go test keeps DM_074 commented out: those table options are
        // somehow ignored by the parser.
        Case::ordered(
            "DM_078",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
            "CREATE TABLE tb2 (a INT PRIMARY KEY, b VARCHAR(10))",
            1,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10))",
        ),
        Case::ordered(
            "DM_080/1",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b), UNIQUE KEY idx_ab(a, b))",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
            -1,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10))",
        ),
        Case::ordered(
            "DM_080/2",
            "CREATE TABLE tb1 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b), UNIQUE KEY idx_ab(a, b))",
            "CREATE TABLE tb2 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b))",
            -1,
            "CREATE TABLE tb3 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b))",
        ),
        // The Go test keeps DM_086 commented out: index visibility is not
        // visible in IndexInfo yet.
        Case::cmp_err(
            "Different index components",
            "CREATE TABLE tbl1 (a INT, b INT, KEY i(a))",
            "CREATE TABLE tbl2 (a INT, b INT, KEY i(b))",
            r".*combining contradicting orders.*",
            "CREATE TABLE tbl3 (a INT, b INT)",
        ),
        Case::cmp_err(
            "Different index order",
            "CREATE TABLE tbl1 (a INT, b INT, KEY i(a, b))",
            "CREATE TABLE tbl2 (a INT, b INT, KEY i(b, a))",
            r".*combining contradicting orders.*",
            "CREATE TABLE tbl3 (a INT, b INT)",
        ),
        Case::cmp_err(
            "Different index length",
            "CREATE TABLE tbl1 (a TEXT, KEY i(a(14)))",
            "CREATE TABLE tbl2 (a TEXT, KEY i(a(15)))",
            r".*distinct singletons.*",
            "CREATE TABLE tbl3 (a TEXT)",
        ),
        Case::errs(
            "Cannot drop key tied to AUTO_INC column",
            "CREATE TABLE tbl1(a INT AUTO_INCREMENT, b INT, KEY i(a))",
            "CREATE TABLE tbl2(a INT AUTO_INCREMENT, b INT, KEY i(a, b))",
            r".*distinct singletons.*",
            r#".*"a".*auto type but not defined as a key"#,
        ),
        Case::cmp_err(
            "not-null column with special types",
            "CREATE TABLE tbl1(
				a1 INT NOT NULL,
				b1 DECIMAL NOT NULL,
				c1 VARCHAR(20) NOT NULL,
				d1 DATETIME(3) NOT NULL,
				e1 ENUM('abc', 'def') NOT NULL
			)",
            "CREATE TABLE tbl2(
				a2 TIME NOT NULL,
				b2 DATE NOT NULL,
				c2 BINARY(50) NOT NULL,
				d2 YEAR(4) NOT NULL,
				e2 SET('abc', 'def') NOT NULL
			)",
            r".*column with no default value cannot be missing",
            "CREATE TABLE tbl3(
				a1 INT NOT NULL DEFAULT 0,
				b1 DECIMAL NOT NULL DEFAULT 0,
				c1 VARCHAR(20) NOT NULL DEFAULT '',
				d1 DATETIME(3) NOT NULL DEFAULT '0000-00-00 00:00:00',
				e1 ENUM('abc', 'def') NOT NULL DEFAULT 'abc',
				a2 TIME NOT NULL DEFAULT '00:00:00',
				b2 DATE NOT NULL DEFAULT '0000-00-00',
				c2 BINARY(50) NOT NULL DEFAULT '',
				d2 YEAR(4) NOT NULL DEFAULT '0000',
				e2 SET('abc', 'def') NOT NULL DEFAULT ''
			)",
        ),
        Case::ordered(
            "test case 2020-03-17",
            "CREATE TABLE bar (id INT PRIMARY KEY)",
            "CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)",
            -1,
            "CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)",
        ),
        Case::ordered(
            "test case 2020-03-17-alt",
            "CREATE TABLE bar (id VARCHAR(10) PRIMARY KEY)",
            "CREATE TABLE bar (id VARCHAR(10) PRIMARY KEY, c1 INT)",
            -1,
            "CREATE TABLE bar (id VARCHAR(10) PRIMARY KEY, c1 INT)",
        ),
        Case::ordered(
            "test case 2020-03-17-alt-2",
            "CREATE TABLE bar (id INT PRIMARY KEY)",
            "CREATE TABLE bar (id INT, c1 INT)",
            -1,
            "CREATE TABLE bar (id INT, c1 INT)",
        ),
        Case::cmp_err(
            "test case 2020-03-17-alt-3",
            "CREATE TABLE bar (id1 INT PRIMARY KEY, id2 INT)",
            "CREATE TABLE bar (id1 INT, id2 INT PRIMARY KEY)",
            r".*combining contradicting orders.*",
            "CREATE TABLE bar (id1 INT, id2 INT)",
        ),
        Case::ordered(
            "test case 2020-04-28-blob",
            "CREATE TABLE tb1 (a BLOB, b VARCHAR(10))",
            "CREATE TABLE tb2 (a LONGBLOB, b VARCHAR(10))",
            -1,
            "CREATE TABLE tb2 (a LONGBLOB, b VARCHAR(10))",
        ),
        Case::ordered(
            "join equal single primary key",
            "CREATE TABLE t(a INT, b INT, PRIMARY KEY(a))",
            "CREATE TABLE t(a INT, b INT, PRIMARY KEY(a))",
            0,
            "CREATE TABLE t(a INT, b INT, PRIMARY KEY(a))",
        ),
        Case::ordered(
            "join equal composite primary key",
            "CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b))",
            "CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b))",
            0,
            "CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b))",
        ),
        Case::ordered(
            "join equal single index",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX idx_b(b))",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX idx_b(b))",
            0,
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX idx_b(b))",
        ),
        Case::ordered(
            "join equal unique index",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY uni_b(b))",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY uni_b(b))",
            0,
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY uni_b(b))",
        ),
        Case::ordered(
            "join equal composite index",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX idx_bc(b, c))",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX idx_bc(b, c))",
            0,
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX idx_bc(b, c))",
        ),
        Case::ordered(
            "join equal composite unique index",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, UNIQUE INDEX idx_bc(b, c))",
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, UNIQUE INDEX idx_bc(b, c))",
            0,
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, UNIQUE INDEX idx_bc(b, c))",
        ),
    ];

    for tc in &test_cases {
        let tia = to_table_info(tc.a);
        let tib = to_table_info(tc.b);

        let a = encode(&tia);
        let b = encode(&tib);
        check_decode_field_types(&tia, &a);
        check_decode_field_types(&tib, &b);
        let j = if tc.join_err.is_empty() {
            let tij = to_table_info(tc.join);
            Some(encode(&tij))
        } else {
            None
        };

        let cmp = a.compare(&b);
        if !tc.cmp_err.is_empty() {
            assert_regexp(tc.cmp_err, &cmp.unwrap_err());
        } else {
            assert_eq!(cmp.unwrap(), tc.cmp, "case {}", tc.name);
        }

        let cmp = b.compare(&a);
        if !tc.cmp_err.is_empty() {
            assert_regexp(tc.cmp_err, &cmp.unwrap_err());
        } else {
            assert_eq!(cmp.unwrap(), -tc.cmp, "case {}", tc.name);
        }

        let joined = a.join(&b);
        if !tc.join_err.is_empty() {
            assert_regexp(tc.join_err, &joined.unwrap_err());
        } else {
            let joined = joined.unwrap();
            assert!(
                table_eq(&joined, j.as_ref().expect("a join expectation")),
                "case {}: joined = {joined}",
                tc.name
            );
        }

        let joined = b.join(&a);
        if !tc.join_err.is_empty() {
            assert_regexp(tc.join_err, &joined.unwrap_err());
        } else {
            let joined = joined.unwrap();
            assert!(
                table_eq(&joined, j.as_ref().expect("a join expectation")),
                "case {}: joined = {joined}",
                tc.name
            );
            let cmp = joined.compare(&a).unwrap();
            assert!(cmp >= 0);

            let cmp = joined.compare(&b).unwrap();
            assert!(cmp >= 0);
        }
    }
}

// Go `TestTableString`.
#[test]
fn test_table_string() {
    // [input, expect]
    let cases: [[&str; 2]; 7] = [
        [
            "CREATE TABLE tb (a INT, b INT)",
            // the `tbl` name is hardcoded, it's not used.
            "create table `tbl`(`a` int(11), `b` int(11)) collate utf8mb4_bin",
        ],
        [
            "CREATE TABLE tb (a VARCHAR(20) CHARACTER SET utf8, b INT)",
            "create table `tbl`(`a` varchar(20) character set utf8 collate utf8_bin, `b` int(11)) collate utf8mb4_bin",
        ],
        [
            "CREATE TABLE tb (a VARCHAR(20), b INT) COLLATE utf8mb4_general_ci",
            "create table `tbl`(`a` varchar(20) character set utf8mb4 collate utf8mb4_general_ci, `b` int(11)) collate utf8mb4_general_ci",
        ],
        [
            "CREATE TABLE tb (a VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, b INT)",
            "create table `tbl`(`a` varchar(20) character set utf8mb4 collate utf8mb4_general_ci, `b` int(11)) collate utf8mb4_bin",
        ],
        [
            "CREATE TABLE tb (a VARCHAR(20)) CHARSET=binary",
            "create table `tbl`(`a` varbinary(20)) collate binary",
        ],
        [
            "CREATE TABLE tb (a VARCHAR(20)) COLLATE=binary",
            "create table `tbl`(`a` varbinary(20)) collate binary",
        ],
        [
            "CREATE TABLE tb (a VARCHAR(20)) CHARSET=binary COLLATE=binary",
            "create table `tbl`(`a` varbinary(20)) collate binary",
        ],
    ];

    for tc in &cases {
        let ti = to_table_info(tc[0]);
        let sql = encode(&ti).to_string().to_lowercase();
        assert_eq!(sql, tc[1]);
    }
}
