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

//! Filter truthiness in SQL boolean context: the port of Go
//! `pkg/expression/builtin_vectorized_test.go:836` `TestVecEvalBool` (with
//! `TestVectorizedFilterConsiderNull`, `:877`).
//!
//! # Why the port is a case table and not the Go loop
//!
//! Go's `TestVecEvalBool` is a *differential* test: it generates random
//! columns of each `EvalType` and asserts that the vectorized `VecEvalBool`
//! agrees, row by row, with the row-at-a-time `EvalBool`. This engine has one
//! filter evaluator, so there is no second implementation to differ from and
//! the loop would assert nothing. What the Go loop actually covers is
//! `Datum.ToBool` over every eval type plus three-valued CNF handling of
//! NULL -- so that is what is pinned here, per eval type, with the answer
//! taken from real TiDB (`rust/difftests/gorun`) rather than from this
//! engine.
//!
//! Every expected value below is TiDB's own output for the same statements.
//! Four disagreements were live bugs when this file was written, all with the
//! same root cause -- four divergent copies of the truth test, none of them
//! `Datum.ToBool`:
//!
//! | statement | TiDB | this engine (before) |
//! | --- | --- | --- |
//! | `WHERE varchar_col` | the numeric-prefix rows | *no rows at all* |
//! | `WHERE json_col` | every row but JSON `0` | *no rows at all* |
//! | `IF(varchar_col,1,0)` | 1 for a non-zero prefix | always 0 |
//! | `WHERE varchar_col IS TRUE` | the numeric-prefix rows | every non-NULL row |
//!
//! The first two are silent row loss: a correct `WHERE` returned nothing.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

/// The ids a query returns, sorted, so an assertion does not depend on scan
/// order.
fn ids(session: &mut Session, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect();
    rows.sort();
    rows
}

/// One row of the truthiness table: the statement and TiDB's answer.
struct Case {
    sql: &'static str,
    /// TiDB's rows, sorted, cells joined with `|`.
    tidb: &'static [&'static str],
}

fn fixture() -> Session {
    let mut session = Session::new();
    for stmt in [
        // ETString, with every numeric-prefix shape MySQL's StrToFloat
        // accepts and the ones it rejects.
        "CREATE TABLE s1(id INT, v VARCHAR(20))",
        "INSERT INTO s1 VALUES(1,'1'),(2,'abc'),(3,'0'),(4,'0.5'),(5,''),(6,'1abc'),(7,'-1'),\
         (8,'  2  '),(9,'0.0'),(10,'1e2'),(11,'.5'),(12,'e1'),(13,'+0'),(14,NULL)",
        // ETReal.
        "CREATE TABLE r1(id INT, v DOUBLE)",
        "INSERT INTO r1 VALUES(1,0),(2,0.5),(3,-1),(4,1e300),(5,NULL)",
        // ETDecimal.
        "CREATE TABLE d1(id INT, v DECIMAL(10,3))",
        "INSERT INTO d1 VALUES(1,0),(2,0.001),(3,-1),(4,NULL)",
        // ETDatetime and ETTimestamp: the zero value is the false one.
        "CREATE TABLE t1(id INT, v DATETIME)",
        "INSERT INTO t1 VALUES(1,'2020-01-01 00:00:00'),(2,NULL)",
        "CREATE TABLE ts1(id INT, v TIMESTAMP NULL)",
        "INSERT INTO ts1 VALUES(1,'2020-01-01 00:00:01'),(2,NULL)",
        // ETDuration.
        "CREATE TABLE du1(id INT, v TIME)",
        "INSERT INTO du1 VALUES(1,'00:00:00'),(2,'01:00:00'),(3,NULL)",
        // ETJson: truth is a comparison against JSON 0, so JSON `false`,
        // `null`, `[]` and `{}` are all TRUE.
        "CREATE TABLE j1(id INT, v JSON)",
        "INSERT INTO j1 VALUES(1,'0'),(2,'1'),(3,'\"a\"'),(4,'[]'),(5,'{}'),(6,'null'),\
         (7,'false'),(8,'true'),(9,NULL)",
        "CREATE TABLE j2(id INT, v JSON)",
        "INSERT INTO j2 VALUES(1,'0'),(2,'0.0'),(3,'-1'),(4,'\"0\"'),(5,'[0]'),(6,'{\"a\":0}')",
        "CREATE TABLE e1(id INT, v ENUM('a','b'))",
        "INSERT INTO e1 VALUES(1,'a'),(2,'b'),(3,NULL)",
        "CREATE TABLE st1(id INT, v SET('a','b'))",
        "INSERT INTO st1 VALUES(1,'a'),(2,'a,b'),(3,''),(4,NULL)",
        // A binary string takes the same numeric prefix as a character one.
        "CREATE TABLE bl(id INT, v BLOB)",
        "INSERT INTO bl VALUES(1,'1'),(2,'abc'),(3,'0'),(4,NULL)",
        "CREATE TABLE f1(id INT, v FLOAT)",
        "INSERT INTO f1 VALUES(1,0),(2,0.5),(3,-0.5)",
        // The CNF/NULL half of the Go test: several conjuncts per row, with
        // NULL on either side.
        "CREATE TABLE c2(id INT, a VARCHAR(10), b VARCHAR(10))",
        "INSERT INTO c2 VALUES(1,'1','1'),(2,'1','abc'),(3,'abc','1'),(4,'1',NULL),\
         (5,NULL,'1'),(6,NULL,NULL),(7,'0','1')",
        "CREATE TABLE t3(id INT, a VARCHAR(10), b INT)",
        "INSERT INTO t3 VALUES(1,'1',1),(2,'abc',1),(3,'1',0)",
    ] {
        session
            .run(stmt)
            .unwrap_or_else(|error| panic!("fixture `{stmt}` failed: {error:?}"));
    }
    session
}

/// The table, with TiDB's answer for every row (captured with
/// `rust/difftests/gorun`).
const CASES: &[Case] = &[
    // ETString: MySQL's numeric prefix decides, so '1abc', '  2  ', '1e2'
    // and '.5' are TRUE while 'abc', '', '0.0', 'e1' and '+0' are FALSE.
    Case {
        sql: "SELECT id FROM s1 WHERE v",
        tidb: &["1", "10", "11", "4", "6", "7", "8"],
    },
    Case {
        sql: "SELECT id FROM s1 WHERE NOT v",
        tidb: &["12", "13", "2", "3", "5", "9"],
    },
    // `IS TRUE`/`IS FALSE` are never NULL, so the NULL row (14) is in
    // neither -- but it IS in `IS NOT TRUE`.
    Case {
        sql: "SELECT id FROM s1 WHERE v IS TRUE",
        tidb: &["1", "10", "11", "4", "6", "7", "8"],
    },
    Case {
        sql: "SELECT id, v IS TRUE FROM s1",
        tidb: &[
            "10|1", "11|1", "12|0", "13|0", "14|0", "1|1", "2|0", "3|0", "4|1", "5|0", "6|1",
            "7|1", "8|1", "9|0",
        ],
    },
    // The lazy control functions read the condition the same way.
    Case {
        sql: "SELECT id, if(v,1,0) FROM s1",
        tidb: &[
            "10|1", "11|1", "12|0", "13|0", "14|0", "1|1", "2|0", "3|0", "4|1", "5|0", "6|1",
            "7|1", "8|1", "9|0",
        ],
    },
    Case {
        sql: "SELECT id, case when a then 'y' else 'n' end FROM t3",
        tidb: &["1|y", "2|n", "3|y"],
    },
    Case {
        sql: "SELECT id FROM t3 WHERE if(a,1,0) AND b",
        tidb: &["1"],
    },
    // ETReal / float32 / ETDecimal.
    Case {
        sql: "SELECT id FROM r1 WHERE v",
        tidb: &["2", "3", "4"],
    },
    Case {
        sql: "SELECT id FROM f1 WHERE v",
        tidb: &["2", "3"],
    },
    Case {
        sql: "SELECT id FROM d1 WHERE v",
        tidb: &["2", "3"],
    },
    // ETDatetime / ETTimestamp / ETDuration.
    Case {
        sql: "SELECT id FROM t1 WHERE v",
        tidb: &["1"],
    },
    Case {
        sql: "SELECT id FROM ts1 WHERE v",
        tidb: &["1"],
    },
    Case {
        sql: "SELECT id FROM du1 WHERE v",
        tidb: &["2"],
    },
    // ETJson: only a value that compares equal to JSON 0 is false, so the
    // JSON strings, arrays, objects, `null`, `false` and `true` are all true.
    Case {
        sql: "SELECT id FROM j1 WHERE v",
        tidb: &["2", "3", "4", "5", "6", "7", "8"],
    },
    Case {
        sql: "SELECT id FROM j2 WHERE v",
        tidb: &["3", "4", "5", "6"],
    },
    // ENUM/SET convert through their numeric value, so SET('') is false.
    Case {
        sql: "SELECT id FROM e1 WHERE v",
        tidb: &["1", "2"],
    },
    Case {
        sql: "SELECT id FROM st1 WHERE v",
        tidb: &["1", "2"],
    },
    // A BLOB takes the same numeric prefix as a VARCHAR.
    Case {
        sql: "SELECT id FROM bl WHERE v",
        tidb: &["1"],
    },
    // The CNF half: three-valued logic across conjuncts, with the string
    // coercion applied to each.
    Case {
        sql: "SELECT id FROM c2 WHERE a AND b",
        tidb: &["1"],
    },
    Case {
        sql: "SELECT id FROM c2 WHERE a OR b",
        tidb: &["1", "2", "3", "4", "5", "7"],
    },
    Case {
        sql: "SELECT id FROM c2 WHERE a XOR b",
        tidb: &["2", "3", "7"],
    },
    Case {
        sql: "SELECT id FROM c2 WHERE a IS FALSE",
        tidb: &["3", "7"],
    },
    Case {
        sql: "SELECT id FROM c2 WHERE a IS NOT TRUE",
        tidb: &["3", "5", "6", "7"],
    },
];

#[test]
fn eval_bool_matches_tidb_per_eval_type() {
    let mut session = fixture();
    for case in CASES {
        assert_eq!(ids(&mut session, case.sql), case.tidb, "{}", case.sql);
    }
}

/// The one row of the table this engine cannot answer: a BIT column in a
/// boolean context. TiDB converts the binary literal to an integer, so the
/// non-zero row passes; here the chunk row getter for `Bit` is still deferred
/// and reading the cell panics before any truth test runs. Go's answer is
/// asserted so this stays a tracked work item rather than a silent gap.
#[test]
#[ignore = "chunk Row::get_datum has no Bit column getter yet (panics before the truth test)"]
fn eval_bool_on_bit_column() {
    let mut session = Session::new();
    session.run("CREATE TABLE b1(id INT, v BIT(8))").unwrap();
    session
        .run("INSERT INTO b1 VALUES(1,0),(2,1),(3,NULL)")
        .unwrap();
    assert_eq!(ids(&mut session, "SELECT id FROM b1 WHERE v"), ["2"]);
}
