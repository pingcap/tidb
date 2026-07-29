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

//! Statement-level rollback: a DML statement that fails partway leaves the
//! table exactly as it was, while the transaction around it continues.
//!
//! Every expectation here was captured from real TiDB through mock-store
//! `testkit` (`pkg/executor/test/executor/zz_dump_stmt_rollback_test.go`,
//! not staged). The Go mechanism is `pkg/session/session.go`'s
//! `StmtCommit`/`StmtRollback` over the membuffer staging handle
//! (`pkg/kv/union_store.go`: `Staging()` / `Release()` / `Cleanup()`), chosen
//! between by `pkg/executor/adapter.go` on the statement's success or failure.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// A five-row UPDATE that collides on the THIRD row it processes leaves the
/// table completely unchanged, not two rows moved.
///
/// Capture: over `(1,10),(2,20),(3,30),(4,40),(5,50)`,
/// `UPDATE five3 SET a = IF(a<3, a+100, a+1) ORDER BY a ASC` is
/// `[kv:1062]Duplicate entry '4' for key 'five3.PRIMARY'` and the table is
/// still `1|10;2|20;3|30;4|40;5|50` — the two rows already moved to 101 and
/// 102 are undone with it.
#[test]
fn a_five_row_update_colliding_on_the_third_row_leaves_the_table_unchanged() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE five3 (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO five3 VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .unwrap();
    let error = session
        .run("UPDATE five3 SET a = IF(a<3, a+100, a+1) ORDER BY a ASC")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1062);
    assert_eq!(error.message, "Duplicate entry '4' for key 'five3.PRIMARY'");
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM five3 ORDER BY a"),
        [
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
            ["4", "40"],
            ["5", "50"]
        ]
    );
}

/// Inside an explicit transaction a failed statement discards only its OWN
/// writes: the statement before it survives, a statement after it still runs,
/// and COMMIT publishes both.
///
/// Capture: after `BEGIN; INSERT (100,1000);` the same colliding UPDATE errors
/// and the transaction still reads `...;100|1000`; a following
/// `INSERT (101,1010)` succeeds; `COMMIT` leaves
/// `1|10;2|20;3|30;4|40;5|50;100|1000;101|1010`.
#[test]
fn a_failed_statement_inside_a_transaction_keeps_the_transactions_other_writes() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tx (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO tx VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .unwrap();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO tx VALUES (100,1000)").unwrap();
    let error = session
        .run("UPDATE tx SET a = IF(a<3, a+200, a+1) ORDER BY a ASC")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1062);
    // The failed statement's writes are gone; the earlier one's are not.
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM tx ORDER BY a"),
        [
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
            ["4", "40"],
            ["5", "50"],
            ["100", "1000"]
        ]
    );
    // The transaction is still usable after the failure.
    session.run("INSERT INTO tx VALUES (101,1010)").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM tx ORDER BY a"),
        [
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
            ["4", "40"],
            ["5", "50"],
            ["100", "1000"],
            ["101", "1010"]
        ]
    );
}

/// A multi-row INSERT whose THIRD value list duplicates a stored key writes
/// nothing at all; the same statement with IGNORE writes every other row.
///
/// Capture: into a table holding only `3|30`,
/// `INSERT INTO ins VALUES (1,10),(2,20),(3,99),(4,40)` is
/// `[kv:1062]Duplicate entry '3' for key 'ins.PRIMARY'` and the table is still
/// `3|30`; `INSERT IGNORE` of the same list gives `1|10;2|20;3|30;4|40`.
#[test]
fn a_multi_row_insert_with_a_duplicate_in_the_middle_writes_nothing() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ins (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO ins VALUES (3,30)").unwrap();
    let error = session
        .run("INSERT INTO ins VALUES (1,10),(2,20),(3,99),(4,40)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1062);
    assert_eq!(error.message, "Duplicate entry '3' for key 'ins.PRIMARY'");
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM ins ORDER BY a"),
        [["3", "30"]]
    );

    session
        .run("INSERT IGNORE INTO ins VALUES (1,10),(2,20),(3,99),(4,40)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM ins ORDER BY a"),
        [["1", "10"], ["2", "20"], ["3", "30"], ["4", "40"]]
    );
}

/// A REPLACE whose second value list violates NOT NULL writes none of the
/// three rows.
///
/// Capture: into a table holding `9|90`,
/// `REPLACE INTO rep VALUES (1,10),(2,NULL),(3,30)` is
/// `[table:1048]Column 'b' cannot be null` and the table is still `9|90`.
#[test]
fn a_replace_failing_on_its_second_row_writes_none_of_them() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE rep (a INT PRIMARY KEY, b INT NOT NULL)")
        .unwrap();
    session.run("INSERT INTO rep VALUES (9,90)").unwrap();
    let error = session
        .run("REPLACE INTO rep VALUES (1,10),(2,NULL),(3,30)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1048);
    assert_eq!(error.message, "Column 'b' cannot be null");
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM rep ORDER BY a"),
        [["9", "90"]]
    );
}

/// Statement rollback undoes the ROWS a failed statement wrote and NOT the
/// AUTO_INCREMENT ids it burned: Go allocates ids outside transaction
/// semantics.
///
/// Capture: `ai1` holds `1|1` after one insert; the failing
/// `INSERT INTO ai1 (v) VALUES (1)` (duplicate on the unique `v`) leaves the
/// table at `1|1`, and the NEXT successful insert lands at id `3` — the failed
/// statement burned `2` and the rollback did not give it back.
///
/// One failing row, not three, because HOW MANY ids a multi-row insert burns
/// is a different question from whether a burn is returned: Go pre-allocates
/// the whole batch (a failing `VALUES (2),(1),(3)` burns 2, 3 AND 4, so its
/// successor is `5`) while this tier allocates row by row. That divergence
/// belongs to the allocator, and pinning it here would make this test fail for
/// a reason that has nothing to do with statement rollback.
#[test]
fn statement_rollback_undoes_the_rows_but_never_the_auto_increment_burn() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ai1 (id INT AUTO_INCREMENT PRIMARY KEY, v INT UNIQUE)")
        .unwrap();
    session.run("INSERT INTO ai1 (v) VALUES (1)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id,v FROM ai1 ORDER BY id"),
        [["1", "1"]]
    );

    let error = session
        .run("INSERT INTO ai1 (v) VALUES (1)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1062);
    assert_eq!(
        rows(&mut session, "SELECT id,v FROM ai1 ORDER BY id"),
        [["1", "1"]]
    );

    session.run("INSERT INTO ai1 (v) VALUES (9)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id,v FROM ai1 ORDER BY id"),
        [["1", "1"], ["3", "9"]]
    );
}
