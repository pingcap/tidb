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


//! INSERT, UPDATE and DELETE through Go's statement paths: set-operation
//! sources, batch point deletes, IGNORE's warnings, duplicate keys in a
//! batch, the readers a ranged write plans, positional ORDER BY, LIMIT 0
//! validation, affected rows and write diagnostics, over the cop-backed
//! TestKit stack (X11).

use super::sql_kit::*;
use super::*;

#[test]
fn insert_set_operation_source_uses_the_common_query_plan() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 690);
    exec_all(&mut tk, &[
        "CREATE TABLE insert_union_src (v BIGINT)",
        "CREATE TABLE insert_union_dst (v BIGINT)",
        "INSERT INTO insert_union_src VALUES (2), (1)",
    ]);
    assert_eq!(affected(&mut tk, "INSERT INTO insert_union_dst SELECT v FROM insert_union_src UNION ALL SELECT 3"), 3);
    assert_eq!(q(&mut tk, "SELECT v FROM insert_union_dst ORDER BY v"), [["1"], ["2"], ["3"]]);
}

/// Go plans `DELETE ... WHERE pk IN (...)` over a Batch_Point_Get.
#[test]
fn batch_point_delete_reads_records_with_one_batch_get() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 691);
    exec_all(&mut tk, &[
        "CREATE TABLE batch_delete (id BIGINT PRIMARY KEY, value INT)",
        "INSERT INTO batch_delete VALUES (1, 10), (2, 20), (3, 30)",
    ]);
    let plan = plan_lines(&mut tk, "DELETE FROM batch_delete WHERE id IN (1, 3, 9)");
    assert!(plan[0].starts_with("Delete") && plan[1].contains("Batch_Point_Get"), "{plan:?}");
    assert_eq!(affected(&mut tk, "DELETE FROM batch_delete WHERE id IN (1, 3, 9)"), 2);
    assert_eq!(q(&mut tk, "SELECT id FROM batch_delete"), [["2"]]);
}

/// INSERT IGNORE / UPDATE IGNORE turn a unique conflict into one 1062
/// warning naming the original conflict.
#[test]
fn ignore_uses_the_original_unique_conflict_without_reading_it_again() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 692);
    exec_all(&mut tk, &[
        "CREATE TABLE conflict_identity (id INT PRIMARY KEY, v INT, UNIQUE KEY unique_v(v))",
        "INSERT INTO conflict_identity VALUES (1, 10), (2, 20)",
    ]);
    let duplicate = ["Warning".to_owned(), "1062".to_owned(), "Duplicate entry '20' for key 'conflict_identity.unique_v'".to_owned()];
    assert_eq!(affected(&mut tk, "INSERT IGNORE INTO conflict_identity VALUES (3, 20)"), 0);
    assert_eq!(q(&mut tk, "SHOW WARNINGS"), [duplicate.clone()]);
    rows(&mut tk, "UPDATE IGNORE conflict_identity SET v = 20 WHERE id = 1");
    assert_eq!(q(&mut tk, "SHOW WARNINGS"), [duplicate]);
    assert_eq!(q(&mut tk, "SELECT id, v FROM conflict_identity ORDER BY id"), [["1", "10"], ["2", "20"]]);
}

/// An existing clustered key, and a key repeated inside one VALUES list, are
/// both 1062.
#[test]
fn clustered_batch_insert_keeps_primary_duplicate_errors() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 693);
    exec_all(&mut tk, &[
        "CREATE TABLE batch_insert (id BIGINT PRIMARY KEY, value INT)",
        "INSERT INTO batch_insert VALUES (1, 10), (2, 20), (3, 30)",
    ]);
    assert_eq!(error(&mut tk, "INSERT INTO batch_insert VALUES (2, 200)").code, 1062);
    assert_eq!(error(&mut tk, "INSERT INTO batch_insert VALUES (5, 50), (5, 500)").code, 1062);
    assert!(q(&mut tk, "SELECT id FROM batch_insert WHERE id = 5").is_empty());
}

#[test]
fn a_consumed_handle_range_update_keeps_its_table_reader() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 694);
    rows(&mut tk, "CREATE TABLE target (id INT PRIMARY KEY, value INT)");
    let plan = explain_brief(&mut tk, "UPDATE target SET value = 1 WHERE id BETWEEN 10 AND 20");
    assert_eq!(plan.len(), 3, "{plan:#?}");
    assert_eq!(plan[0][0], "Update");
    assert_eq!((plan[1][0].as_str(), plan[1][2].as_str(), plan[1][4].as_str()), ("└─TableReader", "root", "data:TableRangeScan"));
    assert_eq!((plan[2][0].as_str(), plan[2][2].as_str(), plan[2][3].as_str()), ("  └─TableRangeScan", "cop[tikv]", "table:target"));
    assert!(plan[2][4].contains("range:[10,20]"), "{plan:#?}");
}

#[test]
fn a_delete_on_a_secondary_index_reads_through_it() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 695);
    exec_all(&mut tk, &[
        "CREATE TABLE t1 (c1 INT PRIMARY KEY, c2 INT, c3 INT, INDEX c2 (c2))",
        "INSERT INTO t1 VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)",
    ]);
    let plan = plan_lines(&mut tk, "DELETE FROM t1 WHERE c2 = 10");
    assert!(plan.iter().any(|line| line.contains("IndexRangeScan") && line.contains("index:c2(c2)")), "{plan:?}");
    assert!(!plan.iter().any(|line| line.contains("TableFullScan")), "{plan:?}");
    assert_eq!(affected(&mut tk, "DELETE FROM t1 WHERE c2 = 10"), 2);
    assert_eq!(q(&mut tk, "SELECT c1 FROM t1 ORDER BY c1"), [["2"]]);
}

#[test]
fn dml_positional_order_by_resolves_to_column() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 696);
    exec_all(&mut tk, &[
        "CREATE TABLE t (a BIGINT, b BIGINT)",
        "INSERT INTO t VALUES (1, 30), (2, 20), (3, 10)",
        "CREATE TABLE t2 (a BIGINT, b BIGINT)",
        "INSERT INTO t2 VALUES (1, 30), (2, 20), (3, 10)",
    ]);
    assert_eq!(affected(&mut tk, "UPDATE t SET a = a + 100 ORDER BY 2 LIMIT 1"), 1);
    assert_eq!(q(&mut tk, "SELECT a, b FROM t ORDER BY b"), [["103", "10"], ["2", "20"], ["1", "30"]]);
    assert_eq!(affected(&mut tk, "DELETE FROM t2 ORDER BY 2 LIMIT 1"), 1);
    assert_eq!(q(&mut tk, "SELECT a, b FROM t2 ORDER BY b"), [["2", "20"], ["1", "30"]]);
}

/// LIMIT 0 still resolves ORDER BY columns and partition selections.
#[test]
fn limit_zero_dml_still_validates_the_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 697);
    exec_all(&mut tk, &["CREATE TABLE lz (a BIGINT, b BIGINT)", "INSERT INTO lz VALUES (1, 10), (2, 20)"]);
    assert_eq!(affected(&mut tk, "UPDATE lz SET b = 1 LIMIT 0"), 0);
    assert_eq!(affected(&mut tk, "DELETE FROM lz LIMIT 0"), 0);
    for sql in [
        "UPDATE lz SET b = 1 ORDER BY nope LIMIT 0",
        "DELETE FROM lz ORDER BY nope LIMIT 0",
        "UPDATE lz PARTITION (p0) SET b = 1 LIMIT 0",
        "DELETE FROM lz PARTITION (p0) LIMIT 0",
    ] {
        error(&mut tk, sql);
    }
    assert_eq!(q(&mut tk, "SELECT a, b FROM lz"), [["1", "10"], ["2", "20"]]);
}

#[test]
fn update_and_delete_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 698);
    exec_all(&mut tk, &["CREATE TABLE w (a BIGINT, b BIGINT)", "INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)"]);
    assert_eq!(affected(&mut tk, "UPDATE w SET b = b + 1 WHERE a >= 2"), 2);
    assert_eq!(q(&mut tk, "SELECT a, b FROM w"), [["1", "10"], ["2", "21"], ["3", "31"]]);
    assert_eq!(affected(&mut tk, "UPDATE w SET b = b WHERE a = 1"), 0);
    assert_eq!(affected(&mut tk, "UPDATE w SET a = 7, b = a WHERE a = 1"), 1);
    assert_eq!(q(&mut tk, "SELECT a, b FROM w WHERE a = 7"), [["7", "1"]]);
    assert_eq!(affected(&mut tk, "UPDATE w SET b = 0"), 3);
    assert_eq!(affected(&mut tk, "DELETE FROM w WHERE a >= 3"), 2);
    assert_eq!(q(&mut tk, "SELECT a FROM w"), [["2"]]);
    assert_eq!(affected(&mut tk, "DELETE FROM w"), 1);
    assert!(q(&mut tk, "SELECT a FROM w").is_empty());
    rows(&mut tk, "INSERT INTO w VALUES (9, 9)");
    assert_eq!(q(&mut tk, "SELECT a FROM w"), [["9"]]);
    assert_eq!(error(&mut tk, "UPDATE w SET zzz = 1").code, 1054);
    assert_eq!(affected(&mut tk, "UPDATE IGNORE w SET a = 1"), 1);
    assert_eq!(q(&mut tk, "SELECT a, b FROM w"), [["1", "9"]]);
}

/// Go reports a write's out-of-range value at the source row it came from.
#[test]
fn write_diagnostics_batch_single_update_reports_source_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 699);
    exec_all(&mut tk, &["CREATE TABLE wr (id INT PRIMARY KEY, v TINYINT)", "INSERT INTO wr VALUES (1, 1), (2, 2)"]);
    let refused = error(&mut tk, "UPDATE wr SET v = id * 100 ORDER BY id");
    assert_eq!((refused.code, refused.message.as_str()), (1264, "Out of range value for column 'v' at row 2"));
}

#[test]
fn write_diagnostics_batch_join_update_reports_source_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 700);
    exec_all(&mut tk, &[
        "CREATE TABLE wr (id INT PRIMARY KEY, v TINYINT)",
        "CREATE TABLE ws (id INT PRIMARY KEY)",
        "INSERT INTO wr VALUES (1, 1), (2, 2)",
        "INSERT INTO ws VALUES (1), (2)",
    ]);
    let refused = error(&mut tk, "UPDATE wr JOIN ws ON wr.id = ws.id SET wr.v = ws.id * 100 ORDER BY wr.id");
    assert_eq!((refused.code, refused.message.as_str()), (1264, "Out of range value for column 'v' at row 2"));
}
