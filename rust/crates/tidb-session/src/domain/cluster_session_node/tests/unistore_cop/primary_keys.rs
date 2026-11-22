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


//! What a primary key does to the row's identity -- the integer key that
//! becomes the handle, the clustered common handle, the non-clustered key an
//! index enforces, the `_tidb_rowid` heap -- and how the planner reads each,
//! over the cop-backed TestKit stack (X11; Go `pkg/table/tables`
//! `PKIsHandle` / `IsCommonHandle`, `pkg/ddl` `ShouldBuildClusteredIndex`).

use super::sql_kit::*;
use super::*;

/// A single-column integer PRIMARY KEY is the row handle: rows scan in key
/// order, a repeat is 1062 and leaves the row alone, negative keys work.
#[test]
fn integer_primary_key_is_the_row_handle() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 600);
    for (name, ddl) in [
        ("p1", "CREATE TABLE p1 (id BIGINT PRIMARY KEY, v BIGINT)"),
        ("p2", "CREATE TABLE p2 (id BIGINT, v BIGINT, PRIMARY KEY (id))"),
    ] {
        rows(&mut tk, ddl);
        rows(&mut tk, &format!("INSERT INTO {name} VALUES (10, 100), (20, 200)"));
        rows(&mut tk, &format!("INSERT INTO {name} VALUES (5, 50)"));
        assert_eq!(q(&mut tk, &format!("SELECT id FROM {name}")), [["5"], ["10"], ["20"]], "{ddl}");
        assert_eq!(error(&mut tk, &format!("INSERT INTO {name} VALUES (10, 999)")).code, 1062, "{ddl}");
        assert_eq!(q(&mut tk, &format!("SELECT v FROM {name} WHERE id = 10")), [["100"]], "{ddl}");
        rows(&mut tk, &format!("INSERT INTO {name} VALUES (-1, 1)"));
        assert_eq!(q(&mut tk, &format!("SELECT id FROM {name}")).len(), 4, "{ddl}");
    }
}

/// Without a primary key the handle is the allocated `_tidb_rowid`.
#[test]
fn without_a_primary_key_rows_repeat_freely() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 601);
    exec_all(&mut tk, &["CREATE TABLE h (a BIGINT)", "INSERT INTO h VALUES (1), (1), (1)"]);
    assert_eq!(q(&mut tk, "SELECT a FROM h").len(), 3);
}

/// Go `checkDuplicateKey` / `ErrMultiplePriKey`: two primary keys is 1068.
#[test]
fn unsupported_constraints_are_rejected() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 602);
    assert_eq!(error(&mut tk, "CREATE TABLE c (a BIGINT PRIMARY KEY, b BIGINT PRIMARY KEY)").code, 1068);
}

/// A string primary key is enforced on the way in whichever handle it gets.
#[test]
fn a_non_integer_primary_key_is_enforced_by_its_index() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 603);
    exec_all(&mut tk, &["CREATE TABLE s (k VARCHAR(10) PRIMARY KEY)", "INSERT INTO s VALUES ('a'), ('b')"]);
    assert_eq!(q(&mut tk, "SELECT k FROM s").len(), 2);
    assert_eq!(error(&mut tk, "INSERT INTO s VALUES ('a')").code, 1062);
}

/// A clustered common handle: key order, key columns read back from the key,
/// duplicates refused, writes addressed through the key, a freed key reusable,
/// composite keys unique only as a whole, secondary indexes resolving to it.
#[test]
fn clustered_common_handle() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 604);
    exec_all(&mut tk, &[
        "CREATE TABLE c (k VARCHAR(8) PRIMARY KEY, v BIGINT)",
        "INSERT INTO c VALUES ('b', 2), ('a', 1), ('c', 3)",
    ]);
    assert_eq!(q(&mut tk, "SELECT k FROM c"), [["a"], ["b"], ["c"]]);
    assert_eq!(q(&mut tk, "SELECT v FROM c WHERE k = 'b'"), [["2"]]);
    assert_eq!(error(&mut tk, "INSERT INTO c VALUES ('a', 9)").code, 1062);
    rows(&mut tk, "UPDATE c SET v = 20 WHERE k = 'b'");
    assert_eq!(q(&mut tk, "SELECT v FROM c WHERE k = 'b'"), [["20"]]);
    rows(&mut tk, "DELETE FROM c WHERE k = 'a'");
    assert_eq!(q(&mut tk, "SELECT k FROM c").len(), 2);
    rows(&mut tk, "INSERT INTO c VALUES ('a', 1)");

    exec_all(&mut tk, &[
        "CREATE TABLE m (a BIGINT, b VARCHAR(4), v BIGINT, PRIMARY KEY (a, b))",
        "INSERT INTO m VALUES (1, 'y', 10), (1, 'x', 20), (2, 'a', 30)",
    ]);
    assert_eq!(q(&mut tk, "SELECT a, b FROM m"), [["1", "x"], ["1", "y"], ["2", "a"]]);
    assert_eq!(error(&mut tk, "INSERT INTO m VALUES (1, 'x', 99)").code, 1062);
    rows(&mut tk, "INSERT INTO m VALUES (1, 'z', 40)");

    exec_all(&mut tk, &[
        "CREATE TABLE s (k VARCHAR(4) PRIMARY KEY, tag BIGINT, KEY tag_idx (tag))",
        "INSERT INTO s VALUES ('p', 1), ('q', 2)",
    ]);
    assert_eq!(q(&mut tk, "SELECT k FROM s WHERE tag >= 2"), [["q"]]);
}

/// Go master `selectivity.go` floors a pseudo source at one row and
/// `adjustCountAfterAccess` applies 1 / SelectionFactor: the covering
/// composite index wins at 1.25 rows.
#[test]
fn pseudo_composite_index_applies_master_selectivity_floor() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 605);
    rows(&mut tk, "CREATE TABLE t (id INT PRIMARY KEY, bucket INT, rare INT, INDEX idx_bucket(bucket), INDEX idx_rare(rare), INDEX idx_cover(bucket, rare))");
    let plan = explain_brief(&mut tk, "SELECT * FROM t WHERE bucket = 1 AND rare = 7");
    let scan = plan.iter().find(|row| row[0].contains("IndexRangeScan")).expect("an index range");
    assert!(scan[3].contains("idx_cover"), "{plan:?}");
    assert_eq!(scan[1], "1.25", "{plan:?}");
}

/// With analyzed statistics the composite index still dominates the single
/// equality index.
#[test]
fn analyzed_composite_index_dominates_single_equality_index() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 606);
    rows(&mut tk, "CREATE TABLE t (id INT PRIMARY KEY, bucket INT, rare INT, payload VARCHAR(20), INDEX idx_bucket(bucket), INDEX idx_rare(rare), INDEX idx_cover(bucket, rare))");
    let values = (1..=2000).map(|id| format!("({id}, {}, {id}, 'payload')", id % 4)).collect::<Vec<_>>().join(",");
    rows(&mut tk, &format!("INSERT INTO t VALUES {values}"));
    rows(&mut tk, "ANALYZE TABLE t");
    let plan = explain_brief(&mut tk, "SELECT * FROM t WHERE bucket = 1 AND rare = 7");
    let scan = plan.iter().find(|row| row[0].contains("IndexRangeScan")).expect("an index range");
    assert!(scan[3].contains("idx_cover"), "{plan:?}");
}

/// Go's `FieldName.String` prints the lowercase CIStr identity.
#[test]
fn explain_uses_the_lowercase_field_name_identity() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 607);
    rows(&mut tk, "CREATE TABLE lc (UPPER_COL BIGINT NOT NULL)");
    let plan = plan_lines(&mut tk, "SELECT SUM(UPPER_COL) FROM lc");
    assert!(plan.iter().any(|line| line.contains("sum(test.lc.upper_col)")), "{plan:#?}");
    assert!(plan.iter().all(|line| !line.contains("UPPER_COL")), "{plan:#?}");
}

/// A common-handle table path lowers to `PhysicalTableScan`, a TableFullScan.
#[test]
fn a_common_handle_table_path_is_a_table_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 608);
    rows(&mut tk, "CREATE TABLE ch (id VARCHAR(50) NOT NULL, part BIGINT NOT NULL, created DATE NOT NULL, PRIMARY KEY (id, part, created)) PARTITION BY RANGE COLUMNS(part, created) (PARTITION p0 VALUES LESS THAN (200000, '2023-01-01'), PARTITION p1 VALUES LESS THAN (300000, '2023-01-01'))");
    let plan = plan_lines(&mut tk, "SELECT * FROM ch WHERE part > 199999");
    assert!(plan.iter().any(|line| line.contains("TableFullScan") && line.contains("table:ch")), "{plan:?}");
    assert!(plan.iter().all(|line| !line.contains("IndexFullScan")), "{plan:?}");
}

/// A covering non-clustered PRIMARY index wins the full-range read.
#[test]
fn a_covering_nonclustered_primary_key_beats_the_table_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 609);
    exec_all(&mut tk, &[
        "SET tidb_enable_clustered_index = INT_ONLY",
        "CREATE TABLE np (id VARCHAR(50) NOT NULL, part BIGINT NOT NULL, created DATE NOT NULL, PRIMARY KEY (id, part, created)) PARTITION BY RANGE COLUMNS(part, created) (PARTITION p0 VALUES LESS THAN (200000, '2023-01-01'), PARTITION p1 VALUES LESS THAN (300000, '2023-01-01'))",
        "INSERT INTO np VALUES ('1', 200000, '2022-12-29'), ('2', 200000, '2023-01-01')",
        "ANALYZE TABLE np",
    ]);
    assert_eq!(q(&mut tk, "SELECT TIDB_PK_TYPE FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 'np'"), [["NONCLUSTERED"]]);
    let plan = plan_lines(&mut tk, "SELECT * FROM np WHERE part > 199999");
    assert!(plan.iter().any(|line| line.contains("IndexFullScan") && line.contains("index:PRIMARY(id, part, created)")), "{plan:?}");
}

/// Go `DecodeHandleToDatumMap` unflattens handle columns and
/// `decodeHandleToDatum` reads an UNSIGNED int handle as unsigned.
#[test]
fn a_handle_column_reads_back_in_its_own_type() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 610);
    exec_all(&mut tk, &["CREATE TABLE tb (a BIT(1) NOT NULL, PRIMARY KEY (a))", "INSERT INTO tb VALUES (1), (0)"]);
    let bits = rows(&mut tk, "SELECT a FROM tb");
    assert!(bits.iter().all(|row| matches!(row[0], Datum::Bit(_))), "{bits:?}");
    assert_eq!(rows(&mut tk, "SELECT a + 0 FROM tb"), vec![vec![Datum::UInt(0)], vec![Datum::UInt(1)]]);
    exec_all(&mut tk, &[
        "CREATE TABLE tdt (a DATETIME NOT NULL, b INT, PRIMARY KEY (a))",
        "INSERT INTO tdt VALUES ('2020-01-02 03:04:05', 9)",
    ]);
    let times = rows(&mut tk, "SELECT a, b FROM tdt");
    assert!(matches!(times[0][0], Datum::Time(_)), "{times:?}");
    assert_eq!(displayed(times), [["2020-01-02 03:04:05", "9"]]);
    exec_all(&mut tk, &[
        "CREATE TABLE tu (a BIGINT UNSIGNED NOT NULL, b INT, PRIMARY KEY (a))",
        "INSERT INTO tu VALUES (18446744073709551615, 5)",
    ]);
    assert_eq!(rows(&mut tk, "SELECT a, b FROM tu"), vec![vec![Datum::UInt(u64::MAX), Datum::Int(5)]]);
}

fn pk_type(tk: &mut Session, name: &str) -> String {
    q(tk, &format!("SELECT TIDB_PK_TYPE FROM information_schema.tables WHERE table_schema = 'test' AND table_name = '{name}'"))[0][0].clone()
}

/// Go `ShouldBuildClusteredIndex` crossed with `isSingleIntPK`, over the
/// three key shapes and the three modes of `@@tidb_enable_clustered_index`;
/// a non-clustered table keeps `_tidb_rowid`.
#[test]
fn the_clustered_index_mode_decides_the_handle() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 611);
    let shapes = ["(a BIGINT PRIMARY KEY)", "(a VARCHAR(10) PRIMARY KEY)", "(a BIGINT, b BIGINT, PRIMARY KEY (a, b))"];
    for (mode, clustered) in [("ON", [true, true, true]), ("INT_ONLY", [true, false, false]), ("OFF", [false, false, false])] {
        rows(&mut tk, &format!("SET tidb_enable_clustered_index = {mode}"));
        for (shape, clustered) in shapes.iter().zip(clustered) {
            exec_all(&mut tk, &["DROP TABLE IF EXISTS m", &format!("CREATE TABLE m {shape}")]);
            let want = if clustered { "CLUSTERED" } else { "NONCLUSTERED" };
            assert_eq!(pk_type(&mut tk, "m"), want, "{shape} under {mode}");
            let rowid = tk.node_conn().execute("SELECT _tidb_rowid FROM m");
            assert_eq!(rowid.is_ok(), !clustered, "{shape} under {mode}");
        }
    }
}

/// An explicit CLUSTERED / NONCLUSTERED overrides the variable in both
/// directions with no type-eligibility test (`create_table.go`).
#[test]
fn an_explicit_clause_overrides_the_variable() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 612);
    for (mode, ddl, want) in [
        ("ON", "(a BIGINT PRIMARY KEY NONCLUSTERED)", "NONCLUSTERED"),
        ("ON", "(a VARCHAR(10), PRIMARY KEY (a) NONCLUSTERED)", "NONCLUSTERED"),
        ("INT_ONLY", "(a BIGINT, PRIMARY KEY (a) NONCLUSTERED)", "NONCLUSTERED"),
        ("OFF", "(a BIGINT PRIMARY KEY CLUSTERED)", "CLUSTERED"),
        ("OFF", "(a VARCHAR(10), PRIMARY KEY (a) CLUSTERED)", "CLUSTERED"),
        ("INT_ONLY", "(a BIGINT, b VARCHAR(4), PRIMARY KEY (a, b) CLUSTERED)", "CLUSTERED"),
    ] {
        exec_all(&mut tk, &[&format!("SET tidb_enable_clustered_index = {mode}"), "DROP TABLE IF EXISTS m", &format!("CREATE TABLE m {ddl}")]);
        assert_eq!(pk_type(&mut tk, "m"), want, "{ddl} under {mode}");
    }
}

/// Go `ErrUnsupportedClusteredSecondaryKey`: the keyword on a secondary key
/// is refused.
#[test]
fn the_keyword_is_refused_on_a_secondary_key() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 613);
    for ddl in [
        "CREATE TABLE m (a BIGINT, UNIQUE KEY u (a) CLUSTERED)",
        "CREATE TABLE m (a BIGINT, UNIQUE KEY u (a) NONCLUSTERED)",
        "CREATE TABLE m (a BIGINT, KEY k (a) CLUSTERED)",
    ] {
        let refused = error(&mut tk, ddl);
        assert!(refused.message.contains("CLUSTERED/NONCLUSTERED keyword is only supported for primary key"), "{ddl}: {}", refused.message);
    }
}

/// Go `pkg/util/ranger` `TestIssue50051`.
#[test]
fn issue_50051_unsigned_boundaries_range_correctly() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 614);
    exec_all(&mut tk, &[
        "CREATE TABLE tt (c BIGINT UNSIGNED NOT NULL, d INT NOT NULL, PRIMARY KEY (c, d))",
        "INSERT INTO tt VALUES (9223372036854775810, 3)",
        "CREATE TABLE t5 (d INT NOT NULL, c INT NOT NULL, PRIMARY KEY (d, c))",
        "CREATE TABLE t6 (d BIGINT UNSIGNED NOT NULL)",
        "INSERT INTO t5 VALUES (-3, 6)",
        "INSERT INTO t6 VALUES (0), (1), (2), (3)",
    ]);
    assert_eq!(q(&mut tk, "SELECT c FROM tt WHERE c > 9223372036854775807 AND c > 1"), [["9223372036854775810"]]);
    assert_eq!(q(&mut tk, "SELECT d FROM t5 WHERE d < (SELECT min(d) FROM t6) AND d < 3"), [["-3"]]);
}
