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

//! `CREATE TABLE ... PARTITION BY HASH`, `BY RANGE`, and scalar `BY LIST` are
//! REAL; the remaining methods are refused, and the refusal is still a
//! tripwire.
//!
//! This node stores a `PartitionSpec` (Go `model.PartitionInfo`; see
//! `tidb_executor::partition_routing`), routes each row into one of N
//! physical key prefixes exactly as Go's `locatePartition` does, PRUNES which
//! of them a `WHERE` reads (`tidb_executor::partition_pruning`, Go
//! `pkg/planner/core/rule/rule_partition_processor.go`), answers
//! `... PARTITION (p)` from the named partitions alone, and prints the clause
//! back through `SHOW CREATE TABLE`. What it still has no answer for is
//! KEY, `RANGE COLUMNS`, and `LIST COLUMNS` routing, so those methods are
//! refused rather than answered wrongly.
//!
//! # The captures these tests are written against
//!
//! Every `GO_*` constant below is real TiDB's own `SHOW CREATE TABLE` text,
//! and every routing answer in [`hash_routing_matches_real_tidb`] and
//! [`range_routing_matches_real_tidb`] is the partition real TiDB put that
//! row in, both captured through a mock-store session. The HASH and RANGE
//! rows are asserted as EQUALITY; the rest are still asserted as refusals,
//! and become equalities when their routing lands.

#![cfg(test)]

use crate::tests_support::show_create;
use crate::*;

/// Go's `SHOW CREATE TABLE h1` for
/// `create table h1 (a int, b int) partition by hash(a) partitions 4`.
const GO_HASH: &str = "CREATE TABLE `h1` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY HASH (`a`) PARTITIONS 4";

/// Go's `SHOW CREATE TABLE h3` for `partition by hash(a+b) partitions 3`.
/// The expression keeps Go's own bracketed, space-free spelling.
const GO_HASH_EXPR: &str = "CREATE TABLE `h3` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY HASH ((`a`+`b`)) PARTITIONS 3";

/// Go's `SHOW CREATE TABLE lh` for `partition by linear hash(a) partitions
/// 4`: the LINEAR keyword is accepted, warned about, and NOT printed back.
const GO_LINEAR_HASH: &str = "CREATE TABLE `lh` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY HASH (`a`) PARTITIONS 4";

/// Go's `SHOW CREATE TABLE r1` for a three-way RANGE table with `MAXVALUE`.
const GO_RANGE: &str = "CREATE TABLE `r1` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE (`a`)\n(PARTITION `p0` VALUES LESS THAN (10),\n PARTITION `p1` VALUES LESS THAN (20),\n PARTITION `pm` VALUES LESS THAN (MAXVALUE))";

/// Go's `SHOW CREATE TABLE l1` for a two-way LIST table.
const GO_LIST: &str = "CREATE TABLE `l1` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY LIST (`a`)\n(PARTITION `p0` VALUES IN (1,2,3),\n PARTITION `p1` VALUES IN (4,5,6))";

/// Go's `SHOW CREATE TABLE rc1` for a two-column `RANGE COLUMNS` table.
/// Note that Go prints `COLUMNS(` with NO space and the bounds with no space
/// after the comma -- a different spelling from the expression form.
const GO_RANGE_COLUMNS: &str = "CREATE TABLE `rc1` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE COLUMNS(`a`,`b`)\n(PARTITION `p0` VALUES LESS THAN (10,10),\n PARTITION `p1` VALUES LESS THAN (MAXVALUE,MAXVALUE))";

/// Go's `SHOW CREATE TABLE lc1` for a two-column `LIST COLUMNS` table.
const GO_LIST_COLUMNS: &str = "CREATE TABLE `lc1` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY LIST COLUMNS(`a`,`b`)\n(PARTITION `p0` VALUES IN ((1,1),(2,2)))";

/// Go's `SHOW CREATE TABLE k1` for `partition by key(a) partitions 3`.
const GO_KEY: &str = "CREATE TABLE `k1` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY KEY (`a`) PARTITIONS 3";

/// How many rows each partition of `table` holds, in definition order. This
/// is the routing made visible; see `KvTable::partition_row_counts`.
fn partition_counts(session: &mut Session, table: &str) -> Vec<(String, usize)> {
    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(kv)) = catalog.table_mut_in("test", table)
            else {
                panic!("{table} is not stored as bytes");
            };
            Ok(kv.partition_row_counts().expect("partition row counts"))
        })
        .expect("catalog")
}

/// The name of the one partition holding rows, for a table with exactly one
/// row in it.
fn sole_holder(session: &mut Session, table: &str) -> String {
    let counts = partition_counts(session, table);
    let holding: Vec<&(String, usize)> = counts.iter().filter(|(_, rows)| *rows > 0).collect();
    assert_eq!(holding.len(), 1, "expected one holder, got {counts:?}");
    holding[0].0.clone()
}

/// A HASH-partitioned `CREATE TABLE` succeeds and `SHOW CREATE TABLE`
/// restores Go's own text verbatim -- both the bare-column form and the
/// expression form, whose bracketed spelling is Go's, not the AST restorer's.
#[test]
fn hash_partitioning_round_trips_gos_show_create_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h1 (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .expect("a HASH-partitioned CREATE TABLE is accepted");
    assert_eq!(show_create(&mut session, "h1"), GO_HASH);

    session
        .run("CREATE TABLE h3 (a int, b int) PARTITION BY HASH(a+b) PARTITIONS 3")
        .expect("an expression partition function is accepted");
    assert_eq!(show_create(&mut session, "h3"), GO_HASH_EXPR);
}

/// `PARTITION BY HASH(a)` with no `PARTITIONS` is ONE partition, which Go
/// accepts (captured: the statement returns no error) and prints as
/// `PARTITIONS 1`.
#[test]
fn an_omitted_partition_count_means_one_partition() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int) PARTITION BY HASH(a)")
        .expect("an omitted PARTITIONS count is one partition");
    assert!(show_create(&mut session, "h").ends_with("PARTITION BY HASH (`a`) PARTITIONS 1"));
}

/// `LINEAR HASH` is ACCEPTED as plain HASH with warning 8200, and the keyword
/// is not printed back. Captured verbatim from real TiDB.
#[test]
fn linear_hash_is_accepted_as_plain_hash_with_a_warning() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE lh (a int, b int) PARTITION BY LINEAR HASH(a) PARTITIONS 4")
        .expect("LINEAR HASH is accepted");
    assert_eq!(
        session.warnings(),
        [SqlWarning {
            level: WarningLevel::Warning,
            code: 8200,
            message: "LINEAR HASH is not supported, using non-linear HASH instead".to_owned(),
        }]
    );
    assert_eq!(show_create(&mut session, "lh"), GO_LINEAR_HASH);
}

/// Every boundary value this unit captured from real TiDB lands in the
/// partition TiDB put it in.
///
/// This is the assertion the whole unit exists for: a row routed to the wrong
/// partition is silent data loss, and the only defence is agreeing with Go
/// value by value. The captures are `partition by hash(a) partitions 4` over
/// `0,1,3,4,-1,-3,-4,-7,NULL` -- note that a NEGATIVE value takes the
/// magnitude, and NULL goes to the first partition -- and
/// `hash(a+b) partitions 3` over `(1,1),(1,2),(-1,-1),(NULL,1)`.
#[test]
fn hash_routing_matches_real_tidb() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h1 (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    for (value, expected) in [
        ("0", "p0"),
        ("1", "p1"),
        ("3", "p3"),
        ("4", "p0"),
        ("-1", "p1"),
        ("-3", "p3"),
        ("-4", "p0"),
        ("-7", "p3"),
        ("NULL", "p0"),
    ] {
        session
            .run(&format!("INSERT INTO h1 VALUES ({value}, 0)"))
            .unwrap();
        assert_eq!(
            sole_holder(&mut session, "h1"),
            expected,
            "hash({value}) over 4 partitions"
        );
        session.run("DELETE FROM h1").unwrap();
    }

    session
        .run("CREATE TABLE h3 (a int, b int) PARTITION BY HASH(a+b) PARTITIONS 3")
        .unwrap();
    for (a, b, expected) in [
        ("1", "1", "p2"),
        ("1", "2", "p0"),
        ("-1", "-1", "p2"),
        ("NULL", "1", "p0"),
    ] {
        session
            .run(&format!("INSERT INTO h3 VALUES ({a}, {b})"))
            .unwrap();
        assert_eq!(
            sole_holder(&mut session, "h3"),
            expected,
            "hash({a}+{b}) over 3 partitions"
        );
        session.run("DELETE FROM h3").unwrap();
    }
}

/// The rows are SPREAD, and every one of them comes back.
///
/// A scan that covered only one partition, and a router that sent everything
/// to `p0`, would each fail one half of this: the counts prove the spread,
/// the `SELECT` proves the scan reaches all of it.
#[test]
fn every_row_written_to_a_partitioned_table_is_read_back() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session
        .run("INSERT INTO h VALUES (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)")
        .unwrap();
    assert_eq!(
        partition_counts(&mut session, "h"),
        vec![
            ("p0".to_owned(), 2),
            ("p1".to_owned(), 2),
            ("p2".to_owned(), 2),
            ("p3".to_owned(), 2),
        ]
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM h")),
        vec![vec!["8".to_owned()]],
        "the scan must span every partition"
    );
    // And the consistency check agrees with itself over the whole table.
    session.run("ADMIN CHECK TABLE h").expect("admin check");
}

/// An UPDATE that changes the partitioning column MOVES the row between
/// partitions, and does not leave a copy behind.
///
/// This is the write path's own version of the routing trap: the old record
/// key and the new one differ only in the partition id, so a path that
/// removed the old row only when the HANDLE changed would duplicate it.
#[test]
fn an_update_that_changes_the_partition_key_moves_the_row() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session.run("INSERT INTO h VALUES (1, 1)").unwrap();
    assert_eq!(sole_holder(&mut session, "h"), "p1");
    session.run("UPDATE h SET a = 2").unwrap();
    assert_eq!(
        sole_holder(&mut session, "h"),
        "p2",
        "the row must have moved, exactly once"
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT a, b FROM h")),
        vec![vec!["2".to_owned(), "1".to_owned()]]
    );
    session.run("ADMIN CHECK TABLE h").expect("admin check");
}

/// A DELETE removes the row from the partition it is actually in, not from
/// the table prefix it would have had if the table were unpartitioned.
#[test]
fn a_delete_reaches_the_partition_the_row_is_in() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int primary key, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session
        .run("INSERT INTO h VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    session.run("DELETE FROM h WHERE a = 2").unwrap();
    let counts = partition_counts(&mut session, "h");
    assert_eq!(counts.iter().map(|(_, rows)| rows).sum::<usize>(), 2);
    assert_eq!(counts[2].1, 0, "p2 held `2` and must be empty, {counts:?}");
    assert_eq!(
        tests_support::row_text(session.run("SELECT a FROM h ORDER BY a")),
        vec![vec!["1".to_owned()], vec!["3".to_owned()]]
    );
    session.run("ADMIN CHECK TABLE h").expect("admin check");
}

/// A unique index over a HASH-partitioned table still enforces uniqueness,
/// and its entries survive the partitioned write path.
///
/// The index is keyed by the TABLE id rather than by the partition id, which
/// is sound only because every unique key must include the partitioning
/// columns (8264, asserted below) -- so this is the test that the two halves
/// of that argument agree.
#[test]
fn a_unique_key_over_the_partition_column_still_collides() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int, UNIQUE KEY ua (a)) PARTITION BY HASH(a) PARTITIONS 4")
        .expect("a unique key on the partitioning column is allowed");
    session.run("INSERT INTO h VALUES (1, 1)").unwrap();
    assert!(
        session.run("INSERT INTO h VALUES (1, 2)").is_err(),
        "a duplicate unique key must be rejected"
    );
    session.run("ADMIN CHECK TABLE h").expect("admin check");
}

/// The unique-key rule reaches `ADD INDEX`, not just `CREATE TABLE`.
///
/// Go runs it in `checkCreateGlobalIndex` (`pkg/ddl/executor.go`), which
/// raises the same 8264 naming the INDEX for a unique index on a partitioned
/// table that does not cover the partitioning columns. Without it, a table
/// created legally could grow an index that breaks the argument this tier's
/// table-id index keying rests on.
#[test]
fn adding_a_unique_index_off_the_partition_column_is_refused_too() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    let rendered = session
        .run("ALTER TABLE h ADD UNIQUE KEY ub (b)")
        .expect_err("a unique index off the partition column needs GLOBAL")
        .to_mysql_error();
    assert_eq!(rendered.code, 8264);
    assert_eq!(
        rendered.message,
        "Global Index is needed for index 'ub', since the unique index is not including all \
         partitioning columns, and GLOBAL is not given as IndexOption"
    );
    // The same index over the partitioning column is fine, and so is a
    // NON-unique index over any column.
    session
        .run("ALTER TABLE h ADD UNIQUE KEY ua (a)")
        .expect("a unique index on the partitioning column is allowed");
    session
        .run("ALTER TABLE h ADD KEY kb (b)")
        .expect("a non-unique index is unrestricted");
}

/// A RANGE-partitioned `CREATE TABLE` succeeds and `SHOW CREATE TABLE`
/// restores Go's own text verbatim, definition list and all.
#[test]
fn range_partitioning_round_trips_gos_show_create_table() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE r1 (a int, b int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN \
             (10), PARTITION p1 VALUES LESS THAN (20), PARTITION pm VALUES LESS THAN MAXVALUE)",
        )
        .expect("a RANGE-partitioned CREATE TABLE is accepted");
    assert_eq!(show_create(&mut session, "r1"), GO_RANGE);
}

/// Scalar LIST folds its values at CREATE, routes by exact integer value, and
/// keeps the `NULL` and `DEFAULT` fallbacks distinct. These are Go's
/// `ForListPruning.LocatePartition` branches, exercised through physical
/// partition reads rather than an implementation-local routing helper.
#[test]
fn scalar_list_partitioning_routes_and_round_trips() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE l1 (a int, b int) PARTITION BY LIST(a) (PARTITION p0 VALUES IN \
             (1,2,3), PARTITION p1 VALUES IN (4,5,6))",
        )
        .expect("scalar LIST is routed");
    assert_eq!(show_create(&mut session, "l1"), GO_LIST);

    session
        .run(
            "CREATE TABLE ld (a int) PARTITION BY LIST(a) (PARTITION p0 VALUES IN (1), \
             PARTITION pn VALUES IN (NULL), PARTITION pd DEFAULT)",
        )
        .expect("NULL and DEFAULT are real LIST partition definitions");
    for (value, expected) in [("1", "p0"), ("NULL", "pn"), ("7", "pd")] {
        session
            .run(&format!("INSERT INTO ld VALUES ({value})"))
            .unwrap();
        assert_eq!(sole_holder(&mut session, "ld"), expected, "LIST({value})");
        session.run("DELETE FROM ld").unwrap();
    }

    session
        .run("CREATE TABLE ln (a int) PARTITION BY LIST(a) (PARTITION p0 VALUES IN (1))")
        .unwrap();
    let error = session
        .run("INSERT INTO ln VALUES (2)")
        .expect_err("no LIST partition accepts 2")
        .to_mysql_error();
    assert_eq!(error.code, 1526);
    assert_eq!(error.message, "Table has no partition for value 2");
}

/// A `VALUES LESS THAN` bound is EVALUATED at `CREATE` and stored as the
/// folded integer, which is what `SHOW CREATE TABLE` prints back.
///
/// Captured: `VALUES LESS THAN (5+20)` comes back as `(25)`. A node that
/// stored the expression text would print `5+20` and, worse, would have to
/// re-evaluate it per routed row.
#[test]
fn a_range_bound_is_folded_at_create_time() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE e19 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN (5+20))",
        )
        .expect("a constant bound expression is folded");
    assert!(
        show_create(&mut session, "e19").ends_with(
            "\nPARTITION BY RANGE (`a`)\n(PARTITION `p0` VALUES LESS THAN (10),\n PARTITION `p1` \
             VALUES LESS THAN (25))"
        ),
        "got {}",
        show_create(&mut session, "e19")
    );
}

/// A DDL-time constant fold reads the SESSION's `time_zone`, not UTC.
///
/// Captured from real TiDB (`gorun`), the SAME `CREATE TABLE` issued from two
/// sessions and read back with `SHOW CREATE TABLE`:
///
/// ```text
/// set time_zone = '+00:00'  ->  (PARTITION `p0` VALUES LESS THAN (1578064200),
/// set time_zone = '+08:00'  ->  (PARTITION `p0` VALUES LESS THAN (1578035400),
/// ```
///
/// -- 28800 seconds apart, exactly the offset. This bound was REFUSED
/// outright until the session's context reached `run_create_table_in`,
/// because that entry re-parses the statement and folding under a fixed UTC
/// settles the bound to the other session's integer: a row real TiDB routes
/// to one partition lands in another, and every later pruned read answers
/// from it wrongly, with no error at all.
#[test]
fn a_range_bound_folds_under_the_sessions_time_zone() {
    let bound = |zone: &str| {
        let mut session = Session::new();
        session
            .apply_set(&format!("SET time_zone = '{zone}'"))
            .unwrap();
        session
            .run(
                "CREATE TABLE p (a int, t timestamp) PARTITION BY RANGE (UNIX_TIMESTAMP(t)) (\
                 PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2020-01-03 15:10:00')), \
                 PARTITION p1 VALUES LESS THAN (MAXVALUE))",
            )
            .expect("a time_zone-dependent bound is folded, not refused");
        show_create(&mut session, "p")
    };

    assert!(
        bound("+00:00").ends_with(
            "\nPARTITION BY RANGE (UNIX_TIMESTAMP(`t`))\n(PARTITION `p0` VALUES LESS THAN \
             (1578064200),\n PARTITION `p1` VALUES LESS THAN (MAXVALUE))"
        ),
        "got {}",
        bound("+00:00")
    );
    assert!(
        bound("+08:00").ends_with(
            "\nPARTITION BY RANGE (UNIX_TIMESTAMP(`t`))\n(PARTITION `p0` VALUES LESS THAN \
             (1578035400),\n PARTITION `p1` VALUES LESS THAN (MAXVALUE))"
        ),
        "got {}",
        bound("+08:00")
    );
}

/// The CONTROL for the fold above: a bound that reads no zone is the same
/// integer in every session (captured, both zones: `LESS THAN (15)`), so a
/// mutation that neuters the threading has to fail the zone test while
/// leaving this one passing.
#[test]
fn a_zone_independent_range_bound_is_the_same_in_every_session() {
    for zone in ["+00:00", "+08:00"] {
        let mut session = Session::new();
        session
            .apply_set(&format!("SET time_zone = '{zone}'"))
            .unwrap();
        session
            .run(
                "CREATE TABLE q (a int) PARTITION BY RANGE (a) (\
                 PARTITION q0 VALUES LESS THAN (10 + 5), \
                 PARTITION q1 VALUES LESS THAN (MAXVALUE))",
            )
            .unwrap();
        let created = show_create(&mut session, "q");
        assert!(
            created.ends_with(
                "\nPARTITION BY RANGE (`a`)\n(PARTITION `q0` VALUES LESS THAN (15),\n PARTITION \
                 `q1` VALUES LESS THAN (MAXVALUE))"
            ),
            "under {zone} got {created}"
        );
    }
}

/// Every RANGE routing this unit captured from real TiDB, BY VALUE, with the
/// boundary rows that decide `VALUES LESS THAN`'s exclusivity.
///
/// The capture is `range(a) (p0 < 10, p1 < 20, pm < MAXVALUE)` over
/// `5,9,10,19,20,100,-1,NULL`, read back through `SELECT ... PARTITION (p)`.
/// `9` and `10` straddle the first bound and `19`/`20` the second; a router
/// off by one on either would move exactly those rows and answer every later
/// pruned read wrongly, with no error at all.
#[test]
fn range_routing_matches_real_tidb() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE r1 (a int, b int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN \
             (10), PARTITION p1 VALUES LESS THAN (20), PARTITION pm VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .run("INSERT INTO r1 VALUES (5,1),(9,2),(10,3),(19,4),(20,5),(100,6),(-1,7),(NULL,8)")
        .unwrap();
    // Captured: p0 holds NULL,-1,5,9; p1 holds 10,19; pm holds 20,100.
    assert_eq!(
        partition_counts(&mut session, "r1"),
        vec![
            ("p0".to_owned(), 4),
            ("p1".to_owned(), 2),
            ("pm".to_owned(), 2),
        ]
    );
    // Captured verbatim, NULL first because `ORDER BY a` sorts it below
    // every value.
    for (partition, rows) in [
        ("p0", vec!["NULL", "-1", "5", "9"]),
        ("p1", vec!["10", "19"]),
        ("pm", vec!["20", "100"]),
    ] {
        assert_eq!(
            tests_support::row_text(session.run(&format!(
                "SELECT a FROM r1 PARTITION ({partition}) ORDER BY a"
            )))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
            rows.iter().map(|v| (*v).to_owned()).collect::<Vec<_>>(),
            "the rows real TiDB put in {partition}"
        );
    }
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM r1")),
        vec![vec!["8".to_owned()]],
        "the scan must span every partition"
    );
    session.run("ADMIN CHECK TABLE r1").expect("admin check");
}

/// Without a `MAXVALUE` partition a row above the last bound is REFUSED with
/// 1526, while NULL still lands in the lowest partition.
///
/// Both captured on `range(a) (p0 < 10, p1 < 20)`: `INSERT (25,1)` fails and
/// `INSERT (NULL,1)` succeeds, with `PARTITION (p0)` then returning NULL.
#[test]
fn a_range_table_without_maxvalue_refuses_the_row_it_cannot_place() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE r2 (a int, b int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN \
             (10), PARTITION p1 VALUES LESS THAN (20))",
        )
        .unwrap();
    session.run("INSERT INTO r2 VALUES (5,1)").unwrap();
    let rendered = session
        .run("INSERT INTO r2 VALUES (25,1)")
        .expect_err("no partition accepts 25")
        .to_mysql_error();
    assert_eq!(rendered.code, 1526);
    assert_eq!(rendered.message, "Table has no partition for value 25");
    session.run("INSERT INTO r2 VALUES (NULL,1)").unwrap();
    assert_eq!(
        tests_support::row_text(session.run("SELECT a FROM r2 PARTITION (p0) ORDER BY a"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec!["NULL".to_owned(), "5".to_owned()]
    );
}

/// A RANGE table partitioned on an EXPRESSION routes by the expression's
/// value, not the column's.
///
/// Captured on `range(a+1) (p0 < 10, p1 < 20)`: `8` is in `p0` while `9` and
/// `10` are in `p1`, so the boundary sits at `a = 9`, one below where a node
/// reading the bare column would put it.
#[test]
fn a_range_expression_routes_by_its_own_value() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE r3 (a int) PARTITION BY RANGE(a+1) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN (20))",
        )
        .unwrap();
    session.run("INSERT INTO r3 VALUES (8),(9),(10)").unwrap();
    assert_eq!(
        partition_counts(&mut session, "r3"),
        vec![("p0".to_owned(), 1), ("p1".to_owned(), 2)]
    );
    assert!(show_create(&mut session, "r3").contains("PARTITION BY RANGE ((`a`+1))"));
}

/// The RECORDS a pruned read actually touches, from `EXPLAIN ANALYZE`'s
/// `actRows` -- the receipt no plan assertion can give.
///
/// A plan may name a table scan and still walk every partition underneath:
/// the `WHERE` above would filter the surplus, the answer would be right, and
/// every assertion about the printed tree would pass while the read that
/// pruning exists to avoid still happened. `actRows` on the source counts
/// rows READ before any filter, so it is the one number that tells a pruned
/// read from an unpruned one.
///
/// The table holds three rows per partition, so an unpruned read of any of
/// these reports 9.
#[test]
fn range_pruning_reads_only_the_partitions_that_can_match() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE r (a int, b int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN \
             (10), PARTITION p1 VALUES LESS THAN (20), PARTITION pm VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .run("INSERT INTO r VALUES (1,1),(5,5),(9,9),(10,10),(15,15),(19,19),(20,20),(25,25),(99,99)")
        .unwrap();
    // (predicate, records the scan must read, rows the statement returns)
    for (predicate, read, returned) in [
        // One partition each, with the BOUNDARY values on both sides: `9`
        // and `10` differ by one and must land in different partitions, so a
        // pruner off by one reads the wrong three records here.
        ("a < 10", "3", "3"),
        ("a = 9", "3", "1"),
        ("a = 10", "3", "1"),
        ("a >= 20", "3", "3"),
        // Two partitions: the predicate straddles the first boundary.
        ("a >= 9 AND a <= 10", "6", "2"),
        // All three.
        ("a > 0", "9", "9"),
        // No partition can hold it, and the scan reads NOTHING -- the one
        // direction that must never become "everything".
        ("a >= 10 AND a < 10", "0", "0"),
        // A predicate on a NON-partitioning column prunes nothing.
        ("b = 15", "9", "1"),
    ] {
        let rows = tests_support::row_text(session.run(&format!(
            "EXPLAIN ANALYZE SELECT b FROM r WHERE {predicate}"
        )));
        let scan = rows.last().expect("a plan has a source row");
        assert_eq!(
            scan[2], read,
            "records read changed for `{predicate}` (source: {})",
            scan[0]
        );
        assert_eq!(
            rows[0][2], returned,
            "rows returned changed for `{predicate}`"
        );
    }
}

/// HASH pruning must narrow the physical scan, not merely leave the WHERE to
/// discard rows after all partitions were read. Each partition holds three
/// rows, so the source `actRows` makes the distinction observable.
#[test]
fn hash_pruning_reads_only_the_partitions_that_can_match() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session
        .run(
            "INSERT INTO h VALUES (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),\
             (6,6),(7,7),(8,8),(9,9),(10,10),(11,11)",
        )
        .unwrap();

    for (predicate, read, returned) in [
        ("a = 5", "3", "1"),
        ("a IN (1, 5)", "3", "2"),
        ("a BETWEEN 5 AND 6", "6", "2"),
        ("a > 0", "12", "11"),
    ] {
        let rows = tests_support::row_text(session.run(&format!(
            "EXPLAIN ANALYZE SELECT b FROM h WHERE {predicate}"
        )));
        let scan = rows.last().expect("a plan has a source row");
        assert_eq!(scan[2], read, "records read changed for `{predicate}`");
        assert_eq!(
            rows[0][2], returned,
            "rows returned changed for `{predicate}`"
        );
    }
}

/// An UPDATE that moves a row across a RANGE boundary moves its storage too,
/// and leaves no copy behind.
#[test]
fn an_update_across_a_range_boundary_moves_the_row() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE r (a int primary key, b int) PARTITION BY RANGE(a) (PARTITION p0 VALUES \
             LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
        )
        .unwrap();
    session.run("INSERT INTO r VALUES (9, 1)").unwrap();
    assert_eq!(sole_holder(&mut session, "r"), "p0");
    session.run("UPDATE r SET a = 10").unwrap();
    assert_eq!(
        sole_holder(&mut session, "r"),
        "p1",
        "the row must have moved, exactly once"
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT a, b FROM r")),
        vec![vec!["10".to_owned(), "1".to_owned()]]
    );
    session.run("ADMIN CHECK TABLE r").expect("admin check");
}

/// The methods still without routing are REFUSED, and the refusal names
/// the method. The `GO_*` text each row carries is what this must answer
/// instead once that method lands.
#[test]
fn key_and_columns_partitioning_are_still_refused() {
    for (sql, table, go) in [
        (
            "CREATE TABLE k1 (a int PRIMARY KEY, b int) PARTITION BY KEY(a) PARTITIONS 3",
            "k1",
            GO_KEY,
        ),
        (
            "CREATE TABLE rc1 (a int, b int) PARTITION BY RANGE COLUMNS (a, b) (PARTITION p0 VALUES LESS THAN (10, 10), PARTITION p1 VALUES LESS THAN (MAXVALUE, MAXVALUE))",
            "rc1",
            GO_RANGE_COLUMNS,
        ),
        (
            "CREATE TABLE lc1 (a int, b int) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((1,1),(2,2)))",
            "lc1",
            GO_LIST_COLUMNS,
        ),
    ] {
        let mut session = Session::new();
        let error = session
            .run(sql)
            .expect_err("a method without routing must not report success");
        let text = format!("{error:?}");
        assert!(
            text.contains("PARTITION BY"),
            "the refusal must name the clause it refused, got {text}"
        );
        // The refusal is total: nothing was created under that name, so a
        // later statement cannot read a table this node built wrongly.
        assert!(
            session.run(&format!("SHOW CREATE TABLE {table}")).is_err(),
            "a refused CREATE TABLE must leave no table behind"
        );
        assert!(go.contains("PARTITION BY"), "capture sanity");
    }
}

/// The partitioning is scoped to the clause alone: the SAME table without it
/// is still created, still behaves, and prints no partition clause.
#[test]
fn an_unpartitioned_create_table_is_untouched() {
    let mut session = Session::new();
    session.run("CREATE TABLE q (a int, b int)").unwrap();
    session.run("INSERT INTO q VALUES (1, 2)").unwrap();
    assert!(
        !show_create(&mut session, "q").contains("PARTITION BY"),
        "an unpartitioned table prints no partition clause"
    );
    assert!(partition_counts(&mut session, "q").is_empty());
}

/// The partition definitions real TiDB REJECTS, with the errno and message it
/// rejects each one under, and whether the RULE behind it is ported.
///
/// A `true` row is checked as errno EQUALITY. A `false` row is only checked
/// for rejection: it is refused, but by something OTHER than the rule -- the
/// method gate for a method still without routing, or the parser -- and its
/// errno becomes an assertion when the rule itself is what refuses it. Each
/// `false` row carries a comment saying which.
const GO_REJECTED: &[(&str, u16, &str, bool)] = &[
    (
        "CREATE TABLE e1 (a varchar(10)) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10))",
        1659,
        "Field 'a' is of a not allowed type for this type of partitioning",
        true,
    ),
    (
        "CREATE TABLE e14 (a double) PARTITION BY HASH(a) PARTITIONS 2",
        1659,
        "Field 'a' is of a not allowed type for this type of partitioning",
        true,
    ),
    (
        "CREATE TABLE e2 (a int) PARTITION BY RANGE(b) (PARTITION p0 VALUES LESS THAN (10))",
        1054,
        "Unknown column 'b' in 'partition function'",
        true,
    ),
    (
        "CREATE TABLE e2h (a int) PARTITION BY HASH(b) PARTITIONS 2",
        1054,
        "Unknown column 'b' in 'partition function'",
        true,
    ),
    (
        "CREATE TABLE e3 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (5))",
        1493,
        "VALUES LESS THAN value must be strictly increasing for each partition",
        true,
    ),
    (
        "CREATE TABLE e4 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p0 VALUES LESS THAN (20))",
        1517,
        "Duplicate partition name p0",
        false,
    ),
    (
        "CREATE TABLE e4h (a int) PARTITION BY HASH(a) (PARTITION p0, PARTITION p0)",
        1517,
        "Duplicate partition name p0",
        true,
    ),
    (
        "CREATE TABLE e5 (a int) PARTITION BY LIST(a) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (1))",
        1495,
        "Multiple definition of same constant in list partitioning",
        true,
    ),
    (
        "CREATE TABLE e6 (a int) PARTITION BY HASH(a) PARTITIONS 0",
        1504,
        "Number of partitions = 0 is not an allowed value",
        true,
    ),
    (
        // Refused, but by the PARSER (1064) rather than by the DDL rule: this
        // node's grammar requires a definition list after `PARTITION BY
        // RANGE`, where Go's admits the empty one and rejects it at build
        // time with 1492. `build_range_bounds` raises the real 1492 for every
        // shape that does reach it, and this row becomes an errno equality
        // when the grammar accepts the statement Go accepts.
        "CREATE TABLE e7 (a int) PARTITION BY RANGE(a)",
        1492,
        "For RANGE partitions each partition must be defined",
        false,
    ),
    (
        "CREATE TABLE e8 (a int) PARTITION BY LIST(a) (PARTITION p0 VALUES LESS THAN (1))",
        1480,
        "Only RANGE PARTITIONING can use VALUES LESS THAN in partition definition",
        false,
    ),
    (
        "CREATE TABLE e10 (a int) PARTITION BY HASH(rand()) PARTITIONS 2",
        1564,
        "This partition function is not allowed",
        true,
    ),
    (
        "CREATE TABLE e11 (a int UNIQUE KEY, b int) PARTITION BY HASH(b) PARTITIONS 2",
        8264,
        "Global Index is needed for index 'a', since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption",
        true,
    ),
    // The RANGE rules this unit captured and ported with the routing. Each
    // one is a statement that would have started SILENTLY SUCCEEDING the
    // moment `PARTITION BY RANGE` was accepted without it.
    (
        // Refused by the PARSER (1064), which pairs each method with its own
        // value clause before the DDL builder sees the statement; Go's
        // grammar accepts the pair and `PartitionDefinitionClause.Validate`
        // rejects it with 1480. `build_range_bounds` raises the real 1480
        // for it, so this row becomes an errno equality if the grammar is
        // ever loosened to Go's.
        "CREATE TABLE e9 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES IN (1))",
        1480,
        "Only LIST PARTITIONING can use VALUES IN in partition definition",
        false,
    ),
    (
        "CREATE TABLE e12 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN MAXVALUE, PARTITION p1 VALUES LESS THAN (20))",
        1481,
        "MAXVALUE can only be used in last partition definition",
        true,
    ),
    (
        "CREATE TABLE e13 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN ('abc'))",
        1697,
        "VALUES value for partition 'p0' must have type INT",
        true,
    ),
    (
        "CREATE TABLE e16 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (1.5))",
        1697,
        "VALUES value for partition 'p0' must have type INT",
        true,
    ),
    (
        "CREATE TABLE e15 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (NULL))",
        1659,
        "Field 'NULL' is of a not allowed type for this type of partitioning",
        true,
    ),
    (
        "CREATE TABLE e17 (a int UNIQUE KEY, b int) PARTITION BY RANGE(b) (PARTITION p0 VALUES LESS THAN (10))",
        8264,
        "Global Index is needed for index 'a', since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption",
        true,
    ),
    (
        "CREATE TABLE e18 (a int) PARTITION BY RANGE(rand()) (PARTITION p0 VALUES LESS THAN (10))",
        1564,
        "This partition function is not allowed",
        true,
    ),
    (
        "CREATE TABLE e3b (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (10))",
        1493,
        "VALUES LESS THAN value must be strictly increasing for each partition",
        true,
    ),
];

/// Every definition TiDB rejects, this node rejects too.
///
/// The unported rows agree for the WRONG REASON: one blanket method refusal
/// rather than the rule. That is what keeps this a tripwire -- the moment
/// RANGE or LIST is accepted, those rows start SUCCEEDING unless the
/// validation in Go's `buildTablePartitionInfo` is ported with the routing,
/// and this test is where that shows up.
#[test]
fn every_definition_tidb_rejects_is_rejected_here_too() {
    for (sql, errno, message, _) in GO_REJECTED {
        let mut session = Session::new();
        assert!(
            session.run(sql).is_err(),
            "TiDB rejects this with {errno} ({message}); this node must not accept it: {sql}"
        );
    }
}

/// The rejections whose rule is ported carry TiDB's own errno and message.
#[test]
fn the_ported_rejections_carry_tidbs_own_errno() {
    for (sql, errno, message, ported) in GO_REJECTED {
        if !ported {
            continue;
        }
        let mut session = Session::new();
        let rendered = session.run(sql).expect_err(sql).to_mysql_error();
        assert_eq!(rendered.code, *errno, "{sql}");
        assert_eq!(rendered.message, *message, "{sql}");
    }
}

/// `PARTITION BY` written on a `CREATE TABLE IF NOT EXISTS` is validated too:
/// `IF NOT EXISTS` suppresses the "already exists" error, never the admission
/// check.
#[test]
fn if_not_exists_does_not_suppress_the_validation() {
    let mut session = Session::new();
    assert!(session
        .run("CREATE TABLE IF NOT EXISTS ine (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (5))")
        .is_err());
    session
        .run("CREATE TABLE IF NOT EXISTS ine (a int) PARTITION BY HASH(a) PARTITIONS 2")
        .expect("a HASH clause is built under IF NOT EXISTS too");
}

/// `SELECT ... PARTITION (p)` reads THOSE partitions and no others, for every
/// method that has routing, and a name the table does not have is 1735.
///
/// This is the third way a partitioned table can answer wrongly: ignoring the
/// clause returns MORE rows than were asked for, with no error. The
/// assertions are therefore row-set assertions -- `p0` of a hash table over
/// `1,2,4` holds `4` alone, and the whole table holds three rows.
#[test]
fn a_partition_selection_reads_only_those_partitions() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session
        .run("INSERT INTO h VALUES (1,1),(2,2),(4,4)")
        .unwrap();
    assert_eq!(
        tests_support::row_text(session.run("SELECT a FROM h PARTITION (p0) ORDER BY a")),
        vec![vec!["4".to_owned()]]
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT a FROM h PARTITION (p0, p1) ORDER BY a")),
        vec![vec!["1".to_owned()], vec!["4".to_owned()]]
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM h")),
        vec![vec!["3".to_owned()]],
        "the unrestricted read still spans the whole table"
    );
    // Captured: `Unknown partition 'nosuch' in table 'ok1'`.
    let rendered = session
        .run("SELECT a FROM h PARTITION (nosuch)")
        .expect_err("an unknown partition is an error")
        .to_mysql_error();
    assert_eq!(rendered.code, 1735);
    assert_eq!(rendered.message, "Unknown partition 'nosuch' in table 'h'");
    // An UNPARTITIONED table has no name to resolve, so the same 1735.
    session.run("CREATE TABLE q (a int)").unwrap();
    assert_eq!(
        session
            .run("SELECT a FROM q PARTITION (p0)")
            .expect_err("no partition of an unpartitioned table")
            .to_mysql_error()
            .code,
        1735
    );
}

/// `UPDATE`/`DELETE ... PARTITION (p)` restrict which existing rows the
/// statement reaches. `INSERT ... PARTITION (p)` instead validates each new
/// row's routed destination against the selected set.
#[test]
fn updates_and_deletes_restricted_to_partitions_do_not_escape_the_named_set() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session
        .run("INSERT INTO h VALUES (1,1),(2,2),(4,4)")
        .unwrap();
    assert_eq!(
        session.run("UPDATE h PARTITION (p0) SET b = 9").unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        session.run("DELETE FROM h PARTITION (p1)").unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        session
            .run("INSERT INTO h PARTITION (p0) VALUES (4, 4)")
            .unwrap(),
        StmtResult::Affected(1)
    );
    let error = session
        .run("INSERT INTO h PARTITION (p0) VALUES (1, 1)")
        .expect_err("a row outside the named destination set is rejected")
        .to_mysql_error();
    assert_eq!(error.code, 1748);
    assert_eq!(
        error.message,
        "Found a row not matching the given partition set"
    );
    assert_eq!(
        session
            .run("INSERT INTO h PARTITION (p0) SELECT 8, 8")
            .unwrap(),
        StmtResult::Affected(1),
        "VALUES and INSERT ... SELECT share the completed-row routing check"
    );
    let error = session
        .run("INSERT INTO h PARTITION (nosuch) VALUES (4, 4)")
        .expect_err("a named partition is resolved before the write starts")
        .to_mysql_error();
    assert_eq!(error.code, 1735);
    assert_eq!(error.message, "Unknown partition 'nosuch' in table 'h'");
    assert_eq!(
        tests_support::row_text(session.run("SELECT a, b FROM h ORDER BY a")),
        vec![
            vec!["2".to_owned(), "2".to_owned()],
            vec!["4".to_owned(), "9".to_owned()],
            vec!["4".to_owned(), "4".to_owned()],
            vec!["8".to_owned(), "8".to_owned()],
        ]
    );
    session.run("CREATE TABLE q (a int)").unwrap();
    let error = session
        .run("UPDATE q PARTITION (p0) SET a = 1")
        .expect_err("an unpartitioned target has no named partition")
        .to_mysql_error();
    assert_eq!(error.code, 1735);
    assert_eq!(error.message, "Unknown partition 'p0' in table 'q'");
    let error = session
        .run("INSERT INTO q PARTITION (p0) VALUES (1)")
        .expect_err("an unpartitioned INSERT target has no named partition")
        .to_mysql_error();
    assert_eq!(error.code, 1735);
    assert_eq!(error.message, "Unknown partition 'p0' in table 'q'");
}

/// A selected UPDATE is not just a restricted scan: the destination of an
/// update that changes the partition expression must stay in that selection.
#[test]
fn a_partition_qualified_update_cannot_move_a_row_outside_its_selected_set() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session.run("INSERT INTO h VALUES (4, 40)").unwrap();

    let error = session
        .run("UPDATE h PARTITION (p0) SET a = 1")
        .expect_err("the p0 source cannot be moved into p1")
        .to_mysql_error();
    assert_eq!(error.code, 1748);
    assert_eq!(
        error.message,
        "Found a row not matching the given partition set"
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT a, b FROM h")),
        vec![vec!["4".to_owned(), "40".to_owned()]],
        "the rejected staged write leaves the selected row in p0"
    );
}

/// The partition expression is keyed by the NAMES it reads, so an
/// `ALTER TABLE` that inserts a column before the partitioning column cannot
/// re-route the table.
///
/// This is #202's second pin, and it is the one with no error to notice:
/// with the expression indexing the row by offset, `ADD COLUMN z int FIRST`
/// shifted `b` from offset 1 to 2 while routing went on reading offset 1 --
/// which is now `a` -- so rows written after the ALTER landed in a different
/// partition from identically-keyed rows written before it, and a
/// partition-pruned read then found only half of them.
#[test]
fn a_column_move_does_not_reroute_a_partitioned_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE hm (a int, b int) PARTITION BY HASH(b) PARTITIONS 4")
        .unwrap();
    session.run("INSERT INTO hm VALUES (7, 1)").unwrap();
    let holder = sole_holder(&mut session, "hm");

    session
        .run("ALTER TABLE hm ADD COLUMN z int FIRST")
        .unwrap();
    session
        .run("INSERT INTO hm (a, b, z) VALUES (8, 1, 100)")
        .unwrap();

    let counts = partition_counts(&mut session, "hm");
    assert_eq!(
        counts
            .iter()
            .find(|(name, _)| *name == holder)
            .map(|(_, rows)| *rows),
        Some(2),
        "both b=1 rows belong to the partition b=1 hashes to, got {counts:?}"
    );
}

/// Go `checkDropColumnWithPartitionConstraint` (`pkg/ddl/executor.go`), which
/// `RenameColumn` and `DropColumn` both call: a column the partition
/// expression -- or a `COLUMNS` list -- reads cannot be renamed or dropped,
/// because nothing rewrites that expression. 3855, reporting the column name
/// LOWERCASED, which is Go's `GenWithStackByArgs(colName.L)`.
#[test]
fn renaming_a_partitioning_column_is_refused() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE hp (Ab int, b int) PARTITION BY HASH(Ab) PARTITIONS 4")
        .unwrap();
    let rendered = session
        .run("ALTER TABLE hp RENAME COLUMN Ab TO z")
        .expect_err("the partition expression reads Ab")
        .to_mysql_error();
    assert_eq!(rendered.code, 3855);
    assert_eq!(
        rendered.message,
        "Column 'ab' has a partitioning function dependency and cannot be dropped or renamed"
    );
    assert_eq!(
        session
            .run("ALTER TABLE hp DROP COLUMN Ab")
            .expect_err("the partition expression reads Ab")
            .to_mysql_error()
            .code,
        3855
    );
    // The column the expression does NOT read renames.
    session.run("ALTER TABLE hp RENAME COLUMN b TO b2").unwrap();
}

/// The NON-EMPTY `access object` cells of a plan, which is where the
/// partition annotation lands. The empty ones belong to the operators above
/// the leaves (`Projection`, `Selection`, the union itself) and say nothing
/// about which partition was read.
fn plan_access_objects(session: &mut Session, sql: &str) -> Vec<String> {
    crate::tests_support::row_text(session.run(sql))
        .into_iter()
        .map(|row| row[3].clone())
        .filter(|object| !object.is_empty())
        .collect()
}

/// Go `BatchPointGetPlan.AccessObject()`: a batch point get names the
/// partitions its handles route into, deduplicated, in DEFINITION order and
/// in the case they were DECLARED in.
///
/// Recorded verbatim in
/// `tests/integrationtest/r/planner/core/partition_pruner.result`, whose
/// partitions are deliberately spelled `P0`, `p1`, `P2`:
///
/// ```text
/// explain format = 'brief' select * from t where a IN (1, 2);
///   Batch_Point_Get  2.00  root  table:t, partition:p1,P2  handle:[1 2], ...
/// ```
///
/// MUTATION: return an empty vector from `KvTable::handle_partition_names`
/// and the annotation vanishes; sort the ordinals by first appearance instead
/// of ascending, or lowercase the names, and the text stops matching TiDB's.
#[test]
fn a_batch_point_get_names_the_partitions_its_handles_reach() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a INT PRIMARY KEY, b INT, KEY (b)) \
             PARTITION BY HASH(a) (PARTITION P0, PARTITION p1, PARTITION P2)",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(2,2),(3,3)")
        .unwrap();

    let objects = plan_access_objects(&mut session, "EXPLAIN SELECT * FROM t WHERE a IN (1, 2)");
    assert_eq!(objects, vec!["table:t, partition:p1,P2".to_owned()]);
    // A repeated handle contributes no second partition name.
    assert_eq!(
        plan_access_objects(&mut session, "EXPLAIN SELECT * FROM t WHERE a IN (1, 2, 1)"),
        vec!["table:t, partition:p1,P2".to_owned()]
    );
    // Handles 1, 2, 3 land in p1, P2, P0 -- so the ASCENDING handle order and
    // the DEFINITION order disagree, and Go sorts the ORDINALS. Without that
    // sort this reads `p1,P2,P0`.
    assert_eq!(
        plan_access_objects(&mut session, "EXPLAIN SELECT * FROM t WHERE a IN (1, 2, 3)"),
        vec!["table:t, partition:P0,p1,P2".to_owned()]
    );
    // Control: an UNPARTITIONED table prints no partition clause at all.
    session
        .run("CREATE TABLE u (a INT PRIMARY KEY, b INT)")
        .unwrap();
    assert_eq!(
        plan_access_objects(&mut session, "EXPLAIN SELECT * FROM u WHERE a IN (1, 2)"),
        vec!["table:u".to_owned()]
    );
}

/// Go's `rule_partition_processor` under `@@tidb_partition_prune_mode =
/// 'static'`: one scan per SURVIVING partition, under a `PartitionUnion`,
/// each naming its own partition. Under `dynamic` -- the shipped default --
/// there is one scan and no partition clause on it.
///
/// MUTATION: ignore `StmtContext::static_partition_prune` and the dynamic
/// control below gains three scans it must not have; drop the pruning filter
/// from `surviving_partition_names` and the range case names `p0` too.
#[test]
fn static_prune_mode_fans_a_partitioned_scan_out_per_partition() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t2 (a INT) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (0), \
             PARTITION p1 VALUES LESS THAN (10), \
             PARTITION p2 VALUES LESS THAN (20))",
        )
        .unwrap();

    // Control: the shipped `dynamic` mode leaves the one scan alone.
    assert_eq!(
        plan_access_objects(&mut session, "EXPLAIN SELECT * FROM t2 WHERE a >= 5"),
        vec!["table:t2".to_owned()]
    );

    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    // `a >= 5` cannot be in `p0` (`a < 0`), so the fan-out has TWO branches,
    // not three: it names what survived pruning.
    assert_eq!(
        plan_access_objects(&mut session, "EXPLAIN SELECT * FROM t2 WHERE a >= 5"),
        vec![
            "table:t2, partition:p1".to_owned(),
            "table:t2, partition:p2".to_owned(),
        ]
    );
    // A statement's own `PARTITION (p)` narrows it the same way, and one
    // surviving partition is annotated WITHOUT a union of one branch.
    assert_eq!(
        plan_access_objects(&mut session, "EXPLAIN SELECT * FROM t2 PARTITION (p2)"),
        vec!["table:t2, partition:p2".to_owned()]
    );
}
