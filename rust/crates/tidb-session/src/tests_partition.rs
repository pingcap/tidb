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

//! `CREATE TABLE ... PARTITION BY HASH`, `BY KEY`, `BY RANGE`, `BY RANGE
//! COLUMNS`, and scalar `BY LIST` are REAL; unsupported methods remain
//! refused as a tripwire.
//!
//! This node stores a `PartitionSpec` (Go `model.PartitionInfo`; see
//! `tidb_executor::partition_routing`), routes each row into one of N
//! physical key prefixes exactly as Go's `locatePartition` does, PRUNES which
//! of them a `WHERE` reads (`tidb_executor::partition_pruning`, Go
//! `pkg/planner/core/rule/rule_partition_processor.go`), answers
//! `... PARTITION (p)` from the named partitions alone, and prints the clause
//! back through `SHOW CREATE TABLE`.
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

/// LIST COLUMNS routes the declared, converted tuple as one key. Matching one
/// component alone must not select a partition; DEFAULT receives a tuple no
/// explicit definition owns.
#[test]
fn list_columns_partitioning_routes_typed_tuples() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE lc (a int, b varchar(8), c int) PARTITION BY LIST COLUMNS(a,b) (\
             PARTITION p0 VALUES IN ((1,'a'),(2,'b')), \
             PARTITION p1 VALUES IN ((1,'b')), PARTITION pd DEFAULT)",
        )
        .expect("LIST COLUMNS is routed as a tuple");
    for ((a, b), expected) in [
        (("1", "'a'"), "p0"),
        (("2", "'b'"), "p0"),
        (("1", "'b'"), "p1"),
        (("9", "'z'"), "pd"),
    ] {
        session
            .run(&format!("INSERT INTO lc VALUES ({a},{b},1)"))
            .unwrap();
        assert_eq!(
            sole_holder(&mut session, "lc"),
            expected,
            "LIST COLUMNS({a},{b})"
        );
        session.run("DELETE FROM lc").unwrap();
    }
    let shown = show_create(&mut session, "lc");
    assert!(
        shown.contains("PARTITION BY LIST COLUMNS(`a`,`b`)"),
        "{shown}"
    );
    assert!(
        shown.contains("PARTITION `p0` VALUES IN ((1,'a'),(2,'b'))"),
        "{shown}"
    );
    assert!(shown.contains("PARTITION `pd` DEFAULT"), "{shown}");

    session
        .run(
            "CREATE TABLE lc1 (a int, b int) PARTITION BY LIST COLUMNS(a,b) \
             (PARTITION p0 VALUES IN ((1,1),(2,2)))",
        )
        .unwrap();
    assert_eq!(show_create(&mut session, "lc1"), GO_LIST_COLUMNS);
}

/// RANGE COLUMNS compares the declared typed tuple lexicographically.  The
/// boundary belongs to the first partition whose `VALUES LESS THAN` tuple is
/// greater; `MAXVALUE` ends the search and NULL remains in the first
/// partition, as it does for scalar RANGE.
#[test]
fn range_columns_partitioning_routes_typed_tuples() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE rc (a int, b varchar(8), c int) PARTITION BY RANGE COLUMNS(a,b) (\
             PARTITION p0 VALUES LESS THAN (2,'m'), \
             PARTITION p1 VALUES LESS THAN (4,'z'), \
             PARTITION pm VALUES LESS THAN (MAXVALUE,MAXVALUE))",
        )
        .expect("RANGE COLUMNS must create a routed table");
    for ((a, b), expected) in [
        (("1", "'z'"), "p0"),
        (("2", "'a'"), "p0"),
        (("2", "'m'"), "p1"),
        (("4", "'z'"), "pm"),
        (("NULL", "'z'"), "p0"),
    ] {
        session
            .run(&format!("INSERT INTO rc VALUES ({a},{b},1)"))
            .unwrap();
        assert_eq!(
            sole_holder(&mut session, "rc"),
            expected,
            "RANGE COLUMNS({a},{b})"
        );
        session.run("DELETE FROM rc").unwrap();
    }
    let shown = show_create(&mut session, "rc");
    assert!(
        shown.contains("PARTITION BY RANGE COLUMNS(`a`,`b`)"),
        "{shown}"
    );
    assert!(
        shown.contains("PARTITION `p0` VALUES LESS THAN (2,'m')"),
        "{shown}"
    );
    assert!(
        shown.contains("PARTITION `pm` VALUES LESS THAN (MAXVALUE,MAXVALUE)"),
        "{shown}"
    );
}

/// LIST COLUMNS validates the typed tuple set during DDL and refuses an
/// unmatched write without DEFAULT. These rules must happen before any row is
/// written into a physical partition.
#[test]
fn list_columns_validates_normalized_tuple_definitions() {
    let duplicate = Session::new()
        .run(
            "CREATE TABLE lcd (a int, b date) PARTITION BY LIST COLUMNS(a,b) (\
             PARTITION p0 VALUES IN ((1,'2020-02-02')), \
             PARTITION p1 VALUES IN ((+1,'20200202')))",
        )
        .expect_err("converted date/integer tuples collide")
        .to_mysql_error();
    assert_eq!(duplicate.code, 1495);
    assert_eq!(
        duplicate.message,
        "Multiple definition of same constant in list partitioning"
    );

    let wrong_type = Session::new()
        .run(
            "CREATE TABLE lct (a tinyint) PARTITION BY LIST COLUMNS(a) \
             (PARTITION p0 VALUES IN (65536))",
        )
        .expect_err("a definition must fit its declared column type")
        .to_mysql_error();
    assert_eq!(wrong_type.code, 1654);
    assert_eq!(
        wrong_type.message,
        "Partition column values of incorrect type"
    );

    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE lcn (a int, b int) PARTITION BY LIST COLUMNS(a,b) \
             (PARTITION p0 VALUES IN ((1,2)))",
        )
        .unwrap();
    let unmatched = session
        .run("INSERT INTO lcn VALUES (1,3)")
        .expect_err("one matching component is not a matching tuple")
        .to_mysql_error();
    assert_eq!(unmatched.code, 1526);
    assert_eq!(
        unmatched.message,
        "Table has no partition for value from column_list"
    );
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
    session
        .run("INSERT IGNORE INTO r2 VALUES (25,2)")
        .expect("IGNORE skips an unroutable row");
    assert_eq!(
        crate::tests_support::row_text(session.run("SHOW WARNINGS")).as_slice(),
        [["Warning", "1526", "Table has no partition for value 25"]]
    );
    session
        .run("INSERT IGNORE INTO r2 PARTITION (p0) VALUES (15,2)")
        .expect("IGNORE skips a row outside the named partition set");
    assert_eq!(
        crate::tests_support::row_text(session.run("SHOW WARNINGS")).as_slice(),
        [[
            "Warning",
            "1748",
            "Found a row not matching the given partition set"
        ]]
    );
    session
        .run("UPDATE IGNORE r2 SET a=25 WHERE a=5")
        .expect("IGNORE leaves an update in its original partition");
    assert_eq!(
        crate::tests_support::row_text(session.run("SHOW WARNINGS")).as_slice(),
        [["Warning", "1526", "Table has no partition for value 25"]]
    );
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

/// LIST pruning is value ownership, not a range-bound approximation. Point,
/// IN, NULL, and impossible-value predicates must touch exactly the physical
/// partitions that can own a matching row.
#[test]
fn scalar_list_pruning_reads_only_matching_owners() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE lpr (a int, b int) PARTITION BY LIST(a) (PARTITION p0 VALUES IN \
             (1,3,7), PARTITION p1 VALUES IN (NULL,5,9))",
        )
        .unwrap();
    session
        .run("INSERT INTO lpr VALUES (1,1),(3,3),(7,7),(NULL,0),(5,5),(9,9)")
        .unwrap();
    for (predicate, read, returned) in [
        ("a = 1", "3", "1"),
        ("a IN (1,5)", "6", "2"),
        ("a IS NULL", "3", "1"),
        ("a = 2", "0", "0"),
    ] {
        let rows = tests_support::row_text(session.run(&format!(
            "EXPLAIN ANALYZE SELECT b FROM lpr WHERE {predicate}"
        )));
        let scan = rows.last().expect("a plan has a source row");
        assert_eq!(scan[2], read, "records read for `{predicate}`: {}", scan[0]);
        assert_eq!(rows[0][2], returned, "rows returned for `{predicate}`");
    }
}

/// Tuple predicates negotiate both LIST COLUMNS key parts with the ranger.
/// `a = 1 AND b = 2` reads the one owning partition, while an `a`-only range
/// retains every partition containing that leading value.
#[test]
fn list_columns_pruning_reads_matching_tuple_owners() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE lcpr (a int, b int, c int) PARTITION BY LIST COLUMNS(a,b) (\
             PARTITION p0 VALUES IN ((1,2),(2,2)), \
             PARTITION p1 VALUES IN ((1,3),(3,3)))",
        )
        .unwrap();
    session
        .run("INSERT INTO lcpr VALUES (1,2,12),(2,2,22),(1,3,13),(3,3,33)")
        .unwrap();
    for (predicate, read, returned) in [
        ("a = 1 AND b = 2", "2", "1"),
        ("a = 1", "4", "2"),
        ("a = 9 AND b = 9", "0", "0"),
    ] {
        let rows = tests_support::row_text(session.run(&format!(
            "EXPLAIN ANALYZE SELECT c FROM lcpr WHERE {predicate}"
        )));
        let scan = rows.last().expect("a plan has a source row");
        assert_eq!(scan[2], read, "records read for `{predicate}`: {}", scan[0]);
        assert_eq!(rows[0][2], returned, "rows returned for `{predicate}`");
    }
}

/// A full tuple point is one RANGE COLUMNS destination; a leading-column
/// predicate is not enough information to discard a lexicographic neighbor.
#[test]
fn range_columns_pruning_reads_the_matching_tuple_partition() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE rcpr (a int, b int, c int) PARTITION BY RANGE COLUMNS(a,b) (\
             PARTITION p0 VALUES LESS THAN (2,0), \
             PARTITION p1 VALUES LESS THAN (4,0), \
             PARTITION pm VALUES LESS THAN (MAXVALUE,MAXVALUE))",
        )
        .unwrap();
    session
        .run("INSERT INTO rcpr VALUES (1,1,11),(1,9,19),(2,1,21),(3,9,39),(4,1,41),(9,9,99)")
        .unwrap();
    for (predicate, read, returned) in [
        ("a = 2 AND b = 1", "2", "1"),
        ("a = 1 AND b = 9", "2", "1"),
        ("a = 9 AND b = 9", "2", "1"),
        ("a = 1", "6", "2"),
    ] {
        let rows = tests_support::row_text(session.run(&format!(
            "EXPLAIN ANALYZE SELECT c FROM rcpr WHERE {predicate}"
        )));
        let scan = rows.last().expect("a plan has a source row");
        assert_eq!(scan[2], read, "records read for `{predicate}`: {}", scan[0]);
        assert_eq!(rows[0][2], returned, "rows returned for `{predicate}`");
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

/// A complete KEY tuple point has exactly one CRC32 destination. This is a
/// physical scan assertion: all three partitions contain one row, so reading
/// one rather than filtering three after the scan proves the pruning path is
/// wired to the same router as INSERT.
#[test]
fn key_partition_pruning_reads_the_matching_crc32_partition() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE kpr (a int, b int) PARTITION BY KEY(a) PARTITIONS 3")
        .unwrap();
    session
        .run("INSERT INTO kpr VALUES (1,11),(2,22),(7,77)")
        .unwrap();
    let rows =
        tests_support::row_text(session.run("EXPLAIN ANALYZE SELECT b FROM kpr WHERE a = 7"));
    let scan = rows.last().expect("a plan has a source row");
    assert_eq!(scan[2], "1", "one KEY partition must be read: {}", scan[0]);
    assert_eq!(rows[0][2], "1", "the point result survives pruning");
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

/// KEY hashes the ordered `Datum.ToHashKey` values with IEEE CRC32. The
/// captured `a=1` destination is p2 (`CRC32("1") % 3 == 2`); a NULL key adds
/// one zero byte and reaches p1. The independent partition selections make
/// both routing and physical read placement observable.
#[test]
fn key_partitioning_routes_crc32_column_tuples() {
    let (sql, table) = (
        "CREATE TABLE k1 (a int PRIMARY KEY, b int) PARTITION BY KEY(a) PARTITIONS 3",
        "k1",
    );
    let mut session = Session::new();
    session.run(sql).expect("KEY must create a routed table");
    for (value, partition) in [("1", "p2"), ("2", "p1")] {
        session
            .run(&format!("INSERT INTO {table} VALUES ({value}, 1)"))
            .unwrap();
        assert_eq!(sole_holder(&mut session, table), partition, "KEY({value})");
        session.run(&format!("DELETE FROM {table}")).unwrap();
    }
    assert_eq!(show_create(&mut session, table), GO_KEY);

    session
        .run("CREATE TABLE kn (a int, b int) PARTITION BY KEY(a) PARTITIONS 3")
        .unwrap();
    session.run("INSERT INTO kn VALUES (NULL, 1)").unwrap();
    assert_eq!(sole_holder(&mut session, "kn"), "p1", "KEY(NULL)");

    session
        .run("CREATE TABLE ki (a int PRIMARY KEY, b int) PARTITION BY KEY() PARTITIONS 3")
        .unwrap();
    session.run("INSERT INTO ki VALUES (1, 1)").unwrap();
    assert_eq!(sole_holder(&mut session, "ki"), "p2", "KEY() uses PRIMARY");

    session
        .run("CREATE TABLE kh (a int) PARTITION BY KEY() PARTITIONS 3")
        .unwrap();
    session.run("INSERT INTO kh VALUES (1)").unwrap();
    assert_eq!(
        sole_holder(&mut session, "kh"),
        "p0",
        "KEY() without PRIMARY"
    );
}

/// Go rejects duplicate KEY columns and the field types it cannot encode as a
/// partition key before it creates any physical partitions.
#[test]
fn key_partitioning_validates_column_identity_and_type() {
    for (sql, code) in [
        (
            "CREATE TABLE kd (a int) PARTITION BY KEY(a,a) PARTITIONS 2",
            1652,
        ),
        (
            "CREATE TABLE kb (a blob) PARTITION BY KEY(a) PARTITIONS 2",
            1659,
        ),
    ] {
        let error = Session::new()
            .run(sql)
            .expect_err("KEY definition must fail");
        assert_eq!(error.to_mysql_error().code, code, "{sql}");
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
        "CREATE TABLE e3c (a int, b int) PARTITION BY RANGE COLUMNS(a,b) (PARTITION p0 VALUES LESS THAN (2,9), PARTITION p1 VALUES LESS THAN (2,8))",
        1493,
        "VALUES LESS THAN value must be strictly increasing for each partition",
        true,
    ),
    (
        "CREATE TABLE e3d (a int) PARTITION BY RANGE COLUMNS(a,a) (PARTITION p0 VALUES LESS THAN (2,9))",
        1652,
        "Duplicate partition field name 'a'",
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

/// A partition-qualified INSERT keeps the same partition wrapper when a
/// duplicate is found: both the candidate row and the completed update must
/// belong to the named set.
#[test]
fn partition_qualified_on_duplicate_cannot_escape_the_named_set() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a int primary key, b varchar(32)) \
             PARTITION BY RANGE (a) (\
               PARTITION p0 VALUES LESS THAN (5),\
               PARTITION p1 VALUES LESS THAN (10),\
               PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .run("INSERT INTO t PARTITION (p0) VALUES (4, 'original')")
        .unwrap();

    let escaping_update = session
        .run(
            "INSERT INTO t PARTITION (p0) VALUES (4, 'candidate') \
             ON DUPLICATE KEY UPDATE a = a + 1, b = VALUES(b)",
        )
        .expect_err("the duplicate update cannot move the p0 row into p1")
        .to_mysql_error();
    assert_eq!(escaping_update.code, 1748);
    assert_eq!(
        escaping_update.message,
        "Found a row not matching the given partition set"
    );

    let mismatched_candidate = session
        .run(
            "INSERT INTO t PARTITION (p1) VALUES (4, 'candidate') \
             ON DUPLICATE KEY UPDATE b = VALUES(b)",
        )
        .expect_err("the candidate is checked before duplicate-key resolution")
        .to_mysql_error();
    assert_eq!(mismatched_candidate.code, 1748);
    assert_eq!(
        mismatched_candidate.message,
        "Found a row not matching the given partition set"
    );

    assert_eq!(
        tests_support::row_text(session.run("SELECT a, b FROM t")),
        vec![vec!["4".to_owned(), "original".to_owned()]],
        "both rejected statements leave the original p0 row unchanged"
    );
    assert_eq!(
        session
            .run(
                "INSERT IGNORE INTO t PARTITION (p0) VALUES (4, 'candidate') \
                 ON DUPLICATE KEY UPDATE a = a + 1, b = VALUES(b)",
            )
            .unwrap(),
        StmtResult::Affected(0),
        "IGNORE turns the partition update failure into a skipped row"
    );
    assert_eq!(
        tests_support::row_text(session.run("SHOW WARNINGS")).as_slice(),
        [[
            "Warning",
            "1748",
            "Found a row not matching the given partition set"
        ]]
    );
    assert_eq!(
        session
            .run(
                "INSERT INTO t PARTITION (p0) VALUES (4, 'updated') \
                 ON DUPLICATE KEY UPDATE b = VALUES(b)",
            )
            .unwrap(),
        StmtResult::Affected(2),
        "an update that remains in p0 is still accepted"
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT a, b FROM t PARTITION (p0)")),
        vec![vec!["4".to_owned(), "updated".to_owned()]]
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

/// Go `checkPartitionModifiableColumn` allows every non-partitioning column
/// to be modified: the DDL rewrites each record in its existing physical
/// partition and rebuilds that partition's local indexes. The partition
/// method does not change this rule.
#[test]
fn modifying_a_non_partition_column_preserves_physical_ownership() {
    let cases = [
        (
            "mh",
            "CREATE TABLE mh (a int, b int, KEY idx_b(b)) \
             PARTITION BY HASH(a) PARTITIONS 4",
        ),
        (
            "mr",
            "CREATE TABLE mr (a int, b int, KEY idx_b(b)) \
             PARTITION BY RANGE(a) (PARTITION pn VALUES LESS THAN (0), \
             PARTITION p0 VALUES LESS THAN (10), PARTITION pm VALUES LESS THAN (MAXVALUE))",
        ),
        (
            "ml",
            "CREATE TABLE ml (a int, b int, KEY idx_b(b)) \
             PARTITION BY LIST COLUMNS(a) (PARTITION p0 VALUES IN ((0)), \
             PARTITION p1 VALUES IN ((1)), PARTITION pd DEFAULT)",
        ),
        (
            "mk",
            "CREATE TABLE mk (a int PRIMARY KEY, b int, KEY idx_b(b)) \
             PARTITION BY KEY(a) PARTITIONS 3",
        ),
    ];

    for (table, create) in cases {
        let mut session = Session::new();
        session.run(create).unwrap();
        session
            .run(&format!(
                "INSERT INTO {table} VALUES (-1,11),(0,12),(1,13),(11,14)"
            ))
            .unwrap();
        let before = partition_counts(&mut session, table);

        session
            .run(&format!(
                "ALTER TABLE {table} MODIFY COLUMN b BIGINT UNSIGNED"
            ))
            .expect("a non-partitioning column is independently modifiable");

        assert_eq!(
            partition_counts(&mut session, table),
            before,
            "MODIFY must retain every row's physical partition for {table}"
        );
        assert_eq!(
            tests_support::row_text(session.run(&format!("SELECT a,b FROM {table} ORDER BY a"))),
            [
                ["-1".to_owned(), "11".to_owned()],
                ["0".to_owned(), "12".to_owned()],
                ["1".to_owned(), "13".to_owned()],
                ["11".to_owned(), "14".to_owned()],
            ],
            "converted rows remain visible for {table}"
        );
        assert_eq!(
            tests_support::row_text(session.run(&format!(
                "SELECT a FROM {table} FORCE INDEX(idx_b) WHERE b=13"
            ))),
            [["1".to_owned()]],
            "the local index is rebuilt under the same physical id for {table}"
        );
    }
}

#[test]
fn modifying_a_partition_column_keeps_the_source_safety_boundary() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE mp (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session.run("INSERT INTO mp VALUES (1,10)").unwrap();

    session
        .run("ALTER TABLE mp MODIFY COLUMN a INT DEFAULT 7")
        .expect("a default-only change cannot alter partition routing");

    let error = session
        .run("ALTER TABLE mp MODIFY COLUMN a VARCHAR(20)")
        .expect_err("a routing type change needs partition reorganization")
        .to_mysql_error();
    assert_eq!(error.code, 8200);
    assert_eq!(
        error.message,
        "Unsupported modify column: can't change the partitioning column, since it would require reorganize all partitions"
    );

    let error = session
        .run("ALTER TABLE mp CHANGE COLUMN a renamed INT")
        .expect_err("the partition expression still names a")
        .to_mysql_error();
    assert_eq!(error.code, 3855);
    assert_eq!(
        error.message,
        "Column 'a' has a partitioning function dependency and cannot be dropped or renamed"
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT a,b FROM mp")),
        [["1".to_owned(), "10".to_owned()]]
    );
}

#[test]
fn partition_column_modify_uses_the_source_method_allowlist() {
    let cases = [
        (
            "ak",
            "CREATE TABLE ak (a tinyint, b int) PARTITION BY KEY(a) PARTITIONS 3",
            "ALTER TABLE ak MODIFY COLUMN a INT",
            "INSERT INTO ak VALUES (1000,1)",
        ),
        (
            "aks",
            "CREATE TABLE aks (a varchar(4), b int) PARTITION BY KEY(a) PARTITIONS 3",
            "ALTER TABLE aks MODIFY COLUMN a VARCHAR(16)",
            "INSERT INTO aks VALUES ('extended',1)",
        ),
        (
            "ake",
            "CREATE TABLE ake (a enum('x','y'), b int) PARTITION BY KEY(a) PARTITIONS 3",
            "ALTER TABLE ake MODIFY COLUMN a ENUM('x','y','z')",
            "INSERT INTO ake VALUES ('z',1)",
        ),
        (
            "akset",
            "CREATE TABLE akset (a set('x','y'), b int) PARTITION BY KEY(a) PARTITIONS 3",
            "ALTER TABLE akset MODIFY COLUMN a SET('x','y','z')",
            "INSERT INTO akset VALUES ('z',1)",
        ),
        (
            "arc",
            "CREATE TABLE arc (a tinyint, b int) PARTITION BY RANGE COLUMNS(a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION pm VALUES LESS THAN (MAXVALUE))",
            "ALTER TABLE arc MODIFY COLUMN a INT",
            "INSERT INTO arc VALUES (1000,1)",
        ),
        (
            "arcd",
            "CREATE TABLE arcd (a datetime, b int) PARTITION BY RANGE COLUMNS(a) (\
             PARTITION p0 VALUES LESS THAN ('2024-06-01'), \
             PARTITION pm VALUES LESS THAN (MAXVALUE))",
            "ALTER TABLE arcd MODIFY COLUMN a DATETIME(3)",
            "INSERT INTO arcd VALUES ('2024-07-01 00:00:00.123',1)",
        ),
        (
            "ah",
            "CREATE TABLE ah (a tinyint, b int) PARTITION BY HASH(a) PARTITIONS 4",
            "ALTER TABLE ah MODIFY COLUMN a INT",
            "INSERT INTO ah VALUES (1000,1)",
        ),
        (
            "atd",
            "CREATE TABLE atd (a datetime, b int) PARTITION BY RANGE (TO_DAYS(a)) (\
             PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-06-01')), \
             PARTITION pm VALUES LESS THAN (MAXVALUE))",
            "ALTER TABLE atd MODIFY COLUMN a DATETIME(3)",
            "INSERT INTO atd VALUES ('2024-07-01 00:00:00.123',1)",
        ),
        (
            "aex",
            "CREATE TABLE aex (a time, b int) PARTITION BY RANGE (EXTRACT(SECOND FROM a)) (\
             PARTITION p0 VALUES LESS THAN (30), PARTITION pm VALUES LESS THAN (MAXVALUE))",
            "ALTER TABLE aex MODIFY COLUMN a TIME(3)",
            "INSERT INTO aex VALUES ('00:00:45.123',1)",
        ),
    ];

    for (table, create, alter, insert) in cases {
        let mut session = Session::new();
        session
            .run(create)
            .unwrap_or_else(|error| panic!("create {table}: {error:?}"));
        session
            .run(alter)
            .unwrap_or_else(|error| panic!("alter {table}: {error:?}"));
        session
            .run(insert)
            .unwrap_or_else(|error| panic!("insert {table}: {error:?}"));
        assert_eq!(
            tests_support::row_text(session.run(&format!("SELECT count(*) FROM {table}"))),
            [["1".to_owned()]],
            "accepted partition-column extension remains routable for {table}"
        );
    }

    let mut session = Session::new();
    session
        .run("CREATE TABLE an (a int NULL, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    let error = session
        .run("ALTER TABLE an MODIFY COLUMN a INT NOT NULL")
        .expect_err("NULL to NOT NULL is outside the source allowlist")
        .to_mysql_error();
    assert_eq!(error.code, 8200);

    session
        .run("CREATE TABLE ae (a enum('x','y'), b int) PARTITION BY KEY(a) PARTITIONS 3")
        .unwrap();
    let error = session
        .run("ALTER TABLE ae MODIFY COLUMN a ENUM('y','x','z')")
        .expect_err("existing ENUM ordinals cannot move")
        .to_mysql_error();
    assert_eq!(error.code, 8200);

    session
        .run(
            "CREATE TABLE af (a datetime, b int) PARTITION BY RANGE (FLOOR(TO_DAYS(a))) (\
             PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-06-01')), \
             PARTITION pm VALUES LESS THAN (MAXVALUE))",
        )
        .unwrap();
    let error = session
        .run("ALTER TABLE af MODIFY COLUMN a DATETIME(3)")
        .expect_err("an unsupported function on the column path blocks FSP widening")
        .to_mysql_error();
    assert_eq!(error.code, 8200);
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
    let repeated =
        crate::tests_support::row_text(session.run("EXPLAIN SELECT * FROM t WHERE a IN (1, 2, 1)"));
    let batch = repeated
        .iter()
        .find(|row| row[0].contains("Batch_Point_Get"))
        .unwrap_or_else(|| panic!("the partitioned IN remains a batch point get: {repeated:?}"));
    assert_eq!(batch[1], "3.00", "the estimate precedes handle dedup");
    // Handles 1, 2, 3 land in p1, P2, P0 -- so the ASCENDING handle order and
    // the DEFINITION order disagree, and Go sorts the ORDINALS. Without that
    // sort this reads `p1,P2,P0`.
    assert_eq!(
        plan_access_objects(&mut session, "EXPLAIN SELECT * FROM t WHERE a IN (1, 2, 3)"),
        vec!["table:t, partition:P0,p1,P2".to_owned()]
    );
    let mut rows =
        crate::tests_support::row_text(session.run("SELECT a,b FROM t WHERE a IN (1,2)"));
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "2".to_owned()]
        ]
    );
    assert!(crate::tests_support::row_text(
        session.run("SELECT * FROM t PARTITION (P0) WHERE a IN (1,2)")
    )
    .is_empty());
    let excluded = crate::tests_support::row_text(
        session.run("EXPLAIN SELECT * FROM t PARTITION (P0) WHERE a IN (1,2)"),
    );
    assert!(
        excluded.iter().any(|row| row[0].contains("TableDual")),
        "Go prunes an all-excluded handle list to TableDual: {excluded:?}"
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

/// Go `CheckDropTablePartition` plus the LIST metadata transition: dropping
/// a partition removes both its definition and its physical rows; adding it
/// back creates a fresh empty physical partition whose values route once.
/// LIST and LIST COLUMNS share that lifecycle even though their value maps
/// have different representations.
#[test]
fn list_partition_drop_and_add_replace_metadata_and_physical_rows() {
    for columns in [false, true] {
        let mut session = Session::new();
        let method = if columns {
            "LIST COLUMNS(a)"
        } else {
            "LIST(a)"
        };
        session
            .run(&format!(
                "CREATE TABLE t (a INT) PARTITION BY {method} (\
                 PARTITION p0 VALUES IN (0,1,2,3,4),\
                 PARTITION p1 VALUES IN (5,6,7,8,9),\
                 PARTITION p2 VALUES IN (10,11,12,13,14),\
                 PARTITION p3 VALUES IN (15,16,17,18,19))"
            ))
            .unwrap();
        session
            .run("INSERT INTO t VALUES (0),(5),(10),(15)")
            .unwrap();

        session
            .run("ALTER TABLE t DROP PARTITION IF EXISTS missing")
            .unwrap();
        assert_eq!(
            crate::tests_support::row_text(session.run("SHOW WARNINGS")).as_slice(),
            [["Note", "1507", "Error in list of partitions to DROP"]]
        );

        session.run("ALTER TABLE t DROP PARTITION p0").unwrap();
        assert_eq!(
            crate::tests_support::row_text(session.run("SELECT * FROM t ORDER BY a")).as_slice(),
            [["5"], ["10"], ["15"]]
        );
        assert_eq!(
            session
                .run("SELECT * FROM t PARTITION (p0)")
                .unwrap_err()
                .to_mysql_error()
                .code,
            1735
        );

        session.run("ALTER TABLE t DROP PARTITION p1,p2").unwrap();
        assert_eq!(
            crate::tests_support::row_text(session.run("SELECT * FROM t ORDER BY a")).as_slice(),
            [["15"]]
        );
        assert_eq!(
            session
                .run("ALTER TABLE t DROP PARTITION p3")
                .unwrap_err()
                .to_mysql_error()
                .code,
            1508
        );

        session
            .run("ALTER TABLE t ADD PARTITION (PARTITION p0 VALUES IN (0,1,2,3,4))")
            .unwrap();
        session
            .run(
                "ALTER TABLE t ADD PARTITION IF NOT EXISTS \
                 (PARTITION p0 VALUES IN (0,1,2,3,4))",
            )
            .unwrap();
        assert_eq!(
            crate::tests_support::row_text(session.run("SHOW WARNINGS")).as_slice(),
            [["Note", "1517", "Duplicate partition name p0"]]
        );
        session
            .run(
                "ALTER TABLE t ADD PARTITION (\
                 PARTITION p1 VALUES IN (5,6,7,8,9),\
                 PARTITION p2 VALUES IN (10,11,12,13,14))",
            )
            .unwrap();
        session.run("INSERT INTO t VALUES (0),(5),(10)").unwrap();
        assert_eq!(
            crate::tests_support::row_text(session.run("SELECT * FROM t ORDER BY a")).as_slice(),
            [["0"], ["5"], ["10"], ["15"]]
        );
    }
}

#[test]
fn range_columns_rejects_null_values_less_than_bounds() {
    let mut session = Session::new();
    let error = session
        .run(
            "CREATE TABLE t (a INT, b DATETIME, c VARCHAR(255)) \
             PARTITION BY RANGE COLUMNS(a,b,c) (\
             PARTITION p0 VALUES LESS THAN (NULL,NULL,NULL))",
        )
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1566);
    assert_eq!(
        error.message,
        "Not allowed to use NULL value in VALUES LESS THAN"
    );
}

#[test]
fn range_columns_filter_uses_the_partition_columns_collation() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a CHAR(32) COLLATE utf8mb4_unicode_ci) \
             PARTITION BY RANGE COLUMNS(a) (\
             PARTITION p0 VALUES LESS THAN ('c'),\
             PARTITION p1 VALUES LESS THAN ('F'),\
             PARTITION p2 VALUES LESS THAN ('h'),\
             PARTITION p3 VALUES LESS THAN ('L'),\
             PARTITION p4 VALUES LESS THAN ('t'),\
             PARTITION p5 VALUES LESS THAN (MAXVALUE))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO t VALUES \
             ('a'),('A'),('c'),('C'),('f'),('F'),('h'),('H'),\
             ('l'),('L'),('t'),('T'),('z'),('Z')",
        )
        .unwrap();
    session
        .run("CREATE TABLE u (a CHAR(32) COLLATE utf8mb4_unicode_ci)")
        .unwrap();
    session
        .run(
            "INSERT INTO u VALUES \
             ('a'),('A'),('c'),('C'),('f'),('F'),('h'),('H'),\
             ('l'),('L'),('t'),('T'),('z'),('Z')",
        )
        .unwrap();
    let unpartitioned = crate::tests_support::row_text(
        session.run("SELECT * FROM u WHERE a > 'c' AND a < 'Q' ORDER BY a"),
    );
    assert_eq!(
        unpartitioned.as_slice(),
        [["f"], ["F"], ["h"], ["H"], ["l"], ["L"]]
    );

    let rows = crate::tests_support::row_text(
        session.run("SELECT * FROM t WHERE a > 'c' AND a < 'Q' ORDER BY a"),
    );
    assert_eq!(rows.as_slice(), [["f"], ["F"], ["h"], ["H"], ["l"], ["L"]]);
}

/// Go `AppendPartitionInfo` prints the compact `PARTITIONS n` form for HASH
/// and KEY only when every partition is still the one it would have
/// generated: named `p<i>`, with no comment and no placement ref
/// (`ddl/partition.go:5147-5171`). Anything else prints the definition list,
/// because the compact form cannot express it.
///
/// Printing `PARTITIONS n` unconditionally meant `SHOW CREATE TABLE` emitted
/// DDL that would build a DIFFERENT table from the one it described: the
/// written names and any comments were simply gone.
#[test]
fn hash_and_key_print_the_definition_list_when_it_is_not_the_default_one() {
    let mut session = Session::new();
    // Default names, no comments: the compact form still applies.
    session
        .run("CREATE TABLE h0 (a int) PARTITION BY HASH(a) PARTITIONS 2")
        .expect("HASH is accepted");
    assert!(
        show_create(&mut session, "h0").contains("PARTITION BY HASH (`a`) PARTITIONS 2"),
        "a default-shaped HASH table keeps the compact clause: {}",
        show_create(&mut session, "h0")
    );

    // Explicitly NAMED partitions cannot be expressed by `PARTITIONS n`.
    session
        .run("CREATE TABLE h1 (a int) PARTITION BY HASH(a) (PARTITION west, PARTITION east)")
        .expect("named HASH partitions are accepted");
    let named = show_create(&mut session, "h1");
    assert!(
        named.contains("PARTITION `west`") && named.contains("PARTITION `east`"),
        "the written names must survive SHOW CREATE: {named}"
    );
    assert!(
        !named.contains("PARTITIONS 2"),
        "the compact form cannot carry those names: {named}"
    );

    // A COMMENT likewise forces the definition list, and is printed on it.
    session
        .run("CREATE TABLE h2 (a int) PARTITION BY HASH(a) (PARTITION p0 COMMENT 'first', PARTITION p1)")
        .expect("a commented HASH partition is accepted");
    let commented = show_create(&mut session, "h2");
    assert!(
        commented.contains("PARTITION `p0` COMMENT 'first'"),
        "the comment must survive SHOW CREATE: {commented}"
    );
}

/// Go's `writeColumnListToBuffer` emits NOTHING when the KEY column list was
/// filled in from the primary key (`ddl/partition.go:5125`), so the clause
/// reads back as written and re-creating the table resolves the key again
/// rather than pinning today's columns.
#[test]
fn an_empty_key_clause_prints_back_empty() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE k1 (id int NOT NULL PRIMARY KEY, v int) PARTITION BY KEY() PARTITIONS 2")
        .expect("PARTITION BY KEY() is accepted");
    let created = show_create(&mut session, "k1");
    assert!(
        created.contains("PARTITION BY KEY () PARTITIONS 2"),
        "an empty KEY clause prints back empty: {created}"
    );
}

/// `SHOW CREATE TABLE` prints a RANGE partition from the STORED bound text,
/// as Go's `AppendPartitionDefs` does (`ddl/partition.go:5204`) -- including
/// for a partition added by `ALTER`, which records the same text a `CREATE`
/// would.
///
/// Both renderers now read stored text. Leaving RANGE on the folded bounds
/// while LIST read stored text meant two sources for one question, which is
/// how a partition added by `ALTER` came to print as a bare `DEFAULT` on the
/// LIST side before it was populated.
#[test]
fn an_added_range_partition_prints_the_bound_it_was_given() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE ar (a int) PARTITION BY RANGE(a) \
             (PARTITION p0 VALUES LESS THAN (10))",
        )
        .expect("a RANGE table is accepted");
    session
        .run("ALTER TABLE ar ADD PARTITION (PARTITION p1 VALUES LESS THAN (20))")
        .expect("the partition is added");
    let created = show_create(&mut session, "ar");
    assert!(
        created.contains("PARTITION `p1` VALUES LESS THAN (20)"),
        "the added bound must survive SHOW CREATE: {created}"
    );
    assert!(
        created.contains("PARTITION `p0` VALUES LESS THAN (10)"),
        "and the original bound is unchanged: {created}"
    );
}

/// Every divergence an independent re-reading of Go turned up in the ported
/// partition rules, as the statement that separates the two engines.
///
/// The existing suite passed on all of these: each one is a case where this
/// node refused what Go accepts, accepted what Go refuses, or reported a
/// different error for the same mistake. They are grouped by the Go function
/// whose behaviour they pin, and every expectation here was read out of Go's
/// source rather than out of this implementation.
///
/// A `None` errno means Go ACCEPTS the statement.
const GO_DIVERGENCES: &[(&str, Option<u16>, &str)] = &[
    // -- `checkPartitionExprArgs`, the `case ast.Extract:` arm (partition.go:5000).
    // The parser gives EXTRACT its own node, so the whole rule set was skipped.
    (
        "CREATE TABLE d1 (a int) PARTITION BY HASH (extract(year from a)) PARTITIONS 4",
        Some(1486),
        "hasDateArgs over an INT column is false",
    ),
    (
        "CREATE TABLE d2 (d date) PARTITION BY HASH (extract(day_hour from d)) PARTITIONS 4",
        Some(1486),
        "the DAY_HOUR family needs hasDatetimeArgs -- a DATE does not satisfy it",
    ),
    (
        "CREATE TABLE d3 (d date) PARTITION BY HASH (extract(week from d)) PARTITIONS 4",
        Some(1486),
        "WEEK reaches Go's unconditional `default:` refusal",
    ),
    (
        "CREATE TABLE d4 (d datetime) PARTITION BY HASH (extract(day_hour from d)) PARTITIONS 4",
        None,
        "a DATETIME does satisfy hasDatetimeArgs",
    ),
    // -- `checkNoTimestampArgs` on the whitelisted operators (partition.go:4968).
    (
        "CREATE TABLE d5 (ts timestamp) PARTITION BY HASH (ts + 0) PARTITIONS 2",
        Some(1486),
        "a TIMESTAMP operand routes by the session time zone",
    ),
    (
        "CREATE TABLE d6 (ts timestamp) PARTITION BY RANGE (-ts) (PARTITION p0 VALUES LESS THAN (0))",
        Some(1486),
        "the unary arm runs checkNoTimestampArgs too -- and this was 1491",
    ),
    // -- the allowed-leaf list (partition.go:4975): NULL is a `*driver.ValueExpr`.
    (
        "CREATE TABLE d7 (a int) PARTITION BY HASH (-null) PARTITIONS 2",
        Some(1486),
        "the whitelist admits NULL, so the no-column rule is what refuses it",
    ),
    // -- `collectArgsType` / `extractColumns` quote the FOLDED name.
    (
        "CREATE TABLE d8 (d date) PARTITION BY HASH (year(NoSuch)) PARTITIONS 2",
        Some(1054),
        "Go quotes col.Name.Name.L, so the message says 'nosuch'",
    ),
    // -- `checkColumnsPartitionType` (partition.go:787) is 1488, not 1054.
    (
        "CREATE TABLE d9 (a int) PARTITION BY KEY (nosuch) PARTITIONS 2",
        Some(1488),
        "a column LIST reports ErrFieldNotFoundPart",
    ),
    (
        "CREATE TABLE d10 (a int) PARTITION BY RANGE COLUMNS (b) (PARTITION p0 VALUES LESS THAN (1))",
        Some(1488),
        "same rule on RANGE COLUMNS",
    ),
    (
        "CREATE TABLE d11 (a int) PARTITION BY LIST COLUMNS (b) (PARTITION p0 VALUES IN (1))",
        Some(1488),
        "same rule on LIST COLUMNS",
    ),
    // -- `isValidKeyPartitionColType` (partition.go:798) omits TypeTinyBlob.
    (
        "CREATE TABLE d12 (a tinytext) PARTITION BY KEY (a) PARTITIONS 2",
        None,
        "Go's reject list has BLOB/MEDIUMBLOB/LONGBLOB but NOT TINYBLOB",
    ),
    // -- Go's phase order: buildTablePartitionInfo, then
    //    checkPartitionDefinitionConstraints (1517, 1499, 1652), then
    //    checkPartitioningKeysConstraints (1105).
    (
        "CREATE TABLE d13 (a int, b int, KEY k(b)) PARTITION BY KEY () PARTITIONS 10000",
        Some(1499),
        "the HASH/KEY cap is checked inside the definition builder, long before 1105",
    ),
    (
        "CREATE TABLE d14 (a int, b int, KEY k(b)) PARTITION BY KEY () (PARTITION p0, PARTITION p0)",
        Some(1517),
        "name-uniqueness precedes the 1105 as well",
    ),
    (
        "CREATE TABLE d15 (a int, b int) PARTITION BY KEY (a, a) PARTITIONS 10000",
        Some(1499),
        "the cap precedes checkPartitionColumnsUnique's 1652",
    ),
    (
        "CREATE TABLE d16 (a int) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p0 VALUES LESS THAN (5))",
        Some(1517),
        "1517 precedes checkPartitionByRange's 1493",
    ),
    (
        "CREATE TABLE d17 (a int) PARTITION BY LIST (a) \
         (PARTITION p0 VALUES IN (1), PARTITION p0 VALUES IN (1))",
        Some(1517),
        "1517 precedes checkPartitionByList's 1495",
    ),
];

#[test]
fn the_rules_read_back_out_of_go_agree_statement_by_statement() {
    for (sql, expected, why) in GO_DIVERGENCES {
        let mut session = Session::new();
        match (session.run(sql), expected) {
            (Ok(_), None) => {}
            (Ok(_), Some(errno)) => {
                panic!("{sql}\n  expected {errno} ({why}), but the statement was ACCEPTED")
            }
            (Err(error), None) => {
                let rendered = error.to_mysql_error();
                panic!(
                    "{sql}\n  expected acceptance ({why}), but got {} {}",
                    rendered.code, rendered.message
                )
            }
            (Err(error), Some(errno)) => {
                let rendered = error.to_mysql_error();
                assert_eq!(rendered.code, *errno, "{sql}\n  {why}\n  got: {}", rendered.message);
            }
        }
    }
}

/// `SHOW CREATE TABLE` prints what Go prints, for the definition shapes that
/// re-deriving the values instead of reading the stored text got wrong.
///
/// Each of these is a case where the output was not merely differently
/// spelled but WRONG: a name that no longer re-parses, values silently
/// dropped, a comment that broke the statement across lines. Go builds all of
/// them in one function, `AppendPartitionDefs`, from `def.LessThan` and
/// `def.InValues` -- so this asserts on the whole tail, not on a fragment.
#[test]
fn show_create_prints_the_stored_definition_text() {
    let mut session = Session::new();

    // Go `stringutil.Escape`: the quote character DOUBLES, or the dump does
    // not re-parse.
    session
        .run("CREATE TABLE bt (a int) PARTITION BY RANGE(a) (PARTITION `p``0` VALUES LESS THAN (10))")
        .unwrap();
    assert!(
        tests_support::show_create(&mut session, "bt").ends_with(
            "PARTITION BY RANGE (`a`)\n(PARTITION `p``0` VALUES LESS THAN (10))"
        ),
        "{}",
        tests_support::show_create(&mut session, "bt")
    );

    // A LIST COLUMNS partition that carries real values AND `DEFAULT` keeps
    // the values: only an EMPTY `InValues`, or the single literal word, is
    // Go's bare-DEFAULT marker (`ddl/partition.go:5209`).
    session
        .run(
            "CREATE TABLE lcd (a int) PARTITION BY LIST COLUMNS(a) \
             (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (2,3,DEFAULT))",
        )
        .unwrap();
    let rendered = tests_support::show_create(&mut session, "lcd");
    assert!(
        rendered.contains("PARTITION `p1` VALUES IN (2,3,DEFAULT)"),
        "the real values were dropped: {rendered}"
    );

    // Go `format.OutputFormat` escapes NUL, LF and CR -- and does NOT touch a
    // backslash. An unescaped newline broke the statement in half.
    session
        .run(
            "CREATE TABLE cmt (a int) PARTITION BY RANGE(a) \
             (PARTITION p0 VALUES LESS THAN (10) COMMENT 'x\ny')",
        )
        .unwrap();
    let rendered = tests_support::show_create(&mut session, "cmt");
    assert!(
        rendered.contains(r"COMMENT 'x\ny'"),
        "the newline was not escaped: {rendered:?}"
    );
}

/// Go `checkAddPartitionValue` (`ddl/partition.go:428`) validates EVERY added
/// definition, not merely the first one against the table's last bound.
///
/// These four statements are the arms of Go's loop. They are asserted at the
/// session level because the rule lives on the `ALTER` path, which reads the
/// table's existing bounds -- a unit test over the bound vector alone cannot
/// reach it.
#[test]
fn add_partition_validates_every_added_definition_as_go_does() {
    let base = "CREATE TABLE ap (a INT) PARTITION BY RANGE (a) \
                (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))";

    // Two added definitions that do not increase BETWEEN THEMSELVES. Go walks
    // to the second and answers 1493; comparing only the first against the
    // table's last bound accepted this.
    let mut session = Session::new();
    session.run(base).expect("base table");
    let rendered = session
        .run("ALTER TABLE ap ADD PARTITION (PARTITION p2 VALUES LESS THAN (30), \
              PARTITION p3 VALUES LESS THAN (25))")
        .expect_err("Go refuses a non-increasing pair")
        .to_mysql_error();
    assert_eq!(rendered.code, 1493);

    // A MAXVALUE that is not the last added definition is 1481.
    let mut session = Session::new();
    session.run(base).expect("base table");
    let rendered = session
        .run("ALTER TABLE ap ADD PARTITION (PARTITION p2 VALUES LESS THAN (MAXVALUE), \
              PARTITION p3 VALUES LESS THAN (40))")
        .expect_err("Go refuses MAXVALUE before the last definition")
        .to_mysql_error();
    assert_eq!(rendered.code, 1481);

    // A MAXVALUE that IS last ends Go's loop successfully.
    let mut session = Session::new();
    session.run(base).expect("base table");
    session
        .run("ALTER TABLE ap ADD PARTITION (PARTITION p2 VALUES LESS THAN (30), \
              PARTITION p3 VALUES LESS THAN (MAXVALUE))")
        .expect("a trailing MAXVALUE is accepted");

    // The table's own last bound being MAXVALUE is refused before any added
    // definition is read.
    let mut session = Session::new();
    session
        .run("CREATE TABLE apm (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10), PARTITION pm VALUES LESS THAN (MAXVALUE))")
        .expect("base table");
    let rendered = session
        .run("ALTER TABLE apm ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))")
        .expect_err("nothing can follow MAXVALUE")
        .to_mysql_error();
    assert_eq!(rendered.code, 1481);
}

/// Go validates an addition by CONCATENATING it onto the existing
/// definitions and running the whole CREATE-time battery over the result
/// (`CheckAndUpdateAddedPartitionDefinitions`, `ddl/executor.go`).
///
/// RANGE COLUMNS reaches that battery and nothing else, because
/// `checkAddPartitionValue`'s own increase loop runs only for the scalar
/// form (`len(meta.Partition.Columns) == 0`). So a pair of added tuples that
/// does not increase among ITSELF has to be caught by the combined check.
#[test]
fn add_partition_on_range_columns_checks_the_combined_list() {
    let base = "CREATE TABLE apc (a INT, b INT) PARTITION BY RANGE COLUMNS (a, b) \
                (PARTITION p0 VALUES LESS THAN (10, 10), \
                 PARTITION p1 VALUES LESS THAN (20, 20))";

    // The first added tuple increases against the table's last bound, but the
    // SECOND does not increase against the first.
    let mut session = Session::new();
    session.run(base).expect("base table");
    let rendered = session
        .run("ALTER TABLE apc ADD PARTITION (PARTITION p2 VALUES LESS THAN (30, 30), \
              PARTITION p3 VALUES LESS THAN (25, 25))")
        .expect_err("Go refuses a non-increasing added pair")
        .to_mysql_error();
    assert_eq!(rendered.code, 1493);

    // A properly increasing addition still lands.
    let mut session = Session::new();
    session.run(base).expect("base table");
    session
        .run("ALTER TABLE apc ADD PARTITION (PARTITION p2 VALUES LESS THAN (30, 30), \
              PARTITION p3 VALUES LESS THAN (40, 40))")
        .expect("an increasing addition is accepted");
}

/// Go's 1735 for an unknown partition carries a DIFFERENT case depending on
/// which statement raised it.
///
/// `TruncateTablePartition` passes `name.L` (`ddl/executor.go:2851`), so the
/// written case is folded away, while the SELECT/DML partition-list errors
/// pass `.O` and keep it (`executor/builder.go:6258`). Both spellings are
/// deliberate on Go's side, so a single rule here would be wrong for one of
/// them.
#[test]
fn an_unknown_partition_name_carries_gos_case_per_statement() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE up (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10))")
        .expect("base table");

    // TRUNCATE folds the name.
    let rendered = session
        .run("ALTER TABLE up TRUNCATE PARTITION NoSuch")
        .expect_err("no such partition")
        .to_mysql_error();
    assert_eq!(rendered.code, 1735);
    assert!(
        rendered.message.contains("'nosuch'"),
        "TRUNCATE folds the name as Go's `name.L` does, got: {}",
        rendered.message
    );

    // The SELECT partition list keeps the written case.
    let rendered = session
        .run("SELECT * FROM up PARTITION (NoSuch)")
        .expect_err("no such partition")
        .to_mysql_error();
    assert_eq!(rendered.code, 1735);
    assert!(
        rendered.message.contains("'NoSuch'"),
        "SELECT keeps the written case as Go's `.O` does, got: {}",
        rendered.message
    );
}

/// The TEXT a partition expression is stored as, which `CREATE TABLE` writes,
/// the loader parses back, and `SHOW CREATE TABLE` prints.
///
/// Go restores it under `DefaultRestoreFlags | RestoreBracketAroundBinaryOperation
/// | RestoreWithoutSchemaName | RestoreWithoutTableName` (`ddl/partition.go:619`).
///
/// The spacing rule is `restoreBinaryOpWithSpacesAround`
/// (`parser/ast/expressions.go`): a space goes around the operator when the
/// `RestoreSpacesAroundBinaryOperation` flag is set -- it is NOT, here -- or
/// when the operator is a KEYWORD. Of the operators partitioning admits
/// (`AllowedPartition4BinaryOpMap`: `+ - * DIV %`), only `DIV` is a keyword
/// (`parser/opcode/opcode.go`, `IntDiv.isKeyword: true`), so `DIV` is spaced
/// and the arithmetic symbols are not.
#[test]
fn a_partition_expression_is_stored_the_way_go_spells_it() {
    let cases: &[(&str, &str)] = &[
        // A bare column is back-quoted.
        ("CREATE TABLE e1 (a INT) PARTITION BY HASH (a) PARTITIONS 2", "HASH (`a`)"),
        // A binary operation is BRACKETED by the partition flags, and a
        // symbol operator carries no spaces.
        (
            "CREATE TABLE e2 (col1 INT, col3 BIGINT) PARTITION BY HASH (col1 * col3) PARTITIONS 2",
            "HASH ((`col1`*`col3`))",
        ),
        (
            "CREATE TABLE e6 (a INT, b INT) PARTITION BY HASH (a + b) PARTITIONS 2",
            "HASH ((`a`+`b`))",
        ),
        // ... but `DIV` is a keyword operator, so it IS spaced.
        (
            "CREATE TABLE e7 (a INT, b INT) PARTITION BY HASH (a DIV b) PARTITIONS 2",
            "HASH ((`a` DIV `b`))",
        ),
        // A function name is upper-cased, its column argument back-quoted.
        (
            "CREATE TABLE e3 (dob DATE) PARTITION BY RANGE (year(dob)) \
             (PARTITION p0 VALUES LESS THAN (2000))",
            "RANGE (YEAR(`dob`))",
        ),
        (
            "CREATE TABLE e4 (a INT) PARTITION BY LIST (abs(a)) (PARTITION p0 VALUES IN (1))",
            "LIST (ABS(`a`))",
        ),
        (
            "CREATE TABLE e5 (end_time DATETIME) PARTITION BY RANGE (month(end_time)) \
             (PARTITION p0 VALUES LESS THAN (6))",
            "RANGE (MONTH(`end_time`))",
        ),
    ];
    for (create, expected) in cases {
        let mut session = Session::new();
        session.run(create).unwrap_or_else(|error| panic!("{create}: {error:?}"));
        let name = create.split_whitespace().nth(2).expect("CREATE TABLE <name>");
        let shown = show_create(&mut session, name);
        assert!(
            shown.contains(expected),
            "{create}\n  expected to contain: {expected}\n  got: {shown}"
        );
    }
}

/// `RANGE ... INTERVAL (...)` GENERATES partition definitions from a step.
/// This node does not expand them, so it must refuse the clause: accepting it
/// and ignoring the INTERVAL would build a table with DIFFERENT partitions
/// from the ones the statement asked for, which is worse than not serving it.
///
/// The refusal has to cover the COLUMNS spelling too. Go's
/// `generatePartitionDefinitionsFromInterval` handles both, and the check
/// here sits after the COLUMNS arms have already returned.
#[test]
fn interval_partitioning_is_refused_on_every_spelling() {
    let statements = [
        "CREATE TABLE i1 (a INT) PARTITION BY RANGE (a) \
         INTERVAL (10) FIRST PARTITION LESS THAN (0) LAST PARTITION LESS THAN (100)",
        "CREATE TABLE i2 (a DATE) PARTITION BY RANGE COLUMNS (a) \
         INTERVAL (1 MONTH) FIRST PARTITION LESS THAN ('2020-01-01') \
         LAST PARTITION LESS THAN ('2020-06-01')",
    ];
    for sql in statements {
        let mut session = Session::new();
        let rendered = session
            .run(sql)
            .expect_err("INTERVAL must be refused, not silently ignored")
            .to_mysql_error();
        assert!(
            rendered.message.contains("INTERVAL"),
            "the refusal must name INTERVAL, so it cannot be turned into an \
             acceptance by a change elsewhere: {sql}\n  got: {}",
            rendered.message
        );
    }
}

/// A GLOBAL unique index spans every partition, so its uniqueness is
/// cluster-wide. A LOCAL unique index on a partitioned table enforces
/// uniqueness only WITHIN each partition, so building a GLOBAL one as local
/// would admit duplicates that live in different partitions -- no error, and
/// a unique constraint that is not a constraint.
///
/// This tier maintains only per-partition index entries
/// (`kv_table/index_entries.rs` fixes `global: false`), so it must REFUSE
/// the clause. The assertion is on the refusal naming GLOBAL, not merely on
/// a refusal happening: an incidental error from somewhere else would leave
/// the door open for a later change to turn it into a silent acceptance.
#[test]
fn a_global_unique_index_is_refused_rather_than_built_local() {
    let mut session = Session::new();
    // `b` is not a partitioning column, so uniqueness on it cannot be
    // enforced per-partition: this index has to be GLOBAL to mean anything.
    let rendered = session
        .run(
            "CREATE TABLE g1 (a INT, b INT, UNIQUE KEY ub(b) GLOBAL) \
             PARTITION BY HASH(a) PARTITIONS 2",
        )
        .expect_err("a GLOBAL index cannot be served by per-partition entries")
        .to_mysql_error();
    assert!(
        rendered.message.contains("GLOBAL"),
        "the refusal must name GLOBAL so it cannot decay into a silent \
         downgrade, got: {}",
        rendered.message
    );
}

/// Go runs partition checks in TWO phases, and which error a doubly-wrong
/// statement reports depends on that split.
///
/// Phase one is `buildTablePartitionInfo` (`ddl/partition.go:600`), whose
/// definition loop validates each definition's values, then its comment,
/// then its name, before reading the next one -- and whose
/// `buildHashPartitionDefinitions` opens with the partition-count cap
/// (`:1455`). Phase two is `checkTableInfoValidWithStmt`
/// (`ddl/create_table.go:511`) calling `checkPartitionDefinitionConstraints`
/// -- 1517, then 1499, then 1652, then the type-specific 1493/1495/1504 --
/// and only LAST `checkPartitioningKeysConstraints`, where the 1105 for
/// "the table has keys but none can serve KEY()" lives.
///
/// Each statement below is wrong in TWO ways. The errno asserted is the one
/// Go's phase order reports, read out of the Go source rather than out of
/// this implementation. These are the cases the restructure was made for, so
/// they are pinned against it drifting back.
#[test]
fn a_doubly_wrong_partition_clause_reports_gos_first_error() {
    let cases: &[(&str, u16, &str)] = &[
        // The KEY()-with-no-usable-key refusal is LAST in Go, so the
        // partition-count cap from phase one wins.
        (
            "CREATE TABLE d1 (a INT, b INT, KEY k(b)) PARTITION BY KEY () PARTITIONS 10000",
            1499,
            "phase-one partition cap beats the phase-two 1105",
        ),
        // ... and so does the duplicate partition NAME, which is the first
        // check of phase two.
        (
            "CREATE TABLE d2 (a INT, b INT, KEY k(b)) PARTITION BY KEY () \
             (PARTITION p0, PARTITION p0)",
            1517,
            "1517 is the first phase-two check, ahead of 1105",
        ),
        // The duplicate partition COLUMN (1652) comes after the cap (1499).
        (
            "CREATE TABLE d3 (a INT, b INT) PARTITION BY KEY (a, a) PARTITIONS 10000",
            1499,
            "the cap fires in phase one, 1652 only in phase two",
        ),
        // A duplicate NAME beside a non-increasing bound: 1517 precedes the
        // type-specific 1493.
        (
            "CREATE TABLE d4 (a INT) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p0 VALUES LESS THAN (5))",
            1517,
            "1517 precedes checkPartitionByRange's 1493",
        ),
        // Same, for LIST's duplicate-constant 1495.
        (
            "CREATE TABLE d5 (a INT) PARTITION BY LIST (a) \
             (PARTITION p0 VALUES IN (1), PARTITION p0 VALUES IN (1))",
            1517,
            "1517 precedes checkPartitionByList's 1495",
        ),
    ];
    for (sql, errno, why) in cases {
        let mut session = Session::new();
        let rendered = session
            .run(sql)
            .expect_err(sql)
            .to_mysql_error();
        assert_eq!(rendered.code, *errno, "{why}\n  {sql}\n  got: {}", rendered.message);
    }
}

/// Go prints HASH and KEY in the compact `PARTITIONS n` form only when every
/// definition is DEFAULT-SHAPED: named `p{i}`, carrying no comment, and
/// carrying no placement policy (`AppendPartitionInfo`, `ddl/partition.go`).
/// Anything else forces the full definition list.
///
/// The comment half of that rule only became reachable once partition
/// comments were actually persisted, so it is pinned here.
#[test]
fn a_commented_hash_partition_prints_the_full_definition_list() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE hc (a INT) PARTITION BY HASH (a) \
              (PARTITION p0 COMMENT 'first', PARTITION p1)")
        .expect("hash table with a commented partition");
    let shown = show_create(&mut session, "hc");
    assert!(
        !shown.contains("PARTITIONS 2"),
        "a commented partition is not default-shaped, so Go prints the full \
         list rather than the compact form, got: {shown}"
    );
    assert!(
        shown.contains("COMMENT 'first'"),
        "the comment must survive to the printed definition, got: {shown}"
    );

    // A default-shaped one still takes the compact form.
    let mut session = Session::new();
    session
        .run("CREATE TABLE hd (a INT) PARTITION BY HASH (a) PARTITIONS 2")
        .expect("plain hash table");
    let shown = show_create(&mut session, "hd");
    assert!(
        shown.contains("PARTITIONS 2"),
        "an all-default definition list keeps the compact form, got: {shown}"
    );
}

/// `CREATE`, `ALTER` and `DROP PLACEMENT POLICY`, against Go's rules in
/// `pkg/ddl/executor.go:6802` onward.
///
/// A placement policy is a schema object that tables and partitions
/// REFERENCE, so its lifecycle rules are about those references: a duplicate
/// name is 8238, an unknown one 8239, and dropping one that something still
/// names is 8241.
#[test]
fn placement_policy_lifecycle_follows_gos_rules() {
    let mut session = Session::new();

    session
        .run("CREATE PLACEMENT POLICY p1 FOLLOWERS=4")
        .expect("a new policy is created");

    // Go: `ErrPlacementPolicyExists` (8238).
    let rendered = session
        .run("CREATE PLACEMENT POLICY p1 FOLLOWERS=2")
        .expect_err("a duplicate policy name is refused")
        .to_mysql_error();
    assert_eq!(rendered.code, 8238);
    assert!(rendered.message.contains("'p1'"), "{}", rendered.message);

    // IF NOT EXISTS demotes that to a note.
    session
        .run("CREATE PLACEMENT POLICY IF NOT EXISTS p1 FOLLOWERS=2")
        .expect("IF NOT EXISTS suppresses the duplicate");

    // Go checks the OR REPLACE / IF NOT EXISTS pairing BEFORE building the
    // settings, and calls it `ErrWrongUsage` (1221).
    let rendered = session
        .run("CREATE OR REPLACE PLACEMENT POLICY IF NOT EXISTS p2 FOLLOWERS=1")
        .expect_err("OR REPLACE and IF NOT EXISTS cannot be combined")
        .to_mysql_error();
    assert_eq!(rendered.code, 1221);

    // Go: `ErrPlacementPolicyNotExists` (8239).
    let rendered = session
        .run("ALTER PLACEMENT POLICY nosuch FOLLOWERS=1")
        .expect_err("altering an unknown policy is refused")
        .to_mysql_error();
    assert_eq!(rendered.code, 8239);
    let rendered = session
        .run("DROP PLACEMENT POLICY nosuch")
        .expect_err("dropping an unknown policy is refused")
        .to_mysql_error();
    assert_eq!(rendered.code, 8239);
    session
        .run("DROP PLACEMENT POLICY IF EXISTS nosuch")
        .expect("IF EXISTS suppresses the missing policy");

    // An unreferenced policy drops cleanly.
    session
        .run("ALTER PLACEMENT POLICY p1 FOLLOWERS=3")
        .expect("altering an existing policy");
    session
        .run("DROP PLACEMENT POLICY p1")
        .expect("an unreferenced policy drops");
}

/// A table or partition REFERENCES a policy, and Go enforces both ends of
/// that reference: naming a policy that does not exist is 8239, and dropping
/// a policy something still names is 8241
/// (`CheckPlacementPolicyNotInUseFromInfoSchema`).
#[test]
fn a_placement_policy_reference_is_enforced_at_both_ends() {
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY pp FOLLOWERS=2")
        .expect("policy");

    // Naming an unknown policy on a table is refused, not silently dropped.
    let rendered = session
        .run("CREATE TABLE t1 (a INT) PLACEMENT POLICY = nosuch")
        .expect_err("an unknown policy is refused")
        .to_mysql_error();
    assert_eq!(rendered.code, 8239);

    // A table may name a policy that exists.
    session
        .run("CREATE TABLE t2 (a INT) PLACEMENT POLICY = pp")
        .expect("a table naming a real policy");

    // ... and while it does, the policy cannot be dropped.
    let rendered = session
        .run("DROP PLACEMENT POLICY pp")
        .expect_err("a referenced policy cannot be dropped")
        .to_mysql_error();
    assert_eq!(rendered.code, 8241);
    // IF EXISTS does NOT suppress this: the policy DOES exist, it is in use.
    let rendered = session
        .run("DROP PLACEMENT POLICY IF EXISTS pp")
        .expect_err("IF EXISTS does not excuse an in-use policy")
        .to_mysql_error();
    assert_eq!(rendered.code, 8241);

    // Once the reference is gone, the drop succeeds.
    session.run("DROP TABLE t2").expect("drop the referencing table");
    session
        .run("DROP PLACEMENT POLICY pp")
        .expect("an unreferenced policy drops");
}

/// A PARTITION can name a policy of its own, and that reference counts for
/// the in-use check exactly as a table's does -- Go's
/// `CheckPlacementPolicyNotInUseFromInfoSchema` walks partition definitions
/// as well as tables.
#[test]
fn a_partition_level_placement_policy_counts_as_a_reference() {
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY pq FOLLOWERS=1")
        .expect("policy");
    session
        .run("CREATE TABLE t3 (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10) PLACEMENT POLICY = pq, \
               PARTITION p1 VALUES LESS THAN (20))")
        .expect("a partition naming a policy");
    let rendered = session
        .run("DROP PLACEMENT POLICY pq")
        .expect_err("a policy referenced by a PARTITION is still in use")
        .to_mysql_error();
    assert_eq!(rendered.code, 8241);
}

/// A resolved policy reference carries Go's `PolicyRefInfo` in FULL -- the
/// policy's id as well as its name.
///
/// The id is not decoration. Placement bundles, which are what actually
/// carry these settings to PD, resolve a reference through
/// `PolicyGetter::get_policy(policy_id)`; a reference that remembered only
/// the name would look correct in the catalog and build no bundle at all.
/// An unknown policy on a PARTITION is refused with 8239, exactly as one on
/// the table is.
#[test]
fn a_resolved_policy_reference_carries_the_policy_id() {
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY pr FOLLOWERS=2")
        .expect("policy");

    // An unknown policy named by a PARTITION is refused, not stored with a
    // dangling reference.
    let rendered = session
        .run("CREATE TABLE tr (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10) PLACEMENT POLICY = nosuch)")
        .expect_err("an unknown policy on a partition is refused")
        .to_mysql_error();
    assert_eq!(rendered.code, 8239);

    // A resolved one is recorded, and holds the policy down.
    session
        .run("CREATE TABLE tr2 (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10) PLACEMENT POLICY = pr, \
               PARTITION p1 VALUES LESS THAN (20))")
        .expect("a partition naming a real policy");
    let rendered = session
        .run("DROP PLACEMENT POLICY pr")
        .expect_err("the reference holds the policy down")
        .to_mysql_error();
    assert_eq!(rendered.code, 8241);
}

/// Go prints a partition's policy as a feature-gated comment:
/// ` /*T![placement] PLACEMENT POLICY=<name> */`, after any COMMENT
/// (`AppendPartitionDefs`, `ddl/partition.go:5241`).
///
/// The `/*T![placement] ... */` wrapper matters: another MySQL-compatible
/// parser skips it while TiDB reads it back, so a dump stays loadable
/// elsewhere. And a partition carrying a policy is NOT default-shaped, so a
/// HASH table with one prints its definition list rather than the compact
/// `PARTITIONS n` -- which would otherwise drop the policy from the dump
/// entirely.
#[test]
fn a_partition_policy_prints_as_gos_feature_gated_comment() {
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY ps FOLLOWERS=2")
        .expect("policy");
    session
        .run("CREATE TABLE tp (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10) COMMENT 'c' PLACEMENT POLICY = ps, \
               PARTITION p1 VALUES LESS THAN (20))")
        .expect("a partition naming a policy");
    let shown = show_create(&mut session, "tp");
    assert!(
        shown.contains("/*T![placement] PLACEMENT POLICY=`ps` */"),
        "the policy prints as a feature-gated comment, got: {shown}"
    );
    // Go emits the COMMENT first, then the placement.
    let comment_at = shown.find("COMMENT 'c'").expect("the comment prints");
    let placement_at = shown.find("/*T![placement]").expect("the placement prints");
    assert!(
        comment_at < placement_at,
        "Go writes the comment before the placement, got: {shown}"
    );

    // A HASH partition carrying a policy is not default-shaped.
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY ph FOLLOWERS=1")
        .expect("policy");
    session
        .run("CREATE TABLE th (a INT) PARTITION BY HASH (a) \
              (PARTITION p0 PLACEMENT POLICY = ph, PARTITION p1)")
        .expect("hash table with a policy on one partition");
    let shown = show_create(&mut session, "th");
    assert!(
        !shown.contains("PARTITIONS 2"),
        "a policy-carrying partition forces the full list, got: {shown}"
    );
    assert!(
        shown.contains("/*T![placement] PLACEMENT POLICY=`ph` */"),
        "and the policy survives into it, got: {shown}"
    );
}

/// `SHOW CREATE PLACEMENT POLICY`, per Go's
/// `ConstructResultOfShowCreatePlacementPolicy` (`executor/show.go:1742`)
/// over `PlacementSettings.String()` (`meta/model/placement.go`).
///
/// The settings clause has its OWN emission order, which is not the struct's
/// field order: primary region, regions, schedule, constraints, leader
/// constraints, then voters, voter constraints, followers, follower
/// constraints, learners, learner constraints, and survival preferences
/// last. Unset items -- an empty string or a zero count -- are skipped, and
/// what remains is joined by single spaces.
#[test]
fn show_create_placement_policy_prints_gos_clause() {
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY sp PRIMARY_REGION=\"us\" REGIONS=\"us,eu\" FOLLOWERS=3")
        .expect("policy");
    let shown = crate::tests_support::show_create_policy(&mut session, "sp");
    assert_eq!(
        shown,
        "CREATE PLACEMENT POLICY `sp` PRIMARY_REGION=\"us\" REGIONS=\"us,eu\" FOLLOWERS=3"
    );

    // VOTERS precedes FOLLOWERS in Go's order even when written after it.
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY so FOLLOWERS=2 VOTERS=5")
        .expect("policy");
    let shown = crate::tests_support::show_create_policy(&mut session, "so");
    assert_eq!(shown, "CREATE PLACEMENT POLICY `so` VOTERS=5 FOLLOWERS=2");

    // An unknown policy is 8239, as Go's `fetchShowCreatePlacementPolicy`
    // reports it.
    let rendered = session
        .run("SHOW CREATE PLACEMENT POLICY nosuch")
        .expect_err("unknown policy")
        .to_mysql_error();
    assert_eq!(rendered.code, 8239);
}

/// A TABLE's own placement policy prints too, under the same feature gate a
/// partition's uses, after the comment and before the cached marker
/// (`ShowCreateTable`, `executor/show.go:1425`).
#[test]
fn a_table_policy_prints_in_show_create_table() {
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY tp1 FOLLOWERS=2")
        .expect("policy");
    session
        .run("CREATE TABLE tt (a INT) COMMENT='hello' PLACEMENT POLICY = tp1")
        .expect("table naming a policy");
    let shown = show_create(&mut session, "tt");
    assert!(
        shown.contains("/*T![placement] PLACEMENT POLICY=`tp1` */"),
        "the table's policy prints, got: {shown}"
    );
    // Go writes the comment first, then the placement.
    let comment_at = shown.find("COMMENT='hello'").expect("the comment prints");
    let placement_at = shown.find("/*T![placement]").expect("the placement prints");
    assert!(
        comment_at < placement_at,
        "Go writes COMMENT before the placement, got: {shown}"
    );

    // A table naming no policy prints no clause at all.
    session
        .run("CREATE TABLE tn (a INT)")
        .expect("plain table");
    let shown = show_create(&mut session, "tn");
    assert!(
        !shown.contains("PLACEMENT POLICY"),
        "a table without a policy prints none, got: {shown}"
    );
}

/// `ALTER TABLE ... PLACEMENT POLICY = name`, per Go's
/// `ast.TableOptionPlacementPolicy` arm (`ddl/executor.go:1927`).
///
/// The reference it records must hold the policy down for `DROP PLACEMENT
/// POLICY` exactly as one written at CREATE does -- a reference the in-use
/// check cannot see is a reference that lets the policy be dropped out from
/// under the table.
#[test]
fn alter_table_can_set_a_placement_policy() {
    let mut session = Session::new();
    session
        .run("CREATE PLACEMENT POLICY ap FOLLOWERS=2")
        .expect("policy");
    session.run("CREATE TABLE at1 (a INT)").expect("table");

    // An unknown policy is refused rather than recorded.
    let rendered = session
        .run("ALTER TABLE at1 PLACEMENT POLICY = nosuch")
        .expect_err("an unknown policy is refused")
        .to_mysql_error();
    assert_eq!(rendered.code, 8239);

    session
        .run("ALTER TABLE at1 PLACEMENT POLICY = ap")
        .expect("setting a real policy");
    let shown = show_create(&mut session, "at1");
    assert!(
        shown.contains("/*T![placement] PLACEMENT POLICY=`ap` */"),
        "the policy prints after ALTER, got: {shown}"
    );

    // And it counts for the in-use check.
    let rendered = session
        .run("DROP PLACEMENT POLICY ap")
        .expect_err("the ALTER-set reference holds the policy down")
        .to_mysql_error();
    assert_eq!(rendered.code, 8241);
}

/// `TRUNCATE TABLE` on a PARTITIONED table must actually empty it.
///
/// A partitioned table's rows are keyed by the PARTITION's physical id, not
/// the table's, so giving the table a new id while the partitions keep
/// theirs would leave every row exactly where it was and still addressable.
/// Go reassigns the partition ids for precisely this reason
/// (`onTruncateTable`, `ddl/table.go:510`, whose comment says the old data
/// "can not be accessed anymore" BECAUSE the ids changed).
#[test]
fn truncate_empties_a_partitioned_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tt (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))")
        .expect("partitioned table");
    session.run("INSERT INTO tt VALUES (1),(2),(11),(12)").expect("rows");
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM tt")),
        vec![vec!["4".to_owned()]]
    );
    session.run("TRUNCATE TABLE tt").expect("truncate");
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM tt")),
        vec![vec!["0".to_owned()]],
        "a truncated partitioned table holds no rows"
    );
    // And each partition individually.
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM tt PARTITION (p0)")),
        vec![vec!["0".to_owned()]]
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM tt PARTITION (p1)")),
        vec![vec!["0".to_owned()]]
    );
}

/// Every partition-management statement Go serves that this node does not
/// must be REFUSED, never accepted and ignored.
///
/// The distinction is the whole point. A refused `REORGANIZE PARTITION` is a
/// feature gap the user can see and work around; an accepted-and-ignored one
/// reports success and leaves the table partitioned the old way, so the next
/// statement operates on partitions the user believes are gone.
#[test]
fn unserved_partition_management_is_refused_not_ignored() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE pm (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))")
        .expect("range table");
    session
        .run("CREATE TABLE pmh (a INT) PARTITION BY HASH (a) PARTITIONS 4")
        .expect("hash table");
    session.run("CREATE TABLE plain (a INT)").expect("plain table");

    let unserved = [
        "ALTER TABLE pm REORGANIZE PARTITION p0, p1 INTO \
         (PARTITION q0 VALUES LESS THAN (20))",
        "ALTER TABLE pmh COALESCE PARTITION 2",
        "ALTER TABLE pm EXCHANGE PARTITION p0 WITH TABLE plain",
        "ALTER TABLE pmh ADD PARTITION PARTITIONS 2",
        "ALTER TABLE plain PARTITION BY HASH (a) PARTITIONS 2",
    ];
    for sql in unserved {
        match session.run(sql) {
            Err(_) => {}
            Ok(outcome) => panic!(
                "{sql} was ACCEPTED ({outcome:?}); an unserved partition change \
                 must be refused, or it reports success and changes nothing"
            ),
        }
    }
}

/// `information_schema.PARTITIONS`, per Go's `setDataFromPartitions`
/// (`executor/infoschema_reader.go`).
///
/// Every visible table produces at least one row: an UNPARTITIONED one gets a
/// single row with the partition columns NULL, not zero rows, so a client
/// joining against this table still sees it. Reporting nothing would make an
/// unpartitioned table look absent rather than unpartitioned.
#[test]
fn information_schema_partitions_reports_gos_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ip (a INT) PARTITION BY RANGE (a) \
              (PARTITION p0 VALUES LESS THAN (10) COMMENT 'first', \
               PARTITION p1 VALUES LESS THAN (MAXVALUE))")
        .expect("range table");
    session.run("CREATE TABLE plainp (a INT)").expect("plain table");

    let rows = tests_support::row_text(session.run(
        "SELECT partition_name, partition_ordinal_position, partition_method, \
                partition_expression, partition_description, partition_comment \
         FROM information_schema.partitions \
         WHERE table_name = 'ip' ORDER BY partition_ordinal_position",
    ));
    assert_eq!(
        rows,
        vec![
            vec![
                "p0".to_owned(), "1".to_owned(), "RANGE".to_owned(),
                "`a`".to_owned(), "10".to_owned(), "first".to_owned()
            ],
            vec![
                "p1".to_owned(), "2".to_owned(), "RANGE".to_owned(),
                "`a`".to_owned(), "MAXVALUE".to_owned(), "NULL".to_owned()
            ],
        ],
        "the ordinal is ONE-based and the description is the stored bound"
    );

    // An unpartitioned table is present with NULL partition columns.
    let rows = tests_support::row_text(session.run(
        "SELECT partition_name, partition_method FROM information_schema.partitions \
         WHERE table_name = 'plainp'",
    ));
    assert_eq!(
        rows,
        vec![vec!["NULL".to_owned(), "NULL".to_owned()]],
        "an unpartitioned table is one NULL row, not zero rows"
    );
}

/// The COLUMNS forms are named differently from the expression forms, and
/// print the column list where the expression would go.
#[test]
fn information_schema_partitions_names_the_columns_forms_as_go_does() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE rc (a INT, b INT) PARTITION BY RANGE COLUMNS (a, b) \
              (PARTITION p0 VALUES LESS THAN (10, 10))")
        .expect("range columns table");
    let rows = tests_support::row_text(session.run(
        "SELECT partition_method, partition_expression \
         FROM information_schema.partitions WHERE table_name = 'rc'",
    ));
    assert_eq!(
        rows,
        vec![vec!["RANGE COLUMNS".to_owned(), "`a`,`b`".to_owned()]],
        "Go names it RANGE COLUMNS and prints the column list"
    );
}

/// `SHOW TABLE STATUS` reports `Create_options`, and Go reads that cell
/// straight out of `information_schema.tables` -- its
/// `fetchShowTableStatus` (`executor/show.go:636`) SELECTs `create_options`
/// from there. So a partitioned table reports `partitioned` in BOTH places,
/// and a cached one `cached=on`.
///
/// Reporting it empty made every partitioned table look unpartitioned to any
/// client that asks this way rather than through `SHOW CREATE TABLE`.
#[test]
fn show_table_status_reports_partitioned_like_information_schema_does() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE sp (a INT) PARTITION BY HASH (a) PARTITIONS 2")
        .expect("partitioned table");
    session.run("CREATE TABLE sq (a INT)").expect("plain table");

    let status = tests_support::row_text(session.run("SHOW TABLE STATUS LIKE 'sp'"));
    let create_options = status[0]
        .get(16)
        .unwrap_or_else(|| panic!("Create_options column, got row {:?}", status[0]));
    assert_eq!(create_options, "partitioned");

    // And the two surfaces agree, which is the property Go gets for free by
    // reading one from the other.
    let from_infoschema = tests_support::row_text(session.run(
        "SELECT create_options FROM information_schema.tables WHERE table_name = 'sp'",
    ));
    assert_eq!(from_infoschema, vec![vec!["partitioned".to_owned()]]);

    let status = tests_support::row_text(session.run("SHOW TABLE STATUS LIKE 'sq'"));
    assert_eq!(status[0].get(16).map(String::as_str), Some(""));
}

/// A predicate over a constant ABOVE the maximum signed 64-bit integer must
/// answer the same on the index path and the scan path, and answer correctly.
///
/// This shape regressed once on this branch: `u >= 9223372036854775808`
/// returned every row on the scan path and the right two through the index,
/// which is the signature of a predicate being LOST rather than
/// mis-evaluated -- a scan with no filter returns everything. It is fixed
/// now, and pinned here because a differential like this is the only thing
/// that catches it: each path alone looks plausible, and only comparing them
/// shows one of them is not filtering at all.
#[test]
fn a_constant_above_i64_max_filters_the_same_on_both_paths() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ix (u BIGINT UNSIGNED, e BIGINT UNSIGNED, KEY ku(u), KEY ke(e))")
        .expect("table");
    for value in ["1", "2", "9223372036854775808", "18446744073709551615", "0"] {
        session
            .run(&format!("INSERT INTO ix VALUES ({value}, {value})"))
            .expect("row");
    }

    // Two rows are at or above 2^63: 9223372036854775808 and 2^64-1.
    let expected = vec![vec!["2".to_owned()]];
    assert_eq!(
        tests_support::row_text(
            session.run("SELECT count(*) FROM ix WHERE u >= 9223372036854775808")
        ),
        expected,
        "the index path filters"
    );
    assert_eq!(
        tests_support::row_text(session.run(
            "SELECT count(*) FROM ix IGNORE INDEX (ku) WHERE u >= 9223372036854775808"
        )),
        expected,
        "and so does the scan path -- returning every row here means the \
         predicate was dropped, not evaluated"
    );

    // And an equality on the largest representable value.
    let one = vec![vec!["1".to_owned()]];
    assert_eq!(
        tests_support::row_text(
            session.run("SELECT count(*) FROM ix WHERE e = 18446744073709551615")
        ),
        one
    );
    assert_eq!(
        tests_support::row_text(session.run(
            "SELECT count(*) FROM ix IGNORE INDEX (ke) WHERE e = 18446744073709551615"
        )),
        one
    );
}

/// An UNSIGNED row handle stores values above the maximum signed 64-bit
/// integer, and the handle key encoding is signed -- so such a value encodes
/// NEGATIVE and sorts, in key order, before every ordinary one.
///
/// Go copes by splitting a scan's ranges at that boundary
/// (`SplitRangesAcrossInt64Boundary`, `distsql/request_builder.go:575`) and
/// reading the two halves in the right order.
///
/// These answers were pinned BEFORE that split was ported, while this tier
/// still refused to range over an unsigned handle and full-scanned instead --
/// because the port trades a correct slow plan for a fast one whose failure
/// mode is silently missing or duplicated rows, and the cases below are
/// exactly where it would break: at `i64::MAX`, one past it, and at the top
/// of the domain. They are unchanged by the port; what changed is the plan
/// underneath them, which
/// [`an_unsigned_row_handle_is_read_through_ranges_like_go`] pins.
#[test]
fn an_unsigned_row_handle_answers_correctly_across_the_int64_boundary() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uh (id BIGINT UNSIGNED PRIMARY KEY, v INT)")
        .expect("table");
    for value in [
        "0",
        "1",
        "9223372036854775807",  // i64::MAX
        "9223372036854775808",  // i64::MAX + 1, the first value that encodes negative
        "18446744073709551615", // u64::MAX
    ] {
        session
            .run(&format!("INSERT INTO uh VALUES ({value}, 1)"))
            .expect("row");
    }

    let count = |session: &mut Session, sql: &str| {
        tests_support::row_text(session.run(sql))
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
            .unwrap_or_default()
    };

    assert_eq!(count(&mut session, "SELECT count(*) FROM uh"), "5");
    // The two values at or above the boundary.
    assert_eq!(
        count(&mut session, "SELECT count(*) FROM uh WHERE id >= 9223372036854775808"),
        "2"
    );
    // ... and the three below it.
    assert_eq!(
        count(&mut session, "SELECT count(*) FROM uh WHERE id < 9223372036854775808"),
        "3"
    );
    // A range that STRADDLES the boundary is the case Go has to split.
    assert_eq!(
        count(
            &mut session,
            "SELECT count(*) FROM uh WHERE id BETWEEN 1 AND 18446744073709551615"
        ),
        "4"
    );
    // Exactly at the boundary, both sides.
    assert_eq!(
        count(&mut session, "SELECT count(*) FROM uh WHERE id = 9223372036854775807"),
        "1"
    );
    assert_eq!(
        count(&mut session, "SELECT count(*) FROM uh WHERE id = 9223372036854775808"),
        "1"
    );

    // Ordering is UNSIGNED, not the signed order the key encoding would give.
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM uh ORDER BY id")),
        vec![
            vec!["0".to_owned()],
            vec!["1".to_owned()],
            vec!["9223372036854775807".to_owned()],
            vec!["9223372036854775808".to_owned()],
            vec!["18446744073709551615".to_owned()],
        ],
        "reading in KEY order would put the two large values first"
    );
}

/// The plan the answers above are now reached through, and the three places
/// the unsigned domain is VISIBLE in it.
///
/// Go's ranger materialises an open endpoint in the column's own domain
/// (`convertPointsInPlace`, `ranger.go:71-78`), so an unsigned handle's open
/// LOW is `0` rather than `-inf`, and `formatDatum` (`ranger/types.go:371`)
/// prints a `KindUint64` low bound as the number. Its open HIGH is
/// `MaxUint64`, which that same function prints as `+inf`.
///
/// A point access prints the handle's UNSIGNED reading, because Go's plan
/// carries `UnsignedHandle` and formats the identical 64 bits with
/// `strconv.FormatUint` (`physical_batch_point_get.go:206`). Without it
/// `18446744073709551615` prints as `-1`.
#[test]
fn an_unsigned_row_handle_is_read_through_ranges_like_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uh (id BIGINT UNSIGNED PRIMARY KEY, v INT)")
        .expect("table");
    session
        .run("INSERT INTO uh VALUES (1, 1), (18446744073709551615, 1)")
        .expect("rows");

    let scan_line = |session: &mut Session, sql: &str| {
        tests_support::row_text(session.run(sql))
            .into_iter()
            .map(|row| row.join(" "))
            .find(|line| line.contains("Scan") || line.contains("Point_Get"))
            .unwrap_or_default()
    };

    // The predicate is CONSUMED by the range, so this is not a full scan with
    // a filter above it: the range is the whole of the restriction.
    assert!(
        scan_line(&mut session, "EXPLAIN SELECT * FROM uh WHERE id >= 9223372036854775808")
            .contains("range:[9223372036854775808,+inf]"),
        "an unsigned bound above the signed maximum has to reach the scan as a range"
    );
    // The open LOW end is the unsigned domain's `0`, not `-inf`.
    assert!(
        scan_line(&mut session, "EXPLAIN SELECT * FROM uh WHERE id < 9223372036854775808")
            .contains("range:[0,9223372036854775808)"),
        "an unsigned handle's open low bound is 0"
    );
    // ... while the open HIGH end still prints `+inf`, because `MaxUint64` on
    // the right side is what Go's formatter spells that way.
    assert!(
        scan_line(&mut session, "EXPLAIN SELECT * FROM uh WHERE id > 1").contains("range:(1,+inf]"),
        "an unsigned handle's open high bound prints +inf"
    );
    for (sql, expected) in [
        (
            "EXPLAIN SELECT * FROM uh WHERE id = 18446744073709551615",
            "handle:18446744073709551615",
        ),
        (
            "EXPLAIN SELECT * FROM uh WHERE id IN (1, 18446744073709551615)",
            "handle:[1 18446744073709551615]",
        ),
    ] {
        assert!(
            scan_line(&mut session, sql).contains(expected),
            "{sql} must print the handle's unsigned reading, not its signed one"
        );
    }
}

/// The same handle, PARTITIONED: the merge that orders one scan's partitions
/// has to compare in the handle's own domain too.
///
/// A partitioned keep-order table scan reads one key range per partition and
/// merges them. The merge key is the record key, whose handle is written with
/// the SIGNED integer codec -- so an unsigned handle above `i64::MAX` sorts
/// FIRST in key order while its value is the largest. Merging on the raw
/// bytes answered `ORDER BY id` with `2^63, u64::MAX, 1, i64::MAX`.
///
/// Go compares the `byItems`' decoded VALUES instead
/// (`NewSortedSelectResults`), which is this order.
#[test]
fn an_unsigned_row_handle_orders_across_partitions_by_value_not_by_key() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE up (id BIGINT UNSIGNED PRIMARY KEY, v INT) \
             PARTITION BY HASH (id) PARTITIONS 4",
        )
        .expect("table");
    for value in [
        "0",
        "1",
        "9223372036854775807",
        "9223372036854775808",
        "18446744073709551615",
    ] {
        session
            .run(&format!("INSERT INTO up VALUES ({value}, 1)"))
            .expect("row");
    }
    let ascending = vec![
        vec!["1".to_owned()],
        vec!["9223372036854775807".to_owned()],
        vec!["9223372036854775808".to_owned()],
        vec!["18446744073709551615".to_owned()],
    ];
    // A `WHERE` is what puts the scan on the RANGE path, which is the path
    // that merges; without one the read is a full scan of each partition.
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM up WHERE id >= 1 ORDER BY id")),
        ascending
    );
    let mut descending = ascending.clone();
    descending.reverse();
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM up WHERE id >= 1 ORDER BY id DESC")),
        descending
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM up WHERE id >= 9223372036854775808")),
        vec![vec!["2".to_owned()]]
    );
}

/// A statement that WRITES through an unsigned handle range, which is where a
/// range that reads the wrong rows stops being recoverable.
///
/// `UPDATE`/`DELETE` take the same table path as the reads above, so the
/// straddling range has to be cut for them too -- and the count they report
/// is the count of rows they touched.
#[test]
fn writes_restricted_by_an_unsigned_handle_range_touch_exactly_those_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uw (id BIGINT UNSIGNED PRIMARY KEY, v INT)")
        .expect("table");
    for value in [
        "0",
        "1",
        "9223372036854775807",
        "9223372036854775808",
        "18446744073709551615",
    ] {
        session
            .run(&format!("INSERT INTO uw VALUES ({value}, 1)"))
            .expect("row");
    }
    // Only the two rows at or above the boundary.
    assert_eq!(
        session
            .run("UPDATE uw SET v = 2 WHERE id >= 9223372036854775808")
            .expect("update"),
        StmtResult::Affected(2)
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM uw WHERE v = 2 ORDER BY id")),
        vec![
            vec!["9223372036854775808".to_owned()],
            vec!["18446744073709551615".to_owned()],
        ]
    );
    // A STRADDLING range: two rows below the boundary and one above it.
    assert_eq!(
        session
            .run("DELETE FROM uw WHERE id BETWEEN 1 AND 9223372036854775808")
            .expect("delete"),
        StmtResult::Affected(3)
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM uw ORDER BY id")),
        vec![
            vec!["0".to_owned()],
            vec!["18446744073709551615".to_owned()],
        ]
    );
}

/// A NARROW unsigned handle never reaches the boundary, and the split must
/// leave it alone.
///
/// `INT UNSIGNED` tops out at `4294967295`, so no range of it crosses the
/// point where the key encoding flips sign -- Go's `sort.Search` finds no
/// bound past `MaxInt64` and returns the ranges untouched. The open high end
/// is still the DOMAIN's `MaxUint64`, which is why it prints `+inf` rather
/// than the column's own maximum.
#[test]
fn a_narrow_unsigned_row_handle_is_ranged_over_without_a_split() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ui (id INT UNSIGNED PRIMARY KEY, v INT)")
        .expect("table");
    for value in ["0", "1", "4294967295"] {
        session
            .run(&format!("INSERT INTO ui VALUES ({value}, 1)"))
            .expect("row");
    }
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM ui WHERE id > 0 ORDER BY id")),
        vec![vec!["1".to_owned()], vec!["4294967295".to_owned()]]
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT count(*) FROM ui WHERE id < 4294967295")),
        vec![vec!["2".to_owned()]]
    );
    assert!(
        tests_support::row_text(session.run("EXPLAIN SELECT id FROM ui WHERE id > 0"))
            .into_iter()
            .any(|row| row.join(" ").contains("range:(0,+inf]")),
        "the open high end is the unsigned DOMAIN's maximum, not the column's"
    );
}

/// A whole-table scan of an unsigned handle KEEPS ORDER, because the scan is
/// cut at the sign flip like any other range.
///
/// Go's `matchProperty` makes this claim without consulting the handle's
/// signedness (`find_best_task.go:1084`), and its table reader earns it by
/// splitting `ranger.FullIntRange(true)` -- the domain as one range -- into
/// the two halves it reads in value order. Reading the raw key range instead
/// walks the block above `i64::MAX` FIRST, so the claim would be a lie and
/// `ORDER BY id` would need a `Sort` this plan does not have.
#[test]
fn a_full_scan_of_an_unsigned_handle_keeps_order() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uh (id BIGINT UNSIGNED PRIMARY KEY, v INT)")
        .expect("table");
    for value in UNSIGNED_HANDLE_VALUES {
        session
            .run(&format!("INSERT INTO uh VALUES ({value}, 1)"))
            .expect("row");
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        tests_support::row_text(session.run(sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect()
    };
    let ordered = plan(&mut session, "EXPLAIN SELECT id FROM uh ORDER BY id");
    assert!(
        ordered.iter().any(|line| line.contains("keep order:true")),
        "the scan itself has to promise the order: {ordered:?}"
    );
    assert!(
        !ordered.iter().any(|line| line.contains("Sort")),
        "and so nothing above it sorts: {ordered:?}"
    );

    let ascending: Vec<Vec<String>> = UNSIGNED_HANDLE_VALUES
        .iter()
        .map(|value| vec![(*value).to_owned()])
        .collect();
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM uh ORDER BY id")),
        ascending
    );
    let mut descending = ascending.clone();
    descending.reverse();
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM uh ORDER BY id DESC")),
        descending
    );
    // A `LIMIT` rides that promise into the scan, so taking the wrong two rows
    // is the failure this pins: the largest values live in the LOWEST keys.
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM uh ORDER BY id LIMIT 2")),
        ascending[..2].to_vec()
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT id FROM uh ORDER BY id DESC LIMIT 2")),
        descending[..2].to_vec()
    );
}

/// Nine values around the point where the handle key encoding flips sign:
/// either side of `i64::MAX`, either side of `i64::MAX + 1`, and either side
/// of the domain's ends. They are both the stored rows and the bounds the
/// predicates below are written against, so every interval endpoint lands on,
/// just below, or just above a row that exists.
const UNSIGNED_HANDLE_VALUES: &[&str] = &[
    "0",
    "1",
    "2",
    "9223372036854775806",
    "9223372036854775807",
    "9223372036854775808",
    "9223372036854775809",
    "18446744073709551614",
    "18446744073709551615",
];

/// One question down two paths that must agree, over the whole neighbourhood
/// of the sign flip.
///
/// `uh` stores the unsigned column as its CLUSTERED HANDLE, so a predicate on
/// it becomes a key range and the predicate itself is dropped. `up` is the
/// same table partitioned four ways, so the range is cut per partition and
/// re-merged. `uf` stores the identical values in a plain column with a
/// `_tidb_rowid` handle, so nothing is ranged and every row is tested.
///
/// A range that reads the wrong records cannot hide here: the answer is
/// compared unordered, ascending and descending, for every comparison and
/// interval over nine values chosen around `i64::MAX`, `i64::MAX + 1` and
/// `u64::MAX`.
#[test]
fn every_unsigned_handle_predicate_reads_the_same_rows_ranged_as_filtered() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uh (id BIGINT UNSIGNED PRIMARY KEY, v INT)")
        .expect("uh");
    session
        .run("CREATE TABLE uf (id BIGINT UNSIGNED, v INT)")
        .expect("uf");
    for value in UNSIGNED_HANDLE_VALUES {
        session
            .run(&format!("INSERT INTO uh VALUES ({value}, 1)"))
            .expect("uh row");
        session
            .run(&format!("INSERT INTO uf VALUES ({value}, 1)"))
            .expect("uf row");
    }

    let mut predicates: Vec<String> = Vec::new();
    for bound in UNSIGNED_HANDLE_VALUES {
        for op in ["=", "!=", "<", "<=", ">", ">="] {
            predicates.push(format!("id {op} {bound}"));
        }
    }
    for low in UNSIGNED_HANDLE_VALUES {
        for high in UNSIGNED_HANDLE_VALUES {
            predicates.push(format!("id BETWEEN {low} AND {high}"));
            predicates.push(format!("id > {low} AND id < {high}"));
            predicates.push(format!("id >= {low} AND id <= {high}"));
            predicates.push(format!("id < {low} OR id > {high}"));
        }
    }
    predicates.push("id IN (0, 9223372036854775808, 18446744073709551615)".to_owned());
    predicates.push("id IN (1, 2) OR id > 18446744073709551614".to_owned());
    predicates.push("id NOT IN (1, 9223372036854775808)".to_owned());
    predicates.push("id IS NULL".to_owned());
    predicates.push("id IS NOT NULL".to_owned());

    let read = |session: &mut Session, sql: &str| -> Vec<String> {
        let mut rows: Vec<String> = tests_support::row_text(session.run(sql))
            .into_iter()
            .map(|row| row.join("|"))
            .collect();
        rows.sort();
        rows
    };

    session
        .run("CREATE TABLE up (id BIGINT UNSIGNED PRIMARY KEY, v INT) PARTITION BY HASH (id) PARTITIONS 4")
        .expect("up");
    for value in UNSIGNED_HANDLE_VALUES {
        session
            .run(&format!("INSERT INTO up VALUES ({value}, 1)"))
            .expect("up row");
    }
    let ordered = |session: &mut Session, sql: &str| -> Vec<String> {
        tests_support::row_text(session.run(sql))
            .into_iter()
            .map(|row| row.join("|"))
            .collect()
    };

    let mut mismatches = Vec::new();
    for predicate in &predicates {
        let ranged = read(&mut session, &format!("SELECT id FROM uh WHERE {predicate}"));
        let filtered = read(&mut session, &format!("SELECT id FROM uf WHERE {predicate}"));
        if ranged != filtered {
            mismatches.push(format!(
                "  {predicate}\n    range : {ranged:?}\n    filter: {filtered:?}"
            ));
        }
        let partitioned = read(&mut session, &format!("SELECT id FROM up WHERE {predicate}"));
        if partitioned != filtered {
            mismatches.push(format!(
                "  {predicate}\n    parts : {partitioned:?}\n    filter: {filtered:?}"
            ));
        }
        for direction in ["ASC", "DESC"] {
            let want = ordered(
                &mut session,
                &format!("SELECT id FROM uf WHERE {predicate} ORDER BY id {direction}"),
            );
            for table in ["uh", "up"] {
                let got = ordered(
                    &mut session,
                    &format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id {direction}"),
                );
                if got != want {
                    mismatches.push(format!(
                        "  {predicate} ORDER BY id {direction}\n    {table}: {got:?}\n    filter: {want:?}"
                    ));
                }
            }
        }
    }
    println!("checked {} predicates", predicates.len());
    assert!(
        mismatches.is_empty(),
        "the range path and the filter path disagree:\n{}",
        mismatches.join("\n")
    );
}

/// A partitioned UNORDERED index lookup answers PARTITION BY PARTITION, in
/// pruned-partition order, handles ascending within each partition -- never
/// as one globally handle-sorted (or index-ordered) stream.
///
/// Go's `IndexLookUpExecutor` builds one index request per pruned partition
/// (`buildTableKeyRanges`), drains each partition's result before the next
/// (`indexWorker.fetchHandles`), tags every `lookupTableTask` with exactly
/// one partition (`buildAndDispatchLookupTasks`:
/// `tableLookUpTask.partitionTable = prunedPartitions[curResultIdx]`), and
/// sorts each task's handles before its table read
/// (`buildTableReaderFromHandles`: `slices.SortFunc`). Captured:
/// `executor/index_lookup_pushdown_partition`'s `select ... from tp3`
/// records `4 | 1,5 | 2,6 | 3`.
///
/// The layout: `HASH(a) PARTITIONS 4` routes `a` 1..6 as p0={4}, p1={1,5},
/// p2={2,6}, p3={3}, and `_tidb_rowid` follows insertion order 1..6, so
/// partition-major handle order (4,1,5,2,6,3), global handle order (1..6)
/// and index order over `b` (6,5,4,3,2,1) are three DIFFERENT sequences --
/// the assertion cannot pass by accident of any other rule.
#[test]
fn a_partitioned_index_lookup_answers_partition_by_partition_in_handle_order() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tp (a INT, b INT, c INT, KEY b(b)) \
             PARTITION BY HASH(a) PARTITIONS 4",
        )
        .expect("tp");
    session
        .run(
            "INSERT INTO tp VALUES (1, 10, 10), (2, 9, 20), (3, 8, 30), \
             (4, 7, 40), (5, 6, 50), (6, 5, 60)",
        )
        .expect("rows");
    // The read has to be the DOUBLE READ under test, not a table scan.
    let plan: Vec<String> = tests_support::row_text(
        session.run("EXPLAIN SELECT * FROM tp USE INDEX(b) WHERE b <= 10"),
    )
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    assert!(
        plan.iter().any(|line| line.contains("IndexLookUp")),
        "USE INDEX(b) under SELECT * must plan a double read: {plan:?}"
    );
    assert_eq!(
        tests_support::row_text(session.run("SELECT a FROM tp USE INDEX(b) WHERE b <= 10")),
        [4, 1, 5, 2, 6, 3]
            .iter()
            .map(|a| vec![a.to_string()])
            .collect::<Vec<_>>(),
        "partition-major, handle-ascending within each partition \
         (p0={{4}}, p1={{1,5}}, p2={{2,6}}, p3={{3}})"
    );
}

/// A MULTI-RANGE partitioned lookup is still PARTITION-major, not
/// range-major.
///
/// Go puts EVERY range into each partition's one index request
/// (`buildTableKeyRanges` hands `buildKeyRanges` all of `e.ranges` for all
/// `tableIDs`), so `b IN (5, 8, 10)` reads p1's matches, then p2's, then
/// p3's. Walking range by range across the partitions instead would answer
/// `6, 3, 1` -- the `[5,5]` matches of every partition first.
#[test]
fn a_multi_range_partitioned_lookup_stays_partition_major() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tp (a INT, b INT, c INT, KEY b(b)) \
             PARTITION BY HASH(a) PARTITIONS 4",
        )
        .expect("tp");
    session
        .run(
            "INSERT INTO tp VALUES (1, 10, 10), (2, 9, 20), (3, 8, 30), \
             (4, 7, 40), (5, 6, 50), (6, 5, 60)",
        )
        .expect("rows");
    // b=10 -> a=1 (p1), b=8 -> a=3 (p3), b=5 -> a=6 (p2): partition order
    // p1, p2, p3.
    assert_eq!(
        tests_support::row_text(session.run("SELECT a FROM tp USE INDEX(b) WHERE b IN (5, 8, 10)")),
        [1, 6, 3]
            .iter()
            .map(|a| vec![a.to_string()])
            .collect::<Vec<_>>(),
        "every range belongs to each partition's one request, so the answer \
         is p1(b=10), p2(b=5), p3(b=8) -- not the ranges in listed order"
    );
}

/// An embedded `LIMIT` truncates a PLAIN partitioned lookup in INDEX order,
/// and an `INDEX_LOOKUP_PUSHDOWN` one in HANDLE order -- the two flavours
/// keep a DIFFERENT row of the partition the limit lands in.
///
/// The `WHERE` is fully consumed by the index range (`b < 10`, nothing
/// else), which is what lets the limit embed into the lookup at all -- a
/// leftover conjunct keeps a root `Limit` above a root `Selection` here,
/// and a root limit over the partition-major stream truncates like the
/// pushdown flavour by construction.
///
/// Plain: Go's `extractTaskHandles` cuts the index stream cumulatively
/// BEFORE the handle sort (`leftCnt := Offset + Count - scannedKeys`), so
/// the partition where the budget runs out contributes its INDEX-order
/// prefix. p0 and p1 contribute one qualifying row each (p1's `b=10` entry
/// is outside the range), leaving budget 1 in p2, whose index order over
/// `b` is `a=6 (b=5), a=2 (b=9)` -- so the plain lookup keeps 6.
///
/// Pushdown: the pushed Limit rides INSIDE each per-partition cop request
/// (`Limit | cop[tikv]` under `LocalIndexLookUp`), unistore's local lookup
/// sorts the surviving keys by handle (`indexLookUpExec.fetchTableScans`:
/// `sort.Slice(sortedHandles, ...)`), and the cumulative stop counts those
/// ARRIVALS (`extractLookUpPushDownRowsOrHandles`: `scannedKeys > Offset +
/// Count` stops) -- so p2 contributes its HANDLE-order prefix, keeping 2.
/// The same mechanism is captured in the corpus:
/// `executor/index_lookup_pushdown_partition` records `4, 5, 2` for its
/// hinted `where b < 10 and a > 1 limit 3`.
#[test]
fn an_embedded_limit_truncates_plain_and_pushdown_partitioned_lookups_differently() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tp (a INT PRIMARY KEY, b INT, c INT, KEY b(b)) \
             PARTITION BY HASH(a) PARTITIONS 4",
        )
        .expect("tp");
    session
        .run(
            "INSERT INTO tp VALUES (1, 10, 10), (2, 9, 20), (3, 8, 30), \
             (4, 7, 40), (5, 6, 50), (6, 5, 60)",
        )
        .expect("rows");
    // `SELECT *` keeps the read NON-COVERING (the corpus statements read the
    // whole row): a bare `SELECT a` is answered by the index alone -- Go's
    // `PhysicalIndexReader` -- and never builds the handle batch under test.
    let firsts = |rows: Vec<Vec<String>>| -> Vec<String> {
        rows.into_iter()
            .map(|row| row.into_iter().next().expect("a"))
            .collect()
    };
    assert_eq!(
        firsts(tests_support::row_text(session.run(
            "SELECT /*+ index_lookup_pushdown(tp, b) */ * FROM tp \
             WHERE b < 10 LIMIT 3"
        ))),
        vec!["4", "5", "2"],
        "pushdown: p2's handle-order prefix"
    );
    assert_eq!(
        firsts(tests_support::row_text(session.run(
            "SELECT /*+ use_index(tp, b) */ * FROM tp \
             WHERE b < 10 LIMIT 3"
        ))),
        vec!["4", "5", "6"],
        "plain: p2's index-order prefix (b=5 -> a=6 comes first)"
    );
}

/// The same two flavours over a COMMON-handle RANGE COLUMNS table: the
/// mirror of `executor/index_lookup_pushdown_partition`'s `tp2`, whose
/// hinted `limit 5` records `a,b,c,d,e` -- p2's HANDLE-order prefix `e`,
/// where the plain lookup's index-order truncation keeps `f` (`a=44` is
/// p2's first index key).
#[test]
fn a_common_handle_partitioned_lookup_limit_keeps_the_flavour_prefix() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tpc (id1 VARCHAR(32), id2 INT, a INT, b INT, \
             PRIMARY KEY (id1, id2) CLUSTERED, INDEX a(a)) \
             PARTITION BY RANGE COLUMNS (id1) (\
             PARTITION p0 VALUES LESS THAN ('c'), \
             PARTITION p1 VALUES LESS THAN ('e'), \
             PARTITION p2 VALUES LESS THAN ('g'), \
             PARTITION p3 VALUES LESS THAN MAXVALUE)",
        )
        .expect("tpc");
    session
        .run(
            "INSERT INTO tpc VALUES ('a', 1, 99, 10), ('b', 2, 88, 20), \
             ('c', 3, 77, 30), ('d', 4, 66, 40), ('e', 5, 55, 50), \
             ('f', 6, 44, 60), ('g', 7, 33, 70), ('h', 8, 22, 80)",
        )
        .expect("rows");
    let firsts = |rows: Vec<Vec<String>>| -> Vec<String> {
        rows.into_iter()
            .map(|row| row.into_iter().next().expect("id1"))
            .collect()
    };
    // `SELECT *` keeps the read NON-COVERING, as in the corpus: `id1` alone
    // is a common-handle prefix the index itself carries.
    assert_eq!(
        firsts(tests_support::row_text(session.run(
            "SELECT /*+ index_lookup_pushdown(tpc, a) */ * FROM tpc \
             WHERE a > 33 LIMIT 5"
        ))),
        vec!["a", "b", "c", "d", "e"],
        "the recorded corpus answer: p0 (a,b), p1 (c,d), then p2's \
         handle-order prefix (e)"
    );
    assert_eq!(
        firsts(tests_support::row_text(session.run(
            "SELECT /*+ use_index(tpc, a) */ * FROM tpc \
             WHERE a > 33 LIMIT 5"
        ))),
        vec!["a", "b", "c", "d", "f"],
        "plain: p2's INDEX-order prefix -- a=44 belongs to f"
    );
}
