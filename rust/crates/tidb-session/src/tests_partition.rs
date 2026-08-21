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
