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

//! `CREATE TABLE ... PARTITION BY HASH` is REAL; the other three methods are
//! refused, and the refusal is still a tripwire.
//!
//! This node stores a `PartitionSpec` (Go `model.PartitionInfo`; see
//! `tidb_executor::partition_routing`), routes each row into one of N
//! physical key prefixes exactly as Go's `locatePartition` does, and prints
//! the clause back through `SHOW CREATE TABLE`. What it still has no answer
//! for is RANGE/LIST/KEY routing and partition PRUNING
//! (`pkg/planner/core/rule_partition_processor.go`), so those methods -- and
//! `SELECT ... PARTITION (p0)` for every method -- are refused rather than
//! answered wrongly.
//!
//! # The captures these tests are written against
//!
//! Every `GO_*` constant below is real TiDB's own `SHOW CREATE TABLE` text,
//! and every routing answer in [`hash_routing_matches_real_tidb`] is the
//! partition real TiDB put that row in, both captured through a mock-store
//! session. The HASH rows are asserted as EQUALITY; the other three are still
//! asserted as refusals, and become equalities when their routing lands.

#![cfg(test)]

use crate::tests_support::show_create;
use crate::*;

/// Go's `SHOW CREATE TABLE h1` for
/// `create table h1 (a int, b int) partition by hash(a) partitions 4`.
const GO_HASH: &str = "CREATE TABLE `h1` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY HASH (`a`) PARTITIONS 4";

/// Go's `SHOW CREATE TABLE h3` for `partition by hash(a+b) partitions 3`.
/// The expression keeps Go's own bracketed, space-free spelling.
const GO_HASH_EXPR: &str = "CREATE TABLE `h3` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY HASH ((`a`+`b`)) PARTITIONS 3";

/// Go's `SHOW CREATE TABLE lh` for `partition by linear hash(a) partitions
/// 4`: the LINEAR keyword is accepted, warned about, and NOT printed back.
const GO_LINEAR_HASH: &str = "CREATE TABLE `lh` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY HASH (`a`) PARTITIONS 4";

/// Go's `SHOW CREATE TABLE r1` for a three-way RANGE table with `MAXVALUE`.
const GO_RANGE: &str = "CREATE TABLE `r1` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE (`a`)\n(PARTITION `p0` VALUES LESS THAN (10),\n PARTITION `p1` VALUES LESS THAN (20),\n PARTITION `pm` VALUES LESS THAN (MAXVALUE))";

/// Go's `SHOW CREATE TABLE l1` for a two-way LIST table.
const GO_LIST: &str = "CREATE TABLE `l1` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY LIST (`a`)\n(PARTITION `p0` VALUES IN (1,2,3),\n PARTITION `p1` VALUES IN (4,5,6))";

/// Go's `SHOW CREATE TABLE k1` for `partition by key(a) partitions 3`.
const GO_KEY: &str = "CREATE TABLE `k1` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY KEY (`a`) PARTITIONS 3";

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

/// The three methods still without routing are REFUSED, and the refusal names
/// the method. The `GO_*` text each row carries is what this must answer
/// instead once that method lands.
#[test]
fn range_list_and_key_partitioning_are_still_refused() {
    for (sql, table, go) in [
        (
            "CREATE TABLE r1 (a int, b int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), PARTITION pm VALUES LESS THAN MAXVALUE)",
            "r1",
            GO_RANGE,
        ),
        (
            "CREATE TABLE l1 (a int, b int) PARTITION BY LIST(a) (PARTITION p0 VALUES IN (1,2,3), PARTITION p1 VALUES IN (4,5,6))",
            "l1",
            GO_LIST,
        ),
        (
            "CREATE TABLE k1 (a int PRIMARY KEY, b int) PARTITION BY KEY(a) PARTITIONS 3",
            "k1",
            GO_KEY,
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
/// for rejection: it is refused by the method gate rather than by the rule,
/// and its errno becomes an assertion when that method lands.
const GO_REJECTED: &[(&str, u16, &str, bool)] = &[
    (
        "CREATE TABLE e1 (a varchar(10)) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10))",
        1659,
        "Field 'a' is of a not allowed type for this type of partitioning",
        false,
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
        false,
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
        false,
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
        false,
    ),
    (
        "CREATE TABLE e6 (a int) PARTITION BY HASH(a) PARTITIONS 0",
        1504,
        "Number of partitions = 0 is not an allowed value",
        true,
    ),
    (
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
        .run("CREATE TABLE IF NOT EXISTS ine (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10))")
        .is_err());
    session
        .run("CREATE TABLE IF NOT EXISTS ine (a int) PARTITION BY HASH(a) PARTITIONS 2")
        .expect("a HASH clause is built under IF NOT EXISTS too");
}

/// `SELECT ... PARTITION (p0)` is REFUSED rather than answered with the whole
/// table, which is what ignoring the clause would do.
#[test]
fn a_partition_selection_is_refused_rather_than_ignored() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE h (a int, b int) PARTITION BY HASH(a) PARTITIONS 4")
        .unwrap();
    session.run("INSERT INTO h VALUES (1,1),(2,2)").unwrap();
    assert!(
        session.run("SELECT * FROM h PARTITION (p0)").is_err(),
        "a partition selection must not silently read the whole table"
    );
    assert!(session.run("DELETE FROM h PARTITION (p0)").is_err());
    assert!(session
        .run("INSERT INTO h PARTITION (p0) VALUES (4, 4)")
        .is_err());
}
