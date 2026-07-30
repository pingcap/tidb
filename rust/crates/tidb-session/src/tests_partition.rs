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

//! `CREATE TABLE ... PARTITION BY` is refused, and the refusal is a tripwire.
//!
//! This node stores no `model.PartitionInfo` (Go `pkg/ddl/partition.go`
//! `buildTablePartitionInfo`), has no per-partition physical tables
//! (`pkg/table/tables/partition.go`) and no partition pruning
//! (`pkg/planner/core/rule_partition_processor.go`). Until it has all three,
//! accepting the clause would build an ORDINARY table and answer every
//! partition-aware query wrongly while reporting success -- so
//! `tidb_executor::ddl::table_partition` refuses instead.
//!
//! # The captures these tests are written against
//!
//! Every `GO_*` constant below is real TiDB's own `SHOW CREATE TABLE` text,
//! captured through a mock-store session. They are here on purpose: when
//! partitioning lands, the assertions in [`refused_forms`] are replaced by
//! equality against these constants, and the test file needs no new capture
//! work to become the acceptance suite.
//!
//! Each `#[test]` therefore states TWO things: what this node answers today
//! (a refusal), and what it must answer the day the refusal is deleted.

#![cfg(test)]

use crate::*;

/// Go's `SHOW CREATE TABLE h1` for
/// `create table h1 (a int, b int) partition by hash(a) partitions 4`.
const GO_HASH: &str = "CREATE TABLE `h1` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY HASH (`a`) PARTITIONS 4";

/// Go's `SHOW CREATE TABLE r1` for a three-way RANGE table with `MAXVALUE`.
const GO_RANGE: &str = "CREATE TABLE `r1` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE (`a`)\n(PARTITION `p0` VALUES LESS THAN (10),\n PARTITION `p1` VALUES LESS THAN (20),\n PARTITION `pm` VALUES LESS THAN (MAXVALUE))";

/// Go's `SHOW CREATE TABLE l1` for a two-way LIST table.
const GO_LIST: &str = "CREATE TABLE `l1` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY LIST (`a`)\n(PARTITION `p0` VALUES IN (1,2,3),\n PARTITION `p1` VALUES IN (4,5,6))";

/// Go's `SHOW CREATE TABLE k1` for `partition by key(a) partitions 3`.
const GO_KEY: &str = "CREATE TABLE `k1` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY KEY (`a`) PARTITIONS 3";

/// The four partition methods a user can write, with the statement that
/// creates one and the Go answer that statement earns. When the refusal is
/// deleted, this is the acceptance table.
fn captured_forms() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "CREATE TABLE h1 (a int, b int) PARTITION BY HASH(a) PARTITIONS 4",
            "h1",
            GO_HASH,
        ),
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
    ]
}

/// Every partition method is REFUSED, and the refusal names the method.
///
/// The `GO_*` text each row carries is what this must answer instead once
/// partitioning is implemented; the assertion below then becomes
/// `assert_eq!(show, go)`.
#[test]
fn refused_forms() {
    for (sql, table, go) in captured_forms() {
        let mut session = Session::new();
        let error = session
            .run(sql)
            .expect_err("a partitioned CREATE TABLE must not report success");
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

/// The exact defect this refusal closes: the statement used to SUCCEED and
/// build an ordinary table whose `SHOW CREATE TABLE` printed no partition
/// clause at all. Both halves are asserted, because the silent success and
/// the lost clause are two separate lies.
#[test]
fn a_partitioned_create_never_silently_becomes_an_ordinary_table() {
    let mut session = Session::new();
    assert!(session
        .run("CREATE TABLE p (a int) PARTITION BY HASH(a) PARTITIONS 2")
        .is_err());
    assert!(
        session.run("SHOW CREATE TABLE p").is_err(),
        "no unpartitioned `p` may exist after the refusal"
    );
    assert!(
        session.run("INSERT INTO p VALUES (1)").is_err(),
        "and nothing may be written into it"
    );
}

/// The refusal is scoped to the partitioning clause alone: the SAME table
/// without it is still created and still behaves. This is the check that
/// would catch a refusal written too broadly.
#[test]
fn an_unpartitioned_create_table_is_untouched() {
    let mut session = Session::new();
    session.run("CREATE TABLE q (a int, b int)").unwrap();
    session.run("INSERT INTO q VALUES (1, 2)").unwrap();
    let show = session.run("SHOW CREATE TABLE q");
    assert!(show.is_ok());
}

/// The partition definitions real TiDB REJECTS, with the errno and message it
/// rejects each one under. Captured the same way the `GO_*` texts above were.
///
/// Go rejects a great deal at `CREATE`, and each of these is a rule a future
/// implementation owes: none of them can be inferred from the grammar, and all
/// of them are places where accepting the statement would be a different
/// silent wrong answer than the one this unit closed.
const GO_REJECTED: &[(&str, u16, &str)] = &[
    (
        "CREATE TABLE e1 (a varchar(10)) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10))",
        1659,
        "Field 'a' is of a not allowed type for this type of partitioning",
    ),
    (
        "CREATE TABLE e14 (a double) PARTITION BY HASH(a) PARTITIONS 2",
        1659,
        "Field 'a' is of a not allowed type for this type of partitioning",
    ),
    (
        "CREATE TABLE e2 (a int) PARTITION BY RANGE(b) (PARTITION p0 VALUES LESS THAN (10))",
        1054,
        "Unknown column 'b' in 'partition function'",
    ),
    (
        "CREATE TABLE e3 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (5))",
        1493,
        "VALUES LESS THAN value must be strictly increasing for each partition",
    ),
    (
        "CREATE TABLE e4 (a int) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p0 VALUES LESS THAN (20))",
        1517,
        "Duplicate partition name p0",
    ),
    (
        "CREATE TABLE e5 (a int) PARTITION BY LIST(a) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (1))",
        1495,
        "Multiple definition of same constant in list partitioning",
    ),
    (
        "CREATE TABLE e6 (a int) PARTITION BY HASH(a) PARTITIONS 0",
        1504,
        "Number of partitions = 0 is not an allowed value",
    ),
    (
        "CREATE TABLE e7 (a int) PARTITION BY RANGE(a)",
        1492,
        "For RANGE partitions each partition must be defined",
    ),
    (
        "CREATE TABLE e8 (a int) PARTITION BY LIST(a) (PARTITION p0 VALUES LESS THAN (1))",
        1480,
        "Only RANGE PARTITIONING can use VALUES LESS THAN in partition definition",
    ),
    (
        "CREATE TABLE e10 (a int) PARTITION BY HASH(rand()) PARTITIONS 2",
        1564,
        "This partition function is not allowed",
    ),
    (
        "CREATE TABLE e11 (a int UNIQUE KEY, b int) PARTITION BY HASH(b) PARTITIONS 2",
        8264,
        "Global Index is needed for index 'a', since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption",
    ),
];

/// Every definition TiDB rejects, this node rejects too -- agreement on
/// rejection, which is the strongest thing a node without the feature can
/// claim about them.
///
/// It agrees for the WRONG REASON today (one blanket refusal, not the eleven
/// rules), and that is exactly what makes this a tripwire: the moment
/// `PARTITION BY` is accepted, ten of these eleven start SUCCEEDING unless the
/// validation in Go's `buildTablePartitionInfo` is ported with them, and this
/// test is where that shows up. The errno and message each row carries are
/// what the assertion becomes then.
#[test]
fn every_definition_tidb_rejects_is_rejected_here_too() {
    for (sql, errno, message) in GO_REJECTED {
        let mut session = Session::new();
        assert!(
            session.run(sql).is_err(),
            "TiDB rejects this with {errno} ({message}); this node must not accept it: {sql}"
        );
    }
}

/// `PARTITION BY` written on a `CREATE TABLE IF NOT EXISTS` is refused too:
/// `IF NOT EXISTS` suppresses the "already exists" error, never the
/// admission check.
#[test]
fn if_not_exists_does_not_suppress_the_refusal() {
    let mut session = Session::new();
    assert!(session
        .run("CREATE TABLE IF NOT EXISTS ine (a int) PARTITION BY HASH(a) PARTITIONS 2")
        .is_err());
}
