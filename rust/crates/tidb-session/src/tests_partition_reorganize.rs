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

//! `REORGANIZE PARTITION`, `ALTER TABLE ... PARTITION BY` and `REMOVE
//! PARTITIONING`, read back through the session, as `ddl/reorg_partition.test`,
//! `ddl/db_integration.test`, `ddl/db_partition.test` and
//! `globalindex/ddl.test` record.

use crate::tests_support::row_text;
use crate::Session;

fn code(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

fn sorted(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let mut rows = row_text(session.run(sql));
    rows.sort();
    rows
}

fn create_text(session: &mut Session, table: &str) -> String {
    row_text(session.run(&format!("show create table {table}")))[0][1].clone()
}

/// Go `ReorganizePartitions` + `onReorganizePartition`: rows move into the
/// new definitions, indexes keep answering, and the statistics warning
/// follows.
#[test]
fn reorganize_partition_moves_rows_and_keeps_indexes() {
    let mut session = Session::new();
    session
        .run("create table t (a int unsigned not null, b varchar(55), c int, key (b), key (c, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20), partition pMax values less than (maxvalue))")
        .unwrap();
    session
        .run("insert into t values (1,'1',1), (12,'12',21), (23,'23',32), (34,'34',43), (45,'45',54)")
        .unwrap();
    session
        .run("alter table t reorganize partition pMax into (partition p2 values less than (30), partition pMax values less than (maxvalue))")
        .unwrap();
    assert_eq!(
        row_text(session.run("show warnings")),
        vec![vec![
            "Warning",
            "1105",
            "The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"
        ]]
    );
    assert!(create_text(&mut session, "t").contains(
        "(PARTITION `p0` VALUES LESS THAN (10),\n PARTITION `p1` VALUES LESS THAN (20),\n PARTITION `p2` VALUES LESS THAN (30),\n PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"
    ));
    assert_eq!(
        sorted(&mut session, "select a from t partition (p2)"),
        vec![vec!["23"]]
    );
    assert_eq!(
        sorted(&mut session, "select a from t partition (pMax)"),
        vec![vec!["34"], vec!["45"]]
    );
    assert_eq!(
        sorted(&mut session, "select a from t use index (b) where b = '23'"),
        vec![vec!["23"]]
    );
    assert_eq!(
        sorted(&mut session, "select a from t use index (c) where c > 30"),
        vec![vec!["23"], vec!["34"], vec!["45"]]
    );
    session.run("admin check table t").unwrap();
    session
        .run("alter table t reorganize partition p0, p1 into (partition p01 values less than (20))")
        .unwrap();
    assert_eq!(
        sorted(&mut session, "select a from t partition (p01)"),
        vec![vec!["1"], vec!["12"]]
    );
    session.run("admin check table t").unwrap();
}

/// Go's REORGANIZE refusals, each before anything moves.
#[test]
fn reorganize_partition_refuses_what_go_refuses() {
    let mut session = Session::new();
    session
        .run("create table t (id int) partition by range (id) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30))")
        .unwrap();
    session.run("insert into t values (5), (15), (25)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t reorganize partition p0, p2 into (partition p0 values less than (30))"
        ),
        (
            8200,
            "Unsupported REORGANIZE PARTITION of RANGE; not adjacent partitions".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "alter table t reorganize partition px into (partition p0 values less than (10))"
        ),
        (1567, "Incorrect partition name".to_owned())
    );
    assert_eq!(
        code(
            &mut session,
            "alter table t reorganize partition p0, p0 into (partition p0 values less than (10))"
        ),
        (1517, "Duplicate partition name %-.192s".to_owned())
    );
    // The replaced run must end where it ended before.
    assert_eq!(
        code(
            &mut session,
            "alter table t reorganize partition p0 into (partition p0 values less than (5))"
        )
        .0,
        1493
    );
    // The last partition may move its end, but not past a row.
    assert_eq!(
        code(
            &mut session,
            "alter table t reorganize partition p2 into (partition p2 values less than (21))"
        ),
        (1526, "Table has no partition for value 25".to_owned())
    );
    session
        .run("create table h (id int) partition by hash (id) partitions 2")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table h reorganize partition p0 into (partition p0)"
        ),
        (8200, "Unsupported reorganize partition".to_owned())
    );
    assert_eq!(
        sorted(&mut session, "select id from t"),
        vec![vec!["15"], vec!["25"], vec!["5"]]
    );
}

/// Go routes moved rows through the reorganized table, which holds only the
/// new definitions: an untouched DEFAULT partition does not take them.
#[test]
fn reorganized_rows_do_not_fall_into_an_untouched_default_partition() {
    let mut session = Session::new();
    session
        .run("create table t (a int) partition by list (a) (partition p0 values in (0, 4), partition p1 values in (1, null, default), partition p2 values in (2, 7, 10))")
        .unwrap();
    session
        .run("insert into t values (0), (4), (1), (2)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t reorganize partition p0 into (partition p0 values in (0))"
        ),
        (1526, "Table has no partition for value 4".to_owned())
    );
    assert_eq!(
        sorted(&mut session, "select a from t partition (p0)"),
        vec![vec!["0"], vec!["4"]]
    );
}

/// Go `AlterTablePartitioning`: a plain table is partitioned, gets a new
/// table id, and `UPDATE INDEXES` moves a recreated global index to the end.
#[test]
fn partition_by_repartitions_and_recreates_global_indexes() {
    let mut session = Session::new();
    session
        .run("create table t (a int, b int, unique key a (a), key b (b))")
        .unwrap();
    session
        .run("insert into t values (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let table_id = |session: &mut Session| {
        row_text(session.run(
            "select tidb_table_id from information_schema.tables where table_schema = database() and table_name = 't'",
        ))[0][0]
            .clone()
    };
    let before = table_id(&mut session);
    assert_eq!(
        code(&mut session, "alter table t partition by hash (b) partitions 3"),
        (
            8264,
            "Global Index is needed for index 'a', since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption".to_owned()
        )
    );
    session
        .run("alter table t partition by hash (b) partitions 3 update indexes (a global)")
        .unwrap();
    assert_eq!(
        row_text(session.run("show warnings")),
        vec![vec![
            "Warning",
            "1105",
            "The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"
        ]]
    );
    assert_ne!(table_id(&mut session), before);
    let text = create_text(&mut session, "t");
    assert!(
        text.contains("  KEY `b` (`b`),\n  UNIQUE KEY `a` (`a`) /*T![global_index] GLOBAL */\n"),
        "{text}"
    );
    assert!(
        text.ends_with("PARTITION BY HASH (`b`) PARTITIONS 3"),
        "{text}"
    );
    assert_eq!(
        sorted(&mut session, "select a, b from t"),
        vec![vec!["1", "10"], vec!["2", "20"], vec!["3", "30"]]
    );
    assert_eq!(
        sorted(&mut session, "select b from t use index (a) where a = 2"),
        vec![vec!["20"]]
    );
    assert_eq!(code(&mut session, "insert into t values (2, 99)").0, 1062);
    session.run("admin check table t").unwrap();

    session.run("alter table t remove partitioning").unwrap();
    let text = create_text(&mut session, "t");
    assert!(!text.contains("PARTITION BY"), "{text}");
    assert!(!text.contains("GLOBAL"), "{text}");
    assert_eq!(
        sorted(&mut session, "select a from t use index (a) where a >= 2"),
        vec![vec!["2"], vec!["3"]]
    );
    session.run("admin check table t").unwrap();
    assert_eq!(
        code(&mut session, "alter table t remove partitioning"),
        (
            1505,
            "Partition management on a not partitioned table is not possible".to_owned()
        )
    );
}

/// Go's backfill gives a row a new `_tidb_rowid` when an earlier moved row
/// already holds its record key, which EXCHANGE PARTITION makes possible.
#[test]
fn reorganize_reassigns_a_duplicate_row_id() {
    let mut session = Session::new();
    session
        .run("create table t (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
        .unwrap();
    session.run("insert into t values (1)").unwrap();
    session.run("create table s (a int)").unwrap();
    session.run("insert into s values (11)").unwrap();
    session
        .run("alter table t exchange partition p1 with table s")
        .unwrap();
    let row_ids = sorted(&mut session, "select _tidb_rowid from t");
    assert_eq!(
        row_ids[0], row_ids[1],
        "both tables numbered their row from 1"
    );
    session
        .run("alter table t reorganize partition p0, p1 into (partition p01 values less than (20))")
        .unwrap();
    assert_eq!(
        sorted(&mut session, "select a from t partition (p01)"),
        vec![vec!["1"], vec!["11"]]
    );
    let after = sorted(&mut session, "select _tidb_rowid from t");
    assert_eq!(after.len(), 2);
    assert_ne!(after[0], after[1], "{row_ids:?} -> {after:?}");
}

/// Go `checkGlobalIndex` / `checkCreateGlobalIndex`, and the clustered
/// primary key that cannot be global.
#[test]
fn global_indexes_need_a_partitioned_table() {
    let mut session = Session::new();
    let unsupported = (
        8200,
        "Unsupported Global Index on non-partitioned table".to_owned(),
    );
    assert_eq!(
        code(
            &mut session,
            "create table t (a int, b int, unique index idx (a) global)"
        ),
        unsupported
    );
    session.run("create table t (a int, b int)").unwrap();
    assert_eq!(
        code(&mut session, "alter table t add index idx (b) global"),
        unsupported
    );
    assert_eq!(
        code(
            &mut session,
            "create table c (a int, b int, primary key (a) global) partition by hash (a) partitions 2"
        ),
        (
            8200,
            "Unsupported create an index that is both a global index and a clustered index"
                .to_owned()
        )
    );
}

/// Go `generatePartitionDefinitionsFromInterval`: INTERVAL expands into
/// `P_LT_<bound>` definitions, and FIRST / LAST PARTITION drop and add along
/// the same steps.
#[test]
fn interval_partitioning_generates_and_moves_its_ends() {
    let mut session = Session::new();
    session
        .run("create table t (id int unsigned) partition by range (id) interval (100) first partition less than (100) last partition less than (300) maxvalue partition")
        .unwrap();
    assert!(create_text(&mut session, "t").ends_with(
        "(PARTITION `P_LT_100` VALUES LESS THAN (100),\n PARTITION `P_LT_200` VALUES LESS THAN (200),\n PARTITION `P_LT_300` VALUES LESS THAN (300),\n PARTITION `P_MAXVALUE` VALUES LESS THAN (MAXVALUE))"
    ));
    session
        .run("insert into t values (50), (150), (250)")
        .unwrap();
    session
        .run("alter table t first partition less than (200)")
        .unwrap();
    assert_eq!(
        sorted(&mut session, "select id from t"),
        vec![vec!["150"], vec!["250"]]
    );
    assert_eq!(
        code(&mut session, "alter table t last partition less than (500)"),
        (
            8200,
            "Unsupported LAST PARTITION when MAXVALUE partition exists".to_owned()
        )
    );

    session
        .run("create table d (c datetime not null) partition by range columns (c) interval (1 minute) first partition less than ('2024-01-01') last partition less than ('2024-01-01 00:03:00')")
        .unwrap();
    session
        .run("alter table d last partition less than ('2024-01-01 00:05:00')")
        .unwrap();
    let text = create_text(&mut session, "d");
    assert!(
        text.ends_with(
            " PARTITION `P_LT_2024-01-01 00:04:00` VALUES LESS THAN ('2024-01-01 00:04:00'),\n PARTITION `P_LT_2024-01-01 00:05:00` VALUES LESS THAN ('2024-01-01 00:05:00'))"
        ),
        "{text}"
    );
    assert_eq!(
        code(
            &mut session,
            "create table x (id int) partition by range (id) interval (100) first partition less than (100) last partition less than (250)"
        ),
        (
            8200,
            "Unsupported INTERVAL: expr (250) not matching FIRST + n INTERVALs (100 + n * 100)".to_owned()
        )
    );
}
