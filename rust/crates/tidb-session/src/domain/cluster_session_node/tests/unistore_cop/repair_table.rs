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

//! Go `TestRepairTable` and `TestRepairTableWithPartition`
//! (`pkg/ddl/repair_table_test.go`).

use super::*;
use tidb_domainutil::REPAIR_INFO;

/// Go's `domainutil.RepairInfo.SetRepairMode(true)` with the test's deferred
/// reset: the flag, the repair list and the failpoint are process-wide.
struct RepairMode;

impl RepairMode {
    fn enter() -> Self {
        REPAIR_INFO.set_repair_mode(true);
        Self
    }
}

impl Drop for RepairMode {
    fn drop(&mut self) {
        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/infoschema/repairFetchCreateTable");
        REPAIR_INFO.set_repair_table_list(Vec::new());
        REPAIR_INFO.set_repair_mode(false);
    }
}

/// Go's `turnRepairModeAndInit(true)` followed by a CREATE TABLE the
/// `repairFetchCreateTable` failpoint diverts into `RepairInfo`.
fn create_broken(session: &mut Session, list: &str, create: &str) {
    REPAIR_INFO.set_repair_mode(true);
    REPAIR_INFO.set_repair_table_list(vec![list.to_owned()]);
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/infoschema/repairFetchCreateTable", "return(true)").unwrap();
    rows(session, create);
    tidb_failpoint::disable("github.com/pingcap/tidb/pkg/infoschema/repairFetchCreateTable");
}

fn refused(session: &mut Session, sql: &str) -> String {
    let error = session.node_conn().execute_write(sql).unwrap_err();
    format!("{}|{}", error.code, error.message)
}

#[test]
fn admin_repair_table_runs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2121)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");

    // Test repair table when TiDB is not in repair mode.
    rows(
        &mut session,
        "CREATE TABLE t (a int primary key nonclustered, b varchar(10))",
    );
    let repair_t = "admin repair table t CREATE TABLE t (a float primary key, b varchar(5))";
    assert_eq!(
        refused(&mut session, repair_t),
        "8215|Failed to repair table: TiDB is not in REPAIR MODE"
    );

    // Test repair table when the repaired list is empty.
    let _repair_mode = RepairMode::enter();
    assert_eq!(
        refused(&mut session, repair_t),
        "8215|Failed to repair table: repair list is empty"
    );

    // Test repair table when it's database isn't in repairInfo.
    REPAIR_INFO.set_repair_table_list(vec!["test.other_table".to_owned()]);
    assert_eq!(
        refused(&mut session, repair_t),
        "8215|Failed to repair table: database test is not in repair"
    );

    // Test repair table when the table isn't in repairInfo.
    create_broken(
        &mut session,
        "test.other_table",
        "CREATE TABLE other_table (a int, b varchar(1), key using hash(b))",
    );
    assert_eq!(
        refused(&mut session, repair_t),
        "8215|Failed to repair table: table t is not in repair"
    );

    // Test user can't access to the repaired table.
    let hidden = session
        .node_conn()
        .execute("select * from other_table")
        .err()
        .unwrap();
    assert_eq!(
        (hidden.code, hidden.message.as_str()),
        (1146, "Table 'test.other_table' doesn't exist")
    );

    // Test create statement use the same name with what is in repaired.
    assert_eq!(
        refused(&mut session, "CREATE TABLE other_table (a int)"),
        "1103|Incorrect table name 'other_table'%!(EXTRA string=this table is in repair)"
    );

    // Test column lost in repaired table.
    assert_eq!(
        refused(
            &mut session,
            "admin repair table other_table CREATE TABLE other_table (a int, c char(1))"
        ),
        "8215|Failed to repair table: Column c has lost"
    );

    // Test column type should be the same.
    assert_eq!(
        refused(
            &mut session,
            "admin repair table other_table CREATE TABLE other_table (a bigint, b varchar(1), key using hash(b))"
        ),
        "8215|Failed to repair table: Column a type should be the same"
    );

    // Test index lost in repaired table.
    assert_eq!(
        refused(
            &mut session,
            "admin repair table other_table CREATE TABLE other_table (a int unique)"
        ),
        "8215|Failed to repair table: Index a has lost"
    );

    // Test index type should be the same.
    assert_eq!(
        refused(
            &mut session,
            "admin repair table other_table CREATE TABLE other_table (a int, b varchar(2) unique)"
        ),
        "8215|Failed to repair table: Index b type should be the same"
    );

    // Test exclusive repair table succeeds.
    rows(
        &mut session,
        "admin repair table other_table CREATE TABLE other_table (a int)",
    );
    let row = displayed(rows(&mut session, "admin show ddl jobs 1")).remove(0);
    assert_eq!(
        [3, 4, 11].map(|column| row[column].clone()).join("|"),
        "repair table|public|synced"
    );
    // Go's `executor.RepairTable` removes the table from the repair info,
    // and `RemoveFromRepairInfo` leaves REPAIR MODE with its last table.
    assert!(REPAIR_INFO.get_repair_table_list().is_empty());
    assert!(!REPAIR_INFO.in_repair_mode());
    assert!(rows(&mut session, "select * from other_table").is_empty());

    // Test the table name is case-sensitive.
    create_broken(
        &mut session,
        "test.other_table2",
        "CREATE TABLE otHer_tAblE2 (a int, b varchar(1))",
    );
    rows(
        &mut session,
        "admin repair table otHer_tAblE2 CREATE TABLE otHeR_tAbLe (a int, b varchar(2))",
    );
    let repaired = REPAIR_INFO.get_repaired_table_info_by_table_name("test", "other_table2");
    assert!(repaired.0.is_none());
    // The repaired table takes the new name as written. It now shares its
    // lowercased name with `other_table` (Go's repair checks no name), and
    // the name answers the table applied last, as Go's name map does.
    let (schema, repaired_table) = stack
        .factory
        .catalog_snapshot()
        .table_info_by_name("test", "otHeR_tAbLe")
        .unwrap();
    assert_eq!(repaired_table.read().name.original(), "otHeR_tAbLe");
    let names: Vec<String> = schema
        .read()
        .deprecated_tables
        .iter_deref()
        .map(|table| table.read().name.original().to_owned())
        .collect();
    assert!(names.contains(&"otHeR_tAbLe".to_owned()), "{names:?}");
    assert!(!names.contains(&"otHer_tAblE2".to_owned()), "{names:?}");

    // Test memory and system database is not for repair.
    REPAIR_INFO.set_repair_mode(true);
    REPAIR_INFO.set_repair_table_list(vec!["test.xxx".to_owned()]);
    assert_eq!(
        refused(
            &mut session,
            "admin repair table performance_schema.xxx CREATE TABLE yyy (a int)"
        ),
        "8215|Failed to repair table: memory or system database is not for repair"
    );

    // Test the repair detail.
    create_broken(
        &mut session,
        "test.origin",
        "CREATE TABLE origin (a int primary key nonclustered auto_increment, b varchar(10), c int)",
    );
    let origin = REPAIR_INFO
        .get_repaired_table_info_by_table_name("test", "origin")
        .0
        .unwrap();
    let ids = |table: &tidb_model::TableInfo| {
        (
            table.id,
            table.auto_inc_id,
            table
                .columns
                .iter_deref()
                .map(|column| column.read().id)
                .collect::<Vec<_>>(),
            table
                .indices
                .iter_deref()
                .map(|index| index.read().id)
                .collect::<Vec<_>>(),
        )
    };
    let origin_ids = ids(&origin.read());
    assert_eq!((origin_ids.2.len(), origin_ids.3.len()), (3, 1));
    rows(
        &mut session,
        "admin repair table origin CREATE TABLE origin (a int primary key nonclustered auto_increment, b varchar(5), c int)",
    );
    let (_, table) = stack
        .factory
        .catalog_snapshot()
        .table_info_by_name("test", "origin")
        .unwrap();
    assert_eq!(ids(&table.read()), origin_ids);
    assert_eq!(table.read().columns.get(1).unwrap().read().get_flen(), 5);
    assert_eq!(
        displayed(rows(&mut session, "show create table origin"))[0][1],
        "CREATE TABLE `origin` (\n  `a` int NOT NULL AUTO_INCREMENT,\n  `b` varchar(5) DEFAULT NULL,\n  `c` int DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // A column and an index keep the old IDs by NAME, wherever the repair
    // statement lists them.
    create_broken(
        &mut session,
        "test.reordered",
        "CREATE TABLE reordered (a int, b varchar(10), c int, key idx_a (a), key idx_c (c))",
    );
    let named_ids = |table: &tidb_model::TableInfo| {
        let mut ids: Vec<(String, i64)> = table
            .columns
            .iter_deref()
            .map(|column| {
                let column = column.read();
                (column.name.lowercase().to_owned(), column.id)
            })
            .chain(table.indices.iter_deref().map(|index| {
                let index = index.read();
                (index.name.lowercase().to_owned(), index.id)
            }))
            .collect();
        ids.sort();
        ids
    };
    let reordered = REPAIR_INFO
        .get_repaired_table_info_by_table_name("test", "reordered")
        .0
        .unwrap();
    let reordered_ids = named_ids(&reordered.read());
    rows(
        &mut session,
        "admin repair table reordered CREATE TABLE reordered (c int, b varchar(10), a int, key idx_c (c), key idx_a (a))",
    );
    let (_, table) = stack
        .factory
        .catalog_snapshot()
        .table_info_by_name("test", "reordered")
        .unwrap();
    assert_eq!(named_ids(&table.read()), reordered_ids);
    let first = table.read().columns.get(0).unwrap();
    assert_eq!((first.read().name.lowercase(), first.read().id), ("c", 3));
}

/// Go `TestRepairTableWithPartition`.
#[test]
fn admin_repair_table_keeps_partition_ids_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2122)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "drop table if exists origin");
    let _repair_mode = RepairMode::enter();
    REPAIR_INFO.set_repair_table_list(vec!["test.origin".to_owned()]);
    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/infoschema/repairFetchCreateTable", "return(true)").unwrap();
    let range = |name: &str, partitions: &[&str]| {
        format!(
            "create table {name} (a int not null) partition by RANGE(a) ({})",
            partitions
                .iter()
                .map(|partition| {
                    let (name, bound) = partition.split_once(' ').unwrap();
                    format!("partition {name} values less than ({bound})")
                })
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    // Domain reload the tableInfo and add it into repairInfo.
    rows(
        &mut session,
        &range("origin", &["p10 10", "p30 30", "p50 50", "p70 70", "p90 90"]),
    );
    // Some old partition has lost, some partition changed the condition,
    // some partition changed the partition name.
    for (partitions, lost) in [
        (["p10 10", "p30 30", "p50 50", "p90 90", "p100 100"].as_slice(), "p100"),
        (["p10 10", "p20 25", "p50 50", "p90 90"].as_slice(), "p20"),
        (["p10 10", "p30 30", "pNew 50", "p90 90"].as_slice(), "pnew"),
    ] {
        assert_eq!(
            refused(
                &mut session,
                &format!("admin repair table origin {}", range("origin", partitions))
            ),
            format!("8215|Failed to repair table: Partition {lost} has lost")
        );
    }

    let ids = |table: &tidb_model::TableInfo| {
        (
            table.id,
            table
                .columns
                .iter_deref()
                .map(|column| column.read().id)
                .collect::<Vec<_>>(),
            table
                .partition
                .as_ref()
                .unwrap()
                .read()
                .definitions
                .snapshot()
                .iter()
                .map(|definition| definition.id)
                .collect::<Vec<_>>(),
        )
    };
    let origin = |name: &str| {
        ids(&REPAIR_INFO
            .get_repaired_table_info_by_table_name("test", name)
            .0
            .unwrap()
            .read())
    };
    let stored = |name: &str| {
        let (_, table) = stack
            .factory
            .catalog_snapshot()
            .table_info_by_name("test", name)
            .unwrap();
        let ids = ids(&table.read());
        ids
    };
    let (table_id, columns, partitions) = origin("origin");
    rows(
        &mut session,
        &format!(
            "admin repair table origin {}",
            range("origin_rename", &["p10 10", "p30 30", "p50 50", "p90 90"])
        ),
    );
    assert_eq!(columns.len(), 1);
    assert_eq!(
        stored("origin_rename"),
        (
            table_id,
            columns,
            vec![partitions[0], partitions[1], partitions[2], partitions[4]]
        )
    );

    // Test hash partition.
    rows(&mut session, "drop table if exists origin");
    REPAIR_INFO.set_repair_mode(true);
    REPAIR_INFO.set_repair_table_list(vec!["test.origin".to_owned()]);
    rows(
        &mut session,
        "create table origin (a varchar(1), b int not null, c int, key idx(c)) partition by hash(b) partitions 30",
    );
    // Test partition num in repair should be exactly same with old one,
    // other wise will cause partition semantic problem.
    assert_eq!(
        refused(
            &mut session,
            "admin repair table origin create table origin (a varchar(2), b int not null, c int, key idx(c)) partition by hash(b) partitions 20"
        ),
        "8215|Failed to repair table: Hash partition num should be the same"
    );
    let (table_id, _, partitions) = origin("origin");
    rows(
        &mut session,
        "admin repair table origin create table origin (a varchar(3), b int not null, c int, key idx(c)) partition by hash(b) partitions 30",
    );
    let repaired = stored("origin");
    assert_eq!(partitions.len(), 30);
    assert_eq!((repaired.0, repaired.2), (table_id, partitions));
}
