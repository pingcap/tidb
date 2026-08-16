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

//! Source-backed tests for the `LOCK STATS` / `UNLOCK STATS` executors.
//!
//! Ports `pkg/executor/lockstats/lock_stats_executor_test.go` in full:
//! `TestPopulatePartitionIDAndNames`, `TestPopulateTableAndPartitionIDs`, and
//! their `tInfo` fixture builder.

use std::collections::BTreeMap;

use tidb_ast::CiString;
use tidb_exec::lock_stats_exec::{
    gen_full_partition_name, populate_partition_id_and_names, populate_table_and_partition_ids,
    LockExec, LockStatsError, LockStatsTableName, SchemaResolver, StatsLockHandle, UnlockExec,
};
use tidb_exec::warning_publication::StaticWarningHandler;
use tidb_model::partition::{PartitionDefinition, PartitionInfo};
use tidb_model::table_info::TableInfo;
use tidb_model::{GoShared, GoSharedSlice};
use tidb_stats::StatsLockTable;

/// Port of the Go test's `tInfo` helper: a table with `id`, `tableName`, and
/// partition IDs running `id+1`, `id+2`, ... for the named partitions.
fn t_info(id: i64, table_name: &str, partition_names: &[&str]) -> TableInfo {
    let mut table = TableInfo {
        id,
        name: CiString::new(table_name),
        ..TableInfo::default()
    };
    if !partition_names.is_empty() {
        let definitions: Vec<PartitionDefinition> = partition_names
            .iter()
            .enumerate()
            .map(|(index, partition_name)| PartitionDefinition {
                id: id + 1 + index as i64,
                name: CiString::new(*partition_name),
                ..PartitionDefinition::default()
            })
            .collect();
        table.partition = Some(GoShared::new(PartitionInfo {
            enable: true,
            definitions: GoSharedSlice::from_vec(definitions),
            ..PartitionInfo::default()
        }));
    }
    table
}

/// Stand-in for Go `infoschema.MockInfoSchema`, which files every table under
/// the `test` schema.
struct MockInfoSchema {
    tables: Vec<TableInfo>,
}

impl MockInfoSchema {
    fn new(tables: Vec<TableInfo>) -> Self {
        Self { tables }
    }
}

impl SchemaResolver for MockInfoSchema {
    fn table_by_name(
        &self,
        schema: &CiString,
        name: &CiString,
    ) -> Result<&TableInfo, LockStatsError> {
        if schema.lowercase() != "test" {
            return Err(LockStatsError::TableNotExists(format!(
                "{}.{}",
                schema.lowercase(),
                name.lowercase()
            )));
        }
        self.tables
            .iter()
            .find(|table| table.name.lowercase() == name.lowercase())
            .ok_or_else(|| {
                LockStatsError::TableNotExists(format!(
                    "{}.{}",
                    schema.lowercase(),
                    name.lowercase()
                ))
            })
    }
}

#[test]
fn populate_partition_id_and_names_matches_source() {
    // Source: pkg/executor/lockstats/lock_stats_executor_test.go:26-50
    // (TestPopulatePartitionIDAndNames).
    let fake_info = MockInfoSchema::new(vec![t_info(1, "t1", &["p1", "p2"])]);

    let table = LockStatsTableName::with_partitions("test", "t1", ["p1", "p2"]);

    let (got_tid, got_pid_names) =
        populate_partition_id_and_names(Some(&table), &table.partition_names, &fake_info)
            .expect("the partitioned table must resolve");
    assert_eq!(got_tid, 1);
    assert_eq!(
        got_pid_names,
        BTreeMap::from([(2, "p1".to_owned()), (3, "p2".to_owned())])
    );

    // Empty partition names.
    let error = populate_partition_id_and_names(None, &[], &fake_info)
        .expect_err("an empty partition list is an error");
    assert_eq!(error, LockStatsError::EmptyPartitionList);
    assert_eq!(error.to_string(), "partition list should not be empty");
}

#[test]
fn populate_table_and_partition_ids_matches_source() {
    // Source: pkg/executor/lockstats/lock_stats_executor_test.go:52-81
    // (TestPopulateTableAndPartitionIDs).
    let fake_info = MockInfoSchema::new(vec![
        t_info(1, "t1", &["p1", "p2"]),
        t_info(4, "t2", &[]),
    ]);

    let tables = vec![
        LockStatsTableName::with_partitions("test", "t1", ["p1", "p2"]),
        LockStatsTableName::new("test", "t2"),
    ];

    let table_with_partitions = populate_table_and_partition_ids(&tables, &fake_info)
        .expect("both tables must resolve");
    assert_eq!(table_with_partitions.len(), 2);
    assert_eq!(table_with_partitions[&1].full_name, "test.t1");
    assert_eq!(
        table_with_partitions[&1]
            .partition_info
            .as_ref()
            .expect("t1 is partitioned")[&2],
        "test.t1 partition (p1)"
    );
    assert_eq!(table_with_partitions[&4].full_name, "test.t2");
    // The source's `continue` leaves a non-partitioned table's map nil.
    assert!(table_with_partitions[&4].partition_info.is_none());

    // Empty table list.
    let error = populate_table_and_partition_ids(&[], &fake_info)
        .expect_err("an empty table list is an error");
    assert_eq!(error, LockStatsError::EmptyTableList);
    assert_eq!(error.to_string(), "table list should not be empty");
}

#[test]
fn gen_full_partition_name_lowercases_both_identifiers() {
    // Source: pkg/executor/lockstats/lock_stats_executor.go:157-159.
    let table = LockStatsTableName::new("TeSt", "T1");
    assert_eq!(
        gen_full_partition_name(&table, "p1"),
        "test.t1 partition (p1)"
    );
}

#[test]
fn populate_partition_id_and_names_rejects_a_non_partitioned_table() {
    // Source: pkg/executor/lockstats/lock_stats_executor.go:110-114.
    let fake_info = MockInfoSchema::new(vec![t_info(4, "t2", &[])]);
    let table = LockStatsTableName::with_partitions("test", "t2", ["p1"]);

    let error = populate_partition_id_and_names(Some(&table), &table.partition_names, &fake_info)
        .expect_err("a non-partitioned table cannot have partitions locked");
    assert_eq!(error, LockStatsError::NotPartitionTable("test.t2".to_owned()));
    assert_eq!(error.to_string(), "table test.t2 is not a partition table");
}

#[test]
fn populate_partition_id_and_names_rejects_an_unknown_partition() {
    // Source: pkg/table/tables/partition.go:2145-2154
    // (tables.FindPartitionByName), reached from
    // lock_stats_executor.go:117-123.
    let fake_info = MockInfoSchema::new(vec![t_info(1, "T1", &["p1"])]);
    let table = LockStatsTableName::with_partitions("test", "t1", ["nosuch"]);

    let error = populate_partition_id_and_names(Some(&table), &table.partition_names, &fake_info)
        .expect_err("an unknown partition is an error");
    assert_eq!(
        error.to_string(),
        "Unknown partition 'nosuch' in table 'T1'",
        "Go names the table in its original case here"
    );
}

/// Records what the executors handed the narrowed statistics handle.
#[derive(Default)]
struct RecordingHandle {
    message: String,
    calls: std::cell::RefCell<Vec<String>>,
}

impl RecordingHandle {
    fn with_message(message: &str) -> Self {
        Self {
            message: message.to_owned(),
            calls: std::cell::RefCell::new(Vec::new()),
        }
    }

    fn record(&self, call: String) -> Result<String, LockStatsError> {
        self.calls.borrow_mut().push(call);
        Ok(self.message.clone())
    }
}

impl StatsLockHandle for RecordingHandle {
    fn lock_partitions(
        &self,
        table_id: i64,
        table_name: &str,
        partition_names: &BTreeMap<i64, String>,
    ) -> Result<String, LockStatsError> {
        self.record(format!(
            "lock_partitions({table_id}, {table_name}, {partition_names:?})"
        ))
    }

    fn lock_tables(
        &self,
        tables: &BTreeMap<i64, StatsLockTable>,
    ) -> Result<String, LockStatsError> {
        self.record(format!("lock_tables({:?})", tables.keys().collect::<Vec<_>>()))
    }

    fn remove_locked_partitions(
        &self,
        table_id: i64,
        table_name: &str,
        partition_names: &BTreeMap<i64, String>,
    ) -> Result<String, LockStatsError> {
        self.record(format!(
            "remove_locked_partitions({table_id}, {table_name}, {partition_names:?})"
        ))
    }

    fn remove_locked_tables(
        &self,
        tables: &BTreeMap<i64, StatsLockTable>,
    ) -> Result<String, LockStatsError> {
        self.record(format!(
            "remove_locked_tables({:?})",
            tables.keys().collect::<Vec<_>>()
        ))
    }
}

#[test]
fn lock_exec_routes_partitions_and_publishes_the_skipped_message() {
    // Source: pkg/executor/lockstats/lock_stats_executor.go:42-84.
    let fake_info = MockInfoSchema::new(vec![t_info(1, "T1", &["p1", "p2"])]);
    let handle = RecordingHandle::with_message("skip locking t1");
    let warnings = StaticWarningHandler::default();

    let executor = LockExec::new(vec![LockStatsTableName::with_partitions(
        "TeSt",
        "T1",
        ["p1"],
    )]);
    assert!(executor.only_lock_partitions());
    executor
        .next(Some(&handle), &fake_info, &warnings)
        .expect("locking one partition must succeed");

    assert_eq!(
        handle.calls.borrow().as_slice(),
        [r#"lock_partitions(1, test.t1, {2: "p1"})"#.to_owned()],
        "LOCK STATS names the table in lowercase"
    );
    let published = warnings.warnings_snapshot();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].message, "skip locking t1");
}

#[test]
fn lock_exec_routes_whole_tables_and_stays_quiet_without_a_message() {
    // Source: pkg/executor/lockstats/lock_stats_executor.go:69-82.
    let fake_info = MockInfoSchema::new(vec![t_info(1, "t1", &["p1"]), t_info(4, "t2", &[])]);
    let handle = RecordingHandle::default();
    let warnings = StaticWarningHandler::default();

    let executor = LockExec::new(vec![
        LockStatsTableName::new("test", "t1"),
        LockStatsTableName::new("test", "t2"),
    ]);
    assert!(!executor.only_lock_partitions());
    executor
        .next(Some(&handle), &fake_info, &warnings)
        .expect("locking two tables must succeed");

    assert_eq!(
        handle.calls.borrow().as_slice(),
        ["lock_tables([1, 4])".to_owned()]
    );
    assert!(warnings.warnings_snapshot().is_empty());
}

#[test]
fn unlock_exec_names_the_table_in_its_original_case() {
    // Source: pkg/executor/lockstats/unlock_stats_executor.go:51-62. The
    // unlock path formats the table name from `Schema.O`/`Name.O` where the
    // lock path uses `.L`.
    let fake_info = MockInfoSchema::new(vec![t_info(1, "T1", &["p1"])]);
    let handle = RecordingHandle::default();
    let warnings = StaticWarningHandler::default();

    let executor = UnlockExec::new(vec![LockStatsTableName::with_partitions(
        "TeSt",
        "T1",
        ["p1"],
    )]);
    assert!(executor.only_unlock_partitions());
    executor
        .next(Some(&handle), &fake_info, &warnings)
        .expect("unlocking one partition must succeed");

    assert_eq!(
        handle.calls.borrow().as_slice(),
        [r#"remove_locked_partitions(1, TeSt.T1, {2: "p1"})"#.to_owned()]
    );
}

#[test]
fn both_executors_reject_a_nil_handle_and_an_empty_table_list() {
    // Source: lock_stats_executor.go:43-50 and
    // unlock_stats_executor.go:40-47, including the unlock message's
    // trailing space.
    let fake_info = MockInfoSchema::new(Vec::new());
    let handle = RecordingHandle::default();
    let warnings = StaticWarningHandler::default();

    let lock = LockExec::default();
    assert_eq!(
        lock.next(None, &fake_info, &warnings)
            .expect_err("a nil handle is an error")
            .to_string(),
        "Lock Stats: handle is nil"
    );
    assert_eq!(
        lock.next(Some(&handle), &fake_info, &warnings)
            .expect_err("an empty table list is an error")
            .to_string(),
        "Lock Stats: table should not empty"
    );

    let unlock = UnlockExec::default();
    assert_eq!(
        unlock
            .next(None, &fake_info, &warnings)
            .expect_err("a nil handle is an error")
            .to_string(),
        "Unlock Stats: handle is nil"
    );
    assert_eq!(
        unlock
            .next(Some(&handle), &fake_info, &warnings)
            .expect_err("an empty table list is an error")
            .to_string(),
        "Unlock Stats: table should not empty ",
        "the source's trailing space is preserved"
    );

    assert!(handle.calls.borrow().is_empty());
    lock.open();
    lock.close();
    unlock.open();
    unlock.close();
}
