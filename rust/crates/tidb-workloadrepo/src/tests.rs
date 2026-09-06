// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{Local, TimeZone, Timelike};
use tidb_owner::{Context, MockManager};

use super::*;

#[derive(Default)]
struct MemoryStore {
    values: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    watchers: Mutex<Vec<Arc<dyn Fn(Vec<u8>) + Send + Sync>>>,
}

impl RepositoryStore for MemoryStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    fn create(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        let mut values = self.values.lock().unwrap();
        if values.contains_key(key) {
            return Ok(false);
        }
        values.insert(key.to_vec(), value.to_vec());
        drop(values);
        for watcher in self.watchers.lock().unwrap().iter() {
            watcher(value.to_vec());
        }
        Ok(true)
    }

    fn compare_value_and_put(
        &self,
        key: &[u8],
        expected: &[u8],
        value: &[u8],
    ) -> Result<bool, String> {
        let mut values = self.values.lock().unwrap();
        if values.get(key).map(Vec::as_slice) != Some(expected) {
            return Ok(false);
        }
        values.insert(key.to_vec(), value.to_vec());
        drop(values);
        for watcher in self.watchers.lock().unwrap().iter() {
            watcher(value.to_vec());
        }
        Ok(true)
    }

    fn watch(
        &self,
        _key: &[u8],
        callback: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    ) -> Result<Box<dyn Send>, String> {
        self.watchers.lock().unwrap().push(callback);
        Ok(Box::new(()))
    }
}

#[derive(Clone, Debug)]
struct Capture {
    table: String,
    args: Vec<SqlArg>,
    at: Instant,
}

#[derive(Default)]
struct MemoryDatabase {
    schema_exists: bool,
    tables: HashMap<String, TableInfo>,
    max_snap_id: u64,
    captures: Vec<Capture>,
    snapshot_updates: Vec<(u64, Vec<SqlArg>)>,
}

#[derive(Clone, Default)]
struct MemorySession {
    database: Arc<Mutex<MemoryDatabase>>,
}

fn names_in(sql: &str) -> Vec<String> {
    sql.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| {
            token.len() == 9
                && token.starts_with('p')
                && token[1..].bytes().all(|byte| byte.is_ascii_digit())
        })
        .map(ToOwned::to_owned)
        .collect()
}

fn table_after(sql: &str, marker: &str) -> Option<String> {
    let suffix = sql.get(sql.find(marker)? + marker.len()..)?;
    suffix
        .split(|character: char| character.is_ascii_whitespace() || matches!(character, '`' | '('))
        .find(|token| !token.is_empty())
        .map(|token| token.trim_matches('`').to_ascii_uppercase())
}

impl RepositorySession for MemorySession {
    fn execute(&mut self, sql: &str, args: &[SqlArg]) -> Result<Vec<Vec<SqlArg>>, String> {
        let upper = sql.to_ascii_uppercase();
        let mut database = self.database.lock().unwrap();
        if sql.starts_with("SELECT MAX") {
            return Ok(vec![vec![if database.max_snap_id == 0 {
                SqlArg::Null
            } else {
                SqlArg::UInt(database.max_snap_id)
            }]]);
        }
        if upper.starts_with("CREATE DATABASE") {
            database.schema_exists = true;
            return Ok(Vec::new());
        }
        if upper.starts_with("CREATE TABLE") {
            let table = upper
                .split("`WORKLOAD_SCHEMA`.`")
                .nth(1)
                .and_then(|suffix| suffix.split('`').next())
                .ok_or_else(|| format!("cannot parse create table: {sql}"))?
                .to_owned();
            database.tables.entry(table).or_insert_with(|| TableInfo {
                columns: vec![Column {
                    name: "ID".to_owned(),
                    type_desc: "BIGINT".to_owned(),
                    comment: String::new(),
                }],
                partitions: names_in(sql),
            });
            return Ok(Vec::new());
        }
        if upper.starts_with("ALTER TABLE") && upper.contains(" ADD PARTITION ") {
            let table = table_after(&upper, "`WORKLOAD_SCHEMA`.`")
                .ok_or_else(|| format!("cannot parse alter table: {sql}"))?;
            let info = database
                .tables
                .get_mut(&table)
                .ok_or_else(|| format!("unknown table {table}"))?;
            for partition in names_in(sql) {
                if !info.partitions.contains(&partition) {
                    info.partitions.push(partition);
                }
            }
            return Ok(Vec::new());
        }
        if upper.starts_with("ALTER TABLE") && upper.contains(" DROP PARTITION ") {
            let table = table_after(&upper, "`WORKLOAD_SCHEMA`.`")
                .ok_or_else(|| format!("cannot parse alter table: {sql}"))?;
            let partition = names_in(sql)
                .into_iter()
                .next()
                .ok_or_else(|| format!("cannot parse partition: {sql}"))?;
            let info = database
                .tables
                .get_mut(&table)
                .ok_or_else(|| format!("unknown table {table}"))?;
            if info.partitions.len() == 1 {
                return Err("cannot remove all partitions".to_owned());
            }
            info.partitions.retain(|name| name != &partition);
            return Ok(Vec::new());
        }
        if upper.starts_with("INSERT") {
            let table = upper
                .split("`WORKLOAD_SCHEMA`.`")
                .nth(1)
                .and_then(|suffix| suffix.split('`').next())
                .ok_or_else(|| format!("cannot parse insert: {sql}"))?
                .to_owned();
            if table == HIST_SNAPSHOTS {
                if let Some(SqlArg::UInt(snap_id)) = args.first() {
                    database.max_snap_id = database.max_snap_id.max(*snap_id);
                }
            } else {
                database.captures.push(Capture {
                    table,
                    args: args.to_vec(),
                    at: Instant::now(),
                });
            }
            return Ok(Vec::new());
        }
        if upper.starts_with("UPDATE") && upper.contains(HIST_SNAPSHOTS) {
            if let Some(SqlArg::UInt(snap_id)) = args.last() {
                database.snapshot_updates.push((*snap_id, args.to_vec()));
            }
        }
        Ok(Vec::new())
    }

    fn schema_exists(&self, _schema: &str) -> bool {
        self.database.lock().unwrap().schema_exists
    }

    fn table_info(&mut self, schema: &str, table: &str) -> Result<TableInfo, String> {
        if schema.eq_ignore_ascii_case("INFORMATION_SCHEMA") {
            return Ok(TableInfo {
                columns: vec![Column {
                    name: "ID".to_owned(),
                    type_desc: "BIGINT".to_owned(),
                    comment: String::new(),
                }],
                partitions: Vec::new(),
            });
        }
        self.database
            .lock()
            .unwrap()
            .tables
            .get(&table.to_ascii_uppercase())
            .cloned()
            .ok_or_else(|| format!("unknown table {schema}.{table}"))
    }
}

#[derive(Clone, Default)]
struct MemoryPool {
    database: Arc<Mutex<MemoryDatabase>>,
}

impl SessionPool for MemoryPool {
    fn get(&self) -> Result<Box<dyn RepositorySession>, String> {
        Ok(Box::new(MemorySession {
            database: Arc::clone(&self.database),
        }))
    }
}

fn owner_factory(id: &'static str) -> OwnerFactory {
    Arc::new(move |key, _prompt| {
        Arc::new(MockManager::new(Context::background(), id, Some(id), key))
    })
}

fn test_worker(id: &str, cluster: &str, store: Arc<MemoryStore>, pool: MemoryPool) -> Arc<Worker> {
    let owner_id = id.to_owned();
    let store_id = cluster.to_owned();
    let factory: OwnerFactory = Arc::new(move |key, _prompt| {
        Arc::new(MockManager::new(
            Context::background(),
            owner_id.clone(),
            Some(&store_id),
            key,
        ))
    });
    Worker::new(Some(store), Some(Arc::new(pool)), Some(factory), id)
}

fn test_cluster() -> (String, Arc<MemoryStore>, MemoryPool) {
    static NEXT_CLUSTER: AtomicU64 = AtomicU64::new(1);
    let cluster = format!(
        "workloadrepo-test-{}",
        NEXT_CLUSTER.fetch_add(1, AtomicOrdering::Relaxed)
    );
    (
        cluster,
        Arc::new(MemoryStore::default()),
        MemoryPool::default(),
    )
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        predicate(),
        "condition did not become true within {timeout:?}"
    );
}

fn date(offset: i64) -> String {
    generate_partition_name(
        Local::now()
            .date_naive()
            .checked_add_signed(chrono::Duration::days(offset))
            .unwrap(),
    )
}

fn configured_worker(max_snap_id: u64) -> Arc<Worker> {
    let pool = MemoryPool::default();
    pool.database.lock().unwrap().schema_exists = true;
    pool.database.lock().unwrap().max_snap_id = max_snap_id;
    Worker::new(
        Some(Arc::new(MemoryStore::default())),
        Some(Arc::new(pool)),
        Some(owner_factory("worker")),
        "worker",
    )
}

// Transcreated from Go `TestCalcNextTick`.
#[test]
fn test_calc_next_tick() {
    let before_midnight = Local
        .with_ymd_and_hms(2024, 12, 6, 23, 59, 59)
        .single()
        .unwrap()
        .with_nanosecond(999_999_999)
        .unwrap();
    assert_eq!(
        calc_next_tick(before_midnight),
        Duration::from_secs(2 * 60 * 60) + Duration::from_nanos(1)
    );
    let at_midnight = Local
        .with_ymd_and_hms(2024, 12, 7, 0, 0, 0)
        .single()
        .unwrap();
    assert_eq!(
        calc_next_tick(at_midnight),
        Duration::from_secs(2 * 60 * 60)
    );
    let after_two = Local
        .with_ymd_and_hms(2024, 12, 7, 2, 0, 0)
        .single()
        .unwrap()
        .with_nanosecond(1)
        .unwrap();
    assert_eq!(
        calc_next_tick(after_two),
        Duration::from_secs(24 * 60 * 60) - Duration::from_nanos(1)
    );
    let before_two = Local
        .with_ymd_and_hms(2024, 12, 7, 1, 59, 59)
        .single()
        .unwrap()
        .with_nanosecond(999_999_999)
        .unwrap();
    assert_eq!(calc_next_tick(before_two), Duration::from_nanos(1));

    let pacific = chrono_tz::America::Los_Angeles;
    let spring_forward = NaiveDate::from_ymd_opt(2024, 3, 10).unwrap();
    let normalized = date_at_two(&pacific, spring_forward);
    assert_eq!(normalized.to_rfc3339(), "2024-03-10T01:00:00-08:00");
}

#[test]
fn test_recover_snap_id() {
    let worker = configured_worker(41);
    worker.state.lock().unwrap().enabled = true;
    worker.take_snapshot().unwrap();
    assert_eq!(
        worker.store.as_ref().unwrap().get(SNAP_ID_KEY).unwrap(),
        Some(b"42".to_vec())
    );
}

// Transcreated from Go `TestRaceToCreateTablesWorker`.
#[test]
fn test_race_to_create_tables_worker() {
    let (cluster, store, pool) = test_cluster();
    let first = test_worker("worker1", &cluster, Arc::clone(&store), pool.clone());
    let second = test_worker("worker2", &cluster, store, pool.clone());
    let now = Local::now();
    std::thread::scope(|scope| {
        scope.spawn(|| first.create_all_tables(now).unwrap());
        scope.spawn(|| second.create_all_tables(now).unwrap());
    });
    let database = pool.database.lock().unwrap();
    assert_eq!(database.tables.len(), workload_tables().len());
    assert!(database
        .tables
        .values()
        .all(|table| table.partitions == [date(1), date(2)]));
}

// Transcreated from Go `TestAddNewPartitionsOnStart`.
#[test]
fn test_add_new_partitions_on_start() {
    let (cluster, store, pool) = test_cluster();
    let worker = test_worker("worker", &cluster, store, pool.clone());
    let now = Local::now();
    worker.create_all_tables(now).unwrap();
    assert!(worker.check_tables_exist(now));
    assert!(pool
        .database
        .lock()
        .unwrap()
        .tables
        .values()
        .all(|table| table.partitions == [date(1), date(2)]));
}

// Transcreated from Go `TestCreatePartition`.
#[test]
fn test_create_partition() {
    let cases = [
        (vec![date(0)], vec![date(0), date(1), date(2)], false),
        (vec![date(1)], vec![date(1), date(2)], false),
        (vec![date(2)], vec![date(2)], true),
        (vec![date(1), date(2)], vec![date(1), date(2)], true),
        (vec![date(3)], vec![date(3)], true),
        (vec![date(1), date(4)], vec![date(1), date(4)], true),
    ];
    for (initial, expected, skipped) in cases {
        let (cluster, store, pool) = test_cluster();
        pool.database.lock().unwrap().schema_exists = true;
        pool.database.lock().unwrap().tables.insert(
            "HIST_PROCESSLIST".to_owned(),
            TableInfo {
                columns: Vec::new(),
                partitions: initial,
            },
        );
        let worker = test_worker("worker", &cluster, store, pool.clone());
        *worker.tables.lock().unwrap() =
            vec![RepositoryTable::source("PROCESSLIST", TableType::Sampling)];
        let before = pool
            .database
            .lock()
            .unwrap()
            .tables
            .get("HIST_PROCESSLIST")
            .unwrap()
            .partitions
            .clone();
        worker.create_all_partitions(Local::now()).unwrap();
        let after = pool
            .database
            .lock()
            .unwrap()
            .tables
            .get("HIST_PROCESSLIST")
            .unwrap()
            .partitions
            .clone();
        assert_eq!(after, expected);
        assert_eq!(before == after, skipped);
    }
}

// Transcreated from Go `TestDropOldPartitions`.
#[test]
fn test_drop_old_partitions() {
    let cases = [
        (
            vec![date(0), date(1), date(2)],
            1,
            vec![date(0), date(1), date(2)],
            false,
        ),
        (
            vec![date(-1), date(1), date(2)],
            1,
            vec![date(1), date(2)],
            false,
        ),
        (
            vec![date(-2), date(-1), date(0), date(1), date(2)],
            1,
            vec![date(0), date(1), date(2)],
            false,
        ),
        (
            vec![date(-1), date(0), date(1), date(2)],
            2,
            vec![date(-1), date(0), date(1), date(2)],
            false,
        ),
        (
            vec![date(-2), date(-1), date(2)],
            2,
            vec![date(-1), date(2)],
            false,
        ),
        (vec![date(0)], 2, vec![date(0)], false),
        (vec![date(-1)], 1, vec![date(-1)], true),
    ];
    for (initial, retention, expected, should_error) in cases {
        let (cluster, store, pool) = test_cluster();
        pool.database.lock().unwrap().schema_exists = true;
        pool.database.lock().unwrap().tables.insert(
            "HIST_PROCESSLIST".to_owned(),
            TableInfo {
                columns: Vec::new(),
                partitions: initial,
            },
        );
        let worker = test_worker("worker", &cluster, store, pool.clone());
        *worker.tables.lock().unwrap() =
            vec![RepositoryTable::source("PROCESSLIST", TableType::Sampling)];
        let result = worker.drop_old_partitions(Local::now(), retention);
        assert_eq!(result.is_err(), should_error);
        assert_eq!(
            pool.database
                .lock()
                .unwrap()
                .tables
                .get("HIST_PROCESSLIST")
                .unwrap()
                .partitions,
            expected
        );
    }
}

fn worker_is_owner(worker: &Worker) -> bool {
    worker
        .state
        .lock()
        .unwrap()
        .owner
        .as_ref()
        .is_some_and(|owner| owner.is_owner())
}

// Transcreated from Go `TestGlobalWorker`.
#[test]
fn test_global_worker() {
    let (cluster, store, pool) = test_cluster();
    let worker = test_worker("worker", &cluster, store, pool.clone());
    assert_eq!(
        worker.take_snapshot().unwrap_err(),
        "Workload repository is not enabled"
    );
    worker.set_sampling_interval("1").unwrap();
    worker.set_snapshot_interval("3600").unwrap();
    worker.set_repository_dest("table").unwrap();
    wait_until(Duration::from_secs(3), || {
        worker.check_tables_exist(Local::now())
    });
    wait_until(Duration::from_secs(3), || {
        pool.database
            .lock()
            .unwrap()
            .captures
            .iter()
            .any(|capture| capture.table == "HIST_MEMORY_USAGE")
    });
    worker.take_snapshot().unwrap();
    wait_until(Duration::from_secs(2), || {
        pool.database.lock().unwrap().max_snap_id > 0
    });
    worker.stop();
    assert!(worker.take_snapshot().is_err());
}

// Transcreated from Go `TestAdminWorkloadRepo`.
#[test]
fn test_admin_workload_repo() {
    let (cluster, store, pool) = test_cluster();
    let worker = test_worker("worker", &cluster, store, pool.clone());
    assert!(worker.take_snapshot().is_err());
    worker.set_sampling_interval("600").unwrap();
    worker.set_snapshot_interval("5000").unwrap();
    worker.start().unwrap();
    wait_until(Duration::from_secs(3), || {
        worker.check_tables_exist(Local::now())
    });
    worker.take_snapshot().unwrap();
    wait_until(Duration::from_secs(2), || {
        let database = pool.database.lock().unwrap();
        database.max_snap_id == 1 && !database.snapshot_updates.is_empty()
    });
    worker.stop();
    assert_eq!(
        worker.take_snapshot().unwrap_err(),
        "Workload repository is not enabled"
    );
}

// Transcreated from Go `TestSamplingTimingWorker`.
#[test]
fn test_sampling_timing_worker() {
    let (cluster, store, pool) = test_cluster();
    let worker = test_worker("worker", &cluster, store, pool.clone());
    worker.set_sampling_interval("1").unwrap();
    worker.set_snapshot_interval("3600").unwrap();
    worker.start().unwrap();
    wait_until(Duration::from_secs(5), || {
        pool.database
            .lock()
            .unwrap()
            .captures
            .iter()
            .filter(|capture| capture.table == "HIST_MEMORY_USAGE")
            .count()
            >= 3
    });
    let first = pool
        .database
        .lock()
        .unwrap()
        .captures
        .iter()
        .filter(|capture| capture.table == "HIST_MEMORY_USAGE")
        .cloned()
        .collect::<Vec<_>>();
    assert!(first
        .windows(2)
        .all(|pair| { pair[1].at.duration_since(pair[0].at) >= Duration::from_millis(900) }));
    assert!(first
        .iter()
        .all(|capture| { capture.args == [SqlArg::String("worker".to_owned())] }));

    worker.set_sampling_interval("2").unwrap();
    let before = first.len();
    wait_until(Duration::from_secs(6), || {
        pool.database
            .lock()
            .unwrap()
            .captures
            .iter()
            .filter(|capture| capture.table == "HIST_MEMORY_USAGE")
            .count()
            >= before + 2
    });
    let captures = pool
        .database
        .lock()
        .unwrap()
        .captures
        .iter()
        .filter(|capture| capture.table == "HIST_MEMORY_USAGE")
        .cloned()
        .collect::<Vec<_>>();
    assert!(captures[before..]
        .windows(2)
        .all(|pair| { pair[1].at.duration_since(pair[0].at) >= Duration::from_millis(1_800) }));
    worker.stop();
}

// Transcreated from Go `TestSnapshotTimingWorker`.
#[test]
fn test_snapshot_timing_worker() {
    let (cluster, store, pool) = test_cluster();
    let worker = test_worker("worker", &cluster, store, pool.clone());
    let mut tables = workload_tables();
    tables[1] = RepositoryTable {
        schema: "INFORMATION_SCHEMA".to_owned(),
        table: "MEMORY_USAGE".to_owned(),
        table_type: TableType::Snapshot,
        dest_table: "HIST_MEMORY_USAGE2".to_owned(),
        where_clause: String::new(),
        create_stmt: String::new(),
        insert_stmt: String::new(),
    };
    tables.push(RepositoryTable {
        dest_table: "HIST_MEMORY_USAGE3".to_owned(),
        ..tables[1].clone()
    });
    *worker.tables.lock().unwrap() = tables;
    worker.set_sampling_interval("600").unwrap();
    worker.set_snapshot_interval("1").unwrap();
    worker.start().unwrap();
    wait_until(Duration::from_secs(5), || {
        let database = pool.database.lock().unwrap();
        ["HIST_MEMORY_USAGE2", "HIST_MEMORY_USAGE3"]
            .into_iter()
            .all(|table| {
                database
                    .captures
                    .iter()
                    .filter(|capture| capture.table == table)
                    .count()
                    >= 3
            })
    });
    let database = pool.database.lock().unwrap();
    for table in ["HIST_MEMORY_USAGE2", "HIST_MEMORY_USAGE3"] {
        let captures = database
            .captures
            .iter()
            .filter(|capture| capture.table == table)
            .collect::<Vec<_>>();
        assert_eq!(captures.len(), 3);
        for (index, capture) in captures.iter().enumerate() {
            assert_eq!(
                capture.args,
                [
                    SqlArg::UInt((index + 1) as u64),
                    SqlArg::String("worker".to_owned()),
                ]
            );
        }
    }
    drop(database);
    worker.stop();
}

// Transcreated from Go `TestMultipleWorker`.
#[test]
fn test_multiple_worker() {
    let (cluster, store, pool) = test_cluster();
    let first = test_worker("worker1", &cluster, Arc::clone(&store), pool.clone());
    let second = test_worker("worker2", &cluster, store, pool.clone());
    for worker in [&first, &second] {
        worker.set_sampling_interval("1").unwrap();
        worker.set_snapshot_interval("3600").unwrap();
    }
    first.start().unwrap();
    wait_until(Duration::from_secs(4), || worker_is_owner(&first));
    second.start().unwrap();
    wait_until(Duration::from_secs(4), || {
        let database = pool.database.lock().unwrap();
        ["worker1", "worker2"].into_iter().all(|worker| {
            database.captures.iter().any(|capture| {
                capture.table == "HIST_MEMORY_USAGE"
                    && capture.args == [SqlArg::String(worker.to_owned())]
            })
        })
    });
    assert!(worker_is_owner(&first));
    assert!(!worker_is_owner(&second));

    first.stop();
    wait_until(Duration::from_secs(4), || worker_is_owner(&second));
    first.start().unwrap();
    wait_until(Duration::from_secs(4), || {
        first.enabled() && !worker_is_owner(&first) && worker_is_owner(&second)
    });
    second.stop();
    wait_until(Duration::from_secs(4), || worker_is_owner(&first));
    first.stop();
}

// Transcreated from Go `TestHouseKeeperThread`.
#[test]
fn test_house_keeper_thread() {
    let (cluster, store, pool) = test_cluster();
    pool.database.lock().unwrap().schema_exists = true;
    pool.database.lock().unwrap().tables.insert(
        "HIST_PROCESSLIST".to_owned(),
        TableInfo {
            columns: Vec::new(),
            partitions: vec![date(0), date(1)],
        },
    );
    pool.database.lock().unwrap().tables.insert(
        "HIST_DATA_LOCK_WAITS".to_owned(),
        TableInfo {
            columns: Vec::new(),
            partitions: vec![date(-2), date(-1), date(0), date(1)],
        },
    );
    let worker = test_worker("worker", &cluster, store, pool.clone());
    *worker.tables.lock().unwrap() = vec![
        RepositoryTable::source("PROCESSLIST", TableType::Sampling),
        RepositoryTable::source("DATA_LOCK_WAITS", TableType::Sampling),
    ];
    worker.set_retention_days("2").unwrap();
    let owner =
        (worker.owner_factory.as_ref().unwrap())("/tidb/workloadrepo/owner", "workloadrepo");
    owner.campaign_owner(&[]).unwrap();
    wait_until(Duration::from_secs(3), || owner.is_owner());
    let stop = Arc::new(AtomicBool::new(false));
    let runner = {
        let worker = Arc::clone(&worker);
        let owner = Arc::clone(&owner);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            worker.run_housekeeping_loop_with(owner.as_ref(), stop.as_ref(), |_| {
                Duration::from_millis(50)
            });
        })
    };
    wait_until(Duration::from_secs(2), || {
        let database = pool.database.lock().unwrap();
        database.tables["HIST_PROCESSLIST"].partitions == [date(0), date(1), date(2)]
            && database.tables["HIST_DATA_LOCK_WAITS"].partitions
                == [date(-1), date(0), date(1), date(2)]
    });
    worker.set_retention_days("1").unwrap();
    wait_until(Duration::from_secs(2), || {
        pool.database.lock().unwrap().tables["HIST_DATA_LOCK_WAITS"].partitions
            == [date(0), date(1), date(2)]
    });
    stop.store(true, Ordering::Release);
    runner.join().unwrap();
    owner.close();
}

fn worker_owner(worker: &Worker) -> Arc<dyn Manager> {
    Arc::clone(
        worker
            .state
            .lock()
            .unwrap()
            .owner
            .as_ref()
            .expect("started worker has an owner manager"),
    )
}

// Transcreated from Go `TestOwnerRandomDown`. The source repeats these three
// loss modes three times; one full cycle covers the distinct worker behavior
// without making the unit suite spend another half minute on identical mock
// election sleeps.
#[test]
fn test_owner_random_down() {
    let (cluster, store, pool) = test_cluster();
    let workers = (0..3)
        .map(|index| {
            let worker = test_worker(
                &format!("worker{index}"),
                &cluster,
                Arc::clone(&store),
                pool.clone(),
            );
            worker.set_sampling_interval("600").unwrap();
            worker.set_snapshot_interval("1").unwrap();
            worker.start().unwrap();
            worker
        })
        .collect::<Vec<_>>();
    wait_until(Duration::from_secs(5), || {
        workers.iter().any(|worker| worker_is_owner(worker))
            && workers[0].check_tables_exist(Local::now())
            && pool.database.lock().unwrap().max_snap_id > 0
    });

    for mode in 0..3 {
        let old_index = workers
            .iter()
            .position(|worker| worker_is_owner(worker))
            .expect("one worker owns the category");
        let previous_snap_id = pool.database.lock().unwrap().max_snap_id;
        let old_owner = worker_owner(&workers[old_index]);
        match mode {
            0 => workers[old_index].stop(),
            1 => old_owner.campaign_cancel(),
            2 => old_owner.resign_owner(&Context::background()).unwrap(),
            _ => unreachable!(),
        }
        wait_until(Duration::from_secs(5), || {
            workers
                .iter()
                .enumerate()
                .any(|(index, worker)| index != old_index && worker_is_owner(worker))
        });
        wait_until(Duration::from_secs(4), || {
            pool.database.lock().unwrap().max_snap_id > previous_snap_id
        });
        match mode {
            0 => workers[old_index].start().unwrap(),
            1 => old_owner.campaign_owner(&[3]).unwrap(),
            2 => {}
            _ => unreachable!(),
        }
    }
    for worker in workers {
        worker.stop();
    }
}

// Transcreated from Go `TestStoppingAndRestartingWorker`.
#[test]
fn test_stopping_and_restarting_worker() {
    let (cluster, store, pool) = test_cluster();
    let worker = test_worker("worker", &cluster, store, pool.clone());
    worker.set_sampling_interval("1").unwrap();
    worker.set_snapshot_interval("1").unwrap();
    worker.set_repository_dest("table").unwrap();
    wait_until(Duration::from_secs(4), || {
        let database = pool.database.lock().unwrap();
        !database.captures.is_empty() && database.max_snap_id > 0
    });
    worker.stop();
    let stopped = {
        let database = pool.database.lock().unwrap();
        (database.captures.len(), database.max_snap_id)
    };
    std::thread::sleep(Duration::from_millis(1_200));
    {
        let database = pool.database.lock().unwrap();
        assert_eq!((database.captures.len(), database.max_snap_id), stopped);
    }
    worker.start().unwrap();
    wait_until(Duration::from_secs(4), || {
        let database = pool.database.lock().unwrap();
        database.captures.len() > stopped.0 && database.max_snap_id > stopped.1
    });
    worker.stop();
}
