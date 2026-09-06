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

//! Go `pkg/util/workloadrepo`: the cluster workload-repository worker.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, LocalResult, NaiveDate, Offset, TimeZone};
use tidb_owner::Manager;
use tidb_pd_client::{EtcdClient, EtcdWatcher};
use tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP;

/// Go `mysql.WorkloadSchema`.
pub const WORKLOAD_SCHEMA: &str = "WORKLOAD_SCHEMA";
/// Go `repositoryDest`.
pub const REPOSITORY_DEST: &str = "tidb_workload_repository_dest";
/// Go `repositoryRetentionDays`.
pub const REPOSITORY_RETENTION_DAYS: &str = "tidb_workload_repository_retention_days";
/// Go `repositorySamplingInterval`.
pub const REPOSITORY_SAMPLING_INTERVAL: &str = "tidb_workload_repository_active_sampling_interval";
/// Go `repositorySnapshotInterval`.
pub const REPOSITORY_SNAPSHOT_INTERVAL: &str = "tidb_workload_repository_snapshot_interval";

const SNAP_ID_KEY: &[u8] = b"/tidb/workloadrepo/snap_id";
const HIST_SNAPSHOTS: &str = "HIST_SNAPSHOTS";
const SNAPSHOT_RETRIES: usize = 5;

/// One parameter passed to an internal SQL statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlArg {
    /// SQL NULL.
    Null,
    /// Unsigned integer.
    UInt(u64),
    /// String.
    String(String),
}

/// One source-column definition copied into a repository table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    /// Original column spelling.
    pub name: String,
    /// TiDB type description.
    pub type_desc: String,
    /// Column comment.
    pub comment: String,
}

/// The table metadata workloadrepo reads from InfoSchema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableInfo {
    /// Source columns.
    pub columns: Vec<Column>,
    /// Existing partition names in definition order.
    pub partitions: Vec<String>,
}

/// One pooled internal SQL session.
pub trait RepositorySession {
    /// Runs and drains one internal statement.
    fn execute(&mut self, sql: &str, args: &[SqlArg]) -> Result<Vec<Vec<SqlArg>>, String>;
    /// Whether a schema exists.
    fn schema_exists(&self, schema: &str) -> bool;
    /// Reads one table's current metadata.
    fn table_info(&mut self, schema: &str, table: &str) -> Result<TableInfo, String>;
}

/// Go `sessionPool`.
pub trait SessionPool: Send + Sync {
    /// Gets one internal session. Callers retry forever, one second apart.
    fn get(&self) -> Result<Box<dyn RepositorySession>, String>;
}

/// Snapshot-ID operations and watches required from etcd.
pub trait RepositoryStore: Send + Sync {
    /// Reads one exact key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;
    /// Creates one unleased key iff absent.
    fn create(&self, key: &[u8], value: &[u8]) -> Result<bool, String>;
    /// Value-based compare-and-swap.
    fn compare_value_and_put(
        &self,
        key: &[u8],
        expected: &[u8],
        value: &[u8],
    ) -> Result<bool, String>;
    /// Watches all later updates to one key.
    fn watch(
        &self,
        key: &[u8],
        callback: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    ) -> Result<Box<dyn Send>, String>;
}

impl RepositoryStore for EtcdClient {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        EtcdClient::get(self, key).map_err(|error| error.to_string())
    }

    fn create(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        EtcdClient::create(self, key, value).map_err(|error| error.to_string())
    }

    fn compare_value_and_put(
        &self,
        key: &[u8],
        expected: &[u8],
        value: &[u8],
    ) -> Result<bool, String> {
        EtcdClient::compare_value_and_put(self, key, expected, value)
            .map_err(|error| error.to_string())
    }

    fn watch(
        &self,
        key: &[u8],
        callback: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    ) -> Result<Box<dyn Send>, String> {
        let watcher: EtcdWatcher = self
            .watch_key(key.to_vec(), 0, move |event| {
                if !event.deleted {
                    callback(event.value.clone());
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Box::new(watcher))
    }
}

/// Go `repositoryTable.tableType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableType {
    /// Captured only for a snapshot ID.
    Snapshot,
    /// Captured on the active sampling interval.
    Sampling,
    /// Snapshot metadata.
    Metadata,
}

/// Go `repositoryTable`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryTable {
    /// Source schema.
    pub schema: String,
    /// Source table.
    pub table: String,
    /// Repository table kind.
    pub table_type: TableType,
    /// Destination table.
    pub dest_table: String,
    /// Optional source predicate.
    pub where_clause: String,
    create_stmt: String,
    insert_stmt: String,
}

impl RepositoryTable {
    fn source(table: &str, table_type: TableType) -> Self {
        Self {
            schema: "INFORMATION_SCHEMA".to_owned(),
            table: table.to_owned(),
            table_type,
            dest_table: format!("HIST_{table}"),
            where_clause: String::new(),
            create_stmt: String::new(),
            insert_stmt: String::new(),
        }
    }
}

/// The eleven tables in pinned Go `workloadTables`.
#[must_use]
pub fn workload_tables() -> Vec<RepositoryTable> {
    let metadata = RepositoryTable {
        schema: String::new(),
        table: String::new(),
        table_type: TableType::Metadata,
        dest_table: HIST_SNAPSHOTS.to_owned(),
        where_clause: String::new(),
        create_stmt: format!(
            "CREATE TABLE IF NOT EXISTS `{WORKLOAD_SCHEMA}`.`{HIST_SNAPSHOTS}` (\n\t\t\tSNAP_ID int unsigned NOT NULL COMMENT 'Global unique identifier of the snapshot',\n\t\t\tBEGIN_TIME DATETIME NOT NULL COMMENT 'Datetime that TiDB begins taking this snapshot.',\n\t\t\tEND_TIME DATETIME NULL COMMENT 'Datetime that TiDB finish taking this snapshot.',\n\t\t\tDB_VER JSON NULL COMMENT 'Versions of TiDB, TiKV, PD at the moment',\n\t\t\tWR_VER int unsigned NULL COMMENT 'Version to identify the compatibility of workload schema between releases.',\n\t\t\tSOURCE VARCHAR(20) NULL COMMENT 'The program that initializes the snaphost. ',\n\t\t\tERROR TEXT DEFAULT NULL COMMENT 'extra messages are written if anything happens to block that snapshots.')"
        ),
        insert_stmt: String::new(),
    };
    let mut tables = vec![metadata];
    for name in [
        "TIDB_INDEX_USAGE",
        "TIDB_STATEMENTS_STATS",
        "CLIENT_ERRORS_SUMMARY_BY_HOST",
        "CLIENT_ERRORS_SUMMARY_BY_USER",
        "CLIENT_ERRORS_SUMMARY_GLOBAL",
    ] {
        tables.push(RepositoryTable::source(name, TableType::Snapshot));
    }
    for name in [
        "PROCESSLIST",
        "DATA_LOCK_WAITS",
        "TIDB_TRX",
        "MEMORY_USAGE",
        "DEADLOCKS",
    ] {
        tables.push(RepositoryTable::source(name, TableType::Sampling));
    }
    tables
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

/// Go `buildCreateQuery`.
pub fn build_create_query(table: &RepositoryTable, source: &TableInfo) -> Result<String, String> {
    if table.table_type == TableType::Metadata {
        return Err("buildCreateQuery invoked on metadataTable".to_owned());
    }
    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (",
        quote_identifier(WORKLOAD_SCHEMA),
        quote_identifier(&table.dest_table)
    );
    if table.table_type == TableType::Snapshot {
        sql.push_str("`SNAP_ID` INT UNSIGNED NOT NULL, ");
    }
    sql.push_str("`TS` DATETIME NOT NULL, `INSTANCE_ID` VARCHAR(64) DEFAULT NULL");
    for column in &source.columns {
        let _ = write!(
            sql,
            ", {} {} COMMENT {} ",
            quote_identifier(&column.name),
            column.type_desc,
            quote_string(&column.comment)
        );
    }
    sql.push_str(") DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ");
    Ok(sql)
}

/// Go `buildInsertQuery`.
pub fn build_insert_query(table: &RepositoryTable, source: &TableInfo) -> Result<String, String> {
    if table.table_type == TableType::Metadata {
        return Err("buildInsertQuery invoked on metadataTable".to_owned());
    }
    let mut columns = Vec::new();
    let mut selected = Vec::new();
    if table.table_type == TableType::Snapshot {
        columns.push("`SNAP_ID`".to_owned());
        selected.push("%?".to_owned());
    }
    columns.extend(["`TS`".to_owned(), "`INSTANCE_ID`".to_owned()]);
    selected.extend(["now()".to_owned(), "%?".to_owned()]);
    for column in &source.columns {
        let name = quote_identifier(&column.name);
        columns.push(name.clone());
        selected.push(name);
    }
    let mut sql = format!(
        "INSERT {}.{} ({}) SELECT {} FROM {}.{}",
        quote_identifier(WORKLOAD_SCHEMA),
        quote_identifier(&table.dest_table),
        columns.join(", "),
        selected.join(", "),
        quote_identifier(&table.schema),
        quote_identifier(&table.table)
    );
    if !table.where_clause.is_empty() {
        let _ = write!(sql, "WHERE {}", table.where_clause);
    }
    Ok(sql)
}

/// Go `generatePartitionName`.
#[must_use]
pub fn generate_partition_name(day: NaiveDate) -> String {
    format!("p{}", day.format("%Y%m%d"))
}

/// Go `parsePartitionName`.
pub fn parse_partition_name(name: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(name, "p%Y%m%d").map_err(|error| error.to_string())
}

/// Go `generatePartitionRanges`.
pub fn generate_partition_ranges(
    partitions: &[String],
    now: DateTime<Local>,
) -> Result<(bool, String), String> {
    let today = now.date_naive();
    let mut last = today;
    if let Some(name) = partitions.last() {
        let parsed = parse_partition_name(name)?;
        if parsed > last {
            last = parsed;
        }
    }
    let mut definitions = Vec::new();
    for offset in [1_i64, 2] {
        let day = today
            .checked_add_signed(chrono::Duration::days(offset))
            .ok_or_else(|| "partition date overflow".to_owned())?;
        if day > last {
            definitions.push(format!(
                "PARTITION {} VALUES LESS THAN (TO_DAYS('{}'))",
                generate_partition_name(day),
                day.format("%Y-%m-%d")
            ));
        }
    }
    Ok((definitions.is_empty(), definitions.join(", ")))
}

/// Go `calcNextTick`.
#[must_use]
pub fn calc_next_tick(now: DateTime<Local>) -> Duration {
    let day = now.date_naive();
    let mut next = date_at_two(&Local, day);
    if next <= now {
        next = date_at_two(&Local, day.succ_opt().expect("date overflow"));
    }
    (next - now).to_std().unwrap_or_default()
}

fn date_at_two<Tz>(timezone: &Tz, day: NaiveDate) -> DateTime<Tz>
where
    Tz: TimeZone,
{
    date_at_hour(timezone, day, 2)
}

fn date_at_hour<Tz>(timezone: &Tz, day: NaiveDate, hour: u32) -> DateTime<Tz>
where
    Tz: TimeZone,
{
    let naive = day
        .and_hms_opt(hour, 0, 0)
        .expect("the requested hour is a valid naive time");
    match timezone.from_local_datetime(&naive) {
        LocalResult::Single(value) | LocalResult::Ambiguous(value, _) => value,
        LocalResult::None => {
            // Go `time.Date` resolves a skipped wall-clock time with one of the
            // offsets adjacent to the transition. Its current implementation
            // uses the post-transition offset for a spring-forward gap.
            let offset = (1..=24 * 60)
                .find_map(|minutes| {
                    let candidate = naive.checked_add_signed(chrono::Duration::minutes(minutes))?;
                    match timezone.from_local_datetime(&candidate) {
                        LocalResult::Single(value) | LocalResult::Ambiguous(value, _) => {
                            Some(value.offset().fix().local_minus_utc())
                        }
                        LocalResult::None => None,
                    }
                })
                .expect("a timezone must contain a valid local time within one day");
            let utc = naive
                .checked_sub_signed(chrono::Duration::seconds(i64::from(offset)))
                .expect("timezone conversion is in range");
            timezone.from_utc_datetime(&utc)
        }
    }
}

/// Go `validateDest`.
pub fn validate_dest(value: &str) -> Result<String, String> {
    let value = value.to_ascii_lowercase();
    if value.is_empty() || value == "table" {
        Ok(value)
    } else {
        Err(format!(
            "Variable '{REPOSITORY_DEST}' can't be set to the value of '{value}': valid values are '' and 'table'"
        ))
    }
}

/// Domain's owner-manager constructor.
pub type OwnerFactory = Arc<dyn Fn(&str, &str) -> Arc<dyn Manager> + Send + Sync>;

struct RuntimeState {
    enabled: bool,
    sampling_interval: u64,
    snapshot_interval: u64,
    retention_days: i64,
    owner: Option<Arc<dyn Manager>>,
    stop: Option<Arc<AtomicBool>>,
    threads: Vec<JoinHandle<()>>,
}

/// One Go `worker`, with package-external authorities expressed as traits.
pub struct Worker {
    store: Option<Arc<dyn RepositoryStore>>,
    sessions: Option<Arc<dyn SessionPool>>,
    owner_factory: Option<OwnerFactory>,
    instance_id: String,
    tables: Mutex<Vec<RepositoryTable>>,
    state: Mutex<RuntimeState>,
}

impl Worker {
    /// Go `initializeWorker` plus Domain-provided authorities.
    #[must_use]
    pub fn new(
        store: Option<Arc<dyn RepositoryStore>>,
        sessions: Option<Arc<dyn SessionPool>>,
        owner_factory: Option<OwnerFactory>,
        instance_id: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            sessions,
            owner_factory,
            instance_id: instance_id.into(),
            tables: Mutex::new(workload_tables()),
            state: Mutex::new(RuntimeState {
                enabled: false,
                sampling_interval: 5,
                snapshot_interval: 3_600,
                retention_days: 7,
                owner: None,
                stop: None,
                threads: Vec::new(),
            }),
        })
    }

    fn get_session(&self) -> Box<dyn RepositorySession> {
        loop {
            if let Ok(session) = self
                .sessions
                .as_ref()
                .expect("repository session pool was checked before startup")
                .get()
            {
                return session;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn execute_retry(
        session: &mut dyn RepositorySession,
        sql: &str,
        args: &[SqlArg],
    ) -> Result<Vec<Vec<SqlArg>>, String> {
        let mut errors = Vec::with_capacity(5);
        for _ in 0..5 {
            match session.execute(sql, args) {
                Ok(rows) => return Ok(rows),
                Err(error) => errors.push(error),
            }
        }
        Err(errors.join("\n"))
    }

    /// Go `start`.
    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.stop.is_some() {
            return Ok(());
        }
        state.enabled = true;
        if self.sessions.is_none() {
            return Ok(());
        }
        let store = Arc::clone(
            self.store
                .as_ref()
                .ok_or_else(|| "etcd client required for workload repository".to_owned())?,
        );
        let factory = self
            .owner_factory
            .as_ref()
            .ok_or_else(|| "owner manager required for workload repository".to_owned())?;
        let owner = factory("/tidb/workloadrepo/owner", "workloadrepo");
        let stop = Arc::new(AtomicBool::new(false));
        state.owner = Some(Arc::clone(&owner));
        state.stop = Some(Arc::clone(&stop));
        STMT_SUMMARY_BY_DIGEST_MAP.set_history_enabled(false);

        let worker = Arc::clone(self);
        let thread = std::thread::Builder::new()
            .name("workloadrepo-prestart".to_owned())
            .spawn(move || worker.run_repository(owner, store, stop))
            .map_err(|error| error.to_string())?;
        state.threads.push(thread);
        Ok(())
    }

    /// Go `stop`.
    pub fn stop(&self) {
        let (owner, threads) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.enabled = false;
            let Some(stop) = state.stop.take() else {
                return;
            };
            stop.store(true, Ordering::Release);
            (state.owner.take(), std::mem::take(&mut state.threads))
        };
        for thread in threads {
            let _ = thread.join();
        }
        STMT_SUMMARY_BY_DIGEST_MAP.set_history_enabled(true);
        if let Some(owner) = owner {
            owner.close();
        }
    }

    /// Go `setRepositoryDest` after sysvar validation.
    pub fn set_repository_dest(self: &Arc<Self>, value: &str) -> Result<(), String> {
        match value {
            "table" => self.start(),
            _ => {
                self.stop();
                Ok(())
            }
        }
    }

    /// Go `changeSamplingInterval`.
    pub fn set_sampling_interval(&self, value: &str) -> Result<(), String> {
        let value = value.parse::<u64>().map_err(|_| {
            format!("Incorrect argument type to variable '{REPOSITORY_SAMPLING_INTERVAL}'")
        })?;
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sampling_interval = value;
        Ok(())
    }

    /// Go `changeSnapshotInterval`.
    pub fn set_snapshot_interval(&self, value: &str) -> Result<(), String> {
        let value = value.parse::<u64>().map_err(|_| {
            format!("Incorrect argument type to variable '{REPOSITORY_SNAPSHOT_INTERVAL}'")
        })?;
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot_interval = value;
        Ok(())
    }

    /// Go `setRetentionDays`.
    pub fn set_retention_days(&self, value: &str) -> Result<(), String> {
        let value = value.parse::<i64>().map_err(|_| {
            format!("Incorrect argument type to variable '{REPOSITORY_RETENTION_DAYS}'")
        })?;
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retention_days = value;
        Ok(())
    }

    fn run_repository(
        self: Arc<Self>,
        owner: Arc<dyn Manager>,
        store: Arc<dyn RepositoryStore>,
        stop: Arc<AtomicBool>,
    ) {
        let _ = owner.campaign_owner(&[]);
        while !stop.load(Ordering::Acquire) {
            if owner.is_owner() {
                let _ = self.create_all_tables(Local::now());
            }
            if self.check_tables_exist(Local::now()) {
                self.run_loops(owner, store, stop);
                return;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn run_loops(
        &self,
        owner: Arc<dyn Manager>,
        store: Arc<dyn RepositoryStore>,
        stop: Arc<AtomicBool>,
    ) {
        std::thread::scope(|scope| {
            scope.spawn(|| self.run_sampling_loop(&stop));
            scope.spawn(|| self.run_snapshot_loop(owner.as_ref(), store.as_ref(), &stop));
            scope.spawn(|| self.run_housekeeping_loop(owner.as_ref(), &stop));
        });
    }

    fn run_sampling_loop(&self, stop: &AtomicBool) {
        let initial = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sampling_interval;
        let mut last_interval = initial;
        let mut next = Instant::now() + Duration::from_secs(initial);
        while !stop.load(Ordering::Acquire) {
            let interval = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .sampling_interval;
            let now = Instant::now();
            if interval != last_interval {
                last_interval = interval;
                next = now + Duration::from_secs(interval);
            }
            if interval > 0 && now >= next {
                self.sample_all();
                next = now + Duration::from_secs(interval);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn run_snapshot_loop(
        &self,
        owner: &dyn Manager,
        store: &dyn RepositoryStore,
        stop: &AtomicBool,
    ) {
        let (sender, receiver) = std::sync::mpsc::channel::<Vec<u8>>();
        let callback = Arc::new(move |value| {
            let _ = sender.send(value);
        });
        let _watcher = store.watch(SNAP_ID_KEY, callback).ok();
        let initial = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot_interval;
        let mut last_interval = initial;
        let mut next = Instant::now() + Duration::from_secs(initial);
        while !stop.load(Ordering::Acquire) {
            let interval = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .snapshot_interval;
            let now = Instant::now();
            if interval != last_interval {
                last_interval = interval;
                next = now + Duration::from_secs(interval);
            }
            if now >= next {
                if owner.is_owner() {
                    let _ = self.take_snapshot_inner();
                }
                next = now + Duration::from_secs(interval);
            }
            let mut last_update = None;
            while let Ok(value) = receiver.try_recv() {
                last_update = Some(value);
            }
            if let Some(value) = last_update {
                if let Ok(text) = std::str::from_utf8(&value) {
                    if let Ok(snap_id) = text.parse::<u64>() {
                        self.snapshot_all(snap_id);
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn run_housekeeping_loop(&self, owner: &dyn Manager, stop: &AtomicBool) {
        self.run_housekeeping_loop_with(owner, stop, calc_next_tick);
    }

    /// Go `getHouseKeeper`, with its next-tick function kept as the source
    /// test seam and with the timer left unarmed after a non-owner tick or an
    /// error.
    fn run_housekeeping_loop_with(
        &self,
        owner: &dyn Manager,
        stop: &AtomicBool,
        next_tick: impl Fn(DateTime<Local>) -> Duration,
    ) {
        let mut next = Some(Instant::now() + next_tick(Local::now()));
        while !stop.load(Ordering::Acquire) {
            let now = Instant::now();
            if next.is_some_and(|deadline| now >= deadline) {
                if !owner.is_owner() {
                    next = None;
                } else {
                    let retention = self
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .retention_days;
                    let current = Local::now();
                    if self.create_all_partitions(current).is_ok()
                        && self.drop_old_partitions(current, retention).is_ok()
                    {
                        next = Some(Instant::now() + next_tick(Local::now()));
                    } else {
                        next = None;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Go package-level `takeSnapshot`; internal errors are deliberately masked.
    pub fn take_snapshot(&self) -> Result<(), String> {
        // Go's package-level wrapper holds the worker mutex across the full
        // manual snapshot initiation, serializing it with start/stop and
        // interval changes.
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.enabled {
            return Err("Workload repository is not enabled".to_owned());
        }
        let result = self
            .take_snapshot_inner()
            .map(|_| ())
            .map_err(|_| "Snapshot initiation failed".to_owned());
        drop(state);
        result
    }

    fn take_snapshot_inner(&self) -> Result<u64, String> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| "etcd client required".to_owned())?;
        let mut session = self.get_session();
        let mut last_error = String::new();
        for _ in 0..SNAPSHOT_RETRIES {
            let current = match store.get(SNAP_ID_KEY) {
                Ok(current) => current,
                Err(error) => {
                    last_error = format!("cannot get current snapid: {error}");
                    continue;
                }
            };
            let resolved = match current {
                Some(value) if !value.is_empty() => (
                    std::str::from_utf8(&value)
                        .map_err(|error| error.to_string())
                        .and_then(|value| value.parse::<u64>().map_err(|error| error.to_string())),
                    false,
                ),
                // Go's `etcdGet` returns the empty string both when the key is
                // absent and when its stored value is empty; `getSnapID`
                // treats both cases as `errKeyNotFound` and recovers from the
                // repository table's maximum snapshot ID.
                Some(_) | None => (Self::query_max_snap_id(session.as_mut()), true),
            };
            let (snap_id, absent) = match resolved {
                (Ok(snap_id), absent) => (snap_id, absent),
                (Err(error), _) => {
                    last_error = format!("cannot get current snapid: {error}");
                    continue;
                }
            };
            let next = snap_id.wrapping_add(1);
            if let Err(error) = session.execute(
                &format!("INSERT INTO `{WORKLOAD_SCHEMA}`.`{HIST_SNAPSHOTS}` (`BEGIN_TIME`, `SNAP_ID`) VALUES (now(), %?) ON DUPLICATE KEY UPDATE `BEGIN_TIME` = now()"),
                &[SqlArg::UInt(next)],
            ) {
                last_error = format!("could not insert into hist_snapshots: {error}");
                continue;
            }
            let succeeded = if absent {
                store.create(SNAP_ID_KEY, next.to_string().as_bytes())
            } else {
                store.compare_value_and_put(
                    SNAP_ID_KEY,
                    snap_id.to_string().as_bytes(),
                    next.to_string().as_bytes(),
                )
            };
            let succeeded = match succeeded {
                Ok(succeeded) => succeeded,
                Err(error) => {
                    last_error = format!("cannot update current snapid to {snap_id}: {error}");
                    continue;
                }
            };
            if succeeded {
                return Ok(snap_id);
            }
            last_error = format!("cannot update current snapid to {snap_id}");
        }
        Err(last_error)
    }

    fn query_max_snap_id(session: &mut dyn RepositorySession) -> Result<u64, String> {
        let rows = session.execute(
            &format!("SELECT MAX(`SNAP_ID`) FROM `{WORKLOAD_SCHEMA}`.`{HIST_SNAPSHOTS}`"),
            &[],
        )?;
        let Some(value) = rows.first().and_then(|row| row.first()) else {
            return Err("no rows returned when querying max snap id".to_owned());
        };
        match value {
            SqlArg::Null => Ok(0),
            SqlArg::UInt(value) => Ok(*value),
            SqlArg::String(_) => Err("MAX(SNAP_ID) returned a non-integer".to_owned()),
        }
    }

    fn sample_all(&self) {
        let tables = self
            .tables
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        std::thread::scope(|scope| {
            for table in tables
                .iter()
                .filter(|table| table.table_type == TableType::Sampling)
            {
                scope.spawn(move || {
                    let _ = self.capture_table(table, &[SqlArg::String(self.instance_id.clone())]);
                });
            }
        });
    }

    fn snapshot_all(&self, snap_id: u64) {
        let tables = self
            .tables
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let snapshot_tables = tables
            .iter()
            .filter(|table| table.table_type == TableType::Snapshot)
            .collect::<Vec<_>>();
        let errors = Mutex::new(vec![None; snapshot_tables.len()]);
        std::thread::scope(|scope| {
            for (index, table) in snapshot_tables.into_iter().enumerate() {
                let errors = &errors;
                scope.spawn(move || {
                    if let Err(error) = self.capture_table(
                        table,
                        &[
                            SqlArg::UInt(snap_id),
                            SqlArg::String(self.instance_id.clone()),
                        ],
                    ) {
                        errors.lock().unwrap_or_else(|error| error.into_inner())[index] =
                            Some(error);
                    }
                });
            }
        });
        let joined = errors
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter_map(Option::as_deref)
            .collect::<Vec<_>>()
            .join("\n");
        let error_arg = if joined.is_empty() {
            SqlArg::Null
        } else {
            SqlArg::String(joined)
        };
        let mut session = self.get_session();
        let _ = session.execute(
            &format!("UPDATE `{WORKLOAD_SCHEMA}`.`{HIST_SNAPSHOTS}` SET `END_TIME` = now(), `ERROR` = COALESCE(CONCAT(ERROR, %?), ERROR, %?) WHERE `SNAP_ID` = %?"),
            &[error_arg.clone(), error_arg, SqlArg::UInt(snap_id)],
        );
    }

    fn capture_table(&self, table: &RepositoryTable, args: &[SqlArg]) -> Result<(), String> {
        let mut session = self.get_session();
        let sql = if table.insert_stmt.is_empty() {
            let source = session
                .table_info(&table.schema, &table.table)
                .map_err(|error| {
                    format!(
                        "could not generate insert statement for `{}`: {error}",
                        table.dest_table
                    )
                })?;
            let built = build_insert_query(table, &source).map_err(|error| {
                format!(
                    "could not generate insert statement for `{}`: {error}",
                    table.dest_table
                )
            })?;
            if let Some(shared) = self
                .tables
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter_mut()
                .find(|candidate| candidate.dest_table == table.dest_table)
            {
                if shared.insert_stmt.is_empty() {
                    shared.insert_stmt.clone_from(&built);
                }
                shared.insert_stmt.clone()
            } else {
                built
            }
        } else {
            table.insert_stmt.clone()
        };
        session.execute(&sql, args).map(|_| ()).map_err(|error| {
            format!(
                "could not run insert statement for `{}`: {error}",
                table.dest_table
            )
        })
    }

    fn create_all_tables(&self, now: DateTime<Local>) -> Result<(), String> {
        let mut session = self.get_session();
        if !session.schema_exists(WORKLOAD_SCHEMA) {
            Self::execute_retry(
                session.as_mut(),
                &format!("create database if not exists {WORKLOAD_SCHEMA}"),
                &[],
            )?;
        }
        let mut tables = self
            .tables
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for table in tables.iter_mut() {
            if session
                .table_info(WORKLOAD_SCHEMA, &table.dest_table)
                .is_ok()
            {
                continue;
            }
            let mut create = if table.table_type == TableType::Metadata {
                table.create_stmt.clone()
            } else {
                let source = session.table_info(&table.schema, &table.table)?;
                build_create_query(table, &source)?
            };
            let (_, definitions) = generate_partition_ranges(&[], now)?;
            let column = if table.table_type == TableType::Metadata {
                "BEGIN_TIME"
            } else {
                "TS"
            };
            let _ = write!(
                create,
                " PARTITION BY RANGE( TO_DAYS({column}) ) ({definitions})"
            );
            Self::execute_retry(session.as_mut(), &create, &[])?;
        }
        drop(tables);
        self.create_all_partitions(now)
    }

    fn check_tables_exist(&self, now: DateTime<Local>) -> bool {
        let mut session = self.get_session();
        self.tables
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .all(|table| {
                session
                    .table_info(WORKLOAD_SCHEMA, &table.dest_table)
                    .ok()
                    .and_then(|info| info.partitions.last().cloned())
                    .and_then(|name| parse_partition_name(&name).ok())
                    .is_some_and(|last| last > now.date_naive().succ_opt().unwrap())
            })
    }

    fn create_all_partitions(&self, now: DateTime<Local>) -> Result<(), String> {
        let mut session = self.get_session();
        for table in self
            .tables
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
        {
            let info = session.table_info(WORKLOAD_SCHEMA, &table.dest_table)?;
            let (skip, definitions) = generate_partition_ranges(&info.partitions, now)?;
            if !skip {
                Self::execute_retry(
                    session.as_mut(),
                    &format!(
                        "ALTER TABLE `{WORKLOAD_SCHEMA}`.`{}` ADD PARTITION ({definitions})",
                        table.dest_table
                    ),
                    &[],
                )?;
            }
        }
        Ok(())
    }

    fn drop_old_partitions(&self, now: DateTime<Local>, retention: i64) -> Result<(), String> {
        if retention == 0 {
            return Ok(());
        }
        let mut session = self.get_session();
        let mut errors = Vec::new();
        for table in self
            .tables
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
        {
            let result = (|| {
                let info = session.table_info(WORKLOAD_SCHEMA, &table.dest_table)?;
                for partition in info.partitions {
                    let day = parse_partition_name(&partition)?;
                    let partition_midnight = date_at_hour(&Local, day, 0);
                    if now.signed_duration_since(partition_midnight).num_hours() / 24 < retention {
                        continue;
                    }
                    Self::execute_retry(
                        session.as_mut(),
                        &format!(
                            "ALTER TABLE `{WORKLOAD_SCHEMA}`.`{}` DROP PARTITION `{partition}`",
                            table.dest_table
                        ),
                        &[],
                    )?;
                }
                Ok::<(), String>(())
            })();
            if let Err(error) = result {
                errors.push(error);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("\n"))
        }
    }

    /// Whether the worker is enabled.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .enabled
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(stop) = self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .stop
            .as_ref()
        {
            stop.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests;
