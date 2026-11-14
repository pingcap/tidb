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

//! Go `pkg/executor/brie.go`: queue, job inspection and local metadata SHOW.
//! Physical `task.RunBackup` / `RunRestore` are still absent; inspection must
//! not fabricate successful backup jobs or a Rust-specific archive format.

use crate::{DriverError, StmtOutput};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tidb_ast::{BrieKind, BrieStmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, Time, TimeType};
use tidb_gocontext::{CancelFunc, Context};

const CLEAR_INTERVAL: Duration = Duration::from_secs(10 * 60);
const OUTDATED_DURATION: i64 = 30 * 60;

/// Go `brieTaskProgress`.
pub struct BrieTaskProgress {
    current: AtomicI64,
    lock: Mutex<(String, i64)>,
}

impl BrieTaskProgress {
    /// Go `brieQueue.registerTask`'s initial progress.
    fn waiting() -> Self {
        Self {
            current: AtomicI64::new(0),
            lock: Mutex::new(("Wait".into(), 1)),
        }
    }
    /// Go `brieTaskProgress.Inc`.
    pub fn inc(&self) {
        self.inc_by(1);
    }
    /// Go `brieTaskProgress.IncBy`.
    pub fn inc_by(&self, count: i64) {
        self.current.fetch_add(count, Ordering::SeqCst);
    }
    /// Go `brieTaskProgress.GetCurrent`.
    pub fn get_current(&self) -> i64 {
        self.current.load(Ordering::SeqCst)
    }
    /// Go `brieTaskProgress.Close`.
    pub fn close(&self) {
        let mut state = self.lock.lock().unwrap();
        if self.get_current() < state.1 {
            state.0.push_str(" Canceled");
        }
        self.current.store(state.1, Ordering::SeqCst);
    }
    /// Go `tidbGlue.StartProgress`.
    pub fn start_progress(&self, name: &str, total: i64) {
        let mut state = self.lock.lock().unwrap();
        *state = (name.into(), total);
        self.current.store(0, Ordering::SeqCst);
    }
}

/// Go `brieTaskInfo`. Times are local wall-clock seconds as `CurrentTime` is;
/// `None` represents Go's zero DATETIME, not SQL NULL.
pub struct BrieTaskInfo {
    /// Go `id`.
    pub id: u64,
    /// Go `query` (restored without storage credentials).
    pub query: String,
    /// Go `queueTime`.
    pub queue_time: Option<i64>,
    /// Go `execTime`.
    pub exec_time: Option<i64>,
    /// Go `finishTime`.
    pub finish_time: Option<i64>,
    /// Go `kind`.
    pub kind: BrieKind,
    /// Go `storage`, redacted before storing for SHOW.
    pub storage: String,
    /// Go `connID`.
    pub conn_id: u64,
    /// Go `backupTS`.
    pub backup_ts: u64,
    /// Go `restoreTS`.
    pub restore_ts: u64,
    /// Go `archiveSize`.
    pub archive_size: u64,
    /// Go `message` (empty becomes SQL NULL).
    pub message: String,
}

impl BrieTaskInfo {
    /// Go `executorBuilder.buildBRIE`'s job identity and `restoreQuery`.
    pub fn new(stmt: &BrieStmt) -> Self {
        let mut stmt = stmt.clone();
        if matches!(stmt.storage.split(':').next(), Some("s3" | "gs" | "gcs")) {
            if let Some(query) = stmt.storage.find('?') {
                let fragment = stmt.storage[query..]
                    .find('#')
                    .map(|offset| stmt.storage[query + offset..].to_owned())
                    .unwrap_or_default();
                stmt.storage.truncate(query);
                stmt.storage.push_str(&fragment);
            }
        }
        Self {
            id: 0,
            query: stmt.secure_text(),
            queue_time: None,
            exec_time: None,
            finish_time: None,
            kind: stmt.kind,
            storage: tidb_ast::redact_url(&stmt.storage),
            conn_id: 0,
            backup_ts: 0,
            restore_ts: 0,
            archive_size: 0,
            message: String::new(),
        }
    }
    /// Go `tidbGlue.Record`.
    pub fn record(&mut self, name: &str, value: u64) {
        match name {
            "BackupTS" => self.backup_ts = value,
            "RestoreTS" => self.restore_ts = value,
            "Size" => self.archive_size = value,
            _ => {}
        }
    }
}

/// Go `brieQueueItem`.
struct BrieQueueItem {
    info: Arc<Mutex<BrieTaskInfo>>,
    progress: Arc<BrieTaskProgress>,
    cancel: CancelFunc,
}

/// Go `brieQueue`: one worker per process, cancellable waiting jobs.
pub struct BrieQueue {
    next_id: AtomicU64,
    tasks: Mutex<BTreeMap<u64, BrieQueueItem>>,
    last_clear_time: Mutex<Option<Instant>>,
    worker_tx: crossbeam_channel::Sender<()>,
    worker_rx: crossbeam_channel::Receiver<()>,
}

/// Go `globalBRIEQueue`.
pub static GLOBAL_BRIE_QUEUE: LazyLock<BrieQueue> = LazyLock::new(BrieQueue::new);

impl BrieQueue {
    /// Go `globalBRIEQueue`'s initialization.
    pub fn new() -> Self {
        let (worker_tx, worker_rx) = crossbeam_channel::bounded(1);
        Self {
            next_id: AtomicU64::new(0),
            tasks: Mutex::new(BTreeMap::new()),
            last_clear_time: Mutex::new(None),
            worker_tx,
            worker_rx,
        }
    }
    /// Go `brieQueue.registerTask`.
    pub fn register_task(&self, ctx: &Context, mut info: BrieTaskInfo) -> (Context, u64) {
        let (ctx, cancel) = Context::with_cancel(ctx);
        let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        info.id = id;
        self.tasks.lock().unwrap().insert(
            id,
            BrieQueueItem {
                info: Arc::new(Mutex::new(info)),
                progress: Arc::new(BrieTaskProgress::waiting()),
                cancel,
            },
        );
        (ctx, id)
    }
    /// Go `brieQueue.queryTask`.
    pub fn query_task(&self, id: u64) -> Option<Arc<Mutex<BrieTaskInfo>>> {
        self.tasks
            .lock()
            .unwrap()
            .get(&id)
            .map(|item| item.info.clone())
    }
    /// Go `brieQueue.acquireTask`. Pair successful acquisition with release.
    pub fn acquire_task(&self, ctx: &Context, id: u64) -> Result<Arc<BrieTaskProgress>, String> {
        crossbeam_channel::select! {
            send(self.worker_tx, ()) -> _ => {
                let progress = self.tasks.lock().unwrap().get(&id).map(|item| item.progress.clone());
                if let Some(progress) = progress { return Ok(progress); }
                self.release_task();
                Err(format!("backup/restore task {id} is canceled"))
            },
            recv(ctx.done()) -> _ => Err(ctx.err().unwrap().to_string()),
        }
    }
    /// Go `brieQueue.releaseTask`.
    pub fn release_task(&self) {
        self.worker_rx.recv().unwrap();
    }
    /// Go `brieQueue.cancelTask`: retain the row after cancelling.
    pub fn cancel_task(&self, id: u64) -> bool {
        let tasks = self.tasks.lock().unwrap();
        let Some(item) = tasks.get(&id) else {
            return false;
        };
        item.cancel.cancel();
        item.progress.close();
        true
    }
    /// Go `brieQueue.clearTask`.
    pub fn clear_task(&self) {
        self.clear_task_at(chrono::Local::now().timestamp());
    }
    /// Go `brieQueue.clearTask` with the current time supplied by its caller.
    fn clear_task_at(&self, now: i64) {
        let mut last_clear = self.last_clear_time.lock().unwrap();
        if last_clear.is_some_and(|last| last.elapsed() < CLEAR_INTERVAL) {
            return;
        }
        *last_clear = Some(Instant::now());
        self.tasks.lock().unwrap().retain(|_, item| {
            item.info
                .lock()
                .unwrap()
                .finish_time
                .is_some_and(|finish| now - finish <= OUTDATED_DURATION)
        });
    }
    /// Go `ShowExec.fetchShowBRIE`: fetch BEFORE opportunistic cleanup.
    pub fn fetch_show_brie(&self, kind: BrieKind) -> Vec<Vec<Datum>> {
        let rows = self
            .tasks
            .lock()
            .unwrap()
            .values()
            .filter_map(|item| {
                let info = item.info.lock().unwrap();
                if info.kind != kind {
                    return None;
                }
                let progress = item.progress.lock.lock().unwrap();
                Some(vec![
                    Datum::UInt(info.id),
                    Datum::new_string_datum(info.storage.as_str()),
                    Datum::new_string_datum(progress.0.as_str()),
                    Datum::Real(100.0 * item.progress.get_current() as f64 / progress.1 as f64),
                    local_time(info.queue_time),
                    local_time(info.exec_time),
                    local_time(info.finish_time),
                    Datum::UInt(info.conn_id),
                    if info.message.is_empty() {
                        Datum::Null
                    } else {
                        Datum::new_string_datum(info.message.as_str())
                    },
                ])
            })
            .collect();
        self.clear_task();
        rows
    }
}

impl Default for BrieQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `types.CurrentTime(mysql.TypeDatetime)` / zero-valued `types.Time`.
fn local_time(timestamp: Option<i64>) -> Datum {
    let core = timestamp.map_or_else(tidb_datatype::CoreTime::default, |ts| {
        tidb_datatype::core_time_from_datetime(
            chrono::DateTime::from_timestamp(ts, 0)
                .unwrap()
                .with_timezone(&chrono::Local),
        )
    });
    Datum::Time(Time::new(core, TimeType::DateTime, 0).unwrap())
}

/// Go `showQueryExec.Next`.
pub fn show_query_next(queue: &BrieQueue, id: u64) -> StmtOutput {
    let rows = queue
        .query_task(id)
        .map(|info| {
            vec![vec![Datum::new_string_datum(
                info.lock().unwrap().query.as_str(),
            )]]
        })
        .unwrap_or_default();
    StmtOutput::Rows {
        columns: vec![(
            "Query".into(),
            FieldType::new(FieldTypeCode::Varchar).with_flen(4096),
        )],
        rows,
    }
}

/// Go `cancelJobExec.Next`.
pub fn cancel_job_next(
    queue: &BrieQueue,
    id: u64,
    session: &mut (impl crate::sessionctx::SessionContext + ?Sized),
) -> StmtOutput {
    if !queue.cancel_task(id) {
        let error = (&*tidb_exeerrors::ERR_LOAD_DATA_JOB_NOT_FOUND)
            .gen_with_stack_by_args(&[id.into()])
            .to_sql_error();
        session.append_sql_warning(tidb_context::SqlWarn::with_code(
            tidb_context::WarningLevel::Warning,
            error.code,
            error.message,
        ));
    }
    StmtOutput::Affected(0)
}

/// Go `buildShowMetadataConfigFrom` / `showMetaExec.Next`.
pub fn show_meta_next(
    stmt: &BrieStmt,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<StmtOutput, DriverError> {
    let ctx = Context::background();
    let exec = tidb_br::task::show::create_exec(
        &ctx,
        tidb_br::task::show::Config {
            storage: stmt.storage.clone(),
            cipher: tidb_proto::backup::CipherInfo {
                cipher_type: tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32,
                ..Default::default()
            },
        },
    )
    .map_err(|err| DriverError::unsupported(format!("failed to create show exec: {err}")))?;
    let result = exec.read(&ctx).map_err(|err| {
        DriverError::unsupported(format!("failed to read metadata from backupmeta: {err}"))
    })?;
    let display_time = |ts: u64| {
        let time = chrono::DateTime::from_timestamp_millis((ts >> 18) as i64)
            .unwrap()
            .with_timezone(zone);
        Datum::Time(
            Time::new(
                tidb_datatype::core_time_from_datetime(time),
                TimeType::DateTime,
                0,
            )
            .unwrap(),
        )
    };
    let rows = result
        .tables
        .into_iter()
        .map(|table| {
            vec![
                Datum::new_string_datum(table.db_name),
                Datum::new_string_datum(table.table_name),
                Datum::Int(table.kv_count as i64),
                Datum::Int(table.kv_size as i64),
                if result.start_version.0 > 0 {
                    display_time(result.start_version.0)
                } else {
                    Datum::Null
                },
                display_time(result.end_version.0),
            ]
        })
        .collect();
    let columns = [
        ("Database", FieldTypeCode::Varchar, 255),
        ("Table", FieldTypeCode::Varchar, 255),
        ("Total_kvs", FieldTypeCode::LongLong, 20),
        ("Total_bytes", FieldTypeCode::LongLong, 20),
        ("Time_range_start", FieldTypeCode::Datetime, 19),
        ("Time_range_end", FieldTypeCode::Datetime, 19),
    ]
    .into_iter()
    .map(|(name, code, flen)| (name.into(), FieldType::new(code).with_flen(flen)))
    .collect();
    Ok(StmtOutput::Rows { columns, rows })
}

#[cfg(test)]
#[path = "brie_tests.rs"]
mod tests;

/// Go `BRIEExec`'s builder state. `Next`'s physical BR task invocation and
/// tidbGlue/restore-DDL integration are not implemented yet.
pub struct BRIEExec {
    /// Go `backupCfg`.
    pub backup_cfg: Option<tidb_br::task::backup::BackupConfig>,
    /// Go `restoreCfg`.
    pub restore_cfg: Option<tidb_br::task::restore::RestoreConfig>,
    /// Go `info`.
    pub info: BrieTaskInfo,
}

/// Go `executorBuilder.parseTSString`.
fn parse_ts_string(value: &str, zone: &tidb_datatype::SessionTimeZone) -> Result<u64, DriverError> {
    let time = tidb_datatype::parse_time(value, TimeType::Timestamp, 6, false, false, false, zone)
        .map_err(|e| DriverError::unsupported(e.to_string()))?
        .time;
    let time = time
        .go_time(zone)
        .map_err(|e| DriverError::unsupported(e.to_string()))?;
    Ok((time.timestamp_millis() as u64) << 18)
}

/// Go `executorBuilder.buildBRIE`, backup/restore configuration branches.
/// Remote backend option extraction and physical runners remain unported.
pub fn build_brie(
    stmt: &BrieStmt,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<BRIEExec, DriverError> {
    use tidb_ast::BrieOptionValue as V;
    use tidb_proto::{backup::CompressionType, encryptionpb::EncryptionMethod};
    let global = tidb_config::config_tree::config::get_global_config();
    let scheme = stmt
        .storage
        .split_once(':')
        .filter(|(s, _)| !s.contains('/'))
        .map_or("".into(), |(s, _)| s.to_ascii_lowercase());
    if tidb_sem::is_enabled() && matches!(scheme.as_str(), "local" | "file" | "" | "hdfs") {
        let storage = if scheme == "hdfs" {
            "hdfs storage"
        } else {
            "local storage"
        };
        return Err(DriverError::from(
            tidb_plannererrors::ERR_NOT_SUPPORTED_WITH_SEM
                .gen_with_stack_by_args(&[storage.into()]),
        ));
    }
    let store = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/modifyStore")
        .map_or_else(|| global.store.0.clone(), |value| value.string());
    let kind = if stmt.kind == BrieKind::Backup {
        "BACKUP"
    } else {
        "RESTORE"
    };
    if store != "tikv" {
        return Err(DriverError::unsupported(format!(
            "{kind} requires tikv store, not {store}"
        )));
    }
    let mut config = tidb_br::task::common::default_config();
    config.pd = global.path.split(',').map(str::to_owned).collect();
    config.tls = tidb_br::task::common::TLSConfig {
        ca: global.security.cluster_ssl_ca.clone(),
        cert: global.security.cluster_ssl_cert.clone(),
        key: global.security.cluster_ssl_key.clone(),
    };
    config.storage = stmt.storage.clone();
    for option in &stmt.options {
        let uint = match &option.value {
            V::Unsigned(n) | V::RateLimitBytes(n) | V::MicrosecondsAgo(n) => *n,
            V::Level(tidb_ast::BrieOptionLevel::Off) => 0,
            V::Level(tidb_ast::BrieOptionLevel::Required) => 1,
            V::Level(tidb_ast::BrieOptionLevel::Optional) => 2,
            V::Level(tidb_ast::BrieOptionLevel::Other(n)) => *n,
            _ => 0,
        };
        let text = match &option.value {
            V::String(s) => s.as_str(),
            _ => "",
        };
        match option.name.as_str() {
            "RATE_LIMIT" => config.rate_limit = uint,
            "CONCURRENCY" => config.concurrency = uint as u32,
            "CHECKSUM" => config.checksum = uint != 0,
            "SEND_CREDENTIALS_TO_TIKV" => config.send_creds = uint != 0,
            "CHECKSUM_CONCURRENCY" => config.checksum_concurrency = uint as usize,
            "ENCRYPTION_KEY_FILE" => {
                config.cipher_info.cipher_key =
                    tidb_br::task::common::get_cipher_key_content("", text)
                        .map_err(DriverError::unsupported)?
            }
            "ENCRYPTION_METHOD" => {
                config.cipher_info.cipher_type = match text {
                    "aes128-ctr" => EncryptionMethod::Aes128Ctr,
                    "aes192-ctr" => EncryptionMethod::Aes192Ctr,
                    "aes256-ctr" => EncryptionMethod::Aes256Ctr,
                    "plaintext" => EncryptionMethod::Plaintext,
                    _ => {
                        return Err(DriverError::unsupported(format!(
                            "unsupported encryption method: {text}"
                        )))
                    }
                } as i32
            }
            _ => {}
        }
    }
    config.table_filter = if !stmt.tables.is_empty() {
        let tables: Vec<_> = stmt
            .tables
            .iter()
            .map(|table| tidb_table_filter::Table {
                schema: if table.len() > 1 {
                    table[0].clone()
                } else {
                    String::new()
                },
                name: table.last().cloned().unwrap_or_default(),
            })
            .collect();
        config
            .filter_str
            .extend(tables.iter().map(ToString::to_string));
        tidb_table_filter::new_tables_filter(&tables)
    } else if !stmt.schemas.is_empty() {
        config
            .filter_str
            .extend(stmt.schemas.iter().map(|s| format!("`{s}`.*")));
        tidb_table_filter::new_schemas_filter(
            &stmt.schemas.iter().map(String::as_str).collect::<Vec<_>>(),
        )
    } else {
        tidb_table_filter::all()
    };
    config.table_filter = tidb_table_filter::case_insensitive(config.table_filter);
    let mut exec = BRIEExec {
        info: BrieTaskInfo::new(stmt),
        backup_cfg: None,
        restore_cfg: None,
    };
    match stmt.kind {
        BrieKind::Backup => {
            let mut backup = tidb_br::task::backup::default_backup_config(config);
            for option in &stmt.options {
                let uint = match option.value {
                    V::Unsigned(n) | V::MicrosecondsAgo(n) => n,
                    _ => 0,
                };
                let text = match &option.value {
                    V::String(s) => s.as_str(),
                    _ => "",
                };
                match option.name.as_str() {
                    "LAST_BACKUP" => {
                        backup.last_backup_ts = if matches!(option.value, V::String(_)) {
                            parse_ts_string(text, zone)?
                        } else {
                            uint
                        }
                    }
                    "SNAPSHOT" => match &option.value {
                        V::String(_) => backup.backup_ts = parse_ts_string(text, zone)?,
                        V::MicrosecondsAgo(_) => backup.time_ago = uint as i64,
                        _ => backup.backup_ts = uint,
                    },
                    "COMPRESSION_LEVEL" => {
                        backup.compression_config.compression_level = uint as i32
                    }
                    "COMPRESSION_TYPE" => {
                        backup.compression_config.compression_type = match text {
                            "zstd" => CompressionType::Zstd,
                            "snappy" => CompressionType::Snappy,
                            "lz4" => CompressionType::Lz4,
                            _ => {
                                return Err(DriverError::unsupported(format!(
                                    "unsupported compression type: {text}"
                                )))
                            }
                        }
                    }
                    "IGNORE_STATS" => backup.ignore_stats = uint != 0,
                    _ => {}
                }
            }
            exec.backup_cfg = Some(backup);
        }
        BrieKind::Restore => {
            let mut restore = tidb_br::task::restore::default_restore_config(config);
            for option in &stmt.options {
                let value = matches!(option.value, V::Unsigned(n) if n != 0);
                match option.name.as_str() {
                    "ONLINE" => restore.online = value,
                    "WAIT_TIFLASH_READY" => restore.wait_tiflash_ready = value,
                    "WITH_SYS_TABLE" => restore.with_sys_table = value,
                    "LOAD_STATS" => restore.load_stats = value,
                    _ => {}
                }
            }
            exec.restore_cfg = Some(restore);
        }
        _ => {
            return Err(DriverError::unsupported(format!(
                "unsupported BRIE statement kind: {}",
                stmt.secure_text()
            )))
        }
    }
    Ok(exec)
}

/// Go `PlanBuilder.buildSimple` / `buildShow`: BRIE dynamic visit privileges.
pub fn require_brie_privilege(
    session: &(impl crate::sessionctx::SessionContext + ?Sized),
    kind: BrieKind,
) -> Result<(), DriverError> {
    let privilege = if kind == BrieKind::Restore {
        "RESTORE_ADMIN"
    } else {
        "BACKUP_ADMIN"
    };
    if !session.privilege_checks_bypassed()
        && !session
            .privilege_manager()
            .request_dynamic_verification(privilege, false)
    {
        return Err(DriverError::from(
            tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                .gen_with_stack_by_args(&[format!("SUPER or {privilege}").into()]),
        ));
    }
    Ok(())
}
