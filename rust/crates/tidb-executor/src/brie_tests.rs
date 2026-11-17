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

//! Go `pkg/executor/brie_test.go` queue assertions.
use super::*;

fn statement(kind: BrieKind, storage: &str) -> BrieStmt {
    BrieStmt {
        kind,
        storage: storage.into(),
        schemas: Vec::new(),
        tables: Vec::new(),
        job_id: 0,
        options: Vec::new(),
    }
}

/// Go `TestFetchShowBRIE`: cleanup after fetch, throttling, ID reassignment,
/// and empty messages as SQL NULL. Time is set beyond the boundary to avoid
/// the Go test's wall-clock rounding dependence.
#[test]
fn test_fetch_show_brie() {
    let queue = BrieQueue::new();
    let now = chrono::Local::now().timestamp();
    let info = || {
        let mut info = BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://test/backup1"));
        info.queue_time = Some(now - OUTDATED_DURATION - 1);
        info.exec_time = info.queue_time;
        info.finish_time = info.queue_time;
        info.message = "killed".into();
        info
    };
    let (_, first) = queue.register_task(&Context::background(), info());
    assert_eq!(first, 1);
    let rows = queue.fetch_show_brie(BrieKind::Backup);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![
            Datum::UInt(1),
            Datum::new_string_datum("noop://test/backup1"),
            Datum::new_string_datum("Wait"),
            Datum::Real(0.0),
            local_time(Some(now - OUTDATED_DURATION - 1)),
            local_time(Some(now - OUTDATED_DURATION - 1)),
            local_time(Some(now - OUTDATED_DURATION - 1)),
            Datum::UInt(0),
            Datum::new_string_datum("killed")
        ]
    );
    assert!(queue.fetch_show_brie(BrieKind::Backup).is_empty());
    let (_, second) = queue.register_task(&Context::background(), info());
    assert_eq!(second, 2);
    let rows = queue.fetch_show_brie(BrieKind::Backup);
    assert_eq!(rows[0][0], Datum::UInt(2));
    assert_eq!(rows, queue.fetch_show_brie(BrieKind::Backup));
    *queue.last_clear_time.lock().unwrap() =
        Some(Instant::now() - CLEAR_INTERVAL - Duration::from_secs(1));
    let mut latest = BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://test/backup2"));
    latest.queue_time = Some(now);
    latest.exec_time = Some(now);
    latest.finish_time = Some(now);
    let (_, third) = queue.register_task(&Context::background(), latest);
    queue.clear_task_at(now);
    let rows = queue.fetch_show_brie(BrieKind::Backup);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::UInt(third));
    assert_eq!(rows[0][8], Datum::Null);
    assert!(queue.fetch_show_brie(BrieKind::Restore).is_empty());
}

/// Go's `modifyStore` failpoint set to `tikv`, held exclusively: the
/// failpoint is process-wide and its guard disables it on drop.
fn tikv_store() -> (tidb_failpoint::Guard, std::sync::MutexGuard<'static, ()>) {
    static STORE_TYPE: Mutex<()> = Mutex::new(());
    let lock = STORE_TYPE.lock().unwrap_or_else(|e| e.into_inner());
    // Tuple fields drop in order: the failpoint is disabled before unlocking.
    let failpoint = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/modifyStore",
        "return(\"tikv\")",
    );
    (failpoint, lock)
}

fn parse_brie(sql: &str) -> BrieStmt {
    let tidb_ast::Stmt::Admin(admin) = tidb_parser::parse(sql).unwrap() else {
        panic!()
    };
    let tidb_ast::AdminStmt::Brie(stmt) = &*admin else {
        panic!()
    };
    (**stmt).clone()
}

/// Go `buildBRIE`'s destination handling: `objstore.ParseRawURL` and
/// `ExtractQueryParameters` errors are annotated `invalid destination URL`
/// and quote no part of the URL, an S3 or GCS query moves into the backend
/// options, and `cfg.Storage` is the parsed URL's `String`. The job shows
/// that storage through `ast.RedactURL`.
#[test]
fn build_brie_parses_the_destination_like_go() {
    let _guard = tikv_store();
    let zone = tidb_datatype::SessionTimeZone::default();
    let build = |sql: &str| build_brie(&parse_brie(sql), &zone);
    for sql in [
        "BACKUP DATABASE * TO '1invalid:'",
        "BACKUP DATABASE * TO 's3://bad bucket/p?access-key=AKSECRET&secret-access-key=SKSECRET'",
    ] {
        let err = build(sql).err().unwrap().to_string();
        assert!(
            err.starts_with("invalid destination URL: parse storage URL failed: invalid format"),
            "{err}"
        );
        assert!(!err.contains("AKSECRET") && !err.contains("1invalid"), "{err}");
    }
    let err = build("BACKUP DATABASE * TO 's3://b/p?access-key=AK%zz&secret-access-key=SKSECRET'")
        .err()
        .unwrap()
        .to_string();
    assert!(
        err.starts_with("invalid destination URL: parse storage URL query failed: invalid format"),
        "{err}"
    );
    assert!(!err.contains("SKSECRET"), "{err}");

    let exec = build("BACKUP DATABASE * TO 's3://bucket/prefix?access-key=AK&secret-access-key=a+b/c&force_path_style=false&Region=us'").unwrap();
    let cfg = exec.backup_cfg.unwrap().config;
    let s3 = &cfg.backend_options.s3;
    assert_eq!((s3.access_key.as_str(), s3.secret_access_key.as_str()), ("AK", "a+b/c"));
    assert_eq!(s3.region, "us");
    assert!(!s3.force_path_style);
    assert_eq!(cfg.storage, "s3://bucket/prefix");
    assert_eq!(exec.info.storage, "s3://bucket/prefix");
    assert_eq!(exec.info.query, "BACKUP DATABASE * TO 's3://bucket/prefix'");

    let exec = build("RESTORE DATABASE * FROM 'gcs://bucket/p?credentials-file=%2Ftmp%2Fc&endpoint=http://e/'").unwrap();
    let cfg = exec.restore_cfg.unwrap().config;
    assert_eq!(cfg.backend_options.gcs.credentials_file, "/tmp/c");
    assert_eq!(cfg.backend_options.gcs.endpoint, "http://e/");
    assert_eq!(cfg.storage, "gcs://bucket/p");

    // Other schemes keep their query in the task storage; Go lowercases the
    // scheme. The job's shown storage drops a query `RedactURL` cannot vet.
    let exec = build("BACKUP DATABASE * TO 'LOCAL:///tmp/a%20b?x=1'").unwrap();
    assert_eq!(exec.backup_cfg.unwrap().config.storage, "local:///tmp/a%20b?x=1");
    assert_eq!(exec.info.storage, "local:///tmp/a%20b");
}

/// Go `TestClearTaskKeepsUnfinishedBRIETasks`.
#[test]
fn test_clear_task_keeps_unfinished_brie_tasks() {
    let queue = BrieQueue::new();
    let (_, running) =
        queue.register_task(&Context::background(), BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://running")));
    *queue.last_clear_time.lock().unwrap() = Some(Instant::now() - CLEAR_INTERVAL - Duration::from_secs(1));
    queue.clear_task();
    assert!(queue.tasks.lock().unwrap().contains_key(&running), "still-running BRIE tasks must survive queue cleanup");
    assert!(queue.cancel_task(running));
    let mut finished = BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://finished"));
    finished.finish_time = Some(chrono::Local::now().timestamp() - OUTDATED_DURATION - 60);
    let (_, finished) = queue.register_task(&Context::background(), finished);
    *queue.last_clear_time.lock().unwrap() = Some(Instant::now() - CLEAR_INTERVAL - Duration::from_secs(1));
    queue.clear_task();
    assert!(!queue.tasks.lock().unwrap().contains_key(&finished), "outdated finished BRIE tasks should still be garbage-collected");
}

/// Go `TestBRIEBuilderOptions`, both BACKUP and RESTORE assertions.
#[test]
fn test_brie_builder_options() {
    use tidb_proto::{backup::CompressionType, encryptionpb::EncryptionMethod};
    let _guard = tikv_store();
    let keyfile = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(keyfile.path(), "A".repeat(128)).unwrap();
    let parse = |sql: &str| {
        let tidb_ast::Stmt::Admin(admin) = tidb_parser::parse(sql).unwrap() else {
            panic!()
        };
        let tidb_ast::AdminStmt::Brie(stmt) = &*admin else {
            panic!()
        };
        stmt.clone()
    };
    let stmt = parse(&format!("BACKUP TABLE `a` TO 'noop://' CHECKSUM_CONCURRENCY = 4 IGNORE_STATS = 1 COMPRESSION_LEVEL = 4 COMPRESSION_TYPE = 'lz4' ENCRYPTION_METHOD = 'aes256-ctr' ENCRYPTION_KEYFILE = '{}'", keyfile.path().display()));
    let zone = tidb_datatype::SessionTimeZone::default();
    let exec = build_brie(&stmt, &zone).unwrap();
    let cfg = exec.backup_cfg.unwrap();
    assert!(!cfg.config.checksum);
    assert_eq!(cfg.config.checksum_concurrency, 4);
    assert_eq!(cfg.compression_config.compression_level, 4);
    assert!(cfg.ignore_stats);
    assert_eq!(
        cfg.compression_config.compression_type,
        CompressionType::Lz4
    );
    assert_eq!(
        cfg.config.cipher_info.cipher_type,
        EncryptionMethod::Aes256Ctr as i32
    );
    assert!(!cfg.config.cipher_info.cipher_key.is_empty());
    let stmt = parse("RESTORE TABLE `a` FROM 'noop://' CHECKSUM_CONCURRENCY = 4 WAIT_TIFLASH_READY = 1 WITH_SYS_TABLE = 1 LOAD_STATS = 1");
    let exec = build_brie(&stmt, &zone).unwrap();
    let cfg = exec.restore_cfg.unwrap();
    assert_eq!(cfg.config.checksum_concurrency, 4);
    assert!(!cfg.config.checksum);
    assert!(cfg.wait_tiflash_ready);
    assert!(cfg.with_sys_table);
    assert!(cfg.load_stats);
}

/// Go `tests/realtikvtest/brietest.TestCancel`, the queue's cancellable
/// wait. This exercises the queue directly, not a physical BR runner.
#[test]
fn test_cancel_waiting_task() {
    let queue = Arc::new(BrieQueue::new());
    let (first, first_id) = queue.register_task(
        &Context::background(),
        BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://")),
    );
    let _progress = queue.acquire_task(&first, first_id).unwrap();
    let (second, second_id) = queue.register_task(
        &Context::background(),
        BrieTaskInfo::new(&statement(BrieKind::Restore, "noop://")),
    );
    let q = queue.clone();
    let handle = std::thread::spawn(move || q.acquire_task(&second, second_id).err().unwrap());
    assert!(queue.cancel_task(second_id));
    assert_eq!(handle.join().unwrap(), "context canceled");
    queue.release_task();
    assert!(!queue.cancel_task(u64::MAX));
}

/// Serializes the tests that use the process-wide BRIE queue and node.
fn global_queue() -> std::sync::MutexGuard<'static, ()> {
    static QUEUE: Mutex<()> = Mutex::new(());
    QUEUE.lock().unwrap_or_else(|e| e.into_inner())
}

/// Go `TestGlueGetVersion`.
#[test]
fn test_glue_get_version() {
    struct NoNode;
    impl BrieNode for NoNode {
        fn storage(&self) -> Result<Arc<dyn tidb_br::glue::Storage>, String> {
            Err("none".into())
        }
        fn domain(&self) -> Result<Arc<dyn tidb_br::glue::Domain>, String> {
            Err("none".into())
        }
        fn create_session(&self) -> Result<Box<dyn tidb_br::glue::Session>, String> {
            Err("none".into())
        }
    }
    let glue = TidbGlue {
        node: Arc::new(NoNode),
        progress: Arc::new(BrieTaskProgress::waiting()),
        info: Arc::new(Mutex::new(BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://")))),
    };
    let version = tidb_br::glue::Glue::get_version(&glue);
    assert!(version.starts_with("TiDB\n"), "{version}");
    assert!(version.contains("Release Version"), "{version}");
    assert!(version.contains("Git Commit Hash"), "{version}");
    // The glue records what the task reports for SHOW BACKUPS.
    tidb_br::glue::Glue::record(&glue, "BackupTS", 7);
    tidb_br::glue::Glue::record(&glue, "Size", 9);
    let p = tidb_br::glue::Glue::start_progress(&glue, &Context::background(), "Backup", 4, false);
    p.inc();
    assert_eq!(p.get_current(), 1);
    let info = glue.info.lock().unwrap();
    assert_eq!((info.backup_ts, info.archive_size), (7, 9));
}

/// Go `TestBRIEKillMonitorHandlesWrappedQueryInterrupted`.
#[test]
fn test_brie_kill_monitor_handles_query_interrupted() {
    let killer = Arc::new(tidb_sqlkiller::SqlKiller::default());
    let (task_ctx, cancel_task_ctx) = Context::with_cancel(&Context::background());
    let (tick_tx, tick_rx) = crossbeam_channel::bounded(1);
    let (cancelled_tx, cancelled_rx) = crossbeam_channel::bounded(1);
    let k = Arc::clone(&killer);
    std::thread::spawn(move || {
        cancel_brie_on_kill(&task_ctx, &k, &tick_rx, || {
            let _ = cancelled_tx.send(());
        });
    });
    killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
    tick_tx.send(Instant::now()).unwrap();
    assert!(cancelled_rx.recv_timeout(Duration::from_secs(1)).is_ok(), "BRIE task was not cancelled after QueryInterrupted");
    cancel_task_ctx.cancel();
}

/// Go `TestMapBRIEErrPreservesCancelCause`.
#[test]
fn test_map_brie_err_preserves_cancel_cause() {
    let killer = tidb_sqlkiller::SqlKiller::default();
    let backup_failed = &*tidb_exeerrors::ERR_BRIE_BACKUP_FAILED;
    let (err, message) = map_brie_err(&killer, "disk full".into(), Some(backup_failed));
    assert_eq!(err.to_mysql_error().code, 8124);
    assert_eq!(message, "[executor:8124]Backup failed: disk full");

    killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
    let (err, _) = map_brie_err(&killer, "context canceled".into(), Some(backup_failed));
    assert_eq!(err.to_mysql_error().code, 1317);

    // CANCEL BR JOB cancels the task without the killer: 8124 stays.
    let no_kill = tidb_sqlkiller::SqlKiller::default();
    let (err, _) = map_brie_err(&no_kill, "context canceled".into(), Some(backup_failed));
    assert_eq!(err.to_mysql_error().code, 8124);
    let (err, message) = map_brie_err(&no_kill, "context canceled".into(), None);
    assert!(err.to_string().contains("context canceled"), "{err}");
    assert_eq!(message, "context canceled");
}

/// Go `TestBRIEAcquireTaskCancellationRecordsTerminalState`: a queued task
/// whose statement is cancelled records its finish and message.
#[test]
fn test_brie_acquire_task_cancellation_records_terminal_state() {
    let _queue = global_queue();
    // Another task holds the worker.
    let (held_ctx, held_id) = GLOBAL_BRIE_QUEUE.register_task(&Context::background(), BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://held")));
    let _held = GLOBAL_BRIE_QUEUE.acquire_task(&held_ctx, held_id).unwrap();
    let exec = BRIEExec { backup_cfg: None, restore_cfg: None, info: BrieTaskInfo::new(&statement(BrieKind::Backup, "noop://queued")) };
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let (tx, rx) = crossbeam_channel::bounded(1);
    std::thread::spawn(move || {
        let _ = tx.send(brie_next(&ctx, exec, 3, Arc::new(tidb_sqlkiller::SqlKiller::default())));
    });
    let queued = (0..100)
        .find_map(|_| {
            std::thread::sleep(Duration::from_millis(10));
            GLOBAL_BRIE_QUEUE.tasks.lock().unwrap().iter().find(|(_, item)| item.info.lock().unwrap().storage == "noop://queued").map(|(id, _)| *id)
        })
        .expect("the task is queued");
    cancel.cancel();
    let err = rx.recv_timeout(Duration::from_secs(1)).expect("the queued task returns after cancellation").unwrap_err();
    assert!(err.to_string().contains("context canceled"), "{err}");
    let info = GLOBAL_BRIE_QUEUE.query_task(queued).unwrap();
    let info = info.lock().unwrap();
    assert!(info.finish_time.is_some());
    assert_eq!(info.message, "context canceled");
    assert_eq!(info.conn_id, 3);
    drop(info);
    GLOBAL_BRIE_QUEUE.release_task();
    GLOBAL_BRIE_QUEUE.cancel_task(held_id);
}

/// A BACKUP on a node without a TiKV store fails as Go's task does when it
/// cannot open the store: 8124, recorded for SHOW BACKUPS.
#[test]
fn test_backup_without_a_node_fails_with_8124() {
    let _queue = global_queue();
    let (_fp, _store) = tikv_store();
    set_brie_node(None);
    let exec = build_brie(&parse_brie("BACKUP DATABASE d TO 'noop://nonode'"), &tidb_datatype::SessionTimeZone::default()).unwrap();
    let err = brie_next(&Context::background(), exec, 5, Arc::new(tidb_sqlkiller::SqlKiller::default())).unwrap_err();
    let mysql = err.to_mysql_error();
    assert_eq!(mysql.code, 8124);
    assert_eq!(mysql.message, "Backup failed: BACKUP and RESTORE need a TiKV store, and this instance has none");
    let rows = GLOBAL_BRIE_QUEUE.fetch_show_brie(BrieKind::Backup);
    let row = rows.iter().find(|r| r[1] == Datum::new_string_datum("noop://nonode")).expect("the job is listed");
    assert_eq!(row[8], Datum::new_string_datum("[executor:8124]Backup failed: BACKUP and RESTORE need a TiKV store, and this instance has none"));
    assert_eq!(row[7], Datum::UInt(5));
}

/// Go `buildBackupRestoreSchema`: the BACKUP and RESTORE result columns.
#[test]
fn test_backup_restore_columns() {
    let names = |kind| backup_restore_columns(kind).into_iter().map(|(n, _)| n).collect::<Vec<_>>();
    assert_eq!(names(BrieKind::Backup), ["Destination", "Size", "BackupTS", "Queue Time", "Execution Time"]);
    assert_eq!(names(BrieKind::Restore), ["Destination", "Size", "BackupTS", "Cluster TS", "Queue Time", "Execution Time"]);
    let columns = backup_restore_columns(BrieKind::Backup);
    assert_eq!(columns[0].1.flen(), 255);
    assert!(!columns[0].1.has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED));
    assert!(columns[1].1.has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED));
    assert_eq!(columns[4].1.flen(), 19);
}
