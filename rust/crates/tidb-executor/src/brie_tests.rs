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

/// Go `TestBRIEBuilderOptions`, both BACKUP and RESTORE assertions.
#[test]
fn test_brie_builder_options() {
    use tidb_proto::{backup::CompressionType, encryptionpb::EncryptionMethod};
    let _guard = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/modifyStore",
        "return(\"tikv\")",
    );
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
