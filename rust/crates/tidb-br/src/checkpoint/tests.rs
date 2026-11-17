//! Go `br/pkg/checkpoint/checkpoint_test.go`: the backup runner, its
//! metadata and its lock over local storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_proto::backup::{CipherInfo, File};
use tidb_proto::encryptionpb::EncryptionMethod;

use super::backup::*;
use super::GlobalTimer;
use crate::errors::Result;

/// Go `mockTimer`.
struct MockTimer(i64, i64);

impl GlobalTimer for MockTimer {
    fn get_ts(&self, _: &Context) -> Result<(i64, i64)> {
        Ok((self.0, self.1))
    }
}

fn timer(p: i64, l: i64) -> Option<Arc<dyn GlobalTimer>> {
    Some(Arc::new(MockTimer(p, l)))
}

fn local() -> (tempfile::TempDir, crate::metautil::Storage) {
    let dir = tempfile::tempdir().unwrap();
    for sub in [
        CHECKPOINT_DATA_DIR_FOR_BACKUP,
        CHECKPOINT_CHECKSUM_DIR_FOR_BACKUP,
    ] {
        std::fs::create_dir_all(dir.path().join(sub)).unwrap();
    }
    let s: crate::metautil::Storage =
        Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
    (dir, s)
}

fn cipher() -> CipherInfo {
    CipherInfo {
        cipher_type: EncryptionMethod::Aes256Ctr as i32,
        cipher_key: b"01234567890123456789012345678901".to_vec(),
    }
}

/// Go `TestCheckpointMetaForBackup`.
#[test]
fn checkpoint_meta_for_backup() {
    let ctx = Context::background();
    let (_dir, s) = local();
    let meta = CheckpointMetadataForBackup {
        config_hash: b"123456".to_vec(),
        backup_ts: 123_456,
        ..Default::default()
    };
    save_checkpoint_metadata(&ctx, &s, &meta).unwrap();
    let meta2 = load_checkpoint_metadata(&ctx, &s).unwrap();
    assert_eq!(meta2.config_hash, meta.config_hash);
    assert_eq!(meta2.backup_ts, meta.backup_ts);
}

/// Go `TestCheckpointBackupRunner`.
#[test]
fn checkpoint_backup_runner() {
    let ctx = Context::background();
    let (_dir, s) = local();
    let cipher = cipher();
    let runner = start_checkpoint_backup_runner_for_test(
        &ctx,
        s.clone(),
        Some(cipher.clone()),
        Duration::from_secs(5),
        timer(10, 10),
    )
    .unwrap();
    let data: HashMap<&str, [&str; 4]> = [
        ("a", ["a", "b", "c", "d"]),
        ("A", ["A", "B", "C", "D"]),
        ("1", ["1", "2", "3", "4"]),
        ("+", ["+", "-", "*", "/"]),
    ]
    .into();
    let file = |name: &str| File {
        name: name.to_owned(),
        ..Default::default()
    };
    for key in ["a", "A", "1"] {
        let d = data[key];
        append_for_backup(
            &ctx,
            &runner,
            d[0].as_bytes(),
            d[1].as_bytes(),
            &[file(d[2]), file(d[3])],
        )
        .unwrap();
    }
    for i in 1..=4_u64 {
        runner.flush_checksum(&ctx, i as i64, i, i, i).unwrap();
    }
    let d = data["+"];
    append_for_backup(
        &ctx,
        &runner,
        d[0].as_bytes(),
        d[1].as_bytes(),
        &[file(d[2]), file(d[3])],
    )
    .unwrap();
    runner.wait_for_finish(&ctx, true);

    let mut seen = 0;
    walk_checkpoint_file_for_backup(&ctx, &s, Some(&cipher), &mut |_, resp| {
        let d = data[std::str::from_utf8(&resp.start_key).unwrap()];
        assert_eq!(resp.end_key, d[1].as_bytes());
        assert_eq!(resp.files[0].name, d[2]);
        assert_eq!(resp.files[1].name, d[3]);
        seen += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, 4);

    let meta = CheckpointMetadataForBackup {
        config_hash: b"123456".to_vec(),
        backup_ts: 123_456,
        ..Default::default()
    };
    save_checkpoint_metadata(&ctx, &s, &meta).unwrap();
    let meta = load_checkpoint_metadata(&ctx, &s).unwrap();
    for i in 1..=4_i64 {
        assert_eq!(meta.checkpoint_checksum[&i].crc64xor, i as u64);
    }
}

/// Go `getLockData`.
fn lock_data(p: i64, l: i64) -> Vec<u8> {
    format!(
        "{{\"lock-id\":{},\"expire-at\":{}}}",
        crate::pdclient::compose_ts(p, l),
        p + 10
    )
    .into_bytes()
}

/// Go `TestCheckpointRunnerLock`.
#[test]
fn checkpoint_runner_lock() {
    let ctx = Context::background();
    let (_dir, s) = local();
    let cipher = Some(cipher());
    s.write_file(&ctx, CHECKPOINT_LOCK_PATH_FOR_BACKUP, &lock_data(10, 20))
        .unwrap();
    let tick = Duration::from_secs(5);
    assert!(start_checkpoint_backup_runner_for_test(
        &ctx,
        s.clone(),
        cipher.clone(),
        tick,
        timer(10, 10)
    )
    .is_err());
    let runner = start_checkpoint_backup_runner_for_test(
        &ctx,
        s.clone(),
        cipher.clone(),
        tick,
        timer(30, 10),
    )
    .unwrap();
    assert!(start_checkpoint_backup_runner_for_test(&ctx, s, cipher, tick, timer(40, 10)).is_err());
    runner.wait_for_finish(&ctx, true);
}
