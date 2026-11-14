// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use super::*;
use crate::local_storage::LocalStorage;
use tidb_proto::backup::{BackupMeta, CipherInfo, File, Schema};

fn check_load(partition: bool) {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap();
    let table = if partition {
        br#"{"id":123,"name":{"O":"t1","L":"t1"},"partition":{"definitions":[{"id":124},{"id":125}]}}"#.to_vec()
    } else {
        br#"{"id":123,"name":{"O":"t1","L":"t1"}}"#.to_vec()
    };
    let ids = if partition {
        vec![124, 124, 125, 122]
    } else {
        vec![123, 122]
    };
    let files = ids
        .into_iter()
        .enumerate()
        .map(|(i, id)| File {
            name: format!("{}.sst", i + 1),
            start_key: tidb_tablecodec::table_key::encode_row_key(id, b"a"),
            end_key: tidb_tablecodec::table_key::encode_row_key(id + 1, b"a"),
            ..Default::default()
        })
        .collect();
    let meta = BackupMeta {
        files,
        schemas: vec![Schema {
            db: br#"{"id":1,"db_name":{"O":"test","L":"test"}}"#.to_vec(),
            table,
            stats: br#"{"database_name":"test","table_name":"t1"}"#.to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let reader = MetaReader::new(
        meta,
        storage,
        CipherInfo {
            cipher_type: tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32,
            ..Default::default()
        },
    );
    let dbs = load_backup_tables(&Context::background(), &reader, true).unwrap();
    let table = dbs["test"].get_table("t1").unwrap();
    assert_eq!(
        table.files_of_physicals.len(),
        if partition { 2 } else { 1 }
    );
    let mut names: Vec<_> = table
        .files_of_physicals
        .values()
        .flatten()
        .map(|file| file.name.as_str())
        .collect();
    names.sort();
    assert_eq!(
        names,
        if partition {
            vec!["1.sst", "2.sst", "3.sst"]
        } else {
            vec!["1.sst"]
        }
    );
    assert_eq!(table.stats.as_ref().unwrap().table_name, "t1");
    let dbs = load_backup_tables(&Context::background(), &reader, false).unwrap();
    assert!(dbs["test"].get_table("t1").unwrap().stats.is_none());
}
/// Go `load_test.go:TestLoadBackupMeta`.
#[test]
fn test_load_backup_meta() {
    check_load(false);
}
/// Go `load_test.go:TestLoadBackupMetaPartionTable` (Go spelling retained).
#[test]
fn test_load_backup_meta_partion_table() {
    check_load(true);
}
