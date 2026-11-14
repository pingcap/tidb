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

//! Go `br/pkg/task/show/cmd_test.go`, original archive bytes.
use super::*;
use tidb_proto::encryptionpb::EncryptionMethod;

fn config(path: &std::path::Path, encrypted: bool) -> Config {
    Config {
        storage: format!("local://{}", path.display()),
        cipher: CipherInfo {
            cipher_type: if encrypted {
                EncryptionMethod::Aes256Ctr
            } else {
                EncryptionMethod::Plaintext
            } as i32,
            cipher_key: if encrypted {
                vec![0x42; 32]
            } else {
                Vec::new()
            },
        },
    }
}

fn assert_tpcc(items: &ShowResult) {
    assert_eq!(items.cluster_id, 7211076907329653533);
    assert_eq!(items.cluster_version, "\"7.1.0-alpha\"\n");
    let mut names: Vec<_> = items
        .tables
        .iter()
        .map(|table| {
            assert_eq!(table.db_name, "tpcc");
            table.table_name.as_str()
        })
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "customer",
            "district",
            "history",
            "item",
            "new_order",
            "order_line",
            "orders",
            "stock",
            "warehouse"
        ]
    );
}

/// Go `TestFull`.
#[test]
fn test_full() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("backupmeta"),
        include_bytes!("../../../../../br/pkg/task/show/testdata/full-schema.meta"),
    )
    .unwrap();
    let ctx = Context::background();
    let items = create_exec(&ctx, config(dir.path(), false))
        .unwrap()
        .read(&ctx)
        .unwrap();
    assert_tpcc(&items);
    assert_eq!(items.end_version.0, 440689413714870273);
}

/// Go `TestV2AndSmallTables`.
#[test]
fn test_v2_and_small_tables() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../br/pkg/task/show/testdata/v2");
    let ctx = Context::background();
    let items = create_exec(&ctx, config(&path, false))
        .unwrap()
        .read(&ctx)
        .unwrap();
    assert_eq!(items.tables.len(), 500);
    assert_eq!(items.cluster_id, 7211076907329653533);
    assert_eq!(items.end_version.0, 440691270926467074);
}

/// Go `TestV2Encrypted`.
#[test]
fn test_v2_encrypted() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../br/pkg/task/show/testdata/v2-enc");
    let ctx = Context::background();
    let items = create_exec(&ctx, config(&path, true))
        .unwrap()
        .read(&ctx)
        .unwrap();
    assert_tpcc(&items);
}

/// Go `zzprobe/e07.TestMetadataEdges`; output retained in
/// `testdata/show-metadata-go.txt`. No archive/table model is invented here.
#[test]
fn test_metadata_edges_go_capture() {
    use prost::Message;
    use tidb_proto::backup::{BackupMeta, MetaFile, RawRange as PbRawRange, Schema};
    let read = |meta: BackupMeta| {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("backupmeta"), meta.encode_to_vec()).unwrap();
        let ctx = Context::background();
        let mut cfg = config(dir.path(), false);
        cfg.storage = format!("file://{}", dir.path().display());
        create_exec(&ctx, cfg).unwrap().read(&ctx)
    };
    let raw = read(BackupMeta {
        is_raw_kv: true,
        raw_ranges: vec![PbRawRange {
            cf: "default".into(),
            start_key: vec![1],
            end_key: vec![2],
        }],
        ..Default::default()
    })
    .unwrap();
    assert!(raw.tables.is_empty());
    assert_eq!(
        raw.raw_ranges,
        vec![RawRange {
            column_family: "default".into(),
            start_key: vec![1],
            end_key: vec![2]
        }]
    );
    let raw_v2 = read(BackupMeta {
        is_raw_kv: true,
        raw_range_index: Some(MetaFile::default()),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(raw_v2, "[BR:Common:ErrInvalidMetaFile]invalid metafile: show raw kv with backup meta v2 isn't supported for now");
    let reversed = read(BackupMeta {
        start_version: 2,
        end_version: 1,
        ..Default::default()
    })
    .unwrap_err();
    // Go's TimeStamp.String uses the process-local zone; the probe used UTC.
    assert_eq!(reversed, format!("[BR:Common:ErrInvalidMetaFile]invalid metafile: the start version({}) is greater than the end version({}), perhaps reading a backup meta from log backup", TimeStamp(2), TimeStamp(1)));
    let database = read(BackupMeta {
        schemas: vec![Schema {
            db: br#"{"id":42,"db_name":{"O":"Db","L":"db"}}"#.to_vec(),
            total_kvs: 123,
            total_bytes: 456,
            ..Default::default()
        }],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        database.tables,
        vec![Table {
            db_name: "Db".into(),
            table_name: String::new(),
            kv_count: 123,
            kv_size: 456,
            tiflash_replica: 0
        }]
    );
}
