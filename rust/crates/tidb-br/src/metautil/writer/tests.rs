// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use super::*;

/// Go `metafile_test.go:TestMetaFileSize`.
#[test]
fn test_meta_file_size() {
    let file = |name: String| File {
        name,
        size: 99999,
        ..Default::default()
    };
    let mut metas = SizedMetaFile::new(50);
    assert!(!metas.append(MetaItem::DataFile(
        (0..6).map(|i| file(format!("f{i}"))).collect()
    )));
    assert!(metas.append(MetaItem::DataFile(vec![file("f5".into())])));
    let mut metas = SizedMetaFile::new(50);
    for i in 0..4 {
        assert!(!metas.append(MetaItem::MetaFile(file(format!("meta{i}")))));
    }
    assert!(metas.append(MetaItem::MetaFile(file("meta4".into()))));
}

/// Go `metafile_test.go:TestNewMetaWriterInitializesBackupSchemaVersion`.
#[test]
fn test_new_meta_writer_initializes_backup_schema_version() {
    let dir = tempfile::tempdir().unwrap();
    let writer = MetaWriter::new(
        LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap(),
        META_FILE_SIZE,
        false,
        "",
        None,
    );
    assert_eq!(writer.backupmeta().backup_schema_version, 1);
}

/// Go `metafile_test.go:TestEncryptAndDecrypt`, including invalid cipher/key
/// and plaintext cases; AES key sizes follow the owning Go crypto/aes API.
#[test]
fn test_encrypt_and_decrypt() {
    use tidb_proto::encryptionpb::EncryptionMethod as M;
    for (method, size) in [
        (M::Unknown, 0),
        (M::Plaintext, 0),
        (M::Aes128Ctr, 16),
        (M::Aes192Ctr, 24),
        (M::Aes256Ctr, 32),
    ] {
        let cipher = CipherInfo {
            cipher_type: method as i32,
            cipher_key: vec![b'0'; size],
        };
        let encrypted = encrypt(b"pingcap", Some(&cipher));
        if method == M::Unknown {
            assert!(encrypted.is_err());
            continue;
        }
        let (data, iv) = encrypted.unwrap();
        assert_eq!(
            crate::utils::decrypt(&data, Some(&cipher), &iv).unwrap(),
            b"pingcap"
        );
        if size != 0 {
            assert_ne!(data, b"pingcap");
            assert_eq!(iv.len(), 16);
            let wrong = CipherInfo {
                cipher_key: vec![b'1'; size],
                ..cipher.clone()
            };
            assert_ne!(
                crate::utils::decrypt(&data, Some(&wrong), &iv).unwrap(),
                b"pingcap"
            );
            let wrong = CipherInfo {
                cipher_key: vec![0; 15],
                ..cipher
            };
            assert!(encrypt(b"pingcap", Some(&wrong)).is_err());
        } else {
            assert_eq!(data, b"pingcap");
            assert!(iv.is_empty());
        }
    }
    assert_eq!(
        encrypt(b"pingcap", None).unwrap(),
        (b"pingcap".to_vec(), vec![])
    );
    assert_eq!(
        encrypt(b"", Some(&CipherInfo::default())).unwrap(),
        (vec![], vec![])
    );
}

/// Go `zzprobe/e07writer:TestMetaWriter` captured in testdata/metawriter-go.txt.
/// Exact bytes independently pin phase buffering, adjacent data-file batches,
/// the shared sequence, SHA-256, DDL merging, and size accounting for V1/V2.
#[test]
fn test_meta_writer() {
    let mut actual = String::new();
    for v2 in [false, true] {
        let ctx = Context::background();
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap();
        let mut writer = MetaWriter::new(storage.clone(), 16, v2, "", None);
        writer.update(|meta| {
            meta.start_version = 10;
            meta.end_version = 20;
        });
        writer.start_write_metas_async(&ctx, AppendOp::DataFile);
        for batch in [
            vec![("default.sst", 100), ("write.sst", 200)],
            vec![("other.sst", 300)],
        ] {
            writer
                .send(MetaItem::DataFile(
                    batch
                        .into_iter()
                        .map(|(name, size)| File {
                            name: name.into(),
                            size,
                            ..Default::default()
                        })
                        .collect(),
                ))
                .unwrap();
        }
        writer.finish_write_metas(&ctx, AppendOp::DataFile).unwrap();
        writer.start_write_metas_async(&ctx, AppendOp::Schema);
        writer
            .send(MetaItem::Schema(Schema {
                db: br#"{"id":1}"#.to_vec(),
                table: br#"{"id":123}"#.to_vec(),
                crc64xor: 7,
                total_kvs: 2,
                total_bytes: 10,
                stats_index: vec![tidb_proto::backup::StatsFileIndex {
                    size_enc: 15,
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .unwrap();
        writer.finish_write_metas(&ctx, AppendOp::Schema).unwrap();
        writer.start_write_metas_async(&ctx, AppendOp::Ddl);
        for ddl in [br#"{"id":8}"#, br#"{"id":9}"#] {
            writer.send(MetaItem::Ddl(ddl.to_vec())).unwrap();
        }
        writer.finish_write_metas(&ctx, AppendOp::Ddl).unwrap();
        writer.flush_backup_meta(&ctx).unwrap();
        actual.push_str(&format!(
            "E07 v2={v2} archive={} metas={}\n",
            writer.archive_size(),
            writer.meta_files_size()
        ));
        let mut names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        names.sort();
        for name in names {
            let name = name.to_str().unwrap();
            actual.push_str(&format!(
                "E07 {name} {}\n",
                hex::encode(storage.read_file(name).unwrap())
            ));
        }
    }
    assert_eq!(actual, include_str!("../../../testdata/metawriter-go.txt"));
}

/// Go `zzprobe/e07writer:TestMetadataEdges`: encrypted V1 and V2 DDLs,
/// archive checksums and local-file replacement, pinned to captured output.
#[test]
fn test_metadata_edges() {
    use crate::metautil::{MetaReader, ReadSchemaConfig, archive_table_size};
    use tidb_proto::encryptionpb::EncryptionMethod;
    let ctx = Context::background();
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap();
    let mut actual = String::new();
    for name in ["file", "/nested/file"] {
        let before = storage.file_exists(name).unwrap();
        storage.write_file(name, b"first").unwrap();
        storage.write_file(name, b"second").unwrap();
        let after = storage.file_exists(name).unwrap();
        let data = String::from_utf8(storage.read_file(name).unwrap()).unwrap();
        actual.push_str(&format!(
            "E07 local={name} before={before} after={after} data={data}\n"
        ));
    }
    let meta = BackupMeta {
        schemas: vec![Schema {
            db: br#"{"id":1}"#.to_vec(),
            table: br#"{"id":123}"#.to_vec(),
            ..Default::default()
        }],
        files: vec![
            File {
                start_key: tidb_tablecodec::table_key::encode_row_key(123, b"a"),
                crc64xor: 7,
                total_kvs: 2,
                total_bytes: 10,
                size: 100,
                ..Default::default()
            },
            File {
                start_key: tidb_tablecodec::table_key::encode_row_key(123, b"b"),
                crc64xor: 3,
                total_kvs: 4,
                total_bytes: 20,
                size: 200,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let reader = MetaReader::new(
        meta,
        storage.clone(),
        CipherInfo {
            cipher_type: EncryptionMethod::Plaintext as i32,
            ..Default::default()
        },
    );
    let tables = reader
        .read_schemas_files_with_options(&ctx, ReadSchemaConfig::default())
        .unwrap();
    let table = &tables[0];
    let stats = table.calculate_checksum_stats_on_files();
    actual.push_str(&format!(
        "E07 crc={} kvs={} bytes={} exists={} size={}\n",
        stats.crc64_xor,
        stats.total_kvs,
        stats.total_bytes,
        stats.checksum_exists(),
        archive_table_size(table)
    ));
    assert!(!crate::metautil::ChecksumStats::default().checksum_exists());
    assert_eq!(crate::metautil::archive_tables_size(&tables), 300);
    for v2 in [false, true] {
        let cipher = CipherInfo {
            cipher_type: EncryptionMethod::Aes256Ctr as i32,
            cipher_key: b"01234567890123456789012345678901".to_vec(),
        };
        let mut writer = MetaWriter::new(storage.clone(), 1, v2, "encrypted", Some(cipher.clone()));
        writer.start_write_metas_async(&ctx, AppendOp::Ddl);
        writer.send(MetaItem::Ddl(br#"{"id":1}"#.to_vec())).unwrap();
        writer.finish_write_metas(&ctx, AppendOp::Ddl).unwrap();
        writer.flush_backup_meta(&ctx).unwrap();
        let bytes = storage.read_file("encrypted").unwrap();
        let plain = crate::utils::decrypt(&bytes[16..], Some(&cipher), &bytes[..16]).unwrap();
        let meta = BackupMeta::decode(plain.as_slice()).unwrap();
        let version = meta.version;
        let schema = meta.backup_schema_version;
        let reader = MetaReader::new(meta, storage.clone(), cipher);
        let ddls = String::from_utf8(reader.read_ddls(&ctx).unwrap()).unwrap();
        actual.push_str(&format!(
            "E07 encrypted={v2} ddl={ddls} version={version} schema={schema}\n"
        ));
    }
    assert_eq!(
        actual,
        include_str!("../../../testdata/metadata-edges-go.txt")
    );
}
