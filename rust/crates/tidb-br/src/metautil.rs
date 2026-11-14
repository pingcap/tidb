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

//! Go `br/pkg/metautil`: backup metadata writing and schema/file loading.

use crate::local_storage::LocalStorage;
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tidb_proto::backup::{BackupMeta, CipherInfo, MetaFile, Schema};

mod writer;
pub use writer::{AppendOp, MetaItem, MetaWriter, SizedMetaFile, encrypt};
pub use writer::{BACKUP_SCHEMA_VERSION, META_FILE_SIZE};

mod load;
pub use load::{Database, load_backup_tables};

/// Go `readSchemaConfig` / `ReadSchemaOption`.
#[derive(Clone, Copy, Default)]
pub struct ReadSchemaConfig {
    /// Go `SkipFiles`.
    pub skip_files: bool,
    /// Go `SkipStats`.
    pub skip_stats: bool,
}

/// Go `metautil.MetaReader`.
pub struct MetaReader {
    backup_meta: BackupMeta,
    storage: LocalStorage,
    cipher: CipherInfo,
}

/// Go `metautil.Table`, the schema and checksum fields used by SHOW.
pub struct Table {
    /// Go `Table.DB`.
    pub db: tidb_model::DBInfo,
    /// Go `Table.Info`; absent for a database-only entry.
    pub info: Option<tidb_model::TableInfo>,
    /// Go `Table.TotalKvs`.
    pub total_kvs: u64,
    /// Go `Table.TotalBytes`.
    pub total_bytes: u64,
    /// Go `Table.TiFlashReplicas`.
    pub tiflash_replicas: u32,
    /// Go `Table.Crc64Xor`.
    pub crc64_xor: u64,
    /// Go `Table.FilesOfPhysicals`.
    pub files_of_physicals: BTreeMap<i64, Vec<tidb_proto::backup::File>>,
    /// Go `Table.Stats`.
    pub stats: Option<tidb_stats_util::JsonTable>,
    /// Go `Table.StatsFileIndexes`.
    pub stats_file_indexes: Vec<tidb_proto::backup::StatsFileIndex>,
    /// Go `Table.IsMergeOptionAllowed`.
    pub is_merge_option_allowed: bool,
    /// Go `Table.PartitionMergeOptionAllowed`.
    pub partition_merge_option_allowed: std::collections::HashMap<String, bool>,
}

/// Go `ChecksumStats`.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct ChecksumStats {
    /// Go `Crc64Xor`.
    pub crc64_xor: u64,
    /// Go `TotalKvs`.
    pub total_kvs: u64,
    /// Go `TotalBytes`.
    pub total_bytes: u64,
}
impl ChecksumStats {
    /// Go `ChecksumStats.ChecksumExists`.
    pub fn checksum_exists(&self) -> bool {
        self.crc64_xor != 0 || self.total_kvs != 0 || self.total_bytes != 0
    }
}
impl Table {
    /// Go `Table.CalculateChecksumStatsOnFiles`.
    pub fn calculate_checksum_stats_on_files(&self) -> ChecksumStats {
        let mut stats = ChecksumStats::default();
        for file in self.files_of_physicals.values().flatten() {
            stats.crc64_xor ^= file.crc64xor;
            stats.total_kvs = stats.total_kvs.wrapping_add(file.total_kvs);
            stats.total_bytes = stats.total_bytes.wrapping_add(file.total_bytes);
        }
        stats
    }
}

/// Go `ArchiveSize`.
pub fn archive_size(files: &[tidb_proto::backup::File]) -> u64 {
    files
        .iter()
        .fold(0u64, |sum, file| sum.wrapping_add(file.size))
}
/// Go `ArchiveTableSize`.
pub fn archive_table_size(table: &Table) -> u64 {
    table
        .files_of_physicals
        .values()
        .fold(0u64, |sum, files| sum.wrapping_add(archive_size(files)))
}
/// Go `ArchiveTablesSize`.
pub fn archive_tables_size(tables: &[Table]) -> u64 {
    tables.iter().fold(0u64, |sum, table| {
        sum.wrapping_add(archive_table_size(table))
    })
}

impl MetaReader {
    /// Go `NewMetaReader`.
    pub fn new(backup_meta: BackupMeta, storage: LocalStorage, cipher: CipherInfo) -> Self {
        Self {
            backup_meta,
            storage,
            cipher,
        }
    }

    /// Go `MetaReader.GetBasic`.
    pub fn get_basic(&self) -> &BackupMeta {
        &self.backup_meta
    }

    /// Go `MetaReader.ReadSchemasFiles(ctx, out, SkipFiles, SkipStats)`.
    pub fn read_schemas_files(&self, ctx: &tidb_gocontext::Context) -> Result<Vec<Table>, String> {
        self.read_schemas_files_with_options(
            ctx,
            ReadSchemaConfig {
                skip_files: true,
                skip_stats: true,
            },
        )
    }

    /// Go `MetaReader.ReadSchemasFiles` with explicit options.
    pub fn read_schemas_files_with_options(
        &self,
        ctx: &tidb_gocontext::Context,
        opts: ReadSchemaConfig,
    ) -> Result<Vec<Table>, String> {
        let mut tables = Vec::new();
        let mut collect = |schema: &Schema| {
            if let Some(err) = ctx.err() {
                return Err(err.to_string());
            }
            tables.push(parse_schema_file(schema, opts.skip_stats)?);
            Ok(())
        };
        for schema in &self.backup_meta.schemas {
            collect(schema)?;
        }
        walk_leaf_meta_file(
            ctx,
            &self.storage,
            self.backup_meta.schema_index.as_ref(),
            &self.cipher,
            &mut |file| {
                for schema in &file.schemas {
                    collect(schema)?;
                }
                Ok(())
            },
        )?;
        let mut file_map: BTreeMap<i64, Vec<tidb_proto::backup::File>> = BTreeMap::new();
        if !opts.skip_files {
            for file in self.read_data_files(ctx)? {
                let physical_id = tidb_tablecodec::table_key::decode_table_id(&file.start_key);
                assert_ne!(physical_id, 0, "tableID must not equal to 0");
                file_map.entry(physical_id).or_default().push(file);
            }
        }
        let mut result = Vec::new();
        let mut iter = tables.into_iter();
        loop {
            // Go receiveBatch deduplicates table IDs within each batch of 1024.
            let mut batch = BTreeMap::new();
            for mut table in iter.by_ref().take(1024) {
                let id = if let Some(info) = &table.info {
                    if let Some(files) = file_map.get(&info.id) {
                        table.files_of_physicals.insert(info.id, files.clone());
                    }
                    if let Some(partition) = &info.partition {
                        for definition in partition.read().definitions.snapshot() {
                            if let Some(files) = file_map.get(&definition.id) {
                                table
                                    .files_of_physicals
                                    .insert(definition.id, files.clone());
                            }
                        }
                    }
                    info.id
                } else {
                    table.db.id
                };
                batch.insert(id, table);
            }
            if batch.is_empty() {
                break;
            }
            result.extend(batch.into_values());
        }
        Ok(result)
    }

    /// Go `MetaReader.readDataFiles`: read both inline and indexed files.
    pub fn read_data_files(
        &self,
        ctx: &tidb_gocontext::Context,
    ) -> Result<Vec<tidb_proto::backup::File>, String> {
        let mut files = self.backup_meta.files.clone();
        walk_leaf_meta_file(
            ctx,
            &self.storage,
            self.backup_meta.file_index.as_ref(),
            &self.cipher,
            &mut |meta| {
                files.extend(meta.data_files.iter().cloned());
                Ok(())
            },
        )?;
        Ok(files)
    }

    /// Go `MetaReader.ReadDDLs` / `readDDLs`.
    pub fn read_ddls(&self, ctx: &tidb_gocontext::Context) -> Result<Vec<u8>, String> {
        if let Some(error) = ctx.err() {
            return Err(error.to_string());
        }
        if self.backup_meta.version == 0 {
            return Ok(self.backup_meta.ddls.clone());
        }
        let mut ddls = Vec::new();
        walk_leaf_meta_file(
            ctx,
            &self.storage,
            self.backup_meta.ddl_indexes.as_ref(),
            &self.cipher,
            &mut |meta| {
                ddls.extend(meta.ddls.iter().cloned());
                Ok(())
            },
        )?;
        if ddls.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(writer::merge_ddls(&ddls))
        }
    }
}

/// Go `parseSchemaFile` with `SkipStats` applied before parsing.
fn parse_schema_file(schema: &Schema, skip_stats: bool) -> Result<Table, String> {
    Ok(Table {
        db: serde_json::from_slice(&schema.db).map_err(|e| e.to_string())?,
        info: if schema.table.is_empty() {
            None
        } else {
            Some(serde_json::from_slice(&schema.table).map_err(|e| e.to_string())?)
        },
        total_kvs: schema.total_kvs,
        total_bytes: schema.total_bytes,
        tiflash_replicas: schema.tiflash_replicas,
        crc64_xor: schema.crc64xor,
        files_of_physicals: BTreeMap::new(),
        stats: if skip_stats || schema.stats.is_empty() {
            None
        } else {
            serde_json::from_slice(&schema.stats).map_err(|e| e.to_string())?
        },
        stats_file_indexes: if skip_stats {
            Vec::new()
        } else {
            schema.stats_index.clone()
        },
        is_merge_option_allowed: schema.is_merge_option_allowed,
        partition_merge_option_allowed: schema.partition_merge_option_allowed.clone(),
    })
}

/// Go `walkLeafMetaFile`: decrypt and verify every child before decoding it.
fn walk_leaf_meta_file(
    ctx: &tidb_gocontext::Context,
    storage: &LocalStorage,
    file: Option<&MetaFile>,
    cipher: &CipherInfo,
    output: &mut dyn FnMut(&MetaFile) -> Result<(), String>,
) -> Result<(), String> {
    if let Some(err) = ctx.err() {
        return Err(err.to_string());
    }
    let Some(file) = file else { return Ok(()) };
    if file.meta_files.is_empty() {
        return output(file);
    }
    for node in &file.meta_files {
        let content = storage.read_file(&node.name)?;
        let content = crate::utils::decrypt(&content, Some(cipher), &node.cipher_iv)?;
        let checksum = Sha256::digest(&content);
        if node.sha256.as_slice() != checksum.as_slice() {
            return Err(format!(
                "[BR:Common:ErrInvalidMetaFile]invalid metafile: checksum mismatch expect {}, got {}",
                hex::encode(&node.sha256),
                hex::encode(checksum)
            ));
        }
        let child = MetaFile::decode(content.as_slice()).map_err(|e| e.to_string())?;
        walk_leaf_meta_file(ctx, storage, Some(&child), cipher, output)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_gocontext::Context;
    use tidb_proto::backup::File;

    fn cipher() -> CipherInfo {
        CipherInfo {
            cipher_type: tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32,
            ..Default::default()
        }
    }
    fn leaf(db: &str, table: &str) -> MetaFile {
        MetaFile {
            schemas: vec![Schema {
                db: db.as_bytes().to_vec(),
                table: table.as_bytes().to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Go `TestWalkMetaFileEmpty`.
    #[test]
    fn test_walk_meta_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap();
        let mut files = Vec::new();
        walk_leaf_meta_file(&Context::background(), &store, None, &cipher(), &mut |f| {
            files.push(f.clone());
            Ok(())
        })
        .unwrap();
        assert!(files.is_empty());
        walk_leaf_meta_file(
            &Context::background(),
            &store,
            Some(&MetaFile::default()),
            &cipher(),
            &mut |f| {
                files.push(f.clone());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(files, vec![MetaFile::default()]);
    }

    /// Go `TestWalkMetaFileLeaf`.
    #[test]
    fn test_walk_meta_file_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap();
        let file = leaf("db", "table");
        let mut files = Vec::new();
        walk_leaf_meta_file(
            &Context::background(),
            &store,
            Some(&file),
            &cipher(),
            &mut |f| {
                files.push(f.clone());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(files, vec![file]);
    }

    /// Go `TestWalkMetaFileInvalid`: no output from a leaf with a wrong checksum.
    #[test]
    fn test_walk_meta_file_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap();
        std::fs::write(dir.path().join("leaf"), leaf("db", "table").encode_to_vec()).unwrap();
        let root = MetaFile {
            meta_files: vec![File {
                name: "leaf".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = walk_leaf_meta_file(
            &Context::background(),
            &store,
            Some(&root),
            &cipher(),
            &mut |_| panic!("unreachable"),
        )
        .unwrap_err();
        assert!(err.contains("ErrInvalidMetaFile"));
    }

    /// Go `TestWalkMetaFile`: a two-level index and a leaf at the root.
    #[test]
    fn test_walk_meta_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStorage::from_url(dir.path().to_str().unwrap()).unwrap();
        let write = |name: &str, file: &MetaFile| {
            let bytes = file.encode_to_vec();
            std::fs::write(dir.path().join(name), &bytes).unwrap();
            File {
                name: name.into(),
                sha256: Sha256::digest(&bytes).to_vec(),
                ..Default::default()
            }
        };
        let expected = [
            leaf("db31S1", "table31S1"),
            leaf("db31S2", "table31S2"),
            leaf("db32S1", "table32S1"),
            leaf("db23S1", "table23S1"),
        ];
        let node21 = MetaFile {
            meta_files: vec![
                write("leaf31S1", &expected[0]),
                write("leaf31S2", &expected[1]),
            ],
            ..Default::default()
        };
        let node22 = MetaFile {
            meta_files: vec![write("leaf32S1", &expected[2])],
            ..Default::default()
        };
        let root = MetaFile {
            meta_files: vec![
                write("node21", &node21),
                write("node22", &node22),
                write("leaf23S1", &expected[3]),
            ],
            ..Default::default()
        };
        let mut files = Vec::new();
        walk_leaf_meta_file(
            &Context::background(),
            &store,
            Some(&root),
            &cipher(),
            &mut |f| {
                files.push(f.clone());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(files.len(), expected.len());
        for file in files {
            assert!(expected.contains(&file));
        }
    }
}
