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

//! Go `br/pkg/metautil` metadata reader used by `task/show`.
//! This owns the SkipFiles/SkipStats path; it is not a restore/import runner.

use crate::local_storage::LocalStorage;
use prost::Message;
use sha2::{Digest, Sha256};
use tidb_proto::backup::{BackupMeta, CipherInfo, MetaFile, Schema};

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
        let mut tables = Vec::new();
        let mut collect = |schema: &Schema| {
            if let Some(err) = ctx.err() {
                return Err(err.to_string());
            }
            tables.push(parse_schema_file(schema)?);
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
        Ok(tables)
    }
}

/// Go `parseSchemaFile` with `SkipStats` already applied.
fn parse_schema_file(schema: &Schema) -> Result<Table, String> {
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
            return Err(format!("[BR:Common:ErrInvalidMetaFile]invalid metafile: checksum mismatch expect {}, got {}",
                hex::encode(&node.sha256), hex::encode(checksum)));
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
