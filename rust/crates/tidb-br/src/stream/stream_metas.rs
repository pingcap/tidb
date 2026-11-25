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


//! Go `stream/stream_metas.go`: the migrations of a log backup storage
//! (`v1/migrations/BASE` and the `<sn>_<hash>.mgrt` layers) and appending
//! one under the two-phase lock.

use std::sync::Arc;

use prost::Message as _;
use tidb_gocontext::Context;
use tidb_goruntime::go_crc64::update_iso;
use tidb_objstore::locking::{lock_with_retry, try_lock_remote_read, try_lock_remote_write, RemoteLock};
use tidb_objstore::storeapi::{Storage, WalkOption};
use tidb_proto::backup::{MetaEdit, Migration, MigrationVersion};

use crate::errors::{self, Result, ERR_INVALID_META_FILE, ERR_MIGRATION_VERSION_NOT_SUPPORTED, ERR_UNKNOWN};
use crate::operation::{self, LOCK_RESOURCE_MIGRATION_APPEND, LOCK_RESOURCE_MIGRATION_READ};

/// Go `baseMigrationSN`.
const BASE_MIGRATION_SN: i64 = 0;
/// Go `baseMigrationName`.
const BASE_MIGRATION_NAME: &str = "BASE";
/// Go `migrationPrefix`.
pub const MIGRATION_PREFIX: &str = "v1/migrations";
/// Go `lockPrefix`.
pub const LOCK_PREFIX: &str = "v1/LOCK";
/// Go `appendLockPrefix`.
pub const APPEND_LOCK_PREFIX: &str = "v1/APPEND_LOCK";
/// Go `SupportedMigVersion`.
const SUPPORTED_MIG_VERSION: MigrationVersion = MigrationVersion::M2;
/// Go `TruncateSafePointFileName`.
pub const TRUNCATE_SAFE_POINT_FILE_NAME: &str = "v1_stream_trancate_safepoint.txt";

fn objstore_err(e: tidb_objstore::Error) -> errors::Error {
    errors::errorf(e.to_string())
}

/// Go `NewMigration`.
#[must_use]
pub fn new_migration() -> Migration {
    Migration {
        version: MigrationVersion::M2 as i32,
        creator: format!(
            "br;commit={};branch={}",
            tidb_versioninfo::TIDB_GIT_HASH,
            tidb_versioninfo::TIDB_GIT_BRANCH
        ),
        ..Migration::default()
    }
}

/// Go `GetTSFromFile`: 0 when the file is absent.
///
/// # Errors
/// The read failed or the content isn't a number.
pub fn get_ts_from_file(ctx: &Context, s: &dyn Storage, filename: &str) -> Result<u64> {
    if !s.file_exists(ctx, filename).map_err(objstore_err)? {
        return Ok(0);
    }
    let data = s.read_file(ctx, filename).map_err(objstore_err)?;
    String::from_utf8_lossy(&data)
        .parse()
        .map_err(|_| errors::annotatef(&ERR_INVALID_META_FILE, "failed to parse the truncate safepoint"))
}

/// Go `OrderedMigration`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderedMigration {
    /// Go `SeqNum`.
    pub seq_num: i64,
    /// Go `Path`.
    pub path: String,
    /// Go `Content`.
    pub content: Migration,
}

/// Go `Migrations`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Migrations {
    /// Go `Base`.
    pub base: Migration,
    /// Go `Layers`, by sequence number.
    pub layers: Vec<OrderedMigration>,
}

impl Migrations {
    /// Go `ListAll`: the BASE, then the layers.
    #[must_use]
    pub fn list_all(&self) -> Vec<&Migration> {
        std::iter::once(&self.base).chain(self.layers.iter().map(|l| &l.content)).collect()
    }
}

/// Go `migIdOf`.
fn mig_id_of(s: &str) -> Result<i64> {
    if s == BASE_MIGRATION_NAME {
        return Ok(BASE_MIGRATION_SN);
    }
    if s.len() < 8 {
        return Err(errors::annotatef(
            &ERR_UNKNOWN,
            format!("migration name {s} is too short, perhaps `migrations` dir corrupted"),
        ));
    }
    s[..8].parse().map_err(|e: std::num::ParseIntError| {
        errors::errorf(format!("migration name {s} is not a valid number, perhaps `migrations` dir corrupted: {e}"))
    })
}

/// Go `hashMetaEdit`.
fn hash_meta_edit(meta_edit: &MetaEdit) -> u64 {
    let mut res = 0u64;
    for df in &meta_edit.delete_physical_files {
        res ^= update_iso(0, df.as_bytes());
    }
    for spans in &meta_edit.delete_logical_files {
        for span in &spans.spans {
            let crc = update_iso(0, spans.path.as_bytes());
            let crc = update_iso(crc, &span.offset.to_le_bytes());
            res ^= update_iso(crc, &span.length.to_le_bytes());
        }
    }
    res ^ update_iso(0, &[u8::from(meta_edit.destruct_self)])
}

/// Go `hashMigration`.
fn hash_migration(m: &Migration) -> u64 {
    let mut res = 0u64;
    for compaction in &m.compactions {
        res ^= compaction.artifacts_hash;
    }
    for meta_edit in &m.edit_meta {
        res ^= hash_meta_edit(meta_edit);
    }
    for ext_bkup in &m.ingested_sst_paths {
        res ^= update_iso(0, ext_bkup.as_bytes());
    }
    res ^ m.truncated_to
}

/// Go `nameOf`.
fn name_of(mig: &Migration, sn: i64) -> String {
    format!("{sn:08}_{:016X}.mgrt", hash_migration(mig))
}

/// Go `MigrationExt`.
#[derive(Clone)]
pub struct MigrationExt {
    s: Arc<dyn Storage>,
    prefix: String,
    operation_context: operation::Context,
}

/// Go `MigrationExtension`.
#[must_use]
pub fn migration_extension(s: Arc<dyn Storage>) -> MigrationExt {
    MigrationExt { s, prefix: MIGRATION_PREFIX.to_owned(), operation_context: operation::Context::default() }
}

impl MigrationExt {
    /// Go `WithOperationContext`: the context that tags the locks.
    #[must_use]
    pub fn with_operation_context(mut self, ctx: operation::Context) -> Self {
        self.operation_context = ctx;
        self
    }

    /// Go `Load`: the living migrations, the BASE apart.
    ///
    /// # Errors
    /// A migration is unreadable, misnamed, or of an unsupported version.
    pub fn load(&self, ctx: &Context) -> Result<Migrations> {
        let mut names = Vec::new();
        self.s
            .walk_dir(ctx, Some(&WalkOption { sub_dir: self.prefix.clone(), ..WalkOption::default() }), &mut |path, _| {
                names.push(path.to_owned());
                Ok(())
            })
            .map_err(objstore_err)?;
        let mut items = Vec::with_capacity(names.len());
        for name in names {
            let b = self
                .s
                .read_file(ctx, &name)
                .map_err(|e| errors::errorf(format!("during reading meta file {name} from storage: {e}")))?;
            let item = (|| {
                let base = name.rsplit('/').next().unwrap_or(&name);
                let seq_num = mig_id_of(base)?;
                let content = Migration::decode(b.as_slice()).map_err(|e| errors::errorf(e.to_string()))?;
                if content.version > SUPPORTED_MIG_VERSION as i32 {
                    return Err(errors::annotatef(
                        &ERR_MIGRATION_VERSION_NOT_SUPPORTED,
                        format!(
                            "the migration at {name} has version {}, the max version we support is {}",
                            content.version, SUPPORTED_MIG_VERSION as i32
                        ),
                    ));
                }
                let mut t = OrderedMigration { seq_num, path: name.clone(), content };
                if t.seq_num == BASE_MIGRATION_SN {
                    // The legacy truncation doesn't append a migration; its
                    // safe point tells a truncation happened.
                    let truncated_ts = get_ts_from_file(ctx, self.s.as_ref(), TRUNCATE_SAFE_POINT_FILE_NAME)
                        .map_err(|e| errors::annotate(e, "failed to get the truncate safepoint for base migration"))?;
                    t.content.truncated_to = t.content.truncated_to.max(truncated_ts);
                }
                Ok(t)
            })()
            .map_err(|e| errors::annotate(e, format!("failed to unmarshal file {name}")))?;
            items.push(item);
        }
        items.sort_by_key(|i| i.seq_num);
        if items.first().is_some_and(|i| i.seq_num == BASE_MIGRATION_SN) {
            let base = items.remove(0).content;
            return Ok(Migrations { base, layers: items });
        }
        // No BASE: `migrate-to` never ran.
        Ok(Migrations { base: new_migration(), layers: items })
    }

    /// Go `lockForAppend`: a read lock on `v1/LOCK` (restores may still
    /// read) and the write lock on `v1/APPEND_LOCK` (one appender).
    fn lock_for_append(&self, ctx: &Context, hint: &str) -> Result<(RemoteLock, RemoteLock)> {
        let read_input = self
            .operation_context
            .lock_meta(LOCK_RESOURCE_MIGRATION_READ, &format!("{hint} (read)"))
            .map_err(|e| errors::annotate(e, "failed to build migration read lock metadata"))?;
        let read_lock = lock_with_retry(ctx, try_lock_remote_read, &self.s, LOCK_PREFIX, &read_input)
            .map_err(|e| errors::annotate(objstore_err(e), "failed to acquire read lock for append operation"))?;
        let append_input = match self.operation_context.lock_meta(LOCK_RESOURCE_MIGRATION_APPEND, &format!("{hint} (append)")) {
            Ok(input) => input,
            Err(e) => {
                warn_unlock(read_lock.unlock_on_clean_up(ctx));
                return Err(errors::annotate(e, "failed to build migration append lock metadata"));
            }
        };
        match lock_with_retry(ctx, try_lock_remote_write, &self.s, APPEND_LOCK_PREFIX, &append_input) {
            Ok(append_lock) => Ok((read_lock, append_lock)),
            Err(e) => {
                warn_unlock(read_lock.unlock_on_clean_up(ctx));
                Err(errors::annotate(objstore_err(e), "failed to acquire append lock"))
            }
        }
    }

    /// Go `AppendMigration`: the migration as the next layer; returns its
    /// sequence number.
    ///
    /// # Errors
    /// The locks, the load or the write failed.
    pub fn append_migration(&self, ctx: &Context, mig: &Migration) -> Result<i64> {
        tidb_log::info("appending migration, trying to acquire two-phase lock", &[]);
        let (read_lock, append_lock) = self.lock_for_append(ctx, "AppendMigration")?;
        let res = (|| {
            let migs = self.load(ctx)?;
            let new_sn = migs.layers.last().map_or(1, |l| l.seq_num + 1);
            let name = format!("{MIGRATION_PREFIX}/{}", name_of(mig, new_sn));
            self.s.write_file(ctx, &name, &mig.encode_to_vec()).map_err(objstore_err)?;
            Ok(new_sn)
        })();
        warn_unlock(append_lock.unlock_on_clean_up(ctx));
        warn_unlock(read_lock.unlock_on_clean_up(ctx));
        res
    }
}

/// Go `UnlockOnCleanUp`'s warning.
fn warn_unlock(err: Option<tidb_objstore::Error>) {
    if let Some(err) = err {
        tidb_log::warn(
            "Failed to unlock a lock, you may need to manually delete it.",
            &[
                tidb_log::Field::new("pid", tidb_log::Value::U64(u64::from(std::process::id()))),
                tidb_log::Field::new("error", tidb_log::Value::Str(err.to_string())),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use tidb_proto::backup::{DeleteSpansOfFile, LogFileCompaction, Span};

    use super::*;

    /// Go `nameOf` over the same migrations gives these names (checked
    /// against the Go `hashMigration`).
    #[test]
    fn test_name_of_matches_go() {
        let m = Migration {
            ingested_sst_paths: vec!["v1/ext_backups/backup-0000000000000001/extbackupmeta".into()],
            truncated_to: 42,
            compactions: vec![LogFileCompaction { artifacts_hash: 0xdead_beef, ..Default::default() }],
            edit_meta: vec![MetaEdit {
                delete_physical_files: vec!["a".into(), "b".into()],
                delete_logical_files: vec![DeleteSpansOfFile {
                    path: "p".into(),
                    spans: vec![Span { offset: 1, length: 2 }],
                    ..Default::default()
                }],
                destruct_self: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(name_of(&m, 3), "00000003_0A9F88361531AFB2.mgrt");
        let m = Migration { ingested_sst_paths: vec!["x".into()], ..Default::default() };
        assert_eq!(name_of(&m, 1), "00000001_2310000000000000.mgrt");
    }

    fn test_ext() -> (MigrationExt, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s: Arc<dyn Storage> = Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
        (migration_extension(s).with_operation_context(operation::Context::new("test")), dir)
    }

    /// Appending numbers the layers after the last one, and the locks are
    /// released.
    #[test]
    fn test_append_migration() {
        let ctx = Context::background();
        let (ext, dir) = test_ext();
        let mut m = new_migration();
        m.ingested_sst_paths.push("v1/ext_backups/a/extbackupmeta".into());
        assert_eq!(ext.append_migration(&ctx, &m).unwrap(), 1);
        assert_eq!(ext.append_migration(&ctx, &m).unwrap(), 2);
        let migs = ext.load(&ctx).unwrap();
        assert_eq!(migs.layers.iter().map(|l| l.seq_num).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(migs.layers[0].content, m);
        let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("v1"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.contains("LOCK"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    /// A held write lock on `v1/LOCK` (a truncation) blocks the append's
    /// read lock until the context ends.
    #[test]
    fn test_append_migration_waits_for_write_lock() {
        let (ext, _dir) = test_ext();
        let input = tidb_objstore::locking::LockMetaInput { hint: "truncating".into(), ..Default::default() };
        let held = try_lock_remote_write(&Context::background(), &ext.s, LOCK_PREFIX, &input).unwrap();
        let (ctx, cancel) = Context::with_timeout(&Context::background(), std::time::Duration::from_millis(300));
        let err = ext.append_migration(&ctx, &new_migration()).unwrap_err();
        assert!(err.to_string().contains("failed to acquire read lock for append operation"), "{err}");
        cancel.cancel();
        held.unlock(&Context::background()).unwrap();
    }
}
