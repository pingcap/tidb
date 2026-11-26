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

//! Go `br/pkg/task/restore.go`: the snapshot restore task SQL `RESTORE`
//! runs (`RunRestore`, `runSnapshotRestore` and their checks). Log restore
//! and PiTR (`RunStreamRestore`, the table mapping and blocklists), restore
//! name routing (`--rename`), the repo layout, EBS and raw / txn restore
//! have no SQL caller and are not ported.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use sha2::Digest as _;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::{ActionType, Job};
use tidb_proto::backup::BackupMeta;

use super::common::Config;
use crate::checkpoint::gojson;
use crate::checkpoint::manager::{new_snapshot_table_meta_manager, SnapshotMetaManager};
use crate::checkpoint::restore::PreallocIDs as CheckpointPreallocIDs;
use crate::checkpoint::storage::{is_checkpoint_db, SNAPSHOT_RESTORE_CHECKPOINT_DATABASE_NAME};
use crate::config::ConfigTerm;
use crate::conn::{Mgr, VersionCheckerType};
use crate::errors::{self, Result};
use crate::glue::{Domain, Glue};
use crate::metautil;
use crate::registry::{new_restore_registry, RegistrationInfo, Registry};
use crate::restore::misc::{
    get_table_schema, get_ts_with_retry, has_restore_id_column, transfer_bool_to_value,
    UniqueTableName,
};
use crate::restore::snap_client::SnapClient;
use crate::utils::filter::{match_schema, match_table};
use crate::utils::misc::{enclose_db_and_table, enclose_name, get_sys_db_cistr_name, is_sys_db};

/// Go `FullRestoreCmd`.
pub const FULL_RESTORE_CMD: &str = "Full Restore";
/// Go `DBRestoreCmd`.
pub const DB_RESTORE_CMD: &str = "DataBase Restore";
/// Go `TableRestoreCmd`.
pub const TABLE_RESTORE_CMD: &str = "Table Restore";
/// Go `PointRestoreCmd`.
pub const POINT_RESTORE_CMD: &str = "Point Restore";

/// Go `defaultRestoreConcurrency`.
const DEFAULT_RESTORE_CONCURRENCY: u32 = 128;
/// Go `defaultPDConcurrency`.
const DEFAULT_PD_CONCURRENCY: u32 = 1;
/// Go `defaultRegionScanConcurrency`.
const DEFAULT_REGION_SCAN_CONCURRENCY: usize = 256;
/// Go `defaultStatsConcurrency`.
const DEFAULT_STATS_CONCURRENCY: usize = 12;
/// Go `defaultBatchFlushInterval`.
const DEFAULT_BATCH_FLUSH_INTERVAL: Duration = Duration::from_secs(16);
/// Go `defaultFlagDdlBatchSize`.
const DEFAULT_FLAG_DDL_BATCH_SIZE: usize = 128;
/// Go `defaultSwitchInterval`.
const DEFAULT_SWITCH_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Go `restore.CoarseGrained`.
const COARSE_GRAINED: &str = "coarse-grained";

/// Go `RestoreConfig` with its embedded `RestoreCommonConfig`: the fields a
/// snapshot restore reads, with Go's flag defaults from
/// [`default_restore_config`].
pub struct RestoreConfig {
    /// Go embedded `Config`.
    pub config: Config,
    /// Go `Online`.
    pub online: bool,
    /// Go `Granularity` (deprecated).
    pub granularity: String,
    /// Go `ConcurrencyPerStore`.
    pub concurrency_per_store: ConfigTerm<u64>,
    /// Go `MergeSmallRegionSizeBytes`.
    pub merge_small_region_size_bytes: ConfigTerm<u64>,
    /// Go `MergeSmallRegionKeyCount`.
    pub merge_small_region_key_count: ConfigTerm<u64>,
    /// Go `WithSysTable`.
    pub with_sys_table: bool,
    /// Go `SysCheckCollation`.
    pub sys_check_collation: bool,
    /// Go `NoSchema`.
    pub no_schema: bool,
    /// Go `LoadStats`.
    pub load_stats: bool,
    /// Go `FastLoadSysTables`: on by default, but only a full restore with
    /// no filter loads physically, which SQL `RESTORE` never is.
    pub fast_load_sys_tables: bool,
    /// Go `PDConcurrency` (deprecated).
    pub pd_concurrency: u32,
    /// Go `RegionScanConcurrency`.
    pub region_scan_concurrency: usize,
    /// Go `SplitRegionIndexStep`.
    pub split_region_index_step: usize,
    /// Go `CoarseScatter`.
    pub coarse_scatter: bool,
    /// Go `StatsConcurrency`.
    pub stats_concurrency: usize,
    /// Go `BatchFlushInterval`.
    pub batch_flush_interval: Duration,
    /// Go `DdlBatchSize`.
    pub ddl_batch_size: usize,
    /// Go `WithPlacementPolicy`.
    pub with_placement_policy: String,
    /// Go `AllowPITRFromIncremental`.
    pub allow_pitr_from_incremental: bool,
    /// Go `UseCheckpoint`.
    pub use_checkpoint: bool,
    /// Go `UpstreamClusterID`.
    pub upstream_cluster_id: u64,
    /// Go `RestoreID`.
    pub restore_id: u64,
    /// Go `WaitTiflashReady`.
    pub wait_tiflash_ready: bool,
    /// Go `RestoreStartTS`.
    pub restore_start_ts: u64,
    /// Go `RestoreTS` (log restore's; 0 for a snapshot restore).
    pub restore_ts: u64,
    /// Go `StartTS` (log restore's; 0 for a snapshot restore).
    pub start_ts: u64,
    snapshot_checkpoint_meta_manager: Option<Arc<dyn SnapshotMetaManager>>,
    restore_registry: Option<Arc<Registry>>,
}

/// Go `DefaultRestoreConfig`: the restore flags' defaults over `config`.
#[must_use]
pub fn default_restore_config(config: Config) -> RestoreConfig {
    RestoreConfig {
        config,
        online: false,
        granularity: COARSE_GRAINED.to_owned(),
        concurrency_per_store: ConfigTerm {
            value: crate::conn::DEFAULT_IMPORT_NUM_GOROUTINES,
            modified: false,
        },
        merge_small_region_size_bytes: ConfigTerm {
            value: crate::conn::DEFAULT_MERGE_REGION_SIZE_BYTES,
            modified: false,
        },
        merge_small_region_key_count: ConfigTerm {
            value: crate::conn::DEFAULT_MERGE_REGION_KEY_COUNT,
            modified: false,
        },
        with_sys_table: true,
        sys_check_collation: true,
        no_schema: false,
        load_stats: true,
        fast_load_sys_tables: true,
        pd_concurrency: DEFAULT_PD_CONCURRENCY,
        region_scan_concurrency: DEFAULT_REGION_SCAN_CONCURRENCY,
        split_region_index_step: crate::restore::split::DEFAULT_REGION_INDEX_STEP,
        coarse_scatter: false,
        stats_concurrency: DEFAULT_STATS_CONCURRENCY,
        batch_flush_interval: DEFAULT_BATCH_FLUSH_INTERVAL,
        ddl_batch_size: DEFAULT_FLAG_DDL_BATCH_SIZE,
        with_placement_policy: "STRICT".to_owned(),
        allow_pitr_from_incremental: true,
        use_checkpoint: true,
        upstream_cluster_id: 0,
        restore_id: 0,
        wait_tiflash_ready: false,
        restore_start_ts: 0,
        restore_ts: 0,
        start_ts: 0,
        snapshot_checkpoint_meta_manager: None,
        restore_registry: None,
    }
}

impl RestoreConfig {
    /// Go `RestoreConfig.Adjust`.
    pub fn adjust(&mut self) {
        self.config.adjust();
        // Go `RestoreCommonConfig.adjust`.
        if !self.merge_small_region_key_count.modified {
            self.merge_small_region_key_count.value = crate::conn::DEFAULT_MERGE_REGION_KEY_COUNT;
        }
        if !self.merge_small_region_size_bytes.modified {
            self.merge_small_region_size_bytes.value = crate::conn::DEFAULT_MERGE_REGION_SIZE_BYTES;
        }
        if self.granularity.is_empty() {
            self.granularity = COARSE_GRAINED.to_owned();
        }
        if !self.concurrency_per_store.modified {
            self.concurrency_per_store.value = crate::conn::DEFAULT_IMPORT_NUM_GOROUTINES;
        }
        if self.config.concurrency == 0 {
            self.config.concurrency = DEFAULT_RESTORE_CONCURRENCY;
        }
        if self.config.switch_mode_interval.is_zero() {
            self.config.switch_mode_interval = DEFAULT_SWITCH_INTERVAL;
        }
        if self.pd_concurrency == 0 {
            self.pd_concurrency = DEFAULT_PD_CONCURRENCY;
        }
        self.split_region_index_step =
            crate::restore::split::normalize_region_index_step(self.split_region_index_step);
        if self.stats_concurrency == 0 {
            self.stats_concurrency = DEFAULT_STATS_CONCURRENCY;
        }
        if self.batch_flush_interval.is_zero() {
            self.batch_flush_interval = DEFAULT_BATCH_FLUSH_INTERVAL;
        }
        if self.ddl_batch_size == 0 {
            self.ddl_batch_size = DEFAULT_FLAG_DDL_BATCH_SIZE;
        }
    }

    /// Go `LocalEncryptionEnabled`.
    #[must_use]
    pub fn local_encryption_enabled(&self) -> bool {
        self.config.cipher_info.cipher_type
            != tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32
    }

    /// Go `RestoreConfig.Hash`: the SHA-256 of Go's `json.Marshal` of the
    /// configuration a checkpointed restore must keep.
    #[must_use]
    pub fn hash(&self, cmd_name: &str) -> Vec<u8> {
        let hash = sha2::Sha256::digest(self.immutable_json(
            cmd_name,
            &storage_url_for_restore_hash(&self.config.storage),
        ));
        tidb_log::info(
            "hash of restore config",
            &[
                Field::new("hash", Value::Str(hex::encode(hash))),
                Field::new(
                    "config",
                    Value::Str(self.immutable_json(
                        cmd_name,
                        &tidb_objstore::redact_url(&self.config.storage),
                    )),
                ),
            ],
        );
        hash.to_vec()
    }

    /// Go `json.Marshal(immutableRestoreConfig{...})` without a repo layout
    /// or routes, whose fields are `omitempty`. Go never sets
    /// `ExplictFilter`, so it is always `false`.
    fn immutable_json(&self, cmd_name: &str, storage: &str) -> String {
        let b = |v: bool| if v { "true" } else { "false" };
        let filters = if self.config.filter_str.is_empty() {
            "null".to_owned()
        } else {
            format!(
                "[{}]",
                self.config
                    .filter_str
                    .iter()
                    .map(|f| gojson::string(f))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        format!(
            "{{\"CmdName\":{},\"UpstreamClusterID\":{},\"Storage\":{},\"ExplictFilter\":{},\"FilterStr\":{filters},\"WithSysTable\":{},\"FastLoadSysTables\":{},\"LoadStats\":{}}}",
            gojson::string(cmd_name),
            self.upstream_cluster_id,
            gojson::string(storage),
            b(false),
            b(self.with_sys_table),
            b(self.fast_load_sys_tables),
            b(self.load_stats),
        )
    }

    /// Go `CloseCheckpointMetaManager`.
    fn close_checkpoint_meta_manager(&self) {
        if let Some(m) = &self.snapshot_checkpoint_meta_manager {
            m.close();
        }
    }
}

/// Go `storageURLForRestoreHash`: the legacy redacted URL a checkpoint's
/// identity is frozen to. Credentials are masked; other parts stay.
#[must_use]
pub fn storage_url_for_restore_hash(raw_url: &str) -> String {
    if !raw_url.contains("://") && !raw_url.contains('?') {
        return raw_url.to_owned();
    }
    let Ok(mut u) = tidb_go_url::parse(raw_url) else {
        return "(invalid URL)".to_owned();
    };
    u.user = None;
    let scheme = String::from_utf8_lossy(&u.scheme).to_lowercase();
    let keys: &[&str] = match scheme.as_str() {
        "s3" | "ks3" | "oss" => &[
            "access-key",
            "secret-access-key",
            "session-token",
            "sse-kms-context",
        ],
        "azure" | "azblob" => &["account-key", "encryption-key", "sas-token", "endpoint"],
        _ => &[],
    };
    if !keys.is_empty() {
        u.fragment.clear();
        u.raw_fragment.clear();
        let (mut values, err) = tidb_go_url::parse_query(&u.raw_query);
        if err.is_some() {
            u.raw_query.clear();
            return u.string();
        }
        let names: Vec<Vec<u8>> = values.0.keys().cloned().collect();
        for key in names {
            let normalized = String::from_utf8_lossy(&key)
                .replace('_', "-")
                .to_lowercase();
            if keys.contains(&normalized.as_str()) {
                // Go keeps its legacy handling of a split encryption context.
                if normalized == "sse-kms-context"
                    && values.0[&key]
                        .iter()
                        .any(|v| serde_json::from_slice::<serde_json::Value>(v).is_err())
                {
                    u.raw_query.clear();
                    return u.string();
                }
                values.0.insert(key, vec![b"xxxxxx".to_vec()]);
            }
        }
        u.raw_query = values.encode();
    }
    u.string()
}

/// Go `isFullRestore`.
#[must_use]
pub fn is_full_restore(cmd_name: &str) -> bool {
    cmd_name == FULL_RESTORE_CMD
}

/// Go `IsStreamRestore`.
#[must_use]
pub fn is_stream_restore(cmd_name: &str) -> bool {
    cmd_name == POINT_RESTORE_CMD
}

fn log_err(msg: &str, err: &errors::Error) {
    tidb_log::warn(msg, &[Field::new("error", Value::Str(err.to_string()))]);
}

/// Go `ReadBackupMeta` (and `SnapshotStorageRef.LoadBackupMeta` for the
/// legacy layout): the storage and the backup meta, its schema version
/// checked. Go also refuses unknown protobuf fields, which this port's
/// decoder drops unseen.
///
/// # Errors
/// The storage, read, decrypt or decode error, or a newer schema version.
pub fn read_backup_meta(
    ctx: &Context,
    cfg: &Config,
) -> Result<(
    tidb_proto::backup::StorageBackend,
    metautil::Storage,
    BackupMeta,
)> {
    use prost::Message as _;
    let (u, s) = super::get_storage(
        ctx,
        &cfg.storage,
        &cfg.backend_options,
        cfg.no_creds,
        cfg.send_creds,
    )
    .map_err(errors::errorf)?;
    let data = s
        .read_file(ctx, metautil::META_FILE)
        .map_err(|e| errors::annotate(errors::errorf(e.to_string()), "load backupmeta failed"))?;
    let data = super::decrypt_full_backup_meta_if_needed(&data, &cfg.cipher_info)
        .map_err(errors::errorf)?;
    let meta = BackupMeta::decode(data.as_slice()).map_err(|e| {
        errors::annotate(
            errors::errorf(e.to_string()),
            "parse backupmeta failed because of wrong aes cipher",
        )
    })?;
    if meta.backup_schema_version > metautil::BACKUP_SCHEMA_VERSION {
        let err = errors::annotatef(
            &errors::ERR_VERSION_MISMATCH,
            format!(
                "backupmeta requires schema version {}, current BR supports up to {}. restoring with an older BR may silently ignore newer backup metadata semantics. backup cluster version: {}, backup BR version: {}. use --check-requirements=false to skip this check",
                meta.backup_schema_version,
                metautil::BACKUP_SCHEMA_VERSION,
                meta.cluster_version,
                meta.br_version
            ),
        );
        if cfg.check_requirements {
            return Err(err);
        }
        log_err("skip backupmeta compatibility check error", &err);
    }
    Ok((u, s, meta))
}

/// Go `checkSnapshotRestoreMode`: a raw or txn KV backup cannot be restored
/// as tables.
fn check_snapshot_restore_mode(ctx: &Context, cfg: &RestoreConfig) -> Result<()> {
    let (_, _, meta) = read_backup_meta(ctx, &cfg.config)?;
    if meta.is_raw_kv || meta.is_txn_kv {
        return Err(errors::annotate(
            errors::new(&errors::ERR_RESTORE_MODE_MISMATCH),
            "cannot do transactional restore from raw/txn kv data",
        ));
    }
    Ok(())
}

/// Go `CheckNewCollationEnable`: the backup's new-collation setting must
/// be the cluster's.
///
/// # Errors
/// [`errors::ERR_UNKNOWN`] when they differ, or (with the requirement
/// check) when the backup has none.
pub fn check_new_collation_enable(
    backup_new_collation_enable: &str,
    g: &dyn Glue,
    check_requirements: bool,
) -> Result<bool> {
    let se = g.create_session()?;
    let new_collation_enable =
        se.get_global_variable(crate::utils::misc::TIDB_NEW_COLLATION_ENABLED)?;
    // Go sets the collate package's switch from the cluster here; this
    // node's collations already follow the cluster's bootstrap.
    let enabled = new_collation_enable == "True";
    tidb_log::info(
        &format!("set {}", crate::utils::misc::TIDB_NEW_COLLATION_ENABLED),
        &[Field::new("new_collation_enabled", Value::Bool(enabled))],
    );
    let name = crate::utils::misc::TIDB_NEW_COLLATION_ENABLED;
    if backup_new_collation_enable.is_empty() {
        if check_requirements {
            return Err(errors::annotatef(
                &errors::ERR_UNKNOWN,
                format!(
                    "the value '{name}' not found in backupmeta. you can use \"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='{name}';\" to manually check the config. if you ensure the value '{name}' in backup cluster is as same as restore cluster, use --check-requirements=false to skip this check"
                ),
            ));
        }
        tidb_log::warn(&format!("the config '{name}' is not in backupmeta"), &[]);
        return Ok(enabled);
    }
    if !backup_new_collation_enable.eq_ignore_ascii_case(&new_collation_enable) {
        return Err(errors::annotatef(
            &errors::ERR_UNKNOWN,
            format!("the config '{name}' not match, upstream:{backup_new_collation_enable}, downstream: {new_collation_enable}"),
        ));
    }
    Ok(enabled)
}

/// Go `VerifyDBAndTableInBackup`: the `restore db` / `restore table`
/// targets were backed up.
///
/// # Errors
/// [`errors::ERR_UNDEFINED_RESTORE_DB_OR_TABLE`].
pub fn verify_db_and_table_in_backup(
    schemas: &[metautil::Database],
    cfg: &RestoreConfig,
) -> Result<()> {
    if cfg.config.schemas.is_empty() && cfg.config.tables.is_empty() {
        return Ok(());
    }
    let mut schemas_map = HashSet::new();
    let mut tables_map = HashSet::new();
    for db in schemas {
        let mut db_name = db.info.name.lowercase().to_owned();
        let (sys, ok) = get_sys_db_cistr_name(&db.info.name);
        if ok && is_sys_db(sys.original()) {
            db_name = sys.lowercase().to_owned();
        }
        schemas_map.insert(enclose_name(&db_name));
        for t in &db.tables {
            if let Some(info) = &t.info {
                tables_map.insert(enclose_db_and_table(&db_name, info.name.lowercase()));
            }
        }
    }
    for schema in &cfg.config.schemas {
        if !schemas_map.contains(&schema.to_lowercase()) {
            return Err(errors::annotatef(
                &errors::ERR_UNDEFINED_RESTORE_DB_OR_TABLE,
                format!("[database: {schema}] has not been backup, please ensure you has input a correct database name"),
            ));
        }
    }
    for table in &cfg.config.tables {
        if !tables_map.contains(&table.to_lowercase()) {
            return Err(errors::annotatef(
                &errors::ERR_UNDEFINED_RESTORE_DB_OR_TABLE,
                format!("[table: {table}] has not been backup, please ensure you has input a correct table name"),
            ));
        }
    }
    Ok(())
}

/// Go `streamhelper.PrefixOfTask()`.
const LOG_BACKUP_TASK_PREFIX: &str = "/tidb/br-stream/info/";

/// Go `checkConflictingLogBackup` for a snapshot restore: a running log
/// backup is fine only for a filtered, unencrypted restore into an
/// unencrypted task.
fn check_conflicting_log_backup(
    cfg: &RestoreConfig,
    etcd: &dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps,
) -> Result<()> {
    if cfg.config.pd.is_empty() {
        return Err(errors::annotatef(
            &errors::ERR_INVALID_ARGUMENT,
            "the command needs access to PD, please specify `-u` or `--pd`",
        ));
    }
    let kvs = etcd
        .get_prefix(LOG_BACKUP_TASK_PREFIX)
        .map_err(errors::errorf)?;
    for (_, value) in kvs {
        let task =
            <tidb_proto::backup::StreamBackupTaskInfo as prost::Message>::decode(value.as_slice())
                .map_err(|e| errors::errorf(e.to_string()))?;
        let base = format!(
            "log backup task is running: {}, and isn't compatible with your restore.You may check the extra information to get rid of this. If that doesn't work, you may stop the task before restore, and after the restore operation finished, create log-backup task again and create a full backup on this cluster.",
            task.name
        );
        let base = || errors::errorf(base.clone());
        if !cfg.config.user_filtered() {
            return Err(errors::annotate(
                base(),
                "you want to restore a whole cluster, you may use `-f` or `restore table|database` to specify the tables to restore to continue",
            ));
        }
        if cfg.local_encryption_enabled() {
            return Err(errors::annotate(base(), "the data you want to restore is encrypted, they cannot be copied to the log storage"));
        }
        if task
            .security_config
            .as_ref()
            .is_some_and(|s| s.encryption.is_some())
        {
            return Err(errors::annotate(base(), "the running log backup task is encrypted, the data copied to the log storage cannot work"));
        }
    }
    Ok(())
}

/// Go `restoreOperationCommandName`.
fn restore_operation_command_name(cmd_name: &str) -> &str {
    if is_stream_restore(cmd_name) {
        return "log-restore";
    }
    cmd_name
}

/// The server's etcd client as the reads of Go's `MetaDataClient` the PITR
/// collector makes; the advancer's writes and watches are not needed here.
struct EtcdOpsMetaStore(Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>);

impl crate::streamhelper::MetaStore for EtcdOpsMetaStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = String::from_utf8_lossy(key).into_owned();
        Ok(self.0.get_prefix(&key).map_err(errors::errorf)?.into_iter().find(|(k, _)| *k == key).map(|(_, v)| v))
    }
    fn get_prefix(&self, prefix: &[u8]) -> Result<crate::streamhelper::client::PrefixRead> {
        let kvs = self.0.get_prefix(&String::from_utf8_lossy(prefix)).map_err(errors::errorf)?;
        Ok((kvs.into_iter().map(|(k, v)| (k.into_bytes(), v)).collect(), 0))
    }
    fn scan(&self, from: &[u8], to: &[u8], limit: usize) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)> {
        self.0.scan(from, to, limit).map_err(errors::errorf)
    }
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.put(&String::from_utf8_lossy(key), value).map_err(errors::errorf)
    }
    // The collector makes none of the advancer's deadline-bounded metadata
    // requests; the server's etcd client bounds each call itself.
    fn get_with_timeout(&self, key: &[u8], _timeout: std::time::Duration) -> Result<Option<Vec<u8>>> {
        self.get(key)
    }
    fn put_with_timeout(&self, key: &[u8], value: &[u8], _timeout: std::time::Duration) -> Result<()> {
        self.put(key, value)
    }
    fn delete(&self, key: &[u8]) -> Result<()> {
        self.0.delete(&String::from_utf8_lossy(key)).map_err(errors::errorf)
    }
    fn watch_prefix(
        &self,
        _prefix: &[u8],
        _start_revision: i64,
        _is_cancelled: Box<dyn Fn() -> bool + Send>,
        _on_response: Box<dyn Fn(&tidb_pd_client::WatchResponse) + Send>,
    ) -> Result<Box<dyn Send>> {
        Err(errors::errorf("watching is not supported over the restore's etcd client"))
    }
}

/// Go `checkIncompatibleChangefeed`. The threshold is BackupTS for snapshot
/// restore and the target TS for PITR.
fn check_incompatible_changefeed(
    restore_ts: u64,
    etcd: &dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps,
) -> Result<()> {
    let names = tidb_cdcutil::get_incompatible_changefeeds_with_safe_ts(etcd, restore_ts)
        .map_err(errors::errorf)?;
    if !names.is_empty() {
        return Err(errors::errorf(format!(
            "cannot restore: {}restoreTS is BackupTS for snapshot restore or the target timestamp for PITR. \
             The effective checkpointTS is the maximum of the changefeed checkpoint-ts and start-ts. \
             Please advance the changefeed checkpoint to at least restoreTS, or remove the incompatible changefeed(s) before retrying restore; \
             pausing a changefeed alone does not resolve the timestamp conflict",
            names.message_to_user()
        )));
    }
    Ok(())
}

/// The server's etcd client as the task register's `clientv3` calls.
struct RegisterEtcdOps(Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>);

impl crate::utils::register::RegisterEtcd for RegisterEtcdOps {
    fn get_key_lease(&self, key: &str) -> std::result::Result<Option<i64>, String> {
        self.0.get_key_lease(key)
    }
    fn lease_grant(&self, ttl_seconds: i64) -> std::result::Result<i64, String> {
        self.0.lease_grant(ttl_seconds)
    }
    fn put_with_lease(
        &self,
        key: &str,
        value: &[u8],
        lease: i64,
    ) -> std::result::Result<(), String> {
        self.0.put_with_lease(key, value, lease)
    }
    fn lease_keep_alive_once(&self, lease: i64) -> std::result::Result<(), String> {
        self.0.lease_keep_alive_once(lease)
    }
    fn lease_revoke(&self, lease: i64) -> std::result::Result<(), String> {
        self.0.lease_revoke(lease)
    }
}

/// Go `registerTaskToPD`: `/tidb/brie/import/restore/restore-<uuid>`, so
/// TiCDC and log backup see the restore.
fn register_task_to_pd(
    etcd: Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>,
) -> Result<crate::utils::register::TaskRegister> {
    let mut register = crate::utils::register::TaskRegister::new(
        Arc::new(RegisterEtcdOps(etcd)),
        crate::utils::register::RegisterTaskType::Restore,
        &format!("restore-{}", uuid::Uuid::new_v4()),
    );
    register.register_task().map_err(errors::errorf)?;
    Ok(register)
}

/// Go `RegisterRestoreIfNeeded`: a registration in the restore registry,
/// with its heartbeat, unless the cluster predates it.
fn register_restore_if_needed(
    ctx: &Context,
    cfg: &mut RestoreConfig,
    cmd_name: &str,
    dom: &Arc<dyn Domain>,
) -> Result<()> {
    if cfg.restore_id != 0 {
        tidb_log::info(
            "restore task already registered, skipping re-registration",
            &[
                Field::new("restoreID", Value::U64(cfg.restore_id)),
                Field::new("cmdName", Value::Str(cmd_name.into())),
            ],
        );
        return Ok(());
    }
    if !has_restore_id_column(dom) {
        tidb_log::info("skipping restore registration since target TiDB version doesn't support restore registry", &[]);
        return Ok(());
    }
    let Some(registry) = cfg.restore_registry.clone() else {
        return Ok(());
    };
    let info = RegistrationInfo {
        // Go `SnapshotRegistrationFilterHashInput` without a repo backup ID.
        filter_hash_input: cfg
            .config
            .filter_str
            .join(crate::registry::FILTER_SEPARATOR),
        start_ts: cfg.start_ts,
        restored_ts: cfg.restore_ts,
        filter_strings: cfg.config.filter_str.clone(),
        upstream_cluster_id: cfg.upstream_cluster_id,
        with_sys_table: cfg.with_sys_table,
        cmd: cmd_name.to_owned(),
        ..Default::default()
    };
    // A snapshot restore never has a user-specified restored TS.
    let (restore_id, resolved) = registry.resume_or_create_registration(ctx, info, false)?;
    cfg.restore_id = restore_id;
    if resolved != cfg.restore_ts {
        tidb_log::info(
            "registry resolved a different RestoreTS, updating config",
            &[
                Field::new("original_restored_ts", Value::U64(cfg.restore_ts)),
                Field::new("resolved_restored_ts", Value::U64(resolved)),
                Field::new("restore_id", Value::U64(restore_id)),
            ],
        );
        cfg.restore_ts = resolved;
    }
    tidb_log::info(
        "registered restore task",
        &[
            Field::new("restoreID", Value::U64(restore_id)),
            Field::new("cmdName", Value::Str(cmd_name.into())),
        ],
    );
    registry.start_heartbeat_manager(ctx, restore_id);
    Ok(())
}

/// Go `checkpointEnabledAndExists`: the table checkpoint manager, and
/// whether an earlier run left checkpoint metadata.
fn checkpoint_enabled_and_exists(
    ctx: &Context,
    g: &dyn Glue,
    cfg: &mut RestoreConfig,
    dom: &Arc<dyn Domain>,
) -> Result<bool> {
    if !cfg.use_checkpoint {
        return Ok(false);
    }
    if cfg.snapshot_checkpoint_meta_manager.is_none() {
        let m = new_snapshot_table_meta_manager(
            g,
            Arc::clone(dom),
            SNAPSHOT_RESTORE_CHECKPOINT_DATABASE_NAME,
            cfg.restore_id,
        )?;
        cfg.snapshot_checkpoint_meta_manager = Some(Arc::new(m));
    }
    cfg.snapshot_checkpoint_meta_manager
        .as_ref()
        .expect("set above")
        .exists_checkpoint_metadata(ctx)
}

/// Go `checkPreallocIDReusable`: the IDs the checkpoint reserved, when its
/// configuration hash is this one's.
fn check_prealloc_id_reusable(
    ctx: &Context,
    cfg: &RestoreConfig,
    hash: &[u8],
    checkpoint_exist: bool,
) -> Result<Option<CheckpointPreallocIDs>> {
    if !checkpoint_exist {
        return Ok(None);
    }
    let meta = cfg
        .snapshot_checkpoint_meta_manager
        .as_ref()
        .expect("an existing checkpoint has its manager")
        .load_checkpoint_metadata(ctx)?;
    tidb_log::info(
        "checkpoint metadata exists",
        &[
            Field::new("hash", Value::Str(hex::encode(hash))),
            Field::new("checkpoint hash", Value::Str(hex::encode(&meta.hash))),
        ],
    );
    if meta.hash != hash {
        return Err(errors::annotatef(
            &errors::ERR_RESTORE_CHECKPOINT_MISMATCH,
            "checkpoint hash mismatch, please use the same setting as the previous restore",
        ));
    }
    Ok(meta.prealloc_ids)
}

/// Go `checkMandatoryClusterRequirements`: the named databases and tables
/// are in the backup, and a full restore goes to an empty cluster.
fn check_mandatory_cluster_requirements(
    client: &SnapClient,
    cfg: &RestoreConfig,
    checkpoint_enabled_and_exists: bool,
    cmd_name: &str,
) -> Result<()> {
    verify_db_and_table_in_backup(&client.get_databases(), cfg)?;
    if is_full_restore(cmd_name)
        && client
            .need_check_fresh_cluster(cfg.config.explicit_filter, checkpoint_enabled_and_exists)
    {
        client.ensure_no_user_tables()?;
    }
    Ok(())
}

/// Go `getMaxReplica`: PD's `max-replicas`.
fn get_max_replica(ctx: &Context, mgr: &Mgr) -> Result<u64> {
    let rt = Arc::clone(mgr.get_storage().tikv().runtime());
    let mut resp = HashMap::new();
    crate::utils::retry::with_retry(
        ctx,
        || {
            resp = rt
                .block_on(
                    mgr.pd_controller
                        .get_pd_http_client()
                        .get_replicate_config(),
                )
                .map_err(crate::pdclient::from_client_error)?;
            Ok(())
        },
        &mut crate::utils::backoff::new_aggressive_pd_backoff_strategy(),
    )?;
    let key = "max-replicas";
    let Some(val) = resp.get(key) else {
        return Err(errors::errorf(format!(
            "key {key} not found in response {resp:?}"
        )));
    };
    Ok(val.as_f64().unwrap_or(0.0) as u64)
}

/// Go `getStores`.
fn get_stores(ctx: &Context, mgr: &Mgr) -> Result<tikv_client::pd_http::StoresInfo> {
    let rt = Arc::clone(mgr.get_storage().tikv().runtime());
    let mut stores = None;
    crate::utils::retry::with_retry(
        ctx,
        || {
            stores = Some(
                rt.block_on(mgr.pd_controller.get_pd_http_client().get_stores())
                    .map_err(crate::pdclient::from_client_error)?,
            );
            Ok(())
        },
        &mut crate::utils::backoff::new_aggressive_pd_backoff_strategy(),
    )?;
    Ok(stores.unwrap_or_default())
}

/// Go `EstimateTikvUsage`: the archive's share per TiKV store.
#[must_use]
pub fn estimate_tikv_usage(archive_size: u64, replica_cnt: u64, store_cnt: u64) -> u64 {
    if store_cnt == 0 {
        return 0;
    }
    let replica_cnt = replica_cnt.min(store_cnt);
    tidb_log::info(
        "estimate tikv usage",
        &[
            Field::new("total size", Value::U64(archive_size)),
            Field::new("replicaCnt", Value::U64(replica_cnt)),
            Field::new("store count", Value::U64(store_cnt)),
        ],
    );
    archive_size * replica_cnt / store_cnt
}

/// Go `EstimateTiflashUsage`: the TiFlash replicas' share per store.
#[must_use]
pub fn estimate_tiflash_usage(tables: &[metautil::Table], store_cnt: u64) -> u64 {
    if store_cnt == 0 {
        return 0;
    }
    let mut total = 0;
    for t in tables {
        let Some(replica) = &t.info().tiflash_replica else {
            continue;
        };
        let count = replica.read().count;
        if count == 0 {
            continue;
        }
        total += metautil::archive_table_size(t) * count;
    }
    tidb_log::info(
        "estimate tiflash usage",
        &[
            Field::new("total size", Value::U64(total)),
            Field::new("store count", Value::U64(store_cnt)),
        ],
    );
    total / store_cnt
}

/// Go `CheckStoreSpace`.
///
/// # Errors
/// [`errors::ERR_KV_DISK_FULL`], or an unreadable available size.
pub fn check_store_space(necessary: u64, store: &tikv_client::pd_http::StoreInfo) -> Result<()> {
    let invalid = || {
        errors::annotatef(
            &errors::ERR_PD_INVALID_RESPONSE,
            format!(
                "store {} has invalid available space {}",
                store.store.id, store.status.available
            ),
        )
    };
    let available =
        tidb_config_configtypes::ram_in_bytes(&store.status.available).map_err(|_| invalid())?;
    if available <= 0 {
        return Err(invalid());
    }
    if (available as u64) < necessary {
        return Err(errors::annotatef(
            &errors::ERR_KV_DISK_FULL,
            format!(
                "store {} has no space left on device, available {}, necessary {}",
                store.store.id,
                tidb_config_configtypes::bytes_size(available as f64),
                tidb_config_configtypes::bytes_size(necessary as f64)
            ),
        ));
    }
    Ok(())
}

/// Go `engine.IsTiFlashHTTPResp`.
fn is_tiflash_http_resp(store: &tikv_client::pd_http::MetaStore) -> bool {
    store
        .labels
        .iter()
        .any(|l| l.key == "engine" && (l.value == "tiflash" || l.value == "tiflash_compute"))
}

/// Go `checkDiskSpace`: every store has room for its share, with 10%
/// (TiKV) or 40% (TiFlash) to spare.
fn check_disk_space(
    ctx: &Context,
    mgr: &Mgr,
    tables: &[metautil::Table],
    archive_size: u64,
) -> Result<()> {
    const PB: u64 = 1 << 50;
    let max_replica = get_max_replica(ctx, mgr)?;
    let stores = get_stores(ctx, mgr)?;
    let tiflash_cnt = stores
        .stores
        .iter()
        .filter(|s| is_tiflash_http_resp(&s.store))
        .count() as u64;
    let tikv_cnt = stores.stores.len() as u64 - tiflash_cnt;
    // Go's `base * uint64(ratio*10) / 10`, below 1000 PB.
    let preserve = |base: u64, ratio_tenths: u64| {
        if base > 1000 * PB {
            base
        } else {
            base * ratio_tenths / 10
        }
    };
    let tikv_usage = preserve(estimate_tikv_usage(archive_size, max_replica, tikv_cnt), 11);
    let tiflash_usage = preserve(estimate_tiflash_usage(tables, tiflash_cnt), 14);
    tidb_log::info(
        "preserved disk space",
        &[
            Field::new("tikv", Value::U64(tikv_usage)),
            Field::new("tiflash", Value::U64(tiflash_usage)),
        ],
    );
    crate::utils::retry::with_retry(
        ctx,
        || {
            for store in get_stores(ctx, mgr)?.stores {
                let need = if is_tiflash_http_resp(&store.store) {
                    tiflash_usage
                } else {
                    tikv_usage
                };
                check_store_space(need, &store)?;
            }
            Ok(())
        },
        &mut crate::utils::backoff::new_disk_check_backoff_strategy(),
    )
}

/// Go `checkTableExistence`.
///
/// # Errors
/// [`errors::ERR_TABLES_ALREADY_EXISTED`] naming them.
pub fn check_table_existence(dom: &dyn Domain, tables: &[metautil::Table]) -> Result<()> {
    let is = dom.info_schema()?;
    let mut message = "table already exists: ".to_owned();
    let mut all_unique = true;
    for t in tables {
        if is
            .table_by_name(t.db.name.original(), t.info().name.original())
            .is_some()
        {
            message.push_str(&format!("{}.{} ", t.db.name, t.info().name));
            all_unique = false;
        }
    }
    if !all_unique {
        return Err(errors::annotate(
            errors::new(&errors::ERR_TABLES_ALREADY_EXISTED),
            message,
        ));
    }
    Ok(())
}

/// Go `checkOptionalClusterRequirements`: disk space and existing tables,
/// unless the requirement check is off or a checkpoint resumes.
fn check_optional_cluster_requirements(
    ctx: &Context,
    client: &SnapClient,
    cfg: &RestoreConfig,
    checkpoint_enabled_and_exists: bool,
    mgr: &Mgr,
    tables: &[metautil::Table],
    archive_size: u64,
) -> Result<()> {
    if cfg.config.check_requirements && !checkpoint_enabled_and_exists {
        check_disk_space(ctx, mgr, tables, archive_size)?;
    }
    // Go `shouldCheckRestoreTargetExistence` without name routing.
    if !checkpoint_enabled_and_exists && !client.is_incremental() && cfg.config.check_requirements {
        check_table_existence(client.dom()?.as_ref(), tables)?;
    }
    Ok(())
}

/// Go `getAnyFileKeyFromTables`.
fn get_any_file_key_from_tables(tables: &[metautil::Table]) -> Option<Vec<u8>> {
    tables
        .iter()
        .flat_map(|t| t.files_of_physicals.values().flatten())
        .find(|f| !f.start_key.is_empty())
        .map(|f| f.start_key.clone())
}

/// Go `filterRestoreFiles`: the databases and tables the filter keeps,
/// without the checkpoint databases.
///
/// # Errors
/// [`errors::ERR_RESTORE_INVALID_BACKUP`] for tables without databases.
pub fn filter_restore_files(
    client: &SnapClient,
    cfg: &RestoreConfig,
    load_stats_physical: bool,
) -> Result<(
    BTreeMap<i64, metautil::Table>,
    BTreeMap<i64, metautil::Database>,
)> {
    let mut table_map = BTreeMap::new();
    let mut db_map = BTreeMap::new();
    let f = cfg.config.table_filter.as_ref();
    for db in client.get_databases() {
        let db_name = db.info.name.original().to_owned();
        if is_checkpoint_db(&db_name) {
            continue;
        }
        if !load_stats_physical && !match_schema(f, &db_name, cfg.with_sys_table) {
            continue;
        }
        for table in &db.tables {
            let Some(info) = &table.info else { continue };
            let stats_temp =
                load_stats_physical && crate::restore::snap_client::systable_restore::get_db_name_if_stats_temporary_table(&db_name, info.name.original()).is_some();
            if !stats_temp && !match_table(f, &db_name, info.name.original(), cfg.with_sys_table) {
                continue;
            }
            // Go also skips materialized views and their logs and clears a
            // base table's view links; this port's table model has neither.
            table_map.insert(info.id, table.clone());
        }
        db_map.insert(db.info.id, db);
    }
    if db_map.is_empty() && !table_map.is_empty() {
        return Err(errors::annotate(
            errors::new(&errors::ERR_RESTORE_INVALID_BACKUP),
            "contains tables but no databases",
        ));
    }
    Ok((table_map, db_map))
}

/// Go `PreCheckTableTiFlashReplica`: a replica is never restored as
/// available; one the cluster's TiFlash stores cannot hold is dropped, and
/// a next-gen restore drops them all.
///
/// # Errors
/// The TiFlash stores cannot be listed.
pub fn pre_check_table_tiflash_replica(
    ctx: &Context,
    pd_client: &dyn crate::pdclient::PdClient,
    tables: &mut [metautil::Table],
    is_next_gen_restore: bool,
) -> Result<()> {
    if is_next_gen_restore {
        tidb_log::warn(
            "Restoring to NextGen TiFlash is experimental. TiFlash replicas are disabled; please reset them manually after restore.",
            &[],
        );
        for t in tables.iter_mut() {
            if let Some(info) = t.info.as_mut() {
                info.tiflash_replica = None;
            }
        }
        return Ok(());
    }
    let tiflash_count = crate::conn::get_all_tikv_stores_with_retry(
        ctx,
        pd_client,
        crate::conn::StoreBehavior::TiFlashOnly,
    )?
    .len() as u64;
    for t in tables.iter_mut() {
        let (db, name) = (t.db.name.to_string(), t.info().name.to_string());
        let Some(info) = t.info.as_mut() else {
            continue;
        };
        let Some(replica) = &info.tiflash_replica else {
            continue;
        };
        // A restored replica becomes available once TiFlash catches up.
        let mut r = replica.read().clone();
        r.available = false;
        r.available_partition_ids = Vec::new().into();
        let count = r.count;
        info.tiflash_replica = Some(tidb_model::GoShared::new(r));
        if count > tiflash_count {
            tidb_log::warn(
                "table does not satisfy tiflash replica requirements, set tiflash replcia to unavailable",
                &[
                    Field::new("db", Value::Str(db)),
                    Field::new("table", Value::Str(name)),
                    Field::new("expect tiflash replica", Value::U64(count)),
                    Field::new("actual tiflash store", Value::U64(tiflash_count)),
                ],
            );
            info.tiflash_replica = None;
        }
    }
    Ok(())
}

fn cluster_index_mismatch(
    schema: &str,
    table: &str,
    backup: bool,
    existing: bool,
) -> errors::Error {
    tidb_log::error(
        "Clustered index option mismatch",
        &[
            Field::new("schemaName", Value::Str(schema.into())),
            Field::new("tableName", Value::Str(table.into())),
        ],
    );
    errors::annotatef(
        &errors::ERR_RESTORE_MODE_MISMATCH,
        format!(
            "Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be {} (backup table = {backup}, created table = {existing}).",
            transfer_bool_to_value(backup)
        ),
    )
}

/// Go `PreCheckTableClusterIndex`: an existing table (or one a DDL job
/// creates) keeps the backup's clustered-index choice.
///
/// # Errors
/// [`errors::ERR_RESTORE_MODE_MISMATCH`].
pub fn pre_check_table_cluster_index(
    tables: &[metautil::Table],
    ddl_jobs: &[Job],
    dom: &dyn Domain,
) -> Result<()> {
    for t in tables {
        if let Ok(old) = get_table_schema(dom, t.db.name.original(), t.info().name.original()) {
            if t.info().is_common_handle != old.is_common_handle {
                return Err(cluster_index_mismatch(
                    t.db.name.original(),
                    t.info().name.original(),
                    t.info().is_common_handle,
                    old.is_common_handle,
                ));
            }
        }
    }
    for job in ddl_jobs {
        if job.type_ != ActionType::ACTION_CREATE_TABLE {
            continue;
        }
        let Some(info) = job
            .binlog_info
            .as_ref()
            .and_then(|b| b.read().table_info.as_ref().map(|t| t.read().clone()))
        else {
            continue;
        };
        let schema = job.schema_name.to_string();
        if let Ok(old) = get_table_schema(dom, &schema, info.name.original()) {
            if info.is_common_handle != old.is_common_handle {
                return Err(cluster_index_mismatch(
                    &schema,
                    info.name.original(),
                    info.is_common_handle,
                    old.is_common_handle,
                ));
            }
        }
    }
    Ok(())
}

fn job_version(j: &Job) -> i64 {
    j.binlog_info
        .as_ref()
        .map_or(0, |b| b.read().schema_version)
}

/// Go `FilterDDLJobs`: the jobs touching the restored databases or tables
/// under any name or ID they had, newest schema version first.
#[must_use]
pub fn filter_ddl_jobs(all_ddl_jobs: &mut [Job], tables: &[metautil::Table]) -> Vec<Job> {
    all_ddl_jobs.sort_by_key(|j| std::cmp::Reverse(job_version(j)));
    let mut ddl_jobs = Vec::new();
    // Go `getDatabases`: each table's database, first seen first.
    let mut seen = HashSet::new();
    let dbs: Vec<&tidb_model::DBInfo> = tables
        .iter()
        .map(|t| &t.db)
        .filter(|db| seen.insert(db.id))
        .collect();
    for db in dbs {
        // Every name and ID the database had: a rename, drop and recreate
        // and rename back is found through them.
        let mut db_ids = HashSet::from([db.id]);
        let mut db_names = HashSet::from([db.name.to_string()]);
        for job in all_ddl_jobs.iter() {
            let Some(db_info) = job
                .binlog_info
                .as_ref()
                .and_then(|b| b.read().db_info.as_ref().map(|d| d.read().name.to_string()))
            else {
                continue;
            };
            if db_ids.contains(&job.schema_id) || db_names.contains(&db_info) {
                ddl_jobs.push(job.clone());
                db_ids.insert(job.schema_id);
                db_names.insert(db_info);
            }
        }
    }
    for t in tables {
        let mut table_ids = HashSet::from([t.info().id]);
        let mut table_names = HashSet::from([UniqueTableName {
            db: t.db.name.to_string(),
            table: t.info().name.to_string(),
        }]);
        for job in all_ddl_jobs.iter() {
            let Some(info) = job
                .binlog_info
                .as_ref()
                .and_then(|b| b.read().table_info.as_ref().map(|t| t.read().clone()))
            else {
                continue;
            };
            let name = UniqueTableName {
                db: job.schema_name.to_string(),
                table: info.name.to_string(),
            };
            if table_ids.contains(&job.table_id) || table_names.contains(&name) {
                ddl_jobs.push(job.clone());
                table_ids.insert(job.table_id);
                // A truncated table changes its ID.
                table_ids.insert(info.id);
                table_names.insert(name);
            }
        }
    }
    ddl_jobs
}

/// Go `DDLJobBlockListRule`: TiFlash replica and table lock jobs are not
/// replayed.
#[must_use]
pub fn ddl_job_block_list_rule(job: &Job) -> bool {
    [
        ActionType::ACTION_SET_TI_FLASH_REPLICA,
        ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS,
        ActionType::ACTION_LOCK_TABLE,
        ActionType::ACTION_UNLOCK_TABLE,
    ]
    .contains(&job.type_)
}

/// Go `DDLJobLogIncrementalCompactBlockListRule`: backfilling jobs.
#[must_use]
pub fn ddl_job_log_incremental_compact_block_list_rule(job: &Job) -> bool {
    [
        ActionType::ACTION_ADD_INDEX,
        ActionType::ACTION_MODIFY_COLUMN,
        ActionType::ACTION_REORGANIZE_PARTITION,
    ]
    .contains(&job.type_)
}

/// Go `FilterDDLJobByRules`: the jobs no rule matches.
#[must_use]
pub fn filter_ddl_job_by_rules(jobs: Vec<Job>, rules: &[fn(&Job) -> bool]) -> Vec<Job> {
    jobs.into_iter()
        .filter(|j| !rules.iter().any(|r| r(j)))
        .collect()
}

/// Go `CheckDDLJobByRules`.
///
/// # Errors
/// [`errors::ERR_RESTORE_MODE_MISMATCH`] for a job a rule matches.
pub fn check_ddl_job_by_rules(jobs: &[Job], rules: &[fn(&Job) -> bool]) -> Result<()> {
    for job in jobs {
        if rules.iter().any(|r| r(job)) {
            return Err(errors::annotatef(
                &errors::ERR_RESTORE_MODE_MISMATCH,
                format!("DDL job {job} is not allowed in incremental restore when --allow-pitr-from-incremental enabled"),
            ));
        }
    }
    Ok(())
}

/// Go `encodeCompactAndCheckKey`: the table prefixes of the preallocated
/// range in the store's key space.
fn encode_compact_and_check_key(
    storage: &dyn crate::glue::Storage,
    pre_alloced: [i64; 2],
) -> (Vec<u8>, Vec<u8>) {
    let start = tidb_tablecodec::table_key::encode_table_prefix(pre_alloced[0]);
    let end = tidb_tablecodec::table_key::encode_table_prefix(pre_alloced[1]);
    storage.encode_range(&start, &end)
}

/// Go `rewriteKeyRanges`: the region range of the preallocated IDs, for
/// pausing the schedulers there.
fn rewrite_key_ranges(
    storage: &dyn crate::glue::Storage,
    pre_alloced: [i64; 2],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    if pre_alloced == [0, 0] {
        return Vec::new();
    }
    let (start, end) = encode_compact_and_check_key(storage, pre_alloced);
    // Go `EncodeRegionRange`: the memcomparable encoding regions use.
    let encode = |k: &[u8]| {
        let mut out = Vec::new();
        tidb_codec::encode_bytes(&mut out, k);
        out
    };
    vec![(encode(&start), encode(&end))]
}

/// Go `tweakLocalConfForRestore`: the DDL index and column limits at their
/// maximums while tables are created; the returned function restores them.
fn tweak_local_conf_for_restore() -> impl Fn() {
    let restore = tidb_config::config_tree::config::restore_func();
    tidb_config::config_tree::config::update_global(|conf| {
        // Go `DefMaxOfMaxIndexLength`, `DefMaxOfIndexLimit`,
        // `DefMaxOfTableColumnCountLimit`.
        conf.max_index_length = 3072 * 4;
        tidb_log::warn(
            "set max-index-length to max(3072*4) to skip check index length in DDL",
            &[],
        );
        conf.index_limit = 64 * 8;
        tidb_log::warn(
            "set index-limit to max(64*8) to skip check index count in DDL",
            &[],
        );
        conf.table_column_count_limit = 4096;
        tidb_log::warn(
            "set table-column-count to max(4096) to skip check column count in DDL",
            &[],
        );
    });
    restore
}

/// Go `cleanUpCheckpoints`.
fn clean_up_checkpoints(ctx: &Context, cfg: &RestoreConfig) {
    if !cfg.use_checkpoint {
        tidb_log::info(
            "checkpoint not enabled, skip to remove checkpoint data",
            &[],
        );
        return;
    }
    tidb_log::info("start to remove checkpoint data restore", &[]);
    if let Some(m) = &cfg.snapshot_checkpoint_meta_manager {
        if let Err(err) = m.remove_checkpoint_data(ctx) {
            log_err(
                "failed to remove checkpoint data for snapshot restore",
                &err,
            );
        }
    }
    tidb_log::info("all checkpoint data removed.", &[]);
}

/// Go `hasCheckpointPersisted`.
fn has_checkpoint_persisted(ctx: &Context, cfg: &RestoreConfig) -> Result<bool> {
    match &cfg.snapshot_checkpoint_meta_manager {
        Some(m) => m.exists_checkpoint_metadata(ctx),
        None => Ok(false),
    }
}

/// Go `vardef.PDEnableFollowerHandleRegion`.
const PD_ENABLE_FOLLOWER_HANDLE_REGION: &str = "pd_enable_follower_handle_region";

/// Go `RunRestore` for a snapshot restore: the log backup check, the PD
/// task registration, the restore registry, then [`run_snapshot_restore`];
/// a failed restore pauses its registration when a checkpoint was saved and
/// unregisters it otherwise.
///
/// # Errors
/// Any step's error.
pub fn run_restore(
    c: &Context,
    g: &dyn Glue,
    cmd_name: &str,
    cfg: &mut RestoreConfig,
) -> Result<()> {
    cfg.config.ensure_operation_context(restore_operation_command_name(cmd_name))?;
    if is_stream_restore(cmd_name) {
        return Err(errors::errorf("log restore (PiTR) is not supported"));
    }
    // Go dials PD's etcd from the task's PD addresses; BRIE's are this
    // node's own, so its etcd client is used.
    let dom = g.get_domain()?;
    let etcd = dom
        .etcd_client()
        .ok_or_else(|| errors::errorf("the restore needs PD's etcd, and this node has none"))?;
    check_conflicting_log_backup(cfg, etcd.as_ref())
        .map_err(|e| errors::annotate(e, "failed to check task exists"))?;
    let mut register = register_task_to_pd(Arc::clone(&etcd))
        .map_err(|e| errors::annotate(e, "failed to register task to pd"))?;
    let res = run_restore_registered(c, g, cmd_name, cfg, &dom);
    let _ = register.close();
    res
}

fn run_restore_registered(
    c: &Context,
    g: &dyn Glue,
    cmd_name: &str,
    cfg: &mut RestoreConfig,
    dom: &Arc<dyn Domain>,
) -> Result<()> {
    let keyspace = cfg.config.keyspace_name.clone();
    tidb_config::config_tree::config::update_global(|conf| conf.keyspace_name = keyspace);
    // Checked early, before the heavier connection setup.
    check_snapshot_restore_mode(c, cfg)?;
    let mgr = super::common::new_mgr(c, g, &cfg.config, true, VersionCheckerType::Normal)?;
    let res = run_restore_with_mgr(c, g, cmd_name, cfg, &mgr, dom);
    cfg.close_checkpoint_meta_manager();
    mgr.close();
    res
}

fn run_restore_with_mgr(
    c: &Context,
    g: &dyn Glue,
    cmd_name: &str,
    cfg: &mut RestoreConfig,
    mgr: &Mgr,
    dom: &Arc<dyn Domain>,
) -> Result<()> {
    // Go hands `pd_enable_follower_handle_region` to the PD client; this
    // node's PD client is shared, so the variable is only read.
    g.use_one_shot_session(false, &mut |se| {
        se.get_global_sys_var(PD_ENABLE_FOLLOWER_HANDLE_REGION)
            .map(|_| ())
    })?;
    let registry = Arc::new(new_restore_registry(g, dom.as_ref())?);
    cfg.restore_registry = Some(Arc::clone(&registry));
    let res = (|| {
        if cfg.config.check_requirements {
            crate::version::check_cluster_version(
                c,
                mgr.get_pd_client().as_ref(),
                &crate::version::check_version_for_br,
            )?;
        }
        run_snapshot_restore(c, mgr, g, cmd_name, cfg)
    })();
    let mut res = res;
    if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore").is_some()
    {
        tidb_log::info(
            "failpoint fail-at-end-of-restore injected, failing at the end of restore task",
            &[],
        );
        res = Err(errors::errorf("failpoint: fail-at-end-of-restore"));
    }
    if let Err(err) = res {
        // Without a registration (an old cluster, or a failure before it)
        // there is nothing to clean up.
        if cfg.restore_id != 0 {
            let persisted = has_checkpoint_persisted(c, cfg).unwrap_or_else(|e| {
                tidb_log::error(
                    "failed to check if checkpoint is persisted",
                    &[Field::new("error", Value::Str(e.to_string()))],
                );
                false
            });
            let id = Field::new("restoreId", Value::U64(cfg.restore_id));
            if persisted {
                tidb_log::info(
                    "pausing restore task from registry",
                    &[id.clone(), Field::new("error", Value::Str(err.to_string()))],
                );
                if let Err(e) = registry.pause_task(c, cfg.restore_id) {
                    tidb_log::error(
                        "failed to pause restore task from registry",
                        &[id, Field::new("error", Value::Str(e.to_string()))],
                    );
                }
            } else {
                tidb_log::info(
                    "unregistering restore task from registry",
                    &[id.clone(), Field::new("error", Value::Str(err.to_string()))],
                );
                if let Err(e) = registry.unregister(c, cfg.restore_id) {
                    tidb_log::error(
                        "failed to unregister restore task from registry",
                        &[id, Field::new("error", Value::Str(e.to_string()))],
                    );
                }
            }
        }
        registry.close();
        return Err(err);
    }
    if cfg.restore_id != 0 {
        if let Err(e) = registry.unregister(c, cfg.restore_id) {
            tidb_log::warn(
                "failed to unregister restore task from registry",
                &[
                    Field::new("restoreId", Value::U64(cfg.restore_id)),
                    Field::new("error", Value::Str(e.to_string())),
                ],
            );
        }
    }
    clean_up_checkpoints(c, cfg);
    registry.close();
    Ok(())
}

/// Go `isRestoreSysTablesPhysically`: `(sys tables, stats)` by renaming,
/// never on a next-gen kernel.
fn is_restore_sys_tables_physically(cfg: &RestoreConfig) -> (bool, bool) {
    if tidb_config_kerneltype::is_next_gen() {
        return (false, false);
    }
    (
        cfg.fast_load_sys_tables && cfg.with_sys_table,
        cfg.fast_load_sys_tables && cfg.load_stats,
    )
}

/// Go `utils.CheckNextGenCompatibility`: whether this is a next-gen
/// restore. Go exits the process for a bad combination; it is an error here.
fn check_next_gen_compatibility(keyspace_name: &str, check_requirements: bool) -> Result<bool> {
    if !tidb_config_kerneltype::is_next_gen() {
        if !keyspace_name.is_empty() {
            let msg = "classic kernel does not support keyspace restore; it may cause high disk usage. If you are certain this can be ignored, set --check-requirements=false or better use the next-gen build instead.";
            if check_requirements {
                return Err(errors::errorf(msg));
            }
            tidb_log::warn(
                &format!("{msg} Skipping check due to --check-requirements=false."),
                &[],
            );
        }
        return Ok(false);
    }
    if keyspace_name.is_empty() {
        return Err(errors::errorf(
            "next-gen restore requires keyspaceName; missing value may cause SST ingest panic.",
        ));
    }
    Ok(true)
}

/// Go `tikv.DecodeKey(key, version)`: the keyspace prefix of an API V2 key.
fn decode_keyspace(key: &[u8], api_version: i32) -> Vec<u8> {
    if api_version == tidb_proto::kvrpcpb::ApiVersion::V2 as i32 && key.len() >= 4 {
        key[..4].to_vec()
    } else {
        Vec::new()
    }
}

/// Go `fallbackStatsTables`.
fn fallback_stats_tables(tables: Vec<metautil::Table>) -> Vec<metautil::Table> {
    tables
        .into_iter()
        .filter(|t| {
            crate::restore::snap_client::systable_restore::get_db_name_if_stats_temporary_table(
                t.db.name.original(),
                t.info().name.original(),
            )
            .is_none()
        })
        .collect()
}

/// What `runSnapshotRestore` defers, run in Go's reverse order once the
/// restore ends.
struct SnapshotCleanup<'a> {
    ctx: Context,
    client: Option<SnapClient>,
    post_work: Option<(
        Arc<crate::restore::import_mode_switcher::ImportModeSwitcher>,
        crate::pdutil::UndoFunc,
        bool,
    )>,
    can_restore_schedulers: bool,
    use_checkpoint: bool,
    wait_checkpoint: bool,
    gc: Option<(
        tidb_gocontext::CancelFunc,
        crate::gc::BrServiceSafePoint,
        &'a Mgr,
    )>,
    restore_db_config: Option<Box<dyn Fn()>>,
}

impl Drop for SnapshotCleanup<'_> {
    fn drop(&mut self) {
        if let Some(restore) = self.restore_db_config.take() {
            restore();
        }
        if let Some((cancel, sp, mgr)) = self.gc.take() {
            tidb_log::info("start to remove gc-safepoint keeper", &[]);
            cancel.cancel();
            if let Err(err) = mgr
                .get_gc_manager()
                .delete_service_safe_point(&self.ctx, &sp)
            {
                log_err(
                    "failed to remove service safe point, backup may fail if gc triggered",
                    &err,
                );
            }
            tidb_log::info("finish removing gc-safepoint keeper", &[]);
        }
        if self.wait_checkpoint {
            tidb_log::info("wait for flush checkpoint...", &[]);
            if let Some(client) = &self.client {
                // A full flush lets the next retry jump past the ranges done.
                client.wait_for_finish_checkpoint(&self.ctx, !self.can_restore_schedulers);
            }
        }
        if let Some((switcher, undo, online)) = self.post_work.take() {
            if self.use_checkpoint && !self.can_restore_schedulers {
                tidb_log::info("skip removing pd scheduler for next retry", &[]);
            } else {
                tidb_log::info("start to restore pd scheduler", &[]);
                crate::restore::import_mode_switcher::restore_post_work(
                    &self.ctx, &switcher, undo, online,
                );
                tidb_log::info("finish restoring pd scheduler", &[]);
            }
        }
        if let Some(client) = self.client.take() {
            client.close();
        }
    }
}

/// Go `runSnapshotRestore`: the backed-up schemas recreated and their data
/// imported, checksummed and analyzed.
///
/// # Errors
/// Any step's error.
pub fn run_snapshot_restore(
    c: &Context,
    mgr: &Mgr,
    g: &dyn Glue,
    cmd_name: &str,
    cfg: &mut RestoreConfig,
) -> Result<()> {
    cfg.adjust();
    let (ctx, cancel) = Context::with_cancel(c);
    let res = run_snapshot_restore_inner(&ctx, mgr, g, cmd_name, cfg);
    cancel.cancel();
    crate::summary::summary(cmd_name);
    res
}

#[allow(clippy::too_many_lines)]
fn run_snapshot_restore_inner(
    ctx: &Context,
    mgr: &Mgr,
    g: &dyn Glue,
    cmd_name: &str,
    cfg: &mut RestoreConfig,
) -> Result<()> {
    tidb_log::info("starting snapshot restore", &[]);
    let (mut load_sys_table_physical, mut load_stats_physical) =
        is_restore_sys_tables_physically(cfg);
    let (u, s, backup_meta) = read_backup_meta(ctx, &cfg.config)?;
    if backup_meta.is_raw_kv || backup_meta.is_txn_kv {
        return Err(errors::annotate(
            errors::new(&errors::ERR_RESTORE_MODE_MISMATCH),
            "cannot do transactional restore from raw/txn kv data",
        ));
    }
    if cfg.upstream_cluster_id == 0 {
        cfg.upstream_cluster_id = backup_meta.cluster_id;
    }
    if load_stats_physical || load_sys_table_physical {
        let fallback = |why: &str, fields: &[Field]| {
            tidb_log::warn(
                &format!("{why}. Fallback to logically load system tables."),
                fields,
            )
        };
        let upstream = crate::version::normalize_backup_version(&backup_meta.cluster_version);
        let downstream = mgr.pd_controller.get_cluster_version(ctx);
        match (&upstream, &downstream) {
            (None, _) => {
                fallback(
                    "The cluster version from backupmeta is invalid",
                    &[Field::new(
                        "backupmeta cluster version",
                        Value::Str(backup_meta.cluster_version.clone()),
                    )],
                );
                (load_stats_physical, load_sys_table_physical) = (false, false);
            }
            (_, Err(err)) => {
                fallback(
                    "Failed to get the downstream cluster version",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                (load_stats_physical, load_sys_table_physical) = (false, false);
            }
            (Some(up), Ok(down)) => match crate::version::normalize_backup_version(down) {
                None => {
                    fallback(
                        "The downstream cluster version is invalid",
                        &[Field::new(
                            "downstream cluster version",
                            Value::Str(down.clone()),
                        )],
                    );
                    (load_stats_physical, load_sys_table_physical) = (false, false);
                }
                Some(down)
                    if load_sys_table_physical
                        && (up.major != down.major || up.minor != down.minor) =>
                {
                    tidb_log::warn(
                        "Cannot set --fast-load-sys-tables when the cluster version of snapshot backup is different from that of the downstream cluster. Fallback to logically load system tables.",
                        &[
                            Field::new("upstream version", Value::Str(format!("{}.{}", up.major, up.minor))),
                            Field::new("downstream version", Value::Str(format!("{}.{}", down.major, down.minor))),
                        ],
                    );
                    load_sys_table_physical = false;
                }
                Some(_) => {}
            },
        }
    }
    let dom = mgr
        .get_domain()
        .cloned()
        .ok_or_else(|| errors::errorf("the restore needs the domain"))?;
    if cfg.config.check_requirements {
        tidb_log::info(
            "Checking incompatible TiCDC changefeeds before restoring.",
            &[Field::new(
                "restore-ts",
                Value::U64(backup_meta.end_version),
            )],
        );
        if let Some(etcd) = dom.etcd_client() {
            check_incompatible_changefeed(backup_meta.end_version, etcd.as_ref())?;
        }
        if let Some(backup_version) =
            crate::version::normalize_backup_version(&backup_meta.cluster_version)
        {
            crate::version::check_cluster_version(
                ctx,
                mgr.get_pd_client().as_ref(),
                &crate::version::check_version_for_backup(backup_version),
            )?;
        }
    }
    check_new_collation_enable(
        &backup_meta.new_collations_enabled,
        g,
        cfg.config.check_requirements,
    )?;

    // TiKV's own settings, for regions that split dynamically
    // (https://github.com/pingcap/tidb/issues/34167).
    let mut kv_configs = crate::config::KvConfig {
        import_goroutines: cfg.concurrency_per_store,
        merge_region_size: cfg.merge_small_region_size_bytes,
        merge_region_key_count: cfg.merge_small_region_key_count,
    };
    let security = super::common::cluster_security(&cfg.config);
    let http_client = crate::conn::new_http_client(&security)?;
    mgr.process_tikv_configs(ctx, &mut kv_configs, &http_client);

    let mut cleanup = SnapshotCleanup {
        ctx: ctx.clone(),
        client: None,
        post_work: None,
        can_restore_schedulers: false,
        use_checkpoint: false,
        wait_checkpoint: false,
        gc: None,
        restore_db_config: None,
    };
    let runtime = Arc::clone(mgr.get_storage().tikv().runtime());
    cleanup.client = Some(SnapClient::new(
        Arc::clone(mgr.get_pd_client()),
        Some(mgr.pd_controller.get_pd_http_client().clone()),
        security.clone(),
        super::common::get_keepalive(&cfg.config),
        Arc::clone(&runtime),
    ));
    let client = cleanup.client.as_mut().expect("set above");
    cfg.concurrency_per_store = kv_configs.import_goroutines;
    client.set_concurrency_per_store(cfg.concurrency_per_store.value as usize);
    configure_restore_client(ctx, client, cfg);
    client.init_connections(g)?;
    register_restore_if_needed(ctx, cfg, cmd_name, &dom)?;
    let is_next_gen_restore =
        check_next_gen_compatibility(&cfg.config.keyspace_name, cfg.config.check_requirements)?;
    if is_next_gen_restore {
        tidb_log::info("start restore to next-gen cluster", &[]);
    }

    let meta_reader = metautil::MetaReader::new(
        backup_meta.clone(),
        s.clone(),
        cfg.config.cipher_info.clone(),
    );
    let api_version = tidb_proto::kvrpcpb::ApiVersion::try_from(mgr.get_storage().api_version())
        .unwrap_or_default();
    client.load_schema_if_needed_and_init_client(
        ctx,
        backup_meta.clone(),
        Some(u.clone()),
        &meta_reader,
        cfg.load_stats,
        api_version,
        cfg.config.explicit_filter,
        is_full_restore(cmd_name),
        cfg.with_sys_table,
    )?;
    if client.is_incremental() || cfg.config.explicit_filter || !is_full_restore(cmd_name) {
        if load_stats_physical {
            tidb_log::warn("Cannot set --fast-load-sys-tables when it is not full restore. Fallback to logically load stats.", &[]);
            load_stats_physical = false;
        }
        if load_sys_table_physical {
            tidb_log::warn("Cannot set --fast-load-sys-tables when it is not full restore. Fallback to logically load system tables.", &[]);
            load_sys_table_physical = false;
        }
    }
    if client.is_incremental() {
        tidb_log::info(
            "the incremental snapshot restore doesn't support checkpoint mode, disable checkpoint.",
            &[],
        );
        cfg.use_checkpoint = false;
    }

    let hash = cfg.hash(cmd_name);
    let cp_enabled_and_exists = checkpoint_enabled_and_exists(ctx, g, cfg, &dom)?;
    let reuse_prealloc_ids = check_prealloc_id_reusable(ctx, cfg, &hash, cp_enabled_and_exists)?;
    tidb_log::info(
        "checkpoint status in restore",
        &[
            Field::new("enabled", Value::Bool(cfg.use_checkpoint)),
            Field::new("exists", Value::Bool(cp_enabled_and_exists)),
        ],
    );
    let client = cleanup.client.as_mut().expect("set above");
    check_mandatory_cluster_requirements(client, cfg, cp_enabled_and_exists, cmd_name)?;

    let (table_map, db_map) = filter_restore_files(client, cfg, load_stats_physical)?;
    tidb_log::info(
        "found items to restore after filtering",
        &[
            Field::new("tables", Value::U64(table_map.len() as u64)),
            Field::new("db", Value::U64(db_map.len() as u64)),
        ],
    );
    let mut tables: Vec<metautil::Table> = table_map.into_values().collect();
    let dbs: Vec<metautil::Database> = db_map.into_values().collect();

    if let Some(registry) = cfg
        .restore_registry
        .as_ref()
        .filter(|_| cfg.restore_id != 0)
    {
        tidb_log::info(
            "checking ongoing conflicting restore task using restore registry",
            &[
                Field::new("tables_count", Value::U64(tables.len() as u64)),
                Field::new("current_restore_id", Value::U64(cfg.restore_id)),
            ],
        );
        registry.check_tables_with_registered_tasks(ctx, cfg.restore_id, &dbs, &tables)?;
    }

    let archive_size = metautil::archive_tables_size(&tables);
    check_optional_cluster_requirements(
        ctx,
        client,
        cfg,
        cp_enabled_and_exists,
        mgr,
        &tables,
        archive_size,
    )?;
    g.record(crate::summary::RESTORE_DATA_SIZE, archive_size);
    // https://github.com/pingcap/tidb/issues/27247: BRIE reads "Size".
    g.record("Size", archive_size);
    let restore_ts = get_ts_with_retry(ctx, mgr.get_pd_client().as_ref())?;

    if client.is_full_cluster_restore() && client.has_backed_up_sys_db() {
        let can_load_physical =
            crate::restore::snap_client::systable_restore::check_sys_table_compatibility(
                dom.as_ref(),
                &tables,
                client.get_check_privilege_table_rows_collate_compatibility(),
            )?;
        if load_sys_table_physical && !can_load_physical {
            tidb_log::info("The system tables schema is not compatible. Fallback to logically load system tables.", &[]);
            load_sys_table_physical = false;
        }
        if client.get_check_privilege_table_rows_collate_compatibility() && can_load_physical {
            tidb_log::info(
                "The system tables schema match so no need to set sys check collation",
                &[],
            );
            client.set_check_privilege_table_rows_collate_compatibility(false);
        }
    }

    // Before any DDL or database creation (the checkpoint's too) takes a
    // global ID.
    if client.alloc_table_ids(
        ctx,
        &tables,
        load_stats_physical,
        load_sys_table_physical,
        reuse_prealloc_ids.as_ref(),
    )? {
        tidb_log::warn("Cannot load stats physically because not all table ids are reused. Fallback to logically load stats.", &[]);
        tables = fallback_stats_tables(tables);
        load_stats_physical = false;
    }
    let mut tables = client.clean_tables_if_temporary_system_tables_renamed(
        load_stats_physical,
        load_sys_table_physical,
        tables,
    );
    pre_check_table_tiflash_replica(
        ctx,
        mgr.get_pd_client().as_ref(),
        &mut tables,
        is_next_gen_restore,
    )?;
    // Go `buildRestoreNamePlan` without routes: each target is its source.
    let mut db_plans: Vec<crate::restore_utils::common::DatabaseRestorePlan> = dbs
        .iter()
        .map(|db| crate::restore_utils::common::DatabaseRestorePlan {
            source: db.clone(),
            target: db.info.clone_like_go(),
            reused: false,
        })
        .collect();
    let table_plans: Vec<crate::restore_utils::common::TableRestorePlan> = tables
        .iter()
        .map(|t| crate::restore_utils::common::TableRestorePlan {
            source: t.clone(),
            target_db: t.db.clone_like_go(),
            target_info: t.info().clone_like_go(),
        })
        .collect();
    let pre_alloc_range = client.get_pre_alloced_table_id_range()?;

    let switcher = crate::restore::import_mode_switcher::ImportModeSwitcher::new(
        Arc::clone(mgr.get_pd_client()),
        cfg.config.switch_mode_interval,
        security,
        Arc::clone(&runtime),
    );
    let (mut restore_schedulers, schedulers_config, res) =
        if (is_full_restore(cmd_name) && !cfg.config.explicit_filter) || client.is_incremental() {
            crate::restore::import_mode_switcher::restore_pre_work(
                ctx, mgr, &switcher, cfg.online, true,
            )
        } else {
            let key_range = rewrite_key_ranges(mgr.get_storage().as_ref(), pre_alloc_range);
            crate::restore::import_mode_switcher::fine_grained_restore_pre_work(
                ctx,
                mgr,
                &switcher,
                &key_range,
                !cfg.online,
            )
        };
    cleanup.use_checkpoint = cfg.use_checkpoint;
    res?;

    let mut checkpoint_set_with_table_id = HashMap::new();
    if cfg.use_checkpoint {
        let manager = cfg
            .snapshot_checkpoint_meta_manager
            .clone()
            .expect("checkpoint mode has its manager");
        let client = cleanup.client.as_mut().expect("set above");
        let init = client.init_checkpoint(
            ctx,
            manager.as_ref(),
            schedulers_config.as_ref(),
            cfg.restore_start_ts,
            0,
            &hash,
            cp_enabled_and_exists,
        );
        let init = match init {
            Ok(i) => i,
            Err(err) => {
                cleanup.post_work = Some((switcher, restore_schedulers, cfg.online));
                return Err(err);
            }
        };
        tidb_log::info(
            "try to reuse restore start timestamp for blocklist",
            &[
                Field::new(
                    "new restore start ts",
                    Value::U64(init.new_restore_start_ts),
                ),
                Field::new("old restore start ts", Value::U64(cfg.restore_start_ts)),
            ],
        );
        cfg.restore_start_ts = init.new_restore_start_ts;
        if let Some(mut from_checkpoint) = init.checkpoint_cluster_config {
            // The last range rule is dropped when the last restore quits.
            from_checkpoint.rule_id = schedulers_config
                .as_ref()
                .map(|c| c.rule_id.clone())
                .unwrap_or_default();
            restore_schedulers = mgr
                .pd_controller
                .make_undo_function_by_config(from_checkpoint);
        }
        checkpoint_set_with_table_id = init.checkpoint_set_with_table_id;
        cleanup.wait_checkpoint = true;
    }
    cleanup.post_work = Some((switcher, restore_schedulers, cfg.online));

    let etcd = dom
        .etcd_client()
        .ok_or_else(|| errors::errorf("the restore needs PD's etcd, and this node has none"))?;
    let client = cleanup.client.as_mut().expect("set above");
    client.install_pitr_support(
        ctx,
        crate::restore::snap_client::pitr_collector::PitrCollDep::new(
            Arc::clone(mgr.get_pd_client()),
            crate::streamhelper::MetaDataClient::new(Arc::new(EtcdOpsMetaStore(etcd))),
            u.clone(),
            cfg.config.operation_context.clone(),
        ),
    )?;

    let sp = crate::gc::BrServiceSafePoint {
        backup_ts: restore_ts,
        ttl: crate::gc::DEFAULT_BR_GC_SAFE_POINT_TTL,
        id: crate::gc::make_safe_point_id(),
    };
    g.record("BackupTS", backup_meta.end_version);
    g.record("RestoreTS", restore_ts);
    // The checksum reads at its start TS, so the safe point stays put
    // (GC must not pass the transaction).
    let (cctx, keeper_cancel) = Context::with_cancel(ctx);
    cleanup.gc = Some((keeper_cancel, sp.clone(), mgr));
    let _keeper =
        crate::gc::start_service_safe_point_keeper(&cctx, sp, Arc::clone(mgr.get_gc_manager()))?;

    let client = cleanup.client.as_mut().expect("set above");
    let mut all_jobs = client.get_ddl_jobs().to_vec();
    let mut ddl_jobs = filter_ddl_jobs(&mut all_jobs, &tables);
    ddl_jobs = filter_ddl_job_by_rules(ddl_jobs, &[ddl_job_block_list_rule]);
    if cfg.allow_pitr_from_incremental {
        check_ddl_job_by_rules(
            &ddl_jobs,
            &[ddl_job_log_incremental_compact_block_list_rule],
        )?;
    }
    pre_check_table_cluster_index(&tables, &ddl_jobs, dom.as_ref())?;
    cleanup.restore_db_config = Some(Box::new(tweak_local_conf_for_restore()));

    let client = cleanup.client.as_mut().expect("set above");
    if client.get_support_policy() {
        let policies = client.get_placement_policies()?;
        if is_full_restore(cmd_name) {
            // A full restore restores every policy.
            client.create_policies(ctx, &policies)?;
        } else {
            client.set_policy_map(Some(Arc::new(policies)));
        }
    }
    client.exec_ddls(ctx, &mut ddl_jobs)?;
    if dbs.is_empty() && tables.is_empty() {
        tidb_log::info(
            "nothing to restore, all databases and tables are filtered out",
            &[],
        );
    }
    let mut created_tables =
        create_dbs_and_tables(ctx, client, cfg, mgr, &mut db_plans, &table_plans)?;

    if let Some(file) =
        tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/task/sleep_for_check_scheduler_status")
            .map(|v| v.string())
    {
        if let Err(err) = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&file)
        {
            tidb_log::warn(
                "failed to create file",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        } else {
            while std::path::Path::new(&file).exists() {
                std::thread::sleep(Duration::from_millis(300));
            }
        }
    }

    match get_any_file_key_from_tables(&tables) {
        None => {
            tidb_log::info("no files, empty databases and tables are restored", &[]);
            crate::summary::set_success_status(true);
        }
        Some(any_file_key) => {
            let old_keyspace = decode_keyspace(&any_file_key, backup_meta.api_version);
            let new_keyspace = mgr.get_storage().keyspace_prefix();
            // API V2 data needs the keyspace rewrite mode.
            if (!old_keyspace.is_empty() || !new_keyspace.is_empty())
                && client.get_rewrite_mode() == crate::restore::snap_client::RewriteMode::Legacy
            {
                return Err(errors::annotate(
                    errors::new(&errors::ERR_RESTORE_MODE_MISMATCH),
                    "cluster only supports legacy rewrite mode",
                ));
            }
            for ct in &mut created_tables {
                let Some(rules) = ct.rewrite_rule.as_mut() else {
                    continue;
                };
                rules.old_keyspace.clone_from(&old_keyspace);
                rules.new_keyspace.clone_from(&new_keyspace);
                for rule in &mut rules.data {
                    rule.old_key_prefix =
                        [old_keyspace.as_slice(), rule.old_key_prefix.as_slice()].concat();
                    rule.new_key_prefix =
                        [new_keyspace.as_slice(), rule.new_key_prefix.as_slice()].concat();
                }
            }
        }
    }

    // An incremental restore must not move the TSO back.
    if !client.is_incremental() {
        client
            .reset_ts(ctx, &mgr.pd_controller)
            .inspect_err(|err| {
                tidb_log::error(
                    "reset pd TS failed",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
            })?;
    }

    let (compact_protect_start_key, compact_protect_end_key) =
        encode_compact_and_check_key(mgr.get_storage().as_ref(), pre_alloc_range);
    client.restore_tables(
        ctx,
        &crate::restore::snap_client::RestoreTablesContext {
            log_progress: cfg.config.log_progress,
            split_size_bytes: kv_configs.merge_region_size.value,
            split_key_count: kv_configs.merge_region_key_count.value,
            // BRIE splits on tables by TiDB's `split-table` (Go
            // `ddl.EnableSplitTableRegion`).
            split_on_table: tidb_config::config_tree::config::get_global_config().split_table,
            online: cfg.online,
            created_tables: &created_tables,
            checkpoint_set_with_table_id: &checkpoint_set_with_table_id,
            compact_protect_start_key,
            compact_protect_end_key,
            glue: g,
        },
    )?;

    let pl_ctx = crate::restore::snap_client::pipeline_items::PipelineContext {
        // An incremental backup has no checksums to validate.
        checksum: cfg.config.checksum && !client.is_incremental(),
        load_stats: cfg.load_stats,
        load_stats_physical,
        load_sys_table_physical,
        wait_tiflash_ready: cfg.wait_tiflash_ready,
        log_progress: cfg.config.log_progress,
        checksum_concurrency: cfg.config.checksum_concurrency,
        stats_concurrency: cfg.stats_concurrency,
        restore_ts,
        kv_client: mgr.get_storage().get_client(),
        ext_storage: s,
        glue: g,
    };
    client.restore_pipeline(ctx, &pl_ctx, &created_tables)?;

    tidb_log::info(
        "restoring system schemas",
        &[
            Field::new("withSys", Value::Bool(cfg.with_sys_table)),
            Field::new("filter", Value::Str(format!("{:?}", cfg.config.filter_str))),
        ],
    );
    client.restore_system_schemas(
        ctx,
        cfg.config.table_filter.as_ref(),
        load_sys_table_physical,
    )?;

    cleanup.can_restore_schedulers = true;
    tidb_log::info("snapshot restore success", &[]);
    crate::summary::set_success_status(true);
    Ok(())
}

/// Go `configureRestoreClient`.
fn configure_restore_client(ctx: &Context, client: &mut SnapClient, cfg: &RestoreConfig) {
    client.set_rate_limit(cfg.config.rate_limit);
    client.set_crypter(Some(cfg.config.cipher_info.clone()));
    client.set_region_scan_concurrency(cfg.region_scan_concurrency);
    client.set_split_region_index_step(cfg.split_region_index_step);
    client.set_coarse_scatter(cfg.coarse_scatter);
    if cfg.no_schema {
        client.enable_skip_create_sql();
    }
    client.set_batch_ddl_size(cfg.ddl_batch_size);
    client.set_placement_policy_mode(&cfg.with_placement_policy);
    client.set_with_sys_table(cfg.with_sys_table);
    client.set_rewrite_mode(ctx);
    client.set_check_privilege_table_rows_collate_compatibility(cfg.sys_check_collation);
}

/// Go `createDBsAndTables`: the databases, then the tables; an incremental
/// restore rewrites to a TS taken after its DDL jobs ran
/// (https://github.com/pingcap/tidb/issues/54426).
fn create_dbs_and_tables(
    ctx: &Context,
    client: &mut SnapClient,
    cfg: &RestoreConfig,
    mgr: &Mgr,
    dbs: &mut [crate::restore_utils::common::DatabaseRestorePlan],
    tables: &[crate::restore_utils::common::TableRestorePlan],
) -> Result<Vec<crate::restore_utils::common::CreatedTable<metautil::Table>>> {
    let mut new_ts = 0;
    if client.is_incremental() && !cfg.allow_pitr_from_incremental {
        new_ts = get_ts_with_retry(ctx, mgr.get_pd_client().as_ref())?;
    }
    client.create_databases_with_plan(ctx, dbs)?;
    client.create_tables_with_plan(ctx, tables, new_ts)
}

#[cfg(test)]
mod tests;
