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

//! Go `br/pkg/task/backup.go`: the snapshot backup task.
//!
//! The repo layout (`--storage-layout=repo`), EBS backups and the CLI flag
//! parser have no SQL caller and are not ported: a BRIE backup is always
//! the legacy layout.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sha2::Digest as _;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::{BackupMeta, BackupRequest, CompressionType};
use tidb_proto::kvrpcpb;

use super::common::Config;
use crate::backup::client::{Client, ClientMgr, ProgressUnit};
use crate::backup::schema::BackupSchemasArgs;
use crate::checkpoint::gojson;
use crate::conn::{Mgr, VersionCheckerType};
use crate::errors::{self, Result, ERR_INVALID_ARGUMENT};
use crate::gc::{self, BrServiceSafePoint};
use crate::glue::Glue;
use crate::metautil::{self, AppendOp, MetaWriter};
use crate::rtree::KeyRange;

/// Go `defaultBackupConcurrency`.
const DEFAULT_BACKUP_CONCURRENCY: u32 = 4;
/// Go `maxBackupConcurrency`.
const MAX_BACKUP_CONCURRENCY: u32 = 256;
/// Go `FullBackupCmd`.
pub const FULL_BACKUP_CMD: &str = "Full Backup";
/// Go `DBBackupCmd`.
pub const DB_BACKUP_CMD: &str = "Database Backup";
/// Go `TableBackupCmd`.
pub const TABLE_BACKUP_CMD: &str = "Table Backup";

/// Go `CompressionConfig`.
pub struct CompressionConfig {
    /// Go `CompressionType`.
    pub compression_type: CompressionType,
    /// Go `CompressionLevel`.
    pub compression_level: i32,
}

/// Go `BackupConfig`, the fields a BRIE backup reaches.
pub struct BackupConfig {
    /// Go embedded `Config`.
    pub config: Config,
    /// Go `TimeAgo`, nanoseconds (Go time.Duration).
    pub time_ago: i64,
    /// Go `BackupTS`.
    pub backup_ts: u64,
    /// Go `LastBackupTS`.
    pub last_backup_ts: u64,
    /// Go `GCTTL`, seconds.
    pub gc_ttl: i64,
    /// Go `RemoveSchedulers`.
    pub remove_schedulers: bool,
    /// Go `RangeLimit`.
    pub range_limit: i64,
    /// Go `IgnoreStats`.
    pub ignore_stats: bool,
    /// Go `UseBackupMetaV2`.
    pub use_backup_meta_v2: bool,
    /// Go `UseCheckpoint`.
    pub use_checkpoint: bool,
    /// Go `ReplicaReadLabel`.
    pub replica_read_label: BTreeMap<String, String>,
    /// Go `TableConcurrency` (it shadows the common config's).
    pub table_concurrency: usize,
    /// Go embedded `CompressionConfig`.
    pub compression_config: CompressionConfig,
}

/// Go `DefaultBackupConfig`: the backup flags' defaults over `config`.
#[must_use]
pub fn default_backup_config(config: Config) -> BackupConfig {
    BackupConfig {
        config,
        time_ago: 0,
        backup_ts: 0,
        last_backup_ts: 0,
        gc_ttl: gc::DEFAULT_BR_GC_SAFE_POINT_TTL,
        remove_schedulers: false,
        range_limit: crate::backup::client::RANGES_SENT_THRESHOLD,
        ignore_stats: true,
        use_backup_meta_v2: true,
        use_checkpoint: true,
        replica_read_label: BTreeMap::new(),
        table_concurrency: crate::backup::schema::DEFAULT_SCHEMA_CONCURRENCY,
        compression_config: CompressionConfig { compression_type: CompressionType::Zstd, compression_level: 0 },
    }
}

impl BackupConfig {
    /// Go `BackupConfig.Adjust`.
    pub fn adjust(&mut self) {
        self.config.adjust();
        let mut using_default_concurrency = false;
        if self.config.concurrency == 0 {
            self.config.concurrency = DEFAULT_BACKUP_CONCURRENCY;
            using_default_concurrency = true;
        }
        if self.config.concurrency > MAX_BACKUP_CONCURRENCY {
            self.config.concurrency = MAX_BACKUP_CONCURRENCY;
        }
        if self.config.rate_limit != 0 {
            // TiKV limits the upload rate per request, so concurrent
            // requests would exceed it.
            if !using_default_concurrency {
                tidb_log::warn(
                    "setting `--ratelimit` and `--concurrency` at the same time, ignoring `--concurrency`: `--ratelimit` forces sequential (i.e. concurrency = 1) backup",
                    &[
                        Field::new("ratelimit", Value::Str(format!("{}/s", crate::summary::human_size(self.config.rate_limit as f64)))),
                        Field::new("concurrency-specified", Value::U64(u64::from(self.config.concurrency))),
                    ],
                );
            }
            self.config.concurrency = 1;
        }
        if self.gc_ttl == 0 {
            self.gc_ttl = gc::DEFAULT_BR_GC_SAFE_POINT_TTL;
        }
        if self.compression_config.compression_type == CompressionType::Unknown {
            self.compression_config.compression_type = CompressionType::Zstd;
        }
    }

    /// Go `BackupConfig.Hash`: the SHA-256 of Go's `json.Marshal` of the
    /// configuration a resumed backup must keep (`immutableBackupConfig`).
    #[must_use]
    pub fn hash(&self) -> Vec<u8> {
        sha2::Sha256::digest(self.immutable_json().as_bytes()).to_vec()
    }

    /// Go `json.Marshal(immutableBackupConfig{...})`.
    fn immutable_json(&self) -> String {
        let c = &self.config;
        let s = |v: &str| gojson::string(v);
        let b = |v: bool| if v { "true" } else { "false" };
        let s3 = &c.backend_options.s3;
        let gcs = &c.backend_options.gcs;
        let az = &c.backend_options.azblob;
        let s3_json = format!(
            "{{\"endpoint\":{},\"region\":{},\"storage-class\":{},\"sse\":{},\"sse-kms-key-id\":{},\"sse-kms-context\":{},\"acl\":{},\"access-key\":{},\"secret-access-key\":{},\"session-token\":{},\"provider\":{},\"force-path-style\":{},\"use-accelerate-endpoint\":{},\"role-arn\":{},\"external-id\":{},\"profile\":{},\"object-lock-enabled\":{}}}",
            s(&s3.endpoint),
            s(&s3.region),
            s(&s3.storage_class),
            s(&s3.sse),
            s(&s3.sse_kms_key_id),
            s(&s3.sse_kms_context),
            s(&s3.acl),
            s(&s3.access_key),
            s(&s3.secret_access_key),
            s(&s3.session_token),
            s(&s3.provider),
            b(s3.force_path_style),
            b(s3.use_accelerate_endpoint),
            s(&s3.role_arn),
            s(&s3.external_id),
            s(&s3.profile),
            b(s3.object_lock_enabled),
        );
        let gcs_json = format!(
            "{{\"endpoint\":{},\"storage-class\":{},\"predefined-acl\":{},\"credentials-file\":{}}}",
            s(&gcs.endpoint),
            s(&gcs.storage_class),
            s(&gcs.predefined_acl),
            s(&gcs.credentials_file),
        );
        let az_json = format!(
            "{{\"endpoint\":{},\"account-name\":{},\"account-key\":{},\"access-tier\":{},\"sas-token\":{},\"encryption-scope\":{},\"encryption-key\":{}}}",
            s(&az.endpoint),
            s(&az.account_name),
            s(&az.account_key),
            s(&az.access_tier),
            s(&az.sas_token),
            s(&az.encryption_scope),
            s(&az.encryption_key),
        );
        let list = |v: &[String]| {
            if v.is_empty() {
                // Go marshals a nil slice as null; the BRIE builder always
                // has one.
                "null".to_owned()
            } else {
                format!("[{}]", v.iter().map(|x| s(x)).collect::<Vec<_>>().join(","))
            }
        };
        // gogoproto `CipherInfo`: both fields `omitempty`.
        let mut cipher = Vec::new();
        if c.cipher_info.cipher_type != 0 {
            cipher.push(format!("\"cipher_type\":{}", c.cipher_info.cipher_type));
        }
        if !c.cipher_info.cipher_key.is_empty() {
            cipher.push(format!("\"cipher_key\":{}", gojson::bytes(&c.cipher_info.cipher_key)));
        }
        format!(
            "{{\"last-backup-ts\":{},\"ignore-stats\":{},\"use-checkpoint\":{},\"s3\":{s3_json},\"gcs\":{gcs_json},\"azblob\":{az_json},\"storage\":{},\"pd\":{},\"send-credentials-to-tikv\":{},\"no-credentials\":{},\"filter-strings\":{},\"cipher\":{{{}}},\"keyspace-name\":{}}}",
            self.last_backup_ts,
            b(self.ignore_stats),
            b(self.use_checkpoint),
            s(&c.storage),
            list(&c.pd),
            b(c.send_creds),
            b(c.no_creds),
            list(&c.filter_str),
            cipher.join(","),
            s(&c.keyspace_name),
        )
    }
}

/// Go `isFullBackup`.
fn is_full_backup(cmd_name: &str) -> bool {
    cmd_name == FULL_BACKUP_CMD
}

/// Go `kvutil.BuildRequestSource(true, kv.InternalTxnBR, kvutil.ExplicitTypeBR)`.
const BR_REQUEST_SOURCE: &str = "internal_br_br";

/// Go `getProgressCountOfRanges`: the regions of the ranges, or the ranges
/// themselves past a thousand of them.
fn get_progress_count_of_ranges(ctx: &Context, mgr: &Mgr, ranges: &[KeyRange]) -> Result<(i64, ProgressUnit)> {
    if ranges.len() > 1000 {
        return Ok((ranges.len() as i64, ProgressUnit::Range));
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/task/progress-call-back").is_some() && ranges.len() > 100 {
        return Ok((ranges.len() as i64, ProgressUnit::Range));
    }
    let mut approximate_regions = 0;
    for r in ranges {
        approximate_regions += mgr.pd_controller.get_region_count(ctx, &r.start_key, &r.end_key)?;
    }
    crate::summary::collect_int("backup total regions", approximate_regions);
    Ok((approximate_regions, ProgressUnit::Region))
}

/// The meta writer between the steps that share it.
fn mw(m: &mut Arc<MetaWriter>) -> &mut MetaWriter {
    Arc::get_mut(m).expect("no backup step holds the meta writer")
}

/// The cleanups Go defers in `RunBackup`, run in reverse order.
#[derive(Default)]
struct Defers(Vec<Box<dyn FnOnce()>>);

impl Defers {
    fn push(&mut self, f: impl FnOnce() + 'static) {
        self.0.push(Box::new(f));
    }
}

impl Drop for Defers {
    fn drop(&mut self) {
        while let Some(f) = self.0.pop() {
            f();
        }
    }
}

/// Go `RunBackup`: a snapshot backup of the tables `cfg`'s filter matches
/// to `cfg.storage`, inside the calling thread.
///
/// # Errors
/// Any step's error; the backup is not usable then.
pub fn run_backup(c: &Context, g: &dyn Glue, cmd_name: &str, cfg: &mut BackupConfig) -> Result<()> {
    cfg.adjust();
    let res = run_backup_inner(c, g, cmd_name, cfg);
    crate::summary::summary(cmd_name);
    res
}

#[allow(clippy::too_many_lines)]
fn run_backup_inner(c: &Context, g: &dyn Glue, cmd_name: &str, cfg: &mut BackupConfig) -> Result<()> {
    let mut defers = Defers::default();
    let (ctx, cancel) = Context::with_cancel_cause(c);
    {
        let cancel = cancel.clone();
        defers.push(move || cancel.cancel(None));
    }

    let is_incremental_backup = cfg.last_backup_ts > 0;
    let skip_checksum = !cfg.config.checksum || is_incremental_backup;
    let u = tidb_objstore::parse_backend(&cfg.config.storage, Some(&cfg.config.backend_options)).map_err(errors::errorf)?;
    if matches!(u.backend, Some(tidb_proto::backup::storage_backend::Backend::Noop(_))) {
        tidb_log::info("since noop external storage is used, turn off checkpoint mode", &[]);
        cfg.use_checkpoint = false;
    }
    let skip_stats = cfg.ignore_stats;
    // The domain is needed only for the statistics.
    let need_domain = !skip_stats;
    let mgr = Arc::new(super::common::new_mgr(&ctx, g, &cfg.config, need_domain, VersionCheckerType::Normal)?);
    {
        let mgr = Arc::clone(&mgr);
        defers.push(move || mgr.close());
    }
    if cfg.use_checkpoint {
        if let Err(err) = crate::version::check_checkpoint_support() {
            tidb_log::warn(
                "unable to use checkpoint mode, fall back to normal mode",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
            cfg.use_checkpoint = false;
        }
    }
    let stats_handle = if skip_stats { None } else { mgr.get_domain().cloned() };

    let mut new_collation_enable = String::new();
    g.use_one_shot_session(!need_domain, &mut |se| {
        new_collation_enable = se.get_global_variable(crate::utils::misc::TIDB_NEW_COLLATION_ENABLED)?;
        tidb_log::info(
            &format!("get {} config from mysql.tidb table", crate::utils::misc::TIDB_NEW_COLLATION_ENABLED),
            &[Field::new(crate::utils::misc::TIDB_NEW_COLLATION_ENABLED, Value::Str(new_collation_enable.clone()))],
        );
        Ok(())
    })?;

    let mut client = Client::new_table_backup_client(&ctx, Arc::clone(&mgr) as Arc<dyn ClientMgr>);
    // The cipher is set only for the checkpoint.
    client.set_cipher(&cfg.config.cipher_info);
    client.set_skip_checksum(skip_checksum);

    let opts = tidb_objstore::storeapi::Options {
        no_credentials: cfg.config.no_creds,
        send_credentials: cfg.config.send_creds,
        check_s3_object_lock_options: true,
        ..Default::default()
    };
    let cfg_hash = cfg.hash();
    client.set_storage_and_check_not_in_use(&ctx, u, &opts)?;
    // A checkpoint of another configuration stops the backup, even when
    // checkpoint mode is off now.
    client.check_checkpoint(&cfg_hash)?;
    client.set_lock_file(&ctx)?;
    if cfg.use_checkpoint && cfg.gc_ttl == gc::DEFAULT_BR_GC_SAFE_POINT_TTL {
        cfg.gc_ttl = gc::DEFAULT_CHECKPOINT_GC_SAFE_POINT_TTL;
        tidb_log::info("use checkpoint's default GC TTL", &[Field::new("GC TTL", Value::I64(cfg.gc_ttl))]);
    }
    client.set_gc_ttl(cfg.gc_ttl);

    let backup_ts = client.get_ts(&ctx, cfg.time_ago, cfg.backup_ts)?;
    g.record("BackupTS", backup_ts);
    let safe_point_id = client.get_safe_point_id();
    let mut sp = BrServiceSafePoint { backup_ts, ttl: client.get_gc_ttl(), id: safe_point_id.clone() };
    // An incremental backup keeps the last backup's TS.
    if is_incremental_backup {
        sp.backup_ts = cfg.last_backup_ts;
    }
    let mut fields = vec![];
    fields.extend(sp.log_fields());
    tidb_log::info("current backup safePoint job", &fields);
    let (cctx, keeper_cancel) = Context::with_cancel(&ctx);
    let keeper_removable = Arc::new(AtomicBool::new(false));
    {
        let use_checkpoint = cfg.use_checkpoint;
        let removable = Arc::clone(&keeper_removable);
        let gc_mgr = Arc::clone(mgr.get_gc_manager());
        let sp = sp.clone();
        let keeper_cancel = keeper_cancel.clone();
        let ctx = ctx.clone();
        defers.push(move || {
            // A checkpoint backup that did not finish keeps its safe point
            // for the next attempt.
            if use_checkpoint && !removable.load(Ordering::SeqCst) {
                tidb_log::info("skip removing gc-safepoint keeper for next retry", &[Field::new("gc-id", Value::Str(sp.id.clone()))]);
                return;
            }
            tidb_log::info("start to remove gc-safepoint keeper", &[]);
            keeper_cancel.cancel();
            // Go removes it with the task's context, done by now on a
            // failure; PD is still asked.
            let _ = ctx;
            if let Err(err) = gc_mgr.delete_service_safe_point(&Context::background(), &sp) {
                tidb_log::warn(
                    "failed to remove service safe point, backup may fail if gc triggered",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
            }
            tidb_log::info("finish removing gc-safepoint keeper", &[]);
        });
    }
    let keeper = gc::start_service_safe_point_keeper(&cctx, sp.clone(), Arc::clone(mgr.get_gc_manager()))?;
    {
        // Go's keeper panics the process when the GC safe point passes the
        // backup; here the task is cancelled with that error.
        let cancel = cancel.clone();
        std::thread::spawn(move || {
            if let Ok(Some(err)) = keeper.join() {
                cancel.cancel(Some(err));
            }
        });
    }

    if cfg.remove_schedulers {
        tidb_log::debug("removing some PD schedulers", &[]);
        let (restore, e) = mgr.pd_controller.remove_schedulers(&ctx);
        let ctx = ctx.clone();
        defers.push(move || {
            let ctx = if ctx.err().is_some() {
                tidb_log::warn("context canceled, doing clean work with background context", &[]);
                Context::background()
            } else {
                ctx
            };
            if let Err(err) = restore(&ctx) {
                tidb_log::warn(
                    "failed to restore removed schedulers, you may need to restore them manually",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
            }
        });
        e?;
    }

    let req = BackupRequest {
        cluster_id: client.get_cluster_id(),
        start_version: cfg.last_backup_ts,
        end_version: backup_ts,
        rate_limit: cfg.config.rate_limit,
        storage_backend: client.get_storage_backend().cloned(),
        concurrency: DEFAULT_BACKUP_CONCURRENCY,
        compression_type: cfg.compression_config.compression_type as i32,
        compression_level: cfg.compression_config.compression_level,
        cipher_info: Some(cfg.config.cipher_info.clone()),
        replica_read: !cfg.replica_read_label.is_empty(),
        context: Some(kvrpcpb::Context {
            resource_control_context: Some(kvrpcpb::ResourceControlContext::default()),
            request_source: BR_REQUEST_SOURCE.to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let br_version = g.get_version();
    let cluster_version = mgr.pd_controller.get_cluster_version(&ctx)?;

    let (ranges, schemas, policies) = client.build_backup_range_and_schema(
        mgr.get_storage(),
        Arc::from(cfg.config.table_filter.to_lower()),
        backup_ts,
        is_full_backup(cmd_name),
    )?;

    let mut metawriter = Arc::new(MetaWriter::new(
        client.get_storage().clone(),
        metautil::META_FILE_SIZE,
        cfg.use_backup_meta_v2,
        "",
        Some(cfg.config.cipher_info.clone()),
    ));
    let api_version = mgr.get_storage().api_version();
    metawriter.update(|m: &mut BackupMeta| {
        m.start_version = req.start_version;
        m.end_version = req.end_version;
        m.is_raw_kv = req.is_raw_kv;
        m.cluster_id = req.cluster_id;
        m.cluster_version = cluster_version.clone();
        m.br_version = br_version.clone();
        m.new_collations_enabled = new_collation_enable.clone();
        m.api_version = api_version;
    });
    tidb_log::info("get placement policies", &[Field::new("count", Value::I64(policies.len() as i64))]);
    if !policies.is_empty() {
        metawriter.update(|m| m.policies = policies);
    }

    if ranges.is_empty() && schemas.as_ref().is_none_or(|s| s.is_empty()) {
        tidb_log::warn(
            "Nothing to backup, maybe connected to cluster for restoring",
            &[Field::new("PD address", Value::Str(cfg.config.pd.join(",")))],
        );
        metawriter.flush_backup_meta(&ctx).map_err(errors::errorf)?;
        keeper_removable.store(true, Ordering::SeqCst);
        crate::summary::set_success_status(true);
        return Ok(());
    }

    if is_incremental_backup {
        if backup_ts <= cfg.last_backup_ts {
            tidb_log::error("LastBackupTS is larger or equal to current TS", &[]);
            return Err(errors::annotate(errors::new(&ERR_INVALID_ARGUMENT), "LastBackupTS is larger or equal to current TS"));
        }
        gc::check_gc_safe_point(&ctx, mgr.get_gc_manager().as_ref(), cfg.last_backup_ts).inspect_err(|err| {
            tidb_log::error("Check gc safepoint for last backup ts failed", &[Field::new("error", Value::Str(err.to_string()))]);
        })?;
        mw(&mut metawriter).start_write_metas_async(&ctx, AppendOp::Ddl);
        crate::backup::client::write_backup_ddl_jobs(&metawriter, g, mgr.get_storage(), cfg.last_backup_ts, backup_ts, need_domain)?;
        mw(&mut metawriter).finish_write_metas(&ctx, AppendOp::Ddl).map_err(errors::errorf)?;
    }

    crate::summary::collect_int("backup total ranges", ranges.len() as i64);
    let (progress_total_count, progress_unit) = get_progress_count_of_ranges(&ctx, &mgr, &ranges)?;
    let update_ch = g.start_progress(&ctx, cmd_name, progress_total_count, !cfg.config.log_progress);
    let progress_call_back: Arc<dyn Fn(ProgressUnit) + Send + Sync> = {
        let update_ch = Arc::clone(&update_ch);
        let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        Arc::new(move |unit: ProgressUnit| {
            if unit == progress_unit {
                update_ch.inc();
                tidb_failpoint::inject("github.com/pingcap/tidb/br/pkg/task/progress-call-back", |v| {
                    tidb_log::info("failpoint progress-call-back injected", &[]);
                    let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                    let file = v.string();
                    if !file.is_empty() {
                        use std::io::Write as _;
                        match std::fs::OpenOptions::new().create(true).write(true).truncate(false).open(&file) {
                            Ok(mut f) => {
                                if let Err(err) = writeln!(f, "{}:{n}", progress_unit.as_str()) {
                                    tidb_log::warn("failed to write data to file", &[Field::new("error", Value::Str(err.to_string()))]);
                                }
                            }
                            Err(err) => {
                                tidb_log::warn("failed to create file", &[Field::new("error", Value::Str(err.to_string()))]);
                            }
                        }
                    }
                });
            }
        })
    };

    if cfg.use_checkpoint {
        client.start_checkpoint_runner(&ctx, &cfg_hash, backup_ts, 0, &safe_point_id)?;
    }
    // The checkpoint cleanup runs first of the deferred steps, as Go's last
    // `defer`; the client is borrowed until then.
    let result = (|| -> Result<()> {
        tidb_failpoint::inject("github.com/pingcap/tidb/br/pkg/task/s3-outage-during-writing-file", |v| {
            tidb_log::info(
                "failpoint s3-outage-during-writing-file injected, process will sleep for 5s and notify the shell to kill s3 service.",
                &[],
            );
            let file = v.string();
            if !file.is_empty() {
                let _ = std::fs::File::create(file);
            }
            std::thread::sleep(Duration::from_secs(5));
        });

        mw(&mut metawriter).start_write_metas_async(&ctx, AppendOp::DataFile);
        let checksum_map = client.backup_ranges(
            &ctx,
            &ranges,
            req.clone(),
            cfg.config.concurrency as usize,
            cfg.range_limit,
            cfg.replica_read_label.clone(),
            Arc::clone(&metawriter),
            Arc::clone(&progress_call_back),
        )?;
        // The backup has finished.
        update_ch.close();
        mw(&mut metawriter).finish_write_metas(&ctx, AppendOp::DataFile).map_err(errors::errorf)?;

        let checksum_progress = if checksum_map.is_empty() { 0 } else { schemas.as_ref().map_or(0, |s| s.len() as i64) };
        if skip_checksum {
            if is_incremental_backup {
                tidb_log::info("Skip fast checksum in incremental backup", &[]);
            } else {
                tidb_log::info("Skip fast checksum", &[]);
            }
        }
        let update_ch = g.start_progress(&ctx, "Checksum", checksum_progress, !cfg.config.log_progress);
        if let Some(schemas) = schemas.as_ref().filter(|s| !s.is_empty()) {
            let schemas_concurrency = cfg.table_concurrency.min(schemas.len());
            let domain = mgr.get_domain().cloned().or_else(|| g.get_domain().ok());
            schemas.backup_schemas(
                &ctx,
                BackupSchemasArgs {
                    meta_writer: mw(&mut metawriter),
                    checkpoint_runner: client.get_checkpoint_runner(),
                    store: mgr.get_storage(),
                    stats_handle: stats_handle.as_deref(),
                    domain: domain.as_deref(),
                    backup_ts,
                    checksum_map: Some(&checksum_map),
                    concurrency: schemas_concurrency,
                    cop_concurrency: cfg.config.checksum_concurrency as u64,
                    skip_checksum,
                    update_ch: Some(Arc::clone(&update_ch)),
                },
            )?;
        }
        metawriter.flush_backup_meta(&ctx).map_err(errors::errorf)?;
        // The backup meta is on the storage: the safe point and the
        // checkpoint data may go.
        keeper_removable.store(true, Ordering::SeqCst);
        update_ch.close();

        let archive_size = metawriter.archive_size();
        g.record(crate::summary::BACKUP_DATA_SIZE, archive_size);
        // Go issue 27247: BRIE reads the size as "Size".
        g.record("Size", archive_size);
        crate::summary::set_success_status(true);
        Ok(())
    })();
    if cfg.use_checkpoint {
        if keeper_removable.load(Ordering::SeqCst) {
            tidb_log::info("start to remove checkpoint data for backup", &[]);
            client.wait_for_finish_checkpoint(&ctx, false);
            match crate::checkpoint::backup::remove_checkpoint_data_for_backup(&ctx, client.get_storage()) {
                Ok(()) => tidb_log::info("the checkpoint data for backup is removed.", &[]),
                Err(err) => tidb_log::warn(
                    "failed to remove checkpoint data for backup",
                    &[Field::new("error", Value::Str(err.to_string()))],
                ),
            }
        } else {
            tidb_log::info("wait for checkpoint flush and keep it for resume", &[]);
            client.wait_for_finish_checkpoint(&ctx, true);
        }
    }
    drop(defers);
    result
}

#[cfg(test)]
mod tests;
