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

//! Go `br/pkg/backup/schema.go`: each backed-up schema with its table
//! checksum, statistics and merge option, written to the meta file.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::{DBInfo, TableInfo};
use tidb_proto::backup::{Schema, StatsFileIndex};

use crate::checkpoint::backup::BackupRunner;
use crate::checkpoint::ChecksumItem;
use crate::checksum::ExecutorBuilder;
use crate::errors::{self, Result, ERR_BACKUP_CHECKSUM_MISMATCH};
use crate::glue::{Domain, Progress, Storage};
use crate::metautil::{AppendOp, ChecksumStats, MetaItem, MetaWriter, StatsWriter};
use crate::utils::misc::{is_sys_db, temporary_db_name, LABEL_RULE_BATCH_SIZE};

/// Go `DefaultSchemaConcurrency`.
pub const DEFAULT_SCHEMA_CONCURRENCY: usize = 64;

/// Go `kvutil.ExplicitTypeBR`.
pub(crate) const EXPLICIT_TYPE_BR: &str = "br";

/// Go `schemaInfo`.
#[derive(Default)]
struct SchemaInfo {
    table_info: Option<TableInfo>,
    db_info: DBInfo,
    crc64xor: u64,
    total_kvs: u64,
    total_bytes: u64,
    stats_index: Vec<StatsFileIndex>,
    is_merge_option_allowed: bool,
    partition_merge_option_allowed: HashMap<String, bool>,
}

/// Go `iterFuncTp`: calls `f` with each schema to back up.
pub type IterFunc =
    Box<dyn Fn(&mut dyn FnMut(DBInfo, Option<TableInfo>)) -> Result<()> + Send + Sync>;

/// Go `Schemas`.
pub struct Schemas {
    iter_func: IterFunc,
    size: usize,
    checkpoint_checksum: Option<HashMap<i64, ChecksumItem>>,
}

/// The arguments of [`Schemas::backup_schemas`] beyond the schemas.
pub struct BackupSchemasArgs<'a> {
    /// Go `metaWriter`.
    pub meta_writer: &'a mut MetaWriter,
    /// Go `checkpointRunner`.
    pub checkpoint_runner: Option<&'a BackupRunner>,
    /// Go `store`.
    pub store: &'a Arc<dyn Storage>,
    /// Go `statsHandle`: the domain whose statistics are dumped, `None` to
    /// skip them.
    pub stats_handle: Option<&'a dyn Domain>,
    /// The domain whose PD label rules give the merge option.
    pub domain: Option<&'a dyn Domain>,
    /// Go `backupTS`.
    pub backup_ts: u64,
    /// Go `checksumMap`: the checksums of the backed-up files, matched with
    /// TiDB's.
    pub checksum_map: Option<&'a BTreeMap<i64, ChecksumStats>>,
    /// Go `concurrency`.
    pub concurrency: usize,
    /// Go `copConcurrency`.
    pub cop_concurrency: u64,
    /// Go `skipChecksum`.
    pub skip_checksum: bool,
    /// Go `updateCh`.
    pub update_ch: Option<Arc<dyn Progress>>,
}

impl Schemas {
    /// Go `NewBackupSchemas`.
    #[must_use]
    pub fn new(iter_func: IterFunc, size: usize) -> Self {
        Self {
            iter_func,
            size,
            checkpoint_checksum: None,
        }
    }

    /// Go `SetCheckpointChecksum`.
    pub fn set_checkpoint_checksum(&mut self, checksum: HashMap<i64, ChecksumItem>) {
        self.checkpoint_checksum = Some(checksum);
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.size
    }

    /// Whether no schema is backed up.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Go `BackupSchemas`: each schema's checksum (from the checkpoint when
    /// it has one, else TiKV's, matched with the files'), statistics and
    /// merge option, `concurrency` at a time, written to the meta file's
    /// schema phase.
    ///
    /// Go hands each schema to the worker pool as the iteration reaches it;
    /// here the iteration collects them first.
    ///
    /// # Errors
    /// A checksum, statistics or meta write error, or a checksum mismatch.
    pub fn backup_schemas(&self, ctx: &Context, args: BackupSchemasArgs<'_>) -> Result<()> {
        let start_all = Instant::now();
        let op = AppendOp::Schema;
        args.meta_writer.start_write_metas_async(ctx, op);
        let mut queue = Vec::new();
        (self.iter_func)(&mut |mut db_info, table_info| {
            if is_sys_db(db_info.name.lowercase()) {
                db_info.name = tidb_ast::CiString::new(temporary_db_name(db_info.name.original()));
            }
            let checksum = match (&self.checkpoint_checksum, &table_info) {
                (Some(m), Some(t)) => m.get(&t.id).cloned(),
                _ => None,
            };
            queue.push((
                SchemaInfo {
                    table_info,
                    db_info,
                    ..Default::default()
                },
                checksum,
            ));
        })?;
        queue.reverse();
        let queue = Mutex::new(queue);
        let first: Mutex<Option<errors::Error>> = Mutex::new(None);
        let (ectx, ecancel) = Context::with_cancel(ctx);
        let meta_writer: &MetaWriter = args.meta_writer;
        std::thread::scope(|scope| {
            for _ in 0..args.concurrency.max(1) {
                scope.spawn(|| loop {
                    if ectx.err().is_some() {
                        return;
                    }
                    let Some((mut schema, checksum)) = queue.lock().unwrap().pop() else {
                        return;
                    };
                    if let Err(err) =
                        backup_one(ctx, &ectx, &args, meta_writer, &mut schema, checksum)
                    {
                        first.lock().unwrap().get_or_insert(err);
                        ecancel.cancel();
                        return;
                    }
                });
            }
        });
        if let Some(err) = first.into_inner().unwrap() {
            return Err(err);
        }
        tidb_log::info(
            "Backup calculated table checksum into metas",
            &[Field::new(
                "take",
                Value::Duration(start_all.elapsed().as_nanos() as i64),
            )],
        );
        crate::summary::collect_duration("backup checksum", start_all.elapsed());
        args.meta_writer
            .finish_write_metas(ctx, op)
            .map_err(errors::errorf)
    }
}

/// One schema of Go `BackupSchemas`' worker.
fn backup_one(
    ctx: &Context,
    ectx: &Context,
    args: &BackupSchemasArgs<'_>,
    meta_writer: &MetaWriter,
    schema: &mut SchemaInfo,
    checksum: Option<ChecksumItem>,
) -> Result<()> {
    if let Some(table) = schema.table_info.clone() {
        let db = schema.db_info.name.original().to_owned();
        let fields = |extra: Vec<Field>| {
            let mut f = vec![
                Field::new("db", Value::Str(db.clone())),
                Field::new("table", Value::Str(table.name.original().to_owned())),
            ];
            f.extend(extra);
            f
        };
        if !args.skip_checksum {
            tidb_log::info("Calculate table checksum start", &fields(vec![]));
            if let Some(c) = checksum {
                schema.crc64xor = c.crc64xor;
                schema.total_kvs = c.total_kvs;
                schema.total_bytes = c.total_bytes;
                tidb_log::info(
                    "Calculate table checksum completed (from checkpoint)",
                    &fields(vec![
                        Field::new("Crc64Xor", Value::U64(schema.crc64xor)),
                        Field::new("TotalKvs", Value::U64(schema.total_kvs)),
                        Field::new("TotalBytes", Value::U64(schema.total_bytes)),
                    ]),
                );
            } else {
                let start = Instant::now();
                schema.calculate_checksum(
                    ectx,
                    args.store,
                    args.backup_ts,
                    args.cop_concurrency,
                )?;
                let cost = start.elapsed();
                if let Some(runner) = args.checkpoint_runner {
                    runner.flush_checksum(
                        ctx,
                        table.id,
                        schema.crc64xor,
                        schema.total_kvs,
                        schema.total_bytes,
                    )?;
                }
                tidb_log::info(
                    "Calculate table checksum completed",
                    &fields(vec![
                        Field::new("Crc64Xor", Value::U64(schema.crc64xor)),
                        Field::new("TotalKvs", Value::U64(schema.total_kvs)),
                        Field::new("TotalBytes", Value::U64(schema.total_bytes)),
                        Field::new("TimeTaken", Value::Duration(cost.as_nanos() as i64)),
                    ]),
                );
            }
            if let Some(m) = args.checksum_map {
                schema.match_checksum(m)?;
            }
        }
        if let Some(handle) = args.stats_handle {
            let mut writer = meta_writer.new_stats_writer();
            if let Err(err) = schema.dump_stats_to_json(ctx, &mut writer, handle, args.backup_ts) {
                tidb_log::error(
                    "dump table stats failed",
                    &fields(vec![Field::new("error", Value::Str(err.to_string()))]),
                );
                return Err(err);
            }
        }
        match schema.check_merge_option_allowed(args.store.as_ref(), args.domain) {
            Err(err) => tidb_log::warn(
                "failed to check merge_option for table",
                &fields(vec![Field::new("error", Value::Str(err.to_string()))]),
            ),
            Ok((allowed, partitions)) => {
                schema.is_merge_option_allowed = allowed;
                schema.partition_merge_option_allowed = partitions;
            }
        }
    }
    let s = schema.encode_to_schema()?;
    meta_writer
        .send(MetaItem::Schema(s))
        .map_err(errors::errorf)?;
    if let Some(p) = &args.update_ch {
        p.inc();
    }
    Ok(())
}

impl SchemaInfo {
    /// Go `calculateChecksum`.
    fn calculate_checksum(
        &mut self,
        ctx: &Context,
        store: &Arc<dyn Storage>,
        backup_ts: u64,
        concurrency: u64,
    ) -> Result<()> {
        let table = self.table_info.as_ref().expect("a table schema");
        let exe = ExecutorBuilder::new(table, backup_ts)
            .set_explicit_request_source_type(EXPLICIT_TYPE_BR)
            .set_concurrency(concurrency)
            .build()?;
        let resp = exe.execute(ctx, store.get_client().as_ref(), || {})?;
        self.crc64xor = resp.checksum.unwrap_or_default();
        self.total_kvs = resp.total_kvs.unwrap_or_default();
        self.total_bytes = resp.total_bytes.unwrap_or_default();
        Ok(())
    }

    /// Go `matchChecksum`: TiKV's checksum of the table and its partitions
    /// against the files'.
    fn match_checksum(&self, checksum_map: &BTreeMap<i64, ChecksumStats>) -> Result<()> {
        let table = self.table_info.as_ref().expect("a table schema");
        let mut ids = vec![table.id];
        if let Some(part) = table.get_partition_info() {
            ids.extend(part.read().definitions.map_visible(|d| d.id));
        }
        let (mut crc, mut kvs, mut bytes) = (0_u64, 0_u64, 0_u64);
        for id in ids {
            if let Some(c) = checksum_map.get(&id) {
                crc ^= c.crc64_xor;
                kvs = kvs.wrapping_add(c.total_kvs);
                bytes = bytes.wrapping_add(c.total_bytes);
            }
        }
        let db = self.db_info.name.original().to_owned();
        let tbl = table.name.original().to_owned();
        if self.crc64xor != crc || self.total_kvs != kvs || self.total_bytes != bytes {
            tidb_log::error(
                "checksum mismatch",
                &[
                    Field::new("db", Value::Str(db)),
                    Field::new("table", Value::Str(tbl)),
                    Field::new("origin tidb crc64", Value::U64(self.crc64xor)),
                    Field::new("calculated crc64", Value::U64(crc)),
                    Field::new("origin tidb total kvs", Value::U64(self.total_kvs)),
                    Field::new("calculated total kvs", Value::U64(kvs)),
                    Field::new("origin tidb total bytes", Value::U64(self.total_bytes)),
                    Field::new("calculated total bytes", Value::U64(bytes)),
                ],
            );
            return Err(errors::new(&ERR_BACKUP_CHECKSUM_MISMATCH));
        }
        tidb_log::info(
            "checksum success",
            &[
                Field::new("db", Value::Str(db)),
                Field::new("table", Value::Str(tbl)),
            ],
        );
        Ok(())
    }

    /// Go `dumpStatsToJSON`.
    fn dump_stats_to_json(
        &mut self,
        ctx: &Context,
        writer: &mut StatsWriter,
        handle: &dyn Domain,
        backup_ts: u64,
    ) -> Result<()> {
        let table = self.table_info.as_ref().expect("a table schema");
        tidb_log::info(
            "dump stats to json",
            &[
                Field::new("db", Value::Str(self.db_info.name.original().to_owned())),
                Field::new("table", Value::Str(table.name.original().to_owned())),
            ],
        );
        handle.persist_stats_by_snapshot(
            self.db_info.name.original(),
            table,
            backup_ts,
            &mut |json, id| writer.backup_stats(ctx, json, id).map_err(errors::errorf),
        )?;
        self.stats_index = writer.backup_stats_done(ctx).map_err(errors::errorf)?;
        Ok(())
    }

    /// Go `encodeToSchema`. Go's `stats` field is never set by backup, so
    /// `Stats` stays empty.
    fn encode_to_schema(&self) -> Result<Schema> {
        let json = |e: serde_json::Error| errors::errorf(e.to_string());
        let db = tidb_model::serde_helpers::to_go_json(&self.db_info).map_err(json)?;
        let table = match &self.table_info {
            Some(t) => tidb_model::serde_helpers::to_go_json(t).map_err(json)?,
            None => Vec::new(),
        };
        Ok(Schema {
            db,
            table,
            crc64xor: self.crc64xor,
            total_kvs: self.total_kvs,
            total_bytes: self.total_bytes,
            stats: Vec::new(),
            stats_index: self.stats_index.clone(),
            is_merge_option_allowed: self.is_merge_option_allowed,
            partition_merge_option_allowed: self.partition_merge_option_allowed.clone(),
            ..Default::default()
        })
    }

    /// Go `checkMergeOptionAllowed`: whether the table's (and each
    /// partition's) PD label rule carries `merge_option=allow`.
    fn check_merge_option_allowed(
        &self,
        store: &dyn Storage,
        domain: Option<&dyn Domain>,
    ) -> Result<(bool, HashMap<String, bool>)> {
        let table = self.table_info.as_ref().expect("a table schema");
        let codec = StoreCodec(store);
        let db_name = self.db_info.name.lowercase();
        let table_name = table.name.lowercase();
        let rule_id = tidb_ddl_label::new_rule_id(&codec, db_name, table_name, "");
        let parts: Vec<(String, String)> = table.get_partition_info().map_or_else(Vec::new, |p| {
            p.read()
                .definitions
                .map_visible(|d| (d.name.lowercase().to_owned(), d.name.original().to_owned()))
        });
        let mut rule_ids = vec![rule_id.clone()];
        for (l, _) in &parts {
            rule_ids.push(tidb_ddl_label::new_rule_id(&codec, db_name, table_name, l));
        }
        let domain = domain.ok_or_else(|| errors::errorf("no domain to read the label rules"))?;
        let mut rules: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for batch in rule_ids.chunks(LABEL_RULE_BATCH_SIZE) {
            rules.extend(domain.get_label_rules(batch)?);
        }
        let allows = |id: &str| {
            rules.get(id).is_some_and(|labels| {
                labels
                    .iter()
                    .any(|(k, v)| k == "merge_option" && v == "allow")
            })
        };
        let table_allowed = allows(&rule_id);
        let mut partitions = HashMap::new();
        for (l, o) in &parts {
            if allows(&tidb_ddl_label::new_rule_id(&codec, db_name, table_name, l)) {
                partitions.insert(o.clone(), true);
            }
        }
        Ok((table_allowed, partitions))
    }
}

/// The store's codec, as label rule IDs read it.
struct StoreCodec<'a>(&'a dyn Storage);

impl tidb_ddl_label::LabelCodec for StoreCodec<'_> {
    fn has_keyspace_meta(&self) -> bool {
        !self.0.keyspace_prefix().is_empty()
    }

    fn keyspace_id(&self) -> u32 {
        self.0.keyspace_id()
    }

    fn encode_region_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        self.0.encode_range(start, end)
    }
}
