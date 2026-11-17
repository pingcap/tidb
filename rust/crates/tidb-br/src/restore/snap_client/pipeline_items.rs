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

//! Go `br/pkg/restore/snap_client/pipeline_items.go`: the steps run on
//! each table once its data is in (checksum, statistics, TiFlash replica
//! sync), chained as concurrent stages. The physical stats and system
//! table loads (`replaceTables`) are `br` CLI options and are refused.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, select, Receiver, Sender};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::File;

use super::client::SnapClient;
use crate::errors::{self, Result};
use crate::glue::{Domain, Glue, MetaUpdate, Progress};
use crate::metautil;
use crate::restore_utils::common::CreatedTable;
use crate::restore_utils::misc::get_table_id_map;
use crate::utils::backoff::RetryState;
use crate::utils::retry::with_retry;

/// Go `defaultChannelSize`.
const DEFAULT_CHANNEL_SIZE: usize = 1024;
/// Go `defaultChecksumConcurrency`.
const DEFAULT_CHECKSUM_CONCURRENCY: usize = 64;
/// Go `statsMetaItemBufferSize`.
const STATS_META_ITEM_BUFFER_SIZE: usize = 3000;

/// A created table of the restore.
pub type Created = CreatedTable<metautil::Table>;

fn ctx_error(ctx: &Context) -> errors::Error {
    errors::errorf(
        ctx.err()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "context canceled".into()),
    )
}

/// Go `PipelineContext`.
pub struct PipelineContext {
    /// Go `Checksum`.
    pub checksum: bool,
    /// Go `LoadStats`.
    pub load_stats: bool,
    /// Go `LoadStatsPhysical`.
    pub load_stats_physical: bool,
    /// Go `LoadSysTablePhysical`.
    pub load_sys_table_physical: bool,
    /// Go `WaitTiflashReady`.
    pub wait_tiflash_ready: bool,
    /// Go `LogProgress`.
    pub log_progress: bool,
    /// Go `ChecksumConcurrency`.
    pub checksum_concurrency: usize,
    /// Go `StatsConcurrency`.
    pub stats_concurrency: usize,
    /// Go `RestoreTS`.
    pub restore_ts: u64,
    /// Go `KvClient`.
    pub kv_client: Arc<dyn tidb_txnkv::Client>,
    /// Go `ExtStorage`.
    pub ext_storage: metautil::Storage,
    /// Go `Glue`.
    pub glue: Arc<dyn Glue>,
}

type ProcessFn<'a> = Box<dyn Fn(&Context, &Created) -> Result<()> + Send + Sync + 'a>;
type EndFn<'a> = Box<dyn Fn(&Context) -> Result<()> + Send + Sync + 'a>;

/// Go `pipelineFunction`.
struct PipelineFunction<'a> {
    task_label: String,
    concurrency: usize,
    process_fn: ProcessFn<'a>,
    end_fn: EndFn<'a>,
}

/// Go `PipelineConcurrentBuilder`.
#[derive(Default)]
pub struct PipelineConcurrentBuilder<'a> {
    pipeline_functions: Vec<PipelineFunction<'a>>,
    load_stats_physical: bool,
    load_sys_table_physical: bool,
}

/// The first error of the pipeline's stages, which cancels the others (Go's
/// `errgroup.WithContext`).
struct FirstError {
    err: Mutex<Option<errors::Error>>,
    cancel: tidb_gocontext::CancelFunc,
}

impl FirstError {
    fn set(&self, err: errors::Error) {
        let mut first = self.err.lock().unwrap();
        if first.is_none() {
            *first = Some(err);
            self.cancel.cancel();
        }
    }
}

impl<'a> PipelineConcurrentBuilder<'a> {
    /// Go `RegisterPipelineTask`.
    pub fn register_pipeline_task(
        &mut self,
        task_label: &str,
        concurrency: usize,
        process_fn: impl Fn(&Context, &Created) -> Result<()> + Send + Sync + 'a,
        end_fn: impl Fn(&Context) -> Result<()> + Send + Sync + 'a,
    ) {
        self.pipeline_functions.push(PipelineFunction {
            task_label: task_label.to_owned(),
            concurrency: concurrency.max(1),
            process_fn: Box::new(process_fn),
            end_fn: Box::new(end_fn),
        });
    }

    /// Go `StartPipelineTask`: the tables flow through each stage in turn;
    /// the first stage error cancels the rest and is returned.
    ///
    /// # Errors
    /// The first stage error, or `ctx` done.
    pub fn start_pipeline_task(&self, ctx: &Context, created_tables: &[Created]) -> Result<()> {
        let (pctx, cancel) = Context::with_cancel(ctx);
        let first = FirstError {
            err: Mutex::new(None),
            cancel: cancel.clone(),
        };
        std::thread::scope(|s| {
            let (tx, mut rx) = bounded::<usize>(0);
            {
                let (pctx, first) = (&pctx, &first);
                let (stats_phys, sys_phys) =
                    (self.load_stats_physical, self.load_sys_table_physical);
                s.spawn(move || {
                    if let Err(e) =
                        after_table_restored(pctx, created_tables, stats_phys, sys_phys, &tx)
                    {
                        first.set(e);
                    }
                });
            }
            for f in &self.pipeline_functions {
                let (out_tx, out_rx) = bounded::<usize>(DEFAULT_CHANNEL_SIZE);
                let (pctx, first) = (&pctx, &first);
                s.spawn(move || {
                    // The error cancels the pipeline before the output
                    // closes, so the next stage cannot take the close for
                    // a finished input.
                    if let Err(e) = concurrent_handle_tables(pctx, rx, &out_tx, f, created_tables) {
                        first.set(e);
                    }
                    drop(out_tx);
                });
                rx = out_rx;
            }
            let (pctx, first) = (&pctx, &first);
            s.spawn(move || {
                // Go `dropToBlackhole`.
                loop {
                    select! {
                        recv(pctx.done()) -> _ => { first.set(ctx_error(pctx)); return; }
                        recv(rx) -> msg => if msg.is_err() { return; },
                    }
                }
            });
        });
        cancel.cancel();
        first.err.into_inner().unwrap().map_or(Ok(()), Err)
    }
}

/// Go `afterTableRestoredCh`: the tables, less the ones a physical load
/// already handled.
fn after_table_restored(
    pctx: &Context,
    created_tables: &[Created],
    load_stats_physical: bool,
    load_sys_table_physical: bool,
    out: &Sender<usize>,
) -> Result<()> {
    use super::systable_restore::{
        get_db_name_if_renameable_sys_temporary_table, get_db_name_if_stats_temporary_table,
    };
    for (i, t) in created_tables.iter().enumerate() {
        let old = t
            .old_table
            .as_ref()
            .expect("a created table has its backup table");
        let (db, name) = (old.db.name.original(), old.info().name.original());
        if load_stats_physical && get_db_name_if_stats_temporary_table(db, name).is_some() {
            continue;
        }
        if load_sys_table_physical
            && get_db_name_if_renameable_sys_temporary_table(db, name).is_some()
        {
            continue;
        }
        select! {
            recv(pctx.done()) -> _ => return Err(ctx_error(pctx)),
            send(out, i) -> _ => {}
        }
    }
    Ok(())
}

/// Go `concurrentHandleTablesCh`: up to `concurrency` tables processed at
/// once; the end function runs once every table has passed and nothing
/// failed.
fn concurrent_handle_tables(
    pctx: &Context,
    in_rx: Receiver<usize>,
    out_tx: &Sender<usize>,
    f: &PipelineFunction<'_>,
    created_tables: &[Created],
) -> Result<()> {
    let (ectx, ecancel) = Context::with_cancel(pctx);
    let stage_err: Mutex<Option<errors::Error>> = Mutex::new(None);
    let (give, take) = bounded::<()>(f.concurrency);
    for _ in 0..f.concurrency {
        let _ = give.send(());
    }
    std::thread::scope(|s| loop {
        let i = select! {
            recv(ectx.done()) -> _ => break,
            recv(in_rx) -> msg => match msg { Ok(i) => i, Err(_) => break },
        };
        if ectx.err().is_some() {
            break;
        }
        select! {
            recv(ectx.done()) -> _ => break,
            recv(take) -> _ => {}
        }
        let (ectx, ecancel, stage_err, give) = (&ectx, &ecancel, &stage_err, give.clone());
        s.spawn(move || {
            let res = (f.process_fn)(ectx, &created_tables[i]);
            let _ = give.send(());
            match res {
                Err(e) => {
                    let mut first = stage_err.lock().unwrap();
                    if first.is_none() {
                        *first = Some(e);
                        ecancel.cancel();
                    }
                }
                Ok(()) => {
                    select! {
                        recv(ectx.done()) -> _ => {}
                        send(out_tx, i) -> _ => {}
                    }
                }
            }
        });
    });
    ecancel.cancel();
    if let Some(err) = stage_err.into_inner().unwrap() {
        tidb_log::error(
            "pipeline item execution is failed",
            &[
                Field::new("task", Value::Str(f.task_label.clone())),
                Field::new("error", Value::Str(err.to_string())),
            ],
        );
        return Err(err);
    }
    if pctx.err().is_some() {
        return Err(ctx_error(pctx));
    }
    if let Err(err) = (f.end_fn)(pctx) {
        tidb_log::error(
            "pipeline defer execution is failed",
            &[
                Field::new("task", Value::Str(f.task_label.clone())),
                Field::new("error", Value::Str(err.to_string())),
            ],
        );
        return Err(err);
    }
    Ok(())
}

/// Go `statsMetaItemBuffer`: stats meta rows written in batches of 3000.
#[derive(Default)]
pub struct StatsMetaItemBuffer {
    meta_updates: Mutex<Vec<MetaUpdate>>,
}

impl StatsMetaItemBuffer {
    /// Go `appendItem`: the full batch, once there is one.
    fn append_item(&self, item: MetaUpdate) -> Vec<MetaUpdate> {
        let mut updates = self.meta_updates.lock().unwrap();
        updates.push(item);
        if updates.len() < STATS_META_ITEM_BUFFER_SIZE {
            return Vec::new();
        }
        std::mem::replace(
            &mut *updates,
            Vec::with_capacity(STATS_META_ITEM_BUFFER_SIZE),
        )
    }

    /// Go `UpdateMetasRest`.
    ///
    /// # Errors
    /// The write error after its retries.
    pub fn update_metas_rest(&self, ctx: &Context, dom: &dyn Domain) -> Result<()> {
        let updates = std::mem::take(&mut *self.meta_updates.lock().unwrap());
        if updates.is_empty() {
            return Ok(());
        }
        save_meta_to_storage_with_retry(ctx, dom, &updates)
    }

    /// Go `TryUpdateMetas`.
    ///
    /// # Errors
    /// The write error after its retries.
    pub fn try_update_metas(
        &self,
        ctx: &Context,
        dom: &dyn Domain,
        physical_id: i64,
        count: i64,
    ) -> Result<()> {
        let updates = self.append_item(MetaUpdate {
            physical_id,
            count,
            modify_count: count,
        });
        if updates.is_empty() {
            return Ok(());
        }
        save_meta_to_storage_with_retry(ctx, dom, &updates)
    }
}

/// Go `saveMetaToStorageWithRetry`: 8 tries, 500ms apart.
fn save_meta_to_storage_with_retry(
    ctx: &Context,
    dom: &dyn Domain,
    updates: &[MetaUpdate],
) -> Result<()> {
    let mut state = RetryState::new(8, Duration::from_millis(500), Duration::from_millis(500));
    with_retry(
        ctx,
        || {
            dom.save_meta_to_storage(updates).inspect_err(|err| {
                tidb_log::error(
                    "failed to save meta to storage",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
            })
        },
        &mut state,
    )
}

/// Go `calculateRowCountForPhysicalTable`: the row KVs of the files.
fn calculate_row_count_for_physical_table(files: &[File]) -> i64 {
    files
        .iter()
        .filter(|f| tidb_tablecodec::is_record_key(&f.start_key))
        .map(|f| f.total_kvs)
        .sum::<u64>() as i64
}

/// Go `updateStatsMetaForTable`: the row counts of the table and, for a
/// partitioned one, each partition, from its files.
fn update_stats_meta_for_table(
    ctx: &Context,
    buffer: &StatsMetaItemBuffer,
    dom: &dyn Domain,
    tbl: &Created,
) -> Result<()> {
    let old = tbl
        .old_table
        .as_ref()
        .expect("a created table has its backup table");
    let new = tbl.table.as_ref().expect("a created table has its schema");
    let log_err = |e: &errors::Error| {
        tidb_log::error(
            "update stats meta failed",
            &[Field::new("error", Value::Str(e.to_string()))],
        )
    };
    let Some(part) = &old.info().partition else {
        let files = old
            .files_of_physicals
            .get(&old.info().id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        return buffer
            .try_update_metas(
                ctx,
                dom,
                new.id,
                calculate_row_count_for_physical_table(files),
            )
            .inspect_err(log_err);
    };
    let mut total = 0;
    let mut counts = std::collections::HashMap::new();
    for (physical_id, files) in &old.files_of_physicals {
        if *physical_id == old.info().id {
            continue;
        }
        let count = calculate_row_count_for_physical_table(files);
        total += count;
        counts.insert(*physical_id, count);
    }
    for old_def in part.read().definitions.snapshot() {
        let count = counts.get(&old_def.id).copied().unwrap_or(0);
        if count > 0 {
            let new_def_id = crate::checksum::get_partition_by_name(new, old_def.name.lowercase())
                .inspect_err(|_| {
                    tidb_log::error(
                        "failed to get the partition by name",
                        &[
                            Field::new("db name", Value::Str(old.db.name.original().into())),
                            Field::new("table name", Value::Str(new.name.original().into())),
                            Field::new(
                                "partition name",
                                Value::Str(old_def.name.original().into()),
                            ),
                            Field::new("downstream table id", Value::I64(new.id)),
                            Field::new("upstream partition id", Value::I64(old_def.id)),
                        ],
                    );
                })?;
            buffer
                .try_update_metas(ctx, dom, new_def_id, count)
                .inspect_err(log_err)?;
        }
    }
    buffer
        .try_update_metas(ctx, dom, new.id, total)
        .inspect_err(log_err)
}

impl SnapClient {
    /// Go `RestorePipeline`: checksum, statistics and TiFlash sync, each a
    /// stage when enabled.
    ///
    /// # Errors
    /// A stage's error; a physical load is not supported.
    pub fn restore_pipeline(
        &self,
        ctx: &Context,
        pl_ctx: &PipelineContext,
        created_tables: &[Created],
    ) -> Result<()> {
        let start = Instant::now();
        let res = self.restore_pipeline_inner(ctx, pl_ctx, created_tables);
        crate::summary::collect_duration("restore pipeline", start.elapsed());
        res
    }

    fn restore_pipeline_inner(
        &self,
        ctx: &Context,
        pl_ctx: &PipelineContext,
        created_tables: &[Created],
    ) -> Result<()> {
        let pipeline_num = usize::from(pl_ctx.checksum)
            + usize::from(pl_ctx.load_stats && !pl_ctx.load_stats_physical)
            + usize::from(pl_ctx.wait_tiflash_ready);
        if pl_ctx.load_stats_physical || pl_ctx.load_sys_table_physical {
            return Err(errors::errorf(
                "loading statistics or system tables physically is not supported",
            ));
        }
        let progress_len = (pipeline_num * created_tables.len()) as i64;
        // Redirect to the log when there is no log file.
        let update_ch =
            pl_ctx
                .glue
                .start_progress(ctx, "Restore Pipeline", progress_len, !pl_ctx.log_progress);
        let mut builder = PipelineConcurrentBuilder {
            load_stats_physical: pl_ctx.load_stats_physical,
            load_sys_table_physical: pl_ctx.load_sys_table_physical,
            ..Default::default()
        };
        if pl_ctx.checksum {
            self.register_validate_checksum(
                &mut builder,
                Arc::clone(&pl_ctx.kv_client),
                Arc::clone(&update_ch),
                pl_ctx.checksum_concurrency,
            );
        }
        if pl_ctx.load_stats && !pl_ctx.load_stats_physical {
            self.register_update_meta_and_load_stats(
                &mut builder,
                pl_ctx.ext_storage.clone(),
                Arc::clone(&update_ch),
                pl_ctx.stats_concurrency,
            )?;
        }
        if pl_ctx.wait_tiflash_ready {
            self.register_wait_tiflash_ready(&mut builder, Arc::clone(&update_ch))?;
        }
        let res = builder.start_pipeline_task(ctx, created_tables);
        update_ch.close();
        res
    }

    /// Go `registerValidateChecksum`.
    fn register_validate_checksum<'a>(
        &'a self,
        builder: &mut PipelineConcurrentBuilder<'a>,
        kv_client: Arc<dyn tidb_txnkv::Client>,
        update_ch: Arc<dyn Progress>,
        concurrency: usize,
    ) {
        builder.register_pipeline_task(
            "Restore Checksum",
            DEFAULT_CHECKSUM_CONCURRENCY,
            move |c, tbl| {
                self.exec_and_validate_checksum(c, tbl, kv_client.as_ref(), concurrency)?;
                update_ch.inc();
                Ok(())
            },
            |_| {
                tidb_log::info("all checksum ended", &[]);
                Ok(())
            },
        );
    }

    /// Go `registerUpdateMetaAndLoadStats`: a table's backed-up statistics
    /// loaded (whole, or per partition from the stats files); without
    /// them, or when the load fails, its row counts from the files.
    fn register_update_meta_and_load_stats<'a>(
        &'a self,
        builder: &mut PipelineConcurrentBuilder<'a>,
        s: metautil::Storage,
        update_ch: Arc<dyn Progress>,
        stats_concurrency: usize,
    ) -> Result<()> {
        let dom = Arc::clone(self.dom()?);
        let buffer = Arc::new(StatsMetaItemBuffer::default());
        let (dom2, buffer2) = (Arc::clone(&dom), Arc::clone(&buffer));
        builder.register_pipeline_task(
            "Update Stats",
            stats_concurrency,
            move |c, tbl| {
                let old = tbl
                    .old_table
                    .as_ref()
                    .expect("a created table has its backup table");
                let new = tbl.table.as_ref().expect("a created table has its schema");
                let fields = |start: Instant| {
                    [
                        Field::new("table", Value::Str(old.info().name.to_string())),
                        Field::new("db", Value::Str(old.db.name.to_string())),
                        Field::new("cost", Value::Str(format!("{:?}", start.elapsed()))),
                    ]
                };
                let ids = [
                    Field::new("old id", Value::I64(old.info().id)),
                    Field::new("new id", Value::I64(new.id)),
                ];
                let mut stats_err = None;
                if let Some(stats) = &old.stats {
                    let mut target = stats.clone();
                    target.database_name = tbl.target_db_name().original().to_owned();
                    target.table_name = new.name.original().to_owned();
                    tidb_log::info("start loads analyze after validate checksum", &ids);
                    let start = Instant::now();
                    // Go skips updating the stats cache after the load.
                    if let Err(e) = dom.load_stats_from_json_no_update(c, &target) {
                        tidb_log::error(
                            "analyze table failed",
                            &[Field::new("error", Value::Str(e.to_string()))],
                        );
                        stats_err = Some(e);
                    }
                    tidb_log::info("restore stat done", &fields(start));
                } else if !old.stats_file_indexes.is_empty() {
                    tidb_log::info("start to load statistic data for each partition", &ids);
                    let start = Instant::now();
                    let rewrite_id_map = get_table_id_map(new, old.info());
                    if let Err(e) = metautil::statsfile::restore_stats(
                        c,
                        &s,
                        self.cipher.as_ref(),
                        dom.as_ref(),
                        new,
                        &old.stats_file_indexes,
                        &rewrite_id_map,
                    ) {
                        tidb_log::error(
                            "analyze table failed",
                            &[Field::new("error", Value::Str(e.to_string()))],
                        );
                        stats_err = Some(e);
                    }
                    tidb_log::info("restore statistic data done", &fields(start));
                }
                if stats_err.is_some() || (old.stats.is_none() && old.stats_file_indexes.is_empty())
                {
                    // A failed statistics load only falls back to the row
                    // counts.
                    tidb_log::info(
                        "start update metas",
                        &[
                            Field::new("table", Value::Str(old.info().name.to_string())),
                            Field::new("db", Value::Str(old.db.name.to_string())),
                        ],
                    );
                    update_stats_meta_for_table(c, &buffer, dom.as_ref(), tbl)?;
                }
                update_ch.inc();
                Ok(())
            },
            move |c| {
                buffer2
                    .update_metas_rest(c, dom2.as_ref())
                    .inspect_err(|e| {
                        tidb_log::error(
                            "update stats meta failed",
                            &[Field::new("error", Value::Str(e.to_string()))],
                        );
                    })?;
                tidb_log::info("all stats updated", &[]);
                Ok(())
            },
        );
        Ok(())
    }

    /// Go `registerWaitTiFlashReady`: each table with a TiFlash replica is
    /// polled every 5 seconds until its (partitions') progress is 1.
    fn register_wait_tiflash_ready<'a>(
        &'a self,
        builder: &mut PipelineConcurrentBuilder<'a>,
        update_ch: Arc<dyn Progress>,
    ) -> Result<()> {
        let dom = Arc::clone(self.dom()?);
        builder.register_pipeline_task(
            "Wait For Tiflash Ready",
            4,
            move |c, tbl| {
                let old = tbl.old_table.as_ref().expect("a created table has its backup table");
                let fields = || {
                    [
                        Field::new("table", Value::Str(old.info().name.to_string())),
                        Field::new("db", Value::Str(old.db.name.to_string())),
                    ]
                };
                let new = tbl.table.as_ref().expect("a created table has its schema");
                let Some(replica) = &new.tiflash_replica else {
                    tidb_log::info("table has no tiflash replica", &fields());
                    update_ch.inc();
                    return Ok(());
                };
                let count = replica.read().count;
                tidb_log::info("table has tiflash replica, start sync..", &fields());
                let partition_ids: Vec<i64> = new
                    .get_partition_info()
                    .map(|p| p.read().definitions.map_visible(|d| d.id))
                    .unwrap_or_default();
                loop {
                    let progress = if partition_ids.is_empty() {
                        match dom.tiflash_progress(c, new.id, count) {
                            Ok(p) => p,
                            Err(e) => {
                                tidb_log::warn(
                                    "failed to get progress for tiflash replica, retry it",
                                    &[Field::new("tableID", Value::I64(new.id)), Field::new("error", Value::Str(e.to_string()))],
                                );
                                if c.sleep(Duration::from_secs(1)) {
                                    return Err(ctx_error(c));
                                }
                                continue;
                            }
                        }
                    } else {
                        // Go skips a failed partition after a second and
                        // averages over all of them.
                        let mut sum = 0.0;
                        for &id in &partition_ids {
                            match dom.tiflash_progress(c, id, count) {
                                Ok(p) => sum += p,
                                Err(e) => {
                                    tidb_log::warn(
                                        "failed to get progress for tiflash partition replica, retry it",
                                        &[
                                            Field::new("tableID", Value::I64(new.id)),
                                            Field::new("partitionID", Value::I64(id)),
                                            Field::new("error", Value::Str(e.to_string())),
                                        ],
                                    );
                                    if c.sleep(Duration::from_secs(1)) {
                                        return Err(ctx_error(c));
                                    }
                                }
                            }
                        }
                        sum / partition_ids.len() as f64
                    };
                    #[allow(clippy::float_cmp)]
                    if progress == 1.0 {
                        tidb_log::info("tiflash replica synced", &fields());
                        break;
                    }
                    // TiFlash reports its progress every 2s.
                    if c.sleep(Duration::from_secs(5)) {
                        return Err(ctx_error(c));
                    }
                }
                update_ch.inc();
                Ok(())
            },
            |_| {
                tidb_log::info("all tiflash replica synced", &[]);
                Ok(())
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
