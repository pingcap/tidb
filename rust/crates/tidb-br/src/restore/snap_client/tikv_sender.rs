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

//! Go `br/pkg/restore/snap_client/tikv_sender.go`: the restored tables'
//! files merged into split keys and file batches, the regions split and
//! scattered, then the batches imported.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::File;
use tidb_proto::{import_sstpb, metapb};

use super::client::SnapClient;
use super::import::restore_err;
use super::placement_rule_manager::new_placement_rule_manager;
use crate::conn::{get_all_tikv_stores_with_retry, StoreBehavior};
use crate::errors::{self, Result};
use crate::glue::{self, Glue};
use crate::metautil;
use crate::restore::import_client::ImporterClient;
use crate::restore::restorer::{get_file_range_key, BackupFileSet, BatchBackupFileSet};
use crate::restore::split::RegionSplitter;
use crate::restore_utils::{
    get_partition_id_map, merge_and_rewrite_file_ranges, validate_file_rewrite_rule, CreatedTable,
    RewriteRules,
};
use crate::summary;
use crate::utils::worker_pool::{ErrGroup, WorkerPool};

/// Go `PhysicalTable`.
#[derive(Clone, Debug, Default)]
pub struct PhysicalTable {
    /// Go `NewPhysicalID`.
    pub new_physical_id: i64,
    /// Go `OldPhysicalID`.
    pub old_physical_id: i64,
    /// Go `RewriteRules`.
    pub rewrite_rules: Option<Arc<RewriteRules>>,
    /// Go `Files`.
    pub files: Vec<Arc<File>>,
}

/// Go `getSortedPhysicalTables`: each table and partition with its files,
/// by new physical ID.
fn get_sorted_physical_tables(
    created_tables: &[CreatedTable<metautil::Table>],
) -> Vec<PhysicalTable> {
    let mut physical_tables = Vec::with_capacity(created_tables.len());
    for created in created_tables {
        let (Some(table), Some(old)) = (&created.table, &created.old_table) else {
            continue;
        };
        let Some(old_info) = &old.info else { continue };
        let rules = created.rewrite_rule.clone().map(Arc::new);
        let files_of = |id: i64| -> Vec<Arc<File>> {
            old.files_of_physicals
                .get(&id)
                .map(|fs| fs.iter().cloned().map(Arc::new).collect())
                .unwrap_or_default()
        };
        physical_tables.push(PhysicalTable {
            new_physical_id: table.id,
            old_physical_id: old_info.id,
            rewrite_rules: rules.clone(),
            files: files_of(old_info.id),
        });
        for (old_id, new_id) in get_partition_id_map(table, old_info) {
            physical_tables.push(PhysicalTable {
                new_physical_id: new_id,
                old_physical_id: old_id,
                rewrite_rules: rules.clone(),
                files: files_of(old_id),
            });
        }
    }
    physical_tables.sort_by_key(|t| t.new_physical_id);
    physical_tables
}

/// Go `GetSortedPhysicalTables` (`export_test.go`).
#[cfg(test)]
pub(crate) fn get_sorted_physical_tables_for_test(
    created_tables: &[CreatedTable<metautil::Table>],
) -> Vec<PhysicalTable> {
    get_sorted_physical_tables(created_tables)
}

/// Go `filterOutFiles`: the files whose range key is not checkpointed; the
/// skipped ones are counted into the summary.
fn filter_out_files(
    checkpoint_set: Option<&HashSet<String>>,
    files: &[Arc<File>],
) -> Vec<Arc<File>> {
    let (mut progress, mut total_kvs, mut total_bytes) = (0, 0_u64, 0_u64);
    let mut new_files = Vec::with_capacity(files.len());
    for file in files {
        if checkpoint_set.is_some_and(|set| set.contains(get_file_range_key(&file.name))) {
            progress += 1;
            total_kvs += file.total_kvs;
            total_bytes += file.total_bytes;
        } else {
            new_files.push(Arc::clone(file));
        }
    }
    if progress > 0 {
        use summary::SuccessArg::Data;
        summary::collect_success_unit(summary::TOTAL_KV, 1, Data(total_kvs));
        summary::collect_success_unit(summary::SKIPPED_KV_COUNT_BY_CHECKPOINT, 1, Data(total_kvs));
        summary::collect_success_unit(summary::TOTAL_BYTES, 1, Data(total_bytes));
        summary::collect_success_unit(summary::SKIPPED_BYTES_BY_CHECKPOINT, 1, Data(total_bytes));
    }
    new_files
}

/// Go `MergedRangeCountThreshold`.
pub const MERGED_RANGE_COUNT_THRESHOLD: usize = 1536;

/// Go `SortAndValidateFileRanges`: the split keys and the file batches of
/// the tables, a batch closing when it would pass the split size, the
/// split key count or [`MERGED_RANGE_COUNT_THRESHOLD`] files (and at each
/// table with `split_on_table`).
///
/// # Errors
/// A file outside its table's rewrite rules, or a range merge error.
pub fn sort_and_validate_file_ranges(
    created_tables: &[CreatedTable<metautil::Table>],
    checkpoint_set_with_table_id: &HashMap<i64, HashSet<String>>,
    split_size_bytes: u64,
    split_key_count: u64,
    split_on_table: bool,
) -> Result<(Vec<Vec<u8>>, Vec<BatchBackupFileSet>)> {
    let sorted_physical_tables = get_sorted_physical_tables(created_tables);
    let mut sorted_split_keys: Vec<Vec<u8>> = Vec::new();
    let (mut group_size, mut group_count) = (0_u64, 0_u64);
    let mut last_key: Option<Vec<u8>> = None;
    let mut table_id_with_files_group: Vec<BatchBackupFileSet> = Vec::new();
    let mut last_files_group: Option<BatchBackupFileSet> = None;
    let (mut merged_range_count, mut total_write_cf_file, mut total_default_cf_file) =
        (0_usize, 0_usize, 0_usize);
    tidb_log::info(
        "start to merge ranges",
        &[
            Field::new("kv size threshold", Value::U64(split_size_bytes)),
            Field::new("kv count threshold", Value::U64(split_key_count)),
        ],
    );
    let merged_fields = |size: u64, count: u64, ranges: usize| {
        [
            Field::new("merged kv size", Value::U64(size)),
            Field::new("merged kv count", Value::U64(count)),
            Field::new("merged range count", Value::I64(ranges as i64)),
        ]
    };
    for table in &sorted_physical_tables {
        let rules = table.rewrite_rules.as_deref();
        for file in &table.files {
            validate_file_rewrite_rule(file, rules).map_err(restore_err)?;
        }
        let (sorted_ranges, stat) =
            merge_and_rewrite_file_ranges(&table.files, rules, split_size_bytes, split_key_count)
                .map_err(restore_err)?;
        total_default_cf_file += stat.total_default_cf_file;
        total_write_cf_file += stat.total_write_cf_file;
        tidb_log::info(
            "merge and validate file",
            &[
                Field::new("new physical ID", Value::I64(table.new_physical_id)),
                Field::new("old physical ID", Value::I64(table.old_physical_id)),
                Field::new("Files(total)", Value::I64(stat.total_files as i64)),
                Field::new("File(write)", Value::I64(stat.total_write_cf_file as i64)),
                Field::new(
                    "File(default)",
                    Value::I64(stat.total_default_cf_file as i64),
                ),
                Field::new("Region(total)", Value::I64(stat.total_regions as i64)),
                Field::new("Regoin(keys avg)", Value::I64(stat.region_keys_avg as i64)),
                Field::new(
                    "Region(bytes avg)",
                    Value::I64(stat.region_bytes_avg as i64),
                ),
                Field::new("Merged(regions)", Value::I64(stat.merged_regions as i64)),
                Field::new(
                    "Merged(keys avg)",
                    Value::I64(stat.merged_region_keys_avg as i64),
                ),
                Field::new(
                    "Merged(bytes avg)",
                    Value::I64(stat.merged_region_bytes_avg as i64),
                ),
            ],
        );
        let checkpoint_set = checkpoint_set_with_table_id.get(&table.new_physical_id);
        for rg in &sorted_ranges {
            let after_size = group_size + rg.size;
            let after_count = group_count + rg.count;
            if after_size > split_size_bytes
                || after_count > split_key_count
                || merged_range_count > MERGED_RANGE_COUNT_THRESHOLD
            {
                tidb_log::info(
                    "merge ranges across tables due to kv size/count or merged count threshold exceeded",
                    &merged_fields(group_size, group_count, merged_range_count),
                );
                (group_size, group_count) = (rg.size, rg.count);
                merged_range_count = 0;
                if let Some(k) = last_key.take() {
                    sorted_split_keys.push(k);
                }
                if let Some(g) = last_files_group.take() {
                    table_id_with_files_group.push(g);
                }
            } else {
                (group_size, group_count) = (after_size, after_count);
            }
            last_key = Some(rg.end_key().to_vec());
            merged_range_count += rg.range.files.len();
            let new_files = filter_out_files(checkpoint_set, &rg.range.files);
            if !new_files.is_empty() {
                let group = last_files_group.get_or_insert_with(Vec::new);
                if group
                    .last()
                    .is_none_or(|g| g.table_id != table.new_physical_id)
                {
                    group.push(BackupFileSet {
                        table_id: table.new_physical_id,
                        sst_files: Vec::new(),
                        rewrite_rules: table.rewrite_rules.clone(),
                    });
                }
                group.last_mut().unwrap().sst_files.extend(new_files);
            }
        }
        if split_on_table {
            tidb_log::info(
                "merge ranges across tables due to split on table",
                &merged_fields(group_size, group_count, merged_range_count),
            );
            (group_size, group_count) = (0, 0);
            merged_range_count = 0;
            last_key = None;
            if let Some(g) = last_files_group.take() {
                table_id_with_files_group.push(g);
            }
        }
    }
    if let Some(k) = last_key {
        sorted_split_keys.push(k);
    }
    if let Some(g) = last_files_group {
        tidb_log::info(
            "merge ranges across tables due to the last group",
            &merged_fields(group_size, group_count, merged_range_count),
        );
        table_id_with_files_group.push(g);
    }
    summary::collect_int("default CF files", total_default_cf_file as i64);
    summary::collect_int("write CF files", total_write_cf_file as i64);
    tidb_log::info(
        "range and file prepared",
        &[
            Field::new(
                "default file count",
                Value::I64(total_default_cf_file as i64),
            ),
            Field::new("write file count", Value::I64(total_write_cf_file as i64)),
        ],
    );
    Ok((sorted_split_keys, table_id_with_files_group))
}

/// Go `RestoreTablesContext`.
pub struct RestoreTablesContext<'a> {
    /// Go `LogProgress`.
    pub log_progress: bool,
    /// Go `SplitSizeBytes`.
    pub split_size_bytes: u64,
    /// Go `SplitKeyCount`.
    pub split_key_count: u64,
    /// Go `SplitOnTable`.
    pub split_on_table: bool,
    /// Go `Online`.
    pub online: bool,
    /// Go `CreatedTables`.
    pub created_tables: &'a [CreatedTable<metautil::Table>],
    /// Go `CheckpointSetWithTableID`.
    pub checkpoint_set_with_table_id: &'a HashMap<i64, HashSet<String>>,
    /// Go `CompactProtectStartKey`.
    pub compact_protect_start_key: Vec<u8>,
    /// Go `CompactProtectEndKey`.
    pub compact_protect_end_key: Vec<u8>,
    /// Go `Glue`.
    pub glue: &'a dyn Glue,
}

impl SnapClient {
    /// Go `RestoreTables`: placement rules for an online restore, the ranges
    /// merged, the regions split and scattered, then the files imported,
    /// with TiKV's compaction kept off the restored range meanwhile.
    ///
    /// # Errors
    /// The first stage's error.
    pub fn restore_tables(&self, ctx: &Context, rt: &RestoreTablesContext<'_>) -> Result<()> {
        let mut placement_rule_manager = new_placement_rule_manager(
            ctx,
            Arc::clone(&self.pd_client),
            self.pd_http_client.clone(),
            self.security.clone(),
            Arc::clone(&self.runtime),
            rt.online,
        )?;
        placement_rule_manager.set_placement_rule(ctx, rt.created_tables)?;
        let res = self.restore_tables_with_rules(ctx, rt);
        if let Err(err) = placement_rule_manager.reset_placement_rules(ctx) {
            tidb_log::warn(
                "failed to reset placement rules",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        }
        res
    }

    fn restore_tables_with_rules(
        &self,
        ctx: &Context,
        rt: &RestoreTablesContext<'_>,
    ) -> Result<()> {
        let start = Instant::now();
        let (sorted_split_keys, table_id_with_files_group) = sort_and_validate_file_ranges(
            rt.created_tables,
            rt.checkpoint_set_with_table_id,
            rt.split_size_bytes,
            rt.split_key_count,
            rt.split_on_table,
        )?;
        let elapsed = start.elapsed();
        summary::collect_duration("merge ranges", elapsed);
        tidb_log::info(
            "Restore Stage Duration",
            &[
                Field::new("stage", Value::Str("merge ranges".into())),
                Field::new("take", Value::Duration(elapsed.as_nanos() as i64)),
            ],
        );
        let (protect_start, protect_end) =
            (&rt.compact_protect_start_key, &rt.compact_protect_end_key);
        let skip_protect = |op: &str| {
            if protect_start >= protect_end {
                tidb_log::warn(
                    &format!("start key must be smaller than end key, so skip sending {op} partition range request"),
                    &[
                        Field::new("start key", Value::Str(tidb_redact::key_owned(protect_start))),
                        Field::new("end key", Value::Str(tidb_redact::key_owned(protect_end))),
                    ],
                );
                true
            } else {
                false
            }
        };
        glue::with_progress(
            ctx,
            rt.glue,
            "Split&Scatter Regions",
            sorted_split_keys.len() as i64,
            !rt.log_progress,
            |update| {
                let update = Arc::clone(update);
                self.split_points(ctx, &sorted_split_keys, Arc::new(move |n| update.inc_by(n)))?;
                if skip_protect("add") {
                    return Ok(());
                }
                tidb_log::info("start to check and compact the restore range", &[]);
                self.compact_and_check_sst_range(ctx, protect_start, protect_end)
            },
        )?;
        glue::with_progress(
            ctx,
            rt.glue,
            "Download&Ingest SST",
            table_id_with_files_group.len() as i64,
            !rt.log_progress,
            |update| {
                let update = Arc::clone(update);
                self.restore_sst_files(
                    ctx,
                    table_id_with_files_group,
                    Arc::new(move |n| update.inc_by(n)),
                )?;
                if skip_protect("remove") {
                    return Ok(());
                }
                tidb_log::info("start to remove the force partition restore range", &[]);
                self.remove_force_partition_range(ctx, protect_start, protect_end)
            },
        )
    }

    /// Go `SplitPoints` (`isRawKv` false): the regions split at the keys,
    /// each split counted into `on_progress`.
    ///
    /// # Errors
    /// The split's error.
    pub fn split_points(
        &self,
        ctx: &Context,
        sorted_split_keys: &[Vec<u8>],
        on_progress: Arc<dyn Fn(i64) + Send + Sync>,
    ) -> Result<()> {
        let start = Instant::now();
        let on_split: crate::restore::split::OnSplit =
            Box::new(move |keys| on_progress(keys.len() as i64));
        let mut splitter = RegionSplitter::new(
            self.new_split_client(self.store_count + 1, Some(on_split)),
            self.get_split_region_index_step(),
        );
        splitter.set_coarse_scatter(self.get_coarse_scatter());
        splitter.execute_sorted_keys(ctx, sorted_split_keys)?;
        let elapsed = start.elapsed();
        tidb_log::info(
            "Restore Stage Duration",
            &[
                Field::new("stage", Value::Str("split regions".into())),
                Field::new("take", Value::Duration(elapsed.as_nanos() as i64)),
            ],
        );
        summary::collect_duration("split regions", elapsed);
        summary::collect_int("split keys", sorted_split_keys.len() as i64);
        Ok(())
    }

    /// Go `sendRequestToStore`: `send_fn` on every up TiKV store with a
    /// status address.
    fn send_request_to_store(
        &self,
        ctx: &Context,
        send_fn: impl Fn(&Context, &dyn ImporterClient, u64) -> Result<()> + Send + Sync + 'static,
    ) -> Result<()> {
        let stores = get_all_tikv_stores_with_retry(
            ctx,
            self.pd_client.as_ref(),
            StoreBehavior::SkipTiFlash,
        )?;
        let (eg, ectx) = ErrGroup::with_context(ctx);
        let pool = WorkerPool::new(stores.len(), "check and compact");
        let client = Arc::clone(
            self.importer
                .as_ref()
                .expect("initClients sets the importer")
                .import_client(),
        );
        let send_fn = Arc::new(send_fn);
        for store in stores {
            if store.status_address.is_empty() || store.state != metapb::StoreState::Up as i32 {
                continue;
            }
            let (ectx, client, send_fn) = (ectx.clone(), Arc::clone(&client), Arc::clone(&send_fn));
            pool.apply_on_error_group(&eg, move || send_fn(&ectx, client.as_ref(), store.id));
        }
        eg.wait()
    }

    /// Go `compactAndCheckSSTRange`: TiKV keeps the range in its own
    /// compaction partition for 2 hours.
    fn compact_and_check_sst_range(
        &self,
        ctx: &Context,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        let req = import_sstpb::AddPartitionRangeRequest {
            range: Some(import_sstpb::Range {
                start: start_key.to_vec(),
                end: end_key.to_vec(),
            }),
            ttl_seconds: 7200,
        };
        self.send_request_to_store(ctx, move |ectx, client, store_id| {
            match client.add_force_partition_range(ectx, store_id, req.clone()) {
                Err(err) if errors::grpc_code(err.as_ref()) == tonic::Code::Unimplemented => {
                    tidb_log::warn(
                        "tikv node doesn't support check and compact.",
                        &[Field::new("store id", Value::U64(store_id))],
                    );
                    Ok(())
                }
                other => other,
            }
        })
    }

    /// Go `removeForcePartitionRange`.
    fn remove_force_partition_range(
        &self,
        ctx: &Context,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        let req = import_sstpb::RemovePartitionRangeRequest {
            range: Some(import_sstpb::Range {
                start: start_key.to_vec(),
                end: end_key.to_vec(),
            }),
        };
        self.send_request_to_store(ctx, move |ectx, client, store_id| {
            match client.remove_force_partition_range(ectx, store_id, req.clone()) {
                Err(err) if errors::grpc_code(err.as_ref()) == tonic::Code::Unimplemented => {
                    tidb_log::warn(
                        "tikv node doesn't support remove force partition range.",
                        &[Field::new("store id", Value::U64(store_id))],
                    );
                    Ok(())
                }
                other => other,
            }
        })
    }

    /// Go `RestoreSSTFiles`.
    ///
    /// # Errors
    /// The first import error.
    pub fn restore_sst_files(
        &self,
        ctx: &Context,
        mut table_id_with_files_group: Vec<BatchBackupFileSet>,
        on_progress: Arc<dyn Fn(i64) + Send + Sync>,
    ) -> Result<()> {
        let mut forced_err = None;
        if let Some(v) =
            tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files")
        {
            match v.string().as_str() {
                "corrupt-last-table-files" => {
                    table_id_with_files_group.pop();
                    forced_err = Some(errors::errorf("skip the last table files"));
                }
                "only-last-table-files" => {
                    let n = table_id_with_files_group.len().saturating_sub(1);
                    for sets in &table_id_with_files_group[..n] {
                        assert!(
                            sets.iter().all(|s| s.sst_files.is_empty()),
                            "has files but not the last table files"
                        );
                    }
                }
                _ => {}
            }
        }
        let r = self.get_restorer(ctx, self.checkpoint_runner.clone());
        let res = r
            .go_restore(on_progress, table_id_with_files_group)
            .and_then(|()| r.wait_until_finish());
        match forced_err {
            Some(err) => Err(err),
            None => res,
        }
    }
}
