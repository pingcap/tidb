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

//! Go `preSplitAndScatter` (`pkg/ddl/executor.go`) and the region splits it
//! runs (`pkg/ddl/split_region.go`): after a CREATE TABLE commits, a node
//! with `split-table` on cuts the new table's regions -- its record range
//! (and each partition's), and with `SHARD_ROW_ID_BITS` / `AUTO_RANDOM` plus
//! `PRE_SPLIT_REGIONS` the shard ranges and index ranges too.
//!
//! Failures are logged and swallowed, as Go's are: TiKV splits the table on
//! its own later.

use tidb_codec::table_key::{
    encode_row_key_with_handle, encode_table_index_prefix, gen_table_prefix, RecordHandle,
};
use tidb_executor::region_map::RegionStore;
use tidb_log::{Field, Value};
use tidb_model::TableInfo;

/// Go `GlobalScatterGroupID`.
const GLOBAL_SCATTER_GROUP_ID: i64 = -1;
/// Go `autoid.RowIDBitLength`.
const ROW_ID_BIT_LENGTH: u64 = 64;

/// Go `getScatterConfig`: whether to scatter, and the scatter group.
fn scatter_config(scope: &str, table_id: i64) -> (bool, i64) {
    match scope {
        "table" => (true, table_id),
        "global" => (true, GLOBAL_SCATTER_GROUP_ID),
        _ => (false, table_id),
    }
}

/// Go `shardingBits`.
fn sharding_bits(table: &TableInfo) -> u64 {
    if table.shard_row_id_bits > 0 {
        table.shard_row_id_bits
    } else {
        table.auto_random_bits
    }
}

/// Go's "pre split some table (index) regions failed" lines.
fn log_pre_split_failure(msg: &str, table: &TableInfo, region_count: usize, error: &str) {
    tidb_ddl_logutil::ddl_logger().warn(
        msg,
        &[
            Field::new("table", Value::Str(table.name.original().to_owned())),
            Field::new("successful region count", Value::I64(region_count as i64)),
            zap_error(error),
        ],
    );
}

/// zap `zap.Error(err)`.
fn zap_error(error: &str) -> Field {
    Field::new("error", Value::Str(error.to_owned()))
}

/// Go `preSplitAndScatter` for a table just created. `scatter_scope` is
/// the session's `tidb_scatter_region`.
pub fn pre_split_and_scatter(store: &dyn RegionStore, table: &TableInfo, scatter_scope: &str) {
    if table.temp_table_type != tidb_model::TempTableType::NONE {
        return;
    }
    let partitions: Vec<i64> = table
        .get_partition_info()
        .map(|info| {
            info.read()
                .definitions
                .snapshot()
                .iter()
                .map(|definition| definition.id)
                .collect()
        })
        .unwrap_or_default();
    // Go runs the split in the background when nothing is scattered; the
    // regions it leaves are the same.
    let region_ids = if partitions.is_empty() {
        split_table_region(store, table, scatter_scope)
    } else {
        split_partition_table_region(store, table, &partitions, scatter_scope)
    };
    if !scatter_scope.is_empty() {
        wait_scatter_region_finish(store, &region_ids);
    }
}

/// Go `splitTableRegion` without split policies.
fn split_table_region(store: &dyn RegionStore, table: &TableInfo, scope: &str) -> Vec<u64> {
    if sharding_bits(table) > 0 && table.pre_split_regions > 0 {
        pre_split_physical_table_by_shard_row_id(store, table, table.id, scope)
    } else {
        vec![split_record_region(store, table, table.id, scope)]
    }
}

/// Go `splitPartitionTableRegion` without split policies.
fn split_partition_table_region(
    store: &dyn RegionStore,
    table: &TableInfo,
    partitions: &[i64],
    scope: &str,
) -> Vec<u64> {
    let mut region_ids = Vec::new();
    if sharding_bits(table) > 0 && table.pre_split_regions > 0 {
        let (scatter, table_id) = scatter_config(scope, table.id);
        // The global indexes' regions first.
        region_ids.extend(split_index_region(store, table, scatter, table_id));
        for &partition in partitions {
            region_ids.extend(pre_split_physical_table_by_shard_row_id(
                store, table, partition, scope,
            ));
        }
    } else {
        for &partition in partitions {
            region_ids.push(split_record_region(store, table, partition, scope));
        }
    }
    region_ids
}

/// Go `preSplitPhysicalTableByShardRowID`: `2^PRE_SPLIT_REGIONS` record
/// regions over the shard bits, then the index regions.
fn pre_split_physical_table_by_shard_row_id(
    store: &dyn RegionStore,
    table: &TableInfo,
    physical_id: i64,
    scope: &str,
) -> Vec<u64> {
    // Go `autoid.NewShardIDFormat` over the primary key's type (or BIGINT).
    let unsigned = table.get_pk_col_info().is_some_and(|column| {
        column.read().get_flag() & u64::from(crate::result_metadata::UNSIGNED_FLAG) != 0
    });
    let shard_bits = sharding_bits(table);
    let mut incremental_bits = if table.auto_random_range_bits == 0 {
        ROW_ID_BIT_LENGTH - shard_bits
    } else {
        table.auto_random_range_bits - shard_bits
    };
    if !unsigned {
        incremental_bits -= 1;
    }
    let step = 1_i64 << (shard_bits - table.pre_split_regions);
    let max = 1_i64 << shard_bits;
    let mut keys = vec![gen_table_prefix(physical_id)];
    let mut shard = step;
    while shard < max {
        let record_id = shard << incremental_bits;
        keys.push(encode_row_key_with_handle(
            physical_id,
            &RecordHandle::Int(record_id),
        ));
        shard += step;
    }
    let (scatter, table_id) = scatter_config(scope, table.id);
    let (mut region_ids, error) = store.split_regions(keys, scatter, Some(table_id));
    if let Some(error) = error {
        log_pre_split_failure(
            "pre split some table regions failed",
            table,
            region_ids.len(),
            &error,
        );
    }
    region_ids.extend(split_index_region(store, table, scatter, physical_id));
    region_ids
}

/// Go `SplitRecordRegion`: one split at the physical table's prefix; the
/// region id when exactly one was created, else 0.
fn split_record_region(
    store: &dyn RegionStore,
    table: &TableInfo,
    physical_id: i64,
    scope: &str,
) -> u64 {
    let (scatter, table_id) = scatter_config(scope, table.id);
    let (region_ids, error) =
        store.split_regions(vec![gen_table_prefix(physical_id)], scatter, Some(table_id));
    if let Some(error) = error {
        // It will be automatically split by TiKV later.
        tidb_ddl_logutil::ddl_logger().warn("split table region failed", &[zap_error(&error)]);
    }
    match region_ids.as_slice() {
        [region_id] => *region_id,
        _ => 0,
    }
}

/// Go `splitIndexRegion`: one split past each index's range (at a global
/// index's own prefix), for the indexes stored under `physical_id`.
fn split_index_region(
    store: &dyn RegionStore,
    table: &TableInfo,
    scatter: bool,
    physical_id: i64,
) -> Vec<u64> {
    let partitioned = table.get_partition_info().is_some();
    let mut keys = Vec::new();
    for index in table.indices.iter_deref() {
        let index = index.read();
        if partitioned
            && ((index.global && table.id != physical_id)
                || (!index.global && table.id == physical_id))
        {
            continue;
        }
        let id = if index.global { index.id } else { index.id + 1 };
        keys.push(encode_table_index_prefix(physical_id, id));
    }
    let (region_ids, error) = store.split_regions(keys, scatter, Some(physical_id));
    if let Some(error) = error {
        log_pre_split_failure(
            "pre split some table index regions failed",
            table,
            region_ids.len(),
            &error,
        );
    }
    region_ids
}

/// Go `WaitScatterRegionFinish`: waits for each region in turn, giving up at
/// the first failure that is not PD's.
fn wait_scatter_region_finish(store: &dyn RegionStore, region_ids: &[u64]) {
    for &region_id in region_ids {
        if let Err(error) = store.wait_scatter_region_finish(region_id, 0) {
            tidb_ddl_logutil::ddl_logger().warn(
                "wait scatter region failed",
                &[
                    Field::new("regionID", Value::U64(region_id)),
                    zap_error(&error),
                ],
            );
            break;
        }
    }
}

/// Go's bootstrap splits on a store with `split-table` on: `InitDDLTables`'
/// `createAndSplitTables` cuts each DDL table version group's prefixes in
/// one split (no scatter, the global scatter group), then every `mysql.*`
/// table's `CREATE TABLE` pre-splits its record range
/// (`preSplitAndScatterTable` with no scatter). `previous_ddl_table_version`
/// is the store's DDL table version before this bootstrap.
pub fn split_bootstrap_tables(store: &dyn RegionStore, previous_ddl_table_version: i64) {
    for group in tidb_metadef::DDL_TABLE_VERSION_TABLES
        .iter()
        .filter(|group| previous_ddl_table_version < group.version)
    {
        let keys = group
            .tables
            .iter()
            .map(|table| gen_table_prefix(table.id))
            .collect();
        let (_, error) = store.split_regions(keys, false, Some(GLOBAL_SCATTER_GROUP_ID));
        // Go `splitAndScatterTable`.
        if let Some(error) = error {
            tidb_util::logutil::bg_logger().warn("split table region failed", &[zap_error(&error)]);
        }
    }
    for table in tidb_metadef::BOOTSTRAP_TABLES {
        let (_, error) =
            store.split_regions(vec![gen_table_prefix(table.id)], false, Some(table.id));
        // Go's `CREATE TABLE` runs `SplitRecordRegion`.
        if let Some(error) = error {
            tidb_ddl_logutil::ddl_logger().warn("split table region failed", &[zap_error(&error)]);
        }
    }
}
