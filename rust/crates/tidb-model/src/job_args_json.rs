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

//! Ordered `encoding/json` receiver codecs for native `JobArgs` values.

use crate::go_runtime::GoSliceElementLayout;
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
    NullNoopSeed, OptionSharedAtomicReplaceSeed, OptionSharedMergeSeed, SharedPointerSliceSeed,
};
use crate::serde_shared_slices::{SharedObjectSliceSeed, SharedScalarSliceSeed};
use crate::{
    AlterIndexVisibilityArgs, AlterTableModeArgs, AlterTablePartitionArgs, BatchCreateTableArgs,
    CreateSchemaArgs, CreateTableArgs, DropForeignKeyArgs, DropSchemaArgs, EmptyArgs,
    ExchangeTablePartitionArgs, ModifySchemaArgs, ModifyTableAutoIDCacheArgs,
    ModifyTableCharsetAndCollateArgs, ModifyTableCommentArgs, ModifyTableEngineAttributeArgs,
    RebaseAutoIDArgs, RefreshMetaArgs, SetDefaultValueArgs, ShardRowIDArgs, TableIDIndexID,
    TablePartitionArgs, TruncateTableArgs,
};

impl_go_json_merge_object!(EmptyArgs, _destination, map, _key, {
    ignore_unknown(&mut map)?;
});
impl_go_json_deserialize!(EmptyArgs);

impl_go_json_merge_object!(CreateSchemaArgs, destination, map, key, {
    if go_json_field_matches(&key, "db_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut *destination.db_info.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(CreateSchemaArgs);

impl_go_json_merge_object!(DropSchemaArgs, destination, map, key, {
    if go_json_field_matches(&key, "fk_check") {
        map.next_value_seed(NullNoopSeed(&mut *destination.fk_check.write()))?;
    } else if go_json_field_matches(&key, "all_dropped_table_ids") {
        map.next_value_seed(SharedScalarSliceSeed::new(
            &mut *destination.all_dropped_table_ids.write(),
            8,
            GoSliceElementLayout::NoPointers,
        ))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(DropSchemaArgs);

impl_go_json_merge_object!(ModifySchemaArgs, destination, map, key, {
    if go_json_field_matches(&key, "to_charset") {
        map.next_value_seed(NullNoopSeed(&mut *destination.to_charset.write()))?;
    } else if go_json_field_matches(&key, "to_collate") {
        map.next_value_seed(NullNoopSeed(&mut *destination.to_collate.write()))?;
    } else if go_json_field_matches(&key, "policy_ref") {
        map.next_value_seed(OptionSharedMergeSeed(&mut *destination.policy_ref.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(ModifySchemaArgs);

impl_go_json_merge_object!(CreateTableArgs, destination, map, key, {
    if go_json_field_matches(&key, "table_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut *destination.table_info.write()))?;
    } else if go_json_field_matches(&key, "on_exist_replace") {
        map.next_value_seed(NullNoopSeed(&mut *destination.on_exist_replace.write()))?;
    } else if go_json_field_matches(&key, "old_view_tbl_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.old_view_table_id.write()))?;
    } else if go_json_field_matches(&key, "fk_check") {
        map.next_value_seed(NullNoopSeed(&mut *destination.fk_check.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(CreateTableArgs);

impl_go_json_merge_object!(BatchCreateTableArgs, destination, map, key, {
    if go_json_field_matches(&key, "tables") {
        map.next_value_seed(SharedPointerSliceSeed(&mut *destination.tables.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(BatchCreateTableArgs);

impl_go_json_merge_object!(TruncateTableArgs, destination, map, key, {
    if go_json_field_matches(&key, "fk_check") {
        map.next_value_seed(NullNoopSeed(&mut *destination.fk_check.write()))?;
    } else if go_json_field_matches(&key, "new_table_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.new_table_id.write()))?;
    } else if go_json_field_matches(&key, "new_partition_ids") {
        map.next_value_seed(SharedScalarSliceSeed::new(
            &mut *destination.new_partition_ids.write(),
            8,
            GoSliceElementLayout::NoPointers,
        ))?;
    } else if go_json_field_matches(&key, "old_partition_ids") {
        map.next_value_seed(SharedScalarSliceSeed::new(
            &mut *destination.old_partition_ids.write(),
            8,
            GoSliceElementLayout::NoPointers,
        ))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(TruncateTableArgs);

impl_go_json_merge_object!(TableIDIndexID, destination, map, key, {
    if go_json_field_matches(&key, "TableID") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_id))?;
    } else if go_json_field_matches(&key, "IndexID") {
        map.next_value_seed(NullNoopSeed(&mut destination.index_id))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(TableIDIndexID);

impl_go_json_merge_object!(TablePartitionArgs, destination, map, key, {
    if go_json_field_matches(&key, "part_names") {
        map.next_value_seed(SharedScalarSliceSeed::new(
            &mut *destination.part_names.write(),
            16,
            GoSliceElementLayout::PointerBearing,
        ))?;
    } else if go_json_field_matches(&key, "part_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut *destination.part_info.write()))?;
    } else if go_json_field_matches(&key, "old_physical_tbl_ids") {
        map.next_value_seed(SharedScalarSliceSeed::new(
            &mut *destination.old_physical_table_ids.write(),
            8,
            GoSliceElementLayout::NoPointers,
        ))?;
    } else if go_json_field_matches(&key, "old_global_indexes") {
        map.next_value_seed(SharedObjectSliceSeed::new(
            &mut *destination.old_global_indexes.write(),
            16,
            GoSliceElementLayout::NoPointers,
        ))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(TablePartitionArgs);

impl_go_json_merge_object!(ExchangeTablePartitionArgs, destination, map, key, {
    if go_json_field_matches(&key, "partition_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.partition_id.write()))?;
    } else if go_json_field_matches(&key, "pt_schema_id") {
        map.next_value_seed(NullNoopSeed(
            &mut *destination.partitioned_table_schema_id.write(),
        ))?;
    } else if go_json_field_matches(&key, "pt_table_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.partitioned_table_id.write()))?;
    } else if go_json_field_matches(&key, "partition_name") {
        map.next_value_seed(NullNoopSeed(&mut *destination.partition_name.write()))?;
    } else if go_json_field_matches(&key, "with_validation") {
        map.next_value_seed(NullNoopSeed(&mut *destination.with_validation.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(ExchangeTablePartitionArgs);

impl_go_json_merge_object!(AlterTablePartitionArgs, destination, map, key, {
    if go_json_field_matches(&key, "partition_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.partition_id.write()))?;
    } else if go_json_field_matches(&key, "label_rule") {
        map.next_value_seed(OptionSharedAtomicReplaceSeed::new(
            &mut *destination.label_rule.write(),
            serde_json::Value::default,
        ))?;
    } else if go_json_field_matches(&key, "policy_ref_info") {
        map.next_value_seed(OptionSharedMergeSeed(
            &mut *destination.policy_ref_info.write(),
        ))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(AlterTablePartitionArgs);

impl_go_json_merge_object!(RebaseAutoIDArgs, destination, map, key, {
    if go_json_field_matches(&key, "new_base") {
        map.next_value_seed(NullNoopSeed(&mut *destination.new_base.write()))?;
    } else if go_json_field_matches(&key, "force") {
        map.next_value_seed(NullNoopSeed(&mut *destination.force.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(RebaseAutoIDArgs);

impl_go_json_merge_object!(ModifyTableCommentArgs, destination, map, key, {
    if go_json_field_matches(&key, "comment") {
        map.next_value_seed(NullNoopSeed(&mut *destination.comment.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(ModifyTableCommentArgs);

impl_go_json_merge_object!(ModifyTableCharsetAndCollateArgs, destination, map, key, {
    if go_json_field_matches(&key, "to_charset") {
        map.next_value_seed(NullNoopSeed(&mut *destination.to_charset.write()))?;
    } else if go_json_field_matches(&key, "to_collate") {
        map.next_value_seed(NullNoopSeed(&mut *destination.to_collate.write()))?;
    } else if go_json_field_matches(&key, "needs_overwrite_cols") {
        map.next_value_seed(NullNoopSeed(
            &mut *destination.needs_overwrite_columns.write(),
        ))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(ModifyTableCharsetAndCollateArgs);

impl_go_json_merge_object!(AlterIndexVisibilityArgs, destination, map, key, {
    if go_json_field_matches(&key, "index_name") {
        map.next_value_seed(NullNoopSeed(&mut *destination.index_name.write()))?;
    } else if go_json_field_matches(&key, "invisible") {
        map.next_value_seed(NullNoopSeed(&mut *destination.invisible.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(AlterIndexVisibilityArgs);

impl_go_json_merge_object!(DropForeignKeyArgs, destination, map, key, {
    if go_json_field_matches(&key, "fk_name") {
        map.next_value_seed(NullNoopSeed(&mut *destination.foreign_key_name.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(DropForeignKeyArgs);

impl_go_json_merge_object!(ModifyTableAutoIDCacheArgs, destination, map, key, {
    if go_json_field_matches(&key, "new_cache") {
        map.next_value_seed(NullNoopSeed(&mut *destination.new_cache.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(ModifyTableAutoIDCacheArgs);

impl_go_json_merge_object!(ShardRowIDArgs, destination, map, key, {
    if go_json_field_matches(&key, "shard_row_id_bits") {
        map.next_value_seed(NullNoopSeed(&mut *destination.shard_row_id_bits.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(ShardRowIDArgs);

impl_go_json_merge_object!(SetDefaultValueArgs, destination, map, key, {
    if go_json_field_matches(&key, "column_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut *destination.column.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(SetDefaultValueArgs);

impl_go_json_merge_object!(RefreshMetaArgs, destination, map, key, {
    if go_json_field_matches(&key, "schema_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.schema_id.write()))?;
    } else if go_json_field_matches(&key, "table_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.table_id.write()))?;
    } else if go_json_field_matches(&key, "involved_db") {
        map.next_value_seed(NullNoopSeed(&mut *destination.involved_database.write()))?;
    } else if go_json_field_matches(&key, "involved_table") {
        map.next_value_seed(NullNoopSeed(&mut *destination.involved_table.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(RefreshMetaArgs);

impl_go_json_merge_object!(ModifyTableEngineAttributeArgs, destination, map, key, {
    if go_json_field_matches(&key, "engine_attribute") {
        map.next_value_seed(NullNoopSeed(&mut *destination.engine_attribute.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(ModifyTableEngineAttributeArgs);

impl_go_json_merge_object!(AlterTableModeArgs, destination, map, key, {
    if go_json_field_matches(&key, "table_mode") {
        map.next_value_seed(NullNoopSeed(&mut *destination.table_mode.write()))?;
    } else if go_json_field_matches(&key, "schema_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.schema_id.write()))?;
    } else if go_json_field_matches(&key, "table_id") {
        map.next_value_seed(NullNoopSeed(&mut *destination.table_id.write()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(AlterTableModeArgs);
