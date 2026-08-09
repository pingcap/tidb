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

//! Go-compatible persisted JSON stream decoders for DDL jobs and their nested
//! metadata. Domain types and lifecycle rules remain in [`crate::job`].

use serde::de::DeserializeSeed;
use serde::Deserialize;
use serde_json::value::RawValue;
use tidb_error::terror::{TerrorCode, TerrorError};

use crate::go_runtime::{GoSharedSlice, GoSliceElementLayout};
use crate::job::{
    HistoryInfo, InvolvingSchemaInfo, Job, JobMeta, JobPauseReason, JobResumeReason,
    MultiSchemaInfo, PersistedRawJson, SubJob, TimeZoneLocation, TraceInfo,
};
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
    FatalSeed, NullNoopSeed, OptionSharedAtomicReplaceSeed, OptionSharedGoStringMapMergeSeed,
    OptionSharedMergeSeed, SharedPointerSliceSeed, ValueMergeSeed,
};
use crate::serde_shared_slices::SharedObjectSliceSeed;
use crate::table_info::TableInfo;

fn zero_terror_error() -> TerrorError {
    TerrorError::compatible(TerrorCode::new(0), "")
}

struct RawMessageSeed<'a>(&'a mut Option<PersistedRawJson>);

impl<'de> DeserializeSeed<'de> for RawMessageSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = <&RawValue>::deserialize(deserializer)?;
        let destination = self.0.get_or_insert_with(PersistedRawJson::default);
        destination.replace_unmarshal_json(raw.get().as_bytes().to_vec());
        Ok(())
    }
}

struct SharedBytesSeed<'a>(&'a mut GoSharedSlice<u8>);

impl<'de> DeserializeSeed<'de> for SharedBytesSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Option::<String>::deserialize(deserializer)? {
            None => *self.0 = GoSharedSlice::default(),
            Some(text) => {
                let (bytes, capacity) =
                    crate::serde_helpers::go_bytes::decode_with_capacity(&text)?;
                *self.0 = GoSharedSlice::from_vec_with_capacity(bytes, capacity);
            }
        }
        Ok(())
    }
}

impl_go_json_merge_object!(JobPauseReason, destination, map, key, {
    if go_json_field_matches(&key, "type") {
        map.next_value_seed(NullNoopSeed(&mut destination.type_))?;
    } else if go_json_field_matches(&key, "message") {
        map.next_value_seed(NullNoopSeed(&mut destination.message))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(JobPauseReason);

impl_go_json_merge_object!(JobResumeReason, destination, map, key, {
    if go_json_field_matches(&key, "type") {
        map.next_value_seed(NullNoopSeed(&mut destination.type_))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(JobResumeReason);

impl_go_json_merge_object!(JobMeta, destination, map, key, {
    if go_json_field_matches(&key, "schema_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_id))?;
    } else if go_json_field_matches(&key, "table_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_id))?;
    } else if go_json_field_matches(&key, "job_type") {
        map.next_value_seed(NullNoopSeed(&mut destination.type_))?;
    } else if go_json_field_matches(&key, "query") {
        map.next_value_seed(NullNoopSeed(&mut destination.query))?;
    } else if go_json_field_matches(&key, "priority") {
        map.next_value_seed(NullNoopSeed(&mut destination.priority))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(JobMeta);

impl_go_json_merge_object!(TimeZoneLocation, destination, map, key, {
    if go_json_field_matches(&key, "name") {
        map.next_value_seed(NullNoopSeed(&mut destination.name))?;
    } else if go_json_field_matches(&key, "offset") {
        map.next_value_seed(NullNoopSeed(&mut destination.offset))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(TimeZoneLocation);

impl_go_json_merge_object!(TraceInfo, destination, map, key, {
    if go_json_field_matches(&key, "session_alias") {
        map.next_value_seed(NullNoopSeed(&mut destination.session_alias))?;
    } else if go_json_field_matches(&key, "trace_id") {
        map.next_value_seed(SharedBytesSeed(&mut destination.trace_id))?;
    } else if go_json_field_matches(&key, "connection_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.connection_id))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(TraceInfo);

impl_go_json_merge_object!(InvolvingSchemaInfo, destination, map, key, {
    if go_json_field_matches(&key, "database") {
        map.next_value_seed(NullNoopSeed(&mut destination.database))?;
    } else if go_json_field_matches(&key, "table") {
        map.next_value_seed(NullNoopSeed(&mut destination.table))?;
    } else if go_json_field_matches(&key, "policy") {
        map.next_value_seed(NullNoopSeed(&mut destination.policy))?;
    } else if go_json_field_matches(&key, "resource_group") {
        map.next_value_seed(NullNoopSeed(&mut destination.resource_group))?;
    } else if go_json_field_matches(&key, "mode") {
        map.next_value_seed(NullNoopSeed(&mut destination.mode))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(InvolvingSchemaInfo);

impl_go_json_merge_object!(SubJob, destination, map, key, {
    if go_json_field_matches(&key, "type") {
        map.next_value_seed(NullNoopSeed(&mut destination.type_))?;
    } else if go_json_field_matches(&key, "raw_args") {
        map.next_value_seed(RawMessageSeed(&mut destination.raw_args))?;
    } else if go_json_field_matches(&key, "schema_state") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_state))?;
    } else if go_json_field_matches(&key, "snapshot_ver") {
        map.next_value_seed(NullNoopSeed(&mut destination.snapshot_version))?;
    } else if go_json_field_matches(&key, "real_start_ts") {
        map.next_value_seed(NullNoopSeed(&mut destination.real_start_ts))?;
    } else if go_json_field_matches(&key, "revertible") {
        map.next_value_seed(NullNoopSeed(&mut destination.revertible))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "row_count") {
        map.next_value_seed(NullNoopSeed(&mut destination.row_count))?;
    } else if go_json_field_matches(&key, "warning") {
        map.next_value_seed(FatalSeed(OptionSharedAtomicReplaceSeed::new(
            &mut destination.warning,
            zero_terror_error,
        )))?;
    } else if go_json_field_matches(&key, "schema_version") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_version))?;
    } else if go_json_field_matches(&key, "reorg_tp") {
        map.next_value_seed(NullNoopSeed(&mut destination.reorg_type))?;
    } else if go_json_field_matches(&key, "reorg_stage") {
        map.next_value_seed(NullNoopSeed(&mut destination.reorg_stage))?;
    } else if go_json_field_matches(&key, "analyze_state") {
        map.next_value_seed(NullNoopSeed(&mut destination.analyze_state))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(SubJob);

impl_go_json_merge_object!(MultiSchemaInfo, destination, map, key, {
    if go_json_field_matches(&key, "sub_jobs") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.sub_jobs))?;
    } else if go_json_field_matches(&key, "revertible") {
        map.next_value_seed(NullNoopSeed(&mut destination.revertible))?;
    } else if go_json_field_matches(&key, "seq") {
        map.next_value_seed(NullNoopSeed(&mut destination.seq))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(MultiSchemaInfo);

// `HistoryInfo` owns pointers to these two model objects. Go's decoder reuses
// an existing pointed-to allocation and mutates its fields in declaration
// order, so replacing either object through derived `Deserialize` loses both
// omitted fields and later-field continuation after an error.
impl_go_json_merge_object!(TableInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else if go_json_field_matches(&key, "charset") {
        map.next_value_seed(NullNoopSeed(&mut destination.charset))?;
    } else if go_json_field_matches(&key, "collate") {
        map.next_value_seed(NullNoopSeed(&mut destination.collate))?;
    } else if go_json_field_matches(&key, "cols") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.columns))?;
    } else if go_json_field_matches(&key, "index_info") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.indices))?;
    } else if go_json_field_matches(&key, "constraint_info") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.constraints))?;
    } else if go_json_field_matches(&key, "fk_info") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.foreign_keys))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "pk_is_handle") {
        map.next_value_seed(NullNoopSeed(&mut destination.pk_is_handle))?;
    } else if go_json_field_matches(&key, "is_common_handle") {
        map.next_value_seed(NullNoopSeed(&mut destination.is_common_handle))?;
    } else if go_json_field_matches(&key, "common_handle_version") {
        map.next_value_seed(NullNoopSeed(&mut destination.common_handle_version))?;
    } else if go_json_field_matches(&key, "comment") {
        map.next_value_seed(NullNoopSeed(&mut destination.comment))?;
    } else if go_json_field_matches(&key, "auto_inc_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_inc_id))?;
    } else if go_json_field_matches(&key, "auto_inc_id_extra") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_inc_id_extra))?;
    } else if go_json_field_matches(&key, "auto_id_cache") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_id_cache))?;
    } else if go_json_field_matches(&key, "auto_rand_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_rand_id))?;
    } else if go_json_field_matches(&key, "max_col_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.max_column_id))?;
    } else if go_json_field_matches(&key, "max_idx_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.max_index_id))?;
    } else if go_json_field_matches(&key, "max_fk_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.max_foreign_key_id))?;
    } else if go_json_field_matches(&key, "max_cst_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.max_constraint_id))?;
    } else if go_json_field_matches(&key, "update_timestamp") {
        map.next_value_seed(NullNoopSeed(&mut destination.update_ts))?;
    } else if go_json_field_matches(&key, "old_schema_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_id_schema_id))?;
    } else if go_json_field_matches(&key, "ShardRowIDBits") {
        map.next_value_seed(NullNoopSeed(&mut destination.shard_row_id_bits))?;
    } else if go_json_field_matches(&key, "max_shard_row_id_bits") {
        map.next_value_seed(NullNoopSeed(&mut destination.max_shard_row_id_bits))?;
    } else if go_json_field_matches(&key, "auto_random_bits") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_random_bits))?;
    } else if go_json_field_matches(&key, "auto_random_range_bits") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_random_range_bits))?;
    } else if go_json_field_matches(&key, "pre_split_regions") {
        map.next_value_seed(NullNoopSeed(&mut destination.pre_split_regions))?;
    } else if go_json_field_matches(&key, "partition") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.partition))?;
    } else if go_json_field_matches(&key, "compression") {
        map.next_value_seed(NullNoopSeed(&mut destination.compression))?;
    } else if go_json_field_matches(&key, "view") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.view))?;
    } else if go_json_field_matches(&key, "sequence") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.sequence))?;
    } else if go_json_field_matches(&key, "Lock") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.lock))?;
    } else if go_json_field_matches(&key, "version") {
        map.next_value_seed(NullNoopSeed(&mut destination.version))?;
    } else if go_json_field_matches(&key, "tiflash_replica") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.tiflash_replica))?;
    } else if go_json_field_matches(&key, "is_columnar") {
        map.next_value_seed(NullNoopSeed(&mut destination.is_columnar))?;
    } else if go_json_field_matches(&key, "temp_table_type") {
        map.next_value_seed(NullNoopSeed(&mut destination.temp_table_type))?;
    } else if go_json_field_matches(&key, "cache_table_status") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_cache_status_type))?;
    } else if go_json_field_matches(&key, "policy_ref_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.placement_policy_ref))?;
    } else if go_json_field_matches(&key, "stats_options") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.stats_options))?;
    } else if go_json_field_matches(&key, "exchange_partition_info") {
        map.next_value_seed(OptionSharedMergeSeed(
            &mut destination.exchange_partition_info,
        ))?;
    } else if go_json_field_matches(&key, "ttl_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.ttl_info))?;
    } else if go_json_field_matches(&key, "is_active_active") {
        map.next_value_seed(NullNoopSeed(&mut destination.is_active_active))?;
    } else if go_json_field_matches(&key, "softdelete_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.softdelete_info))?;
    } else if go_json_field_matches(&key, "affinity") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.affinity))?;
    } else if go_json_field_matches(&key, "table_split_policy") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.table_split_policy))?;
    } else if go_json_field_matches(&key, "revision") {
        map.next_value_seed(NullNoopSeed(&mut destination.revision))?;
    } else if go_json_field_matches(&key, "engine_attribute") {
        map.next_value_seed(NullNoopSeed(&mut destination.engine_attribute))?;
    } else if go_json_field_matches(&key, "storage_class_tier") {
        map.next_value_seed(NullNoopSeed(&mut destination.storage_class_tier))?;
    } else if go_json_field_matches(&key, "storage_class_transitions") {
        map.next_value_seed(SharedObjectSliceSeed::new(
            &mut destination.storage_class_transitions,
            32,
            GoSliceElementLayout::PointerBearing,
        ))?;
    } else if go_json_field_matches(&key, "mode") {
        map.next_value_seed(NullNoopSeed(&mut destination.mode))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(TableInfo);

impl_go_json_merge_object!(HistoryInfo, destination, map, key, {
    if go_json_field_matches(&key, "SchemaVersion") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_version))?;
    } else if go_json_field_matches(&key, "DBInfo") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.db_info))?;
    } else if go_json_field_matches(&key, "TableInfo") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.table_info))?;
    } else if go_json_field_matches(&key, "FinishedTS") {
        map.next_value_seed(NullNoopSeed(&mut destination.finished_ts))?;
    } else if go_json_field_matches(&key, "MultipleTableInfos") {
        map.next_value_seed(SharedPointerSliceSeed(
            &mut destination.multiple_table_infos,
        ))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(HistoryInfo);

impl_go_json_merge_object!(Job, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "type") {
        map.next_value_seed(NullNoopSeed(&mut destination.type_))?;
    } else if go_json_field_matches(&key, "schema_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_id))?;
    } else if go_json_field_matches(&key, "table_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_id))?;
    } else if go_json_field_matches(&key, "schema_name") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_name))?;
    } else if go_json_field_matches(&key, "table_name") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_name))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "warning") {
        map.next_value_seed(FatalSeed(OptionSharedAtomicReplaceSeed::new(
            &mut destination.warning,
            zero_terror_error,
        )))?;
    } else if go_json_field_matches(&key, "err") {
        map.next_value_seed(FatalSeed(OptionSharedAtomicReplaceSeed::new(
            &mut destination.error,
            zero_terror_error,
        )))?;
    } else if go_json_field_matches(&key, "err_count") {
        map.next_value_seed(NullNoopSeed(&mut destination.error_count))?;
    } else if go_json_field_matches(&key, "row_count") {
        map.next_value_seed(NullNoopSeed(&mut destination.row_count))?;
    } else if go_json_field_matches(&key, "raw_args") {
        map.next_value_seed(RawMessageSeed(&mut destination.raw_args))?;
    } else if go_json_field_matches(&key, "schema_state") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_state))?;
    } else if go_json_field_matches(&key, "snapshot_ver") {
        map.next_value_seed(NullNoopSeed(&mut destination.snapshot_version))?;
    } else if go_json_field_matches(&key, "real_start_ts") {
        map.next_value_seed(NullNoopSeed(&mut destination.real_start_ts))?;
    } else if go_json_field_matches(&key, "start_ts") {
        map.next_value_seed(NullNoopSeed(&mut destination.start_ts))?;
    } else if go_json_field_matches(&key, "dependency_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.dependency_id))?;
    } else if go_json_field_matches(&key, "query") {
        map.next_value_seed(NullNoopSeed(&mut destination.query))?;
    } else if go_json_field_matches(&key, "binlog") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.binlog_info))?;
    } else if go_json_field_matches(&key, "version") {
        map.next_value_seed(NullNoopSeed(&mut destination.version))?;
    } else if go_json_field_matches(&key, "reorg_meta") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.reorg_meta))?;
    } else if go_json_field_matches(&key, "multi_schema_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.multi_schema_info))?;
    } else if go_json_field_matches(&key, "priority") {
        map.next_value_seed(NullNoopSeed(&mut destination.priority))?;
    } else if go_json_field_matches(&key, "seq_num") {
        map.next_value_seed(NullNoopSeed(&mut destination.sequence_number))?;
    } else if go_json_field_matches(&key, "charset") {
        map.next_value_seed(NullNoopSeed(&mut destination.charset))?;
    } else if go_json_field_matches(&key, "collate") {
        map.next_value_seed(NullNoopSeed(&mut destination.collate))?;
    } else if go_json_field_matches(&key, "involving_schema_info") {
        map.next_value_seed(SharedObjectSliceSeed::new(
            &mut destination.involving_schema_info,
            72,
            GoSliceElementLayout::PointerBearing,
        ))?;
    } else if go_json_field_matches(&key, "admin_operator") {
        map.next_value_seed(NullNoopSeed(&mut destination.admin_operator))?;
    } else if go_json_field_matches(&key, "pause_reason") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.pause_reason))?;
    } else if go_json_field_matches(&key, "resume_reason") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.resume_reason))?;
    } else if go_json_field_matches(&key, "trace_info") {
        map.next_value_seed(OptionSharedMergeSeed(&mut destination.trace_info))?;
    } else if go_json_field_matches(&key, "bdr_role") {
        map.next_value_seed(NullNoopSeed(&mut destination.bdr_role))?;
    } else if go_json_field_matches(&key, "cdc_write_source") {
        map.next_value_seed(NullNoopSeed(&mut destination.cdc_write_source))?;
    } else if go_json_field_matches(&key, "local_mode") {
        map.next_value_seed(NullNoopSeed(&mut destination.local_mode))?;
    } else if go_json_field_matches(&key, "sql_mode") {
        map.next_value_seed(NullNoopSeed(&mut destination.sql_mode))?;
    } else if go_json_field_matches(&key, "session_vars") {
        map.next_value_seed(OptionSharedGoStringMapMergeSeed(
            &mut destination.session_vars,
        ))?;
    } else if go_json_field_matches(&key, "last_schema_version") {
        map.next_value_seed(NullNoopSeed(&mut destination.last_schema_version))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});
impl_go_json_deserialize!(Job);
