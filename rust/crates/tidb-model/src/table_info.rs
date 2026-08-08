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

//! `TableInfo` from `pkg/meta/model/table.go`: the central table-metadata
//! struct. All of its field types and nearly all of its methods are ported.
//!
//! The pointer-slice representation and source-shaped table APIs are ported in
//! this layer. The still-derived Rust `Clone` is deliberately not claimed as
//! Go `TableInfo.Clone`: the owning follow-on semantic cluster must migrate
//! the remaining pointer fields to shared handles and then replace the derive
//! with Go's selective deep/shallow copy policy.
//!
//! Go's `Equals`/`Hash64` identity rule is exposed explicitly by
//! [`TableInfo::equals_id`] and [`TableInfo::hash64`]; Rust structural equality
//! is deliberately not overloaded with that narrower planner identity.

use tidb_ast::CiString;
use tidb_datatype::FieldTypeFlags;

use crate::cascades_hash::HashInt64;
use crate::column::ColumnInfo;
use crate::engine_attribute::{build_storage_class_string, StorageClassTransitRule};
use crate::go_runtime::{GoPointerAny, GoShared, GoSharedPointerSlice, GoTime};
use crate::index::{IndexInfo, RegionSplitPolicy};
use crate::partition::PartitionInfo;
use crate::placement::PolicyRefInfo;
use crate::schema_state::SchemaState;
use crate::table::{
    ConstraintInfo, ExchangePartitionInfo, FKInfo, SequenceInfo, SoftdeleteInfo, StatsOptions,
    TTLInfo, TableAffinityInfo, TableCacheStatusType, TableLockInfo, TempTableType,
    TiFlashReplicaInfo, ViewInfo,
};
use crate::table_mode::TableMode;

/// Go's `omitempty` check for `Mode TableMode`: the zero mode is omitted.
fn is_default_table_mode(mode: &TableMode) -> bool {
    *mode == TableMode::default()
}

/// Go `TableInfoVersion0`.
pub const TABLE_INFO_VERSION0: u16 = 0;
/// Go `TableInfoVersion1`.
pub const TABLE_INFO_VERSION1: u16 = 1;
/// Go `TableInfoVersion2`.
pub const TABLE_INFO_VERSION2: u16 = 2;
/// Go `TableInfoVersion3`.
pub const TABLE_INFO_VERSION3: u16 = 3;
/// Go `TableInfoVersion4`.
pub const TABLE_INFO_VERSION4: u16 = 4;
/// Go `TableInfoVersion5`: separate auto-increment allocator support.
pub const TABLE_INFO_VERSION5: u16 = 5;
/// Go `CurrLatestTableInfoVersion`.
pub const CURR_LATEST_TABLE_INFO_VERSION: u16 = TABLE_INFO_VERSION5;

/// Go `TableInfo`: metadata describing a table.
///
/// Go's pointer sub-structs (`*PartitionInfo`, `*ViewInfo`, ...) become
/// `Option<Box<..>>`; its two embedded fields (`TempTableType`,
/// `TableCacheStatusType`) become named fields.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TableInfo {
    /// The table ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The table name.
    #[serde(rename = "name", default)]
    pub name: CiString,
    /// The table charset.
    #[serde(
        rename = "charset",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub charset: String,
    /// The table collation.
    #[serde(
        rename = "collate",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub collate: String,
    /// The columns.
    #[serde(rename = "cols", default)]
    pub columns: GoSharedPointerSlice<ColumnInfo>,
    /// The indexes.
    #[serde(rename = "index_info", default)]
    pub indices: GoSharedPointerSlice<IndexInfo>,
    /// The CHECK constraints.
    #[serde(rename = "constraint_info", default)]
    pub constraints: GoSharedPointerSlice<ConstraintInfo>,
    /// The foreign keys.
    #[serde(rename = "fk_info", default)]
    pub foreign_keys: GoSharedPointerSlice<FKInfo>,
    /// The online-DDL state.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
    /// Whether the primary key is the handle.
    #[serde(rename = "pk_is_handle", default)]
    pub pk_is_handle: bool,
    /// Whether the table uses a clustered common handle.
    #[serde(rename = "is_common_handle", default)]
    pub is_common_handle: bool,
    /// The common-handle version.
    #[serde(rename = "common_handle_version", default)]
    pub common_handle_version: u16,
    /// The table comment.
    #[serde(
        rename = "comment",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub comment: String,
    /// The next auto-increment ID.
    #[serde(rename = "auto_inc_id", default)]
    pub auto_inc_id: i64,
    /// An extra auto-increment ID reservation.
    #[serde(
        rename = "auto_inc_id_extra",
        default,
        skip_serializing_if = "crate::serde_helpers::is_zero_i64"
    )]
    pub auto_inc_id_extra: i64,
    /// The auto-ID cache size.
    #[serde(rename = "auto_id_cache", default)]
    pub auto_id_cache: i64,
    /// The next auto-random ID.
    #[serde(rename = "auto_rand_id", default)]
    pub auto_rand_id: i64,
    /// The maximum column ID.
    #[serde(rename = "max_col_id", default)]
    pub max_column_id: i64,
    /// The maximum index ID.
    #[serde(rename = "max_idx_id", default)]
    pub max_index_id: i64,
    /// The maximum foreign-key ID.
    #[serde(rename = "max_fk_id", default)]
    pub max_foreign_key_id: i64,
    /// The maximum constraint ID.
    #[serde(rename = "max_cst_id", default)]
    pub max_constraint_id: i64,
    /// The last-update timestamp (a TSO).
    #[serde(rename = "update_timestamp", default)]
    pub update_ts: u64,
    /// The schema ID that owns the auto-ID (for `RENAME`).
    #[serde(
        rename = "old_schema_id",
        default,
        skip_serializing_if = "crate::serde_helpers::is_zero_i64"
    )]
    pub auto_id_schema_id: i64,
    /// The `SHARD_ROW_ID_BITS` setting.
    #[serde(rename = "ShardRowIDBits", default)]
    pub shard_row_id_bits: u64,
    /// The maximum `SHARD_ROW_ID_BITS`.
    #[serde(rename = "max_shard_row_id_bits", default)]
    pub max_shard_row_id_bits: u64,
    /// The `AUTO_RANDOM` bit count.
    #[serde(rename = "auto_random_bits", default)]
    pub auto_random_bits: u64,
    /// The `AUTO_RANDOM` range-bit count.
    #[serde(rename = "auto_random_range_bits", default)]
    pub auto_random_range_bits: u64,
    /// The pre-split region count.
    #[serde(rename = "pre_split_regions", default)]
    pub pre_split_regions: u64,
    /// The partitioning metadata.
    #[serde(rename = "partition", default)]
    pub partition: Option<Box<PartitionInfo>>,
    /// The compression setting.
    #[serde(
        rename = "compression",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub compression: String,
    /// The view metadata, if this is a view.
    #[serde(rename = "view", default)]
    pub view: Option<Box<ViewInfo>>,
    /// The sequence metadata, if this is a sequence.
    #[serde(rename = "sequence", default)]
    pub sequence: Option<Box<SequenceInfo>>,
    /// The table lock, if held.
    #[serde(rename = "Lock", default)]
    pub lock: Option<Box<TableLockInfo>>,
    /// The table-info version.
    #[serde(rename = "version", default)]
    pub version: u16,
    /// The TiFlash replica configuration.
    #[serde(rename = "tiflash_replica", default)]
    pub tiflash_replica: Option<Box<TiFlashReplicaInfo>>,
    /// Whether the table is columnar.
    #[serde(rename = "is_columnar", default)]
    pub is_columnar: bool,
    /// The temporary-table kind (Go's embedded `TempTableType`).
    #[serde(rename = "temp_table_type", default)]
    pub temp_table_type: TempTableType,
    /// The cache status (Go's embedded `TableCacheStatusType`).
    #[serde(rename = "cache_table_status", default)]
    pub table_cache_status_type: TableCacheStatusType,
    /// The placement-policy reference.
    #[serde(rename = "policy_ref_info", default)]
    pub placement_policy_ref: Option<PolicyRefInfo>,
    /// The persisted ANALYZE options.
    #[serde(rename = "stats_options", default)]
    pub stats_options: Option<Box<StatsOptions>>,
    /// In-progress partition-exchange metadata.
    #[serde(rename = "exchange_partition_info", default)]
    pub exchange_partition_info: Option<Box<ExchangePartitionInfo>>,
    /// The TTL configuration.
    #[serde(rename = "ttl_info", default)]
    pub ttl_info: Option<Box<TTLInfo>>,
    /// Whether the table is active-active.
    #[serde(
        rename = "is_active_active",
        default,
        skip_serializing_if = "crate::serde_helpers::is_false"
    )]
    pub is_active_active: bool,
    /// The soft-delete configuration.
    #[serde(
        rename = "softdelete_info",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub softdelete_info: Option<Box<SoftdeleteInfo>>,
    /// The affinity configuration.
    #[serde(rename = "affinity", default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Box<TableAffinityInfo>>,
    /// The persistent region-split policy.
    #[serde(
        rename = "table_split_policy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub table_split_policy: Option<Box<RegionSplitPolicy>>,
    /// The schema revision.
    #[serde(rename = "revision", default)]
    pub revision: u64,
    /// The owning database ID (not serialized).
    #[serde(skip)]
    pub db_id: i64,
    /// The `ENGINE_ATTRIBUTE` value.
    #[serde(
        rename = "engine_attribute",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        skip_serializing_if = "crate::serde_helpers::is_empty_str"
    )]
    pub engine_attribute: String,
    /// The storage-class tier.
    #[serde(
        rename = "storage_class_tier",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        skip_serializing_if = "crate::serde_helpers::is_empty_str"
    )]
    pub storage_class_tier: String,
    /// The storage-class transitions.
    #[serde(
        rename = "storage_class_transitions",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        skip_serializing_if = "crate::serde_helpers::is_empty_vec"
    )]
    pub storage_class_transitions: Vec<StorageClassTransitRule>,
    /// The table mode (normal/import/restore).
    #[serde(
        rename = "mode",
        default,
        skip_serializing_if = "is_default_table_mode"
    )]
    pub mode: TableMode,
}

impl TableInfo {
    /// Go `TableInfo.Equals` compares only persisted table identity.
    #[must_use]
    pub fn equals_id(&self, other: &Self) -> bool {
        self.id == other.id
    }

    /// Exact Go `Equals(any)`, including wrong dynamic types and typed-nil
    /// pointer interfaces. `receiver` represents Go's possibly nil method
    /// receiver.
    #[must_use]
    pub fn equals(receiver: Option<&Self>, other: GoPointerAny<'_, Self>) -> bool {
        let GoPointerAny::Typed(other) = other else {
            return false;
        };
        match (receiver, other) {
            (None, None) => true,
            (Some(left), Some(right)) => left.id == right.id,
            _ => false,
        }
    }

    /// Go `Hash64`: hash the persisted table ID as one whole cascades
    /// `HashInt64` step.
    pub fn hash64<H: HashInt64>(&self, state: &mut H) {
        state.hash_int64(self.id);
    }

    /// Nil-receiver-capable Go `Hash64` call boundary. A nil `*TableInfo`
    /// panics when Go evaluates `t.ID`.
    pub fn hash64_pointer<H: HashInt64>(table: Option<&Self>, state: &mut H) {
        table.expect("nil *TableInfo").hash64(state);
    }

    /// Go `GetPartitionInfo`: the partition info when partitioning is enabled.
    #[must_use]
    pub fn get_partition_info(&self) -> Option<&PartitionInfo> {
        match &self.partition {
            Some(p) if p.enable => Some(p),
            _ => None,
        }
    }

    /// Mutable form of Go `GetPartitionInfo`; mutations affect this table.
    pub fn get_partition_info_mut(&mut self) -> Option<&mut PartitionInfo> {
        match &mut self.partition {
            Some(partition) if partition.enable => Some(partition),
            _ => None,
        }
    }

    /// Go `GetUpdateTime`: the last-update time (from the `update_ts` TSO).
    #[must_use]
    pub fn get_update_time(&self) -> GoTime {
        GoTime::from_tso(self.update_ts)
    }

    /// Go `GetPkColInfo`: the primary-key column (by the PRI-KEY flag).
    #[must_use]
    pub fn get_pk_col_info(&self) -> Option<GoShared<ColumnInfo>> {
        self.columns
            .iter_deref()
            .find(|column| column.read().get_flag() & u64::from(FieldTypeFlags::PRI_KEY) != 0)
    }

    /// Mutable handle form of Go `GetPkColInfo`.
    pub fn get_pk_col_info_mut(&mut self) -> Option<GoShared<ColumnInfo>> {
        self.get_pk_col_info()
    }

    /// Go `GetPkName`: the primary-key column name (empty when none).
    #[must_use]
    pub fn get_pk_name(&self) -> CiString {
        self.get_pk_col_info()
            .map_or_else(CiString::default, |column| column.read().name.clone())
    }

    /// Go `ContainsAutoRandomBits`: whether `AUTO_RANDOM` is configured.
    #[must_use]
    pub fn contains_auto_random_bits(&self) -> bool {
        self.auto_random_bits != 0
    }

    /// Go `IsAutoRandomBitColUnsigned`: whether the auto-random handle column
    /// is unsigned.
    #[must_use]
    pub fn is_auto_random_bit_col_unsigned(&self) -> bool {
        if !self.pk_is_handle || self.auto_random_bits == 0 {
            return false;
        }
        self.get_pk_col_info()
            .expect("PKIsHandle with AutoRandomBits requires a primary-key column")
            .read()
            .get_flag()
            & u64::from(FieldTypeFlags::UNSIGNED)
            != 0
    }

    /// Go `Cols`: the public columns in offset-indexed slots. A transient DDL
    /// gap is retained as `None`, exactly matching the nil element Go leaves
    /// in its returned slice.
    #[must_use]
    pub fn cols(&self) -> GoSharedPointerSlice<ColumnInfo> {
        let mut slots = vec![None; self.columns.len()];
        let mut max_offset: i64 = -1;
        for column in self.columns.iter_deref() {
            let col = column.read();
            if col.state != SchemaState::PUBLIC {
                continue;
            }
            let off = col.offset as usize;
            // Go indexes `publicColumns[col.Offset]` directly. Invalid
            // metadata is an invariant violation and panics rather than being
            // silently dropped.
            slots[off] = Some(column.clone());
            if col.offset > max_offset {
                max_offset = col.offset;
            }
        }
        let visible_len = (max_offset + 1) as usize;
        slots.truncate(visible_len);
        GoSharedPointerSlice::from_handles_with_capacity(slots, self.columns.len())
    }

    /// Compatibility spelling for callers that already named the Go gap.
    #[must_use]
    pub fn cols_with_gaps(&self) -> GoSharedPointerSlice<ColumnInfo> {
        self.cols()
    }

    /// Present public columns, used by Rust callers that explicitly do not
    /// consume Go's nil-gap invariant.
    #[must_use]
    pub fn present_cols(&self) -> Vec<GoShared<ColumnInfo>> {
        self.cols().handles().into_iter().flatten().collect()
    }

    /// Mutable handles for the exact Go `Cols` result.
    pub fn cols_mut(&mut self) -> GoSharedPointerSlice<ColumnInfo> {
        self.cols()
    }

    /// Go `FindPublicColumnByName`: the public column named `col_name_l`
    /// (already lower-cased).
    #[must_use]
    pub fn find_public_column_by_name(&self, col_name_l: &str) -> Option<GoShared<ColumnInfo>> {
        self.cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == col_name_l)
    }

    /// Mutable handle form of Go `FindPublicColumnByName`.
    pub fn find_public_column_by_name_mut(
        &mut self,
        col_name_l: &str,
    ) -> Option<GoShared<ColumnInfo>> {
        self.find_public_column_by_name(col_name_l)
    }

    /// Go `GetPrimaryKey`: the explicit primary index, else an implicit one
    /// (a unique index over only non-null, non-hidden public columns).
    #[must_use]
    pub fn get_primary_key(&self) -> Option<GoShared<IndexInfo>> {
        let cols = self.cols();
        let mut implicit_pk = None;
        for key in self.indices.iter_deref() {
            let index = key.read();
            if index.primary {
                drop(index);
                return Some(key);
            }
            if index.columns.is_empty() {
                continue;
            }
            if implicit_pk.is_none() && index.unique {
                let mut all_col_not_null = true;
                let mut skip = false;
                for idx_col in &index.columns {
                    let col = cols
                        .iter_deref()
                        .find(|column| column.read().name.lowercase() == idx_col.name.lowercase());
                    match col {
                        None => {
                            skip = true;
                            break;
                        }
                        Some(column) => {
                            let column = column.read();
                            if column.hidden {
                                skip = true;
                                break;
                            }
                            if column.get_flag() & u64::from(FieldTypeFlags::NOT_NULL) == 0 {
                                all_col_not_null = false;
                                break;
                            }
                        }
                    }
                }
                if skip {
                    continue;
                }
                if all_col_not_null {
                    implicit_pk = Some(key.clone());
                }
            }
        }
        implicit_pk
    }

    /// Mutable handle form of Go `GetPrimaryKey`.
    pub fn get_primary_key_mut(&mut self) -> Option<GoShared<IndexInfo>> {
        self.get_primary_key()
    }

    /// Go `FindColumnByID`: the column with `id` (any state).
    #[must_use]
    pub fn find_column_by_id(&self, id: i64) -> Option<GoShared<ColumnInfo>> {
        self.columns
            .iter_deref()
            .find(|column| column.read().id == id)
    }

    /// Mutable handle form of Go `FindColumnByID`.
    pub fn find_column_by_id_mut(&mut self, id: i64) -> Option<GoShared<ColumnInfo>> {
        self.find_column_by_id(id)
    }

    /// Go `GetColumnByID`: the public column with `id`.
    #[must_use]
    pub fn get_column_by_id(&self, id: i64) -> Option<GoShared<ColumnInfo>> {
        self.columns.iter_deref().find(|column| {
            let column = column.read();
            column.state == SchemaState::PUBLIC && column.id == id
        })
    }

    /// Mutable handle form of Go `GetColumnByID`.
    pub fn get_column_by_id_mut(&mut self, id: i64) -> Option<GoShared<ColumnInfo>> {
        self.get_column_by_id(id)
    }

    /// Go `FindIndexByName`: the index named `idx_name` (already lower-cased).
    #[must_use]
    pub fn find_index_by_name(&self, idx_name: &str) -> Option<GoShared<IndexInfo>> {
        self.indices
            .iter_deref()
            .find(|index| index.read().name.lowercase() == idx_name)
    }

    /// Mutable handle form of Go `FindIndexByName`.
    pub fn find_index_by_name_mut(&mut self, idx_name: &str) -> Option<GoShared<IndexInfo>> {
        self.find_index_by_name(idx_name)
    }

    /// Go `FindIndexByID`: the index with `id`.
    #[must_use]
    pub fn find_index_by_id(&self, id: i64) -> Option<GoShared<IndexInfo>> {
        self.indices
            .iter_deref()
            .find(|index| index.read().id == id)
    }

    /// Mutable handle form of Go `FindIndexByID`.
    pub fn find_index_by_id_mut(&mut self, id: i64) -> Option<GoShared<IndexInfo>> {
        self.find_index_by_id(id)
    }

    /// Go `FindConstraintInfoByName`: the CHECK constraint named `constr_name`
    /// (case-insensitive).
    #[must_use]
    pub fn find_constraint_info_by_name(
        &self,
        constr_name: &str,
    ) -> Option<GoShared<ConstraintInfo>> {
        let low = tidb_mysql::to_lowercase(constr_name);
        self.constraints
            .iter_deref()
            .find(|constraint| constraint.read().name.lowercase() == low)
    }

    /// Mutable handle form of Go `FindConstraintInfoByName`.
    pub fn find_constraint_info_by_name_mut(
        &mut self,
        constr_name: &str,
    ) -> Option<GoShared<ConstraintInfo>> {
        self.find_constraint_info_by_name(constr_name)
    }

    /// Go `GetAutoIncrementColInfo`: the auto-increment column, if any.
    #[must_use]
    pub fn get_auto_increment_col_info(&self) -> Option<GoShared<ColumnInfo>> {
        self.columns.iter_deref().find(|column| {
            column.read().get_flag() & u64::from(FieldTypeFlags::AUTO_INCREMENT) != 0
        })
    }

    /// Mutable handle form of Go `GetAutoIncrementColInfo`.
    pub fn get_auto_increment_col_info_mut(&mut self) -> Option<GoShared<ColumnInfo>> {
        self.get_auto_increment_col_info()
    }

    /// Go `ColumnIsInIndex`: whether column `c` participates in any index.
    #[must_use]
    pub fn column_is_in_index(&self, c: &ColumnInfo) -> bool {
        self.indices.iter_deref().any(|index| {
            let index = index.read();
            index
                .columns
                .iter()
                .any(|ic| ic.name.lowercase() == c.name.lowercase())
        })
    }

    /// Go `HasClusteredIndex`: whether the table has a clustered index.
    #[must_use]
    pub fn has_clustered_index(&self) -> bool {
        self.pk_is_handle || self.is_common_handle
    }

    /// Go `IsAutoIncColUnsigned`: whether the auto-increment column is unsigned.
    #[must_use]
    pub fn is_auto_inc_col_unsigned(&self) -> bool {
        self.get_auto_increment_col_info().is_some_and(|column| {
            column.read().get_flag() & u64::from(FieldTypeFlags::UNSIGNED) != 0
        })
    }

    /// Go `FindColumnNameByID`: the (lower-cased) name of column `id`, or "".
    #[must_use]
    pub fn find_column_name_by_id(&self, id: i64) -> String {
        self.find_column_by_id(id)
            .map_or_else(String::new, |column| {
                column.read().name.lowercase().to_owned()
            })
    }

    /// Go `FindIndexNameByID`: the (lower-cased) name of index `id`, or "".
    #[must_use]
    pub fn find_index_name_by_id(&self, id: i64) -> String {
        self.indices
            .iter_deref()
            .find(|index| index.read().id == id)
            .map_or_else(String::new, |index| {
                index.read().name.lowercase().to_owned()
            })
    }

    /// Go `GetNonTempColumns`: the non-removing columns, with a changing
    /// column's origin column excluded. Keyed by lower-cased name; the remove
    /// key is the changing column's origin name (Go's original-case
    /// `GetChangingOriginName`), matching Go's map behavior.
    #[must_use]
    pub fn get_non_temp_columns(&self) -> Vec<GoShared<ColumnInfo>> {
        use std::collections::BTreeMap;
        let mut col_map = BTreeMap::new();
        for column in self.columns.iter_deref() {
            let col = column.read();
            if col.is_removing() {
                continue;
            }
            col_map.insert(col.name.lowercase().to_owned(), column.clone());
        }
        for column in self.columns.iter_deref() {
            let col = column.read();
            if col.is_removing() {
                continue;
            }
            if col.is_changing() {
                col_map.remove(&col.get_changing_origin_name());
            }
        }
        col_map.into_values().collect()
    }

    /// Mutable handles for Go `GetNonTempColumns`.
    pub fn get_non_temp_columns_mut(&mut self) -> Vec<GoShared<ColumnInfo>> {
        self.get_non_temp_columns()
    }

    /// Go `ClearPlacement`: drop the table's and partitions' placement refs.
    pub fn clear_placement(&mut self) {
        self.placement_policy_ref = None;
        if let Some(p) = &mut self.partition {
            for def in &mut p.definitions {
                def.placement_policy_ref = None;
            }
        }
    }

    /// Go `SepAutoInc`: whether the table uses a separate auto-increment
    /// allocator (version >= 5 and an auto-ID cache of 1).
    #[must_use]
    pub fn sep_auto_inc(&self) -> bool {
        self.version >= TABLE_INFO_VERSION5 && self.auto_id_cache == 1
    }

    /// Go `StorageClassString`: the JSON string describing the storage class.
    #[must_use]
    pub fn storage_class_string(&self) -> String {
        build_storage_class_string(&self.storage_class_tier, &self.storage_class_transitions)
    }

    /// Go `MoveColumnInfo`: move the column at `from` to `to`, re-numbering
    /// column offsets and fixing up the offsets referenced by index columns,
    /// affected columns, and change-state dependencies. Go assumes each
    /// column's offset equals its position.
    pub fn move_column_info(&mut self, from: isize, to: isize) {
        use std::collections::BTreeMap;
        if from == to {
            return;
        }

        // Go reads the source slot before validating the destination. Keep
        // that ordering so invalid signed indexes and recovered panics expose
        // the same partially shifted receiver.
        let src = self.columns.get(from as usize);
        let mut updated = BTreeMap::new();
        if from < to {
            let mut i = from;
            while i < to {
                let next = self.columns.get((i + 1) as usize);
                self.columns.set(i as usize, next);
                self.columns
                    .get(i as usize)
                    .expect("nil *ColumnInfo in MoveColumnInfo")
                    .write()
                    .offset = i as i64;
                updated.insert(i + 1, i);
                i += 1;
            }
        } else {
            let mut i = from;
            while i > to {
                let previous = self.columns.get((i - 1) as usize);
                self.columns.set(i as usize, previous);
                self.columns
                    .get(i as usize)
                    .expect("nil *ColumnInfo in MoveColumnInfo")
                    .write()
                    .offset = i as i64;
                updated.insert(i - 1, i);
                i -= 1;
            }
        }
        self.columns.set(to as usize, src);
        self.columns
            .get(to as usize)
            .expect("nil *ColumnInfo in MoveColumnInfo")
            .write()
            .offset = to as i64;
        updated.insert(from, to);

        for index in self.indices.iter_deref() {
            let mut idx = index.write();
            for ic in &mut idx.columns {
                if let Some(&new_offset) = updated.get(&(ic.offset as isize)) {
                    ic.offset = new_offset as i64;
                }
            }
            for ac in &mut idx.affect_column {
                if let Some(&new_offset) = updated.get(&(ac.offset as isize)) {
                    ac.offset = new_offset as i64;
                }
            }
        }
        for column in self.columns.iter_deref() {
            let mut col = column.write();
            if let Some(cs) = &mut col.change_state_info {
                if let Some(&new_offset) = updated.get(&(cs.dependency_column_offset as isize)) {
                    cs.dependency_column_offset = new_offset as i64;
                }
            }
        }
    }

    /// Go `IsView`.
    #[must_use]
    pub fn is_view(&self) -> bool {
        self.view.is_some()
    }

    /// Go `IsSequence`.
    #[must_use]
    pub fn is_sequence(&self) -> bool {
        self.sequence.is_some()
    }

    /// Go `IsBaseTable`: neither a view nor a sequence.
    #[must_use]
    pub fn is_base_table(&self) -> bool {
        self.sequence.is_none() && self.view.is_none()
    }

    /// Go `IsLocked`: whether the table lock is held by a session.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.lock.as_ref().is_some_and(|l| !l.sessions.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::ViewInfo;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn pk_col(name: &str, unsigned: bool) -> ColumnInfo {
        let mut c = ColumnInfo::new_extra_handle_col_info();
        c.name = CiString::new(name);
        c.field_type = FieldType::new(FieldTypeCode::LongLong);
        c.set_flag(u64::from(FieldTypeFlags::PRI_KEY));
        if unsigned {
            c.add_flag(u64::from(FieldTypeFlags::UNSIGNED));
        }
        c
    }

    #[test]
    fn public_column_slots_preserve_transient_gaps() {
        let table = TableInfo {
            columns: vec![
                ColumnInfo {
                    name: CiString::new("hidden-state"),
                    offset: 0,
                    state: SchemaState::WRITE_ONLY,
                    ..Default::default()
                },
                ColumnInfo {
                    name: CiString::new("public"),
                    offset: 1,
                    state: SchemaState::PUBLIC,
                    ..Default::default()
                },
            ]
            .into(),
            ..Default::default()
        };
        let slots = table.cols_with_gaps();
        assert!(slots.is_allocated());
        assert_eq!(slots.len(), 2);
        assert_eq!(slots.capacity(), table.columns.len());
        assert!(!slots.backing_ptr_eq(&table.columns));
        assert!(slots.get(0).is_none());
        assert_eq!(slots.get(1).unwrap().read().name.original(), "public");
        assert!(slots.get(1).unwrap().ptr_eq(&table.columns.get(1).unwrap()));

        let no_public = TableInfo {
            columns: vec![ColumnInfo {
                state: SchemaState::WRITE_ONLY,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        let no_public_cols = no_public.cols();
        assert!(no_public_cols.is_allocated());
        assert_eq!(no_public_cols.len(), 0);
        assert_eq!(no_public_cols.capacity(), 1);

        assert!(std::panic::catch_unwind(|| table.find_public_column_by_name("public")).is_err());

        let invalid_offset = TableInfo {
            columns: vec![ColumnInfo {
                offset: 1,
                state: SchemaState::PUBLIC,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        assert!(std::panic::catch_unwind(|| invalid_offset.cols_with_gaps()).is_err());
    }

    #[test]
    fn table_identity_hash_and_equality_use_only_id() {
        let left = TableInfo {
            id: 9,
            name: CiString::new("left"),
            ..Default::default()
        };
        let right = TableInfo {
            id: 9,
            name: CiString::new("right"),
            ..Default::default()
        };
        let other = TableInfo {
            id: 10,
            name: CiString::new("left"),
            ..Default::default()
        };
        assert!(left.equals_id(&right));
        assert!(!left.equals_id(&other));
        assert!(TableInfo::equals(
            Some(&left),
            GoPointerAny::typed(Some(&right))
        ));
        assert!(!TableInfo::equals(Some(&left), GoPointerAny::Other));
        assert!(!TableInfo::equals(Some(&left), GoPointerAny::typed(None)));
        assert!(TableInfo::equals(None, GoPointerAny::typed(None)));
        assert!(!TableInfo::equals(None, GoPointerAny::typed(Some(&right))));
        let hash = |table: &TableInfo| {
            let mut state = crate::cascades_hash::CascadesHasher::new();
            table.hash64(&mut state);
            state.sum64()
        };
        assert_eq!(hash(&left), hash(&right));
        assert!(std::panic::catch_unwind(|| {
            let mut state = crate::cascades_hash::CascadesHasher::new();
            TableInfo::hash64_pointer(None, &mut state);
        })
        .is_err());
        for (id, expected) in [
            (0, 12_638_153_115_695_167_455),
            (-1, 5_808_589_858_502_755_950),
            (i64::MIN, 3_414_781_078_840_391_647),
            (i64::MAX, 15_031_961_895_357_531_758),
        ] {
            let table = TableInfo {
                id,
                ..Default::default()
            };
            assert_eq!(hash(&table), expected);
        }
    }

    #[test]
    fn update_time_retains_go_unix_milli_domain() {
        let table = TableInfo {
            update_ts: u64::MAX,
            ..Default::default()
        };
        let update_time = table.get_update_time();
        assert_eq!(update_time.unix_millis(), (1_i64 << 46) - 1);
        assert!(update_time.to_chrono_utc().is_some());
    }

    #[test]
    fn kind_predicates() {
        let mut t = TableInfo {
            name: CiString::new("t"),
            ..Default::default()
        };
        assert!(t.is_base_table());
        assert!(!t.is_view());
        assert!(!t.is_sequence());

        t.view = Some(Box::new(ViewInfo::default()));
        assert!(t.is_view());
        assert!(!t.is_base_table());

        t.view = None;
        t.sequence = Some(Box::new(SequenceInfo::default()));
        assert!(t.is_sequence());
    }

    #[test]
    fn pk_and_auto_random() {
        let mut t = TableInfo {
            columns: vec![
                {
                    let mut c = ColumnInfo::new_extra_handle_col_info();
                    c.name = CiString::new("data");
                    c.set_flag(0);
                    c
                },
                pk_col("id", true),
            ]
            .into(),
            ..Default::default()
        };
        assert_eq!(t.get_pk_name().original(), "id");
        assert!(t.get_pk_col_info().is_some());

        assert!(!t.contains_auto_random_bits());
        t.auto_random_bits = 5;
        assert!(t.contains_auto_random_bits());
        // Not a handle yet.
        assert!(!t.is_auto_random_bit_col_unsigned());
        t.pk_is_handle = true;
        assert!(t.is_auto_random_bit_col_unsigned()); // id is unsigned

        t.columns.clear();
        assert!(std::panic::catch_unwind(|| t.is_auto_random_bit_col_unsigned()).is_err());
    }

    fn column(name: &str, offset: i64, public: bool, not_null: bool) -> ColumnInfo {
        let mut c = ColumnInfo::new_extra_handle_col_info();
        c.name = CiString::new(name);
        c.offset = offset;
        c.field_type = FieldType::new(FieldTypeCode::LongLong);
        c.set_flag(if not_null {
            u64::from(FieldTypeFlags::NOT_NULL)
        } else {
            0
        });
        c.state = if public {
            SchemaState::PUBLIC
        } else {
            SchemaState::WRITE_ONLY
        };
        c
    }

    #[test]
    fn cols_and_primary_key() {
        use crate::index::{IndexColumn, IndexInfo};

        let t = TableInfo {
            columns: vec![
                column("a", 0, true, false),
                column("b", 1, true, true),
                column("c", 2, false, false), // non-public -> excluded
            ]
            .into(),
            indices: vec![IndexInfo {
                name: CiString::new("uk_b"),
                unique: true,
                columns: vec![IndexColumn {
                    name: CiString::new("b"),
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };

        // Public columns only, in offset order.
        let cols = t.cols();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols.get(0).unwrap().read().name.original(), "a");
        assert_eq!(cols.get(1).unwrap().read().name.original(), "b");
        assert!(t.find_public_column_by_name("b").is_some());
        assert!(t.find_public_column_by_name("c").is_none()); // not public

        // Implicit PK: the unique index over non-null public column b.
        let pk = t.get_primary_key().unwrap();
        assert_eq!(pk.read().name.original(), "uk_b");

        // An explicit primary index wins.
        let mut t2 = t.clone();
        let implicit = t2.indices.get(0).unwrap();
        t2.indices = GoSharedPointerSlice::from_handles(vec![
            Some(GoShared::new(IndexInfo {
                name: CiString::new("pk"),
                primary: true,
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            })),
            Some(implicit),
        ]);
        assert_eq!(t2.get_primary_key().unwrap().read().name.original(), "pk");
    }

    #[test]
    fn finders() {
        use crate::index::{IndexColumn, IndexInfo};

        let mut c_pub = column("a", 0, true, false);
        c_pub.id = 100;
        let mut c_hidden = column("b", 1, false, false);
        c_hidden.id = 101;
        c_hidden.set_flag(u64::from(FieldTypeFlags::AUTO_INCREMENT));

        let mut t = TableInfo {
            columns: vec![c_pub, c_hidden].into(),
            indices: vec![IndexInfo {
                id: 5,
                name: CiString::new("idx_a"),
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            }]
            .into(),
            constraints: vec![ConstraintInfo {
                name: CiString::new("chk1"),
                ..Default::default()
            }]
            .into(),
            pk_is_handle: true,
            ..Default::default()
        };

        assert_eq!(
            t.find_column_by_id(101).unwrap().read().name.original(),
            "b"
        );
        // get_column_by_id only returns public columns.
        assert!(t.get_column_by_id(101).is_none());
        assert_eq!(t.get_column_by_id(100).unwrap().read().name.original(), "a");
        assert!(t.find_index_by_name("idx_a").is_some());
        assert!(t.find_index_by_id(5).is_some());
        assert!(t.find_constraint_info_by_name("CHK1").is_some()); // case-insensitive
        t.constraints.get(0).unwrap().write().name = CiString::new("i");
        assert!(t.find_constraint_info_by_name("\u{130}").is_some());
        assert_eq!(
            t.get_auto_increment_col_info()
                .unwrap()
                .read()
                .name
                .original(),
            "b"
        );
        assert!(t.column_is_in_index(&t.columns.get(0).unwrap().read())); // "a" is in idx_a
        assert!(!t.column_is_in_index(&t.columns.get(1).unwrap().read())); // "b" is not
        assert!(t.has_clustered_index()); // pk_is_handle
    }

    #[test]
    fn misc_methods() {
        use crate::partition::{PartitionDefinition, PartitionInfo};
        use crate::placement::PolicyRefInfo;

        assert_eq!(CURR_LATEST_TABLE_INFO_VERSION, TABLE_INFO_VERSION5);

        let mut c = column("a", 0, true, false);
        c.id = 100;
        let t = TableInfo {
            columns: vec![c].into(),
            version: TABLE_INFO_VERSION5,
            auto_id_cache: 1,
            storage_class_tier: "STANDARD".into(),
            ..Default::default()
        };
        assert_eq!(t.find_column_name_by_id(100), "a");
        assert_eq!(t.find_column_name_by_id(999), "");
        assert!(t.sep_auto_inc()); // version 5 + cache 1
        assert_eq!(t.storage_class_string(), "STANDARD");
        // Non-temp columns exclude removing ones.
        assert_eq!(t.get_non_temp_columns().len(), 1);

        // clear_placement drops table + partition refs.
        let mut t2 = TableInfo {
            placement_policy_ref: Some(PolicyRefInfo::default()),
            partition: Some(Box::new(PartitionInfo {
                definitions: vec![PartitionDefinition {
                    placement_policy_ref: Some(PolicyRefInfo::default()),
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        t2.clear_placement();
        assert!(t2.placement_policy_ref.is_none());
        assert!(t2.partition.unwrap().definitions[0]
            .placement_policy_ref
            .is_none());
    }

    #[test]
    fn go_pointer_returning_helpers_have_write_through_surfaces() {
        use crate::partition::PartitionInfo;

        let mut primary_column = column("pk", 0, true, false);
        primary_column.id = 1;
        primary_column.set_flag(
            u64::from(FieldTypeFlags::PRI_KEY) | u64::from(FieldTypeFlags::AUTO_INCREMENT),
        );
        let mut table = TableInfo {
            columns: vec![primary_column].into(),
            indices: vec![IndexInfo {
                id: 10,
                name: CiString::new("primary"),
                primary: true,
                ..Default::default()
            }]
            .into(),
            constraints: vec![ConstraintInfo {
                name: CiString::new("check_a"),
                ..Default::default()
            }]
            .into(),
            partition: Some(Box::new(PartitionInfo {
                enable: true,
                ..Default::default()
            })),
            ..Default::default()
        };

        table.get_partition_info_mut().unwrap().expr = "p".to_owned();
        assert_eq!(table.partition.as_ref().unwrap().expr, "p");
        table.get_pk_col_info_mut().unwrap().write().comment = "pk".to_owned();
        table
            .get_auto_increment_col_info_mut()
            .unwrap()
            .write()
            .generated_stored = true;
        table
            .find_column_by_id_mut(1)
            .unwrap()
            .write()
            .default_is_expr = true;
        table.get_column_by_id_mut(1).unwrap().write().hidden = true;
        table
            .find_public_column_by_name_mut("pk")
            .unwrap()
            .write()
            .version = 7;
        {
            let columns = table.cols_mut();
            columns.get(0).unwrap().write().id = 2;
        }
        {
            let columns = table.present_cols();
            columns[0].write().id = 1;
        }
        {
            let columns = table.get_non_temp_columns_mut();
            columns[0].write().comment = "non-temp".to_owned();
        }
        assert_eq!(table.columns.get(0).unwrap().read().comment, "non-temp");
        assert!(table.columns.get(0).unwrap().read().generated_stored);
        assert!(table.columns.get(0).unwrap().read().default_is_expr);
        assert!(table.columns.get(0).unwrap().read().hidden);
        assert_eq!(table.columns.get(0).unwrap().read().version, 7);

        table.get_primary_key_mut().unwrap().write().comment = "primary".to_owned();
        table
            .find_index_by_name_mut("primary")
            .unwrap()
            .write()
            .invisible = true;
        table.find_index_by_id_mut(10).unwrap().write().global = true;
        assert_eq!(table.indices.get(0).unwrap().read().comment, "primary");
        assert!(table.indices.get(0).unwrap().read().invisible);
        assert!(table.indices.get(0).unwrap().read().global);

        table
            .find_constraint_info_by_name_mut("CHECK_A")
            .unwrap()
            .write()
            .expr_string = "a > 0".to_owned();
        assert_eq!(
            table.constraints.get(0).unwrap().read().expr_string,
            "a > 0"
        );
    }

    #[test]
    fn move_column() {
        use crate::index::{IndexColumn, IndexInfo};

        let mut columns = vec![
            column("a", 0, true, false),
            column("b", 1, true, false),
            column("c", 2, true, false),
        ];
        columns[2].change_state_info = Some(crate::column::ChangeStateInfo {
            dependency_column_offset: 0,
        });
        let mut t = TableInfo {
            columns: columns.into(),
            indices: vec![IndexInfo {
                // an index referencing column "a" at offset 0
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    offset: 0,
                    ..Default::default()
                }]
                .into(),
                affect_column: vec![IndexColumn {
                    name: CiString::new("a"),
                    offset: 0,
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        // Move "a" (offset 0) to the end (position 2).
        t.move_column_info(0, 2);
        assert_eq!(
            t.columns
                .iter_deref()
                .map(|column| column.read().name.original().to_owned())
                .collect::<Vec<_>>(),
            vec!["b".to_owned(), "c".to_owned(), "a".to_owned()]
        );
        assert_eq!(t.columns.get(0).unwrap().read().offset, 0); // b
        assert_eq!(t.columns.get(2).unwrap().read().offset, 2); // a
                                                                // The index column's offset was remapped 0 -> 2.
        assert_eq!(t.indices.get(0).unwrap().read().columns[0].offset, 2);
        assert_eq!(t.indices.get(0).unwrap().read().affect_column[0].offset, 2);
        assert_eq!(
            t.columns
                .get(1)
                .unwrap()
                .read()
                .change_state_info
                .as_ref()
                .unwrap()
                .dependency_column_offset,
            2
        );

        // Moving in the opposite direction remaps every dependent offset too.
        t.move_column_info(2, 0);
        assert_eq!(t.indices.get(0).unwrap().read().columns[0].offset, 0);
        assert_eq!(t.indices.get(0).unwrap().read().affect_column[0].offset, 0);
        assert_eq!(
            t.columns
                .get(2)
                .unwrap()
                .read()
                .change_state_info
                .as_ref()
                .unwrap()
                .dependency_column_offset,
            0
        );

        // A no-op move leaves everything unchanged.
        let before = t.columns.get(0).unwrap().read().name.original().to_owned();
        t.move_column_info(1, 1);
        assert_eq!(t.columns.get(0).unwrap().read().name.original(), before);
    }

    #[test]
    fn move_column_ports_the_exact_upstream_sequence_and_signed_panics() {
        fn make_column(id: i64) -> ColumnInfo {
            ColumnInfo {
                id,
                name: CiString::new(&format!("c_{id}")),
                offset: id,
                state: SchemaState::PUBLIC,
                ..Default::default()
            }
        }

        fn make_index(id: i64, ids: &[i64]) -> IndexInfo {
            IndexInfo {
                id,
                name: CiString::new(&format!("i_{id}")),
                columns: ids
                    .iter()
                    .map(|id| IndexColumn {
                        name: CiString::new(&format!("c_{id}")),
                        offset: *id,
                        ..Default::default()
                    })
                    .collect::<Vec<_>>()
                    .into(),
                ..Default::default()
            }
        }

        fn check_offsets(table: &TableInfo, ids: &[i64]) {
            assert_eq!(table.columns.len(), ids.len());
            for (offset, id) in ids.iter().copied().enumerate() {
                let column = table.columns.get(offset).expect("source column is non-nil");
                let column = column.read();
                assert_eq!(column.name.lowercase(), format!("c_{id}"));
                assert_eq!(column.offset, offset as i64);
            }
            for column in table.columns.iter_deref() {
                let column = column.read();
                for index in table.indices.iter_deref() {
                    let index = index.read();
                    for index_column in &index.columns {
                        if column.name.lowercase() == index_column.name.lowercase() {
                            assert_eq!(column.offset, index_column.offset);
                        }
                    }
                }
            }
        }

        let mut table = TableInfo {
            id: 1,
            name: CiString::new("t"),
            columns: (0..5).map(make_column).collect::<Vec<_>>().into(),
            indices: vec![
                make_index(0, &[0, 1, 2, 3, 4]),
                make_index(1, &[4, 2]),
                make_index(2, &[0, 4]),
                make_index(3, &[1, 2, 3]),
                make_index(4, &[3, 2, 1]),
            ]
            .into(),
            ..Default::default()
        };

        for (from, to, expected) in [
            (4, 0, vec![4, 0, 1, 2, 3]),
            (2, 3, vec![4, 0, 2, 1, 3]),
            (3, 2, vec![4, 0, 1, 2, 3]),
            (0, 4, vec![0, 1, 2, 3, 4]),
            (2, 2, vec![0, 1, 2, 3, 4]),
            (0, 0, vec![0, 1, 2, 3, 4]),
            (1, 4, vec![0, 2, 3, 4, 1]),
            (3, 0, vec![4, 0, 2, 3, 1]),
        ] {
            table.move_column_info(from, to);
            check_offsets(&table, &expected);
        }

        let before = table.columns.handles();
        table.move_column_info(-1, -1);
        for (before, after) in before.into_iter().zip(table.columns.handles()) {
            assert!(before.unwrap().ptr_eq(&after.unwrap()));
        }

        let mut invalid = TableInfo {
            columns: (0..3).map(make_column).collect::<Vec<_>>().into(),
            ..Default::default()
        };
        let invalid_alias = invalid.columns.clone();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            invalid.move_column_info(0, 3);
        }))
        .is_err());
        assert_eq!(
            invalid
                .columns
                .iter_deref()
                .map(|column| column.read().name.lowercase().to_owned())
                .collect::<Vec<_>>(),
            vec!["c_1", "c_2", "c_2"]
        );
        assert_eq!(invalid.columns.get(2).unwrap().read().offset, 1);
        assert!(invalid.columns.backing_ptr_eq(&invalid_alias));
        assert_eq!(
            invalid_alias
                .iter_deref()
                .map(|column| column.read().name.lowercase().to_owned())
                .collect::<Vec<_>>(),
            vec!["c_1", "c_2", "c_2"]
        );

        let mut negative = TableInfo {
            columns: (0..2).map(make_column).collect::<Vec<_>>().into(),
            ..Default::default()
        };
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            negative.move_column_info(-1, 0);
        }))
        .is_err());
        assert_eq!(
            negative.columns.get(0).unwrap().read().name.lowercase(),
            "c_0"
        );
    }

    #[test]
    fn partition_and_lock() {
        use crate::partition::PartitionInfo;
        use crate::table::{SessionInfo, TableLockInfo, TableLockState};

        let mut t = TableInfo::default();
        assert!(t.get_partition_info().is_none());
        let pi = PartitionInfo {
            enable: false,
            ..Default::default()
        };
        t.partition = Some(Box::new(pi));
        // Disabled partitioning still returns None.
        assert!(t.get_partition_info().is_none());
        t.partition.as_mut().unwrap().enable = true;
        assert!(t.get_partition_info().is_some());

        assert!(!t.is_locked());
        t.lock = Some(Box::new(TableLockInfo {
            tp: tidb_ast::TableLockType::default(),
            sessions: vec![SessionInfo::default()].into(),
            state: TableLockState::PUBLIC,
            ts: 0,
        }));
        assert!(t.is_locked());
    }
}
