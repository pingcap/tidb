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
//! struct. All of its field types are ported (see the other modules).
//!
//! The struct and its simple, self-contained methods are ported here. Its
//! many remaining methods (public-column projection `Cols`, index/handle
//! helpers, DDL-oriented logic, etc.) are a following tranche.

use chrono::{DateTime, Utc};
use tidb_ast::CiString;
use tidb_datatype::FieldTypeFlags;

use crate::bdr::ts_convert_2_time;
use crate::column::ColumnInfo;
use crate::engine_attribute::StorageClassTransitRule;
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

/// Go `TableInfo`: metadata describing a table.
///
/// Go's pointer sub-structs (`*PartitionInfo`, `*ViewInfo`, ...) become
/// `Option<Box<..>>`; its two embedded fields (`TempTableType`,
/// `TableCacheStatusType`) become named fields.
#[derive(Clone, Debug, Default)]
pub struct TableInfo {
    /// The table ID.
    pub id: i64,
    /// The table name.
    pub name: CiString,
    /// The table charset.
    pub charset: String,
    /// The table collation.
    pub collate: String,
    /// The columns.
    pub columns: Vec<ColumnInfo>,
    /// The indexes.
    pub indices: Vec<IndexInfo>,
    /// The CHECK constraints.
    pub constraints: Vec<ConstraintInfo>,
    /// The foreign keys.
    pub foreign_keys: Vec<FKInfo>,
    /// The online-DDL state.
    pub state: SchemaState,
    /// Whether the primary key is the handle.
    pub pk_is_handle: bool,
    /// Whether the table uses a clustered common handle.
    pub is_common_handle: bool,
    /// The common-handle version.
    pub common_handle_version: u16,
    /// The table comment.
    pub comment: String,
    /// The next auto-increment ID.
    pub auto_inc_id: i64,
    /// An extra auto-increment ID reservation.
    pub auto_inc_id_extra: i64,
    /// The auto-ID cache size.
    pub auto_id_cache: i64,
    /// The next auto-random ID.
    pub auto_rand_id: i64,
    /// The maximum column ID.
    pub max_column_id: i64,
    /// The maximum index ID.
    pub max_index_id: i64,
    /// The maximum foreign-key ID.
    pub max_foreign_key_id: i64,
    /// The maximum constraint ID.
    pub max_constraint_id: i64,
    /// The last-update timestamp (a TSO).
    pub update_ts: u64,
    /// The schema ID that owns the auto-ID (for `RENAME`).
    pub auto_id_schema_id: i64,
    /// The `SHARD_ROW_ID_BITS` setting.
    pub shard_row_id_bits: u64,
    /// The maximum `SHARD_ROW_ID_BITS`.
    pub max_shard_row_id_bits: u64,
    /// The `AUTO_RANDOM` bit count.
    pub auto_random_bits: u64,
    /// The `AUTO_RANDOM` range-bit count.
    pub auto_random_range_bits: u64,
    /// The pre-split region count.
    pub pre_split_regions: u64,
    /// The partitioning metadata.
    pub partition: Option<Box<PartitionInfo>>,
    /// The compression setting.
    pub compression: String,
    /// The view metadata, if this is a view.
    pub view: Option<Box<ViewInfo>>,
    /// The sequence metadata, if this is a sequence.
    pub sequence: Option<Box<SequenceInfo>>,
    /// The table lock, if held.
    pub lock: Option<Box<TableLockInfo>>,
    /// The table-info version.
    pub version: u16,
    /// The TiFlash replica configuration.
    pub tiflash_replica: Option<Box<TiFlashReplicaInfo>>,
    /// Whether the table is columnar.
    pub is_columnar: bool,
    /// The temporary-table kind (Go's embedded `TempTableType`).
    pub temp_table_type: TempTableType,
    /// The cache status (Go's embedded `TableCacheStatusType`).
    pub table_cache_status_type: TableCacheStatusType,
    /// The placement-policy reference.
    pub placement_policy_ref: Option<PolicyRefInfo>,
    /// The persisted ANALYZE options.
    pub stats_options: Option<Box<StatsOptions>>,
    /// In-progress partition-exchange metadata.
    pub exchange_partition_info: Option<Box<ExchangePartitionInfo>>,
    /// The TTL configuration.
    pub ttl_info: Option<Box<TTLInfo>>,
    /// Whether the table is active-active.
    pub is_active_active: bool,
    /// The soft-delete configuration.
    pub softdelete_info: Option<Box<SoftdeleteInfo>>,
    /// The affinity configuration.
    pub affinity: Option<Box<TableAffinityInfo>>,
    /// The persistent region-split policy.
    pub table_split_policy: Option<Box<RegionSplitPolicy>>,
    /// The schema revision.
    pub revision: u64,
    /// The owning database ID (not serialized).
    pub db_id: i64,
    /// The `ENGINE_ATTRIBUTE` value.
    pub engine_attribute: String,
    /// The storage-class tier.
    pub storage_class_tier: String,
    /// The storage-class transitions.
    pub storage_class_transitions: Vec<StorageClassTransitRule>,
    /// The table mode (normal/import/restore).
    pub mode: TableMode,
}

impl TableInfo {
    /// Go `GetPartitionInfo`: the partition info when partitioning is enabled.
    #[must_use]
    pub fn get_partition_info(&self) -> Option<&PartitionInfo> {
        match &self.partition {
            Some(p) if p.enable => Some(p),
            _ => None,
        }
    }

    /// Go `GetUpdateTime`: the last-update time (from the `update_ts` TSO).
    #[must_use]
    pub fn get_update_time(&self) -> DateTime<Utc> {
        ts_convert_2_time(self.update_ts)
    }

    /// Go `GetPkColInfo`: the primary-key column (by the PRI-KEY flag).
    #[must_use]
    pub fn get_pk_col_info(&self) -> Option<&ColumnInfo> {
        self.columns
            .iter()
            .find(|c| c.get_flag() & FieldTypeFlags::PRI_KEY != 0)
    }

    /// Go `GetPkName`: the primary-key column name (empty when none).
    #[must_use]
    pub fn get_pk_name(&self) -> CiString {
        self.get_pk_col_info()
            .map_or_else(CiString::default, |c| c.name.clone())
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
            .is_some_and(|c| c.get_flag() & FieldTypeFlags::UNSIGNED != 0)
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
        c.set_flag(FieldTypeFlags::PRI_KEY);
        if unsigned {
            c.add_flag(FieldTypeFlags::UNSIGNED);
        }
        c
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
            ],
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
            sessions: vec![SessionInfo::default()],
            state: TableLockState::PUBLIC,
            ts: 0,
        }));
        assert!(t.is_locked());
    }
}
