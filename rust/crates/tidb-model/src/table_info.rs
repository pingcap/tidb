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

    /// Go `Cols`: the public columns, in offset order.
    ///
    /// Go builds an offset-indexed slice of the public columns and returns it
    /// up to the max offset; a gap (a non-public column at a lower offset than
    /// a public one, a transient DDL state) would leave a `nil` in Go's result
    /// and panic any consumer. Here the present columns are collected in
    /// offset order, matching Go for the normal contiguous case.
    #[must_use]
    pub fn cols(&self) -> Vec<&ColumnInfo> {
        let mut slots: Vec<Option<&ColumnInfo>> = vec![None; self.columns.len()];
        let mut max_offset: i64 = -1;
        for col in &self.columns {
            if col.state != SchemaState::PUBLIC {
                continue;
            }
            let off = col.offset as usize;
            if off < slots.len() {
                slots[off] = Some(col);
            }
            if i64::from(col.offset) > max_offset {
                max_offset = i64::from(col.offset);
            }
        }
        slots
            .into_iter()
            .take((max_offset + 1) as usize)
            .flatten()
            .collect()
    }

    /// Go `FindPublicColumnByName`: the public column named `col_name_l`
    /// (already lower-cased).
    #[must_use]
    pub fn find_public_column_by_name(&self, col_name_l: &str) -> Option<&ColumnInfo> {
        self.cols()
            .into_iter()
            .find(|c| c.name.lowercase() == col_name_l)
    }

    /// Go `GetPrimaryKey`: the explicit primary index, else an implicit one
    /// (a unique index over only non-null, non-hidden public columns).
    #[must_use]
    pub fn get_primary_key(&self) -> Option<&IndexInfo> {
        let cols = self.cols();
        let mut implicit_pk: Option<&IndexInfo> = None;
        for key in &self.indices {
            if key.primary {
                return Some(key);
            }
            if key.columns.is_empty() {
                continue;
            }
            if implicit_pk.is_none() && key.unique {
                let mut all_col_not_null = true;
                let mut skip = false;
                for idx_col in &key.columns {
                    let col = cols
                        .iter()
                        .find(|c| c.name.lowercase() == idx_col.name.lowercase());
                    match col {
                        None => {
                            skip = true;
                            break;
                        }
                        Some(c) if c.hidden => {
                            skip = true;
                            break;
                        }
                        Some(c) => {
                            if c.get_flag() & FieldTypeFlags::NOT_NULL == 0 {
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
                    implicit_pk = Some(key);
                }
            }
        }
        implicit_pk
    }

    /// Go `FindColumnByID`: the column with `id` (any state).
    #[must_use]
    pub fn find_column_by_id(&self, id: i64) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.id == id)
    }

    /// Go `GetColumnByID`: the public column with `id`.
    #[must_use]
    pub fn get_column_by_id(&self, id: i64) -> Option<&ColumnInfo> {
        self.columns
            .iter()
            .find(|c| c.state == SchemaState::PUBLIC && c.id == id)
    }

    /// Go `FindIndexByName`: the index named `idx_name` (already lower-cased).
    #[must_use]
    pub fn find_index_by_name(&self, idx_name: &str) -> Option<&IndexInfo> {
        self.indices.iter().find(|i| i.name.lowercase() == idx_name)
    }

    /// Go `FindIndexByID`: the index with `id`.
    #[must_use]
    pub fn find_index_by_id(&self, id: i64) -> Option<&IndexInfo> {
        self.indices.iter().find(|i| i.id == id)
    }

    /// Go `FindConstraintInfoByName`: the CHECK constraint named `constr_name`
    /// (case-insensitive).
    #[must_use]
    pub fn find_constraint_info_by_name(&self, constr_name: &str) -> Option<&ConstraintInfo> {
        let low = constr_name.to_lowercase();
        self.constraints.iter().find(|c| c.name.lowercase() == low)
    }

    /// Go `GetAutoIncrementColInfo`: the auto-increment column, if any.
    #[must_use]
    pub fn get_auto_increment_col_info(&self) -> Option<&ColumnInfo> {
        self.columns
            .iter()
            .find(|c| c.get_flag() & FieldTypeFlags::AUTO_INCREMENT != 0)
    }

    /// Go `ColumnIsInIndex`: whether column `c` participates in any index.
    #[must_use]
    pub fn column_is_in_index(&self, c: &ColumnInfo) -> bool {
        self.indices.iter().any(|index| {
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

    fn column(name: &str, offset: i32, public: bool, not_null: bool) -> ColumnInfo {
        let mut c = ColumnInfo::new_extra_handle_col_info();
        c.name = CiString::new(name);
        c.offset = offset;
        c.field_type = FieldType::new(FieldTypeCode::LongLong);
        c.set_flag(if not_null {
            FieldTypeFlags::NOT_NULL
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
            ],
            indices: vec![IndexInfo {
                name: CiString::new("uk_b"),
                unique: true,
                columns: vec![IndexColumn {
                    name: CiString::new("b"),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        // Public columns only, in offset order.
        let cols = t.cols();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name.original(), "a");
        assert_eq!(cols[1].name.original(), "b");
        assert!(t.find_public_column_by_name("b").is_some());
        assert!(t.find_public_column_by_name("c").is_none()); // not public

        // Implicit PK: the unique index over non-null public column b.
        let pk = t.get_primary_key().unwrap();
        assert_eq!(pk.name.original(), "uk_b");

        // An explicit primary index wins.
        let mut t2 = t.clone();
        t2.indices.insert(
            0,
            IndexInfo {
                name: CiString::new("pk"),
                primary: true,
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        assert_eq!(t2.get_primary_key().unwrap().name.original(), "pk");
    }

    #[test]
    fn finders() {
        use crate::index::{IndexColumn, IndexInfo};

        let mut c_pub = column("a", 0, true, false);
        c_pub.id = 100;
        let mut c_hidden = column("b", 1, false, false);
        c_hidden.id = 101;
        c_hidden.set_flag(FieldTypeFlags::AUTO_INCREMENT);

        let t = TableInfo {
            columns: vec![c_pub, c_hidden],
            indices: vec![IndexInfo {
                id: 5,
                name: CiString::new("idx_a"),
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            constraints: vec![ConstraintInfo {
                name: CiString::new("chk1"),
                ..Default::default()
            }],
            pk_is_handle: true,
            ..Default::default()
        };

        assert_eq!(t.find_column_by_id(101).unwrap().name.original(), "b");
        // get_column_by_id only returns public columns.
        assert!(t.get_column_by_id(101).is_none());
        assert_eq!(t.get_column_by_id(100).unwrap().name.original(), "a");
        assert!(t.find_index_by_name("idx_a").is_some());
        assert!(t.find_index_by_id(5).is_some());
        assert!(t.find_constraint_info_by_name("CHK1").is_some()); // case-insensitive
        assert_eq!(
            t.get_auto_increment_col_info().unwrap().name.original(),
            "b"
        );
        assert!(t.column_is_in_index(&t.columns[0])); // "a" is in idx_a
        assert!(!t.column_is_in_index(&t.columns[1])); // "b" is not
        assert!(t.has_clustered_index()); // pk_is_handle
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
