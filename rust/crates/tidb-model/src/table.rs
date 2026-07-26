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

//! Self-contained pieces of `pkg/meta/model/table.go`: the cache-status /
//! temp-table / table-lock enums, `SessionInfo`, `TiFlashReplicaInfo`, the
//! table-lock info structs, and the sequence-default constants.
//!
//! DEFERRED (the keystone): the `TableInfo` struct itself and its ~50 methods,
//! `PartitionInfo`, `ViewInfo`, `SequenceInfo`, and the other sub-structs.
//! `TableInfo` gates DBInfo and much of meta/model; it is being approached
//! bottom-up from these leaves.

use tidb_ast::{CiString, TableLockType};

use crate::schema_state::SchemaState;

/// Go `ConstraintInfo`: a table CHECK constraint.
#[derive(Clone, Debug, Default)]
pub struct ConstraintInfo {
    /// The constraint ID.
    pub id: i64,
    /// The constraint name.
    pub name: CiString,
    /// The table name.
    pub table: CiString,
    /// The columns the constraint depends on.
    pub constraint_cols: Vec<CiString>,
    /// Whether the constraint is enforced.
    pub enforced: bool,
    /// Whether it is a column-level check.
    pub in_column: bool,
    /// The constraint expression.
    pub expr_string: String,
    /// The online-DDL state of the constraint.
    pub state: SchemaState,
}

/// Go `FKVersion0`: foreign-key syntax accepted but not enforced.
pub const FK_VERSION0: i64 = 0;
/// Go `FKVersion1`: foreign-key constraint enforced.
pub const FK_VERSION1: i64 = 1;

/// Go `FKInfo`: a foreign-key constraint.
#[derive(Clone, Debug, Default)]
pub struct FKInfo {
    /// The foreign-key ID.
    pub id: i64,
    /// The foreign-key name.
    pub name: CiString,
    /// The referenced schema.
    pub ref_schema: CiString,
    /// The referenced table.
    pub ref_table: CiString,
    /// The referenced columns.
    pub ref_cols: Vec<CiString>,
    /// The referencing columns.
    pub cols: Vec<CiString>,
    /// The `ON DELETE` action (an `ast.ReferOptionType` value).
    pub on_delete: i64,
    /// The `ON UPDATE` action (an `ast.ReferOptionType` value).
    pub on_update: i64,
    /// The online-DDL state.
    pub state: SchemaState,
    /// The FK version (see `FK_VERSION*`).
    pub version: i64,
}

// Mirrors `ast.ReferOptionType.String` for the int-valued FKInfo.On{Delete,
// Update} (ReferOptionType is not yet in tidb-ast). NoOption(0)/unknown -> "".
fn refer_option_string(opt: i64) -> &'static str {
    match opt {
        1 => "RESTRICT",
        2 => "CASCADE",
        3 => "SET NULL",
        4 => "NO ACTION",
        5 => "SET DEFAULT",
        _ => "",
    }
}

impl FKInfo {
    /// Go `FKInfo.String`: the `db`.`tb`, CONSTRAINT ... FOREIGN KEY clause.
    /// The referencing columns use their original case; the referenced
    /// schema/table use their lower-case form, and the schema is omitted when
    /// it equals `db` (all matching Go).
    #[must_use]
    pub fn string(&self, db: &str, tb: &str) -> String {
        let mut buf = String::new();
        buf.push('`');
        buf.push_str(db);
        buf.push_str("`.`");
        buf.push_str(tb);
        buf.push_str("`, CONSTRAINT `");
        buf.push_str(self.name.original());
        buf.push_str("` FOREIGN KEY (");
        for (i, col) in self.cols.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            buf.push('`');
            buf.push_str(col.original());
            buf.push('`');
        }
        buf.push_str(") REFERENCES `");
        if self.ref_schema.lowercase() != db {
            buf.push_str(self.ref_schema.lowercase());
            buf.push_str("`.`");
        }
        buf.push_str(self.ref_table.lowercase());
        buf.push_str("` (");
        for (i, col) in self.ref_cols.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            buf.push('`');
            buf.push_str(col.original());
            buf.push('`');
        }
        buf.push(')');
        // Go tests the numeric value against ReferOptionNoOption (0).
        if self.on_delete != 0 {
            buf.push_str(" ON DELETE ");
            buf.push_str(refer_option_string(self.on_delete));
        }
        if self.on_update != 0 {
            buf.push_str(" ON UPDATE ");
            buf.push_str(refer_option_string(self.on_update));
        }
        buf
    }
}

/// Go `ReferredFKInfo`: a foreign key in a child table that cites this table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReferredFKInfo {
    /// The referenced columns.
    pub cols: Vec<CiString>,
    /// The child schema.
    pub child_schema: CiString,
    /// The child table.
    pub child_table: CiString,
    /// The child foreign-key name.
    pub child_fk_name: CiString,
}

/// Go `TTLInfo`: a table's TTL (time-to-live) configuration.
///
/// `get_job_interval` (Go, which parses `job_interval` via
/// `time.ParseDuration`) is deferred until a Go duration parser is available
/// at this layer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TTLInfo {
    /// The TTL column name.
    pub column_name: CiString,
    /// The TTL interval expression.
    pub interval_expr_str: String,
    /// The interval time unit (an `ast.TimeUnitType` value).
    pub interval_time_unit: i64,
    /// Whether TTL is enabled.
    pub enable: bool,
    /// The background-job interval.
    pub job_interval: String,
}

/// Go `SequenceInfo`: a sequence object's configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SequenceInfo {
    /// The start value.
    pub start: i64,
    /// Whether values are cached.
    pub cache: bool,
    /// Whether the sequence cycles.
    pub cycle: bool,
    /// The minimum value.
    pub min_value: i64,
    /// The maximum value.
    pub max_value: i64,
    /// The increment.
    pub increment: i64,
    /// The cache size.
    pub cache_value: i64,
    /// The sequence comment.
    pub comment: String,
}

/// Go `ExchangePartitionInfo`: the partition-exchange metadata of a table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExchangePartitionInfo {
    /// The other table's ID (the non-partitioned table's ID when this info is
    /// on a partitioned table, else the partitioned table's ID).
    pub exchange_partition_table_id: i64,
    /// The exchanged partition definition ID.
    pub exchange_partition_def_id: i64,
    /// Deprecated, unused.
    pub xxx_exchange_partition_flag: bool,
}

/// Go `SoftdeleteInfo`: a table's soft-delete configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SoftdeleteInfo {
    /// The retention period.
    pub retention: String,
    /// Whether the purge job is enabled.
    pub job_enable: bool,
    /// The purge-job interval.
    pub job_interval: String,
}

/// Go `TableAffinityInfo`: a table's affinity configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableAffinityInfo {
    /// The affinity level.
    pub level: String,
}

/// Go `TableCacheStatusType` (an `int`): the caching state of a table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableCacheStatusType(pub i64);

impl TableCacheStatusType {
    /// Caching disabled (Go `TableCacheStatusDisable`, the zero value).
    pub const DISABLE: TableCacheStatusType = TableCacheStatusType(0);
    /// Caching enabled (Go `TableCacheStatusEnable`).
    pub const ENABLE: TableCacheStatusType = TableCacheStatusType(1);
    /// Caching state switching (Go `TableCacheStatusSwitching`).
    pub const SWITCHING: TableCacheStatusType = TableCacheStatusType(2);
}

impl std::fmt::Display for TableCacheStatusType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            TableCacheStatusType::DISABLE => "disable",
            TableCacheStatusType::ENABLE => "enable",
            TableCacheStatusType::SWITCHING => "switching",
            _ => "",
        })
    }
}

/// Go `TempTableType` (a `byte`): whether/how a table is temporary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TempTableType(pub u8);

impl TempTableType {
    /// Not a temporary table (Go `TempTableNone`, the zero value).
    pub const NONE: TempTableType = TempTableType(0);
    /// A global temporary table (Go `TempTableGlobal`).
    pub const GLOBAL: TempTableType = TempTableType(1);
    /// A local temporary table (Go `TempTableLocal`).
    pub const LOCAL: TempTableType = TempTableType(2);
}

impl std::fmt::Display for TempTableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            TempTableType::GLOBAL => "global",
            TempTableType::LOCAL => "local",
            _ => "",
        })
    }
}

/// Go `TableLockState` (a `byte`): the state of a table lock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableLockState(pub u8);

impl TableLockState {
    /// The lock is absent (Go `TableLockStateNone`, the zero value).
    pub const NONE: TableLockState = TableLockState(0);
    /// The lock is pre-locked (Go `TableLockStatePreLock`).
    pub const PRE_LOCK: TableLockState = TableLockState(1);
    /// The lock is public (Go `TableLockStatePublic`).
    pub const PUBLIC: TableLockState = TableLockState(2);
}

impl std::fmt::Display for TableLockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            TableLockState::PRE_LOCK => "pre-lock",
            TableLockState::PUBLIC => "public",
            // TableLockStateNone and any unknown value.
            _ => "none",
        })
    }
}

/// Go `SessionInfo`: a server/session identifier holding a table lock.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionInfo {
    /// The server ID.
    pub server_id: String,
    /// The session ID.
    pub session_id: u64,
}

impl std::fmt::Display for SessionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server: {}_session: {}", self.server_id, self.session_id)
    }
}

/// Go `TableLockInfo`: the lock held on a table.
#[derive(Clone, Debug)]
pub struct TableLockInfo {
    /// The lock type.
    pub tp: TableLockType,
    /// The sessions holding the lock.
    pub sessions: Vec<SessionInfo>,
    /// The lock state.
    pub state: TableLockState,
    /// The lock timestamp.
    pub ts: u64,
}

/// Go `TableLockTpInfo`: a schema/table/lock-type triple.
#[derive(Clone, Copy, Debug)]
pub struct TableLockTpInfo {
    /// The schema ID.
    pub schema_id: i64,
    /// The table ID.
    pub table_id: i64,
    /// The lock type.
    pub tp: TableLockType,
}

/// Go `TiFlashReplicaInfo`: a table's TiFlash replica configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TiFlashReplicaInfo {
    /// The replica count.
    pub count: u64,
    /// The location labels.
    pub location_labels: Vec<String>,
    /// Whether the replica is available.
    pub available: bool,
    /// The IDs of partitions whose replicas are available.
    pub available_partition_ids: Vec<i64>,
}

impl TiFlashReplicaInfo {
    /// Go `IsPartitionAvailable`: whether partition `pid`'s replica is ready.
    #[must_use]
    pub fn is_partition_available(&self, pid: i64) -> bool {
        self.available_partition_ids.contains(&pid)
    }
}

// Sequence default constants (Go's `DefaultSequence*`).
/// Default `CACHE` on/off.
pub const DEFAULT_SEQUENCE_CACHE_BOOL: bool = true;
/// Default `CYCLE` on/off.
pub const DEFAULT_SEQUENCE_CYCLE_BOOL: bool = false;
/// Default `ORDER` on/off.
pub const DEFAULT_SEQUENCE_ORDER_BOOL: bool = false;
/// Default cache size.
pub const DEFAULT_SEQUENCE_CACHE_VALUE: i64 = 1000;
/// Default increment.
pub const DEFAULT_SEQUENCE_INCREMENT_VALUE: i64 = 1;
/// Default start value for a positive-increment sequence.
pub const DEFAULT_POSITIVE_SEQUENCE_START_VALUE: i64 = 1;
/// Default start value for a negative-increment sequence.
pub const DEFAULT_NEGATIVE_SEQUENCE_START_VALUE: i64 = -1;
/// Default min value for a positive-increment sequence.
pub const DEFAULT_POSITIVE_SEQUENCE_MIN_VALUE: i64 = 1;
/// Default max value for a positive-increment sequence.
pub const DEFAULT_POSITIVE_SEQUENCE_MAX_VALUE: i64 = 9_223_372_036_854_775_806;
/// Default max value for a negative-increment sequence.
pub const DEFAULT_NEGATIVE_SEQUENCE_MAX_VALUE: i64 = -1;
/// Default min value for a negative-increment sequence.
pub const DEFAULT_NEGATIVE_SEQUENCE_MIN_VALUE: i64 = -9_223_372_036_854_775_807;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enum_strings() {
        assert_eq!(TableCacheStatusType::DISABLE.to_string(), "disable");
        assert_eq!(TableCacheStatusType::ENABLE.to_string(), "enable");
        assert_eq!(TableCacheStatusType::SWITCHING.to_string(), "switching");
        assert_eq!(TableCacheStatusType(9).to_string(), "");

        assert_eq!(TempTableType::NONE.to_string(), "");
        assert_eq!(TempTableType::GLOBAL.to_string(), "global");
        assert_eq!(TempTableType::LOCAL.to_string(), "local");

        assert_eq!(TableLockState::NONE.to_string(), "none");
        assert_eq!(TableLockState::PRE_LOCK.to_string(), "pre-lock");
        assert_eq!(TableLockState::PUBLIC.to_string(), "public");
        assert_eq!(TableLockState(9).to_string(), "none");
    }

    #[test]
    fn fk_string() {
        let fk = FKInfo {
            name: CiString::new("fk1"),
            ref_schema: CiString::new("db2"),
            ref_table: CiString::new("parent"),
            ref_cols: vec![CiString::new("id"), CiString::new("x")],
            cols: vec![CiString::new("a"), CiString::new("b")],
            on_delete: 2, // CASCADE
            on_update: 0, // NoOption
            ..Default::default()
        };
        assert_eq!(
            fk.string("db1", "child"),
            "`db1`.`child`, CONSTRAINT `fk1` FOREIGN KEY (`a`, `b`) REFERENCES \
             `db2`.`parent` (`id`, `x`) ON DELETE CASCADE"
        );

        // Same-schema reference omits the schema; ON UPDATE included.
        let fk = FKInfo {
            name: CiString::new("fk2"),
            ref_schema: CiString::new("db1"),
            ref_table: CiString::new("parent"),
            ref_cols: vec![CiString::new("id")],
            cols: vec![CiString::new("pid")],
            on_delete: 0,
            on_update: 1, // RESTRICT
            ..Default::default()
        };
        assert_eq!(
            fk.string("db1", "child"),
            "`db1`.`child`, CONSTRAINT `fk2` FOREIGN KEY (`pid`) REFERENCES \
             `parent` (`id`) ON UPDATE RESTRICT"
        );
    }

    #[test]
    fn session_info_string() {
        let s = SessionInfo {
            server_id: "s1".to_owned(),
            session_id: 42,
        };
        assert_eq!(s.to_string(), "server: s1_session: 42");
    }

    #[test]
    fn data_structs_clone() {
        let ttl = TTLInfo {
            column_name: CiString::new("t"),
            enable: true,
            ..Default::default()
        };
        assert_eq!(ttl.clone(), ttl);

        let seq = SequenceInfo {
            start: 1,
            max_value: 100,
            ..Default::default()
        };
        assert_eq!(seq.clone().max_value, 100);

        let ep = ExchangePartitionInfo {
            exchange_partition_table_id: 5,
            ..Default::default()
        };
        assert_eq!(ep, ep.clone());

        let rfk = ReferredFKInfo {
            child_table: CiString::new("child"),
            ..Default::default()
        };
        assert_eq!(rfk.child_table.original(), "child");
    }

    #[test]
    fn tiflash_partition_available() {
        let tr = TiFlashReplicaInfo {
            count: 1,
            available_partition_ids: vec![3, 7, 11],
            ..Default::default()
        };
        assert!(tr.is_partition_available(7));
        assert!(!tr.is_partition_available(5));
    }
}
