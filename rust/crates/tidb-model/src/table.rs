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

use tidb_ast::TableLockType;

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
    fn session_info_string() {
        let s = SessionInfo {
            server_id: "s1".to_owned(),
            session_id: 42,
        };
        assert_eq!(s.to_string(), "server: s1_session: 42");
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
