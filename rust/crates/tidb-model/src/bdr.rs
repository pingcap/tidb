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

//! `pkg/meta/model/bdr.go`: classifying DDL actions by their safety under
//! bidirectional replication (BDR).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::LazyLock;

use crate::action_type::ActionType;
use crate::go_runtime::{GoShared, GoSharedSlice, GoTime};

/// Go `DDLBDRType` (a `string`): a DDL's safety class under BDR.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DDLBDRType(Cow<'static, str>);

impl DDLBDRType {
    /// The DDL can't be run by a user on a Primary/Secondary cluster.
    pub const UNSAFE_DDL: DDLBDRType = DDLBDRType(Cow::Borrowed("unsafe DDL"));
    /// The DDL can be run by a user on a Primary cluster.
    pub const SAFE_DDL: DDLBDRType = DDLBDRType(Cow::Borrowed("safe DDL"));
    /// The DDL can't be synced by CDC.
    pub const UNMANAGEMENT_DDL: DDLBDRType = DDLBDRType(Cow::Borrowed("unmanagement DDL"));
    /// The DDL is unknown.
    pub const UNKNOWN_DDL: DDLBDRType = DDLBDRType(Cow::Borrowed("unknown DDL"));

    /// Constructs an arbitrary Go string value. `DDLBDRType` is a named
    /// string, not a closed enum; zero and future values must round-trip.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(Cow::Owned(value.into()))
    }

    /// Returns the underlying Go string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for DDLBDRType {
    fn default() -> Self {
        Self(Cow::Borrowed(""))
    }
}

impl From<String> for DDLBDRType {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

impl std::fmt::Display for DDLBDRType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Immutable declaration input used by Go package initialization.
const BDR_ACTION_ENTRIES: &[(DDLBDRType, &[ActionType])] = &[
    (
        DDLBDRType::SAFE_DDL,
        &[
            ActionType::ACTION_CREATE_SCHEMA,
            ActionType::ACTION_CREATE_TABLE,
            ActionType::ACTION_ADD_COLUMN,
            ActionType::ACTION_ADD_INDEX,
            ActionType::ACTION_DROP_INDEX,
            ActionType::ACTION_MODIFY_COLUMN,
            ActionType::ACTION_SET_DEFAULT_VALUE,
            ActionType::ACTION_MODIFY_TABLE_COMMENT,
            ActionType::ACTION_RENAME_INDEX,
            ActionType::ACTION_ADD_TABLE_PARTITION,
            ActionType::ACTION_DROP_PRIMARY_KEY,
            ActionType::ACTION_ALTER_INDEX_VISIBILITY,
            ActionType::ACTION_CREATE_TABLES,
            ActionType::ACTION_ALTER_TTLINFO,
            ActionType::ACTION_ALTER_TTLREMOVE,
            ActionType::ACTION_CREATE_VIEW,
            ActionType::ACTION_DROP_VIEW,
            ActionType::ACTION_ALTER_TABLE_AFFINITY,
        ],
    ),
    (
        DDLBDRType::UNSAFE_DDL,
        &[
            ActionType::ACTION_DROP_SCHEMA,
            ActionType::ACTION_DROP_TABLE,
            ActionType::ACTION_DROP_COLUMN,
            ActionType::ACTION_ADD_FOREIGN_KEY,
            ActionType::ACTION_DROP_FOREIGN_KEY,
            ActionType::ACTION_TRUNCATE_TABLE,
            ActionType::ACTION_REBASE_AUTO_ID,
            ActionType::ACTION_RENAME_TABLE,
            ActionType::ACTION_SHARD_ROW_ID,
            ActionType::ACTION_DROP_TABLE_PARTITION,
            ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE,
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
            ActionType::ACTION_RECOVER_TABLE,
            ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
            ActionType::ACTION_LOCK_TABLE,
            ActionType::ACTION_UNLOCK_TABLE,
            ActionType::ACTION_REPAIR_TABLE,
            ActionType::ACTION_SET_TI_FLASH_REPLICA,
            ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS,
            ActionType::ACTION_ADD_PRIMARY_KEY,
            ActionType::ACTION_CREATE_SEQUENCE,
            ActionType::ACTION_ALTER_SEQUENCE,
            ActionType::ACTION_DROP_SEQUENCE,
            ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE,
            ActionType::ACTION_REBASE_AUTO_RANDOM_BASE,
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION,
            ActionType::ACTION_ADD_CHECK_CONSTRAINT,
            ActionType::ACTION_DROP_CHECK_CONSTRAINT,
            ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
            ActionType::ACTION_RENAME_TABLES,
            ActionType::ACTION_ALTER_TABLE_ATTRIBUTES,
            ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES,
            ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
            ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT,
            ActionType::ACTION_ALTER_TABLE_PLACEMENT,
            ActionType::ACTION_ALTER_CACHE_TABLE,
            ActionType::ACTION_ALTER_TABLE_STATS_OPTIONS,
            ActionType::ACTION_ALTER_NO_CACHE_TABLE,
            ActionType::ACTION_MULTI_SCHEMA_CHANGE,
            ActionType::ACTION_FLASHBACK_CLUSTER,
            ActionType::ACTION_RECOVER_SCHEMA,
            ActionType::ACTION_REORGANIZE_PARTITION,
            ActionType::ACTION_ALTER_TABLE_PARTITIONING,
            ActionType::ACTION_REMOVE_PARTITIONING,
            ActionType::ACTION_ADD_COLUMNAR_INDEX,
            ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE,
            ActionType::ACTION_ALTER_TABLE_MODE,
            ActionType::ACTION_REFRESH_META,
            ActionType::ACTION_MODIFY_SCHEMA_READ_ONLY,
            ActionType::ACTION_MODIFY_SCHEMA_SOFT_DELETE_AND_ACTIVE_ACTIVE,
            ActionType::ACTION_ALTER_TABLE_SOFT_DELETE_INFO,
            ActionType::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY,
        ],
    ),
    (
        DDLBDRType::UNMANAGEMENT_DDL,
        &[
            ActionType::ACTION_CREATE_PLACEMENT_POLICY,
            ActionType::ACTION_ALTER_PLACEMENT_POLICY,
            ActionType::ACTION_DROP_PLACEMENT_POLICY,
            ActionType::ACTION_CREATE_MASKING_POLICY,
            ActionType::ACTION_ALTER_MASKING_POLICY,
            ActionType::ACTION_DROP_MASKING_POLICY,
            ActionType::ACTION_CREATE_RESOURCE_GROUP,
            ActionType::ACTION_ALTER_RESOURCE_GROUP,
            ActionType::ACTION_DROP_RESOURCE_GROUP,
        ],
    ),
    (
        DDLBDRType::UNKNOWN_DDL,
        &[ActionType::DEPRECATEDACTION_ALTER_TABLE_ALTER_PARTITION],
    ),
];

/// Source-shaped Go `BDRActionMap` value.
pub type BDRActionMap = HashMap<DDLBDRType, GoSharedSlice<ActionType>>;

/// Source-shaped Go `ActionBDRMap` value.
pub type ActionBDRMap = HashMap<ActionType, DDLBDRType>;

fn initial_bdr_action_map() -> BDRActionMap {
    BDR_ACTION_ENTRIES
        .iter()
        .map(|(role, actions)| (role.clone(), GoSharedSlice::from_vec(actions.to_vec())))
        .collect()
}

fn initial_action_bdr_map() -> ActionBDRMap {
    let mut action_map = HashMap::new();
    for (role, actions) in BDR_ACTION_ENTRIES {
        for action in *actions {
            action_map.insert(*action, role.clone());
        }
    }
    action_map
}

/// Go `BDRActionMap`: one mutable map allocation whose slice values retain
/// Go backing-array identity when copied.
pub static BDR_ACTION_MAP: LazyLock<GoShared<BDRActionMap>> =
    LazyLock::new(|| GoShared::new(initial_bdr_action_map()));

/// Go `ActionBDRMap` (built by `init` from `BDRActionMap`): the BDR class of
/// each DDL action. Initialization uses the declaration input once, matching
/// Go package `init`; later mutations of either public map do not repair the
/// other map automatically.
pub static ACTION_BDR_MAP: LazyLock<GoShared<ActionBDRMap>> =
    LazyLock::new(|| GoShared::new(initial_action_bdr_map()));

/// Go `TSConvert2Time`: converts a TSO timestamp to a time (the high bits are
/// physical milliseconds; the low 18 bits are the logical counter).
#[must_use]
pub const fn ts_convert_2_time(ts: u64) -> GoTime {
    GoTime::from_tso(ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action_type::ACTION_MAP;

    // Go TestActionBDRMap: every classified action maps back to its class,
    // and the classification covers exactly the named actions.
    #[test]
    fn action_bdr_map() {
        let actions_by_role = BDR_ACTION_MAP.read();
        let role_by_action = ACTION_BDR_MAP.read();
        assert_eq!(ACTION_MAP.len(), role_by_action.len());
        let mut total = 0;
        for (bdr_type, actions) in actions_by_role.iter() {
            for action in actions.snapshot() {
                assert_eq!(
                    role_by_action.get(&action),
                    Some(bdr_type),
                    "action {action}"
                );
            }
            total += actions.len();
        }
        assert_eq!(total, role_by_action.len());
    }

    #[test]
    fn roles_and_maps_keep_open_string_and_alias_semantics() {
        assert_eq!(DDLBDRType::default().as_str(), "");
        let future = DDLBDRType::new("future role");
        assert_eq!(future.to_string(), "future role");

        let by_role = GoShared::new(initial_bdr_action_map());
        let alias = by_role.clone();
        assert!(alias.ptr_eq(&by_role));
        alias.write().insert(
            future.clone(),
            GoSharedSlice::from_vec(vec![ActionType::ACTION_CREATE_TABLE]),
        );
        assert_eq!(by_role.read().get(&future).unwrap().len(), 1);

        let safe = by_role.read().get(&DDLBDRType::SAFE_DDL).unwrap().clone();
        let safe_alias = safe.clone();
        safe_alias.set(0, ActionType::ACTION_DROP_SCHEMA);
        assert_eq!(safe.get(0), ActionType::ACTION_DROP_SCHEMA);
    }

    #[test]
    fn ts_convert() {
        assert_eq!(ts_convert_2_time(0).unix_millis(), 0);
        assert_eq!(ts_convert_2_time((1_u64 << 18) - 1).unix_millis(), 0);
        assert_eq!(ts_convert_2_time(1_u64 << 18).unix_millis(), 1);
        // 1700000000000 ms shifted into the physical position.
        let ts = 1_700_000_000_000u64 << 18;
        assert_eq!(ts_convert_2_time(ts).unix_millis(), 1_700_000_000_000);
        assert_eq!(
            ts_convert_2_time(u64::MAX).unix_millis(),
            (u64::MAX >> 18) as i64
        );
        assert_eq!(
            ts_convert_2_time(0).location(),
            crate::go_runtime::GoTimeLocation::Local
        );
    }
}
