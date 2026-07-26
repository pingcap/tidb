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

use std::collections::HashMap;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};

use crate::action_type::ActionType;

/// Go `DDLBDRType` (a `string`): a DDL's safety class under BDR.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DDLBDRType(pub &'static str);

impl DDLBDRType {
    /// The DDL can't be run by a user on a Primary/Secondary cluster.
    pub const UNSAFE_DDL: DDLBDRType = DDLBDRType("unsafe DDL");
    /// The DDL can be run by a user on a Primary cluster.
    pub const SAFE_DDL: DDLBDRType = DDLBDRType("safe DDL");
    /// The DDL can't be synced by CDC.
    pub const UNMANAGEMENT_DDL: DDLBDRType = DDLBDRType("unmanagement DDL");
    /// The DDL is unknown.
    pub const UNKNOWN_DDL: DDLBDRType = DDLBDRType("unknown DDL");
}

impl std::fmt::Display for DDLBDRType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Go `BDRActionMap`: the DDL actions in each BDR safety class.
pub const BDR_ACTION_MAP: &[(DDLBDRType, &[ActionType])] = &[
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

/// Go `ActionBDRMap` (built by `init` from `BDRActionMap`): the BDR class of
/// each DDL action.
pub static ACTION_BDR_MAP: LazyLock<HashMap<ActionType, DDLBDRType>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    for (bdr_type, actions) in BDR_ACTION_MAP {
        for action in *actions {
            m.insert(*action, *bdr_type);
        }
    }
    m
});

/// Go `TSConvert2Time`: converts a TSO timestamp to a time (the high bits are
/// physical milliseconds; the low 18 bits are the logical counter).
#[must_use]
pub fn ts_convert_2_time(ts: u64) -> DateTime<Utc> {
    let ms = (ts >> 18) as i64;
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action_type::ACTION_MAP;

    // Go TestActionBDRMap: every classified action maps back to its class,
    // and the classification covers exactly the named actions.
    #[test]
    fn action_bdr_map() {
        assert_eq!(ACTION_MAP.len(), ACTION_BDR_MAP.len());
        let mut total = 0;
        for (bdr_type, actions) in BDR_ACTION_MAP {
            for action in *actions {
                assert_eq!(
                    ACTION_BDR_MAP.get(action),
                    Some(bdr_type),
                    "action {action}"
                );
            }
            total += actions.len();
        }
        assert_eq!(total, ACTION_BDR_MAP.len());
    }

    #[test]
    fn ts_convert() {
        // 1700000000000 ms shifted into the physical position.
        let ts = 1_700_000_000_000u64 << 18;
        assert_eq!(ts_convert_2_time(ts).timestamp_millis(), 1_700_000_000_000);
    }
}
