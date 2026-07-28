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

//! `ActionType` from `pkg/meta/model/job.go`: the DDL action kinds and their
//! display names. Constants and the name table were extracted from the Go
//! source by script for fidelity.

/// Go `ActionType` (a `byte`): the kind of a DDL job. A newtype over `u8` so
/// any stored value round-trips; [`Display`](std::fmt::Display) yields the
/// name from [`ACTION_MAP`] or `"none"` for an unknown/absent value.
///
/// `encoding/json` marshals a `byte`-based type as a bare number, so the serde
/// representation is transparent rather than a wrapper object.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct ActionType(pub u8);

impl ActionType {
    /// Go `ActionNone`.
    pub const ACTION_NONE: ActionType = ActionType(0);
    /// Go `ActionCreateSchema`.
    pub const ACTION_CREATE_SCHEMA: ActionType = ActionType(1);
    /// Go `ActionDropSchema`.
    pub const ACTION_DROP_SCHEMA: ActionType = ActionType(2);
    /// Go `ActionCreateTable`.
    pub const ACTION_CREATE_TABLE: ActionType = ActionType(3);
    /// Go `ActionDropTable`.
    pub const ACTION_DROP_TABLE: ActionType = ActionType(4);
    /// Go `ActionAddColumn`.
    pub const ACTION_ADD_COLUMN: ActionType = ActionType(5);
    /// Go `ActionDropColumn`.
    pub const ACTION_DROP_COLUMN: ActionType = ActionType(6);
    /// Go `ActionAddIndex`.
    pub const ACTION_ADD_INDEX: ActionType = ActionType(7);
    /// Go `ActionDropIndex`.
    pub const ACTION_DROP_INDEX: ActionType = ActionType(8);
    /// Go `ActionAddForeignKey`.
    pub const ACTION_ADD_FOREIGN_KEY: ActionType = ActionType(9);
    /// Go `ActionDropForeignKey`.
    pub const ACTION_DROP_FOREIGN_KEY: ActionType = ActionType(10);
    /// Go `ActionTruncateTable`.
    pub const ACTION_TRUNCATE_TABLE: ActionType = ActionType(11);
    /// Go `ActionModifyColumn`.
    pub const ACTION_MODIFY_COLUMN: ActionType = ActionType(12);
    /// Go `ActionRebaseAutoID`.
    pub const ACTION_REBASE_AUTO_ID: ActionType = ActionType(13);
    /// Go `ActionRenameTable`.
    pub const ACTION_RENAME_TABLE: ActionType = ActionType(14);
    /// Go `ActionSetDefaultValue`.
    pub const ACTION_SET_DEFAULT_VALUE: ActionType = ActionType(15);
    /// Go `ActionShardRowID`.
    pub const ACTION_SHARD_ROW_ID: ActionType = ActionType(16);
    /// Go `ActionModifyTableComment`.
    pub const ACTION_MODIFY_TABLE_COMMENT: ActionType = ActionType(17);
    /// Go `ActionRenameIndex`.
    pub const ACTION_RENAME_INDEX: ActionType = ActionType(18);
    /// Go `ActionAddTablePartition`.
    pub const ACTION_ADD_TABLE_PARTITION: ActionType = ActionType(19);
    /// Go `ActionDropTablePartition`.
    pub const ACTION_DROP_TABLE_PARTITION: ActionType = ActionType(20);
    /// Go `ActionCreateView`.
    pub const ACTION_CREATE_VIEW: ActionType = ActionType(21);
    /// Go `ActionModifyTableCharsetAndCollate`.
    pub const ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE: ActionType = ActionType(22);
    /// Go `ActionTruncateTablePartition`.
    pub const ACTION_TRUNCATE_TABLE_PARTITION: ActionType = ActionType(23);
    /// Go `ActionDropView`.
    pub const ACTION_DROP_VIEW: ActionType = ActionType(24);
    /// Go `ActionRecoverTable`.
    pub const ACTION_RECOVER_TABLE: ActionType = ActionType(25);
    /// Go `ActionModifySchemaCharsetAndCollate`.
    pub const ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE: ActionType = ActionType(26);
    /// Go `ActionLockTable`.
    pub const ACTION_LOCK_TABLE: ActionType = ActionType(27);
    /// Go `ActionUnlockTable`.
    pub const ACTION_UNLOCK_TABLE: ActionType = ActionType(28);
    /// Go `ActionRepairTable`.
    pub const ACTION_REPAIR_TABLE: ActionType = ActionType(29);
    /// Go `ActionSetTiFlashReplica`.
    pub const ACTION_SET_TI_FLASH_REPLICA: ActionType = ActionType(30);
    /// Go `ActionUpdateTiFlashReplicaStatus`.
    pub const ACTION_UPDATE_TI_FLASH_REPLICA_STATUS: ActionType = ActionType(31);
    /// Go `ActionAddPrimaryKey`.
    pub const ACTION_ADD_PRIMARY_KEY: ActionType = ActionType(32);
    /// Go `ActionDropPrimaryKey`.
    pub const ACTION_DROP_PRIMARY_KEY: ActionType = ActionType(33);
    /// Go `ActionCreateSequence`.
    pub const ACTION_CREATE_SEQUENCE: ActionType = ActionType(34);
    /// Go `ActionAlterSequence`.
    pub const ACTION_ALTER_SEQUENCE: ActionType = ActionType(35);
    /// Go `ActionDropSequence`.
    pub const ACTION_DROP_SEQUENCE: ActionType = ActionType(36);
    /// Go `ActionAddColumns`.
    pub const ACTION_ADD_COLUMNS: ActionType = ActionType(37);
    /// Go `ActionDropColumns`.
    pub const ACTION_DROP_COLUMNS: ActionType = ActionType(38);
    /// Go `ActionModifyTableAutoIDCache`.
    pub const ACTION_MODIFY_TABLE_AUTO_IDCACHE: ActionType = ActionType(39);
    /// Go `ActionRebaseAutoRandomBase`.
    pub const ACTION_REBASE_AUTO_RANDOM_BASE: ActionType = ActionType(40);
    /// Go `ActionAlterIndexVisibility`.
    pub const ACTION_ALTER_INDEX_VISIBILITY: ActionType = ActionType(41);
    /// Go `ActionExchangeTablePartition`.
    pub const ACTION_EXCHANGE_TABLE_PARTITION: ActionType = ActionType(42);
    /// Go `ActionAddCheckConstraint`.
    pub const ACTION_ADD_CHECK_CONSTRAINT: ActionType = ActionType(43);
    /// Go `ActionDropCheckConstraint`.
    pub const ACTION_DROP_CHECK_CONSTRAINT: ActionType = ActionType(44);
    /// Go `ActionAlterCheckConstraint`.
    pub const ACTION_ALTER_CHECK_CONSTRAINT: ActionType = ActionType(45);
    /// Go `_DEPRECATEDActionAlterTableAlterPartition`.
    pub const DEPRECATEDACTION_ALTER_TABLE_ALTER_PARTITION: ActionType = ActionType(46);
    /// Go `ActionRenameTables`.
    pub const ACTION_RENAME_TABLES: ActionType = ActionType(47);
    /// Go `_DEPRECATEDActionDropIndexes`.
    pub const DEPRECATEDACTION_DROP_INDEXES: ActionType = ActionType(48);
    /// Go `ActionAlterTableAttributes`.
    pub const ACTION_ALTER_TABLE_ATTRIBUTES: ActionType = ActionType(49);
    /// Go `ActionAlterTablePartitionAttributes`.
    pub const ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES: ActionType = ActionType(50);
    /// Go `ActionCreatePlacementPolicy`.
    pub const ACTION_CREATE_PLACEMENT_POLICY: ActionType = ActionType(51);
    /// Go `ActionAlterPlacementPolicy`.
    pub const ACTION_ALTER_PLACEMENT_POLICY: ActionType = ActionType(52);
    /// Go `ActionDropPlacementPolicy`.
    pub const ACTION_DROP_PLACEMENT_POLICY: ActionType = ActionType(53);
    /// Go `ActionAlterTablePartitionPlacement`.
    pub const ACTION_ALTER_TABLE_PARTITION_PLACEMENT: ActionType = ActionType(54);
    /// Go `ActionModifySchemaDefaultPlacement`.
    pub const ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT: ActionType = ActionType(55);
    /// Go `ActionAlterTablePlacement`.
    pub const ACTION_ALTER_TABLE_PLACEMENT: ActionType = ActionType(56);
    /// Go `ActionAlterCacheTable`.
    pub const ACTION_ALTER_CACHE_TABLE: ActionType = ActionType(57);
    /// Go `ActionAlterTableStatsOptions`.
    pub const ACTION_ALTER_TABLE_STATS_OPTIONS: ActionType = ActionType(58);
    /// Go `ActionAlterNoCacheTable`.
    pub const ACTION_ALTER_NO_CACHE_TABLE: ActionType = ActionType(59);
    /// Go `ActionCreateTables`.
    pub const ACTION_CREATE_TABLES: ActionType = ActionType(60);
    /// Go `ActionMultiSchemaChange`.
    pub const ACTION_MULTI_SCHEMA_CHANGE: ActionType = ActionType(61);
    /// Go `ActionFlashbackCluster`.
    pub const ACTION_FLASHBACK_CLUSTER: ActionType = ActionType(62);
    /// Go `ActionRecoverSchema`.
    pub const ACTION_RECOVER_SCHEMA: ActionType = ActionType(63);
    /// Go `ActionReorganizePartition`.
    pub const ACTION_REORGANIZE_PARTITION: ActionType = ActionType(64);
    /// Go `ActionAlterTTLInfo`.
    pub const ACTION_ALTER_TTLINFO: ActionType = ActionType(65);
    /// Go `ActionAlterTTLRemove`.
    pub const ACTION_ALTER_TTLREMOVE: ActionType = ActionType(67);
    /// Go `ActionCreateResourceGroup`.
    pub const ACTION_CREATE_RESOURCE_GROUP: ActionType = ActionType(68);
    /// Go `ActionAlterResourceGroup`.
    pub const ACTION_ALTER_RESOURCE_GROUP: ActionType = ActionType(69);
    /// Go `ActionDropResourceGroup`.
    pub const ACTION_DROP_RESOURCE_GROUP: ActionType = ActionType(70);
    /// Go `ActionAlterTablePartitioning`.
    pub const ACTION_ALTER_TABLE_PARTITIONING: ActionType = ActionType(71);
    /// Go `ActionRemovePartitioning`.
    pub const ACTION_REMOVE_PARTITIONING: ActionType = ActionType(72);
    /// Go `ActionAddColumnarIndex`.
    pub const ACTION_ADD_COLUMNAR_INDEX: ActionType = ActionType(73);
    /// Go `ActionModifyEngineAttribute`.
    pub const ACTION_MODIFY_ENGINE_ATTRIBUTE: ActionType = ActionType(74);
    /// Go `ActionAlterTableMode`.
    pub const ACTION_ALTER_TABLE_MODE: ActionType = ActionType(75);
    /// Go `ActionRefreshMeta`.
    pub const ACTION_REFRESH_META: ActionType = ActionType(76);
    /// Go `ActionModifySchemaReadOnly`.
    pub const ACTION_MODIFY_SCHEMA_READ_ONLY: ActionType = ActionType(77);
    /// Go `ActionAlterTableAffinity`.
    pub const ACTION_ALTER_TABLE_AFFINITY: ActionType = ActionType(78);
    /// Go `ActionAlterTableSoftDeleteInfo`.
    pub const ACTION_ALTER_TABLE_SOFT_DELETE_INFO: ActionType = ActionType(79);
    /// Go `ActionModifySchemaSoftDeleteAndActiveActive`.
    pub const ACTION_MODIFY_SCHEMA_SOFT_DELETE_AND_ACTIVE_ACTIVE: ActionType = ActionType(80);
    /// Go `ActionCreateMaskingPolicy`.
    pub const ACTION_CREATE_MASKING_POLICY: ActionType = ActionType(81);
    /// Go `ActionAlterMaskingPolicy`.
    pub const ACTION_ALTER_MASKING_POLICY: ActionType = ActionType(82);
    /// Go `ActionDropMaskingPolicy`.
    pub const ACTION_DROP_MASKING_POLICY: ActionType = ActionType(83);
    /// Go `ActionAlterTableSetRegionSplitPolicy`.
    pub const ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY: ActionType = ActionType(84);
}

/// Go `ActionMap`: the display name of each DDL action.
pub const ACTION_MAP: &[(ActionType, &str)] = &[
    (ActionType::ACTION_CREATE_SCHEMA, "create schema"),
    (ActionType::ACTION_DROP_SCHEMA, "drop schema"),
    (ActionType::ACTION_CREATE_TABLE, "create table"),
    (ActionType::ACTION_CREATE_TABLES, "create tables"),
    (ActionType::ACTION_DROP_TABLE, "drop table"),
    (ActionType::ACTION_ADD_COLUMN, "add column"),
    (ActionType::ACTION_DROP_COLUMN, "drop column"),
    (ActionType::ACTION_ADD_INDEX, "add index"),
    (ActionType::ACTION_DROP_INDEX, "drop index"),
    (ActionType::ACTION_ADD_FOREIGN_KEY, "add foreign key"),
    (ActionType::ACTION_DROP_FOREIGN_KEY, "drop foreign key"),
    (ActionType::ACTION_TRUNCATE_TABLE, "truncate table"),
    (ActionType::ACTION_MODIFY_COLUMN, "modify column"),
    (
        ActionType::ACTION_REBASE_AUTO_ID,
        "rebase auto_increment ID",
    ),
    (ActionType::ACTION_RENAME_TABLE, "rename table"),
    (ActionType::ACTION_RENAME_TABLES, "rename tables"),
    (ActionType::ACTION_SET_DEFAULT_VALUE, "set default value"),
    (ActionType::ACTION_SHARD_ROW_ID, "shard row ID"),
    (
        ActionType::ACTION_MODIFY_TABLE_COMMENT,
        "modify table comment",
    ),
    (ActionType::ACTION_RENAME_INDEX, "rename index"),
    (ActionType::ACTION_ADD_TABLE_PARTITION, "add partition"),
    (ActionType::ACTION_DROP_TABLE_PARTITION, "drop partition"),
    (ActionType::ACTION_CREATE_VIEW, "create view"),
    (
        ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE,
        "modify table charset and collate",
    ),
    (
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
        "truncate partition",
    ),
    (ActionType::ACTION_DROP_VIEW, "drop view"),
    (ActionType::ACTION_RECOVER_TABLE, "recover table"),
    (
        ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
        "modify schema charset and collate",
    ),
    (ActionType::ACTION_LOCK_TABLE, "lock table"),
    (ActionType::ACTION_UNLOCK_TABLE, "unlock table"),
    (ActionType::ACTION_REPAIR_TABLE, "repair table"),
    (
        ActionType::ACTION_SET_TI_FLASH_REPLICA,
        "set tiflash replica",
    ),
    (
        ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS,
        "update tiflash replica status",
    ),
    (ActionType::ACTION_ADD_PRIMARY_KEY, "add primary key"),
    (ActionType::ACTION_DROP_PRIMARY_KEY, "drop primary key"),
    (ActionType::ACTION_CREATE_SEQUENCE, "create sequence"),
    (ActionType::ACTION_ALTER_SEQUENCE, "alter sequence"),
    (ActionType::ACTION_DROP_SEQUENCE, "drop sequence"),
    (
        ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE,
        "modify auto id cache",
    ),
    (
        ActionType::ACTION_REBASE_AUTO_RANDOM_BASE,
        "rebase auto_random ID",
    ),
    (
        ActionType::ACTION_ALTER_INDEX_VISIBILITY,
        "alter index visibility",
    ),
    (
        ActionType::ACTION_EXCHANGE_TABLE_PARTITION,
        "exchange partition",
    ),
    (
        ActionType::ACTION_ADD_CHECK_CONSTRAINT,
        "add check constraint",
    ),
    (
        ActionType::ACTION_DROP_CHECK_CONSTRAINT,
        "drop check constraint",
    ),
    (
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
        "alter check constraint",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_ATTRIBUTES,
        "alter table attributes",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
        "alter table partition placement",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES,
        "alter table partition attributes",
    ),
    (
        ActionType::ACTION_CREATE_PLACEMENT_POLICY,
        "create placement policy",
    ),
    (
        ActionType::ACTION_ALTER_PLACEMENT_POLICY,
        "alter placement policy",
    ),
    (
        ActionType::ACTION_DROP_PLACEMENT_POLICY,
        "drop placement policy",
    ),
    (
        ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT,
        "modify schema default placement",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_PLACEMENT,
        "alter table placement",
    ),
    (ActionType::ACTION_ALTER_CACHE_TABLE, "alter table cache"),
    (
        ActionType::ACTION_ALTER_NO_CACHE_TABLE,
        "alter table nocache",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_STATS_OPTIONS,
        "alter table statistics options",
    ),
    (
        ActionType::ACTION_MULTI_SCHEMA_CHANGE,
        "alter table multi-schema change",
    ),
    (ActionType::ACTION_FLASHBACK_CLUSTER, "flashback cluster"),
    (ActionType::ACTION_RECOVER_SCHEMA, "flashback schema"),
    (
        ActionType::ACTION_REORGANIZE_PARTITION,
        "alter table reorganize partition",
    ),
    (ActionType::ACTION_ALTER_TTLINFO, "alter table ttl"),
    (ActionType::ACTION_ALTER_TTLREMOVE, "alter table no_ttl"),
    (
        ActionType::ACTION_CREATE_RESOURCE_GROUP,
        "create resource group",
    ),
    (
        ActionType::ACTION_ALTER_RESOURCE_GROUP,
        "alter resource group",
    ),
    (
        ActionType::ACTION_DROP_RESOURCE_GROUP,
        "drop resource group",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_PARTITIONING,
        "alter table partition by",
    ),
    (
        ActionType::ACTION_REMOVE_PARTITIONING,
        "alter table remove partitioning",
    ),
    (ActionType::ACTION_ADD_COLUMNAR_INDEX, "add columnar index"),
    (
        ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE,
        "modify engine attribute",
    ),
    (ActionType::ACTION_ALTER_TABLE_MODE, "alter table mode"),
    (ActionType::ACTION_REFRESH_META, "refresh meta"),
    (
        ActionType::ACTION_MODIFY_SCHEMA_READ_ONLY,
        "modify schema read only",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_AFFINITY,
        "alter table affinity",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_SOFT_DELETE_INFO,
        "alter soft delete info",
    ),
    (
        ActionType::ACTION_MODIFY_SCHEMA_SOFT_DELETE_AND_ACTIVE_ACTIVE,
        "modify schema soft delete and active active",
    ),
    (
        ActionType::ACTION_CREATE_MASKING_POLICY,
        "create masking policy",
    ),
    (
        ActionType::ACTION_ALTER_MASKING_POLICY,
        "alter masking policy",
    ),
    (
        ActionType::ACTION_DROP_MASKING_POLICY,
        "drop masking policy",
    ),
    (
        ActionType::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY,
        "alter table set region split policy",
    ),
    (
        ActionType::DEPRECATEDACTION_ALTER_TABLE_ALTER_PARTITION,
        "alter partition",
    ),
];

impl std::fmt::Display for ActionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Go's String(): ActionMap lookup, else "none".
        for (action, name) in ACTION_MAP {
            if action == self {
                return f.write_str(name);
            }
        }
        f.write_str("none")
    }
}
