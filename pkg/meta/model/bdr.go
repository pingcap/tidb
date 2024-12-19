// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"time"
)

// DDLBDRType is the type for DDL when BDR enable.
type DDLBDRType string

const (
	// UnsafeDDL means the DDL can't be executed by user when cluster is Primary/Secondary.
	UnsafeDDL DDLBDRType = "unsafe DDL"
	// SafeDDL means the DDL can be executed by user when cluster is Primary.
	SafeDDL DDLBDRType = "safe DDL"
	// UnmanagementDDL means the DDL can't be synced by CDC.
	UnmanagementDDL DDLBDRType = "unmanagement DDL"
	// UnknownDDL means the DDL is unknown.
	UnknownDDL DDLBDRType = "unknown DDL"
)

// ActionBDRMap is the map of DDL ActionType to DDLBDRType.
var ActionBDRMap = map[ActionType]DDLBDRType{}

// BDRActionMap is the map of DDLBDRType to ActionType (reversed from ActionBDRMap).
var BDRActionMap = map[DDLBDRType][]ActionType{
	SafeDDL: {
		ActionCreateSchema,
		ActionCreateTable,
		ActionAddColumn, // add a new column to table if it’s nullable or with default value.
		ActionAddIndex,  //add non-unique index
		ActionDropIndex,
		ActionModifyColumn, // add or update comments for column, change default values of one particular column
		ActionSetDefaultValue,
		ActionModifyTableComment,
		ActionRenameIndex,
		ActionAddTablePartition,
		ActionDropPrimaryKey,
		ActionAlterIndexVisibility,
		ActionCreateTables,
		ActionAlterTTLInfo,
		ActionAlterTTLRemove,
		ActionCreateView,
		ActionDropView,
	},
	UnsafeDDL: {
		ActionDropSchema,
		ActionDropTable,
		ActionDropColumn,
		ActionAddForeignKey,
		ActionDropForeignKey,
		ActionTruncateTable,
		ActionRebaseAutoID,
		ActionRenameTable,
		ActionShardRowID,
		ActionDropTablePartition,
		ActionModifyTableCharsetAndCollate,
		ActionTruncateTablePartition,
		ActionRecoverTable,
		ActionModifySchemaCharsetAndCollate,
		ActionLockTable,
		ActionUnlockTable,
		ActionRepairTable,
		ActionSetTiFlashReplica,
		ActionUpdateTiFlashReplicaStatus,
		ActionAddPrimaryKey,
		ActionCreateSequence,
		ActionAlterSequence,
		ActionDropSequence,
		ActionModifyTableAutoIDCache,
		ActionRebaseAutoRandomBase,
		ActionExchangeTablePartition,
		ActionAddCheckConstraint,
		ActionDropCheckConstraint,
		ActionAlterCheckConstraint,
		ActionRenameTables,
		ActionAlterTableAttributes,
		ActionAlterTablePartitionAttributes,
		ActionAlterTablePartitionPlacement,
		ActionModifySchemaDefaultPlacement,
		ActionAlterTablePlacement,
		ActionAlterCacheTable,
		ActionAlterTableStatsOptions,
		ActionAlterNoCacheTable,
		ActionMultiSchemaChange,
		ActionFlashbackCluster,
		ActionRecoverSchema,
		ActionReorganizePartition,
		ActionAlterTablePartitioning,
		ActionRemovePartitioning,
		ActionAddVectorIndex,
	},
	UnmanagementDDL: {
		ActionCreatePlacementPolicy,
		ActionAlterPlacementPolicy,
		ActionDropPlacementPolicy,
		ActionCreateResourceGroup,
		ActionAlterResourceGroup,
		ActionDropResourceGroup,
	},
	UnknownDDL: {
		_DEPRECATEDActionAlterTableAlterPartition,
	},
}

// TSConvert2Time converts timestamp to time.
func TSConvert2Time(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 is for the logical time.
	return time.UnixMilli(t)
}

func init() {
	for bdrType, v := range BDRActionMap {
		for _, action := range v {
			ActionBDRMap[action] = bdrType
		}
	}
}
