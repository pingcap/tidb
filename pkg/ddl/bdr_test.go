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

package ddl

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeniedByBDRWhenAddColumn(t *testing.T) {
	tests := []struct {
		name     string
		options  []*ast.ColumnOption
		expected bool
	}{
		{
			name:     "Test with no options(implicit nullable)",
			options:  []*ast.ColumnOption{},
			expected: false,
		},
		{
			name:     "Test with nullable option",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionNull}},
			expected: false,
		},
		{
			name:     "Test with implicit nullable and defaultValue options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionDefaultValue}},
			expected: false,
		},
		{
			name:     "Test with nullable and defaultValue options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionNotNull}, {Tp: ast.ColumnOptionDefaultValue}},
			expected: false,
		},
		{
			name:     "Test with comment options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionComment}},
			expected: false,
		},
		{
			name:     "Test with generated options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionGenerated}},
			expected: false,
		},
		{
			name:     "Test with comment and generated options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionComment}, {Tp: ast.ColumnOptionGenerated}},
			expected: false,
		},
		{
			name:     "Test with other options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionCheck}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deniedByBDRWhenAddColumn(tt.options)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDeniedByBDRWhenModifyColumn(t *testing.T) {
	tests := []struct {
		name         string
		newFieldType types.FieldType
		oldFieldType types.FieldType
		options      []*ast.ColumnOption
		expected     bool
	}{
		{
			name:         "Test when newFieldType and oldFieldType are not equal",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeVarchar),
			options:      []*ast.ColumnOption{},
			expected:     true,
		},
		{
			name:         "Test when only defaultValue option is provided",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeLong),
			options:      []*ast.ColumnOption{{Tp: ast.ColumnOptionDefaultValue}},
			expected:     false,
		},
		{
			name:         "Test when defaultValue and comment options are provided",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeLong),
			options:      []*ast.ColumnOption{{Tp: ast.ColumnOptionDefaultValue}, {Tp: ast.ColumnOptionComment}},
			expected:     false,
		},
		{
			name:         "Test when other options are provided",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeLong),
			options:      []*ast.ColumnOption{{Tp: ast.ColumnOptionComment}},
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deniedByBDRWhenModifyColumn(tt.newFieldType, tt.oldFieldType, tt.options)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDeniedByBDR(t *testing.T) {
	testCases := []struct {
		role     ast.BDRRole
		action   model.ActionType
		expected bool
	}{
		// Roles for ActionCreateSchema
		{ast.BDRRolePrimary, model.ActionCreateSchema, false},
		{ast.BDRRoleSecondary, model.ActionCreateSchema, true},
		{ast.BDRRoleNone, model.ActionCreateSchema, false},

		// Roles for ActionDropSchema
		{ast.BDRRolePrimary, model.ActionDropSchema, true},
		{ast.BDRRoleSecondary, model.ActionDropSchema, true},
		{ast.BDRRoleNone, model.ActionDropSchema, false},

		// Roles for ActionCreateTable
		{ast.BDRRolePrimary, model.ActionCreateTable, false},
		{ast.BDRRoleSecondary, model.ActionCreateTable, true},
		{ast.BDRRoleNone, model.ActionCreateTable, false},

		// Roles for ActionDropTable
		{ast.BDRRolePrimary, model.ActionDropTable, true},
		{ast.BDRRoleSecondary, model.ActionDropTable, true},
		{ast.BDRRoleNone, model.ActionDropTable, false},

		// Roles for ActionAddColumn
		{ast.BDRRolePrimary, model.ActionAddColumn, false},
		{ast.BDRRoleSecondary, model.ActionAddColumn, true},
		{ast.BDRRoleNone, model.ActionAddColumn, false},

		// Roles for ActionDropColumn
		{ast.BDRRolePrimary, model.ActionDropColumn, true},
		{ast.BDRRoleSecondary, model.ActionDropColumn, true},
		{ast.BDRRoleNone, model.ActionDropColumn, false},

		// Roles for ActionAddIndex
		{ast.BDRRolePrimary, model.ActionAddIndex, false},
		{ast.BDRRoleSecondary, model.ActionAddIndex, true},
		{ast.BDRRoleNone, model.ActionAddIndex, false},

		// Roles for ActionDropIndex
		{ast.BDRRolePrimary, model.ActionDropIndex, false},
		{ast.BDRRoleSecondary, model.ActionDropIndex, true},
		{ast.BDRRoleNone, model.ActionDropIndex, false},

		// Roles for ActionAddForeignKey
		{ast.BDRRolePrimary, model.ActionAddForeignKey, true},
		{ast.BDRRoleSecondary, model.ActionAddForeignKey, true},
		{ast.BDRRoleNone, model.ActionAddForeignKey, false},

		// Roles for ActionDropForeignKey
		{ast.BDRRolePrimary, model.ActionDropForeignKey, true},
		{ast.BDRRoleSecondary, model.ActionDropForeignKey, true},
		{ast.BDRRoleNone, model.ActionDropForeignKey, false},

		// Roles for ActionTruncateTable
		{ast.BDRRolePrimary, model.ActionTruncateTable, true},
		{ast.BDRRoleSecondary, model.ActionTruncateTable, true},
		{ast.BDRRoleNone, model.ActionTruncateTable, false},

		// Roles for ActionModifyColumn
		{ast.BDRRolePrimary, model.ActionModifyColumn, false},
		{ast.BDRRoleSecondary, model.ActionModifyColumn, true},
		{ast.BDRRoleNone, model.ActionModifyColumn, false},

		// Roles for ActionRebaseAutoID
		{ast.BDRRolePrimary, model.ActionRebaseAutoID, true},
		{ast.BDRRoleSecondary, model.ActionRebaseAutoID, true},
		{ast.BDRRoleNone, model.ActionRebaseAutoID, false},

		// Roles for ActionRenameTable
		{ast.BDRRolePrimary, model.ActionRenameTable, true},
		{ast.BDRRoleSecondary, model.ActionRenameTable, true},
		{ast.BDRRoleNone, model.ActionRenameTable, false},

		// Roles for ActionSetDefaultValue
		{ast.BDRRolePrimary, model.ActionSetDefaultValue, false},
		{ast.BDRRoleSecondary, model.ActionSetDefaultValue, true},
		{ast.BDRRoleNone, model.ActionSetDefaultValue, false},

		// Roles for ActionShardRowID
		{ast.BDRRolePrimary, model.ActionShardRowID, true},
		{ast.BDRRoleSecondary, model.ActionShardRowID, true},
		{ast.BDRRoleNone, model.ActionShardRowID, false},

		// Roles for ActionModifyTableComment
		{ast.BDRRolePrimary, model.ActionModifyTableComment, false},
		{ast.BDRRoleSecondary, model.ActionModifyTableComment, true},
		{ast.BDRRoleNone, model.ActionModifyTableComment, false},

		// Roles for ActionRenameIndex
		{ast.BDRRolePrimary, model.ActionRenameIndex, false},
		{ast.BDRRoleSecondary, model.ActionRenameIndex, true},
		{ast.BDRRoleNone, model.ActionRenameIndex, false},

		// Roles for ActionAddTablePartition
		{ast.BDRRolePrimary, model.ActionAddTablePartition, false},
		{ast.BDRRoleSecondary, model.ActionAddTablePartition, true},
		{ast.BDRRoleNone, model.ActionAddTablePartition, false},

		// Roles for ActionDropTablePartition
		{ast.BDRRolePrimary, model.ActionDropTablePartition, true},
		{ast.BDRRoleSecondary, model.ActionDropTablePartition, true},
		{ast.BDRRoleNone, model.ActionDropTablePartition, false},

		// Roles for ActionCreateView
		{ast.BDRRolePrimary, model.ActionCreateView, false},
		{ast.BDRRoleSecondary, model.ActionCreateView, true},
		{ast.BDRRoleNone, model.ActionCreateView, false},

		// Roles for ActionModifyTableCharsetAndCollate
		{ast.BDRRolePrimary, model.ActionModifyTableCharsetAndCollate, true},
		{ast.BDRRoleSecondary, model.ActionModifyTableCharsetAndCollate, true},
		{ast.BDRRoleNone, model.ActionModifyTableCharsetAndCollate, false},

		// Roles for ActionTruncateTablePartition
		{ast.BDRRolePrimary, model.ActionTruncateTablePartition, true},
		{ast.BDRRoleSecondary, model.ActionTruncateTablePartition, true},
		{ast.BDRRoleNone, model.ActionTruncateTablePartition, false},

		// Roles for ActionDropView
		{ast.BDRRolePrimary, model.ActionDropView, false},
		{ast.BDRRoleSecondary, model.ActionDropView, true},
		{ast.BDRRoleNone, model.ActionDropView, false},

		// Roles for ActionRecoverTable
		{ast.BDRRolePrimary, model.ActionRecoverTable, true},
		{ast.BDRRoleSecondary, model.ActionRecoverTable, true},
		{ast.BDRRoleNone, model.ActionRecoverTable, false},

		// Roles for ActionModifySchemaCharsetAndCollate
		{ast.BDRRolePrimary, model.ActionModifySchemaCharsetAndCollate, true},
		{ast.BDRRoleSecondary, model.ActionModifySchemaCharsetAndCollate, true},
		{ast.BDRRoleNone, model.ActionModifySchemaCharsetAndCollate, false},

		// Roles for ActionLockTable
		{ast.BDRRolePrimary, model.ActionLockTable, true},
		{ast.BDRRoleSecondary, model.ActionLockTable, true},
		{ast.BDRRoleNone, model.ActionLockTable, false},

		// Roles for ActionUnlockTable
		{ast.BDRRolePrimary, model.ActionUnlockTable, true},
		{ast.BDRRoleSecondary, model.ActionUnlockTable, true},
		{ast.BDRRoleNone, model.ActionUnlockTable, false},

		// Roles for ActionRepairTable
		{ast.BDRRolePrimary, model.ActionRepairTable, true},
		{ast.BDRRoleSecondary, model.ActionRepairTable, true},
		{ast.BDRRoleNone, model.ActionRepairTable, false},

		// Roles for ActionSetTiFlashReplica
		{ast.BDRRolePrimary, model.ActionSetTiFlashReplica, true},
		{ast.BDRRoleSecondary, model.ActionSetTiFlashReplica, true},
		{ast.BDRRoleNone, model.ActionSetTiFlashReplica, false},

		// Roles for ActionUpdateTiFlashReplicaStatus
		{ast.BDRRolePrimary, model.ActionUpdateTiFlashReplicaStatus, true},
		{ast.BDRRoleSecondary, model.ActionUpdateTiFlashReplicaStatus, true},
		{ast.BDRRoleNone, model.ActionUpdateTiFlashReplicaStatus, false},

		// Roles for ActionAddPrimaryKey
		{ast.BDRRolePrimary, model.ActionAddPrimaryKey, true},
		{ast.BDRRoleSecondary, model.ActionAddPrimaryKey, true},
		{ast.BDRRoleNone, model.ActionAddPrimaryKey, false},

		// Roles for ActionDropPrimaryKey
		{ast.BDRRolePrimary, model.ActionDropPrimaryKey, false},
		{ast.BDRRoleSecondary, model.ActionDropPrimaryKey, true},
		{ast.BDRRoleNone, model.ActionDropPrimaryKey, false},

		// Roles for ActionCreateSequence
		{ast.BDRRolePrimary, model.ActionCreateSequence, true},
		{ast.BDRRoleSecondary, model.ActionCreateSequence, true},
		{ast.BDRRoleNone, model.ActionCreateSequence, false},

		// Roles for ActionAlterSequence
		{ast.BDRRolePrimary, model.ActionAlterSequence, true},
		{ast.BDRRoleSecondary, model.ActionAlterSequence, true},
		{ast.BDRRoleNone, model.ActionAlterSequence, false},

		// Roles for ActionDropSequence
		{ast.BDRRolePrimary, model.ActionDropSequence, true},
		{ast.BDRRoleSecondary, model.ActionDropSequence, true},
		{ast.BDRRoleNone, model.ActionDropSequence, false},

		// Roles for ActionModifyTableAutoIDCache
		{ast.BDRRolePrimary, model.ActionModifyTableAutoIDCache, true},
		{ast.BDRRoleSecondary, model.ActionModifyTableAutoIDCache, true},
		{ast.BDRRoleNone, model.ActionModifyTableAutoIDCache, false},

		// Roles for ActionRebaseAutoRandomBase
		{ast.BDRRolePrimary, model.ActionRebaseAutoRandomBase, true},
		{ast.BDRRoleSecondary, model.ActionRebaseAutoRandomBase, true},
		{ast.BDRRoleNone, model.ActionRebaseAutoRandomBase, false},

		// Roles for ActionAlterIndexVisibility
		{ast.BDRRolePrimary, model.ActionAlterIndexVisibility, false},
		{ast.BDRRoleSecondary, model.ActionAlterIndexVisibility, true},
		{ast.BDRRoleNone, model.ActionAlterIndexVisibility, false},

		// Roles for ActionExchangeTablePartition
		{ast.BDRRolePrimary, model.ActionExchangeTablePartition, true},
		{ast.BDRRoleSecondary, model.ActionExchangeTablePartition, true},
		{ast.BDRRoleNone, model.ActionExchangeTablePartition, false},

		// Roles for ActionAddCheckConstraint
		{ast.BDRRolePrimary, model.ActionAddCheckConstraint, true},
		{ast.BDRRoleSecondary, model.ActionAddCheckConstraint, true},
		{ast.BDRRoleNone, model.ActionAddCheckConstraint, false},

		// Roles for ActionDropCheckConstraint
		{ast.BDRRolePrimary, model.ActionDropCheckConstraint, true},
		{ast.BDRRoleSecondary, model.ActionDropCheckConstraint, true},
		{ast.BDRRoleNone, model.ActionDropCheckConstraint, false},

		// Roles for ActionAlterCheckConstraint
		{ast.BDRRolePrimary, model.ActionAlterCheckConstraint, true},
		{ast.BDRRoleSecondary, model.ActionAlterCheckConstraint, true},
		{ast.BDRRoleNone, model.ActionAlterCheckConstraint, false},

		// Roles for ActionRenameTables
		{ast.BDRRolePrimary, model.ActionRenameTables, true},
		{ast.BDRRoleSecondary, model.ActionRenameTables, true},
		{ast.BDRRoleNone, model.ActionRenameTables, false},

		// Roles for ActionAlterTableAttributes
		{ast.BDRRolePrimary, model.ActionAlterTableAttributes, true},
		{ast.BDRRoleSecondary, model.ActionAlterTableAttributes, true},
		{ast.BDRRoleNone, model.ActionAlterTableAttributes, false},

		// Roles for ActionAlterTablePartitionAttributes
		{ast.BDRRolePrimary, model.ActionAlterTablePartitionAttributes, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePartitionAttributes, true},
		{ast.BDRRoleNone, model.ActionAlterTablePartitionAttributes, false},

		// Roles for ActionCreatePlacementPolicy
		{ast.BDRRolePrimary, model.ActionCreatePlacementPolicy, false},
		{ast.BDRRoleSecondary, model.ActionCreatePlacementPolicy, false},
		{ast.BDRRoleNone, model.ActionCreatePlacementPolicy, false},

		// Roles for ActionAlterPlacementPolicy
		{ast.BDRRolePrimary, model.ActionAlterPlacementPolicy, false},
		{ast.BDRRoleSecondary, model.ActionAlterPlacementPolicy, false},
		{ast.BDRRoleNone, model.ActionAlterPlacementPolicy, false},

		// Roles for ActionDropPlacementPolicy
		{ast.BDRRolePrimary, model.ActionDropPlacementPolicy, false},
		{ast.BDRRoleSecondary, model.ActionDropPlacementPolicy, false},
		{ast.BDRRoleNone, model.ActionDropPlacementPolicy, false},

		// Roles for ActionAlterTablePartitionPlacement
		{ast.BDRRolePrimary, model.ActionAlterTablePartitionPlacement, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePartitionPlacement, true},
		{ast.BDRRoleNone, model.ActionAlterTablePartitionPlacement, false},

		// Roles for ActionModifySchemaDefaultPlacement
		{ast.BDRRolePrimary, model.ActionModifySchemaDefaultPlacement, true},
		{ast.BDRRoleSecondary, model.ActionModifySchemaDefaultPlacement, true},
		{ast.BDRRoleNone, model.ActionModifySchemaDefaultPlacement, false},

		// Roles for ActionAlterTablePlacement
		{ast.BDRRolePrimary, model.ActionAlterTablePlacement, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePlacement, true},
		{ast.BDRRoleNone, model.ActionAlterTablePlacement, false},

		// Roles for ActionAlterCacheTable
		{ast.BDRRolePrimary, model.ActionAlterCacheTable, true},
		{ast.BDRRoleSecondary, model.ActionAlterCacheTable, true},
		{ast.BDRRoleNone, model.ActionAlterCacheTable, false},

		// Roles for ActionAlterTableStatsOptions
		{ast.BDRRolePrimary, model.ActionAlterTableStatsOptions, true},
		{ast.BDRRoleSecondary, model.ActionAlterTableStatsOptions, true},
		{ast.BDRRoleNone, model.ActionAlterTableStatsOptions, false},

		// Roles for ActionAlterNoCacheTable
		{ast.BDRRolePrimary, model.ActionAlterNoCacheTable, true},
		{ast.BDRRoleSecondary, model.ActionAlterNoCacheTable, true},
		{ast.BDRRoleNone, model.ActionAlterNoCacheTable, false},

		// Roles for ActionCreateTables
		{ast.BDRRolePrimary, model.ActionCreateTables, false},
		{ast.BDRRoleSecondary, model.ActionCreateTables, true},
		{ast.BDRRoleNone, model.ActionCreateTables, false},

		// Roles for ActionMultiSchemaChange
		{ast.BDRRolePrimary, model.ActionMultiSchemaChange, true},
		{ast.BDRRoleSecondary, model.ActionMultiSchemaChange, true},
		{ast.BDRRoleNone, model.ActionMultiSchemaChange, false},

		// Roles for ActionFlashbackCluster
		{ast.BDRRolePrimary, model.ActionFlashbackCluster, true},
		{ast.BDRRoleSecondary, model.ActionFlashbackCluster, true},
		{ast.BDRRoleNone, model.ActionFlashbackCluster, false},

		// Roles for ActionRecoverSchema
		{ast.BDRRolePrimary, model.ActionRecoverSchema, true},
		{ast.BDRRoleSecondary, model.ActionRecoverSchema, true},
		{ast.BDRRoleNone, model.ActionRecoverSchema, false},

		// Roles for ActionReorganizePartition
		{ast.BDRRolePrimary, model.ActionReorganizePartition, true},
		{ast.BDRRoleSecondary, model.ActionReorganizePartition, true},
		{ast.BDRRoleNone, model.ActionReorganizePartition, false},

		// Roles for ActionAlterTTLInfo
		{ast.BDRRolePrimary, model.ActionAlterTTLInfo, false},
		{ast.BDRRoleSecondary, model.ActionAlterTTLInfo, true},
		{ast.BDRRoleNone, model.ActionAlterTTLInfo, false},

		// Roles for ActionAlterTTLRemove
		{ast.BDRRolePrimary, model.ActionAlterTTLRemove, false},
		{ast.BDRRoleSecondary, model.ActionAlterTTLRemove, true},
		{ast.BDRRoleNone, model.ActionAlterTTLRemove, false},

		// Roles for ActionCreateResourceGroup
		{ast.BDRRolePrimary, model.ActionCreateResourceGroup, false},
		{ast.BDRRoleSecondary, model.ActionCreateResourceGroup, false},
		{ast.BDRRoleNone, model.ActionCreateResourceGroup, false},

		// Roles for ActionAlterResourceGroup
		{ast.BDRRolePrimary, model.ActionAlterResourceGroup, false},
		{ast.BDRRoleSecondary, model.ActionAlterResourceGroup, false},
		{ast.BDRRoleNone, model.ActionAlterResourceGroup, false},

		// Roles for ActionDropResourceGroup
		{ast.BDRRolePrimary, model.ActionDropResourceGroup, false},
		{ast.BDRRoleSecondary, model.ActionDropResourceGroup, false},
		{ast.BDRRoleNone, model.ActionDropResourceGroup, false},

		// Roles for ActionAlterTablePartitioning
		{ast.BDRRolePrimary, model.ActionAlterTablePartitioning, true},
		{ast.BDRRoleSecondary, model.ActionAlterTablePartitioning, true},
		{ast.BDRRoleNone, model.ActionAlterTablePartitioning, false},

		// Roles for ActionRemovePartitioning
		{ast.BDRRolePrimary, model.ActionRemovePartitioning, true},
		{ast.BDRRoleSecondary, model.ActionRemovePartitioning, true},
		{ast.BDRRoleNone, model.ActionRemovePartitioning, false},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, DeniedByBDR(tc.role, tc.action, nil), fmt.Sprintf("role: %v, action: %v", tc.role, tc.action))
	}

	// test special cases
	testCases2 := []struct {
		role     ast.BDRRole
		action   model.ActionType
		job      *model.Job
		expected bool
	}{
		{
			role:   ast.BDRRolePrimary,
			action: model.ActionAddPrimaryKey,
			job: &model.Job{
				Type: model.ActionAddPrimaryKey,
				Args: []any{true},
			},
			expected: true,
		},
		{
			role:   ast.BDRRolePrimary,
			action: model.ActionAddIndex,
			job: &model.Job{
				Type: model.ActionAddIndex,
				Args: []any{true},
			},
			expected: true,
		},
		{
			role:   ast.BDRRolePrimary,
			action: model.ActionAddIndex,
			job: &model.Job{
				Type: model.ActionAddIndex,
				Args: []any{false},
			},
			expected: false,
		},
	}

	for _, tc := range testCases2 {
		assert.Equal(t, tc.expected, DeniedByBDR(tc.role, tc.action, tc.job), fmt.Sprintf("role: %v, action: %v", tc.role, tc.action))
	}
}
