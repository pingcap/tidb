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
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestGetOrDecodeArgsV2(t *testing.T) {
	j := &Job{
		Version: JobVersion2,
		Type:    ActionTruncateTable,
	}
	j.FillArgs(&TruncateTableArgs{
		FKCheck: true,
	})
	_, err := j.Encode(true)
	require.NoError(t, err)
	require.NotNil(t, j.RawArgs)
	// return existing argsV2
	argsV2, err := getOrDecodeArgsV2[*TruncateTableArgs](j)
	require.NoError(t, err)
	require.Same(t, j.args[0], argsV2)
	// unmarshal from json
	var argsBak *TruncateTableArgs
	argsBak, j.args = j.args[0].(*TruncateTableArgs), nil
	argsV2, err = getOrDecodeArgsV2[*TruncateTableArgs](j)
	require.NoError(t, err)
	require.NotNil(t, argsV2)
	require.NotSame(t, argsBak, argsV2)
}

func getJobBytes(t *testing.T, inArgs JobArgs, ver JobVersion, tp ActionType) []byte {
	j := &Job{
		Version: ver,
		Type:    tp,
	}
	j.FillArgs(inArgs)
	bytes, err := j.Encode(true)
	require.NoError(t, err)
	return bytes
}

func getFinishedJobBytes(t *testing.T, inArgs FinishedJobArgs, ver JobVersion, tp ActionType) []byte {
	j := &Job{
		Version: ver,
		Type:    tp,
	}
	j.FillFinishedArgs(inArgs)
	bytes, err := j.Encode(true)
	require.NoError(t, err)
	return bytes
}

func TestCreateSchemaArgs(t *testing.T) {
	inArgs := &CreateSchemaArgs{
		DBInfo: &DBInfo{ID: 100},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionCreateSchema)))
		args, err := GetCreateSchemaArgs(j2)
		require.NoError(t, err)
		require.EqualValues(t, inArgs.DBInfo, args.DBInfo)
	}
}

func TestDropSchemaArgs(t *testing.T) {
	inArgs := &DropSchemaArgs{
		FKCheck: true,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionDropSchema)))
		args, err := GetDropSchemaArgs(j2)
		require.NoError(t, err)
		require.EqualValues(t, inArgs.FKCheck, args.FKCheck)
	}

	inArgs = &DropSchemaArgs{
		AllDroppedTableIDs: []int64{1, 2},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionDropSchema)))
		args, err := GetFinishedDropSchemaArgs(j2)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2}, args.AllDroppedTableIDs)
	}
}

func TestModifySchemaArgs(t *testing.T) {
	inArgs := &ModifySchemaArgs{
		ToCharset: "aa",
		ToCollate: "bb",
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionModifySchemaCharsetAndCollate)))
		args, err := GetModifySchemaArgs(j2)
		require.NoError(t, err)
		require.Equal(t, "aa", args.ToCharset)
		require.Equal(t, "bb", args.ToCollate)
	}
	for _, inArgs = range []*ModifySchemaArgs{
		{PolicyRef: &PolicyRefInfo{ID: 123}},
		{},
	} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionModifySchemaDefaultPlacement)))
			args, err := GetModifySchemaArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.PolicyRef, args.PolicyRef)
		}
	}
}

func TestCreateTableArgs(t *testing.T) {
	t.Run("create table", func(t *testing.T) {
		inArgs := &CreateTableArgs{
			TableInfo: &TableInfo{ID: 100},
			FKCheck:   true,
		}
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionCreateTable)))
			args, err := GetCreateTableArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.TableInfo, args.TableInfo)
			require.EqualValues(t, inArgs.FKCheck, args.FKCheck)
		}
	})
	t.Run("create view", func(t *testing.T) {
		inArgs := &CreateTableArgs{
			TableInfo:      &TableInfo{ID: 122},
			OnExistReplace: true,
			OldViewTblID:   123,
		}
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionCreateView)))
			args, err := GetCreateTableArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.TableInfo, args.TableInfo)
			require.EqualValues(t, inArgs.OnExistReplace, args.OnExistReplace)
			require.EqualValues(t, inArgs.OldViewTblID, args.OldViewTblID)
		}
	})
	t.Run("create sequence", func(t *testing.T) {
		inArgs := &CreateTableArgs{
			TableInfo: &TableInfo{ID: 22},
		}
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionCreateSequence)))
			args, err := GetCreateTableArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.TableInfo, args.TableInfo)
		}
	})
}

func TestBatchCreateTableArgs(t *testing.T) {
	inArgs := &BatchCreateTableArgs{
		Tables: []*CreateTableArgs{
			{TableInfo: &TableInfo{ID: 100}, FKCheck: true},
			{TableInfo: &TableInfo{ID: 101}, FKCheck: false},
		},
	}
	// in job version 1, we only save one FKCheck value for all tables.
	j2 := &Job{}
	require.NoError(t, j2.Decode(getJobBytes(t, inArgs, JobVersion1, ActionCreateTables)))
	args, err := GetBatchCreateTableArgs(j2)
	require.NoError(t, err)
	for i := 0; i < len(inArgs.Tables); i++ {
		require.EqualValues(t, inArgs.Tables[i].TableInfo, args.Tables[i].TableInfo)
		require.EqualValues(t, true, args.Tables[i].FKCheck)
	}

	j2 = &Job{}
	require.NoError(t, j2.Decode(getJobBytes(t, inArgs, JobVersion2, ActionCreateTables)))
	args, err = GetBatchCreateTableArgs(j2)
	require.NoError(t, err)
	require.EqualValues(t, inArgs.Tables, args.Tables)
}

func TestDropTableArgs(t *testing.T) {
	inArgs := &DropTableArgs{
		Identifiers: []ast.Ident{
			{Schema: ast.NewCIStr("db"), Name: ast.NewCIStr("tbl")},
			{Schema: ast.NewCIStr("db2"), Name: ast.NewCIStr("tbl2")},
		},
		FKCheck: true,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionDropTable)))
		args, err := GetDropTableArgs(j2)
		require.NoError(t, err)
		require.EqualValues(t, inArgs, args)
	}
	for _, tp := range []ActionType{ActionDropView, ActionDropSequence} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))
			if v == JobVersion1 {
				require.Equal(t, json.RawMessage("null"), j2.RawArgs)
			} else {
				args, err := GetDropTableArgs(j2)
				require.NoError(t, err)
				require.EqualValues(t, inArgs, args)
			}
		}
	}
}

func TestFinishedDropTableArgs(t *testing.T) {
	inArgs := &DropTableArgs{
		StartKey:        []byte("xxx"),
		OldPartitionIDs: []int64{1, 2},
		OldRuleIDs:      []string{"schema/test/a/par1", "schema/test/a/par2"},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionDropTable)))
		args, err := GetFinishedDropTableArgs(j2)
		require.NoError(t, err)
		require.EqualValues(t, inArgs, args)
	}
}

func TestTruncateTableArgs(t *testing.T) {
	inArgs := &TruncateTableArgs{
		NewTableID:      1,
		FKCheck:         true,
		OldPartitionIDs: []int64{11, 2},
		NewPartitionIDs: []int64{2, 3},
	}
	for _, tp := range []ActionType{ActionTruncateTable, ActionTruncateTablePartition} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))
			args, err := GetTruncateTableArgs(j2)
			require.NoError(t, err)
			if tp == ActionTruncateTable {
				require.Equal(t, int64(1), args.NewTableID)
				require.Equal(t, true, args.FKCheck)
			} else {
				require.Equal(t, []int64{11, 2}, args.OldPartitionIDs)
			}
			require.Equal(t, []int64{2, 3}, args.NewPartitionIDs)
		}
	}

	inArgs = &TruncateTableArgs{
		OldPartitionIDs: []int64{5, 6},
	}
	for _, tp := range []ActionType{ActionTruncateTable, ActionTruncateTablePartition} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, tp)))
			args, err := GetFinishedTruncateTableArgs(j2)
			require.NoError(t, err)
			require.Equal(t, []int64{5, 6}, args.OldPartitionIDs)
		}
	}
}

func TestTablePartitionArgs(t *testing.T) {
	inArgs := &TablePartitionArgs{
		PartNames: []string{"a", "b"},
		PartInfo: &PartitionInfo{Type: ast.PartitionTypeRange, Definitions: []PartitionDefinition{
			{ID: 1, Name: ast.NewCIStr("a"), LessThan: []string{"1"}},
			{ID: 2, Name: ast.NewCIStr("b"), LessThan: []string{"2"}},
		}},
	}
	for _, tp := range []ActionType{
		ActionAlterTablePartitioning,
		ActionRemovePartitioning,
		ActionReorganizePartition,
		ActionAddTablePartition,
		ActionDropTablePartition,
	} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))
			args, err := GetTablePartitionArgs(j2)
			require.NoError(t, err)
			if v == JobVersion2 {
				require.Equal(t, inArgs, args)
			} else {
				if j2.Type != ActionAddTablePartition {
					require.Equal(t, inArgs.PartNames, args.PartNames)
				}
				if j2.Type != ActionDropTablePartition {
					require.Equal(t, inArgs.PartInfo, args.PartInfo)
				} else {
					require.EqualValues(t, &PartitionInfo{}, args.PartInfo)
				}
			}
		}
	}

	// for ActionDropTablePartition in V2, check PartInfo is not nil
	j2 := &Job{}
	require.NoError(t, j2.Decode(getJobBytes(t, &TablePartitionArgs{PartNames: []string{"a", "b"}},
		JobVersion2, ActionDropTablePartition)))
	args, err := GetTablePartitionArgs(j2)
	require.NoError(t, err)
	require.EqualValues(t, &PartitionInfo{}, args.PartInfo)

	for _, ver := range []JobVersion{JobVersion1, JobVersion2} {
		j := &Job{
			Version: ver,
			Type:    ActionAddTablePartition,
		}
		j.FillArgs(inArgs)
		_, err := j.Encode(true)
		require.NoError(t, err)
		partNames := []string{"aaaa", "bbb"}
		FillRollbackArgsForAddPartition(j,
			&TablePartitionArgs{
				PartNames: partNames,
				PartInfo: &PartitionInfo{Type: ast.PartitionTypeRange, Definitions: []PartitionDefinition{
					{ID: 1, Name: ast.NewCIStr("aaaa"), LessThan: []string{"1"}},
					{ID: 2, Name: ast.NewCIStr("bbb"), LessThan: []string{"2"}},
				}},
			})
		require.Len(t, j.args, 1)
		if ver == JobVersion1 {
			require.EqualValues(t, partNames, j.args[0].([]string))
		} else {
			args := j.args[0].(*TablePartitionArgs)
			require.EqualValues(t, partNames, args.PartNames)
			require.Nil(t, args.PartInfo)
			require.Nil(t, args.OldPhysicalTblIDs)
		}

		j.State = JobStateRollingback
		bytes, err := j.Encode(true)
		require.NoError(t, err)

		j2 := &Job{}
		require.NoError(t, j2.Decode(bytes))
		args, err := GetTablePartitionArgs(j2)
		require.NoError(t, err)
		require.EqualValues(t, partNames, args.PartNames)
		require.EqualValues(t, &PartitionInfo{}, args.PartInfo)
	}
}

func TestFinishedTablePartitionArgs(t *testing.T) {
	inArgs := &TablePartitionArgs{
		OldPhysicalTblIDs: []int64{1, 2},
	}
	for _, tp := range []ActionType{
		ActionAlterTablePartitioning,
		ActionRemovePartitioning,
		ActionReorganizePartition,
		ActionDropTablePartition,
	} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, tp)))
			args, err := GetFinishedTablePartitionArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.OldPhysicalTblIDs, args.OldPhysicalTblIDs)
		}
	}

	// ActionAddTablePartition can use FillFinishedArgs when rollback
	for _, ver := range []JobVersion{JobVersion1, JobVersion2} {
		j := &Job{
			Version: ver,
			Type:    ActionAddTablePartition,
			State:   JobStateRollbackDone,
		}
		j.FillFinishedArgs(inArgs)
		bytes, err := j.Encode(true)
		require.NoError(t, err)

		j2 := &Job{}
		require.NoError(t, j2.Decode(bytes))
		args, err := GetFinishedTablePartitionArgs(j2)
		require.NoError(t, err)
		require.EqualValues(t, inArgs.OldPhysicalTblIDs, args.OldPhysicalTblIDs)
	}
}

func TestExchangeTablePartitionArgs(t *testing.T) {
	inArgs := &ExchangeTablePartitionArgs{
		PartitionID:    100,
		PTSchemaID:     123,
		PTTableID:      345,
		PartitionName:  "c",
		WithValidation: true,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionExchangeTablePartition)))
		args, err := GetExchangeTablePartitionArgs(j2)
		require.NoError(t, err)
		require.EqualValues(t, inArgs, args)
	}
}

func TestAlterTablePartitionArgs(t *testing.T) {
	inArgs := &AlterTablePartitionArgs{
		PartitionID:   123,
		LabelRule:     &pdhttp.LabelRule{ID: "ss"},
		PolicyRefInfo: &PolicyRefInfo{ID: 462},
	}
	for _, tp := range []ActionType{ActionAlterTablePartitionAttributes, ActionAlterTablePartitionPlacement} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))
			args, err := GetAlterTablePartitionArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.PartitionID, args.PartitionID)
			if tp == ActionAlterTablePartitionAttributes {
				require.EqualValues(t, inArgs.LabelRule, args.LabelRule)
			} else {
				require.EqualValues(t, inArgs.PolicyRefInfo, args.PolicyRefInfo)
			}
		}
	}
}

func TestRenameTableArgs(t *testing.T) {
	inArgs := &RenameTableArgs{
		OldSchemaID:   9527,
		OldSchemaName: ast.NewCIStr("old_schema_name"),
		NewTableName:  ast.NewCIStr("new_table_name"),
	}

	jobvers := []JobVersion{JobVersion1, JobVersion2}
	for _, jobver := range jobvers {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, jobver, ActionRenameTable)))

		// get the args after decode
		args, err := GetRenameTableArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetRenameTablesArgs(t *testing.T) {
	inArgs := &RenameTablesArgs{
		RenameTableInfos: []*RenameTableArgs{
			{OldSchemaID: 1, OldSchemaName: ast.CIStr{O: "db1", L: "db1"},
				NewTableName: ast.CIStr{O: "tb3", L: "tb3"}, OldTableName: ast.CIStr{O: "tb1", L: "tb1"},
				NewSchemaID: 3, TableID: 100},
			{OldSchemaID: 2, OldSchemaName: ast.CIStr{O: "db2", L: "db2"},
				NewTableName: ast.CIStr{O: "tb2", L: "tb2"}, OldTableName: ast.CIStr{O: "tb4", L: "tb4"},
				NewSchemaID: 3, TableID: 101},
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionRenameTables)))

		args, err := GetRenameTablesArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs.RenameTableInfos[0], args.RenameTableInfos[0])
		require.Equal(t, inArgs.RenameTableInfos[1], args.RenameTableInfos[1])
	}
}

func TestResourceGroupArgs(t *testing.T) {
	inArgs := &ResourceGroupArgs{
		RGInfo: &ResourceGroupInfo{ID: 100, Name: ast.NewCIStr("rg_name")},
	}
	for _, tp := range []ActionType{ActionCreateResourceGroup, ActionAlterResourceGroup, ActionDropResourceGroup} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))
			args, err := GetResourceGroupArgs(j2)
			require.NoError(t, err)
			if tp == ActionDropResourceGroup {
				require.EqualValues(t, inArgs.RGInfo.Name, args.RGInfo.Name)
			} else {
				require.EqualValues(t, inArgs, args)
			}
		}
	}
}

func TestGetAlterSequenceArgs(t *testing.T) {
	inArgs := &AlterSequenceArgs{
		Ident: ast.Ident{
			Schema: ast.NewCIStr("test_db"),
			Name:   ast.NewCIStr("test_t"),
		},
		SeqOptions: []*ast.SequenceOption{
			{
				Tp:       ast.SequenceOptionIncrementBy,
				IntValue: 7527,
			}, {
				Tp:       ast.SequenceCache,
				IntValue: 9528,
			},
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAlterSequence)))
		args, err := GetAlterSequenceArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetRebaseAutoIDArgs(t *testing.T) {
	inArgs := &RebaseAutoIDArgs{
		NewBase: 9527,
		Force:   true,
	}
	for _, tp := range []ActionType{ActionRebaseAutoID, ActionRebaseAutoRandomBase} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))
			args, err := GetRebaseAutoIDArgs(j2)
			require.NoError(t, err)
			require.Equal(t, inArgs, args)
		}
	}
}

func TestGetModifyTableCommentArgs(t *testing.T) {
	inArgs := &ModifyTableCommentArgs{
		Comment: "TiDB is great",
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionModifyTableComment)))
		args, err := GetModifyTableCommentArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetAlterIndexVisibilityArgs(t *testing.T) {
	inArgs := &AlterIndexVisibilityArgs{
		IndexName: ast.NewCIStr("index-name"),
		Invisible: true,
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAlterIndexVisibility)))
		args, err := GetAlterIndexVisibilityArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetAddForeignKeyArgs(t *testing.T) {
	inArgs := &AddForeignKeyArgs{
		FkInfo: &FKInfo{
			ID:   7527,
			Name: ast.NewCIStr("fk-name"),
		},
		FkCheck: true,
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddForeignKey)))
		args, err := GetAddForeignKeyArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetModifyTableAutoIDCacheArgs(t *testing.T) {
	inArgs := &ModifyTableAutoIDCacheArgs{
		NewCache: 7527,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionModifyTableAutoIDCache)))
		args, err := GetModifyTableAutoIDCacheArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetShardRowIDArgs(t *testing.T) {
	inArgs := &ShardRowIDArgs{
		ShardRowIDBits: 101,
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionShardRowID)))
		args, err := GetShardRowIDArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetDropForeignKeyArgs(t *testing.T) {
	inArgs := &DropForeignKeyArgs{
		FkName: ast.NewCIStr("fk-name"),
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionDropForeignKey)))
		args, err := GetDropForeignKeyArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetAlterTTLInfoArgs(t *testing.T) {
	ttlEanble := true
	ttlCronJobSchedule := "ttl-schedule"
	inArgs := &AlterTTLInfoArgs{
		TTLInfo: &TTLInfo{
			ColumnName:       ast.NewCIStr("column_name"),
			IntervalExprStr:  "1",
			IntervalTimeUnit: 10010,
		},
		TTLEnable:          &ttlEanble,
		TTLCronJobSchedule: &ttlCronJobSchedule,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAlterTTLInfo)))
		args, err := GetAlterTTLInfoArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestAddCheckConstraintArgs(t *testing.T) {
	Constraint :=
		&ConstraintInfo{
			Name:       ast.NewCIStr("t3_c1"),
			Table:      ast.NewCIStr("t3"),
			ExprString: "id<10",
			State:      StateDeleteOnly,
		}
	inArgs := &AddCheckConstraintArgs{
		Constraint: Constraint,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddCheckConstraint)))
		args, err := GetAddCheckConstraintArgs(j2)
		require.NoError(t, err)
		require.Equal(t, "t3_c1", args.Constraint.Name.O)
		require.Equal(t, "t3", args.Constraint.Table.O)
		require.Equal(t, "id<10", args.Constraint.ExprString)
		require.Equal(t, StateDeleteOnly, args.Constraint.State)
	}
}

func TestCheckConstraintArgs(t *testing.T) {
	inArgs := &CheckConstraintArgs{
		ConstraintName: ast.NewCIStr("c1"),
		Enforced:       true,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionDropCheckConstraint)))
		args, err := GetCheckConstraintArgs(j2)
		require.NoError(t, err)
		require.Equal(t, "c1", args.ConstraintName.O)
		require.True(t, args.Enforced)
	}
}

func TestGetAlterTablePlacementArgs(t *testing.T) {
	inArgs := &AlterTablePlacementArgs{
		PlacementPolicyRef: &PolicyRefInfo{
			ID:   7527,
			Name: ast.NewCIStr("placement-policy"),
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAlterTablePlacement)))
		args, err := GetAlterTablePlacementArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}

	// test PlacementPolicyRef is nil
	inArgs = &AlterTablePlacementArgs{
		PlacementPolicyRef: nil,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAlterTablePlacement)))
		args, err := GetAlterTablePlacementArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetSetTiFlashReplicaArgs(t *testing.T) {
	inArgs := &SetTiFlashReplicaArgs{
		TiflashReplica: ast.TiFlashReplicaSpec{
			Count:  3,
			Labels: []string{"TiFlash1", "TiFlash2", "TiFlash3"},
			Hypo:   true,
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionSetTiFlashReplica)))
		args, err := GetSetTiFlashReplicaArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestGetUpdateTiFlashReplicaStatusArgs(t *testing.T) {
	inArgs := &UpdateTiFlashReplicaStatusArgs{
		Available:  true,
		PhysicalID: 1001,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionUpdateTiFlashReplicaStatus)))
		args, err := GetUpdateTiFlashReplicaStatusArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}
func TestLockTableArgs(t *testing.T) {
	inArgs := &LockTablesArgs{
		LockTables:    []TableLockTpInfo{{1, 1, ast.TableLockNone}},
		UnlockTables:  []TableLockTpInfo{{2, 2, ast.TableLockNone}},
		IndexOfLock:   13,
		IndexOfUnlock: 24,
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		for _, tp := range []ActionType{ActionLockTable, ActionUnlockTable} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))

			args, err := GetLockTablesArgs(j2)
			require.NoError(t, err)
			require.Equal(t, inArgs.LockTables, args.LockTables)
			require.Equal(t, inArgs.UnlockTables, args.UnlockTables)
			require.Equal(t, inArgs.IndexOfLock, args.IndexOfLock)
			require.Equal(t, inArgs.IndexOfUnlock, args.IndexOfUnlock)
		}
	}
}

func TestRepairTableArgs(t *testing.T) {
	inArgs := &RepairTableArgs{&TableInfo{ID: 1, Name: ast.NewCIStr("t")}}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionRepairTable)))

		args, err := GetRepairTableArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs.TableInfo, args.TableInfo)
	}
}

func TestRecoverArgs(t *testing.T) {
	recoverInfo := &RecoverTableInfo{
		SchemaID:  1,
		DropJobID: 2,
		TableInfo: &TableInfo{
			ID:   100,
			Name: ast.NewCIStr("table"),
		},
		OldSchemaName: "old",
		OldTableName:  "table",
	}

	inArgs := &RecoverArgs{
		RecoverInfo: &RecoverSchemaInfo{
			RecoverTableInfos: []*RecoverTableInfo{recoverInfo},
		},
		CheckFlag: 2,
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		for _, tp := range []ActionType{ActionRecoverTable, ActionRecoverSchema} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))

			args, err := GetRecoverArgs(j2)
			require.NoError(t, err)
			require.Equal(t, inArgs.CheckFlag, args.CheckFlag)
			require.Equal(t, inArgs.RecoverInfo, args.RecoverInfo)
		}
	}
}

func TestPlacementPolicyArgs(t *testing.T) {
	inArgs := &PlacementPolicyArgs{
		Policy:         &PolicyInfo{ID: 1, Name: ast.NewCIStr("policy"), State: StateDeleteOnly},
		PolicyName:     ast.NewCIStr("policy_name"),
		PolicyID:       123,
		ReplaceOnExist: false,
	}
	for _, tp := range []ActionType{ActionCreatePlacementPolicy, ActionAlterPlacementPolicy, ActionDropPlacementPolicy} {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, tp)))
			j2.SchemaID = inArgs.PolicyID
			args, err := GetPlacementPolicyArgs(j2)
			require.NoError(t, err)
			if tp == ActionCreatePlacementPolicy {
				require.EqualValues(t, inArgs.Policy, args.Policy)
				require.EqualValues(t, inArgs.ReplaceOnExist, args.ReplaceOnExist)
			} else if tp == ActionAlterPlacementPolicy {
				require.EqualValues(t, inArgs.Policy, args.Policy)
				require.EqualValues(t, inArgs.PolicyID, args.PolicyID)
			} else {
				require.EqualValues(t, inArgs.PolicyName, args.PolicyName)
				require.EqualValues(t, inArgs.PolicyID, args.PolicyID)
			}
		}
	}
}

func TestGetSetDefaultValueArgs(t *testing.T) {
	inArgs := &SetDefaultValueArgs{
		Col: &ColumnInfo{
			ID:   7527,
			Name: ast.NewCIStr("col_name"),
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionSetDefaultValue)))
		args, err := GetSetDefaultValueArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestFlashbackClusterArgs(t *testing.T) {
	inArgs := &FlashbackClusterArgs{
		FlashbackTS:       111,
		StartTS:           222,
		CommitTS:          333,
		EnableGC:          true,
		EnableAutoAnalyze: true,
		EnableTTLJob:      true,
		SuperReadOnly:     true,
		LockedRegionCnt:   444,
		PDScheduleValue:   map[string]any{"t1": 123.0},
		FlashbackKeyRanges: []KeyRange{
			{StartKey: []byte("db1"), EndKey: []byte("db2")},
			{StartKey: []byte("db2"), EndKey: []byte("db3")},
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionFlashbackCluster)))
		args, err := GetFlashbackClusterArgs(j2)
		require.NoError(t, err)

		require.Equal(t, inArgs, args)
	}
}

func TestDropColumnArgs(t *testing.T) {
	inArgs := &TableColumnArgs{
		Col: &ColumnInfo{
			Name: ast.NewCIStr("col_name"),
		},
		IgnoreExistenceErr: true,
		IndexIDs:           []int64{1, 2, 3},
		PartitionIDs:       []int64{4, 5, 6},
		Pos:                &ast.ColumnPosition{},
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionDropColumn)))
		args, err := GetTableColumnArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
		if v == JobVersion1 {
			require.Len(t, j2.args, 4)
		}
	}

	j2 := &Job{}
	require.NoError(t, j2.Decode(getJobBytes(t, &TableColumnArgs{Col: &ColumnInfo{}}, JobVersion1, ActionDropColumn)))
	var rawArgs []json.RawMessage
	require.NoError(t, json.Unmarshal(j2.RawArgs, &rawArgs))
	require.Len(t, rawArgs, 2)
}

func TestAddColumnArgs(t *testing.T) {
	inArgs := &TableColumnArgs{
		Col: &ColumnInfo{
			ID:   7527,
			Name: ast.NewCIStr("col_name"),
		},
		Pos: &ast.ColumnPosition{
			Tp: ast.ColumnPositionFirst,
		},
		Offset:             1001,
		IgnoreExistenceErr: true,
	}
	dropArgs := &TableColumnArgs{
		Col: &ColumnInfo{
			Name: ast.NewCIStr("drop_column"),
		},
		Pos: &ast.ColumnPosition{},
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddColumn)))
		args, err := GetTableColumnArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)

		FillRollBackArgsForAddColumn(j2, dropArgs)
		j2.State = JobStateRollingback
		jobBytes, err := j2.Encode(true)
		require.NoError(t, err)
		j3 := &Job{}
		require.NoError(t, j3.Decode(jobBytes))
		args, err = GetTableColumnArgs(j3)
		require.NoError(t, err)
		require.Equal(t, dropArgs, args)
	}
}
func TestAlterTableAttributesArgs(t *testing.T) {
	inArgs := &AlterTableAttributesArgs{
		LabelRule: &pdhttp.LabelRule{
			ID:       "id",
			Index:    2,
			RuleType: "rule",
			Labels:   []pdhttp.RegionLabel{{Key: "key", Value: "value"}},
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAlterTableAttributes)))
		args, err := GetAlterTableAttributesArgs(j2)
		require.NoError(t, err)

		require.Equal(t, *inArgs.LabelRule, *args.LabelRule)
	}
}

func TestAddIndexArgs(t *testing.T) {
	inArgs := &ModifyIndexArgs{
		IndexArgs: []*IndexArg{{
			Global:                  false,
			Unique:                  true,
			IndexName:               ast.NewCIStr("idx1"),
			IndexPartSpecifications: []*ast.IndexPartSpecification{{Length: 2}},
			IndexOption:             &ast.IndexOption{},
			HiddenCols:              []*ColumnInfo{{}, {}},
			SQLMode:                 mysql.ModeANSI,
			IndexID:                 1,
			IfExist:                 false,
			IsGlobal:                false,
			FuncExpr:                "test_string",
		}},
		PartitionIDs: []int64{100, 101, 102},
		OpType:       OpAddIndex,
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		inArgs.IndexArgs[0].IsVector = false
		inArgs.IndexArgs[0].IsPK = false
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddIndex)))

		args, err := GetModifyIndexArgs(j2)
		require.NoError(t, err)

		a := args.IndexArgs[0]
		require.Equal(t, inArgs.IndexArgs[0].Global, a.Global)
		require.Equal(t, inArgs.IndexArgs[0].Unique, a.Unique)
		require.Equal(t, inArgs.IndexArgs[0].IndexName, a.IndexName)
		require.Equal(t, inArgs.IndexArgs[0].IndexPartSpecifications, a.IndexPartSpecifications)
		require.Equal(t, inArgs.IndexArgs[0].IndexOption, a.IndexOption)
		require.Equal(t, inArgs.IndexArgs[0].HiddenCols, a.HiddenCols)
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		inArgs.IndexArgs[0].IsVector = false
		inArgs.IndexArgs[0].IsPK = true
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddPrimaryKey)))

		args, err := GetModifyIndexArgs(j2)
		require.NoError(t, err)

		a := args.IndexArgs[0]
		require.Equal(t, inArgs.IndexArgs[0].Global, a.Global)
		require.Equal(t, inArgs.IndexArgs[0].Unique, a.Unique)
		require.Equal(t, inArgs.IndexArgs[0].IndexName, a.IndexName)
		require.Equal(t, inArgs.IndexArgs[0].IndexPartSpecifications, a.IndexPartSpecifications)
		require.Equal(t, inArgs.IndexArgs[0].SQLMode, a.SQLMode)
		require.Equal(t, inArgs.IndexArgs[0].IndexOption, a.IndexOption)
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		inArgs.IndexArgs[0].IsVector = true
		inArgs.IndexArgs[0].IsPK = false
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddVectorIndex)))

		args, err := GetModifyIndexArgs(j2)
		require.NoError(t, err)

		a := args.IndexArgs[0]
		require.Equal(t, inArgs.IndexArgs[0].IndexName, a.IndexName)
		require.Equal(t, inArgs.IndexArgs[0].IndexPartSpecifications, a.IndexPartSpecifications)
		require.Equal(t, inArgs.IndexArgs[0].IndexOption, a.IndexOption)
		require.Equal(t, inArgs.IndexArgs[0].FuncExpr, a.FuncExpr)
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionAddIndex)))

		args, err := GetFinishedModifyIndexArgs(j2)
		require.NoError(t, err)

		a := args.IndexArgs[0]
		require.Equal(t, inArgs.IndexArgs[0].IndexID, a.IndexID)
		require.Equal(t, inArgs.IndexArgs[0].IfExist, a.IfExist)
		require.Equal(t, inArgs.IndexArgs[0].IsGlobal, a.IsGlobal)
		require.Equal(t, inArgs.PartitionIDs, args.PartitionIDs)
	}
}

func TestDropIndexArguements(t *testing.T) {
	checkFunc := func(t *testing.T, inArgs *ModifyIndexArgs) {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionDropIndex)))
			args, err := GetDropIndexArgs(j2)
			require.NoError(t, err)
			for i, expect := range inArgs.IndexArgs {
				require.EqualValues(t, expect.IndexName, args.IndexArgs[i].IndexName)
				require.EqualValues(t, expect.IfExist, args.IndexArgs[i].IfExist)
			}

			j2 = &Job{}
			require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionDropIndex)))
			args2, err := GetFinishedModifyIndexArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.IndexArgs, args2.IndexArgs)
			require.EqualValues(t, inArgs.PartitionIDs, args2.PartitionIDs)
		}
	}

	inArgs := &ModifyIndexArgs{
		IndexArgs: []*IndexArg{
			{
				IndexName: ast.NewCIStr("i2"),
				IfExist:   true,
				IsVector:  true,
				IndexID:   1,
			},
		},
		PartitionIDs: []int64{100, 101, 102, 103},
		OpType:       OpDropIndex,
	}
	checkFunc(t, inArgs)
}

func TestGetRenameIndexArgs(t *testing.T) {
	inArgs := &ModifyIndexArgs{
		IndexArgs: []*IndexArg{
			{IndexName: ast.NewCIStr("old")},
			{IndexName: ast.NewCIStr("new")},
		},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionRenameIndex)))

		args, err := GetModifyIndexArgs(j2)
		require.NoError(t, err)
		require.Equal(t, inArgs, args)
	}
}

func TestModifyColumnsArgs(t *testing.T) {
	inArgs := &ModifyColumnArgs{
		Column:           &ColumnInfo{ID: 111, Name: ast.NewCIStr("col1")},
		OldColumnName:    ast.NewCIStr("aa"),
		Position:         &ast.ColumnPosition{Tp: ast.ColumnPositionFirst},
		ModifyColumnType: 1,
		NewShardBits:     123,
		ChangingColumn:   &ColumnInfo{ID: 222, Name: ast.NewCIStr("col2")},
		RedundantIdxs:    []int64{1, 2},
		IndexIDs:         []int64{3, 4},
		PartitionIDs:     []int64{5, 6},
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionModifyColumn)))
		args, err := GetModifyColumnArgs(j2)
		require.NoError(t, err)

		require.Equal(t, *inArgs.Column, *args.Column)
		require.Equal(t, inArgs.OldColumnName, args.OldColumnName)
		require.Equal(t, inArgs.Position, args.Position)
		require.Equal(t, inArgs.ModifyColumnType, args.ModifyColumnType)
		require.Equal(t, inArgs.NewShardBits, args.NewShardBits)
		require.Equal(t, *inArgs.ChangingColumn, *args.ChangingColumn)
		require.Equal(t, inArgs.ChangingIdxs, args.ChangingIdxs)
		require.Equal(t, inArgs.RedundantIdxs, args.RedundantIdxs)

		if v == JobVersion1 {
			var rawArgs []json.RawMessage
			require.NoError(t, json.Unmarshal(j2.RawArgs, &rawArgs))
			require.Len(t, rawArgs, 8)
		}
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionModifyColumn)))
		args, err := GetFinishedModifyColumnArgs(j2)
		require.NoError(t, err)

		require.Equal(t, inArgs.IndexIDs, args.IndexIDs)
		require.Equal(t, inArgs.PartitionIDs, args.PartitionIDs)
	}

	j2 := &Job{}
	require.NoError(t, j2.Decode(getJobBytes(t, &ModifyColumnArgs{}, JobVersion1, ActionModifyColumn)))
	var rawArgs []json.RawMessage
	require.NoError(t, json.Unmarshal(j2.RawArgs, &rawArgs))
	require.Len(t, rawArgs, 5)
}
