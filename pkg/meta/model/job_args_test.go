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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestGetOrDecodeArgsV2(t *testing.T) {
	j := &Job{
		Version: JobVersion2,
		Type:    ActionTruncateTable,
		Args: []any{&TruncateTableArgs{
			FKCheck: true,
		}},
	}
	_, err := j.Encode(true)
	require.NoError(t, err)
	require.NotNil(t, j.RawArgs)
	// return existing argsV2
	argsV2, err := getOrDecodeArgsV2[*TruncateTableArgs](j)
	require.NoError(t, err)
	require.Same(t, j.Args[0], argsV2)
	// unmarshal from json
	var argsBak *TruncateTableArgs
	argsBak, j.Args = j.Args[0].(*TruncateTableArgs), nil
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

func TestTruncateTableArgs(t *testing.T) {
	inArgs := &TruncateTableArgs{
		NewTableID:      1,
		FKCheck:         true,
		NewPartitionIDs: []int64{2, 3},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionTruncateTable)))
		args, err := GetTruncateTableArgs(j2)
		require.NoError(t, err)
		require.Equal(t, int64(1), args.NewTableID)
		require.Equal(t, true, args.FKCheck)
		require.Equal(t, []int64{2, 3}, args.NewPartitionIDs)
	}

	inArgs = &TruncateTableArgs{
		OldPartitionIDs: []int64{5, 6},
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		j2 := &Job{}
		require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionTruncateTable)))
		args, err := GetFinishedTruncateTableArgs(j2)
		require.NoError(t, err)
		require.Equal(t, []int64{5, 6}, args.OldPartitionIDs)
	}
}

func TestDecodeAddIndexArgsCompatibility(t *testing.T) {
	cases := []struct {
		raw                     json.RawMessage
		uniques                 []bool
		indexNames              []model.CIStr
		indexPartSpecifications [][]*ast.IndexPartSpecification
		indexOptions            []*ast.IndexOption
		hiddenCols              [][]*ColumnInfo
	}{
		{
			raw: json.RawMessage(`[
true,
{"O":"t","L":"t"},
[
	{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"a","L":"a"}},"Length":-1,"Desc":false,"Expr":null},
	{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"b","L":"b"}},"Length":-1,"Desc":false,"Expr":null}
],
null,
[],
false]`),
			uniques: []bool{true},
			indexNames: []model.CIStr{
				{O: "t", L: "t"},
			},
			indexPartSpecifications: [][]*ast.IndexPartSpecification{
				{
					{
						Column: &ast.ColumnName{
							Schema: model.CIStr{O: "", L: ""},
							Table:  model.CIStr{O: "", L: ""},
							Name:   model.CIStr{O: "a", L: "a"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
					{
						Column: &ast.ColumnName{
							Schema: model.CIStr{O: "", L: ""},
							Table:  model.CIStr{O: "", L: ""},
							Name:   model.CIStr{O: "b", L: "b"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
				},
			},
			indexOptions: []*ast.IndexOption{nil},
			hiddenCols:   [][]*ColumnInfo{{}},
		},
		{
			raw: json.RawMessage(`[
[false,true],
[{"O":"t","L":"t"},{"O":"t1","L":"t1"}],
[
	[
		{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"a","L":"a"}},"Length":-1,"Desc":false,"Expr":null},
		{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"b","L":"b"}},"Length":-1,"Desc":false,"Expr":null}
	],
	[
		{"Column":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"a","L":"a"}},"Length":-1,"Desc":false,"Expr":null}
	]
],
[null,null],
[[],[]],
[false,false]]`),
			uniques: []bool{false, true},
			indexNames: []model.CIStr{
				{O: "t", L: "t"}, {O: "t1", L: "t1"},
			},
			indexPartSpecifications: [][]*ast.IndexPartSpecification{
				{
					{
						Column: &ast.ColumnName{
							Schema: model.CIStr{O: "", L: ""},
							Table:  model.CIStr{O: "", L: ""},
							Name:   model.CIStr{O: "a", L: "a"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
					{
						Column: &ast.ColumnName{
							Schema: model.CIStr{O: "", L: ""},
							Table:  model.CIStr{O: "", L: ""},
							Name:   model.CIStr{O: "b", L: "b"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
				},
				{
					{
						Column: &ast.ColumnName{
							Schema: model.CIStr{O: "", L: ""},
							Table:  model.CIStr{O: "", L: ""},
							Name:   model.CIStr{O: "a", L: "a"},
						},
						Length: -1,
						Desc:   false,
						Expr:   nil,
					},
				},
			},
			indexOptions: []*ast.IndexOption{nil, nil},
			hiddenCols:   [][]*ColumnInfo{{}, {}},
		},
	}

	for _, c := range cases {
		job := &Job{
			Version: JobVersion1,
			Type:    ActionAddIndex,
			RawArgs: c.raw,
		}

		args, err := getAddIndexArgs(job)
		require.NoError(t, err)
		for i, a := range args.IndexArgs {
			require.Equal(t, c.uniques[i], a.Unique)
			require.Equal(t, c.indexNames[i], a.IndexName)
			require.Equal(t, c.indexPartSpecifications[i], a.IndexPartSpecifications)
			require.Equal(t, c.indexOptions[i], a.IndexOption)
			require.Equal(t, c.hiddenCols[i], a.HiddenCols)
		}
	}
}

func TestAddIndexArgs(t *testing.T) {
	inArgs := &AddIndexArgs{
		IndexArgs: []*IndexArg{{
			Global:                  false,
			Unique:                  true,
			IndexName:               model.NewCIStr("idx1"),
			IndexPartSpecifications: []*ast.IndexPartSpecification{},
			IndexOption:             &ast.IndexOption{},
			HiddenCols:              []*ColumnInfo{{}, {}},
			SQLMode:                 mysql.ModeANSI,
			Warning:                 "test warning",
			AddIndexID:              1,
			IfExist:                 false,
			IsGlobal:                false,
		}},
		PartitionIDs: []int64{100, 101, 102},
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		inArgs.IsPK = false
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddIndex)))

		args, err := GetAddIndexArgs(j2)
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
		inArgs.IsFinishedArg = true
		j2 := &Job{}
		require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionAddIndex)))

		args, err := GetFinishedAddIndexArgs(j2)
		require.NoError(t, err)

		a := args.IndexArgs[0]
		require.Equal(t, inArgs.IndexArgs[0].AddIndexID, a.AddIndexID)
		require.Equal(t, inArgs.IndexArgs[0].IfExist, a.IfExist)
		require.Equal(t, inArgs.IndexArgs[0].IsGlobal, a.IsGlobal)
		require.Equal(t, inArgs.PartitionIDs, args.PartitionIDs)
	}

	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		inArgs.IsPK = true
		j2 := &Job{}
		require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionAddPrimaryKey)))

		args, err := getAddPrimaryKeyArgs(j2)
		require.NoError(t, err)

		a := args.IndexArgs[0]
		require.Equal(t, inArgs.IndexArgs[0].Global, a.Global)
		require.Equal(t, inArgs.IndexArgs[0].Unique, a.Unique)
		require.Equal(t, inArgs.IndexArgs[0].IndexName, a.IndexName)
		require.Equal(t, inArgs.IndexArgs[0].IndexPartSpecifications, a.IndexPartSpecifications)
		require.Equal(t, inArgs.IndexArgs[0].SQLMode, a.SQLMode)
		require.Equal(t, inArgs.IndexArgs[0].Warning, a.Warning)
		require.Equal(t, inArgs.IndexArgs[0].IndexOption, a.IndexOption)
	}
	for _, v := range []JobVersion{JobVersion1, JobVersion2} {
		inArgs.IsFinishedArg = true
		j2 := &Job{}
		require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionAddIndex)))

		args, err := GetFinishedAddIndexArgs(j2)
		require.NoError(t, err)

		a := args.IndexArgs[0]
		require.Equal(t, inArgs.IndexArgs[0].AddIndexID, a.AddIndexID)
		require.Equal(t, inArgs.IndexArgs[0].IfExist, a.IfExist)
		require.Equal(t, inArgs.IndexArgs[0].IsGlobal, a.IsGlobal)
		require.Equal(t, inArgs.PartitionIDs, args.PartitionIDs)
	}
}

func TestDropIndex(t *testing.T) {
	checkFunc := func(t *testing.T, inArgs *DropIndexArgs) {
		for _, v := range []JobVersion{JobVersion1, JobVersion2} {
			j2 := &Job{}
			require.NoError(t, j2.Decode(getJobBytes(t, inArgs, v, ActionDropIndex)))
			args, err := GetDropIndexArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.IndexNames, args.IndexNames)
			require.EqualValues(t, inArgs.IfExists, args.IfExists)

			require.NoError(t, j2.Decode(getFinishedJobBytes(t, inArgs, v, ActionDropIndex)))
			args2, err := GetFinishedDropIndexArgs(j2)
			require.NoError(t, err)
			require.EqualValues(t, inArgs.IndexNames, args2.IndexNames)
			require.EqualValues(t, inArgs.IfExists, args2.IfExists)
			require.EqualValues(t, inArgs.IndexIDs, args2.IndexIDs)
			require.EqualValues(t, inArgs.PartitionIDs, args2.PartitionIDs)
		}
	}

	inArgs := &DropIndexArgs{
		IndexNames:   []model.CIStr{model.NewCIStr("i2")},
		IfExists:     []bool{false},
		IndexIDs:     []int64{1, 2, 3},
		PartitionIDs: []int64{100, 101, 102, 103},
	}

	inArgs = &DropIndexArgs{
		IndexNames:   []model.CIStr{model.NewCIStr("i1"), model.NewCIStr("i2")},
		IfExists:     []bool{false, false},
		IndexIDs:     []int64{1},
		PartitionIDs: []int64{100, 101, 102, 103},
	}
	checkFunc(t, inArgs)
}

func TestRenameTableArgs(t *testing.T) {
	inArgs := &RenameTableArgs{
		OldSchemaID:   9527,
		OldSchemaName: model.NewCIStr("old_schema_name"),
		NewTableName:  model.NewCIStr("new_table_name"),
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

func TestUpdateRenameTableArgs(t *testing.T) {
	inArgs := &RenameTableArgs{
		OldSchemaID:   9527,
		OldSchemaName: model.NewCIStr("old_schema_name"),
		NewTableName:  model.NewCIStr("new_table_name"),
	}

	jobvers := []JobVersion{JobVersion1, JobVersion2}
	for _, jobver := range jobvers {
		job := &Job{
			SchemaID: 9528,
			Version:  jobver,
			Type:     ActionRenameTable,
		}
		job.FillArgs(inArgs)

		err := UpdateRenameTableArgs(job)
		require.NoError(t, err)

		args, err := GetRenameTableArgs(job)
		require.NoError(t, err)
		require.Equal(t, &RenameTableArgs{
			OldSchemaID:   9528,
			OldSchemaName: model.NewCIStr("old_schema_name"),
			NewTableName:  model.NewCIStr("new_table_name"),
		}, args)
	}
}
