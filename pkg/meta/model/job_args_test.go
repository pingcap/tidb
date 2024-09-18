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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
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
