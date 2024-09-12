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
