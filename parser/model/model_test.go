// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/stretchr/testify/require"
)

func TestT(t *testing.T) {
	abc := NewCIStr("aBC")
	require.Equal(t, "aBC", abc.O)
	require.Equal(t, "abc", abc.L)
	require.Equal(t, "aBC", abc.String())
}

func TestModelBasic(t *testing.T) {
	column := &ColumnInfo{
		ID:           1,
		Name:         NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column.Flag |= mysql.PriKeyFlag

	index := &IndexInfo{
		Name:  NewCIStr("key"),
		Table: NewCIStr("t"),
		Columns: []*IndexColumn{
			{
				Name:   NewCIStr("c"),
				Offset: 0,
				Length: 10,
			}},
		Unique:  true,
		Primary: true,
	}

	fk := &FKInfo{
		RefCols: []CIStr{NewCIStr("a")},
		Cols:    []CIStr{NewCIStr("a")},
	}

	seq := &SequenceInfo{
		Increment: 1,
		MinValue:  1,
		MaxValue:  100,
	}

	table := &TableInfo{
		ID:          1,
		Name:        NewCIStr("t"),
		Charset:     "utf8",
		Collate:     "utf8_bin",
		Columns:     []*ColumnInfo{column},
		Indices:     []*IndexInfo{index},
		ForeignKeys: []*FKInfo{fk},
		PKIsHandle:  true,
	}

	table2 := &TableInfo{
		ID:       2,
		Name:     NewCIStr("s"),
		Sequence: seq,
	}

	dbInfo := &DBInfo{
		ID:      1,
		Name:    NewCIStr("test"),
		Charset: "utf8",
		Collate: "utf8_bin",
		Tables:  []*TableInfo{table},
	}

	n := dbInfo.Clone()
	require.Equal(t, dbInfo, n)

	pkName := table.GetPkName()
	require.Equal(t, NewCIStr("c"), pkName)
	newColumn := table.GetPkColInfo()
	require.Equal(t, true, newColumn.Hidden)
	require.Equal(t, column, newColumn)
	inIdx := table.ColumnIsInIndex(column)
	require.Equal(t, true, inIdx)
	tp := IndexTypeBtree
	require.Equal(t, "BTREE", tp.String())
	tp = IndexTypeHash
	require.Equal(t, "HASH", tp.String())
	tp = 1e5
	require.Equal(t, "", tp.String())
	has := index.HasPrefixIndex()
	require.Equal(t, true, has)
	require.Equal(t, TSConvert2Time(table.UpdateTS), table.GetUpdateTime())
	require.True(t, table2.IsSequence())
	require.False(t, table2.IsBaseTable())

	// Corner cases
	column.Flag ^= mysql.PriKeyFlag
	pkName = table.GetPkName()
	require.Equal(t, NewCIStr(""), pkName)
	newColumn = table.GetPkColInfo()
	require.Nil(t, newColumn)
	anCol := &ColumnInfo{
		Name: NewCIStr("d"),
	}
	exIdx := table.ColumnIsInIndex(anCol)
	require.Equal(t, false, exIdx)
	anIndex := &IndexInfo{
		Columns: []*IndexColumn{},
	}
	no := anIndex.HasPrefixIndex()
	require.Equal(t, false, no)

	extraPK := NewExtraHandleColInfo()
	require.Equal(t, mysql.NotNullFlag|mysql.PriKeyFlag, extraPK.Flag)
	require.Equal(t, charset.CharsetBin, extraPK.Charset)
	require.Equal(t, charset.CollationBin, extraPK.Collate)
}

func TestJobStartTime(t *testing.T) {
	job := &Job{
		ID:         123,
		BinlogInfo: &HistoryInfo{},
	}
	require.Equal(t, TSConvert2Time(job.StartTS), time.Unix(0, 0))
	require.Equal(t, fmt.Sprintf("ID:123, Type:none, State:none, SchemaState:queueing, SchemaID:0, TableID:0, RowCount:0, ArgLen:0, start time: %s, Err:<nil>, ErrCount:0, SnapshotVersion:0", time.Unix(0, 0)), job.String())
}

func TestJobCodec(t *testing.T) {
	type A struct {
		Name string
	}
	job := &Job{
		ID:         1,
		TableID:    2,
		SchemaID:   1,
		BinlogInfo: &HistoryInfo{},
		Args:       []interface{}{NewCIStr("a"), A{Name: "abc"}},
	}
	job.BinlogInfo.AddDBInfo(123, &DBInfo{ID: 1, Name: NewCIStr("test_history_db")})
	job.BinlogInfo.AddTableInfo(123, &TableInfo{ID: 1, Name: NewCIStr("test_history_tbl")})

	// Test IsDependentOn.
	// job: table ID is 2
	// job1: table ID is 2
	var err error
	job1 := &Job{
		ID:         2,
		TableID:    2,
		SchemaID:   1,
		Type:       ActionRenameTable,
		BinlogInfo: &HistoryInfo{},
		Args:       []interface{}{int64(3), NewCIStr("new_table_name")},
	}
	job1.RawArgs, err = json.Marshal(job1.Args)
	require.NoError(t, err)
	isDependent, err := job.IsDependentOn(job1)
	require.NoError(t, err)
	require.True(t, isDependent)
	// job1: rename table, old schema ID is 3
	// job2: create schema, schema ID is 3
	job2 := &Job{
		ID:         3,
		TableID:    3,
		SchemaID:   3,
		Type:       ActionCreateSchema,
		BinlogInfo: &HistoryInfo{},
	}
	isDependent, err = job2.IsDependentOn(job1)
	require.NoError(t, err)
	require.True(t, isDependent)

	require.Equal(t, false, job.IsCancelled())
	b, err := job.Encode(false)
	require.NoError(t, err)
	newJob := &Job{}
	err = newJob.Decode(b)
	require.NoError(t, err)
	require.Equal(t, job.BinlogInfo, newJob.BinlogInfo)
	name := CIStr{}
	a := A{}
	err = newJob.DecodeArgs(&name, &a)
	require.NoError(t, err)
	require.Equal(t, NewCIStr(""), name)
	require.Equal(t, A{Name: ""}, a)
	require.Greater(t, len(newJob.String()), 0)

	job.BinlogInfo.Clean()
	b1, err := job.Encode(true)
	require.NoError(t, err)
	newJob = &Job{}
	err = newJob.Decode(b1)
	require.NoError(t, err)
	require.Equal(t, &HistoryInfo{}, newJob.BinlogInfo)
	name = CIStr{}
	a = A{}
	err = newJob.DecodeArgs(&name, &a)
	require.NoError(t, err)
	require.Equal(t, NewCIStr("a"), name)
	require.Equal(t, A{Name: "abc"}, a)
	require.Greater(t, len(newJob.String()), 0)

	b2, err := job.Encode(true)
	require.NoError(t, err)
	newJob = &Job{}
	err = newJob.Decode(b2)
	require.NoError(t, err)
	name = CIStr{}
	// Don't decode to a here.
	err = newJob.DecodeArgs(&name)
	require.NoError(t, err)
	require.Equal(t, NewCIStr("a"), name)
	require.Greater(t, len(newJob.String()), 0)

	job.State = JobStateDone
	require.True(t, job.IsDone())
	require.True(t, job.IsFinished())
	require.False(t, job.IsRunning())
	require.False(t, job.IsSynced())
	require.False(t, job.IsRollbackDone())
	job.SetRowCount(3)
	require.Equal(t, int64(3), job.GetRowCount())
}

func TestState(t *testing.T) {
	schemaTbl := []SchemaState{
		StateDeleteOnly,
		StateWriteOnly,
		StateWriteReorganization,
		StateDeleteReorganization,
		StatePublic,
		StateGlobalTxnOnly,
	}

	for _, state := range schemaTbl {
		require.Greater(t, len(state.String()), 0)
	}

	jobTbl := []JobState{
		JobStateRunning,
		JobStateDone,
		JobStateCancelled,
		JobStateRollingback,
		JobStateRollbackDone,
		JobStateSynced,
	}

	for _, state := range jobTbl {
		require.Greater(t, len(state.String()), 0)
	}
}

func TestString(t *testing.T) {
	acts := []struct {
		act    ActionType
		result string
	}{
		{ActionNone, "none"},
		{ActionAddForeignKey, "add foreign key"},
		{ActionDropForeignKey, "drop foreign key"},
		{ActionTruncateTable, "truncate table"},
		{ActionModifyColumn, "modify column"},
		{ActionRenameTable, "rename table"},
		{ActionRenameTables, "rename tables"},
		{ActionSetDefaultValue, "set default value"},
		{ActionCreateSchema, "create schema"},
		{ActionDropSchema, "drop schema"},
		{ActionCreateTable, "create table"},
		{ActionDropTable, "drop table"},
		{ActionAddIndex, "add index"},
		{ActionDropIndex, "drop index"},
		{ActionAddColumn, "add column"},
		{ActionAddColumns, "add multi-columns"},
		{ActionDropColumn, "drop column"},
		{ActionDropColumns, "drop multi-columns"},
		{ActionModifySchemaCharsetAndCollate, "modify schema charset and collate"},
		{ActionDropIndexes, "drop multi-indexes"},
		{ActionAlterTablePlacement, "alter table placement"},
		{ActionAlterTablePartitionPlacement, "alter table partition placement"},
		{ActionAlterNoCacheTable, "alter table nocache"},
	}

	for _, v := range acts {
		str := v.act.String()
		require.Equal(t, v.result, str)
	}
}

func TestUnmarshalCIStr(t *testing.T) {
	var ci CIStr

	// Test unmarshal CIStr from a single string.
	str := "aaBB"
	buf, err := json.Marshal(str)
	require.NoError(t, err)
	require.NoError(t, ci.UnmarshalJSON(buf))
	require.Equal(t, str, ci.O)
	require.Equal(t, "aabb", ci.L)

	buf, err = json.Marshal(ci)
	require.NoError(t, err)
	require.Equal(t, `{"O":"aaBB","L":"aabb"}`, string(buf))
	require.NoError(t, ci.UnmarshalJSON(buf))
	require.Equal(t, str, ci.O)
	require.Equal(t, "aabb", ci.L)
}

func TestDefaultValue(t *testing.T) {
	srcCol := &ColumnInfo{
		ID: 1,
	}
	randPlainStr := "random_plain_string"

	oldPlainCol := srcCol.Clone()
	oldPlainCol.Name = NewCIStr("oldPlainCol")
	oldPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	oldPlainCol.DefaultValue = randPlainStr
	oldPlainCol.OriginDefaultValue = randPlainStr

	newPlainCol := srcCol.Clone()
	newPlainCol.Name = NewCIStr("newPlainCol")
	newPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	err := newPlainCol.SetDefaultValue(1)
	require.NoError(t, err)
	require.Equal(t, 1, newPlainCol.GetDefaultValue())
	err = newPlainCol.SetDefaultValue(randPlainStr)
	require.NoError(t, err)
	require.Equal(t, randPlainStr, newPlainCol.GetDefaultValue())

	randBitStr := string([]byte{25, 185})

	oldBitCol := srcCol.Clone()
	oldBitCol.Name = NewCIStr("oldBitCol")
	oldBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	oldBitCol.DefaultValue = randBitStr
	oldBitCol.OriginDefaultValue = randBitStr

	newBitCol := srcCol.Clone()
	newBitCol.Name = NewCIStr("newBitCol")
	newBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	err = newBitCol.SetDefaultValue(1)
	// Only string type is allowed in BIT column.
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid default value")
	require.Equal(t, 1, newBitCol.GetDefaultValue())
	err = newBitCol.SetDefaultValue(randBitStr)
	require.NoError(t, err)
	require.Equal(t, randBitStr, newBitCol.GetDefaultValue())

	nullBitCol := srcCol.Clone()
	nullBitCol.Name = NewCIStr("nullBitCol")
	nullBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	err = nullBitCol.SetOriginDefaultValue(nil)
	require.NoError(t, err)
	require.Nil(t, nullBitCol.GetOriginDefaultValue())

	testCases := []struct {
		col          *ColumnInfo
		isConsistent bool
	}{
		{oldPlainCol, true},
		{oldBitCol, false},
		{newPlainCol, true},
		{newBitCol, true},
		{nullBitCol, true},
	}
	for _, tc := range testCases {
		col, isConsistent := tc.col, tc.isConsistent
		comment := fmt.Sprintf("%s assertion failed", col.Name.O)
		bytes, err := json.Marshal(col)
		require.NoError(t, err, comment)
		var newCol ColumnInfo
		err = json.Unmarshal(bytes, &newCol)
		require.NoError(t, err, comment)
		if isConsistent {
			require.Equal(t, newCol.GetDefaultValue(), col.GetDefaultValue())
			require.Equal(t, newCol.GetOriginDefaultValue(), col.GetOriginDefaultValue())
		} else {
			require.False(t, col.DefaultValue == newCol.DefaultValue, comment)
			require.False(t, col.DefaultValue == newCol.DefaultValue, comment)
		}
	}
}

func TestPlacementSettingsString(t *testing.T) {
	settings := &PlacementSettings{
		PrimaryRegion: "us-east-1",
		Regions:       "us-east-1,us-east-2",
		Schedule:      "EVEN",
	}
	require.Equal(t, "PRIMARY_REGION=\"us-east-1\" REGIONS=\"us-east-1,us-east-2\" SCHEDULE=\"EVEN\"", settings.String())

	settings = &PlacementSettings{
		LeaderConstraints: "[+region=bj]",
	}
	require.Equal(t, "LEADER_CONSTRAINTS=\"[+region=bj]\"", settings.String())

	settings = &PlacementSettings{
		Voters:              1,
		VoterConstraints:    "[+region=us-east-1]",
		Followers:           2,
		FollowerConstraints: "[+disk=ssd]",
		Learners:            3,
		LearnerConstraints:  "[+region=us-east-2]",
	}
	require.Equal(t, "VOTERS=1 VOTER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2 FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=us-east-2]\"", settings.String())

	settings = &PlacementSettings{
		Voters:      3,
		Followers:   2,
		Learners:    1,
		Constraints: "{+us-east-1:1,+us-east-2:1}",
	}
	require.Equal(t, "CONSTRAINTS=\"{+us-east-1:1,+us-east-2:1}\" VOTERS=3 FOLLOWERS=2 LEARNERS=1", settings.String())
}
