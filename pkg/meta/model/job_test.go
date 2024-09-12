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
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestJobStartTime(t *testing.T) {
	job := &Job{
		Version:    JobVersion1,
		ID:         123,
		BinlogInfo: &HistoryInfo{},
	}
	require.Equal(t, TSConvert2Time(job.StartTS), time.Unix(0, 0))
	require.Equal(t, fmt.Sprintf("ID:123, Type:none, State:none, SchemaState:none, SchemaID:0, TableID:0, RowCount:0, ArgLen:0, start time: %s, Err:<nil>, ErrCount:0, SnapshotVersion:0, Version: v1", time.Unix(0, 0)), job.String())
}

func TestState(t *testing.T) {
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

func TestJobCodec(t *testing.T) {
	type A struct {
		Name string
	}
	tzName, tzOffset := time.Now().In(time.UTC).Zone()
	job := &Job{
		Version:    JobVersion1,
		ID:         1,
		TableID:    2,
		SchemaID:   1,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{model.NewCIStr("a"), A{Name: "abc"}},
		ReorgMeta: &DDLReorgMeta{
			Location: &TimeZoneLocation{Name: tzName, Offset: tzOffset},
		},
	}
	job.BinlogInfo.AddDBInfo(123, &DBInfo{ID: 1, Name: model.NewCIStr("test_history_db")})
	job.BinlogInfo.AddTableInfo(123, &TableInfo{ID: 1, Name: model.NewCIStr("test_history_tbl")})

	// Test IsDependentOn.
	// job: table ID is 2
	// job1: table ID is 2
	var err error
	job1 := &Job{
		Version:    JobVersion1,
		ID:         2,
		TableID:    2,
		SchemaID:   1,
		Type:       ActionRenameTable,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{int64(3), model.NewCIStr("new_table_name")},
	}
	job1.RawArgs, err = json.Marshal(job1.Args)
	require.NoError(t, err)
	isDependent, err := job.IsDependentOn(job1)
	require.NoError(t, err)
	require.True(t, isDependent)
	// job1: rename table, old schema ID is 3
	// job2: create schema, schema ID is 3
	job2 := &Job{
		Version:    JobVersion1,
		ID:         3,
		TableID:    3,
		SchemaID:   3,
		Type:       ActionCreateSchema,
		BinlogInfo: &HistoryInfo{},
	}
	isDependent, err = job2.IsDependentOn(job1)
	require.NoError(t, err)
	require.True(t, isDependent)

	// Test IsDependentOn for exchange partition with table.
	// test ActionCreateSchema and ActionExchangeTablePartition is dependent.
	job3 := &Job{
		Version:    JobVersion1,
		ID:         4,
		TableID:    4,
		SchemaID:   4,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{int64(6), int64(3), int64(5), "pt", true},
	}
	job3.RawArgs, err = json.Marshal(job3.Args)
	require.NoError(t, err)
	isDependent, err = job3.IsDependentOn(job2)
	require.NoError(t, err)
	require.True(t, isDependent)

	// test random and ActionExchangeTablePartition is dependent because TableID is same.
	job4 := &Job{
		Version:    JobVersion1,
		ID:         5,
		TableID:    5,
		SchemaID:   3,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{6, 4, 2, "pt", true},
	}
	job4.RawArgs, err = json.Marshal(job4.Args)
	require.NoError(t, err)
	isDependent, err = job4.IsDependentOn(job)
	require.NoError(t, err)
	require.True(t, isDependent)

	// test ActionExchangeTablePartition and ActionExchangeTablePartition is dependent.
	job5 := &Job{
		Version:    JobVersion1,
		ID:         6,
		TableID:    6,
		SchemaID:   6,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{2, 6, 5, "pt", true},
	}
	job5.RawArgs, err = json.Marshal(job5.Args)
	require.NoError(t, err)
	isDependent, err = job5.IsDependentOn(job4)
	require.NoError(t, err)
	require.True(t, isDependent)

	job6 := &Job{
		Version:    JobVersion1,
		ID:         7,
		TableID:    7,
		SchemaID:   7,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{6, 4, 2, "pt", true},
	}
	job6.RawArgs, err = json.Marshal(job6.Args)
	require.NoError(t, err)
	isDependent, err = job6.IsDependentOn(job5)
	require.NoError(t, err)
	require.True(t, isDependent)

	job7 := &Job{
		Version:    JobVersion1,
		ID:         8,
		TableID:    8,
		SchemaID:   8,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{8, 4, 6, "pt", true},
	}
	job7.RawArgs, err = json.Marshal(job7.Args)
	require.NoError(t, err)
	isDependent, err = job7.IsDependentOn(job6)
	require.NoError(t, err)
	require.True(t, isDependent)

	job8 := &Job{
		Version:    JobVersion1,
		ID:         9,
		TableID:    9,
		SchemaID:   9,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{8, 9, 9, "pt", true},
	}
	job8.RawArgs, err = json.Marshal(job8.Args)
	require.NoError(t, err)
	isDependent, err = job8.IsDependentOn(job7)
	require.NoError(t, err)
	require.True(t, isDependent)

	job9 := &Job{
		Version:    JobVersion1,
		ID:         10,
		TableID:    10,
		SchemaID:   10,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{10, 10, 8, "pt", true},
	}
	job9.RawArgs, err = json.Marshal(job9.Args)
	require.NoError(t, err)
	isDependent, err = job9.IsDependentOn(job8)
	require.NoError(t, err)
	require.True(t, isDependent)

	// test ActionDropSchema and ActionExchangeTablePartition is dependent.
	job10 := &Job{
		Version:    JobVersion1,
		ID:         11,
		TableID:    11,
		SchemaID:   11,
		Type:       ActionDropSchema,
		BinlogInfo: &HistoryInfo{},
	}
	job10.RawArgs, err = json.Marshal(job10.Args)
	require.NoError(t, err)

	job11 := &Job{
		Version:    JobVersion1,
		ID:         12,
		TableID:    12,
		SchemaID:   11,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{10, 10, 8, "pt", true},
	}
	job11.RawArgs, err = json.Marshal(job11.Args)
	require.NoError(t, err)
	isDependent, err = job11.IsDependentOn(job10)
	require.NoError(t, err)
	require.True(t, isDependent)

	// test ActionDropTable and ActionExchangeTablePartition is dependent.
	job12 := &Job{
		Version:    JobVersion1,
		ID:         13,
		TableID:    13,
		SchemaID:   11,
		Type:       ActionDropTable,
		BinlogInfo: &HistoryInfo{},
	}
	job12.RawArgs, err = json.Marshal(job12.Args)
	require.NoError(t, err)
	isDependent, err = job11.IsDependentOn(job12)
	require.NoError(t, err)
	require.False(t, isDependent)

	job13 := &Job{
		Version:    JobVersion1,
		ID:         14,
		TableID:    12,
		SchemaID:   14,
		Type:       ActionDropTable,
		BinlogInfo: &HistoryInfo{},
	}
	job13.RawArgs, err = json.Marshal(job13.Args)
	require.NoError(t, err)
	isDependent, err = job11.IsDependentOn(job13)
	require.NoError(t, err)
	require.True(t, isDependent)

	// test ActionDropTable and ActionExchangeTablePartition is dependent.
	job14 := &Job{
		Version:    JobVersion1,
		ID:         15,
		TableID:    15,
		SchemaID:   15,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{16, 17, 12, "pt", true},
	}
	job14.RawArgs, err = json.Marshal(job14.Args)
	require.NoError(t, err)
	isDependent, err = job13.IsDependentOn(job14)
	require.NoError(t, err)
	require.True(t, isDependent)

	// test ActionFlashbackCluster with other ddl jobs are dependent.
	job15 := &Job{
		Version:    JobVersion1,
		ID:         16,
		Type:       ActionFlashbackCluster,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{0, map[string]any{}, "ON", true},
	}
	job15.RawArgs, err = json.Marshal(job15.Args)
	require.NoError(t, err)
	isDependent, err = job.IsDependentOn(job15)
	require.NoError(t, err)
	require.True(t, isDependent)

	require.Equal(t, false, job.IsCancelled())
	b, err := job.Encode(false)
	require.NoError(t, err)
	newJob := &Job{}
	err = newJob.Decode(b)
	require.NoError(t, err)
	require.Equal(t, job.BinlogInfo, newJob.BinlogInfo)
	name := model.CIStr{}
	a := A{}
	err = newJob.DecodeArgs(&name, &a)
	require.NoError(t, err)
	require.Equal(t, model.NewCIStr(""), name)
	require.Equal(t, A{Name: ""}, a)
	require.Greater(t, len(newJob.String()), 0)
	require.Equal(t, newJob.ReorgMeta.Location.Name, tzName)
	require.Equal(t, newJob.ReorgMeta.Location.Offset, tzOffset)

	job.BinlogInfo.Clean()
	b1, err := job.Encode(true)
	require.NoError(t, err)
	newJob = &Job{}
	err = newJob.Decode(b1)
	require.NoError(t, err)
	require.Equal(t, &HistoryInfo{}, newJob.BinlogInfo)
	name = model.CIStr{}
	a = A{}
	err = newJob.DecodeArgs(&name, &a)
	require.NoError(t, err)
	require.Equal(t, model.NewCIStr("a"), name)
	require.Equal(t, A{Name: "abc"}, a)
	require.Greater(t, len(newJob.String()), 0)

	b2, err := job.Encode(true)
	require.NoError(t, err)
	newJob = &Job{}
	err = newJob.Decode(b2)
	require.NoError(t, err)
	name = model.CIStr{}
	// Don't decode to a here.
	err = newJob.DecodeArgs(&name)
	require.NoError(t, err)
	require.Equal(t, model.NewCIStr("a"), name)
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

func TestLocation(t *testing.T) {
	// test offset = 0
	loc := &TimeZoneLocation{}
	nLoc, err := loc.GetLocation()
	require.NoError(t, err)
	require.Equal(t, nLoc.String(), "UTC")
	// test loc.location != nil
	loc.Name = "Asia/Shanghai"
	nLoc, err = loc.GetLocation()
	require.NoError(t, err)
	require.Equal(t, nLoc.String(), "UTC")
	// timezone +05:00
	loc1 := &TimeZoneLocation{Name: "UTC", Offset: 18000}
	loc1Byte, err := json.Marshal(loc1)
	require.NoError(t, err)
	loc2 := &TimeZoneLocation{}
	err = json.Unmarshal(loc1Byte, loc2)
	require.NoError(t, err)
	require.Equal(t, loc2.Offset, loc1.Offset)
	require.Equal(t, loc2.Name, loc1.Name)
	nLoc, err = loc2.GetLocation()
	require.NoError(t, err)
	require.Equal(t, nLoc.String(), "UTC")
	location := time.FixedZone("UTC", loc1.Offset)
	require.Equal(t, nLoc, location)
}

func TestJobClone(t *testing.T) {
	job := &Job{
		Version:         JobVersion1,
		ID:              100,
		Type:            ActionCreateTable,
		SchemaID:        101,
		TableID:         102,
		SchemaName:      "test",
		TableName:       "t",
		State:           JobStateDone,
		MultiSchemaInfo: nil,
	}
	clone := job.Clone()
	require.Equal(t, job.ID, clone.ID)
	require.Equal(t, job.Type, clone.Type)
	require.Equal(t, job.SchemaID, clone.SchemaID)
	require.Equal(t, job.TableID, clone.TableID)
	require.Equal(t, job.SchemaName, clone.SchemaName)
	require.Equal(t, job.TableName, clone.TableName)
	require.Equal(t, job.State, clone.State)
	require.Equal(t, job.MultiSchemaInfo, clone.MultiSchemaInfo)
}

func TestJobSize(t *testing.T) {
	msg := `Please make sure that the following methods work as expected:
- SubJob.FromProxyJob()
- SubJob.ToProxyJob()
`
	job := Job{}
	require.Equal(t, 400, int(unsafe.Sizeof(job)), msg)
}

func TestBackfillMetaCodec(t *testing.T) {
	jm := &JobMeta{
		SchemaID: 1,
		TableID:  2,
		Query:    "alter table t add index idx(a)",
		Priority: 1,
	}
	bm := &BackfillMeta{
		EndInclude: true,
		Error:      terror.ErrResultUndetermined,
		JobMeta:    jm,
	}
	bmBytes, err := bm.Encode()
	require.NoError(t, err)
	bmRet := &BackfillMeta{}
	bmRet.Decode(bmBytes)
	require.Equal(t, bm, bmRet)
}

func TestMayNeedReorg(t *testing.T) {
	//TODO(bb7133): add more test cases for different ActionType.
	reorgJobTypes := []ActionType{
		ActionReorganizePartition,
		ActionRemovePartitioning,
		ActionAlterTablePartitioning,
		ActionAddIndex,
		ActionAddPrimaryKey,
	}
	generalJobTypes := []ActionType{
		ActionCreateTable,
		ActionDropTable,
	}
	job := &Job{
		Version:         JobVersion1,
		ID:              100,
		Type:            ActionCreateTable,
		SchemaID:        101,
		TableID:         102,
		SchemaName:      "test",
		TableName:       "t",
		State:           JobStateDone,
		MultiSchemaInfo: nil,
	}
	for _, jobType := range reorgJobTypes {
		job.Type = jobType
		require.True(t, job.MayNeedReorg())
	}
	for _, jobType := range generalJobTypes {
		job.Type = jobType
		require.False(t, job.MayNeedReorg())
	}
}

func TestInFinalState(t *testing.T) {
	for s, v := range map[JobState]bool{
		JobStateSynced:       true,
		JobStateCancelled:    true,
		JobStatePaused:       true,
		JobStateCancelling:   false,
		JobStateRollbackDone: false,
	} {
		require.Equal(t, v, (&Job{State: s}).InFinalState())
	}
}

func TestSchemaState(t *testing.T) {
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
		{ActionDropColumn, "drop column"},
		{ActionModifySchemaCharsetAndCollate, "modify schema charset and collate"},
		{ActionAlterTablePlacement, "alter table placement"},
		{ActionAlterTablePartitionPlacement, "alter table partition placement"},
		{ActionAlterNoCacheTable, "alter table nocache"},
	}

	for _, v := range acts {
		str := v.act.String()
		require.Equal(t, v.result, str)
	}
}

func TestJobEncodeV2(t *testing.T) {
	j := &Job{
		Version: JobVersion2,
		Type:    ActionTruncateTable,
		Args: []any{&TruncateTableArgs{
			FKCheck: true,
		}},
	}
	_, err := j.Encode(false)
	require.NoError(t, err)
	require.Nil(t, j.RawArgs)
	_, err = j.Encode(true)
	require.NoError(t, err)
	require.NotNil(t, j.RawArgs)
	args := &TruncateTableArgs{}
	require.NoError(t, json.Unmarshal(j.RawArgs, args))
	require.EqualValues(t, j.Args[0], args)
}
