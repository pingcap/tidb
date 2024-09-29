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
	job1 := &Job{
		Version:    GetJobVerInUse(),
		ID:         2,
		TableID:    2,
		SchemaID:   1,
		Type:       ActionRenameTable,
		BinlogInfo: &HistoryInfo{},
	}
	job1.FillArgs(&RenameTableArgs{OldSchemaID: 3, NewTableName: model.NewCIStr("new_table_name")})
	job2 := &Job{
		Version:    JobVersion1,
		ID:         3,
		TableID:    3,
		SchemaID:   3,
		Type:       ActionCreateSchema,
		BinlogInfo: &HistoryInfo{},
	}
	job3 := &Job{
		Version:    JobVersion1,
		ID:         4,
		TableID:    4,
		SchemaID:   4,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job3.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 6, PTSchemaID: 3, PTTableID: 5, PartitionName: "pt", WithValidation: true})
	job4 := &Job{
		Version:    JobVersion1,
		ID:         5,
		TableID:    5,
		SchemaID:   3,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job4.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 6, PTSchemaID: 4, PTTableID: 2, PartitionName: "pt", WithValidation: true})
	job5 := &Job{
		Version:    JobVersion1,
		ID:         6,
		TableID:    6,
		SchemaID:   6,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job5.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 2, PTSchemaID: 6, PTTableID: 5, PartitionName: "pt", WithValidation: true})
	job6 := &Job{
		Version:    JobVersion1,
		ID:         7,
		TableID:    7,
		SchemaID:   7,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job6.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 6, PTSchemaID: 4, PTTableID: 2, PartitionName: "pt", WithValidation: true})
	job7 := &Job{
		Version:    JobVersion1,
		ID:         8,
		TableID:    8,
		SchemaID:   8,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job7.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 8, PTSchemaID: 4, PTTableID: 6, PartitionName: "pt", WithValidation: true})
	job8 := &Job{
		Version:    JobVersion1,
		ID:         9,
		TableID:    9,
		SchemaID:   9,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job8.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 8, PTSchemaID: 9, PTTableID: 9, PartitionName: "pt", WithValidation: true})
	job9 := &Job{
		Version:    JobVersion1,
		ID:         10,
		TableID:    10,
		SchemaID:   10,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job9.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 10, PTSchemaID: 10, PTTableID: 8, PartitionName: "pt", WithValidation: true})
	job10 := &Job{
		Version:    JobVersion1,
		ID:         11,
		TableID:    11,
		SchemaID:   11,
		Type:       ActionDropSchema,
		BinlogInfo: &HistoryInfo{},
	}
	job11 := &Job{
		Version:    JobVersion1,
		ID:         12,
		TableID:    12,
		SchemaID:   11,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job11.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 10, PTSchemaID: 10, PTTableID: 8, PartitionName: "pt", WithValidation: true})
	job12 := &Job{
		Version:    JobVersion1,
		ID:         13,
		TableID:    13,
		SchemaID:   11,
		Type:       ActionDropTable,
		BinlogInfo: &HistoryInfo{},
	}
	job13 := &Job{
		Version:    JobVersion1,
		ID:         14,
		TableID:    12,
		SchemaID:   14,
		Type:       ActionDropTable,
		BinlogInfo: &HistoryInfo{},
	}
	job14 := &Job{
		Version:    JobVersion1,
		ID:         15,
		TableID:    15,
		SchemaID:   15,
		Type:       ActionExchangeTablePartition,
		BinlogInfo: &HistoryInfo{},
	}
	job14.FillArgs(&ExchangeTablePartitionArgs{PartitionID: 16, PTSchemaID: 17, PTTableID: 12, PartitionName: "pt", WithValidation: true})
	job15 := &Job{
		Version:    JobVersion1,
		ID:         16,
		Type:       ActionFlashbackCluster,
		BinlogInfo: &HistoryInfo{},
		Args:       []any{0, map[string]any{}, "ON", true},
	}

	for _, j := range []*Job{job1, job2, job3, job4, job5, job6, job7, job8, job9, job10, job11, job12, job13, job14, job15} {
		_, err := j.Encode(true)
		require.NoError(t, err)
	}

	cases := []struct {
		name        string
		left, right *Job
		dependent   bool
	}{
		{"same table id", job, job1, true},
		{"related to same schema", job2, job1, true},
		{"test ActionCreateSchema and ActionExchangeTablePartition is dependent.", job3, job2, true},
		{"test random and ActionExchangeTablePartition is dependent because TableID is same.", job4, job, true},
		{"test ActionExchangeTablePartition and ActionExchangeTablePartition is dependent.", job5, job4, true},
		// this case is invalid, actually.
		{"PT partition ID same as other job's NT table ID", job6, job5, true},
		// invalid too
		{"PT table ID same as other job's partition ID", job7, job6, true},
		// invalid too
		{"2 PT table has same partition ID", job8, job7, true},
		// invalid too
		{"PT table ID same as other job's partition ID", job9, job8, true},
		{"test ActionDropSchema and ActionExchangeTablePartition is dependent.", job11, job10, true},
		{"test ActionDropTable and ActionExchangeTablePartition is dependent.", job11, job12, false},
		{"NT table ID same as other job's table ID", job11, job13, true},
		{"test ActionDropTable and ActionExchangeTablePartition is dependent.", job13, job14, true},
		{"test ActionFlashbackCluster with other ddl jobs are dependent.", job, job15, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isDependent, err := c.left.IsDependentOn(c.right)
			require.NoError(t, err)
			require.Equal(t, c.dependent, isDependent)
		})
	}

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
	}
	j.FillArgs(&TruncateTableArgs{
		FKCheck: true,
	})
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
