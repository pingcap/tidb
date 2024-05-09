// Copyright 2022 PingCAP, Inc.
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

package ttlworker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTTLTableStatusRows(status ...*cache.TableStatus) []chunk.Row {
	c := chunk.NewChunkWithCapacity([]*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong), // table_id
		types.NewFieldType(mysql.TypeLonglong), // parent_table_id
		types.NewFieldType(mysql.TypeString),   // table_statistics
		types.NewFieldType(mysql.TypeString),   // last_job_id
		types.NewFieldType(mysql.TypeDatetime), // last_job_start_time
		types.NewFieldType(mysql.TypeDatetime), // last_job_finish_time
		types.NewFieldType(mysql.TypeDatetime), // last_job_ttl_expire
		types.NewFieldType(mysql.TypeString),   // last_job_summary
		types.NewFieldType(mysql.TypeString),   // current_job_id
		types.NewFieldType(mysql.TypeString),   // current_job_owner_id
		types.NewFieldType(mysql.TypeString),   // current_job_owner_addr
		types.NewFieldType(mysql.TypeDatetime), // current_job_hb_time
		types.NewFieldType(mysql.TypeDatetime), // current_job_start_time
		types.NewFieldType(mysql.TypeDatetime), // current_job_ttl_expire
		types.NewFieldType(mysql.TypeString),   // current_job_state
		types.NewFieldType(mysql.TypeString),   // current_job_status
		types.NewFieldType(mysql.TypeDatetime), // current_job_status_update_time
	}, len(status))
	var rows []chunk.Row

	for _, s := range status {
		tableID := types.NewDatum(s.TableID)
		c.AppendDatum(0, &tableID)
		parentTableID := types.NewDatum(s.ParentTableID)
		c.AppendDatum(1, &parentTableID)
		if s.TableStatistics == "" {
			c.AppendNull(2)
		} else {
			tableStatistics := types.NewDatum(s.TableStatistics)
			c.AppendDatum(2, &tableStatistics)
		}

		if s.LastJobID == "" {
			c.AppendNull(3)
		} else {
			lastJobID := types.NewDatum(s.LastJobID)
			c.AppendDatum(3, &lastJobID)
		}

		lastJobStartTime := types.NewDatum(types.NewTime(types.FromGoTime(s.LastJobStartTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(4, &lastJobStartTime)
		lastJobFinishTime := types.NewDatum(types.NewTime(types.FromGoTime(s.LastJobFinishTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(5, &lastJobFinishTime)
		lastJobTTLExpire := types.NewDatum(types.NewTime(types.FromGoTime(s.LastJobTTLExpire), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(6, &lastJobTTLExpire)

		if s.LastJobSummary == "" {
			c.AppendNull(7)
		} else {
			lastJobSummary := types.NewDatum(s.LastJobSummary)
			c.AppendDatum(7, &lastJobSummary)
		}
		if s.CurrentJobID == "" {
			c.AppendNull(8)
		} else {
			currentJobID := types.NewDatum(s.CurrentJobID)
			c.AppendDatum(8, &currentJobID)
		}
		if s.CurrentJobOwnerID == "" {
			c.AppendNull(9)
		} else {
			currentJobOwnerID := types.NewDatum(s.CurrentJobOwnerID)
			c.AppendDatum(9, &currentJobOwnerID)
		}
		if s.CurrentJobOwnerAddr == "" {
			c.AppendNull(10)
		} else {
			currentJobOwnerAddr := types.NewDatum(s.CurrentJobOwnerAddr)
			c.AppendDatum(10, &currentJobOwnerAddr)
		}

		currentJobOwnerHBTime := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobOwnerHBTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(11, &currentJobOwnerHBTime)
		currentJobStartTime := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobStartTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(12, &currentJobStartTime)
		currentJobTTLExpire := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobTTLExpire), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(13, &currentJobTTLExpire)

		if s.CurrentJobState == "" {
			c.AppendNull(14)
		} else {
			currentJobState := types.NewDatum(s.CurrentJobState)
			c.AppendDatum(14, &currentJobState)
		}
		if s.CurrentJobStatus == "" {
			c.AppendNull(15)
		} else {
			currentJobStatus := types.NewDatum(s.CurrentJobStatus)
			c.AppendDatum(15, &currentJobStatus)
		}

		currentJobStatusUpdateTime := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobStatusUpdateTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(16, &currentJobStatusUpdateTime)
	}

	iter := chunk.NewIterator4Chunk(c)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		rows = append(rows, row)
	}
	return rows
}

var updateStatusSQL = "SELECT LOW_PRIORITY table_id,parent_table_id,table_statistics,last_job_id,last_job_start_time,last_job_finish_time,last_job_ttl_expire,last_job_summary,current_job_id,current_job_owner_id,current_job_owner_addr,current_job_owner_hb_time,current_job_start_time,current_job_ttl_expire,current_job_state,current_job_status,current_job_status_update_time FROM mysql.tidb_ttl_table_status"

// TTLJob exports the ttlJob for test
type TTLJob = ttlJob

// GetSessionForTest is used for test
func GetSessionForTest(pool sessionPool) (session.Session, error) {
	return getSession(pool)
}

// LockJob is an exported version of lockNewJob for test
func (m *JobManager) LockJob(ctx context.Context, se session.Session, table *cache.PhysicalTable, now time.Time, createJobID string, checkInterval bool) (*TTLJob, error) {
	if createJobID == "" {
		return m.lockHBTimeoutJob(ctx, se, table, now)
	}
	return m.lockNewJob(ctx, se, table, now, createJobID, checkInterval)
}

// RunningJobs returns the running jobs inside ttl job manager
func (m *JobManager) RunningJobs() []*TTLJob {
	return m.runningJobs
}

// InfoSchemaCache is an exported getter of infoSchemaCache for test
func (m *JobManager) InfoSchemaCache() *cache.InfoSchemaCache {
	return m.infoSchemaCache
}

// TableStatusCache is an exported getter of TableStatusCache for test.
func (m *JobManager) TableStatusCache() *cache.TableStatusCache {
	return m.tableStatusCache
}

// RescheduleJobs is an exported version of rescheduleJobs for test
func (m *JobManager) RescheduleJobs(se session.Session, now time.Time) {
	m.rescheduleJobs(se, now)
}

func (m *JobManager) SubmitJob(se session.Session, tableID, physicalID int64, requestID string) error {
	ch := make(chan error, 1)
	m.handleSubmitJobRequest(se, &SubmitTTLManagerJobRequest{
		TableID:    tableID,
		PhysicalID: physicalID,
		RequestID:  requestID,
		RespCh:     ch,
	})
	return <-ch
}

// TaskManager is an exported getter of task manager for test
func (m *JobManager) TaskManager() *taskManager {
	return m.taskManager
}

// UpdateHeartBeat is an exported version of updateHeartBeat for test
func (m *JobManager) UpdateHeartBeat(ctx context.Context, se session.Session, now time.Time) error {
	return m.updateHeartBeat(ctx, se, now)
}

// ReportMetrics is an exported version of reportMetrics
func (m *JobManager) ReportMetrics(se session.Session) {
	m.reportMetrics(se)
}

func (j *ttlJob) Finish(se session.Session, now time.Time, summary *TTLSummary) {
	j.finish(se, now, summary)
}

func (j *ttlJob) ID() string {
	return j.id
}

func newMockTTLJob(tbl *cache.PhysicalTable, status cache.JobStatus) *ttlJob {
	return &ttlJob{tbl: tbl, status: status}
}

func TestReadyForLockHBTimeoutJobTables(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")
	m := NewJobManager("test-id", nil, nil, nil, nil)
	m.sessPool = newMockSessionPool(t, tbl)
	se := newMockSession(t, tbl)

	tblWithDailyInterval := newMockTTLTbl(t, "t2")
	tblWithDailyInterval.TTLInfo.JobInterval = "1d"

	cases := []struct {
		name             string
		infoSchemaTables []*cache.PhysicalTable
		tableStatus      []*cache.TableStatus
		shouldSchedule   bool
	}{
		// for a newly inserted table, it'll always not be scheduled because no job running
		{"newly created", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID}}, false},
		// table only in the table status cache will not be scheduled
		{"proper subset", []*cache.PhysicalTable{}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID}}, false},
		// table whose current job owner id is not empty, and heart beat time is long enough will not be scheduled
		{"current job not empty", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, CurrentJobID: "job1", CurrentJobOwnerID: "test-another-id", CurrentJobOwnerHBTime: se.Now()}}, false},
		// table whose current job owner id is not empty, but heart beat time is expired will be scheduled
		{"hb time expired", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, CurrentJobID: "job1", CurrentJobOwnerID: "test-another-id", CurrentJobOwnerHBTime: se.Now().Add(-time.Hour)}}, true},
		// if the last start time is too near, it will not be scheduled because no job running
		{"last start time too near", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, LastJobStartTime: se.Now()}}, false},
		// if the last start time is expired, it will not be scheduled because no job running
		{"last start time expired", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, LastJobStartTime: se.Now().Add(-time.Hour * 2)}}, false},
		// if the interval is 24h, and the last start time is near, it will not be scheduled because no job running
		{"last start time too near for 24h", []*cache.PhysicalTable{tblWithDailyInterval}, []*cache.TableStatus{{TableID: tblWithDailyInterval.ID, ParentTableID: tblWithDailyInterval.ID, LastJobStartTime: se.Now().Add(-time.Hour * 2)}}, false},
		// if the interval is 24h, and the last start time is far enough, it will not be scheduled because no job running
		{"last start time far enough for 24h", []*cache.PhysicalTable{tblWithDailyInterval}, []*cache.TableStatus{{TableID: tblWithDailyInterval.ID, ParentTableID: tblWithDailyInterval.ID, LastJobStartTime: se.Now().Add(-time.Hour * 25)}}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m.infoSchemaCache.Tables = make(map[int64]*cache.PhysicalTable)
			for _, ist := range c.infoSchemaTables {
				m.infoSchemaCache.Tables[ist.ID] = ist
			}
			m.tableStatusCache.Tables = make(map[int64]*cache.TableStatus)
			for _, st := range c.tableStatus {
				m.tableStatusCache.Tables[st.TableID] = st
			}

			tables := m.readyForLockHBTimeoutJobTables(se.Now())
			if c.shouldSchedule {
				assert.Len(t, tables, 1)
				assert.Equal(t, tbl.ID, tables[0].ID)
				assert.Equal(t, tbl.ID, tables[0].TableInfo.ID)
			} else {
				assert.Len(t, tables, 0)
			}
		})
	}
}

func TestOnTimerTick(t *testing.T) {
	var leader atomic.Bool
	m := NewJobManager("test-id", newMockSessionPool(t), nil, nil, func() bool {
		return leader.Load()
	})

	tbl := newMockTTLTbl(t, "t1")
	se := newMockSession(t)
	se.sessionInfoSchema = newMockInfoSchemaWithVer(100, tbl.TableInfo)

	timerStore := timerapi.NewMemoryTimerStore()
	defer timerStore.Close()

	a := &mockJobAdapter{}
	a.On("CanSubmitJob").Return(false).Maybe()

	rt := newTTLTimerRuntime(timerStore, a)
	require.Nil(t, rt.rt)
	defer rt.Pause()

	now := time.UnixMilli(3600 * 24)
	syncer := NewTTLTimerSyncer(m.sessPool, timerapi.NewDefaultTimerClient(timerStore))
	syncer.nowFunc = func() time.Time {
		return now
	}

	// pause after init
	m.onTimerTick(se, rt, syncer, now)
	require.Nil(t, rt.rt)
	require.Equal(t, 0, len(syncer.key2Timers))
	syncTime, syncVer := syncer.GetLastSyncInfo()
	require.Zero(t, syncVer)
	require.True(t, syncTime.IsZero())

	// resume first time
	leader.Store(true)
	m.onTimerTick(se, rt, syncer, now)
	innerRT := rt.rt
	require.NotNil(t, innerRT)
	require.True(t, innerRT.Running())
	require.Equal(t, 1, len(syncer.key2Timers))
	syncTime, syncVer = syncer.GetLastSyncInfo()
	require.Equal(t, int64(100), syncVer)
	require.Equal(t, now, syncTime)

	// resume after a very short duration
	now = now.Add(time.Second)
	se.sessionInfoSchema = newMockInfoSchemaWithVer(101, tbl.TableInfo)
	m.onTimerTick(se, rt, syncer, now)
	require.Same(t, innerRT, rt.rt)
	require.True(t, innerRT.Running())
	require.Equal(t, 1, len(syncer.key2Timers))
	syncTime, syncVer = syncer.GetLastSyncInfo()
	require.Equal(t, int64(100), syncVer)
	require.Equal(t, now.Add(-time.Second), syncTime)

	// resume after a middle duration
	now = now.Add(6 * time.Second)
	m.onTimerTick(se, rt, syncer, now)
	require.Same(t, innerRT, rt.rt)
	require.True(t, innerRT.Running())
	require.Equal(t, 1, len(syncer.key2Timers))
	syncTime, syncVer = syncer.GetLastSyncInfo()
	require.Equal(t, int64(101), syncVer)
	require.Equal(t, now, syncTime)

	// resume after a middle duration but infoschema not change
	now = now.Add(6 * time.Second)
	m.onTimerTick(se, rt, syncer, now)
	require.Same(t, innerRT, rt.rt)
	require.True(t, innerRT.Running())
	require.Equal(t, 1, len(syncer.key2Timers))
	syncTime, syncVer = syncer.GetLastSyncInfo()
	require.Equal(t, int64(101), syncVer)
	require.Equal(t, now.Add(-6*time.Second), syncTime)

	// resume after a long duration
	now = now.Add(3 * time.Minute)
	m.onTimerTick(se, rt, syncer, now)
	require.Same(t, innerRT, rt.rt)
	require.True(t, innerRT.Running())
	require.Equal(t, 1, len(syncer.key2Timers))
	syncTime, syncVer = syncer.GetLastSyncInfo()
	require.Equal(t, int64(101), syncVer)
	require.Equal(t, now, syncTime)

	// pause
	leader.Store(false)
	m.onTimerTick(se, rt, syncer, now)
	require.Nil(t, rt.rt)
	require.False(t, innerRT.Running())
	syncTime, syncVer = syncer.GetLastSyncInfo()
	require.Zero(t, syncVer)
	require.True(t, syncTime.IsZero())
}

func TestLockTable(t *testing.T) {
	now, err := time.Parse(timeFormat, "2022-12-05 17:13:05")
	assert.NoError(t, err)
	newJobExpireTime := now.Add(-time.Minute)
	oldJobExpireTime := now.Add(-time.Hour)
	oldJobStartTime := now.Add(-30 * time.Minute)

	testPhysicalTable := &cache.PhysicalTable{ID: 1, Schema: model.NewCIStr("test"), TableInfo: &model.TableInfo{ID: 1, Name: model.NewCIStr("t1"), TTLInfo: &model.TTLInfo{ColumnName: model.NewCIStr("test"), IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitMinute), JobInterval: "1h"}}}

	type executeInfo struct {
		sql  string
		args []any
	}
	getExecuteInfo := func(sql string, args []any) executeInfo {
		return executeInfo{
			sql,
			args,
		}
	}
	getExecuteInfoForUpdate := func(sql string, args []any) executeInfo {
		return executeInfo{
			sql + " FOR UPDATE NOWAIT",
			args,
		}
	}
	getExecuteInfoWithErr := func(sql string, args []any, err error) executeInfo {
		require.NoError(t, err)
		return executeInfo{
			sql,
			args,
		}
	}

	type sqlExecute struct {
		executeInfo

		rows []chunk.Row
		err  error
	}
	cases := []struct {
		name          string
		table         *cache.PhysicalTable
		sqls          []sqlExecute
		isCreate      bool
		checkInterval bool
		hasError      bool
	}{
		{"normal lock table for create", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("new-job-id", 1, now, now, newJobExpireTime, "test-id")),
				nil, nil,
			},
			{
				getExecuteInfo(createJobHistorySQL("new-job-id", testPhysicalTable, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfoWithErr(cache.InsertIntoTTLTask(newMockSession(t), "new-job-id", 1, 0, nil, nil, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfo(updateStatusSQL, nil),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, true, false, false},
		{"normal lock table for create and check interval", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("new-job-id", 1, now, now, newJobExpireTime, "test-id")),
				nil, nil,
			},
			{
				getExecuteInfo(createJobHistorySQL("new-job-id", testPhysicalTable, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfoWithErr(cache.InsertIntoTTLTask(newMockSession(t), "new-job-id", 1, 0, nil, nil, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfo(updateStatusSQL, nil),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, true, true, false},
		{"normal lock table for exist job", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, false, false, true},
		{"select nothing for create", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				nil, nil,
			},
			{
				getExecuteInfo(insertNewTableIntoStatusSQL(1, 1)),
				nil, nil,
			},
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("new-job-id", 1, now, now, newJobExpireTime, "test-id")),
				nil, nil,
			},
			{
				getExecuteInfo(createJobHistorySQL("new-job-id", testPhysicalTable, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfoWithErr(cache.InsertIntoTTLTask(newMockSession(t), "new-job-id", 1, 0, nil, nil, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfo(updateStatusSQL, nil),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, true, false, false},
		{"select nothing for create and check interval", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				nil, nil,
			},
			{
				getExecuteInfo(insertNewTableIntoStatusSQL(1, 1)),
				nil, nil,
			},
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("new-job-id", 1, now, now, newJobExpireTime, "test-id")),
				nil, nil,
			},
			{
				getExecuteInfo(createJobHistorySQL("new-job-id", testPhysicalTable, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfoWithErr(cache.InsertIntoTTLTask(newMockSession(t), "new-job-id", 1, 0, nil, nil, newJobExpireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfo(updateStatusSQL, nil),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, true, true, false},
		{"select nothing for exist job", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				nil, nil,
			},
		}, false, false, true},
		{"running job but create", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1, CurrentJobTTLExpire: oldJobExpireTime, CurrentJobID: "job1", CurrentJobOwnerID: "owner1", CurrentJobOwnerHBTime: now, CurrentJobStartTime: oldJobStartTime}),
				nil,
			},
		}, true, false, true},
		{"running job but create and check interval", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1, CurrentJobTTLExpire: oldJobExpireTime, CurrentJobID: "job1", CurrentJobOwnerID: "owner1", CurrentJobOwnerHBTime: now, CurrentJobStartTime: oldJobStartTime}),
				nil,
			},
		}, true, true, true},
		{"running job but lock for exist job", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1, CurrentJobTTLExpire: oldJobExpireTime, CurrentJobID: "job1", CurrentJobOwnerID: "owner1", CurrentJobOwnerHBTime: now, CurrentJobStartTime: oldJobStartTime}),
				nil,
			},
		}, false, false, true},
		{"heartbeat timeout job but create", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1, CurrentJobTTLExpire: oldJobExpireTime, CurrentJobID: "job1", CurrentJobOwnerID: "owner1", CurrentJobOwnerHBTime: now.Add(-20 * time.Minute), CurrentJobStartTime: oldJobStartTime}),
				nil,
			},
		}, true, false, true},
		{"heartbeat timeout job but create with check interval", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1, CurrentJobTTLExpire: oldJobExpireTime, CurrentJobID: "job1", CurrentJobOwnerID: "owner1", CurrentJobOwnerHBTime: now.Add(-20 * time.Minute), CurrentJobStartTime: oldJobStartTime}),
				nil,
			},
		}, true, true, true},
		{"heartbeat timeout job for lock", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1, CurrentJobTTLExpire: oldJobExpireTime, CurrentJobID: "job1", CurrentJobOwnerID: "owner1", CurrentJobOwnerHBTime: now.Add(-20 * time.Minute), CurrentJobStartTime: oldJobStartTime}),
				nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("job1", 1, oldJobStartTime, now, oldJobExpireTime, "test-id")),
				nil, nil,
			},
			{
				getExecuteInfo(updateStatusSQL, nil),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, false, false, false},
		{"return error", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("new-job-id", 1, now, now, newJobExpireTime, "test-id")),
				nil, errors.New("test error message"),
			},
		}, true, false, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := NewJobManager("test-id", newMockSessionPool(t), nil, nil, nil)
			m.infoSchemaCache.Tables[c.table.ID] = c.table
			sqlCounter := 0
			se := newMockSession(t)
			se.executeSQL = func(ctx context.Context, sql string, args ...any) (rows []chunk.Row, err error) {
				assert.Less(t, sqlCounter, len(c.sqls))
				assert.Equal(t, c.sqls[sqlCounter].sql, sql)
				assert.Equal(t, c.sqls[sqlCounter].args, args)

				rows = c.sqls[sqlCounter].rows
				err = c.sqls[sqlCounter].err
				sqlCounter += 1
				return
			}

			m.ctx = cache.SetMockExpireTime(context.Background(), newJobExpireTime)
			var job *ttlJob
			if c.isCreate {
				job, err = m.lockNewJob(context.Background(), se, c.table, now, "new-job-id", c.checkInterval)
			} else {
				job, err = m.lockHBTimeoutJob(context.Background(), se, c.table, now)
			}
			require.Equal(t, len(c.sqls), sqlCounter)
			if c.hasError {
				assert.NotNil(t, err)
				assert.Nil(t, job)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, job)
				assert.Equal(t, "test-id", job.ownerID)
				assert.Equal(t, cache.JobStatusRunning, job.status)
				assert.NotNil(t, job.tbl)
				assert.Same(t, c.table, job.tbl)
				if c.isCreate {
					assert.Equal(t, "new-job-id", job.id)
					assert.Equal(t, now, job.createTime)
					assert.Equal(t, newJobExpireTime, job.ttlExpireTime)
				} else {
					assert.Equal(t, "job1", job.id)
					assert.Equal(t, oldJobStartTime, job.createTime)
					assert.Equal(t, oldJobExpireTime, job.ttlExpireTime)
				}
				require.Equal(t, 1, len(m.runningJobs))
				require.Same(t, job, m.runningJobs[0])
			}
		})
	}
}

func TestLocalJobs(t *testing.T) {
	tbl1 := newMockTTLTbl(t, "t1")
	tbl1.ID = 1
	tbl2 := newMockTTLTbl(t, "t2")
	tbl2.ID = 2
	m := NewJobManager("test-id", nil, nil, nil, nil)
	m.sessPool = newMockSessionPool(t, tbl1, tbl2)

	m.runningJobs = []*ttlJob{{tbl: tbl1, id: "1"}, {tbl: tbl2, id: "2"}}
	m.tableStatusCache.Tables = map[int64]*cache.TableStatus{
		tbl1.ID: {
			CurrentJobOwnerID: m.id,
		},
		tbl2.ID: {
			CurrentJobOwnerID: "another-id",
		},
	}
	assert.Len(t, m.localJobs(), 1)
	assert.Equal(t, m.localJobs()[0].id, "1")
}
