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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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

// LockNewJob is an exported version of lockNewJob for test
func (m *JobManager) LockNewJob(ctx context.Context, se session.Session, table *cache.PhysicalTable, now time.Time, ignoreScheduleInterval bool) (*TTLJob, error) {
	return m.lockNewJob(ctx, se, table, now, ignoreScheduleInterval)
}

// RunningJobs returns the running jobs inside ttl job manager
func (m *JobManager) RunningJobs() []*TTLJob {
	return m.runningJobs
}

// InfoSchemaCache is an exported getter of infoSchemaCache for test
func (m *JobManager) InfoSchemaCache() *cache.InfoSchemaCache {
	return m.infoSchemaCache
}

// RescheduleJobs is an exported version of rescheduleJobs for test
func (m *JobManager) RescheduleJobs(se session.Session, now time.Time) {
	m.rescheduleJobs(se, now)
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
func (m *JobManager) ReportMetrics() {
	m.reportMetrics()
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

func TestReadyForNewJobTables(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")
	m := NewJobManager("test-id", nil, nil, nil)
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
		// for a newly inserted table, it'll always be scheduled
		{"newly created", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID}}, true},
		// table only in the table status cache will not be scheduled
		{"proper subset", []*cache.PhysicalTable{}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID}}, false},
		// table whose current job owner id is not empty, and heart beat time is long enough will not be scheduled
		{"current job not empty", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, CurrentJobOwnerID: "test-another-id", CurrentJobOwnerHBTime: time.Now()}}, false},
		// table whose current job owner id is not empty, but heart beat time is expired will be scheduled
		{"hb time expired", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, CurrentJobOwnerID: "test-another-id", CurrentJobOwnerHBTime: time.Now().Add(-time.Hour)}}, true},
		// if the last finished time is too near, it will also not be scheduled
		{"last start time too near", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, LastJobStartTime: time.Now()}}, false},
		// if the last finished time is expired, it will be scheduled
		{"last start time expired", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, LastJobStartTime: time.Now().Add(-time.Hour * 2)}}, true},
		// if the interval is 24h, and the last finished time is near, it will not be scheduled
		{"last start time too near for 24h", []*cache.PhysicalTable{tblWithDailyInterval}, []*cache.TableStatus{{TableID: tblWithDailyInterval.ID, ParentTableID: tblWithDailyInterval.ID, LastJobStartTime: time.Now().Add(-time.Hour * 2)}}, false},
		// if the interval is 24h, and the last finished time is far enough, it will be scheduled
		{"last start time far enough for 24h", []*cache.PhysicalTable{tblWithDailyInterval}, []*cache.TableStatus{{TableID: tblWithDailyInterval.ID, ParentTableID: tblWithDailyInterval.ID, LastJobStartTime: time.Now().Add(-time.Hour * 25)}}, true},
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

			tables := m.readyForNewJobTables(se.Now())
			if c.shouldSchedule {
				assert.Len(t, tables, 1)
				assert.Equal(t, int64(0), tables[0].ID)
				assert.Equal(t, int64(0), tables[0].TableInfo.ID)
			} else {
				assert.Len(t, tables, 0)
			}
		})
	}
}

func TestLockNewTable(t *testing.T) {
	now, err := time.Parse(timeFormat, "2022-12-05 17:13:05")
	assert.NoError(t, err)
	expireTime := now

	testPhysicalTable := &cache.PhysicalTable{ID: 1, Schema: model.NewCIStr("test"), TableInfo: &model.TableInfo{ID: 1, Name: model.NewCIStr("t1"), TTLInfo: &model.TTLInfo{ColumnName: model.NewCIStr("test"), IntervalExprStr: "5 Year", JobInterval: "1h"}}}

	type executeInfo struct {
		sql  string
		args []interface{}
	}
	getExecuteInfo := func(sql string, args []interface{}) executeInfo {
		return executeInfo{
			sql,
			args,
		}
	}
	getExecuteInfoForUpdate := func(sql string, args []interface{}) executeInfo {
		return executeInfo{
			sql + " FOR UPDATE NOWAIT",
			args,
		}
	}
	getExecuteInfoWithErr := func(sql string, args []interface{}, err error) executeInfo {
		require.NoError(t, err)
		return executeInfo{
			sql,
			args,
		}
	}
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/set-job-uuid", `return("test-job-id")`)
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/set-job-uuid")

	type sqlExecute struct {
		executeInfo

		rows []chunk.Row
		err  error
	}
	cases := []struct {
		name     string
		table    *cache.PhysicalTable
		sqls     []sqlExecute
		hasJob   bool
		hasError bool
	}{
		{"normal lock table", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("test-job-id", 1, now, expireTime, "test-id")),
				nil, nil,
			},
			{
				getExecuteInfo(createJobHistorySQL("test-job-id", testPhysicalTable, expireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfoWithErr(cache.InsertIntoTTLTask(newMockSession(t), "test-job-id", 1, 0, nil, nil, expireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfo(updateStatusSQL, nil),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, true, false},
		{"select nothing", testPhysicalTable, []sqlExecute{
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
				getExecuteInfo(setTableStatusOwnerSQL("test-job-id", 1, now, expireTime, "test-id")),
				nil, nil,
			},
			{
				getExecuteInfo(createJobHistorySQL("test-job-id", testPhysicalTable, expireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfoWithErr(cache.InsertIntoTTLTask(newMockSession(t), "test-job-id", 1, 0, nil, nil, expireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfo(updateStatusSQL, nil),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, true, false},
		{"return error", testPhysicalTable, []sqlExecute{
			{
				getExecuteInfoForUpdate(cache.SelectFromTTLTableStatusWithID(1)),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				getExecuteInfo(setTableStatusOwnerSQL("test-job-id", 1, now, expireTime, "test-id")),
				nil, errors.New("test error message"),
			},
			{
				getExecuteInfo(createJobHistorySQL("test-job-id", testPhysicalTable, expireTime, now)),
				nil, nil,
			},
			{
				getExecuteInfoWithErr(cache.InsertIntoTTLTask(newMockSession(t), "test-job-id", 1, 0, nil, nil, expireTime, now)),
				nil, nil,
			},
		}, false, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := NewJobManager("test-id", newMockSessionPool(t), nil, nil)
			m.infoSchemaCache.Tables[c.table.ID] = c.table
			sqlCounter := 0
			se := newMockSession(t)
			se.executeSQL = func(ctx context.Context, sql string, args ...interface{}) (rows []chunk.Row, err error) {
				assert.Less(t, sqlCounter, len(c.sqls))
				assert.Equal(t, c.sqls[sqlCounter].sql, sql)
				assert.Equal(t, c.sqls[sqlCounter].args, args)

				rows = c.sqls[sqlCounter].rows
				err = c.sqls[sqlCounter].err
				sqlCounter += 1
				return
			}
			se.evalExpire = now

			job, err := m.lockNewJob(context.Background(), se, c.table, now, false)
			if c.hasJob {
				assert.NotNil(t, job)
			} else {
				assert.Nil(t, job)
			}
			if c.hasError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestLocalJobs(t *testing.T) {
	tbl1 := newMockTTLTbl(t, "t1")
	tbl1.ID = 1
	tbl2 := newMockTTLTbl(t, "t2")
	tbl2.ID = 2
	m := NewJobManager("test-id", nil, nil, nil)
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

func TestRescheduleJobsOutOfWindow(t *testing.T) {
	// TODO: use failpoint to mock return job, and schedule

	tbl := newMockTTLTbl(t, "t1")
	se := newMockSession(t, tbl)

	scanWorker1 := NewMockScanWorker(t)
	scanWorker1.Start()
	scanWorker1.setOneRowResult(tbl, 2022)
	scanWorker2 := NewMockScanWorker(t)
	scanWorker2.Start()
	scanWorker2.setOneRowResult(tbl, 2022)

	m := NewJobManager("test-id", nil, nil, nil)
	m.sessPool = newMockSessionPool(t, tbl)
	m.taskManager.SetScanWorkers4Test([]worker{
		scanWorker1,
		scanWorker2,
	})

	// jobs will not be scheduled
	m.tableStatusCache.Tables = map[int64]*cache.TableStatus{
		tbl.ID: {
			CurrentJobOwnerID: m.id,
		},
	}
	m.runningJobs = []*ttlJob{newMockTTLJob(tbl, cache.JobStatusWaiting)}
	savedttlJobScheduleWindowStartTime := variable.TTLJobScheduleWindowStartTime.Load()
	savedttlJobScheduleWindowEndTime := variable.TTLJobScheduleWindowEndTime.Load()
	ttlJobScheduleWindowStartTime, _ := time.ParseInLocation(variable.FullDayTimeFormat, "12:00 +0000", time.UTC)
	variable.TTLJobScheduleWindowStartTime.Store(ttlJobScheduleWindowStartTime)
	ttlJobScheduleWindowEndTime, _ := time.ParseInLocation(variable.FullDayTimeFormat, "12:05 +0000", time.UTC)
	variable.TTLJobScheduleWindowEndTime.Store(ttlJobScheduleWindowEndTime)
	defer func() {
		variable.TTLJobScheduleWindowStartTime.Store(savedttlJobScheduleWindowStartTime)
		variable.TTLJobScheduleWindowEndTime.Store(savedttlJobScheduleWindowEndTime)
	}()

	now, _ := time.ParseInLocation(variable.FullDayTimeFormat, "12:06 +0000", time.UTC)
	m.rescheduleJobs(se, now)
	scanWorker1.checkWorkerStatus(workerStatusRunning, true, nil)
	scanWorker1.checkPollResult(false, "")
	scanWorker2.checkWorkerStatus(workerStatusRunning, true, nil)
	scanWorker2.checkPollResult(false, "")

	// jobs will be scheduled within the time window
	now, _ = time.ParseInLocation(variable.FullDayTimeFormat, "12:02 +0000", time.UTC)
	m.rescheduleJobs(se, now)
	//scanWorker1.checkWorkerStatus(workerStatusRunning, false, m.runningJobs[0].tasks[0])
	scanWorker1.checkPollResult(false, "")
	scanWorker2.checkWorkerStatus(workerStatusRunning, true, nil)
	scanWorker2.checkPollResult(false, "")
}
