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

package ttlworker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	dbsession "github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/client"
	"github.com/pingcap/tidb/ttl/metrics"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/ttl/ttlworker"
	"github.com/pingcap/tidb/util/logutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func sessionFactory(t *testing.T, store kv.Storage) func() session.Session {
	return func() session.Session {
		dbSession, err := dbsession.CreateSession4Test(store)
		require.NoError(t, err)
		se := session.NewSession(dbSession, dbSession, nil)

		_, err = se.ExecuteSQL(context.Background(), "ROLLBACK")
		require.NoError(t, err)
		_, err = se.ExecuteSQL(context.Background(), "set tidb_retry_limit=0")
		require.NoError(t, err)

		return se
	}
}

func TestParallelLockNewJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)

	sessionFactory := sessionFactory(t, store)

	testTable := &cache.PhysicalTable{ID: 2, TableInfo: &model.TableInfo{ID: 1, TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay), JobInterval: "1h"}}}
	// simply lock a new job
	m := ttlworker.NewJobManager("test-id", nil, store, nil)
	m.InfoSchemaCache().Tables[testTable.ID] = testTable

	se := sessionFactory()
	job, err := m.LockNewJob(context.Background(), se, testTable, time.Now(), false)
	require.NoError(t, err)
	job.Finish(se, time.Now(), &ttlworker.TTLSummary{})

	// lock one table in parallel, only one of them should lock successfully
	testTimes := 100
	concurrency := 5
	now := time.Now()
	for i := 0; i < testTimes; i++ {
		successCounter := atomic.NewUint64(0)
		successJob := &ttlworker.TTLJob{}

		now = now.Add(time.Hour * 48)

		wg := sync.WaitGroup{}
		for j := 0; j < concurrency; j++ {
			jobManagerID := fmt.Sprintf("test-ttl-manager-%d", j)
			wg.Add(1)
			go func() {
				m := ttlworker.NewJobManager(jobManagerID, nil, store, nil)
				m.InfoSchemaCache().Tables[testTable.ID] = testTable

				se := sessionFactory()
				job, err := m.LockNewJob(context.Background(), se, testTable, now, false)
				if err == nil {
					successCounter.Add(1)
					successJob = job
				} else {
					logutil.BgLogger().Info("lock new job with error", zap.Error(err))
				}
				wg.Done()
			}()
		}
		wg.Wait()

		require.Equal(t, uint64(1), successCounter.Load())
		successJob.Finish(se, time.Now(), &ttlworker.TTLSummary{})
	}
}

func TestFinishJob(t *testing.T) {
	timeFormat := "2006-01-02 15:04:05"
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, store)

	testTable := &cache.PhysicalTable{ID: 2, Schema: model.NewCIStr("db1"), TableInfo: &model.TableInfo{ID: 1, Name: model.NewCIStr("t1"), TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay)}}}

	tk.MustExec("insert into mysql.tidb_ttl_table_status(table_id) values (2)")

	// finish with error
	m := ttlworker.NewJobManager("test-id", nil, store, nil)
	m.InfoSchemaCache().Tables[testTable.ID] = testTable
	se := sessionFactory()
	startTime := time.Now()
	job, err := m.LockNewJob(context.Background(), se, testTable, startTime, false)
	require.NoError(t, err)

	expireTime, err := testTable.EvalExpireTime(context.Background(), se, startTime)
	require.NoError(t, err)
	tk.MustQuery("select * from mysql.tidb_ttl_job_history").Check(testkit.Rows(strings.Join([]string{
		job.ID(), "2", "1", "db1", "t1", "<nil>",
		startTime.Format(timeFormat),
		time.Unix(1, 0).Format(timeFormat),
		expireTime.Format(timeFormat),
		"<nil>", "<nil>", "<nil>", "<nil>",
		"running",
	}, " ")))

	summary := &ttlworker.TTLSummary{
		ScanTaskErr: "\"'an error message contains both single and double quote'\"",
		TotalRows:   128,
		SuccessRows: 120,
		ErrorRows:   8,
	}
	summaryBytes, err := json.Marshal(summary)
	summary.SummaryText = string(summaryBytes)

	require.NoError(t, err)
	endTime := time.Now()
	job.Finish(se, endTime, summary)
	tk.MustQuery("select table_id, last_job_summary from mysql.tidb_ttl_table_status").Check(testkit.Rows("2 " + summary.SummaryText))
	tk.MustQuery("select * from mysql.tidb_ttl_task").Check(testkit.Rows())
	expectedRow := []string{
		job.ID(), "2", "1", "db1", "t1", "<nil>",
		startTime.Format(timeFormat), endTime.Format(timeFormat), expireTime.Format(timeFormat),
		summary.SummaryText, "128", "120", "8", "finished",
	}
	tk.MustQuery("select * from mysql.tidb_ttl_job_history").Check(testkit.Rows(strings.Join(expectedRow, " ")))
}

func TestTTLAutoAnalyze(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/update-info-schema-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/update-info-schema-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/update-status-table-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/update-status-table-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/resize-workers-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/resize-workers-interval")
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/task-manager-loop-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/task-manager-loop-interval")

	originAutoAnalyzeMinCnt := handle.AutoAnalyzeMinCnt
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = originAutoAnalyzeMinCnt
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int, created_at datetime) ttl = `created_at` + interval 1 day")

	// insert ten rows, the 2,3,4,6,9,10 of them are expired
	for i := 1; i <= 10; i++ {
		t := time.Now()
		if i%2 == 0 || i%3 == 0 {
			t = t.Add(-time.Hour * 48)
		}

		tk.MustExec("insert into t values(?, ?)", i, t.Format(time.RFC3339))
	}
	// TODO: use a better way to pause and restart ttl worker after analyze the table to make it more stable
	// but as the ttl worker takes several seconds to start, it's not too serious.
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("show stats_meta").Rows()
	require.Equal(t, rows[0][4], "0")
	require.Equal(t, rows[0][5], "10")

	retryTime := 15
	retryInterval := time.Second * 2
	deleted := false
	for retryTime >= 0 {
		retryTime--
		time.Sleep(retryInterval)

		rows := tk.MustQuery("select count(*) from t").Rows()
		count := rows[0][0].(string)
		if count == "3" {
			deleted = true
			break
		}
	}
	require.True(t, deleted, "ttl should remove expired rows")

	h := dom.StatsHandle()
	is := dom.InfoSchema()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.True(t, h.HandleAutoAnalyze(is))
}

func TestTriggerTTLJob(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/task-manager-loop-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/task-manager-loop-interval")

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Minute)
	defer cancel()

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, t timestamp) TTL=`t` + INTERVAL 1 DAY")
	tbl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	tblID := tbl.Meta().ID
	require.NoError(t, err)

	// make sure the table had run a job one time to make the test stable
	cli := do.TTLJobManager().GetCommandCli()
	_, _ = client.TriggerNewTTLJob(ctx, cli, "test", "t")
	r := tk.MustQuery("select last_job_id, current_job_id from mysql.tidb_ttl_table_status where table_id=?", tblID)
	require.Equal(t, 1, len(r.Rows()))
	waitTTLJobFinished(t, tk, tblID)

	now := time.Now()
	nowDateStr := now.Format("2006-01-02 15:04:05.999999")
	expire := now.Add(-time.Hour * 25)
	expreDateStr := expire.Format("2006-01-02 15:04:05.999999")
	tk.MustExec("insert into t values(1, ?)", expreDateStr)
	tk.MustExec("insert into t values(2, ?)", nowDateStr)
	tk.MustExec("insert into t values(3, ?)", expreDateStr)
	tk.MustExec("insert into t values(4, ?)", nowDateStr)

	res, err := client.TriggerNewTTLJob(ctx, cli, "test", "t")
	require.NoError(t, err)
	require.Equal(t, 1, len(res.TableResult))
	tableResult := res.TableResult[0]
	require.Equal(t, tblID, tableResult.TableID)
	require.NotEmpty(t, tableResult.JobID)
	require.Equal(t, "test", tableResult.DBName)
	require.Equal(t, "t", tableResult.TableName)
	require.Equal(t, "", tableResult.ErrorMessage)
	require.Equal(t, "", tableResult.PartitionName)

	waitTTLJobFinished(t, tk, tblID)
	tk.MustQuery("select id from t order by id asc").Check(testkit.Rows("2", "4"))
}

func TestTTLDeleteWithTimeZoneChange(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/task-manager-loop-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/task-manager-loop-interval")

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.time_zone='Asia/Shanghai'")
	tk.MustExec("set @@time_zone='Asia/Shanghai'")

	tk.MustExec("create table t1(id int primary key, t datetime) TTL=`t` + INTERVAL 1 DAY TTL_ENABLE='OFF'")
	tbl1, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tblID1 := tbl1.Meta().ID
	tk.MustExec("insert into t1 values(1, NOW()), (2, NOW() - INTERVAL 31 HOUR), (3, NOW() - INTERVAL 33 HOUR)")

	tk.MustExec("create table t2(id int primary key, t timestamp) TTL=`t` + INTERVAL 1 DAY TTL_ENABLE='OFF'")
	tbl2, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tblID2 := tbl2.Meta().ID
	tk.MustExec("insert into t2 values(1, NOW()), (2, NOW() - INTERVAL 31 HOUR), (3, NOW() - INTERVAL 33 HOUR)")

	tk.MustExec("set @@global.time_zone='UTC'")
	tk.MustExec("set @@time_zone='UTC'")
	tk.MustExec("alter table t1 TTL_ENABLE='ON'")
	tk.MustExec("alter table t2 TTL_ENABLE='ON'")

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	defer cancel()
	cli := do.TTLJobManager().GetCommandCli()
	_, _ = client.TriggerNewTTLJob(ctx, cli, "test", "t1")
	_, _ = client.TriggerNewTTLJob(ctx, cli, "test", "t2")

	waitTTLJobFinished(t, tk, tblID1)
	tk.MustQuery("select id from t1 order by id asc").Check(testkit.Rows("1", "2"))

	waitTTLJobFinished(t, tk, tblID2)
	tk.MustQuery("select id from t2 order by id asc").Check(testkit.Rows("1"))
}

func waitTTLJobFinished(t *testing.T, tk *testkit.TestKit, tableID int64) {
	start := time.Now()
	for time.Since(start) < time.Minute {
		time.Sleep(time.Second)
		r := tk.MustQuery("select last_job_id, current_job_id from mysql.tidb_ttl_table_status where table_id=?", tableID)
		rows := r.Rows()
		if len(rows) == 0 {
			continue
		}

		if rows[0][0] == "<nil>" {
			continue
		}

		if rows[0][1] != "<nil>" {
			continue
		}

		return
	}
	require.FailNow(t, "timeout")
}

func TestTTLJobDisable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/update-info-schema-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/update-info-schema-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/update-status-table-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/update-status-table-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/ttl/ttlworker/resize-workers-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/ttl/ttlworker/resize-workers-interval")

	originAutoAnalyzeMinCnt := handle.AutoAnalyzeMinCnt
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = originAutoAnalyzeMinCnt
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int, created_at datetime) ttl = `created_at` + interval 1 day")

	// insert ten rows, the 2,3,4,6,9,10 of them are expired
	for i := 1; i <= 10; i++ {
		t := time.Now()
		if i%2 == 0 || i%3 == 0 {
			t = t.Add(-time.Hour * 48)
		}

		tk.MustExec("insert into t values(?, ?)", i, t.Format(time.RFC3339))
	}
	// turn off the `tidb_ttl_job_enable`
	tk.MustExec("set global tidb_ttl_job_enable = 'OFF'")
	defer tk.MustExec("set global tidb_ttl_job_enable = 'ON'")

	retryTime := 15
	retryInterval := time.Second * 2
	deleted := false
	for retryTime >= 0 {
		retryTime--
		time.Sleep(retryInterval)

		rows := tk.MustQuery("select count(*) from t").Rows()
		count, err := strconv.Atoi(rows[0][0].(string))
		require.NoError(t, err)
		if count < 10 {
			deleted = true
			break
		}

		require.Len(t, dom.TTLJobManager().RunningJobs(), 0)
	}
	require.False(t, deleted)
}

func TestRescheduleJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	now := time.Now()
	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	m := ttlworker.NewJobManager("manager-1", nil, store, nil)
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	// schedule jobs
	m.RescheduleJobs(se, now)
	sql, args := cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	tableStatus, err := cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)

	originalJobID := tableStatus.CurrentJobID
	require.NotEmpty(t, originalJobID)
	require.Equal(t, "manager-1", tableStatus.CurrentJobOwnerID)
	// there is already a task
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))

	// another manager should get this job, if the heart beat is not updated
	anotherManager := ttlworker.NewJobManager("manager-2", nil, store, nil)
	anotherManager.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, anotherManager.InfoSchemaCache().Update(se))
	anotherManager.RescheduleJobs(se, now.Add(time.Hour))
	sql, args = cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
	rows, err = se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	tableStatus, err = cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)

	// but the orignal job should be inherited
	require.NotEmpty(t, tableStatus.CurrentJobID)
	require.Equal(t, "manager-2", tableStatus.CurrentJobOwnerID)
	require.Equal(t, originalJobID, tableStatus.CurrentJobID)

	// if the time leaves the time window, it'll finish the job
	tk.MustExec("set global tidb_ttl_job_schedule_window_start_time='23:58'")
	tk.MustExec("set global tidb_ttl_job_schedule_window_end_time='23:59'")
	anotherManager.RescheduleJobs(se, time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, now.Nanosecond(), now.Location()))
	tk.MustQuery("select last_job_summary->>'$.scan_task_err' from mysql.tidb_ttl_table_status").Check(testkit.Rows("ttl job is disabled"))
}

func TestJobTimeout(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	now := time.Now()
	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	m := ttlworker.NewJobManager("manager-1", nil, store, nil)
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	// schedule jobs
	m.RescheduleJobs(se, now)
	// set the worker to be empty, so none of the tasks will be scheduled
	m.TaskManager().SetScanWorkers4Test([]ttlworker.Worker{})

	sql, args := cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	tableStatus, err := cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)

	require.NotEmpty(t, tableStatus.CurrentJobID)
	require.Equal(t, "manager-1", tableStatus.CurrentJobOwnerID)
	// there is already a task
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))

	// the timeout will be checked while updating heartbeat
	require.NoError(t, m.UpdateHeartBeat(ctx, se, now.Add(7*time.Hour)))
	tk.MustQuery("select last_job_summary->>'$.scan_task_err' from mysql.tidb_ttl_table_status").Check(testkit.Rows("job is timeout"))
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))
}

func TestTriggerScanTask(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)
	now := time.Now()
	se := sessionFactory()

	waitAndStopTTLManager(t, dom)

	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")

	m := ttlworker.NewJobManager("manager-1", nil, store, nil)
	require.NoError(t, m.InfoSchemaCache().Update(se))
	m.TaskManager().ResizeWorkersWithSysVar()
	m.Start()
	defer func() {
		m.Stop()
		require.NoError(t, m.WaitStopped(context.Background(), time.Second*10))
	}()

	nCli := m.GetNotificationCli()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-nCli.WatchNotification(context.Background(), "scan")
		wg.Done()
	}()
	m.RescheduleJobs(se, now)

	// notification is sent
	wg.Wait()

	for time.Now().Before(now.Add(time.Second * 5)) {
		time.Sleep(time.Second)
		rows := tk.MustQuery("SELECT status FROM mysql.tidb_ttl_task").Rows()
		if len(rows) == 0 {
			break
		}
		if rows[0][0] == cache.TaskStatusFinished {
			break
		}
	}
}

func waitAndStopTTLManager(t *testing.T, dom *domain.Domain) {
	maxWaitTime := 30
	for {
		maxWaitTime--
		if maxWaitTime < 0 {
			require.Fail(t, "fail to stop ttl manager")
		}
		if dom.TTLJobManager() != nil {
			dom.TTLJobManager().Stop()
			require.NoError(t, dom.TTLJobManager().WaitStopped(context.Background(), time.Second*10))
			return
		}
		time.Sleep(time.Second)
		continue
	}
}

func TestGCScanTasks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	addTableStatusRecord := func(tableID, parentTableID, curJobID int64) {
		tk.MustExec("INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (?, ?)", tableID, parentTableID)
		if curJobID == 0 {
			return
		}

		tk.MustExec(`UPDATE mysql.tidb_ttl_table_status
			SET current_job_id = ?,
				current_job_owner_id = '12345',
				current_job_start_time = NOW(),
				current_job_status = 'running',
				current_job_status_update_time = NOW(),
				current_job_ttl_expire = NOW(),
				current_job_owner_hb_time = NOW()
			WHERE table_id = ?`, curJobID, tableID)
	}

	addScanTaskRecord := func(jobID, tableID, scanID int64) {
		tk.MustExec(`INSERT INTO mysql.tidb_ttl_task SET
			job_id = ?,
			table_id = ?,
			scan_id = ?,
			expire_time = NOW(),
			created_time = NOW()`, jobID, tableID, scanID)
	}

	addTableStatusRecord(1, 1, 1)
	addScanTaskRecord(1, 1, 1)
	addScanTaskRecord(1, 1, 2)
	addScanTaskRecord(2, 1, 1)
	addScanTaskRecord(2, 1, 2)
	addScanTaskRecord(3, 2, 1)
	addScanTaskRecord(3, 2, 2)

	se := session.NewSession(tk.Session(), tk.Session(), func(_ session.Session) {})
	ttlworker.DoGC(context.TODO(), se)
	tk.MustQuery("select job_id, scan_id from mysql.tidb_ttl_task order by job_id, scan_id asc").Check(testkit.Rows("1 1", "1 2"))
}

func TestGCTTLHistory(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	addHistory := func(jobID, createdBeforeDays int) {
		tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.tidb_ttl_job_history (
				job_id,
				table_id,
				parent_table_id,
				table_schema,
				table_name,
				partition_name,
				create_time,
				finish_time,
				ttl_expire,
				summary_text,
				expired_rows,
				deleted_rows,
				error_delete_rows,
				status
			)
		VALUES
			(
			 	%d, 1, 1, 'test', 't1', '',
			 	CURDATE() - INTERVAL %d DAY,
			 	CURDATE() - INTERVAL %d DAY + INTERVAL 1 HOUR,
			 	CURDATE() - INTERVAL %d DAY,
			 	"", 100, 100, 0, "finished"
		)`, jobID, createdBeforeDays, createdBeforeDays, createdBeforeDays))
	}

	addHistory(1, 1)
	addHistory(2, 30)
	addHistory(3, 60)
	addHistory(4, 89)
	addHistory(5, 90)
	addHistory(6, 91)
	addHistory(7, 100)
	se := session.NewSession(tk.Session(), tk.Session(), func(_ session.Session) {})
	ttlworker.DoGC(context.TODO(), se)
	tk.MustQuery("select job_id from mysql.tidb_ttl_job_history order by job_id asc").Check(testkit.Rows("1", "2", "3", "4", "5"))
}

func TestJobMetrics(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	now := time.Now()
	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	m := ttlworker.NewJobManager("manager-1", nil, store, nil)
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	// schedule jobs
	m.RescheduleJobs(se, now)
	// set the worker to be empty, so none of the tasks will be scheduled
	m.TaskManager().SetScanWorkers4Test([]ttlworker.Worker{})

	sql, args := cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	tableStatus, err := cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)

	require.NotEmpty(t, tableStatus.CurrentJobID)
	require.Equal(t, "manager-1", tableStatus.CurrentJobOwnerID)
	require.Equal(t, cache.JobStatusRunning, tableStatus.CurrentJobStatus)

	m.ReportMetrics()
	out := &dto.Metric{}
	require.NoError(t, metrics.RunningJobsCnt.Write(out))
	require.Equal(t, float64(1), out.GetGauge().GetValue())
}
