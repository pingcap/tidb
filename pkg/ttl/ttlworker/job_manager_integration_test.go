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
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/testkit/testflag"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	timertable "github.com/pingcap/tidb/pkg/timer/tablestore"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/client"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/skip"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func sessionFactory(t *testing.T, dom *domain.Domain) func() session.Session {
	pool := dom.SysSessionPool()

	return func() session.Session {
		se, err := ttlworker.GetSessionForTest(pool)
		require.NoError(t, err)

		return se
	}
}

func TestGetSession(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@time_zone = 'Asia/Shanghai'")
	tk.MustExec("set @@global.time_zone= 'Europe/Berlin'")
	tk.MustExec("set @@tidb_retry_limit=1")
	tk.MustExec("set @@tidb_enable_1pc=0")
	tk.MustExec("set @@tidb_enable_async_commit=0")
	tk.MustExec("set @@tidb_isolation_read_engines='tiflash,tidb'")
	var getCnt atomic.Int32

	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		if getCnt.CompareAndSwap(0, 1) {
			return tk.Session(), nil
		}
		require.FailNow(t, "get session more than once")
		return nil, nil
	}, 1, 1, 0)
	defer pool.Close()

	se, err := ttlworker.GetSessionForTest(pool)
	require.NoError(t, err)
	defer se.Close()

	// global time zone should not change
	tk.MustQuery("select @@global.time_zone").Check(testkit.Rows("Europe/Berlin"))
	tz, err := se.GlobalTimeZone(context.TODO())
	require.NoError(t, err)
	require.Equal(t, "Europe/Berlin", tz.String())

	// session variables should be set
	tk.MustQuery("select @@time_zone, @@tidb_retry_limit, @@tidb_enable_1pc, @@tidb_enable_async_commit, @@tidb_isolation_read_engines").
		Check(testkit.Rows("UTC 0 1 1 tikv,tiflash,tidb"))

	// all session variables should be restored after close
	se.Close()
	tk.MustQuery("select @@time_zone, @@tidb_retry_limit, @@tidb_enable_1pc, @@tidb_enable_async_commit, @@tidb_isolation_read_engines").
		Check(testkit.Rows("Asia/Shanghai 1 0 0 tiflash,tidb"))
}

func TestParallelLockNewJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, dom)

	testTable := &cache.PhysicalTable{ID: 2, TableInfo: &model.TableInfo{ID: 1, TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay), JobInterval: "1h"}}}
	// simply lock a new job
	m := ttlworker.NewJobManager("test-id", nil, store, nil, nil)
	m.InfoSchemaCache().Tables[testTable.ID] = testTable

	se := sessionFactory()
	defer se.Close()
	job, err := m.LockJob(context.Background(), se, testTable, se.Now(), uuid.NewString(), false)
	require.NoError(t, err)
	job.Finish(se, se.Now(), &ttlworker.TTLSummary{})

	// lock one table in parallel, only one of them should lock successfully
	testDuration := time.Second
	concurrency := 5
	if testflag.Long() {
		testDuration = 5 * time.Minute
		concurrency = 50
	}

	testStart := time.Now()
	for time.Since(testStart) < testDuration {
		now := se.Now()

		// reset the table status.
		tk.MustExec("delete from mysql.tidb_ttl_table_status")

		successCounter := atomic.NewUint64(0)
		successJob := &ttlworker.TTLJob{}

		wg := sync.WaitGroup{}
		stopErr := atomic.NewError(nil)
		for j := 0; j < concurrency; j++ {
			jobManagerID := fmt.Sprintf("test-ttl-manager-%d", j)
			wg.Add(1)
			go func() {
				m := ttlworker.NewJobManager(jobManagerID, nil, store, nil, nil)
				m.InfoSchemaCache().Tables[testTable.ID] = testTable

				se := sessionFactory()
				defer se.Close()
				job, err := m.LockJob(context.Background(), se, testTable, now, uuid.NewString(), false)
				if err == nil {
					successCounter.Add(1)
					successJob = job
				} else {
					logutil.BgLogger().Info("lock new job with error", zap.Error(err))
				}

				m.Stop()
				err = m.WaitStopped(context.Background(), 5*time.Second)
				stopErr.CompareAndSwap(nil, err)
				wg.Done()
			}()
		}
		wg.Wait()

		require.Equal(t, uint64(1), successCounter.Load())
		require.Nil(t, stopErr.Load())
		successJob.Finish(se, se.Now(), &ttlworker.TTLSummary{})
	}
}

func TestFinishJob(t *testing.T) {
	timeFormat := time.DateTime
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, dom)

	testTable := &cache.PhysicalTable{ID: 2, Schema: ast.NewCIStr("db1"), TableInfo: &model.TableInfo{ID: 1, Name: ast.NewCIStr("t1"), TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay)}}}

	tk.MustExec("insert into mysql.tidb_ttl_table_status(table_id) values (2)")

	// finish with error
	m := ttlworker.NewJobManager("test-id", nil, store, nil, nil)
	m.InfoSchemaCache().Tables[testTable.ID] = testTable
	se := sessionFactory()
	startTime := se.Now()
	job, err := m.LockJob(context.Background(), se, testTable, startTime, uuid.NewString(), false)
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
	endTime := se.Now()
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
	defer boostJobScheduleForTest(t)()

	originAutoAnalyzeMinCnt := statistics.AutoAnalyzeMinCnt
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = originAutoAnalyzeMinCnt
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int, created_at datetime, index idx(id, created_at))")

	// insert ten rows, the 2,3,4,6,9,10 of them are expired
	for i := 1; i <= 10; i++ {
		t := time.Now()
		if i%2 == 0 || i%3 == 0 {
			t = t.Add(-time.Hour * 48)
		}

		tk.MustExec("insert into t values(?, ?)", i, t.Format(time.RFC3339))
	}
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("show stats_meta").Rows()
	require.Equal(t, rows[0][4], "0")
	require.Equal(t, rows[0][5], "10")
	tk.MustExec("alter table t ttl = `created_at` + interval 1 day")

	retryTime := 300
	retryInterval := 100 * time.Millisecond
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), is))
	require.True(t, h.HandleAutoAnalyze())
}

func TestTriggerTTLJob(t *testing.T) {
	defer boostJobScheduleForTest(t)()

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Minute)
	defer cancel()

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, t timestamp) TTL=`t` + INTERVAL 1 DAY")
	tbl, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	tblID := tbl.Meta().ID
	require.NoError(t, err)

	timerStore := timertable.NewTableTimerStore(0, do.SysSessionPool(), "mysql", "tidb_timers", nil)
	defer timerStore.Close()
	timerCli := timerapi.NewDefaultTimerClient(timerStore)

	// make sure the table had run a job one time to make the test stable
	waitTTLJobFinished(t, tk, tblID, timerCli)

	now := time.Now()
	nowDateStr := now.Format("2006-01-02 15:04:05.999999")
	expire := now.Add(-time.Hour * 25)
	expreDateStr := expire.Format("2006-01-02 15:04:05.999999")
	tk.MustExec("insert into t values(1, ?)", expreDateStr)
	tk.MustExec("insert into t values(2, ?)", nowDateStr)
	tk.MustExec("insert into t values(3, ?)", expreDateStr)
	tk.MustExec("insert into t values(4, ?)", nowDateStr)

	cli := do.TTLJobManager().GetCommandCli()
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

	waitTTLJobFinished(t, tk, tblID, timerCli)
	tk.MustQuery("select id from t order by id asc").Check(testkit.Rows("2", "4"))
}

func TestTTLDeleteWithTimeZoneChange(t *testing.T) {
	defer boostJobScheduleForTest(t)()

	store, do := testkit.CreateMockStoreAndDomain(t)
	timerStore := timertable.NewTableTimerStore(0, do.SysSessionPool(), "mysql", "tidb_timers", nil)
	defer timerStore.Close()
	timerCli := timerapi.NewDefaultTimerClient(timerStore)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.time_zone='Asia/Shanghai'")
	tk.MustExec("set @@time_zone='Asia/Shanghai'")
	tk.MustExec("set @@global.tidb_ttl_running_tasks=32")

	tk.MustExec("create table t1(id int primary key, t datetime) TTL=`t` + INTERVAL 1 DAY TTL_ENABLE='OFF'")
	tbl1, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tblID1 := tbl1.Meta().ID
	tk.MustExec("insert into t1 values(1, NOW()), (2, NOW() - INTERVAL 31 HOUR), (3, NOW() - INTERVAL 33 HOUR)")

	tk.MustExec("create table t2(id int primary key, t timestamp) TTL=`t` + INTERVAL 1 DAY TTL_ENABLE='OFF'")
	tbl2, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tblID2 := tbl2.Meta().ID
	tk.MustExec("insert into t2 values(1, NOW()), (2, NOW() - INTERVAL 31 HOUR), (3, NOW() - INTERVAL 33 HOUR)")

	tk.MustExec("set @@global.time_zone='UTC'")
	tk.MustExec("set @@time_zone='UTC'")
	tk.MustExec("alter table t1 TTL_ENABLE='ON'")
	tk.MustExec("alter table t2 TTL_ENABLE='ON'")

	waitTTLJobFinished(t, tk, tblID1, timerCli)
	tk.MustQuery("select id from t1 order by id asc").Check(testkit.Rows("1", "2"))

	waitTTLJobFinished(t, tk, tblID2, timerCli)
	tk.MustQuery("select id from t2 order by id asc").Check(testkit.Rows("1"))
}

func waitTTLJobFinished(t *testing.T, tk *testkit.TestKit, tableID int64, timerCli timerapi.TimerClient) {
	start := time.Now()
	for time.Since(start) < time.Minute {
		time.Sleep(10 * time.Millisecond)
		r := tk.MustQuery("select last_job_id, current_job_id, parent_table_id from mysql.tidb_ttl_table_status where table_id=?", tableID)
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

		parentID, err := strconv.ParseInt(rows[0][2].(string), 10, 64)
		require.NoError(t, err)

		timer, err := timerCli.GetTimerByKey(context.Background(), fmt.Sprintf("/tidb/ttl/physical_table/%d/%d", parentID, tableID))
		require.NoError(t, err)
		if timer.EventStatus == timerapi.SchedEventTrigger {
			continue
		}

		return
	}
	require.FailNow(t, "timeout")
}

func TestTTLJobDisable(t *testing.T) {
	defer boostJobScheduleForTest(t)()
	originAutoAnalyzeMinCnt := statistics.AutoAnalyzeMinCnt
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = originAutoAnalyzeMinCnt
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// turn off the `tidb_ttl_job_enable`
	tk.MustExec("set global tidb_ttl_job_enable = 'OFF'")
	defer tk.MustExec("set global tidb_ttl_job_enable = 'ON'")

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

	time.Sleep(time.Second)

	// no rows should be deleted as the ttl job is disabled
	rows := tk.MustQuery("select count(*) from t").Rows()
	count, err := strconv.Atoi(rows[0][0].(string))
	require.NoError(t, err)
	require.Equal(t, 10, count)

	// no jobs
	require.Len(t, dom.TTLJobManager().RunningJobs(), 0)
	tk.MustQuery("select count(1) from mysql.tidb_ttl_job_history").Check(testkit.Rows("0"))
}

func TestSubmitJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, dom)

	waitAndStopTTLManager(t, dom)

	tk.MustExec("use test")
	tk.MustExec("create table ttlp1(a int, t timestamp) TTL=`t`+interval 1 HOUR PARTITION BY RANGE (a) (" +
		"PARTITION p0 VALUES LESS THAN (10)," +
		"PARTITION p1 VALUES LESS THAN (100)" +
		")")
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttlp1"))
	require.NoError(t, err)
	tableID := table.Meta().ID
	var physicalID int64
	for _, def := range table.Meta().Partition.Definitions {
		if def.Name.L == "p0" {
			physicalID = def.ID
			break
		}
	}
	require.NotZero(t, physicalID)

	var leader atomic.Bool
	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return leader.Load()
	})

	se := sessionFactory()

	// not leader
	err = m.SubmitJob(se, tableID, physicalID, "req1")
	require.EqualError(t, err, "current TTL manager is not the leader")

	leader.Store(true)
	// invalid table
	err = m.SubmitJob(se, 9999, 9999, "req1")
	require.ErrorContains(t, err, "not exists in information schema")

	err = m.SubmitJob(se, tableID, 9999, "req1")
	require.ErrorContains(t, err, "not exists in information schema")

	err = m.SubmitJob(se, 9999, physicalID, "req1")
	require.ErrorContains(t, err, "for physical table with id")

	// check no success job submitted
	tk.MustQuery("select count(1) from mysql.tidb_ttl_table_status").Check(testkit.Rows("0"))
	tk.MustQuery("select count(1) from mysql.tidb_ttl_job_history").Check(testkit.Rows("0"))
	tk.MustQuery("select count(1) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))

	// submit successfully
	now := time.Now()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
	require.NoError(t, m.SubmitJob(se, tableID, physicalID, "request1"))
	sql, args := cache.SelectFromTTLTableStatusWithID(physicalID)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	tableStatus, err := cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)
	require.Equal(t, physicalID, tableStatus.TableID)
	require.Equal(t, tableID, tableStatus.ParentTableID)
	require.Equal(t, "request1", tableStatus.CurrentJobID)
	require.Equal(t, "manager-1", tableStatus.CurrentJobOwnerID)
	require.Equal(t, cache.JobStatusRunning, tableStatus.CurrentJobStatus)
	require.InDelta(t, tableStatus.CurrentJobTTLExpire.Unix(), now.Unix()-3600, 300)
	require.Greater(t, tableStatus.CurrentJobOwnerHBTime.Unix(), now.Unix()-10)
	require.Greater(t, tableStatus.CurrentJobStartTime.Unix(), now.Unix()-10)
	require.Greater(t, tableStatus.CurrentJobStatusUpdateTime.Unix(), now.Unix()-10)
	tk.MustQuery("select table_id, scan_id, UNIX_TIMESTAMP(expire_time), status from mysql.tidb_ttl_task where job_id='request1'").
		Check(testkit.Rows(fmt.Sprintf("%d 0 %d waiting", physicalID, tableStatus.CurrentJobTTLExpire.Unix())))
	tk.MustQuery("select parent_table_id, table_id, table_schema, table_name, partition_name, " +
		"UNIX_TIMESTAMP(create_time), UNIX_TIMESTAMP(ttl_expire), status " +
		"from mysql.tidb_ttl_job_history where job_id='request1'").Check(testkit.Rows(fmt.Sprintf(
		"%d %d test ttlp1 p0 %d %d running",
		tableID, physicalID, tableStatus.CurrentJobStartTime.Unix(), tableStatus.CurrentJobTTLExpire.Unix(),
	)))
}

func TestRescheduleJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, dom)

	waitAndStopTTLManager(t, dom)

	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	now := se.Now()
	m := ttlworker.NewJobManager("manager-1", dom.SysSessionPool(), store, nil, func() bool {
		return true
	})
	defer m.TaskManager().ResizeWorkersToZero(t)
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	require.NoError(t, m.TableStatusCache().Update(context.Background(), se))
	// submit job
	require.NoError(t, m.SubmitJob(se, table.Meta().ID, table.Meta().ID, "request1"))
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
	anotherManager := ttlworker.NewJobManager("manager-2", dom.SysSessionPool(), store, nil, nil)
	defer anotherManager.TaskManager().ResizeWorkersToZero(t)
	anotherManager.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, anotherManager.InfoSchemaCache().Update(se))
	require.NoError(t, anotherManager.TableStatusCache().Update(context.Background(), se))
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
	rescheduleTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, now.Nanosecond(), now.Location())
	anotherManager.RescheduleJobs(se, rescheduleTime)
	tkTZ := tk.Session().GetSessionVars().Location()
	tk.MustQuery("select last_job_summary->>'$.scan_task_err' from mysql.tidb_ttl_table_status").Check(testkit.Rows("out of TTL job schedule window"))
	tk.MustQuery("select last_job_finish_time from mysql.tidb_ttl_table_status").Check(testkit.Rows(rescheduleTime.In(tkTZ).Format(time.DateTime)))
}

func TestRescheduleJobsAfterTableDropped(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, dom)

	waitAndStopTTLManager(t, dom)

	now := time.Now().In(time.UTC)
	createTableSQL := "create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'"
	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	removeBehaviors := []struct {
		remove string
		resume string
	}{
		{"drop table test.t", createTableSQL},
		{"alter table test.t remove ttl", "alter table test.t ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'"},
		{"alter table test.t ttl_enable = 'OFF'", "alter table test.t ttl_enable = 'ON'"},
	}
	for i, rb := range removeBehaviors {
		se := sessionFactory()
		m := ttlworker.NewJobManager("manager-1", dom.SysSessionPool(), store, nil, func() bool {
			return true
		})
		m.TaskManager().ResizeWorkersWithSysVar()
		require.NoError(t, m.InfoSchemaCache().Update(se))
		require.NoError(t, m.TableStatusCache().Update(context.Background(), se))
		// submit job
		require.NoError(t, m.SubmitJob(se, table.Meta().ID, table.Meta().ID, fmt.Sprintf("request%d", i)))
		sql, args := cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
		rows, err := se.ExecuteSQL(ctx, sql, args...)
		require.NoError(t, err)
		tableStatus, err := cache.RowToTableStatus(se, rows[0])
		require.NoError(t, err)
		require.Equal(t, "manager-1", tableStatus.CurrentJobOwnerID)
		// there is already a task
		tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))

		// break the table
		tk.MustExec(rb.remove)
		require.NoError(t, m.InfoSchemaCache().Update(se))
		require.NoError(t, m.TableStatusCache().Update(context.Background(), se))
		m.RescheduleJobs(se, time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, now.Nanosecond(), now.Location()))
		tk.MustQuery("select last_job_summary->>'$.scan_task_err' from mysql.tidb_ttl_table_status").Check(testkit.Rows("TTL table has been removed or the TTL on this table has been stopped"))

		// resume the table
		tk.MustExec(rb.resume)
		table, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		m.DoGC(context.TODO(), se, now)

		m.TaskManager().ResizeWorkersToZero(t)
	}
}

func TestJobTimeout(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, dom)

	waitAndStopTTLManager(t, dom)

	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	tableID := table.Meta().ID
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	now := se.Now()
	m := ttlworker.NewJobManager("manager-1", dom.SysSessionPool(), store, nil, func() bool {
		return true
	})
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	require.NoError(t, m.TableStatusCache().Update(context.Background(), se))
	// submit job
	require.NoError(t, m.SubmitJob(se, tableID, tableID, "request1"))
	// set the worker to be empty, so none of the tasks will be scheduled
	m.TaskManager().ResizeWorkersToZero(t)

	sql, args := cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	tableStatus, err := cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)

	require.NotEmpty(t, tableStatus.CurrentJobID)
	require.Equal(t, "manager-1", tableStatus.CurrentJobOwnerID)
	// there is already a task
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))

	m2 := ttlworker.NewJobManager("manager-2", dom.SysSessionPool(), store, nil, nil)
	m2.TaskManager().ResizeWorkersWithSysVar()
	defer m2.TaskManager().ResizeWorkersToZero(t)

	require.NoError(t, m2.InfoSchemaCache().Update(se))
	require.NoError(t, m2.TableStatusCache().Update(context.Background(), se))
	// schedule jobs
	now = now.Add(10 * time.Minute)
	m2.RescheduleJobs(se, now)
	jobs := m2.RunningJobs()
	require.Equal(t, 1, len(jobs))
	require.Equal(t, jobs[0].ID(), tableStatus.CurrentJobID)

	// check job has taken by another manager
	sql, args = cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
	rows, err = se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	newTableStatus, err := cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)
	require.Equal(t, "manager-2", newTableStatus.CurrentJobOwnerID)
	require.Equal(t, tableStatus.CurrentJobID, newTableStatus.CurrentJobID)
	require.Equal(t, tableStatus.CurrentJobStartTime, newTableStatus.CurrentJobStartTime)
	require.Equal(t, tableStatus.CurrentJobTTLExpire, newTableStatus.CurrentJobTTLExpire)
	require.Equal(t, now.Unix(), newTableStatus.CurrentJobOwnerHBTime.Unix())
	// the `CurrentJobStatusUpdateTime` only has `s` precision, so use any format with only `s` precision and a TZ to compare.
	require.Equal(t, now.Format(time.RFC3339), newTableStatus.CurrentJobStatusUpdateTime.Format(time.RFC3339))

	// the timeout will be checked while updating heartbeat
	require.NoError(t, m2.UpdateHeartBeatForJob(ctx, se, now.Add(7*time.Hour), m2.RunningJobs()[0]))
	tk.MustQuery("select last_job_summary->>'$.scan_task_err' from mysql.tidb_ttl_table_status").Check(testkit.Rows("job is timeout"))
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))
}

func TestTriggerScanTask(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, dom)
	se := sessionFactory()

	waitAndStopTTLManager(t, dom)

	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID

	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
	require.NoError(t, m.InfoSchemaCache().Update(se))
	nCli := m.GetNotificationCli()
	done := make(chan struct{})
	go func() {
		<-nCli.WatchNotification(context.Background(), "scan")
		close(done)
	}()
	require.NoError(t, m.SubmitJob(se, tblID, tblID, "request1"))

	// notification is sent
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-timeoutCtx.Done():
		require.FailNow(t, "notification not got")
	}
}

func waitAndStopTTLManager(t *testing.T, dom *domain.Domain) {
	maxWaitTime := 300
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
		time.Sleep(time.Millisecond * 10)
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

	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
	se := session.NewSession(tk.Session(), tk.Session(), func(_ session.Session) {})
	m.DoGC(context.TODO(), se, se.Now())
	tk.MustQuery("select job_id, scan_id from mysql.tidb_ttl_task order by job_id, scan_id asc").Check(testkit.Rows("1 1", "1 2"))
}

func TestGCTableStatus(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// stop TTLJobManager to avoid unnecessary job schedule and make test stable
	dom.TTLJobManager().Stop()
	require.NoError(t, dom.TTLJobManager().WaitStopped(context.Background(), time.Minute))

	// insert table status without corresponding table
	tk.MustExec("INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (?, ?)", 2024, 2024)

	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
	se := session.NewSession(tk.Session(), tk.Session(), func(_ session.Session) {})
	m.DoGC(context.TODO(), se, se.Now())
	tk.MustQuery("select * from mysql.tidb_ttl_table_status").Check(nil)

	// insert a running table status without corresponding table
	tk.MustExec("INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (?, ?)", 2024, 2024)
	tk.MustExec(`UPDATE mysql.tidb_ttl_table_status
			SET current_job_id = ?,
				current_job_owner_id = '12345',
				current_job_start_time = NOW(),
				current_job_status = 'running',
				current_job_status_update_time = NOW(),
				current_job_ttl_expire = NOW(),
				current_job_owner_hb_time = NOW()
			WHERE table_id = ?`, 1, 2024)
	m.DoGC(context.TODO(), se, se.Now())
	// it'll not be removed
	tk.MustQuery("select current_job_id from mysql.tidb_ttl_table_status").Check(testkit.Rows("1"))
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

	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
	se := session.NewSession(tk.Session(), tk.Session(), func(_ session.Session) {})
	m.DoGC(context.TODO(), se, se.Now())
	tk.MustQuery("select job_id from mysql.tidb_ttl_job_history order by job_id asc").Check(testkit.Rows("1", "2", "3", "4", "5"))
}

func TestJobMetrics(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, dom)

	waitAndStopTTLManager(t, dom)

	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	m := ttlworker.NewJobManager("manager-1", dom.SysSessionPool(), store, nil, func() bool {
		return true
	})
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	// submit job
	require.NoError(t, m.SubmitJob(se, table.Meta().ID, table.Meta().ID, "request1"))
	// set the worker to be empty, so none of the tasks will be scheduled
	m.TaskManager().ResizeWorkersToZero(t)

	sql, args := cache.SelectFromTTLTableStatusWithID(table.Meta().ID)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	require.NoError(t, err)
	tableStatus, err := cache.RowToTableStatus(se, rows[0])
	require.NoError(t, err)

	require.NotEmpty(t, tableStatus.CurrentJobID)
	require.Equal(t, "manager-1", tableStatus.CurrentJobOwnerID)
	require.Equal(t, cache.JobStatusRunning, tableStatus.CurrentJobStatus)

	m.ReportMetrics(se)
	out := &dto.Metric{}
	require.NoError(t, metrics.RunningJobsCnt.Write(out))
	require.Equal(t, float64(1), out.GetGauge().GetValue())
}

func TestDelayMetrics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// disable ttl job to make test stable
	tk.MustExec("set @@global.tidb_ttl_job_enable=0")
	tk.MustExec("use test")
	tk.MustExec("create table t1(t timestamp) TTL=`t`+interval 1 day TTL_JOB_INTERVAL='1h'")
	tk.MustExec("create table t2(t timestamp) TTL=`t`+interval 1 day TTL_JOB_INTERVAL='1h'")
	tk.MustExec("create table t3(t timestamp) TTL=`t`+interval 1 day TTL_JOB_INTERVAL='1h'")
	tk.MustExec("create table t4(t timestamp) TTL=`t`+interval 1 day TTL_JOB_INTERVAL='2h'")
	tk.MustExec("create table t5(t timestamp) TTL=`t`+interval 1 day TTL_JOB_INTERVAL='2h'")
	tk.MustExec("create table tx(t timestamp)")
	rows := tk.MustQuery("select table_name, tidb_table_id, cast(unix_timestamp(create_time) as signed) from information_schema.tables where TABLE_SCHEMA='test'").Rows()
	tableInfos := make(map[string]struct {
		id         int64
		createTime time.Time
	})
	for _, row := range rows {
		name := row[0].(string)
		id, err := strconv.ParseInt(row[1].(string), 10, 64)
		require.NoError(t, err)
		ts, err := strconv.ParseInt(row[2].(string), 10, 64)
		require.NoError(t, err)
		tableInfos[name] = struct {
			id         int64
			createTime time.Time
		}{id: id, createTime: time.Unix(ts, 0)}
	}

	now := time.Unix(time.Now().Add(time.Minute).Unix(), 0)

	insertHistory := func(tblName string, jobStart time.Time, running bool, err bool) {
		tblInfo, ok := tableInfos[tblName]
		require.True(t, ok)
		status := "finished"
		if running {
			status = "running"
		}

		summaryText := `{"total_rows":100,"success_rows":100,"error_rows":0,"total_scan_task":1,"scheduled_scan_task":1,"finished_scan_task":1}`
		if err {
			summaryText = `{"scan_task_err": "err1", "total_rows":100,"success_rows":100,"error_rows":0,"total_scan_task":1,"scheduled_scan_task":1,"finished_scan_task":1}`
		}

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
			 	'%s', %d, %d, 'test', '%s', '',
			 	from_unixtime(%d),
			 	from_unixtime(%d),
			 	from_unixtime(%d),
			 	'%s', 100, 100, 0, '%s'
		)`,
			uuid.NewString(), tblInfo.id, tblInfo.id, tblName,
			jobStart.Unix(),
			jobStart.Unix()+int64(time.Minute.Seconds()),
			jobStart.Unix()-int64(time.Hour.Seconds()),
			summaryText,
			status,
		))
	}

	var emptyTime time.Time
	checkRecord := func(records map[int64]*metrics.DelayMetricsRecord, name string, jobStartTime time.Time) {
		info, ok := tableInfos[name]
		require.True(t, ok)
		record, ok := records[info.id]
		require.True(t, ok)
		require.Equal(t, jobStartTime, record.LastJobTime)

		absoluteDelay := now.Sub(jobStartTime)
		if jobStartTime == emptyTime {
			absoluteDelay = now.Sub(info.createTime)
		}
		require.Equal(t, absoluteDelay, record.AbsoluteDelay)

		relativeDelay := absoluteDelay - time.Hour
		if jobStartTime == emptyTime {
			relativeDelay = absoluteDelay
		} else if name == "t4" {
			relativeDelay = absoluteDelay - 2*time.Hour
		}

		if relativeDelay < 0 {
			relativeDelay = 0
		}

		require.Equal(t, relativeDelay, record.ScheduleRelativeDelay)
	}

	insertHistory("t1", now, false, false)
	insertHistory("t1", now.Add(-time.Hour), false, false)
	insertHistory("t2", now.Add(-time.Hour), false, false)
	insertHistory("t2", now.Add(-2*time.Hour), false, false)
	insertHistory("t3", now.Add(-3*time.Hour), false, false)
	insertHistory("t3", now.Add(-time.Hour), true, false)
	insertHistory("t4", now.Add(-3*time.Hour), false, false)
	insertHistory("t4", now.Add(-time.Hour), false, true)

	se := session.NewSession(tk.Session(), tk.Session(), func(s session.Session) {})
	records, err := ttlworker.GetDelayMetricRecords(context.Background(), se, now)
	require.NoError(t, err)
	require.Equal(t, 5, len(records))
	checkRecord(records, "t1", now)
	checkRecord(records, "t2", now.Add(-time.Hour))
	checkRecord(records, "t3", now.Add(-3*time.Hour))
	checkRecord(records, "t4", now.Add(-3*time.Hour))
	checkRecord(records, "t5", emptyTime)
}

type poolTestWrapper struct {
	util.SessionPool
	inuse atomic.Int64
}

func wrapPoolForTest(pool util.SessionPool) *poolTestWrapper {
	return &poolTestWrapper{SessionPool: pool}
}

func (w *poolTestWrapper) Get() (pools.Resource, error) {
	r, err := w.SessionPool.Get()
	if err == nil {
		w.inuse.Add(1)
	}
	return r, err
}

func (w *poolTestWrapper) Put(r pools.Resource) {
	w.inuse.Add(-1)
	w.SessionPool.Put(r)
}

func (w *poolTestWrapper) AssertNoSessionInUse(t *testing.T) {
	require.Zero(t, w.inuse.Load())
}

func TestManagerJobAdapterCanSubmitJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	pool := wrapPoolForTest(dom.SysSessionPool())
	defer pool.AssertNoSessionInUse(t)
	adapter := ttlworker.NewManagerJobAdapter(store, pool, nil)

	// stop TTLJobManager to avoid unnecessary job schedule and make test stable
	dom.TTLJobManager().Stop()
	require.NoError(t, dom.TTLJobManager().WaitStopped(context.Background(), time.Minute))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// no table
	require.False(t, adapter.CanSubmitJob(9999, 9999))

	// not ttl table
	tk.MustExec("create table t1(t timestamp)")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	// ttl table
	tk.MustExec("create table ttl1(t timestamp) TTL=`t`+interval 1 DAY")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl1"))
	require.NoError(t, err)
	require.True(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	// ttl table but disabled
	tk.MustExec("create table ttl2(t timestamp) TTL=`t`+interval 1 DAY TTL_ENABLE='OFF'")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl2"))
	require.NoError(t, err)
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	// ttl partition table
	tk.MustExec("create table ttlp1(a int, t timestamp) TTL=`t`+interval 1 DAY PARTITION BY RANGE (a) (" +
		"PARTITION p0 VALUES LESS THAN (10)," +
		"PARTITION p1 VALUES LESS THAN (100)" +
		")")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttlp1"))
	require.NoError(t, err)
	for _, def := range tbl.Meta().Partition.Definitions {
		require.True(t, adapter.CanSubmitJob(tbl.Meta().ID, def.ID))
	}

	// limit max running tasks
	tk.MustExec("set @@global.tidb_ttl_running_tasks=8")
	defer tk.MustExec("set @@global.tidb_ttl_running_tasks=-1")
	for i := 1; i <= 16; i++ {
		jobID := strconv.Itoa(i)
		sql, args, err := cache.InsertIntoTTLTask(tk.Session(), jobID, int64(1000+i), i, nil, nil, time.Now(), time.Now())
		require.NoError(t, err)
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
		_, err = tk.Session().ExecuteInternal(ctx, sql, args...)
		require.NoError(t, err)

		if i <= 4 {
			// tasks 1 - 4 are running and 5 - 7 are waiting
			tk.MustExec("update mysql.tidb_ttl_task set status='running' where job_id=?", jobID)
		}

		if i > 7 {
			// tasks after 8 are finished
			tk.MustExec("update mysql.tidb_ttl_task set status='finished' where job_id=?", jobID)
		}
	}
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl1"))
	require.NoError(t, err)
	require.True(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))
	tk.MustExec("update mysql.tidb_ttl_task set status='running' where job_id='8'")
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))
}

func TestManagerJobAdapterSubmitJob(t *testing.T) {
	ch := make(chan *ttlworker.SubmitTTLManagerJobRequest)
	adapter := ttlworker.NewManagerJobAdapter(nil, nil, ch)

	var reqPointer atomic.Pointer[ttlworker.SubmitTTLManagerJobRequest]
	responseRequest := func(err error) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()
		select {
		case <-ctx.Done():
			require.FailNow(t, "timeout")
		case req, ok := <-ch:
			require.True(t, ok)
			reqPointer.Store(req)
			select {
			case req.RespCh <- err:
			default:
				require.FailNow(t, "blocked")
			}
		}
	}

	// normal submit
	go responseRequest(nil)
	job, err := adapter.SubmitJob(context.TODO(), 1, 2, "req1", time.Now())
	require.NoError(t, err)
	require.Equal(t, "req1", job.RequestID)
	require.False(t, job.Finished)
	require.Nil(t, job.Summary)
	req := reqPointer.Load()
	require.NotNil(t, req)
	require.Equal(t, int64(1), req.TableID)
	require.Equal(t, int64(2), req.PhysicalID)
	require.Equal(t, "req1", req.RequestID)

	// submit but reply error
	go responseRequest(errors.New("mockErr"))
	job, err = adapter.SubmitJob(context.TODO(), 1, 2, "req1", time.Now())
	require.EqualError(t, err, "mockErr")
	require.Nil(t, job)

	// context timeout when send request
	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	job, err = adapter.SubmitJob(ctx, 1, 2, "req1", time.Now())
	require.Same(t, err, ctx.Err())
	require.Nil(t, job)

	// context timeout when waiting response
	ch = make(chan *ttlworker.SubmitTTLManagerJobRequest, 1)
	adapter = ttlworker.NewManagerJobAdapter(nil, nil, ch)
	ctx, cancel = context.WithTimeout(context.TODO(), 100*time.Millisecond)
	defer cancel()
	job, err = adapter.SubmitJob(ctx, 1, 2, "req1", time.Now())
	require.EqualError(t, err, ctx.Err().Error())
	require.Nil(t, job)
}

func TestManagerJobAdapterGetJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	pool := wrapPoolForTest(dom.SysSessionPool())
	defer pool.AssertNoSessionInUse(t)
	adapter := ttlworker.NewManagerJobAdapter(store, pool, nil)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	summary := ttlworker.TTLSummary{
		TotalRows:         1000,
		SuccessRows:       998,
		ErrorRows:         2,
		TotalScanTask:     10,
		ScheduledScanTask: 9,
		FinishedScanTask:  8,
		ScanTaskErr:       "err1",
	}

	summaryText, err := json.Marshal(summary)
	require.NoError(t, err)

	insertJob := func(tableID, physicalID int64, jobID string, status cache.JobStatus) {
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
			 	'%s', %d, %d, 'test', '%s', '', now() - interval 1 MINUTE, now(), now() - interval 1 DAY,
			 	'%s', %d, %d, %d, '%s'
		)`,
			jobID, physicalID, tableID, "t1", summaryText, summary.TotalRows, summary.SuccessRows, summary.ErrorRows, status,
		))
	}

	// job not exists
	job, err := adapter.GetJob(context.TODO(), 1, 2, "req1")
	require.NoError(t, err)
	require.Nil(t, job)

	// job table id not match
	insertJob(2, 2, "req1", cache.JobStatusFinished)
	require.NoError(t, err)
	require.Nil(t, job)
	tk.MustExec("delete from mysql.tidb_ttl_job_history")

	// job physical id not match
	insertJob(1, 3, "req1", cache.JobStatusFinished)
	require.NoError(t, err)
	require.Nil(t, job)
	tk.MustExec("delete from mysql.tidb_ttl_job_history")

	// job request id not match
	insertJob(1, 2, "req2", cache.JobStatusFinished)
	require.NoError(t, err)
	require.Nil(t, job)
	tk.MustExec("delete from mysql.tidb_ttl_job_history")

	// job exists with status
	statusList := []cache.JobStatus{
		cache.JobStatusWaiting,
		cache.JobStatusRunning,
		cache.JobStatusCancelling,
		cache.JobStatusCancelled,
		cache.JobStatusTimeout,
		cache.JobStatusFinished,
	}
	for _, status := range statusList {
		insertJob(1, 2, "req1", status)
		job, err = adapter.GetJob(context.TODO(), 1, 2, "req1")
		require.NoError(t, err, status)
		require.NotNil(t, job, status)
		require.Equal(t, "req1", job.RequestID, status)
		switch status {
		case cache.JobStatusTimeout, cache.JobStatusFinished, cache.JobStatusCancelled:
			require.True(t, job.Finished, status)
		default:
			require.False(t, job.Finished, status)
		}
		require.Equal(t, summary, *job.Summary, status)
		tk.MustExec("delete from mysql.tidb_ttl_job_history")
	}
}

func TestManagerJobAdapterNow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	pool := wrapPoolForTest(dom.SysSessionPool())
	defer pool.AssertNoSessionInUse(t)
	adapter := ttlworker.NewManagerJobAdapter(store, pool, nil)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.time_zone ='Europe/Berlin'")
	tk.MustExec("set @@time_zone='Asia/Shanghai'")

	now, err := adapter.Now()
	require.NoError(t, err)
	localNow := time.Now()

	require.Equal(t, "Europe/Berlin", now.Location().String())
	require.InDelta(t, now.Unix(), localNow.Unix(), 10)
}

func TestFinishAndUpdateOwnerAtSameTime(t *testing.T) {
	// Finishing a `TTLJob` will remove all the `TTLTask` of the job, and at the same time
	// the `task_manager` may update the owner of the task, which may cause a write conflict.
	// This test is to simulate this scenario.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, dom)
	se := sessionFactory()

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR")
	testTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	now := se.Now().Add(time.Hour * 48)
	m := ttlworker.NewJobManager("test-ttl-job-manager", nil, store, nil, nil)
	require.NoError(t, m.InfoSchemaCache().Update(se))

	job, err := m.LockJob(context.Background(), se, m.InfoSchemaCache().Tables[testTable.Meta().ID], now, uuid.NewString(), false)
	require.NoError(t, err)
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))

	continueFinish := make(chan struct{})
	doneFinish := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ttl/ttlworker/ttl-before-remove-task-in-finish", func() {
		<-continueFinish
	})

	go func() {
		defer close(doneFinish)
		finishSe := sessionFactory()
		require.NoError(t, job.Finish(finishSe, finishSe.Now(), &ttlworker.TTLSummary{}))
	}()

	_, err = m.TaskManager().LockScanTask(se, &cache.TTLTask{
		ScanID:  0,
		JobID:   job.ID(),
		TableID: testTable.Meta().ID,
	}, now)
	require.NoError(t, err)
	close(continueFinish)
	<-doneFinish

	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))
}

func TestFinishError(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, dom)
	se := sessionFactory()

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR")
	testTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	errCount := 5
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ttl/ttlworker/ttl-finish", func(err *error) {
		errCount -= 1
		if errCount > 0 {
			*err = errors.New("mock error")
		}
	})

	now := se.Now()

	m := ttlworker.NewJobManager("test-ttl-job-manager", nil, store, nil, nil)
	require.NoError(t, m.InfoSchemaCache().Update(se))

	initializeTest := func() {
		errCount = 5
		now = now.Add(time.Hour * 48)
		job, err := m.LockJob(context.Background(), se, m.InfoSchemaCache().Tables[testTable.Meta().ID], now, uuid.NewString(), false)
		require.NoError(t, err)
		tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))
		task, err := m.TaskManager().LockScanTask(se, &cache.TTLTask{
			ScanID:  0,
			JobID:   job.ID(),
			TableID: testTable.Meta().ID,
		}, now)
		require.NoError(t, err)
		task.SetResult(nil)
		err = m.TaskManager().ReportTaskFinished(se, now, task)
		require.NoError(t, err)
		tk.MustQuery("select status from mysql.tidb_ttl_task").Check(testkit.Rows("finished"))
	}

	// Test the `CheckFinishedJob` can tolerate the `job.finish` error
	initializeTest()
	for i := 0; i < 4; i++ {
		m.CheckFinishedJob(se)
		tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))
	}
	m.CheckFinishedJob(se)
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))

	// Test the `rescheduleJobs` can tolerate the `job.finish` error
	// cancel job branch
	initializeTest()
	variable.EnableTTLJob.Store(false)
	t.Cleanup(func() {
		variable.EnableTTLJob.Store(true)
	})
	for i := 0; i < 4; i++ {
		m.RescheduleJobs(se, now)
		tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))
	}
	m.RescheduleJobs(se, now)
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))
	variable.EnableTTLJob.Store(true)
	// remove table branch
	initializeTest()
	tk.MustExec("drop table t")
	require.NoError(t, m.InfoSchemaCache().Update(se))
	for i := 0; i < 4; i++ {
		m.RescheduleJobs(se, now)
		tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))
	}
	m.RescheduleJobs(se, now)
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))
	tk.MustExec("CREATE TABLE t (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR")
	require.NoError(t, m.InfoSchemaCache().Update(se))
	testTable, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	// Teset the `updateHeartBeat` can tolerate the `job.finish` error
	initializeTest()
	for i := 0; i < 4; i++ {
		// timeout is 6h
		now = now.Add(time.Hour * 8)
		m.UpdateHeartBeat(context.Background(), se, now)
		tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("1"))
	}
	m.UpdateHeartBeat(context.Background(), se, now)
	tk.MustQuery("select count(*) from mysql.tidb_ttl_task").Check(testkit.Rows("0"))
}

func boostJobScheduleForTest(t *testing.T) func() {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/check-job-triggered-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/sync-timer", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/check-job-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/resize-workers-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-status-table-cache-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-info-schema-cache-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/check-task-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/gc-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))

	return func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/check-job-triggered-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/sync-timer"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/check-job-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/resize-workers-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-status-table-cache-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-info-schema-cache-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/check-task-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/gc-interval"))
	}
}

func TestDisableTTLAfterLoseHeartbeat(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, dom)
	se := sessionFactory()

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR")
	testTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	ctx := context.Background()
	m1 := ttlworker.NewJobManager("test-ttl-job-manager-1", nil, store, nil, nil)
	m2 := ttlworker.NewJobManager("test-ttl-job-manager-2", nil, store, nil, nil)

	now := se.Now()

	acquireJob := func(now time.Time) {
		require.NoError(t, m1.InfoSchemaCache().Update(se))
		require.NoError(t, m1.TableStatusCache().Update(ctx, se))
		require.NoError(t, m2.InfoSchemaCache().Update(se))
		require.NoError(t, m2.TableStatusCache().Update(ctx, se))
		_, err = m1.LockJob(context.Background(), se, m1.InfoSchemaCache().Tables[testTable.Meta().ID], now, uuid.NewString(), false)
		require.NoError(t, err)
		tk.MustQuery("select current_job_status from mysql.tidb_ttl_table_status").Check(testkit.Rows("running"))
	}
	t.Run("disable TTL globally after losing heartbeat", func(t *testing.T) {
		// now the TTL job should be scheduled again
		now = now.Add(time.Hour * 8)
		acquireJob(now)

		// lose heartbeat. Simulate the situation that m1 doesn't update the hearbeat for 8 hours.
		now = now.Add(time.Hour * 8)

		// stop the tidb_ttl_job_enable
		tk.MustExec("set global tidb_ttl_job_enable = 'OFF'")
		defer tk.MustExec("set global tidb_ttl_job_enable = 'ON'")

		// reschedule and try to get the job
		require.NoError(t, m2.InfoSchemaCache().Update(se))
		require.NoError(t, m2.TableStatusCache().Update(ctx, se))
		m2.RescheduleJobs(se, now)

		// the job should have been cancelled
		tk.MustQuery("select current_job_status from mysql.tidb_ttl_table_status").Check(testkit.Rows("<nil>"))
	})

	t.Run("disable TTL for a table after losing heartbeat", func(t *testing.T) {
		// now the TTL job should be scheduled again
		now = now.Add(time.Hour * 8)
		acquireJob(now)

		// lose heartbeat. Simulate the situation that m1 doesn't update the hearbeat for 8 hours.
		now = now.Add(time.Hour * 8)

		tk.MustExec("ALTER TABLE t TTL_ENABLE = 'OFF'")
		defer tk.MustExec("ALTER TABLE t TTL_ENABLE = 'ON'")

		// reschedule and try to get the job
		require.NoError(t, m2.InfoSchemaCache().Update(se))
		require.NoError(t, m2.TableStatusCache().Update(ctx, se))
		m2.RescheduleJobs(se, now)

		// the job cannot be cancelled, because it doesn't exist in the infoschema cache.
		tk.MustQuery("select current_job_status from mysql.tidb_ttl_table_status").Check(testkit.Rows("running"))

		// run GC
		m2.DoGC(ctx, se, now)

		// the job should have been cancelled
		tk.MustQuery("select current_job_status, current_job_owner_hb_time from mysql.tidb_ttl_table_status").Check(testkit.Rows())
	})

	t.Run("drop a TTL table after losing heartbeat", func(t *testing.T) {
		// now the TTL job should be scheduled again
		now = now.Add(time.Hour * 8)
		acquireJob(now)

		// lose heartbeat. Simulate the situation that m1 doesn't update the hearbeat for 8 hours.
		now = now.Add(time.Hour * 8)

		tk.MustExec("DROP TABLE t")
		defer func() {
			tk.MustExec("CREATE TABLE t (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR")
			testTable, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
		}()

		// reschedule and try to get the job
		require.NoError(t, m2.InfoSchemaCache().Update(se))
		require.NoError(t, m2.TableStatusCache().Update(ctx, se))
		m2.RescheduleJobs(se, now)

		// the job cannot be cancelled, because it doesn't exist in the infoschema cache.
		tk.MustQuery("select current_job_status from mysql.tidb_ttl_table_status").Check(testkit.Rows("running"))

		// run GC
		m2.DoGC(ctx, se, now)

		// the job should have been cancelled
		tk.MustQuery("select current_job_status, current_job_owner_hb_time  from mysql.tidb_ttl_table_status").Check(testkit.Rows())
	})
}

func TestJobHeartBeatFailNotBlockOthers(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, dom)
	se := sessionFactory()

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR")
	tk.MustExec("CREATE TABLE t2 (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR")
	testTable1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	testTable2, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)

	ctx := context.Background()
	m := ttlworker.NewJobManager("test-ttl-job-manager-1", nil, store, nil, nil)

	now := se.Now()
	// acquire two jobs
	require.NoError(t, m.InfoSchemaCache().Update(se))
	require.NoError(t, m.TableStatusCache().Update(ctx, se))
	_, err = m.LockJob(context.Background(), se, m.InfoSchemaCache().Tables[testTable1.Meta().ID], now, uuid.NewString(), false)
	require.NoError(t, err)
	_, err = m.LockJob(context.Background(), se, m.InfoSchemaCache().Tables[testTable2.Meta().ID], now, uuid.NewString(), false)
	require.NoError(t, err)
	tk.MustQuery("select current_job_status from mysql.tidb_ttl_table_status").Check(testkit.Rows("running", "running"))

	// assign the first job to another manager
	tk.MustExec("update mysql.tidb_ttl_table_status set current_job_owner_id = 'test-ttl-job-manager-2' where table_id = ?", testTable1.Meta().ID)
	// the heartbeat of the first job will fail, but the second one will still success
	now = now.Add(time.Hour)
	require.Error(t, m.UpdateHeartBeatForJob(context.Background(), se, now, m.RunningJobs()[0]))
	require.NoError(t, m.UpdateHeartBeatForJob(context.Background(), se, now, m.RunningJobs()[1]))

	now = now.Add(time.Hour)
	m.UpdateHeartBeat(ctx, se, now)
	tkTZ := tk.Session().GetSessionVars().Location()
	tk.MustQuery("select table_id, current_job_owner_hb_time from mysql.tidb_ttl_table_status").Sort().Check(testkit.Rows(
		fmt.Sprintf("%d %s", testTable1.Meta().ID, now.Add(-2*time.Hour).In(tkTZ).Format(time.DateTime)),
		fmt.Sprintf("%d %s", testTable2.Meta().ID, now.In(tkTZ).Format(time.DateTime))))
}

var _ fault = &faultWithProbability{}

type faultWithProbability struct {
	percent float64
}

func (f *faultWithProbability) shouldFault(sql string) bool {
	return rand.Float64() < f.percent
}

func newFaultWithProbability(percent float64) *faultWithProbability {
	return &faultWithProbability{percent: percent}
}

func accelerateHeartBeat(t *testing.T, tk *testkit.TestKit) func() {
	tk.MustExec("ALTER TABLE mysql.tidb_ttl_table_status MODIFY COLUMN current_job_owner_hb_time TIMESTAMP(6)")
	tk.MustExec("ALTER TABLE mysql.tidb_ttl_task MODIFY COLUMN owner_hb_time TIMESTAMP(6)")
	ttlworker.SetTimeFormat(time.DateTime + ".999999")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/heartbeat-interval", fmt.Sprintf("return(%d)", time.Millisecond*100)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-heartbeat-interval", fmt.Sprintf("return(%d)", time.Millisecond*100)))
	return func() {
		tk.MustExec("ALTER TABLE mysql.tidb_ttl_table_status MODIFY COLUMN current_job_owner_hb_time TIMESTAMP")
		tk.MustExec("ALTER TABLE mysql.tidb_ttl_task MODIFY COLUMN owner_hb_time TIMESTAMP")
		ttlworker.SetTimeFormat(time.DateTime)

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/heartbeat-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-heartbeat-interval"))
	}
}

func overwriteJobInterval(t *testing.T) func() {
	// TODO: it sounds better to explicitly support 'ms' unit for test in the parser. The only issue is that we need to port a new `intest`
	// pkg to the `parser` pkg, as it cannot depend on the `tidb` root pkg.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/meta/model/overwrite-ttl-job-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/timer/api/overwrite-ttl-job-interval", fmt.Sprintf("return(%d)", 100*time.Millisecond)))

	return func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/meta/model/overwrite-ttl-job-interval"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/timer/api/overwrite-ttl-job-interval"))
	}
}

func TestJobManagerWithFault(t *testing.T) {
	// TODO: add a flag `-long` to enable this test
	skip.NotUnderLong(t)

	defer boostJobScheduleForTest(t)()
	defer overwriteJobInterval(t)()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)

	tk := testkit.NewTestKit(t, store)
	defer accelerateHeartBeat(t, tk)()
	tk.MustExec("set @@global.tidb_ttl_running_tasks=32")

	// don't run too many manager, or the test will be unstable
	managerCount := runtime.GOMAXPROCS(0) * 2
	testDuration := 10 * time.Minute
	faultPercent := 0.5

	leader := atomic.NewString("")
	isLeaderFactory := func(id string) func() bool {
		return func() bool {
			return leader.Load() == id
		}
	}

	type managerWithPool struct {
		m    *ttlworker.JobManager
		pool util.SessionPool
	}
	managers := make([]managerWithPool, 0, managerCount)
	for i := 0; i < managerCount; i++ {
		pool := wrapPoolForTest(dom.SysSessionPool())
		faultPool := newFaultSessionPool(pool)

		id := fmt.Sprintf("test-ttl-job-manager-%d", i)
		m := ttlworker.NewJobManager(id, faultPool, store, nil, isLeaderFactory(id))
		managers = append(managers, managerWithPool{
			m:    m,
			pool: faultPool,
		})

		m.Start()
	}

	stopTestCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(1)

	fault := newFaultWithFilter(func(sql string) bool {
		// skip some local only sql, ref `getSession()` in `session.go`
		if strings.HasPrefix(sql, "set tidb_") || strings.HasPrefix(sql, "set @@") ||
			strings.ToUpper(sql) == "COMMIT" || strings.ToUpper(sql) == "ROLLBACK" {
			return false
		}

		return true
	}, newFaultWithProbability(faultPercent))
	go func() {
		defer wg.Done()

		faultTicker := time.NewTicker(time.Second)
		for {
			select {
			case <-stopTestCh:
				// Recover all sessions
				for _, m := range managers {
					m.pool.(*faultSessionPool).setFault(nil)
				}

				return
			case <-faultTicker.C:
				// Recover all sessions
				for _, m := range managers {
					m.pool.(*faultSessionPool).setFault(nil)
				}

				faultCount := rand.Int() % managerCount
				logutil.BgLogger().Info("inject fault", zap.Int("faultCount", faultCount))
				rand.Shuffle(managerCount, func(i, j int) {
					managers[i], managers[j] = managers[j], managers[i]
				})
				// the first non-faultt manager is the leader
				leader.Store(managers[faultCount].m.ID())
				logutil.BgLogger().Info("set leader", zap.String("leader", leader.Load()))
				for i := 0; i < faultCount; i++ {
					m := managers[i]
					logutil.BgLogger().Info("inject fault", zap.String("id", m.m.ID()))
					m.pool.(*faultSessionPool).setFault(fault)
				}
			}
		}
	}()

	// run the workload goroutine
	testStart := time.Now()
	for time.Since(testStart) < testDuration {
		// create a new table
		tk.MustExec("use test")
		tk.MustExec("DROP TABLE if exists t")
		tk.MustExec("CREATE TABLE t (id INT PRIMARY KEY, created_at DATETIME) TTL = created_at + INTERVAL 1 HOUR TTL_ENABLE='OFF'")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		logutil.BgLogger().Info("create table", zap.Int64("table_id", tbl.Meta().ID))

		// insert some data
		for i := 0; i < 5; i++ {
			tk.MustExec(fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i, time.Now().Add(-time.Hour*2).Format(time.DateTime)))
		}
		for i := 0; i < 5; i++ {
			tk.MustExec(fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i+5, time.Now().Format(time.DateTime)))
		}

		tk.MustExec("ALTER TABLE t TTL_ENABLE='ON'")

		start := time.Now()
		require.Eventually(t, func() bool {
			rows := tk.MustQuery("SELECT COUNT(*) FROM t").Rows()
			if len(rows) == 1 && rows[0][0].(string) == "5" {
				return true
			}

			logutil.BgLogger().Info("get row count", zap.String("count", rows[0][0].(string)))

			tableStatus := tk.MustQuery("SELECT * FROM mysql.tidb_ttl_table_status").String()
			logutil.BgLogger().Info("get job state", zap.String("tidb_ttl_table_status", tableStatus))
			return false
		}, time.Second*30, time.Millisecond*100)

		require.Eventually(t, func() bool {
			rows := tk.MustQuery("SELECT current_job_state FROM mysql.tidb_ttl_table_status").Rows()
			if len(rows) == 1 && rows[0][0].(string) == "<nil>" {
				return true
			}

			tableStatus := tk.MustQuery("SELECT * FROM mysql.tidb_ttl_table_status").String()
			logutil.BgLogger().Info("get job state", zap.String("tidb_ttl_table_status", tableStatus))
			return false
		}, time.Second*30, time.Millisecond*100)

		logutil.BgLogger().Info("finish workload", zap.Duration("duration", time.Since(start)))
	}

	logutil.BgLogger().Info("test finished")
	stopTestCh <- struct{}{}
	close(stopTestCh)

	wg.Wait()

	for _, m := range managers {
		m.m.Stop()
		require.NoError(t, m.m.WaitStopped(context.Background(), time.Minute))
	}
}

func TestTimerJobAfterDropTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)

	pool := wrapPoolForTest(dom.SysSessionPool())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (created_at datetime) TTL = created_at + INTERVAL 1 HOUR")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	m := ttlworker.NewJobManager("test-job-manager", pool, store, nil, func() bool { return true })

	se, err := ttlworker.GetSessionForTest(pool)
	require.NoError(t, err)
	defer se.Close()

	// First, schedule the job. The row in the `tidb_ttl_table_status` and `tidb_ttl_job_history` will be created
	jobID := "test-job-id"

	require.NoError(t, m.InfoSchemaCache().Update(se))
	err = m.SubmitJob(se, tbl.Meta().ID, tbl.Meta().ID, jobID)
	require.NoError(t, err)
	now := se.Now()
	tk.MustQuery("select count(*) from mysql.tidb_ttl_table_status").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from mysql.tidb_ttl_job_history").Check(testkit.Rows("1"))

	// Drop the table, then the `m` somehow lost heartbeat for 2*heartbeat interval, and GC TTL jobs
	tk.MustExec("drop table t")

	now = now.Add(time.Hour * 2)
	m.DoGC(context.Background(), se, now)
	tk.MustQuery("select count(*) from mysql.tidb_ttl_table_status").Check(testkit.Rows("0"))
	tk.MustQuery("select status from mysql.tidb_ttl_job_history").Check(testkit.Rows("cancelled"))

	require.NoError(t, m.TableStatusCache().Update(context.Background(), se))
	require.NoError(t, m.InfoSchemaCache().Update(se))
	m.CheckNotOwnJob()
	require.Len(t, m.RunningJobs(), 0)

	// The adapter should not return the job
	adapter := ttlworker.NewManagerJobAdapter(store, pool, nil)
	job, err := adapter.GetJob(context.Background(), tbl.Meta().ID, tbl.Meta().ID, jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.True(t, job.Finished)
}
