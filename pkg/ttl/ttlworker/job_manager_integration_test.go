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

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	dbsession "github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/testkit"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	timertable "github.com/pingcap/tidb/pkg/timer/tablestore"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/client"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util/logutil"
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

func TestGetSession(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@time_zone = 'Asia/Shanghai'")
	tk.MustExec("set @@global.time_zone= 'Europe/Berlin'")
	tk.MustExec("set @@tidb_retry_limit=1")
	tk.MustExec("set @@tidb_enable_1pc=0")
	tk.MustExec("set @@tidb_enable_async_commit=0")
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
	tk.MustQuery("select @@time_zone, @@tidb_retry_limit, @@tidb_enable_1pc, @@tidb_enable_async_commit").
		Check(testkit.Rows("UTC 0 1 1"))

	// all session variables should be restored after close
	se.Close()
	tk.MustQuery("select @@time_zone, @@tidb_retry_limit, @@tidb_enable_1pc, @@tidb_enable_async_commit").
		Check(testkit.Rows("Asia/Shanghai 1 0 0"))
}

func TestParallelLockNewJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)

	sessionFactory := sessionFactory(t, store)

	testTable := &cache.PhysicalTable{ID: 2, TableInfo: &model.TableInfo{ID: 1, TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay), JobInterval: "1h"}}}
	// simply lock a new job
	m := ttlworker.NewJobManager("test-id", nil, store, nil, nil)
	m.InfoSchemaCache().Tables[testTable.ID] = testTable

	se := sessionFactory()
	job, err := m.LockJob(context.Background(), se, testTable, se.Now(), uuid.NewString(), false)
	require.NoError(t, err)
	job.Finish(se, se.Now(), &ttlworker.TTLSummary{})

	// lock one table in parallel, only one of them should lock successfully
	testTimes := 100
	concurrency := 5
	now := se.Now()
	for i := 0; i < testTimes; i++ {
		successCounter := atomic.NewUint64(0)
		successJob := &ttlworker.TTLJob{}

		now = now.Add(time.Hour * 48)

		wg := sync.WaitGroup{}
		for j := 0; j < concurrency; j++ {
			jobManagerID := fmt.Sprintf("test-ttl-manager-%d", j)
			wg.Add(1)
			go func() {
				m := ttlworker.NewJobManager(jobManagerID, nil, store, nil, nil)
				m.InfoSchemaCache().Tables[testTable.ID] = testTable

				se := sessionFactory()
				job, err := m.LockJob(context.Background(), se, testTable, now, uuid.NewString(), false)
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
		successJob.Finish(se, se.Now(), &ttlworker.TTLSummary{})
	}
}

func TestFinishJob(t *testing.T) {
	timeFormat := time.DateTime
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	sessionFactory := sessionFactory(t, store)

	testTable := &cache.PhysicalTable{ID: 2, Schema: model.NewCIStr("db1"), TableInfo: &model.TableInfo{ID: 1, Name: model.NewCIStr("t1"), TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay)}}}

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
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-info-schema-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-info-schema-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-status-table-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-status-table-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/resize-workers-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/resize-workers-interval")
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval")

	originAutoAnalyzeMinCnt := exec.AutoAnalyzeMinCnt
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = originAutoAnalyzeMinCnt
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	require.True(t, h.HandleAutoAnalyze())
}

func TestTriggerTTLJob(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval")

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Minute)
	defer cancel()

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, t timestamp) TTL=`t` + INTERVAL 1 DAY")
	tbl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	tblID := tbl.Meta().ID
	require.NoError(t, err)

	timerStore := timertable.NewTableTimerStore(0, do.SysSessionPool(), "mysql", "tidb_timers", nil)
	defer timerStore.Close()
	timerCli := timerapi.NewDefaultTimerClient(timerStore)

	// make sure the table had run a job one time to make the test stable
	cli := do.TTLJobManager().GetCommandCli()
	_, _ = client.TriggerNewTTLJob(ctx, cli, "test", "t")
	r := tk.MustQuery("select last_job_id, current_job_id from mysql.tidb_ttl_table_status where table_id=?", tblID)
	require.Equal(t, 1, len(r.Rows()))
	waitTTLJobFinished(t, tk, tblID, timerCli)

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

	waitTTLJobFinished(t, tk, tblID, timerCli)
	tk.MustQuery("select id from t order by id asc").Check(testkit.Rows("2", "4"))
}

func TestTTLDeleteWithTimeZoneChange(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/task-manager-loop-interval")

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

	waitTTLJobFinished(t, tk, tblID1, timerCli)
	tk.MustQuery("select id from t1 order by id asc").Check(testkit.Rows("1", "2"))

	waitTTLJobFinished(t, tk, tblID2, timerCli)
	tk.MustQuery("select id from t2 order by id asc").Check(testkit.Rows("1"))
}

func waitTTLJobFinished(t *testing.T, tk *testkit.TestKit, tableID int64, timerCli timerapi.TimerClient) {
	start := time.Now()
	for time.Since(start) < time.Minute {
		time.Sleep(time.Second)
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-info-schema-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-info-schema-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-status-table-cache-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/update-status-table-cache-interval")
	failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/resize-workers-interval", fmt.Sprintf("return(%d)", time.Second))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ttl/ttlworker/resize-workers-interval")

	originAutoAnalyzeMinCnt := exec.AutoAnalyzeMinCnt
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = originAutoAnalyzeMinCnt
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

func TestSubmitJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	tk.MustExec("use test")
	tk.MustExec("create table ttlp1(a int, t timestamp) TTL=`t`+interval 1 HOUR PARTITION BY RANGE (a) (" +
		"PARTITION p0 VALUES LESS THAN (10)," +
		"PARTITION p1 VALUES LESS THAN (100)" +
		")")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ttlp1"))
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
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	now := time.Now()
	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
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
	anotherManager := ttlworker.NewJobManager("manager-2", nil, store, nil, nil)
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
	anotherManager.RescheduleJobs(se, time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, now.Nanosecond(), now.Location()))
	tk.MustQuery("select last_job_summary->>'$.scan_task_err' from mysql.tidb_ttl_table_status").Check(testkit.Rows("ttl job is disabled"))
}

func TestRescheduleJobsAfterTableDropped(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	now := time.Now()
	createTableSQL := "create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'"
	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
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
		m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
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
		table, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		m.DoGC(context.TODO(), se)
	}
}

func TestJobTimeout(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	tableID := table.Meta().ID
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	now := se.Now()
	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	require.NoError(t, m.TableStatusCache().Update(context.Background(), se))
	// submit job
	require.NoError(t, m.SubmitJob(se, tableID, tableID, "request1"))
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

	m2 := ttlworker.NewJobManager("manager-2", nil, store, nil, nil)
	m2.TaskManager().ResizeWorkersWithSysVar()
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

	// the timeout will be checked while updating heartbeat
	require.NoError(t, m2.UpdateHeartBeat(ctx, se, now.Add(7*time.Hour)))
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
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID

	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
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
	require.NoError(t, m.SubmitJob(se, tblID, tblID, "request1"))

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

	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
	se := session.NewSession(tk.Session(), tk.Session(), func(_ session.Session) {})
	m.DoGC(context.TODO(), se)
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
	m.DoGC(context.TODO(), se)
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
	m.DoGC(context.TODO(), se)
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
	m.DoGC(context.TODO(), se)
	tk.MustQuery("select job_id from mysql.tidb_ttl_job_history order by job_id asc").Check(testkit.Rows("1", "2", "3", "4", "5"))
}

func TestJobMetrics(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	waitAndStopTTLManager(t, dom)

	tk.MustExec("create table test.t (id int, created_at datetime) ttl = `created_at` + interval 1 minute ttl_job_interval = '1m'")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)

	se := sessionFactory()
	m := ttlworker.NewJobManager("manager-1", nil, store, nil, func() bool {
		return true
	})
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))
	// submit job
	require.NoError(t, m.SubmitJob(se, table.Meta().ID, table.Meta().ID, "request1"))
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

func TestManagerJobAdapterCanSubmitJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	adapter := ttlworker.NewManagerJobAdapter(store, dom.SysSessionPool(), nil)

	// stop TTLJobManager to avoid unnecessary job schedule and make test stable
	dom.TTLJobManager().Stop()
	require.NoError(t, dom.TTLJobManager().WaitStopped(context.Background(), time.Minute))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// no table
	require.False(t, adapter.CanSubmitJob(9999, 9999))

	// not ttl table
	tk.MustExec("create table t1(t timestamp)")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	// ttl table
	tk.MustExec("create table ttl1(t timestamp) TTL=`t`+interval 1 DAY")
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ttl1"))
	require.NoError(t, err)
	require.True(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	// ttl table but disabled
	tk.MustExec("create table ttl2(t timestamp) TTL=`t`+interval 1 DAY TTL_ENABLE='OFF'")
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ttl2"))
	require.NoError(t, err)
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	// ttl partition table
	tk.MustExec("create table ttlp1(a int, t timestamp) TTL=`t`+interval 1 DAY PARTITION BY RANGE (a) (" +
		"PARTITION p0 VALUES LESS THAN (10)," +
		"PARTITION p1 VALUES LESS THAN (100)" +
		")")
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ttlp1"))
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
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ttl1"))
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
	adapter := ttlworker.NewManagerJobAdapter(store, dom.SysSessionPool(), nil)

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
	adapter := ttlworker.NewManagerJobAdapter(store, dom.SysSessionPool(), nil)

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
