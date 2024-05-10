// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util/logutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestParallelLockNewTask(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ttl_running_tasks = 1000")
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
	tk.MustExec("create table test.t (id int, created_at datetime) TTL= created_at + interval 1 hour")
	testTable, err := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	sessionFactory := sessionFactory(t, store)
	se := sessionFactory()

	now := se.Now()

	isc := cache.NewInfoSchemaCache(time.Minute)
	require.NoError(t, isc.Update(se))
	m := ttlworker.NewTaskManager(context.Background(), nil, isc, "test-id", store)

	// insert and lock a new task
	sql, args, err := cache.InsertIntoTTLTask(tk.Session(), "test-job", testTable.Meta().ID, 1, nil, nil, now, now)
	require.NoError(t, err)
	_, err = tk.Session().ExecuteInternal(ctx, sql, args...)
	require.NoError(t, err)
	_, err = m.LockScanTask(se, &cache.TTLTask{
		ScanID:  1,
		JobID:   "test-job",
		TableID: testTable.Meta().ID,
	}, now)
	require.NoError(t, err)
	tk.MustExec("DELETE FROM mysql.tidb_ttl_task")

	// lock one table in parallel, only one of them should lock successfully
	testTimes := 100
	concurrency := 5
	for i := 0; i < testTimes; i++ {
		sql, args, err := cache.InsertIntoTTLTask(tk.Session(), "test-job", testTable.Meta().ID, 1, nil, nil, now, now)
		require.NoError(t, err)
		_, err = tk.Session().ExecuteInternal(ctx, sql, args...)
		require.NoError(t, err)

		successCounter := atomic.NewUint64(0)

		now = now.Add(time.Hour * 48)

		wg := sync.WaitGroup{}
		for j := 0; j < concurrency; j++ {
			scanManagerID := fmt.Sprintf("test-ttl-manager-%d", j)
			wg.Add(1)
			go func() {
				se := sessionFactory()

				isc := cache.NewInfoSchemaCache(time.Minute)
				require.NoError(t, isc.Update(se))
				m := ttlworker.NewTaskManager(context.Background(), nil, isc, scanManagerID, store)

				_, err := m.LockScanTask(se, &cache.TTLTask{
					ScanID:  1,
					JobID:   "test-job",
					TableID: testTable.Meta().ID,
				}, now)
				if err == nil {
					successCounter.Add(1)
				} else {
					logutil.BgLogger().Info("lock new task with error", zap.Error(err))
				}
				wg.Done()
			}()
		}
		wg.Wait()

		require.Equal(t, uint64(1), successCounter.Load())
		tk.MustExec("DELETE FROM mysql.tidb_ttl_task")
	}
}

func TestParallelSchedule(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ttl_running_tasks = 1000")
	sessionFactory := sessionFactory(t, store)

	tk.MustExec("create table test.t(id int, created_at datetime) ttl=created_at + interval 1 day")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	// 16 tasks and 16 scan workers (in 4 task manager) should be able to be scheduled in a single "reschedule"
	for i := 0; i < 16; i++ {
		sql := fmt.Sprintf("insert into mysql.tidb_ttl_task(job_id,table_id,scan_id,expire_time,created_time) values ('test-job', %d, %d, NOW(), NOW())", table.Meta().ID, i)
		tk.MustExec(sql)
	}
	isc := cache.NewInfoSchemaCache(time.Second)
	require.NoError(t, isc.Update(sessionFactory()))
	scheduleWg := sync.WaitGroup{}
	finishTasks := make([]func(), 0, 4)
	for i := 0; i < 4; i++ {
		workers := []ttlworker.Worker{}
		for j := 0; j < 4; j++ {
			scanWorker := ttlworker.NewMockScanWorker(t)
			scanWorker.Start()
			workers = append(workers, scanWorker)
		}

		managerID := fmt.Sprintf("task-manager-%d", i)
		m := ttlworker.NewTaskManager(context.Background(), nil, isc, managerID, store)
		m.SetScanWorkers4Test(workers)
		scheduleWg.Add(1)
		go func() {
			se := sessionFactory()
			m.RescheduleTasks(se, se.Now())
			scheduleWg.Done()
		}()
		finishTasks = append(finishTasks, func() {
			se := sessionFactory()
			for _, task := range m.GetRunningTasks() {
				require.Nil(t, task.Context().Err(), fmt.Sprintf("%s %d", managerID, task.ScanID))
				task.SetResult(nil)
				m.CheckFinishedTask(se, se.Now())
				require.NotNil(t, task.Context().Err(), fmt.Sprintf("%s %d", managerID, task.ScanID))
			}
		})
	}
	scheduleWg.Wait()
	// all tasks should have been scheduled
	tk.MustQuery("select count(1) from mysql.tidb_ttl_task where status = 'running'").Check(testkit.Rows("16"))
	for i := 0; i < 4; i++ {
		sql := fmt.Sprintf("select count(1) from mysql.tidb_ttl_task where status = 'running' AND owner_id = 'task-manager-%d'", i)
		tk.MustQuery(sql).Check(testkit.Rows("4"))
		finishTasks[i]()
		sql = fmt.Sprintf("select count(1) from mysql.tidb_ttl_task where status = 'finished' AND owner_id = 'task-manager-%d'", i)
		tk.MustQuery(sql).Check(testkit.Rows("4"))
	}
}

func TestTaskScheduleExpireHeartBeat(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ttl_running_tasks = 1000")
	sessionFactory := sessionFactory(t, store)

	// create table and scan task
	tk.MustExec("create table test.t(id int, created_at datetime) ttl=created_at + interval 1 day")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	sql := fmt.Sprintf("insert into mysql.tidb_ttl_task(job_id,table_id,scan_id,expire_time,created_time) values ('test-job', %d, %d, NOW(), NOW())", table.Meta().ID, 1)
	tk.MustExec(sql)

	// update the infoschema cache
	isc := cache.NewInfoSchemaCache(time.Second)
	require.NoError(t, isc.Update(sessionFactory()))

	// schedule in a task manager
	scanWorker := ttlworker.NewMockScanWorker(t)
	scanWorker.Start()
	m := ttlworker.NewTaskManager(context.Background(), nil, isc, "task-manager-1", store)
	m.SetScanWorkers4Test([]ttlworker.Worker{scanWorker})
	se := sessionFactory()
	now := se.Now()
	m.RescheduleTasks(se, now)
	tk.MustQuery("select status,owner_id from mysql.tidb_ttl_task").Check(testkit.Rows("running task-manager-1"))

	// another task manager should fetch this task after heartbeat expire
	scanWorker2 := ttlworker.NewMockScanWorker(t)
	scanWorker2.Start()
	m2 := ttlworker.NewTaskManager(context.Background(), nil, isc, "task-manager-2", store)
	m2.SetScanWorkers4Test([]ttlworker.Worker{scanWorker2})
	m2.RescheduleTasks(sessionFactory(), now.Add(time.Hour))
	tk.MustQuery("select status,owner_id from mysql.tidb_ttl_task").Check(testkit.Rows("running task-manager-2"))

	// another task manager shouldn't fetch this task if it has finished
	task := m2.GetRunningTasks()[0]
	task.SetResult(nil)
	m2.CheckFinishedTask(sessionFactory(), now)
	scanWorker3 := ttlworker.NewMockScanWorker(t)
	scanWorker3.Start()
	m3 := ttlworker.NewTaskManager(context.Background(), nil, isc, "task-manager-3", store)
	m3.SetScanWorkers4Test([]ttlworker.Worker{scanWorker3})
	m3.RescheduleTasks(sessionFactory(), now.Add(time.Hour))
	tk.MustQuery("select status,owner_id from mysql.tidb_ttl_task").Check(testkit.Rows("finished task-manager-2"))
}

func TestTaskMetrics(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ttl_running_tasks = 1000")
	sessionFactory := sessionFactory(t, store)

	// create table and scan task
	tk.MustExec("create table test.t(id int, created_at datetime) ttl=created_at + interval 1 day")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	sql := fmt.Sprintf("insert into mysql.tidb_ttl_task(job_id,table_id,scan_id,expire_time,created_time) values ('test-job', %d, %d, NOW(), NOW())", table.Meta().ID, 1)
	tk.MustExec(sql)

	// update the infoschema cache
	isc := cache.NewInfoSchemaCache(time.Second)
	require.NoError(t, isc.Update(sessionFactory()))

	// schedule in a task manager
	scanWorker := ttlworker.NewMockScanWorker(t)
	scanWorker.Start()
	m := ttlworker.NewTaskManager(context.Background(), nil, isc, "task-manager-1", store)
	m.SetScanWorkers4Test([]ttlworker.Worker{scanWorker})
	se := sessionFactory()
	now := se.Now()
	m.RescheduleTasks(sessionFactory(), now)
	tk.MustQuery("select status,owner_id from mysql.tidb_ttl_task").Check(testkit.Rows("running task-manager-1"))

	m.ReportMetrics()
	out := &dto.Metric{}
	require.NoError(t, metrics.DeletingTaskCnt.Write(out))
	require.Equal(t, float64(1), out.GetGauge().GetValue())
}

func TestRescheduleWithError(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ttl_running_tasks = 1000")

	sessionFactory := sessionFactory(t, store)
	// insert a wrong scan task with random table id
	sql := fmt.Sprintf("insert into mysql.tidb_ttl_task(job_id,table_id,scan_id,expire_time,created_time) values ('test-job', %d, %d, NOW(), NOW())", 613, 1)
	tk.MustExec(sql)

	se := sessionFactory()
	now := se.Now()
	isc := cache.NewInfoSchemaCache(time.Second)
	require.NoError(t, isc.Update(se))

	// schedule in a task manager
	scanWorker := ttlworker.NewMockScanWorker(t)
	scanWorker.Start()
	m := ttlworker.NewTaskManager(context.Background(), nil, isc, "task-manager-1", store)
	m.SetScanWorkers4Test([]ttlworker.Worker{scanWorker})
	notify := make(chan struct{})
	go func() {
		m.RescheduleTasks(sessionFactory(), now)
		notify <- struct{}{}
	}()
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	select {
	case <-timeout.Done():
		require.Fail(t, "reschedule didn't finish in time")
	case <-notify:
	}
	tk.MustQuery("select status from mysql.tidb_ttl_task").Check(testkit.Rows("waiting"))
}

func TestTTLRunningTasksLimitation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)
	sessionFactory := sessionFactory(t, store)

	tk.MustExec("set global tidb_ttl_running_tasks = 32")
	tk.MustExec("create table test.t(id int, created_at datetime) ttl=created_at + interval 1 day")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	// 64 tasks and 128 scan workers (in 16 task manager) should only schedule 32 tasks
	for i := 0; i < 128; i++ {
		sql := fmt.Sprintf("insert into mysql.tidb_ttl_task(job_id,table_id,scan_id,expire_time,created_time) values ('test-job', %d, %d, NOW(), NOW())", table.Meta().ID, i)
		tk.MustExec(sql)
	}
	isc := cache.NewInfoSchemaCache(time.Second)
	require.NoError(t, isc.Update(sessionFactory()))
	scheduleWg := sync.WaitGroup{}
	for i := 0; i < 16; i++ {
		workers := []ttlworker.Worker{}
		for j := 0; j < 8; j++ {
			scanWorker := ttlworker.NewMockScanWorker(t)
			scanWorker.Start()
			workers = append(workers, scanWorker)
		}

		ctx := logutil.WithKeyValue(context.Background(), "ttl-worker-test", fmt.Sprintf("task-manager-%d", i))
		m := ttlworker.NewTaskManager(ctx, nil, isc, fmt.Sprintf("task-manager-%d", i), store)
		m.SetScanWorkers4Test(workers)
		scheduleWg.Add(1)
		go func() {
			se := sessionFactory()
			m.RescheduleTasks(se, se.Now())
			scheduleWg.Done()
		}()
	}
	scheduleWg.Wait()
	// all tasks should have been scheduled
	tk.MustQuery("select count(1) from mysql.tidb_ttl_task where status = 'running'").Check(testkit.Rows("32"))
}

func TestMeetTTLRunningTasks(t *testing.T) {
	// initialize a cluster with 3 TiKV
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithStoreType(mockstore.MockTiKV),
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiStores(c, 3)
		}))
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)

	// -1, the default value, means the count of TiKV
	require.True(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(2, cache.TaskStatusWaiting))
	require.False(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(3, cache.TaskStatusWaiting))
	require.True(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(2, cache.TaskStatusRunning))

	// positive number means the limitation
	tk.MustExec("set global tidb_ttl_running_tasks = 32")
	require.False(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(32, cache.TaskStatusWaiting))
	require.True(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(31, cache.TaskStatusWaiting))
	require.True(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(32, cache.TaskStatusRunning))

	// set it back to auto value
	tk.MustExec("set global tidb_ttl_running_tasks = -1")
	require.True(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(2, cache.TaskStatusWaiting))
	require.False(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(3, cache.TaskStatusWaiting))
	require.True(t, dom.TTLJobManager().TaskManager().MeetTTLRunningTasks(3, cache.TaskStatusRunning))
}
