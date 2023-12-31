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

package ttlworker

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// NewTaskManager is an exported version of newTaskManager for test
var NewTaskManager = newTaskManager

// Worker is an exported version of worker
type Worker = worker

func (m *taskManager) SetScanWorkers4Test(workers []worker) {
	m.scanWorkers = workers
}

// LockScanTask is an exported version of lockScanTask
func (m *taskManager) LockScanTask(se session.Session, task *cache.TTLTask, now time.Time) (*runningScanTask, error) {
	return m.lockScanTask(se, task, now)
}

// ResizeWorkersWithSysVar is an exported version of resizeWorkersWithSysVar
func (m *taskManager) ResizeWorkersWithSysVar() {
	m.resizeWorkersWithSysVar()
}

// RescheduleTasks is an exported version of rescheduleTasks
func (m *taskManager) RescheduleTasks(se session.Session, now time.Time) {
	m.rescheduleTasks(se, now)
}

// ReportMetrics is an exported version of reportMetrics
func (m *taskManager) ReportMetrics() {
	m.reportMetrics()
}

// CheckFinishedTask is an exported version of checkFinishedTask
func (m *taskManager) CheckFinishedTask(se session.Session, now time.Time) {
	m.checkFinishedTask(se, now)
}

// ReportTaskFinished is an exported version of reportTaskFinished
func (m *taskManager) GetRunningTasks() []*runningScanTask {
	return m.runningTasks
}

// MeetTTLRunningTasks is an exported version of meetTTLRunningTask
func (m *taskManager) MeetTTLRunningTasks(count int, taskStatus cache.TaskStatus) bool {
	return m.meetTTLRunningTask(count, taskStatus)
}

// ReportTaskFinished is an exported version of reportTaskFinished
func (t *runningScanTask) SetResult(err error) {
	t.result = &ttlScanTaskExecResult{
		task: t.ttlScanTask,
		err:  err,
	}
}

func TestResizeWorkers(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")

	// scale workers
	scanWorker1 := NewMockScanWorker(t)
	scanWorker1.Start()
	scanWorker2 := NewMockScanWorker(t)

	m := newTaskManager(context.Background(), nil, nil, "test-id", nil)
	m.sessPool = newMockSessionPool(t, tbl)
	m.SetScanWorkers4Test([]worker{
		scanWorker1,
	})
	newWorkers, _, err := m.resizeWorkers(m.scanWorkers, 2, func() worker {
		return scanWorker2
	})
	assert.NoError(t, err)
	assert.Len(t, newWorkers, 2)
	scanWorker1.checkWorkerStatus(workerStatusRunning, true, nil)
	scanWorker2.checkWorkerStatus(workerStatusRunning, true, nil)

	// shrink scan workers
	scanWorker1 = NewMockScanWorker(t)
	scanWorker1.Start()
	scanWorker2 = NewMockScanWorker(t)
	scanWorker2.Start()

	m = newTaskManager(context.Background(), nil, nil, "test-id", nil)
	m.sessPool = newMockSessionPool(t, tbl)
	m.SetScanWorkers4Test([]worker{
		scanWorker1,
		scanWorker2,
	})

	assert.NoError(t, m.resizeScanWorkers(1))
	scanWorker2.checkWorkerStatus(workerStatusStopped, false, nil)

	// shrink scan workers after job is run
	scanWorker1 = NewMockScanWorker(t)
	scanWorker1.Start()
	scanWorker2 = NewMockScanWorker(t)
	scanWorker2.Start()

	m = newTaskManager(context.Background(), nil, nil, "test-id", nil)
	m.sessPool = newMockSessionPool(t, tbl)
	m.SetScanWorkers4Test([]worker{
		scanWorker1,
		scanWorker2,
	})
	m.runningTasks = append(m.runningTasks, &runningScanTask{
		ttlScanTask: &ttlScanTask{
			tbl: tbl,
			TTLTask: &cache.TTLTask{
				JobID:  "test-job-id",
				ScanID: 1,
			},
		},
	})

	scanWorker2.curTaskResult = &ttlScanTaskExecResult{task: &ttlScanTask{tbl: tbl, TTLTask: &cache.TTLTask{
		JobID:  "test-job-id",
		ScanID: 1,
	}}}
	assert.NoError(t, m.resizeScanWorkers(1))
	scanWorker2.checkWorkerStatus(workerStatusStopped, false, nil)
	assert.NotNil(t, m.runningTasks[0].result)
}

type mockKVStore struct {
	kv.Storage
}

type mockTiKVStore struct {
	mockKVStore
	tikv.Storage
	regionCache *tikv.RegionCache
}

func (s *mockTiKVStore) GetRegionCache() *tikv.RegionCache {
	return s.regionCache
}

func TestGetMaxRunningTasksLimit(t *testing.T) {
	variable.TTLRunningTasks.Store(1)
	require.Equal(t, 1, getMaxRunningTasksLimit(&mockTiKVStore{}))

	variable.TTLRunningTasks.Store(2)
	require.Equal(t, 2, getMaxRunningTasksLimit(&mockTiKVStore{}))

	variable.TTLRunningTasks.Store(-1)
	require.Equal(t, variable.MaxConfigurableConcurrency, getMaxRunningTasksLimit(nil))
	require.Equal(t, variable.MaxConfigurableConcurrency, getMaxRunningTasksLimit(&mockKVStore{}))
	require.Equal(t, variable.MaxConfigurableConcurrency, getMaxRunningTasksLimit(&mockTiKVStore{}))

	s := &mockTiKVStore{regionCache: tikv.NewRegionCache(nil)}
	s.GetRegionCache().SetRegionCacheStore(1, "", "", tikvrpc.TiKV, 1, nil)
	s.GetRegionCache().SetRegionCacheStore(2, "", "", tikvrpc.TiKV, 1, nil)
	s.GetRegionCache().SetRegionCacheStore(3, "", "", tikvrpc.TiFlash, 1, nil)
	require.Equal(t, 2, getMaxRunningTasksLimit(s))
}
