// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl"
	"github.com/stretchr/testify/require"
)

type mockScanWorker struct {
	*ttlScanWorker
	t        *testing.T
	delCh    chan *ttlDeleteTask
	notifyCh chan interface{}
	sessPoll *mockSessionPool
}

func newMockScanWorker(t *testing.T) *mockScanWorker {
	w := &mockScanWorker{
		t:        t,
		delCh:    make(chan *ttlDeleteTask),
		notifyCh: make(chan interface{}, 10),
		sessPoll: newMockSessionPool(t),
	}

	w.ttlScanWorker = newScanWorker(w.delCh, w.notifyCh, w.sessPoll)
	require.Equal(t, workerStatusCreated, w.Status())
	require.False(t, w.Idle())
	result, ok := w.PollTaskResult()
	require.False(t, ok)
	require.Nil(t, result)
	return w
}

func (w *mockScanWorker) checkWorkerStatus(status workerStatus, idle bool, curTask *ttlScanTask) {
	require.Equal(w.t, status, w.status)
	require.Equal(w.t, idle, w.Idle())
	require.Same(w.t, curTask, w.CurrentTask())
}

func (w *mockScanWorker) checkPollResult(exist bool, err string) {
	curTask := w.CurrentTask()
	r, ok := w.PollTaskResult()
	require.Equal(w.t, exist, ok)
	if !exist {
		require.Nil(w.t, r)
	} else {
		require.NotNil(w.t, r)
		require.NotNil(w.t, r.task)
		require.Same(w.t, curTask, r.task)
		if err == "" {
			require.NoError(w.t, r.err)
		} else {
			require.EqualError(w.t, r.err, err)
		}
	}
}

func (w *mockScanWorker) waitNotifyScanTaskEnd() *scanTaskExecEndMsg {
	select {
	case msg := <-w.notifyCh:
		endMsg, ok := msg.(*scanTaskExecEndMsg)
		require.True(w.t, ok)
		require.NotNil(w.t, endMsg.result)
		require.Same(w.t, w.CurrentTask(), endMsg.result.task)
		return endMsg
	case <-time.After(10 * time.Second):
		require.FailNow(w.t, "timeout")
	}

	require.FailNow(w.t, "")
	return nil
}

func (w *mockScanWorker) pollDelTask() *ttlDeleteTask {
	select {
	case del := <-w.delCh:
		require.NotNil(w.t, del)
		require.NotNil(w.t, del.statistics)
		require.NotEqual(w.t, 0, len(del.rows))
		return del
	case <-time.After(10 * time.Second):
		require.FailNow(w.t, "timeout")
	}

	require.FailNow(w.t, "")
	return nil
}

func (w *mockScanWorker) setOneRowResult(tbl *ttl.PhysicalTable, val ...interface{}) {
	w.sessPoll.se.sessionInfoSchema = newMockInfoSchema(tbl.TableInfo)
	w.sessPoll.se.rows = newMockRows(tbl.KeyColumnTypes...).Append(w.t, val...).Rows()
}

func (w *mockScanWorker) clearInfoSchema() {
	w.sessPoll.se.sessionInfoSchema = newMockInfoSchema()
}

func (w *mockScanWorker) stopWithWait() {
	w.Stop()
	require.NoError(w.t, w.WaitStopped(context.TODO(), 10*time.Second))
}

func TestScanWorkerSchedule(t *testing.T) {
	origLimit := variable.TTLScanBatchSize.Load()
	variable.TTLScanBatchSize.Store(5)
	defer variable.TTLScanBatchSize.Store(origLimit)

	tbl := newMockTTLTbl(t, "t1")
	w := newMockScanWorker(t)
	w.setOneRowResult(tbl, 7)
	defer w.stopWithWait()

	task := &ttlScanTask{
		tbl:        tbl,
		expire:     time.UnixMilli(0),
		statistics: &ttlStatistics{},
	}

	require.EqualError(t, w.Schedule(task), "worker is not running")
	w.checkWorkerStatus(workerStatusCreated, false, nil)
	w.checkPollResult(false, "")

	w.Start()
	w.checkWorkerStatus(workerStatusRunning, true, nil)
	w.checkPollResult(false, "")

	require.NoError(t, w.Schedule(task))
	w.checkWorkerStatus(workerStatusRunning, false, task)
	w.checkPollResult(false, "")

	require.EqualError(t, w.Schedule(task), "a task is running")
	w.checkWorkerStatus(workerStatusRunning, false, task)
	w.checkPollResult(false, "")

	del := w.pollDelTask()
	require.Equal(t, 1, len(del.rows))
	require.Equal(t, 1, len(del.rows[0]))
	require.Equal(t, int64(7), del.rows[0][0].GetInt64())

	msg := w.waitNotifyScanTaskEnd()
	require.Same(t, task, msg.result.task)
	require.NoError(t, msg.result.err)
	w.checkWorkerStatus(workerStatusRunning, false, task)
	w.checkPollResult(true, "")
	w.checkWorkerStatus(workerStatusRunning, true, nil)
	w.checkPollResult(false, "")
}

func TestScanWorkerScheduleWithFailedTask(t *testing.T) {
	origLimit := variable.TTLScanBatchSize.Load()
	variable.TTLScanBatchSize.Store(5)
	defer variable.TTLScanBatchSize.Store(origLimit)

	tbl := newMockTTLTbl(t, "t1")
	w := newMockScanWorker(t)
	w.clearInfoSchema()
	defer w.stopWithWait()

	task := &ttlScanTask{
		tbl:        tbl,
		expire:     time.UnixMilli(0),
		statistics: &ttlStatistics{},
	}

	w.Start()
	w.checkWorkerStatus(workerStatusRunning, true, nil)
	w.checkPollResult(false, "")

	require.NoError(t, w.Schedule(task))
	w.checkWorkerStatus(workerStatusRunning, false, task)
	w.checkPollResult(false, "")

	msg := w.waitNotifyScanTaskEnd()
	require.Same(t, task, msg.result.task)
	require.EqualError(t, msg.result.err, "table 'test.t1' meta changed, should abort current job: [schema:1146]Table 'test.t1' doesn't exist")
	w.checkWorkerStatus(workerStatusRunning, false, task)
	w.checkPollResult(true, msg.result.err.Error())
	w.checkWorkerStatus(workerStatusRunning, true, nil)
}
