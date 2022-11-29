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

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func newMockScanWorker(sessFactory func() pools.Resource) (*ttlScanWorker, chan *ttlDeleteTask, chan interface{}) {
	pool := newMockSessionPool(sessFactory)
	delCh := make(chan *ttlDeleteTask)
	notifyStateCh := make(chan interface{}, 10)
	return newScanWorker(delCh, notifyStateCh, pool), delCh, notifyStateCh
}

func TestScanWorkerSchedule(t *testing.T) {
	origLimit := variable.TTLScanBatchSize.Load()
	variable.TTLScanBatchSize.Store(5)
	defer variable.TTLScanBatchSize.Store(origLimit)

	tbl := newMockTTLTbl(t, "t1")
	task := &ttlScanTask{
		tbl:        tbl,
		expire:     time.UnixMilli(0),
		statistics: &ttlStatistics{},
	}
	w, delCh, notifyCh := newMockScanWorker(func() pools.Resource {
		s := newMockSession(t, tbl)
		s.executeSQL = func(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
			return newMockRows(tbl.KeyColumnTypes...).Append(t, 7).Rows(), nil
		}
		return s
	})

	defer func() {
		w.Stop()
		require.NoError(t, w.WaitStopped(context.TODO(), time.Second*10))
	}()

	require.Equal(t, workerStatusCreated, w.Status())
	require.False(t, w.Idle())
	err := w.Schedule(task)
	require.False(t, w.Idle())
	require.Nil(t, w.CurrentTask())
	require.EqualError(t, err, "worker is not running")

	w.Start()
	defer w.Stop()
	require.Equal(t, workerStatusRunning, w.Status())
	require.True(t, w.Idle())
	require.Nil(t, w.CurrentTask())

	require.NoError(t, w.Schedule(task))
	require.False(t, w.Idle())
	require.Same(t, w.CurrentTask(), task)

	err = w.Schedule(task)
	require.EqualError(t, err, "a task is running")
	result, ok := w.PollTaskResult()
	require.False(t, ok)
	require.Nil(t, result)
	select {
	case del := <-delCh:
		require.Equal(t, 1, len(del.rows))
		require.Equal(t, int64(7), del.rows[0][0].GetInt64())
	case <-time.After(time.Minute):
		require.FailNow(t, "timeout")
	}

	select {
	case msg := <-notifyCh:
		require.Equal(t, 0, len(notifyCh))
		endMsg, ok := msg.(*scanTaskExecEndMsg)
		require.True(t, ok)
		require.NotNil(t, endMsg.result)
		require.Nil(t, endMsg.result.err)
		require.Same(t, task, endMsg.result.task)

		require.False(t, w.Idle())
		require.Same(t, w.CurrentTask(), task)

		err = w.Schedule(task)
		require.EqualError(t, err, "the result of previous task has not been polled")

		result, ok = w.PollTaskResult()
		require.True(t, ok)
		require.Same(t, endMsg.result, result)
	case <-time.After(time.Minute):
		require.FailNow(t, "timeout")
	}
	require.Equal(t, workerStatusRunning, w.Status())
}

func TestScanWorkerScheduleWithFailedTask(t *testing.T) {
	origLimit := variable.TTLScanBatchSize.Load()
	variable.TTLScanBatchSize.Store(5)
	defer variable.TTLScanBatchSize.Store(origLimit)

	tbl := newMockTTLTbl(t, "t1")
	task := &ttlScanTask{
		tbl:        tbl,
		expire:     time.UnixMilli(0),
		statistics: &ttlStatistics{},
	}
	w, _, notifyCh := newMockScanWorker(func() pools.Resource {
		s := newMockSession(t)
		s.executeSQL = func(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
			return nil, nil
		}
		return s
	})

	defer func() {
		w.Stop()
		require.NoError(t, w.WaitStopped(context.TODO(), time.Second*10))
	}()

	w.Start()
	require.NoError(t, w.Schedule(task))

	select {
	case msg := <-notifyCh:
		require.Equal(t, 0, len(notifyCh))
		endMsg, ok := msg.(*scanTaskExecEndMsg)
		require.True(t, ok)
		require.NotNil(t, endMsg.result)
		require.EqualError(t, endMsg.result.err, "table 'test.t1' meta changed, should abort current job: [schema:1146]Table 'test.t1' doesn't exist")
		require.Same(t, task, endMsg.result.task)

		result, ok := w.PollTaskResult()
		require.True(t, ok)
		require.Same(t, endMsg.result, result)
	case <-time.After(time.Second * 2):
		require.FailNow(t, "timeout")
	}
	require.Equal(t, workerStatusRunning, w.Status())
}
