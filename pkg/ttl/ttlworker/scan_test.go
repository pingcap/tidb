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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type mockScanWorker struct {
	*ttlScanWorker
	t        *testing.T
	delCh    chan *ttlDeleteTask
	notifyCh chan any
	sessPoll *mockSessionPool
}

func NewMockScanWorker(t *testing.T) *mockScanWorker {
	w := &mockScanWorker{
		t:        t,
		delCh:    make(chan *ttlDeleteTask),
		notifyCh: make(chan any, 10),
		sessPoll: newMockSessionPool(t),
	}

	w.ttlScanWorker = newScanWorker(w.delCh, w.notifyCh, w.sessPoll)
	require.Equal(t, workerStatusCreated, w.Status())
	require.False(t, w.CouldSchedule())
	result := w.PollTaskResult()
	require.Nil(t, result)
	return w
}

func (w *mockScanWorker) checkWorkerStatus(status workerStatus, idle bool, curTask *ttlScanTask) {
	require.Equal(w.t, status, w.status)
	require.Equal(w.t, idle, w.CouldSchedule())
	require.Same(w.t, curTask, w.CurrentTask())
}

func (w *mockScanWorker) checkPollResult(exist bool, err string) {
	curTask := w.CurrentTask()
	r := w.PollTaskResult()
	require.Equal(w.t, exist, r != nil)
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
		require.Same(w.t, w.curTask.tbl, del.tbl)
		require.Equal(w.t, w.curTask.ExpireTime, del.expire)
		require.NotEqual(w.t, 0, len(del.rows))
		return del
	case <-time.After(10 * time.Second):
		require.FailNow(w.t, "timeout")
	}

	require.FailNow(w.t, "")
	return nil
}

func (w *mockScanWorker) setOneRowResult(tbl *cache.PhysicalTable, val ...any) {
	w.sessPoll.se.sessionInfoSchema = newMockInfoSchema(tbl.TableInfo)
	w.sessPoll.se.rows = newMockRows(w.t, tbl.KeyColumnTypes...).Append(val...).Rows()
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
	w := NewMockScanWorker(t)
	w.setOneRowResult(tbl, 7)
	defer w.stopWithWait()

	task := &ttlScanTask{
		ctx: cache.SetMockExpireTime(context.Background(), time.Now()),
		tbl: tbl,
		TTLTask: &cache.TTLTask{
			ExpireTime: time.UnixMilli(0),
		},
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
	w := NewMockScanWorker(t)
	w.clearInfoSchema()
	defer w.stopWithWait()

	task := &ttlScanTask{
		ctx: cache.SetMockExpireTime(context.Background(), time.Now()),
		tbl: tbl,
		TTLTask: &cache.TTLTask{
			ExpireTime: time.UnixMilli(0),
		},
		statistics: &ttlStatistics{},
	}

	w.Start()
	w.checkWorkerStatus(workerStatusRunning, true, nil)
	w.checkPollResult(false, "")

	require.NoError(t, w.Schedule(task))
	w.checkWorkerStatus(workerStatusRunning, false, task)
	msg := w.waitNotifyScanTaskEnd()
	require.Same(t, task, msg.result.task)
	require.EqualError(t, msg.result.err, "table 'test.t1' meta changed, should abort current job: [schema:1146]Table 'test.t1' doesn't exist")
	w.checkWorkerStatus(workerStatusRunning, false, task)
	w.checkPollResult(true, msg.result.err.Error())
	w.checkWorkerStatus(workerStatusRunning, true, nil)
}

type mockScanTask struct {
	*ttlScanTask
	t        *testing.T
	tbl      *cache.PhysicalTable
	sessPool *mockSessionPool
	sqlRetry []int

	delCh               chan *ttlDeleteTask
	prevSQL             string
	prevSQLRetry        int
	delTasks            []*ttlDeleteTask
	schemaChangeIdx     int
	schemaChangeInRetry int
}

func newMockScanTask(t *testing.T, sqlCnt int) *mockScanTask {
	tbl := newMockTTLTbl(t, "t1")
	task := &mockScanTask{
		t: t,
		ttlScanTask: &ttlScanTask{
			ctx: context.Background(),
			tbl: tbl,
			TTLTask: &cache.TTLTask{
				ExpireTime:     time.UnixMilli(0),
				ScanRangeStart: []types.Datum{types.NewIntDatum(0)},
			},
			statistics: &ttlStatistics{},
		},
		tbl:             tbl,
		delCh:           make(chan *ttlDeleteTask, sqlCnt*(scanTaskExecuteSQLMaxRetry+1)),
		sessPool:        newMockSessionPool(t),
		sqlRetry:        make([]int, sqlCnt),
		schemaChangeIdx: -1,
	}
	task.sessPool.se.executeSQL = task.execSQL
	return task
}

func (t *mockScanTask) selectSQL(i int) string {
	op := ">"
	if i == 0 {
		op = ">="
	}
	return fmt.Sprintf("SELECT LOW_PRIORITY SQL_NO_CACHE `_tidb_rowid` FROM `test`.`t1` WHERE `_tidb_rowid` %s %d AND `time` < FROM_UNIXTIME(0) ORDER BY `_tidb_rowid` ASC LIMIT 3", op, i*100)
}

func (t *mockScanTask) runDoScanForTest(delTaskCnt int, errString string) *ttlScanTaskExecResult {
	t.ttlScanTask.statistics.Reset()
	origLimit := variable.TTLScanBatchSize.Load()
	variable.TTLScanBatchSize.Store(3)
	origRetryInterval := scanTaskExecuteSQLRetryInterval
	scanTaskExecuteSQLRetryInterval = time.Millisecond
	defer func() {
		variable.TTLScanBatchSize.Store(origLimit)
		scanTaskExecuteSQLRetryInterval = origRetryInterval
	}()

	t.sessPool.se.sessionInfoSchema = newMockInfoSchema(t.tbl.TableInfo)
	t.prevSQL = ""
	t.prevSQLRetry = 0
	t.sessPool.lastSession = nil
	r := t.doScan(context.TODO(), t.delCh, t.sessPool)
	require.NotNil(t.t, t.sessPool.lastSession)
	require.True(t.t, t.sessPool.lastSession.closed)
	require.Greater(t.t, t.sessPool.lastSession.resetTimeZoneCalls, 0)
	require.NotNil(t.t, r)
	require.Same(t.t, t.ttlScanTask, r.task)
	if errString == "" {
		require.NoError(t.t, r.err)
	} else {
		require.EqualError(t.t, r.err, errString)
	}

	previousIdx := delTaskCnt
	if errString == "" {
		previousIdx = len(t.sqlRetry) - 1
	}
	require.Equal(t.t, t.selectSQL(previousIdx), t.prevSQL)
	if errString == "" {
		require.Equal(t.t, t.sqlRetry[previousIdx], t.prevSQLRetry)
	} else if previousIdx == t.schemaChangeIdx && t.schemaChangeInRetry <= scanTaskExecuteSQLMaxRetry {
		require.Equal(t.t, t.schemaChangeInRetry, t.prevSQLRetry)
	} else {
		require.Equal(t.t, scanTaskExecuteSQLMaxRetry, t.prevSQLRetry)
	}
	t.delTasks = make([]*ttlDeleteTask, 0, len(t.sqlRetry))
loop:
	for {
		select {
		case del, ok := <-t.delCh:
			if !ok {
				break loop
			}
			t.delTasks = append(t.delTasks, del)
		default:
			break loop
		}
	}

	require.Equal(t.t, delTaskCnt, len(t.delTasks))
	expectTotalRows := 0
	for i, del := range t.delTasks {
		require.NotNil(t.t, del)
		require.NotNil(t.t, del.statistics)
		require.Same(t.t, t.statistics, del.statistics)
		require.Same(t.t, t.tbl, del.tbl)
		require.Equal(t.t, t.ExpireTime, del.expire)
		if i < len(t.sqlRetry)-1 {
			require.Equal(t.t, 3, len(del.rows))
			require.Equal(t.t, 1, len(del.rows[2]))
			require.Equal(t.t, int64((i+1)*100), del.rows[2][0].GetInt64())
		} else {
			require.Equal(t.t, 2, len(del.rows))
		}
		require.Equal(t.t, 1, len(del.rows[0]))
		require.Equal(t.t, int64(i*100+1), del.rows[0][0].GetInt64())
		require.Equal(t.t, 1, len(del.rows[0]))
		require.Equal(t.t, int64(i*100+2), del.rows[1][0].GetInt64())
		expectTotalRows += len(del.rows)
	}
	require.Equal(t.t, expectTotalRows, int(t.statistics.TotalRows.Load()))
	return r
}

func (t *mockScanTask) checkDelTasks(cnt int) {
	require.Equal(t.t, cnt, len(t.delTasks))
	for i := 0; i < cnt; i++ {
		del := t.delTasks[i]
		require.Nil(t.t, del)
		require.NotNil(t.t, del.statistics)
		require.Same(t.t, t.statistics, del.statistics)
		if i < 2 {
			require.Equal(t.t, 3, len(del.rows))
			require.Equal(t.t, 1, len(del.rows[2]))
			require.Equal(t.t, int64((i+1)*100), del.rows[2][0].GetInt64())
		} else {
			require.Equal(t.t, 2, len(del.rows))
		}
		require.Equal(t.t, 1, len(del.rows[0]))
		require.Equal(t.t, int64(i*100+1), del.rows[0][0].GetInt64())
		require.Equal(t.t, 1, len(del.rows[0]))
		require.Equal(t.t, int64(i*100+2), del.rows[1][0].GetInt64())
	}
}

func (t *mockScanTask) execSQL(_ context.Context, sql string, _ ...any) ([]chunk.Row, error) {
	var i int
	found := false
	for i = 0; i < len(t.sqlRetry); i++ {
		if sql == t.selectSQL(i) {
			found = true
			break
		}
	}
	require.True(t.t, found, sql)

	curRetry := 0
	if sql == t.prevSQL {
		curRetry = t.prevSQLRetry + 1
	}

	if curRetry == 0 && i > 0 {
		require.Equal(t.t, t.selectSQL(i-1), t.prevSQL)
		require.Equal(t.t, t.sqlRetry[i-1], t.prevSQLRetry)
	}
	t.prevSQL = sql
	t.prevSQLRetry = curRetry
	require.LessOrEqual(t.t, curRetry, t.sqlRetry[i])

	if t.schemaChangeIdx == i && t.schemaChangeInRetry == curRetry {
		t.sessPool.lastSession.sessionInfoSchema = newMockInfoSchema()
	}

	if curRetry < t.sqlRetry[i] {
		return nil, errors.New("mockErr")
	}

	rows := newMockRows(t.t, t.tbl.KeyColumnTypes...).Append(i*100 + 1).Append(i*100 + 2)
	if i < len(t.sqlRetry)-1 {
		rows.Append((i + 1) * 100)
	}
	return rows.Rows(), nil
}

func TestScanTaskDoScan(t *testing.T) {
	task := newMockScanTask(t, 3)
	task.ctx = cache.SetMockExpireTime(task.ctx, time.Now())
	task.sqlRetry[1] = scanTaskExecuteSQLMaxRetry
	task.runDoScanForTest(3, "")

	task.sqlRetry[1] = scanTaskExecuteSQLMaxRetry + 1
	task.runDoScanForTest(1, "mockErr")

	task.sqlRetry[1] = scanTaskExecuteSQLMaxRetry
	task.schemaChangeIdx = 1
	task.schemaChangeInRetry = 0
	task.runDoScanForTest(1, "table 'test.t1' meta changed, should abort current job: [schema:1146]Table 'test.t1' doesn't exist")

	task.sqlRetry[1] = scanTaskExecuteSQLMaxRetry
	task.schemaChangeIdx = 1
	task.schemaChangeInRetry = 2
	task.runDoScanForTest(1, "table 'test.t1' meta changed, should abort current job: [schema:1146]Table 'test.t1' doesn't exist")
}

func TestScanTaskCheck(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")
	pool := newMockSessionPool(t, tbl)
	pool.se.rows = newMockRows(t, types.NewFieldType(mysql.TypeInt24)).Append(12).Rows()
	ctx := cache.SetMockExpireTime(context.Background(), time.Unix(100, 0))

	task := &ttlScanTask{
		ctx: ctx,
		TTLTask: &cache.TTLTask{
			ExpireTime: time.Unix(101, 0).Add(time.Minute),
		},
		tbl:        tbl,
		statistics: &ttlStatistics{},
	}

	ch := make(chan *ttlDeleteTask, 1)
	result := task.doScan(context.Background(), ch, pool)
	require.Equal(t, task, result.task)
	require.EqualError(t, result.err, "current expire time is after safe expire time. (161 > 160)")
	require.Equal(t, 0, len(ch))
	require.Equal(t, "Total Rows: 0, Success Rows: 0, Error Rows: 0", task.statistics.String())

	task = &ttlScanTask{
		ctx: ctx,
		TTLTask: &cache.TTLTask{
			ExpireTime: time.Unix(100, 0).Add(time.Minute),
		},
		tbl:        tbl,
		statistics: &ttlStatistics{},
	}
	result = task.doScan(context.Background(), ch, pool)
	require.Equal(t, task, result.task)
	require.NoError(t, result.err)
	require.Equal(t, 1, len(ch))
	require.Equal(t, "Total Rows: 1, Success Rows: 0, Error Rows: 0", task.statistics.String())
}
