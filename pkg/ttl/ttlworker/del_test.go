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
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestTTLDelRetryBuffer(t *testing.T) {
	createTask := func(name string) (*ttlDeleteTask, [][]types.Datum, *ttlStatistics) {
		rows := make([][]types.Datum, 10)
		statistics := &ttlStatistics{}
		statistics.IncTotalRows(10)
		task := &ttlDeleteTask{
			tbl:        newMockTTLTbl(t, name),
			expire:     time.UnixMilli(0),
			rows:       rows,
			statistics: statistics,
		}
		return task, rows, statistics
	}

	shouldNotDoRetry := func(task *ttlDeleteTask) [][]types.Datum {
		require.FailNow(t, "should not do retry")
		return nil
	}

	start := time.UnixMilli(0)
	tm := start
	buffer := newTTLDelRetryBuffer()
	require.Equal(t, delRetryBufferSize, buffer.maxSize)
	require.Equal(t, delMaxRetry, buffer.maxRetry)
	require.Equal(t, delRetryInterval, buffer.retryInterval)

	buffer.maxSize = 3
	buffer.maxRetry = 2
	buffer.retryInterval = 10 * time.Second
	buffer.getTime = func() time.Time {
		return tm
	}

	// add success task
	task1, rows1, statics1 := createTask("t1")
	buffer.RecordTaskResult(task1, nil)
	require.Equal(t, 0, buffer.Len())
	buffer.DoRetry(shouldNotDoRetry)
	require.Equal(t, uint64(0), statics1.ErrorRows.Load())

	// add a task with 1 failed rows
	buffer.RecordTaskResult(task1, rows1[:1])
	require.Equal(t, 1, buffer.Len())
	buffer.DoRetry(shouldNotDoRetry)
	require.Equal(t, uint64(0), statics1.ErrorRows.Load())

	// add another task with 2 failed rows
	tm = tm.Add(time.Second)
	task2, rows2, statics2 := createTask("t2")
	buffer.RecordTaskResult(task2, rows2[:2])
	require.Equal(t, 2, buffer.Len())
	buffer.DoRetry(shouldNotDoRetry)
	require.Equal(t, uint64(0), statics2.ErrorRows.Load())

	// add another task with 3 failed rows
	tm = tm.Add(time.Second)
	task3, rows3, statics3 := createTask("t3")
	buffer.RecordTaskResult(task3, rows3[:3])
	require.Equal(t, 3, buffer.Len())
	buffer.DoRetry(shouldNotDoRetry)
	require.Equal(t, uint64(0), statics3.ErrorRows.Load())

	// add new task will eliminate old tasks
	tm = tm.Add(time.Second)
	task4, rows4, statics4 := createTask("t4")
	buffer.RecordTaskResult(task4, rows4[:4])
	require.Equal(t, 3, buffer.Len())
	buffer.DoRetry(shouldNotDoRetry)
	require.Equal(t, uint64(0), statics4.ErrorRows.Load())
	require.Equal(t, uint64(1), statics1.ErrorRows.Load())

	// poll up-to-date tasks
	tm = tm.Add(10*time.Second - time.Millisecond)
	tasks := make([]*ttlDeleteTask, 0)
	doRetrySuccess := func(task *ttlDeleteTask) [][]types.Datum {
		task.statistics.IncSuccessRows(len(task.rows))
		tasks = append(tasks, task)
		return nil
	}
	nextInterval := buffer.DoRetry(doRetrySuccess)
	require.Equal(t, time.Millisecond, nextInterval)
	require.Equal(t, 2, len(tasks))
	require.Equal(t, "t2", tasks[0].tbl.Name.L)
	require.Equal(t, time.UnixMilli(0), tasks[0].expire)
	require.Equal(t, 2, len(tasks[0].rows))
	require.Equal(t, uint64(2), statics2.SuccessRows.Load())
	require.Equal(t, uint64(0), statics2.ErrorRows.Load())
	require.Equal(t, "t3", tasks[1].tbl.Name.L)
	require.Equal(t, time.UnixMilli(0), tasks[0].expire)
	require.Equal(t, 3, len(tasks[1].rows))
	require.Equal(t, 1, buffer.Len())
	require.Equal(t, uint64(3), statics3.SuccessRows.Load())
	require.Equal(t, uint64(0), statics3.ErrorRows.Load())
	require.Equal(t, uint64(0), statics4.SuccessRows.Load())
	require.Equal(t, uint64(0), statics4.ErrorRows.Load())

	// poll next
	tm = tm.Add(time.Millisecond)
	tasks = make([]*ttlDeleteTask, 0)
	nextInterval = buffer.DoRetry(doRetrySuccess)
	require.Equal(t, 10*time.Second, nextInterval)
	require.Equal(t, 1, len(tasks))
	require.Equal(t, "t4", tasks[0].tbl.Name.L)
	require.Equal(t, time.UnixMilli(0), tasks[0].expire)
	require.Equal(t, 4, len(tasks[0].rows))
	require.Equal(t, 0, buffer.Len())
	require.Equal(t, uint64(4), statics4.SuccessRows.Load())
	require.Equal(t, uint64(0), statics4.ErrorRows.Load())

	// test retry max count
	retryCnt := 0
	doRetryFail := func(task *ttlDeleteTask) [][]types.Datum {
		retryCnt++
		task.statistics.SuccessRows.Add(1)
		return task.rows[1:]
	}
	task5, rows5, statics5 := createTask("t5")
	buffer.RecordTaskResult(task5, rows5[:5])
	require.Equal(t, 1, buffer.Len())
	tm = tm.Add(10 * time.Second)
	nextInterval = buffer.DoRetry(doRetryFail)
	require.Equal(t, 10*time.Second, nextInterval)
	require.Equal(t, uint64(1), statics5.SuccessRows.Load())
	require.Equal(t, uint64(0), statics5.ErrorRows.Load())
	require.Equal(t, 1, retryCnt)
	tm = tm.Add(10 * time.Second)
	buffer.DoRetry(doRetryFail)
	require.Equal(t, uint64(2), statics5.SuccessRows.Load())
	require.Equal(t, uint64(3), statics5.ErrorRows.Load())
	require.Equal(t, 2, retryCnt)
	require.Equal(t, 0, buffer.Len())

	// test task should be immutable
	require.Equal(t, 10, len(task5.rows))

	// test drain
	require.Equal(t, 0, buffer.Len())
	task6, rows6, statics6 := createTask("t6")
	buffer.RecordTaskResult(task6, rows6[:7])
	require.Equal(t, 1, buffer.Len())
	require.Equal(t, uint64(0), statics6.SuccessRows.Load())
	require.Equal(t, uint64(0), statics6.ErrorRows.Load())
	buffer.Drain()
	require.Equal(t, 0, buffer.Len())
	require.Equal(t, uint64(0), statics6.SuccessRows.Load())
	require.Equal(t, uint64(7), statics6.ErrorRows.Load())
}

func TestTTLDeleteTaskDoDelete(t *testing.T) {
	origBatchSize := variable.TTLDeleteBatchSize.Load()
	delBatch := 3
	variable.TTLDeleteBatchSize.Store(int64(delBatch))
	defer variable.TTLDeleteBatchSize.Store(origBatchSize)

	t1 := newMockTTLTbl(t, "t1")
	s := newMockSession(t)
	var sqls []string
	var retryErrBatches []int
	var nonRetryBatches []int
	var afterExecuteSQL func()
	s.executeSQL = func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error) {
		s.sessionInfoSchema = newMockInfoSchema(t1.TableInfo)
		sqls = append(sqls, sql)

		if !strings.Contains(sql, "`t1`") {
			require.FailNow(t, "")
		}

		defer func() {
			if afterExecuteSQL != nil {
				afterExecuteSQL()
			}
		}()

		if slices.Contains(retryErrBatches, len(sqls)-1) {
			return nil, errors.New("mockErr")
		}

		if slices.Contains(nonRetryBatches, len(sqls)-1) {
			// set an infoschema that contains no table to make an error that cannot retry
			s.sessionInfoSchema = newMockInfoSchema()
			return nil, nil
		}

		return nil, nil
	}

	nRows := func(n int) [][]types.Datum {
		rows := make([][]types.Datum, n)
		for i := 0; i < n; i++ {
			rows[i] = []types.Datum{
				types.NewIntDatum(int64(i)),
			}
		}
		return rows
	}

	delTask := func(batchCnt int) *ttlDeleteTask {
		task := &ttlDeleteTask{
			tbl:        t1,
			expire:     time.UnixMilli(0),
			rows:       nRows(batchCnt * delBatch),
			statistics: &ttlStatistics{},
		}
		task.statistics.TotalRows.Add(uint64(batchCnt * delBatch))
		return task
	}

	cases := []struct {
		batchCnt          int
		retryErrBatches   []int
		noRetryErrBatches []int
		cancelCtx         bool
		cancelCtxBatch    int
	}{
		{
			// all success
			batchCnt: 10,
		},
		{
			// all retries
			batchCnt:        10,
			retryErrBatches: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// all errors without retry
			batchCnt:          10,
			noRetryErrBatches: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// some retries and some not
			batchCnt:          10,
			noRetryErrBatches: []int{3, 8, 9},
			retryErrBatches:   []int{1, 2, 4},
		},
		{
			// some retries and some not
			batchCnt:          10,
			noRetryErrBatches: []int{3, 8, 9},
			retryErrBatches:   []int{1, 2, 4},
			cancelCtx:         true,
			cancelCtxBatch:    6,
		},
	}

	for _, c := range cases {
		ctx, cancel := context.WithCancel(context.Background())
		if c.cancelCtx && c.cancelCtxBatch == 0 {
			cancel()
		}

		afterExecuteSQL = func() {
			if c.cancelCtx {
				if len(sqls) == c.cancelCtxBatch {
					cancel()
				}
			}
		}

		task := delTask(c.batchCnt)
		require.Equal(t, len(task.rows), c.batchCnt*delBatch)
		sqls = make([]string, 0, c.batchCnt)
		retryErrBatches = c.retryErrBatches
		nonRetryBatches = c.noRetryErrBatches
		retryRows := task.doDelete(ctx, s)
		realBatchCnt := c.batchCnt
		if c.cancelCtx {
			realBatchCnt = c.cancelCtxBatch
		}
		require.LessOrEqual(t, realBatchCnt, c.batchCnt)

		// check SQLs
		require.Equal(t, realBatchCnt, len(sqls))
		expectedSQLs := make([]string, 0, len(sqls))
		for i := 0; i < realBatchCnt; i++ {
			batch := task.rows[i*delBatch : (i+1)*delBatch]
			idList := make([]string, 0, delBatch)
			for _, row := range batch {
				idList = append(idList, strconv.FormatInt(row[0].GetInt64(), 10))
			}
			sql := fmt.Sprintf("DELETE LOW_PRIORITY FROM `test`.`t1` "+
				"WHERE `_tidb_rowid` IN (%s) AND `time` < FROM_UNIXTIME(0) LIMIT %d",
				strings.Join(idList, ", "),
				delBatch,
			)
			expectedSQLs = append(expectedSQLs, sql)
		}
		require.Equal(t, strings.Join(expectedSQLs, "\n"), strings.Join(sqls, "\n"))

		// check retry rows
		var expectedRetryRows [][]types.Datum
		for i := 0; i < realBatchCnt; i++ {
			if slices.Contains(c.retryErrBatches, i) {
				expectedRetryRows = append(expectedRetryRows, task.rows[i*delBatch:(i+1)*delBatch]...)
			}
		}
		require.Equal(t, expectedRetryRows, retryRows)

		// check statistics
		var expectedErrRows uint64
		for i := 0; i < c.batchCnt; i++ {
			if i >= realBatchCnt || slices.Contains(c.noRetryErrBatches, i) {
				expectedErrRows += uint64(delBatch)
			}
		}
		expectedSuccessRows := uint64(len(task.rows)) - expectedErrRows - uint64(len(expectedRetryRows))
		require.Equal(t, expectedSuccessRows, task.statistics.SuccessRows.Load())
		require.Equal(t, expectedErrRows, task.statistics.ErrorRows.Load())
	}
}

func TestTTLDeleteRateLimiter(t *testing.T) {
	origDeleteLimit := variable.TTLDeleteRateLimit.Load()
	defer func() {
		variable.TTLDeleteRateLimit.Store(origDeleteLimit)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	variable.TTLDeleteRateLimit.Store(100000)
	require.NoError(t, globalDelRateLimiter.Wait(ctx))
	require.Equal(t, rate.Limit(100000), globalDelRateLimiter.limiter.Limit())
	require.Equal(t, int64(100000), globalDelRateLimiter.limit.Load())

	variable.TTLDeleteRateLimit.Store(0)
	require.NoError(t, globalDelRateLimiter.Wait(ctx))
	require.Equal(t, rate.Limit(0), globalDelRateLimiter.limiter.Limit())
	require.Equal(t, int64(0), globalDelRateLimiter.limit.Load())

	// 0 stands for no limit
	require.NoError(t, globalDelRateLimiter.Wait(ctx))
	// cancel ctx returns an error
	cancel()
	cancel = nil
	require.EqualError(t, globalDelRateLimiter.Wait(ctx), "context canceled")
}

func TestTTLDeleteTaskWorker(t *testing.T) {
	origBatchSize := variable.TTLDeleteBatchSize.Load()
	variable.TTLDeleteBatchSize.Store(3)
	defer variable.TTLDeleteBatchSize.Store(origBatchSize)

	t1 := newMockTTLTbl(t, "t1")
	t2 := newMockTTLTbl(t, "t2")
	t3 := newMockTTLTbl(t, "t3")
	t4 := newMockTTLTbl(t, "t4")
	s := newMockSession(t)
	pool := newMockSessionPool(t)
	pool.se = s

	sqlMap := make(map[string]int)
	t3Retried := make(chan struct{})
	t4Retried := make(chan struct{})
	s.executeSQL = func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error) {
		pool.lastSession.sessionInfoSchema = newMockInfoSchema(t1.TableInfo, t2.TableInfo, t3.TableInfo, t4.TableInfo)
		if strings.Contains(sql, "`t1`") {
			// success
			return nil, nil
		}

		if strings.Contains(sql, "`t2`") {
			// first error, retry success
			if _, ok := sqlMap[sql]; ok {
				close(t3Retried)
				return nil, nil
			}
			sqlMap[sql] = 1
			return nil, errors.New("mockErr")
		}

		if strings.Contains(sql, "`t3`") {
			// error no retry
			pool.lastSession.sessionInfoSchema = newMockInfoSchema()
			return nil, nil
		}

		if strings.Contains(sql, "`t4`") {
			// error and retry still error
			// this is to test the retry buffer should be drained after the delete worker stopped
			i := sqlMap[sql]
			if i >= 2 {
				// i >= 2 means t4 has retried once and records in retry buffer
				close(t4Retried)
			}
			sqlMap[sql] = i + 1
			return nil, errors.New("mockErr")
		}

		require.FailNow(t, "")
		return nil, nil
	}

	delCh := make(chan *ttlDeleteTask)
	w := newDeleteWorker(delCh, pool)
	w.retryBuffer.retryInterval = time.Millisecond
	w.retryBuffer.maxRetry = math.MaxInt
	require.Equal(t, workerStatusCreated, w.Status())
	w.Start()
	require.Equal(t, workerStatusRunning, w.Status())
	defer func() {
		w.Stop()
		require.NoError(t, w.WaitStopped(context.TODO(), 10*time.Second))
	}()

	tasks := make([]*ttlDeleteTask, 0)
	for _, tbl := range []*cache.PhysicalTable{t1, t2, t3, t4} {
		task := &ttlDeleteTask{
			tbl:    tbl,
			expire: time.UnixMilli(0),
			rows: [][]types.Datum{
				{types.NewIntDatum(1)},
				{types.NewIntDatum(2)},
				{types.NewIntDatum(3)},
			},
			statistics: &ttlStatistics{},
		}
		task.statistics.TotalRows.Add(3)
		tasks = append(tasks, task)
		select {
		case delCh <- task:
		case <-time.After(time.Second):
			require.FailNow(t, "")
		}
	}

	select {
	case <-t3Retried:
	case <-time.After(time.Second):
		require.FailNow(t, "")
	}

	select {
	case <-t4Retried:
	case <-time.After(time.Second):
		require.FailNow(t, "")
	}

	// before stop, t4 should always retry without any error rows
	require.Equal(t, uint64(0), tasks[3].statistics.ErrorRows.Load())
	w.Stop()
	require.NoError(t, w.WaitStopped(context.Background(), 10*time.Second))

	require.Equal(t, uint64(3), tasks[0].statistics.SuccessRows.Load())
	require.Equal(t, uint64(0), tasks[0].statistics.ErrorRows.Load())

	require.Equal(t, uint64(3), tasks[1].statistics.SuccessRows.Load())
	require.Equal(t, uint64(0), tasks[1].statistics.ErrorRows.Load())

	require.Equal(t, uint64(0), tasks[2].statistics.SuccessRows.Load())
	require.Equal(t, uint64(3), tasks[2].statistics.ErrorRows.Load())

	require.Equal(t, uint64(0), tasks[3].statistics.SuccessRows.Load())
	require.Equal(t, uint64(3), tasks[3].statistics.ErrorRows.Load())
}
