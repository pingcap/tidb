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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	scanTaskExecuteSQLMaxRetry      = 5
	scanTaskExecuteSQLRetryInterval = 2 * time.Second
)

type ttlStatistics struct {
	TotalRows   atomic.Uint64
	SuccessRows atomic.Uint64
	ErrorRows   atomic.Uint64
}

type ttlScanTask struct {
	tbl        *ttl.PhysicalTable
	expire     time.Time
	rangeStart []types.Datum
	rangeEnd   []types.Datum
	statistics *ttlStatistics
}

type ttlScanTaskExecResult struct {
	task *ttlScanTask
	err  error
}

func (t *ttlScanTask) result(err error) *ttlScanTaskExecResult {
	return &ttlScanTaskExecResult{task: t, err: err}
}

func (t *ttlScanTask) getDatumRows(datums [][]types.Datum, rows []chunk.Row) [][]types.Datum {
	if cap(datums) < len(rows) {
		datums = make([][]types.Datum, 0, len(rows))
	}
	datums = datums[:len(rows)]
	for i, row := range rows {
		datums[i] = row.GetDatumRow(t.tbl.KeyColumnTypes)
	}
	return datums
}

func (t *ttlScanTask) doScan(ctx context.Context, delCh chan<- *ttlDeleteTask, sessPool sessionPool) *ttlScanTaskExecResult {
	rawSess, err := getSession(sessPool)
	if err != nil {
		return t.result(err)
	}
	sess := newTableSession(rawSess, t.tbl, t.expire)

	generator, err := ttl.NewScanQueryGenerator(t.tbl, t.expire, t.rangeStart, t.rangeEnd)
	if err != nil {
		return t.result(err)
	}

	retrySQL := ""
	retryTimes := 0
	var lastResult [][]types.Datum
	for {
		if err = ctx.Err(); err != nil {
			return t.result(err)
		}

		sql := retrySQL
		if sql == "" {
			limit := int(variable.TTLScanBatchSize.Load())
			if sql, err = generator.NextSQL(lastResult, limit); err != nil {
				return t.result(err)
			}
		}

		if sql == "" {
			return t.result(nil)
		}

		rows, retryable, sqlErr := sess.ExecuteSQLWithCheck(ctx, sql)
		if sqlErr != nil {
			needRetry := retryable && retryTimes <= scanTaskExecuteSQLMaxRetry
			logutil.BgLogger().Error("execute query for ttl scan task failed",
				zap.String("SQL", sql),
				zap.Int("retryTimes", retryTimes),
				zap.Bool("needRetry", needRetry),
				zap.Error(err),
			)

			if !needRetry {
				return t.result(sqlErr)
			}
			retrySQL = sql
			retryTimes++
			select {
			case <-ctx.Done():
				return t.result(ctx.Err())
			case <-time.After(scanTaskExecuteSQLRetryInterval):
			}
			continue
		}

		retrySQL = ""
		retryTimes = 0
		lastResult = t.getDatumRows(lastResult, rows)
		if len(lastResult) == 0 {
			continue
		}

		delTask := &ttlDeleteTask{
			rows:       lastResult,
			statistics: t.statistics,
		}
		select {
		case <-ctx.Done():
			return t.result(ctx.Err())
		case delCh <- delTask:
			t.statistics.TotalRows.Add(uint64(len(lastResult)))
		}
	}
}

type scanTaskExecEndMsg struct {
	result *ttlScanTaskExecResult
}

type ttlScanWorker struct {
	baseWorker
	curTask       *ttlScanTask
	curTaskResult *ttlScanTaskExecResult
	delCh         chan<- *ttlDeleteTask
	notifyStateCh chan<- interface{}
	sessionPool   sessionPool
}

func newScanWorker(delCh chan<- *ttlDeleteTask, notifyStateCh chan<- interface{}, sessPool sessionPool) *ttlScanWorker {
	w := &ttlScanWorker{
		delCh:         delCh,
		notifyStateCh: notifyStateCh,
		sessionPool:   sessPool,
	}
	w.init(w.loop)
	return w
}

func (w *ttlScanWorker) Idle() bool {
	w.Lock()
	defer w.Unlock()
	return w.status == workerStatusRunning && w.curTask == nil
}

func (w *ttlScanWorker) Schedule(task *ttlScanTask) error {
	w.Lock()
	defer w.Unlock()
	if w.status != workerStatusRunning {
		return errors.New("worker is not running")
	}

	if w.curTaskResult != nil {
		return errors.New("the result of previous task has not been polled")
	}

	if w.curTask != nil {
		return errors.New("a task is running")
	}

	w.curTask = task
	w.curTaskResult = nil
	w.baseWorker.ch <- task
	return nil
}

func (w *ttlScanWorker) CurrentTask() *ttlScanTask {
	w.Lock()
	defer w.Unlock()
	return w.curTask
}

func (w *ttlScanWorker) PollTaskResult() (*ttlScanTaskExecResult, bool) {
	w.Lock()
	defer w.Unlock()
	if r := w.curTaskResult; r != nil {
		w.curTask = nil
		w.curTaskResult = nil
		return r, true
	}
	return nil, false
}

func (w *ttlScanWorker) loop() error {
	ctx := w.baseWorker.ctx
	for w.status == workerStatusRunning {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-w.baseWorker.ch:
			switch task := msg.(type) {
			case *ttlScanTask:
				w.handleScanTask(ctx, task)
			default:
				logutil.BgLogger().Warn("unrecognized message for ttlScanWorker", zap.Any("msg", msg))
			}
		}
	}
	return nil
}

func (w *ttlScanWorker) handleScanTask(ctx context.Context, task *ttlScanTask) {
	result := task.doScan(ctx, w.delCh, w.sessionPool)
	if result == nil {
		result = &ttlScanTaskExecResult{task: task}
	}

	w.baseWorker.Lock()
	w.curTaskResult = result
	w.baseWorker.Unlock()

	if w.notifyStateCh != nil {
		select {
		case w.notifyStateCh <- &scanTaskExecEndMsg{result: result}:
		default:
		}
	}
}
