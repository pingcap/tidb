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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/sqlbuilder"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	scanTaskExecuteSQLMaxRetry      = 5
	scanTaskExecuteSQLRetryInterval = 2 * time.Second
	taskStartCheckErrorRateCnt      = 10000
	taskMaxErrorRate                = 0.4
)

type ttlStatistics struct {
	TotalRows   atomic.Uint64
	SuccessRows atomic.Uint64
	ErrorRows   atomic.Uint64
}

func (s *ttlStatistics) IncTotalRows(cnt int) {
	metrics.ScannedExpiredRows.Add(float64(cnt))
	s.TotalRows.Add(uint64(cnt))
}

func (s *ttlStatistics) IncSuccessRows(cnt int) {
	metrics.DeleteSuccessExpiredRows.Add(float64(cnt))
	s.SuccessRows.Add(uint64(cnt))
}

func (s *ttlStatistics) IncErrorRows(cnt int) {
	metrics.DeleteErrorExpiredRows.Add(float64(cnt))
	s.ErrorRows.Add(uint64(cnt))
}

func (s *ttlStatistics) Reset() {
	s.SuccessRows.Store(0)
	s.ErrorRows.Store(0)
	s.TotalRows.Store(0)
}

func (s *ttlStatistics) String() string {
	return fmt.Sprintf("Total Rows: %d, Success Rows: %d, Error Rows: %d", s.TotalRows.Load(), s.SuccessRows.Load(), s.ErrorRows.Load())
}

type ttlScanTask struct {
	ctx context.Context

	*cache.TTLTask

	tbl        *cache.PhysicalTable
	statistics *ttlStatistics
}

type ttlScanTaskExecResult struct {
	task *ttlScanTask
	err  error
}

func (t *ttlScanTask) result(err error) *ttlScanTaskExecResult {
	return &ttlScanTaskExecResult{task: t, err: err}
}

func (t *ttlScanTask) getDatumRows(rows []chunk.Row) [][]types.Datum {
	datums := make([][]types.Datum, len(rows))
	for i, row := range rows {
		datums[i] = row.GetDatumRow(t.tbl.KeyColumnTypes)
	}
	return datums
}

func (t *ttlScanTask) doScan(ctx context.Context, delCh chan<- *ttlDeleteTask, sessPool sessionPool) *ttlScanTaskExecResult {
	// TODO: merge the ctx and the taskCtx in ttl scan task, to allow both "cancel" and gracefully stop workers
	// now, the taskCtx is only check at the beginning of every loop
	taskCtx := t.ctx
	tracer := metrics.PhaseTracerFromCtx(ctx)
	defer tracer.EnterPhase(tracer.Phase())

	tracer.EnterPhase(metrics.PhaseOther)
	rawSess, err := getSession(sessPool)
	if err != nil {
		return t.result(err)
	}
	defer rawSess.Close()

	safeExpire, err := t.tbl.EvalExpireTime(taskCtx, rawSess, rawSess.Now())
	if err != nil {
		return t.result(err)
	}
	safeExpire = safeExpire.Add(time.Minute)

	// Check the expired time to avoid to delete some unexpected rows.
	// It can happen that the table metas used by job and task is different. For example:
	// 	1. Job computes TTL expire time with expired interval 2 days
	//  2. User updates the expired interval to 1 day after job submitted
	//  3. A task deserialized and begins to execute. The expired time it uses is computed by job
	//     but table meta is the latest one that got from the information schema.
	// If we do not make this check, the scan task will continue to run without any error
	// because `ExecuteSQLWithCheck` only do checks when the table meta used by task is different with the latest one.
	// In this case, some rows will be deleted unexpectedly.
	if t.ExpireTime.After(safeExpire) {
		return t.result(errors.Errorf("current expire time is after safe expire time. (%d > %d)", t.ExpireTime.Unix(), safeExpire.Unix()))
	}

	origConcurrency := rawSess.GetSessionVars().DistSQLScanConcurrency()
	if _, err = rawSess.ExecuteSQL(ctx, "set @@tidb_distsql_scan_concurrency=1"); err != nil {
		return t.result(err)
	}

	defer func() {
		_, err = rawSess.ExecuteSQL(ctx, "set @@tidb_distsql_scan_concurrency="+strconv.Itoa(origConcurrency))
		terror.Log(err)
	}()

	sess := newTableSession(rawSess, t.tbl, t.ExpireTime)
	generator, err := sqlbuilder.NewScanQueryGenerator(t.tbl, t.ExpireTime, t.ScanRangeStart, t.ScanRangeEnd)
	if err != nil {
		return t.result(err)
	}

	retrySQL := ""
	retryTimes := 0
	var lastResult [][]types.Datum
	for {
		if err = taskCtx.Err(); err != nil {
			return t.result(err)
		}
		if err = ctx.Err(); err != nil {
			return t.result(err)
		}

		if total := t.statistics.TotalRows.Load(); total > uint64(taskStartCheckErrorRateCnt) {
			if t.statistics.ErrorRows.Load() > uint64(float64(total)*taskMaxErrorRate) {
				return t.result(errors.Errorf("error exceeds the limit"))
			}
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

		sqlStart := time.Now()
		rows, retryable, sqlErr := sess.ExecuteSQLWithCheck(ctx, sql)
		selectInterval := time.Since(sqlStart)
		if sqlErr != nil {
			metrics.SelectErrorDuration.Observe(selectInterval.Seconds())
			needRetry := retryable && retryTimes < scanTaskExecuteSQLMaxRetry && ctx.Err() == nil
			logutil.BgLogger().Error("execute query for ttl scan task failed",
				zap.String("SQL", sql),
				zap.Int("retryTimes", retryTimes),
				zap.Bool("needRetry", needRetry),
				zap.Error(sqlErr),
			)

			if !needRetry {
				return t.result(sqlErr)
			}
			retrySQL = sql
			retryTimes++

			tracer.EnterPhase(metrics.PhaseWaitRetry)
			select {
			case <-ctx.Done():
				return t.result(ctx.Err())
			case <-time.After(scanTaskExecuteSQLRetryInterval):
			}
			tracer.EnterPhase(metrics.PhaseOther)
			continue
		}

		metrics.SelectSuccessDuration.Observe(selectInterval.Seconds())
		retrySQL = ""
		retryTimes = 0
		lastResult = t.getDatumRows(rows)
		if len(rows) == 0 {
			continue
		}

		delTask := &ttlDeleteTask{
			tbl:        t.tbl,
			expire:     t.ExpireTime,
			rows:       lastResult,
			statistics: t.statistics,
		}

		tracer.EnterPhase(metrics.PhaseDispatch)
		select {
		case <-ctx.Done():
			return t.result(ctx.Err())
		case delCh <- delTask:
			t.statistics.IncTotalRows(len(lastResult))
		}
		tracer.EnterPhase(metrics.PhaseOther)
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
	notifyStateCh chan<- any
	sessionPool   sessionPool
}

func newScanWorker(delCh chan<- *ttlDeleteTask, notifyStateCh chan<- any, sessPool sessionPool) *ttlScanWorker {
	w := &ttlScanWorker{
		delCh:         delCh,
		notifyStateCh: notifyStateCh,
		sessionPool:   sessPool,
	}
	w.init(w.loop)
	return w
}

func (w *ttlScanWorker) CouldSchedule() bool {
	w.Lock()
	defer w.Unlock()
	// see `Schedule`. If a `worker.CouldSchedule()` is true, `worker.Schedule` must success
	return w.status == workerStatusRunning && w.curTask == nil && w.curTaskResult == nil
}

func (w *ttlScanWorker) Schedule(task *ttlScanTask) error {
	w.Lock()
	if w.status != workerStatusRunning {
		w.Unlock()
		return errors.New("worker is not running")
	}

	if w.curTaskResult != nil {
		w.Unlock()
		return errors.New("the result of previous task has not been polled")
	}

	if w.curTask != nil {
		w.Unlock()
		return errors.New("a task is running")
	}

	w.curTask = task
	w.curTaskResult = nil
	w.Unlock()
	w.baseWorker.ch <- task
	return nil
}

func (w *ttlScanWorker) CurrentTask() *ttlScanTask {
	w.Lock()
	defer w.Unlock()
	return w.curTask
}

func (w *ttlScanWorker) PollTaskResult() *ttlScanTaskExecResult {
	w.Lock()
	defer w.Unlock()
	if r := w.curTaskResult; r != nil {
		w.curTask = nil
		w.curTaskResult = nil
		return r
	}
	return nil
}

func (w *ttlScanWorker) loop() error {
	ctx := w.baseWorker.ctx
	tracer := metrics.NewScanWorkerPhaseTracer()
	defer func() {
		tracer.EndPhase()
		logutil.BgLogger().Info("ttlScanWorker loop exited.")
	}()

	ticker := time.Tick(time.Second * 5)
	for w.Status() == workerStatusRunning {
		tracer.EnterPhase(metrics.PhaseIdle)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker:
			// ticker is used to update metrics on time
		case msg, ok := <-w.baseWorker.ch:
			tracer.EnterPhase(metrics.PhaseOther)
			if !ok {
				return nil
			}
			switch task := msg.(type) {
			case *ttlScanTask:
				w.handleScanTask(tracer, task)
			default:
				logutil.BgLogger().Warn("unrecognized message for ttlScanWorker", zap.Any("msg", msg))
			}
		}
	}
	return nil
}

func (w *ttlScanWorker) handleScanTask(tracer *metrics.PhaseTracer, task *ttlScanTask) {
	ctx := metrics.CtxWithPhaseTracer(w.ctx, tracer)
	result := task.doScan(ctx, w.delCh, w.sessionPool)
	if result == nil {
		result = task.result(nil)
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

type scanWorker interface {
	worker

	CouldSchedule() bool
	Schedule(*ttlScanTask) error
	PollTaskResult() *ttlScanTaskExecResult
	CurrentTask() *ttlScanTask
}
