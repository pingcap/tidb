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
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/metrics"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/ttl/sqlbuilder"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	delMaxRetry        = 3
	delRetryBufferSize = 128
	delRetryInterval   = time.Second * 5
)

var globalDelRateLimiter = newDelRateLimiter()

type delRateLimiter struct {
	sync.Mutex
	limiter *rate.Limiter
	limit   atomic.Int64
}

func newDelRateLimiter() *delRateLimiter {
	limiter := &delRateLimiter{}
	limiter.limiter = rate.NewLimiter(0, 1)
	limiter.limit.Store(0)
	return limiter
}

func (l *delRateLimiter) Wait(ctx context.Context) error {
	limit := l.limit.Load()
	if variable.TTLDeleteRateLimit.Load() != limit {
		limit = l.reset()
	}

	if limit == 0 {
		return ctx.Err()
	}

	return l.limiter.Wait(ctx)
}

func (l *delRateLimiter) reset() (newLimit int64) {
	l.Lock()
	defer l.Unlock()
	newLimit = variable.TTLDeleteRateLimit.Load()
	if newLimit != l.limit.Load() {
		l.limit.Store(newLimit)
		l.limiter.SetLimit(rate.Limit(newLimit))
	}
	return
}

type ttlDeleteTask struct {
	tbl        *cache.PhysicalTable
	expire     time.Time
	rows       [][]types.Datum
	statistics *ttlStatistics
}

func (t *ttlDeleteTask) doDelete(ctx context.Context, rawSe session.Session) (retryRows [][]types.Datum) {
	tracer := metrics.PhaseTracerFromCtx(ctx)
	defer tracer.EnterPhase(tracer.Phase())
	tracer.EnterPhase(metrics.PhaseOther)

	leftRows := t.rows
	se := newTableSession(rawSe, t.tbl, t.expire)
	for len(leftRows) > 0 {
		maxBatch := variable.TTLDeleteBatchSize.Load()
		var delBatch [][]types.Datum
		if int64(len(leftRows)) < maxBatch {
			delBatch = leftRows
			leftRows = nil
		} else {
			delBatch = leftRows[0:maxBatch]
			leftRows = leftRows[maxBatch:]
		}

		sql, err := sqlbuilder.BuildDeleteSQL(t.tbl, delBatch, t.expire)
		if err != nil {
			t.statistics.IncErrorRows(len(delBatch))
			logutil.BgLogger().Warn(
				"build delete SQL in TTL failed",
				zap.Error(err),
				zap.String("table", t.tbl.Schema.O+"."+t.tbl.Name.O),
			)
			return
		}

		tracer.EnterPhase(metrics.PhaseWaitToken)
		if err = globalDelRateLimiter.Wait(ctx); err != nil {
			t.statistics.IncErrorRows(len(delBatch))
			return
		}
		tracer.EnterPhase(metrics.PhaseOther)

		sqlStart := time.Now()
		_, needRetry, err := se.ExecuteSQLWithCheck(ctx, sql)
		sqlInterval := time.Since(sqlStart)
		if err != nil {
			metrics.DeleteErrorDuration.Observe(sqlInterval.Seconds())
			needRetry = needRetry && ctx.Err() == nil
			logutil.BgLogger().Warn(
				"delete SQL in TTL failed",
				zap.Error(err),
				zap.String("SQL", sql),
				zap.Bool("needRetry", needRetry),
			)

			if needRetry {
				if retryRows == nil {
					retryRows = make([][]types.Datum, 0, len(leftRows)+len(delBatch))
				}
				retryRows = append(retryRows, delBatch...)
			} else {
				t.statistics.IncErrorRows(len(delBatch))
			}
			continue
		}

		metrics.DeleteSuccessDuration.Observe(sqlInterval.Seconds())
		t.statistics.IncSuccessRows(len(delBatch))
	}
	return retryRows
}

type ttlDelRetryItem struct {
	task     *ttlDeleteTask
	retryCnt int
	inTime   time.Time
}

type ttlDelRetryBuffer struct {
	list          *list.List
	maxSize       int
	maxRetry      int
	retryInterval time.Duration
	getTime       func() time.Time
}

func newTTLDelRetryBuffer() *ttlDelRetryBuffer {
	return &ttlDelRetryBuffer{
		list:          list.New(),
		maxSize:       delRetryBufferSize,
		maxRetry:      delMaxRetry,
		retryInterval: delRetryInterval,
		getTime:       time.Now,
	}
}

func (b *ttlDelRetryBuffer) Len() int {
	return b.list.Len()
}

func (b *ttlDelRetryBuffer) RecordTaskResult(task *ttlDeleteTask, retryRows [][]types.Datum) {
	b.recordRetryItem(task, retryRows, 0)
}

func (b *ttlDelRetryBuffer) DoRetry(do func(*ttlDeleteTask) [][]types.Datum) time.Duration {
	for b.list.Len() > 0 {
		ele := b.list.Front()
		item, ok := ele.Value.(*ttlDelRetryItem)
		if !ok {
			logutil.BgLogger().Error(fmt.Sprintf("invalid retry buffer item type: %T", ele))
			b.list.Remove(ele)
			continue
		}

		now := b.getTime()
		interval := now.Sub(item.inTime)
		if interval < b.retryInterval {
			return b.retryInterval - interval
		}

		b.list.Remove(ele)
		if retryRows := do(item.task); len(retryRows) > 0 {
			b.recordRetryItem(item.task, retryRows, item.retryCnt+1)
		}
	}
	return b.retryInterval
}

func (b *ttlDelRetryBuffer) recordRetryItem(task *ttlDeleteTask, retryRows [][]types.Datum, retryCnt int) bool {
	if len(retryRows) == 0 {
		return false
	}

	if retryCnt >= b.maxRetry {
		task.statistics.IncErrorRows(len(retryRows))
		return false
	}

	for b.list.Len() > 0 && b.list.Len() >= b.maxSize {
		ele := b.list.Front()
		if item, ok := ele.Value.(*ttlDelRetryItem); ok {
			item.task.statistics.IncErrorRows(len(item.task.rows))
		} else {
			logutil.BgLogger().Error(fmt.Sprintf("invalid retry buffer item type: %T", ele))
		}
		b.list.Remove(b.list.Front())
	}

	newTask := *task
	newTask.rows = retryRows
	b.list.PushBack(&ttlDelRetryItem{
		task:     &newTask,
		inTime:   b.getTime(),
		retryCnt: retryCnt,
	})
	return true
}

type ttlDeleteWorker struct {
	baseWorker
	delCh       <-chan *ttlDeleteTask
	sessionPool sessionPool
	retryBuffer *ttlDelRetryBuffer
}

func newDeleteWorker(delCh <-chan *ttlDeleteTask, sessPool sessionPool) *ttlDeleteWorker {
	w := &ttlDeleteWorker{
		delCh:       delCh,
		sessionPool: sessPool,
		retryBuffer: newTTLDelRetryBuffer(),
	}
	w.init(w.loop)
	return w
}

func (w *ttlDeleteWorker) loop() error {
	tracer := metrics.NewDeleteWorkerPhaseTracer()
	defer func() {
		tracer.EndPhase()
		logutil.BgLogger().Info("ttlDeleteWorker loop exited.")
	}()

	tracer.EnterPhase(metrics.PhaseOther)
	se, err := getSession(w.sessionPool)
	if err != nil {
		return err
	}

	ctx := metrics.CtxWithPhaseTracer(w.baseWorker.ctx, tracer)

	doRetry := func(task *ttlDeleteTask) [][]types.Datum {
		return task.doDelete(ctx, se)
	}

	timer := time.NewTimer(w.retryBuffer.retryInterval)
	defer timer.Stop()

	for w.Status() == workerStatusRunning {
		tracer.EnterPhase(metrics.PhaseIdle)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			tracer.EnterPhase(metrics.PhaseOther)
			nextInterval := w.retryBuffer.DoRetry(doRetry)
			timer.Reset(nextInterval)
		case task, ok := <-w.delCh:
			tracer.EnterPhase(metrics.PhaseOther)
			if !ok {
				return nil
			}
			retryRows := task.doDelete(ctx, se)
			w.retryBuffer.RecordTaskResult(task, retryRows)
		}
	}
	return nil
}
