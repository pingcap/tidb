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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/ttl/sqlbuilder"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	delMaxRetry        = 3
	delRetryBufferSize = 128
	delRetryInterval   = time.Second * 5
)

type delRateLimiter interface {
	WaitDelToken(ctx context.Context) error
}

var globalDelRateLimiter = newDelRateLimiter()

type defaultDelRateLimiter struct {
	sync.Mutex
	// limiter limits the rate of delete operation.
	// limit.Limit() has a range [1.0, +rate.Inf].
	// When the value of system variable `tidb_ttl_delete_rate_limit` is `0`, `limit.Limit()` returns `rate.Inf`.
	limiter *rate.Limiter
	// limit is the rate limit of the limiter that is the same value of system variable `tidb_ttl_delete_rate_limit`.
	// When it is 0, it means unlimited and `limiter.Limit()` will return `rate.Inf`.
	limit atomic.Int64
}

func newDelRateLimiter() delRateLimiter {
	limiter := &defaultDelRateLimiter{}
	limiter.limiter = rate.NewLimiter(rate.Inf, 1)
	limiter.limit.Store(0)
	return limiter
}

type beforeWaitLimiterForTestType struct{}

var beforeWaitLimiterForTest = &beforeWaitLimiterForTestType{}

func (l *defaultDelRateLimiter) WaitDelToken(ctx context.Context) error {
	limit := l.limit.Load()
	if variable.TTLDeleteRateLimit.Load() != limit {
		limit = l.reset()
	}

	intest.Assert(limit >= 0)
	if limit <= 0 {
		return ctx.Err()
	}

	if intest.InTest {
		intest.Assert(l.limiter.Limit() > 0)
		if fn, ok := ctx.Value(beforeWaitLimiterForTest).(func()); ok {
			fn()
		}
	}

	return l.limiter.Wait(ctx)
}

func (l *defaultDelRateLimiter) reset() (newLimit int64) {
	l.Lock()
	defer l.Unlock()
	newLimit = variable.TTLDeleteRateLimit.Load()
	if newLimit != l.limit.Load() {
		l.limit.Store(newLimit)
		rateLimit := rate.Inf
		if newLimit > 0 {
			// When `TTLDeleteRateLimit > 0`, use the setting as the rate limit.
			// Otherwise, use `rate.Inf` to make it unlimited.
			rateLimit = rate.Limit(newLimit)
		}
		l.limiter.SetLimit(rateLimit)
	}
	return
}

type ttlDeleteTask struct {
<<<<<<< HEAD
=======
	jobID      string
	scanID     int64
	jobType    session.TTLJobType
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
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
	defer func() {
		if len(leftRows) > 0 {
			retryRows = append(retryRows, leftRows...)
		}
	}()

	se, err := newTableSession(rawSe, t.tbl, t.expire, t.jobType)
	if err != nil {
		t.statistics.IncErrorRows(t.jobType, len(leftRows))
		t.taskLogger(logutil.Logger(ctx)).Warn(
			"create ttl table session failed",
			zap.Error(err),
		)
		return
	}

	for len(leftRows) > 0 && ctx.Err() == nil {
		maxBatch := variable.TTLDeleteBatchSize.Load()
		var delBatch [][]types.Datum
		if int64(len(leftRows)) < maxBatch {
			delBatch = leftRows
			leftRows = nil
		} else {
			delBatch = leftRows[0:maxBatch]
			leftRows = leftRows[maxBatch:]
		}

		minCheckpointTS := uint64(0)
		if t.jobType == session.TTLJobTypeSoftDelete && t.tbl.TableInfo.IsActiveActive {
			minCheckpointTS, err = rawSe.GetMinActiveActiveCheckpointTS(
				ctx,
				t.tbl.Schema.O,
				t.tbl.Name.O,
			)
			if err != nil {
				t.statistics.IncErrorRows(t.jobType, len(leftRows))
				t.taskLogger(logutil.Logger(ctx)).Warn("get ticdc min checkpoint ts failed", zap.Error(err))
				return
			}
		}

		sql, err := sqlbuilder.BuildDeleteSQL(t.tbl, t.jobType, delBatch, t.expire, minCheckpointTS)
		if err != nil {
<<<<<<< HEAD
			t.statistics.IncErrorRows(len(delBatch))
			logutil.BgLogger().Warn(
=======
			t.statistics.IncErrorRows(t.jobType, len(delBatch))
			t.taskLogger(logutil.Logger(ctx)).Warn(
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
				"build delete SQL in TTL failed",
				zap.Error(err),
				zap.String("table", t.tbl.Schema.O+"."+t.tbl.Name.O),
			)
			return
		}

		tracer.EnterPhase(metrics.PhaseWaitToken)
		if err = globalDelRateLimiter.WaitDelToken(ctx); err != nil {
			tracer.EnterPhase(metrics.PhaseOther)
			logutil.BgLogger().Info(
				"wait TTL delete rate limiter interrupted",
				zap.Error(err),
				zap.Int("waitDelRowCnt", len(delBatch)),
			)
			retryRows = append(retryRows, delBatch...)
			continue
		}
		tracer.EnterPhase(metrics.PhaseOther)

		sqlStart := time.Now()
		_, needRetry, err := se.ExecuteSQLWithCheck(ctx, sql)
		sqlInterval := time.Since(sqlStart)
		if err != nil {
<<<<<<< HEAD
			metrics.DeleteErrorDuration.Observe(sqlInterval.Seconds())
			logutil.BgLogger().Warn(
=======
			metrics.QueryDuration(metrics.SQLTypeDelete, t.jobType, false).Observe(sqlInterval.Seconds())
			t.taskLogger(logutil.Logger(ctx)).Warn(
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
				"delete SQL in TTL failed",
				zap.Error(err),
				zap.String("SQL", sql),
				zap.String("jobType", t.jobType),
				zap.Bool("needRetry", needRetry),
			)

			if needRetry {
				if retryRows == nil {
					retryRows = make([][]types.Datum, 0, len(leftRows)+len(delBatch))
				}
				retryRows = append(retryRows, delBatch...)
			} else {
				t.statistics.IncErrorRows(t.jobType, len(delBatch))
			}
			continue
		}

		metrics.QueryDuration(metrics.SQLTypeDelete, t.jobType, true).Observe(sqlInterval.Seconds())
		t.statistics.IncSuccessRows(t.jobType, len(delBatch))
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
	l := b.list.Len()
	// When `retryInterval==0`, to avoid the infinite retries, limit the max loop to the buffer length.
	// It means one item only has one chance to retry in one `DoRetry` invoking.
	for i := 0; i < l; i++ {
		ele := b.list.Front()
		if ele == nil {
			break
		}
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

// SetRetryInterval sets the retry interval of the buffer.
func (b *ttlDelRetryBuffer) SetRetryInterval(interval time.Duration) {
	b.retryInterval = interval
}

// Drain drains a retry buffer.
func (b *ttlDelRetryBuffer) Drain() {
	for ele := b.list.Front(); ele != nil; ele = ele.Next() {
		if item, ok := ele.Value.(*ttlDelRetryItem); ok {
			item.task.statistics.IncErrorRows(item.task.jobType, len(item.task.rows))
		} else {
			logutil.BgLogger().Error(fmt.Sprintf("invalid retry buffer item type: %T", ele))
		}
	}
	b.list = list.New()
}

func (b *ttlDelRetryBuffer) recordRetryItem(task *ttlDeleteTask, retryRows [][]types.Datum, retryCnt int) bool {
	if len(retryRows) == 0 {
		return false
	}

	if retryCnt >= b.maxRetry {
<<<<<<< HEAD
		task.statistics.IncErrorRows(len(retryRows))
=======
		task.taskLogger(logutil.BgLogger()).Warn(
			"discard TTL rows that has failed more than maxRetry times",
			zap.Int("rowCnt", len(retryRows)),
			zap.Int("retryCnt", retryCnt),
		)
		task.statistics.IncErrorRows(task.jobType, len(retryRows))
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
		return false
	}

	for b.list.Len() > 0 && b.list.Len() >= b.maxSize {
		ele := b.list.Front()
		if item, ok := ele.Value.(*ttlDelRetryItem); ok {
<<<<<<< HEAD
			item.task.statistics.IncErrorRows(len(item.task.rows))
=======
			task.taskLogger(logutil.BgLogger()).Warn(
				"discard TTL rows because the retry buffer is full",
				zap.Int("rowCnt", len(retryRows)),
				zap.Int("bufferSize", b.list.Len()),
			)
			item.task.statistics.IncErrorRows(task.jobType, len(item.task.rows))
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
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
	sessionPool util.SessionPool
	retryBuffer *ttlDelRetryBuffer
}

func newDeleteWorker(delCh <-chan *ttlDeleteTask, sessPool util.SessionPool) *ttlDeleteWorker {
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
	defer se.Close()

	ctx := metrics.CtxWithPhaseTracer(w.baseWorker.ctx, tracer)

	doRetry := func(task *ttlDeleteTask) [][]types.Datum {
		return task.doDelete(ctx, se)
	}

	timer := time.NewTimer(w.retryBuffer.retryInterval)
	defer timer.Stop()

	defer func() {
		// Have a final try to delete all rows in retry buffer while the worker stops
		// to avoid leaving any TTL rows undeleted when shrinking the delete worker.
		if w.retryBuffer.Len() > 0 {
			start := time.Now()
			log.Info(
				"try to delete TTL rows in del worker buffer immediately because the worker is going to stop",
				zap.Int("bufferLen", w.retryBuffer.Len()),
			)
			retryCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			w.retryBuffer.SetRetryInterval(0)
			w.retryBuffer.DoRetry(func(task *ttlDeleteTask) [][]types.Datum {
				return task.doDelete(retryCtx, se)
			})
			log.Info(
				"delete TTL rows in del worker buffer finished",
				zap.Duration("duration", time.Since(start)),
			)
		}

		// drain retry buffer to make sure the statistics are correct
		if w.retryBuffer.Len() > 0 {
			log.Warn(
				"some TTL rows are still in the buffer while the worker is going to stop, mark them as error",
				zap.Int("bufferLen", w.retryBuffer.Len()),
			)
			w.retryBuffer.Drain()
		}
	}()

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
