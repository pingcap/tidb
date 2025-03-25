// Copyright 2020 PingCAP, Inc.
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

package local

import (
	"container/heap"
	"context"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	putil "github.com/pingcap/tidb/pkg/util"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// jobPrepareAndSendOperator is used to generate jobs and send to downstream operators.
type jobPrepareAndSendOperator struct {
	workerCtx   context.Context
	workerGroup *putil.ErrorGroupWithRecover
	jobWg       *sync.WaitGroup

	local    *Backend
	balancer *storeBalanceOperator
	worker   *jobOperator

	engine          common.Engine
	regionSplitSize int64
	regionSplitKeys int64

	sendErr       error
	sendWg        sync.WaitGroup
	jobToWorkerCh operator.DataChannel[*regionJob]
}

func newJobPrepareAndSendOperator(
	workerCtx context.Context,
	workGroup *putil.ErrorGroupWithRecover,
	jobWg *sync.WaitGroup,
	local *Backend,
	balancer *storeBalanceOperator,
	op *jobOperator,
	engine common.Engine,
	regionSplitSize int64,
	regionSplitKeys int64,
) *jobPrepareAndSendOperator {
	return &jobPrepareAndSendOperator{
		workerCtx:   workerCtx,
		workerGroup: workGroup,
		jobWg:       jobWg,

		local:    local,
		balancer: balancer,
		worker:   op,

		engine:          engine,
		regionSplitSize: regionSplitSize,
		regionSplitKeys: regionSplitKeys,
	}
}

func (*jobPrepareAndSendOperator) String() string {
	return "jobPrepareAndSendOperator"
}

// SetSink implements WithSink interface.
func (sender *jobPrepareAndSendOperator) SetSink(sink operator.DataChannel[*regionJob]) {
	sender.jobToWorkerCh = sink
}

// Open implements Operator interface.
func (sender *jobPrepareAndSendOperator) Open() error {
	sender.sendWg.Add(1)
	sender.workerGroup.Go(sender.generateJobs)
	return nil
}

func (sender *jobPrepareAndSendOperator) generateJobs() error {
	defer sender.sendWg.Done()
	sender.sendErr = sender.local.generateAndSendJob(
		sender.workerCtx,
		sender.engine,
		sender.regionSplitSize,
		sender.regionSplitKeys,
		sender.jobToWorkerCh.Channel(),
		sender.jobWg,
	)
	return sender.sendErr
}

// Close implements Operator interface.
func (sender *jobPrepareAndSendOperator) Close() error {
	// Wait for the sender to stop. This happens when sending is finished,
	// an error occurs during sending, or the context is canceled due to
	// errors in other goroutines.
	sender.sendWg.Wait()

	// If sender meets error internally, other components will exit due do
	// context cancel. We don't need to wait for pending jobs.
	if sender.sendErr != nil {
		return sender.sendErr
	}

	// After sending all jobs, jobWg will be decreased to zero eventually.
	sender.jobWg.Wait()

	// Sanity check
	if sender.balancer != nil {
		intest.AssertFunc(func() bool {
			allZero := true
			sender.balancer.storeLoadMap.Range(func(_, value any) bool {
				if value.(int) != 0 {
					allZero = false
					return false
				}
				return true
			})
			return allZero
		})
	}

	// Now we can safely close the channel.
	sender.worker.close()
	sender.jobToWorkerCh.Finish()
	return sender.workerGroup.Wait()
}

// storeBalancer is used to balance the store load when sending region jobs to
// worker. Internally it maintains a large enough buffer to hold all region jobs,
// and pick the job related to stores that has the least load to send to worker.
// Because it does not have backpressure, it should not be used with external
// engine to avoid OOM.
type storeBalanceOperator struct {
	workerCtx   context.Context
	workerGroup *putil.ErrorGroupWithRecover
	jobWg       *sync.WaitGroup
	local       *Backend

	jobToWorkerDataCh      operator.DataChannel[*regionJob]
	innerJobToWorkerDataCh operator.DataChannel[*regionJob]

	wakeSendToWorker chan struct{}

	// map[uint64]int. 0 can appear in the map after it's decremented to 0.
	storeLoadMap sync.Map
	jobs         sync.Map
	jobIdx       int
}

func newStoreBalanceOperator(
	workerCtx context.Context,
	workerGroup *putil.ErrorGroupWithRecover,
	jobWg *sync.WaitGroup,
	local *Backend,
) *storeBalanceOperator {
	return &storeBalanceOperator{
		workerCtx:   workerCtx,
		workerGroup: workerGroup,
		jobWg:       jobWg,
		local:       local,

		wakeSendToWorker: make(chan struct{}, 1),
	}
}

func (*storeBalanceOperator) String() string {
	return "storeBalanceOperator"
}

// SetSink implements WithSink interface.
func (b *storeBalanceOperator) SetSink(sink operator.DataChannel[*regionJob]) {
	b.innerJobToWorkerDataCh = sink
}

// SetSource implements WithSink interface.
func (b *storeBalanceOperator) SetSource(source operator.DataChannel[*regionJob]) {
	b.jobToWorkerDataCh = source
}

// Open implements Operator interface.
func (b *storeBalanceOperator) Open() error {
	b.workerGroup.Go(b.run)
	return nil
}

// Close implements Operator interface.
func (b *storeBalanceOperator) Close() error {
	return nil
}

func (b *storeBalanceOperator) run() error {
	// all goroutine will not return error except panic, so we make use of
	// ErrorGroupWithRecover.
	eg, ctx2 := util2.NewErrorGroupWithRecoverWithCtx(b.workerCtx)
	sendToWorkerCtx, cancelSendToWorker := context.WithCancel(ctx2)
	eg.Go(func() error {
		b.runReadToWorkerCh(ctx2)
		cancelSendToWorker()
		return nil
	})
	eg.Go(func() error {
		b.runSendToWorker(sendToWorkerCtx)
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	b.jobs.Range(func(_, value any) bool {
		value.(*regionJob).done(b.jobWg)
		return true
	})
	return nil
}

func (b *storeBalanceOperator) runReadToWorkerCh(workerCtx context.Context) {
	jobToWorkerCh := b.jobToWorkerDataCh.Channel()

	for {
		select {
		case <-workerCtx.Done():
			return
		case job, ok := <-jobToWorkerCh:
			if !ok {
				b.innerJobToWorkerDataCh.Finish()
				return
			}
			b.jobs.Store(b.jobIdx, job)
			b.jobIdx++

			select {
			case b.wakeSendToWorker <- struct{}{}:
			default:
			}
		}
	}
}

func (b *storeBalanceOperator) jobLen() int {
	cnt := 0
	b.jobs.Range(func(_, _ any) bool {
		cnt++
		return true
	})
	return cnt
}

func (b *storeBalanceOperator) runSendToWorker(workerCtx context.Context) {
	toWorkerCh := b.innerJobToWorkerDataCh.Channel()

	for {
		select {
		case <-workerCtx.Done():
			return
		case <-b.wakeSendToWorker:
		}

		remainJobCnt := b.jobLen()
		for i := 0; i < remainJobCnt; i++ {
			j := b.pickJob()
			if j == nil {
				// j can be nil if it's executed after the jobs.Store of runReadToWorkerCh
				// and before the sending to wakeSendToWorker of runReadToWorkerCh.
				break
			}

			// after the job is picked and before the job is sent to worker, the score may
			// have changed so we should pick again to get the optimal job. However for
			// simplicity we don't do it. The optimal job will be picked in the next round.
			select {
			case <-workerCtx.Done():
				j.done(b.jobWg)
				if j.region != nil && j.region.Region != nil {
					b.releaseStoreLoad(j.region.Region.Peers)
				}
				return
			case toWorkerCh <- j:
			}
		}
	}
}

func (b *storeBalanceOperator) pickJob() *regionJob {
	var (
		best     *regionJob
		bestIdx  = -1
		minScore = math.MaxInt64
	)
	b.jobs.Range(func(key, value any) bool {
		idx := key.(int)
		job := value.(*regionJob)

		score := 0
		// in unit tests, the fields of job may not set
		if job.region == nil || job.region.Region == nil {
			best = job
			bestIdx = idx
			return false
		}

		for _, p := range job.region.Region.Peers {
			if v, ok := b.storeLoadMap.Load(p.StoreId); ok {
				score += v.(int)
			}
		}

		if score == 0 {
			best = job
			bestIdx = idx
			return false
		}
		if score < minScore {
			minScore = score
			best = job
			bestIdx = idx
		}
		return true
	})
	if bestIdx == -1 {
		return nil
	}

	b.jobs.Delete(bestIdx)
	// in unit tests, the fields of job may not set
	if best.region == nil || best.region.Region == nil {
		return best
	}

	for _, p := range best.region.Region.Peers {
	retry:
		val, loaded := b.storeLoadMap.LoadOrStore(p.StoreId, 1)
		if !loaded {
			continue
		}

		old := val.(int)
		if !b.storeLoadMap.CompareAndSwap(p.StoreId, old, old+1) {
			// retry the whole check because the entry may have been deleted
			goto retry
		}
	}
	return best
}

func (b *storeBalanceOperator) releaseStoreLoad(peers []*metapb.Peer) {
	for _, p := range peers {
	retry:
		val, ok := b.storeLoadMap.Load(p.StoreId)
		if !ok {
			intest.Assert(false,
				"missing key in storeLoadMap. key: %d",
				p.StoreId,
			)
			log.L().Error("missing key in storeLoadMap",
				zap.Uint64("storeID", p.StoreId))
			continue
		}

		old := val.(int)
		if !b.storeLoadMap.CompareAndSwap(p.StoreId, old, old-1) {
			goto retry
		}
	}
}

// regionJobRetryer is a concurrent-safe queue holding jobs that need to put
// back later, and put back when the regionJob.waitUntil is reached. It maintains
// a heap of jobs internally based on the regionJob.waitUntil field.
type jobDispatchOperator struct {
	// lock acquiring order: protectedClosed > protectedQueue > protectedToPutBack
	protectedClosed struct {
		mu     sync.Mutex
		closed bool
	}
	protectedQueue struct {
		mu sync.Mutex
		q  regionJobRetryHeap
	}
	protectedToPutBack struct {
		mu        sync.Mutex
		toPutBack *regionJob
	}
	reload chan struct{}

	jobPutBackDataCh    operator.DataChannel[*regionJob]
	jobFromWorkerDataCh operator.DataChannel[*regionJob]

	workerCtx   context.Context
	workerGroup *putil.ErrorGroupWithRecover
	jobWg       *sync.WaitGroup

	retryCtx      context.Context
	cancelRetryer context.CancelFunc

	local    *Backend
	balancer *storeBalanceOperator
}

// newRegionJobRetryer creates a regionJobRetryer. regionJobRetryer.run is
// expected to be called soon.
func newJobDispatchOperator(
	workerCtx context.Context,
	workerGroup *putil.ErrorGroupWithRecover,
	jobWg *sync.WaitGroup,
	local *Backend,
	balancer *storeBalanceOperator,
) *jobDispatchOperator {
	retryCtx, cancelRetryer := context.WithCancel(workerCtx)

	ret := &jobDispatchOperator{
		reload: make(chan struct{}, 1),

		workerCtx:   workerCtx,
		workerGroup: workerGroup,
		jobWg:       jobWg,
		local:       local,
		balancer:    balancer,

		retryCtx:      retryCtx,
		cancelRetryer: cancelRetryer,
	}
	ret.protectedQueue.q = make(regionJobRetryHeap, 0, 16)
	return ret
}

func (*jobDispatchOperator) String() string {
	return "jobRetryOperator"
}

// SetSink implements WithSink interface.
func (q *jobDispatchOperator) SetSink(sink operator.DataChannel[*regionJob]) {
	q.jobPutBackDataCh = sink
}

// SetSource implements SetSource interface.
func (q *jobDispatchOperator) SetSource(source operator.DataChannel[*regionJob]) {
	q.jobFromWorkerDataCh = source
}

// Open implements Operator interface.
func (q *jobDispatchOperator) Open() error {
	q.workerGroup.Go(q.retryJobLoop)
	q.workerGroup.Go(q.fetchJobLoop)
	return nil
}

// Close implements Operator interface.
func (q *jobDispatchOperator) Close() error {
	err := q.workerGroup.Wait()
	if err == nil {
		intest.AssertFunc(func() bool {
			q.protectedToPutBack.mu.Lock()
			defer q.protectedToPutBack.mu.Unlock()
			return q.protectedToPutBack.toPutBack == nil
		}, "toPutBack should be nil considering it's happy path")
		intest.AssertFunc(func() bool {
			q.protectedQueue.mu.Lock()
			defer q.protectedQueue.mu.Unlock()
			return len(q.protectedQueue.q) == 0
		}, "queue should be empty considering it's happy path")
	}
	return err
}

// run occupies the goroutine and starts the retry loop. Cancel the `retryCtx` will
// stop retryer and `jobWg.Done` will be trigger for jobs that are not put back
// yet. It should only be used in error case.
func (q *jobDispatchOperator) retryJobLoop() error {
	defer q.cleanupUnprocessedJobs()

	jobPutBackCh := q.jobPutBackDataCh.Channel()

	for {
		var front *regionJob
		q.protectedQueue.mu.Lock()
		if len(q.protectedQueue.q) > 0 {
			front = q.protectedQueue.q[0]
		}
		q.protectedQueue.mu.Unlock()

		switch {
		case front != nil:
			select {
			case <-q.retryCtx.Done():
				return nil
			case <-q.reload:
			case <-time.After(time.Until(front.waitUntil)):
				q.protectedQueue.mu.Lock()
				q.protectedToPutBack.mu.Lock()
				q.protectedToPutBack.toPutBack = heap.Pop(&q.protectedQueue.q).(*regionJob)
				// release the lock of queue to avoid blocking regionJobRetryer.push
				q.protectedQueue.mu.Unlock()

				// hold the lock of toPutBack to make sending to putBackCh and
				// resetting toPutBack atomic w.r.t. regionJobRetryer.close
				select {
				case <-q.retryCtx.Done():
					q.protectedToPutBack.mu.Unlock()
					return nil
				case jobPutBackCh <- q.protectedToPutBack.toPutBack:
					q.protectedToPutBack.toPutBack = nil
					q.protectedToPutBack.mu.Unlock()
				}
			}
		default:
			select {
			case <-q.retryCtx.Done():
				return nil
			case <-q.reload:
			}
		}
	}
}

// fetchJobLoop get executed job from channel and retry failed jobs.
func (q *jobDispatchOperator) fetchJobLoop() error {
	var (
		jobFromWorkerCh = q.jobFromWorkerDataCh.Channel()
		job             *regionJob
		ok              bool
	)
	for {
		select {
		case <-q.workerCtx.Done():
			return nil
		case job, ok = <-jobFromWorkerCh:
		}
		if !ok {
			q.cancelRetryer()
			return nil
		}
		switch job.stage {
		case regionScanned, wrote:
			job.retryCount++
			if job.retryCount > MaxWriteAndIngestRetryTimes {
				job.done(q.jobWg)
				lastErr := job.lastRetryableErr
				intest.Assert(lastErr != nil, "lastRetryableErr should not be nil")
				if lastErr == nil {
					lastErr = errors.New("retry limit exceeded")
					log.FromContext(q.workerCtx).Error(
						"lastRetryableErr should not be nil",
						logutil.Key("startKey", job.keyRange.Start),
						logutil.Key("endKey", job.keyRange.End),
						zap.Stringer("stage", job.stage),
						zap.Error(lastErr))
				}
				return lastErr
			}
			// max retry backoff time: 2+4+8+16+30*26=810s
			sleepSecond := math.Pow(2, float64(job.retryCount))
			if sleepSecond > float64(maxRetryBackoffSecond) {
				sleepSecond = float64(maxRetryBackoffSecond)
			}
			job.waitUntil = time.Now().Add(time.Second * time.Duration(sleepSecond))
			log.FromContext(q.workerCtx).Info("put job back to jobCh to retry later",
				logutil.Key("startKey", job.keyRange.Start),
				logutil.Key("endKey", job.keyRange.End),
				zap.Stringer("stage", job.stage),
				zap.Int("retryCount", job.retryCount),
				zap.Time("waitUntil", job.waitUntil))
			if !q.push(job) {
				// retryer is closed by worker error
				job.done(q.jobWg)
			}
		case ingested:
			job.done(q.jobWg)
		case needRescan:
			panic("should not reach here")
		}
	}
}

// cleanupUnprocessedJobs is only internally used, caller should not use it.
func (q *jobDispatchOperator) cleanupUnprocessedJobs() {
	q.protectedClosed.mu.Lock()
	defer q.protectedClosed.mu.Unlock()
	q.protectedClosed.closed = true

	if q.protectedToPutBack.toPutBack != nil {
		q.protectedToPutBack.toPutBack.done(q.jobWg)
	}
	for _, job := range q.protectedQueue.q {
		job.done(q.jobWg)
	}
}

// push should not be blocked for long time in any cases.
func (q *jobDispatchOperator) push(job *regionJob) bool {
	q.protectedClosed.mu.Lock()
	defer q.protectedClosed.mu.Unlock()
	if q.protectedClosed.closed {
		return false
	}

	q.protectedQueue.mu.Lock()
	heap.Push(&q.protectedQueue.q, job)
	q.protectedQueue.mu.Unlock()

	select {
	case q.reload <- struct{}{}:
	default:
	}
	return true
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (r *regionJob) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return "region job", "RecoverArgs", func() {}, false
}

type jobWorker struct {
	ctx   context.Context
	jobWg *sync.WaitGroup
	op    *jobOperator

	local    *Backend
	balancer *storeBalanceOperator
}

func (w *jobWorker) getAfterExecuteJob() func([]*metapb.Peer) {
	if w.balancer != nil {
		return w.balancer.releaseStoreLoad
	}
	return nil
}

func (w *jobWorker) HandleTask(job *regionJob, sender func(*regionJob)) {
	failpoint.Inject("injectPanicForTableScan", func() {
		panic("mock panic")
	})

	if w.op.hasError() {
		return
	}

	ctx := w.ctx
	afterExecuteJob := w.getAfterExecuteJob()

	var peers []*metapb.Peer
	// in unit test, we may not have the real peers
	if job.region != nil && job.region.Region != nil {
		peers = job.region.Region.GetPeers()
	}
	failpoint.InjectCall("beforeExecuteRegionJob", ctx)
	metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Inc()
	err := w.local.executeJob(ctx, job)
	metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Dec()

	if afterExecuteJob != nil {
		afterExecuteJob(peers)
	}
	switch job.stage {
	case regionScanned, wrote, ingested:
		select {
		case <-ctx.Done():
			job.done(w.jobWg)
			return
		default:
			sender(job)
		}
	case needRescan:
		jobs, err2 := w.local.generateJobForRange(
			ctx,
			job.ingestData,
			[]common.Range{job.keyRange},
			job.regionSplitSize,
			job.regionSplitKeys,
		)
		if err2 != nil {
			// Don't need to put the job back to retry, because generateJobForRange
			// has done the retry internally. Here just done for the "needRescan"
			// job and exit directly.
			job.done(w.jobWg)
			w.op.onError(err2)
			return
		}
		// 1 "needRescan" job becomes len(jobs) "regionScanned" jobs.
		newJobCnt := len(jobs) - 1
		for newJobCnt > 0 {
			job.ref(w.jobWg)
			newJobCnt--
		}
		for _, j := range jobs {
			j.lastRetryableErr = job.lastRetryableErr
			select {
			case <-ctx.Done():
				j.done(w.jobWg)
				// don't exit here, we mark done for each job and exit in the outer loop
			default:
				sender(j)
			}
		}
	}

	if err != nil {
		w.op.onError(err)
	}
}

func (w *jobWorker) Close() {}

type jobOperator struct {
	*operator.AsyncOperator[*regionJob, *regionJob]

	workerGroup *putil.ErrorGroupWithRecover
	jobWg       *sync.WaitGroup

	logCtx context.Context
	subCtx context.Context
	cancel context.CancelFunc

	firstErr atomic.Error
	errCh    chan error
}

func newJobOperator(
	workerCtx context.Context,
	workGroup *putil.ErrorGroupWithRecover,
	jobWg *sync.WaitGroup,
	concurrency int,
	local *Backend,
	balancer *storeBalanceOperator,
) *jobOperator {
	subCtx, cancel := context.WithCancel(workerCtx)

	op := &jobOperator{
		logCtx:      workerCtx,
		workerGroup: workGroup,
		subCtx:      subCtx,
		cancel:      cancel,
		errCh:       make(chan error),
	}

	pool := workerpool.NewWorkerPool(
		"IngestWorkerOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[*regionJob, *regionJob] {
			return &jobWorker{
				ctx:      subCtx,
				jobWg:    jobWg,
				op:       op,
				local:    local,
				balancer: balancer,
			}
		})

	op.AsyncOperator = operator.NewAsyncOperator(subCtx, pool)
	return op
}

func (*jobOperator) String() string {
	return "jobOperator"
}

func (j *jobOperator) Open() error {
	j.workerGroup.Go(func() error {
		for err := range j.errCh {
			if j.firstErr.CompareAndSwap(nil, err) {
				return j.firstErr.Load()
			} else {
				if errors.Cause(err) != context.Canceled {
					log.FromContext(j.logCtx).Error("error on encode and sort", zap.Error(err))
				}
			}
		}
		return nil
	})
	return j.AsyncOperator.Open()
}

func (j *jobOperator) close() {
	j.cancel()
	j.AsyncOperator.Close()
	close(j.errCh)
}

func (j *jobOperator) Close() error {
	return j.firstErr.Load()
}

func (j *jobOperator) onError(err error) {
	j.errCh <- err
}

func (j *jobOperator) hasError() bool {
	return j.firstErr.Load() != nil
}
