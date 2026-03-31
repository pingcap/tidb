package pkdbremoteexec

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	tidbutil "github.com/pingcap/tidb/pkg/util"
)

// remoteExecWorkerPool is an elastic worker pool for remoteExecTask.
//
// Design goals:
//  1. Reuse goroutines (avoid per-request goroutine creation / morestack overhead).
//  2. Keep a stable baseline number of workers for steady load.
//  3. Burst by spawning extra workers when all workers are busy.
//  4. Reclaim extra workers after they have been idle for some time.
//
// This pool uses a buffered channel to reduce head-of-line blocking:
// tasks can be queued in the buffer when all workers are busy, reducing latency spikes.
// When the buffer is also full, submitting blocks until a worker becomes available.
type remoteExecWorkerPool struct {
	minWorkers     int32
	baseMaxWorkers int32
	hardMaxWorkers int32
	idleTimeout    time.Duration
	bufferSize     int32

	tasks chan *remoteExecTask

	stopCh    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	workerCount int32 // atomic
	softMax     int32 // atomic
}

// defaultWorkerPoolBufferSize is the default buffer size for the task channel.
// This allows tasks to queue without blocking when workers are temporarily busy.
const defaultWorkerPoolBufferSize = 128

func newRemoteExecWorkerPool(minWorkers, baseMaxWorkers, hardMaxWorkers int, idleTimeout time.Duration) *remoteExecWorkerPool {
	return newRemoteExecWorkerPoolWithBuffer(minWorkers, baseMaxWorkers, hardMaxWorkers, idleTimeout, defaultWorkerPoolBufferSize)
}

func newRemoteExecWorkerPoolWithBuffer(minWorkers, baseMaxWorkers, hardMaxWorkers int, idleTimeout time.Duration, bufferSize int) *remoteExecWorkerPool {
	if minWorkers <= 0 {
		minWorkers = 1
	}
	if baseMaxWorkers < minWorkers {
		baseMaxWorkers = minWorkers
	}
	if hardMaxWorkers < baseMaxWorkers {
		hardMaxWorkers = baseMaxWorkers
	}
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	if bufferSize < 0 {
		bufferSize = 0
	}

	p := &remoteExecWorkerPool{
		minWorkers:     int32(minWorkers),
		baseMaxWorkers: int32(baseMaxWorkers),
		hardMaxWorkers: int32(hardMaxWorkers),
		idleTimeout:    idleTimeout,
		bufferSize:     int32(bufferSize),
		tasks:          make(chan *remoteExecTask, bufferSize),
		stopCh:         make(chan struct{}),
		workerCount:    int32(minWorkers),
		softMax:        int32(baseMaxWorkers),
	}

	// Initialize metrics
	metrics.RemotePlanWorkerPoolHardMax.Set(float64(hardMaxWorkers))
	metrics.RemotePlanWorkerPoolSoftMax.Set(float64(baseMaxWorkers))
	metrics.RemotePlanWorkerPoolWorkers.Set(float64(minWorkers))
	metrics.RemotePlanWorkerPoolQueueLen.Set(0)

	for i := 0; i < minWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(true /* core */)
	}
	return p
}

func (p *remoteExecWorkerPool) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		close(p.stopCh)
		p.wg.Wait()
	})
}

// Submit hands off the task to an idle worker or queues it in the buffer.
// If the buffer is full, it tries to spawn extra workers (up to soft/hard max).
// It returns false if ctx is done or the pool is closed before the task is accepted.
func (p *remoteExecWorkerPool) Submit(ctx context.Context, task *remoteExecTask) bool {
	if p == nil || task == nil {
		return false
	}

	// Check if pool is stopped
	select {
	case <-p.stopCh:
		return false
	default:
	}

	// Fast path: try non-blocking send to buffer/worker
	select {
	case p.tasks <- task:
		// Update queue length metric (approximate, as workers consume concurrently)
		metrics.RemotePlanWorkerPoolQueueLen.Set(float64(len(p.tasks)))
		return true
	default:
	}

	// Buffer is full (or no idle worker for unbuffered). Record this event.
	metrics.RemotePlanWorkerPoolQueueFull.Inc()

	// Try to spawn an extra worker to help drain the queue.
	softMax := atomic.LoadInt32(&p.softMax)
	if !p.trySpawnWorker(softMax) {
		// Soft cap exhausted (or raced). Grow the soft cap (up to hardMax) and try again.
		if p.growSoftMax() {
			softMax = atomic.LoadInt32(&p.softMax)
		}
		_ = p.trySpawnWorker(softMax)
	}

	// Try non-blocking again after potentially spawning worker
	select {
	case p.tasks <- task:
		metrics.RemotePlanWorkerPoolQueueLen.Set(float64(len(p.tasks)))
		return true
	default:
	}

	// Buffer still full - must block. Record buffer full event.
	metrics.RemotePlanWorkerPoolBufferFull.Inc()

	// Slow path: block until space available, or ctx/pool is closed.
	// Record the wait time for diagnostics.
	start := time.Now()
	select {
	case <-ctx.Done():
		return false
	case <-p.stopCh:
		return false
	case p.tasks <- task:
		metrics.RemotePlanWorkerPoolSubmitWaitDuration.Observe(time.Since(start).Seconds())
		metrics.RemotePlanWorkerPoolQueueLen.Set(float64(len(p.tasks)))
		return true
	}
}

func (p *remoteExecWorkerPool) trySpawnWorker(limit int32) bool {
	if limit > p.hardMaxWorkers {
		limit = p.hardMaxWorkers
	}
	for {
		cur := atomic.LoadInt32(&p.workerCount)
		if cur >= limit {
			return false
		}
		if atomic.CompareAndSwapInt32(&p.workerCount, cur, cur+1) {
			metrics.RemotePlanWorkerPoolWorkers.Inc()
			p.wg.Add(1)
			go p.runWorker(false /* core */)
			return true
		}
	}
}

func (p *remoteExecWorkerPool) growSoftMax() bool {
	for {
		cur := atomic.LoadInt32(&p.softMax)
		if cur >= p.hardMaxWorkers {
			return false
		}
		next := cur * 2
		if next < cur {
			next = p.hardMaxWorkers
		}
		if next > p.hardMaxWorkers {
			next = p.hardMaxWorkers
		}
		if atomic.CompareAndSwapInt32(&p.softMax, cur, next) {
			metrics.RemotePlanWorkerPoolSoftMax.Set(float64(next))
			return true
		}
	}
}

func (p *remoteExecWorkerPool) shrinkSoftMaxIfIdle(leavingWorker bool) {
	if p == nil {
		return
	}
	if atomic.LoadInt32(&p.softMax) <= p.baseMaxWorkers {
		return
	}
	workerCount := atomic.LoadInt32(&p.workerCount)
	if leavingWorker {
		workerCount--
	}
	if workerCount <= p.baseMaxWorkers {
		atomic.StoreInt32(&p.softMax, p.baseMaxWorkers)
		metrics.RemotePlanWorkerPoolSoftMax.Set(float64(p.baseMaxWorkers))
	}
}

func (p *remoteExecWorkerPool) runWorker(core bool) {
	defer p.wg.Done()
	defer atomic.AddInt32(&p.workerCount, -1)
	defer metrics.RemotePlanWorkerPoolWorkers.Dec()

	if core {
		for {
			select {
			case <-p.stopCh:
				return
			case task := <-p.tasks:
				p.handleTask(task)
			}
		}
	}

	timer := time.NewTimer(p.idleTimeout)
	defer timer.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case task := <-p.tasks:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			p.handleTask(task)
			timer.Reset(p.idleTimeout)
		case <-timer.C:
			// Extra worker idle timeout: reclaim it.
			p.shrinkSoftMaxIfIdle(true /* leavingWorker */)
			return
		}
	}
}

func (*remoteExecWorkerPool) handleTask(task *remoteExecTask) {
	if task == nil {
		return
	}
	defer task.wg.Done()
	defer tidbutil.Recover(task.RecoverArgs())
	task.server.processRequest(
		task.ctx,
		task.requestID,
		task.request,
		task.responseChan,
		task.activeRequests,
		task.activeRequestsMu,
		task.responsesClosed,
	)
}
