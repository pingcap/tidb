// Copyright 2023 PingCAP, Inc.
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

package ttlworker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/types"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/util/codec"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var errScanWorkerShrink = errors.New("scan worker shrink")
var idleScanTaskChan = make(<-chan *ttlScanTask)

type JobManagerV2 struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	sessPool sessionPool
	store    kv.Storage

	scanWorkers   []worker
	delWorkers    []worker
	notifyStateCh chan interface{}
	scanTaskChan  chan *ttlScanTask
	delTaskChan   chan *ttlDeleteTask
	timerRT       *timerRuntime

	ownerFunc func() bool
}

func NewJobManagerV2(sessPool sessionPool, store kv.Storage, etcd *clientv3.Client, isOwner func() bool) *JobManagerV2 {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &JobManagerV2{
		ctx:           ctx,
		cancel:        cancel,
		sessPool:      sessPool,
		store:         store,
		notifyStateCh: make(chan interface{}, 1),
		scanTaskChan:  make(chan *ttlScanTask),
		delTaskChan:   make(chan *ttlDeleteTask),
		ownerFunc:     isOwner,
		timerRT:       newTimerRuntime(sessPool, etcd, &disttaskTTLJobAdapter{}),
	}
	manager.wg.Add(1)
	go manager.loop()
	return manager
}

func (m *JobManagerV2) IsOwner() bool {
	if fn := m.ownerFunc; fn != nil {
		return fn()
	}
	return false
}

func (m *JobManagerV2) Stop() {
	m.cancel()
	m.wg.Wait()
}

func (m *JobManagerV2) ScheduleTask(ctx context.Context, minimalTask *ttlMinimalTask) (<-chan *ttlScanTaskExecResult, error) {
	task, err := m.createTTLScanTask(ctx, minimalTask)
	if err != nil {
		return nil, err
	}

	select {
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case m.scanTaskChan <- task:
		return task.respChan, nil
	}
}

func (m *JobManagerV2) createTTLScanTask(ctx context.Context, task *ttlMinimalTask) (_ *ttlScanTask, err error) {
	var rangeStart, rangeEnd []types.Datum
	if len(task.RangeEndEncoded) > 0 {
		rangeStart, err = codec.Decode(task.RangeStartEncoded, len(task.RangeEndEncoded))
		if err != nil {
			return nil, err
		}
	}

	if len(task.RangeEndEncoded) > 0 {
		rangeEnd, err = codec.Decode(task.RangeEndEncoded, len(task.RangeEndEncoded))
		if err != nil {
			return nil, err
		}
	}

	se, err := getSession(m.sessPool)
	if err != nil {
		return nil, err
	}
	defer se.Close()

	partitionID := int64(0)
	if task.PhysicalID != task.TableID {
		partitionID = task.PhysicalID
	}

	tbl, err := cache.NewPhysicalTableByID(task.TableID, partitionID, se.SessionInfoSchema())
	if err != nil {
		return nil, err
	}

	return &ttlScanTask{
		ctx: ctx,
		TTLTask: &cache.TTLTask{
			JobID:          task.JobID,
			TableID:        task.PhysicalID,
			ScanID:         task.ScanID,
			ScanRangeStart: rangeStart,
			ScanRangeEnd:   rangeEnd,
			ExpireTime:     time.Unix(task.ExpireTimeUnix, 0),
		},
		tbl:        tbl,
		statistics: &task.statistics,
		respChan:   make(chan *ttlScanTaskExecResult, 1),
	}, nil
}

func (m *JobManagerV2) responseTask(r *ttlScanTaskExecResult) {
	select {
	case r.task.respChan <- r:
	default:
		logutil.BgLogger().Error("failed to response TTL scan task, channel is blocking")
	}
}

func (m *JobManagerV2) loop() {
	defer m.resizeScanAndDelWorkers(0, 0)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	syncTicker := time.NewTicker(5 * time.Second)
	defer syncTicker.Stop()

	for {
		scanTaskChan := idleScanTaskChan
		idleScanWorker := m.handleScanWorkers()
		if idleScanWorker != nil {
			scanTaskChan = m.scanTaskChan
		}

		m.resizeScanAndDelWorkers(variable.TTLScanWorkerCount.Load(), variable.TTLDeleteWorkerCount.Load())
		select {
		case task := <-scanTaskChan:
			if err := idleScanWorker.Schedule(task); err != nil {
				m.responseTask(&ttlScanTaskExecResult{task: task, err: err})
			}
		case <-ticker.C:
			if m.IsOwner() && variable.EnableDistTask.Load() {
				m.timerRT.Resume(m.ctx)
			} else {
				m.timerRT.SyncTimers(m.ctx)
			}
		case <-syncTicker.C:
			if !m.timerRT.Paused() && variable.EnableDistTask.Load() {
				m.timerRT.SyncTimers(m.ctx)
			}
		case <-m.notifyStateCh:
		}
	}
}

func (m *JobManagerV2) handleScanWorkers() (idleWorker *ttlScanWorker) {
	for _, w := range m.scanWorkers {
		wk := w.(*ttlScanWorker)
		if r := wk.PollTaskResult(); r != nil {
			m.responseTask(r)
		}

		if idleWorker == nil && wk.CouldSchedule() {
			idleWorker = wk
		}
	}
	return
}

func (m *JobManagerV2) resizeWorkers(workers []worker, count int, factory func() worker) ([]worker, []worker, error) {
	if count < len(workers) {
		logutil.Logger(m.ctx).Info("shrink ttl worker", zap.Int("originalCount", len(workers)), zap.Int("newCount", count))

		for _, w := range workers[count:] {
			w.Stop()
		}

		var errs error
		// don't use `m.ctx` here, because when shutdown the server, `m.ctx` has already been cancelled
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		for _, w := range workers[count:] {
			err := w.WaitStopped(ctx, 30*time.Second)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to stop ttl worker", zap.Error(err))
				errs = multierr.Append(errs, err)
			}
		}
		cancel()

		// remove the existing workers, and keep the left workers
		return workers[:count], workers[count:], errs
	}

	if count > len(workers) {
		logutil.Logger(m.ctx).Info("scale ttl worker", zap.Int("originalCount", len(workers)), zap.Int("newCount", count))

		for i := len(workers); i < count; i++ {
			w := factory()
			w.Start()
			workers = append(workers, w)
		}
		return workers, nil, nil
	}

	return workers, nil, nil
}

func (m *JobManagerV2) resizeScanAndDelWorkers(scanWorkerCnt, delWorkerCnt int32) {
	newScanWorkers, workers, err := m.resizeWorkers(m.scanWorkers, int(scanWorkerCnt), func() worker {
		return newScanWorker(m.delTaskChan, m.notifyStateCh, m.sessPool)
	})

	if err != nil {
		logutil.BgLogger().Warn("error occurs when resize scan workers with sys var", zap.Error(err))
	}

	m.scanWorkers = newScanWorkers
	for _, w := range workers {
		if task := w.(*ttlScanWorker).CurrentTask(); task != nil {
			r := &ttlScanTaskExecResult{task: task, err: errScanWorkerShrink}
			m.responseTask(r)
		}
	}

	newDelWorkers, _, err := m.resizeWorkers(m.delWorkers, int(delWorkerCnt), func() worker {
		return newDeleteWorker(m.delTaskChan, m.sessPool)
	})

	if err != nil {
		logutil.BgLogger().Warn("error occurs when resize del workers with sys var", zap.Error(err))
	}

	m.delWorkers = newDelWorkers
}
