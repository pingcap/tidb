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

package aggregate

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

const (
	// maxSpillTimes indicates how many times the data can spill at most.
	maxSpillTimes       = 10
	spillFlag           = 1
	isSpillingFlag      = 2
	hasErrorFlag        = 3
	partialStageFlag    = 4
	spilledPartitionNum = 256
	spillTasksDoneFlag  = -1

	spillMode    = 0
	notSpillMode = 1

	spillLogInfo string = "memory exceeds quota, set aggregate mode to spill-mode"
)

type parallelHashAggSpillHelper struct {
	lock struct {
		mu                         *sync.Mutex
		cond                       *sync.Cond
		nextPartitionIdx           int
		spilledChunksIO            [][]*chunk.DataInDiskByChunks
		spillTriggered             int32
		isSpilling                 int32
		isAllPartialWorkerFinished bool
	}

	// partitionNeedRestore int

	diskTracker    *disk.Tracker
	isPartialStage int32
	hasError       int32

	// When spill is triggered, all partial workers should stop
	// and we can ensure this when the spill action gets write lock.
	syncLock sync.RWMutex
}

func newSpillHelper() *parallelHashAggSpillHelper {
	mu := new(sync.Mutex)
	return &parallelHashAggSpillHelper{
		lock: struct {
			mu                         *sync.Mutex
			cond                       *sync.Cond
			nextPartitionIdx           int
			spilledChunksIO            [][]*chunk.DataInDiskByChunks
			spillTriggered             int32
			isSpilling                 int32
			isAllPartialWorkerFinished bool
		}{
			mu:                         mu,
			cond:                       sync.NewCond(mu),
			spilledChunksIO:            make([][]*chunk.DataInDiskByChunks, spilledPartitionNum),
			spillTriggered:             0,
			isSpilling:                 0,
			nextPartitionIdx:           spilledPartitionNum - 1,
			isAllPartialWorkerFinished: false,
		},
		isPartialStage: partialStageFlag,
		hasError:       0,
	}
}

func (p *parallelHashAggSpillHelper) close() {
	for _, ios := range p.lock.spilledChunksIO {
		for _, io := range ios {
			io.Close()
		}
	}
}

func (p *parallelHashAggSpillHelper) getNextPartition() (int, bool) {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	partitionIdx := p.lock.nextPartitionIdx
	if partitionIdx < 0 {
		return -1, false
	}

	p.lock.nextPartitionIdx--
	return partitionIdx, true
}

func (p *parallelHashAggSpillHelper) addListInDisks(dataInDisk []*chunk.DataInDiskByChunks) {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	for i, data := range dataInDisk {
		p.lock.spilledChunksIO[i] = append(p.lock.spilledChunksIO[i], data)
	}
}

func (p *parallelHashAggSpillHelper) getListInDisks(partitionNum int) []*chunk.DataInDiskByChunks {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	return p.lock.spilledChunksIO[partitionNum]
}

func (p *parallelHashAggSpillHelper) isSpillTriggered() bool {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	return p.lock.spillTriggered == spillFlag
}

func (p *parallelHashAggSpillHelper) isSpillTriggeredNoLock() bool {
	return p.lock.spillTriggered == spillFlag
}

func (p *parallelHashAggSpillHelper) setSpillTriggered() {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	p.lock.spillTriggered = spillFlag
}

func (p *parallelHashAggSpillHelper) isInPartialStage() bool {
	return atomic.LoadInt32(&p.isPartialStage) == partialStageFlag
}

func (p *parallelHashAggSpillHelper) isInSpilling() bool {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	return p.lock.isSpilling == isSpillingFlag
}

func (p *parallelHashAggSpillHelper) isInSpillingNoLock() bool {
	return p.lock.isSpilling == isSpillingFlag
}

func (p *parallelHashAggSpillHelper) setIsSpilling() {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	p.lock.isSpilling = isSpillingFlag
}

func (p *parallelHashAggSpillHelper) resetIsSpilling() {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	p.lock.isSpilling = isSpillingFlag + 1
}

func (p *parallelHashAggSpillHelper) setAllPartialWorkersFinished() {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	p.lock.isAllPartialWorkerFinished = true
}

func (p *parallelHashAggSpillHelper) checkAllPartialWorkersFinished() bool {
	p.lock.mu.Lock()
	defer p.lock.mu.Unlock()
	return p.lock.isAllPartialWorkerFinished
}

// We need to check error with atmoic as multi partial workers may access it.
func (p *parallelHashAggSpillHelper) checkError() bool {
	return atomic.LoadInt32(&p.hasError) == hasErrorFlag
}

// We need to set error with atmoic as multi partial workers may access it.
func (p *parallelHashAggSpillHelper) setError() {
	atomic.StoreInt32(&p.hasError, hasErrorFlag)
}

func isInSpillMode(inSpillMode *uint32) bool {
	return atomic.LoadUint32(inSpillMode) == spillMode
}

// Guarantee that processed data is at least 20% of the threshold, to avoid spilling too frequently.
func hasEnoughDataToSpill(aggTracker *memory.Tracker, passedInTracker *memory.Tracker) bool {
	return aggTracker.BytesConsumed() >= passedInTracker.GetBytesLimit()/5
}

// AggSpillDiskAction implements memory.ActionOnExceed for unparalleled HashAgg.
// If the memory quota of a query is exceeded, AggSpillDiskAction.Action is
// triggered.
type AggSpillDiskAction struct {
	memory.BaseOOMAction
	e          *HashAggExec
	spillTimes uint32
}

// Action set HashAggExec spill mode.
func (a *AggSpillDiskAction) Action(t *memory.Tracker) {
	if isInSpillMode(&a.e.inSpillMode) && hasEnoughDataToSpill(a.e.memTracker, t) && a.spillTimes < maxSpillTimes {
		a.spillTimes++
		logutil.BgLogger().Info(spillLogInfo,
			zap.Uint32("spillTimes", a.spillTimes),
			zap.Int64("consumed", t.BytesConsumed()),
			zap.Int64("quota", t.GetBytesLimit()))
		atomic.StoreUint32(&a.e.inSpillMode, 1)
		memory.QueryForceDisk.Add(1)
		return
	}
	if fallback := a.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// GetPriority get the priority of the Action
func (*AggSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

// ParallelAggSpillDiskAction implements memory.ActionOnExceed for parallel HashAgg.
type ParallelAggSpillDiskAction struct {
	memory.BaseOOMAction
	e           *HashAggExec
	spillHelper *parallelHashAggSpillHelper
	once        sync.Once
}

// Action set HashAggExec spill mode.
func (p *ParallelAggSpillDiskAction) Action(t *memory.Tracker) {
	if !p.actionImpl(t) {
		return
	}

	if t.CheckExceed() {
		p.triggerFallBackAction(t)
	}
}

// Return false if we should keep executing.
func (p *ParallelAggSpillDiskAction) actionImpl(t *memory.Tracker) bool {
	p.spillHelper.lock.mu.Lock()
	defer p.spillHelper.lock.mu.Unlock()

	if !p.spillHelper.isInPartialStage() {
		// The spill can be triggered only when we are in partial stage.
		p.triggerFallBackAction(t)
		return false
	}

	if !p.spillHelper.isSpillTriggeredNoLock() && hasEnoughDataToSpill(p.e.memTracker, t) {
		p.once.Do(func() {
			go func() {
				p.doActionForParallelHashAgg(t)
				p.spillHelper.lock.cond.Broadcast()
			}()
		})
		return false
	}

	for p.spillHelper.isInSpillingNoLock() {
		p.spillHelper.lock.cond.Wait()
	}

	return true
}

func (p *ParallelAggSpillDiskAction) triggerFallBackAction(t *memory.Tracker) {
	if fallback := p.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// GetPriority get the priority of the Action
func (*ParallelAggSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

// syncLock has been held outside of this function
func (p *ParallelAggSpillDiskAction) doActionForParallelHashAgg(t *memory.Tracker) {
	p.spillHelper.setIsSpilling()
	p.spillHelper.syncLock.Lock()
	defer p.spillHelper.syncLock.Unlock()
	defer p.spillHelper.resetIsSpilling()
	if p.spillHelper.checkError() {
		return
	}

	// When all partial workers exit, we shouldn't spill.
	// When it's the first triggered spill, spillTriggered flag has not been set yet
	// and partial workers will shuffle data to final workers by channels as spillTriggered
	// flag tells them there is no spill triggered. It will cause unexpected behaviour
	// if we still spill data when all data have been shuffled to final workers.
	if p.spillHelper.checkAllPartialWorkersFinished() {
		return
	}

	logutil.BgLogger().Info(spillLogInfo,
		zap.Int64("consumed", t.BytesConsumed()),
		zap.Int64("quota", t.GetBytesLimit()))

	p.spillHelper.setSpillTriggered()
	p.spill()
}

func (p *ParallelAggSpillDiskAction) spill() {
	waiter := sync.WaitGroup{}
	waiter.Add(len(p.e.partialWorkers))
	for i := range p.e.partialWorkers {
		go func(worker *HashAggPartialWorker) {
			err := worker.spillDataToDisk()
			if err != nil {
				worker.processError(err)
			}
			waiter.Done()
		}(&p.e.partialWorkers[i])
	}

	// Wait for the finish of spill
	waiter.Wait()
}
