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

	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// maxSpillTimes indicates how many times the data can spill at most.
const maxSpillTimes = 10

const spillFlag = 1
const isSpillingFlag = 2
const hasErrorFlag = 3
const partialStageFlag = 4
const spilledPartitionNum = 256
const spillTasksDoneFlag = -1

type parallelHashAggSpillHelper struct {
	lock            sync.Mutex
	spilledChunksIO [][]*chunk.ListInDisk
	spillTriggered  int32
	isSpilling      int32
	isPartialStage  int32
	hasError        int32

	runningPartialWorkerNum int32

	// This records how many partial workers are waiting for the finish of spill.
	waitingWorkerNum int32

	// Final worker will decrease this var after reading it.
	partitionNeedRestore int

	// When spill is triggered, action will be blocked in this channel to ensure
	// that all partial workers have finished their current chunks and no one will be executing
	// when acction is spilling.
	// Once partial worker finds that it's in spill mode, they will send message to this channel.
	// When spill action is waked up by this message it will check if all partial workers have
	// be ready to wait for the finish of spill action.
	waitForPartialWorkersSyncer chan struct{}

	// It's used for synchronizing the spill action and partial workers.
	// Partial workers will be blocked in this channel while spill action is spilling.
	// When spill is finished, action will send message to this channel to wake up all partial workers.
	//
	// Buffer size is equal to the number of partial workers.
	spillActionAndPartialWorkerSyncer chan struct{}
}

func (p *parallelHashAggSpillHelper) close() {
	close(p.waitForPartialWorkersSyncer)
	close(p.spillActionAndPartialWorkerSyncer)
	for _, ios := range p.spilledChunksIO {
		for _, io := range ios {
			io.Close()
		}
	}
}

func (p *parallelHashAggSpillHelper) getRestoredPartitionNum() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.partitionNeedRestore == spillTasksDoneFlag {
		return spillTasksDoneFlag
	}

	tmp := p.partitionNeedRestore
	p.partitionNeedRestore--
	if p.partitionNeedRestore < 0 {
		p.partitionNeedRestore = spillTasksDoneFlag
	}
	return tmp
}

func (p *parallelHashAggSpillHelper) addListInDisks(listInDisk []*chunk.ListInDisk) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for i := 0; i < spilledPartitionNum; i++ {
		p.spilledChunksIO[i] = append(p.spilledChunksIO[i], listInDisk[i])
	}
}

func (p *parallelHashAggSpillHelper) getListInDisks(partitionNum int) []*chunk.ListInDisk {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.spilledChunksIO[partitionNum]) == 0 {
		return make([]*chunk.ListInDisk, 0)
	}

	return p.spilledChunksIO[partitionNum]
}

func (p *parallelHashAggSpillHelper) isSpillTriggered() bool {
	return atomic.LoadInt32(&p.spillTriggered) == spillFlag
}

func (p *parallelHashAggSpillHelper) triggerSpill() {
	atomic.StoreInt32(&p.spillTriggered, spillFlag)
}

func (p *parallelHashAggSpillHelper) isInSpilling() bool {
	return atomic.LoadInt32(&p.isSpilling) == isSpillingFlag
}

func (p *parallelHashAggSpillHelper) leavePartialStage() {
	atomic.StoreInt32(&p.isPartialStage, partialStageFlag+1)
}

func (p *parallelHashAggSpillHelper) isInPartialStage() bool {
	return atomic.LoadInt32(&p.isPartialStage) == partialStageFlag
}

func (p *parallelHashAggSpillHelper) setIsSpilling() {
	atomic.StoreInt32(&p.isSpilling, isSpillingFlag)
}

func (p *parallelHashAggSpillHelper) resetIsSpilling() {
	atomic.StoreInt32(&p.isSpilling, isSpillingFlag+1)
}

func (p *parallelHashAggSpillHelper) checkErrorAndCloseSpillSyncer() bool {
	res := atomic.LoadInt32(&p.hasError) == hasErrorFlag
	if res {
		close(p.spillActionAndPartialWorkerSyncer)
	}
	return res
}

func (p *parallelHashAggSpillHelper) setError() {
	atomic.StoreInt32(&p.hasError, hasErrorFlag)
}

// AggSpillDiskAction implements memory.ActionOnExceed for unparalleled HashAgg.
// If the memory quota of a query is exceeded, AggSpillDiskAction.Action is
// triggered.
type AggSpillDiskAction struct {
	memory.BaseOOMAction
	e          *HashAggExec
	spillTimes uint32

	spillHelper *parallelHashAggSpillHelper
}

func (a *AggSpillDiskAction) triggerFallBackAction(t *memory.Tracker) {
	if fallback := a.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// Action set HashAggExec spill mode.
func (a *AggSpillDiskAction) Action(t *memory.Tracker) {
	// Guarantee that processed data is at least 20% of the threshold, to avoid spilling too frequently.
	if atomic.LoadUint32(&a.e.inSpillMode) == 0 && a.spillTimes < maxSpillTimes && a.e.memTracker.BytesConsumed() >= t.GetBytesLimit()/5 {
		if a.e.IsUnparallelExec {
			a.spillTimes++
			atomic.StoreUint32(&a.e.inSpillMode, 1)
			memory.QueryForceDisk.Add(1)
		} else if a.spillHelper.isInPartialStage() {
			if len(a.e.partialWorkers) > 0 {
				a.doActionForParallelHashAgg()
			} else {
				logutil.BgLogger().Error("0 length of partialWorkers0 in parallel hash aggregation is illegal")
			}
		} else {
			a.triggerFallBackAction(t)
			return
		}
		logutil.BgLogger().Info("memory exceeds quota, set aggregate mode to spill-mode",
			zap.Uint32("spillTimes", a.spillTimes),
			zap.Int64("consumed", t.BytesConsumed()),
			zap.Int64("quota", t.GetBytesLimit()))
		return
	}
	a.triggerFallBackAction(t)
}

// GetPriority get the priority of the Action
func (*AggSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (a *AggSpillDiskAction) doActionForParallelHashAgg() {
	a.spillHelper.lock.Lock()
	defer a.spillHelper.lock.Unlock()
	if a.spillHelper.isInSpilling() {
		return
	}

	if a.spillHelper.checkErrorAndCloseSpillSyncer() {
		return
	}
	runningPartialWorkerNum := a.spillHelper.runningPartialWorkerNum
	a.spillHelper.setIsSpilling()
	a.e.partialWorkers[0].spillHelper.triggerSpill()
	go a.doActionForParallelHashAggImpl(runningPartialWorkerNum)
}

func (a *AggSpillDiskAction) doActionForParallelHashAggImpl(runningPartialWorkerNum int32) {
	// Ensure that all partial workers are waiting for the finish of spill.
	for {
		<-a.spillHelper.waitForPartialWorkersSyncer
		if a.spillHelper.checkErrorAndCloseSpillSyncer() {
			return
		}
		a.spillHelper.lock.Lock()
		if a.spillHelper.waitingWorkerNum == runningPartialWorkerNum {
			a.spillHelper.lock.Unlock()
			break
		}
		a.spillHelper.lock.Unlock()
	}

	a.spill()

	// Spill is done, wake up all partial workers
	a.spillHelper.lock.Lock()
	defer a.spillHelper.lock.Unlock()
	a.spillHelper.resetIsSpilling()
	for i := 0; i < int(a.spillHelper.runningPartialWorkerNum); i++ {
		a.spillHelper.spillActionAndPartialWorkerSyncer <- struct{}{}
	}
}

func (a *AggSpillDiskAction) spill() {
	syncer := make(chan struct{}, a.spillHelper.runningPartialWorkerNum)
	for _, worker := range a.e.partialWorkers {
		go func(worker *HashAggPartialWorker) {
			worker.spillDataToDisk()
			syncer <- struct{}{}
		}(&worker)
	}

	// Wait for the finish of spill
	finishedWorkerNum := int32(0)
	for {
		<-syncer
		finishedWorkerNum++
		if finishedWorkerNum == a.spillHelper.runningPartialWorkerNum {
			break
		}
	}
}
