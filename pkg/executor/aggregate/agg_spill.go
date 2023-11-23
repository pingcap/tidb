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
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
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
	lock                        sync.Mutex
	diskTracker                 *disk.Tracker
	isTrackerEnabled            bool
	spilledChunksIO             [][]*chunk.DataInDiskByRows
	spillTriggered              int32
	isSpilling                  int32
	isPartialStage              int32
	hasError                    int32
	areAllPartialWorkerFinished bool

	// When spill is triggered, all partial workers should stop
	// and we can ensure this when the spill action gets write lock.
	syncLock sync.RWMutex

	// Final worker will decrease this var after reading it.
	partitionNeedRestore int
}

func (p *parallelHashAggSpillHelper) close() {
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

func (p *parallelHashAggSpillHelper) addListInDisks(dataInDisk []*chunk.DataInDiskByRows) {
	num := len(dataInDisk)

	p.lock.Lock()
	defer p.lock.Unlock()
	for i := 0; i < num; i++ {
		p.spilledChunksIO[i] = append(p.spilledChunksIO[i], dataInDisk[i])
	}
}

func (p *parallelHashAggSpillHelper) getListInDisks(partitionNum int) []*chunk.DataInDiskByRows {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.spilledChunksIO[partitionNum]) == 0 {
		return make([]*chunk.DataInDiskByRows, 0)
	}

	return p.spilledChunksIO[partitionNum]
}

func (p *parallelHashAggSpillHelper) isSpillTriggered() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.spillTriggered == spillFlag
}

func (p *parallelHashAggSpillHelper) setSpillTriggered() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.spillTriggered = spillFlag
}

func (p *parallelHashAggSpillHelper) isInPartialStage() bool {
	return atomic.LoadInt32(&p.isPartialStage) == partialStageFlag
}

func (p *parallelHashAggSpillHelper) isInSpilling() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.isSpilling == isSpillingFlag
}

func (p *parallelHashAggSpillHelper) isInSpillingNoLock() bool {
	return p.isSpilling == isSpillingFlag
}

func (p *parallelHashAggSpillHelper) setIsSpillingNoLock() {
	p.isSpilling = isSpillingFlag
}

func (p *parallelHashAggSpillHelper) resetIsSpilling() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.isSpilling = isSpillingFlag + 1
}

func (p *parallelHashAggSpillHelper) setAllPartialWorkersFinished() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.areAllPartialWorkerFinished = true
}

func (p *parallelHashAggSpillHelper) checkAllPartialWorkersFinished() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.areAllPartialWorkerFinished
}

// We need to check error with atmoic as multi partial workers may access it.
func (p *parallelHashAggSpillHelper) checkError() bool {
	return atomic.LoadInt32(&p.hasError) == hasErrorFlag
}

// We need to set error with atmoic as multi partial workers may access it.
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

	spillHelper  *parallelHashAggSpillHelper
	isLogPrinted bool
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
			logutil.BgLogger().Info("memory exceeds quota, set aggregate mode to spill-mode",
				zap.Uint32("spillTimes", a.spillTimes),
				zap.Int64("consumed", t.BytesConsumed()),
				zap.Int64("quota", t.GetBytesLimit()))
		} else if a.spillHelper.isInPartialStage() {
			if len(a.e.partialWorkers) > 0 {
				a.doActionForParallelHashAgg(t)
			} else {
				logutil.BgLogger().Error("0 length of partialWorkers in parallel hash aggregation is illegal")
			}
		} else {
			a.triggerFallBackAction(t)
			return
		}
		return
	}
	a.triggerFallBackAction(t)
}

// GetPriority get the priority of the Action
func (*AggSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (a *AggSpillDiskAction) doActionForParallelHashAgg(t *memory.Tracker) {
	a.spillHelper.lock.Lock()
	defer a.spillHelper.lock.Unlock()
	if a.spillHelper.isInSpillingNoLock() {
		// It means that another goroutine is in spill execution, we should return here.
		return
	}

	a.spillHelper.setIsSpillingNoLock()
	go a.doActionForParallelHashAggImpl(t)
}

func (a *AggSpillDiskAction) doActionForParallelHashAggImpl(t *memory.Tracker) {
	a.spillHelper.syncLock.Lock()
	defer a.spillHelper.syncLock.Unlock()
	defer a.spillHelper.resetIsSpilling()

	if a.spillHelper.checkError() {
		return
	}

	// When all partial workers exit, we shouldn't spill.
	// When it's the first triggered spill, spillTriggered flag has not been set yet
	// and partial workers will shuffle data to final workers by channels as spillTriggered
	// flag tells them there is no spill triggered. It will cause unexpected behaviour
	// if we still spill data when all data have been shuffled to final workers.
	if a.spillHelper.checkAllPartialWorkersFinished() {
		return
	}

	if !a.isLogPrinted {
		logutil.BgLogger().Info("memory exceeds quota, hash aggregate is set to spill mode",
			zap.Int64("consumed", t.BytesConsumed()),
			zap.Int64("quota", t.GetBytesLimit()))
		a.isLogPrinted = true
	}

	a.spillHelper.setSpillTriggered()
	a.spill()
}

func (a *AggSpillDiskAction) spill() {
	enableIntest := false
	failpoint.Inject("enableAggSpillIntest", func(val failpoint.Value) {
		if val.(bool) {
			enableIntest = true
		}
	})

	waiter := sync.WaitGroup{}
	waiter.Add(len(a.e.partialWorkers))
	for i := range a.e.partialWorkers {
		go func(worker *HashAggPartialWorker) {
			err := worker.spillDataToDisk()
			if intest.InTest && enableIntest {
				num := rand.Intn(1000)
				if num < 3 {
					err = errors.Errorf("Random fail is triggered in AggSpillDiskAction")
				}
			}
			if err != nil {
				worker.processError(err)
			}
			waiter.Done()
		}(&a.e.partialWorkers[i])
	}

	// Wait for the finish of spill
	waiter.Wait()
}
