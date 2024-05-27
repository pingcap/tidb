// Copyright 2024 PingCAP, Inc.
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

package sortexec

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type topNSpillHelper struct {
	cond             *sync.Cond
	spillStatus      int
	sortedRowsInDisk []*chunk.DataInDiskByChunks

	finishCh      <-chan struct{}
	errOutputChan chan<- rowWithError

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	fieldTypes         []*types.FieldType
	tmpSpillChunksChan chan *chunk.Chunk

	workers []*topNWorker

	bytesConsumed atomic.Int64
	bytesLimit    atomic.Int64
}

func newTopNSpillerHelper(
	topn *TopNExec,
	finishCh <-chan struct{},
	errOutputChan chan<- rowWithError,
	memTracker *memory.Tracker,
	diskTracker *disk.Tracker,
	fieldTypes []*types.FieldType,
	workers []*topNWorker,
	concurrencyNum int,
) *topNSpillHelper {
	lock := sync.Mutex{}
	tmpSpillChunksChan := make(chan *chunk.Chunk, concurrencyNum)
	for i := 0; i < len(workers); i++ {
		tmpSpillChunksChan <- exec.TryNewCacheChunk(topn.Children(0))
	}

	return &topNSpillHelper{
		cond:               sync.NewCond(&lock),
		spillStatus:        notSpilled,
		sortedRowsInDisk:   make([]*chunk.DataInDiskByChunks, 0),
		finishCh:           finishCh,
		errOutputChan:      errOutputChan,
		memTracker:         memTracker,
		diskTracker:        diskTracker,
		fieldTypes:         fieldTypes,
		tmpSpillChunksChan: tmpSpillChunksChan,
		workers:            workers,
		bytesConsumed:      atomic.Int64{},
		bytesLimit:         atomic.Int64{},
	}
}

func (t *topNSpillHelper) close() {
	for _, inDisk := range t.sortedRowsInDisk {
		inDisk.Close()
	}
}

func (t *topNSpillHelper) isNotSpilledNoLock() bool {
	return t.spillStatus == notSpilled
}

func (t *topNSpillHelper) isInSpillingNoLock() bool {
	return t.spillStatus == inSpilling
}

func (t *topNSpillHelper) isSpillNeeded() bool {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	return t.spillStatus == needSpill
}

func (t *topNSpillHelper) isSpillTriggered() bool {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	return len(t.sortedRowsInDisk) > 0
}

func (t *topNSpillHelper) setInSpilling() {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	t.spillStatus = inSpilling
	logutil.BgLogger().Info(spillInfo, zap.Int64("consumed", t.bytesConsumed.Load()), zap.Int64("quota", t.bytesLimit.Load()))
}

func (t *topNSpillHelper) setNotSpilled() {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	t.spillStatus = notSpilled
}

func (t *topNSpillHelper) setNeedSpillNoLock() {
	t.spillStatus = needSpill
}

func (t *topNSpillHelper) addInDisk(inDisk *chunk.DataInDiskByChunks) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	t.sortedRowsInDisk = append(t.sortedRowsInDisk, inDisk)
}

func (*topNSpillHelper) spillTmpSpillChunk(inDisk *chunk.DataInDiskByChunks, tmpSpillChunk *chunk.Chunk) error {
	err := inDisk.Add(tmpSpillChunk)
	if err != nil {
		return err
	}
	tmpSpillChunk.Reset()
	return nil
}

func (t *topNSpillHelper) spill() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
	}()

	select {
	case <-t.finishCh:
		return nil
	default:
	}

	t.setInSpilling()
	defer t.cond.Broadcast()
	defer t.setNotSpilled()

	workerNum := len(t.workers)
	errChan := make(chan error, workerNum)
	workerWaiter := &sync.WaitGroup{}
	workerWaiter.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func(idx int) {
			defer func() {
				if r := recover(); r != nil {
					processPanicAndLog(t.errOutputChan, r)
				}
				workerWaiter.Done()
			}()

			spillErr := t.spillHeap(t.workers[idx].chkHeap)
			if spillErr != nil {
				errChan <- spillErr
			}
		}(i)
	}

	workerWaiter.Wait()
	close(errChan)

	// Fetch only one error is enough
	spillErr := <-errChan
	if spillErr != nil {
		return spillErr
	}
	return nil
}

func (t *topNSpillHelper) spillHeap(chkHeap *topNChunkHeap) error {
	if chkHeap.Len() <= 0 && chkHeap.rowChunks.Len() <= 0 {
		return nil
	}

	if !chkHeap.isRowPtrsInit {
		// Do not consume memory here, as it will hang
		chkHeap.initPtrsImpl()
	}
	slices.SortFunc(chkHeap.rowPtrs, chkHeap.keyColumnsCompare)

	tmpSpillChunk := <-t.tmpSpillChunksChan
	tmpSpillChunk.Reset()
	defer func() {
		t.tmpSpillChunksChan <- tmpSpillChunk
	}()

	inDisk := chunk.NewDataInDiskByChunks(t.fieldTypes)
	inDisk.GetDiskTracker().AttachTo(t.diskTracker)

	rowPtrNum := chkHeap.Len()
	for ; chkHeap.idx < rowPtrNum; chkHeap.idx++ {
		if tmpSpillChunk.IsFull() {
			err := t.spillTmpSpillChunk(inDisk, tmpSpillChunk)
			if err != nil {
				return err
			}
		}
		tmpSpillChunk.AppendRow(chkHeap.rowChunks.GetRow(chkHeap.rowPtrs[chkHeap.idx]))
	}

	// Spill remaining rows in tmpSpillChunk
	if tmpSpillChunk.NumRows() > 0 {
		err := t.spillTmpSpillChunk(inDisk, tmpSpillChunk)
		if err != nil {
			return err
		}
	}

	t.addInDisk(inDisk)
	injectTopNRandomFail(200)

	chkHeap.clear()
	return nil
}

type topNSpillAction struct {
	memory.BaseOOMAction
	spillHelper *topNSpillHelper
}

// GetPriority get the priority of the Action.
func (*topNSpillAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (t *topNSpillAction) Action(tracker *memory.Tracker) {
	t.spillHelper.cond.L.Lock()
	defer t.spillHelper.cond.L.Unlock()

	for t.spillHelper.isInSpillingNoLock() {
		t.spillHelper.cond.Wait()
	}

	hasEnoughData := hasEnoughDataToSpill(t.spillHelper.memTracker, tracker)
	if tracker.CheckExceed() && t.spillHelper.isNotSpilledNoLock() && hasEnoughData {
		t.spillHelper.setNeedSpillNoLock()
		t.spillHelper.bytesConsumed.Store(tracker.BytesConsumed())
		t.spillHelper.bytesLimit.Store(tracker.GetBytesLimit())
		return
	}

	if tracker.CheckExceed() && !hasEnoughData {
		t.GetFallback()
	}
}
