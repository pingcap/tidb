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
	"sync"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type parallelSortSpillHelper struct {
	cond             *sync.Cond
	spillStatus      int
	sortedRowsInDisk []*chunk.DataInDiskByChunks
	cursor           []*dataCursor
	spillError       error
	sortExec         *SortExec

	finishCh chan struct{}

	fieldTypes    []*types.FieldType
	tmpSpillChunk *chunk.Chunk
}

func newParallelSortSpillHelper(sortExec *SortExec, fieldTypes []*types.FieldType, finishCh chan struct{}) *parallelSortSpillHelper {
	return &parallelSortSpillHelper{
		cond:          sync.NewCond(new(sync.Mutex)),
		spillStatus:   notSpilled,
		spillError:    nil,
		sortExec:      sortExec,
		finishCh:      finishCh,
		fieldTypes:    fieldTypes,
		tmpSpillChunk: chunk.NewChunkWithCapacity(fieldTypes, spillChunkSize),
	}
}

func (p *parallelSortSpillHelper) isNotSpilledNoLock() bool {
	return p.spillStatus == notSpilled
}

func (p *parallelSortSpillHelper) isInSpillingNoLock() bool {
	return p.spillStatus == inSpilling
}

func (p *parallelSortSpillHelper) isSpillNeeded() bool {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	return p.spillStatus == needSpill
}

func (p *parallelSortSpillHelper) isSpillTriggered() bool {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	return len(p.sortedRowsInDisk) > 0
}

func (p *parallelSortSpillHelper) checkSpillError() error {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	return p.spillError
}

func (p *parallelSortSpillHelper) setSpillError(err error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.spillError = err
}

func (p *parallelSortSpillHelper) setInSpilling() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.spillStatus = inSpilling
}

func (p *parallelSortSpillHelper) setNeedSpillNoLock() {
	p.spillStatus = needSpill
}

func (p *parallelSortSpillHelper) setNotSpilled() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.spillStatus = notSpilled
}

func (p *parallelSortSpillHelper) spill() {
	select {
	case <-p.finishCh:
		return
	default:
	}

	workerNum := len(p.sortExec.Parallel.workers)
	workerWaiter := &sync.WaitGroup{}
	workerWaiter.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func(idx int) {
			defer func() {
				if r := recover(); r != nil {
					processPanicAndLog(p.setSpillError, r)
				}
				workerWaiter.Done()
			}()

			// Let workers sort existing rows
			// p.sortExec.Parallel.workers[idx].mergeSortGlobalRows()
		}(i)
	}

	workerWaiter.Wait()
	p.setInSpilling()
	p.spillImpl()

	// Spill is done, broadcast to wake up some sleep goroutine
	p.setNotSpilled()
	p.cond.Broadcast()
}

func (p *parallelSortSpillHelper) reloadCursor(i int) (bool, error) {
	spilledChkNum := p.sortedRowsInDisk[i].NumChunks()
	restoredChkID := p.cursor[i].getChkID() + 1
	if restoredChkID >= spilledChkNum {
		// All data has been consumed
		return false, nil
	}

	chk, err := p.sortedRowsInDisk[i].GetChunk(restoredChkID)
	if err != nil {
		return false, err
	}
	p.cursor[i].setChunk(chk, restoredChkID)
	return true, nil
}

func (p *parallelSortSpillHelper) releaseMemory() {
	totalReleasedMemory := int64(0)
	for _, worker := range p.sortExec.Parallel.workers {
		totalReleasedMemory += worker.totalMemoryUsage
		worker.totalMemoryUsage = 0
	}
	p.sortExec.memTracker.Consume(-totalReleasedMemory)
}

func (p *parallelSortSpillHelper) getRowsNeedingSpill() []chunk.Row {
	return nil
}

func (p *parallelSortSpillHelper) spillImpl() {
	p.tmpSpillChunk.Reset()
	inDisk := chunk.NewDataInDiskByChunks(p.fieldTypes)
	inDisk.GetDiskTracker().AttachTo(p.sortExec.diskTracker)

	spilledRows := p.getRowsNeedingSpill()
	for _, row := range spilledRows {
		select {
		case <-p.finishCh:
			return
		default:
		}

		p.tmpSpillChunk.AppendRow(row)
		if p.tmpSpillChunk.IsFull() {
			err := inDisk.Add(p.tmpSpillChunk)
			if err != nil {
				p.setSpillError(err)
			}
			p.tmpSpillChunk.Reset()
		}
	}

	if p.tmpSpillChunk.NumRows() > 0 {
		err := inDisk.Add(p.tmpSpillChunk)
		if err != nil {
			p.setSpillError(err)
		}
		p.tmpSpillChunk.Reset()
	}

	p.sortedRowsInDisk = append(p.sortedRowsInDisk, inDisk)
	p.releaseMemory()
}
