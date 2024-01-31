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
	spillError       error
	sortExec         *SortExec

	lessRowFunc func(chunk.Row, chunk.Row) int
	finishCh    chan struct{}

	fieldTypes    []*types.FieldType
	tmpSpillChunk *chunk.Chunk
}

func newParallelSortSpillHelper(sortExec *SortExec, fieldTypes []*types.FieldType, finishCh chan struct{}, lessRowFunc func(chunk.Row, chunk.Row) int) *parallelSortSpillHelper {
	return &parallelSortSpillHelper{
		cond:          sync.NewCond(new(sync.Mutex)),
		spillStatus:   notSpilled,
		spillError:    nil,
		sortExec:      sortExec,
		lessRowFunc:   lessRowFunc,
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

func (p *parallelSortSpillHelper) spill() error {
	select {
	case <-p.finishCh:
		return nil
	default:
	}

	workerNum := len(p.sortExec.Parallel.workers)
	workerWaiter := &sync.WaitGroup{}
	workerWaiter.Add(workerNum)
	sortedRowsIters := make([]*chunk.Iterator4Slice, workerNum)
	for i := 0; i < workerNum; i++ {
		go func(idx int) {
			defer func() {
				if r := recover(); r != nil {
					processPanicAndLog(p.setSpillError, r)
				}
				workerWaiter.Done()
			}()

			sortedRowsIters[idx] = chunk.NewIterator4Slice(nil)
			sortedRowsIters[idx].Reset(p.sortExec.Parallel.workers[idx].sortLocalRows())
		}(i)
	}

	workerWaiter.Wait()
	p.setInSpilling()

	// Spill is done, broadcast to wake up all sleep goroutines
	defer p.cond.Broadcast()
	defer p.setNotSpilled()

	merger := newMultiWayMerger(sortedRowsIters, p.lessRowFunc)
	merger.init()
	return p.spillImpl(merger)
}

func (p *parallelSortSpillHelper) releaseMemory() {
	totalReleasedMemory := int64(0)
	for _, worker := range p.sortExec.Parallel.workers {
		totalReleasedMemory += worker.totalMemoryUsage
		worker.totalMemoryUsage = 0
	}
	p.sortExec.memTracker.Consume(-totalReleasedMemory)
}

func (p *parallelSortSpillHelper) spillTmpSpillChunk(inDisk *chunk.DataInDiskByChunks) error {
	err := inDisk.Add(p.tmpSpillChunk)
	if err != nil {
		p.setSpillError(err)
		return err
	}
	p.tmpSpillChunk.Reset()
	return nil
}

func (p *parallelSortSpillHelper) spillImpl(merger *multiWayMerger) error {
	p.tmpSpillChunk.Reset()
	inDisk := chunk.NewDataInDiskByChunks(p.fieldTypes)
	inDisk.GetDiskTracker().AttachTo(p.sortExec.diskTracker)

	spilledRowChannel := make(chan chunk.Row, 10000)
	go func() {
		defer close(spilledRowChannel)

		for {
			row := merger.next()
			if row.IsEmpty() {
				break
			}
			spilledRowChannel <- row
		}
	}()

	var (
		row chunk.Row
		ok  bool
	)
	for {
		select {
		case <-p.finishCh:
			return nil
		case row, ok = <-spilledRowChannel:
			if !ok {
				if p.tmpSpillChunk.NumRows() > 0 {
					err := p.spillTmpSpillChunk(inDisk)
					if err != nil {
						return err
					}
				}

				p.sortedRowsInDisk = append(p.sortedRowsInDisk, inDisk)
				p.releaseMemory()
				return nil
			}
		}

		p.tmpSpillChunk.AppendRow(row)
		if p.tmpSpillChunk.IsFull() {
			err := p.spillTmpSpillChunk(inDisk)
			if err != nil {
				return err
			}
		}
	}
}
