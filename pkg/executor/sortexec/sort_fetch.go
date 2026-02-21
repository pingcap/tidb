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

package sortexec

import (
	"context"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

func (e *SortExec) fetchChunksUnparallel(ctx context.Context) error {
	fields := exec.RetTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}

	err := e.switchToNewSortPartition(fields, byItemsDesc, false)
	if err != nil {
		return err
	}

	for {
		chk := exec.TryNewCacheChunk(e.Children(0))
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}

		err = e.storeChunk(chk, fields, byItemsDesc)
		if err != nil {
			return err
		}

		failpoint.Inject("unholdSyncLock", func(val failpoint.Value) {
			if val.(bool) {
				// Ensure that spill can get `syncLock`.
				time.Sleep(1 * time.Millisecond)
			}
		})
	}

	failpoint.Inject("waitForSpill", func(val failpoint.Value) {
		if val.(bool) {
			// Ensure that spill is triggered before returning data.
			time.Sleep(50 * time.Millisecond)
		}
	})

	failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			if e.Ctx().GetSessionVars().ConnectionID == 123456 {
				e.Ctx().GetSessionVars().MemTracker.Killer.SendKillSignal(sqlkiller.QueryMemoryExceeded)
			}
		}
	})

	err = e.handleCurrentPartitionBeforeExit()
	if err != nil {
		return err
	}

	e.Unparallel.sortPartitions = append(e.Unparallel.sortPartitions, e.curPartition)
	e.curPartition = nil
	return nil
}

func (e *SortExec) fetchChunksParallel(ctx context.Context) error {
	// Wait for the finish of all workers
	workersWaiter := util.WaitGroupWrapper{}
	// Wait for the finish of chunk fetcher
	fetcherWaiter := util.WaitGroupWrapper{}

	for i := range e.Parallel.workers {
		e.Parallel.workers[i] = newParallelSortWorker(i, e.lessRow, e.Parallel.chunkChannel, e.Parallel.fetcherAndWorkerSyncer, e.Parallel.resultChannel, e.finishCh, e.memTracker, e.Parallel.sortedRowsIters[i], e.MaxChunkSize(), e.Parallel.spillHelper, &e.Ctx().GetSessionVars().SQLKiller)
		worker := e.Parallel.workers[i]
		workersWaiter.Run(func() {
			worker.run()
		})
	}

	// Fetch chunks from child and put chunks into chunkChannel
	fetcherWaiter.Run(func() {
		e.fetchChunksFromChild(ctx)
	})

	go e.generateResult(&workersWaiter, &fetcherWaiter)
	return nil
}

func (e *SortExec) spillRemainingRowsWhenNeeded() error {
	if e.Parallel.spillHelper.isSpillTriggered() {
		return e.Parallel.spillHelper.spill()
	}
	return nil
}

func (e *SortExec) checkSpillAndExecute() error {
	if e.Parallel.spillHelper.isSpillNeeded() {
		// Wait for the stop of all workers
		e.Parallel.fetcherAndWorkerSyncer.Wait()
		return e.Parallel.spillHelper.spill()
	}
	return nil
}

// Fetch chunks from child and put chunks into chunkChannel
func (e *SortExec) fetchChunksFromChild(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(e.Parallel.resultChannel, r)
		}

		e.Parallel.fetcherAndWorkerSyncer.Wait()
		err := e.spillRemainingRowsWhenNeeded()
		if err != nil {
			e.Parallel.resultChannel <- rowWithError{err: err}
		}

		failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
			if val.(bool) {
				if e.Ctx().GetSessionVars().ConnectionID == 123456 {
					e.Ctx().GetSessionVars().MemTracker.Killer.SendKillSignal(sqlkiller.QueryMemoryExceeded)
				}
			}
		})

		// We must place it after the spill as workers will process its received
		// chunks after channel is closed and this will cause data race.
		close(e.Parallel.chunkChannel)
	}()

	for {
		chk := exec.TryNewCacheChunk(e.Children(0))
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			e.Parallel.resultChannel <- rowWithError{err: err}
			return
		}

		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}

		chkWithMemoryUsage := &chunkWithMemoryUsage{
			Chk:         chk,
			MemoryUsage: chk.MemoryUsage() + chunk.RowSize*int64(rowCount),
		}

		e.memTracker.Consume(chkWithMemoryUsage.MemoryUsage)

		e.Parallel.fetcherAndWorkerSyncer.Add(1)

		select {
		case <-e.finishCh:
			e.Parallel.fetcherAndWorkerSyncer.Done()
			return
		case e.Parallel.chunkChannel <- chkWithMemoryUsage:
		}

		err = e.checkSpillAndExecute()
		if err != nil {
			e.Parallel.resultChannel <- rowWithError{err: err}
			return
		}
		injectParallelSortRandomFail(3)
	}
}

func (e *SortExec) initCompareFuncs(ctx expression.EvalContext) error {
	var err error
	failpoint.Inject("ParallelSortRandomFail", func(val failpoint.Value) {
		if val.(bool) {
			randNum := rand.Int31n(10000)
			if randNum < 500 {
				err = errors.NewNoStackError("return error by random failpoint")
			}
		}
	})
	if err != nil {
		return err
	}

	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType(ctx)
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
		if e.keyCmpFuncs[i] == nil {
			return errors.Errorf("Sort executor not supports type %s", types.TypeStr(keyType.GetType()))
		}
	}
	return nil
}

func (e *SortExec) buildKeyColumns() error {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		switch col := by.Expr.(type) {
		case *expression.Column:
			e.keyColumns = append(e.keyColumns, col.Index)
		case *expression.Constant:
			// Ignore constant as constant can not affect the sorted result
		default:
			return errors.NewNoStackError("Get unexpected expression")
		}
	}
	return nil
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) int {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (e *SortExec) compareRow(rowI, rowJ chunk.Row) int {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// IsSpillTriggeredInParallelSortForTest tells if spill is triggered in parallel sort.
func (e *SortExec) IsSpillTriggeredInParallelSortForTest() bool {
	return e.Parallel.spillHelper.isSpillTriggered()
}

// GetSpilledRowNumInParallelSortForTest tells if spill is triggered in parallel sort.
func (e *SortExec) GetSpilledRowNumInParallelSortForTest() int64 {
	totalSpilledRows := int64(0)
	for _, disk := range e.Parallel.spillHelper.sortedRowsInDisk {
		totalSpilledRows += disk.NumRows()
	}
	return totalSpilledRows
}

// IsSpillTriggeredInOnePartitionForTest tells if spill is triggered in a specific partition, it's only used in test.
func (e *SortExec) IsSpillTriggeredInOnePartitionForTest(idx int) bool {
	return e.Unparallel.sortPartitions[idx].isSpillTriggered()
}

// GetRowNumInOnePartitionDiskForTest returns number of rows a partition holds in disk, it's only used in test.
func (e *SortExec) GetRowNumInOnePartitionDiskForTest(idx int) int64 {
	return e.Unparallel.sortPartitions[idx].numRowInDiskForTest()
}

// GetRowNumInOnePartitionMemoryForTest returns number of rows a partition holds in memory, it's only used in test.
func (e *SortExec) GetRowNumInOnePartitionMemoryForTest(idx int) int64 {
	return e.Unparallel.sortPartitions[idx].numRowInMemoryForTest()
}

// GetSortPartitionListLenForTest returns the number of partitions, it's only used in test.
func (e *SortExec) GetSortPartitionListLenForTest() int {
	return len(e.Unparallel.sortPartitions)
}

// GetSortMetaForTest returns some sort meta, it's only used in test.
func (e *SortExec) GetSortMetaForTest() (keyColumns []int, keyCmpFuncs []chunk.CompareFunc, byItemsDesc []bool) {
	keyColumns = e.keyColumns
	keyCmpFuncs = e.keyCmpFuncs
	byItemsDesc = make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	return
}
