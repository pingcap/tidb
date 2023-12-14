// Copyright 2017 PingCAP, Inc.
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
	"container/heap"
	"context"
	"errors"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type sortedRows []chunk.Row

// SortExec represents sorting executor.
type SortExec struct {
	exec.BaseExecutor

	ByItems    []*util.ByItems
	fetched    bool
	ExecSchema *expression.Schema

	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMerge

	IsUnparallel bool

	Unparallel struct {
		Idx int

		// rowChunks is the chunks to store row values.
		rowChunks *chunk.SortedRowContainer

		// PartitionList is the chunks to store row values for partitions. Every partition is a sorted list.
		PartitionList []*chunk.SortedRowContainer

		// spillAction save the Action for spill disk.
		spillAction *chunk.SortAndSpillDiskAction
	}

	Parallel struct {
		result sortedRows
		rowNum int64
		idx    int64

		mpmcQueue *chunk.MPMCQueue
		workers   []*parallelSortWorker

		errRWLock sync.RWMutex
		err       error
	}
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.ID(), -1)
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
		e.diskTracker = memory.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)
	}

	e.IsUnparallel = !e.Ctx().GetSessionVars().EnableParallelSort
	if e.IsUnparallel {
		e.Unparallel.Idx = 0
		e.Unparallel.PartitionList = e.Unparallel.PartitionList[:0]
	} else {
		e.Parallel.idx = 0
		e.Parallel.workers = make([]*parallelSortWorker, e.Ctx().GetSessionVars().ExecutorConcurrency)
		e.Parallel.mpmcQueue = chunk.NewMPMCQueue(chunk.DefaultMPMCQueueLimitNum)
		e.Parallel.err = nil
	}

	return exec.Open(ctx, e.Children(0))
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	e.memTracker = nil
	e.diskTracker = nil
	e.multiWayMerge = nil

	if e.IsUnparallel {
		for _, container := range e.Unparallel.PartitionList {
			err := container.Close()
			if err != nil {
				return err
			}
		}
		e.Unparallel.PartitionList = e.Unparallel.PartitionList[:0]

		if e.Unparallel.rowChunks != nil {
			e.memTracker.Consume(-e.Unparallel.rowChunks.GetMemTracker().BytesConsumed())
			e.Unparallel.rowChunks = nil
		}

		if e.Unparallel.spillAction != nil {
			e.Unparallel.spillAction.SetFinished()
		}
		e.Unparallel.spillAction = nil
	} else {
		e.Parallel.result = nil
		e.Parallel.mpmcQueue = nil
		e.Parallel.workers = nil
	}
	e.memTracker.Consume(-e.memTracker.BytesConsumed())
	return e.Children(0).Close()
}

// Next implements the Executor Next interface.
// Sort constructs the result following these step in unparallel mode:
//  1. Read as mush as rows into memory.
//  2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//     the memory quota trigger and return to step 1
//  3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
//  4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//     just like in-memory sort before.
//
// TODO add introduction of parallel mode
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.initCompareFuncs()
		e.buildKeyColumns()
		err := e.fetchChunks(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}

	if e.IsUnparallel {
		return e.getChunkInUnparallelMode(req)
	} else {
		e.getChunkInParallelMode(req)
	}
	return nil
}

func (e *SortExec) getChunkInParallelMode(req *chunk.Chunk) {
	for ; !req.IsFull() && e.Parallel.idx < e.Parallel.rowNum; e.Parallel.idx++ {
		req.AppendRow(e.Parallel.result[e.Parallel.idx])
	}
}

func (e *SortExec) getChunkInUnparallelMode(req *chunk.Chunk) error {
	if len(e.Unparallel.PartitionList) == 0 {
		return nil
	}
	if len(e.Unparallel.PartitionList) > 1 {
		if err := e.externalSorting(req); err != nil {
			return err
		}
	} else {
		for !req.IsFull() && e.Unparallel.Idx < e.Unparallel.PartitionList[0].NumRow() {
			_, _, err := e.Unparallel.PartitionList[0].GetSortedRowAndAlwaysAppendToChunk(e.Unparallel.Idx, req)
			if err != nil {
				return err
			}
			e.Unparallel.Idx++
		}
	}
	return nil
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
	if e.multiWayMerge == nil {
		e.multiWayMerge = &multiWayMerge{e.lessRow, e.compressRow, make([]partitionPointer, 0, len(e.Unparallel.PartitionList))}
		for i := 0; i < len(e.Unparallel.PartitionList); i++ {
			chk := chunk.New(exec.RetTypes(e), 1, 1)

			row, _, err := e.Unparallel.PartitionList[i].GetSortedRowAndAlwaysAppendToChunk(0, chk)
			if err != nil {
				return err
			}
			e.multiWayMerge.elements = append(e.multiWayMerge.elements, partitionPointer{chk: chk, row: row, partitionID: i, consumed: 0})
		}
		heap.Init(e.multiWayMerge)
	}

	for !req.IsFull() && e.multiWayMerge.Len() > 0 {
		partitionPtr := e.multiWayMerge.elements[0]
		req.AppendRow(partitionPtr.row)
		partitionPtr.consumed++
		partitionPtr.chk.Reset()
		if partitionPtr.consumed >= e.Unparallel.PartitionList[partitionPtr.partitionID].NumRow() {
			heap.Remove(e.multiWayMerge, 0)
			continue
		}

		partitionPtr.row, _, err = e.Unparallel.PartitionList[partitionPtr.partitionID].
			GetSortedRowAndAlwaysAppendToChunk(partitionPtr.consumed, partitionPtr.chk)
		if err != nil {
			return err
		}
		e.multiWayMerge.elements[0] = partitionPtr
		heap.Fix(e.multiWayMerge, 0)
	}
	return nil
}

func (e *SortExec) fetchChunks(ctx context.Context) error {
	if e.IsUnparallel {
		return e.fetchChunksUnparallel(ctx)
	}
	return e.fetchChunksParallel(ctx)
}

func (e *SortExec) fetchChunksUnparallel(ctx context.Context) error {
	fields := exec.RetTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	e.Unparallel.rowChunks = chunk.NewSortedRowContainer(fields, e.MaxChunkSize(), byItemsDesc, e.keyColumns, e.keyCmpFuncs)
	e.Unparallel.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.Unparallel.rowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
	if variable.EnableTmpStorageOnOOM.Load() {
		e.Unparallel.spillAction = e.Unparallel.rowChunks.ActionSpill()
		failpoint.Inject("testSortedRowContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				e.Unparallel.spillAction = e.Unparallel.rowChunks.ActionSpillForTest()
				defer e.Unparallel.spillAction.WaitForTest()
			}
		})
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.Unparallel.spillAction)
		e.Unparallel.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
		e.Unparallel.rowChunks.GetDiskTracker().SetLabel(memory.LabelForRowChunks)
	}
	for {
		chk := exec.TryNewCacheChunk(e.Children(0))
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			return err
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		if err := e.Unparallel.rowChunks.Add(chk); err != nil {
			if errors.Is(err, chunk.ErrCannotAddBecauseSorted) {
				e.Unparallel.PartitionList = append(e.Unparallel.PartitionList, e.Unparallel.rowChunks)
				e.Unparallel.rowChunks = chunk.NewSortedRowContainer(fields, e.MaxChunkSize(), byItemsDesc, e.keyColumns, e.keyCmpFuncs)
				e.Unparallel.rowChunks.GetMemTracker().AttachTo(e.memTracker)
				e.Unparallel.rowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
				e.Unparallel.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
				e.Unparallel.rowChunks.GetDiskTracker().SetLabel(memory.LabelForRowChunks)
				e.Unparallel.spillAction = e.Unparallel.rowChunks.ActionSpill()
				failpoint.Inject("testSortedRowContainerSpill", func(val failpoint.Value) {
					if val.(bool) {
						e.Unparallel.spillAction = e.Unparallel.rowChunks.ActionSpillForTest()
						defer e.Unparallel.spillAction.WaitForTest()
					}
				})
				e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.Unparallel.spillAction)
				err = e.Unparallel.rowChunks.Add(chk)
			}
			if err != nil {
				return err
			}
		}
	}
	failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			if e.Ctx().GetSessionVars().ConnectionID == 123456 {
				e.Ctx().GetSessionVars().MemTracker.Killer.SendKillSignal(sqlkiller.QueryMemoryExceeded)
			}
		}
	})
	if e.Unparallel.rowChunks.NumRow() > 0 {
		err := e.Unparallel.rowChunks.Sort()
		if err != nil {
			return err
		}
		e.Unparallel.PartitionList = append(e.Unparallel.PartitionList, e.Unparallel.rowChunks)
	}
	return nil
}

func (e *SortExec) fetchChunksParallel(ctx context.Context) error {
	workerNum := len(e.Parallel.workers)
	publicSpace := publicMergeSpace{}
	waitGroup := sync.WaitGroup{}

	// Add before the start of goroutine to avoid that the counter in waitGroup is minus to negative.
	waitGroup.Add(workerNum + 1)

	// Fetch chunks from child and put chunks into MPMCQueue
	go e.fetchChunksFromChild(ctx, e.Parallel.mpmcQueue, &waitGroup)

	// Create workers
	for i := range e.Parallel.workers {
		e.Parallel.workers[i] = newParallelSortWorker(&publicSpace, &waitGroup, &e.Parallel.result, e.Parallel.mpmcQueue, e.checkErrorForParallel, e.processErrorForParallel, e.memTracker)
	}

	// Run workers
	for _, worker := range e.Parallel.workers {
		go worker.run()
	}

	// Wait for the finish of all goroutines
	waitGroup.Wait()

	e.getResult(&publicSpace)
	return nil
}

func (e *SortExec) fetchChunksFromChild(ctx context.Context, mpmcQueue *chunk.MPMCQueue, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			processErrorAndLog(e.processErrorForParallel, r)
		}
		waitGroup.Done()
	}()

	for {
		chk := exec.TryNewCacheChunk(e.Children(0))
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			e.processErrorForParallel(err)
			return
		}

		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}

		chkWithMemoryUsage := &chunk.ChunkWithMemoryUsage{
			Chk:         chk,
			MemoryUsage: chk.MemoryUsage() + chunk.RowSize*int64(rowCount),
		}

		e.memTracker.Consume(chkWithMemoryUsage.MemoryUsage)

		// Push chunk into mpmcQueue.
		res := e.Parallel.mpmcQueue.Push(chkWithMemoryUsage)
		if res != chunk.OK {
			return
		}

		err = e.checkErrorForParallel()
		if err != nil {
			return
		}
	}
}

func (e *SortExec) checkErrorForParallel() error {
	e.Parallel.errRWLock.RLock()
	defer e.Parallel.errRWLock.RUnlock()
	return e.Parallel.err
}

func (e *SortExec) processErrorForParallel(err error) {
	e.Parallel.errRWLock.Lock()
	defer e.Parallel.errRWLock.Unlock()
	e.Parallel.err = err
	e.Parallel.mpmcQueue.Close()
}

func (e *SortExec) getResult(publicSpace *publicMergeSpace) {
	partitionNum := publicSpace.publicQueue.Len()
	if partitionNum > 1 {
		panic("Sort is not completed.")
	}

	if partitionNum == 0 {
		e.Parallel.rowNum = 0
		return
	}

	sortedData := popFromList(&publicSpace.publicQueue)
	e.Parallel.rowNum = int64(len(sortedData))
	e.Parallel.result = sortedData
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyColumns() {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

func (e *SortExec) compressRow(rowI, rowJ chunk.Row) int {
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
