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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
)

// SortExec represents sorting executor.
type SortExec struct {
	exec.BaseExecutor

	ByItems    []*plannerutil.ByItems
	fetched    *atomic.Bool
	ExecSchema *expression.Schema

	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc

	curPartition *sortPartition

	// We can't spill if size of data is lower than the limit
	spillLimit int64

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	IsUnparallel bool

	finishCh chan struct{}

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMergeImpl

	Unparallel struct {
		Idx int

		// sortPartitions is the chunks to store row values for partitions. Every partition is a sorted list.
		sortPartitions []*sortPartition

		spillAction *sortPartitionSpillDiskAction
	}

	Parallel struct {
		chunkChannel chan *chunkWithMemoryUsage
		// It's useful when spill is triggered and the fetcher could know when workers finish their works.
		fetcherAndWorkerSyncer *sync.WaitGroup
		workers                []*parallelSortWorker

		// Each worker will put their results into the given iter
		sortedRowsIters []*chunk.Iterator4Slice
		merger          *multiWayMerger

		resultChannel chan rowWithError

		closeSync chan struct{}

		spillHelper *parallelSortSpillHelper
		spillAction *parallelSortSpillAction
	}

	enableTmpStorageOnOOM bool
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	// TopN not initialize `e.finishCh` but it will call the Close function
	if e.finishCh != nil {
		close(e.finishCh)
	}
	if e.Unparallel.spillAction != nil {
		e.Unparallel.spillAction.SetFinished()
	}

	if e.IsUnparallel {
		for _, partition := range e.Unparallel.sortPartitions {
			partition.close()
		}
	} else if e.finishCh != nil {
		if e.fetched.CompareAndSwap(false, true) {
			close(e.Parallel.resultChannel)
			close(e.Parallel.chunkChannel)
		} else {
			for range e.Parallel.chunkChannel {
				e.Parallel.fetcherAndWorkerSyncer.Done()
			}
		}

		<-e.Parallel.closeSync

		// Ensure that `generateResult()` has exited,
		// or data race may happen as `generateResult()`
		// will use `e.Parallel.workers` and `e.Parallel.merger`.
		<-e.Parallel.resultChannel
		for i := range e.Parallel.workers {
			e.Parallel.workers[i].batchRows = nil
			e.Parallel.workers[i].localSortedRows = nil
			e.Parallel.workers[i].sortedRowsIter = nil
			e.Parallel.workers[i].merger = nil
			e.Parallel.workers[i].memTracker.ReplaceBytesUsed(0)
		}
		e.Parallel.merger = nil
		if e.Parallel.spillAction != nil {
			e.Parallel.spillAction.SetFinished()
		}
	}

	if e.memTracker != nil {
		e.memTracker.ReplaceBytesUsed(0)
	}

	return exec.Close(e.Children(0))
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = &atomic.Bool{}
	e.fetched.Store(false)
	e.enableTmpStorageOnOOM = variable.EnableTmpStorageOnOOM.Load()
	e.finishCh = make(chan struct{}, 1)

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.ID(), -1)
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
		e.spillLimit = e.Ctx().GetSessionVars().MemTracker.GetBytesLimit() / 10
		e.diskTracker = disk.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)
	}

	e.IsUnparallel = !e.Ctx().GetSessionVars().EnableParallelSort
	if e.IsUnparallel {
		e.Unparallel.Idx = 0
		e.Unparallel.sortPartitions = e.Unparallel.sortPartitions[:0]
	} else {
		e.Parallel.workers = make([]*parallelSortWorker, e.Ctx().GetSessionVars().ExecutorConcurrency)
		e.Parallel.chunkChannel = make(chan *chunkWithMemoryUsage, e.Ctx().GetSessionVars().ExecutorConcurrency)
		e.Parallel.fetcherAndWorkerSyncer = &sync.WaitGroup{}
		e.Parallel.sortedRowsIters = make([]*chunk.Iterator4Slice, len(e.Parallel.workers))
		e.Parallel.resultChannel = make(chan rowWithError, e.MaxChunkSize())
		e.Parallel.closeSync = make(chan struct{})
		e.Parallel.merger = newMultiWayMerger(e.Parallel.sortedRowsIters, e.lessRow)
		e.Parallel.spillHelper = newParallelSortSpillHelper(e, exec.RetTypes(e), e.finishCh, e.lessRow, e.Parallel.resultChannel)
		e.Parallel.spillAction = newParallelSortSpillDiskAction(e.Parallel.spillHelper)
		for i := range e.Parallel.sortedRowsIters {
			e.Parallel.sortedRowsIters[i] = chunk.NewIterator4Slice(nil)
		}
		if e.enableTmpStorageOnOOM {
			e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.Parallel.spillAction)
		}
	}

	return exec.Open(ctx, e.Children(0))
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
// Here we explain the execution flow of the parallel sort implementation.
// There are 3 main components:
//  1. Chunks Fetcher: Fetcher is responsible for fetching chunks from child and send them to channel.
//  2. Parallel Sort Worker: Worker receives chunks from channel it will sort these chunks after the
//     number of rows in these chunks exceeds limit, we call them as sorted rows after chunks are sorted.
//     Then each worker will have several sorted rows, we use multi-way merge to sort them and each worker
//     will have only one sorted rows in the end.
//  3. Result Generator: Generator gets n sorted rows from n workers, it will use multi-way merge to sort
//     these rows, once it gets the next row, it will send it into `resultChannel` and the goroutine who
//     calls `Next()` will fetch result from `resultChannel`.
/*
                        ┌─────────┐
                        │  Child  │
                        └────▲────┘
                             │
                           Fetch
                             │
                     ┌───────┴───────┐
                     │ Chunk Fetcher │
                     └───────┬───────┘
                             │
                            Push
                             │
                             ▼
        ┌────────────────►Channel◄───────────────────┐
        │                    ▲                       │
        │                    │                       │
      Fetch                Fetch                   Fetch
        │                    │                       │
   ┌────┴───┐            ┌───┴────┐              ┌───┴────┐
   │ Worker │            │ Worker │   ......     │ Worker │
   └────┬───┘            └───┬────┘              └───┬────┘
        │                    │                       │
        │                    │                       │
       Sort                 Sort                    Sort
        │                    │                       │
        │                    │                       │
 ┌──────┴──────┐      ┌──────┴──────┐         ┌──────┴──────┐
 │ Sorted Rows │      │ Sorted Rows │ ......  │ Sorted Rows │
 └──────▲──────┘      └──────▲──────┘         └──────▲──────┘
        │                    │                       │
	   Pull                 Pull                    Pull
        │                    │                       │
        └────────────────────┼───────────────────────┘
                             │
                     Multi-way Merge
                             │
                      ┌──────┴──────┐
                      │  Generator  │
                      └──────┬──────┘
                             │
                            Push
                             │
                             ▼
                       resultChannel
*/
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.fetched.CompareAndSwap(false, true) {
		e.initCompareFuncs()
		e.buildKeyColumns()
		err := e.fetchChunks(ctx)
		if err != nil {
			return err
		}
	}

	req.Reset()
	if e.IsUnparallel {
		return e.appendResultToChunkInUnparallelMode(req)
	}
	return e.appendResultToChunkInParallelMode(req)
}

func (e *SortExec) appendResultToChunkInParallelMode(req *chunk.Chunk) error {
	for !req.IsFull() {
		row, ok := <-e.Parallel.resultChannel
		if row.err != nil {
			return row.err
		}
		if !ok {
			return nil
		}
		req.AppendRow(row.row)
	}
	return nil
}

func (e *SortExec) appendResultToChunkInUnparallelMode(req *chunk.Chunk) error {
	sortPartitionListLen := len(e.Unparallel.sortPartitions)
	if sortPartitionListLen == 0 {
		return nil
	}

	if sortPartitionListLen == 1 {
		if err := e.onePartitionSorting(req); err != nil {
			return err
		}
	} else {
		if err := e.externalSorting(req); err != nil {
			return err
		}
	}
	return nil
}

func (e *SortExec) generateResultWhenSpillTriggeredOnlyOnce() {
	inDisk := e.Parallel.spillHelper.sortedRowsInDisk[0]
	chunkNum := inDisk.NumChunks()
	for i := 0; i < chunkNum; i++ {
		chk, err := inDisk.GetChunk(i)
		if err != nil {
			e.Parallel.resultChannel <- rowWithError{err: err}
			return
		}

		injectParallelSortRandomFail(1)

		rowNum := chk.NumRows()
		for j := 0; j < rowNum; j++ {
			select {
			case <-e.finishCh:
				return
			case e.Parallel.resultChannel <- rowWithError{row: chk.GetRow(j)}:
			}
		}
	}
}

func (e *SortExec) generateResultWhenSpillTriggeredWithMulWayMerge() error {
	inDiskNum := len(e.Parallel.spillHelper.sortedRowsInDisk)
	multiWayMerge := &multiWayMergeImpl{
		lessRowFunction: e.lessRow,
		elements:        make([]rowWithPartition, 0, inDiskNum),
	}

	cursors := make([]*dataCursor, 0, inDiskNum)

	// Init multiWayMerge
	for i := 0; i < inDiskNum; i++ {
		chk, err := e.Parallel.spillHelper.sortedRowsInDisk[i].GetChunk(0)
		if err != nil {
			return err
		}
		cursor := NewDataCursor()
		cursor.setChunk(chk, 0)
		cursors = append(cursors, cursor)
		row := cursor.begin()
		if row.IsEmpty() {
			continue
		}
		multiWayMerge.elements = append(multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(multiWayMerge)

	// multi-way merge the data in disk
	for multiWayMerge.Len() > 0 {
		elem := multiWayMerge.elements[0]
		select {
		case <-e.finishCh:
			return nil
		case e.Parallel.resultChannel <- rowWithError{row: elem.row}:
		}

		partitionID := elem.partitionID
		newRow := cursors[partitionID].next()
		if newRow.IsEmpty() {
			// Try to fetch more data from the disk
			success, err := reloadCursor(cursors[partitionID], e.Parallel.spillHelper.sortedRowsInDisk[partitionID])
			if err != nil {
				e.Parallel.resultChannel <- rowWithError{err: err}
				return err
			}

			if !success {
				// All data in this inDisk has been consumed
				heap.Remove(multiWayMerge, 0)
				continue
			}

			// Get new row
			newRow = cursors[partitionID].begin()
			if newRow.IsEmpty() {
				err := errors.New("Get an empty row")
				e.Parallel.resultChannel <- rowWithError{err: err}
				return err
			}
		}
		multiWayMerge.elements[0].row = newRow
		heap.Fix(multiWayMerge, 0)

		injectParallelSortRandomFail(1)
	}
	return nil
}

func (e *SortExec) generateResultWhenSpillTriggered() {
	inDiskNum := len(e.Parallel.spillHelper.sortedRowsInDisk)
	if inDiskNum == 0 {
		panic("inDiskNum can't be 0 when we generate result with spill triggered")
	}

	if inDiskNum == 1 {
		e.generateResultWhenSpillTriggeredOnlyOnce()
		return
	}
	e.generateResultWhenSpillTriggeredWithMulWayMerge()
}

// Return true when spill is triggered
func (e *SortExec) generateResultInMemory() bool {
	if e.Parallel.merger == nil {
		// Sort has been closed
		return false
	}
	e.Parallel.merger.init()

	maxChunkSize := e.MaxChunkSize()
	resBuf := make([]rowWithError, 0, maxChunkSize)
	var row chunk.Row
	for {
		resBuf = resBuf[:0]
		for i := 0; i < maxChunkSize; i++ {
			row = e.Parallel.merger.next()
			if row.IsEmpty() {
				break
			}
			resBuf = append(resBuf, rowWithError{row: row, err: nil})
		}

		if len(resBuf) == 0 {
			return false
		}

		for _, row := range resBuf {
			select {
			case <-e.finishCh:
				return false
			case e.Parallel.resultChannel <- row:
			}
		}

		injectParallelSortRandomFail(3)

		if e.Parallel.spillHelper.isSpillNeeded() {
			return true
		}
	}
}

func (e *SortExec) generateResult(waitGroups ...*util.WaitGroupWrapper) {
	for _, waitGroup := range waitGroups {
		waitGroup.Wait()
	}
	close(e.Parallel.closeSync)

	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(e.Parallel.resultChannel, r)
			logutil.BgLogger().Error("parallel sort panicked", zap.Error(util.GetRecoverError(r)), zap.Stack("stack"))
		}

		close(e.Parallel.resultChannel)
		for i := range e.Parallel.sortedRowsIters {
			e.Parallel.sortedRowsIters[i].Reset(nil)
		}
		if e.Parallel.spillHelper.isSpillTriggered() {
			for _, disk := range e.Parallel.spillHelper.sortedRowsInDisk {
				disk.Close()
			}
		}
		e.Parallel.merger = nil
	}()

	if e.Parallel.spillHelper.isSpillTriggered() {
		e.generateResultWhenSpillTriggered()
		return
	}

	spillTriggered := e.generateResultInMemory()
	if spillTriggered {
		err := e.spillSortedRowsInMemory()
		if err != nil {
			e.Parallel.resultChannel <- rowWithError{err: err}
			return
		}
		e.generateResultWhenSpillTriggered()
	}
}

func (e *SortExec) spillSortedRowsInMemory() error {
	return e.Parallel.spillHelper.spillImpl(e.Parallel.merger)
}

func (e *SortExec) initExternalSorting() error {
	e.multiWayMerge = &multiWayMergeImpl{e.lessRow, make([]rowWithPartition, 0, len(e.Unparallel.sortPartitions))}
	for i := 0; i < len(e.Unparallel.sortPartitions); i++ {
		// We should always get row here
		row, err := e.Unparallel.sortPartitions[i].getNextSortedRow()
		if err != nil {
			return err
		}

		e.multiWayMerge.elements = append(e.multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(e.multiWayMerge)
	return nil
}

func (e *SortExec) onePartitionSorting(req *chunk.Chunk) (err error) {
	err = e.Unparallel.sortPartitions[0].checkError()
	if err != nil {
		return err
	}

	for !req.IsFull() {
		row, err := e.Unparallel.sortPartitions[0].getNextSortedRow()
		if err != nil {
			return err
		}

		if row.IsEmpty() {
			return nil
		}

		req.AppendRow(row)
	}
	return nil
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
	// We only need to check error for the last partition as previous partitions
	// have been checked when we call `switchToNewSortPartition` function.
	err = e.Unparallel.sortPartitions[len(e.Unparallel.sortPartitions)-1].checkError()
	if err != nil {
		return err
	}

	if e.multiWayMerge == nil {
		err := e.initExternalSorting()
		if err != nil {
			return err
		}
	}

	for !req.IsFull() && e.multiWayMerge.Len() > 0 {
		// Get and insert data
		element := e.multiWayMerge.elements[0]
		req.AppendRow(element.row)

		// Get a new row from that partition which inserted data belongs to
		partitionID := element.partitionID
		row, err := e.Unparallel.sortPartitions[partitionID].getNextSortedRow()
		if err != nil {
			return err
		}

		if row.IsEmpty() {
			// All data in this partition have been consumed
			heap.Remove(e.multiWayMerge, 0)
			continue
		}

		e.multiWayMerge.elements[0].row = row
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

func (e *SortExec) switchToNewSortPartition(fields []*types.FieldType, byItemsDesc []bool, appendPartition bool) error {
	if appendPartition {
		// Put the full partition into list
		e.Unparallel.sortPartitions = append(e.Unparallel.sortPartitions, e.curPartition)
	}

	if e.curPartition != nil {
		err := e.curPartition.checkError()
		if err != nil {
			return err
		}
	}

	e.curPartition = newSortPartition(fields, byItemsDesc, e.keyColumns, e.keyCmpFuncs, e.spillLimit)
	e.curPartition.getMemTracker().AttachTo(e.memTracker)
	e.curPartition.getMemTracker().SetLabel(memory.LabelForRowChunks)
	e.Unparallel.spillAction = e.curPartition.actionSpill()
	if e.enableTmpStorageOnOOM {
		e.curPartition.getDiskTracker().AttachTo(e.diskTracker)
		e.curPartition.getDiskTracker().SetLabel(memory.LabelForRowChunks)
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.Unparallel.spillAction)
	}
	return nil
}

func (e *SortExec) checkError() error {
	for _, partition := range e.Unparallel.sortPartitions {
		err := partition.checkError()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *SortExec) storeChunk(chk *chunk.Chunk, fields []*types.FieldType, byItemsDesc []bool) error {
	err := e.curPartition.checkError()
	if err != nil {
		return err
	}

	if !e.curPartition.add(chk) {
		err := e.switchToNewSortPartition(fields, byItemsDesc, true)
		if err != nil {
			return err
		}

		if !e.curPartition.add(chk) {
			return errFailToAddChunk
		}
	}
	return nil
}

func (e *SortExec) handleCurrentPartitionBeforeExit() error {
	err := e.checkError()
	if err != nil {
		return err
	}

	err = e.curPartition.sort()
	if err != nil {
		return err
	}

	return nil
}

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

	// Fetch chunks from child and put chunks into chunkChannel
	fetcherWaiter.Run(func() {
		e.fetchChunksFromChild(ctx)
	})

	for i := range e.Parallel.workers {
		e.Parallel.workers[i] = newParallelSortWorker(i, e.lessRow, e.Parallel.chunkChannel, e.Parallel.fetcherAndWorkerSyncer, e.Parallel.resultChannel, e.finishCh, e.memTracker, e.Parallel.sortedRowsIters[i], e.MaxChunkSize(), e.Parallel.spillHelper)
		worker := e.Parallel.workers[i]
		workersWaiter.Run(func() {
			worker.run()
		})
	}

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
