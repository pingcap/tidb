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

// SortExec represents sorting executor.
type SortExec struct {
	exec.BaseExecutor

	ByItems    []*util.ByItems
	Idx        int
	fetched    bool
	ExecSchema *expression.Schema

	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.SortedRowContainer

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// PartitionList is the chunks to store row values for partitions. Every partition is a sorted list.
	PartitionList []*chunk.SortedRowContainer

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMerge
	// spillAction save the Action for spill disk.
	spillAction *chunk.SortAndSpillDiskAction
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	for _, container := range e.PartitionList {
		err := container.Close()
		if err != nil {
			return err
		}
	}
	e.PartitionList = e.PartitionList[:0]

	if e.rowChunks != nil {
		e.memTracker.Consume(-e.rowChunks.GetMemTracker().BytesConsumed())
		e.rowChunks = nil
	}
	e.memTracker = nil
	e.diskTracker = nil
	e.multiWayMerge = nil
	if e.spillAction != nil {
		e.spillAction.SetFinished()
	}
	e.spillAction = nil
	return exec.Close(e.Children(0))
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.Idx = 0

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.ID(), -1)
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
		e.diskTracker = memory.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)
	}
	e.PartitionList = e.PartitionList[:0]
	return exec.Open(ctx, e.Children(0))
}

// Next implements the Executor Next interface.
// Sort constructs the result following these step:
//  1. Read as mush as rows into memory.
//  2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//     the memory quota trigger and return to step 1
//  3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
//  4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//     just like in-memory sort before.
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.initCompareFuncs()
		e.buildKeyColumns()
		err := e.fetchRowChunks(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}

	if len(e.PartitionList) == 0 {
		return nil
	}
	if len(e.PartitionList) > 1 {
		if err := e.externalSorting(req); err != nil {
			return err
		}
	} else {
		for !req.IsFull() && e.Idx < e.PartitionList[0].NumRow() {
			_, _, err := e.PartitionList[0].GetSortedRowAndAlwaysAppendToChunk(e.Idx, req)
			if err != nil {
				return err
			}
			e.Idx++
		}
	}
	return nil
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
	if e.multiWayMerge == nil {
		e.multiWayMerge = &multiWayMerge{e.lessRow, e.compressRow, make([]partitionPointer, 0, len(e.PartitionList))}
		for i := 0; i < len(e.PartitionList); i++ {
			chk := chunk.New(exec.RetTypes(e), 1, 1)

			row, _, err := e.PartitionList[i].GetSortedRowAndAlwaysAppendToChunk(0, chk)
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
		if partitionPtr.consumed >= e.PartitionList[partitionPtr.partitionID].NumRow() {
			heap.Remove(e.multiWayMerge, 0)
			continue
		}

		partitionPtr.row, _, err = e.PartitionList[partitionPtr.partitionID].
			GetSortedRowAndAlwaysAppendToChunk(partitionPtr.consumed, partitionPtr.chk)
		if err != nil {
			return err
		}
		e.multiWayMerge.elements[0] = partitionPtr
		heap.Fix(e.multiWayMerge, 0)
	}
	return nil
}

func (e *SortExec) fetchRowChunks(ctx context.Context) error {
	fields := exec.RetTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	e.rowChunks = chunk.NewSortedRowContainer(fields, e.MaxChunkSize(), byItemsDesc, e.keyColumns, e.keyCmpFuncs)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
	if variable.EnableTmpStorageOnOOM.Load() {
		e.spillAction = e.rowChunks.ActionSpill()
		failpoint.Inject("testSortedRowContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				e.spillAction = e.rowChunks.ActionSpillForTest()
				defer e.spillAction.WaitForTest()
			}
		})
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
		e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
		e.rowChunks.GetDiskTracker().SetLabel(memory.LabelForRowChunks)
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
		if err := e.rowChunks.Add(chk); err != nil {
			if errors.Is(err, chunk.ErrCannotAddBecauseSorted) {
				e.PartitionList = append(e.PartitionList, e.rowChunks)
				e.rowChunks = chunk.NewSortedRowContainer(fields, e.MaxChunkSize(), byItemsDesc, e.keyColumns, e.keyCmpFuncs)
				e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
				e.rowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
				e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
				e.rowChunks.GetDiskTracker().SetLabel(memory.LabelForRowChunks)
				e.spillAction = e.rowChunks.ActionSpill()
				failpoint.Inject("testSortedRowContainerSpill", func(val failpoint.Value) {
					if val.(bool) {
						e.spillAction = e.rowChunks.ActionSpillForTest()
						defer e.spillAction.WaitForTest()
					}
				})
				e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
				err = e.rowChunks.Add(chk)
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
	if e.rowChunks.NumRow() > 0 {
		err := e.rowChunks.Sort()
		if err != nil {
			return err
		}
		e.PartitionList = append(e.PartitionList, e.rowChunks)
	}
	return nil
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

type partitionPointer struct {
	chk         *chunk.Chunk
	row         chunk.Row
	partitionID int
	consumed    int
}

type multiWayMerge struct {
	lessRowFunction     func(rowI chunk.Row, rowJ chunk.Row) bool
	compressRowFunction func(rowI chunk.Row, rowJ chunk.Row) int
	elements            []partitionPointer
}

func (h *multiWayMerge) Less(i, j int) bool {
	rowI := h.elements[i].row
	rowJ := h.elements[j].row
	return h.lessRowFunction(rowI, rowJ)
}

func (h *multiWayMerge) Len() int {
	return len(h.elements)
}

func (*multiWayMerge) Push(interface{}) {
	// Should never be called.
}

func (h *multiWayMerge) Pop() interface{} {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMerge) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}
