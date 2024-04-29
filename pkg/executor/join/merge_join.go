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

package join

import (
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var (
	_ exec.Executor = &MergeJoinExec{}
)

// MergeJoinExec implements the merge join algorithm.
// This operator assumes that two iterators of both sides
// will provide required order on join condition:
// 1. For equal-join, one of the join key from each side
// matches the order given.
// 2. For other cases its preferred not to use SMJ and operator
// will throw error.
type MergeJoinExec struct {
	exec.BaseExecutor

	StmtCtx      *stmtctx.StatementContext
	CompareFuncs []expression.CompareFunc
	Joiner       Joiner
	IsOuterJoin  bool
	Desc         bool

	InnerTable *MergeJoinTable
	OuterTable *MergeJoinTable

	hasMatch bool
	hasNull  bool

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
}

// MergeJoinTable is used for merge join
type MergeJoinTable struct {
	inited     bool
	IsInner    bool
	ChildIndex int
	JoinKeys   []*expression.Column
	Filters    []expression.Expression

	executed          bool
	childChunk        *chunk.Chunk
	childChunkIter    *chunk.Iterator4Chunk
	groupChecker      *vecgroupchecker.VecGroupChecker
	groupRowsSelected []int
	groupRowsIter     chunk.Iterator

	// for inner table, an unbroken group may refer many chunks
	rowContainer *chunk.RowContainer

	// for outer table, save result of filters
	filtersSelected []bool

	memTracker *memory.Tracker
}

func (t *MergeJoinTable) init(executor *MergeJoinExec) {
	child := executor.Children(t.ChildIndex)
	t.childChunk = exec.TryNewCacheChunk(child)
	t.childChunkIter = chunk.NewIterator4Chunk(t.childChunk)

	items := make([]expression.Expression, 0, len(t.JoinKeys))
	for _, col := range t.JoinKeys {
		items = append(items, col)
	}
	vecEnabled := executor.Ctx().GetSessionVars().EnableVectorizedExpression
	t.groupChecker = vecgroupchecker.NewVecGroupChecker(executor.Ctx().GetExprCtx().GetEvalCtx(), vecEnabled, items)
	t.groupRowsIter = chunk.NewIterator4Chunk(t.childChunk)

	if t.IsInner {
		t.rowContainer = chunk.NewRowContainer(child.RetFieldTypes(), t.childChunk.Capacity())
		t.rowContainer.GetMemTracker().AttachTo(executor.memTracker)
		t.rowContainer.GetMemTracker().SetLabel(memory.LabelForInnerTable)
		t.rowContainer.GetDiskTracker().AttachTo(executor.diskTracker)
		t.rowContainer.GetDiskTracker().SetLabel(memory.LabelForInnerTable)
		if variable.EnableTmpStorageOnOOM.Load() {
			actionSpill := t.rowContainer.ActionSpill()
			failpoint.Inject("testMergeJoinRowContainerSpill", func(val failpoint.Value) {
				if val.(bool) {
					actionSpill = t.rowContainer.ActionSpillForTest()
				}
			})
			executor.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(actionSpill)
		}
		t.memTracker = memory.NewTracker(memory.LabelForInnerTable, -1)
	} else {
		t.filtersSelected = make([]bool, 0, executor.MaxChunkSize())
		t.memTracker = memory.NewTracker(memory.LabelForOuterTable, -1)
	}

	t.memTracker.AttachTo(executor.memTracker)
	t.inited = true
	t.memTracker.Consume(t.childChunk.MemoryUsage())
}

func (t *MergeJoinTable) finish() error {
	if !t.inited {
		return nil
	}
	t.memTracker.Consume(-t.childChunk.MemoryUsage())

	if t.IsInner {
		failpoint.Inject("testMergeJoinRowContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				actionSpill := t.rowContainer.ActionSpill()
				actionSpill.WaitForTest()
			}
		})
		if err := t.rowContainer.Close(); err != nil {
			return err
		}
	}

	t.executed = false
	t.childChunk = nil
	t.childChunkIter = nil
	t.groupChecker = nil
	t.groupRowsSelected = nil
	t.groupRowsIter = nil
	t.rowContainer = nil
	t.filtersSelected = nil
	t.memTracker = nil
	return nil
}

func (t *MergeJoinTable) selectNextGroup() {
	t.groupRowsSelected = t.groupRowsSelected[:0]
	begin, end := t.groupChecker.GetNextGroup()
	if t.IsInner && t.hasNullInJoinKey(t.childChunk.GetRow(begin)) {
		return
	}

	for i := begin; i < end; i++ {
		t.groupRowsSelected = append(t.groupRowsSelected, i)
	}
	t.childChunk.SetSel(t.groupRowsSelected)
}

func (t *MergeJoinTable) fetchNextChunk(ctx context.Context, executor *MergeJoinExec) error {
	oldMemUsage := t.childChunk.MemoryUsage()
	err := exec.Next(ctx, executor.Children(t.ChildIndex), t.childChunk)
	t.memTracker.Consume(t.childChunk.MemoryUsage() - oldMemUsage)
	if err != nil {
		return err
	}
	t.executed = t.childChunk.NumRows() == 0
	return nil
}

func (t *MergeJoinTable) fetchNextInnerGroup(ctx context.Context, exec *MergeJoinExec) error {
	t.childChunk.SetSel(nil)
	if err := t.rowContainer.Reset(); err != nil {
		return err
	}

fetchNext:
	if t.executed && t.groupChecker.IsExhausted() {
		// Ensure iter at the end, since sel of childChunk has been cleared.
		t.groupRowsIter.ReachEnd()
		return nil
	}

	isEmpty := true
	// For inner table, rows have null in join keys should be skip by selectNextGroup.
	for isEmpty && !t.groupChecker.IsExhausted() {
		t.selectNextGroup()
		isEmpty = len(t.groupRowsSelected) == 0
	}

	// For inner table, all the rows have the same join keys should be put into one group.
	for !t.executed && t.groupChecker.IsExhausted() {
		if !isEmpty {
			// Group is not empty, hand over the management of childChunk to t.RowContainer.
			if err := t.rowContainer.Add(t.childChunk); err != nil {
				return err
			}
			t.memTracker.Consume(-t.childChunk.MemoryUsage())
			t.groupRowsSelected = nil

			t.childChunk = t.rowContainer.AllocChunk()
			t.childChunkIter = chunk.NewIterator4Chunk(t.childChunk)
			t.memTracker.Consume(t.childChunk.MemoryUsage())
		}

		if err := t.fetchNextChunk(ctx, exec); err != nil {
			return err
		}
		if t.executed {
			break
		}

		isFirstGroupSameAsPrev, err := t.groupChecker.SplitIntoGroups(t.childChunk)
		if err != nil {
			return err
		}
		if isFirstGroupSameAsPrev && !isEmpty {
			t.selectNextGroup()
		}
	}
	if isEmpty {
		goto fetchNext
	}

	// iterate all data in t.RowContainer and t.childChunk
	var iter chunk.Iterator
	if t.rowContainer.NumChunks() != 0 {
		iter = chunk.NewIterator4RowContainer(t.rowContainer)
	}
	if len(t.groupRowsSelected) != 0 {
		if iter != nil {
			iter = chunk.NewMultiIterator(iter, t.childChunkIter)
		} else {
			iter = t.childChunkIter
		}
	}
	t.groupRowsIter = iter
	t.groupRowsIter.Begin()
	return nil
}

func (t *MergeJoinTable) fetchNextOuterGroup(ctx context.Context, exec *MergeJoinExec, requiredRows int) error {
	if t.executed && t.groupChecker.IsExhausted() {
		return nil
	}

	if !t.executed && t.groupChecker.IsExhausted() {
		// It's hard to calculate selectivity if there is any filter or it's inner join,
		// so we just push the requiredRows down when it's outer join and has no filter.
		if exec.IsOuterJoin && len(t.Filters) == 0 {
			t.childChunk.SetRequiredRows(requiredRows, exec.MaxChunkSize())
		}
		err := t.fetchNextChunk(ctx, exec)
		if err != nil || t.executed {
			return err
		}

		t.childChunkIter.Begin()
		t.filtersSelected, err = expression.VectorizedFilter(exec.Ctx().GetExprCtx().GetEvalCtx(), exec.Ctx().GetSessionVars().EnableVectorizedExpression, t.Filters, t.childChunkIter, t.filtersSelected)
		if err != nil {
			return err
		}

		_, err = t.groupChecker.SplitIntoGroups(t.childChunk)
		if err != nil {
			return err
		}
	}

	t.selectNextGroup()
	t.groupRowsIter.Begin()
	return nil
}

func (t *MergeJoinTable) hasNullInJoinKey(row chunk.Row) bool {
	for _, col := range t.JoinKeys {
		ordinal := col.Index
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	if err := e.InnerTable.finish(); err != nil {
		return err
	}
	if err := e.OuterTable.finish(); err != nil {
		return err
	}

	e.hasMatch = false
	e.hasNull = false
	e.memTracker = nil
	e.diskTracker = nil
	return e.BaseExecutor.Close()
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	e.diskTracker = disk.NewTracker(e.ID(), -1)
	e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)

	e.InnerTable.init(e)
	e.OuterTable.init(e)
	return nil
}

// Next implements the Executor Next interface.
// Note the inner group collects all identical keys in a group across multiple chunks, but the outer group just covers
// the identical keys within a chunk, so identical keys may cover more than one chunk.
func (e *MergeJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()

	innerIter := e.InnerTable.groupRowsIter
	outerIter := e.OuterTable.groupRowsIter
	for !req.IsFull() {
		failpoint.Inject("ConsumeRandomPanic", nil)
		if innerIter.Current() == innerIter.End() {
			if err := e.InnerTable.fetchNextInnerGroup(ctx, e); err != nil {
				return err
			}
			innerIter = e.InnerTable.groupRowsIter
		}
		if outerIter.Current() == outerIter.End() {
			if err := e.OuterTable.fetchNextOuterGroup(ctx, e, req.RequiredRows()-req.NumRows()); err != nil {
				return err
			}
			outerIter = e.OuterTable.groupRowsIter
			if e.OuterTable.executed {
				return nil
			}
		}

		cmpResult := -1
		if e.Desc {
			cmpResult = 1
		}
		if innerIter.Current() != innerIter.End() {
			cmpResult, err = e.compare(outerIter.Current(), innerIter.Current())
			if err != nil {
				return err
			}
		}
		// the inner group falls behind
		if (cmpResult > 0 && !e.Desc) || (cmpResult < 0 && e.Desc) {
			innerIter.ReachEnd()
			continue
		}
		// the Outer group falls behind
		if (cmpResult < 0 && !e.Desc) || (cmpResult > 0 && e.Desc) {
			for row := outerIter.Current(); row != outerIter.End() && !req.IsFull(); row = outerIter.Next() {
				e.Joiner.OnMissMatch(false, row, req)
			}
			continue
		}

		for row := outerIter.Current(); row != outerIter.End() && !req.IsFull(); row = outerIter.Next() {
			if !e.OuterTable.filtersSelected[row.Idx()] {
				e.Joiner.OnMissMatch(false, row, req)
				continue
			}
			// compare each Outer item with each inner item
			// the inner maybe not exhausted at one time
			for innerIter.Current() != innerIter.End() {
				matched, isNull, err := e.Joiner.TryToMatchInners(row, innerIter, req)
				if err != nil {
					return err
				}
				e.hasMatch = e.hasMatch || matched
				e.hasNull = e.hasNull || isNull
				if req.IsFull() {
					if innerIter.Current() == innerIter.End() {
						break
					}
					return nil
				}
			}

			if !e.hasMatch {
				e.Joiner.OnMissMatch(e.hasNull, row, req)
			}
			e.hasMatch = false
			e.hasNull = false
			innerIter.Begin()
		}
	}
	return nil
}

func (e *MergeJoinExec) compare(outerRow, innerRow chunk.Row) (int, error) {
	outerJoinKeys := e.OuterTable.JoinKeys
	innerJoinKeys := e.InnerTable.JoinKeys
	for i := range outerJoinKeys {
		cmp, _, err := e.CompareFuncs[i](e.Ctx().GetExprCtx().GetEvalCtx(), outerJoinKeys[i], innerJoinKeys[i], outerRow, innerRow)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return int(cmp), nil
		}
	}
	return 0, nil
}
