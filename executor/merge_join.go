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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/v4/config"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/disk"
	"github.com/pingcap/tidb/v4/util/memory"
	"github.com/pingcap/tidb/v4/util/stringutil"
)

// MergeJoinExec implements the merge join algorithm.
// This operator assumes that two iterators of both sides
// will provide required order on join condition:
// 1. For equal-join, one of the join key from each side
// matches the order given.
// 2. For other cases its preferred not to use SMJ and operator
// will throw error.
type MergeJoinExec struct {
	baseExecutor

	stmtCtx      *stmtctx.StatementContext
	compareFuncs []expression.CompareFunc
	joiner       joiner
	isOuterJoin  bool
	desc         bool

	innerTable *mergeJoinTable
	outerTable *mergeJoinTable

	hasMatch bool
	hasNull  bool

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
}

var (
	innerTableLabel fmt.Stringer = stringutil.StringerStr("innerTable")
	outerTableLabel fmt.Stringer = stringutil.StringerStr("outerTable")
)

type mergeJoinTable struct {
	isInner    bool
	childIndex int
	joinKeys   []*expression.Column
	filters    []expression.Expression

	executed          bool
	childChunk        *chunk.Chunk
	childChunkIter    *chunk.Iterator4Chunk
	groupChecker      *vecGroupChecker
	groupRowsSelected []int
	groupRowsIter     chunk.Iterator

	// for inner table, an unbroken group may refer many chunks
	rowContainer *chunk.RowContainer

	// for outer table, save result of filters
	filtersSelected []bool

	memTracker *memory.Tracker
}

func (t *mergeJoinTable) init(exec *MergeJoinExec) {
	child := exec.children[t.childIndex]
	t.childChunk = newFirstChunk(child)
	t.childChunkIter = chunk.NewIterator4Chunk(t.childChunk)

	items := make([]expression.Expression, 0, len(t.joinKeys))
	for _, col := range t.joinKeys {
		items = append(items, col)
	}
	t.groupChecker = newVecGroupChecker(exec.ctx, items)
	t.groupRowsIter = chunk.NewIterator4Chunk(t.childChunk)

	if t.isInner {
		t.rowContainer = chunk.NewRowContainer(child.base().retFieldTypes, t.childChunk.Capacity())
		t.rowContainer.GetMemTracker().AttachTo(exec.memTracker)
		t.rowContainer.GetMemTracker().SetLabel(innerTableLabel)
		t.rowContainer.GetDiskTracker().AttachTo(exec.diskTracker)
		t.rowContainer.GetDiskTracker().SetLabel(innerTableLabel)
		if config.GetGlobalConfig().OOMUseTmpStorage {
			actionSpill := t.rowContainer.ActionSpill()
			exec.ctx.GetSessionVars().StmtCtx.MemTracker.SetActionOnExceed(actionSpill)
		}
		t.memTracker = memory.NewTracker(innerTableLabel, -1)
	} else {
		t.filtersSelected = make([]bool, 0, exec.maxChunkSize)
		t.memTracker = memory.NewTracker(outerTableLabel, -1)
	}

	t.memTracker.AttachTo(exec.memTracker)
	t.memTracker.Consume(t.childChunk.MemoryUsage())
}

func (t *mergeJoinTable) finish() error {
	t.memTracker.Consume(-t.childChunk.MemoryUsage())

	if t.isInner {
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

func (t *mergeJoinTable) selectNextGroup() {
	t.groupRowsSelected = t.groupRowsSelected[:0]
	begin, end := t.groupChecker.getNextGroup()
	if t.isInner && t.hasNullInJoinKey(t.childChunk.GetRow(begin)) {
		return
	}

	for i := begin; i < end; i++ {
		t.groupRowsSelected = append(t.groupRowsSelected, i)
	}
	t.childChunk.SetSel(t.groupRowsSelected)
}

func (t *mergeJoinTable) fetchNextChunk(ctx context.Context, exec *MergeJoinExec) error {
	oldMemUsage := t.childChunk.MemoryUsage()
	err := Next(ctx, exec.children[t.childIndex], t.childChunk)
	t.memTracker.Consume(t.childChunk.MemoryUsage() - oldMemUsage)
	if err != nil {
		return err
	}
	t.executed = t.childChunk.NumRows() == 0
	return nil
}

func (t *mergeJoinTable) fetchNextInnerGroup(ctx context.Context, exec *MergeJoinExec) error {
	t.childChunk.SetSel(nil)
	if err := t.rowContainer.Reset(); err != nil {
		return err
	}

fetchNext:
	if t.executed && t.groupChecker.isExhausted() {
		// Ensure iter at the end, since sel of childChunk has been cleared.
		t.groupRowsIter.ReachEnd()
		return nil
	}

	isEmpty := true
	// For inner table, rows have null in join keys should be skip by selectNextGroup.
	for isEmpty && !t.groupChecker.isExhausted() {
		t.selectNextGroup()
		isEmpty = len(t.groupRowsSelected) == 0
	}

	// For inner table, all the rows have the same join keys should be put into one group.
	for !t.executed && t.groupChecker.isExhausted() {
		if !isEmpty {
			// Group is not empty, hand over the management of childChunk to t.rowContainer.
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

		isFirstGroupSameAsPrev, err := t.groupChecker.splitIntoGroups(t.childChunk)
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

	// iterate all data in t.rowContainer and t.childChunk
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

func (t *mergeJoinTable) fetchNextOuterGroup(ctx context.Context, exec *MergeJoinExec, requiredRows int) error {
	if t.executed && t.groupChecker.isExhausted() {
		return nil
	}

	if !t.executed && t.groupChecker.isExhausted() {
		// It's hard to calculate selectivity if there is any filter or it's inner join,
		// so we just push the requiredRows down when it's outer join and has no filter.
		if exec.isOuterJoin && len(t.filters) == 0 {
			t.childChunk.SetRequiredRows(requiredRows, exec.maxChunkSize)
		}
		err := t.fetchNextChunk(ctx, exec)
		if err != nil || t.executed {
			return err
		}

		t.childChunkIter.Begin()
		t.filtersSelected, err = expression.VectorizedFilter(exec.ctx, t.filters, t.childChunkIter, t.filtersSelected)
		if err != nil {
			return err
		}

		_, err = t.groupChecker.splitIntoGroups(t.childChunk)
		if err != nil {
			return err
		}
	}

	t.selectNextGroup()
	t.groupRowsIter.Begin()
	return nil
}

func (t *mergeJoinTable) hasNullInJoinKey(row chunk.Row) bool {
	for _, col := range t.joinKeys {
		ordinal := col.Index
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	if err := e.innerTable.finish(); err != nil {
		return err
	}
	if err := e.outerTable.finish(); err != nil {
		return err
	}

	e.hasMatch = false
	e.hasNull = false
	e.memTracker = nil
	e.diskTracker = nil
	return e.baseExecutor.Close()
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.diskTracker = disk.NewTracker(e.id, -1)
	e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)

	e.innerTable.init(e)
	e.outerTable.init(e)
	return nil
}

// Next implements the Executor Next interface.
// Note the inner group collects all identical keys in a group across multiple chunks, but the outer group just covers
// the identical keys within a chunk, so identical keys may cover more than one chunk.
func (e *MergeJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()

	innerIter := e.innerTable.groupRowsIter
	outerIter := e.outerTable.groupRowsIter
	for !req.IsFull() {
		if innerIter.Current() == innerIter.End() {
			if err := e.innerTable.fetchNextInnerGroup(ctx, e); err != nil {
				return err
			}
			innerIter = e.innerTable.groupRowsIter
		}
		if outerIter.Current() == outerIter.End() {
			if err := e.outerTable.fetchNextOuterGroup(ctx, e, req.RequiredRows()-req.NumRows()); err != nil {
				return err
			}
			outerIter = e.outerTable.groupRowsIter
			if e.outerTable.executed {
				return nil
			}
		}

		cmpResult := -1
		if e.desc {
			cmpResult = 1
		}
		if innerIter.Current() != innerIter.End() {
			cmpResult, err = e.compare(outerIter.Current(), innerIter.Current())
			if err != nil {
				return err
			}
		}
		// the inner group falls behind
		if (cmpResult > 0 && !e.desc) || (cmpResult < 0 && e.desc) {
			innerIter.ReachEnd()
			continue
		}
		// the outer group falls behind
		if (cmpResult < 0 && !e.desc) || (cmpResult > 0 && e.desc) {
			for row := outerIter.Current(); row != outerIter.End() && !req.IsFull(); row = outerIter.Next() {
				e.joiner.onMissMatch(false, row, req)
			}
			continue
		}

		for row := outerIter.Current(); row != outerIter.End() && !req.IsFull(); row = outerIter.Next() {
			if !e.outerTable.filtersSelected[row.Idx()] {
				e.joiner.onMissMatch(false, row, req)
				continue
			}
			// compare each outer item with each inner item
			// the inner maybe not exhausted at one time
			for innerIter.Current() != innerIter.End() {
				matched, isNull, err := e.joiner.tryToMatchInners(row, innerIter, req)
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
				e.joiner.onMissMatch(e.hasNull, row, req)
			}
			e.hasMatch = false
			e.hasNull = false
			innerIter.Begin()
		}
	}
	return nil
}

func (e *MergeJoinExec) compare(outerRow, innerRow chunk.Row) (int, error) {
	outerJoinKeys := e.outerTable.joinKeys
	innerJoinKeys := e.innerTable.joinKeys
	for i := range outerJoinKeys {
		cmp, _, err := e.compareFuncs[i](e.ctx, outerJoinKeys[i], innerJoinKeys[i], outerRow, innerRow)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return int(cmp), nil
		}
	}
	return 0, nil
}
