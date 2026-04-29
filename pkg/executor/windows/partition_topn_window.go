// Copyright 2026 PingCAP, Inc.
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

package windows

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// PartitionTopNWindowExec fuses a row_number stream window with an upper-bound
// filter and only produces the first K rows of each partition.
type PartitionTopNWindowExec struct {
	exec.BaseExecutor

	groupChecker *vecgroupchecker.VecGroupChecker
	childResult  *chunk.Chunk

	// limitCount is the per-partition upper bound from row_number() <= K.
	limitCount   uint64
	resultColIdx int

	groupStart int
	groupEnd   int
	// groupRank is the current row_number value within the partition. It keeps
	// increasing across child chunks when one partition spans multiple chunks.
	groupRank uint64
	exhausted bool

	resumeLastPartition bool
}

// Open implements the Executor Open interface.
func (e *PartitionTopNWindowExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.OpenSelf()
}

// OpenSelf initializes the executor state without opening children.
func (e *PartitionTopNWindowExec) OpenSelf() error {
	e.childResult = nil
	e.groupStart = 0
	e.groupEnd = 0
	e.groupRank = 0
	e.exhausted = false
	e.resumeLastPartition = false
	return nil
}

// Close implements the Executor Close interface.
func (e *PartitionTopNWindowExec) Close() error {
	return errors.Trace(e.BaseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *PartitionTopNWindowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.limitCount == 0 {
		return nil
	}
	for !req.IsFull() {
		if e.groupStart >= e.groupEnd {
			drained, err := e.loadNextGroup(ctx)
			if err != nil {
				return err
			}
			if drained {
				return nil
			}
		}
		if e.groupRank >= e.limitCount {
			e.groupStart = e.groupEnd
			continue
		}
		row := e.childResult.GetRow(e.groupStart)
		e.groupStart++
		e.groupRank++
		req.AppendPartialRow(0, row)
		req.AppendInt64(e.resultColIdx, int64(e.groupRank))
	}
	return nil
}

func (e *PartitionTopNWindowExec) loadNextGroup(ctx context.Context) (bool, error) {
	for {
		if e.exhausted {
			return true, nil
		}
		if e.groupChecker.IsExhausted() {
			childResult := exec.TryNewCacheChunk(e.Children(0))
			if err := exec.Next(ctx, e.Children(0), childResult); err != nil {
				return false, errors.Trace(err)
			}
			if childResult.NumRows() == 0 {
				e.exhausted = true
				return true, nil
			}
			e.childResult = childResult
			samePartition, err := e.groupChecker.SplitIntoGroups(childResult)
			if err != nil {
				return false, errors.Trace(err)
			}
			e.resumeLastPartition = samePartition
		}
		begin, end := e.groupChecker.GetNextGroup()
		if begin == end {
			continue
		}
		e.groupStart = begin
		e.groupEnd = end
		if e.resumeLastPartition {
			e.resumeLastPartition = false
		} else {
			e.groupRank = 0
		}
		return false, nil
	}
}
