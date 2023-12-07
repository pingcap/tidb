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
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// SortBase contains common fields in sort executor.
type SortBase struct {
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
}

func (s *SortBase) openBase() {
	s.fetched = false

	// To avoid duplicated initialization for TopNExec.
	if s.memTracker == nil {
		s.memTracker = memory.NewTracker(s.ID(), -1)
		s.memTracker.AttachTo(s.Ctx().GetSessionVars().StmtCtx.MemTracker)
		s.diskTracker = memory.NewTracker(s.ID(), -1)
		s.diskTracker.AttachTo(s.Ctx().GetSessionVars().StmtCtx.DiskTracker)
	}
}

func (s *SortBase) closeBase() {
	s.memTracker = nil
	s.diskTracker = nil
	s.multiWayMerge = nil
}

func (s *SortBase) initCompareFuncs() {
	s.keyCmpFuncs = make([]chunk.CompareFunc, len(s.ByItems))
	for i := range s.ByItems {
		keyType := s.ByItems[i].Expr.GetType()
		s.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (s *SortBase) buildKeyColumns() {
	s.keyColumns = make([]int, 0, len(s.ByItems))
	for _, by := range s.ByItems {
		col := by.Expr.(*expression.Column)
		s.keyColumns = append(s.keyColumns, col.Index)
	}
}

func (s *SortBase) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range s.keyColumns {
		cmpFunc := s.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if s.ByItems[i].Desc {
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

func (s *SortBase) compressRow(rowI, rowJ chunk.Row) int {
	for i, colIdx := range s.keyColumns {
		cmpFunc := s.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if s.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}


