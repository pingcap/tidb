// Copyright 2019 PingCAP, Inc.
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

package aggfuncs

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type rank struct {
	baseAggFunc
	isDense bool
	rowComparer
}

type partialResult4Rank struct {
	curIdx   int64
	lastRank int64
	rows     []chunk.Row
}

func (r *rank) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4Rank{})
}

func (r *rank) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Rank)(pr)
	p.curIdx = 0
	p.lastRank = 0
	p.rows = p.rows[:0]
}

func (r *rank) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (int, error) {
	p := (*partialResult4Rank)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	return 0, nil
}

func (r *rank) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Rank)(pr)
	p.curIdx++
	if p.curIdx == 1 {
		p.lastRank = 1
		chk.AppendInt64(r.ordinal, p.lastRank)
		return nil
	}
	if r.compareRows(p.rows[p.curIdx-2], p.rows[p.curIdx-1]) == 0 {
		chk.AppendInt64(r.ordinal, p.lastRank)
		return nil
	}
	if r.isDense {
		p.lastRank++
	} else {
		p.lastRank = p.curIdx
	}
	chk.AppendInt64(r.ordinal, p.lastRank)
	return nil
}

type rowComparer struct {
	cmpFuncs []chunk.CompareFunc
	colIdx   []int
}

func buildRowComparer(cols []*expression.Column) rowComparer {
	rc := rowComparer{}
	rc.colIdx = make([]int, 0, len(cols))
	rc.cmpFuncs = make([]chunk.CompareFunc, 0, len(cols))
	for _, col := range cols {
		cmpFunc := chunk.GetCompareFunc(col.RetType)
		if cmpFunc == nil {
			continue
		}
		rc.cmpFuncs = append(rc.cmpFuncs, chunk.GetCompareFunc(col.RetType))
		rc.colIdx = append(rc.colIdx, col.Index)
	}
	return rc
}

func (rc *rowComparer) compareRows(prev, curr chunk.Row) int {
	for i, idx := range rc.colIdx {
		res := rc.cmpFuncs[i](prev, idx, curr, idx)
		if res != 0 {
			return res
		}
	}
	return 0
}
