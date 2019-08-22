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
	seenRows int64
	results  []int64
	lastRow  chunk.Row
}

func (p *partialResult4Rank) reset() {
	p.curIdx = 0
	p.seenRows = 0
	p.results = p.results[:0]
}

func (p *partialResult4Rank) updatePartialResult(
	rowsInGroup []chunk.Row,
	isDense bool,
	compareRows func(prev, curr chunk.Row) int,
) {
	if len(rowsInGroup) == 0 {
		return
	}
	lastRow := p.lastRow
	for _, row := range rowsInGroup {
		p.seenRows++
		if p.seenRows == 1 {
			p.results = append(p.results, 1)
			lastRow = row
			continue
		}
		var rank int64
		if compareRows(lastRow, row) == 0 {
			rank = p.results[len(p.results)-1]
		} else if isDense {
			rank = p.results[len(p.results)-1] + 1
		} else {
			rank = p.seenRows
		}
		p.results = append(p.results, rank)
		lastRow = row
	}
	p.lastRow = rowsInGroup[len(rowsInGroup)-1].CopyConstruct()
}

func (r *rank) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4Rank{})
}

func (r *rank) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Rank)(pr)
	p.reset()
}

func (r *rank) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Rank)(pr)
	p.updatePartialResult(rowsInGroup, r.isDense, r.compareRows)
	return nil
}

func (r *rank) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Rank)(pr)
	chk.AppendInt64(r.ordinal, p.results[p.curIdx])
	p.curIdx++
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
