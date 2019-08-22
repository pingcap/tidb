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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// percentRank calculates the percentage of partition values less than the value in the current row, excluding the highest value.
// It can be calculated as `(rank - 1) / (total_rows_in_set - 1).
type percentRank struct {
	baseAggFunc
	rowComparer
}

func (pr *percentRank) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4Rank{})
}

func (pr *percentRank) ResetPartialResult(partial PartialResult) {
	p := (*partialResult4Rank)(partial)
	p.reset()
}

func (pr *percentRank) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partial PartialResult) error {
	p := (*partialResult4Rank)(partial)
	p.updatePartialResult(rowsInGroup, false, pr.compareRows)
	return nil
}

func (pr *percentRank) AppendFinalResult2Chunk(sctx sessionctx.Context, partial PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Rank)(partial)
	numRows := len(p.results)
	if numRows == 1 {
		chk.AppendFloat64(pr.ordinal, 0)
	} else {
		chk.AppendFloat64(pr.ordinal, float64(p.results[p.curIdx]-1)/float64(numRows-1))
	}
	p.curIdx++
	return nil
}
