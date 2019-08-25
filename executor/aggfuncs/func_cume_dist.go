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

type cumeDist struct {
	baseAggFunc
	rowComparer
}

type partialResult4CumeDist struct {
	partialResult4Rank
	cum int64
}

func (r *cumeDist) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4CumeDist{})
}

func (r *cumeDist) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CumeDist)(pr)
	p.partialResult4Rank.reset()
	p.cum = 0
}

func (r *cumeDist) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4CumeDist)(pr)
	p.partialResult4Rank.updatePartialResult(rowsInGroup, false, r.compareRows)
	return nil
}

func (r *cumeDist) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CumeDist)(pr)
	numRows := int64(len(p.results))
	for p.cum < numRows && p.results[p.cum] == p.results[p.curIdx] {
		p.cum++
	}
	p.curIdx++
	chk.AppendFloat64(r.ordinal, float64(p.cum)/float64(numRows))
	return nil
}
