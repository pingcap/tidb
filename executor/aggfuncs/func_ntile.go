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
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/util/chunk"
)

// ntile divides the partition into n ranked groups and returns the group number a row belongs to.
// e.g. We have 11 rows and n = 3. They will be divided into 3 groups.
//      First 4 rows belongs to group 1. Following 4 rows belongs to group 2. The last 3 rows belongs to group 3.
type ntile struct {
	n uint64
	baseAggFunc
}

type partialResult4Ntile struct {
	curIdx      uint64
	curGroupIdx uint64
	remainder   uint64
	quotient    uint64
	numRows     uint64
}

func (n *ntile) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4Ntile{curGroupIdx: 1})
}

func (n *ntile) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Ntile)(pr)
	p.curIdx = 0
	p.curGroupIdx = 1
	p.numRows = 0
}

func (n *ntile) UpdatePartialResult(_ sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Ntile)(pr)
	p.numRows += uint64(len(rowsInGroup))
	// Update the quotient and remainder.
	if n.n != 0 {
		p.quotient = p.numRows / n.n
		p.remainder = p.numRows % n.n
	}
	return nil
}

func (n *ntile) AppendFinalResult2Chunk(_ sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Ntile)(pr)

	// If the divisor is 0, the arg of NTILE would be NULL. So we just return NULL.
	if n.n == 0 {
		chk.AppendNull(n.ordinal)
		return nil
	}

	chk.AppendUint64(n.ordinal, p.curGroupIdx)

	p.curIdx++
	curMaxIdx := p.quotient
	if p.curGroupIdx <= p.remainder {
		curMaxIdx++
	}
	if p.curIdx == curMaxIdx {
		p.curIdx = 0
		p.curGroupIdx++
	}
	return nil
}
