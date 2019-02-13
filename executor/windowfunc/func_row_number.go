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

package windowfuncs

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type rowNumber struct {
	baseWindowFunc
}

type partialResult4RowNumber struct {
	curIdx int
}

func (wf *rowNumber) ProcessOneChunk(sctx sessionctx.Context, rows []chunk.Row, pr PartialResult, dest *chunk.Chunk, remained int) error {
	p := (*partialResult4RowNumber)(pr)
	processedRows, numRows := 0, len(rows)
	for p.curIdx < numRows && processedRows < remained {
		p.curIdx++
		processedRows++
		dest.AppendInt64(wf.ordinal, int64(p.curIdx))
	}
	return nil
}

func (wf *rowNumber) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4RowNumber{})
}

func (wf *rowNumber) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4RowNumber)(pr)
	p.curIdx = 0
}
