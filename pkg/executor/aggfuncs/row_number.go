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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// DefPartialResult4RowNumberSize is the size of partialResult4RowNumberSize
	DefPartialResult4RowNumberSize = int64(unsafe.Sizeof(partialResult4RowNumber{}))
)

type rowNumber struct {
	baseAggFunc
}

type partialResult4RowNumber struct {
	curIdx int64
}

func (*rowNumber) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4RowNumber{}), DefPartialResult4RowNumberSize
}

func (*rowNumber) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4RowNumber)(pr)
	p.curIdx = 0
}

func (*rowNumber) UpdatePartialResult(_ AggFuncUpdateContext, _ []chunk.Row, _ PartialResult) (memDelta int64, err error) {
	return 0, nil
}

func (rn *rowNumber) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4RowNumber)(pr)
	p.curIdx++
	chk.AppendInt64(rn.ordinal, p.curIdx)
	return nil
}

var _ SlidingWindowAggFunc = &rowNumber{}

func (*rowNumber) Slide(_ AggFuncUpdateContext, _ func(uint64) chunk.Row, _, _, _, _ uint64, _ PartialResult) error {
	return nil
}
