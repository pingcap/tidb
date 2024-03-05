// Copyright 2018 PingCAP, Inc.
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
	"math"
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// DefPartialResult4BitFuncSize the size of partialResult4BitFunc
	DefPartialResult4BitFuncSize = int64(unsafe.Sizeof(partialResult4BitFunc(0)))
)

type baseBitAggFunc struct {
	baseAggFunc
}

type partialResult4BitFunc = uint64

func (*baseBitAggFunc) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4BitFunc)), DefPartialResult4BitFuncSize
}

func (*baseBitAggFunc) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4BitFunc)(pr)
	*p = 0
}

func (e *baseBitAggFunc) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4BitFunc)(pr)
	chk.AppendUint64(e.ordinal, *p)
	return nil
}

func (e *baseBitAggFunc) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4BitFunc)(partialResult)
	resBuf := spillHelper.serializePartialResult4BitFunc(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *baseBitAggFunc) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *baseBitAggFunc) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4BitFunc)(pr)
	success := helper.deserializePartialResult4BitFunc(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type bitOrUint64 struct {
	baseBitAggFunc
}

func (e *bitOrUint64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4BitFunc)(pr)
	for _, row := range rowsInGroup {
		inputValue, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		*p |= uint64(inputValue)
	}
	return memDelta, nil
}

func (*bitOrUint64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4BitFunc)(src), (*partialResult4BitFunc)(dst)
	*p2 |= *p1
	return memDelta, nil
}

type bitXorUint64 struct {
	baseBitAggFunc
}

func (e *bitXorUint64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4BitFunc)(pr)
	for _, row := range rowsInGroup {
		inputValue, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		*p ^= uint64(inputValue)
	}
	return memDelta, nil
}

var _ SlidingWindowAggFunc = &bitXorUint64{}

func (e *bitXorUint64) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4BitFunc)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		inputValue, isNull, err := e.args[0].EvalInt(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p ^= uint64(inputValue)
	}
	for i := uint64(0); i < shiftEnd; i++ {
		inputValue, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p ^= uint64(inputValue)
	}
	return nil
}

func (*bitXorUint64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4BitFunc)(src), (*partialResult4BitFunc)(dst)
	*p2 ^= *p1
	return memDelta, nil
}

type bitAndUint64 struct {
	baseBitAggFunc
}

func (*bitAndUint64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4BitFunc)
	*p = math.MaxUint64
	return PartialResult(p), DefPartialResult4BitFuncSize
}

func (*bitAndUint64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4BitFunc)(pr)
	*p = math.MaxUint64
}

func (e *bitAndUint64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4BitFunc)(pr)
	for _, row := range rowsInGroup {
		inputValue, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}

		*p &= uint64(inputValue)
	}
	return memDelta, nil
}

func (*bitAndUint64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4BitFunc)(src), (*partialResult4BitFunc)(dst)
	*p2 &= *p1
	return memDelta, nil
}
