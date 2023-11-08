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

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// DefPartialResult4BitFuncSize the size of partialResult4BitFunc
	DefPartialResult4BitFuncSize = int64(unsafe.Sizeof(partialResult4BitFuncMetaType(0)))
)

type baseBitAggFunc struct {
	baseAggFunc
}

type partialResult4BitFuncMetaType = uint64

func (*baseBitAggFunc) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4BitFuncMetaType)), DefPartialResult4BitFuncSize
}

func (*baseBitAggFunc) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4BitFuncMetaType)(pr)
	*p = 0
}

func (b *baseBitAggFunc) AppendFinalResult2Chunk(_ sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4BitFuncMetaType)(pr)
	chk.AppendUint64(b.ordinal, *p)
	return nil
}

func (b *baseBitAggFunc) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SpillSerializeHelper) {
	pr := (*partialResult4BitFuncMetaType)(partialResult)
	resBuf := spillHelper.serializePartialResult4BitFunc(*pr)
	chk.AppendBytes(b.ordinal, resBuf)
}

func (b *baseBitAggFunc) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, b.ordinal, b.deserializeForSpill)
}

func (b *baseBitAggFunc) deserializeForSpill(helper *spillDeserializeHelper) (PartialResult, int64) {
	pr, memDelta := b.AllocPartialResult()
	result := (*partialResult4BitFuncMetaType)(pr)
	success := helper.deserializePartialResult4BitFunc(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type bitOrUint64 struct {
	baseBitAggFunc
}

func (b *bitOrUint64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4BitFuncMetaType)(pr)
	for _, row := range rowsInGroup {
		inputValue, isNull, err := b.args[0].EvalInt(sctx, row)
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

func (*bitOrUint64) MergePartialResult(_ sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4BitFuncMetaType)(src), (*partialResult4BitFuncMetaType)(dst)
	*p2 |= *p1
	return memDelta, nil
}

type bitXorUint64 struct {
	baseBitAggFunc
}

func (b *bitXorUint64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4BitFuncMetaType)(pr)
	for _, row := range rowsInGroup {
		inputValue, isNull, err := b.args[0].EvalInt(sctx, row)
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

func (b *bitXorUint64) Slide(sctx sessionctx.Context, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4BitFuncMetaType)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		inputValue, isNull, err := b.args[0].EvalInt(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p ^= uint64(inputValue)
	}
	for i := uint64(0); i < shiftEnd; i++ {
		inputValue, isNull, err := b.args[0].EvalInt(sctx, getRow(lastEnd+i))
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

func (*bitXorUint64) MergePartialResult(_ sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4BitFuncMetaType)(src), (*partialResult4BitFuncMetaType)(dst)
	*p2 ^= *p1
	return memDelta, nil
}

type bitAndUint64 struct {
	baseBitAggFunc
}

func (*bitAndUint64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4BitFuncMetaType)
	*p = math.MaxUint64
	return PartialResult(p), DefPartialResult4BitFuncSize
}

func (*bitAndUint64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4BitFuncMetaType)(pr)
	*p = math.MaxUint64
}

func (b *bitAndUint64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4BitFuncMetaType)(pr)
	for _, row := range rowsInGroup {
		inputValue, isNull, err := b.args[0].EvalInt(sctx, row)
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

func (*bitAndUint64) MergePartialResult(_ sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4BitFuncMetaType)(src), (*partialResult4BitFuncMetaType)(dst)
	*p2 &= *p1
	return memDelta, nil
}
