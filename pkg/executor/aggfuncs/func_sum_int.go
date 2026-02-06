// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/set"
)

const (
	// DefPartialResult4SumInt64Size is the size of partialResult4SumInt64
	DefPartialResult4SumInt64Size = int64(unsafe.Sizeof(partialResult4SumInt64{}))
	// DefPartialResult4SumDistinctInt64Size is the size of partialResult4SumDistinctInt64
	DefPartialResult4SumDistinctInt64Size = int64(unsafe.Sizeof(partialResult4SumDistinctInt64{}))
	// DefPartialResult4SumUint64Size is the size of partialResult4SumUint64
	DefPartialResult4SumUint64Size = int64(unsafe.Sizeof(partialResult4SumUint64{}))
	// DefPartialResult4SumDistinctUint64Size is the size of partialResult4SumDistinctUint64
	DefPartialResult4SumDistinctUint64Size = int64(unsafe.Sizeof(partialResult4SumDistinctUint64{}))
)

type partialResult4SumInt64 struct {
	val             int64
	notNullRowCount int64
}

type partialResult4SumDistinctInt64 struct {
	val    int64
	isNull bool
	valSet set.Int64SetWithMemoryUsage
}

type partialResult4SumUint64 struct {
	val             uint64
	notNullRowCount int64
}

type partialResult4SumDistinctUint64 struct {
	val    uint64
	isNull bool
	valSet set.Int64SetWithMemoryUsage
}

type baseSumIntAggFunc struct {
	baseAggFunc
}

type sumInt struct {
	baseSumIntAggFunc
}

var _ SlidingWindowAggFunc = &sumInt{}

func (*sumInt) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumInt64)
	return PartialResult(p), DefPartialResult4SumInt64Size
}

func (*sumInt) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumInt64)(pr)
	p.val = 0
	p.notNullRowCount = 0
}

func (e *sumInt) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumInt64)(pr)
	if p.notNullRowCount == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *sumInt) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumInt64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.notNullRowCount == 0 {
			p.val = input
			p.notNullRowCount = 1
			continue
		}
		sum, err := types.AddInt64(p.val, input)
		if err != nil {
			return 0, err
		}
		p.val = sum
		p.notNullRowCount++
	}
	return 0, nil
}

func (*sumInt) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4SumInt64)(src), (*partialResult4SumInt64)(dst)
	if p1.notNullRowCount == 0 {
		return 0, nil
	}
	if p2.notNullRowCount == 0 {
		p2.val = p1.val
		p2.notNullRowCount = p1.notNullRowCount
		return 0, nil
	}
	sum, err := types.AddInt64(p2.val, p1.val)
	if err != nil {
		return 0, err
	}
	p2.val = sum
	p2.notNullRowCount += p1.notNullRowCount
	return 0, nil
}

func (e *sumInt) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4SumInt64)(partialResult)
	resBuf := spillHelper.serializePartialResult4SumInt64(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *sumInt) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *sumInt) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4SumInt64)(pr)
	success := helper.deserializePartialResult4SumInt64(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

func (e *sumInt) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4SumInt64)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		sum, err := types.AddInt64(p.val, input)
		if err != nil {
			return err
		}
		p.val = sum
		p.notNullRowCount++
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		sum, err := types.SubInt64(p.val, input)
		if err != nil {
			return err
		}
		p.val = sum
		p.notNullRowCount--
	}
	return nil
}

type sumDistinctInt64 struct {
	baseSumIntAggFunc
}

func (*sumDistinctInt64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	setSize := int64(0)
	p := new(partialResult4SumDistinctInt64)
	p.isNull = true
	p.valSet, setSize = set.NewInt64SetWithMemoryUsage()
	return PartialResult(p), DefPartialResult4SumDistinctInt64Size + setSize
}

func (*sumDistinctInt64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctInt64)(pr)
	p.isNull = true
	p.valSet, _ = set.NewInt64SetWithMemoryUsage()
}

func (e *sumDistinctInt64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDistinctInt64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}
		memDelta += p.valSet.Insert(input)
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		sum, err := types.AddInt64(p.val, input)
		if err != nil {
			return memDelta, err
		}
		p.val = sum
	}
	return memDelta, nil
}

func (e *sumDistinctInt64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctInt64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

type sumUint struct {
	baseSumIntAggFunc
}

var _ SlidingWindowAggFunc = &sumUint{}

func (*sumUint) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumUint64)
	return PartialResult(p), DefPartialResult4SumUint64Size
}

func (*sumUint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumUint64)(pr)
	p.val = 0
	p.notNullRowCount = 0
}

func (e *sumUint) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumUint64)(pr)
	if p.notNullRowCount == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}

func (e *sumUint) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumUint64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		uintVal := uint64(input)
		if p.notNullRowCount == 0 {
			p.val = uintVal
			p.notNullRowCount = 1
			continue
		}
		sum, err := types.AddUint64(p.val, uintVal)
		if err != nil {
			return 0, err
		}
		p.val = sum
		p.notNullRowCount++
	}
	return 0, nil
}

func (*sumUint) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4SumUint64)(src), (*partialResult4SumUint64)(dst)
	if p1.notNullRowCount == 0 {
		return 0, nil
	}
	if p2.notNullRowCount == 0 {
		p2.val = p1.val
		p2.notNullRowCount = p1.notNullRowCount
		return 0, nil
	}
	sum, err := types.AddUint64(p2.val, p1.val)
	if err != nil {
		return 0, err
	}
	p2.val = sum
	p2.notNullRowCount += p1.notNullRowCount
	return 0, nil
}

func (e *sumUint) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4SumUint64)(partialResult)
	resBuf := spillHelper.serializePartialResult4SumUint64(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *sumUint) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *sumUint) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4SumUint64)(pr)
	success := helper.deserializePartialResult4SumUint64(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

func (e *sumUint) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4SumUint64)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		sum, err := types.AddUint64(p.val, uint64(input))
		if err != nil {
			return err
		}
		p.val = sum
		p.notNullRowCount++
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		sum, err := types.SubUint64(p.val, uint64(input))
		if err != nil {
			return err
		}
		p.val = sum
		p.notNullRowCount--
	}
	return nil
}

type sumDistinctUint64 struct {
	baseSumIntAggFunc
}

func (*sumDistinctUint64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	setSize := int64(0)
	p := new(partialResult4SumDistinctUint64)
	p.isNull = true
	p.valSet, setSize = set.NewInt64SetWithMemoryUsage()
	return PartialResult(p), DefPartialResult4SumDistinctUint64Size + setSize
}

func (*sumDistinctUint64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctUint64)(pr)
	p.isNull = true
	p.valSet, _ = set.NewInt64SetWithMemoryUsage()
}

func (e *sumDistinctUint64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDistinctUint64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		uintVal := uint64(input)
		key := int64(uintVal)
		if p.valSet.Exist(key) {
			continue
		}
		memDelta += p.valSet.Insert(key)
		if p.isNull {
			p.val = uintVal
			p.isNull = false
			continue
		}
		sum, err := types.AddUint64(p.val, uintVal)
		if err != nil {
			return memDelta, err
		}
		p.val = sum
	}
	return memDelta, nil
}

func (e *sumDistinctUint64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctUint64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}
