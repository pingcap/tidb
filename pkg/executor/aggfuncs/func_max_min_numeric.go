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
	"cmp"

	"github.com/pingcap/tidb/pkg/util/chunk"
)
type maxMin4Int struct {
	baseMaxMinAggFunc
}

func (*maxMin4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinInt)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinIntSize
}

func (*maxMin4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinInt)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Int) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinInt)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Int) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinInt)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Int) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinInt)(src), (*partialResult4MaxMinInt)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Int) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinInt)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinInt(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Int) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Int) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinInt)(pr)
	success := helper.deserializePartialResult4MaxMinInt(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4IntSliding struct {
	maxMin4Int
	windowInfo
}

func (e *maxMin4IntSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Int.ResetPartialResult(pr)
	(*partialResult4MaxMinInt)(pr).deque.Reset()
}

func (e *maxMin4IntSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Int.AllocPartialResult()
	(*partialResult4MaxMinInt)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(i.(int64), j.(int64))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4IntSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinInt)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		// MinMaxDeque needs the absolute position of each element, here i only denotes the relative position in rowsInGroup.
		// To get the absolute position, we need to add offset of e.start, which represents the absolute index of start
		// of window.
		err = p.deque.Enqueue(uint64(i)+e.start, input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4IntSliding{}

func (e *maxMin4IntSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinInt)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	// check for unsigned underflow
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Uint struct {
	baseMaxMinAggFunc
}

func (*maxMin4Uint) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinUint)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinUintSize
}

func (*maxMin4Uint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinUint)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Uint) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinUint)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Uint) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinUint)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		uintVal := uint64(input)
		if p.isNull {
			p.val = uintVal
			p.isNull = false
			continue
		}
		if e.isMax && uintVal > p.val || !e.isMax && uintVal < p.val {
			p.val = uintVal
		}
	}
	return 0, nil
}

func (e *maxMin4Uint) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinUint)(src), (*partialResult4MaxMinUint)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Uint) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinUint)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinUint(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Uint) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Uint) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinUint)(pr)
	success := helper.deserializePartialResult4MaxMinUint(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4UintSliding struct {
	maxMin4Uint
	windowInfo
}

func (e *maxMin4UintSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Uint.AllocPartialResult()
	(*partialResult4MaxMinUint)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(i.(uint64), j.(uint64))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4UintSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Uint.ResetPartialResult(pr)
	(*partialResult4MaxMinUint)(pr).deque.Reset()
}

func (e *maxMin4UintSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinUint)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, uint64(input))
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(uint64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4UintSliding{}

func (e *maxMin4UintSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinUint)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, uint64(input))
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(uint64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

// maxMin4Float32 gets a float32 input and returns a float32 result.
type maxMin4Float32 struct {
	baseMaxMinAggFunc
}

func (*maxMin4Float32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinFloat32)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinFloat32Size
}

func (*maxMin4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat32)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		f := float32(input)
		if p.isNull {
			p.val = f
			p.isNull = false
			continue
		}
		if e.isMax && f > p.val || !e.isMax && f < p.val {
			p.val = f
		}
	}
	return 0, nil
}

func (e *maxMin4Float32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinFloat32)(src), (*partialResult4MaxMinFloat32)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Float32) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinFloat32)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinFloat32(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Float32) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Float32) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinFloat32)(pr)
	success := helper.deserializePartialResult4MaxMinFloat32(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4Float32Sliding struct {
	maxMin4Float32
	windowInfo
}

func (e *maxMin4Float32Sliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Float32.AllocPartialResult()
	(*partialResult4MaxMinFloat32)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(float64(i.(float32)), float64(j.(float32)))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4Float32Sliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Float32.ResetPartialResult(pr)
	(*partialResult4MaxMinFloat32)(pr).deque.Reset()
}

func (e *maxMin4Float32Sliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat32)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, float32(input))
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4Float32Sliding{}

func (e *maxMin4Float32Sliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalReal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, float32(input))
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Float64 struct {
	baseMaxMinAggFunc
}

func (*maxMin4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinFloat64)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinFloat64Size
}

func (*maxMin4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat64)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Float64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinFloat64)(src), (*partialResult4MaxMinFloat64)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Float64) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinFloat64)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinFloat64(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Float64) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Float64) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinFloat64)(pr)
	success := helper.deserializePartialResult4MaxMinFloat64(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4Float64Sliding struct {
	maxMin4Float64
	windowInfo
}

func (e *maxMin4Float64Sliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Float64.AllocPartialResult()
	(*partialResult4MaxMinFloat64)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(i.(float64), j.(float64))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4Float64Sliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Float64.ResetPartialResult(pr)
	(*partialResult4MaxMinFloat64)(pr).deque.Reset()
}

func (e *maxMin4Float64Sliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat64)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4Float64Sliding{}

func (e *maxMin4Float64Sliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalReal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}
