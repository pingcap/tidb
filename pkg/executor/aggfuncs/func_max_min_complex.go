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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

type maxMin4Decimal struct {
	baseMaxMinAggFunc
}

func (*maxMin4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinDecimal)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinDecimalSize
}

func (*maxMin4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDecimal)(pr)
	p.isNull = true
}

func (e *maxMin4Decimal) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	if e.retTp == nil {
		return errors.New("e.retTp of max or min should not be nil")
	}
	frac := e.retTp.GetDecimal()
	if frac == -1 {
		frac = mysql.MaxDecimalScale
	}
	err := p.val.Round(&p.val, frac, types.ModeHalfUp)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *maxMin4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}
		cmp := input.Compare(&p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			p.val = *input
		}
	}
	return 0, nil
}

func (e *maxMin4Decimal) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinDecimal)(src), (*partialResult4MaxMinDecimal)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := (&p1.val).Compare(&p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Decimal) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinDecimal)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinDecimal(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Decimal) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Decimal) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinDecimal)(pr)
	success := helper.deserializePartialResult4MaxMinDecimal(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4DecimalSliding struct {
	maxMin4Decimal
	windowInfo
}

func (e *maxMin4DecimalSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Decimal.AllocPartialResult()
	(*partialResult4MaxMinDecimal)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		src := i.(types.MyDecimal)
		dst := j.(types.MyDecimal)
		return src.Compare(&dst)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4DecimalSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Decimal.ResetPartialResult(pr)
	(*partialResult4MaxMinDecimal)(pr).deque.Reset()
}

func (e *maxMin4DecimalSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDecimal)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, *input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(types.MyDecimal)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4DecimalSliding{}

func (e *maxMin4DecimalSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalDecimal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, *input)
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
		p.val = val.Item.(types.MyDecimal)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4String struct {
	baseMaxMinAggFunc
	retTp *types.FieldType
}

func (*maxMin4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinString)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinStringSize
}

func (*maxMin4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinString)(pr)
	p.isNull = true
}

func (e *maxMin4String) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinString)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.val)
	return nil
}

func (e *maxMin4String) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinString)(pr)
	tp := e.args[0].GetType(sctx)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			// The string returned by `EvalString` may be referenced to an underlying buffer,
			// for example ‘Chunk’, which could be reset and reused multiply times.
			// We have to deep copy that string to avoid some potential risks
			// when the content of that underlying buffer changed.
			p.val = stringutil.Copy(input)
			memDelta += int64(len(input))
			p.isNull = false
			continue
		}
		cmp := types.CompareString(input, p.val, tp.GetCollate())
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			oldMem := len(p.val)
			newMem := len(input)
			memDelta += int64(newMem - oldMem)
			p.val = stringutil.Copy(input)
		}
	}
	return memDelta, nil
}

func (e *maxMin4String) MergePartialResult(ctx AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinString)(src), (*partialResult4MaxMinString)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	tp := e.args[0].GetType(ctx)
	cmp := types.CompareString(p1.val, p2.val, tp.GetCollate())
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4String) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinString)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinString(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4String) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4String) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinString)(pr)
	success := helper.deserializePartialResult4MaxMinString(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4StringSliding struct {
	maxMin4String
	windowInfo
	collate string
}

func (e *maxMin4StringSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4String.AllocPartialResult()
	(*partialResult4MaxMinString)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return types.CompareString(i.(string), j.(string), e.collate)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4StringSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4String.ResetPartialResult(pr)
	(*partialResult4MaxMinString)(pr).deque.Reset()
}

func (e *maxMin4StringSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinString)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
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
		p.val = val.Item.(string)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4StringSliding{}

func (e *maxMin4StringSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinString)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalString(sctx, getRow(lastEnd+i))
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
		p.val = val.Item.(string)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Time struct {
	baseMaxMinAggFunc
}

func (*maxMin4Time) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinTime)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinTimeSize
}

func (*maxMin4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinTime)(pr)
	p.isNull = true
}

func (e *maxMin4Time) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinTime)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Time) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinTime)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalTime(sctx, row)
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
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Time) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinTime)(src), (*partialResult4MaxMinTime)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Time) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinTime)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinTime(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Time) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Time) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinTime)(pr)
	success := helper.deserializePartialResult4MaxMinTime(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4TimeSliding struct {
	maxMin4Time
	windowInfo
}

func (e *maxMin4TimeSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Time.AllocPartialResult()
	(*partialResult4MaxMinTime)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		src := i.(types.Time)
		dst := j.(types.Time)
		return src.Compare(dst)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4TimeSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Time.ResetPartialResult(pr)
	(*partialResult4MaxMinTime)(pr).deque.Reset()
}

func (e *maxMin4TimeSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinTime)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalTime(sctx, row)
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
		p.val = val.Item.(types.Time)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4TimeSliding) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinTime)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinTime(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4TimeSliding) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4TimeSliding) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinTime)(pr)
	success := helper.deserializePartialResult4MaxMinTime(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

var _ SlidingWindowAggFunc = &maxMin4DurationSliding{}

func (e *maxMin4TimeSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinTime)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalTime(sctx, getRow(lastEnd+i))
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
		p.val = val.Item.(types.Time)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Duration struct {
	baseMaxMinAggFunc
}

func (*maxMin4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinDuration)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinDurationSize
}

func (*maxMin4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDuration)(pr)
	p.isNull = true
}

func (e *maxMin4Duration) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDuration)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Duration) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDuration)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
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
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Duration) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinDuration)(src), (*partialResult4MaxMinDuration)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Duration) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinDuration)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinDuration(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Duration) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Duration) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinDuration)(pr)
	success := helper.deserializePartialResult4MaxMinDuration(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4DurationSliding struct {
	maxMin4Duration
	windowInfo
}

func (e *maxMin4DurationSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Duration.AllocPartialResult()
	(*partialResult4MaxMinDuration)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		src := i.(types.Duration)
		dst := j.(types.Duration)
		return src.Compare(dst)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4DurationSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Duration.ResetPartialResult(pr)
	(*partialResult4MaxMinDuration)(pr).deque.Reset()
}

func (e *maxMin4DurationSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDuration)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
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
		p.val = val.Item.(types.Duration)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4DurationSliding{}

func (e *maxMin4DurationSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinDuration)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalDuration(sctx, getRow(lastEnd+i))
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
		p.val = val.Item.(types.Duration)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}
