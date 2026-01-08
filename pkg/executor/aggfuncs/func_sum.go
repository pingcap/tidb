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
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/set"
)

const (
	pointerSize = int64(unsafe.Sizeof(&types.MyDecimal{}))

	// DefPartialResult4SumFloat64Size is the size of partialResult4SumFloat64
	DefPartialResult4SumFloat64Size = int64(unsafe.Sizeof(partialResult4SumFloat64{}))
	// DefPartialResult4SumDecimalSize is the size of partialResult4SumDecimal
	DefPartialResult4SumDecimalSize = int64(unsafe.Sizeof(partialResult4SumDecimal{}))
	// DefPartialResult4SumDistinctFloat64Size is the size of partialResult4SumDistinctFloat64
	DefPartialResult4SumDistinctFloat64Size = int64(unsafe.Sizeof(partialResult4SumDistinctFloat64{}))
	// DefPartialResult4SumDistinctDecimalSize is the size of partialResult4SumDistinctDecimal
	DefPartialResult4SumDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4SumDistinctDecimal{}))
)

type partialResult4SumFloat64 struct {
	val             float64
	notNullRowCount int64
}

type partialResult4SumDecimal struct {
	val             types.MyDecimal
	notNullRowCount int64
}

type partialResult4SumDistinctFloat64 struct {
	val    float64
	isNull bool
	valSet set.Float64SetWithMemoryUsage
}

type partialResult4SumDistinctDecimal struct {
	val    types.MyDecimal
	isNull bool
	valSet set.StringToDecimalSetWithMemoryUsage
}

type baseSumAggFunc struct {
	baseAggFunc
}

type baseSum4Float64 struct {
	baseSumAggFunc
}

func (*baseSum4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumFloat64)
	return PartialResult(p), DefPartialResult4SumFloat64Size
}

func (*baseSum4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumFloat64)(pr)
	p.val = 0
	p.notNullRowCount = 0
}

func (e *baseSum4Float64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumFloat64)(pr)
	if p.notNullRowCount == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *baseSum4Float64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.val += input
		p.notNullRowCount++
	}
	return 0, nil
}

func (*baseSum4Float64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4SumFloat64)(src), (*partialResult4SumFloat64)(dst)
	if p1.notNullRowCount == 0 {
		return 0, nil
	}
	p2.val += p1.val
	p2.notNullRowCount += p1.notNullRowCount
	return 0, nil
}

func (e *baseSum4Float64) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4SumFloat64)(partialResult)
	resBuf := spillHelper.serializePartialResult4SumFloat64(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *baseSum4Float64) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *baseSum4Float64) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4SumFloat64)(pr)
	success := helper.deserializePartialResult4SumFloat64(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type sum4Float64 struct {
	baseSum4Float64
}

var _ SlidingWindowAggFunc = &sum4Float64{}

func (e *sum4Float64) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4SumFloat64)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalReal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.val += input
		p.notNullRowCount++
	}
	for i := range shiftStart {
		input, isNull, err := e.args[0].EvalReal(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.val -= input
		p.notNullRowCount--
	}
	return nil
}

type sum4Float64HighPrecision struct {
	baseSum4Float64
}

type sum4Decimal struct {
	baseSumAggFunc
}

func (e *sum4Decimal) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4SumDecimal)(partialResult)
	resBuf := spillHelper.serializePartialResult4SumDecimal(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *sum4Decimal) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *sum4Decimal) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4SumDecimal)(pr)
	success := helper.deserializePartialResult4SumDecimal(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

func (*sum4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumDecimal)
	return PartialResult(p), DefPartialResult4SumDecimalSize
}

func (*sum4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDecimal)(pr)
	p.notNullRowCount = 0
}

func (e *sum4Decimal) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDecimal)(pr)
	if p.notNullRowCount == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	if e.retTp == nil {
		return errors.New("e.retTp of sum should not be nil")
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

func (e *sum4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.notNullRowCount == 0 {
			p.val = *input
			p.notNullRowCount = 1
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.val, input, newSum)
		if err != nil {
			return 0, err
		}
		p.val = *newSum
		p.notNullRowCount++
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &sum4Decimal{}

func (e *sum4Decimal) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4SumDecimal)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalDecimal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.notNullRowCount == 0 {
			p.val = *input
			p.notNullRowCount = 1
			continue
		}
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.val, input, newSum)
		if err != nil {
			return err
		}
		p.val = *newSum
		p.notNullRowCount++
	}
	for i := range shiftStart {
		input, isNull, err := e.args[0].EvalDecimal(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		newSum := new(types.MyDecimal)
		err = types.DecimalSub(&p.val, input, newSum)
		if err != nil {
			return err
		}
		p.val = *newSum
		p.notNullRowCount--
	}
	return nil
}

func (*sum4Decimal) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4SumDecimal)(src), (*partialResult4SumDecimal)(dst)
	if p1.notNullRowCount == 0 {
		return 0, nil
	}
	newSum := new(types.MyDecimal)
	err = types.DecimalAdd(&p1.val, &p2.val, newSum)
	if err != nil {
		return 0, err
	}
	p2.val = *newSum
	p2.notNullRowCount += p1.notNullRowCount
	return 0, nil
}

type baseSumDistinct struct {
	baseAggFunc
}

func (*baseSumDistinct) AllocPartialResult() (PartialResult, int64) {
	panic("Not implemented")
}

func (*baseSumDistinct) ResetPartialResult(PartialResult) {
	panic("Not implemented")
}

func (*baseSumDistinct) UpdatePartialResult(AggFuncUpdateContext, []chunk.Row, PartialResult) (int64, error) {
	panic("Not implemented")
}

type baseSumDistinct4Float64 struct {
	baseSumDistinct
}

func (e *baseSumDistinct4Float64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

type sum4PartialDistinctFloat64 struct {
	baseSumDistinct4Float64
}

func (*sum4PartialDistinctFloat64) MergePartialResult(_ AggFuncUpdateContext, src PartialResult, dst PartialResult) (memDelta int64, err error) {
	s, d := (*partialResult4SumDistinctFloat64)(src), (*partialResult4SumDistinctFloat64)(dst)
	for val := range s.valSet.M {
		if d.valSet.Exist(val) {
			continue
		}

		memDelta += d.valSet.Insert(val)
		d.val += val
		d.isNull = false
	}
	return memDelta, nil
}

type sum4OriginalDistinct4Float64 struct {
	baseSumDistinct4Float64
}

func (*sum4OriginalDistinct4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	setSize := int64(0)
	p := new(partialResult4SumDistinctFloat64)
	p.isNull = true
	p.valSet, setSize = set.NewFloat64SetWithMemoryUsage()
	return PartialResult(p), DefPartialResult4SumDistinctFloat64Size + setSize
}

func (*sum4OriginalDistinct4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctFloat64)(pr)
	p.isNull = true
	p.valSet, _ = set.NewFloat64SetWithMemoryUsage()
}

func (e *sum4OriginalDistinct4Float64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
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
		p.val += input
	}
	return memDelta, nil
}

type baseSumDistinct4Decimal struct {
	baseSumDistinct
}

func (e *baseSumDistinct4Decimal) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

type sum4PartialDistinct4Decimal struct {
	baseSumDistinct4Decimal
}

func (*sum4PartialDistinct4Decimal) MergePartialResult(_ AggFuncUpdateContext, src PartialResult, dst PartialResult) (memDelta int64, err error) {
	s, d := (*partialResult4SumDistinctDecimal)(src), (*partialResult4SumDistinctDecimal)(dst)
	for key, val := range s.valSet.M {
		if d.valSet.Exist(key) {
			continue
		}

		memDelta += d.valSet.Insert(key, val) + int64(len(key)+types.MyDecimalStructSize)

		newSum := new(types.MyDecimal)
		if err = types.DecimalAdd(&d.val, val, newSum); err != nil {
			return memDelta, err
		}

		d.val = *newSum
		d.isNull = false
	}
	return memDelta, nil
}

type sum4OriginalDistinct4Decimal struct {
	baseSumDistinct4Decimal
}

func (*sum4OriginalDistinct4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumDistinctDecimal)
	p.isNull = true
	setSize := int64(0)
	p.valSet, setSize = set.NewStringToDecimalSetWithMemoryUsage()
	return PartialResult(p), DefPartialResult4SumDistinctDecimalSize + setSize
}

func (*sum4OriginalDistinct4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctDecimal)(pr)
	p.isNull = true
	p.valSet, _ = set.NewStringToDecimalSetWithMemoryUsage()
}

func (e *sum4OriginalDistinct4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDistinctDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		hash, err := input.ToHashKey()
		if err != nil {
			return memDelta, err
		}
		keyStr := string(hack.String(hash))
		if p.valSet.Exist(keyStr) {
			continue
		}
		memDelta += p.valSet.Insert(keyStr, input.Clone()) + int64(len(keyStr)) + pointerSize
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}
		newSum := new(types.MyDecimal)
		if err = types.DecimalAdd(&p.val, input, newSum); err != nil {
			return memDelta, err
		}
		p.val = *newSum
	}
	return memDelta, nil
}
