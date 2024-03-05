// Copyright 2020 PingCAP, Inc.
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
	// DefPartialResult4CountSize is the size of partialResult4Count
	DefPartialResult4CountSize = int64(unsafe.Sizeof(partialResult4Count(0)))
)

type baseCount struct {
	baseAggFunc
}

type partialResult4Count = int64

func (*baseCount) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4Count)), DefPartialResult4CountSize
}

func (*baseCount) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Count)(pr)
	*p = 0
}

func (e *baseCount) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Count)(pr)
	chk.AppendInt64(e.ordinal, *p)
	return nil
}

func (e *baseCount) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4Count)(partialResult)
	resBuf := spillHelper.serializePartialResult4Count(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *baseCount) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *baseCount) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := *(*partialResult4Count)(pr)
	success := helper.deserializePartialResult4Count(&result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type countOriginal4Int struct {
	baseCount
}

func (e *countOriginal4Int) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

var _ SlidingWindowAggFunc = &countOriginal4Int{}

func (e *countOriginal4Int) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalInt(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Real struct {
	baseCount
}

func (e *countOriginal4Real) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

var _ SlidingWindowAggFunc = &countOriginal4Real{}

func (e *countOriginal4Real) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalReal(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalReal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Decimal struct {
	baseCount
}

func (e *countOriginal4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

var _ SlidingWindowAggFunc = &countOriginal4Decimal{}

func (e *countOriginal4Decimal) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalDecimal(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalDecimal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Time struct {
	baseCount
}

func (e *countOriginal4Time) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

var _ SlidingWindowAggFunc = &countOriginal4Time{}

func (e *countOriginal4Time) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalTime(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalTime(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Duration struct {
	baseCount
}

func (e *countOriginal4Duration) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

var _ SlidingWindowAggFunc = &countOriginal4Duration{}

func (e *countOriginal4Duration) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalDuration(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalDuration(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4JSON struct {
	baseCount
}

func (e *countOriginal4JSON) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

var _ SlidingWindowAggFunc = &countOriginal4JSON{}

func (e *countOriginal4JSON) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalJSON(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalJSON(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4String struct {
	baseCount
}

func (e *countOriginal4String) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

var _ SlidingWindowAggFunc = &countOriginal4String{}

func (e *countOriginal4String) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalString(sctx, getRow(lastStart+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalString(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countPartial struct {
	baseCount
}

func (e *countPartial) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p += input
	}
	return 0, nil
}

func (*countPartial) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4Count)(src), (*partialResult4Count)(dst)
	*p2 += *p1
	return 0, nil
}
