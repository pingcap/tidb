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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// All the AggFunc implementations for "AVG" are listed here.
var (
	_ AggFunc = (*avgDedup4Decimal)(nil)
	_ AggFunc = (*avgOriginal4Decimal)(nil)
	_ AggFunc = (*avgPartial4Decimal)(nil)

	_ AggFunc = (*avgDedup4Float64)(nil)
	_ AggFunc = (*avgOriginal4Float64)(nil)
	_ AggFunc = (*avgPartial4Float64)(nil)

	_ AggFunc = (*avgDedup4Float32)(nil)
	_ AggFunc = (*avgOriginal4Float32)(nil)
	_ AggFunc = (*avgPartial4Float32)(nil)
)

// All the following avg function implementations return the decimal result,
// which store the partial results in "partialResult4AvgDecimal".
//
// "baseAvgDecimal" is wrapped by:
// - "avgDedup4Decimal"
// - "avgOriginal4Decimal"
// - "avgPartial4Decimal"
type baseAvgDecimal struct {
	baseAggFunc
}

type partialResult4AvgDecimal struct {
	sum   types.MyDecimal
	count int64
}

func (e *baseAvgDecimal) toPartialResult(partialBytes []byte) *partialResult4AvgDecimal {
	return (*partialResult4AvgDecimal)(unsafe.Pointer(&partialBytes[0]))
}

func (e *baseAvgDecimal) AllocPartialResult() []byte {
	partialBytes := make([]byte, unsafe.Sizeof(partialResult4AvgDecimal{}))
	e.ResetPartialResult(partialBytes)
	return partialBytes
}

func (e *baseAvgDecimal) ResetPartialResult(partialBytes []byte) {
	*e.toPartialResult(partialBytes) = partialResult4AvgDecimal{}
}

func (e *baseAvgDecimal) AppendPartialResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendMyDecimal(e.output[0], &partialResult.sum)
	chk.AppendInt64(e.output[1], partialResult.count)
	return nil
}

func (e *baseAvgDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	if partialResult.count == 0 {
		chk.AppendNull(e.output[0])
		return nil
	}
	decimalCount := types.NewDecFromInt(partialResult.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&partialResult.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendMyDecimal(e.output[0], finalResult)
	return nil
}

type avgDedup4Decimal struct {
	baseAvgDecimal
}

func (e *avgDedup4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	return errors.New("not implemented yet")
}

type avgOriginal4Decimal struct {
	baseAvgDecimal
	deDuper map[types.MyDecimal]bool
}

func (e *avgOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	newSum := new(types.MyDecimal)
	for _, row := range rowsInGroup {
		input, isNull, err := e.input[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull || (e.deDuper != nil && e.deDuper[*input]) {
			continue
		}
		err = types.DecimalAdd(&partialResult.sum, input, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		partialResult.sum = *newSum
		partialResult.count++
		if e.deDuper != nil {
			e.deDuper[*input] = true
		}
	}
	return nil
}

type avgPartial4Decimal struct {
	baseAvgDecimal
}

func (e *avgPartial4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	newSum := new(types.MyDecimal)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.input[1].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull {
			continue
		}

		inputCount, isNull, err := e.input[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull {
			continue
		}

		err = types.DecimalAdd(&partialResult.sum, inputSum, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		partialResult.sum = *newSum
		partialResult.count += inputCount
	}
	return nil
}

// All the following avg function implementations return the float64 result,
// which store the partial results in "partialResult4AvgFloat64".
//
// "baseAvgFloat64" is wrapped by:
// - "avgDedup4Float64"
// - "avgOriginal4Float64"
// - "avgPartial4Float64"
type baseAvgFloat64 struct {
	baseAggFunc
}

type partialResult4AvgFloat64 struct {
	sum   float64
	count int64
}

func (e *baseAvgFloat64) toPartialResult(partialBytes []byte) *partialResult4AvgFloat64 {
	return (*partialResult4AvgFloat64)(unsafe.Pointer(&partialBytes[0]))
}

func (e *baseAvgFloat64) AllocPartialResult() []byte {
	partialBytes := make([]byte, unsafe.Sizeof(partialResult4AvgFloat64{}))
	e.ResetPartialResult(partialBytes)
	return partialBytes
}

func (e *baseAvgFloat64) ResetPartialResult(partialBytes []byte) {
	*e.toPartialResult(partialBytes) = partialResult4AvgFloat64{}
}

func (e *baseAvgFloat64) AppendPartialResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendFloat64(e.output[0], partialResult.sum)
	chk.AppendInt64(e.output[1], partialResult.count)
	return nil
}

func (e *baseAvgFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendFloat64(e.output[0], partialResult.sum/float64(partialResult.count))
	return nil
}

type avgDedup4Float64 struct {
	baseAvgFloat64
}

func (e *avgDedup4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	return errors.New("not implemented yet")
}

type avgOriginal4Float64 struct {
	baseAvgFloat64
	deDuper map[float64]bool
}

func (e *avgOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	for _, row := range rowsInGroup {
		input, isNull, err := e.input[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull || (e.deDuper != nil && e.deDuper[input]) {
			continue
		}
		partialResult.sum += input
		partialResult.count++
		if e.deDuper != nil {
			e.deDuper[input] = true
		}
	}
	return nil
}

type avgPartial4Float64 struct {
	baseAvgFloat64
}

func (e *avgPartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.input[1].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull {
			continue
		}

		inputCount, isNull, err := e.input[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull {
			continue
		}
		partialResult.sum += inputSum
		partialResult.count += inputCount
	}
	return nil
}

// All the following avg function implementations return the float32 result,
// which store the partial results in "partialResult4AvgFloat32".
//
// "baseAvgFloat32" is wrapped by:
// - "avgDedup4Float32"
// - "avgOriginal4Float32"
// - "avgPartial4Float32"
type baseAvgFloat32 struct {
	baseAggFunc
}

type partialResult4AvgFloat32 struct {
	sum   float32
	count int64
}

func (e *baseAvgFloat32) toPartialResult(partialBytes []byte) *partialResult4AvgFloat32 {
	return (*partialResult4AvgFloat32)(unsafe.Pointer(&partialBytes[0]))
}

func (e *baseAvgFloat32) AllocPartialResult() []byte {
	partialBytes := make([]byte, unsafe.Sizeof(partialResult4AvgFloat32{}))
	e.ResetPartialResult(partialBytes)
	return partialBytes
}

func (e *baseAvgFloat32) ResetPartialResult(partialBytes []byte) {
	*e.toPartialResult(partialBytes) = partialResult4AvgFloat32{}
}

func (e *baseAvgFloat32) AppendPartialResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendFloat32(e.output[0], partialResult.sum)
	chk.AppendInt64(e.output[1], partialResult.count)
	return nil
}

func (e *baseAvgFloat32) AppendFinalResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendFloat32(e.output[0], partialResult.sum/float32(partialResult.count))
	return nil
}

type avgDedup4Float32 struct {
	baseAvgFloat32
}

func (e *avgDedup4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	return errors.New("not implemented yet")
}

type avgOriginal4Float32 struct {
	baseAvgFloat32
	deDuper map[float32]bool
}

func (e *avgOriginal4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	for _, row := range rowsInGroup {
		input, isNull, err := e.input[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull || (e.deDuper != nil && e.deDuper[float32(input)]) {
			continue
		}
		partialResult.sum += float32(input)
		partialResult.count++
		if e.deDuper != nil {
			e.deDuper[float32(input)] = true
		}
	}
	return nil
}

type avgPartial4Float32 struct {
	baseAvgFloat32
}

func (e *avgPartial4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.input[1].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull {
			continue
		}

		inputCount, isNull, err := e.input[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull {
			continue
		}
		partialResult.sum += float32(inputSum)
		partialResult.count += inputCount
	}
	return nil
}
