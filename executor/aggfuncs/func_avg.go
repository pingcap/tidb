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

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
)

const (
	// DefPartialResult4AvgDecimalSize is the size of partialResult4AvgDecimal
	DefPartialResult4AvgDecimalSize = int64(unsafe.Sizeof(partialResult4AvgDecimal{}))
	// DefPartialResult4AvgDistinctDecimalSize is the size of partialResult4AvgDistinctDecimal
	DefPartialResult4AvgDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4AvgDistinctDecimal{}))
	// DefPartialResult4AvgFloat64Size is the size of partialResult4AvgFloat64
	DefPartialResult4AvgFloat64Size = int64(unsafe.Sizeof(partialResult4AvgFloat64{}))
	// DefPartialResult4AvgDistinctFloat64Size is the size of partialResult4AvgDistinctFloat64
	DefPartialResult4AvgDistinctFloat64Size = int64(unsafe.Sizeof(partialResult4AvgDistinctFloat64{}))
)

// All the following avg function implementations return the decimal result,
// which store the partial results in "partialResult4AvgDecimal".
//
// "baseAvgDecimal" is wrapped by:
// - "avgOriginal4Decimal"
// - "avgPartial4Decimal"
type baseAvgDecimal struct {
	baseAggFunc
}

type partialResult4AvgDecimal struct {
	sum   types.MyDecimal
	count int64
}

func (e *baseAvgDecimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4AvgDecimal{}), DefPartialResult4AvgDecimalSize
}

func (e *baseAvgDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDecimal)(pr)
	p.sum = *types.NewDecFromInt(0)
	p.count = int64(0)
}

func (e *baseAvgDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDecimal)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	decimalCount := types.NewDecFromInt(p.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		return err
	}
	// Make the decimal be the result of type inferring.
	frac := e.args[0].GetType().Decimal
	if len(e.args) == 2 {
		frac = e.args[1].GetType().Decimal
	}
	if frac == -1 {
		frac = mysql.MaxDecimalScale
	}
	err = finalResult.Round(finalResult, mathutil.Min(frac, mysql.MaxDecimalScale), types.ModeHalfEven)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type avgOriginal4Decimal struct {
	baseAvgDecimal
}

func (e *avgOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return 0, err
		}
		p.sum = *newSum
		p.count++
	}
	return 0, nil
}

func (e *avgOriginal4Decimal) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4AvgDecimal)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return err
		}
		p.sum = *newSum
		p.count++
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		newSum := new(types.MyDecimal)
		err = types.DecimalSub(&p.sum, input, newSum)
		if err != nil {
			return err
		}
		p.sum = *newSum
		p.count--
	}
	return nil
}

type avgPartial4Decimal struct {
	baseAvgDecimal
}

func (e *avgPartial4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDecimal)(pr)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, inputSum, newSum)
		if err != nil {
			return 0, err
		}
		p.sum = *newSum
		p.count += inputCount
	}
	return 0, nil
}

func (e *avgPartial4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4AvgDecimal)(src), (*partialResult4AvgDecimal)(dst)
	if p1.count == 0 {
		return 0, nil
	}
	newSum := new(types.MyDecimal)
	err = types.DecimalAdd(&p1.sum, &p2.sum, newSum)
	if err != nil {
		return 0, err
	}
	p2.sum = *newSum
	p2.count += p1.count
	return 0, nil
}

type partialResult4AvgDistinctDecimal struct {
	partialResult4AvgDecimal
	valSet  set.StringSet
	valList []types.MyDecimal // ordered value set
	keyList []string          // ordered key set
}

type avgOriginal4DistinctDecimal struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctDecimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := &partialResult4AvgDistinctDecimal{
		valSet:  set.NewStringSet(),
		valList: make([]types.MyDecimal, 0),
		keyList: make([]string, 0),
	}
	return PartialResult(p), DefPartialResult4AvgDistinctDecimalSize
}

func (e *avgOriginal4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDistinctDecimal)(pr)
	p.sum = *types.NewDecFromInt(0)
	p.count = int64(0)
	p.valSet = set.NewStringSet()
	p.valList, p.keyList = p.valList[:0], p.keyList[:0]
}

func (e *avgOriginal4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDistinctDecimal)(pr)
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
		decStr := string(hack.String(hash))
		if p.valSet.Exist(decStr) {
			continue
		}
		p.valSet.Insert(decStr)
		p.valList = append(p.valList, *input)
		p.keyList = append(p.keyList, decStr)
		memDelta += int64(len(decStr))
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return memDelta, err
		}
		p.sum = *newSum
		p.count++
	}
	return memDelta, nil
}

func (e *avgOriginal4DistinctDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDistinctDecimal)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	decimalCount := types.NewDecFromInt(p.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		return err
	}
	// Make the decimal be the result of type inferring.
	frac := e.args[0].GetType().Decimal
	if frac == -1 {
		frac = mysql.MaxDecimalScale
	}
	err = finalResult.Round(finalResult, mathutil.Min(frac, mysql.MaxDecimalScale), types.ModeHalfEven)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type avgPartial4DistinctDecimal struct {
	avgOriginal4DistinctDecimal
}

func (e *avgPartial4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	return 0, terror.ClassOptimizer.New(mysql.ErrInternal, mysql.MySQLErrName[mysql.ErrInternal]).GenWithStack("avgPartial4DistinctDecimal.UpdatePartialResult should not be called")
}

func (e *avgPartial4DistinctDecimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4AvgDistinctDecimal)(src), (*partialResult4AvgDistinctDecimal)(dst)
	if p1.count == 0 {
		return 0, nil
	}
	for i := range p1.valList {
		if p2.valSet.Exist(p1.keyList[i]) {
			continue
		}
		p2.valSet.Insert(p1.keyList[i])
		p2.valList = append(p2.valList, p1.valList[i])
		p2.keyList = append(p2.keyList, p1.keyList[i])
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p2.sum, &p1.valList[i], newSum)
		if err != nil {
			return 0, err
		}
		p2.sum = *newSum
		p2.count++
	}
	return 0, nil
}

func (e *avgPartial4DistinctDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDistinctDecimal)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	decimalCount := types.NewDecFromInt(p.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		return err
	}
	// Make the decimal be the result of type inferring.
	frac := e.args[1].GetType().Decimal
	if frac == -1 {
		frac = mysql.MaxDecimalScale
	}
	err = finalResult.Round(finalResult, mathutil.Min(frac, mysql.MaxDecimalScale), types.ModeHalfEven)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

// All the following avg function implementations return the float64 result,
// which store the partial results in "partialResult4AvgFloat64".
//
// "baseAvgFloat64" is wrapped by:
// - "avgOriginal4Float64"
// - "avgPartial4Float64"
type baseAvgFloat64 struct {
	baseAggFunc
}

type partialResult4AvgFloat64 struct {
	sum   float64
	count int64
}

func (e *baseAvgFloat64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return (PartialResult)(&partialResult4AvgFloat64{}), DefPartialResult4AvgFloat64Size
}

func (e *baseAvgFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgFloat64)(pr)
	p.sum = 0
	p.count = 0
}

func (e *baseAvgFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
	} else {
		chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
	}
	return nil
}

type avgOriginal4Float64HighPrecision struct {
	baseAvgFloat64
}

func (e *avgOriginal4Float64HighPrecision) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		p.sum += input
		p.count++
	}
	return 0, nil
}

type avgOriginal4Float64 struct {
	avgOriginal4Float64HighPrecision
}

func (e *avgOriginal4Float64) Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4AvgFloat64)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.sum += input
		p.count++
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.sum -= input
		p.count--
	}
	return nil
}

type avgPartial4Float64 struct {
	baseAvgFloat64
}

func (e *avgPartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgFloat64)(pr)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.sum += inputSum
		p.count += inputCount
	}
	return 0, nil
}

func (e *avgPartial4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4AvgFloat64)(src), (*partialResult4AvgFloat64)(dst)
	p2.sum += p1.sum
	p2.count += p1.count
	return 0, nil
}

type partialResult4AvgDistinctFloat64 struct {
	partialResult4AvgFloat64
	valSet  set.Float64Set
	valList []float64 // ordered value set
}

type avgOriginal4DistinctFloat64 struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctFloat64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := &partialResult4AvgDistinctFloat64{
		valSet:  set.NewFloat64Set(),
		valList: make([]float64, 0),
	}
	return PartialResult(p), DefPartialResult4AvgDistinctFloat64Size
}

func (e *avgOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	p.sum = float64(0)
	p.count = int64(0)
	p.valSet = set.NewFloat64Set()
	p.valList = p.valList[:0]
}

func (e *avgOriginal4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}

		p.sum += input
		p.count++
		p.valSet.Insert(input)
		p.valList = append(p.valList, input)
		memDelta += DefFloat64Size
	}
	return memDelta, nil
}

func (e *avgOriginal4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
	return nil
}

type avgPartial4DistinctFloat64 struct {
	avgOriginal4DistinctFloat64
}

func (e *avgPartial4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	return 0, terror.ClassOptimizer.New(mysql.ErrInternal, mysql.MySQLErrName[mysql.ErrInternal]).GenWithStack("avgPartial4DistinctFloat64.UpdatePartialResult should not be called")
}

func (e *avgPartial4DistinctFloat64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4AvgDistinctFloat64)(src), (*partialResult4AvgDistinctFloat64)(dst)
	if p1.count == 0 {
		return 0, nil
	}
	for _, f := range p1.valList {
		if p2.valSet.Exist(f) {
			continue
		}
		p2.valSet.Insert(f)
		p2.valList = append(p2.valList, f)
		p2.sum += f
		p2.count++
	}
	return 0, nil
}
