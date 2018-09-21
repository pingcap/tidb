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
	"math"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
	"github.com/pkg/errors"
)

type baseStddevSampDecimal struct {
	baseAggFunc
}

type partialResult4StddevSampDecimal struct {
	sum       types.MyDecimal
	sumSquare types.MyDecimal
	count     int64
}

func (e *baseStddevSampDecimal) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4StddevSampDecimal{})
}

func (e *baseStddevSampDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevSampDecimal)(pr)
	p.sum, p.sumSquare = *types.NewDecFromInt(0), *types.NewDecFromInt(0)
	p.count = int64(0)
}

func (e *baseStddevSampDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevSampDecimal)(pr)
	if p.count <= 1 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	decimalCount := types.NewDecFromInt(p.count)
	avgSum := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, avgSum, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	squareAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(avgSum, avgSum, squareAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	sumMulAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(&p.sum, avgSum, sumMulAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	doubleSumMulAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(sumMulAvgSum, types.NewDecFromInt(2), doubleSumMulAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	tmpVariance := new(types.MyDecimal)
	err = types.DecimalSub(&p.sumSquare, doubleSumMulAvgSum, tmpVariance)
	if err != nil {
		return errors.Trace(err)
	}
	countMulSquareAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(squareAvgSum, decimalCount, countMulSquareAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	tmpVariance2 := new(types.MyDecimal)
	err = types.DecimalAdd(tmpVariance, countMulSquareAvgSum, tmpVariance2)
	if err != nil {
		return errors.Trace(err)
	}
	variance := new(types.MyDecimal)
	decimalCountSubOne := types.NewDecFromInt(p.count - 1)
	err = types.DecimalDiv(tmpVariance2, decimalCountSubOne, variance, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	finalResult := new(types.MyDecimal)
	err = types.DecimalSqrt(variance, finalResult, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type stddevSampOriginal4Decimal struct {
	baseStddevSampDecimal
}

func (e *stddevSampOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevSampDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.sum = *newSum

		newSuqareSum := new(types.MyDecimal)
		squareInput := new(types.MyDecimal)
		err = types.DecimalMul(input, input, squareInput)
		if err != nil {
			return errors.Trace(err)
		}
		err = types.DecimalAdd(&p.sumSquare, squareInput, newSuqareSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.sumSquare = *newSuqareSum

		p.count++
	}
	return nil
}

type stddevSampPartial4Decimal struct {
	baseStddevSampDecimal
}

func (e *stddevSampPartial4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevSampDecimal)(pr)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		inputSumSquare, isNull, err := e.args[2].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		newSum, newSumSquare := new(types.MyDecimal), new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, inputSum, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.sum = *newSum
		err = types.DecimalAdd(&p.sumSquare, inputSumSquare, newSumSquare)
		if err != nil {
			return errors.Trace(err)
		}
		p.sumSquare = *newSumSquare
		p.count += inputCount
	}
	return nil
}

func (e *stddevSampPartial4Decimal) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4StddevSampDecimal)(src), (*partialResult4StddevSampDecimal)(dst)
	if p1.count == 0 {
		return nil
	}
	newSum, newSumSquare := new(types.MyDecimal), new(types.MyDecimal)
	err := types.DecimalAdd(&p1.sum, &p2.sum, newSum)
	if err != nil {
		return errors.Trace(err)
	}
	err = types.DecimalAdd(&p1.sumSquare, &p2.sumSquare, newSumSquare)
	if err != nil {
		return errors.Trace(err)
	}
	p2.sum = *newSum
	p2.sumSquare = *newSumSquare
	p2.count += p1.count
	return nil
}

type partialResult4StddevSampDistinctDecimal struct {
	partialResult4StddevSampDecimal
	valSet set.DecimalSet
}

type stddevSampOriginal4DistinctDecimal struct {
	baseAggFunc
}

func (e *stddevSampOriginal4DistinctDecimal) AllocPartialResult() PartialResult {
	p := &partialResult4StddevSampDistinctDecimal{
		valSet: set.NewDecimalSet(),
	}
	return PartialResult(p)
}

func (e *stddevSampOriginal4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevSampDistinctDecimal)(pr)
	p.sum, p.sumSquare = *types.NewDecFromInt(0), *types.NewDecFromInt(0)
	p.count = int64(0)
	p.valSet = set.NewDecimalSet()
}

func (e *stddevSampOriginal4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevSampDistinctDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.sum = *newSum

		newSuqareSum := new(types.MyDecimal)
		squareInput := new(types.MyDecimal)
		err = types.DecimalMul(input, input, squareInput)
		if err != nil {
			return errors.Trace(err)
		}
		err = types.DecimalAdd(&p.sumSquare, squareInput, newSuqareSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.sumSquare = *newSuqareSum

		p.count++
		p.valSet.Insert(input)
	}
	return nil
}

func (e *stddevSampOriginal4DistinctDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevSampDistinctDecimal)(pr)
	if p.count <= 1 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	decimalCount := types.NewDecFromInt(p.count)
	avgSum := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, avgSum, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	squareAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(avgSum, avgSum, squareAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	sumMulAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(&p.sum, avgSum, sumMulAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	doubleSumMulAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(sumMulAvgSum, types.NewDecFromInt(2), doubleSumMulAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	tmpVariance := new(types.MyDecimal)
	err = types.DecimalSub(&p.sumSquare, doubleSumMulAvgSum, tmpVariance)
	if err != nil {
		return errors.Trace(err)
	}
	countMulSquareAvgSum := new(types.MyDecimal)
	err = types.DecimalMul(squareAvgSum, decimalCount, countMulSquareAvgSum)
	if err != nil {
		return errors.Trace(err)
	}
	tmpVariance2 := new(types.MyDecimal)
	err = types.DecimalAdd(tmpVariance, countMulSquareAvgSum, tmpVariance2)
	if err != nil {
		return errors.Trace(err)
	}
	variance := new(types.MyDecimal)
	decimalCountSubOne := types.NewDecFromInt(p.count - 1)
	err = types.DecimalDiv(tmpVariance2, decimalCountSubOne, variance, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	finalResult := new(types.MyDecimal)
	err = types.DecimalSqrt(variance, finalResult, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type baseStddevSampFloat64 struct {
	baseAggFunc
}

type partialResult4StddevSampFloat64 struct {
	sum       float64
	sumSquare float64
	count     int64
}

func (e *baseStddevSampFloat64) AllocPartialResult() PartialResult {
	return (PartialResult)(&partialResult4StddevSampFloat64{})
}

func (e *baseStddevSampFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevSampFloat64)(pr)
	p.sum, p.sumSquare, p.count = 0, 0, 0
}

func (e *baseStddevSampFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevSampFloat64)(pr)
	if p.count <= 1 {
		chk.AppendNull(e.ordinal)
	} else {
		count := float64(p.count)
		avgSum := p.sum / count
		chk.AppendFloat64(e.ordinal, math.Sqrt((p.sumSquare-2*p.sum*avgSum+count*avgSum*avgSum)/(count-1)))
	}
	return nil
}

type stddevSampOriginal4Float64 struct {
	baseStddevSampFloat64
}

func (e *stddevSampOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevSampFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		p.sum += input
		p.sumSquare += input * input
		p.count++
	}
	return nil
}

type stddevSampPartial4Float64 struct {
	baseStddevSampFloat64
}

func (e *stddevSampPartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevSampFloat64)(pr)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		inputSumSquare, isNull, err := e.args[2].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		p.sum += inputSum
		p.sumSquare += inputSumSquare
		p.count += inputCount
	}
	return nil
}

func (e *stddevSampPartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4StddevSampFloat64)(src), (*partialResult4StddevSampFloat64)(dst)
	p2.sum += p1.sum
	p2.sumSquare += p1.sumSquare
	p2.count += p1.count
	return nil
}

type partialResult4StddevSampDistinctFloat64 struct {
	partialResult4StddevSampFloat64
	valSet set.Float64Set
}

type stddevSampOriginal4DistinctFloat64 struct {
	baseAggFunc
}

func (e *stddevSampOriginal4DistinctFloat64) AllocPartialResult() PartialResult {
	p := &partialResult4StddevSampDistinctFloat64{
		valSet: set.NewFloat64Set(),
	}
	return PartialResult(p)
}

func (e *stddevSampOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevSampDistinctFloat64)(pr)
	p.sum, p.sumSquare, p.count = 0, 0, 0
	p.valSet = set.NewFloat64Set()
}

func (e *stddevSampOriginal4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevSampDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}

		p.sum += input
		p.sumSquare += input * input
		p.count++
		p.valSet.Insert(input)
	}
	return nil
}

func (e *stddevSampOriginal4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevSampDistinctFloat64)(pr)
	if p.count <= 1 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	count := float64(p.count)
	avgSum := p.sum / count
	chk.AppendFloat64(e.ordinal, math.Sqrt((p.sumSquare-2*p.sum*avgSum+count*avgSum*avgSum)/(count-1)))
	return nil
}
