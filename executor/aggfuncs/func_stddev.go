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

type baseStddevDecimal struct {
	baseAggFunc
}

type partialResult4StddevDecimal struct {
	sum       types.MyDecimal
	sumSquare types.MyDecimal
	count     int64
}

func (e *baseStddevDecimal) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4StddevDecimal{})
}

func (e *baseStddevDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevDecimal)(pr)
	p.sum, p.sumSquare = *types.NewDecFromInt(0), *types.NewDecFromInt(0)
	p.count = int64(0)
}

func (e *baseStddevDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevDecimal)(pr)
	if p.count == 0 {
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
	tmpVariance2 := new(types.MyDecimal)
	err = types.DecimalAdd(tmpVariance, squareAvgSum, tmpVariance2)
	if err != nil {
		return errors.Trace(err)
	}
	variance := new(types.MyDecimal)
	err = types.DecimalDiv(tmpVariance2, decimalCount, variance, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	finalResult := new(types.MyDecimal)
	err = types.DecimalSqrt(variance, finalResult)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type stddevOriginal4Decimal struct {
	baseStddevDecimal
}

func (e *stddevOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevDecimal)(pr)
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

type stddevPartial4Decimal struct {
	baseStddevDecimal
}

func (e *stddevPartial4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevDecimal)(pr)
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

func (e *stddevPartial4Decimal) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4StddevDecimal)(src), (*partialResult4StddevDecimal)(dst)
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

type partialResult4StddevDistinctDecimal struct {
	partialResult4StddevDecimal
	valSet set.DecimalSet
}

type stddevOriginal4DistinctDecimal struct {
	baseAggFunc
}

func (e *stddevOriginal4DistinctDecimal) AllocPartialResult() PartialResult {
	p := &partialResult4StddevDistinctDecimal{
		valSet: set.NewDecimalSet(),
	}
	return PartialResult(p)
}

func (e *stddevOriginal4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevDistinctDecimal)(pr)
	p.sum, p.sumSquare = *types.NewDecFromInt(0), *types.NewDecFromInt(0)
	p.count = int64(0)
	p.valSet = set.NewDecimalSet()
}

func (e *stddevOriginal4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevDistinctDecimal)(pr)
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

func (e *stddevOriginal4DistinctDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevDistinctDecimal)(pr)
	if p.count == 0 {
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
	tmpVariance2 := new(types.MyDecimal)
	err = types.DecimalAdd(tmpVariance, squareAvgSum, tmpVariance2)
	if err != nil {
		return errors.Trace(err)
	}
	variance := new(types.MyDecimal)
	err = types.DecimalDiv(tmpVariance2, decimalCount, variance, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	finalResult := new(types.MyDecimal)
	err = types.DecimalSqrt(variance, finalResult)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type baseStddevFloat64 struct {
	baseAggFunc
}

type partialResult4StddevFloat64 struct {
	sum       float64
	sumSquare float64
	count     int64
}

func (e *baseStddevFloat64) AllocPartialResult() PartialResult {
	return (PartialResult)(&partialResult4StddevFloat64{})
}

func (e *baseStddevFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevFloat64)(pr)
	p.sum, p.sumSquare, p.count = 0, 0, 0
}

func (e *baseStddevFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
	} else {
		avgSum := p.sum / (float64)(p.count)
		chk.AppendFloat64(e.ordinal, math.Sqrt((p.sumSquare-2*p.sum*avgSum+avgSum*avgSum)/(float64)(p.count)))
	}
	return nil
}

type stddevOriginal4Float64 struct {
	baseStddevFloat64
}

func (e *stddevOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevFloat64)(pr)
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

type stddevPartial4Float64 struct {
	baseStddevFloat64
}

func (e *stddevPartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevFloat64)(pr)
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

func (e *stddevPartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4StddevFloat64)(src), (*partialResult4StddevFloat64)(dst)
	p2.sum += p1.sum
	p2.sumSquare += p1.sumSquare
	p2.count += p1.count
	return nil
}

type partialResult4StddevDistinctFloat64 struct {
	partialResult4StddevFloat64
	valSet set.Float64Set
}

type stddevOriginal4DistinctFloat64 struct {
	baseAggFunc
}

func (e *stddevOriginal4DistinctFloat64) AllocPartialResult() PartialResult {
	p := &partialResult4StddevDistinctFloat64{
		valSet: set.NewFloat64Set(),
	}
	return PartialResult(p)
}

func (e *stddevOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StddevDistinctFloat64)(pr)
	p.sum, p.sumSquare, p.count = 0, 0, 0
	p.valSet = set.NewFloat64Set()
}

func (e *stddevOriginal4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StddevDistinctFloat64)(pr)
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

func (e *stddevOriginal4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StddevDistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	avgSum := p.sum / (float64)(p.count)
	chk.AppendFloat64(e.ordinal, math.Sqrt((p.sumSquare-2*p.sum*avgSum+avgSum*avgSum)/(float64)(p.count)))
	return nil
}
