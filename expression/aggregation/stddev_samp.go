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

package aggregation

import (
	"math"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

type stddevSampFunction struct {
	aggFunction
}

func (sf *stddevSampFunction) updateSingle(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	a := sf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if sf.HasDistinct {
		d, err1 := evalCtx.DistinctChecker.Check([]types.Datum{value})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	evalCtx.Value, err = calculateSum(sc, evalCtx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	evalCtx.ValueSumSquare, err = calculateSumSquare(sc, evalCtx.ValueSumSquare, value)
	if err != nil {
		return errors.Trace(err)
	}
	evalCtx.Count++
	return nil
}

func (sf *stddevSampFunction) updateAll(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	value, err := sf.Args[1].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	valueSumSquare, err := sf.Args[2].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	evalCtx.Value, err = calculateSum(sc, evalCtx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	evalCtx.ValueSumSquare, err = calculateSum(sc, evalCtx.ValueSumSquare, valueSumSquare)
	if err != nil {
		return errors.Trace(err)
	}
	count, err := sf.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	evalCtx.Count += count.GetInt64()
	return nil
}

// Update implements Aggregation interface.
func (sf *stddevSampFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) (err error) {
	switch sf.Mode {
	case Partial1Mode, CompleteMode:
		err = sf.updateSingle(sc, evalCtx, row)
	case Partial2Mode, FinalMode:
		err = sf.updateAll(sc, evalCtx, row)
	case DedupMode:
		panic("DedupMode is not supported now")
	}
	return errors.Trace(err)
}

// GetResult implements Aggregation interface.
func (sf *stddevSampFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	switch evalCtx.Value.Kind() {
	case types.KindFloat64:
		sum := evalCtx.Value.GetFloat64()
		sumSquare := evalCtx.ValueSumSquare.GetFloat64()
		count := float64(evalCtx.Count)
		avg := sum / count
		d.SetFloat64(math.Sqrt((sumSquare - 2*sum*avg + count*avg*avg) / (count - 1)))
	case types.KindMysqlDecimal:
		sum := evalCtx.Value.GetMysqlDecimal()
		sumSquare := evalCtx.ValueSumSquare.GetMysqlDecimal()
		count := types.NewDecFromInt(evalCtx.Count)
		avgSum := new(types.MyDecimal)
		err := types.DecimalDiv(sum, count, avgSum, types.DivFracIncr)
		terror.Log(errors.Trace(err))
		squareAvgSum := new(types.MyDecimal)
		err = types.DecimalMul(avgSum, avgSum, squareAvgSum)
		terror.Log(errors.Trace(err))
		sumMulAvgSum := new(types.MyDecimal)
		err = types.DecimalMul(sum, avgSum, sumMulAvgSum)
		terror.Log(errors.Trace(err))
		doubleSumMulAvgSum := new(types.MyDecimal)
		err = types.DecimalMul(sumMulAvgSum, types.NewDecFromInt(2), doubleSumMulAvgSum)
		terror.Log(errors.Trace(err))
		tmpVariance := new(types.MyDecimal)
		err = types.DecimalSub(sumSquare, doubleSumMulAvgSum, tmpVariance)
		terror.Log(errors.Trace(err))
		countMulSquareAvgSum := new(types.MyDecimal)
		err = types.DecimalMul(squareAvgSum, count, countMulSquareAvgSum)
		terror.Log(errors.Trace(err))
		tmpVariance2 := new(types.MyDecimal)
		err = types.DecimalAdd(tmpVariance, countMulSquareAvgSum, tmpVariance2)
		terror.Log(errors.Trace(err))
		countSubOne := new(types.MyDecimal)
		err = types.DecimalSub(count, types.NewDecFromInt(1), countSubOne)
		terror.Log(errors.Trace(err))
		variance := new(types.MyDecimal)
		err = types.DecimalDiv(tmpVariance2, countSubOne, variance, types.DivFracIncr)
		terror.Log(errors.Trace(err))
		finalResult := new(types.MyDecimal)
		err = types.DecimalSqrt(variance, finalResult, types.DivFracIncr)
		terror.Log(errors.Trace(err))
		d.SetMysqlDecimal(finalResult)
	}
	return
}

// GetPartialResult implements Aggregation interface.
func (sf *stddevSampFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{types.NewIntDatum(evalCtx.Count), evalCtx.Value, evalCtx.ValueSumSquare}
}
