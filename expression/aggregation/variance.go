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

type baseVarianceFunction struct {
	aggFunction
}

func (vf *baseVarianceFunction) mergeSingle(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	a := vf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if vf.HasDistinct {
		d, err1 := evalCtx.DistinctChecker.Check([]types.Datum{value})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	evalCtx.Value, evalCtx.Variance, err = calculateVariance(sc, evalCtx.Count, 1, evalCtx.Variance, types.NewIntDatum(0), evalCtx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (vf *baseVarianceFunction) mergeAll(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	value, err := vf.Args[1].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	valueVariance, err := vf.Args[2].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	count, err := vf.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	evalCtx.Value, evalCtx.Variance, err = calculateVariance(sc, evalCtx.Count, count.GetInt64(), evalCtx.Variance, valueVariance, evalCtx.Value, value)
	if err != nil {
		return err
	}
	return nil
}

// Update implements Aggregation interface.
func (vf *baseVarianceFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) (err error) {
	switch vf.Mode {
	case Partial1Mode, CompleteMode:
		err = vf.mergeSingle(sc, evalCtx, row)
	case Partial2Mode, FinalMode:
		err = vf.mergeAll(sc, evalCtx, row)
	case DedupMode:
		panic("DedupMode is not supported now")
	}
	return errors.Trace(err)
}

// GetPartialResult implements Aggregation interface.
func (vf *baseVarianceFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{types.NewIntDatum(evalCtx.Count), evalCtx.Value, evalCtx.Variance}
}

type stddevPopFunction struct {
	baseVarianceFunction
}

// GetResult implements Aggregation interface.
func (vf *stddevPopFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	if evalCtx.Count == 0 {
		d.SetNull()
		return
	}
	switch evalCtx.Value.Kind() {
	case types.KindFloat64:
		variance := evalCtx.Variance.GetFloat64()
		count := float64(evalCtx.Count)
		d.SetFloat64(math.Sqrt(variance / count))
	case types.KindMysqlDecimal:
		variance := evalCtx.Variance.GetMysqlDecimal()
		count := types.NewDecFromInt(evalCtx.Count)
		tmp, res := new(types.MyDecimal), new(types.MyDecimal)
		var err error
		err = types.DecimalDiv(variance, count, tmp, types.DivFracIncr)
		terror.Log(errors.Trace(err))
		err = types.DecimalSqrt(tmp, res, types.SqrtFracIncr)
		terror.Log(errors.Trace(err))
		d.SetMysqlDecimal(res)
	}
	return
}

type stddevSampFunction struct {
	baseVarianceFunction
}

// GetResult implements Aggregation interface.
func (vf *stddevSampFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	if evalCtx.Count < 1 {
		d.SetNull()
		return
	}
	switch evalCtx.Value.Kind() {
	case types.KindFloat64:
		variance := evalCtx.Variance.GetFloat64()
		count := float64(evalCtx.Count)
		d.SetFloat64(math.Sqrt(variance / (count - 1)))
	case types.KindMysqlDecimal:
		variance := evalCtx.Variance.GetMysqlDecimal()
		count := types.NewDecFromInt(evalCtx.Count - 1)
		tmp, res := new(types.MyDecimal), new(types.MyDecimal)
		var err error
		err = types.DecimalDiv(variance, count, tmp, types.DivFracIncr)
		terror.Log(errors.Trace(err))
		err = types.DecimalSqrt(tmp, res, types.SqrtFracIncr)
		terror.Log(errors.Trace(err))
		d.SetMysqlDecimal(res)
	}
	return
}
