// Copyright 2017 PingCAP, Inc.
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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
)

type avgFunction struct {
	aggFunction
}

func (af *avgFunction) updateAvg(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row types.Row) error {
	a := af.Args[1]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if af.HasDistinct {
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
	count, err := af.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	evalCtx.Count += count.GetInt64()
	return nil
}

// Update implements Aggregation interface.
func (af *avgFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	if af.Mode == FinalMode {
		return af.updateAvg(sc, evalCtx, row)
	}
	return af.updateSum(sc, evalCtx, row)
}

// GetResult implements Aggregation interface.
func (af *avgFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	var x *types.MyDecimal
	switch evalCtx.Value.Kind() {
	case types.KindFloat64:
		x = new(types.MyDecimal)
		err := x.FromFloat64(evalCtx.Value.GetFloat64())
		terror.Log(errors.Trace(err))
	case types.KindMysqlDecimal:
		x = evalCtx.Value.GetMysqlDecimal()
	default:
		return
	}
	y := types.NewDecFromInt(evalCtx.Count)
	to := new(types.MyDecimal)
	err := types.DecimalDiv(x, y, to, types.DivFracIncr)
	terror.Log(errors.Trace(err))
	frac := af.RetTp.Decimal
	if frac == -1 {
		frac = mysql.MaxDecimalScale
	}
	err = to.Round(to, frac, types.ModeHalfEven)
	terror.Log(errors.Trace(err))
	if evalCtx.Value.Kind() == types.KindFloat64 {
		f, err := to.ToFloat64()
		terror.Log(errors.Trace(err))
		d.SetFloat64(f)
		return
	}
	d.SetMysqlDecimal(to)
	return
}

// GetPartialResult implements Aggregation interface.
func (af *avgFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{types.NewIntDatum(evalCtx.Count), evalCtx.Value}
}
