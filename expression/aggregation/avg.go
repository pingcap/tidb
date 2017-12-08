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

// Clone implements Aggregation interface.
func (af *avgFunction) Clone() Aggregation {
	nf := *af
	for i, arg := range af.Args {
		nf.Args[i] = arg.Clone()
	}
	return &nf
}

// GetType implements Aggregation interface.
func (af *avgFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	types.SetBinChsClnFlag(ft)
	ft.Flen, ft.Decimal = mysql.MaxRealWidth, af.Args[0].GetType().Decimal
	return ft
}

func (af *avgFunction) updateAvg(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	a := af.Args[1]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if af.Distinct {
		d, err1 := ctx.DistinctChecker.Check([]types.Datum{value})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	ctx.Value, err = calculateSum(sc, ctx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	count, err := af.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Count += count.GetInt64()
	return nil
}

// Update implements Aggregation interface.
func (af *avgFunction) Update(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	if af.mode == FinalMode {
		return af.updateAvg(ctx, sc, row)
	}
	return af.updateSum(ctx, sc, row)
}

// GetResult implements Aggregation interface.
func (af *avgFunction) GetResult(ctx *AggEvaluateContext) (d types.Datum) {
	var x *types.MyDecimal
	switch ctx.Value.Kind() {
	case types.KindFloat64:
		x = new(types.MyDecimal)
		err := x.FromFloat64(ctx.Value.GetFloat64())
		terror.Log(errors.Trace(err))
	case types.KindMysqlDecimal:
		x = ctx.Value.GetMysqlDecimal()
	default:
		return
	}
	y := types.NewDecFromInt(ctx.Count)
	to := new(types.MyDecimal)
	err := types.DecimalDiv(x, y, to, types.DivFracIncr)
	terror.Log(errors.Trace(err))
	err = to.Round(to, ctx.Value.Frac()+types.DivFracIncr, types.ModeHalfEven)
	terror.Log(errors.Trace(err))
	d.SetMysqlDecimal(to)
	return
}

// GetPartialResult implements Aggregation interface.
func (af *avgFunction) GetPartialResult(ctx *AggEvaluateContext) []types.Datum {
	return []types.Datum{types.NewIntDatum(ctx.Count), ctx.Value}
}
