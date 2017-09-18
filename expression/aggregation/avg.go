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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
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
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// GetType implements Aggregation interface.
func (af *avgFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	types.SetBinChsClnFlag(ft)
	ft.Flen, ft.Decimal = mysql.MaxRealWidth, af.Args[0].GetType().Decimal
	return ft
}

func (af *avgFunction) updateAvg(row []types.Datum, groupKey []byte, sc *variable.StatementContext) error {
	ctx := af.getContext(groupKey)
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
func (af *avgFunction) Update(row []types.Datum, groupKey []byte, sc *variable.StatementContext) error {
	if af.mode == FinalMode {
		return af.updateAvg(row, groupKey, sc)
	}
	return af.updateSum(row, groupKey, sc)
}

// StreamUpdate implements Aggregation interface.
func (af *avgFunction) StreamUpdate(row []types.Datum, sc *variable.StatementContext) error {
	return af.streamUpdateSum(row, sc)
}

func (af *avgFunction) calculateResult(ctx *aggEvaluateContext) (d types.Datum) {
	switch ctx.Value.Kind() {
	case types.KindFloat64:
		t := ctx.Value.GetFloat64() / float64(ctx.Count)
		d.SetValue(t)
	case types.KindMysqlDecimal:
		x := ctx.Value.GetMysqlDecimal()
		y := types.NewDecFromInt(ctx.Count)
		to := new(types.MyDecimal)
		types.DecimalDiv(x, y, to, types.DivFracIncr)
		to.Round(to, ctx.Value.Frac()+types.DivFracIncr, types.ModeHalfEven)
		d.SetMysqlDecimal(to)
	}
	return
}

// GetGroupResult implements Aggregation interface.
func (af *avgFunction) GetGroupResult(groupKey []byte) types.Datum {
	ctx := af.getContext(groupKey)
	return af.calculateResult(ctx)
}

// GetPartialResult implements Aggregation interface.
func (af *avgFunction) GetPartialResult(groupKey []byte) []types.Datum {
	ctx := af.getContext(groupKey)
	return []types.Datum{types.NewIntDatum(ctx.Count), ctx.Value}
}

// GetStreamResult implements Aggregation interface.
func (af *avgFunction) GetStreamResult() (d types.Datum) {
	if af.streamCtx == nil {
		return
	}
	d = af.calculateResult(af.streamCtx)
	af.streamCtx = nil
	return
}
