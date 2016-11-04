// Copyright 2016 PingCAP, Inc.
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

package xeval

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	"math"
)

func (e *Evaluator) evalMathFuncs(expr *tipb.Expr) (d types.Datum, err error) {
	switch expr.GetTp() {
	case tipb.ExprType_Pow:
		return e.evalPow(expr)
	case tipb.ExprType_Round:
		return e.evalRound(expr)
	case tipb.ExprType_Abs:
		return e.evalAbs(expr)
	}
	return
}

func (e *Evaluator) evalAbs(expr *tipb.Expr) (types.Datum, error) {
	if len(expr.Children) != 1 {
		return types.Datum{}, ErrInvalid.Gen("ABS need 1 operand, got %d", len(expr.Children))
	}
	d, err := e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch d.Kind() {
	case types.KindNull:
		return d, nil
	case types.KindUint64:
		return d, nil
	case types.KindInt64:
		iv := d.GetInt64()
		if iv >= 0 {
			d.SetInt64(iv)
			return d, nil
		}
		d.SetInt64(-iv)
		return d, nil
	default:
		f, err := d.ToFloat64()
		d.SetFloat64(math.Abs(f))
		return d, errors.Trace(err)
	}
}

func (e *Evaluator) evalPow(expr *tipb.Expr) (types.Datum, error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d := types.Datum{}
	if left.IsNull() || right.IsNull() {
		return d, nil
	}
	x, err := left.ToFloat64()
	if err != nil {
		return d, errors.Trace(err)
	}
	y, err := right.ToFloat64()
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetFloat64(math.Pow(x, y))
	return d, nil
}

func (e *Evaluator) evalRound(expr *tipb.Expr) (types.Datum, error) {
	child0, err := e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, err
	}
	if child0.IsNull() {
		return types.Datum{}, nil
	}
	x, err := child0.ToDecimal()
	if err != nil {
		return types.Datum{}, err
	}
	var d int64
	if len(expr.Children) == 2 {
		child1, err := e.Eval(expr.Children[1])
		if err != nil {
			return types.Datum{}, err
		}
		if child1.IsNull() {
			return types.Datum{}, nil
		}
		d, err = child1.ToInt64()
		if err != nil {
			return types.Datum{}, err
		}
	}
	to := new(mysql.MyDecimal)
	err = x.Round(to, int(d))
	if err != nil {
		return types.Datum{}, err
	}
	return types.NewDecimalDatum(to), nil
}
