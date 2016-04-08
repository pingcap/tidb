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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// Error instances.
var (
	ErrInvalid = terror.ClassXEval.New(CodeInvalid, "invalid operation")
)

// Error codes.
const (
	CodeInvalid = 1
)

const (
	compareResultNull = -2
)

// Evaluator evaluates tipb.Expr.
type Evaluator struct {
	Row map[int64]types.Datum // column values.
}

// Eval evaluates expr to a Datum.
func (e *Evaluator) Eval(expr *tipb.Expr) (types.Datum, error) {
	switch expr.GetTp() {
	case tipb.ExprType_Null:
		return types.Datum{}, nil
	case tipb.ExprType_Int64:
		return e.evalInt(expr.Val)
	case tipb.ExprType_Uint64:
		return e.evalUint(expr.Val)
	case tipb.ExprType_String:
		return e.evalString(expr.Val)
	case tipb.ExprType_Bytes:
		return types.NewBytesDatum(expr.Val), nil
	case tipb.ExprType_Float32:
		return e.evalFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		return e.evalFloat(expr.Val, false)
	case tipb.ExprType_ColumnRef:
		return e.evalColumnRef(expr.Val)
	case tipb.ExprType_LT:
		return e.evalLT(expr)
	case tipb.ExprType_LE:
		return e.evalLE(expr)
	case tipb.ExprType_EQ:
		return e.evalEQ(expr)
	case tipb.ExprType_NE:
		return e.evalNE(expr)
	case tipb.ExprType_GE:
		return e.evalGE(expr)
	case tipb.ExprType_GT:
		return e.evalGT(expr)
	case tipb.ExprType_NullEQ:
		return e.evalNullEQ(expr)
	case tipb.ExprType_And:
		return e.evalAnd(expr)
	case tipb.ExprType_Or:
		return e.evalOr(expr)
	}
	return types.Datum{}, nil
}

func (e *Evaluator) evalColumnRef(val []byte) (types.Datum, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return d, errors.Trace(err)
	}
	d, ok := e.Row[i]
	if !ok {
		return d, ErrInvalid.Gen("column % x not found", val)
	}
	return d, nil
}

func (e *Evaluator) evalInt(val []byte) (types.Datum, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return d, ErrInvalid.Gen("invalid int % x", val)
	}
	d.SetInt64(i)
	return d, nil
}

func (e *Evaluator) evalUint(val []byte) (types.Datum, error) {
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return d, ErrInvalid.Gen("invalid uint % x", val)
	}
	d.SetUint64(u)
	return d, nil
}

func (e *Evaluator) evalString(val []byte) (types.Datum, error) {
	var d types.Datum
	d.SetBytesAsString(val)
	return d, nil
}

func (e *Evaluator) evalFloat(val []byte, f32 bool) (types.Datum, error) {
	var d types.Datum
	_, f, err := codec.DecodeFloat(val)
	if err != nil {
		return d, ErrInvalid.Gen("invalid float % x", val)
	}
	if f32 {
		d.SetFloat32(float32(f))
	} else {
		d.SetFloat64(f)
	}
	return d, nil
}

func (e *Evaluator) evalLT(expr *tipb.Expr) (types.Datum, error) {
	cmp, err := e.compareTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == compareResultNull {
		return types.Datum{}, nil
	} else if cmp < 0 {
		return types.NewIntDatum(1), nil
	} else {
		return types.NewIntDatum(0), nil
	}
}

func (e *Evaluator) evalLE(expr *tipb.Expr) (types.Datum, error) {
	cmp, err := e.compareTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == compareResultNull {
		return types.Datum{}, nil
	} else if cmp <= 0 {
		return types.NewIntDatum(1), nil
	} else {
		return types.NewIntDatum(0), nil
	}
}

func (e *Evaluator) evalEQ(expr *tipb.Expr) (types.Datum, error) {
	cmp, err := e.compareTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == compareResultNull {
		return types.Datum{}, nil
	} else if cmp == 0 {
		return types.NewIntDatum(1), nil
	} else {
		return types.NewIntDatum(0), nil
	}
}

func (e *Evaluator) evalNE(expr *tipb.Expr) (types.Datum, error) {
	cmp, err := e.compareTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == compareResultNull {
		return types.Datum{}, nil
	} else if cmp != 0 {
		return types.NewIntDatum(1), nil
	} else {
		return types.NewIntDatum(0), nil
	}
}

func (e *Evaluator) evalGE(expr *tipb.Expr) (types.Datum, error) {
	cmp, err := e.compareTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == compareResultNull {
		return types.Datum{}, nil
	} else if cmp >= 0 {
		return types.NewIntDatum(1), nil
	} else {
		return types.NewIntDatum(0), nil
	}
}

func (e *Evaluator) evalGT(expr *tipb.Expr) (types.Datum, error) {
	cmp, err := e.compareTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == compareResultNull {
		return types.Datum{}, nil
	} else if cmp > 0 {
		return types.NewIntDatum(1), nil
	} else {
		return types.NewIntDatum(0), nil
	}
}

func (e *Evaluator) evalNullEQ(expr *tipb.Expr) (types.Datum, error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	cmp, err := left.CompareDatum(right)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) compareTwoChildren(expr *tipb.Expr) (int, error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if left.Kind() == types.KindNull || right.Kind() == types.KindNull {
		return compareResultNull, nil
	}
	return left.CompareDatum(right)
}

func (e *Evaluator) evalAnd(expr *tipb.Expr) (types.Datum, error) {
	leftBool, rightBool, err := e.evalTwoBoolChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if leftBool == 1 && rightBool == 1 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalOr(expr *tipb.Expr) (types.Datum, error) {
	leftBool, rightBool, err := e.evalTwoBoolChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if leftBool == 1 || rightBool == 1 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalTwoBoolChildren(expr *tipb.Expr) (leftBool, rightBool int64, err error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if left.Kind() == types.KindNull {
		leftBool = compareResultNull
	} else {
		leftBool, err = left.ToBool()
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
	}
	if right.Kind() == types.KindNull {
		rightBool = compareResultNull
	} else {
		rightBool, err = right.ToBool()
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
	}
	return
}

func (e *Evaluator) evalTwoChildren(expr *tipb.Expr) (left, right types.Datum, err error) {
	if len(expr.Children) != 2 {
		err = ErrInvalid.Gen("need 2 operands but got %d", len(expr.Children))
		return
	}
	left, err = e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, types.Datum{}, errors.Trace(err)
	}
	right, err = e.Eval(expr.Children[1])
	if err != nil {
		return types.Datum{}, types.Datum{}, errors.Trace(err)
	}
	return
}
