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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (e *Evaluator) evalDataType(expr *tipb.Expr) (types.Datum, error) {
	switch op := expr.GetTp(); op {
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
	case tipb.ExprType_MysqlDecimal:
		return e.evalDecimal(expr.Val)
	case tipb.ExprType_MysqlDuration:
		return e.evalDuration(expr.Val)
	case tipb.ExprType_ColumnRef:
		return e.evalColumnRef(expr.Val)
	default:
		return types.Datum{}, errors.Errorf("Unknown binop type: %v", op)
	}
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

func (e *Evaluator) evalDecimal(val []byte) (types.Datum, error) {
	_, dec, err := codec.DecodeDecimal(val)
	if err != nil {
		return dec, ErrInvalid.Gen("invalid decimal % x", val)
	}
	return dec, nil
}

func (e *Evaluator) evalDuration(val []byte) (types.Datum, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return d, ErrInvalid.Gen("invalid duration %d", i)
	}
	d.SetMysqlDuration(mysql.Duration{Duration: time.Duration(i), Fsp: mysql.MaxFsp})
	return d, nil
}
