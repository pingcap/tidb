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

package mocktikv

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func pbToExpr(expr *tipb.Expr, colIDs map[int64]int, sc *variable.StatementContext) (expression.Expression, error) {
	switch expr.Tp {
	case tipb.ExprType_ColumnRef:
		_, id, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offset, ok := colIDs[id]
		if !ok {
			return nil, errors.Errorf("Can't find column id %d", id)
		}
		return &expression.Column{Index: offset}, nil
	case tipb.ExprType_Null:
		return &expression.Constant{}, nil
	case tipb.ExprType_Int64:
		return convertInt(expr.Val)
	case tipb.ExprType_Uint64:
		return convertUint(expr.Val)
	case tipb.ExprType_String:
		return convertString(expr.Val)
	case tipb.ExprType_Bytes:
		return &expression.Constant{Value: types.NewBytesDatum(expr.Val)}, nil
	case tipb.ExprType_Float32:
		return convertFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		return convertFloat(expr.Val, false)
	case tipb.ExprType_MysqlDecimal:
		return convertDecimal(expr.Val)
	case tipb.ExprType_MysqlDuration:
		return convertDuration(expr.Val)
	}
	args := make([]expression.Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		arg, err := pbToExpr(child, colIDs, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		args = append(args, arg)
	}
	// Then it must be a scalar function.
	return expression.NewDistSQLFunction(sc, expr.Tp, args)
}

func convertInt(val []byte) (*expression.Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid int % x", val)
	}
	d.SetInt64(i)
	return &expression.Constant{Value: d}, nil
}

func convertUint(val []byte) (*expression.Constant, error) {
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid uint % x", val)
	}
	d.SetUint64(u)
	return &expression.Constant{Value: d}, nil
}

func convertString(val []byte) (*expression.Constant, error) {
	var d types.Datum
	d.SetBytesAsString(val)
	return &expression.Constant{Value: d}, nil
}

func convertFloat(val []byte, f32 bool) (*expression.Constant, error) {
	var d types.Datum
	_, f, err := codec.DecodeFloat(val)
	if err != nil {
		return nil, errors.Errorf("invalid float % x", val)
	}
	if f32 {
		d.SetFloat32(float32(f))
	} else {
		d.SetFloat64(f)
	}
	return &expression.Constant{Value: d}, nil
}

func convertDecimal(val []byte) (*expression.Constant, error) {
	_, dec, err := codec.DecodeDecimal(val)
	if err != nil {
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	return &expression.Constant{Value: dec}, nil
}

func convertDuration(val []byte) (*expression.Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid duration %d", i)
	}
	d.SetMysqlDuration(types.Duration{Duration: time.Duration(i), Fsp: types.MaxFsp})
	return &expression.Constant{Value: d}, nil
}
