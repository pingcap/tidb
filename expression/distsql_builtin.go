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

package expression

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var distFuncs = map[tipb.ExprType]builtinFunc{
	// compare op
	tipb.ExprType_LT:     &builtinCompareSig{op: opcode.LT},
	tipb.ExprType_LE:     &builtinCompareSig{op: opcode.LE},
	tipb.ExprType_GT:     &builtinCompareSig{op: opcode.GT},
	tipb.ExprType_GE:     &builtinCompareSig{op: opcode.GE},
	tipb.ExprType_EQ:     &builtinCompareSig{op: opcode.EQ},
	tipb.ExprType_NE:     &builtinCompareSig{op: opcode.NE},
	tipb.ExprType_NullEQ: &builtinCompareSig{op: opcode.NullEQ},

	// bit op
	tipb.ExprType_BitAnd:    &builtinBitOpSig{op: opcode.And},
	tipb.ExprType_BitOr:     &builtinBitOpSig{op: opcode.Or},
	tipb.ExprType_BitXor:    &builtinBitOpSig{op: opcode.Xor},
	tipb.ExprType_RighShift: &builtinBitOpSig{op: opcode.RightShift},
	tipb.ExprType_LeftShift: &builtinBitOpSig{op: opcode.LeftShift},
	tipb.ExprType_BitNeg:    &builtinUnaryOpSig{op: opcode.BitNeg}, // TODO: uniform it!

	// logical op
	tipb.ExprType_And: &builtinAndAndSig{},
	tipb.ExprType_Or:  &builtinOrOrSig{},
	tipb.ExprType_Xor: &builtinLogicXorSig{},
	tipb.ExprType_Not: &builtinUnaryOpSig{op: opcode.Not},

	// arithmetic operator
	tipb.ExprType_Plus:   &builtinArithmeticSig{op: opcode.Plus},
	tipb.ExprType_Minus:  &builtinArithmeticSig{op: opcode.Minus},
	tipb.ExprType_Mul:    &builtinArithmeticSig{op: opcode.Mul},
	tipb.ExprType_Div:    &builtinArithmeticSig{op: opcode.Div},
	tipb.ExprType_IntDiv: &builtinArithmeticSig{op: opcode.IntDiv},
	tipb.ExprType_Mod:    &builtinArithmeticSig{op: opcode.Mod},

	// control operator
	tipb.ExprType_Case:   &builtinCaseWhenSig{},
	tipb.ExprType_If:     &builtinIfSig{},
	tipb.ExprType_IfNull: &builtinIfNullSig{},
	tipb.ExprType_NullIf: &builtinNullIfSig{},

	// other operator
	tipb.ExprType_Like:     &builtinLikeSig{},
	tipb.ExprType_In:       &builtinInSig{},
	tipb.ExprType_IsNull:   &builtinIsNullSig{},
	tipb.ExprType_Coalesce: &builtinCoalesceSig{},
}

// newDistSQLFunction only creates function for mock-tikv.
func newDistSQLFunction(sc *variable.StatementContext, exprType tipb.ExprType, args []Expression) (*ScalarFunction, error) {
	f, ok := distFuncs[exprType]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs(exprType)
	}
	// TODO: Too ugly...
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = sc
	f.init(args, ctx)
	return &ScalarFunction{Function: f}, nil
}

// PBToExpr converts pb structure to expression.
func PBToExpr(expr *tipb.Expr, colIDs map[int64]int, sc *variable.StatementContext) (Expression, error) {
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
		return &Column{Index: offset}, nil
	case tipb.ExprType_Null:
		return &Constant{}, nil
	case tipb.ExprType_Int64:
		return convertInt(expr.Val)
	case tipb.ExprType_Uint64:
		return convertUint(expr.Val)
	case tipb.ExprType_String:
		return convertString(expr.Val)
	case tipb.ExprType_Bytes:
		return &Constant{Value: types.NewBytesDatum(expr.Val)}, nil
	case tipb.ExprType_Float32:
		return convertFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		return convertFloat(expr.Val, false)
	case tipb.ExprType_MysqlDecimal:
		return convertDecimal(expr.Val)
	case tipb.ExprType_MysqlDuration:
		return convertDuration(expr.Val)
	}
	// Then it must be a scalar function.
	args := make([]Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		if child.Tp == tipb.ExprType_ValueList {
			results, err := decodeValueList(child.Val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(results) == 0 {
				return &Constant{Value: types.NewDatum(false)}, nil
			}
			args = append(args, results...)
			continue
		}
		arg, err := PBToExpr(child, colIDs, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		args = append(args, arg)
	}
	return newDistSQLFunction(sc, expr.Tp, args)
}

func decodeValueList(data []byte) ([]Expression, error) {
	if len(data) == 0 {
		return nil, nil
	}
	list, err := codec.Decode(data, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := make([]Expression, 0, len(list))
	for _, value := range list {
		result = append(result, &Constant{Value: value})
	}
	return result, nil
}

func convertInt(val []byte) (*Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid int % x", val)
	}
	d.SetInt64(i)
	return &Constant{Value: d}, nil
}

func convertUint(val []byte) (*Constant, error) {
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid uint % x", val)
	}
	d.SetUint64(u)
	return &Constant{Value: d}, nil
}

func convertString(val []byte) (*Constant, error) {
	var d types.Datum
	d.SetBytesAsString(val)
	return &Constant{Value: d}, nil
}

func convertFloat(val []byte, f32 bool) (*Constant, error) {
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
	return &Constant{Value: d}, nil
}

func convertDecimal(val []byte) (*Constant, error) {
	_, dec, err := codec.DecodeDecimal(val)
	if err != nil {
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	return &Constant{Value: dec}, nil
}

func convertDuration(val []byte) (*Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid duration %d", i)
	}
	d.SetMysqlDuration(types.Duration{Duration: time.Duration(i), Fsp: types.MaxFsp})
	return &Constant{Value: d}, nil
}
