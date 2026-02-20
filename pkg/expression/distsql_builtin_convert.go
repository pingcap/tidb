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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

func newDistSQLFunctionBySig(ctx BuildContext, sigCode tipb.ScalarFuncSig, tp *tipb.FieldType, args []Expression) (Expression, error) {
	f, err := getSignatureByPB(ctx, sigCode, tp, args)
	if err != nil {
		return nil, err
	}
	// derive collation information for string function, and we must do it
	// before doing implicit cast.
	funcName := tipb.ScalarFuncSig_name[int32(sigCode)]
	argTps := make([]types.EvalType, 0, len(args))
	for _, arg := range args {
		argTps = append(argTps, arg.GetType(ctx.GetEvalCtx()).EvalType())
	}
	ec, err := deriveCollation(ctx, funcName, args, f.getRetTp().EvalType(), argTps...)
	if err != nil {
		return nil, err
	}
	funcF := &ScalarFunction{
		FuncName: ast.NewCIStr(fmt.Sprintf("sig_%T", f)),
		Function: f,
		RetType:  f.getRetTp(),
	}
	funcF.SetCoercibility(ec.Coer)
	return funcF, nil
}

// PBToExprs converts pb structures to expressions.
func PBToExprs(ctx BuildContext, pbExprs []*tipb.Expr, fieldTps []*types.FieldType) ([]Expression, error) {
	exprs := make([]Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := PBToExpr(ctx, expr, fieldTps)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e == nil {
			return nil, errors.Errorf("pb to expression failed, pb expression is %v", expr)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

// PBToExpr converts pb structure to expression.
func PBToExpr(ctx BuildContext, expr *tipb.Expr, tps []*types.FieldType) (Expression, error) {
	evalCtx := ctx.GetEvalCtx()
	switch expr.Tp {
	case tipb.ExprType_ColumnRef:
		_, offset, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, err
		}
		return &Column{Index: int(offset), RetType: tps[offset]}, nil
	case tipb.ExprType_Null:
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}, nil
	case tipb.ExprType_Int64:
		return convertInt(expr.Val, expr.FieldType)
	case tipb.ExprType_Uint64:
		return convertUint(expr.Val, expr.FieldType)
	case tipb.ExprType_String:
		return convertString(expr.Val, expr.FieldType)
	case tipb.ExprType_Bytes:
		return &Constant{Value: types.NewBytesDatum(expr.Val), RetType: types.NewFieldType(mysql.TypeString)}, nil
	case tipb.ExprType_MysqlBit:
		return &Constant{Value: types.NewMysqlBitDatum(expr.Val), RetType: types.NewFieldType(mysql.TypeString)}, nil
	case tipb.ExprType_Float32:
		return convertFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		return convertFloat(expr.Val, false)
	case tipb.ExprType_MysqlDecimal:
		return convertDecimal(expr.Val, expr.FieldType)
	case tipb.ExprType_MysqlDuration:
		return convertDuration(expr.Val)
	case tipb.ExprType_MysqlTime:
		return convertTime(expr.Val, expr.FieldType, evalCtx.Location())
	case tipb.ExprType_MysqlJson:
		return convertJSON(expr.Val)
	case tipb.ExprType_MysqlEnum:
		return convertEnum(expr.Val, expr.FieldType)
	case tipb.ExprType_TiDBVectorFloat32:
		return convertVectorFloat32(expr.Val)
	}
	if expr.Tp != tipb.ExprType_ScalarFunc {
		panic("should be a tipb.ExprType_ScalarFunc")
	}
	// Then it must be a scalar function.
	args := make([]Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		if child.Tp == tipb.ExprType_ValueList {
			results, err := decodeValueList(child.Val)
			if err != nil {
				return nil, err
			}
			if len(results) == 0 {
				return &Constant{Value: types.NewDatum(false), RetType: types.NewFieldType(mysql.TypeLonglong)}, nil
			}
			args = append(args, results...)
			continue
		}
		arg, err := PBToExpr(ctx, child, tps)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	sf, err := newDistSQLFunctionBySig(ctx, expr.Sig, expr.FieldType, args)
	if err != nil {
		return nil, err
	}

	return sf, nil
}

func convertTime(data []byte, ftPB *tipb.FieldType, tz *time.Location) (*Constant, error) {
	ft := PbTypeToFieldType(ftPB)
	_, v, err := codec.DecodeUint(data)
	if err != nil {
		return nil, err
	}
	var t types.Time
	t.SetType(ft.GetType())
	t.SetFsp(ft.GetDecimal())
	err = t.FromPackedUint(v)
	if err != nil {
		return nil, err
	}
	if ft.GetType() == mysql.TypeTimestamp && tz != time.UTC {
		err = t.ConvertTimeZone(time.UTC, tz)
		if err != nil {
			return nil, err
		}
	}
	return &Constant{Value: types.NewTimeDatum(t), RetType: ft}, nil
}

func decodeValueList(data []byte) ([]Expression, error) {
	if len(data) == 0 {
		return nil, nil
	}
	list, err := codec.Decode(data, 1)
	if err != nil {
		return nil, err
	}
	result := make([]Expression, 0, len(list))
	for _, value := range list {
		result = append(result, &Constant{Value: value})
	}
	return result, nil
}

func convertInt(val []byte, tp *tipb.FieldType) (*Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid int % x", val)
	}
	d.SetInt64(i)
	return &Constant{Value: d, RetType: PbTypeToFieldType(tp)}, nil
}

func convertUint(val []byte, tp *tipb.FieldType) (*Constant, error) {
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid uint % x", val)
	}
	d.SetUint64(u)
	return &Constant{Value: d, RetType: PbTypeToFieldType(tp)}, nil
}

func convertString(val []byte, tp *tipb.FieldType) (*Constant, error) {
	var d types.Datum
	d.SetBytesAsString(val, collate.ProtoToCollation(tp.Collate), uint32(tp.Flen))
	return &Constant{Value: d, RetType: PbTypeToFieldType(tp)}, nil
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
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeDouble)}, nil
}

func convertDecimal(val []byte, ftPB *tipb.FieldType) (*Constant, error) {
	ft := PbTypeToFieldType(ftPB)
	_, dec, precision, frac, err := codec.DecodeDecimal(val)
	var d types.Datum
	d.SetMysqlDecimal(dec)
	d.SetLength(precision)
	d.SetFrac(frac)
	if err != nil {
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	return &Constant{Value: d, RetType: ft}, nil
}

func convertDuration(val []byte) (*Constant, error) {
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid duration %d", i)
	}
	d.SetMysqlDuration(types.Duration{Duration: time.Duration(i), Fsp: types.MaxFsp})
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeDuration)}, nil
}

func convertJSON(val []byte) (*Constant, error) {
	var d types.Datum
	_, d, err := codec.DecodeOne(val)
	if err != nil {
		return nil, errors.Errorf("invalid json % x", val)
	}
	if d.Kind() != types.KindMysqlJSON {
		return nil, errors.Errorf("invalid Datum.Kind() %d", d.Kind())
	}
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeJSON)}, nil
}

func convertVectorFloat32(val []byte) (*Constant, error) {
	v, _, err := types.ZeroCopyDeserializeVectorFloat32(val)
	if err != nil {
		return nil, errors.Errorf("invalid VectorFloat32 %x", val)
	}
	var d types.Datum
	d.SetVectorFloat32(v)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)}, nil
}

func convertEnum(val []byte, tp *tipb.FieldType) (*Constant, error) {
	_, uVal, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid enum % x", val)
	}
	// If uVal is 0, it should return Enum{}
	var e = types.Enum{}
	if uVal != 0 {
		e, err = types.ParseEnumValue(tp.Elems, uVal)
		if err != nil {
			return nil, err
		}
	}
	d := types.NewMysqlEnumDatum(e)
	return &Constant{Value: d, RetType: FieldTypeFromPB(tp)}, nil
}
