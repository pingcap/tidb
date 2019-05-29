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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"time"
)

var _ = Suite(&testEvalSuite{})

type testEvalSuite struct {
	colID int64
}

func (s *testEvalSuite) SetUpSuite(c *C) {
	s.colID = 0
}

func (s *testEvalSuite) allocColID() int64 {
	s.colID++
	return s.colID
}

// TestEval test expr.Eval().
// TODO: add more tests.
func (s *testEvalSuite) TestEval(c *C) {
	row := chunk.MutRowFromDatums([]types.Datum{types.NewDatum(100)}).ToRow()
	fieldTps := make([]*types.FieldType, 1)
	fieldTps[0] = types.NewFieldType(mysql.TypeLonglong)
	tests := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		// Datums.
		{
			datumExpr(c, types.NewFloat32Datum(1.1)),
			types.NewFloat32Datum(1.1),
		},
		{
			datumExpr(c, types.NewFloat64Datum(1.1)),
			types.NewFloat64Datum(1.1),
		},
		{
			datumExpr(c, types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			datumExpr(c, types.NewUintDatum(1)),
			types.NewUintDatum(1),
		},
		{
			datumExpr(c, types.NewBytesDatum([]byte("abc"))),
			types.NewBytesDatum([]byte("abc")),
		},
		{
			datumExpr(c, types.NewStringDatum("abc")),
			types.NewStringDatum("abc"),
		},
		{
			datumExpr(c, types.Datum{}),
			types.Datum{},
		},
		{
			datumExpr(c, types.NewDurationDatum(types.Duration{Duration: time.Hour})),
			types.NewDurationDatum(types.Duration{Duration: time.Hour}),
		},
		{
			datumExpr(c, types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1))),
			types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1)),
		},
		// Columns.
		{
			columnExpr(0),
			types.NewIntDatum(100),
		},
		// Scalar Functions.
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(c, `true`),
			),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonDatumExpr(c, `[10, {"a": 20}]`),
			),
			types.NewIntDatum(3),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_JsonSearchSig,
				toPBFieldType(newJSONFieldType()),
				jsonDatumExpr(c, `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`),
				datumExpr(c, types.NewBytesDatum([]byte(`all`))),
				datumExpr(c, types.NewBytesDatum([]byte(`10`))),
				datumExpr(c, types.NewBytesDatum([]byte(`\`))),
				datumExpr(c, types.NewBytesDatum([]byte(`$**.k`))),
			),
			newJSONDatum(c, `"$[1][0].k"`),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewIntDatum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewFloat64Datum(2333),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewStringDatum("2333"),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastDecimalAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2333")))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastIntAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntDatum(2333))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastRealAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewFloat64Datum(2333))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_CastStringAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewStringDatum("2333"))),
			types.NewDecimalDatum(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(2)), datumExpr(c, types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(1)), datumExpr(c, types.NewIntDatum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(2)), datumExpr(c, types.NewFloat64Datum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Datum(1)), datumExpr(c, types.NewFloat64Datum(2))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "2")))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_GEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_LEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationDatum(newDuration(time.Second))), datumExpr(c, types.NewDurationDatum(newDuration(time.Second*2)))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_NullEQDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil)), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DecimalIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_DurationIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_RealIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDatum(nil))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntDatum(-1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsUInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewUintDatum(1))),
			types.NewIntDatum(1),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Datum(-1.23))),
			types.NewFloat64Datum(1.23),
		},
		{
			scalarFunctionExpr(tipb.ScalarFuncSig_AbsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalDatum(newMyDecimal(c, "-1.23")))),
			types.NewDecimalDatum(newMyDecimal(c, "1.23")),
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		expr, err := PBToExpr(tt.expr, fieldTps, sc)
		c.Assert(err, IsNil)
		result, err := expr.Eval(row)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, tt.result.Kind())
		cmp, err := result.CompareDatum(sc, &tt.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func buildExpr(tp tipb.ExprType, children ...interface{}) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tp
	expr.Children = make([]*tipb.Expr, len(children))
	for i, child := range children {
		switch x := child.(type) {
		case types.Datum:
			expr.Children[i] = datumExpr(nil, x)
		case *tipb.Expr:
			expr.Children[i] = x
		}
	}
	return expr
}

func datumExpr(c *C, d types.Datum) *tipb.Expr {
	expr := new(tipb.Expr)
	switch d.Kind() {
	case types.KindInt64:
		expr.Tp = tipb.ExprType_Int64
		expr.Val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		expr.Tp = tipb.ExprType_Uint64
		expr.Val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		expr.Tp = tipb.ExprType_String
		expr.Val = d.GetBytes()
	case types.KindBytes:
		expr.Tp = tipb.ExprType_Bytes
		expr.Val = d.GetBytes()
	case types.KindFloat32:
		expr.Tp = tipb.ExprType_Float32
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		expr.Tp = tipb.ExprType_Float64
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		expr.Tp = tipb.ExprType_MysqlDuration
		expr.Val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		expr.Tp = tipb.ExprType_MysqlDecimal
		var err error
		expr.Val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		c.Assert(err, IsNil)
	case types.KindMysqlJSON:
		expr.Tp = tipb.ExprType_MysqlJson
		var err error
		expr.Val = make([]byte, 0, 1024)
		expr.Val, err = codec.EncodeValue(nil, expr.Val, d)
		c.Assert(err, IsNil)
	default:
		expr.Tp = tipb.ExprType_Null
	}
	return expr
}

func newJSONDatum(c *C, s string) (d types.Datum) {
	j, err := json.ParseBinaryFromString(s)
	c.Assert(err, IsNil)
	d.SetMysqlJSON(j)
	return d
}

func jsonDatumExpr(c *C, s string) *tipb.Expr {
	return datumExpr(c, newJSONDatum(c, s))
}

func columnExpr(columnID int64) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tipb.ExprType_ColumnRef
	expr.Val = codec.EncodeInt(nil, columnID)
	return expr
}

// toPBFieldType converts *types.FieldType to *tipb.FieldType.
func toPBFieldType(ft *types.FieldType) *tipb.FieldType {
	return &tipb.FieldType{
		Tp:      int32(ft.Tp),
		Flag:    uint32(ft.Flag),
		Flen:    int32(ft.Flen),
		Decimal: int32(ft.Decimal),
		Charset: ft.Charset,
		Collate: collationToProto(ft.Collate),
	}
}

func newMyDecimal(c *C, s string) *types.MyDecimal {
	d := new(types.MyDecimal)
	c.Assert(d.FromString([]byte(s)), IsNil)
	return d
}

func newDuration(dur time.Duration) types.Duration {
	return types.Duration{
		Duration: dur,
		Fsp:      types.DefaultFsp,
	}
}

func newIntFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    mysql.MaxIntWidth,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
	}
}

func newDurFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeDuration,
		Flag: types.DefaultFsp,
	}
}

func newStringFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeVarString,
		Flen: types.UnspecifiedLength,
	}
}

func newRealFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeFloat,
		Flen: types.UnspecifiedLength,
	}
}

func newDecimalFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeNewDecimal,
		Flen: types.UnspecifiedLength,
	}
}

func newJSONFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeJSON,
		Flen:    types.UnspecifiedLength,
		Decimal: 0,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
}

func scalarFunctionExpr(sigCode tipb.ScalarFuncSig, retType *tipb.FieldType, args ...*tipb.Expr) *tipb.Expr {
	return &tipb.Expr{
		Tp:        tipb.ExprType_ScalarFunc,
		Sig:       sigCode,
		Children:  args,
		FieldType: retType,
	}
}
