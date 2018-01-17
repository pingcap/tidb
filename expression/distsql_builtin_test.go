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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

var _ = Suite(&testEvalSuite{})

type testEvalSuite struct{}

// TestEval test expr.Eval().
// TODO: add more tests.
func (s *testEvalSuite) TestEval(c *C) {
	row := types.DatumRow{types.NewDatum(100)}
	fieldTps := make([]*types.FieldType, 1)
	fieldTps[0] = types.NewFieldType(mysql.TypeDouble)
	tests := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		// Datums.
		{
			datumExpr(types.NewFloat32Datum(1.1)),
			types.NewFloat32Datum(1.1),
		},
		{
			datumExpr(types.NewFloat64Datum(1.1)),
			types.NewFloat64Datum(1.1),
		},
		{
			datumExpr(types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			datumExpr(types.NewUintDatum(1)),
			types.NewUintDatum(1),
		},
		{
			datumExpr(types.NewBytesDatum([]byte("abc"))),
			types.NewBytesDatum([]byte("abc")),
		},
		{
			datumExpr(types.NewStringDatum("abc")),
			types.NewStringDatum("abc"),
		},
		{
			datumExpr(types.Datum{}),
			types.Datum{},
		},
		{
			datumExpr(types.NewDurationDatum(types.Duration{Duration: time.Hour})),
			types.NewDurationDatum(types.Duration{Duration: time.Hour}),
		},
		{
			datumExpr(types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1))),
			types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1)),
		},
		{
			columnExpr(0),
			types.NewIntDatum(100),
		},
		// Comparison operations.
		{
			buildExpr(tipb.ExprType_LT, types.NewIntDatum(100), types.NewIntDatum(1)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_LT, types.NewIntDatum(1), types.NewIntDatum(100)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_LT, types.NewIntDatum(100), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_LE, types.NewIntDatum(100), types.NewIntDatum(1)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_LE, types.NewIntDatum(1), types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_LE, types.NewIntDatum(100), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_EQ, types.NewIntDatum(100), types.NewIntDatum(1)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_EQ, types.NewIntDatum(100), types.NewIntDatum(100)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_EQ, types.NewIntDatum(100), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_NE, types.NewIntDatum(100), types.NewIntDatum(100)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_NE, types.NewIntDatum(100), types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_NE, types.NewIntDatum(100), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_GE, types.NewIntDatum(1), types.NewIntDatum(100)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_GE, types.NewIntDatum(100), types.NewIntDatum(100)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_GE, types.NewIntDatum(100), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_GT, types.NewIntDatum(100), types.NewIntDatum(100)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_GT, types.NewIntDatum(100), types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_GT, types.NewIntDatum(100), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_NullEQ, types.NewIntDatum(1), types.Datum{}),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_NullEQ, types.Datum{}, types.Datum{}),
			types.NewIntDatum(1),
		},
		// Logic operation.
		{
			buildExpr(tipb.ExprType_And, types.NewIntDatum(0), types.NewIntDatum(1)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_And, buildExpr(tipb.ExprType_And, types.NewIntDatum(1), types.NewIntDatum(1)), buildExpr(tipb.ExprType_And, types.NewIntDatum(0), types.NewIntDatum(1))),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_And, types.NewIntDatum(1), types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_And, types.NewIntDatum(0), types.Datum{}),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_And, types.Datum{}, types.NewIntDatum(0)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_And, types.NewIntDatum(1), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_Or, types.NewIntDatum(0), types.NewIntDatum(0)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_Or, types.NewIntDatum(0), types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_Or, types.NewIntDatum(0), types.Datum{}),
			types.Datum{},
		},
		{
			buildExpr(tipb.ExprType_Or, types.NewIntDatum(1), types.Datum{}),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_Or, types.Datum{}, types.NewIntDatum(1)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_And,
				buildExpr(tipb.ExprType_EQ, types.NewIntDatum(1), types.NewIntDatum(1)),
				buildExpr(tipb.ExprType_EQ, types.NewIntDatum(1), types.NewIntDatum(1))),
			types.NewIntDatum(1),
		},
		{
			notExpr(datumExpr(types.NewIntDatum(1))),
			types.NewIntDatum(0),
		},
		{
			notExpr(datumExpr(types.NewIntDatum(0))),
			types.NewIntDatum(1),
		},
		{
			notExpr(datumExpr(types.Datum{})),
			types.Datum{},
		},
		// Arithmetic operation.
		{
			buildExpr(tipb.ExprType_Plus, types.NewIntDatum(-1), types.NewIntDatum(1)),
			types.NewIntDatum(0),
		},
		{
			buildExpr(tipb.ExprType_Plus, types.NewIntDatum(-1), types.NewFloat64Datum(1.5)),
			types.NewFloat64Datum(0.5),
		},
		{
			buildExpr(tipb.ExprType_Minus, types.NewIntDatum(-1), types.NewIntDatum(1)),
			types.NewIntDatum(-2),
		},
		{
			buildExpr(tipb.ExprType_Minus, types.NewIntDatum(-1), types.NewFloat64Datum(1.5)),
			types.NewFloat64Datum(-2.5),
		},
		{
			buildExpr(tipb.ExprType_Mul, types.NewFloat64Datum(-1), types.NewFloat64Datum(1)),
			types.NewFloat64Datum(-1),
		},
		{
			buildExpr(tipb.ExprType_Mul, types.NewFloat64Datum(-1.5), types.NewFloat64Datum(2)),
			types.NewFloat64Datum(-3),
		},
		{
			buildExpr(tipb.ExprType_Div, types.NewFloat64Datum(-3), types.NewFloat64Datum(2)),
			types.NewFloat64Datum(-1.5),
		},
		{
			buildExpr(tipb.ExprType_Div, types.NewFloat64Datum(-3), types.NewFloat64Datum(0)),
			types.NewDatum(nil),
		},
		{
			buildExpr(tipb.ExprType_IntDiv, types.NewIntDatum(3), types.NewIntDatum(2)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_IntDiv, types.NewFloat64Datum(3.0), types.NewFloat64Datum(1.9)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_Mod, types.NewIntDatum(3), types.NewIntDatum(2)),
			types.NewIntDatum(1),
		},
		{
			buildExpr(tipb.ExprType_Mod, types.NewFloat64Datum(3.0), types.NewFloat64Datum(1.9)),
			types.NewFloat64Datum(1.1),
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
			expr.Children[i] = datumExpr(x)
		case *tipb.Expr:
			expr.Children[i] = x
		}
	}
	return expr
}

func datumExpr(d types.Datum) *tipb.Expr {
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
		expr.Val = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
	default:
		expr.Tp = tipb.ExprType_Null
	}
	return expr
}

func columnExpr(columnID int64) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tipb.ExprType_ColumnRef
	expr.Val = codec.EncodeInt(nil, columnID)
	return expr
}

func notExpr(value interface{}) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tipb.ExprType_Not
	switch x := value.(type) {
	case types.Datum:
		expr.Children = []*tipb.Expr{datumExpr(x)}
	case *tipb.Expr:
		expr.Children = []*tipb.Expr{x}
	}
	return expr
}

func (s *testEvalSuite) TestEvalIsNull(c *C) {
	null, trueAns, falseAns := types.Datum{}, types.NewIntDatum(1), types.NewIntDatum(0)
	tests := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr:   buildExpr(tipb.ExprType_IsNull, types.NewStringDatum("abc")),
			result: falseAns,
		},
		{
			expr:   buildExpr(tipb.ExprType_IsNull, null),
			result: trueAns,
		},
		{
			expr:   buildExpr(tipb.ExprType_IsNull, types.NewIntDatum(0)),
			result: falseAns,
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		expr, err := PBToExpr(tt.expr, nil, sc)
		c.Assert(err, IsNil, Commentf("%v", tt))
		result, err := expr.Eval(nil)
		c.Assert(err, IsNil, Commentf("%v", tt))
		c.Assert(result.Kind(), Equals, tt.result.Kind(), Commentf("%v", tt))
		cmp, err := result.CompareDatum(sc, &tt.result)
		c.Assert(err, IsNil, Commentf("%v", tt))
		c.Assert(cmp, Equals, 0, Commentf("%v", tt))
	}
}
