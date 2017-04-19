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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testEvalSuite{})

type testEvalSuite struct{}

// TODO: add more tests.
func (s *testEvalSuite) TestEval(c *C) {
	colID := int64(1)
	xevaluator := NewEvaluator(new(variable.StatementContext))
	xevaluator.Row[colID] = types.NewIntDatum(100)
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
			columnExpr(1),
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
	for _, tt := range tests {
		result, err := xevaluator.Eval(tt.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, tt.result.Kind())
		cmp, err := result.CompareDatum(xevaluator.StatementCtx, tt.result)
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
		expr.Val = codec.EncodeDecimal(nil, d)
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

func likeExpr(target, pattern string) *tipb.Expr {
	targetExpr := datumExpr(types.NewStringDatum(target))
	patternExpr := datumExpr(types.NewStringDatum(pattern))
	return &tipb.Expr{Tp: tipb.ExprType_Like, Children: []*tipb.Expr{targetExpr, patternExpr}}
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

func (s *testEvalSuite) TestLike(c *C) {
	tests := []struct {
		expr   *tipb.Expr
		result int64
	}{
		{
			expr:   likeExpr("a", ""),
			result: 0,
		},
		{
			expr:   likeExpr("a", "a"),
			result: 1,
		},
		{
			expr:   likeExpr("a", "b"),
			result: 0,
		},
		{
			expr:   likeExpr("aAb", "AaB"),
			result: 1,
		},
		{
			expr:   likeExpr("a", "%"),
			result: 1,
		},
		{
			expr:   likeExpr("aAD", "%d"),
			result: 1,
		},
		{
			expr:   likeExpr("aAeD", "%e"),
			result: 0,
		},
		{
			expr:   likeExpr("aAb", "Aa%"),
			result: 1,
		},
		{
			expr:   likeExpr("abAb", "Aa%"),
			result: 0,
		},
		{
			expr:   likeExpr("aAcb", "%C%"),
			result: 1,
		},
		{
			expr:   likeExpr("aAb", "%C%"),
			result: 0,
		},
	}
	ev := NewEvaluator(new(variable.StatementContext))
	for _, tt := range tests {
		res, err := ev.Eval(tt.expr)
		c.Check(err, IsNil)
		c.Check(res.GetInt64(), Equals, tt.result)
	}
}

func (s *testEvalSuite) TestWhereIn(c *C) {
	tests := []struct {
		expr   *tipb.Expr
		result interface{}
	}{
		{
			expr:   inExpr(1, 1, 2),
			result: true,
		},
		{
			expr:   inExpr(1, 1, nil),
			result: true,
		},
		{
			expr:   inExpr(1, 2, nil),
			result: nil,
		},
		{
			expr:   inExpr(nil, 1, nil),
			result: nil,
		},
		{
			expr:   inExpr(2, 1, nil),
			result: nil,
		},
		{
			expr:   inExpr(2),
			result: false,
		},
		{
			expr:   inExpr("abc", "abc", "ab"),
			result: true,
		},
		{
			expr:   inExpr("abc", "aba", "bab"),
			result: false,
		},
	}
	ev := NewEvaluator(new(variable.StatementContext))
	for _, tt := range tests {
		res, err := ev.Eval(tt.expr)
		c.Check(err, IsNil)
		if tt.result == nil {
			c.Check(res.Kind(), Equals, types.KindNull)
		} else {
			c.Check(res.Kind(), Equals, types.KindInt64)
			if tt.result == true {
				c.Check(res.GetInt64(), Equals, int64(1))
			} else {
				c.Check(res.GetInt64(), Equals, int64(0))
			}
		}
	}
}

func (s *testEvalSuite) TestEvalIsNull(c *C) {
	colID := int64(1)
	xevaluator := NewEvaluator(new(variable.StatementContext))
	xevaluator.Row[colID] = types.NewIntDatum(100)
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
	for _, tt := range tests {
		result, err := xevaluator.Eval(tt.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, tt.result.Kind())
		cmp, err := result.CompareDatum(xevaluator.StatementCtx, tt.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func inExpr(target interface{}, list ...interface{}) *tipb.Expr {
	targetDatum := types.NewDatum(target)
	var listDatums []types.Datum
	for _, v := range list {
		listDatums = append(listDatums, types.NewDatum(v))
	}
	sc := new(variable.StatementContext)
	types.SortDatums(sc, listDatums)
	targetExpr := datumExpr(targetDatum)
	val, _ := codec.EncodeValue(nil, listDatums...)
	listExpr := &tipb.Expr{Tp: tipb.ExprType_ValueList, Val: val}
	return &tipb.Expr{Tp: tipb.ExprType_In, Children: []*tipb.Expr{targetExpr, listExpr}}
}
