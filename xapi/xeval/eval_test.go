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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testEvalSuite{})

type testEvalSuite struct{}

// TODO: add more tests.
func (s *testEvalSuite) TestEval(c *C) {
	colID := int64(1)
	row := make(map[int64]types.Datum)
	row[colID] = types.NewIntDatum(100)
	xevaluator := &Evaluator{Row: row}
	cases := []struct {
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
			datumExpr(types.Datum{}),
			types.Datum{},
		},
		{
			columnExpr(1),
			types.NewIntDatum(100),
		},
		// Comparison operations.
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(1), tipb.ExprType_LT),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(1), types.NewIntDatum(100), tipb.ExprType_LT),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.Datum{}, tipb.ExprType_LT),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(1), tipb.ExprType_LE),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(1), types.NewIntDatum(1), tipb.ExprType_LE),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.Datum{}, tipb.ExprType_LE),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(1), tipb.ExprType_EQ),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(100), tipb.ExprType_EQ),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.Datum{}, tipb.ExprType_EQ),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(100), tipb.ExprType_NE),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(1), tipb.ExprType_NE),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.Datum{}, tipb.ExprType_NE),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(1), types.NewIntDatum(100), tipb.ExprType_GE),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(100), tipb.ExprType_GE),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.Datum{}, tipb.ExprType_GE),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(100), tipb.ExprType_GT),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.NewIntDatum(1), tipb.ExprType_GT),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(100), types.Datum{}, tipb.ExprType_GT),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(1), types.Datum{}, tipb.ExprType_NullEQ),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.Datum{}, types.Datum{}, tipb.ExprType_NullEQ),
			types.NewIntDatum(1),
		},
		// Logic operation.
		{
			binaryExpr(types.NewIntDatum(0), types.NewIntDatum(1), tipb.ExprType_And),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(1), types.NewIntDatum(1), tipb.ExprType_And),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(0), types.Datum{}, tipb.ExprType_And),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(1), types.Datum{}, tipb.ExprType_And),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(0), types.NewIntDatum(0), tipb.ExprType_Or),
			types.NewIntDatum(0),
		},
		{
			binaryExpr(types.NewIntDatum(0), types.NewIntDatum(1), tipb.ExprType_Or),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(types.NewIntDatum(0), types.Datum{}, tipb.ExprType_Or),
			types.Datum{},
		},
		{
			binaryExpr(types.NewIntDatum(1), types.Datum{}, tipb.ExprType_Or),
			types.NewIntDatum(1),
		},
		{
			binaryExpr(
				binaryExpr(types.NewIntDatum(1), types.NewIntDatum(1), tipb.ExprType_EQ),
				binaryExpr(types.NewIntDatum(1), types.NewIntDatum(1), tipb.ExprType_EQ),
				tipb.ExprType_And),
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
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, ca.result.Kind())
		cmp, err := result.CompareDatum(ca.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func binaryExpr(left, right interface{}, tp tipb.ExprType) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tp.Enum()
	expr.Children = make([]*tipb.Expr, 2)
	switch x := left.(type) {
	case types.Datum:
		expr.Children[0] = datumExpr(x)
	case *tipb.Expr:
		expr.Children[0] = x
	}
	switch x := right.(type) {
	case types.Datum:
		expr.Children[1] = datumExpr(x)
	case *tipb.Expr:
		expr.Children[1] = x
	}
	return expr
}

func datumExpr(d types.Datum) *tipb.Expr {
	expr := new(tipb.Expr)
	switch d.Kind() {
	case types.KindInt64:
		expr.Tp = tipb.ExprType_Int64.Enum()
		expr.Val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		expr.Tp = tipb.ExprType_Uint64.Enum()
		expr.Val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		expr.Tp = tipb.ExprType_String.Enum()
		expr.Val = d.GetBytes()
	case types.KindBytes:
		expr.Tp = tipb.ExprType_Bytes.Enum()
		expr.Val = d.GetBytes()
	case types.KindFloat32:
		expr.Tp = tipb.ExprType_Float32.Enum()
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		expr.Tp = tipb.ExprType_Float64.Enum()
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	default:
		expr.Tp = tipb.ExprType_Null.Enum()
	}
	return expr
}

func columnExpr(columnID int64) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tipb.ExprType_ColumnRef.Enum()
	expr.Val = codec.EncodeInt(nil, columnID)
	return expr
}

func likeExpr(target, pattern string) *tipb.Expr {
	targetExpr := datumExpr(types.NewStringDatum(target))
	patternExpr := datumExpr(types.NewStringDatum(pattern))
	return &tipb.Expr{Tp: tipb.ExprType_Like.Enum(), Children: []*tipb.Expr{targetExpr, patternExpr}}
}

func notExpr(value interface{}) *tipb.Expr {
	expr := new(tipb.Expr)
	expr.Tp = tipb.ExprType_Not.Enum()
	switch x := value.(type) {
	case types.Datum:
		expr.Children = []*tipb.Expr{datumExpr(x)}
	case *tipb.Expr:
		expr.Children = []*tipb.Expr{x}
	}
	return expr
}

func (s *testEvalSuite) TestLike(c *C) {
	tbl := []struct {
		pattern string
		input   string
		escape  byte
		match   bool
	}{
		{"", "a", '\\', false},
		{"a", "a", '\\', true},
		{"a", "b", '\\', false},
		{"aA", "aA", '\\', true},
		{"_", "a", '\\', true},
		{"_", "ab", '\\', false},
		{"__", "b", '\\', false},
		{"_ab", "AAB", '\\', true},
		{"%", "abcd", '\\', true},
		{"%", "", '\\', true},
		{"%a", "AAA", '\\', true},
		{"%b", "AAA", '\\', false},
		{"b%", "BBB", '\\', true},
		{"%a%", "BBB", '\\', false},
		{"%a%", "BAB", '\\', true},
		{"a%", "BBB", '\\', false},
		{`\%a`, `%a`, '\\', true},
		{`\%a`, `aa`, '\\', false},
		{`\_a`, `_a`, '\\', true},
		{`\_a`, `aa`, '\\', false},
		{`\\_a`, `\xa`, '\\', true},
		{`\a\b`, `\a\b`, '\\', true},
		{"%%_", `abc`, '\\', true},
		{`+_a`, `_a`, '+', true},
		{`+%a`, `%a`, '+', true},
		{`\%a`, `%a`, '+', false},
		{`++a`, `+a`, '+', true},
		{`++_a`, `+xa`, '+', true},
	}
	for _, v := range tbl {
		patChars, patTypes := compilePattern(v.pattern, v.escape)
		match := matchPattern(v.input, patChars, patTypes)
		c.Assert(match, Equals, v.match, Commentf("%v", v))
	}
	cases := []struct {
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
			expr:   likeExpr("aA", "Aa"),
			result: 1,
		},
		{
			expr:   likeExpr("aAb", "Aa%"),
			result: 1,
		},
		{
			expr:   likeExpr("aAb", "Aa_"),
			result: 1,
		},
	}
	ev := &Evaluator{}
	for _, ca := range cases {
		res, err := ev.Eval(ca.expr)
		c.Check(err, IsNil)
		c.Check(res.GetInt64(), Equals, ca.result)
	}
}
