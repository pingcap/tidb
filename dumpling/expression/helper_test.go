package expression

import (
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
)

var _ = Suite(&testHelperSuite{})

type testHelperSuite struct {
}

func (s *testHelperSuite) TestContainAggFunc(c *C) {
	v := Value{}
	tbl := []struct {
		Expr   Expression
		Expect bool
	}{
		{Value{1}, false},
		{&BinaryOperation{L: v, R: v}, false},
		{&Call{F: "count", Args: []Expression{v}}, true},
		{&Call{F: "abs", Args: []Expression{v}}, false},
		{&IsNull{Expr: v}, false},
		{&PExpr{Expr: v}, false},
		{&PatternIn{Expr: v, List: []Expression{v}}, false},
		{&PatternLike{Expr: v, Pattern: v}, false},
		{&UnaryOperation{V: v}, false},
		{&ParamMarker{Expr: v}, false},
		{&FunctionCast{Expr: v}, false},
		{&FunctionConvert{Expr: v}, false},
		{&FunctionSubstring{StrExpr: v, Pos: v, Len: v}, false},
		{&FunctionCase{Value: v, WhenClauses: []*WhenClause{{Expr: v, Result: v}}, ElseClause: v}, false},
		{&WhenClause{Expr: v, Result: v}, false},
		{&IsTruth{Expr: v}, false},
		{&Between{Expr: v, Left: v, Right: v}, false},
		{&Row{Values: []Expression{v, v}}, false},
	}

	for _, t := range tbl {
		b := ContainAggregateFunc(t.Expr)
		c.Assert(b, Equals, t.Expect)
	}

	expr := &Call{
		F: "count",
		Args: []Expression{
			&Call{F: "count", Args: []Expression{v}},
		},
	}
	_, err := MentionedAggregateFuncs(expr)
	c.Assert(err, NotNil)
}

func (s *testHelperSuite) TestMentionedColumns(c *C) {
	v := Value{}
	tbl := []struct {
		Expr   Expression
		Expect int
	}{
		{Value{1}, 0},
		{&BinaryOperation{L: v, R: v}, 0},
		{&Ident{CIStr: model.NewCIStr("id")}, 1},
		{&Call{F: "count", Args: []Expression{v}}, 0},
		{&IsNull{Expr: v}, 0},
		{&PExpr{Expr: v}, 0},
		{&PatternIn{Expr: v, List: []Expression{v}}, 0},
		{&PatternLike{Expr: v, Pattern: v}, 0},
		{&UnaryOperation{V: v}, 0},
		{&ParamMarker{Expr: v}, 0},
		{&FunctionCast{Expr: v}, 0},
		{&FunctionConvert{Expr: v}, 0},
		{&FunctionSubstring{StrExpr: v, Pos: v, Len: v}, 0},
		{&FunctionCase{Value: v, WhenClauses: []*WhenClause{{Expr: v, Result: v}}, ElseClause: v}, 0},
		{&WhenClause{Expr: v, Result: v}, 0},
		{&IsTruth{Expr: v}, 0},
		{&Between{Expr: v, Left: v, Right: v}, 0},
		{&Row{Values: []Expression{v, v}}, 0},
	}

	for _, t := range tbl {
		ret := MentionedColumns(t.Expr)
		c.Assert(ret, HasLen, t.Expect)
	}
}

func NewTestRow(v1 interface{}, v2 interface{}, args ...interface{}) *Row {
	r := &Row{}
	a := make([]Expression, len(args))
	for i := range a {
		a[i] = Value{args[i]}
	}
	r.Values = append([]Expression{Value{v1}, Value{v2}}, a...)
	return r
}

func (s *testHelperSuite) TestBase(c *C) {
	e1 := Value{1}
	e2 := &PExpr{Expr: e1}

	e3 := Expr(e2)
	c.Assert(e1, DeepEquals, e3)

	tbl := []struct {
		Expr interface{}
		Ret  interface{}
	}{
		{Value{1}, 1},
		{int64(1), int64(1)},
		{&UnaryOperation{Op: opcode.Plus, V: Value{1}}, 1},
		{&UnaryOperation{Op: opcode.Not, V: Value{1}}, nil},
		{&UnaryOperation{Op: opcode.Plus, V: &Ident{CIStr: model.NewCIStr("id")}}, nil},
		{nil, nil},
	}

	for _, t := range tbl {
		v := FastEval(t.Expr)
		c.Assert(v, DeepEquals, t.Ret)
	}

	v, err := EvalBoolExpr(nil, Value{1}, nil)
	c.Assert(v, IsTrue)

	v, err = EvalBoolExpr(nil, Value{nil}, nil)
	c.Assert(v, IsFalse)

	v, err = EvalBoolExpr(nil, Value{errors.New("must error")}, nil)
	c.Assert(err, NotNil)

	v, err = EvalBoolExpr(nil, mockExpr{err: errors.New("must error")}, nil)
	c.Assert(err, NotNil)

	err = CheckOneColumn(nil, &Row{})
	c.Assert(err, NotNil)

	err = CheckOneColumn(nil, Value{nil})
	c.Assert(err, IsNil)

	//	err = CheckOneColumn(nil, newMockSubQuery([][]interface{}{}, []string{"id", "name"}))
	//	c.Assert(err, NotNil)

	columns := []struct {
		lhs     Expression
		rhs     Expression
		checker Checker
	}{
		{Value{nil}, Value{nil}, IsNil},
		{Value{nil}, &Row{}, NotNil},
		{NewTestRow(1, 2), NewTestRow(1, 2), IsNil},
		{NewTestRow(1, 2, 3), NewTestRow(1, 2), NotNil},
	}

	for _, t := range columns {
		err = hasSameColumnCount(nil, t.lhs, t.rhs)
		c.Assert(err, t.checker)

		err = hasSameColumnCount(nil, t.rhs, t.lhs)
		c.Assert(err, t.checker)
	}
}

func convert(v interface{}) interface{} {
	switch x := v.(type) {
	case int:
		return int64(x)
	}

	return v
}
