package expressions

import (
	"errors"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var _ = Suite(&testHelperSuite{})

type testHelperSuite struct {
}

func (s *testHelperSuite) TestContainAggFunc(c *C) {
	v := Value{}
	tbl := []struct {
		Expr   expression.Expression
		Expect bool
	}{
		{Value{1}, false},
		{&BinaryOperation{L: v, R: v}, false},
		{&Call{F: "count", Args: []expression.Expression{v}}, true},
		{&Call{F: "abs", Args: []expression.Expression{v}}, false},
		{&IsNull{Expr: v}, false},
		{&PExpr{Expr: v}, false},
		{&PatternIn{Expr: v, List: []expression.Expression{v}}, false},
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
		{&Row{Values: []expression.Expression{v, v}}, false},
	}

	for _, t := range tbl {
		b := ContainAggregateFunc(t.Expr)
		c.Assert(b, Equals, t.Expect)
	}
}

func (s *testHelperSuite) TestMentionedColumns(c *C) {
	v := Value{}
	tbl := []struct {
		Expr   expression.Expression
		Expect int
	}{
		{Value{1}, 0},
		{&BinaryOperation{L: v, R: v}, 0},
		{&Ident{model.NewCIStr("id")}, 1},
		{&Call{F: "count", Args: []expression.Expression{v}}, 0},
		{&IsNull{Expr: v}, 0},
		{&PExpr{Expr: v}, 0},
		{&PatternIn{Expr: v, List: []expression.Expression{v}}, 0},
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
		{&Row{Values: []expression.Expression{v, v}}, 0},
	}

	for _, t := range tbl {
		ret := MentionedColumns(t.Expr)
		c.Assert(ret, HasLen, t.Expect)
	}
}

func newTestRow(v1 interface{}, v2 interface{}, args ...interface{}) *Row {
	r := &Row{}
	a := make([]expression.Expression, len(args))
	for i := range a {
		a[i] = Value{args[i]}
	}
	r.Values = append([]expression.Expression{Value{v1}, Value{v2}}, a...)
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
		{&UnaryOperation{Op: opcode.Plus, V: &Ident{model.NewCIStr("id")}}, nil},
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

	err = CheckOneColumn(nil, newMockSubQuery([][]interface{}{}, []string{"id", "name"}))
	c.Assert(err, NotNil)

	columns := []struct {
		lhs     expression.Expression
		rhs     expression.Expression
		checker Checker
	}{
		{Value{nil}, Value{nil}, IsNil},
		{Value{nil}, &Row{}, NotNil},
		{newTestRow(1, 2), newTestRow(1, 2), IsNil},
		{newTestRow(1, 2, 3), newTestRow(1, 2), NotNil},
	}

	for _, t := range columns {
		err = hasSameColumnCount(nil, t.lhs, t.rhs)
		c.Assert(err, t.checker)

		err = hasSameColumnCount(nil, t.rhs, t.lhs)
		c.Assert(err, t.checker)
	}
}

func (s *testHelperSuite) TestGetTimeValue(c *C) {
	v, err := GetTimeValue(nil, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok := v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	ctx := newMockCtx()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)

	sessionVars.Systems["timestamp"] = ""
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	sessionVars.Systems["timestamp"] = "0"
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	delete(sessionVars.Systems, "timestamp")
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	sessionVars.Systems["timestamp"] = "1234"

	tbl := []struct {
		Expr interface{}
		Ret  interface{}
	}{
		{"2012-12-12 00:00:00", "2012-12-12 00:00:00"},
		{CurrentTimestamp, time.Unix(1234, 0).Format(mysql.TimeFormat)},
		{ZeroTimestamp, "0000-00-00 00:00:00"},
		{Value{"2012-12-12 00:00:00"}, "2012-12-12 00:00:00"},
		{Value{int64(0)}, "0000-00-00 00:00:00"},
		{Value{}, nil},
		{CurrentTimeExpr, CurrentTimestamp},
		{NewUnaryOperation(opcode.Minus, Value{int64(0)}), "0000-00-00 00:00:00"},
		{mockExpr{}, nil},
	}

	for _, t := range tbl {
		v, err := GetTimeValue(ctx, t.Expr, mysql.TypeTimestamp, mysql.MinFsp)
		c.Assert(err, IsNil)

		switch x := v.(type) {
		case mysql.Time:
			c.Assert(x.String(), DeepEquals, t.Ret)
		default:
			c.Assert(x, DeepEquals, t.Ret)
		}
	}

	errTbl := []struct {
		Expr interface{}
	}{
		{"2012-13-12 00:00:00"},
		{Value{"2012-13-12 00:00:00"}},
		{Value{0}},
		{Value{int64(1)}},
		{&Ident{model.NewCIStr("xxx")}},
		{NewUnaryOperation(opcode.Minus, Value{int64(1)})},
	}

	for _, t := range errTbl {
		_, err := GetTimeValue(ctx, t.Expr, mysql.TypeTimestamp, mysql.MinFsp)
		c.Assert(err, NotNil)
	}
}

func (s *testHelperSuite) TestIsCurrentTimeExpr(c *C) {
	v := IsCurrentTimeExpr(mockExpr{})
	c.Assert(v, IsFalse)

	v = IsCurrentTimeExpr(CurrentTimeExpr)
	c.Assert(v, IsTrue)
}

func convert(v interface{}) interface{} {
	switch x := v.(type) {
	case nil:
		return nil
	case int:
		return int64(x)
	}

	return v
}
