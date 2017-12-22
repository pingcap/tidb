package executor

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	goctx "golang.org/x/net/context"
)

var _ = Suite(&pkgTestSuite{})

type pkgTestSuite struct {
}

type MockExec struct {
	baseExecutor

	fields    []*ast.ResultField
	Rows      []Row
	curRowIdx int
}

func (m *MockExec) Next(goCtx goctx.Context) (Row, error) {
	if m.curRowIdx >= len(m.Rows) {
		return nil, nil
	}
	r := m.Rows[m.curRowIdx]
	m.curRowIdx++
	if len(m.fields) > 0 {
		for i, d := range r {
			m.fields[i].Expr.SetValue(d.GetValue())
		}
	}
	return r, nil
}

func (m *MockExec) Close() error {
	m.curRowIdx = 0
	return nil
}

func (m *MockExec) Open(goCtx goctx.Context) error {
	m.curRowIdx = 0
	return nil
}

func (s *pkgTestSuite) TestNestedLoopApply(c *C) {
	goCtx := goctx.Background()
	ctx := mock.NewContext()
	bigExec := &MockExec{
		baseExecutor: newBaseExecutor(nil, ctx),
		Rows: []Row{
			types.MakeDatums(1),
			types.MakeDatums(2),
			types.MakeDatums(3),
			types.MakeDatums(4),
			types.MakeDatums(5),
			types.MakeDatums(6),
		}}
	smallExec := &MockExec{Rows: []Row{
		types.MakeDatums(1),
		types.MakeDatums(2),
		types.MakeDatums(3),
		types.MakeDatums(4),
		types.MakeDatums(5),
		types.MakeDatums(6),
	}}
	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	col1 := &expression.Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLong)}
	con := &expression.Constant{Value: types.NewDatum(6), RetType: types.NewFieldType(mysql.TypeLong)}
	bigFilter := expression.NewFunctionInternal(ctx, ast.LT, types.NewFieldType(mysql.TypeTiny), col0, con)
	smallFilter := bigFilter.Clone()
	otherFilter := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col0, col1)
	generator := newJoinResultGenerator(ctx, plan.InnerJoin, false,
		make([]types.Datum, smallExec.Schema().Len()), []expression.Expression{otherFilter}, nil, nil)
	join := &NestedLoopApplyExec{
		baseExecutor:    newBaseExecutor(nil, ctx),
		BigExec:         bigExec,
		SmallExec:       smallExec,
		BigFilter:       []expression.Expression{bigFilter},
		SmallFilter:     []expression.Expression{smallFilter},
		resultGenerator: generator,
	}
	row, err := join.Next(goCtx)
	c.Check(err, IsNil)
	c.Check(row, NotNil)
	c.Check(fmt.Sprintf("%v %v", row[0].GetValue(), row[1].GetValue()), Equals, "1 1")
	row, err = join.Next(goCtx)
	c.Check(err, IsNil)
	c.Check(row, NotNil)
	c.Check(fmt.Sprintf("%v %v", row[0].GetValue(), row[1].GetValue()), Equals, "2 2")
	row, err = join.Next(goCtx)
	c.Check(err, IsNil)
	c.Check(row, NotNil)
	c.Check(fmt.Sprintf("%v %v", row[0].GetValue(), row[1].GetValue()), Equals, "3 3")
	row, err = join.Next(goCtx)
	c.Check(err, IsNil)
	c.Check(row, NotNil)
	c.Check(fmt.Sprintf("%v %v", row[0].GetValue(), row[1].GetValue()), Equals, "4 4")
	row, err = join.Next(goCtx)
	c.Check(err, IsNil)
	c.Check(row, NotNil)
	c.Check(fmt.Sprintf("%v %v", row[0].GetValue(), row[1].GetValue()), Equals, "5 5")
	row, err = join.Next(goCtx)
	c.Check(err, IsNil)
	c.Check(row, IsNil)
}
