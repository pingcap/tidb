package aggregation

import (
	"github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

var _ = check.Suite(&testBaseFuncSuite{})

type testBaseFuncSuite struct {
	ctx sessionctx.Context
}

func (s *testBaseFuncSuite) SetUpSuite(c *check.C) {
	s.ctx = mock.NewContext()
}

func (s *testBaseFuncSuite) TestClone(c *check.C) {
	col := &expression.Column{
		UniqueID: 0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	desc, err := newBaseFuncDesc(s.ctx, ast.AggFuncFirstRow, []expression.Expression{col})
	c.Assert(err, check.IsNil)
	cloned := desc.clone()
	c.Assert(desc.equal(s.ctx, cloned), check.IsTrue)

	col1 := &expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	}
	cloned.Args[0] = col1

	c.Assert(desc.Args[0], check.Equals, col)
	c.Assert(desc.equal(s.ctx, cloned), check.IsFalse)
}
