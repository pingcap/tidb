package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
)

var _ = Suite(&testVisitorSuite{})

type testVisitorSuite struct {
}

func (s *testVisitorSuite) TestIdentEval(c *C) {
	exp := NewBinaryOperation(opcode.Plus, &Ident{CIStr: model.NewCIStr("a")}, Value{Val: 1})
	iev := NewIdentEvalVisitor()
	iev.Set("a", 3)
	exp.Accept(iev)
	v, err := exp.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(4))
}
