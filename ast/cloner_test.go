package ast

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/opcode"
)

var _ = Suite(&testClonerSuite{})

type testClonerSuite struct {
}

func (ts *testClonerSuite) TestCloner(c *C) {
	cloner := &Cloner{}

	a := &UnaryOperationExpr{
		Op: opcode.Not,
		V:  &UnaryOperationExpr{V: NewValueExpr(true)},
	}

	b, ok := a.Accept(cloner)
	c.Assert(ok, IsTrue)
	a1 := a.V
	b1 := b.(*UnaryOperationExpr).V
	c.Assert(a1, Not(Equals), b1)
	a2 := a1.(*UnaryOperationExpr).V
	b2 := b1.(*UnaryOperationExpr).V
	c.Assert(a2, Not(Equals), b2)
	a3 := a2.(*ValueExpr)
	b3 := b2.(*ValueExpr)
	c.Assert(a3, Not(Equals), b3)
	c.Assert(a3.GetValue(), Equals, int64(1))
	c.Assert(b3.GetValue(), Equals, int64(1))
}

func (ts *testClonerSuite) TestClonePrepareStmt(c *C) {
	cloner := &Cloner{}

	a := &PrepareStmt{
		stmtNode: stmtNode{
			node: node{
				text: "prepare stmt from 'select 1+?'",
			},
		},
		Name:    "stmt",
		SQLText: "select 1+?",
		SQLVar:  (*VariableExpr)(nil),
	}

	b, ok := a.Accept(cloner)
	c.Assert(ok, IsTrue)
	c.Check(a, DeepEquals, b)
}
