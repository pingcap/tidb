package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testEvaluatorSuite) TestSimpleRewriter(c *C) {
	ctx := mock.NewContext()
	sch := NewSchema()
	_, err := ParseSimpleExprsWithSchema(ctx, "NULLIF(1, 2, 3)", sch)
	c.Assert(err, NotNil)

	exprs, err := ParseSimpleExprsWithSchema(ctx, "NULLIF(1, 2)", sch)
	c.Assert(err, IsNil)
	num, _, _ := exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "NULLIF(1, 1)", sch)
	c.Assert(err, IsNil)
	_, isNull, _ := exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(isNull, IsTrue)

	exprs, err = ParseSimpleExprsWithSchema(ctx, "+1", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "-1", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(-1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "'abc' like '%b%'", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "'abcdef' REGEXP '.*cd.*'", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "(1, 1) = (1, 1)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "5 between 1 and 10", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "1 is true", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "0 is not true", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "1 in (1, 2, 3)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	_, err = ParseSimpleExprsWithSchema(ctx, "1 in ()", sch)
	c.Assert(err, NotNil)

	exprs, err = ParseSimpleExprsWithSchema(ctx, "1 in (1, 1, 1, 1)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "(1, 2) in ((1, 2), (2, 2))", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "1 < 2", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "1 <= 2", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "2 >= 1", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "2 > 1", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "(1, 2) < (1, 3)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "(1, 2) <= (1, 3)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "(1, 3) > (1, 2)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "(1, 3) >= (1, 2)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))
}
