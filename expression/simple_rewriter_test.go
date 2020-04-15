// Copyright 2019 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/mock"
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

	exprs, err = ParseSimpleExprsWithSchema(ctx, "1 not between 5 and 10", sch)
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

	exprs, err = ParseSimpleExprsWithSchema(ctx, "(1, 2) not in ((2, 2))", sch)
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

	_, err = ParseSimpleExprsWithSchema(ctx, "abs(?)", sch)
	c.Assert(err, IsNil)

	exprs, err = ParseSimpleExprsWithSchema(ctx, "cast('1' as unsigned)", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "trim(leading 'z' from 'zxyz')", sch)
	c.Assert(err, IsNil)
	str, _, _ := exprs[0].EvalString(ctx, chunk.Row{})
	c.Assert(str, Equals, "xyz")

	exprs, err = ParseSimpleExprsWithSchema(ctx, "get_format(datetime, 'ISO')", sch)
	c.Assert(err, IsNil)
	str, _, _ = exprs[0].EvalString(ctx, chunk.Row{})
	c.Assert(str, Equals, "%Y-%m-%d %H:%i:%s")

	exprs, err = ParseSimpleExprsWithSchema(ctx, "extract(day_minute from '2184-07-03 18:42:18.895059')", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(31842))

	exprs, err = ParseSimpleExprsWithSchema(ctx, "unix_timestamp('2008-05-01 00:00:00')", sch)
	c.Assert(err, IsNil)
	num, _, _ = exprs[0].EvalInt(ctx, chunk.Row{})
	c.Assert(num, Equals, int64(1209571200))
}
