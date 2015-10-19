// Copyright 2015 PingCAP, Inc.
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
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testCastSuite{})

type testCastSuite struct {
}

func (s *testCastSuite) TestCast(c *C) {
	f := types.NewFieldType(mysql.TypeLonglong)

	expr := &FunctionCast{
		Expr: Value{1},
		Tp:   f,
	}

	f.Flag |= mysql.UnsignedFlag
	c.Assert(len(expr.String()), Greater, 0)
	f.Flag = 0
	c.Assert(len(expr.String()), Greater, 0)
	f.Tp = mysql.TypeDatetime
	c.Assert(len(expr.String()), Greater, 0)

	f.Tp = mysql.TypeLonglong
	c.Assert(expr.Clone(), NotNil)

	c.Assert(expr.IsStatic(), IsTrue)

	v, err := expr.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v.(*types.DataItem).Data, Equals, int64(1))

	f.Flag |= mysql.UnsignedFlag
	v, err = expr.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v.(*types.DataItem).Data, Equals, uint64(1))

	f.Tp = mysql.TypeString
	f.Charset = charset.CharsetBin
	v, err = expr.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v.(*types.DataItem).Data, DeepEquals, []byte("1"))

	f.Tp = mysql.TypeString
	f.Charset = "utf8"
	v, err = expr.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v.(*types.DataItem).Data, DeepEquals, "1")

	expr.Expr = Value{nil}
	v, err = expr.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v.(*types.DataItem).Data, Equals, nil)

	expr.Expr = mockExpr{err: errors.New("must error")}
	c.Assert(expr.Clone(), NotNil)

	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)

	// For String()
	f = types.NewFieldType(mysql.TypeLonglong)
	expr = &FunctionCast{
		Expr: Value{1},
		Tp:   f,
	}
	str := expr.String()
	c.Assert(str, Equals, "CAST(1 AS SIGNED)")
	expr.FunctionType = ConvertFunction
	str = expr.String()
	c.Assert(str, Equals, "CONVERT(1, SIGNED)")
	expr.FunctionType = BinaryOperator
	str = expr.String()
	c.Assert(str, Equals, "BINARY 1")
}
