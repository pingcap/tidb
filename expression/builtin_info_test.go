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
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testEvaluatorSuite) TestDatabase(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Database]
	ctx := mock.NewContext()
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)
	ctx.GetSessionVars().CurrentDB = "test"
	d, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")

	// Test case for schema().
	fc = funcs[ast.Schema]
	c.Assert(fc, NotNil)
	f, err = fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err = evalBuiltinFunc(f, chunk.MutRowFromDatums(types.MakeDatums()).ToRow())
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")
}

func (s *testEvaluatorSuite) TestFoundRows(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.LastFoundRows = 2

	fc := funcs[ast.FoundRows]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(2))
}

func (s *testEvaluatorSuite) TestUser(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}

	fc := funcs[ast.User]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestCurrentUser(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}

	fc := funcs[ast.CurrentUser]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestConnectionID(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.ConnectionID = uint64(1)

	fc := funcs[ast.ConnectionID]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(1))
}

func (s *testEvaluatorSuite) TestVersion(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Version]
	f, err := fc.getFunction(s.ctx, nil)
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, mysql.ServerVersion)
}

func (s *testEvaluatorSuite) TestBenchMark(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Benchmark]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil, nil)))
	c.Assert(f, IsNil)
	c.Assert(err, ErrorMatches, "*FUNCTION BENCHMARK does not exist")
}

func (s *testEvaluatorSuite) TestCharset(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Charset]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(f, IsNil)
	c.Assert(err, ErrorMatches, "*FUNCTION CHARSET does not exist")
}

func (s *testEvaluatorSuite) TestCoercibility(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Coercibility]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(f, IsNil)
	c.Assert(err, ErrorMatches, "*FUNCTION COERCIBILITY does not exist")
}

func (s *testEvaluatorSuite) TestCollation(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Collation]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(f, IsNil)
	c.Assert(err, ErrorMatches, "*FUNCTION COLLATION does not exist")
}

func (s *testEvaluatorSuite) TestRowCount(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.PrevAffectedRows = 10

	f, err := funcs[ast.RowCount].getFunction(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(f, NotNil)
	sig, ok := f.(*builtinRowCountSig)
	c.Assert(ok, IsTrue)
	c.Assert(sig, NotNil)
	intResult, isNull, err := sig.evalInt(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(intResult, Equals, int64(10))
}

// Test case for tidb_server().
func (s *testEvaluatorSuite) TestTiDBVersion(c *C) {
	defer testleak.AfterTest(c)()
	f, err := newFunctionForTest(s.ctx, ast.TiDBVersion, s.primitiveValsToConstants([]interface{}{})...)
	c.Assert(err, IsNil)
	v, err := f.Eval(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, printer.GetTiDBInfo())
}

func (s *testEvaluatorSuite) TestLastInsertID(c *C) {
	defer testleak.AfterTest(c)()

	maxUint64 := uint64(math.MaxUint64)
	cases := []struct {
		insertID uint64
		args     interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{0, 1, 1, false, false},
		{0, 1.1, 1, false, false},
		{0, maxUint64, maxUint64, false, false},
		{0, -1, math.MaxUint64, false, false},
		{1, nil, 1, false, false},
		{math.MaxUint64, nil, math.MaxUint64, false, false},
	}

	for _, t := range cases {
		var (
			f   Expression
			err error
		)
		if t.insertID > 0 {
			s.ctx.GetSessionVars().PrevLastInsertID = t.insertID
		}

		if t.args != nil {
			f, err = newFunctionForTest(s.ctx, ast.LastInsertId, s.primitiveValsToConstants([]interface{}{t.args})...)
		} else {
			f, err = newFunctionForTest(s.ctx, ast.LastInsertId)
		}
		tp := f.GetType()
		c.Assert(err, IsNil)
		c.Assert(tp.Tp, Equals, mysql.TypeLonglong)
		c.Assert(tp.Charset, Equals, charset.CharsetBin)
		c.Assert(tp.Collate, Equals, charset.CollationBin)
		c.Assert(tp.Flag&mysql.BinaryFlag, Equals, uint(mysql.BinaryFlag))
		c.Assert(tp.Flen, Equals, mysql.MaxIntWidth)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.LastInsertId].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}
