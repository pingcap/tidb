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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testEvaluatorSuite) TestDatabase(c *C) {
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
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())

	// Test case for schema().
	fc = funcs[ast.Schema]
	c.Assert(fc, NotNil)
	f, err = fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err = evalBuiltinFunc(f, chunk.MutRowFromDatums(types.MakeDatums()).ToRow())
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestFoundRows(c *C) {
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
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}

	fc := funcs[ast.User]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestCurrentUser(c *C) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "localhost"}

	fc := funcs[ast.CurrentUser]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestCurrentRole(c *C) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.ActiveRoles = make([]*auth.RoleIdentity, 0, 10)
	sessionVars.ActiveRoles = append(sessionVars.ActiveRoles, &auth.RoleIdentity{Username: "r_1", Hostname: "%"})
	sessionVars.ActiveRoles = append(sessionVars.ActiveRoles, &auth.RoleIdentity{Username: "r_2", Hostname: "localhost"})

	fc := funcs[ast.CurrentRole]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "`r_1`@`%`,`r_2`@`localhost`")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestConnectionID(c *C) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.ConnectionID = uint64(1)

	fc := funcs[ast.ConnectionID]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(1))
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestVersion(c *C) {
	fc := funcs[ast.Version]
	f, err := fc.getFunction(s.ctx, nil)
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, mysql.ServerVersion)
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestBenchMark(c *C) {
	cases := []struct {
		LoopCount  int
		Expression interface{}
		Expected   int64
		IsNil      bool
	}{
		{-3, 1, 0, true},
		{0, 1, 0, false},
		{3, 1, 0, false},
		{3, 1.234, 0, false},
		{3, types.NewDecFromFloatForTest(1.234), 0, false},
		{3, "abc", 0, false},
		{3, types.CurrentTime(mysql.TypeDatetime), 0, false},
		{3, types.CurrentTime(mysql.TypeTimestamp), 0, false},
		{3, types.CurrentTime(mysql.TypeDuration), 0, false},
		{3, json.CreateBinary("[1]"), 0, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Benchmark, s.primitiveValsToConstants([]interface{}{
			t.LoopCount,
			t.Expression,
		})...)
		c.Assert(err, IsNil)

		d, err := f.Eval(chunk.Row{})
		c.Assert(err, IsNil)
		if t.IsNil {
			c.Assert(d.IsNull(), IsTrue)
		} else {
			c.Assert(d.GetInt64(), Equals, t.Expected)
		}

		// test clone
		b1 := f.Clone().(*ScalarFunction).Function.(*builtinBenchmarkSig)
		c.Assert(b1.constLoopCount, Equals, int64(t.LoopCount))
	}
}

func (s *testEvaluatorSuite) TestCharset(c *C) {
	fc := funcs[ast.Charset]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(f, IsNil)
	c.Assert(err, ErrorMatches, "*FUNCTION CHARSET does not exist")
}

func (s *testEvaluatorSuite) TestCoercibility(c *C) {
	fc := funcs[ast.Coercibility]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(f, NotNil)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestCollation(c *C) {
	fc := funcs[ast.Collation]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(f, NotNil)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestRowCount(c *C) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.StmtCtx.PrevAffectedRows = 10

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
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

// TestTiDBVersion for tidb_server().
func (s *testEvaluatorSuite) TestTiDBVersion(c *C) {
	f, err := newFunctionForTest(s.ctx, ast.TiDBVersion, s.primitiveValsToConstants([]interface{}{})...)
	c.Assert(err, IsNil)
	v, err := f.Eval(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, printer.GetTiDBInfo())
}

func (s *testEvaluatorSuite) TestLastInsertID(c *C) {
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
			s.ctx.GetSessionVars().StmtCtx.PrevLastInsertID = t.insertID
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

	_, err := funcs[ast.LastInsertId].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestFormatBytes(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{float64(0), "0 bytes"},
		{float64(2048), "2.00 KiB"},
		{float64(75295729), "71.81 MiB"},
		{float64(5287242702), "4.92 GiB"},
		{float64(5039757204245), "4.58 TiB"},
		{float64(890250274520475525), "790.70 PiB"},
		{float64(18446644073709551615), "16.00 EiB"},
		{float64(287952852482075252752429875), "2.50e+08 EiB"},
		{float64(-18446644073709551615), "-16.00 EiB"},
	}
	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.FormatBytes]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestFormatNanoTime(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{float64(0), "0 ns"},
		{float64(2000), "2.00 us"},
		{float64(898787877), "898.79 ms"},
		{float64(9999999991), "10.00 s"},
		{float64(898787877424), "14.98 min"},
		{float64(5827527520021), "1.62 h"},
		{float64(42566623663736353), "492.67 d"},
		{float64(4827524825702572425242552), "5.59e+10 d"},
		{float64(-9999999991), "-10.00 s"},
	}
	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.FormatNanoTime]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}
