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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestDatabase(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	fc := Funcs[ast.Database]
	f, _ := fc.getFunction(nil, ctx)
	d, err := f.eval(types.MakeDatums())
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)
	ctx.GetSessionVars().CurrentDB = "test"
	d, err = f.eval(types.MakeDatums())
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")

	// Test case for schema().
	fc = Funcs[ast.Schema]
	f, _ = fc.getFunction(nil, ctx)
	c.Assert(f, NotNil)
	d, err = f.eval(types.MakeDatums())
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")
}

func (s *testEvaluatorSuite) TestFoundRows(c *C) {
	defer testleak.AfterTest(c)()
	d, err := (&builtinFoundRows{newBaseBuiltinFunc(nil, true, s.ctx)}).eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(0))
}

func (s *testEvaluatorSuite) TestUser(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = "root@localhost"

	d, err := (&builtinUser{newBaseBuiltinFunc(nil, true, ctx)}).eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestCurrentUser(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = "root@localhost"

	d, err := (&builtinCurrentUser{newBaseBuiltinFunc(nil, true, ctx)}).eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestConnectionID(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.ConnectionID = uint64(1)

	d, err := (&builtinConnectionID{newBaseBuiltinFunc(nil, true, ctx)}).eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(1))
}

func (s *testEvaluatorSuite) TestVersion(c *C) {
	defer testleak.AfterTest(c)()
	v, err := (&builtinVersion{newBaseBuiltinFunc(nil, true, s.ctx)}).eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, mysql.ServerVersion)
}
