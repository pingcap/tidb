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
	fc := funcs[ast.Database]
	ctx := mock.NewContext()
	f, err := fc.getFunction(nil, ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)
	ctx.GetSessionVars().CurrentDB = "test"
	d, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")

	// Test case for schema().
	fc = funcs[ast.Schema]
	c.Assert(fc, NotNil)
	f, err = fc.getFunction(nil, ctx)
	c.Assert(err, IsNil)
	d, err = f.eval(types.MakeDatums())
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")
}

func (s *testEvaluatorSuite) TestFoundRows(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.FoundRows]
	f, err := fc.getFunction(nil, s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(0))
}

func (s *testEvaluatorSuite) TestUser(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = "root@localhost"

	fc := funcs[ast.User]
	f, err := fc.getFunction(nil, ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestCurrentUser(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = "root@localhost"

	fc := funcs[ast.CurrentUser]
	f, err := fc.getFunction(nil, ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestConnectionID(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.ConnectionID = uint64(1)

	fc := funcs[ast.ConnectionID]
	f, err := fc.getFunction(nil, ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(1))
}

func (s *testEvaluatorSuite) TestVersion(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Version]
	f, err := fc.getFunction(nil, s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, mysql.ServerVersion)
}
