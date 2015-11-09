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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testVariableSuite{})

type testVariableSuite struct {
	ctx context.Context
}

func (s *testVariableSuite) SetUpSuite(c *C) {
	nc := mock.NewContext()
	s.ctx = nc
	variable.BindSessionVars(s.ctx)
	variable.BindGlobalVarAccessor(s.ctx, nc)
}

func (s *testVariableSuite) TestVariable(c *C) {
	name := "timestamp"

	e := &Variable{
		Name:     name,
		IsGlobal: false,
		IsSystem: true,
	}

	c.Assert(e.IsStatic(), IsFalse)
	c.Assert(e.String(), Equals, "@@"+name)

	v, err := e.Eval(s.ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")

	sessionVars := variable.GetSessionVars(s.ctx)
	sessionVars.Systems[name] = "1234"

	v, err = e.Eval(s.ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "1234")

	ec := e.Clone()

	e2, ok := ec.(*Variable)
	c.Assert(ok, IsTrue)

	e2.IsGlobal = true
	c.Assert(e2.String(), Equals, "@@GLOBAL."+name)

	e2.IsSystem = false
	c.Assert(e2.String(), Equals, "@"+name)

	v, err = e2.Eval(s.ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	sessionVars.Users[name] = "5678"

	v, err = e2.Eval(s.ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "5678")

	// check error.
	e2.Name = "xxx"
	e2.IsSystem = true

	_, err = e2.Eval(s.ctx, nil)
	c.Assert(err, NotNil)
}
