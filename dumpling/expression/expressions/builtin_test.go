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

package expressions

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var _ = Suite(&testBuiltinSuite{})

type testBuiltinSuite struct {
}

func (s *testBuiltinSuite) TestBadNArgs(c *C) {
	err := badNArgs(1, "", nil)
	c.Assert(err, NotNil)

	err = badNArgs(0, "", []interface{}{1})
	c.Assert(err, NotNil)
}

func (s *testBuiltinSuite) TestCoalesce(c *C) {
	args := []interface{}{1, nil}
	v, err := builtinCoalesce(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, 1)

	args = []interface{}{nil, nil}
	v, err = builtinCoalesce(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
}

func (s *testBuiltinSuite) TestDatabase(c *C) {
	ctx := newMockCtx()
	m := map[interface{}]interface{}{}
	v, err := builtinDatabase(nil, m)
	c.Assert(err, NotNil)

	m[ExprEvalArgCtx] = ctx
	v, err = builtinDatabase(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	db.BindCurrentSchema(ctx, "test")
	v, err = builtinDatabase(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "test")
}

func (s *testBuiltinSuite) TestFoundRows(c *C) {
	ctx := newMockCtx()
	m := map[interface{}]interface{}{}
	v, err := builtinFoundRows(nil, m)
	c.Assert(err, NotNil)

	variable.BindSessionVars(ctx)

	m[ExprEvalArgCtx] = ctx
	v, err = builtinFoundRows(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(0))
}
