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

package rsets

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
)

var _ = Suite(&testHelperSuite{})

type testHelperSuite struct {
	fields []*field.Field
}

func (s *testHelperSuite) SetUpSuite(c *C) {
	fldx := &field.Field{Expr: &expressions.Ident{CIStr: model.NewCIStr("name")}, Name: "a"}
	expr, err := expressions.NewCall("count", []expression.Expression{expressions.Value{Val: 1}}, false)
	c.Assert(err, IsNil)
	fldy := &field.Field{Expr: expr}

	s.fields = []*field.Field{fldx, fldy}
}

func (s *testHelperSuite) TestGetAggFields(c *C) {
	aggFields := GetAggFields(s.fields)
	c.Assert(aggFields, HasLen, 1)
}

func (s *testHelperSuite) TestHasAggFields(c *C) {
	ok := HasAggFields(s.fields)
	c.Assert(ok, IsTrue)
}
