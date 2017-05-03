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
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) TestDistinct(c *check.C) {
	defer testleak.AfterTest(c)()
	dc := createDistinctChecker()
	tests := []struct {
		vals   []interface{}
		expect bool
	}{
		{[]interface{}{1, 1}, true},
		{[]interface{}{1, 1}, false},
		{[]interface{}{1, 2}, true},
		{[]interface{}{1, 2}, false},
		{[]interface{}{1, nil}, true},
		{[]interface{}{1, nil}, false},
	}
	for _, tt := range tests {
		d, err := dc.Check(types.MakeDatums(tt.vals...))
		c.Assert(err, check.IsNil)
		c.Assert(d, check.Equals, tt.expect)
	}
}

func (s *testUtilSuite) TestSubstituteCorCol2Constant(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	corCol1 := &CorrelatedColumn{Data: &One.Value}
	corCol2 := &CorrelatedColumn{Data: &One.Value}
	cast := NewCastFunc(types.NewFieldType(mysql.TypeLonglong), corCol1, ctx)
	plus := newFunction(ast.Plus, cast, corCol2)
	plus2 := newFunction(ast.Plus, plus, One)
	ans := &Constant{Value: types.NewIntDatum(3)}
	ret, err := SubstituteCorCol2Constant(plus2)
	c.Assert(err, check.IsNil)
	c.Assert(ret.Equal(ans, ctx), check.IsTrue)
	col1 := &Column{Index: 1}
	newCol, err := SubstituteCorCol2Constant(col1)
	c.Assert(err, check.IsNil)
	c.Assert(newCol.Equal(col1, ctx), check.IsTrue)
}
