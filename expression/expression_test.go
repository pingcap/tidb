// Copyright 2016 PingCAP, Inc.
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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testExpressionSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testExpressionSuite struct{}

func getColIndex(exprs []Expression, schema Schema, col *ast.ColumnName) (int, error) {
	newCol, err := schema.FindSelectFieldColumn(col, exprs)
	if err != nil {
		return -1, err
	}
	return schema.GetIndex(newCol), nil
}

func (s *testExpressionSuite) TestExpression(c *C) {
	defer testleak.AfterTest(c)()

	var schema Schema
	for i := 0; i < 3; i++ {
		schema = append(schema, &Column{ColName: model.NewCIStr("t"), FromID: "mock", Position: i})
	}
	tc1 := &Column{FromID: "t", ColName: model.NewCIStr("c1")}
	kc1 := &Column{FromID: "k", ColName: model.NewCIStr("c1")}
	tc2 := &Column{FromID: "t", ColName: model.NewCIStr("c2")}
	con := &Constant{Value: types.NewDatum(10)}
	col := &ast.ColumnName{Name: model.NewCIStr("t")}
	// t.c1 as t, t.c2 as t, 10 as t => error
	index, err := getColIndex([]Expression{tc1, tc2, con}, schema, col)
	c.Check(err, NotNil)
	// t.c1 as t, 10 as t, t.c2 as t => 10
	index, err = getColIndex([]Expression{tc1, con, tc2}, schema, col)
	c.Assert(index, Equals, 1)
	// t.c1 as t, t.c1 as t, 10 as t => 10
	index, err = getColIndex([]Expression{tc1, tc1, con}, schema, col)
	c.Assert(index, Equals, 2)
	// t.c1 as t, k.c1 as t, 10 as t => error
	index, err = getColIndex([]Expression{tc1, kc1, con}, schema, col)
	c.Check(err, NotNil)
}
