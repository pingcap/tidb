// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
)

func (s *testUnitTestSuit) rewriteSimpleExpr(str string, schema *expression.Schema) ([]expression.Expression, error) {
	if str == "" {
		return nil, nil
	}
	filters, err := expression.ParseSimpleExprsWithSchema(s.ctx, str, schema)
	if err != nil {
		return nil, err
	}
	if sf, ok := filters[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
		filters = expression.FlattenCNFConditions(sf)
	}
	return filters, nil
}

func (s *testUnitTestSuit) TestIndexJoinAnalyzeLookUpFilters(c *C) {
	s.ctx.GetSessionVars().PlanID = -1
	joinNode := LogicalJoin{}.Init(s.ctx, 0)
	dataSourceNode := DataSource{}.Init(s.ctx, 0)
	dsSchema := expression.NewSchema()
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("a"),
		TblName:  model.NewCIStr("t"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("b"),
		TblName:  model.NewCIStr("t"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("c"),
		TblName:  model.NewCIStr("t"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("d"),
		TblName:  model.NewCIStr("t"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dataSourceNode.schema = dsSchema
	outerChildSchema := expression.NewSchema()
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("e"),
		TblName:  model.NewCIStr("t1"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("f"),
		TblName:  model.NewCIStr("t1"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("g"),
		TblName:  model.NewCIStr("t1"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		ColName:  model.NewCIStr("h"),
		TblName:  model.NewCIStr("t1"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	joinNode.SetSchema(expression.MergeSchema(dsSchema, outerChildSchema))
	path := &accessPath{
		idxCols:    append(make([]*expression.Column, 0, 4), dsSchema.Columns...),
		idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength, 2, types.UnspecifiedLength},
	}

	tests := []struct {
		innerKeys       []*expression.Column
		pushedDownConds string
		otherConds      string
		ranges          string
		idxOff2KeyOff   string
		accesses        string
		remained        string
		compareFilters  string
	}{
		// Join key not continuous and no pushed filter to match.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[0], dsSchema.Columns[2]},
			pushedDownConds: "",
			otherConds:      "",
			ranges:          "[[NULL,NULL]]",
			idxOff2KeyOff:   "[0 -1 -1 -1]",
			accesses:        "[]",
			remained:        "[]",
			compareFilters:  "<nil>",
		},
		// Join key and pushed eq filter not continuous.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[2]},
			pushedDownConds: "a = 1",
			otherConds:      "",
			ranges:          "[]",
			idxOff2KeyOff:   "[]",
			accesses:        "[]",
			remained:        "[]",
			compareFilters:  "<nil>",
		},
		// Keys are continuous.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1",
			otherConds:      "",
			ranges:          "[[1 NULL,1 NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[eq(test.t.a, 1)]",
			remained:        "[]",
			compareFilters:  "<nil>",
		},
		// Keys are continuous and there're correlated filters.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1",
			otherConds:      "c > g and c < concat(g, \"ab\")",
			ranges:          "[[1 NULL NULL,1 NULL NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[eq(test.t.a, 1) gt(test.t.c, test.t1.g) lt(test.t.c, concat(test.t1.g, ab))]",
			remained:        "[]",
			compareFilters:  "gt(test.t.c, test.t1.g) lt(test.t.c, concat(test.t1.g, ab))",
		},
		// cast function won't be involved.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1",
			otherConds:      "c > g and c < g + 10",
			ranges:          "[[1 NULL NULL,1 NULL NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[eq(test.t.a, 1) gt(test.t.c, test.t1.g)]",
			remained:        "[]",
			compareFilters:  "gt(test.t.c, test.t1.g)",
		},
		// Can deal with prefix index correctly.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1 and c > 'a' and c < 'aaaaaa'",
			otherConds:      "",
			ranges:          "[(1 NULL \"a\",1 NULL \"[97 97]\"]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[eq(test.t.a, 1) gt(test.t.c, a) lt(test.t.c, aaaaaa)]",
			remained:        "[gt(test.t.c, a) lt(test.t.c, aaaaaa)]",
			compareFilters:  "<nil>",
		},
		// Can generate correct ranges for in functions.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a in (1, 2, 3) and c in ('a', 'b', 'c')",
			otherConds:      "",
			ranges:          "[[1 NULL \"a\",1 NULL \"a\"] [2 NULL \"a\",2 NULL \"a\"] [3 NULL \"a\",3 NULL \"a\"] [1 NULL \"b\",1 NULL \"b\"] [2 NULL \"b\",2 NULL \"b\"] [3 NULL \"b\",3 NULL \"b\"] [1 NULL \"c\",1 NULL \"c\"] [2 NULL \"c\",2 NULL \"c\"] [3 NULL \"c\",3 NULL \"c\"]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[in(test.t.a, 1, 2, 3) in(test.t.c, a, b, c)]",
			remained:        "[in(test.t.c, a, b, c)]",
			compareFilters:  "<nil>",
		},
		// Can generate correct ranges for in functions with correlated filters..
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a in (1, 2, 3) and c in ('a', 'b', 'c')",
			otherConds:      "d > h and d < h + 100",
			ranges:          "[[1 NULL \"a\" NULL,1 NULL \"a\" NULL] [2 NULL \"a\" NULL,2 NULL \"a\" NULL] [3 NULL \"a\" NULL,3 NULL \"a\" NULL] [1 NULL \"b\" NULL,1 NULL \"b\" NULL] [2 NULL \"b\" NULL,2 NULL \"b\" NULL] [3 NULL \"b\" NULL,3 NULL \"b\" NULL] [1 NULL \"c\" NULL,1 NULL \"c\" NULL] [2 NULL \"c\" NULL,2 NULL \"c\" NULL] [3 NULL \"c\" NULL,3 NULL \"c\" NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[in(test.t.a, 1, 2, 3) in(test.t.c, a, b, c) gt(test.t.d, test.t1.h) lt(test.t.d, plus(test.t1.h, 100))]",
			remained:        "[in(test.t.c, a, b, c)]",
			compareFilters:  "gt(test.t.d, test.t1.h) lt(test.t.d, plus(test.t1.h, 100))",
		},
		// Join keys are not continuous and the pushed key connect the key but not eq/in functions.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[0], dsSchema.Columns[2]},
			pushedDownConds: "b > 1",
			otherConds:      "",
			ranges:          "[(NULL 1,NULL +inf]]",
			idxOff2KeyOff:   "[0 -1 -1 -1]",
			accesses:        "[gt(test.t.b, 1)]",
			remained:        "[]",
			compareFilters:  "<nil>",
		},
	}
	for i, tt := range tests {
		pushed, err := s.rewriteSimpleExpr(tt.pushedDownConds, dsSchema)
		c.Assert(err, IsNil)
		dataSourceNode.pushedDownConds = pushed
		others, err := s.rewriteSimpleExpr(tt.otherConds, joinNode.schema)
		c.Assert(err, IsNil)
		joinNode.OtherConditions = others
		helper := &indexJoinBuildHelper{join: joinNode, lastColManager: nil}
		_, err = helper.analyzeLookUpFilters(path, dataSourceNode, tt.innerKeys)
		c.Assert(err, IsNil)
		c.Assert(fmt.Sprintf("%v", helper.chosenRanges), Equals, tt.ranges, Commentf("test case: #%v", i))
		c.Assert(fmt.Sprintf("%v", helper.idxOff2KeyOff), Equals, tt.idxOff2KeyOff)
		c.Assert(fmt.Sprintf("%v", helper.chosenAccess), Equals, tt.accesses)
		c.Assert(fmt.Sprintf("%v", helper.chosenRemained), Equals, tt.remained)
		c.Assert(fmt.Sprintf("%v", helper.lastColManager), Equals, tt.compareFilters)
	}
}
