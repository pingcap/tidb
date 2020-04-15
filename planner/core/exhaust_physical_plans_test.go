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
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/planner/util"
	"github.com/pingcap/tidb/v4/types"
)

func (s *testUnitTestSuit) rewriteSimpleExpr(str string, schema *expression.Schema, names types.NameSlice) ([]expression.Expression, error) {
	if str == "" {
		return nil, nil
	}
	filters, err := expression.ParseSimpleExprsWithNames(s.ctx, str, schema, names)
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
	var dsNames types.NameSlice
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("a"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("b"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldTypeWithCollation(mysql.TypeVarchar, mysql.DefaultCollationName, types.UnspecifiedLength),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("c"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("d"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dataSourceNode.schema = dsSchema
	outerChildSchema := expression.NewSchema()
	var outerChildNames types.NameSlice
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("e"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("f"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldTypeWithCollation(mysql.TypeVarchar, mysql.DefaultCollationName, types.UnspecifiedLength),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("g"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("h"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	joinNode.SetSchema(expression.MergeSchema(dsSchema, outerChildSchema))
	path := &util.AccessPath{
		IdxCols:    append(make([]*expression.Column, 0, 4), dsSchema.Columns...),
		IdxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength, 2, types.UnspecifiedLength},
	}
	joinColNames := append(dsNames.Shallow(), outerChildNames...)

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
			accesses:        "[eq(Column#1, 1)]",
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
			accesses:        "[eq(Column#1, 1) gt(Column#3, Column#7) lt(Column#3, concat(Column#7, ab))]",
			remained:        "[]",
			compareFilters:  "gt(Column#3, Column#7) lt(Column#3, concat(Column#7, ab))",
		},
		// cast function won't be involved.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1",
			otherConds:      "c > g and c < g + 10",
			ranges:          "[[1 NULL NULL,1 NULL NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[eq(Column#1, 1) gt(Column#3, Column#7)]",
			remained:        "[]",
			compareFilters:  "gt(Column#3, Column#7)",
		},
		// Can deal with prefix index correctly.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1 and c > 'a' and c < 'aaaaaa'",
			otherConds:      "",
			ranges:          "[(1 NULL \"a\",1 NULL \"[97 97]\"]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[eq(Column#1, 1) gt(Column#3, a) lt(Column#3, aaaaaa)]",
			remained:        "[gt(Column#3, a) lt(Column#3, aaaaaa)]",
			compareFilters:  "<nil>",
		},
		// Can generate correct ranges for in functions.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a in (1, 2, 3) and c in ('a', 'b', 'c')",
			otherConds:      "",
			ranges:          "[[1 NULL \"a\",1 NULL \"a\"] [2 NULL \"a\",2 NULL \"a\"] [3 NULL \"a\",3 NULL \"a\"] [1 NULL \"b\",1 NULL \"b\"] [2 NULL \"b\",2 NULL \"b\"] [3 NULL \"b\",3 NULL \"b\"] [1 NULL \"c\",1 NULL \"c\"] [2 NULL \"c\",2 NULL \"c\"] [3 NULL \"c\",3 NULL \"c\"]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[in(Column#1, 1, 2, 3) in(Column#3, a, b, c)]",
			remained:        "[in(Column#3, a, b, c)]",
			compareFilters:  "<nil>",
		},
		// Can generate correct ranges for in functions with correlated filters..
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a in (1, 2, 3) and c in ('a', 'b', 'c')",
			otherConds:      "d > h and d < h + 100",
			ranges:          "[[1 NULL \"a\" NULL,1 NULL \"a\" NULL] [2 NULL \"a\" NULL,2 NULL \"a\" NULL] [3 NULL \"a\" NULL,3 NULL \"a\" NULL] [1 NULL \"b\" NULL,1 NULL \"b\" NULL] [2 NULL \"b\" NULL,2 NULL \"b\" NULL] [3 NULL \"b\" NULL,3 NULL \"b\" NULL] [1 NULL \"c\" NULL,1 NULL \"c\" NULL] [2 NULL \"c\" NULL,2 NULL \"c\" NULL] [3 NULL \"c\" NULL,3 NULL \"c\" NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1]",
			accesses:        "[in(Column#1, 1, 2, 3) in(Column#3, a, b, c) gt(Column#4, Column#8) lt(Column#4, plus(Column#8, 100))]",
			remained:        "[in(Column#3, a, b, c)]",
			compareFilters:  "gt(Column#4, Column#8) lt(Column#4, plus(Column#8, 100))",
		},
		// Join keys are not continuous and the pushed key connect the key but not eq/in functions.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[0], dsSchema.Columns[2]},
			pushedDownConds: "b > 1",
			otherConds:      "",
			ranges:          "[(NULL 1,NULL +inf]]",
			idxOff2KeyOff:   "[0 -1 -1 -1]",
			accesses:        "[gt(Column#2, 1)]",
			remained:        "[]",
			compareFilters:  "<nil>",
		},
	}
	for i, tt := range tests {
		pushed, err := s.rewriteSimpleExpr(tt.pushedDownConds, dsSchema, dsNames)
		c.Assert(err, IsNil)
		dataSourceNode.pushedDownConds = pushed
		others, err := s.rewriteSimpleExpr(tt.otherConds, joinNode.schema, joinColNames)
		c.Assert(err, IsNil)
		joinNode.OtherConditions = others
		helper := &indexJoinBuildHelper{join: joinNode, lastColManager: nil}
		_, err = helper.analyzeLookUpFilters(path, dataSourceNode, tt.innerKeys)
		c.Assert(err, IsNil)
		c.Assert(fmt.Sprintf("%v", helper.chosenAccess), Equals, tt.accesses)
		c.Assert(fmt.Sprintf("%v", helper.chosenRanges), Equals, tt.ranges, Commentf("test case: #%v", i))
		c.Assert(fmt.Sprintf("%v", helper.idxOff2KeyOff), Equals, tt.idxOff2KeyOff)
		c.Assert(fmt.Sprintf("%v", helper.chosenRemained), Equals, tt.remained)
		c.Assert(fmt.Sprintf("%v", helper.lastColManager), Equals, tt.compareFilters)
	}
}
