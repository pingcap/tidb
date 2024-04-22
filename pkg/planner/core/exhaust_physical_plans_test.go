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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func rewriteSimpleExpr(ctx expression.BuildContext, str string, schema *expression.Schema, names types.NameSlice) ([]expression.Expression, error) {
	if str == "" {
		return nil, nil
	}
	filter, err := expression.ParseSimpleExpr(ctx, str, expression.WithInputSchemaAndNames(schema, names, nil))
	if err != nil {
		return nil, err
	}
	if sf, ok := filter.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
		return expression.FlattenCNFConditions(sf), nil
	}
	return []expression.Expression{filter}, nil
}

type indexJoinContext struct {
	dataSourceNode *DataSource
	dsNames        types.NameSlice
	path           *util.AccessPath
	joinNode       *LogicalJoin
	joinColNames   types.NameSlice
}

func prepareForAnalyzeLookUpFilters() *indexJoinContext {
	ctx := MockContext()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	ctx.GetSessionVars().PlanID.Store(-1)
	joinNode := LogicalJoin{}.Init(ctx.GetPlanCtx(), 0)
	dataSourceNode := DataSource{}.Init(ctx.GetPlanCtx(), 0)
	dsSchema := expression.NewSchema()
	var dsNames types.NameSlice
	dsSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("a"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("b"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldTypeWithCollation(mysql.TypeVarchar, mysql.DefaultCollationName, types.UnspecifiedLength),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("c"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("d"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dsSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldTypeWithCollation(mysql.TypeVarchar, charset.CollationASCII, types.UnspecifiedLength),
	})
	dsNames = append(dsNames, &types.FieldName{
		ColName: model.NewCIStr("c_ascii"),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
	})
	dataSourceNode.schema = dsSchema
	dataSourceNode.SetStats(&property.StatsInfo{StatsVersion: statistics.PseudoVersion})
	path := &util.AccessPath{
		IdxCols:    append(make([]*expression.Column, 0, 5), dsSchema.Columns...),
		IdxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength, 2, types.UnspecifiedLength, 2},
	}
	outerChildSchema := expression.NewSchema()
	var outerChildNames types.NameSlice
	outerChildSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("e"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("f"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldTypeWithCollation(mysql.TypeVarchar, mysql.DefaultCollationName, types.UnspecifiedLength),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("g"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	outerChildSchema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	outerChildNames = append(outerChildNames, &types.FieldName{
		ColName: model.NewCIStr("h"),
		TblName: model.NewCIStr("t1"),
		DBName:  model.NewCIStr("test"),
	})
	joinNode.SetSchema(expression.MergeSchema(dsSchema, outerChildSchema))
	joinColNames := append(dsNames.Shallow(), outerChildNames...)
	return &indexJoinContext{
		dataSourceNode: dataSourceNode,
		dsNames:        dsNames,
		path:           path,
		joinNode:       joinNode,
		joinColNames:   joinColNames,
	}
}

type indexJoinTestCase struct {
	// input
	innerKeys       []*expression.Column
	pushedDownConds string
	otherConds      string
	rangeMaxSize    int64
	rebuildMode     bool

	// expected output
	ranges         string
	idxOff2KeyOff  string
	accesses       string
	remained       string
	compareFilters string
}

func testAnalyzeLookUpFilters(t *testing.T, testCtx *indexJoinContext, testCase *indexJoinTestCase, msgAndArgs ...any) *indexJoinBuildHelper {
	ctx := testCtx.dataSourceNode.SCtx()
	ctx.GetSessionVars().RangeMaxSize = testCase.rangeMaxSize
	dataSourceNode := testCtx.dataSourceNode
	joinNode := testCtx.joinNode
	pushed, err := rewriteSimpleExpr(ctx.GetExprCtx(), testCase.pushedDownConds, dataSourceNode.schema, testCtx.dsNames)
	require.NoError(t, err)
	dataSourceNode.pushedDownConds = pushed
	others, err := rewriteSimpleExpr(ctx.GetExprCtx(), testCase.otherConds, joinNode.schema, testCtx.joinColNames)
	require.NoError(t, err)
	joinNode.OtherConditions = others
	helper := &indexJoinBuildHelper{join: joinNode, lastColManager: nil, innerPlan: dataSourceNode}
	_, err = helper.analyzeLookUpFilters(testCtx.path, dataSourceNode, testCase.innerKeys, testCase.innerKeys, testCase.rebuildMode)
	if helper.chosenRanges == nil {
		helper.chosenRanges = ranger.Ranges{}
	}
	require.NoError(t, err)
	if testCase.rebuildMode {
		require.Equal(t, testCase.ranges, fmt.Sprintf("%v", helper.chosenRanges.Range()), msgAndArgs)
	} else {
		require.Equal(t, testCase.accesses, fmt.Sprintf("%v", helper.chosenAccess), msgAndArgs)
		require.Equal(t, testCase.ranges, fmt.Sprintf("%v", helper.chosenRanges.Range()), msgAndArgs)
		require.Equal(t, testCase.idxOff2KeyOff, fmt.Sprintf("%v", helper.idxOff2KeyOff), msgAndArgs)
		require.Equal(t, testCase.remained, fmt.Sprintf("%v", helper.chosenRemained), msgAndArgs)
		require.Equal(t, testCase.compareFilters, fmt.Sprintf("%v", helper.lastColManager), msgAndArgs)
	}
	return helper
}

func TestIndexJoinAnalyzeLookUpFilters(t *testing.T) {
	indexJoinCtx := prepareForAnalyzeLookUpFilters()
	dsSchema := indexJoinCtx.dataSourceNode.schema
	tests := []indexJoinTestCase{
		// Join key not continuous and no pushed filter to match.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[0], dsSchema.Columns[2]},
			pushedDownConds: "",
			otherConds:      "",
			ranges:          "[[NULL,NULL]]",
			idxOff2KeyOff:   "[0 -1 -1 -1 -1]",
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
			idxOff2KeyOff:   "[-1 0 -1 -1 -1]",
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
			idxOff2KeyOff:   "[-1 0 -1 -1 -1]",
			accesses:        "[eq(Column#1, 1) gt(Column#3, Column#8) lt(Column#3, concat(Column#8, ab))]",
			remained:        "[]",
			compareFilters:  "gt(Column#3, Column#8) lt(Column#3, concat(Column#8, ab))",
		},
		// cast function won't be involved.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1",
			otherConds:      "c > g and c < g + 10",
			ranges:          "[[1 NULL NULL,1 NULL NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1 -1]",
			accesses:        "[eq(Column#1, 1) gt(Column#3, Column#8)]",
			remained:        "[]",
			compareFilters:  "gt(Column#3, Column#8)",
		},
		// Can deal with prefix index correctly.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1 and c > 'a' and c < 'aaaaaa'",
			otherConds:      "",
			ranges:          "[(1 NULL \"a\",1 NULL \"aa\"]]",
			idxOff2KeyOff:   "[-1 0 -1 -1 -1]",
			accesses:        "[eq(Column#1, 1) gt(Column#3, a) lt(Column#3, aaaaaa)]",
			remained:        "[gt(Column#3, a) lt(Column#3, aaaaaa)]",
			compareFilters:  "<nil>",
		},
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1], dsSchema.Columns[2], dsSchema.Columns[3]},
			pushedDownConds: "a = 1 and c_ascii > 'a' and c_ascii < 'aaaaaa'",
			otherConds:      "",
			ranges:          "[(1 NULL NULL NULL \"a\",1 NULL NULL NULL \"aa\"]]",
			idxOff2KeyOff:   "[-1 0 1 2 -1]",
			accesses:        "[eq(Column#1, 1) gt(Column#5, a) lt(Column#5, aaaaaa)]",
			remained:        "[gt(Column#5, a) lt(Column#5, aaaaaa)]",
			compareFilters:  "<nil>",
		},
		// Can generate correct ranges for in functions.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a in (1, 2, 3) and c in ('a', 'b', 'c')",
			otherConds:      "",
			ranges:          "[[1 NULL \"a\",1 NULL \"a\"] [1 NULL \"b\",1 NULL \"b\"] [1 NULL \"c\",1 NULL \"c\"] [2 NULL \"a\",2 NULL \"a\"] [2 NULL \"b\",2 NULL \"b\"] [2 NULL \"c\",2 NULL \"c\"] [3 NULL \"a\",3 NULL \"a\"] [3 NULL \"b\",3 NULL \"b\"] [3 NULL \"c\",3 NULL \"c\"]]",
			idxOff2KeyOff:   "[-1 0 -1 -1 -1]",
			accesses:        "[in(Column#1, 1, 2, 3) in(Column#3, a, b, c)]",
			remained:        "[in(Column#3, a, b, c)]",
			compareFilters:  "<nil>",
		},
		// Can generate correct ranges for in functions with correlated filters..
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a in (1, 2, 3) and c in ('a', 'b', 'c')",
			otherConds:      "d > h and d < h + 100",
			ranges:          "[[1 NULL \"a\" NULL,1 NULL \"a\" NULL] [1 NULL \"b\" NULL,1 NULL \"b\" NULL] [1 NULL \"c\" NULL,1 NULL \"c\" NULL] [2 NULL \"a\" NULL,2 NULL \"a\" NULL] [2 NULL \"b\" NULL,2 NULL \"b\" NULL] [2 NULL \"c\" NULL,2 NULL \"c\" NULL] [3 NULL \"a\" NULL,3 NULL \"a\" NULL] [3 NULL \"b\" NULL,3 NULL \"b\" NULL] [3 NULL \"c\" NULL,3 NULL \"c\" NULL]]",
			idxOff2KeyOff:   "[-1 0 -1 -1 -1]",
			accesses:        "[in(Column#1, 1, 2, 3) in(Column#3, a, b, c) gt(Column#4, Column#9) lt(Column#4, plus(Column#9, 100))]",
			remained:        "[in(Column#3, a, b, c)]",
			compareFilters:  "gt(Column#4, Column#9) lt(Column#4, plus(Column#9, 100))",
		},
		// Join keys are not continuous and the pushed key connect the key but not eq/in functions.
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[0], dsSchema.Columns[2]},
			pushedDownConds: "b > 1",
			otherConds:      "",
			ranges:          "[(NULL 1,NULL +inf]]",
			idxOff2KeyOff:   "[0 -1 -1 -1 -1]",
			accesses:        "[gt(Column#2, 1)]",
			remained:        "[]",
			compareFilters:  "<nil>",
		},
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a = 1 and c > 'a' and c < '一二三'",
			otherConds:      "",
			ranges:          "[(1 NULL \"a\",1 NULL \"一二\"]]",
			idxOff2KeyOff:   "[-1 0 -1 -1 -1]",
			accesses:        "[eq(Column#1, 1) gt(Column#3, a) lt(Column#3, 一二三)]",
			remained:        "[gt(Column#3, a) lt(Column#3, 一二三)]",
			compareFilters:  "<nil>",
		},
	}
	for i, tt := range tests {
		testAnalyzeLookUpFilters(t, indexJoinCtx, &tt, fmt.Sprintf("test case: %v", i))
	}
}

func checkRangeFallbackAndReset(t *testing.T, ctx base.PlanContext, expectedRangeFallback bool) {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	hasRangeFallbackWarn := false
	for _, warn := range stmtCtx.GetWarnings() {
		hasRangeFallbackWarn = hasRangeFallbackWarn || strings.Contains(warn.Err.Error(), "'tidb_opt_range_max_size' exceeded when building ranges")
	}
	require.Equal(t, expectedRangeFallback, hasRangeFallbackWarn)
	stmtCtx.PlanCacheTracker = contextutil.NewPlanCacheTracker(stmtCtx)
	stmtCtx.RangeFallbackHandler = contextutil.NewRangeFallbackHandler(&stmtCtx.PlanCacheTracker, stmtCtx)
	stmtCtx.SetWarnings(nil)
}

func TestRangeFallbackForAnalyzeLookUpFilters(t *testing.T) {
	ijCtx := prepareForAnalyzeLookUpFilters()
	ctx := ijCtx.dataSourceNode.SCtx()
	dsSchema := ijCtx.dataSourceNode.schema

	type testOutput struct {
		ranges         string
		idxOff2KeyOff  string
		accesses       string
		remained       string
		compareFilters string
	}

	tests := []struct {
		innerKeys       []*expression.Column
		pushedDownConds string
		otherConds      string
		outputs         []testOutput
	}{
		{
			innerKeys:       []*expression.Column{dsSchema.Columns[1], dsSchema.Columns[3]},
			pushedDownConds: "a in (1, 3) and c in ('aaa', 'bbb')",
			otherConds:      "",
			outputs: []testOutput{
				{
					ranges:         "[[1 NULL \"aa\" NULL,1 NULL \"aa\" NULL] [1 NULL \"bb\" NULL,1 NULL \"bb\" NULL] [3 NULL \"aa\" NULL,3 NULL \"aa\" NULL] [3 NULL \"bb\" NULL,3 NULL \"bb\" NULL]]",
					idxOff2KeyOff:  "[-1 0 -1 1 -1]",
					accesses:       "[in(Column#1, 1, 3) in(Column#3, aaa, bbb)]",
					remained:       "[in(Column#3, aaa, bbb)]",
					compareFilters: "<nil>",
				},
				{
					ranges:         "[[1 NULL \"aa\",1 NULL \"aa\"] [1 NULL \"bb\",1 NULL \"bb\"] [3 NULL \"aa\",3 NULL \"aa\"] [3 NULL \"bb\",3 NULL \"bb\"]]",
					idxOff2KeyOff:  "[-1 0 -1 -1 -1]",
					accesses:       "[in(Column#1, 1, 3) in(Column#3, aaa, bbb)]",
					remained:       "[in(Column#3, aaa, bbb)]",
					compareFilters: "<nil>",
				},
				{
					ranges:         "[[1 NULL,1 NULL] [3 NULL,3 NULL]]",
					idxOff2KeyOff:  "[-1 0 -1 -1 -1]",
					accesses:       "[in(Column#1, 1, 3)]",
					remained:       "[in(Column#3, aaa, bbb)]",
					compareFilters: "<nil>",
				},
				{
					ranges:         "[]",
					idxOff2KeyOff:  "[]",
					accesses:       "[]",
					remained:       "[]",
					compareFilters: "<nil>",
				},
			},
		},
		{
			// test haveExtraCol
			innerKeys:       []*expression.Column{dsSchema.Columns[0]},
			pushedDownConds: "b in (1, 3, 5)",
			otherConds:      "c > g and c < concat(g, 'aaa')",
			outputs: []testOutput{
				{
					ranges:         "[[NULL 1 NULL,NULL 1 NULL] [NULL 3 NULL,NULL 3 NULL] [NULL 5 NULL,NULL 5 NULL]]",
					idxOff2KeyOff:  "[0 -1 -1 -1 -1]",
					accesses:       "[in(Column#2, 1, 3, 5) gt(Column#3, Column#8) lt(Column#3, concat(Column#8, aaa))]",
					remained:       "[]",
					compareFilters: "gt(Column#3, Column#8) lt(Column#3, concat(Column#8, aaa))",
				},
				{
					ranges:         "[[NULL 1,NULL 1] [NULL 3,NULL 3] [NULL 5,NULL 5]]",
					idxOff2KeyOff:  "[0 -1 -1 -1 -1]",
					accesses:       "[in(Column#2, 1, 3, 5)]",
					remained:       "[]",
					compareFilters: "<nil>",
				},
				{
					ranges:         "[[NULL,NULL]]",
					idxOff2KeyOff:  "[0 -1 -1 -1 -1]",
					accesses:       "[]",
					remained:       "[in(Column#2, 1, 3, 5)]",
					compareFilters: "<nil>",
				},
			},
		},
		{
			// test nextColRange
			innerKeys:       []*expression.Column{dsSchema.Columns[1]},
			pushedDownConds: "a in (1, 3) and c > 'aaa' and c < 'bbb'",
			otherConds:      "",
			outputs: []testOutput{
				{
					ranges:         "[[1 NULL \"aa\",1 NULL \"bb\"] [3 NULL \"aa\",3 NULL \"bb\"]]",
					idxOff2KeyOff:  "[-1 0 -1 -1 -1]",
					accesses:       "[in(Column#1, 1, 3) gt(Column#3, aaa) lt(Column#3, bbb)]",
					remained:       "[gt(Column#3, aaa) lt(Column#3, bbb)]",
					compareFilters: "<nil>",
				},
				{
					ranges:         "[[1 NULL,1 NULL] [3 NULL,3 NULL]]",
					idxOff2KeyOff:  "[-1 0 -1 -1 -1]",
					accesses:       "[in(Column#1, 1, 3)]",
					remained:       "[gt(Column#3, aaa) lt(Column#3, bbb)]",
					compareFilters: "<nil>",
				},
			},
		},
	}
	for _, tt := range tests {
		ijCase := &indexJoinTestCase{
			innerKeys:       tt.innerKeys,
			pushedDownConds: tt.pushedDownConds,
			otherConds:      tt.otherConds,
			rangeMaxSize:    0,
		}
		for i, res := range tt.outputs {
			ijCase.ranges = res.ranges
			ijCase.idxOff2KeyOff = res.idxOff2KeyOff
			ijCase.accesses = res.accesses
			ijCase.remained = res.remained
			ijCase.compareFilters = res.compareFilters
			ijHelper := testAnalyzeLookUpFilters(t, ijCtx, ijCase)
			checkRangeFallbackAndReset(t, ctx, i > 0)
			ijCase.rangeMaxSize = ijHelper.chosenRanges.Range().MemUsage() - 1
		}
	}

	// test that building ranges doesn't have mem limit under rebuild mode
	ijCase := &indexJoinTestCase{
		innerKeys:       []*expression.Column{dsSchema.Columns[0], dsSchema.Columns[2]},
		pushedDownConds: "b in (1, 3) and d in (2, 4)",
		otherConds:      "",
		rangeMaxSize:    1,
		rebuildMode:     true,
		ranges:          "[[NULL 1 NULL 2,NULL 1 NULL 2] [NULL 1 NULL 4,NULL 1 NULL 4] [NULL 3 NULL 2,NULL 3 NULL 2] [NULL 3 NULL 4,NULL 3 NULL 4]]",
	}
	ijHelper := testAnalyzeLookUpFilters(t, ijCtx, ijCase)
	checkRangeFallbackAndReset(t, ctx, false)
	require.Greater(t, ijHelper.chosenRanges.Range().MemUsage(), ijCase.rangeMaxSize)
}
