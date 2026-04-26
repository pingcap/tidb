// Copyright 2025 PingCAP, Inc.
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

package logicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLogicalSchemaClone(t *testing.T) {
	ctx := mock.NewContext()
	sp := &logicalop.LogicalSchemaProducer{}
	col1 := &expression.Column{
		ID: 1,
	}
	schema := expression.NewSchema()
	// alloc cap.
	schema.Columns = make([]*expression.Column, 0, 10)
	sp.SetSchema(schema)
	sp.Schema().Append(col1)
	name := &types.FieldName{ColName: ast.NewCIStr("a")}
	names := types.NameSlice{name}
	sp.SetOutputNames(names)
	sp.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "test", nil, 0)
	child1 := logicalop.NewBaseLogicalPlan(ctx, "child1", nil, 0)
	sp.BaseLogicalPlan.SetChildren(child1.GetBaseLogicalPlan())

	cloneSp := *sp
	require.NotNil(t, cloneSp.Schema())
	require.True(t, sp.Schema().Len() > 0)
	require.True(t, cloneSp.Schema().Len() > 0)
	// *schema is shared
	require.True(t, cloneSp.Schema() == sp.Schema())
	// *Name slice is shallow.
	require.True(t, len(cloneSp.OutputNames()) > 0)
	require.True(t, len(sp.OutputNames()) > 0)
	// BaseLogicalPlan struct is a new one.
	require.False(t, &sp.BaseLogicalPlan == &cloneSp.BaseLogicalPlan)
	// children slice inside BaseLogicalPlan is shared.
	require.True(t, len(sp.Children()) == 1)
	require.True(t, len(cloneSp.Children()) == 1)
	require.True(t, sp.Children()[0] == cloneSp.Children()[0])
	// test clonedSp schema append, should affect sp's schema
	col2 := &expression.Column{
		ID: 2,
	}
	cloneSp.Schema().Append(col2)
	// the column slice inside schema will grow at both case.
	require.Equal(t, cloneSp.Schema().Len(), 2)
	require.Equal(t, sp.Schema().Len(), 2)
}

func TestLogicalApplyClone(t *testing.T) {
	ctx := mock.NewContext()
	sp := logicalop.LogicalSchemaProducer{}
	col1 := &expression.Column{
		ID: 1,
	}
	sp.SetSchema(expression.NewSchema(col1))
	name := &types.FieldName{ColName: ast.NewCIStr("a")}
	names := types.NameSlice{name}
	sp.SetOutputNames(names)
	sp.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "test", nil, 0)
	child1 := logicalop.NewBaseLogicalPlan(ctx, "child1", nil, 0)
	sp.BaseLogicalPlan.SetChildren(child1.GetBaseLogicalPlan())

	apply := &logicalop.LogicalApply{
		LogicalJoin: logicalop.LogicalJoin{
			LogicalSchemaProducer: sp,
			EqualConditions:       []*expression.ScalarFunction{},
		},
	}
	apply.EqualConditions = make([]*expression.ScalarFunction, 0, 17)
	apply.EqualConditions = append(apply.EqualConditions, &expression.ScalarFunction{FuncName: ast.NewCIStr("f1")})
	apply.EqualConditions = append(apply.EqualConditions, &expression.ScalarFunction{FuncName: ast.NewCIStr("f2")})
	clonedApply := *apply
	// require.True(t, &apply.EqualConditions == &clonedApply.EqualConditions)
	clonedApply.EqualConditions = append(clonedApply.EqualConditions, &expression.ScalarFunction{FuncName: ast.NewCIStr("f3")})
	require.True(t, len(apply.LogicalJoin.EqualConditions) == 2)
	require.True(t, len(clonedApply.LogicalJoin.EqualConditions) == 3)

	tmp := clonedApply.EqualConditions[0]
	clonedApply.EqualConditions[0] = clonedApply.EqualConditions[1]
	clonedApply.EqualConditions[1] = tmp
	require.True(t, clonedApply.EqualConditions[0].FuncName.L == "f2")
	require.True(t, apply.EqualConditions[0].FuncName.L == "f2")
}

func TestLogicalProjectionPushDownTopN(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`CREATE TABLE table_test (
col16 json DEFAULT NULL,
col17 json DEFAULT NULL
);`)
	sql := `explain format='brief' SELECT 
       s.column16 AS column16,
       s.column17 AS column17
FROM
  (SELECT 
          col16 -> '$[].optUid' AS column16,
          JSON_UNQUOTE(JSON_EXTRACT(col17, '$[0].value')) AS column17
   FROM
     (SELECT 
             col16,
             col17
      FROM table_test) ta24e
   ) AS s 
ORDER BY CONVERT(column16 USING GBK) ASC,column17 ASC
LIMIT 0,
      20;`
	tk.MustQuery(sql).Check(testkit.Rows(
		"Projection 20.00 root  Column#4, Column#5",
		"└─TopN 20.00 root  Column#6, Column#5, offset:0, count:20",
		"  └─Projection 10000.00 root  Column#4, Column#5, convert(cast(Column#4, var_string(16777216)), gbk)->Column#6",
		"    └─TableReader 10000.00 root  data:Projection",
		"      └─Projection 10000.00 cop[tikv]  json_extract(test.table_test.col16, $[].optUid)->Column#4, json_unquote(cast(json_extract(test.table_test.col17, $[0].value), var_string(16777216)))->Column#5",
		"        └─TableFullScan 10000.00 cop[tikv] table:table_test keep order:false, stats:pseudo"))
	tk.MustExec(`INSERT INTO mysql.opt_rule_blacklist VALUES("topn_push_down");`)
	tk.MustExec(`admin reload opt_rule_blacklist;`)
	tk.MustQuery(sql).Check(testkit.Rows(
		"Limit 20.00 root  offset:0, count:20",
		"└─Projection 20.00 root  Column#4, Column#5",
		"  └─Sort 20.00 root  Column#6, Column#5",
		"    └─Projection 10000.00 root  Column#4, Column#5, convert(cast(Column#4, var_string(16777216)), gbk)->Column#6",
		"      └─TableReader 10000.00 root  data:Projection",
		"        └─Projection 10000.00 cop[tikv]  json_extract(test.table_test.col16, $[].optUid)->Column#4, json_unquote(cast(json_extract(test.table_test.col17, $[0].value), var_string(16777216)))->Column#5",
		"          └─TableFullScan 10000.00 cop[tikv] table:table_test keep order:false, stats:pseudo"))
}
