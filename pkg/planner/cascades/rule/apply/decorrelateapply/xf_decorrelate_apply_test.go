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

package decorrelateapply_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cascades"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule/apply/decorrelateapply"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/stretchr/testify/require"
)

// when the original is applied with pulling up predicate and generate a temporary apply.
// this apply is called intermediary apply, which is not the final target of what we want.
// the subsequent XFDeCorrelate rules will be applied on them later, once generate a new
// other intermediary apply, current src intermediary apply should be removed from memo.
func TestXFDeCorrelateShouldDeleteIntermediaryApply(t *testing.T) {
	t.Skip("skip this test for now, cause decorrelateapply.XFDeCorrelateSimpleApply rule is not applied in the cascades optimizer fully, and this test is not meaningful now.")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats"))
	}()

	ctx := mock.NewContext()
	// new logical schema producer.
	sp := logicalop.LogicalSchemaProducer{}
	col1 := &expression.Column{
		ID: 1,
	}
	sp.SetSchema(expression.NewSchema(col1))
	name := &types.FieldName{ColName: ast.NewCIStr("a")}
	names := types.NameSlice{name}
	sp.SetOutputNames(names)
	sp.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "apply", nil, 0)

	sp2 := logicalop.LogicalSchemaProducer{}
	col2 := &expression.Column{
		ID: 2,
	}
	sp2.SetSchema(expression.NewSchema(col2))
	name = &types.FieldName{ColName: ast.NewCIStr("b")}
	names = types.NameSlice{name}
	sp2.SetOutputNames(names)
	sp2.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "ds1", nil, 0)
	ds2 := &logicalop.DataSource{
		LogicalSchemaProducer: sp2,
	}
	sp3 := logicalop.LogicalSchemaProducer{}
	col3 := &expression.Column{
		ID: 3,
	}
	sp3.SetSchema(expression.NewSchema(col3))
	name = &types.FieldName{ColName: ast.NewCIStr("c")}
	names = types.NameSlice{name}
	sp3.SetOutputNames(names)
	sp3.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "ds2", nil, 0)
	ds3 := &logicalop.DataSource{
		LogicalSchemaProducer: sp3,
	}
	hasher := base.NewHashEqualer()
	ds2.Hash64(hasher)
	ds2Hash64 := hasher.Sum64()
	hasher.Reset()
	ds3.Hash64(hasher)
	ds3Hash64 := hasher.Sum64()
	require.NotEqual(t, ds2Hash64, ds3Hash64)

	sp.BaseLogicalPlan.SetChildren(ds2, ds3)
	join := &logicalop.LogicalJoin{
		LogicalSchemaProducer: sp,
	}
	apply := &logicalop.LogicalApply{LogicalJoin: *join, NoDecorrelate: false}
	apply.SetSelf(apply)
	apply.SetTP(plancodec.TypeApply)
	mm := memo.NewMemo()
	mm.Init(apply)
	myRule := decorrelateapply.NewXFDeCorrelateSimpleApply()
	cas, err := cascades.NewOptimizer(apply)
	require.Nil(t, err)
	// only allow NewXFDeCorrelateSimpleApply.
	cas.SetRules([]uint{myRule.ID()})
	defer cas.Destroy()
	err = cas.Execute()
	require.Nil(t, err)

	length := 0
	// logical plan num should be 2.
	cas.GetMemo().NewIterator().Each(func(plan corebase.LogicalPlan) bool {
		if length == 0 {
			apply := plan.(*logicalop.LogicalApply)
			require.True(t, apply != nil)
			require.True(t, apply.Schema().Columns[0].ID == 1)
			require.True(t, apply.Self() == apply)
			require.True(t, apply.TP() == plancodec.TypeApply)

			ds1 := plan.Children()[0].(*logicalop.DataSource)
			require.True(t, ds1 != nil)
			require.True(t, ds1.Schema().Columns[0].ID == 2)
			ds2 := plan.Children()[1].(*logicalop.DataSource)
			require.True(t, ds2 != nil)
			require.True(t, ds2.Schema().Columns[0].ID == 3)
		} else if length == 1 {
			join := plan.(*logicalop.LogicalJoin)
			require.True(t, join != nil)
			require.True(t, join.Schema().Columns[0].ID == 1)
			require.True(t, join.Schema().Columns[0] == apply.Schema().Columns[0]) // ref
			require.True(t, join.Self() == join)
			require.True(t, join.TP() == plancodec.TypeJoin)

			ds1 := plan.Children()[0].(*logicalop.DataSource)
			require.True(t, ds1 != nil)
			require.True(t, ds1.Schema().Columns[0].ID == 2)
			ds2 := plan.Children()[1].(*logicalop.DataSource)
			require.True(t, ds2 != nil)
			require.True(t, ds2.Schema().Columns[0].ID == 3)
		}
		length++
		return true
	})
	require.True(t, length == 2)

	// restore the original plan tree, and mark the Apply generated from xForm rule.
	apply.BaseLogicalPlan.SetChildren(ds2, ds3)
	apply.SetFlag(logicalop.ApplyGenFromXFDeCorrelateRuleFlag)
	mm.Destroy()
	mm.Init(apply)
	myRule = decorrelateapply.NewXFDeCorrelateSimpleApply()
	cas, err = cascades.NewOptimizer(apply)
	require.Nil(t, err)
	// only allow NewXFDeCorrelateSimpleApply.
	cas.SetRules([]uint{myRule.ID()})
	defer cas.Destroy()
	err = cas.Execute()
	require.Nil(t, err)

	// logical plan num should be 1.
	length = 0
	cas.GetMemo().NewIterator().Each(func(plan corebase.LogicalPlan) bool {
		if length == 0 {
			join := plan.(*logicalop.LogicalJoin)
			require.True(t, join != nil)
			require.True(t, join.Schema().Columns[0].ID == 1)
			require.True(t, join.Schema().Columns[0] == apply.Schema().Columns[0]) // ref
			require.True(t, join.Self() == join)
			require.True(t, join.TP() == plancodec.TypeJoin)

			ds1 := plan.Children()[0].(*logicalop.DataSource)
			require.True(t, ds1 != nil)
			require.True(t, ds1.Schema().Columns[0].ID == 2)
			ds2 := plan.Children()[1].(*logicalop.DataSource)
			require.True(t, ds2 != nil)
			require.True(t, ds2.Schema().Columns[0].ID == 3)
		}
		length++
		return true
	})
	require.True(t, length == 1)
}
