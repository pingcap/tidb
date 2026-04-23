// Copyright 2019 PingCAP, Inc.
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

package old_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/cascades/old"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func testGroupToString(t *testing.T, input []string, output []struct {
	SQL    string
	Result []string
}, optimizer *old.ExportedOptimizer) {
	p := parser.New()
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	for i, sql := range input {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		nodeW := resolve.NewNodeW(stmt)
		plan, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, nodeW, is)
		require.NoError(t, err)

		logic, ok := plan.(base.LogicalPlan)
		require.True(t, ok)

		logic, err = old.ExportedOptimizerOnPhasePreprocessing(optimizer, ctx, logic)
		require.NoError(t, err)

		group := memo.Convert2Group(logic)
		err = old.ExportedOptimizerOnPhaseExploration(optimizer, ctx, group)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Result = old.ExportedToString(ctx, group)
		})
		require.Equalf(t, output[i].Result, old.ExportedToString(ctx, group), "case:%v, sql:%s", i, sql)
	}
}

func RunAggPushDownGather(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(old.ExportedTransformationRuleBatch{
		pattern.OperandAggregation: {
			old.ExportedNewRulePushAggDownGather(),
		},
		pattern.OperandDataSource: {
			old.ExportedNewRuleEnumeratePaths(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)

	p := parser.New()
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)
	for i, sql := range input {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		nodeW := resolve.NewNodeW(stmt)
		plan, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, nodeW, is)
		require.NoError(t, err)

		logic, ok := plan.(base.LogicalPlan)
		require.True(t, ok)

		logic, err = old.ExportedOptimizerOnPhasePreprocessing(optimizer, ctx, logic)
		require.NoError(t, err)

		group := memo.Convert2Group(logic)
		err = old.ExportedOptimizerOnPhaseExploration(optimizer, ctx, group)
		require.NoError(t, err)

		// BuildKeyInfo here to test the KeyInfo for partialAgg.
		group.BuildKeyInfo()
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Result = old.ExportedToString(ctx, group)
		})
		require.Equalf(t, output[i].Result, old.ExportedToString(ctx, group), "case:%v, sql:%s", i, sql)
	}
}

func RunPredicatePushDown(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(
		old.ExportedTransformationRuleBatch{ // TiDB layer
			pattern.OperandSelection: {
				old.ExportedNewRulePushSelDownSort(),
				old.ExportedNewRulePushSelDownProjection(),
				old.ExportedNewRulePushSelDownAggregation(),
				old.ExportedNewRulePushSelDownJoin(),
				old.ExportedNewRulePushSelDownUnionAll(),
				old.ExportedNewRulePushSelDownWindow(),
				old.ExportedNewRuleMergeAdjacentSelection(),
			},
			pattern.OperandJoin: {
				old.ExportedNewRuleTransformJoinCondToSel(),
			},
		},
		old.ExportedTransformationRuleBatch{ // TiKV layer
			pattern.OperandSelection: {
				old.ExportedNewRulePushSelDownTableScan(),
				old.ExportedNewRulePushSelDownTiKVSingleGather(),
				old.ExportedNewRulePushSelDownIndexScan(),
			},
			pattern.OperandDataSource: {
				old.ExportedNewRuleEnumeratePaths(),
			},
		},
	)
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunTopNRules(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(
		old.ExportedTransformationRuleBatch{ // TiDB layer
			pattern.OperandLimit: {
				old.ExportedNewRuleTransformLimitToTopN(),
				old.ExportedNewRulePushLimitDownProjection(),
				old.ExportedNewRulePushLimitDownUnionAll(),
				old.ExportedNewRulePushLimitDownOuterJoin(),
				old.ExportedNewRuleMergeAdjacentLimit(),
			},
			pattern.OperandTopN: {
				old.ExportedNewRulePushTopNDownProjection(),
				old.ExportedNewRulePushTopNDownOuterJoin(),
				old.ExportedNewRulePushTopNDownUnionAll(),
			},
		},
		old.ExportedTransformationRuleBatch{ // TiKV layer
			pattern.OperandLimit: {
				old.ExportedNewRulePushLimitDownTiKVSingleGather(),
			},
			pattern.OperandTopN: {
				old.ExportedNewRulePushTopNDownTiKVSingleGather(),
			},
			pattern.OperandDataSource: {
				old.ExportedNewRuleEnumeratePaths(),
			},
		},
	)

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunProjectionElimination(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(old.ExportedTransformationRuleBatch{
		pattern.OperandProjection: {
			old.ExportedNewRuleEliminateProjection(),
			old.ExportedNewRuleMergeAdjacentProjection(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunEliminateMaxMin(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandAggregation: {
			old.ExportedNewRuleEliminateSingleMaxMin(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunMergeAggregationProjection(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandAggregation: {
			old.ExportedNewRuleMergeAggregationProjection(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunMergeAdjacentTopN(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandLimit: {
			old.ExportedNewRuleTransformLimitToTopN(),
		},
		pattern.OperandTopN: {
			old.ExportedNewRulePushTopNDownProjection(),
			old.ExportedNewRuleMergeAdjacentTopN(),
		},
		pattern.OperandProjection: {
			old.ExportedNewRuleMergeAdjacentProjection(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunMergeAdjacentLimit(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(old.ExportedTransformationRuleBatch{
		pattern.OperandLimit: {
			old.ExportedNewRulePushLimitDownProjection(),
			old.ExportedNewRuleMergeAdjacentLimit(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunTransformLimitToTableDual(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(old.ExportedTransformationRuleBatch{
		pattern.OperandLimit: {
			old.ExportedNewRuleTransformLimitToTableDual(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunPostTransformationRules(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(old.ExportedTransformationRuleBatch{
		pattern.OperandLimit: {
			old.ExportedNewRuleTransformLimitToTopN(),
		},
	}, old.ExportedPostTransformationBatch())
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunPushLimitDownTiKVSingleGather(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandLimit: {
			old.ExportedNewRulePushLimitDownTiKVSingleGather(),
		},
		pattern.OperandProjection: {
			old.ExportedNewRuleEliminateProjection(),
		},
		pattern.OperandDataSource: {
			old.ExportedNewRuleEnumeratePaths(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunEliminateOuterJoin(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandAggregation: {
			old.ExportedNewRuleEliminateOuterJoinBelowAggregation(),
		},
		pattern.OperandProjection: {
			old.ExportedNewRuleEliminateOuterJoinBelowProjection(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunTransformAggregateCaseToSelection(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandAggregation: {
			old.ExportedNewRuleTransformAggregateCaseToSelection(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunTransformAggToProj(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandAggregation: {
			old.ExportedNewRuleTransformAggToProj(),
		},
		pattern.OperandProjection: {
			old.ExportedNewRuleMergeAdjacentProjection(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunDecorrelate(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandApply: {
			old.ExportedNewRulePullSelectionUpApply(),
			old.ExportedNewRuleTransformApplyToJoin(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunInjectProj(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandLimit: {
			old.ExportedNewRuleTransformLimitToTopN(),
		},
	}, map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandAggregation: {
			old.ExportedNewRuleInjectProjectionBelowAgg(),
		},
		pattern.OperandTopN: {
			old.ExportedNewRuleInjectProjectionBelowTopN(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func RunMergeAdjacentWindow(t *testing.T) {
	optimizer := old.ExportedNewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]old.ExportedTransformation{
		pattern.OperandProjection: {
			old.ExportedNewRuleMergeAdjacentProjection(),
			old.ExportedNewRuleEliminateProjection(),
		},
		pattern.OperandWindow: {
			old.ExportedNewRuleMergeAdjacentWindow(),
		},
	})
	defer func() {
		old.ExportedOptimizerResetTransformationRules(optimizer, old.ExportedGetDefaultRuleBatches()...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}
