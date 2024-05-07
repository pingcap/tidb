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

package cascades

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func testGroupToString(t *testing.T, input []string, output []struct {
	SQL    string
	Result []string
}, optimizer *Optimizer) {
	p := parser.New()
	ctx := plannercore.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	for i, sql := range input {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		plan, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
		require.NoError(t, err)

		logic, ok := plan.(base.LogicalPlan)
		require.True(t, ok)

		logic, err = optimizer.onPhasePreprocessing(ctx, logic)
		require.NoError(t, err)

		group := memo.Convert2Group(logic)
		err = optimizer.onPhaseExploration(ctx, group)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Result = ToString(group)
		})
		require.Equalf(t, output[i].Result, ToString(group), "case:%v, sql:%s", i, sql)
	}
}

func TestAggPushDownGather(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		pattern.OperandAggregation: {
			NewRulePushAggDownGather(),
		},
		pattern.OperandDataSource: {
			NewRuleEnumeratePaths(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)

	p := parser.New()
	ctx := plannercore.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)
	for i, sql := range input {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		plan, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
		require.NoError(t, err)

		logic, ok := plan.(base.LogicalPlan)
		require.True(t, ok)

		logic, err = optimizer.onPhasePreprocessing(ctx, logic)
		require.NoError(t, err)

		group := memo.Convert2Group(logic)
		err = optimizer.onPhaseExploration(ctx, group)
		require.NoError(t, err)

		// BuildKeyInfo here to test the KeyInfo for partialAgg.
		group.BuildKeyInfo()
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Result = ToString(group)
		})
		require.Equalf(t, output[i].Result, ToString(group), "case:%v, sql:%s", i, sql)
	}
}

func TestPredicatePushDown(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(
		TransformationRuleBatch{ // TiDB layer
			pattern.OperandSelection: {
				NewRulePushSelDownSort(),
				NewRulePushSelDownProjection(),
				NewRulePushSelDownAggregation(),
				NewRulePushSelDownJoin(),
				NewRulePushSelDownUnionAll(),
				NewRulePushSelDownWindow(),
				NewRuleMergeAdjacentSelection(),
			},
			pattern.OperandJoin: {
				NewRuleTransformJoinCondToSel(),
			},
		},
		TransformationRuleBatch{ // TiKV layer
			pattern.OperandSelection: {
				NewRulePushSelDownTableScan(),
				NewRulePushSelDownTiKVSingleGather(),
				NewRulePushSelDownIndexScan(),
			},
			pattern.OperandDataSource: {
				NewRuleEnumeratePaths(),
			},
		},
	)
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTopNRules(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(
		TransformationRuleBatch{ // TiDB layer
			pattern.OperandLimit: {
				NewRuleTransformLimitToTopN(),
				NewRulePushLimitDownProjection(),
				NewRulePushLimitDownUnionAll(),
				NewRulePushLimitDownOuterJoin(),
				NewRuleMergeAdjacentLimit(),
			},
			pattern.OperandTopN: {
				NewRulePushTopNDownProjection(),
				NewRulePushTopNDownOuterJoin(),
				NewRulePushTopNDownUnionAll(),
			},
		},
		TransformationRuleBatch{ // TiKV layer
			pattern.OperandLimit: {
				NewRulePushLimitDownTiKVSingleGather(),
			},
			pattern.OperandTopN: {
				NewRulePushTopNDownTiKVSingleGather(),
			},
			pattern.OperandDataSource: {
				NewRuleEnumeratePaths(),
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

func TestProjectionElimination(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		pattern.OperandProjection: {
			NewRuleEliminateProjection(),
			NewRuleMergeAdjacentProjection(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestEliminateMaxMin(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandAggregation: {
			NewRuleEliminateSingleMaxMin(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAggregationProjection(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandAggregation: {
			NewRuleMergeAggregationProjection(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAdjacentTopN(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandLimit: {
			NewRuleTransformLimitToTopN(),
		},
		pattern.OperandTopN: {
			NewRulePushTopNDownProjection(),
			NewRuleMergeAdjacentTopN(),
		},
		pattern.OperandProjection: {
			NewRuleMergeAdjacentProjection(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAdjacentLimit(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		pattern.OperandLimit: {
			NewRulePushLimitDownProjection(),
			NewRuleMergeAdjacentLimit(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTransformLimitToTableDual(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		pattern.OperandLimit: {
			NewRuleTransformLimitToTableDual(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestPostTransformationRules(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		pattern.OperandLimit: {
			NewRuleTransformLimitToTopN(),
		},
	}, PostTransformationBatch)
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestPushLimitDownTiKVSingleGather(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandLimit: {
			NewRulePushLimitDownTiKVSingleGather(),
		},
		pattern.OperandProjection: {
			NewRuleEliminateProjection(),
		},
		pattern.OperandDataSource: {
			NewRuleEnumeratePaths(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestEliminateOuterJoin(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandAggregation: {
			NewRuleEliminateOuterJoinBelowAggregation(),
		},
		pattern.OperandProjection: {
			NewRuleEliminateOuterJoinBelowProjection(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTransformAggregateCaseToSelection(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandAggregation: {
			NewRuleTransformAggregateCaseToSelection(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTransformAggToProj(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandAggregation: {
			NewRuleTransformAggToProj(),
		},
		pattern.OperandProjection: {
			NewRuleMergeAdjacentProjection(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestDecorrelate(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandApply: {
			NewRulePullSelectionUpApply(),
			NewRuleTransformApplyToJoin(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestInjectProj(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandLimit: {
			NewRuleTransformLimitToTopN(),
		},
	}, map[pattern.Operand][]Transformation{
		pattern.OperandAggregation: {
			NewRuleInjectProjectionBelowAgg(),
		},
		pattern.OperandTopN: {
			NewRuleInjectProjectionBelowTopN(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAdjacentWindow(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[pattern.Operand][]Transformation{
		pattern.OperandProjection: {
			NewRuleMergeAdjacentProjection(),
			NewRuleEliminateProjection(),
		},
		pattern.OperandWindow: {
			NewRuleMergeAdjacentWindow(),
		},
	})
	defer func() {
		optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.LoadTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}
