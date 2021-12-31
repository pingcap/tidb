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

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func testGroupToString(t *testing.T, input []string, output []struct {
	SQL    string
	Result []string
}, optimizer *Optimizer) {
	p := parser.New()
	ctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	for i, sql := range input {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		plan, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
		require.NoError(t, err)

		logic, ok := plan.(plannercore.LogicalPlan)
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
		memo.OperandAggregation: {
			NewRulePushAggDownGather(),
		},
		memo.OperandDataSource: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)

	p := parser.New()
	ctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)
	for i, sql := range input {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		plan, _, err := plannercore.BuildLogicalPlanForTest(context.Background(), ctx, stmt, is)
		require.NoError(t, err)

		logic, ok := plan.(plannercore.LogicalPlan)
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
			memo.OperandSelection: {
				NewRulePushSelDownSort(),
				NewRulePushSelDownProjection(),
				NewRulePushSelDownAggregation(),
				NewRulePushSelDownJoin(),
				NewRulePushSelDownUnionAll(),
				NewRulePushSelDownWindow(),
				NewRuleMergeAdjacentSelection(),
			},
			memo.OperandJoin: {
				NewRuleTransformJoinCondToSel(),
			},
		},
		TransformationRuleBatch{ // TiKV layer
			memo.OperandSelection: {
				NewRulePushSelDownTableScan(),
				NewRulePushSelDownTiKVSingleGather(),
				NewRulePushSelDownIndexScan(),
			},
			memo.OperandDataSource: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTopNRules(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(
		TransformationRuleBatch{ // TiDB layer
			memo.OperandLimit: {
				NewRuleTransformLimitToTopN(),
				NewRulePushLimitDownProjection(),
				NewRulePushLimitDownUnionAll(),
				NewRulePushLimitDownOuterJoin(),
				NewRuleMergeAdjacentLimit(),
			},
			memo.OperandTopN: {
				NewRulePushTopNDownProjection(),
				NewRulePushTopNDownOuterJoin(),
				NewRulePushTopNDownUnionAll(),
			},
		},
		TransformationRuleBatch{ // TiKV layer
			memo.OperandLimit: {
				NewRulePushLimitDownTiKVSingleGather(),
			},
			memo.OperandTopN: {
				NewRulePushTopNDownTiKVSingleGather(),
			},
			memo.OperandDataSource: {
				NewRuleEnumeratePaths(),
			},
		},
	)

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestProjectionElimination(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		memo.OperandProjection: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestEliminateMaxMin(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAggregationProjection(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAdjacentTopN(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandLimit: {
			NewRuleTransformLimitToTopN(),
		},
		memo.OperandTopN: {
			NewRulePushTopNDownProjection(),
			NewRuleMergeAdjacentTopN(),
		},
		memo.OperandProjection: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAdjacentLimit(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		memo.OperandLimit: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTransformLimitToTableDual(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		memo.OperandLimit: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestPostTransformationRules(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(TransformationRuleBatch{
		memo.OperandLimit: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestPushLimitDownTiKVSingleGather(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandLimit: {
			NewRulePushLimitDownTiKVSingleGather(),
		},
		memo.OperandProjection: {
			NewRuleEliminateProjection(),
		},
		memo.OperandDataSource: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestEliminateOuterJoin(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
			NewRuleEliminateOuterJoinBelowAggregation(),
		},
		memo.OperandProjection: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTransformAggregateCaseToSelection(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestTransformAggToProj(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
			NewRuleTransformAggToProj(),
		},
		memo.OperandProjection: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestDecorrelate(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandApply: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestInjectProj(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandLimit: {
			NewRuleTransformLimitToTopN(),
		},
	}, map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
			NewRuleInjectProjectionBelowAgg(),
		},
		memo.OperandTopN: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}

func TestMergeAdjacentWindow(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandProjection: {
			NewRuleMergeAdjacentProjection(),
			NewRuleEliminateProjection(),
		},
		memo.OperandWindow: {
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
	transformationRulesSuiteData.GetTestCases(t, &input, &output)
	testGroupToString(t, input, output, optimizer)
}
