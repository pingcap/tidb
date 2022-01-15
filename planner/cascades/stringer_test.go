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

func TestGroupStringer(t *testing.T) {
	optimizer := NewOptimizer()
	optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandSelection: {
			NewRulePushSelDownTiKVSingleGather(),
			NewRulePushSelDownTableScan(),
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
	stringerSuiteData.GetTestCases(t, &input, &output)

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
		require.NoError(t, optimizer.onPhaseExploration(ctx, group))

		group.BuildKeyInfo()
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Result = ToString(group)
		})
		require.Equalf(t, output[i].Result, ToString(group), "case:%v, sql:%s", i, sql)
	}
}
