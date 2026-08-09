// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLocalMatchCostReflectsDocumentAndQueryWork(t *testing.T) {
	shortSimple := localMatchCostExprForTest(t, 16, 1, 0, false)
	longSimple := localMatchCostExprForTest(t, 4096, 1, 0, false)
	shortPhrase := localMatchCostExprForTest(t, 16, 8, 1, false)
	longSparsePhrase := localMatchCostExprForTest(t, 4096, 8, 4, false)
	shortNever := localMatchCostExprForTest(t, 16, 1, 0, true)
	longNever := localMatchCostExprForTest(t, 4096, 1, 4, true)

	shortSimpleCost := localMatchAgainstFilterCostWeight([]expression.Expression{shortSimple})
	longSimpleCost := localMatchAgainstFilterCostWeight([]expression.Expression{longSimple})
	shortPhraseCost := localMatchAgainstFilterCostWeight([]expression.Expression{shortPhrase})
	longSparsePhraseCost := localMatchAgainstFilterCostWeight([]expression.Expression{longSparsePhrase})
	shortNeverCost := localMatchAgainstFilterCostWeight([]expression.Expression{shortNever})
	longNeverCost := localMatchAgainstFilterCostWeight([]expression.Expression{longNever})
	require.Equal(t, float64(4), shortSimpleCost)
	require.Greater(t, longSimpleCost, shortSimpleCost)
	require.Greater(t, shortPhraseCost, shortSimpleCost)
	require.Greater(t, longSparsePhraseCost, longSimpleCost+shortPhraseCost,
		"document size and phrase work must interact instead of being purely additive")
	require.Zero(t, shortNeverCost)
	require.Equal(t, shortNeverCost, longNeverCost,
		"queries proved false return before document-sized work")
}

func localMatchCostExprForTest(
	t *testing.T,
	estimatedRowBytes, queryMatchCost, queryDocumentCost float64,
	matchNothing bool,
) expression.Expression {
	t.Helper()
	ctx := mock.NewContext()
	stringType := types.NewFieldType(mysql.TypeVarchar)
	fn, err := expression.NewFunction(
		ctx,
		ast.FTSMysqlMatchAgainst,
		types.NewFieldType(mysql.TypeDouble),
		&expression.Constant{Value: types.NewStringDatum("tidb"), RetType: stringType},
		&expression.Column{Index: 0, ID: 1, UniqueID: 1, RetType: stringType},
	)
	require.NoError(t, err)
	sf := fn.(*expression.ScalarFunction)
	require.NoError(t, expression.SetFTSMysqlMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))
	require.NoError(t, expression.SetFTSMysqlMatchAgainstLocalEvalInfo(sf, &expression.FTSLocalEvalInfo{
		ColumnIDs:         []int64{1},
		ColumnUniqueIDs:   []int64{1},
		EstimatedRowBytes: estimatedRowBytes,
		QueryMatchCost:    queryMatchCost,
		QueryDocumentCost: queryDocumentCost,
		MatchNothing:      matchNothing,
		NoScore:           true,
	}))
	return sf
}
