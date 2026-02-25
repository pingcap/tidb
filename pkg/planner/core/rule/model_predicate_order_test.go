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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPredicateSimplificationModelPredictOrder(t *testing.T) {
	sctx := mock.NewContext()
	ectx := sctx.GetExprCtx()

	col := &expression.Column{
		ID:       1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	modelOutput := expression.NewFunctionInternal(
		ectx,
		ast.ModelPredictOutput,
		types.NewFieldType(mysql.TypeDouble),
		expression.NewStrConst("m1"),
		expression.NewStrConst("score"),
		col,
	)
	modelPred := expression.NewFunctionInternal(
		ectx,
		ast.GT,
		types.NewFieldType(mysql.TypeTiny),
		modelOutput,
		expression.NewInt64Const(0),
	)
	cheapPred := expression.NewFunctionInternal(
		ectx,
		ast.GT,
		types.NewFieldType(mysql.TypeTiny),
		col,
		expression.NewInt64Const(1),
	)
	conds := applyPredicateSimplification(sctx.GetPlanCtx(), []expression.Expression{modelPred, cheapPred}, false, nil)
	require.Len(t, conds, 2)

	isModelPredicate := func(expr expression.Expression) bool {
		return expression.CheckFuncInExpr(expr, ast.ModelPredict) || expression.CheckFuncInExpr(expr, ast.ModelPredictOutput)
	}

	require.False(t, isModelPredicate(conds[0]), "model predicate should be ordered after cheap predicate")
	require.True(t, isModelPredicate(conds[1]), "expected model predicate in the second position")
}

func TestPredicateSimplificationLLMOrder(t *testing.T) {
	sctx := mock.NewContext()
	ectx := sctx.GetExprCtx()

	col := &expression.Column{
		ID:       1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	llmExpr := expression.NewFunctionInternal(
		ectx,
		ast.LLMComplete,
		types.NewFieldType(mysql.TypeString),
		expression.NewStrConst("hello"),
	)
	llmPred := expression.NewFunctionInternal(
		ectx,
		ast.EQ,
		types.NewFieldType(mysql.TypeTiny),
		llmExpr,
		expression.NewStrConst("ok"),
	)
	cheapPred := expression.NewFunctionInternal(
		ectx,
		ast.GT,
		types.NewFieldType(mysql.TypeTiny),
		col,
		expression.NewInt64Const(1),
	)
	conds := applyPredicateSimplification(sctx.GetPlanCtx(), []expression.Expression{llmPred, cheapPred}, false, nil)
	require.Len(t, conds, 2)

	isLLMPredicate := func(expr expression.Expression) bool {
		return expression.CheckFuncInExpr(expr, ast.LLMComplete) || expression.CheckFuncInExpr(expr, ast.LLMEmbedText)
	}

	require.False(t, isLLMPredicate(conds[0]), "llm predicate should be ordered after cheap predicate")
	require.True(t, isLLMPredicate(conds[1]), "expected llm predicate in the second position")
}
