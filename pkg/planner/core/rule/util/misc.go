// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/zeropool"
)

// ResolveExprAndReplace replaces columns fields of expressions by children logical plans.
func ResolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) expression.Expression {
	switch expr := origin.(type) {
	case *expression.Column:
		return ResolveColumnAndReplace(expr, replace)
	case *expression.CorrelatedColumn:
		newCol, changed := resolveColumnAndReplace(&expr.Column, replace)
		if !changed {
			return expr
		}
		newExpr := expr.Clone().(*expression.CorrelatedColumn)
		newExpr.Data = expr.Data
		newExpr.Column = *newCol
		return newExpr
	case *expression.ScalarFunction:
		for i, arg := range expr.GetArgs() {
			expr.GetArgs()[i] = ResolveExprAndReplace(arg, replace)
		}
		return expr
	}
	return origin
}

// ResolveColumnAndReplace replaces columns fields of expressions by children logical plans.
func ResolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) *expression.Column {
	newCol, _ := resolveColumnAndReplace(origin, replace)
	return newCol
}

func resolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) (*expression.Column, bool) {
	dst := replace[string(origin.HashCode())]
	if dst != nil {
		// To avoid origin column is shared by multiple operators,
		// need to clone it before modification.
		newCol := dst.Clone().(*expression.Column)
		newCol.RetType, newCol.InOperand = origin.RetType, origin.InOperand
		return newCol, true
	}
	return origin, false
}

// ReplaceColumnOfExpr replaces column of expression by another LogicalProjection.
func ReplaceColumnOfExpr(expr expression.Expression, exprs []expression.Expression, schema *expression.Schema) expression.Expression {
	switch v := expr.(type) {
	case *expression.Column:
		idx := schema.ColumnIndex(v)
		if idx != -1 && idx < len(exprs) {
			return exprs[idx]
		}
	case *expression.ScalarFunction:
		for i := range v.GetArgs() {
			v.GetArgs()[i] = ReplaceColumnOfExpr(v.GetArgs()[i], exprs, schema)
		}
	}
	return expr
}

// IsColsAllFromOuterTable check whether the cols all from outer plan
func IsColsAllFromOuterTable(cols []*expression.Column, outerUniqueIDs *intset.FastIntSet) bool {
	// There are two cases "return false" here:
	// 1. If cols represents aggCols, then "len(cols) == 0" means not all aggregate functions are duplicate agnostic before.
	// 2. If cols represents parentCols, then "len(cols) == 0" means no parent logical plan of this join plan.
	if len(cols) == 0 {
		return false
	}
	for _, col := range cols {
		if !outerUniqueIDs.Has(int(col.UniqueID)) {
			return false
		}
	}
	return true
}

// IsColFromInnerTable check whether a column exists in the inner plan
func IsColFromInnerTable(cols []*expression.Column, innerUniqueIDs *intset.FastIntSet) bool {
	return slices.ContainsFunc(cols, func(col *expression.Column) bool {
		return innerUniqueIDs.Has(int(col.UniqueID))
	})
}

// CheckMaxOneRowCond check if a condition is the form of (uniqueKey = constant) or (uniqueKey =
// Correlated column), it returns at most one row.
func CheckMaxOneRowCond(eqColIDs map[int64]struct{}, childSchema *expression.Schema) bool {
	if len(eqColIDs) == 0 {
		return false
	}
	// We check `UniqueKeys` as well since the condition is `col = con | corr`, not `col <=> con | corr`.
	keys := make([]expression.KeyInfo, 0, len(childSchema.PKOrUK)+len(childSchema.NullableUK))
	keys = append(keys, childSchema.PKOrUK...)
	keys = append(keys, childSchema.NullableUK...)
	var maxOneRow bool
	for _, cols := range keys {
		maxOneRow = true
		for _, c := range cols {
			if _, ok := eqColIDs[c.UniqueID]; !ok {
				maxOneRow = false
				break
			}
		}
		if maxOneRow {
			return true
		}
	}
	return false
}

// CheckIndexCanBeKey checks whether an Index can be a Key in schema.
func CheckIndexCanBeKey(idx *model.IndexInfo, columns []*model.ColumnInfo, schema *expression.Schema) (uniqueKey, newKey expression.KeyInfo) {
	if !idx.Unique {
		return nil, nil
	}
	newKeyOK := true
	uniqueKeyOK := true
	for _, idxCol := range idx.Columns {
		// The columns of this index should all occur in column schema.
		// Since null value could be duplicate in unique key. So we check NotNull flag of every column.
		findUniqueKey := false
		for i, col := range columns {
			if idxCol.Name.L == col.Name.L {
				uniqueKey = append(uniqueKey, schema.Columns[i])
				findUniqueKey = true
				if newKeyOK {
					if !mysql.HasNotNullFlag(col.GetFlag()) {
						newKeyOK = false
						break
					}
					newKey = append(newKey, schema.Columns[i])
					break
				}
			}
		}
		if !findUniqueKey {
			newKeyOK = false
			uniqueKeyOK = false
			break
		}
	}
	if newKeyOK {
		return nil, newKey
	} else if uniqueKeyOK {
		return uniqueKey, nil
	}

	return nil, nil
}

// SetPredicatePushDownFlag is a hook for other packages to set rule flag.
var SetPredicatePushDownFlag func(uint64) uint64

// ApplyPredicateSimplificationForJoin is a hook for other packages to simplify the expression.
var ApplyPredicateSimplificationForJoin func(sctx base.PlanContext, predicates []expression.Expression,
	schema1, schema2 *expression.Schema,
	propagateConstant bool, filter expression.VaildConstantPropagationExpressionFuncType) []expression.Expression

// ApplyPredicateSimplification is a hook for other packages to simplify the expression.
var ApplyPredicateSimplification func(sctx base.PlanContext, predicates []expression.Expression,
	propagateConstant bool, filter expression.VaildConstantPropagationExpressionFuncType) []expression.Expression

var childSchemaSlicePool = zeropool.New[[]*expression.Schema](func() []*expression.Schema {
	return make([]*expression.Schema, 0, 4)
})

// BuildKeyInfoPortal recursively calls base.LogicalPlan's BuildKeyInfo method.
func BuildKeyInfoPortal(lp base.LogicalPlan) {
	for _, child := range lp.Children() {
		BuildKeyInfoPortal(child)
	}
	childSchema := childSchemaSlicePool.Get()
	childSchema = slices.Grow(childSchema, len(lp.Children()))
	defer func() {
		childSchemaSlicePool.Put(childSchema[:0])
	}()
	for _, child := range lp.Children() {
		childSchema = append(childSchema, child.Schema())
	}
	lp.BuildKeyInfo(lp.Schema(), childSchema)
}
