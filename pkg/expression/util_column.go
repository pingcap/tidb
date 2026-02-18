// Copyright 2016 PingCAP, Inc.
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

package expression

import (
	"cmp"
	"maps"
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// cowExprRef is a copy-on-write slice ref util using in `ColumnSubstitute`
// to reduce unnecessary allocation for Expression arguments array
type cowExprRef struct {
	ref []Expression
	new []Expression
}

// Set will allocate new array if changed flag true
func (c *cowExprRef) Set(i int, changed bool, val Expression) {
	if c.new != nil {
		c.new[i] = val
		return
	}
	if !changed {
		return
	}
	c.new = slices.Clone(c.ref)
	c.new[i] = val
}

// Result return the final reference
func (c *cowExprRef) Result() []Expression {
	if c.new != nil {
		return c.new
	}
	return c.ref
}

// Filter the input expressions, append the results to result.
func Filter(result []Expression, input []Expression, filter func(Expression) bool) []Expression {
	for _, e := range input {
		if filter(e) {
			result = append(result, e)
		}
	}
	return result
}

// FilterOutInPlace do the filtering out in place.
// The remained are the ones who doesn't match the filter, storing in the original slice.
// The filteredOut are the ones match the filter, storing in a new slice.
func FilterOutInPlace(input []Expression, filter func(Expression) bool) (remained, filteredOut []Expression) {
	for i := len(input) - 1; i >= 0; i-- {
		if filter(input[i]) {
			filteredOut = append(filteredOut, input[i])
			input = slices.Delete(input, i, i+1)
		}
	}
	return input, filteredOut
}

// ExtractDependentColumns extracts all dependent columns from a virtual column.
func ExtractDependentColumns(expr Expression) []*Column {
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	result := make([]*Column, 0, 8)
	return extractDependentColumns(result, expr)
}

func extractDependentColumns(result []*Column, expr Expression) []*Column {
	switch v := expr.(type) {
	case *Column:
		result = append(result, v)
		if v.VirtualExpr != nil {
			result = extractDependentColumns(result, v.VirtualExpr)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractDependentColumns(result, arg)
		}
	}
	return result
}

// ExtractColumns extracts all columns from an expression.
func ExtractColumns(expr Expression) []*Column {
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	tmp := make(map[int64]*Column, 8)
	extractColumns(tmp, expr, nil)
	result := slices.Collect(maps.Values(tmp))
	// The keys in a map are unordered, so to ensure stability, we need to sort them here.
	slices.SortFunc(result, func(a, b *Column) int {
		return cmp.Compare(a.UniqueID, b.UniqueID)
	})
	return result
}

// ExtractCorColumns extracts correlated column from given expression.
func ExtractCorColumns(expr Expression) (cols []*CorrelatedColumn) {
	switch v := expr.(type) {
	case *CorrelatedColumn:
		return []*CorrelatedColumn{v}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			cols = append(cols, ExtractCorColumns(arg)...)
		}
	}
	return
}

// ExtractColumnsFromExpressions is a more efficient version of ExtractColumns for batch operation.
// filter can be nil, or a function to filter the result column.
// It's often observed that the pattern of the caller like this:
//
// cols := ExtractColumns(...)
//
//	for _, col := range cols {
//	    if xxx(col) {...}
//	}
//
// Provide an additional filter argument, this can be done in one step.
// To avoid allocation for cols that not need.
func ExtractColumnsFromExpressions(exprs []Expression, filter func(*Column) bool) []*Column {
	if len(exprs) == 0 {
		return nil
	}
	m := make(map[int64]*Column, len(exprs))
	for _, expr := range exprs {
		extractColumns(m, expr, filter)
	}
	result := slices.Collect(maps.Values(m))
	// The keys in a map are unordered, so to ensure stability, we need to sort them here.
	slices.SortFunc(result, func(a, b *Column) int {
		return cmp.Compare(a.UniqueID, b.UniqueID)
	})
	return result
}

// ExtractColumnsMapFromExpressions it the same as ExtractColumnsFromExpressions, but return a map
func ExtractColumnsMapFromExpressions(filter func(*Column) bool, exprs ...Expression) map[int64]*Column {
	if len(exprs) == 0 {
		return nil
	}
	m := make(map[int64]*Column, len(exprs))
	for _, expr := range exprs {
		extractColumns(m, expr, filter)
	}
	return m
}

var uniqueIDToColumnMapPool = sync.Pool{
	New: func() any {
		return make(map[int64]*Column, 4)
	},
}

// GetUniqueIDToColumnMap gets map[int64]*Column map from the pool.
func GetUniqueIDToColumnMap() map[int64]*Column {
	return uniqueIDToColumnMapPool.Get().(map[int64]*Column)
}

// PutUniqueIDToColumnMap puts map[int64]*Column map back to the pool.
func PutUniqueIDToColumnMap(m map[int64]*Column) {
	clear(m)
	uniqueIDToColumnMapPool.Put(m)
}

// ExtractColumnsMapFromExpressionsWithReusedMap is the same as ExtractColumnsFromExpressions, but map can be reused.
func ExtractColumnsMapFromExpressionsWithReusedMap(m map[int64]*Column, filter func(*Column) bool, exprs ...Expression) {
	if len(exprs) == 0 {
		return
	}
	if m == nil {
		m = make(map[int64]*Column, len(exprs))
	}
	for _, expr := range exprs {
		extractColumns(m, expr, filter)
	}
}

// ExtractAllColumnsFromExpressionsInUsedSlices is the same as ExtractColumns. but it can reuse the memory.
func ExtractAllColumnsFromExpressionsInUsedSlices(reuse []*Column, filter func(*Column) bool, exprs ...Expression) []*Column {
	if len(exprs) == 0 {
		return nil
	}
	for _, expr := range exprs {
		reuse = extractColumnsSlices(reuse, expr, filter)
	}
	slices.SortFunc(reuse, func(a, b *Column) int {
		return cmp.Compare(a.UniqueID, b.UniqueID)
	})
	reuse = slices.CompactFunc(reuse, func(a, b *Column) bool {
		return a.UniqueID == b.UniqueID
	})
	return reuse
}

// ExtractAllColumnsFromExpressions is the same as ExtractColumnsFromExpressions. But this will not remove duplicates.
func ExtractAllColumnsFromExpressions(exprs []Expression, filter func(*Column) bool) []*Column {
	if len(exprs) == 0 {
		return nil
	}
	result := make([]*Column, 0, 8)
	for _, expr := range exprs {
		result = extractColumnsSlices(result, expr, filter)
	}
	return result
}

// ExtractColumnsSetFromExpressions is the same as ExtractColumnsFromExpressions
// it use the FastIntSet to save the Unique ID.
func ExtractColumnsSetFromExpressions(m *intset.FastIntSet, filter func(*Column) bool, exprs ...Expression) {
	if len(exprs) == 0 {
		return
	}
	intest.Assert(m != nil)
	for _, expr := range exprs {
		extractColumnsSet(m, expr, filter)
	}
}

func extractColumns(result map[int64]*Column, expr Expression, filter func(*Column) bool) {
	switch v := expr.(type) {
	case *Column:
		if filter == nil || filter(v) {
			result[v.UniqueID] = v
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			extractColumns(result, arg, filter)
		}
	}
}

func extractColumnsSlices(result []*Column, expr Expression, filter func(*Column) bool) []*Column {
	switch v := expr.(type) {
	case *Column:
		if filter == nil || filter(v) {
			result = append(result, v)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractColumnsSlices(result, arg, filter)
		}
	}
	return result
}

func extractColumnsSet(result *intset.FastIntSet, expr Expression, filter func(*Column) bool) {
	switch v := expr.(type) {
	case *Column:
		if filter == nil || filter(v) {
			result.Insert(int(v.UniqueID))
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			extractColumnsSet(result, arg, filter)
		}
	}
}

// ExtractEquivalenceColumns detects the equivalence from CNF exprs.
func ExtractEquivalenceColumns(result [][]Expression, exprs []Expression) [][]Expression {
	// exprs are CNF expressions, EQ condition only make sense in the top level of every expr.
	for _, expr := range exprs {
		result = extractEquivalenceColumns(result, expr)
	}
	return result
}

// FindUpperBound looks for column < constant or column <= constant and returns both the column
// and constant. It return nil, 0 if the expression is not of this form.
// It is used by derived Top N pattern and it is put here since it looks like
// a general purpose routine. Similar routines can be added to find lower bound as well.
func FindUpperBound(expr Expression) (*Column, int64) {
	scalarFunction, scalarFunctionOk := expr.(*ScalarFunction)
	if scalarFunctionOk {
		args := scalarFunction.GetArgs()
		if len(args) == 2 {
			col, colOk := args[0].(*Column)
			constant, constantOk := args[1].(*Constant)
			if colOk && constantOk && (scalarFunction.FuncName.L == ast.LT || scalarFunction.FuncName.L == ast.LE) {
				value, valueOk := constant.Value.GetValue().(int64)
				if valueOk {
					if scalarFunction.FuncName.L == ast.LT {
						return col, value - 1
					}
					return col, value
				}
			}
		}
	}
	return nil, 0
}

func extractEquivalenceColumns(result [][]Expression, expr Expression) [][]Expression {
	switch v := expr.(type) {
	case *ScalarFunction:
		// a==b, a<=>b, the latter one is evaluated to true when a,b are both null.
		if v.FuncName.L == ast.EQ || v.FuncName.L == ast.NullEQ {
			args := v.GetArgs()
			if len(args) == 2 {
				col1, ok1 := args[0].(*Column)
				col2, ok2 := args[1].(*Column)
				if ok1 && ok2 {
					result = append(result, []Expression{col1, col2})
				}
				col, ok1 := args[0].(*Column)
				scl, ok2 := args[1].(*ScalarFunction)
				if ok1 && ok2 {
					result = append(result, []Expression{col, scl})
				}
				col, ok1 = args[1].(*Column)
				scl, ok2 = args[0].(*ScalarFunction)
				if ok1 && ok2 {
					result = append(result, []Expression{col, scl})
				}
			}
			return result
		}
		if v.FuncName.L == ast.In {
			args := v.GetArgs()
			// only `col in (only 1 element)`, can we build an equivalence here.
			if len(args[1:]) == 1 {
				col1, ok1 := args[0].(*Column)
				col2, ok2 := args[1].(*Column)
				if ok1 && ok2 {
					result = append(result, []Expression{col1, col2})
				}
				col, ok1 := args[0].(*Column)
				scl, ok2 := args[1].(*ScalarFunction)
				if ok1 && ok2 {
					result = append(result, []Expression{col, scl})
				}
				col, ok1 = args[1].(*Column)
				scl, ok2 = args[0].(*ScalarFunction)
				if ok1 && ok2 {
					result = append(result, []Expression{col, scl})
				}
			}
			return result
		}
		// For Non-EQ function, we don't have to traverse down.
		// eg: (a=b or c=d) doesn't make any definitely equivalence assertion.
	}
	return result
}

// extractColumnsAndCorColumns extracts columns and correlated columns from `expr` and append them to `result`.
func extractColumnsAndCorColumns(result []*Column, expr Expression) []*Column {
	switch v := expr.(type) {
	case *Column:
		result = append(result, v)
	case *CorrelatedColumn:
		result = append(result, &v.Column)
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractColumnsAndCorColumns(result, arg)
		}
	}
	return result
}

// ExtractConstantEqColumnsOrScalar detects the constant equal relationship from CNF exprs.
func ExtractConstantEqColumnsOrScalar(ctx BuildContext, result []Expression, exprs []Expression) []Expression {
	// exprs are CNF expressions, EQ condition only make sense in the top level of every expr.
	for _, expr := range exprs {
		result = extractConstantEqColumnsOrScalar(ctx, result, expr)
	}
	return result
}

func extractConstantEqColumnsOrScalar(ctx BuildContext, result []Expression, expr Expression) []Expression {
	switch v := expr.(type) {
	case *ScalarFunction:
		if v.FuncName.L == ast.EQ || v.FuncName.L == ast.NullEQ {
			args := v.GetArgs()
			if len(args) == 2 {
				col, ok1 := args[0].(*Column)
				_, ok2 := args[1].(*Constant)
				if ok1 && ok2 {
					result = append(result, col)
				}
				col, ok1 = args[1].(*Column)
				_, ok2 = args[0].(*Constant)
				if ok1 && ok2 {
					result = append(result, col)
				}
				// take the correlated column as constant here.
				col, ok1 = args[0].(*Column)
				_, ok2 = args[1].(*CorrelatedColumn)
				if ok1 && ok2 {
					result = append(result, col)
				}
				col, ok1 = args[1].(*Column)
				_, ok2 = args[0].(*CorrelatedColumn)
				if ok1 && ok2 {
					result = append(result, col)
				}
				scl, ok1 := args[0].(*ScalarFunction)
				_, ok2 = args[1].(*Constant)
				if ok1 && ok2 {
					result = append(result, scl)
				}
				scl, ok1 = args[1].(*ScalarFunction)
				_, ok2 = args[0].(*Constant)
				if ok1 && ok2 {
					result = append(result, scl)
				}
				// take the correlated column as constant here.
				scl, ok1 = args[0].(*ScalarFunction)
				_, ok2 = args[1].(*CorrelatedColumn)
				if ok1 && ok2 {
					result = append(result, scl)
				}
				scl, ok1 = args[1].(*ScalarFunction)
				_, ok2 = args[0].(*CorrelatedColumn)
				if ok1 && ok2 {
					result = append(result, scl)
				}
			}
			return result
		}
		if v.FuncName.L == ast.In {
			args := v.GetArgs()
			allArgsIsConst := true
			// only `col in (all same const)`, can col be the constant column.
			// eg: a in (1, "1") does, while a in (1, '2') doesn't.
			guard := args[1]
			for i, v := range args[1:] {
				if _, ok := v.(*Constant); !ok {
					allArgsIsConst = false
					break
				}
				if i == 0 {
					continue
				}
				if !guard.Equal(ctx.GetEvalCtx(), v) {
					allArgsIsConst = false
					break
				}
			}
			if allArgsIsConst {
				if col, ok := args[0].(*Column); ok {
					result = append(result, col)
				} else if scl, ok := args[0].(*ScalarFunction); ok {
					result = append(result, scl)
				}
			}
			return result
		}
		// For Non-EQ function, we don't have to traverse down.
	}
	return result
}

// ExtractColumnsAndCorColumnsFromExpressions extracts columns and correlated columns from expressions and append them to `result`.
func ExtractColumnsAndCorColumnsFromExpressions(result []*Column, list []Expression) []*Column {
	for _, expr := range list {
		result = extractColumnsAndCorColumns(result, expr)
	}
	return result
}

// ExtractColumnSet extracts the different values of `UniqueId` for columns in expressions.
func ExtractColumnSet(exprs ...Expression) intset.FastIntSet {
	set := intset.NewFastIntSet()
	for _, expr := range exprs {
		extractColumnSet(expr, &set)
	}
	return set
}

func extractColumnSet(expr Expression, set *intset.FastIntSet) {
	switch v := expr.(type) {
	case *Column:
		set.Insert(int(v.UniqueID))
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			extractColumnSet(arg, set)
		}
	}
}

// SetExprColumnInOperand is used to set columns in expr as InOperand.
func SetExprColumnInOperand(expr Expression) Expression {
	switch v := expr.(type) {
	case *Column:
		col := v.Clone().(*Column)
		col.InOperand = true
		return col
	case *ScalarFunction:
		args := v.GetArgs()
		for i, arg := range args {
			args[i] = SetExprColumnInOperand(arg)
		}
	}
	return expr
}

// ColumnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
// TODO: remove this function and only use ColumnSubstituteImpl since this function swallows the error, which seems unsafe.
func ColumnSubstitute(ctx BuildContext, expr Expression, schema *Schema, newExprs []Expression) Expression {
	_, _, resExpr := ColumnSubstituteImpl(ctx, expr, schema, newExprs, false)
	return resExpr
}

// ColumnSubstituteAll substitutes the columns just like ColumnSubstitute, but we don't accept partial substitution.
// Only accept:
//
//	1: substitute them all once find col in schema.
//	2: nothing in expr can be substituted.
func ColumnSubstituteAll(ctx BuildContext, expr Expression, schema *Schema, newExprs []Expression) (bool, Expression) {
	_, hasFail, resExpr := ColumnSubstituteImpl(ctx, expr, schema, newExprs, true)
	return hasFail, resExpr
}

// ColumnSubstituteImpl tries to substitute column expr using newExprs,
// the newFunctionInternal is only called if its child is substituted
// @return bool means whether the expr has changed.
// @return bool means whether the expr should change (has the dependency in schema, while the corresponding expr has some compatibility), but finally fallback.
// @return Expression, the original expr or the changed expr, it depends on the first @return bool.
func ColumnSubstituteImpl(ctx BuildContext, expr Expression, schema *Schema, newExprs []Expression, fail1Return bool) (bool, bool, Expression) {
	switch v := expr.(type) {
	case *Column:
		id := schema.ColumnIndex(v)
		if id == -1 {
			return false, false, v
		}
		newExpr := newExprs[id]
		if v.InOperand {
			newExpr = SetExprColumnInOperand(newExpr)
		}
		return true, false, newExpr
	case *ScalarFunction:
		substituted := false
		hasFail := false
		if v.FuncName.L == ast.Cast || v.FuncName.L == ast.Grouping {
			var newArg Expression
			substituted, hasFail, newArg = ColumnSubstituteImpl(ctx, v.GetArgs()[0], schema, newExprs, fail1Return)
			if fail1Return && hasFail {
				return substituted, hasFail, v
			}
			if substituted {
				flag := v.RetType.GetFlag()
				var e Expression
				var err error
				if v.FuncName.L == ast.Cast {
					// If the newArg is a ScalarFunction(cast), BuildCastFunctionWithCheck will modify the newArg.RetType,
					// So we need to deep copy RetType.
					// TODO: Expression interface needs a deep copy method.
					if newArgFunc, ok := newArg.(*ScalarFunction); ok {
						newArgFunc.RetType = newArgFunc.RetType.DeepCopy()
						newArg = newArgFunc
					}
					e, err = BuildCastFunctionWithCheck(ctx, newArg, v.RetType, false, v.Function.IsExplicitCharset())
					terror.Log(err)
				} else {
					// for grouping function recreation, use clone (meta included) instead of newFunction
					e = v.Clone()
					e.(*ScalarFunction).Function.getArgs()[0] = newArg
				}
				e.SetCoercibility(v.Coercibility())
				e.GetType(ctx.GetEvalCtx()).SetFlag(flag)
				return true, false, e
			}
			return false, false, v
		}
		// If the collation of the column is PAD SPACE,
		// we can't propagate the constant to the length function.
		// For example, schema = ['name'], newExprs = ['a'], v = length(name).
		// We can't substitute name with 'a' in length(name) because the collation of name is PAD SPACE.
		// TODO: We will fix it here temporarily, and redesign the logic if we encounter more similar functions or situations later.
		// Fixed issue #53730
		if ctx.IsConstantPropagateCheck() && v.FuncName.L == ast.Length {
			arg0, isColumn := v.GetArgs()[0].(*Column)
			if isColumn {
				id := schema.ColumnIndex(arg0)
				if id != -1 {
					_, isConstant := newExprs[id].(*Constant)
					if isConstant {
						mappedNewColumnCollate := schema.Columns[id].GetStaticType().GetCollate()
						if mappedNewColumnCollate == charset.CollationUTF8MB4 ||
							mappedNewColumnCollate == charset.CollationUTF8 {
							return false, false, v
						}
					}
				}
			}
		}
		// cowExprRef is a copy-on-write util, args array allocation happens only
		// when expr in args is changed
		refExprArr := cowExprRef{v.GetArgs(), nil}
		oldCollEt, err := CheckAndDeriveCollationFromExprs(ctx, v.FuncName.L, v.RetType.EvalType(), v.GetArgs()...)
		if err != nil {
			logutil.BgLogger().Warn("Unexpected error happened during ColumnSubstitution", zap.Stack("stack"), zap.Error(err))
			return false, false, v
		}
		var tmpArgForCollCheck []Expression
		if collate.NewCollationEnabled() {
			tmpArgForCollCheck = make([]Expression, len(v.GetArgs()))
		}
		for idx, arg := range v.GetArgs() {
			changed, failed, newFuncExpr := ColumnSubstituteImpl(ctx, arg, schema, newExprs, fail1Return)
			if fail1Return && failed {
				return changed, failed, v
			}
			oldChanged := changed
			if collate.NewCollationEnabled() && changed {
				// Make sure the collation used by the ScalarFunction isn't changed and its result collation is not weaker than the collation used by the ScalarFunction.
				changed = false
				copy(tmpArgForCollCheck, refExprArr.Result())
				tmpArgForCollCheck[idx] = newFuncExpr
				newCollEt, err := CheckAndDeriveCollationFromExprs(ctx, v.FuncName.L, v.RetType.EvalType(), tmpArgForCollCheck...)
				if err != nil {
					logutil.BgLogger().Warn("Unexpected error happened during ColumnSubstitution", zap.Stack("stack"), zap.Error(err))
					return false, failed, v
				}
				if oldCollEt.Collation == newCollEt.Collation {
					if newFuncExpr.GetType(ctx.GetEvalCtx()).GetCollate() == arg.GetType(ctx.GetEvalCtx()).GetCollate() && newFuncExpr.Coercibility() == arg.Coercibility() {
						// It's safe to use the new expression, otherwise some cases in projection push-down will be wrong.
						changed = true
					} else {
						changed = checkCollationStrictness(oldCollEt.Collation, newFuncExpr.GetType(ctx.GetEvalCtx()).GetCollate())
					}
				}
			}
			hasFail = hasFail || failed || oldChanged != changed
			if fail1Return && oldChanged != changed {
				// Only when the oldChanged is true and changed is false, we will get here.
				// And this means there some dependency in this arg can be substituted with
				// given expressions, while it has some collation compatibility, finally we
				// fall back to use the origin args. (commonly used in projection elimination
				// in which fallback usage is unacceptable)
				return changed, true, v
			}
			refExprArr.Set(idx, changed, newFuncExpr)
			if changed {
				substituted = true
			}
		}
		if substituted {
			var newFunc Expression
			var err error
			switch v.FuncName.L {
			case ast.EQ:
				// keep order as col=value to avoid flaky test.
				args := refExprArr.Result()
				switch args[0].(type) {
				case *Constant:
					newFunc, err = NewFunction(ctx, v.FuncName.L, v.RetType, args[1], args[0])
				default:
					newFunc, err = NewFunction(ctx, v.FuncName.L, v.RetType, args[0], args[1])
				}
			default:
				newFunc, err = NewFunction(ctx, v.FuncName.L, v.RetType, refExprArr.Result()...)
			}
			if err != nil {
				return true, true, v
			}
			return true, hasFail, newFunc
		}
	}
	return false, false, expr
}
