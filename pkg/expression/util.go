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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
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
	result := make([]*Column, 0, 8)
	return extractColumns(result, expr, nil)
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
func ExtractColumnsFromExpressions(result []*Column, exprs []Expression, filter func(*Column) bool) []*Column {
	for _, expr := range exprs {
		result = extractColumns(result, expr, filter)
	}
	return result
}

func extractColumns(result []*Column, expr Expression, filter func(*Column) bool) []*Column {
	switch v := expr.(type) {
	case *Column:
		if filter == nil || filter(v) {
			result = append(result, v)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractColumns(result, arg, filter)
		}
	}
	return result
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
			newFunc, err := NewFunction(ctx, v.FuncName.L, v.RetType, refExprArr.Result()...)
			if err != nil {
				return true, true, v
			}
			return true, hasFail, newFunc
		}
	}
	return false, false, expr
}

// checkCollationStrictness check collation strictness-ship between `coll` and `newFuncColl`
// return true iff `newFuncColl` is not weaker than `coll`
func checkCollationStrictness(coll, newFuncColl string) bool {
	collGroupID, ok1 := CollationStrictnessGroup[coll]
	newFuncCollGroupID, ok2 := CollationStrictnessGroup[newFuncColl]

	if ok1 && ok2 {
		if collGroupID == newFuncCollGroupID {
			return true
		}

		if slices.Contains(CollationStrictness[collGroupID], newFuncCollGroupID) {
			return true
		}
	}

	return false
}

// getValidPrefix gets a prefix of string which can parsed to a number with base. the minimum base is 2 and the maximum is 36.
func getValidPrefix(s string, base int64) string {
	var (
		validLen int
		upper    rune
	)
	switch {
	case base >= 2 && base <= 9:
		upper = rune('0' + base)
	case base <= 36:
		upper = rune('A' + base - 10)
	default:
		return ""
	}
Loop:
	for i := range len(s) {
		c := rune(s[i])
		switch {
		case unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c):
			c = unicode.ToUpper(c)
			if c >= upper {
				break Loop
			}
			validLen = i + 1
		case c == '+' || c == '-':
			if i != 0 {
				break Loop
			}
		default:
			break Loop
		}
	}
	if validLen > 1 && s[0] == '+' {
		return s[1:validLen]
	}
	return s[:validLen]
}

// SubstituteCorCol2Constant will substitute correlated column to constant value which it contains.
// If the args of one scalar function are all constant, we will substitute it to constant.
func SubstituteCorCol2Constant(ctx BuildContext, expr Expression) (Expression, error) {
	switch x := expr.(type) {
	case *ScalarFunction:
		allConstant := true
		newArgs := make([]Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg, err := SubstituteCorCol2Constant(ctx, arg)
			if err != nil {
				return nil, err
			}
			_, ok := newArg.(*Constant)
			newArgs = append(newArgs, newArg)
			allConstant = allConstant && ok
		}
		if allConstant {
			val, err := x.Eval(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return nil, err
			}
			return &Constant{Value: val, RetType: x.GetType(ctx.GetEvalCtx())}, nil
		}
		var (
			err   error
			newSf Expression
		)
		if x.FuncName.L == ast.Cast {
			newSf = BuildCastFunction(ctx, newArgs[0], x.RetType)
		} else if x.FuncName.L == ast.Grouping {
			newSf = x.Clone()
			newSf.(*ScalarFunction).GetArgs()[0] = newArgs[0]
		} else {
			newSf, err = NewFunction(ctx, x.FuncName.L, x.GetType(ctx.GetEvalCtx()), newArgs...)
		}
		return newSf, err
	case *CorrelatedColumn:
		return &Constant{Value: *x.Data, RetType: x.GetType(ctx.GetEvalCtx())}, nil
	case *Constant:
		if x.DeferredExpr != nil {
			newExpr := FoldConstant(ctx, x)
			return &Constant{Value: newExpr.(*Constant).Value, RetType: x.GetType(ctx.GetEvalCtx())}, nil
		}
	}
	return expr, nil
}

func locateStringWithCollation(str, substr, coll string) int64 {
	collator := collate.GetCollator(coll)
	strKey := collator.KeyWithoutTrimRightSpace(str)
	subStrKey := collator.KeyWithoutTrimRightSpace(substr)

	index := bytes.Index(strKey, subStrKey)
	if index == -1 || index == 0 {
		return int64(index + 1)
	}

	// todo: we can use binary search to make it faster.
	count := int64(0)
	for {
		r, size := utf8.DecodeRuneInString(str)
		count++
		index -= len(collator.KeyWithoutTrimRightSpace(string(r)))
		if index <= 0 {
			return count + 1
		}
		str = str[size:]
	}
}

// timeZone2Duration converts timezone whose format should satisfy the regular condition
// `(^(+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^+13:00$)` to int for use by time.FixedZone().
func timeZone2int(tz string) int {
	sign := 1
	if strings.HasPrefix(tz, "-") {
		sign = -1
	}

	i := strings.Index(tz, ":")
	h, err := strconv.Atoi(tz[1:i])
	terror.Log(err)
	m, err := strconv.Atoi(tz[i+1:])
	terror.Log(err)
	return sign * ((h * 3600) + (m * 60))
}

var logicalOps = map[string]struct{}{
	ast.LT:                 {},
	ast.GE:                 {},
	ast.GT:                 {},
	ast.LE:                 {},
	ast.EQ:                 {},
	ast.NE:                 {},
	ast.UnaryNot:           {},
	ast.LogicAnd:           {},
	ast.LogicOr:            {},
	ast.LogicXor:           {},
	ast.In:                 {},
	ast.IsNull:             {},
	ast.IsTruthWithoutNull: {},
	ast.IsFalsity:          {},
	ast.Like:               {},
}

var oppositeOp = map[string]string{
	ast.LT:       ast.GE,
	ast.GE:       ast.LT,
	ast.GT:       ast.LE,
	ast.LE:       ast.GT,
	ast.EQ:       ast.NE,
	ast.NE:       ast.EQ,
	ast.LogicOr:  ast.LogicAnd,
	ast.LogicAnd: ast.LogicOr,
}

// a op b is equal to b symmetricOp a
var symmetricOp = map[opcode.Op]opcode.Op{
	opcode.LT:     opcode.GT,
	opcode.GE:     opcode.LE,
	opcode.GT:     opcode.LT,
	opcode.LE:     opcode.GE,
	opcode.EQ:     opcode.EQ,
	opcode.NE:     opcode.NE,
	opcode.NullEQ: opcode.NullEQ,
}

func pushNotAcrossArgs(ctx BuildContext, exprs []Expression, not bool) ([]Expression, bool) {
	newExprs := make([]Expression, 0, len(exprs))
	flag := false
	for _, expr := range exprs {
		newExpr, changed := pushNotAcrossExpr(ctx, expr, not)
		flag = changed || flag
		newExprs = append(newExprs, newExpr)
	}
	return newExprs, flag
}

// todo: consider more no precision-loss downcast cases.
func noPrecisionLossCastCompatible(cast, argCol *types.FieldType) bool {
	// now only consider varchar type and integer.
	if !(types.IsTypeVarchar(cast.GetType()) && types.IsTypeVarchar(argCol.GetType())) &&
		!(mysql.IsIntegerType(cast.GetType()) && mysql.IsIntegerType(argCol.GetType())) {
		// varchar type and integer on the storage layer is quite same, while the char type has its padding suffix.
		return false
	}
	if types.IsTypeVarchar(cast.GetType()) {
		// cast varchar function only bear the flen extension.
		if cast.GetFlen() < argCol.GetFlen() {
			return false
		}
		if !collate.CompatibleCollate(cast.GetCollate(), argCol.GetCollate()) {
			return false
		}
	} else {
		// For integers, we should ignore the potential display length represented by flen, using the default flen of the type.
		castFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(cast.GetType())
		originFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(argCol.GetType())
		// cast integer function only bear the flen extension and signed symbol unchanged.
		if castFlen < originFlen {
			return false
		}
		if mysql.HasUnsignedFlag(cast.GetFlag()) != mysql.HasUnsignedFlag(argCol.GetFlag()) {
			return false
		}
	}
	return true
}

func unwrapCast(sctx BuildContext, parentF *ScalarFunction, castOffset int) (Expression, bool) {
	_, collation := parentF.CharsetAndCollation()
	cast, ok := parentF.GetArgs()[castOffset].(*ScalarFunction)
	if !ok || cast.FuncName.L != ast.Cast {
		return parentF, false
	}
	// eg: if (cast(A) EQ const) with incompatible collation, even if cast is eliminated, the condition still can not be used to build range.
	if cast.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(cast.RetType.GetCollate(), collation) {
		return parentF, false
	}
	// 1-castOffset should be constant
	if _, ok := parentF.GetArgs()[1-castOffset].(*Constant); !ok {
		return parentF, false
	}

	// the direct args of cast function should be column.
	c, ok := cast.GetArgs()[0].(*Column)
	if !ok {
		return parentF, false
	}

	// current only consider varchar and integer
	if !noPrecisionLossCastCompatible(cast.RetType, c.RetType) {
		return parentF, false
	}

	// the column is covered by indexes, deconstructing it out.
	if castOffset == 0 {
		return NewFunctionInternal(sctx, parentF.FuncName.L, parentF.RetType, c, parentF.GetArgs()[1]), true
	}
	return NewFunctionInternal(sctx, parentF.FuncName.L, parentF.RetType, parentF.GetArgs()[0], c), true
}

// eliminateCastFunction will detect the original arg before and the cast type after, once upon
// there is no precision loss between them, current cast wrapper can be eliminated. For string
// type, collation is also taken into consideration. (mainly used to build range or point)
func eliminateCastFunction(sctx BuildContext, expr Expression) (_ Expression, changed bool) {
	f, ok := expr.(*ScalarFunction)
	if !ok {
		return expr, false
	}
	_, collation := expr.CharsetAndCollation()
	switch f.FuncName.L {
	case ast.LogicOr:
		dnfItems := FlattenDNFConditions(f)
		rmCast := false
		rmCastItems := make([]Expression, len(dnfItems))
		for i, dnfItem := range dnfItems {
			newExpr, curDowncast := eliminateCastFunction(sctx, dnfItem)
			rmCastItems[i] = newExpr
			if curDowncast {
				rmCast = true
			}
		}
		if rmCast {
			// compose the new DNF expression.
			return ComposeDNFCondition(sctx, rmCastItems...), true
		}
		return expr, false
	case ast.LogicAnd:
		cnfItems := FlattenCNFConditions(f)
		rmCast := false
		rmCastItems := make([]Expression, len(cnfItems))
		for i, cnfItem := range cnfItems {
			newExpr, curDowncast := eliminateCastFunction(sctx, cnfItem)
			rmCastItems[i] = newExpr
			if curDowncast {
				rmCast = true
			}
		}
		if rmCast {
			// compose the new CNF expression.
			return ComposeCNFCondition(sctx, rmCastItems...), true
		}
		return expr, false
	case ast.EQ, ast.NullEQ, ast.LE, ast.GE, ast.LT, ast.GT:
		// for case: eq(cast(test.t2.a, varchar(100), "aaaaa"), once t2.a is covered by index or pk, try deconstructing it out.
		if newF, ok := unwrapCast(sctx, f, 0); ok {
			return newF, true
		}
		// for case: eq("aaaaa"， cast(test.t2.a, varchar(100)), once t2.a is covered by index or pk, try deconstructing it out.
		if newF, ok := unwrapCast(sctx, f, 1); ok {
			return newF, true
		}
	case ast.In:
		// case for: cast(a<int> as bigint) in (1,2,3), we could deconstruct column 'a out directly.
		cast, ok := f.GetArgs()[0].(*ScalarFunction)
		if !ok || cast.FuncName.L != ast.Cast {
			return expr, false
		}
		// eg: if (cast(A) IN {const}) with incompatible collation, even if cast is eliminated, the condition still can not be used to build range.
		if cast.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(cast.RetType.GetCollate(), collation) {
			return expr, false
		}
		for _, arg := range f.GetArgs()[1:] {
			if _, ok := arg.(*Constant); !ok {
				return expr, false
			}
		}
		// the direct args of cast function should be column.
		c, ok := cast.GetArgs()[0].(*Column)
		if !ok {
			return expr, false
		}
		// current only consider varchar and integer
		if !noPrecisionLossCastCompatible(cast.RetType, c.RetType) {
			return expr, false
		}
		newArgs := []Expression{c}
		newArgs = append(newArgs, f.GetArgs()[1:]...)
		return NewFunctionInternal(sctx, f.FuncName.L, f.RetType, newArgs...), true
	}
	return expr, false
}

// pushNotAcrossExpr try to eliminate the NOT expr in expression tree.
// Input `not` indicates whether there's a `NOT` be pushed down.
// Output `changed` indicates whether the output expression differs from the
// input `expr` because of the pushed-down-not.
func pushNotAcrossExpr(ctx BuildContext, expr Expression, not bool) (_ Expression, changed bool) {
	if f, ok := expr.(*ScalarFunction); ok {
		switch f.FuncName.L {
		case ast.UnaryNot:
			child, err := wrapWithIsTrue(ctx, true, f.GetArgs()[0], true)
			if err != nil {
				return expr, false
			}
			var childExpr Expression
			childExpr, changed = pushNotAcrossExpr(ctx, child, !not)
			if !changed && !not {
				return expr, false
			}
			return childExpr, true
		case ast.LT, ast.GE, ast.GT, ast.LE, ast.EQ, ast.NE:
			if not {
				return NewFunctionInternal(ctx, oppositeOp[f.FuncName.L], f.GetType(ctx.GetEvalCtx()), f.GetArgs()...), true
			}
			newArgs, changed := pushNotAcrossArgs(ctx, f.GetArgs(), false)
			if !changed {
				return f, false
			}
			return NewFunctionInternal(ctx, f.FuncName.L, f.GetType(ctx.GetEvalCtx()), newArgs...), true
		case ast.LogicAnd, ast.LogicOr:
			var (
				newArgs []Expression
				changed bool
			)
			funcName := f.FuncName.L
			if not {
				newArgs, _ = pushNotAcrossArgs(ctx, f.GetArgs(), true)
				funcName = oppositeOp[f.FuncName.L]
				changed = true
			} else {
				newArgs, changed = pushNotAcrossArgs(ctx, f.GetArgs(), false)
			}
			if !changed {
				return f, false
			}
			return NewFunctionInternal(ctx, funcName, f.GetType(ctx.GetEvalCtx()), newArgs...), true
		}
	}
	if not {
		expr = NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), expr)
	}
	return expr, not
}

// GetExprInsideIsTruth get the expression inside the `istrue_with_null` and `istrue`.
// This is useful when handling expressions from "not" or "!", because we might wrap `istrue_with_null` or `istrue`
// when handling them. See pushNotAcrossExpr() and wrapWithIsTrue() for details.
func GetExprInsideIsTruth(expr Expression) Expression {
	if f, ok := expr.(*ScalarFunction); ok {
		switch f.FuncName.L {
		case ast.IsTruthWithNull, ast.IsTruthWithoutNull:
			return GetExprInsideIsTruth(f.GetArgs()[0])
		default:
			return expr
		}
	}
	return expr
}

// PushDownNot pushes the `not` function down to the expression's arguments.
func PushDownNot(ctx BuildContext, expr Expression) Expression {
	newExpr, _ := pushNotAcrossExpr(ctx, expr, false)
	return newExpr
}

// EliminateNoPrecisionLossCast remove the redundant cast function for range build convenience.
// 1: deeper cast embedded in other complicated function will not be considered.
// 2: cast args should be one for original base column and one for constant.
// 3: some collation compatibility and precision loss will be considered when remove this cast func.
func EliminateNoPrecisionLossCast(sctx BuildContext, expr Expression) Expression {
	newExpr, _ := eliminateCastFunction(sctx, expr)
	return newExpr
}

// ContainOuterNot checks if there is an outer `not`.
func ContainOuterNot(expr Expression) bool {
	return containOuterNot(expr, false)
}

// containOuterNot checks if there is an outer `not`.
// Input `not` means whether there is `not` outside `expr`
//
// eg.
//
//	not(0+(t.a == 1 and t.b == 2)) returns true
//	not(t.a) and not(t.b) returns false
func containOuterNot(expr Expression, not bool) bool {
	if f, ok := expr.(*ScalarFunction); ok {
		switch f.FuncName.L {
		case ast.UnaryNot:
			return containOuterNot(f.GetArgs()[0], true)
		case ast.IsTruthWithNull, ast.IsNull:
			return containOuterNot(f.GetArgs()[0], not)
		default:
			if not {
				return true
			}
			hasNot := false
			for _, expr := range f.GetArgs() {
				hasNot = hasNot || containOuterNot(expr, not)
				if hasNot {
					return hasNot
				}
			}
			return hasNot
		}
	}
	return false
}

// Contains tests if `exprs` contains `e`.
func Contains(ectx EvalContext, exprs []Expression, e Expression) bool {
	return slices.ContainsFunc(exprs, func(expr Expression) bool {
		if expr == nil {
			return e == nil
		}
		return e == expr || expr.Equal(ectx, e)
	})
}

// ExtractFiltersFromDNFs checks whether the cond is DNF. If so, it will get the extracted part and the remained part.
// The original DNF will be replaced by the remained part or just be deleted if remained part is nil.
// And the extracted part will be appended to the end of the original slice.
func ExtractFiltersFromDNFs(ctx BuildContext, conditions []Expression) []Expression {
	var allExtracted []Expression
	for i := len(conditions) - 1; i >= 0; i-- {
		if sf, ok := conditions[i].(*ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			extracted, remained := extractFiltersFromDNF(ctx, sf)
			allExtracted = append(allExtracted, extracted...)
			if remained == nil {
				conditions = slices.Delete(conditions, i, i+1)
			} else {
				conditions[i] = remained
			}
		}
	}
	return append(conditions, allExtracted...)
}

// extractFiltersFromDNF extracts the same condition that occurs in every DNF item and remove them from dnf leaves.
func extractFiltersFromDNF(ctx BuildContext, dnfFunc *ScalarFunction) ([]Expression, Expression) {
	dnfItems := FlattenDNFConditions(dnfFunc)
	codeMap := make(map[string]int)
	hashcode2Expr := make(map[string]Expression)
	for i, dnfItem := range dnfItems {
		innerMap := make(map[string]struct{})
		cnfItems := SplitCNFItems(dnfItem)
		for _, cnfItem := range cnfItems {
			code := cnfItem.HashCode()
			if i == 0 {
				codeMap[string(code)] = 1
				hashcode2Expr[string(code)] = cnfItem
			} else if _, ok := codeMap[string(code)]; ok {
				// We need this check because there may be the case like `select * from t, t1 where (t.a=t1.a and t.a=t1.a) or (something).
				// We should make sure that the two `t.a=t1.a` contributes only once.
				// TODO: do this out of this function.
				if _, ok = innerMap[string(code)]; !ok {
					codeMap[string(code)]++
					innerMap[string(code)] = struct{}{}
				}
			}
		}
	}
	// We should make sure that this item occurs in every DNF item.
	for hashcode, cnt := range codeMap {
		if cnt < len(dnfItems) {
			delete(hashcode2Expr, hashcode)
		}
	}
	if len(hashcode2Expr) == 0 {
		return nil, dnfFunc
	}
	newDNFItems := make([]Expression, 0, len(dnfItems))
	onlyNeedExtracted := false
	for _, dnfItem := range dnfItems {
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			code := cnfItem.HashCode()
			_, ok := hashcode2Expr[string(code)]
			if !ok {
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If the extracted part is just one leaf of the DNF expression. Then the value of the total DNF expression is
		// always the same with the value of the extracted part.
		if len(newCNFItems) == 0 {
			onlyNeedExtracted = true
			break
		}
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	extractedExpr := make([]Expression, 0, len(hashcode2Expr))
	for _, expr := range hashcode2Expr {
		extractedExpr = append(extractedExpr, expr)
	}
	if onlyNeedExtracted {
		return extractedExpr, nil
	}
	return extractedExpr, ComposeDNFCondition(ctx, newDNFItems...)
}

// DeriveRelaxedFiltersFromDNF given a DNF expression, derive a relaxed DNF expression which only contains columns
// in specified schema; the derived expression is a superset of original expression, i.e, any tuple satisfying
// the original expression must satisfy the derived expression. Return nil when the derived expression is universal set.
// A running example is: for schema of t1, `(t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2)` would be derived as
// `t1.a=1 or t1.a=2`, while `t1.a=1 or t2.a=1` would get nil.
func DeriveRelaxedFiltersFromDNF(ctx BuildContext, expr Expression, schema *Schema) Expression {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.LogicOr {
		return nil
	}
	dnfItems := FlattenDNFConditions(sf)
	newDNFItems := make([]Expression, 0, len(dnfItems))
	for _, dnfItem := range dnfItems {
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			if itemSF, ok := cnfItem.(*ScalarFunction); ok && itemSF.FuncName.L == ast.LogicOr {
				relaxedCNFItem := DeriveRelaxedFiltersFromDNF(ctx, cnfItem, schema)
				if relaxedCNFItem != nil {
					newCNFItems = append(newCNFItems, relaxedCNFItem)
				}
				// If relaxed expression for embedded DNF is universal set, just drop this CNF item
				continue
			}
			// This cnfItem must be simple expression now
			// If it cannot be fully covered by schema, just drop this CNF item
			if ExprFromSchema(cnfItem, schema) {
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If this DNF item involves no column of specified schema, the relaxed expression must be universal set
		if len(newCNFItems) == 0 {
			return nil
		}
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	return ComposeDNFCondition(ctx, newDNFItems...)
}

// GetRowLen gets the length if the func is row, returns 1 if not row.
func GetRowLen(e Expression) int {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		return len(f.GetArgs())
	}
	return 1
}

// CheckArgsNotMultiColumnRow checks the args are not multi-column row.
func CheckArgsNotMultiColumnRow(args ...Expression) error {
	for _, arg := range args {
		if GetRowLen(arg) != 1 {
			return ErrOperandColumns.GenWithStackByArgs(1)
		}
	}
	return nil
}

// GetFuncArg gets the argument of the function at idx.
func GetFuncArg(e Expression, idx int) Expression {
	if f, ok := e.(*ScalarFunction); ok {
		return f.GetArgs()[idx]
	}
	return nil
}

// PopRowFirstArg pops the first element and returns the rest of row.
// e.g. After this function (1, 2, 3) becomes (2, 3).
func PopRowFirstArg(ctx BuildContext, e Expression) (ret Expression, err error) {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		args := f.GetArgs()
		if len(args) == 2 {
			return args[1], nil
		}
		ret, err = NewFunction(ctx, ast.RowFunc, f.GetType(ctx.GetEvalCtx()), args[1:]...)
		return ret, err
	}
	return
}

// DatumToConstant generates a Constant expression from a Datum.
func DatumToConstant(d types.Datum, tp byte, flag uint) *Constant {
	t := types.NewFieldType(tp)
	t.AddFlag(flag)
	return &Constant{Value: d, RetType: t}
}

// ParamMarkerExpression generate a getparam function expression.
func ParamMarkerExpression(ctx BuildContext, v *driver.ParamMarkerExpr, needParam bool) (*Constant, error) {
	useCache := ctx.IsUseCache()
	tp := types.NewFieldType(mysql.TypeUnspecified)
	types.InferParamTypeFromDatum(&v.Datum, tp)
	value := &Constant{Value: v.Datum, RetType: tp}
	if useCache || needParam {
		value.ParamMarker = &ParamMarker{
			order: v.Order,
		}
	}
	return value, nil
}

// ParamMarkerInPrepareChecker checks whether the given ast tree has paramMarker and is in prepare statement.
type ParamMarkerInPrepareChecker struct {
	InPrepareStmt bool
}

// Enter implements Visitor Interface.
func (pc *ParamMarkerInPrepareChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch v := in.(type) {
	case *driver.ParamMarkerExpr:
		pc.InPrepareStmt = !v.InExecute
		return v, true
	}
	return in, false
}

// Leave implements Visitor Interface.
func (pc *ParamMarkerInPrepareChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// DisableParseJSONFlag4Expr disables ParseToJSONFlag for `expr` except Column.
// We should not *PARSE* a string as JSON under some scenarios. ParseToJSONFlag
// is 0 for JSON column yet(as well as JSON correlated column), so we can skip
// it. Moreover, Column.RetType refers to the infoschema, if we modify it, data
// race may happen if another goroutine read from the infoschema at the same
// time.
func DisableParseJSONFlag4Expr(ctx EvalContext, expr Expression) {
	if _, isColumn := expr.(*Column); isColumn {
		return
	}
	if _, isCorCol := expr.(*CorrelatedColumn); isCorCol {
		return
	}
	expr.GetType(ctx).SetFlag(expr.GetType(ctx).GetFlag() & ^mysql.ParseToJSONFlag)
}

// ConstructPositionExpr constructs PositionExpr with the given ParamMarkerExpr.
func ConstructPositionExpr(p *driver.ParamMarkerExpr) *ast.PositionExpr {
	return &ast.PositionExpr{P: p}
}

// PosFromPositionExpr generates a position value from PositionExpr.
func PosFromPositionExpr(ctx BuildContext, v *ast.PositionExpr) (int, bool, error) {
	if v.P == nil {
		return v.N, false, nil
	}
	value, err := ParamMarkerExpression(ctx, v.P.(*driver.ParamMarkerExpr), false)
	if err != nil {
		return 0, true, err
	}
	pos, isNull, err := GetIntFromConstant(ctx.GetEvalCtx(), value)
	if err != nil || isNull {
		return 0, true, err
	}
	return pos, false, nil
}

// GetStringFromConstant gets a string value from the Constant expression.
func GetStringFromConstant(ctx EvalContext, value Expression) (string, bool, error) {
	con, ok := value.(*Constant)
	if !ok {
		err := errors.Errorf("Not a Constant expression %+v", value)
		return "", true, err
	}
	str, isNull, err := con.EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		return "", true, err
	}
	return str, false, nil
}

// GetIntFromConstant gets an integer value from the Constant expression.
func GetIntFromConstant(ctx EvalContext, value Expression) (int, bool, error) {
	str, isNull, err := GetStringFromConstant(ctx, value)
	if err != nil || isNull {
		return 0, true, err
	}
	intNum, err := strconv.Atoi(str)
	if err != nil {
		return 0, true, nil
	}
	return intNum, false, nil
}

// BuildNotNullExpr wraps up `not(isnull())` for given expression.
func BuildNotNullExpr(ctx BuildContext, expr Expression) Expression {
	isNull := NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
	notNull := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), isNull)
	return notNull
}

// IsRuntimeConstExpr checks if a expr can be treated as a constant in **executor**.
func IsRuntimeConstExpr(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return false
		}
		for _, arg := range x.GetArgs() {
			if !IsRuntimeConstExpr(arg) {
				return false
			}
		}
		return true
	case *Column:
		return false
	case *Constant, *CorrelatedColumn:
		return true
	}
	return false
}

// CheckNonDeterministic checks whether the current expression contains a non-deterministic func.
func CheckNonDeterministic(e Expression) bool {
	switch x := e.(type) {
	case *Constant, *Column, *CorrelatedColumn:
		return false
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return true
		}
		if slices.ContainsFunc(x.GetArgs(), CheckNonDeterministic) {
			return true
		}
	}
	return false
}

// CheckFuncInExpr checks whether there's a given function in the expression.
func CheckFuncInExpr(e Expression, funcName string) bool {
	switch x := e.(type) {
	case *Constant, *Column, *CorrelatedColumn:
		return false
	case *ScalarFunction:
		if x.FuncName.L == funcName {
			return true
		}
		for _, arg := range x.GetArgs() {
			if CheckFuncInExpr(arg, funcName) {
				return true
			}
		}
	}
	return false
}

// IsMutableEffectsExpr checks if expr contains function which is mutable or has side effects.
func IsMutableEffectsExpr(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := mutableEffectsFunctions[x.FuncName.L]; ok {
			return true
		}
		if slices.ContainsFunc(x.GetArgs(), IsMutableEffectsExpr) {
			return true
		}
	case *Column:
	case *Constant:
		if x.DeferredExpr != nil {
			return IsMutableEffectsExpr(x.DeferredExpr)
		}
	}
	return false
}

// IsImmutableFunc checks whether this expression only consists of foldable functions.
// This expression can be evaluated by using `expr.Eval(chunk.Row{})` directly and the result won't change if it's immutable.
func IsImmutableFunc(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return false
		}
		if _, ok := mutableEffectsFunctions[x.FuncName.L]; ok {
			return false
		}
		for _, arg := range x.GetArgs() {
			if !IsImmutableFunc(arg) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

// RemoveDupExprs removes identical exprs. Not that if expr contains functions which
// are mutable or have side effects, we cannot remove it even if it has duplicates;
// if the plan is going to be cached, we cannot remove expressions containing `?` neither.
func RemoveDupExprs(exprs []Expression) []Expression {
	if len(exprs) <= 1 {
		return exprs
	}
	exists := make(map[string]struct{}, len(exprs))
	return slices.DeleteFunc(exprs, func(expr Expression) bool {
		key := string(expr.HashCode())
		if _, ok := exists[key]; !ok || IsMutableEffectsExpr(expr) {
			exists[key] = struct{}{}
			return false
		}
		return true
	})
}

// GetUint64FromConstant gets a uint64 from constant expression.
func GetUint64FromConstant(ctx EvalContext, expr Expression) (uint64, bool, bool) {
	con, ok := expr.(*Constant)
	if !ok {
		logutil.BgLogger().Warn("not a constant expression", zap.String("expression", expr.ExplainInfo(ctx)))
		return 0, false, false
	}
	dt := con.Value
	if con.ParamMarker != nil {
		var err error
		dt, err = con.ParamMarker.GetUserVar(ctx)
		if err != nil {
			logutil.BgLogger().Warn("get param failed", zap.Error(err))
			return 0, false, false
		}
	} else if con.DeferredExpr != nil {
		var err error
		dt, err = con.DeferredExpr.Eval(ctx, chunk.Row{})
		if err != nil {
			logutil.BgLogger().Warn("eval deferred expr failed", zap.Error(err))
			return 0, false, false
		}
	}
	switch dt.Kind() {
	case types.KindNull:
		return 0, true, true
	case types.KindInt64:
		val := dt.GetInt64()
		if val < 0 {
			return 0, false, false
		}
		return uint64(val), false, true
	case types.KindUint64:
		return dt.GetUint64(), false, true
	}
	return 0, false, false
}

// ContainVirtualColumn checks if the expressions contain a virtual column
func ContainVirtualColumn(exprs []Expression) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Column:
			if v.VirtualExpr != nil {
				return true
			}
		case *ScalarFunction:
			if ContainVirtualColumn(v.GetArgs()) {
				return true
			}
		}
	}
	return false
}

// ContainCorrelatedColumn checks if the expressions contain a correlated column
func ContainCorrelatedColumn(exprs []Expression) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *CorrelatedColumn:
			return true
		case *ScalarFunction:
			if ContainCorrelatedColumn(v.GetArgs()) {
				return true
			}
		}
	}
	return false
}

func jsonUnquoteFunctionBenefitsFromPushedDown(sf *ScalarFunction) bool {
	arg0 := sf.GetArgs()[0]
	// Only `->>` which parsed to JSONUnquote(CAST(JSONExtract() AS string)) can be pushed down to tikv
	if fChild, ok := arg0.(*ScalarFunction); ok {
		if fChild.FuncName.L == ast.Cast {
			if fGrand, ok := fChild.GetArgs()[0].(*ScalarFunction); ok {
				if fGrand.FuncName.L == ast.JSONExtract {
					return true
				}
			}
		}
	}
	return false
}

// ProjectionBenefitsFromPushedDown evaluates if the expressions can improve performance when pushed down to TiKV
// Projections are not pushed down to tikv by default, thus we need to check strictly here to avoid potential performance degradation.
// Note: virtual column is not considered here, since this function cares performance instead of functionality
func ProjectionBenefitsFromPushedDown(exprs []Expression, inputSchemaLen int) bool {
	// In debug usage, we need to force push down projections to tikv to check tikv expression behavior.
	failpoint.Inject("forcePushDownTiKV", func() {
		failpoint.Return(true)
	})
	allColRef := true
	colRefCount := 0
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Column:
			colRefCount = colRefCount + 1
			continue
		case *ScalarFunction:
			allColRef = false
			switch v.FuncName.L {
			case ast.JSONDepth, ast.JSONLength, ast.JSONType, ast.JSONValid, ast.JSONContains, ast.JSONContainsPath,
				ast.JSONExtract, ast.JSONKeys, ast.JSONSearch, ast.JSONMemberOf, ast.JSONOverlaps:
				continue
			case ast.JSONUnquote:
				if jsonUnquoteFunctionBenefitsFromPushedDown(v) {
					continue
				}
				return false
			default:
				return false
			}
		default:
			return false
		}
	}
	// For all col refs, only push down column pruning projections
	if allColRef {
		return colRefCount < inputSchemaLen
	}
	return true
}

// MaybeOverOptimized4PlanCache used to check whether an optimization can work
// for the statement when we enable the plan cache.
// In some situations, some optimizations maybe over-optimize and cache an
// overOptimized plan. The cached plan may not get the correct result when we
// reuse the plan for other statements.
// For example, `pk>=$a and pk<=$b` can be optimized to a PointGet when
// `$a==$b`, but it will cause wrong results when `$a!=$b`.
// So we need to do the check here. The check includes the following aspects:
// 1. Whether the plan cache switch is enable.
// 2. Whether the statement can be cached.
// 3. Whether the expressions contain a lazy constant.
// TODO: Do more careful check here.
func MaybeOverOptimized4PlanCache(ctx BuildContext, exprs ...Expression) bool {
	// If we do not enable plan cache, all the optimization can work correctly.
	if !ctx.IsUseCache() {
		return false
	}
	return containMutableConst(ctx.GetEvalCtx(), exprs)
}

// containMutableConst checks if the expressions contain a lazy constant.
func containMutableConst(ctx EvalContext, exprs []Expression) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Constant:
			if v.ParamMarker != nil || v.DeferredExpr != nil {
				return true
			}
		case *ScalarFunction:
			if containMutableConst(ctx, v.GetArgs()) {
				return true
			}
		}
	}
	return false
}

// RemoveMutableConst used to remove the `ParamMarker` and `DeferredExpr` in the `Constant` expr.
func RemoveMutableConst(ctx BuildContext, exprs ...Expression) (err error) {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Constant:
			v.ParamMarker = nil
			if v.DeferredExpr != nil { // evaluate and update v.Value to convert v to a complete immutable constant.
				// TODO: remove or hide DeferredExpr since it's too dangerous (hard to be consistent with v.Value all the time).
				v.Value, err = v.DeferredExpr.Eval(ctx.GetEvalCtx(), chunk.Row{})
				if err != nil {
					return err
				}
				v.DeferredExpr = nil
			}
			v.DeferredExpr = nil // do nothing since v.Value has already been evaluated in this case.
		case *ScalarFunction:
			err := RemoveMutableConst(ctx, v.GetArgs()...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	_   = iota
	kib = 1 << (10 * iota)
	mib = 1 << (10 * iota)
	gib = 1 << (10 * iota)
	tib = 1 << (10 * iota)
	pib = 1 << (10 * iota)
	eib = 1 << (10 * iota)
)

const (
	nano    = 1
	micro   = 1000 * nano
	milli   = 1000 * micro
	sec     = 1000 * milli
	minute  = 60 * sec
	hour    = 60 * minute
	dayTime = 24 * hour
)

// GetFormatBytes convert byte count to value with units.
func GetFormatBytes(bytes float64) string {
	var divisor float64
	var unit string

	bytesAbs := math.Abs(bytes)
	if bytesAbs >= eib {
		divisor = eib
		unit = "EiB"
	} else if bytesAbs >= pib {
		divisor = pib
		unit = "PiB"
	} else if bytesAbs >= tib {
		divisor = tib
		unit = "TiB"
	} else if bytesAbs >= gib {
		divisor = gib
		unit = "GiB"
	} else if bytesAbs >= mib {
		divisor = mib
		unit = "MiB"
	} else if bytesAbs >= kib {
		divisor = kib
		unit = "KiB"
	} else {
		divisor = 1
		unit = "bytes"
	}

	if divisor == 1 {
		return strconv.FormatFloat(bytes, 'f', 0, 64) + " " + unit
	}
	value := bytes / divisor
	if math.Abs(value) >= 100000.0 {
		return strconv.FormatFloat(value, 'e', 2, 64) + " " + unit
	}
	return strconv.FormatFloat(value, 'f', 2, 64) + " " + unit
}

// GetFormatNanoTime convert time in nanoseconds to value with units.
func GetFormatNanoTime(time float64) string {
	var divisor float64
	var unit string

	timeAbs := math.Abs(time)
	if timeAbs >= dayTime {
		divisor = dayTime
		unit = "d"
	} else if timeAbs >= hour {
		divisor = hour
		unit = "h"
	} else if timeAbs >= minute {
		divisor = minute
		unit = "min"
	} else if timeAbs >= sec {
		divisor = sec
		unit = "s"
	} else if timeAbs >= milli {
		divisor = milli
		unit = "ms"
	} else if timeAbs >= micro {
		divisor = micro
		unit = "us"
	} else {
		divisor = 1
		unit = "ns"
	}

	if divisor == 1 {
		return strconv.FormatFloat(time, 'f', 0, 64) + " " + unit
	}
	value := time / divisor
	if math.Abs(value) >= 100000.0 {
		return strconv.FormatFloat(value, 'e', 2, 64) + " " + unit
	}
	return strconv.FormatFloat(value, 'f', 2, 64) + " " + unit
}

// SQLDigestTextRetriever is used to find the normalized SQL statement text by SQL digests in statements_summary table.
// It's exported for test purposes. It's used by the `tidb_decode_sql_digests` builtin function, but also exposed to
// be used in other modules.
type SQLDigestTextRetriever struct {
	// SQLDigestsMap is the place to put the digests that's requested for getting SQL text and also the place to put
	// the query result.
	SQLDigestsMap map[string]string

	// Replace querying for test purposes.
	mockLocalData  map[string]string
	mockGlobalData map[string]string
	// There are two ways for querying information: 1) query specified digests by WHERE IN query, or 2) query all
	// information to avoid the too long WHERE IN clause. If there are more than `fetchAllLimit` digests needs to be
	// queried, the second way will be chosen; otherwise, the first way will be chosen.
	fetchAllLimit int
}

// NewSQLDigestTextRetriever creates a new SQLDigestTextRetriever.
func NewSQLDigestTextRetriever() *SQLDigestTextRetriever {
	return &SQLDigestTextRetriever{
		SQLDigestsMap: make(map[string]string),
		fetchAllLimit: 512,
	}
}

func (r *SQLDigestTextRetriever) runMockQuery(data map[string]string, inValues []any) (map[string]string, error) {
	if len(inValues) == 0 {
		return data, nil
	}
	res := make(map[string]string, len(inValues))
	for _, digest := range inValues {
		if text, ok := data[digest.(string)]; ok {
			res[digest.(string)] = text
		}
	}
	return res, nil
}

// runFetchDigestQuery runs query to the system tables to fetch the kv mapping of SQL digests and normalized SQL texts
// of the given SQL digests, if `inValues` is given, or all these mappings otherwise. If `queryGlobal` is false, it
// queries information_schema.statements_summary and information_schema.statements_summary_history; otherwise, it
// queries the cluster version of these two tables.
func (r *SQLDigestTextRetriever) runFetchDigestQuery(ctx context.Context, exec expropt.SQLExecutor, queryGlobal bool, inValues []any) (map[string]string, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	// If mock data is set, query the mock data instead of the real statements_summary tables.
	if !queryGlobal && r.mockLocalData != nil {
		return r.runMockQuery(r.mockLocalData, inValues)
	} else if queryGlobal && r.mockGlobalData != nil {
		return r.runMockQuery(r.mockGlobalData, inValues)
	}

	// Information in statements_summary will be periodically moved to statements_summary_history. Union them together
	// to avoid missing information when statements_summary is just cleared.
	stmt := "select digest, digest_text from information_schema.statements_summary union distinct " +
		"select digest, digest_text from information_schema.statements_summary_history"
	if queryGlobal {
		stmt = "select digest, digest_text from information_schema.cluster_statements_summary union distinct " +
			"select digest, digest_text from information_schema.cluster_statements_summary_history"
	}
	// Add the where clause if `inValues` is specified.
	if len(inValues) > 0 {
		stmt += " where digest in (" + strings.Repeat("%?,", len(inValues)-1) + "%?)"
	}

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, stmt, inValues...)
	if err != nil {
		return nil, err
	}

	res := make(map[string]string, len(rows))
	for _, row := range rows {
		res[row.GetString(0)] = row.GetString(1)
	}
	return res, nil
}

func (r *SQLDigestTextRetriever) updateDigestInfo(queryResult map[string]string) {
	for digest, text := range r.SQLDigestsMap {
		if len(text) > 0 {
			// The text of this digest is already known
			continue
		}
		sqlText, ok := queryResult[digest]
		if ok {
			r.SQLDigestsMap[digest] = sqlText
		}
	}
}

// RetrieveLocal tries to retrieve the SQL text of the SQL digests from local information.
func (r *SQLDigestTextRetriever) RetrieveLocal(ctx context.Context, exec expropt.SQLExecutor) error {
	if len(r.SQLDigestsMap) == 0 {
		return nil
	}

	var queryResult map[string]string
	if len(r.SQLDigestsMap) <= r.fetchAllLimit {
		inValues := make([]any, 0, len(r.SQLDigestsMap))
		for key := range r.SQLDigestsMap {
			inValues = append(inValues, key)
		}
		var err error
		queryResult, err = r.runFetchDigestQuery(ctx, exec, false, inValues)
		if err != nil {
			return errors.Trace(err)
		}

		if len(queryResult) == len(r.SQLDigestsMap) {
			r.SQLDigestsMap = queryResult
			return nil
		}
	} else {
		var err error
		queryResult, err = r.runFetchDigestQuery(ctx, exec, false, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateDigestInfo(queryResult)
	return nil
}

// RetrieveGlobal tries to retrieve the SQL text of the SQL digests from the information of the whole cluster.
func (r *SQLDigestTextRetriever) RetrieveGlobal(ctx context.Context, exec expropt.SQLExecutor) error {
	err := r.RetrieveLocal(ctx, exec)
	if err != nil {
		return errors.Trace(err)
	}

	// In some unit test environments it's unable to retrieve global info, and this function blocks it for tens of
	// seconds, which wastes much time during unit test. In this case, enable this failpoint to bypass retrieving
	// globally.
	failpoint.Inject("sqlDigestRetrieverSkipRetrieveGlobal", func() {
		failpoint.Return(nil)
	})

	var unknownDigests []any
	for k, v := range r.SQLDigestsMap {
		if len(v) == 0 {
			unknownDigests = append(unknownDigests, k)
		}
	}

	if len(unknownDigests) == 0 {
		return nil
	}

	var queryResult map[string]string
	if len(r.SQLDigestsMap) <= r.fetchAllLimit {
		queryResult, err = r.runFetchDigestQuery(ctx, exec, true, unknownDigests)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		queryResult, err = r.runFetchDigestQuery(ctx, exec, true, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateDigestInfo(queryResult)
	return nil
}

// ExprsToStringsForDisplay convert a slice of Expression to a slice of string using Expression.String(), and
// to make it better for display and debug, it also escapes the string to corresponding golang string literal,
// which means using \t, \n, \x??, \u????, ... to represent newline, control character, non-printable character,
// invalid utf-8 bytes and so on.
func ExprsToStringsForDisplay(ctx EvalContext, exprs []Expression) []string {
	strs := make([]string, len(exprs))
	for i, cond := range exprs {
		quote := `"`
		// We only need the escape functionality of strconv.Quote, the quoting is not needed,
		// so we trim the \" prefix and suffix here.
		strs[i] = strings.TrimSuffix(
			strings.TrimPrefix(
				strconv.Quote(cond.StringWithCtx(ctx, errors.RedactLogDisable)),
				quote),
			quote)
	}
	return strs
}

// HasColumnWithCondition tries to retrieve the expression (column or function) if it contains the target column.
func HasColumnWithCondition(e Expression, cond func(*Column) bool) bool {
	return hasColumnWithCondition(e, cond)
}

func hasColumnWithCondition(e Expression, cond func(*Column) bool) bool {
	switch v := e.(type) {
	case *Column:
		return cond(v)
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			if hasColumnWithCondition(arg, cond) {
				return true
			}
		}
	}
	return false
}

// ConstExprConsiderPlanCache indicates whether the expression can be considered as a constant expression considering planCache.
// If the expression is in plan cache, it should have a const level `ConstStrict` because it can be shared across statements.
// If the expression is not in plan cache, `ConstOnlyInContext` is enough because it is only used in one statement.
// Please notice that if the expression may be cached in other ways except plan cache, we should not use this function.
func ConstExprConsiderPlanCache(expr Expression, inPlanCache bool) bool {
	switch expr.ConstLevel() {
	case ConstStrict:
		return true
	case ConstOnlyInContext:
		return !inPlanCache
	default:
		return false
	}
}

// ExprsHasSideEffects checks if any of the expressions has side effects.
func ExprsHasSideEffects(exprs []Expression) bool {
	return slices.ContainsFunc(exprs, ExprHasSetVarOrSleep)
}

// ExprHasSetVarOrSleep checks if the expression has SetVar function or Sleep function.
func ExprHasSetVarOrSleep(expr Expression) bool {
	scalaFunc, isScalaFunc := expr.(*ScalarFunction)
	if !isScalaFunc {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar || scalaFunc.FuncName.L == ast.Sleep {
		return true
	}
	return slices.ContainsFunc(scalaFunc.GetArgs(), ExprHasSetVarOrSleep)
}

// ExecBinaryParam parse execute binary param arguments to datum slice.
func ExecBinaryParam(typectx types.Context, binaryParams []param.BinaryParam) (params []Expression, err error) {
	var (
		tmp any
	)

	params = make([]Expression, len(binaryParams))
	args := make([]types.Datum, len(binaryParams))
	for i := range args {
		tp := binaryParams[i].Tp
		isUnsigned := binaryParams[i].IsUnsigned

		switch tp {
		case mysql.TypeNull:
			var nilDatum types.Datum
			nilDatum.SetNull()
			args[i] = nilDatum
			continue

		case mysql.TypeTiny:
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(binaryParams[i].Val[0]))
			} else {
				args[i] = types.NewIntDatum(int64(int8(binaryParams[i].Val[0])))
			}
			continue

		case mysql.TypeShort, mysql.TypeYear:
			valU16 := binary.LittleEndian.Uint16(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU16))
			} else {
				args[i] = types.NewIntDatum(int64(int16(valU16)))
			}
			continue

		case mysql.TypeInt24, mysql.TypeLong:
			valU32 := binary.LittleEndian.Uint32(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU32))
			} else {
				args[i] = types.NewIntDatum(int64(int32(valU32)))
			}
			continue

		case mysql.TypeLonglong:
			valU64 := binary.LittleEndian.Uint64(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(valU64)
			} else {
				args[i] = types.NewIntDatum(int64(valU64))
			}
			continue

		case mysql.TypeFloat:
			args[i] = types.NewFloat32Datum(math.Float32frombits(binary.LittleEndian.Uint32(binaryParams[i].Val)))
			continue

		case mysql.TypeDouble:
			args[i] = types.NewFloat64Datum(math.Float64frombits(binary.LittleEndian.Uint64(binaryParams[i].Val)))
			continue

		case mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeDatetime:
			switch len(binaryParams[i].Val) {
			case 0:
				tmp = types.ZeroDatetimeStr
			case 4:
				_, tmp = binaryDate(0, binaryParams[i].Val)
			case 7:
				_, tmp = binaryDateTime(0, binaryParams[i].Val)
			case 11:
				_, tmp = binaryTimestamp(0, binaryParams[i].Val)
			case 13:
				_, tmp = binaryTimestampWithTZ(0, binaryParams[i].Val)
			default:
				err = mysql.ErrMalformPacket
				return
			}
			// TODO: generate the time datum directly
			var parseTime func(types.Context, string) (types.Time, error)
			switch tp {
			case mysql.TypeDate:
				parseTime = types.ParseDate
			case mysql.TypeDatetime:
				parseTime = types.ParseDatetime
			case mysql.TypeTimestamp:
				// To be compatible with MySQL, even the type of parameter is
				// TypeTimestamp, the return type should also be `Datetime`.
				parseTime = types.ParseDatetime
			}
			var time types.Time
			time, err = parseTime(typectx, tmp.(string))
			err = typectx.HandleTruncate(err)
			if err != nil {
				return
			}
			args[i] = types.NewDatum(time)
			continue

		case mysql.TypeDuration:
			fsp := 0
			switch len(binaryParams[i].Val) {
			case 0:
				tmp = "0"
			case 8:
				isNegative := binaryParams[i].Val[0]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				_, tmp = binaryDuration(1, binaryParams[i].Val, isNegative)
			case 12:
				isNegative := binaryParams[i].Val[0]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				_, tmp = binaryDurationWithMS(1, binaryParams[i].Val, isNegative)
				fsp = types.MaxFsp
			default:
				err = mysql.ErrMalformPacket
				return
			}
			// TODO: generate the duration datum directly
			var dur types.Duration
			dur, _, err = types.ParseDuration(typectx, tmp.(string), fsp)
			err = typectx.HandleTruncate(err)
			if err != nil {
				return
			}
			args[i] = types.NewDatum(dur)
			continue
		case mysql.TypeNewDecimal:
			if binaryParams[i].IsNull {
				args[i] = types.NewDecimalDatum(nil)
			} else {
				var dec types.MyDecimal
				err = typectx.HandleTruncate(dec.FromString(binaryParams[i].Val))
				if err != nil {
					return nil, err
				}
				args[i] = types.NewDecimalDatum(&dec)
			}
			continue
		case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if binaryParams[i].IsNull {
				args[i] = types.NewBytesDatum(nil)
			} else {
				args[i] = types.NewBytesDatum(binaryParams[i].Val)
			}
			continue
		case mysql.TypeUnspecified, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeGeometry, mysql.TypeBit:
			if !binaryParams[i].IsNull {
				tmp = string(hack.String(binaryParams[i].Val))
			} else {
				tmp = nil
			}
			args[i] = types.NewDatum(tmp)
			continue
		default:
			err = param.ErrUnknownFieldType.GenWithStack("stmt unknown field type %d", tp)
			return
		}
	}

	for i := range params {
		ft := new(types.FieldType)
		types.InferParamTypeFromUnderlyingValue(args[i].GetValue(), ft)
		params[i] = &Constant{Value: args[i], RetType: ft}
	}
	return
}

func binaryDate(pos int, paramValues []byte) (int, string) {
	year := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
	pos += 2
	month := paramValues[pos]
	pos++
	day := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func binaryDateTime(pos int, paramValues []byte) (int, string) {
	pos, date := binaryDate(pos, paramValues)
	hour := paramValues[pos]
	pos++
	minute := paramValues[pos]
	pos++
	second := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func binaryTimestamp(pos int, paramValues []byte) (int, string) {
	pos, dateTime := binaryDateTime(pos, paramValues)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func binaryTimestampWithTZ(pos int, paramValues []byte) (int, string) {
	pos, timestamp := binaryTimestamp(pos, paramValues)
	tzShiftInMin := int16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
	tzShiftHour := tzShiftInMin / 60
	tzShiftAbsMin := tzShiftInMin % 60
	if tzShiftAbsMin < 0 {
		tzShiftAbsMin = -tzShiftAbsMin
	}
	pos += 2
	return pos, fmt.Sprintf("%s%+02d:%02d", timestamp, tzShiftHour, tzShiftAbsMin)
}

func binaryDuration(pos int, paramValues []byte, isNegative uint8) (int, string) {
	sign := ""
	if isNegative == 1 {
		sign = "-"
	}
	days := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	hours := paramValues[pos]
	pos++
	minutes := paramValues[pos]
	pos++
	seconds := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s%d %02d:%02d:%02d", sign, days, hours, minutes, seconds)
}

func binaryDurationWithMS(pos int, paramValues []byte,
	isNegative uint8) (int, string) {
	pos, dur := binaryDuration(pos, paramValues, isNegative)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dur, microSecond)
}

// IsConstNull is used to check whether the expression is a constant null expression.
// For example, `1 > NULL` is a constant null expression.
// Now we just assume that the first argrument is a column,
// the second argument is a constant null.
func IsConstNull(expr Expression) bool {
	if e, ok := expr.(*ScalarFunction); ok {
		switch e.FuncName.L {
		case ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE:
			if constExpr, ok := e.GetArgs()[1].(*Constant); ok && constExpr.Value.IsNull() && constExpr.DeferredExpr == nil {
				return true
			}
		}
	}
	return false
}
