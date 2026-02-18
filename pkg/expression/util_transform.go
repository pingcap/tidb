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
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

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
	ast.Like:               {},
	ast.LogicAnd:           {},
	ast.LogicOr:            {},
	ast.LogicXor:           {},
	ast.In:                 {},
	ast.IsNull:             {},
	ast.IsFalsity:          {},
	ast.IsTruthWithoutNull: {},
	ast.IsTruthWithNull:    {},
	ast.NullEQ:             {},
	ast.Regexp:             {},
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

// CompareOpMap records all comparison operators.
var CompareOpMap = map[string]struct{}{
	ast.LT:     {},
	ast.GE:     {},
	ast.GT:     {},
	ast.LE:     {},
	ast.EQ:     {},
	ast.NE:     {},
	ast.NullEQ: {},
	ast.In:     {},
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
// Input `not` indicates whether there's a `NOT` to be pushed down from the parent.
// Logical operators can cancel double NOT; non-logical expressions are wrapped
// with is_true_with_null to preserve three-valued logic.
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
