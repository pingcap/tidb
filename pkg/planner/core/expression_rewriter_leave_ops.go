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

package core

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (er *expressionRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	stkLen := len(er.ctxStack)
	l := expression.GetRowLen(er.ctxStack[stkLen-lLen-1])
	for i := range lLen {
		if l != expression.GetRowLen(er.ctxStack[stkLen-lLen+i]) {
			er.err = expression.ErrOperandColumns.GenWithStackByArgs(l)
			return
		}
	}
	args := er.ctxStack[stkLen-lLen-1:]
	leftFt := args[0].GetType(er.sctx.GetEvalCtx())
	leftEt, leftIsNull := leftFt.EvalType(), leftFt.GetType() == mysql.TypeNull
	if leftIsNull {
		er.ctxStackPop(lLen + 1)
		er.ctxStackAppend(expression.NewNull(), types.EmptyName)
		return
	}
	if leftEt == types.ETInt {
		for i := 1; i < len(args); i++ {
			if c, ok := args[i].(*expression.Constant); ok {
				var isExceptional bool
				if expression.MaybeOverOptimized4PlanCache(er.sctx, c) {
					if c.GetType(er.sctx.GetEvalCtx()).EvalType() == types.ETInt {
						continue // no need to refine it
					}
					er.sctx.SetSkipPlanCache(fmt.Sprintf("'%v' may be converted to INT", c.StringWithCtx(er.sctx.GetEvalCtx(), errors.RedactLogDisable)))
					if err := expression.RemoveMutableConst(er.sctx, c); err != nil {
						er.err = err
						return
					}
				}
				args[i], isExceptional = expression.RefineComparedConstant(er.sctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					args[i] = c
				}
			}
		}
	}
	allSameType := true
	for _, arg := range args[1:] {
		if arg.GetType(er.sctx.GetEvalCtx()).GetType() != mysql.TypeNull && expression.GetAccurateCmpType(er.sctx.GetEvalCtx(), args[0], arg) != leftEt {
			allSameType = false
			break
		}
	}
	var function expression.Expression
	if allSameType && l == 1 && lLen > 1 {
		function = er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	} else {
		// If we rewrite IN to EQ, we need to decide what's the collation EQ uses.
		coll := er.deriveCollationForIn(l, lLen, args)
		if er.err != nil {
			return
		}
		er.castCollationForIn(l, lLen, stkLen, coll)
		eqFunctions := make([]expression.Expression, 0, lLen)
		for i := stkLen - lLen; i < stkLen; i++ {
			expr, err := er.constructBinaryOpFunction(args[0], er.ctxStack[i], ast.EQ)
			if err != nil {
				er.err = err
				return
			}
			eqFunctions = append(eqFunctions, expr)
		}
		function = expression.ComposeDNFCondition(er.sctx, eqFunctions...)
		if not {
			var err error
			function, err = er.newFunction(ast.UnaryNot, tp, function)
			if err != nil {
				er.err = err
				return
			}
		}
	}
	er.ctxStackPop(lLen + 1)
	er.ctxStackAppend(function, types.EmptyName)
}

// deriveCollationForIn derives collation for in expression.
// We don't handle the cases if the element is a tuple, such as (a, b, c) in ((x1, y1, z1), (x2, y2, z2)).
func (er *expressionRewriter) deriveCollationForIn(colLen int, _ int, args []expression.Expression) *expression.ExprCollation {
	if colLen == 1 {
		// a in (x, y, z) => coll[0]
		coll2, err := expression.CheckAndDeriveCollationFromExprs(er.sctx, "IN", types.ETInt, args...)
		er.err = err
		if er.err != nil {
			return nil
		}
		return coll2
	}
	return nil
}

// castCollationForIn casts collation info for arguments in the `in clause` to make sure the used collation is correct after we
// rewrite it to equal expression.
func (er *expressionRewriter) castCollationForIn(colLen int, elemCnt int, stkLen int, coll *expression.ExprCollation) {
	// We don't handle the cases if the element is a tuple, such as (a, b, c) in ((x1, y1, z1), (x2, y2, z2)).
	if colLen != 1 {
		return
	}
	if !collate.NewCollationEnabled() {
		// See https://github.com/pingcap/tidb/issues/52772
		// This function will apply CoercibilityExplicit to the casted expression, but some checks(during ColumnSubstituteImpl) is missed when the new
		// collation is disabled, then lead to panic.
		// To work around this issue, we can skip the function, it should be good since the collation is disabled.
		return
	}
	for i := stkLen - elemCnt; i < stkLen; i++ {
		// todo: consider refining the code and reusing expression.BuildCollationFunction here
		if er.ctxStack[i].GetType(er.sctx.GetEvalCtx()).EvalType() == types.ETString {
			rowFunc, ok := er.ctxStack[i].(*expression.ScalarFunction)
			if ok && rowFunc.FuncName.String() == ast.RowFunc {
				continue
			}
			// Don't convert it if it's charset is binary. So that we don't convert 0x12 to a string.
			if er.ctxStack[i].GetType(er.sctx.GetEvalCtx()).GetCollate() == coll.Collation {
				continue
			}
			tp := er.ctxStack[i].GetType(er.sctx.GetEvalCtx()).Clone()
			if er.ctxStack[i].GetType(er.sctx.GetEvalCtx()).Hybrid() {
				if !(expression.GetAccurateCmpType(er.sctx.GetEvalCtx(), er.ctxStack[stkLen-elemCnt-1], er.ctxStack[i]) == types.ETString) {
					continue
				}
				tp = types.NewFieldType(mysql.TypeVarString)
			} else if coll.Charset == charset.CharsetBin {
				// When cast character string to binary string, if we still use fixed length representation,
				// then 0 padding will be used, which can affect later execution.
				// e.g. https://github.com/pingcap/tidb/pull/35053#pullrequestreview-1008757770 gives an unexpected case.
				// On the other hand, we can not directly return origin expr back,
				// since we need binary collation to do string comparison later.
				// Here we use VarString type of cast, i.e `cast(a as binary)`, to avoid this problem.
				tp.SetType(mysql.TypeVarString)
			}
			tp.SetCharset(coll.Charset)
			tp.SetCollate(coll.Collation)
			er.ctxStack[i] = expression.BuildCastFunction(er.sctx, er.ctxStack[i], tp)
			er.ctxStack[i].SetCoercibility(expression.CoercibilityExplicit)
		}
	}
}

func (er *expressionRewriter) caseToExpression(v *ast.CaseExpr) {
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		argsLen++
	}
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-argsLen:]...)
	if er.err != nil {
		return
	}

	// value                          -> ctxStack[stkLen-argsLen-1]
	// when clause(condition, result) -> ctxStack[stkLen-argsLen:stkLen-1];
	// else clause                    -> ctxStack[stkLen-1]
	var args []expression.Expression
	if v.Value != nil {
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		value := er.ctxStack[stkLen-argsLen-1]
		args = make([]expression.Expression, 0, argsLen)
		for i := stkLen - argsLen; i < stkLen-1; i += 2 {
			arg, err := er.newFunction(ast.EQ, types.NewFieldType(mysql.TypeTiny), value, er.ctxStack[i])
			if err != nil {
				er.err = err
				return
			}
			args = append(args, arg)
			args = append(args, er.ctxStack[i+1])
		}
		if v.ElseClause != nil {
			args = append(args, er.ctxStack[stkLen-1])
		}
		argsLen++ // for trimming the value element later
	} else {
		// args:  condition1, result1,
		//        condition2, result2,
		//        ...
		//        else clause
		args = er.ctxStack[stkLen-argsLen:]
	}
	function, err := er.newFunction(ast.Case, &v.Type, args...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackPop(argsLen)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) patternLikeOrIlikeToExpression(v *ast.PatternLikeOrIlikeExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}

	char, col := er.sctx.GetCharsetInfo()
	var function expression.Expression
	fieldType := &types.FieldType{}
	isPatternExactMatch := false
	// Treat predicate 'like' or 'ilike' the same way as predicate '=' when it is an exact match and new collation is not enabled.
	if patExpression, ok := er.ctxStack[l-1].(*expression.Constant); ok && !collate.NewCollationEnabled() {
		patString, isNull, err := patExpression.EvalString(er.sctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			er.err = err
			return
		}
		if !isNull {
			patValue, patTypes := stringutil.CompilePattern(patString, v.Escape)
			if stringutil.IsExactMatch(patTypes) && er.ctxStack[l-2].GetType(er.sctx.GetEvalCtx()).EvalType() == types.ETString {
				op := ast.EQ
				if v.Not {
					op = ast.NE
				}
				types.DefaultTypeForValue(string(patValue), fieldType, char, col)
				function, er.err = er.constructBinaryOpFunction(er.ctxStack[l-2],
					&expression.Constant{Value: types.NewStringDatum(string(patValue)), RetType: fieldType},
					op)
				isPatternExactMatch = true
			}
		}
	}
	if !isPatternExactMatch {
		funcName := ast.Like
		if !v.IsLike {
			funcName = ast.Ilike
		}
		types.DefaultTypeForValue(int(v.Escape), fieldType, char, col)
		function = er.notToExpression(v.Not, funcName, &v.Type,
			er.ctxStack[l-2], er.ctxStack[l-1], &expression.Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: fieldType})
	}

	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) rowToScalarFunc(v *ast.RowExpr) {
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]expression.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		rows = append(rows, er.ctxStack[i])
	}
	er.ctxStackPop(length)
	function, err := er.newFunction(ast.RowFunc, rows[0].GetType(er.sctx.GetEvalCtx()), rows...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) wrapExpWithCast() (expr, lexp, rexp expression.Expression) {
	stkLen := len(er.ctxStack)
	expr, lexp, rexp = er.ctxStack[stkLen-3], er.ctxStack[stkLen-2], er.ctxStack[stkLen-1]
	var castFunc func(expression.BuildContext, expression.Expression) expression.Expression
	switch expression.ResolveType4Between(er.sctx.GetEvalCtx(), [3]expression.Expression{expr, lexp, rexp}) {
	case types.ETInt:
		expr = expression.WrapWithCastAsInt(er.sctx, expr, nil)
		lexp = expression.WrapWithCastAsInt(er.sctx, lexp, nil)
		rexp = expression.WrapWithCastAsInt(er.sctx, rexp, nil)
		return
	case types.ETReal:
		castFunc = expression.WrapWithCastAsReal
	case types.ETDecimal:
		castFunc = expression.WrapWithCastAsDecimal
	case types.ETString:
		castFunc = func(ctx expression.BuildContext, e expression.Expression) expression.Expression {
			// string kind expression do not need cast
			if e.GetType(er.sctx.GetEvalCtx()).EvalType().IsStringKind() {
				return e
			}
			return expression.WrapWithCastAsString(ctx, e)
		}
	case types.ETDuration:
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(mysql.TypeDuration))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(mysql.TypeDuration))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(mysql.TypeDuration))
		return
	case types.ETDatetime:
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(mysql.TypeDatetime))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(mysql.TypeDatetime))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(mysql.TypeDatetime))
		return
	default:
		return
	}

	expr = castFunc(er.sctx, expr)
	lexp = castFunc(er.sctx, lexp)
	rexp = castFunc(er.sctx, rexp)
	return
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		return
	}

	expr, lexp, rexp := er.wrapExpWithCast()

	coll, err := expression.CheckAndDeriveCollationFromExprs(er.sctx, "BETWEEN", types.ETInt, expr, lexp, rexp)
	er.err = err
	if er.err != nil {
		return
	}

	// Handle enum or set. We need to know their real type to decide whether to cast them.
	lt := expression.GetAccurateCmpType(er.sctx.GetEvalCtx(), expr, lexp)
	rt := expression.GetAccurateCmpType(er.sctx.GetEvalCtx(), expr, rexp)
	enumOrSetRealTypeIsStr := lt != types.ETInt && rt != types.ETInt

	expr = expression.BuildCastCollationFunction(er.sctx, expr, coll, enumOrSetRealTypeIsStr)
	lexp = expression.BuildCastCollationFunction(er.sctx, lexp, coll, enumOrSetRealTypeIsStr)
	rexp = expression.BuildCastCollationFunction(er.sctx, rexp, coll, enumOrSetRealTypeIsStr)

	var l, r expression.Expression
	l, er.err = expression.NewFunction(er.sctx, ast.GE, &v.Type, expr, lexp)
	if er.err != nil {
		return
	}
	r, er.err = expression.NewFunction(er.sctx, ast.LE, &v.Type, expr, rexp)
	if er.err != nil {
		return
	}
	function, err := er.newFunction(ast.LogicAnd, &v.Type, l, r)
	if err != nil {
		er.err = err
		return
	}
	if v.Not {
		function, err = er.newFunction(ast.UnaryNot, &v.Type, function)
		if err != nil {
			er.err = err
			return
		}
	}
	er.ctxStackPop(3)
	er.ctxStackAppend(function, types.EmptyName)
}

