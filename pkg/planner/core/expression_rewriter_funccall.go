// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// rewriteFuncCall handles a FuncCallExpr and generates a customized function.
// It should return true if for the given FuncCallExpr a rewrite is performed so that original behavior is skipped.
// Otherwise it should return false to indicate (the caller) that original behavior needs to be performed.
func (er *expressionRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	switch v.FnName.L {
	// when column is not null, ifnull on such column can be optimized to a cast.
	case ast.Ifnull:
		if len(v.Args) != 2 {
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		lhs := er.ctxStack[stackLen-2]
		rhs := er.ctxStack[stackLen-1]
		col, isColumn := lhs.(*expression.Column)
		var isEnumSet bool
		if lhs.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeEnum || lhs.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeSet {
			isEnumSet = true
		}
		// if expr1 is a column with not null flag, then we can optimize it as a cast.
		if isColumn && !isEnumSet && mysql.HasNotNullFlag(col.RetType.GetFlag()) {
			retTp, err := expression.InferType4ControlFuncs(er.sctx, ast.Ifnull, lhs, rhs)
			if err != nil {
				er.err = err
				return true
			}
			retTp.AddFlag((lhs.GetType(er.sctx.GetEvalCtx()).GetFlag() & mysql.NotNullFlag) | (rhs.GetType(er.sctx.GetEvalCtx()).GetFlag() & mysql.NotNullFlag))
			if lhs.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeNull && rhs.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeNull {
				retTp.SetType(mysql.TypeNull)
				retTp.SetFlen(0)
				retTp.SetDecimal(0)
				types.SetBinChsClnFlag(retTp)
			}
			er.ctxStackPop(len(v.Args))
			casted := expression.BuildCastFunction(er.sctx, lhs, retTp)
			er.ctxStackAppend(casted, types.EmptyName)
			return true
		}

		return false
	case ast.Nullif:
		if len(v.Args) != 2 {
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		param1 := er.ctxStack[stackLen-2]
		param2 := er.ctxStack[stackLen-1]
		// param1 = param2
		funcCompare, err := er.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			er.err = err
			return true
		}
		// NULL
		nullTp := types.NewFieldType(mysql.TypeNull)
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNull)
		nullTp.SetFlen(flen)
		nullTp.SetDecimal(decimal)
		paramNull := &expression.Constant{
			Value:   types.NewDatum(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := er.newFunction(ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			er.err = err
			return true
		}
		er.ctxStackPop(len(v.Args))
		er.ctxStackAppend(funcIf, types.EmptyName)
		return true
	default:
		return false
	}
}

func (er *expressionRewriter) funcCallToExpressionWithPlanCtx(planCtx *exprRewriterPlanCtx, v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.err = expression.CheckArgsNotMultiColumnRow(args...)
	if er.err != nil {
		return
	}

	var function expression.Expression
	er.ctxStackPop(len(v.Args))
	switch v.FnName.L {
	case ast.Grouping:
		// grouping function should fetch the underlying grouping-sets meta and rewrite the args here.
		// eg: grouping(a) actually is try to find in which grouping-set that the column 'a' is remained,
		// collecting those gid as a collection and filling it into the grouping function meta. Besides,
		// the first arg of grouping function should be rewritten as gid column defined/passed by Expand
		// from the bottom up.
		intest.AssertNotNil(planCtx)
		if planCtx.rollExpand == nil {
			er.err = plannererrors.ErrInvalidGroupFuncUse
			er.ctxStackAppend(nil, types.EmptyName)
		} else {
			// whether there is some duplicate grouping sets, gpos is only be used in shuffle keys and group keys
			// rather than grouping function.
			// eg: rollup(a,a,b), the decided grouping sets are {a,a,b},{a,a,null},{a,null,null},{null,null,null}
			// for the second and third grouping set: {a,a,null} and {a,null,null}, a here is the col ref of original
			// column `a`. So from the static layer, this two grouping set are equivalent, we don't need to copy col
			// `a double times at the every beginning and resort to gpos to distinguish them.
			//  {col-a, col-b, gid, gpos}
			//  {a, b, 0, 1}, {a, null, 1, 2}, {a, null, 1, 3}, {null, null, 2, 4}
			// grouping function still only need to care about gid is enough, gpos what group and shuffle keys cared.
			if len(args) > 64 {
				er.err = plannererrors.ErrInvalidNumberOfArgs.GenWithStackByArgs("GROUPING", 64)
				er.ctxStackAppend(nil, types.EmptyName)
				return
			}
			// resolve grouping args in group by items or not.
			resolvedCols, err := planCtx.rollExpand.ResolveGroupingFuncArgsInGroupBy(args)
			if err != nil {
				er.err = err
				er.ctxStackAppend(nil, types.EmptyName)
				return
			}
			newArg := planCtx.rollExpand.GID.Clone()
			init := func(groupingFunc *expression.ScalarFunction) (*expression.ScalarFunction, error) {
				err = groupingFunc.Function.(*expression.BuiltinGroupingImplSig).SetMetadata(planCtx.rollExpand.GroupingMode, planCtx.rollExpand.GenerateGroupingMarks(resolvedCols))
				return groupingFunc, err
			}
			function, er.err = er.newFunctionWithInit(v.FnName.L, &v.Type, init, newArg)
			er.ctxStackAppend(function, types.EmptyName)
		}
	default:
		er.err = errors.Errorf("invalid function: %s", v.FnName.L)
		er.ctxStackAppend(nil, types.EmptyName)
	}
}

func (er *expressionRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.err = expression.CheckArgsNotMultiColumnRow(args...)
	if er.err != nil {
		return
	}

	if er.rewriteFuncCall(v) {
		return
	}

	var function expression.Expression
	er.ctxStackPop(len(v.Args))
	if ok := expression.IsDeferredFunctions(er.sctx, v.FnName.L); er.useCache() && ok {
		// When the expression is unix_timestamp and the number of argument is not zero,
		// we deal with it as normal expression.
		if v.FnName.L == ast.UnixTimestamp && len(v.Args) != 0 {
			function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
			er.ctxStackAppend(function, types.EmptyName)
		} else {
			function, er.err = expression.NewFunctionBase(er.sctx, v.FnName.L, &v.Type, args...)
			c := &expression.Constant{Value: types.NewDatum(nil), RetType: function.GetType(er.sctx.GetEvalCtx()).Clone(), DeferredExpr: function}
			er.ctxStackAppend(c, types.EmptyName)
		}
	} else {
		function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
		er.ctxStackAppend(function, types.EmptyName)
	}
}
