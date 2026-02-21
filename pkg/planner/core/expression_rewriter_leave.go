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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

func initConstantRepertoire(ctx expression.EvalContext, c *expression.Constant) {
	c.SetRepertoire(expression.ASCII)
	if c.GetType(ctx).EvalType() == types.ETString {
		for _, b := range c.Value.GetBytes() {
			// if any character in constant is not ascii, set the repertoire to UNICODE.
			if b >= 0x80 {
				c.SetRepertoire(expression.UNICODE)
				break
			}
		}
	}
}

func (er *expressionRewriter) adjustUTF8MB4Collation(tp *types.FieldType) {
	if tp.GetFlag()&mysql.UnderScoreCharsetFlag > 0 && charset.CharsetUTF8MB4 == tp.GetCharset() {
		tp.SetCollate(er.sctx.GetDefaultCollationForUTF8MB4())
	}
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	defer func() {
		if len(er.astNodeStack) > 0 {
			er.astNodeStack = er.astNodeStack[:len(er.astNodeStack)-1]
		}
	}()
	if er.err != nil {
		return retNode, false
	}
	var inNode = originInNode
	if er.preprocess != nil {
		inNode = er.preprocess(inNode)
	}

	withPlanCtx := func(fn func(*exprRewriterPlanCtx), detail string) {
		planCtx, err := er.requirePlanCtx(inNode, detail)
		if err != nil {
			er.err = err
			return
		}
		fn(planCtx)
	}

	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause,
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr, *ast.ValuesExpr, *ast.WindowFuncExpr, *ast.TableNameExpr:
	case *driver.ValueExpr:
		// set right not null flag for constant value
		retType := v.Type.Clone()
		switch v.Datum.Kind() {
		case types.KindNull:
			retType.DelFlag(mysql.NotNullFlag)
		default:
			retType.AddFlag(mysql.NotNullFlag)
		}
		v.Datum.SetValue(v.Datum.GetValue(), retType)
		value := &expression.Constant{Value: v.Datum, RetType: retType}
		initConstantRepertoire(er.sctx.GetEvalCtx(), value)
		er.adjustUTF8MB4Collation(retType)
		if er.err != nil {
			return retNode, false
		}
		er.ctxStackAppend(value, types.EmptyName)
	case *driver.ParamMarkerExpr:
		er.toParamMarker(v)
	case *ast.VariableExpr:
		if v.IsSystem {
			withPlanCtx(func(planCtx *exprRewriterPlanCtx) {
				er.rewriteSystemVariable(planCtx, v)
			}, "accessing system variable requires plan context")
		} else {
			er.rewriteUserVariable(v)
		}
	case *ast.FuncCallExpr:
		switch v.FnName.L {
		case ast.Grouping:
			withPlanCtx(func(planCtx *exprRewriterPlanCtx) {
				er.funcCallToExpressionWithPlanCtx(planCtx, v)
			}, "grouping function requires plan context")
		default:
			if _, ok := expression.TryFoldFunctions[v.FnName.L]; ok {
				er.tryFoldCounter--
			}
			er.funcCallToExpression(v)
			if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
				er.disableFoldCounter--
			}
		}
	case *ast.TableName:
		er.toTable(v)
	case *ast.ColumnName:
		er.toColumn(v)
	case *ast.UnaryOperationExpr:
		er.unaryOpToExpression(v)
	case *ast.BinaryOperationExpr:
		if v.Op == opcode.LogicAnd || v.Op == opcode.LogicOr {
			er.tryFoldCounter--
		}
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		if _, ok := expression.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter--
		}
		er.caseToExpression(v)
		if _, ok := expression.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter--
		}
	case *ast.FuncCastExpr:
		if v.Tp.IsArray() && !er.allowBuildCastArray {
			er.err = expression.ErrNotSupportedYet.GenWithStackByArgs("Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions")
			return retNode, false
		}
		arg := er.ctxStack[len(er.ctxStack)-1]
		er.err = expression.CheckArgsNotMultiColumnRow(arg)
		if er.err != nil {
			return retNode, false
		}

		// check the decimal precision of "CAST(AS TIME)".
		er.err = er.checkTimePrecision(v.Tp)
		if er.err != nil {
			return retNode, false
		}

		castFunction, err := expression.BuildCastFunctionWithCheck(er.sctx, arg, v.Tp, false, v.ExplicitCharSet)
		if err != nil {
			er.err = err
			return retNode, false
		}
		if v.Tp.EvalType() == types.ETString {
			castFunction.SetCoercibility(expression.CoercibilityImplicit)
			if v.Tp.GetCharset() == charset.CharsetASCII {
				castFunction.SetRepertoire(expression.ASCII)
			} else {
				castFunction.SetRepertoire(expression.UNICODE)
			}
		} else {
			castFunction.SetCoercibility(expression.CoercibilityNumeric)
			castFunction.SetRepertoire(expression.ASCII)
		}

		er.ctxStack[len(er.ctxStack)-1] = castFunction
		er.ctxNameStk[len(er.ctxNameStk)-1] = types.EmptyName
	case *ast.JSONSumCrc32Expr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		jsonSumFunction, err := expression.BuildJSONSumCrc32FunctionWithCheck(er.sctx, arg, v.Tp)
		if err != nil {
			er.err = err
			return retNode, false
		}

		jsonSumFunction.SetCoercibility(expression.CoercibilityNumeric)
		jsonSumFunction.SetRepertoire(expression.ASCII)
		er.ctxStack[len(er.ctxStack)-1] = jsonSumFunction
		er.ctxNameStk[len(er.ctxNameStk)-1] = types.EmptyName
	case *ast.PatternLikeOrIlikeExpr:
		er.patternLikeOrIlikeToExpression(v)
	case *ast.PatternRegexpExpr:
		er.regexpToScalarFunc(v)
	case *ast.RowExpr:
		er.rowToScalarFunc(v)
	case *ast.PatternInExpr:
		if v.Sel == nil {
			er.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *ast.PositionExpr:
		withPlanCtx(func(planCtx *exprRewriterPlanCtx) {
			er.positionToScalarFunc(planCtx, v)
		}, "")
	case *ast.IsNullExpr:
		er.isNullToExpression(v)
	case *ast.IsTruthExpr:
		er.isTrueToScalarFunc(v)
	case *ast.DefaultExpr:
		if planCtx := er.planCtx; planCtx != nil {
			er.evalDefaultExprWithPlanCtx(planCtx, v)
		} else if er.sourceTable != nil {
			er.evalDefaultExprForTable(v, er.sourceTable)
		} else {
			er.err = errors.Errorf("Unsupported expr %T when source table not provided", v)
		}
	// TODO: Perhaps we don't need to transcode these back to generic integers/strings
	case *ast.TrimDirectionExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewIntDatum(int64(v.Direction)),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, types.EmptyName)
	case *ast.TimeUnitExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewStringDatum(v.Unit.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}, types.EmptyName)
	case *ast.GetFormatSelectorExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewStringDatum(v.Selector.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}, types.EmptyName)
	case *ast.SetCollationExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		if collate.NewCollationEnabled() {
			var collInfo *charset.Collation
			// TODO(bb7133): use charset.ValidCharsetAndCollation when its bug is fixed.
			if collInfo, er.err = collate.GetCollationByName(v.Collate); er.err != nil {
				break
			}
			chs := arg.GetType(er.sctx.GetEvalCtx()).GetCharset()
			// if the field is json, the charset is always utf8mb4.
			if arg.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeJSON {
				chs = mysql.UTF8MB4Charset
			}
			if chs != "" && collInfo.CharsetName != chs {
				er.err = charset.ErrCollationCharsetMismatch.GenWithStackByArgs(collInfo.Name, chs)
				break
			}
		}
		// SetCollationExpr sets the collation explicitly, even when the evaluation type of the expression is non-string.
		if _, ok := arg.(*expression.Column); ok || arg.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeJSON {
			if arg.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeEnum || arg.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeSet {
				er.err = plannererrors.ErrNotSupportedYet.GenWithStackByArgs("use collate clause for enum or set")
				break
			}
			// Wrap a cast here to avoid changing the original FieldType of the column expression.
			exprType := arg.GetType(er.sctx.GetEvalCtx()).Clone()
			// if arg type is json, we should cast it to longtext if there is collate clause.
			if arg.GetType(er.sctx.GetEvalCtx()).GetType() == mysql.TypeJSON {
				exprType = types.NewFieldType(mysql.TypeLongBlob)
				exprType.SetCharset(mysql.UTF8MB4Charset)
			}
			exprType.SetCollate(v.Collate)
			casted := expression.BuildCastFunction(er.sctx, arg, exprType)
			arg = casted
			er.ctxStackPop(1)
			er.ctxStackAppend(casted, types.EmptyName)
		} else {
			// For constant and scalar function, we can set its collate directly.
			arg.GetType(er.sctx.GetEvalCtx()).SetCollate(v.Collate)
		}
		er.ctxStack[len(er.ctxStack)-1].SetCoercibility(expression.CoercibilityExplicit)
		er.ctxStack[len(er.ctxStack)-1].SetCharsetAndCollation(arg.GetType(er.sctx.GetEvalCtx()).GetCharset(), arg.GetType(er.sctx.GetEvalCtx()).GetCollate())
	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	if er.err != nil {
		return retNode, false
	}
	return originInNode, true
}

// newFunctionWithInit chooses which expression.NewFunctionImpl() will be used.
func (er *expressionRewriter) newFunctionWithInit(funcName string, retType *types.FieldType, init expression.ScalarFunctionCallBack, args ...expression.Expression) (ret expression.Expression, err error) {
	if init != nil {
		ret, err = expression.NewFunctionWithInit(er.sctx, funcName, retType, init, args...)
	} else if er.disableFoldCounter > 0 {
		ret, err = expression.NewFunctionBase(er.sctx, funcName, retType, args...)
	} else if er.tryFoldCounter > 0 {
		ret, err = expression.NewFunctionTryFold(er.sctx, funcName, retType, args...)
	} else {
		ret, err = expression.NewFunction(er.sctx, funcName, retType, args...)
	}
	if err != nil {
		return
	}
	if scalarFunc, ok := ret.(*expression.ScalarFunction); ok {
		if er.planCtx != nil && er.planCtx.builder != nil {
			er.planCtx.builder.ctx.BuiltinFunctionUsageInc(scalarFunc.Function.PbCode().String())
		}
	}
	return
}

// newFunction is being redirected to newFunctionWithInit.
func (er *expressionRewriter) newFunction(funcName string, retType *types.FieldType, args ...expression.Expression) (ret expression.Expression, err error) {
	return er.newFunctionWithInit(funcName, retType, nil, args...)
}

func (*expressionRewriter) checkTimePrecision(ft *types.FieldType) error {
	if ft.EvalType() == types.ETDuration && ft.GetDecimal() > types.MaxFsp {
		return plannererrors.ErrTooBigPrecision.GenWithStackByArgs(ft.GetDecimal(), "CAST", types.MaxFsp)
	}
	return nil
}

func (er *expressionRewriter) useCache() bool {
	return er.sctx.IsUseCache()
}

func (er *expressionRewriter) rewriteUserVariable(v *ast.VariableExpr) {
	stkLen := len(er.ctxStack)
	name := strings.ToLower(v.Name)
	evalCtx := er.sctx.GetEvalCtx()
	if v.Value != nil {
		if !evalCtx.GetOptionalPropSet().Contains(exprctx.OptPropSessionVars) {
			er.err = errors.Errorf("rewriting user variable requires '%s' in evalCtx", exprctx.OptPropSessionVars.String())
			return
		}

		sessionVars, err := expropt.SessionVarsPropReader{}.GetSessionVars(evalCtx)
		if err != nil {
			er.err = err
			return
		}

		intest.Assert(er.planCtx == nil || sessionVars == er.planCtx.builder.ctx.GetSessionVars())

		tp := er.ctxStack[stkLen-1].GetType(er.sctx.GetEvalCtx())
		er.ctxStack[stkLen-1], er.err = er.newFunction(ast.SetVar, tp,
			expression.DatumToConstant(types.NewDatum(name), mysql.TypeString, 0),
			er.ctxStack[stkLen-1])
		er.ctxNameStk[stkLen-1] = types.EmptyName
		// Store the field type of the variable into SessionVars.UserVarTypes.
		// Normally we can infer the type from SessionVars.User, but we need SessionVars.UserVarTypes when
		// GetVar has not been executed to fill the SessionVars.Users.
		sessionVars.SetUserVarType(name, tp)
		return
	}
	tp, ok := evalCtx.GetUserVarsReader().GetUserVarType(name)
	if !ok {
		tp = types.NewFieldType(mysql.TypeVarString)
		tp.SetFlen(mysql.MaxFieldVarCharLength)
	}
	f, err := er.newFunction(ast.GetVar, tp, expression.DatumToConstant(types.NewStringDatum(name), mysql.TypeString, 0))
	if err != nil {
		er.err = err
		return
	}
	f.SetCoercibility(expression.CoercibilityImplicit)
	er.ctxStackAppend(f, types.EmptyName)
}

func (er *expressionRewriter) rewriteSystemVariable(planCtx *exprRewriterPlanCtx, v *ast.VariableExpr) {
	name := strings.ToLower(v.Name)
	sessionVars := planCtx.builder.ctx.GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		er.err = variable.ErrUnknownSystemVar.FastGenByArgs(name)
		if err := variable.CheckSysVarIsRemoved(name); err != nil {
			// Removed vars still return an error, but we customize it from
			// "unknown" to an explanation of why it is not supported.
			// This is important so users at least know they had the name correct.
			er.err = err
		}
		return
	}
	if sysVar.IsNoop && !vardef.EnableNoopVariables.Load() {
		// The variable does nothing, append a warning to the statement output.
		sessionVars.StmtCtx.AppendWarning(plannererrors.ErrGettingNoopVariable.FastGenByArgs(sysVar.Name))
	}
	if sem.IsEnabled() && sem.IsInvisibleSysVar(sysVar.Name) {
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_VARIABLES_ADMIN")
		planCtx.builder.visitInfo = appendDynamicVisitInfo(planCtx.builder.visitInfo, []string{"RESTRICTED_VARIABLES_ADMIN"}, false, err)
	}
	if v.ExplicitScope && !sysVar.HasNoneScope() {
		if v.IsGlobal && !(sysVar.HasGlobalScope() || sysVar.HasInstanceScope()) {
			er.err = variable.ErrIncorrectScope.GenWithStackByArgs(name, "SESSION")
			return
		}
		if v.IsInstance && !sysVar.HasInstanceScope() {
			er.err = variable.ErrIncorrectScope.GenWithStackByArgs(name, "SESSION or GLOBAL")
			return
		}
		if !v.IsGlobal && !v.IsInstance {
			if !sysVar.HasSessionScope() {
				er.err = variable.ErrIncorrectScope.GenWithStackByArgs(name, "GLOBAL")
				return
			}
			if sysVar.InternalSessionVariable {
				er.err = variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
				return
			}
		}
	}
	var val string
	var err error
	if sysVar.HasNoneScope() {
		val = sysVar.Value
	} else if v.IsGlobal || v.IsInstance {
		val, err = sessionVars.GetGlobalSystemVar(er.ctx, name)
	} else {
		val, err = sessionVars.GetSessionOrGlobalSystemVar(er.ctx, name)
	}
	if err != nil {
		er.err = err
		return
	}
	nativeVal, nativeType, nativeFlag := sysVar.GetNativeValType(val)
	e := expression.DatumToConstant(nativeVal, nativeType, nativeFlag)
	switch nativeType {
	case mysql.TypeVarString:
		charset, _ := sessionVars.GetSystemVar(vardef.CharacterSetConnection)
		e.GetType(er.sctx.GetEvalCtx()).SetCharset(charset)
		collate, _ := sessionVars.GetSystemVar(vardef.CollationConnection)
		e.GetType(er.sctx.GetEvalCtx()).SetCollate(collate)
	case mysql.TypeLong, mysql.TypeLonglong:
		e.GetType(er.sctx.GetEvalCtx()).SetCharset(charset.CharsetBin)
		e.GetType(er.sctx.GetEvalCtx()).SetCollate(charset.CollationBin)
	default:
		er.err = errors.Errorf("Not supported type(%x) in GetNativeValType() function", nativeType)
		return
	}
	er.ctxStackAppend(e, types.EmptyName)
}

func (er *expressionRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var op string
	switch v.Op {
	case opcode.Plus:
		// expression (+ a) is equal to a
		return
	case opcode.Minus:
		op = ast.UnaryMinus
	case opcode.BitNeg:
		op = ast.BitNeg
	case opcode.Not, opcode.Not2:
		op = ast.UnaryNot
	default:
		er.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	er.ctxStack[stkLen-1], er.err = er.newFunction(op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxNameStk[stkLen-1] = types.EmptyName
}

func (er *expressionRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var function expression.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, er.err = er.constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			v.Op.String())
	default:
		lLen := expression.GetRowLen(er.ctxStack[stkLen-2])
		rLen := expression.GetRowLen(er.ctxStack[stkLen-1])
		if lLen != 1 || rLen != 1 {
			er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
			return
		}
		function, er.err = er.newFunction(v.Op.String(), types.NewFieldType(mysql.TypeUnspecified), er.ctxStack[stkLen-2:]...)
	}
	if er.err != nil {
		return
	}
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...expression.Expression) expression.Expression {
	opFunc, err := er.newFunction(op, tp, args...)
	if err != nil {
		er.err = err
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = er.newFunction(ast.UnaryNot, tp, opFunc)
	if err != nil {
		er.err = err
		return nil
	}
	return opFunc
}

func (er *expressionRewriter) isNullToExpression(v *ast.IsNullExpr) {
	stkLen := len(er.ctxStack)
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) positionToScalarFunc(planCtx *exprRewriterPlanCtx, v *ast.PositionExpr) {
	intest.AssertNotNil(planCtx)
	pos := v.N
	str := strconv.Itoa(pos)
	if v.P != nil {
		stkLen := len(er.ctxStack)
		val := er.ctxStack[stkLen-1]
		intNum, isNull, err := expression.GetIntFromConstant(er.sctx.GetEvalCtx(), val)
		str = "?"
		if err == nil {
			if isNull {
				return
			}
			pos = intNum
			er.ctxStackPop(1)
		}
		er.err = err
	}
	if er.err == nil && pos > 0 && pos <= er.schema.Len() && !er.schema.Columns[pos-1].IsHidden {
		er.ctxStackAppend(er.schema.Columns[pos-1], er.names[pos-1])
	} else {
		er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(str, clauseMsg[planCtx.builder.curClause])
	}
}

func (er *expressionRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruthWithoutNull
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

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

