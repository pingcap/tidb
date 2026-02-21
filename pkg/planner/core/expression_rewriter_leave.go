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
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
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

