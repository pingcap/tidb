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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// specialFoldHandler stores functions for special UDF to constant fold
var specialFoldHandler = map[string]func(BuildContext, *ScalarFunction) (Expression, bool){}

func init() {
	specialFoldHandler = map[string]func(BuildContext, *ScalarFunction) (Expression, bool){
		ast.If:     ifFoldHandler,
		ast.Ifnull: ifNullFoldHandler,
		ast.Case:   caseWhenHandler,
		ast.IsNull: isNullHandler,
	}
}

// FoldConstant does constant folding optimization on an expression excluding deferred ones.
func FoldConstant(ctx BuildContext, expr Expression) Expression {
	e, _ := foldConstant(ctx, expr)
	// keep the original coercibility, charset, collation and repertoire values after folding
	e.SetCoercibility(expr.Coercibility())

	charset, collate := expr.GetType(ctx.GetEvalCtx()).GetCharset(), expr.GetType(ctx.GetEvalCtx()).GetCollate()
	e.GetType(ctx.GetEvalCtx()).SetCharset(charset)
	e.GetType(ctx.GetEvalCtx()).SetCollate(collate)
	e.SetRepertoire(expr.Repertoire())
	return e
}

func isNullHandler(ctx BuildContext, expr *ScalarFunction) (Expression, bool) {
	arg0 := expr.GetArgs()[0]
	if constArg, isConst := arg0.(*Constant); isConst {
		isDeferredConst := constArg.DeferredExpr != nil || constArg.ParamMarker != nil
		value, err := expr.Eval(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			// Failed to fold this expr to a constant, print the DEBUG log and
			// return the original expression to let the error to be evaluated
			// again, in that time, the error is returned to the client.
			logutil.BgLogger().Debug("fold expression to constant", zap.String("expression", expr.ExplainInfo(ctx.GetEvalCtx())), zap.Error(err))
			return expr, isDeferredConst
		}
		if isDeferredConst {
			return &Constant{Value: value, RetType: expr.RetType, DeferredExpr: expr}, true
		}
		return &Constant{Value: value, RetType: expr.RetType}, false
	}
	if mysql.HasNotNullFlag(arg0.GetType(ctx.GetEvalCtx()).GetFlag()) {
		return NewZero(), false
	}
	return expr, false
}

func ifFoldHandler(ctx BuildContext, expr *ScalarFunction) (Expression, bool) {
	args := expr.GetArgs()
	foldedArg0, _ := foldConstant(ctx, args[0])
	if constArg, isConst := foldedArg0.(*Constant); isConst {
		arg0, isNull0, err := constArg.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			// Failed to fold this expr to a constant, print the DEBUG log and
			// return the original expression to let the error to be evaluated
			// again, in that time, the error is returned to the client.
			logutil.BgLogger().Debug("fold expression to constant", zap.String("expression", expr.ExplainInfo(ctx.GetEvalCtx())), zap.Error(err))
			return expr, false
		}
		if !isNull0 && arg0 != 0 {
			return foldConstant(ctx, args[1])
		}
		return foldConstant(ctx, args[2])
	}
	// if the condition is not const, which branch is unknown to run, so directly return.
	return expr, false
}

func ifNullFoldHandler(ctx BuildContext, expr *ScalarFunction) (Expression, bool) {
	args := expr.GetArgs()
	foldedArg0, isDeferred := foldConstant(ctx, args[0])
	if constArg, isConst := foldedArg0.(*Constant); isConst {
		// Only check constArg.Value here. Because deferred expression is
		// evaluated to constArg.Value after foldConstant(args[0]), it's not
		// needed to be checked.
		if constArg.Value.IsNull() {
			foldedExpr, isConstant := foldConstant(ctx, args[1])

			// See https://github.com/pingcap/tidb/issues/51765. If the first argument can
			// be folded into NULL, the collation of IFNULL should be the same as the second
			// arguments.
			expr.GetType(ctx.GetEvalCtx()).SetCharset(args[1].GetType(ctx.GetEvalCtx()).GetCharset())
			expr.GetType(ctx.GetEvalCtx()).SetCollate(args[1].GetType(ctx.GetEvalCtx()).GetCollate())

			return foldedExpr, isConstant
		}
		return constArg, isDeferred
	}
	// if the condition is not const, which branch is unknown to run, so directly return.
	return expr, false
}

func caseWhenHandler(ctx BuildContext, expr *ScalarFunction) (Expression, bool) {
	args, l := expr.GetArgs(), len(expr.GetArgs())
	var isDeferred, isDeferredConst bool
	for i := 0; i < l-1; i += 2 {
		expr.GetArgs()[i], isDeferred = foldConstant(ctx, args[i])
		isDeferredConst = isDeferredConst || isDeferred
		if _, isConst := expr.GetArgs()[i].(*Constant); !isConst {
			// for no-const, here should return directly, because the following branches are unknown to be run or not
			return expr, false
		}
		// If the condition is const and true, and the previous conditions
		// has no expr, then the folded execution body is returned, otherwise
		// the arguments of the casewhen are folded and replaced.
		val, isNull, err := args[i].EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return expr, false
		}
		if val != 0 && !isNull {
			foldedExpr, isDeferred := foldConstant(ctx, args[i+1])
			isDeferredConst = isDeferredConst || isDeferred
			if _, isConst := foldedExpr.(*Constant); isConst {
				foldedExpr.GetType(ctx.GetEvalCtx()).SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
				return foldedExpr, isDeferredConst
			}
			return foldedExpr, isDeferredConst
		}
	}
	// If the number of arguments in casewhen is odd, and the previous conditions
	// is false, then the folded else execution body is returned. otherwise
	// the execution body of the else are folded and replaced.
	if l%2 == 1 {
		foldedExpr, isDeferred := foldConstant(ctx, args[l-1])
		isDeferredConst = isDeferredConst || isDeferred
		if _, isConst := foldedExpr.(*Constant); isConst {
			foldedExpr.GetType(ctx.GetEvalCtx()).SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
			return foldedExpr, isDeferredConst
		}
		return foldedExpr, isDeferredConst
	}
	return expr, isDeferredConst
}

func foldConstant(ctx BuildContext, expr Expression) (Expression, bool) {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return expr, false
		}
		if _, ok := x.Function.(*extensionFuncSig); ok {
			// we should not fold the extension function, because it may have a side effect.
			return expr, false
		}
		if function := specialFoldHandler[x.FuncName.L]; function != nil && !MaybeOverOptimized4PlanCache(ctx, []Expression{expr}) {
			return function(ctx, x)
		}

		args := x.GetArgs()
		argIsConst := make([]bool, len(args))
		hasNullArg := false
		allConstArg := true
		isDeferredConst := false
		for i := 0; i < len(args); i++ {
			switch x := args[i].(type) {
			case *Constant:
				isDeferredConst = isDeferredConst || x.DeferredExpr != nil || x.ParamMarker != nil
				argIsConst[i] = true
				hasNullArg = hasNullArg || x.Value.IsNull()
			default:
				allConstArg = false
			}
		}
		if !allConstArg {
			// try to optimize on the situation when not all arguments are const
			// for most functions, if one of the arguments are NULL, the result can be a constant (NULL or something else)
			//
			// NullEQ and ConcatWS are excluded, because they could have different value when the non-constant value is
			// 1 or NULL. For example, concat_ws(NULL, NULL) gives NULL, but concat_ws(1, NULL) gives ''
			if !hasNullArg || !ctx.IsInNullRejectCheck() || x.FuncName.L == ast.NullEQ || x.FuncName.L == ast.ConcatWS {
				return expr, isDeferredConst
			}
			constArgs := make([]Expression, len(args))
			for i, arg := range args {
				if argIsConst[i] {
					constArgs[i] = arg
				} else {
					constArgs[i] = NewOne()
				}
			}
			dummyScalarFunc, err := NewFunctionBase(ctx, x.FuncName.L, x.GetType(ctx.GetEvalCtx()), constArgs...)
			if err != nil {
				return expr, isDeferredConst
			}
			value, err := dummyScalarFunc.Eval(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return expr, isDeferredConst
			}
			if value.IsNull() {
				// This Constant is created to compose the result expression of EvaluateExprWithNull when InNullRejectCheck
				// is true. We just check whether the result expression is null or false and then let it die. Basically,
				// the constant is used once briefly and will not be retained for a long time. Hence setting DeferredExpr
				// of Constant to nil is ok.
				return &Constant{Value: value, RetType: x.RetType}, false
			}
			evalCtx := ctx.GetEvalCtx()
			if isTrue, err := value.ToBool(evalCtx.TypeCtx()); err == nil && isTrue == 0 {
				// This Constant is created to compose the result expression of EvaluateExprWithNull when InNullRejectCheck
				// is true. We just check whether the result expression is null or false and then let it die. Basically,
				// the constant is used once briefly and will not be retained for a long time. Hence setting DeferredExpr
				// of Constant to nil is ok.
				return &Constant{Value: value, RetType: x.RetType}, false
			}
			return expr, isDeferredConst
		}
		value, err := x.Eval(ctx.GetEvalCtx(), chunk.Row{})
		retType := x.RetType.Clone()
		if !hasNullArg {
			// set right not null flag for constant value
			switch value.Kind() {
			case types.KindNull:
				retType.DelFlag(mysql.NotNullFlag)
			default:
				retType.AddFlag(mysql.NotNullFlag)
			}
		}
		if err != nil {
			logutil.BgLogger().Debug("fold expression to constant", zap.String("expression", x.ExplainInfo(ctx.GetEvalCtx())), zap.Error(err))
			return expr, isDeferredConst
		}
		if isDeferredConst {
			return &Constant{Value: value, RetType: retType, DeferredExpr: x}, true
		}
		return &Constant{Value: value, RetType: retType}, false
	case *Constant:
		if x.ParamMarker != nil {
			return &Constant{
				Value:        x.ParamMarker.GetUserVar(ctx.GetEvalCtx()),
				RetType:      x.RetType,
				DeferredExpr: x.DeferredExpr,
				ParamMarker:  x.ParamMarker,
			}, true
		} else if x.DeferredExpr != nil {
			value, err := x.DeferredExpr.Eval(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				logutil.BgLogger().Debug("fold expression to constant", zap.String("expression", x.ExplainInfo(ctx.GetEvalCtx())), zap.Error(err))
				return expr, true
			}
			return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x.DeferredExpr}, true
		}
	}
	return expr, false
}
