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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	log "github.com/sirupsen/logrus"
)

// FoldConstant does constant folding optimization on an expression excluding deferred ones.
func FoldConstant(expr Expression) Expression {
	e, _ := foldConstant(expr)
	return e
}

func foldConstant(expr Expression) (Expression, bool) {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return expr, false
		}
		args := x.GetArgs()
		canFold := true
		isDeferredConst := false
		for i := 0; i < len(args); i++ {
			foldedArg, isDeferred := foldConstant(args[i])
			x.GetArgs()[i] = foldedArg
			_, conOK := foldedArg.(*Constant)
			if !conOK {
				canFold = false
			}
			isDeferredConst = isDeferredConst || isDeferred
		}
		if !canFold {
			return expr, isDeferredConst
		}
		value, err := x.Eval(nil)
		if err != nil {
			log.Warnf("fold constant %s: %s", x.ExplainInfo(), err.Error())
			return expr, isDeferredConst
		}
		if isDeferredConst {
			return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x}, true
		}
		return &Constant{Value: value, RetType: x.RetType}, false
	case *Constant:
		if x.DeferredExpr != nil {
			value, err := x.DeferredExpr.Eval(nil)
			if err != nil {
				log.Warnf("fold constant %s: %s", x.ExplainInfo(), err.Error())
				return expr, true
			}
			return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x.DeferredExpr}, true
		}
	}
	return expr, false
}
