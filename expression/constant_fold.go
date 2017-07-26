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
	"github.com/ngaut/log"
)

// FoldConstant does constant folding optimization on an expression.
func FoldConstant(expr Expression) Expression {
	scalarFunc, ok := expr.(*ScalarFunction)
	if !ok || !scalarFunc.Function.isDeterministic() {
		return expr
	}
	args := scalarFunc.GetArgs()
	canFold := true
	for i := 0; i < len(args); i++ {
		foldedArg := FoldConstant(args[i])
		scalarFunc.GetArgs()[i] = foldedArg
		if _, ok := foldedArg.(*Constant); !ok {
			canFold = false
		}
	}
	if !canFold {
		return expr
	}
	value, err := scalarFunc.Eval(nil)
	if err != nil {
		log.Warnf("There may exist an error during constant folding. The function name is %s, args are %s", scalarFunc.FuncName, args)
		return expr
	}
	return &Constant{
		Value:   value,
		RetType: scalarFunc.RetType,
	}
}
