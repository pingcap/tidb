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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/util/types"
)

// FoldConstant does constant folding optimization on an expression.
func FoldConstant(ctx context.Context, expr Expression) (Expression, error) {
	scalarFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return expr, nil
	}
	if _, isDynamic := evaluator.DynamicFuncs[scalarFunc.FuncName.L]; isDynamic {
		return expr, nil
	}
	args := scalarFunc.Args
	datums := make([]types.Datum, 0, len(args))
	canFold := true
	for i := 0; i < len(args); i++ {
		foldedArg, err := FoldConstant(ctx, args[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		scalarFunc.Args[i] = foldedArg
		if con, ok := foldedArg.(*Constant); ok {
			datums = append(datums, con.Value)
		} else {
			canFold = false
		}
	}
	if !canFold {
		return expr, nil
	}
	value, err := scalarFunc.Function(datums, ctx)
	if err != nil {
		log.Warnf("There may exist an error during constant folding. The function name is %s, args are %s", scalarFunc.FuncName, args)
		return nil, errors.Trace(err)
	}
	return &Constant{
		Value:   value,
		RetType: scalarFunc.RetType,
	}, nil
}
