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
// See the License for the specific language governing permissions and
// limitations under the License.

package xeval

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression/xeval"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var jsonFunctions = map[tipb.ExprType]func([]types.Datum, *variable.StatementContext) (types.Datum, error){
	tipb.ExprType_JsonType:    xeval.JSONType,
	tipb.ExprType_JsonExtract: xeval.JSONExtract,
	tipb.ExprType_JsonUnquote: xeval.JSONUnquote,
	tipb.ExprType_JsonMerge:   xeval.JSONMerge,
	tipb.ExprType_JsonSet:     xeval.JSONSet,
	tipb.ExprType_JsonInsert:  xeval.JSONInsert,
	tipb.ExprType_JsonReplace: xeval.JSONReplace,
}

func (e *Evaluator) evalJSONFunctions(expr *tipb.Expr) (d types.Datum, _ error) {
	var args = make([]types.Datum, 0, len(expr.Children))
	for _, child := range expr.Children {
		arg, err := e.Eval(child)
		if err != nil {
			return d, errors.Trace(err)
		}
		args = append(args, arg)
	}
	if fn, ok := jsonFunctions[expr.GetTp()]; ok {
		return fn(args, e.StatementCtx)
	}
	return d, errors.Errorf("the ExprType %d is not implemented", expr.GetTp())
}
