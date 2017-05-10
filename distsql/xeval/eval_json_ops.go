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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (e *Evaluator) evalJsonOps(expr *tipb.Expr) (d types.Datum, err error) {
	switch op := expr.GetTp(); op {
	case tipb.ExprType_JsonExtract:
		jsonDatum, _ := e.Eval(expr.Children[0])
		jsonPath, _ := e.Eval(expr.Children[1])
		return expression.JsonExtract(jsonDatum, jsonPath)
	default:
		d.SetNull()
		return d, nil
	}
}
