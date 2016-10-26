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

package xeval

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (e *Evaluator) evalIf(expr *tipb.Expr) (d types.Datum, err error) {
	if len(expr.Children) != 3 {
		err = ErrInvalid.Gen("IF needs 3 operands but got %d", len(expr.Children))
		return
	}
	child1, err := e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	child2, err := e.Eval(expr.Children[1])
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	child3, err := e.Eval(expr.Children[2])
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if child1.IsNull() {
		return child3, nil
	}
	x, err := child1.ToBool()
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if x == 1 {
		return child2, nil
	}
	return child3, nil
}

func (e *Evaluator) evalIfNull(expr *tipb.Expr) (types.Datum, error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if left.IsNull() {
		return right
	}
	return left
}

func (e *Evaluator) evalNullIf(expr *tipb.Expr) (types.Datum, error) {
	ans := types.Datum{}
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return ans, errors.Trace(err)
	}
	if left.IsNull() || right.IsNull() {
		return left, nil
	}
	x, err := left.CompareDatum(right)
	if err != nil {
		return ans, errors.Trace(err)
	}
	if x == 0 {
		return left, nil
	}
	ans.SetNull()
	return ans, nil
}


