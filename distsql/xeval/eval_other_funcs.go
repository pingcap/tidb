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

func (e *Evaluator) evalCaseWhen(expr *tipb.Expr) (d types.Datum, err error) {
	l := len(expr.Children)
	for i := 0; i < l-1; i += 2 {
		child, err := e.Eval(expr.Children[i])
		if err != nil {
			return d, errors.Trace(err)
		}
		if child.IsNull() {
			continue
		}
		x, err := child.ToBool()
		if err != nil {
			return d, errors.Trace(err)
		}
		if x == 1 {
			ans, err := e.Eval(expr.Children[i+1])
			if err != nil {
				return d, errors.Trace(err)
			}
			return ans, nil
		}
	}
	if l%2 == 1 { // Else statement
		ans, err := e.Eval(expr.Children[l-1])
		if err != nil {
			return d, errors.Trace(err)
		}
		return ans, nil
	}
	return d, nil
}

func (e *Evaluator) evalCoalesce(expr *tipb.Expr) (d types.Datum, err error) {
	for _, child := range expr.Children {
		d, err = e.Eval(child)
		if err != nil {
			break
		}
		if !d.IsNull() {
			break
		}
	}
	return d, errors.Trace(err)
}
