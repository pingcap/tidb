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

func (e *Evaluator) evalStrcmp(expr *tipb.Expr) (types.Datum, error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	var d types.Datum
	if left.IsNull() || right.IsNull() {
		d.SetNull()
		return d, nil
	}
	sa, err := left.ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	sb, err := right.ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	ans := types.CompareString(sa, sb)
	d.SetInt64(int64(ans))
	return d, nil
}
