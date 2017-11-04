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

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &ProjectionExec{}

// ProjectionExec represents a select fields executor.
type ProjectionExec struct {
	baseExecutor

	exprs []expression.Expression
}

// Next implements the Executor Next interface.
func (e *ProjectionExec) Next() (retRow Row, err error) {
	srcRow, err := e.children[0].Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	row := make([]types.Datum, 0, len(e.exprs))
	for _, expr := range e.exprs {
		val, err := expr.Eval(srcRow)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row = append(row, val)
	}
	return row, nil
}
