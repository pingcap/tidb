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
	"github.com/pingcap/tidb/types"
)

var _ Executor = &MaxOneRowExec{}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open() error {
	e.evaluated = false
	return errors.Trace(e.children[0].Open())
}

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next() (Row, error) {
	if !e.evaluated {
		e.evaluated = true
		srcRow, err := e.children[0].Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return make([]types.Datum, e.schema.Len()), nil
		}
		srcRow1, err := e.children[0].Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow1 != nil {
			return nil, errors.New("subquery returns more than 1 row")
		}
		return srcRow, nil
	}
	return nil, nil
}
