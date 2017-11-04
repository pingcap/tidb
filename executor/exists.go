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
	"github.com/pingcap/tidb/util/types"
)

var _ Executor = &ExistsExec{}

// ExistsExec represents exists executor.
type ExistsExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *ExistsExec) Open() error {
	e.evaluated = false
	return errors.Trace(e.children[0].Open())
}

// Next implements the Executor Next interface.
// We always return one row with one column which has true or false value.
func (e *ExistsExec) Next() (Row, error) {
	if !e.evaluated {
		e.evaluated = true
		srcRow, err := e.children[0].Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return Row{types.NewDatum(srcRow != nil)}, nil
	}
	return nil, nil
}
