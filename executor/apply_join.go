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
)

// ApplyJoinExec is the new logic of apply.
type ApplyJoinExec struct {
	join        joinExec
	outerSchema []*expression.CorrelatedColumn
	cursor      int
	resultRows  []Row
	schema      *expression.Schema
}

// Schema implements the Executor interface.
func (e *ApplyJoinExec) Schema() *expression.Schema {
	return e.schema
}

// Close implements the Executor interface.
func (e *ApplyJoinExec) Close() error {
	return nil
}

// Open implements the Executor interface.
func (e *ApplyJoinExec) Open() error {
	e.cursor = 0
	e.resultRows = nil
	return errors.Trace(e.join.Open())
}

// Next implements the Executor interface.
func (e *ApplyJoinExec) Next() (Row, error) {
	for {
		if e.cursor < len(e.resultRows) {
			row := e.resultRows[e.cursor]
			e.cursor++
			return row, nil
		}
		bigRow, match, err := e.join.fetchBigRow()
		if bigRow == nil || err != nil {
			return nil, errors.Trace(err)
		}
		for _, col := range e.outerSchema {
			*col.Data = bigRow[col.Index]
		}
		err = e.join.prepare()
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.resultRows, err = e.join.doJoin(bigRow, match)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.cursor = 0
	}
}
