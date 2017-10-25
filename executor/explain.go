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

package executor

import (
	"github.com/pingcap/tidb/expression"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	baseExecutor

	rows   []Row
	cursor int
}

// Schema implements the Executor Schema interface.
func (e *ExplainExec) Schema() *expression.Schema {
	return e.schema
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next() (Row, error) {
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	e.rows = nil
	return nil
}
