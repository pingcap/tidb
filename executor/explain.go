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
	"encoding/json"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

// ExplainExec represents an explain executor.
// See https://dev.mysql.com/doc/refman/5.7/en/explain-output.html
type ExplainExec struct {
	StmtPlan plan.Plan
	schema   *expression.Schema
	rows     []*Row
	cursor   int
}

// Schema implements the Executor Schema interface.
func (e *ExplainExec) Schema() *expression.Schema {
	return e.schema
}

func (e *ExplainExec) prepareExplainInfo(p plan.Plan, parent plan.Plan) error {
	for _, child := range p.Children() {
		err := e.prepareExplainInfo(child, p)
		if err != nil {
			return errors.Trace(err)
		}
	}
	explain, err := json.MarshalIndent(p, "", "    ")
	if err != nil {
		return errors.Trace(err)
	}
	parentStr := ""
	if parent != nil {
		parentStr = parent.ID()
	}
	row := &Row{
		Data: types.MakeDatums(p.ID(), string(explain), parentStr),
	}
	e.rows = append(e.rows, row)
	return nil
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next() (*Row, error) {
	if e.cursor == 0 {
		err := e.prepareExplainInfo(e.StmtPlan, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
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
