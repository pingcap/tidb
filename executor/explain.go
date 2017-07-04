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
	"strings"
)

// ExplainExec represents an explain executor.
// See https://dev.mysql.com/doc/refman/5.7/en/explain-output.html
type ExplainExec struct {
	baseExecutor

	StmtPlan plan.Plan
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

// TODO[jianzhang.zj]: avoid a plan being visited twice
// prepare ["id", "parents", "schema", "key info", "task type", "operator info"] for every plan
func (e *ExplainExec) prepareDAGExplainInfo(p plan.Plan) error {
	for _, child := range p.Children() {
		err := e.prepareDAGExplainInfo(child)
		if err != nil {
			return errors.Trace(err)
		}
	}

	parents := p.Parents()
	parentIDs := make([]string, 0, len(parents))
	for _, parent := range parents {
		parentIDs = append(parentIDs, parent.ID())
	}
	parentInfo := strings.Join(parentIDs, ",")
	columnInfo := p.Schema().ColumnInfo()
	uniqueKeyInfo := p.Schema().KeyInfo()
	taskTypeInfo := "unknown"
	operatorInfo := "unknown"

	row := &Row{
		Data: types.MakeDatums(p.ID(), parentInfo, columnInfo, uniqueKeyInfo, taskTypeInfo, operatorInfo),
	}
	e.rows = append(e.rows, row)

	return nil
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next() (*Row, error) {
	if e.cursor == 0 {
		if plan.UseDAGPlanBuilder(e.ctx) {
			e.StmtPlan.SetParents()
			err := e.prepareDAGExplainInfo(e.StmtPlan)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			err := e.prepareExplainInfo(e.StmtPlan, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
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
