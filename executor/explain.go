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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

// ExplainExec represents an explain executor.
// See https://dev.mysql.com/doc/refman/5.7/en/explain-output.html
type ExplainExec struct {
	baseExecutor

	StmtPlan       plan.Plan
	rows           []*Row
	cursor         int
	explainedPlans map[string]bool
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

// prepareExplainInfo4DAGTask prepares these informations for every plan in the directed acyclic plan graph:
// ["id", "parents", "schema", "key info", "task type", "operator info"] for every plan
func (e *ExplainExec) prepareExplainInfo4DAGTask(p plan.Plan, taskType string) error {
	parents := p.Parents()
	parentIDs := make([]string, 0, len(parents))
	for _, parent := range parents {
		parentIDs = append(parentIDs, parent.ID())
	}
	parentInfo := strings.Join(parentIDs, ",")
	children := p.Children()
	childrenIDs := make([]string, 0, len(children))
	for _, child := range children {
		childrenIDs = append(childrenIDs, child.ID())
	}
	childrenInfo := strings.Join(childrenIDs, ",")
	columnInfo := p.Schema().ColumnInfo()
	uniqueKeyInfo := p.Schema().KeyInfo()
	operatorInfoBytes, err := json.Marshal(p)
	if err != nil {
		return errors.Trace(err)
	}
	operatorInfo := string(operatorInfoBytes)
	row := &Row{
		Data: types.MakeDatums(p.ID(), parentInfo, childrenInfo, columnInfo, uniqueKeyInfo, taskType, operatorInfo),
	}
	e.rows = append(e.rows, row)
	return nil
}

func (e *ExplainExec) prepareCopTaskInfo(plans []plan.PhysicalPlan) error {
	for _, p := range plans {
		err := e.prepareExplainInfo4DAGTask(p, "cop task")
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *ExplainExec) prepareRootTaskInfo(p plan.Plan) error {
	e.explainedPlans[p.ID()] = true
	for _, child := range p.Children() {
		if e.explainedPlans[child.ID()] {
			continue
		}
		err := e.prepareRootTaskInfo(child)
		if err != nil {
			return errors.Trace(err)
		}
	}
	switch copPlan := p.(type) {
	case *plan.PhysicalTableReader:
		e.prepareCopTaskInfo(copPlan.TablePlans)
	case *plan.PhysicalIndexReader:
		e.prepareCopTaskInfo(copPlan.IndexPlans)
	case *plan.PhysicalIndexLookUpReader:
		e.prepareCopTaskInfo(copPlan.IndexPlans)
		e.prepareCopTaskInfo(copPlan.TablePlans)
	}
	err := e.prepareExplainInfo4DAGTask(p, "root task")
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next() (*Row, error) {
	if e.cursor == 0 {
		if plan.UseDAGPlanBuilder(e.ctx) {
			e.StmtPlan.SetParents()
			e.explainedPlans = map[string]bool{}
			err := e.prepareRootTaskInfo(e.StmtPlan)
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
