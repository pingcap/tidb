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
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

// ExplainExec represents an explain executor.
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

// prepareExplainInfo4DAGTask generates the following informations for every plan:
// ["id", "parents", "schema", "key", "task", "operator info"]
func (e *ExplainExec) prepareExplainInfo4DAGTask(p plan.PhysicalPlan, taskType string) error {
	parents := p.Parents()
	parentIDs := make([]string, 0, len(parents))
	for _, parent := range parents {
		parentIDs = append(parentIDs, parent.ID())
	}
	parentInfo := strings.Join(parentIDs, ",")
	columnInfo := p.Schema().ColumnInfo()
	uniqueKeyInfo := p.Schema().KeyInfo()
	operatorInfo := p.ExplainInfo()
	row := &Row{
		Data: types.MakeDatums(fmt.Sprintf("%s(%T)", p.ID(), p), parentInfo, columnInfo, uniqueKeyInfo, taskType, operatorInfo),
	}
	e.rows = append(e.rows, row)
	return nil
}

// prepareCopTaskInfo generates explain informations for cop-tasks.
// Only PhysicalTableReader, PhysicalIndexReader and PhysicalIndexLookUpReader have cop-tasks currently.
func (e *ExplainExec) prepareCopTaskInfo(plans []plan.PhysicalPlan) error {
	for _, p := range plans {
		err := e.prepareExplainInfo4DAGTask(p, "cop")
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// prepareRootTaskInfo generates explain informations for root-tasks.
func (e *ExplainExec) prepareRootTaskInfo(p plan.PhysicalPlan) error {
	e.explainedPlans[p.ID()] = true
	for _, child := range p.Children() {
		if e.explainedPlans[child.ID()] {
			continue
		}
		err := e.prepareRootTaskInfo(child.(plan.PhysicalPlan))
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
	err := e.prepareExplainInfo4DAGTask(p, "root")
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
			err := e.prepareRootTaskInfo(e.StmtPlan.(plan.PhysicalPlan))
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
