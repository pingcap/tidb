// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/planner/property"
)

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *basePhysicalPlan) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0 // the default implementation, the operator have no cost
	for _, child := range p.children {
		childCost, err := child.GetPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalSelection) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalProjection) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexLookUpReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexMergeReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableScan) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexScan) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexHashJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexMergeJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalApply) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalMergeJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalStreamAgg) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashAgg) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalSort) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTopN) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *BatchPointGetPlan) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PointGetPlan) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalUnionAll) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalExchangeReceiver) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}
