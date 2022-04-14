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

// CalPlanCost calculates and returns the cost of current plan.
func (p *basePhysicalPlan) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0 // the default implementation, the operator have no cost
	for _, child := range p.children {
		childCost, err := child.CalPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCostInit = true
	return p.planCost, nil
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalSelection) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalProjection) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalIndexLookUpReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalIndexReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalTableReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalIndexMergeReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalTableScan) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalIndexScan) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalIndexJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalIndexHashJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalIndexMergeJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalApply) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalMergeJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalHashJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalStreamAgg) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalHashAgg) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalSort) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalTopN) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *BatchPointGetPlan) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PointGetPlan) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalUnionAll) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// CalPlanCost calculates and returns the cost of current plan.
func (p *PhysicalExchangeReceiver) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}
