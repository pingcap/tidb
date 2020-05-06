// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/implementation"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
)

// Enforcer defines the interface for enforcer rules.
type Enforcer interface {
	// NewProperty generates relaxed property with the help of enforcer.
	NewProperty(prop *property.PhysicalProperty) (newProp *property.PhysicalProperty)
	// OnEnforce adds physical operators on top of child implementation to satisfy
	// required physical property.
	OnEnforce(reqProp *property.PhysicalProperty, child memo.Implementation) (impl memo.Implementation)
	// GetEnforceCost calculates cost of enforcing required physical property.
	GetEnforceCost(inputCount float64) float64
}

// GetEnforcerRules gets all candidate enforcer rules based
// on required physical property.
func GetEnforcerRules(prop *property.PhysicalProperty) (enforcers []Enforcer) {
	if !prop.IsEmpty() {
		enforcers = append(enforcers, orderEnforcer)
	}
	return
}

// OrderEnforcer enforces order property on child implementation.
type OrderEnforcer struct {
}

var orderEnforcer = &OrderEnforcer{}

// NewProperty removes order property from required physical property.
func (e *OrderEnforcer) NewProperty(prop *property.PhysicalProperty) (newProp *property.PhysicalProperty) {
	// Order property cannot be empty now.
	newProp = &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt}
	return
}

// OnEnforce adds sort operator to satisfy required order property.
func (e *OrderEnforcer) OnEnforce(reqProp *property.PhysicalProperty, child memo.Implementation) (impl memo.Implementation) {
	sort := &plannercore.PhysicalSort{
		ByItems: make([]*util.ByItems, 0, len(reqProp.Items)),
	}
	for _, item := range reqProp.Items {
		item := &util.ByItems{
			Expr: item.Col,
			Desc: item.Desc,
		}
		sort.ByItems = append(sort.ByItems, item)
	}
	sort.SetChildren(child.GetPlan())
	impl = implementation.NewSortImpl(sort)
	return
}

// GetEnforceCost calculates cost of sort operator.
func (e *OrderEnforcer) GetEnforceCost(inputCount float64) float64 {
	sort := &plannercore.PhysicalSort{}
	cost := sort.GetCost(inputCount)
	return cost
}
