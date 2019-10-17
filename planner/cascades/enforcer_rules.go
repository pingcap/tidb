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
	"math"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/implementation"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
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
func GetEnforcerRules(g *memo.Group, prop *property.PhysicalProperty) (enforcers []Enforcer) {
	if g.EngineType != memo.EngineTiDB {
		return
	}
	if !prop.IsEmpty() {
		orderEnforcer.group = g
		enforcers = append(enforcers, orderEnforcer)
	}
	return
}

// OrderEnforcer enforces order property on child implementation.
type OrderEnforcer struct {
	group *memo.Group
	sort  *plannercore.PhysicalSort
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
	lp := e.group.Equivalents.Front().Value.(*memo.GroupExpr).ExprNode
	sort := plannercore.PhysicalSort{
		ByItems: make([]*plannercore.ByItems, 0, len(reqProp.Items)),
	}.Init(lp.SCtx(), e.group.Prop.Stats, lp.SelectBlockOffset(), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	for _, item := range reqProp.Items {
		item := &plannercore.ByItems{
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
	lp := e.group.Equivalents.Front().Value.(*memo.GroupExpr).ExprNode
	// To calculate the cost of PhysicalSort, we need to create a PhysicalSort with a SessionCTX
	// which contains the SessionVars.
	sort := plannercore.PhysicalSort{}.Init(lp.SCtx(), nil, 0, nil)
	cost := sort.GetCost(inputCount)
	return cost
}
