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
	"github.com/pingcap/tidb/planner/property"
)

// Enforcer defines the interface for enforcer rules.
type Enforcer interface {
	// NewProperties generates relaxed properties with the help of enforcer.
	NewProperties(prop *property.PhysicalProperty) (newProps []*property.PhysicalProperty)
	// OnEnforce adds physical operators on top of child implementation to satisfy
	// required physical property. Newly added operators by this method should have
	// calculated cost. The group parameter is for cost computation.
	OnEnforce(child Implementation, group *Group) (impl Implementation)
}

// GetEnforcerRules gets all candidate enforcer rules based
// on required physical property.
func GetEnforcerRules(prop *property.PhysicalProperty) (enforcers []Enforcer) {
	if !prop.IsEmpty() {
		orderEnforcer := &OrderEnforcer{}
		enforcers = append(enforcers, orderEnforcer)
	}
	if prop.TaskTp == property.RootTaskType {
		rootEnforcer := &RootEnforcer{}
		enforcers = append(enforcers, rootEnforcer)
	}
	return
}

// OrderEnforcer enforces order property on child implementation.
type OrderEnforcer struct {
	reqProp *property.PhysicalProperty
}

// NewProperties removes order property from required physical property.
func (e *OrderEnforcer) NewProperties(prop *property.PhysicalProperty) (newProps []*property.PhysicalProperty) {
	// Order property cannot be empty now.
	e.reqProp = prop
	newProp := &property.PhysicalProperty{TaskTp: prop.TaskTp, ExpectedCnt: prop.ExpectedCnt}
	newProps = append(newProps, newProp)
	return
}

// OnEnforce adds sort operator to satisfy required order property.
func (e *OrderEnforcer) OnEnforce(child Implementation, group *Group) (impl Implementation) {
	return nil
}

// RootEnforcer enforces root property on child implementation.
type RootEnforcer struct {
}

// NewProperties converts RootTaskType to CopSingleReadTaskType or CopDoubleReadTaskType.
func (e *RootEnforcer) NewProperties(prop *property.PhysicalProperty) (newProps []*property.PhysicalProperty) {
	// prop.TaskTp must be RootTaskType now.
	copSingleProp := &property.PhysicalProperty{
		Cols:        prop.Cols,
		Desc:        prop.Desc,
		TaskTp:      property.CopSingleReadTaskType,
		ExpectedCnt: prop.ExpectedCnt,
	}
	copDoubleProp := &property.PhysicalProperty{
		Cols:        prop.Cols,
		Desc:        prop.Desc,
		TaskTp:      property.CopDoubleReadTaskType,
		ExpectedCnt: prop.ExpectedCnt,
	}
	newProps = append(newProps, copSingleProp, copDoubleProp)
	return
}

// OnEnforce finishes cop operators and adds additional root operator on top of reader if needed.
func (e *RootEnforcer) OnEnforce(child Implementation, group *Group) (impl Implementation) {
	// Check if it is able to be enforced
	// Insert reader to convert CopTaskType to RootTaskType
	// Add one more operator of child.getPlan() on top of reader
	return nil
}
