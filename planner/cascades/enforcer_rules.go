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
	// NewProperty generates relaxed property with the help of enforcer.
	NewProperty(prop *property.PhysicalProperty) (newProp *property.PhysicalProperty)
	// OnEnforce adds physical operators on top of child implementation to satisfy
	// required physical property.
	OnEnforce(reqProp *property.PhysicalProperty, child Implementation) (impl Implementation)
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
func (e *OrderEnforcer) OnEnforce(reqProp *property.PhysicalProperty, child Implementation) (impl Implementation) {
	return nil
}
