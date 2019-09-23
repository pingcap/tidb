// Copyright 2019 PingCAP, Inc.
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

package implementation

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
)

// ProjectionImpl is the implementation of PhysicalProjection.
type ProjectionImpl struct {
	baseImpl
}

// NewProjectionImpl creates a new projection Implementation.
func NewProjectionImpl(proj *plannercore.PhysicalProjection) *ProjectionImpl {
	return &ProjectionImpl{baseImpl{plan: proj}}
}

// ShowImpl is the Implementation of PhysicalShow.
type ShowImpl struct {
	baseImpl
}

// NewShowImpl creates a new ShowImpl.
func NewShowImpl(show *plannercore.PhysicalShow) *ShowImpl {
	return &ShowImpl{baseImpl: baseImpl{plan: show}}
}

// RootSelectionImpl is the implementation of PhysicalSelection in TiDB layer.
type RootSelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *RootSelectionImpl) CalcCost(outCount float64, childCosts []float64, children ...*memo.Group) float64 {
	sel.cost = outCount*plannercore.CPUFactor + childCosts[0]
	return sel.cost
}

// NewRootSelectionImpl creates a new RootSelectionImpl.
func NewRootSelectionImpl(sel *plannercore.PhysicalSelection) *RootSelectionImpl {
	return &RootSelectionImpl{baseImpl{plan: sel}}
}

// TiKVCopSelectionImpl is the implementation of PhysicalSelection in TiKV layer.
type TiKVCopSelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *TiKVCopSelectionImpl) CalcCost(outCount float64, childCosts []float64, children ...*memo.Group) float64 {
	sel.cost = outCount*plannercore.CopCPUFactor + childCosts[0]
	return sel.cost
}

// NewTiKVCopSelectionImpl creates a new TiKVCopSelectionImpl.
func NewTiKVCopSelectionImpl(sel *plannercore.PhysicalSelection) *TiKVCopSelectionImpl {
	return &TiKVCopSelectionImpl{baseImpl{plan: sel}}
}
