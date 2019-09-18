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

// SelectionImpl is the implementation of PhysicalSelection.
type SelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *SelectionImpl) CalcCost(outCount float64, childCosts []float64, children ...*memo.Group) float64 {
	// TODO: When CPUFactor and CopCPUFactor can be set by users, we need to distinguish them.
	sel.cost = outCount*plannercore.CPUFactor + childCosts[0]
	return sel.cost
}

// NewSelectionImpl creates a new SelectionImpl.
func NewSelectionImpl(sel *plannercore.PhysicalSelection) *SelectionImpl {
	return &SelectionImpl{baseImpl{plan: sel}}
}
