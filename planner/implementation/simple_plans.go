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

// TiDBSelectionImpl is the implementation of PhysicalSelection in TiDB layer.
type TiDBSelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *TiDBSelectionImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	sel.cost = children[0].GetPlan().Stats().RowCount*sel.plan.SCtx().GetSessionVars().CPUFactor + children[0].GetCost()
	return sel.cost
}

// NewTiDBSelectionImpl creates a new TiDBSelectionImpl.
func NewTiDBSelectionImpl(sel *plannercore.PhysicalSelection) *TiDBSelectionImpl {
	return &TiDBSelectionImpl{baseImpl{plan: sel}}
}

// TiKVSelectionImpl is the implementation of PhysicalSelection in TiKV layer.
type TiKVSelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *TiKVSelectionImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	sel.cost = children[0].GetPlan().Stats().RowCount*sel.plan.SCtx().GetSessionVars().CopCPUFactor + children[0].GetCost()
	return sel.cost
}

// NewTiKVSelectionImpl creates a new TiKVSelectionImpl.
func NewTiKVSelectionImpl(sel *plannercore.PhysicalSelection) *TiKVSelectionImpl {
	return &TiKVSelectionImpl{baseImpl{plan: sel}}
}
