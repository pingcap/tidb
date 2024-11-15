// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
)

func clonePhysicalPlansForPlanCache(newCtx base.PlanContext, plans []base.PhysicalPlan) ([]base.PhysicalPlan, bool) {
	clonedPlans := make([]base.PhysicalPlan, len(plans))
	for i, plan := range plans {
		cloned, ok := plan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		clonedPlans[i] = cloned.(base.PhysicalPlan)
	}
	return clonedPlans, true
}

func cloneExpressionsForPlanCache(exprs, cloned []expression.Expression) []expression.Expression {
	if exprs == nil {
		return nil
	}
	allSafe := true
	for _, e := range exprs {
		if !e.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return exprs
	}
	if cloned == nil {
		cloned = make([]expression.Expression, 0, len(exprs))
	} else {
		cloned = cloned[:0]
	}
	for _, e := range exprs {
		if e.SafeToShareAcrossSession() {
			cloned = append(cloned, e)
		} else {
			cloned = append(cloned, e.Clone())
		}
	}
	return cloned
}

func cloneExpression2DForPlanCache(exprs [][]expression.Expression) [][]expression.Expression {
	if exprs == nil {
		return nil
	}
	cloned := make([][]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, cloneExpressionsForPlanCache(e, nil))
	}
	return cloned
}

func cloneScalarFunctionsForPlanCache(scalarFuncs, cloned []*expression.ScalarFunction) []*expression.ScalarFunction {
	if scalarFuncs == nil {
		return nil
	}
	allSafe := true
	for _, f := range scalarFuncs {
		if !f.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return scalarFuncs
	}
	if cloned == nil {
		cloned = make([]*expression.ScalarFunction, 0, len(scalarFuncs))
	} else {
		cloned = cloned[:0]
	}
	for _, f := range scalarFuncs {
		if f.SafeToShareAcrossSession() {
			cloned = append(cloned, f)
		} else {
			cloned = append(cloned, f.Clone().(*expression.ScalarFunction))
		}
	}
	return cloned
}

func cloneColumnsForPlanCache(cols, cloned []*expression.Column) []*expression.Column {
	if cols == nil {
		return nil
	}
	allSafe := true
	for _, c := range cols {
		if !c.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return cols
	}
	if cloned == nil {
		cloned = make([]*expression.Column, 0, len(cols))
	} else {
		cloned = cloned[:0]
	}
	for _, c := range cols {
		if c == nil {
			cloned = append(cloned, nil)
			continue
		}
		if c.SafeToShareAcrossSession() {
			cloned = append(cloned, c)
		} else {
			cloned = append(cloned, c.Clone().(*expression.Column))
		}
	}
	return cloned
}

func cloneConstantsForPlanCache(constants, cloned []*expression.Constant) []*expression.Constant {
	if constants == nil {
		return nil
	}
	allSafe := true
	for _, c := range constants {
		if !c.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return constants
	}
	if cloned == nil {
		cloned = make([]*expression.Constant, 0, len(constants))
	} else {
		cloned = cloned[:0]
	}
	for _, c := range constants {
		if c.SafeToShareAcrossSession() {
			cloned = append(cloned, c)
		} else {
			cloned = append(cloned, c.Clone().(*expression.Constant))
		}
	}
	return cloned
}

func cloneConstant2DForPlanCache(constants [][]*expression.Constant) [][]*expression.Constant {
	if constants == nil {
		return nil
	}
	cloned := make([][]*expression.Constant, 0, len(constants))
	for _, c := range constants {
		cloned = append(cloned, cloneConstantsForPlanCache(c, nil))
	}
	return cloned
}

// FastClonePointGetForPlanCache is a fast path to clone a PointGetPlan for plan cache.
func FastClonePointGetForPlanCache(newCtx base.PlanContext, src, dst *PointGetPlan) *PointGetPlan {
	if dst == nil {
		dst = new(PointGetPlan)
	}
	dst.Plan = src.Plan
	dst.Plan.SetSCtx(newCtx)
	dst.probeParents = src.probeParents
	dst.PartitionNames = src.PartitionNames
	dst.dbName = src.dbName
	dst.schema = src.schema
	dst.TblInfo = src.TblInfo
	dst.IndexInfo = src.IndexInfo
	dst.PartitionIdx = nil // partition prune will be triggered during execution phase
	dst.Handle = nil       // handle will be set during rebuild phase
	if src.HandleConstant == nil {
		dst.HandleConstant = nil
	} else {
		if src.HandleConstant.SafeToShareAcrossSession() {
			dst.HandleConstant = src.HandleConstant
		} else {
			dst.HandleConstant = src.HandleConstant.Clone().(*expression.Constant)
		}
	}
	dst.handleFieldType = src.handleFieldType
	dst.HandleColOffset = src.HandleColOffset
	if len(dst.IndexValues) < len(src.IndexValues) { // actually set during rebuild phase
		dst.IndexValues = make([]types.Datum, len(src.IndexValues))
	} else {
		dst.IndexValues = dst.IndexValues[:len(src.IndexValues)]
	}
	dst.IndexConstants = cloneConstantsForPlanCache(src.IndexConstants, dst.IndexConstants)
	dst.ColsFieldType = src.ColsFieldType
	dst.IdxCols = cloneColumnsForPlanCache(src.IdxCols, dst.IdxCols)
	dst.IdxColLens = src.IdxColLens
	dst.AccessConditions = cloneExpressionsForPlanCache(src.AccessConditions, dst.AccessConditions)
	dst.UnsignedHandle = src.UnsignedHandle
	dst.IsTableDual = src.IsTableDual
	dst.Lock = src.Lock
	dst.outputNames = src.outputNames
	dst.LockWaitTime = src.LockWaitTime
	dst.Columns = src.Columns

	// remaining fields are unnecessary to clone:
	// cost, planCostInit, planCost, planCostVer2, accessCols
	return dst
}
