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
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
)

// FastClonePointGetForPlanCache is a fast path to clone a PointGetPlan for plan cache.
func FastClonePointGetForPlanCache(newCtx base.PlanContext, src, dst *physicalop.PointGetPlan) *physicalop.PointGetPlan {
	if dst == nil {
		dst = new(physicalop.PointGetPlan)
	}
	dst.Plan = src.Plan
	dst.Plan.SetSCtx(newCtx)
	dst.ProbeParents = src.ProbeParents
	dst.PartitionNames = src.PartitionNames
	dst.DBName = src.DBName
	dst.SetSchema(src.Schema())
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
	dst.HandleFieldType = src.HandleFieldType
	dst.HandleColOffset = src.HandleColOffset
	if len(dst.IndexValues) < len(src.IndexValues) { // actually set during rebuild phase
		dst.IndexValues = make([]types.Datum, len(src.IndexValues))
	} else {
		dst.IndexValues = dst.IndexValues[:len(src.IndexValues)]
	}
	dst.IndexConstants = utilfuncp.CloneConstantsForPlanCache(src.IndexConstants, dst.IndexConstants)
	dst.ColsFieldType = src.ColsFieldType
	dst.IdxCols = utilfuncp.CloneColumnsForPlanCache(src.IdxCols, dst.IdxCols)
	dst.IdxColLens = src.IdxColLens
	dst.AccessConditions = utilfuncp.CloneExpressionsForPlanCache(src.AccessConditions, dst.AccessConditions)
	dst.UnsignedHandle = src.UnsignedHandle
	dst.IsTableDual = src.IsTableDual
	dst.Lock = src.Lock
	dst.SetOutputNames(src.OutputNames())
	dst.LockWaitTime = src.LockWaitTime
	dst.Columns = src.Columns

	// remaining fields are unnecessary to clone:
	// cost, planCostInit, planCost, planCostVer2, accessCols
	return dst
}
