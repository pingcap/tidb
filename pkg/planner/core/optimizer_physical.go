// Copyright 2015 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/atomic"
)

// avoidColumnEvaluatorForProjBelowUnion sets AvoidColumnEvaluator to false for the projection operator which is a child of Union operator.
func avoidColumnEvaluatorForProjBelowUnion(p base.PhysicalPlan) base.PhysicalPlan {
	iteratePhysicalPlan(p, func(p base.PhysicalPlan) bool {
		x, ok := p.(*physicalop.PhysicalUnionAll)
		if ok {
			for _, child := range x.Children() {
				if proj, ok := child.(*physicalop.PhysicalProjection); ok {
					proj.AvoidColumnEvaluator = true
				}
			}
		}
		return true
	})
	return p
}

// eliminateUnionScanAndLock set lock property for PointGet and BatchPointGet and eliminates UnionScan and Lock.
func eliminateUnionScanAndLock(sctx base.PlanContext, p base.PhysicalPlan) base.PhysicalPlan {
	var pointGet *physicalop.PointGetPlan
	var batchPointGet *physicalop.BatchPointGetPlan
	var physLock *physicalop.PhysicalLock
	var unionScan *physicalop.PhysicalUnionScan
	iteratePhysicalPlan(p, func(p base.PhysicalPlan) bool {
		if len(p.Children()) > 1 {
			return false
		}
		switch x := p.(type) {
		case *physicalop.PointGetPlan:
			pointGet = x
		case *physicalop.BatchPointGetPlan:
			batchPointGet = x
		case *physicalop.PhysicalLock:
			physLock = x
		case *physicalop.PhysicalUnionScan:
			unionScan = x
		}
		return true
	})
	if pointGet == nil && batchPointGet == nil {
		return p
	}
	if physLock == nil && unionScan == nil {
		return p
	}
	if physLock != nil {
		lock, waitTime := getLockWaitTime(sctx, physLock.Lock)
		if lock {
			if pointGet != nil {
				pointGet.Lock = lock
				pointGet.LockWaitTime = waitTime
			} else {
				batchPointGet.Lock = lock
				batchPointGet.LockWaitTime = waitTime
			}
		}
	}
	return transformPhysicalPlan(p, func(p base.PhysicalPlan) base.PhysicalPlan {
		if p == physLock {
			return p.Children()[0]
		}
		if p == unionScan {
			return p.Children()[0]
		}
		return p
	})
}

func iteratePhysicalPlan(p base.PhysicalPlan, f func(p base.PhysicalPlan) bool) {
	if !f(p) {
		return
	}
	for _, child := range p.Children() {
		iteratePhysicalPlan(child, f)
	}
}

func transformPhysicalPlan(p base.PhysicalPlan, f func(p base.PhysicalPlan) base.PhysicalPlan) base.PhysicalPlan {
	for i, child := range p.Children() {
		p.Children()[i] = transformPhysicalPlan(child, f)
	}
	return f(p)
}

func existsCartesianProduct(p base.LogicalPlan) bool {
	if join, ok := p.(*logicalop.LogicalJoin); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == base.InnerJoin || join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin
	}
	return slices.ContainsFunc(p.Children(), existsCartesianProduct)
}

// DefaultDisabledLogicalRulesList indicates the logical rules which should be banned.
var DefaultDisabledLogicalRulesList *atomic.Value

func disableReuseChunkIfNeeded(sctx base.PlanContext, plan base.PhysicalPlan) {
	if !sctx.GetSessionVars().IsAllocValid() {
		return
	}
	if disableReuseChunk, continueIterating := checkOverlongColType(sctx, plan); disableReuseChunk || !continueIterating {
		return
	}
	for _, child := range plan.Children() {
		disableReuseChunkIfNeeded(sctx, child)
	}
}

// checkOverlongColType Check if read field type is long field.
func checkOverlongColType(sctx base.PlanContext, plan base.PhysicalPlan) (skipReuseChunk bool, continueIterating bool) {
	if plan == nil {
		return false, false
	}
	switch plan.(type) {
	case *physicalop.PhysicalTableReader, *physicalop.PhysicalIndexReader,
		*physicalop.PhysicalIndexLookUpReader, *physicalop.PhysicalIndexMergeReader:
		if existsOverlongType(plan.Schema(), false) {
			sctx.GetSessionVars().ClearAlloc(nil, false)
			return true, false
		}
	case *physicalop.PointGetPlan:
		if existsOverlongType(plan.Schema(), true) {
			sctx.GetSessionVars().ClearAlloc(nil, false)
			return true, false
		}
	default:
		// Other physical operators do not read data, so we can continue to iterate.
		return false, true
	}
	// PhysicalReader and PointGet is at the root, their children are nil or on the tikv/tiflash side.
	// So we can stop iterating.
	return false, false
}

var (
	// MaxMemoryLimitForOverlongType is the memory limit for overlong type column check.
	// Why is it not 128 ?
	// Because many customers allocate a portion of memory to their management programs,
	// the actual amount of usable memory does not align to 128GB.
	// TODO: We are also lacking test data for instances with less than 128GB of memory, so we need to plan the rules here.
	// TODO: internal sql can force to use chunk reuse if we ensure the memory usage is safe.
	// TODO: We can consider the limit/Topn in the future.
	MaxMemoryLimitForOverlongType = 120 * size.GB
	maxFlenForOverlongType        = mysql.MaxBlobWidth * 2
)

// existsOverlongType Check if exists long type column.
// If pointGet is true, we will check the total Flen of all columns, if it exceeds maxFlenForOverlongType,
// we will disable chunk reuse.
// For a point get, there is only one row, so we can easily estimate the size.
// However, for a non-point get, there may be many rows, and it is impossible to determine the memory size used.
// Therefore, we can only forcibly skip the reuse chunk.
func existsOverlongType(schema *expression.Schema, pointGet bool) bool {
	if schema == nil {
		return false
	}
	totalFlen := 0
	for _, column := range schema.Columns {
		switch column.RetType.GetType() {
		case mysql.TypeLongBlob,
			mysql.TypeBlob, mysql.TypeJSON, mysql.TypeTiDBVectorFloat32:
			return true
		case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob:
			// if the column is varchar and the length of
			// the column is defined to be more than 1000,
			// the column is considered a large type and
			// disable chunk_reuse.
			if column.RetType.GetFlen() <= 1000 {
				continue
			}
			if pointGet {
				totalFlen += column.RetType.GetFlen()
				if checkOverlongTypeForPointGet(totalFlen) {
					return true
				}
				continue
			}
			return true
		}
	}
	return false
}

func checkOverlongTypeForPointGet(totalFlen int) bool {
	totalMemory, err := memory.MemTotal()
	if err != nil || totalMemory <= 0 {
		return true
	}
	if totalMemory >= MaxMemoryLimitForOverlongType {
		if totalFlen <= maxFlenForOverlongType {
			return false
		}
	}
	return true
}
