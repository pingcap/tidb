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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// canRemoveLimit1 checks if a LIMIT 1 can be removed because it's already covered
// by a unique or primary key index. This optimization is safe because unique/primary
// key indexes guarantee at most one row will be returned.
func canRemoveLimit1(limit *physicalop.PhysicalLimit, child base.PhysicalPlan) bool {
	// Only optimize LIMIT 1 (not LIMIT 2, LIMIT 3, etc.)
	if limit.Count != 1 || limit.Offset != 0 {
		return false
	}

	// Check if the child plan guarantees at most one row
	return isGuaranteedSingleRow(child)
}

// isGuaranteedSingleRow checks if a physical plan is guaranteed to return at most one row
// based on unique/primary key constraints.
func isGuaranteedSingleRow(plan base.PhysicalPlan) bool {
	switch p := plan.(type) {
	case *physicalop.PhysicalTableReader:
		// Check if it's a point get on primary key or unique index
		if len(p.TablePlans) == 1 {
			return isGuaranteedSingleRow(p.TablePlans[0])
		}
	case *physicalop.PhysicalTableScan:
		// Check if it's a point get on primary key
		return isPointGetOnPrimaryKey(p)
	case *physicalop.PhysicalIndexReader:
		// Check if it's a point get on unique index
		if len(p.IndexPlans) == 1 {
			return isGuaranteedSingleRow(p.IndexPlans[0])
		}
	case *physicalop.PhysicalIndexScan:
		// Check if it's a point get on unique index
		return isPointGetOnUniqueIndex(p)
	case *physicalop.PhysicalIndexLookUpReader:
		// Check if both index and table plans guarantee single row
		if len(p.IndexPlans) == 1 && len(p.TablePlans) == 1 {
			return isGuaranteedSingleRow(p.IndexPlans[0]) && isGuaranteedSingleRow(p.TablePlans[0])
		}
	case *physicalop.PointGetPlan:
		// Point get always returns at most one row
		return true
	case *physicalop.BatchPointGetPlan:
		// Batch point get can return multiple rows, but we can check if it's a single point
		return len(p.Handles) == 1
	}
	return false
}

// isPointGetOnPrimaryKey checks if a table scan is a point get on primary key
func isPointGetOnPrimaryKey(scan *physicalop.PhysicalTableScan) bool {
	// Check if it's a point get (single range)
	if len(scan.Ranges) != 1 {
		return false
	}

	// Check if the range is a point (lowVal == highVal)
	range_ := scan.Ranges[0]
	if len(range_.LowVal) != len(range_.HighVal) {
		return false
	}

	for i := 0; i < len(range_.LowVal); i++ {
		cmp, err := range_.LowVal[i].Compare(types.DefaultStmtNoWarningContext, &range_.HighVal[i], collate.GetBinaryCollator())
		if err != nil || cmp != 0 {
			return false
		}
	}

	// Check if it's on primary key
	return scan.Table.PKIsHandle || scan.Table.IsCommonHandle
}

// isPointGetOnUniqueIndex checks if an index scan is a point get on unique index
func isPointGetOnUniqueIndex(scan *physicalop.PhysicalIndexScan) bool {
	// Check if the index is unique
	if !scan.Index.Unique {
		return false
	}

	// Check if it's a point get (single range)
	if len(scan.Ranges) != 1 {
		return false
	}

	// Check if the range is a point (lowVal == highVal)
	range_ := scan.Ranges[0]
	if len(range_.LowVal) != len(range_.HighVal) {
		return false
	}

	for i := 0; i < len(range_.LowVal); i++ {
		cmp, err := range_.LowVal[i].Compare(types.DefaultStmtNoWarningContext, &range_.HighVal[i], collate.GetBinaryCollator())
		if err != nil || cmp != 0 {
			return false
		}
	}

	// Check if all range values are non-null (unique index allows NULLs)
	for _, val := range range_.LowVal {
		if val.IsNull() {
			return false
		}
	}

	return true
}

// optimizeLimit1 removes LIMIT 1 when it's covered by unique/primary key constraints
func optimizeLimit1(limit *physicalop.PhysicalLimit, child base.PhysicalPlan) base.PhysicalPlan {
	if canRemoveLimit1(limit, child) {
		// Return the child plan directly, removing the LIMIT 1
		return child
	}
	return limit
}
