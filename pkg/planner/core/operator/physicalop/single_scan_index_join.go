// Copyright 2026 PingCAP, Inc.
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

package physicalop

import (
	"slices"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// HasSingleScanIndexJoin reports whether the physical plan tree contains an
// index join whose inner side is a single scan: a probe on the handle / primary
// key, or on an index that covers everything the join needs. Such an inner side
// reads only the rows the outer side asks for and never pays a table lookup, so
// it is the access method most easily lost when a plan is rebuilt under a
// different storage engine.
//
// An inner side that double-reads (IndexLookUp, IndexMerge) is deliberately not
// reported: it already pays a lookup per outer row, so it has no comparable
// advantage to protect.
func HasSingleScanIndexJoin(plan base.PhysicalPlan) bool {
	if plan == nil {
		return false
	}
	if ij := asIndexJoin(plan); ij != nil {
		children := ij.Children()
		if ij.InnerChildIdx >= 0 && ij.InnerChildIdx < len(children) &&
			isSingleScanRead(children[ij.InnerChildIdx]) {
			return true
		}
	}
	if cte, ok := plan.(*PhysicalCTE); ok {
		if HasSingleScanIndexJoin(cte.SeedPlan) || HasSingleScanIndexJoin(cte.RecurPlan) {
			return true
		}
	}
	return slices.ContainsFunc(plan.Children(), HasSingleScanIndexJoin)
}

// asIndexJoin returns the embedded PhysicalIndexJoin of any index join variant,
// or nil for other operators. The hash and merge variants embed
// PhysicalIndexJoin by value, so a plain type assertion misses them.
func asIndexJoin(plan base.PhysicalPlan) *PhysicalIndexJoin {
	switch x := plan.(type) {
	case *PhysicalIndexJoin:
		return x
	case *PhysicalIndexHashJoin:
		return &x.PhysicalIndexJoin
	case *PhysicalIndexMergeJoin:
		return &x.PhysicalIndexJoin
	}
	return nil
}

// isSingleScanRead reports whether an index join's inner side reads its rows in
// a single scan. The reader can sit under wrapper operators the inner task
// carries (Selection, Projection, UnionScan), so descend through single-child
// operators until a reader is reached.
func isSingleScanRead(plan base.PhysicalPlan) bool {
	for plan != nil {
		switch x := plan.(type) {
		case *PhysicalIndexReader:
			// A covering index: the index scan alone answers the probe.
			return true
		case *PhysicalTableReader:
			// A handle / primary-key probe. TiFlash has no index join, so any
			// other store type here is not a single-scan inner read.
			return x.StoreType == kv.TiKV
		case *PhysicalIndexLookUpReader, *PhysicalIndexMergeReader:
			return false
		}
		children := plan.Children()
		if len(children) != 1 {
			return false
		}
		plan = children[0]
	}
	return false
}
