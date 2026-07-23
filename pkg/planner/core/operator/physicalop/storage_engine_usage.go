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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// StorageEngineUsage reports whether the physical plan tree reads from TiKV
// and/or TiFlash. Only storage boundaries are inspected: every operator below a
// reader runs on that reader's engine, so there is no need to descend into
// reader subtrees. TiDB-side operators (roots, memory-table scans, Selections
// produced by RootTaskConds) count as neither engine.
func StorageEngineUsage(plan base.PhysicalPlan) (hasTiKV, hasTiFlash bool) {
	if plan == nil {
		return false, false
	}
	switch x := plan.(type) {
	case *PhysicalTableReader:
		return x.StoreType != kv.TiFlash, x.StoreType == kv.TiFlash
	case *PhysicalIndexReader, *PhysicalIndexLookUpReader, *PhysicalIndexMergeReader,
		*PointGetPlan, *BatchPointGetPlan:
		return true, false
	case *PhysicalCTE:
		hasTiKV, hasTiFlash = StorageEngineUsage(x.SeedPlan)
		recurTiKV, recurTiFlash := StorageEngineUsage(x.RecurPlan)
		return hasTiKV || recurTiKV, hasTiFlash || recurTiFlash
	}
	for _, child := range plan.Children() {
		childTiKV, childTiFlash := StorageEngineUsage(child)
		hasTiKV = hasTiKV || childTiKV
		hasTiFlash = hasTiFlash || childTiFlash
		if hasTiKV && hasTiFlash {
			return true, true
		}
	}
	return hasTiKV, hasTiFlash
}
