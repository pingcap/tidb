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

package core

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

func rebuildTiDBRowIDTableScanRange(
	sctx base.PlanContext,
	scan *PhysicalTableScan,
) (bool, error) {
	if len(scan.TiDBRowIDRangeOps) == 0 {
		return false, nil
	}
	physicalIDs, keyRanges, err := resolveTiDBRowIDRangeBounds(
		sctx.GetExprCtx().GetEvalCtx(),
		scan.Table,
		scan.TiDBRowIDAllowedPartitionNames,
		scan.TiDBRowIDRangeOps,
		scan.TiDBRowIDRangeBounds,
	)
	if err != nil {
		return true, err
	}
	scan.TiDBRowIDRangePhysicalIDs = physicalIDs
	scan.TiDBRowIDKeyRanges = keyRanges
	return true, nil
}

func rebuildTiDBRowIDPointGetRange(
	sctx base.PlanContext,
	plan *PointGetPlan,
) (bool, error) {
	if len(plan.TiDBRowIDCandidates) == 0 {
		return false, nil
	}
	plan.Handle = nil
	plan.PartitionIdx = nil
	plan.IsTableDual = true
	handles, partitionIdxs, err := resolveTiDBRowIDCandidates(
		sctx.GetExprCtx().GetEvalCtx(),
		plan.TblInfo,
		plan.TiDBRowIDAllowedPartitionNames,
		plan.TiDBRowIDCandidates,
	)
	if err != nil || len(handles) == 0 {
		return true, err
	}
	plan.Handle = handles[0]
	plan.IsTableDual = false
	if len(partitionIdxs) > 0 {
		partitionIdx := partitionIdxs[0]
		plan.PartitionIdx = &partitionIdx
	}
	return true, nil
}

func rebuildTiDBRowIDBatchPointGetRange(
	sctx base.PlanContext,
	plan *BatchPointGetPlan,
) (bool, error) {
	if len(plan.TiDBRowIDCandidates) == 0 {
		return false, nil
	}
	plan.Handles = nil
	plan.PartitionIdxs = nil
	handles, partitionIdxs, err := resolveTiDBRowIDCandidates(
		sctx.GetExprCtx().GetEvalCtx(),
		plan.TblInfo,
		plan.TiDBRowIDAllowedPartitionNames,
		plan.TiDBRowIDCandidates,
	)
	if err != nil {
		return true, err
	}
	plan.Handles = handles
	plan.PartitionIdxs = partitionIdxs
	return true, nil
}
