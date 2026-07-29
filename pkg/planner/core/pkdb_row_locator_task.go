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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func adjustTiDBRowIDMatchProp(
	path *util.AccessPath,
	prop *property.PhysicalProperty,
	result property.PhysicalPropMatchResult,
) property.PhysicalPropMatchResult {
	if (len(path.TiDBRowIDCandidates) > 0 || len(path.TiDBRowIDRangeOps) > 0) && !prop.IsSortItemEmpty() {
		return property.PropNotMatched
	}
	return result
}

func getTableScanCandidate(ds *logicalop.DataSource, candidate *candidatePath) *candidatePath {
	if len(candidate.path.TiDBRowIDCandidates) == 0 {
		return candidate
	}
	// Locator ranges only preserve a stable point-vs-batch shape. They are not
	// SQL handle ranges and must never reach a fallback table scan.
	fallbackPath := *candidate.path
	if fallbackPath.IsCommonHandlePath {
		fallbackPath.Ranges = ranger.FullNotNullRange()
	} else {
		unsigned := false
		if pkCol := ds.TableInfo.GetPkColInfo(); ds.TableInfo.PKIsHandle && pkCol != nil {
			unsigned = mysql.HasUnsignedFlag(pkCol.GetFlag())
		}
		fallbackPath.Ranges = ranger.FullIntRange(unsigned)
	}
	fallbackPath.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	fallbackPath.CountAfterIndex = fallbackPath.CountAfterAccess
	fallbackCandidate := *candidate
	fallbackCandidate.path = &fallbackPath
	return &fallbackCandidate
}

func buildTiDBRowIDPointGetTask(
	ds *logicalop.DataSource,
	prop *property.PhysicalProperty,
	candidate *candidatePath,
	plan *PointGetPlan,
	task *RootTask,
) bool {
	if len(candidate.path.TiDBRowIDCandidates) == 0 {
		return false
	}
	plan.TiDBRowIDCandidates = candidate.path.TiDBRowIDCandidates
	plan.TiDBRowIDAllowedPartitionNames = candidate.path.TiDBRowIDAllowedPartitionNames
	plan.accessCols = ds.TblCols
	if len(candidate.path.TiDBRowIDHandles) == 0 {
		plan.IsTableDual = true
	} else {
		plan.Handle = candidate.path.TiDBRowIDHandles[0]
		if len(candidate.path.TiDBRowIDPartitionIdxs) > 0 {
			partitionIdx := candidate.path.TiDBRowIDPartitionIdxs[0]
			plan.PartitionIdx = &partitionIdx
		}
	}
	if len(candidate.path.TableFilters) > 0 {
		selection := PhysicalSelection{
			Conditions: candidate.path.TableFilters,
		}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ds.QueryBlockOffset())
		selection.SetChildren(plan)
		task.SetPlan(selection)
	}
	return true
}

func buildTiDBRowIDBatchPointGetTask(
	ds *logicalop.DataSource,
	prop *property.PhysicalProperty,
	candidate *candidatePath,
	plan *BatchPointGetPlan,
	task *RootTask,
	accessCount float64,
) bool {
	if len(candidate.path.TiDBRowIDCandidates) == 0 {
		return false
	}
	plan.TiDBRowIDCandidates = candidate.path.TiDBRowIDCandidates
	plan.TiDBRowIDAllowedPartitionNames = candidate.path.TiDBRowIDAllowedPartitionNames
	plan.Handles = append(plan.Handles, candidate.path.TiDBRowIDHandles...)
	plan.PartitionIdxs = append(plan.PartitionIdxs[:0], candidate.path.TiDBRowIDPartitionIdxs...)
	plan.SinglePartition = false
	plan.KeepOrder = false
	plan.accessCols = ds.TblCols
	if len(candidate.path.TableFilters) > 0 {
		plan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(accessCount), ds.Schema().Clone(), ds.OutputNames(), ds.QueryBlockOffset())
		selection := PhysicalSelection{
			Conditions: candidate.path.TableFilters,
		}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ds.QueryBlockOffset())
		selection.SetChildren(plan)
		task.SetPlan(selection)
	}
	return true
}
