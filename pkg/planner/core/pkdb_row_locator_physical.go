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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/size"
)

var errEmptyTiDBRowIDMPPRange = errors.New(noMPPTasksForTableScanErrorMsg)

// IsEmptyTiDBRowIDMPPRangeError reports whether MPP task generation found no
// partition that intersects a TiDB row locator range.
func IsEmptyTiDBRowIDMPPRangeError(err error) bool {
	return errors.Cause(err) == errEmptyTiDBRowIDMPPRange
}

func constructTiDBRowIDMPPBuildTaskReq(
	ts *PhysicalTableScan,
	partitions []table.PhysicalTable,
) (*kv.MPPBuildTasksRequest, []int64) {
	allowedPhysicalIDs := make(map[int64]struct{}, len(partitions))
	for _, partition := range partitions {
		allowedPhysicalIDs[partition.GetPhysicalID()] = struct{}{}
	}
	partitionRanges := make([]kv.PartitionIDAndRanges, 0, len(ts.TiDBRowIDKeyRanges))
	physicalIDs := make([]int64, 0, len(ts.TiDBRowIDRangePhysicalIDs))
	for i, physicalID := range ts.TiDBRowIDRangePhysicalIDs {
		if _, ok := allowedPhysicalIDs[physicalID]; !ok {
			continue
		}
		partitionRanges = append(partitionRanges, kv.PartitionIDAndRanges{
			ID:        physicalID,
			KeyRanges: []kv.KeyRange{ts.TiDBRowIDKeyRanges[i]},
		})
		physicalIDs = append(physicalIDs, physicalID)
	}
	return &kv.MPPBuildTasksRequest{PartitionIDAndRanges: partitionRanges}, physicalIDs
}

func constructTiDBRowIDMPPBuildTaskForPhysicalTable(
	ts *PhysicalTableScan,
	physicalID int64,
) *kv.MPPBuildTasksRequest {
	for i, rangePhysicalID := range ts.TiDBRowIDRangePhysicalIDs {
		if rangePhysicalID == physicalID {
			return &kv.MPPBuildTasksRequest{KeyRanges: []kv.KeyRange{ts.TiDBRowIDKeyRanges[i]}}
		}
	}
	return &kv.MPPBuildTasksRequest{}
}

func setTiDBRowIDTableScanPath(scan *PhysicalTableScan, path *util.AccessPath) {
	scan.TiDBRowIDRangeOps = path.TiDBRowIDRangeOps
	scan.TiDBRowIDRangeBounds = path.TiDBRowIDRangeBounds
	scan.TiDBRowIDAllowedPartitionNames = path.TiDBRowIDAllowedPartitionNames
	scan.TiDBRowIDRangePhysicalIDs = path.TiDBRowIDRangePhysicalIDs
	scan.TiDBRowIDKeyRanges = path.TiDBRowIDKeyRanges
	if len(path.TiDBRowIDRangeOps) > 0 {
		scan.rangeInfo = "TIDB_ROW_ID()"
	}
}

func cloneTiDBRowIDTableScanFields(cloned, original *PhysicalTableScan) {
	cloned.TiDBRowIDRangeOps = slices.Clone(original.TiDBRowIDRangeOps)
	cloned.TiDBRowIDRangeBounds = util.CloneExprs(original.TiDBRowIDRangeBounds)
	cloned.TiDBRowIDAllowedPartitionNames = slices.Clone(original.TiDBRowIDAllowedPartitionNames)
	cloned.TiDBRowIDRangePhysicalIDs = slices.Clone(original.TiDBRowIDRangePhysicalIDs)
	cloned.TiDBRowIDKeyRanges = cloneTiDBRowIDKeyRanges(original.TiDBRowIDKeyRanges)
}

func tiDBRowIDTableScanMemoryUsage(scan *PhysicalTableScan) int64 {
	usage := int64(cap(scan.TiDBRowIDRangePhysicalIDs)) * size.SizeOfInt64
	usage += kv.KeyRangeSliceMemUsage(scan.TiDBRowIDKeyRanges)
	for _, bound := range scan.TiDBRowIDRangeBounds {
		usage += bound.MemoryUsage()
	}
	return usage
}

func cloneTiDBRowIDKeyRanges(ranges []kv.KeyRange) []kv.KeyRange {
	cloned := make([]kv.KeyRange, len(ranges))
	for i, keyRange := range ranges {
		cloned[i] = kv.KeyRange{
			StartKey: slices.Clone(keyRange.StartKey),
			EndKey:   slices.Clone(keyRange.EndKey),
		}
	}
	return cloned
}
