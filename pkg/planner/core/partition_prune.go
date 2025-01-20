// Copyright 2020 PingCAP, Inc.
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
	tmodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

// PartitionPruning finds all used partitions according to query conditions, it will
// return nil if condition match none of partitions. The return value is a array of the
// idx in the partition definitions array, use pi.Definitions[idx] to get the partition ID
func PartitionPruning(ctx base.PlanContext, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []ast.CIStr,
	columns []*expression.Column, names types.NameSlice) ([]int, error) {
	s := PartitionProcessor{}
	pi := tbl.Meta().Partition
	switch pi.Type {
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		return s.pruneHashOrKeyPartition(ctx, tbl, partitionNames, conds, columns, names)
	case ast.PartitionTypeRange:
		rangeOr, err := s.pruneRangePartition(ctx, pi, tbl, conds, columns, names)
		if err != nil {
			return nil, err
		}
		ret := s.convertToIntSlice(rangeOr, pi, partitionNames)
		ret = handleDroppingForRange(pi, partitionNames, ret)
		return ret, nil
	case ast.PartitionTypeList:
		return s.pruneListPartition(ctx, tbl, partitionNames, conds, columns)
	}
	return []int{FullRange}, nil
}

func handleDroppingForRange(pi *tmodel.PartitionInfo, partitionNames []ast.CIStr, usedPartitions []int) []int {
	if pi.CanHaveOverlappingDroppingPartition() {
		if len(usedPartitions) == 1 && usedPartitions[0] == FullRange {
			usedPartitions = make([]int, 0, len(pi.Definitions))
			for i := range pi.Definitions {
				usedPartitions = append(usedPartitions, i)
			}
		}
		ret := make([]int, 0, len(usedPartitions))
		for i := range usedPartitions {
			idx := pi.GetOverlappingDroppingPartitionIdx(usedPartitions[i])
			if idx == -1 {
				// dropped without overlapping partition, skip it
				continue
			}
			if idx == usedPartitions[i] {
				// non-dropped partition
				ret = append(ret, idx)
				continue
			}
			// partition being dropped, remove the consecutive range of dropping partitions
			// and add the overlapping partition.
			end := i + 1
			for ; end < len(usedPartitions) && usedPartitions[end] < idx; end++ {
				continue
			}
			// add the overlapping partition, if not already included
			if end >= len(usedPartitions) || usedPartitions[end] != idx {
				// It must also match partitionNames if explicitly given
				s := PartitionProcessor{}
				if len(partitionNames) == 0 || s.findByName(partitionNames, pi.Definitions[idx].Name.L) {
					ret = append(ret, idx)
				}
			}
			if end < len(usedPartitions) {
				ret = append(ret, usedPartitions[end:]...)
			}
			break
		}
		usedPartitions = ret
	}
	if len(usedPartitions) == len(pi.Definitions) {
		return []int{FullRange}
	}
	return usedPartitions
}
