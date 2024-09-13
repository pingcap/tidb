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
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"slices"
	"strings"
)

// PartitionPruning finds all used partitions according to query conditions, it will
// return nil if condition match none of partitions. The return value is a array of the
// idx in the partition definitions array, use pi.Definitions[idx] to get the partition ID
func PartitionPruning(ctx base.PlanContext, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr,
	columns []*expression.Column, names types.NameSlice) ([]int, error) {
	s := PartitionProcessor{}
	pi := tbl.Meta().Partition
	switch pi.Type {
	case model.PartitionTypeHash, model.PartitionTypeKey:
		return s.pruneHashOrKeyPartition(ctx, tbl, partitionNames, conds, columns, names)
	case model.PartitionTypeRange:
		rangeOr, err := s.pruneRangePartition(ctx, pi, tbl, conds, columns, names)
		if err != nil {
			return nil, err
		}
		ret := s.convertToIntSlice(rangeOr, pi, partitionNames)
		if len(pi.DroppingDefinitions) > 0 && len(pi.AddingDefinitions) == 0 && pi.DDLState == metamodel.StateWriteOnly {
			if len(ret) == 1 && ret[0] == FullRange {
				ret = make([]int, 0, len(pi.Definitions))
				for i := range pi.Definitions {
					ret = append(ret, i)
				}
			}
			newRet := make([]int, 0, len(ret))
			// For range partitioning, one can only drop a continues range of partitions!
			lastDroppedPartitionPos := -1
			for _, pos := range ret {
				pid := pi.Definitions[pos].ID
				dropping := false
				for i, droppingDef := range pi.DroppingDefinitions {
					if droppingDef.ID == pid {
						dropping = true
						lastDroppedPartitionPos = i
						break
					}
				}
				if !dropping {
					newRet = append(newRet, pos)
				}
			}
			if lastDroppedPartitionPos == -1 {
				return ret, nil
			}
			if lastDroppedPartitionPos+1 == len(pi.Definitions) {
				// Dropped the last partition, cannot add the next :)
				return newRet, nil
			}
			for i, newPos := range newRet {
				if newPos < lastDroppedPartitionPos+1 {
					continue
				}
				if newPos == lastDroppedPartitionPos+1 {
					// already includes the next higher partition pos
					return newRet, nil
				}
				// Next higher partition pos is not included
				if len(partitionNames) > 0 {
					for _, name := range partitionNames {
						if strings.EqualFold(name.L, pi.Definitions[lastDroppedPartitionPos+1].Name.L) {
							return slices.Insert(newRet, i, lastDroppedPartitionPos+1), nil
						}
					}
					return newRet, nil
				}
				return slices.Insert(newRet, i, lastDroppedPartitionPos+1), nil
			}
			if len(partitionNames) > 0 {
				for _, name := range partitionNames {
					if strings.EqualFold(name.L, pi.Definitions[lastDroppedPartitionPos+1].Name.L) {
						return append(newRet, lastDroppedPartitionPos+1), nil
					}
				}
				return newRet, nil
			}
			return append(newRet, lastDroppedPartitionPos+1), nil
		}
		return ret, nil
	case model.PartitionTypeList:
		// TODO: Handle Default partition and Drop Partition
		return s.pruneListPartition(ctx, tbl, partitionNames, conds, columns)
	}
	return []int{FullRange}, nil
}
