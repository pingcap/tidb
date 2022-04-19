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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

// PartitionPruning finds all used partitions according to query conditions, it will
// return nil if condition match none of partitions. The return value is a array of the
// idx in the partition definitions array, use pi.Definitions[idx] to get the partition ID
func PartitionPruning(ctx sessionctx.Context, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr,
	columns []*expression.Column, names types.NameSlice) ([]int, error) {
	s := partitionProcessor{}
	pi := tbl.Meta().Partition
	switch pi.Type {
	case model.PartitionTypeHash:
		return s.pruneHashPartition(ctx, tbl, partitionNames, conds, columns, names)
	case model.PartitionTypeRange:
		rangeOr, _, err := s.pruneRangePartition(ctx, pi, tbl, conds, columns, names, nil)
		if err != nil {
			return nil, err
		}
		ret := s.convertToIntSlice(rangeOr, pi, partitionNames)
		return ret, nil
	case model.PartitionTypeList:
		return s.pruneListPartition(ctx, tbl, partitionNames, conds)
	}
	return []int{FullRange}, nil
}
