// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
)

// PartitionPruning finds all used partitions according to query conditions, it will
// return nil if condition match none of partitions. The return value is a array of the
// idx in the partition definitions array, use pi.Definitions[idx] to get the partition ID
func PartitionPruning(ctx sessionctx.Context, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr) ([]int, error) {
	s := partitionProcessor{}
	tblInfo := tbl.Meta()
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, dbName, tblInfo.Name, tblInfo.Columns, tblInfo)
	if err != nil {
		return nil, err
	}
	pi := tblInfo.Partition
	switch pi.Type {
	case model.PartitionTypeHash:
		return s.pruneHashPartition(ctx, tbl, partitionNames, conds, columns, names)
	case model.PartitionTypeRange:
		rangeOr, err := s.pruneRangePartition(ctx, pi, tbl, conds, columns, names)
		if err != nil {
			return nil, err
		}
		ret := s.convertToIntSlice(rangeOr, pi, partitionNames)
		return ret, nil
	}
	return []int{FullRange}, nil
}
