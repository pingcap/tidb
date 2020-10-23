// Copyright 2018 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/tidb/table"
)

// partitionProcessor2 is here because it's easier to prune partition after predicate push down.
type partitionProcessor2 struct{}

func (s *partitionProcessor2) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	return s.rewriteDataSource(lp)
}

func (*partitionProcessor2) name() string {
	return "partition_processor_v2"
}

func (s *partitionProcessor2) rewriteDataSource(lp LogicalPlan) (LogicalPlan, error) {
	switch p := lp.(type) {
	case *DataSource:
		return s.prune(p)
	default:
		children := lp.Children()
		for i, child := range children {
			newChild, err := s.rewriteDataSource(child)
			if err != nil {
				return nil, err
			}
			children[i] = newChild
		}
	}

	return lp, nil
}

func (s *partitionProcessor2) prune(ds *DataSource) (LogicalPlan, error) {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}
	tbl := ds.table.(table.PartitionedTable)
	usedPartitionIDs, err := PartitionPruning(ds.SCtx(), tbl, ds.allConds, ds.partitionNames, ds.TblCols, ds.names)
	if err != nil {
		return nil, err
	}
	ds.usedPartitionIDs = usedPartitionIDs
	return nil, nil
}
