// Copyright 2018 PingCAP, Inc.
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

package rule

import (
	"context"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// FullRange represent used all partitions.
const FullRange = -1

// PartitionProcessor rewrites the ast for table partition.
// Used by static partition prune mode.
/*
// create table t (id int) partition by range (id)
//   (partition p1 values less than (10),
//    partition p2 values less than (20),
//    partition p3 values less than (30))
//
// select * from t is equal to
// select * from (union all
//      select * from p1 where id < 10
//      select * from p2 where id < 20
//      select * from p3 where id < 30)
*/
// PartitionProcessor is here because it's easier to prune partition after predicate push down.
type PartitionProcessor struct{}

// Optimize implements the LogicalOptRule.<0th> interface.
func (s *PartitionProcessor) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	p, err := s.rewriteDataSource(lp)
	return p, planChanged, err
}

// Name implements the LogicalOptRule.<1st> interface.
func (*PartitionProcessor) Name() string {
	return "partition_processor"
}

func (s *PartitionProcessor) rewriteDataSource(lp base.LogicalPlan) (base.LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch p := lp.(type) {
	case *logicalop.DataSource:
		return s.prune(p)
	case *logicalop.LogicalUnionScan:
		ds := p.Children()[0]
		ds, err := s.prune(ds.(*logicalop.DataSource))
		if err != nil {
			return nil, err
		}
		if ua, ok := ds.(*logicalop.LogicalPartitionUnionAll); ok {
			// Adjust the UnionScan->Union->DataSource1, DataSource2 ... to
			// Union->(UnionScan->DataSource1), (UnionScan->DataSource2)
			children := make([]base.LogicalPlan, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				us := logicalop.LogicalUnionScan{
					Conditions: p.Conditions,
					HandleCols: p.HandleCols,
				}.Init(ua.SCtx(), ua.QueryBlockOffset())
				us.SetChildren(child)
				children = append(children, us)
			}
			ua.SetChildren(children...)
			ua.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("Static partition pruning mode")
			return ua, nil
		}
		// Only one partition, no union all.
		p.SetChildren(ds)
		return p, nil
	case *logicalop.LogicalCTE:
		return lp, nil
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


func (s *PartitionProcessor) prune(ds *logicalop.DataSource) (base.LogicalPlan, error) {
	pi := ds.TableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}
	// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from ds.AllConds, the condition
	// like 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
	// Now, PushDownNot have be done in the ApplyPredicateSimplification
	// AllConds and PushedDownConds may become inconsistent in subsequent ApplyPredicateSimplification calls.
	// They must be kept in sync to ensure correctness after PR #61571.
	ds.PushedDownConds = applyPredicateSimplification(ds.SCtx(), ds.PushedDownConds, false, nil)
	ds.AllConds = applyPredicateSimplification(ds.SCtx(), ds.AllConds, false, nil)
	// Return table dual when filter is constant false or null.
	dual := logicalop.Conds2TableDual(ds, ds.AllConds)
	if dual != nil {
		return dual, nil
	}
	// Try to locate partition directly for hash partition.
	// TODO: See if there is a way to remove conditions that does not
	// apply for some partitions like:
	// a = 1 OR a = 2 => for p1 only "a = 1" and for p2 only "a = 2"
	// since a cannot be 2 in p1 and a cannot be 1 in p2
	switch pi.Type {
	case ast.PartitionTypeRange:
		return s.processRangePartition(ds, pi)
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		return s.processHashOrKeyPartition(ds, pi)
	case ast.PartitionTypeList:
		return s.processListPartition(ds, pi)
	}

	return s.makeUnionAllChildren(ds, pi, GetFullRange(len(pi.Definitions)))
}

// FindByName checks whether object name exists in list.
func (*PartitionProcessor) FindByName(partitionNames []ast.CIStr, partitionName string) bool {
	for _, s := range partitionNames {
		if s.L == partitionName {
			return true
		}
	}
	return false
}
