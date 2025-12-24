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

package core

import (
	"cmp"
	"math"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type joinReorderGreedySolver struct {
	allInnerJoin bool // allInnerJoin indicates whether all joins in the current join group are inner joins.
	*baseSingleGroupJoinOrderSolver
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//
//	For base node, its CumCount equals to the sum of the count of its subtree.
//	See baseNodeCumCost for more details.
//
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodePlans []base.LogicalPlan) (base.LogicalPlan, error) {
	var err error
	s.curJoinGroup, err = s.generateJoinOrderNode(joinNodePlans)
	if err != nil {
		return nil, err
	}
	var leadingJoinNodes []*jrNode
	if s.leadingJoinGroup != nil {
		// We have a leading hint to let some tables join first. The result is stored in the s.leadingJoinGroup.
		// We generate jrNode separately for it.
		leadingJoinNodes, err = s.generateJoinOrderNode([]base.LogicalPlan{s.leadingJoinGroup})
		if err != nil {
			return nil, err
		}
	}
	// Sort plans by cost
	slices.SortStableFunc(s.curJoinGroup, func(i, j *jrNode) int {
		return cmp.Compare(i.cumCost, j.cumCost)
	})

	// If there's an ORDER BY and a table has a matching index, prioritize it as the starting table.
	// This enables index join to preserve ordering and avoid a separate sort operation.
	// Only apply when tidb_opt_ordering_preserving_join_discount < 1.0 (feature enabled)
	discount := s.ctx.GetSessionVars().OrderingPreservingJoinDiscount
	if discount > 0 && discount < 1.0 {
		if orderingIdx := s.findOrderingPreservingTableIndex(); orderingIdx > 0 {
			// Move the ordering-preserving table to the front
			orderingNode := s.curJoinGroup[orderingIdx]
			s.curJoinGroup = slices.Delete(s.curJoinGroup, orderingIdx, orderingIdx+1)
			s.curJoinGroup = slices.Insert(s.curJoinGroup, 0, orderingNode)
		}
	}

	// joinNodeNum indicates the number of join nodes except leading join nodes in the current join group
	joinNodeNum := len(s.curJoinGroup)
	if leadingJoinNodes != nil {
		// The leadingJoinNodes should be the first element in the s.curJoinGroup.
		// So it can be joined first.
		leadingJoinNodes := append(leadingJoinNodes, s.curJoinGroup...)
		s.curJoinGroup = leadingJoinNodes
	}
	var cartesianGroup []base.LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree()
		if err != nil {
			return nil, err
		}
		if joinNodeNum > 0 && len(s.curJoinGroup) == joinNodeNum {
			// Getting here means that there is no join condition between the table used in the leading hint and other tables
			// For example: select /*+ leading(t3) */ * from t1 join t2 on t1.a=t2.a cross join t3
			// We can not let table t3 join first.
			// TODO(hawkingrei): we find the problem in the TestHint.
			// 	`select * from t1, t2, t3 union all select /*+ leading(t3, t2) */ * from t1, t2, t3 union all select * from t1, t2, t3`
			//  this sql should not return the warning. but It will not affect the result. so we will fix it as soon as possible.
			s.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check if the leading hint table has join conditions with other tables")
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	cartesianThreshold := s.ctx.GetSessionVars().CartesianJoinOrderThreshold
	for {
		bestCost := math.MaxFloat64
		bestIdx, whateverValidOneIdx, bestIsCartesian := -1, -1, false
		var finalRemainOthers, remainOthersOfWhateverValidOne []expression.Expression
		var bestJoin, whateverValidOne base.LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers, isCartesian := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
			if isCartesian {
				s.ctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptCartesianJoinOrderThreshold)
			}
			if newJoin == nil || // can't yield a valid join
				(cartesianThreshold <= 0 && isCartesian) { // disable cartesian join
				continue
			}
			_, _, err := newJoin.RecursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
			whateverValidOne = newJoin
			whateverValidOneIdx = i
			remainOthersOfWhateverValidOne = remainOthers
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)

			// Cartesian join is risky but skipping it brutally may lead to bad join orders, see #63290.
			// To trade-off, we use a ratio as penalty to control the preference.
			// Only select a cartesian join when cost(cartesian)*ratio < cost(non-cartesian).
			curIsBetter := false
			if !bestIsCartesian && isCartesian {
				curIsBetter = curCost*cartesianThreshold < bestCost
			} else if bestIsCartesian && !isCartesian {
				curIsBetter = curCost < bestCost*cartesianThreshold
			} else {
				curIsBetter = curCost < bestCost
			}

			if curIsBetter {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
				bestIsCartesian = isCartesian
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			if whateverValidOne == nil {
				break
			}
			// This branch is for the unexpected case.
			// We throw assertion in test env. And create a valid join to avoid wrong result in the production env.
			intest.Assert(false, "Join reorder should find one valid join but failed.")
			bestJoin = whateverValidOne
			bestCost = math.MaxFloat64
			bestIdx = whateverValidOneIdx
			finalRemainOthers = remainOthersOfWhateverValidOne
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = slices.Delete(s.curJoinGroup, bestIdx, bestIdx+1)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftPlan, rightPlan base.LogicalPlan) (base.LogicalPlan, []expression.Expression, bool) {
	leftPlan, rightPlan, usedEdges, joinType := s.checkConnection(leftPlan, rightPlan)
	if len(usedEdges) == 0 && // cartesian join
		(!s.allInnerJoin || // not all joins are inner joins
			s.ctx.GetSessionVars().CartesianJoinOrderThreshold <= 0) { // cartesian join is disabled
		// For outer joins like `t1 left join t2 left join t3`, we have to ensure t1 participates join before
		// t2 and t3, and cartesian join between t2 and t3 might lead to incorrect results.
		// For safety we don't allow cartesian outer join here.
		// For inner joins like `t1 join t2 join t3`, we can reorder them freely, so we allow cartesian join here.
		return nil, nil, false
	}
	join, otherConds := s.makeJoin(leftPlan, rightPlan, usedEdges, joinType)
	return join, otherConds, len(usedEdges) == 0
}

// findOrderingPreservingTableIndex finds the index of a table in the join group
// that has an index matching the ORDER BY columns. Returns -1 if none found.
func (s *joinReorderGreedySolver) findOrderingPreservingTableIndex() int {
	if len(s.orderingProperties) == 0 {
		return -1
	}

	for _, orderingCols := range s.orderingProperties {
		if len(orderingCols) == 0 {
			continue
		}

		// Build a set of ordering column UniqueIDs - this identifies the SPECIFIC table instance
		// (important when the same table appears multiple times with different aliases)
		orderingColUniqueIDs := make(map[int64]bool, len(orderingCols))
		orderingColIDs := make(map[int64]bool, len(orderingCols))
		for _, col := range orderingCols {
			if col.UniqueID > 0 {
				orderingColUniqueIDs[col.UniqueID] = true
			}
			if col.ID > 0 {
				orderingColIDs[col.ID] = true
			}
		}
		
		if len(orderingColUniqueIDs) == 0 {
			continue
		}

		// Check each table in the join group - find the one whose schema contains the ordering column
		for i, node := range s.curJoinGroup {
			// First check if this table's schema contains the ordering column (by UniqueID)
			if !s.schemaContainsOrderingColumn(node.p, orderingColUniqueIDs) {
				continue
			}
			// Then check if it has a matching index
			if s.tableHasIndexMatchingOrdering(node.p, orderingColIDs) {
				return i
			}
		}
	}
	return -1
}

// schemaContainsOrderingColumn checks if the plan's schema contains any of the ordering columns
// by matching UniqueID (which is unique per plan instance)
func (s *joinReorderGreedySolver) schemaContainsOrderingColumn(plan base.LogicalPlan, orderingColUniqueIDs map[int64]bool) bool {
	schema := plan.Schema()
	for _, col := range schema.Columns {
		if orderingColUniqueIDs[col.UniqueID] {
			return true
		}
	}
	return false
}


// tableHasIndexMatchingOrdering checks if a plan contains a DataSource with an index
// that can provide the ordering specified by orderingColIDs.
// targetTableName is the expected table name from the ORDER BY column's OrigName.
// It also considers that equality predicates on prefix columns allow ordering on subsequent columns.
func (s *joinReorderGreedySolver) tableHasIndexMatchingOrdering(plan base.LogicalPlan, orderingColIDs map[int64]bool) bool {
	ds := s.findDataSource(plan)
	if ds == nil {
		return false
	}
	

	// Get equality predicate columns from the plan's conditions
	eqColIDs := s.getEqualityPredicateColumnIDs(plan)

	// Check TableInfo.Indices directly for available indexes
	for _, idx := range ds.TableInfo.Indices {
		if idx.State != model.StatePublic || idx.Invisible {
			continue
		}
		
		
		// Check if index can provide ordering:
		// Pattern: [equality predicate columns...] + [ORDER BY columns...]
		// All prefix columns must have equality predicates, then ORDER BY column(s) follow
		foundOrderingCol := false
		allPrefixHaveEq := true
		
		for _, idxCol := range idx.Columns {
			// Get the table column for this index column
			if idxCol.Offset >= len(ds.TableInfo.Columns) {
				break
			}
			tblCol := ds.TableInfo.Columns[idxCol.Offset]
			colID := tblCol.ID
			
			
			// Is this the ordering column?
			if orderingColIDs[colID] {
				if allPrefixHaveEq {
					foundOrderingCol = true
					break
				} else {
					break
				}
			}
			
			// Is this column covered by an equality predicate?
			// If not, this index can't provide ordering
			if !eqColIDs[colID] {
				break
			}
		}
		
		if foundOrderingCol {
			return true
		}
	}
	return false
}

// getEqualityPredicateColumnIDs extracts column IDs that have equality predicates
// from the plan's pushed-down conditions
func (s *joinReorderGreedySolver) getEqualityPredicateColumnIDs(plan base.LogicalPlan) map[int64]bool {
	result := make(map[int64]bool)
	
	// Check if it's a Selection with conditions
	if sel, ok := plan.(*logicalop.LogicalSelection); ok {
		for _, cond := range sel.Conditions {
			s.extractEqualityColumns(cond, result)
		}
		// Also check child
		if len(sel.Children()) > 0 {
			childResult := s.getEqualityPredicateColumnIDs(sel.Children()[0])
			for k, v := range childResult {
				result[k] = v
			}
		}
	}
	
	// Check if it's a DataSource with pushed conditions
	if ds, ok := plan.(*logicalop.DataSource); ok {
		for _, cond := range ds.AllConds {
			s.extractEqualityColumns(cond, result)
		}
	}
	
	return result
}

// extractEqualityColumns finds columns involved in equality predicates (col = const)
func (s *joinReorderGreedySolver) extractEqualityColumns(expr expression.Expression, result map[int64]bool) {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return
	}
	
	// Check for EQ function: col = value
	if sf.FuncName.L == "eq" && len(sf.GetArgs()) == 2 {
		// Check if one side is a column and other is a constant
		for _, arg := range sf.GetArgs() {
			if col, ok := arg.(*expression.Column); ok {
				if col.ID > 0 {
					result[col.ID] = true
				}
			}
		}
	}
	
	// Check for IN function: col IN (values)
	if sf.FuncName.L == "in" && len(sf.GetArgs()) > 0 {
		if col, ok := sf.GetArgs()[0].(*expression.Column); ok {
			if col.ID > 0 {
				result[col.ID] = true
			}
		}
	}
	
	// Recursively check AND conditions
	if sf.FuncName.L == "and" || sf.FuncName.L == "or" {
		for _, arg := range sf.GetArgs() {
			s.extractEqualityColumns(arg, result)
		}
	}
}

// findDataSource recursively finds a DataSource in a plan tree
func (s *joinReorderGreedySolver) findDataSource(plan base.LogicalPlan) *logicalop.DataSource {
	if ds, ok := plan.(*logicalop.DataSource); ok {
		return ds
	}
	// Check children
	for _, child := range plan.Children() {
		if ds := s.findDataSource(child); ds != nil {
			return ds
		}
	}
	return nil
}
