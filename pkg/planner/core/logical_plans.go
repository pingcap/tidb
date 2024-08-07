// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/intset"
)

var (
	_ base.LogicalPlan = &LogicalJoin{}
	_ base.LogicalPlan = &LogicalAggregation{}
	_ base.LogicalPlan = &logicalop.LogicalProjection{}
	_ base.LogicalPlan = &LogicalSelection{}
	_ base.LogicalPlan = &LogicalApply{}
	_ base.LogicalPlan = &logicalop.LogicalMaxOneRow{}
	_ base.LogicalPlan = &logicalop.LogicalTableDual{}
	_ base.LogicalPlan = &DataSource{}
	_ base.LogicalPlan = &TiKVSingleGather{}
	_ base.LogicalPlan = &LogicalTableScan{}
	_ base.LogicalPlan = &LogicalIndexScan{}
	_ base.LogicalPlan = &LogicalUnionAll{}
	_ base.LogicalPlan = &logicalop.LogicalSort{}
	_ base.LogicalPlan = &logicalop.LogicalLock{}
	_ base.LogicalPlan = &logicalop.LogicalLimit{}
	_ base.LogicalPlan = &logicalop.LogicalWindow{}
	_ base.LogicalPlan = &LogicalExpand{}
	_ base.LogicalPlan = &logicalop.LogicalUnionScan{}
	_ base.LogicalPlan = &logicalop.LogicalMemTable{}
	_ base.LogicalPlan = &logicalop.LogicalShow{}
	_ base.LogicalPlan = &logicalop.LogicalShowDDLJobs{}
	_ base.LogicalPlan = &LogicalCTE{}
	_ base.LogicalPlan = &logicalop.LogicalCTETable{}
	_ base.LogicalPlan = &logicalop.LogicalSequence{}
)

// ExtractNotNullFromConds extracts not-null columns from conditions.
func ExtractNotNullFromConds(conditions []expression.Expression, p base.LogicalPlan) intset.FastIntSet {
	// extract the column NOT NULL rejection characteristic from selection condition.
	// CNF considered only, DNF doesn't have its meanings (cause that condition's eval may don't take effect)
	//
	// Take this case: select * from t where (a = 1) and (b is null):
	//
	// If we wanna where phrase eval to true, two pre-condition: {a=1} and {b is null} both need to be true.
	// Hence, we assert that:
	//
	// 1: `a` must not be null since `NULL = 1` is evaluated as NULL.
	// 2: `b` must be null since only `NULL is NULL` is evaluated as true.
	//
	// As a result,	`a` will be extracted as not-null column to abound the FDSet.
	notnullColsUniqueIDs := intset.NewFastIntSet()
	for _, condition := range conditions {
		var cols []*expression.Column
		cols = expression.ExtractColumnsFromExpressions(cols, []expression.Expression{condition}, nil)
		if util.IsNullRejected(p.SCtx(), p.Schema(), condition) {
			for _, col := range cols {
				notnullColsUniqueIDs.Insert(int(col.UniqueID))
			}
		}
	}
	return notnullColsUniqueIDs
}

// ExtractConstantCols extracts constant columns from conditions.
func ExtractConstantCols(conditions []expression.Expression, sctx base.PlanContext, fds *fd.FDSet) intset.FastIntSet {
	// extract constant cols
	// eg: where a=1 and b is null and (1+c)=5.
	// TODO: Some columns can only be determined to be constant from multiple constraints (e.g. x <= 1 AND x >= 1)
	var (
		constObjs      []expression.Expression
		constUniqueIDs = intset.NewFastIntSet()
	)
	constObjs = expression.ExtractConstantEqColumnsOrScalar(sctx.GetExprCtx(), constObjs, conditions)
	for _, constObj := range constObjs {
		switch x := constObj.(type) {
		case *expression.Column:
			constUniqueIDs.Insert(int(x.UniqueID))
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				constUniqueIDs.Insert(uniqueID)
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				constUniqueIDs.Insert(scalarUniqueID)
			}
		}
	}
	return constUniqueIDs
}

// ExtractEquivalenceCols extracts equivalence columns from conditions.
func ExtractEquivalenceCols(conditions []expression.Expression, sctx base.PlanContext, fds *fd.FDSet) [][]intset.FastIntSet {
	var equivObjsPair [][]expression.Expression
	equivObjsPair = expression.ExtractEquivalenceColumns(equivObjsPair, conditions)
	equivUniqueIDs := make([][]intset.FastIntSet, 0, len(equivObjsPair))
	for _, equivObjPair := range equivObjsPair {
		// lhs of equivalence.
		var (
			lhsUniqueID int
			rhsUniqueID int
		)
		switch x := equivObjPair[0].(type) {
		case *expression.Column:
			lhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				lhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				lhsUniqueID = scalarUniqueID
			}
		}
		// rhs of equivalence.
		switch x := equivObjPair[1].(type) {
		case *expression.Column:
			rhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				rhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				rhsUniqueID = scalarUniqueID
			}
		}
		equivUniqueIDs = append(equivUniqueIDs, []intset.FastIntSet{intset.NewFastIntSet(lhsUniqueID), intset.NewFastIntSet(rhsUniqueID)})
	}
	return equivUniqueIDs
}
