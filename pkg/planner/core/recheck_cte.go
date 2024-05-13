// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// RecheckCTE fills the IsOuterMostCTE field for CTEs.
// It's a temp solution to before we fully use the Sequence to optimize the CTEs.
// This func checks whether the CTE is referenced only by the main query or not.
func RecheckCTE(p base.LogicalPlan) {
	visited := intset.NewFastIntSet()
	findCTEs(p, &visited, true)
}

func findCTEs(
	p base.LogicalPlan,
	visited *intset.FastIntSet,
	isRootTree bool,
) {
	if cteReader, ok := p.(*LogicalCTE); ok {
		cte := cteReader.cte
		if !isRootTree {
			// Set it to false since it's referenced by other CTEs.
			cte.isOuterMostCTE = false
		}
		if visited.Has(cte.IDForStorage) {
			return
		}
		visited.Insert(cte.IDForStorage)
		// Set it when we meet it first time.
		cte.isOuterMostCTE = isRootTree
		findCTEs(cte.seedPartLogicalPlan, visited, false)
		if cte.recursivePartLogicalPlan != nil {
			findCTEs(cte.recursivePartLogicalPlan, visited, false)
		}
		return
	}
	for _, child := range p.Children() {
		findCTEs(child, visited, isRootTree)
	}
}
