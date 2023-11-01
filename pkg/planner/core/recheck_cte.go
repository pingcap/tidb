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

// RecheckCTE fills the IsOuterMostCTE field for CTEs.
// It's a temp solution to before we fully use the Sequence to optimize the CTEs.
// This func will find the dependent relation of each CTEs.
func RecheckCTE(p LogicalPlan) {
	ctes := make(map[int]*CTEClass)
	inDegreeMap := make(map[int]int)
	findCTEs(p, ctes, true, inDegreeMap)
	for id, cte := range ctes {
		cte.isOuterMostCTE = inDegreeMap[id] == 0
	}
}

func findCTEs(
	p LogicalPlan,
	ctes map[int]*CTEClass,
	isRootTree bool,
	inDegree map[int]int,
) {
	if cteReader, ok := p.(*LogicalCTE); ok {
		cte := cteReader.cte
		if !isRootTree {
			inDegree[cte.IDForStorage]++
		}
		if _, ok := ctes[cte.IDForStorage]; ok {
			return
		}
		ctes[cte.IDForStorage] = cte
		findCTEs(cte.seedPartLogicalPlan, ctes, false, inDegree)
		if cte.recursivePartLogicalPlan != nil {
			findCTEs(cte.recursivePartLogicalPlan, ctes, false, inDegree)
		}
		return
	}
	for _, child := range p.Children() {
		findCTEs(child, ctes, isRootTree, inDegree)
	}
}
