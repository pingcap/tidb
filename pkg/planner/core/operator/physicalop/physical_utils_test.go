// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package physicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/stretchr/testify/require"
)

func connectOneChildPlans(plans ...base.PhysicalPlan) base.PhysicalPlan {
	for i := 0; i < len(plans)-1; i++ {
		plans[i].SetChildren(plans[i+1])
	}
	return plans[0]
}

func TestFlattenListPushDownPlan(t *testing.T) {
	plans := []base.PhysicalPlan{
		&PhysicalLimit{},
		&PhysicalSelection{},
		&PhysicalProjection{},
		&PhysicalTableReader{},
	}
	flatten := FlattenListPushDownPlan(connectOneChildPlans(plans...))
	require.Equal(t, len(plans), len(flatten))
	require.Same(t, plans[0], flatten[3])
	require.Same(t, plans[1], flatten[2])
	require.Same(t, plans[2], flatten[1])
	require.Same(t, plans[3], flatten[0])
}

func TestFlattenTreePushDownPlan(t *testing.T) {
	//  Though the below tree is not a valid plan tree, it is only used to test the FlattenTreePushDownPlan function.
	//	         Limit1
	//	        /
	//	       IndexLookUp1
	//	     /          \
	//	    Limit2      IndexLookUp2
	//	   /           /           \
	//	  Projection  IndexScan2   TableScan
	//	 /
	//	IndexScan1
	//  Should have order: IndexScan1, Projection, Limit2, IndexScan2, TableScan, IndexLookUp2, IndexLookUp1, Limit1

	limit1 := &PhysicalLimit{}
	indexLoopUp1 := &PhysicalLocalIndexLookUp{}
	limit2 := &PhysicalLimit{}
	projection := &PhysicalProjection{}
	indexScan1 := &PhysicalIndexScan{}
	indexLoopUp2 := &PhysicalLocalIndexLookUp{}
	indexScan2 := &PhysicalIndexScan{}
	tableScan := &PhysicalTableScan{}

	limit1.SetChildren(indexLoopUp1)
	indexLoopUp1.SetChildren(connectOneChildPlans(limit2, projection, indexScan1), indexLoopUp2)
	indexLoopUp2.SetChildren(indexScan2, tableScan)
	flatten, m := FlattenTreePushDownPlan(limit1)
	require.Equal(t, 8, len(flatten))
	require.Same(t, indexScan1, flatten[0])
	require.Same(t, projection, flatten[1])
	require.Same(t, limit2, flatten[2])
	require.Same(t, indexScan2, flatten[3])
	require.Same(t, tableScan, flatten[4])
	require.Same(t, indexLoopUp2, flatten[5])
	require.Same(t, indexLoopUp1, flatten[6])
	require.Same(t, limit1, flatten[7])
	require.Equal(t, 2, len(m))
	require.Equal(t, 6, m[2])
	require.Equal(t, 5, m[3])
}
