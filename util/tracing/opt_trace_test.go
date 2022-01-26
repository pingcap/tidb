// Copyright 2021 PingCAP, Inc.
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

package tracing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlattenLogicalPlanTrace(t *testing.T) {
	root1 := &PlanTrace{
		ID:          1,
		TP:          "foo1",
		ExplainInfo: "bar1",
		Children: []*PlanTrace{
			{
				ID:          2,
				TP:          "foo2",
				ExplainInfo: "bar2",
				Children:    nil,
			},
		},
	}
	root2 := &PlanTrace{
		ID:          1,
		TP:          "foo1",
		ExplainInfo: "bar1",
		Children: []*PlanTrace{
			{
				ID:          2,
				TP:          "foo2",
				ExplainInfo: "bar2",
				Children:    nil,
			},
			{
				ID:          3,
				TP:          "foo3",
				ExplainInfo: "bar3",
				Children: []*PlanTrace{
					{
						ID:          4,
						TP:          "foo4",
						ExplainInfo: "bar4",
						Children:    nil,
					},
				},
			},
		},
	}
	expect1 := []*PlanTrace{
		{
			ID:          2,
			TP:          "foo2",
			ExplainInfo: "bar2",
			ChildrenID:  []int{},
		},
		{
			ID:          1,
			TP:          "foo1",
			ExplainInfo: "bar1",
			ChildrenID:  []int{2},
		},
	}
	expect2 := []*PlanTrace{
		{
			ID:          2,
			TP:          "foo2",
			ExplainInfo: "bar2",
			ChildrenID:  []int{},
		},
		{
			ID:          4,
			TP:          "foo4",
			ExplainInfo: "bar4",
			ChildrenID:  []int{},
		},
		{
			ID:          3,
			TP:          "foo3",
			ExplainInfo: "bar3",
			ChildrenID:  []int{4},
		},
		{
			ID:          1,
			TP:          "foo1",
			ExplainInfo: "bar1",
			ChildrenID:  []int{2, 3},
		},
	}
	require.EqualValues(t, toFlattenPlanTrace(root1), expect1)
	require.EqualValues(t, toFlattenPlanTrace(root2), expect2)
}
