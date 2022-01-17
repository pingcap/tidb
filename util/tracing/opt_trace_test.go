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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlattenLogicalPlanTrace(t *testing.T) {
	root1 := &LogicalPlanTrace{
		ID:          1,
		TP:          "foo1",
		ExplainInfo: "bar1",
		Children: []*LogicalPlanTrace{
			{
				ID:          2,
				TP:          "foo2",
				ExplainInfo: "bar2",
				Children:    nil,
			},
		},
	}
	root2 := &LogicalPlanTrace{
		ID:          1,
		TP:          "foo1",
		ExplainInfo: "bar1",
		Children: []*LogicalPlanTrace{
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
				Children: []*LogicalPlanTrace{
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
	expect1 := []FlattenLogicalPlanTrace{
		{
			ID:          2,
			TP:          "foo2",
			ExplainInfo: "bar2",
			Children:    []int{},
		},
		{
			ID:          1,
			TP:          "foo1",
			ExplainInfo: "bar1",
			Children:    []int{2},
		},
	}
	expect2 := []FlattenLogicalPlanTrace{
		{
			ID:          2,
			TP:          "foo2",
			ExplainInfo: "bar2",
			Children:    []int{},
		},
		{
			ID:          4,
			TP:          "foo4",
			ExplainInfo: "bar4",
			Children:    []int{},
		},
		{
			ID:          3,
			TP:          "foo3",
			ExplainInfo: "bar3",
			Children:    []int{4},
		},
		{
			ID:          1,
			TP:          "foo1",
			ExplainInfo: "bar1",
			Children:    []int{2, 3},
		},
	}
	require.EqualValues(t, toFlattenLogicalPlanTrace(root1), expect1)
	require.EqualValues(t, toFlattenLogicalPlanTrace(root2), expect2)
}

func TestFlattenPhysicalPlanTrace(t *testing.T) {
	tracer := &PhysicalOptimizeTracer{
		State: map[string]map[string]*PhysicalOptimizeTraceInfo{},
	}
	tracer.State["lp_1"] = map[string]*PhysicalOptimizeTraceInfo{
		"p1": {
			Property: "p1",
			BestTask: &PhysicalPlanTrace{
				ID:       1,
				TP:       "pp",
				Property: "p1",
			},
			Candidates: []*PhysicalPlanTrace{
				{
					ID:       1,
					TP:       "pp",
					Property: "p1",
				},
				{
					ID:       2,
					TP:       "pp",
					Property: "p1",
				},
			},
		},
		"p2": {
			Property: "p2",
			BestTask: nil,
			Candidates: []*PhysicalPlanTrace{
				{
					ID:       3,
					TP:       "pp",
					Property: "p2",
				},
			},
		},
	}
	expected := &FlattenPhysicalPlanTrace{
		PhysicalPlanCandidatesTrace: []*PhysicalPlanTrace{
			{
				ID:                 1,
				TP:                 "pp",
				Selected:           true,
				Property:           "p1",
				MappingLogicalPlan: "lp_1",
			},
			{
				ID:                 2,
				TP:                 "pp",
				Selected:           false,
				Property:           "p1",
				MappingLogicalPlan: "lp_1",
			},
			{
				ID:                 3,
				TP:                 "pp",
				Selected:           false,
				Property:           "p2",
				MappingLogicalPlan: "lp_1",
			},
		},
	}
	tracer.BuildFlattenPhysicalPlanTrace()
	sort.Slice(tracer.FlattenPhysicalPlanTrace.PhysicalPlanCandidatesTrace, func(i, j int) bool {
		return tracer.FlattenPhysicalPlanTrace.PhysicalPlanCandidatesTrace[i].ID <
			tracer.FlattenPhysicalPlanTrace.PhysicalPlanCandidatesTrace[j].ID
	})
	sort.Slice(expected.PhysicalPlanCandidatesTrace, func(i, j int) bool {
		return expected.PhysicalPlanCandidatesTrace[i].ID <
			expected.PhysicalPlanCandidatesTrace[j].ID
	})
	isFlattenPhysicalPlanTraceEqual(t, tracer.FlattenPhysicalPlanTrace, expected)
}

func isFlattenPhysicalPlanTraceEqual(t *testing.T, actual, expected *FlattenPhysicalPlanTrace) {
	require.Len(t, actual.PhysicalPlanCandidatesTrace, len(expected.PhysicalPlanCandidatesTrace))
	for i, v := range actual.PhysicalPlanCandidatesTrace {
		require.EqualValues(t, v, expected.PhysicalPlanCandidatesTrace[i])
	}
}
