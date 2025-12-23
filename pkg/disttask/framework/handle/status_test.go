// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestCalculateRequiredNodes(t *testing.T) {
	cases := []struct {
		cpuCount int
		// 0: concurrency, 1: max-node-count
		params   [][]int
		expected int
	}{
		// no task
		{cpuCount: 8, params: [][]int{}, expected: 1},
		// single task cases
		{cpuCount: 8, params: [][]int{{1, 1}}, expected: 1},
		{cpuCount: 8, params: [][]int{{1, 3}}, expected: 3},
		{cpuCount: 8, params: [][]int{{3, 1}}, expected: 1},
		{cpuCount: 8, params: [][]int{{8, 1}}, expected: 1},
		{cpuCount: 8, params: [][]int{{8, 4}}, expected: 4},
		// multiple tasks of concurrency < cpuCount
		{cpuCount: 8, params: [][]int{{1, 1}, {2, 1}, {2, 1}, {3, 1}}, expected: 1},
		{cpuCount: 8, params: [][]int{{1, 3}, {2, 4}}, expected: 4},
		{cpuCount: 8, params: [][]int{{1, 3}, {2, 4}, {6, 1}}, expected: 4},
		{cpuCount: 8, params: [][]int{{1, 3}, {2, 4}, {3, 2}}, expected: 4},
		{cpuCount: 8, params: [][]int{{1, 3}, {2, 4}, {3, 2}, {2, 2}}, expected: 4},
		{cpuCount: 8, params: [][]int{{1, 3}, {2, 4}, {3, 2}, {5, 2}}, expected: 4},
		{cpuCount: 8, params: [][]int{{1, 3}, {2, 4}, {3, 2}, {2, 3}}, expected: 4},
		{cpuCount: 8, params: [][]int{{1, 3}, {2, 4}, {3, 2}, {2, 3}, {6, 1}}, expected: 4},
		{cpuCount: 8, params: [][]int{{3, 2}, {5, 2}}, expected: 2},
		{cpuCount: 8, params: [][]int{{3, 2}, {6, 2}}, expected: 4},
		// multiple tasks that use all available CPUs
		{cpuCount: 8, params: [][]int{{8, 4}, {8, 6}, {8, 5}}, expected: 15},
		{cpuCount: 8, params: [][]int{{8, 4}, {8, 6}, {8, 5}, {8, 20}}, expected: 35},
		// mixing cases
		{cpuCount: 8, params: [][]int{{8, 5}, {1, 3}, {2, 4}}, expected: 9},
		{cpuCount: 8, params: [][]int{{8, 5}, {1, 3}, {2, 4}, {8, 2}}, expected: 11},
		{cpuCount: 8, params: [][]int{{8, 5}, {1, 3}, {8, 2}, {2, 4}}, expected: 11},
		{cpuCount: 8, params: [][]int{{3, 3}, {8, 5}, {6, 4}}, expected: 12},
		{cpuCount: 8, params: [][]int{{3, 3}, {8, 5}, {5, 4}}, expected: 9},
		{cpuCount: 8, params: [][]int{{3, 3}, {5, 4}, {8, 5}}, expected: 9},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			tasks := make([]*proto.TaskBase, 0, len(c.params))
			for _, params := range c.params {
				task := &proto.TaskBase{
					RequiredSlots: params[0],
					MaxNodeCount:  params[1],
				}
				tasks = append(tasks, task)
			}
			require.Equal(t, c.expected, CalculateRequiredNodes(tasks, c.cpuCount))
		})
	}
}
