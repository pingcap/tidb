// Copyright 2025 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// DecodeFlatPlan decodes a string back to a FlatPhysicalPlan.
func DecodeFlatPlan(encoded string) (*FlatPhysicalPlan, error) {
	if encoded == "" {
		return nil, nil
	}

	// Decompress the encoded string
	decompressed, err := plancodec.Decompress(encoded)
	if err != nil {
		return nil, err
	}

	// Create a new FlatPhysicalPlan
	flat := &FlatPhysicalPlan{
		Main: make(FlatPlanTree, 0),
		CTEs: make([]FlatPlanTree, 0),
	}

	// Split the decompressed string into lines
	lines := strings.Split(string(decompressed), "\n")
	if len(lines) == 0 {
		return flat, nil
	}

	// Track the current depth and parent-child relationships
	type nodeInfo struct {
		depth     int
		parentIdx int
		children  []int
	}
	nodes := make([]nodeInfo, 0)
	operators := make([]*FlatOperator, 0)

	// First pass: collect all operators and their relationships
	for _, line := range lines {
		if line == "" {
			continue
		}

		// Parse the line into operator information
		depth, pid, planType, rowCount, taskTypeInfo, explainInfo, actRows, analyzeInfo, memoryInfo, diskInfo, err := plancodec.DecodePlanNode(line)
		if err != nil {
			return nil, err
		}

		// Extract operator properties from the ID string
		// Format: "id" + label
		parts := strings.Split(pid, "(")
		id := strings.TrimSpace(parts[0])
		label := Empty
		if len(parts) > 1 {
			labelStr := strings.TrimSuffix(parts[1], ")")
			switch labelStr {
			case "Build":
				label = BuildSide
			case "Probe":
				label = ProbeSide
			case "Seed Part":
				label = SeedPart
			case "Recursive Part":
				label = RecursivePart
			}
		}

		// Extract task type information
		isRoot, storeType, err := plancodec.DecodeTaskType(taskTypeInfo)

		// Create the operator
		op := &FlatOperator{
			Origin:         nil, // We can't reconstruct the original plan
			Depth:          uint32(info.Depth),
			Label:          label,
			IsRoot:         isRoot,
			StoreType:      storeType,
			IsPhysicalPlan: true, // Assume physical plan since we're decoding a FlatPhysicalPlan
		}

		// Add to our tracking structures
		operators = append(operators, op)
		nodes = append(nodes, nodeInfo{
			depth:     info.Depth,
			parentIdx: -1,
			children:  make([]int, 0),
		})

		// Update parent-child relationships
		if info.Depth > 0 {
			// Find the parent (the last node with depth-1)
			for i := len(nodes) - 2; i >= 0; i-- {
				if nodes[i].depth == info.Depth-1 {
					nodes[len(nodes)-1].parentIdx = i
					nodes[i].children = append(nodes[i].children, len(nodes)-1)
					break
				}
			}
		}
	}

	// Second pass: set up children indices
	for i, node := range nodes {
		if len(node.children) > 0 {
			operators[i].ChildrenIdx = node.children
			operators[i].ChildrenEndIdx = node.children[len(node.children)-1]
		}
	}

	// Add operators to the main plan tree
	flat.Main = operators

	return flat, nil
}
