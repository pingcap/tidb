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

package proto

import "github.com/docker/go-units"

// ManagedNode is a TiDB node that is managed by the framework.
type ManagedNode struct {
	// ID see GenerateExecID, it's named as host in the meta table.
	ID string
	// Role of the node, either "" or "background"
	// all managed node should have the same role
	Role     string
	CPUCount int
}

// NodeResource is the resource of the node.
// exported for test.
type NodeResource struct {
	TotalCPU  int
	TotalMem  int64
	TotalDisk uint64
}

// NewNodeResource creates a new NodeResource.
func NewNodeResource(totalCPU int, totalMem int64, totalDisk uint64) *NodeResource {
	return &NodeResource{
		TotalCPU:  totalCPU,
		TotalMem:  totalMem,
		TotalDisk: totalDisk,
	}
}

// NodeResourceForTest is only used for test.
var NodeResourceForTest = NewNodeResource(32, 32*units.GB, 100*units.GB)

// GetStepResource gets the step resource according to concurrency.
func (nr *NodeResource) GetStepResource(concurrency int) *StepResource {
	return &StepResource{
		CPU: NewAllocatable(int64(concurrency)),
		// same proportion as CPU
		Mem: NewAllocatable(int64(float64(concurrency) / float64(nr.TotalCPU) * float64(nr.TotalMem))),
	}
}

// GetTaskDiskResource gets available disk for a task.
func (nr *NodeResource) GetTaskDiskResource(concurrency int, quotaHint uint64) uint64 {
	availableDisk := min(nr.TotalDisk, quotaHint)
	return uint64(float64(concurrency) / float64(nr.TotalCPU) * float64(availableDisk))
}
