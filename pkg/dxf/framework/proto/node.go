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

import (
	"math"

	"github.com/docker/go-units"
)

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

// LimitDXFResource returns the resource available to DXF under the given percentage limit.
func (nr *NodeResource) LimitDXFResource(limit int) *NodeResource {
	usableCPU := getLimitedDXFCPU(nr.TotalCPU, limit)
	if usableCPU == nr.TotalCPU || nr.TotalCPU <= 0 {
		return NewNodeResource(nr.TotalCPU, nr.TotalMem, nr.TotalDisk)
	}
	usableMem := int64(float64(usableCPU) / float64(nr.TotalCPU) * float64(nr.TotalMem))
	// this feature is for premium based cluster, in which we are only support
	// global sort, there is no local disk. so we leave the disk as is.
	return NewNodeResource(usableCPU, usableMem, nr.TotalDisk)
}

// getLimitedDXFCPU returns the CPU slots available to DXF under the given percentage limit.
func getLimitedDXFCPU(totalCPU int, limit int) int {
	if totalCPU <= 0 || limit >= 100 {
		return totalCPU
	}
	// use CEIL might cause the real limit to be higher than the given limit.
	// as DXF use slots or CPU cores as the unit of resource, that's acceptable.
	usableCPU := int(math.Ceil(float64(totalCPU) * float64(limit) / 100))
	if usableCPU < 1 {
		return 1
	}
	return min(usableCPU, totalCPU)
}

// NodeResourceForTest is only used for test.
var NodeResourceForTest = NewNodeResource(32, 32*units.GB, 100*units.GB)

// GetStepResource gets the step resource according to slots.
func (nr *NodeResource) GetStepResource(task *TaskBase) *StepResource {
	slots := task.GetRuntimeSlots()
	return &StepResource{
		CPU: NewAllocatable(int64(slots)),
		// same proportion as CPU
		Mem: NewAllocatable(int64(float64(slots) / float64(nr.TotalCPU) * float64(nr.TotalMem))),
	}
}

// GetTaskDiskResource gets available disk for a task.
func (nr *NodeResource) GetTaskDiskResource(task *TaskBase, quotaHint uint64) uint64 {
	slots := task.GetRuntimeSlots()
	availableDisk := min(nr.TotalDisk, quotaHint)
	return uint64(float64(slots) / float64(nr.TotalCPU) * float64(availableDisk))
}
