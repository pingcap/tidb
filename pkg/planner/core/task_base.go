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
	"github.com/pingcap/tidb/pkg/util/size"
)

var (
	_ Task = &RootTask{}
)

// Task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTaskMeta or a ParallelTask.
type Task interface {
	// Count returns current task's row count.
	Count() float64
	// Copy return a shallow copy of current task with the same pointer to p.
	Copy() Task
	// Plan returns current task's plan.
	Plan() PhysicalPlan
	// Invalid returns whether current task is invalid.
	Invalid() bool
	// ConvertToRootTask will convert current task as root type.
	ConvertToRootTask(ctx PlanContext) *RootTask
	// MemoryUsage returns the memory usage of current task.
	MemoryUsage() int64
}

// RootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type RootTask struct {
	p       PhysicalPlan
	isEmpty bool // isEmpty indicates if this task contains a dual table and returns empty data.
	// TODO: The flag 'isEmpty' is only checked by Projection and UnionAll. We should support more cases in the future.
}

// GetPlan returns the root task's plan.
func (t *RootTask) GetPlan() PhysicalPlan {
	return t.p
}

// SetPlan sets the root task' plan.
func (t *RootTask) SetPlan(p PhysicalPlan) {
	t.p = p
}

// IsEmpty indicates whether root task is empty.
func (t *RootTask) IsEmpty() bool {
	return t.isEmpty
}

// SetEmpty set the root task as empty.
func (t *RootTask) SetEmpty(x bool) {
	t.isEmpty = x
}

// Copy implements Task interface.
func (t *RootTask) Copy() Task {
	return &RootTask{
		p: t.p,
	}
}

// ConvertToRootTask implements Task interface.
func (t *RootTask) ConvertToRootTask(_ PlanContext) *RootTask {
	return t.Copy().(*RootTask)
}

// Invalid implements Task interface.
func (t *RootTask) Invalid() bool {
	return t.p == nil
}

// Count implements Task interface.
func (t *RootTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Plan implements Task interface.
func (t *RootTask) Plan() PhysicalPlan {
	return t.p
}

// MemoryUsage return the memory usage of rootTask
func (t *RootTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}
	sum = size.SizeOfInterface + size.SizeOfBool
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return sum
}
