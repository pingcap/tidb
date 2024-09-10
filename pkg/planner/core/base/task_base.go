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

package base

// Note: appending the new adding method to the last, for the convenience of easy
// locating in other implementor from other package.

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
	// Here we change return type as interface to avoid import cycle.
	// Basic interface definition shouldn't depend on concrete implementation structure.
	ConvertToRootTask(ctx PlanContext) Task
	// MemoryUsage returns the memory usage of current task.
	MemoryUsage() int64
}

// InvalidTask is just a common invalid singleton instance initialized by core's empty RootTask.
var InvalidTask Task
