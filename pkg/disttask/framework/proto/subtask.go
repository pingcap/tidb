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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
)

// subtask state machine for normal subtask:
//
// NOTE: `running` -> `pending` only happens when some node is taken as dead, so
// its running subtask is balanced to other node, and the subtask is idempotent,
// we do this to make the subtask can be scheduled to other node again, it's NOT
// a normal state transition.
//
//	               ┌──────────────┐
//	               │          ┌───┴──┐
//	               │ ┌───────►│paused│
//	               ▼ │        └──────┘
//	┌───────┐    ┌───┴───┐    ┌───────┐
//	│pending├───►│running├───►│succeed│
//	└───────┘    └┬──┬───┘    └───────┘
//	     ▲        │  │        ┌──────┐
//	     └────────┘  ├───────►│failed│
//	                 │        └──────┘
//	                 │        ┌────────┐
//	                 └───────►│canceled│
//	                          └────────┘
const (
	SubtaskStatePending  SubtaskState = "pending"
	SubtaskStateRunning  SubtaskState = "running"
	SubtaskStateSucceed  SubtaskState = "succeed"
	SubtaskStateFailed   SubtaskState = "failed"
	SubtaskStateCanceled SubtaskState = "canceled"
	SubtaskStatePaused   SubtaskState = "paused"
)

type (
	// SubtaskState is the state of subtask.
	SubtaskState string
)

func (s SubtaskState) String() string {
	return string(s)
}

// SubtaskBase contains the basic information of a subtask.
// we define this to avoid load subtask meta which might be very large into memory.
type SubtaskBase struct {
	ID   int64
	Step Step
	Type TaskType
	// taken from task_key of the subtask table
	TaskID int64
	State  SubtaskState
	// Concurrency is the concurrency of the subtask, should <= task's concurrency.
	// some subtasks like post-process of import into, don't consume too many resources,
	// can lower this value.
	Concurrency int
	// ExecID is the ID of target executor, right now it's the same as instance_id,
	// its value is IP:PORT, see GenerateExecID
	ExecID     string
	CreateTime time.Time
	// StartTime is the time when the subtask is started.
	// it's 0 if it hasn't started yet.
	StartTime time.Time
	// Ordinal is the ordinal of subtask, should be unique for some task and step.
	// starts from 1.
	Ordinal int
}

func (t *SubtaskBase) String() string {
	return fmt.Sprintf("[ID=%d, Step=%d, Type=%s, TaskID=%d, State=%s, ExecID=%s]",
		t.ID, t.Step, t.Type, t.TaskID, t.State, t.ExecID)
}

// IsDone checks if the subtask is done.
func (t *SubtaskBase) IsDone() bool {
	return t.State == SubtaskStateSucceed || t.State == SubtaskStateCanceled ||
		t.State == SubtaskStateFailed
}

// Subtask represents the subtask of distribute framework.
// subtasks of a task are run in parallel on different nodes, but on each node,
// at most 1 subtask can be run at the same time, see StepExecutor too.
type Subtask struct {
	SubtaskBase
	// UpdateTime is the time when the subtask is updated.
	// it can be used as subtask end time if the subtask is finished.
	// it's 0 if it hasn't started yet.
	UpdateTime time.Time
	// Meta is the metadata of subtask, should not be nil.
	// meta of different subtasks of same step must be different too.
	// NOTE: this field can be changed by StepExecutor.OnFinished method, to store
	// some result, and framework will update the subtask meta in the storage.
	// On other code path, this field should be read-only.
	Meta    []byte
	Summary string
}

// NewSubtask create a new subtask.
func NewSubtask(step Step, taskID int64, tp TaskType, execID string, concurrency int, meta []byte, ordinal int) *Subtask {
	s := &Subtask{
		SubtaskBase: SubtaskBase{
			Step:        step,
			Type:        tp,
			TaskID:      taskID,
			ExecID:      execID,
			Concurrency: concurrency,
			Ordinal:     ordinal,
		},
		Meta: meta,
	}
	return s
}

// Allocatable is a resource with capacity that can be allocated, it's routine safe.
type Allocatable struct {
	capacity int64
	used     atomic.Int64
}

// NewAllocatable creates a new Allocatable.
func NewAllocatable(capacity int64) *Allocatable {
	return &Allocatable{capacity: capacity}
}

// Capacity returns the capacity of the Allocatable.
func (a *Allocatable) Capacity() int64 {
	return a.capacity
}

// Used returns the used resource of the Allocatable.
func (a *Allocatable) Used() int64 {
	return a.used.Load()
}

// Alloc allocates v from the Allocatable.
func (a *Allocatable) Alloc(n int64) bool {
	for {
		used := a.used.Load()
		if used+n > a.capacity {
			return false
		}
		if a.used.CompareAndSwap(used, used+n) {
			return true
		}
	}
}

// Free frees v from the Allocatable.
func (a *Allocatable) Free(n int64) {
	a.used.Add(-n)
}

// StepResource is the max resource that a task step can use.
// it's also the max resource that a subtask can use, as we run subtasks of task
// step in sequence.
type StepResource struct {
	CPU *Allocatable
	Mem *Allocatable
}

// String implements Stringer interface.
func (s *StepResource) String() string {
	return fmt.Sprintf("[CPU=%d, Mem=%s]", s.CPU.Capacity(),
		units.BytesSize(float64(s.Mem.Capacity())))
}
