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
	"cmp"
	"fmt"
	"time"
)

// see doc.go for more details.
const (
	TaskStatePending            TaskState = "pending"
	TaskStateRunning            TaskState = "running"
	TaskStateSucceed            TaskState = "succeed"
	TaskStateFailed             TaskState = "failed"
	TaskStateReverting          TaskState = "reverting"
	TaskStateAwaitingResolution TaskState = "awaiting-resolution"
	TaskStateReverted           TaskState = "reverted"
	TaskStateCancelling         TaskState = "cancelling"
	TaskStatePausing            TaskState = "pausing"
	TaskStatePaused             TaskState = "paused"
	TaskStateResuming           TaskState = "resuming"
	TaskStateModifying          TaskState = "modifying"
)

type (
	// TaskState is the state of task.
	TaskState string
	// TaskType is the type of task.
	TaskType string
)

func (t TaskType) String() string {
	return string(t)
}

func (s TaskState) String() string {
	return string(s)
}

// CanMoveToModifying checks if current state can move to 'modifying' state.
func (s TaskState) CanMoveToModifying() bool {
	return s == TaskStatePending || s == TaskStateRunning || s == TaskStatePaused
}

const (
	// TaskIDLabelName is the label name of task id.
	TaskIDLabelName = "task_id"
	// NormalPriority represents the normal priority of task.
	NormalPriority = 512
)

// MaxConcurrentTask is the max concurrency of task.
// TODO: remove this limit later.
var MaxConcurrentTask = 16

// ExtraParams is the extra params of task.
// Note: only store params that's not used for filter or sort in this struct.
type ExtraParams struct {
	// ManualRecovery indicates whether the task can be recovered manually.
	// if enabled, the task will enter 'awaiting-resolution' state when it failed,
	// then the user can recover the task manually or fail it if it's not recoverable.
	ManualRecovery bool `json:"manual_recovery,omitempty"`
	// MaxRuntimeSlots is the max slots when running subtasks of this task in
	// TargetSteps steps.
	// normally it's 0, means we will use the RequiredSlots to run the subtasks.
	// if set, we will use the min of RequiredSlots and MaxRuntimeSlots as the
	// execution effective slots of the task step defined in TargetSteps.
	// this field is used to workaround OOM issue where TiDB might repeatedly
	// restart. the DXF framework won't detect changes in this field, so it's not
	// part of normal schedule workflow, when TiDB restarts the newest value will
	// be used.
	// RequiredSlots might be modified, and MaxRuntimeSlots is not touched in this
	// case due to above reason, so MaxRuntimeSlots might > RequiredSlots.
	MaxRuntimeSlots int `json:"max_runtime_slots,omitempty"`
	// TargetSteps indicates the steps that MaxRuntimeSlots takes effect.
	// if empty or nil, MaxRuntimeSlots takes effect in all steps.
	// normally OOM only happens in some specific steps, so we can just limit the
	// concurrency in those steps to reduce the impact on the overall performance.
	TargetSteps []Step `json:"target_steps,omitempty"`
}

// TaskBase contains the basic information of a task.
// we define this to avoid load task meta which might be very large into memory.
type TaskBase struct {
	ID    int64
	Key   string
	Type  TaskType
	State TaskState
	Step  Step
	// Priority is the priority of task, the smaller value means the higher priority.
	// valid range is [1, 1024], default is NormalPriority.
	Priority int
	// RequiredSlots is the required slots of the task.
	// we use this field to allocate slots when scheduling and creating the task
	// executor, but the effective slots when running the task is determined by
	// GetEffectiveSlots.
	// in normal case, they are the same. but when meeting OOM and TiDB repeatedly
	// restarts, we might set a lower MaxRuntimeSlots in ExtraParams, then the
	// effective slots is smaller than RequiredSlots.
	// Note: in application layer, don't use this field directly, use GetEffectiveSlots
	// or GetResource of step executor instead.
	// Note: in the system table, we store it inside 'concurrency' column as
	// required slots is introduced later.
	RequiredSlots int
	// TargetScope indicates that the task should be running on tidb nodes which
	// contain the tidb_service_scope=TargetScope label.
	// To be compatible with previous version, if it's "" or "background", the
	// task try run on nodes of "background" scope,
	// if there is no such nodes, will try nodes of "" scope.
	TargetScope  string
	CreateTime   time.Time
	MaxNodeCount int
	ExtraParams  ExtraParams
	// keyspace name is the keyspace that the task belongs to.
	// it's only useful for nextgen cluster.
	Keyspace string
}

// IsDone checks if the task is done.
func (t *TaskBase) IsDone() bool {
	return t.State == TaskStateSucceed || t.State == TaskStateReverted ||
		t.State == TaskStateFailed
}

// CompareTask a wrapper of Compare.
func (t *TaskBase) CompareTask(other *Task) int {
	return t.Compare(&other.TaskBase)
}

// Compare compares two tasks by task rank.
// returns < 0 represents rank of t is higher than 'other'.
func (t *TaskBase) Compare(other *TaskBase) int {
	if r := cmp.Compare(t.Priority, other.Priority); r != 0 {
		return r
	}
	if r := t.CreateTime.Compare(other.CreateTime); r != 0 {
		return r
	}
	return cmp.Compare(t.ID, other.ID)
}

// GetEffectiveSlots gets the effective slots of current task step.
// application layer might use this as the concurrency of the task step.
func (t *TaskBase) GetEffectiveSlots() int {
	if t.ExtraParams.MaxRuntimeSlots > 0 {
		if len(t.ExtraParams.TargetSteps) == 0 {
			return min(t.ExtraParams.MaxRuntimeSlots, t.RequiredSlots)
		}
		for _, step := range t.ExtraParams.TargetSteps {
			if step == t.Step {
				return min(t.ExtraParams.MaxRuntimeSlots, t.RequiredSlots)
			}
		}
	}
	return t.RequiredSlots
}

// String implements fmt.Stringer interface.
func (t *TaskBase) String() string {
	return fmt.Sprintf("{id: %d, key: %s, type: %s, state: %s, step: %s, priority: %d, required slots: %d, target scope: %s, create time: %s}",
		t.ID, t.Key, t.Type, t.State, Step2Str(t.Type, t.Step), t.Priority, t.RequiredSlots, t.TargetScope, t.CreateTime.Format(time.RFC3339Nano))
}

// Task represents the task of distributed framework, see doc.go for more details.
type Task struct {
	TaskBase
	// SchedulerID is not used now.
	SchedulerID     string
	StartTime       time.Time
	StateUpdateTime time.Time
	// Meta is the metadata of task, it's read-only in most cases, but it can be
	// changed in below case, and framework will update the task meta in the storage.
	// 	- task switches to next step in Scheduler.OnNextSubtasksBatch
	// 	- on task cleanup, we might do some redaction on the meta.
	// 	- on task 'modifying', params inside the meta can be changed.
	Meta        []byte
	Error       error
	ModifyParam ModifyParam
}

var (
	// EmptyMeta is the empty meta of task/subtask.
	EmptyMeta = []byte("{}")
)
