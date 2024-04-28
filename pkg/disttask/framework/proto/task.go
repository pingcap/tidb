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
	"time"
)

// task state machine
//
// Note: if a task fails during running, it will end with `reverted` state.
// The `failed` state is used to mean the framework cannot run the task, such as
// invalid task type, scheduler init error(fatal), etc.
//
//	                            ┌────────┐
//	                ┌───────────│resuming│◄────────┐
//	                │           └────────┘         │
//	┌──────┐        │           ┌───────┐       ┌──┴───┐
//	│failed│        │ ┌────────►│pausing├──────►│paused│
//	└──────┘        │ │         └───────┘       └──────┘
//	   ▲            ▼ │
//	┌──┴────┐     ┌───┴───┐     ┌────────┐
//	│pending├────►│running├────►│succeed │
//	└──┬────┘     └──┬┬───┘     └────────┘
//	   │             ││         ┌─────────┐     ┌────────┐
//	   │             │└────────►│reverting├────►│reverted│
//	   │             ▼          └─────────┘     └────────┘
//	   │          ┌──────────┐    ▲
//	   └─────────►│cancelling├────┘
//	              └──────────┘
const (
	TaskStatePending    TaskState = "pending"
	TaskStateRunning    TaskState = "running"
	TaskStateSucceed    TaskState = "succeed"
	TaskStateFailed     TaskState = "failed"
	TaskStateReverting  TaskState = "reverting"
	TaskStateReverted   TaskState = "reverted"
	TaskStateCancelling TaskState = "cancelling"
	TaskStatePausing    TaskState = "pausing"
	TaskStatePaused     TaskState = "paused"
	TaskStateResuming   TaskState = "resuming"
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

const (
	// TaskIDLabelName is the label name of task id.
	TaskIDLabelName = "task_id"
	// NormalPriority represents the normal priority of task.
	NormalPriority = 512
)

// MaxConcurrentTask is the max concurrency of task.
// TODO: remove this limit later.
var MaxConcurrentTask = 16

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
	// Concurrency controls the max resource usage of the task, i.e. the max number
	// of slots the task can use on each node.
	Concurrency int
	// TargetScope indicates that the task should be running on tidb nodes which
	// contain the tidb_service_scope=TargetScope label.
	// To be compatible with previous version, if it's "" or "background", the task try run on nodes of "background" scope,
	// if there is no such nodes, will try nodes of "" scope.
	TargetScope string
	CreateTime  time.Time
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
	if t.Priority != other.Priority {
		return t.Priority - other.Priority
	}
	if t.CreateTime != other.CreateTime {
		if t.CreateTime.Before(other.CreateTime) {
			return -1
		}
		return 1
	}
	return int(t.ID - other.ID)
}

// Task represents the task of distributed framework.
// A task is abstracted as multiple steps that runs in sequence, each step contains
// multiple sub-tasks that runs in parallel, such as:
//
//	task
//	├── step1
//	│   ├── subtask1
//	│   ├── subtask2
//	│   └── subtask3
//	└── step2
//	    ├── subtask1
//	    ├── subtask2
//	    └── subtask3
//
// tasks are run in the order of rank, and the rank is defined by:
//
//	priority asc, create_time asc, id asc.
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
	Meta  []byte
	Error error
}

var (
	// EmptyMeta is the empty meta of task/subtask.
	EmptyMeta = []byte("{}")
)
