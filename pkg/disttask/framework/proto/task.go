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
//		                            ┌────────┐
//		                ┌───────────│resuming│◄────────┐
//		                │           └────────┘         │
//		┌──────┐        │           ┌───────┐       ┌──┴───┐
//		│failed│        │ ┌────────►│pausing├──────►│paused│
//		└──────┘        │ │         └───────┘       └──────┘
//		   ▲            ▼ │
//		┌──┴────┐     ┌───┴───┐     ┌────────┐
//		│pending├────►│running├────►│succeed │
//		└──┬────┘     └──┬┬───┘     └────────┘
//		   │             ││         ┌─────────┐     ┌────────┐
//		   │             │└────────►│reverting├────►│reverted│
//		   │             ▼          └────┬────┘     └────────┘
//		   │          ┌──────────┐    ▲  │          ┌─────────────┐
//		   └─────────►│cancelling├────┘  └─────────►│revert_failed│
//		              └──────────┘                  └─────────────┘
//	 1. succeed:		pending -> running -> succeed
//	 2. failed:			pending -> running -> reverting -> reverted/revert_failed, pending -> failed
//	 3. canceled:		pending -> running -> cancelling -> reverting -> reverted/revert_failed
//	 4. pause/resume:	pending -> running -> pausing -> paused -> running
//
// TODO: we don't have revert_failed task for now.
const (
	TaskStatePending      TaskState = "pending"
	TaskStateRunning      TaskState = "running"
	TaskStateSucceed      TaskState = "succeed"
	TaskStateFailed       TaskState = "failed"
	TaskStateReverting    TaskState = "reverting"
	TaskStateReverted     TaskState = "reverted"
	TaskStateRevertFailed TaskState = "revert_failed"
	TaskStateCancelling   TaskState = "cancelling"
	TaskStatePausing      TaskState = "pausing"
	TaskStatePaused       TaskState = "paused"
	TaskStateResuming     TaskState = "resuming"
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
var MaxConcurrentTask = 4

// Task represents the task of distributed framework.
// tasks are run in the order of: priority asc, create_time asc, id asc.
type Task struct {
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
	CreateTime  time.Time

	// depends on query, below fields might not be filled.

	// SchedulerID is not used now.
	SchedulerID     string
	StartTime       time.Time
	StateUpdateTime time.Time
	Meta            []byte
	Error           error
}

// IsDone checks if the task is done.
func (t *Task) IsDone() bool {
	return t.State == TaskStateSucceed || t.State == TaskStateReverted ||
		t.State == TaskStateFailed
}

var (
	// EmptyMeta is the empty meta of task/subtask.
	EmptyMeta = []byte("{}")
)

// Compare compares two tasks by task order.
// returns < 0 represents priority of t is higher than other.
func (t *Task) Compare(other *Task) int {
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
