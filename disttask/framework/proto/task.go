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
//  1. succeed:			pending -> running -> succeed
//  2. failed:			pending -> running -> reverting -> reverted/revert_failed
//  3. canceled:		pending -> running -> cancelling -> reverting -> reverted/revert_failed
//  3. pause/resume:	pending -> running -> pausing -> paused -> running
//
// subtask state machine
//  1. succeed/failed:	pending -> running -> succeed/failed
//  2. canceled:		pending -> running -> canceled
//  3. rollback:		revert_pending -> reverting -> reverted/revert_failed
//  4. pause/resume:	pending -> running -> paused -> running
const (
	TaskStatePending       = "pending"
	TaskStateRunning       = "running"
	TaskStateSucceed       = "succeed"
	TaskStateReverting     = "reverting"
	TaskStateFailed        = "failed"
	TaskStateRevertFailed  = "revert_failed"
	TaskStateCancelling    = "cancelling"
	TaskStateCanceled      = "canceled"
	TaskStatePausing       = "pausing"
	TaskStatePaused        = "paused"
	TaskStateRevertPending = "revert_pending"
	TaskStateReverted      = "reverted"
)

// TaskStep is the step of task.
const (
	StepInit int64 = -1
	StepOne  int64 = 1
	StepTwo  int64 = 2
)

// Task represents the task of distribute framework.
type Task struct {
	ID    int64
	Key   string
	Type  string
	State string
	Step  int64
	// not used now.
	DispatcherID    string
	Concurrency     uint64
	StartTime       time.Time
	StateUpdateTime time.Time
	Meta            []byte
	Error           []byte
}

// IsFinished checks if the task is finished.
func (t *Task) IsFinished() bool {
	return t.State == TaskStateSucceed || t.State == TaskStateReverted
}

// Subtask represents the subtask of distribute framework.
// Each task is divided into multiple subtasks by dispatcher.
type Subtask struct {
	ID   int64
	Step int64
	Type string
	// taken from task_key of the subtask table
	TaskID int64
	State  string
	// SchedulerID is the ID of scheduler, right now it's the same as instance_id, exec_id, and ID in ServerInfo.
	// ID in ServerInfo is a UUID generated on startup, so it changes every time the server restarts.
	SchedulerID string
	StartTime   uint64
	EndTime     time.Time
	Meta        []byte
}

// NewSubtask create a new subtask.
func NewSubtask(taskID int64, tp, schedulerID string, meta []byte) *Subtask {
	return &Subtask{
		Type:        tp,
		TaskID:      taskID,
		SchedulerID: schedulerID,
		Meta:        meta,
	}
}

// MinimalTask is the minimal task of distribute framework.
// Each subtask is divided into multiple minimal tasks by scheduler.
type MinimalTask interface {
	// IsMinimalTask is a marker to check if it is a minimal task for compiler.
	IsMinimalTask()
}

const (
	// TaskTypeExample is TaskType of Example.
	TaskTypeExample = "Example"
	// LoadData is TaskType of LoadData.
	LoadData = "LoadData"
)

// Type2Int converts task type to int.
func Type2Int(t string) int {
	switch t {
	case TaskTypeExample:
		return 1
	case LoadData:
		return 2
	default:
		return 0
	}
}

// Int2Type converts int to task type.
func Int2Type(i int) string {
	switch i {
	case 1:
		return TaskTypeExample
	case 2:
		return LoadData
	default:
		return ""
	}
}
