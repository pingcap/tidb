// Copyright 2022 PingCAP, Inc.
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
//  2. failed:			pending -> running -> reverting -> failed/revert_failed
//  3. canceled:		pending -> running -> reverting -> canceled/revert_failed
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
	TaskStateCanceled      = "canceled"
	TaskStatePausing       = "pausing"
	TaskStatePaused        = "paused"
	TaskStateRevertPending = "revert_pending"
	TaskStateReverted      = "reverted"
)

// TaskStep is the step of task.
const (
	StepInit int64 = -1
)

// Task represents the task of distribute framework.
type Task struct {
	ID           int64
	Type         string
	State        string
	Step         int64
	DispatcherID string
	Concurrency  uint64
	StartTime    time.Time
	Meta         []byte
}

// Subtask represents the subtask of distribute framework.
// Each task is divided into multiple subtasks by dispatcher.
type Subtask struct {
	ID          int64
	Type        string
	TaskID      int64
	State       string
	SchedulerID string
	StartTime   time.Time
	Meta        []byte
}

// MinimalTask is the minimal task of distribute framework.
// Each subtask is divided into multiple minimal tasks by scheduler.
type MinimalTask interface{}
