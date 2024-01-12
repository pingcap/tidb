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
	"time"
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
//
// for reverting subtask:
//
//	┌──────────────┐    ┌─────────┐   ┌─────────┐
//	│revert_pending├───►│reverting├──►│ reverted│
//	└──────────────┘    └────┬────┘   └─────────┘
//	                         │         ┌─────────────┐
//	                         └────────►│revert_failed│
//	                                   └─────────────┘
//	 1. succeed/failed:	pending -> running -> succeed/failed
//	 2. canceled:		pending -> running -> canceled
//	 3. rollback:		revert_pending -> reverting -> reverted/revert_failed
//	 4. pause/resume:	pending -> running -> paused -> running
const (
	SubtaskStatePending       SubtaskState = "pending"
	SubtaskStateRunning       SubtaskState = "running"
	SubtaskStateSucceed       SubtaskState = "succeed"
	SubtaskStateFailed        SubtaskState = "failed"
	SubtaskStateCanceled      SubtaskState = "canceled"
	SubtaskStatePaused        SubtaskState = "paused"
	SubtaskStateRevertPending SubtaskState = "revert_pending"
	SubtaskStateReverting     SubtaskState = "reverting"
	SubtaskStateReverted      SubtaskState = "reverted"
	SubtaskStateRevertFailed  SubtaskState = "revert_failed"
)

type (
	// SubtaskState is the state of subtask.
	SubtaskState string
)

func (s SubtaskState) String() string {
	return string(s)
}

// Subtask represents the subtask of distribute framework.
// Each task is divided into multiple subtasks by scheduler.
type Subtask struct {
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
	// UpdateTime is the time when the subtask is updated.
	// it can be used as subtask end time if the subtask is finished.
	// it's 0 if it hasn't started yet.
	UpdateTime time.Time
	// Meta is the metadata of subtask, should not be nil.
	// meta of different subtasks of same step must be different too.
	Meta    []byte
	Summary string
	// Ordinal is the ordinal of subtask, should be unique for some task and step.
	// starts from 1, for reverting subtask, it's NULL in database.
	Ordinal int
}

func (t *Subtask) String() string {
	return fmt.Sprintf("Subtask[ID=%d, Step=%d, Type=%s, TaskID=%d, State=%s, ExecID=%s]",
		t.ID, t.Step, t.Type, t.TaskID, t.State, t.ExecID)
}

// IsDone checks if the subtask is done.
func (t *Subtask) IsDone() bool {
	return t.State == SubtaskStateSucceed || t.State == SubtaskStateReverted || t.State == SubtaskStateCanceled ||
		t.State == SubtaskStateFailed || t.State == SubtaskStateRevertFailed
}

// NewSubtask create a new subtask.
func NewSubtask(step Step, taskID int64, tp TaskType, execID string, concurrency int, meta []byte, ordinal int) *Subtask {
	s := &Subtask{
		Step:        step,
		Type:        tp,
		TaskID:      taskID,
		ExecID:      execID,
		Concurrency: concurrency,
		Meta:        meta,
		Ordinal:     ordinal,
	}
	return s
}
