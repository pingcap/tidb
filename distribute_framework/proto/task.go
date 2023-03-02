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
	"encoding/json"
	"time"
)

type TaskID uint64

type TaskType string

type InstanceID string

const (
	TaskTypeExample     TaskType = "example"
	TaskTypeCreateIndex TaskType = "create_index"
	TaskTypeImport      TaskType = "import"
	TaskTypeTTL         TaskType = "ttl"
)

type TaskState string

// task state machine
//  1. succeed:			pending -> running -> succeed
//  2. failed:			pending -> running -> canceling -> reverting -> failed/revert_failed
//  3. canceled:		pending -> running -> canceling -> reverting -> canceled/revert_failed
//  3. pause/resume:	pending -> running -> pausing -> paused -> resuming -> running
//
// subtask state machine
//  1. succeed/failed:	pending -> running -> succeed/failed
//  2. canceled:		pending -> running -> canceled
//  3. rollback:		revert_pending -> reverting -> reverted/revert_failed/canceled
//  4. pause/resume:	pending -> running -> paused -> running
const (
	TaskStatePending       TaskState = "pending"
	TaskStateRunning       TaskState = "running"
	TaskStateSucceed       TaskState = "succeed"
	TaskStateCanceling     TaskState = "canceling"
	TaskStateReverting     TaskState = "reverting"
	TaskStateFailed        TaskState = "failed"
	TaskStateRevertFailed  TaskState = "revert_failed"
	TaskStateCanceled      TaskState = "canceled"
	TaskStatePausing       TaskState = "pausing"
	TaskStatePaused        TaskState = "paused"
	TaskStateRevertPending TaskState = "revert_pending"
	TaskStateReverted      TaskState = "reverted"
)

type TaskStep int

const (
	StepInit TaskStep = -1
	StepOne  TaskStep = iota
	StepTwo
)

type Task struct {
	ID    TaskID
	Type  TaskType
	State TaskState
	// TODO: redefine
	Meta GlobalTaskMeta
	Step TaskStep

	DispatcherID string
	StartTime    time.Time

	Concurrency uint64
}

type SubtaskID uint64

type Subtask struct {
	ID          SubtaskID
	Type        TaskType
	TaskID      TaskID
	State       TaskState
	SchedulerID InstanceID
	Meta        SubTaskMeta

	StartTime time.Time
}

func (st *Subtask) String() string {
	return ""
}

type GlobalTaskMeta interface {
	Serialize() []byte
	GetType() TaskType
	GetConcurrency() uint64
}

type SubTaskMeta interface {
	Serialize() []byte
}

func UnSerializeGlobalTaskMeta(b []byte) GlobalTaskMeta {
	if b[0] == 0x1 {
		return &SimpleNumberGTaskMeta{}
	}
	return nil
}

func UnSerializeSubTaskMeta(b []byte) SubTaskMeta {
	if b[0] == 0x1 {
		m := &SimpleNumberSTaskMeta{}
		err := json.Unmarshal(b[1:], &m.Numbers)
		if err != nil {
			// TODO: handle error
		}
		return m
	}
	return nil
}

// SimpleNumberGTaskMeta is a simple implementation of GlobalTaskMeta.
type SimpleNumberGTaskMeta struct {
}

func (g *SimpleNumberGTaskMeta) Serialize() []byte {
	return []byte{0x1}
}

func (g *SimpleNumberGTaskMeta) GetType() TaskType {
	return TaskTypeExample
}

func (g *SimpleNumberGTaskMeta) GetConcurrency() uint64 {
	return 4
}

type SimpleNumberSTaskMeta struct {
	Numbers []int `json:"numbers"`
}

func (g *SimpleNumberSTaskMeta) Serialize() []byte {
	head := []byte{0x1}
	jsonMeta, err := json.Marshal(g.Numbers)
	if err != nil {
		// TODO handle error
		return head
	}
	return append(head, jsonMeta...)
}
