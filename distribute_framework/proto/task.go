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

const (
	TaskTypeExample     = "example"
	TaskTypeCreateIndex = "create_index"
	TaskTypeImport      = "import"
	TaskTypeTTL         = "ttl"
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

const (
	StepInit int64 = iota
	StepOne
	StepTwo
)

type Task struct {
	ID    int64
	Type  string
	State string
	// TODO: redefine
	Meta GlobalTaskMeta
	Step int64

	DispatcherID string
	StartTime    time.Time

	Concurrency uint64
}

type Subtask struct {
	ID          int64
	Type        string
	TaskID      int64
	State       string
	SchedulerID string
	Meta        SubTaskMeta

	StartTime time.Time
}

func (st *Subtask) String() string {
	return ""
}

type MinimalTask interface{}

type GlobalTaskMeta interface {
	Serialize() []byte
	GetType() string
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

func (g *SimpleNumberGTaskMeta) GetType() string {
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
