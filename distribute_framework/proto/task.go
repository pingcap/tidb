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

	"github.com/pingcap/tidb/distribute_framework/dispatcher"
)

type TaskID uint64

type TaskType string

type TiDBID string

const (
	TaskTypeExample     TaskType = "example"
	TaskTypeCreateIndex TaskType = "create_index"
	TaskTypeImport      TaskType = "import"
	TaskTypeTTL         TaskType = "ttl"
)

type TaskState string

const (
	TaskStatePending      TaskState = "pending"
	TaskStateRunning      TaskState = "running"
	TaskStateReverting    TaskState = "reverting"
	TaskStatePaused       TaskState = "paused"
	TaskStateFailed       TaskState = "failed"
	TaskStateSucceed      TaskState = "succeed"
	TaskStateRevertFailed TaskState = "revert_failed"
	TaskStateCanceled     TaskState = "canceled"
)

type Task struct {
	ID    TaskID
	Type  TaskType
	State TaskState
	Meta  []byte
	// TODO: redefine
	MetaM *TaskMeta

	DispatcherID string
	StartTime    time.Time

	Concurrency uint64
}

type TaskMeta struct {
	DistPlan *dispatcher.DistPlanner
}

type SubtaskID uint64

type Subtask struct {
	ID          SubtaskID
	Type        TaskType
	TaskID      TaskID
	State       TaskState
	SchedulerID TiDBID
	Meta        []byte

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
