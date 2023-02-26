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

package scheduler

import "time"

var (
	heartbeatInterval = 5 * time.Second
)

type TaskID uint64

type SubtaskID uint64

type TaskType uint64

type TiDBID string

const (
	TaskTypeExample TaskType = iota
	TaskTypeCreateIndex
	TaskTypeImport
	TaskTypeTTL
)

type TaskStatus uint64

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusRunning
	TaskStatusReverting
	TaskStatusPaused
	TaskStatusFailed
	TaskStatusSucceed
	TaskStatusRevertFailed
	TaskStatusCanceled
)

type Task struct {
	Type        TaskType
	ID          TaskID
	Status      TaskStatus
	Concurrency uint64
}

type Subtask struct {
	Type   TaskType
	TaskID TaskID
	ID     SubtaskID
	Status TaskStatus
}

type Pool interface {
	Run(func()) error
	RunWithConcurrency(func(), uint64) error
	ReleaseAndWait()
}

type DefaultPool struct{}

func (*DefaultPool) Run(f func()) error {
	go f()
	return nil
}

func (*DefaultPool) RunWithConcurrency(f func(), i uint64) error {
	f()
	return nil
}

func (*DefaultPool) ReleaseAndWait() {}

func NewPool(concurrency int) Pool {
	return &DefaultPool{}
}

type GlobalTaskTable interface {
	GetRunningTasks() []*Task
	GetCanceledTasks() []*Task
}

type SubtaskTable interface {
	HasRunningSubtasks(TaskID, TiDBID) bool
	GetRunningSubtasks(TaskID) []*Subtask
	UpdateSubtaskStatus(SubtaskID, TaskStatus)
	UpdateHeartbeat(TaskID, TiDBID)
}

type DefaultGlobalTaskTable struct{}

func (*DefaultGlobalTaskTable) GetRunningTasks() []*Task {
	return []*Task{}
}

func (*DefaultGlobalTaskTable) GetCanceledTasks() []*Task {
	return []*Task{}
}

func NewGlobalTaskTable() GlobalTaskTable {
	return &DefaultGlobalTaskTable{}
}

func NewSubtaskTable() SubtaskTable {
	return &DefaultSubtaskTable{}
}

type DefaultSubtaskTable struct{}

func (*DefaultSubtaskTable) GetRunningSubtasks(id TaskID) []*Subtask {
	return []*Subtask{}
}

func (*DefaultSubtaskTable) HasRunningSubtasks(taskID TaskID, tidbID TiDBID) bool {
	return true
}

func (*DefaultSubtaskTable) UpdateSubtaskStatus(id SubtaskID, status TaskStatus) {}

func (*DefaultSubtaskTable) UpdateHeartbeat(id TaskID, tidbID TiDBID) {}
