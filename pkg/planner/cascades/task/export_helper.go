// Copyright 2026 PingCAP, Inc.
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

package task

import (
	"sync"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
)

// Exported types for testing

// ExportedStack exports Stack for testing
type ExportedStack = Stack

// ExportedNewTaskStack exports newTaskStack for testing
func ExportedNewTaskStack() *Stack {
	return newTaskStack()
}

// ExportedNewTaskStackWithCap exports newTaskStackWithCap for testing
func ExportedNewTaskStackWithCap(c int) *Stack {
	return newTaskStackWithCap(c)
}

// ExportedGetStackPool exports stackPool for testing
func ExportedGetStackPool() *sync.Pool {
	return &stackPool
}

// ExportedGetStackTasks gets the tasks field from a Stack
func ExportedGetStackTasks(s *Stack) []base.Task {
	return s.tasks
}

// ExportedSetStackTasks sets the tasks field for a Stack
func ExportedSetStackTasks(s *Stack, tasks []base.Task) {
	s.tasks = tasks
}

// ExportedNewSimpleTaskScheduler exports NewSimpleTaskScheduler for testing
func ExportedNewSimpleTaskScheduler() base.Scheduler {
	return NewSimpleTaskScheduler()
}
