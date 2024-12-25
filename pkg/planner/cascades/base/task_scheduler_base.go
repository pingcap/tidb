// Copyright 2024 PingCAP, Inc.
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

package base

// Scheduler is a scheduling interface defined for serializing(single thread)/concurrent(multi thread) running.
type Scheduler interface {
	// ExecuteTasks start the internal scheduling.
	ExecuteTasks() error
	// Destroy release the internal resource if any.
	Destroy()
	// PushTask is outside portal for inserting a new task in. task running can also trigger another successive task.
	PushTask(task Task)
}
