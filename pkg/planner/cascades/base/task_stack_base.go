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

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
)

// Stack is abstract definition of task container.(TaskStack is a kind of array stack implementation of it)
type Stack interface {
	Push(one Task)
	Pop() Task
	Empty() bool
	Destroy()
}

// Task is an interface defined for all type of optimizing work: exploring, implementing,
// deriving-stats, join-reordering and so on.
type Task interface {
	// Execute task self executing logic
	Execute() error
	// Desc task self description string.
	Desc(w util.StrBufferWriter)
}
