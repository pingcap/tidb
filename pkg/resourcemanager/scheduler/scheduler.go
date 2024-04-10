// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
)

// Command is the command for scheduler
type Command int

const (
	// Downclock is to reduce the number of concurrency.
	Downclock Command = iota
	// Hold is to hold the number of concurrency.
	Hold
	// Overclock is to increase the number of concurrency.
	Overclock
)

// Scheduler is a scheduler interface
type Scheduler interface {
	Tune(component util.Component, p util.GoroutinePool) Command
}
