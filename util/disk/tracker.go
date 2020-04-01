// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package disk

import (
	"fmt"
	"sync"

	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var globalStorageLabel fmt.Stringer = stringutil.StringerStr("GlobalStorageLabel")

// Tracker is used to track the disk usage during query execution.
type Tracker = memory.Tracker

// NewTracker creates a disk tracker.
//	1. "label" is the label used in the usage string.
//	2. "bytesLimit <= 0" means no limit.
var NewTracker = memory.NewTracker

// NewGlobalDisTracker create GlobalDisTracker
func NewGlobalDisTracker(bytesLimit int64) *Tracker {
	return NewTracker(globalStorageLabel, bytesLimit)
}

// GlobalPanicOnExceed panics when GlobalDisTracker storage usage exceeds storage quota.
type GlobalPanicOnExceed struct {
	mutex sync.Mutex // For synchronization.
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *GlobalPanicOnExceed) SetLogHook(hook func(uint64)) {}

// Action panics when storage usage exceeds storage quota.
func (a *GlobalPanicOnExceed) Action(t *Tracker) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	panic(GlobalPanicStorageExceed)
}

// SetFallback sets a fallback action.
func (a *GlobalPanicOnExceed) SetFallback(memory.ActionOnExceed) {}

const (
	// GlobalPanicStorageExceed represents the panic message when out of storage quota.
	GlobalPanicStorageExceed string = "Out Of Global Storage Quota!"
)
