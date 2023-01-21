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

package gscheduler

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util/grunning"
)

// HandleKey is a key when to add rbTreeQueue
type HandleKey struct {
	differenceWithAllottedAtLastCheck time.Duration
	id                                uint64
}

func newHandleKey(id uint64, differenceWithAllottedAtLastCheck time.Duration) *HandleKey {
	return &HandleKey{differenceWithAllottedAtLastCheck, id}
}

func handleKeyCompare(a, b HandleKey) int {
	switch {
	case a.differenceWithAllottedAtLastCheck > b.differenceWithAllottedAtLastCheck:
		return 1
	case a.differenceWithAllottedAtLastCheck < b.differenceWithAllottedAtLastCheck:
		return -1
	default:
		switch {
		case a.id > b.id:
			return 1
		case a.id < b.id:
			return 1
		default:
		}
	}
	return 0
}

// Handle is for manage the time slice for the goroutine
type Handle struct {
	cpuStart                                                  time.Duration
	allotted                                                  time.Duration
	enableSchedule                                            bool
	priority                                                  int
	runningTimeAtLastCheck, differenceWithAllottedAtLastCheck time.Duration
	sched                                                     chan struct{}
	isStop                                                    atomic.Bool
}

// NewHandle is to create new handle
func NewHandle(allotted time.Duration, schedule bool) *Handle {
	h := &Handle{
		allotted:       allotted,
		enableSchedule: schedule,
		sched:          make(chan struct{}),
	}
	h.cpuStart = h.runningTime()
	return h
}

func (h *Handle) runningTime() time.Duration {
	return grunning.Difference(grunning.Time(), h.cpuStart)
}

// Checkpoint is to check the time slice
func (h *Handle) Checkpoint() {
	if h.enableSchedule {
		<-h.sched
	}
}

// OverLimit is to check whether the time slice is over
func (h *Handle) OverLimit() (overLimit bool, difference time.Duration) {
	return h.overLimitInner()
}

func (h *Handle) overLimitInner() (overLimit bool, difference time.Duration) {
	runningTime := h.runningTime()
	if runningTime >= h.allotted {
		return true, grunning.Difference(runningTime, h.allotted)
	}
	h.runningTimeAtLastCheck, h.differenceWithAllottedAtLastCheck = runningTime, grunning.Difference(runningTime, h.allotted)
	return false, h.differenceWithAllottedAtLastCheck
}

// Stop is to close the handle
func (h *Handle) Stop() {
	h.isStop.Store(true)
}

// IsStop is to check whether the handle is stopped
func (h *Handle) IsStop() bool {
	return h.isStop.Load()
}
