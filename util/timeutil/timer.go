// Copyright 2021 PingCAP, Inc.
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

package timeutil

import (
	"time"
)

// WrappedTimer wraps the standard time.Timer to handle clean stop and reset.
// As Russ Cox suggested, the correct way to use time.Timer is:
// 1. All the Timer operations (Timer.Stop, Timer.Reset and receiving from or draining the channel) should be done in the same goroutine.
// 2. The program should manage an extra status showing whether it has received from the Timer's channel or not.
// https://github.com/golang/go/issues/11513#issuecomment-157062583
// https://groups.google.com/g/golang-dev/c/c9UUfASVPoU/m/tlbK2BpFEwAJ
// **NOTE**: All the functions of WrappedTimer *should* be used in the same goroutine.
type WrappedTimer struct {
	t    *time.Timer // The actual timer
	read bool        // Whether t.C has already been read from
}

// NewWrappedTimer creates an instance of WrappedTimer.
func NewWrappedTimer(d time.Duration) *WrappedTimer {
	return &WrappedTimer{t: time.NewTimer(d)}
}

// ReadC waits until it can read from the wrapped timer's channel C.
// It returns the time value received from the channel C, a zero time value if the channel C has already been read from.
func (gt *WrappedTimer) ReadC() time.Time {
	if gt.read {
		return time.Time{}
	}
	tv := <-gt.t.C
	gt.read = true
	return tv
}

// C returns the chan of wrapped timer for select.
func (gt *WrappedTimer) C() <-chan time.Time {
	return gt.t.C
}

// SetRead set the read flag, you must call it if timer chan is read, otherwise stop/reset will hang.
func (gt *WrappedTimer) SetRead() {
	gt.read = true
}

// Reset changes the timer to expire after duration d.
func (gt *WrappedTimer) Reset(d time.Duration) {
	gt.Stop()
	gt.t.Reset(d)
	gt.read = false
}

// Stop prevents the Timer from firing.
// It returns true if the call stops the timer, false if the timer has already expired or been stopped.
func (gt *WrappedTimer) Stop() bool {
	stopped := gt.t.Stop()
	if !stopped && !gt.read {
		// Drain the gt.t.C if it has not been read from already
		<-gt.t.C
	}
	return stopped
}
