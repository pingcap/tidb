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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"runtime"
	"sync/atomic"
)

const (
	// pauseStateRunning indicates the pauser is running (not paused)
	pauseStateRunning uint32 = iota
	// pauseStatePaused indicates the pauser is paused
	pauseStatePaused
	// pauseStateLocked indicates the pauser is being held for exclusive access
	// of its waiters field, and no other goroutines should be able to
	// read/write the map of waiters when the state is Locked (all other
	// goroutines trying to access waiters should cooperatively enter a spin
	// loop).
	pauseStateLocked
)

// The implementation is based on https://github.com/golang/sync/blob/master/semaphore/semaphore.go

// Pauser is a type which could allow multiple goroutines to wait on demand,
// similar to a gate or traffic light.
type Pauser struct {
	// state has two purposes: (1) records whether we are paused, and (2) acts
	// as a spin-lock for the `waiters` map.
	state   uint32
	waiters map[chan<- struct{}]struct{}
}

// NewPauser returns an initialized pauser.
func NewPauser() *Pauser {
	return &Pauser{
		state:   pauseStateRunning,
		waiters: make(map[chan<- struct{}]struct{}, 32),
	}
}

// Pause causes all calls to Wait() to block.
func (p *Pauser) Pause() {
	// If the state was Paused, we do nothing.
	// If the state was Locked, we loop again until the state becomes not Locked.
	// If the state was Running, we atomically move into Paused state.

	for {
		oldState := atomic.LoadUint32(&p.state)
		if oldState == pauseStatePaused || atomic.CompareAndSwapUint32(&p.state, pauseStateRunning, pauseStatePaused) {
			return
		}
		runtime.Gosched()
	}
}

// Resume causes all calls to Wait() to continue.
func (p *Pauser) Resume() {
	// If the state was Running, we do nothing.
	// If the state was Locked, we loop again until the state becomes not Locked.
	// If the state was Paused, we Lock the pauser, clear the waiter map,
	// then move into Running state.

	for {
		oldState := atomic.LoadUint32(&p.state)
		if oldState == pauseStateRunning {
			return
		}
		if atomic.CompareAndSwapUint32(&p.state, pauseStatePaused, pauseStateLocked) {
			break
		}
		runtime.Gosched()
	}

	// extract all waiters, then notify them we changed from "Paused" to "Not Paused".
	allWaiters := p.waiters
	p.waiters = make(map[chan<- struct{}]struct{}, len(allWaiters))

	atomic.StoreUint32(&p.state, pauseStateRunning)

	for waiter := range allWaiters {
		close(waiter)
	}
}

// IsPaused gets whether the current state is paused or not.
func (p *Pauser) IsPaused() bool {
	return atomic.LoadUint32(&p.state) != pauseStateRunning
}

// Wait blocks the current goroutine if the current state is paused, until the
// pauser itself is resumed at least once.
//
// If `ctx` is done, this method will also unblock immediately, and return the
// context error.
func (p *Pauser) Wait(ctx context.Context) error {
	// If the state is Running, we return immediately (this path is hot and must
	// be taken as soon as possible)
	// If the state is Locked, we loop again until the state becomes not Locked.
	// If the state is Paused, we Lock the pauser, add a waiter to the map, then
	// revert to the original (Paused) state.

	for {
		oldState := atomic.LoadUint32(&p.state)
		if oldState == pauseStateRunning {
			return nil
		}
		if atomic.CompareAndSwapUint32(&p.state, pauseStatePaused, pauseStateLocked) {
			break
		}
		runtime.Gosched()
	}

	waiter := make(chan struct{})
	p.waiters[waiter] = struct{}{}

	atomic.StoreUint32(&p.state, pauseStatePaused)

	select {
	case <-ctx.Done():
		err := ctx.Err()
		p.cancel(waiter)
		return err
	case <-waiter:
		return nil
	}
}

// cancel removes a waiter from the waiters map
func (p *Pauser) cancel(waiter chan<- struct{}) {
	// If the state is Locked, we loop again until the state becomes not Locked.
	// Otherwise, we Lock the pauser, remove the waiter from the map, then
	// revert to the original state.

	for {
		oldState := atomic.LoadUint32(&p.state)
		if oldState != pauseStateLocked && atomic.CompareAndSwapUint32(&p.state, oldState, pauseStateLocked) {
			delete(p.waiters, waiter)
			atomic.StoreUint32(&p.state, oldState)
			return
		}
		runtime.Gosched()
	}
}
