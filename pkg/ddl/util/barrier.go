// Copyright 2025 PingCAP, Inc.
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

package util

import (
	"context"
	"sync"
	"sync/atomic"
)

// Barrier use for coordinating writers and ingestion with a pause / drain protocol.
type Barrier struct {
	cond     *sync.Cond
	mu       sync.Mutex
	inFlight atomic.Int32
	paused   bool
}

// NewBarrier creates a new Barrier.
func NewBarrier() *Barrier {
	b := &Barrier{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// CurrentInFlight returns the number of active writers.
func (b *Barrier) CurrentInFlight() int32 {
	return b.inFlight.Load()
}

// IsPaused reports whether the barrier is currently paused.
func (b *Barrier) IsPaused() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.paused
}

// Enter blocks only if paused, otherwise increments in-flight writers.
func (b *Barrier) Enter() {
	b.mu.Lock()
	for b.paused {
		b.cond.Wait()
	}
	b.inFlight.Add(1)
	b.mu.Unlock()
}

// Exit decrements in-flight writers and signals waiter if drain is complete.
func (b *Barrier) Exit() {
	remaining := b.inFlight.Add(-1)
	b.mu.Lock()
	if b.paused && remaining == 0 {
		b.cond.Broadcast()
	}
	b.mu.Unlock()
}

// PauseAndWait pauses and waits until all in-flight writers exit.
func (b *Barrier) PauseAndWait(ctx context.Context) error {
	b.mu.Lock()
	if !b.paused {
		b.paused = true
	}
	for b.inFlight.Load() != 0 && ctx.Err() == nil {
		b.cond.Wait()
	}
	err := ctx.Err()
	b.mu.Unlock()
	return err
}

// Resume unpauses and releases blocked Enter() callers.
func (b *Barrier) Resume() {
	b.mu.Lock()
	if b.paused {
		b.paused = false
		b.cond.Broadcast()
	}
	b.mu.Unlock()
}
