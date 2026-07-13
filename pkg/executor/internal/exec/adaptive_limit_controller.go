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

package exec

import (
	"context"
	"sync"
)

// AdaptiveLimitSnapshot is a point-in-time view of an AdaptiveLimitController.
type AdaptiveLimitSnapshot struct {
	DemandRows      uint64
	OutputRows      uint64
	OuterFetched    uint64
	OuterConsumed   uint64
	OuterReserved   uint64
	LookupReserved  uint64
	LookupDiscarded uint64
	DesiredWindow   uint64
	Stopped         bool
}

// AdaptiveLimitController bounds speculative work for an early-stop LIMIT.
// It is owned by one executor tree and learns only from the current execution.
type AdaptiveLimitController struct {
	mu sync.Mutex

	demandRows      uint64
	outputRows      uint64
	outerFetched    uint64
	outerConsumed   uint64
	outerReserved   uint64
	lookupReserved  uint64
	lookupDiscarded uint64

	initialWindow       uint64
	maxWindow           uint64
	desiredWindow       uint64
	nextZeroYieldGrowth uint64
	stopped             bool
	outerChanged        chan struct{}
	lookupChanged       chan struct{}
	stopCh              chan struct{}
}

// NewAdaptiveLimitController creates a statement-local admission controller.
func NewAdaptiveLimitController(demandRows, initialWindow, maxWindow uint64) *AdaptiveLimitController {
	if initialWindow == 0 {
		initialWindow = 1
	}
	if maxWindow < initialWindow {
		maxWindow = initialWindow
	}
	if demandRows > 0 && initialWindow > demandRows {
		initialWindow = demandRows
	}
	c := &AdaptiveLimitController{
		demandRows:          demandRows,
		initialWindow:       initialWindow,
		maxWindow:           maxWindow,
		desiredWindow:       initialWindow,
		nextZeroYieldGrowth: initialWindow,
		outerChanged:        make(chan struct{}, 1),
		lookupChanged:       make(chan struct{}, 1),
		stopCh:              make(chan struct{}),
	}
	if demandRows == 0 {
		c.stopLocked()
	}
	return c
}

// Reset prepares the controller for another Open/Next/Close lifecycle.
// Callers must ensure all producers from the previous lifecycle have exited.
func (c *AdaptiveLimitController) Reset() {
	c.mu.Lock()
	if !c.stopped && c.outputRows == 0 && c.outerFetched == 0 && c.outerConsumed == 0 &&
		c.outerReserved == 0 && c.lookupReserved == 0 && c.lookupDiscarded == 0 &&
		c.desiredWindow == c.initialWindow {
		c.mu.Unlock()
		return
	}
	c.outputRows = 0
	c.outerFetched = 0
	c.outerConsumed = 0
	c.outerReserved = 0
	c.lookupReserved = 0
	c.lookupDiscarded = 0
	c.desiredWindow = c.initialWindow
	c.nextZeroYieldGrowth = c.initialWindow
	c.stopped = false
	c.outerChanged = make(chan struct{}, 1)
	c.lookupChanged = make(chan struct{}, 1)
	c.stopCh = make(chan struct{})
	if c.demandRows == 0 {
		c.stopLocked()
	}
	c.mu.Unlock()
}

// ReserveOuter waits until up to maxRows can be admitted to the join outer
// pipeline. The bool is false after LIMIT completion or context cancellation.
func (c *AdaptiveLimitController) ReserveOuter(ctx context.Context, maxRows int) (int, bool) {
	return c.reserve(ctx, maxRows, true)
}

// ReserveLookup bounds handles admitted to the double-read table lookup stage.
func (c *AdaptiveLimitController) ReserveLookup(ctx context.Context, maxRows int) (int, bool) {
	return c.reserve(ctx, maxRows, false)
}

func (c *AdaptiveLimitController) reserve(ctx context.Context, maxRows int, outer bool) (int, bool) {
	if maxRows <= 0 {
		return 0, true
	}
	for {
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			return 0, false
		}
		var outstanding uint64
		if outer {
			outstanding = c.outerFetched - min(c.outerFetched, c.outerConsumed) + c.outerReserved
		} else {
			outstanding = c.lookupReserved
		}
		if outstanding < c.desiredWindow {
			rows := min(uint64(maxRows), c.desiredWindow-outstanding)
			if outer {
				c.outerReserved += rows
			} else {
				c.lookupReserved += rows
			}
			c.mu.Unlock()
			return int(rows), true
		}
		changed := c.lookupChanged
		if outer {
			changed = c.outerChanged
		}
		stopCh := c.stopCh
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return 0, false
		case <-stopCh:
			return 0, false
		case <-changed:
		}
	}
}

// CommitOuter converts an outer reservation into rows actually fetched.
func (c *AdaptiveLimitController) CommitOuter(reserved, fetched int) {
	if reserved < 0 || fetched < 0 {
		return
	}
	c.mu.Lock()
	released := min(uint64(reserved), c.outerReserved)
	c.outerReserved -= released
	c.outerFetched += uint64(fetched)
	c.recomputeWindowLocked()
	c.notifyAllLocked()
	c.mu.Unlock()
}

// ObserveJoinProgress updates input consumption and output production under one
// lock so a partially reported join batch is not mistaken for zero yield.
func (c *AdaptiveLimitController) ObserveJoinProgress(consumedRows, outputRows int) {
	if consumedRows <= 0 && outputRows <= 0 {
		return
	}
	c.mu.Lock()
	if consumedRows > 0 {
		c.outerConsumed = min(c.outerFetched, c.outerConsumed+uint64(consumedRows))
	}
	if outputRows > 0 {
		c.outputRows += uint64(outputRows)
	}
	c.recomputeWindowLocked()
	c.notifyAllLocked()
	c.mu.Unlock()
}

// ReleaseLookup releases handles after their table lookup result is consumed.
func (c *AdaptiveLimitController) ReleaseLookup(rows int) {
	if rows <= 0 {
		return
	}
	c.mu.Lock()
	c.lookupReserved -= min(uint64(rows), c.lookupReserved)
	c.notifyLocked(c.lookupChanged)
	c.mu.Unlock()
}

// ScanConcurrencyLimit returns a dynamic ceiling for future index scan ranges,
// or zero after the LIMIT has stopped.
func (c *AdaptiveLimitController) ScanConcurrencyLimit(ceiling, batchRows int) int {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return 0
	}
	window := c.desiredWindow
	c.mu.Unlock()
	if ceiling < 1 {
		return 1
	}
	if batchRows < 1 {
		batchRows = 1
	}
	limit := int((window + uint64(batchRows) - 1) / uint64(batchRows))
	return min(max(limit, 1), ceiling)
}

// SuggestedBatchSize returns the current row window bounded by the caller's
// configured batch ceiling.
func (c *AdaptiveLimitController) SuggestedBatchSize(ceiling int) int {
	if ceiling < 1 {
		return 1
	}
	c.mu.Lock()
	window := c.desiredWindow
	c.mu.Unlock()
	return min(max(int(min(window, uint64(ceiling))), 1), ceiling)
}

// Stop prevents future admission and wakes all blocked producers.
func (c *AdaptiveLimitController) Stop() {
	c.mu.Lock()
	if !c.stopped {
		c.stopLocked()
	}
	c.mu.Unlock()
}

// Snapshot returns the current controller counters for tests and diagnostics.
func (c *AdaptiveLimitController) Snapshot() AdaptiveLimitSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return AdaptiveLimitSnapshot{
		DemandRows:      c.demandRows,
		OutputRows:      c.outputRows,
		OuterFetched:    c.outerFetched,
		OuterConsumed:   c.outerConsumed,
		OuterReserved:   c.outerReserved,
		LookupReserved:  c.lookupReserved,
		LookupDiscarded: c.lookupDiscarded,
		DesiredWindow:   c.desiredWindow,
		Stopped:         c.stopped,
	}
}

func (c *AdaptiveLimitController) recomputeWindowLocked() {
	if c.stopped {
		return
	}
	if c.outputRows >= c.demandRows {
		c.stopLocked()
		return
	}
	if c.outputRows == 0 {
		outstanding := c.outerFetched - min(c.outerFetched, c.outerConsumed) + c.outerReserved
		if outstanding == 0 && c.outerConsumed >= c.nextZeroYieldGrowth {
			nextWindow := c.desiredWindow * 2
			if nextWindow < c.desiredWindow {
				nextWindow = c.maxWindow
			}
			c.desiredWindow = min(nextWindow, c.maxWindow)
			c.nextZeroYieldGrowth = c.outerConsumed + c.desiredWindow
		}
		return
	}

	remainingOutput := c.demandRows - c.outputRows
	estimatedInput := divideAndRoundUp(saturatingMultiply(remainingOutput, c.outerConsumed), c.outputRows)
	// Retain more producer/consumer overlap early, then taper the speculative
	// headroom as the LIMIT approaches completion. The remaining-input estimate
	// itself can still run concurrently, so the tail does not become row-at-a-time.
	var target uint64
	switch {
	case remainingOutput <= c.demandRows/4:
		target = estimatedInput
	case remainingOutput <= c.demandRows/2:
		target = divideAndRoundUp(saturatingMultiply(estimatedInput, 9), 8)
	default:
		target = divideAndRoundUp(saturatingMultiply(estimatedInput, 5), 4)
	}
	c.desiredWindow = min(max(target, uint64(1)), c.maxWindow)
}

func (c *AdaptiveLimitController) stopLocked() {
	if c.stopped {
		return
	}
	c.stopped = true
	c.lookupDiscarded += c.lookupReserved
	c.lookupReserved = 0
	c.outerReserved = 0
	c.desiredWindow = 0
	close(c.stopCh)
}

func (c *AdaptiveLimitController) notifyAllLocked() {
	c.notifyLocked(c.outerChanged)
	c.notifyLocked(c.lookupChanged)
}

func (*AdaptiveLimitController) notifyLocked(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func divideAndRoundUp(value, divisor uint64) uint64 {
	if divisor == 0 {
		return 0
	}
	return value/divisor + boolToUint64(value%divisor != 0)
}

func boolToUint64(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func saturatingMultiply(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}
