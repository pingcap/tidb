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

const adaptiveYieldWindowSize = 4

type adaptiveYieldWindow struct {
	inputs  [adaptiveYieldWindowSize]uint64
	outputs [adaptiveYieldWindowSize]uint64
	next    uint8
}

func (w *adaptiveYieldWindow) add(input, output uint64) {
	w.inputs[w.next] = input
	w.outputs[w.next] = output
	w.next = (w.next + 1) % adaptiveYieldWindowSize
}

func (w *adaptiveYieldWindow) totals() (input, output uint64) {
	for i := range adaptiveYieldWindowSize {
		input = saturatingAdd(input, w.inputs[i])
		output = saturatingAdd(output, w.outputs[i])
	}
	return input, output
}

// AdaptiveLimitSnapshot is a point-in-time view of an AdaptiveLimitController.
type AdaptiveLimitSnapshot struct {
	DemandRows              uint64
	OutputRows              uint64
	OuterFetched            uint64
	OuterConsumed           uint64
	OuterReserved           uint64
	OuterWindow             uint64
	OuterOutstandingAtStop  uint64
	LookupReserved          uint64
	LookupHandles           uint64
	LookupRows              uint64
	LookupWindow            uint64
	LookupOutstandingAtStop uint64
	Stopped                 bool
}

// AdaptiveLimitController bounds speculative work for an early-stop LIMIT.
// It is owned by one executor tree and learns only from the current execution.
type AdaptiveLimitController struct {
	mu sync.Mutex

	demandRows             uint64
	outputRows             uint64
	outerFetched           uint64
	outerConsumed          uint64
	outerReserved          uint64
	outerOutstandingAtStop uint64
	pendingOuterOutput     uint64
	recentOuterYield       adaptiveYieldWindow

	lookupReserved          uint64
	lookupHandles           uint64
	lookupRows              uint64
	recentLookupYield       adaptiveYieldWindow
	lookupOutstandingAtStop uint64

	initialOuterWindow uint64
	maxOuterWindow     uint64
	outerWindow        uint64
	outerGrowthBarrier uint64
	outerNoOutputRows  uint64

	initialLookupWindow   uint64
	maxLookupWindow       uint64
	lookupWindow          uint64
	lookupGrowthProgress  uint64
	lookupNoOutputRows    uint64
	lookupInNoOutputPhase bool

	stopped       bool
	outerChanged  chan struct{}
	lookupChanged chan struct{}
	stopCh        chan struct{}
}

// NewAdaptiveLimitController creates a statement-local admission controller.
func NewAdaptiveLimitController(
	demandRows, initialOuterWindow, maxOuterWindow, initialLookupWindow, maxLookupWindow uint64,
) *AdaptiveLimitController {
	initialOuterWindow, maxOuterWindow = normalizeAdaptiveWindow(initialOuterWindow, maxOuterWindow)
	initialLookupWindow, maxLookupWindow = normalizeAdaptiveWindow(initialLookupWindow, maxLookupWindow)
	if demandRows > 0 && initialOuterWindow > demandRows {
		initialOuterWindow = demandRows
	}
	if demandRows > 0 && initialLookupWindow > demandRows {
		initialLookupWindow = demandRows
	}
	c := &AdaptiveLimitController{
		demandRows:          demandRows,
		initialOuterWindow:  initialOuterWindow,
		maxOuterWindow:      maxOuterWindow,
		outerWindow:         initialOuterWindow,
		initialLookupWindow: initialLookupWindow,
		maxLookupWindow:     maxLookupWindow,
		lookupWindow:        initialLookupWindow,
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
		c.outerReserved == 0 && c.lookupReserved == 0 && c.lookupHandles == 0 && c.lookupRows == 0 &&
		c.outerOutstandingAtStop == 0 && c.lookupOutstandingAtStop == 0 && c.outerWindow == c.initialOuterWindow &&
		c.lookupWindow == c.initialLookupWindow {
		c.mu.Unlock()
		return
	}
	c.outputRows = 0
	c.outerFetched = 0
	c.outerConsumed = 0
	c.outerReserved = 0
	c.outerOutstandingAtStop = 0
	c.pendingOuterOutput = 0
	c.recentOuterYield = adaptiveYieldWindow{}
	c.outerNoOutputRows = 0
	c.outerWindow = c.initialOuterWindow
	c.outerGrowthBarrier = 0
	c.lookupReserved = 0
	c.lookupHandles = 0
	c.lookupRows = 0
	c.recentLookupYield = adaptiveYieldWindow{}
	c.lookupNoOutputRows = 0
	c.lookupInNoOutputPhase = false
	c.lookupWindow = c.initialLookupWindow
	c.lookupGrowthProgress = 0
	c.lookupOutstandingAtStop = 0
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
// pipeline. The bool is false after LIMIT completion; context cancellation is
// returned as an error.
func (c *AdaptiveLimitController) ReserveOuter(ctx context.Context, maxRows int) (int, bool, error) {
	return c.reserve(ctx, maxRows, true)
}

// ReserveLookup bounds handles admitted to the double-read table lookup stage.
// The bool is false after LIMIT completion; context cancellation is returned as
// an error.
func (c *AdaptiveLimitController) ReserveLookup(ctx context.Context, maxRows int) (int, bool, error) {
	return c.reserve(ctx, maxRows, false)
}

func (c *AdaptiveLimitController) reserve(ctx context.Context, maxRows int, outer bool) (int, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if maxRows <= 0 {
		return 0, true, nil
	}
	for {
		c.mu.Lock()
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return 0, false, err
		}
		if c.stopped {
			c.mu.Unlock()
			return 0, false, nil
		}
		window := c.lookupWindow
		outstanding := c.lookupReserved
		changed := c.lookupChanged
		if outer {
			window = c.outerWindow
			outstanding = c.outerFetched - min(c.outerFetched, c.outerConsumed) + c.outerReserved
			changed = c.outerChanged
		}
		if outstanding < window {
			rows := min(uint64(maxRows), window-outstanding)
			if outer {
				c.outerReserved += rows
			} else {
				c.lookupReserved += rows
			}
			c.mu.Unlock()
			return int(rows), true, nil
		}
		stopCh := c.stopCh
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		case <-stopCh:
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
			return 0, false, nil
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
	c.outerFetched += min(uint64(fetched), released)
	c.notifyLocked(c.outerChanged)
	c.mu.Unlock()
}

// ObserveJoinProgress updates input consumption and output production under one
// lock. Output produced before an outer row is fully consumed is retained until
// it can be paired with completed outer-row input.
func (c *AdaptiveLimitController) ObserveJoinProgress(consumedRows, outputRows int) {
	if consumedRows <= 0 && outputRows <= 0 {
		return
	}
	c.mu.Lock()
	previousConsumed := c.outerConsumed
	if consumedRows > 0 {
		c.outerConsumed = min(c.outerFetched, c.outerConsumed+uint64(consumedRows))
	}
	consumed := c.outerConsumed - previousConsumed
	if outputRows > 0 {
		c.outputRows = saturatingAdd(c.outputRows, uint64(outputRows))
	}
	if c.outputRows >= c.demandRows {
		c.stopLocked()
		c.mu.Unlock()
		return
	}
	if consumed == 0 {
		if outputRows > 0 {
			c.pendingOuterOutput = saturatingAdd(c.pendingOuterOutput, uint64(outputRows))
		}
		c.mu.Unlock()
		return
	}
	pairedOutput := c.pendingOuterOutput
	if outputRows > 0 {
		pairedOutput = saturatingAdd(pairedOutput, uint64(outputRows))
	}
	c.pendingOuterOutput = 0
	if pairedOutput > 0 {
		c.recentOuterYield.add(consumed, pairedOutput)
		c.outerNoOutputRows = 0
		c.recomputeOuterWindowLocked()
		c.recomputeLookupWindowLocked()
	} else {
		if c.outerNoOutputRows == 0 {
			c.recentOuterYield = adaptiveYieldWindow{}
		}
		c.outerNoOutputRows = saturatingAdd(c.outerNoOutputRows, consumed)
		c.growOuterWindowIfDrainedLocked()
	}
	c.notifyAllLocked()
	c.mu.Unlock()
}

// CompleteLookup releases a fully consumed lookup task and learns its
// handle-to-row yield. reserved is admission accounting; handles is the task's
// actual input. Callers must report tasks in result-consumption order.
func (c *AdaptiveLimitController) CompleteLookup(reserved, handles, rows int) {
	if reserved <= 0 || handles < 0 || rows < 0 {
		return
	}
	c.mu.Lock()
	if c.stopped || uint64(reserved) > c.lookupReserved {
		c.mu.Unlock()
		return
	}
	c.lookupReserved -= uint64(reserved)
	c.lookupHandles = saturatingAdd(c.lookupHandles, uint64(handles))
	c.lookupRows = saturatingAdd(c.lookupRows, uint64(rows))
	if rows > 0 {
		c.recentLookupYield.add(uint64(handles), uint64(rows))
		c.lookupNoOutputRows = 0
		c.lookupInNoOutputPhase = false
		c.recomputeLookupWindowLocked()
	} else {
		if !c.lookupInNoOutputPhase {
			c.recentLookupYield = adaptiveYieldWindow{}
		}
		c.lookupInNoOutputPhase = true
		c.lookupNoOutputRows = saturatingAdd(c.lookupNoOutputRows, uint64(max(handles, 1)))
		c.growLookupWindowIfDrainedLocked()
	}
	c.notifyLocked(c.lookupChanged)
	c.mu.Unlock()
}

// AbortLookup releases lookup admission without using the task as a yield
// sample. It is used for extraction errors, cancellation, and undispatched work.
func (c *AdaptiveLimitController) AbortLookup(handles int) {
	if handles <= 0 {
		return
	}
	c.mu.Lock()
	c.lookupReserved -= min(uint64(handles), c.lookupReserved)
	c.notifyLocked(c.lookupChanged)
	c.mu.Unlock()
}

// SuggestedScanConcurrency returns the scan concurrency justified by the
// current lookup window. The caller's configured value remains the hard
// ceiling. The initial lookup window represents one unit of scan concurrency;
// unlike the task batch size, it stays stable as the controller adapts.
func (c *AdaptiveLimitController) SuggestedScanConcurrency(ceiling int) int {
	if ceiling < 1 {
		return 1
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return 0
	}
	window := c.lookupWindow
	initialWindow := c.initialLookupWindow
	c.mu.Unlock()
	concurrency := divideAndRoundUp(window, initialWindow)
	return min(max(int(min(concurrency, uint64(ceiling))), 1), ceiling)
}

// SuggestedBatchSize returns the lookup-handle window bounded by the caller's
// configured batch ceiling.
func (c *AdaptiveLimitController) SuggestedBatchSize(ceiling int) int {
	if ceiling < 1 {
		return 1
	}
	c.mu.Lock()
	window := c.lookupWindow
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
		DemandRows:              c.demandRows,
		OutputRows:              c.outputRows,
		OuterFetched:            c.outerFetched,
		OuterConsumed:           c.outerConsumed,
		OuterReserved:           c.outerReserved,
		OuterWindow:             c.outerWindow,
		OuterOutstandingAtStop:  c.outerOutstandingAtStop,
		LookupReserved:          c.lookupReserved,
		LookupHandles:           c.lookupHandles,
		LookupRows:              c.lookupRows,
		LookupWindow:            c.lookupWindow,
		LookupOutstandingAtStop: c.lookupOutstandingAtStop,
		Stopped:                 c.stopped,
	}
}

func (c *AdaptiveLimitController) recomputeOuterWindowLocked() {
	remainingOutput := c.demandRows - c.outputRows
	estimatedInput := divideAndRoundUp(saturatingMultiply(remainingOutput, c.outerConsumed), c.outputRows)
	recentConsumed, recentOutput := c.recentOuterYield.totals()
	if recentOutput > 0 {
		recentEstimate := divideAndRoundUp(
			saturatingMultiply(remainingOutput, recentConsumed),
			recentOutput,
		)
		estimatedInput = max(estimatedInput, recentEstimate)
	}
	var target uint64
	switch {
	case remainingOutput <= c.demandRows/4:
		target = estimatedInput
	case remainingOutput <= c.demandRows/2:
		target = divideAndRoundUp(saturatingMultiply(estimatedInput, 9), 8)
	default:
		target = divideAndRoundUp(saturatingMultiply(estimatedInput, 5), 4)
	}
	var grew bool
	c.outerWindow, grew = adjustAdaptiveWindow(
		target, c.outerWindow, 1, c.maxOuterWindow, c.outerConsumed > c.outerGrowthBarrier,
	)
	if grew {
		c.outerGrowthBarrier = c.outerFetched
	}
}

func (c *AdaptiveLimitController) recomputeLookupWindowLocked() {
	if c.lookupRows == 0 || c.lookupInNoOutputPhase {
		return
	}
	lookupBuffered := c.lookupRows - min(c.lookupRows, c.outerConsumed)
	outerBuffered := c.outerFetched - min(c.outerFetched, c.outerConsumed)
	bufferedRows := max(lookupBuffered, outerBuffered)
	remainingOuter := c.outerWindow - min(c.outerWindow, bufferedRows)
	target := divideAndRoundUp(saturatingMultiply(remainingOuter, c.lookupHandles), c.lookupRows)
	recentHandles, recentRows := c.recentLookupYield.totals()
	if recentRows > 0 {
		recentTarget := divideAndRoundUp(
			saturatingMultiply(remainingOuter, recentHandles),
			recentRows,
		)
		target = max(target, recentTarget)
	}
	// A lookup task below the statement's initial window turns periodic low
	// selectivity into row-at-a-time table RPCs. The initial window is derived
	// from this LIMIT and the configured batch ceiling, not a fixed row threshold.
	var grew bool
	c.lookupWindow, grew = adjustAdaptiveWindow(
		target, c.lookupWindow, c.initialLookupWindow, c.maxLookupWindow,
		c.lookupHandles > c.lookupGrowthProgress,
	)
	if grew {
		c.lookupGrowthProgress = c.lookupHandles
	}
}

func (c *AdaptiveLimitController) growOuterWindowIfDrainedLocked() {
	outstanding := c.outerFetched - min(c.outerFetched, c.outerConsumed) + c.outerReserved
	if outstanding != 0 || c.outerNoOutputRows < c.outerWindow {
		return
	}
	nextWindow := growAdaptiveWindow(c.outerWindow, c.maxOuterWindow)
	if nextWindow > c.outerWindow {
		c.outerGrowthBarrier = c.outerFetched
	}
	c.outerWindow = nextWindow
	c.outerNoOutputRows = 0
	if !c.lookupInNoOutputPhase {
		c.recomputeLookupWindowLocked()
	}
}

func (c *AdaptiveLimitController) growLookupWindowIfDrainedLocked() {
	if c.lookupReserved != 0 || c.lookupNoOutputRows < c.lookupWindow {
		return
	}
	nextWindow := growAdaptiveWindow(c.lookupWindow, c.maxLookupWindow)
	if nextWindow > c.lookupWindow {
		c.lookupGrowthProgress = c.lookupHandles
	}
	c.lookupWindow = nextWindow
	c.lookupNoOutputRows = 0
}

func (c *AdaptiveLimitController) stopLocked() {
	if c.stopped {
		return
	}
	c.stopped = true
	c.outerOutstandingAtStop = saturatingAdd(
		c.outerFetched-min(c.outerFetched, c.outerConsumed),
		c.outerReserved,
	)
	c.lookupOutstandingAtStop = c.lookupReserved
	c.lookupReserved = 0
	c.outerReserved = 0
	c.outerWindow = 0
	c.lookupWindow = 0
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

func normalizeAdaptiveWindow(initial, maximum uint64) (uint64, uint64) {
	if initial == 0 {
		initial = 1
	}
	if maximum < initial {
		maximum = initial
	}
	return initial, maximum
}

func growAdaptiveWindow(window, maximum uint64) uint64 {
	next := saturatingMultiply(window, 2)
	return min(next, maximum)
}

func adjustAdaptiveWindow(target, current, minimum, maximum uint64, canGrow bool) (uint64, bool) {
	target = min(max(target, minimum), maximum)
	if target > current {
		if !canGrow {
			return current, false
		}
		return min(target, growAdaptiveWindow(current, maximum)), true
	}
	return target, false
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

func saturatingAdd(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}

func saturatingMultiply(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}
