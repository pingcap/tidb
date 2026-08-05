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
	"time"
)

const adaptiveYieldWindowSize = 4

type adaptiveLimitMode uint8

const (
	adaptiveLimitIndexJoin adaptiveLimitMode = iota
	adaptiveLimitDirectIndexLookup
)

// adaptiveYieldWindow keeps a small recent sample alongside the controller's
// cumulative counters. The recent sample lets an ordered scan react when the
// selectivity near its current position differs from earlier input.
type adaptiveYieldWindow struct {
	inputs  [adaptiveYieldWindowSize]uint64
	outputs [adaptiveYieldWindowSize]uint64
	next    uint8
}

type admissionBlockStats struct {
	blockedSince time.Time
	waiters      int
	blockedTime  time.Duration
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
// Outer fields are measured in outer rows. Lookup reservation, handle, window,
// and batch fields are measured in handles; LookupRows is measured in table
// rows. DemandRows and OutputRows are measured in final output rows. For an
// IndexJoin, output rows are observed by the join; for a direct IndexLookUp,
// they are observed when a lookup task is fully consumed.
// AdmissionBlocked fields are wall-clock durations with overlapping waits in
// the same stage counted once.
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
	LookupBatchSize         uint64
	LookupPhysicalWindow    uint64
	LookupOutstandingAtStop uint64
	OuterAdmissionBlocked   time.Duration
	LookupAdmissionBlocked  time.Duration
	Stopped                 bool
}

// AdaptiveLimitConfig defines the immutable bounds of one statement-local
// adaptive LIMIT controller. Outer windows are measured in outer rows; lookup
// windows and batches are measured in index handles.
type AdaptiveLimitConfig struct {
	DemandRows             uint64
	InitialOuterWindow     uint64
	MaxOuterWindow         uint64
	InitialLookupWindow    uint64
	MaxLookupWindow        uint64
	InitialLookupBatchSize uint64
	MaxLookupBatchSize     uint64
}

// AdaptiveLimitController bounds speculative work for an early-stop LIMIT.
// It is owned by one executor tree and learns only from the current execution.
//
// The controller maintains two independent admission lifecycles:
//
//	outer rows:     ReserveOuter -> CommitOuter -> ObserveJoinProgress
//	lookup handles: ReserveLookup -> lookup task -> CompleteLookup / AbortLookup
//
// Outer admission is measured in IndexJoin outer rows. Lookup admission is
// measured in index handles. The accounting must remain separate because a
// table-side filter can make many handles produce few or no rows. Every
// reservation must eventually be committed, completed, aborted, or cleared by
// Stop. All mutable state below is protected by mu.
type AdaptiveLimitController struct {
	outerChanged            chan struct{}
	stopCh                  chan struct{}
	lookupChanged           chan struct{}
	lookupAdmissionBlocked  admissionBlockStats
	outerAdmissionBlocked   admissionBlockStats
	recentLookupYield       adaptiveYieldWindow
	recentOuterYield        adaptiveYieldWindow
	outerGrowthBarrier      uint64
	maxOuterWindow          uint64
	pendingOuterOutput      uint64
	outerReserved           uint64
	outerWindow             uint64
	lookupHandles           uint64
	lookupRows              uint64
	outerConsumed           uint64
	lookupOutstandingAtStop uint64
	demandRows              uint64
	outerFetched            uint64
	initialOuterWindow      uint64
	lookupReserved          uint64
	outerOutstandingAtStop  uint64
	outerNoOutputRows       uint64
	initialLookupWindow     uint64
	maxLookupWindow         uint64
	lookupWindow            uint64
	initialLookupBatchSize  uint64
	maxLookupBatchSize      uint64
	lookupBatchSize         uint64
	lookupGrowthProgress    uint64
	lookupNoOutputRows      uint64
	outputRows              uint64
	mu                      sync.Mutex
	stopped                 bool
	lookupInNoOutputPhase   bool
	mode                    adaptiveLimitMode
}

// NewAdaptiveLimitController creates a statement-local admission controller.
func NewAdaptiveLimitController(config AdaptiveLimitConfig) *AdaptiveLimitController {
	return newAdaptiveLimitController(config, adaptiveLimitIndexJoin)
}

// NewAdaptiveLimitLookupController creates a controller for a direct ordered
// IndexLookUp under LIMIT. Direct lookup has no Join outer stage, so only its
// lookup handle budget is initialized.
func NewAdaptiveLimitLookupController(
	demandRows, initialLookupWindow, maxLookupWindow, initialLookupBatchSize, maxLookupBatchSize uint64,
) *AdaptiveLimitController {
	return newAdaptiveLimitController(AdaptiveLimitConfig{
		DemandRows:             demandRows,
		InitialLookupWindow:    initialLookupWindow,
		MaxLookupWindow:        maxLookupWindow,
		InitialLookupBatchSize: initialLookupBatchSize,
		MaxLookupBatchSize:     maxLookupBatchSize,
	}, adaptiveLimitDirectIndexLookup)
}

func newAdaptiveLimitController(config AdaptiveLimitConfig, mode adaptiveLimitMode) *AdaptiveLimitController {
	demandRows := config.DemandRows
	initialOuterWindow, maxOuterWindow := config.InitialOuterWindow, config.MaxOuterWindow
	initialLookupWindow, maxLookupWindow := config.InitialLookupWindow, config.MaxLookupWindow
	initialOuterWindow, maxOuterWindow = normalizeAdaptiveWindow(initialOuterWindow, maxOuterWindow)
	initialLookupWindow, maxLookupWindow = normalizeAdaptiveWindow(initialLookupWindow, maxLookupWindow)
	maxLookupBatchSize := min(max(config.MaxLookupBatchSize, uint64(1)), maxLookupWindow)
	initialLookupBatchSize := min(
		max(config.InitialLookupBatchSize, min(initialLookupWindow, maxLookupBatchSize)),
		maxLookupBatchSize,
	)
	if demandRows > 0 && initialOuterWindow > demandRows {
		initialOuterWindow = demandRows
	}
	if demandRows > 0 && initialLookupWindow > demandRows {
		initialLookupWindow = demandRows
	}
	c := &AdaptiveLimitController{
		mode:                   mode,
		demandRows:             demandRows,
		initialOuterWindow:     initialOuterWindow,
		maxOuterWindow:         maxOuterWindow,
		outerWindow:            initialOuterWindow,
		initialLookupWindow:    initialLookupWindow,
		maxLookupWindow:        maxLookupWindow,
		lookupWindow:           initialLookupWindow,
		initialLookupBatchSize: initialLookupBatchSize,
		maxLookupBatchSize:     maxLookupBatchSize,
		lookupBatchSize:        initialLookupBatchSize,
		outerChanged:           make(chan struct{}, 1),
		lookupChanged:          make(chan struct{}, 1),
		stopCh:                 make(chan struct{}),
	}
	if mode == adaptiveLimitDirectIndexLookup {
		c.initialOuterWindow = 0
		c.maxOuterWindow = 0
		c.outerWindow = 0
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
	c.outputRows = 0
	c.outerFetched = 0
	c.outerConsumed = 0
	c.outerReserved = 0
	c.outerOutstandingAtStop = 0
	c.outerAdmissionBlocked = admissionBlockStats{}
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
	c.lookupBatchSize = c.initialLookupBatchSize
	c.lookupGrowthProgress = 0
	c.lookupOutstandingAtStop = 0
	c.lookupAdmissionBlocked = admissionBlockStats{}
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
	if c.mode == adaptiveLimitDirectIndexLookup {
		return 0, false, nil
	}
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
	waiting := false
	for {
		c.mu.Lock()
		if err := ctx.Err(); err != nil {
			if waiting {
				c.endAdmissionBlockedLocked(outer, time.Now())
			}
			c.mu.Unlock()
			return 0, false, err
		}
		if c.stopped {
			if waiting {
				c.endAdmissionBlockedLocked(outer, time.Now())
			}
			c.mu.Unlock()
			return 0, false, nil
		}
		window := c.lookupPhysicalWindowLocked()
		outstanding := c.lookupReserved
		changed := c.lookupChanged
		if outer {
			window = c.outerWindow
			// Fetched-but-unconsumed rows and uncommitted reservations both
			// consume outer admission capacity.
			outstanding = c.outerFetched - min(c.outerFetched, c.outerConsumed) + c.outerReserved
			changed = c.outerChanged
		}
		if outstanding < window {
			if waiting {
				c.endAdmissionBlockedLocked(outer, time.Now())
			}
			rows := min(uint64(maxRows), window-outstanding)
			if outer {
				c.outerReserved += rows
			} else {
				rows = min(rows, c.lookupBatchSize)
				c.lookupReserved += rows
			}
			c.mu.Unlock()
			return int(rows), true, nil
		}
		if !waiting {
			c.beginAdmissionBlockedLocked(outer, time.Now())
			waiting = true
		}
		stopCh := c.stopCh
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			c.endAdmissionBlocked(outer)
			return 0, false, ctx.Err()
		case <-stopCh:
			c.endAdmissionBlocked(outer)
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
			return 0, false, nil
		case <-changed:
		}
	}
}

func (c *AdaptiveLimitController) beginAdmissionBlockedLocked(outer bool, now time.Time) {
	stats := &c.lookupAdmissionBlocked
	if outer {
		stats = &c.outerAdmissionBlocked
	}
	if stats.waiters == 0 {
		stats.blockedSince = now
	}
	stats.waiters++
}

func (c *AdaptiveLimitController) endAdmissionBlocked(outer bool) {
	c.mu.Lock()
	c.endAdmissionBlockedLocked(outer, time.Now())
	c.mu.Unlock()
}

func (c *AdaptiveLimitController) endAdmissionBlockedLocked(outer bool, now time.Time) {
	stats := &c.lookupAdmissionBlocked
	if outer {
		stats = &c.outerAdmissionBlocked
	}
	if stats.waiters == 0 {
		return
	}
	stats.waiters--
	if stats.waiters == 0 {
		stats.blockedTime += now.Sub(stats.blockedSince)
		stats.blockedSince = time.Time{}
	}
}

func (c *AdaptiveLimitController) finishAdmissionBlockedLocked(outer bool, now time.Time) {
	stats := &c.lookupAdmissionBlocked
	if outer {
		stats = &c.outerAdmissionBlocked
	}
	if stats.waiters == 0 {
		return
	}
	stats.blockedTime += now.Sub(stats.blockedSince)
	stats.blockedSince = time.Time{}
	stats.waiters = 0
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
	if c.mode == adaptiveLimitDirectIndexLookup {
		c.outputRows = saturatingAdd(c.outputRows, uint64(rows))
		if c.outputRows >= c.demandRows {
			c.stopLocked()
			c.mu.Unlock()
			return
		}
	}
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

// SuggestedBatchSize returns the current execution batch bounded by the
// caller's configured batch ceiling.
func (c *AdaptiveLimitController) SuggestedBatchSize(ceiling int) int {
	if ceiling < 1 {
		return 1
	}
	c.mu.Lock()
	batchSize := c.lookupBatchSize
	c.mu.Unlock()
	return min(max(int(min(batchSize, uint64(ceiling))), 1), ceiling)
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
		LookupBatchSize:         c.lookupBatchSize,
		LookupPhysicalWindow:    c.lookupPhysicalWindowLocked(),
		LookupOutstandingAtStop: c.lookupOutstandingAtStop,
		OuterAdmissionBlocked:   c.admissionBlockedTimeLocked(true),
		LookupAdmissionBlocked:  c.admissionBlockedTimeLocked(false),
		Stopped:                 c.stopped,
	}
}

func (c *AdaptiveLimitController) recomputeOuterWindowLocked() {
	// Estimate the input needed for the remaining LIMIT from cumulative yield.
	// The recent estimate can detect a low-yield region hidden by earlier
	// productive input; taking the larger estimate is conservative for progress.
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
	// Preserve more headroom early in the statement, then taper it as LIMIT
	// completion approaches to reduce tail over-admission.
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
	if c.mode == adaptiveLimitDirectIndexLookup {
		c.recomputeDirectLookupWindowLocked()
		return
	}
	if c.lookupRows == 0 || c.lookupInNoOutputPhase {
		return
	}
	lookupBuffered := c.lookupRows - min(c.lookupRows, c.outerConsumed)
	outerBuffered := c.outerFetched - min(c.outerFetched, c.outerConsumed)
	// Both stages can already hold rows that will satisfy the outer window. Use
	// the larger visible buffer to avoid admitting the same demand twice.
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
	// Productive feedback adjusts the logical budget. Execution granularity is
	// tracked separately by lookupBatchSize, so shrinking this window does not
	// turn a small LIMIT into row-at-a-time table RPCs.
	var grew bool
	c.lookupWindow, grew = adjustAdaptiveWindow(
		target, c.lookupWindow, c.initialLookupWindow, c.maxLookupWindow,
		c.lookupHandles > c.lookupGrowthProgress,
	)
	if grew {
		c.lookupGrowthProgress = c.lookupHandles
	}
}

func (c *AdaptiveLimitController) recomputeDirectLookupWindowLocked() {
	if c.lookupRows == 0 || c.lookupInNoOutputPhase {
		return
	}
	remainingOutput := c.demandRows - min(c.demandRows, c.outputRows)
	estimatedHandles := divideAndRoundUp(
		saturatingMultiply(remainingOutput, c.lookupHandles),
		c.lookupRows,
	)
	recentHandles, recentRows := c.recentLookupYield.totals()
	if recentRows > 0 {
		recentEstimate := divideAndRoundUp(
			saturatingMultiply(remainingOutput, recentHandles),
			recentRows,
		)
		estimatedHandles = max(estimatedHandles, recentEstimate)
	}

	var target uint64
	switch {
	case remainingOutput <= c.demandRows/4:
		target = estimatedHandles
	case remainingOutput <= c.demandRows/2:
		target = divideAndRoundUp(saturatingMultiply(estimatedHandles, 9), 8)
	default:
		target = divideAndRoundUp(saturatingMultiply(estimatedHandles, 5), 4)
	}
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
	// A zero-output phase has no usable yield ratio. Grow only after the current
	// window produced no output and all outstanding work drained, which
	// guarantees progress without jumping to the maximum after one empty task.
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
	// Grow both the logical budget and execution batch after a fully drained
	// zero-output phase. Growing only the budget would still force a long series
	// of tiny table lookup tasks through a low-selectivity interval.
	if c.lookupReserved != 0 || c.lookupNoOutputRows < c.lookupWindow {
		return
	}
	nextWindow := growAdaptiveWindow(c.lookupWindow, c.maxLookupWindow)
	if nextWindow > c.lookupWindow {
		c.lookupGrowthProgress = c.lookupHandles
	}
	c.lookupWindow = nextWindow
	c.lookupBatchSize = growAdaptiveWindow(c.lookupBatchSize, c.maxLookupBatchSize)
	c.lookupNoOutputRows = 0
}

func (c *AdaptiveLimitController) lookupPhysicalWindowLocked() uint64 {
	// Round the logical handle budget up to whole execution batches. Unless the
	// configured maximum truncates it, physical slack is less than one batch.
	if c.lookupWindow == 0 || c.lookupBatchSize == 0 {
		return 0
	}
	batchCount := divideAndRoundUp(c.lookupWindow, c.lookupBatchSize)
	return min(saturatingMultiply(batchCount, c.lookupBatchSize), c.maxLookupWindow)
}

func (c *AdaptiveLimitController) stopLocked() {
	if c.stopped {
		return
	}
	c.stopped = true
	now := time.Now()
	c.finishAdmissionBlockedLocked(true, now)
	c.finishAdmissionBlockedLocked(false, now)
	c.outerOutstandingAtStop = saturatingAdd(
		c.outerFetched-min(c.outerFetched, c.outerConsumed),
		c.outerReserved,
	)
	c.lookupOutstandingAtStop = c.lookupReserved
	c.lookupReserved = 0
	c.outerReserved = 0
	c.outerWindow = 0
	c.lookupWindow = 0
	c.lookupBatchSize = 0
	close(c.stopCh)
}

func (c *AdaptiveLimitController) admissionBlockedTimeLocked(outer bool) time.Duration {
	stats := c.lookupAdmissionBlocked
	if outer {
		stats = c.outerAdmissionBlocked
	}
	if stats.waiters == 0 {
		return stats.blockedTime
	}
	return stats.blockedTime + time.Since(stats.blockedSince)
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

func normalizeAdaptiveWindow(initial, maximum uint64) (normalizedInitial, normalizedMaximum uint64) {
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
	// Shrink immediately near LIMIT completion. Growth requires new input
	// progress and is capped at 2x so transient feedback cannot open the window
	// without bound in one adjustment.
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
