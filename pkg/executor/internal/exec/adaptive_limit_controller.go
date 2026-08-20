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

// adaptiveAdmissionStage identifies both the admitted unit and the accounting
// path: outer admission counts rows, while lookup admission counts handles.
type adaptiveAdmissionStage uint8

const (
	adaptiveOuterRowsAdmission adaptiveAdmissionStage = iota
	adaptiveLookupHandlesAdmission
)

// adaptiveYieldWindow keeps a small recent sample alongside the controller's
// cumulative counters. The recent sample lets an ordered scan react when the
// selectivity near its current position differs from earlier input.
type adaptiveYieldWindow struct {
	// inputs stores recent input counts: outer rows for join feedback or
	// index handles for lookup feedback.
	inputs [adaptiveYieldWindowSize]uint64
	// outputs stores the output count paired with each inputs sample.
	outputs [adaptiveYieldWindowSize]uint64
	// next is the ring-buffer slot replaced by the next sample.
	next uint8
}

// admissionBlockStats measures the union of admission-blocked intervals for
// one stage. Concurrent waiters overlap in wall-clock time and are counted once.
type admissionBlockStats struct {
	// blockedSince is the start of the current blocked interval, or zero when
	// no reserver is waiting.
	blockedSince time.Time
	// waiters is the number of reservers currently blocked by admission.
	waiters int
	// blockedTime accumulates completed blocked intervals.
	blockedTime time.Duration
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
	// DemandRows is the final-row demand derived from LIMIT offset plus count.
	DemandRows uint64
	// OutputRows is the cumulative number of final rows observed by the controller.
	OutputRows uint64
	// OuterFetched is the cumulative number of committed IndexJoin outer rows.
	OuterFetched uint64
	// OuterConsumed is the cumulative number of outer rows fully consumed by the join.
	OuterConsumed uint64
	// OuterReserved is the number of admitted outer rows not yet committed or aborted.
	OuterReserved uint64
	// OuterWindow is the current logical outer-row admission limit.
	OuterWindow uint64
	// OuterOutstandingAtStop captures fetched-but-unconsumed and reserved outer
	// rows when the controller stops.
	OuterOutstandingAtStop uint64
	// LookupReserved is the number of admitted handles not yet completed or aborted.
	LookupReserved uint64
	// LookupHandles is the cumulative number of handles in fully consumed lookup tasks.
	LookupHandles uint64
	// LookupRows is the cumulative number of table rows returned by those lookup tasks.
	LookupRows uint64
	// LookupWindow is the current logical lookup-handle admission limit.
	LookupWindow uint64
	// LookupBatchSize is the current execution-task size in index handles.
	LookupBatchSize uint64
	// LookupPhysicalWindow is LookupWindow rounded to executable whole batches
	// without exceeding the configured maximum window.
	LookupPhysicalWindow uint64
	// LookupOutstandingAtStop captures reserved lookup handles when the controller stops.
	LookupOutstandingAtStop uint64
	// OuterAdmissionBlocked is the wall-clock time with at least one blocked
	// outer reserver.
	OuterAdmissionBlocked time.Duration
	// LookupAdmissionBlocked is the wall-clock time with at least one blocked
	// lookup reserver.
	LookupAdmissionBlocked time.Duration
	// Stopped reports whether the controller rejects future admission.
	Stopped bool
}

// AdaptiveLimitConfig defines the immutable bounds of one statement-local
// adaptive LIMIT controller. Outer windows are measured in outer rows; lookup
// windows and batches are measured in index handles.
type AdaptiveLimitConfig struct {
	// DemandRows is the final-row demand derived from LIMIT offset plus count.
	DemandRows uint64
	// InitialOuterWindow is the starting outer-row admission window.
	InitialOuterWindow uint64
	// MaxOuterWindow is the upper bound of the outer-row admission window.
	MaxOuterWindow uint64
	// InitialLookupWindow is the starting lookup-handle admission window.
	InitialLookupWindow uint64
	// MaxLookupWindow is the upper bound of the lookup-handle admission window.
	MaxLookupWindow uint64
	// InitialLookupBatchSize is the starting number of handles assigned to one
	// lookup task.
	InitialLookupBatchSize uint64
	// MaxLookupBatchSize is the upper bound on handles assigned to one lookup task.
	MaxLookupBatchSize uint64
}

// AdaptiveLimitController bounds speculative work for an early-stop LIMIT.
// It is owned by one executor tree and learns only from the current execution.
//
// The controller maintains two independent admission lifecycles:
//
//	outer rows:     ReserveOuter -> CommitOuter -> ObserveJoinProgress
//	lookup handles: ReserveLookup -> lookup task -> CompleteLookup / AbortLookup
//
// Both lifecycles are active for an IndexJoin: lookup admission bounds the
// index handles used to produce outer rows, while outer admission bounds the
// rows consumed by the join to produce final rows. A direct IndexLookUp uses
// only the lookup lifecycle.
//
// Outer admission is measured in IndexJoin outer rows. Lookup admission is
// measured in index handles. The accounting must remain separate because a
// table-side filter can make many handles produce few or no rows. Every
// reservation must eventually be committed, completed, aborted, or cleared by
// Stop. All mutable state below is protected by mu.
type AdaptiveLimitController struct {
	// outerChanged wakes outer reservers after capacity or lifecycle changes.
	// Its single buffered signal coalesces duplicate notifications.
	outerChanged chan struct{}
	// stopCh is closed when the controller stops so all blocked producers wake up.
	stopCh chan struct{}
	// lookupChanged wakes lookup reservers after capacity or lifecycle changes.
	// Its single buffered signal coalesces duplicate notifications.
	lookupChanged chan struct{}
	// lookupAdmissionBlocked tracks lookup admission waiters and union wait time.
	lookupAdmissionBlocked admissionBlockStats
	// outerAdmissionBlocked tracks outer admission waiters and union wait time.
	outerAdmissionBlocked admissionBlockStats
	// recentLookupYield tracks recent handle-to-table-row yield near the current
	// ordered scan position.
	recentLookupYield adaptiveYieldWindow
	// recentOuterYield tracks recent consumed-outer-to-final-row yield.
	recentOuterYield adaptiveYieldWindow
	// outerGrowthBarrier records outerFetched at the latest productive growth,
	// preventing another growth decision until consumption passes that frontier.
	outerGrowthBarrier uint64
	// maxOuterWindow is the immutable upper bound of outer admission.
	maxOuterWindow uint64
	// pendingOuterOutput holds final rows observed before their corresponding
	// outer rows are reported as consumed.
	pendingOuterOutput uint64
	// outerReserved counts admitted outer rows not yet committed or aborted.
	outerReserved uint64
	// outerWindow is the current logical outer-row admission limit.
	outerWindow uint64
	// lookupHandles is the cumulative handle count from fully consumed lookup tasks.
	lookupHandles uint64
	// lookupRows is the cumulative table-row count from fully consumed lookup tasks.
	lookupRows uint64
	// outerConsumed is the cumulative number of outer rows fully consumed by the join.
	outerConsumed uint64
	// lookupOutstandingAtStop captures lookupReserved when the controller stops.
	lookupOutstandingAtStop uint64
	// demandRows is the immutable final-row demand derived from LIMIT offset plus count.
	demandRows uint64
	// outerFetched is the cumulative number of committed IndexJoin outer rows.
	outerFetched uint64
	// initialOuterWindow is the outer-row window restored by Reset.
	initialOuterWindow uint64
	// lookupReserved counts admitted handles not yet completed or aborted.
	lookupReserved uint64
	// outerOutstandingAtStop captures fetched-but-unconsumed and reserved outer
	// rows when the controller stops.
	outerOutstandingAtStop uint64
	// outerNoOutputRows counts outer rows consumed in the current zero-output phase.
	outerNoOutputRows uint64
	// initialLookupWindow is the logical lookup-handle window restored by Reset.
	initialLookupWindow uint64
	// maxLookupWindow is the immutable upper bound of lookup-handle admission.
	maxLookupWindow uint64
	// lookupWindow is the current logical lookup-handle admission limit.
	lookupWindow uint64
	// initialLookupBatchSize is the lookup task size restored by Reset.
	initialLookupBatchSize uint64
	// maxLookupBatchSize is the immutable upper bound on lookup task size.
	maxLookupBatchSize uint64
	// lookupBatchSize is the current execution-task size in index handles,
	// independent of the logical lookup window.
	lookupBatchSize uint64
	// lookupGrowthProgress records lookupHandles at the latest growth decision,
	// preventing repeated growth without newly completed lookup work.
	lookupGrowthProgress uint64
	// lookupNoOutputRows counts handles consumed in the current zero-output phase.
	lookupNoOutputRows uint64
	// outputRows is the cumulative number of final rows observed by the controller.
	outputRows uint64
	// mu protects lifecycle state, counters, feedback windows, and wait statistics.
	mu sync.Mutex
	// stopped indicates that future admission must fail and stopCh has been closed.
	stopped bool
	// lookupInNoOutputPhase suppresses productive lookup-yield adjustment until
	// a subsequent lookup task returns rows.
	lookupInNoOutputPhase bool
	// mode selects IndexJoin accounting or direct IndexLookUp accounting.
	mode adaptiveLimitMode
}

// NewAdaptiveLimitController creates a statement-local admission controller.
func NewAdaptiveLimitController(config AdaptiveLimitConfig) *AdaptiveLimitController {
	return newAdaptiveLimitController(config, adaptiveLimitIndexJoin)
}

// NewAdaptiveLimitLookupController creates a controller for a direct ordered
// IndexLookUp under LIMIT. Direct lookup has no Join outer stage, so only its
// lookup handle budget is initialized.
func NewAdaptiveLimitLookupController(config AdaptiveLimitConfig) *AdaptiveLimitController {
	return newAdaptiveLimitController(config, adaptiveLimitDirectIndexLookup)
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
	return c.reserve(ctx, maxRows, adaptiveOuterRowsAdmission)
}

// ReserveLookup bounds handles admitted to the double-read table lookup stage.
// The bool is false after LIMIT completion; context cancellation is returned as
// an error.
func (c *AdaptiveLimitController) ReserveLookup(ctx context.Context, maxHandles int) (int, bool, error) {
	return c.reserve(ctx, maxHandles, adaptiveLookupHandlesAdmission)
}

func (c *AdaptiveLimitController) reserve(
	ctx context.Context, maxUnits int, stage adaptiveAdmissionStage,
) (int, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if maxUnits <= 0 {
		return 0, true, nil
	}
	waiting := false
	for {
		c.mu.Lock()
		if err := ctx.Err(); err != nil {
			if waiting {
				c.endAdmissionBlockedLocked(stage, time.Now())
			}
			c.mu.Unlock()
			return 0, false, err
		}
		if c.stopped {
			if waiting {
				c.endAdmissionBlockedLocked(stage, time.Now())
			}
			c.mu.Unlock()
			return 0, false, nil
		}
		window := c.lookupPhysicalWindowLocked()
		outstanding := c.lookupReserved
		changed := c.lookupChanged
		if stage == adaptiveOuterRowsAdmission {
			window = c.outerWindow
			// Fetched-but-unconsumed rows and uncommitted reservations both
			// consume outer admission capacity.
			outstanding = c.outerFetched - min(c.outerFetched, c.outerConsumed) + c.outerReserved
			changed = c.outerChanged
		}
		if outstanding < window {
			if waiting {
				c.endAdmissionBlockedLocked(stage, time.Now())
			}
			units := min(uint64(maxUnits), window-outstanding)
			if stage == adaptiveOuterRowsAdmission {
				c.outerReserved += units
			} else {
				units = min(units, c.lookupBatchSize)
				c.lookupReserved += units
			}
			c.mu.Unlock()
			return int(units), true, nil
		}
		if !waiting {
			c.beginAdmissionBlockedLocked(stage, time.Now())
			waiting = true
		}
		stopCh := c.stopCh
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			c.endAdmissionBlocked(stage)
			return 0, false, ctx.Err()
		case <-stopCh:
			c.endAdmissionBlocked(stage)
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
			return 0, false, nil
		case <-changed:
		}
	}
}

func (c *AdaptiveLimitController) beginAdmissionBlockedLocked(stage adaptiveAdmissionStage, now time.Time) {
	stats := &c.lookupAdmissionBlocked
	if stage == adaptiveOuterRowsAdmission {
		stats = &c.outerAdmissionBlocked
	}
	if stats.waiters == 0 {
		stats.blockedSince = now
	}
	stats.waiters++
}

func (c *AdaptiveLimitController) endAdmissionBlocked(stage adaptiveAdmissionStage) {
	c.mu.Lock()
	c.endAdmissionBlockedLocked(stage, time.Now())
	c.mu.Unlock()
}

func (c *AdaptiveLimitController) endAdmissionBlockedLocked(stage adaptiveAdmissionStage, now time.Time) {
	stats := &c.lookupAdmissionBlocked
	if stage == adaptiveOuterRowsAdmission {
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

func (c *AdaptiveLimitController) finishAdmissionBlockedLocked(stage adaptiveAdmissionStage, now time.Time) {
	stats := &c.lookupAdmissionBlocked
	if stage == adaptiveOuterRowsAdmission {
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
		OuterAdmissionBlocked:   c.admissionBlockedTimeLocked(adaptiveOuterRowsAdmission),
		LookupAdmissionBlocked:  c.admissionBlockedTimeLocked(adaptiveLookupHandlesAdmission),
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
	target := addAdaptiveWindowHeadroom(estimatedInput, remainingOutput, c.demandRows)
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

	target := addAdaptiveWindowHeadroom(estimatedHandles, remainingOutput, c.demandRows)
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
	c.finishAdmissionBlockedLocked(adaptiveOuterRowsAdmission, now)
	c.finishAdmissionBlockedLocked(adaptiveLookupHandlesAdmission, now)
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

func (c *AdaptiveLimitController) admissionBlockedTimeLocked(stage adaptiveAdmissionStage) time.Duration {
	stats := c.lookupAdmissionBlocked
	if stage == adaptiveOuterRowsAdmission {
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

func addAdaptiveWindowHeadroom(estimatedInput, remainingOutput, demandRows uint64) uint64 {
	// Preserve more headroom early in the statement, then taper it as LIMIT
	// completion approaches to reduce tail over-admission.
	switch {
	case remainingOutput <= demandRows/4:
		return estimatedInput
	case remainingOutput <= demandRows/2:
		return divideAndRoundUp(saturatingMultiply(estimatedInput, 9), 8)
	default:
		return divideAndRoundUp(saturatingMultiply(estimatedInput, 5), 4)
	}
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
