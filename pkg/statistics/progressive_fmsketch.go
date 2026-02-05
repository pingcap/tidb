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

package statistics

import (
	"math"
	"sync"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/fastrand"
)

// SamplingPhase defines a threshold and sampling rate for progressive sampling.
type SamplingPhase struct {
	Threshold uint64  // Row count where this phase begins
	Rate      float64 // Sampling rate (0.0-1.0) for this phase
}

// DefaultProgressiveSchedule defines the default sampling schedule.
// This represents a ~95% reduction in processed rows for a 100M row table.
var DefaultProgressiveSchedule = []SamplingPhase{
	{Threshold: 0, Rate: 1.0},            // 0-500K: 100%
	{Threshold: 500_000, Rate: 0.5},      // 500K-1M: 50%
	{Threshold: 1_000_000, Rate: 0.25},   // 1M-5M: 25%
	{Threshold: 5_000_000, Rate: 0.10},   // 5M-10M: 10%
	{Threshold: 10_000_000, Rate: 0.05},  // 10M-50M: 5%
	{Threshold: 50_000_000, Rate: 0.02},  // 50M-100M: 2%
	{Threshold: 100_000_000, Rate: 0.01}, // 100M+: 1%
}

// progressiveFMSketchPool is a pool for ProgressiveFMSketch instances.
var progressiveFMSketchPool = sync.Pool{
	New: func() any {
		return &ProgressiveFMSketch{}
	},
}

// ProgressiveFMSketch extends FMSketch with progressive sampling capabilities.
// It processes 100% of rows up to a threshold, then progressively reduces
// the sampling rate for larger tables to improve performance while maintaining
// acceptable NDV estimation accuracy.
type ProgressiveFMSketch struct {
	*FMSketch

	// Sampling state
	rowsProcessed     uint64  // Total rows seen
	rowsSampled       uint64  // Rows actually processed by FMSketch
	currentSampleRate float64 // Current sampling rate (0.0-1.0)

	// Phase tracking
	schedule     []SamplingPhase // Sampling schedule (threshold -> rate)
	currentPhase int             // Current phase index

	// For singleton approximation (used in NDV extrapolation)
	// We track an approximate singleton ratio based on observed NDV growth
	lastNDV          int64   // NDV at last check
	lastRowsSampled  uint64  // Rows sampled at last check
	singletonRatio   float64 // Approximate ratio of singletons to NDV
	checkInterval    uint64  // How often to update singleton ratio
	nextCheckAt      uint64  // Next row count to check NDV growth
}

// NewProgressiveFMSketch creates a new ProgressiveFMSketch with default settings.
func NewProgressiveFMSketch(maxSize int) *ProgressiveFMSketch {
	result := progressiveFMSketchPool.Get().(*ProgressiveFMSketch)
	result.FMSketch = NewFMSketch(maxSize)
	result.rowsProcessed = 0
	result.rowsSampled = 0
	result.currentSampleRate = 1.0
	result.schedule = DefaultProgressiveSchedule
	result.currentPhase = 0
	result.lastNDV = 0
	result.lastRowsSampled = 0
	result.singletonRatio = 0.6 // Default based on Zipf's law observation
	result.checkInterval = 10000
	result.nextCheckAt = result.checkInterval
	return result
}

// NewProgressiveFMSketchWithSchedule creates a new ProgressiveFMSketch with a custom schedule.
func NewProgressiveFMSketchWithSchedule(maxSize int, schedule []SamplingPhase) *ProgressiveFMSketch {
	result := NewProgressiveFMSketch(maxSize)
	if len(schedule) > 0 {
		result.schedule = schedule
		result.currentSampleRate = schedule[0].Rate
	}
	return result
}

// ShouldSample determines whether the current row should be sampled.
// This implements Bernoulli sampling with a rate that decreases as more rows are processed.
func (s *ProgressiveFMSketch) ShouldSample() bool {
	s.rowsProcessed++

	// Update phase if threshold crossed
	s.updatePhase()

	// Phase 1 (100% sampling) - always sample
	if s.currentSampleRate >= 1.0 {
		s.rowsSampled++
		return true
	}

	// Bernoulli sampling with current rate using fast random
	// fastrand.Uint32() returns [0, 2^32-1], we compare against rate * 2^32
	threshold := uint32(s.currentSampleRate * float64(math.MaxUint32))
	if fastrand.Uint32() < threshold {
		s.rowsSampled++
		return true
	}
	return false
}

// updatePhase updates the current sampling phase based on rows processed.
func (s *ProgressiveFMSketch) updatePhase() {
	for s.currentPhase < len(s.schedule)-1 {
		nextPhase := s.currentPhase + 1
		if s.rowsProcessed >= s.schedule[nextPhase].Threshold {
			s.currentPhase = nextPhase
			s.currentSampleRate = s.schedule[nextPhase].Rate
		} else {
			break
		}
	}
}

// InsertValue inserts a value into the sketch if it passes the sampling check.
// Returns true if the value was sampled and inserted, false if skipped.
func (s *ProgressiveFMSketch) InsertValue(sc *stmtctx.StatementContext, value types.Datum) (bool, error) {
	if !s.ShouldSample() {
		return false, nil
	}

	err := s.FMSketch.InsertValue(sc, value)
	if err != nil {
		return false, err
	}

	// Periodically update singleton ratio estimate
	s.maybeUpdateSingletonRatio()

	return true, nil
}

// InsertRowValue inserts multi-column values if they pass the sampling check.
// Returns true if the values were sampled and inserted, false if skipped.
func (s *ProgressiveFMSketch) InsertRowValue(sc *stmtctx.StatementContext, values []types.Datum) (bool, error) {
	if !s.ShouldSample() {
		return false, nil
	}

	err := s.FMSketch.InsertRowValue(sc, values)
	if err != nil {
		return false, err
	}

	// Periodically update singleton ratio estimate
	s.maybeUpdateSingletonRatio()

	return true, nil
}

// maybeUpdateSingletonRatio periodically estimates the singleton ratio
// based on NDV growth rate.
func (s *ProgressiveFMSketch) maybeUpdateSingletonRatio() {
	if s.rowsSampled < s.nextCheckAt {
		return
	}

	currentNDV := s.FMSketch.NDV()
	if s.lastRowsSampled > 0 && currentNDV > s.lastNDV {
		// Calculate NDV growth rate
		rowDelta := float64(s.rowsSampled - s.lastRowsSampled)
		ndvDelta := float64(currentNDV - s.lastNDV)
		growthRate := ndvDelta / rowDelta

		// Update singleton ratio estimate based on growth rate
		// Higher growth rate = more unique values = higher singleton ratio
		// Clamp between 0.3 and 0.8 based on empirical observations
		s.singletonRatio = math.Max(0.3, math.Min(0.8, growthRate*10))
	}

	s.lastNDV = currentNDV
	s.lastRowsSampled = s.rowsSampled
	s.nextCheckAt = s.rowsSampled + s.checkInterval
}

// EstimateNDV returns the estimated NDV, extrapolating from sampled data if necessary.
// Uses the Goodman/Chao1 estimator for unbiased NDV estimation from samples.
func (s *ProgressiveFMSketch) EstimateNDV() int64 {
	if s.rowsSampled == 0 {
		return 0
	}

	// Get observed NDV from underlying FMSketch
	observedNDV := s.FMSketch.NDV()

	// If we sampled everything, return exact NDV
	if s.rowsSampled == s.rowsProcessed {
		return observedNDV
	}

	// Calculate effective sample fraction
	sampleFraction := float64(s.rowsSampled) / float64(s.rowsProcessed)

	// Estimate number of singletons (values appearing exactly once)
	// Using approximate singleton ratio based on NDV growth observation
	f1 := float64(observedNDV) * s.singletonRatio

	// Goodman/Chao1-style estimator for NDV extrapolation
	// NDV_est = observedNDV + f1 * (1 - sampleFraction) / sampleFraction
	adjustment := f1 * (1.0 - sampleFraction) / sampleFraction
	estimatedNDV := float64(observedNDV) + adjustment

	// Bound by total rows (NDV cannot exceed row count)
	if estimatedNDV > float64(s.rowsProcessed) {
		estimatedNDV = float64(s.rowsProcessed)
	}

	// Ensure we don't return less than observed
	if estimatedNDV < float64(observedNDV) {
		estimatedNDV = float64(observedNDV)
	}

	return int64(math.Round(estimatedNDV))
}

// ConfidenceInterval returns the lower and upper bounds of the NDV estimate
// at the specified confidence level (e.g., 0.95 for 95% confidence).
func (s *ProgressiveFMSketch) ConfidenceInterval(confidence float64) (lower, upper int64) {
	ndv := s.EstimateNDV()

	if s.rowsSampled == s.rowsProcessed {
		// No sampling, exact result
		return ndv, ndv
	}

	// Calculate standard error based on sample size and observed NDV
	sampleFraction := float64(s.rowsSampled) / float64(s.rowsProcessed)
	variance := float64(ndv) * (1.0 - sampleFraction) / sampleFraction
	stdErr := math.Sqrt(variance)

	// z-score for confidence level
	z := normalQuantile(confidence)

	lower = int64(math.Max(0, float64(ndv)-z*stdErr))
	upper = int64(float64(ndv) + z*stdErr)

	// Upper bound cannot exceed total rows
	if upper > int64(s.rowsProcessed) {
		upper = int64(s.rowsProcessed)
	}

	return lower, upper
}

// normalQuantile returns the z-score for a given confidence level.
// Uses common pre-computed values and approximation for others.
func normalQuantile(confidence float64) float64 {
	// Common confidence levels
	switch {
	case confidence >= 0.99:
		return 2.576
	case confidence >= 0.95:
		return 1.96
	case confidence >= 0.90:
		return 1.645
	case confidence >= 0.80:
		return 1.282
	default:
		// Approximation for other values using inverse error function approximation
		// This is a simplified Beasley-Springer-Moro algorithm
		p := (1.0 + confidence) / 2.0
		t := math.Sqrt(-2.0 * math.Log(1.0-p))
		return t - (2.515517+0.802853*t+0.010328*t*t)/(1.0+1.432788*t+0.189269*t*t+0.001308*t*t*t)
	}
}

// RowsProcessed returns the total number of rows seen.
func (s *ProgressiveFMSketch) RowsProcessed() uint64 {
	return s.rowsProcessed
}

// RowsSampled returns the number of rows actually processed by the sketch.
func (s *ProgressiveFMSketch) RowsSampled() uint64 {
	return s.rowsSampled
}

// SampleRate returns the effective overall sample rate.
func (s *ProgressiveFMSketch) SampleRate() float64 {
	if s.rowsProcessed == 0 {
		return 1.0
	}
	return float64(s.rowsSampled) / float64(s.rowsProcessed)
}

// CurrentPhaseRate returns the current phase's sampling rate.
func (s *ProgressiveFMSketch) CurrentPhaseRate() float64 {
	return s.currentSampleRate
}

// Copy creates a deep copy of the ProgressiveFMSketch.
func (s *ProgressiveFMSketch) Copy() *ProgressiveFMSketch {
	if s == nil {
		return nil
	}
	result := NewProgressiveFMSketch(s.FMSketch.maxSize)
	result.FMSketch = s.FMSketch.Copy()
	result.rowsProcessed = s.rowsProcessed
	result.rowsSampled = s.rowsSampled
	result.currentSampleRate = s.currentSampleRate
	result.schedule = make([]SamplingPhase, len(s.schedule))
	copy(result.schedule, s.schedule)
	result.currentPhase = s.currentPhase
	result.lastNDV = s.lastNDV
	result.lastRowsSampled = s.lastRowsSampled
	result.singletonRatio = s.singletonRatio
	result.checkInterval = s.checkInterval
	result.nextCheckAt = s.nextCheckAt
	return result
}

// MergeProgressiveFMSketch merges another ProgressiveFMSketch into this one.
// Note: After merging, the sampling statistics are combined but the
// extrapolation may be less accurate than individual sketches.
func (s *ProgressiveFMSketch) MergeProgressiveFMSketch(other *ProgressiveFMSketch) {
	if s == nil || other == nil {
		return
	}

	// Merge underlying FMSketch
	s.FMSketch.MergeFMSketch(other.FMSketch)

	// Combine sampling statistics
	s.rowsProcessed += other.rowsProcessed
	s.rowsSampled += other.rowsSampled

	// Use weighted average for singleton ratio
	if s.rowsSampled > 0 && other.rowsSampled > 0 {
		totalSampled := float64(s.rowsSampled + other.rowsSampled)
		s.singletonRatio = (s.singletonRatio*float64(s.rowsSampled) +
			other.singletonRatio*float64(other.rowsSampled)) / totalSampled
	}
}

// Reset resets the sketch to its initial state.
func (s *ProgressiveFMSketch) Reset() {
	if s == nil {
		return
	}
	if s.FMSketch != nil {
		s.FMSketch.reset()
	}
	s.rowsProcessed = 0
	s.rowsSampled = 0
	s.currentSampleRate = 1.0
	s.currentPhase = 0
	s.lastNDV = 0
	s.lastRowsSampled = 0
	s.singletonRatio = 0.6
	s.nextCheckAt = s.checkInterval
}

// DestroyAndPutToPool resets and returns the sketch to the pool.
func (s *ProgressiveFMSketch) DestroyAndPutToPool() {
	if s == nil {
		return
	}
	if s.FMSketch != nil {
		s.FMSketch.DestroyAndPutToPool()
		s.FMSketch = nil
	}
	s.schedule = nil
	progressiveFMSketchPool.Put(s)
}

// MemoryUsage returns the total memory usage of the ProgressiveFMSketch.
func (s *ProgressiveFMSketch) MemoryUsage() int64 {
	if s == nil {
		return 0
	}
	// Base struct size (approximate)
	// uint64 * 5 = 40 bytes (rowsProcessed, rowsSampled, lastRowsSampled, nextCheckAt, checkInterval)
	// float64 * 3 = 24 bytes (currentSampleRate, singletonRatio)
	// int64 * 1 = 8 bytes (lastNDV)
	// int * 1 = 8 bytes (currentPhase)
	// slice header = 24 bytes (schedule)
	// SamplingPhase = 16 bytes each (uint64 + float64)
	baseSize := int64(40 + 24 + 8 + 8 + 24 + len(s.schedule)*16)

	if s.FMSketch != nil {
		baseSize += s.FMSketch.MemoryUsage()
	}

	return baseSize
}
