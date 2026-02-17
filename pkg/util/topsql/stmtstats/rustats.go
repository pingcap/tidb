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

// Package stmtstats provides statement statistics collection.
//
// TopRU Design Notes (Phase 1 - M1/M2):
//
// This file defines RU (Request Unit) collection data structures for TopRU feature.
// TopRU collects per-SQL RU consumption and aggregates by (user, sql_digest, plan_digest),
// running in parallel with TopSQL (CPU time) but using separate data paths.
//
// Key Design Decisions:
//   - D2: RU data stored separately from TopSQL stmtstats (not in StatementStatsItem)
//   - Delta-based sampling: Only RU deltas are collected to avoid double-counting
//   - User dimension: RUKey extends SQL/Plan digest with User for per-user attribution
//
// Data Flow (Phase 1):
//
//	Session -> StatementStats (execCtx + finishedRUBuffer)
//	-> aggregator.aggregateRU() [1s tick]
//	-> RUCollector.CollectRUIncrements()
//	-> Reporter channel
//
// Phase 2 Status:
//   - Two-level TopN: Implemented in reporter/ru_datamodel.go (200 users × 200 SQLs)
//   - Backpressure: Implemented in aggregator.go (10,000 key cap)
//   - TODO(M4): Executor hooks (OnRUExecutionBegin/Finished calls)
package stmtstats

import "github.com/tikv/client-go/v2/util"

// RUKey uniquely identifies a SQL execution for RU aggregation.
// Unlike TopSQL which aggregates by (sql_digest, plan_digest),
// TopRU aggregates by (user, sql_digest, plan_digest) to support
// per-user RU attribution.
//
// Design Rationale:
//   - User dimension enables per-tenant RU diagnosis in multi-tenant scenarios
//   - Reuses BinaryDigest type from existing TopSQL infrastructure
//
// Phase 2 Extension Point:
//   - TODO(M3): May add ResourceGroup field for resource control integration
type RUKey struct {
	User       string
	SQLDigest  BinaryDigest
	PlanDigest BinaryDigest
}

// ExecutionContext tracks RU consumption for a single SQL execution.
// It holds the cached RUDetails pointer, identifying key, and the last sampled
// RU value to enable delta calculation on each tick.
//
// Design Invariants:
//   - At most 1 active ExecutionContext per session at any time
//   - Created at SQL start (OnRUExecutionBegin), cleared at SQL finish (OnRUExecutionFinished)
//   - LastRUTotal is updated after each sample/finish to enable correct delta
//
// Delta Calculation:
//   - RUDetails contains cumulative RRU+WRU values from tikv/client-go
//   - delta = (current.RRU + current.WRU) - LastRUTotal
//   - Negative or zero deltas are discarded (guards against reset/skew)
//
// Performance Note:
//   - RUDetails pointer is cached at begin time to avoid per-tick context.Value()
//     traversal. The pointer is stable for the execution lifetime; internal values
//     (RRU/WRU) are updated atomically by tikv client-go.
type ExecutionContext struct {
	// RUDetails is the cached *util.RUDetails extracted from context at execution begin.
	// Avoids repeated context.Value() lookups on every aggregator tick.
	RUDetails *util.RUDetails

	// Key identifies this execution by (user, sql_digest, plan_digest).
	Key RUKey

	// LastRUTotal stores the last observed cumulative RU total (RRU + WRU).
	// Used to compute delta = currentTotal - LastRUTotal.
	LastRUTotal float64
}

// RUIncrement represents a delta RU consumption for a specific RUKey.
// This is the unit of data produced by StatementStats.MergeRUInto() and
// consumed by RUCollector.CollectRUIncrements().
//
// Design Rationale:
//   - TotalRU: Combined RRU+WRU simplifies aggregation; fine-grained breakdown deferred
//   - ExecCount: Distinguishes finished (count=1) from mid-flight (count=0) SQLs
//   - ExecDuration: Enables avg duration calculation at report time
//
// Phase 2 Extension Point:
//   - TODO(M3): May split RRU/WRU for finer diagnosis
type RUIncrement struct {
	// TotalRU is the delta RU consumption (RRU + WRU).
	TotalRU float64

	// ExecCount is the number of SQL executions that contributed to this increment.
	// Begin-based semantics: each execution contributes at most one count on its
	// first positive RU delta (tick or finish); later deltas carry count=0.
	ExecCount uint64

	// ExecDuration is the cumulative execution time in nanoseconds.
	// For active executions, this reflects duration up to the sample point.
	ExecDuration uint64
}

// Merge merges other into this RUIncrement.
func (r *RUIncrement) Merge(other *RUIncrement) {
	r.TotalRU += other.TotalRU
	r.ExecCount += other.ExecCount
	r.ExecDuration += other.ExecDuration
}

// RUIncrementMap maps RUKey to aggregated RU increments.
// This is the output type of StatementStats.MergeRUInto() and the input
// type for RUCollector.CollectRUIncrements().
//
// Design Rationale:
//   - Pointer values (*RUIncrement) enable in-place aggregation during Merge
//   - Map key by value (RUKey) rather than pointer for correct grouping
//
// Memory Bounds (Phase 2):
//   - TopN filtering (200 users × 200 SQLs) applied in reporter.ruCollecting
type RUIncrementMap map[RUKey]*RUIncrement

// Merge merges other into RUIncrementMap.
// Values with the same RUKey will be aggregated.
func (m RUIncrementMap) Merge(other RUIncrementMap) {
	if m == nil || other == nil {
		return
	}
	for key, otherIncr := range other {
		incr, ok := m[key]
		if !ok {
			m[key] = otherIncr
			continue
		}
		incr.Merge(otherIncr)
	}
}
