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
// TopRU notes:
//
// This file defines RU data structures for TopRU.
// TopRU aggregates RU by (user, sql_digest, plan_digest) and uses a pipeline
// separate from TopSQL CPU reporting.
package stmtstats

import "github.com/tikv/client-go/v2/util"

// RUKey identifies an RU aggregation key by user, SQL digest, and plan digest.
type RUKey struct {
	User       string
	SQLDigest  BinaryDigest
	PlanDigest BinaryDigest
}

// ExecutionContext tracks RU state for one SQL execution.
// It keeps the cached RUDetails pointer, the RU key, and the last sampled total.
// Delta is calculated as (RRU + WRU) - LastRUTotal, and non-positive deltas
// are ignored.
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
// TotalRU stores RRU+WRU.
// ExecCount is begin-based: the first positive delta contributes 1, and later
// deltas contribute 0.
// ExecDuration stores cumulative execution time in nanoseconds.
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
// Pointer values (*RUIncrement) allow in-place aggregation during Merge.
// RUKey is used by value to keep grouping stable.
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
