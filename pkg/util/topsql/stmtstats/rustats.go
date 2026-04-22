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
// This file defines RU data types used by TopRU.
package stmtstats

import "github.com/tikv/client-go/v2/util"

// RUKey identifies an RU aggregation key by user, SQL digest, and plan digest.
type RUKey struct {
	User       string
	SQLDigest  BinaryDigest
	PlanDigest BinaryDigest
}

// ExecutionContext stores RU sampling state for one active SQL execution.
type ExecutionContext struct {
	// RUDetails caches *util.RUDetails from execution begin.
	RUDetails *util.RUDetails

	// Key identifies this execution by (user, sql_digest, plan_digest).
	Key RUKey

	// LastRUTotal is the last observed cumulative RU total (RRU + WRU).
	LastRUTotal float64
}

// RUIncrement represents a delta RU consumption for a specific RUKey.
// This is the unit of data produced by StatementStats.MergeRUInto() and consumed by RUCollector.CollectRUIncrements().
type RUIncrement struct {
	// TotalRU is the delta RU consumption (RRU + WRU).
	TotalRU float64

	// ExecCount is the number of SQL executions included in this increment.
	// Begin-based semantics: each execution contributes at most one count on its
	// first positive RU delta (tick or finish); later deltas carry count=0.
	ExecCount uint64

	// ExecDuration is the cumulative execution time in nanoseconds.
	ExecDuration uint64
}

// Merge merges other into this RUIncrement.
func (r *RUIncrement) Merge(other *RUIncrement) {
	r.TotalRU += other.TotalRU
	r.ExecCount += other.ExecCount
	r.ExecDuration += other.ExecDuration
}

// RUIncrementMap maps RUKey to aggregated RU increments.
// This is the output type of StatementStats.MergeRUInto() and the input type for RUCollector.CollectRUIncrements().
type RUIncrementMap map[RUKey]*RUIncrement
