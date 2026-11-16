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

package execdetails

import (
	"math"
	"slices"

	"github.com/pingcap/tipb/go-tipb"
)

// TiFlashUnitFields describes presence in received summaries, not task coverage.
type TiFlashUnitFields uint8

// Presence bits for fields carried by a received execution summary.
const (
	TiFlashUnitRows TiFlashUnitFields = 1 << iota
	TiFlashUnitHash
	TiFlashUnitScan
	TiFlashUnitNetwork
)

// TiFlashExecutionUnits retains raw MPP evidence separately from EXPLAIN's dummy
// summaries. Missing evidence is best-effort zero; present invalid evidence is not.
// Streamed summaries may already combine tasks, so Observed never proves coverage.
type TiFlashExecutionUnits struct {
	Rows                uint64
	HashDistinctEntries uint64
	HashBuildRows       uint64
	UserReadBytes       uint64
	InnerZoneSendBytes  uint64
	InterZoneSendBytes  uint64
	Observed            TiFlashUnitFields
	Missing             TiFlashUnitFields
	Invalid             bool
}

func tiFlashExecutionUnits(summary *tipb.ExecutorExecutionSummary) TiFlashExecutionUnits {
	var units TiFlashExecutionUnits
	if summary.NumProducedRows != nil {
		units.Rows = summary.GetNumProducedRows()
		units.Observed |= TiFlashUnitRows
	}
	if stats := summary.GetTiflashHashTableStats(); stats != nil && stats.Size_ != nil {
		switch stats.GetSizeKind() {
		case tipb.TiFlashHashTableSizeKind_TIFLASH_HASH_TABLE_SIZE_KIND_DISTINCT_KEY_COUNT:
			units.HashDistinctEntries = stats.GetSize_()
			units.Observed |= TiFlashUnitHash
		case tipb.TiFlashHashTableSizeKind_TIFLASH_HASH_TABLE_SIZE_KIND_BUILD_ROW_COUNT:
			units.HashBuildRows = stats.GetSize_()
			units.Observed |= TiFlashUnitHash
		}
	}
	if scan := summary.GetTiflashScanContext(); scan != nil && scan.UserReadBytes != nil {
		units.UserReadBytes = scan.GetUserReadBytes()
		units.Observed |= TiFlashUnitScan
	} else if scan := summary.GetColumnarScanContext(); scan != nil && scan.UserReadBytes != nil {
		// Columnar user_read_bytes is the producer's returned-block byte count,
		// not physical storage I/O. Do not add its separate mvcc_input_bytes.
		units.UserReadBytes = scan.GetUserReadBytes()
		units.Observed |= TiFlashUnitScan
	}
	if network := summary.GetTiflashNetworkSummary(); network != nil {
		units.InnerZoneSendBytes = network.GetInnerZoneSendBytes()
		units.InterZoneSendBytes = network.GetInterZoneSendBytes()
		if network.InnerZoneSendBytes != nil && network.InterZoneSendBytes != nil {
			units.Observed |= TiFlashUnitNetwork
		}
	}
	units.Missing = (TiFlashUnitRows | TiFlashUnitHash | TiFlashUnitScan | TiFlashUnitNetwork) &^ units.Observed
	return units
}

func (u *TiFlashExecutionUnits) merge(other TiFlashExecutionUnits) {
	u.Observed |= other.Observed
	u.Missing |= other.Missing
	u.Invalid = u.Invalid || other.Invalid
	add := func(dst *uint64, n uint64) {
		if n > math.MaxUint64-*dst {
			u.Invalid = true
			return
		}
		*dst += n
	}
	add(&u.Rows, other.Rows)
	add(&u.HashDistinctEntries, other.HashDistinctEntries)
	add(&u.HashBuildRows, other.HashBuildRows)
	add(&u.UserReadBytes, other.UserReadBytes)
	add(&u.InnerZoneSendBytes, other.InnerZoneSendBytes)
	add(&u.InterZoneSendBytes, other.InterZoneSendBytes)
	// The plan walker uses signed row counts. Validate before any conversion.
	u.Invalid = u.Invalid || u.Rows > math.MaxInt64
}

// RecordTiFlashExecutionSummaries accepts one consumed MPP response or one direct
// task report. Callers own response/task deduplication and the reporting route.
// IDs outside this gather cannot contribute work to another plan.
func (e *RuntimeStatsColl) RecordTiFlashExecutionSummaries(planIDs []int, summaries []*tipb.ExecutorExecutionSummary) {
	if len(summaries) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tiFlashExecutionUnits == nil {
		e.tiFlashExecutionUnits = make(map[int]TiFlashExecutionUnits)
	}
	seen := make(map[int]struct{}, len(summaries))
	for _, summary := range summaries {
		id, ok := getPlanIDFromExecutionSummary(summary)
		if !ok || id <= 0 || !slices.Contains(planIDs, id) {
			continue
		}
		units := e.tiFlashExecutionUnits[id]
		if _, duplicate := seen[id]; duplicate {
			units.Invalid = true
		} else {
			units.merge(tiFlashExecutionUnits(summary))
			seen[id] = struct{}{}
		}
		e.tiFlashExecutionUnits[id] = units
	}
}

// GetTiFlashExecutionUnits returns an immutable value snapshot. Missing summaries
// remain distinct from observed zero, even though both contribute zero units.
func (e *RuntimeStatsColl) GetTiFlashExecutionUnits(planID int) (TiFlashExecutionUnits, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	units, found := e.tiFlashExecutionUnits[planID]
	return units, found
}
