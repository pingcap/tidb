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
	"testing"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestTiFlashExecutionUnits(t *testing.T) {
	id := "HashJoin_1"
	rows, size, bytes := uint64(8), uint64(1), uint64(20)
	summary := &tipb.ExecutorExecutionSummary{
		ExecutorId: &id, NumProducedRows: &rows,
		TiflashHashTableStats: &tipb.TiFlashHashTableStats{Size_: &size},
		DetailInfo:            &tipb.ExecutorExecutionSummary_TiflashScanContext{TiflashScanContext: &tipb.TiFlashScanContext{UserReadBytes: &bytes}},
		TiflashNetworkSummary: &tipb.TiFlashNetWorkSummary{InnerZoneSendBytes: &bytes, InterZoneSendBytes: &size, InnerZoneReceiveBytes: &bytes},
	}
	stats := NewRuntimeStatsColl(nil)
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{summary})
	size = 8
	summary.TiflashHashTableStats.SizeKind = tipb.TiFlashHashTableSizeKind_TIFLASH_HASH_TABLE_SIZE_KIND_BUILD_ROW_COUNT.Enum()
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{summary})
	units, found := stats.GetTiFlashExecutionUnits(1)
	require.True(t, found)
	require.False(t, units.Invalid)
	require.Equal(t, uint64(16), units.Rows)
	require.Equal(t, uint64(1), units.HashDistinctEntries)
	require.Equal(t, uint64(8), units.HashBuildRows)
	require.Equal(t, uint64(40), units.UserReadBytes)
	require.Equal(t, uint64(40), units.InnerZoneSendBytes)
	require.Equal(t, uint64(9), units.InterZoneSendBytes)
	require.Zero(t, units.Missing)

	// Dummy/legacy fields cannot establish new evidence. Explicit zero can.
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{{ExecutorId: &id}})
	zero := uint64(0)
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, NumProducedRows: &zero}})
	units, _ = stats.GetTiFlashExecutionUnits(1)
	require.NotZero(t, units.Missing&TiFlashUnitRows)
	require.NotZero(t, units.Observed&TiFlashUnitRows)
	require.Equal(t, uint64(16), units.Rows)

	// Collector reuse is the usual next-statement path, including reused plan IDs.
	require.Same(t, stats, NewRuntimeStatsColl(stats))
	_, found = stats.GetTiFlashExecutionUnits(1)
	require.False(t, found)
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, NumProducedRows: &zero}})
	units, found = stats.GetTiFlashExecutionUnits(1)
	require.True(t, found)
	require.Zero(t, units.Rows)
	require.NotZero(t, units.Observed&TiFlashUnitRows)
}

func TestTiFlashExecutionUnitsInvalid(t *testing.T) {
	id, other := "TableScan_1", "TableScan_2"
	one, tooMany := uint64(1), uint64(math.MaxInt64)+1
	tests := []struct {
		name      string
		summaries []*tipb.ExecutorExecutionSummary
		invalid   bool
	}{
		{"duplicate", []*tipb.ExecutorExecutionSummary{{ExecutorId: &id}, {ExecutorId: &id}}, true},
		{"row conversion overflow", []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, NumProducedRows: &tooMany}}, true},
		{"unrelated and nil", []*tipb.ExecutorExecutionSummary{nil, {ExecutorId: &other, NumProducedRows: &tooMany}, {ExecutorId: &id, NumProducedRows: &one}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stats := NewRuntimeStatsColl(nil)
			stats.RecordTiFlashExecutionSummaries([]int{1}, tc.summaries)
			// Later normal evidence must not erase invalid state.
			stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, NumProducedRows: &one}})
			units, _ := stats.GetTiFlashExecutionUnits(1)
			require.Equal(t, tc.invalid, units.Invalid)
			_, found := stats.GetTiFlashExecutionUnits(2)
			require.False(t, found)
		})
	}
	stats := NewRuntimeStatsColl(nil)
	maxBytes := uint64(math.MaxUint64)
	summary := &tipb.ExecutorExecutionSummary{ExecutorId: &id, TiflashNetworkSummary: &tipb.TiFlashNetWorkSummary{InnerZoneSendBytes: &maxBytes}}
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{summary})
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{summary})
	units, _ := stats.GetTiFlashExecutionUnits(1)
	require.True(t, units.Invalid)

	unknown := tipb.TiFlashHashTableSizeKind(99)
	stats = NewRuntimeStatsColl(nil)
	stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, NumProducedRows: &one, TiflashHashTableStats: &tipb.TiFlashHashTableStats{Size_: &one, SizeKind: &unknown}}})
	units, _ = stats.GetTiFlashExecutionUnits(1)
	require.False(t, units.Invalid)
	require.Equal(t, one, units.Rows)
	require.NotZero(t, units.Missing&TiFlashUnitHash)
	require.Zero(t, units.HashDistinctEntries+units.HashBuildRows)
}

func TestTiFlashExecutionUnitsColumnar(t *testing.T) {
	id := "TableScan_1"
	zero, readBytes, mvccBytes, maxBytes := uint64(0), uint64(20), uint64(100), uint64(math.MaxUint64)
	for _, tc := range []struct {
		name              string
		bytes             *uint64
		wantBytes         uint64
		observed, invalid bool
	}{
		{"missing", nil, 0, false, false},
		{"zero", &zero, 0, true, false},
		{"nonzero", &readBytes, 40, true, false},
		{"overflow", &maxBytes, math.MaxUint64, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := NewRuntimeStatsColl(nil)
			summary := &tipb.ExecutorExecutionSummary{ExecutorId: &id,
				DetailInfo: &tipb.ExecutorExecutionSummary_ColumnarScanContext{ColumnarScanContext: &tipb.ColumnarScanContext{
					UserReadBytes: tc.bytes, MvccInputBytes: &mvccBytes,
				}},
			}
			// Task contributions merge, but MVCC input bytes are not added to user-read bytes.
			stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{summary})
			stats.RecordTiFlashExecutionSummaries([]int{1}, []*tipb.ExecutorExecutionSummary{summary})
			units, found := stats.GetTiFlashExecutionUnits(1)
			require.True(t, found)
			require.Equal(t, tc.wantBytes, units.UserReadBytes)
			require.Equal(t, tc.observed, units.Observed&TiFlashUnitScan != 0)
			require.Equal(t, !tc.observed, units.Missing&TiFlashUnitScan != 0)
			require.Equal(t, tc.invalid, units.Invalid)
		})
	}
}
