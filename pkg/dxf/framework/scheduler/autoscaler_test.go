// Copyright 2025 PingCAP, Inc.
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

package scheduler

import (
	"fmt"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/dxf/framework/schstatus"
	"github.com/stretchr/testify/require"
)

func TestCalcMaxNodeCountByTableSize(t *testing.T) {
	tests := []struct {
		tableSize int64
		cores     int
		expected  int
	}{
		{0, 8, 1},
		{10, 0, 0},
		{320*units.GiB + 100, 4, 3},
		{100 * units.TiB, 4, 60},
		{10 * units.GiB, 8, 1},
		{200 * units.GiB, 8, 1},
		{800 * units.GiB, 8, 4},
		{1100 * units.GiB, 8, 6},
		{200 * units.TiB, 8, 30},
		{200 * units.GiB, 16, 1},
		{600 * units.GiB, 16, 2},
		{1200 * units.GiB, 16, 3},
		{4 * units.TiB, 16, 10},
		{6 * units.TiB, 16, 15},
		{10 * units.TiB, 16, 15},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			cal := NewRCCalc(tt.tableSize, tt.cores, 0, schstatus.GetDefaultTuneFactors())
			got := cal.CalcMaxNodeCountForAddIndex()
			require.Equal(t, tt.expected, got, fmt.Sprintf("tableSize:%d cores:%d", tt.tableSize, tt.cores))
		})
	}
}
func TestCalcMaxNodeCountByDataSize(t *testing.T) {
	tests := []struct {
		dataSize int64
		cores    int
		expected int
	}{
		{0, 8, 1},
		{10, 0, 0},
		{320*units.GiB + 100, 4, 3},
		{100 * units.TiB, 4, 64},
		{10 * units.GiB, 8, 1},
		{200 * units.GiB, 8, 1},
		{800 * units.GiB, 8, 4},
		{1100*units.GiB - 100, 8, 5},
		{1100 * units.GiB, 8, 6},
		{200 * units.TiB, 8, 32},
		{200 * units.GiB, 16, 1},
		{600 * units.GiB, 16, 2},
		{1200 * units.GiB, 16, 3},
		{4 * units.TiB, 16, 10},
		{6 * units.TiB, 16, 15},
		{10 * units.TiB, 16, 16},
		{100 * units.TiB, 16, 16},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			calc := NewRCCalc(tt.dataSize, tt.cores, 0, schstatus.GetDefaultTuneFactors())
			got := calc.CalcMaxNodeCountForImportInto()
			require.Equal(t, tt.expected, got,
				fmt.Sprintf("dataSize:%d cores:%d", tt.dataSize, tt.cores))
		})
	}
}

func TestCalcRequiredSlotsByDataSize(t *testing.T) {
	tests := []struct {
		dataSize int64
		cores    int
		expected int
	}{
		{0, 5, 4},
		{-100, 3, 4},
		{24 * units.GiB, 5, 1},
		{25 * units.GiB, 5, 1},
		{25 * units.GiB, 1, 1},
		{50 * units.GiB, 4, 2},
		{100 * units.GiB, 3, 3},
		{37*units.GiB + 512*units.MiB, 8, 2},
		{50 * units.GiB, 8, 2},
		{100 * units.GiB, 8, 4},
		{50 * units.GiB, 10, 2},
		{75 * units.GiB, 4, 3},
		{25 * 1000 * units.GiB, 16, 16},
		{1, 5, 1},
	}
	for _, tt := range tests {
		calc := NewRCCalc(tt.dataSize, tt.cores, 0, schstatus.GetDefaultTuneFactors())
		require.Equal(t, tt.expected, calc.CalcRequiredSlots(),
			fmt.Sprintf("dataSize:%d cores:%d", tt.dataSize, tt.cores))
	}
}

func TestCalcDistSQLConcurrency(t *testing.T) {
	tests := []struct {
		// concurrency, nodeCount, nodeCoreCount
		c, n, nc int
		expected int
	}{
		// on 8c machine
		{c: 1, n: 1, nc: 8, expected: 15},
		{c: 3, n: 1, nc: 8, expected: 45},
		{c: 7, n: 1, nc: 8, expected: 105},
		{c: 8, n: 1, nc: 8, expected: 120},
		{c: 8, n: 2, nc: 8, expected: 124},
		{c: 8, n: 5, nc: 8, expected: 137},
		{c: 8, n: 32, nc: 8, expected: 256},
		{c: 8, n: 33, nc: 8, expected: 256},
		{c: 8, n: 50, nc: 8, expected: 256},
		// on 16c machine
		{c: 1, n: 1, nc: 16, expected: 15},
		{c: 7, n: 1, nc: 16, expected: 105},
		{c: 16, n: 1, nc: 16, expected: 240},
		{c: 16, n: 5, nc: 16, expected: 275},
		{c: 16, n: 32, nc: 16, expected: 512},
		{c: 16, n: 33, nc: 16, expected: 512},
		{c: 16, n: 50, nc: 16, expected: 512},
		// on 32c machine
		{c: 1, n: 1, nc: 32, expected: 15},
		{c: 7, n: 1, nc: 32, expected: 105},
		{c: 32, n: 1, nc: 32, expected: 480},
		{c: 32, n: 5, nc: 32, expected: 550},
		{c: 32, n: 32, nc: 32, expected: 1024},
		{c: 32, n: 33, nc: 32, expected: 1024},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			require.Equal(t, tt.expected, CalcDistSQLConcurrency(tt.c, tt.n, tt.nc))
		})
	}
}

func TestIndexSizeRatio(t *testing.T) {
	tests := []struct {
		dataSize       int64
		cores          int
		indexSizeRatio float64
		expectedC      int
		expectedN      int
	}{
		{100 * units.GiB, 8, 0, 4, 1},
		{100 * units.GiB, 8, 1.0, 8, 1},
		{100 * units.GiB, 8, 1.5, 8, 1},
		{100 * units.GiB, 8, 2.0, 8, 2},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			calc := NewRCCalc(tt.dataSize, tt.cores, tt.indexSizeRatio, schstatus.GetDefaultTuneFactors())
			require.Equal(t, tt.expectedC, calc.CalcRequiredSlots())
			require.Equal(t, tt.expectedN, calc.CalcMaxNodeCountForImportInto())
		})
	}
}

func TestTuneFactors(t *testing.T) {
	tests := []struct {
		dataSize      int64
		cores         int
		amplifyFactor float64
		expectedC     int
		importN       int
		addIndexN     int
	}{
		{100 * units.GiB, 8, 1, 4, 1, 1},
		{1000 * units.GiB, 8, 1, 8, 5, 5},
		{1000 * units.GiB, 8, 1.5, 8, 8, 8},
		{1000 * units.GiB, 8, 2, 8, 10, 10},
		{1000 * units.GiB, 8, 10, 8, 50, 50},
		{100 * units.TiB, 8, 2, 8, 64, 60},
		{100 * units.TiB, 8, 5, 8, 160, 150},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			calc := NewRCCalc(tc.dataSize, tc.cores, 0, &schstatus.TuneFactors{AmplifyFactor: tc.amplifyFactor})
			require.Equal(t, tc.expectedC, calc.CalcRequiredSlots())
			require.Equal(t, tc.importN, calc.CalcMaxNodeCountForImportInto())
			require.Equal(t, tc.addIndexN, calc.CalcMaxNodeCountForAddIndex())
		})
	}
}
