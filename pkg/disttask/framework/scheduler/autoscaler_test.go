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
		{1100 * units.GiB, 8, 5},
		{200 * units.TiB, 8, 30},
		{200 * units.GiB, 16, 1},
		{600 * units.GiB, 16, 1},
		{1200 * units.GiB, 16, 3},
		{4 * units.TiB, 16, 10},
		{6 * units.TiB, 16, 15},
		{10 * units.TiB, 16, 15},
	}
	for _, tt := range tests {
		got := CalcMaxNodeCountByTableSize(tt.tableSize, tt.cores)
		require.Equal(t, tt.expected, got, fmt.Sprintf("tableSize:%d cores:%d", tt.tableSize, tt.cores))
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
		{1100 * units.GiB, 8, 5},
		{200 * units.TiB, 8, 32},
		{200 * units.GiB, 16, 1},
		{600 * units.GiB, 16, 1},
		{1200 * units.GiB, 16, 3},
		{4 * units.TiB, 16, 10},
		{6 * units.TiB, 16, 15},
		{10 * units.TiB, 16, 16},
		{100 * units.TiB, 16, 16},
	}
	for _, tt := range tests {
		got := CalcMaxNodeCountByDataSize(tt.dataSize, tt.cores)
		require.Equal(t, tt.expected, got,
			fmt.Sprintf("dataSize:%d cores:%d", tt.dataSize, tt.cores))
	}
}

func TestCalcConcurrencyByDataSize(t *testing.T) {
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
		{50 * units.GiB, 10, 2},
		{75 * units.GiB, 4, 3},
		{25 * 1000 * units.GiB, 16, 16},
		{1, 5, 1},
	}
	for _, tt := range tests {
		require.Equal(t, tt.expected, CalcConcurrencyByDataSize(tt.dataSize, tt.cores),
			fmt.Sprintf("dataSize:%d cores:%d", tt.dataSize, tt.cores))
	}
}
