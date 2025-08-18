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
	"context"
	"fmt"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestCalcMaxNodeCountByTableSize(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		tableSize int64
		cores     int
		expected  int64
	}{
		{0, 8, 1},
		{1, 8, 1},
		{200 * units.GiB, 8, 1},         // 200GiB
		{200 * units.GiB * 3, 8, 3},     // 600GiB
		{200 * units.GiB * 6, 8, 6},     // 1.2TiB
		{200 * units.GiB * 30, 8, 30},   // 6TiB
		{200 * units.GiB * 1000, 8, 30}, // 200TiB
		{200 * units.GiB * 25, 4, 50},   // 5TiB
		{200 * units.GiB * 30, 3, 80},   // 6TiB
		{200*units.GiB*29 + 100, 8, 29}, // 5.8TiB
		{1000, 0, 0},
	}
	for _, tt := range tests {
		got := CalcMaxNodeCountByTableSize(ctx, tt.tableSize, tt.cores)
		require.Equal(t, tt.expected, got, fmt.Sprintf("tableSize:%d cores:%d", tt.tableSize, tt.cores))
	}
}
func TestCalcMaxNodeCountByDataSize(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		dataSize int64
		cores    int
		expected int64
	}{
		{0, 8, 1},
		{10, 0, 0},
		{10 * units.GiB, 8, 1},
		{105 * units.GiB, 8, 1},
		{200 * units.GiB, 8, 1},
		{800 * units.GiB, 8, 4},
		{800 * units.GiB, 5, 6},
		{1*units.TiB + 100*units.GiB, 8, 5},
		{200 * units.TiB, 8, 32},
		{200 * units.TiB, 4, 64},
		{320*units.GiB + 100, 4, 3},
	}
	for _, tt := range tests {
		got := CalcMaxNodeCountByDataSize(ctx, tt.dataSize, tt.cores)
		require.Equal(t, tt.expected, got,
			fmt.Sprintf("dataSize:%d cores:%d", tt.dataSize, tt.cores))
	}
}

func TestCalcConcurrencyByDataSize(t *testing.T) {
	ctx := context.Background()
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
		{100 * units.GiB, 3, 2},
		{50 * units.GiB, 10, 2},
		{75 * units.GiB, 4, 3},
		{25 * 1000 * units.GiB, 16, 15},
		{1, 5, 1},
	}
	for _, tt := range tests {
		require.Equal(t, tt.expected, CalcConcurrencyByDataSize(ctx, tt.dataSize, tt.cores),
			fmt.Sprintf("dataSize:%d cores:%d", tt.dataSize, tt.cores))
	}
}
