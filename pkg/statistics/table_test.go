// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloneColAndIdxExistenceMap(t *testing.T) {
	m := NewColAndIndexExistenceMapWithoutSize()
	m.InsertCol(1, true)
	m.InsertIndex(1, true)
	m.SetChecked()

	m2 := m.Clone()
	require.Equal(t, m, m2)
}

func TestTableHealth(t *testing.T) {
	// Test with RealtimeCount and ModifyCount
	tbl := &Table{
		RealtimeCount: 1000,
		ModifyCount:   100,
	}
	health := tbl.Health()
	require.True(t, health.Valid)
	require.InDelta(t, 0.1, health.ModifyPct, 0.001)

	// Test with zero row count but modifications
	tbl2 := &Table{
		RealtimeCount: 0,
		ModifyCount:   100,
	}
	health = tbl2.Health()
	require.True(t, health.Valid)
	require.Equal(t, 1.0, health.ModifyPct)

	// Test with zero modifications
	tbl3 := &Table{
		RealtimeCount: 1000,
		ModifyCount:   0,
	}
	health = tbl3.Health()
	require.True(t, health.Valid)
	require.Equal(t, 0.0, health.ModifyPct)

	// Test with column stats
	col := &Column{}
	col.IsFullLoad = func() bool { return true }
	col.TotalRowCount = func() float64 { return 1000 }
	tbl4 := &Table{
		columns:     map[int64]*Column{1: col},
		ModifyCount: 300,
	}
	health = tbl4.Health()
	require.True(t, health.Valid)
	require.InDelta(t, 0.3, health.ModifyPct, 0.001)
}

func TestHealthAndGetStatsHealthy(t *testing.T) {
	// Test Health method
	tbl := &Table{
		RealtimeCount: 1000,
		ModifyCount:   300,
	}
	health := tbl.Health()
	require.True(t, health.Valid)
	require.InDelta(t, 0.3, health.ModifyPct, 0.001)

	// Test GetStatsHealthy method with analyzed table
	tbl.LastAnalyzeVersion = 1 // Set to non-zero to indicate it's been analyzed
	healthy, ok := tbl.GetStatsHealthy()
	require.True(t, ok)
	require.Equal(t, int64(70), healthy) // 70% healthy (30% modified)

	// Test with pseudo table
	pseudoTable := &Table{
		Pseudo: true,
	}
	healthy, ok = pseudoTable.GetStatsHealthy()
	require.False(t, ok)
	require.Equal(t, int64(0), healthy)
}
