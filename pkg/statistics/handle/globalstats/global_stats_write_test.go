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

package globalstats_test

import (
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/stretchr/testify/require"
)

type saveResultStatsHandle struct {
	// Embed the unused methods so the test double only needs to override the
	// persistence operation exercised by WriteGlobalStatsToStorage.
	statstypes.StatsHandle
	saveResults []error
	saveCalls   int
}

func (h *saveResultStatsHandle) SaveColOrIdxStatsToStorage(
	_ int64,
	_, _ int64,
	_ int,
	_ *statistics.Histogram,
	_ *statistics.CMSketch,
	_ *statistics.TopN,
	_ int,
	_ int64,
	_ bool,
	_ string,
) error {
	result := h.saveResults[h.saveCalls]
	h.saveCalls++
	return result
}

// TestWriteGlobalStatsToStoragePreservesEarlierError verifies that a later
// successful histogram write does not hide an earlier failure. It injects a
// failure followed by success and expects both a partial update and the first error.
func TestWriteGlobalStatsToStoragePreservesEarlierError(t *testing.T) {
	firstErr := errors.New("first histogram write failed")
	handle := &saveResultStatsHandle{saveResults: []error{firstErr, nil}}
	globalStats := &globalstats.GlobalStats{
		Hg:   []*statistics.Histogram{{ID: 1}, {ID: 2}},
		Cms:  make([]*statistics.CMSketch, 2),
		TopN: make([]*statistics.TopN, 2),
		Num:  2,
	}

	updated, err := globalstats.WriteGlobalStatsToStorage(handle, globalStats, &statstypes.GlobalStatsInfo{}, 42)

	require.True(t, updated)
	require.ErrorIs(t, err, firstErr)
	require.Equal(t, 2, handle.saveCalls)
}
