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

package crr_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestPartialCRRReplicationFailsRestoreValidationEvenIfCheckpointMatches(t *testing.T) {
	ctx := context.Background()
	stores := testutil.StoreIDRange(1, 6)
	boundaries, err := testutil.NewRegionLayoutBuilder().
		AddRoundRobinRegions(10, stores...).
		Build()
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarness(ctx, t.TempDir(), boundaries)
	require.NoError(t, err)
	t.Cleanup(h.Close)

	var upstreamCheckpoint uint64
	for range 3 {
		for _, storeID := range stores {
			_, err := h.FlushSim.FlushStore(ctx, storeID)
			require.NoError(t, err)
		}
		upstreamCheckpoint = h.PDSim.CurrentTSO()
	}

	require.NoError(t, h.UploadGlobalCheckpoint(ctx, upstreamCheckpoint))

	pulled := h.PullMessages(0)
	require.Greater(t, pulled, 0)

	replicated, err := h.Replicate(ctx, pulled/3)
	require.NoError(t, err)
	require.Greater(t, replicated, 0)
	require.Less(t, replicated, pulled)

	// A naive checkpoint-only check can still pass.
	downstreamCheckpoint := upstreamCheckpoint
	err = h.AssertDownstreamCanRestoreTo(ctx, downstreamCheckpoint)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not readable")
}
