// Copyright 2023 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestIsUnsupportedBroadcastQueryErr(t *testing.T) {
	// An old peer rejects the BroadcastQuery executor during a rolling upgrade;
	// the message is produced by PBPlanBuilder.pbToPhysicalPlan.
	require.True(t, isUnsupportedBroadcastQueryErr(errors.New("other error: this exec type 17 doesn't support yet")))
	require.True(t, isUnsupportedBroadcastQueryErr(errors.Trace(errors.New("this exec type 17 doesn't support yet"))))

	// Unrelated errors must still propagate and fail analyze.
	require.False(t, isUnsupportedBroadcastQueryErr(nil))
	require.False(t, isUnsupportedBroadcastQueryErr(errors.New("context canceled")))
	require.False(t, isUnsupportedBroadcastQueryErr(errors.New("region unavailable")))
}

// https://github.com/pingcap/tidb/issues/45690
func TestGetAnalyzePanicErr(t *testing.T) {
	errMsg := fmt.Sprintf("%s", getAnalyzePanicErr(exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(123)))
	require.NotContains(t, errMsg, `%!(EXTRA`)
}

func TestCollectStatsDeltaFlushObjectsForAnalyzeDottedNames(t *testing.T) {
	plan := &core.Analyze{
		ColTasks: []core.AnalyzeColumnsTask{
			// Quoted identifiers may contain dots. These first two targets both
			// stringify to "a.b.c" if db and table names are joined with ".".
			{AnalyzeInfo: core.AnalyzeInfo{DBName: "a.b", TableName: "c"}},
			{AnalyzeInfo: core.AnalyzeInfo{DBName: "a", TableName: "b.c"}},
			// Keep the duplicate target deduped.
			{AnalyzeInfo: core.AnalyzeInfo{DBName: "a", TableName: "b.c"}},
		},
	}

	flushObjects := collectStatsDeltaFlushObjectsForAnalyze(plan)
	targets := make([][2]string, 0, len(flushObjects))
	for _, obj := range flushObjects {
		targets = append(targets, [2]string{obj.DBName.O, obj.TableName.O})
	}

	require.ElementsMatch(t, [][2]string{
		{"a.b", "c"},
		{"a", "b.c"},
	}, targets)
}

func TestCanBroadcastToTiDBRPCForTestRejectsInvalidEndpoints(t *testing.T) {
	// Regression for next-gen realcluster tests: in-process domains can register
	// multiple server infos with an empty IP/default :10080 but no TiDB RPC
	// listener. Such targets must not take the broadcast path.
	require.False(t, canBroadcastToTiDBRPCForTest(context.Background(), []string{"", ""}))
}

func TestAnalyzeBatchScanBudget(t *testing.T) {
	// Match a simple reference over the complete small-input rectangle.
	for regionCount := 0; regionCount <= 512; regionCount++ {
		for inputBudget := 0; inputBudget <= 128; inputBudget++ {
			budget := max(inputBudget, 1)
			wantWidth := 1
			if regionCount > 2*budget {
				pressure := (regionCount + budget - 1) / budget
				for width := 1; width <= pressure && width <= budget/(width+1); width++ {
					wantWidth = width
				}
			}
			outer, batchSize := analyzeBatchScanBudget(regionCount, inputBudget)
			require.Equalf(t, []int{budget / wantWidth, wantWidth - 1}, []int{outer, batchSize}, "N=%d C=%d", regionCount, inputBudget)
			require.LessOrEqual(t, outer*(batchSize+1), budget)
		}
	}

	// Both threshold and ceiling calculations remain safe at the integer limit.
	maxInt := int(^uint(0) >> 1)
	outer, batchSize := analyzeBatchScanBudget(maxInt, 64)
	width := batchSize + 1
	require.Equal(t, 7, width)
	require.Equal(t, 64/width, outer)
	outer, batchSize = analyzeBatchScanBudget(maxInt, maxInt)
	require.Equal(t, maxInt, outer)
	require.Zero(t, batchSize)
}

func TestCountAnalyzeRequestRegions(t *testing.T) {
	var cluster testutils.Cluster
	var tailRegionID uint64
	store, err := mockstore.NewMockStore(
		mockstore.WithStoreType(mockstore.EmbedUnistore),
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			_, _, tailRegionID = mockstore.BootstrapWithSingleStore(c)
			cluster = c
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	// Lookup errors and empty range sets safely disable batching.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ranges := kv.NewNonPartitionedKeyRanges([]kv.KeyRange{{StartKey: []byte("a"), EndKey: []byte("z")}})
	require.Zero(t, countAnalyzeRequestRegions(canceledCtx, store, ranges, nil))
	emptyRanges := kv.NewPartitionedKeyRanges(nil)
	require.Zero(t, countAnalyzeRequestRegions(context.Background(), store, emptyRanges, nil))
	// Unsorted overlapping partitions count each Region only once.
	for _, key := range []string{"g", "n", "t"} {
		newRegionID, newPeerID := cluster.AllocID(), cluster.AllocID()
		cluster.Split(tailRegionID, newRegionID, store.GetCodec().EncodeKey([]byte(key)), []uint64{newPeerID}, newPeerID)
		tailRegionID = newRegionID
	}
	ranges = kv.NewPartitionedKeyRanges([][]kv.KeyRange{
		{{StartKey: []byte("h"), EndKey: []byte("z")}},
		{{StartKey: []byte("a"), EndKey: []byte("m")}},
	})
	require.Equal(t, 4, countAnalyzeRequestRegions(context.Background(), store, ranges, nil))
}

// BuildExecutorForTest builds stmt's executor tree. It is exported only for
// external package tests that need to assert executor-build behavior.
func BuildExecutorForTest(ctx context.Context, stmt *ExecStmt) error {
	_, err := stmt.buildExecutor(ctx)
	return err
}
