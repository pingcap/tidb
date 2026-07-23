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
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
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
	tests := []struct {
		name            string
		concurrency     int
		storeBatchSize  int
		wantConcurrency int
		wantBatch       int
	}{
		{"batching disabled", 15, 0, 15, 0},
		{"negative batch treated as disabled", 15, -1, 15, 0},
		{"large-cluster adaptive 15/4 fills budget exactly", 15, 4, 3, 4},
		{"analyze defaults 4/4 shrink group to budget", 4, 4, 1, 3},
		{"tiny concurrency shrinks group width", 2, 4, 1, 1},
		{"single scan degenerates to unbatched", 1, 4, 1, 0},
		{"slack rounds down, never exceeds", 7, 4, 1, 4},
		{"huge batch capped by server maximum", 8, 100, 1, 4},
		{"wide budget still respects server maximum", 32, 100, 6, 4},
		{"non-positive concurrency clamped", 0, 4, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotConcurrency, gotBatch := analyzeBatchScanBudget(tt.concurrency, tt.storeBatchSize)
			require.Equal(t, tt.wantConcurrency, gotConcurrency)
			require.Equal(t, tt.wantBatch, gotBatch)

			// The invariants behind every case: the physical fan-out
			// (workers × regions scanned per group) stays within the
			// pre-batching scan budget, and a group never exceeds the
			// store-supported width.
			require.LessOrEqual(t, gotConcurrency*(gotBatch+1), max(tt.concurrency, 1))
			require.GreaterOrEqual(t, gotConcurrency, 1)
			require.LessOrEqual(t, gotBatch, maxAnalyzeStoreBatchSize)
		})
	}
}

// BuildExecutorForTest builds stmt's executor tree. It is exported only for
// external package tests that need to assert executor-build behavior.
func BuildExecutorForTest(ctx context.Context, stmt *ExecStmt) error {
	_, err := stmt.buildExecutor(ctx)
	return err
}
