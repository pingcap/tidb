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
	"net"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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

// BuildExecutorForTest builds stmt's executor tree. It is exported only for
// external package tests that need to assert executor-build behavior.
func BuildExecutorForTest(ctx context.Context, stmt *ExecStmt) error {
	_, err := stmt.buildExecutor(ctx)
	return err
}

func TestCanBroadcastAnalyzeStatsDeltaSkipsNonServingNodes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	port := listener.Addr().(*net.TCPAddr).Port
	const fp = "github.com/pingcap/tidb/pkg/domain/infosync/mockGetAllServerInfo"
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
	for _, tc := range []struct {
		name         string
		info         string
		canBroadcast bool
	}{
		{"only disabled", `{"br":{"disable_rpc":true}}`, false},
		{"serving and disabled", fmt.Sprintf(`{"tidb":{"ip":"127.0.0.1","status_port":%d},"br":{"disable_rpc":true}}`, port), true},
		{"explicit false and disabled", fmt.Sprintf(`{"tidb":{"ip":"127.0.0.1","status_port":%d,"disable_rpc":false},"br":{"disable_rpc":true}}`, port), true},
		{"non-serving endpoint without flag", fmt.Sprintf(`{"tidb":{"ip":"127.0.0.1","status_port":%d},"other":{"ip":""}}`, port), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, failpoint.Enable(fp, fmt.Sprintf("return(`%s`)", tc.info)))
			canBroadcast, err := canBroadcastAnalyzeStatsDeltaForTest(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.canBroadcast, canBroadcast)
		})
	}
}
