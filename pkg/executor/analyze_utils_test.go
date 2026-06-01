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

	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

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
